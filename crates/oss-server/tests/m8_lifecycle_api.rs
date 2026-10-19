//! M8 S4b：console 与 S3 生命周期入口共用 `LifecycleStore`，以及 Transition sweep 入队/去重。

mod common;

use std::sync::Arc;

use common::state;
use s3s::dto::{
    BucketLifecycleConfiguration, GetBucketLifecycleConfigurationInput, LifecycleRule,
    LifecycleRuleFilter, PutBucketLifecycleConfigurationInput, Transition, TransitionStorageClass,
};
use s3s::{S3, S3Request};
use xl_s3::lifecycle::LifecycleRuleSpec;
use xl_s3::service::OssS3;

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: axum::http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn s3_rule(id: &str) -> LifecycleRule {
    LifecycleRule {
        abort_incomplete_multipart_upload: None,
        expiration: None,
        filter: Some(LifecycleRuleFilter {
            prefix: Some("s3/".into()),
            ..Default::default()
        }),
        id: Some(id.into()),
        noncurrent_version_expiration: None,
        noncurrent_version_transitions: None,
        prefix: None,
        status: s3s::dto::ExpirationStatus::from_static(s3s::dto::ExpirationStatus::ENABLED),
        transitions: Some(vec![Transition {
            days: Some(1),
            storage_class: Some(TransitionStorageClass::from("STANDARD_IA".to_string())),
            ..Default::default()
        }]),
    }
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn login(base: &str) -> String {
    let v: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["token"].as_str().unwrap().to_string()
}

/// console PUT → S3 GET → S3 PUT（全量替换）→ console GET 同一份规则；旧 M4 哨兵行回读。
#[tokio::test]
async fn console_and_s3_share_one_store() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    let s3 = OssS3::new(Arc::clone(&st.store), st.acl(), st.cfg.max_put_bytes)
        .with_lifecycle(Some(Arc::clone(&st.lifecycle)));
    let db = Arc::clone(&st.db);
    let base = serve(oss_server::router::build_router(st).unwrap()).await;
    let token = login(&base).await;
    let http = reqwest::Client::new();

    // console PUT：过期规则 + Transition-only 规则（同一校验）
    let resp = http
        .put(format!("{base}/api/v1/buckets/bkt/lifecycle"))
        .bearer_auth(&token)
        .json(&serde_json::json!([
            {"prefix": "logs/", "expiration_days": 30},
            {"transition_days": 0, "transition_class": "STANDARD_IA", "enabled": true}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204, "{}", resp.text().await.unwrap());

    let rows = db.list_lifecycle_rules("bkt").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|r| r.rule_id.as_deref() == Some("rule-1") && r.expiration_days == Some(30))
    );
    assert!(rows.iter().any(|r| r.rule_id.as_deref() == Some("rule-2")
        && r.transition_class.as_deref() == Some("STANDARD_IA")));

    // S3 GET 看到同一组
    let out = s3
        .get_bucket_lifecycle_configuration(req(GetBucketLifecycleConfigurationInput {
            bucket: "bkt".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let rules = out.rules.unwrap();
    assert_eq!(rules.len(), 2);

    // S3 PUT 全量替换；console GET 回读新的唯一规则
    s3.put_bucket_lifecycle_configuration(req(PutBucketLifecycleConfigurationInput {
        bucket: "bkt".into(),
        lifecycle_configuration: Some(BucketLifecycleConfiguration {
            rules: vec![s3_rule("s3only")],
        }),
        ..Default::default()
    }))
    .await
    .unwrap();
    let listed: Vec<serde_json::Value> = http
        .get(format!("{base}/api/v1/buckets/bkt/lifecycle"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["rule_id"], "s3only");
    assert_eq!(listed[0]["transition_days"], 1);
    assert_eq!(listed[0]["expire_days"], -1);

    // 旧 M4 行（rule_id NULL + expire_days 哨兵）→ 只读合成 rule-1，expiration 兼容
    db.set_lifecycle_rules(
        "bkt",
        &[oss_server::db::LifecycleRuleRow {
            bucket: "bkt".into(),
            rule_id: None,
            prefix: "old/".into(),
            expire_days: 45,
            expiration_days: None,
            transition_days: None,
            transition_class: None,
            transition_tier: None,
            enabled: true,
            updated_at: 0,
        }],
        0,
    )
    .await
    .unwrap();
    let listed: Vec<serde_json::Value> = http
        .get(format!("{base}/api/v1/buckets/bkt/lifecycle"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["rule_id"], "rule-1");
    assert_eq!(listed[0]["expire_days"], 45);
    assert_eq!(listed[0]["expiration_days"], 45);
}

/// M4 worker 只处理过期规则；Transition-only 规则必须跳过。
#[tokio::test]
async fn sweep_once_skips_transition_only_rule() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    st.store
        .put_object("bkt", "a.txt", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    let row = |expire_days: i64, expiration_days: Option<i64>, transition: bool| {
        oss_server::db::LifecycleRuleRow {
            bucket: "bkt".into(),
            rule_id: None,
            prefix: String::new(),
            expire_days,
            expiration_days,
            transition_days: transition.then_some(0),
            transition_class: transition.then(|| "STANDARD_IA".to_string()),
            transition_tier: None,
            enabled: true,
            updated_at: 0,
        }
    };
    st.db
        .set_lifecycle_rules("bkt", &[row(-1, None, true)], 1)
        .await
        .unwrap();
    assert_eq!(oss_server::lifecycle::sweep_once(&st).await, 0);
    assert!(st.store.get_object("bkt", "a.txt").await.is_ok());

    // 旧 M4 行（expiration_days NULL、expire_days>=0）仍执行
    st.db
        .set_lifecycle_rules("bkt", &[row(0, None, false)], 2)
        .await
        .unwrap();
    assert_eq!(oss_server::lifecycle::sweep_once(&st).await, 1);
    assert!(matches!(
        st.store.get_object("bkt", "a.txt").await,
        Err(xl_store::error::Error::NoSuchKey(_))
    ));
}

/// Transition sweep：到期才入队、replication PENDING 跳过、dedupe 键不重复、payload 冻结 class。
#[tokio::test]
async fn sweep_transitions_enqueues_and_dedupes() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    let a = st
        .store
        .put_object("bkt", "a.txt", bytes::Bytes::from_static(b"a"), None)
        .await
        .unwrap();
    st.store
        .put_object("bkt", "skip.txt", bytes::Bytes::from_static(b"b"), None)
        .await
        .unwrap();
    // 复制 PENDING 的版本不参与本轮（下轮重试）
    st.store
        .set_replication_status("bkt", "skip.txt", "null", Some("PENDING"))
        .await
        .unwrap();

    st.lifecycle
        .put_rules(
            "bkt",
            &[LifecycleRuleSpec {
                rule_id: Some("t1".into()),
                prefix: String::new(),
                expiration_days: None,
                transition_days: Some(0),
                transition_class: Some("STANDARD_IA".into()),
                transition_tier: None,
                enabled: true,
            }],
        )
        .await
        .unwrap();

    let queued = oss_server::tier_jobs::sweep_transitions(&st.db, &st.store, st.lifecycle.as_ref())
        .await
        .unwrap();
    assert_eq!(queued, 1, "只入队 a.txt（skip.txt 复制 PENDING）");
    let again = oss_server::tier_jobs::sweep_transitions(&st.db, &st.store, st.lifecycle.as_ref())
        .await
        .unwrap();
    assert_eq!(again, 0, "重复 sweep 被 dedupe 键吸收");

    let jobs = st.db.list_jobs(10).await.unwrap();
    assert_eq!(jobs.len(), 1);
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT action, payload_json, dedupe_key FROM job_tasks ORDER BY job_id, task_no",
    )
    .fetch_all(st.db.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "Transition");
    assert_eq!(
        rows[0].2.as_deref(),
        Some("tier:bkt:a.txt:null:STANDARD_IA")
    );
    let payload: serde_json::Value = serde_json::from_str(&rows[0].1).unwrap();
    assert_eq!(payload["class"], "STANDARD_IA");
    assert_eq!(payload["version_id"], a.version_id.unwrap());

    // C7：任务终态清空 dedupe_key，同键可再次入队。
    let job_id = jobs[0]["id"].as_i64().unwrap();
    sqlx::query("UPDATE job_tasks SET state = 'running' WHERE job_id = ? AND task_no = 1")
        .bind(job_id)
        .execute(st.db.pool())
        .await
        .unwrap();
    st.db
        .finish_task(job_id, 1, true, "ok", None, 0, 3)
        .await
        .unwrap();
    let key: Option<String> =
        sqlx::query_scalar("SELECT dedupe_key FROM job_tasks WHERE job_id = ? AND task_no = 1")
            .bind(job_id)
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert!(key.is_none(), "终态必须清空 dedupe_key");
    let requeued =
        oss_server::tier_jobs::sweep_transitions(&st.db, &st.store, st.lifecycle.as_ref())
            .await
            .unwrap();
    assert_eq!(requeued, 1, "清空后同键可再次入队");

    // 删桶清理规则（S0 共用清理）
    st.db.clear_bucket_m8("bkt").await.unwrap();
    assert!(st.db.list_lifecycle_rules("bkt").await.unwrap().is_empty());
}

/// admin `POST /api/v1/tiers/sweep` 强制跑一轮。
#[tokio::test]
async fn force_tier_sweep_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    st.store
        .put_object("bkt", "a.txt", bytes::Bytes::from_static(b"a"), None)
        .await
        .unwrap();
    st.lifecycle
        .put_rules(
            "bkt",
            &[LifecycleRuleSpec {
                rule_id: Some("t1".into()),
                prefix: String::new(),
                expiration_days: None,
                transition_days: Some(0),
                transition_class: Some("STANDARD_IA".into()),
                transition_tier: None,
                enabled: true,
            }],
        )
        .await
        .unwrap();
    let base = serve(oss_server::router::build_router(st).unwrap()).await;
    let token = login(&base).await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/tiers/sweep"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["queued"], 1, "{v}");
}
