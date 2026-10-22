//! M4 切片 6：生命周期到期删除 + jobs 进度（验收 ③ 的删除面；转换/分层归 M8）。

mod common;

use common::state;
use oss_server::db::LifecycleRuleRow;
use oss_server::lifecycle::{sweep_once, sweep_once_at};
use xl_store::store::VersioningMode;

fn rule(prefix: &str, days: i64) -> LifecycleRuleRow {
    LifecycleRuleRow {
        bucket: "bkt".into(),
        rule_id: None,
        prefix: prefix.into(),
        expire_days: days,
        expiration_days: None,
        transition_days: None,
        transition_class: None,
        transition_tier: None,
        enabled: true,
        updated_at: 0,
        config_json: None,
    }
}

/// 到期对象被删、未到期与 prefix 域外不动；jobs 记录 scanned/deleted 与 done。
#[tokio::test]
async fn lifecycle_expires_and_records_job() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    let body = bytes::Bytes::from_static(b"x");
    st.store
        .put_object("bkt", "old.txt", body.clone(), None)
        .await
        .unwrap();
    st.store
        .put_object("bkt", "keep/other.txt", body.clone(), None)
        .await
        .unwrap();

    // prefix=old 且立即到期 → 只删 old.txt
    st.db
        .set_lifecycle_rules("bkt", &[rule("old", 0)], 1)
        .await
        .unwrap();
    let ran = sweep_once(&st).await;
    assert_eq!(ran, 1);
    assert!(
        matches!(
            st.store.get_object("bkt", "old.txt").await,
            Err(xl_store::error::Error::NoSuchKey(_))
        ),
        "到期对象应被删"
    );
    assert!(
        st.store.get_object("bkt", "keep/other.txt").await.is_ok(),
        "prefix 域外不动"
    );
    let jobs = st.db.list_jobs(10).await.unwrap();
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    assert_eq!(jobs[0]["state"], "done");
    assert_eq!(jobs[0]["deleted"], 1);
    assert_eq!(jobs[0]["scanned"], 1);

    // 未到期（days=1）：再放一个不删
    st.db
        .set_lifecycle_rules("bkt", &[rule("", 1)], 2)
        .await
        .unwrap();
    st.store
        .put_object("bkt", "fresh.txt", body.clone(), None)
        .await
        .unwrap();
    sweep_once(&st).await;
    assert!(
        st.store.get_object("bkt", "fresh.txt").await.is_ok(),
        "未到期不动"
    );
    let jobs = st.db.list_jobs(10).await.unwrap();
    assert_eq!(jobs[0]["state"], "done");
    assert_eq!(jobs[0]["deleted"], 0);
    // prefix="" 全桶：存活对象 = keep/other.txt + fresh.txt
    assert_eq!(jobs[0]["scanned"], 2);
}

/// jobs 的 error 路径（verifier 覆盖缺口补）：直写负值规则（绕过 console 校验）→
/// worker fail-closed 拒绝执行并记 state=error。
#[tokio::test]
async fn negative_expire_days_rule_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("nbk").await.unwrap();
    st.db.insert_bucket("nbk", None).await.unwrap();
    st.store
        .put_object("nbk", "keep.txt", bytes::Bytes::from_static(b"k"), None)
        .await
        .unwrap();
    // M8 S4b 后负值只可能出自直接写 `expiration_days`（旧 `expire_days=-1` 是
    // Transition-only 哨兵，按无过期跳过）。
    let mut bad = rule("", -1);
    bad.bucket = "nbk".into();
    bad.expiration_days = Some(-1);
    st.db.set_lifecycle_rules("nbk", &[bad], 1).await.unwrap();

    sweep_once(&st).await;
    let jobs = st.db.list_jobs(10).await.unwrap();
    assert_eq!(jobs[0]["state"], "error", "{jobs:?}");
    assert!(
        jobs[0]["error"].as_str().unwrap_or("").contains("负"),
        "{jobs:?}"
    );
    assert!(
        st.store.get_object("nbk", "keep.txt").await.is_ok(),
        "负值规则必须 fail-closed，不得变相全删"
    );
}

/// 版本控制桶：到期删除 = delete marker（不是物理删；版本列表可见墓碑）。
#[tokio::test]
async fn lifecycle_versioned_bucket_marks_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("vbk").await.unwrap();
    st.db.insert_bucket("vbk", None).await.unwrap();
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = 'vbk'")
        .execute(st.db.pool())
        .await
        .unwrap();
    st.store
        .put_object("vbk", "v.txt", bytes::Bytes::from_static(b"v"), None)
        .await
        .unwrap();
    let mut r = rule("", 0);
    r.bucket = "vbk".into();
    st.db.set_lifecycle_rules("vbk", &[r], 1).await.unwrap();

    sweep_once(&st).await;
    assert!(
        matches!(
            st.store.get_object("vbk", "v.txt").await,
            Err(xl_store::error::Error::NoSuchKey(_))
        ),
        "marker 遮蔽后普通读 404"
    );
    let vl = st
        .store
        .list_object_versions("vbk", "", 100, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries.len(), 2, "原版本 + marker：{vl:?}");
    assert!(vl.entries[0].is_marker && vl.entries[0].is_latest);
}

/// M6 切片 4（V3②）：生命周期删除（marker）入复制队列。
#[tokio::test]
async fn lifecycle_delete_enqueues_replication() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("lrb").await.unwrap();
    st.db.insert_bucket("lrb", None).await.unwrap();
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = 'lrb'")
        .execute(st.db.pool())
        .await
        .unwrap();
    st.store
        .put_object("lrb", "old.txt", bytes::Bytes::from_static(b"v"), None)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO replication_rules (bucket, rules_json) VALUES ('lrb', ?) \
         ON CONFLICT(bucket) DO UPDATE SET rules_json=excluded.rules_json",
    )
    .bind(r#"[{"id":"r","prefix":null,"remote_id":"r1","dest_bucket":"dst","delete_marker":true,"enabled":true}]"#)
    .execute(st.db.pool())
    .await
    .unwrap();
    let mut r = rule("", 0);
    r.bucket = "lrb".into();
    st.db.set_lifecycle_rules("lrb", &[r], 1).await.unwrap();

    sweep_once(&st).await;
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE bucket='lrb' AND is_delete_marker=1 \
         AND version_id != ''",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(n, 1, "生命周期 marker 应带版本号入复制队列");
}

// ——— S6-1/S6-3：走真控制面路由的回归 ———

use oss_server::router::build_router;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn admin_env(dir: &std::path::Path) -> (oss_server::state::AppState, String) {
    let st = state(dir).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let login: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (st, format!("{base}|{}", login["token"].as_str().unwrap()))
}

/// S6-1：删桶清规则——同名桶重建后旧规则不得复活误删新对象。
#[tokio::test]
async fn delete_bucket_clears_lifecycle_rules() {
    let dir = tempfile::tempdir().unwrap();
    let (st, base_tok) = admin_env(dir.path()).await;
    let (base, tok) = base_tok.split_once('|').unwrap();
    let client = reqwest::Client::new();

    st.store.create_bucket("rbk").await.unwrap();
    st.db.insert_bucket("rbk", None).await.unwrap();
    st.db
        .set_lifecycle_rules("rbk", &[rule("", 0)], 1)
        .await
        .unwrap();
    assert_eq!(st.db.list_lifecycle_rules("rbk").await.unwrap().len(), 1);

    // 走控制面删桶（会连带清理规则）
    let resp = client
        .delete(format!("{base}/api/v1/buckets/rbk"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert!(
        st.db.list_lifecycle_rules("rbk").await.unwrap().is_empty(),
        "删桶必须清规则（S6-1）"
    );

    // 重建同名桶 + 放对象 + sweep：不得被旧规则（expire 0）删
    st.store.create_bucket("rbk").await.unwrap();
    st.db.insert_bucket("rbk", None).await.unwrap();
    st.store
        .put_object("rbk", "new.txt", bytes::Bytes::from_static(b"n"), None)
        .await
        .unwrap();
    sweep_once(&st).await;
    assert!(
        st.store.get_object("rbk", "new.txt").await.is_ok(),
        "旧规则复活会误删新对象（S6-1 回归）"
    );
}

/// P1：同 prefix 多条规则合法（AWS 允许；PK 已改为 (bucket,rule_id)）。
#[tokio::test]
async fn duplicate_prefix_allowed_204() {
    let dir = tempfile::tempdir().unwrap();
    let (st, base_tok) = admin_env(dir.path()).await;
    let (base, tok) = base_tok.split_once('|').unwrap();
    st.store.create_bucket("dbk").await.unwrap();
    st.db.insert_bucket("dbk", None).await.unwrap();

    let resp = reqwest::Client::new()
        .put(format!("{base}/api/v1/buckets/dbk/lifecycle"))
        .bearer_auth(tok)
        .json(&serde_json::json!([
            {"prefix": "a", "expire_days": 1},
            {"prefix": "a", "expire_days": 2, "rule_id": "second"}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let rows = st.db.list_lifecycle_rules("dbk").await.unwrap();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().any(|r| r.rule_id.as_deref() == Some("second")));
}

/// M6 切片 0（R5/P2-4）：生命周期删除发 LifecycleExpiration 事件（非版本 Delete /
/// 版本桶 MarkerCreated 两型各一）。
#[tokio::test]
async fn lifecycle_emits_expiration_events() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;

    // 非版本桶：ObjectRemoved 型 Delete
    st.store.create_bucket("lpa").await.unwrap();
    st.db.insert_bucket("lpa", None).await.unwrap();
    sqlx::query("INSERT INTO event_configs (bucket, config_json) VALUES ('lpa', ?)")
        .bind(
            r#"[{"id":"a","target":"t1","events":["s3:LifecycleExpiration:Delete"],"prefix":null,"suffix":null}]"#,
        )
        .execute(st.db.pool())
        .await
        .unwrap();
    st.store
        .put_object("lpa", "gone.txt", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    st.db
        .set_lifecycle_rules("lpa", &[rule("", 0)], 1)
        .await
        .unwrap();

    // 版本桶：打 marker → MarkerCreated 型
    st.store.create_bucket("lpb").await.unwrap();
    st.db.insert_bucket("lpb", None).await.unwrap();
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = 'lpb'")
        .execute(st.db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO event_configs (bucket, config_json) VALUES ('lpb', ?)")
        .bind(
            r#"[{"id":"b","target":"t2","events":["s3:LifecycleExpiration:DeleteMarkerCreated"],"prefix":null,"suffix":null}]"#,
        )
        .execute(st.db.pool())
        .await
        .unwrap();
    // 版本桶写入：with_meta 的 versioned=true 追加版本
    st.store
        .put_object_stream_with_meta(
            "lpb",
            "vm.txt",
            Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from_static(b"y"))
            })),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    st.db
        .set_lifecycle_rules("lpb", &[rule("", 0)], 1)
        .await
        .unwrap();

    sweep_once(&st).await;

    let names: Vec<(String, String)> =
        sqlx::query_as("SELECT target_id, event_json FROM event_outbox ORDER BY id")
            .fetch_all(st.db.pool())
            .await
            .unwrap();
    assert_eq!(names.len(), 2, "{names:?}");
    let n0: serde_json::Value = serde_json::from_str(&names[0].1).unwrap();
    assert_eq!(n0["eventName"], "s3:LifecycleExpiration:Delete");
    assert_eq!(n0["source"]["userAgent"], "oss-lifecycle");
    let n1: serde_json::Value = serde_json::from_str(&names[1].1).unwrap();
    assert_eq!(
        n1["eventName"],
        "s3:LifecycleExpiration:DeleteMarkerCreated"
    );
}

// ——— P1/P2：结构化过滤、Date、EODM、noncurrent ———

use oss_server::state::AppState;
use xl_s3::lifecycle::{
    ExpirationAction, LifecycleFilter, LifecycleRuleSpec, NoncurrentExpiration,
};
use xl_store::meta::ObjectTag;

fn blank_spec() -> LifecycleRuleSpec {
    LifecycleRuleSpec {
        rule_id: None,
        filter: LifecycleFilter::default(),
        expiration: None,
        transitions: vec![],
        noncurrent_expiration: None,
        noncurrent_transitions: vec![],
        enabled: true,
    }
}

async fn versioned_bucket(st: &AppState, bucket: &str) {
    st.store.create_bucket(bucket).await.unwrap();
    st.db.insert_bucket(bucket, None).await.unwrap();
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = ?")
        .bind(bucket)
        .execute(st.db.pool())
        .await
        .unwrap();
}

fn stream(b: &'static [u8]) -> xl_store::store::ByteStream {
    Box::pin(futures::stream::once(async move {
        Ok(bytes::Bytes::from_static(b))
    }))
}

async fn put_version(st: &AppState, bucket: &str, key: &str, body: &'static [u8]) -> String {
    st.store
        .put_object_stream_with_meta(
            bucket,
            key,
            stream(body),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .version_id
        .unwrap()
}

/// Tag + Size 过滤：只有同时满足前缀/标签/大小的对象被删；配置落 config_json。
#[tokio::test]
async fn structured_tag_and_size_filter() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("fbk").await.unwrap();
    st.db.insert_bucket("fbk", None).await.unwrap();
    let put = |key: &'static str, body: &'static [u8], tags: Option<Vec<ObjectTag>>| {
        let st = st.clone();
        async move {
            st.store
                .put_object_stream_with_meta(
                    "fbk",
                    key,
                    stream(body),
                    None,
                    Default::default(),
                    None,
                    VersioningMode::Off,
                    None,
                    None,
                    tags,
                    None,
                )
                .await
                .unwrap();
        }
    };
    let prod = || {
        Some(vec![ObjectTag {
            key: "env".into(),
            value: "prod".into(),
        }])
    };
    put("logs/a.txt", b"hello", prod()).await;
    put("logs/empty.txt", b"", prod()).await;
    put("logs/dev.txt", b"hello", None).await;
    put("other/c.txt", b"hello", prod()).await;

    let mut spec = blank_spec();
    spec.filter = LifecycleFilter {
        prefix: "logs/".into(),
        tags: prod().unwrap(),
        size_gt: Some(0),
        size_lt: None,
    };
    spec.expiration = Some(ExpirationAction::Days { days: 0 });
    st.lifecycle.put_rules("fbk", &[spec]).await.unwrap();
    assert!(
        st.db.list_lifecycle_rules("fbk").await.unwrap()[0]
            .config_json
            .is_some(),
        "Tag/Size 规则必须落结构化 blob"
    );

    assert_eq!(sweep_once(&st).await, 1);
    assert!(matches!(
        st.store.get_object("fbk", "logs/a.txt").await,
        Err(xl_store::error::Error::NoSuchKey(_))
    ));
    for kept in ["logs/empty.txt", "logs/dev.txt", "other/c.txt"] {
        assert!(
            st.store.get_object("fbk", kept).await.is_ok(),
            "{kept} 不应被删"
        );
    }
}

/// Expiration.Date：执行日后清掉当日之前已存在的对象（用注入时刻模拟日期到达）。
#[tokio::test]
async fn expiration_date_deletes_objects_existing_before_date() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("dbk").await.unwrap();
    st.db.insert_bucket("dbk", None).await.unwrap();
    st.store
        .put_object("dbk", "old.txt", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let date = (now / 86_400 + 1) * 86_400;
    let mut spec = blank_spec();
    spec.expiration = Some(ExpirationAction::Date { date });
    st.lifecycle.put_rules("dbk", &[spec]).await.unwrap();

    // 未到执行日：不动
    assert_eq!(sweep_once(&st).await, 1);
    assert!(st.store.get_object("dbk", "old.txt").await.is_ok());
    // 执行日之后：删除
    sweep_once_at(&st, date + 3600).await;
    assert!(matches!(
        st.store.get_object("dbk", "old.txt").await,
        Err(xl_store::error::Error::NoSuchKey(_))
    ));
}

/// EODM：唯一版本且为 delete marker 的墓碑被清除。
#[tokio::test]
async fn eodm_removes_sole_delete_marker() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    versioned_bucket(&st, "ebk").await;
    let out = st
        .store
        .delete_object_versioned("ebk", "gone.txt", None, VersioningMode::Enabled)
        .await
        .unwrap();
    assert!(matches!(out, xl_store::store::DeleteOutcome::Marker { .. }));
    let mut spec = blank_spec();
    spec.expiration = Some(ExpirationAction::ExpiredObjectDeleteMarker);
    st.lifecycle.put_rules("ebk", &[spec]).await.unwrap();

    assert_eq!(sweep_once(&st).await, 1);
    let vl = st
        .store
        .list_object_versions("ebk", "", 10, None, None)
        .await
        .unwrap();
    assert!(vl.entries.is_empty(), "墓碑应被删除：{vl:?}");
    let jobs = st.db.list_jobs(10).await.unwrap();
    assert_eq!(jobs[0]["deleted"], 1, "{jobs:?}");
}

/// noncurrent：NewerNoncurrentVersions 保留最新 N 个；链上前一条的创建时间是年龄时钟。
#[tokio::test]
async fn noncurrent_expiration_retains_newer_versions() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    versioned_bucket(&st, "nbk").await;
    let mut ids = Vec::new();
    for body in [b"v0".as_slice(), b"v1", b"v2", b"v3"] {
        ids.push(put_version(&st, "nbk", "k.txt", body).await);
    }
    let mut spec = blank_spec();
    spec.filter.prefix = "k".into();
    spec.noncurrent_expiration = Some(NoncurrentExpiration {
        days: 1,
        newer: Some(2),
    });
    st.lifecycle
        .put_rules("nbk", &[spec.clone()])
        .await
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let future = now + 2 * 86_400;
    sweep_once_at(&st, future).await;

    let vl = st
        .store
        .list_object_versions("nbk", "", 10, None, None)
        .await
        .unwrap();
    assert_eq!(
        vl.entries.len(),
        3,
        "保留当前 + 最新 2 个 noncurrent：{vl:?}"
    );
    assert!(!vl.entries.iter().any(|e| e.version_id == ids[0]));

    // 取消保留数后其余 noncurrent 全部过期，只剩当前版本
    spec.noncurrent_expiration = Some(NoncurrentExpiration {
        days: 1,
        newer: None,
    });
    st.lifecycle.put_rules("nbk", &[spec]).await.unwrap();
    sweep_once_at(&st, future).await;
    let vl = st
        .store
        .list_object_versions("nbk", "", 10, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries.len(), 1, "{vl:?}");
    assert_eq!(vl.entries[0].version_id, ids[3]);
}

/// noncurrent delete marker 同样可过期（AWS 语义），删后不遮蔽当前版本。
#[tokio::test]
async fn noncurrent_expiration_deletes_noncurrent_markers() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    versioned_bucket(&st, "mbk").await;
    put_version(&st, "mbk", "m.txt", b"v0").await;
    st.store
        .delete_object_versioned("mbk", "m.txt", None, VersioningMode::Enabled)
        .await
        .unwrap();
    let latest = put_version(&st, "mbk", "m.txt", b"v1").await;
    let mut spec = blank_spec();
    spec.filter.prefix = "m".into();
    spec.noncurrent_expiration = Some(NoncurrentExpiration {
        days: 1,
        newer: None,
    });
    st.lifecycle.put_rules("mbk", &[spec]).await.unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    sweep_once_at(&st, now + 2 * 86_400).await;

    let vl = st
        .store
        .list_object_versions("mbk", "", 10, None, None)
        .await
        .unwrap();
    assert_eq!(
        vl.entries.len(),
        1,
        "noncurrent marker + 旧版本都过期：{vl:?}"
    );
    assert_eq!(vl.entries[0].version_id, latest);
    assert!(st.store.get_object("mbk", "m.txt").await.is_ok());
}

/// 坏 config_json fail-closed：开单记 error，绝不按空规则执行删除。
#[tokio::test]
async fn corrupt_config_json_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("cbk").await.unwrap();
    st.db.insert_bucket("cbk", None).await.unwrap();
    st.store
        .put_object("cbk", "keep.txt", bytes::Bytes::from_static(b"k"), None)
        .await
        .unwrap();
    let mut bad = rule("", 0);
    bad.bucket = "cbk".into();
    bad.config_json = Some("{not json".into());
    st.db.set_lifecycle_rules("cbk", &[bad], 1).await.unwrap();

    assert_eq!(sweep_once(&st).await, 1);
    let jobs = st.db.list_jobs(10).await.unwrap();
    assert_eq!(jobs[0]["state"], "error", "{jobs:?}");
    assert!(
        jobs[0]["error"]
            .as_str()
            .unwrap_or("")
            .contains("config_json"),
        "{jobs:?}"
    );
    assert!(st.store.get_object("cbk", "keep.txt").await.is_ok());
}
