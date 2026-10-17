//! M4 切片 6：生命周期到期删除 + jobs 进度（验收 ③ 的删除面；转换/分层归 M8）。

mod common;

use common::state;
use oss_server::db::LifecycleRuleRow;
use oss_server::lifecycle::sweep_once;

fn rule(prefix: &str, days: i64) -> LifecycleRuleRow {
    LifecycleRuleRow {
        bucket: "bkt".into(),
        prefix: prefix.into(),
        expire_days: days,
        enabled: true,
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
    let mut bad = rule("", -1);
    bad.bucket = "nbk".into();
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

/// S6-3：重复 prefix → 400（PK(bucket,prefix) 撞库曾是 500）。
#[tokio::test]
async fn duplicate_prefix_rejected_400() {
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
            {"prefix": "a", "expire_days": 2}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    // 正常单条放行
    let resp = reqwest::Client::new()
        .put(format!("{base}/api/v1/buckets/dbk/lifecycle"))
        .bearer_auth(tok)
        .json(&serde_json::json!([{"prefix": "a", "expire_days": 1}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
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
            true,
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
