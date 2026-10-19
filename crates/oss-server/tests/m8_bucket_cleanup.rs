//! M8 B10：四条桶删除路径的 M8 状态清理 + AP 缓存失效验收。
//!
//! ① xl-s3 `OssS3::delete_bucket`（经 AppState.bucket_cleanup 装配）
//! ② 控制台 `DELETE /api/v1/buckets/{bucket}`
//! ③ 站点 changelog `apply_bucket` 的 delete（事务内清表，commit 后才失效 cache）
//! ④ 集群控制面 `BucketRowSet { set: None }`

mod common;

use std::sync::{Arc, Mutex};

use common::state;
use oss_server::access_points_api::DbApSource;
use oss_server::cluster_control::SqliteControlBackend;
use oss_server::router::build_router;
use oss_server::site::{Entry, apply_changelog};
use oss_server::state::AppState;
use s3s::dto::DeleteBucketInput;
use s3s::{S3, S3Request};
use xl_s3::ap::{AccessPoint, ApSource};
use xl_s3::bucket_cleanup::BucketCleanupSink;
use xl_s3::service::OssS3;
use xl_store::mesh::{ControlBackend, ControlWrite};

const REGION: &str = "us-east-1";
/// 五张按 bucket 列的 M8 表（olap_configs 按 access_point，单独断言）。
const M8_TABLES: [&str; 5] = [
    "access_points",
    "inventory_configs",
    "inventory_runs",
    "tier_restores",
    "lifecycle_rules",
];

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// 播种：物理桶 + 桶行 + 全量 M8 行（AP/OLAP/Inventory/Tier/Lifecycle）+ 预热 AP 缓存。
async fn seed(st: &AppState, bucket: &str, ap: &str) {
    st.store.create_bucket(bucket).await.unwrap();
    st.db.insert_bucket(bucket, None).await.unwrap();
    // 值均为测试常量（桶名/AP 名皆为字面量），审计后断言 SQL 安全。
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "INSERT INTO access_points (name,bucket,owner_id,policy_json,created_at,updated_at) \
           VALUES ('{ap}','{bucket}',0,NULL,1,1);\
         INSERT INTO olap_configs (access_point,kind,endpoint,updated_at) \
           VALUES ('{ap}','http','http://old',1);\
         INSERT INTO inventory_configs (bucket,id,owner_id,config_json,updated_at) \
           VALUES ('{bucket}','daily',0,'{{}}',1);\
         INSERT INTO inventory_runs (bucket,config_id,run_key,started_at,state,created_at) \
           VALUES ('{bucket}','daily','k',1,'done',1);\
         INSERT INTO tier_restores (bucket,key,version_id,created_at) \
           VALUES ('{bucket}','k','v',1);\
         INSERT INTO lifecycle_rules (bucket,prefix,expire_days,enabled,updated_at) \
           VALUES ('{bucket}','',1,1,1);"
    )))
    .execute(st.db.pool())
    .await
    .unwrap();
    st.ap_cache
        .put(AccessPoint::from_row(ap.into(), bucket.into(), 0, None, REGION).unwrap());
}

async fn count(st: &AppState, sql: &str, binder: &str) -> i64 {
    // 调用方只传字面量 SQL（表名来自常量数组）。
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .bind(binder)
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

/// 该桶的 M8 表与 AP 缓存必须已清空。
async fn assert_cleared(st: &AppState, bucket: &str, ap: &str) {
    for table in M8_TABLES {
        let n = count(
            st,
            &format!("SELECT COUNT(*) FROM {table} WHERE bucket = ?"),
            bucket,
        )
        .await;
        assert_eq!(n, 0, "{table} 未清（bucket={bucket}）");
    }
    let olap = count(
        st,
        "SELECT COUNT(*) FROM olap_configs WHERE access_point = ?",
        ap,
    )
    .await;
    assert_eq!(olap, 0, "olap_configs 未清（ap={ap}）");
    assert!(st.ap_cache.get(ap).is_none(), "AP 缓存未失效（ap={ap}）");
}

/// 重建同名桶后不得继承旧 AP/Inventory 配置（AP 路由解析 = 404；Inventory 无行）。
async fn assert_not_inherited(st: &AppState, bucket: &str, ap: &str) {
    st.store.create_bucket(bucket).await.unwrap();
    st.db.insert_bucket(bucket, None).await.unwrap();
    let src = DbApSource {
        db: Arc::clone(&st.db),
        region: REGION.into(),
    };
    assert!(
        src.lookup(ap).await.unwrap().is_none(),
        "重建桶继承了旧 AP（路由应 404）"
    );
    let inv = count(
        st,
        "SELECT COUNT(*) FROM inventory_configs WHERE bucket = ?",
        bucket,
    )
    .await;
    assert_eq!(inv, 0, "重建桶继承了旧 Inventory 配置");
}

/// 清理失败桩：clear 恒失败、invalidate 照常记账（验证 best-effort 不影响删除结果）。
#[derive(Default)]
struct FailCleanup {
    invalidated: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl BucketCleanupSink for FailCleanup {
    async fn clear_bucket_m8(&self, _bucket: &str) -> Result<(), String> {
        Err("injected cleanup failure".into())
    }
    fn invalidate_ap_cache(&self, bucket: &str) {
        self.invalidated.lock().unwrap().push(bucket.into());
    }
}

fn delete_req(bucket: &str) -> S3Request<DeleteBucketInput> {
    S3Request {
        input: DeleteBucketInput {
            bucket: bucket.into(),
            ..Default::default()
        },
        method: reqwest::Method::DELETE,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// ① xl-s3 服务路径：物理删除后清 M8 表 + 失效缓存；清理失败不改变删除结果。
#[tokio::test]
async fn path_s3_service_clears_m8_and_cache() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    seed(&st, "s3bkt", "s3-ap").await;
    let oss = OssS3::new(Arc::clone(&st.store), st.acl(), 1 << 20)
        .with_bucket_cleanup(Some(Arc::clone(&st.bucket_cleanup)));
    oss.delete_bucket(delete_req("s3bkt")).await.unwrap();
    assert!(!st.store.bucket_exists("s3bkt").await.unwrap());
    assert_cleared(&st, "s3bkt", "s3-ap").await;
    assert_not_inherited(&st, "s3bkt", "s3-ap").await;

    // 清理失败：删除仍成功，缓存照常失效。
    st.store.create_bucket("failbkt").await.unwrap();
    let sink = Arc::new(FailCleanup::default());
    let oss = OssS3::new(Arc::clone(&st.store), st.acl(), 1 << 20)
        .with_bucket_cleanup(Some(sink.clone() as Arc<dyn BucketCleanupSink>));
    oss.delete_bucket(delete_req("failbkt")).await.unwrap();
    assert!(!st.store.bucket_exists("failbkt").await.unwrap());
    assert_eq!(*sink.invalidated.lock().unwrap(), ["failbkt".to_string()]);
}

/// ② 控制台删桶：清 M8 表 + 失效缓存。
#[tokio::test]
async fn path_console_delete_clears_m8_and_cache() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    seed(&st, "conbkt", "con-ap").await;
    let app = build_router(st.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");

    let login: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = login["token"].as_str().unwrap();
    let resp = reqwest::Client::new()
        .delete(format!("{base}/api/v1/buckets/conbkt"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert!(!st.store.bucket_exists("conbkt").await.unwrap());
    assert_cleared(&st, "conbkt", "con-ap").await;
    assert_not_inherited(&st, "conbkt", "con-ap").await;
}

/// ③ 站点 apply delete：事务内清表，commit 成功后失效缓存。
#[tokio::test]
async fn path_site_apply_clears_m8_and_invalidates_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    seed(&st, "sitebkt", "site-ap").await;
    let entry = Entry {
        seq: 1,
        origin_site_id: "peer-site".into(),
        entity: "bucket".into(),
        entity_id: "sitebkt".into(),
        op: "delete".into(),
        payload: serde_json::json!({}),
        priority: 0,
        ts: now(),
    };
    apply_changelog(&st, vec![entry]).await.unwrap();
    assert!(!st.store.bucket_exists("sitebkt").await.unwrap());
    assert_cleared(&st, "sitebkt", "site-ap").await;
    assert_not_inherited(&st, "sitebkt", "site-ap").await;
}

/// ③ commit 失败：cache 不得被失效（回滚后 M8 行仍在，预热缓存仍有效）。
#[tokio::test]
async fn path_site_apply_commit_failure_keeps_cache() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    seed(&st, "sitefail", "sitefail-ap").await;
    // 延迟到 COMMIT 才校验的外键：AFTER DELETE 触发器插入引用已删桶的行。
    sqlx::raw_sql(
        "CREATE TABLE commit_guard (\
           name TEXT REFERENCES buckets(name) DEFERRABLE INITIALLY DEFERRED);\
         CREATE TRIGGER commit_guard_trg AFTER DELETE ON buckets BEGIN \
           INSERT INTO commit_guard (name) VALUES (OLD.name); END;",
    )
    .execute(st.db.pool())
    .await
    .unwrap();
    let entry = Entry {
        seq: 2,
        origin_site_id: "peer-site".into(),
        entity: "bucket".into(),
        entity_id: "sitefail".into(),
        op: "delete".into(),
        payload: serde_json::json!({}),
        priority: 0,
        ts: now(),
    };
    let err = apply_changelog(&st, vec![entry]).await.unwrap_err();
    assert!(err.contains("FOREIGN KEY"), "应因 commit 期外键失败：{err}");
    assert!(
        st.ap_cache.get("sitefail-ap").is_some(),
        "commit 失败不得失效 AP 缓存"
    );
    // 事务回滚：AP 行仍在（clear 未提交）。
    assert!(
        st.db
            .get_access_point("sitefail-ap")
            .await
            .unwrap()
            .is_some()
    );
}

/// ④ 集群控制面删桶行：注入清理时清表 + 失效缓存；`new(pool)` 无 sink 仍可删行。
#[tokio::test]
async fn path_cluster_control_clears_m8_and_cache() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    seed(&st, "clbkt", "cl-ap").await;
    let backend = SqliteControlBackend::new(st.db.pool().clone())
        .with_bucket_cleanup(Some(Arc::clone(&st.bucket_cleanup)));
    backend
        .control_write(ControlWrite::BucketRowSet {
            bucket: "clbkt".into(),
            set: None,
        })
        .await
        .unwrap();
    let rows = count(&st, "SELECT COUNT(*) FROM buckets WHERE name = ?", "clbkt").await;
    assert_eq!(rows, 0, "桶行应已删除");
    assert_cleared(&st, "clbkt", "cl-ap").await;

    // 既有 `new(pool)` 无 sink：删行照常成功（清理为可选注入）。
    st.db.insert_bucket("nobkt", None).await.unwrap();
    SqliteControlBackend::new(st.db.pool().clone())
        .control_write(ControlWrite::BucketRowSet {
            bucket: "nobkt".into(),
            set: None,
        })
        .await
        .unwrap();
    let rows = count(&st, "SELECT COUNT(*) FROM buckets WHERE name = ?", "nobkt").await;
    assert_eq!(rows, 0);
}

/// ④ 清理失败：只告警，控制面写仍返回成功，缓存照常失效。
#[tokio::test]
async fn path_cluster_control_cleanup_failure_keeps_delete_result() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.insert_bucket("clfail", None).await.unwrap();
    let sink = Arc::new(FailCleanup::default());
    let backend = SqliteControlBackend::new(st.db.pool().clone())
        .with_bucket_cleanup(Some(sink.clone() as Arc<dyn BucketCleanupSink>));
    backend
        .control_write(ControlWrite::BucketRowSet {
            bucket: "clfail".into(),
            set: None,
        })
        .await
        .unwrap();
    let rows = count(&st, "SELECT COUNT(*) FROM buckets WHERE name = ?", "clfail").await;
    assert_eq!(rows, 0, "清理失败不得回滚/改写删行结果");
    assert_eq!(*sink.invalidated.lock().unwrap(), ["clfail".to_string()]);
}
