//! M8 B10：三条桶删除路径的 M8 状态清理 + AP 缓存失效验收。
//!
//! ① xl-s3 `OssS3::delete_bucket`（经 AppState.bucket_cleanup 装配）
//! ② 控制台 `DELETE /api/v1/buckets/{bucket}`
//! ③ 站点 changelog `apply_bucket` 的 delete（事务内清表，commit 后才失效 cache）
//!
//! 原 ④（SQLite 延迟外键注入 commit 失败）随 SQLite 后端删除；对象根 CAS
//! 失败回滚/缓存失效由 `control_store_obj` 单元测试覆盖。

mod common;

use std::sync::{Arc, Mutex};

use common::state;
use oss_server::access_points_api::DbApSource;
use oss_server::router::build_router;
use oss_server::site::{Entry, apply_changelog};
use oss_server::state::AppState;
use s3s::dto::DeleteBucketInput;
use s3s::{S3, S3Request};
use xl_s3::ap::{AccessPoint, ApSource};
use xl_s3::bucket_cleanup::BucketCleanupSink;
use xl_s3::service::OssS3;
use xl_store::control_store::inventory::{InventoryConfigEntry, InventoryRunRow};
use xl_store::control_store::lifecycle::LifecycleRuleEntry;
use xl_store::control_store::tier::TierRestoreRow;
use xl_store::control_store::{AccessPointEntry, OlapConfigEntry, OwnerRef};

const REGION: &str = "us-east-1";
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
    let b = bucket.to_string();
    let a = ap.to_string();
    st.db
        .control_write("bucket-cleanup-seed", move |txn| {
            let (b, a) = (b.clone(), a.clone());
            Box::pin(async move {
                txn.access_point_create(&AccessPointEntry {
                    name: a.clone(),
                    bucket: b.clone(),
                    owner: OwnerRef::Admin,
                    policy_json: None,
                    created_at: 1,
                    updated_at: 1,
                })
                .await?;
                txn.olap_config_upsert(&OlapConfigEntry {
                    access_point: a,
                    kind: "http".into(),
                    endpoint: Some("http://old".into()),
                    module: None,
                    module_sha256: None,
                    timeout_ms: 5000,
                    max_response_bytes: 1024,
                    updated_at: 1,
                })
                .await?;
                txn.inventory_config_upsert(&InventoryConfigEntry {
                    bucket: b.clone(),
                    id: "daily".into(),
                    owner: OwnerRef::Admin,
                    config_json: "{}".into(),
                    updated_at: 1,
                })
                .await?;
                txn.inventory_run_upsert(&InventoryRunRow {
                    id: 1,
                    bucket: b.clone(),
                    config_id: "daily".into(),
                    run_key: "k".into(),
                    started_at: 1,
                    state: "done".into(),
                    job_id: None,
                    manifest_key: None,
                    error: None,
                    created_at: 1,
                    fence_term: 0,
                    fence_leader: 0,
                })
                .await?;
                txn.tier_restore_upsert(&TierRestoreRow {
                    bucket: b.clone(),
                    key: "k".into(),
                    version_id: "v".into(),
                    expires_at: 0,
                    job_id: None,
                    state: "restored".into(),
                    created_at: 1,
                    fence_term: 0,
                    fence_leader: 0,
                })
                .await?;
                txn.replace_lifecycle_rules(
                    &b,
                    &[LifecycleRuleEntry {
                        bucket: b.clone(),
                        prefix: String::new(),
                        rule_id: None,
                        expire_days: 1,
                        expiration_days: None,
                        transition_days: None,
                        transition_class: None,
                        transition_tier: None,
                        enabled: true,
                        updated_at: 1,
                        config_json: None,
                    }],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    st.ap_cache.put(
        AccessPoint::from_row(
            ap.into(),
            bucket.into(),
            None,
            REGION,
            xl_s3::access::LEGACY_ACCOUNT,
        )
        .unwrap(),
    );
}

/// 该桶的 M8 行与 AP 缓存必须已清空。
async fn assert_cleared(st: &AppState, bucket: &str, ap: &str) {
    let cs = st.db.control_store();
    assert!(
        cs.access_point(ap).await.unwrap().is_none(),
        "access_points 未清（ap={ap}）"
    );
    assert!(
        cs.olap_config(ap).await.unwrap().is_none(),
        "olap_configs 未清（ap={ap}）"
    );
    assert!(
        cs.inventory_config(bucket, "daily")
            .await
            .unwrap()
            .is_none(),
        "inventory_configs 未清（bucket={bucket}）"
    );
    assert!(
        cs.inventory_run_by_key(bucket, "daily", "k")
            .await
            .unwrap()
            .is_none(),
        "inventory_runs 未清（bucket={bucket}）"
    );
    assert!(
        cs.tier_restore(bucket, "k", "v").await.unwrap().is_none(),
        "tier_restores 未清（bucket={bucket}）"
    );
    assert!(
        cs.lifecycle_rules(bucket).await.unwrap().is_empty(),
        "lifecycle_rules 未清（bucket={bucket}）"
    );
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
    let inv = st
        .db
        .control_store()
        .inventory_config_count(bucket)
        .await
        .unwrap();
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
