//! M8 S5：Inventory 配置 CRUD/权限、调度槽、生成 worker 的输出格式。
//!
//! 走真实 `AppState`（SQLite 内存库 + 本地盘），手动驱动 `due_runs`/`worker_once`，
//! 输出用真实 store 读回并解 gzip/CSV/JSON 核对。

mod common;

use std::sync::Arc;
use xl_store::store::VersioningMode;

use bytes::Bytes;
use common::state;
use oss_server::db::PermissionRow;
use oss_server::inventory::{InventoryPayload, daily_key, due_runs, enqueue_run, weekly_key};
use oss_server::jobs::{JobEnv, TaskExecutor as _, worker_once};
use oss_server::state::AppState;
use reqwest::StatusCode as HttpStatus;
use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::access::{AuthedUser, LEGACY_ACCOUNT, account_id_of};
use xl_s3::inventory::InventoryError;
use xl_s3::service::OssS3;
use xl_store::crypto::EncSpec;
use xl_store::meta::{ChecksumRecord, ObjectLockRecord};

const ENV: JobEnv = JobEnv {
    workers: 1,
    interval_secs: 1,
    max_attempts: 1,
    timeout_secs: 30,
    tier_timeout_secs: 30,
};

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

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

fn as_user<T>(mut r: S3Request<T>, uid: i64, is_admin: bool) -> S3Request<T> {
    r.extensions.insert(AuthedUser {
        uid,
        is_admin,
        ak: "test-ak".to_string(),
    });
    r
}

fn user(uid: i64, is_admin: bool) -> AuthedUser {
    AuthedUser {
        uid,
        is_admin,
        ak: "test-ak".to_string(),
    }
}

async fn boot(dir: &tempfile::TempDir) -> (AppState, i64) {
    let st = state(dir.path()).await;
    let uid = st.db.create_user("erik", "pw", true).await.unwrap();
    (st, uid)
}

async fn add_bucket(st: &AppState, name: &str, owner: Option<i64>) {
    st.store.create_bucket(name).await.unwrap();
    st.db.insert_bucket(name, owner).await.unwrap();
}

fn cfg(id: &str, freq: &str, dest: &str) -> InventoryConfiguration {
    InventoryConfiguration {
        destination: InventoryDestination {
            s3_bucket_destination: InventoryS3BucketDestination {
                account_id: None,
                bucket: dest.to_string(),
                encryption: None,
                format: InventoryFormat::from_static(InventoryFormat::CSV),
                prefix: None,
            },
        },
        filter: None,
        id: id.to_string(),
        included_object_versions: InventoryIncludedObjectVersions::from_static(
            InventoryIncludedObjectVersions::CURRENT,
        ),
        is_enabled: true,
        optional_fields: None,
        schedule: InventorySchedule {
            frequency: InventoryFrequency::from(freq.to_string()),
        },
    }
}

fn with_optional(mut c: InventoryConfiguration, fields: &[&str]) -> InventoryConfiguration {
    c.optional_fields = Some(
        fields
            .iter()
            .map(|f| InventoryOptionalField::from(f.to_string()))
            .collect(),
    );
    c
}

async fn put(
    st: &AppState,
    uid: i64,
    bucket: &str,
    c: &InventoryConfiguration,
) -> Result<(), InventoryError> {
    st.inventory.put_config(bucket, &user(uid, true), c).await
}

fn s3(st: &AppState) -> OssS3 {
    OssS3::new(Arc::clone(&st.store), st.acl(), st.cfg.max_put_bytes)
        .with_inventory(Some(Arc::clone(&st.inventory)))
}

async fn run_once(st: &AppState) {
    let got = worker_once(&st.db, &st.store, st.inventory_runner.as_ref(), ENV, now())
        .await
        .unwrap();
    assert!(got, "worker 应领到 inventory 任务");
}

/// 目标桶里某前缀下的全部对象键（升序）。
async fn keys_under(st: &AppState, bucket: &str, prefix: &str) -> Vec<String> {
    let mut keys: Vec<String> = st
        .store
        .list_objects(
            bucket,
            &xl_store::store::ListRequest {
                prefix: Some(prefix.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .objects
        .into_iter()
        .map(|o| o.key)
        .collect();
    keys.sort();
    keys
}

async fn run_row(
    st: &AppState,
    bucket: &str,
    id: &str,
    key: &str,
) -> oss_server::db::InventoryRunRow {
    st.db
        .inventory_run_by_key(bucket, id, key)
        .await
        .unwrap()
        .unwrap()
}

/// 读回一次成功运行的 manifest 与数据文件字节。
async fn output(
    st: &AppState,
    dest: &str,
    bucket: &str,
    id: &str,
    key: &str,
) -> (serde_json::Value, Vec<u8>) {
    let run = run_row(st, bucket, id, key).await;
    assert_eq!(run.state, "done", "run error: {:?}", run.error);
    let mkey = run.manifest_key.expect("manifest_key");
    let (_, mbytes) = st.store.get_object(dest, &mkey).await.unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&mbytes).unwrap();
    let dkey = manifest["files"][0]["key"].as_str().unwrap();
    let (_, data) = st.store.get_object(dest, dkey).await.unwrap();
    (manifest, data.to_vec())
}

fn gunzip_csv(bytes: &[u8]) -> Vec<Vec<String>> {
    use std::io::Read as _;
    assert_eq!(&bytes[..2], &[0x1f, 0x8b], "数据文件必须是 gzip");
    let mut plain = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut plain)
        .unwrap();
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(&plain[..])
        .records()
        .map(|r| {
            r.unwrap()
                .iter()
                .map(std::string::ToString::to_string)
                .collect()
        })
        .collect()
}

const MANDATORY: [&str; 9] = [
    "Bucket",
    "Key",
    "VersionId",
    "IsLatest",
    "DeleteMarker",
    "Size",
    "LastModifiedDate",
    "ETag",
    "StorageClass",
];

const OPTIONAL: [&str; 6] = [
    "ReplicationStatus",
    "EncryptionStatus",
    "ObjectLockRetainUntilDate",
    "ObjectLockMode",
    "ObjectLockLegalHoldStatus",
    "ChecksumAlgorithm",
];

// ---------- S3 配置面 ----------

#[tokio::test]
async fn s3_crud_and_access_gate() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;

    // 未装配 → 501；配置面本身不带 ACL（access 层判权），无身份扩展 → AccessDenied
    let bare = OssS3::new(Arc::clone(&st.store), st.acl(), st.cfg.max_put_bytes);
    let err = bare
        .get_bucket_inventory_configuration(req(GetBucketInventoryConfigurationInput {
            bucket: "src".into(),
            id: "c1".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NotImplemented");

    let svc = s3(&st);
    let c = cfg("c1", "Daily", "dst");
    let err = svc
        .put_bucket_inventory_configuration(req(PutBucketInventoryConfigurationInput {
            bucket: "src".into(),
            id: "c1".into(),
            inventory_configuration: c.clone(),
            expected_bucket_owner: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "AccessDenied");

    // URL id 与 body id 必须一致
    let mut mismatched = c.clone();
    mismatched.id = "c2".to_string();
    let err = svc
        .put_bucket_inventory_configuration(as_user(
            req(PutBucketInventoryConfigurationInput {
                bucket: "src".into(),
                id: "c1".into(),
                inventory_configuration: mismatched,
                expected_bucket_owner: None,
            }),
            uid,
            true,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "InvalidArgument");

    svc.put_bucket_inventory_configuration(as_user(
        req(PutBucketInventoryConfigurationInput {
            bucket: "src".into(),
            id: "c1".into(),
            inventory_configuration: c,
            expected_bucket_owner: None,
        }),
        uid,
        true,
    ))
    .await
    .unwrap();

    let got = svc
        .get_bucket_inventory_configuration(req(GetBucketInventoryConfigurationInput {
            bucket: "src".into(),
            id: "c1".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .inventory_configuration
        .unwrap();
    assert_eq!(got.id, "c1");
    assert_eq!(got.destination.s3_bucket_destination.bucket, "dst");

    let listed = svc
        .list_bucket_inventory_configurations(req(ListBucketInventoryConfigurationsInput {
            bucket: "src".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(listed.inventory_configuration_list.unwrap().len(), 1);
    assert_eq!(listed.is_truncated, Some(false));

    let err = svc
        .get_bucket_inventory_configuration(req(GetBucketInventoryConfigurationInput {
            bucket: "src".into(),
            id: "missing".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NoSuchConfiguration");
    assert_eq!(err.status_code(), Some(HttpStatus::NOT_FOUND));

    svc.delete_bucket_inventory_configuration(req(DeleteBucketInventoryConfigurationInput {
        bucket: "src".into(),
        id: "c1".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 删除幂等
    svc.delete_bucket_inventory_configuration(req(DeleteBucketInventoryConfigurationInput {
        bucket: "src".into(),
        id: "c1".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert!(
        svc.list_bucket_inventory_configurations(req(ListBucketInventoryConfigurationsInput {
            bucket: "src".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .inventory_configuration_list
        .unwrap()
        .is_empty()
    );

    // 权限口径：只读可 Get/List，不可 Put/Delete；自有桶可写
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    st.db
        .set_permissions(
            sub,
            &[PermissionRow {
                bucket: "src".into(),
                read: true,
                write: false,
                delete: false,
            }],
        )
        .await
        .unwrap();
    add_bucket(&st, "mine", Some(sub)).await;
    let acl = st.acl();
    let u = user(sub, false);
    let check = |op: &'static str, b: &'static str| {
        let acl = acl.clone();
        let u = u.clone();
        async move {
            acl.check_op(&u, "test-ak", op, Some(b), None, None)
                .await
                .unwrap()
                .0
        }
    };
    assert!(check("GetBucketInventoryConfiguration", "src").await);
    assert!(check("ListBucketInventoryConfigurations", "src").await);
    assert!(!check("PutBucketInventoryConfiguration", "src").await);
    assert!(!check("DeleteBucketInventoryConfiguration", "src").await);
    assert!(check("PutBucketInventoryConfiguration", "mine").await);
}

#[tokio::test]
async fn validation_matrix_and_per_bucket_limit() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;

    let bad = |mutate: &dyn Fn(&mut InventoryConfiguration)| {
        let mut c = cfg("c1", "Daily", "dst");
        mutate(&mut c);
        c
    };
    // id 字符集 / 长度
    assert!(matches!(
        put(&st, uid, "src", &bad(&|c| c.id = "bad id".into())).await,
        Err(InventoryError::InvalidArgument(_))
    ));
    assert!(matches!(
        put(&st, uid, "src", &bad(&|c| c.id = "x".repeat(65))).await,
        Err(InventoryError::InvalidArgument(_))
    ));
    // 频率
    assert!(matches!(
        put(
            &st,
            uid,
            "src",
            &bad(&|c| { c.schedule.frequency = InventoryFrequency::from("Hourly".to_string()) })
        )
        .await,
        Err(InventoryError::InvalidArgument(_))
    ));
    // 未知格式 → 501（CSV/Parquet 已支持）
    assert!(matches!(
        put(
            &st,
            uid,
            "src",
            &bad(&|c| {
                c.destination.s3_bucket_destination.format =
                    InventoryFormat::from("ORC".to_string())
            })
        )
        .await,
        Err(InventoryError::NotImplemented(_))
    ));
    // 不支持的可选列必须 400（不得静默空列）
    for f in ["IsMultipartUploaded", "IntelligentTieringAccessTier"] {
        assert!(matches!(
            put(
                &st,
                uid,
                "src",
                &with_optional(cfg("c1", "Daily", "dst"), &[f])
            )
            .await,
            Err(InventoryError::InvalidArgument(_))
        ));
    }
    // 未知可选列
    assert!(matches!(
        put(
            &st,
            uid,
            "src",
            &with_optional(cfg("c1", "Daily", "dst"), &["Nope"])
        )
        .await,
        Err(InventoryError::InvalidArgument(_))
    ));
    // included_object_versions 值域
    assert!(matches!(
        put(
            &st,
            uid,
            "src",
            &bad(&|c| {
                c.included_object_versions =
                    InventoryIncludedObjectVersions::from("Both".to_string())
            })
        )
        .await,
        Err(InventoryError::InvalidArgument(_))
    ));
    // 目标桶不存在 / 源桶不存在
    assert!(matches!(
        put(&st, uid, "src", &cfg("c1", "Daily", "nope")).await,
        Err(InventoryError::InvalidArgument(_))
    ));
    assert!(matches!(
        put(&st, uid, "nope", &cfg("c1", "Daily", "dst")).await,
        Err(InventoryError::NoSuchBucket)
    ));
    // 配置所有者无目标桶 PutObject → 403
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    add_bucket(&st, "subsrc", Some(sub)).await;
    let err = st
        .inventory
        .put_config("subsrc", &user(sub, false), &cfg("c1", "Daily", "dst"))
        .await
        .unwrap_err();
    assert!(matches!(err, InventoryError::AccessDenied(_)));

    // 每桶上限（默认 10）：第 11 个拒绝；覆盖已有配置仍允许
    for i in 0..10 {
        put(&st, uid, "src", &cfg(&format!("c{i}"), "Daily", "dst"))
            .await
            .unwrap();
    }
    let err = put(&st, uid, "src", &cfg("c10", "Daily", "dst"))
        .await
        .unwrap_err();
    assert!(matches!(err, InventoryError::InvalidArgument(ref m) if m.contains("最多")));
    put(&st, uid, "src", &cfg("c0", "Weekly", "dst"))
        .await
        .unwrap();
    assert_eq!(st.db.inventory_config_count("src").await.unwrap(), 10);
}

#[tokio::test]
async fn console_crud_validation_and_owner_guard() {
    let dir = tempfile::tempdir().unwrap();
    let (st, _uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    add_bucket(&st, "subsrc", Some(sub)).await;

    let base = serve(oss_server::router::build_router(st.clone()).unwrap()).await;
    let http = reqwest::Client::new();
    let token = login(&base, "erik", "pw").await;

    // 建配置
    let body = serde_json::json!({
        "id": "c1", "frequency": "Daily", "destination_bucket": "dst",
        "included_object_versions": "Current", "optional_fields": ["EncryptionStatus"]
    });
    assert_eq!(
        http.put(format!("{base}/api/v1/buckets/src/inventory"))
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status(),
        HttpStatus::NO_CONTENT
    );
    let listed: Vec<serde_json::Value> = http
        .get(format!("{base}/api/v1/buckets/src/inventory"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["id"], "c1");
    assert_eq!(listed[0]["optional_fields"][0], "EncryptionStatus");

    // 校验矩阵的错误码映射
    for (body, status) in [
        (
            serde_json::json!({"id": "bad id", "frequency": "Daily", "destination_bucket": "dst"}),
            HttpStatus::BAD_REQUEST,
        ),
        (
            serde_json::json!({"id": "c2", "frequency": "Daily", "destination_bucket": "dst", "format": "ORC"}),
            HttpStatus::NOT_IMPLEMENTED,
        ),
        (
            serde_json::json!({"id": "c2", "frequency": "Daily", "destination_bucket": "dst", "optional_fields": ["IsMultipartUploaded"]}),
            HttpStatus::BAD_REQUEST,
        ),
        (
            serde_json::json!({"id": "c2", "frequency": "Daily", "destination_bucket": "nope"}),
            HttpStatus::BAD_REQUEST,
        ),
    ] {
        assert_eq!(
            http.put(format!("{base}/api/v1/buckets/src/inventory"))
                .bearer_auth(&token)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            status,
            "body={body}"
        );
    }

    // 子账户：非桶主 → 403；自有桶 → 204
    let sub_token = login(&base, "sub", "pw").await;
    let put = |bucket: &str, dest: &str| {
        let bucket = bucket.to_string();
        let dest = dest.to_string();
        let http = http.clone();
        let base = base.clone();
        let token = sub_token.clone();
        let body = serde_json::json!({
            "id": "sub1", "frequency": "Weekly", "destination_bucket": dest
        });
        async move {
            http.put(format!("{base}/api/v1/buckets/{bucket}/inventory"))
                .bearer_auth(token)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(put("src", "dst").await, HttpStatus::FORBIDDEN);
    assert_eq!(put("subsrc", "subsrc").await, HttpStatus::NO_CONTENT);
}

// ---------- 调度槽 ----------

#[tokio::test]
async fn due_slots_dedupe_and_only_latest_catch_up() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "src2", None).await;
    add_bucket(&st, "dst", None).await;
    put(&st, uid, "src", &cfg("d1", "Daily", "dst"))
        .await
        .unwrap();
    put(&st, uid, "src2", &cfg("w1", "Weekly", "dst"))
        .await
        .unwrap();

    // 2021-01-01T00:00:00Z：Daily=2021-01-01，Weekly=2020-W53
    let t = 1_609_459_200i64;
    assert_eq!(due_runs(&st.db, t).await.unwrap(), 2);
    assert_eq!(due_runs(&st.db, t + 60).await.unwrap(), 0, "同槽去重");

    let daily = run_row(&st, "src", "d1", &daily_key(t)).await;
    assert_eq!(daily.state, "running");
    assert_eq!(daily.started_at, 0);
    assert!(daily.job_id.is_some());
    assert!(
        st.db
            .inventory_run_by_key("src2", "w1", &weekly_key(t))
            .await
            .unwrap()
            .is_some()
    );

    // 停机两天后只补当前槽：不补 01-02，只建 01-03
    let t2 = t + 2 * 86_400;
    assert_eq!(due_runs(&st.db, t2).await.unwrap(), 1);
    assert!(
        st.db
            .inventory_run_by_key("src", "d1", &daily_key(t2))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        st.db
            .inventory_run_by_key("src", "d1", &daily_key(t + 86_400))
            .await
            .unwrap()
            .is_none(),
        "旧槽不补"
    );
    // 同一 ISO 周内 Weekly 仍去重
    assert_eq!(weekly_key(t), weekly_key(t2));
    assert_eq!(due_runs(&st.db, t2).await.unwrap(), 0);
}

// ---------- worker 输出 ----------

#[tokio::test]
async fn force_run_and_disabled_config() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "src2", None).await;
    add_bucket(&st, "dst", None).await;
    put(&st, uid, "src", &cfg("c1", "Daily", "dst"))
        .await
        .unwrap();
    let mut off = cfg("c2", "Daily", "dst");
    off.is_enabled = false;
    put(&st, uid, "src2", &off).await.unwrap();
    st.store
        .put_object("src", "a.txt", Bytes::from_static(b"x"), None)
        .await
        .unwrap();

    let base = serve(oss_server::router::build_router(st.clone()).unwrap()).await;
    let http = reqwest::Client::new();
    let token = login(&base, "erik", "pw").await;

    // 禁用配置不调度
    assert_eq!(
        due_runs(&st.db, 1_609_459_200).await.unwrap(),
        1,
        "只有启用的 c1 入队"
    );

    // 手动 force-run：202 + run_key=manual-<unix>；入队时 started_at 仍为 0
    let resp = http
        .post(format!("{base}/api/v1/buckets/src/inventory/c1/run"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), HttpStatus::ACCEPTED);
    let v: serde_json::Value = resp.json().await.unwrap();
    let run_key = v["run_key"].as_str().unwrap().to_string();
    assert!(run_key.starts_with("manual-"), "{run_key}");
    assert_eq!(
        v["job_id"].as_i64(),
        run_row(&st, "src", "c1", &run_key).await.job_id
    );
    assert_eq!(run_row(&st, "src", "c1", &run_key).await.started_at, 0);
    // I3：force-run 留控制面审计（uid/op/目标/detail）
    let (actor, decision, detail): (i64, String, Option<String>) = sqlx::query_as(
        "SELECT uid, decision, detail FROM audit_log WHERE op = 'control:inventory-run' AND bucket = 'src' AND object_key = 'c1'",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!((actor, decision.as_str()), (uid, "allow"));
    assert!(
        detail.unwrap().contains(&format!("run_key={run_key};job=")),
        "detail 应记录 run_key 与 job"
    );

    // 不存在的配置 → 404
    assert_eq!(
        http.post(format!("{base}/api/v1/buckets/src/inventory/nope/run"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        HttpStatus::NOT_FOUND
    );

    run_once(&st).await; // 可能是 c1 的日槽或 manual 任务之一
    // 把剩余任务跑完，确认 manual 那次成功
    while worker_once(&st.db, &st.store, st.inventory_runner.as_ref(), ENV, now())
        .await
        .unwrap()
    {}
    let done = run_row(&st, "src", "c1", &run_key).await;
    assert_eq!(done.state, "done", "error: {:?}", done.error);
    assert!(done.started_at > 0, "started_at 必须是实际开始时刻");
    let (manifest, data) = output(&st, "dst", "src", "c1", &run_key).await;
    assert_eq!(
        manifest["files"][0]["size"].as_u64().unwrap(),
        data.len() as u64
    );

    // 禁用配置 force-run → skipped，无输出
    let resp = http
        .post(format!("{base}/api/v1/buckets/src2/inventory/c2/run"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), HttpStatus::ACCEPTED);
    let off_key = resp.json::<serde_json::Value>().await.unwrap()["run_key"]
        .as_str()
        .unwrap()
        .to_string();
    while worker_once(&st.db, &st.store, st.inventory_runner.as_ref(), ENV, now())
        .await
        .unwrap()
    {}
    let skipped = run_row(&st, "src2", "c2", &off_key).await;
    assert_eq!(skipped.state, "skipped");
    assert_eq!(skipped.manifest_key, None);
}

#[tokio::test]
async fn empty_bucket_manifest_and_gzip_csv() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let mut c = with_optional(cfg("c1", "Daily", "dst"), &["ReplicationStatus"]);
    c.destination.s3_bucket_destination.prefix = Some("inv/out".to_string());
    put(&st, uid, "src", &c).await.unwrap();

    let t = now();
    let key = "manual-empty";
    let job = enqueue_run(&st.db, "src", "c1", key, Some(uid), t)
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;

    let (manifest, data) = output(&st, "dst", "src", "c1", key).await;
    assert_eq!(manifest["sourceBucket"], "src");
    assert_eq!(manifest["destinationBucket"], "dst");
    assert_eq!(manifest["version"], "2016-11-30");
    assert_eq!(manifest["fileFormat"], "CSV");
    assert_eq!(
        manifest["fileSchema"],
        format!("{},ReplicationStatus", MANDATORY.join(","))
    );
    assert_eq!(
        manifest["creationTimestamp"].as_str().unwrap().len(),
        24,
        "ISO8601 UTC 毫秒"
    );
    let file = &manifest["files"][0];
    assert_eq!(file["size"].as_u64().unwrap(), data.len() as u64);
    let digest = {
        use base64::Engine as _;
        use md5::{Digest as _, Md5};
        base64::engine::general_purpose::STANDARD.encode(Md5::digest(&data))
    };
    assert_eq!(file["md5Checksum"], digest);
    let dkey = file["key"].as_str().unwrap();
    assert!(
        dkey.starts_with(&format!("inv/out/src/c1/{key}/data/")),
        "{dkey}"
    );
    assert!(dkey.ends_with(".csv.gz"));

    // 空桶：只有表头（含请求的可选列），gzip 可解
    let rows = gunzip_csv(&data);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        MANDATORY
            .iter()
            .copied()
            .chain(["ReplicationStatus"])
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
    );
    let job_row = st.db.job_get(job).await.unwrap().unwrap();
    assert_eq!(job_row["progress"], 0);
}

#[tokio::test]
async fn current_scan_paginates_over_1000_and_applies_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    for i in 0..1001 {
        st.store
            .put_object(
                "src",
                &format!("logs/o{i:04}"),
                Bytes::from_static(b"x"),
                None,
            )
            .await
            .unwrap();
    }
    st.store
        .put_object("src", "other/skip", Bytes::from_static(b"x"), None)
        .await
        .unwrap();

    let mut c = cfg("c1", "Daily", "dst");
    c.filter = Some(InventoryFilter {
        prefix: "logs/".to_string(),
    });
    put(&st, uid, "src", &c).await.unwrap();

    let key = "manual-page";
    let job = enqueue_run(&st.db, "src", "c1", key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;

    let (_, data) = output(&st, "dst", "src", "c1", key).await;
    let rows = gunzip_csv(&data);
    assert_eq!(rows.len(), 1 + 1001, "表头 + 全部分页对象");
    let header = &rows[0];
    assert_eq!(header.len(), MANDATORY.len());
    assert_eq!(header[0], "Bucket");
    assert_eq!(header[8], "StorageClass");
    let keys: Vec<&str> = rows[1..].iter().map(|r| r[1].as_str()).collect();
    assert!(keys.iter().all(|k| k.starts_with("logs/")));
    assert_eq!(keys.first(), Some(&"logs/o0000"));
    assert_eq!(keys.last(), Some(&"logs/o1000"));
    assert_eq!(rows[1][0], "src");
    assert_eq!(rows[1][3], "TRUE"); // IsLatest
    assert_eq!(rows[1][4], "FALSE"); // DeleteMarker
    assert_eq!(rows[1][8], "STANDARD");
    let job_row = st.db.job_get(job).await.unwrap().unwrap();
    assert_eq!(job_row["progress"], 1001, "progress = 扫描数");
}

#[tokio::test]
async fn versions_and_optional_metadata_columns() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = 'src'")
        .execute(st.db.pool())
        .await
        .unwrap();
    st.store
        .init_encryption(Some(dir.path().join("master.key")), None)
        .await
        .unwrap();

    let lock = ObjectLockRecord {
        mode: Some("GOVERNANCE".to_string()),
        retain_until_nanos: Some((now() + 3600) * 1_000_000_000),
        legal_hold: true,
    };
    let checksum = ChecksumRecord {
        algorithm: "SHA256".to_string(),
        value_base64: "Zm9v".to_string(),
    };
    let put_v = |body: &'static [u8]| {
        let lock = lock.clone();
        let checksum = checksum.clone();
        let store = Arc::clone(&st.store);
        async move {
            store
                .put_object_stream_with_meta(
                    "src",
                    "v.txt",
                    Box::pin(futures::stream::once(async move {
                        Ok(Bytes::from_static(body))
                    })),
                    None,
                    Default::default(),
                    Some(checksum),
                    VersioningMode::Enabled,
                    Some(EncSpec::SseS3),
                    Some(lock),
                    None,
                    None,
                )
                .await
                .unwrap()
        }
    };
    let v1 = put_v(b"one").await;
    let v2 = put_v(b"two").await;
    st.store
        .set_replication_status(
            "src",
            "v.txt",
            &v1.version_id.clone().unwrap(),
            Some("PENDING"),
        )
        .await
        .unwrap();
    // 无版本号删除 → 追加 delete marker（成为 latest）
    st.store
        .delete_object_versioned("src", "v.txt", None, VersioningMode::Enabled)
        .await
        .unwrap();
    st.store
        .put_object("src", "plain.txt", Bytes::from_static(b"p"), None)
        .await
        .unwrap();

    let mut c = with_optional(cfg("c1", "Daily", "dst"), &OPTIONAL);
    c.included_object_versions =
        InventoryIncludedObjectVersions::from_static(InventoryIncludedObjectVersions::ALL);
    put(&st, uid, "src", &c).await.unwrap();
    let key = "manual-versions";
    enqueue_run(&st.db, "src", "c1", key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;

    let (manifest, data) = output(&st, "dst", "src", "c1", key).await;
    assert_eq!(
        manifest["fileSchema"],
        MANDATORY
            .iter()
            .chain(OPTIONAL.iter())
            .copied()
            .collect::<Vec<_>>()
            .join(",")
    );
    let rows = gunzip_csv(&data);
    assert_eq!(rows.len(), 1 + 2 + 1 + 1, "表头 + 2 版本 + marker + plain");
    let col = |name: &str| rows[0].iter().position(|h| h == name).unwrap();
    let find = |key: &str, vid: &str| {
        rows[1..]
            .iter()
            .find(|r| r[col("Key")] == key && r[col("VersionId")] == vid)
            .unwrap_or_else(|| panic!("缺行 {key}/{vid}"))
    };
    let ids: Vec<String> = rows[1..]
        .iter()
        .filter(|r| r[col("Key")] == "v.txt")
        .map(|r| r[col("VersionId")].clone())
        .collect();
    let (id1, id2) = (
        v1.version_id.clone().unwrap(),
        v2.version_id.clone().unwrap(),
    );
    assert!(ids.contains(&id1) && ids.contains(&id2));
    for vid in [&id1, &id2] {
        let r = find("v.txt", vid);
        assert_eq!(r[col("DeleteMarker")], "FALSE");
        assert_eq!(r[col("EncryptionStatus")], "SSE-S3");
        assert_eq!(r[col("ChecksumAlgorithm")], "SHA256");
        assert_eq!(r[col("ObjectLockMode")], "GOVERNANCE");
        assert_eq!(r[col("ObjectLockLegalHoldStatus")], "ON");
        assert!(r[col("ObjectLockRetainUntilDate")].ends_with('Z'));
    }
    assert_eq!(find("v.txt", &id1)[col("ReplicationStatus")], "PENDING");
    assert_eq!(find("v.txt", &id2)[col("ReplicationStatus")], "");

    let marker = rows[1..]
        .iter()
        .find(|r| r[col("Key")] == "v.txt" && r[col("DeleteMarker")] == "TRUE")
        .expect("delete marker 行");
    assert_eq!(marker[col("IsLatest")], "TRUE");
    assert_eq!(marker[col("Size")], "");
    assert_eq!(marker[col("ETag")], "");
    assert_eq!(marker[col("StorageClass")], "");
    assert_eq!(marker[col("EncryptionStatus")], "");
    assert_eq!(marker[col("ObjectLockLegalHoldStatus")], "");
    assert!(!marker[col("VersionId")].is_empty());

    let plain = rows[1..]
        .iter()
        .find(|r| r[col("Key")] == "plain.txt")
        .expect("plain 行");
    assert_eq!(plain[col("EncryptionStatus")], "NOT-ENCRYPTED");
    assert_eq!(plain[col("ObjectLockLegalHoldStatus")], "OFF");
    assert_eq!(plain[col("ChecksumAlgorithm")], "");
}

#[tokio::test]
async fn destination_revoked_marks_job_error() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();

    st.db
        .set_permissions(
            sub,
            &[PermissionRow {
                bucket: "dst".into(),
                read: false,
                write: true,
                delete: false,
            }],
        )
        .await
        .unwrap();
    let c = cfg("c1", "Daily", "dst");
    st.inventory
        .put_config("src", &user(sub, false), &c)
        .await
        .unwrap();

    // 撤掉目标桶写权限后运行：run/job 报错
    st.db.set_permissions(sub, &[]).await.unwrap();
    let key = "manual-revoked";
    let job = enqueue_run(&st.db, "src", "c1", key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;

    let run = run_row(&st, "src", "c1", key).await;
    assert_eq!(run.state, "error");
    assert_eq!(run.manifest_key, None);
    assert!(
        run.error.as_deref().unwrap_or("").contains("PutObject"),
        "error={:?}",
        run.error
    );
    let job_row = st.db.job_get(job).await.unwrap().unwrap();
    assert_eq!(job_row["state"], "error");
    assert_eq!(job_row["progress"], 0);
    // 目标桶无任何输出
    assert!(
        st.store
            .list_objects("dst", &Default::default())
            .await
            .unwrap()
            .objects
            .is_empty()
    );
}

/// 运行期复核所有者仍启用：停用后配置不得继续产出（fail-closed，无输出）。
#[tokio::test]
async fn disabled_owner_marks_job_error() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    st.db
        .set_permissions(
            sub,
            &[
                PermissionRow {
                    bucket: "src".into(),
                    read: true,
                    write: false,
                    delete: false,
                },
                PermissionRow {
                    bucket: "dst".into(),
                    read: false,
                    write: true,
                    delete: false,
                },
            ],
        )
        .await
        .unwrap();
    st.inventory
        .put_config("src", &user(sub, false), &cfg("c1", "Daily", "dst"))
        .await
        .unwrap();

    assert!(st.db.set_user_status(sub, false).await.unwrap());
    let key = "manual-disabled";
    let job = enqueue_run(&st.db, "src", "c1", key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;

    let run = run_row(&st, "src", "c1", key).await;
    assert_eq!(run.state, "error");
    assert_eq!(run.manifest_key, None);
    assert!(
        run.error.as_deref().unwrap_or("").contains("已停用"),
        "error={:?}",
        run.error
    );
    let job_row = st.db.job_get(job).await.unwrap().unwrap();
    assert_eq!(job_row["state"], "error");
    assert_eq!(job_row["progress"], 0);
    assert!(
        keys_under(&st, "dst", &format!("src/c1/{key}"))
            .await
            .is_empty()
    );
}

/// 运行期复核源桶读权限：撤销后失败关闭，不扫描也不写输出。
#[tokio::test]
async fn source_revoked_marks_job_error() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    st.db
        .set_permissions(
            sub,
            &[
                PermissionRow {
                    bucket: "src".into(),
                    read: true,
                    write: false,
                    delete: false,
                },
                PermissionRow {
                    bucket: "dst".into(),
                    read: false,
                    write: true,
                    delete: false,
                },
            ],
        )
        .await
        .unwrap();
    st.inventory
        .put_config("src", &user(sub, false), &cfg("c1", "Daily", "dst"))
        .await
        .unwrap();

    // 只撤源桶读、保留目标桶写：目标侧校验仍通过，源扫描授权必须拦住。
    st.db
        .set_permissions(
            sub,
            &[PermissionRow {
                bucket: "dst".into(),
                read: false,
                write: true,
                delete: false,
            }],
        )
        .await
        .unwrap();
    let key = "manual-src-revoked";
    let job = enqueue_run(&st.db, "src", "c1", key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;

    let run = run_row(&st, "src", "c1", key).await;
    assert_eq!(run.state, "error");
    assert_eq!(run.manifest_key, None);
    assert!(
        run.error.as_deref().unwrap_or("").contains("ListBucket"),
        "error={:?}",
        run.error
    );
    let job_row = st.db.job_get(job).await.unwrap().unwrap();
    assert_eq!(job_row["state"], "error");
    assert_eq!(job_row["progress"], 0);
    assert!(
        keys_under(&st, "dst", &format!("src/c1/{key}"))
            .await
            .is_empty()
    );
}

/// I4：目标桶策略显式 Deny PutObject 压过所有者/管理员表权限——配置期拒绝、运行期报错且无输出。
#[tokio::test]
async fn destination_bucket_policy_deny_wins() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let deny = serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:PutObject",
        "Resource":"arn:aws:s3:::dst/*"}]})
    .to_string();
    st.db.set_bucket_policy("dst", &deny, 0).await.unwrap();

    let err = put(&st, uid, "src", &cfg("c1", "Daily", "dst"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, InventoryError::AccessDenied(ref m) if m.contains("PutObject")),
        "配置期必须被桶策略 Deny 拦住：err={err:?}"
    );

    // 显式 Allow（所有者/管理员路径）→ 配置成功
    let allow = serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject",
        "Resource":"arn:aws:s3:::dst/*"}]})
    .to_string();
    st.db.set_bucket_policy("dst", &allow, 0).await.unwrap();
    put(&st, uid, "src", &cfg("c1", "Daily", "dst"))
        .await
        .unwrap();

    // 配置后目标桶收紧为 Deny：运行期复核失败，job/run 报错且无输出
    st.db.set_bucket_policy("dst", &deny, 0).await.unwrap();
    let key = "manual-deny";
    let job = enqueue_run(&st.db, "src", "c1", key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    let run = run_row(&st, "src", "c1", key).await;
    assert_eq!(run.state, "error");
    assert_eq!(run.manifest_key, None);
    assert!(
        run.error.as_deref().unwrap_or("").contains("PutObject"),
        "error={:?}",
        run.error
    );
    let job_row = st.db.job_get(job).await.unwrap().unwrap();
    assert_eq!(job_row["state"], "error");
    assert!(
        st.store
            .list_objects("dst", &Default::default())
            .await
            .unwrap()
            .objects
            .is_empty(),
        "Deny 后不得写出任何目标对象"
    );
}

/// I5：Inventory 数据文件与 manifest 必须套目标桶默认加密（AES256 → SSE-S3）。
#[tokio::test]
async fn outputs_apply_destination_default_encryption() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    st.store
        .init_encryption(Some(dir.path().join("master.key")), None)
        .await
        .unwrap();
    st.db
        .set_bucket_default_encryption("dst", "AES256")
        .await
        .unwrap();
    st.store
        .put_object("src", "a.txt", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    put(&st, uid, "src", &cfg("c1", "Daily", "dst"))
        .await
        .unwrap();

    let key = "manual-enc";
    enqueue_run(&st.db, "src", "c1", key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;

    let run = run_row(&st, "src", "c1", key).await;
    assert_eq!(run.state, "done", "error: {:?}", run.error);
    let mkey = run.manifest_key.expect("manifest_key");
    let rec = st
        .store
        .head_object("dst", &mkey)
        .await
        .unwrap()
        .encryption
        .expect("manifest 应套用桶默认加密");
    assert_eq!(rec.key_id, "file", "AES256 → SSE-S3（file provider）");

    let (manifest, data) = output(&st, "dst", "src", "c1", key).await;
    assert!(!data.is_empty());
    let dkey = manifest["files"][0]["key"].as_str().unwrap();
    let rec = st
        .store
        .head_object("dst", dkey)
        .await
        .unwrap()
        .encryption
        .expect("数据文件应套用桶默认加密");
    assert_eq!(rec.key_id, "file", "AES256 → SSE-S3（file provider）");
}

// ---------- console 测试脚手架 ----------

// ---------- 3.7 跨账户目标 ----------

fn account(uid: i64) -> String {
    account_id_of(uid).expect("uid 派生 12 位账户")
}

/// 跨账户目标只允许管理员创建；持有目标桶写权限的非管理员仍被拒绝。
#[tokio::test]
async fn cross_account_destination_admin_only() {
    let dir = tempfile::tempdir().unwrap();
    let (st, admin) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    add_bucket(&st, "subsrc", Some(sub)).await;
    add_bucket(&st, "subdst", Some(sub)).await;
    st.db
        .set_permissions(
            sub,
            &[PermissionRow {
                bucket: "dst".into(),
                read: false,
                write: true,
                delete: false,
            }],
        )
        .await
        .unwrap();

    let err = st
        .inventory
        .put_config("subsrc", &user(sub, false), &cfg("c1", "Daily", "dst"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, InventoryError::AccessDenied(ref m) if m.contains("跨账户")),
        "{err:?}"
    );

    // 同账户目标（subsrc -> subdst）：非管理员不受影响。
    st.inventory
        .put_config("subsrc", &user(sub, false), &cfg("same", "Daily", "subdst"))
        .await
        .unwrap();
    // 跨账户目标：管理员允许。
    st.inventory
        .put_config("subsrc", &user(admin, true), &cfg("cross", "Daily", "dst"))
        .await
        .unwrap();
}

/// 目标账户必须为 12 位且等于目标桶当前账户；桶字段接受 ARN 并归一为桶名。
#[tokio::test]
async fn destination_account_validation_and_arn() {
    let dir = tempfile::tempdir().unwrap();
    let (st, admin) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    let sub_acct = account(sub);

    let mut c = cfg("c1", "Daily", "dst");
    c.destination.s3_bucket_destination.account_id = Some(sub_acct.clone());
    let err = put(&st, admin, "src", &c).await.unwrap_err();
    assert!(
        matches!(err, InventoryError::InvalidArgument(ref m) if m.contains("不一致")),
        "{err:?}"
    );

    c.destination.s3_bucket_destination.account_id = Some("123".into());
    assert!(matches!(
        put(&st, admin, "src", &c).await,
        Err(InventoryError::InvalidArgument(_))
    ));

    c.destination.s3_bucket_destination.account_id = Some(LEGACY_ACCOUNT.into());
    put(&st, admin, "src", &c).await.unwrap();
    let got = st.inventory.get_config("src", "c1").await.unwrap().unwrap();
    assert_eq!(
        got.destination.s3_bucket_destination.account_id.as_deref(),
        Some(LEGACY_ACCOUNT)
    );

    // ARN 形态：校验通过并归一为桶名存储/返回。
    let mut arn = cfg("c2", "Daily", "arn:aws:s3:::dst");
    arn.destination.s3_bucket_destination.account_id = Some(LEGACY_ACCOUNT.into());
    put(&st, admin, "src", &arn).await.unwrap();
    let got = st.inventory.get_config("src", "c2").await.unwrap().unwrap();
    assert_eq!(got.destination.s3_bucket_destination.bucket, "dst");
    assert!(matches!(
        put(
            &st,
            admin,
            "src",
            &cfg("c3", "Daily", "arn:aws:s3:::dst/key")
        )
        .await,
        Err(InventoryError::InvalidArgument(_))
    ));

    // S3 线格式同样接受并回显 destination AccountId。
    let mut s3c = cfg("s3c", "Daily", "dst");
    s3c.destination.s3_bucket_destination.account_id = Some(LEGACY_ACCOUNT.into());
    let svc = s3(&st);
    svc.put_bucket_inventory_configuration(as_user(
        req(PutBucketInventoryConfigurationInput {
            bucket: "src".into(),
            id: "s3c".into(),
            inventory_configuration: s3c,
            expected_bucket_owner: None,
        }),
        admin,
        true,
    ))
    .await
    .unwrap();
    let got = svc
        .get_bucket_inventory_configuration(req(GetBucketInventoryConfigurationInput {
            bucket: "src".into(),
            id: "s3c".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .inventory_configuration
        .unwrap();
    assert_eq!(
        got.destination.s3_bucket_destination.account_id.as_deref(),
        Some(LEGACY_ACCOUNT)
    );
}

/// 跨账户配置由管理员创建后可运行；manifest 记录源/目标账户。
#[tokio::test]
async fn cross_account_run_manifest_accounts() {
    let dir = tempfile::tempdir().unwrap();
    let (st, admin) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    add_bucket(&st, "subdst", Some(sub)).await;
    let sub_acct = account(sub);

    let mut c = cfg("c1", "Daily", "subdst");
    c.destination.s3_bucket_destination.account_id = Some(sub_acct.clone());
    put(&st, admin, "src", &c).await.unwrap();
    st.store
        .put_object("src", "a.txt", Bytes::from_static(b"x"), None)
        .await
        .unwrap();

    let key = "manual-xacct";
    enqueue_run(&st.db, "src", "c1", key, Some(admin), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;

    let (manifest, _) = output(&st, "subdst", "src", "c1", key).await;
    assert_eq!(manifest["sourceBucket"], "src");
    assert_eq!(manifest["destinationBucket"], "subdst");
    assert_eq!(manifest["accountId"], LEGACY_ACCOUNT);
    assert_eq!(
        manifest["destinationAccountId"].as_str(),
        Some(sub_acct.as_str())
    );
}

/// 运行期复核记录源账户与目标桶账户：属主/账户漂移 → 失败且不写输出。
#[tokio::test]
async fn account_drift_fails_run_closed() {
    let dir = tempfile::tempdir().unwrap();
    let (st, admin) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    add_bucket(&st, "subdst", Some(sub)).await;
    let sub_acct = account(sub);

    let mut c = cfg("c1", "Daily", "subdst");
    c.destination.s3_bucket_destination.account_id = Some(sub_acct.clone());
    put(&st, admin, "src", &c).await.unwrap();
    st.store
        .put_object("src", "a.txt", Bytes::from_static(b"x"), None)
        .await
        .unwrap();

    let ok = "manual-ok";
    enqueue_run(&st.db, "src", "c1", ok, Some(admin), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    output(&st, "subdst", "src", "c1", ok).await;

    // 目标桶归属变更：配置账户不再匹配 → 失败关闭，且不写任何目标对象。
    sqlx::query("UPDATE buckets SET owner_id = NULL WHERE name = 'subdst'")
        .execute(st.db.pool())
        .await
        .unwrap();
    let drift = "manual-drift";
    enqueue_run(&st.db, "src", "c1", drift, Some(admin), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    let run = run_row(&st, "src", "c1", drift).await;
    assert_eq!(run.state, "error");
    assert_eq!(run.manifest_key, None);
    assert!(
        run.error.as_deref().unwrap_or("").contains("账户校验失败"),
        "error={:?}",
        run.error
    );
    assert!(
        keys_under(&st, "subdst", "src/c1/manual-drift")
            .await
            .is_empty()
    );

    // 恢复目标桶归属，改源桶归属：记录的源账户漂移同样失败关闭。
    sqlx::query("UPDATE buckets SET owner_id = ? WHERE name = 'subdst'")
        .bind(sub)
        .execute(st.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE buckets SET owner_id = ? WHERE name = 'src'")
        .bind(sub)
        .execute(st.db.pool())
        .await
        .unwrap();
    let src_drift = "manual-src-drift";
    enqueue_run(&st.db, "src", "c1", src_drift, Some(admin), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    let run = run_row(&st, "src", "c1", src_drift).await;
    assert_eq!(run.state, "error");
    assert!(
        run.error
            .as_deref()
            .unwrap_or("")
            .contains("源桶账户已变化"),
        "error={:?}",
        run.error
    );
}

/// 旧配置 JSON（无源账户扩展键）仍可读写/运行；manifest 按当前桶账户推导。
#[tokio::test]
async fn legacy_config_json_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (st, admin) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let c = cfg("old", "Daily", "dst");
    let legacy_json = serde_json::to_string(&c).unwrap();
    st.db
        .inventory_upsert_config("src", "old", admin, &legacy_json, 0)
        .await
        .unwrap();

    assert_eq!(
        st.inventory.get_config("src", "old").await.unwrap(),
        Some(c.clone())
    );
    assert_eq!(
        st.inventory.list_configs("src").await.unwrap(),
        vec![c.clone()]
    );

    let key = "manual-legacy";
    enqueue_run(&st.db, "src", "old", key, Some(admin), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    let (manifest, _) = output(&st, "dst", "src", "old", key).await;
    assert_eq!(manifest["accountId"], LEGACY_ACCOUNT);
    assert_eq!(manifest["destinationAccountId"], LEGACY_ACCOUNT);
}

/// 控制台：跨账户目标非管理员 403（即使有目标桶写权限）；列表返回源/目标账户。
#[tokio::test]
async fn console_cross_account_admin_only_and_account_fields() {
    let dir = tempfile::tempdir().unwrap();
    let (st, _admin) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    let sub = st.db.create_user("sub", "pw", false).await.unwrap();
    add_bucket(&st, "subsrc", Some(sub)).await;
    st.db
        .set_permissions(
            sub,
            &[PermissionRow {
                bucket: "dst".into(),
                read: false,
                write: true,
                delete: false,
            }],
        )
        .await
        .unwrap();

    let base = serve(oss_server::router::build_router(st.clone()).unwrap()).await;
    let http = reqwest::Client::new();
    let sub_token = login(&base, "sub", "pw").await;
    let admin_token = login(&base, "erik", "pw").await;

    let body = serde_json::json!({
        "id": "c1", "frequency": "Daily", "destination_bucket": "dst",
        "destination_account_id": LEGACY_ACCOUNT
    });
    assert_eq!(
        http.put(format!("{base}/api/v1/buckets/subsrc/inventory"))
            .bearer_auth(&sub_token)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status(),
        HttpStatus::FORBIDDEN
    );

    // 管理员同账户配置：204，列表回显源/目标账户。
    assert_eq!(
        http.put(format!("{base}/api/v1/buckets/src/inventory"))
            .bearer_auth(&admin_token)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status(),
        HttpStatus::NO_CONTENT
    );
    let listed: Vec<serde_json::Value> = http
        .get(format!("{base}/api/v1/buckets/src/inventory"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed[0]["source_account_id"], LEGACY_ACCOUNT);
    assert_eq!(listed[0]["destination_account_id"], LEGACY_ACCOUNT);

    // 非 12 位目标账户 → 400。
    let bad = serde_json::json!({
        "id": "c2", "frequency": "Daily", "destination_bucket": "dst",
        "destination_account_id": "42"
    });
    assert_eq!(
        http.put(format!("{base}/api/v1/buckets/src/inventory"))
            .bearer_auth(&admin_token)
            .json(&bad)
            .send()
            .await
            .unwrap()
            .status(),
        HttpStatus::BAD_REQUEST
    );
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn login(base: &str, user: &str, pw: &str) -> String {
    let v: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": user, "password": pw}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["token"].as_str().unwrap().to_string()
}

/// 4.9：Hive `dt=` 分区取 run 行的 created_at（入队时刻），重试跨分钟/跨天
/// 也只写一个分区，不会因重试生成第二个 symlink 分区。
#[tokio::test]
async fn hive_partition_is_stable_across_retries() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    st.store
        .put_object("src", "a.txt", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    put(&st, uid, "src", &cfg("c1", "Daily", "dst"))
        .await
        .unwrap();

    // 入队时刻设为 3 天前：分区必须由它决定，而不是 worker 实际执行时刻。
    let enqueued = now() - 3 * 86_400;
    enqueue_run(&st.db, "src", "c1", "manual-hive", Some(uid), enqueued)
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    assert_eq!(run_row(&st, "src", "c1", "manual-hive").await.state, "done");

    let dt = time::OffsetDateTime::from_unix_timestamp(enqueued).unwrap();
    let expected = format!(
        "src/c1/manual-hive/hive/dt={:04}-{:02}-{:02}-{:02}-{:02}/symlink.txt",
        dt.year(),
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute()
    );
    let hive = keys_under(&st, "dst", "src/c1/manual-hive/hive/").await;
    assert_eq!(hive, vec![expected.clone()]);

    // 模拟同一 run_key 的重试（实际时刻已跨分钟/跨天）。
    let payload = InventoryPayload {
        bucket: "src".into(),
        config_id: "c1".into(),
        run_key: "manual-hive".into(),
    };
    st.inventory_runner
        .execute("Inventory", &serde_json::to_string(&payload).unwrap())
        .await
        .unwrap();
    let hive = keys_under(&st, "dst", "src/c1/manual-hive/hive/").await;
    assert_eq!(hive, vec![expected], "重试不得再写第二个 dt= 分区");
}

/// 3.6：增量模式按 config 开启；首轮全量、后续只出 last_modified >= 水位 的对象，
/// manifest 记录上轮/本轮水位；关闭后恢复全量且旧水位作废。
#[tokio::test]
async fn incremental_mode_reports_only_new_objects_and_records_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    st.store
        .put_object("src", "a-old", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    put(&st, uid, "src", &cfg("c1", "Daily", "dst"))
        .await
        .unwrap();
    st.db
        .inventory_incremental_set("src", "c1", true, now())
        .await
        .unwrap();

    // 首轮无水位：全量快照 + 记录新水位
    let k1 = "manual-inc-1";
    enqueue_run(&st.db, "src", "c1", k1, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    let (m1, d1) = output(&st, "dst", "src", "c1", k1).await;
    let rows = gunzip_csv(&d1);
    assert_eq!(rows.len(), 2, "表头 + 全量对象：{rows:?}");
    assert_eq!(m1["incremental"]["enabled"], true);
    assert!(m1["incremental"]["previousCursor"].is_null(), "{m1}");
    let cursor1 = m1["incremental"]["cursor"].as_str().unwrap().to_string();

    // 跨过水位后再写新对象：第二轮只出增量
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    st.store
        .put_object("src", "b-new", Bytes::from_static(b"y"), None)
        .await
        .unwrap();
    let k2 = "manual-inc-2";
    enqueue_run(&st.db, "src", "c1", k2, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    let (m2, d2) = output(&st, "dst", "src", "c1", k2).await;
    let rows = gunzip_csv(&d2);
    assert_eq!(rows.len(), 2, "仅新增对象：{rows:?}");
    assert_eq!(rows[1][1], "b-new");
    assert_eq!(m2["incremental"]["previousCursor"], cursor1.as_str());
    assert_ne!(m2["incremental"]["cursor"], cursor1.as_str());

    // 关闭增量 → 旧水位作废，下一轮恢复全量（默认行为）
    st.db
        .inventory_incremental_set("src", "c1", false, now())
        .await
        .unwrap();
    let k3 = "manual-full-3";
    enqueue_run(&st.db, "src", "c1", k3, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    let (m3, d3) = output(&st, "dst", "src", "c1", k3).await;
    let rows = gunzip_csv(&d3);
    assert_eq!(rows.len(), 3, "关闭增量后应全量：{rows:?}");
    assert!(m3.get("incremental").is_none(), "{m3}");
}

/// H1：水位只在 run 落 done 之后推进；done 失败后的重试必须重放上一区间，不得静默漏项。
#[tokio::test]
async fn incremental_watermark_advances_only_after_run_done() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src", None).await;
    add_bucket(&st, "dst", None).await;
    for key in ["a-old", "b-old"] {
        st.store
            .put_object("src", key, Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }
    put(&st, uid, "src", &cfg("c1", "Daily", "dst"))
        .await
        .unwrap();
    st.db
        .inventory_incremental_set("src", "c1", true, now())
        .await
        .unwrap();
    let run_key = "manual-crash-1";
    enqueue_run(&st.db, "src", "c1", run_key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    let payload = serde_json::to_string(&InventoryPayload {
        bucket: "src".into(),
        config_id: "c1".into(),
        run_key: run_key.into(),
    })
    .unwrap();

    // 注入 done 失败：模拟数据/manifest 已写、标记 done 前崩溃。
    sqlx::raw_sql(
        "CREATE TRIGGER fail_done BEFORE UPDATE ON inventory_runs \
         WHEN NEW.state = 'done' BEGIN SELECT RAISE(ABORT, 'injected done failure'); END;",
    )
    .execute(st.db.pool())
    .await
    .unwrap();
    let err = st
        .inventory_runner
        .execute("Inventory", &payload)
        .await
        .unwrap_err();
    assert!(err.contains("injected"), "{err}");
    let cfg_row = st
        .db
        .inventory_get_config("src", "c1")
        .await
        .unwrap()
        .unwrap();
    assert!(
        st.db
            .inventory_cursor_get("src", "c1", cfg_row.updated_at)
            .await
            .unwrap()
            .is_none(),
        "done 失败不得推进水位"
    );
    assert_ne!(run_row(&st, "src", "c1", run_key).await.state, "done");

    // 故障消除后重试同一 run_key：水位仍是首轮之前 → 两个旧对象必须重新出现。
    sqlx::raw_sql("DROP TRIGGER fail_done")
        .execute(st.db.pool())
        .await
        .unwrap();
    st.inventory_runner
        .execute("Inventory", &payload)
        .await
        .unwrap();
    let (_m, d) = output(&st, "dst", "src", "c1", run_key).await;
    let rows = gunzip_csv(&d);
    assert_eq!(rows.len(), 3, "重试应重放首轮对象：{rows:?}");
    let keys: Vec<&str> = rows.iter().skip(1).map(|r| r[1].as_str()).collect();
    assert!(
        keys.contains(&"a-old") && keys.contains(&"b-old"),
        "{keys:?}"
    );
    let cfg_row = st
        .db
        .inventory_get_config("src", "c1")
        .await
        .unwrap()
        .unwrap();
    assert!(
        st.db
            .inventory_cursor_get("src", "c1", cfg_row.updated_at)
            .await
            .unwrap()
            .is_some(),
        "done 后必须推进水位"
    );
}
