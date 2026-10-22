// crates/xl-s3/tests/lock_ops.rs
//! M5 遗留补齐：Put/GetObjectRetention 线级矩阵（桶锁前置、versionId、模式/日期校验、
//! COMPLIANCE 只延长、GOVERNANCE bypass、legal hold 独立、版本化读）。
use std::sync::Arc;

use bytes::Bytes;
use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

// helper 与 object_ops.rs 有意各留一份（直调 service，不走 s3s 授权链）。
async fn service() -> (OssS3, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    (OssS3::new(store, acl().await, 1024 * 1024), dir)
}

async fn acl() -> xl_s3::access::Acl {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0, object_lock INTEGER NOT NULL DEFAULT 0, object_lock_default_mode TEXT, object_lock_default_days INTEGER, default_encryption TEXT, replication_mode TEXT NOT NULL DEFAULT 'async', deleting INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
          can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, can_delete INTEGER NOT NULL DEFAULT 0, \
          PRIMARY KEY (user_id, bucket));\
         CREATE TABLE bucket_policies (bucket TEXT PRIMARY KEY, policy_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, uid INTEGER NOT NULL, op TEXT NOT NULL, bucket TEXT, object_key TEXT, decision TEXT NOT NULL, detail TEXT);\
         CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, user_id INTEGER NOT NULL, \
          status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL, \
          parent_ak TEXT, session_policy TEXT, kind TEXT NOT NULL DEFAULT 'long-term');\
         CREATE TABLE event_configs (bucket TEXT PRIMARY KEY, config_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);",
    )
    .execute(&pool)
    .await
    .unwrap();
    xl_s3::access::Acl::new(pool)
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn now_nanos_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn future_ts(secs: i64) -> ObjectLockRetainUntilDate {
    xl_s3::lock::ts_from_nanos(now_nanos_i64() + secs * 1_000_000_000)
}

fn retention(mode: &'static str, until: ObjectLockRetainUntilDate) -> ObjectLockRetention {
    ObjectLockRetention {
        mode: Some(ObjectLockRetentionMode::from_static(mode)),
        retain_until_date: Some(until),
    }
}

async fn lock_bucket(s3: &OssS3, bucket: &'static str) {
    s3.create_bucket(req(CreateBucketInput {
        bucket: bucket.into(),
        object_lock_enabled_for_bucket: Some(true),
        ..Default::default()
    }))
    .await
    .unwrap();
}

async fn put_obj(
    s3: &OssS3,
    bucket: &'static str,
    key: &'static str,
    body: &'static [u8],
) -> String {
    let out = s3
        .put_object(req(PutObjectInput {
            bucket: bucket.into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    out.version_id.expect("锁桶版本化：PUT 必回 versionId")
}

async fn put_retention(
    s3: &OssS3,
    bucket: &'static str,
    key: &str,
    version_id: Option<&str>,
    retention: Option<ObjectLockRetention>,
    bypass: Option<bool>,
) -> s3s::S3Result<PutObjectRetentionOutput> {
    s3.put_object_retention(req(PutObjectRetentionInput {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version_id.map(str::to_string),
        retention,
        bypass_governance_retention: bypass,
        ..Default::default()
    }))
    .await
    .map(|r| r.output)
}

async fn get_retention(
    s3: &OssS3,
    bucket: &'static str,
    key: &str,
    version_id: Option<&str>,
) -> s3s::S3Result<GetObjectRetentionOutput> {
    s3.get_object_retention(req(GetObjectRetentionInput {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version_id.map(str::to_string),
        ..Default::default()
    }))
    .await
    .map(|r| r.output)
}

/// 基本写读 + HEAD 头 + 清除。
#[tokio::test]
async fn put_get_retention_roundtrip() {
    let (s3, _d) = service().await;
    lock_bucket(&s3, "lockb").await;
    let vid = put_obj(&s3, "lockb", "a", b"x").await;

    // 未设置 → 404 NoSuchObjectLockConfiguration
    let e = get_retention(&s3, "lockb", "a", Some(&vid))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        &s3s::S3ErrorCode::NoSuchObjectLockConfiguration,
        "{e:?}"
    );

    put_retention(
        &s3,
        "lockb",
        "a",
        Some(&vid),
        Some(retention("GOVERNANCE", future_ts(3600))),
        None,
    )
    .await
    .unwrap();

    // 带 versionId / 不带 versionId 都读得到
    for v in [Some(vid.as_str()), None] {
        let out = get_retention(&s3, "lockb", "a", v).await.unwrap();
        let r = out.retention.expect("必须有 Retention");
        assert_eq!(r.mode.as_ref().map(|m| m.as_str()), Some("GOVERNANCE"));
        assert!(r.retain_until_date.is_some());
    }
    // HEAD 响应头同步
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "lockb".into(),
            key: "a".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        head.object_lock_mode.as_ref().map(|m| m.as_str()),
        Some("GOVERNANCE")
    );
    assert!(head.object_lock_retain_until_date.is_some());

    // 空 Retention = 清除（GOVERNANCE 未到期 → 需 bypass）
    put_retention(
        &s3,
        "lockb",
        "a",
        Some(&vid),
        Some(ObjectLockRetention::default()),
        Some(true),
    )
    .await
    .unwrap();
    let e = get_retention(&s3, "lockb", "a", Some(&vid))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::NoSuchObjectLockConfiguration);
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "lockb".into(),
            key: "a".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(head.object_lock_mode.is_none());
}

/// 桶锁前置 / versionId 必填 / 参数校验。
#[tokio::test]
async fn retention_preconditions_and_validation() {
    let (s3, _d) = service().await;
    // 普通桶：Put/Get 都 400；桶不存在 404
    s3.create_bucket(req(CreateBucketInput {
        bucket: "plain".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "plain".into(),
        key: "a".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        ..Default::default()
    }))
    .await
    .unwrap();
    for e in [
        put_retention(
            &s3,
            "plain",
            "a",
            Some("null"),
            Some(retention("GOVERNANCE", future_ts(60))),
            None,
        )
        .await
        .unwrap_err(),
        get_retention(&s3, "plain", "a", None).await.unwrap_err(),
    ] {
        assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest, "{e:?}");
    }
    for e in [
        put_retention(
            &s3,
            "nob",
            "a",
            Some("v"),
            Some(retention("GOVERNANCE", future_ts(60))),
            None,
        )
        .await
        .unwrap_err(),
        get_retention(&s3, "nob", "a", None).await.unwrap_err(),
    ] {
        assert_eq!(e.code(), &s3s::S3ErrorCode::NoSuchBucket, "{e:?}");
    }

    // 锁桶：versionId 必填（AWS 允许 latest 的差异记档）；键/版本不存在
    lock_bucket(&s3, "lockb").await;
    let vid = put_obj(&s3, "lockb", "a", b"x").await;
    let e = put_retention(
        &s3,
        "lockb",
        "a",
        None,
        Some(retention("GOVERNANCE", future_ts(60))),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest, "{e:?}");
    assert_eq!(
        get_retention(&s3, "lockb", "missing", None)
            .await
            .unwrap_err()
            .code(),
        &s3s::S3ErrorCode::NoSuchKey
    );
    assert_eq!(
        get_retention(&s3, "lockb", "a", Some("missing"))
            .await
            .unwrap_err()
            .code(),
        &s3s::S3ErrorCode::NoSuchKey
    );

    // body 缺失 / 只有 mode / 过去日期 / 非法 mode → 400
    let bad: Vec<s3s::S3Error> = vec![
        put_retention(&s3, "lockb", "a", Some(&vid), None, None)
            .await
            .unwrap_err(),
        put_retention(
            &s3,
            "lockb",
            "a",
            Some(&vid),
            Some(ObjectLockRetention {
                mode: Some(ObjectLockRetentionMode::from_static("GOVERNANCE")),
                retain_until_date: None,
            }),
            None,
        )
        .await
        .unwrap_err(),
        put_retention(
            &s3,
            "lockb",
            "a",
            Some(&vid),
            Some(retention(
                "GOVERNANCE",
                xl_s3::lock::ts_from_nanos(now_nanos_i64() - 1_000_000_000),
            )),
            None,
        )
        .await
        .unwrap_err(),
        put_retention(
            &s3,
            "lockb",
            "a",
            Some(&vid),
            Some(retention("WORM", future_ts(60))),
            None,
        )
        .await
        .unwrap_err(),
    ];
    for e in bad {
        assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest, "{e:?}");
    }
}

/// COMPLIANCE：只能延长；缩短/清除/降级一律 403（bypass 无效）。
#[tokio::test]
async fn compliance_retention_only_extends() {
    let (s3, _d) = service().await;
    lock_bucket(&s3, "lockb").await;
    let vid = put_obj(&s3, "lockb", "c", b"x").await;
    put_retention(
        &s3,
        "lockb",
        "c",
        Some(&vid),
        Some(retention("COMPLIANCE", future_ts(3600))),
        None,
    )
    .await
    .unwrap();

    for bypass in [None, Some(true)] {
        let e = put_retention(
            &s3,
            "lockb",
            "c",
            Some(&vid),
            Some(retention("COMPLIANCE", future_ts(1800))),
            bypass,
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), &s3s::S3ErrorCode::AccessDenied, "缩短：{e:?}");
        let e = put_retention(
            &s3,
            "lockb",
            "c",
            Some(&vid),
            Some(ObjectLockRetention::default()),
            bypass,
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), &s3s::S3ErrorCode::AccessDenied, "清除：{e:?}");
        let e = put_retention(
            &s3,
            "lockb",
            "c",
            Some(&vid),
            Some(retention("GOVERNANCE", future_ts(7200))),
            bypass,
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), &s3s::S3ErrorCode::AccessDenied, "降级：{e:?}");
    }

    // 延长放行
    put_retention(
        &s3,
        "lockb",
        "c",
        Some(&vid),
        Some(retention("COMPLIANCE", future_ts(7200))),
        None,
    )
    .await
    .unwrap();
    let r = get_retention(&s3, "lockb", "c", Some(&vid))
        .await
        .unwrap()
        .retention
        .unwrap();
    assert_eq!(r.mode.as_ref().map(|m| m.as_str()), Some("COMPLIANCE"));
}

/// GOVERNANCE：缩短/清除不带 bypass → 403；带了 → 放行；延长无需 bypass。
#[tokio::test]
async fn governance_retention_bypass_matrix() {
    let (s3, _d) = service().await;
    lock_bucket(&s3, "lockb").await;
    let vid = put_obj(&s3, "lockb", "g", b"x").await;
    put_retention(
        &s3,
        "lockb",
        "g",
        Some(&vid),
        Some(retention("GOVERNANCE", future_ts(3600))),
        None,
    )
    .await
    .unwrap();

    let e = put_retention(
        &s3,
        "lockb",
        "g",
        Some(&vid),
        Some(retention("GOVERNANCE", future_ts(1800))),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::AccessDenied, "{e:?}");
    put_retention(
        &s3,
        "lockb",
        "g",
        Some(&vid),
        Some(retention("GOVERNANCE", future_ts(1800))),
        Some(true),
    )
    .await
    .unwrap();

    let e = put_retention(
        &s3,
        "lockb",
        "g",
        Some(&vid),
        Some(ObjectLockRetention::default()),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::AccessDenied, "{e:?}");
    put_retention(
        &s3,
        "lockb",
        "g",
        Some(&vid),
        Some(ObjectLockRetention::default()),
        Some(true),
    )
    .await
    .unwrap();
    assert_eq!(
        get_retention(&s3, "lockb", "g", Some(&vid))
            .await
            .unwrap_err()
            .code(),
        &s3s::S3ErrorCode::NoSuchObjectLockConfiguration
    );

    // 延长（无 bypass）放行
    put_retention(
        &s3,
        "lockb",
        "g",
        Some(&vid),
        Some(retention("GOVERNANCE", future_ts(7200))),
        None,
    )
    .await
    .unwrap();
}

/// retention 与 legal hold 互不覆盖。
#[tokio::test]
async fn retention_and_legal_hold_are_independent() {
    let (s3, _d) = service().await;
    lock_bucket(&s3, "lockb").await;
    let vid = put_obj(&s3, "lockb", "h", b"x").await;
    // legal hold ON
    s3.put_object_legal_hold(req(PutObjectLegalHoldInput {
        bucket: "lockb".into(),
        key: "h".into(),
        version_id: Some(vid.clone()),
        legal_hold: Some(ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from_static("ON")),
        }),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 写 retention 不清 hold
    put_retention(
        &s3,
        "lockb",
        "h",
        Some(&vid),
        Some(retention("GOVERNANCE", future_ts(3600))),
        None,
    )
    .await
    .unwrap();
    let hold = s3
        .get_object_legal_hold(req(GetObjectLegalHoldInput {
            bucket: "lockb".into(),
            key: "h".into(),
            version_id: Some(vid.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        hold.legal_hold
            .and_then(|h| h.status)
            .map(|s| s.as_str().to_string()),
        Some("ON".into()),
        "写 retention 不得清 legal hold"
    );
    // 清 retention（bypass）不动 hold
    put_retention(
        &s3,
        "lockb",
        "h",
        Some(&vid),
        Some(ObjectLockRetention::default()),
        Some(true),
    )
    .await
    .unwrap();
    let hold = s3
        .get_object_legal_hold(req(GetObjectLegalHoldInput {
            bucket: "lockb".into(),
            key: "h".into(),
            version_id: Some(vid),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        hold.legal_hold
            .and_then(|h| h.status)
            .map(|s| s.as_str().to_string()),
        Some("ON".into()),
        "清 retention 不得清 legal hold"
    );
}

/// 版本化读：target 版本有 retention，latest 无；marker 版本拒绝。
#[tokio::test]
async fn retention_is_per_version() {
    let (s3, _d) = service().await;
    lock_bucket(&s3, "lockb").await;
    let v1 = put_obj(&s3, "lockb", "v", b"one").await;
    put_retention(
        &s3,
        "lockb",
        "v",
        Some(&v1),
        Some(retention("GOVERNANCE", future_ts(3600))),
        None,
    )
    .await
    .unwrap();
    let v2 = put_obj(&s3, "lockb", "v", b"two").await;

    assert_eq!(
        get_retention(&s3, "lockb", "v", Some(&v1))
            .await
            .unwrap()
            .retention
            .unwrap()
            .mode
            .as_ref()
            .map(|m| m.as_str()),
        Some("GOVERNANCE")
    );
    assert_eq!(
        get_retention(&s3, "lockb", "v", None)
            .await
            .unwrap_err()
            .code(),
        &s3s::S3ErrorCode::NoSuchObjectLockConfiguration,
        "latest（v2）无 retention"
    );
    let _ = v2;

    // marker 版本：GET/PUT 都 400 InvalidArgument
    let out = s3
        .delete_object(req(DeleteObjectInput {
            bucket: "lockb".into(),
            key: "v".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let marker = out.version_id.expect("版本化桶删除回 marker id");
    let e = get_retention(&s3, "lockb", "v", Some(&marker))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument, "{e:?}");
    let e = put_retention(
        &s3,
        "lockb",
        "v",
        Some(&marker),
        Some(retention("GOVERNANCE", future_ts(60))),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument, "{e:?}");
    // 无 versionId 读 latest = marker → NoSuchKey
    assert_eq!(
        get_retention(&s3, "lockb", "v", None)
            .await
            .unwrap_err()
            .code(),
        &s3s::S3ErrorCode::NoSuchKey
    );
}
