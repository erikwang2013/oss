// crates/xl-s3/tests/bucket_ops.rs
use std::sync::Arc;

use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

/// 最小判权库（这些用例直调 service 方法、不走 s3s 授权链；Acl 只被
/// create/delete_bucket 的记账路径用到）。
async fn acl() -> xl_s3::access::Acl {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, versioning TEXT NOT NULL DEFAULT 'Off');\
         CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
          can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, can_delete INTEGER NOT NULL DEFAULT 0, \
          PRIMARY KEY (user_id, bucket));",
    )
    .execute(&pool)
    .await
    .unwrap();
    xl_s3::access::Acl::new(pool)
}

async fn service() -> (OssS3, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    (OssS3::new(store, acl().await, 1024 * 1024), dir)
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

#[tokio::test]
async fn create_head_list_delete_bucket() {
    let (s3, _d) = service().await;

    // creation_date 必须是真实建桶时间，不是 list 时现填的 now()：判别点是紧上界——
    // 取值须早于 list 之前捕获的 before_list，now() 冒充必然晚于它而失败。
    // 下界放 50ms 容差：文件时间戳取自内核粗粒度时钟，实测滞后约一个 tick
    //（本机 ~1ms，CLK_TCK=100 的机器上限 10ms），无容差会假阳性。
    let before_create = std::time::SystemTime::now();
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let before_list = CreationDate::from(std::time::SystemTime::now());
    s3.head_bucket(req(HeadBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let out = s3
        .list_buckets(req(ListBucketsInput::default()))
        .await
        .unwrap()
        .output;
    let buckets = out.buckets.unwrap();
    let names: Vec<String> = buckets.iter().filter_map(|b| b.name.clone()).collect();
    assert_eq!(names, vec!["alpha".to_string()]);

    let created = buckets
        .into_iter()
        .find(|b| b.name.as_deref() == Some("alpha"))
        .and_then(|b| b.creation_date)
        .unwrap();
    let tolerance = std::time::Duration::from_millis(50);
    assert!(
        CreationDate::from(before_create - tolerance) <= created && created <= before_list,
        "creation_date 应为真实建桶时间（介于建桶前与 list 前之间）：\
         before_create={:?} created={created:?} before_list={before_list:?}",
        CreationDate::from(before_create)
    );

    s3.delete_bucket(req(DeleteBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert!(
        s3.head_bucket(req(HeadBucketInput {
            bucket: "alpha".into(),
            ..Default::default()
        }))
        .await
        .is_err()
    );
}

#[tokio::test]
async fn head_missing_bucket_is_no_such_bucket() {
    let (s3, _d) = service().await;
    let err = s3
        .head_bucket(req(HeadBucketInput {
            bucket: "ghost".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchBucket);
}

/// M4 切片 3：版本控制开关——开启幂等；**已启用后拒绝改回 Off**（写入会整链替换 → 静默丢历史）。
/// Suspended 亦拒绝（语义未实现，明确 501 不静默）。
#[tokio::test]
async fn versioning_enabled_and_off_rejected() {
    // 带身份建桶（record_bucket_owner 需要 AuthedUser 扩展——版本控制状态存在 buckets 行上）
    fn with_user<T>(input: T) -> S3Request<T> {
        let mut r = req(input);
        r.extensions.insert(xl_s3::access::AuthedUser {
            uid: 1,
            is_admin: true,
        });
        r
    }
    let (s3, _d) = service().await;
    s3.create_bucket(with_user(CreateBucketInput {
        bucket: "vbk".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let cfg = |st: &'static str| {
        req(PutBucketVersioningInput {
            bucket: "vbk".into(),
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            mfa: None,
            versioning_configuration: VersioningConfiguration {
                mfa_delete: None,
                status: Some(BucketVersioningStatus::from_static(st)),
            },
        })
    };
    s3.put_bucket_versioning(cfg("Enabled")).await.unwrap();
    let got = s3
        .get_bucket_versioning(req(GetBucketVersioningInput {
            bucket: "vbk".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        got.output.status.as_ref().map(|s| s.as_str()),
        Some("Enabled")
    );
    // 已启用 → Off 拒绝（501；此前会静默接受 → 下一次写丢整链）
    let err = s3.put_bucket_versioning(cfg("Off")).await.unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NotImplemented);
    // 被拒后桶状态必须不变（拒在 set 之前）
    let got = s3
        .get_bucket_versioning(req(GetBucketVersioningInput {
            bucket: "vbk".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        got.output.status.as_ref().map(|s| s.as_str()),
        Some("Enabled"),
        "Off 被拒后仍应 Enabled"
    );
    let err = s3
        .put_bucket_versioning(cfg("Suspended"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NotImplemented);
    // 未启用的桶：Off 幂等接受
    s3.create_bucket(with_user(CreateBucketInput {
        bucket: "vbk2".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_bucket_versioning(req(PutBucketVersioningInput {
        bucket: "vbk2".into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        mfa: None,
        versioning_configuration: VersioningConfiguration {
            mfa_delete: None,
            status: Some(BucketVersioningStatus::from_static("Off")),
        },
    }))
    .await
    .unwrap();
}
