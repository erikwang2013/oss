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
        "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL);\
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
