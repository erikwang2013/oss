// crates/xl-s3/tests/bucket_ops.rs
use std::sync::Arc;

use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

async fn service() -> (OssS3, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    (OssS3::new(store, 1024 * 1024), dir)
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

    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
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
    let names: Vec<String> = out
        .buckets
        .unwrap()
        .into_iter()
        .filter_map(|b| b.name)
        .collect();
    assert_eq!(names, vec!["alpha".to_string()]);

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
