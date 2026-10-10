// crates/xl-s3/tests/object_ops.rs
use std::sync::Arc;

use bytes::Bytes;
use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

// helper 与 bucket_ops.rs 有意各留一份：计划未要求共用模块（Task 12 继续往本文件追加）。
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
async fn put_head_get_delete() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let body = StreamingBlob::from_bytes(Bytes::from_static(b"hello s3"));
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "a/b.txt".into(),
        body: Some(body),
        content_type: Some("text/plain".into()),
        ..Default::default()
    }))
    .await
    .unwrap();

    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "a/b.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.content_length, Some(8));
    // ETag 必须是整份 MD5 —— 测试里现算，不写死
    let expected_md5 = {
        use md5::{Digest as _, Md5};
        let mut h = Md5::new();
        h.update(b"hello s3");
        hex::encode(h.finalize())
    };
    assert_eq!(
        head.e_tag.as_ref().map(ETag::value),
        Some(expected_md5.as_str())
    );

    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "a/b.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(got.content_length, Some(8));

    s3.delete_object(req(DeleteObjectInput {
        bucket: "alpha".into(),
        key: "a/b.txt".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert!(
        s3.head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "a/b.txt".into(),
            ..Default::default()
        }))
        .await
        .is_err()
    );
}

#[tokio::test]
async fn delete_objects_is_batch_and_idempotent() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    for k in ["k1", "k2"] {
        s3.put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: k.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            ..Default::default()
        }))
        .await
        .unwrap();
    }
    // DeleteObjectsInput 没有 Default，用生成 builder 构造
    let out = s3
        .delete_objects(req(DeleteObjectsInput::builder()
            .bucket("alpha".to_string())
            .delete(Delete {
                objects: vec![
                    ObjectIdentifier {
                        key: "k1".into(),
                        ..Default::default()
                    },
                    ObjectIdentifier {
                        key: "k2".into(),
                        ..Default::default()
                    },
                    ObjectIdentifier {
                        key: "never-existed".into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            })
            .build()
            .unwrap()))
        .await
        .unwrap()
        .output;
    assert!(out.errors.is_none(), "删除不存在的键不应报错（S3 语义）");
}
