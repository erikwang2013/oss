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
    // body 真身：把 StreamingBlob 收完，钉住字节而非仅长度
    let mut body = got.body.unwrap();
    let mut buf = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut body).await {
        buf.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(buf, b"hello s3");

    s3.delete_object(req(DeleteObjectInput {
        bucket: "alpha".into(),
        key: "a/b.txt".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 规格 §3.3 主用例：删后 head 报 NoSuchKey（404）
    let err = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "a/b.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchKey);
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
    // 批量确实删掉了（幂等删除不存在的键不足以证明）
    for k in ["k1", "k2"] {
        assert!(
            s3.head_object(req(HeadObjectInput {
                bucket: "alpha".into(),
                key: k.into(),
                ..Default::default()
            }))
            .await
            .is_err(),
            "{k} 已被批量删除"
        );
    }
}

#[tokio::test]
async fn delete_objects_reports_per_key_errors() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    for k in ["ok1", "ok2"] {
        s3.put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: k.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            ..Default::default()
        }))
        .await
        .unwrap();
    }

    // 混批：合法键 + 非法键（a//b 含空路径段）。§3.4：部分失败不回滚，
    // 成功的进 deleted、失败的进 errors。
    let out = s3
        .delete_objects(req(DeleteObjectsInput::builder()
            .bucket("alpha".to_string())
            .delete(Delete {
                objects: vec![
                    ObjectIdentifier {
                        key: "ok1".into(),
                        ..Default::default()
                    },
                    ObjectIdentifier {
                        key: "a//b".into(),
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

    let deleted: Vec<String> = out
        .deleted
        .unwrap()
        .into_iter()
        .filter_map(|d| d.key)
        .collect();
    assert_eq!(deleted, vec!["ok1".to_string()]);

    let errors = out.errors.unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].key.as_deref(), Some("a//b"));
    assert_eq!(errors[0].code.as_deref(), Some("InvalidArgument"));

    assert!(
        s3.head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "ok1".into(),
            ..Default::default()
        }))
        .await
        .is_err(),
        "合法键已删"
    );
    s3.head_object(req(HeadObjectInput {
        bucket: "alpha".into(),
        key: "ok2".into(),
        ..Default::default()
    }))
    .await
    .expect("非法键失败不影响同批其他键");
}

#[tokio::test]
async fn delete_objects_rejects_more_than_1000_keys() {
    let (s3, _d) = service().await;
    let objects: Vec<ObjectIdentifier> = (0..=1000)
        .map(|i| ObjectIdentifier {
            key: format!("k{i}"),
            ..Default::default()
        })
        .collect();
    let err = s3
        .delete_objects(req(DeleteObjectsInput::builder()
            .bucket("alpha".to_string())
            .delete(Delete {
                objects,
                ..Default::default()
            })
            .build()
            .unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn put_object_max_put_bytes_boundary() {
    let (s3, _d) = service().await; // 上限 1 MiB
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let limit = 1024 * 1024;

    // 恰 1 MiB：通过（判定严格大于，且在 append 之前）
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "at-limit".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from(vec![0u8; limit]))),
        ..Default::default()
    }))
    .await
    .unwrap();

    let err = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "over-limit".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from(vec![0u8; limit + 1]))),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::EntityTooLarge);
}
