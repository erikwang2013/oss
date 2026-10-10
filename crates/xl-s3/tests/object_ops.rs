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

    // 混批：合法键 + 非法键（a//b 含空路径段）+ 又一个合法键。§3.4：部分失败不回滚，
    // 成功的进 deleted、失败的进 errors，且错误之后的键继续处理。
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
                    ObjectIdentifier {
                        key: "ok2".into(),
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
    assert_eq!(deleted, vec!["ok1".to_string(), "ok2".to_string()]);

    let errors = out.errors.unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].key.as_deref(), Some("a//b"));
    assert_eq!(errors[0].code.as_deref(), Some("InvalidArgument"));

    // ok1 在非法键之前、ok2 在非法键之后：都真删了才钉住「错误后继续」且不回滚
    for k in ["ok1", "ok2"] {
        assert!(
            s3.head_object(req(HeadObjectInput {
                bucket: "alpha".into(),
                key: k.into(),
                ..Default::default()
            }))
            .await
            .is_err(),
            "{k} 已删"
        );
    }
}

#[tokio::test]
async fn delete_objects_accepts_exactly_1000_keys() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    // 上限是「一次最多 1000 个」：恰 1000 必须通过（>1000 的拒绝用例见上）
    let objects: Vec<ObjectIdentifier> = (0..1000)
        .map(|i| ObjectIdentifier {
            key: format!("k{i}"),
            ..Default::default()
        })
        .collect();
    let out = s3
        .delete_objects(req(DeleteObjectsInput::builder()
            .bucket("alpha".to_string())
            .delete(Delete {
                objects,
                ..Default::default()
            })
            .build()
            .unwrap()))
        .await
        .unwrap()
        .output;
    assert_eq!(out.deleted.unwrap().len(), 1000);
    assert!(out.errors.is_none());
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

#[tokio::test]
async fn list_objects_v2_prefix_delimiter_and_paging() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    for k in ["a.txt", "docs/1.txt", "docs/2.txt", "docs/deep/3.txt"] {
        s3.put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: k.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            ..Default::default()
        }))
        .await
        .unwrap();
    }

    // prefix + delimiter：docs/ 下的对象返回、更深的目录聚合成 CommonPrefixes
    let out = s3
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "alpha".into(),
            prefix: Some("docs/".into()),
            delimiter: Some("/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let keys: Vec<String> = out
        .contents
        .as_ref()
        .unwrap()
        .iter()
        .filter_map(|o| o.key.clone())
        .collect();
    assert_eq!(
        keys,
        vec!["docs/1.txt".to_string(), "docs/2.txt".to_string()]
    );
    assert_eq!(
        out.common_prefixes.unwrap()[0].prefix.as_deref(),
        Some("docs/deep/")
    );
    assert_eq!(out.key_count, Some(2));
    assert_eq!(out.is_truncated, Some(false));

    // 分页：max-keys=2 截断，跟着 token 取下一页，两页拼回全量（无重无漏）
    let page = s3
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "alpha".into(),
            max_keys: Some(2),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(page.is_truncated, Some(true));
    assert_eq!(page.key_count, Some(2));
    let token = page.next_continuation_token.clone().unwrap();
    let mut all: Vec<String> = page
        .contents
        .unwrap()
        .into_iter()
        .filter_map(|o| o.key)
        .collect();

    let page2 = s3
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "alpha".into(),
            max_keys: Some(2),
            continuation_token: Some(token),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(page2.is_truncated, Some(false));
    assert_eq!(page2.next_continuation_token, None);
    all.extend(page2.contents.unwrap().into_iter().filter_map(|o| o.key));
    assert_eq!(
        all,
        vec![
            "a.txt".to_string(),
            "docs/1.txt".to_string(),
            "docs/2.txt".to_string(),
            "docs/deep/3.txt".to_string(),
        ]
    );
}

#[tokio::test]
async fn list_objects_v2_rejects_negative_max_keys() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    // i32 → usize 的 `as` 对负数会回绕成巨值，再被 store 的 .min(1000) 钳成 1000 ——
    // 非法参数被静默当默认值。口径：max-keys < 0 → InvalidRequest。
    let err = s3
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "alpha".into(),
            max_keys: Some(-1),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn copy_object_copies_bytes() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "src".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"copy"))),
        ..Default::default()
    }))
    .await
    .unwrap();

    // CopyObjectInput 没有 Default；copy_source 是必填枚举（s3s 已解码并去掉前导 '/'）
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("alpha".to_string())
        .key("dst".to_string())
        .copy_source(CopySource::Bucket {
            bucket: "alpha".into(),
            key: "src".into(),
            version_id: None,
        })
        .build()
        .unwrap()))
        .await
        .unwrap();

    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "dst".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(got.content_length, Some(4));
    // body 真身：钉住字节而非仅长度
    let mut body = got.body.unwrap();
    let mut buf = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut body).await {
        buf.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(buf, b"copy");
}

#[tokio::test]
async fn copy_object_rejects_same_source_and_destination() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "src".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        ..Default::default()
    }))
    .await
    .unwrap();

    // 规格 §3.5 边界：M1 无 metadata-directive REPLACE / 存储类变更概念，
    // src == dst（同桶同键）一律 InvalidRequest（S3 语义）
    let err = s3
        .copy_object(req(CopyObjectInput::builder()
            .bucket("alpha".to_string())
            .key("src".to_string())
            .copy_source(CopySource::Bucket {
                bucket: "alpha".into(),
                key: "src".into(),
                version_id: None,
            })
            .build()
            .unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
}
