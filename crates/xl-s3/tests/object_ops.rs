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
    (OssS3::new(store, acl().await, 1024 * 1024), dir)
}

/// 最小判权库（这些用例直调 service 方法、不走 s3s 授权链；Acl 只被记账路径用到）。
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
    // 元数据字段取有区分力的值：size 与 ETag 都不是占位
    let first = &out.contents.as_ref().unwrap()[0];
    assert_eq!(first.size, Some(1));
    let expected_md5 = {
        use md5::{Digest as _, Md5};
        let mut h = Md5::new();
        h.update(b"x");
        hex::encode(h.finalize())
    };
    assert_eq!(
        first.e_tag.as_ref().map(ETag::value),
        Some(expected_md5.as_str())
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
    let copy_out = s3
        .copy_object(req(CopyObjectInput::builder()
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
        .unwrap()
        .output;
    let copied_lm = copy_out
        .copy_object_result
        .unwrap()
        .last_modified
        .expect("CopyObjectResult 必须带 LastModified");

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

    // LastModified 必须是 dst 对象的真实 mtime（与 head 一致），不是 now() 冒充
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "dst".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.last_modified, Some(copied_lm));
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

#[tokio::test]
async fn copy_object_rejects_non_bucket_copy_source() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    // M1 只支持桶形式的 x-amz-copy-source；Access Point ARN 形式一律 InvalidRequest
    let err = s3
        .copy_object(req(CopyObjectInput::builder()
            .bucket("alpha".to_string())
            .key("dst".to_string())
            .copy_source(CopySource::AccessPoint {
                partition: "aws".into(),
                region: "us-east-1".into(),
                account_id: "123456789012".into(),
                access_point_name: "ap".into(),
                key: "src".into(),
                version_id: None,
            })
            .build()
            .unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn copy_object_source_missing_is_no_such_key() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let err = s3
        .copy_object(req(CopyObjectInput::builder()
            .bucket("alpha".to_string())
            .key("dst".to_string())
            .copy_source(CopySource::Bucket {
                bucket: "alpha".into(),
                key: "missing".into(),
                version_id: None,
            })
            .build()
            .unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchKey);
}

#[tokio::test]
async fn metadata_roundtrip_through_service() {
    // M2a Task 1：x-amz-meta-* 经 PutObjectInput.metadata → 内核 → GET/HEAD 输出回写
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let mut md = std::collections::HashMap::new();
    md.insert("foo".to_string(), "bar".to_string());
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        metadata: Some(md),
        ..Default::default()
    }))
    .await
    .unwrap();

    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        head.metadata.as_ref().and_then(|m| m.get("foo")),
        Some(&"bar".to_string())
    );

    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        got.metadata.as_ref().and_then(|m| m.get("foo")),
        Some(&"bar".to_string())
    );

    // 覆写不带元数据 = 清空（全量替换语义）
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"y"))),
        ..Default::default()
    }))
    .await
    .unwrap();
    let head2 = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(
        head2.metadata.is_none() || head2.metadata.as_ref().is_some_and(|m| m.is_empty()),
        "覆写后元数据应为空，实际: {:?}",
        head2.metadata
    );
}

#[tokio::test]
async fn get_object_range_returns_206_and_416() {
    // M2a Task 2：Range → 206 + Content-Range；越界 → 416 InvalidRange
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let content: Vec<u8> = (0u8..100).collect();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "r".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from(content.clone()))),
        ..Default::default()
    }))
    .await
    .unwrap();

    let resp = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "r".into(),
            range: Some(Range::Int {
                first: 10,
                last: Some(19),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(resp.status, Some(http::StatusCode::PARTIAL_CONTENT));
    assert_eq!(
        resp.output.content_range.as_deref(),
        Some("bytes 10-19/100")
    );
    assert_eq!(resp.output.content_length, Some(10));
    let mut body = resp.output.body.unwrap();
    let mut buf = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut body).await {
        buf.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(buf, &content[10..20]);

    // 语缀区间 bytes=-5 → 最后 5 字节
    let resp = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "r".into(),
            range: Some(Range::Suffix { length: 5 }),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        resp.output.content_range.as_deref(),
        Some("bytes 95-99/100")
    );

    // 越界 → 416 InvalidRange
    let err = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "r".into(),
            range: Some(Range::Int {
                first: 1000,
                last: None,
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRange);
}

/// 测试用可控分块流：先吐 `remaining` 个 `chunk`，再（可选）吐一个错误，然后结束。
struct Chunks {
    remaining: usize,
    chunk: Bytes,
    fail_at_end: bool,
    sent_err: bool,
}

impl futures::Stream for Chunks {
    type Item = Result<Bytes, s3s::StdError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        if self.remaining > 0 {
            self.remaining -= 1;
            return Poll::Ready(Some(Ok(self.chunk.clone())));
        }
        if self.fail_at_end && !self.sent_err {
            self.sent_err = true;
            return Poll::Ready(Some(Err(std::io::Error::other("boom").into())));
        }
        Poll::Ready(None)
    }
}

impl s3s::stream::ByteStream for Chunks {}

fn blob_from_chunks(n: usize, size: usize, fail_at_end: bool) -> StreamingBlob {
    StreamingBlob::from(Box::pin(Chunks {
        remaining: n,
        chunk: Bytes::from(vec![7u8; size]),
        fail_at_end,
        sent_err: false,
    }) as s3s::stream::DynByteStream)
}

#[tokio::test]
async fn put_object_streams_multi_chunk_body() {
    // M2a Task 3：多分块流式 PUT 直通内核（不再整份读进内存）
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    let s3 = OssS3::new(store, acl().await, 8 * 1024 * 1024);
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let chunk = 1024 * 1024; // 1 MiB × 3
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "big".into(),
        body: Some(blob_from_chunks(3, chunk, false)),
        ..Default::default()
    }))
    .await
    .unwrap();

    let got = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "big".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(got.content_length, Some((3 * chunk) as i64));
}

#[tokio::test]
async fn put_object_declared_oversize_is_413() {
    // 声明长度超限：不读体直接 413 EntityTooLarge（service() 的 max = 1 MiB）
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let err = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            content_length: Some(2 * 1024 * 1024),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::EntityTooLarge);
}

#[tokio::test]
async fn put_object_streaming_over_limit_is_413_and_leaves_no_residue() {
    // 未声明 content-length 的超限流：流上计数兜底 → 413；tmp 无残留（内核失败清理路径）
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    let s3 = OssS3::new(Arc::clone(&store), acl().await, 1024 * 1024);
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let err = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            body: Some(blob_from_chunks(3, 512 * 1024, false)), // 1.5 MiB > 1 MiB 上限
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::EntityTooLarge);
    assert!(
        s3.head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .is_err(),
        "超限 PUT 不得留下已提交对象"
    );
    let mut tmp = tokio::fs::read_dir(dir.path().join(".oss.sys/tmp"))
        .await
        .unwrap();
    assert!(
        tmp.next_entry().await.unwrap().is_none(),
        "失败 PUT 不得在 .oss.sys/tmp 留下残留"
    );
}

#[tokio::test]
async fn put_object_body_error_is_incomplete_body() {
    // 请求体读取失败（客户端断连语义）→ 4xx IncompleteBody，不是 500
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let err = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            body: Some(blob_from_chunks(1, 16, true)),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::IncompleteBody);
}

#[tokio::test]
async fn multipart_flow_through_service() {
    // M2a Task 5：create → list_parts → upload_part → complete → GET → abort(NoSuchUpload)
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let created = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            content_type: Some("text/plain".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let upload_id = created.upload_id.unwrap();

    // 单段小文件（末段 <5 MiB 允许）
    let up = s3
        .upload_part(req(UploadPartInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            upload_id: upload_id.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(
                b"multipart-hello",
            ))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let etag = up.e_tag.unwrap().value().to_string();

    let listed = s3
        .list_parts(req(ListPartsInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            upload_id: upload_id.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let listed = listed.parts.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].size, Some(15));

    let done = s3
        .complete_multipart_upload(req(CompleteMultipartUploadInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            upload_id: upload_id.clone(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(ETag::Strong(etag)),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(done.e_tag.unwrap().value().ends_with("-1"));

    // GET 回读内容
    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let mut body = got.body.unwrap();
    let mut buf = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut body).await {
        buf.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(buf, b"multipart-hello");

    // complete 后 abort → NoSuchUpload（mpu.meta 已转正为 done）
    let err = s3
        .abort_multipart_upload(req(AbortMultipartUploadInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            upload_id,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchUpload);
}

#[tokio::test]
async fn conditional_requests_follow_rfc9110() {
    // S3 协议补全 Task 1：If-Match / If-None-Match / If-Modified-Since / If-Unmodified-Since。
    // 条件对同一份 GET/HEAD 生效；304 无 body；412 = PreconditionFailed。
    use s3s::dto::{ETagCondition, IfMatch, IfModifiedSince, IfNoneMatch, IfUnmodifiedSince};

    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let put = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "cond".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"payload"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let etag = put.e_tag.unwrap().value().to_string();
    let info = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "cond".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let mtime = info.last_modified.unwrap();

    let get_with = |if_match: Option<IfMatch>,
                    if_none: Option<IfNoneMatch>,
                    if_modified: Option<IfModifiedSince>,
                    if_unmodified: Option<IfUnmodifiedSince>| {
        let input = GetObjectInput {
            bucket: "alpha".into(),
            key: "cond".into(),
            if_match,
            if_none_match: if_none,
            if_modified_since: if_modified,
            if_unmodified_since: if_unmodified,
            ..Default::default()
        };
        // 借用捕获：否则 async move 把 s3 搬进 future，闭包成 FnOnce（只能调用一次）
        let s3 = &s3;
        async move { s3.get_object(req(input)).await }
    };

    // ① If-Match 命中 → 200；错值 → 412
    let ok = get_with(
        Some(ETagCondition::ETag(ETag::Strong(etag.clone()))),
        None,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(ok.status, Some(http::StatusCode::OK), "命中 → 200");
    let err = get_with(
        Some(ETagCondition::ETag(ETag::Strong("deadbeef".into()))),
        None,
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::PreconditionFailed);

    // ② If-None-Match 命中 → 304、无 body
    let not_modified = get_with(
        None,
        Some(ETagCondition::ETag(ETag::Strong(etag.clone()))),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(not_modified.status, Some(http::StatusCode::NOT_MODIFIED));
    assert!(not_modified.output.body.is_none(), "304 不得带 body");

    // ③ If-None-Match: * 对已存在对象 → 304；④ If-Match: * → 200
    let star_304 = get_with(None, Some(ETagCondition::Any), None, None)
        .await
        .unwrap();
    assert_eq!(star_304.status, Some(http::StatusCode::NOT_MODIFIED));
    let star_200 = get_with(Some(ETagCondition::Any), None, None, None)
        .await
        .unwrap();
    assert_eq!(star_200.status, Some(http::StatusCode::OK));

    // ⑤ If-Modified-Since 拨到未来 → 304；If-Unmodified-Since 拨到过去 → 412
    let future =
        IfModifiedSince::from(std::time::SystemTime::now() + std::time::Duration::from_secs(3600));
    let past = IfUnmodifiedSince::from(
        std::time::SystemTime::now() - std::time::Duration::from_secs(3600),
    );
    assert_eq!(
        get_with(None, None, Some(future), None)
            .await
            .unwrap()
            .status,
        Some(http::StatusCode::NOT_MODIFIED)
    );
    assert_eq!(
        get_with(None, None, None, Some(past))
            .await
            .unwrap_err()
            .code(),
        &s3s::S3ErrorCode::PreconditionFailed
    );
    // 反对照：If-Modified-Since 拨到过去 → 200（正常服务）
    let old = IfModifiedSince::from(
        std::time::SystemTime::now() - std::time::Duration::from_secs(3600 * 24 * 365),
    );
    assert_eq!(
        get_with(None, None, Some(old), None).await.unwrap().status,
        Some(http::StatusCode::OK)
    );
    // 对象 mtime 必然 ≤ 现在：If-Unmodified-Since = mtime 本身 → 条件成立（<= 语义）
    // (mtime 即 Timestamp；IfUnmodifiedSince 是同类型别名，直接传)
    assert_eq!(
        get_with(None, None, None, Some(mtime))
            .await
            .unwrap()
            .status,
        Some(http::StatusCode::OK)
    );

    // ⑥ 条件先于 Range：If-None-Match 命中 + Range → 304（不是 206）
    let with_range = GetObjectInput {
        bucket: "alpha".into(),
        key: "cond".into(),
        if_none_match: Some(ETagCondition::ETag(ETag::Strong(etag.clone()))),
        range: Some(Range::Int {
            first: 0,
            last: Some(2),
        }),
        ..Default::default()
    };
    let resp = s3.get_object(req(with_range)).await.unwrap();
    assert_eq!(resp.status, Some(http::StatusCode::NOT_MODIFIED));

    // ⑦ HEAD 同口径：If-Match 错值 → 412；If-None-Match 命中 → 304
    let head_err = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "cond".into(),
            if_match: Some(ETagCondition::ETag(ETag::Strong("deadbeef".into()))),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(head_err.code(), &s3s::S3ErrorCode::PreconditionFailed);
    let head_304 = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "cond".into(),
            if_none_match: Some(ETagCondition::ETag(ETag::Strong(etag))),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(head_304.status, Some(http::StatusCode::NOT_MODIFIED));
}

#[tokio::test]
async fn response_content_params_override_headers() {
    // S3 协议补全 Task 2：response-content-* 查询参数覆盖响应头（含 response-expires 的 HttpDate）
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "rc".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        content_type: Some("text/plain".into()),
        ..Default::default()
    }))
    .await
    .unwrap();

    // 未带参数：对象自有 content_type 原样，覆盖字段为 None
    let plain = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "rc".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(plain.content_type.as_deref(), Some("text/plain"));
    assert!(plain.content_disposition.is_none());

    // 全带：逐字段覆盖（filename 的 RFC 5987 编码由客户端负责，服务端原样透传）
    let expires = s3s::dto::Timestamp::from(
        std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
    );
    let over = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "rc".into(),
            response_content_type: Some("text/x-custom".into()),
            response_content_disposition: Some("attachment; filename*=UTF-8''%E6%B5%8B.txt".into()),
            response_cache_control: Some("no-store".into()),
            response_content_language: Some("zh-CN".into()),
            response_content_encoding: Some("identity".into()),
            response_expires: Some(expires),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(over.content_type.as_deref(), Some("text/x-custom"));
    assert_eq!(
        over.content_disposition.as_deref(),
        Some("attachment; filename*=UTF-8''%E6%B5%8B.txt")
    );
    assert_eq!(over.cache_control.as_deref(), Some("no-store"));
    assert_eq!(over.content_language.as_deref(), Some("zh-CN"));
    assert_eq!(over.content_encoding.as_deref(), Some("identity"));
    // HttpDate 形态：形如 "Sun, 12 Oct 2026 05:00:00 GMT"
    let exp = over.expires.expect("expires 应被设置");
    assert!(exp.ends_with("GMT"), "HttpDate 形态: {exp}");
}

#[tokio::test]
async fn checksum_verified_before_commit_and_written_back() {
    // S3 协议补全 Task 3：x-amz-checksum-* / Content-MD5 —— 提交前校验（fail-closed）+ GET/HEAD 回写。
    // 期望值用 s3s 自己的 ChecksumHasher 现算（免引 base64 依赖，也顺带对拍线格式）。
    use s3s::checksum::ChecksumHasher;

    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let body = b"payload-123";
    let mut h = ChecksumHasher {
        sha256: Some(Default::default()),
        md5: Some(Default::default()),
        ..Default::default()
    };
    h.update(body);
    let sums = h.finalize();
    let sha256_b64 = sums.checksum_sha256.clone().unwrap();
    let md5_b64 = sums.checksum_md5.clone().unwrap();

    // ① 正确 SHA256 → 提交成功，GET/HEAD 回写同一 base64
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "ck".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
        checksum_sha256: Some(sha256_b64.clone()),
        checksum_algorithm: Some("SHA256".to_string().into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "ck".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.checksum_sha256.as_deref(), Some(sha256_b64.as_str()));
    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "ck".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(got.checksum_sha256.as_deref(), Some(sha256_b64.as_str()));

    // ② 错值 → BadDigest，且对象**不存在**（提交点前 fail-closed）
    let err = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "bad".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
            checksum_sha256: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::BadDigest);
    assert!(
        s3.head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "bad".into(),
            ..Default::default()
        }))
        .await
        .is_err(),
        "校验失败不得留下对象"
    );

    // ③ Content-MD5：正确提交 + 回写 checksum_md5；错值 BadDigest
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "md5".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
        content_md5: Some(md5_b64.clone()),
        ..Default::default()
    }))
    .await
    .unwrap();
    let head_md5 = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "md5".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head_md5.checksum_md5.as_deref(), Some(md5_b64.as_str()));
    let err_md5 = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "md5bad".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
            content_md5: Some("AAAAAAAAAAAAAAAAAAAAAA==".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err_md5.code(), &s3s::S3ErrorCode::BadDigest);

    // ④ 多个校验和并存 → InvalidRequest（明确拒绝，不静默取一）
    let err_conflict = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "conflict".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
            content_md5: Some(md5_b64.clone()),
            checksum_sha256: Some(sha256_b64.clone()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err_conflict.code(), &s3s::S3ErrorCode::InvalidRequest);

    // ⑤ 只声明算法无值 → InvalidRequest
    let err_algo_only = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "algonly".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
            checksum_algorithm: Some("SHA256".to_string().into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err_algo_only.code(), &s3s::S3ErrorCode::InvalidRequest);

    // ⑥ 同值双声明（content_md5 与 x-amz-checksum-md5 同值）→ 视为一份，放行
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "dup".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
        content_md5: Some(md5_b64.clone()),
        checksum_md5: Some(md5_b64.clone()),
        ..Default::default()
    }))
    .await
    .unwrap();

    // ⑦ 206 分段响应**不得**回整对象校验和（SDK 响应校验会拿片段验整对象而失败）
    let ranged = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "ck".into(),
            range: Some(Range::Int {
                first: 0,
                last: Some(3),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(ranged.status, Some(http::StatusCode::PARTIAL_CONTENT));
    assert!(
        ranged.output.checksum_sha256.is_none(),
        "206 不得回写整对象校验和: {:?}",
        ranged.output.checksum_sha256
    );
}

#[tokio::test]
async fn head_with_range_still_returns_full_length() {
    // 协议补全 Task 4：HEAD 带 Range **不支持**（钉契约防未来静默变化）——
    // 仍回 200 + 全量 Content-Length（aws-cli/rclone 均不依赖 HEAD+Range 的 206 语义）。
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "hr".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from(vec![b'x'; 100]))),
        ..Default::default()
    }))
    .await
    .unwrap();
    let resp = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "hr".into(),
            range: Some(Range::Int {
                first: 0,
                last: Some(9),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    // HEAD 路径用 S3Response::new（status None = 隐式 200；GET 才统一 with_status）
    assert_eq!(resp.status, None);
    assert_eq!(
        resp.output.content_length,
        Some(100),
        "HEAD+Range 仍回全量长度"
    );
}
