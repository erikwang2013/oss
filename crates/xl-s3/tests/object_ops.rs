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

/// 带身份（生产路径 check() 恒塞 AuthedUser；批量删逐键判权依赖它）。
fn with_user<T>(input: T) -> S3Request<T> {
    let mut r = req(input);
    r.extensions.insert(xl_s3::access::AuthedUser {
        uid: 1,
        is_admin: true,
        ak: String::new(),
    });
    r
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
        .delete_objects(with_user(
            DeleteObjectsInput::builder()
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
                .unwrap(),
        ))
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
        .delete_objects(with_user(
            DeleteObjectsInput::builder()
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
                .unwrap(),
        ))
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
        .delete_objects(with_user(
            DeleteObjectsInput::builder()
                .bucket("alpha".to_string())
                .delete(Delete {
                    objects,
                    ..Default::default()
                })
                .build()
                .unwrap(),
        ))
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
        .delete_objects(with_user(
            DeleteObjectsInput::builder()
                .bucket("alpha".to_string())
                .delete(Delete {
                    objects,
                    ..Default::default()
                })
                .build()
                .unwrap(),
        ))
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
async fn list_objects_v1_empty_bucket_and_max_keys_zero() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let out = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.contents.as_ref().unwrap().is_empty());
    assert_eq!(out.is_truncated, Some(false));
    assert_eq!(out.max_keys, Some(1000)); // 未指定 → 默认 1000
    assert_eq!(out.next_marker, None);

    // max-keys=0：空页且不截断（不是被截断的首个空页）
    let out = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(0),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.contents.as_ref().unwrap().is_empty());
    assert_eq!(out.is_truncated, Some(false));
    assert_eq!(out.next_marker, None);
}

#[tokio::test]
async fn list_objects_v1_prefix_delimiter_marker_paging() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    for k in [
        "a.txt",
        "docs/1.txt",
        "docs/2.txt",
        "docs/deep/3.txt",
        "z.txt",
    ] {
        s3.put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: k.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            ..Default::default()
        }))
        .await
        .unwrap();
    }
    let keys = |out: &ListObjectsOutput| -> Vec<String> {
        out.contents
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|o| o.key.clone())
            .collect()
    };

    // prefix + delimiter（与 V2 同结果）
    let out = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            prefix: Some("docs/".into()),
            delimiter: Some("/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(keys(&out), ["docs/1.txt", "docs/2.txt"]);
    assert_eq!(
        out.common_prefixes.unwrap()[0].prefix.as_deref(),
        Some("docs/deep/")
    );

    // 无 delimiter：截断时 NextMarker 不返回，用最后一个 Contents 键续页
    let page1 = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(2),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(keys(&page1), ["a.txt", "docs/1.txt"]);
    assert_eq!(page1.is_truncated, Some(true));
    assert_eq!(page1.next_marker, None);
    let mut all = keys(&page1);

    let page2 = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(2),
            marker: Some("docs/1.txt".into()), // 排他：不得重复返回 marker 本身
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(keys(&page2), ["docs/2.txt", "docs/deep/3.txt"]);
    assert_eq!(page2.is_truncated, Some(true));
    all.extend(keys(&page2));

    let page3 = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(2),
            marker: Some("docs/deep/3.txt".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(!page3.is_truncated.unwrap());
    all.extend(keys(&page3));
    assert_eq!(
        all,
        [
            "a.txt",
            "docs/1.txt",
            "docs/2.txt",
            "docs/deep/3.txt",
            "z.txt"
        ]
    );

    // 带 delimiter：NextMarker 返回最后已返回输出；跟随后不重复 cp
    let p1 = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(1),
            delimiter: Some("/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(keys(&p1), ["a.txt"]);
    assert_eq!(p1.next_marker.as_deref(), Some("a.txt"));

    let p2 = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(1),
            delimiter: Some("/".into()),
            marker: p1.next_marker.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(keys(&p2).is_empty());
    assert_eq!(
        p2.common_prefixes.as_ref().unwrap()[0].prefix.as_deref(),
        Some("docs/")
    );
    assert_eq!(p2.next_marker.as_deref(), Some("docs/"));

    let p3 = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(1),
            delimiter: Some("/".into()),
            marker: p2.next_marker.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(keys(&p3), ["z.txt"]);
    assert_eq!(p3.common_prefixes.unwrap().len(), 0);
    assert_eq!(p3.is_truncated, Some(false));
}

#[tokio::test]
async fn list_objects_v1_encoding_type_url() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    for k in ["a b.txt", "c(1).png", "中文.txt"] {
        s3.put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: k.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            ..Default::default()
        }))
        .await
        .unwrap();
    }

    let out = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let keys: Vec<String> = out
        .contents
        .unwrap()
        .into_iter()
        .filter_map(|o| o.key)
        .collect();
    assert_eq!(
        keys,
        ["a%20b.txt", "c%281%29.png", "%E4%B8%AD%E6%96%87.txt"]
    );
    assert_eq!(
        out.encoding_type.as_ref().map(EncodingType::as_str),
        Some("url")
    );

    // prefix / delimiter 同样按 encoding-type=url 编码
    let out = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            prefix: Some("中文".into()),
            delimiter: Some("/".into()),
            encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.prefix.as_deref(), Some("%E4%B8%AD%E6%96%87"));
    assert_eq!(out.delimiter.as_deref(), Some("%2F"));
}

#[tokio::test]
async fn list_objects_v1_caps_at_1000_keys_and_pages() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    store.create_bucket("alpha").await.unwrap();
    for i in 0..1001 {
        store
            .put_object("alpha", &format!("k{i:04}"), Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }
    let s3 = OssS3::new(store, acl().await, 1024 * 1024);

    // max-keys=1001：按 S3 上限钳到 1000（响应回显实际生效值）
    let page1 = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(1001),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(page1.max_keys, Some(1000));
    assert_eq!(page1.contents.as_ref().unwrap().len(), 1000);
    assert_eq!(page1.is_truncated, Some(true));
    assert_eq!(page1.next_marker, None);
    let last = page1.contents.as_ref().unwrap().last().unwrap().key.clone();

    let page2 = s3
        .list_objects(req(ListObjectsInput {
            bucket: "alpha".into(),
            max_keys: Some(1000),
            marker: last,
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(page2.contents.as_ref().unwrap().len(), 1);
    assert_eq!(
        page2.contents.as_ref().unwrap()[0].key.as_deref(),
        Some("k1000")
    );
    assert_eq!(page2.is_truncated, Some(false));
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
async fn multipart_create_tagging_lands_and_rejects_oversize() {
    // M8 缺口 A：Create 的 x-amz-tagging 在 complete 落地；>10 条在 Create 即拒
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
            key: "mpt".into(),
            tagging: Some("env=prod&team=core".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let upload_id = created.upload_id.unwrap();
    let up = s3
        .upload_part(req(UploadPartInput {
            bucket: "alpha".into(),
            key: "mpt".into(),
            upload_id: upload_id.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"t"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let etag = up.e_tag.unwrap().value().to_string();
    s3.complete_multipart_upload(req(CompleteMultipartUploadInput {
        bucket: "alpha".into(),
        key: "mpt".into(),
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
    .unwrap();
    let got = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "alpha".into(),
            key: "mpt".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let mut pairs: Vec<(String, String)> = got
        .tag_set
        .into_iter()
        .map(|t| (t.key.unwrap(), t.value.unwrap()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "core".to_string())
        ]
    );

    // 11 条 → InvalidTag；暂存区不创建
    let oversize = (0..11)
        .map(|i| format!("k{i}=v"))
        .collect::<Vec<_>>()
        .join("&");
    let err = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "alpha".into(),
            key: "mpt-bad".into(),
            tagging: Some(oversize),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidTag);
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

// ---------- M5 切片 3：SSE 线级（P3-c 复验补——P2-a 类接线错此前无闸能抓） ----------

/// 加密接线版 service（保留 store 句柄以 init_encryption；SSE-C 用例不需要 provider）。
async fn service_enc() -> (OssS3, Arc<Store>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    let sys = xl_store::layout::sys_dir(dir.path(), "");
    store
        .init_encryption(Some(sys.join("master.key")), Some(sys.join("kms")))
        .await
        .unwrap();
    (
        OssS3::new(store.clone(), acl().await, 16 * 1024 * 1024),
        store,
        dir,
    )
}

#[tokio::test]
async fn sse_c_wire_roundtrip_and_headers() {
    use base64::Engine as _;
    let (s3, _store, _d) = service_enc().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let key = [7u8; 32];
    let k = xl_store::crypto::SseCKey::from_key(key);
    let b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5 = k.md5_b64().to_string();

    // PUT 带 SSE-C：响应回 algorithm + key-MD5（P2-a 钉死）
    let out = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "c".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(
                b"hello-sse-c",
            ))),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64.clone()),
            sse_customer_key_md5: Some(md5.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.sse_customer_algorithm.as_deref(), Some("AES256"));
    assert_eq!(
        out.sse_customer_key_md5.as_deref(),
        Some(md5.as_str()),
        "PUT 响应须回 key-MD5（P2-a）"
    );
    assert!(out.server_side_encryption.is_none(), "SSE-C 不回 SSE 头");

    // GET 带钥：algorithm 字段 + key-MD5 响应头（R6 with_headers 补）
    let g = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "c".into(),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64.clone()),
            sse_customer_key_md5: Some(md5.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(g.output.sse_customer_algorithm.as_deref(), Some("AES256"));
    assert!(g.output.server_side_encryption.is_none());
    assert_eq!(
        g.headers
            .get("x-amz-server-side-encryption-customer-key-md5")
            .unwrap(),
        md5.as_str()
    );

    // GET 缺钥 → 400 InvalidArgument
    let e = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "c".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        &s3s::S3ErrorCode::InvalidArgument,
        "缺钥应 400：{e:?}"
    );

    // PUT 错 md5 头 → 400
    let e = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "c2".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64.clone()),
            sse_customer_key_md5: Some("AAAAAAAAAAAAAAAAAAAAAAAA".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        &s3s::S3ErrorCode::InvalidArgument,
        "错 md5 头应 400：{e:?}"
    );

    // HEAD 带钥 ✓ / 缺钥 400
    let h = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "c".into(),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64.clone()),
            sse_customer_key_md5: Some(md5.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(h.output.content_length, Some(11));
    let e = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "c".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument);

    // MPU 全链（P2-a 同族：create/upload/complete 的 SSE-C 字段与头）
    let co = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "alpha".into(),
            key: "m".into(),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64.clone()),
            sse_customer_key_md5: Some(md5.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(co.sse_customer_key_md5.as_deref(), Some(md5.as_str()));
    let upload_id = co.upload_id.unwrap();
    let part = s3
        .upload_part(req(UploadPartInput {
            bucket: "alpha".into(),
            key: "m".into(),
            upload_id: upload_id.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from(vec![
                1u8;
                5 * 1024 * 1024
                    + 1
            ]))),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64.clone()),
            sse_customer_key_md5: Some(md5.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        part.sse_customer_key_md5.as_deref(),
        Some(md5.as_str()),
        "UploadPart 响应须回 key-MD5（P2-a）"
    );
    let done = s3
        .complete_multipart_upload(req(CompleteMultipartUploadInput {
            bucket: "alpha".into(),
            key: "m".into(),
            upload_id,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(ETag::Strong(
                        part.e_tag.as_ref().unwrap().value().to_string(),
                    )),
                    ..Default::default()
                }]),
            }),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64.clone()),
            sse_customer_key_md5: Some(md5.clone()),
            ..Default::default()
        }))
        .await
        .unwrap();
    // CompleteMultipartUploadOutput 无 SSE-C 字段 → 双头补齐
    assert_eq!(
        done.headers
            .get("x-amz-server-side-encryption-customer-algorithm")
            .unwrap(),
        "AES256"
    );
    assert_eq!(
        done.headers
            .get("x-amz-server-side-encryption-customer-key-md5")
            .unwrap(),
        md5.as_str()
    );
}

#[tokio::test]
async fn sse_s3_wire_headers_and_kms_cmk_rejected() {
    let (s3, _store, _d) = service_enc().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // SSE-S3：PUT/GET 回 AES256
    let out = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "e".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(
                b"hello-sse-s3",
            ))),
            server_side_encryption: Some(ServerSideEncryption::from_static("AES256")),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        out.server_side_encryption.as_ref().map(|s| s.as_str()),
        Some("AES256")
    );
    let g = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "e".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        g.output.server_side_encryption.as_ref().map(|s| s.as_str()),
        Some("AES256")
    );
    assert!(g.output.sse_customer_algorithm.is_none());
    // SSE-KMS：回 aws:kms + key_id（k<n>）
    let out = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(
                b"hello-sse-kms",
            ))),
            server_side_encryption: Some(ServerSideEncryption::from_static("aws:kms")),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        out.server_side_encryption.as_ref().map(|s| s.as_str()),
        Some("aws:kms")
    );
    assert!(out.ssekms_key_id.as_deref().unwrap_or("").starts_with('k'));
    // 显式 CMK → 400（R2）
    let e = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "ck".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            server_side_encryption: Some(ServerSideEncryption::from_static("aws:kms")),
            ssekms_key_id: Some("arn:aws:kms:cmk".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        &s3s::S3ErrorCode::InvalidRequest,
        "显式 CMK 应 400：{e:?}"
    );
}

/// M8 缺口 C：桶默认加密——PUT/MPU create+complete/Copy 目标都套默认；显式 SSE 优先；
/// DELETE 后恢复明文。
#[tokio::test]
async fn bucket_default_encryption_applied_and_overridden() {
    let (s3, _store, _d) = service_enc().await;
    // 默认加密存在 buckets 行上 → 建桶必须带身份（record_bucket_owner）
    let mut create = req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    });
    create.extensions.insert(xl_s3::access::AuthedUser {
        uid: 1,
        is_admin: true,
        ak: String::new(),
    });
    s3.create_bucket(create).await.unwrap();
    let put = |key: &'static str, body: &'static [u8], sse: Option<&'static str>| {
        req(PutObjectInput {
            bucket: "alpha".into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(body))),
            server_side_encryption: sse.map(ServerSideEncryption::from_static),
            ..Default::default()
        })
    };
    let get = |key: &'static str| {
        req(GetObjectInput {
            bucket: "alpha".into(),
            key: key.into(),
            ..Default::default()
        })
    };
    let enc_cfg = |algo: &'static str| {
        req(PutBucketEncryptionInput {
            bucket: "alpha".into(),
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            server_side_encryption_configuration: ServerSideEncryptionConfiguration {
                rules: vec![ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                        kms_master_key_id: None,
                        sse_algorithm: ServerSideEncryption::from_static(algo),
                    }),
                    ..Default::default()
                }],
            },
        })
    };

    // 默认配置前：明文源对象（拷贝的输入）
    let out = s3.put_object(put("src", b"copy-me", None)).await.unwrap();
    assert!(out.output.server_side_encryption.is_none());

    s3.put_bucket_encryption(enc_cfg("AES256")).await.unwrap();

    // PUT 无 SSE → 套默认 AES256（响应与回读一致）
    let out = s3
        .put_object(put("dflt", b"plain-put", None))
        .await
        .unwrap();
    assert_eq!(
        out.output
            .server_side_encryption
            .as_ref()
            .map(|s| s.as_str()),
        Some("AES256")
    );
    let g = s3.get_object(get("dflt")).await.unwrap().output;
    assert_eq!(
        g.server_side_encryption.as_ref().map(|s| s.as_str()),
        Some("AES256")
    );

    // 显式 SSE-KMS 覆盖桶默认
    let out = s3
        .put_object(put("explicit", b"explicit-kms", Some("aws:kms")))
        .await
        .unwrap();
    assert_eq!(
        out.output
            .server_side_encryption
            .as_ref()
            .map(|s| s.as_str()),
        Some("aws:kms")
    );
    assert!(
        out.output
            .ssekms_key_id
            .as_deref()
            .unwrap_or("")
            .starts_with('k')
    );

    // 显式 SSE-C 同样优先于桶默认（不套默认、不回 SSE 头），且可带钥读回
    use base64::Engine as _;
    let ckey = [9u8; 32];
    let ck = xl_store::crypto::SseCKey::from_key(ckey);
    let cb64 = base64::engine::general_purpose::STANDARD.encode(ckey);
    let cmd5 = ck.md5_b64().to_string();
    let out = s3
        .put_object(req(PutObjectInput {
            bucket: "alpha".into(),
            key: "sse-c".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"sse-c-wins"))),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(cb64.clone()),
            sse_customer_key_md5: Some(cmd5.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.sse_customer_algorithm.as_deref(), Some("AES256"));
    assert!(
        out.server_side_encryption.is_none(),
        "显式 SSE-C 不得被桶默认覆盖为 SSE-S3"
    );
    let g = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "sse-c".into(),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(cb64),
            sse_customer_key_md5: Some(cmd5),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(g.server_side_encryption.is_none());

    // MPU：create 无 SSE → 响应回 AES256；complete 落地
    let created = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        created.server_side_encryption.as_ref().map(|s| s.as_str()),
        Some("AES256")
    );
    let upload_id = created.upload_id.unwrap();
    let up = s3
        .upload_part(req(UploadPartInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            upload_id: upload_id.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(
                b"multipart-default",
            ))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let done = s3
        .complete_multipart_upload(req(CompleteMultipartUploadInput {
            bucket: "alpha".into(),
            key: "mp".into(),
            upload_id,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(ETag::Strong(up.e_tag.unwrap().value().to_string())),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        done.server_side_encryption.as_ref().map(|s| s.as_str()),
        Some("AES256")
    );
    let g = s3.get_object(get("mp")).await.unwrap().output;
    assert_eq!(
        g.server_side_encryption.as_ref().map(|s| s.as_str()),
        Some("AES256")
    );

    // CopyObject：目标桶默认生效（源为明文）
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("alpha".to_string())
        .key("copied".to_string())
        .copy_source(CopySource::Bucket {
            bucket: "alpha".into(),
            key: "src".into(),
            version_id: None,
        })
        .build()
        .unwrap()))
        .await
        .unwrap();
    let g = s3.get_object(get("copied")).await.unwrap().output;
    assert_eq!(
        g.server_side_encryption.as_ref().map(|s| s.as_str()),
        Some("AES256")
    );

    // DELETE 默认 → 新 PUT 恢复明文
    s3.delete_bucket_encryption(req(DeleteBucketEncryptionInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let out = s3.put_object(put("after", b"no-enc", None)).await.unwrap();
    assert!(out.output.server_side_encryption.is_none());
}

// ---------- M5 切片 4：对象锁线级 ----------

fn now_nanos_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn future_ts(secs: i64) -> ObjectLockRetainUntilDate {
    xl_s3::lock::ts_from_nanos(now_nanos_i64() + secs * 1_000_000_000)
}

fn lock_cfg(mode: &'static str, days: i32) -> ObjectLockConfiguration {
    ObjectLockConfiguration {
        object_lock_enabled: Some(ObjectLockEnabled::from_static("Enabled")),
        rule: Some(ObjectLockRule {
            default_retention: Some(DefaultRetention {
                mode: Some(ObjectLockRetentionMode::from_static(mode)),
                days: Some(days),
                years: None,
            }),
        }),
    }
}

#[tokio::test]
async fn object_lock_wire_roundtrip_and_guards() {
    let (s3, _d) = service().await;
    // 建桶带锁标志 → 自动开版本控制（AWS 语义）
    s3.create_bucket(req(CreateBucketInput {
        bucket: "lockb".into(),
        object_lock_enabled_for_bucket: Some(true),
        ..Default::default()
    }))
    .await
    .unwrap();
    // Get config → Enabled（尚无默认规则）
    let cfg = s3
        .get_object_lock_configuration(req(GetObjectLockConfigurationInput {
            bucket: "lockb".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let oc = cfg.object_lock_configuration.unwrap();
    assert_eq!(
        oc.object_lock_enabled.as_ref().map(|e| e.as_str()),
        Some("Enabled")
    );
    assert!(oc.rule.is_none());
    // Put config：默认 COMPLIANCE 30 天
    s3.put_object_lock_configuration(req(PutObjectLockConfigurationInput {
        bucket: "lockb".into(),
        object_lock_configuration: Some(lock_cfg("COMPLIANCE", 30)),
        ..Default::default()
    }))
    .await
    .unwrap();
    let cfg = s3
        .get_object_lock_configuration(req(GetObjectLockConfigurationInput {
            bucket: "lockb".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let dr = cfg
        .object_lock_configuration
        .unwrap()
        .rule
        .unwrap()
        .default_retention
        .unwrap();
    assert_eq!(dr.days, Some(30));

    // PUT 无显式头 → 默认规则落地（HEAD 见 COMPLIANCE + 未来截止）
    let put = s3
        .put_object(req(PutObjectInput {
            bucket: "lockb".into(),
            key: "d".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let vid = put.version_id.clone().unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "lockb".into(),
            key: "d".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        head.object_lock_mode.as_ref().map(|m| m.as_str()),
        Some("COMPLIANCE")
    );
    assert!(head.object_lock_retain_until_date.is_some());

    // 点名删锁定版本 → 403 AccessDenied；bypass 对 COMPLIANCE 无效
    for bypass in [None, Some(true)] {
        let e = s3
            .delete_object(req(DeleteObjectInput {
                bucket: "lockb".into(),
                key: "d".into(),
                version_id: Some(vid.clone()),
                bypass_governance_retention: bypass,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(e.code(), &s3s::S3ErrorCode::AccessDenied, "{e:?}");
    }

    // GOVERNANCE + 未来：无 bypass 403；bypass 放行
    let putg = s3
        .put_object(req(PutObjectInput {
            bucket: "lockb".into(),
            key: "g".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"y"))),
            object_lock_mode: Some(ObjectLockMode::from_static("GOVERNANCE")),
            object_lock_retain_until_date: Some(future_ts(3600)),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let gvid = putg.version_id.unwrap();
    let e = s3
        .delete_object(req(DeleteObjectInput {
            bucket: "lockb".into(),
            key: "g".into(),
            version_id: Some(gvid.clone()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::AccessDenied);
    let ok = s3
        .delete_object(req(DeleteObjectInput {
            bucket: "lockb".into(),
            key: "g".into(),
            version_id: Some(gvid),
            bypass_governance_retention: Some(true),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(ok.output.delete_marker, None, "删的是版本不是加 marker");

    // legal hold：put 带 hold → 一律拒（含 bypass）
    s3.put_object(req(PutObjectInput {
        bucket: "lockb".into(),
        key: "h".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"z"))),
        object_lock_legal_hold_status: Some(ObjectLockLegalHoldStatus::from_static("ON")),
        ..Default::default()
    }))
    .await
    .unwrap();
    let hvid = s3
        .head_object(req(HeadObjectInput {
            bucket: "lockb".into(),
            key: "h".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .version_id
        .unwrap();
    let e = s3
        .delete_object(req(DeleteObjectInput {
            bucket: "lockb".into(),
            key: "h".into(),
            version_id: Some(hvid),
            bypass_governance_retention: Some(true),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::AccessDenied);

    // 未开锁桶：带锁头 PUT → 400；Get config → 404；Put config（无版本）→ InvalidBucketState
    s3.create_bucket(req(CreateBucketInput {
        bucket: "plain".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let e = s3
        .put_object(req(PutObjectInput {
            bucket: "plain".into(),
            key: "x".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"p"))),
            object_lock_mode: Some(ObjectLockMode::from_static("GOVERNANCE")),
            object_lock_retain_until_date: Some(future_ts(60)),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    let e = s3
        .get_object_lock_configuration(req(GetObjectLockConfigurationInput {
            bucket: "plain".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        &s3s::S3ErrorCode::ObjectLockConfigurationNotFoundError
    );
    let e = s3
        .put_object_lock_configuration(req(PutObjectLockConfigurationInput {
            bucket: "plain".into(),
            object_lock_configuration: Some(lock_cfg("GOVERNANCE", 1)),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidBucketState);
}

#[tokio::test]
async fn object_lock_wire_fixes() {
    let (s3, _store, _d) = service_enc().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "locks".into(),
        object_lock_enabled_for_bucket: Some(true),
        ..Default::default()
    }))
    .await
    .unwrap();

    // N3：默认规则超 36500 天上限 → 400
    let e = s3
        .put_object_lock_configuration(req(PutObjectLockConfigurationInput {
            bucket: "locks".into(),
            object_lock_configuration: Some(lock_cfg("COMPLIANCE", 40_000)),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);

    // N4：不存在的桶 + 锁头 → 404 NoSuchBucket
    let e = s3
        .put_object(req(PutObjectInput {
            bucket: "nope".into(),
            key: "x".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            object_lock_mode: Some(ObjectLockMode::from_static("GOVERNANCE")),
            object_lock_retain_until_date: Some(future_ts(60)),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::NoSuchBucket);

    // N2：legal hold 开关线级（ON → 挡删；缺版本号 400；OFF → 放行）
    let ph = s3
        .put_object(req(PutObjectInput {
            bucket: "locks".into(),
            key: "h".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"h"))),
            object_lock_legal_hold_status: Some(ObjectLockLegalHoldStatus::from_static("ON")),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let hvid = ph.version_id.unwrap();
    let g = s3
        .get_object_legal_hold(req(GetObjectLegalHoldInput {
            bucket: "locks".into(),
            key: "h".into(),
            version_id: Some(hvid.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        g.legal_hold.unwrap().status.as_ref().map(|s| s.as_str()),
        Some("ON")
    );
    let e = s3
        .put_object_legal_hold(req(PutObjectLegalHoldInput {
            bucket: "locks".into(),
            key: "h".into(),
            version_id: None,
            legal_hold: Some(ObjectLockLegalHold {
                status: Some(ObjectLockLegalHoldStatus::from_static("OFF")),
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    s3.put_object_legal_hold(req(PutObjectLegalHoldInput {
        bucket: "locks".into(),
        key: "h".into(),
        version_id: Some(hvid.clone()),
        legal_hold: Some(ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from_static("OFF")),
        }),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.delete_object(req(DeleteObjectInput {
        bucket: "locks".into(),
        key: "h".into(),
        version_id: Some(hvid),
        ..Default::default()
    }))
    .await
    .unwrap();

    // §8-i：批量删逐键守卫——COMPLIANCE 版进 errors、版本仍可读
    let pc = s3
        .put_object(req(PutObjectInput {
            bucket: "locks".into(),
            key: "c".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"c"))),
            object_lock_mode: Some(ObjectLockMode::from_static("COMPLIANCE")),
            object_lock_retain_until_date: Some(future_ts(3600)),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let cvid = pc.version_id.unwrap();
    let out = s3
        .delete_objects(with_user(
            DeleteObjectsInput::builder()
                .bucket("locks".to_string())
                .delete(Delete {
                    objects: vec![ObjectIdentifier {
                        key: "c".into(),
                        version_id: Some(cvid.clone()),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .build()
                .unwrap(),
        ))
        .await
        .unwrap()
        .output;
    let errs = out.errors.unwrap_or_default();
    assert!(
        errs.iter()
            .any(|e| e.code.as_deref() == Some("AccessDenied")),
        "锁定版本批量删应进 errors：{errs:?}"
    );
    assert!(
        s3.head_object(req(HeadObjectInput {
            bucket: "locks".into(),
            key: "c".into(),
            version_id: Some(cvid),
            ..Default::default()
        }))
        .await
        .is_ok()
    );

    // §8-ii：CopyObject 目的锁
    s3.put_object(req(PutObjectInput {
        bucket: "locks".into(),
        key: "src".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"src"))),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("locks".to_string())
        .key("dst".to_string())
        .copy_source(CopySource::Bucket {
            bucket: "locks".into(),
            key: "src".into(),
            version_id: None,
        })
        .object_lock_mode(Some(ObjectLockMode::from_static("GOVERNANCE")))
        .object_lock_retain_until_date(Some(future_ts(60)))
        .build()
        .unwrap()))
        .await
        .unwrap();
    let hd = s3
        .head_object(req(HeadObjectInput {
            bucket: "locks".into(),
            key: "dst".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        hd.object_lock_mode.as_ref().map(|m| m.as_str()),
        Some("GOVERNANCE")
    );

    // §8-iii：CreateMPU 线级锁参数（create 带 → complete 落地）
    let co = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "locks".into(),
            key: "m".into(),
            object_lock_mode: Some(ObjectLockMode::from_static("COMPLIANCE")),
            object_lock_retain_until_date: Some(future_ts(3600)),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let upid = co.upload_id.unwrap();
    let part = s3
        .upload_part(req(UploadPartInput {
            bucket: "locks".into(),
            key: "m".into(),
            upload_id: upid.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from(vec![
                1u8;
                5 * 1024 * 1024
                    + 1
            ]))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    s3.complete_multipart_upload(req(CompleteMultipartUploadInput {
        bucket: "locks".into(),
        key: "m".into(),
        upload_id: upid,
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: Some(ETag::Strong(
                    part.e_tag.as_ref().unwrap().value().to_string(),
                )),
                ..Default::default()
            }]),
        }),
        ..Default::default()
    }))
    .await
    .unwrap();
    let hm = s3
        .head_object(req(HeadObjectInput {
            bucket: "locks".into(),
            key: "m".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        hm.object_lock_mode.as_ref().map(|m| m.as_str()),
        Some("COMPLIANCE")
    );

    // 显式 legal-hold 优先：不叠默认 retention；二次 Put config 改默认规则生效
    s3.put_object_lock_configuration(req(PutObjectLockConfigurationInput {
        bucket: "locks".into(),
        object_lock_configuration: Some(lock_cfg("COMPLIANCE", 30)),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "locks".into(),
        key: "h2".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"h2"))),
        object_lock_legal_hold_status: Some(ObjectLockLegalHoldStatus::from_static("ON")),
        ..Default::default()
    }))
    .await
    .unwrap();
    let hd2 = s3
        .head_object(req(HeadObjectInput {
            bucket: "locks".into(),
            key: "h2".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(
        hd2.object_lock_mode.is_none(),
        "显式 hold 不叠默认 retention（显式优先）"
    );
    assert_eq!(
        hd2.object_lock_legal_hold_status
            .as_ref()
            .map(|s| s.as_str()),
        Some("ON")
    );
    s3.put_object_lock_configuration(req(PutObjectLockConfigurationInput {
        bucket: "locks".into(),
        object_lock_configuration: Some(lock_cfg("GOVERNANCE", 1)),
        ..Default::default()
    }))
    .await
    .unwrap();
    let cfg2 = s3
        .get_object_lock_configuration(req(GetObjectLockConfigurationInput {
            bucket: "locks".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let dr2 = cfg2
        .object_lock_configuration
        .unwrap()
        .rule
        .unwrap()
        .default_retention
        .unwrap();
    assert_eq!(dr2.days, Some(1));
    assert_eq!(
        dr2.mode.as_ref().map(|m| m.as_str().to_string()),
        Some("GOVERNANCE".to_string())
    );
}

// P3-a/P3-b 复验补（切片 4 收口轮）：legal hold 门禁 + SSE-C 对象 hold 元数据读
#[tokio::test]
async fn object_lock_legal_hold_gates() {
    use base64::Engine as _;
    let (s3, _store, _d) = service_enc().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "locks".into(),
        object_lock_enabled_for_bucket: Some(true),
        ..Default::default()
    }))
    .await
    .unwrap();

    // P3-a：未开锁桶设 legal hold → 400（须开锁桶）；不存在桶 → 404
    s3.create_bucket(req(CreateBucketInput {
        bucket: "nolo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let pn = s3
        .put_object(req(PutObjectInput {
            bucket: "nolo".into(),
            key: "x".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let e = s3
        .put_object_legal_hold(req(PutObjectLegalHoldInput {
            bucket: "nolo".into(),
            key: "x".into(),
            version_id: pn.version_id.clone().or(Some("null".into())),
            legal_hold: Some(ObjectLockLegalHold {
                status: Some(ObjectLockLegalHoldStatus::from_static("ON")),
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        &s3s::S3ErrorCode::InvalidRequest,
        "未开锁桶设 hold 应 400：{e:?}"
    );
    let e = s3
        .put_object_legal_hold(req(PutObjectLegalHoldInput {
            bucket: "ghost".into(),
            key: "x".into(),
            version_id: Some("v".into()),
            legal_hold: Some(ObjectLockLegalHold {
                status: Some(ObjectLockLegalHoldStatus::from_static("ON")),
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::NoSuchBucket);

    // P3-b：SSE-C 对象（带 legal hold）的 GetObjectLegalHold 不要求客户钥
    let key = [9u8; 32];
    let k = xl_store::crypto::SseCKey::from_key(key);
    let b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5 = k.md5_b64().to_string();
    let ph = s3
        .put_object(req(PutObjectInput {
            bucket: "locks".into(),
            key: "ssec".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"ssec"))),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64.clone()),
            sse_customer_key_md5: Some(md5.clone()),
            object_lock_legal_hold_status: Some(ObjectLockLegalHoldStatus::from_static("ON")),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let svid = ph.version_id.unwrap();
    let g = s3
        .get_object_legal_hold(req(GetObjectLegalHoldInput {
            bucket: "locks".into(),
            key: "ssec".into(),
            version_id: Some(svid.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        g.legal_hold.unwrap().status.as_ref().map(|s| s.as_str()),
        Some("ON"),
        "P3-b：SSE-C 对象 hold 可读（无需客户钥）"
    );
    // 数据面仍须钥（hold 元数据读不能变成数据读的旁路）
    let e = s3
        .get_object(req(GetObjectInput {
            bucket: "locks".into(),
            key: "ssec".into(),
            version_id: Some(svid),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        &s3s::S3ErrorCode::InvalidArgument,
        "SSE-C 数据读缺钥仍 400"
    );
}

// ---------- M6 切片 0：事件发射六路径 + 通知配置 API ----------

/// 捕获的事件（op, bucket, key, version_id, size, etag）。
type MockEvent = (
    xl_s3::events::S3EventOp,
    String,
    String,
    Option<String>,
    Option<u64>,
    Option<String>,
);

#[derive(Default)]
struct MockSink {
    got: std::sync::Mutex<Vec<MockEvent>>,
}

#[async_trait::async_trait]
impl xl_s3::events::EventSink for MockSink {
    async fn emit(
        &self,
        op: xl_s3::events::S3EventOp,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: Option<u64>,
        etag: Option<&str>,
        _source: xl_s3::events::EventSource,
    ) {
        self.got.lock().unwrap().push((
            op,
            bucket.to_string(),
            key.to_string(),
            version_id.map(str::to_string),
            size,
            etag.map(str::to_string),
        ));
    }
}

#[tokio::test]
async fn events_emitted_on_all_write_paths() {
    use xl_s3::events::S3EventOp;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    let sink = Arc::new(MockSink::default());
    let s3 = OssS3::new(store, acl().await, 16 * 1024 * 1024)
        .with_events(Some(sink.clone() as Arc<dyn xl_s3::events::EventSink>));

    // 版本桶（marker 场景）；带身份建桶以落 buckets 行（版本状态查询前提）
    s3.create_bucket(with_user(CreateBucketInput {
        bucket: "evb".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_bucket_versioning(req(PutBucketVersioningInput {
        bucket: "evb".into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        mfa: None,
        versioning_configuration: VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static("Enabled")),
            mfa_delete: None,
        },
    }))
    .await
    .unwrap();

    // 1) Put
    let p = s3
        .put_object(req(PutObjectInput {
            bucket: "evb".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"hello"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let v1 = p.version_id.clone().unwrap();
    // 2) Copy
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("evb".to_string())
        .key("k2".to_string())
        .copy_source(CopySource::Bucket {
            bucket: "evb".into(),
            key: "k".into(),
            version_id: None,
        })
        .build()
        .unwrap()))
        .await
        .unwrap();
    // 3) CompleteMPU
    let up = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "evb".into(),
            key: "mpu".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let part = s3
        .upload_part(req(UploadPartInput {
            bucket: "evb".into(),
            key: "mpu".into(),
            upload_id: up.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from(vec![
                1u8;
                5 * 1024 * 1024
                    + 1
            ]))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    s3.complete_multipart_upload(req(CompleteMultipartUploadInput {
        bucket: "evb".into(),
        key: "mpu".into(),
        upload_id: up,
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: Some(ETag::Strong(
                    part.e_tag.as_ref().unwrap().value().to_string(),
                )),
                ..Default::default()
            }]),
        }),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 4) 点名删版本 → RemovedDelete
    s3.delete_object(req(DeleteObjectInput {
        bucket: "evb".into(),
        key: "k".into(),
        version_id: Some(v1.clone()),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 5) 无版本号删 → marker
    s3.delete_object(req(DeleteObjectInput {
        bucket: "evb".into(),
        key: "k".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 6) 批量删（逐键）
    s3.delete_objects(with_user(
        DeleteObjectsInput::builder()
            .bucket("evb".to_string())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "k2".into(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .build()
            .unwrap(),
    ))
    .await
    .unwrap();

    let got = sink.got.lock().unwrap().clone();
    let names: Vec<&str> = got.iter().map(|g| g.0.event_name()).collect();
    assert_eq!(
        names,
        vec![
            "s3:ObjectCreated:Put",
            "s3:ObjectCreated:Copy",
            "s3:ObjectCreated:CompleteMultipartUpload",
            "s3:ObjectRemoved:Delete",
            "s3:ObjectRemoved:DeleteMarkerCreated",
            "s3:ObjectRemoved:DeleteMarkerCreated", // 批量删对版本桶的空版本号删 = marker
        ],
        "{got:?}"
    );
    // 字段抽查：put 带 size/etag/version；copy 目标键正确
    let put = &got[0];
    assert_eq!(put.1, "evb");
    assert_eq!(put.2, "k");
    assert_eq!(put.4, Some(5));
    assert!(put.5.is_some());
    assert_eq!(got[1].2, "k2");
    let _ = S3EventOp::RemovedDelete;
}

#[tokio::test]
async fn notification_config_api_roundtrip_and_errors() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "notifb".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 写配置
    let cfg = NotificationConfiguration {
        queue_configurations: Some(QueueConfigurationList::from(vec![QueueConfiguration {
            id: Some(NotificationId::from("cfg-0".to_string())),
            queue_arn: QueueArn::from("arn:oss:sqs::t1:kafka".to_string()),
            events: EventList::from(vec![
                Event::from("s3:ObjectCreated:Put".to_string()),
                Event::from("s3:ObjectRemoved:Delete".to_string()),
            ]),
            filter: Some(NotificationConfigurationFilter {
                key: Some(S3KeyFilter {
                    filter_rules: Some(FilterRuleList::from(vec![FilterRule {
                        name: Some(FilterRuleName::from_static("prefix")),
                        value: Some(FilterRuleValue::from("logs/".to_string())),
                    }])),
                }),
            }),
        }])),
        ..Default::default()
    };
    s3.put_bucket_notification_configuration(req(
        PutBucketNotificationConfigurationInput::builder()
            .bucket("notifb".to_string())
            .notification_configuration(cfg)
            .build()
            .unwrap(),
    ))
    .await
    .unwrap();
    // 回读
    let got = s3
        .get_bucket_notification_configuration(req(GetBucketNotificationConfigurationInput {
            bucket: "notifb".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let qs = got.queue_configurations.unwrap();
    assert_eq!(qs.len(), 1);
    assert_eq!(qs[0].queue_arn.as_str(), "arn:oss:sqs::t1:kafka");
    let rules = qs[0]
        .filter
        .as_ref()
        .unwrap()
        .key
        .as_ref()
        .unwrap()
        .filter_rules
        .as_ref()
        .unwrap();
    assert_eq!(rules[0].value.as_ref().unwrap().as_str(), "logs/");

    // ObjectAccessed → 400（R5 记档：本片不实现）
    let bad = NotificationConfiguration {
        queue_configurations: Some(QueueConfigurationList::from(vec![QueueConfiguration {
            id: None,
            queue_arn: QueueArn::from("arn:oss:sqs::t1:kafka".to_string()),
            events: EventList::from(vec![Event::from("s3:ObjectAccessed:Get".to_string())]),
            filter: None,
        }])),
        ..Default::default()
    };
    let e = s3
        .put_bucket_notification_configuration(req(
            PutBucketNotificationConfigurationInput::builder()
                .bucket("notifb".to_string())
                .notification_configuration(bad)
                .build()
                .unwrap(),
        ))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument);

    // 不存在的桶 → 404
    let e = s3
        .get_bucket_notification_configuration(req(GetBucketNotificationConfigurationInput {
            bucket: "ghost".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::NoSuchBucket);

    // 空配置清空 → 回读空
    s3.put_bucket_notification_configuration(req(
        PutBucketNotificationConfigurationInput::builder()
            .bucket("notifb".to_string())
            .notification_configuration(NotificationConfiguration::default())
            .build()
            .unwrap(),
    ))
    .await
    .unwrap();
    let got = s3
        .get_bucket_notification_configuration(req(GetBucketNotificationConfigurationInput {
            bucket: "notifb".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(got.queue_configurations.unwrap_or_default().is_empty());
}

/// M6 切片 0（P2-1）：幂等空删——204 但**不发事件**；真删仍发（对照）。
#[tokio::test]
async fn events_not_emitted_on_empty_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    let sink = Arc::new(MockSink::default());
    let s3 = OssS3::new(store, acl().await, 1024 * 1024)
        .with_events(Some(sink.clone() as Arc<dyn xl_s3::events::EventSink>));

    // 非版本桶：空删不发射
    s3.create_bucket(req(CreateBucketInput {
        bucket: "ebk".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.delete_object(req(DeleteObjectInput {
        bucket: "ebk".into(),
        key: "never".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert!(sink.got.lock().unwrap().is_empty(), "空删不得发事件");
    // 真删 → 发射
    s3.put_object(req(PutObjectInput {
        bucket: "ebk".into(),
        key: "real".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"r"))),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.delete_object(req(DeleteObjectInput {
        bucket: "ebk".into(),
        key: "real".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_eq!(sink.got.lock().unwrap().len(), 2, "put + delete 各一条");

    // 版本桶：点名删不存在的版本 → 不发射；批量删缺失键 → deleted 列出但不发射
    s3.create_bucket(with_user(CreateBucketInput {
        bucket: "ebkv".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_bucket_versioning(req(PutBucketVersioningInput {
        bucket: "ebkv".into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        mfa: None,
        versioning_configuration: VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static("Enabled")),
            mfa_delete: None,
        },
    }))
    .await
    .unwrap();
    let before = sink.got.lock().unwrap().len();
    s3.delete_object(req(DeleteObjectInput {
        bucket: "ebkv".into(),
        key: "k".into(),
        version_id: Some("no-such-version".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_eq!(sink.got.lock().unwrap().len(), before, "空版本删不得发事件");
    // 版本桶无版本号删缺失键 = 建 marker（AWS 语义）→ 事件**应发**；对照断言：
    let out = s3
        .delete_objects(with_user(
            DeleteObjectsInput::builder()
                .bucket("ebkv".to_string())
                .delete(Delete {
                    objects: vec![ObjectIdentifier {
                        key: "missing".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .build()
                .unwrap(),
        ))
        .await
        .unwrap()
        .output;
    assert!(
        out.deleted
            .as_ref()
            .unwrap()
            .iter()
            .any(|d| d.key.as_deref() == Some("missing")),
        "S3 语义：不存在的键也进 deleted[]"
    );
    assert_eq!(
        sink.got.lock().unwrap().len(),
        before + 1,
        "版本桶无版本号删缺失键 = 建 marker → DeleteMarkerCreated 应发"
    );
    // 非版本桶批量删缺失键：进 deleted[] 但不发事件（P2-1 核心）
    let before2 = sink.got.lock().unwrap().len();
    let out = s3
        .delete_objects(with_user(
            DeleteObjectsInput::builder()
                .bucket("ebk".to_string())
                .delete(Delete {
                    objects: vec![ObjectIdentifier {
                        key: "missing2".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .build()
                .unwrap(),
        ))
        .await
        .unwrap()
        .output;
    assert!(
        out.deleted
            .as_ref()
            .unwrap()
            .iter()
            .any(|d| d.key.as_deref() == Some("missing2"))
    );
    assert_eq!(
        sink.got.lock().unwrap().len(),
        before2,
        "非版本桶空删不得发事件"
    );
}

// ---------- M6 切片 3：复制状态面（REPLICA 摄入 + HEAD/GET 响应头） ----------

#[tokio::test]
async fn replication_status_headers_and_replica_ingest() {
    let (s3, store, _d) = service_enc().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "repbucket".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 普通 PUT → 无复制状态
    let out = s3
        .put_object(req(PutObjectInput {
            bucket: "repbucket".into(),
            key: "plain".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"hi"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let vid = out.version_id.unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "repbucket".into(),
            key: "plain".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(head.replication_status.is_none());

    // store 设 PENDING → HEAD/GET 输出 x-amz-replication-status
    store
        .set_replication_status("repbucket", "plain", &vid, Some("PENDING"))
        .await
        .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "repbucket".into(),
            key: "plain".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        head.replication_status.as_ref().map(|r| r.as_str()),
        Some("PENDING")
    );
    let get = s3
        .get_object(req(GetObjectInput {
            bucket: "repbucket".into(),
            key: "plain".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        get.replication_status.as_ref().map(|r| r.as_str()),
        Some("PENDING")
    );

    // REPLICA 摄入：复制源 PUT 带 x-amz-replication-status: REPLICA → 落为 REPLICA
    let mut r = req(PutObjectInput {
        bucket: "repbucket".into(),
        key: "copy".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"rep"))),
        ..Default::default()
    });
    r.headers
        .insert("x-amz-replication-status", "REPLICA".parse().unwrap());
    s3.put_object(r).await.unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "repbucket".into(),
            key: "copy".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        head.replication_status.as_ref().map(|r| r.as_str()),
        Some("REPLICA")
    );
}

// ---- M8 前置：对象标签（切片 1） ----

async fn versioned(s3: &OssS3, bucket: &str) {
    // 版本控制状态经 ACL 登记表读写 → 建桶须带身份（记账 owner）
    s3.create_bucket(with_user(CreateBucketInput {
        bucket: bucket.into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_bucket_versioning(req(PutBucketVersioningInput {
        bucket: bucket.into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        mfa: None,
        versioning_configuration: VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static("Enabled")),
            mfa_delete: None,
        },
    }))
    .await
    .unwrap();
}

fn tag_pair(t: &Tag) -> (&str, &str) {
    (t.key.as_deref().unwrap(), t.value.as_deref().unwrap())
}

#[tokio::test]
async fn tagging_roundtrip_put_get_delete_and_counts() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        tagging: Some("env=prod&team=core".into()),
        ..Default::default()
    }))
    .await
    .unwrap();

    // 顺序保持（保序是契约）
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.tag_set.len(), 2);
    assert_eq!(tag_pair(&out.tag_set[0]), ("env", "prod"));
    assert_eq!(tag_pair(&out.tag_set[1]), ("team", "core"));
    assert_eq!(out.version_id.as_deref(), Some("null"));
    // 非版本桶用字面量 null 点名版本
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "alpha".into(),
            key: "k".into(),
            version_id: Some("null".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.tag_set.len(), 2);
    // header URL 解码：%20→空格、%2B→'+'、字面 '+'→空格
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "url".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        tagging: Some("k%20ey=v%2B1&lit=a+b".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "alpha".into(),
            key: "url".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(tag_pair(&out.tag_set[0]), ("k ey", "v+1"));
    assert_eq!(tag_pair(&out.tag_set[1]), ("lit", "a b"));

    // GET/HEAD 带 x-amz-tagging-count
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.tag_count, Some(2));
    let get = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(get.tag_count, Some(2));

    // PUT Tagging 整组替换
    s3.put_object_tagging(req(PutObjectTaggingInput {
        bucket: "alpha".into(),
        key: "k".into(),
        tagging: Tagging {
            tag_set: vec![Tag {
                key: Some("only".into()),
                value: Some("one".into()),
            }],
        },
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        request_payer: None,
        version_id: None,
    }))
    .await
    .unwrap();
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.tag_set.len(), 1);
    assert_eq!(tag_pair(&out.tag_set[0]), ("only", "one"));

    // DELETE Tagging 清空
    s3.delete_object_tagging(req(DeleteObjectTaggingInput {
        bucket: "alpha".into(),
        key: "k".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.tag_set.is_empty());
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "alpha".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.tag_count, None);

    // 缺失键 → NoSuchKey
    let err = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "alpha".into(),
            key: "missing".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchKey);
    // 缺桶 → NoSuchBucket
    let err = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "nobucket".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchBucket);
}

/// Suspended 端到端：PUT 写 null 版本、DELETE 追加 null marker、点名删 marker 恢复历史。
#[tokio::test]
async fn suspended_put_delete_end_to_end() {
    async fn read_body(mut b: StreamingBlob) -> Vec<u8> {
        let mut buf = Vec::new();
        while let Some(chunk) = futures::StreamExt::next(&mut b).await {
            buf.extend_from_slice(&chunk.unwrap());
        }
        buf
    }
    let (s3, _d) = service().await;
    versioned(&s3, "vsus").await;
    let v1 = s3
        .put_object(req(PutObjectInput {
            bucket: "vsus".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"1"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .version_id
        .unwrap();
    assert_ne!(v1, "null");

    let cfg = |st: &'static str| {
        req(PutBucketVersioningInput {
            bucket: "vsus".into(),
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            mfa: None,
            versioning_configuration: VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(st)),
                mfa_delete: None,
            },
        })
    };
    s3.put_bucket_versioning(cfg("Suspended")).await.unwrap();
    let v2 = s3
        .put_object(req(PutObjectInput {
            bucket: "vsus".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"2"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .version_id
        .unwrap();
    assert_eq!(v2, "null");
    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "vsus".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(read_body(got.body.unwrap()).await, b"2");

    // 版本列表：null 版本为 latest，真 UUID 历史保留
    let lv = s3
        .list_object_versions(req(ListObjectVersionsInput {
            bucket: "vsus".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let versions = lv.versions.unwrap();
    assert_eq!(versions.len(), 2, "{versions:?}");
    assert_eq!(versions[0].version_id.as_deref(), Some("null"));
    assert_eq!(versions[0].is_latest, Some(true));
    assert_eq!(versions[1].version_id.as_deref(), Some(v1.as_str()));

    // 无版本号删 → null delete marker；默认读 404
    let del = s3
        .delete_object(req(DeleteObjectInput {
            bucket: "vsus".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(del.delete_marker, Some(true));
    assert_eq!(del.version_id.as_deref(), Some("null"));
    let err = s3
        .get_object(req(GetObjectInput {
            bucket: "vsus".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchKey);
    let lv = s3
        .list_object_versions(req(ListObjectVersionsInput {
            bucket: "vsus".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let markers = lv.delete_markers.unwrap();
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].version_id.as_deref(), Some("null"));
    assert_eq!(
        lv.versions.unwrap().len(),
        1,
        "非 null 历史保留；null 数据版已移除"
    );

    // 点名删 null marker → 恢复可见（v1）
    s3.delete_object(req(DeleteObjectInput {
        bucket: "vsus".into(),
        key: "k".into(),
        version_id: Some("null".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "vsus".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(read_body(got.body.unwrap()).await, b"1");

    // 回 Enabled：新写为真 UUID
    s3.put_bucket_versioning(cfg("Enabled")).await.unwrap();
    let v3 = s3
        .put_object(req(PutObjectInput {
            bucket: "vsus".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"3"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .version_id
        .unwrap();
    assert_ne!(v3, "null");
}

#[tokio::test]
async fn tagging_version_isolation_and_marker_errors() {
    let (s3, _d) = service().await;
    versioned(&s3, "vtag").await;
    let v1 = s3
        .put_object(req(PutObjectInput {
            bucket: "vtag".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"1"))),
            tagging: Some("a=1".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .version_id
        .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "vtag".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"2"))),
        tagging: Some("b=2".into()),
        ..Default::default()
    }))
    .await
    .unwrap();

    // 点名旧版本
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "vtag".into(),
            key: "k".into(),
            version_id: Some(v1.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.version_id.as_deref(), Some(v1.as_str()));
    assert_eq!(tag_pair(&out.tag_set[0]), ("a", "1"));
    // 改旧版本标签不影响链尾
    s3.put_object_tagging(req(PutObjectTaggingInput {
        bucket: "vtag".into(),
        key: "k".into(),
        version_id: Some(v1.clone()),
        tagging: Tagging {
            tag_set: vec![Tag {
                key: Some("c".into()),
                value: Some("3".into()),
            }],
        },
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        request_payer: None,
    }))
    .await
    .unwrap();
    let tail = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "vtag".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(tag_pair(&tail.tag_set[0]), ("b", "2"));

    // 无版本号删 → 链尾 marker：默认读视同不存在，点名 marker → 405
    let del = s3
        .delete_object(req(DeleteObjectInput {
            bucket: "vtag".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(del.delete_marker, Some(true));
    let err = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "vtag".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchKey);
    let marker_vid = s3
        .list_object_versions(req(ListObjectVersionsInput {
            bucket: "vtag".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .delete_markers
        .unwrap()[0]
        .version_id
        .clone()
        .unwrap();
    let err = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "vtag".into(),
            key: "k".into(),
            version_id: Some(marker_vid.clone()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::MethodNotAllowed);
    let err = s3
        .delete_object_tagging(req(DeleteObjectTaggingInput {
            bucket: "vtag".into(),
            key: "k".into(),
            version_id: Some(marker_vid),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::MethodNotAllowed);
}

#[tokio::test]
async fn tagging_invalid_matrix_and_boundaries() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "ttag".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let put = |tagging: &str| {
        let tagging = tagging.to_string();
        req(PutObjectInput {
            bucket: "ttag".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            tagging: Some(tagging),
            ..Default::default()
        })
    };
    let bad = [
        "a=1&a=2", // 重复键
        "=v",      // 空键
        "a#=1",    // 非法字符
        "a=1&=2",  // 第二个空键
        "a=%0A",   // 控制字符
        "a!=1",    // 非法字符
        "a=%zz",   // 坏转义（保留原文 → '%' 非法）
    ];
    for t in bad {
        let err = s3.put_object(put(t)).await.unwrap_err();
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidTag, "{t}");
    }
    // 11 条超限
    let many: Vec<String> = (0..11).map(|i| format!("k{i}=v")).collect();
    let err = s3.put_object(put(&many.join("&"))).await.unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidTag);
    // 键 129 / 值 257 超限
    let k129 = format!("{}=", "k".repeat(129));
    let err = s3.put_object(put(&k129)).await.unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidTag);
    let v257 = format!("a={}", "v".repeat(257));
    let err = s3.put_object(put(&v257)).await.unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidTag);

    // 正向边界：10 条、键 128、值 256
    let k128 = format!("{}=v", "k".repeat(128));
    let v256 = format!("a={}", "v".repeat(256));
    let mut ten: Vec<String> = (0..8).map(|i| format!("k{i}=v")).collect();
    ten.push(k128);
    ten.push(v256);
    s3.put_object(put(&ten.join("&"))).await.unwrap();
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "ttag".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.tag_set.len(), 10);
    assert_eq!(out.tag_set[8].key.as_deref().unwrap().len(), 128);
    assert_eq!(out.tag_set[9].value.as_deref().unwrap().len(), 256);
    // 128 个中文字符键（按 Unicode 标量计数）
    let zh = format!("{}=v", "中".repeat(128));
    s3.put_object(put(&zh)).await.unwrap();
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "ttag".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.tag_set[0].key.as_deref().unwrap().chars().count(), 128);

    // PUT Tagging：缺 key → InvalidTag；空 TagSet 合法（= 清空）
    let err = s3
        .put_object_tagging(req(PutObjectTaggingInput {
            bucket: "ttag".into(),
            key: "k".into(),
            tagging: Tagging {
                tag_set: vec![Tag {
                    key: None,
                    value: Some("v".into()),
                }],
            },
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            request_payer: None,
            version_id: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidTag);
    s3.put_object_tagging(req(PutObjectTaggingInput {
        bucket: "ttag".into(),
        key: "k".into(),
        tagging: Tagging { tag_set: vec![] },
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        request_payer: None,
        version_id: None,
    }))
    .await
    .unwrap();
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "ttag".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.tag_set.is_empty());
}

#[tokio::test]
async fn tagging_replica_ingest_drops_tags() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "rtag".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let mut r = req(PutObjectInput {
        bucket: "rtag".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        tagging: Some("a=1".into()),
        ..Default::default()
    });
    r.headers
        .insert("x-amz-replication-status", "REPLICA".parse().unwrap());
    s3.put_object(r).await.unwrap();
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "rtag".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.tag_set.is_empty(), "复制摄入不落标签（标签不跨界）");
}

#[tokio::test]
async fn post_object_form_tagging_reaches_store() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "posttag".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // s3s 默认 post_object 委托 put_object，tagging 经 input 映射透传
    s3.post_object(req(PostObjectInput {
        bucket: "posttag".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"p"))),
        tagging: Some("form=yes".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "posttag".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(tag_pair(&out.tag_set[0]), ("form", "yes"));
}

// ---- M8 前置：CopyObject 两轴 DIRECTIVE（切片 2） ----

fn src(bucket: &str, key: &str, vid: Option<&str>) -> CopySource {
    CopySource::Bucket {
        bucket: bucket.into(),
        key: key.into(),
        version_id: vid.map(Into::into),
    }
}

fn meta(pairs: &[(&str, &str)]) -> Metadata {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

async fn tags_of(s3: &OssS3, bucket: &str, key: &str) -> Vec<(String, String)> {
    let out = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: bucket.into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    out.tag_set
        .iter()
        .map(|t| {
            (
                t.key.clone().unwrap_or_default(),
                t.value.clone().unwrap_or_default(),
            )
        })
        .collect()
}

#[tokio::test]
async fn copy_directive_axes_are_independent() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "copyb".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "copyb".into(),
        key: "src".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"s"))),
        content_type: Some("text/plain".into()),
        metadata: Some(meta(&[("m", "1")])),
        tagging: Some("a=1".into()),
        ..Default::default()
    }))
    .await
    .unwrap();

    // 默认 COPY：元数据 + 标签全继承
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("copyb".to_string())
        .key("both".to_string())
        .copy_source(src("copyb", "src", None))
        .build()
        .unwrap()))
        .await
        .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "copyb".into(),
            key: "both".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.content_type.as_deref(), Some("text/plain"));
    assert_eq!(head.metadata.as_ref(), Some(&meta(&[("m", "1")])));
    assert_eq!(
        tags_of(&s3, "copyb", "both").await,
        [("a".into(), "1".into())]
    );

    // metadata REPLACE（仅 content-type）：元数据轴替换、标签轴仍 COPY
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("copyb".to_string())
        .key("md".to_string())
        .copy_source(src("copyb", "src", None))
        .metadata_directive(Some(MetadataDirective::from_static("REPLACE")))
        .content_type(Some("text/x-t".into()))
        .build()
        .unwrap()))
        .await
        .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "copyb".into(),
            key: "md".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.content_type.as_deref(), Some("text/x-t"));
    assert_eq!(
        head.metadata.as_ref(),
        Some(&Metadata::default()),
        "REPLACE 未给 metadata → 清空"
    );
    assert_eq!(
        tags_of(&s3, "copyb", "md").await,
        [("a".into(), "1".into())]
    );

    // metadata REPLACE（仅 metadata）：content-type 不继承
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("copyb".to_string())
        .key("md2".to_string())
        .copy_source(src("copyb", "src", None))
        .metadata_directive(Some(MetadataDirective::from_static("REPLACE")))
        .metadata(Some(meta(&[("m", "2")])))
        .build()
        .unwrap()))
        .await
        .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "copyb".into(),
            key: "md2".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.content_type, None);
    assert_eq!(head.metadata.as_ref(), Some(&meta(&[("m", "2")])));

    // tagging REPLACE + header → 标签替换、metadata 仍继承
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("copyb".to_string())
        .key("tg".to_string())
        .copy_source(src("copyb", "src", None))
        .tagging_directive(Some(TaggingDirective::from_static("REPLACE")))
        .tagging(Some("x=9".into()))
        .build()
        .unwrap()))
        .await
        .unwrap();
    assert_eq!(
        tags_of(&s3, "copyb", "tg").await,
        [("x".into(), "9".into())]
    );
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "copyb".into(),
            key: "tg".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.metadata.as_ref(), Some(&meta(&[("m", "1")])));

    // COPY 时携带 x-amz-tagging → 忽略、继承源
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("copyb".to_string())
        .key("ign".to_string())
        .copy_source(src("copyb", "src", None))
        .tagging(Some("z=9".into()))
        .build()
        .unwrap()))
        .await
        .unwrap();
    assert_eq!(
        tags_of(&s3, "copyb", "ign").await,
        [("a".into(), "1".into())]
    );

    // tagging REPLACE 不带 header → 清空
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("copyb".to_string())
        .key("empty".to_string())
        .copy_source(src("copyb", "src", None))
        .tagging_directive(Some(TaggingDirective::from_static("REPLACE")))
        .build()
        .unwrap()))
        .await
        .unwrap();
    assert!(tags_of(&s3, "copyb", "empty").await.is_empty());
}

#[tokio::test]
async fn copy_from_old_version_inherits_that_versions_tags() {
    let (s3, _d) = service().await;
    versioned(&s3, "cpv").await;
    let v1 = s3
        .put_object(req(PutObjectInput {
            bucket: "cpv".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"1"))),
            tagging: Some("old=1".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .version_id
        .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "cpv".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"2"))),
        tagging: Some("new=2".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("cpv".to_string())
        .key("dst".to_string())
        .copy_source(src("cpv", "k", Some(v1.as_str())))
        .build()
        .unwrap()))
        .await
        .unwrap();
    assert_eq!(
        tags_of(&s3, "cpv", "dst").await,
        [("old".into(), "1".into())]
    );
}

#[tokio::test]
async fn copy_to_self_requires_metadata_replace() {
    let (s3, _d) = service().await;
    versioned(&s3, "selfv").await;
    let v1 = s3
        .put_object(req(PutObjectInput {
            bucket: "selfv".into(),
            key: "k".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            metadata: Some(meta(&[("m", "1")])),
            tagging: Some("a=1".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .version_id
        .unwrap();

    // 仅 td=REPLACE（md=COPY）→ 仍拒
    let err = s3
        .copy_object(req(CopyObjectInput::builder()
            .bucket("selfv".to_string())
            .key("k".to_string())
            .copy_source(src("selfv", "k", None))
            .tagging_directive(Some(TaggingDirective::from_static("REPLACE")))
            .build()
            .unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);
    // 默认 COPY → 拒
    let err = s3
        .copy_object(req(CopyObjectInput::builder()
            .bucket("selfv".to_string())
            .key("k".to_string())
            .copy_source(src("selfv", "k", None))
            .build()
            .unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidRequest);

    // md=REPLACE 放行：新版本、元数据替换、标签默认继承
    let out = s3
        .copy_object(req(CopyObjectInput::builder()
            .bucket("selfv".to_string())
            .key("k".to_string())
            .copy_source(src("selfv", "k", None))
            .metadata_directive(Some(MetadataDirective::from_static("REPLACE")))
            .metadata(Some(meta(&[("m", "2")])))
            .build()
            .unwrap()))
        .await
        .unwrap()
        .output;
    let v2 = out.version_id.unwrap();
    assert_ne!(v1, v2);
    let versions = s3
        .list_object_versions(req(ListObjectVersionsInput {
            bucket: "selfv".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(versions.versions.unwrap().len(), 2);
    // 旧版本元数据/标签未动
    let old = s3
        .head_object(req(HeadObjectInput {
            bucket: "selfv".into(),
            key: "k".into(),
            version_id: Some(v1.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(old.metadata.as_ref(), Some(&meta(&[("m", "1")])));
    assert_eq!(
        tags_of(&s3, "selfv", "k").await,
        [("a".into(), "1".into())],
        "链尾标签默认 COPY 继承"
    );
    let tail = s3
        .head_object(req(HeadObjectInput {
            bucket: "selfv".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(tail.metadata.as_ref(), Some(&meta(&[("m", "2")])));

    // 非版本桶：md=REPLACE 原地覆盖成功
    s3.create_bucket(req(CreateBucketInput {
        bucket: "selfn".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "selfn".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        metadata: Some(meta(&[("m", "1")])),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("selfn".to_string())
        .key("k".to_string())
        .copy_source(src("selfn", "k", None))
        .metadata_directive(Some(MetadataDirective::from_static("REPLACE")))
        .metadata(Some(meta(&[("m", "3")])))
        .build()
        .unwrap()))
        .await
        .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "selfn".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.metadata.as_ref(), Some(&meta(&[("m", "3")])));
}

#[tokio::test]
async fn copy_rejects_bad_directives_and_bad_tags() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "cpbad".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "cpbad".into(),
        key: "src".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 非法指令值（s3s 类型解析宽松，服务端拒）
    for (md, td) in [
        (Some(MetadataDirective::from("replace".to_string())), None),
        (None, Some(TaggingDirective::from("COPYX".to_string()))),
    ] {
        let err = s3
            .copy_object(req(CopyObjectInput::builder()
                .bucket("cpbad".to_string())
                .key("dst".to_string())
                .copy_source(src("cpbad", "src", None))
                .metadata_directive(md)
                .tagging_directive(td)
                .build()
                .unwrap()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidArgument);
    }
    // REPLACE 标签非法 → InvalidTag，且目标未产生
    let err = s3
        .copy_object(req(CopyObjectInput::builder()
            .bucket("cpbad".to_string())
            .key("bad".to_string())
            .copy_source(src("cpbad", "src", None))
            .tagging_directive(Some(TaggingDirective::from_static("REPLACE")))
            .tagging(Some("a=%0A".into()))
            .build()
            .unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidTag);
    let err = s3
        .head_object(req(HeadObjectInput {
            bucket: "cpbad".into(),
            key: "bad".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::NoSuchKey);
}

// ---- M8 S0：删桶跨 crate 清理钩子 ----

#[derive(Default)]
struct FakeCleanup {
    cleared: std::sync::Mutex<Vec<String>>,
    invalidated: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl xl_s3::bucket_cleanup::BucketCleanupSink for FakeCleanup {
    async fn clear_bucket_m8(&self, bucket: &str) -> Result<(), String> {
        self.cleared.lock().unwrap().push(bucket.to_string());
        Ok(())
    }
    fn invalidate_ap_cache(&self, bucket: &str) {
        self.invalidated.lock().unwrap().push(bucket.to_string());
    }
}

#[tokio::test]
async fn delete_bucket_invokes_m8_cleanup_sink() {
    let (s3, _d) = service().await;
    let sink = Arc::new(FakeCleanup::default());
    let s3 = s3.with_bucket_cleanup(Some(sink.clone()));
    s3.create_bucket(with_user(CreateBucketInput {
        bucket: "m8clean".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.delete_bucket(with_user(DeleteBucketInput {
        bucket: "m8clean".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_eq!(*sink.cleared.lock().unwrap(), ["m8clean".to_string()]);
    assert_eq!(*sink.invalidated.lock().unwrap(), ["m8clean".to_string()]);
}

// ---- M8 S4a：存储类 ----

#[tokio::test]
async fn storage_class_wire_put_head_list_copy() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "cls".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "cls".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
        storage_class: Some(StorageClass::from("STANDARD_IA".to_string())),
        ..Default::default()
    }))
    .await
    .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "cls".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        head.storage_class.as_ref().map(|s| s.as_str()),
        Some("STANDARD_IA")
    );
    let get = s3
        .get_object(req(GetObjectInput {
            bucket: "cls".into(),
            key: "k".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        get.storage_class.as_ref().map(|s| s.as_str()),
        Some("STANDARD_IA")
    );
    let list = s3
        .list_objects_v2(req(ListObjectsV2Input {
            bucket: "cls".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        list.contents.unwrap()[0]
            .storage_class
            .as_ref()
            .map(|s| s.as_str()),
        Some("STANDARD_IA")
    );

    // Copy 目标显式 class；未给 = STANDARD
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("cls".to_string())
        .key("dst".to_string())
        .copy_source(CopySource::Bucket {
            bucket: "cls".into(),
            key: "k".into(),
            version_id: None,
        })
        .storage_class(Some(StorageClass::from("ONEZONE_IA".to_string())))
        .build()
        .unwrap()))
        .await
        .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "cls".into(),
            key: "dst".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        head.storage_class.as_ref().map(|s| s.as_str()),
        Some("ONEZONE_IA")
    );
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("cls".to_string())
        .key("dst2".to_string())
        .copy_source(CopySource::Bucket {
            bucket: "cls".into(),
            key: "k".into(),
            version_id: None,
        })
        .build()
        .unwrap()))
        .await
        .unwrap();
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "cls".into(),
            key: "dst2".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.storage_class, None);

    // 未知名/归档类直写 → InvalidStorageClass
    for bad in ["WARM", "GLACIER"] {
        let err = s3
            .put_object(req(PutObjectInput {
                bucket: "cls".into(),
                key: "bad".into(),
                body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
                storage_class: Some(StorageClass::from(bad.to_string())),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidStorageClass, "{bad}");
    }
}

#[tokio::test]
async fn storage_class_mpu_create_validation() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "clm".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let err = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "clm".into(),
            key: "k".into(),
            storage_class: Some(StorageClass::from("GLACIER".to_string())),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidStorageClass);
    let err = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "clm".into(),
            key: "k".into(),
            storage_class: Some(StorageClass::from("WARM".to_string())),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &s3s::S3ErrorCode::InvalidStorageClass);
    // 合法类可创建（Complete 落地在 store 单测覆盖）
    s3.create_multipart_upload(req(CreateMultipartUploadInput {
        bucket: "clm".into(),
        key: "k".into(),
        storage_class: Some(StorageClass::from("INTELLIGENT_TIERING".to_string())),
        ..Default::default()
    }))
    .await
    .unwrap();
}
