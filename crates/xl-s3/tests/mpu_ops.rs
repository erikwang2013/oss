// crates/xl-s3/tests/mpu_ops.rs
//! MPU 缺口：UploadPartCopy（范围 / 条件 / 版本 / SSE-C / 上限）与
//! ListMultipartUploads（分页 / delimiter / max 边界）。
use std::sync::Arc;

use bytes::Bytes;
use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

/// 16 MiB 上限：范围用例的 part 需 ≥5 MiB（complete 的末段除外）。
async fn service() -> (OssS3, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    (OssS3::new(store, acl().await, 16 * 1024 * 1024), dir)
}

/// 加密接线版（SSE-C 读路径需要密钥库状态；与 object_ops::service_enc 同款）。
async fn service_enc() -> (OssS3, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    let sys = xl_store::layout::sys_dir(dir.path(), "");
    store
        .init_encryption(Some(sys.join("master.key")), Some(sys.join("kms")))
        .await
        .unwrap();
    (OssS3::new(store, acl().await, 16 * 1024 * 1024), dir)
}

/// 最小判权库（这些用例直调 service 方法、不走 s3s 授权链；Acl 只被记账/版本路径用到）。
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

async fn bucket(s3: &OssS3) {
    s3.create_bucket(req(CreateBucketInput {
        bucket: "alpha".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
}

async fn put(s3: &OssS3, key: &str, body: Vec<u8>) -> Option<String> {
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: key.into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from(body))),
        ..Default::default()
    }))
    .await
    .unwrap()
    .output
    .version_id
}

async fn create_mpu(s3: &OssS3, key: &str) -> String {
    s3.create_multipart_upload(req(CreateMultipartUploadInput {
        bucket: "alpha".into(),
        key: key.into(),
        ..Default::default()
    }))
    .await
    .unwrap()
    .output
    .upload_id
    .unwrap()
}

fn src(key: &str, version: Option<String>) -> CopySource {
    CopySource::Bucket {
        bucket: "alpha".into(),
        key: key.into(),
        version_id: version.map(Into::into),
    }
}

fn upc(dst_key: &str, upload_id: &str, part: i32, source: CopySource) -> UploadPartCopyInput {
    UploadPartCopyInput {
        bucket: "alpha".into(),
        key: dst_key.into(),
        upload_id: upload_id.into(),
        part_number: part,
        copy_source: source,
        copy_source_if_match: None,
        copy_source_if_modified_since: None,
        copy_source_if_none_match: None,
        copy_source_if_unmodified_since: None,
        copy_source_range: None,
        copy_source_sse_customer_algorithm: None,
        copy_source_sse_customer_key: None,
        copy_source_sse_customer_key_md5: None,
        expected_bucket_owner: None,
        expected_source_bucket_owner: None,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
    }
}

async fn complete(s3: &OssS3, key: &str, id: &str, parts: Vec<(i32, String)>) -> String {
    let out = s3
        .complete_multipart_upload(req(CompleteMultipartUploadInput {
            bucket: "alpha".into(),
            key: key.into(),
            upload_id: id.into(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(
                    parts
                        .into_iter()
                        .map(|(n, e)| CompletedPart {
                            part_number: Some(n),
                            e_tag: Some(ETag::Strong(e)),
                            ..Default::default()
                        })
                        .collect(),
                ),
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    out.e_tag.unwrap().value().to_string()
}

async fn get(s3: &OssS3, key: &str) -> Vec<u8> {
    let out = s3
        .get_object(req(GetObjectInput {
            bucket: "alpha".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let mut body = out.body.unwrap();
    let mut buf = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut body).await {
        buf.extend_from_slice(&chunk.unwrap());
    }
    buf
}

fn md5(b: &[u8]) -> String {
    use md5::{Digest as _, Md5};
    hex::encode(Md5::digest(b))
}

fn src_bytes() -> Vec<u8> {
    (0..6 * 1024 * 1024).map(|i| (i % 251) as u8).collect()
}

/// 整份源 → UPC → complete：内容/ETag/响应字段全链路。
#[tokio::test]
async fn upload_part_copy_whole_source_roundtrip() {
    let (s3, _d) = service().await;
    bucket(&s3).await;
    let body = b"copy-me-whole".to_vec();
    put(&s3, "src", body.clone()).await.unwrap();
    let id = create_mpu(&s3, "dst").await;

    let out = s3
        .upload_part_copy(req(upc("dst", &id, 1, src("src", None))))
        .await
        .unwrap()
        .output;
    let part = out.copy_part_result.unwrap();
    assert_eq!(
        part.e_tag.as_ref().map(ETag::value),
        Some(md5(&body).as_str())
    );
    assert!(part.last_modified.is_some());
    // 未启用版本控制的源桶不回 copy-source-version-id
    assert!(out.copy_source_version_id.is_none());
    assert!(out.sse_customer_key_md5.is_none());

    let etag = complete(&s3, "dst", &id, vec![(1, md5(&body))]).await;
    assert!(etag.ends_with("-1"), "complete etag：{etag}");
    assert_eq!(get(&s3, "dst").await, body);
}

/// 三种 Range 形态（闭区间 / 开区间 / 后缀）+ complete 后内容逐字节一致。
#[tokio::test]
async fn upload_part_copy_ranges() {
    let (s3, _d) = service().await;
    bucket(&s3).await;
    let body = src_bytes();
    put(&s3, "src", body.clone()).await.unwrap();
    let five_mib = 5 * 1024 * 1024usize;

    // A：bytes=0-5242879 + bytes=5242880-
    let id = create_mpu(&s3, "dst-a").await;
    let mut req_a = upc("dst-a", &id, 1, src("src", None));
    req_a.copy_source_range = Some(format!("bytes=0-{}", five_mib - 1));
    let p1 = s3
        .upload_part_copy(req(req_a))
        .await
        .unwrap()
        .output
        .copy_part_result
        .unwrap();
    let mut req_b = upc("dst-a", &id, 2, src("src", None));
    req_b.copy_source_range = Some(format!("bytes={five_mib}-"));
    let p2 = s3
        .upload_part_copy(req(req_b))
        .await
        .unwrap()
        .output
        .copy_part_result
        .unwrap();
    assert_eq!(
        p1.e_tag.as_ref().map(ETag::value),
        Some(md5(&body[..five_mib]).as_str())
    );
    assert_eq!(
        p2.e_tag.as_ref().map(ETag::value),
        Some(md5(&body[five_mib..]).as_str())
    );
    complete(
        &s3,
        "dst-a",
        &id,
        vec![(1, md5(&body[..five_mib])), (2, md5(&body[five_mib..]))],
    )
    .await;
    assert_eq!(get(&s3, "dst-a").await, body);

    // B：后缀 range（最后 1 MiB）
    let id = create_mpu(&s3, "dst-b").await;
    let mut rb = upc("dst-b", &id, 1, src("src", None));
    rb.copy_source_range = Some(format!("bytes=0-{}", five_mib - 1));
    s3.upload_part_copy(req(rb)).await.unwrap();
    let mut rb2 = upc("dst-b", &id, 2, src("src", None));
    rb2.copy_source_range = Some("bytes=-1048576".into());
    let p = s3
        .upload_part_copy(req(rb2))
        .await
        .unwrap()
        .output
        .copy_part_result
        .unwrap();
    assert_eq!(
        p.e_tag.as_ref().map(ETag::value),
        Some(md5(&body[body.len() - 1024 * 1024..]).as_str())
    );
    complete(
        &s3,
        "dst-b",
        &id,
        vec![
            (1, md5(&body[..five_mib])),
            (2, md5(&body[body.len() - 1024 * 1024..])),
        ],
    )
    .await;
    assert_eq!(get(&s3, "dst-b").await, body);
}

/// 多段 range → 400；越界 → 416。
#[tokio::test]
async fn upload_part_copy_range_errors() {
    let (s3, _d) = service().await;
    bucket(&s3).await;
    put(&s3, "src", b"0123456789".to_vec()).await.unwrap();
    let id = create_mpu(&s3, "dst").await;

    let mut multi = upc("dst", &id, 1, src("src", None));
    multi.copy_source_range = Some("bytes=0-1,3-4".into());
    let e = s3.upload_part_copy(req(multi)).await.unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument);

    let mut oob = upc("dst", &id, 1, src("src", None));
    oob.copy_source_range = Some("bytes=100-".into());
    let e = s3.upload_part_copy(req(oob)).await.unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRange);
}

/// copy-source 条件头：ETag 不等 / If-None-Match 命中 → 412（复制语义）。
#[tokio::test]
async fn upload_part_copy_conditions() {
    let (s3, _d) = service().await;
    bucket(&s3).await;
    put(&s3, "src", b"hello".to_vec()).await.unwrap();
    let id = create_mpu(&s3, "dst").await;

    let mut bad_match = upc("dst", &id, 1, src("src", None));
    bad_match.copy_source_if_match = Some(ETagCondition::ETag(ETag::Strong("deadbeef".into())));
    let e = s3.upload_part_copy(req(bad_match)).await.unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::PreconditionFailed);

    let mut none_match = upc("dst", &id, 1, src("src", None));
    none_match.copy_source_if_none_match = Some(ETagCondition::Any);
    let e = s3.upload_part_copy(req(none_match)).await.unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::PreconditionFailed);
}

/// 源版本：复制旧版本内容，响应回 copy_source_version_id。
#[tokio::test]
async fn upload_part_copy_source_version() {
    let (s3, _d) = service().await;
    // 带身份建桶：版本控制状态存在 buckets 行上，只有塞了 AuthedUser 才会记账。
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
    s3.put_bucket_versioning(req(PutBucketVersioningInput {
        bucket: "alpha".into(),
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
    let old = b"version-one".to_vec();
    let vid = put(&s3, "src", old.clone()).await.unwrap();
    put(&s3, "src", b"version-two".to_vec()).await.unwrap();

    let id = create_mpu(&s3, "dst").await;
    let out = s3
        .upload_part_copy(req(upc("dst", &id, 1, src("src", Some(vid.clone())))))
        .await
        .unwrap()
        .output;
    assert_eq!(out.copy_source_version_id.as_deref(), Some(vid.as_str()));
    complete(&s3, "dst", &id, vec![(1, md5(&old))]).await;
    assert_eq!(get(&s3, "dst").await, old, "必须复制点名旧版本");
}

/// partNumber 边界：0 / 10001 → 400。
#[tokio::test]
async fn upload_part_copy_part_bounds() {
    let (s3, _d) = service().await;
    bucket(&s3).await;
    put(&s3, "src", b"x".to_vec()).await.unwrap();
    let id = create_mpu(&s3, "dst").await;
    for pn in [0, 10_001] {
        let e = s3
            .upload_part_copy(req(upc("dst", &id, pn, src("src", None))))
            .await
            .unwrap_err();
        assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument, "part {pn}");
    }
}

/// 超过 max_put_bytes：读盘前就 413（第二个 OssS3 共享 store、更小上限）。
#[tokio::test]
async fn upload_part_copy_entity_too_large() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    let big = OssS3::new(store.clone(), acl().await, 16 * 1024 * 1024);
    let small = OssS3::new(store, acl().await, 8);
    bucket(&big).await;
    put(&big, "src", b"0123456789".to_vec()).await.unwrap();
    let id = create_mpu(&big, "dst").await;

    let e = small
        .upload_part_copy(req(upc("dst", &id, 1, src("src", None))))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::EntityTooLarge);
}

/// SSE-C：源钥缺/错 400；目标钥与 create 记录不一致 400；正确钥全链路可完成。
#[tokio::test]
async fn upload_part_copy_sse_c_rules() {
    use base64::Engine as _;
    let (s3, _d) = service_enc().await;
    bucket(&s3).await;
    let (src_key, dst_key) = ([1u8; 32], [2u8; 32]);
    let src_b64 = base64::engine::general_purpose::STANDARD.encode(src_key);
    let src_md5 = xl_store::crypto::SseCKey::from_key(src_key)
        .md5_b64()
        .to_string();
    let dst_b64 = base64::engine::general_purpose::STANDARD.encode(dst_key);
    let dst_md5 = xl_store::crypto::SseCKey::from_key(dst_key)
        .md5_b64()
        .to_string();
    let body = b"ss-e-c-source".to_vec();
    s3.put_object(req(PutObjectInput {
        bucket: "alpha".into(),
        key: "src".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from(body.clone()))),
        sse_customer_algorithm: Some("AES256".into()),
        sse_customer_key: Some(src_b64.clone()),
        sse_customer_key_md5: Some(src_md5.clone()),
        ..Default::default()
    }))
    .await
    .unwrap();

    // 源为 SSE-C 而未带源钥 → 400
    let id = create_mpu(&s3, "plain").await;
    let e = s3
        .upload_part_copy(req(upc("plain", &id, 1, src("src", None))))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument);

    // 源钥错（用目标钥冒充）→ 400
    let mut wrong = upc("plain", &id, 1, src("src", None));
    wrong.copy_source_sse_customer_algorithm = Some("AES256".into());
    wrong.copy_source_sse_customer_key = Some(dst_b64.clone());
    wrong.copy_source_sse_customer_key_md5 = Some(dst_md5.clone());
    let e = s3.upload_part_copy(req(wrong)).await.unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument);

    // 正确源钥 → 成功；目标 MPU 未用 SSE-C，complete 后为明文
    let mut ok = upc("plain", &id, 1, src("src", None));
    ok.copy_source_sse_customer_algorithm = Some("AES256".into());
    ok.copy_source_sse_customer_key = Some(src_b64.clone());
    ok.copy_source_sse_customer_key_md5 = Some(src_md5.clone());
    let out = s3.upload_part_copy(req(ok)).await.unwrap().output;
    assert_eq!(
        out.copy_part_result
            .unwrap()
            .e_tag
            .as_ref()
            .map(ETag::value),
        Some(md5(&body).as_str())
    );
    complete(&s3, "plain", &id, vec![(1, md5(&body))]).await;
    assert_eq!(get(&s3, "plain").await, body);

    // 目标 MPU 以 SSE-C 创建：用错钥的 part copy → 400（源钥正确，目标钥不一致）
    let id2 = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "alpha".into(),
            key: "enc".into(),
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(dst_b64.clone()),
            sse_customer_key_md5: Some(dst_md5.clone()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let mut wrong_target = upc("enc", &id2, 1, src("src", None));
    wrong_target.copy_source_sse_customer_algorithm = Some("AES256".into());
    wrong_target.copy_source_sse_customer_key = Some(src_b64.clone());
    wrong_target.copy_source_sse_customer_key_md5 = Some(src_md5.clone());
    wrong_target.sse_customer_algorithm = Some("AES256".into());
    wrong_target.sse_customer_key = Some(src_b64.clone());
    wrong_target.sse_customer_key_md5 = Some(src_md5.clone());
    let e = s3.upload_part_copy(req(wrong_target)).await.unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument);

    // 正确目标钥：响应回 key-MD5
    let mut ok_target = upc("enc", &id2, 1, src("src", None));
    ok_target.copy_source_sse_customer_algorithm = Some("AES256".into());
    ok_target.copy_source_sse_customer_key = Some(src_b64);
    ok_target.copy_source_sse_customer_key_md5 = Some(src_md5);
    ok_target.sse_customer_algorithm = Some("AES256".into());
    ok_target.sse_customer_key = Some(dst_b64);
    ok_target.sse_customer_key_md5 = Some(dst_md5.clone());
    let out = s3.upload_part_copy(req(ok_target)).await.unwrap().output;
    assert_eq!(out.sse_customer_key_md5.as_deref(), Some(dst_md5.as_str()));
}

/// ListMultipartUploads 基本形状 + 空桶。
#[tokio::test]
async fn list_uploads_basic_and_empty() {
    let (s3, _d) = service().await;
    bucket(&s3).await;
    let a = create_mpu(&s3, "a/1").await;
    let b = create_mpu(&s3, "b").await;
    let out = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "alpha".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.bucket.as_deref(), Some("alpha"));
    assert_eq!(out.is_truncated, Some(false));
    assert_eq!(out.max_uploads, Some(1000));
    assert!(out.common_prefixes.as_ref().unwrap().is_empty());
    let ups = out.uploads.unwrap();
    assert_eq!(
        ups.iter()
            .map(|u| (u.key.as_deref().unwrap(), u.upload_id.as_deref().unwrap()))
            .collect::<Vec<_>>(),
        [("a/1", a.as_str()), ("b", b.as_str())]
    );
    assert!(ups.iter().all(|u| u.initiated.is_some()));
    assert_eq!(
        ups[0].storage_class.as_ref().map(StorageClass::as_str),
        Some("STANDARD")
    );

    s3.create_bucket(req(CreateBucketInput {
        bucket: "empty".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let out = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "empty".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.uploads.unwrap().is_empty());
    assert_eq!(out.is_truncated, Some(false));
}

/// 同键分页：NextKeyMarker/NextUploadIdMarker 逐页走完无重无漏。
#[tokio::test]
async fn list_uploads_pagination_same_key() {
    let (s3, _d) = service().await;
    bucket(&s3).await;
    for _ in 0..3 {
        create_mpu(&s3, "k").await;
    }
    let full = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "alpha".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .uploads
        .unwrap();
    let ordered: Vec<String> = full.iter().map(|u| u.upload_id.clone().unwrap()).collect();

    let mut got = Vec::new();
    let mut input = ListMultipartUploadsInput {
        bucket: "alpha".into(),
        max_uploads: Some(1),
        ..Default::default()
    };
    for _ in 0..10 {
        let out = s3
            .list_multipart_uploads(req(input.clone()))
            .await
            .unwrap()
            .output;
        for u in out.uploads.unwrap() {
            got.push(u.upload_id.unwrap());
        }
        if out.is_truncated != Some(true) {
            break;
        }
        input.key_marker = out.next_key_marker;
        input.upload_id_marker = out.next_upload_id_marker;
    }
    assert_eq!(got, ordered);
}

/// delimiter 折叠 / prefix / max 边界（0、负数、>1000）。
#[tokio::test]
async fn list_uploads_delimiter_and_max_bounds() {
    let (s3, _d) = service().await;
    bucket(&s3).await;
    for k in ["a/1", "a/2", "b"] {
        create_mpu(&s3, k).await;
    }

    let out = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "alpha".into(),
            delimiter: Some("/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(
        out.common_prefixes
            .unwrap()
            .iter()
            .map(|p| p.prefix.clone().unwrap())
            .collect::<Vec<_>>(),
        ["a/"]
    );
    assert_eq!(out.uploads.unwrap().len(), 1);

    let out = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "alpha".into(),
            prefix: Some("a/".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.uploads.unwrap().len(), 2);

    let out = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "alpha".into(),
            max_uploads: Some(0),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(out.uploads.unwrap().is_empty());
    assert_eq!(out.is_truncated, Some(false));

    let e = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "alpha".into(),
            max_uploads: Some(-1),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);

    let out = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "alpha".into(),
            max_uploads: Some(5000),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(out.max_uploads, Some(1000), "响应回显夹后的上限");
    assert_eq!(out.uploads.unwrap().len(), 3);
    assert_eq!(out.is_truncated, Some(false));
}
