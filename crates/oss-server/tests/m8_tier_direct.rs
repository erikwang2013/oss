//! 归档对象 Range 直读：分块哈希校验、对齐读取、fail-closed 回退、HEAD/条件语义。

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt as _;
use s3s::dto::*;
use s3s::{S3, S3ErrorCode, S3Request, S3Response};
use xl_s3::service::OssS3;
use xl_s3::tier_direct::{TierDirectError, TierDirectRead as _};
use xl_store::meta::{TierChunks, TierPhase};

use oss_server::config::{TierKind, TierTarget};
use oss_server::jobs::TaskExecutor as _;
use oss_server::state::AppState;
use oss_server::tier::{
    S3TierGc, TierBackend, TierBackends, TierByteStream, TierPutResult, tier_key,
};
use oss_server::tier_direct::TierDirectReader;
use oss_server::tier_jobs::{M8TaskExecutor, TIER_CHUNK};

struct MockTier {
    objs: Mutex<HashMap<String, Bytes>>,
    range_calls: Mutex<Vec<(u64, u64)>>,
    range_ok: AtomicBool,
}

impl Default for MockTier {
    fn default() -> Self {
        Self {
            objs: Mutex::new(HashMap::new()),
            range_calls: Mutex::new(Vec::new()),
            range_ok: AtomicBool::new(true),
        }
    }
}

#[async_trait::async_trait]
impl TierBackend for MockTier {
    fn id(&self) -> &str {
        "cold"
    }

    async fn put(
        &self,
        key: &str,
        mut body: TierByteStream,
        size: u64,
    ) -> Result<TierPutResult, String> {
        let mut buf = Vec::with_capacity(size as usize);
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk?);
        }
        if buf.len() as u64 != size {
            return Err(format!("mock 长度 {} ≠ {size}", buf.len()));
        }
        use sha2::Digest as _;
        let md5 = hex::encode(md5::Md5::digest(&buf));
        let sha256 = hex::encode(sha2::Sha256::digest(&buf));
        self.objs
            .lock()
            .unwrap()
            .insert(key.to_string(), Bytes::from(buf));
        Ok(TierPutResult {
            etag: format!("mock-{md5}"),
            md5,
            sha256,
        })
    }

    async fn get(&self, key: &str) -> Result<TierByteStream, String> {
        let b = self
            .objs
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| format!("missing {key}"))?;
        Ok(Box::pin(futures::stream::once(async move { Ok(b) })))
    }

    fn supports_range(&self) -> bool {
        self.range_ok.load(Ordering::SeqCst)
    }

    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes, String> {
        self.range_calls.lock().unwrap().push((offset, len));
        let b = self
            .objs
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| format!("missing {key}"))?;
        let end = offset
            .checked_add(len)
            .filter(|_| len > 0)
            .ok_or_else(|| "mock Range 参数非法".to_string())?;
        if end > b.len() as u64 {
            return Err("mock Range 越界".into());
        }
        Ok(b.slice(offset as usize..end as usize))
    }

    async fn head(&self, key: &str) -> Result<Option<u64>, String> {
        Ok(self.objs.lock().unwrap().get(key).map(|b| b.len() as u64))
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.objs.lock().unwrap().remove(key);
        Ok(())
    }
}

fn tier_target() -> TierTarget {
    TierTarget {
        id: "cold".into(),
        kind: TierKind::S3,
        path: None,
        endpoint: "http://127.0.0.1:9".into(),
        bucket: "tb".into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
        server_name: None,
    }
}

async fn setup() -> (AppState, Arc<MockTier>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut st = common::state_with_tiers(dir.path(), vec![tier_target()]).await;
    let mock = Arc::new(MockTier::default());
    let backends = Arc::new(TierBackends::new(vec![
        Arc::clone(&mock) as Arc<dyn TierBackend>
    ]));
    st.tiers = Arc::clone(&backends);
    st.tier_gc = Arc::new(S3TierGc::new(Arc::clone(&backends)));
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    (st, mock, dir)
}

fn service(st: &AppState) -> OssS3 {
    OssS3::new(Arc::clone(&st.store), st.acl(), st.cfg.max_put_bytes)
        .with_lifecycle(Some(Arc::clone(&st.lifecycle)))
        .with_restore(Some(Arc::clone(&st.restore_sink)))
        .with_tier_gc(Some(Arc::clone(&st.tier_gc)))
}

/// 显式注入直读（与 `OSS_TIER_DIRECT_READ=1` 的 router 接线等价；测试不改进程环境）。
fn direct_service(st: &AppState) -> OssS3 {
    service(st).with_tier_direct(Some(Arc::new(TierDirectReader::new(
        Arc::clone(&st.store),
        Arc::clone(&st.tiers),
    )) as Arc<dyn xl_s3::tier_direct::TierDirectRead>))
}

fn executor(st: &AppState) -> M8TaskExecutor {
    M8TaskExecutor::new(
        Arc::clone(&st.db),
        Arc::clone(&st.store),
        Arc::clone(&st.tiers),
        Arc::clone(&st.events),
    )
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: axum::http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn payload(bucket: &str, key: &str, vid: &str) -> String {
    serde_json::json!({
        "bucket": bucket, "key": key, "version_id": vid, "class": "GLACIER", "tier": "cold"
    })
    .to_string()
}

async fn archive(st: &AppState, key: &str, data: &[u8]) -> String {
    let info = st
        .store
        .put_object("bkt", key, Bytes::copy_from_slice(data), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    let out = executor(st)
        .execute("Transition", &payload("bkt", key, &vid))
        .await
        .unwrap();
    assert_eq!(out, "archived");
    vid
}

async fn body_of(resp: S3Response<GetObjectOutput>) -> Vec<u8> {
    let mut body = resp.output.body.unwrap();
    let mut buf = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut body).await {
        buf.extend_from_slice(&chunk.unwrap());
    }
    buf
}

fn ranged(first: u64, last: u64) -> GetObjectInput {
    GetObjectInput {
        bucket: "bkt".into(),
        key: "cold/obj".into(),
        range: Some(Range::Int {
            first,
            last: Some(last),
        }),
        ..Default::default()
    }
}

fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

/// 直读只取请求所在的对齐分块，字节与 Restore 输出一致，ETag/Content-Range 保持源语义。
#[tokio::test]
async fn direct_range_matches_restore_and_fetches_aligned_chunks() {
    let (st, mock, _d) = setup().await;
    let data = pattern(TIER_CHUNK as usize + 1000);
    let vid = archive(&st, "cold/obj", &data).await;
    let s3 = direct_service(&st);

    let v = st
        .store
        .tier_version("bkt", "cold/obj", &vid)
        .await
        .unwrap()
        .unwrap();
    let t = v.tier.as_ref().unwrap();
    let chunks: &TierChunks = t.chunks.as_ref().expect("Transition 必须写分块表");
    assert_eq!(chunks.chunk_size, TIER_CHUNK);
    assert_eq!(chunks.sha256.len(), 2, "8 MiB + 1000 字节 = 2 块");
    assert!(chunks.validate(t.size).is_ok());
    let source_etag = v.etag.clone();

    // 第二块内部：只应取第二块（对齐起点 8 MiB，长度 = 1000）。
    let (first, last) = (TIER_CHUNK + 100, TIER_CHUNK + 199);
    let resp = s3.get_object(req(ranged(first, last))).await.unwrap();
    assert_eq!(resp.status, Some(axum::http::StatusCode::PARTIAL_CONTENT));
    assert_eq!(resp.output.e_tag, Some(ETag::Strong(source_etag.clone())));
    assert_eq!(
        resp.output.content_range.as_deref(),
        Some(format!("bytes {first}-{last}/{}", data.len()).as_str())
    );
    assert_eq!(
        body_of(resp).await,
        &data[first as usize..=last as usize],
        "直读输出必须与源字节一致（等价于 Restore 后读取）"
    );
    assert_eq!(
        *mock.range_calls.lock().unwrap(),
        vec![(TIER_CHUNK, 1000)],
        "不得整对象下载"
    );

    // 跨块区间：一次取对齐后的连续窗口并逐块校验。
    let (first, last) = (TIER_CHUNK - 10, TIER_CHUNK + 5);
    let resp = s3.get_object(req(ranged(first, last))).await.unwrap();
    assert_eq!(
        body_of(resp).await,
        &data[first as usize..=last as usize],
        "跨块读取"
    );
    assert_eq!(
        *mock.range_calls.lock().unwrap(),
        vec![(TIER_CHUNK, 1000), (0, TIER_CHUNK + 1000)]
    );

    // 整段 Range 仍 206，且可直读（不必 Restore）。
    let resp = s3
        .get_object(req(ranged(0, data.len() as u64 - 1)))
        .await
        .unwrap();
    assert_eq!(resp.status, Some(axum::http::StatusCode::PARTIAL_CONTENT));
    assert_eq!(body_of(resp).await, data);
}

/// 分块哈希不符 → 500（绝不回退成 403 掩盖损坏）。
#[tokio::test]
async fn corrupted_tier_chunk_is_rejected_with_500() {
    let (st, mock, _d) = setup().await;
    let data = pattern(TIER_CHUNK as usize + 100);
    let vid = archive(&st, "cold/obj", &data).await;
    let s3 = direct_service(&st);
    let remote = tier_key("cold", "bkt", &vid, "cold/obj");
    {
        let mut objs = mock.objs.lock().unwrap();
        let b = objs.get_mut(&remote).unwrap();
        let mut bad = b.to_vec();
        let i = TIER_CHUNK as usize + 50;
        bad[i] ^= 0xff;
        *b = Bytes::from(bad);
    }
    let err = s3
        .get_object(req(ranged(TIER_CHUNK + 40, TIER_CHUNK + 60)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    // 未损坏的块不受影响（校验按块进行）。
    let resp = s3.get_object(req(ranged(10, 20))).await.unwrap();
    assert_eq!(body_of(resp).await, &data[10..=20]);
}

/// 旧对象（无分块表）直读 fail closed → 403；Restore 重算 backfill，过期回收后仍可直读。
#[tokio::test]
async fn old_object_without_chunks_403_until_restore_backfills() {
    let (st, mock, _d) = setup().await;
    let data = pattern(5000);
    let info = st
        .store
        .put_object("bkt", "cold/obj", Bytes::copy_from_slice(&data), None)
        .await
        .unwrap();
    let vid = info.version_id.clone().unwrap();
    let remote = tier_key("cold", "bkt", &vid, "cold/obj");
    // 模拟 S4b 之前的归档：远端有对象、meta 无 chunks。
    mock.objs
        .lock()
        .unwrap()
        .insert(remote.clone(), Bytes::copy_from_slice(&data));
    use sha2::Digest as _;
    st.store
        .archive_version(
            "bkt",
            "cold/obj",
            &vid,
            xl_store::store::ArchiveSpec {
                backend: "cold".into(),
                key: remote.clone(),
                size: data.len() as u64,
                source_etag: info.etag.clone(),
                etag: "remote-etag".into(),
                sha256: hex::encode(sha2::Sha256::digest(&data)),
                class: "GLACIER".into(),
                chunks: None,
            },
            100,
        )
        .await
        .unwrap();
    let s3 = direct_service(&st);
    let err = s3.get_object(req(ranged(0, 9))).await.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InvalidObjectState);
    assert!(mock.range_calls.lock().unwrap().is_empty(), "缺表不碰后端");

    // Restore 作业按 canonical 边界重算分块表并 backfill。
    st.store
        .mark_restore_pending("bkt", "cold/obj", &vid)
        .await
        .unwrap();
    let restore = serde_json::json!({
        "bucket": "bkt", "key": "cold/obj", "version_id": vid, "days": 1
    })
    .to_string();
    assert_eq!(
        executor(&st)
            .execute("RestoreObject", &restore)
            .await
            .unwrap(),
        "restored"
    );
    let v = st
        .store
        .tier_version("bkt", "cold/obj", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.tier.as_ref().unwrap().phase, TierPhase::Restored);
    let chunks = v.tier.as_ref().unwrap().chunks.clone().unwrap();
    assert_eq!(chunks.sha256.len(), 1);

    // 过期回收回 Archived：本地副本没了，但表已 backfill → 直读仍可用。
    assert!(
        st.store
            .expire_restore("bkt", "cold/obj", &vid, i64::MAX / 2)
            .await
            .unwrap()
    );
    let resp = s3.get_object(req(ranged(100, 199))).await.unwrap();
    assert_eq!(body_of(resp).await, &data[100..=199]);
    assert_eq!(*mock.range_calls.lock().unwrap(), vec![(0, 5000)]);
}

/// 开关关闭（未注入直读）与后端不支持 Range 时都保持既有 403。
#[tokio::test]
async fn flag_off_and_backend_without_range_stay_403() {
    let (st, mock, _d) = setup().await;
    let data = pattern(4096);
    archive(&st, "cold/obj", &data).await;

    let err = service(&st)
        .get_object(req(ranged(0, 9)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InvalidObjectState, "默认必须 403");
    // 全量 GET/partNumber 也不受直读影响（本片只开 Range 读）。
    let err = service(&st)
        .get_object(req(GetObjectInput {
            bucket: "bkt".into(),
            key: "cold/obj".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InvalidObjectState);

    mock.range_ok.store(false, Ordering::SeqCst);
    let err = direct_service(&st)
        .get_object(req(ranged(0, 9)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InvalidObjectState);
    assert!(mock.range_calls.lock().unwrap().is_empty());
}

/// HEAD 只读元数据（不取远端）；条件 304/412 在直读前裁决；正确 If-Match 放行 206。
#[tokio::test]
async fn head_and_conditional_semantics_are_preserved() {
    let (st, mock, _d) = setup().await;
    let data = pattern(TIER_CHUNK as usize + 77);
    let vid = archive(&st, "cold/obj", &data).await;
    let s3 = direct_service(&st);
    let v = st
        .store
        .tier_version("bkt", "cold/obj", &vid)
        .await
        .unwrap()
        .unwrap();
    let etag = v.etag.clone();

    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "bkt".into(),
            key: "cold/obj".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(head.output.content_length, Some(data.len() as i64));
    assert_eq!(head.output.e_tag, Some(ETag::Strong(etag.clone())));
    assert_eq!(
        head.output.storage_class.as_ref().map(|c| c.as_str()),
        Some("GLACIER")
    );

    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "bkt".into(),
            key: "cold/obj".into(),
            range: Some(Range::Int {
                first: 5,
                last: Some(14),
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(head.status, None, "HEAD + Range 仍 200");
    assert_eq!(head.output.content_length, Some(10));
    assert_eq!(
        head.output.content_range.as_deref(),
        Some(format!("bytes 5-14/{}", data.len()).as_str())
    );
    assert!(
        mock.range_calls.lock().unwrap().is_empty(),
        "HEAD 不取 body"
    );

    // If-None-Match 命中 → 304，且不读远端。
    let resp = s3
        .get_object(req(GetObjectInput {
            if_none_match: Some(ETagCondition::ETag(ETag::Strong(etag.clone()))),
            ..ranged(0, 9)
        }))
        .await
        .unwrap();
    assert_eq!(resp.status, Some(axum::http::StatusCode::NOT_MODIFIED));
    assert!(mock.range_calls.lock().unwrap().is_empty());

    // If-Match 不符 → 412（PreconditionFailed），不读远端。
    let err = s3
        .get_object(req(GetObjectInput {
            if_match: Some(ETagCondition::ETag(ETag::Strong("deadbeef".into()))),
            ..ranged(0, 9)
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);
    assert!(mock.range_calls.lock().unwrap().is_empty());

    // If-Match 相符 → 206，内容正确。
    let resp = s3
        .get_object(req(GetObjectInput {
            if_match: Some(ETagCondition::ETag(ETag::Strong(etag))),
            ..ranged(0, 9)
        }))
        .await
        .unwrap();
    assert_eq!(resp.status, Some(axum::http::StatusCode::PARTIAL_CONTENT));
    assert_eq!(body_of(resp).await, &data[0..=9]);
}

/// C1：同一 null 版本被同尺寸替换并重新归档后，过期 HEAD 快照不得按新副本直读
/// （修复前会返回 B 的字节、却沿用 A 的 ETag）。
#[tokio::test]
async fn stale_head_after_same_size_replacement_is_unavailable() {
    let (st, _mock, _d) = setup().await;
    let a = vec![0xAAu8; 4096];
    let b = vec![0xBBu8; 4096];
    let info_a = st
        .store
        .put_object("bkt", "cold/obj", Bytes::from(a.clone()), None)
        .await
        .unwrap();
    let vid = info_a.version_id.clone().unwrap();
    assert_eq!(
        executor(&st)
            .execute("Transition", &payload("bkt", "cold/obj", &vid))
            .await
            .unwrap(),
        "archived"
    );

    // 无版本桶：同尺寸覆盖同一 key 后重新归档（仍是 "null" 版本，tier key 也相同）。
    let info_b = st
        .store
        .put_object("bkt", "cold/obj", Bytes::from(b.clone()), None)
        .await
        .unwrap();
    assert_eq!(info_b.version_id.as_deref(), Some(vid.as_str()));
    assert_ne!(info_a.etag, info_b.etag);
    assert_eq!(
        executor(&st)
            .execute("Transition", &payload("bkt", "cold/obj", &vid))
            .await
            .unwrap(),
        "archived"
    );

    let reader = TierDirectReader::new(Arc::clone(&st.store), Arc::clone(&st.tiers));
    let err = reader
        .read_range("bkt", "cold/obj", Some(&vid), &info_a, 0..16)
        .await
        .unwrap_err();
    assert!(matches!(err, TierDirectError::Unavailable(_)), "{err:?}");

    // 与条件请求组合：If-Match 指向旧 ETag → 412；新鲜 GET 服务 B 的字节与 ETag。
    let s3 = direct_service(&st);
    let err = s3
        .get_object(req(GetObjectInput {
            if_match: Some(ETagCondition::ETag(ETag::Strong(info_a.etag.clone()))),
            ..ranged(0, 15)
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);
    let resp = s3.get_object(req(ranged(0, 15))).await.unwrap();
    assert_eq!(resp.status, Some(axum::http::StatusCode::PARTIAL_CONTENT));
    assert_eq!(resp.output.e_tag, Some(ETag::Strong(info_b.etag.clone())));
    assert_eq!(body_of(resp).await, &b[0..=15]);
}

/// C1：meta 中源 ETag 被改写而 HEAD 快照仍旧 → 直读不可用（不返回错配字节）。
#[tokio::test]
async fn stale_source_etag_in_meta_is_unavailable() {
    let (st, _mock, d) = setup().await;
    let data = pattern(1000);
    let vid = archive(&st, "cold/obj", &data).await;
    let info = st.store.head_object("bkt", "cold/obj").await.unwrap();
    common::rewrite_meta(&d.path().join("data"), "bkt", "cold/obj", |m| {
        let v = m.versions.iter_mut().find(|v| v.tier.is_some()).unwrap();
        v.etag = "deadbeef".into();
    });
    let reader = TierDirectReader::new(Arc::clone(&st.store), Arc::clone(&st.tiers));
    let err = reader
        .read_range("bkt", "cold/obj", Some(&vid), &info, 0..8)
        .await
        .unwrap_err();
    assert!(matches!(err, TierDirectError::Unavailable(_)), "{err:?}");
}

/// C4：tier.sha256 与源 part.sha256 不一致 → 直读 500（不碰远端，不静默回退）。
#[tokio::test]
async fn tier_digest_must_match_source_digest() {
    let (st, mock, d) = setup().await;
    let data = pattern(1000);
    archive(&st, "cold/obj", &data).await;
    common::rewrite_meta(&d.path().join("data"), "bkt", "cold/obj", |m| {
        let v = m.versions.iter_mut().find(|v| v.tier.is_some()).unwrap();
        v.tier.as_mut().unwrap().sha256 = "11".repeat(32);
    });

    let err = direct_service(&st)
        .get_object(req(ranged(0, 9)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    assert!(
        mock.range_calls.lock().unwrap().is_empty(),
        "摘要不符不得读远端"
    );
}

/// C4 legacy 例外：`parts=Some` 的旧对象跳过摘要绑定，不误伤。
#[tokio::test]
async fn legacy_parts_object_skips_digest_binding() {
    let (st, _mock, d) = setup().await;
    let data = pattern(2000);
    archive(&st, "cold/obj", &data).await;
    common::rewrite_meta(&d.path().join("data"), "bkt", "cold/obj", |m| {
        let v = m.versions.iter_mut().find(|v| v.tier.is_some()).unwrap();
        let first = v.part.clone();
        v.parts = Some(vec![first]);
        v.tier.as_mut().unwrap().sha256 = "22".repeat(32);
    });

    let resp = direct_service(&st)
        .get_object(req(ranged(100, 199)))
        .await
        .unwrap();
    assert_eq!(body_of(resp).await, &data[100..=199]);
}
