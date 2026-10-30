//! 归档 tier 对象 Range 直读实现（默认关，router 按 `OSS_TIER_DIRECT_READ=1` 注入）。
//!
//! 只取请求区间所在的 canonical 8 MiB 分块（前后各至多补一块），逐块对照
//! `xl.meta` 中的 SHA-256；任何不符 → `Integrity`（500），条件不满足 → `Unavailable`
//! （回退既有 403 `InvalidObjectState`）。缺分块表的旧对象必须 Restore 一次 backfill。

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sha2::Digest as _;
use xl_s3::tier_direct::{TierDirectError, TierDirectRead};
use xl_store::meta::TierPhase;
use xl_store::store::{ObjectInfo, Store};

use crate::tier::TierBackends;

pub struct TierDirectReader {
    store: Arc<Store>,
    tiers: Arc<TierBackends>,
}

impl TierDirectReader {
    pub fn new(store: Arc<Store>, tiers: Arc<TierBackends>) -> Self {
        Self { store, tiers }
    }
}

#[async_trait]
impl TierDirectRead for TierDirectReader {
    async fn read_range(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        info: &ObjectInfo,
        range: Range<u64>,
    ) -> Result<Bytes, TierDirectError> {
        if range.start >= range.end || range.end > info.size {
            return Err(TierDirectError::Unavailable(format!(
                "直读区间非法：{range:?}/{}",
                info.size
            )));
        }
        if info.encryption.is_some() {
            return Err(TierDirectError::Unavailable("SSE 对象不支持直读".into()));
        }
        let vid = version_id.unwrap_or("null");
        let v = self
            .store
            .tier_version(bucket, key, vid)
            .await
            .map_err(|e| TierDirectError::Unavailable(format!("读取 tier 元数据失败：{e}")))?
            .ok_or_else(|| TierDirectError::Unavailable("版本已不存在".into()))?;
        let Some(t) = v.tier else {
            return Err(TierDirectError::Unavailable("无 tier 记录".into()));
        };
        if t.phase != TierPhase::Archived {
            return Err(TierDirectError::Unavailable(format!(
                "phase={:?} 非 Archived",
                t.phase
            )));
        }
        // 元数据与 HEAD 快照必须描述同一份对象；尺寸/源 ETag 漂移不服务（回到 403）。
        // （tier.etag 是远端 PUT ETag，与源 ETag 口径不同，不能拿来比。）
        if t.size != info.size || t.size != v.size {
            return Err(TierDirectError::Unavailable(
                "tier 元数据与 HEAD 快照/源版本尺寸不一致".into(),
            ));
        }
        if v.etag != info.etag {
            return Err(TierDirectError::Unavailable(
                "源 ETag 与 HEAD 快照不一致".into(),
            ));
        }
        // 归档副本必须绑定源已提交的整对象摘要（legacy parts=Some / 空摘要跳过）。
        if v.parts.is_none() && !v.part.sha256.is_empty() && t.sha256 != v.part.sha256 {
            return Err(TierDirectError::Integrity(format!(
                "tier 副本 SHA-256 与源版本摘要不符（tier={}，源={}）",
                t.sha256, v.part.sha256
            )));
        }
        let Some(chunks) = t.chunks.as_ref() else {
            return Err(TierDirectError::Unavailable(
                "缺分块哈希表（需 Restore 或重新归档）".into(),
            ));
        };
        chunks.validate(t.size).map_err(|e| {
            TierDirectError::Integrity(format!("分块哈希表不自洽（meta 应已拒收）：{e}"))
        })?;
        // 远端 Range 也会整段读进内存；套用与本地读相同的缓冲上限。
        self.store
            .check_buffered_read(range.end - range.start)
            .map_err(|e| TierDirectError::Unavailable(e.to_string()))?;
        let backend = self
            .tiers
            .get(&t.backend)
            .ok_or_else(|| TierDirectError::Unavailable(format!("tier {:?} 未配置", t.backend)))?;
        if !backend.supports_range() {
            return Err(TierDirectError::Unavailable("tier 后端不支持 Range".into()));
        }
        let cs = chunks.chunk_size;
        let first = range.start / cs;
        let last = (range.end - 1) / cs + 1; // exclusive
        let fetch_start = first * cs;
        let fetch_end = (last * cs).min(t.size);
        let want = fetch_end - fetch_start;
        let bytes = backend
            .get_range(&t.key, fetch_start, want)
            .await
            .map_err(|e| TierDirectError::Unavailable(format!("tier Range 读取失败：{e}")))?;
        if bytes.len() as u64 != want {
            return Err(TierDirectError::Integrity(format!(
                "tier Range 返回长度 {} ≠ {want}",
                bytes.len()
            )));
        }
        // 信任边界：远端内容/后端行为都不可信，逐块重算 SHA-256。
        for idx in first..last {
            let chunk_start = idx * cs;
            let chunk_end = (chunk_start + cs).min(t.size);
            let a = (chunk_start - fetch_start) as usize;
            let b = (chunk_end - fetch_start) as usize;
            let got = hex::encode(sha2::Sha256::digest(&bytes[a..b]));
            let want_hash = &chunks.sha256[idx as usize];
            if &got != want_hash {
                return Err(TierDirectError::Integrity(format!(
                    "分块 {idx} SHA-256 不符（期望 {want_hash}，实得 {got}）"
                )));
            }
        }
        let a = (range.start - fetch_start) as usize;
        let b = (range.end - fetch_start) as usize;
        Ok(bytes.slice(a..b))
    }
}
