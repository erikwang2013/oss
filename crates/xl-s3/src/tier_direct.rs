//! 归档 tier 对象 Range 直读契约（默认关；oss-server 按 `OSS_TIER_DIRECT_READ=1` 注入）。
//!
//! 读路径只信任 xl.meta 里的分块哈希：实现必须先校验再返回字节；调用方对
//! `Unavailable` 回退既有 403 `InvalidObjectState`（fail closed），对 `Integrity`
//! 返回 500（绝不把损坏/篡改降级成「不可读」）。

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use xl_store::store::ObjectInfo;

#[derive(Debug)]
pub enum TierDirectError {
    /// 直读条件不满足（无分块表/后端不支持 Range/元数据变化/远端错误）→ 回退 403。
    Unavailable(String),
    /// 分块哈希或数据形态不符 → 500（不得掩盖）。
    Integrity(String),
}

#[async_trait]
pub trait TierDirectRead: Send + Sync {
    /// 读 `range`（end exclusive，调用方已按对象大小裁剪），仅对 Archived 对象调用。
    async fn read_range(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        info: &ObjectInfo,
        range: Range<u64>,
    ) -> Result<Bytes, TierDirectError>;
}
