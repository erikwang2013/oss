//! M9 S3：删除统一入口（S3 面 / 生命周期 / Batch / 控制台共用）。
//!
//! 实现方（oss-server `SiteSyncDelete`）对 sync 桶走**远端优先**：intent 落盘 →
//! 对端 token 路由删除 → 本地提交；非 sync 桶原样委托 store。接收端内部路由只调
//! 本地方法——本 trait 不暴露 local-only 路径，外部 caller 无从绕过。

use async_trait::async_trait;
use xl_store::error::Result;
use xl_store::store::DeleteOutcome;

#[async_trait]
pub trait SyncDeleteSink: Send + Sync {
    /// 版本感知删除（`version_id=None` + versioned = 预生成同 id delete marker）。
    async fn delete_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        versioned: bool,
        bypass_governance: bool,
    ) -> Result<DeleteOutcome>;

    /// 整键硬删（控制台对象删除；sync 桶远端先删全部版本）。
    /// 返回本地是否真的删掉了对象。
    async fn delete_key_all_versions(&self, bucket: &str, key: &str) -> Result<bool>;

    /// 删桶（sync 桶：双站 quiesce/查空/远端删除 → 本地删除；失败留 intent 重试）。
    async fn delete_bucket(&self, bucket: &str) -> Result<()>;

    /// DeleteObjects 因远端失败整体 503（部分键已提交）计数。
    fn record_partial(&self) {}
}
