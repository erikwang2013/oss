//! M8 S0：桶删除的跨 crate 清理契约。
//!
//! `xl-s3` 不能依赖 `oss-server` 的 `Db`；删除物理桶后由注入的实现清理 M8 控制面状态
//! （access_points/inventory/tier_restores/lifecycle 等）并失效 AP 缓存。

use async_trait::async_trait;

#[async_trait]
pub trait BucketCleanupSink: Send + Sync {
    /// 清理该桶的 M8 表（jobs 按审计保留）。失败只应导致调用方 warn，不回滚物理删除。
    async fn clear_bucket_m8(&self, bucket: &str) -> Result<(), String>;
    /// 失效 AP 缓存（同名桶重建后不得继承旧策略/endpoint）。
    fn invalidate_ap_cache(&self, bucket: &str);
}
