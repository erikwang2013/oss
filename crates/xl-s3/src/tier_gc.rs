//! M8 S4a：删除归档版本后的远端 tier 清理契约（真实实现在 S4b 注入）。

use async_trait::async_trait;
use xl_store::store::TierRef;

#[async_trait]
pub trait TierGcSink: Send + Sync {
    /// 删除远端 tier 对象；失败只 warn（不改变已完成的本地删除）。
    async fn delete(&self, refs: &[TierRef]) -> Result<(), String>;
}
