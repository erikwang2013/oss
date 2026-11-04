//! sync_write_intents / sync_delete_intents 域的行类型与后端中立 trait。
//!
//! 写路径的 `PRAGMA synchronous = FULL` 仍由调用方在同一连接上执行（落盘保证），
//! 适配器只执行 SQL，不 begin/commit/rollback。

use super::ControlError;

/// 写 intent 恢复扫描行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncWriteIntentRow {
    pub id: String,
    pub bucket: String,
    pub key: String,
}

/// 写 intent 整行输入（`intent_begin`）。
#[derive(Debug, Clone)]
pub struct NewSyncWriteIntent {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub local_version_id: String,
    pub remote_version_id: String,
    pub tee_path: String,
    pub commit_json: String,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 删 intent 恢复扫描行（版本列可为 NULL）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncDeleteIntentRow {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub op: String,
    pub local_version_id: Option<String>,
    pub remote_version_id: Option<String>,
    pub marker_version_id: Option<String>,
    pub state: String,
}

/// 删 intent 整行输入（版本列为空串时按空串存，与迁移前绑定一致）。
#[derive(Debug, Clone)]
pub struct NewSyncDeleteIntent {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub op: String,
    pub local_version_id: String,
    pub remote_version_id: String,
    pub marker_version_id: String,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 只读 sync intent 访问。
#[async_trait::async_trait]
pub trait SyncIntentStore: Send + Sync {
    /// `updated_at < cutoff`（写 intent 恢复；不排序，与迁移前一致）。
    async fn sync_write_intents_stale(
        &self,
        cutoff: i64,
    ) -> Result<Vec<SyncWriteIntentRow>, ControlError>;
    async fn sync_write_intent_tee(&self, id: &str) -> Result<Option<String>, ControlError>;
    /// 全部 tee 路径（孤儿 tee 清理的引用集）。
    async fn sync_write_intent_tees(&self) -> Result<Vec<String>, ControlError>;
    /// `updated_at < cutoff`，`updated_at` 升序限 `limit`（删 intent 恢复）。
    async fn sync_delete_intents_stale(
        &self,
        cutoff: i64,
        limit: i64,
    ) -> Result<Vec<SyncDeleteIntentRow>, ControlError>;
}

/// 调用方事务内的 sync intent 写访问；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait SyncIntentTxn: Send {
    async fn sync_write_intent_insert(
        &mut self,
        new: &NewSyncWriteIntent,
    ) -> Result<(), ControlError>;
    async fn sync_write_intent_mark_done(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<u64, ControlError>;
    async fn sync_write_intent_delete(&mut self, id: &str) -> Result<u64, ControlError>;
    async fn sync_write_intent_count(&mut self) -> Result<i64, ControlError>;
    async fn sync_write_intent_delete_all(&mut self) -> Result<u64, ControlError>;

    async fn sync_delete_intent_insert(
        &mut self,
        new: &NewSyncDeleteIntent,
    ) -> Result<(), ControlError>;
    async fn sync_delete_intent_mark_done(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<u64, ControlError>;
    async fn sync_delete_intent_delete(&mut self, id: &str) -> Result<u64, ControlError>;
    async fn sync_delete_intent_count(&mut self) -> Result<i64, ControlError>;
    async fn sync_delete_intent_delete_all(&mut self) -> Result<u64, ControlError>;
}
