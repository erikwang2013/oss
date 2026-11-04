//! uploads 域的行类型与后端中立 trait（控制台上传记录）。
//!
//! 归属清理（删除用户/站点 apply 的 `owner_id = NULL`）仍由 IAM/site 路径持有，
//! 不在本 trait 内——避免与另一属主的迁移面交叉。

use super::ControlError;

/// 上传记录行（字段集与原 `UploadRecord` 一致）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRow {
    pub saved_path: String,
    pub bucket: String,
    pub key: String,
    pub group_name: String,
    pub size: i64,
    pub etag: String,
    pub original_name: Option<String>,
    pub created_at: i64,
}

/// 记录上传的入参（`saved_path` 为自然键，冲突时保留旧行）。
#[derive(Debug, Clone)]
pub struct NewUpload {
    pub saved_path: String,
    pub bucket: String,
    pub key: String,
    pub group_name: String,
    pub size: i64,
    pub etag: String,
    pub created_at: i64,
}

/// 只读 uploads 访问。
#[async_trait::async_trait]
pub trait UploadStore: Send + Sync {
    async fn upload(&self, saved_path: &str) -> Result<Option<UploadRow>, ControlError>;
    /// `created_at` 降序、`saved_path` 升序。
    async fn uploads(&self, limit: i64) -> Result<Vec<UploadRow>, ControlError>;
    /// 同上，仅 `owner_id` 命中的行。
    async fn uploads_for(&self, owner_id: i64, limit: i64) -> Result<Vec<UploadRow>, ControlError>;
}

/// 调用方事务内的 uploads 写访问；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait UploadTxn: Send {
    /// `ON CONFLICT(saved_path) DO NOTHING`。
    async fn upload_record(&mut self, new: &NewUpload) -> Result<(), ControlError>;
    /// 登记 `original_name`/归属；管理员无条件，其他用户仅无归属或自己的记录。
    async fn upload_register(
        &mut self,
        saved_path: &str,
        original_name: &str,
        owner_id: i64,
        is_admin: bool,
    ) -> Result<bool, ControlError>;
    /// 仅当尚无归属时写 owner。
    async fn upload_claim(&mut self, saved_path: &str, owner_id: i64)
    -> Result<bool, ControlError>;
}
