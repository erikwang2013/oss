//! audit_log 域的行类型与后端中立 trait。
//!
//! 审计是 best-effort 批量写：`AuditTxn` 只借用调用方事务；查询过滤片段由适配器
//! 按固定顺序拼接、值一律绑定（与迁移前 `db.rs::list_audit` 逐字一致）。

use super::ControlError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub id: i64,
    pub ts: i64,
    pub uid: i64,
    pub op: String,
    pub bucket: Option<String>,
    pub object_key: Option<String>,
    pub decision: String,
    pub detail: Option<String>,
}

/// 批量写入的整行输入（id 由 AUTOINCREMENT 分配）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAuditRecord {
    pub ts: i64,
    pub uid: i64,
    pub op: String,
    pub bucket: Option<String>,
    pub object_key: Option<String>,
    pub decision: String,
    pub detail: Option<String>,
}

/// 控制台审计查询（`limit` 必填；其余为可选等值/时间窗/前缀过滤）。
#[derive(Debug, Clone, Default)]
pub struct AuditQuery {
    pub limit: i64,
    pub decision: Option<String>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub uid: Option<i64>,
    pub op: Option<String>,
    pub bucket: Option<String>,
    pub key_prefix: Option<String>,
}

#[async_trait::async_trait]
pub trait AuditStore: Send + Sync {
    /// `id` 降序（最新在前）。
    async fn audit_page(&self, query: &AuditQuery) -> Result<Vec<AuditRecord>, ControlError>;
}

/// 调用方事务内的 audit_log 写访问；不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait AuditTxn: Send {
    /// 按入参顺序逐行插入（单事务内）。
    async fn audit_insert_batch(&mut self, rows: &[NewAuditRecord]) -> Result<(), ControlError>;
    /// 每批最多 `limit` 行（id 子查询圈定），返回删除数。
    async fn audit_delete_older_than(
        &mut self,
        cutoff: i64,
        limit: i64,
    ) -> Result<u64, ControlError>;
}
