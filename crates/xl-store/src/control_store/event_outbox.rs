//! event_outbox 域的行类型与后端中立 trait（通知事件的两态租约队列）。
//!
//! 行为口径与 `events.rs` 迁移前逐字一致：`claimed_at` 即 claim token，
//! 续租/完成/回写都以 `claimed_at IS ? AND state='sending'` 守卫；claim 用
//! 单条 `UPDATE ... RETURNING`，按 `id` 升序逐条投递。

use super::ControlError;

/// 死信列表投影（console `/events/dead`；只读）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadEventRow {
    pub id: i64,
    pub bucket: String,
    pub event_json: String,
    pub attempts: i64,
    pub last_error: Option<String>,
    pub created_at: i64,
}

/// claim 结果（`claimed_at` 是后续续租/完成守卫的 token）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOutboxClaim {
    pub id: i64,
    pub target_id: String,
    pub event_json: String,
    pub attempts: i64,
    pub claimed_at: Option<i64>,
}

/// 只读 event_outbox 访问。
#[async_trait::async_trait]
pub trait EventOutboxStore: Send + Sync {
    /// `state='dead'`，`id` 降序。
    async fn dead_events(&self, limit: i64) -> Result<Vec<DeadEventRow>, ControlError>;
}

/// 调用方事务内的 event_outbox 写访问；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait EventOutboxTxn: Send {
    /// 入队（`state='pending'`、`attempts=0`、`next_retry_at=0`）。
    async fn event_enqueue(
        &mut self,
        bucket: &str,
        target_id: &str,
        event_json: &str,
        now: i64,
    ) -> Result<(), ControlError>;
    /// 回收过期租约（`claimed_at < now - lease_secs`）→ pending；返回行数。
    async fn event_reclaim_expired(
        &mut self,
        now: i64,
        lease_secs: i64,
    ) -> Result<u64, ControlError>;
    /// 原子 claim 一批（`id` 升序）；返回 claim 行。
    async fn event_claim(
        &mut self,
        now: i64,
        limit: i64,
    ) -> Result<Vec<EventOutboxClaim>, ControlError>;
    /// 续租并轮换 token；token 不符/非 sending → `false`。
    async fn event_renew(
        &mut self,
        id: i64,
        old: Option<i64>,
        now: i64,
    ) -> Result<bool, ControlError>;
    /// 成功删行（token 守卫）。
    async fn event_finish_ok(&mut self, id: i64, token: Option<i64>) -> Result<bool, ControlError>;
    /// 失败回 pending（token 守卫；`retry_at` 由调用方按退避算好）。
    async fn event_retry(
        &mut self,
        id: i64,
        token: Option<i64>,
        attempts: i64,
        retry_at: i64,
        last_error: &str,
    ) -> Result<bool, ControlError>;
    /// 永久错超限 → dead（token 守卫）。
    async fn event_bury(
        &mut self,
        id: i64,
        token: Option<i64>,
        attempts: i64,
        last_error: &str,
    ) -> Result<bool, ControlError>;
    /// 死信重投：dead → pending 并复位 attempts/claimed_at/next_retry_at/last_error。
    async fn event_redeliver(&mut self, id: i64, now: i64) -> Result<bool, ControlError>;
}
