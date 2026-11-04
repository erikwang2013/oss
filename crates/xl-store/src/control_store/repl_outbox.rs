//! repl_outbox 域的行类型与后端中立 trait（桶/站点复制出站队列）。
//!
//! 语义与迁移前 `replication.rs` / `db.rs` 一致：`seq` 升序 claim、`claimed_at`
//! token 守卫、永久错超限进 dead；站点解配按 `target_kind='site'` 清行。

use super::ControlError;

/// 出站行（claim 结果与 worker 所需的全部列）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplOutboxRow {
    pub seq: i64,
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub etag: String,
    pub remote_id: String,
    pub dest_bucket: String,
    pub is_delete_marker: bool,
    /// 4.16：目标按同源 version_id 定点删除（不造 marker）。
    pub is_version_delete: bool,
    /// `"remote"` / `"site"`。
    pub target_kind: String,
    pub attempts: i64,
    pub claimed_at: Option<i64>,
}

/// 入队整行输入（两个入队点只有 target_kind/dest_bucket 不同）。
#[derive(Debug, Clone)]
pub struct NewReplOutbox {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub etag: String,
    pub size: i64,
    pub remote_id: String,
    pub dest_bucket: String,
    pub is_delete_marker: bool,
    pub is_version_delete: bool,
    pub target_kind: String,
    pub created_at: i64,
}

/// 同版本终态聚合（`settle_version_status`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplOutboxCounts {
    pub pending: i64,
    pub sending: i64,
    pub dead: i64,
}

/// 只读 repl_outbox 访问。
#[async_trait::async_trait]
pub trait ReplOutboxStore: Send + Sync {
    /// 同 `(bucket,key,version_id)` 的 pending/sending/dead 计数（单条聚合）。
    async fn repl_version_counts(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ReplOutboxCounts, ControlError>;
    /// 同版本更早的在途行数（版本删除保序等待）。
    async fn repl_earlier_inflight(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        seq: i64,
    ) -> Result<i64, ControlError>;
    /// 某站点出站行按 state 计数（console status）。
    async fn repl_site_state_count(&self, peer_id: &str, state: &str) -> Result<i64, ControlError>;
    /// 某站点全部出站行数（repair 清理报告）。
    async fn repl_site_total(&self, peer_id: &str) -> Result<i64, ControlError>;
}

/// 调用方事务内的 repl_outbox 写访问；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait ReplOutboxTxn: Send {
    async fn repl_enqueue(&mut self, new: &NewReplOutbox) -> Result<(), ControlError>;
    /// 过期租约回收；返回行数。
    async fn repl_reclaim_expired(
        &mut self,
        now: i64,
        lease_secs: i64,
    ) -> Result<u64, ControlError>;
    /// 原子 claim 一批（`seq` 升序）。
    async fn repl_claim(
        &mut self,
        now: i64,
        limit: i64,
    ) -> Result<Vec<ReplOutboxRow>, ControlError>;
    /// 续租并轮换 token；token 不符/非 sending → `false`。
    async fn repl_renew(
        &mut self,
        seq: i64,
        old: Option<i64>,
        now: i64,
    ) -> Result<bool, ControlError>;
    /// 成功/站点已解配时删行（token 守卫）。
    async fn repl_finish_ok(&mut self, seq: i64, token: Option<i64>) -> Result<bool, ControlError>;
    /// 失败回 pending（token 守卫；`retry_at` 由调用方按退避算好）。
    async fn repl_retry(
        &mut self,
        seq: i64,
        token: Option<i64>,
        attempts: i64,
        retry_at: i64,
        last_error: &str,
    ) -> Result<bool, ControlError>;
    /// 永久错超限 → dead（token 守卫）。
    async fn repl_bury(
        &mut self,
        seq: i64,
        token: Option<i64>,
        attempts: i64,
        last_error: &str,
    ) -> Result<bool, ControlError>;
    /// 站点解配：删该 peer 的出站行；返回被删掉的非空版本 `(bucket,key,version)`。
    async fn repl_drop_site(
        &mut self,
        peer_id: &str,
    ) -> Result<Vec<(String, String, String)>, ControlError>;
    /// repair：删除该 peer 的全部站点出站行；返回行数。
    async fn repl_delete_site_all(&mut self, peer_id: &str) -> Result<u64, ControlError>;
    /// repair：清理前在同一事务内计数该 peer 的站点出站行。
    async fn repl_site_total(&mut self, peer_id: &str) -> Result<i64, ControlError>;
}
