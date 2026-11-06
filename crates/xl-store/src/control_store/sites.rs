//! M7 `sites` / `site_cursors` / `site_config` / changelog / clock 的行类型与
//! 后端中立 trait。

use serde::{Deserialize, Serialize};

use super::ControlError;

/// 对端站点行（`sites` 全列；含出站凭据——接口层负责脱敏）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SitePeerRow {
    pub peer_id: String,
    pub remote_site_id: String,
    pub name: String,
    pub endpoint: String,
    pub region: String,
    pub priority: i64,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub peer_site_token: String,
    pub created_at: i64,
    pub last_seen: Option<i64>,
    pub last_error: Option<String>,
    /// M9：对端 Raft u64 节点 id（M7 行 NULL）。
    pub raft_node_id: Option<u64>,
    /// M9 S7.4：sync 修复完成前拒绝向该对端复制/服务。
    pub reconcile_required: bool,
}

/// 本站身份（`site_config` 单行）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteIdentityRow {
    pub id: String,
    pub name: String,
    pub region: String,
    pub priority: i64,
    pub token: String,
}

/// 站点出站凭据（worker 每行读取的列子集）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteCredentialsRow {
    pub endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub region: String,
    pub peer_site_token: String,
}

/// 出站 changelog 行（`payload` 为 JSON 文本，出货前重新解析）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SiteChangelogRow {
    pub seq: i64,
    pub origin_site_id: String,
    pub entity: String,
    pub entity_id: String,
    pub op: String,
    pub payload: String,
    pub priority: i64,
    pub ts: i64,
    pub created_at: i64,
}

/// 实体当前胜出的 LWW 元组。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SiteEntityClockRow {
    pub entity: String,
    pub entity_id: String,
    pub ts: i64,
    pub priority: i64,
    pub origin_site_id: String,
    pub seq: i64,
}

/// 只读站点表访问。
#[async_trait::async_trait]
pub trait SiteStore: Send + Sync {
    async fn site_identity(&self) -> Result<Option<SiteIdentityRow>, ControlError>;
    async fn site(&self, peer_id: &str) -> Result<Option<SitePeerRow>, ControlError>;
    /// `created_at` 升序、`peer_id` 升序。
    async fn sites(&self) -> Result<Vec<SitePeerRow>, ControlError>;
    async fn site_count(&self) -> Result<i64, ControlError>;
    async fn site_remote_id(&self, peer_id: &str) -> Result<Option<String>, ControlError>;
    /// 任意一行的 `remote_site_id`（console 配对展示。
    async fn first_site_remote_id(&self) -> Result<Option<String>, ControlError>;
    /// `peer_id` 升序。
    async fn site_peer_ids(&self) -> Result<Vec<String>, ControlError>;
    async fn site_credentials(
        &self,
        peer_id: &str,
    ) -> Result<Option<SiteCredentialsRow>, ControlError>;
    async fn site_cursor(&self, peer_id: &str) -> Result<Option<i64>, ControlError>;
    /// cutover 前置：出站 changelog 未追平的行数。
    async fn site_lag_rows(&self) -> Result<i64, ControlError>;
    /// `seq` 升序；出货 worker 自行按批大小截断。
    async fn site_changelog_after(&self, seq: i64) -> Result<Vec<SiteChangelogRow>, ControlError>;
    async fn site_changelog_max_seq(&self) -> Result<i64, ControlError>;
    async fn site_clock(
        &self,
        entity: &str,
        entity_id: &str,
    ) -> Result<Option<SiteEntityClockRow>, ControlError>;
}

/// 调用方事务内的站点表写访问；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait SiteTxn: Send {
    /// 收敛 `site_config` 到最早一行（旧版本多行缺陷的启动修复）。
    async fn site_config_trim(&mut self) -> Result<u64, ControlError>;
    async fn site_config_id(&mut self) -> Result<Option<String>, ControlError>;
    /// 首启种子（`WHERE NOT EXISTS` 幂等）。
    async fn site_config_seed(
        &mut self,
        id: &str,
        token: &str,
        created_at: i64,
    ) -> Result<(), ControlError>;
    /// `[site]` 覆盖；None 保持原值。
    async fn site_config_apply(
        &mut self,
        name: Option<&str>,
        region: Option<&str>,
        priority: Option<i64>,
        token: Option<&str>,
    ) -> Result<(), ControlError>;
    /// 按 `peer_id` 或 `remote_site_id` 更新，未命中再插入（UPDATE-then-INSERT）。
    async fn site_pair_upsert(&mut self, peer: &SitePeerRow) -> Result<(), ControlError>;
    /// 插入配对行 + `site_cursors` 起始游标（0；调用方事务内）。
    async fn site_insert(&mut self, peer: &SitePeerRow, now: i64) -> Result<(), ControlError>;
    async fn site_remote_id(&mut self, peer_id: &str) -> Result<Option<String>, ControlError>;
    async fn site_set_reconcile_required(
        &mut self,
        peer_id: &str,
        required: bool,
    ) -> Result<(), ControlError>;
    async fn site_update_ping(
        &mut self,
        peer_id: &str,
        ts: i64,
        error: Option<&str>,
    ) -> Result<(), ControlError>;
    async fn site_cursor_delete(&mut self, peer_id: &str) -> Result<u64, ControlError>;
    /// 只改 `last_error`（出货成功清空 / 失败记录）。
    async fn site_set_last_error(
        &mut self,
        peer_id: &str,
        error: Option<&str>,
    ) -> Result<(), ControlError>;
    /// apply 死信按对端 `site_config.id` 归属（`origin_site_id`）。
    async fn site_set_last_error_for_remote(
        &mut self,
        remote_site_id: &str,
        error: Option<&str>,
    ) -> Result<(), ControlError>;
    async fn site_delete(&mut self, peer_id: &str) -> Result<u64, ControlError>;
}
