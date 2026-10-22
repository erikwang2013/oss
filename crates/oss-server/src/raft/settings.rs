//! `[raft]` 运行时设置（与 `config::RaftToml` 解耦，测试可不读配置文件）。

use std::path::PathBuf;

use openraft::Config;

/// `[raft]` 运行时设置（与 `config::RaftToml` 解耦，测试可不读配置文件）。
#[derive(Debug, Clone)]
pub struct RaftSettings {
    pub node_id: u64,
    pub role: crate::config::RaftRole,
    pub bootstrap: bool,
    pub advertise_endpoint: String,
    pub cluster_token: String,
    pub db_path: PathBuf,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub install_snapshot_timeout_ms: u64,
    pub max_in_snapshot_log_to_keep: u64,
    pub purge_batch_size: u64,
    pub write_timeout_ms: u64,
    pub snapshot_max_chunk_size: u64,
    pub snapshot_max_bytes: u64,
    /// 配置愿意激活的最高 feature 级别（R11 在 `RaftToml`/`from_toml` 都校验）。
    pub feature_level: u8,
}

impl RaftSettings {
    /// 从 `[raft]` 段构造。`db_path` 必须显式给出（`raft.db`），避免误写默认目录。
    pub fn from_toml(t: &crate::config::RaftToml) -> Result<Self, String> {
        let db_path = t
            .db_path
            .clone()
            .ok_or_else(|| "[raft].db_path 必填（raft.db 路径）".to_string())?;
        if !(super::compat::MIN_FEATURE_LEVEL..=super::compat::MAX_FEATURE_LEVEL)
            .contains(&t.feature_level)
        {
            return Err(format!(
                "[raft].feature_level 必须在 {}..={}，实际 {}",
                super::compat::MIN_FEATURE_LEVEL,
                super::compat::MAX_FEATURE_LEVEL,
                t.feature_level
            ));
        }
        Ok(Self {
            node_id: t.node_id,
            role: t.role,
            bootstrap: t.bootstrap,
            advertise_endpoint: t.advertise_endpoint.clone(),
            cluster_token: t.cluster_token.clone(),
            db_path,
            election_timeout_ms: t.election_timeout_ms,
            heartbeat_interval_ms: t.heartbeat_interval_ms,
            install_snapshot_timeout_ms: t.install_snapshot_timeout_ms,
            max_in_snapshot_log_to_keep: t.max_in_snapshot_log_to_keep,
            purge_batch_size: t.purge_batch_size,
            write_timeout_ms: t.write_timeout_ms,
            snapshot_max_chunk_size: t.snapshot_max_chunk_size,
            snapshot_max_bytes: t.snapshot_max_bytes,
            feature_level: t.feature_level,
        })
    }

    pub(super) fn openraft_config(&self) -> Result<Config, openraft::ConfigError> {
        Config {
            cluster_name: "oss-control".to_string(),
            heartbeat_interval: self.heartbeat_interval_ms,
            election_timeout_min: self.election_timeout_ms,
            election_timeout_max: self.election_timeout_ms.saturating_mul(2),
            install_snapshot_timeout: self.install_snapshot_timeout_ms,
            max_in_snapshot_log_to_keep: self.max_in_snapshot_log_to_keep,
            purge_batch_size: self.purge_batch_size,
            snapshot_max_chunk_size: self.snapshot_max_chunk_size,
            ..Default::default()
        }
        .validate()
    }
}
