//! M9 控制面：Raft 基座（S5）。
//!
//! S0 只提供能力开关；S5 接入 openraft 0.9.25 单节点运行时。命令应用到真实控制表由 S6
//! 负责，本模块的 `ControlCommand` 只覆盖基座自测用的最小 KV，集成方通过 [`spawn`] 接入。

// openraft 的 ConfigError/StorageError/RPCError 体积由第三方类型固定，调用方无法瘦身。
#![allow(clippy::result_large_err)]

pub mod network;
pub mod status;
pub mod store;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, Raft};
use serde::{Deserialize, Serialize};

use store::SqliteStateMachine;

/// 控制写（console/S3 配置写）是否已可走 Raft。S6 出口翻转。
pub const RAFT_READY: bool = false;
/// sync 桶创建/读写是否可用。S3 出口翻转。
pub const SYNC_READY: bool = false;
/// stretch 2×6 数据面（S4）已实现；控制面（Raft 桶创建/身份配对）就绪后随 S7/S9 翻转。
pub const STRETCH_READY: bool = false;

/// Raft RPC 集群令牌头（`/api/v1/site/raft/*`；常量时间校验见 [`network::token_matches`]）。
pub const RPC_TOKEN_HEADER: &str = "x-oss-raft-token";
/// append/vote/snapshot 路由体上限：4 MiB 原始分块的 serde JSON 约 16 MiB。
pub const RPC_BODY_LIMIT: usize = 32 * 1024 * 1024;
/// 单条控制命令上限（16 MiB 模块 base64 后约 21.4 MiB）。
pub const COMMAND_MAX_BYTES: usize = 24 * 1024 * 1024;
/// 快照原始分块上限（JSON 放大约 4 倍，故与 32 MiB 路由配套）。
pub const SNAPSHOT_CHUNK_MAX: u64 = 4 * 1024 * 1024;
/// 快照总上限硬顶。
pub const SNAPSHOT_MAX_BYTES_CAP: u64 = 1024 * 1024 * 1024;

openraft::declare_raft_types!(
    /// M9 控制面 Raft 类型：u64 节点 id、BasicNode 地址、`Cursor<Vec<u8>>` 快照。
    pub TypeConfig:
        D = ControlCommand,
        R = ControlResponse,
);

/// 最小控制命令。S6 用真实实体命令替换/扩展；基座只验证日志、apply 与快照路径。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCommand {
    Set { key: String, value: String },
    Delete { key: String },
}

/// 每条 entry 必须返回一个响应（成员/blank entry 也返回）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    Ok,
}

/// `[raft]` 运行时设置（与 `config::RaftToml` 解耦，测试可不读配置文件）。
#[derive(Debug, Clone)]
pub struct RaftSettings {
    pub node_id: u64,
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
}

impl RaftSettings {
    /// 从 `[raft]` 段构造。`db_path` 必须显式给出（`raft.db`），避免误写默认目录。
    pub fn from_toml(t: &crate::config::RaftToml) -> Result<Self, String> {
        let db_path = t
            .db_path
            .clone()
            .ok_or_else(|| "[raft].db_path 必填（raft.db 路径）".to_string())?;
        Ok(Self {
            node_id: t.node_id,
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
        })
    }

    fn openraft_config(&self) -> Result<Config, openraft::ConfigError> {
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

/// 基座错误：调用方（S6/S7）负责映射为 HTTP 状态；这里只保证底层错误可上报。
pub type RaftResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

/// 已启动的 Raft 节点。字段私有，集成方通过访问器取 `Raft`/状态机。
pub struct RaftHandle {
    raft: Raft<TypeConfig>,
    settings: RaftSettings,
    state_machine: SqliteStateMachine,
}

impl RaftHandle {
    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    pub fn node_id(&self) -> u64 {
        self.settings.node_id
    }

    pub fn settings(&self) -> &RaftSettings {
        &self.settings
    }

    pub fn state_machine(&self) -> &SqliteStateMachine {
        &self.state_machine
    }

    /// 应用层 deadline 的写入口：超时返回错误（写可能已提交，调用方按超时语义处理）。
    pub async fn write(&self, cmd: ControlCommand) -> Result<ControlResponse, String> {
        let timeout = Duration::from_millis(self.settings.write_timeout_ms);
        match tokio::time::timeout(timeout, self.raft.client_write(cmd)).await {
            Err(_) => Err(format!("raft client_write timed out after {timeout:?}")),
            Ok(Ok(resp)) => Ok(resp.data),
            Ok(Err(e)) => Err(e.to_string()),
        }
    }

    /// 等待本节点 apply 到 `index`（follower 转发后的有界读点，S6 使用）。
    pub async fn wait_applied(&self, index: u64, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let applied = {
                let m = self.raft.metrics();
                let m = m.borrow();
                m.last_applied.as_ref().map(|l| l.index).unwrap_or(0)
            };
            if applied >= index {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// S6 的 `GET /api/v1/site/raft/status` 数据源。
    pub fn status_json(&self) -> serde_json::Value {
        status::status_json(self)
    }

    pub async fn shutdown(&self) {
        if let Err(e) = self.raft.shutdown().await {
            tracing::warn!("raft shutdown: {e}");
        }
    }
}

/// S5 基座入口：打开（或恢复）`raft.db`，启动 Raft，bootstrap=true 且无成员时单节点 initialize。
///
/// `bootstrap=false` 且尚无成员时只启动运行时并等待 leader/join（S7 接线），不报错。
pub async fn spawn(settings: RaftSettings) -> RaftResult<RaftHandle> {
    let config = Arc::new(settings.openraft_config()?);
    let log_store = store::SqliteLogStore::open(&settings.db_path).await?;
    // 未 apply 的成员日志（崩溃窗口）也要阻止再次 initialize；openraft 启动时会重放。
    let has_logs = log_store.has_logs().await?;
    let state_machine =
        SqliteStateMachine::open(&settings.db_path, settings.snapshot_max_bytes).await?;

    let network = network::HttpNetworkFactory::new(settings.cluster_token.clone());
    let raft = Raft::new(
        settings.node_id,
        config,
        network,
        log_store,
        state_machine.clone(),
    )
    .await?;

    let handle = RaftHandle {
        raft,
        settings,
        state_machine,
    };
    handle.bootstrap_if_needed(has_logs).await?;
    Ok(handle)
}

/// [`spawn`] 的别名（计划中 `init_single` 名称；集成方二选一）。
pub async fn init_single(settings: RaftSettings) -> RaftResult<RaftHandle> {
    spawn(settings).await
}

impl RaftHandle {
    async fn bootstrap_if_needed(&self, has_logs: bool) -> RaftResult<()> {
        let (_, membership) = self.state_machine.applied().await?;
        if membership.voter_ids().next().is_some() || has_logs {
            return Ok(());
        }
        if self.settings.bootstrap {
            let node = BasicNode::new(self.settings.advertise_endpoint.clone());
            self.raft
                .initialize(BTreeMap::from([(self.settings.node_id, node)]))
                .await?;
        } else {
            tracing::warn!(
                node_id = self.settings.node_id,
                "Raft 尚无成员且 bootstrap=false：等待 leader/join（S7）"
            );
        }
        Ok(())
    }
}

/// `GET /api/v1/raft/status` 的 S0 骨架（S5 补 leader/term/applied）。
/// capabilities 供 UI 置灰未就绪模式；运行时状态见 [`status::status_json`]。
pub fn status_json() -> serde_json::Value {
    serde_json::json!({
        "enabled": false,
        "capabilities": {
            "raft": RAFT_READY,
            "sync": SYNC_READY,
            "stretch": STRETCH_READY,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use openraft::storage::{RaftLogStorage, RaftStateMachine};
    use openraft::{CommittedLeaderId, LogId, RaftLogReader};

    use super::store::{SqliteLogStore, SqliteStateMachine};
    use super::*;

    fn test_settings(dir: &Path) -> RaftSettings {
        RaftSettings {
            node_id: 1,
            bootstrap: true,
            advertise_endpoint: "http://127.0.0.1:1".to_string(),
            cluster_token: "test-cluster-token".to_string(),
            db_path: dir.join("raft.db"),
            // 共享构建机负载高：放宽选举/写超时，避免测试因 CPU 抢占抖动。
            election_timeout_ms: 2_000,
            heartbeat_interval_ms: 200,
            install_snapshot_timeout_ms: 5_000,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1024,
            write_timeout_ms: 20_000,
            snapshot_max_chunk_size: 4 * 1024 * 1024,
            snapshot_max_bytes: 512 * 1024 * 1024,
        }
    }

    async fn wait_leader(handle: &RaftHandle) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let leader = {
                let m = handle.raft().metrics();
                let m = m.borrow();
                m.current_leader
            };
            if let Some(l) = leader {
                return l;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "leader election timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_snapshot(handle: &RaftHandle, min_index: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let snap = {
                let m = handle.raft().metrics();
                let m = m.borrow();
                m.snapshot.as_ref().map(|l| l.index)
            };
            // 快照可能覆盖采样 last_applied 之后又提交的 entry（如选举后的 blank log）。
            if snap.is_some_and(|i| i >= min_index) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "snapshot build timed out (snapshot={snap:?}, want >= {min_index})"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_write_restart_and_membership() {
        let dir = tempfile::tempdir().unwrap();
        let settings = test_settings(dir.path());

        let handle = spawn(settings.clone()).await.unwrap();
        assert_eq!(wait_leader(&handle).await, 1);
        assert_eq!(
            handle
                .write(ControlCommand::Set {
                    key: "k".into(),
                    value: "v1".into(),
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );

        let applied = {
            let m = handle.raft().metrics();
            let m = m.borrow();
            m.last_applied.unwrap()
        };
        assert!(
            handle
                .wait_applied(applied.index, Duration::from_secs(15))
                .await
        );
        assert_eq!(
            handle.state_machine().get("k").await.unwrap(),
            Some("v1".to_string())
        );
        let (_, membership) = handle.state_machine().applied().await.unwrap();
        assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![1]);

        handle.shutdown().await;
        drop(handle);

        // 重启：不重新 initialize，完整 LogId + StoredMembership 与 SM 数据保留。
        let handle = spawn(settings).await.unwrap();
        assert_eq!(wait_leader(&handle).await, 1);
        let (reopened_applied, membership) = handle.state_machine().applied().await.unwrap();
        assert!(reopened_applied.unwrap().index >= applied.index);
        assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![1]);
        assert_eq!(
            handle.state_machine().get("k").await.unwrap(),
            Some("v1".to_string())
        );

        handle
            .write(ControlCommand::Delete { key: "k".into() })
            .await
            .unwrap();
        assert_eq!(handle.state_machine().get("k").await.unwrap(), None);
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_build_and_install_preserves_state() {
        let dir = tempfile::tempdir().unwrap();
        let handle = spawn(test_settings(dir.path())).await.unwrap();
        wait_leader(&handle).await;
        for i in 0..3 {
            handle
                .write(ControlCommand::Set {
                    key: format!("k{i}"),
                    value: format!("v{i}"),
                })
                .await
                .unwrap();
        }
        let applied = {
            let m = handle.raft().metrics();
            let m = m.borrow();
            m.last_applied.unwrap()
        };
        handle.raft().trigger().snapshot().await.unwrap();
        wait_snapshot(&handle, applied.index).await;

        let mut sm = handle.state_machine().clone();
        let snapshot = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
            .expect("snapshot must exist after trigger");
        assert!(snapshot.meta.last_log_id.unwrap().index >= applied.index);
        assert_eq!(
            snapshot
                .meta
                .last_membership
                .voter_ids()
                .collect::<Vec<_>>(),
            vec![1]
        );
        let bytes = snapshot.snapshot.get_ref().clone();
        assert!(!bytes.is_empty());

        // 安装到全新状态机：走 begin_receiving_snapshot 的 Cursor 接收路径。
        let dir2 = tempfile::tempdir().unwrap();
        let mut fresh = SqliteStateMachine::open(&dir2.path().join("raft.db"), 512 * 1024 * 1024)
            .await
            .unwrap();
        let mut cursor = fresh.begin_receiving_snapshot().await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut *cursor, &bytes)
            .await
            .unwrap();
        assert_eq!(fresh.applied().await.unwrap().0, None);
        RaftStateMachine::<TypeConfig>::install_snapshot(&mut fresh, &snapshot.meta, cursor)
            .await
            .unwrap();

        let (fresh_applied, fresh_membership) = fresh.applied().await.unwrap();
        assert_eq!(fresh_applied, snapshot.meta.last_log_id);
        assert_eq!(fresh_membership.voter_ids().collect::<Vec<_>>(), vec![1]);
        for i in 0..3 {
            assert_eq!(
                fresh.get(&format!("k{i}")).await.unwrap(),
                Some(format!("v{i}"))
            );
        }
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn log_store_append_read_truncate_purge_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let handle = spawn(test_settings(dir.path())).await.unwrap();
        wait_leader(&handle).await;
        for i in 0..3 {
            handle
                .write(ControlCommand::Set {
                    key: format!("k{i}"),
                    value: format!("v{i}"),
                })
                .await
                .unwrap();
        }
        handle.shutdown().await;
        drop(handle);

        let mut store = SqliteLogStore::open(&path).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        let last = state.last_log_id.expect("logs must exist");
        let entries = store.try_get_log_entries(..).await.unwrap();
        assert!(
            entries.len() >= 4,
            "membership + 3 writes, got {}",
            entries.len()
        );
        assert_eq!(entries.last().unwrap().log_id, last);

        // vote / committed 持久化（重开文件后仍在）。
        let vote = openraft::Vote::new_committed(9, 1);
        store.save_vote(&vote).await.unwrap();
        let committed = LogId::new(CommittedLeaderId::new(9, 1), last.index);
        store.save_committed(Some(committed)).await.unwrap();
        drop(store);

        let mut store = SqliteLogStore::open(&path).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        assert_eq!(store.read_committed().await.unwrap(), Some(committed));

        // truncate 从指定 LogId（含）起删除；purge 从起点删到指定 LogId（含）。
        let truncate_at = entries[1].log_id;
        store.truncate(truncate_at).await.unwrap();
        let after_truncate = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(after_truncate.last().unwrap().log_id, entries[0].log_id);

        store.purge(entries[0].log_id).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(entries[0].log_id));
        assert_eq!(state.last_log_id, Some(entries[0].log_id));
        assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
    }
}
