//! S7.x in-process Raft 节点骨架：独立 raft.db + 真实 HTTP RPC 监听（随机端口）。
//!
//! S7.3（成员变更）与 S7.6（端到端）可直接用它组 3-voter 集群：`spawn_node` 返回
//! 真实 advertise 地址，成员表里的节点地址即可达。

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use oss_server::config::RaftRole;
use oss_server::raft::{RaftHandle, RaftSettings, routes};

pub struct TestNode {
    pub handle: Arc<RaftHandle>,
    pub base: String,
    pub token: String,
    /// 传入控制库时保留一份句柄，测试可直接断言行。
    pub pool: Option<sqlx::SqlitePool>,
}

impl TestNode {
    pub async fn shutdown(self) {
        self.handle.shutdown().await;
    }
}

/// 单节点测试参数（共享构建机负载高：选举/写超时放宽）。
pub fn settings(dir: &Path, node_id: u64, bootstrap: bool) -> RaftSettings {
    RaftSettings {
        node_id,
        role: RaftRole::Voter,
        bootstrap,
        advertise_endpoint: "http://127.0.0.1:1".into(),
        cluster_token: "s71-harness-token".into(),
        db_path: dir.join(format!("raft-{node_id}.db")),
        election_timeout_ms: 2_000,
        heartbeat_interval_ms: 200,
        install_snapshot_timeout_ms: 10_000,
        max_in_snapshot_log_to_keep: 0,
        purge_batch_size: 1024,
        write_timeout_ms: 20_000,
        snapshot_max_chunk_size: oss_server::raft::SNAPSHOT_CHUNK_MAX,
        snapshot_max_bytes: 512 * 1024 * 1024,
    }
}

/// 启动节点并在随机端口暴露 `rpc_router`；`advertise_endpoint` 改写为该地址。
pub async fn spawn_node(settings: RaftSettings) -> TestNode {
    spawn_node_with_pool(settings, None).await
}

/// 与 [`spawn_node`] 相同，但把控制库连接池交给状态机（成员/转发测试需要真实控制表）。
pub async fn spawn_node_with_pool(
    mut settings: RaftSettings,
    control_pool: Option<sqlx::SqlitePool>,
) -> TestNode {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    settings.advertise_endpoint = format!("http://{addr}");
    let token = settings.cluster_token.clone();
    let pool = control_pool.clone();
    let handle = Arc::new(
        oss_server::raft::spawn_with_control(settings, control_pool)
            .await
            .unwrap(),
    );
    let app = routes::rpc_router(Arc::clone(&handle), token.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    TestNode {
        handle,
        base: format!("http://{addr}"),
        token,
        pool,
    }
}

pub async fn wait_leader(handle: &RaftHandle) -> u64 {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
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
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
