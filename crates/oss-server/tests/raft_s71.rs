//! M9 S7.1：身份持久化、Raft HTTP 路由（鉴权/体上限/转发）、分块快照安装、3 节点骨架。

mod common;

use std::path::Path;

use common::raft_harness::{self, settings};
use openraft::Vote;
use openraft::raft::InstallSnapshotRequest;
use openraft::storage::RaftStateMachine;
use oss_server::db::Db;
use oss_server::raft::membership;
use oss_server::raft::{ControlCommand, ControlResponse, RPC_TOKEN_HEADER, TypeConfig};
use oss_server::state::AppState;

fn raft_db(dir: &Path) -> String {
    format!("sqlite://{}?mode=rwc", dir.join("control.db").display())
}

async fn control_db(dir: &Path, name: &str) -> Db {
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.join(format!("{name}.db")).display()
    );
    Db::connect_raft(&url).await.unwrap()
}

fn write_cfg(
    dir: &Path,
    node_id: u64,
    bootstrap: bool,
    site_id: &str,
    advertise: &str,
) -> std::path::PathBuf {
    let path = dir.join("oss.toml");
    std::fs::write(
        &path,
        format!(
            r#"
listen = "127.0.0.1:0"
data_dir = "{data}"
staging_dir = "{staging}"
database_url = "sqlite://{db}?mode=rwc"
jwt_secret = "0123456789abcdef0123456789abcdef"

[site]
id = "{site_id}"
name = "s71"

[raft]
node_id = {node_id}
bootstrap = {bootstrap}
advertise_endpoint = "{advertise}"
cluster_token = "s71-token"
db_path = "{raft_db}"
election_timeout_ms = 500
heartbeat_interval_ms = 100
write_timeout_ms = 5000
"#,
            data = dir.join("data").display(),
            staging = dir.join("staging").display(),
            db = dir.join("control.db").display(),
            raft_db = dir.join("raft.db").display(),
        ),
    )
    .unwrap();
    path
}

/// 重启身份一致；node_id/bootstrap/advertise/[site].id 任一变更都拒绝启动；
/// `raft_initialized=1` + 空日志 + 空成员 → force-recovery 提示。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_restart_mismatch_and_lost_state() {
    let dir = tempfile::tempdir().unwrap();
    let advertise = "http://127.0.0.1:19002";
    let cfg_path = write_cfg(dir.path(), 1, true, "site-a", advertise);
    let state = AppState::load(&cfg_path).await.unwrap();
    assert!(state.raft.is_some(), "load 后必须保留 Raft 句柄");
    let db = state.db.clone();
    assert_eq!(
        db.schema_meta("raft_node_id").await.unwrap().as_deref(),
        Some("1")
    );
    assert_eq!(
        db.schema_meta("raft_role").await.unwrap().as_deref(),
        Some("voter")
    );
    assert_eq!(
        db.schema_meta("raft_bootstrap").await.unwrap().as_deref(),
        Some("1")
    );
    assert_eq!(
        db.schema_meta("raft_advertise_endpoint")
            .await
            .unwrap()
            .as_deref(),
        Some(advertise)
    );
    assert_eq!(
        db.schema_meta("raft_site_id").await.unwrap().as_deref(),
        Some("site-a")
    );
    assert_eq!(
        db.schema_meta("raft_initialized").await.unwrap().as_deref(),
        Some("1")
    );
    assert_eq!(state.site.id, "site-a");
    state.raft.as_ref().unwrap().shutdown().await;
    drop(state);

    // node_id 变更
    write_cfg(dir.path(), 2, true, "site-a", advertise);
    let err = AppState::load(&cfg_path)
        .await
        .err()
        .expect("node_id 变更必须拒绝");
    assert!(err.to_string().contains("raft_node_id"), "{err}");

    // [site].id 变更
    write_cfg(dir.path(), 1, true, "site-b", advertise);
    let err = AppState::load(&cfg_path)
        .await
        .err()
        .expect("[site].id 变更必须拒绝");
    assert!(err.to_string().contains("[site].id"), "{err}");

    // bootstrap 变更
    write_cfg(dir.path(), 1, false, "site-a", advertise);
    let err = AppState::load(&cfg_path)
        .await
        .err()
        .expect("bootstrap 变更必须拒绝");
    assert!(err.to_string().contains("raft_bootstrap"), "{err}");

    // advertise 变更
    write_cfg(dir.path(), 1, true, "site-a", "http://127.0.0.1:19003");
    let err = AppState::load(&cfg_path)
        .await
        .err()
        .expect("advertise 变更必须拒绝");
    assert!(err.to_string().contains("raft_advertise_endpoint"), "{err}");

    // 状态丢失：initialized=1 但 raft.db 被清空且成员表为空。
    let db = Db::connect_offline(&raft_db(dir.path())).await.unwrap();
    sqlx::query("DELETE FROM raft_applied")
        .execute(db.pool())
        .await
        .unwrap();
    drop(db);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    std::fs::remove_file(dir.path().join("raft.db")).expect("删除 raft.db");
    for f in ["raft.db-wal", "raft.db-shm"] {
        let _ = std::fs::remove_file(dir.path().join(f));
    }
    write_cfg(dir.path(), 1, true, "site-a", advertise);
    let err = AppState::load(&cfg_path)
        .await
        .err()
        .expect("空 raft.db + 空成员必须拒绝启动");
    assert!(err.to_string().contains("raft-force-recover"), "{err}");
}

/// 路由令牌鉴权 + 逐路由体上限（membership 64 KiB / append 32 MiB）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_routes_auth_and_body_limits() {
    let dir = tempfile::tempdir().unwrap();
    let node = raft_harness::spawn_node(settings(dir.path(), 1, true)).await;
    raft_harness::wait_leader(&node.handle).await;
    let http = reqwest::Client::new();
    let status = format!("{}/api/v1/site/raft/status", node.base);
    assert_eq!(http.get(&status).send().await.unwrap().status(), 401);
    assert_eq!(
        http.get(&status)
            .header(RPC_TOKEN_HEADER, "wrong")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let v: serde_json::Value = http
        .get(&status)
        .header(RPC_TOKEN_HEADER, &node.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["node_id"], 1);
    assert_eq!(v["role"], "voter");
    assert_eq!(v["members"], serde_json::json!([1]));

    let membership = format!("{}/api/v1/site/raft/membership", node.base);
    let resp = http
        .post(&membership)
        .header(RPC_TOKEN_HEADER, &node.token)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(vec![b'x'; 70 * 1024])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413, "membership 上限 64 KiB");
    let resp = http
        .post(&membership)
        .header(RPC_TOKEN_HEADER, &node.token)
        .json(&serde_json::json!({"op": "not_a_membership_op"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "未知成员操作必须是 400（S7.3 已接线）");

    let append = format!("{}/api/v1/site/raft/append", node.base);
    let resp = http
        .post(&append)
        .header(RPC_TOKEN_HEADER, &node.token)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(vec![b'x'; 33 * 1024 * 1024])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413, "append 上限 32 MiB");

    node.shutdown().await;
}

/// follower/无 leader 写转发：无 leader → 503 + hint；带转发标记不再二次转发。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_write_without_leader_is_503_with_hint() {
    let dir = tempfile::tempdir().unwrap();
    let node = raft_harness::spawn_node(settings(dir.path(), 9, false)).await;
    let write = format!("{}/api/v1/site/raft/write", node.base);
    let cmd = serde_json::json!({"UserDelete": {"username": "nobody"}});
    let resp = reqwest::Client::new()
        .post(&write)
        .header(RPC_TOKEN_HEADER, &node.token)
        .json(&cmd)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "no leader");
    assert!(body["hint"].as_str().unwrap().contains("retry"));
    node.shutdown().await;
}

/// post-release 观测：follower 的 status 带 lag/stale（追平后 stale=false）；
/// follower 控制写单跳转发，`forward_total` 计数前进。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_status_stale_and_forward_counter() {
    let dir = tempfile::tempdir().unwrap();
    let a_db = control_db(dir.path(), "obs-a").await;
    let b_db = control_db(dir.path(), "obs-b").await;
    let mut sa = settings(dir.path(), 1, true);
    sa.cluster_token = "s71-obs-token".into();
    let a = raft_harness::spawn_node_with_pool(sa, Some(a_db.pool().clone())).await;
    let mut sb = settings(dir.path(), 2, false);
    sb.cluster_token = "s71-obs-token".into();
    let b = raft_harness::spawn_node_with_pool(sb, Some(b_db.pool().clone())).await;
    assert_eq!(raft_harness::wait_leader(&a.handle).await, 1);
    membership::promote_voter(&a.handle, 2, b.base.clone())
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let status = format!("{}/api/v1/site/raft/status", b.base);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let v: serde_json::Value = loop {
        let v: serde_json::Value = http
            .get(&status)
            .header(RPC_TOKEN_HEADER, &b.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if v["leader"] == 1 && v["state"] == "follower" && v["lag"] == 0 {
            break v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "follower 未追平 leader: {v}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert_eq!(v["node_id"], 2);
    assert_eq!(v["stale"], false, "追平的 follower 不应标记 stale");

    // follower 控制写：单跳转交 leader（用户不存在 → NotFound），并等本地 apply。
    let resp = b
        .handle
        .client_write(ControlCommand::UserDelete {
            username: "__obs_missing__".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp, ControlResponse::NotFound);
    assert_eq!(
        b.handle.counters().forwards(),
        1,
        "follower 必须恰好转发一次"
    );

    // leader 变更计数由后台采样任务累加（首次当选 +1），等它观察到。
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while a.handle.counters().leader_changes() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader_changes 未累加"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    for n in [a, b] {
        n.shutdown().await;
    }
}

/// 真实 HTTP 分块安装：leader 构建快照 → 手动按 offset 分块 POST → 新节点应用。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_build_upload_and_install_over_http() {
    const CHUNK: usize = 16 * 1024;
    let dir_a = tempfile::tempdir().unwrap();
    let mut sa = settings(dir_a.path(), 1, true);
    sa.snapshot_max_chunk_size = CHUNK as u64;
    let a = raft_harness::spawn_node(sa).await;
    raft_harness::wait_leader(&a.handle).await;
    // S7.2 前快照载荷只有 raft_kv；直接从状态机库填充，验证跨块搬运即可。
    let value = "v".repeat(8 * 1024);
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}?mode=rwc",
        a.handle.settings().db_path.display()
    ))
    .await
    .unwrap();
    for i in 0..8 {
        sqlx::query("INSERT INTO raft_kv (key, value) VALUES (?, ?)")
            .bind(format!("k{i}"))
            .bind(&value)
            .execute(&pool)
            .await
            .unwrap();
    }
    drop(pool);
    a.handle.raft().trigger().snapshot().await.unwrap();
    // trigger 只投递命令；等快照真正落库。
    let mut sm = a.handle.state_machine().clone();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let snapshot = loop {
        if let Some(s) = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
        {
            break s;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "snapshot build timed out"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    let bytes = snapshot.snapshot.get_ref().clone();
    assert!(
        bytes.len() > CHUNK,
        "快照必须跨多块，实际 {} 字节",
        bytes.len()
    );
    let last = snapshot.meta.last_log_id.unwrap();
    let vote = Vote::new_committed(last.leader_id.term, last.leader_id.node_id);

    let dir_b = tempfile::tempdir().unwrap();
    let mut sb = settings(dir_b.path(), 2, false);
    sb.snapshot_max_chunk_size = CHUNK as u64;
    sb.election_timeout_ms = 60_000; // 安装期间不发起选举，避免自身 term 抢先
    let b = raft_harness::spawn_node(sb).await;
    let url = format!("{}/api/v1/site/raft/snapshot", b.base);
    let http = reqwest::Client::new();
    let total = bytes.len();
    let mut offset = 0;
    while offset < total {
        let end = (offset + CHUNK).min(total);
        let req = InstallSnapshotRequest::<TypeConfig> {
            vote,
            meta: snapshot.meta.clone(),
            offset: offset as u64,
            data: bytes[offset..end].to_vec(),
            done: end == total,
        };
        let resp = http
            .post(&url)
            .header(RPC_TOKEN_HEADER, &b.token)
            .json(&req)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "chunk {offset} 失败: {}",
            resp.text().await.unwrap_or_default()
        );
        offset = end;
    }
    let want = snapshot.meta.last_log_id.unwrap();
    assert!(
        b.handle
            .wait_applied(want.index, std::time::Duration::from_secs(10))
            .await,
        "B 未应用到快照 index {}",
        want.index
    );
    let (b_applied, b_membership) = b.handle.state_machine().applied().await.unwrap();
    assert_eq!(b_applied, snapshot.meta.last_log_id);
    assert_eq!(b_membership.voter_ids().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        b.handle.state_machine().get("k7").await.unwrap(),
        Some(value)
    );
    a.shutdown().await;
    b.shutdown().await;
}

/// S7.3/S7.6 可用的 3 节点骨架：各自独立 raft.db + 可达 advertise/RPC 地址。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_node_harness_spawns_isolated_rpc_endpoints() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(raft_harness::spawn_node(settings(dir.path(), i as u64 + 1, false)).await);
    }
    let http = reqwest::Client::new();
    for (i, node) in nodes.iter().enumerate() {
        let v: serde_json::Value = http
            .get(format!("{}/api/v1/site/raft/status", node.base))
            .header(RPC_TOKEN_HEADER, &node.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(v["node_id"], i as u64 + 1);
    }
    for node in nodes {
        node.shutdown().await;
    }
}
