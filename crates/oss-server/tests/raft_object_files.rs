//! F2：对象后端 Raft 文件存储接线。
//!
//! 3 节点对象集群在 `<db_path>.d/` 文件上启动/选举/复制；follower 停机重启追赶；
//! 快照构建 → purge → 空节点安装；witness 元数据状态机；旧 SQLite `raft.db` 拒绝。
//! 更细的 append/meta 崩溃窗口（kill9）由 `raft::file_log::tests` 覆盖。

mod common;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use common::raft_harness;
use openraft::{BasicNode, StoredMembership};
use oss_server::config::RaftRole;
use oss_server::control_store_obj::{ObjectControlStore, RootIo};
use oss_server::local_store::LocalStore;
use oss_server::raft::file_log::raft_dir;
use oss_server::raft::{ControlCommand, ControlResponse, RaftHandle, routes};
use xl_store::control_state::root::Mode;
use xl_store::control_store::bucket::BucketStore;
use xl_store::store::{Store, StoreConfig};

struct ObjNode {
    handle: Arc<RaftHandle>,
    control: Arc<ObjectControlStore>,
    base: String,
    addr: SocketAddr,
    db_path: PathBuf,
    server: tokio::task::JoinHandle<()>,
}

impl ObjNode {
    async fn shutdown(self) {
        self.handle.shutdown().await;
        self.server.abort();
    }
}

/// 每个节点一个目录：控制对象存储 `<dir>/store`，Raft 文件 `<dir>/raft-<id>.db.d`。
async fn open_control(dir: &Path) -> Arc<ObjectControlStore> {
    std::fs::create_dir_all(dir).unwrap();
    let mut cfg = StoreConfig::new(dir);
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let local = LocalStore::open(dir.join("local")).unwrap();
    let control = ObjectControlStore::new(Arc::new(RootIo::new(store))).with_local_store(local);
    control.bootstrap(Mode::Raft, 100).await.unwrap();
    Arc::new(control)
}

async fn bind_retry(addr: SocketAddr) -> tokio::net::TcpListener {
    for _ in 0..200 {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => return listener,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("rebind {addr} failed");
}

async fn spawn_object_node(
    dir: &Path,
    node_id: u64,
    bootstrap: bool,
    fixed: Option<SocketAddr>,
) -> ObjNode {
    let listener = match fixed {
        Some(addr) => bind_retry(addr).await,
        None => tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
    };
    let addr = listener.local_addr().unwrap();
    let control = open_control(&dir.join("store")).await;
    let mut settings = raft_harness::settings(dir, node_id, bootstrap);
    settings.advertise_endpoint = format!("http://{addr}");
    let token = settings.cluster_token.clone();
    let db_path = settings.db_path.clone();
    let handle = Arc::new(
        oss_server::raft::spawn_object(settings, Arc::clone(&control), None, None)
            .await
            .unwrap(),
    );
    let app = routes::rpc_router(Arc::clone(&handle), token, false, false);
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    ObjNode {
        handle,
        control,
        base: format!("http://{addr}"),
        addr,
        db_path,
        server,
    }
}

async fn spawn_witness_node(
    dir: &Path,
    node_id: u64,
    fixed: Option<SocketAddr>,
) -> (
    Arc<RaftHandle>,
    String,
    SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let listener = match fixed {
        Some(addr) => bind_retry(addr).await,
        None => tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
    };
    let addr = listener.local_addr().unwrap();
    let mut settings = raft_harness::settings(dir, node_id, false);
    settings.role = RaftRole::Witness;
    settings.advertise_endpoint = format!("http://{addr}");
    let handle = Arc::new(
        oss_server::raft::spawn_witness(settings, None)
            .await
            .unwrap(),
    );
    let app = routes::rpc_router(
        Arc::clone(&handle),
        "s71-harness-token".to_string(),
        false,
        false,
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (handle, format!("http://{addr}"), addr, server)
}

async fn wait_for(
    what: &str,
    mut f: impl FnMut() -> Pin<Box<dyn std::future::Future<Output = bool> + 'static>>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if f().await {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{what} timed out");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_bucket(node: &ObjNode, name: &str) {
    let control = Arc::clone(&node.control);
    wait_for(name, move || {
        let control = Arc::clone(&control);
        let name = name.to_string();
        Box::pin(async move { control.bucket(&name).await.unwrap().is_some() })
    })
    .await;
}

fn bucket_create(name: &str) -> ControlCommand {
    ControlCommand::BucketCreate {
        name: name.into(),
        owner: None,
        created_at: 100,
        replication_mode: "async".into(),
        versioning: "Off".into(),
        object_lock: false,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        default_encryption: None,
    }
}

/// 当前 leader 在 `nodes` 中的下标（写入/成员变更必须发给 leader；`write` 不转发）。
async fn leader_index(nodes: &[&ObjNode]) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        for (i, node) in nodes.iter().enumerate() {
            let current = {
                let m = node.handle.raft().metrics();
                m.borrow().current_leader
            };
            if current == Some(node.handle.node_id()) {
                return i;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader election timed out"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn add_voters(leader: &RaftHandle, members: &[(u64, &str)], voters: BTreeSet<u64>) {
    for (id, base) in members {
        leader
            .raft()
            .add_learner(*id, BasicNode::new((*base).to_string()), true)
            .await
            .unwrap();
    }
    leader
        .raft()
        .change_membership(voters, false)
        .await
        .unwrap();
}

fn assert_file_backed(node: &ObjNode) {
    assert!(
        !node.db_path.exists(),
        "对象模式不得创建 legacy raft.db: {}",
        node.db_path.display()
    );
    let dir = raft_dir(&node.db_path);
    assert!(dir.join("log").is_dir(), "缺少文件日志目录");
    assert!(dir.join("meta").is_file(), "缺少文件 meta");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_node_object_cluster_replicates_on_files() {
    let root = tempfile::tempdir().unwrap();
    let n1 = spawn_object_node(&root.path().join("n1"), 1, true, None).await;
    let n2 = spawn_object_node(&root.path().join("n2"), 2, false, None).await;
    let n3 = spawn_object_node(&root.path().join("n3"), 3, false, None).await;
    let nodes = [&n1, &n2, &n3];
    raft_harness::wait_leader(&n1.handle).await;
    add_voters(
        &nodes[leader_index(&nodes).await].handle,
        &[(2, &n2.base), (3, &n3.base)],
        BTreeSet::from([1, 2, 3]),
    )
    .await;

    let out = nodes[leader_index(&nodes).await]
        .handle
        .write(bucket_create("b1"))
        .await
        .unwrap();
    assert_eq!(out, ControlResponse::Ok);
    for node in [&n1, &n2, &n3] {
        wait_bucket(node, "b1").await;
        assert_file_backed(node);
    }

    // 快照路径也走文件目录（此时可能尚未构建）。
    assert!(raft_dir(&n1.db_path).join("snapshot").is_dir());

    for node in [n3, n2, n1] {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_restart_catches_up_on_files() {
    let root = tempfile::tempdir().unwrap();
    let n1 = spawn_object_node(&root.path().join("n1"), 1, true, None).await;
    let n2 = spawn_object_node(&root.path().join("n2"), 2, false, None).await;
    let n3 = spawn_object_node(&root.path().join("n3"), 3, false, None).await;
    let nodes = [&n1, &n2, &n3];
    raft_harness::wait_leader(&n1.handle).await;
    add_voters(
        &nodes[leader_index(&nodes).await].handle,
        &[(2, &n2.base), (3, &n3.base)],
        BTreeSet::from([1, 2, 3]),
    )
    .await;

    nodes[leader_index(&nodes).await]
        .handle
        .write(bucket_create("b1"))
        .await
        .unwrap();
    wait_bucket(&n3, "b1").await;

    let n3_addr = n3.addr;
    n3.shutdown().await;
    let alive = [&n1, &n2];
    alive[leader_index(&alive).await]
        .handle
        .write(bucket_create("b2"))
        .await
        .unwrap();
    wait_bucket(&n2, "b2").await;

    // 同一目录、同一 advertise 地址重启：节点从文件日志追平停机期间的写入。
    let n3b = spawn_object_node(&root.path().join("n3"), 3, false, Some(n3_addr)).await;
    wait_bucket(&n3b, "b2").await;
    assert_eq!(
        n3b.handle.settings().advertise_endpoint,
        format!("http://{n3_addr}")
    );
    assert_file_backed(&n3b);

    for node in [n3b, n2, n1] {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_purge_and_empty_node_install_on_files() {
    let root = tempfile::tempdir().unwrap();
    let n1 = spawn_object_node(&root.path().join("n1"), 1, true, None).await;
    raft_harness::wait_leader(&n1.handle).await;
    n1.handle.write(bucket_create("b1")).await.unwrap();

    n1.handle.raft().trigger().snapshot().await.unwrap();
    wait_for("snapshot build", || {
        let handle = Arc::clone(&n1.handle);
        Box::pin(async move {
            let m = handle.raft().metrics();
            m.borrow().snapshot.is_some()
        })
    })
    .await;
    let snap_index = {
        let m = n1.handle.raft().metrics();
        m.borrow().snapshot.unwrap().index
    };
    n1.handle
        .raft()
        .trigger()
        .purge_log(snap_index)
        .await
        .unwrap();
    let meta_path = raft_dir(&n1.db_path).join("meta");
    wait_for("log purge", || {
        let meta_path = meta_path.clone();
        Box::pin(async move {
            let Ok(bytes) = std::fs::read(&meta_path) else {
                return false;
            };
            let meta: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            meta["last_purged"]["index"]
                .as_u64()
                .is_some_and(|i| i >= snap_index)
        })
    })
    .await;

    let n2 = spawn_object_node(&root.path().join("n2"), 2, false, None).await;
    n1.handle
        .raft()
        .add_learner(2, BasicNode::new(n2.base.clone()), true)
        .await
        .unwrap();
    assert!(
        n2.handle
            .wait_applied(snap_index, Duration::from_secs(30))
            .await,
        "空节点未安装快照（index {snap_index}）"
    );
    wait_bucket(&n2, "b1").await;
    let learner_snapshots = raft_dir(&n2.db_path).join("snapshot");
    let mut seen = false;
    if let Ok(entries) = std::fs::read_dir(&learner_snapshots) {
        for entry in entries.flatten() {
            if entry.path().join("manifest").is_file() {
                seen = true;
            }
        }
    }
    assert!(
        seen,
        "learner 未落盘快照文件: {}",
        learner_snapshots.display()
    );

    // purge 后重启 leader：文件日志/快照/root 的三方位置必须自洽，仍可继续写。
    let addr = n1.addr;
    n1.shutdown().await;
    let n1b = spawn_object_node(&root.path().join("n1"), 1, true, Some(addr)).await;
    raft_harness::wait_leader(&n1b.handle).await;
    wait_bucket(&n1b, "b1").await;
    n1b.handle.write(bucket_create("b2")).await.unwrap();
    wait_bucket(&n1b, "b2").await;
    wait_bucket(&n2, "b2").await;

    for node in [n2, n1b] {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn witness_joins_as_voter_with_metadata_only_state() {
    let root = tempfile::tempdir().unwrap();
    let n1 = spawn_object_node(&root.path().join("n1"), 1, true, None).await;
    raft_harness::wait_leader(&n1.handle).await;
    let witness_dir = root.path().join("w");
    let (w, w_base, w_addr, w_server) = spawn_witness_node(&witness_dir, 2, None).await;
    n1.handle
        .raft()
        .add_learner(2, BasicNode::new(w_base.clone()), true)
        .await
        .unwrap();
    n1.handle
        .raft()
        .change_membership(BTreeSet::from([1, 2]), false)
        .await
        .unwrap();
    n1.handle.write(bucket_create("b1")).await.unwrap();
    // 写入返回只保证多数派复制；witness 的 apply 可能仍停在成员变更条目。
    let target = n1.handle.applied_index();

    let w_db = witness_dir.join("raft-2.db");
    let state_path = raft_dir(&w_db).join("witness").join("state");
    let apply_state_path = state_path.clone();
    wait_for("witness apply", move || {
        let state_path = apply_state_path.clone();
        Box::pin(async move {
            let Ok(bytes) = std::fs::read(&state_path) else {
                return false;
            };
            let state: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let Ok(membership) = serde_json::from_value::<StoredMembership<u64, BasicNode>>(
                state["membership"].clone(),
            ) else {
                return false;
            };
            state["applied"]["index"]
                .as_u64()
                .is_some_and(|i| i >= target)
                && membership.voter_ids().count() == 2
        })
    })
    .await;
    assert!(!w_db.exists(), "witness 不得创建 legacy raft.db");
    assert!(
        !witness_dir.join("state.db").exists(),
        "witness 不得打开 SQLite state_db"
    );
    assert!(raft_dir(&w_db).join("log").is_dir());
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(state["identity"]["raft_role"], "witness");
    assert_eq!(state["identity"]["raft_node_id"], "2");
    let applied_before = state["applied"].clone();

    // 重启后 metadata 状态仍可加载，initialized 由成员存在补齐为 1。
    w.shutdown().await;
    w_server.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (w2, _base, _addr, server2) = spawn_witness_node(&witness_dir, 2, Some(w_addr)).await;
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(state["applied"], applied_before, "applied 必须保留");
    // The witness replays its log asynchronously after spawn; membership (and
    // therefore raft_initialized) becomes visible once replay reaches the
    // membership entry.
    let mut initialized = state["identity"]["raft_initialized"].clone();
    for _ in 0..100 {
        if initialized == "1" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let state: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        initialized = state["identity"]["raft_initialized"].clone();
    }
    assert_eq!(initialized, "1");
    w2.shutdown().await;
    server2.abort();
    n1.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_sqlite_raft_db_is_refused_with_actionable_message() {
    let root = tempfile::tempdir().unwrap();
    let node_dir = root.path().join("n1");
    let control = open_control(&node_dir.join("store")).await;
    let settings = raft_harness::settings(&node_dir, 1, true);
    std::fs::write(&settings.db_path, b"SQLite format 3\0").unwrap();
    let err = oss_server::raft::spawn_object(settings, control, None, None)
        .await
        .err()
        .expect("legacy raft.db 必须拒绝")
        .to_string();
    assert!(err.contains("legacy SQLite raft database"), "{err}");
    assert!(err.contains("control-migrate"), "错误需给出迁移命令: {err}");
    assert!(!raft_dir(&node_dir.join("raft-1.db")).exists());
}
