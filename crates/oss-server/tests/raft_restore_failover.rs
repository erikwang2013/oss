//! M8：Restore 后续状态的 leader 故障切换演练 + follower 清理的即时 fencing。
//!
//! 覆盖：3 voter 上 restored 行复制 → 旧 leader 死亡 → 新 leader 接管后行不丢失、不重复
//! 提交；follower 在拿不到 leader epoch（`UNFENCED`）时发起清理，命令必须携带行自己的
//! 非零 fence（而不是 term 0 盲删），且一次清理只追加一条日志、重复清理不再 append。

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oss_server::db::{Db, JobSpec};
use oss_server::job_state::JobState;
use oss_server::raft::command::ControlCommand;
use oss_server::raft::control::ControlPlane;
use oss_server::raft::runstate::{RunState, read_runstate};
use oss_server::raft::{FencingEpoch, RaftHandle};
use oss_server::state::AppState;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

struct Node {
    id: u64,
    handle: Arc<RaftHandle>,
    db: Arc<Db>,
}

async fn connect(dir: &Path, name: &str) -> Arc<Db> {
    Arc::new(
        Db::connect_raft(&format!(
            "sqlite://{}?mode=rwc",
            dir.join(format!("{name}.db")).display()
        ))
        .await
        .unwrap(),
    )
}

async fn start_cluster(dir: &Path) -> (Vec<Node>, AppState) {
    let st = common::state(dir).await;
    let db1 = connect(dir, "a").await;
    let mut s1 = common::raft_harness::settings(dir, 1, true);
    s1.feature_level = 7;
    let n1 = common::raft_harness::spawn_node_with_pool(s1, Some(db1.pool().clone())).await;
    common::raft_harness::wait_leader(&n1.handle).await;
    let mut nodes = vec![Node {
        id: 1,
        handle: n1.handle,
        db: db1,
    }];
    for id in [2u64, 3] {
        let db = connect(dir, &format!("n{id}")).await;
        let mut s = common::raft_harness::settings(dir, id, false);
        s.feature_level = 7;
        let n = common::raft_harness::spawn_node_with_pool(s, Some(db.pool().clone())).await;
        oss_server::raft::membership::promote_voter(&nodes[0].handle, id, n.base.clone())
            .await
            .unwrap();
        nodes.push(Node {
            id,
            handle: n.handle,
            db,
        });
    }
    (nodes, st)
}

fn control_for(node: &Node, st: &AppState) -> Arc<ControlPlane> {
    Arc::new(ControlPlane::raft(
        Arc::clone(&node.handle),
        Arc::clone(&node.db),
        Arc::clone(&st.store),
        Arc::clone(&st.ap_cache),
        Arc::clone(&st.olap_cache),
        st.ap_region.clone(),
    ))
}

async fn wait_ready_at(node: &Node, level: u8) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if node.handle.is_leader() && node.handle.effective_feature() == Some(level) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {} never became feature-{level} leader",
            node.id
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_until<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !f().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timeout waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn runstate(db: &Db) -> RunState {
    read_runstate(db.pool()).await.unwrap()
}

/// 节点自己的 raft 日志中出现过的命令（按子串过滤）。
async fn log_entries(handle: &RaftHandle, needle: &str) -> Vec<String> {
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&handle.settings().db_path)
        .busy_timeout(Duration::from_secs(5));
    let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
    let rows: Vec<String> = sqlx::query_scalar("SELECT entry FROM raft_log ORDER BY idx")
        .fetch_all(&pool)
        .await
        .unwrap();
    pool.close().await;
    rows.into_iter().filter(|e| e.contains(needle)).collect()
}

fn restore_spec() -> JobSpec {
    JobSpec {
        kind: "restore".into(),
        bucket: "src".into(),
        prefix: "obj".into(),
        ..JobSpec::default()
    }
}

fn delete_fence(entry: &str) -> i64 {
    use openraft::{EntryPayload, entry::Entry};
    let entry: Entry<oss_server::raft::TypeConfig> = serde_json::from_str(entry).unwrap();
    match entry.payload {
        EntryPayload::Normal(ControlCommand::TierRestoreDelete { fence_term, .. }) => fence_term,
        other => panic!("unexpected raft entry: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_cleanup_after_leader_failover_is_fenced_by_row_term() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path()).await;
    wait_ready_at(&nodes[0], 7).await;
    let old_leader = &nodes[0];
    let old_fence = old_leader.handle.fencing_epoch().unwrap();
    assert!(old_fence.term > 0);
    let jobs = JobState::new(Some(control_for(old_leader, &st)));

    let at = now();
    jobs.create_restore(
        &old_leader.db,
        &restore_spec(),
        "src",
        "obj",
        "v1",
        at,
        old_fence,
    )
    .await
    .unwrap();
    jobs.tier_restore_set_state(
        &old_leader.db,
        "src",
        "obj",
        "v1",
        "restored",
        at + 7200,
        old_fence,
    )
    .await
    .unwrap();

    // 故障前：restored 行已复制到全部 voter（含行所有者 term）。
    let owner_term = old_fence.term as i64;
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("restored row on every voter", || async {
            runstate(&db).await.tier_restores.iter().any(|r| {
                r.state == "restored" && r.expires_at == at + 7200 && r.fence_term == owner_term
            })
        })
        .await;
    }
    assert_eq!(
        log_entries(&old_leader.handle, "TierRestoreUpsert")
            .await
            .len(),
        1
    );

    // 旧 leader 死亡 → 新 leader 接管（移除死节点以恢复 feature 7 生效级别）。
    old_leader.handle.shutdown().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let new_leader = loop {
        if let Some(n) = nodes[1..].iter().find(|n| n.handle.is_leader()) {
            break n;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no new leader after shutdown"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    oss_server::raft::membership::apply_local(
        &new_leader.handle,
        &oss_server::raft::membership::MembershipOp::RemoveNode { node_id: 1 },
    )
    .await
    .unwrap();
    wait_ready_at(new_leader, 7).await;
    let new_fence = new_leader.handle.fencing_epoch().unwrap();
    assert_ne!(new_fence.term, old_fence.term);

    // 不丢状态、不重复提交：restored 行与唯一一条 upsert 都还在。
    for node in nodes.iter().filter(|n| n.id != 1) {
        let db = Arc::clone(&node.db);
        wait_until("restored row survives failover", || async {
            runstate(&db).await.tier_restores.iter().any(|r| {
                r.state == "restored" && r.expires_at == at + 7200 && r.fence_term == owner_term
            })
        })
        .await;
    }
    assert_eq!(
        log_entries(&new_leader.handle, "TierRestoreUpsert")
            .await
            .len(),
        1,
        "failover must not re-commit the restore post-state"
    );

    // follower 的读路径拿不到 leader epoch → UNFENCED；清理仍须立即生效。
    let follower = nodes
        .iter()
        .find(|n| n.id != 1 && n.id != new_leader.id)
        .unwrap();
    let follower_jobs = JobState::new(Some(control_for(follower, &st)));
    follower_jobs
        .tier_restore_delete(&follower.db, "src", "obj", "v1", FencingEpoch::UNFENCED)
        .await
        .unwrap();

    for node in nodes.iter().filter(|n| n.id != 1) {
        let db = Arc::clone(&node.db);
        wait_until("cleanup on every surviving voter", || async {
            runstate(&db).await.tier_restores.is_empty()
        })
        .await;
    }

    // 命令携带行所有者的非零 term（term 0 盲删只会在 leader 上 no-op）。
    let deletes = log_entries(&new_leader.handle, "TierRestoreDelete").await;
    assert_eq!(deletes.len(), 1, "exactly one cleanup command");
    assert_eq!(
        delete_fence(&deletes[0]),
        owner_term,
        "follower cleanup must be fenced by the row term, not UNFENCED(0)"
    );

    // 幂等：行已在本地消失，重复清理不 append。
    follower_jobs
        .tier_restore_delete(&follower.db, "src", "obj", "v1", FencingEpoch::UNFENCED)
        .await
        .unwrap();
    assert_eq!(
        log_entries(&new_leader.handle, "TierRestoreDelete")
            .await
            .len(),
        1,
        "duplicate cleanup must not append"
    );
}
