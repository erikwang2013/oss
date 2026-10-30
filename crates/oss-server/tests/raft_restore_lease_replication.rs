//! feature 7：tier restore 后续状态与调度租约经 Raft 复制。
//!
//! 3 voter 覆盖：restored/清理收敛到全部 voter（清理可从 follower 转交）；租约续约
//! 不刷日志、leader 死亡后新 leader 必须等旧租约到期才接管；feature 6 集群在 append
//! 前拒绝 level-7 命令（不写日志）。时间由调用方注入，测试不依赖 sleep 等到期。

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oss_server::db::{Db, JobSpec};
use oss_server::job_state::JobState;
use oss_server::raft::command::ControlCommand;
use oss_server::raft::control::ControlPlane;
use oss_server::raft::runstate::{RunState, read_runstate};
use oss_server::raft::{FencingEpoch, LeaderGate, RaftHandle};
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

async fn start_cluster(dir: &Path, feature_level: u8) -> (Vec<Node>, AppState) {
    let st = common::state(dir).await;
    let db1 = connect(dir, "a").await;
    let mut s1 = common::raft_harness::settings(dir, 1, true);
    s1.feature_level = feature_level;
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
        s.feature_level = feature_level;
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

async fn start_single(dir: &Path, feature_level: u8) -> (Node, AppState) {
    let st = common::state(dir).await;
    let db = connect(dir, "solo").await;
    let mut s = common::raft_harness::settings(dir, 1, true);
    s.feature_level = feature_level;
    let n = common::raft_harness::spawn_node_with_pool(s, Some(db.pool().clone())).await;
    common::raft_harness::wait_leader(&n.handle).await;
    (
        Node {
            id: 1,
            handle: n.handle,
            db,
        },
        st,
    )
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_poststate_replicates_and_cleanup_converges() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 7).await;
    wait_ready_at(&nodes[0], 7).await;
    let leader = &nodes[0];
    let control = control_for(leader, &st);
    let jobs = JobState::new(Some(Arc::clone(&control)));
    assert!(jobs.restore_replicated());
    let fence = leader.handle.fencing_epoch().unwrap();

    let at = now();
    jobs.create_restore(&leader.db, &restore_spec(), "src", "obj", "v1", at, fence)
        .await
        .unwrap();
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("pending restore on every voter", || async {
            runstate(&db)
                .await
                .tier_restores
                .iter()
                .any(|r| r.state == "pending")
        })
        .await;
    }

    jobs.tier_restore_set_state(&leader.db, "src", "obj", "v1", "restored", at + 3600, fence)
        .await
        .unwrap();
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("restored on every voter", || async {
            runstate(&db)
                .await
                .tier_restores
                .iter()
                .any(|r| r.state == "restored" && r.expires_at == at + 3600)
        })
        .await;
    }

    // 同内容同 token 重复写：无逻辑变化，不 append。
    let before = log_entries(&leader.handle, "TierRestoreUpsert").await.len();
    jobs.tier_restore_set_state(&leader.db, "src", "obj", "v1", "restored", at + 3600, fence)
        .await
        .unwrap();
    // 旧 term 的迟到状态：no-op，不 append、不覆盖 restored。
    let stale = FencingEpoch {
        term: fence.term.saturating_sub(1),
        leader: 0,
    };
    jobs.tier_restore_set_state(&leader.db, "src", "obj", "v1", "pending", 0, stale)
        .await
        .unwrap();
    assert_eq!(
        log_entries(&leader.handle, "TierRestoreUpsert").await.len(),
        before,
        "duplicate and old-term restore writes must not append"
    );
    assert!(
        runstate(&leader.db)
            .await
            .tier_restores
            .iter()
            .all(|r| r.state == "restored")
    );

    // 清理从 follower 转交 leader；两次删除幂等。
    let follower = nodes.iter().find(|n| n.id != leader.id).unwrap();
    let follower_jobs = JobState::new(Some(control_for(follower, &st)));
    for _ in 0..2 {
        follower_jobs
            .tier_restore_delete(&follower.db, "src", "obj", "v1", fence)
            .await
            .unwrap();
    }
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("cleanup on every voter", || async {
            runstate(&db).await.tier_restores.is_empty()
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_renews_and_takes_over_only_after_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 7).await;
    wait_ready_at(&nodes[0], 7).await;
    let old_leader = &nodes[0];
    let old_term = old_leader.handle.fencing_epoch().unwrap().term;
    let jobs = JobState::new(Some(control_for(old_leader, &st)));
    let gate = LeaderGate::from_raft(Some(Arc::clone(&old_leader.handle)));
    let ttl = 10i64;
    let t0 = now();

    let epoch = jobs
        .schedule_fence(&old_leader.db, &gate, t0, ttl)
        .await
        .unwrap()
        .expect("leader acquires the lease");
    assert_eq!(epoch.term, old_term);
    assert_eq!(epoch.leader, old_leader.id);
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("lease on every voter", || async {
            runstate(&db)
                .await
                .schedule_lease
                .is_some_and(|l| l.node == old_leader.id as i64 && l.expires_at == t0 + ttl)
        })
        .await;
    }

    // 剩余充足时不续约、不 append；过半后延长 expiry（acquired_at 不变）。
    let before = log_entries(&old_leader.handle, "ScheduleLeaseAcquire")
        .await
        .len();
    jobs.schedule_fence(&old_leader.db, &gate, t0 + 4, ttl)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        log_entries(&old_leader.handle, "ScheduleLeaseAcquire")
            .await
            .len(),
        before
    );
    let renewed_at = t0 + 6;
    jobs.schedule_fence(&old_leader.db, &gate, renewed_at, ttl)
        .await
        .unwrap()
        .unwrap();
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("renewed lease on every voter", || async {
            runstate(&db)
                .await
                .schedule_lease
                .is_some_and(|l| l.expires_at == renewed_at + ttl && l.acquired_at == t0)
        })
        .await;
    }
    let old_expiry = renewed_at + ttl;

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
    // 移除死节点，让生效级别回到 7（否则 level-7 写 fail-closed）。
    oss_server::raft::membership::apply_local(
        &new_leader.handle,
        &oss_server::raft::membership::MembershipOp::RemoveNode { node_id: 1 },
    )
    .await
    .unwrap();
    wait_ready_at(new_leader, 7).await;
    let gate = LeaderGate::from_raft(Some(Arc::clone(&new_leader.handle)));
    let jobs = JobState::new(Some(control_for(new_leader, &st)));

    // 旧租约未到期：不得接管，也不写日志。
    let before = log_entries(&new_leader.handle, "ScheduleLeaseAcquire")
        .await
        .len();
    assert!(
        jobs.schedule_fence(&new_leader.db, &gate, old_expiry - 1, ttl)
            .await
            .unwrap()
            .is_none(),
        "takeover before expiry must wait"
    );
    assert_eq!(
        log_entries(&new_leader.handle, "ScheduleLeaseAcquire")
            .await
            .len(),
        before
    );
    let epoch = jobs
        .schedule_fence(&new_leader.db, &gate, old_expiry, ttl)
        .await
        .unwrap()
        .expect("expired lease is taken over");
    assert_eq!(epoch.leader, new_leader.id);
    assert_ne!(epoch.term, old_term);
    for node in nodes.iter().filter(|n| n.id != 1) {
        let db = Arc::clone(&node.db);
        wait_until("takeover on every surviving voter", || async {
            runstate(&db)
                .await
                .schedule_lease
                .is_some_and(|l| l.node == new_leader.id as i64 && l.term > old_term as i64)
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feature_six_rejects_level_seven_commands_before_append() {
    let dir = tempfile::tempdir().unwrap();
    let (node, st) = start_single(dir.path(), 6).await;
    wait_ready_at(&node, 6).await;
    let control = control_for(&node, &st);
    let now = now();
    let lease = ControlCommand::ScheduleLeaseAcquire {
        lease: oss_server::raft::runstate::ScheduleLeaseRow {
            term: 1,
            node: 1,
            expires_at: now + 10,
            acquired_at: now,
        },
        now,
    };
    let restore = ControlCommand::TierRestoreDelete {
        bucket: "src".into(),
        key: "obj".into(),
        version_id: "v1".into(),
        fence_term: 1,
    };
    for cmd in [lease, restore] {
        let err = control.write(cmd).await.expect_err("level-7 rejected");
        assert!(err.to_string().contains("feature level 7"), "{err}");
    }
    assert!(
        log_entries(&node.handle, "ScheduleLeaseAcquire")
            .await
            .is_empty(),
        "rejected command must not reach the log"
    );
    assert!(
        log_entries(&node.handle, "TierRestoreDelete")
            .await
            .is_empty(),
        "rejected command must not reach the log"
    );
    // 命令从未 apply：租约行不存在。
    assert!(runstate(&node.db).await.schedule_lease.is_none());
}
