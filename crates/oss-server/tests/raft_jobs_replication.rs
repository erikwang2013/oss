//! M8-2：jobs/job_tasks 运行态经 Raft 复制的垂直切片（feature 4）。
//!
//! 用 3 voter 而不是 2 voter：2 voter 集群在 leader 死亡后无法再凑多数派，无法验证
//! “新 leader 看到作业行并接着跑”；这是对任务书“2 节点 harness”的有意偏离，已在报告注明。
//! 覆盖：leader 建/领/收 → follower 收敛；旧 term/重复/回退行被 apply 闸拒绝；
//! leader 死亡 → 新 leader 用既有 stale 回收接管；feature≤3 保持本地直写且 Job* 不进日志；
//! 混版旧 peer（advertise 3）把生效级别压回 3、Job* 被拒，移除后恢复；
//! 命令尺寸上限；写出的行在 `m8.runstate` 快照/安装后一致。

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oss_server::db::{Db, JobSpec, NewJobTask};
use oss_server::job_state::JobState;
use oss_server::raft::command::{ControlCommand, ControlResponse};
use oss_server::raft::control::ControlPlane;
use oss_server::raft::data::ControlData;
use oss_server::raft::runstate::{
    JobRow, JobTaskRow, RunState, TierRestoreRow, decode_runstate_snapshot,
    encode_runstate_snapshot, install_runstate_tx, read_runstate,
};
use oss_server::raft::{FencingEpoch, LeaderGate, RaftHandle};
use oss_server::state::AppState;
use oss_server::tier_jobs::SqlRestoreSink;
use xl_s3::restore::RestoreSink;

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

/// 3 voter 集群；`st` 只提供 Store/AP 缓存给 `ControlPlane::raft`（作业路径不碰它们）。
async fn start_cluster(dir: &Path, feature_level: u8) -> (Vec<Node>, AppState) {
    start_cluster_with(dir, [feature_level; 3]).await
}

/// 同 [`start_cluster`]，但每节点可用不同 feature level（模拟混版旧 peer）。
async fn start_cluster_with(dir: &Path, levels: [u8; 3]) -> (Vec<Node>, AppState) {
    let st = common::state(dir).await;
    let db1 = connect(dir, "a").await;
    let mut s1 = common::raft_harness::settings(dir, 1, true);
    s1.feature_level = levels[0];
    let n1 = common::raft_harness::spawn_node_with_pool(s1, Some(db1.pool().clone())).await;
    common::raft_harness::wait_leader(&n1.handle).await;
    let mut nodes = vec![Node {
        id: 1,
        handle: n1.handle,
        db: db1,
    }];
    for (i, id) in [2u64, 3].into_iter().enumerate() {
        let db = connect(dir, &format!("n{id}")).await;
        let mut s = common::raft_harness::settings(dir, id, false);
        s.feature_level = levels[i + 1];
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

async fn wait_ready(node: &Node) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if node.handle.is_leader() && node.handle.effective_feature() == Some(4) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {} never became feature-4 leader",
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

/// 节点自己的 raft 日志（`raft-<id>.db`，不是控制库）中出现过的 Job* 命令。
async fn job_log_entries(handle: &RaftHandle) -> Vec<String> {
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&handle.settings().db_path)
        .busy_timeout(Duration::from_secs(5));
    let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
    let rows: Vec<String> = sqlx::query_scalar("SELECT entry FROM raft_log ORDER BY idx")
        .fetch_all(&pool)
        .await
        .unwrap();
    pool.close().await;
    rows.into_iter()
        .filter(|e| {
            ["JobBatch", "JobUpsert", "JobTaskUpsert"]
                .iter()
                .any(|name| e.contains(name))
        })
        .collect()
}

fn batch_spec(tasks: usize) -> JobSpec {
    JobSpec {
        kind: "batch".into(),
        bucket: "src".into(),
        prefix: "p/".into(),
        requested_by: Some(1),
        priority: 1,
        tasks: (0..tasks)
            .map(|i| NewJobTask {
                action: "DeleteObject".into(),
                payload: format!(r#"{{"key":"p/{i}"}}"#),
                dedupe_key: None,
            })
            .collect(),
        ..JobSpec::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_writes_replicate_and_survive_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    let leader = &nodes[0];
    wait_ready(leader).await;
    let jobs = JobState::new(Some(control_for(leader, &st)));
    assert!(
        jobs.replicated(),
        "feature-4 leader must replicate job rows"
    );

    let id = jobs
        .create_job(&leader.db, &batch_spec(2), now())
        .await
        .unwrap();
    assert_eq!(id, 1);
    for f in &nodes[1..] {
        let db = Arc::clone(&f.db);
        wait_until("job rows on follower", || async {
            let rs = runstate(&db).await;
            rs.jobs.iter().any(|j| j.id == id && j.state == "queued")
                && rs.job_tasks.iter().filter(|t| t.job_id == id).count() == 2
        })
        .await;
    }

    let fence = leader.handle.fencing_epoch().expect("leader epoch");
    assert!(fence.term > 0);
    let t1 = jobs
        .claim_task_fenced(&leader.db, now(), 60, fence)
        .await
        .unwrap()
        .expect("task 1");
    assert_eq!(t1.task_no, 1);

    // 旧 term 的收尾：必须返回 fenced 且不落库（任务保持 running）。
    let stale = FencingEpoch {
        term: fence.term.saturating_sub(1),
        leader: 0,
    };
    let rejected = jobs
        .finish_task_fenced(
            &leader.db,
            t1.job_id,
            t1.task_no,
            true,
            "Succeeded",
            None,
            now(),
            3,
            stale,
        )
        .await
        .unwrap();
    assert_eq!(rejected, "fenced");
    let rs = runstate(&leader.db).await;
    assert_eq!(
        rs.job_tasks
            .iter()
            .find(|t| t.job_id == id && t.task_no == 1)
            .unwrap()
            .state,
        "running"
    );

    assert_eq!(
        jobs.finish_task_fenced(
            &leader.db,
            t1.job_id,
            t1.task_no,
            true,
            "Succeeded",
            None,
            now(),
            3,
            fence
        )
        .await
        .unwrap(),
        "done"
    );
    let t2 = jobs
        .claim_task_fenced(&leader.db, now(), 60, fence)
        .await
        .unwrap()
        .expect("task 2");
    assert_eq!(t2.task_no, 2);
    assert_eq!(
        jobs.finish_task_fenced(
            &leader.db,
            t2.job_id,
            t2.task_no,
            true,
            "Succeeded",
            None,
            now(),
            3,
            fence
        )
        .await
        .unwrap(),
        "done"
    );

    for f in &nodes[1..] {
        let db = Arc::clone(&f.db);
        wait_until("job done on follower", || async {
            let rs = runstate(&db).await;
            let job = rs.jobs.iter().find(|j| j.id == id);
            job.is_some_and(|j| j.state == "done" && j.succeeded == 2 && j.progress == 2)
                && rs
                    .job_tasks
                    .iter()
                    .filter(|t| t.job_id == id && t.state == "done")
                    .count()
                    == 2
        })
        .await;
    }

    // 重复/回退：后到的旧状态（updated_at/计数更低、term 更旧）必须被 apply 闸拒绝。
    let rs = runstate(&nodes[1].db).await;
    let done_job = rs.jobs.iter().find(|j| j.id == id).unwrap().clone();
    let done_tasks: Vec<JobTaskRow> = rs
        .job_tasks
        .iter()
        .filter(|t| t.job_id == id)
        .cloned()
        .collect();
    let mut stale_job = done_job.clone();
    stale_job.state = "queued".into();
    stale_job.updated_at -= 1_000;
    stale_job.progress = 0;
    stale_job.succeeded = 0;
    stale_job.finished_at = None;
    let mut stale_task = done_tasks[0].clone();
    stale_task.state = "queued".into();
    stale_task.attempt = 0;
    stale_task.fence_term = 0;
    stale_task.fence_leader = 0;
    let control = control_for(leader, &st);
    assert_eq!(
        control
            .write(ControlCommand::JobBatch {
                jobs: vec![stale_job],
                tasks: vec![stale_task],
                restores: Vec::new(),
            })
            .await
            .expect("stale batch reaches apply"),
        ControlResponse::Conflict,
        "stale row must be rejected as a whole, not written"
    );
    // 原样重放终态（重复投递）同样被版本闸拒绝，但不改变任何列。
    assert_eq!(
        control
            .write(ControlCommand::JobBatch {
                jobs: vec![done_job],
                tasks: done_tasks,
                restores: Vec::new(),
            })
            .await
            .expect("duplicate batch reaches apply"),
        ControlResponse::Conflict,
        "duplicate version is a no-op rejection"
    );
    for f in &nodes[1..] {
        let db = Arc::clone(&f.db);
        wait_until("no regression on follower", || async {
            let rs = runstate(&db).await;
            let job = rs.jobs.iter().find(|j| j.id == id);
            job.is_some_and(|j| j.state == "done" && j.succeeded == 2)
                && rs
                    .job_tasks
                    .iter()
                    .all(|t| t.job_id != id || t.state == "done")
        })
        .await;
    }

    // 快照/安装：行与补齐后的 next_job_id 一致（runstate_meta 未播种也不得触发 validate 拒绝）。
    let rs = runstate(&nodes[1].db).await;
    assert_eq!(
        rs.next_job_id,
        id + 1,
        "read_runstate 必须把 next_job_id 补到 max(id)+1"
    );
    let snap = encode_runstate_snapshot(&ControlData::default(), &rs, 3).unwrap();
    let (_, decoded) = decode_runstate_snapshot(&snap).unwrap();
    assert_eq!(decoded, rs);
    let fresh = connect(dir.path(), "snap").await;
    let mut tx = fresh.pool().begin().await.unwrap();
    install_runstate_tx(&mut tx, &rs).await.unwrap();
    tx.commit().await.unwrap();
    let back = runstate(&fresh).await;
    assert_eq!(back.jobs, rs.jobs);
    assert_eq!(back.job_tasks, rs.job_tasks);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_leader_reclaims_stale_claim_and_finishes_job() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let old = JobState::new(Some(control_for(&nodes[0], &st)));
    let id = old
        .create_job(&nodes[0].db, &batch_spec(2), now())
        .await
        .unwrap();
    let fence = nodes[0].handle.fencing_epoch().unwrap();
    let t1 = old
        .claim_task_fenced(&nodes[0].db, now(), 60, fence)
        .await
        .unwrap()
        .expect("task 1 claimed before failover");
    assert_eq!(t1.task_no, 1);

    nodes[0].handle.shutdown().await;
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
    // P1-6：死 voter 仍在成员表 → 新 term 生效 feature 降为 1，feature-4 写必须失败关闭，
    // 不得退回本地直写（否则 follower 永远收不到该作业）。
    let leader_handle = Arc::clone(&new_leader.handle);
    wait_until("effective feature drops after leader death", || async {
        leader_handle.effective_feature().is_some_and(|l| l < 4)
    })
    .await;
    let jobs = JobState::new(Some(control_for(new_leader, &st)));
    let err = jobs
        .create_job(&new_leader.db, &batch_spec(1), now())
        .await
        .expect_err("feature-4 write must fail closed while a dead voter remains");
    assert!(err.contains("effective feature"), "{err}");
    assert!(
        runstate(&new_leader.db)
            .await
            .jobs
            .iter()
            .all(|j| j.id != id + 1),
        "fail-closed write must not create a local-only job row"
    );
    // 永久失联必须先移除成员（运维修复步骤）才能继续复制；这是本切片的已知运维约束。
    oss_server::raft::membership::apply_local(
        &new_leader.handle,
        &oss_server::raft::membership::MembershipOp::RemoveNode { node_id: 1 },
    )
    .await
    .unwrap();
    wait_ready(new_leader).await;
    let rs = runstate(&new_leader.db).await;
    assert!(
        rs.jobs.iter().any(|j| j.id == id),
        "new leader must hold job row"
    );
    assert_eq!(
        rs.job_tasks
            .iter()
            .find(|t| t.job_id == id && t.task_no == 1)
            .unwrap()
            .state,
        "running"
    );

    let fence = new_leader.handle.fencing_epoch().unwrap();
    // 既有 stale 回收：接管旧 holder 的 running 行（按一次失败重排）。
    let n = jobs
        .recover_stale_tasks(&new_leader.db, now() + 60, 3, fence)
        .await
        .unwrap();
    assert_eq!(n, 1);
    // 重排任务的 next_at 有抖动，可能先领到另一条；两条都跑完才算作业收尾。
    let t1 = jobs
        .claim_task_fenced(&new_leader.db, now() + 2_000, 60, fence)
        .await
        .unwrap()
        .expect("reclaimed task claimable");
    assert_eq!(
        jobs.finish_task_fenced(
            &new_leader.db,
            t1.job_id,
            t1.task_no,
            true,
            "Succeeded",
            None,
            now(),
            3,
            fence
        )
        .await
        .unwrap(),
        "done"
    );
    let t2 = jobs
        .claim_task_fenced(&new_leader.db, now() + 2_000, 60, fence)
        .await
        .unwrap()
        .expect("task 2 claimable");
    assert_eq!(
        jobs.finish_task_fenced(
            &new_leader.db,
            t2.job_id,
            t2.task_no,
            true,
            "Succeeded",
            None,
            now(),
            3,
            fence
        )
        .await
        .unwrap(),
        "done"
    );
    let survivor = nodes
        .iter()
        .find(|n| n.id != 1 && n.id != new_leader.id)
        .expect("third node");
    let db = Arc::clone(&survivor.db);
    wait_until("final state on survivor", || async {
        let rs = runstate(&db).await;
        rs.jobs
            .iter()
            .find(|j| j.id == id)
            .is_some_and(|j| j.state == "done" && j.succeeded == 2)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feature_two_keeps_local_job_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 2).await;
    common::raft_harness::wait_leader(&nodes[0].handle).await;
    let jobs = JobState::new(Some(control_for(&nodes[0], &st)));
    assert!(!jobs.replicated(), "feature 2 must not replicate job rows");
    let id = jobs
        .create_job(&nodes[0].db, &batch_spec(1), now())
        .await
        .unwrap();
    assert!(runstate(&nodes[0].db).await.jobs.iter().any(|j| j.id == id));
    tokio::time::sleep(Duration::from_millis(500)).await;
    for f in &nodes[1..] {
        assert!(
            runstate(&f.db).await.jobs.iter().all(|j| j.id != id),
            "feature<4 follower must not receive job rows"
        );
    }
}

/// feature 3（v1.22 声明级别）不启用作业复制：JobState 仍本地直写，Job* 命令在
/// leader 的 `check_feature` 处被拒且不产生日志 entry（旧节点绝不会收到）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feature_three_keeps_local_job_writes_and_rejects_job_commands() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 3).await;
    let leader = &nodes[0];
    let handle = Arc::clone(&leader.handle);
    wait_until("feature-3 leader ready", || async {
        handle.effective_feature() == Some(3)
    })
    .await;
    let jobs = JobState::new(Some(control_for(leader, &st)));
    assert!(!jobs.replicated(), "feature 3 must not replicate job rows");
    let id = jobs
        .create_job(&leader.db, &batch_spec(1), now())
        .await
        .unwrap();
    assert!(runstate(&leader.db).await.jobs.iter().any(|j| j.id == id));
    tokio::time::sleep(Duration::from_millis(500)).await;
    for f in &nodes[1..] {
        assert!(
            runstate(&f.db).await.jobs.iter().all(|j| j.id != id),
            "feature-3 follower must not receive job rows"
        );
    }
    let err = control_for(leader, &st)
        .write(ControlCommand::JobBatch {
            jobs: Vec::new(),
            tasks: Vec::new(),
            restores: Vec::new(),
        })
        .await
        .expect_err("feature-3 leader must reject Job* before append");
    assert!(err.to_string().contains("需要 feature 4"), "{err}");
    assert!(
        job_log_entries(&leader.handle).await.is_empty(),
        "rejected/本地 Job* 不得进入 raft_log"
    );
}

/// 滚动升级窗口：集群先在 feature 3 组成（v1.22 状态），leader 升级到 4 时旧 peer
/// 仍 advertise 3 → 生效级别 3，Job* 在 append 前被拒；逐个升级/移除旧 voter 后
/// 生效级别才回到 4。旧节点绝不能收到 Job*。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_peer_caps_feature_and_blocks_job_replication() {
    let dir = tempfile::tempdir().unwrap();
    // 1) v1.22 状态：3 个 feature-3 voter。
    let (mut nodes, st) = start_cluster_with(dir.path(), [3, 3, 3]).await;

    // 2) 升级 leader：沿用原 raft/控制库，feature_level=4。harness 的旧 HTTP 监听
    //    无法关闭，重启换端口；leader 出站复制/选举不依赖自身旧地址，语义等价。
    nodes[0].handle.shutdown().await;
    let mut s1 = common::raft_harness::settings(dir.path(), 1, true);
    s1.feature_level = 4;
    let n1 = common::raft_harness::spawn_node_with_pool(s1, Some(nodes[0].db.pool().clone())).await;
    nodes[0].handle = n1.handle;

    // 旧 peer 仍在线；立即让升级后的 node1 竞选，确保 leader 是新二进制。
    let handle1 = Arc::clone(&nodes[0].handle);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if handle1.is_leader() && handle1.effective_feature() == Some(3) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "feature-4 leader with feature-3 peers never converged"
        );
        let _ = handle1.raft().trigger().elect().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // 3) 旧 peer 在线：生效级别=3，Job* 必须在 append 前被拒。
    let leader = &nodes[0];
    let control = control_for(leader, &st);
    let err = control
        .write(ControlCommand::JobBatch {
            jobs: vec![JobRow {
                id: 900,
                ..JobRow::default()
            }],
            tasks: Vec::new(),
            restores: Vec::new(),
        })
        .await
        .expect_err("JobBatch must be rejected while a feature-3 peer is a voter");
    assert!(err.to_string().contains("需要 feature 4"), "{err}");

    // JobState 同样 fail-closed（不得退回本地直写）。
    let jobs = JobState::new(Some(Arc::clone(&control)));
    assert!(jobs.replicated(), "leader itself advertises feature 4");
    let err = jobs
        .create_job(&leader.db, &batch_spec(1), now())
        .await
        .unwrap_err();
    assert!(
        err.contains("effective feature 3") && err.contains("< 4"),
        "{err}"
    );
    assert!(
        job_log_entries(&leader.handle).await.is_empty(),
        "no Job* entry while capped"
    );
    assert!(runstate(&leader.db).await.jobs.is_empty());
    assert!(
        runstate(&nodes[2].db).await.jobs.is_empty(),
        "old peer must never receive job rows"
    );

    // 4) 升级其余 voter：停掉旧 node2 → 移出成员 → 新库/feature 4 重新入列。
    nodes[1].handle.shutdown().await;
    oss_server::raft::membership::apply_local(
        &leader.handle,
        &oss_server::raft::membership::MembershipOp::RemoveNode { node_id: 2 },
    )
    .await
    .unwrap();
    let mut s2 = common::raft_harness::settings(dir.path(), 2, false);
    s2.db_path = dir.path().join("raft-2-upgraded.db");
    s2.feature_level = 4;
    let db2 = connect(dir.path(), "n2-upgraded").await;
    let n2 = common::raft_harness::spawn_node_with_pool(s2, Some(db2.pool().clone())).await;
    oss_server::raft::membership::promote_voter(&leader.handle, 2, n2.base.clone())
        .await
        .unwrap();
    nodes[1] = Node {
        id: 2,
        handle: n2.handle,
        db: db2,
    };

    // 5) 移除最后的 v1.22 节点；生效级别回到 4 后才允许 Job*。
    let leader = &nodes[0];
    oss_server::raft::membership::apply_local(
        &leader.handle,
        &oss_server::raft::membership::MembershipOp::RemoveNode { node_id: 3 },
    )
    .await
    .unwrap();
    wait_ready(leader).await;
    assert_eq!(leader.handle.effective_feature(), Some(4));
    let id = jobs
        .create_job(&leader.db, &batch_spec(1), now())
        .await
        .unwrap();
    let db = Arc::clone(&nodes[1].db);
    wait_until("job row on upgraded follower", || async {
        runstate(&db).await.jobs.iter().any(|j| j.id == id)
    })
    .await;
    assert!(
        !job_log_entries(&leader.handle).await.is_empty(),
        "accepted JobBatch must be logged"
    );
}

async fn submit(
    control: &ControlPlane,
    jobs: Vec<JobRow>,
    tasks: Vec<JobTaskRow>,
) -> ControlResponse {
    control
        .write(ControlCommand::JobBatch {
            jobs,
            tasks,
            restores: Vec::new(),
        })
        .await
        .expect("job batch must reach apply")
}

/// P1-2/P1-3：apply 的整批只读校验 —— 任何回退行都返回 Conflict 且零写入；
/// 重复/回退的日志事实不会半批落库。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_guards_reject_whole_batch_without_partial_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let control = control_for(leader, &st);
    let jobs = JobState::new(Some(Arc::clone(&control)));
    let id = jobs
        .create_job(&leader.db, &batch_spec(2), now())
        .await
        .unwrap();

    // 合法新行 + 非法旧行同批：整批拒绝，合法行也不得写入。
    let mut bad = runstate(&leader.db).await.jobs[0].clone();
    bad.updated_at -= 1;
    let fresh_id = id + 100;
    let fresh = JobRow {
        id: fresh_id,
        kind: "batch".into(),
        bucket: "src".into(),
        prefix: "p/".into(),
        state: "queued".into(),
        created_at: now(),
        updated_at: now(),
        ..JobRow::default()
    };
    assert_eq!(
        submit(&control, vec![fresh, bad], Vec::new()).await,
        ControlResponse::Conflict
    );
    assert!(
        runstate(&leader.db)
            .await
            .jobs
            .iter()
            .all(|j| j.id != fresh_id),
        "conflict must not half-apply the valid row"
    );
    // 任务引用不存在的作业：整批拒绝。
    let orphan = JobTaskRow {
        job_id: 999_999,
        task_no: 1,
        action: "DeleteObject".into(),
        payload_json: "{}".into(),
        state: "queued".into(),
        next_at: now(),
        ..JobTaskRow::default()
    };
    assert_eq!(
        submit(&control, Vec::new(), vec![orphan]).await,
        ControlResponse::Conflict
    );
    // Restore 行引用不存在的作业：apply 预检同样整批拒绝。
    let orphan_restore = TierRestoreRow {
        bucket: "src".into(),
        key: "p/x".into(),
        version_id: "v9".into(),
        expires_at: 0,
        job_id: Some(999_999),
        state: "pending".into(),
        created_at: now(),
        ..TierRestoreRow::default()
    };
    assert_eq!(
        control
            .write(ControlCommand::JobBatch {
                jobs: Vec::new(),
                tasks: Vec::new(),
                restores: vec![orphan_restore],
            })
            .await
            .expect("restore batch must reach apply"),
        ControlResponse::Conflict
    );
    assert!(
        runstate(&leader.db).await.tier_restores.is_empty(),
        "conflicting restore must not be written"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        runstate(&nodes[1].db)
            .await
            .jobs
            .iter()
            .all(|j| j.id != fresh_id),
        "follower must reach the same rejection (no divergence)"
    );

    let fence = leader.handle.fencing_epoch().unwrap();
    let t = jobs
        .claim_task_fenced(&leader.db, now(), 60, fence)
        .await
        .unwrap()
        .expect("task claimed");
    let cur = runstate(&leader.db).await.jobs[0].clone();
    assert_eq!(cur.state, "running");
    let mut state_back = cur.clone();
    state_back.state = "queued".into();
    state_back.updated_at += 1;
    assert_eq!(
        submit(&control, vec![state_back], Vec::new()).await,
        ControlResponse::Conflict,
        "running -> queued must be rejected"
    );
    let mut counter_back = cur.clone();
    counter_back.updated_at += 1;
    counter_back.scanned = -1;
    assert_eq!(
        submit(&control, vec![counter_back], Vec::new()).await,
        ControlResponse::Conflict,
        "counter regression must be rejected"
    );
    let mut started_clear = cur.clone();
    started_clear.updated_at += 1;
    started_clear.started_at = None;
    assert_eq!(
        submit(&control, vec![started_clear], Vec::new()).await,
        ControlResponse::Conflict,
        "started_at must be immutable once set"
    );
    // P3-2：job 身份字段（bucket/prefix/spec_json）冻结；更新版本号也不得改写。
    let mut identity = cur.clone();
    identity.updated_at += 1;
    identity.bucket = "other".into();
    assert_eq!(
        submit(&control, vec![identity], Vec::new()).await,
        ControlResponse::Conflict,
        "job bucket is identity"
    );
    let mut identity = cur.clone();
    identity.updated_at += 1;
    identity.prefix = "other/".into();
    assert_eq!(
        submit(&control, vec![identity], Vec::new()).await,
        ControlResponse::Conflict,
        "job prefix is identity"
    );
    let mut identity = cur.clone();
    identity.updated_at += 1;
    identity.spec_json = Some("{\"changed\":true}".into());
    assert_eq!(
        submit(&control, vec![identity], Vec::new()).await,
        ControlResponse::Conflict,
        "job spec_json is identity"
    );
    let mut task_back = runstate(&leader.db).await.job_tasks[0].clone();
    task_back.attempt = -1;
    assert_eq!(
        submit(&control, Vec::new(), vec![task_back]).await,
        ControlResponse::Conflict,
        "attempt regression must be rejected"
    );

    jobs.job_set_error(&leader.db, id, "boom", now())
        .await
        .unwrap();
    let cur = runstate(&leader.db).await.jobs[0].clone();
    let mut error_clear = cur.clone();
    error_clear.updated_at += 1;
    error_clear.error = None;
    assert_eq!(
        submit(&control, vec![error_clear], Vec::new()).await,
        ControlResponse::Conflict,
        "error must not be cleared"
    );

    assert_eq!(
        jobs.finish_task_fenced(
            &leader.db,
            t.job_id,
            t.task_no,
            true,
            "Succeeded",
            None,
            now(),
            3,
            fence
        )
        .await
        .unwrap(),
        "done"
    );
    let t2 = jobs
        .claim_task_fenced(&leader.db, now(), 60, fence)
        .await
        .unwrap()
        .expect("second task");
    assert_eq!(
        jobs.finish_task_fenced(
            &leader.db,
            t2.job_id,
            t2.task_no,
            true,
            "Succeeded",
            None,
            now(),
            3,
            fence
        )
        .await
        .unwrap(),
        "done"
    );
    let done = runstate(&leader.db).await.jobs[0].clone();
    assert_eq!(done.state, "done");
    let mut revive = done.clone();
    revive.state = "running".into();
    revive.updated_at += 1;
    assert_eq!(
        submit(&control, vec![revive], Vec::new()).await,
        ControlResponse::Conflict,
        "terminal job must not revive"
    );
    let mut done_task = runstate(&leader.db)
        .await
        .job_tasks
        .iter()
        .find(|x| x.task_no == t.task_no)
        .unwrap()
        .clone();
    done_task.state = "cancelled".into();
    assert_eq!(
        submit(&control, Vec::new(), vec![done_task]).await,
        ControlResponse::Conflict,
        "terminal task must not be rewritten"
    );
    // P3-1：终态行的 action/payload_json/next_at/deadline_at 一并冻结。
    let terminal = runstate(&leader.db)
        .await
        .job_tasks
        .iter()
        .find(|x| x.task_no == t.task_no)
        .unwrap()
        .clone();
    for (why, mutate) in [
        (
            "terminal action frozen",
            (|x: &mut JobTaskRow| x.action = "PutObject".into()) as fn(&mut JobTaskRow),
        ),
        (
            "terminal payload_json frozen",
            (|x: &mut JobTaskRow| x.payload_json = "{}".into()) as fn(&mut JobTaskRow),
        ),
        (
            "terminal next_at frozen",
            (|x: &mut JobTaskRow| x.next_at += 1) as fn(&mut JobTaskRow),
        ),
        (
            "terminal deadline_at frozen",
            (|x: &mut JobTaskRow| x.deadline_at = Some(now())) as fn(&mut JobTaskRow),
        ),
    ] {
        let mut row = terminal.clone();
        mutate(&mut row);
        assert_eq!(
            submit(&control, Vec::new(), vec![row]).await,
            ControlResponse::Conflict,
            "{why}"
        );
    }
    let rs = runstate(&leader.db).await;
    assert!(
        rs.job_tasks
            .iter()
            .filter(|x| x.job_id == id)
            .all(|x| x.state == "done"),
        "rejected terminal rewrite must not change rows"
    );

    // P2：去重键冲突是业务拒绝（Conflict），不是 SQL 唯一约束错误 → 不会 fail-stop。
    let mut dedupe_spec = batch_spec(1);
    dedupe_spec.tasks[0].dedupe_key = Some("dup-key".into());
    let first = jobs
        .create_job(&leader.db, &dedupe_spec, now())
        .await
        .unwrap();
    let before = runstate(&leader.db).await.jobs.len();
    let err = jobs
        .create_job(&leader.db, &dedupe_spec, now())
        .await
        .unwrap_err();
    assert!(err.contains("Conflict"), "{err}");
    let rs = runstate(&leader.db).await;
    assert_eq!(
        rs.jobs.len(),
        before,
        "rejected dedupe job must not leave a row"
    );
    assert!(rs.jobs.iter().any(|j| j.id == first));
    let mut dup_batch = batch_spec(2);
    dup_batch.tasks[0].dedupe_key = Some("dup-batch".into());
    dup_batch.tasks[1].dedupe_key = Some("dup-batch".into());
    let err = jobs
        .create_job(&leader.db, &dup_batch, now())
        .await
        .unwrap_err();
    assert!(err.contains("Conflict"), "{err}");

    // P3-3：job_finish(error=None) 保留已落库错误，不得被 error 冻结整批拒绝。
    let jid = jobs
        .job_start(&leader.db, "lifecycle", "src", "", now())
        .await
        .unwrap();
    jobs.job_set_error(&leader.db, jid, "keep-me", now())
        .await
        .unwrap();
    jobs.job_finish(&leader.db, jid, "error", 0, 0, 0, None, now() + 1)
        .await
        .unwrap();
    let finished = runstate(&leader.db)
        .await
        .jobs
        .iter()
        .find(|j| j.id == jid)
        .cloned()
        .expect("finished job row");
    assert_eq!(finished.state, "error");
    assert_eq!(finished.error.as_deref(), Some("keep-me"));
}

/// P1-1/P2-1：inventory 进度与 Restore 作业都经 `JobState` 进复制日志；Restore 的
/// `tier_restores` pending 行与作业同批复制到 follower（不再是节点本地补写）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inventory_progress_and_restore_use_logged_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let jobs = Arc::new(JobState::new(Some(control_for(leader, &st))));
    let id = jobs
        .create_job(&leader.db, &batch_spec(1), now())
        .await
        .unwrap();
    let t = now();
    jobs.set_progress(&leader.db, id, 5, t).await.unwrap();
    // scanned-1 的负值补偿：progress 允许回退，其余计数仍单调。
    jobs.set_progress(&leader.db, id, -1, t + 1).await.unwrap();
    for f in &nodes[1..] {
        let db = Arc::clone(&f.db);
        wait_until("inventory progress on follower", || async {
            runstate(&db)
                .await
                .jobs
                .iter()
                .any(|j| j.id == id && j.progress == -1)
        })
        .await;
    }

    let sink = SqlRestoreSink::new(
        Arc::clone(&leader.db),
        Arc::clone(&jobs),
        LeaderGate::from_raft(Some(Arc::clone(&leader.handle))),
    );
    let rid = sink
        .enqueue_restore("src", "p/restore", "v1", 1)
        .await
        .unwrap();
    let rs = runstate(&leader.db).await;
    let job = rs
        .jobs
        .iter()
        .find(|j| j.id == rid)
        .expect("restore job row");
    assert_eq!(job.kind, "restore");
    assert!(
        rs.job_tasks
            .iter()
            .any(|t| t.job_id == rid && t.action == "RestoreObject")
    );
    let pending = leader
        .db
        .restore_row("src", "p/restore", "v1")
        .await
        .unwrap()
        .expect("local pending row");
    assert_eq!(pending.state, "pending");
    assert_eq!(pending.job_id, Some(rid));
    for f in &nodes[1..] {
        let db = Arc::clone(&f.db);
        wait_until("restore job + pending row on follower", || async {
            runstate(&db).await.jobs.iter().any(|j| j.id == rid)
                && db
                    .restore_row("src", "p/restore", "v1")
                    .await
                    .unwrap()
                    .is_some_and(|r| r.state == "pending" && r.job_id == Some(rid))
        })
        .await;
    }
}

/// P2-1：入队（pending 行已复制）后、执行前 leader 换届，新 leader 仍能看到该行，
/// 不会出现“缺行 → 该版本永远不会被恢复”的窗口。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_row_survives_leader_change_before_execution() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let jobs = Arc::new(JobState::new(Some(control_for(&nodes[0], &st))));
    let sink = SqlRestoreSink::new(
        Arc::clone(&nodes[0].db),
        Arc::clone(&jobs),
        LeaderGate::from_raft(Some(Arc::clone(&nodes[0].handle))),
    );
    let rid = sink
        .enqueue_restore("src", "p/failover", "v1", 1)
        .await
        .unwrap();
    // 等副本都拿到 pending 行后再杀 leader（模拟“已入队、尚未执行”）。
    for f in &nodes[1..] {
        let db = Arc::clone(&f.db);
        wait_until("pending row on follower before failover", || async {
            db.restore_row("src", "p/failover", "v1")
                .await
                .unwrap()
                .is_some_and(|r| r.state == "pending" && r.job_id == Some(rid))
        })
        .await;
    }
    nodes[0].handle.shutdown().await;
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
    let row = new_leader
        .db
        .restore_row("src", "p/failover", "v1")
        .await
        .unwrap()
        .expect("new leader must already have the replicated pending row");
    assert_eq!(row.state, "pending");
    assert_eq!(row.job_id, Some(rid));
    assert!(
        runstate(&new_leader.db)
            .await
            .jobs
            .iter()
            .any(|j| j.id == rid),
        "restore job row must survive the failover too"
    );
}

/// P1-4：follower 的管理写不得读旧行后转发；必须明确拒绝。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_job_writes_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let leader_jobs = JobState::new(Some(control_for(&nodes[0], &st)));
    let id = leader_jobs
        .create_job(&nodes[0].db, &batch_spec(1), now())
        .await
        .unwrap();
    let follower = &nodes[1];
    let db = Arc::clone(&follower.db);
    wait_until("job row on follower", || async {
        runstate(&db).await.jobs.iter().any(|j| j.id == id)
    })
    .await;
    assert!(!follower.handle.is_leader());
    let jobs = JobState::new(Some(control_for(follower, &st)));
    assert!(jobs.replicated(), "follower is configured feature 4");
    let errors = [
        jobs.create_job(&follower.db, &batch_spec(1), now())
            .await
            .unwrap_err(),
        jobs.job_set_state(&follower.db, id, "cancel", now())
            .await
            .unwrap_err(),
        jobs.job_set_error(&follower.db, id, "x", now())
            .await
            .unwrap_err(),
        jobs.job_heartbeat(&follower.db, id, now())
            .await
            .unwrap_err(),
        jobs.set_progress(&follower.db, id, 7, now())
            .await
            .unwrap_err(),
        jobs.finish_task_fenced(
            &follower.db,
            id,
            1,
            true,
            "Succeeded",
            None,
            now(),
            3,
            FencingEpoch::UNFENCED,
        )
        .await
        .unwrap_err(),
    ];
    for err in errors {
        assert!(err.contains("not leader"), "{err}");
    }
    let rs = runstate(&follower.db).await;
    assert!(
        rs.jobs
            .iter()
            .find(|j| j.id == id)
            .is_some_and(|j| { j.state == "queued" && j.progress == 0 && j.error.is_none() }),
        "follower-local row must not be written"
    );
}

/// P1-3/P1-5：leader 的逻辑版本推进不依赖墙钟；失败重排使用 `Db::retry_backoff_secs`。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_version_bump_and_bounded_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let jobs = JobState::new(Some(control_for(leader, &st)));
    let id = jobs
        .create_job(&leader.db, &batch_spec(2), now())
        .await
        .unwrap();
    let fence = leader.handle.fencing_epoch().unwrap();
    let t = jobs
        .claim_task_fenced(&leader.db, now(), 60, fence)
        .await
        .unwrap()
        .expect("task claimed");

    let cur = runstate(&leader.db).await.jobs[0].updated_at;
    // 同一 now 连续心跳：leader 必须把 updated_at 推成 cur+1、cur+2（逻辑版本）。
    jobs.job_heartbeat(&leader.db, id, cur).await.unwrap();
    let after1 = runstate(&leader.db).await.jobs[0].updated_at;
    assert_eq!(after1, cur + 1);
    jobs.job_heartbeat(&leader.db, id, after1).await.unwrap();
    let after2 = runstate(&leader.db).await.jobs[0].updated_at;
    assert_eq!(after2, after1 + 1);

    assert_eq!(
        jobs.finish_task_fenced(
            &leader.db,
            t.job_id,
            t.task_no,
            false,
            "InternalError",
            None,
            after2,
            3,
            fence
        )
        .await
        .unwrap(),
        "queued"
    );
    let leader_task = runstate(&leader.db)
        .await
        .job_tasks
        .iter()
        .find(|x| x.job_id == id && x.task_no == t.task_no)
        .unwrap()
        .clone();
    assert_eq!(leader_task.attempt, 1);
    let backoff = leader_task.next_at - after2;
    assert!(
        (10..=19).contains(&backoff),
        "attempt-0 backoff {backoff} outside [10,19]"
    );
    for f in &nodes[1..] {
        let db = Arc::clone(&f.db);
        let expected = leader_task.clone();
        wait_until("retry row on follower", || async {
            runstate(&db)
                .await
                .job_tasks
                .iter()
                .any(|x| x.job_id == id && x.task_no == t.task_no && *x == expected)
        })
        .await;
    }
}

#[test]
fn job_batch_respects_command_size_cap() {
    let big = ControlCommand::JobBatch {
        jobs: Vec::new(),
        tasks: vec![JobTaskRow {
            job_id: 1,
            task_no: 1,
            payload_json: "x".repeat(24 * 1024 * 1024 + 1),
            ..JobTaskRow::default()
        }],
        restores: Vec::new(),
    };
    assert!(big.check_size().is_err(), "超过 24 MiB 必须拒绝 append");
    let small = ControlCommand::JobBatch {
        jobs: vec![JobRow {
            id: 1,
            ..JobRow::default()
        }],
        tasks: Vec::new(),
        restores: Vec::new(),
    };
    assert!(small.check_size().is_ok());
}
