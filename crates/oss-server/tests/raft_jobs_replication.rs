//! M8-2：jobs/job_tasks 运行态经 Raft 复制的垂直切片（level 3）。
//!
//! 用 3 voter 而不是 2 voter：2 voter 集群在 leader 死亡后无法再凑多数派，无法验证
//! “新 leader 看到作业行并接着跑”；这是对任务书“2 节点 harness”的有意偏离，已在报告注明。
//! 覆盖：leader 建/领/收 → follower 收敛；旧 term/重复/回退行被 apply 闸拒绝；
//! leader 死亡 → 新 leader 用既有 stale 回收接管；feature<3 保持本地直写；
//! 命令尺寸上限；写出的行在 `m8.runstate` 快照/安装后一致。

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oss_server::db::{Db, JobSpec, NewJobTask};
use oss_server::job_state::JobState;
use oss_server::raft::command::ControlCommand;
use oss_server::raft::control::ControlPlane;
use oss_server::raft::data::ControlData;
use oss_server::raft::runstate::{
    JobRow, JobTaskRow, RunState, decode_runstate_snapshot, encode_runstate_snapshot,
    install_runstate_tx, read_runstate,
};
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

/// 3 voter 集群；`st` 只提供 Store/AP 缓存给 `ControlPlane::raft`（作业路径不碰它们）。
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
        if node.handle.is_leader() && node.handle.effective_feature() == Some(3) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {} never became level-3 leader",
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
    let (nodes, st) = start_cluster(dir.path(), 3).await;
    let leader = &nodes[0];
    wait_ready(leader).await;
    let jobs = JobState::new(Some(control_for(leader, &st)));
    assert!(jobs.replicated(), "level-3 leader must replicate job rows");

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
    control
        .write(ControlCommand::JobBatch {
            jobs: vec![stale_job],
            tasks: vec![stale_task],
        })
        .await
        .expect("stale batch applies as a no-op");
    // 原样重放终态（重复投递）也不得改变任何列。
    control
        .write(ControlCommand::JobBatch {
            jobs: vec![done_job],
            tasks: done_tasks,
        })
        .await
        .expect("duplicate batch is idempotent");
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
    let (nodes, st) = start_cluster(dir.path(), 3).await;
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
    let leader_id = loop {
        let seen = nodes[1].handle.raft().metrics().borrow().current_leader;
        if let Some(l) = seen
            && l != 1
        {
            break l;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no new leader after shutdown"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // 两个存活节点都应有该作业行（新 leader 必须有已提交行）。
    let new_leader = nodes
        .iter()
        .find(|n| n.handle.raft().metrics().borrow().id == leader_id)
        .expect("new leader in cluster");
    // 死掉的 voter 没有当前 term 的握手 → 生效 feature=1，level-3 命令被现有门拒绝。
    // 永久失联必须先移除成员（运维修复步骤）才能继续复制；这是本切片的已知运维约束。
    oss_server::raft::membership::apply_local(
        &new_leader.handle,
        &oss_server::raft::membership::MembershipOp::RemoveNode { node_id: 1 },
    )
    .await
    .unwrap();
    wait_ready(new_leader).await;
    let jobs = JobState::new(Some(control_for(new_leader, &st)));
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
async fn feature_below_three_keeps_local_job_writes() {
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
            "feature<3 follower must not receive job rows"
        );
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
    };
    assert!(big.check_size().is_err(), "超过 24 MiB 必须拒绝 append");
    let small = ControlCommand::JobBatch {
        jobs: vec![JobRow {
            id: 1,
            ..JobRow::default()
        }],
        tasks: Vec::new(),
    };
    assert!(small.check_size().is_ok());
}
