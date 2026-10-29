//! M8-2 下一片：inventory 运行态（runs/cursors/incrementals）经 Raft 复制（feature 4）。
//!
//! 3 voter 覆盖 leader 建 run/游标/增量 → follower 收敛；leader 死亡后新 leader 用同一
//! run 行继续（同 run_key 去重，重试不会产生第二个输出槽）；apply 整批预检零写入；
//! feature≤3 保持本地直写且 Inventory* 命令被 append 前拒绝；follower 读改写 fail-closed；
//! 24 MiB 命令上限。

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oss_server::db::{Db, JobSpec, NewJobTask};
use oss_server::job_state::JobState;
use oss_server::raft::command::{ControlCommand, ControlResponse};
use oss_server::raft::control::ControlPlane;
use oss_server::raft::runstate::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, RunState, read_runstate,
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

/// 节点自己的 raft 日志中出现过的 Inventory* 命令。
async fn inventory_log_entries(handle: &RaftHandle) -> Vec<String> {
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
        .filter(|e| e.contains("InventoryBatch") || e.contains("InventoryClear"))
        .collect()
}

fn inventory_spec(config_id: &str) -> JobSpec {
    JobSpec {
        kind: "inventory".into(),
        bucket: "src".into(),
        prefix: config_id.into(),
        requested_by: Some(1),
        tasks: vec![NewJobTask {
            action: "Inventory".into(),
            payload: format!(r#"{{"bucket":"src","config_id":"{config_id}"}}"#),
            dedupe_key: None,
        }],
        ..JobSpec::default()
    }
}

/// 建 run + 作业并回填 job_id（生产入队路径的三条日志事实）。
async fn create_run_with_job(
    jobs: &JobState,
    db: &Db,
    config_id: &str,
    run_key: &str,
    fence: FencingEpoch,
) -> (i64, i64) {
    let run_id = jobs
        .inventory_create_run(db, "src", config_id, run_key, now(), fence)
        .await
        .unwrap()
        .expect("new run slot");
    let job_id = jobs
        .create_job(db, &inventory_spec(config_id), now())
        .await
        .unwrap();
    jobs.inventory_run_set_job(db, run_id, job_id, fence)
        .await
        .unwrap();
    (run_id, job_id)
}

async fn submit(
    control: &ControlPlane,
    runs: Vec<InventoryRunRow>,
    cursors: Vec<InventoryCursorRow>,
    incrementals: Vec<InventoryIncrementalRow>,
) -> ControlResponse {
    control
        .write(ControlCommand::InventoryBatch {
            runs,
            cursors,
            incrementals,
        })
        .await
        .expect("inventory batch must reach apply")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inventory_run_state_replicates_to_followers() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    let leader = &nodes[0];
    wait_ready(leader).await;
    let jobs = JobState::new(Some(control_for(leader, &st)));
    assert!(jobs.replicated());
    let fence = leader.handle.fencing_epoch().unwrap();

    let (run_id, job_id) =
        create_run_with_job(&jobs, &leader.db, "inv1", "2026-10-30", fence).await;
    jobs.inventory_run_started(&leader.db, run_id, now(), fence)
        .await
        .unwrap();
    jobs.inventory_cursor_set(&leader.db, "src", "inv1", 7, Some("100"), now(), fence)
        .await
        .unwrap();
    jobs.inventory_incremental_set(&leader.db, "src", "inv1", true, now())
        .await
        .unwrap();
    jobs.inventory_run_finish(
        &leader.db,
        run_id,
        "done",
        Some("dest/src/inv1/2026-10-30/manifest.json"),
        None,
        fence,
    )
    .await
    .unwrap();

    for follower in &nodes[1..] {
        let db = Arc::clone(&follower.db);
        wait_until("inventory rows on follower", || async {
            let rs = runstate(&db).await;
            rs.inventory_runs.iter().any(|r| {
                r.id == run_id
                    && r.job_id == Some(job_id)
                    && r.state == "done"
                    && r.manifest_key.is_some()
            }) && rs
                .inventory_cursors
                .iter()
                .any(|c| c.bucket == "src" && c.config_id == "inv1" && c.cursor == "100")
                && rs
                    .inventory_incremental
                    .iter()
                    .any(|i| i.bucket == "src" && i.config_id == "inv1" && i.enabled)
        })
        .await;
    }
}

/// leader 死亡 → 移除死 voter → 新 leader 用同一 run 行继续；同 run_key 去重，
/// 重试只会写同一输出前缀，新 run_key 才分配新 run id。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_leader_continues_same_run_without_duplicate_output() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let old = JobState::new(Some(control_for(&nodes[0], &st)));
    let fence = nodes[0].handle.fencing_epoch().unwrap();
    let (run_id, job_id) =
        create_run_with_job(&old, &nodes[0].db, "inv1", "2026-10-30", fence).await;
    old.inventory_run_started(&nodes[0].db, run_id, now(), fence)
        .await
        .unwrap();

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
    let handle = Arc::clone(&new_leader.handle);
    wait_until("effective feature drops after leader death", || async {
        handle.effective_feature().is_some_and(|l| l < 4)
    })
    .await;
    let jobs = JobState::new(Some(control_for(new_leader, &st)));
    let err = jobs
        .inventory_create_run(&new_leader.db, "src", "inv1", "next", now(), fence)
        .await
        .expect_err("feature-4 inventory write must fail closed while a dead voter remains");
    assert!(err.contains("effective feature"), "{err}");
    assert!(
        runstate(&new_leader.db)
            .await
            .inventory_runs
            .iter()
            .all(|r| r.run_key != "next"),
        "fail-closed write must not create a local-only run row"
    );

    oss_server::raft::membership::apply_local(
        &new_leader.handle,
        &oss_server::raft::membership::MembershipOp::RemoveNode { node_id: 1 },
    )
    .await
    .unwrap();
    wait_ready(new_leader).await;
    wait_until("run row on new leader", || async {
        runstate(&new_leader.db)
            .await
            .inventory_runs
            .iter()
            .any(|r| r.id == run_id && r.job_id == Some(job_id))
    })
    .await;

    let fence = new_leader.handle.fencing_epoch().unwrap();
    jobs.inventory_run_started(&new_leader.db, run_id, now(), fence)
        .await
        .unwrap();
    jobs.inventory_cursor_set(&new_leader.db, "src", "inv1", 7, Some("200"), now(), fence)
        .await
        .unwrap();
    jobs.inventory_run_finish(
        &new_leader.db,
        run_id,
        "done",
        Some("dest/src/inv1/2026-10-30/manifest.json"),
        None,
        fence,
    )
    .await
    .unwrap();

    // 重试同一槽：run 行已存在 → None（复用同一 run id/输出前缀，不产生第二个输出槽）。
    assert_eq!(
        jobs.inventory_create_run(&new_leader.db, "src", "inv1", "2026-10-30", now(), fence)
            .await
            .unwrap(),
        None
    );
    // 新槽分配新 run id；路径前缀按 run_key 隔离。
    let next = jobs
        .inventory_create_run(&new_leader.db, "src", "inv1", "2026-10-31", now(), fence)
        .await
        .unwrap()
        .expect("new run slot");
    assert!(next > run_id);
    let survivor = nodes
        .iter()
        .find(|n| n.id != 1 && n.id != new_leader.id)
        .expect("third node");
    let db = Arc::clone(&survivor.db);
    wait_until("final run state on survivor", || async {
        let rs = runstate(&db).await;
        rs.inventory_runs
            .iter()
            .any(|r| r.id == run_id && r.state == "done")
            && rs
                .inventory_cursors
                .iter()
                .any(|c| c.config_id == "inv1" && c.cursor == "200")
            && rs.inventory_runs.iter().any(|r| r.id == next)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_guards_reject_whole_batch_and_freeze_terminal_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let control = control_for(leader, &st);
    let jobs = JobState::new(Some(Arc::clone(&control)));
    let fence = leader.handle.fencing_epoch().unwrap();
    let (run_id, _) = create_run_with_job(&jobs, &leader.db, "inv1", "slot-a", fence).await;
    jobs.inventory_cursor_set(&leader.db, "src", "inv1", 7, Some("100"), now(), fence)
        .await
        .unwrap();
    jobs.inventory_incremental_set(&leader.db, "src", "inv1", true, now())
        .await
        .unwrap();

    // 合法新 run + 回退游标同批：整批 Conflict，合法行也不落库。
    let fresh = InventoryRunRow {
        id: 999,
        bucket: "src".into(),
        config_id: "inv1".into(),
        run_key: "slot-b".into(),
        state: "running".into(),
        created_at: now(),
        ..InventoryRunRow::default()
    };
    let mut cursor = runstate(&leader.db).await.inventory_cursors[0].clone();
    cursor.updated_at += 1;
    cursor.cursor = "50".into();
    assert_eq!(
        submit(&control, vec![fresh], vec![cursor], Vec::new()).await,
        ControlResponse::Conflict
    );
    assert!(
        runstate(&leader.db)
            .await
            .inventory_runs
            .iter()
            .all(|r| r.id != 999),
        "conflict must not half-apply the valid run"
    );

    // 增量行 updated_at 回退：整批拒绝。
    let mut inc = runstate(&leader.db).await.inventory_incremental[0].clone();
    inc.updated_at -= 1;
    assert_eq!(
        submit(&control, Vec::new(), Vec::new(), vec![inc.clone()]).await,
        ControlResponse::Conflict
    );
    // 重复的整行 apply 是幂等 no-op。
    let run = runstate(&leader.db).await.inventory_runs[0].clone();
    assert_eq!(
        submit(&control, vec![run.clone()], Vec::new(), Vec::new()).await,
        ControlResponse::Ok
    );

    // done 终态冻结：任何改写（含复活为 running）都拒绝。
    jobs.inventory_run_finish(&leader.db, run_id, "done", Some("m.json"), None, fence)
        .await
        .unwrap();
    let mut revived = runstate(&leader.db).await.inventory_runs[0].clone();
    assert_eq!(revived.state, "done");
    revived.state = "running".into();
    revived.fence_term += 1;
    assert_eq!(
        submit(&control, vec![revived], Vec::new(), Vec::new()).await,
        ControlResponse::Conflict
    );
    // 重复 done 行（逐字段相同）仍是 no-op。
    let done = runstate(&leader.db).await.inventory_runs[0].clone();
    assert_eq!(
        submit(&control, vec![done], Vec::new(), Vec::new()).await,
        ControlResponse::Ok
    );

    // 关闭增量：重复 clear 幂等，游标与开关都清掉。
    for _ in 0..2 {
        assert_eq!(
            control
                .write(ControlCommand::InventoryClear {
                    bucket: "src".into(),
                    config_id: "inv1".into(),
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );
    }
    let rs = runstate(&leader.db).await;
    assert!(rs.inventory_cursors.is_empty() && rs.inventory_incremental.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feature_three_keeps_local_inventory_writes_and_rejects_commands() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 3).await;
    common::raft_harness::wait_leader(&nodes[0].handle).await;
    let jobs = JobState::new(Some(control_for(&nodes[0], &st)));
    assert!(
        !jobs.replicated(),
        "feature 3 must not replicate inventory rows"
    );
    let run_id = jobs
        .inventory_create_run(
            &nodes[0].db,
            "src",
            "inv1",
            "slot-a",
            now(),
            FencingEpoch::UNFENCED,
        )
        .await
        .unwrap()
        .expect("local run row");
    assert!(
        runstate(&nodes[0].db)
            .await
            .inventory_runs
            .iter()
            .any(|r| r.id == run_id)
    );

    let cmd = ControlCommand::InventoryBatch {
        runs: vec![InventoryRunRow {
            id: 2,
            bucket: "src".into(),
            config_id: "inv1".into(),
            run_key: "slot-b".into(),
            state: "running".into(),
            ..InventoryRunRow::default()
        }],
        cursors: Vec::new(),
        incrementals: Vec::new(),
    };
    let err = control_for(&nodes[0], &st)
        .write(cmd)
        .await
        .expect_err("feature 3 must reject Inventory* before append");
    assert!(err.to_string().contains("feature"), "{err}");
    assert!(
        inventory_log_entries(&nodes[0].handle).await.is_empty(),
        "rejected command must not enter the raft log"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    for follower in &nodes[1..] {
        assert!(
            runstate(&follower.db).await.inventory_runs.is_empty(),
            "feature<4 follower must not receive inventory rows"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_inventory_writes_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready(&nodes[0]).await;
    let follower = &nodes[1];
    assert!(!follower.handle.is_leader());
    let jobs = JobState::new(Some(control_for(follower, &st)));
    assert!(jobs.replicated());
    let err = jobs
        .inventory_create_run(
            &follower.db,
            "src",
            "inv1",
            "slot-a",
            now(),
            follower
                .handle
                .fencing_epoch()
                .unwrap_or(FencingEpoch::UNFENCED),
        )
        .await
        .expect_err("follower must not build rows from stale local state");
    assert!(err.contains("not leader"), "{err}");
    assert!(
        runstate(&follower.db).await.inventory_runs.is_empty(),
        "follower must not write locally"
    );
}

#[test]
fn inventory_batch_respects_command_size_cap() {
    let big = ControlCommand::InventoryBatch {
        runs: vec![InventoryRunRow {
            id: 1,
            run_key: "x".repeat(24 * 1024 * 1024 + 1),
            ..InventoryRunRow::default()
        }],
        cursors: Vec::new(),
        incrementals: Vec::new(),
    };
    assert!(big.check_size().is_err(), "超过 24 MiB 必须拒绝 append");
    let small = ControlCommand::InventoryBatch {
        runs: vec![InventoryRunRow {
            id: 1,
            ..InventoryRunRow::default()
        }],
        cursors: Vec::new(),
        incrementals: Vec::new(),
    };
    assert!(small.check_size().is_ok());
}
