//! M8-2 下一片：inventory 运行态（runs/cursors/incrementals）经 Raft 复制（feature 5）。
//!
//! 3 voter 覆盖 leader 建 run/游标/增量 → follower 收敛；leader 死亡后新 leader 用同一
//! run 行继续（同 run_key 去重，重试不会产生第二个输出槽）；apply 整批预检零写入；
//! feature 4（v1.23.0 等值）保持本地直写且 Inventory* 命令被 append 前拒绝；peer
//! advertise 4 的混版窗口 effective 压回 4；follower 读改写 fail-closed、幂等 clear
//! 可转发；桶删除在同一日志内级联清三表；24 MiB 命令上限。

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

async fn wait_ready(node: &Node) {
    wait_ready_at(node, 5).await;
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
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    let leader = &nodes[0];
    wait_ready(leader).await;
    let jobs = JobState::new(Some(control_for(leader, &st)));
    assert!(jobs.inventory_replicated());
    let fence = leader.handle.fencing_epoch().unwrap();

    let (run_id, job_id) =
        create_run_with_job(&jobs, &leader.db, "inv1", "2026-10-30", fence).await;
    jobs.inventory_run_started(&leader.db, run_id, now(), fence)
        .await
        .unwrap();
    // 旧 term 的重放必须 no-op（不覆盖 job_id、不产生新日志 entry）。
    let log_before = inventory_log_entries(&leader.handle).await.len();
    let old = FencingEpoch {
        term: fence.term.saturating_sub(1),
        leader: 0,
    };
    jobs.inventory_run_set_job(&leader.db, run_id, 999_999, old)
        .await
        .unwrap();
    let rs = runstate(&leader.db).await;
    let current = rs.inventory_runs.iter().find(|r| r.id == run_id).unwrap();
    assert_eq!(
        current.job_id,
        Some(job_id),
        "old-term write must be a no-op"
    );
    assert_eq!(
        inventory_log_entries(&leader.handle).await.len(),
        log_before,
        "old-term no-op must not append"
    );
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
    let (nodes, st) = start_cluster(dir.path(), 5).await;
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
        handle.effective_feature().is_some_and(|l| l < 5)
    })
    .await;
    let jobs = JobState::new(Some(control_for(new_leader, &st)));
    let err = jobs
        .inventory_create_run(&new_leader.db, "src", "inv1", "next", now(), fence)
        .await
        .expect_err("feature-5 inventory write must fail closed while a dead voter remains");
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
    let (nodes, st) = start_cluster(dir.path(), 5).await;
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

/// v1.23.0 等值场景：feature 4 的 inventory 仍是节点本地写，Inventory* 必须被拒。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feature_four_keeps_local_inventory_writes_and_rejects_commands() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    common::raft_harness::wait_leader(&nodes[0].handle).await;
    let jobs = JobState::new(Some(control_for(&nodes[0], &st)));
    assert!(
        !jobs.inventory_replicated(),
        "feature 3/4 must not replicate inventory rows"
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
    // Raft<5 的增量开关必须显式拒绝，而不是节点本地写（P2-2）。
    let err = jobs
        .inventory_incremental_preflight()
        .await
        .expect_err("feature 4 must reject incremental upsert");
    assert!(err.contains("feature_level>=5"), "{err}");

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
        .expect_err("feature 4 must reject Inventory* before append");
    assert!(err.to_string().contains("feature"), "{err}");
    assert!(
        inventory_log_entries(&nodes[0].handle).await.is_empty(),
        "rejected command must not enter the raft log"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    for follower in &nodes[1..] {
        assert!(
            runstate(&follower.db).await.inventory_runs.is_empty(),
            "feature<5 follower must not receive inventory rows"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_inventory_writes_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let follower = &nodes[1];
    assert!(!follower.handle.is_leader());
    let jobs = JobState::new(Some(control_for(follower, &st)));
    assert!(jobs.inventory_replicated());
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

/// v1.23.0 advertise 4 的混版窗口：effective 压回 4，Inventory* 在 append 前拒绝。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_advertising_four_blocks_inventory_commands_until_upgraded() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let term = {
        let metrics = leader.handle.raft().metrics();
        metrics.borrow().current_term
    };
    nodes[2].handle.shutdown().await;
    leader
        .handle
        .peers()
        .record(nodes[2].id, term, oss_server::raft::compat::WIRE_VERSION, 4);
    wait_until("effective feature 4 with an old peer", || async {
        leader.handle.effective_feature() == Some(4)
    })
    .await;

    let jobs = JobState::new(Some(control_for(leader, &st)));
    assert!(
        jobs.inventory_replicated(),
        "local willingness is still 5; effective must block"
    );
    let err = jobs
        .inventory_create_run(
            &leader.db,
            "src",
            "inv1",
            "slot-a",
            now(),
            FencingEpoch::UNFENCED,
        )
        .await
        .expect_err("effective 4 must fail closed");
    assert!(err.contains("effective feature 4 < 5"), "{err}");
    let cmd = ControlCommand::InventoryBatch {
        runs: vec![InventoryRunRow {
            id: 1,
            bucket: "src".into(),
            config_id: "inv1".into(),
            run_key: "slot-a".into(),
            state: "running".into(),
            ..InventoryRunRow::default()
        }],
        cursors: Vec::new(),
        incrementals: Vec::new(),
    };
    let err = control_for(leader, &st)
        .write(cmd)
        .await
        .expect_err("feature-5 command must be rejected before append");
    assert!(err.to_string().contains("feature"), "{err}");
    assert!(
        inventory_log_entries(&leader.handle).await.is_empty(),
        "old binary would fail to decode Inventory*; nothing may be appended"
    );
    assert!(runstate(&leader.db).await.inventory_runs.is_empty());
}

/// 桶删除在 apply 级联三张 inventory 表（服务请求的 follower 不本地直删）；重建同
/// run_key 不冲突、在途 run 不残留。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_delete_cascades_inventory_rows_on_every_replica() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let fence = leader.handle.fencing_epoch().unwrap();
    let jobs = JobState::new(Some(control_for(leader, &st)));
    let (run_id, _) = create_run_with_job(&jobs, &leader.db, "inv1", "slot-a", fence).await;
    jobs.inventory_cursor_set(&leader.db, "src", "inv1", 7, Some("100"), now(), fence)
        .await
        .unwrap();
    jobs.inventory_incremental_set(&leader.db, "src", "inv1", true, now())
        .await
        .unwrap();
    let (inflight, _) = create_run_with_job(&jobs, &leader.db, "inv1", "slot-live", fence).await;

    // 从 follower 发起：client_write 转发 leader 落日志，apply 在每个副本执行同一级联。
    let response = control_for(&nodes[1], &st)
        .write(ControlCommand::BucketDelete { name: "src".into() })
        .await
        .unwrap();
    assert!(matches!(
        response,
        ControlResponse::Ok | ControlResponse::NotFound
    ));
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("inventory cascade on every replica", || async {
            let rs = runstate(&db).await;
            rs.inventory_runs.is_empty()
                && rs.inventory_cursors.is_empty()
                && rs.inventory_incremental.is_empty()
        })
        .await;
    }

    // 在途 run 的结算读不到行 → 幂等 Ok；不得在任一副本复活/卡住。
    let current = nodes.iter().find(|n| n.handle.is_leader()).unwrap();
    let jobs = JobState::new(Some(control_for(current, &st)));
    jobs.inventory_run_finish(&current.db, inflight, "done", Some("m.json"), None, fence)
        .await
        .unwrap();
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("late finish stays gone", || async {
            runstate(&db).await.inventory_runs.is_empty()
        })
        .await;
    }

    // 同 run_key 重建不冲突；id 由 apply 推进的 next_run_id 单调分配，不复用历史 id。
    let fence = current.handle.fencing_epoch().unwrap();
    let recreated = jobs
        .inventory_create_run(&current.db, "src", "inv1", "slot-a", now(), fence)
        .await
        .unwrap()
        .expect("recreate after delete");
    assert!(recreated > run_id && recreated > inflight);
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("recreated run on replica", || async {
            runstate(&db)
                .await
                .inventory_runs
                .iter()
                .any(|r| r.id == recreated && r.run_key == "slot-a")
        })
        .await;
    }
}

/// v1.23.0 等值：feature 4 的 inventory 行是各节点本地行；桶删除仍由同一 apply
/// 级联清掉所有副本的本地行（服务请求的 follower 不本地直删）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feature_four_bucket_delete_cascades_local_inventory_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    wait_ready_at(&nodes[0], 4).await;
    for node in &nodes {
        for sql in [
            "INSERT INTO inventory_runs \
             (bucket, config_id, run_key, started_at, state, created_at) \
             VALUES ('src', 'inv1', 'local', 1, 'running', 1)",
            "INSERT INTO inventory_cursors \
             (bucket, config_id, cursor, config_updated_at, updated_at) \
             VALUES ('src', 'inv1', '100', 1, 1)",
            "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
             VALUES ('src', 'inv1', 1, 1)",
        ] {
            sqlx::query(sql).execute(node.db.pool()).await.unwrap();
        }
    }

    let response = control_for(&nodes[1], &st)
        .write(ControlCommand::BucketDelete { name: "src".into() })
        .await
        .unwrap();
    assert!(matches!(
        response,
        ControlResponse::Ok | ControlResponse::NotFound
    ));
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("feature-4 local inventory cascade", || async {
            let rs = runstate(&db).await;
            rs.inventory_runs.is_empty()
                && rs.inventory_cursors.is_empty()
                && rs.inventory_incremental.is_empty()
        })
        .await;
    }
}

/// follower 的增量开关 upsert 预检 fail-closed（不再本地写），但幂等 clear 可转发。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_forwards_idempotent_clear_but_rejects_incremental_upsert() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let fence = leader.handle.fencing_epoch().unwrap();
    let jobs = JobState::new(Some(control_for(leader, &st)));
    let _ = create_run_with_job(&jobs, &leader.db, "inv1", "slot-a", fence).await;
    jobs.inventory_cursor_set(&leader.db, "src", "inv1", 7, Some("100"), now(), fence)
        .await
        .unwrap();
    jobs.inventory_incremental_set(&leader.db, "src", "inv1", true, now())
        .await
        .unwrap();

    let follower = &nodes[1];
    let follower_jobs = JobState::new(Some(control_for(follower, &st)));
    let err = follower_jobs
        .inventory_incremental_preflight()
        .await
        .expect_err("follower must reject the API upsert before writing config");
    assert!(err.contains("not leader"), "{err}");
    let err = follower_jobs
        .inventory_incremental_set(&follower.db, "src", "inv1", true, now())
        .await
        .expect_err("follower must not write the switch locally");
    assert!(err.contains("not leader"), "{err}");

    follower_jobs
        .inventory_incremental_set(&follower.db, "src", "inv1", false, now())
        .await
        .expect("idempotent clear forwards to the leader");
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("clear replicated to every replica", || async {
            let rs = runstate(&db).await;
            rs.inventory_cursors.is_empty() && rs.inventory_incremental.is_empty()
        })
        .await;
    }
}

/// 同批不同 id 的相同自然键必须在预检期零写 Conflict（而非 SQL 不变量 fail-stop）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_natural_key_in_batch_is_zero_write_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let control = control_for(&nodes[0], &st);
    let row = |id: i64| InventoryRunRow {
        id,
        bucket: "src".into(),
        config_id: "inv1".into(),
        run_key: "dup".into(),
        state: "running".into(),
        created_at: now(),
        ..InventoryRunRow::default()
    };
    assert_eq!(
        submit(&control, vec![row(900), row(901)], Vec::new(), Vec::new()).await,
        ControlResponse::Conflict
    );
    assert!(
        runstate(&nodes[0].db)
            .await
            .inventory_runs
            .iter()
            .all(|r| r.id != 900 && r.id != 901),
        "conflict must be zero-write"
    );
}

/// 配置删除/更新只清游标与开关；运行历史行保留为孤儿（读路径按配置版本过滤），
/// 直到桶删除级联。文档化该语义并验证不会误删日志事实。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_delete_leaves_orphan_runs_until_bucket_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let fence = leader.handle.fencing_epoch().unwrap();
    let control = control_for(leader, &st);
    assert_eq!(
        control
            .write(ControlCommand::BucketCreate {
                name: "src".into(),
                owner: None,
                created_at: now(),
                replication_mode: "async".into(),
                versioning: "Off".into(),
                object_lock: false,
                object_lock_default_mode: None,
                object_lock_default_days: None,
                default_encryption: None,
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    // follower 发起配置 upsert：client_write 转发 leader 落日志，各副本 apply。
    assert_eq!(
        control_for(&nodes[1], &st)
            .write(ControlCommand::InventoryPut {
                bucket: "src".into(),
                id: "inv1".into(),
                owner: None,
                config_json: "{}".into(),
                updated_at: now(),
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    let leader_db = Arc::clone(&leader.db);
    wait_until("follower config upsert on leader", || async {
        leader_db.inventory_config_count("src").await.unwrap() == 1
    })
    .await;
    let jobs = JobState::new(Some(Arc::clone(&control)));
    let (run_id, job_id) = create_run_with_job(&jobs, &leader.db, "inv1", "slot-a", fence).await;
    jobs.inventory_cursor_set(&leader.db, "src", "inv1", 7, Some("100"), now(), fence)
        .await
        .unwrap();
    jobs.inventory_incremental_set(&leader.db, "src", "inv1", true, now())
        .await
        .unwrap();

    assert_eq!(
        control
            .write(ControlCommand::InventoryDelete {
                bucket: "src".into(),
                id: "inv1".into(),
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("config delete on every replica", || async {
            let rs = runstate(&db).await;
            rs.inventory_cursors.is_empty()
                && rs.inventory_incremental.is_empty()
                && rs
                    .inventory_runs
                    .iter()
                    .any(|r| r.id == run_id && r.job_id == Some(job_id))
        })
        .await;
    }

    assert_eq!(
        control
            .write(ControlCommand::BucketDelete { name: "src".into() })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("bucket delete removes orphan runs", || async {
            runstate(&db).await.inventory_runs.is_empty()
        })
        .await;
    }
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
