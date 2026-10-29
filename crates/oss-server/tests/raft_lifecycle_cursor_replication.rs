//! M8-2 下一片：lifecycle 扫描游标经 Raft 复制（feature 5）。
//!
//! 3 voter 覆盖游标写入 → follower 收敛；leader 死亡后新 leader 接管继续推进；旧 term
//! 写入与重复写 no-op；apply 预检 Conflict 零写入；feature 4 保持本地直写且命令在
//! append 前被拒；peer advertise 4 的混版窗口 effective 压回 4；follower 读改写
//! fail-closed、幂等清除可转发；24 MiB 命令上限。

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oss_server::db::Db;
use oss_server::job_state::JobState;
use oss_server::raft::command::{ControlCommand, ControlResponse};
use oss_server::raft::control::ControlPlane;
use oss_server::raft::runstate::{LifecycleCursorRow, RunState, read_runstate};
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

async fn bucket_exists(db: &Db) -> bool {
    let hit: Option<i64> = sqlx::query_scalar("SELECT 1 FROM buckets WHERE name = 'src'")
        .fetch_optional(db.pool())
        .await
        .unwrap();
    hit.is_some()
}

/// 节点自己的 raft 日志中出现过的 Lifecycle* 命令。
async fn lifecycle_log_entries(handle: &RaftHandle) -> Vec<String> {
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
        .filter(|e| e.contains("LifecycleCursor"))
        .collect()
}

async fn create_bucket(control: &ControlPlane) {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_cursor_replicates_and_old_term_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let control = control_for(leader, &st);
    create_bucket(&control).await;
    let fence = leader.handle.fencing_epoch().unwrap();
    let jobs = JobState::new(Some(Arc::clone(&control)));
    assert!(jobs.lifecycle_replicated());

    jobs.lifecycle_cursor_set(&leader.db, "src", "r1", "current", Some("k1"), now(), fence)
        .await
        .unwrap();
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("cursor on every voter", || async {
            runstate(&db)
                .await
                .lifecycle_cursors
                .iter()
                .any(|c| c.rule_id == "r1" && c.cursor == "k1")
        })
        .await;
    }

    // 旧 term 的迟到写入：no-op，不 append。
    let log_before = lifecycle_log_entries(&leader.handle).await.len();
    let old = FencingEpoch {
        term: fence.term.saturating_sub(1),
        leader: 0,
    };
    jobs.lifecycle_cursor_set(
        &leader.db,
        "src",
        "r1",
        "current",
        Some("stale"),
        now(),
        old,
    )
    .await
    .unwrap();
    // 同内容同 token 重复写：无逻辑变化，也不 append。
    jobs.lifecycle_cursor_set(&leader.db, "src", "r1", "current", Some("k1"), now(), fence)
        .await
        .unwrap();
    assert_eq!(
        lifecycle_log_entries(&leader.handle).await.len(),
        log_before,
        "old-term and duplicate writes must not append"
    );
    assert!(
        runstate(&leader.db)
            .await
            .lifecycle_cursors
            .iter()
            .any(|c| c.cursor == "k1"),
        "old-term write must not overwrite the watermark"
    );

    jobs.lifecycle_cursor_set(&leader.db, "src", "r1", "current", Some("k2"), now(), fence)
        .await
        .unwrap();
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("advanced cursor on every voter", || async {
            runstate(&db)
                .await
                .lifecycle_cursors
                .iter()
                .any(|c| c.rule_id == "r1" && c.cursor == "k2")
        })
        .await;
    }

    // 阶段扫完的删除同样复制（幂等）。
    for _ in 0..2 {
        jobs.lifecycle_cursor_set(&leader.db, "src", "r1", "current", None, now(), fence)
            .await
            .unwrap();
    }
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("cleared cursor on every voter", || async {
            runstate(&db).await.lifecycle_cursors.is_empty()
        })
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_leader_continues_from_replicated_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let old_leader = &nodes[0];
    let fence = old_leader.handle.fencing_epoch().unwrap();
    let old_term = fence.term;
    let jobs = JobState::new(Some(control_for(old_leader, &st)));
    create_bucket(&control_for(old_leader, &st)).await;
    jobs.lifecycle_cursor_set(
        &old_leader.db,
        "src",
        "r1",
        "current",
        Some("k1"),
        now(),
        fence,
    )
    .await
    .unwrap();
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("cursor before leader death", || async {
            runstate(&db)
                .await
                .lifecycle_cursors
                .iter()
                .any(|c| c.cursor == "k1")
        })
        .await;
    }

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
    let handle = Arc::clone(&new_leader.handle);
    wait_until("effective feature drops after leader death", || async {
        handle.effective_feature().is_some_and(|l| l < 5)
    })
    .await;
    let jobs = JobState::new(Some(control_for(new_leader, &st)));
    wait_until("bucket on new leader", || async {
        bucket_exists(&new_leader.db).await
    })
    .await;
    let err = jobs
        .lifecycle_cursor_set(
            &new_leader.db,
            "src",
            "r1",
            "current",
            Some("early"),
            now(),
            fence,
        )
        .await
        .expect_err("feature-5 cursor write must fail closed while a dead voter remains");
    assert!(err.contains("effective feature"), "{err}");
    assert!(
        runstate(&new_leader.db)
            .await
            .lifecycle_cursors
            .iter()
            .all(|c| c.cursor != "early"),
        "fail-closed write must not create a local-only cursor"
    );

    oss_server::raft::membership::apply_local(
        &new_leader.handle,
        &oss_server::raft::membership::MembershipOp::RemoveNode { node_id: 1 },
    )
    .await
    .unwrap();
    wait_ready(new_leader).await;
    let fence = new_leader.handle.fencing_epoch().unwrap();
    jobs.lifecycle_cursor_set(
        &new_leader.db,
        "src",
        "r1",
        "current",
        Some("k2"),
        now(),
        fence,
    )
    .await
    .unwrap();

    // 旧 leader 的 term 重放：no-op，不 append、不覆盖新进度。
    let stale = FencingEpoch {
        term: old_term,
        leader: 1,
    };
    let before = lifecycle_log_entries(&new_leader.handle).await.len();
    jobs.lifecycle_cursor_set(
        &new_leader.db,
        "src",
        "r1",
        "current",
        Some("late"),
        now(),
        stale,
    )
    .await
    .unwrap();
    assert_eq!(
        lifecycle_log_entries(&new_leader.handle).await.len(),
        before
    );

    let survivor = nodes
        .iter()
        .find(|n| n.id != 1 && n.id != new_leader.id)
        .expect("third node");
    let db = Arc::clone(&survivor.db);
    wait_until("survivor sees new leader's cursor", || async {
        runstate(&db)
            .await
            .lifecycle_cursors
            .iter()
            .any(|c| c.rule_id == "r1" && c.cursor == "k2")
    })
    .await;
}

/// 预检失败 = 零写入 Conflict；重复整行 apply 幂等。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_rejects_stale_fence_and_empty_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let control = control_for(leader, &st);
    create_bucket(&control).await;
    let fence = leader.handle.fencing_epoch().unwrap();
    let jobs = JobState::new(Some(Arc::clone(&control)));
    jobs.lifecycle_cursor_set(&leader.db, "src", "r1", "current", Some("k1"), now(), fence)
        .await
        .unwrap();
    let base = runstate(&leader.db).await.lifecycle_cursors[0].clone();

    let submit = |cursor: LifecycleCursorRow| {
        let control = Arc::clone(&control);
        async move {
            control
                .write(ControlCommand::LifecycleCursorUpsert { cursor })
                .await
                .expect("command must reach apply")
        }
    };

    let mut stale = base.clone();
    stale.cursor = "stale".into();
    assert_eq!(submit(stale).await, ControlResponse::Conflict);
    let mut empty = base.clone();
    empty.cursor = String::new();
    empty.updated_at += 1;
    assert_eq!(submit(empty).await, ControlResponse::Conflict);
    let mut regressed = base.clone();
    regressed.cursor = "regressed".into();
    regressed.updated_at += 1;
    regressed.fence_term -= 1;
    assert_eq!(submit(regressed).await, ControlResponse::Conflict);
    let after = runstate(&leader.db).await.lifecycle_cursors[0].clone();
    assert_eq!(
        after, base,
        "conflicting rows must not half-apply or bump the watermark"
    );
    // 重复整行 apply 是幂等 no-op。
    assert_eq!(submit(base).await, ControlResponse::Ok);
}

/// v1.23.0 等值场景：feature 4 的 lifecycle 游标仍是节点本地写，Lifecycle* 必须被拒。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feature_four_keeps_local_cursor_writes_and_rejects_commands() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 4).await;
    common::raft_harness::wait_leader(&nodes[0].handle).await;
    let jobs = JobState::new(Some(control_for(&nodes[0], &st)));
    assert!(
        !jobs.lifecycle_replicated(),
        "feature 4 must not replicate lifecycle cursors"
    );
    jobs.lifecycle_cursor_set(
        &nodes[0].db,
        "src",
        "r1",
        "current",
        Some("local"),
        now(),
        FencingEpoch::UNFENCED,
    )
    .await
    .unwrap();
    assert!(
        runstate(&nodes[0].db)
            .await
            .lifecycle_cursors
            .iter()
            .any(|c| c.cursor == "local")
    );

    let cmd = ControlCommand::LifecycleCursorUpsert {
        cursor: LifecycleCursorRow {
            bucket: "src".into(),
            rule_id: "r1".into(),
            stage: "current".into(),
            cursor: "cmd".into(),
            updated_at: 2,
            ..LifecycleCursorRow::default()
        },
    };
    let err = control_for(&nodes[0], &st)
        .write(cmd)
        .await
        .expect_err("feature 4 must reject Lifecycle* before append");
    assert!(err.to_string().contains("feature"), "{err}");
    assert!(
        lifecycle_log_entries(&nodes[0].handle).await.is_empty(),
        "rejected command must not enter the raft log"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    for follower in &nodes[1..] {
        assert!(
            runstate(&follower.db).await.lifecycle_cursors.is_empty(),
            "feature<5 follower must not receive lifecycle cursors"
        );
    }
}

/// v1.23.0 advertise 4 的混版窗口：effective 压回 4，Lifecycle* 在 append 前拒绝。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_advertising_four_blocks_lifecycle_commands() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    create_bucket(&control_for(leader, &st)).await;
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
    let err = jobs
        .lifecycle_cursor_set(
            &leader.db,
            "src",
            "r1",
            "current",
            Some("blocked"),
            now(),
            FencingEpoch::UNFENCED,
        )
        .await
        .expect_err("effective 4 must fail closed");
    assert!(err.contains("effective feature 4 < 5"), "{err}");
    let cmd = ControlCommand::LifecycleCursorUpsert {
        cursor: LifecycleCursorRow {
            bucket: "src".into(),
            rule_id: "r1".into(),
            stage: "current".into(),
            cursor: "blocked".into(),
            updated_at: 2,
            ..LifecycleCursorRow::default()
        },
    };
    let err = control_for(leader, &st)
        .write(cmd)
        .await
        .expect_err("feature-5 command must be rejected before append");
    assert!(err.to_string().contains("feature"), "{err}");
    assert!(
        lifecycle_log_entries(&leader.handle).await.is_empty(),
        "old binary would fail to decode Lifecycle*; nothing may be appended"
    );
    assert!(runstate(&leader.db).await.lifecycle_cursors.is_empty());
}

/// follower 的游标 upsert fail-closed（不得读旧行转发）；幂等清除可转发到 leader。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_rejects_upsert_and_forwards_clear() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 5).await;
    wait_ready(&nodes[0]).await;
    let leader = &nodes[0];
    let fence = leader.handle.fencing_epoch().unwrap();
    let jobs = JobState::new(Some(control_for(leader, &st)));
    create_bucket(&control_for(leader, &st)).await;
    jobs.lifecycle_cursor_set(&leader.db, "src", "r1", "current", Some("k1"), now(), fence)
        .await
        .unwrap();

    let follower = &nodes[1];
    assert!(!follower.handle.is_leader());
    let follower_jobs = JobState::new(Some(control_for(follower, &st)));
    let err = follower_jobs
        .lifecycle_cursor_set(
            &follower.db,
            "src",
            "r1",
            "current",
            Some("follow"),
            now(),
            fence,
        )
        .await
        .expect_err("follower must not build a row from stale local state");
    assert!(err.contains("not leader"), "{err}");
    for node in &nodes {
        assert!(
            runstate(&node.db)
                .await
                .lifecycle_cursors
                .iter()
                .all(|c| c.cursor != "follow"),
            "follower must not write a local-only cursor"
        );
    }

    follower_jobs
        .lifecycle_cursor_set(&follower.db, "src", "r1", "current", None, now(), fence)
        .await
        .expect("idempotent clear forwards to the leader");
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("clear replicated to every voter", || async {
            runstate(&db).await.lifecycle_cursors.is_empty()
        })
        .await;
    }
}

#[test]
fn lifecycle_cursor_command_respects_command_size_cap() {
    let big = ControlCommand::LifecycleCursorUpsert {
        cursor: LifecycleCursorRow {
            bucket: "b".into(),
            rule_id: "r".into(),
            stage: "current".into(),
            cursor: "x".repeat(24 * 1024 * 1024 + 1),
            ..LifecycleCursorRow::default()
        },
    };
    assert!(big.check_size().is_err(), "超过 24 MiB 必须拒绝 append");
    let small = ControlCommand::LifecycleCursorUpsert {
        cursor: LifecycleCursorRow {
            bucket: "b".into(),
            rule_id: "r".into(),
            stage: "current".into(),
            cursor: "k".into(),
            ..LifecycleCursorRow::default()
        },
    };
    assert!(small.check_size().is_ok());
}
