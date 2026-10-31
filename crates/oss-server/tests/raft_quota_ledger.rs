//! feature 9：复制账户用量账本（strict 硬配额的唯一执行依据）。
//!
//! 覆盖 3-voter 精确预留/边界拒绝/释放复制、并发抢边界只有一个成功、
//! 快照携带账本并在新库重开可读、feature 8 集群在 append 前拒绝账本命令。

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

use oss_server::db::Db;
use oss_server::raft::command::{ControlCommand, ControlResponse};
use oss_server::raft::control::{ControlError, ControlPlane};
use oss_server::raft::data::{self, AccountUsageLedgerRow, ControlData};
use oss_server::raft::store::SqliteStateMachine;
use oss_server::raft::{RaftHandle, TypeConfig};
use oss_server::state::AppState;

struct Node {
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
    let mut nodes: Vec<Node> = Vec::new();
    for id in [1u64, 2, 3] {
        let db = connect(dir, &format!("q{id}")).await;
        let mut s = common::raft_harness::settings(dir, id, id == 1);
        s.feature_level = feature_level;
        let n = common::raft_harness::spawn_node_with_pool(s, Some(db.pool().clone())).await;
        if id == 1 {
            common::raft_harness::wait_leader(&n.handle).await;
        } else {
            oss_server::raft::membership::promote_voter(&nodes[0].handle, id, n.base.clone())
                .await
                .unwrap();
        }
        nodes.push(Node {
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
            "node never became feature-{level} leader"
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

/// 节点自身的 raft 日志（settings.db_path，不是控制库）里按子串过滤命令。
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

async fn ledger(db: &Db, account: &str) -> Option<(i64, i64)> {
    sqlx::query_as("SELECT used_bytes, object_count FROM account_usage_ledger WHERE account_id = ?")
        .bind(account)
        .fetch_optional(db.pool())
        .await
        .unwrap()
}

async fn set_limit(node: &Node, account: &str, bytes: Option<i64>, objects: Option<i64>) {
    sqlx::query(
        "INSERT INTO accounts (id, name, status, created_at, updated_at, hard_byte_quota, \
           object_quota) VALUES (?, ?, 'active', 1, 1, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET hard_byte_quota = excluded.hard_byte_quota, \
           object_quota = excluded.object_quota",
    )
    .bind(account)
    .bind(account)
    .bind(bytes)
    .bind(objects)
    .execute(node.db.pool())
    .await
    .unwrap();
}

fn reserve(account: &str, bytes: i64, objects: i64) -> ControlCommand {
    ControlCommand::AccountUsageReserve {
        account: account.into(),
        bytes,
        objects,
        baseline_bytes: 0,
        baseline_objects: 0,
    }
}

fn release(account: &str, bytes: i64, objects: i64) -> ControlCommand {
    ControlCommand::AccountUsageRelease {
        account: account.into(),
        bytes,
        objects,
        baseline_bytes: 0,
        baseline_objects: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserve_limit_release_replicates_to_all_voters() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 9).await;
    wait_ready_at(&nodes[0], 9).await;
    for node in &nodes {
        set_limit(node, "000000000001", Some(10), None).await;
    }
    let control = control_for(&nodes[0], &st);

    assert_eq!(
        control.write(reserve("000000000001", 6, 1)).await.unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        control.write(reserve("000000000001", 5, 1)).await.unwrap(),
        ControlResponse::Invalid,
        "6+5 > 10：apply 内稳定拒绝"
    );
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("ledger replicated", || async {
            ledger(&db, "000000000001").await == Some((6, 1))
        })
        .await;
    }

    assert_eq!(
        control.write(release("000000000001", 6, 1)).await.unwrap(),
        ControlResponse::Ok
    );
    for node in &nodes {
        let db = Arc::clone(&node.db);
        wait_until("release replicated", || async {
            ledger(&db, "000000000001").await == Some((0, 0))
        })
        .await;
    }

    // 恰好上限放行；再 +1 拒绝。
    assert_eq!(
        control.write(reserve("000000000001", 10, 0)).await.unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        control.write(reserve("000000000001", 1, 0)).await.unwrap(),
        ControlResponse::Invalid
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reservations_at_boundary_only_one_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 9).await;
    wait_ready_at(&nodes[0], 9).await;
    for node in &nodes {
        set_limit(node, "000000000002", Some(10), None).await;
    }
    let control = control_for(&nodes[0], &st);

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let c = Arc::clone(&control);
        tasks.push(tokio::spawn(async move {
            c.write(reserve("000000000002", 6, 1)).await
        }));
    }
    let mut ok = 0;
    let mut denied = 0;
    for t in tasks {
        match t.await.unwrap().unwrap() {
            ControlResponse::Ok => ok += 1,
            ControlResponse::Invalid => denied += 1,
            other => panic!("unexpected response {other:?}"),
        }
    }
    assert_eq!((ok, denied), (1, 7), "Raft apply 串行化边界预留");
    assert_eq!(ledger(&nodes[0].db, "000000000002").await, Some((6, 1)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feature8_cluster_rejects_ledger_before_append() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, st) = start_cluster(dir.path(), 8).await;
    wait_ready_at(&nodes[0], 8).await;
    let control = control_for(&nodes[0], &st);
    let err = control
        .write(reserve("000000000003", 1, 1))
        .await
        .unwrap_err();
    assert!(
        matches!(err, ControlError::Unavailable(_)),
        "feature 8 effective 必须在 append 前拒绝：{err:?}"
    );
    assert!(
        log_entries(&nodes[0].handle, "AccountUsageReserve")
            .await
            .is_empty(),
        "rejected command must not be appended"
    );
    assert_eq!(ledger(&nodes[0].db, "000000000003").await, None);
}

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

/// 状态机快照真实路径：apply 账本命令 → OSSCTL3 level 9 快照 → 新库安装 →
/// 重开状态机仍能读到同一账本；低级别快照不得清空本机账本。
#[tokio::test]
async fn snapshot_round_trip_and_reopen_preserve_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = connect(dir.path(), "snap-a").await;
    let mut sm_a = SqliteStateMachine::open_with_pool(db_a.pool().clone(), 1 << 20, false, None, 9)
        .await
        .unwrap();
    let entries = [
        Entry {
            log_id: log_id(1),
            payload: EntryPayload::Normal(reserve("000000000004", 7, 2)),
        },
        Entry {
            log_id: log_id(2),
            payload: EntryPayload::Normal(release("000000000004", 3, 1)),
        },
    ];
    RaftStateMachine::<TypeConfig>::apply(&mut sm_a, entries)
        .await
        .unwrap();
    assert_eq!(ledger(&db_a, "000000000004").await, Some((4, 1)));

    let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm_a)
        .await
        .unwrap();
    let bytes = snap.snapshot.get_ref().clone();
    let decoded = data::decode_snapshot(&bytes).unwrap();
    assert_eq!(decoded.feature_level, 9);
    assert_eq!(
        decoded.data.account_usage_ledger,
        vec![AccountUsageLedgerRow {
            account_id: "000000000004".into(),
            used_bytes: 4,
            object_count: 1,
        }]
    );

    let db_b = connect(dir.path(), "snap-b").await;
    let mut sm_b = SqliteStateMachine::open_with_pool(db_b.pool().clone(), 1 << 20, false, None, 9)
        .await
        .unwrap();
    RaftStateMachine::<TypeConfig>::install_snapshot(
        &mut sm_b,
        &snap.meta,
        Box::new(std::io::Cursor::new(bytes)),
    )
    .await
    .unwrap();
    assert_eq!(ledger(&db_b, "000000000004").await, Some((4, 1)));
    // 重开：快照安装结果已落库（不是仅内存态）。
    drop(sm_b);
    let mut reopened =
        SqliteStateMachine::open_with_pool(db_b.pool().clone(), 1 << 20, false, None, 9)
            .await
            .unwrap();
    let again = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut reopened)
        .await
        .unwrap();
    let decoded = data::decode_snapshot(again.snapshot.get_ref()).unwrap();
    assert_eq!(decoded.feature_level, 9);
    assert_eq!(decoded.data.account_usage_ledger.len(), 1);

    // 低级别快照（不携带账本）安装到已有账本行的库：整表替换会丢失行 → 拒绝。
    let db_c = connect(dir.path(), "snap-c").await;
    sqlx::query(
        "INSERT INTO account_usage_ledger (account_id, used_bytes, object_count) \
         VALUES ('000000000004', 4, 1)",
    )
    .execute(db_c.pool())
    .await
    .unwrap();
    let mut sm_c = SqliteStateMachine::open_with_pool(db_c.pool().clone(), 1 << 20, false, None, 8)
        .await
        .unwrap();
    let level8 = data::encode_snapshot_at(&ControlData::default(), 8).unwrap();
    let meta = &snap.meta;
    let err = RaftStateMachine::<TypeConfig>::install_snapshot(
        &mut sm_c,
        meta,
        Box::new(std::io::Cursor::new(level8)),
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err}").contains("account_usage_ledger"),
        "level-8 快照不得清空账本：{err}"
    );
    assert_eq!(ledger(&db_c, "000000000004").await, Some((4, 1)));
}
