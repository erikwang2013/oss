//! F3: object vs SQLite feature-level install accept/refuse matrix.

use std::io::Cursor;

use openraft::StoredMembership;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use tempfile::TempDir;

use super::super::TypeConfig;
use super::super::data;
use super::super::runstate::{self, InventoryRunRow, JobRow, RunState};
use super::super::store::SqliteStateMachine;
use super::ObjectStateMachine;
use super::snapshot_tests::{
    ObjectMachine, build, golden, install, meta, object_machine, seed_admin,
};
use crate::control_store_obj::WriteOrigin;
use crate::db::Db;

struct Pair {
    _odir: TempDir,
    osm: ObjectStateMachine,
    _sdir: TempDir,
    db: Db,
    ssm: SqliteStateMachine,
}

async fn pair(configured: u8, max_bytes: u64) -> Pair {
    let ObjectMachine { _dir, sm } = object_machine(configured, max_bytes).await;
    seed_admin(&sm).await;
    let sdir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        sdir.path().join("control.db").display()
    );
    let db = Db::connect_raft(&url).await.unwrap();
    for sql in [
        "UPDATE raft_apply_ctx SET active = 1 WHERE id = 1",
        "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
         VALUES (1, 'golden-admin', 'admin-hash', 1, 'active', 1)",
        "UPDATE raft_apply_ctx SET active = 0 WHERE id = 1",
    ] {
        sqlx::query(sql).execute(db.pool()).await.unwrap();
    }
    let ssm =
        SqliteStateMachine::open_with_pool(db.pool().clone(), max_bytes, false, None, configured)
            .await
            .unwrap();
    Pair {
        _odir: _dir,
        osm: sm,
        _sdir: sdir,
        db,
        ssm,
    }
}

impl Pair {
    async fn install(&mut self, bytes: &[u8]) -> (bool, bool) {
        let snap_meta = meta(None, StoredMembership::default());
        let object = install(&mut self.osm, &snap_meta, bytes).await.is_ok();
        let sqlite = RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut self.ssm,
            &snap_meta,
            Box::new(Cursor::new(bytes.to_vec())),
        )
        .await
        .is_ok();
        (object, sqlite)
    }

    async fn install_ok(&mut self, bytes: &[u8]) {
        assert_eq!(self.install(bytes).await, (true, true), "both must accept");
    }

    async fn install_accept(&mut self, bytes: &[u8]) {
        self.install_ok(bytes).await;
        let object = build(&mut self.osm).await.snapshot.get_ref().clone();
        let sqlite = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut self.ssm)
            .await
            .unwrap()
            .snapshot
            .get_ref()
            .clone();
        assert_eq!(object, bytes, "object re-export");
        assert_eq!(sqlite, bytes, "sqlite re-export");
    }

    async fn install_refuse(&mut self, bytes: &[u8]) {
        assert_eq!(
            self.install(bytes).await,
            (false, false),
            "both must refuse"
        );
    }
}

#[tokio::test]
async fn accepts_every_exported_golden() {
    for label in ["ossctl1", "ossctl2", "ossctl3", "ossctl3_runstate"] {
        let bytes = golden::load(&format!("snapshots/export_{label}.bin"));
        // The configured level must match the source activation: a level-3
        // snapshot with non-log runstate rows is refused at configured >= 4.
        let level = data::decode_snapshot(&bytes).unwrap().feature_level;
        let mut p = pair(level, 4 << 20).await;
        p.install_accept(&bytes).await;
    }
}

#[tokio::test]
async fn refuses_lower_level_over_activation_and_ledger() {
    let l2 = golden::load("snapshots/export_ossctl2.bin");
    let l3 = golden::load("snapshots/export_ossctl3.bin");

    // Local activation 3 refuses a level-2 install even though no marker rows
    // would be erased by name.
    let mut p = pair(3, 4 << 20).await;
    p.install_ok(&l3).await;
    p.install_refuse(&l2).await;

    // Local level-9 ledger rows refuse any snapshot below 9.
    let mut data9 = data::decode_snapshot(&l3).unwrap().data;
    data9
        .account_usage_ledger
        .push(data::AccountUsageLedgerRow {
            account_id: "123456789012".into(),
            used_bytes: 1,
            object_count: 1,
        });
    let baseline9 = data::encode_snapshot_at(&data9, 9).unwrap();
    let mut p = pair(9, 4 << 20).await;
    p.install_ok(&baseline9).await;
    p.install_refuse(&l3).await;
}

fn jobs() -> RunState {
    RunState {
        jobs: vec![JobRow {
            id: 1,
            kind: "lifecycle".into(),
            bucket: "b".into(),
            state: "running".into(),
            created_at: 1,
            updated_at: 1,
            ..JobRow::default()
        }],
        next_job_id: 2,
        next_run_id: 1,
        ..RunState::default()
    }
}

fn inventory() -> RunState {
    RunState {
        inventory_runs: vec![InventoryRunRow {
            id: 1,
            bucket: "b".into(),
            config_id: "c".into(),
            run_key: "r".into(),
            state: "done".into(),
            ..InventoryRunRow::default()
        }],
        next_job_id: 1,
        next_run_id: 2,
        ..RunState::default()
    }
}

#[tokio::test]
async fn refuses_non_log_runstate_above_the_source_level() {
    let data3 = data::decode_snapshot(&golden::load("snapshots/export_ossctl3.bin"))
        .unwrap()
        .data;
    let with_jobs = runstate::encode_runstate_snapshot(&data3, &jobs(), 3).unwrap();
    let mut p = pair(4, 4 << 20).await;
    p.install_refuse(&with_jobs).await;
    let mut p = pair(3, 4 << 20).await;
    p.install_accept(&with_jobs).await;

    let with_inventory = runstate::encode_runstate_snapshot(&data3, &inventory(), 4).unwrap();
    let mut p = pair(5, 4 << 20).await;
    p.install_refuse(&with_inventory).await;
    let mut p = pair(4, 4 << 20).await;
    p.install_accept(&with_inventory).await;
}

/// A canonical marker on empty account tables is a startup rule, not data: it
/// must not block joining an older cluster.
#[tokio::test]
async fn marker_without_rows_does_not_block_lower_install() {
    let l2 = golden::load("snapshots/export_ossctl2.bin");
    let mut p = pair(2, 4 << 20).await;
    let mut txn = p
        .osm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .unwrap();
    txn.set_marker(crate::db::META_ACCOUNTS_CANONICAL, "done");
    txn.commit().await.unwrap();
    sqlx::query("INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, 'done')")
        .bind(crate::db::META_ACCOUNTS_CANONICAL)
        .execute(p.db.pool())
        .await
        .unwrap();
    p.install_accept(&l2).await;
}

#[tokio::test]
async fn refuses_bad_magic_unknown_extension_and_oversize() {
    let data3 = data::decode_snapshot(&golden::load("snapshots/export_ossctl3.bin"))
        .unwrap()
        .data;
    let mut extensions = data::SnapshotExtensions::new();
    extensions.insert(
        "future.feature".into(),
        data::SnapshotExtension {
            version: 1,
            payload_b64: String::new(),
        },
    );
    let unknown = data::encode_snapshot_with_extensions(&data3, 3, extensions).unwrap();

    let mut p = pair(9, 4 << 20).await;
    p.install_refuse(&unknown).await;
    p.install_refuse(b"not-a-control-snapshot").await;

    let mut tiny = pair(9, 64).await;
    tiny.install_refuse(&golden::load("snapshots/export_ossctl3.bin"))
        .await;
}
