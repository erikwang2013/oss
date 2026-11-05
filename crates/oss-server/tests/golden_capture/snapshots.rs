//! OSSCTL1/2/3 + `m8.runstate` export bytes, install into a fresh control
//! state machine, and re-export; export and install must be byte-identical.

use std::io::Cursor;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{SnapshotMeta, StoredMembership};

use oss_server::db::Db;
use oss_server::raft::TypeConfig;
use oss_server::raft::data::{self, ControlData};
use oss_server::raft::runstate::{self, RunState};
use oss_server::raft::store::SqliteStateMachine;

use super::fixtures;
use crate::golden;

pub async fn capture() {
    roundtrip("ossctl1", &fixtures::level_one(), 1, None).await;
    roundtrip("ossctl2", &fixtures::level_two(), 2, None).await;

    // Production at level 3 always writes the `m8.runstate` extension, even
    // for an empty runtime, so the plain OSSCTL3 fixture does too.
    roundtrip(
        "ossctl3",
        &fixtures::level_three(),
        3,
        Some(&RunState::default()),
    )
    .await;
    let rs = fixtures::runstate();
    roundtrip("ossctl3_runstate", &fixtures::level_three(), 3, Some(&rs)).await;

    golden::write(
        "snapshots/runstate_extension.bin",
        &runstate::encode_extension(&rs).unwrap(),
    );
    golden::write_serde(
        "snapshots/runstate_extension.json",
        &runstate::to_snapshot_extension(&rs).unwrap(),
    );
}

async fn roundtrip(label: &str, data: &ControlData, level: u8, rs: Option<&RunState>) {
    let bytes = match rs {
        Some(rs) => runstate::encode_runstate_snapshot(data, rs, level).unwrap(),
        None => data::encode_snapshot_at(data, level).unwrap(),
    };
    golden::write(&format!("snapshots/export_{label}.bin"), &bytes);

    let (_dir, db) = fresh_control().await;
    let mut sm =
        SqliteStateMachine::open_with_pool(db.pool().clone(), 4 * 1024 * 1024, false, None, level)
            .await
            .unwrap();
    let meta = SnapshotMeta {
        last_log_id: None,
        last_membership: StoredMembership::default(),
        snapshot_id: format!("golden-{label}"),
    };
    RaftStateMachine::<TypeConfig>::install_snapshot(
        &mut sm,
        &meta,
        Box::new(Cursor::new(bytes.clone())),
    )
    .await
    .unwrap_or_else(|e| panic!("install {label}: {e}"));

    let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm)
        .await
        .expect("re-export snapshot");
    let reexport = snap.snapshot.get_ref().clone();
    golden::write(&format!("snapshots/install_{label}.bin"), &reexport);
    assert_eq!(
        reexport, bytes,
        "{label}: install then export must be byte-identical"
    );

    if let Some(want) = rs {
        let got = runstate::read_runstate(db.pool()).await.unwrap();
        assert_eq!(&got, want, "{label}: runstate after install");
    }
}

/// Fresh raft-mode control DB with one local admin (preserved across install).
async fn fresh_control() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("control.db").display()
    );
    let db = Db::connect_raft(&url).await.unwrap();
    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
         VALUES (1, 'golden-admin', 'admin-hash', 1, 'active', 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
        .execute(db.pool())
        .await
        .unwrap();
    (dir, db)
}
