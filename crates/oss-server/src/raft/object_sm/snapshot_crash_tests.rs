//! F3: object snapshot install crash matrix at the root-CAS boundary.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use openraft::storage::RaftStateMachine;
use openraft::{BasicNode, CommittedLeaderId, LogId, Membership, StoredMembership};
use tempfile::TempDir;
use xl_store::control_state::root::Mode;
use xl_store::control_store::iam::{IamStore, IamTxn};
use xl_store::store::{Store, StoreConfig};

use super::super::TypeConfig;
use super::super::data;
use super::ObjectStateMachine;
use super::snapshot_tests::{build, golden, install, meta, seed_admin};
use crate::control_store_obj::{
    CRASH_AFTER_ROOT_CAS, CRASH_BEFORE_ROOT_CAS, CrashIo, ObjectControlStore, WriteOrigin,
};
use crate::local_store::LocalStore;

fn membership() -> StoredMembership<u64, BasicNode> {
    StoredMembership::new(None, Membership::new(vec![BTreeSet::from([1u64])], ()))
}

async fn crash_machine(
    feature_level: u8,
    max: u64,
) -> (TempDir, ObjectStateMachine, Arc<AtomicU8>) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let local = LocalStore::open(dir.path().join("local")).unwrap();
    let (root, crash) = CrashIo::root_io(store);
    let control = ObjectControlStore::new(Arc::new(root)).with_local_store(local);
    let control = Arc::new(control);
    control.bootstrap(Mode::Raft, 100).await.unwrap();
    let snapshots = Arc::new(
        crate::raft::file_snapshot::FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap(),
    );
    let sm = ObjectStateMachine::open(control, snapshots, max, None, feature_level)
        .await
        .unwrap();
    (dir, sm, crash)
}

async fn seed_old(sm: &ObjectStateMachine) {
    let mut txn = sm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .unwrap();
    txn.user_create_default_status("old-user", "h", false, 5)
        .await
        .unwrap();
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn crash_before_root_cas_keeps_old_root_and_retry_succeeds() {
    let (_dir, mut sm, crash) = crash_machine(3, 4 << 20).await;
    seed_admin(&sm).await;
    seed_old(&sm).await;
    let before = sm.store.pin().await.unwrap().version;
    let bytes = golden::load("snapshots/export_ossctl3.bin");
    let snap_user = data::decode_snapshot(&bytes).unwrap().data.users[0]
        .username
        .clone();
    let log_id = LogId::new(CommittedLeaderId::new(9, 1), 77);
    let snap_meta = meta(Some(log_id), membership());

    crash.store(CRASH_BEFORE_ROOT_CAS, Ordering::SeqCst);
    assert!(install(&mut sm, &snap_meta, &bytes).await.is_err());
    crash.store(0, Ordering::SeqCst);

    let pinned = sm.store.pin().await.unwrap();
    assert_eq!(
        pinned.version, before,
        "pre-CAS crash must not move the root"
    );
    assert!(sm.store.user("old-user").await.unwrap().is_some());
    assert!(sm.store.user(&snap_user).await.unwrap().is_none());
    assert_eq!(sm.applied().await.unwrap().0, None);

    install(&mut sm, &snap_meta, &bytes).await.unwrap();
    assert!(sm.store.user("old-user").await.unwrap().is_none());
    assert!(sm.store.user(&snap_user).await.unwrap().is_some());
    assert_eq!(sm.applied().await.unwrap().0, Some(log_id));
}

#[tokio::test]
async fn crash_after_root_cas_commits_new_state_and_reinstall_is_noop() {
    let (_dir, mut sm, crash) = crash_machine(3, 4 << 20).await;
    seed_admin(&sm).await;
    seed_old(&sm).await;
    let bytes = golden::load("snapshots/export_ossctl3_runstate.bin");
    let snap_user = data::decode_snapshot(&bytes).unwrap().data.users[0]
        .username
        .clone();
    let log_id = LogId::new(CommittedLeaderId::new(9, 2), 88);
    let snap_meta = meta(Some(log_id), membership());

    crash.store(CRASH_AFTER_ROOT_CAS, Ordering::SeqCst);
    assert!(install(&mut sm, &snap_meta, &bytes).await.is_err());
    crash.store(0, Ordering::SeqCst);

    assert_eq!(sm.applied().await.unwrap().0, Some(log_id));
    assert_eq!(
        sm.applied()
            .await
            .unwrap()
            .1
            .voter_ids()
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(sm.store.user("old-user").await.unwrap().is_none());
    assert!(sm.store.user(&snap_user).await.unwrap().is_some());
    assert!(sm.store.user("golden-admin").await.unwrap().is_some());
    let committed = sm.store.pin().await.unwrap().version;

    // The snapshot's applied id already committed: replay must not CAS again.
    install(&mut sm, &snap_meta, &bytes).await.unwrap();
    assert_eq!(sm.store.pin().await.unwrap().version, committed);
    let snap = build(&mut sm).await;
    assert_eq!(snap.meta.last_log_id, Some(log_id));
    assert_eq!(snap.meta.last_membership, membership());
    golden::assert_bytes(
        "snapshots/export_ossctl3_runstate.bin",
        snap.snapshot.get_ref(),
    );
    let current = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
        .await
        .unwrap()
        .expect("snapshot remembered after the successful reinstall");
    assert_eq!(current.meta.last_log_id, Some(log_id));
    assert_eq!(current.snapshot.get_ref(), &bytes);
}
