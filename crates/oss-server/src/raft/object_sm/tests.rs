//! F1 focused tests: per-entry apply semantics over the object root.

use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership};
use tempfile::TempDir;
use xl_store::control_state::root::Mode;
use xl_store::control_store::iam::IamStore;
use xl_store::store::{Store, StoreConfig};

use super::super::file_snapshot::FileSnapshotStore;
use super::super::runstate::ScheduleLeaseRow;
use super::super::{ControlCommand, ControlResponse, TypeConfig, compat};
use super::ObjectStateMachine;
use crate::control_store_obj::{ObjectControlStore, RootIo, WriteOrigin};
use crate::local_store::LocalStore;

async fn machine(feature_level: u8) -> (TempDir, ObjectStateMachine) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let local = LocalStore::open(dir.path().join("local")).unwrap();
    let control = ObjectControlStore::new(Arc::new(RootIo::new(store))).with_local_store(local);
    let control = Arc::new(control);
    control.bootstrap(Mode::Raft, 100).await.unwrap();
    let snapshots = Arc::new(FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap());
    let sm = ObjectStateMachine::open(control, snapshots, 1 << 20, None, feature_level)
        .await
        .unwrap();
    (dir, sm)
}

fn entry(term: u64, index: u64, payload: EntryPayload<TypeConfig>) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
        payload,
    }
}

#[tokio::test]
async fn business_rejection_advances_applied_and_replay_is_noop() {
    let (_dir, mut sm) = machine(9).await;
    let e = entry(
        1,
        1,
        EntryPayload::Normal(ControlCommand::FeatureProbe { reject: true }),
    );
    let out = sm.apply(vec![e.clone()]).await.unwrap();
    assert_eq!(out, vec![ControlResponse::Invalid]);
    let (applied, _) = sm.applied().await.unwrap();
    assert_eq!(applied.map(|l| (l.leader_id.term, l.index)), Some((1, 1)));
    let generation = sm.store.pin().await.unwrap().version;

    let out = sm.apply(vec![e]).await.unwrap();
    assert_eq!(out, vec![ControlResponse::Ok]);
    assert_eq!(
        sm.store.pin().await.unwrap().version,
        generation,
        "replayed entry must not commit"
    );
}

#[tokio::test]
async fn marker_and_lease_commit_in_one_generation() {
    let (_dir, mut sm) = machine(9).await;
    let before = sm.store.pin().await.unwrap().version;
    let cmd = ControlCommand::ScheduleLeaseAcquire {
        lease: ScheduleLeaseRow {
            term: 1,
            node: 1,
            expires_at: 200,
            acquired_at: 100,
        },
        now: 100,
    };
    let out = sm
        .apply(vec![entry(1, 1, EntryPayload::Normal(cmd))])
        .await
        .unwrap();
    assert_eq!(out, vec![ControlResponse::Ok]);

    let pinned = sm.store.pin().await.unwrap();
    assert_eq!(pinned.version, before + 1, "one root CAS per entry");
    assert_eq!(pinned.root.meta.feature_level, 7);
    assert!(
        pinned
            .root
            .meta
            .markers
            .contains_key(compat::META_RUNSTATE_ACTIVATED)
    );
    assert_eq!(sm.activation_level(), 7);

    let mut txn = sm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .unwrap();
    let rs = super::super::runstate::read_runstate_txn(&mut txn)
        .await
        .unwrap();
    let lease = rs
        .schedule_lease
        .expect("lease row visible on the new root");
    assert_eq!(
        (lease.term, lease.node, lease.expires_at, lease.acquired_at),
        (1, 1, 200, 100)
    );
}

#[tokio::test]
async fn failed_entry_leaves_prior_entries_applied() {
    let (_dir, mut sm) = machine(1).await;
    let create = ControlCommand::UserCreate {
        username: "alice".into(),
        password_hash: "hash".into(),
        status: "active".into(),
        created_at: 1,
    };
    let out = sm
        .apply(vec![
            entry(1, 1, EntryPayload::Normal(create)),
            entry(
                1,
                2,
                EntryPayload::Normal(ControlCommand::FeatureProbe { reject: false }),
            ),
        ])
        .await;
    assert!(
        out.is_err(),
        "level-2 entry above [raft].feature_level = 1 must fail-stop"
    );
    let (applied, _) = sm.applied().await.unwrap();
    assert_eq!(applied.map(|l| l.index), Some(1));
    assert!(sm.store.user("alice").await.unwrap().is_some());
}

#[tokio::test]
async fn membership_entry_commits_metadata_only() {
    let (_dir, mut sm) = machine(9).await;
    let m = Membership::new(vec![BTreeSet::from([1u64])], ());
    let out = sm
        .apply(vec![entry(1, 5, EntryPayload::Membership(m))])
        .await
        .unwrap();
    assert_eq!(out, vec![ControlResponse::Ok]);
    let (applied, stored) = sm.applied().await.unwrap();
    assert_eq!(applied.map(|l| l.index), Some(5));
    assert_eq!(stored.voter_ids().collect::<Vec<_>>(), vec![1]);
}
