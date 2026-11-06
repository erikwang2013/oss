//! G1 end-to-end tests over the full fixture: dry-run, apply + boot, interrupted
//! apply, idempotent rerun, Raft file preservation, and fail-closed verification.

mod fixture;

use std::sync::Arc;

use openraft::storage::{RaftLogReader, RaftLogStorage};
use openraft::{Entry, Vote};
use xl_store::control_state::root::Mode;
use xl_store::control_store::sites::SiteStore;

use super::source::{self, Source};
use super::{export, object_install, run};
use crate::control_store_obj::{ObjectControlStore, RootIo, WriteOrigin};
use crate::raft::compat::{META_INVENTORY_ACTIVATED, META_RUNSTATE_ACTIVATED};
use fixture::{
    Fixture, checkpoint, fixture, log_id, open_store, raft_checksum, read_control, read_source,
    source_checksum,
};

async fn apply(f: &Fixture) -> super::Report {
    run(&f.control, &f.state, &f.raft, false).await.unwrap()
}

/// Schema-only control DB: no control rows and an unseeded runstate. `marker`
/// sets `schema_meta.raft_feature_level`; `None` leaves it absent (level 1).
async fn empty_fixture(raft: bool, marker: Option<&str>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let control = dir.path().join("oss.db");
    let raft_db = dir.path().join("raft.db");
    let state = dir.path().join("state");
    let control_url = format!("sqlite://{}?mode=rwc", control.display());
    let db = crate::db::Db::connect_site(&control_url, raft, None)
        .await
        .unwrap();
    if let Some(level) = marker {
        db.schema_meta_set(crate::raft::compat::META_FEATURE_LEVEL, level)
            .await
            .unwrap();
    }
    db.pool().close().await;
    drop(db);
    checkpoint(&control_url).await;
    Fixture {
        _dir: dir,
        control,
        raft: raft_db,
        state,
    }
}

#[tokio::test]
async fn dry_run_reports_without_writing() {
    let f = fixture().await;
    let before = source_checksum(&f);
    let report = run(&f.control, &f.state, &f.raft, true).await.unwrap();
    assert_eq!(report.status, "dry-run");
    assert_eq!(report.mode, "raft");
    assert_eq!(report.feature.marker, 9);
    assert_eq!(report.feature.effective, 9);
    assert!(report.object.is_none());
    let local = report.local.as_ref().unwrap();
    assert_eq!(local.uploads, 1);
    assert_eq!(local.audit, 1);
    assert_eq!(local.events, 3);
    assert_eq!(local.repls, 2);
    assert_eq!(local.write_intents, 1);
    assert_eq!(local.delete_intents, 1);
    assert_eq!(local.cursors, 1);
    assert_eq!(local.dead, 1);
    assert_eq!(local.usage, 1);
    assert_eq!(local.instant, 1);
    assert_eq!(report.raft.as_ref().unwrap().entries, 2);
    assert!(report.control_table_counts["users"] >= 3);
    assert!(!f.state.exists(), "dry run must not create the target");
    assert_eq!(source_checksum(&f), before);
}

#[tokio::test]
async fn apply_boots_and_rerun_is_idempotent() {
    let f = fixture().await;
    let before = source_checksum(&f);
    let raft_before = raft_checksum(&f);
    let report = apply(&f).await;
    assert_eq!(report.status, "applied");
    let object = report.object.as_ref().unwrap();
    assert!(object.generation >= 1);
    assert_eq!(report.local.as_ref().unwrap().events, 3);
    assert_eq!(source_checksum(&f), before);
    assert_eq!(raft_checksum(&f), raft_before);

    let source: Source = read_source(&f).await;
    let boot =
        crate::control_backend::ObjectControl::open(open_store(&f.state).await, Mode::Raft, None)
            .await
            .unwrap();
    let site = boot.control.site_identity().await.unwrap().unwrap();
    assert_eq!(
        Some(&site.id),
        source.raw.site_config.as_ref().map(|c| &c.id)
    );
    let pinned = boot.control.pin().await.unwrap();
    assert_eq!(pinned.root.meta.mode, Mode::Raft);
    assert_eq!(pinned.root.meta.feature_level, 9);
    assert_eq!(
        pinned
            .root
            .meta
            .markers
            .get(META_RUNSTATE_ACTIVATED)
            .map(String::as_str),
        Some("1"),
        "源激活事实必须随迁移写入对象根"
    );
    assert_eq!(
        pinned
            .root
            .meta
            .markers
            .get(META_INVENTORY_ACTIVATED)
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        pinned.root.meta.next_site_seq,
        source.raw.changelog.iter().map(|r| r.seq).max().unwrap() + 1
    );
    drop(boot);

    let again = apply(&f).await;
    assert_eq!(again.status, "already-applied");
    assert_eq!(
        again.object.as_ref().unwrap().generation,
        object.generation,
        "rerun must not rewrite the root"
    );
    assert_eq!(source_checksum(&f), before);
}

/// A fresh source has an empty runstate and no activation marker. A fresh Raft
/// source has no account rows either, so its snapshot must stay level 1 instead
/// of being rejected for a level-3 extension. Standalone backfill inserts the
/// system account, so the same empty runstate legitimately reaches level 3.
#[tokio::test]
async fn empty_runstate_source_migrates_and_boots() {
    for (raft, marker, mode, required) in [
        (true, None, Mode::Raft, 1),
        (true, Some("1"), Mode::Raft, 1),
        (false, None, Mode::Standalone, 3),
    ] {
        let f = empty_fixture(raft, marker).await;
        let source = read_source(&f).await;
        assert!(source.runstate.is_empty(), "fixture must be empty");
        assert_eq!(source.feature_marker, 1, "marker {marker:?}");
        assert_eq!(source.mode, mode, "marker {marker:?}");
        assert_eq!(source::required_feature(&source), required);
        assert_eq!(source::effective_feature(&source), required);

        let report = apply(&f).await;
        assert_eq!(report.status, "applied");
        assert_eq!(report.feature.marker, 1);
        assert_eq!(report.feature.required, required);
        assert_eq!(report.feature.effective, required);
        let object = report.object.as_ref().unwrap();
        assert_eq!(object.snapshot_sha256, report.source_snapshot_sha256);
        assert_eq!(object.runstate_sha256, report.source_runstate_sha256);

        let boot =
            crate::control_backend::ObjectControl::open(open_store(&f.state).await, mode, None)
                .await
                .unwrap();
        let pinned = boot.control.pin().await.unwrap();
        assert_eq!(pinned.root.meta.mode, mode);
        assert_eq!(pinned.root.meta.feature_level, u16::from(required));
        drop(boot);

        assert_eq!(apply(&f).await.status, "already-applied");
    }
}

#[tokio::test]
async fn interrupted_before_root_commit_leaves_no_root_and_rerun_works() {
    let f = fixture().await;
    let source = read_source(&f).await;
    let store = open_store(&f.state).await;
    let control = ObjectControlStore::new(Arc::new(RootIo::new(store)))
        .with_local_dir(crate::control_backend::local_state_dir(&f.state))
        .unwrap();
    let err = object_install::install_with(&control, &source, || {
        Err(std::io::Error::other("injected stop").into())
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("injected stop"));
    assert!(
        control.root.load().await.unwrap().is_none(),
        "an interrupted install must not publish a root"
    );
    drop(control);

    let report = apply(&f).await;
    assert_eq!(report.status, "applied");
    assert!(report.object.is_some());
}

#[tokio::test]
async fn raft_files_preserve_vote_committed_log_and_snapshot() {
    let f = fixture().await;
    apply(&f).await;
    let dir = crate::raft::file_log::raft_dir(&f.raft);
    let mut log = crate::raft::file_log::FileLogStore::open_dir(&dir)
        .await
        .unwrap();
    let state = log.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(1)));
    assert_eq!(state.last_log_id, Some(log_id(3)));
    assert_eq!(
        log.read_vote().await.unwrap(),
        Some(Vote::new_committed(2, 1))
    );
    assert_eq!(log.read_committed().await.unwrap(), Some(log_id(2)));
    let entries: Vec<Entry<crate::raft::TypeConfig>> =
        RaftLogReader::try_get_log_entries(&mut log, ..)
            .await
            .unwrap();
    let indices: Vec<u64> = entries.iter().map(|e| e.log_id.index).collect();
    assert_eq!(indices, vec![2, 3]);

    let snapshots = crate::raft::file_snapshot::FileSnapshotStore::open(&f.raft).unwrap();
    let snapshot = snapshots.read("snap-1").await.unwrap().unwrap();
    assert_eq!(snapshot.meta.snapshot_id, "snap-1");
    assert_eq!(snapshot.meta.last_log_id, Some(log_id(2)));
    assert_eq!(snapshot.snapshot.get_ref().as_slice(), b"snapshot-bytes");
}

/// v1.31.0 的已应用根没有激活 marker；重跑 control-migrate 必须在验证数据一致后补写，
/// 否则对象+raft 启动守卫会拒绝非空运行态。
#[tokio::test]
async fn rerun_repairs_missing_activation_markers_on_legacy_root() {
    let f = fixture().await;
    apply(&f).await;
    let control = Arc::new(read_control(&f).await);
    control
        .transaction(WriteOrigin::local("test-legacy-root"), |txn| {
            Box::pin(async move {
                txn.remove_marker(META_RUNSTATE_ACTIVATED);
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(
        control
            .pin()
            .await
            .unwrap()
            .root
            .meta
            .markers
            .get(META_RUNSTATE_ACTIVATED)
            .is_none()
    );

    let report = apply(&f).await;
    assert_eq!(report.status, "already-applied");
    let pinned = control.pin().await.unwrap();
    assert_eq!(
        pinned
            .root
            .meta
            .markers
            .get(META_RUNSTATE_ACTIVATED)
            .map(String::as_str),
        Some("1")
    );
    // 补写后必须能通过对象+raft 启动守卫（用空文件快照隔离 Raft sink 的合成快照）。
    let snap_dir = tempfile::tempdir().unwrap();
    let snapshots =
        crate::raft::file_snapshot::FileSnapshotStore::open(&snap_dir.path().join("raft.db"))
            .unwrap();
    crate::raft::compat::guard_object_startup(&control, &snapshots, 9)
        .await
        .unwrap();
}

#[tokio::test]
async fn verify_detects_source_drift() {
    let f = fixture().await;
    apply(&f).await;
    let control = read_control(&f).await;
    let mut source = read_source(&f).await;
    export::verify(&control, &source).await.unwrap();

    source.data.users[0].status = "disabled".into();
    assert!(
        export::verify(&control, &source).await.is_err(),
        "encoder-level drift must fail verification"
    );

    let mut source: Source = read_source(&f).await;
    let admin = source.raw.users[0].is_admin;
    source.raw.users[0].is_admin = !admin;
    assert!(
        export::verify(&control, &source).await.is_err(),
        "raw local-attribute drift must fail verification"
    );
}
