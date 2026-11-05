//! Crash windows where fsynced entry frames outrun the meta rename, or the meta
//! bounds are durable before the physical trim/suffix deletion. These cover the
//! kill9-at-append/meta boundaries deterministically via fault injection.

use std::io;
use std::sync::{Arc, Mutex};

use openraft::RaftLogReader;
use openraft::storage::RaftLogStorage;

use super::{append, blank, indices, log_id};
use crate::raft::file_log::{FaultPoint, FileLogStore, raft_dir};
use crate::raft::file_meta::RaftMetaFile;

#[tokio::test]
async fn raft_file_log_crash_after_entry_fsync_before_meta_rename_recovers_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let store = FileLogStore::open(&path).await.unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    {
        let calls = Arc::clone(&calls);
        store.lock().fault = Some(Arc::new(move |point| {
            calls.lock().unwrap().push(point);
            if point == FaultPoint::BeforeMetaStore {
                Err(io::Error::other("injected crash before meta rename"))
            } else {
                Ok(())
            }
        }));
    }
    let err = store.append_locked(&[blank(1)]).unwrap_err().to_string();
    assert!(err.contains("injected meta fault"), "{err}");
    assert_eq!(
        *calls.lock().unwrap(),
        vec![FaultPoint::EntriesSynced, FaultPoint::BeforeMetaStore],
        "entries must be fsynced before the meta rename is attempted"
    );
    drop(store);

    // Meta still says "no logs"; the durable orphan frame is ignored and truncated.
    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(store.get_log_state().await.unwrap().last_log_id, None);
    assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
    store.lock().fault = None;
    append(&mut store, vec![blank(1)]);
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![1]
    );
}

#[tokio::test]
async fn raft_file_log_replays_meta_bounds_after_crash_before_physical_trim() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let meta_dir = raft_dir(&path);
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(&mut store, (1..=5).map(blank).collect());
    drop(store);

    // Crash after purge persisted last_purged=3 but before the prefix was deleted.
    let mut meta = RaftMetaFile::load(&meta_dir).unwrap().unwrap();
    meta.last_purged = Some(log_id(3));
    meta.store(&meta_dir).unwrap();
    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![4, 5]
    );
    assert_eq!(
        store.get_log_state().await.unwrap().last_purged_log_id,
        Some(log_id(3))
    );

    // Crash after truncate persisted last_index=4 but before the suffix was deleted.
    let mut meta = RaftMetaFile::load(&meta_dir).unwrap().unwrap();
    meta.last_index = Some(log_id(4));
    meta.store(&meta_dir).unwrap();
    drop(store);
    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![4]
    );
}
