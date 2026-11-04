use std::fs;

use openraft::entry::{Entry, EntryPayload};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{BasicNode, CommittedLeaderId, LogId, SnapshotMeta, StoredMembership};

use super::*;
use crate::raft::ControlCommand;
use crate::raft::store::SqliteStateMachine;

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

fn meta(index: u64, id: &str) -> SnapshotMeta<u64, BasicNode> {
    SnapshotMeta {
        last_log_id: Some(log_id(index)),
        last_membership: StoredMembership::default(),
        snapshot_id: id.to_string(),
    }
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

#[tokio::test]
async fn raft_file_snapshot_roundtrip_multiple_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap();
    let bytes = payload(600 * 1024);
    let m = meta(7, "7-1700000000000");
    store.write(&m, &bytes).await.unwrap();

    let snap_dir = dir.path().join("raft.db.d/snapshot/7-1700000000000");
    assert!(snap_dir.join("manifest").is_file());
    let chunk0 = fs::read(snap_dir.join("chunk-000000")).unwrap();
    let chunk1 = fs::read(snap_dir.join("chunk-000001")).unwrap();
    let chunk2 = fs::read(snap_dir.join("chunk-000002")).unwrap();
    assert_eq!(chunk0.len(), SNAPSHOT_CHUNK_BYTES);
    assert_eq!(chunk1.len(), SNAPSHOT_CHUNK_BYTES);
    assert_eq!(chunk2.len(), 600 * 1024 - 2 * SNAPSHOT_CHUNK_BYTES);
    assert_eq!(chunk0.as_slice(), &bytes[..SNAPSHOT_CHUNK_BYTES]);
    assert_eq!(
        chunk1.as_slice(),
        &bytes[SNAPSHOT_CHUNK_BYTES..2 * SNAPSHOT_CHUNK_BYTES]
    );

    let listed = store.list().await.unwrap();
    assert_eq!(listed, vec![m.clone()]);
    let latest = store.latest().await.unwrap().unwrap();
    assert_eq!(latest.meta, m);
    assert_eq!(latest.snapshot.get_ref(), &bytes);
    let read = store.read(&m.snapshot_id).await.unwrap().unwrap();
    assert_eq!(read.snapshot.get_ref(), &bytes);

    store.delete(&m.snapshot_id).await.unwrap();
    assert!(store.list().await.unwrap().is_empty());
    assert!(store.read(&m.snapshot_id).await.unwrap().is_none());
    assert!(store.latest().await.unwrap().is_none());
}

#[tokio::test]
async fn raft_file_snapshot_chunk_bytes_match_sqlite_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let sm_path = dir.path().join("sqlite-sm.db");
    let mut sm = SqliteStateMachine::open(&sm_path, 64 * 1024 * 1024)
        .await
        .unwrap();
    for index in 1..=4u64 {
        RaftStateMachine::<TypeConfig>::apply(
            &mut sm,
            [Entry {
                log_id: log_id(index),
                payload: EntryPayload::Normal(ControlCommand::Set {
                    key: format!("k{index}"),
                    value: "x".repeat(100 * 1024),
                }),
            }],
        )
        .await
        .unwrap();
    }
    let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm)
        .await
        .unwrap();
    let sqlite_chunks = sm.snapshot_chunks_for_test().await;
    assert!(sqlite_chunks.len() >= 2, "fixture must exercise chunking");

    let store = FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap();
    store
        .write(&snap.meta, snap.snapshot.get_ref())
        .await
        .unwrap();
    let file_chunks: Vec<Vec<u8>> = sqlite_chunks
        .iter()
        .enumerate()
        .map(|(seq, _)| {
            fs::read(
                dir.path()
                    .join("raft.db.d/snapshot")
                    .join(&snap.meta.snapshot_id)
                    .join(chunk_name(seq)),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(
        file_chunks, sqlite_chunks,
        "file snapshot chunks must equal raft_snapshot_chunk.data bytes"
    );
    let read = store.read(&snap.meta.snapshot_id).await.unwrap().unwrap();
    assert_eq!(read.snapshot.get_ref(), snap.snapshot.get_ref());
}

#[tokio::test]
async fn raft_file_snapshot_corrupt_chunk_and_manifest_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap();
    let bytes = payload(300 * 1024);
    let m = meta(9, "9-1");
    store.write(&m, &bytes).await.unwrap();
    let snap_dir = dir.path().join("raft.db.d/snapshot/9-1");

    let chunk_path = snap_dir.join("chunk-000000");
    let mut chunk = fs::read(&chunk_path).unwrap();
    chunk[10] ^= 0xff;
    fs::write(&chunk_path, &chunk).unwrap();
    let err = store.read(&m.snapshot_id).await.unwrap_err().to_string();
    assert!(err.contains("corrupt raft snapshot"), "{err}");
    assert!(
        store.latest().await.is_err(),
        "latest must not skip a corrupt snapshot"
    );

    // Repair the chunk, then corrupt the manifest CRC instead.
    let mut chunk = fs::read(&chunk_path).unwrap();
    chunk[10] ^= 0xff;
    fs::write(&chunk_path, &chunk).unwrap();
    store.read(&m.snapshot_id).await.unwrap().unwrap();
    let manifest_path = snap_dir.join("manifest");
    let mut manifest = fs::read(&manifest_path).unwrap();
    let last = manifest.len() - 3;
    manifest[last] ^= 0xff;
    fs::write(&manifest_path, &manifest).unwrap();
    let err = store.read(&m.snapshot_id).await.unwrap_err().to_string();
    assert!(err.contains("corrupt raft snapshot"), "{err}");
    let err = store.list().await.unwrap_err().to_string();
    assert!(err.contains("corrupt raft snapshot"), "{err}");
}

#[tokio::test]
async fn raft_file_snapshot_list_orders_and_latest_wins() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap();
    let older = meta(3, "3-100");
    let newer = meta(8, "8-200");
    store.write(&newer, b"newer").await.unwrap();
    store.write(&older, b"older").await.unwrap();
    assert_eq!(
        store
            .list()
            .await
            .unwrap()
            .iter()
            .map(|m| m.snapshot_id.clone())
            .collect::<Vec<_>>(),
        vec!["3-100", "8-200"]
    );
    assert_eq!(
        store.latest().await.unwrap().unwrap().meta.snapshot_id,
        "8-200"
    );
}

#[tokio::test]
async fn raft_file_snapshot_rejects_path_traversal_ids() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSnapshotStore::open(&dir.path().join("raft.db")).unwrap();
    for id in ["", ".", "..", "../escape", "a/b", "a\\b"] {
        let err = store
            .write(&meta(1, id), b"x")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid raft snapshot id"), "{id:?}: {err}");
        assert!(store.read(id).await.is_err());
        assert!(store.delete(id).await.is_err());
    }
}
