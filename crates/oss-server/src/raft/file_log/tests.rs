use std::fs;
use std::io::Write;
use std::ops::Bound;
use std::path::PathBuf;

use openraft::entry::{Entry, EntryPayload};
use openraft::storage::RaftLogStorage;
use openraft::{CommittedLeaderId, LogId, LogState, RaftLogReader, Vote};

use super::segments::{
    FRAME_HEADER_LEN, FRAME_MAGIC, SEGMENT_MAX_FRAMES, encode_frame, segment_path,
};
use super::{FileLogStore, LOG_DIR, raft_dir};
use crate::raft::file_meta::RaftMetaFile;
use crate::raft::store::encode_entry;
use crate::raft::{ControlCommand, TypeConfig};

mod crash;

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

fn term_log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

fn blank(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Blank,
    }
}

fn term_blank(term: u64, index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: term_log_id(term, index),
        payload: EntryPayload::Blank,
    }
}

fn set_value(index: u64, value: &str) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(ControlCommand::Set {
            key: format!("k{index}"),
            value: value.into(),
        }),
    }
}

fn indices(entries: &[Entry<TypeConfig>]) -> Vec<u64> {
    entries.iter().map(|e| e.log_id.index).collect()
}

fn append(store: &mut FileLogStore, entries: Vec<Entry<TypeConfig>>) {
    store.append_locked(&entries).unwrap();
}

fn raw_payload(store: &FileLogStore, index: u64) -> Vec<u8> {
    let inner = store.lock();
    let (si, frame) = *inner.by_index.get(&index).expect("frame present");
    let bytes = fs::read(&inner.segments[si].path).unwrap();
    let begin = frame.offset as usize + FRAME_HEADER_LEN;
    let end = begin + frame.payload_len as usize;
    bytes[begin..end].to_vec()
}

fn segment_paths(store: &FileLogStore) -> Vec<PathBuf> {
    let inner = store.lock();
    inner.segments.iter().map(|s| s.path.clone()).collect()
}

#[tokio::test]
async fn raft_file_log_bootstraps_empty_dir_and_roundtrips_trait_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    assert!(RaftMetaFile::path(&raft_dir(&path)).exists());
    assert_eq!(
        store.get_log_state().await.unwrap(),
        LogState {
            last_purged_log_id: None,
            last_log_id: None,
        }
    );

    append(&mut store, (1..=3).map(blank).collect());
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(3))
    );
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![1, 2, 3]
    );
    let mid = store
        .try_get_log_entries((Bound::Excluded(1u64), Bound::Included(3u64)))
        .await
        .unwrap();
    assert_eq!(indices(&mid), vec![2, 3]);
    assert!(
        store
            .try_get_log_entries((Bound::Excluded(3u64), Bound::Included(1u64)))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.try_get_log_entries(5..7).await.unwrap().is_empty());

    let vote = Vote::new_committed(4, 1);
    store.save_vote(&vote).await.unwrap();
    store.save_committed(Some(log_id(2))).await.unwrap();
    drop(store);

    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    assert_eq!(store.read_committed().await.unwrap(), Some(log_id(2)));
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![1, 2, 3]
    );
}

/// openraft 0.9 `initialize()` appends the initial membership entry at `LogId::default()`
/// (index 0). The file log must accept and re-open that 0-based log.
#[tokio::test]
async fn raft_file_log_accepts_initial_entry_at_index_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    let init = Entry {
        log_id: LogId::default(),
        payload: EntryPayload::Blank,
    };
    append(&mut store, vec![init, blank(1)]);
    drop(store);

    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![0, 1]
    );
    store.truncate(log_id(1)).await.unwrap();
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(LogId::default())
    );
    drop(store);

    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![0]
    );
    append(&mut store, vec![blank(1)]);
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![0, 1]
    );
}

#[tokio::test]
async fn raft_file_log_entry_bytes_match_encoder() {
    let dir = tempfile::tempdir().unwrap();
    let entries = vec![blank(1), set_value(2, &"x".repeat(4096))];
    let expected: Vec<Vec<u8>> = entries
        .iter()
        .map(|entry| encode_entry(entry).unwrap().into_bytes())
        .collect();
    // 大 entry 必须走压缩形态，且前缀无歧义。
    assert!(expected[1].starts_with(b"z1:"));

    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(&mut store, entries.clone());
    for (i, expected) in expected.iter().enumerate() {
        assert_eq!(
            &raw_payload(&store, (i + 1) as u64),
            expected,
            "file log entry {} must keep the exact encoded bytes",
            i + 1
        );
    }
    let read = store.try_get_log_entries(..).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&read).unwrap(),
        serde_json::to_vec(&entries).unwrap()
    );
}

#[tokio::test]
async fn raft_file_log_torn_tail_is_truncated_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(&mut store, vec![blank(1), blank(2)]);
    let segment = segment_paths(&store).pop().unwrap();
    let good_len = fs::metadata(&segment).unwrap().len();
    drop(store);

    // Simulate a torn append: a partial frame beyond the meta bound.
    let mut file = fs::OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(&FRAME_MAGIC).unwrap();
    file.write_all(&[0u8; 6]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(fs::metadata(&segment).unwrap().len() > good_len);

    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![1, 2]
    );
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(2))
    );
    assert_eq!(
        fs::metadata(&segment).unwrap().len(),
        good_len,
        "torn tail must be truncated, not kept"
    );
}

#[tokio::test]
async fn raft_file_log_truncate_purge_and_append_after_purge() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(&mut store, (1..=5).map(blank).collect());

    store.truncate(log_id(2)).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![1]
    );
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(1))
    );

    store.purge(log_id(1)).await.unwrap();
    assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(1)));
    assert_eq!(state.last_log_id, Some(log_id(1)));

    append(&mut store, vec![blank(2)]);
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![2]
    );
    drop(store);

    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![2]
    );
    assert_eq!(
        store.get_log_state().await.unwrap().last_purged_log_id,
        Some(log_id(1))
    );
}

#[tokio::test]
async fn raft_file_log_partial_purge_rewrites_boundary_segment_readably() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(&mut store, (1..=5).map(blank).collect());
    store.purge(log_id(2)).await.unwrap();
    let entries = store.try_get_log_entries(..).await.unwrap();
    assert_eq!(indices(&entries), vec![3, 4, 5]);
    assert_eq!(entries[2].log_id, log_id(5));
    drop(store);

    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![3, 4, 5]
    );
}

#[tokio::test]
async fn raft_file_log_interrupted_purge_rewrite_prefers_new_segment() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let meta_dir = raft_dir(&path);
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(&mut store, (1..=5).map(blank).collect());
    let old_segment = segment_paths(&store).pop().unwrap();

    // Crash window: the purge bound is durable and the rewritten segment (3..=5) was renamed
    // into place, but the old segment (1..=5) was not deleted yet.
    let mut meta = RaftMetaFile::load(&meta_dir).unwrap().unwrap();
    meta.last_purged = Some(log_id(2));
    meta.store(&meta_dir).unwrap();
    let mut rewritten = Vec::new();
    for index in 3..=5u64 {
        rewritten.extend(encode_frame(index, 1, 1, &raw_payload(&store, index)));
    }
    let new_segment = segment_path(&meta_dir.join(LOG_DIR), 3);
    fs::write(&new_segment, rewritten).unwrap();
    drop(store);

    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![3, 4, 5]
    );
    assert!(
        !old_segment.exists(),
        "stale overlapping segment must be dropped"
    );
    assert!(new_segment.exists());
}

#[tokio::test]
async fn raft_file_log_conflicting_append_replaces_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(
        &mut store,
        vec![term_blank(1, 1), term_blank(1, 2), term_blank(1, 3)],
    );
    append(&mut store, vec![term_blank(2, 2), term_blank(2, 3)]);
    let entries = store.try_get_log_entries(..).await.unwrap();
    assert_eq!(indices(&entries), vec![1, 2, 3]);
    assert_eq!(entries[0].log_id.leader_id.term, 1);
    assert_eq!(entries[1].log_id.leader_id.term, 2);
    assert_eq!(entries[2].log_id.leader_id.term, 2);
    drop(store);

    let mut store = FileLogStore::open(&path).await.unwrap();
    let entries = store.try_get_log_entries(..).await.unwrap();
    assert_eq!(entries[1].log_id.leader_id.term, 2);
    assert_eq!(entries[2].log_id.leader_id.term, 2);
}

#[tokio::test]
async fn raft_file_log_rolls_segments_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    let count = SEGMENT_MAX_FRAMES as u64 + 1;
    append(&mut store, (1..=count).map(blank).collect());
    assert!(segment_paths(&store).len() >= 2);
    drop(store);

    let mut store = FileLogStore::open(&path).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        (1..=count).collect::<Vec<_>>()
    );
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(count))
    );
}

#[tokio::test]
async fn raft_file_log_refuses_higher_format_and_missing_meta() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let meta_dir = raft_dir(&path);
    let store = FileLogStore::open(&path).await.unwrap();
    drop(store);

    let mut meta = RaftMetaFile::load(&meta_dir).unwrap().unwrap();
    meta.log_format = 3;
    meta.store(&meta_dir).unwrap();
    let err = FileLogStore::open(&path)
        .await
        .err()
        .expect("open must reject the format")
        .to_string();
    assert!(err.contains("unsupported raft log format"), "{err}");
    assert!(err.contains("upgrade"), "{err}");

    meta.log_format = 1;
    meta.store(&meta_dir).unwrap();
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(&mut store, vec![blank(1)]);
    drop(store);

    fs::remove_file(RaftMetaFile::path(&meta_dir)).unwrap();
    let err = FileLogStore::open(&path)
        .await
        .err()
        .expect("open must reject missing meta with segments")
        .to_string();
    assert!(err.contains("meta file"), "{err}");
    assert!(err.contains("missing"), "{err}");
}

#[tokio::test]
async fn raft_file_log_refuses_legacy_sqlite_raft_db() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    fs::write(&path, b"SQLite format 3\0legacy").unwrap();
    let err = FileLogStore::open(&path)
        .await
        .err()
        .expect("open must refuse legacy SQLite")
        .to_string();
    assert!(err.contains("legacy SQLite raft database"), "{err}");
    assert!(err.contains("migration"), "{err}");
    assert!(
        !raft_dir(&path).exists(),
        "must refuse before creating the file-store directory"
    );
}

#[tokio::test]
async fn raft_file_log_truncate_purge_ignore_unknown_verb_boundaries() {
    // purge is idempotent and truncating beyond the tail is a no-op, as in SqliteLogStore.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("raft.db");
    let mut store = FileLogStore::open(&path).await.unwrap();
    append(&mut store, vec![blank(1), blank(2)]);
    store.truncate(log_id(9)).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![1, 2]
    );
    store.purge(log_id(1)).await.unwrap();
    store.purge(log_id(1)).await.unwrap();
    assert_eq!(
        indices(&store.try_get_log_entries(..).await.unwrap()),
        vec![2]
    );
    assert_eq!(
        store.get_log_state().await.unwrap().last_purged_log_id,
        Some(log_id(1))
    );
}
