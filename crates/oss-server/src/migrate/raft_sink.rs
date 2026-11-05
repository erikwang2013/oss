//! Copy the SQLite Raft database into the A7 file stores.
//!
//! The file log is built under `<raft_db>.migrate.d` and renamed to
//! `<raft_db>.d` only after vote, committed, log entries and snapshot are all
//! durable. An existing final directory is never rewritten: it is verified
//! against the source so a rerun after an interruption either completes the
//! missing rename or proves the target already matches.

use std::path::{Path, PathBuf};

use openraft::storage::{RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, LogState, Vote};

use crate::raft::TypeConfig;
use crate::raft::file_log::{FileLogStore, raft_dir};
use crate::raft::file_snapshot::FileSnapshotStore;

use super::raft_source::sha256;
use super::source::{BoxError, Source, error};

#[derive(Debug, Default)]
pub(crate) struct RaftStats {
    pub entries: u64,
    pub vote: bool,
    pub committed: bool,
    pub last_purged: Option<u64>,
    pub snapshot: bool,
    pub snapshot_sha256: Option<String>,
    pub entries_sha256: Option<String>,
    pub source_checksum: Option<String>,
}

pub(crate) async fn apply(raft_db: &Path, source: &Source) -> Result<RaftStats, BoxError> {
    let final_dir = raft_dir(raft_db);
    if final_dir.is_dir() {
        return verify(raft_db, source).await;
    }
    let raft = &source.raft;
    if !raft.has_state() {
        return Ok(RaftStats::default());
    }
    let tmp_db = temp_db(raft_db);
    let tmp_dir = raft_dir(&tmp_db);
    remove_path(&tmp_db)?;
    remove_path(&tmp_dir)?;
    if let Some(parent) = raft_db.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut log = FileLogStore::open(&tmp_db).await.map_err(storage_error)?;
    if let Some(purged) = raft.last_purged {
        log.purge(purged).await.map_err(storage_error)?;
    }
    log.append_import(&raft.entries)
        .await
        .map_err(storage_error)?;
    if let Some(vote) = raft.vote {
        log.save_vote(&vote).await.map_err(storage_error)?;
    }
    log.save_committed(raft.committed)
        .await
        .map_err(storage_error)?;

    if let Some((meta, bytes)) = &raft.snapshot {
        let snapshots = FileSnapshotStore::open(&tmp_db).map_err(storage_error)?;
        snapshots.write(meta, bytes).await.map_err(storage_error)?;
    }

    std::fs::rename(&tmp_dir, &final_dir)?;
    sync_parent(&final_dir)?;
    let stats = verify(raft_db, source).await?;
    if stats.entries != raft.entries.len() as u64 {
        return error(format!(
            "raft import wrote {} entries, source has {}",
            stats.entries,
            raft.entries.len()
        ));
    }
    Ok(stats)
}

pub(crate) async fn verify(raft_db: &Path, source: &Source) -> Result<RaftStats, BoxError> {
    let raft = &source.raft;
    let dir = raft_dir(raft_db);
    if !raft.has_state() {
        if !dir.exists() {
            return Ok(RaftStats::default());
        }
        // A failed standalone boot may have created an empty file log; accept
        // exactly that, but never a target that already holds state.
        let mut log = FileLogStore::open_dir(&dir).await.map_err(storage_error)?;
        let state = log.get_log_state().await.map_err(storage_error)?;
        if state.last_log_id.is_some() || state.last_purged_log_id.is_some() {
            return error(format!(
                "raft source is empty but target {} holds log state",
                dir.display()
            ));
        }
        if log.read_vote().await.map_err(storage_error)?.is_some()
            || log.read_committed().await.map_err(storage_error)?.is_some()
        {
            return error(format!(
                "raft source is empty but target {} holds vote/committed",
                dir.display()
            ));
        }
        return Ok(RaftStats::default());
    }
    let mut log = FileLogStore::open_dir(&dir).await.map_err(storage_error)?;
    let state: LogState<TypeConfig> = log.get_log_state().await.map_err(storage_error)?;
    let want_last = last_log(raft.entries.last().map(|e| e.log_id), raft.last_purged);
    if state.last_log_id != want_last {
        return error(format!(
            "raft log state last_log_id {:?} != source {want_last:?}",
            state.last_log_id
        ));
    }
    if state.last_purged_log_id != raft.last_purged {
        return error(format!(
            "raft log state last_purged {:?} != source {:?}",
            state.last_purged_log_id, raft.last_purged
        ));
    }
    let vote: Option<Vote<u64>> = log.read_vote().await.map_err(storage_error)?;
    if vote != raft.vote {
        return error(format!("raft vote {vote:?} != source {:?}", raft.vote));
    }
    let committed: Option<LogId<u64>> = log.read_committed().await.map_err(storage_error)?;
    if committed != raft.committed {
        return error(format!(
            "raft committed {committed:?} != source {:?}",
            raft.committed
        ));
    }
    let entries: Vec<Entry<TypeConfig>> = RaftLogReader::try_get_log_entries(&mut log, ..)
        .await
        .map_err(storage_error)?;
    if entries.len() != raft.entries.len() {
        return error(format!(
            "raft target has {} entries, source has {}",
            entries.len(),
            raft.entries.len()
        ));
    }
    for (got, want) in entries.iter().zip(&raft.entries) {
        if serde_json::to_vec(got)? != serde_json::to_vec(want)? {
            return error(format!(
                "raft entry {} bytes differ from source",
                want.log_id.index
            ));
        }
    }

    let snapshots = FileSnapshotStore::open(raft_db).map_err(storage_error)?;
    let listed = snapshots.list().await.map_err(storage_error)?;
    match &raft.snapshot {
        None => {
            if !listed.is_empty() {
                return error(format!(
                    "raft target has {} snapshots, source has none",
                    listed.len()
                ));
            }
        }
        Some((want_meta, want_bytes)) => {
            if listed.len() != 1 {
                return error(format!(
                    "raft target has {} snapshots, source has 1",
                    listed.len()
                ));
            }
            if serde_json::to_vec(&listed[0])? != serde_json::to_vec(want_meta)? {
                return error("raft snapshot metadata differs from source");
            }
            let got = snapshots
                .read(&want_meta.snapshot_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| {
                    std::io::Error::other(format!(
                        "raft snapshot {} missing",
                        want_meta.snapshot_id
                    ))
                })?;
            let bytes = got.snapshot.get_ref().as_slice();
            if sha256(bytes) != sha256(want_bytes) {
                return error("raft snapshot bytes differ from source");
            }
        }
    }

    Ok(RaftStats {
        entries: entries.len() as u64,
        vote: raft.vote.is_some(),
        committed: raft.committed.is_some(),
        last_purged: raft.last_purged.map(|l| l.index),
        snapshot: raft.snapshot.is_some(),
        snapshot_sha256: raft.snapshot_sha256.clone(),
        entries_sha256: raft.entries_sha256.clone(),
        source_checksum: raft.checksum.clone(),
    })
}

fn last_log(a: Option<LogId<u64>>, b: Option<LogId<u64>>) -> Option<LogId<u64>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if a.index >= b.index { a } else { b }),
        (a, b) => a.or(b),
    }
}

fn temp_db(raft_db: &Path) -> PathBuf {
    let mut path = raft_db.as_os_str().to_os_string();
    path.push(".migrate");
    PathBuf::from(path)
}

fn remove_path(path: &Path) -> Result<(), BoxError> {
    match std::fs::symlink_metadata(path) {
        Ok(md) if md.is_dir() => std::fs::remove_dir_all(path)?,
        Ok(_) => std::fs::remove_file(path)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), BoxError> {
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn storage_error(e: openraft::StorageError<u64>) -> BoxError {
    std::io::Error::other(e.to_string()).into()
}
