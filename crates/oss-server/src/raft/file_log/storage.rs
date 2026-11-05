//! `FileLogStore`: open/bootstrap, append/truncate/purge, and the openraft log traits.

#![allow(clippy::result_large_err)]

use std::fmt::Debug;
use std::fs;
use std::io::{self, Read};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    CommittedLeaderId, Entry, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend,
    RaftLogReader, StorageError, Vote,
};

use crate::raft::TypeConfig;
use crate::raft::file_meta::{CURRENT_LOG_FORMAT, RaftMetaFile};
use crate::raft::store::{decode_entry_with_len, encode_entry, log_corrupt, log_err, storage_err};

use super::segments::{FRAME_HEADER_LEN, segment_files, write_segment_frames};
use super::{FaultPoint, FrameLoc, LOG_DIR, LogInner, raft_dir};

/// Translate the log_id of `index` (or the purge bound when the index is at/below it).
fn log_id_at_or_below(inner: &LogInner, index: u64) -> Option<LogId<u64>> {
    if index == 0 {
        return None;
    }
    if let Some((_, frame)) = inner.by_index.get(&index) {
        return Some(LogId::new(
            CommittedLeaderId::new(frame.term, frame.node),
            index,
        ));
    }
    match inner.meta.last_purged {
        Some(purged) if purged.index >= index => Some(purged),
        _ => None,
    }
}

/// The open file log store. Clones share one in-memory index and are used as openraft's
/// `LogReader`; all writes are serialized by the mutex.
#[derive(Clone)]
pub struct FileLogStore {
    dir: PathBuf,
    inner: Arc<Mutex<LogInner>>,
}

impl FileLogStore {
    /// Open (or bootstrap) the file log for `db_path`. Refuses a legacy SQLite `raft.db` file.
    pub async fn open(db_path: &Path) -> Result<Self, StorageError<u64>> {
        let dir = raft_dir(db_path);
        match fs::symlink_metadata(db_path) {
            Ok(md) if md.file_type().is_symlink() || md.is_file() => {
                return Err(log_err(
                    ErrorVerb::Read,
                    format!(
                        "legacy SQLite raft database found at {}: refusing to open it. Stop the \
                         server and run the offline migration (`oss-server control-migrate`) \
                         before starting this binary; file raft storage lives in {}.",
                        db_path.display(),
                        dir.display()
                    ),
                ));
            }
            Ok(md) if md.is_dir() => {
                return Err(log_err(
                    ErrorVerb::Read,
                    format!(
                        "raft db path {} is a directory; expected the file store at {}",
                        db_path.display(),
                        dir.display()
                    ),
                ));
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(log_err(
                    ErrorVerb::Read,
                    format!("stat {}: {e}", db_path.display()),
                ));
            }
        }
        Self::open_dir(&dir).await
    }

    /// Open the file log directly at its `<raft.db>.d` directory without checking
    /// for a legacy SQLite file at the sibling path. Offline migration uses this
    /// to verify a final directory while the source `raft.db` still exists.
    pub(crate) async fn open_dir(dir: &Path) -> Result<Self, StorageError<u64>> {
        fs::create_dir_all(dir)
            .map_err(|e| log_err(ErrorVerb::Write, format!("create {}: {e}", dir.display())))?;
        let meta = match RaftMetaFile::load(dir)
            .map_err(|e| storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?
        {
            Some(meta) => meta,
            None => {
                let log_dir = dir.join(LOG_DIR);
                fs::create_dir_all(&log_dir).map_err(|e| {
                    log_err(
                        ErrorVerb::Write,
                        format!("create {}: {e}", log_dir.display()),
                    )
                })?;
                if !segment_files(&log_dir)?.is_empty() {
                    return Err(log_err(
                        ErrorVerb::Read,
                        format!(
                            "raft meta file {} is missing but log segments exist: refusing to \
                             guess (restore the backup or re-run the migration)",
                            RaftMetaFile::path(dir).display()
                        ),
                    ));
                }
                let meta = RaftMetaFile::default();
                meta.store_err(dir)?;
                meta
            }
        };
        let inner = LogInner::load(dir, meta)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, LogInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn store_meta(&self, inner: &LogInner, meta: &RaftMetaFile) -> Result<(), StorageError<u64>> {
        inner
            .fault(FaultPoint::BeforeMetaStore)
            .map_err(|e| log_err(ErrorVerb::Write, format!("injected meta fault: {e}")))?;
        meta.store_err(&self.dir)
    }

    /// Offline-import append (no openraft flush callback). Same contiguity and
    /// hole checks as a live append.
    pub async fn append_import(
        &self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<(), StorageError<u64>> {
        self.append_locked(entries)
    }

    pub(super) fn append_locked(
        &self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<(), StorageError<u64>> {
        if entries.is_empty() {
            return Ok(());
        }
        let first = entries[0].log_id.index;
        for (i, entry) in entries.iter().enumerate() {
            if entry.log_id.index != first + i as u64 {
                return Err(log_err(
                    ErrorVerb::Write,
                    format!(
                        "raft log append is not contiguous: expected {}, got {}",
                        first + i as u64,
                        entry.log_id.index
                    ),
                ));
            }
        }
        let mut inner = self.lock();
        let cur_last = inner
            .meta
            .last_index
            .map(|l| l.index)
            .or(inner.meta.last_purged.map(|l| l.index));
        if let Some(cur_last) = cur_last {
            if first <= cur_last {
                // Conflicting suffix: commit the truncation bound first, then overwrite on disk.
                let mut meta = inner.meta.clone();
                meta.last_index = log_id_at_or_below(&inner, first.saturating_sub(1));
                meta.log_format = CURRENT_LOG_FORMAT;
                self.store_meta(&inner, &meta)?;
                inner.meta = meta;
                inner.trim_suffix(first, &self.dir)?;
            } else if first != cur_last + 1 {
                return Err(log_err(
                    ErrorVerb::Write,
                    format!(
                        "raft log hole: append starts at {first}, expected {}",
                        cur_last + 1
                    ),
                ));
            }
        } else if first != 1 {
            return Err(log_err(
                ErrorVerb::Write,
                format!("raft log hole: empty log append starts at {first}, expected 1"),
            ));
        }
        let mut payloads = Vec::with_capacity(entries.len());
        for entry in entries {
            let encoded = encode_entry(entry)?;
            payloads.push((
                entry.log_id.index,
                entry.log_id.leader_id.term,
                entry.log_id.leader_id.voted_for().unwrap_or_default(),
                encoded.into_bytes(),
            ));
        }
        write_segment_frames(&mut inner, &self.dir, &payloads)?;
        inner
            .fault(FaultPoint::EntriesSynced)
            .map_err(|e| log_err(ErrorVerb::Write, format!("injected entry fault: {e}")))?;
        let last = entries.last().expect("non-empty").log_id;
        let mut meta = inner.meta.clone();
        meta.last_index = Some(last);
        meta.log_format = CURRENT_LOG_FORMAT;
        if let Err(e) = self.store_meta(&inner, &meta) {
            // The frames are durable but the meta commit failed: drop the orphan tail so a later
            // in-process append does not collide with it. Reopen would do the same.
            inner.trim_suffix(first, &self.dir).ok();
            return Err(e);
        }
        inner.meta = meta;
        inner.rebuild_index();
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for FileLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let start = match range.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(i) => match i.checked_add(1) {
                Some(v) => v,
                None => return Ok(Vec::new()),
            },
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(i) => match i.checked_sub(1) {
                Some(v) => v,
                None => return Ok(Vec::new()),
            },
            Bound::Unbounded => u64::MAX,
        };
        if start > end {
            return Ok(Vec::new());
        }
        let inner = self.lock();
        let locations: Vec<(u64, usize, FrameLoc)> = inner
            .by_index
            .range(start..=end)
            .map(|(index, (si, frame))| (*index, *si, *frame))
            .collect();
        let mut entries = Vec::with_capacity(locations.len());
        let mut cached: Option<(usize, Vec<u8>)> = None;
        for (index, si, frame) in locations {
            if cached.as_ref().map(|(c, _)| *c) != Some(si) {
                let path = &inner.segments[si].path;
                let mut bytes = Vec::new();
                fs::File::open(path)
                    .and_then(|mut f| f.read_to_end(&mut bytes))
                    .map_err(|e| {
                        log_err(ErrorVerb::Read, format!("read {}: {e}", path.display()))
                    })?;
                cached = Some((si, bytes));
            }
            let bytes = &cached.as_ref().expect("cached segment").1;
            let begin = frame.offset as usize + FRAME_HEADER_LEN;
            let end = begin + frame.payload_len as usize;
            if end > bytes.len() {
                return Err(log_corrupt(format!(
                    "raft log entry {index} is truncated in segment {}",
                    inner.segments[si].path.display()
                )));
            }
            let text = std::str::from_utf8(&bytes[begin..end])
                .map_err(|e| log_corrupt(format!("entry {index} is not utf-8: {e}")))?;
            let (entry, _) = decode_entry_with_len(text)?;
            if entry.log_id.index != index || entry.log_id.leader_id.term != frame.term {
                return Err(log_corrupt(format!(
                    "raft log frame {index} holds entry {}/term {}",
                    entry.log_id.index, entry.log_id.leader_id.term
                )));
            }
            entries.push(entry);
        }
        for pair in entries.windows(2) {
            let (prev, next) = (pair[0].log_id.index, pair[1].log_id.index);
            if next != prev + 1 {
                return Err(log_corrupt(format!(
                    "raft log index gap between {prev} and {next}"
                )));
            }
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for FileLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.lock();
        Ok(LogState {
            last_purged_log_id: inner.meta.last_purged,
            last_log_id: inner.meta.last_index.or(inner.meta.last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        let mut meta = inner.meta.clone();
        meta.vote = Some(*vote);
        self.store_meta(&inner, &meta)?;
        inner.meta = meta;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.lock().meta.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        let mut meta = inner.meta.clone();
        meta.committed = committed;
        self.store_meta(&inner, &meta)?;
        inner.meta = meta;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.lock().meta.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let result = self.append_locked(&entries);
        match &result {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(e) => callback.log_io_completed(Err(io::Error::other(e.to_string()))),
        }
        result
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        let cur_last = inner
            .meta
            .last_index
            .map(|l| l.index)
            .or(inner.meta.last_purged.map(|l| l.index));
        if let Some(cur_last) = cur_last
            && log_id.index > cur_last
        {
            return Ok(());
        }
        let new_last = match log_id.index.checked_sub(1) {
            Some(prev) => log_id_at_or_below(&inner, prev),
            None => None,
        };
        let mut meta = inner.meta.clone();
        meta.last_index = new_last;
        self.store_meta(&inner, &meta)?;
        inner.meta = meta;
        inner.trim_suffix(log_id.index, &self.dir)
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        if let Some(purged) = inner.meta.last_purged
            && purged.index >= log_id.index
        {
            return Ok(());
        }
        let cur_last = inner.meta.last_index.map(|l| l.index);
        let new_last = match cur_last {
            Some(last) if last > log_id.index => {
                log_id_at_or_below(&inner, last).or(inner.meta.last_index)
            }
            _ => Some(log_id),
        };
        let mut meta = inner.meta.clone();
        meta.last_purged = Some(log_id);
        meta.last_index = new_last;
        self.store_meta(&inner, &meta)?;
        inner.meta = meta;
        inner.trim_prefix(log_id.index, &self.dir)
    }
}
