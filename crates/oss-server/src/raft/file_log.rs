//! File-backed Raft log under `<raft.db_path>.d/`.
//!
//! Layout: `log/<first-index:020>.seg` segment files. Each segment is a sequence of frames
//! `magic(4) | payload_len(4 LE) | crc32(4 LE) | index(8 LE) | term(8 LE) | node(8 LE) |
//! payload`. The payload is byte-for-byte the value the SQLite backend stores in `raft_log.data`
//! (plain JSON or `z1:` compressed), so migrating preserves entry bytes exactly. The frame keeps
//! the full leader id because openraft's default `LogId` order compares `(term, node_id)`, not
//! just the index.
//!
//! Durability: appended frames are fsynced before `meta` is atomically replaced (tmp + fsync +
//! rename + dir fsync). A crash between those two steps leaves orphan frames beyond
//! `meta.last_index`; open ignores and truncates them. A torn last frame (partial write) is
//! truncated to the last complete frame on open. Truncate/purge first persist the new bound in
//! `meta`, then delete/rewrite segment files, so a crash in between is recovered by replaying the
//! bound on open.
//!
//! A regular file at `db_path` is the legacy SQLite `raft.db`; it is refused, never opened.

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Arc;

use openraft::{ErrorVerb, StorageError};

use super::file_meta::{RaftMetaFile, sync_dir};
use super::store::{log_corrupt, log_err};
use segments::{FRAME_HEADER_LEN, scan_segment, segment_files, segment_path, write_atomic};

mod segments;
mod storage;
pub use storage::FileLogStore;

const LOG_DIR: &str = "log";

/// `<raft.db_path>.d` (the path is extended, not replaced: `raft.db` -> `raft.db.d`).
pub fn raft_dir(db_path: &Path) -> PathBuf {
    let mut dir = db_path.as_os_str().to_os_string();
    dir.push(".d");
    PathBuf::from(dir)
}

#[derive(Clone, Copy, Debug)]
struct FrameLoc {
    index: u64,
    term: u64,
    node: u64,
    offset: u64,
    payload_len: u32,
}

struct Segment {
    start: u64,
    path: PathBuf,
    bytes: u64,
    frames: Vec<FrameLoc>,
}

impl Segment {
    fn last_index(&self) -> u64 {
        self.frames.last().expect("non-empty segment").index
    }
}

struct LogInner {
    meta: RaftMetaFile,
    segments: Vec<Segment>,
    by_index: BTreeMap<u64, (usize, FrameLoc)>,
    #[cfg(test)]
    fault: Option<FaultHook>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FaultPoint {
    /// Frames are fsynced, meta has not been replaced yet.
    EntriesSynced,
    /// Called immediately before the meta tmp+rename.
    BeforeMetaStore,
}

#[cfg(test)]
pub(crate) type FaultHook = Arc<dyn Fn(FaultPoint) -> io::Result<()> + Send + Sync>;

impl LogInner {
    fn fault(&self, point: FaultPoint) -> io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = &self.fault {
            hook(point)?;
        }
        #[cfg(not(test))]
        let _ = point;
        Ok(())
    }

    fn rebuild_index(&mut self) {
        self.by_index.clear();
        for (si, segment) in self.segments.iter().enumerate() {
            for frame in &segment.frames {
                self.by_index.insert(frame.index, (si, *frame));
            }
        }
    }

    fn first_index(&self) -> Option<u64> {
        self.segments.first().map(|s| s.start)
    }

    fn last_index(&self) -> Option<u64> {
        self.segments.last().map(Segment::last_index)
    }

    /// Load segments, verify framing/continuity, then replay the meta bounds: drop the purged
    /// prefix and the orphan tail beyond `last_index`.
    fn load(dir: &Path, meta: RaftMetaFile) -> Result<Self, StorageError<u64>> {
        let log_dir = dir.join(LOG_DIR);
        fs::create_dir_all(&log_dir).map_err(|e| {
            log_err(
                ErrorVerb::Read,
                format!("create {}: {e}", log_dir.display()),
            )
        })?;
        let paths = segment_files(&log_dir)?;
        let mut segments = Vec::new();
        let count = paths.len();
        for (i, (start, path)) in paths.into_iter().enumerate() {
            let (frames, torn_at) = scan_segment(&path, start)?;
            if torn_at.is_some() && i + 1 != count {
                return Err(log_corrupt(format!(
                    "torn frame in non-tail raft log segment {}",
                    path.display()
                )));
            }
            if frames.is_empty() {
                fs::remove_file(&path).map_err(|e| {
                    log_err(ErrorVerb::Delete, format!("remove {}: {e}", path.display()))
                })?;
                continue;
            }
            let bytes = fs::metadata(&path)
                .map_err(|e| log_err(ErrorVerb::Read, format!("stat {}: {e}", path.display())))?
                .len();
            segments.push(Segment {
                start,
                path,
                bytes,
                frames,
            });
        }
        // An interrupted purge writes the rewritten boundary segment under its new first index
        // and only then removes the old file; if we crashed in between both exist. The new file
        // is complete and authoritative, the old one only holds purged/duplicate frames.
        let mut deduped: Vec<Segment> = Vec::with_capacity(segments.len());
        for segment in segments {
            while let Some(prev) = deduped.last() {
                if segment.start <= prev.last_index() {
                    let stale = deduped.pop().expect("checked non-empty");
                    let _ = fs::remove_file(&stale.path);
                } else {
                    break;
                }
            }
            deduped.push(segment);
        }
        let segments = deduped;
        for pair in segments.windows(2) {
            if pair[1].start != pair[0].last_index() + 1 {
                return Err(log_corrupt(format!(
                    "raft log segment gap: {} ends at {}, next starts at {}",
                    pair[0].path.display(),
                    pair[0].last_index(),
                    pair[1].start
                )));
            }
        }
        let mut inner = Self {
            meta,
            segments,
            by_index: BTreeMap::new(),
            #[cfg(test)]
            fault: None,
        };
        inner.rebuild_index();
        inner.reconcile(dir)?;
        Ok(inner)
    }

    /// Replay `meta` on top of the scanned log. `meta.last_index` bounds the valid log; frames
    /// above it are the crash-before-meta-rename orphan tail.
    fn reconcile(&mut self, dir: &Path) -> Result<(), StorageError<u64>> {
        let hi = self.meta.last_index.map(|l| l.index);
        let lo = self.meta.last_purged.map(|l| l.index);
        if let (Some(hi), Some(lo)) = (hi, lo)
            && hi < lo
        {
            return Err(log_corrupt(format!(
                "meta last_index {hi} is below last_purged {lo}"
            )));
        }
        match (hi, self.last_index()) {
            (None, None) | (Some(_), None) => {}
            (None, Some(_)) => self.trim_suffix(0, dir)?,
            (Some(hi), Some(end)) if end > hi => self.trim_suffix(hi + 1, dir)?,
            (Some(hi), Some(end)) if end < hi && lo != Some(hi) => {
                return Err(log_corrupt(format!(
                    "meta last_index {hi} is beyond the scanned log end {end}"
                )));
            }
            _ => {}
        }
        if let Some(lo) = lo {
            self.trim_prefix(lo, dir)?;
        }
        // Final invariant: the log covers exactly (last_purged, last_index], no holes.
        let hi = self.meta.last_index.map(|l| l.index);
        match (hi, self.last_index()) {
            (None, None) => {}
            (Some(hi), Some(end)) if end == hi => {}
            (Some(hi), None) if lo == Some(hi) => {}
            (None, Some(end)) => {
                return Err(log_corrupt(format!(
                    "raft log has entries up to {end} but meta has no last_index"
                )));
            }
            (Some(hi), Some(end)) => {
                return Err(log_corrupt(format!(
                    "raft log ends at {end} but meta last_index is {hi}"
                )));
            }
            (Some(hi), None) => {
                return Err(log_corrupt(format!(
                    "raft log is empty but meta last_index is {hi} (last_purged {lo:?})"
                )));
            }
        }
        if let Some(first) = self.first_index() {
            // openraft 0.9 initializes at index 0; 1-based logs are accepted for older
            // fixtures. After a purge the first index must be the purge bound + 1.
            let expected = lo.map(|l| l + 1).unwrap_or(0);
            if first != expected && !(lo.is_none() && first == 1) {
                return Err(log_corrupt(format!(
                    "raft log starts at {first}, expected {expected} (last_purged {lo:?})"
                )));
            }
        }
        Ok(())
    }

    /// Remove frames with index >= `from` from disk and the in-memory index.
    fn trim_suffix(&mut self, from: u64, dir: &Path) -> Result<(), StorageError<u64>> {
        let log_dir = dir.join(LOG_DIR);
        let mut kept = Vec::with_capacity(self.segments.len());
        let mut changed = false;
        for mut segment in self.segments.drain(..) {
            let last = segment.last_index();
            if last < from {
                kept.push(segment);
                continue;
            }
            changed = true;
            if segment.start >= from {
                fs::remove_file(&segment.path).map_err(|e| {
                    log_err(
                        ErrorVerb::Delete,
                        format!("remove {}: {e}", segment.path.display()),
                    )
                })?;
                continue;
            }
            let pos = segment.frames.partition_point(|f| f.index < from);
            let cut = segment.frames[pos].offset;
            let file = OpenOptions::new()
                .write(true)
                .open(&segment.path)
                .map_err(|e| {
                    log_err(
                        ErrorVerb::Write,
                        format!("open {}: {e}", segment.path.display()),
                    )
                })?;
            file.set_len(cut).map_err(|e| {
                log_err(
                    ErrorVerb::Write,
                    format!("truncate {}: {e}", segment.path.display()),
                )
            })?;
            file.sync_all().map_err(|e| {
                log_err(
                    ErrorVerb::Write,
                    format!("fsync {}: {e}", segment.path.display()),
                )
            })?;
            segment.frames.truncate(pos);
            segment.bytes = cut;
            kept.push(segment);
        }
        self.segments = kept;
        if changed {
            sync_dir(&log_dir).map_err(|e| {
                log_err(
                    ErrorVerb::Write,
                    format!("fsync {}: {e}", log_dir.display()),
                )
            })?;
        }
        self.rebuild_index();
        Ok(())
    }

    /// Remove frames with index <= `upto`. The boundary segment is rewritten under its new first
    /// index; fully covered segments are deleted.
    fn trim_prefix(&mut self, upto: u64, dir: &Path) -> Result<(), StorageError<u64>> {
        let log_dir = dir.join(LOG_DIR);
        let mut kept = Vec::with_capacity(self.segments.len());
        let mut changed = false;
        for segment in self.segments.drain(..) {
            let last = segment.last_index();
            if last <= upto {
                changed = true;
                fs::remove_file(&segment.path).map_err(|e| {
                    log_err(
                        ErrorVerb::Delete,
                        format!("remove {}: {e}", segment.path.display()),
                    )
                })?;
                continue;
            }
            if segment.start > upto {
                kept.push(segment);
                continue;
            }
            changed = true;
            let pos = segment.frames.partition_point(|f| f.index <= upto);
            let keep = &segment.frames[pos..];
            let new_start = keep[0].index;
            let old = fs::read(&segment.path).map_err(|e| {
                log_err(
                    ErrorVerb::Read,
                    format!("read {}: {e}", segment.path.display()),
                )
            })?;
            let mut bytes = Vec::with_capacity(old.len());
            let mut frames = Vec::with_capacity(keep.len());
            for frame in keep {
                let begin = frame.offset as usize;
                let end = begin + FRAME_HEADER_LEN + frame.payload_len as usize;
                frames.push(FrameLoc {
                    offset: bytes.len() as u64,
                    ..*frame
                });
                bytes.extend_from_slice(&old[begin..end]);
            }
            let new_path = segment_path(&log_dir, new_start);
            write_atomic(&new_path, &bytes)?;
            fs::remove_file(&segment.path).map_err(|e| {
                log_err(
                    ErrorVerb::Delete,
                    format!("remove {}: {e}", segment.path.display()),
                )
            })?;
            kept.push(Segment {
                start: new_start,
                path: new_path,
                bytes: bytes.len() as u64,
                frames,
            });
        }
        self.segments = kept;
        if changed {
            sync_dir(&log_dir).map_err(|e| {
                log_err(
                    ErrorVerb::Write,
                    format!("fsync {}: {e}", log_dir.display()),
                )
            })?;
        }
        self.rebuild_index();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
