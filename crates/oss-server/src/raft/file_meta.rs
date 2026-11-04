//! Raft meta file: `<raft.db_path>.d/meta`.
//!
//! One small JSON document with the vote, committed index, last durable log id, purge bound and
//! the log format. It is replaced atomically (tmp + fsync + rename + dir fsync), so a crash leaves
//! either the old or the new document. The log entry bytes are made durable before this rename;
//! see [`super::file_log`]. After a crash the log may contain frames newer than `last_index`; those
//! orphan frames are ignored and truncated on the next open.

#![allow(clippy::result_large_err)]

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use openraft::{ErrorSubject, ErrorVerb, LogId, Vote};
use serde::{Deserialize, Serialize};

use super::store::storage_err;

/// Current on-disk raft log format. `1` is the legacy plain-JSON entry encoding; `2` is the
/// compressed-entry encoding written by this binary. Both can be read, only `2` is written.
pub(crate) const CURRENT_LOG_FORMAT: u32 = 2;
const READABLE_FORMATS: std::ops::RangeInclusive<u32> = 1..=CURRENT_LOG_FORMAT;

const META_FILE: &str = "meta";
const META_TMP: &str = "meta.tmp";

/// Durable Raft log metadata. `last_index` is the full [`LogId`] of the last durable entry (or the
/// purge bound when the log is empty), which carries the term as well as the index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMetaFile {
    pub vote: Option<Vote<u64>>,
    pub committed: Option<LogId<u64>>,
    pub last_index: Option<LogId<u64>>,
    pub last_purged: Option<LogId<u64>>,
    pub log_format: u32,
}

impl Default for RaftMetaFile {
    fn default() -> Self {
        Self {
            vote: None,
            committed: None,
            last_index: None,
            last_purged: None,
            log_format: CURRENT_LOG_FORMAT,
        }
    }
}

impl RaftMetaFile {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join(META_FILE)
    }

    /// Read and validate the meta file. `Ok(None)` means it does not exist yet (fresh directory).
    pub fn load(dir: &Path) -> Result<Option<Self>, io::Error> {
        let path = Self::path(dir);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let meta: Self = serde_json::from_slice(&bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt raft meta file {}: {e}", path.display()),
            )
        })?;
        meta.check_format()?;
        if let (Some(last), Some(purged)) = (meta.last_index, meta.last_purged)
            && last.index < purged.index
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "corrupt raft meta file {}: last_index {} is below last_purged {}",
                    path.display(),
                    last.index,
                    purged.index
                ),
            ));
        }
        Ok(Some(meta))
    }

    /// `1` and `2` are readable; anything else (including a future format) fails closed.
    pub fn check_format(&self) -> Result<(), io::Error> {
        let format = self.log_format;
        if READABLE_FORMATS.contains(&format) {
            return Ok(());
        }
        let hint = if format > CURRENT_LOG_FORMAT {
            "written by a newer oss-server; upgrade this binary"
        } else {
            "unknown raft log format"
        };
        Err(io::Error::other(format!(
            "unsupported raft log format {format}: {hint} (this binary reads 1..={CURRENT_LOG_FORMAT})"
        )))
    }

    /// Atomically replace the meta file: write `meta.tmp`, fsync it, rename over `meta`, fsync the
    /// directory. A crash before the rename leaves the previous meta untouched.
    pub fn store(&self, dir: &Path) -> Result<(), io::Error> {
        let path = Self::path(dir);
        let tmp = dir.join(META_TMP);
        let bytes = serde_json::to_vec(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        sync_dir(dir)
    }

    /// Store errors carry openraft's store subject so callers can surface them unchanged.
    pub(crate) fn store_err(&self, dir: &Path) -> Result<(), openraft::StorageError<u64>> {
        self.store(dir)
            .map_err(|e| storage_err(ErrorSubject::Store, ErrorVerb::Write, e))
    }
}

pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests;
