//! File-backed Raft snapshots under `<raft.db_path>.d/snapshot/<id>/`.
//!
//! A snapshot is a `manifest` plus `chunk-*` files. Chunks are raw 256 KiB slices of the snapshot
//! payload, byte-identical to the current SQLite `raft_snapshot_chunk.data` rows. The manifest is
//! written last (tmp + fsync + rename) and carries the [`SnapshotMeta`] and a CRC over every chunk
//! and over itself; a missing/corrupt manifest or chunk fails closed.
//!
//! openraft 0.9.25 ties snapshot persistence to `RaftStateMachine::get_current_snapshot` /
//! `install_snapshot` rather than exposing a separate snapshot-store trait. This type is the
//! storage layer those methods will use when the file state machine is wired in F2.

#![allow(clippy::result_large_err)]

use std::fs::{self, OpenOptions};
use std::io::{self, Cursor, Write};
use std::path::{Path, PathBuf};

use flate2::Crc;
use openraft::{BasicNode, ErrorSubject, ErrorVerb, Snapshot, SnapshotMeta, StorageError};
use serde::{Deserialize, Serialize};

use super::TypeConfig;
use super::file_log::raft_dir;
use super::file_meta::sync_dir;
use super::store::{SNAPSHOT_CHUNK_BYTES, storage_err};

const SNAPSHOT_DIR: &str = "snapshot";
const MANIFEST_FILE: &str = "manifest";
const MANIFEST_TMP: &str = "manifest.tmp";
const MANIFEST_MAGIC: &str = "OSSSNAP1";
const CHUNK_PREFIX: &str = "chunk-";
const TEMP_DIR_PREFIX: &str = ".tmp-";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChunkInfo {
    len: u64,
    crc32: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SnapshotManifest {
    magic: String,
    meta: SnapshotMeta<u64, BasicNode>,
    total_len: u64,
    chunk_size: u32,
    chunks: Vec<ChunkInfo>,
    /// CRC32 over the manifest JSON with this field zeroed.
    crc32: u32,
}

pub struct FileSnapshotStore {
    dir: PathBuf,
}

impl FileSnapshotStore {
    /// Open (and create) the snapshot directory for `db_path`.
    pub fn open(db_path: &Path) -> Result<Self, StorageError<u64>> {
        let dir = raft_dir(db_path).join(SNAPSHOT_DIR);
        fs::create_dir_all(&dir).map_err(|e| snap_err(ErrorVerb::Write, e))?;
        // Leftovers from an interrupted write are never visible snapshots.
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(TEMP_DIR_PREFIX)
                {
                    let _ = fs::remove_dir_all(entry.path());
                }
            }
        }
        Ok(Self { dir })
    }

    /// Persist a snapshot. Chunks and manifest are staged in a temporary directory and the whole
    /// directory is renamed into place, so readers never see a half-written snapshot.
    pub async fn write(
        &self,
        meta: &SnapshotMeta<u64, BasicNode>,
        bytes: &[u8],
    ) -> Result<(), StorageError<u64>> {
        check_id(&meta.snapshot_id)?;
        let final_dir = self.dir.join(&meta.snapshot_id);
        let mut tmp_name = std::ffi::OsString::from(TEMP_DIR_PREFIX);
        tmp_name.push(&meta.snapshot_id);
        let tmp_dir = self.dir.join(tmp_name);
        if tmp_dir.exists() {
            fs::remove_dir_all(&tmp_dir).map_err(|e| snap_err(ErrorVerb::Write, e))?;
        }
        fs::create_dir_all(&tmp_dir).map_err(|e| snap_err(ErrorVerb::Write, e))?;

        let mut chunks = Vec::new();
        for (seq, chunk) in bytes.chunks(SNAPSHOT_CHUNK_BYTES).enumerate() {
            let path = tmp_dir.join(chunk_name(seq));
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .map_err(|e| snap_err(ErrorVerb::Write, e))?;
            file.write_all(chunk)
                .map_err(|e| snap_err(ErrorVerb::Write, e))?;
            file.sync_all().map_err(|e| snap_err(ErrorVerb::Write, e))?;
            chunks.push(ChunkInfo {
                len: chunk.len() as u64,
                crc32: crc32(chunk),
            });
        }
        let mut manifest = SnapshotManifest {
            magic: MANIFEST_MAGIC.to_string(),
            meta: meta.clone(),
            total_len: bytes.len() as u64,
            chunk_size: SNAPSHOT_CHUNK_BYTES as u32,
            chunks,
            crc32: 0,
        };
        manifest.crc32 = manifest_crc(&manifest)?;
        write_manifest(&tmp_dir, &manifest)?;
        if final_dir.exists() {
            fs::remove_dir_all(&final_dir).map_err(|e| snap_err(ErrorVerb::Write, e))?;
        }
        fs::rename(&tmp_dir, &final_dir).map_err(|e| snap_err(ErrorVerb::Write, e))?;
        sync_dir(&self.dir).map_err(|e| snap_err(ErrorVerb::Write, e))
    }

    /// Snapshot metadata, oldest first. Corrupt snapshots fail closed instead of being skipped.
    pub async fn list(&self) -> Result<Vec<SnapshotMeta<u64, BasicNode>>, StorageError<u64>> {
        let entries = fs::read_dir(&self.dir).map_err(|e| snap_err(ErrorVerb::Read, e))?;
        let mut metas = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| snap_err(ErrorVerb::Read, e))?;
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.starts_with('.') {
                continue;
            }
            let manifest = load_manifest(&entry.path())?;
            if manifest.meta.snapshot_id != name {
                return Err(snap_corrupt(format!(
                    "manifest in {} names snapshot {:?}",
                    entry.path().display(),
                    manifest.meta.snapshot_id
                )));
            }
            metas.push(manifest.meta);
        }
        metas.sort_by_key(|meta| {
            (
                meta.last_log_id.map(|l| (l.index, l.leader_id.term)),
                meta.snapshot_id.clone(),
            )
        });
        Ok(metas)
    }

    /// Newest snapshot metadata (manifests only; no chunk reads).
    pub async fn latest_meta(
        &self,
    ) -> Result<Option<SnapshotMeta<u64, BasicNode>>, StorageError<u64>> {
        Ok(self.list().await?.pop())
    }

    /// Newest snapshot by last applied log id (then snapshot id).
    pub async fn latest(&self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let metas = self.list().await?;
        let Some(latest) = metas.last() else {
            return Ok(None);
        };
        self.read(&latest.snapshot_id).await
    }

    pub async fn read(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        check_id(snapshot_id)?;
        let dir = self.dir.join(snapshot_id);
        if !dir.is_dir() {
            return Ok(None);
        }
        let manifest = load_manifest(&dir)?;
        if manifest.meta.snapshot_id != snapshot_id {
            return Err(snap_corrupt(format!(
                "manifest in {} names snapshot {:?}, expected {snapshot_id:?}",
                dir.display(),
                manifest.meta.snapshot_id
            )));
        }
        let expected_chunks = manifest.total_len.div_ceil(SNAPSHOT_CHUNK_BYTES as u64) as usize;
        if manifest.chunks.len() != expected_chunks {
            return Err(snap_corrupt(format!(
                "manifest lists {} chunks, expected {expected_chunks}",
                manifest.chunks.len()
            )));
        }
        let mut data = Vec::with_capacity(manifest.total_len as usize);
        for (seq, info) in manifest.chunks.iter().enumerate() {
            let expected_len = chunk_len(manifest.total_len, seq);
            if info.len != expected_len {
                return Err(snap_corrupt(format!(
                    "chunk {seq} length {} != {expected_len}",
                    info.len
                )));
            }
            let path = dir.join(chunk_name(seq));
            let chunk = fs::read(&path)
                .map_err(|e| snap_corrupt(format!("read chunk {}: {e}", path.display())))?;
            if chunk.len() as u64 != info.len || crc32(&chunk) != info.crc32 {
                return Err(snap_corrupt(format!(
                    "chunk {} checksum/length mismatch",
                    path.display()
                )));
            }
            data.extend_from_slice(&chunk);
        }
        if data.len() as u64 != manifest.total_len {
            return Err(snap_corrupt(format!(
                "snapshot length {} != {}",
                data.len(),
                manifest.total_len
            )));
        }
        Ok(Some(Snapshot {
            meta: manifest.meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }

    /// Newest snapshot payload size from chunk file lengths (no chunk reads).
    /// Missing/corrupt state reports 0, matching the metrics gauge contract.
    pub async fn latest_bytes(&self) -> u64 {
        let Ok(metas) = self.list().await else {
            return 0;
        };
        let Some(latest) = metas.last() else {
            return 0;
        };
        let Ok(entries) = fs::read_dir(self.dir.join(&latest.snapshot_id)) else {
            return 0;
        };
        entries
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with(CHUNK_PREFIX))
            .filter_map(|e| e.metadata().ok().map(|m| m.len()))
            .sum()
    }

    pub async fn delete(&self, snapshot_id: &str) -> Result<(), StorageError<u64>> {
        check_id(snapshot_id)?;
        let dir = self.dir.join(snapshot_id);
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(|e| snap_err(ErrorVerb::Delete, e))?;
            sync_dir(&self.dir).map_err(|e| snap_err(ErrorVerb::Delete, e))?;
        }
        Ok(())
    }
}

fn chunk_name(seq: usize) -> String {
    format!("{CHUNK_PREFIX}{seq:06}")
}

fn chunk_len(total: u64, seq: usize) -> u64 {
    let chunk = SNAPSHOT_CHUNK_BYTES as u64;
    total.saturating_sub(seq as u64 * chunk).min(chunk)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = Crc::new();
    crc.update(bytes);
    crc.sum()
}

fn manifest_crc(manifest: &SnapshotManifest) -> Result<u32, StorageError<u64>> {
    let mut zeroed = manifest.clone();
    zeroed.crc32 = 0;
    let bytes = serde_json::to_vec(&zeroed).map_err(|e| snap_err(ErrorVerb::Write, e))?;
    Ok(crc32(&bytes))
}

fn write_manifest(dir: &Path, manifest: &SnapshotManifest) -> Result<(), StorageError<u64>> {
    let bytes = serde_json::to_vec(manifest).map_err(|e| snap_err(ErrorVerb::Write, e))?;
    let tmp = dir.join(MANIFEST_TMP);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .map_err(|e| snap_err(ErrorVerb::Write, e))?;
        file.write_all(&bytes)
            .map_err(|e| snap_err(ErrorVerb::Write, e))?;
        file.sync_all().map_err(|e| snap_err(ErrorVerb::Write, e))?;
    }
    fs::rename(&tmp, dir.join(MANIFEST_FILE)).map_err(|e| snap_err(ErrorVerb::Write, e))?;
    sync_dir(dir).map_err(|e| snap_err(ErrorVerb::Write, e))
}

fn load_manifest(dir: &Path) -> Result<SnapshotManifest, StorageError<u64>> {
    let path = dir.join(MANIFEST_FILE);
    let bytes =
        fs::read(&path).map_err(|e| snap_corrupt(format!("read {}: {e}", path.display())))?;
    let manifest: SnapshotManifest = serde_json::from_slice(&bytes)
        .map_err(|e| snap_corrupt(format!("parse {}: {e}", path.display())))?;
    if manifest.magic != MANIFEST_MAGIC {
        return Err(snap_corrupt(format!("{}: bad magic", path.display())));
    }
    if manifest.chunk_size as usize != SNAPSHOT_CHUNK_BYTES {
        return Err(snap_corrupt(format!(
            "{}: chunk size {} != {SNAPSHOT_CHUNK_BYTES}",
            path.display(),
            manifest.chunk_size
        )));
    }
    if manifest.crc32 != manifest_crc(&manifest)? {
        return Err(snap_corrupt(format!(
            "{}: manifest checksum mismatch",
            path.display()
        )));
    }
    Ok(manifest)
}

fn check_id(id: &str) -> Result<(), StorageError<u64>> {
    let valid = !id.is_empty()
        && id != "."
        && id != ".."
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(snap_err(
            ErrorVerb::Write,
            io::Error::other(format!("invalid raft snapshot id {id:?}")),
        ))
    }
}

fn snap_err(verb: ErrorVerb, e: impl std::error::Error + 'static) -> StorageError<u64> {
    storage_err(ErrorSubject::Snapshot(None), verb, e)
}

fn snap_corrupt(msg: impl std::fmt::Display) -> StorageError<u64> {
    snap_err(
        ErrorVerb::Read,
        io::Error::other(format!("corrupt raft snapshot: {msg}")),
    )
}

#[cfg(test)]
mod tests;
