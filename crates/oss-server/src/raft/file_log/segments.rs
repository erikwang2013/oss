//! Segment framing: scan/validate/truncate segment files and append frames.

#![allow(clippy::result_large_err)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use flate2::Crc;
use openraft::{ErrorVerb, StorageError};

use super::{FrameLoc, LOG_DIR, LogInner, Segment};
use crate::raft::file_meta::sync_dir;
use crate::raft::store::{log_corrupt, log_err};

pub(super) const FRAME_MAGIC: [u8; 4] = *b"OSLF";
pub(super) const FRAME_HEADER_LEN: usize = 4 + 4 + 4 + 8 + 8 + 8;
/// A single entry payload is bounded by a 24 MiB command; 64 MiB is the sanity cap.
const MAX_FRAME_PAYLOAD: u64 = 64 * 1024 * 1024;
/// Roll before a segment grows past this. A single oversized entry gets its own segment.
const SEGMENT_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// Also roll after this many frames so reads never scan an unbounded file.
pub(super) const SEGMENT_MAX_FRAMES: usize = 1024;
const SEGMENT_SUFFIX: &str = ".seg";

pub(super) fn segment_path(log_dir: &Path, start: u64) -> PathBuf {
    log_dir.join(format!("{start:020}{SEGMENT_SUFFIX}"))
}

/// Scan one segment, verifying magic/CRC/index continuity and truncating a torn tail.
/// Returns the good frames and, when truncated, the offset the file was cut at.
pub(super) fn scan_segment(
    path: &Path,
    start: u64,
) -> Result<(Vec<FrameLoc>, Option<u64>), StorageError<u64>> {
    let bytes = fs::read(path)
        .map_err(|e| log_err(ErrorVerb::Read, format!("read {}: {e}", path.display())))?;
    let mut frames = Vec::new();
    let mut offset = 0usize;
    let mut want = start;
    let mut torn_at = None;
    while offset < bytes.len() {
        if bytes.len() - offset < FRAME_HEADER_LEN {
            torn_at = Some(offset as u64);
            break;
        }
        let payload_len =
            u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().expect("4 bytes")) as u64;
        let stored_crc =
            u32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().expect("4 bytes"));
        let index =
            u64::from_le_bytes(bytes[offset + 12..offset + 20].try_into().expect("8 bytes"));
        let term = u64::from_le_bytes(bytes[offset + 20..offset + 28].try_into().expect("8 bytes"));
        let node = u64::from_le_bytes(bytes[offset + 28..offset + 36].try_into().expect("8 bytes"));
        // Any malformed/torn frame at the scanned tail is the crash tail: truncate from here.
        if bytes[offset..offset + 4] != FRAME_MAGIC || payload_len > MAX_FRAME_PAYLOAD {
            torn_at = Some(offset as u64);
            break;
        }
        let frame_len = FRAME_HEADER_LEN as u64 + payload_len;
        if ((bytes.len() - offset) as u64) < frame_len {
            torn_at = Some(offset as u64);
            break;
        }
        let end = offset + frame_len as usize;
        let payload = &bytes[offset + FRAME_HEADER_LEN..end];
        if payload_crc(index, term, node, payload) != stored_crc {
            torn_at = Some(offset as u64);
            break;
        }
        if index != want {
            return Err(log_corrupt(format!(
                "segment {} frame at offset {offset} has index {index}, expected {want}",
                path.display()
            )));
        }
        frames.push(FrameLoc {
            index,
            term,
            node,
            offset: offset as u64,
            payload_len: payload_len as u32,
        });
        offset = end;
        want = want
            .checked_add(1)
            .ok_or_else(|| log_corrupt("raft log index overflow"))?;
    }
    if let Some(torn) = torn_at {
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| log_err(ErrorVerb::Write, format!("open {}: {e}", path.display())))?;
        file.set_len(torn).map_err(|e| {
            log_err(
                ErrorVerb::Write,
                format!("truncate torn tail {}: {e}", path.display()),
            )
        })?;
        file.sync_all()
            .map_err(|e| log_err(ErrorVerb::Write, format!("fsync {}: {e}", path.display())))?;
    }
    Ok((frames, torn_at))
}

pub(super) fn segment_files(log_dir: &Path) -> Result<Vec<(u64, PathBuf)>, StorageError<u64>> {
    let dir = fs::read_dir(log_dir).map_err(|e| {
        log_err(
            ErrorVerb::Read,
            format!("read_dir {}: {e}", log_dir.display()),
        )
    })?;
    let mut out = Vec::new();
    for entry in dir {
        let entry = entry.map_err(|e| log_err(ErrorVerb::Read, format!("read_dir entry: {e}")))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(SEGMENT_SUFFIX) else {
            continue;
        };
        let start: u64 = stem.parse().map_err(|_| {
            log_err(
                ErrorVerb::Read,
                format!("unexpected raft log segment file {name}"),
            )
        })?;
        out.push((start, entry.path()));
    }
    out.sort_by_key(|(start, _)| *start);
    if let Some(pair) = out.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(log_corrupt(format!(
            "duplicate raft log segment start {}",
            pair[0].0
        )));
    }
    Ok(out)
}

fn payload_crc(index: u64, term: u64, node: u64, payload: &[u8]) -> u32 {
    let mut crc = Crc::new();
    crc.update(&index.to_le_bytes());
    crc.update(&term.to_le_bytes());
    crc.update(&node.to_le_bytes());
    crc.update(payload);
    crc.sum()
}

pub(super) fn encode_frame(index: u64, term: u64, node: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    out.extend_from_slice(&FRAME_MAGIC);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload_crc(index, term, node, payload).to_le_bytes());
    out.extend_from_slice(&index.to_le_bytes());
    out.extend_from_slice(&term.to_le_bytes());
    out.extend_from_slice(&node.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// tmp + fsync + rename (+ dir fsync): used for segment rewrites during purge.
pub(super) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), StorageError<u64>> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .map_err(|e| log_err(ErrorVerb::Write, format!("create {}: {e}", tmp.display())))?;
        file.write_all(bytes)
            .map_err(|e| log_err(ErrorVerb::Write, format!("write {}: {e}", tmp.display())))?;
        file.sync_all()
            .map_err(|e| log_err(ErrorVerb::Write, format!("fsync {}: {e}", tmp.display())))?;
    }
    fs::rename(&tmp, path).map_err(|e| {
        log_err(
            ErrorVerb::Write,
            format!("rename {} -> {}: {e}", tmp.display(), path.display()),
        )
    })
}

pub(super) fn write_segment_frames(
    inner: &mut LogInner,
    dir: &Path,
    frames: &[(u64, u64, u64, Vec<u8>)],
) -> Result<(), StorageError<u64>> {
    let log_dir = dir.join(LOG_DIR);
    let mut touched: Vec<PathBuf> = Vec::new();
    let mut created = false;
    for (index, term, node, payload) in frames {
        if payload.len() as u64 > MAX_FRAME_PAYLOAD {
            return Err(log_err(
                ErrorVerb::Write,
                format!("raft log entry {index} is {} bytes", payload.len()),
            ));
        }
        let frame = encode_frame(*index, *term, *node, payload);
        let roll = match inner.segments.last() {
            None => true,
            Some(segment) => {
                !segment.frames.is_empty()
                    && (segment.bytes + frame.len() as u64 > SEGMENT_MAX_BYTES
                        || segment.frames.len() >= SEGMENT_MAX_FRAMES)
            }
        };
        let si = if roll {
            let path = segment_path(&log_dir, *index);
            if path.exists() {
                return Err(log_err(
                    ErrorVerb::Write,
                    format!("raft log segment {} already exists", path.display()),
                ));
            }
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)
                .map_err(|e| {
                    log_err(ErrorVerb::Write, format!("create {}: {e}", path.display()))
                })?;
            created = true;
            inner.segments.push(Segment {
                start: *index,
                path: path.clone(),
                bytes: 0,
                frames: Vec::new(),
            });
            touched.push(path);
            inner.segments.len() - 1
        } else {
            let si = inner.segments.len() - 1;
            let path = inner.segments[si].path.clone();
            if !touched.contains(&path) {
                touched.push(path);
            }
            si
        };
        let segment = &mut inner.segments[si];
        let offset = segment.bytes;
        let mut file = OpenOptions::new()
            .append(true)
            .open(&segment.path)
            .map_err(|e| {
                log_err(
                    ErrorVerb::Write,
                    format!("open {}: {e}", segment.path.display()),
                )
            })?;
        file.write_all(&frame).map_err(|e| {
            log_err(
                ErrorVerb::Write,
                format!("append {}: {e}", segment.path.display()),
            )
        })?;
        segment.bytes += frame.len() as u64;
        segment.frames.push(FrameLoc {
            index: *index,
            term: *term,
            node: *node,
            offset,
            payload_len: payload.len() as u32,
        });
    }
    for path in &touched {
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| log_err(ErrorVerb::Write, format!("open {}: {e}", path.display())))?;
        file.sync_all()
            .map_err(|e| log_err(ErrorVerb::Write, format!("fsync {}: {e}", path.display())))?;
    }
    if created {
        sync_dir(&log_dir).map_err(|e| {
            log_err(
                ErrorVerb::Write,
                format!("fsync {}: {e}", log_dir.display()),
            )
        })?;
    }
    Ok(())
}
