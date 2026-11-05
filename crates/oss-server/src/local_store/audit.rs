//! Daily rotated append-only audit files.
//!
//! `append(ts, data)` picks the UTC-day file from the caller timestamp. Writes are
//! buffered; every `BATCH` records (and on rotation, `flush`, `scan` or drop) the
//! buffer is flushed and, when fsync is enabled, synced. Audit is best-effort — a
//! crash can lose the current unflushed batch, never earlier batches.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use super::{
    KIND_AUDIT, Result, atomic_write, encode_frame, ensure_dir, invalid_input, open_private,
    read_repaired_tail, scan_frames, sync_dir,
};

const DAY_SECS: i64 = 86_400;
const BATCH: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub ts: i64,
    pub data: Vec<u8>,
}

struct State {
    day: Option<i64>,
    writer: Option<BufWriter<File>>,
    buffered: usize,
}

pub struct AuditLog {
    dir: PathBuf,
    fsync: bool,
    state: Mutex<State>,
}

impl AuditLog {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        ensure_dir(&dir)?;
        Ok(Self {
            dir,
            fsync: true,
            state: Mutex::new(State {
                day: None,
                writer: None,
                buffered: 0,
            }),
        })
    }

    /// Disables fsync-per-batch (still flushes to the OS on every batch).
    pub fn without_fsync(mut self) -> Self {
        self.fsync = false;
        self
    }

    /// Appends one record to the file for UTC day `ts / 86400`.
    pub fn append(&self, ts: i64, data: &[u8]) -> Result<()> {
        if ts < 0 {
            return Err(invalid_input("audit timestamp before unix epoch"));
        }
        let day = ts.div_euclid(DAY_SECS);
        let mut state = lock(&self.state);
        if state.day != Some(day) {
            flush_state(&mut state, self.fsync)?;
            let file = open_day(&self.dir, day)?;
            state.writer = Some(BufWriter::new(file));
            state.day = Some(day);
        }
        let mut payload = Vec::with_capacity(8 + data.len());
        payload.extend_from_slice(&ts.to_le_bytes());
        payload.extend_from_slice(data);
        {
            let writer = state
                .writer
                .as_mut()
                .ok_or_else(|| io::Error::other("audit writer missing"))?;
            writer.write_all(&encode_frame(KIND_AUDIT, &payload))?;
        }
        state.buffered += 1;
        if state.buffered >= BATCH {
            flush_state(&mut state, self.fsync)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let mut state = lock(&self.state);
        flush_state(&mut state, self.fsync)
    }

    /// Records with `from <= ts <= to` satisfying `filter`, in file/day order then
    /// append order. Flushes the current buffer first.
    pub fn scan<F>(&self, from: i64, to: i64, filter: F) -> Result<Vec<AuditRecord>>
    where
        F: Fn(&AuditRecord) -> bool,
    {
        self.flush()?;
        let mut files = day_files(&self.dir)?;
        files.sort_by_key(|(day, _)| *day);
        let mut out = Vec::new();
        for (_, path) in files {
            let bytes = fs::read(&path)?;
            let (frames, _) = scan_frames(&bytes, KIND_AUDIT)?;
            for payload in frames {
                let Some((ts, data)) = decode_payload(payload)? else {
                    continue;
                };
                if ts < from || ts > to {
                    continue;
                }
                let record = AuditRecord {
                    ts,
                    data: data.to_vec(),
                };
                if filter(&record) {
                    out.push(record);
                }
            }
        }
        Ok(out)
    }

    /// Deletes whole day files older than `max_age_secs` relative to `now`.
    /// The file currently open for appends is never removed.
    pub fn retain(&self, now: i64, max_age_secs: i64) -> Result<usize> {
        if max_age_secs < 0 {
            return Err(invalid_input("audit retention age must be >= 0"));
        }
        let cutoff = now.saturating_sub(max_age_secs).div_euclid(DAY_SECS);
        let mut state = lock(&self.state);
        flush_state(&mut state, self.fsync)?;
        let current = state.day;
        let mut removed = 0;
        for (day, path) in day_files(&self.dir)? {
            if day < cutoff && Some(day) != current {
                match fs::remove_file(&path) {
                    Ok(()) => removed += 1,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
        }
        if removed > 0 {
            sync_dir(&self.dir)?;
        }
        Ok(removed)
    }

    /// Deletes up to `limit` records with `ts < cutoff`, oldest first, rewriting
    /// the affected day files atomically. Returns the number removed. This is the
    /// row-level purge behind the `audit_delete_older_than(ts, limit)` trait; the
    /// whole-file [`AuditLog::retain`] stays available for age-based cleanup.
    ///
    /// shortcut: rewrites whole day files on every call; fine while retention keeps
    /// the audit corpus bounded. A concurrent process appending to the same
    /// namespace can lose its buffered records (audit assumes one writer per node).
    pub fn purge(&self, cutoff: i64, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let mut state = lock(&self.state);
        flush_state(&mut state, self.fsync)?;
        // Close the active day file so the rewrite below can replace it.
        state.writer = None;
        state.day = None;

        let mut files = day_files(&self.dir)?;
        files.sort_by_key(|(day, _)| *day);
        let mut parsed: Vec<Vec<(Option<i64>, Vec<u8>)>> = Vec::with_capacity(files.len());
        let mut candidates = Vec::new();
        for (file_index, (_, path)) in files.iter().enumerate() {
            let bytes = fs::read(path)?;
            let (frames, _) = scan_frames(&bytes, KIND_AUDIT)?;
            let mut records = Vec::with_capacity(frames.len());
            for (record_index, payload) in frames.into_iter().enumerate() {
                let ts = decode_payload(payload)?.map(|(ts, _)| ts);
                if let Some(ts) = ts
                    && ts < cutoff
                {
                    candidates.push((ts, file_index, record_index));
                }
                records.push((ts, payload.to_vec()));
            }
            parsed.push(records);
        }
        candidates.sort_by_key(|&(ts, file_index, record_index)| (ts, file_index, record_index));
        let removed = candidates.len().min(limit);
        if removed == 0 {
            return Ok(0);
        }
        let mut drop_flags: Vec<Vec<bool>> = parsed.iter().map(|r| vec![false; r.len()]).collect();
        for &(_, file_index, record_index) in &candidates[..removed] {
            drop_flags[file_index][record_index] = true;
        }
        for (file_index, (_, path)) in files.iter().enumerate() {
            if !drop_flags[file_index].iter().any(|&drop| drop) {
                continue;
            }
            let mut out = Vec::new();
            let mut kept = 0usize;
            for (record_index, (_, payload)) in parsed[file_index].iter().enumerate() {
                if drop_flags[file_index][record_index] {
                    continue;
                }
                out.extend_from_slice(&encode_frame(KIND_AUDIT, payload));
                kept += 1;
            }
            if kept == 0 {
                match fs::remove_file(path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            } else {
                atomic_write(path, &out)?;
            }
        }
        sync_dir(&self.dir)?;
        Ok(removed)
    }
}

impl Drop for AuditLog {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn flush_state(state: &mut State, fsync: bool) -> Result<()> {
    if let Some(writer) = state.writer.as_mut() {
        writer.flush()?;
        if fsync {
            writer.get_ref().sync_data()?;
        }
    }
    state.buffered = 0;
    Ok(())
}

fn day_path(dir: &Path, day: i64) -> PathBuf {
    dir.join(format!("{day:012}.log"))
}

fn open_day(dir: &Path, day: i64) -> Result<File> {
    let path = day_path(dir, day);
    let mut file = open_private(&path, true, true)?;
    read_repaired_tail(&mut file, KIND_AUDIT)?;
    sync_dir(dir)?;
    Ok(file)
}

fn day_files(dir: &Path) -> Result<Vec<(i64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".log") else {
            continue;
        };
        let Ok(day) = stem.parse::<i64>() else {
            continue;
        };
        if day >= 0 {
            out.push((day, entry.path()));
        }
    }
    Ok(out)
}

/// Payload is `i64 ts | data`. A frame shorter than the timestamp is ignored.
fn decode_payload(payload: &[u8]) -> Result<Option<(i64, &[u8])>> {
    if payload.len() < 8 {
        return Ok(None);
    }
    let (ts, data) = payload.split_at(8);
    Ok(Some((
        i64::from_le_bytes(ts.try_into().expect("8 bytes")),
        data,
    )))
}

#[cfg(test)]
mod tests {
    use super::super::LocalStore;

    fn audit(dir: &std::path::Path) -> super::AuditLog {
        LocalStore::open(dir).unwrap().audit("ops").unwrap()
    }

    #[test]
    fn batch_append_keeps_order() {
        let dir = tempfile::tempdir().unwrap();
        let log = audit(dir.path());
        for i in 0..150u32 {
            log.append(1_000 + i64::from(i), &i.to_le_bytes()).unwrap();
        }
        let records = log.scan(0, i64::MAX, |_| true).unwrap();
        assert_eq!(records.len(), 150);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.ts, 1_000 + i as i64);
            assert_eq!(
                u32::from_le_bytes(record.data[..4].try_into().unwrap()),
                i as u32
            );
        }
    }

    #[test]
    fn rotation_creates_daily_files() {
        let dir = tempfile::tempdir().unwrap();
        let log = audit(dir.path());
        let day1 = 10 * 86_400 + 5;
        let day2 = 11 * 86_400 + 5;
        log.append(day1, b"a").unwrap();
        log.append(day2, b"b").unwrap();
        log.flush().unwrap();
        let mut days: Vec<String> = std::fs::read_dir(dir.path().join("audit/ops"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        days.sort();
        assert_eq!(days, ["000000000010.log", "000000000011.log"]);
        let records = log.scan(0, i64::MAX, |_| true).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].data, b"a");
        assert_eq!(records[1].data, b"b");
    }

    #[test]
    fn scan_filters_by_range_and_predicate() {
        let dir = tempfile::tempdir().unwrap();
        let log = audit(dir.path());
        for i in 0..10 {
            log.append(1_000 + i, if i % 2 == 0 { b"keep" } else { b"drop" })
                .unwrap();
        }
        let records = log
            .scan(1_002, 1_006, |r| r.data.starts_with(b"keep"))
            .unwrap();
        let ts: Vec<_> = records.iter().map(|r| r.ts).collect();
        assert_eq!(ts, [1_002, 1_004, 1_006]);
    }

    #[test]
    fn retention_only_removes_old_files() {
        let dir = tempfile::tempdir().unwrap();
        let log = audit(dir.path());
        let day = |d: i64| d * 86_400 + 1;
        log.append(day(10), b"old").unwrap();
        log.append(day(18), b"new").unwrap();
        log.append(day(20), b"today").unwrap();
        log.flush().unwrap();
        assert_eq!(log.retain(day(20), 2 * 86_400).unwrap(), 1);
        let days: Vec<i64> = log
            .scan(0, i64::MAX, |_| true)
            .unwrap()
            .iter()
            .map(|r| r.ts / 86_400)
            .collect();
        assert_eq!(days, [18, 20]);
    }

    #[test]
    fn row_purge_removes_oldest_up_to_limit_and_keeps_appending() {
        let dir = tempfile::tempdir().unwrap();
        let log = audit(dir.path());
        let day = |d: i64| d * 86_400 + 1;
        log.append(day(10), b"a").unwrap();
        log.append(day(10), b"b").unwrap();
        log.append(day(11), b"c").unwrap();
        log.append(day(12), b"d").unwrap();

        assert_eq!(log.purge(day(12), 2).unwrap(), 2);
        let left: Vec<_> = log
            .scan(0, i64::MAX, |_| true)
            .unwrap()
            .into_iter()
            .map(|r| r.data)
            .collect();
        assert_eq!(left, [b"c".to_vec(), b"d".to_vec()]);

        log.append(day(12) + 1, b"e").unwrap();
        let left = log.scan(0, i64::MAX, |_| true).unwrap();
        assert_eq!(left.len(), 3);
        assert_eq!(left[2].data, b"e");
    }
}
