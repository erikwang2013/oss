//! Append-only framed queue journal (event/replication outboxes, sync intents).
//!
//! Semantics: at-least-once delivery. `append` is durable before it returns;
//! `claim` leases an entry and returns a token; `ack` with the current token
//! removes it. A lease that expires is redelivered, and a stale token cannot ack.
//! Callers that need exactly-once deduplicate on their own id/token.
//! Replay happens on open: appended/claimed/dead records rebuild the live map,
//! acked records are gone. Compaction rewrites the journal with only live entries,
//! preserving pending, claimed (lease + attempts) and dead entries.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};

use serde::{Deserialize, Serialize};

use super::{
    KIND_QUEUE, Result, create_new_private, encode_frame, ensure_dir, invalid_input, open_private,
    read_repaired_tail, sync_dir, validate_name,
};

pub const DEFAULT_MAX_ATTEMPTS: u32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueStatus {
    Pending,
    Claimed,
    Dead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEntry {
    pub seq: u64,
    pub id: String,
    pub payload: Vec<u8>,
    pub attempts: u32,
    pub status: QueueStatus,
    /// Set while `status == Claimed`; unix seconds.
    pub lease_until: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub seq: u64,
    pub id: String,
    pub payload: Vec<u8>,
    pub token: String,
    pub attempts: u32,
    pub lease_until: i64,
}

#[derive(Debug, Serialize, Deserialize)]
enum Record {
    /// Written first by compaction so sequence numbers stay monotonic.
    Sequence {
        next_seq: u64,
    },
    Append {
        seq: u64,
        id: String,
        payload: Vec<u8>,
        attempts: u32,
    },
    Claim {
        seq: u64,
        token: String,
        lease_until: i64,
        attempts: u32,
    },
    Ack {
        seq: u64,
    },
    Dead {
        seq: u64,
        reason: String,
    },
}

#[derive(Debug)]
struct Entry {
    id: String,
    payload: Vec<u8>,
    attempts: u32,
    status: QueueStatus,
    token: Option<String>,
    lease_until: Option<i64>,
    reason: Option<String>,
}

struct Inner {
    file: File,
    entries: BTreeMap<u64, Entry>,
    next_seq: u64,
}

struct Shared {
    path: PathBuf,
    max_attempts: u32,
    inner: Mutex<Inner>,
}

/// One process-wide handle per journal path. `flock` excludes other processes;
/// this registry lets several handles inside one process share the same replay
/// state (and therefore serialize claims).
static OPEN_QUEUES: LazyLock<Mutex<HashMap<PathBuf, Weak<Shared>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub struct LocalQueue {
    shared: Arc<Shared>,
}

impl LocalQueue {
    pub fn open(dir: impl Into<PathBuf>, name: &str, max_attempts: u32) -> Result<Self> {
        validate_name(name)?;
        if max_attempts == 0 {
            return Err(invalid_input("queue max_attempts must be > 0"));
        }
        let dir = dir.into();
        ensure_dir(&dir)?;
        let path = dir.join(format!("{name}.log"));
        let registry_key = fs::canonicalize(&dir)?.join(format!("{name}.log"));

        let mut registry = lock(&OPEN_QUEUES);
        if let Some(shared) = registry.get(&registry_key).and_then(Weak::upgrade) {
            if shared.max_attempts != max_attempts {
                return Err(invalid_input(
                    "queue already open with a different max_attempts",
                ));
            }
            return Ok(Self { shared });
        }
        let shared = Arc::new(load(&path, max_attempts)?);
        registry.insert(registry_key, Arc::downgrade(&shared));
        Ok(Self { shared })
    }

    /// Appends a durable entry and returns its sequence number.
    pub fn append(&self, id: &str, payload: &[u8]) -> Result<u64> {
        if id.is_empty() {
            return Err(invalid_input("queue entry id must not be empty"));
        }
        let mut inner = lock(&self.shared.inner);
        let seq = inner.next_seq;
        write_record(
            &mut inner.file,
            &Record::Append {
                seq,
                id: id.to_string(),
                payload: payload.to_vec(),
                attempts: 0,
            },
        )?;
        inner.entries.insert(
            seq,
            Entry {
                id: id.to_string(),
                payload: payload.to_vec(),
                attempts: 0,
                status: QueueStatus::Pending,
                token: None,
                lease_until: None,
                reason: None,
            },
        );
        inner.next_seq = seq
            .checked_add(1)
            .ok_or_else(|| invalid_input("queue sequence overflow"))?;
        Ok(seq)
    }

    /// All entries still tracked (pending, claimed and dead), ordered by seq.
    /// Acked entries disappear and are also dropped by compaction.
    pub fn scan(&self) -> Vec<QueueEntry> {
        let inner = lock(&self.shared.inner);
        inner
            .entries
            .iter()
            .map(|(seq, e)| entry(*seq, e))
            .collect()
    }

    /// Unacked entries (pending + claimed), ordered by seq.
    pub fn pending(&self) -> Vec<QueueEntry> {
        let inner = lock(&self.shared.inner);
        inner
            .entries
            .iter()
            .filter(|(_, e)| e.status != QueueStatus::Dead)
            .map(|(seq, e)| entry(*seq, e))
            .collect()
    }

    /// Claims the oldest claimable entry, incrementing its attempt count.
    /// Entries past `max_attempts` are dead-lettered while scanning.
    pub fn claim(&self, now: i64, lease_secs: i64) -> Result<Option<Claim>> {
        if lease_secs <= 0 {
            return Err(invalid_input("queue lease_secs must be > 0"));
        }
        let max_attempts = self.shared.max_attempts;
        let mut inner = lock(&self.shared.inner);
        let due: Vec<u64> = inner
            .entries
            .iter()
            .filter(|(_, e)| {
                e.status == QueueStatus::Pending
                    || (e.status == QueueStatus::Claimed && e.lease_until.is_some_and(|u| u <= now))
            })
            .map(|(seq, _)| *seq)
            .collect();

        for seq in due {
            let Some(current) = inner.entries.get(&seq) else {
                continue;
            };
            let attempts = current.attempts.saturating_add(1);
            if attempts > max_attempts {
                let reason = format!("attempts exceeded {max_attempts}");
                write_record(
                    &mut inner.file,
                    &Record::Dead {
                        seq,
                        reason: reason.clone(),
                    },
                )?;
                let e = inner.entries.get_mut(&seq).expect("entry exists");
                e.attempts = attempts;
                e.status = QueueStatus::Dead;
                e.token = None;
                e.lease_until = None;
                e.reason = Some(reason);
                continue;
            }
            let token = uuid::Uuid::new_v4().to_string();
            let lease_until = now.saturating_add(lease_secs);
            write_record(
                &mut inner.file,
                &Record::Claim {
                    seq,
                    token: token.clone(),
                    lease_until,
                    attempts,
                },
            )?;
            let e = inner.entries.get_mut(&seq).expect("entry exists");
            e.attempts = attempts;
            e.status = QueueStatus::Claimed;
            e.token = Some(token.clone());
            e.lease_until = Some(lease_until);
            return Ok(Some(Claim {
                seq,
                id: e.id.clone(),
                payload: e.payload.clone(),
                token,
                attempts,
                lease_until,
            }));
        }
        Ok(None)
    }

    /// Removes an entry. Requires the token of the current lease, so a stale
    /// claimant cannot acknowledge a redelivered entry. Duplicate acks are
    /// idempotent and return `false`.
    pub fn ack(&self, seq: u64, token: &str) -> Result<bool> {
        let mut inner = lock(&self.shared.inner);
        let Some(e) = inner.entries.get(&seq) else {
            return Ok(false);
        };
        if e.status != QueueStatus::Claimed || e.token.as_deref() != Some(token) {
            return Ok(false);
        }
        write_record(&mut inner.file, &Record::Ack { seq })?;
        inner.entries.remove(&seq);
        Ok(true)
    }

    /// Manually dead-letters an entry (poison pill). Idempotent.
    pub fn dead(&self, seq: u64, reason: &str) -> Result<bool> {
        let mut inner = lock(&self.shared.inner);
        let Some(e) = inner.entries.get(&seq) else {
            return Ok(false);
        };
        if e.status == QueueStatus::Dead {
            return Ok(false);
        }
        write_record(
            &mut inner.file,
            &Record::Dead {
                seq,
                reason: reason.to_string(),
            },
        )?;
        let e = inner.entries.get_mut(&seq).expect("entry exists");
        e.status = QueueStatus::Dead;
        e.token = None;
        e.lease_until = None;
        e.reason = Some(reason.to_string());
        Ok(true)
    }

    /// Rewrites the journal with live entries only (acked history is dropped).
    /// Pending, claimed (with lease/token/attempts) and dead entries survive.
    pub fn compact(&self) -> Result<()> {
        static NEXT_TMP: AtomicU64 = AtomicU64::new(0);
        let mut inner = lock(&self.shared.inner);
        let dir = self
            .shared
            .path
            .parent()
            .ok_or_else(|| invalid_input("queue path without parent"))?;
        let tmp = dir.join(format!(
            ".compact-{}-{}",
            std::process::id(),
            NEXT_TMP.fetch_add(1, Ordering::Relaxed)
        ));
        let file = match build_compacted(&tmp, inner.next_seq, &inner.entries) {
            Ok(file) => file,
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                return Err(e);
            }
        };
        if let Err(e) = fs::rename(&tmp, &self.shared.path).and_then(|()| sync_dir(dir)) {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        inner.file = file;
        Ok(())
    }
}

fn build_compacted(path: &Path, next_seq: u64, entries: &BTreeMap<u64, Entry>) -> Result<File> {
    let mut file = create_new_private(path)?;
    write_record_buffered(&mut file, &Record::Sequence { next_seq })?;
    for (seq, e) in entries {
        write_record_buffered(
            &mut file,
            &Record::Append {
                seq: *seq,
                id: e.id.clone(),
                payload: e.payload.clone(),
                attempts: e.attempts,
            },
        )?;
        match e.status {
            QueueStatus::Pending => {}
            QueueStatus::Claimed => write_record_buffered(
                &mut file,
                &Record::Claim {
                    seq: *seq,
                    token: e.token.clone().unwrap_or_default(),
                    lease_until: e.lease_until.unwrap_or(0),
                    attempts: e.attempts,
                },
            )?,
            QueueStatus::Dead => write_record_buffered(
                &mut file,
                &Record::Dead {
                    seq: *seq,
                    reason: e.reason.clone().unwrap_or_default(),
                },
            )?,
        }
    }
    file.sync_all()?;
    // Lock the replacement inode before it becomes visible; the old journal
    // stays locked until the caller swaps this handle in.
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(|err| io::Error::new(io::Error::from(err).kind(), err.to_string()))?;
    Ok(file)
}

fn entry(seq: u64, e: &Entry) -> QueueEntry {
    QueueEntry {
        seq,
        id: e.id.clone(),
        payload: e.payload.clone(),
        attempts: e.attempts,
        status: e.status,
        lease_until: e.lease_until,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn load(path: &Path, max_attempts: u32) -> Result<Shared> {
    let mut file = open_private(path, true, true)?;
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
        |err| {
            let err: io::Error = err.into();
            io::Error::new(
                err.kind(),
                format!(
                    "queue {} is locked by another process: {err}",
                    path.display()
                ),
            )
        },
    )?;
    let bytes = read_repaired_tail(&mut file, KIND_QUEUE)?;
    sync_dir(path.parent().expect("queue path has parent"))?;
    let (frames, _) = super::scan_frames(&bytes, KIND_QUEUE)?;

    let mut entries = BTreeMap::new();
    let mut next_seq = 1u64;
    for frame in frames {
        let record: Record = rmp_serde::from_slice(frame)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        match record {
            Record::Sequence { next_seq: saved } => {
                next_seq = next_seq.max(saved);
            }
            Record::Append {
                seq,
                id,
                payload,
                attempts,
            } => {
                entries.insert(
                    seq,
                    Entry {
                        id,
                        payload,
                        attempts,
                        status: QueueStatus::Pending,
                        token: None,
                        lease_until: None,
                        reason: None,
                    },
                );
                next_seq = next_seq.max(seq.saturating_add(1));
            }
            Record::Claim {
                seq,
                token,
                lease_until,
                attempts,
            } => {
                if let Some(e) = entries.get_mut(&seq) {
                    e.status = QueueStatus::Claimed;
                    e.token = Some(token);
                    e.lease_until = Some(lease_until);
                    e.attempts = attempts;
                }
            }
            Record::Ack { seq } => {
                entries.remove(&seq);
            }
            Record::Dead { seq, reason } => {
                if let Some(e) = entries.get_mut(&seq) {
                    e.status = QueueStatus::Dead;
                    e.token = None;
                    e.lease_until = None;
                    e.reason = Some(reason);
                }
            }
        }
    }
    Ok(Shared {
        path: path.to_path_buf(),
        max_attempts,
        inner: Mutex::new(Inner {
            file,
            entries,
            next_seq,
        }),
    })
}

fn write_record(file: &mut File, record: &Record) -> Result<()> {
    write_record_buffered(file, record)?;
    file.sync_data()
}

fn write_record_buffered(file: &mut File, record: &Record) -> Result<()> {
    let body = rmp_serde::to_vec(record).map_err(io::Error::other)?;
    let frame = encode_frame(KIND_QUEUE, &body);
    // Roll back a short write so later appends stay replayable.
    let before = file.metadata()?.len();
    if let Err(e) = file.write_all(&frame) {
        let _ = file.set_len(before);
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
#[path = "queue_tests.rs"]
mod tests;
