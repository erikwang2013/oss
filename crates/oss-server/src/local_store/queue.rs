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
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};

use serde::{Deserialize, Serialize};

use super::{KIND_QUEUE, Result, encode_frame, ensure_dir, invalid_input, sync_dir, validate_name};

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
    /// While pending: earliest unix second the entry may be claimed again
    /// (`None` = immediately).
    pub retry_at: Option<i64>,
    /// Reason recorded by the last dead-letter transition.
    pub reason: Option<String>,
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
        /// Unix seconds the lease was taken. Defaulted for journals written
        /// before the outbox adapters needed it.
        #[serde(default)]
        claimed_at: i64,
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
    /// Pending with an explicit attempt count and earliest next claim time.
    Retry {
        seq: u64,
        attempts: u32,
        retry_at: i64,
    },
    /// Dead-letter with an explicit attempt count (bury).
    Bury {
        seq: u64,
        attempts: u32,
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
    claimed_at: i64,
    lease_until: Option<i64>,
    retry_at: Option<i64>,
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
        let shared = Arc::new(ext::load(&path, max_attempts)?);
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
                claimed_at: 0,
                lease_until: None,
                retry_at: None,
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
                (e.status == QueueStatus::Pending
                    && e.retry_at.is_none_or(|retry_at| retry_at <= now))
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
                    &Record::Bury {
                        seq,
                        attempts,
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
                    claimed_at: now,
                    lease_until,
                    attempts,
                },
            )?;
            let e = inner.entries.get_mut(&seq).expect("entry exists");
            e.attempts = attempts;
            e.status = QueueStatus::Claimed;
            e.token = Some(token.clone());
            e.claimed_at = now;
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
        let attempts = inner
            .entries
            .get(&seq)
            .map(|e| e.attempts)
            .expect("entry exists");
        write_record(
            &mut inner.file,
            &Record::Bury {
                seq,
                attempts,
                reason: reason.to_string(),
            },
        )?;
        let e = inner.entries.get_mut(&seq).expect("entry exists");
        e.status = QueueStatus::Dead;
        e.token = None;
        e.claimed_at = 0;
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
        let file = match ext::build_compacted(&tmp, inner.next_seq, &inner.entries) {
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

fn entry(seq: u64, e: &Entry) -> QueueEntry {
    QueueEntry {
        seq,
        id: e.id.clone(),
        payload: e.payload.clone(),
        attempts: e.attempts,
        status: e.status,
        lease_until: e.lease_until,
        retry_at: e.retry_at,
        reason: e.reason.clone(),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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

#[path = "queue_ext.rs"]
mod ext;
#[path = "queue_import.rs"]
mod import;

pub use import::QueueImport;

#[cfg(test)]
#[path = "queue_tests.rs"]
mod tests;
