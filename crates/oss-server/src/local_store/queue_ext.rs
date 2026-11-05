//! Extended queue operations for the D3 local outbox adapters.
//!
//! The base journal in `queue.rs` is worker-owned: `claim` increments attempts
//! and dead-letters past `max_attempts`. The SQL outbox traits instead use a
//! caller-supplied `claimed_at` token, caller-counted attempts and explicit
//! bury, so these methods add that mode without changing the base semantics.
//!
//! Replay and compaction live here too to keep `queue.rs` under the file cap.

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::Path;

use super::super::{
    KIND_QUEUE, create_new_private, open_private, read_repaired_tail, scan_frames, sync_dir,
};
use super::{
    Claim, Entry, Inner, LocalQueue, QueueStatus, Record, Shared, lock, write_record,
    write_record_buffered,
};

pub(super) fn load(path: &Path, max_attempts: u32) -> io::Result<Shared> {
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
    let (frames, _) = scan_frames(&bytes, KIND_QUEUE)?;

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
                        claimed_at: 0,
                        lease_until: None,
                        retry_at: None,
                        reason: None,
                    },
                );
                next_seq = next_seq.max(seq.saturating_add(1));
            }
            Record::Claim {
                seq,
                token,
                claimed_at,
                lease_until,
                attempts,
            } => {
                if let Some(e) = entries.get_mut(&seq) {
                    e.status = QueueStatus::Claimed;
                    e.token = Some(token);
                    e.claimed_at = claimed_at;
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
                    e.claimed_at = 0;
                    e.lease_until = None;
                    e.reason = Some(reason);
                }
            }
            Record::Retry {
                seq,
                attempts,
                retry_at,
            } => {
                if let Some(e) = entries.get_mut(&seq) {
                    e.status = QueueStatus::Pending;
                    e.attempts = attempts;
                    e.token = None;
                    e.claimed_at = 0;
                    e.lease_until = None;
                    e.retry_at = Some(retry_at);
                }
            }
            Record::Bury {
                seq,
                attempts,
                reason,
            } => {
                if let Some(e) = entries.get_mut(&seq) {
                    e.status = QueueStatus::Dead;
                    e.attempts = attempts;
                    e.token = None;
                    e.claimed_at = 0;
                    e.lease_until = None;
                    e.reason = Some(reason);
                }
            }
        }
    }
    Ok(Shared {
        path: path.to_path_buf(),
        max_attempts,
        inner: std::sync::Mutex::new(Inner {
            file,
            entries,
            next_seq,
        }),
    })
}

pub(super) fn build_compacted(
    path: &Path,
    next_seq: u64,
    entries: &BTreeMap<u64, Entry>,
) -> io::Result<File> {
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
            QueueStatus::Pending => {
                if let Some(retry_at) = e.retry_at {
                    write_record_buffered(
                        &mut file,
                        &Record::Retry {
                            seq: *seq,
                            attempts: e.attempts,
                            retry_at,
                        },
                    )?;
                }
            }
            QueueStatus::Claimed => write_record_buffered(
                &mut file,
                &Record::Claim {
                    seq: *seq,
                    token: e.token.clone().unwrap_or_default(),
                    claimed_at: e.claimed_at,
                    lease_until: e.lease_until.unwrap_or(0),
                    attempts: e.attempts,
                },
            )?,
            QueueStatus::Dead => write_record_buffered(
                &mut file,
                &Record::Bury {
                    seq: *seq,
                    attempts: e.attempts,
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

impl LocalQueue {
    /// Claims the oldest pending entry due at `now`, without incrementing the
    /// attempt counter, auto-dead-lettering or considering expired leases (the
    /// caller runs [`LocalQueue::reclaim_expired`] first). `token` is stored as
    /// the lease token and guards every later transition, mirroring the SQL
    /// outbox `claimed_at` token.
    pub fn claim_exact(&self, now: i64, lease_secs: i64, token: &str) -> io::Result<Option<Claim>> {
        if lease_secs <= 0 {
            return Err(super::invalid_input("queue lease_secs must be > 0"));
        }
        if token.is_empty() {
            return Err(super::invalid_input("queue claim token must not be empty"));
        }
        let mut inner = lock(&self.shared.inner);
        let Some(seq) = inner
            .entries
            .iter()
            .find(|(_, e)| {
                e.status == QueueStatus::Pending && e.retry_at.is_none_or(|retry| retry <= now)
            })
            .map(|(seq, _)| *seq)
        else {
            return Ok(None);
        };
        let (attempts, id, payload) = {
            let e = inner.entries.get(&seq).expect("entry exists");
            (e.attempts, e.id.clone(), e.payload.clone())
        };
        let lease_until = now.saturating_add(lease_secs);
        write_record(
            &mut inner.file,
            &Record::Claim {
                seq,
                token: token.to_string(),
                claimed_at: now,
                lease_until,
                attempts,
            },
        )?;
        let e = inner.entries.get_mut(&seq).expect("entry exists");
        e.status = QueueStatus::Claimed;
        e.token = Some(token.to_string());
        e.claimed_at = now;
        e.lease_until = Some(lease_until);
        Ok(Some(Claim {
            seq,
            id,
            payload,
            token: token.to_string(),
            attempts,
            lease_until,
        }))
    }

    /// Rotates the lease token and pushes the lease end by the same delta as
    /// the claim time (`now` is the new `claimed_at`). Stale tokens fail.
    pub fn renew(&self, seq: u64, old_token: &str, new_token: &str, now: i64) -> io::Result<bool> {
        let mut inner = lock(&self.shared.inner);
        let (attempts, claimed_at, old_lease) = match inner.entries.get(&seq) {
            Some(e)
                if e.status == QueueStatus::Claimed && e.token.as_deref() == Some(old_token) =>
            {
                (e.attempts, e.claimed_at, e.lease_until)
            }
            _ => return Ok(false),
        };
        let lease_until = old_lease
            .map(|lease| lease.saturating_add(now.saturating_sub(claimed_at)))
            .unwrap_or(now);
        write_record(
            &mut inner.file,
            &Record::Claim {
                seq,
                token: new_token.to_string(),
                claimed_at: now,
                lease_until,
                attempts,
            },
        )?;
        let e = inner.entries.get_mut(&seq).expect("entry exists");
        e.token = Some(new_token.to_string());
        e.claimed_at = now;
        e.lease_until = Some(lease_until);
        Ok(true)
    }

    /// Token-guarded failure path: back to pending with the caller's attempt
    /// count and earliest retry time.
    pub fn retry(&self, seq: u64, token: &str, attempts: u32, retry_at: i64) -> io::Result<bool> {
        let mut inner = lock(&self.shared.inner);
        let Some(e) = inner.entries.get(&seq) else {
            return Ok(false);
        };
        if e.status != QueueStatus::Claimed || e.token.as_deref() != Some(token) {
            return Ok(false);
        }
        write_record(
            &mut inner.file,
            &Record::Retry {
                seq,
                attempts,
                retry_at,
            },
        )?;
        let e = inner.entries.get_mut(&seq).expect("entry exists");
        e.status = QueueStatus::Pending;
        e.attempts = attempts;
        e.token = None;
        e.claimed_at = 0;
        e.lease_until = None;
        e.retry_at = Some(retry_at);
        Ok(true)
    }

    /// Token-guarded terminal failure: dead with the caller's attempt count.
    pub fn bury(&self, seq: u64, token: &str, attempts: u32, reason: &str) -> io::Result<bool> {
        let mut inner = lock(&self.shared.inner);
        let Some(e) = inner.entries.get(&seq) else {
            return Ok(false);
        };
        if e.status != QueueStatus::Claimed || e.token.as_deref() != Some(token) {
            return Ok(false);
        }
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
        e.attempts = attempts;
        e.token = None;
        e.claimed_at = 0;
        e.lease_until = None;
        e.reason = Some(reason.to_string());
        Ok(true)
    }

    /// Dead -> pending with a reset attempt count and no error (redeliver).
    pub fn revive(&self, seq: u64, now: i64) -> io::Result<bool> {
        let mut inner = lock(&self.shared.inner);
        let Some(e) = inner.entries.get(&seq) else {
            return Ok(false);
        };
        if e.status != QueueStatus::Dead {
            return Ok(false);
        }
        write_record(
            &mut inner.file,
            &Record::Retry {
                seq,
                attempts: 0,
                retry_at: now,
            },
        )?;
        let e = inner.entries.get_mut(&seq).expect("entry exists");
        e.status = QueueStatus::Pending;
        e.attempts = 0;
        e.token = None;
        e.claimed_at = 0;
        e.lease_until = None;
        e.retry_at = Some(now);
        e.reason = None;
        Ok(true)
    }

    /// Moves claims older than `now - lease_secs` back to pending (attempts are
    /// preserved) and returns the number reclaimed, matching the SQL reclaim.
    pub fn reclaim_expired(&self, now: i64, lease_secs: i64) -> io::Result<u64> {
        if lease_secs < 0 {
            return Err(super::invalid_input("queue lease_secs must be >= 0"));
        }
        let cutoff = now.saturating_sub(lease_secs);
        let mut inner = lock(&self.shared.inner);
        let expired: Vec<u64> = inner
            .entries
            .iter()
            .filter(|(_, e)| e.status == QueueStatus::Claimed && e.claimed_at < cutoff)
            .map(|(seq, _)| *seq)
            .collect();
        let mut reclaimed = 0u64;
        for seq in expired {
            let attempts = inner
                .entries
                .get(&seq)
                .map(|e| e.attempts)
                .expect("entry exists");
            write_record(
                &mut inner.file,
                &Record::Retry {
                    seq,
                    attempts,
                    retry_at: now,
                },
            )?;
            let e = inner.entries.get_mut(&seq).expect("entry exists");
            e.status = QueueStatus::Pending;
            e.token = None;
            e.claimed_at = 0;
            e.lease_until = None;
            e.retry_at = Some(now);
            reclaimed += 1;
        }
        Ok(reclaimed)
    }

    /// Force-removes an entry regardless of state or token (SQL DELETE).
    pub fn remove(&self, seq: u64) -> io::Result<bool> {
        let mut inner = lock(&self.shared.inner);
        if !inner.entries.contains_key(&seq) {
            return Ok(false);
        }
        write_record(&mut inner.file, &Record::Ack { seq })?;
        inner.entries.remove(&seq);
        Ok(true)
    }
}
