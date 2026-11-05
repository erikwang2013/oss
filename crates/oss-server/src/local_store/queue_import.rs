//! Offline queue import (#G1): restore journal entries at explicit sequences.

use super::{Entry, LocalQueue, QueueStatus, Record, Result, invalid_input, lock, write_record};

/// One journal entry restored by offline migration (#G1). `token` is required
/// for `Claimed` (the SQL `claimed_at` token); `claimed_at`/`lease_until` are
/// the claim timestamps; `retry_at` applies to pending entries.
pub struct QueueImport<'a> {
    pub seq: u64,
    pub id: &'a str,
    pub payload: &'a [u8],
    pub attempts: u32,
    pub status: QueueStatus,
    pub token: &'a str,
    pub claimed_at: i64,
    pub lease_until: i64,
    pub retry_at: Option<i64>,
    pub reason: &'a str,
}

impl LocalQueue {
    /// Import an entry at an explicit sequence. Idempotent for an identical
    /// existing entry; a conflicting record at the same sequence fails closed.
    /// A sequence below `next_seq` with no live entry was already acked and is
    /// skipped. Returns whether a record was written.
    pub fn import_entry(&self, entry: &QueueImport<'_>) -> Result<bool> {
        if entry.id.is_empty() {
            return Err(invalid_input("queue entry id must not be empty"));
        }
        if entry.status == QueueStatus::Claimed && entry.token.is_empty() {
            return Err(invalid_input("imported claim token must not be empty"));
        }
        let mut inner = lock(&self.shared.inner);
        if let Some(e) = inner.entries.get(&entry.seq) {
            let same = e.id == entry.id
                && e.payload == entry.payload
                && e.attempts == entry.attempts
                && e.status == entry.status
                && match entry.status {
                    QueueStatus::Pending => e.retry_at == entry.retry_at,
                    QueueStatus::Claimed => {
                        e.token.as_deref() == Some(entry.token)
                            && e.claimed_at == entry.claimed_at
                            && e.lease_until == Some(entry.lease_until)
                    }
                    QueueStatus::Dead => e.reason.as_deref() == Some(entry.reason),
                };
            return if same {
                Ok(false)
            } else {
                Err(invalid_input(format!(
                    "queue sequence {} already holds a different entry",
                    entry.seq
                )))
            };
        }
        if entry.seq < inner.next_seq {
            return Ok(false);
        }
        write_record(
            &mut inner.file,
            &Record::Append {
                seq: entry.seq,
                id: entry.id.to_string(),
                payload: entry.payload.to_vec(),
                attempts: entry.attempts,
            },
        )?;
        match entry.status {
            QueueStatus::Pending => {
                if let Some(retry_at) = entry.retry_at {
                    write_record(
                        &mut inner.file,
                        &Record::Retry {
                            seq: entry.seq,
                            attempts: entry.attempts,
                            retry_at,
                        },
                    )?;
                }
            }
            QueueStatus::Claimed => write_record(
                &mut inner.file,
                &Record::Claim {
                    seq: entry.seq,
                    token: entry.token.to_string(),
                    claimed_at: entry.claimed_at,
                    lease_until: entry.lease_until,
                    attempts: entry.attempts,
                },
            )?,
            QueueStatus::Dead => write_record(
                &mut inner.file,
                &Record::Bury {
                    seq: entry.seq,
                    attempts: entry.attempts,
                    reason: entry.reason.to_string(),
                },
            )?,
        }
        inner.entries.insert(
            entry.seq,
            Entry {
                id: entry.id.to_string(),
                payload: entry.payload.to_vec(),
                attempts: entry.attempts,
                status: entry.status,
                token: (entry.status == QueueStatus::Claimed).then(|| entry.token.to_string()),
                claimed_at: entry.claimed_at,
                lease_until: (entry.status == QueueStatus::Claimed).then_some(entry.lease_until),
                retry_at: if entry.status == QueueStatus::Pending {
                    entry.retry_at
                } else {
                    None
                },
                reason: (entry.status == QueueStatus::Dead).then(|| entry.reason.to_string()),
            },
        );
        inner.next_seq = inner.next_seq.max(
            entry
                .seq
                .checked_add(1)
                .ok_or_else(|| invalid_input("queue sequence overflow"))?,
        );
        Ok(true)
    }
}
