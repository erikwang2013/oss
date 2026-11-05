//! Local-file `repl_outbox` adapter (design D4 / §7).
//!
//! Same lease/token model as the event outbox: `claimed_at` doubles as the
//! guard token, claims are ordered by `seq`, and expired claims are reclaimed
//! explicitly. Site rows are identified by `target_kind == "site"` plus
//! `remote_id`, mirroring the SQL predicates.

use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;
use xl_store::control_store::repl_outbox::{
    NewReplOutbox, ReplOutboxCounts, ReplOutboxRow, ReplOutboxStore, ReplOutboxTxn,
};

use super::local_ops::{
    REPL_LEASE_SECS, REPL_QUEUE, attempts_of, backend, decode, encode, limit_of, queue, seq_of,
    token, token_str,
};
use super::{ObjectControlStore, ObjectControlTxn};
use crate::local_store::{LocalQueue, QueueStatus};

#[derive(Debug, Serialize, Deserialize)]
struct StoredRepl {
    bucket: String,
    key: String,
    version_id: String,
    etag: String,
    size: i64,
    remote_id: String,
    dest_bucket: String,
    is_delete_marker: bool,
    is_version_delete: bool,
    target_kind: String,
    created_at: i64,
}

fn status_of(state: &str) -> Option<QueueStatus> {
    match state {
        "pending" => Some(QueueStatus::Pending),
        "sending" => Some(QueueStatus::Claimed),
        "dead" => Some(QueueStatus::Dead),
        _ => None,
    }
}

fn is_site(payload: &StoredRepl, peer_id: &str) -> bool {
    payload.target_kind == "site" && payload.remote_id == peer_id
}

fn row(seq: u64, attempts: u32, stored: StoredRepl, now: i64) -> ReplOutboxRow {
    ReplOutboxRow {
        seq: seq as i64,
        bucket: stored.bucket,
        key: stored.key,
        version_id: stored.version_id,
        etag: stored.etag,
        remote_id: stored.remote_id,
        dest_bucket: stored.dest_bucket,
        is_delete_marker: stored.is_delete_marker,
        is_version_delete: stored.is_version_delete,
        target_kind: stored.target_kind,
        attempts: i64::from(attempts),
        claimed_at: Some(now),
    }
}

#[async_trait::async_trait]
impl ReplOutboxStore for ObjectControlStore {
    async fn repl_version_counts(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ReplOutboxCounts, ControlError> {
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let mut counts = ReplOutboxCounts {
            pending: 0,
            sending: 0,
            dead: 0,
        };
        for entry in queue.scan() {
            let stored: StoredRepl = decode(&entry.payload)?;
            if stored.bucket != bucket || stored.key != key || stored.version_id != version_id {
                continue;
            }
            match entry.status {
                QueueStatus::Pending => counts.pending += 1,
                QueueStatus::Claimed => counts.sending += 1,
                QueueStatus::Dead => counts.dead += 1,
            }
        }
        Ok(counts)
    }

    async fn repl_earlier_inflight(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        before: i64,
    ) -> Result<i64, ControlError> {
        let Ok(before) = u64::try_from(before) else {
            return Ok(0);
        };
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let mut count = 0;
        for entry in queue.scan() {
            if entry.seq >= before
                || !matches!(entry.status, QueueStatus::Pending | QueueStatus::Claimed)
            {
                continue;
            }
            let stored: StoredRepl = decode(&entry.payload)?;
            if stored.bucket == bucket && stored.key == key && stored.version_id == version_id {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn repl_site_state_count(&self, peer_id: &str, state: &str) -> Result<i64, ControlError> {
        let Some(status) = status_of(state) else {
            return Ok(0);
        };
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let mut count = 0;
        for entry in queue.scan() {
            if entry.status != status {
                continue;
            }
            let stored: StoredRepl = decode(&entry.payload)?;
            if is_site(&stored, peer_id) {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn repl_site_total(&self, peer_id: &str) -> Result<i64, ControlError> {
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let mut count = 0;
        for entry in queue.scan() {
            let stored: StoredRepl = decode(&entry.payload)?;
            if is_site(&stored, peer_id) {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait::async_trait]
impl ReplOutboxTxn for ObjectControlTxn {
    async fn repl_enqueue(&mut self, new: &NewReplOutbox) -> Result<(), ControlError> {
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let payload = encode(&StoredRepl {
            bucket: new.bucket.clone(),
            key: new.key.clone(),
            version_id: new.version_id.clone(),
            etag: new.etag.clone(),
            size: new.size,
            remote_id: new.remote_id.clone(),
            dest_bucket: new.dest_bucket.clone(),
            is_delete_marker: new.is_delete_marker,
            is_version_delete: new.is_version_delete,
            target_kind: new.target_kind.clone(),
            created_at: new.created_at,
        })?;
        queue.append("repl", &payload).map_err(backend)?;
        Ok(())
    }

    async fn repl_reclaim_expired(
        &mut self,
        now: i64,
        lease_secs: i64,
    ) -> Result<u64, ControlError> {
        queue(self.local_store()?, REPL_QUEUE)?
            .reclaim_expired(now, lease_secs)
            .map_err(backend)
    }

    async fn repl_claim(
        &mut self,
        now: i64,
        limit: i64,
    ) -> Result<Vec<ReplOutboxRow>, ControlError> {
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let token = token(now);
        let max = limit_of(limit).unwrap_or(usize::MAX);
        let mut rows = Vec::new();
        for _ in 0..max {
            let Some(claim) = queue
                .claim_exact(now, REPL_LEASE_SECS, &token)
                .map_err(backend)?
            else {
                break;
            };
            let stored: StoredRepl = decode(&claim.payload)?;
            rows.push(row(claim.seq, claim.attempts, stored, now));
        }
        Ok(rows)
    }

    async fn repl_renew(
        &mut self,
        seq: i64,
        old: Option<i64>,
        now: i64,
    ) -> Result<bool, ControlError> {
        let (Some(seq), Some(old)) = (seq_of(seq), token_str(old)) else {
            return Ok(false);
        };
        queue(self.local_store()?, REPL_QUEUE)?
            .renew(seq, &old, &token(now), now)
            .map_err(backend)
    }

    async fn repl_finish_ok(&mut self, seq: i64, token: Option<i64>) -> Result<bool, ControlError> {
        let (Some(seq), Some(token)) = (seq_of(seq), token_str(token)) else {
            return Ok(false);
        };
        queue(self.local_store()?, REPL_QUEUE)?
            .ack(seq, &token)
            .map_err(backend)
    }

    async fn repl_retry(
        &mut self,
        seq: i64,
        token: Option<i64>,
        attempts: i64,
        retry_at: i64,
        _last_error: &str,
    ) -> Result<bool, ControlError> {
        let (Some(seq), Some(token)) = (seq_of(seq), token_str(token)) else {
            return Ok(false);
        };
        queue(self.local_store()?, REPL_QUEUE)?
            .retry(seq, &token, attempts_of(attempts), retry_at)
            .map_err(backend)
    }

    async fn repl_bury(
        &mut self,
        seq: i64,
        token: Option<i64>,
        attempts: i64,
        last_error: &str,
    ) -> Result<bool, ControlError> {
        let (Some(seq), Some(token)) = (seq_of(seq), token_str(token)) else {
            return Ok(false);
        };
        queue(self.local_store()?, REPL_QUEUE)?
            .bury(seq, &token, attempts_of(attempts), last_error)
            .map_err(backend)
    }

    async fn repl_drop_site(
        &mut self,
        peer_id: &str,
    ) -> Result<Vec<(String, String, String)>, ControlError> {
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let mut versions = Vec::new();
        let mut matched = Vec::new();
        for entry in queue.scan() {
            let stored: StoredRepl = decode(&entry.payload)?;
            if !is_site(&stored, peer_id) {
                continue;
            }
            if !stored.version_id.is_empty() {
                versions.push((stored.bucket.clone(), stored.key.clone(), stored.version_id));
            }
            matched.push(entry.seq);
        }
        remove_all(&queue, &matched)?;
        Ok(versions)
    }

    async fn repl_delete_site_all(&mut self, peer_id: &str) -> Result<u64, ControlError> {
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let mut matched = Vec::new();
        for entry in queue.scan() {
            let stored: StoredRepl = decode(&entry.payload)?;
            if is_site(&stored, peer_id) {
                matched.push(entry.seq);
            }
        }
        remove_all(&queue, &matched)
    }

    async fn repl_site_total(&mut self, peer_id: &str) -> Result<i64, ControlError> {
        let queue = queue(self.local_store()?, REPL_QUEUE)?;
        let mut count = 0;
        for entry in queue.scan() {
            let stored: StoredRepl = decode(&entry.payload)?;
            if is_site(&stored, peer_id) {
                count += 1;
            }
        }
        Ok(count)
    }
}

fn remove_all(queue: &LocalQueue, seqs: &[u64]) -> Result<u64, ControlError> {
    let mut removed = 0;
    for &seq in seqs {
        if queue.remove(seq).map_err(backend)? {
            removed += 1;
        }
    }
    Ok(removed)
}
