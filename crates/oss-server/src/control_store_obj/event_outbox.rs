//! Local-file `event_outbox` adapter (design D4 / §7).
//!
//! One queue journal per node; `claimed_at` is both the claim time and the
//! lease token, and every guard requires the live token. Semantics mirror the
//! SQLite adapter: claims are ordered by id, attempts are counted by the caller
//! (retry/bury pass the new value), and expired leases are reclaimed before the
//! next claim. Attempts never auto-dead-letter here; bury is explicit.

use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;
use xl_store::control_store::event_outbox::{
    DeadEventRow, EventOutboxClaim, EventOutboxStore, EventOutboxTxn,
};

use super::local_ops::{
    EVENT_LEASE_SECS, EVENT_QUEUE, attempts_of, backend, decode, encode, limit_of, queue, seq_of,
    token, token_str,
};
use super::{ObjectControlStore, ObjectControlTxn};
use crate::local_store::QueueStatus;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StoredEvent {
    pub(crate) bucket: String,
    pub(crate) target_id: String,
    pub(crate) event_json: String,
    pub(crate) created_at: i64,
}

#[async_trait::async_trait]
impl EventOutboxStore for ObjectControlStore {
    async fn dead_events(&self, limit: i64) -> Result<Vec<DeadEventRow>, ControlError> {
        let queue = queue(self.local_store()?, EVENT_QUEUE)?;
        let mut rows = Vec::new();
        for entry in queue.scan() {
            if entry.status != QueueStatus::Dead {
                continue;
            }
            let stored: StoredEvent = decode(&entry.payload)?;
            rows.push(DeadEventRow {
                id: entry.seq as i64,
                bucket: stored.bucket,
                event_json: stored.event_json,
                attempts: i64::from(entry.attempts),
                last_error: entry.reason,
                created_at: stored.created_at,
            });
        }
        rows.sort_by_key(|row| std::cmp::Reverse(row.id));
        if let Some(limit) = limit_of(limit) {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl EventOutboxTxn for ObjectControlTxn {
    async fn event_enqueue(
        &mut self,
        bucket: &str,
        target_id: &str,
        event_json: &str,
        now: i64,
    ) -> Result<(), ControlError> {
        let queue = queue(self.local_store()?, EVENT_QUEUE)?;
        let payload = encode(&StoredEvent {
            bucket: bucket.to_string(),
            target_id: target_id.to_string(),
            event_json: event_json.to_string(),
            created_at: now,
        })?;
        queue.append("event", &payload).map_err(backend)?;
        Ok(())
    }

    async fn event_reclaim_expired(
        &mut self,
        now: i64,
        lease_secs: i64,
    ) -> Result<u64, ControlError> {
        queue(self.local_store()?, EVENT_QUEUE)?
            .reclaim_expired(now, lease_secs)
            .map_err(backend)
    }

    async fn event_claim(
        &mut self,
        now: i64,
        limit: i64,
    ) -> Result<Vec<EventOutboxClaim>, ControlError> {
        let queue = queue(self.local_store()?, EVENT_QUEUE)?;
        let token = token(now);
        let max = limit_of(limit).unwrap_or(usize::MAX);
        let mut rows = Vec::new();
        for _ in 0..max {
            let Some(claim) = queue
                .claim_exact(now, EVENT_LEASE_SECS, &token)
                .map_err(backend)?
            else {
                break;
            };
            let stored: StoredEvent = decode(&claim.payload)?;
            rows.push(EventOutboxClaim {
                id: claim.seq as i64,
                target_id: stored.target_id,
                event_json: stored.event_json,
                attempts: i64::from(claim.attempts),
                claimed_at: Some(now),
            });
        }
        Ok(rows)
    }

    async fn event_renew(
        &mut self,
        id: i64,
        old: Option<i64>,
        now: i64,
    ) -> Result<bool, ControlError> {
        let (Some(seq), Some(old)) = (seq_of(id), token_str(old)) else {
            return Ok(false);
        };
        queue(self.local_store()?, EVENT_QUEUE)?
            .renew(seq, &old, &token(now), now)
            .map_err(backend)
    }

    async fn event_finish_ok(&mut self, id: i64, token: Option<i64>) -> Result<bool, ControlError> {
        let (Some(seq), Some(token)) = (seq_of(id), token_str(token)) else {
            return Ok(false);
        };
        queue(self.local_store()?, EVENT_QUEUE)?
            .ack(seq, &token)
            .map_err(backend)
    }

    async fn event_retry(
        &mut self,
        id: i64,
        token: Option<i64>,
        attempts: i64,
        retry_at: i64,
        _last_error: &str,
    ) -> Result<bool, ControlError> {
        let (Some(seq), Some(token)) = (seq_of(id), token_str(token)) else {
            return Ok(false);
        };
        // `last_error` is only observable on dead rows; bury stores the reason.
        queue(self.local_store()?, EVENT_QUEUE)?
            .retry(seq, &token, attempts_of(attempts), retry_at)
            .map_err(backend)
    }

    async fn event_bury(
        &mut self,
        id: i64,
        token: Option<i64>,
        attempts: i64,
        last_error: &str,
    ) -> Result<bool, ControlError> {
        let (Some(seq), Some(token)) = (seq_of(id), token_str(token)) else {
            return Ok(false);
        };
        queue(self.local_store()?, EVENT_QUEUE)?
            .bury(seq, &token, attempts_of(attempts), last_error)
            .map_err(backend)
    }

    async fn event_redeliver(&mut self, id: i64, now: i64) -> Result<bool, ControlError> {
        let Some(seq) = seq_of(id) else {
            return Ok(false);
        };
        queue(self.local_store()?, EVENT_QUEUE)?
            .revive(seq, now)
            .map_err(backend)
    }
}
