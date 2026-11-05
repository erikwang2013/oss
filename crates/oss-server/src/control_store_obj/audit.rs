//! Local-file `audit_log` adapter (design D4 / §7).
//!
//! Rows are serialized into the daily audit files; ids come from a persistent
//! KV counter so they keep increasing after a purge, like `AUTOINCREMENT`.
//! Query filtering and `id DESC` ordering happen in memory.
//!
//! shortcut: one audit writer per node (the local store assumes it); a second
//! process appending while a purge rewrites a day file can lose buffered rows.
//! Audit timestamps must be >= 0 because the local day-file format uses unix
//! seconds to select the file.

use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;
use xl_store::control_store::audit::{
    AuditQuery, AuditRecord, AuditStore, AuditTxn, NewAuditRecord,
};

use super::local_ops::{backend, decode, encode, kv, limit_of};
use super::{ObjectControlStore, ObjectControlTxn};
use crate::local_store::{AuditLog, LocalKv};

pub(crate) const AUDIT_NS: &str = "audit_log";
pub(crate) const AUDIT_META_KV: &str = "audit_meta";
pub(crate) const NEXT_ID_KEY: &str = "next_id";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredAudit {
    pub(crate) id: i64,
    pub(crate) ts: i64,
    pub(crate) uid: i64,
    pub(crate) op: String,
    pub(crate) bucket: Option<String>,
    pub(crate) object_key: Option<String>,
    pub(crate) decision: String,
    pub(crate) detail: Option<String>,
}

fn audit_log(local: &crate::local_store::LocalStore) -> Result<AuditLog, ControlError> {
    local.audit(AUDIT_NS).map_err(backend)
}

fn matches(query: &AuditQuery, row: &StoredAudit) -> bool {
    let eq = |want: &Option<String>, have: &str| want.as_ref().is_none_or(|want| want == have);
    eq(&query.decision, &row.decision)
        && eq(&query.op, &row.op)
        && query
            .bucket
            .as_ref()
            .is_none_or(|want| row.bucket.as_deref() == Some(want.as_str()))
        && query.uid.is_none_or(|uid| uid == row.uid)
        // SQL compares a byte prefix; a NULL object_key never matches.
        && query.key_prefix.as_ref().is_none_or(|prefix| {
            row.object_key
                .as_deref()
                .is_some_and(|key| key.starts_with(prefix))
        })
}

#[async_trait::async_trait]
impl AuditStore for ObjectControlStore {
    async fn audit_page(&self, query: &AuditQuery) -> Result<Vec<AuditRecord>, ControlError> {
        let log = audit_log(self.local_store()?)?;
        let from = query.from.unwrap_or(i64::MIN);
        let to = query.to.unwrap_or(i64::MAX);
        let mut rows = Vec::new();
        for record in log.scan(from, to, |_| true).map_err(backend)? {
            let row: StoredAudit = decode(&record.data)?;
            if matches(query, &row) {
                rows.push(AuditRecord {
                    id: row.id,
                    ts: row.ts,
                    uid: row.uid,
                    op: row.op,
                    bucket: row.bucket,
                    object_key: row.object_key,
                    decision: row.decision,
                    detail: row.detail,
                });
            }
        }
        rows.sort_by_key(|row| std::cmp::Reverse(row.id));
        if let Some(limit) = limit_of(query.limit) {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl AuditTxn for ObjectControlTxn {
    async fn audit_insert_batch(&mut self, rows: &[NewAuditRecord]) -> Result<(), ControlError> {
        if rows.is_empty() {
            return Ok(());
        }
        let local = self.local_store()?;
        let log = audit_log(local)?;
        let meta = kv(local, AUDIT_META_KV)?;
        let first = next_id(&meta, &log)?;
        let count = i64::try_from(rows.len())
            .map_err(|_| ControlError::Backend("audit batch too large".into()))?;
        // Reserve the range before appending: a crash may leave a gap but can
        // never let a later batch reuse an id that is already in a day file.
        let after = first
            .checked_add(count)
            .ok_or_else(|| ControlError::Backend("audit id overflow".into()))?;
        meta.put(NEXT_ID_KEY, &after.to_le_bytes())
            .map_err(backend)?;
        for (offset, new) in rows.iter().enumerate() {
            let id = first + offset as i64;
            let data = encode(&StoredAudit {
                id,
                ts: new.ts,
                uid: new.uid,
                op: new.op.clone(),
                bucket: new.bucket.clone(),
                object_key: new.object_key.clone(),
                decision: new.decision.clone(),
                detail: new.detail.clone(),
            })?;
            log.append(new.ts, &data).map_err(backend)?;
        }
        Ok(())
    }

    async fn audit_delete_older_than(
        &mut self,
        cutoff: i64,
        limit: i64,
    ) -> Result<u64, ControlError> {
        let log = audit_log(self.local_store()?)?;
        let limit = limit.max(1) as usize;
        Ok(log.purge(cutoff, limit).map_err(backend)? as u64)
    }
}

/// Next id, recovering from existing files when the counter is missing (e.g. a
/// log written before the counter existed).
fn next_id(meta: &LocalKv, log: &AuditLog) -> Result<i64, ControlError> {
    if let Some(bytes) = meta.get(NEXT_ID_KEY).map_err(backend)? {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| ControlError::Backend("corrupt audit id counter".into()))?;
        return Ok(i64::from_le_bytes(bytes).max(1));
    }
    let mut max = 0;
    for record in log.scan(0, i64::MAX, |_| true).map_err(backend)? {
        let row: StoredAudit = decode(&record.data)?;
        max = max.max(row.id);
    }
    Ok(max.saturating_add(1))
}
