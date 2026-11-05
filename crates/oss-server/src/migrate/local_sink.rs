//! Copy SQLite node-local operational rows into the D4 local store.
//!
//! Every write is idempotent: existing uploads/KV/cursors are compared by value
//! and a difference fails closed, queues import at explicit sequences, and audit
//! rows are matched by id. A rerun after an interruption therefore either
//! completes the missing records or proves the sink already matches.

use std::collections::BTreeMap;

use crate::control_store_obj::ObjectControlStore;
use crate::control_store_obj::migrate_support::{
    AUDIT_META_KV, AUDIT_NS, EVENT_LEASE_SECS, EVENT_QUEUE, NEXT_ID_KEY, REPL_LEASE_SECS,
    REPL_QUEUE, SYNC_DELETE_KV, SYNC_WRITE_KV, StoredAudit, StoredEvent, StoredRepl, decode_local,
    encode_local,
};
use crate::local_store::{LocalQueue, QueueStatus};
use crate::local_store::{UploadRecord, UploadStore};

use super::local_rows::{SourceDead, SourceEvent, SourceRepl};
use super::source::{BoxError, Source, error};

pub(crate) const DEAD_QUEUE: &str = "site_apply_dead";
pub(crate) const USAGE_KV: &str = "account_usage";
pub(crate) const INSTANT_KV: &str = "instant_index";

#[derive(Debug, Default)]
pub(crate) struct LocalStats {
    pub uploads: u64,
    pub audit: u64,
    pub events: u64,
    pub repls: u64,
    pub write_intents: u64,
    pub delete_intents: u64,
    pub cursors: u64,
    pub dead: u64,
    pub usage: u64,
    pub instant: u64,
    pub warnings: Vec<String>,
}

/// Apply every local row. Durable-local writes are not rolled back with a root
/// transaction, so this runs before the root CAS and must stay idempotent.
pub(crate) fn apply(control: &ObjectControlStore, source: &Source) -> Result<LocalStats, BoxError> {
    let local = control
        .local_store()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let rows = &source.local;
    let mut stats = LocalStats::default();

    let uploads = local.uploads()?;
    for record in &rows.uploads {
        put_upload(&uploads, record)?;
        stats.uploads += 1;
    }

    let audit = local.audit(AUDIT_NS)?;
    let mut existing = BTreeMap::new();
    for record in audit.scan(0, i64::MAX, |_| true)? {
        let row: StoredAudit = decode_local(&record.data)?;
        existing.insert(row.id, row);
    }
    let mut max_audit = existing.keys().copied().max().unwrap_or(0);
    for row in &rows.audit {
        if row.principal_account.is_some() || row.target_account.is_some() {
            stats.warnings.push(format!(
                "audit_log id {}: principal_account/target_account dropped (no local field)",
                row.id
            ));
        }
        let stored = StoredAudit {
            id: row.id,
            ts: row.ts,
            uid: row.uid,
            op: row.op.clone(),
            bucket: row.bucket.clone(),
            object_key: row.object_key.clone(),
            decision: row.decision.clone(),
            detail: row.detail.clone(),
        };
        max_audit = max_audit.max(row.id);
        match existing.get(&row.id) {
            Some(have) if *have == stored => {}
            Some(_) => {
                return error(format!(
                    "audit_log id {} already holds a different record",
                    row.id
                ));
            }
            None => audit.append(row.ts, &encode_local(&stored)?)?,
        }
        stats.audit += 1;
    }
    let meta = local.kv(AUDIT_META_KV)?;
    let reserved = match meta.get(NEXT_ID_KEY)? {
        Some(bytes) => {
            let bytes: [u8; 8] = bytes
                .try_into()
                .map_err(|_| std::io::Error::other("corrupt audit id counter"))?;
            i64::from_le_bytes(bytes)
        }
        None => 0,
    };
    let next = reserved.max(max_audit.saturating_add(1)).max(1);
    if reserved != next || meta.get(NEXT_ID_KEY)?.is_none() {
        meta.put(NEXT_ID_KEY, &next.to_le_bytes())?;
    }
    audit.flush()?;

    let event_queue = local.queue(EVENT_QUEUE)?;
    for row in &rows.events {
        import_event(&event_queue, row)?;
        stats.events += 1;
    }
    let repl_queue = local.queue(REPL_QUEUE)?;
    for row in &rows.repls {
        import_repl(&repl_queue, row)?;
        stats.repls += 1;
    }

    let write_kv = local.kv(SYNC_WRITE_KV)?;
    for row in &rows.write_intents {
        put_identical(&write_kv, &row.id, &encode_local(row)?)?;
        stats.write_intents += 1;
    }
    let delete_kv = local.kv(SYNC_DELETE_KV)?;
    for row in &rows.delete_intents {
        put_identical(&delete_kv, &row.id, &encode_local(row)?)?;
        stats.delete_intents += 1;
    }

    for row in &rows.cursors {
        let cursor = local.cursor(&row.peer_id)?;
        match cursor.load()? {
            Some(have) if have.last_seq == row.last_seq && have.updated_at == row.updated_at => {}
            Some(_) => {
                return error(format!(
                    "site cursor {} already holds a different position",
                    row.peer_id
                ));
            }
            None => cursor.store(row.last_seq, row.updated_at)?,
        }
        stats.cursors += 1;
    }

    let dead_queue = local.queue(DEAD_QUEUE)?;
    for (offset, row) in rows.dead.iter().enumerate() {
        let seq = u64::try_from(offset + 1)
            .map_err(|_| std::io::Error::other("dead-letter archive overflow"))?;
        import_dead(&dead_queue, seq, row)?;
        stats.dead += 1;
    }

    let usage_kv = local.kv(USAGE_KV)?;
    for row in &rows.usage {
        put_identical(&usage_kv, &row.account_id, &encode_local(row)?)?;
        stats.usage += 1;
    }
    let instant_kv = local.kv(INSTANT_KV)?;
    for row in &rows.instant {
        put_identical(&instant_kv, &row.k, &encode_local(row)?)?;
        stats.instant += 1;
    }
    Ok(stats)
}

fn put_upload(store: &UploadStore, record: &UploadRecord) -> Result<(), BoxError> {
    match store.get(&record.saved_path)? {
        Some(have) if have == *record => Ok(()),
        Some(_) => error(format!(
            "upload {} already holds a different record",
            record.saved_path
        )),
        None => {
            store.put_if_absent(record)?;
            match store.get(&record.saved_path)? {
                Some(have) if have == *record => Ok(()),
                _ => error(format!("upload {} write did not stick", record.saved_path)),
            }
        }
    }
}

fn put_identical(
    kv: &crate::local_store::LocalKv,
    key: &str,
    value: &[u8],
) -> Result<(), BoxError> {
    match kv.get(key)? {
        Some(have) if have == value => Ok(()),
        Some(_) => error(format!("local key {key} already holds a different value")),
        None => {
            kv.put_if_absent(key, value)?;
            match kv.get(key)? {
                Some(have) if have == value => Ok(()),
                _ => error(format!("local key {key} write did not stick")),
            }
        }
    }
}

fn status(state: &str, seq: i64) -> Result<QueueStatus, BoxError> {
    match state {
        "pending" => Ok(QueueStatus::Pending),
        "sending" => Ok(QueueStatus::Claimed),
        "dead" => Ok(QueueStatus::Dead),
        other => error(format!("outbox sequence {seq} has unknown state {other:?}")),
    }
}

fn import_event(queue: &LocalQueue, row: &SourceEvent) -> Result<(), BoxError> {
    let payload = encode_local(&StoredEvent {
        bucket: row.bucket.clone(),
        target_id: row.target_id.clone(),
        event_json: row.event_json.clone(),
        created_at: row.created_at,
    })?;
    let status = status(&row.state, row.id)?;
    let claimed = row.claimed_at.unwrap_or(0);
    queue.import_entry(&crate::local_store::queue::QueueImport {
        seq: u64::try_from(row.id).map_err(|_| std::io::Error::other("negative event id"))?,
        id: "event",
        payload: &payload,
        attempts: u32::try_from(row.attempts.max(0))
            .map_err(|_| std::io::Error::other("event attempts overflow"))?,
        status,
        token: &claimed.to_string(),
        claimed_at: claimed,
        lease_until: claimed.saturating_add(EVENT_LEASE_SECS),
        retry_at: (row.next_retry_at > 0).then_some(row.next_retry_at),
        reason: row.last_error.as_deref().unwrap_or(""),
    })?;
    Ok(())
}

fn import_repl(queue: &LocalQueue, row: &SourceRepl) -> Result<(), BoxError> {
    let payload = encode_local(&StoredRepl {
        bucket: row.bucket.clone(),
        key: row.key.clone(),
        version_id: row.version_id.clone(),
        etag: row.etag.clone(),
        size: row.size,
        remote_id: row.remote_id.clone(),
        dest_bucket: row.dest_bucket.clone(),
        is_delete_marker: row.is_delete_marker,
        is_version_delete: row.is_version_delete,
        target_kind: row.target_kind.clone(),
        created_at: row.created_at,
    })?;
    let status = status(&row.state, row.seq)?;
    let claimed = row.claimed_at.unwrap_or(0);
    queue.import_entry(&crate::local_store::queue::QueueImport {
        seq: u64::try_from(row.seq).map_err(|_| std::io::Error::other("negative repl seq"))?,
        id: "repl",
        payload: &payload,
        attempts: u32::try_from(row.attempts.max(0))
            .map_err(|_| std::io::Error::other("repl attempts overflow"))?,
        status,
        token: &claimed.to_string(),
        claimed_at: claimed,
        lease_until: claimed.saturating_add(REPL_LEASE_SECS),
        retry_at: (row.next_retry_at > 0).then_some(row.next_retry_at),
        reason: row.last_error.as_deref().unwrap_or(""),
    })?;
    Ok(())
}

fn import_dead(queue: &LocalQueue, seq: u64, row: &SourceDead) -> Result<(), BoxError> {
    let payload = encode_local(row)?;
    queue.import_entry(&crate::local_store::queue::QueueImport {
        seq,
        id: "site_apply_dead",
        payload: &payload,
        attempts: 1,
        status: QueueStatus::Dead,
        token: "",
        claimed_at: 0,
        lease_until: 0,
        retry_at: None,
        reason: &row.error,
    })?;
    Ok(())
}

/// Read every local file back and compare it with the source rows.
pub(crate) fn verify(
    control: &ObjectControlStore,
    source: &Source,
) -> Result<LocalStats, BoxError> {
    let local = control
        .local_store()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let rows = &source.local;
    let mut stats = LocalStats::default();

    let uploads = local.uploads()?;
    for record in &rows.uploads {
        match uploads.get(&record.saved_path)? {
            Some(have) if have == *record => {}
            _ => return error(format!("upload {} missing after apply", record.saved_path)),
        }
        stats.uploads += 1;
    }

    let audit = local.audit(AUDIT_NS)?;
    let mut audit_rows = BTreeMap::new();
    for record in audit.scan(0, i64::MAX, |_| true)? {
        let row: StoredAudit = decode_local(&record.data)?;
        audit_rows.insert(row.id, row);
    }
    for row in &rows.audit {
        let want = StoredAudit {
            id: row.id,
            ts: row.ts,
            uid: row.uid,
            op: row.op.clone(),
            bucket: row.bucket.clone(),
            object_key: row.object_key.clone(),
            decision: row.decision.clone(),
            detail: row.detail.clone(),
        };
        if audit_rows.get(&row.id) != Some(&want) {
            return error(format!("audit_log id {} missing after apply", row.id));
        }
        stats.audit += 1;
    }
    let meta = local.kv(AUDIT_META_KV)?;
    let next = meta
        .get(NEXT_ID_KEY)?
        .and_then(|b| <[u8; 8]>::try_from(b).ok())
        .map(i64::from_le_bytes)
        .unwrap_or(0);
    if next <= rows.audit.iter().map(|r| r.id).max().unwrap_or(0) {
        return error(format!(
            "audit id counter {next} does not exceed imported ids"
        ));
    }

    let event_queue = local.queue(EVENT_QUEUE)?;
    verify_queue(&event_queue, &rows.events, "event")?;
    stats.events = rows.events.len() as u64;
    let repl_queue = local.queue(REPL_QUEUE)?;
    verify_repl_queue(&repl_queue, &rows.repls)?;
    stats.repls = rows.repls.len() as u64;

    let write_kv = local.kv(SYNC_WRITE_KV)?;
    for row in &rows.write_intents {
        if write_kv.get(&row.id)?.as_deref() != Some(&encode_local(row)?[..]) {
            return error(format!("sync write intent {} missing after apply", row.id));
        }
        stats.write_intents += 1;
    }
    let delete_kv = local.kv(SYNC_DELETE_KV)?;
    for row in &rows.delete_intents {
        if delete_kv.get(&row.id)?.as_deref() != Some(&encode_local(row)?[..]) {
            return error(format!("sync delete intent {} missing after apply", row.id));
        }
        stats.delete_intents += 1;
    }
    for row in &rows.cursors {
        match local.cursor(&row.peer_id)?.load()? {
            Some(have) if have.last_seq == row.last_seq && have.updated_at == row.updated_at => {}
            _ => return error(format!("site cursor {} mismatch after apply", row.peer_id)),
        }
        stats.cursors += 1;
    }

    let dead_queue = local.queue(DEAD_QUEUE)?;
    let dead = dead_queue.scan();
    if dead.len() != rows.dead.len() {
        return error(format!(
            "dead-letter archive has {} rows, sqlite has {}",
            dead.len(),
            rows.dead.len()
        ));
    }
    for (offset, row) in rows.dead.iter().enumerate() {
        let want = encode_local(row)?;
        let seq = u64::try_from(offset + 1)
            .map_err(|_| std::io::Error::other("dead-letter archive overflow"))?;
        match dead.iter().find(|e| e.seq == seq) {
            Some(entry)
                if entry.status == QueueStatus::Dead
                    && entry.payload == want
                    && entry.reason.as_deref() == Some(row.error.as_str()) => {}
            _ => return error(format!("dead-letter archive row {seq} mismatch")),
        }
        stats.dead += 1;
    }

    let usage_kv = local.kv(USAGE_KV)?;
    for row in &rows.usage {
        if usage_kv.get(&row.account_id)?.as_deref() != Some(&encode_local(row)?[..]) {
            return error(format!(
                "account usage {} missing after apply",
                row.account_id
            ));
        }
        stats.usage += 1;
    }
    let instant_kv = local.kv(INSTANT_KV)?;
    for row in &rows.instant {
        if instant_kv.get(&row.k)?.as_deref() != Some(&encode_local(row)?[..]) {
            return error(format!("instant index {} missing after apply", row.k));
        }
        stats.instant += 1;
    }
    Ok(stats)
}

fn verify_queue(queue: &LocalQueue, rows: &[SourceEvent], id: &str) -> Result<(), BoxError> {
    let entries = queue.scan();
    if entries.len() != rows.len() {
        return error(format!(
            "event outbox has {} entries, sqlite has {}",
            entries.len(),
            rows.len()
        ));
    }
    for row in rows {
        let want = encode_local(&StoredEvent {
            bucket: row.bucket.clone(),
            target_id: row.target_id.clone(),
            event_json: row.event_json.clone(),
            created_at: row.created_at,
        })?;
        let status = status(&row.state, row.id)?;
        let seq = u64::try_from(row.id).map_err(|_| std::io::Error::other("negative event id"))?;
        match entries.iter().find(|e| e.seq == seq) {
            Some(entry)
                if entry.id == id
                    && entry.payload == want
                    && entry.status == status
                    && entry.attempts == u32::try_from(row.attempts.max(0)).unwrap_or(u32::MAX) => {
            }
            _ => return error(format!("event outbox sequence {seq} mismatch")),
        }
    }
    Ok(())
}

fn verify_repl_queue(queue: &LocalQueue, rows: &[SourceRepl]) -> Result<(), BoxError> {
    let entries = queue.scan();
    if entries.len() != rows.len() {
        return error(format!(
            "repl outbox has {} entries, sqlite has {}",
            entries.len(),
            rows.len()
        ));
    }
    for row in rows {
        let want = encode_local(&StoredRepl {
            bucket: row.bucket.clone(),
            key: row.key.clone(),
            version_id: row.version_id.clone(),
            etag: row.etag.clone(),
            size: row.size,
            remote_id: row.remote_id.clone(),
            dest_bucket: row.dest_bucket.clone(),
            is_delete_marker: row.is_delete_marker,
            is_version_delete: row.is_version_delete,
            target_kind: row.target_kind.clone(),
            created_at: row.created_at,
        })?;
        let status = status(&row.state, row.seq)?;
        let seq = u64::try_from(row.seq).map_err(|_| std::io::Error::other("negative repl seq"))?;
        match entries.iter().find(|e| e.seq == seq) {
            Some(entry)
                if entry.id == "repl"
                    && entry.payload == want
                    && entry.status == status
                    && entry.attempts == u32::try_from(row.attempts.max(0)).unwrap_or(u32::MAX) => {
            }
            _ => return error(format!("repl outbox sequence {seq} mismatch")),
        }
    }
    Ok(())
}
