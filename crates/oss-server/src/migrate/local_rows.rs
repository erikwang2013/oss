//! Node-local SQLite rows (outboxes, audit, uploads, cursors, archives).

use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::sqlite::{SqliteConnection, SqliteRow};

use crate::control_store_obj::{StoredDeleteIntent, StoredWriteIntent};
use crate::local_store::UploadRecord;

pub(crate) struct LocalRows {
    pub uploads: Vec<UploadRecord>,
    pub audit: Vec<SourceAudit>,
    pub events: Vec<SourceEvent>,
    pub repls: Vec<SourceRepl>,
    pub write_intents: Vec<StoredWriteIntent>,
    pub delete_intents: Vec<StoredDeleteIntent>,
    pub cursors: Vec<SourceCursor>,
    pub dead: Vec<SourceDead>,
    pub usage: Vec<SourceUsage>,
    pub instant: Vec<SourceInstant>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SourceAudit {
    pub id: i64,
    pub ts: i64,
    pub uid: i64,
    pub op: String,
    pub bucket: Option<String>,
    pub object_key: Option<String>,
    pub decision: String,
    pub detail: Option<String>,
    pub principal_account: Option<String>,
    pub target_account: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceEvent {
    pub id: i64,
    pub bucket: String,
    pub target_id: String,
    pub event_json: String,
    pub state: String,
    pub attempts: i64,
    pub claimed_at: Option<i64>,
    pub next_retry_at: i64,
    pub created_at: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceRepl {
    pub seq: i64,
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub etag: String,
    pub size: i64,
    pub remote_id: String,
    pub dest_bucket: String,
    pub is_delete_marker: bool,
    pub state: String,
    pub attempts: i64,
    pub claimed_at: Option<i64>,
    pub next_retry_at: i64,
    pub created_at: i64,
    pub last_error: Option<String>,
    pub target_kind: String,
    pub is_version_delete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceCursor {
    pub peer_id: String,
    pub last_seq: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SourceDead {
    pub peer_id: String,
    pub seq: i64,
    pub entity: String,
    pub entity_id: String,
    pub error: String,
    pub payload: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SourceUsage {
    pub account_id: String,
    pub used_bytes: i64,
    pub object_count: i64,
    pub complete: bool,
    pub computed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SourceInstant {
    pub k: String,
    pub v: String,
    pub expires_at: i64,
}

fn get<'r, T>(row: &'r SqliteRow, column: &str, default: T) -> T
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(column).unwrap_or(default)
}

pub(crate) async fn read_local(
    conn: &mut SqliteConnection,
) -> Result<LocalRows, super::source::BoxError> {
    let mut out = LocalRows {
        uploads: Vec::new(),
        audit: Vec::new(),
        events: Vec::new(),
        repls: Vec::new(),
        write_intents: Vec::new(),
        delete_intents: Vec::new(),
        cursors: Vec::new(),
        dead: Vec::new(),
        usage: Vec::new(),
        instant: Vec::new(),
    };
    for r in sqlx::query("SELECT * FROM uploads ORDER BY saved_path")
        .fetch_all(&mut *conn)
        .await?
    {
        out.uploads.push(UploadRecord {
            saved_path: r.get("saved_path"),
            bucket: r.get("bucket"),
            key: r.get("key"),
            group_name: r.get("group_name"),
            size: r.get("size"),
            etag: r.get("etag"),
            original_name: r.get("original_name"),
            owner_uid: r.get("owner_id"),
            created_at: r.get("created_at"),
            expires_at: None,
        });
    }
    for r in sqlx::query("SELECT * FROM audit_log ORDER BY id")
        .fetch_all(&mut *conn)
        .await?
    {
        out.audit.push(SourceAudit {
            id: r.get("id"),
            ts: r.get("ts"),
            uid: r.get("uid"),
            op: r.get("op"),
            bucket: r.get("bucket"),
            object_key: r.get("object_key"),
            decision: r.get("decision"),
            detail: r.get("detail"),
            principal_account: get(&r, "principal_account", None),
            target_account: get(&r, "target_account", None),
        });
    }
    for r in sqlx::query("SELECT * FROM event_outbox ORDER BY id")
        .fetch_all(&mut *conn)
        .await?
    {
        out.events.push(SourceEvent {
            id: r.get("id"),
            bucket: r.get("bucket"),
            target_id: r.get("target_id"),
            event_json: r.get("event_json"),
            state: r.get("state"),
            attempts: r.get("attempts"),
            claimed_at: r.get("claimed_at"),
            next_retry_at: get(&r, "next_retry_at", 0),
            created_at: get(&r, "created_at", 0),
            last_error: r.get("last_error"),
        });
    }
    for r in sqlx::query("SELECT * FROM repl_outbox ORDER BY seq")
        .fetch_all(&mut *conn)
        .await?
    {
        out.repls.push(SourceRepl {
            seq: r.get("seq"),
            bucket: r.get("bucket"),
            key: r.get("key"),
            version_id: r.get("version_id"),
            etag: r.get("etag"),
            size: r.get("size"),
            remote_id: r.get("remote_id"),
            dest_bucket: r.get("dest_bucket"),
            is_delete_marker: get::<i64>(&r, "is_delete_marker", 0) != 0,
            state: r.get("state"),
            attempts: r.get("attempts"),
            claimed_at: r.get("claimed_at"),
            next_retry_at: get(&r, "next_retry_at", 0),
            created_at: get(&r, "created_at", 0),
            last_error: r.get("last_error"),
            target_kind: get(&r, "target_kind", "remote".to_string()),
            is_version_delete: get::<i64>(&r, "is_version_delete", 0) != 0,
        });
    }
    for r in sqlx::query("SELECT * FROM sync_write_intents ORDER BY id")
        .fetch_all(&mut *conn)
        .await?
    {
        out.write_intents.push(StoredWriteIntent {
            id: r.get("id"),
            bucket: r.get("bucket"),
            key: r.get("key"),
            local_version_id: r.get("local_version_id"),
            remote_version_id: r.get("remote_version_id"),
            tee_path: r.get("tee_path"),
            commit_json: r.get("commit_json"),
            state: r.get("state"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        });
    }
    for r in sqlx::query("SELECT * FROM sync_delete_intents ORDER BY id")
        .fetch_all(&mut *conn)
        .await?
    {
        out.delete_intents.push(StoredDeleteIntent {
            id: r.get("id"),
            bucket: r.get("bucket"),
            key: r.get("key"),
            op: r.get("op"),
            local_version_id: r.get("local_version_id"),
            remote_version_id: r.get("remote_version_id"),
            marker_version_id: r.get("marker_version_id"),
            state: r.get("state"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        });
    }
    for r in sqlx::query("SELECT * FROM site_cursors ORDER BY peer_id")
        .fetch_all(&mut *conn)
        .await?
    {
        out.cursors.push(SourceCursor {
            peer_id: r.get("peer_id"),
            last_seq: r.get("last_seq"),
            updated_at: get(&r, "updated_at", 0),
        });
    }
    for r in sqlx::query("SELECT * FROM site_apply_dead ORDER BY peer_id, seq")
        .fetch_all(&mut *conn)
        .await?
    {
        out.dead.push(SourceDead {
            peer_id: r.get("peer_id"),
            seq: r.get("seq"),
            entity: r.get("entity"),
            entity_id: r.get("entity_id"),
            error: r.get("error"),
            payload: r.get("payload"),
            created_at: get(&r, "created_at", 0),
        });
    }
    for r in sqlx::query("SELECT * FROM account_usage ORDER BY account_id")
        .fetch_all(&mut *conn)
        .await?
    {
        out.usage.push(SourceUsage {
            account_id: r.get("account_id"),
            used_bytes: r.get("used_bytes"),
            object_count: r.get("object_count"),
            complete: get::<i64>(&r, "complete", 0) != 0,
            computed_at: get(&r, "computed_at", 0),
        });
    }
    for r in sqlx::query("SELECT * FROM instant_index ORDER BY k")
        .fetch_all(&mut *conn)
        .await?
    {
        out.instant.push(SourceInstant {
            k: r.get("k"),
            v: r.get("v"),
            expires_at: r.get("expires_at"),
        });
    }
    Ok(out)
}
