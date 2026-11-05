//! Non-serde local row bytes. Each mirror is built by exhaustive destructuring
//! of the production row, so adding a field breaks capture instead of silently
//! changing the golden.

use super::fixtures::{BUCKET, ERROR_TEXT};
use crate::golden;
use serde::Serialize;
use xl_store::control_store::audit::AuditRecord;
use xl_store::control_store::event_outbox::{DeadEventRow, EventOutboxClaim};
use xl_store::control_store::repl_outbox::ReplOutboxRow;
use xl_store::control_store::sites::{SiteCredentialsRow, SiteIdentityRow, SitePeerRow};
use xl_store::control_store::sync_intents::{SyncDeleteIntentRow, SyncWriteIntentRow};
use xl_store::control_store::uploads::UploadRow;

pub fn capture() {
    golden::write_serde(
        "rows/sites_outbox_audit/site_peer.json",
        &site_peer(site_peer_row()),
    );
    golden::write_serde(
        "rows/sites_outbox_audit/site_identity.json",
        &site_identity(site_identity_row()),
    );
    golden::write_serde(
        "rows/sites_outbox_audit/site_credentials.json",
        &site_credentials(site_credentials_row()),
    );
    golden::write_serde(
        "rows/sites_outbox_audit/repl_outbox.json",
        &repl_outbox(repl_row()),
    );
    golden::write_serde(
        "rows/sites_outbox_audit/event_outbox_claim.json",
        &event_claim(event_claim_row()),
    );
    golden::write_serde(
        "rows/sites_outbox_audit/dead_event.json",
        &dead_event(dead_event_row()),
    );
    golden::write_serde(
        "rows/sites_outbox_audit/audit_record.json",
        &audit_record(audit_row()),
    );
    golden::write_serde("rows/sites_outbox_audit/upload.json", &upload(upload_row()));
    golden::write_serde(
        "rows/sites_outbox_audit/sync_write_intent.json",
        &sync_write(sync_write_row()),
    );
    golden::write_serde(
        "rows/sites_outbox_audit/sync_delete_intent.json",
        &sync_delete(sync_delete_row()),
    );
}
#[derive(Serialize)]
struct SitePeerMirror {
    peer_id: String,
    remote_site_id: String,
    name: String,
    endpoint: String,
    region: String,
    priority: i64,
    s3_access_key: String,
    s3_secret_key: String,
    peer_site_token: String,
    created_at: i64,
    last_seen: Option<i64>,
    last_error: Option<String>,
    raft_node_id: Option<u64>,
    reconcile_required: bool,
}

fn site_peer(r: SitePeerRow) -> SitePeerMirror {
    let SitePeerRow {
        peer_id,
        remote_site_id,
        name,
        endpoint,
        region,
        priority,
        s3_access_key,
        s3_secret_key,
        peer_site_token,
        created_at,
        last_seen,
        last_error,
        raft_node_id,
        reconcile_required,
    } = r;
    SitePeerMirror {
        peer_id,
        remote_site_id,
        name,
        endpoint,
        region,
        priority,
        s3_access_key,
        s3_secret_key,
        peer_site_token,
        created_at,
        last_seen,
        last_error,
        raft_node_id,
        reconcile_required,
    }
}

fn site_peer_row() -> SitePeerRow {
    SitePeerRow {
        peer_id: "peer-b".into(),
        remote_site_id: "remote-b".into(),
        name: "peer\"q\\b\u{1}".into(),
        endpoint: "https://peer.example:9443".into(),
        region: "us-east-1".into(),
        priority: 3,
        s3_access_key: "AKIAGOLDEN00000001".into(),
        s3_secret_key: "SKGOLDEN00000000001".into(),
        peer_site_token: "b".repeat(32),
        created_at: 1_700_000_000,
        last_seen: Some(1_700_000_100),
        last_error: Some(ERROR_TEXT.into()),
        raft_node_id: Some(3),
        reconcile_required: true,
    }
}
#[derive(Serialize)]
struct SiteIdentityMirror {
    id: String,
    name: String,
    region: String,
    priority: i64,
    token: String,
}

fn site_identity(r: SiteIdentityRow) -> SiteIdentityMirror {
    let SiteIdentityRow {
        id,
        name,
        region,
        priority,
        token,
    } = r;
    SiteIdentityMirror {
        id,
        name,
        region,
        priority,
        token,
    }
}

fn site_identity_row() -> SiteIdentityRow {
    SiteIdentityRow {
        id: super::SITE_ID.into(),
        name: "golden\"q\\b\u{1}".into(),
        region: "us-east-1".into(),
        priority: super::SITE_PRIORITY,
        token: "a".repeat(32),
    }
}

#[derive(Serialize)]
struct SiteCredentialsMirror {
    endpoint: String,
    s3_access_key: String,
    s3_secret_key: String,
    region: String,
    peer_site_token: String,
}

fn site_credentials(r: SiteCredentialsRow) -> SiteCredentialsMirror {
    let SiteCredentialsRow {
        endpoint,
        s3_access_key,
        s3_secret_key,
        region,
        peer_site_token,
    } = r;
    SiteCredentialsMirror {
        endpoint,
        s3_access_key,
        s3_secret_key,
        region,
        peer_site_token,
    }
}

fn site_credentials_row() -> SiteCredentialsRow {
    SiteCredentialsRow {
        endpoint: "http://127.0.0.1:9".into(),
        s3_access_key: "AKIAGOLDEN00000001".into(),
        s3_secret_key: "SKGOLDEN00000000001".into(),
        region: "us-east-1".into(),
        peer_site_token: "b".repeat(32),
    }
}
#[derive(Serialize)]
struct ReplOutboxMirror {
    seq: i64,
    bucket: String,
    key: String,
    version_id: String,
    etag: String,
    remote_id: String,
    dest_bucket: String,
    is_delete_marker: bool,
    is_version_delete: bool,
    target_kind: String,
    attempts: i64,
    claimed_at: Option<i64>,
}

fn repl_outbox(r: ReplOutboxRow) -> ReplOutboxMirror {
    let ReplOutboxRow {
        seq,
        bucket,
        key,
        version_id,
        etag,
        remote_id,
        dest_bucket,
        is_delete_marker,
        is_version_delete,
        target_kind,
        attempts,
        claimed_at,
    } = r;
    ReplOutboxMirror {
        seq,
        bucket,
        key,
        version_id,
        etag,
        remote_id,
        dest_bucket,
        is_delete_marker,
        is_version_delete,
        target_kind,
        attempts,
        claimed_at,
    }
}

fn repl_row() -> ReplOutboxRow {
    ReplOutboxRow {
        seq: 41,
        bucket: BUCKET.into(),
        key: "logs/a b".into(),
        version_id: "v1".into(),
        etag: "etag\"q\\b\u{1}".into(),
        remote_id: "remote-b".into(),
        dest_bucket: "dest-b".into(),
        is_delete_marker: true,
        is_version_delete: true,
        target_kind: "site".into(),
        attempts: 2,
        claimed_at: Some(1_700_000_200),
    }
}

#[derive(Serialize)]
struct EventClaimMirror {
    id: i64,
    target_id: String,
    event_json: String,
    attempts: i64,
    claimed_at: Option<i64>,
}

fn event_claim(r: EventOutboxClaim) -> EventClaimMirror {
    let EventOutboxClaim {
        id,
        target_id,
        event_json,
        attempts,
        claimed_at,
    } = r;
    EventClaimMirror {
        id,
        target_id,
        event_json,
        attempts,
        claimed_at,
    }
}

fn event_claim_row() -> EventOutboxClaim {
    EventOutboxClaim {
        id: 42,
        target_id: "target-1".into(),
        event_json: super::fixtures::EVENT_JSON.into(),
        attempts: 1,
        claimed_at: Some(1_700_000_300),
    }
}

#[derive(Serialize)]
struct DeadEventMirror {
    id: i64,
    bucket: String,
    event_json: String,
    attempts: i64,
    last_error: Option<String>,
    created_at: i64,
}

fn dead_event(r: DeadEventRow) -> DeadEventMirror {
    let DeadEventRow {
        id,
        bucket,
        event_json,
        attempts,
        last_error,
        created_at,
    } = r;
    DeadEventMirror {
        id,
        bucket,
        event_json,
        attempts,
        last_error,
        created_at,
    }
}

fn dead_event_row() -> DeadEventRow {
    DeadEventRow {
        id: 43,
        bucket: BUCKET.into(),
        event_json: super::fixtures::EVENT_JSON.into(),
        attempts: 9,
        last_error: Some(ERROR_TEXT.into()),
        created_at: 1_700_000_400,
    }
}

#[derive(Serialize)]
struct AuditMirror {
    id: i64,
    ts: i64,
    uid: i64,
    op: String,
    bucket: Option<String>,
    object_key: Option<String>,
    decision: String,
    detail: Option<String>,
}

fn audit_record(r: AuditRecord) -> AuditMirror {
    let AuditRecord {
        id,
        ts,
        uid,
        op,
        bucket,
        object_key,
        decision,
        detail,
    } = r;
    AuditMirror {
        id,
        ts,
        uid,
        op,
        bucket,
        object_key,
        decision,
        detail,
    }
}

fn audit_row() -> AuditRecord {
    AuditRecord {
        id: 44,
        ts: 1_700_000_500,
        uid: 10,
        op: "GetObject".into(),
        bucket: Some(BUCKET.into()),
        object_key: Some("logs/a b".into()),
        decision: "deny".into(),
        detail: Some(ERROR_TEXT.into()),
    }
}
#[derive(Serialize)]
struct UploadMirror {
    saved_path: String,
    bucket: String,
    key: String,
    group_name: String,
    size: i64,
    etag: String,
    original_name: Option<String>,
    created_at: i64,
}

fn upload(r: UploadRow) -> UploadMirror {
    let UploadRow {
        saved_path,
        bucket,
        key,
        group_name,
        size,
        etag,
        original_name,
        created_at,
    } = r;
    UploadMirror {
        saved_path,
        bucket,
        key,
        group_name,
        size,
        etag,
        original_name,
        created_at,
    }
}

fn upload_row() -> UploadRow {
    UploadRow {
        saved_path: "a/b/c".into(),
        bucket: BUCKET.into(),
        key: "logs/a b".into(),
        group_name: "file".into(),
        size: 123,
        etag: "etag\"q\\b\u{1}".into(),
        original_name: Some("name\"q\\b\u{1}".into()),
        created_at: 1_700_000_600,
    }
}

#[derive(Serialize)]
struct SyncWriteMirror {
    id: String,
    bucket: String,
    key: String,
}

fn sync_write(r: SyncWriteIntentRow) -> SyncWriteMirror {
    let SyncWriteIntentRow { id, bucket, key } = r;
    SyncWriteMirror { id, bucket, key }
}

fn sync_write_row() -> SyncWriteIntentRow {
    SyncWriteIntentRow {
        id: "intent\"q\\b\u{1}".into(),
        bucket: BUCKET.into(),
        key: "logs/a b".into(),
    }
}

#[derive(Serialize)]
struct SyncDeleteMirror {
    id: String,
    bucket: String,
    key: String,
    op: String,
    local_version_id: Option<String>,
    remote_version_id: Option<String>,
    marker_version_id: Option<String>,
    state: String,
}

fn sync_delete(r: SyncDeleteIntentRow) -> SyncDeleteMirror {
    let SyncDeleteIntentRow {
        id,
        bucket,
        key,
        op,
        local_version_id,
        remote_version_id,
        marker_version_id,
        state,
    } = r;
    SyncDeleteMirror {
        id,
        bucket,
        key,
        op,
        local_version_id,
        remote_version_id,
        marker_version_id,
        state,
    }
}

fn sync_delete_row() -> SyncDeleteIntentRow {
    SyncDeleteIntentRow {
        id: "intent-del\"q\\b\u{1}".into(),
        bucket: BUCKET.into(),
        key: "logs/a b".into(),
        op: "delete_version".into(),
        local_version_id: Some("v1".into()),
        remote_version_id: None,
        marker_version_id: Some("v0".into()),
        state: "pending".into(),
    }
}
