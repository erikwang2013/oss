//! Crate-internal façade for the offline SQLite→object migrator (G1).
//!
//! The domain modules stay private; migration needs the raw row codecs and keys
//! so lossy high-level snapshot installers are bypassed for local attributes.

use xl_store::control_store::ControlError;
use xl_store::control_store::sites::SitePeerRow;

use super::account::LedgerRow;
use super::bucket::{BucketRow, encode_row, key1};
use super::iam::{AccessKeyRow, PermissionStorageRow, UserRow, enc, permission_key, uid_key};
use super::site::{SITE_CONFIG_KEY, changelog_key, clock_key, site_key};
use super::{ObjectControlTxn, TableId};

pub(crate) use super::account::{scan_accounts, scan_ledger, scan_members};
pub(crate) use super::audit::{AUDIT_META_KV, AUDIT_NS, NEXT_ID_KEY, StoredAudit};
pub(crate) use super::event_outbox::StoredEvent;
pub(crate) use super::iam::{scan_access_keys, scan_permissions, scan_users};
pub(crate) use super::local_ops::{
    EVENT_LEASE_SECS, EVENT_QUEUE, REPL_LEASE_SECS, REPL_QUEUE, SYNC_DELETE_KV, SYNC_WRITE_KV,
    decode as decode_local, encode as encode_local,
};
pub(crate) use super::repl_outbox::StoredRepl;
pub(crate) use super::site::{SiteChangelogRow, SiteConfigRow, SiteEntityClockRow};
pub(crate) use super::sync_intents::{StoredDeleteIntent, StoredWriteIntent};

fn backend(e: xl_store::error::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

pub(crate) async fn put_user(
    txn: &mut ObjectControlTxn,
    row: &UserRow,
) -> Result<(), ControlError> {
    txn.put(TableId::Users, &uid_key(row.uid), &enc(row)?)
        .await
        .map_err(backend)
}

pub(crate) async fn put_access_key(
    txn: &mut ObjectControlTxn,
    row: &AccessKeyRow,
) -> Result<(), ControlError> {
    txn.put(TableId::AccessKeys, row.access_key.as_bytes(), &enc(row)?)
        .await
        .map_err(backend)
}

pub(crate) async fn put_permission(
    txn: &mut ObjectControlTxn,
    row: &PermissionStorageRow,
) -> Result<(), ControlError> {
    txn.put(
        TableId::Permissions,
        &permission_key(row.user_id, &row.bucket),
        &enc(row)?,
    )
    .await
    .map_err(backend)
}

pub(crate) async fn put_bucket(
    txn: &mut ObjectControlTxn,
    row: &BucketRow,
) -> Result<(), ControlError> {
    txn.put(TableId::Buckets, &key1(&row.name), &encode_row(row)?)
        .await
        .map_err(backend)
}

pub(crate) async fn put_ledger(
    txn: &mut ObjectControlTxn,
    row: &LedgerRow,
) -> Result<(), ControlError> {
    txn.put(
        TableId::AccountUsageLedger,
        row.account_id.as_bytes(),
        &enc(row)?,
    )
    .await
    .map_err(backend)
}

pub(crate) async fn put_site_config(
    txn: &mut ObjectControlTxn,
    row: &SiteConfigRow,
) -> Result<(), ControlError> {
    txn.put(TableId::SiteConfig, &SITE_CONFIG_KEY, &enc(row)?)
        .await
        .map_err(backend)
}

pub(crate) async fn put_site_peer(
    txn: &mut ObjectControlTxn,
    row: &SitePeerRow,
) -> Result<(), ControlError> {
    txn.put(TableId::Sites, site_key(&row.peer_id), &enc(row)?)
        .await
        .map_err(backend)
}

pub(crate) async fn put_site_changelog(
    txn: &mut ObjectControlTxn,
    row: &SiteChangelogRow,
) -> Result<(), ControlError> {
    txn.put(TableId::SiteChangelog, &changelog_key(row.seq), &enc(row)?)
        .await
        .map_err(backend)
}

pub(crate) async fn put_site_clock(
    txn: &mut ObjectControlTxn,
    row: &SiteEntityClockRow,
) -> Result<(), ControlError> {
    txn.put(
        TableId::SiteEntityClock,
        &clock_key(&row.entity, &row.entity_id),
        &enc(row)?,
    )
    .await
    .map_err(backend)
}

async fn scan_rows<T: serde::de::DeserializeOwned>(
    txn: &ObjectControlTxn,
    table: TableId,
) -> Result<Vec<T>, ControlError> {
    let end = [0xffu8; 256];
    let mut out = Vec::new();
    for (_, value) in txn.scan(table, &b""[..]..&end[..]).await.map_err(backend)? {
        out.push(
            rmp_serde::from_slice(&value)
                .map_err(|e| ControlError::Backend(format!("decode row: {e}")))?,
        );
    }
    Ok(out)
}

pub(crate) async fn bucket_rows(txn: &ObjectControlTxn) -> Result<Vec<BucketRow>, ControlError> {
    let end = [0xffu8; 256];
    txn.scan(TableId::Buckets, &b""[..]..&end[..])
        .await
        .map_err(backend)?
        .iter()
        .map(|(_, value)| super::bucket::decode_row(value))
        .collect()
}

pub(crate) async fn site_config_rows(
    txn: &ObjectControlTxn,
) -> Result<Vec<SiteConfigRow>, ControlError> {
    scan_rows(txn, TableId::SiteConfig).await
}

pub(crate) async fn site_peer_rows(
    txn: &ObjectControlTxn,
) -> Result<Vec<SitePeerRow>, ControlError> {
    scan_rows(txn, TableId::Sites).await
}

pub(crate) async fn site_changelog_rows(
    txn: &ObjectControlTxn,
) -> Result<Vec<SiteChangelogRow>, ControlError> {
    scan_rows(txn, TableId::SiteChangelog).await
}

pub(crate) async fn site_clock_rows(
    txn: &ObjectControlTxn,
) -> Result<Vec<SiteEntityClockRow>, ControlError> {
    scan_rows(txn, TableId::SiteEntityClock).await
}
