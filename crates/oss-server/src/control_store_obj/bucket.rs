//! Object-backed bucket rows: key codec, owner directory, and read adapters.
//!
//! Keys are component-escaped so bytewise tree order equals SQLite BINARY
//! `ORDER BY`: `0x00` escapes as `0x00 0x01`, a component ends `0x00 0x00`, and a
//! nullable component is `0x00` (NULL) or `0x01` + escaped text. Until the IAM
//! slice lands its object tables, owner lookups use `users` keyed by big-endian
//! uid with a `{username, is_admin}` row (C1 will extend, not replace, this).

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use xl_store::control_store::bucket::{
    BucketPolicyEntry, BucketStore, BucketView, EventConfigEntry, ReplicationRuleEntry,
};
use xl_store::control_store::{ControlError, OwnerRef};

use super::{ObjectControlStore, ObjectControlTxn, TableId};

pub(crate) type CResult<T> = std::result::Result<T, ControlError>;

pub(crate) fn backend(e: impl std::fmt::Display) -> ControlError {
    ControlError::Backend(e.to_string())
}

pub(crate) fn encode_row(row: &impl Serialize) -> CResult<Vec<u8>> {
    rmp_serde::to_vec_named(row).map_err(backend)
}

pub(crate) fn decode_row<T: DeserializeOwned>(bytes: &[u8]) -> CResult<T> {
    rmp_serde::from_slice(bytes).map_err(backend)
}

// ---------- key codec ----------

fn component(out: &mut Vec<u8>, raw: &[u8]) {
    for byte in raw {
        out.push(*byte);
        if *byte == 0 {
            out.push(1);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

fn optional(out: &mut Vec<u8>, raw: Option<&str>) {
    match raw {
        None => out.push(0),
        Some(text) => {
            out.push(1);
            component(out, text.as_bytes());
        }
    }
}

pub(crate) fn key1(a: &str) -> Vec<u8> {
    let mut out = Vec::new();
    component(&mut out, a.as_bytes());
    out
}

pub(crate) fn key2(a: &str, b: &str) -> Vec<u8> {
    let mut out = key1(a);
    component(&mut out, b.as_bytes());
    out
}

pub(crate) fn key3(a: &str, b: &str, c: &str) -> Vec<u8> {
    let mut out = key2(a, b);
    component(&mut out, c.as_bytes());
    out
}

pub(crate) fn key_opt2(a: &str, b: Option<&str>, c: &str) -> Vec<u8> {
    let mut out = key1(a);
    optional(&mut out, b);
    component(&mut out, c.as_bytes());
    out
}

/// Exclusive upper bound covering every key that starts with `prefix`.
pub(crate) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    if let Some(last) = end.last_mut() {
        *last = 1;
    }
    end
}

pub(crate) const ALL_END: [u8; 64] = [0xff; 64];

pub(crate) async fn txn_scan_all(
    txn: &ObjectControlTxn,
    table: TableId,
) -> CResult<Vec<(Vec<u8>, Vec<u8>)>> {
    txn.scan(table, &b""[..]..&ALL_END[..])
        .await
        .map_err(backend)
}

pub(crate) async fn store_scan_all(
    store: &ObjectControlStore,
    table: TableId,
) -> CResult<Vec<(Vec<u8>, Vec<u8>)>> {
    store
        .read_range(table, &b""[..]..&ALL_END[..])
        .await
        .map_err(backend)
}

/// Delete every row whose first key component is `first`.
pub(crate) async fn delete_prefix(
    txn: &mut ObjectControlTxn,
    table: TableId,
    first: &str,
) -> CResult<()> {
    let start = key1(first);
    let end = prefix_end(&start);
    let keys: Vec<Vec<u8>> = txn
        .scan(table, &start[..]..&end[..])
        .await
        .map_err(backend)?
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    for key in keys {
        txn.delete(table, &key).await.map_err(backend)?;
    }
    Ok(())
}

pub(crate) async fn clear_table(txn: &mut ObjectControlTxn, table: TableId) -> CResult<()> {
    txn.bulk_rebuild(table);
    for (key, _) in txn_scan_all(txn, table).await? {
        txn.delete(table, &key).await.map_err(backend)?;
    }
    Ok(())
}

// ---------- user directory (B2 seam until C1) ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UserRec {
    pub username: String,
    #[serde(default)]
    pub is_admin: bool,
}

pub(crate) fn user_key(uid: i64) -> Vec<u8> {
    (uid as u64).to_be_bytes().to_vec()
}

#[async_trait::async_trait]
pub(crate) trait UserLookup: Send {
    async fn user_by_uid(&self, uid: i64) -> CResult<Option<UserRec>>;
    /// `uid` ascending.
    async fn users_sorted(&self) -> CResult<Vec<(i64, UserRec)>>;
}

fn parse_user_rows(rows: Vec<(Vec<u8>, Vec<u8>)>) -> CResult<Vec<(i64, UserRec)>> {
    rows.into_iter()
        .map(|(key, value)| {
            let uid = i64::from_be_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| backend("bad object user key"))?,
            );
            Ok((uid, decode_row(&value)?))
        })
        .collect()
}

#[async_trait::async_trait]
impl UserLookup for ObjectControlTxn {
    async fn user_by_uid(&self, uid: i64) -> CResult<Option<UserRec>> {
        let row = self
            .get(TableId::Users, &user_key(uid))
            .await
            .map_err(backend)?;
        row.as_deref().map(decode_row).transpose()
    }

    async fn users_sorted(&self) -> CResult<Vec<(i64, UserRec)>> {
        parse_user_rows(txn_scan_all(self, TableId::Users).await?)
    }
}

#[async_trait::async_trait]
impl UserLookup for ObjectControlStore {
    async fn user_by_uid(&self, uid: i64) -> CResult<Option<UserRec>> {
        let row = self
            .read_row(TableId::Users, &user_key(uid))
            .await
            .map_err(backend)?;
        row.as_deref().map(decode_row).transpose()
    }

    async fn users_sorted(&self) -> CResult<Vec<(i64, UserRec)>> {
        parse_user_rows(store_scan_all(self, TableId::Users).await?)
    }
}

pub(crate) async fn user_by_name<L: UserLookup + ?Sized>(
    dir: &L,
    username: &str,
) -> CResult<Option<(i64, UserRec)>> {
    Ok(dir
        .users_sorted()
        .await?
        .into_iter()
        .find(|(_, row)| row.username == username))
}

pub(crate) async fn admin_uid<L: UserLookup + ?Sized>(dir: &L) -> CResult<Option<i64>> {
    Ok(dir
        .users_sorted()
        .await?
        .into_iter()
        .find(|(_, row)| row.is_admin)
        .map(|(uid, _)| uid))
}

pub(crate) async fn non_admin_uid<L: UserLookup + ?Sized>(
    dir: &L,
    username: &str,
) -> CResult<Option<i64>> {
    Ok(user_by_name(dir, username)
        .await?
        .filter(|(_, row)| !row.is_admin)
        .map(|(uid, _)| uid))
}

// ---------- bucket rows ----------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BucketRow {
    pub name: String,
    pub owner_uid: Option<i64>,
    pub set_id: i64,
    pub creation_date: i64,
    pub versioning: String,
    pub object_lock: bool,
    pub default_encryption: Option<String>,
    pub object_lock_default_mode: Option<String>,
    pub object_lock_default_days: Option<i64>,
    pub replication_mode: String,
    pub deleting: bool,
}

impl BucketRow {
    pub(crate) fn new(
        entry: &xl_store::control_store::bucket::BucketEntry,
        owner_uid: Option<i64>,
        set_id: i64,
    ) -> Self {
        Self {
            name: entry.name.clone(),
            owner_uid,
            set_id,
            creation_date: entry.creation_date,
            versioning: entry.versioning.clone(),
            object_lock: entry.object_lock,
            default_encryption: entry.default_encryption.clone(),
            object_lock_default_mode: entry.object_lock_default_mode.clone(),
            object_lock_default_days: entry.object_lock_default_days,
            replication_mode: entry.replication_mode.clone(),
            deleting: entry.deleting,
        }
    }
}

pub(crate) async fn bucket_view<L: UserLookup + ?Sized>(
    dir: &L,
    row: BucketRow,
) -> CResult<BucketView> {
    let user = match row.owner_uid {
        None | Some(0) => None,
        Some(uid) => dir.user_by_uid(uid).await?,
    };
    let owner = match (row.owner_uid, &user) {
        (None, _) | (Some(0), _) | (_, None) => Some(OwnerRef::Admin),
        (_, Some(user)) => Some(OwnerRef::User(user.username.clone())),
    };
    Ok(BucketView {
        name: row.name,
        owner,
        owner_uid: row.owner_uid,
        owner_is_admin: user.is_some_and(|u| u.is_admin),
        creation_date: row.creation_date,
        versioning: row.versioning,
        object_lock: row.object_lock,
        default_encryption: row.default_encryption,
        object_lock_default_mode: row.object_lock_default_mode,
        object_lock_default_days: row.object_lock_default_days,
        replication_mode: row.replication_mode,
        deleting: row.deleting,
    })
}

// ---------- reads ----------

#[async_trait::async_trait]
impl BucketStore for ObjectControlStore {
    async fn bucket(&self, name: &str) -> CResult<Option<BucketView>> {
        let Some(bytes) = self
            .read_row(TableId::Buckets, &key1(name))
            .await
            .map_err(backend)?
        else {
            return Ok(None);
        };
        bucket_view(self, decode_row(&bytes)?).await.map(Some)
    }

    async fn buckets(&self) -> CResult<Vec<BucketView>> {
        let mut out = Vec::new();
        for (_, value) in store_scan_all(self, TableId::Buckets).await? {
            out.push(bucket_view(self, decode_row(&value)?).await?);
        }
        Ok(out)
    }

    async fn bucket_policy(&self, bucket: &str) -> CResult<Option<BucketPolicyEntry>> {
        let Some(bytes) = self
            .read_row(TableId::BucketPolicies, &key1(bucket))
            .await
            .map_err(backend)?
        else {
            return Ok(None);
        };
        decode_row(&bytes).map(Some)
    }

    async fn bucket_policies(&self) -> CResult<Vec<BucketPolicyEntry>> {
        store_scan_all(self, TableId::BucketPolicies)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn bucket_notification(&self, bucket: &str) -> CResult<Option<EventConfigEntry>> {
        let Some(bytes) = self
            .read_row(TableId::EventConfigs, &key1(bucket))
            .await
            .map_err(backend)?
        else {
            return Ok(None);
        };
        decode_row(&bytes).map(Some)
    }

    async fn event_configs(&self) -> CResult<Vec<EventConfigEntry>> {
        store_scan_all(self, TableId::EventConfigs)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn bucket_replication_rules(
        &self,
        bucket: &str,
    ) -> CResult<Option<ReplicationRuleEntry>> {
        let Some(bytes) = self
            .read_row(TableId::ReplicationRules, &key1(bucket))
            .await
            .map_err(backend)?
        else {
            return Ok(None);
        };
        decode_row(&bytes).map(Some)
    }

    async fn replication_rules(&self) -> CResult<Vec<ReplicationRuleEntry>> {
        store_scan_all(self, TableId::ReplicationRules)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }
}
