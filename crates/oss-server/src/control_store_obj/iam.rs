//! Object-backed IAM adapters (implementation plan C1).
//!
//! `users` rows are keyed by numeric uid (big-endian, never reused); a username
//! uniqueness check scans the table inside the transaction. `access_keys` rows
//! are keyed by the AK natural key and owner lookups scan inside the transaction;
//! `permissions` are keyed `(uid, bucket)`. All reads and the §5.1 raft-guard
//! exception matrix run from the transaction overlay, so a guard re-runs with the
//! generation it commits to.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{IamStore, PermissionRow, UserLogin, UserView};

use super::table::TableId;
use super::{ObjectControlStore, ObjectControlTxn, WriteOrigin};

/// Exclusive upper bound covering every natural key this module writes.
const MAX_KEY: [u8; 256] = [0xff; 256];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UserRow {
    pub uid: i64,
    pub username: String,
    pub password_hash: String,
    pub is_admin: bool,
    pub status: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AccessKeyRow {
    pub access_key: String,
    pub secret_key: String,
    pub user_id: i64,
    pub status: String,
    pub expires_at: Option<i64>,
    pub created_at: i64,
    pub parent_ak: Option<String>,
    pub session_policy: Option<String>,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PermissionStorageRow {
    pub user_id: i64,
    pub bucket: String,
    pub can_read: bool,
    pub can_write: bool,
    pub can_delete: bool,
}

pub(crate) fn backend(e: impl std::fmt::Display) -> ControlError {
    ControlError::Backend(e.to_string())
}

pub(crate) fn enc<T: Serialize>(value: &T) -> Result<Vec<u8>, ControlError> {
    rmp_serde::to_vec_named(value).map_err(backend)
}

pub(crate) fn dec<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ControlError> {
    rmp_serde::from_slice(bytes).map_err(backend)
}

pub(crate) fn uid_key(uid: i64) -> [u8; 8] {
    (uid as u64).to_be_bytes()
}

pub(crate) fn permission_key(uid: i64, bucket: &str) -> Vec<u8> {
    let mut key = uid_key(uid).to_vec();
    key.extend_from_slice(bucket.as_bytes());
    key
}

pub(crate) async fn scan_users(txn: &ObjectControlTxn) -> Result<Vec<UserRow>, ControlError> {
    let rows = txn
        .scan(TableId::Users, &b""[..]..&MAX_KEY[..])
        .await
        .map_err(backend)?;
    rows.iter().map(|(_, v)| dec(v)).collect()
}

pub(crate) async fn user_row_by_uid(
    txn: &ObjectControlTxn,
    uid: i64,
) -> Result<Option<UserRow>, ControlError> {
    match txn
        .get(TableId::Users, &uid_key(uid))
        .await
        .map_err(backend)?
    {
        Some(bytes) => Ok(Some(dec(&bytes)?)),
        None => Ok(None),
    }
}

pub(crate) async fn user_row_by_username(
    txn: &ObjectControlTxn,
    username: &str,
) -> Result<Option<UserRow>, ControlError> {
    Ok(scan_users(txn)
        .await?
        .into_iter()
        .find(|u| u.username == username))
}

pub(crate) async fn user_uid_by_username(
    txn: &ObjectControlTxn,
    username: &str,
) -> Result<Option<i64>, ControlError> {
    Ok(user_row_by_username(txn, username).await?.map(|u| u.uid))
}

pub(crate) async fn scan_access_keys(
    txn: &ObjectControlTxn,
) -> Result<Vec<AccessKeyRow>, ControlError> {
    let rows = txn
        .scan(TableId::AccessKeys, &b""[..]..&MAX_KEY[..])
        .await
        .map_err(backend)?;
    rows.iter().map(|(_, v)| dec(v)).collect()
}

pub(crate) async fn access_key_row(
    txn: &ObjectControlTxn,
    access_key: &str,
) -> Result<Option<AccessKeyRow>, ControlError> {
    match txn
        .get(TableId::AccessKeys, access_key.as_bytes())
        .await
        .map_err(backend)?
    {
        Some(bytes) => Ok(Some(dec(&bytes)?)),
        None => Ok(None),
    }
}

pub(crate) async fn scan_permissions(
    txn: &ObjectControlTxn,
) -> Result<Vec<PermissionStorageRow>, ControlError> {
    let rows = txn
        .scan(TableId::Permissions, &b""[..]..&MAX_KEY[..])
        .await
        .map_err(backend)?;
    rows.iter().map(|(_, v)| dec(v)).collect()
}

pub(crate) async fn users_missing_accounts(
    txn: &ObjectControlTxn,
) -> Result<Vec<(i64, String)>, ControlError> {
    let joined: std::collections::HashSet<i64> = super::account::scan_member_uids(txn)
        .await?
        .into_iter()
        .collect();
    Ok(scan_users(txn)
        .await?
        .into_iter()
        .filter(|u| !u.is_admin && !joined.contains(&u.uid))
        .map(|u| (u.uid, u.username))
        .collect())
}

fn view(row: UserRow) -> UserView {
    UserView {
        uid: row.uid,
        username: row.username,
        is_admin: row.is_admin,
        status: row.status,
        created_at: row.created_at,
    }
}

#[async_trait::async_trait]
impl IamStore for ObjectControlStore {
    async fn user(&self, username: &str) -> Result<Option<UserView>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(user_row_by_username(&txn, username).await?.map(view))
    }

    async fn user_by_id(&self, uid: i64) -> Result<Option<UserView>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(user_row_by_uid(&txn, uid).await?.map(view))
    }

    async fn users(&self) -> Result<Vec<UserView>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(scan_users(&txn).await?.into_iter().map(view).collect())
    }

    async fn user_exists(&self, username: &str) -> Result<bool, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(user_row_by_username(&txn, username).await?.is_some())
    }

    async fn login_user(&self, username: &str) -> Result<Option<UserLogin>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let Some(user) = user_row_by_username(&txn, username).await? else {
            return Ok(None);
        };
        if user.status != "active" {
            return Ok(None);
        }
        if let Some(account_id) = super::account::member_account(&txn, user.uid).await?
            && super::account::account_row(&txn, &account_id)
                .await?
                .is_none_or(|a| a.status != "active")
        {
            return Ok(None);
        }
        Ok(Some(UserLogin {
            uid: user.uid,
            password_hash: user.password_hash,
        }))
    }

    async fn admin_id(&self) -> Result<Option<i64>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(scan_users(&txn)
            .await?
            .into_iter()
            .find(|u| u.is_admin)
            .map(|u| u.uid))
    }

    async fn admin_counts(&self) -> Result<(i64, i64), ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let admins: Vec<UserRow> = scan_users(&txn)
            .await?
            .into_iter()
            .filter(|u| u.is_admin)
            .collect();
        let active = admins.iter().filter(|u| u.status == "active").count() as i64;
        Ok((active, admins.len() as i64))
    }

    async fn permissions_for_user(&self, uid: i64) -> Result<Vec<PermissionRow>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let mut out: Vec<PermissionRow> = scan_permissions(&txn)
            .await?
            .into_iter()
            .filter(|p| p.user_id == uid)
            .map(|p| PermissionRow {
                bucket: p.bucket,
                read: p.can_read,
                write: p.can_write,
                delete: p.can_delete,
            })
            .collect();
        out.sort_by(|a, b| a.bucket.cmp(&b.bucket));
        Ok(out)
    }

    async fn access_keys_for_user(&self, uid: i64) -> Result<Vec<String>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let mut keys: Vec<AccessKeyRow> = scan_access_keys(&txn)
            .await?
            .into_iter()
            .filter(|k| k.user_id == uid && k.kind == "long-term")
            .collect();
        keys.sort_by(|a, b| (a.created_at, &a.access_key).cmp(&(b.created_at, &b.access_key)));
        Ok(keys.into_iter().map(|k| k.access_key).collect())
    }
}

impl ObjectControlStore {
    /// 配对需要一把活跃的管理员长期 AK（含封装后的 SK）。
    pub(crate) async fn admin_long_term_access_key(
        &self,
    ) -> Result<Option<(String, String)>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let Some(admin) = scan_users(&txn).await?.into_iter().find(|u| u.is_admin) else {
            return Ok(None);
        };
        Ok(scan_access_keys(&txn)
            .await?
            .into_iter()
            .find(|k| {
                k.user_id == admin.uid
                    && k.kind == "long-term"
                    && k.parent_ak.is_none()
                    && k.status == "active"
            })
            .map(|k| (k.access_key, k.secret_key)))
    }
}
