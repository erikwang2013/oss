//! Object-backed account / member / ledger adapters and quota checks (B3).
//!
//! Not on the IAM raft-guard table, so these writes carry no origin exceptions;
//! every check reads the transaction overlay. Bucket counts keep the SQLite
//! `account_buckets_from` semantics: owner must be a live non-admin user, the
//! current account is the member row when present, else `printf('%012d', uid)`.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::account::{AccountMemberView, AccountStore, AccountView};

use super::iam::{backend, dec, scan_users, uid_key, user_row_by_uid, user_uid_by_username};
use super::table::TableId;
use super::{ObjectControlStore, ObjectControlTxn, WriteOrigin};

const MAX_KEY: [u8; 256] = [0xff; 256];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AccountRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub admin_uid: Option<i64>,
    pub bucket_quota: Option<i64>,
    pub soft_byte_quota: Option<i64>,
    pub hard_byte_quota: Option<i64>,
    pub object_quota: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MemberRow {
    pub account_id: String,
    pub uid: i64,
    pub role: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LedgerRow {
    pub account_id: String,
    pub used_bytes: i64,
    pub object_count: i64,
}

/// B2 bucket row contract for owner projection. Unrelated fields are ignored by
/// serde; `owner_uid` is the local FK projection, `owner`/`owner_username` the
/// natural key fallback.
#[derive(Debug, Default, Deserialize)]
struct StoredBucket {
    #[serde(default)]
    owner_uid: Option<i64>,
    #[serde(default)]
    owner_id: Option<i64>,
    #[serde(default)]
    owner: Option<OwnerRef>,
    #[serde(default)]
    owner_username: Option<String>,
}

pub(crate) async fn account_row(
    txn: &ObjectControlTxn,
    id: &str,
) -> Result<Option<AccountRow>, ControlError> {
    match txn
        .get(TableId::Accounts, id.as_bytes())
        .await
        .map_err(backend)?
    {
        Some(bytes) => Ok(Some(dec(&bytes)?)),
        None => Ok(None),
    }
}

pub(crate) async fn scan_accounts(txn: &ObjectControlTxn) -> Result<Vec<AccountRow>, ControlError> {
    let rows = txn
        .scan(TableId::Accounts, &b""[..]..&MAX_KEY[..])
        .await
        .map_err(backend)?;
    rows.iter().map(|(_, v)| dec(v)).collect()
}

pub(crate) async fn scan_members(txn: &ObjectControlTxn) -> Result<Vec<MemberRow>, ControlError> {
    let rows = txn
        .scan(TableId::AccountMembers, &b""[..]..&MAX_KEY[..])
        .await
        .map_err(backend)?;
    rows.iter().map(|(_, v)| dec(v)).collect()
}

pub(crate) async fn scan_member_uids(txn: &ObjectControlTxn) -> Result<Vec<i64>, ControlError> {
    let rows = txn
        .scan(TableId::AccountMembers, &b""[..]..&MAX_KEY[..])
        .await
        .map_err(backend)?;
    rows.iter()
        .map(|(key, _)| {
            let bytes: [u8; 8] = key
                .as_slice()
                .try_into()
                .map_err(|_| ControlError::Backend("bad account_members key".into()))?;
            Ok(i64::from_be_bytes(bytes))
        })
        .collect()
}

pub(crate) async fn member_row(
    txn: &ObjectControlTxn,
    uid: i64,
) -> Result<Option<MemberRow>, ControlError> {
    match txn
        .get(TableId::AccountMembers, &uid_key(uid))
        .await
        .map_err(backend)?
    {
        Some(bytes) => Ok(Some(dec(&bytes)?)),
        None => Ok(None),
    }
}

pub(crate) async fn member_account(
    txn: &ObjectControlTxn,
    uid: i64,
) -> Result<Option<String>, ControlError> {
    Ok(member_row(txn, uid).await?.map(|m| m.account_id))
}

pub(crate) async fn ledger_row(
    txn: &ObjectControlTxn,
    account_id: &str,
) -> Result<Option<LedgerRow>, ControlError> {
    match txn
        .get(TableId::AccountUsageLedger, account_id.as_bytes())
        .await
        .map_err(backend)?
    {
        Some(bytes) => Ok(Some(dec(&bytes)?)),
        None => Ok(None),
    }
}

pub(crate) async fn scan_ledger(txn: &ObjectControlTxn) -> Result<Vec<LedgerRow>, ControlError> {
    let rows = txn
        .scan(TableId::AccountUsageLedger, &b""[..]..&MAX_KEY[..])
        .await
        .map_err(backend)?;
    rows.iter().map(|(_, v)| dec(v)).collect()
}

fn parse_bucket(bytes: &[u8]) -> Result<StoredBucket, ControlError> {
    rmp_serde::from_slice(bytes).map_err(backend)
}

/// Live non-admin owner uid of a bucket row (`None` = dangling or admin).
async fn bucket_owner_uid(
    txn: &ObjectControlTxn,
    bucket: &StoredBucket,
) -> Result<Option<i64>, ControlError> {
    let uid = match bucket.owner_uid.or(bucket.owner_id) {
        Some(uid) => Some(uid),
        None => match (&bucket.owner, &bucket.owner_username) {
            (Some(OwnerRef::User(name)), _) => user_uid_by_username(txn, name).await?,
            (Some(OwnerRef::Admin), _) | (None, None) => None,
            (None, Some(name)) => user_uid_by_username(txn, name).await?,
        },
    };
    match uid {
        Some(uid)
            if user_row_by_uid(txn, uid)
                .await?
                .is_some_and(|u| !u.is_admin) =>
        {
            Ok(Some(uid))
        }
        _ => Ok(None),
    }
}

/// `(bucket name, account id)` pairs in `account_buckets_from` scope, name order.
pub(crate) async fn bucket_accounts(
    txn: &ObjectControlTxn,
) -> Result<Vec<(String, String)>, ControlError> {
    let members: HashMap<i64, String> = scan_members(txn)
        .await?
        .into_iter()
        .map(|m| (m.uid, m.account_id))
        .collect();
    let rows = txn
        .scan(TableId::Buckets, &b""[..]..&MAX_KEY[..])
        .await
        .map_err(backend)?;
    let mut out = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let Some(uid) = bucket_owner_uid(txn, &parse_bucket(&value)?).await? else {
            continue;
        };
        // 表键是 `component` 编码（`name\0\0`）；S3 桶名不含 NUL，去掉终止符即原名。
        // 兼容测试夹具直接写入的裸键（无终止符）。
        let name = key
            .strip_suffix(&[0, 0])
            .and_then(|k| std::str::from_utf8(k).ok())
            .or_else(|| std::str::from_utf8(&key).ok())
            .ok_or_else(|| backend("bad buckets key"))?
            .to_owned();
        let account = members
            .get(&uid)
            .cloned()
            .unwrap_or_else(|| format!("{uid:012}"));
        out.push((name, account));
    }
    Ok(out)
}

pub(crate) fn counts_for(pairs: &[(String, String)], id: &str) -> i64 {
    pairs.iter().filter(|(_, account)| account == id).count() as i64
}

fn member_count(members: &[MemberRow], id: &str) -> i64 {
    members.iter().filter(|m| m.account_id == id).count() as i64
}

fn view(row: AccountRow, members: &[MemberRow], buckets: &[(String, String)]) -> AccountView {
    AccountView {
        bucket_count: counts_for(buckets, &row.id),
        member_count: member_count(members, &row.id),
        id: row.id,
        name: row.name,
        status: row.status,
        admin_uid: row.admin_uid,
        bucket_quota: row.bucket_quota,
        soft_byte_quota: row.soft_byte_quota,
        hard_byte_quota: row.hard_byte_quota,
        object_quota: row.object_quota,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

#[async_trait::async_trait]
impl AccountStore for ObjectControlStore {
    async fn account(&self, id: &str) -> Result<Option<AccountView>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let Some(row) = account_row(&txn, id).await? else {
            return Ok(None);
        };
        let members = scan_members(&txn).await?;
        let buckets = bucket_accounts(&txn).await?;
        Ok(Some(view(row, &members, &buckets)))
    }

    async fn accounts(&self, member_uid: Option<i64>) -> Result<Vec<AccountView>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let members = scan_members(&txn).await?;
        let buckets = bucket_accounts(&txn).await?;
        let only = match member_uid {
            Some(uid) => member_row(&txn, uid).await?.map(|m| m.account_id),
            None => None,
        };
        let mut rows = scan_accounts(&txn).await?;
        if member_uid.is_some() {
            rows.retain(|row| only.as_deref() == Some(row.id.as_str()));
        }
        Ok(rows
            .into_iter()
            .map(|r| view(r, &members, &buckets))
            .collect())
    }

    async fn account_status(&self, id: &str) -> Result<Option<String>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(account_row(&txn, id).await?.map(|a| a.status))
    }

    async fn account_bucket_quota(
        &self,
        id: &str,
    ) -> Result<Option<(String, Option<i64>)>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(account_row(&txn, id)
            .await?
            .map(|a| (a.status, a.bucket_quota)))
    }

    async fn account_hard_limits(
        &self,
        id: &str,
    ) -> Result<Option<(Option<i64>, Option<i64>)>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(account_row(&txn, id)
            .await?
            .map(|a| (a.hard_byte_quota, a.object_quota)))
    }

    async fn account_member_account(&self, uid: i64) -> Result<Option<String>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        member_account(&txn, uid).await
    }

    async fn account_active(&self, uid: i64) -> Result<bool, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        match member_row(&txn, uid).await? {
            None => Ok(true),
            Some(m) => Ok(account_row(&txn, &m.account_id)
                .await?
                .is_some_and(|a| a.status == "active")),
        }
    }

    async fn member_uid(
        &self,
        account_id: &str,
        username: &str,
    ) -> Result<Option<i64>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        for m in scan_members(&txn).await? {
            if m.account_id == account_id
                && let Some(u) = user_row_by_uid(&txn, m.uid).await?
                && !u.is_admin
                && u.username == username
            {
                return Ok(Some(m.uid));
            }
        }
        Ok(None)
    }

    async fn member_role(
        &self,
        account_id: &str,
        uid: i64,
    ) -> Result<Option<String>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(member_row(&txn, uid)
            .await?
            .filter(|m| m.account_id == account_id)
            .map(|m| m.role))
    }

    async fn account_members(
        &self,
        account_id: &str,
    ) -> Result<Vec<AccountMemberView>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let mut out = Vec::new();
        for m in scan_members(&txn).await? {
            if m.account_id != account_id {
                continue;
            }
            let Some(u) = user_row_by_uid(&txn, m.uid).await? else {
                continue;
            };
            out.push(AccountMemberView {
                uid: m.uid,
                username: u.username,
                role: m.role,
                is_admin: u.is_admin,
                status: u.status,
                created_at: m.created_at,
            });
        }
        out.sort_by_key(|m| m.uid);
        Ok(out)
    }

    async fn legacy_users_missing_accounts(&self) -> Result<Vec<(i64, String)>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        let joined: HashSet<i64> = scan_member_uids(&txn).await?.into_iter().collect();
        Ok(scan_users(&txn)
            .await?
            .into_iter()
            .filter(|u| !u.is_admin && !joined.contains(&u.uid))
            .map(|u| (u.uid, u.username))
            .collect())
    }

    async fn account_bucket_names(&self, account_id: &str) -> Result<Vec<String>, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(bucket_accounts(&txn)
            .await?
            .into_iter()
            .filter(|(_, account)| account == account_id)
            .map(|(name, _)| name)
            .collect())
    }

    async fn account_counts(&self) -> Result<(i64, i64), ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok((
            scan_accounts(&txn).await?.len() as i64,
            scan_members(&txn).await?.len() as i64,
        ))
    }

    async fn account_ledger_count(&self) -> Result<i64, ControlError> {
        let txn = self
            .begin(WriteOrigin::local("read"))
            .await
            .map_err(backend)?;
        Ok(scan_ledger(&txn).await?.len() as i64)
    }
}
