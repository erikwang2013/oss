//! Object-backed account / member / ledger snapshot export and install (B3).

use xl_store::control_store::ControlError;
use xl_store::control_store::account::{
    AccountEntry, AccountMemberEntry, AccountSnapshotTxn, AccountUsageLedgerEntry,
};

use super::ObjectControlTxn;
use super::account::{
    AccountRow, MemberRow, account_row, scan_accounts, scan_ledger, scan_members,
};
use super::account_write::{install_member, put_account};
use super::iam::{backend, uid_key, user_row_by_uid, user_uid_by_username};
use super::table::TableId;

async fn non_admin_uid(
    txn: &ObjectControlTxn,
    username: &str,
    what: &str,
) -> Result<i64, ControlError> {
    match user_uid_by_username(txn, username).await? {
        Some(uid)
            if user_row_by_uid(txn, uid)
                .await?
                .is_some_and(|u| !u.is_admin) =>
        {
            Ok(uid)
        }
        _ => Err(ControlError::Invalid(format!(
            "{what} {username:?} is not a replicated non-admin user"
        ))),
    }
}

#[async_trait::async_trait]
impl AccountSnapshotTxn for ObjectControlTxn {
    async fn snapshot_accounts(&mut self) -> Result<Vec<AccountEntry>, ControlError> {
        let mut out = Vec::new();
        for row in scan_accounts(self).await? {
            let admin = match row.admin_uid {
                None => None,
                Some(uid) => match user_row_by_uid(self, uid).await? {
                    Some(u) if !u.is_admin => Some(u.username),
                    _ => {
                        return Err(ControlError::Invalid(format!(
                            "account {} admin_uid {} is missing or a global admin (not exportable)",
                            row.id, uid
                        )));
                    }
                },
            };
            out.push(AccountEntry {
                id: row.id,
                name: row.name,
                status: row.status,
                admin,
                bucket_quota: row.bucket_quota,
                soft_byte_quota: row.soft_byte_quota,
                created_at: row.created_at,
                updated_at: row.updated_at,
            });
        }
        Ok(out)
    }

    async fn snapshot_account_members(&mut self) -> Result<Vec<AccountMemberEntry>, ControlError> {
        let mut joined = Vec::new();
        for m in scan_members(self).await? {
            if let Some(u) = user_row_by_uid(self, m.uid).await? {
                joined.push((m, u));
            }
        }
        joined.sort_by(|(a, ua), (b, ub)| {
            (&a.account_id, &ua.username).cmp(&(&b.account_id, &ub.username))
        });
        let mut out = Vec::with_capacity(joined.len());
        for (m, u) in joined {
            if u.is_admin {
                return Err(ControlError::Invalid(format!(
                    "account_members {}/{} points at a global admin (not exportable)",
                    m.account_id, u.username
                )));
            }
            out.push(AccountMemberEntry {
                account_id: m.account_id,
                username: u.username,
                role: m.role,
                created_at: m.created_at,
            });
        }
        Ok(out)
    }

    async fn snapshot_account_usage_ledger(
        &mut self,
    ) -> Result<Vec<AccountUsageLedgerEntry>, ControlError> {
        Ok(scan_ledger(self)
            .await?
            .into_iter()
            .map(|r| AccountUsageLedgerEntry {
                account_id: r.account_id,
                used_bytes: r.used_bytes,
                object_count: r.object_count,
            })
            .collect())
    }

    async fn replace_accounts(
        &mut self,
        accounts: &[AccountEntry],
        members: &[AccountMemberEntry],
        canonical: bool,
    ) -> Result<(), ControlError> {
        self.bulk_rebuild(TableId::AccountMembers);
        self.bulk_rebuild(TableId::Accounts);
        for m in scan_members(self).await? {
            self.delete(TableId::AccountMembers, &uid_key(m.uid))
                .await
                .map_err(backend)?;
        }
        for a in scan_accounts(self).await? {
            self.delete(TableId::Accounts, a.id.as_bytes())
                .await
                .map_err(backend)?;
        }
        for a in accounts {
            if !xl_s3::access::valid_account_id(&a.id) {
                return Err(ControlError::Invalid(format!(
                    "account id {:?} is not 12 ASCII digits",
                    a.id
                )));
            }
            if !matches!(a.status.as_str(), "active" | "disabled") {
                return Err(ControlError::Invalid(format!(
                    "account {} status {:?} invalid",
                    a.id, a.status
                )));
            }
            let admin_uid = match &a.admin {
                None => None,
                Some(name) => Some(non_admin_uid(self, name, "account admin").await?),
            };
            if account_row(self, &a.id).await?.is_some() {
                return Err(ControlError::Backend(
                    "UNIQUE constraint failed: accounts.id".into(),
                ));
            }
            put_account(
                self,
                &AccountRow {
                    id: a.id.clone(),
                    name: a.name.clone(),
                    status: a.status.clone(),
                    admin_uid,
                    bucket_quota: a.bucket_quota,
                    soft_byte_quota: a.soft_byte_quota,
                    hard_byte_quota: None,
                    object_quota: None,
                    created_at: a.created_at,
                    updated_at: a.updated_at,
                },
            )
            .await?;
        }
        for m in members {
            if !matches!(m.role.as_str(), "owner" | "admin" | "member") {
                return Err(ControlError::Invalid(format!(
                    "account {} member {} role {:?} invalid",
                    m.account_id, m.username, m.role
                )));
            }
            let uid = non_admin_uid(self, &m.username, "account member").await?;
            install_member(
                self,
                MemberRow {
                    account_id: m.account_id.clone(),
                    uid,
                    role: m.role.clone(),
                    created_at: m.created_at,
                },
            )
            .await?;
        }
        if canonical {
            self.set_marker(crate::db::META_ACCOUNTS_CANONICAL, "done");
        }
        Ok(())
    }
}
