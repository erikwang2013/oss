//! Object-backed account / member / ledger writes and the user-delete FK cascade.

use std::collections::HashSet;

use xl_store::control_store::ControlError;
use xl_store::control_store::account::AccountTxn;

use super::ObjectControlTxn;
use super::account::{
    AccountRow, LedgerRow, MemberRow, account_row, bucket_accounts, counts_for, ledger_row,
    member_account, member_row, scan_accounts, scan_ledger, scan_members,
};
use super::iam::{backend, enc, scan_users, uid_key, user_row_by_uid};
use super::table::TableId;

pub(crate) async fn install_member(
    txn: &mut ObjectControlTxn,
    row: MemberRow,
) -> Result<(), ControlError> {
    if txn
        .get(TableId::AccountMembers, &uid_key(row.uid))
        .await
        .map_err(backend)?
        .is_some()
    {
        return Err(ControlError::Backend(
            "UNIQUE constraint failed: account_members.uid".into(),
        ));
    }
    txn.put(TableId::AccountMembers, &uid_key(row.uid), &enc(&row)?)
        .await
        .map_err(backend)
}

#[async_trait::async_trait]
impl AccountTxn for ObjectControlTxn {
    async fn account_counts(&mut self) -> Result<(i64, i64), ControlError> {
        Ok((
            scan_accounts(self).await?.len() as i64,
            scan_members(self).await?.len() as i64,
        ))
    }

    async fn account_ledger_count(&mut self) -> Result<i64, ControlError> {
        Ok(scan_ledger(self).await?.len() as i64)
    }

    async fn account_status(&mut self, id: &str) -> Result<Option<String>, ControlError> {
        Ok(account_row(self, id).await?.map(|a| a.status))
    }

    async fn account_hard_limits(
        &mut self,
        id: &str,
    ) -> Result<Option<(Option<i64>, Option<i64>)>, ControlError> {
        Ok(account_row(self, id)
            .await?
            .map(|a| (a.hard_byte_quota, a.object_quota)))
    }

    async fn account_insert_if_absent(
        &mut self,
        id: &str,
        name: &str,
        bucket_quota: Option<i64>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        insert_account_absent(self, id, name, None, bucket_quota, created_at, updated_at).await
    }

    async fn account_system_insert_if_absent(
        &mut self,
        id: &str,
        name: &str,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        insert_account_absent(self, id, name, None, None, created_at, updated_at).await
    }

    async fn account_insert_derived_if_absent(
        &mut self,
        id: &str,
        name: &str,
        admin_uid: i64,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        insert_account_absent(
            self,
            id,
            name,
            Some(admin_uid),
            None,
            created_at,
            updated_at,
        )
        .await
    }

    async fn account_set_name(
        &mut self,
        id: &str,
        name: &str,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let Some(mut row) = account_row(self, id).await? else {
            return Ok(false);
        };
        row.name = name.into();
        row.updated_at = updated_at;
        put_account(self, &row).await?;
        Ok(true)
    }

    async fn account_set_status(
        &mut self,
        id: &str,
        status: &str,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let Some(mut row) = account_row(self, id).await? else {
            return Ok(false);
        };
        if status == "active" {
            row.status = "active".into();
        } else {
            for uid in member_uids_for(self, id).await? {
                if let Some(u) = user_row_by_uid(self, uid).await?
                    && u.is_admin
                    && u.status == "active"
                {
                    return Ok(false);
                }
            }
            row.status = "disabled".into();
        }
        row.updated_at = updated_at;
        put_account(self, &row).await?;
        Ok(true)
    }

    async fn account_set_bucket_quota(
        &mut self,
        id: &str,
        quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let Some(mut row) = account_row(self, id).await? else {
            return Ok(false);
        };
        row.bucket_quota = quota;
        row.updated_at = updated_at;
        put_account(self, &row).await?;
        Ok(true)
    }

    async fn account_set_soft_quota(
        &mut self,
        id: &str,
        quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let Some(mut row) = account_row(self, id).await? else {
            return Ok(false);
        };
        row.soft_byte_quota = quota;
        row.updated_at = updated_at;
        put_account(self, &row).await?;
        Ok(true)
    }

    async fn account_set_hard_limits(
        &mut self,
        id: &str,
        hard_byte_quota: Option<i64>,
        object_quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let Some(mut row) = account_row(self, id).await? else {
            return Ok(false);
        };
        row.hard_byte_quota = hard_byte_quota;
        row.object_quota = object_quota;
        row.updated_at = updated_at;
        put_account(self, &row).await?;
        Ok(true)
    }

    async fn account_delete(&mut self, id: &str) -> Result<bool, ControlError> {
        if account_row(self, id).await?.is_none() {
            return Ok(false);
        }
        for m in scan_members(self).await? {
            if m.account_id == id {
                self.delete(TableId::AccountMembers, &uid_key(m.uid))
                    .await
                    .map_err(backend)?;
            }
        }
        self.delete(TableId::Accounts, id.as_bytes())
            .await
            .map_err(backend)?;
        Ok(true)
    }

    async fn account_member_count(&mut self, id: &str) -> Result<i64, ControlError> {
        Ok(scan_members(self)
            .await?
            .iter()
            .filter(|m| m.account_id == id)
            .count() as i64)
    }

    async fn account_bucket_count(&mut self, id: &str) -> Result<i64, ControlError> {
        Ok(counts_for(&bucket_accounts(self).await?, id))
    }

    async fn member_uid(
        &mut self,
        account_id: &str,
        username: &str,
    ) -> Result<Option<i64>, ControlError> {
        for m in scan_members(self).await? {
            if m.account_id == account_id
                && let Some(u) = user_row_by_uid(self, m.uid).await?
                && !u.is_admin
                && u.username == username
            {
                return Ok(Some(m.uid));
            }
        }
        Ok(None)
    }

    async fn account_member_upsert(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<(), ControlError> {
        let row = match member_row(self, uid).await? {
            Some(mut m) => {
                m.account_id = account_id.into();
                m.role = role.into();
                m
            }
            None => MemberRow {
                account_id: account_id.into(),
                uid,
                role: role.into(),
                created_at,
            },
        };
        self.put(TableId::AccountMembers, &uid_key(uid), &enc(&row)?)
            .await
            .map_err(backend)
    }

    async fn account_member_insert(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<(), ControlError> {
        install_member(
            self,
            MemberRow {
                account_id: account_id.into(),
                uid,
                role: role.into(),
                created_at,
            },
        )
        .await
    }

    async fn account_member_insert_if_absent(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<bool, ControlError> {
        if member_row(self, uid).await?.is_some() {
            return Ok(false);
        }
        self.put(
            TableId::AccountMembers,
            &uid_key(uid),
            &enc(&MemberRow {
                account_id: account_id.into(),
                uid,
                role: role.into(),
                created_at,
            })?,
        )
        .await
        .map_err(backend)?;
        Ok(true)
    }

    async fn account_set_admin_if_unset(
        &mut self,
        account_id: &str,
        uid: i64,
    ) -> Result<(), ControlError> {
        if let Some(mut row) = account_row(self, account_id).await?
            && row.admin_uid.is_none()
        {
            row.admin_uid = Some(uid);
            put_account(self, &row).await?;
        }
        Ok(())
    }

    async fn account_members_delete_admins(&mut self) -> Result<(), ControlError> {
        for m in scan_members(self).await? {
            if user_row_by_uid(self, m.uid)
                .await?
                .is_some_and(|u| u.is_admin)
            {
                self.delete(TableId::AccountMembers, &uid_key(m.uid))
                    .await
                    .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn accounts_delete_admin_derived(&mut self) -> Result<(), ControlError> {
        let members: HashSet<String> = scan_members(self)
            .await?
            .into_iter()
            .map(|m| m.account_id)
            .collect();
        let derived: HashSet<String> = scan_users(self)
            .await?
            .into_iter()
            .filter(|u| u.is_admin)
            .map(|u| format!("{:012}", u.uid))
            .collect();
        for row in scan_accounts(self).await? {
            if derived.contains(&row.id) && !members.contains(&row.id) {
                self.delete(TableId::Accounts, row.id.as_bytes())
                    .await
                    .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn bucket_quota_reached(&mut self, owner_id: Option<i64>) -> Result<bool, ControlError> {
        let Some(uid) = owner_id else {
            return Ok(false);
        };
        let account = match member_account(self, uid).await? {
            Some(account) => Some(account),
            None => xl_s3::access::account_id_of(uid),
        };
        let Some(account) = account else {
            return Ok(false);
        };
        let Some(row) = account_row(self, &account).await? else {
            return Ok(false);
        };
        if row.status != "active" {
            return Ok(true);
        }
        let Some(quota) = row.bucket_quota.filter(|q| *q >= 0) else {
            return Ok(false);
        };
        Ok(counts_for(&bucket_accounts(self).await?, &account) >= quota)
    }

    async fn account_ledger(
        &mut self,
        account_id: &str,
    ) -> Result<Option<(i64, i64)>, ControlError> {
        Ok(ledger_row(self, account_id)
            .await?
            .map(|r| (r.used_bytes, r.object_count)))
    }

    async fn account_ledger_upsert(
        &mut self,
        account_id: &str,
        used_bytes: i64,
        object_count: i64,
    ) -> Result<(), ControlError> {
        self.put(
            TableId::AccountUsageLedger,
            account_id.as_bytes(),
            &enc(&LedgerRow {
                account_id: account_id.into(),
                used_bytes,
                object_count,
            })?,
        )
        .await
        .map_err(backend)
    }

    async fn account_ledger_clear(&mut self) -> Result<(), ControlError> {
        for row in scan_ledger(self).await? {
            self.delete(TableId::AccountUsageLedger, row.account_id.as_bytes())
                .await
                .map_err(backend)?;
        }
        Ok(())
    }
}

async fn insert_account_absent(
    txn: &mut ObjectControlTxn,
    id: &str,
    name: &str,
    admin_uid: Option<i64>,
    bucket_quota: Option<i64>,
    created_at: i64,
    updated_at: i64,
) -> Result<bool, ControlError> {
    if account_row(txn, id).await?.is_some() {
        return Ok(false);
    }
    put_account(
        txn,
        &AccountRow {
            id: id.into(),
            name: name.into(),
            status: "active".into(),
            admin_uid,
            bucket_quota,
            soft_byte_quota: None,
            hard_byte_quota: None,
            object_quota: None,
            created_at,
            updated_at,
        },
    )
    .await?;
    Ok(true)
}

pub(crate) async fn put_account(
    txn: &mut ObjectControlTxn,
    row: &AccountRow,
) -> Result<(), ControlError> {
    txn.put(TableId::Accounts, row.id.as_bytes(), &enc(row)?)
        .await
        .map_err(backend)
}

async fn member_uids_for(txn: &ObjectControlTxn, id: &str) -> Result<Vec<i64>, ControlError> {
    Ok(scan_members(txn)
        .await?
        .into_iter()
        .filter(|m| m.account_id == id)
        .map(|m| m.uid)
        .collect())
}

/// FK `users ON DELETE` behavior: drop the member row and clear admin_uid.
pub(crate) async fn on_user_deleted(
    txn: &mut ObjectControlTxn,
    uid: i64,
) -> Result<(), ControlError> {
    if member_row(txn, uid).await?.is_some() {
        txn.delete(TableId::AccountMembers, &uid_key(uid))
            .await
            .map_err(backend)?;
    }
    for mut row in scan_accounts(txn).await? {
        if row.admin_uid == Some(uid) {
            row.admin_uid = None;
            put_account(txn, &row).await?;
        }
    }
    Ok(())
}
