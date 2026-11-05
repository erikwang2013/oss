//! Object-backed IAM snapshot export/install (C1).
//!
//! Strict export keeps only replicated rows (non-admin users, non-admin
//! long-term root access keys, permissions of non-admin owners) in the same
//! orders as the SQLite adapter. Install runs under `SnapshotInstall`, so the
//! raft guards do not apply; local uids are resolved by username by the caller.

use std::collections::{HashMap, HashSet};

use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{AccessKeyEntry, IamSnapshotTxn, PermissionEntry, UserEntry};

use super::ObjectControlTxn;
use super::account_write::on_user_deleted;
use super::iam::{
    AccessKeyRow, PermissionStorageRow, UserRow, backend, enc, permission_key, scan_access_keys,
    scan_permissions, scan_users, uid_key, user_row_by_uid, user_row_by_username,
};
use super::table::TableId;

async fn username_map(txn: &ObjectControlTxn) -> Result<HashMap<i64, UserRow>, ControlError> {
    Ok(scan_users(txn)
        .await?
        .into_iter()
        .map(|u| (u.uid, u))
        .collect())
}

fn user_entry(row: &UserRow) -> UserEntry {
    UserEntry {
        username: row.username.clone(),
        password_hash: row.password_hash.clone(),
        status: row.status.clone(),
        created_at: row.created_at,
    }
}

#[async_trait::async_trait]
impl IamSnapshotTxn for ObjectControlTxn {
    async fn snapshot_users(&mut self) -> Result<Vec<UserEntry>, ControlError> {
        let mut rows: Vec<UserEntry> = scan_users(self)
            .await?
            .iter()
            .filter(|u| !u.is_admin)
            .map(user_entry)
            .collect();
        rows.sort_by(|a, b| a.username.cmp(&b.username));
        Ok(rows)
    }

    async fn snapshot_access_keys(&mut self) -> Result<Vec<AccessKeyEntry>, ControlError> {
        let users = username_map(self).await?;
        let mut rows: Vec<AccessKeyEntry> = scan_access_keys(self)
            .await?
            .into_iter()
            .filter(|k| k.kind == "long-term" && k.parent_ak.is_none())
            .filter_map(|k| {
                let owner = users.get(&k.user_id)?;
                (!owner.is_admin).then(|| AccessKeyEntry {
                    access_key: k.access_key,
                    secret_key: k.secret_key,
                    username: owner.username.clone(),
                    status: k.status,
                    expires_at: k.expires_at,
                    created_at: k.created_at,
                    parent_ak: k.parent_ak,
                    session_policy: k.session_policy,
                    kind: k.kind,
                })
            })
            .collect();
        rows.sort_by(|a, b| a.access_key.cmp(&b.access_key));
        Ok(rows)
    }

    async fn snapshot_permissions(&mut self) -> Result<Vec<PermissionEntry>, ControlError> {
        let users = username_map(self).await?;
        let mut rows: Vec<PermissionEntry> = scan_permissions(self)
            .await?
            .into_iter()
            .filter_map(|p| {
                let owner = users.get(&p.user_id)?;
                (!owner.is_admin).then(|| PermissionEntry {
                    username: owner.username.clone(),
                    bucket: p.bucket,
                    can_read: p.can_read,
                    can_write: p.can_write,
                    can_delete: p.can_delete,
                })
            })
            .collect();
        rows.sort_by(|a, b| (&a.username, &a.bucket).cmp(&(&b.username, &b.bucket)));
        Ok(rows)
    }

    async fn users_delete_non_admin(&mut self) -> Result<(), ControlError> {
        for row in scan_users(self).await? {
            if !row.is_admin {
                self.delete(TableId::Users, &uid_key(row.uid))
                    .await
                    .map_err(backend)?;
                on_user_deleted(self, row.uid).await?;
            }
        }
        Ok(())
    }

    async fn user_install(&mut self, row: &UserEntry) -> Result<i64, ControlError> {
        if user_row_by_username(self, &row.username).await?.is_some() {
            return Err(ControlError::Backend(
                "UNIQUE constraint failed: users.username".into(),
            ));
        }
        let uid = loop {
            let candidate = self.alloc_uid().map_err(backend)? as i64;
            if user_row_by_uid(self, candidate).await?.is_none() {
                break candidate;
            }
        };
        self.put(
            TableId::Users,
            &uid_key(uid),
            &enc(&UserRow {
                uid,
                username: row.username.clone(),
                password_hash: row.password_hash.clone(),
                is_admin: false,
                status: row.status.clone(),
                created_at: row.created_at,
            })?,
        )
        .await
        .map_err(backend)?;
        Ok(uid)
    }

    async fn access_keys_delete_non_admin(&mut self) -> Result<(), ControlError> {
        let admins: HashSet<i64> = scan_users(self)
            .await?
            .into_iter()
            .filter(|u| u.is_admin)
            .map(|u| u.uid)
            .collect();
        for row in scan_access_keys(self).await? {
            if !admins.contains(&row.user_id) {
                self.delete(TableId::AccessKeys, row.access_key.as_bytes())
                    .await
                    .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn access_key_install(
        &mut self,
        row: &AccessKeyEntry,
        uid: i64,
    ) -> Result<(), ControlError> {
        if self
            .get(TableId::AccessKeys, row.access_key.as_bytes())
            .await
            .map_err(backend)?
            .is_some()
        {
            return Err(ControlError::Backend(
                "UNIQUE constraint failed: access_keys.access_key".into(),
            ));
        }
        self.put(
            TableId::AccessKeys,
            row.access_key.as_bytes(),
            &enc(&AccessKeyRow {
                access_key: row.access_key.clone(),
                secret_key: row.secret_key.clone(),
                user_id: uid,
                status: row.status.clone(),
                expires_at: row.expires_at,
                created_at: row.created_at,
                parent_ak: row.parent_ak.clone(),
                session_policy: row.session_policy.clone(),
                kind: row.kind.clone(),
            })?,
        )
        .await
        .map_err(backend)
    }

    async fn permissions_clear(&mut self) -> Result<(), ControlError> {
        for row in scan_permissions(self).await? {
            self.delete(
                TableId::Permissions,
                &permission_key(row.user_id, &row.bucket),
            )
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn permission_install(
        &mut self,
        row: &PermissionEntry,
        uid: i64,
    ) -> Result<(), ControlError> {
        let key = permission_key(uid, &row.bucket);
        if self
            .get(TableId::Permissions, &key)
            .await
            .map_err(backend)?
            .is_some()
        {
            return Err(ControlError::Backend(
                "UNIQUE constraint failed: permissions.user_id, permissions.bucket".into(),
            ));
        }
        self.put(
            TableId::Permissions,
            &key,
            &enc(&PermissionStorageRow {
                user_id: uid,
                bucket: row.bucket.clone(),
                can_read: row.can_read,
                can_write: row.can_write,
                can_delete: row.can_delete,
            })?,
        )
        .await
        .map_err(backend)
    }

    async fn permissions_delete_admins(&mut self) -> Result<(), ControlError> {
        let admins: HashSet<i64> = scan_users(self)
            .await?
            .into_iter()
            .filter(|u| u.is_admin)
            .map(|u| u.uid)
            .collect();
        for row in scan_permissions(self).await? {
            if admins.contains(&row.user_id) {
                self.delete(
                    TableId::Permissions,
                    &permission_key(row.user_id, &row.bucket),
                )
                .await
                .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn access_keys_delete_orphan_sessions(&mut self) -> Result<(), ControlError> {
        let users: HashSet<i64> = scan_users(self).await?.into_iter().map(|u| u.uid).collect();
        for row in scan_access_keys(self).await? {
            let orphan_session = row.kind == "session" || row.parent_ak.is_some();
            if orphan_session && !users.contains(&row.user_id) {
                self.delete(TableId::AccessKeys, row.access_key.as_bytes())
                    .await
                    .map_err(backend)?;
            }
        }
        Ok(())
    }
}
