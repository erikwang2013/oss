//! Object-backed IAM transaction methods (C1): users / access keys /
//! permissions inside the caller's transaction. Reads are overlay-aware so the
//! raft-guard exceptions and last-admin checks re-run on a CAS retry.

use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{AccessKeyEntry, IamTxn, PermissionRow, UserEntry, UserRef};

use super::ObjectControlTxn;
use super::account_write::on_user_deleted;
use super::iam::{
    AccessKeyRow, PermissionStorageRow, access_key_row, backend, dec, enc, permission_key,
    scan_access_keys, scan_permissions, scan_users, uid_key, user_row_by_uid, user_row_by_username,
    user_uid_by_username, users_missing_accounts,
};
use super::iam_write::{
    access_key_site_row, guard_key_old, guard_permission, guard_user_old, insert_access_key,
    insert_user, owner_is_admin, permission_site_row, user_site_row,
};
use super::table::TableId;

#[async_trait::async_trait]
impl IamTxn for ObjectControlTxn {
    async fn user_by_username(&mut self, username: &str) -> Result<Option<UserRef>, ControlError> {
        Ok(user_row_by_username(self, username)
            .await?
            .map(|u| UserRef {
                uid: u.uid,
                is_admin: u.is_admin,
            }))
    }

    async fn user_uid(&mut self, username: &str) -> Result<Option<i64>, ControlError> {
        user_uid_by_username(self, username).await
    }

    async fn admin_id(&mut self) -> Result<Option<i64>, ControlError> {
        Ok(scan_users(self)
            .await?
            .into_iter()
            .find(|u| u.is_admin)
            .map(|u| u.uid))
    }

    async fn user_uid_non_admin(&mut self, username: &str) -> Result<Option<i64>, ControlError> {
        Ok(user_row_by_username(self, username)
            .await?
            .filter(|u| !u.is_admin)
            .map(|u| u.uid))
    }

    async fn user_non_admin_exists(&mut self, uid: i64) -> Result<bool, ControlError> {
        Ok(user_row_by_uid(self, uid)
            .await?
            .is_some_and(|u| !u.is_admin))
    }

    async fn user_is_admin(&mut self, username: &str) -> Result<bool, ControlError> {
        Ok(user_row_by_username(self, username)
            .await?
            .is_some_and(|u| u.is_admin))
    }

    async fn legacy_users_missing_accounts(&mut self) -> Result<Vec<(i64, String)>, ControlError> {
        users_missing_accounts(self).await
    }

    async fn user_create(&mut self, row: &UserEntry) -> Result<i64, ControlError> {
        insert_user(
            self,
            &row.username,
            &row.password_hash,
            false,
            &row.status,
            row.created_at,
        )
        .await
    }

    async fn user_create_default_status(
        &mut self,
        username: &str,
        password_hash: &str,
        is_admin: bool,
        created_at: i64,
    ) -> Result<i64, ControlError> {
        insert_user(
            self,
            username,
            password_hash,
            is_admin,
            "active",
            created_at,
        )
        .await
    }

    async fn user_delete_guard(&mut self, uid: i64) -> Result<bool, ControlError> {
        let Some(row) = user_row_by_uid(self, uid).await? else {
            return Ok(false);
        };
        guard_user_old(self, Some(&row)).await?;
        if row.is_admin && row.status == "active" {
            let active = scan_users(self)
                .await?
                .iter()
                .filter(|u| u.is_admin && u.status == "active")
                .count();
            return Ok(active > 1);
        }
        Ok(true)
    }

    async fn user_delete_access_keys_permissions(&mut self, uid: i64) -> Result<(), ControlError> {
        for row in scan_access_keys(self).await? {
            if row.user_id == uid {
                guard_key_old(self, Some(&row)).await?;
                let site = access_key_site_row(self, &row).await?;
                self.delete(TableId::AccessKeys, row.access_key.as_bytes())
                    .await
                    .map_err(backend)?;
                self.site_hook_access_key(Some(&site), None, crate::db::now())
                    .await?;
            }
        }
        for row in scan_permissions(self).await? {
            if row.user_id == uid {
                guard_permission(self, row.user_id).await?;
                let site = permission_site_row(self, &row).await?;
                self.delete(TableId::Permissions, &permission_key(uid, &row.bucket))
                    .await
                    .map_err(backend)?;
                self.site_hook_permission(Some(&site), None, crate::db::now())
                    .await?;
            }
        }
        Ok(())
    }

    async fn user_delete(&mut self, uid: i64) -> Result<bool, ControlError> {
        let Some(row) = user_row_by_uid(self, uid).await? else {
            return Ok(false);
        };
        guard_user_old(self, Some(&row)).await?;
        let site = user_site_row(&row);
        self.delete(TableId::Users, &uid_key(uid))
            .await
            .map_err(backend)?;
        self.site_hook_user(Some(&site), None, crate::db::now())
            .await?;
        on_user_deleted(self, uid).await?;
        Ok(true)
    }

    async fn user_set_password(
        &mut self,
        uid: i64,
        password_hash: &str,
    ) -> Result<bool, ControlError> {
        let Some(mut row) = user_row_by_uid(self, uid).await? else {
            return Ok(false);
        };
        guard_user_old(self, Some(&row)).await?;
        let old = user_site_row(&row);
        row.password_hash = password_hash.to_owned();
        self.put(TableId::Users, &uid_key(uid), &enc(&row)?)
            .await
            .map_err(backend)?;
        self.site_hook_user(Some(&old), Some(&user_site_row(&row)), crate::db::now())
            .await?;
        Ok(true)
    }

    async fn user_set_status(&mut self, uid: i64, active: bool) -> Result<bool, ControlError> {
        let Some(mut row) = user_row_by_uid(self, uid).await? else {
            return Ok(false);
        };
        if !active && row.is_admin && row.status == "active" {
            let active_admins = scan_users(self)
                .await?
                .iter()
                .filter(|u| u.is_admin && u.status == "active")
                .count();
            if active_admins <= 1 {
                return Ok(false);
            }
        }
        guard_user_old(self, Some(&row)).await?;
        let old = user_site_row(&row);
        row.status = if active { "active" } else { "disabled" }.into();
        self.put(TableId::Users, &uid_key(uid), &enc(&row)?)
            .await
            .map_err(backend)?;
        self.site_hook_user(Some(&old), Some(&user_site_row(&row)), crate::db::now())
            .await?;
        Ok(true)
    }

    async fn user_set_status_value(
        &mut self,
        uid: i64,
        status: &str,
    ) -> Result<bool, ControlError> {
        let Some(mut row) = user_row_by_uid(self, uid).await? else {
            return Ok(false);
        };
        guard_user_old(self, Some(&row)).await?;
        let old = user_site_row(&row);
        row.status = status.to_owned();
        self.put(TableId::Users, &uid_key(uid), &enc(&row)?)
            .await
            .map_err(backend)?;
        self.site_hook_user(Some(&old), Some(&user_site_row(&row)), crate::db::now())
            .await?;
        Ok(true)
    }

    async fn permissions_replace(
        &mut self,
        uid: i64,
        rows: &[PermissionRow],
    ) -> Result<(), ControlError> {
        for row in scan_permissions(self).await? {
            if row.user_id == uid {
                guard_permission(self, uid).await?;
                let site = permission_site_row(self, &row).await?;
                self.delete(TableId::Permissions, &permission_key(uid, &row.bucket))
                    .await
                    .map_err(backend)?;
                self.site_hook_permission(Some(&site), None, crate::db::now())
                    .await?;
            }
        }
        for p in rows {
            guard_permission(self, uid).await?;
            let row = PermissionStorageRow {
                user_id: uid,
                bucket: p.bucket.clone(),
                can_read: p.read,
                can_write: p.write,
                can_delete: p.delete,
            };
            self.put(
                TableId::Permissions,
                &permission_key(uid, &p.bucket),
                &enc(&row)?,
            )
            .await
            .map_err(backend)?;
            let site = permission_site_row(self, &row).await?;
            self.site_hook_permission(None, Some(&site), crate::db::now())
                .await?;
        }
        Ok(())
    }

    async fn permission_upsert(
        &mut self,
        uid: i64,
        bucket: &str,
        read: bool,
        write: bool,
        delete: bool,
    ) -> Result<(), ControlError> {
        if let Some(mut row) = self
            .get(TableId::Permissions, &permission_key(uid, bucket))
            .await
            .map_err(backend)?
            .map(|bytes| dec::<PermissionStorageRow>(&bytes))
            .transpose()?
        {
            guard_permission(self, row.user_id).await?;
            let old = permission_site_row(self, &row).await?;
            row.can_read = read;
            row.can_write = write;
            row.can_delete = delete;
            self.put(
                TableId::Permissions,
                &permission_key(uid, bucket),
                &enc(&row)?,
            )
            .await
            .map_err(backend)?;
            let new = permission_site_row(self, &row).await?;
            self.site_hook_permission(Some(&old), Some(&new), crate::db::now())
                .await?;
        } else {
            guard_permission(self, uid).await?;
            let row = PermissionStorageRow {
                user_id: uid,
                bucket: bucket.to_owned(),
                can_read: read,
                can_write: write,
                can_delete: delete,
            };
            self.put(
                TableId::Permissions,
                &permission_key(uid, bucket),
                &enc(&row)?,
            )
            .await
            .map_err(backend)?;
            let site = permission_site_row(self, &row).await?;
            self.site_hook_permission(None, Some(&site), crate::db::now())
                .await?;
        }
        Ok(())
    }

    async fn permission_delete(&mut self, uid: i64, bucket: &str) -> Result<(), ControlError> {
        let key = permission_key(uid, bucket);
        if let Some(bytes) = self
            .get(TableId::Permissions, &key)
            .await
            .map_err(backend)?
        {
            let row: PermissionStorageRow = dec(&bytes)?;
            guard_permission(self, uid).await?;
            let site = permission_site_row(self, &row).await?;
            self.delete(TableId::Permissions, &key)
                .await
                .map_err(backend)?;
            self.site_hook_permission(Some(&site), None, crate::db::now())
                .await?;
        }
        Ok(())
    }

    async fn permissions_delete_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError> {
        for row in scan_permissions(self).await? {
            if row.bucket == bucket {
                guard_permission(self, row.user_id).await?;
                let site = permission_site_row(self, &row).await?;
                self.delete(TableId::Permissions, &permission_key(row.user_id, bucket))
                    .await
                    .map_err(backend)?;
                self.site_hook_permission(Some(&site), None, crate::db::now())
                    .await?;
            }
        }
        Ok(())
    }

    async fn access_key_exists(&mut self, access_key: &str) -> Result<bool, ControlError> {
        Ok(access_key_row(self, access_key).await?.is_some())
    }

    async fn access_key_create(&mut self, row: &AccessKeyEntry) -> Result<(), ControlError> {
        let Some(uid) = user_uid_by_username(self, &row.username).await? else {
            return Err(ControlError::Invalid(format!(
                "access key owner {} not found",
                row.username
            )));
        };
        insert_access_key(
            self,
            &AccessKeyRow {
                access_key: row.access_key.clone(),
                secret_key: row.secret_key.clone(),
                user_id: uid,
                status: "active".into(),
                expires_at: None,
                created_at: row.created_at,
                parent_ak: row.parent_ak.clone(),
                session_policy: row.session_policy.clone(),
                kind: row.kind.clone(),
            },
        )
        .await
    }

    async fn access_key_create_legacy(
        &mut self,
        access_key: &str,
        secret_key: &str,
        uid: i64,
        created_at: i64,
    ) -> Result<(), ControlError> {
        insert_access_key(
            self,
            &AccessKeyRow {
                access_key: access_key.into(),
                secret_key: secret_key.into(),
                user_id: uid,
                status: "active".into(),
                expires_at: None,
                created_at,
                parent_ak: None,
                session_policy: None,
                kind: "long-term".into(),
            },
        )
        .await
    }

    async fn access_key_delete(&mut self, access_key: &str) -> Result<bool, ControlError> {
        let Some(row) = access_key_row(self, access_key).await? else {
            return Ok(false);
        };
        guard_key_old(self, Some(&row)).await?;
        let site = access_key_site_row(self, &row).await?;
        self.delete(TableId::AccessKeys, access_key.as_bytes())
            .await
            .map_err(backend)?;
        self.site_hook_access_key(Some(&site), None, crate::db::now())
            .await?;
        Ok(true)
    }

    async fn access_key_set_status(
        &mut self,
        access_key: &str,
        status: &str,
    ) -> Result<bool, ControlError> {
        let Some(mut row) = access_key_row(self, access_key).await? else {
            return Ok(false);
        };
        guard_key_old(self, Some(&row)).await?;
        let old = access_key_site_row(self, &row).await?;
        row.status = status.to_owned();
        self.put(TableId::AccessKeys, access_key.as_bytes(), &enc(&row)?)
            .await
            .map_err(backend)?;
        let new = access_key_site_row(self, &row).await?;
        self.site_hook_access_key(Some(&old), Some(&new), crate::db::now())
            .await?;
        Ok(true)
    }

    async fn access_key_owner_is_admin(&mut self, access_key: &str) -> Result<bool, ControlError> {
        match access_key_row(self, access_key).await? {
            Some(row) => owner_is_admin(self, row.user_id).await,
            None => Ok(false),
        }
    }

    async fn access_key_long_term_owner_is_admin(
        &mut self,
        access_key: &str,
    ) -> Result<bool, ControlError> {
        match access_key_row(self, access_key).await? {
            Some(row) if row.kind == "long-term" => owner_is_admin(self, row.user_id).await,
            _ => Ok(false),
        }
    }

    async fn access_key_user_and_kind(
        &mut self,
        access_key: &str,
    ) -> Result<Option<(i64, String)>, ControlError> {
        Ok(access_key_row(self, access_key)
            .await?
            .map(|row| (row.user_id, row.kind)))
    }

    async fn access_key_delete_long_term(
        &mut self,
        access_key: &str,
    ) -> Result<bool, ControlError> {
        match access_key_row(self, access_key).await? {
            Some(row) if row.kind == "long-term" => {
                guard_key_old(self, Some(&row)).await?;
                let site = access_key_site_row(self, &row).await?;
                self.delete(TableId::AccessKeys, access_key.as_bytes())
                    .await
                    .map_err(backend)?;
                self.site_hook_access_key(Some(&site), None, crate::db::now())
                    .await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn access_key_site_upsert(
        &mut self,
        access_key: &str,
        secret_key: &str,
        uid: i64,
        status: &str,
        expires_at: Option<i64>,
        created_at: i64,
    ) -> Result<(), ControlError> {
        if let Some(mut row) = access_key_row(self, access_key).await? {
            guard_key_old(self, Some(&row)).await?;
            let old = access_key_site_row(self, &row).await?;
            row.secret_key = secret_key.to_owned();
            row.user_id = uid;
            row.status = status.to_owned();
            row.expires_at = expires_at;
            self.put(TableId::AccessKeys, access_key.as_bytes(), &enc(&row)?)
                .await
                .map_err(backend)?;
            let new = access_key_site_row(self, &row).await?;
            self.site_hook_access_key(Some(&old), Some(&new), crate::db::now())
                .await?;
        } else {
            insert_access_key(
                self,
                &AccessKeyRow {
                    access_key: access_key.into(),
                    secret_key: secret_key.into(),
                    user_id: uid,
                    status: status.to_owned(),
                    expires_at,
                    created_at,
                    parent_ak: None,
                    session_policy: None,
                    kind: "long-term".into(),
                },
            )
            .await?;
        }
        Ok(())
    }
}
