//! Object-backed IAM writes (C1): raft-guard exception matrix, uid allocation,
//! duplicate-key mapping and the user-delete cascade. Runs inside the caller's
//! `ObjectControlTransaction`; on a root CAS conflict the whole closure re-runs.

use super::ObjectControlTxn;
use super::iam::{
    AccessKeyRow, PermissionStorageRow, UserRow, access_key_row, backend, enc, uid_key,
    user_row_by_uid, user_row_by_username,
};
use super::site_apply::{AccessKeySiteRow, PermissionSiteRow, UserSiteRow};
use super::table::TableId;
use super::txn::RAFT_GUARD;
use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;

pub(crate) fn user_site_row(row: &UserRow) -> UserSiteRow {
    UserSiteRow {
        username: row.username.clone(),
        password_hash: row.password_hash.clone(),
        status: row.status.clone(),
        created_at: row.created_at,
        is_admin: row.is_admin,
    }
}

pub(crate) async fn access_key_site_row(
    txn: &ObjectControlTxn,
    row: &AccessKeyRow,
) -> Result<AccessKeySiteRow, ControlError> {
    let owner = user_row_by_uid(txn, row.user_id).await?;
    Ok(AccessKeySiteRow {
        access_key: row.access_key.clone(),
        secret_key: row.secret_key.clone(),
        username: owner.as_ref().map(|u| u.username.clone()),
        status: row.status.clone(),
        expires_at: row.expires_at,
        created_at: row.created_at,
        kind: row.kind.clone(),
        owner_is_admin: owner.as_ref().map(|u| u.is_admin),
    })
}

pub(crate) async fn permission_site_row(
    txn: &ObjectControlTxn,
    row: &PermissionStorageRow,
) -> Result<PermissionSiteRow, ControlError> {
    let owner = user_row_by_uid(txn, row.user_id).await?;
    Ok(PermissionSiteRow {
        username: owner.as_ref().map(|u| u.username.clone()),
        bucket: row.bucket.clone(),
        can_read: row.can_read,
        can_write: row.can_write,
        can_delete: row.can_delete,
        owner_is_admin: owner.as_ref().map(|u| u.is_admin),
    })
}

/// True when the raft guard does not apply: standalone mode, or a replicated
/// origin (Raft apply / snapshot install) is the writer.
pub(crate) fn guard_free(txn: &ObjectControlTxn) -> bool {
    txn.meta().mode != Mode::Raft || txn.origin().is_replicated()
}

fn deny() -> ControlError {
    ControlError::Invalid(RAFT_GUARD.into())
}

pub(crate) async fn owner_is_admin(txn: &ObjectControlTxn, uid: i64) -> Result<bool, ControlError> {
    Ok(user_row_by_uid(txn, uid).await?.is_some_and(|u| u.is_admin))
}

async fn guard_user_new(txn: &ObjectControlTxn, is_admin: bool) -> Result<(), ControlError> {
    if guard_free(txn) || is_admin {
        Ok(())
    } else {
        Err(deny())
    }
}

/// OLD row guard for users UPDATE/DELETE; absent rows fire no SQL trigger.
pub(crate) async fn guard_user_old(
    txn: &ObjectControlTxn,
    row: Option<&UserRow>,
) -> Result<(), ControlError> {
    if guard_free(txn) || row.is_none_or(|u| u.is_admin) {
        Ok(())
    } else {
        Err(deny())
    }
}

pub(crate) async fn guard_key_old(
    txn: &ObjectControlTxn,
    row: Option<&AccessKeyRow>,
) -> Result<(), ControlError> {
    if guard_free(txn) {
        return Ok(());
    }
    let Some(row) = row else { return Ok(()) };
    if row.kind == "session" || owner_is_admin(txn, row.user_id).await? {
        Ok(())
    } else {
        Err(deny())
    }
}

async fn guard_key_new(txn: &ObjectControlTxn, kind: &str, uid: i64) -> Result<(), ControlError> {
    if guard_free(txn) || kind == "session" || owner_is_admin(txn, uid).await? {
        Ok(())
    } else {
        Err(deny())
    }
}

pub(crate) async fn guard_permission(txn: &ObjectControlTxn, uid: i64) -> Result<(), ControlError> {
    if guard_free(txn) || owner_is_admin(txn, uid).await? {
        Ok(())
    } else {
        Err(deny())
    }
}

pub(crate) async fn insert_user(
    txn: &mut ObjectControlTxn,
    username: &str,
    password_hash: &str,
    is_admin: bool,
    status: &str,
    created_at: i64,
) -> Result<i64, ControlError> {
    if user_row_by_username(txn, username).await?.is_some() {
        return Err(ControlError::Backend(
            "UNIQUE constraint failed: users.username".into(),
        ));
    }
    guard_user_new(txn, is_admin).await?;
    let uid = txn.alloc_uid().map_err(backend)? as i64;
    let row = UserRow {
        uid,
        username: username.to_owned(),
        password_hash: password_hash.to_owned(),
        is_admin,
        status: status.to_owned(),
        created_at,
    };
    txn.put(TableId::Users, &uid_key(uid), &enc(&row)?)
        .await
        .map_err(backend)?;
    txn.site_hook_user(None, Some(&user_site_row(&row)), crate::db::now())
        .await?;
    Ok(uid)
}

pub(crate) async fn insert_access_key(
    txn: &mut ObjectControlTxn,
    row: &AccessKeyRow,
) -> Result<(), ControlError> {
    if access_key_row(txn, &row.access_key).await?.is_some() {
        return Err(ControlError::Backend(
            "UNIQUE constraint failed: access_keys.access_key".into(),
        ));
    }
    guard_key_new(txn, &row.kind, row.user_id).await?;
    txn.put(TableId::AccessKeys, row.access_key.as_bytes(), &enc(row)?)
        .await
        .map_err(backend)?;
    let site = access_key_site_row(txn, row).await?;
    txn.site_hook_access_key(None, Some(&site), crate::db::now())
        .await
}
