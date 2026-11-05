//! Object-backed IAM writes (C1): raft-guard exception matrix, uid allocation,
//! duplicate-key mapping and the user-delete cascade. Runs inside the caller's
//! `ObjectControlTransaction`; on a root CAS conflict the whole closure re-runs.

use super::ObjectControlTxn;
use super::iam::{
    AccessKeyRow, UserRow, access_key_row, backend, enc, uid_key, user_row_by_uid,
    user_row_by_username,
};
use super::table::TableId;
use super::txn::RAFT_GUARD;
use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;

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
        .map_err(backend)
}
