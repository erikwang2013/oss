//! Level-3 账户/成员命令 apply：自然键（account id / username）+ 与 db.rs 直写方法
//! 相同的业务约束。业务拒绝走 `ControlResponse`，不产生内部错误。
//!
//! 账户表进 OSSCTL3 快照时按 username 规范化（`ControlData`），所以这里绝不把 uid
//! 写进命令；apply 在本地解析 username → uid。

use super::ControlWrite;
use crate::raft::command::{ControlCommand, ControlResponse};
use xl_s3::access::{LEGACY_ACCOUNT, valid_account_id};
use xl_store::control_store::account::AccountTxn;
use xl_store::control_store::iam::IamTxn;

fn valid_account_name(name: &str) -> bool {
    let trimmed = name.trim();
    !trimmed.is_empty() && trimmed.chars().count() <= 128
}

fn valid_role(role: &str) -> bool {
    matches!(role, "admin" | "member")
}

async fn account_status(
    cw: &mut ControlWrite<'_>,
    id: &str,
) -> Result<Option<String>, sqlx::Error> {
    crate::control_store::SqliteControlTxn::new(cw.conn())
        .account_status(id)
        .await
        .map_err(crate::control_store::to_sqlx)
}

/// 非全局管理员的本地 uid（全局管理员不是任何账户的成员）。
async fn non_admin_uid(
    cw: &mut ControlWrite<'_>,
    username: &str,
) -> Result<Option<i64>, sqlx::Error> {
    crate::control_store::SqliteControlTxn::new(cw.conn())
        .user_uid_non_admin(username)
        .await
        .map_err(crate::control_store::to_sqlx)
}

async fn member_uid(
    cw: &mut ControlWrite<'_>,
    account_id: &str,
    username: &str,
) -> Result<Option<i64>, sqlx::Error> {
    crate::control_store::SqliteControlTxn::new(cw.conn())
        .member_uid(account_id, username)
        .await
        .map_err(crate::control_store::to_sqlx)
}

async fn account_bucket_count(cw: &mut ControlWrite<'_>, id: &str) -> Result<i64, sqlx::Error> {
    crate::control_store::SqliteControlTxn::new(cw.conn())
        .account_bucket_count(id)
        .await
        .map_err(crate::control_store::to_sqlx)
}

/// 账户/成员命令的 apply 入口；非本组命令返回 `None`。
pub(super) async fn apply(
    cw: &mut ControlWrite<'_>,
    cmd: &ControlCommand,
) -> Result<Option<ControlResponse>, sqlx::Error> {
    let response = match cmd {
        ControlCommand::AccountCreate {
            id,
            name,
            bucket_quota,
            created_at,
        } => {
            if id == LEGACY_ACCOUNT
                || !valid_account_id(id)
                || !valid_account_name(name)
                || bucket_quota.is_some_and(|q| q < 0)
            {
                ControlResponse::Invalid
            } else {
                let inserted = crate::control_store::SqliteControlTxn::new(cw.conn())
                    .account_insert_if_absent(id, name, *bucket_quota, *created_at, *created_at)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
                if inserted {
                    ControlResponse::Ok
                } else {
                    ControlResponse::Conflict
                }
            }
        }
        ControlCommand::AccountUpdate {
            id,
            name,
            status,
            updated_at,
        } => {
            if name.as_deref().is_some_and(|n| !valid_account_name(n))
                || status
                    .as_deref()
                    .is_some_and(|s| !matches!(s, "active" | "disabled"))
            {
                ControlResponse::Invalid
            } else {
                let mut missing = false;
                if let Some(name) = name {
                    missing = !crate::control_store::SqliteControlTxn::new(cw.conn())
                        .account_set_name(id, name, *updated_at)
                        .await
                        .map_err(crate::control_store::to_sqlx)?;
                }
                if missing {
                    ControlResponse::NotFound
                } else if let Some(status) = status {
                    // 同 db.rs：停用是原子条件更新，含在册全局管理员的账户拒绝。
                    let changed = crate::control_store::SqliteControlTxn::new(cw.conn())
                        .account_set_status(id, status, *updated_at)
                        .await
                        .map_err(crate::control_store::to_sqlx)?;
                    if changed {
                        ControlResponse::Ok
                    } else if account_status(cw, id).await?.is_some() {
                        ControlResponse::Conflict
                    } else {
                        ControlResponse::NotFound
                    }
                } else {
                    ControlResponse::Ok
                }
            }
        }
        ControlCommand::AccountSetQuota {
            id,
            bucket_quota,
            updated_at,
        } => {
            if bucket_quota.is_some_and(|q| q < 0) {
                ControlResponse::Invalid
            } else {
                let changed = crate::control_store::SqliteControlTxn::new(cw.conn())
                    .account_set_bucket_quota(id, *bucket_quota, *updated_at)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
                if changed {
                    ControlResponse::Ok
                } else {
                    ControlResponse::NotFound
                }
            }
        }
        ControlCommand::AccountSetSoftQuota {
            id,
            soft_byte_quota,
            updated_at,
        } => {
            if soft_byte_quota.is_some_and(|q| q < 0) {
                ControlResponse::Invalid
            } else {
                let changed = crate::control_store::SqliteControlTxn::new(cw.conn())
                    .account_set_soft_quota(id, *soft_byte_quota, *updated_at)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
                if changed {
                    ControlResponse::Ok
                } else {
                    ControlResponse::NotFound
                }
            }
        }
        ControlCommand::AccountHardQuotaSet {
            id,
            hard_byte_quota,
            object_quota,
            updated_at,
        } => {
            if hard_byte_quota.is_some_and(|q| q < 0) || object_quota.is_some_and(|q| q < 0) {
                ControlResponse::Invalid
            } else {
                let changed = crate::control_store::SqliteControlTxn::new(cw.conn())
                    .account_set_hard_limits(id, *hard_byte_quota, *object_quota, *updated_at)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
                if changed {
                    ControlResponse::Ok
                } else {
                    ControlResponse::NotFound
                }
            }
        }
        ControlCommand::AccountDelete { id } => {
            if id == LEGACY_ACCOUNT {
                ControlResponse::Invalid
            } else if account_status(cw, id).await?.is_none() {
                ControlResponse::NotFound
            } else {
                let members = crate::control_store::SqliteControlTxn::new(cw.conn())
                    .account_member_count(id)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
                if members != 0 || account_bucket_count(cw, id).await? != 0 {
                    ControlResponse::Conflict
                } else {
                    crate::control_store::SqliteControlTxn::new(cw.conn())
                        .account_delete(id)
                        .await
                        .map_err(crate::control_store::to_sqlx)?;
                    ControlResponse::Ok
                }
            }
        }
        ControlCommand::AccountMemberAttach {
            account_id,
            username,
            role,
            created_at,
        } => {
            if account_id == LEGACY_ACCOUNT || !valid_role(role) {
                ControlResponse::Invalid
            } else {
                match account_status(cw, account_id).await? {
                    None => ControlResponse::NotFound,
                    Some(s) if s != "active" => ControlResponse::Invalid,
                    Some(_) => match non_admin_uid(cw, username).await? {
                        None => ControlResponse::NotFound,
                        Some(uid) => {
                            let mut txn = crate::control_store::SqliteControlTxn::new(cw.conn());
                            txn.account_member_upsert(account_id, uid, role, *created_at)
                                .await
                                .map_err(crate::control_store::to_sqlx)?;
                            if role == "admin" {
                                txn.account_set_admin_if_unset(account_id, uid)
                                    .await
                                    .map_err(crate::control_store::to_sqlx)?;
                            }
                            ControlResponse::Ok
                        }
                    },
                }
            }
        }
        ControlCommand::AccountMemberSetStatus {
            account_id,
            username,
            active,
        } => match member_uid(cw, account_id, username).await? {
            None => ControlResponse::NotFound,
            Some(uid) => {
                if crate::control_store::SqliteControlTxn::new(cw.conn())
                    .user_set_status(uid, *active)
                    .await
                    .map_err(crate::control_store::to_sqlx)?
                {
                    ControlResponse::Ok
                } else {
                    // 同 set_user_status：目标是最后一个在册管理员（命令路径本不产生）。
                    ControlResponse::Conflict
                }
            }
        },
        ControlCommand::AccountUserCreate {
            account_id,
            username,
            password_hash,
            role,
            created_at,
        } => {
            if account_id == LEGACY_ACCOUNT
                || role != "member"
                || username.trim().is_empty()
                || password_hash.is_empty()
            {
                ControlResponse::Invalid
            } else {
                match account_status(cw, account_id).await? {
                    None => ControlResponse::NotFound,
                    Some(s) if s != "active" => ControlResponse::Invalid,
                    Some(_) => {
                        let mut txn = crate::control_store::SqliteControlTxn::new(cw.conn());
                        if txn
                            .user_by_username(username)
                            .await
                            .map_err(crate::control_store::to_sqlx)?
                            .is_some()
                        {
                            ControlResponse::Conflict
                        } else {
                            let uid = txn
                                .user_create_default_status(
                                    username,
                                    password_hash,
                                    false,
                                    *created_at,
                                )
                                .await
                                .map_err(crate::control_store::to_sqlx)?;
                            txn.account_member_insert(account_id, uid, role, *created_at)
                                .await
                                .map_err(crate::control_store::to_sqlx)?;
                            ControlResponse::Ok
                        }
                    }
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(response))
}
