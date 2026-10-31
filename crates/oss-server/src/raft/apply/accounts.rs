//! Level-3 账户/成员命令 apply：自然键（account id / username）+ 与 db.rs 直写方法
//! 相同的业务约束。业务拒绝走 `ControlResponse`，不产生内部错误。
//!
//! 账户表进 OSSCTL3 快照时按 username 规范化（`ControlData`），所以这里绝不把 uid
//! 写进命令；apply 在本地解析 username → uid。

use super::ControlWrite;
use crate::db::Db;
use crate::raft::command::{ControlCommand, ControlResponse};
use xl_s3::access::{LEGACY_ACCOUNT, valid_account_id};

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
    sqlx::query_scalar("SELECT status FROM accounts WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut *cw.conn)
        .await
}

/// 非全局管理员的本地 uid（全局管理员不是任何账户的成员）。
async fn non_admin_uid(
    cw: &mut ControlWrite<'_>,
    username: &str,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar("SELECT id FROM users WHERE username = ? AND is_admin = 0")
        .bind(username)
        .fetch_optional(&mut *cw.conn)
        .await
}

async fn member_uid(
    cw: &mut ControlWrite<'_>,
    account_id: &str,
    username: &str,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT u.id FROM account_members am JOIN users u ON u.id = am.uid \
         WHERE am.account_id = ? AND u.username = ? AND u.is_admin = 0",
    )
    .bind(account_id)
    .bind(username)
    .fetch_optional(&mut *cw.conn)
    .await
}

async fn account_bucket_count(cw: &mut ControlWrite<'_>, id: &str) -> Result<i64, sqlx::Error> {
    let sql = format!(
        "SELECT {} FROM accounts a WHERE a.id = ?",
        Db::account_bucket_count_sql()
    );
    let n: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .fetch_optional(&mut *cw.conn)
        .await?;
    Ok(n.unwrap_or(0))
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
                let n = sqlx::query(
                    "INSERT OR IGNORE INTO accounts \
                     (id, name, status, bucket_quota, created_at, updated_at) \
                     VALUES (?, ?, 'active', ?, ?, ?)",
                )
                .bind(id)
                .bind(name)
                .bind(bucket_quota)
                .bind(created_at)
                .bind(created_at)
                .execute(&mut *cw.conn)
                .await?
                .rows_affected();
                if n == 1 {
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
                    let n =
                        sqlx::query("UPDATE accounts SET name = ?, updated_at = ? WHERE id = ?")
                            .bind(name)
                            .bind(updated_at)
                            .bind(id)
                            .execute(&mut *cw.conn)
                            .await?
                            .rows_affected();
                    missing = n != 1;
                }
                if missing {
                    ControlResponse::NotFound
                } else if let Some(status) = status {
                    let n = if status == "active" {
                        sqlx::query(
                            "UPDATE accounts SET status = 'active', updated_at = ? WHERE id = ?",
                        )
                        .bind(updated_at)
                        .bind(id)
                        .execute(&mut *cw.conn)
                        .await?
                        .rows_affected()
                    } else {
                        // 同 db.rs：停用是原子条件更新，含在册全局管理员的账户拒绝。
                        sqlx::query(
                            "UPDATE accounts SET status = 'disabled', updated_at = ? WHERE id = ? \
                             AND NOT EXISTS (SELECT 1 FROM account_members am \
                               JOIN users u ON u.id = am.uid \
                               WHERE am.account_id = accounts.id \
                                 AND u.is_admin = 1 AND u.status = 'active')",
                        )
                        .bind(updated_at)
                        .bind(id)
                        .execute(&mut *cw.conn)
                        .await?
                        .rows_affected()
                    };
                    if n == 1 {
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
                let n = sqlx::query(
                    "UPDATE accounts SET bucket_quota = ?, updated_at = ? WHERE id = ?",
                )
                .bind(bucket_quota)
                .bind(updated_at)
                .bind(id)
                .execute(&mut *cw.conn)
                .await?
                .rows_affected();
                if n == 1 {
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
                let n = sqlx::query(
                    "UPDATE accounts SET soft_byte_quota = ?, updated_at = ? WHERE id = ?",
                )
                .bind(soft_byte_quota)
                .bind(updated_at)
                .bind(id)
                .execute(&mut *cw.conn)
                .await?
                .rows_affected();
                if n == 1 {
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
                let n = sqlx::query(
                    "UPDATE accounts SET hard_byte_quota = ?, object_quota = ?, updated_at = ? \
                     WHERE id = ?",
                )
                .bind(hard_byte_quota)
                .bind(object_quota)
                .bind(updated_at)
                .bind(id)
                .execute(&mut *cw.conn)
                .await?
                .rows_affected();
                if n == 1 {
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
                let members: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = ?")
                        .bind(id)
                        .fetch_one(&mut *cw.conn)
                        .await?;
                if members != 0 || account_bucket_count(cw, id).await? != 0 {
                    ControlResponse::Conflict
                } else {
                    sqlx::query("DELETE FROM accounts WHERE id = ?")
                        .bind(id)
                        .execute(&mut *cw.conn)
                        .await?;
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
                            sqlx::query(
                                "INSERT INTO account_members (account_id, uid, role, created_at) \
                                 VALUES (?, ?, ?, ?) \
                                 ON CONFLICT(uid) DO UPDATE SET \
                                   account_id = excluded.account_id, role = excluded.role",
                            )
                            .bind(account_id)
                            .bind(uid)
                            .bind(role)
                            .bind(created_at)
                            .execute(&mut *cw.conn)
                            .await?;
                            if role == "admin" {
                                sqlx::query(
                                    "UPDATE accounts SET admin_uid = ? \
                                     WHERE id = ? AND admin_uid IS NULL",
                                )
                                .bind(uid)
                                .bind(account_id)
                                .execute(&mut *cw.conn)
                                .await?;
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
                if Db::set_user_status_raw(cw, uid, *active).await? {
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
                        let exists: Option<i64> =
                            sqlx::query_scalar("SELECT 1 FROM users WHERE username = ?")
                                .bind(username)
                                .fetch_optional(&mut *cw.conn)
                                .await?;
                        if exists.is_some() {
                            ControlResponse::Conflict
                        } else {
                            let uid = sqlx::query(
                                "INSERT INTO users (username, password_hash, is_admin, created_at) \
                                 VALUES (?, ?, 0, ?)",
                            )
                            .bind(username)
                            .bind(password_hash)
                            .bind(created_at)
                            .execute(&mut *cw.conn)
                            .await?
                            .last_insert_rowid();
                            sqlx::query(
                                "INSERT INTO account_members (account_id, uid, role, created_at) \
                                 VALUES (?, ?, ?, ?)",
                            )
                            .bind(account_id)
                            .bind(uid)
                            .bind(role)
                            .bind(created_at)
                            .execute(&mut *cw.conn)
                            .await?;
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
