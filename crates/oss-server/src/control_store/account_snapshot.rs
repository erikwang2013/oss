//! 账户 / 成员 / 账本快照导出与安装（调用方事务内）。
//!
//! 导出按 username 规范化；安装把 username 解析成本地非管理员 uid（跨节点 uid
//! 不稳定），校验失败文本与旧 `raft/data.rs` 完全一致。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::account::{
    AccountEntry, AccountMemberEntry, AccountSnapshotTxn, AccountUsageLedgerEntry,
};

use super::account_write::install_account;
use super::{SqliteControlTxn, backend};

async fn non_admin_uid(
    txn: &mut SqliteControlTxn<'_>,
    username: &str,
    what: &str,
) -> Result<i64, ControlError> {
    sqlx::query_scalar("SELECT id FROM users WHERE username = ? AND is_admin = 0")
        .bind(username)
        .fetch_optional(&mut *txn.conn)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            ControlError::Invalid(format!(
                "{what} {username:?} is not a replicated non-admin user"
            ))
        })
}

#[async_trait::async_trait]
impl AccountSnapshotTxn for SqliteControlTxn<'_> {
    async fn snapshot_accounts(&mut self) -> Result<Vec<AccountEntry>, ControlError> {
        let rows = sqlx::query(
            "SELECT a.id, a.name, a.status, a.admin_uid, a.bucket_quota, a.soft_byte_quota, \
                    a.created_at, a.updated_at, u.username AS admin_username, \
                    u.is_admin AS admin_is_admin \
             FROM accounts a LEFT JOIN users u ON u.id = a.admin_uid ORDER BY a.id",
        )
        .fetch_all(&mut *self.conn)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let admin_uid: Option<i64> = r.get("admin_uid");
            let admin_username: Option<String> = r.get("admin_username");
            let admin_is_admin: Option<bool> = r.get("admin_is_admin");
            if admin_uid.is_some() && (admin_username.is_none() || admin_is_admin == Some(true)) {
                return Err(ControlError::Invalid(format!(
                    "account {} admin_uid {} is missing or a global admin (not exportable)",
                    r.get::<String, _>("id"),
                    admin_uid.unwrap_or_default()
                )));
            }
            out.push(AccountEntry {
                id: r.get("id"),
                name: r.get("name"),
                status: r.get("status"),
                admin: admin_username,
                bucket_quota: r.get("bucket_quota"),
                soft_byte_quota: r.get("soft_byte_quota"),
                created_at: r.get("created_at"),
                updated_at: r.get("updated_at"),
            });
        }
        Ok(out)
    }

    async fn snapshot_account_members(&mut self) -> Result<Vec<AccountMemberEntry>, ControlError> {
        let rows = sqlx::query(
            "SELECT am.account_id, u.username, u.is_admin AS is_admin, am.role, am.created_at \
             FROM account_members am JOIN users u ON u.id = am.uid \
             ORDER BY am.account_id, u.username",
        )
        .fetch_all(&mut *self.conn)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            if r.get::<bool, _>("is_admin") {
                return Err(ControlError::Invalid(format!(
                    "account_members {}/{} points at a global admin (not exportable)",
                    r.get::<String, _>("account_id"),
                    r.get::<String, _>("username")
                )));
            }
            out.push(AccountMemberEntry {
                account_id: r.get("account_id"),
                username: r.get("username"),
                role: r.get("role"),
                created_at: r.get("created_at"),
            });
        }
        Ok(out)
    }

    async fn snapshot_account_usage_ledger(
        &mut self,
    ) -> Result<Vec<AccountUsageLedgerEntry>, ControlError> {
        let rows = sqlx::query(
            "SELECT account_id, used_bytes, object_count FROM account_usage_ledger \
             ORDER BY account_id",
        )
        .fetch_all(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| AccountUsageLedgerEntry {
                account_id: r.get("account_id"),
                used_bytes: r.get("used_bytes"),
                object_count: r.get("object_count"),
            })
            .collect())
    }

    async fn replace_accounts(
        &mut self,
        accounts: &[AccountEntry],
        members: &[AccountMemberEntry],
        canonical: bool,
    ) -> Result<(), ControlError> {
        sqlx::query("DELETE FROM account_members")
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        sqlx::query("DELETE FROM accounts")
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
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
            install_account(self, a, admin_uid).await?;
        }
        for m in members {
            if !matches!(m.role.as_str(), "owner" | "admin" | "member") {
                return Err(ControlError::Invalid(format!(
                    "account {} member {} role {:?} invalid",
                    m.account_id, m.username, m.role
                )));
            }
            let uid = non_admin_uid(self, &m.username, "account member").await?;
            sqlx::query(
                "INSERT INTO account_members (account_id, uid, role, created_at) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind(&m.account_id)
            .bind(uid)
            .bind(&m.role)
            .bind(m.created_at)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        }
        if canonical {
            sqlx::query(
                "INSERT INTO schema_meta (key, value) VALUES (?, 'done') \
                 ON CONFLICT(key) DO UPDATE SET value = 'done'",
            )
            .bind(crate::db::META_ACCOUNTS_CANONICAL)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        }
        Ok(())
    }
}
