//! 账户 / 成员 / 账本计数的只读 SQLite 实现（共享 `Db` 连接池）。
//!
//! 桶数口径与旧 `Db::account_buckets_from` 逐字一致：桶主当前账户（成员行优先，
//! 无行回落 uid 派生账户），管理员桶不计入任何账户。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::account::{AccountMemberView, AccountStore, AccountView};

use super::SqliteControlStore;

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

/// 账户桶集合的 FROM/WHERE 片段；`account` 是账户 id 的 SQL 表达式。
fn account_buckets_from(account: &str) -> String {
    format!(
        "FROM buckets b JOIN users u ON u.id = b.owner_id \
         LEFT JOIN account_members bm ON bm.uid = u.id \
         WHERE u.is_admin = 0 AND COALESCE(bm.account_id, printf('%012d', u.id)) = {account}"
    )
}

fn account_bucket_count_sql() -> String {
    format!("(SELECT COUNT(*) {})", account_buckets_from("a.id"))
}

const ACCOUNT_COLUMNS: &str = "a.id, a.name, a.status, a.admin_uid, a.bucket_quota, \
     a.soft_byte_quota, a.hard_byte_quota, a.object_quota, a.created_at, a.updated_at, \
     (SELECT COUNT(*) FROM account_members m WHERE m.account_id = a.id) AS member_count";

fn account_from(r: &sqlx::sqlite::SqliteRow) -> AccountView {
    AccountView {
        id: r.get("id"),
        name: r.get("name"),
        status: r.get("status"),
        admin_uid: r.get("admin_uid"),
        bucket_quota: r.get("bucket_quota"),
        soft_byte_quota: r.get("soft_byte_quota"),
        hard_byte_quota: r.get("hard_byte_quota"),
        object_quota: r.get("object_quota"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
        member_count: r.get("member_count"),
        bucket_count: r.get("bucket_count"),
    }
}

#[async_trait::async_trait]
impl AccountStore for SqliteControlStore {
    async fn account(&self, id: &str) -> Result<Option<AccountView>, ControlError> {
        let sql = format!(
            "SELECT {ACCOUNT_COLUMNS}, {} AS bucket_count FROM accounts a WHERE a.id = ?",
            account_bucket_count_sql()
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        Ok(row.as_ref().map(account_from))
    }

    async fn accounts(&self, member_uid: Option<i64>) -> Result<Vec<AccountView>, ControlError> {
        let filter = if member_uid.is_some() {
            "WHERE a.id IN (SELECT account_id FROM account_members WHERE uid = ?)"
        } else {
            ""
        };
        let sql = format!(
            "SELECT {ACCOUNT_COLUMNS}, {} AS bucket_count FROM accounts a {filter} ORDER BY a.id",
            account_bucket_count_sql()
        );
        let q = sqlx::query(sqlx::AssertSqlSafe(sql));
        let q = match member_uid {
            Some(uid) => q.bind(uid),
            None => q,
        };
        let rows = q.fetch_all(&self.pool).await.map_err(backend)?;
        Ok(rows.iter().map(account_from).collect())
    }

    async fn account_status(&self, id: &str) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT status FROM accounts WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)
    }

    async fn account_bucket_quota(
        &self,
        id: &str,
    ) -> Result<Option<(String, Option<i64>)>, ControlError> {
        let row = sqlx::query("SELECT status, bucket_quota FROM accounts WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        Ok(row.map(|r| (r.get("status"), r.get("bucket_quota"))))
    }

    async fn account_hard_limits(
        &self,
        id: &str,
    ) -> Result<Option<(Option<i64>, Option<i64>)>, ControlError> {
        let row = sqlx::query("SELECT hard_byte_quota, object_quota FROM accounts WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        Ok(row.map(|r| (r.get("hard_byte_quota"), r.get("object_quota"))))
    }

    async fn account_member_account(&self, uid: i64) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT account_id FROM account_members WHERE uid = ?")
            .bind(uid)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)
    }

    async fn account_active(&self, uid: i64) -> Result<bool, ControlError> {
        let inactive: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM account_members am \
             LEFT JOIN accounts a ON a.id = am.account_id \
             WHERE am.uid = ? AND COALESCE(a.status, 'disabled') <> 'active'",
        )
        .bind(uid)
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        Ok(inactive == 0)
    }

    async fn member_uid(
        &self,
        account_id: &str,
        username: &str,
    ) -> Result<Option<i64>, ControlError> {
        sqlx::query_scalar(
            "SELECT u.id FROM account_members am JOIN users u ON u.id = am.uid \
             WHERE am.account_id = ? AND u.username = ? AND u.is_admin = 0",
        )
        .bind(account_id)
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)
    }

    async fn member_role(
        &self,
        account_id: &str,
        uid: i64,
    ) -> Result<Option<String>, ControlError> {
        let row = sqlx::query("SELECT role FROM account_members WHERE account_id = ? AND uid = ?")
            .bind(account_id)
            .bind(uid)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        Ok(row.map(|r| r.get("role")))
    }

    async fn account_members(
        &self,
        account_id: &str,
    ) -> Result<Vec<AccountMemberView>, ControlError> {
        let rows = sqlx::query(
            "SELECT am.uid AS uid, u.username AS username, am.role AS role, \
                    u.is_admin AS is_admin, u.status AS status, am.created_at AS created_at \
             FROM account_members am JOIN users u ON u.id = am.uid \
             WHERE am.account_id = ? ORDER BY am.uid",
        )
        .bind(account_id)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| AccountMemberView {
                uid: r.get("uid"),
                username: r.get("username"),
                role: r.get("role"),
                is_admin: r.get::<i64, _>("is_admin") != 0,
                status: r.get("status"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    async fn legacy_users_missing_accounts(&self) -> Result<Vec<(i64, String)>, ControlError> {
        let rows = sqlx::query(
            "SELECT u.id, u.username FROM users u WHERE u.is_admin = 0 \
             AND NOT EXISTS (SELECT 1 FROM account_members am WHERE am.uid = u.id) \
             ORDER BY u.id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| (r.get("id"), r.get("username")))
            .collect())
    }

    async fn account_bucket_names(&self, account_id: &str) -> Result<Vec<String>, ControlError> {
        let sql = format!(
            "SELECT b.name {} ORDER BY b.name",
            account_buckets_from("?")
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(account_id)
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        Ok(rows.iter().map(|r| r.get("name")).collect())
    }

    async fn account_counts(&self) -> Result<(i64, i64), ControlError> {
        let row = sqlx::query(
            "SELECT (SELECT COUNT(*) FROM accounts) AS accounts, \
                    (SELECT COUNT(*) FROM account_members) AS members",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        Ok((row.get("accounts"), row.get("members")))
    }

    async fn account_ledger_count(&self) -> Result<i64, ControlError> {
        sqlx::query_scalar("SELECT COUNT(*) FROM account_usage_ledger")
            .fetch_one(&self.pool)
            .await
            .map_err(backend)
    }
}
