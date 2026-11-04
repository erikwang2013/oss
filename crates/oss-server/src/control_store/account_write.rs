//! 账户 / 成员 / 账本的调用方事务内写实现，以及桶数配额判定。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::account::{AccountEntry, AccountTxn};

use super::account::{account_bucket_count_sql, account_buckets_from};
use super::{SqliteControlTxn, backend};

#[async_trait::async_trait]
impl AccountTxn for SqliteControlTxn<'_> {
    async fn account_status(&mut self, id: &str) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT status FROM accounts WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)
    }

    async fn account_hard_limits(
        &mut self,
        id: &str,
    ) -> Result<Option<(Option<i64>, Option<i64>)>, ControlError> {
        sqlx::query_as("SELECT hard_byte_quota, object_quota FROM accounts WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)
    }

    async fn account_insert_if_absent(
        &mut self,
        id: &str,
        name: &str,
        bucket_quota: Option<i64>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query(
            "INSERT OR IGNORE INTO accounts \
             (id, name, status, bucket_quota, created_at, updated_at) \
             VALUES (?, ?, 'active', ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(bucket_quota)
        .bind(created_at)
        .bind(updated_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n == 1)
    }

    async fn account_system_insert_if_absent(
        &mut self,
        id: &str,
        name: &str,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query(
            "INSERT OR IGNORE INTO accounts (id, name, status, created_at, updated_at) \
             VALUES (?, ?, 'active', ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(created_at)
        .bind(updated_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n == 1)
    }

    async fn account_insert_derived_if_absent(
        &mut self,
        id: &str,
        name: &str,
        admin_uid: i64,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query(
            "INSERT OR IGNORE INTO accounts \
             (id, name, status, admin_uid, created_at, updated_at) \
             VALUES (?, ?, 'active', ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(admin_uid)
        .bind(created_at)
        .bind(updated_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n == 1)
    }

    async fn account_set_name(
        &mut self,
        id: &str,
        name: &str,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query("UPDATE accounts SET name = ?, updated_at = ? WHERE id = ?")
            .bind(name)
            .bind(updated_at)
            .bind(id)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n == 1)
    }

    async fn account_set_status(
        &mut self,
        id: &str,
        status: &str,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let n = if status == "active" {
            sqlx::query("UPDATE accounts SET status = 'active', updated_at = ? WHERE id = ?")
                .bind(updated_at)
                .bind(id)
                .execute(&mut *self.conn)
                .await
                .map_err(backend)?
                .rows_affected()
        } else {
            sqlx::query(
                "UPDATE accounts SET status = 'disabled', updated_at = ? WHERE id = ? AND NOT EXISTS (\
                   SELECT 1 FROM account_members am JOIN users u ON u.id = am.uid \
                   WHERE am.account_id = accounts.id AND u.is_admin = 1 AND u.status = 'active')",
            )
            .bind(updated_at)
            .bind(id)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected()
        };
        Ok(n == 1)
    }

    async fn account_set_bucket_quota(
        &mut self,
        id: &str,
        quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query("UPDATE accounts SET bucket_quota = ?, updated_at = ? WHERE id = ?")
            .bind(quota)
            .bind(updated_at)
            .bind(id)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n == 1)
    }

    async fn account_set_soft_quota(
        &mut self,
        id: &str,
        quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query("UPDATE accounts SET soft_byte_quota = ?, updated_at = ? WHERE id = ?")
            .bind(quota)
            .bind(updated_at)
            .bind(id)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n == 1)
    }

    async fn account_set_hard_limits(
        &mut self,
        id: &str,
        hard_byte_quota: Option<i64>,
        object_quota: Option<i64>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query(
            "UPDATE accounts SET hard_byte_quota = ?, object_quota = ?, updated_at = ? \
             WHERE id = ?",
        )
        .bind(hard_byte_quota)
        .bind(object_quota)
        .bind(updated_at)
        .bind(id)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n == 1)
    }

    async fn account_delete(&mut self, id: &str) -> Result<bool, ControlError> {
        let n = sqlx::query("DELETE FROM accounts WHERE id = ?")
            .bind(id)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n == 1)
    }

    async fn account_member_count(&mut self, id: &str) -> Result<i64, ControlError> {
        sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = ?")
            .bind(id)
            .fetch_one(&mut *self.conn)
            .await
            .map_err(backend)
    }

    async fn account_bucket_count(&mut self, id: &str) -> Result<i64, ControlError> {
        let sql = format!(
            "SELECT {} FROM accounts a WHERE a.id = ?",
            account_bucket_count_sql()
        );
        let n: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(n.unwrap_or(0))
    }

    async fn member_uid(
        &mut self,
        account_id: &str,
        username: &str,
    ) -> Result<Option<i64>, ControlError> {
        sqlx::query_scalar(
            "SELECT u.id FROM account_members am JOIN users u ON u.id = am.uid \
             WHERE am.account_id = ? AND u.username = ? AND u.is_admin = 0",
        )
        .bind(account_id)
        .bind(username)
        .fetch_optional(&mut *self.conn)
        .await
        .map_err(backend)
    }

    async fn account_member_upsert(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO account_members (account_id, uid, role, created_at) VALUES (?, ?, ?, ?) \
             ON CONFLICT(uid) DO UPDATE SET account_id = excluded.account_id, role = excluded.role",
        )
        .bind(account_id)
        .bind(uid)
        .bind(role)
        .bind(created_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn account_member_insert(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO account_members (account_id, uid, role, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(uid)
        .bind(role)
        .bind(created_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn account_member_insert_if_absent(
        &mut self,
        account_id: &str,
        uid: i64,
        role: &str,
        created_at: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query(
            "INSERT OR IGNORE INTO account_members (account_id, uid, role, created_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(uid)
        .bind(role)
        .bind(created_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n == 1)
    }

    async fn account_set_admin_if_unset(
        &mut self,
        account_id: &str,
        uid: i64,
    ) -> Result<(), ControlError> {
        sqlx::query("UPDATE accounts SET admin_uid = ? WHERE id = ? AND admin_uid IS NULL")
            .bind(uid)
            .bind(account_id)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn account_members_delete_admins(&mut self) -> Result<(), ControlError> {
        sqlx::query(
            "DELETE FROM account_members WHERE uid IN (SELECT id FROM users WHERE is_admin = 1)",
        )
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn accounts_delete_admin_derived(&mut self) -> Result<(), ControlError> {
        sqlx::query(
            "DELETE FROM accounts \
             WHERE id IN (SELECT printf('%012d', id) FROM users WHERE is_admin = 1) \
               AND id NOT IN (SELECT account_id FROM account_members)",
        )
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn bucket_quota_reached(&mut self, owner_id: Option<i64>) -> Result<bool, ControlError> {
        let Some(uid) = owner_id else {
            return Ok(false);
        };
        let account: Option<String> =
            sqlx::query_scalar("SELECT account_id FROM account_members WHERE uid = ?")
                .bind(uid)
                .fetch_optional(&mut *self.conn)
                .await
                .map_err(backend)?;
        let Some(account) = account.or_else(|| xl_s3::access::account_id_of(uid)) else {
            return Ok(false);
        };
        let Some(r) = sqlx::query("SELECT status, bucket_quota FROM accounts WHERE id = ?")
            .bind(&account)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)?
        else {
            return Ok(false);
        };
        if r.get::<String, _>("status") != "active" {
            return Ok(true);
        }
        let Some(quota) = r.get::<Option<i64>, _>("bucket_quota").filter(|q| *q >= 0) else {
            return Ok(false);
        };
        let sql = format!("SELECT COUNT(*) {}", account_buckets_from("?"));
        let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(&account)
            .fetch_one(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(n >= quota)
    }

    async fn account_ledger(
        &mut self,
        account_id: &str,
    ) -> Result<Option<(i64, i64)>, ControlError> {
        sqlx::query_as(
            "SELECT used_bytes, object_count FROM account_usage_ledger WHERE account_id = ?",
        )
        .bind(account_id)
        .fetch_optional(&mut *self.conn)
        .await
        .map_err(backend)
    }

    async fn account_ledger_upsert(
        &mut self,
        account_id: &str,
        used_bytes: i64,
        object_count: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO account_usage_ledger (account_id, used_bytes, object_count) \
             VALUES (?, ?, ?) ON CONFLICT(account_id) DO UPDATE SET \
               used_bytes = excluded.used_bytes, object_count = excluded.object_count",
        )
        .bind(account_id)
        .bind(used_bytes)
        .bind(object_count)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn account_ledger_clear(&mut self) -> Result<(), ControlError> {
        sqlx::query("DELETE FROM account_usage_ledger")
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(())
    }
}

/// 供快照安装复用：账户行列的写入形状（`admin_uid` 已解析）。
pub(super) async fn install_account(
    txn: &mut SqliteControlTxn<'_>,
    row: &AccountEntry,
    admin_uid: Option<i64>,
) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO accounts \
         (id, name, status, admin_uid, bucket_quota, soft_byte_quota, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&row.id)
    .bind(&row.name)
    .bind(&row.status)
    .bind(admin_uid)
    .bind(row.bucket_quota)
    .bind(row.soft_byte_quota)
    .bind(row.created_at)
    .bind(row.updated_at)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}
