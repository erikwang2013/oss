//! 用户 / 访问密钥 / 权限的只读 SQLite 实现（共享 `Db` 连接池）。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{IamStore, PermissionRow, UserLogin, UserView};

use super::SqliteControlStore;

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

fn user_from(r: &sqlx::sqlite::SqliteRow) -> UserView {
    UserView {
        uid: r.get("id"),
        username: r.get("username"),
        is_admin: r.get::<i64, _>("is_admin") != 0,
        status: r.get("status"),
        created_at: r.get("created_at"),
    }
}

#[async_trait::async_trait]
impl IamStore for SqliteControlStore {
    async fn user(&self, username: &str) -> Result<Option<UserView>, ControlError> {
        let row = sqlx::query(
            "SELECT id, username, is_admin, status, created_at FROM users WHERE username = ?",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.as_ref().map(user_from))
    }

    async fn user_by_id(&self, uid: i64) -> Result<Option<UserView>, ControlError> {
        let row = sqlx::query(
            "SELECT id, username, is_admin, status, created_at FROM users WHERE id = ?",
        )
        .bind(uid)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.as_ref().map(user_from))
    }

    async fn users(&self) -> Result<Vec<UserView>, ControlError> {
        let rows = sqlx::query(
            "SELECT id, username, is_admin, status, created_at FROM users ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows.iter().map(user_from).collect())
    }

    async fn user_exists(&self, username: &str) -> Result<bool, ControlError> {
        let row = sqlx::query("SELECT 1 FROM users WHERE username = ?")
            .bind(username)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        Ok(row.is_some())
    }

    async fn login_user(&self, username: &str) -> Result<Option<UserLogin>, ControlError> {
        let row = sqlx::query(
            "SELECT id, password_hash FROM users u WHERE username = ? AND status = 'active' \
             AND NOT EXISTS (SELECT 1 FROM account_members am \
                LEFT JOIN accounts a ON a.id = am.account_id \
                WHERE am.uid = u.id AND COALESCE(a.status, 'disabled') <> 'active')",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.map(|r| UserLogin {
            uid: r.get("id"),
            password_hash: r.get("password_hash"),
        }))
    }

    async fn admin_id(&self) -> Result<Option<i64>, ControlError> {
        sqlx::query_scalar("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)
    }

    async fn admin_counts(&self) -> Result<(i64, i64), ControlError> {
        let active: i64 =
            sqlx::query_scalar("SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active'")
                .fetch_one(&self.pool)
                .await
                .map_err(backend)?;
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE is_admin = 1")
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?;
        Ok((active, total))
    }

    async fn permissions_for_user(&self, uid: i64) -> Result<Vec<PermissionRow>, ControlError> {
        let rows = sqlx::query(
            "SELECT bucket, can_read, can_write, can_delete FROM permissions \
             WHERE user_id = ? ORDER BY bucket",
        )
        .bind(uid)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| PermissionRow {
                bucket: r.get("bucket"),
                read: r.get::<i64, _>("can_read") != 0,
                write: r.get::<i64, _>("can_write") != 0,
                delete: r.get::<i64, _>("can_delete") != 0,
            })
            .collect())
    }

    async fn access_keys_for_user(&self, uid: i64) -> Result<Vec<String>, ControlError> {
        let rows = sqlx::query(
            "SELECT access_key FROM access_keys WHERE user_id = ? AND kind = 'long-term' \
             ORDER BY created_at, access_key",
        )
        .bind(uid)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows.iter().map(|r| r.get("access_key")).collect())
    }
}
