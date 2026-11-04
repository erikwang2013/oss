//! 用户 / 访问密钥 / 权限的快照导出与安装（调用方事务内）。
//!
//! 严格导出只含可复制的非管理员行；安装行序/列清单与旧 `raft/data.rs` 逐字一致，
//! 本地 uid 由调用方按 username 解析后传入。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{
    AccessKeyEntry, IamSnapshotTxn, PermissionEntry, UserEntry,
};

use super::{SqliteControlTxn, backend};

#[async_trait::async_trait]
impl IamSnapshotTxn for SqliteControlTxn<'_> {
    async fn snapshot_users(&mut self) -> Result<Vec<UserEntry>, ControlError> {
        let rows = sqlx::query(
            "SELECT username, password_hash, status, created_at FROM users \
             WHERE is_admin = 0 ORDER BY username",
        )
        .fetch_all(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| UserEntry {
                username: r.get("username"),
                password_hash: r.get("password_hash"),
                status: r.get("status"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    async fn snapshot_access_keys(&mut self) -> Result<Vec<AccessKeyEntry>, ControlError> {
        let rows = sqlx::query(
            "SELECT a.access_key, a.secret_key, u.username, a.status, a.expires_at, \
                    a.created_at, a.parent_ak, a.session_policy, a.kind \
             FROM access_keys a JOIN users u ON u.id = a.user_id \
             WHERE u.is_admin = 0 AND a.kind = 'long-term' AND a.parent_ak IS NULL \
             ORDER BY a.access_key",
        )
        .fetch_all(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| AccessKeyEntry {
                access_key: r.get("access_key"),
                secret_key: r.get("secret_key"),
                username: r.get("username"),
                status: r.get("status"),
                expires_at: r.get("expires_at"),
                created_at: r.get("created_at"),
                parent_ak: r.get("parent_ak"),
                session_policy: r.get("session_policy"),
                kind: r.get("kind"),
            })
            .collect())
    }

    async fn snapshot_permissions(&mut self) -> Result<Vec<PermissionEntry>, ControlError> {
        let rows = sqlx::query(
            "SELECT u.username, p.bucket, p.can_read, p.can_write, p.can_delete \
             FROM permissions p JOIN users u ON u.id = p.user_id \
             WHERE u.is_admin = 0 ORDER BY u.username, p.bucket",
        )
        .fetch_all(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| PermissionEntry {
                username: r.get("username"),
                bucket: r.get("bucket"),
                can_read: r.get::<i64, _>("can_read") != 0,
                can_write: r.get::<i64, _>("can_write") != 0,
                can_delete: r.get::<i64, _>("can_delete") != 0,
            })
            .collect())
    }

    async fn users_delete_non_admin(&mut self) -> Result<(), ControlError> {
        sqlx::query("DELETE FROM users WHERE is_admin = 0")
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn user_install(&mut self, row: &UserEntry) -> Result<i64, ControlError> {
        let done = sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
             VALUES (?, ?, 0, ?, ?)",
        )
        .bind(&row.username)
        .bind(&row.password_hash)
        .bind(&row.status)
        .bind(row.created_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(done.last_insert_rowid())
    }

    async fn access_keys_delete_non_admin(&mut self) -> Result<(), ControlError> {
        sqlx::query(
            "DELETE FROM access_keys \
             WHERE user_id NOT IN (SELECT id FROM users WHERE is_admin = 1)",
        )
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn access_key_install(
        &mut self,
        row: &AccessKeyEntry,
        uid: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO access_keys \
             (access_key, secret_key, user_id, status, expires_at, created_at, \
              parent_ak, session_policy, kind) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.access_key)
        .bind(&row.secret_key)
        .bind(uid)
        .bind(&row.status)
        .bind(row.expires_at)
        .bind(row.created_at)
        .bind(&row.parent_ak)
        .bind(&row.session_policy)
        .bind(&row.kind)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn permissions_clear(&mut self) -> Result<(), ControlError> {
        sqlx::query("DELETE FROM permissions")
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn permission_install(
        &mut self,
        row: &PermissionEntry,
        uid: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(uid)
        .bind(&row.bucket)
        .bind(row.can_read)
        .bind(row.can_write)
        .bind(row.can_delete)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn permissions_delete_admins(&mut self) -> Result<(), ControlError> {
        sqlx::query(
            "DELETE FROM permissions WHERE user_id IN (SELECT id FROM users WHERE is_admin = 1)",
        )
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn access_keys_delete_orphan_sessions(&mut self) -> Result<(), ControlError> {
        sqlx::query(
            "DELETE FROM access_keys \
             WHERE (kind = 'session' OR parent_ak IS NOT NULL) \
               AND user_id NOT IN (SELECT id FROM users)",
        )
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }
}
