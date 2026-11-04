//! 用户 / 访问密钥 / 权限的调用方事务内写实现。
//!
//! SQL 形状逐字复刻旧 `Db::*_raw` 与 site 原语：users/AK/permissions 的
//! INSERT/UPDATE/DELETE 列清单不可改，site 触发器与 raft guard 依赖它们。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{AccessKeyEntry, IamTxn, PermissionRow, UserEntry, UserRef};

use super::{SqliteControlTxn, backend};

#[async_trait::async_trait]
impl IamTxn for SqliteControlTxn<'_> {
    async fn user_by_username(&mut self, username: &str) -> Result<Option<UserRef>, ControlError> {
        let row = sqlx::query("SELECT id, is_admin FROM users WHERE username = ?")
            .bind(username)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(row.map(|r| UserRef {
            uid: r.get("id"),
            is_admin: r.get::<i64, _>("is_admin") != 0,
        }))
    }

    async fn user_uid(&mut self, username: &str) -> Result<Option<i64>, ControlError> {
        sqlx::query_scalar("SELECT id FROM users WHERE username = ?")
            .bind(username)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)
    }

    async fn user_uid_non_admin(&mut self, username: &str) -> Result<Option<i64>, ControlError> {
        sqlx::query_scalar("SELECT id FROM users WHERE username = ? AND is_admin = 0")
            .bind(username)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)
    }

    async fn user_non_admin_exists(&mut self, uid: i64) -> Result<bool, ControlError> {
        let row: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM users WHERE id = ? AND is_admin = 0")
                .bind(uid)
                .fetch_optional(&mut *self.conn)
                .await
                .map_err(backend)?;
        Ok(row.is_some())
    }

    async fn user_is_admin(&mut self, username: &str) -> Result<bool, ControlError> {
        let row: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM users WHERE username = ? AND is_admin = 1")
                .bind(username)
                .fetch_optional(&mut *self.conn)
                .await
                .map_err(backend)?;
        Ok(row.is_some())
    }

    async fn legacy_users_missing_accounts(&mut self) -> Result<Vec<(i64, String)>, ControlError> {
        let rows = sqlx::query(
            "SELECT u.id, u.username FROM users u WHERE u.is_admin = 0 \
             AND NOT EXISTS (SELECT 1 FROM account_members am WHERE am.uid = u.id) \
             ORDER BY u.id",
        )
        .fetch_all(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| (r.get("id"), r.get("username")))
            .collect())
    }

    async fn user_create(&mut self, row: &UserEntry) -> Result<i64, ControlError> {
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

    async fn user_create_default_status(
        &mut self,
        username: &str,
        password_hash: &str,
        is_admin: bool,
        created_at: i64,
    ) -> Result<i64, ControlError> {
        let done = sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(username)
        .bind(password_hash)
        .bind(is_admin as i64)
        .bind(created_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(done.last_insert_rowid())
    }

    async fn user_delete_guard(&mut self, uid: i64) -> Result<bool, ControlError> {
        let n = sqlx::query(
            "UPDATE users SET id = id WHERE id = ? AND \
             (NOT (is_admin = 1 AND status = 'active') \
              OR (SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active') > 1)",
        )
        .bind(uid)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n == 1)
    }

    async fn user_delete_access_keys_permissions(&mut self, uid: i64) -> Result<(), ControlError> {
        for sql in [
            "DELETE FROM access_keys WHERE user_id = ?",
            "DELETE FROM permissions WHERE user_id = ?",
        ] {
            sqlx::query(sql)
                .bind(uid)
                .execute(&mut *self.conn)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn user_delete(&mut self, uid: i64) -> Result<bool, ControlError> {
        let n = sqlx::query("DELETE FROM users WHERE id = ?")
            .bind(uid)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n == 1)
    }

    async fn user_set_password(
        &mut self,
        uid: i64,
        password_hash: &str,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
            .bind(password_hash)
            .bind(uid)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n == 1)
    }

    async fn user_set_status(&mut self, uid: i64, active: bool) -> Result<bool, ControlError> {
        let n = if active {
            sqlx::query("UPDATE users SET status = 'active' WHERE id = ?")
                .bind(uid)
                .execute(&mut *self.conn)
                .await
                .map_err(backend)?
                .rows_affected()
        } else {
            sqlx::query(
                "UPDATE users SET status = 'disabled' WHERE id = ? AND \
                 (NOT (is_admin = 1 AND status = 'active') \
                  OR (SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active') > 1)",
            )
            .bind(uid)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected()
        };
        Ok(n == 1)
    }

    async fn user_set_status_value(
        &mut self,
        uid: i64,
        status: &str,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query("UPDATE users SET status = ? WHERE id = ?")
            .bind(status)
            .bind(uid)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n == 1)
    }

    async fn permissions_replace(
        &mut self,
        uid: i64,
        rows: &[PermissionRow],
    ) -> Result<(), ControlError> {
        sqlx::query("DELETE FROM permissions WHERE user_id = ?")
            .bind(uid)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        for p in rows {
            sqlx::query(
                "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(uid)
            .bind(&p.bucket)
            .bind(p.read as i64)
            .bind(p.write as i64)
            .bind(p.delete as i64)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
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
        // UPSERT 触发的 `site_log_perm_upd` 会把 `site_entity_clock` 的 PK 冲突
        // 误报为 UNIQUE（行已存在即失败）；UPDATE-then-INSERT 产生同样的
        // upsert changelog（行在→upd，行缺→ins）。
        let n = sqlx::query(
            "UPDATE permissions SET can_read = ?, can_write = ?, can_delete = ? \
             WHERE user_id = ? AND bucket = ?",
        )
        .bind(read as i64)
        .bind(write as i64)
        .bind(delete as i64)
        .bind(uid)
        .bind(bucket)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?
        .rows_affected();
        if n == 0 {
            sqlx::query(
                "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(uid)
            .bind(bucket)
            .bind(read as i64)
            .bind(write as i64)
            .bind(delete as i64)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn permission_delete(&mut self, uid: i64, bucket: &str) -> Result<(), ControlError> {
        sqlx::query("DELETE FROM permissions WHERE user_id = ? AND bucket = ?")
            .bind(uid)
            .bind(bucket)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn permissions_delete_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError> {
        sqlx::query("DELETE FROM permissions WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn access_key_exists(&mut self, access_key: &str) -> Result<bool, ControlError> {
        let row: Option<i64> = sqlx::query_scalar("SELECT 1 FROM access_keys WHERE access_key = ?")
            .bind(access_key)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(row.is_some())
    }

    async fn access_key_create(&mut self, row: &AccessKeyEntry) -> Result<(), ControlError> {
        let uid: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE username = ?")
            .bind(&row.username)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)?;
        let Some(uid) = uid else {
            return Err(ControlError::Invalid(format!(
                "access key owner {} not found",
                row.username
            )));
        };
        // apply 原语固定 status='active' 且不写 expires_at（命令形状如此）。
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, kind, \
             parent_ak, session_policy, created_at) VALUES (?, ?, ?, 'active', ?, ?, ?, ?)",
        )
        .bind(&row.access_key)
        .bind(&row.secret_key)
        .bind(uid)
        .bind(&row.kind)
        .bind(&row.parent_ak)
        .bind(&row.session_policy)
        .bind(row.created_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn access_key_create_legacy(
        &mut self,
        access_key: &str,
        secret_key: &str,
        uid: i64,
        created_at: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, created_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(access_key)
        .bind(secret_key)
        .bind(uid)
        .bind(created_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn access_key_delete(&mut self, access_key: &str) -> Result<bool, ControlError> {
        let n = sqlx::query("DELETE FROM access_keys WHERE access_key = ?")
            .bind(access_key)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n > 0)
    }

    async fn access_key_set_status(
        &mut self,
        access_key: &str,
        status: &str,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query("UPDATE access_keys SET status = ? WHERE access_key = ?")
            .bind(status)
            .bind(access_key)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n > 0)
    }

    async fn access_key_owner_is_admin(&mut self, access_key: &str) -> Result<bool, ControlError> {
        let row: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM access_keys a JOIN users u ON u.id = a.user_id \
             WHERE a.access_key = ? AND u.is_admin = 1",
        )
        .bind(access_key)
        .fetch_optional(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(row.is_some())
    }

    async fn access_key_long_term_owner_is_admin(
        &mut self,
        access_key: &str,
    ) -> Result<bool, ControlError> {
        let row = sqlx::query(
            "SELECT u.is_admin FROM access_keys k JOIN users u ON u.id = k.user_id \
             WHERE k.access_key = ? AND k.kind = 'long-term'",
        )
        .bind(access_key)
        .fetch_optional(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(row.is_some_and(|r| r.get::<i64, _>("is_admin") != 0))
    }

    async fn access_key_user_and_kind(
        &mut self,
        access_key: &str,
    ) -> Result<Option<(i64, String)>, ControlError> {
        let row = sqlx::query("SELECT user_id, kind FROM access_keys WHERE access_key = ?")
            .bind(access_key)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(row.map(|r| (r.get("user_id"), r.get("kind"))))
    }

    async fn access_key_delete_long_term(
        &mut self,
        access_key: &str,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query("DELETE FROM access_keys WHERE access_key = ? AND kind = 'long-term'")
            .bind(access_key)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?
            .rows_affected();
        Ok(n > 0)
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
        // 同 `permission_upsert`：UPSERT + site UPDATE 触发器在 SQLite 下误报
        // UNIQUE；UPDATE-then-INSERT 保持相同 changelog（冲突时不改 created_at/kind）。
        let n = sqlx::query(
            "UPDATE access_keys SET secret_key = ?, user_id = ?, status = ?, expires_at = ? \
             WHERE access_key = ?",
        )
        .bind(secret_key)
        .bind(uid)
        .bind(status)
        .bind(expires_at)
        .bind(access_key)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?
        .rows_affected();
        if n == 0 {
            sqlx::query(
                "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, \
                 created_at, kind) VALUES (?, ?, ?, ?, ?, ?, 'long-term')",
            )
            .bind(access_key)
            .bind(secret_key)
            .bind(uid)
            .bind(status)
            .bind(expires_at)
            .bind(created_at)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        }
        Ok(())
    }
}
