//! uploads 的 SQLite 事务内写实现（与迁移前 db.rs 逐字一致）。
//!
//! 删除用户/站点 apply 的归属清理仍留在原路径（IAM/site 属主），不在本切片。

use xl_store::control_store::ControlError;
use xl_store::control_store::uploads::{NewUpload, UploadTxn};

use super::SqliteControlTxn;

fn conn<'a>(txn: &'a mut SqliteControlTxn<'_>) -> &'a mut sqlx::SqliteConnection {
    &mut *txn.conn
}

#[async_trait::async_trait]
impl UploadTxn for SqliteControlTxn<'_> {
    async fn upload_record(&mut self, new: &NewUpload) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(saved_path) DO NOTHING",
        )
        .bind(&new.saved_path)
        .bind(&new.bucket)
        .bind(&new.key)
        .bind(&new.group_name)
        .bind(new.size)
        .bind(&new.etag)
        .bind(new.created_at)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(())
    }

    async fn upload_register(
        &mut self,
        saved_path: &str,
        original_name: &str,
        owner_id: i64,
        is_admin: bool,
    ) -> Result<bool, ControlError> {
        let n = if is_admin {
            sqlx::query("UPDATE uploads SET original_name = ?, owner_id = ? WHERE saved_path = ?")
                .bind(original_name)
                .bind(owner_id)
                .bind(saved_path)
                .execute(&mut *conn(self))
                .await
                .map_err(super::backend)?
                .rows_affected()
        } else {
            sqlx::query(
                "UPDATE uploads SET original_name = ?, owner_id = ? \
                 WHERE saved_path = ? AND (owner_id IS NULL OR owner_id = ?)",
            )
            .bind(original_name)
            .bind(owner_id)
            .bind(saved_path)
            .bind(owner_id)
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?
            .rows_affected()
        };
        Ok(n == 1)
    }

    async fn upload_claim(
        &mut self,
        saved_path: &str,
        owner_id: i64,
    ) -> Result<bool, ControlError> {
        Ok(
            sqlx::query(
                "UPDATE uploads SET owner_id = ? WHERE saved_path = ? AND owner_id IS NULL",
            )
            .bind(owner_id)
            .bind(saved_path)
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?
            .rows_affected()
                == 1,
        )
    }
}
