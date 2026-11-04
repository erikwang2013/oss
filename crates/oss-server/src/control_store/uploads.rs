//! uploads 的 SQLite 只读实现。

use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use xl_store::control_store::ControlError;
use xl_store::control_store::uploads::{UploadRow, UploadStore};

use super::SqliteControlStore;

fn upload_row(r: &SqliteRow) -> UploadRow {
    UploadRow {
        saved_path: r.get("saved_path"),
        bucket: r.get("bucket"),
        key: r.get("key"),
        group_name: r.get("group_name"),
        size: r.get("size"),
        etag: r.get("etag"),
        original_name: r.get("original_name"),
        created_at: r.get("created_at"),
    }
}

#[async_trait::async_trait]
impl UploadStore for SqliteControlStore {
    async fn upload(&self, saved_path: &str) -> Result<Option<UploadRow>, ControlError> {
        let row = sqlx::query("SELECT * FROM uploads WHERE saved_path = ?")
            .bind(saved_path)
            .fetch_optional(&self.pool)
            .await
            .map_err(super::backend)?;
        Ok(row.as_ref().map(upload_row))
    }

    async fn uploads(&self, limit: i64) -> Result<Vec<UploadRow>, ControlError> {
        let rows =
            sqlx::query("SELECT * FROM uploads ORDER BY created_at DESC, saved_path LIMIT ?")
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(super::backend)?;
        Ok(rows.iter().map(upload_row).collect())
    }

    async fn uploads_for(&self, owner_id: i64, limit: i64) -> Result<Vec<UploadRow>, ControlError> {
        let rows = sqlx::query(
            "SELECT * FROM uploads WHERE owner_id = ? ORDER BY created_at DESC, saved_path LIMIT ?",
        )
        .bind(owner_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(super::backend)?;
        Ok(rows.iter().map(upload_row).collect())
    }
}
