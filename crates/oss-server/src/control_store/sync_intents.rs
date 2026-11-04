//! sync_write_intents / sync_delete_intents 的 SQLite 只读实现。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::sync_intents::{
    SyncDeleteIntentRow, SyncIntentStore, SyncWriteIntentRow,
};

use super::SqliteControlStore;

#[async_trait::async_trait]
impl SyncIntentStore for SqliteControlStore {
    async fn sync_write_intents_stale(
        &self,
        cutoff: i64,
    ) -> Result<Vec<SyncWriteIntentRow>, ControlError> {
        let rows =
            sqlx::query("SELECT id, bucket, key FROM sync_write_intents WHERE updated_at < ?")
                .bind(cutoff)
                .fetch_all(&self.pool)
                .await
                .map_err(super::backend)?;
        Ok(rows
            .iter()
            .map(|r| SyncWriteIntentRow {
                id: r.get("id"),
                bucket: r.get("bucket"),
                key: r.get("key"),
            })
            .collect())
    }

    async fn sync_write_intent_tee(&self, id: &str) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT tee_path FROM sync_write_intents WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(super::backend)
    }

    async fn sync_write_intent_tees(&self) -> Result<Vec<String>, ControlError> {
        sqlx::query_scalar("SELECT tee_path FROM sync_write_intents")
            .fetch_all(&self.pool)
            .await
            .map_err(super::backend)
    }

    async fn sync_delete_intents_stale(
        &self,
        cutoff: i64,
        limit: i64,
    ) -> Result<Vec<SyncDeleteIntentRow>, ControlError> {
        let rows = sqlx::query(
            "SELECT id, bucket, key, op, local_version_id, remote_version_id, marker_version_id, state \
             FROM sync_delete_intents WHERE updated_at < ? ORDER BY updated_at LIMIT ?",
        )
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(super::backend)?;
        Ok(rows
            .iter()
            .map(|r| SyncDeleteIntentRow {
                id: r.get("id"),
                bucket: r.get("bucket"),
                key: r.get("key"),
                op: r.get("op"),
                local_version_id: r.get("local_version_id"),
                remote_version_id: r.get("remote_version_id"),
                marker_version_id: r.get("marker_version_id"),
                state: r.get("state"),
            })
            .collect())
    }
}
