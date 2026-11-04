//! event_outbox 的 SQLite 只读实现（console 死信列表）。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::event_outbox::{DeadEventRow, EventOutboxStore};

use super::SqliteControlStore;

#[async_trait::async_trait]
impl EventOutboxStore for SqliteControlStore {
    async fn dead_events(&self, limit: i64) -> Result<Vec<DeadEventRow>, ControlError> {
        let rows = sqlx::query(
            "SELECT id, bucket, event_json, attempts, last_error, created_at FROM event_outbox \
             WHERE state='dead' ORDER BY id DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(super::backend)?;
        Ok(rows
            .iter()
            .map(|r| DeadEventRow {
                id: r.get("id"),
                bucket: r.get("bucket"),
                event_json: r.get("event_json"),
                attempts: r.get("attempts"),
                last_error: r.get("last_error"),
                created_at: r.get("created_at"),
            })
            .collect())
    }
}
