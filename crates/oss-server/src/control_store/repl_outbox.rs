//! repl_outbox 的 SQLite 只读实现（worker 聚合、console 状态、repair 计数）。

use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use xl_store::control_store::ControlError;
use xl_store::control_store::repl_outbox::{ReplOutboxCounts, ReplOutboxRow, ReplOutboxStore};

use super::SqliteControlStore;

pub(super) fn repl_row(r: &SqliteRow) -> ReplOutboxRow {
    ReplOutboxRow {
        seq: r.get("seq"),
        bucket: r.get("bucket"),
        key: r.get("key"),
        version_id: r.get("version_id"),
        etag: r.get("etag"),
        remote_id: r.get("remote_id"),
        dest_bucket: r.get("dest_bucket"),
        is_delete_marker: r.get::<i64, _>("is_delete_marker") != 0,
        is_version_delete: r.get::<i64, _>("is_version_delete") != 0,
        target_kind: r.get("target_kind"),
        attempts: r.get("attempts"),
        claimed_at: r.get("claimed_at"),
    }
}

#[async_trait::async_trait]
impl ReplOutboxStore for SqliteControlStore {
    async fn repl_version_counts(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ReplOutboxCounts, ControlError> {
        let r = sqlx::query(
            "SELECT COALESCE(SUM(state='pending'),0) AS pending, \
                    COALESCE(SUM(state='sending'),0) AS sending, \
                    COALESCE(SUM(state='dead'),0) AS dead \
             FROM repl_outbox WHERE bucket=? AND key=? AND version_id=?",
        )
        .bind(bucket)
        .bind(key)
        .bind(version_id)
        .fetch_one(&self.pool)
        .await
        .map_err(super::backend)?;
        Ok(ReplOutboxCounts {
            pending: r.get("pending"),
            sending: r.get("sending"),
            dead: r.get("dead"),
        })
    }

    async fn repl_earlier_inflight(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        seq: i64,
    ) -> Result<i64, ControlError> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM repl_outbox WHERE bucket=? AND key=? AND version_id=? \
             AND seq < ? AND state IN ('pending','sending')",
        )
        .bind(bucket)
        .bind(key)
        .bind(version_id)
        .bind(seq)
        .fetch_one(&self.pool)
        .await
        .map_err(super::backend)
    }

    async fn repl_site_state_count(&self, peer_id: &str, state: &str) -> Result<i64, ControlError> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM repl_outbox WHERE target_kind='site' AND remote_id=? \
             AND state=?",
        )
        .bind(peer_id)
        .bind(state)
        .fetch_one(&self.pool)
        .await
        .map_err(super::backend)
    }

    async fn repl_site_total(&self, peer_id: &str) -> Result<i64, ControlError> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ?",
        )
        .bind(peer_id)
        .fetch_one(&self.pool)
        .await
        .map_err(super::backend)
    }
}
