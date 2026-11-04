//! event_outbox 的 SQLite 事务内写实现（与迁移前 `events.rs`/`console.rs` 逐字一致）。
//!
//! 事务由调用方持有；本模块不 begin/commit/rollback。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::event_outbox::{EventOutboxClaim, EventOutboxTxn};

use super::SqliteControlTxn;

fn conn<'a>(txn: &'a mut SqliteControlTxn<'_>) -> &'a mut sqlx::SqliteConnection {
    &mut *txn.conn
}

#[async_trait::async_trait]
impl EventOutboxTxn for SqliteControlTxn<'_> {
    async fn event_enqueue(
        &mut self,
        bucket: &str,
        target_id: &str,
        event_json: &str,
        now: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO event_outbox (bucket, target_id, event_json, state, attempts, \
             next_retry_at, created_at) VALUES (?, ?, ?, 'pending', 0, 0, ?)",
        )
        .bind(bucket)
        .bind(target_id)
        .bind(event_json)
        .bind(now)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(())
    }

    async fn event_reclaim_expired(
        &mut self,
        now: i64,
        lease_secs: i64,
    ) -> Result<u64, ControlError> {
        Ok(sqlx::query(
            "UPDATE event_outbox SET state='pending', claimed_at=NULL, next_retry_at=? \
             WHERE state='sending' AND claimed_at < ?",
        )
        .bind(now)
        .bind(now - lease_secs)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected())
    }

    async fn event_claim(
        &mut self,
        now: i64,
        limit: i64,
    ) -> Result<Vec<EventOutboxClaim>, ControlError> {
        let rows = sqlx::query(
            "UPDATE event_outbox SET state='sending', claimed_at=? \
             WHERE id IN (SELECT id FROM event_outbox WHERE state='pending' AND next_retry_at <= ? \
                          ORDER BY id LIMIT ?) \
             RETURNING id, target_id, event_json, attempts, claimed_at",
        )
        .bind(now)
        .bind(now)
        .bind(limit)
        .fetch_all(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(rows
            .iter()
            .map(|r| EventOutboxClaim {
                id: r.get("id"),
                target_id: r.get("target_id"),
                event_json: r.get("event_json"),
                attempts: r.get("attempts"),
                claimed_at: r.get("claimed_at"),
            })
            .collect())
    }

    async fn event_renew(
        &mut self,
        id: i64,
        old: Option<i64>,
        now: i64,
    ) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "UPDATE event_outbox SET claimed_at=? WHERE id=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(now)
        .bind(id)
        .bind(old)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }

    async fn event_finish_ok(&mut self, id: i64, token: Option<i64>) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "DELETE FROM event_outbox WHERE id=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(id)
        .bind(token)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }

    async fn event_retry(
        &mut self,
        id: i64,
        token: Option<i64>,
        attempts: i64,
        retry_at: i64,
        last_error: &str,
    ) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "UPDATE event_outbox SET state='pending', attempts=?, claimed_at=NULL, \
             next_retry_at=?, last_error=? WHERE id=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(retry_at)
        .bind(last_error)
        .bind(id)
        .bind(token)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }

    async fn event_bury(
        &mut self,
        id: i64,
        token: Option<i64>,
        attempts: i64,
        last_error: &str,
    ) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "UPDATE event_outbox SET state='dead', attempts=?, claimed_at=NULL, last_error=? \
             WHERE id=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(last_error)
        .bind(id)
        .bind(token)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }

    async fn event_redeliver(&mut self, id: i64, now: i64) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "UPDATE event_outbox SET state='pending', claimed_at=NULL, next_retry_at=?, attempts=0, \
             last_error=NULL WHERE id=? AND state='dead'",
        )
        .bind(now)
        .bind(id)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }
}
