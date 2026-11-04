//! repl_outbox 的 SQLite 事务内写实现（与迁移前 replication.rs/db.rs/repair.rs 一致）。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::repl_outbox::{NewReplOutbox, ReplOutboxRow, ReplOutboxTxn};

use super::SqliteControlTxn;
use super::repl_outbox::repl_row;

fn conn<'a>(txn: &'a mut SqliteControlTxn<'_>) -> &'a mut sqlx::SqliteConnection {
    &mut *txn.conn
}

#[async_trait::async_trait]
impl ReplOutboxTxn for SqliteControlTxn<'_> {
    async fn repl_enqueue(&mut self, new: &NewReplOutbox) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, \
             dest_bucket, is_delete_marker, is_version_delete, target_kind, state, attempts, \
             next_retry_at, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, ?)",
        )
        .bind(&new.bucket)
        .bind(&new.key)
        .bind(&new.version_id)
        .bind(&new.etag)
        .bind(new.size)
        .bind(&new.remote_id)
        .bind(&new.dest_bucket)
        .bind(i64::from(new.is_delete_marker))
        .bind(i64::from(new.is_version_delete))
        .bind(&new.target_kind)
        .bind(new.created_at)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(())
    }

    async fn repl_reclaim_expired(
        &mut self,
        now: i64,
        lease_secs: i64,
    ) -> Result<u64, ControlError> {
        Ok(sqlx::query(
            "UPDATE repl_outbox SET state='pending', claimed_at=NULL, next_retry_at=? \
             WHERE state='sending' AND claimed_at < ?",
        )
        .bind(now)
        .bind(now - lease_secs)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected())
    }

    async fn repl_claim(
        &mut self,
        now: i64,
        limit: i64,
    ) -> Result<Vec<ReplOutboxRow>, ControlError> {
        let rows = sqlx::query(
            "UPDATE repl_outbox SET state='sending', claimed_at=? \
             WHERE state='pending' AND seq IN (SELECT seq FROM repl_outbox WHERE state='pending' \
                           AND next_retry_at <= ? ORDER BY seq LIMIT ?) \
             RETURNING seq, bucket, key, version_id, etag, remote_id, dest_bucket, \
             is_delete_marker, is_version_delete, target_kind, attempts, claimed_at",
        )
        .bind(now)
        .bind(now)
        .bind(limit)
        .fetch_all(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(rows.iter().map(repl_row).collect())
    }

    async fn repl_renew(
        &mut self,
        seq: i64,
        old: Option<i64>,
        now: i64,
    ) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "UPDATE repl_outbox SET claimed_at=? WHERE seq=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(now)
        .bind(seq)
        .bind(old)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }

    async fn repl_finish_ok(&mut self, seq: i64, token: Option<i64>) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "DELETE FROM repl_outbox WHERE seq=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(seq)
        .bind(token)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }

    async fn repl_retry(
        &mut self,
        seq: i64,
        token: Option<i64>,
        attempts: i64,
        retry_at: i64,
        last_error: &str,
    ) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "UPDATE repl_outbox SET state='pending', attempts=?, claimed_at=NULL, \
             next_retry_at=?, last_error=? WHERE seq=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(retry_at)
        .bind(last_error)
        .bind(seq)
        .bind(token)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }

    async fn repl_bury(
        &mut self,
        seq: i64,
        token: Option<i64>,
        attempts: i64,
        last_error: &str,
    ) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "UPDATE repl_outbox SET state='dead', attempts=?, claimed_at=NULL, last_error=? \
             WHERE seq=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(last_error)
        .bind(seq)
        .bind(token)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected()
            == 1)
    }

    async fn repl_drop_site(
        &mut self,
        peer_id: &str,
    ) -> Result<Vec<(String, String, String)>, ControlError> {
        let rows = sqlx::query(
            "DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ? \
             AND version_id <> '' RETURNING bucket, key, version_id",
        )
        .bind(peer_id)
        .fetch_all(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        let versions = rows
            .iter()
            .map(|r| (r.get("bucket"), r.get("key"), r.get("version_id")))
            .collect();
        sqlx::query(
            "DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ? \
             AND version_id = ''",
        )
        .bind(peer_id)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(versions)
    }

    async fn repl_delete_site_all(&mut self, peer_id: &str) -> Result<u64, ControlError> {
        Ok(
            sqlx::query("DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ?")
                .bind(peer_id)
                .execute(&mut *conn(self))
                .await
                .map_err(super::backend)?
                .rows_affected(),
        )
    }

    async fn repl_site_total(&mut self, peer_id: &str) -> Result<i64, ControlError> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ?",
        )
        .bind(peer_id)
        .fetch_one(&mut *conn(self))
        .await
        .map_err(super::backend)
    }
}
