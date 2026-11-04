//! sync intent 的 SQLite 事务内写实现（与迁移前 sync.rs / repair.rs 逐字一致）。

use xl_store::control_store::ControlError;
use xl_store::control_store::sync_intents::{
    NewSyncDeleteIntent, NewSyncWriteIntent, SyncIntentTxn,
};

use super::SqliteControlTxn;

fn conn<'a>(txn: &'a mut SqliteControlTxn<'_>) -> &'a mut sqlx::SqliteConnection {
    &mut *txn.conn
}

#[async_trait::async_trait]
impl SyncIntentTxn for SqliteControlTxn<'_> {
    async fn sync_write_intent_insert(
        &mut self,
        new: &NewSyncWriteIntent,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO sync_write_intents \
             (id, bucket, key, local_version_id, remote_version_id, tee_path, commit_json, \
              state, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&new.id)
        .bind(&new.bucket)
        .bind(&new.key)
        .bind(&new.local_version_id)
        .bind(&new.remote_version_id)
        .bind(&new.tee_path)
        .bind(&new.commit_json)
        .bind(&new.state)
        .bind(new.created_at)
        .bind(new.updated_at)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(())
    }

    async fn sync_write_intent_mark_done(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<u64, ControlError> {
        Ok(
            sqlx::query("UPDATE sync_write_intents SET state = ?, updated_at = ? WHERE id = ?")
                .bind(crate::sync::INTENT_DONE)
                .bind(now)
                .bind(id)
                .execute(&mut *conn(self))
                .await
                .map_err(super::backend)?
                .rows_affected(),
        )
    }

    async fn sync_write_intent_delete(&mut self, id: &str) -> Result<u64, ControlError> {
        Ok(sqlx::query("DELETE FROM sync_write_intents WHERE id = ?")
            .bind(id)
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?
            .rows_affected())
    }

    async fn sync_write_intent_count(&mut self) -> Result<i64, ControlError> {
        sqlx::query_scalar("SELECT COUNT(*) FROM sync_write_intents")
            .fetch_one(&mut *conn(self))
            .await
            .map_err(super::backend)
    }

    async fn sync_write_intent_delete_all(&mut self) -> Result<u64, ControlError> {
        Ok(sqlx::query("DELETE FROM sync_write_intents")
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?
            .rows_affected())
    }

    async fn sync_delete_intent_insert(
        &mut self,
        new: &NewSyncDeleteIntent,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO sync_delete_intents \
             (id, bucket, key, op, local_version_id, remote_version_id, marker_version_id, \
              state, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&new.id)
        .bind(&new.bucket)
        .bind(&new.key)
        .bind(&new.op)
        .bind(&new.local_version_id)
        .bind(&new.remote_version_id)
        .bind(&new.marker_version_id)
        .bind(&new.state)
        .bind(new.created_at)
        .bind(new.updated_at)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(())
    }

    async fn sync_delete_intent_mark_done(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<u64, ControlError> {
        Ok(
            sqlx::query("UPDATE sync_delete_intents SET state = ?, updated_at = ? WHERE id = ?")
                .bind(crate::sync::INTENT_DONE)
                .bind(now)
                .bind(id)
                .execute(&mut *conn(self))
                .await
                .map_err(super::backend)?
                .rows_affected(),
        )
    }

    async fn sync_delete_intent_delete(&mut self, id: &str) -> Result<u64, ControlError> {
        Ok(sqlx::query("DELETE FROM sync_delete_intents WHERE id = ?")
            .bind(id)
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?
            .rows_affected())
    }

    async fn sync_delete_intent_count(&mut self) -> Result<i64, ControlError> {
        sqlx::query_scalar("SELECT COUNT(*) FROM sync_delete_intents")
            .fetch_one(&mut *conn(self))
            .await
            .map_err(super::backend)
    }

    async fn sync_delete_intent_delete_all(&mut self) -> Result<u64, ControlError> {
        Ok(sqlx::query("DELETE FROM sync_delete_intents")
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?
            .rows_affected())
    }
}
