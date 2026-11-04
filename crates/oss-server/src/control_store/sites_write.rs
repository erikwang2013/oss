//! `sites` / `site_cursors` / `site_config` 的 SQLite 事务内写实现。
//!
//! SQL 与迁移前 `db.rs` 逐字一致；`sites` 无 UPDATE 触发器，但仍保持
//! UPDATE-then-INSERT（不引入 UPSERT）。

use xl_store::control_store::ControlError;
use xl_store::control_store::sites::{SitePeerRow, SiteTxn};

use super::SqliteControlTxn;

fn conn<'a>(txn: &'a mut SqliteControlTxn<'_>) -> &'a mut sqlx::SqliteConnection {
    &mut *txn.conn
}

#[async_trait::async_trait]
impl SiteTxn for SqliteControlTxn<'_> {
    async fn site_config_trim(&mut self) -> Result<u64, ControlError> {
        Ok(sqlx::query(
            "DELETE FROM site_config WHERE rowid <> (SELECT MIN(rowid) FROM site_config)",
        )
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected())
    }

    async fn site_config_id(&mut self) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT id FROM site_config LIMIT 1")
            .fetch_optional(&mut *conn(self))
            .await
            .map_err(super::backend)
    }

    async fn site_config_seed(
        &mut self,
        id: &str,
        token: &str,
        created_at: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO site_config (id, name, region, priority, site_token, created_at) \
             SELECT ?, 'site', 'us-east-1', 0, ?, ? \
             WHERE NOT EXISTS (SELECT 1 FROM site_config)",
        )
        .bind(id)
        .bind(token)
        .bind(created_at)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(())
    }

    async fn site_config_apply(
        &mut self,
        name: Option<&str>,
        region: Option<&str>,
        priority: Option<i64>,
        token: Option<&str>,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "UPDATE site_config SET name = COALESCE(?, name), region = COALESCE(?, region), \
             priority = COALESCE(?, priority), site_token = COALESCE(?, site_token)",
        )
        .bind(name)
        .bind(region)
        .bind(priority)
        .bind(token)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(())
    }

    async fn site_pair_upsert(&mut self, p: &SitePeerRow) -> Result<(), ControlError> {
        // UPDATE 不触碰 `reconcile_required`——该门只能由 repair 成功清除。
        let updated = sqlx::query(
            "UPDATE sites SET remote_site_id = ?, name = ?, endpoint = ?, region = ?, priority = ?, \
             s3_access_key = ?, s3_secret_key = ?, peer_site_token = ?, raft_node_id = ? \
             WHERE peer_id = ? OR remote_site_id = ?",
        )
        .bind(&p.remote_site_id)
        .bind(&p.name)
        .bind(&p.endpoint)
        .bind(&p.region)
        .bind(p.priority)
        .bind(&p.s3_access_key)
        .bind(&p.s3_secret_key)
        .bind(&p.peer_site_token)
        .bind(p.raft_node_id.map(|v| v as i64))
        .bind(&p.peer_id)
        .bind(&p.remote_site_id)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?
        .rows_affected();
        if updated == 0 {
            sqlx::query(
                "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, region, priority, \
                 s3_access_key, s3_secret_key, peer_site_token, created_at, raft_node_id, \
                 reconcile_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&p.peer_id)
            .bind(&p.remote_site_id)
            .bind(&p.name)
            .bind(&p.endpoint)
            .bind(&p.region)
            .bind(p.priority)
            .bind(&p.s3_access_key)
            .bind(&p.s3_secret_key)
            .bind(&p.peer_site_token)
            .bind(p.created_at)
            .bind(p.raft_node_id.map(|v| v as i64))
            .bind(if p.reconcile_required { 1i64 } else { 0 })
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?;
        }
        Ok(())
    }

    async fn site_insert(&mut self, p: &SitePeerRow, now: i64) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, region, priority, \
             s3_access_key, s3_secret_key, peer_site_token, created_at, raft_node_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&p.peer_id)
        .bind(&p.remote_site_id)
        .bind(&p.name)
        .bind(&p.endpoint)
        .bind(&p.region)
        .bind(p.priority)
        .bind(&p.s3_access_key)
        .bind(&p.s3_secret_key)
        .bind(&p.peer_site_token)
        .bind(p.created_at)
        .bind(p.raft_node_id.map(|v| v as i64))
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        // 出货游标从 0 起（首轮全量送达含配对快照）
        sqlx::query(
            "INSERT OR IGNORE INTO site_cursors (peer_id, last_seq, updated_at) VALUES (?, 0, ?)",
        )
        .bind(&p.peer_id)
        .bind(now)
        .execute(&mut *conn(self))
        .await
        .map_err(super::backend)?;
        Ok(())
    }

    async fn site_remote_id(&mut self, peer_id: &str) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT remote_site_id FROM sites WHERE peer_id = ?")
            .bind(peer_id)
            .fetch_optional(&mut *conn(self))
            .await
            .map_err(super::backend)
    }

    async fn site_set_reconcile_required(
        &mut self,
        peer_id: &str,
        required: bool,
    ) -> Result<(), ControlError> {
        sqlx::query("UPDATE sites SET reconcile_required = ? WHERE peer_id = ?")
            .bind(if required { 1i64 } else { 0 })
            .bind(peer_id)
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?;
        Ok(())
    }

    async fn site_update_ping(
        &mut self,
        peer_id: &str,
        ts: i64,
        error: Option<&str>,
    ) -> Result<(), ControlError> {
        sqlx::query("UPDATE sites SET last_seen = ?, last_error = ? WHERE peer_id = ?")
            .bind(ts)
            .bind(error)
            .bind(peer_id)
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?;
        Ok(())
    }

    async fn site_cursor_delete(&mut self, peer_id: &str) -> Result<u64, ControlError> {
        Ok(sqlx::query("DELETE FROM site_cursors WHERE peer_id = ?")
            .bind(peer_id)
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?
            .rows_affected())
    }

    async fn site_delete(&mut self, peer_id: &str) -> Result<u64, ControlError> {
        Ok(sqlx::query("DELETE FROM sites WHERE peer_id = ?")
            .bind(peer_id)
            .execute(&mut *conn(self))
            .await
            .map_err(super::backend)?
            .rows_affected())
    }
}
