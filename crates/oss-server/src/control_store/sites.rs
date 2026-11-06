//! `sites` / `site_cursors` / `site_config` 的 SQLite 只读实现。

use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use xl_store::control_store::ControlError;
use xl_store::control_store::sites::{
    SiteChangelogRow, SiteCredentialsRow, SiteEntityClockRow, SiteIdentityRow, SitePeerRow,
    SiteStore,
};

use super::SqliteControlStore;

fn site_row(r: &SqliteRow) -> SitePeerRow {
    SitePeerRow {
        peer_id: r.get("peer_id"),
        remote_site_id: r.get("remote_site_id"),
        name: r.get("name"),
        endpoint: r.get("endpoint"),
        region: r.get("region"),
        priority: r.get("priority"),
        s3_access_key: r.get("s3_access_key"),
        s3_secret_key: r.get("s3_secret_key"),
        peer_site_token: r.get("peer_site_token"),
        created_at: r.get("created_at"),
        last_seen: r.get("last_seen"),
        last_error: r.get("last_error"),
        raft_node_id: r.get::<Option<i64>, _>("raft_node_id").map(|v| v as u64),
        reconcile_required: r.get::<i64, _>("reconcile_required") != 0,
    }
}

#[async_trait::async_trait]
impl SiteStore for SqliteControlStore {
    async fn site_identity(&self) -> Result<Option<SiteIdentityRow>, ControlError> {
        let r =
            sqlx::query("SELECT id, name, region, priority, site_token FROM site_config LIMIT 1")
                .fetch_optional(&self.pool)
                .await
                .map_err(super::backend)?;
        Ok(r.map(|r| SiteIdentityRow {
            id: r.get("id"),
            name: r.get("name"),
            region: r.get("region"),
            priority: r.get("priority"),
            token: r.get("site_token"),
        }))
    }

    async fn site(&self, peer_id: &str) -> Result<Option<SitePeerRow>, ControlError> {
        let r = sqlx::query("SELECT * FROM sites WHERE peer_id = ?")
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(super::backend)?;
        Ok(r.as_ref().map(site_row))
    }

    async fn sites(&self) -> Result<Vec<SitePeerRow>, ControlError> {
        let rows = sqlx::query("SELECT * FROM sites ORDER BY created_at, peer_id")
            .fetch_all(&self.pool)
            .await
            .map_err(super::backend)?;
        Ok(rows.iter().map(site_row).collect())
    }

    async fn site_count(&self) -> Result<i64, ControlError> {
        sqlx::query_scalar("SELECT COUNT(*) FROM sites")
            .fetch_one(&self.pool)
            .await
            .map_err(super::backend)
    }

    async fn site_remote_id(&self, peer_id: &str) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT remote_site_id FROM sites WHERE peer_id = ?")
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(super::backend)
    }

    async fn first_site_remote_id(&self) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT remote_site_id FROM sites LIMIT 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(super::backend)
    }

    async fn site_peer_ids(&self) -> Result<Vec<String>, ControlError> {
        sqlx::query_scalar("SELECT peer_id FROM sites ORDER BY peer_id")
            .fetch_all(&self.pool)
            .await
            .map_err(super::backend)
    }

    async fn site_credentials(
        &self,
        peer_id: &str,
    ) -> Result<Option<SiteCredentialsRow>, ControlError> {
        let r = sqlx::query(
            "SELECT endpoint, s3_access_key, s3_secret_key, region, peer_site_token \
             FROM sites WHERE peer_id = ?",
        )
        .bind(peer_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(super::backend)?;
        Ok(r.map(|r| SiteCredentialsRow {
            endpoint: r.get("endpoint"),
            s3_access_key: r.get("s3_access_key"),
            s3_secret_key: r.get("s3_secret_key"),
            region: r.get("region"),
            peer_site_token: r.get("peer_site_token"),
        }))
    }

    async fn site_cursor(&self, peer_id: &str) -> Result<Option<i64>, ControlError> {
        sqlx::query_scalar("SELECT last_seq FROM site_cursors WHERE peer_id = ?")
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(super::backend)
    }

    async fn site_lag_rows(&self) -> Result<i64, ControlError> {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM sites s LEFT JOIN site_cursors c ON c.peer_id = s.peer_id \
             WHERE IFNULL((SELECT MAX(seq) FROM site_changelog), 0) > IFNULL(c.last_seq, 0)",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(super::backend)
    }

    async fn site_changelog_after(&self, seq: i64) -> Result<Vec<SiteChangelogRow>, ControlError> {
        let rows = sqlx::query(
            "SELECT seq, origin_site_id, entity, entity_id, op, payload, priority, ts, \
             created_at FROM site_changelog WHERE seq > ? ORDER BY seq",
        )
        .bind(seq)
        .fetch_all(&self.pool)
        .await
        .map_err(super::backend)?;
        Ok(rows
            .iter()
            .map(|r| SiteChangelogRow {
                seq: r.get("seq"),
                origin_site_id: r.get("origin_site_id"),
                entity: r.get("entity"),
                entity_id: r.get("entity_id"),
                op: r.get("op"),
                payload: r.get("payload"),
                priority: r.get("priority"),
                ts: r.get("ts"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    async fn site_changelog_max_seq(&self) -> Result<i64, ControlError> {
        sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM site_changelog")
            .fetch_one(&self.pool)
            .await
            .map_err(super::backend)
    }

    async fn site_clock(
        &self,
        entity: &str,
        entity_id: &str,
    ) -> Result<Option<SiteEntityClockRow>, ControlError> {
        let r = sqlx::query(
            "SELECT entity, entity_id, ts, priority, origin_site_id, seq \
             FROM site_entity_clock WHERE entity = ? AND entity_id = ?",
        )
        .bind(entity)
        .bind(entity_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(super::backend)?;
        Ok(r.map(|r| SiteEntityClockRow {
            entity: r.get("entity"),
            entity_id: r.get("entity_id"),
            ts: r.get("ts"),
            priority: r.get("priority"),
            origin_site_id: r.get("origin_site_id"),
            seq: r.get("seq"),
        }))
    }
}
