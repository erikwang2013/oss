//! 控制面的 SQLite 适配器：只读 `SqliteControlStore` 与调用方事务内的写
//! `SqliteControlTxn` 共用 `Db` 的既有连接池，不新建第二个池。
//!
//! 事务由调用方持有；适配器不 begin/commit/rollback。

use sqlx::{Row, SqlitePool};
use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryConfigView, InventoryCursorRow, InventoryIncrementalRow,
    InventoryRunRow, InventoryStore, InventoryTxn,
};
use xl_store::control_store::lifecycle::{
    LifecycleCursorRow, LifecycleRuleEntry, LifecycleStore, LifecycleTxn,
};
use xl_store::control_store::tier::{TierRestoreRow, TierStore, TierTxn};
use xl_store::control_store::{
    AccessPointEntry, ControlError, ControlStore, ControlTxn, OlapConfigEntry, VpcEndpoint,
};

mod ap_olap;
mod inventory;
mod lifecycle;
mod tier;

pub struct SqliteControlStore {
    pub(crate) pool: SqlitePool,
}

impl SqliteControlStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn endpoint_from(r: &sqlx::sqlite::SqliteRow) -> VpcEndpoint {
    VpcEndpoint {
        vpce_id: r.get("vpce_id"),
        vpc_id: r.get("vpc_id"),
        account_id: r.get("account_id"),
        state: r.get("state"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

#[async_trait::async_trait]
impl ControlStore for SqliteControlStore {
    async fn vpc_endpoint(&self, vpce_id: &str) -> Result<Option<VpcEndpoint>, ControlError> {
        let row = sqlx::query(
            "SELECT vpce_id, vpc_id, account_id, state, created_at, updated_at \
             FROM vpc_endpoints WHERE vpce_id = ?",
        )
        .bind(vpce_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
        Ok(row.as_ref().map(endpoint_from))
    }

    async fn vpc_endpoints(&self) -> Result<Vec<VpcEndpoint>, ControlError> {
        let rows = sqlx::query(
            "SELECT vpce_id, vpc_id, account_id, state, created_at, updated_at \
             FROM vpc_endpoints ORDER BY vpce_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
        Ok(rows.iter().map(endpoint_from).collect())
    }

    async fn access_point(&self, name: &str) -> Result<Option<AccessPointEntry>, ControlError> {
        ap_olap::access_point(self, name).await
    }

    async fn access_points(&self) -> Result<Vec<AccessPointEntry>, ControlError> {
        ap_olap::access_points(self).await
    }

    async fn olap_config(
        &self,
        access_point: &str,
    ) -> Result<Option<OlapConfigEntry>, ControlError> {
        ap_olap::olap_config(self, access_point).await
    }

    async fn olap_configs(&self) -> Result<Vec<OlapConfigEntry>, ControlError> {
        ap_olap::olap_configs(self).await
    }
}

#[async_trait::async_trait]
impl InventoryStore for SqliteControlStore {
    async fn inventory_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<InventoryConfigView>, ControlError> {
        inventory::inventory_config(self, bucket, id).await
    }

    async fn inventory_configs(
        &self,
        bucket: &str,
    ) -> Result<Vec<InventoryConfigView>, ControlError> {
        inventory::inventory_configs(self, bucket).await
    }

    async fn inventory_configs_all(&self) -> Result<Vec<InventoryConfigView>, ControlError> {
        inventory::inventory_configs_all(self).await
    }

    async fn inventory_config_count(&self, bucket: &str) -> Result<i64, ControlError> {
        inventory::inventory_config_count(self, bucket).await
    }

    async fn inventory_run(&self, id: i64) -> Result<Option<InventoryRunRow>, ControlError> {
        inventory::inventory_run(self, id).await
    }

    async fn inventory_run_by_key(
        &self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> Result<Option<InventoryRunRow>, ControlError> {
        inventory::inventory_run_by_key(self, bucket, config_id, run_key).await
    }

    async fn inventory_cursor(
        &self,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryCursorRow>, ControlError> {
        inventory::inventory_cursor(self, bucket, config_id).await
    }

    async fn inventory_incremental(
        &self,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryIncrementalRow>, ControlError> {
        inventory::inventory_incremental(self, bucket, config_id).await
    }
}

#[async_trait::async_trait]
impl LifecycleStore for SqliteControlStore {
    async fn lifecycle_rules(&self, bucket: &str) -> Result<Vec<LifecycleRuleEntry>, ControlError> {
        lifecycle::lifecycle_rules(self, bucket).await
    }

    async fn enabled_lifecycle_rules(&self) -> Result<Vec<LifecycleRuleEntry>, ControlError> {
        lifecycle::enabled_lifecycle_rules(self).await
    }

    async fn lifecycle_cursor(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<String>, ControlError> {
        lifecycle::lifecycle_cursor(self, bucket, rule_id, stage).await
    }

    async fn lifecycle_cursor_row(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<LifecycleCursorRow>, ControlError> {
        lifecycle::lifecycle_cursor_row(self, bucket, rule_id, stage).await
    }
}

#[async_trait::async_trait]
impl TierStore for SqliteControlStore {
    async fn tier_restore(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<TierRestoreRow>, ControlError> {
        tier::tier_restore(self, bucket, key, version_id).await
    }

    async fn tier_restores(&self, state: &str) -> Result<Vec<TierRestoreRow>, ControlError> {
        tier::tier_restores(self, state).await
    }
}

/// 调用方事务内的写句柄。借用现有连接，绝不 begin/commit/rollback——事务边界
/// 归调用方（Raft apply / 快照安装），这是与 `raft_applied` 原子性的唯一来源。
pub struct SqliteControlTxn<'a> {
    pub(crate) conn: &'a mut sqlx::SqliteConnection,
}

impl<'a> SqliteControlTxn<'a> {
    pub fn new(conn: &'a mut sqlx::SqliteConnection) -> Self {
        Self { conn }
    }
}

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

/// 缝错误 → sqlx 错误（调用方事务内 fail-stop 用）。
pub(crate) fn to_sqlx(e: ControlError) -> sqlx::Error {
    match e {
        ControlError::Invalid(m) => sqlx::Error::Protocol(m),
        ControlError::Backend(m) => sqlx::Error::Protocol(format!("control store backend: {m}")),
    }
}

/// 桶删除级联：7 张表按固定顺序删除（调用方事务内）。
pub(crate) async fn delete_lifecycle_inventory_tier_for_bucket(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
) -> Result<(), ControlError> {
    for sql in [
        "DELETE FROM inventory_runs WHERE bucket = ?",
        "DELETE FROM inventory_configs WHERE bucket = ?",
        "DELETE FROM inventory_cursors WHERE bucket = ?",
        "DELETE FROM inventory_incremental WHERE bucket = ?",
        "DELETE FROM tier_restores WHERE bucket = ?",
        "DELETE FROM lifecycle_rules WHERE bucket = ?",
        "DELETE FROM lifecycle_cursors WHERE bucket = ?",
    ] {
        sqlx::query(sql)
            .bind(bucket)
            .execute(&mut *txn.conn)
            .await
            .map_err(backend)?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl ControlTxn for SqliteControlTxn<'_> {
    async fn account_status(&mut self, account_id: &str) -> Result<Option<String>, ControlError> {
        sqlx::query_scalar("SELECT status FROM accounts WHERE id = ?")
            .bind(account_id)
            .fetch_optional(&mut *self.conn)
            .await
            .map_err(backend)
    }

    async fn vpc_endpoint_upsert(&mut self, row: &VpcEndpoint) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO vpc_endpoints \
             (vpce_id, vpc_id, account_id, state, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(vpce_id) DO UPDATE SET vpc_id = excluded.vpc_id, \
               account_id = excluded.account_id, state = excluded.state, \
               updated_at = excluded.updated_at",
        )
        .bind(&row.vpce_id)
        .bind(&row.vpc_id)
        .bind(&row.account_id)
        .bind(&row.state)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn vpc_endpoint_delete(&mut self, vpce_id: &str) -> Result<bool, ControlError> {
        let res = sqlx::query("DELETE FROM vpc_endpoints WHERE vpce_id = ?")
            .bind(vpce_id)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        Ok(res.rows_affected() > 0)
    }

    async fn vpc_endpoints(&mut self) -> Result<Vec<VpcEndpoint>, ControlError> {
        let rows = sqlx::query(
            "SELECT vpce_id, vpc_id, account_id, state, created_at, updated_at \
             FROM vpc_endpoints ORDER BY vpce_id",
        )
        .fetch_all(&mut *self.conn)
        .await
        .map_err(backend)?;
        Ok(rows.iter().map(endpoint_from).collect())
    }

    async fn replace_vpc_endpoints(&mut self, rows: &[VpcEndpoint]) -> Result<(), ControlError> {
        sqlx::query("DELETE FROM vpc_endpoints")
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        for row in rows {
            sqlx::query(
                "INSERT INTO vpc_endpoints \
                 (vpce_id, vpc_id, account_id, state, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&row.vpce_id)
            .bind(&row.vpc_id)
            .bind(&row.account_id)
            .bind(&row.state)
            .bind(row.created_at)
            .bind(row.updated_at)
            .execute(&mut *self.conn)
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn access_point_create(&mut self, row: &AccessPointEntry) -> Result<bool, ControlError> {
        ap_olap::txn_access_point_create(self, row).await
    }

    async fn access_point_set_policy(
        &mut self,
        name: &str,
        policy_json: Option<&str>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        ap_olap::txn_access_point_set_policy(self, name, policy_json, updated_at).await
    }

    async fn access_point_delete(&mut self, name: &str) -> Result<bool, ControlError> {
        ap_olap::txn_access_point_delete(self, name).await
    }

    async fn access_point_exists(&mut self, name: &str) -> Result<bool, ControlError> {
        ap_olap::txn_access_point_exists(self, name).await
    }

    async fn access_points(&mut self) -> Result<Vec<AccessPointEntry>, ControlError> {
        ap_olap::txn_access_points(self).await
    }

    async fn replace_access_points(
        &mut self,
        rows: &[AccessPointEntry],
    ) -> Result<(), ControlError> {
        ap_olap::txn_replace_access_points(self, rows).await
    }

    async fn olap_config_upsert(&mut self, row: &OlapConfigEntry) -> Result<(), ControlError> {
        ap_olap::txn_olap_config_upsert(self, row).await
    }

    async fn olap_config_delete(&mut self, access_point: &str) -> Result<bool, ControlError> {
        ap_olap::txn_olap_config_delete(self, access_point).await
    }

    async fn olap_configs(&mut self) -> Result<Vec<OlapConfigEntry>, ControlError> {
        ap_olap::txn_olap_configs(self).await
    }

    async fn replace_olap_configs(&mut self, rows: &[OlapConfigEntry]) -> Result<(), ControlError> {
        ap_olap::txn_replace_olap_configs(self, rows).await
    }

    async fn delete_access_points_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError> {
        ap_olap::txn_delete_access_points_for_bucket(self, bucket).await
    }

    async fn delete_lifecycle_inventory_tier_for_bucket(
        &mut self,
        bucket: &str,
    ) -> Result<(), ControlError> {
        delete_lifecycle_inventory_tier_for_bucket(self, bucket).await
    }
}

#[async_trait::async_trait]
impl InventoryTxn for SqliteControlTxn<'_> {
    async fn inventory_config_upsert(
        &mut self,
        row: &InventoryConfigEntry,
    ) -> Result<(), ControlError> {
        inventory::txn_inventory_config_upsert(self, row).await
    }

    async fn inventory_config_delete(
        &mut self,
        bucket: &str,
        id: &str,
    ) -> Result<bool, ControlError> {
        inventory::txn_inventory_config_delete(self, bucket, id).await
    }

    async fn inventory_configs(&mut self) -> Result<Vec<InventoryConfigEntry>, ControlError> {
        inventory::txn_inventory_configs(self).await
    }

    async fn replace_inventory_configs(
        &mut self,
        rows: &[InventoryConfigEntry],
    ) -> Result<(), ControlError> {
        inventory::txn_replace_inventory_configs(self, rows).await
    }

    async fn inventory_runs(&mut self) -> Result<Vec<InventoryRunRow>, ControlError> {
        inventory::txn_inventory_runs(self).await
    }

    async fn replace_inventory_runs(
        &mut self,
        rows: &[InventoryRunRow],
    ) -> Result<(), ControlError> {
        inventory::txn_replace_inventory_runs(self, rows).await
    }

    async fn inventory_cursors(&mut self) -> Result<Vec<InventoryCursorRow>, ControlError> {
        inventory::txn_inventory_cursors(self).await
    }

    async fn replace_inventory_cursors(
        &mut self,
        rows: &[InventoryCursorRow],
    ) -> Result<(), ControlError> {
        inventory::txn_replace_inventory_cursors(self, rows).await
    }

    async fn inventory_incrementals(
        &mut self,
    ) -> Result<Vec<InventoryIncrementalRow>, ControlError> {
        inventory::txn_inventory_incrementals(self).await
    }

    async fn replace_inventory_incrementals(
        &mut self,
        rows: &[InventoryIncrementalRow],
    ) -> Result<(), ControlError> {
        inventory::txn_replace_inventory_incrementals(self, rows).await
    }

    async fn inventory_run(&mut self, id: i64) -> Result<Option<InventoryRunRow>, ControlError> {
        inventory::txn_inventory_run(self, id).await
    }

    async fn inventory_run_by_key(
        &mut self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> Result<Option<InventoryRunRow>, ControlError> {
        inventory::txn_inventory_run_by_key(self, bucket, config_id, run_key).await
    }

    async fn inventory_run_upsert(&mut self, row: &InventoryRunRow) -> Result<bool, ControlError> {
        inventory::txn_inventory_run_upsert(self, row).await
    }

    async fn inventory_run_create(
        &mut self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
        created_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<Option<i64>, ControlError> {
        inventory::txn_inventory_run_create(
            self,
            bucket,
            config_id,
            run_key,
            created_at,
            fence_term,
            fence_leader,
        )
        .await
    }

    async fn inventory_run_set_job(
        &mut self,
        run_id: i64,
        job_id: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError> {
        inventory::txn_inventory_run_set_job(self, run_id, job_id, fence_term, fence_leader).await
    }

    async fn inventory_run_started(
        &mut self,
        run_id: i64,
        started_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError> {
        inventory::txn_inventory_run_started(self, run_id, started_at, fence_term, fence_leader)
            .await
    }

    async fn inventory_run_finish(
        &mut self,
        run_id: i64,
        state: &str,
        manifest_key: Option<&str>,
        error: Option<&str>,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError> {
        inventory::txn_inventory_run_finish(
            self,
            run_id,
            state,
            manifest_key,
            error,
            fence_term,
            fence_leader,
        )
        .await
    }

    async fn inventory_cursor(
        &mut self,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryCursorRow>, ControlError> {
        inventory::txn_inventory_cursor(self, bucket, config_id).await
    }

    async fn inventory_cursor_upsert(
        &mut self,
        row: &InventoryCursorRow,
    ) -> Result<bool, ControlError> {
        inventory::txn_inventory_cursor_upsert(self, row).await
    }

    async fn inventory_cursor_delete(
        &mut self,
        bucket: &str,
        config_id: &str,
        fence_term: Option<i64>,
    ) -> Result<(), ControlError> {
        inventory::txn_inventory_cursor_delete(self, bucket, config_id, fence_term).await
    }

    async fn inventory_incremental(
        &mut self,
        bucket: &str,
        config_id: &str,
    ) -> Result<Option<InventoryIncrementalRow>, ControlError> {
        inventory::txn_inventory_incremental(self, bucket, config_id).await
    }

    async fn inventory_incremental_upsert(
        &mut self,
        row: &InventoryIncrementalRow,
    ) -> Result<bool, ControlError> {
        inventory::txn_inventory_incremental_upsert(self, row).await
    }

    async fn inventory_incremental_delete(
        &mut self,
        bucket: &str,
        config_id: &str,
    ) -> Result<(), ControlError> {
        inventory::txn_inventory_incremental_delete(self, bucket, config_id).await
    }

    async fn inventory_clear(&mut self, bucket: &str, config_id: &str) -> Result<(), ControlError> {
        inventory::txn_inventory_clear(self, bucket, config_id).await
    }
}

#[async_trait::async_trait]
impl LifecycleTxn for SqliteControlTxn<'_> {
    async fn replace_lifecycle_rules(
        &mut self,
        bucket: &str,
        rows: &[LifecycleRuleEntry],
    ) -> Result<(), ControlError> {
        lifecycle::txn_replace_lifecycle_rules(self, bucket, rows).await
    }

    async fn replace_all_lifecycle_rules(
        &mut self,
        rows: &[LifecycleRuleEntry],
    ) -> Result<(), ControlError> {
        lifecycle::txn_replace_all_lifecycle_rules(self, rows).await
    }

    async fn clear_lifecycle_rules(&mut self, bucket: &str) -> Result<(), ControlError> {
        lifecycle::txn_clear_lifecycle_rules(self, bucket).await
    }

    async fn delete_lifecycle_rules_only(&mut self, bucket: &str) -> Result<(), ControlError> {
        lifecycle::txn_delete_lifecycle_rules_only(self, bucket).await
    }

    async fn all_lifecycle_rules(&mut self) -> Result<Vec<LifecycleRuleEntry>, ControlError> {
        lifecycle::txn_all_lifecycle_rules(self).await
    }

    async fn lifecycle_cursor(
        &mut self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<LifecycleCursorRow>, ControlError> {
        lifecycle::txn_lifecycle_cursor(self, bucket, rule_id, stage).await
    }

    async fn lifecycle_cursor_upsert(
        &mut self,
        row: &LifecycleCursorRow,
    ) -> Result<bool, ControlError> {
        lifecycle::txn_lifecycle_cursor_upsert(self, row).await
    }

    async fn lifecycle_cursor_delete(
        &mut self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
        fence_term: i64,
    ) -> Result<(), ControlError> {
        lifecycle::txn_lifecycle_cursor_delete(self, bucket, rule_id, stage, fence_term).await
    }

    async fn lifecycle_cursors(&mut self) -> Result<Vec<LifecycleCursorRow>, ControlError> {
        lifecycle::txn_lifecycle_cursors(self).await
    }

    async fn replace_lifecycle_cursors(
        &mut self,
        rows: &[LifecycleCursorRow],
    ) -> Result<(), ControlError> {
        lifecycle::txn_replace_lifecycle_cursors(self, rows).await
    }
}

#[async_trait::async_trait]
impl TierTxn for SqliteControlTxn<'_> {
    async fn tier_restore(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<TierRestoreRow>, ControlError> {
        tier::txn_tier_restore(self, bucket, key, version_id).await
    }

    async fn tier_restores(&mut self) -> Result<Vec<TierRestoreRow>, ControlError> {
        tier::txn_tier_restores(self).await
    }

    async fn replace_tier_restores(&mut self, rows: &[TierRestoreRow]) -> Result<(), ControlError> {
        tier::txn_replace_tier_restores(self, rows).await
    }

    async fn tier_restore_upsert(&mut self, row: &TierRestoreRow) -> Result<bool, ControlError> {
        tier::txn_tier_restore_upsert(self, row).await
    }

    async fn tier_restore_delete(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
        fence_term: i64,
    ) -> Result<(), ControlError> {
        tier::txn_tier_restore_delete(self, bucket, key, version_id, fence_term).await
    }

    async fn tier_restore_set_state(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
        state: &str,
        expires_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError> {
        tier::txn_tier_restore_set_state(
            self,
            bucket,
            key,
            version_id,
            state,
            expires_at,
            fence_term,
            fence_leader,
        )
        .await
    }
}
