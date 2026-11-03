//! 控制面的 SQLite 适配器：只读 `SqliteControlStore` 与调用方事务内的写
//! `SqliteControlTxn` 共用 `Db` 的既有连接池，不新建第二个池。
//!
//! 事务由调用方持有；适配器不 begin/commit/rollback。

use sqlx::{Row, SqlitePool};
use xl_store::control_store::{
    AccessPointEntry, ControlError, ControlStore, ControlTxn, OlapConfigEntry, VpcEndpoint,
};

mod ap_olap;

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

    async fn replace_access_points(&mut self, rows: &[AccessPointEntry]) -> Result<(), ControlError> {
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
}
