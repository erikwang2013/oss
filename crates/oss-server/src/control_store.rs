//! `ControlStore` 的 SQLite 适配器：复用 `Db` 的既有连接池，不新建第二个池。
//!
//! 本切片只读；写路径的 `ControlTxn` 适配器由后续任务加入。

use sqlx::{Row, SqlitePool};
use xl_store::control_store::{ControlError, ControlStore, VpcEndpoint};

pub struct SqliteControlStore {
    pool: SqlitePool,
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
}
