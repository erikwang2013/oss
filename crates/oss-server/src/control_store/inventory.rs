//! inventory config 的 SQLite 读写实现。
//!
//! 运行期口径（`InventoryStore`）：LEFT JOIN users 解析属主；`owner_id == 0` →
//! `Admin`，用户行缺失 → `None`（调用方保留 legacy 数字 uid 文本）。
//! 快照导出口径（`InventoryTxn::inventory_configs`）：严格 fail-closed，悬空属主
//! 报 `Invalid`。

use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use xl_store::control_store::inventory::{InventoryConfigEntry, InventoryConfigView};
use xl_store::control_store::{ControlError, OwnerRef};

use super::ap_olap::resolve_owner;
use super::{SqliteControlStore, SqliteControlTxn};

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

const CONFIG_SELECT: &str = "SELECT i.bucket, i.id, i.owner_id, u.username AS owner_username, \
     i.config_json, i.updated_at FROM inventory_configs i \
     LEFT JOIN users u ON u.id = i.owner_id WHERE i.bucket = ? AND i.id = ?";

const CONFIGS_SELECT: &str = "SELECT i.bucket, i.id, i.owner_id, u.username AS owner_username, \
     i.config_json, i.updated_at FROM inventory_configs i \
     LEFT JOIN users u ON u.id = i.owner_id WHERE i.bucket = ? ORDER BY i.id";

const ALL_SELECT: &str = "SELECT i.bucket, i.id, i.owner_id, u.username AS owner_username, \
     i.config_json, i.updated_at FROM inventory_configs i \
     LEFT JOIN users u ON u.id = i.owner_id ORDER BY i.bucket, i.id";

const EXPORT_SELECT: &str = "SELECT i.bucket, i.id, i.owner_id, i.config_json, i.updated_at, \
     u.username AS owner_username, u.is_admin AS owner_is_admin FROM inventory_configs i \
     LEFT JOIN users u ON u.id = i.owner_id ORDER BY i.bucket, i.id";

fn runtime_view(r: &SqliteRow) -> InventoryConfigView {
    let owner_uid: i64 = r.get("owner_id");
    let username: Option<String> = r.get("owner_username");
    InventoryConfigView {
        bucket: r.get("bucket"),
        id: r.get("id"),
        owner: if owner_uid == 0 {
            Some(OwnerRef::Admin)
        } else {
            username.map(OwnerRef::User)
        },
        owner_uid,
        config_json: r.get("config_json"),
        updated_at: r.get("updated_at"),
    }
}

/// 严格导出映射：`owner_id == 0` 或 `is_admin == 1` → `Admin`；用户行缺失 → `Invalid`。
fn export_entry(r: &SqliteRow) -> Result<InventoryConfigEntry, ControlError> {
    let owner_id: i64 = r.get("owner_id");
    let username: Option<String> = r.get("owner_username");
    let is_admin: Option<i64> = r.get("owner_is_admin");
    let owner = if owner_id == 0 || is_admin == Some(1) {
        OwnerRef::Admin
    } else {
        match username {
            Some(name) => OwnerRef::User(name),
            None => {
                return Err(ControlError::Invalid(format!(
                    "owner user id {owner_id} missing"
                )));
            }
        }
    };
    Ok(InventoryConfigEntry {
        bucket: r.get("bucket"),
        id: r.get("id"),
        owner,
        config_json: r.get("config_json"),
        updated_at: r.get("updated_at"),
    })
}

pub(super) async fn inventory_config(
    store: &SqliteControlStore,
    bucket: &str,
    id: &str,
) -> Result<Option<InventoryConfigView>, ControlError> {
    let row = sqlx::query(CONFIG_SELECT)
        .bind(bucket)
        .bind(id)
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?;
    Ok(row.as_ref().map(runtime_view))
}

pub(super) async fn inventory_configs(
    store: &SqliteControlStore,
    bucket: &str,
) -> Result<Vec<InventoryConfigView>, ControlError> {
    let rows = sqlx::query(CONFIGS_SELECT)
        .bind(bucket)
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?;
    Ok(rows.iter().map(runtime_view).collect())
}

pub(super) async fn inventory_configs_all(
    store: &SqliteControlStore,
) -> Result<Vec<InventoryConfigView>, ControlError> {
    let rows = sqlx::query(ALL_SELECT)
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?;
    Ok(rows.iter().map(runtime_view).collect())
}

pub(super) async fn inventory_config_count(
    store: &SqliteControlStore,
    bucket: &str,
) -> Result<i64, ControlError> {
    sqlx::query_scalar("SELECT COUNT(*) FROM inventory_configs WHERE bucket = ?")
        .bind(bucket)
        .fetch_one(&store.pool)
        .await
        .map_err(backend)
}

pub(super) async fn txn_inventory_config_upsert(
    txn: &mut SqliteControlTxn<'_>,
    row: &InventoryConfigEntry,
) -> Result<(), ControlError> {
    let owner_id = resolve_owner(&mut *txn.conn, &row.owner).await?;
    sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
        .bind(&row.bucket)
        .bind(&row.id)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
        .bind(&row.bucket)
        .bind(&row.id)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    sqlx::query(
        "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) \
         VALUES (?, ?, ?, ?, ?) ON CONFLICT(bucket, id) DO UPDATE SET \
         owner_id = excluded.owner_id, config_json = excluded.config_json, \
         updated_at = excluded.updated_at",
    )
    .bind(&row.bucket)
    .bind(&row.id)
    .bind(owner_id)
    .bind(&row.config_json)
    .bind(row.updated_at)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_inventory_config_delete(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    id: &str,
) -> Result<bool, ControlError> {
    sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
        .bind(bucket)
        .bind(id)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
        .bind(bucket)
        .bind(id)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    let res = sqlx::query("DELETE FROM inventory_configs WHERE bucket = ? AND id = ?")
        .bind(bucket)
        .bind(id)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn txn_inventory_configs(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<InventoryConfigEntry>, ControlError> {
    let rows = sqlx::query(EXPORT_SELECT)
        .fetch_all(&mut *txn.conn)
        .await
        .map_err(backend)?;
    rows.iter().map(export_entry).collect()
}

pub(super) async fn txn_replace_inventory_configs(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[InventoryConfigEntry],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM inventory_configs")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        let owner_id = resolve_owner(&mut *txn.conn, &row.owner).await?;
        sqlx::query(
            "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&row.bucket)
        .bind(&row.id)
        .bind(owner_id)
        .bind(&row.config_json)
        .bind(row.updated_at)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}
