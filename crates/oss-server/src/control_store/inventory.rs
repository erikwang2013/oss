//! inventory config 的 SQLite 读写实现。
//!
//! 运行期口径（`InventoryStore`）：LEFT JOIN users 解析属主；`owner_id == 0` →
//! `Admin`，用户行缺失 → `None`（调用方保留 legacy 数字 uid 文本）。
//! 快照导出口径（`InventoryTxn::inventory_configs`）：严格 fail-closed，悬空属主
//! 报 `Invalid`。

use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryConfigView, InventoryCursorRow, InventoryIncrementalRow,
    InventoryRunRow,
};
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

// ---------- run / cursor / incremental ----------

const RUN_COLS: &str = "id, bucket, config_id, run_key, started_at, state, job_id, \
     manifest_key, error, created_at, fence_term, fence_leader";

fn run_from(r: &SqliteRow) -> InventoryRunRow {
    InventoryRunRow {
        id: r.get("id"),
        bucket: r.get("bucket"),
        config_id: r.get("config_id"),
        run_key: r.get("run_key"),
        started_at: r.get("started_at"),
        state: r.get("state"),
        job_id: r.get("job_id"),
        manifest_key: r.get("manifest_key"),
        error: r.get("error"),
        created_at: r.get("created_at"),
        fence_term: r.get("fence_term"),
        fence_leader: r.get("fence_leader"),
    }
}

fn cursor_from(r: &SqliteRow) -> InventoryCursorRow {
    InventoryCursorRow {
        bucket: r.get("bucket"),
        config_id: r.get("config_id"),
        cursor: r.get("cursor"),
        config_updated_at: r.get("config_updated_at"),
        updated_at: r.get("updated_at"),
        fence_term: r.get("fence_term"),
        fence_leader: r.get("fence_leader"),
    }
}

fn incremental_from(r: &SqliteRow) -> InventoryIncrementalRow {
    InventoryIncrementalRow {
        bucket: r.get("bucket"),
        config_id: r.get("config_id"),
        enabled: r.get::<i64, _>("enabled") != 0,
        updated_at: r.get("updated_at"),
    }
}

pub(super) async fn inventory_run(
    store: &SqliteControlStore,
    id: i64,
) -> Result<Option<InventoryRunRow>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {RUN_COLS} FROM inventory_runs WHERE id = ?"
    )))
    .bind(id)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(run_from))
}

pub(super) async fn inventory_run_by_key(
    store: &SqliteControlStore,
    bucket: &str,
    config_id: &str,
    run_key: &str,
) -> Result<Option<InventoryRunRow>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {RUN_COLS} FROM inventory_runs WHERE bucket = ? AND config_id = ? AND run_key = ?"
    )))
    .bind(bucket)
    .bind(config_id)
    .bind(run_key)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(run_from))
}

pub(super) async fn inventory_cursor(
    store: &SqliteControlStore,
    bucket: &str,
    config_id: &str,
) -> Result<Option<InventoryCursorRow>, ControlError> {
    let row = sqlx::query(
        "SELECT bucket, config_id, cursor, config_updated_at, updated_at, fence_term, \
         fence_leader FROM inventory_cursors WHERE bucket = ? AND config_id = ?",
    )
    .bind(bucket)
    .bind(config_id)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(cursor_from))
}

pub(super) async fn inventory_incremental(
    store: &SqliteControlStore,
    bucket: &str,
    config_id: &str,
) -> Result<Option<InventoryIncrementalRow>, ControlError> {
    let row = sqlx::query(
        "SELECT bucket, config_id, enabled, updated_at FROM inventory_incremental \
         WHERE bucket = ? AND config_id = ?",
    )
    .bind(bucket)
    .bind(config_id)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(incremental_from))
}

pub(super) async fn txn_inventory_runs(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<InventoryRunRow>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {RUN_COLS} FROM inventory_runs ORDER BY id"
    )))
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(run_from).collect())
}

pub(super) async fn txn_replace_inventory_runs(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[InventoryRunRow],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM inventory_runs")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        sqlx::query(
            "INSERT INTO inventory_runs (id, bucket, config_id, run_key, started_at, state, \
             job_id, manifest_key, error, created_at, fence_term, fence_leader) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id)
        .bind(&row.bucket)
        .bind(&row.config_id)
        .bind(&row.run_key)
        .bind(row.started_at)
        .bind(&row.state)
        .bind(row.job_id)
        .bind(&row.manifest_key)
        .bind(&row.error)
        .bind(row.created_at)
        .bind(row.fence_term)
        .bind(row.fence_leader)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}

pub(super) async fn txn_inventory_cursors(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<InventoryCursorRow>, ControlError> {
    let rows = sqlx::query(
        "SELECT bucket, config_id, cursor, config_updated_at, updated_at, fence_term, \
         fence_leader FROM inventory_cursors ORDER BY bucket, config_id",
    )
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(cursor_from).collect())
}

pub(super) async fn txn_replace_inventory_cursors(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[InventoryCursorRow],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM inventory_cursors")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        sqlx::query(
            "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, \
             updated_at, fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.bucket)
        .bind(&row.config_id)
        .bind(&row.cursor)
        .bind(row.config_updated_at)
        .bind(row.updated_at)
        .bind(row.fence_term)
        .bind(row.fence_leader)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}

pub(super) async fn txn_inventory_incrementals(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<InventoryIncrementalRow>, ControlError> {
    let rows = sqlx::query(
        "SELECT bucket, config_id, enabled, updated_at FROM inventory_incremental \
         ORDER BY bucket, config_id",
    )
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(incremental_from).collect())
}

pub(super) async fn txn_replace_inventory_incrementals(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[InventoryIncrementalRow],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM inventory_incremental")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        sqlx::query(
            "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(&row.bucket)
        .bind(&row.config_id)
        .bind(row.enabled as i64)
        .bind(row.updated_at)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}

pub(super) async fn txn_inventory_run(
    txn: &mut SqliteControlTxn<'_>,
    id: i64,
) -> Result<Option<InventoryRunRow>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {RUN_COLS} FROM inventory_runs WHERE id = ?"
    )))
    .bind(id)
    .fetch_optional(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(run_from))
}

pub(super) async fn txn_inventory_run_by_key(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    config_id: &str,
    run_key: &str,
) -> Result<Option<InventoryRunRow>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {RUN_COLS} FROM inventory_runs WHERE bucket = ? AND config_id = ? AND run_key = ?"
    )))
    .bind(bucket)
    .bind(config_id)
    .bind(run_key)
    .fetch_optional(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(run_from))
}

pub(super) async fn txn_inventory_run_upsert(
    txn: &mut SqliteControlTxn<'_>,
    row: &InventoryRunRow,
) -> Result<bool, ControlError> {
    let res = sqlx::query(
        "INSERT INTO inventory_runs (id, bucket, config_id, run_key, started_at, state, \
         job_id, manifest_key, error, created_at, fence_term, fence_leader) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(bucket, config_id, run_key) DO UPDATE SET id = excluded.id, \
         started_at = excluded.started_at, state = excluded.state, job_id = excluded.job_id, \
         manifest_key = excluded.manifest_key, error = excluded.error, \
         created_at = excluded.created_at, fence_term = excluded.fence_term, \
         fence_leader = excluded.fence_leader",
    )
    .bind(row.id)
    .bind(&row.bucket)
    .bind(&row.config_id)
    .bind(&row.run_key)
    .bind(row.started_at)
    .bind(&row.state)
    .bind(row.job_id)
    .bind(&row.manifest_key)
    .bind(&row.error)
    .bind(row.created_at)
    .bind(row.fence_term)
    .bind(row.fence_leader)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn txn_inventory_run_create(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    config_id: &str,
    run_key: &str,
    created_at: i64,
    fence_term: i64,
    fence_leader: i64,
) -> Result<Option<i64>, ControlError> {
    let res = sqlx::query(
        "INSERT OR IGNORE INTO inventory_runs \
         (bucket, config_id, run_key, started_at, state, created_at, fence_term, fence_leader) \
         VALUES (?, ?, ?, 0, 'running', ?, ?, ?)",
    )
    .bind(bucket)
    .bind(config_id)
    .bind(run_key)
    .bind(created_at)
    .bind(fence_term)
    .bind(fence_leader)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok((res.rows_affected() > 0).then(|| res.last_insert_rowid()))
}

pub(super) async fn txn_inventory_run_set_job(
    txn: &mut SqliteControlTxn<'_>,
    run_id: i64,
    job_id: i64,
    fence_term: i64,
    fence_leader: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE inventory_runs SET job_id = ?, fence_term = ?, fence_leader = ? \
         WHERE id = ? AND fence_term <= ?",
    )
    .bind(job_id)
    .bind(fence_term)
    .bind(fence_leader)
    .bind(run_id)
    .bind(fence_term)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_inventory_run_started(
    txn: &mut SqliteControlTxn<'_>,
    run_id: i64,
    started_at: i64,
    fence_term: i64,
    fence_leader: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE inventory_runs SET \
         started_at = CASE WHEN started_at = 0 THEN ? ELSE started_at END, \
         fence_term = ?, fence_leader = ? WHERE id = ? AND fence_term <= ?",
    )
    .bind(started_at)
    .bind(fence_term)
    .bind(fence_leader)
    .bind(run_id)
    .bind(fence_term)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_inventory_run_finish(
    txn: &mut SqliteControlTxn<'_>,
    run_id: i64,
    state: &str,
    manifest_key: Option<&str>,
    error: Option<&str>,
    fence_term: i64,
    fence_leader: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE inventory_runs SET state = ?, manifest_key = ?, error = ?, \
         fence_term = ?, fence_leader = ? WHERE id = ? AND fence_term <= ?",
    )
    .bind(state)
    .bind(manifest_key)
    .bind(error)
    .bind(fence_term)
    .bind(fence_leader)
    .bind(run_id)
    .bind(fence_term)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_inventory_cursor(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    config_id: &str,
) -> Result<Option<InventoryCursorRow>, ControlError> {
    let row = sqlx::query(
        "SELECT bucket, config_id, cursor, config_updated_at, updated_at, fence_term, \
         fence_leader FROM inventory_cursors WHERE bucket = ? AND config_id = ?",
    )
    .bind(bucket)
    .bind(config_id)
    .fetch_optional(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(cursor_from))
}

pub(super) async fn txn_inventory_cursor_upsert(
    txn: &mut SqliteControlTxn<'_>,
    row: &InventoryCursorRow,
) -> Result<bool, ControlError> {
    let res = sqlx::query(
        "INSERT INTO inventory_cursors \
         (bucket, config_id, cursor, config_updated_at, updated_at, fence_term, fence_leader) \
         VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(bucket, config_id) DO UPDATE SET \
         cursor = excluded.cursor, config_updated_at = excluded.config_updated_at, \
         updated_at = excluded.updated_at, fence_term = excluded.fence_term, \
         fence_leader = excluded.fence_leader \
         WHERE inventory_cursors.fence_term <= excluded.fence_term",
    )
    .bind(&row.bucket)
    .bind(&row.config_id)
    .bind(&row.cursor)
    .bind(row.config_updated_at)
    .bind(row.updated_at)
    .bind(row.fence_term)
    .bind(row.fence_leader)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn txn_inventory_cursor_delete(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    config_id: &str,
    fence_term: Option<i64>,
) -> Result<(), ControlError> {
    match fence_term {
        Some(term) => {
            sqlx::query(
                "DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ? \
                 AND fence_term <= ?",
            )
            .bind(bucket)
            .bind(config_id)
            .bind(term)
        }
        None => sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(config_id),
    }
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_inventory_incremental(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    config_id: &str,
) -> Result<Option<InventoryIncrementalRow>, ControlError> {
    let row = sqlx::query(
        "SELECT bucket, config_id, enabled, updated_at FROM inventory_incremental \
         WHERE bucket = ? AND config_id = ?",
    )
    .bind(bucket)
    .bind(config_id)
    .fetch_optional(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(incremental_from))
}

pub(super) async fn txn_inventory_incremental_upsert(
    txn: &mut SqliteControlTxn<'_>,
    row: &InventoryIncrementalRow,
) -> Result<bool, ControlError> {
    let res = sqlx::query(
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
         VALUES (?, ?, ?, ?) ON CONFLICT(bucket, config_id) DO UPDATE SET \
         enabled = excluded.enabled, updated_at = excluded.updated_at",
    )
    .bind(&row.bucket)
    .bind(&row.config_id)
    .bind(row.enabled as i64)
    .bind(row.updated_at)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn txn_inventory_incremental_delete(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    config_id: &str,
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
        .bind(bucket)
        .bind(config_id)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_inventory_clear(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    config_id: &str,
) -> Result<(), ControlError> {
    txn_inventory_cursor_delete(txn, bucket, config_id, None).await?;
    txn_inventory_incremental_delete(txn, bucket, config_id).await
}
