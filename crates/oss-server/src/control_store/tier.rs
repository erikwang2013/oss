//! tier restore 的 SQLite 读写实现（调用方事务内；不 begin/commit）。

use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use xl_store::control_store::ControlError;
use xl_store::control_store::tier::TierRestoreRow;

use super::{SqliteControlStore, SqliteControlTxn};

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

const COLS: &str = "bucket, key, version_id, expires_at, job_id, state, created_at, \
     fence_term, fence_leader";

fn row_from(r: &SqliteRow) -> TierRestoreRow {
    TierRestoreRow {
        bucket: r.get("bucket"),
        key: r.get("key"),
        version_id: r.get("version_id"),
        expires_at: r.get("expires_at"),
        job_id: r.get("job_id"),
        state: r.get("state"),
        created_at: r.get("created_at"),
        fence_term: r.get("fence_term"),
        fence_leader: r.get("fence_leader"),
    }
}

pub(super) async fn tier_restore(
    store: &SqliteControlStore,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Option<TierRestoreRow>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLS} FROM tier_restores WHERE bucket = ? AND key = ? AND version_id = ?"
    )))
    .bind(bucket)
    .bind(key)
    .bind(version_id)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(row_from))
}

pub(super) async fn tier_restores(
    store: &SqliteControlStore,
    state: &str,
) -> Result<Vec<TierRestoreRow>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLS} FROM tier_restores WHERE state = ? ORDER BY created_at"
    )))
    .bind(state)
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(row_from).collect())
}

pub(super) async fn txn_tier_restore(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Option<TierRestoreRow>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLS} FROM tier_restores WHERE bucket = ? AND key = ? AND version_id = ?"
    )))
    .bind(bucket)
    .bind(key)
    .bind(version_id)
    .fetch_optional(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(row_from))
}

pub(super) async fn txn_tier_restores(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<TierRestoreRow>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLS} FROM tier_restores ORDER BY bucket, key, version_id"
    )))
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(row_from).collect())
}

pub(super) async fn txn_replace_tier_restores(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[TierRestoreRow],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM tier_restores")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        sqlx::query(
            "INSERT INTO tier_restores (bucket, key, version_id, expires_at, job_id, state, \
             created_at, fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.bucket)
        .bind(&row.key)
        .bind(&row.version_id)
        .bind(row.expires_at)
        .bind(row.job_id)
        .bind(&row.state)
        .bind(row.created_at)
        .bind(row.fence_term)
        .bind(row.fence_leader)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}

pub(super) async fn txn_tier_restore_upsert(
    txn: &mut SqliteControlTxn<'_>,
    row: &TierRestoreRow,
) -> Result<bool, ControlError> {
    let res = sqlx::query(
        "INSERT INTO tier_restores (bucket, key, version_id, expires_at, job_id, state, \
         created_at, fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(bucket, key, version_id) DO UPDATE SET \
         expires_at = excluded.expires_at, job_id = excluded.job_id, state = excluded.state, \
         created_at = excluded.created_at, fence_term = excluded.fence_term, \
         fence_leader = excluded.fence_leader \
         WHERE tier_restores.fence_term <= excluded.fence_term",
    )
    .bind(&row.bucket)
    .bind(&row.key)
    .bind(&row.version_id)
    .bind(row.expires_at)
    .bind(row.job_id)
    .bind(&row.state)
    .bind(row.created_at)
    .bind(row.fence_term)
    .bind(row.fence_leader)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn txn_tier_restore_delete(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    key: &str,
    version_id: &str,
    fence_term: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "DELETE FROM tier_restores \
         WHERE bucket = ? AND key = ? AND version_id = ? AND fence_term <= ?",
    )
    .bind(bucket)
    .bind(key)
    .bind(version_id)
    .bind(fence_term)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // 行字段面即参数面。
pub(super) async fn txn_tier_restore_set_state(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    key: &str,
    version_id: &str,
    state: &str,
    expires_at: i64,
    fence_term: i64,
    fence_leader: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE tier_restores SET state = ?, expires_at = ?, \
         fence_term = ?, fence_leader = ? \
         WHERE bucket = ? AND key = ? AND version_id = ? AND fence_term <= ?",
    )
    .bind(state)
    .bind(expires_at)
    .bind(fence_term)
    .bind(fence_leader)
    .bind(bucket)
    .bind(key)
    .bind(version_id)
    .bind(fence_term)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}
