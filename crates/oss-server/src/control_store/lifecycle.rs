//! lifecycle 的 SQLite 读写实现（规则 + 扫描游标）。

use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};
use xl_store::control_store::lifecycle::{LifecycleCursorRow, LifecycleRuleEntry};
use xl_store::control_store::ControlError;

use super::{SqliteControlStore, SqliteControlTxn};

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

const RULE_COLUMNS: &str = "bucket, rule_id, prefix, expire_days, expiration_days, \
     transition_days, transition_class, transition_tier, enabled, updated_at, config_json";

const INSERT_RULE: &str = "INSERT INTO lifecycle_rules (bucket, rule_id, prefix, expire_days, \
     expiration_days, transition_days, transition_class, transition_tier, enabled, updated_at, \
     config_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

const CURSOR_COLUMNS: &str =
    "bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader";

fn rule_from(r: &SqliteRow) -> LifecycleRuleEntry {
    LifecycleRuleEntry {
        bucket: r.get("bucket"),
        prefix: r.get("prefix"),
        rule_id: r.get("rule_id"),
        expire_days: r.get("expire_days"),
        expiration_days: r.get("expiration_days"),
        transition_days: r.get("transition_days"),
        transition_class: r.get("transition_class"),
        transition_tier: r.get("transition_tier"),
        enabled: r.get::<i64, _>("enabled") != 0,
        updated_at: r.get("updated_at"),
        config_json: r.get("config_json"),
    }
}

fn cursor_from(r: &SqliteRow) -> LifecycleCursorRow {
    LifecycleCursorRow {
        bucket: r.get("bucket"),
        rule_id: r.get("rule_id"),
        stage: r.get("stage"),
        cursor: r.get("cursor"),
        updated_at: r.get("updated_at"),
        fence_term: r.get("fence_term"),
        fence_leader: r.get("fence_leader"),
    }
}

async fn insert_rule(
    conn: &mut SqliteConnection,
    bucket: &str,
    r: &LifecycleRuleEntry,
) -> Result<(), ControlError> {
    sqlx::query(INSERT_RULE)
        .bind(bucket)
        .bind(&r.rule_id)
        .bind(&r.prefix)
        .bind(r.expire_days)
        .bind(r.expiration_days)
        .bind(r.transition_days)
        .bind(&r.transition_class)
        .bind(&r.transition_tier)
        .bind(i64::from(r.enabled))
        .bind(r.updated_at)
        .bind(&r.config_json)
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(())
}

pub(super) async fn lifecycle_rules(
    store: &SqliteControlStore,
    bucket: &str,
) -> Result<Vec<LifecycleRuleEntry>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {RULE_COLUMNS} FROM lifecycle_rules WHERE bucket = ? ORDER BY rule_id, prefix"
    )))
    .bind(bucket)
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(rule_from).collect())
}

pub(super) async fn enabled_lifecycle_rules(
    store: &SqliteControlStore,
) -> Result<Vec<LifecycleRuleEntry>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {RULE_COLUMNS} FROM lifecycle_rules WHERE enabled = 1 ORDER BY bucket, rule_id"
    )))
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(rule_from).collect())
}

pub(super) async fn lifecycle_cursor(
    store: &SqliteControlStore,
    bucket: &str,
    rule_id: &str,
    stage: &str,
) -> Result<Option<String>, ControlError> {
    sqlx::query_scalar(
        "SELECT cursor FROM lifecycle_cursors WHERE bucket = ? AND rule_id = ? AND stage = ?",
    )
    .bind(bucket)
    .bind(rule_id)
    .bind(stage)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)
}

pub(super) async fn lifecycle_cursor_row(
    store: &SqliteControlStore,
    bucket: &str,
    rule_id: &str,
    stage: &str,
) -> Result<Option<LifecycleCursorRow>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {CURSOR_COLUMNS} FROM lifecycle_cursors \
         WHERE bucket = ? AND rule_id = ? AND stage = ?"
    )))
    .bind(bucket)
    .bind(rule_id)
    .bind(stage)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(cursor_from))
}

// ---------- 写路径（调用方事务内） ----------

pub(super) async fn txn_replace_lifecycle_rules(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    rows: &[LifecycleRuleEntry],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    // 4.10：规则整桶替换 → 旧扫描游标作废（prefix/动作变了，续扫位置不再可信）。
    sqlx::query("DELETE FROM lifecycle_cursors WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for r in rows {
        insert_rule(&mut txn.conn, bucket, r).await?;
    }
    Ok(())
}

pub(super) async fn txn_replace_all_lifecycle_rules(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[LifecycleRuleEntry],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM lifecycle_rules")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for r in rows {
        insert_rule(&mut txn.conn, &r.bucket, r).await?;
    }
    Ok(())
}

pub(super) async fn txn_clear_lifecycle_rules(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    sqlx::query("DELETE FROM lifecycle_cursors WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_delete_lifecycle_rules_only(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_all_lifecycle_rules(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<LifecycleRuleEntry>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {RULE_COLUMNS} FROM lifecycle_rules ORDER BY bucket, rule_id, prefix"
    )))
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(rule_from).collect())
}

pub(super) async fn txn_lifecycle_cursor(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    rule_id: &str,
    stage: &str,
) -> Result<Option<LifecycleCursorRow>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {CURSOR_COLUMNS} FROM lifecycle_cursors \
         WHERE bucket = ? AND rule_id = ? AND stage = ?"
    )))
    .bind(bucket)
    .bind(rule_id)
    .bind(stage)
    .fetch_optional(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(cursor_from))
}

pub(super) async fn txn_lifecycle_cursor_upsert(
    txn: &mut SqliteControlTxn<'_>,
    row: &LifecycleCursorRow,
) -> Result<bool, ControlError> {
    let res = sqlx::query(
        "INSERT INTO lifecycle_cursors \
         (bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader) \
         VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(bucket, rule_id, stage) DO UPDATE SET \
         cursor = excluded.cursor, updated_at = excluded.updated_at, \
         fence_term = excluded.fence_term, fence_leader = excluded.fence_leader \
         WHERE lifecycle_cursors.fence_term <= excluded.fence_term",
    )
    .bind(&row.bucket)
    .bind(&row.rule_id)
    .bind(&row.stage)
    .bind(&row.cursor)
    .bind(row.updated_at)
    .bind(row.fence_term)
    .bind(row.fence_leader)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn txn_lifecycle_cursor_delete(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
    rule_id: &str,
    stage: &str,
    fence_term: i64,
) -> Result<(), ControlError> {
    sqlx::query(
        "DELETE FROM lifecycle_cursors \
         WHERE bucket = ? AND rule_id = ? AND stage = ? AND fence_term <= ?",
    )
    .bind(bucket)
    .bind(rule_id)
    .bind(stage)
    .bind(fence_term)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_lifecycle_cursors(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<LifecycleCursorRow>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {CURSOR_COLUMNS} FROM lifecycle_cursors ORDER BY bucket, rule_id, stage"
    )))
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(cursor_from).collect())
}

pub(super) async fn txn_replace_lifecycle_cursors(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[LifecycleCursorRow],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM lifecycle_cursors")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for c in rows {
        sqlx::query(
            "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at, \
             fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&c.bucket)
        .bind(&c.rule_id)
        .bind(&c.stage)
        .bind(&c.cursor)
        .bind(c.updated_at)
        .bind(c.fence_term)
        .bind(c.fence_leader)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    }
    Ok(())
}
