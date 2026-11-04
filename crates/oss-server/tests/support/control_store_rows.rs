//! lifecycle/inventory/tier 双后端差分的共享辅助：legacy raw SQL 参考实现
//! （测试内重新实现，不调用生产读方法）。

#![allow(dead_code)] // 每个差分测试二进制只用到自己域的辅助。

use std::sync::Arc;

use oss_server::db::Db;
use sqlx::Row;
use xl_store::control_store::lifecycle::{LifecycleCursorRow, LifecycleRuleEntry};

pub async fn fresh() -> Arc<Db> {
    Arc::new(Db::connect("sqlite::memory:").await.unwrap())
}

// ---------- lifecycle_rules ----------

#[allow(clippy::too_many_arguments)]
pub async fn raw_rule(
    db: &Db,
    bucket: &str,
    prefix: &str,
    rule_id: Option<&str>,
    expire_days: i64,
    expiration_days: Option<i64>,
    enabled: bool,
    updated_at: i64,
) {
    sqlx::query(
        "INSERT INTO lifecycle_rules (bucket, rule_id, prefix, expire_days, expiration_days, \
         transition_days, transition_class, transition_tier, enabled, updated_at, config_json) \
         VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, NULL)",
    )
    .bind(bucket)
    .bind(rule_id)
    .bind(prefix)
    .bind(expire_days)
    .bind(expiration_days)
    .bind(i64::from(enabled))
    .bind(updated_at)
    .execute(db.pool())
    .await
    .unwrap();
}

pub async fn raw_replace_lifecycle_rules(db: &Db, bucket: &str, rows: &[LifecycleRuleEntry]) {
    sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM lifecycle_cursors WHERE bucket = ?")
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
    for r in rows {
        sqlx::query(
            "INSERT INTO lifecycle_rules (bucket, rule_id, prefix, expire_days, expiration_days, \
             transition_days, transition_class, transition_tier, enabled, updated_at, config_json) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
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
        .execute(db.pool())
        .await
        .unwrap();
    }
}

fn lifecycle_rule_entry(r: &sqlx::sqlite::SqliteRow) -> LifecycleRuleEntry {
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

/// 旧 console/engine 读口径（`bucket` 过滤 + `ORDER BY rule_id, prefix`）。
pub async fn legacy_rules(db: &Db, bucket: &str) -> Vec<LifecycleRuleEntry> {
    sqlx::query(
        "SELECT bucket, rule_id, prefix, expire_days, expiration_days, transition_days, \
         transition_class, transition_tier, enabled, updated_at, config_json \
         FROM lifecycle_rules WHERE bucket = ? ORDER BY rule_id, prefix",
    )
    .bind(bucket)
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(lifecycle_rule_entry)
    .collect()
}

/// 旧 worker 读口径（全部 enabled，`ORDER BY bucket, rule_id`）。
pub async fn legacy_enabled_rules(db: &Db) -> Vec<LifecycleRuleEntry> {
    sqlx::query(
        "SELECT bucket, rule_id, prefix, expire_days, expiration_days, transition_days, \
         transition_class, transition_tier, enabled, updated_at, config_json \
         FROM lifecycle_rules WHERE enabled = 1 ORDER BY bucket, rule_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(lifecycle_rule_entry)
    .collect()
}

pub type RawRule = (
    String,
    String,
    Option<String>,
    i64,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    i64,
    i64,
    Option<String>,
);

pub async fn raw_rule_dump(db: &Db) -> Vec<RawRule> {
    sqlx::query(
        "SELECT bucket, prefix, rule_id, expire_days, expiration_days, transition_days, \
         transition_class, transition_tier, enabled, updated_at, config_json \
         FROM lifecycle_rules ORDER BY bucket, rule_id, prefix",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("bucket"),
            r.get("prefix"),
            r.get("rule_id"),
            r.get("expire_days"),
            r.get("expiration_days"),
            r.get("transition_days"),
            r.get("transition_class"),
            r.get("transition_tier"),
            r.get("enabled"),
            r.get("updated_at"),
            r.get("config_json"),
        )
    })
    .collect()
}

// ---------- lifecycle_cursors ----------

#[allow(clippy::too_many_arguments)]
pub async fn raw_cursor(
    db: &Db,
    bucket: &str,
    rule_id: &str,
    stage: &str,
    cursor: &str,
    updated_at: i64,
    fence_term: i64,
    fence_leader: i64,
) {
    sqlx::query(
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at, \
         fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(bucket)
    .bind(rule_id)
    .bind(stage)
    .bind(cursor)
    .bind(updated_at)
    .bind(fence_term)
    .bind(fence_leader)
    .execute(db.pool())
    .await
    .unwrap();
}

pub type RawCursor = (String, String, String, String, i64, i64, i64);

pub async fn raw_cursor_dump(db: &Db) -> Vec<RawCursor> {
    sqlx::query(
        "SELECT bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader \
         FROM lifecycle_cursors ORDER BY bucket, rule_id, stage",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("bucket"),
            r.get("rule_id"),
            r.get("stage"),
            r.get("cursor"),
            r.get("updated_at"),
            r.get("fence_term"),
            r.get("fence_leader"),
        )
    })
    .collect()
}

pub async fn legacy_cursor_row(
    db: &Db,
    bucket: &str,
    rule_id: &str,
    stage: &str,
) -> Option<LifecycleCursorRow> {
    sqlx::query(
        "SELECT bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader \
         FROM lifecycle_cursors WHERE bucket = ? AND rule_id = ? AND stage = ?",
    )
    .bind(bucket)
    .bind(rule_id)
    .bind(stage)
    .fetch_optional(db.pool())
    .await
    .unwrap()
    .as_ref()
    .map(|r| LifecycleCursorRow {
        bucket: r.get("bucket"),
        rule_id: r.get("rule_id"),
        stage: r.get("stage"),
        cursor: r.get("cursor"),
        updated_at: r.get("updated_at"),
        fence_term: r.get("fence_term"),
        fence_leader: r.get("fence_leader"),
    })
}

/// 旧 `fence_term <= excluded.fence_term` 门控 upsert；返回是否恰好 1 行。
pub async fn raw_cursor_upsert(db: &Db, row: &LifecycleCursorRow) -> bool {
    let res = sqlx::query(
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at, \
         fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(bucket, rule_id, stage) DO UPDATE SET \
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
    .execute(db.pool())
    .await
    .unwrap();
    res.rows_affected() == 1
}

pub async fn raw_cursor_delete(db: &Db, bucket: &str, rule_id: &str, stage: &str, fence_term: i64) {
    sqlx::query(
        "DELETE FROM lifecycle_cursors \
         WHERE bucket = ? AND rule_id = ? AND stage = ? AND fence_term <= ?",
    )
    .bind(bucket)
    .bind(rule_id)
    .bind(stage)
    .bind(fence_term)
    .execute(db.pool())
    .await
    .unwrap();
}

// ---------- inventory_configs / cursors / incrementals ----------

pub async fn raw_inventory_config(
    db: &Db,
    bucket: &str,
    id: &str,
    owner_id: i64,
    config_json: &str,
    updated_at: i64,
) {
    sqlx::query(
        "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(bucket)
    .bind(id)
    .bind(owner_id)
    .bind(config_json)
    .bind(updated_at)
    .execute(db.pool())
    .await
    .unwrap();
}

pub type RawInventoryConfig = (String, String, i64, String, i64);

/// 旧导出/调度读口径：`ORDER BY bucket, id`。
pub async fn raw_inventory_config_dump(db: &Db) -> Vec<RawInventoryConfig> {
    sqlx::query(
        "SELECT bucket, id, owner_id, config_json, updated_at \
         FROM inventory_configs ORDER BY bucket, id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("bucket"),
            r.get("id"),
            r.get("owner_id"),
            r.get("config_json"),
            r.get("updated_at"),
        )
    })
    .collect()
}

pub async fn raw_inventory_cursor(
    db: &Db,
    bucket: &str,
    config_id: &str,
    cursor: &str,
    config_updated_at: i64,
    updated_at: i64,
) {
    sqlx::query(
        "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, \
         updated_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(bucket)
    .bind(config_id)
    .bind(cursor)
    .bind(config_updated_at)
    .bind(updated_at)
    .execute(db.pool())
    .await
    .unwrap();
}

pub type RawInventoryCursor = (String, String, String, i64, i64);

pub async fn raw_inventory_cursor_dump(db: &Db) -> Vec<RawInventoryCursor> {
    sqlx::query(
        "SELECT bucket, config_id, cursor, config_updated_at, updated_at \
         FROM inventory_cursors ORDER BY bucket, config_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("bucket"),
            r.get("config_id"),
            r.get("cursor"),
            r.get("config_updated_at"),
            r.get("updated_at"),
        )
    })
    .collect()
}

pub async fn raw_inventory_incremental(
    db: &Db,
    bucket: &str,
    config_id: &str,
    enabled: bool,
    updated_at: i64,
) {
    sqlx::query(
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
         VALUES (?, ?, ?, ?)",
    )
    .bind(bucket)
    .bind(config_id)
    .bind(i64::from(enabled))
    .bind(updated_at)
    .execute(db.pool())
    .await
    .unwrap();
}

pub type RawInventoryIncremental = (String, String, i64, i64);

pub async fn raw_inventory_incremental_dump(db: &Db) -> Vec<RawInventoryIncremental> {
    sqlx::query(
        "SELECT bucket, config_id, enabled, updated_at \
         FROM inventory_incremental ORDER BY bucket, config_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("bucket"),
            r.get("config_id"),
            r.get("enabled"),
            r.get("updated_at"),
        )
    })
    .collect()
}

// ---------- inventory runs（id 是行属性，不是身份键） ----------

pub async fn raw_inventory_run(db: &Db, run: &xl_store::control_store::inventory::InventoryRunRow) {
    sqlx::query(
        "INSERT INTO inventory_runs (id, bucket, config_id, run_key, started_at, state, \
         job_id, manifest_key, error, created_at, fence_term, fence_leader) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(run.id)
    .bind(&run.bucket)
    .bind(&run.config_id)
    .bind(&run.run_key)
    .bind(run.started_at)
    .bind(&run.state)
    .bind(run.job_id)
    .bind(&run.manifest_key)
    .bind(&run.error)
    .bind(run.created_at)
    .bind(run.fence_term)
    .bind(run.fence_leader)
    .execute(db.pool())
    .await
    .unwrap();
}

pub async fn raw_inventory_run_dump(
    db: &Db,
) -> Vec<xl_store::control_store::inventory::InventoryRunRow> {
    sqlx::query(
        "SELECT id, bucket, config_id, run_key, started_at, state, job_id, manifest_key, \
         error, created_at, fence_term, fence_leader FROM inventory_runs ORDER BY id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| xl_store::control_store::inventory::InventoryRunRow {
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
    })
    .collect()
}

pub async fn raw_runstate_next_run_id(db: &Db) -> Option<String> {
    sqlx::query_scalar("SELECT value FROM runstate_meta WHERE key = 'next_run_id'")
        .fetch_optional(db.pool())
        .await
        .unwrap()
}
