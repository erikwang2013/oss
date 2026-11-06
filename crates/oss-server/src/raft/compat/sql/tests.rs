//! SQLite schema_meta activation-guard tests.

use super::*;

#[tokio::test]
async fn activation_marker_defaults_to_one_and_only_increases() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(read_activation(&pool).await.unwrap(), 1);
    let mut conn = pool.acquire().await.unwrap();
    bump_activation_tx(&mut conn, 2).await.unwrap();
    bump_activation_tx(&mut conn, 2).await.unwrap();
    drop(conn);
    assert_eq!(read_activation(&pool).await.unwrap(), 2);
}

#[tokio::test]
async fn runstate_activation_guard_allows_empty_tables_without_marker() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    // feature ≤3 不启用复制，不检查也不落标记（保持本地作业行为）。
    for configured in [1, 2, 3] {
        assert!(
            guard_runstate_activation(db.pool(), configured)
                .await
                .is_ok()
        );
        assert_eq!(db.schema_meta(META_RUNSTATE_ACTIVATED).await.unwrap(), None);
    }
    // 空表 feature 4：放行，但不得提前写标记（effective 可能仍是 3）。
    assert!(guard_runstate_activation(db.pool(), 4).await.is_ok());
    assert_eq!(db.schema_meta(META_RUNSTATE_ACTIVATED).await.unwrap(), None);
}

#[tokio::test]
async fn runstate_activation_guard_rejects_old_local_rows_without_marker() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    db.create_job(&crate::db::JobSpec::default(), 1)
        .await
        .unwrap();
    // feature 3 仍走本地作业路径：非空表不得误拒。
    assert!(guard_runstate_activation(db.pool(), 3).await.is_ok());
    assert_eq!(db.schema_meta(META_RUNSTATE_ACTIVATED).await.unwrap(), None);
    let err = guard_runstate_activation(db.pool(), 4).await.unwrap_err();
    assert!(
        err.contains("jobs=1") && err.contains("feature_level") && err.contains("清空"),
        "{err}"
    );
    // 显式 cutover/level-4 事实留下标记 → 非空表也放行，避免重启误拒。
    let mut conn = db.pool().acquire().await.unwrap();
    mark_runstate_activated_tx(&mut conn).await.unwrap();
    drop(conn);
    assert!(guard_runstate_activation(db.pool(), 4).await.is_ok());
}

#[tokio::test]
async fn runstate_activation_guard_rejects_dangling_job_refs() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    sqlx::query(
        "INSERT INTO inventory_runs \
             (bucket, config_id, run_key, started_at, state, job_id, created_at) \
             VALUES ('b', 'c', 'r', 1, 'done', 42, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let err = guard_runstate_activation(db.pool(), 4).await.unwrap_err();
    assert!(
        err.contains("inventory_runs=1")
            && err.contains("inventory_runs.job_id=1")
            && err.contains("清空"),
        "{err}"
    );
    sqlx::query("DELETE FROM inventory_runs")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(guard_runstate_activation(db.pool(), 4).await.is_ok());
}

#[tokio::test]
async fn runstate_activation_guard_counts_inventory_cursors_and_incrementals() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    sqlx::query(
        "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, updated_at) \
             VALUES ('b', 'c', 'x', 1, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let err = guard_runstate_activation(db.pool(), 4).await.unwrap_err();
    assert!(
        err.contains("inventory_cursors=1") && err.contains("inventory_incremental=0"),
        "{err}"
    );
    sqlx::query("DELETE FROM inventory_cursors")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
             VALUES ('b', 'c', 1, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let err = guard_runstate_activation(db.pool(), 4).await.unwrap_err();
    assert!(err.contains("inventory_incremental=1"), "{err}");
    sqlx::query("DELETE FROM inventory_incremental")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(guard_runstate_activation(db.pool(), 4).await.is_ok());
}

#[tokio::test]
async fn inventory_activation_guard_is_independent_of_runstate_marker() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    // feature ≤4：inventory 仍是节点本地，不检查也不落标记。
    for configured in [1, 2, 3, 4] {
        assert!(
            guard_inventory_activation(db.pool(), configured)
                .await
                .is_ok()
        );
        assert_eq!(
            db.schema_meta(META_INVENTORY_ACTIVATED).await.unwrap(),
            None
        );
    }
    sqlx::query(
        "INSERT INTO inventory_runs \
             (bucket, config_id, run_key, started_at, state, created_at) \
             VALUES ('b', 'c', 'r', 1, 'done', 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    // Job* 已置位的 level-4 marker 不能让 v1.23.0 的本地行绕过 level-5 守卫。
    let mut conn = db.pool().acquire().await.unwrap();
    mark_runstate_activated_tx(&mut conn).await.unwrap();
    drop(conn);
    let err = guard_inventory_activation(db.pool(), 5).await.unwrap_err();
    assert!(
        err.contains("inventory_runs=1") && err.contains("导出") && err.contains("next_run_id"),
        "{err}"
    );
    // 显式 level-5 事实写 marker 后放行（重启不误拒）。
    let mut conn = db.pool().acquire().await.unwrap();
    mark_inventory_activated_tx(&mut conn).await.unwrap();
    drop(conn);
    assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
    // marker 独立：撤销 inventory marker 不会碰 runstate marker，反之亦然。
    let mut conn = db.pool().acquire().await.unwrap();
    clear_inventory_activated_tx(&mut conn).await.unwrap();
    drop(conn);
    assert_eq!(
        db.schema_meta(META_INVENTORY_ACTIVATED).await.unwrap(),
        None
    );
    assert_eq!(
        db.schema_meta(META_RUNSTATE_ACTIVATED).await.unwrap(),
        Some("1".into())
    );
}

#[tokio::test]
async fn inventory_activation_guard_allows_empty_tables_and_checks_meta() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
    assert_eq!(
        db.schema_meta(META_INVENTORY_ACTIVATED).await.unwrap(),
        None
    );
    // 空表但 next_run_id 落后/非法仍 fail-closed，避免删除后复用 id。
    sqlx::query("INSERT INTO runstate_meta (key, value) VALUES ('next_run_id', '0')")
        .execute(db.pool())
        .await
        .unwrap();
    let err = guard_inventory_activation(db.pool(), 5).await.unwrap_err();
    assert!(err.contains("next_run_id"), "{err}");
    sqlx::query("UPDATE runstate_meta SET value = '1' WHERE key = 'next_run_id'")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
    // 只有 cursor/incremental 行也必须拒绝。
    sqlx::query(
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
             VALUES ('b', 'c', 1, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let err = guard_inventory_activation(db.pool(), 5).await.unwrap_err();
    assert!(err.contains("inventory_incremental=1"), "{err}");
}

#[tokio::test]
async fn first_feature_five_activation_clears_legacy_local_lifecycle_cursors() {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    sqlx::query(
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at) \
             VALUES ('b', 'legacy:', 'current', 'k', 9999999999)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_cursors")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(left, 0, "首次 feature-5 激活必须清空本地续扫游标");
    // 已激活后的重启不清日志复制来的游标。
    sqlx::query(
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at) \
             VALUES ('b', 'r', 'current', 'k2', 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let mut conn = db.pool().acquire().await.unwrap();
    mark_inventory_activated_tx(&mut conn).await.unwrap();
    drop(conn);
    assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
    let kept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_cursors")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(kept, 1, "已激活节点重启不得清空日志复制的游标");
}
