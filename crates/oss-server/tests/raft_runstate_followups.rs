//! M8 Task 2 复审跟进：扩展版本单一权威、语义校验、裸连接安装原子性、行结构覆盖。
//!
//! 这些约束先于 level-3 激活生效：任何不一致的运行态必须在 DELETE/INSERT 之前拒绝，
//! 中途 SQL 失败必须回滚九表，扩展条目版本必须与载荷内版本一致。

use serde_json::json;
use sqlx::SqlitePool;

use oss_server::db::Db;
use oss_server::raft::data::DataError;
use oss_server::raft::runstate::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, JobRow, JobTaskRow,
    LifecycleCursorRow, RUNSTATE_EXTENSION_VERSION, RunState, ScheduleLeaseRow, TierRestoreRow,
    decode_extension, from_snapshot_extension, install_runstate_tx, read_runstate,
    to_snapshot_extension,
};

fn db_url(dir: &tempfile::TempDir) -> String {
    format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("control.db").display()
    )
}

async fn fresh_db() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::connect_raft(&db_url(&dir)).await.unwrap();
    (dir, db)
}

/// 每张表一行、引用闭合的最小完整运行态。
fn sample() -> RunState {
    RunState {
        jobs: vec![JobRow {
            id: 7,
            kind: "batch".into(),
            created_at: 10,
            updated_at: 11,
            parent_job_id: None,
            ..JobRow::default()
        }],
        job_tasks: vec![JobTaskRow {
            job_id: 7,
            task_no: 1,
            action: "copy".into(),
            payload_json: "{}".into(),
            ..JobTaskRow::default()
        }],
        inventory_runs: vec![InventoryRunRow {
            id: 42,
            bucket: "b1".into(),
            config_id: "cfg".into(),
            run_key: "r1".into(),
            job_id: Some(7),
            ..InventoryRunRow::default()
        }],
        inventory_cursors: vec![InventoryCursorRow {
            bucket: "b1".into(),
            config_id: "cfg".into(),
            cursor: "k".into(),
            ..InventoryCursorRow::default()
        }],
        inventory_incremental: vec![InventoryIncrementalRow {
            bucket: "b1".into(),
            config_id: "cfg".into(),
            enabled: true,
            ..InventoryIncrementalRow::default()
        }],
        tier_restores: vec![TierRestoreRow {
            bucket: "b1".into(),
            key: "cold/k".into(),
            version_id: "v1".into(),
            job_id: Some(7),
            ..TierRestoreRow::default()
        }],
        lifecycle_cursors: vec![LifecycleCursorRow {
            bucket: "b1".into(),
            rule_id: "r".into(),
            stage: "expire".into(),
            cursor: "k".into(),
            ..LifecycleCursorRow::default()
        }],
        schedule_lease: Some(ScheduleLeaseRow {
            term: 1,
            node: 2,
            expires_at: 100,
            acquired_at: 90,
        }),
        next_job_id: 8,
        next_run_id: 43,
    }
}

#[test]
fn snapshot_extension_and_payload_versions_must_agree() {
    let rs = sample();
    let ext = to_snapshot_extension(&rs).unwrap();
    assert_eq!(ext.version, RUNSTATE_EXTENSION_VERSION);
    assert_eq!(
        from_snapshot_extension(&ext, RUNSTATE_EXTENSION_VERSION).unwrap(),
        rs
    );

    let mut bad = ext.clone();
    bad.version = 2;
    let err = from_snapshot_extension(&bad, 2).unwrap_err();
    assert!(
        matches!(&err, DataError::Format(m) if m.contains("version mismatch")),
        "{err}"
    );

    let mut garbage = ext;
    garbage.payload_b64 = "!!!".into();
    assert!(from_snapshot_extension(&garbage, 1).is_err());
}

#[tokio::test]
async fn invalid_runstate_is_rejected_before_any_write() {
    let (_dir, db) = fresh_db().await;
    let good = sample();
    let mut tx = db.pool().begin().await.unwrap();
    install_runstate_tx(&mut tx, &good).await.unwrap();
    tx.commit().await.unwrap();

    let mut cases: Vec<(&str, RunState)> = Vec::new();
    let mut s = good.clone();
    s.next_job_id = 7;
    cases.push(("next_job_id", s));
    let mut s = good.clone();
    s.next_job_id = 0;
    cases.push(("non-empty runstate", s));
    let mut s = good.clone();
    s.next_run_id = 42;
    cases.push(("next_run_id", s));

    let mut s = good.clone();
    s.next_job_id = -1;
    cases.push(("positive", s));

    // M4：parent_job_id 悬空。
    let mut s = good.clone();
    s.jobs[0].parent_job_id = Some(999);
    cases.push(("parent_job_id", s));

    // 清空 jobs 后，三类作业引用分别悬空；各自先清掉更早的悬空表以隔离报错。
    let mut s = good.clone();
    s.jobs.clear();
    cases.push(("job_tasks", s));
    let mut s = good.clone();
    s.jobs.clear();
    s.job_tasks.clear();
    cases.push(("inventory_runs", s));
    let mut s = good.clone();
    s.jobs.clear();
    s.job_tasks.clear();
    s.inventory_runs[0].job_id = None;
    cases.push(("tier_restores", s));

    for (want, bad) in cases {
        let mut conn = db.pool().acquire().await.unwrap();
        let err = install_runstate_tx(&mut conn, &bad).await.unwrap_err();
        assert!(
            format!("{err}").contains(want),
            "{want}: expected rejection mentioning {want}, got {err}"
        );
        drop(conn);
        assert_eq!(
            read_runstate(db.pool()).await.unwrap(),
            good,
            "{want}: rejected install must not delete or replace rows"
        );
    }
}

#[tokio::test]
async fn bare_connection_mid_install_failure_rolls_back_all_nine_tables() {
    let (_dir, db) = fresh_db().await;
    let good = sample();
    let mut tx = db.pool().begin().await.unwrap();
    install_runstate_tx(&mut tx, &good).await.unwrap();
    tx.commit().await.unwrap();

    // 语义合法但主键重复：DELETE 已执行后才在 INSERT 失败，必须由 savepoint 整体回滚。
    let mut bad = sample();
    bad.job_tasks.push(sample().job_tasks.remove(0));
    let mut conn = db.pool().acquire().await.unwrap();
    assert!(install_runstate_tx(&mut conn, &bad).await.is_err());
    drop(conn);

    assert_eq!(
        read_runstate(db.pool()).await.unwrap(),
        good,
        "bare pooled connection must not leave the nine tables wiped"
    );
}

#[test]
fn every_row_struct_rejects_unknown_fields() {
    let state = serde_json::to_value(sample()).unwrap();
    for table in [
        "jobs",
        "job_tasks",
        "inventory_runs",
        "inventory_cursors",
        "inventory_incremental",
        "tier_restores",
        "lifecycle_cursors",
    ] {
        let mut value = state.clone();
        value[table][0]["future_column"] = json!(1);
        let bytes = serde_json::to_vec(&json!({ "version": 1, "runstate": value })).unwrap();
        assert!(
            matches!(decode_extension(&bytes, 1), Err(DataError::Json(_))),
            "{table} accepted an unknown field"
        );
    }
    let mut value = state;
    value["schedule_lease"]["future_column"] = json!(1);
    let bytes = serde_json::to_vec(&json!({ "version": 1, "runstate": value })).unwrap();
    assert!(matches!(
        decode_extension(&bytes, 1),
        Err(DataError::Json(_))
    ));
}

async fn count_where(pool: &SqlitePool, table: &str, cond: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {table} WHERE {cond}"
    )))
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn read_orders_every_table_by_primary_key() {
    let (_dir, db) = fresh_db().await;
    for sql in [
        "INSERT INTO jobs (id, kind, created_at, updated_at) \
         VALUES (9, 'batch', 1, 1), (3, 'batch', 1, 1)",
        "INSERT INTO job_tasks (job_id, task_no, action, payload_json) \
         VALUES (9, 2, 'copy', '{}'), (9, 1, 'copy', '{}'), (3, 1, 'copy', '{}')",
        "INSERT INTO inventory_runs (id, bucket, config_id, run_key, started_at, state, created_at) \
         VALUES (9, 'b', 'c', 'r', 1, 's', 1), (3, 'a', 'c', 'r', 1, 's', 1)",
        "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, updated_at) \
         VALUES ('b', 'c', 'k', 1, 1), ('a', 'c', 'k', 1, 1)",
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
         VALUES ('b', 'c', 1, 1), ('a', 'c', 0, 1)",
        "INSERT INTO tier_restores (bucket, key, version_id, created_at) \
         VALUES ('b', 'z', 'v', 1), ('a', 'y', 'v', 1)",
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at) \
         VALUES ('b', 'r', 'expire', 'k', 1), ('a', 'r', 'expire', 'k', 1)",
    ] {
        sqlx::raw_sql(sql).execute(db.pool()).await.unwrap();
    }
    let rs = read_runstate(db.pool()).await.unwrap();
    assert_eq!(rs.jobs.iter().map(|r| r.id).collect::<Vec<_>>(), [3, 9]);
    assert_eq!(
        rs.job_tasks
            .iter()
            .map(|r| (r.job_id, r.task_no))
            .collect::<Vec<_>>(),
        [(3, 1), (9, 1), (9, 2)]
    );
    assert_eq!(
        rs.inventory_runs.iter().map(|r| r.id).collect::<Vec<_>>(),
        [3, 9]
    );
    assert_eq!(
        rs.inventory_cursors
            .iter()
            .map(|r| r.bucket.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(
        rs.inventory_incremental
            .iter()
            .map(|r| r.bucket.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(
        rs.tier_restores
            .iter()
            .map(|r| r.key.as_str())
            .collect::<Vec<_>>(),
        ["y", "z"]
    );
    assert_eq!(
        rs.lifecycle_cursors
            .iter()
            .map(|r| r.bucket.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
}

/// 显式 id 插入会把 AUTOINCREMENT 的 `sqlite_sequence` 追平到 max(id)，
/// 因此安装后的本地自增不会与快照序列冲突。
#[tokio::test]
async fn explicit_ids_advance_sqlite_sequence() {
    let (_dir, db) = fresh_db().await;
    let mut tx = db.pool().begin().await.unwrap();
    install_runstate_tx(&mut tx, &sample()).await.unwrap();
    tx.commit().await.unwrap();

    let inserted =
        sqlx::query("INSERT INTO jobs (kind, created_at, updated_at) VALUES ('local', 1, 1)")
            .execute(db.pool())
            .await
            .unwrap();
    assert_eq!(
        inserted.last_insert_rowid(),
        8,
        "AUTOINCREMENT must continue after imported id 7"
    );
    assert_eq!(
        count_where(db.pool(), "jobs", "id = 8").await,
        1,
        "sequence row sanity"
    );
    let seq: Option<i64> =
        sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'jobs'")
            .fetch_optional(db.pool())
            .await
            .unwrap();
    assert!(seq.is_some_and(|s| s >= 8));
}
