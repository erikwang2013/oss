//! M8-2a：运行态（jobs/inventory/restore/lifecycle）的 `m8.runstate` 扩展与整表安装。
//!
//! 覆盖：扩展编解码往返与确定性、未知版本/未知字段/超 64 MiB 拒绝、非空运行态要求
//! feature 3、九个运行态表在同一调用方事务内整表替换（失败回滚不破坏旧状态）、
//! 行结构体与真实 SQLite 列一一对应、读取按主键确定序。

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::json;
use sqlx::Row;
use sqlx::SqlitePool;

use oss_server::db::Db;
use oss_server::raft::data::{
    ControlData, DataError, SnapshotExtension, SnapshotExtensions, decode_snapshot,
    encode_snapshot_with_extensions,
};
use oss_server::raft::runstate::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, JobRow, JobTaskRow,
    LifecycleCursorRow, RUNSTATE_EXTENSION, RUNSTATE_EXTENSION_VERSION, RUNSTATE_MAX_BYTES,
    RunState, ScheduleLeaseRow, TierRestoreRow, decode_extension, encode_extension,
    install_runstate_tx, read_runstate,
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

async fn count(pool: &SqlitePool, table: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn columns(pool: &SqlitePool, table: &str) -> Vec<String> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT name FROM pragma_table_info('{table}')"
    )))
    .fetch_all(pool)
    .await
    .unwrap()
    .iter()
    .map(|r| r.get::<String, _>("name"))
    .collect()
}

/// 全字段非默认的行：任何列被漏掉都会在往返/列覆盖测试里暴露。
fn sample_job(id: i64) -> JobRow {
    JobRow {
        id,
        kind: "batch".into(),
        bucket: "b1".into(),
        prefix: "src/".into(),
        state: "queued".into(),
        scanned: 1,
        deleted: 2,
        skipped: 3,
        error: Some("boom".into()),
        created_at: 10,
        updated_at: 11,
        spec_json: Some(r#"{"op":"copy"}"#.into()),
        progress: 4,
        total: 5,
        succeeded: 6,
        failed: 7,
        report_bucket: Some("rb".into()),
        report_prefix: Some("rp/".into()),
        priority: 2,
        requested_by: Some(3),
        parent_job_id: Some(4),
        started_at: Some(12),
        finished_at: Some(13),
    }
}

fn sample_task(job_id: i64) -> JobTaskRow {
    JobTaskRow {
        job_id,
        task_no: 1,
        action: "copy".into(),
        payload_json: r#"{"src":"a","dst":"b"}"#.into(),
        state: "running".into(),
        attempt: 2,
        next_at: 20,
        dedupe_key: Some("d1".into()),
        deadline_at: Some(21),
        result_code: Some("OK".into()),
        result_string: Some("done".into()),
        started_at: Some(22),
        finished_at: Some(23),
        fence_term: 5,
        fence_leader: 2,
    }
}

fn sample_run(id: i64) -> InventoryRunRow {
    InventoryRunRow {
        id,
        bucket: "b1".into(),
        config_id: "cfg".into(),
        run_key: "2026-10-25".into(),
        started_at: 30,
        state: "running".into(),
        job_id: Some(7),
        manifest_key: Some("m.json".into()),
        error: None,
        created_at: 31,
        fence_term: 5,
        fence_leader: 2,
    }
}

fn sample() -> RunState {
    RunState {
        jobs: vec![sample_job(7)],
        job_tasks: vec![sample_task(7)],
        inventory_runs: vec![sample_run(42)],
        inventory_cursors: vec![InventoryCursorRow {
            bucket: "b1".into(),
            config_id: "cfg".into(),
            cursor: "k900".into(),
            config_updated_at: 40,
            updated_at: 41,
            fence_term: 5,
            fence_leader: 2,
        }],
        inventory_incremental: vec![InventoryIncrementalRow {
            bucket: "b1".into(),
            config_id: "cfg".into(),
            enabled: true,
            updated_at: 42,
        }],
        tier_restores: vec![TierRestoreRow {
            bucket: "b1".into(),
            key: "cold/k".into(),
            version_id: "v1".into(),
            expires_at: 50,
            job_id: Some(7),
            state: "pending".into(),
            created_at: 43,
            fence_term: 5,
            fence_leader: 2,
        }],
        lifecycle_cursors: vec![LifecycleCursorRow {
            bucket: "b1".into(),
            rule_id: "rule".into(),
            stage: "expire".into(),
            cursor: "k9".into(),
            updated_at: 44,
            fence_term: 5,
            fence_leader: 2,
        }],
        schedule_lease: Some(ScheduleLeaseRow {
            term: 5,
            node: 2,
            expires_at: 100,
            acquired_at: 90,
        }),
        next_job_id: 8,
        next_run_id: 43,
    }
}

fn envelope(version: u32, runstate: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({ "version": version, "runstate": runstate })).unwrap()
}

/// 往返确定性 + 非空运行态要求 feature 3（level-2 封套不得携带它）。
#[test]
fn extension_round_trip_is_deterministic_and_requires_feature_three() {
    let rs = sample();
    let bytes = encode_extension(&rs).unwrap();
    assert_eq!(
        decode_extension(&bytes, RUNSTATE_EXTENSION_VERSION).unwrap(),
        rs
    );
    assert_eq!(
        encode_extension(&rs).unwrap(),
        bytes,
        "编码必须确定（checksum 稳定）"
    );

    assert!(RunState::default().is_empty());
    assert_eq!(RunState::default().required_feature_level(), 1);
    assert!(!rs.is_empty());
    assert_eq!(rs.required_feature_level(), 3);

    let mut exts = SnapshotExtensions::new();
    exts.insert(
        RUNSTATE_EXTENSION.into(),
        SnapshotExtension {
            version: RUNSTATE_EXTENSION_VERSION,
            payload_b64: BASE64.encode(&bytes),
        },
    );
    assert!(
        encode_snapshot_with_extensions(&ControlData::default(), 2, exts.clone()).is_err(),
        "level 2 封套不得携带 m8.runstate"
    );
    let snap = encode_snapshot_with_extensions(&ControlData::default(), 3, exts).unwrap();
    let decoded = decode_snapshot(&snap).unwrap();
    let ext = &decoded.extensions[RUNSTATE_EXTENSION];
    let payload = BASE64.decode(&ext.payload_b64).unwrap();
    assert_eq!(
        decode_extension(&payload, ext.version).unwrap(),
        rs,
        "经 OSSCTL3 封套往返后运行态一致"
    );
}

/// 未知/零版本拒绝（版本门在本二进制支持的版本处关闭）。
#[test]
fn unknown_extension_version_is_rejected() {
    let state = serde_json::to_value(sample()).unwrap();
    for version in [0u32, 2] {
        let err = decode_extension(
            &envelope(version, state.clone()),
            RUNSTATE_EXTENSION_VERSION,
        )
        .unwrap_err();
        assert!(
            matches!(&err, DataError::Format(m) if m.contains("version")),
            "{version}: {err}"
        );
    }
    // 声明更高上限的读者可按自身能力接受该版本（未来二进制路径）。
    assert_eq!(decode_extension(&envelope(2, state), 2).unwrap(), sample());
}

/// 信封、RunState、行结构体三层的未知字段都必须整包拒绝。
#[test]
fn unknown_envelope_runstate_and_row_fields_are_rejected() {
    let state = serde_json::to_value(sample()).unwrap();

    let mut bad_envelope = json!({ "version": 1, "runstate": state });
    bad_envelope["future"] = json!(true);
    assert!(matches!(
        decode_extension(
            &serde_json::to_vec(&bad_envelope).unwrap(),
            RUNSTATE_EXTENSION_VERSION
        ),
        Err(DataError::Json(_))
    ));

    let mut bad_state = state.clone();
    bad_state["future_table"] = json!([]);
    assert!(matches!(
        decode_extension(&envelope(1, bad_state), RUNSTATE_EXTENSION_VERSION),
        Err(DataError::Json(_))
    ));

    let mut bad_row = state;
    bad_row["jobs"][0]["future_column"] = json!(1);
    assert!(matches!(
        decode_extension(&envelope(1, bad_row), RUNSTATE_EXTENSION_VERSION),
        Err(DataError::Json(_))
    ));
}

/// 64 MiB 是硬上限：解码先看长度（不做 JSON 放大），编码超限报错而不是截断。
#[test]
fn oversize_extension_is_rejected_on_encode_and_decode() {
    let err = decode_extension(&vec![b'x'; RUNSTATE_MAX_BYTES + 1], 1).unwrap_err();
    assert!(
        matches!(&err, DataError::Format(m) if m.contains("cap")),
        "{err}"
    );

    let mut big = sample();
    big.jobs[0].error = Some("a".repeat(RUNSTATE_MAX_BYTES));
    let err = encode_extension(&big).unwrap_err();
    assert!(
        matches!(&err, DataError::Format(m) if m.contains("cap")),
        "{err}"
    );
}

/// 九个运行态表（7 张运行表 + schedule_lease + runstate_meta）被整表替换。
#[tokio::test]
async fn install_replaces_all_nine_tables_in_caller_transaction() {
    let (_dir, db) = fresh_db().await;
    seed_junk(db.pool()).await;

    let rs = sample();
    let mut tx = db.pool().begin().await.unwrap();
    install_runstate_tx(&mut tx, &rs).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(read_runstate(db.pool()).await.unwrap(), rs);
    for (table, want) in [
        ("jobs", 1),
        ("job_tasks", 1),
        ("inventory_runs", 1),
        ("inventory_cursors", 1),
        ("inventory_incremental", 1),
        ("tier_restores", 1),
        ("lifecycle_cursors", 1),
        ("schedule_lease", 1),
        ("runstate_meta", 2),
    ] {
        assert_eq!(count(db.pool(), table).await, want, "{table}");
    }
    assert_eq!(
        count_where(db.pool(), "jobs", "kind = 'junk'").await,
        0,
        "旧行必须被替换"
    );

    // 未初始化（next ids = 0）同样清空九表：runstate_meta 不留 0 值行。
    let mut tx = db.pool().begin().await.unwrap();
    install_runstate_tx(&mut tx, &RunState::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(read_runstate(db.pool()).await.unwrap(), RunState::default());
    assert_eq!(count(db.pool(), "runstate_meta").await, 0);
    assert_eq!(count(db.pool(), "schedule_lease").await, 0);
}

/// 安装中途失败时，调用方回滚后旧状态完好（删除+插入在同一事务里）。
#[tokio::test]
async fn failed_install_rolls_back_without_touching_previous_state() {
    let (_dir, db) = fresh_db().await;
    let rs = sample();
    let mut tx = db.pool().begin().await.unwrap();
    install_runstate_tx(&mut tx, &rs).await.unwrap();
    tx.commit().await.unwrap();

    let mut bad = sample();
    bad.job_tasks.push(sample_task(7)); // 主键 (job_id, task_no) 重复
    let mut tx = db.pool().begin().await.unwrap();
    assert!(install_runstate_tx(&mut tx, &bad).await.is_err());
    tx.rollback().await.unwrap();

    assert_eq!(
        read_runstate(db.pool()).await.unwrap(),
        rs,
        "失败安装不得留下半套表"
    );
}

/// 行结构体必须覆盖每张表的真实列；漂移（漏列/多列）在此暴露。
#[tokio::test]
async fn row_structs_cover_every_runtime_column() {
    let (_dir, db) = fresh_db().await;
    let cases: [(&str, serde_json::Value); 8] = [
        ("jobs", serde_json::to_value(JobRow::default()).unwrap()),
        (
            "job_tasks",
            serde_json::to_value(JobTaskRow::default()).unwrap(),
        ),
        (
            "inventory_runs",
            serde_json::to_value(InventoryRunRow::default()).unwrap(),
        ),
        (
            "inventory_cursors",
            serde_json::to_value(InventoryCursorRow::default()).unwrap(),
        ),
        (
            "inventory_incremental",
            serde_json::to_value(InventoryIncrementalRow::default()).unwrap(),
        ),
        (
            "tier_restores",
            serde_json::to_value(TierRestoreRow::default()).unwrap(),
        ),
        (
            "lifecycle_cursors",
            serde_json::to_value(LifecycleCursorRow::default()).unwrap(),
        ),
        (
            "schedule_lease",
            serde_json::to_value(ScheduleLeaseRow::default()).unwrap(),
        ),
    ];
    for (table, value) in cases {
        let mut fields: Vec<String> = value
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut cols = columns(db.pool(), table).await;
        if table == "schedule_lease" {
            // 单例行由安装代码固定写 singleton=1，不是数据字段。
            cols.retain(|c| c != "singleton");
        }
        fields.sort();
        cols.sort();
        assert_eq!(fields, cols, "{table} 行结构体与真实列不一致");
    }
    let mut meta_cols = columns(db.pool(), "runstate_meta").await;
    meta_cols.sort();
    assert_eq!(meta_cols, ["key", "value"]);
}

/// 读取按主键排序，保证快照 checksum / 往返字节确定。
#[tokio::test]
async fn read_returns_rows_in_primary_key_order() {
    let (_dir, db) = fresh_db().await;
    for id in [9, 3] {
        sqlx::query("INSERT INTO jobs (id, kind, created_at, updated_at) VALUES (?, 'k', 1, 1)")
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    for (job, task) in [(9, 2), (3, 1)] {
        sqlx::query(
            "INSERT INTO job_tasks (job_id, task_no, action, payload_json) VALUES (?, ?, 'a', '{}')",
        )
        .bind(job)
        .bind(task)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let rs = read_runstate(db.pool()).await.unwrap();
    assert_eq!(
        rs.jobs.iter().map(|j| j.id).collect::<Vec<_>>(),
        [3, 9],
        "jobs 按 id"
    );
    assert_eq!(
        rs.job_tasks
            .iter()
            .map(|t| (t.job_id, t.task_no))
            .collect::<Vec<_>>(),
        [(3, 1), (9, 2)],
        "job_tasks 按 (job_id, task_no)"
    );
}

async fn count_where(pool: &SqlitePool, table: &str, cond: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {table} WHERE {cond}"
    )))
    .fetch_one(pool)
    .await
    .unwrap()
}

/// 九张表各插一行可识别的旧数据（列全用默认值，只填必填列）。
async fn seed_junk(pool: &SqlitePool) {
    for sql in [
        "INSERT INTO jobs (id, kind, created_at, updated_at) VALUES (99, 'junk', 1, 1)",
        "INSERT INTO job_tasks (job_id, task_no, action, payload_json) VALUES (99, 99, 'j', '{}')",
        "INSERT INTO inventory_runs (bucket, config_id, run_key, started_at, state, created_at) \
         VALUES ('junk', 'junk', 'junk', 1, 'junk', 1)",
        "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, updated_at) \
         VALUES ('junk', 'junk', 'junk', 1, 1)",
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
         VALUES ('junk', 'junk', 1, 1)",
        "INSERT INTO tier_restores (bucket, key, version_id, created_at) \
         VALUES ('junk', 'junk', 'junk', 1)",
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at) \
         VALUES ('junk', 'junk', 'junk', 'junk', 1)",
        "INSERT INTO schedule_lease (singleton, term, node, expires_at, acquired_at) \
         VALUES (1, 99, 99, 99, 99)",
        "INSERT INTO runstate_meta (key, value) VALUES ('junk', 99)",
    ] {
        sqlx::raw_sql(sql).execute(pool).await.unwrap();
    }
}
