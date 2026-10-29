//! 运行态九表整表读写（M8-2a；模型见 [`super::model`]）。
//!
//! 读按主键排序（快照 checksum / 往返字节确定）；安装删除九表后按给定顺序插入，
//! 全程使用调用方事务——失败时由调用方回滚，绝不留下半套表。

use sqlx::{Row, SqliteConnection, SqlitePool};

use super::model::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, JobRow, JobTaskRow,
    LifecycleCursorRow, RunState, ScheduleLeaseRow, TierRestoreRow,
};
use crate::raft::data::DataError;

const META_NEXT_JOB_ID: &str = "next_job_id";
const META_NEXT_RUN_ID: &str = "next_run_id";

/// 九张运行态表（安装前整表清空；顺序即删除顺序）。
const RUNSTATE_TABLES: [&str; 9] = [
    "jobs",
    "job_tasks",
    "inventory_runs",
    "inventory_cursors",
    "inventory_incremental",
    "tier_restores",
    "lifecycle_cursors",
    "schedule_lease",
    "runstate_meta",
];

fn sql(e: sqlx::Error) -> DataError {
    DataError::Format(format!("runstate sql: {e}"))
}

async fn read_meta(conn: &mut SqliteConnection, key: &str) -> Result<i64, DataError> {
    Ok(
        sqlx::query_scalar("SELECT value FROM runstate_meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&mut *conn)
            .await
            .map_err(sql)?
            .unwrap_or(0),
    )
}

/// 整行 → [`JobRow`]（`job_state` 的定向查询复用同一映射；列集由查询保证）。
pub(crate) fn job_row_from(r: &sqlx::sqlite::SqliteRow) -> JobRow {
    JobRow {
        id: r.get("id"),
        kind: r.get("kind"),
        bucket: r.get("bucket"),
        prefix: r.get("prefix"),
        state: r.get("state"),
        scanned: r.get("scanned"),
        deleted: r.get("deleted"),
        skipped: r.get("skipped"),
        error: r.get("error"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
        spec_json: r.get("spec_json"),
        progress: r.get("progress"),
        total: r.get("total"),
        succeeded: r.get("succeeded"),
        failed: r.get("failed"),
        report_bucket: r.get("report_bucket"),
        report_prefix: r.get("report_prefix"),
        priority: r.get("priority"),
        requested_by: r.get("requested_by"),
        parent_job_id: r.get("parent_job_id"),
        started_at: r.get("started_at"),
        finished_at: r.get("finished_at"),
    }
}

/// 整行 → [`JobTaskRow`]（同上）。
pub(crate) fn job_task_row_from(r: &sqlx::sqlite::SqliteRow) -> JobTaskRow {
    JobTaskRow {
        job_id: r.get("job_id"),
        task_no: r.get("task_no"),
        action: r.get("action"),
        payload_json: r.get("payload_json"),
        state: r.get("state"),
        attempt: r.get("attempt"),
        next_at: r.get("next_at"),
        dedupe_key: r.get("dedupe_key"),
        deadline_at: r.get("deadline_at"),
        result_code: r.get("result_code"),
        result_string: r.get("result_string"),
        started_at: r.get("started_at"),
        finished_at: r.get("finished_at"),
        fence_term: r.get("fence_term"),
        fence_leader: r.get("fence_leader"),
    }
}

/// 调用方事务/连接内读取九表（行序 = 主键序，checksum 确定性来源）。
pub async fn read_runstate_tx(conn: &mut SqliteConnection) -> Result<RunState, DataError> {
    let mut state = RunState::default();

    for r in sqlx::query(
        "SELECT id, kind, bucket, prefix, state, scanned, deleted, skipped, error, \
                created_at, updated_at, spec_json, progress, total, succeeded, failed, \
                report_bucket, report_prefix, priority, requested_by, parent_job_id, \
                started_at, finished_at FROM jobs ORDER BY id",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        state.jobs.push(job_row_from(&r));
    }

    for r in sqlx::query(
        "SELECT job_id, task_no, action, payload_json, state, attempt, next_at, dedupe_key, \
                deadline_at, result_code, result_string, started_at, finished_at, \
                fence_term, fence_leader FROM job_tasks ORDER BY job_id, task_no",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        state.job_tasks.push(job_task_row_from(&r));
    }

    for r in sqlx::query(
        "SELECT id, bucket, config_id, run_key, started_at, state, job_id, manifest_key, \
                error, created_at, fence_term, fence_leader FROM inventory_runs ORDER BY id",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        state.inventory_runs.push(InventoryRunRow {
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
        });
    }

    for r in sqlx::query(
        "SELECT bucket, config_id, cursor, config_updated_at, updated_at, fence_term, \
                fence_leader FROM inventory_cursors ORDER BY bucket, config_id",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        state.inventory_cursors.push(InventoryCursorRow {
            bucket: r.get("bucket"),
            config_id: r.get("config_id"),
            cursor: r.get("cursor"),
            config_updated_at: r.get("config_updated_at"),
            updated_at: r.get("updated_at"),
            fence_term: r.get("fence_term"),
            fence_leader: r.get("fence_leader"),
        });
    }

    for r in sqlx::query(
        "SELECT bucket, config_id, enabled, updated_at FROM inventory_incremental \
         ORDER BY bucket, config_id",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        state.inventory_incremental.push(InventoryIncrementalRow {
            bucket: r.get("bucket"),
            config_id: r.get("config_id"),
            enabled: r.get::<i64, _>("enabled") != 0,
            updated_at: r.get("updated_at"),
        });
    }

    for r in sqlx::query(
        "SELECT bucket, key, version_id, expires_at, job_id, state, created_at, fence_term, \
                fence_leader FROM tier_restores ORDER BY bucket, key, version_id",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        state.tier_restores.push(TierRestoreRow {
            bucket: r.get("bucket"),
            key: r.get("key"),
            version_id: r.get("version_id"),
            expires_at: r.get("expires_at"),
            job_id: r.get("job_id"),
            state: r.get("state"),
            created_at: r.get("created_at"),
            fence_term: r.get("fence_term"),
            fence_leader: r.get("fence_leader"),
        });
    }

    for r in sqlx::query(
        "SELECT bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader \
         FROM lifecycle_cursors ORDER BY bucket, rule_id, stage",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        state.lifecycle_cursors.push(LifecycleCursorRow {
            bucket: r.get("bucket"),
            rule_id: r.get("rule_id"),
            stage: r.get("stage"),
            cursor: r.get("cursor"),
            updated_at: r.get("updated_at"),
            fence_term: r.get("fence_term"),
            fence_leader: r.get("fence_leader"),
        });
    }

    state.schedule_lease = sqlx::query(
        "SELECT term, node, expires_at, acquired_at FROM schedule_lease WHERE singleton = 1",
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(sql)?
    .map(|r| ScheduleLeaseRow {
        term: r.get("term"),
        node: r.get("node"),
        expires_at: r.get("expires_at"),
        acquired_at: r.get("acquired_at"),
    });

    state.next_job_id = read_meta(conn, META_NEXT_JOB_ID).await?;
    state.next_run_id = read_meta(conn, META_NEXT_RUN_ID).await?;
    // runstate_meta 可能尚未播种（M8-1 本地写入/旧库）：快照与导出的序列号必须
    // 高于现存行，否则安装侧 `validate` 拒绝非空运行态。按行补齐，格式不变。
    state.next_job_id = state
        .next_job_id
        .max(state.jobs.iter().map(|j| j.id + 1).max().unwrap_or(0));
    state.next_run_id = state.next_run_id.max(
        state
            .inventory_runs
            .iter()
            .map(|r| r.id + 1)
            .max()
            .unwrap_or(0),
    );
    if !state.is_empty() {
        state.next_job_id = state.next_job_id.max(1);
        state.next_run_id = state.next_run_id.max(1);
    }
    Ok(state)
}

/// 连接池便捷入口（校验/单次读）。快照或导出构建必须用 [`read_runstate_tx`] 和
/// 控制面同处一个读事务，避免撕裂读。
pub async fn read_runstate(pool: &SqlitePool) -> Result<RunState, DataError> {
    let mut conn = pool.acquire().await.map_err(sql)?;
    read_runstate_tx(&mut conn).await
}

/// 九表整表替换。先做语义校验（不通过则一行不写），再在 savepoint 内删除+插入：
/// 裸连接（自动提交）中途失败也只回滚本次安装；嵌套在调用方事务里同样安全。
pub async fn install_runstate_tx(
    conn: &mut SqliteConnection,
    state: &RunState,
) -> Result<(), DataError> {
    state.validate()?;
    sqlx::query("SAVEPOINT install_runstate")
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    match write_rows(conn, state).await {
        Ok(()) => {
            sqlx::query("RELEASE SAVEPOINT install_runstate")
                .execute(&mut *conn)
                .await
                .map_err(sql)?;
            Ok(())
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK TO SAVEPOINT install_runstate")
                .execute(&mut *conn)
                .await;
            let _ = sqlx::query("RELEASE SAVEPOINT install_runstate")
                .execute(&mut *conn)
                .await;
            Err(e)
        }
    }
}

async fn write_rows(conn: &mut SqliteConnection, state: &RunState) -> Result<(), DataError> {
    for table in RUNSTATE_TABLES {
        sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {table}")))
            .execute(&mut *conn)
            .await
            .map_err(sql)?;
    }

    // 显式 id 插入会把 AUTOINCREMENT 的 sqlite_sequence 追平到 max(id)，之后本地
    // 自增不会再分配到快照里已存在的 id。
    for j in &state.jobs {
        sqlx::query(
            "INSERT INTO jobs (id, kind, bucket, prefix, state, scanned, deleted, skipped, \
              error, created_at, updated_at, spec_json, progress, total, succeeded, failed, \
              report_bucket, report_prefix, priority, requested_by, parent_job_id, \
              started_at, finished_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(j.id)
        .bind(&j.kind)
        .bind(&j.bucket)
        .bind(&j.prefix)
        .bind(&j.state)
        .bind(j.scanned)
        .bind(j.deleted)
        .bind(j.skipped)
        .bind(&j.error)
        .bind(j.created_at)
        .bind(j.updated_at)
        .bind(&j.spec_json)
        .bind(j.progress)
        .bind(j.total)
        .bind(j.succeeded)
        .bind(j.failed)
        .bind(&j.report_bucket)
        .bind(&j.report_prefix)
        .bind(j.priority)
        .bind(j.requested_by)
        .bind(j.parent_job_id)
        .bind(j.started_at)
        .bind(j.finished_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    for t in &state.job_tasks {
        sqlx::query(
            "INSERT INTO job_tasks (job_id, task_no, action, payload_json, state, attempt, \
              next_at, dedupe_key, deadline_at, result_code, result_string, started_at, \
              finished_at, fence_term, fence_leader) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(t.job_id)
        .bind(t.task_no)
        .bind(&t.action)
        .bind(&t.payload_json)
        .bind(&t.state)
        .bind(t.attempt)
        .bind(t.next_at)
        .bind(&t.dedupe_key)
        .bind(t.deadline_at)
        .bind(&t.result_code)
        .bind(&t.result_string)
        .bind(t.started_at)
        .bind(t.finished_at)
        .bind(t.fence_term)
        .bind(t.fence_leader)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    for run in &state.inventory_runs {
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
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    for c in &state.inventory_cursors {
        sqlx::query(
            "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, \
              updated_at, fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&c.bucket)
        .bind(&c.config_id)
        .bind(&c.cursor)
        .bind(c.config_updated_at)
        .bind(c.updated_at)
        .bind(c.fence_term)
        .bind(c.fence_leader)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    for i in &state.inventory_incremental {
        sqlx::query(
            "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(&i.bucket)
        .bind(&i.config_id)
        .bind(i.enabled)
        .bind(i.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    for t in &state.tier_restores {
        sqlx::query(
            "INSERT INTO tier_restores (bucket, key, version_id, expires_at, job_id, state, \
              created_at, fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&t.bucket)
        .bind(&t.key)
        .bind(&t.version_id)
        .bind(t.expires_at)
        .bind(t.job_id)
        .bind(&t.state)
        .bind(t.created_at)
        .bind(t.fence_term)
        .bind(t.fence_leader)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    for c in &state.lifecycle_cursors {
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
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    if let Some(lease) = &state.schedule_lease {
        sqlx::query(
            "INSERT INTO schedule_lease (singleton, term, node, expires_at, acquired_at) \
             VALUES (1, ?, ?, ?, ?)",
        )
        .bind(lease.term)
        .bind(lease.node)
        .bind(lease.expires_at)
        .bind(lease.acquired_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    // 0 = 未初始化：保持 runstate_meta 空，读取时回落到默认 0。
    for (key, value) in [
        (META_NEXT_JOB_ID, state.next_job_id),
        (META_NEXT_RUN_ID, state.next_run_id),
    ] {
        if value > 0 {
            sqlx::query("INSERT INTO runstate_meta (key, value) VALUES (?, ?)")
                .bind(key)
                .bind(value)
                .execute(&mut *conn)
                .await
                .map_err(sql)?;
        }
    }
    Ok(())
}
