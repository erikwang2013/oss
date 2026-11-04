//! jobs / job_tasks / schedule_lease / runstate_meta 的 SQLite 只读实现。
//!
//! 列集、排序、绑定类型与迁移前 `db.rs` / `job_state` 的查询逐字一致；映射函数
//! 集中在文件顶部，供写适配器复用。

use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};
use xl_store::control_store::ControlError;
use xl_store::control_store::jobs::{JobRow, JobStore, JobTaskRow, ScheduleLeaseRow};

use super::SqliteControlStore;

pub(super) fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

pub(super) fn job_row(r: &SqliteRow) -> JobRow {
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

pub(super) fn task_row(r: &SqliteRow) -> JobTaskRow {
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

pub(super) fn lease_row(r: &SqliteRow) -> ScheduleLeaseRow {
    ScheduleLeaseRow {
        term: r.get("term"),
        node: r.get("node"),
        expires_at: r.get("expires_at"),
        acquired_at: r.get("acquired_at"),
    }
}

pub(super) async fn job_conn(
    conn: &mut sqlx::SqliteConnection,
    id: i64,
) -> Result<Option<JobRow>, ControlError> {
    let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(row.as_ref().map(job_row))
}

pub(super) async fn task_conn(
    conn: &mut sqlx::SqliteConnection,
    job_id: i64,
    task_no: i64,
) -> Result<Option<JobTaskRow>, ControlError> {
    let row = sqlx::query("SELECT * FROM job_tasks WHERE job_id = ? AND task_no = ?")
        .bind(job_id)
        .bind(task_no)
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(row.as_ref().map(task_row))
}

pub(super) async fn dedupe_taken_conn(
    conn: &mut sqlx::SqliteConnection,
    key: &str,
    job_id: i64,
    task_no: i64,
) -> Result<bool, ControlError> {
    let row: Option<(i64, i64)> =
        sqlx::query_as("SELECT job_id, task_no FROM job_tasks WHERE dedupe_key = ?")
            .bind(key)
            .fetch_optional(&mut *conn)
            .await
            .map_err(backend)?;
    Ok(row.is_some_and(|(j, t)| j != job_id || t != task_no))
}

async fn jobs(pool: &SqlitePool, sql: &str, job_id: i64) -> Result<Vec<JobRow>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
        .bind(job_id)
        .fetch_all(pool)
        .await
        .map_err(backend)?;
    Ok(rows.iter().map(job_row).collect())
}

async fn dedupe_taken_pool(
    pool: &SqlitePool,
    key: &str,
    job_id: i64,
    task_no: i64,
) -> Result<bool, ControlError> {
    let row: Option<(i64, i64)> =
        sqlx::query_as("SELECT job_id, task_no FROM job_tasks WHERE dedupe_key = ?")
            .bind(key)
            .fetch_optional(pool)
            .await
            .map_err(backend)?;
    Ok(row.is_some_and(|(j, t)| j != job_id || t != task_no))
}

pub(super) async fn job(
    store: &SqliteControlStore,
    id: i64,
) -> Result<Option<JobRow>, ControlError> {
    let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?;
    Ok(row.as_ref().map(job_row))
}

pub(super) async fn job_task(
    store: &SqliteControlStore,
    job_id: i64,
    task_no: i64,
) -> Result<Option<JobTaskRow>, ControlError> {
    let row = sqlx::query("SELECT * FROM job_tasks WHERE job_id = ? AND task_no = ?")
        .bind(job_id)
        .bind(task_no)
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?;
    Ok(row.as_ref().map(task_row))
}

pub(super) async fn job_tasks(
    store: &SqliteControlStore,
    job_id: i64,
) -> Result<Vec<JobTaskRow>, ControlError> {
    let rows = sqlx::query("SELECT * FROM job_tasks WHERE job_id = ? ORDER BY task_no")
        .bind(job_id)
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?;
    Ok(rows.iter().map(task_row).collect())
}

pub(super) async fn failed_job_tasks(
    store: &SqliteControlStore,
    job_id: i64,
) -> Result<Vec<JobTaskRow>, ControlError> {
    let rows = sqlx::query(
        "SELECT * FROM job_tasks WHERE job_id = ? AND state = 'failed' ORDER BY task_no",
    )
    .bind(job_id)
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(task_row).collect())
}

pub(super) async fn queued_task_candidate(
    store: &SqliteControlStore,
    now: i64,
    fence_term: i64,
    queued_jobs_allowed: bool,
) -> Result<Option<JobTaskRow>, ControlError> {
    let job_state = if queued_jobs_allowed {
        "j.state IN ('queued','running')"
    } else {
        "j.state = 'running'"
    };
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT t.* FROM job_tasks t JOIN jobs j ON j.id = t.job_id \
         WHERE t.state = 'queued' AND t.next_at <= ? AND {job_state} \
           AND t.fence_term <= ? \
         ORDER BY j.priority DESC, t.next_at, t.task_no LIMIT 1"
    )))
    .bind(now)
    .bind(fence_term)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(task_row))
}

pub(super) async fn active_task_count(
    store: &SqliteControlStore,
    job_id: i64,
    exclude: Option<(i64, i64)>,
) -> Result<i64, ControlError> {
    let n = match exclude {
        Some((_, task_no)) => sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_tasks WHERE job_id = ? AND task_no <> ? \
             AND state IN ('queued','running')",
        )
        .bind(job_id)
        .bind(task_no)
        .fetch_one(&store.pool)
        .await
        .map_err(backend)?,
        None => sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_tasks WHERE job_id = ? AND state IN ('queued','running')",
        )
        .bind(job_id)
        .fetch_one(&store.pool)
        .await
        .map_err(backend)?,
    };
    Ok(n)
}

pub(super) async fn job_inflight_counts(
    store: &SqliteControlStore,
    requested_by: Option<i64>,
) -> Result<(i64, i64), ControlError> {
    let user: i64 = match requested_by {
        Some(uid) => sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs WHERE requested_by = ? \
             AND state IN ('queued','running','paused','preempted')",
        )
        .bind(uid)
        .fetch_one(&store.pool)
        .await
        .map_err(backend)?,
        None => 0,
    };
    let global: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs \
         WHERE state IN ('queued','running','paused','preempted')",
    )
    .fetch_one(&store.pool)
    .await
    .map_err(backend)?;
    Ok((user, global))
}

pub(super) async fn recent_jobs(
    store: &SqliteControlStore,
    limit: i64,
) -> Result<Vec<JobRow>, ControlError> {
    let rows = sqlx::query("SELECT * FROM jobs ORDER BY id DESC LIMIT ?")
        .bind(limit)
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?;
    Ok(rows.iter().map(job_row).collect())
}

pub(super) async fn job_tasks_page(
    store: &SqliteControlStore,
    job_id: i64,
    offset: i64,
    limit: i64,
) -> Result<Vec<JobTaskRow>, ControlError> {
    let rows =
        sqlx::query("SELECT * FROM job_tasks WHERE job_id = ? ORDER BY task_no LIMIT ? OFFSET ?")
            .bind(job_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&store.pool)
            .await
            .map_err(backend)?;
    Ok(rows.iter().map(task_row).collect())
}

pub(super) async fn terminal_jobs_before(
    store: &SqliteControlStore,
    cutoff: i64,
    limit: i64,
) -> Result<Vec<i64>, ControlError> {
    sqlx::query_scalar(
        "SELECT id FROM jobs WHERE state IN ('done','error','cancelled') \
         AND updated_at < ? ORDER BY id LIMIT ?",
    )
    .bind(cutoff)
    .bind(limit)
    .fetch_all(&store.pool)
    .await
    .map_err(backend)
}

pub(super) async fn dedupe_keys(store: &SqliteControlStore) -> Result<Vec<String>, ControlError> {
    sqlx::query_scalar("SELECT dedupe_key FROM job_tasks WHERE dedupe_key IS NOT NULL")
        .fetch_all(&store.pool)
        .await
        .map_err(backend)
}

pub(super) async fn schedule_lease(
    store: &SqliteControlStore,
) -> Result<Option<ScheduleLeaseRow>, ControlError> {
    let row = sqlx::query(
        "SELECT term, node, expires_at, acquired_at FROM schedule_lease WHERE singleton = 1",
    )
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(lease_row))
}

pub(super) async fn runstate_meta(
    store: &SqliteControlStore,
    key: &str,
) -> Result<Option<i64>, ControlError> {
    sqlx::query_scalar("SELECT value FROM runstate_meta WHERE key = ?")
        .bind(key)
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)
}

pub(super) async fn max_job_id(store: &SqliteControlStore) -> Result<i64, ControlError> {
    sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM jobs")
        .fetch_one(&store.pool)
        .await
        .map_err(backend)
}

pub(super) async fn max_inventory_run_id(store: &SqliteControlStore) -> Result<i64, ControlError> {
    sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM inventory_runs")
        .fetch_one(&store.pool)
        .await
        .map_err(backend)
}

pub(super) async fn running_task_count(store: &SqliteControlStore) -> Result<i64, ControlError> {
    sqlx::query_scalar("SELECT COUNT(*) FROM job_tasks WHERE state = 'running'")
        .fetch_one(&store.pool)
        .await
        .map_err(backend)
}

async fn task_ids(
    store: &SqliteControlStore,
    sql: &str,
    a: i64,
    b: i64,
) -> Result<Vec<(i64, i64)>, ControlError> {
    sqlx::query_as(sqlx::AssertSqlSafe(sql.to_string()))
        .bind(a)
        .bind(b)
        .fetch_all(&store.pool)
        .await
        .map_err(backend)
}

pub(super) async fn recover_expired_task_ids(
    store: &SqliteControlStore,
    now: i64,
    fence_term: i64,
) -> Result<Vec<(i64, i64)>, ControlError> {
    task_ids(
        store,
        "SELECT job_id, task_no FROM job_tasks WHERE state = 'running' \
         AND deadline_at IS NOT NULL AND deadline_at < ? AND fence_term <= ?",
        now,
        fence_term,
    )
    .await
}

pub(super) async fn recover_stale_task_ids(
    store: &SqliteControlStore,
    cutoff: i64,
    fence_term: i64,
) -> Result<Vec<(i64, i64)>, ControlError> {
    task_ids(
        store,
        "SELECT job_id, task_no FROM job_tasks WHERE state = 'running' \
         AND started_at IS NOT NULL AND started_at < ? AND fence_term <= ?",
        cutoff,
        fence_term,
    )
    .await
}

pub(super) async fn jobs_to_promote(store: &SqliteControlStore) -> Result<Vec<i64>, ControlError> {
    sqlx::query_scalar(
        "SELECT id FROM jobs WHERE state = 'queued' AND EXISTS \
         (SELECT 1 FROM job_tasks t WHERE t.job_id = jobs.id AND t.state IN ('queued','running'))",
    )
    .fetch_all(&store.pool)
    .await
    .map_err(backend)
}

pub(super) async fn orphan_running_jobs(
    store: &SqliteControlStore,
    cutoff: i64,
) -> Result<Vec<JobRow>, ControlError> {
    jobs(
        store.pool(),
        "SELECT * FROM jobs WHERE state = 'running' AND updated_at <= ? \
         AND NOT EXISTS (SELECT 1 FROM job_tasks WHERE job_tasks.job_id = jobs.id)",
        cutoff,
    )
    .await
}

pub(super) async fn preemptable_running_jobs(
    store: &SqliteControlStore,
    now: i64,
) -> Result<Vec<JobRow>, ControlError> {
    jobs(
        store.pool(),
        "SELECT * FROM jobs WHERE state = 'running' \
         AND EXISTS (SELECT 1 FROM job_tasks WHERE job_id = jobs.id AND state = 'queued') \
         AND EXISTS ( \
           SELECT 1 FROM jobs h JOIN job_tasks t ON t.job_id = h.id \
           WHERE h.state IN ('queued','running') \
             AND h.priority > jobs.priority \
             AND h.requested_by IS jobs.requested_by \
             AND t.state = 'queued' AND t.next_at <= ?)",
        now,
    )
    .await
}

pub(super) async fn releasable_preempted_jobs(
    store: &SqliteControlStore,
    now: i64,
) -> Result<Vec<JobRow>, ControlError> {
    jobs(
        store.pool(),
        "SELECT * FROM jobs WHERE state = 'preempted' AND NOT EXISTS ( \
           SELECT 1 FROM jobs h JOIN job_tasks t ON t.job_id = h.id \
           WHERE h.id != jobs.id AND h.priority > jobs.priority \
             AND h.requested_by IS jobs.requested_by \
             AND h.state IN ('queued','running') \
             AND t.state = 'queued' AND t.next_at <= ?)",
        now,
    )
    .await
}

pub(super) async fn queued_tasks_for_job(
    store: &SqliteControlStore,
    job_id: i64,
) -> Result<Vec<JobTaskRow>, ControlError> {
    let rows = sqlx::query("SELECT * FROM job_tasks WHERE job_id = ? AND state = 'queued'")
        .bind(job_id)
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?;
    Ok(rows.iter().map(task_row).collect())
}

impl SqliteControlStore {
    pub(super) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl JobStore for SqliteControlStore {
    async fn job(&self, id: i64) -> Result<Option<JobRow>, ControlError> {
        job(self, id).await
    }
    async fn job_task(
        &self,
        job_id: i64,
        task_no: i64,
    ) -> Result<Option<JobTaskRow>, ControlError> {
        job_task(self, job_id, task_no).await
    }
    async fn job_tasks(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError> {
        job_tasks(self, job_id).await
    }
    async fn failed_job_tasks(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError> {
        failed_job_tasks(self, job_id).await
    }
    async fn jobs_to_promote(&self) -> Result<Vec<i64>, ControlError> {
        jobs_to_promote(self).await
    }
    async fn queued_task_candidate(
        &self,
        now: i64,
        fence_term: i64,
        queued_jobs_allowed: bool,
    ) -> Result<Option<JobTaskRow>, ControlError> {
        queued_task_candidate(self, now, fence_term, queued_jobs_allowed).await
    }
    async fn active_task_count(
        &self,
        job_id: i64,
        exclude: Option<(i64, i64)>,
    ) -> Result<i64, ControlError> {
        active_task_count(self, job_id, exclude).await
    }
    async fn job_inflight_counts(
        &self,
        requested_by: Option<i64>,
    ) -> Result<(i64, i64), ControlError> {
        job_inflight_counts(self, requested_by).await
    }
    async fn recent_jobs(&self, limit: i64) -> Result<Vec<JobRow>, ControlError> {
        recent_jobs(self, limit).await
    }
    async fn job_tasks_page(
        &self,
        job_id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<JobTaskRow>, ControlError> {
        job_tasks_page(self, job_id, offset, limit).await
    }
    async fn terminal_jobs_before(
        &self,
        cutoff: i64,
        limit: i64,
    ) -> Result<Vec<i64>, ControlError> {
        terminal_jobs_before(self, cutoff, limit).await
    }
    async fn dedupe_keys(&self) -> Result<Vec<String>, ControlError> {
        dedupe_keys(self).await
    }
    async fn dedupe_taken(
        &self,
        key: &str,
        job_id: i64,
        task_no: i64,
    ) -> Result<bool, ControlError> {
        dedupe_taken_pool(&self.pool, key, job_id, task_no).await
    }
    async fn schedule_lease(&self) -> Result<Option<ScheduleLeaseRow>, ControlError> {
        schedule_lease(self).await
    }
    async fn runstate_meta(&self, key: &str) -> Result<Option<i64>, ControlError> {
        runstate_meta(self, key).await
    }
    async fn max_job_id(&self) -> Result<i64, ControlError> {
        max_job_id(self).await
    }
    async fn max_inventory_run_id(&self) -> Result<i64, ControlError> {
        max_inventory_run_id(self).await
    }
    async fn running_task_count(&self) -> Result<i64, ControlError> {
        running_task_count(self).await
    }
    async fn recover_expired_task_ids(
        &self,
        now: i64,
        fence_term: i64,
    ) -> Result<Vec<(i64, i64)>, ControlError> {
        recover_expired_task_ids(self, now, fence_term).await
    }
    async fn recover_stale_task_ids(
        &self,
        cutoff: i64,
        fence_term: i64,
    ) -> Result<Vec<(i64, i64)>, ControlError> {
        recover_stale_task_ids(self, cutoff, fence_term).await
    }
    async fn orphan_running_jobs(&self, cutoff: i64) -> Result<Vec<JobRow>, ControlError> {
        orphan_running_jobs(self, cutoff).await
    }
    async fn preemptable_running_jobs(&self, now: i64) -> Result<Vec<JobRow>, ControlError> {
        preemptable_running_jobs(self, now).await
    }
    async fn releasable_preempted_jobs(&self, now: i64) -> Result<Vec<JobRow>, ControlError> {
        releasable_preempted_jobs(self, now).await
    }
    async fn queued_tasks_for_job(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError> {
        queued_tasks_for_job(self, job_id).await
    }
}
