//! jobs / job_tasks / schedule_lease / runstate_meta 的 SQLite 事务内写实现。
//!
//! 事务由调用方持有；本模块不 begin/commit/rollback。SQL 与迁移前 `db.rs` /
//! `raft/apply*.rs` 逐字一致（含 `ON CONFLICT`、fence 过滤与返回状态文本）。

use sqlx::Row;
use xl_store::control_store::ControlError;
use xl_store::control_store::jobs::{
    JobClaim, JobRow, JobTaskRow, JobTxn, NewJob, NewTask, ScheduleLeaseRow,
};

use super::SqliteControlTxn;
use super::jobs::{backend, dedupe_taken_conn, job_conn, task_conn};

fn conn<'a>(txn: &'a mut SqliteControlTxn<'_>) -> &'a mut sqlx::SqliteConnection {
    &mut *txn.conn
}

async fn insert_job(txn: &mut SqliteControlTxn<'_>, new: &NewJob) -> Result<i64, ControlError> {
    Ok(sqlx::query(
        "INSERT INTO jobs (kind, bucket, prefix, state, total, priority, requested_by, \
         parent_job_id, report_bucket, report_prefix, spec_json, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&new.kind)
    .bind(&new.bucket)
    .bind(&new.prefix)
    .bind(&new.state)
    .bind(new.total)
    .bind(new.priority)
    .bind(new.requested_by)
    .bind(new.parent_job_id)
    .bind(&new.report_bucket)
    .bind(&new.report_prefix)
    .bind(&new.spec_json)
    .bind(new.created_at)
    .bind(new.updated_at)
    .execute(&mut *conn(txn))
    .await
    .map_err(backend)?
    .last_insert_rowid())
}

async fn insert_task(
    txn: &mut SqliteControlTxn<'_>,
    job_id: i64,
    task_no: i64,
    task: &NewTask,
    ignore_dupes: bool,
) -> Result<u64, ControlError> {
    let prefix = if ignore_dupes {
        "INSERT OR IGNORE"
    } else {
        "INSERT"
    };
    let sql = format!(
        "{prefix} INTO job_tasks \
         (job_id, task_no, action, payload_json, state, next_at, dedupe_key) \
         VALUES (?, ?, ?, ?, 'queued', ?, ?)"
    );
    Ok(sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(job_id)
        .bind(task_no)
        .bind(&task.action)
        .bind(&task.payload_json)
        .bind(task.next_at)
        .bind(&task.dedupe_key)
        .execute(&mut *conn(txn))
        .await
        .map_err(backend)?
        .rows_affected())
}

#[async_trait::async_trait]
impl JobTxn for SqliteControlTxn<'_> {
    async fn job_create(&mut self, new: &NewJob, tasks: &[NewTask]) -> Result<i64, ControlError> {
        let id = insert_job(self, new).await?;
        for (i, task) in tasks.iter().enumerate() {
            insert_task(self, id, i as i64 + 1, task, false).await?;
        }
        Ok(id)
    }

    async fn job_create_dedup(
        &mut self,
        new: &NewJob,
        tasks: &[NewTask],
    ) -> Result<Option<i64>, ControlError> {
        let id = insert_job(self, new).await?;
        let mut inserted = 0i64;
        for (i, task) in tasks.iter().enumerate() {
            inserted += insert_task(self, id, i as i64 + 1, task, true).await? as i64;
        }
        if inserted == 0 {
            return Ok(None);
        }
        if inserted != tasks.len() as i64 {
            sqlx::query("UPDATE jobs SET total = ? WHERE id = ?")
                .bind(inserted)
                .bind(id)
                .execute(&mut *conn(self))
                .await
                .map_err(backend)?;
        }
        Ok(Some(id))
    }

    async fn job_create_restore(
        &mut self,
        new: &NewJob,
        tasks: &[NewTask],
    ) -> Result<(i64, bool), ControlError> {
        let id = insert_job(self, new).await?;
        let mut inserted = false;
        for (i, task) in tasks.iter().enumerate() {
            inserted |= insert_task(self, id, i as i64 + 1, task, true).await? > 0;
        }
        Ok((id, inserted))
    }

    async fn job_start(
        &mut self,
        kind: &str,
        bucket: &str,
        prefix: &str,
        now: i64,
    ) -> Result<i64, ControlError> {
        Ok(sqlx::query(
            "INSERT INTO jobs (kind, bucket, prefix, state, created_at, updated_at) \
             VALUES (?, ?, ?, 'running', ?, ?)",
        )
        .bind(kind)
        .bind(bucket)
        .bind(prefix)
        .bind(now)
        .bind(now)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?
        .last_insert_rowid())
    }

    async fn job_finish(
        &mut self,
        id: i64,
        state: &str,
        scanned: i64,
        deleted: i64,
        skipped: i64,
        error: Option<&str>,
        now: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "UPDATE jobs SET state = ?, scanned = ?, deleted = ?, skipped = ?, \
             error = COALESCE(?, error), updated_at = ? WHERE id = ?",
        )
        .bind(state)
        .bind(scanned)
        .bind(deleted)
        .bind(skipped)
        .bind(error)
        .bind(now)
        .bind(id)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn job_set_progress(
        &mut self,
        job_id: i64,
        progress: i64,
        now: i64,
    ) -> Result<(), ControlError> {
        sqlx::query("UPDATE jobs SET progress = ?, updated_at = ? WHERE id = ?")
            .bind(progress)
            .bind(now)
            .bind(job_id)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn job_retry_failed(&mut self, id: i64, now: i64) -> Result<Option<i64>, ControlError> {
        let Some(parent) = job_conn(conn(self), id).await? else {
            return Ok(None);
        };
        let tasks = sqlx::query(
            "SELECT action, payload_json FROM job_tasks WHERE job_id = ? AND state = 'failed' \
             ORDER BY task_no",
        )
        .bind(id)
        .fetch_all(&mut *conn(self))
        .await
        .map_err(backend)?;
        if tasks.is_empty() {
            return Ok(None);
        }
        let new_id = sqlx::query(
            "INSERT INTO jobs (kind, bucket, prefix, state, total, priority, requested_by, \
             parent_job_id, report_bucket, report_prefix, spec_json, created_at, updated_at) \
             VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&parent.kind)
        .bind(&parent.bucket)
        .bind(&parent.prefix)
        .bind(tasks.len() as i64)
        .bind(parent.priority)
        .bind(parent.requested_by)
        .bind(id)
        .bind(&parent.report_bucket)
        .bind(&parent.report_prefix)
        .bind(&parent.spec_json)
        .bind(now)
        .bind(now)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?
        .last_insert_rowid();
        for (i, t) in tasks.iter().enumerate() {
            sqlx::query(
                "INSERT INTO job_tasks (job_id, task_no, action, payload_json, state, next_at) \
                 VALUES (?, ?, ?, ?, 'queued', ?)",
            )
            .bind(new_id)
            .bind(i as i64 + 1)
            .bind(t.get::<String, _>("action"))
            .bind(t.get::<String, _>("payload_json"))
            .bind(now)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        }
        Ok(Some(new_id))
    }

    async fn job_claim(
        &mut self,
        now: i64,
        timeout_secs: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<Option<JobClaim>, ControlError> {
        sqlx::query(
            "UPDATE jobs SET state = 'running', started_at = COALESCE(started_at, ?), updated_at = ? \
             WHERE state = 'queued' AND EXISTS (SELECT 1 FROM job_tasks t \
             WHERE t.job_id = jobs.id AND t.state IN ('queued','running'))",
        )
        .bind(now)
        .bind(now)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        let row = sqlx::query(
            "SELECT t.job_id, t.task_no, t.action, t.payload_json, t.attempt \
             FROM job_tasks t JOIN jobs j ON j.id = t.job_id \
             WHERE t.state = 'queued' AND t.next_at <= ? AND j.state = 'running' \
               AND t.fence_term <= ? \
             ORDER BY j.priority DESC, t.next_at, t.task_no LIMIT 1",
        )
        .bind(now)
        .bind(fence_term)
        .fetch_optional(&mut *conn(self))
        .await
        .map_err(backend)?;
        let Some(r) = row else {
            return Ok(None);
        };
        let (job_id, task_no): (i64, i64) = (r.get("job_id"), r.get("task_no"));
        sqlx::query(
            "UPDATE job_tasks SET state = 'running', started_at = ?, deadline_at = ?, \
             fence_term = ?, fence_leader = ? \
             WHERE job_id = ? AND task_no = ? AND state = 'queued' AND fence_term <= ?",
        )
        .bind(now)
        .bind(now + timeout_secs)
        .bind(fence_term)
        .bind(fence_leader)
        .bind(job_id)
        .bind(task_no)
        .bind(fence_term)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(Some(JobClaim {
            job_id,
            task_no,
            action: r.get("action"),
            payload_json: r.get("payload_json"),
            attempt: r.get("attempt"),
        }))
    }

    async fn task_set_deadline(
        &mut self,
        job_id: i64,
        task_no: i64,
        deadline_at: i64,
        fence_term: i64,
    ) -> Result<(), ControlError> {
        sqlx::query(
            "UPDATE job_tasks SET deadline_at = ? \
             WHERE job_id = ? AND task_no = ? AND state = 'running' AND fence_term = ?",
        )
        .bind(deadline_at)
        .bind(job_id)
        .bind(task_no)
        .bind(fence_term)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn requeue_task(
        &mut self,
        job_id: i64,
        task_no: i64,
        now: i64,
        fence_term: i64,
    ) -> Result<bool, ControlError> {
        let n = sqlx::query(
            "UPDATE job_tasks SET state = 'queued', started_at = NULL, deadline_at = NULL, \
             next_at = ? WHERE job_id = ? AND task_no = ? AND state = 'running' \
             AND fence_term = ?",
        )
        .bind(now)
        .bind(job_id)
        .bind(task_no)
        .bind(fence_term)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n > 0)
    }

    async fn preempt_lower_priority(
        &mut self,
        capacity: usize,
        now: i64,
    ) -> Result<usize, ControlError> {
        if capacity == 0 {
            return Ok(0);
        }
        let running: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM job_tasks WHERE state = 'running'")
                .fetch_one(&mut *conn(self))
                .await
                .map_err(backend)?;
        if running < capacity as i64 {
            return Ok(0);
        }
        let n = sqlx::query(
            "UPDATE jobs SET state = 'preempted', updated_at = ? \
             WHERE state = 'running' \
             AND EXISTS (SELECT 1 FROM job_tasks WHERE job_id = jobs.id AND state = 'queued') \
             AND EXISTS ( \
               SELECT 1 FROM jobs h JOIN job_tasks t ON t.job_id = h.id \
               WHERE h.state IN ('queued','running') \
                 AND h.priority > jobs.priority \
                 AND h.requested_by IS jobs.requested_by \
                 AND t.state = 'queued' AND t.next_at <= ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n as usize)
    }

    async fn release_preempted(&mut self, now: i64) -> Result<usize, ControlError> {
        let n = sqlx::query(
            "UPDATE jobs SET state = 'queued', updated_at = ? \
             WHERE state = 'preempted' AND NOT EXISTS ( \
               SELECT 1 FROM jobs h JOIN job_tasks t ON t.job_id = h.id \
               WHERE h.id != jobs.id AND h.priority > jobs.priority \
                 AND h.requested_by IS jobs.requested_by \
                 AND h.state IN ('queued','running') \
                 AND t.state = 'queued' AND t.next_at <= ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n as usize)
    }

    async fn finish_task(
        &mut self,
        job_id: i64,
        task_no: i64,
        ok: bool,
        code: &str,
        message: Option<&str>,
        now: i64,
        max_attempts: i64,
        fence_term: i64,
    ) -> Result<&'static str, ControlError> {
        let row = sqlx::query(
            "SELECT state, attempt, fence_term FROM job_tasks WHERE job_id = ? AND task_no = ?",
        )
        .bind(job_id)
        .bind(task_no)
        .fetch_optional(&mut *conn(self))
        .await
        .map_err(backend)?;
        let Some(r) = row else {
            return Ok("missing");
        };
        if r.get::<String, _>("state") != "running" {
            return Ok("not-running");
        }
        if r.get::<i64, _>("fence_term") > fence_term {
            return Ok("fenced");
        }
        let attempt: i64 = r.get("attempt");
        if ok {
            sqlx::query(
                "UPDATE job_tasks SET state = 'done', result_code = ?, result_string = ?, \
                 finished_at = ?, dedupe_key = NULL \
                 WHERE job_id = ? AND task_no = ? AND fence_term <= ?",
            )
            .bind(code)
            .bind(message)
            .bind(now)
            .bind(job_id)
            .bind(task_no)
            .bind(fence_term)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
            sqlx::query(
                "UPDATE jobs SET succeeded = succeeded + 1, progress = progress + 1, updated_at = ? \
                 WHERE id = ?",
            )
            .bind(now)
            .bind(job_id)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        } else if attempt + 1 < max_attempts {
            let backoff = crate::db::Db::retry_backoff_secs(attempt, now, job_id, task_no);
            sqlx::query(
                "UPDATE job_tasks SET state = 'queued', attempt = attempt + 1, next_at = ?, \
                 result_code = ?, result_string = ?, started_at = NULL, deadline_at = NULL \
                 WHERE job_id = ? AND task_no = ? AND fence_term <= ?",
            )
            .bind(now + backoff)
            .bind(code)
            .bind(message)
            .bind(job_id)
            .bind(task_no)
            .bind(fence_term)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
            sqlx::query("UPDATE jobs SET updated_at = ? WHERE id = ?")
                .bind(now)
                .bind(job_id)
                .execute(&mut *conn(self))
                .await
                .map_err(backend)?;
        } else {
            sqlx::query(
                "UPDATE job_tasks SET state = 'failed', attempt = attempt + 1, \
                 result_code = 'PermanentFailure', result_string = ?, finished_at = ?, \
                 dedupe_key = NULL WHERE job_id = ? AND task_no = ? AND fence_term <= ?",
            )
            .bind(message)
            .bind(now)
            .bind(job_id)
            .bind(task_no)
            .bind(fence_term)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
            sqlx::query(
                "UPDATE jobs SET failed = failed + 1, progress = progress + 1, updated_at = ? \
                 WHERE id = ?",
            )
            .bind(now)
            .bind(job_id)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        }
        sqlx::query(
            "UPDATE jobs SET state = CASE WHEN failed > 0 THEN 'error' ELSE 'done' END, \
             finished_at = ?, updated_at = ? \
             WHERE id = ? AND state IN ('running','preempted') AND NOT EXISTS \
             (SELECT 1 FROM job_tasks WHERE job_id = ? AND state IN ('queued','running'))",
        )
        .bind(now)
        .bind(now)
        .bind(job_id)
        .bind(job_id)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(if ok {
            "done"
        } else if attempt + 1 < max_attempts {
            "queued"
        } else {
            "failed"
        })
    }

    async fn take_over_task(
        &mut self,
        job_id: i64,
        task_no: i64,
        started_before: Option<i64>,
        now: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError> {
        let deadline_cond = if started_before.is_some() {
            "started_at IS NOT NULL AND started_at < ?"
        } else {
            "deadline_at IS NOT NULL AND deadline_at < ?"
        };
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE job_tasks SET fence_term = ?, fence_leader = ? \
             WHERE job_id = ? AND task_no = ? AND state = 'running' AND {deadline_cond} \
               AND fence_term <= ?"
        )))
        .bind(fence_term)
        .bind(fence_leader)
        .bind(job_id)
        .bind(task_no)
        .bind(started_before.unwrap_or(now))
        .bind(fence_term)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn recover_orphan_jobs(&mut self, cutoff: i64, now: i64) -> Result<usize, ControlError> {
        let n = sqlx::query(
            "UPDATE jobs SET state = 'error', \
             error = COALESCE(error, '进程崩溃遗留的 running 作业，已自动回收'), \
             finished_at = ?, updated_at = ? \
             WHERE state = 'running' AND updated_at <= ? \
             AND NOT EXISTS (SELECT 1 FROM job_tasks WHERE job_tasks.job_id = jobs.id)",
        )
        .bind(now)
        .bind(now)
        .bind(cutoff)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?
        .rows_affected();
        Ok(n as usize)
    }

    async fn job_heartbeat(&mut self, id: i64, now: i64) -> Result<(), ControlError> {
        sqlx::query("UPDATE jobs SET updated_at = ? WHERE id = ? AND state = 'running'")
            .bind(now)
            .bind(id)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn job_set_error(&mut self, id: i64, msg: &str, now: i64) -> Result<(), ControlError> {
        sqlx::query("UPDATE jobs SET error = ?, updated_at = ? WHERE id = ?")
            .bind(msg)
            .bind(now)
            .bind(id)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn job_set_state(
        &mut self,
        id: i64,
        action: &str,
        now: i64,
    ) -> Result<bool, ControlError> {
        let n = match action {
            "pause" => sqlx::query(
                "UPDATE jobs SET state = 'paused', updated_at = ? \
                 WHERE id = ? AND state IN ('running','preempted')",
            )
            .bind(now)
            .bind(id)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?
            .rows_affected(),
            "resume" => sqlx::query(
                "UPDATE jobs SET state = 'running', updated_at = ? WHERE id = ? AND state = 'paused'",
            )
            .bind(now)
            .bind(id)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?
            .rows_affected(),
            "cancel" => {
                let n = sqlx::query(
                    "UPDATE jobs SET state = 'cancelled', updated_at = ? \
                     WHERE id = ? AND state IN ('queued','running','paused','preempted')",
                )
                .bind(now)
                .bind(id)
                .execute(&mut *conn(self))
                .await
                .map_err(backend)?
                .rows_affected();
                if n > 0 {
                    sqlx::query(
                        "UPDATE job_tasks SET state = 'cancelled', finished_at = ? \
                         WHERE job_id = ? AND state = 'queued'",
                    )
                    .bind(now)
                    .bind(id)
                    .execute(&mut *conn(self))
                    .await
                    .map_err(backend)?;
                }
                n
            }
            _ => 0,
        };
        Ok(n > 0)
    }

    async fn job_upsert(&mut self, job: &JobRow) -> Result<u64, ControlError> {
        let result = sqlx::query(
            "INSERT INTO jobs (id, kind, bucket, prefix, state, scanned, deleted, skipped, error, \
              created_at, updated_at, spec_json, progress, total, succeeded, failed, report_bucket, \
              report_prefix, priority, requested_by, parent_job_id, started_at, finished_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(id) DO UPDATE SET \
              kind = excluded.kind, bucket = excluded.bucket, prefix = excluded.prefix, \
              state = excluded.state, scanned = excluded.scanned, deleted = excluded.deleted, \
              skipped = excluded.skipped, error = excluded.error, created_at = excluded.created_at, \
              updated_at = excluded.updated_at, spec_json = excluded.spec_json, \
              progress = excluded.progress, total = excluded.total, \
              succeeded = excluded.succeeded, failed = excluded.failed, \
              report_bucket = excluded.report_bucket, report_prefix = excluded.report_prefix, \
              priority = excluded.priority, requested_by = excluded.requested_by, \
              parent_job_id = excluded.parent_job_id, started_at = excluded.started_at, \
              finished_at = excluded.finished_at",
        )
        .bind(job.id)
        .bind(&job.kind)
        .bind(&job.bucket)
        .bind(&job.prefix)
        .bind(&job.state)
        .bind(job.scanned)
        .bind(job.deleted)
        .bind(job.skipped)
        .bind(&job.error)
        .bind(job.created_at)
        .bind(job.updated_at)
        .bind(&job.spec_json)
        .bind(job.progress)
        .bind(job.total)
        .bind(job.succeeded)
        .bind(job.failed)
        .bind(&job.report_bucket)
        .bind(&job.report_prefix)
        .bind(job.priority)
        .bind(job.requested_by)
        .bind(job.parent_job_id)
        .bind(job.started_at)
        .bind(job.finished_at)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(result.rows_affected())
    }

    async fn job_task_upsert(&mut self, task: &JobTaskRow) -> Result<u64, ControlError> {
        let result = sqlx::query(
            "INSERT INTO job_tasks (job_id, task_no, action, payload_json, state, attempt, next_at, \
              dedupe_key, deadline_at, result_code, result_string, started_at, finished_at, \
              fence_term, fence_leader) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(job_id, task_no) DO UPDATE SET \
              action = excluded.action, payload_json = excluded.payload_json, \
              state = excluded.state, attempt = excluded.attempt, next_at = excluded.next_at, \
              dedupe_key = CASE WHEN excluded.state IN ('done','failed','cancelled') THEN NULL \
                                ELSE excluded.dedupe_key END, \
              deadline_at = excluded.deadline_at, result_code = excluded.result_code, \
              result_string = excluded.result_string, started_at = excluded.started_at, \
              finished_at = excluded.finished_at, fence_term = excluded.fence_term, \
              fence_leader = excluded.fence_leader",
        )
        .bind(task.job_id)
        .bind(task.task_no)
        .bind(&task.action)
        .bind(&task.payload_json)
        .bind(&task.state)
        .bind(task.attempt)
        .bind(task.next_at)
        .bind(&task.dedupe_key)
        .bind(task.deadline_at)
        .bind(&task.result_code)
        .bind(&task.result_string)
        .bind(task.started_at)
        .bind(task.finished_at)
        .bind(task.fence_term)
        .bind(task.fence_leader)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(result.rows_affected())
    }

    async fn job(&mut self, id: i64) -> Result<Option<JobRow>, ControlError> {
        job_conn(conn(self), id).await
    }

    async fn job_task(
        &mut self,
        job_id: i64,
        task_no: i64,
    ) -> Result<Option<JobTaskRow>, ControlError> {
        task_conn(conn(self), job_id, task_no).await
    }

    async fn dedupe_taken(
        &mut self,
        key: &str,
        job_id: i64,
        task_no: i64,
    ) -> Result<bool, ControlError> {
        dedupe_taken_conn(conn(self), key, job_id, task_no).await
    }

    async fn purge_terminal_jobs(&mut self, ids: &[i64]) -> Result<(u64, usize), ControlError> {
        let mut q = id_list("SELECT id, state FROM jobs WHERE id IN (", ids);
        let rows: Vec<(i64, String)> = q
            .build_query_as()
            .fetch_all(&mut *conn(self))
            .await
            .map_err(backend)?;
        let mut terminal: Vec<i64> = rows
            .iter()
            .filter(|(_, state)| matches!(state.as_str(), "done" | "error" | "cancelled"))
            .map(|(id, _)| *id)
            .collect();
        terminal.sort_unstable();
        let non_terminal = rows.len() - terminal.len();
        if terminal.is_empty() {
            return Ok((0, non_terminal));
        }
        // decision 8：purge 解除 runs/restores 引用与 parent 引用后，RunState::validate 无悬空。
        for prefix in [
            "UPDATE inventory_runs SET job_id = NULL WHERE job_id IN (",
            "UPDATE tier_restores SET job_id = NULL WHERE job_id IN (",
            "UPDATE jobs SET parent_job_id = NULL WHERE parent_job_id IN (",
            "DELETE FROM job_tasks WHERE job_id IN (",
        ] {
            id_list(prefix, &terminal)
                .build()
                .execute(&mut *conn(self))
                .await
                .map_err(backend)?;
        }
        let deleted = id_list(
            "DELETE FROM jobs WHERE state IN ('done','error','cancelled') AND id IN (",
            &terminal,
        )
        .build()
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?
        .rows_affected();
        Ok((deleted, non_terminal))
    }

    async fn schedule_lease_upsert(
        &mut self,
        lease: &ScheduleLeaseRow,
    ) -> Result<u64, ControlError> {
        let result = sqlx::query(
            "INSERT INTO schedule_lease (singleton, term, node, expires_at, acquired_at) \
             VALUES (1, ?, ?, ?, ?) \
             ON CONFLICT(singleton) DO UPDATE SET term = excluded.term, node = excluded.node, \
              expires_at = excluded.expires_at, acquired_at = excluded.acquired_at",
        )
        .bind(lease.term)
        .bind(lease.node)
        .bind(lease.expires_at)
        .bind(lease.acquired_at)
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(result.rows_affected())
    }

    async fn runstate_meta_next_run_id(&mut self, value: i64) -> Result<(), ControlError> {
        sqlx::query(
            "INSERT INTO runstate_meta (key, value) VALUES ('next_run_id', ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value \
             WHERE CAST(runstate_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)",
        )
        .bind(value.to_string())
        .execute(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn schedule_lease(&mut self) -> Result<Option<ScheduleLeaseRow>, ControlError> {
        let row = sqlx::query(
            "SELECT term, node, expires_at, acquired_at FROM schedule_lease WHERE singleton = 1",
        )
        .fetch_optional(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(row.as_ref().map(super::jobs::lease_row))
    }
}

fn id_list(prefix: &str, ids: &[i64]) -> sqlx::QueryBuilder<sqlx::Sqlite> {
    let mut qb = sqlx::QueryBuilder::new(prefix.to_string());
    {
        let mut sep = qb.separated(", ");
        for id in ids {
            sep.push_bind(*id);
        }
    }
    qb.push(")");
    qb
}
