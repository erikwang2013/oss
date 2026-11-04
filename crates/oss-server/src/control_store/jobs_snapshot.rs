//! `RunState` 中 jobs/job_tasks/schedule_lease/runstate_meta 的快照导出与安装。
//!
//! 行序与列序即快照字节确定性来源；安装先 DELETE 四表，再按入参顺序显式 id 插入
//! （AUTOINCREMENT 的 `sqlite_sequence` 行为与迁移前一致）。

use xl_store::control_store::ControlError;
use xl_store::control_store::jobs::{JobRow, JobSnapshotTxn, JobTaskRow, ScheduleLeaseRow};

use super::SqliteControlTxn;
use super::jobs::{backend, job_row, lease_row, task_row};

fn conn<'a>(txn: &'a mut SqliteControlTxn<'_>) -> &'a mut sqlx::SqliteConnection {
    &mut *txn.conn
}

#[async_trait::async_trait]
impl JobSnapshotTxn for SqliteControlTxn<'_> {
    async fn runstate_clear(&mut self) -> Result<(), ControlError> {
        for table in ["jobs", "job_tasks", "schedule_lease", "runstate_meta"] {
            sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {table}")))
                .execute(&mut *conn(self))
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn runstate_jobs(&mut self) -> Result<Vec<JobRow>, ControlError> {
        let rows = sqlx::query(
            "SELECT id, kind, bucket, prefix, state, scanned, deleted, skipped, error, \
                    created_at, updated_at, spec_json, progress, total, succeeded, failed, \
                    report_bucket, report_prefix, priority, requested_by, parent_job_id, \
                    started_at, finished_at FROM jobs ORDER BY id",
        )
        .fetch_all(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(rows.iter().map(job_row).collect())
    }

    async fn runstate_job_tasks(&mut self) -> Result<Vec<JobTaskRow>, ControlError> {
        let rows = sqlx::query(
            "SELECT job_id, task_no, action, payload_json, state, attempt, next_at, dedupe_key, \
                    deadline_at, result_code, result_string, started_at, finished_at, \
                    fence_term, fence_leader FROM job_tasks ORDER BY job_id, task_no",
        )
        .fetch_all(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(rows.iter().map(task_row).collect())
    }

    async fn insert_jobs(&mut self, rows: &[JobRow]) -> Result<(), ControlError> {
        for j in rows {
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
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn insert_job_tasks(&mut self, rows: &[JobTaskRow]) -> Result<(), ControlError> {
        for t in rows {
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
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn snapshot_schedule_lease(&mut self) -> Result<Option<ScheduleLeaseRow>, ControlError> {
        let row = sqlx::query(
            "SELECT term, node, expires_at, acquired_at FROM schedule_lease WHERE singleton = 1",
        )
        .fetch_optional(&mut *conn(self))
        .await
        .map_err(backend)?;
        Ok(row.as_ref().map(lease_row))
    }

    async fn insert_schedule_lease(
        &mut self,
        lease: Option<&ScheduleLeaseRow>,
    ) -> Result<(), ControlError> {
        if let Some(lease) = lease {
            sqlx::query(
                "INSERT INTO schedule_lease (singleton, term, node, expires_at, acquired_at) \
                 VALUES (1, ?, ?, ?, ?)",
            )
            .bind(lease.term)
            .bind(lease.node)
            .bind(lease.expires_at)
            .bind(lease.acquired_at)
            .execute(&mut *conn(self))
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn snapshot_runstate_meta(&mut self, key: &str) -> Result<Option<i64>, ControlError> {
        sqlx::query_scalar("SELECT value FROM runstate_meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&mut *conn(self))
            .await
            .map_err(backend)
    }

    async fn insert_runstate_meta(&mut self, key: &str, value: i64) -> Result<(), ControlError> {
        // 0 = 未初始化：保持 runstate_meta 空，读取时回落到默认 0。
        if value > 0 {
            sqlx::query("INSERT INTO runstate_meta (key, value) VALUES (?, ?)")
                .bind(key)
                .bind(value)
                .execute(&mut *conn(self))
                .await
                .map_err(backend)?;
        }
        Ok(())
    }
}
