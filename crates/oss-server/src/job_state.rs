//! M8-2：jobs/job_tasks 运行态写入门面。
//!
//! 复制模式下（Raft 且 leader 生效 feature ≥3）所有作业行/任务行写入都以完整行
//! 经 [`ControlPlane::write`] 提交，apply 按主键 upsert；follower 只 apply，不领取。
//! 非 Raft 或 feature<3 时退回 M8-1 的本地直写（debug 日志记录降级），行为不变。
//!
//! 读-改-写（领取/收尾/抢占/回收）在进程内用一把 mutex 串行化：Raft apply 有全序，
//! 但“读本行 → 合并计数 → 提交下一行”之间仍可能被同进程其他 worker 插入，丢更新。
//! 跨节点并发由 apply 的 `updated_at`/计数/`fence_term` 单调闸兜底。
//!
//! 已知限制：job id 仍由 leader 本地分配（`runstate_meta`/`MAX(id)+1`）。与
//! `create_restore_job` 的本地 AUTOINCREMENT 在 Raft 往返窗口内相撞时，apply 以
//! `(kind, created_at)` 身份闸拒绝整批（`Conflict`），绝不覆盖另一条作业；调用方
//! 重试即可拿到下一个 id。tier_restores / inventory / lifecycle 状态表不在本切片。

mod admin;

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::db::{ClaimedTask, Db, JobSpec, NewJobTask};
use crate::raft::command::{ControlCommand, ControlResponse};
use crate::raft::control::ControlPlane;
use crate::raft::runstate::{JobRow, JobTaskRow, job_row_from, job_task_row_from};
use crate::raft::{FencingEpoch, compat};

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn new_job(spec: &JobSpec, id: i64, now: i64, total: i64) -> JobRow {
    JobRow {
        id,
        kind: spec.kind.clone(),
        bucket: spec.bucket.clone(),
        prefix: spec.prefix.clone(),
        state: "queued".into(),
        total,
        priority: spec.priority,
        requested_by: spec.requested_by,
        parent_job_id: spec.parent_job_id,
        report_bucket: spec.report_bucket.clone(),
        report_prefix: spec.report_prefix.clone(),
        spec_json: spec.spec_json.clone(),
        created_at: now,
        updated_at: now,
        ..JobRow::default()
    }
}

fn new_task(job_id: i64, task_no: i64, task: &NewJobTask, now: i64) -> JobTaskRow {
    JobTaskRow {
        job_id,
        task_no,
        action: task.action.clone(),
        payload_json: task.payload.clone(),
        state: "queued".into(),
        next_at: now,
        dedupe_key: task.dedupe_key.clone(),
        ..JobTaskRow::default()
    }
}

pub struct JobState {
    control: Option<Arc<ControlPlane>>,
    lock: Mutex<()>,
}

impl JobState {
    pub fn new(control: Option<Arc<ControlPlane>>) -> Self {
        Self {
            control,
            lock: Mutex::new(()),
        }
    }

    /// 无控制面（单机/测试）：全部本地直写，与 M8-1 完全一致。
    pub fn local() -> Self {
        Self::new(None)
    }

    /// Raft 且 leader 生效 feature≥3 → 行进复制日志；follower 收到的管理写按转发
    /// 处理（append 前由 leader 判生效级别）。Local/低级别 → 保持本地直写。
    pub fn replicated(&self) -> bool {
        let Some(handle) = self.control.as_ref().and_then(|c| c.raft_handle()) else {
            return false;
        };
        if handle.advertised_feature() < compat::FEATURE_OSSCTL2 {
            return false;
        }
        if !handle.is_leader() {
            return true;
        }
        handle
            .effective_feature()
            .is_some_and(|level| level >= compat::FEATURE_OSSCTL2)
    }

    fn note_local(&self, op: &str) {
        // debug：worker 每 2s 一轮，info 会刷屏；启动时另有一次性 info。
        tracing::debug!(op, "job 状态写入保持节点本地（raft feature < 3 或未启用）");
    }

    fn require_leader(&self, op: &str) -> Result<(), String> {
        if self.control.as_ref().is_some_and(|c| !c.is_leader()) {
            return Err(format!("{op}: not leader; retry on the leader"));
        }
        Ok(())
    }

    async fn commit(&self, jobs: Vec<JobRow>, tasks: Vec<JobTaskRow>) -> Result<(), String> {
        if jobs.is_empty() && tasks.is_empty() {
            return Ok(());
        }
        let Some(control) = &self.control else {
            return Err("job state commit without control plane".into());
        };
        match control
            .write(ControlCommand::JobBatch { jobs, tasks })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(()),
            // 身份撞车 / 任务引用缺失：整批未写（apply 先校验再写）。
            Ok(other) => Err(format!("job batch rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn read_job(db: &Db, id: i64) -> Result<Option<JobRow>, String> {
        let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(db.pool())
            .await
            .map_err(err)?;
        Ok(row.as_ref().map(job_row_from))
    }

    async fn read_task(db: &Db, job_id: i64, task_no: i64) -> Result<Option<JobTaskRow>, String> {
        let row = sqlx::query("SELECT * FROM job_tasks WHERE job_id = ? AND task_no = ?")
            .bind(job_id)
            .bind(task_no)
            .fetch_optional(db.pool())
            .await
            .map_err(err)?;
        Ok(row.as_ref().map(job_task_row_from))
    }

    async fn next_job_id(db: &Db) -> Result<i64, String> {
        let meta: Option<i64> =
            sqlx::query_scalar("SELECT value FROM runstate_meta WHERE key = 'next_job_id'")
                .fetch_optional(db.pool())
                .await
                .map_err(err)?;
        let max: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM jobs")
            .fetch_one(db.pool())
            .await
            .map_err(err)?;
        Ok(meta.unwrap_or(0).max(max + 1).max(1))
    }

    pub async fn create_job(&self, db: &Db, spec: &JobSpec, now: i64) -> Result<i64, String> {
        if !self.replicated() {
            self.note_local("create_job");
            return db.create_job(spec, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("create_job")?;
        let id = Self::next_job_id(db).await?;
        let tasks: Vec<JobTaskRow> = spec
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| new_task(id, i as i64 + 1, t, now))
            .collect();
        self.commit(vec![new_job(spec, id, now, tasks.len() as i64)], tasks)
            .await?;
        Ok(id)
    }

    pub async fn create_job_dedup(
        &self,
        db: &Db,
        spec: &JobSpec,
        now: i64,
    ) -> Result<Option<i64>, String> {
        if !self.replicated() {
            self.note_local("create_job_dedup");
            return db.create_job_dedup(spec, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("create_job_dedup")?;
        let mut seen: HashSet<String> =
            sqlx::query_scalar("SELECT dedupe_key FROM job_tasks WHERE dedupe_key IS NOT NULL")
                .fetch_all(db.pool())
                .await
                .map_err(err)?
                .into_iter()
                .collect();
        let mut tasks = Vec::new();
        for task in &spec.tasks {
            // 同批重复键也是先到先得（与 INSERT OR IGNORE 一致）。
            if task
                .dedupe_key
                .as_ref()
                .is_none_or(|k| seen.insert(k.clone()))
            {
                tasks.push(new_task(0, tasks.len() as i64 + 1, task, now));
            }
        }
        if tasks.is_empty() {
            return Ok(None);
        }
        let id = Self::next_job_id(db).await?;
        for task in &mut tasks {
            task.job_id = id;
        }
        self.commit(vec![new_job(spec, id, now, tasks.len() as i64)], tasks)
            .await?;
        Ok(Some(id))
    }

    /// 失败任务重试：建子作业（新 id）。子作业必须由 leader 分配 id。
    pub async fn job_retry_failed(
        &self,
        db: &Db,
        id: i64,
        now: i64,
    ) -> Result<Option<i64>, String> {
        if !self.replicated() {
            self.note_local("job_retry_failed");
            return db.job_retry_failed(id, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("job_retry_failed")?;
        let Some(parent) = Self::read_job(db, id).await? else {
            return Ok(None);
        };
        let rows = sqlx::query(
            "SELECT * FROM job_tasks WHERE job_id = ? AND state = 'failed' ORDER BY task_no",
        )
        .bind(id)
        .fetch_all(db.pool())
        .await
        .map_err(err)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let new_id = Self::next_job_id(db).await?;
        let tasks: Vec<JobTaskRow> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let mut task = job_task_row_from(r);
                task.job_id = new_id;
                task.task_no = i as i64 + 1;
                task.state = "queued".into();
                task.attempt = 0;
                task.next_at = now;
                task.dedupe_key = None;
                task.deadline_at = None;
                task.result_code = None;
                task.result_string = None;
                task.started_at = None;
                task.finished_at = None;
                task.fence_term = 0;
                task.fence_leader = 0;
                task
            })
            .collect();
        let job = JobRow {
            id: new_id,
            kind: parent.kind,
            bucket: parent.bucket,
            prefix: parent.prefix,
            state: "queued".into(),
            total: tasks.len() as i64,
            priority: parent.priority,
            requested_by: parent.requested_by,
            parent_job_id: Some(id),
            report_bucket: parent.report_bucket,
            report_prefix: parent.report_prefix,
            spec_json: parent.spec_json,
            created_at: now,
            updated_at: now,
            ..JobRow::default()
        };
        self.commit(vec![job], tasks).await?;
        Ok(Some(new_id))
    }

    pub async fn claim_task_fenced(
        &self,
        db: &Db,
        now: i64,
        timeout_secs: i64,
        fence: FencingEpoch,
    ) -> Result<Option<ClaimedTask>, String> {
        if !self.replicated() {
            self.note_local("claim_task");
            return db
                .claim_task_fenced(now, timeout_secs, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("claim_task")?;
        let promoted: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM jobs WHERE state = 'queued' AND EXISTS \
             (SELECT 1 FROM job_tasks t WHERE t.job_id = jobs.id AND t.state IN ('queued','running'))",
        )
        .fetch_all(db.pool())
        .await
        .map_err(err)?;
        let mut jobs = Vec::new();
        for id in promoted {
            if let Some(mut job) = Self::read_job(db, id).await? {
                job.state = "running".into();
                job.started_at = Some(job.started_at.unwrap_or(now));
                job.updated_at = now;
                jobs.push(job);
            }
        }
        // 作业提升与任务领取在同一批提交：这里必须接受仍为 queued 的作业（本批会提升）。
        let candidate = sqlx::query(
            "SELECT t.* FROM job_tasks t JOIN jobs j ON j.id = t.job_id \
             WHERE t.state = 'queued' AND t.next_at <= ? AND j.state IN ('queued','running') \
               AND t.fence_term <= ? \
             ORDER BY j.priority DESC, t.next_at, t.task_no LIMIT 1",
        )
        .bind(now)
        .bind(fence.term_i64())
        .fetch_optional(db.pool())
        .await
        .map_err(err)?;
        let Some(row) = candidate else {
            self.commit(jobs, Vec::new()).await?;
            return Ok(None);
        };
        let mut task = job_task_row_from(&row);
        let job = match jobs.iter().position(|j| j.id == task.job_id) {
            Some(i) => jobs.remove(i),
            // 领取也要带上完整作业行：tier/lifecycle 等本地创建的作业借此在副本收敛。
            None => Self::read_job(db, task.job_id)
                .await?
                .ok_or_else(|| format!("claim: job {} missing", task.job_id))?,
        };
        let claimed = ClaimedTask {
            job_id: task.job_id,
            task_no: task.task_no,
            action: task.action.clone(),
            payload_json: task.payload_json.clone(),
            attempt: task.attempt,
        };
        task.state = "running".into();
        task.started_at = Some(now);
        task.deadline_at = Some(now + timeout_secs);
        task.fence_term = fence.term_i64();
        task.fence_leader = fence.leader_i64();
        self.commit(vec![job], vec![task]).await?;
        Ok(Some(claimed))
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_locked(
        &self,
        db: &Db,
        job_id: i64,
        task_no: i64,
        ok: bool,
        code: &str,
        message: Option<&str>,
        now: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<&'static str, String> {
        let Some(mut task) = Self::read_task(db, job_id, task_no).await? else {
            return Ok("missing");
        };
        if task.state != "running" {
            return Ok("not-running");
        }
        if task.fence_term > fence.term_i64() {
            return Ok("fenced");
        }
        let Some(mut job) = Self::read_job(db, job_id).await? else {
            return Err(format!("finish: job {job_id} missing for task {task_no}"));
        };
        let attempt = task.attempt;
        if ok {
            task.state = "done".into();
            task.result_code = Some(code.to_string());
            task.result_string = message.map(str::to_string);
            task.finished_at = Some(now);
            task.dedupe_key = None;
            job.succeeded += 1;
            job.progress += 1;
        } else if attempt + 1 < max_attempts {
            let backoff = 10i64
                .saturating_mul(2i64.saturating_pow(attempt.min(30) as u32))
                .min(300)
                + (now % 1000).abs();
            task.state = "queued".into();
            task.attempt = attempt + 1;
            task.next_at = now + backoff;
            task.result_code = Some(code.to_string());
            task.result_string = message.map(str::to_string);
            task.started_at = None;
            task.deadline_at = None;
        } else {
            task.state = "failed".into();
            task.attempt = attempt + 1;
            task.result_code = Some("PermanentFailure".into());
            task.result_string = message.map(str::to_string);
            task.finished_at = Some(now);
            task.dedupe_key = None;
            job.failed += 1;
            job.progress += 1;
        }
        task.fence_term = fence.term_i64();
        task.fence_leader = fence.leader_i64();
        job.updated_at = now;
        let task_active = matches!(task.state.as_str(), "queued" | "running");
        let others: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_tasks WHERE job_id = ? AND task_no <> ? \
             AND state IN ('queued','running')",
        )
        .bind(job_id)
        .bind(task_no)
        .fetch_one(db.pool())
        .await
        .map_err(err)?;
        if !task_active && others == 0 && matches!(job.state.as_str(), "running" | "preempted") {
            job.state = if job.failed > 0 { "error" } else { "done" }.into();
            job.finished_at = Some(now);
        }
        self.commit(vec![job], vec![task]).await?;
        Ok(if ok {
            "done"
        } else if attempt + 1 < max_attempts {
            "queued"
        } else {
            "failed"
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn finish_task_fenced(
        &self,
        db: &Db,
        job_id: i64,
        task_no: i64,
        ok: bool,
        code: &str,
        message: Option<&str>,
        now: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<&'static str, String> {
        if !self.replicated() {
            self.note_local("finish_task");
            return db
                .finish_task_fenced(job_id, task_no, ok, code, message, now, max_attempts, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("finish_task")?;
        self.finish_locked(
            db,
            job_id,
            task_no,
            ok,
            code,
            message,
            now,
            max_attempts,
            fence,
        )
        .await
    }
}
