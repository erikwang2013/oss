//! 作业行/任务行的回收、抢占与管理员状态操作（M8-2；主门面见
//! [`super::JobState`]）。全部经同一 `commit` 提交，保证与引擎写同序。

use super::*;

impl JobState {
    pub async fn requeue_task(
        &self,
        db: &Db,
        job_id: i64,
        task_no: i64,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<bool, String> {
        if !self.replicated() {
            self.note_local("requeue_task");
            return db
                .requeue_task(job_id, task_no, now, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("requeue_task")?;
        let Some(mut task) = Self::read_task(db, job_id, task_no).await? else {
            return Ok(false);
        };
        if task.state != "running" || task.fence_term != fence.term_i64() {
            return Ok(false);
        }
        task.state = "queued".into();
        task.started_at = None;
        task.deadline_at = None;
        task.next_at = now;
        self.commit(db, Vec::new(), vec![task], Vec::new()).await?;
        Ok(true)
    }

    pub async fn set_task_deadline(
        &self,
        db: &Db,
        job_id: i64,
        task_no: i64,
        deadline_at: i64,
        fence: FencingEpoch,
    ) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("set_task_deadline");
            return db
                .set_task_deadline(job_id, task_no, deadline_at, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("set_task_deadline")?;
        let Some(mut task) = Self::read_task(db, job_id, task_no).await? else {
            return Ok(());
        };
        if task.state != "running" || task.fence_term != fence.term_i64() {
            return Ok(());
        }
        task.deadline_at = Some(deadline_at);
        self.commit(db, Vec::new(), vec![task], Vec::new()).await
    }

    pub async fn job_state(&self, db: &Db, id: i64) -> Result<Option<String>, String> {
        Ok(Self::read_job(db, id).await?.map(|j| j.state))
    }

    /// 超时回收：接管（写当前 fence）并按一次失败收尾；旧 holder 的迟到收尾被 term 闸拒绝。
    pub async fn recover_expired_tasks(
        &self,
        db: &Db,
        now: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<usize, String> {
        if !self.replicated() {
            self.note_local("recover_expired_tasks");
            return db
                .recover_expired_tasks(now, max_attempts, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("recover_expired_tasks")?;
        let rows = db
            .control_store()
            .recover_expired_task_ids(now, fence.term_i64())
            .await
            .map_err(err)?;
        for (job_id, task_no) in &rows {
            self.finish_locked(
                db,
                *job_id,
                *task_no,
                false,
                "Timeout",
                Some("task deadline exceeded"),
                now,
                max_attempts,
                fence,
            )
            .await?;
        }
        Ok(rows.len())
    }

    pub async fn recover_stale_tasks(
        &self,
        db: &Db,
        cutoff: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<usize, String> {
        if !self.replicated() {
            self.note_local("recover_stale_tasks");
            return db
                .recover_stale_tasks(cutoff, max_attempts, fence)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("recover_stale_tasks")?;
        let now = crate::db::now();
        let rows = db
            .control_store()
            .recover_stale_task_ids(cutoff, fence.term_i64())
            .await
            .map_err(err)?;
        for (job_id, task_no) in &rows {
            self.finish_locked(
                db,
                *job_id,
                *task_no,
                false,
                "Timeout",
                Some("process restart recovery"),
                now,
                max_attempts,
                fence,
            )
            .await?;
        }
        Ok(rows.len())
    }

    /// 无任务 running 行回收。
    pub async fn recover_orphan_jobs(
        &self,
        db: &Db,
        cutoff: i64,
        now: i64,
    ) -> Result<usize, String> {
        if !self.replicated() {
            self.note_local("recover_orphan_jobs");
            return db.recover_orphan_jobs(cutoff, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("recover_orphan_jobs")?;
        let rows = db
            .control_store()
            .orphan_running_jobs(cutoff)
            .await
            .map_err(err)?;
        let mut jobs = Vec::new();
        for mut job in rows {
            job.state = "error".into();
            if job.error.is_none() {
                job.error = Some("进程崩溃遗留的 running 作业，已自动回收".into());
            }
            job.finished_at = Some(now);
            bump(&mut job, now);
            jobs.push(job);
        }
        let n = jobs.len();
        self.commit(db, jobs, Vec::new(), Vec::new()).await?;
        Ok(n)
    }

    pub async fn preempt_lower_priority(
        &self,
        db: &Db,
        capacity: usize,
        now: i64,
    ) -> Result<usize, String> {
        if !self.replicated() {
            self.note_local("preempt_lower_priority");
            return db.preempt_lower_priority(capacity, now).await.map_err(err);
        }
        if capacity == 0 {
            return Ok(0);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("preempt_lower_priority")?;
        let running = db.control_store().running_task_count().await.map_err(err)?;
        if running < capacity as i64 {
            return Ok(0);
        }
        let rows = db
            .control_store()
            .preemptable_running_jobs(now)
            .await
            .map_err(err)?;
        let mut jobs = Vec::new();
        for mut job in rows {
            job.state = "preempted".into();
            bump(&mut job, now);
            jobs.push(job);
        }
        let n = jobs.len();
        self.commit(db, jobs, Vec::new(), Vec::new()).await?;
        Ok(n)
    }

    pub async fn release_preempted(&self, db: &Db, now: i64) -> Result<usize, String> {
        if !self.replicated() {
            self.note_local("release_preempted");
            return db.release_preempted(now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("release_preempted")?;
        let rows = db
            .control_store()
            .releasable_preempted_jobs(now)
            .await
            .map_err(err)?;
        let mut jobs = Vec::new();
        for mut job in rows {
            job.state = "queued".into();
            bump(&mut job, now);
            jobs.push(job);
        }
        let n = jobs.len();
        self.commit(db, jobs, Vec::new(), Vec::new()).await?;
        Ok(n)
    }

    /// 控制台 pause/resume/cancel。取消时同批落任务行条（queued → cancelled）。
    pub async fn job_set_state(
        &self,
        db: &Db,
        id: i64,
        action: &str,
        now: i64,
    ) -> Result<bool, String> {
        if !self.replicated() {
            self.note_local("job_set_state");
            return db.job_set_state(id, action, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("job_set_state")?;
        let Some(mut job) = Self::read_job(db, id).await? else {
            return Ok(false);
        };
        let mut tasks = Vec::new();
        match action {
            "pause" if matches!(job.state.as_str(), "running" | "preempted") => {
                job.state = "paused".into();
            }
            "resume" if job.state == "paused" => {
                job.state = "running".into();
            }
            "cancel"
                if matches!(
                    job.state.as_str(),
                    "queued" | "running" | "paused" | "preempted"
                ) =>
            {
                job.state = "cancelled".into();
                let rows = db
                    .control_store()
                    .queued_tasks_for_job(id)
                    .await
                    .map_err(err)?;
                for mut task in rows {
                    task.state = "cancelled".into();
                    task.finished_at = Some(now);
                    tasks.push(task);
                }
            }
            _ => return Ok(false),
        }
        bump(&mut job, now);
        self.commit(db, vec![job], tasks, Vec::new()).await?;
        Ok(true)
    }

    pub async fn job_set_error(&self, db: &Db, id: i64, msg: &str, now: i64) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("job_set_error");
            return db.job_set_error(id, msg, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("job_set_error")?;
        let Some(mut job) = Self::read_job(db, id).await? else {
            return Ok(());
        };
        job.error = Some(msg.to_string());
        bump(&mut job, now);
        self.commit(db, vec![job], Vec::new(), Vec::new()).await
    }

    /// 长作业心跳（只续 running 行）。
    pub async fn job_heartbeat(&self, db: &Db, id: i64, now: i64) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("job_heartbeat");
            return db.job_heartbeat(id, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("job_heartbeat")?;
        let Some(mut job) = Self::read_job(db, id).await? else {
            return Ok(());
        };
        if job.state == "running" {
            bump(&mut job, now);
            self.commit(db, vec![job], Vec::new(), Vec::new()).await?;
        }
        Ok(())
    }

    /// 长作业开单（lifecycle 等只写 jobs 行的作业）。
    pub async fn job_start(
        &self,
        db: &Db,
        kind: &str,
        bucket: &str,
        prefix: &str,
        now: i64,
    ) -> Result<i64, String> {
        if !self.replicated() {
            self.note_local("job_start");
            return db.job_start(kind, bucket, prefix, now).await.map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.leader_ready("job_start").await?;
        let id = Self::next_job_id(db).await?;
        let job = JobRow {
            id,
            kind: kind.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            state: "running".into(),
            created_at: now,
            updated_at: now,
            started_at: Some(now),
            ..JobRow::default()
        };
        self.commit(db, vec![job], Vec::new(), Vec::new()).await?;
        Ok(id)
    }

    /// 长作业收尾（done/error + 计数 + 可选错误）。
    #[allow(clippy::too_many_arguments)]
    pub async fn job_finish(
        &self,
        db: &Db,
        id: i64,
        state: &str,
        scanned: i64,
        deleted: i64,
        skipped: i64,
        error: Option<&str>,
        now: i64,
    ) -> Result<(), String> {
        if !self.replicated() {
            self.note_local("job_finish");
            return db
                .job_finish(id, state, scanned, deleted, skipped, error, now)
                .await
                .map_err(err);
        }
        let _guard = self.lock.lock().await;
        self.require_leader("job_finish")?;
        let Some(mut job) = Self::read_job(db, id).await? else {
            return Ok(());
        };
        job.state = state.to_string();
        job.scanned = scanned;
        job.deleted = deleted;
        job.skipped = skipped;
        // error=None 保留已落库的错误（apply 禁止清空；收尾不得整批被拒）。
        if let Some(e) = error {
            job.error = Some(e.to_string());
        }
        bump(&mut job, now);
        self.commit(db, vec![job], Vec::new(), Vec::new()).await
    }
}
