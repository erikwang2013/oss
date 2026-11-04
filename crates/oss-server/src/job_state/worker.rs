//! 任务领取与收尾（复制路径）。完整行经 `commit` 提交；apply 只做确定性 upsert。

use super::*;
use crate::db::ClaimedTask;

impl JobState {
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
        self.leader_ready("claim_task").await?;
        let promoted = db.control_store().jobs_to_promote().await.map_err(err)?;
        let mut jobs = Vec::new();
        for id in promoted {
            if let Some(mut job) = Self::read_job(db, id).await? {
                job.state = "running".into();
                job.started_at = Some(job.started_at.unwrap_or(now));
                bump(&mut job, now);
                jobs.push(job);
            }
        }
        // 作业提升与任务领取在同一批提交：这里必须接受仍为 queued 的作业（本批会提升）。
        let candidate = db
            .control_store()
            .queued_task_candidate(now, fence.term_i64(), true)
            .await
            .map_err(err)?;
        let Some(mut task) = candidate else {
            self.commit(db, jobs, Vec::new(), Vec::new()).await?;
            return Ok(None);
        };
        let job = match jobs.iter().position(|j| j.id == task.job_id) {
            Some(i) => jobs.remove(i),
            // 任务与作业同批提交，apply 前引用自洽；作业行仍需逻辑版本前进。
            None => {
                let mut job = Self::read_job(db, task.job_id)
                    .await?
                    .ok_or_else(|| format!("claim: job {} missing", task.job_id))?;
                bump(&mut job, now);
                job
            }
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
        self.commit(db, vec![job], vec![task], Vec::new()).await?;
        Ok(Some(claimed))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn finish_locked(
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
            let backoff = Db::retry_backoff_secs(attempt, now, job_id, task_no);
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
        bump(&mut job, now);
        let task_active = matches!(task.state.as_str(), "queued" | "running");
        let others = db
            .control_store()
            .active_task_count(job_id, Some((job_id, task_no)))
            .await
            .map_err(err)?;
        if !task_active && others == 0 && matches!(job.state.as_str(), "running" | "preempted") {
            job.state = if job.failed > 0 { "error" } else { "done" }.into();
            job.finished_at = Some(now);
        }
        self.commit(db, vec![job], vec![task], Vec::new()).await?;
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
