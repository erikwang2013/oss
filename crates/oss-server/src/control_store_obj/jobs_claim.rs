//! Claim, preempt, finish and recovery writes for the jobs domain.

use std::collections::BTreeMap;
use xl_store::control_store::ControlError;

use xl_store::control_store::jobs::{JobClaim, JobRow};

use super::ObjectControlTxn;
use super::jobs::{all_jobs_txn, all_tasks_txn, stage_job, stage_task, txn_job, txn_task};

impl ObjectControlTxn {
    pub(super) async fn do_job_claim(
        &mut self,
        now: i64,
        timeout_secs: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<Option<JobClaim>, ControlError> {
        let mut jobs: BTreeMap<i64, JobRow> = all_jobs_txn(self)
            .await?
            .into_iter()
            .map(|j| (j.id, j))
            .collect();
        let mut tasks = all_tasks_txn(self).await?;
        let mut promoted = Vec::new();
        for job in jobs.values_mut() {
            if job.state == "queued"
                && tasks
                    .iter()
                    .any(|t| t.job_id == job.id && matches!(t.state.as_str(), "queued" | "running"))
            {
                job.state = "running".into();
                job.started_at = Some(job.started_at.unwrap_or(now));
                job.updated_at = now;
                promoted.push(job.clone());
            }
        }
        for job in &promoted {
            stage_job(self, job).await?;
        }
        let Some(best) = tasks
            .iter()
            .filter(|t| t.state == "queued" && t.next_at <= now && t.fence_term <= fence_term)
            .filter(|t| jobs.get(&t.job_id).is_some_and(|j| j.state == "running"))
            .min_by_key(|t| {
                let priority = jobs.get(&t.job_id).map(|j| j.priority).unwrap_or(0);
                (-priority, t.next_at, t.task_no, t.job_id)
            })
            .cloned()
        else {
            return Ok(None);
        };
        let Some(task) = tasks
            .iter_mut()
            .find(|t| t.job_id == best.job_id && t.task_no == best.task_no)
        else {
            return Ok(None);
        };
        let attempt = task.attempt;
        task.state = "running".into();
        task.started_at = Some(now);
        task.deadline_at = Some(now + timeout_secs);
        task.fence_term = fence_term;
        task.fence_leader = fence_leader;
        let claimed = task.clone();
        stage_task(self, &claimed).await?;
        Ok(Some(JobClaim {
            job_id: best.job_id,
            task_no: best.task_no,
            action: best.action,
            payload_json: best.payload_json,
            attempt,
        }))
    }

    pub(super) async fn do_task_set_deadline(
        &mut self,
        job_id: i64,
        task_no: i64,
        deadline_at: i64,
        fence_term: i64,
    ) -> Result<(), ControlError> {
        if let Some(mut task) = txn_task(self, job_id, task_no).await?
            && task.state == "running"
            && task.fence_term == fence_term
        {
            task.deadline_at = Some(deadline_at);
            stage_task(self, &task).await?;
        }
        Ok(())
    }

    pub(super) async fn do_requeue_task(
        &mut self,
        job_id: i64,
        task_no: i64,
        now: i64,
        fence_term: i64,
    ) -> Result<bool, ControlError> {
        let Some(mut task) = txn_task(self, job_id, task_no).await? else {
            return Ok(false);
        };
        if task.state != "running" || task.fence_term != fence_term {
            return Ok(false);
        }
        task.state = "queued".into();
        task.started_at = None;
        task.deadline_at = None;
        task.next_at = now;
        stage_task(self, &task).await?;
        Ok(true)
    }

    pub(super) async fn do_preempt_lower_priority(
        &mut self,
        capacity: usize,
        now: i64,
    ) -> Result<usize, ControlError> {
        if capacity == 0 {
            return Ok(0);
        }
        let jobs = all_jobs_txn(self).await?;
        let tasks = all_tasks_txn(self).await?;
        let running = tasks.iter().filter(|t| t.state == "running").count();
        if running < capacity {
            return Ok(0);
        }
        let mut count = 0;
        for mut job in jobs.iter().filter(|j| j.state == "running").cloned() {
            let has_queued = tasks
                .iter()
                .any(|t| t.job_id == job.id && t.state == "queued");
            let has_higher = jobs.iter().any(|h| {
                matches!(h.state.as_str(), "queued" | "running")
                    && h.priority > job.priority
                    && h.requested_by == job.requested_by
                    && tasks
                        .iter()
                        .any(|t| t.job_id == h.id && t.state == "queued" && t.next_at <= now)
            });
            if has_queued && has_higher {
                job.state = "preempted".into();
                job.updated_at = now;
                stage_job(self, &job).await?;
                count += 1;
            }
        }
        Ok(count)
    }

    pub(super) async fn do_release_preempted(&mut self, now: i64) -> Result<usize, ControlError> {
        let jobs = all_jobs_txn(self).await?;
        let tasks = all_tasks_txn(self).await?;
        let mut count = 0;
        for mut job in jobs.iter().filter(|j| j.state == "preempted").cloned() {
            let blocked = jobs.iter().any(|h| {
                h.id != job.id
                    && h.priority > job.priority
                    && h.requested_by == job.requested_by
                    && matches!(h.state.as_str(), "queued" | "running")
                    && tasks
                        .iter()
                        .any(|t| t.job_id == h.id && t.state == "queued" && t.next_at <= now)
            });
            if !blocked {
                job.state = "queued".into();
                job.updated_at = now;
                stage_job(self, &job).await?;
                count += 1;
            }
        }
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn do_finish_task(
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
        let Some(mut task) = txn_task(self, job_id, task_no).await? else {
            return Ok("missing");
        };
        if task.state != "running" {
            return Ok("not-running");
        }
        if task.fence_term > fence_term {
            return Ok("fenced");
        }
        let attempt = task.attempt;
        let retry = !ok && attempt + 1 < max_attempts;
        if ok {
            task.state = "done".into();
            task.result_code = Some(code.into());
            task.result_string = message.map(str::to_owned);
            task.finished_at = Some(now);
            task.dedupe_key = None;
        } else if retry {
            task.state = "queued".into();
            task.attempt = attempt + 1;
            task.next_at = now + crate::db::Db::retry_backoff_secs(attempt, now, job_id, task_no);
            task.result_code = Some(code.into());
            task.result_string = message.map(str::to_owned);
            task.started_at = None;
            task.deadline_at = None;
        } else {
            task.state = "failed".into();
            task.attempt = attempt + 1;
            task.result_code = Some("PermanentFailure".into());
            task.result_string = message.map(str::to_owned);
            task.finished_at = Some(now);
            task.dedupe_key = None;
        }
        stage_task(self, &task).await?;

        let tasks = all_tasks_txn(self).await?;
        if let Some(mut job) = txn_job(self, job_id).await? {
            if ok {
                job.succeeded += 1;
                job.progress += 1;
                job.updated_at = now;
            } else if retry {
                job.updated_at = now;
            } else {
                job.failed += 1;
                job.progress += 1;
                job.updated_at = now;
            }
            let active = tasks
                .iter()
                .any(|t| t.job_id == job_id && matches!(t.state.as_str(), "queued" | "running"));
            if matches!(job.state.as_str(), "running" | "preempted") && !active {
                job.state = if job.failed > 0 { "error" } else { "done" }.into();
                job.finished_at = Some(now);
                job.updated_at = now;
            }
            stage_job(self, &job).await?;
        }
        Ok(if ok {
            "done"
        } else if retry {
            "queued"
        } else {
            "failed"
        })
    }

    pub(super) async fn do_take_over_task(
        &mut self,
        job_id: i64,
        task_no: i64,
        started_before: Option<i64>,
        now: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError> {
        let Some(mut task) = txn_task(self, job_id, task_no).await? else {
            return Ok(());
        };
        let stale = match started_before {
            Some(before) => task.started_at.is_some_and(|s| s < before),
            None => task.deadline_at.is_some_and(|d| d < now),
        };
        if task.state == "running" && stale && task.fence_term <= fence_term {
            task.fence_term = fence_term;
            task.fence_leader = fence_leader;
            stage_task(self, &task).await?;
        }
        Ok(())
    }

    pub(super) async fn do_recover_orphan_jobs(
        &mut self,
        cutoff: i64,
        now: i64,
    ) -> Result<usize, ControlError> {
        let tasks = all_tasks_txn(self).await?;
        let mut count = 0;
        for mut job in all_jobs_txn(self).await? {
            if job.state == "running"
                && job.updated_at <= cutoff
                && !tasks.iter().any(|t| t.job_id == job.id)
            {
                job.state = "error".into();
                job.error
                    .get_or_insert_with(|| "进程崩溃遗留的 running 作业，已自动回收".into());
                job.finished_at = Some(now);
                job.updated_at = now;
                stage_job(self, &job).await?;
                count += 1;
            }
        }
        Ok(count)
    }

    pub(super) async fn do_job_heartbeat(&mut self, id: i64, now: i64) -> Result<(), ControlError> {
        if let Some(mut job) = txn_job(self, id).await?
            && job.state == "running"
        {
            job.updated_at = now;
            stage_job(self, &job).await?;
        }
        Ok(())
    }

    pub(super) async fn do_job_set_error(
        &mut self,
        id: i64,
        msg: &str,
        now: i64,
    ) -> Result<(), ControlError> {
        if let Some(mut job) = txn_job(self, id).await? {
            job.error = Some(msg.into());
            job.updated_at = now;
            stage_job(self, &job).await?;
        }
        Ok(())
    }

    pub(super) async fn do_job_set_state(
        &mut self,
        id: i64,
        action: &str,
        now: i64,
    ) -> Result<bool, ControlError> {
        let Some(mut job) = txn_job(self, id).await? else {
            return Ok(false);
        };
        let allowed = match action {
            "pause" => matches!(job.state.as_str(), "running" | "preempted"),
            "resume" => job.state == "paused",
            "cancel" => matches!(
                job.state.as_str(),
                "queued" | "running" | "paused" | "preempted"
            ),
            _ => false,
        };
        if !allowed {
            return Ok(false);
        }
        job.state = match action {
            "pause" => "paused",
            "resume" => "running",
            _ => "cancelled",
        }
        .into();
        job.updated_at = now;
        stage_job(self, &job).await?;
        if action == "cancel" {
            for mut task in all_tasks_txn(self).await? {
                if task.job_id == id && task.state == "queued" {
                    task.state = "cancelled".into();
                    task.finished_at = Some(now);
                    stage_task(self, &task).await?;
                }
            }
        }
        Ok(true)
    }
}
