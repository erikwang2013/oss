//! Read side of the jobs domain for the object backend.

use xl_store::control_store::ControlError;
use xl_store::control_store::jobs::{JobRow, JobStore, JobTaskRow, ScheduleLeaseRow};

use super::jobs::{
    LEASE_KEY, all_jobs_store, all_tasks_store, backend, decode, get_store, inflight, job_key,
    runstate_meta_store, same_owner, take_limit, task_key,
};
use super::{ObjectControlStore, TableId};

#[async_trait::async_trait]
impl JobStore for ObjectControlStore {
    async fn job(&self, id: i64) -> Result<Option<JobRow>, ControlError> {
        match get_store(self, TableId::Jobs, &job_key(id)).await? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn job_task(
        &self,
        job_id: i64,
        task_no: i64,
    ) -> Result<Option<JobTaskRow>, ControlError> {
        match get_store(self, TableId::JobTasks, &task_key(job_id, task_no)).await? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn job_tasks(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError> {
        let mut rows: Vec<_> = all_tasks_store(self)
            .await?
            .into_iter()
            .filter(|t| t.job_id == job_id)
            .collect();
        rows.sort_by_key(|t| t.task_no);
        Ok(rows)
    }

    async fn failed_job_tasks(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError> {
        let mut rows: Vec<_> = all_tasks_store(self)
            .await?
            .into_iter()
            .filter(|t| t.job_id == job_id && t.state == "failed")
            .collect();
        rows.sort_by_key(|t| t.task_no);
        Ok(rows)
    }

    async fn jobs_to_promote(&self) -> Result<Vec<i64>, ControlError> {
        let jobs = all_jobs_store(self).await?;
        let tasks = all_tasks_store(self).await?;
        Ok(jobs
            .into_iter()
            .filter(|j| {
                j.state == "queued"
                    && tasks.iter().any(|t| {
                        t.job_id == j.id && matches!(t.state.as_str(), "queued" | "running")
                    })
            })
            .map(|j| j.id)
            .collect())
    }

    async fn queued_task_candidate(
        &self,
        now: i64,
        fence_term: i64,
        queued_jobs_allowed: bool,
    ) -> Result<Option<JobTaskRow>, ControlError> {
        let jobs = all_jobs_store(self).await?;
        let tasks = all_tasks_store(self).await?;
        Ok(tasks
            .into_iter()
            .filter(|t| t.state == "queued" && t.next_at <= now && t.fence_term <= fence_term)
            .filter(|t| {
                jobs.iter().any(|j| {
                    j.id == t.job_id
                        && (j.state == "running" || (queued_jobs_allowed && j.state == "queued"))
                })
            })
            .min_by_key(|t| {
                let priority = jobs
                    .iter()
                    .find(|j| j.id == t.job_id)
                    .map(|j| j.priority)
                    .unwrap_or(0);
                (-priority, t.next_at, t.task_no, t.job_id)
            }))
    }

    async fn active_task_count(
        &self,
        job_id: i64,
        exclude: Option<(i64, i64)>,
    ) -> Result<i64, ControlError> {
        Ok(all_tasks_store(self)
            .await?
            .into_iter()
            .filter(|t| {
                t.job_id == job_id
                    && matches!(t.state.as_str(), "queued" | "running")
                    && exclude.is_none_or(|(_, no)| t.task_no != no)
            })
            .count() as i64)
    }

    async fn job_inflight_counts(
        &self,
        requested_by: Option<i64>,
    ) -> Result<(i64, i64), ControlError> {
        let jobs = all_jobs_store(self).await?;
        let global = jobs.iter().filter(|j| inflight(&j.state)).count() as i64;
        let user = requested_by
            .map(|uid| {
                jobs.iter()
                    .filter(|j| j.requested_by == Some(uid) && inflight(&j.state))
                    .count() as i64
            })
            .unwrap_or(0);
        Ok((user, global))
    }

    async fn recent_jobs(&self, limit: i64) -> Result<Vec<JobRow>, ControlError> {
        let mut jobs = all_jobs_store(self).await?;
        jobs.sort_by_key(|j| std::cmp::Reverse(j.id));
        take_limit(&mut jobs, limit);
        Ok(jobs)
    }

    async fn job_tasks_page(
        &self,
        job_id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<JobTaskRow>, ControlError> {
        let mut rows: Vec<_> = all_tasks_store(self)
            .await?
            .into_iter()
            .filter(|t| t.job_id == job_id)
            .collect();
        rows.sort_by_key(|t| t.task_no);
        let offset = offset.max(0) as usize;
        if offset >= rows.len() {
            return Ok(Vec::new());
        }
        rows.drain(..offset);
        take_limit(&mut rows, limit);
        Ok(rows)
    }

    async fn terminal_jobs_before(
        &self,
        cutoff: i64,
        limit: i64,
    ) -> Result<Vec<i64>, ControlError> {
        let mut ids: Vec<i64> = all_jobs_store(self)
            .await?
            .into_iter()
            .filter(|j| matches!(j.state.as_str(), "done" | "error" | "cancelled"))
            .filter(|j| j.updated_at < cutoff)
            .map(|j| j.id)
            .collect();
        ids.sort_unstable();
        take_limit(&mut ids, limit);
        Ok(ids)
    }

    async fn dedupe_keys(&self) -> Result<Vec<String>, ControlError> {
        Ok(all_tasks_store(self)
            .await?
            .into_iter()
            .filter_map(|t| t.dedupe_key)
            .collect())
    }

    async fn dedupe_taken(
        &self,
        key: &str,
        job_id: i64,
        task_no: i64,
    ) -> Result<bool, ControlError> {
        Ok(all_tasks_store(self).await?.iter().any(|t| {
            t.dedupe_key.as_deref() == Some(key) && (t.job_id != job_id || t.task_no != task_no)
        }))
    }

    async fn schedule_lease(&self) -> Result<Option<ScheduleLeaseRow>, ControlError> {
        match get_store(self, TableId::ScheduleLease, &LEASE_KEY).await? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn runstate_meta(&self, key: &str) -> Result<Option<i64>, ControlError> {
        runstate_meta_store(self, key).await
    }

    async fn max_job_id(&self) -> Result<i64, ControlError> {
        Ok(all_jobs_store(self)
            .await?
            .last()
            .map(|j| j.id)
            .unwrap_or(0))
    }

    async fn max_inventory_run_id(&self) -> Result<i64, ControlError> {
        // The root allocator is the authoritative high-water mark; no object
        // inventory adapter is wired into this lane.
        let pinned = self.pin().await.map_err(backend)?;
        Ok(i64::try_from(pinned.root.meta.next_run_id.saturating_sub(1)).unwrap_or(i64::MAX))
    }

    async fn running_task_count(&self) -> Result<i64, ControlError> {
        Ok(all_tasks_store(self)
            .await?
            .iter()
            .filter(|t| t.state == "running")
            .count() as i64)
    }

    async fn recover_expired_task_ids(
        &self,
        now: i64,
        fence_term: i64,
    ) -> Result<Vec<(i64, i64)>, ControlError> {
        Ok(all_tasks_store(self)
            .await?
            .into_iter()
            .filter(|t| t.state == "running")
            .filter(|t| t.deadline_at.is_some_and(|d| d < now) && t.fence_term <= fence_term)
            .map(|t| (t.job_id, t.task_no))
            .collect())
    }

    async fn recover_stale_task_ids(
        &self,
        cutoff: i64,
        fence_term: i64,
    ) -> Result<Vec<(i64, i64)>, ControlError> {
        Ok(all_tasks_store(self)
            .await?
            .into_iter()
            .filter(|t| t.state == "running")
            .filter(|t| t.started_at.is_some_and(|s| s < cutoff) && t.fence_term <= fence_term)
            .map(|t| (t.job_id, t.task_no))
            .collect())
    }

    async fn orphan_running_jobs(&self, cutoff: i64) -> Result<Vec<JobRow>, ControlError> {
        let tasks = all_tasks_store(self).await?;
        Ok(all_jobs_store(self)
            .await?
            .into_iter()
            .filter(|j| j.state == "running" && j.updated_at <= cutoff)
            .filter(|j| !tasks.iter().any(|t| t.job_id == j.id))
            .collect())
    }

    async fn preemptable_running_jobs(&self, now: i64) -> Result<Vec<JobRow>, ControlError> {
        let jobs = all_jobs_store(self).await?;
        let tasks = all_tasks_store(self).await?;
        Ok(jobs
            .iter()
            .filter(|j| j.state == "running")
            .filter(|j| {
                tasks
                    .iter()
                    .any(|t| t.job_id == j.id && t.state == "queued")
            })
            .filter(|j| {
                jobs.iter().any(|h| {
                    matches!(h.state.as_str(), "queued" | "running")
                        && h.priority > j.priority
                        && same_owner(h.requested_by, j.requested_by)
                        && tasks
                            .iter()
                            .any(|t| t.job_id == h.id && t.state == "queued" && t.next_at <= now)
                })
            })
            .cloned()
            .collect())
    }

    async fn releasable_preempted_jobs(&self, now: i64) -> Result<Vec<JobRow>, ControlError> {
        let jobs = all_jobs_store(self).await?;
        let tasks = all_tasks_store(self).await?;
        Ok(jobs
            .iter()
            .filter(|j| j.state == "preempted")
            .filter(|j| {
                !jobs.iter().any(|h| {
                    h.id != j.id
                        && h.priority > j.priority
                        && same_owner(h.requested_by, j.requested_by)
                        && matches!(h.state.as_str(), "queued" | "running")
                        && tasks
                            .iter()
                            .any(|t| t.job_id == h.id && t.state == "queued" && t.next_at <= now)
                })
            })
            .cloned()
            .collect())
    }

    async fn queued_tasks_for_job(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError> {
        Ok(all_tasks_store(self)
            .await?
            .into_iter()
            .filter(|t| t.job_id == job_id && t.state == "queued")
            .collect())
    }
}
