//! jobs domain writes except claim/preempt/finish (see `jobs_claim.rs`).

use std::collections::BTreeSet;

use xl_store::control_store::ControlError;
use xl_store::control_store::inventory::InventoryRunRow;
use xl_store::control_store::jobs::{
    JobClaim, JobRow, JobTaskRow, JobTxn, NewJob, NewTask, ScheduleLeaseRow,
};
use xl_store::control_store::tier::TierRestoreRow;

use super::jobs::{
    LEASE_KEY, RUNSTATE_META_KEYS, all_jobs_txn, all_tasks_txn, backend, blank_job, decode, encode,
    ensure_dedupe_free, insert_or_ignore, job_key, meta_marker, next_job_id, queued_task, scan_txn,
    stage_job, stage_task, stage_tasks, task_key, txn_job, txn_task,
};
use super::{ObjectControlTxn, TableId};

#[async_trait::async_trait]
impl JobTxn for ObjectControlTxn {
    async fn job_create(&mut self, new: &NewJob, tasks: &[NewTask]) -> Result<i64, ControlError> {
        ensure_dedupe_free(self, tasks).await?;
        let id = next_job_id(self)?;
        stage_job(self, &blank_job(id, new)).await?;
        let all: Vec<usize> = (0..tasks.len()).collect();
        stage_tasks(self, id, tasks, &all).await?;
        Ok(id)
    }

    async fn job_create_dedup(
        &mut self,
        new: &NewJob,
        tasks: &[NewTask],
    ) -> Result<Option<i64>, ControlError> {
        let indexes = insert_or_ignore(self, tasks).await?;
        if indexes.is_empty() {
            // SQLite inserts then relies on the caller's rollback; staging
            // nothing is the same committed outcome without a rollback.
            return Ok(None);
        }
        let id = next_job_id(self)?;
        let mut job = blank_job(id, new);
        if indexes.len() != tasks.len() {
            job.total = indexes.len() as i64;
        }
        stage_job(self, &job).await?;
        stage_tasks(self, id, tasks, &indexes).await?;
        Ok(Some(id))
    }

    async fn job_create_restore(
        &mut self,
        new: &NewJob,
        tasks: &[NewTask],
    ) -> Result<(i64, bool), ControlError> {
        let indexes = insert_or_ignore(self, tasks).await?;
        let id = next_job_id(self)?;
        stage_job(self, &blank_job(id, new)).await?;
        stage_tasks(self, id, tasks, &indexes).await?;
        Ok((id, !indexes.is_empty()))
    }

    async fn job_start(
        &mut self,
        kind: &str,
        bucket: &str,
        prefix: &str,
        now: i64,
    ) -> Result<i64, ControlError> {
        let id = next_job_id(self)?;
        let job = JobRow {
            id,
            kind: kind.into(),
            bucket: bucket.into(),
            prefix: prefix.into(),
            state: "running".into(),
            created_at: now,
            updated_at: now,
            ..JobRow::default()
        };
        stage_job(self, &job).await?;
        Ok(id)
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
        if let Some(mut job) = txn_job(self, id).await? {
            job.state = state.into();
            job.scanned = scanned;
            job.deleted = deleted;
            job.skipped = skipped;
            if error.is_some() {
                job.error = error.map(str::to_owned);
            }
            job.updated_at = now;
            stage_job(self, &job).await?;
        }
        Ok(())
    }

    async fn job_set_progress(
        &mut self,
        job_id: i64,
        progress: i64,
        now: i64,
    ) -> Result<(), ControlError> {
        if let Some(mut job) = txn_job(self, job_id).await? {
            job.progress = progress;
            job.updated_at = now;
            stage_job(self, &job).await?;
        }
        Ok(())
    }

    async fn job_retry_failed(&mut self, id: i64, now: i64) -> Result<Option<i64>, ControlError> {
        let Some(parent) = txn_job(self, id).await? else {
            return Ok(None);
        };
        let failed: Vec<JobTaskRow> = all_tasks_txn(self)
            .await?
            .into_iter()
            .filter(|t| t.job_id == id && t.state == "failed")
            .collect();
        if failed.is_empty() {
            return Ok(None);
        }
        let new_id = next_job_id(self)?;
        let job = JobRow {
            id: new_id,
            kind: parent.kind,
            bucket: parent.bucket,
            prefix: parent.prefix,
            state: "queued".into(),
            total: failed.len() as i64,
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
        stage_job(self, &job).await?;
        for (i, old) in failed.iter().enumerate() {
            let mut task = queued_task(
                new_id,
                i as i64 + 1,
                &NewTask {
                    action: old.action.clone(),
                    payload_json: old.payload_json.clone(),
                    dedupe_key: None,
                    next_at: now,
                },
                now,
            );
            task.next_at = now;
            stage_task(self, &task).await?;
        }
        Ok(Some(new_id))
    }

    async fn job_upsert(&mut self, job: &JobRow) -> Result<u64, ControlError> {
        stage_job(self, job).await?;
        Ok(1)
    }

    async fn job_task_upsert(&mut self, task: &JobTaskRow) -> Result<u64, ControlError> {
        let mut task = task.clone();
        if matches!(task.state.as_str(), "done" | "failed" | "cancelled") {
            task.dedupe_key = None;
        }
        stage_task(self, &task).await?;
        Ok(1)
    }

    async fn job(&mut self, id: i64) -> Result<Option<JobRow>, ControlError> {
        txn_job(self, id).await
    }

    async fn job_task(
        &mut self,
        job_id: i64,
        task_no: i64,
    ) -> Result<Option<JobTaskRow>, ControlError> {
        txn_task(self, job_id, task_no).await
    }

    async fn dedupe_taken(
        &mut self,
        key: &str,
        job_id: i64,
        task_no: i64,
    ) -> Result<bool, ControlError> {
        Ok(all_tasks_txn(self).await?.iter().any(|t| {
            t.dedupe_key.as_deref() == Some(key) && (t.job_id != job_id || t.task_no != task_no)
        }))
    }

    async fn purge_terminal_jobs(&mut self, ids: &[i64]) -> Result<(u64, usize), ControlError> {
        let jobs = all_jobs_txn(self).await?;
        let wanted: BTreeSet<i64> = ids.iter().copied().collect();
        let found: Vec<&JobRow> = jobs.iter().filter(|j| wanted.contains(&j.id)).collect();
        let terminal: BTreeSet<i64> = found
            .iter()
            .filter(|j| matches!(j.state.as_str(), "done" | "error" | "cancelled"))
            .map(|j| j.id)
            .collect();
        let non_terminal = found.len() - terminal.len();
        if terminal.is_empty() {
            return Ok((0, non_terminal));
        }
        for (table, is_inventory) in [
            (TableId::InventoryRuns, true),
            (TableId::TierRestores, false),
        ] {
            for (key, value) in scan_txn(self, table).await? {
                let replacement = if is_inventory {
                    let mut row: InventoryRunRow = decode(&value)?;
                    if row.job_id.is_some_and(|id| terminal.contains(&id)) {
                        row.job_id = None;
                        Some(encode(&row)?)
                    } else {
                        None
                    }
                } else {
                    let mut row: TierRestoreRow = decode(&value)?;
                    if row.job_id.is_some_and(|id| terminal.contains(&id)) {
                        row.job_id = None;
                        Some(encode(&row)?)
                    } else {
                        None
                    }
                };
                if let Some(bytes) = replacement {
                    self.put(table, &key, &bytes).await.map_err(backend)?;
                }
            }
        }
        for mut job in all_jobs_txn(self).await? {
            if job.parent_job_id.is_some_and(|id| terminal.contains(&id)) {
                job.parent_job_id = None;
                stage_job(self, &job).await?;
            }
        }
        for task in all_tasks_txn(self).await? {
            if terminal.contains(&task.job_id) {
                self.delete(TableId::JobTasks, &task_key(task.job_id, task.task_no))
                    .await
                    .map_err(backend)?;
            }
        }
        let mut deleted = 0u64;
        for id in &terminal {
            deleted += u64::from(
                self.delete(TableId::Jobs, &job_key(*id))
                    .await
                    .map_err(backend)?,
            );
        }
        Ok((deleted, non_terminal))
    }

    async fn schedule_lease_upsert(
        &mut self,
        lease: &ScheduleLeaseRow,
    ) -> Result<u64, ControlError> {
        self.put(TableId::ScheduleLease, &LEASE_KEY, &encode(lease)?)
            .await
            .map_err(backend)?;
        Ok(1)
    }

    async fn runstate_meta_next_run_id(&mut self, value: i64) -> Result<(), ControlError> {
        let marker = meta_marker("next_run_id");
        let current = self
            .meta()
            .markers
            .get(&marker)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        if value > current {
            self.set_marker(marker, value.to_string());
        }
        let next = self
            .meta()
            .next_run_id
            .max(current.max(value).max(1) as u64);
        self.meta_mut().next_run_id = next;
        Ok(())
    }

    async fn schedule_lease(&mut self) -> Result<Option<ScheduleLeaseRow>, ControlError> {
        match self
            .get(TableId::ScheduleLease, &LEASE_KEY)
            .await
            .map_err(backend)?
        {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn job_claim(
        &mut self,
        now: i64,
        timeout_secs: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<Option<JobClaim>, ControlError> {
        self.do_job_claim(now, timeout_secs, fence_term, fence_leader)
            .await
    }

    async fn task_set_deadline(
        &mut self,
        job_id: i64,
        task_no: i64,
        deadline_at: i64,
        fence_term: i64,
    ) -> Result<(), ControlError> {
        self.do_task_set_deadline(job_id, task_no, deadline_at, fence_term)
            .await
    }

    async fn requeue_task(
        &mut self,
        job_id: i64,
        task_no: i64,
        now: i64,
        fence_term: i64,
    ) -> Result<bool, ControlError> {
        self.do_requeue_task(job_id, task_no, now, fence_term).await
    }

    async fn preempt_lower_priority(
        &mut self,
        capacity: usize,
        now: i64,
    ) -> Result<usize, ControlError> {
        self.do_preempt_lower_priority(capacity, now).await
    }

    async fn release_preempted(&mut self, now: i64) -> Result<usize, ControlError> {
        self.do_release_preempted(now).await
    }

    #[allow(clippy::too_many_arguments)]
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
        self.do_finish_task(
            job_id,
            task_no,
            ok,
            code,
            message,
            now,
            max_attempts,
            fence_term,
        )
        .await
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
        self.do_take_over_task(
            job_id,
            task_no,
            started_before,
            now,
            fence_term,
            fence_leader,
        )
        .await
    }

    async fn recover_orphan_jobs(&mut self, cutoff: i64, now: i64) -> Result<usize, ControlError> {
        self.do_recover_orphan_jobs(cutoff, now).await
    }

    async fn job_heartbeat(&mut self, id: i64, now: i64) -> Result<(), ControlError> {
        self.do_job_heartbeat(id, now).await
    }

    async fn job_set_error(&mut self, id: i64, msg: &str, now: i64) -> Result<(), ControlError> {
        self.do_job_set_error(id, msg, now).await
    }

    async fn job_set_state(
        &mut self,
        id: i64,
        action: &str,
        now: i64,
    ) -> Result<bool, ControlError> {
        self.do_job_set_state(id, action, now).await
    }
}

impl ObjectControlTxn {
    pub(super) async fn clear_runstate_tables(&mut self) -> Result<(), ControlError> {
        for table in [TableId::Jobs, TableId::JobTasks, TableId::ScheduleLease] {
            for (key, _) in scan_txn(self, table).await? {
                self.delete(table, &key).await.map_err(backend)?;
            }
        }
        for key in RUNSTATE_META_KEYS {
            self.remove_marker(&meta_marker(key));
        }
        Ok(())
    }
}
