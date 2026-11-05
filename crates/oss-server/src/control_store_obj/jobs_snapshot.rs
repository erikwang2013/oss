//! `RunState` jobs tables: snapshot export/install in frozen field order.
//!
//! Export order is key order (jobs by id, tasks by `(job_id, task_no)`); install
//! writes explicit ids and advances the root allocator past every installed id.

use xl_store::control_store::ControlError;
use xl_store::control_store::jobs::{JobRow, JobSnapshotTxn, JobTaskRow, ScheduleLeaseRow};

use super::jobs::{
    LEASE_KEY, all_jobs_txn, all_tasks_txn, backend, decode, encode, job_key, meta_marker, task_key,
};
use super::{ObjectControlTxn, TableId};

fn invalid(e: impl std::fmt::Display) -> ControlError {
    ControlError::Invalid(e.to_string())
}

#[async_trait::async_trait]
impl JobSnapshotTxn for ObjectControlTxn {
    async fn runstate_clear(&mut self) -> Result<(), ControlError> {
        self.clear_runstate_tables().await
    }

    async fn runstate_jobs(&mut self) -> Result<Vec<JobRow>, ControlError> {
        all_jobs_txn(self).await
    }

    async fn runstate_job_tasks(&mut self) -> Result<Vec<JobTaskRow>, ControlError> {
        all_tasks_txn(self).await
    }

    async fn insert_jobs(&mut self, rows: &[JobRow]) -> Result<(), ControlError> {
        for row in rows {
            self.put(TableId::Jobs, &job_key(row.id), &encode(row)?)
                .await
                .map_err(backend)?;
            if row.id > 0 {
                self.meta_mut()
                    .observe_job_id(row.id as u64)
                    .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn insert_job_tasks(&mut self, rows: &[JobTaskRow]) -> Result<(), ControlError> {
        for row in rows {
            self.put(
                TableId::JobTasks,
                &task_key(row.job_id, row.task_no),
                &encode(row)?,
            )
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn snapshot_schedule_lease(&mut self) -> Result<Option<ScheduleLeaseRow>, ControlError> {
        match self
            .get(TableId::ScheduleLease, &LEASE_KEY)
            .await
            .map_err(backend)?
        {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn insert_schedule_lease(
        &mut self,
        lease: Option<&ScheduleLeaseRow>,
    ) -> Result<(), ControlError> {
        if let Some(lease) = lease {
            self.put(TableId::ScheduleLease, &LEASE_KEY, &encode(lease)?)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn snapshot_runstate_meta(&mut self, key: &str) -> Result<Option<i64>, ControlError> {
        self.meta()
            .markers
            .get(&meta_marker(key))
            .map(|v| {
                v.parse::<i64>()
                    .map_err(|e| ControlError::Backend(format!("runstate_meta {key}: {e}")))
            })
            .transpose()
    }

    async fn insert_runstate_meta(&mut self, key: &str, value: i64) -> Result<(), ControlError> {
        // 0 = unseeded; keep the meta row absent like the SQLite snapshot.
        if value <= 0 {
            return Ok(());
        }
        self.set_marker(meta_marker(key), value.to_string());
        let next = u64::try_from(value).map_err(invalid)?;
        match key {
            "next_job_id" => {
                let v = self.meta().next_job_id.max(next);
                self.meta_mut().next_job_id = v;
            }
            "next_run_id" => {
                let v = self.meta().next_run_id.max(next);
                self.meta_mut().next_run_id = v;
            }
            _ => {}
        }
        Ok(())
    }
}
