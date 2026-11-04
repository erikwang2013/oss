//! jobs / job_tasks / schedule_lease / runstate_meta 域的行类型与后端中立 trait。
//!
//! 行字段声明顺序即 `RunState` 快照顺序（已发布快照不可改写）；增删列必须同时
//! 升 `m8.runstate` 版本。写 trait 只借用调用方事务，不 begin/commit/rollback。

use serde::{Deserialize, Serialize};

use super::ControlError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct JobRow {
    pub id: i64,
    pub kind: String,
    pub bucket: String,
    pub prefix: String,
    pub state: String,
    pub scanned: i64,
    pub deleted: i64,
    pub skipped: i64,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub spec_json: Option<String>,
    pub progress: i64,
    pub total: i64,
    pub succeeded: i64,
    pub failed: i64,
    pub report_bucket: Option<String>,
    pub report_prefix: Option<String>,
    pub priority: i64,
    pub requested_by: Option<i64>,
    pub parent_job_id: Option<i64>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct JobTaskRow {
    pub job_id: i64,
    pub task_no: i64,
    pub action: String,
    pub payload_json: String,
    pub state: String,
    pub attempt: i64,
    pub next_at: i64,
    pub dedupe_key: Option<String>,
    pub deadline_at: Option<i64>,
    pub result_code: Option<String>,
    pub result_string: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub fence_term: i64,
    pub fence_leader: i64,
}

/// 调度租约（`schedule_lease` 单例行；`singleton` 由适配器固定写 1）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ScheduleLeaseRow {
    pub term: i64,
    pub node: i64,
    pub expires_at: i64,
    pub acquired_at: i64,
}

/// 建作业的整行输入（自然键 + 初始状态；id 由后端分配或快照显式给出）。
#[derive(Debug, Clone, Default)]
pub struct NewJob {
    pub kind: String,
    pub bucket: String,
    pub prefix: String,
    pub state: String,
    pub total: i64,
    pub priority: i64,
    pub requested_by: Option<i64>,
    pub parent_job_id: Option<i64>,
    pub report_bucket: Option<String>,
    pub report_prefix: Option<String>,
    pub spec_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub started_at: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct NewTask {
    pub action: String,
    pub payload_json: String,
    pub dedupe_key: Option<String>,
    pub next_at: i64,
}

/// 领取结果（不暴露整行；行为与 `ClaimedTask` 一致）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobClaim {
    pub job_id: i64,
    pub task_no: i64,
    pub action: String,
    pub payload_json: String,
    pub attempt: i64,
}

/// 只读 jobs 域访问（console/worker/对账共用同一 SQL）。
#[async_trait::async_trait]
pub trait JobStore: Send + Sync {
    async fn job(&self, id: i64) -> Result<Option<JobRow>, ControlError>;
    async fn job_task(&self, job_id: i64, task_no: i64)
    -> Result<Option<JobTaskRow>, ControlError>;
    /// 整作业任务，`task_no` 升序。
    async fn job_tasks(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError>;
    async fn failed_job_tasks(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError>;
    async fn jobs_to_promote(&self) -> Result<Vec<i64>, ControlError>;
    /// 可领取任务；`queued_jobs_allowed` = 作业提升与任务领取同批提交（复制路径）。
    async fn queued_task_candidate(
        &self,
        now: i64,
        fence_term: i64,
        queued_jobs_allowed: bool,
    ) -> Result<Option<JobTaskRow>, ControlError>;
    /// `exclude` 用于收尾时排除当前任务。
    async fn active_task_count(
        &self,
        job_id: i64,
        exclude: Option<(i64, i64)>,
    ) -> Result<i64, ControlError>;
    /// `(该用户, 全局)` 的 inflight 作业数（queued/running/paused/preempted）。
    async fn job_inflight_counts(
        &self,
        requested_by: Option<i64>,
    ) -> Result<(i64, i64), ControlError>;
    /// `id` 降序。
    async fn recent_jobs(&self, limit: i64) -> Result<Vec<JobRow>, ControlError>;
    async fn job_tasks_page(
        &self,
        job_id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<JobTaskRow>, ControlError>;
    async fn terminal_jobs_before(&self, cutoff: i64, limit: i64)
    -> Result<Vec<i64>, ControlError>;
    async fn dedupe_keys(&self) -> Result<Vec<String>, ControlError>;
    async fn dedupe_taken(
        &self,
        key: &str,
        job_id: i64,
        task_no: i64,
    ) -> Result<bool, ControlError>;
    async fn schedule_lease(&self) -> Result<Option<ScheduleLeaseRow>, ControlError>;
    async fn runstate_meta(&self, key: &str) -> Result<Option<i64>, ControlError>;
    async fn max_job_id(&self) -> Result<i64, ControlError>;
    /// `next_run_id` 分配器的行上限（决策 8：本切片收口 allocator 的跨域读）。
    async fn max_inventory_run_id(&self) -> Result<i64, ControlError>;
    async fn running_task_count(&self) -> Result<i64, ControlError>;
    async fn recover_expired_task_ids(
        &self,
        now: i64,
        fence_term: i64,
    ) -> Result<Vec<(i64, i64)>, ControlError>;
    async fn recover_stale_task_ids(
        &self,
        cutoff: i64,
        fence_term: i64,
    ) -> Result<Vec<(i64, i64)>, ControlError>;
    async fn orphan_running_jobs(&self, cutoff: i64) -> Result<Vec<JobRow>, ControlError>;
    async fn preemptable_running_jobs(&self, now: i64) -> Result<Vec<JobRow>, ControlError>;
    async fn releasable_preempted_jobs(&self, now: i64) -> Result<Vec<JobRow>, ControlError>;
    async fn queued_tasks_for_job(&self, job_id: i64) -> Result<Vec<JobTaskRow>, ControlError>;
}

/// 调用方事务内的 jobs 域写访问；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait JobTxn: Send {
    /// INSERT 作业 + 任务（自动 id，`state='queued'`）。
    async fn job_create(&mut self, new: &NewJob, tasks: &[NewTask]) -> Result<i64, ControlError>;
    /// 任务 `INSERT OR IGNORE`；全部被去重 → `Ok(None)`（调用方回滚事务）。
    async fn job_create_dedup(
        &mut self,
        new: &NewJob,
        tasks: &[NewTask],
    ) -> Result<Option<i64>, ControlError>;
    /// Restore 受理：任务 `INSERT OR IGNORE`，`total` 保持入参任务数；返回 (id, 是否插入)。
    async fn job_create_restore(
        &mut self,
        new: &NewJob,
        tasks: &[NewTask],
    ) -> Result<(i64, bool), ControlError>;
    async fn job_start(
        &mut self,
        kind: &str,
        bucket: &str,
        prefix: &str,
        now: i64,
    ) -> Result<i64, ControlError>;
    #[allow(clippy::too_many_arguments)]
    async fn job_finish(
        &mut self,
        id: i64,
        state: &str,
        scanned: i64,
        deleted: i64,
        skipped: i64,
        error: Option<&str>,
        now: i64,
    ) -> Result<(), ControlError>;
    async fn job_set_progress(
        &mut self,
        job_id: i64,
        progress: i64,
        now: i64,
    ) -> Result<(), ControlError>;
    /// 建子作业（只含 failed 任务）；无父作业/无失败任务 → `Ok(None)`。
    async fn job_retry_failed(&mut self, id: i64, now: i64) -> Result<Option<i64>, ControlError>;
    /// 原子领取一条任务（调用方已 `BEGIN IMMEDIATE`）：提升 queued 作业，再按优先级取任务。
    async fn job_claim(
        &mut self,
        now: i64,
        timeout_secs: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<Option<JobClaim>, ControlError>;
    async fn task_set_deadline(
        &mut self,
        job_id: i64,
        task_no: i64,
        deadline_at: i64,
        fence_term: i64,
    ) -> Result<(), ControlError>;
    async fn requeue_task(
        &mut self,
        job_id: i64,
        task_no: i64,
        now: i64,
        fence_term: i64,
    ) -> Result<bool, ControlError>;
    async fn preempt_lower_priority(
        &mut self,
        capacity: usize,
        now: i64,
    ) -> Result<usize, ControlError>;
    async fn release_preempted(&mut self, now: i64) -> Result<usize, ControlError>;
    /// 任务终态/重试 + 作业收敛；返回任务新状态。
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
    ) -> Result<&'static str, ControlError>;
    async fn take_over_task(
        &mut self,
        job_id: i64,
        task_no: i64,
        started_before: Option<i64>,
        now: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError>;
    async fn recover_orphan_jobs(&mut self, cutoff: i64, now: i64) -> Result<usize, ControlError>;
    async fn job_heartbeat(&mut self, id: i64, now: i64) -> Result<(), ControlError>;
    async fn job_set_error(&mut self, id: i64, msg: &str, now: i64) -> Result<(), ControlError>;
    async fn job_set_state(
        &mut self,
        id: i64,
        action: &str,
        now: i64,
    ) -> Result<bool, ControlError>;
    /// 按主键 upsert（apply 用；返回受影响行数，调用方断言 ==1）。
    async fn job_upsert(&mut self, job: &JobRow) -> Result<u64, ControlError>;
    async fn job_task_upsert(&mut self, task: &JobTaskRow) -> Result<u64, ControlError>;
    async fn job(&mut self, id: i64) -> Result<Option<JobRow>, ControlError>;
    async fn job_task(
        &mut self,
        job_id: i64,
        task_no: i64,
    ) -> Result<Option<JobTaskRow>, ControlError>;
    /// `dedupe_key` 已被其他 `(job_id, task_no)` 占用（apply 校验用）。
    async fn dedupe_taken(
        &mut self,
        key: &str,
        job_id: i64,
        task_no: i64,
    ) -> Result<bool, ControlError>;
    /// 删除终态 jobs + tasks，并解除 inventory_runs/tier_restores/parent 引用。
    /// 返回 (删除行数, 存在但非终态的 id 数)。
    async fn purge_terminal_jobs(&mut self, ids: &[i64]) -> Result<(u64, usize), ControlError>;
    async fn schedule_lease_upsert(
        &mut self,
        lease: &ScheduleLeaseRow,
    ) -> Result<u64, ControlError>;
    /// `CAST(...) < CAST(...)` 单调推进（apply 确定）。
    async fn runstate_meta_next_run_id(&mut self, value: i64) -> Result<(), ControlError>;
    async fn schedule_lease(&mut self) -> Result<Option<ScheduleLeaseRow>, ControlError>;
}

/// 快照导出/安装（调用方事务内；顺序即字节确定性来源）。
#[async_trait::async_trait]
pub trait JobSnapshotTxn: Send {
    /// DELETE jobs、job_tasks、schedule_lease、runstate_meta（顺序固定）。
    async fn runstate_clear(&mut self) -> Result<(), ControlError>;
    /// 按 id 升序。
    async fn runstate_jobs(&mut self) -> Result<Vec<JobRow>, ControlError>;
    /// 按 `(job_id, task_no)` 升序。
    async fn runstate_job_tasks(&mut self) -> Result<Vec<JobTaskRow>, ControlError>;
    async fn insert_jobs(&mut self, rows: &[JobRow]) -> Result<(), ControlError>;
    async fn insert_job_tasks(&mut self, rows: &[JobTaskRow]) -> Result<(), ControlError>;
    async fn snapshot_schedule_lease(&mut self) -> Result<Option<ScheduleLeaseRow>, ControlError>;
    async fn insert_schedule_lease(
        &mut self,
        lease: Option<&ScheduleLeaseRow>,
    ) -> Result<(), ControlError>;
    async fn snapshot_runstate_meta(&mut self, key: &str) -> Result<Option<i64>, ControlError>;
    async fn insert_runstate_meta(&mut self, key: &str, value: i64) -> Result<(), ControlError>;
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    /// 旧快照行（与 `raft/runstate/model.rs` 迁移前声明顺序一致）。
    #[derive(Serialize)]
    struct LegacyJob {
        id: i64,
        kind: String,
        bucket: String,
        prefix: String,
        state: String,
        scanned: i64,
        deleted: i64,
        skipped: i64,
        error: Option<String>,
        created_at: i64,
        updated_at: i64,
        spec_json: Option<String>,
        progress: i64,
        total: i64,
        succeeded: i64,
        failed: i64,
        report_bucket: Option<String>,
        report_prefix: Option<String>,
        priority: i64,
        requested_by: Option<i64>,
        parent_job_id: Option<i64>,
        started_at: Option<i64>,
        finished_at: Option<i64>,
    }

    #[derive(Serialize)]
    struct LegacyTask {
        job_id: i64,
        task_no: i64,
        action: String,
        payload_json: String,
        state: String,
        attempt: i64,
        next_at: i64,
        dedupe_key: Option<String>,
        deadline_at: Option<i64>,
        result_code: Option<String>,
        result_string: Option<String>,
        started_at: Option<i64>,
        finished_at: Option<i64>,
        fence_term: i64,
        fence_leader: i64,
    }

    fn sample_job() -> JobRow {
        JobRow {
            id: 7,
            kind: "inventory".into(),
            bucket: "b".into(),
            prefix: "p/".into(),
            state: "running".into(),
            scanned: 1,
            deleted: 2,
            skipped: 3,
            error: Some("e".into()),
            created_at: 11,
            updated_at: 12,
            spec_json: Some("{}".into()),
            progress: 4,
            total: 5,
            succeeded: 6,
            failed: 7,
            report_bucket: Some("rb".into()),
            report_prefix: Some("rp".into()),
            priority: 8,
            requested_by: Some(9),
            parent_job_id: Some(10),
            started_at: Some(13),
            finished_at: Some(14),
        }
    }

    #[test]
    fn job_snapshot_bytes_match_legacy_field_order() {
        let job = sample_job();
        let legacy = LegacyJob {
            id: job.id,
            kind: job.kind.clone(),
            bucket: job.bucket.clone(),
            prefix: job.prefix.clone(),
            state: job.state.clone(),
            scanned: job.scanned,
            deleted: job.deleted,
            skipped: job.skipped,
            error: job.error.clone(),
            created_at: job.created_at,
            updated_at: job.updated_at,
            spec_json: job.spec_json.clone(),
            progress: job.progress,
            total: job.total,
            succeeded: job.succeeded,
            failed: job.failed,
            report_bucket: job.report_bucket.clone(),
            report_prefix: job.report_prefix.clone(),
            priority: job.priority,
            requested_by: job.requested_by,
            parent_job_id: job.parent_job_id,
            started_at: job.started_at,
            finished_at: job.finished_at,
        };
        assert_eq!(
            rmp_serde::to_vec(&job).unwrap(),
            rmp_serde::to_vec(&legacy).unwrap()
        );
    }

    #[test]
    fn job_task_snapshot_bytes_match_legacy_field_order() {
        let task = JobTaskRow {
            job_id: 1,
            task_no: 2,
            action: "delete".into(),
            payload_json: "{}".into(),
            state: "queued".into(),
            attempt: 3,
            next_at: 4,
            dedupe_key: Some("k".into()),
            deadline_at: Some(5),
            result_code: Some("OK".into()),
            result_string: Some("ok".into()),
            started_at: Some(6),
            finished_at: Some(7),
            fence_term: 8,
            fence_leader: 9,
        };
        let legacy = LegacyTask {
            job_id: task.job_id,
            task_no: task.task_no,
            action: task.action.clone(),
            payload_json: task.payload_json.clone(),
            state: task.state.clone(),
            attempt: task.attempt,
            next_at: task.next_at,
            dedupe_key: task.dedupe_key.clone(),
            deadline_at: task.deadline_at,
            result_code: task.result_code.clone(),
            result_string: task.result_string.clone(),
            started_at: task.started_at,
            finished_at: task.finished_at,
            fence_term: task.fence_term,
            fence_leader: task.fence_leader,
        };
        assert_eq!(
            rmp_serde::to_vec(&task).unwrap(),
            rmp_serde::to_vec(&legacy).unwrap()
        );
    }

    /// 编译期校验 trait object 可用于 `Arc<dyn JobStore>` / `Box<dyn JobTxn>`。
    #[test]
    fn traits_are_object_safe() {
        fn store(_: std::sync::Arc<dyn JobStore>) {}
        fn txn(_: Box<dyn JobTxn>) {}
        let _ = (store as fn(_), txn as fn(_));
    }
}
