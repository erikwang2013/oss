//! 运行态行模型（M8-2a）。
//!
//! 7 张运行表 + `schedule_lease` + `runstate_meta` 全部逐列镜像真实 SQLite 列；
//! 行结构体全部 `deny_unknown_fields`（扩展格式 1 已冻结）。加/删列必须同时升
//! `m8.runstate` 版本，`pragma_table_info` 对照测试会先失败。读写实现见
//! [`super::store`]。

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::raft::compat::{FEATURE_LEVEL_3, MIN_FEATURE_LEVEL};
use crate::raft::data::DataError;

pub use xl_store::control_store::inventory::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow,
};
pub use xl_store::control_store::jobs::{JobRow, JobTaskRow, ScheduleLeaseRow};
pub use xl_store::control_store::lifecycle::LifecycleCursorRow;
pub use xl_store::control_store::tier::TierRestoreRow;

/// 九个运行态表的完整载荷。`next_*_id == 0` 表示未初始化（`runstate_meta` 无行）；
/// `RunStateInit` 播种后恒 ≥1。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RunState {
    pub jobs: Vec<JobRow>,
    pub job_tasks: Vec<JobTaskRow>,
    pub inventory_runs: Vec<InventoryRunRow>,
    pub inventory_cursors: Vec<InventoryCursorRow>,
    pub inventory_incremental: Vec<InventoryIncrementalRow>,
    pub tier_restores: Vec<TierRestoreRow>,
    pub lifecycle_cursors: Vec<LifecycleCursorRow>,
    pub schedule_lease: Option<ScheduleLeaseRow>,
    pub next_job_id: i64,
    pub next_run_id: i64,
}

impl RunState {
    /// 无任何运行态（含未播种的序列）：空运行态可留在 OSSCTL1/V2，不必升 level 3。
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
            && self.job_tasks.is_empty()
            && self.inventory_runs.is_empty()
            && self.inventory_cursors.is_empty()
            && self.inventory_incremental.is_empty()
            && self.tier_restores.is_empty()
            && self.lifecycle_cursors.is_empty()
            && self.schedule_lease.is_none()
            && self.next_job_id == 0
            && self.next_run_id == 0
    }

    /// 非空运行态只在 feature 3（OSSCTL3）激活后存在，装封套/导出时必须拒绝更低级别。
    pub fn required_feature_level(&self) -> u8 {
        if self.is_empty() {
            MIN_FEATURE_LEVEL
        } else {
            FEATURE_LEVEL_3
        }
    }

    /// 语义校验（安装/导出校验在 DELETE/写行之前调用）：
    /// 序列号必须高于现存最大 id（0 = 未初始化的空态），作业引用不得悬空。
    pub fn validate(&self) -> Result<(), DataError> {
        if self.next_job_id < 0 || self.next_run_id < 0 {
            return Err(DataError::Format(format!(
                "runstate next ids must be 0 or positive (job={}, run={})",
                self.next_job_id, self.next_run_id
            )));
        }
        // 非空运行态必须已初始化：否则 level-3 的 id 分配器会从 0 开始与行冲突。
        if !self.is_empty() && (self.next_job_id == 0 || self.next_run_id == 0) {
            return Err(DataError::Format(format!(
                "non-empty runstate requires seeded next ids (job={}, run={})",
                self.next_job_id, self.next_run_id
            )));
        }
        check_sequence(
            "next_job_id",
            self.next_job_id,
            self.jobs.iter().map(|j| j.id).max(),
        )?;
        check_sequence(
            "next_run_id",
            self.next_run_id,
            self.inventory_runs.iter().map(|r| r.id).max(),
        )?;

        let job_ids: HashSet<i64> = self.jobs.iter().map(|j| j.id).collect();
        for j in &self.jobs {
            if let Some(parent) = j.parent_job_id
                && !job_ids.contains(&parent)
            {
                return Err(DataError::Format(format!(
                    "runstate jobs {} references missing parent_job_id {parent}",
                    j.id
                )));
            }
        }
        for t in &self.job_tasks {
            if !job_ids.contains(&t.job_id) {
                return Err(DataError::Format(format!(
                    "runstate job_tasks ({}, {}) references missing jobs.id {}",
                    t.job_id, t.task_no, t.job_id
                )));
            }
        }
        for r in &self.inventory_runs {
            if let Some(job_id) = r.job_id
                && !job_ids.contains(&job_id)
            {
                return Err(DataError::Format(format!(
                    "runstate inventory_runs {} references missing jobs.id {job_id}",
                    r.id
                )));
            }
        }
        for t in &self.tier_restores {
            if let Some(job_id) = t.job_id
                && !job_ids.contains(&job_id)
            {
                return Err(DataError::Format(format!(
                    "runstate tier_restores {}/{} references missing jobs.id {job_id}",
                    t.bucket, t.key
                )));
            }
        }
        Ok(())
    }
}

fn check_sequence(name: &str, next: i64, max: Option<i64>) -> Result<(), DataError> {
    if let Some(max) = max
        && next <= max
    {
        return Err(DataError::Format(format!(
            "runstate {name} {next} must exceed max existing id {max}"
        )));
    }
    Ok(())
}
