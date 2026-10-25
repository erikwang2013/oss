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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct InventoryRunRow {
    pub id: i64,
    pub bucket: String,
    pub config_id: String,
    pub run_key: String,
    pub started_at: i64,
    pub state: String,
    pub job_id: Option<i64>,
    pub manifest_key: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub fence_term: i64,
    pub fence_leader: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct InventoryCursorRow {
    pub bucket: String,
    pub config_id: String,
    pub cursor: String,
    pub config_updated_at: i64,
    pub updated_at: i64,
    pub fence_term: i64,
    pub fence_leader: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct InventoryIncrementalRow {
    pub bucket: String,
    pub config_id: String,
    pub enabled: bool,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TierRestoreRow {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub expires_at: i64,
    pub job_id: Option<i64>,
    pub state: String,
    pub created_at: i64,
    pub fence_term: i64,
    pub fence_leader: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LifecycleCursorRow {
    pub bucket: String,
    pub rule_id: String,
    pub stage: String,
    pub cursor: String,
    pub updated_at: i64,
    pub fence_term: i64,
    pub fence_leader: i64,
}

/// 调度租约（`schedule_lease` 的单例行；`singleton` 由安装代码固定写 1）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ScheduleLeaseRow {
    pub term: i64,
    pub node: i64,
    pub expires_at: i64,
    pub acquired_at: i64,
}

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
