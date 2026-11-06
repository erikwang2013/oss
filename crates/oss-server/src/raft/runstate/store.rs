//! 运行态九表整表读写（M8-2a；模型见 [`super::model`]）。
//!
//! 读按主键排序（快照 checksum / 往返字节确定）；安装删除九表后按给定顺序插入，
//! 全程使用调用方事务——失败时由调用方回滚，绝不留下半套表。激活标记不由本模块
//! 写：来源级别决定 `m8_runstate_activated`（见 `compat::mark/clear_runstate_activated_tx`）。

use super::model::RunState;
use xl_store::control_store::{ControlError, ControlTxn};

const META_NEXT_JOB_ID: &str = "next_job_id";
const META_NEXT_RUN_ID: &str = "next_run_id";

/// 调用方事务/连接内读取九表（行序 = 主键序，checksum 确定性来源）。
pub async fn read_runstate_txn(txn: &mut dyn ControlTxn) -> Result<RunState, ControlError> {
    let mut state = RunState::default();
    state.jobs = txn.runstate_jobs().await?;
    state.job_tasks = txn.runstate_job_tasks().await?;
    state.inventory_runs = txn.inventory_runs().await?;
    state.inventory_cursors = txn.inventory_cursors().await?;
    state.inventory_incremental = txn.inventory_incrementals().await?;
    state.tier_restores = txn.tier_restores().await?;
    state.lifecycle_cursors = txn.lifecycle_cursors().await?;
    state.schedule_lease = txn.snapshot_schedule_lease().await?;
    state.next_job_id = txn
        .snapshot_runstate_meta(META_NEXT_JOB_ID)
        .await?
        .unwrap_or_default();
    state.next_run_id = txn
        .snapshot_runstate_meta(META_NEXT_RUN_ID)
        .await?
        .unwrap_or_default();
    // runstate_meta 可能尚未播种（M8-1 本地写入/旧库）：快照与导出的序列号必须
    // 高于现存行，否则安装侧 `validate` 拒绝非空运行态。按行补齐，格式不变。
    state.next_job_id = state
        .next_job_id
        .max(state.jobs.iter().map(|j| j.id + 1).max().unwrap_or(0));
    state.next_run_id = state.next_run_id.max(
        state
            .inventory_runs
            .iter()
            .map(|r| r.id + 1)
            .max()
            .unwrap_or(0),
    );
    if !state.is_empty() {
        state.next_job_id = state.next_job_id.max(1);
        state.next_run_id = state.next_run_id.max(1);
    }
    Ok(state)
}

/// 九表整表替换：先做语义校验（不通过则一行不写），再由调用方事务/根 CAS 提交。
pub async fn install_runstate_rows(
    txn: &mut dyn ControlTxn,
    state: &RunState,
) -> Result<(), ControlError> {
    state
        .validate()
        .map_err(|e| ControlError::Invalid(e.to_string()))?;
    txn.runstate_clear().await?;

    // 显式 id 插入会把 AUTOINCREMENT 的 sqlite_sequence 追平到 max(id)，之后本地
    // 自增不会再分配到快照里已存在的 id。
    txn.insert_jobs(&state.jobs).await?;
    txn.insert_job_tasks(&state.job_tasks).await?;

    txn.replace_inventory_runs(&state.inventory_runs).await?;
    txn.replace_inventory_cursors(&state.inventory_cursors)
        .await?;
    txn.replace_inventory_incrementals(&state.inventory_incremental)
        .await?;
    txn.replace_tier_restores(&state.tier_restores).await?;
    txn.replace_lifecycle_cursors(&state.lifecycle_cursors)
        .await?;

    txn.insert_schedule_lease(state.schedule_lease.as_ref())
        .await?;
    txn.insert_runstate_meta(META_NEXT_JOB_ID, state.next_job_id)
        .await?;
    txn.insert_runstate_meta(META_NEXT_RUN_ID, state.next_run_id)
        .await?;
    // 激活标记由调用方按来源写/清：普通 level-3 快照不是日志事实。
    Ok(())
}
