//! 运行态九表整表读写（M8-2a；模型见 [`super::model`]）。
//!
//! 读按主键排序（快照 checksum / 往返字节确定）；安装删除九表后按给定顺序插入，
//! 全程使用调用方事务——失败时由调用方回滚，绝不留下半套表。激活标记不由本模块
//! 写：来源级别决定 `m8_runstate_activated`（见 `compat::mark/clear_runstate_activated_tx`）。

use sqlx::{SqliteConnection, SqlitePool};

use super::model::RunState;
use crate::raft::data::DataError;
use xl_store::control_store::{ControlError, ControlTxn};

const META_NEXT_JOB_ID: &str = "next_job_id";
const META_NEXT_RUN_ID: &str = "next_run_id";

fn sql(e: sqlx::Error) -> DataError {
    DataError::Format(format!("runstate sql: {e}"))
}

fn control_err(e: ControlError) -> DataError {
    match e {
        ControlError::Invalid(m) => DataError::Format(m),
        ControlError::Backend(m) => DataError::Format(format!("control store backend: {m}")),
        ControlError::MissingOwner(id) => DataError::MissingUser(id),
    }
}

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

/// SQLite 包装。
pub async fn read_runstate_tx(conn: &mut SqliteConnection) -> Result<RunState, DataError> {
    let mut txn = crate::control_store::SqliteControlTxn::new(conn);
    read_runstate_txn(&mut txn).await.map_err(control_err)
}

/// 连接池便捷入口（校验/单次读）。快照或导出构建必须用 [`read_runstate_tx`] 和
/// 控制面同处一个读事务，避免撕裂读。
pub async fn read_runstate(pool: &SqlitePool) -> Result<RunState, DataError> {
    let mut conn = pool.acquire().await.map_err(sql)?;
    read_runstate_tx(&mut conn).await
}

/// 九表整表替换。先做语义校验（不通过则一行不写），再在 savepoint 内删除+插入：
/// 裸连接（自动提交）中途失败也只回滚本次安装；嵌套在调用方事务里同样安全。
pub async fn install_runstate_tx(
    conn: &mut SqliteConnection,
    state: &RunState,
) -> Result<(), DataError> {
    state.validate()?;
    sqlx::query("SAVEPOINT install_runstate")
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    let result = {
        let mut txn = crate::control_store::SqliteControlTxn::new(&mut *conn);
        install_runstate_rows(&mut txn, state)
            .await
            .map_err(|e| DataError::Format(e.to_string()))
    };
    match result {
        Ok(()) => {
            sqlx::query("RELEASE SAVEPOINT install_runstate")
                .execute(&mut *conn)
                .await
                .map_err(sql)?;
            Ok(())
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK TO SAVEPOINT install_runstate")
                .execute(&mut *conn)
                .await;
            let _ = sqlx::query("RELEASE SAVEPOINT install_runstate")
                .execute(&mut *conn)
                .await;
            Err(e)
        }
    }
}

/// 后端无关的九表整表替换（调用方持有事务/根 CAS）。
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
