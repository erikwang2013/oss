//! 账户用量账本 apply（feature 9）：全部判定只依赖命令值与同一事务内的 SQL 行，
//! 不含时钟/随机；并发预留由状态机串行 apply 保证恰好一个越过配额边界。
//!
//! 命令非幂等：apply 成功即改动账本，append 结果不明时调用方不得重试。
//! 行缺失时用命令携带的 baseline 播种（leader 从本节点 `account_usage` 缓存取值），
//! 因此所有副本按同一个值初始化，不依赖各自缓存。

use crate::raft::command::{ControlCommand, ControlResponse, valid_quota_ledger_args};
use xl_store::control_store::{ControlError, ControlTxn};

enum Mode {
    Reserve,
    Release,
    Adjust,
}

async fn load_ledger(
    txn: &mut dyn ControlTxn,
    account: &str,
) -> Result<Option<(i64, i64)>, ControlError> {
    txn.account_ledger(account).await
}

async fn load_limits(
    txn: &mut dyn ControlTxn,
    account: &str,
) -> Result<(Option<i64>, Option<i64>), ControlError> {
    let row = txn.account_hard_limits(account).await?;
    let (bytes, objects) = row.unwrap_or((None, None));
    Ok((bytes.filter(|q| *q >= 0), objects.filter(|q| *q >= 0)))
}

async fn write_ledger(
    txn: &mut dyn ControlTxn,
    account: &str,
    bytes: i64,
    objects: i64,
) -> Result<(), ControlError> {
    txn.account_ledger_upsert(account, bytes, objects).await
}

/// 账本命令入口；非本组命令返回 `None`。
pub(super) async fn apply(
    txn: &mut dyn ControlTxn,
    cmd: &ControlCommand,
) -> Result<Option<ControlResponse>, ControlError> {
    let (account, bytes, objects, baseline_bytes, baseline_objects, mode) = match cmd {
        ControlCommand::AccountUsageReserve {
            account,
            bytes,
            objects,
            baseline_bytes,
            baseline_objects,
        } => (
            account,
            *bytes,
            *objects,
            *baseline_bytes,
            *baseline_objects,
            Mode::Reserve,
        ),
        ControlCommand::AccountUsageRelease {
            account,
            bytes,
            objects,
            baseline_bytes,
            baseline_objects,
        } => (
            account,
            *bytes,
            *objects,
            *baseline_bytes,
            *baseline_objects,
            Mode::Release,
        ),
        ControlCommand::AccountUsageAdjust {
            account,
            bytes,
            objects,
            baseline_bytes,
            baseline_objects,
        } => (
            account,
            *bytes,
            *objects,
            *baseline_bytes,
            *baseline_objects,
            Mode::Adjust,
        ),
        _ => return Ok(None),
    };
    if !valid_quota_ledger_args(account, bytes, objects, baseline_bytes, baseline_objects) {
        return Ok(Some(ControlResponse::Invalid));
    }
    let (mut used, mut count) = match load_ledger(txn, account).await? {
        Some(v) => v,
        None => (baseline_bytes, baseline_objects),
    };
    match mode {
        Mode::Reserve => {
            let (limit_bytes, limit_objects) = load_limits(txn, account).await?;
            used = used.saturating_add(bytes).max(0);
            count = count.saturating_add(objects).max(0);
            if limit_bytes.is_some_and(|q| used > q) || limit_objects.is_some_and(|q| count > q) {
                // 稳定拒绝：ClusterQuota 侧把 Invalid 映射为 quota_exceeded。
                return Ok(Some(ControlResponse::Invalid));
            }
        }
        Mode::Release => {
            used = used.saturating_sub(bytes).max(0);
            count = count.saturating_sub(objects).max(0);
        }
        Mode::Adjust => {
            used = used.saturating_add(bytes).max(0);
            count = count.saturating_add(objects).max(0);
        }
    }
    write_ledger(txn, account, used, count).await?;
    Ok(Some(ControlResponse::Ok))
}

