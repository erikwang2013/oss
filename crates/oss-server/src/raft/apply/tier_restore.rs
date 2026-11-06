//! Tier restore 后续状态（`tier_restores`）的确定性 apply（feature 7）。
//!
//! 受理时的 pending 行随 JobBatch 复制；本模块复制之后的 restored/回滚 pending 与
//! 清理删除。与 lifecycle 游标同语义：先只读预检，不合法 → `Conflict` 且零写入；
//! 通过后整行 upsert，且必须恰好影响 1 行。fence 不回退（行被更高 term 接管后旧
//! 日志不得降级）；删除是幂等盲写，`fence_term` 上限让旧 term 的迟到删除 no-op。

use super::{current_restore, invariant, restore_row_ok};
use xl_store::control_store::{ControlError, ControlTxn};

use crate::raft::command::ControlResponse;
use crate::raft::runstate::TierRestoreRow;

async fn upsert(txn: &mut dyn ControlTxn, r: &TierRestoreRow) -> Result<bool, ControlError> {
    txn.tier_restore_upsert(r).await
}

/// 只读预检 → 整行 upsert。预检失败 = 零写入的 `Conflict`。
pub(crate) async fn apply(
    txn: &mut dyn ControlTxn,
    restore: &TierRestoreRow,
) -> Result<ControlResponse, ControlError> {
    let cur = current_restore(txn, &restore.bucket, &restore.key, &restore.version_id).await?;
    if restore_row_ok(cur.as_ref(), restore).is_err() {
        return Ok(ControlResponse::Conflict);
    }
    if !upsert(txn, restore).await? {
        return Err(invariant(format!(
            "tier restore {}/{}/{} upsert affected 0 rows",
            restore.bucket, restore.key, restore.version_id
        )));
    }
    Ok(ControlResponse::Ok)
}

/// 幂等删除；只删 `fence_term` 不高于命令 token 的行。
pub(crate) async fn delete(
    txn: &mut dyn ControlTxn,
    bucket: &str,
    key: &str,
    version_id: &str,
    fence_term: i64,
) -> Result<ControlResponse, ControlError> {
    txn.tier_restore_delete(bucket, key, version_id, fence_term)
        .await?;
    Ok(ControlResponse::Ok)
}
