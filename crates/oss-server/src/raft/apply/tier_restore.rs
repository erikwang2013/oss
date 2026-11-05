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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::apply::{apply_command, control_tx};
    use crate::raft::command::ControlCommand;

    async fn apply_committed(db: &crate::db::Db, cmd: &ControlCommand) -> ControlResponse {
        let mut tx = db.pool().begin().await.unwrap();
        let response = {
            let mut cw = control_tx(&mut tx);
            apply_command(&mut cw, cmd).await.unwrap().response
        };
        tx.commit().await.unwrap();
        response
    }

    fn row(state: &str, expires_at: i64, fence_term: i64) -> TierRestoreRow {
        TierRestoreRow {
            bucket: "b".into(),
            key: "k".into(),
            version_id: "v".into(),
            expires_at,
            job_id: None,
            state: state.into(),
            created_at: 1,
            fence_term,
            fence_leader: 1,
        }
    }

    #[tokio::test]
    async fn upsert_is_idempotent_and_fence_monotonic() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        let upsert = |r: TierRestoreRow| ControlCommand::TierRestoreUpsert { restore: r };
        assert_eq!(
            apply_committed(&db, &upsert(row("pending", 0, 1))).await,
            ControlResponse::Ok
        );
        let pending = row("pending", 0, 1);
        assert_eq!(
            apply_committed(&db, &upsert(pending.clone())).await,
            ControlResponse::Ok,
            "逐字段相同重复 apply 幂等"
        );
        assert_eq!(
            apply_committed(&db, &upsert(row("restored", 100, 2))).await,
            ControlResponse::Ok
        );
        // 旧 term 的迟到状态：Conflict 且不覆盖 restored。
        assert_eq!(
            apply_committed(&db, &upsert(pending)).await,
            ControlResponse::Conflict
        );
        assert_eq!(
            apply_committed(&db, &upsert(row("bogus", 0, 3))).await,
            ControlResponse::Conflict
        );
        let (state, expires, term): (String, i64, i64) = sqlx::query_as(
            "SELECT state, expires_at, fence_term FROM tier_restores \
             WHERE bucket = 'b' AND key = 'k' AND version_id = 'v'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!((state.as_str(), expires, term), ("restored", 100, 2));
    }

    #[tokio::test]
    async fn delete_is_fence_bounded_and_idempotent() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        let upsert = ControlCommand::TierRestoreUpsert {
            restore: row("restored", 100, 2),
        };
        assert_eq!(apply_committed(&db, &upsert).await, ControlResponse::Ok);
        let delete = |fence_term| ControlCommand::TierRestoreDelete {
            bucket: "b".into(),
            key: "k".into(),
            version_id: "v".into(),
            fence_term,
        };
        assert_eq!(
            apply_committed(&db, &delete(1)).await,
            ControlResponse::Ok,
            "旧 term 删除 no-op"
        );
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tier_restores WHERE bucket = 'b' AND key = 'k'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(apply_committed(&db, &delete(2)).await, ControlResponse::Ok);
        assert_eq!(
            apply_committed(&db, &delete(2)).await,
            ControlResponse::Ok,
            "重复删除幂等"
        );
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM tier_restores WHERE bucket = 'b'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(count, 0);
    }
}
