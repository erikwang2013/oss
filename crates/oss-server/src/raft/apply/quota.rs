//! 账户用量账本 apply（feature 9）：全部判定只依赖命令值与同一事务内的 SQL 行，
//! 不含时钟/随机；并发预留由状态机串行 apply 保证恰好一个越过配额边界。
//!
//! 命令非幂等：apply 成功即改动账本，append 结果不明时调用方不得重试。
//! 行缺失时用命令携带的 baseline 播种（leader 从本节点 `account_usage` 缓存取值），
//! 因此所有副本按同一个值初始化，不依赖各自缓存。

use super::ControlWrite;
use crate::raft::command::{ControlCommand, ControlResponse, valid_quota_ledger_args};
use xl_store::control_store::account::AccountTxn;

enum Mode {
    Reserve,
    Release,
    Adjust,
}

async fn load_ledger(
    cw: &mut ControlWrite<'_>,
    account: &str,
) -> Result<Option<(i64, i64)>, sqlx::Error> {
    crate::control_store::SqliteControlTxn::new(cw.conn())
        .account_ledger(account)
        .await
        .map_err(crate::control_store::to_sqlx)
}

async fn load_limits(
    cw: &mut ControlWrite<'_>,
    account: &str,
) -> Result<(Option<i64>, Option<i64>), sqlx::Error> {
    let row = crate::control_store::SqliteControlTxn::new(cw.conn())
        .account_hard_limits(account)
        .await
        .map_err(crate::control_store::to_sqlx)?;
    let (bytes, objects) = row.unwrap_or((None, None));
    Ok((bytes.filter(|q| *q >= 0), objects.filter(|q| *q >= 0)))
}

async fn write_ledger(
    cw: &mut ControlWrite<'_>,
    account: &str,
    bytes: i64,
    objects: i64,
) -> Result<(), sqlx::Error> {
    crate::control_store::SqliteControlTxn::new(cw.conn())
        .account_ledger_upsert(account, bytes, objects)
        .await
        .map_err(crate::control_store::to_sqlx)
}

/// 账本命令入口；非本组命令返回 `None`。
pub(super) async fn apply(
    cw: &mut ControlWrite<'_>,
    cmd: &ControlCommand,
) -> Result<Option<ControlResponse>, sqlx::Error> {
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
    let (mut used, mut count) = match load_ledger(cw, account).await? {
        Some(v) => v,
        None => (baseline_bytes, baseline_objects),
    };
    match mode {
        Mode::Reserve => {
            let (limit_bytes, limit_objects) = load_limits(cw, account).await?;
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
    write_ledger(cw, account, used, count).await?;
    Ok(Some(ControlResponse::Ok))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reserve_limit_release_and_adjust() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY, hard_byte_quota INTEGER, \
               object_quota INTEGER);
             CREATE TABLE account_usage_ledger (account_id TEXT PRIMARY KEY, \
               used_bytes INTEGER NOT NULL, object_count INTEGER NOT NULL);
             INSERT INTO accounts VALUES ('a1', 10, 2);",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let reserve = |bytes, objects| ControlCommand::AccountUsageReserve {
            account: "a1".into(),
            bytes,
            objects,
            baseline_bytes: 0,
            baseline_objects: 0,
        };
        let mut cw = super::super::control_tx(&mut conn);
        assert_eq!(
            apply(&mut cw, &reserve(6, 1)).await.unwrap(),
            Some(ControlResponse::Ok)
        );
        // 并发边界：第二次同量预留超限 → Invalid，账本不变。
        assert_eq!(
            apply(&mut cw, &reserve(5, 1)).await.unwrap(),
            Some(ControlResponse::Invalid)
        );
        let release = ControlCommand::AccountUsageRelease {
            account: "a1".into(),
            bytes: 6,
            objects: 1,
            baseline_bytes: 0,
            baseline_objects: 0,
        };
        assert_eq!(
            apply(&mut cw, &release).await.unwrap(),
            Some(ControlResponse::Ok)
        );
        let adjust = ControlCommand::AccountUsageAdjust {
            account: "a1".into(),
            bytes: -100,
            objects: -100,
            baseline_bytes: 0,
            baseline_objects: 0,
        };
        assert_eq!(
            apply(&mut cw, &adjust).await.unwrap(),
            Some(ControlResponse::Ok)
        );
        assert_eq!(
            load_ledger(&mut cw, "a1").await.unwrap(),
            Some((0, 0)),
            "release/adjust saturate at zero"
        );
    }
}
