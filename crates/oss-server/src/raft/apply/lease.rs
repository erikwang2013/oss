//! 调度租约（`schedule_lease` 单例行）的确定性 apply（feature 7）。
//!
//! 一条命令同时覆盖 acquire / renew / takeover：`now` 由提交方携带，apply 不取墙钟。
//! 同 term 同节点只能延长 expiry（acquired_at 不变）；同 term 换节点是内部矛盾；
//! 换 term 必须等旧租约过期才允许接管。任何不合法转换 → `Conflict` 零写入。
//! 租约 term 单调，旧 term 的迟到命令永远不会覆盖新 leader 的租约。

use super::{ControlWrite, invariant};
use crate::raft::command::ControlResponse;
use crate::raft::runstate::{ScheduleLeaseRow, lease_row_from};

/// 当前行 + 日志行的确定性校验；逐字段相同的重复 apply 是 no-op。
pub(crate) fn lease_row_ok(
    cur: Option<&ScheduleLeaseRow>,
    next: &ScheduleLeaseRow,
    now: i64,
) -> Result<(), String> {
    if next.node <= 0 || next.term <= 0 {
        return Err(format!(
            "schedule lease: invalid owner {}/{}",
            next.term, next.node
        ));
    }
    if next.expires_at <= now {
        return Err("schedule lease: expiry must be in the future".into());
    }
    let Some(cur) = cur else {
        return Ok(());
    };
    if next == cur {
        return Ok(());
    }
    if next.term < cur.term {
        return Err(format!(
            "schedule lease: term regressed {} < {}",
            next.term, cur.term
        ));
    }
    if next.term == cur.term {
        if next.node != cur.node {
            return Err(format!(
                "schedule lease: term {} owner changed {} -> {}",
                cur.term, cur.node, next.node
            ));
        }
        if next.acquired_at != cur.acquired_at {
            return Err("schedule lease: renewal must preserve acquired_at".into());
        }
        if next.expires_at <= cur.expires_at {
            return Err("schedule lease: renewal must extend expiry".into());
        }
        return Ok(());
    }
    // 更高 term：只有旧租约已过期才能接管（防旧 leader 在途调度）。
    if now < cur.expires_at {
        return Err(format!(
            "schedule lease: takeover before expiry (now {now} < {})",
            cur.expires_at
        ));
    }
    Ok(())
}

async fn current(cw: &mut ControlWrite<'_>) -> Result<Option<ScheduleLeaseRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT term, node, expires_at, acquired_at FROM schedule_lease WHERE singleton = 1",
    )
    .fetch_optional(&mut *cw.conn)
    .await?;
    Ok(row.as_ref().map(lease_row_from))
}

async fn upsert(cw: &mut ControlWrite<'_>, lease: &ScheduleLeaseRow) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO schedule_lease (singleton, term, node, expires_at, acquired_at) \
         VALUES (1, ?, ?, ?, ?) \
         ON CONFLICT(singleton) DO UPDATE SET term = excluded.term, node = excluded.node, \
          expires_at = excluded.expires_at, acquired_at = excluded.acquired_at",
    )
    .bind(lease.term)
    .bind(lease.node)
    .bind(lease.expires_at)
    .bind(lease.acquired_at)
    .execute(&mut *cw.conn)
    .await?;
    Ok(result.rows_affected())
}

/// 只读预检 → 整行 upsert。预检失败 = 零写入的 `Conflict`。
pub(crate) async fn apply(
    cw: &mut ControlWrite<'_>,
    lease: &ScheduleLeaseRow,
    now: i64,
) -> Result<ControlResponse, sqlx::Error> {
    let cur = current(cw).await?;
    if lease_row_ok(cur.as_ref(), lease, now).is_err() {
        return Ok(ControlResponse::Conflict);
    }
    if upsert(cw, lease).await? != 1 {
        return Err(invariant("schedule_lease upsert affected 0 rows".into()));
    }
    Ok(ControlResponse::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(term: i64, node: i64, expires_at: i64, acquired_at: i64) -> ScheduleLeaseRow {
        ScheduleLeaseRow {
            term,
            node,
            expires_at,
            acquired_at,
        }
    }

    #[test]
    fn lease_gate_allows_only_extending_renewal_and_expired_takeover() {
        assert!(lease_row_ok(None, &lease(1, 1, 110, 100), 100).is_ok());
        assert!(lease_row_ok(None, &lease(0, 1, 110, 100), 100).is_err());
        assert!(lease_row_ok(None, &lease(1, 0, 110, 100), 100).is_err());
        assert!(lease_row_ok(None, &lease(1, 1, 100, 100), 100).is_err());
        let cur = lease(1, 1, 110, 100);
        assert!(
            lease_row_ok(Some(&cur), &cur, 105).is_ok(),
            "重复 apply 幂等"
        );
        assert!(
            lease_row_ok(Some(&cur), &lease(1, 1, 120, 100), 105).is_ok(),
            "同 term 续约延长 expiry"
        );
        assert!(
            lease_row_ok(Some(&cur), &lease(1, 1, 110, 105), 105).is_err(),
            "续约不得重置 acquired_at"
        );
        assert!(
            lease_row_ok(Some(&cur), &lease(1, 1, 110, 100), 105).is_ok(),
            "不延长 expiry 的相同行按幂等处理"
        );
        assert!(
            lease_row_ok(Some(&cur), &lease(1, 2, 120, 100), 105).is_err(),
            "同 term 不换 owner"
        );
        assert!(
            lease_row_ok(Some(&cur), &lease(2, 2, 120, 105), 105).is_err(),
            "旧租约未过期不得接管"
        );
        assert!(
            lease_row_ok(Some(&cur), &lease(2, 2, 120, 110), 110).is_ok(),
            "到期后允许更高 term 接管"
        );
        assert!(
            lease_row_ok(Some(&cur), &lease(0, 2, 120, 110), 110).is_err(),
            "term 不得回退"
        );
    }
}
