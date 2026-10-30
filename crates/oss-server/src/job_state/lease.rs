//! 调度租约 acquire / renew / takeover（feature 7）。
//!
//! 调度前先调用 [`JobState::schedule_fence`]：只有 leader 且拿到未过期租约才返回
//! fencing token。租约单行经 [`ControlCommand::ScheduleLeaseAcquire`] 复制；同 term
//! 同节点续约只延长 expiry，新 term 必须等旧租约过期后接管（apply 确定性判定）。
//! advertised feature<7 或非 Raft 时保持 M8-1 语义：直接返回 `LeaderGate` 的 epoch，
//! 不 append 任何命令。

use super::*;
use crate::raft::LeaderGate;
use crate::raft::apply::lease::lease_row_ok;
use crate::raft::runstate::{ScheduleLeaseRow, lease_row_from};

const LEASE_SECS_DEFAULT: i64 = 30;

/// `OSS_SCHEDULE_LEASE_SECS`（默认 30；非法/0 回落默认）。
pub fn schedule_lease_ttl() -> i64 {
    std::env::var("OSS_SCHEDULE_LEASE_SECS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(LEASE_SECS_DEFAULT)
}

impl JobState {
    async fn read_schedule_lease(db: &Db) -> Result<Option<ScheduleLeaseRow>, String> {
        let row = sqlx::query(
            "SELECT term, node, expires_at, acquired_at FROM schedule_lease WHERE singleton = 1",
        )
        .fetch_optional(db.pool())
        .await
        .map_err(err)?;
        Ok(row.as_ref().map(lease_row_from))
    }

    /// 调度放行 token：`None` = 现在不应调度（非 leader / 等待旧租约过期 / 生效级别不足）。
    ///
    /// `now`/`ttl_secs` 由调用方注入（apply 不取墙钟）；剩余租约 > 半 TTL 时不续约，
    /// 避免每个 worker tick 都写日志。租约命令的 apply 是幂等的，并发 tick 重复提交
    /// 同一转换也只会得到 `Ok`。
    pub async fn schedule_fence(
        &self,
        db: &Db,
        gate: &LeaderGate,
        now: i64,
        ttl_secs: i64,
    ) -> Result<Option<FencingEpoch>, String> {
        if !self.lease_replicated() {
            return Ok(gate.epoch());
        }
        let ttl = ttl_secs.max(1);
        let Some(control) = &self.control else {
            return Ok(gate.epoch());
        };
        let _guard = self.lock.lock().await;
        self.require_leader_for("schedule_lease", compat::FEATURE_LEVEL_7)?;
        let Some(handle) = control.raft_handle() else {
            return Ok(gate.epoch());
        };
        let Some(epoch) = handle.fencing_epoch() else {
            return Ok(None);
        };
        let term = epoch.term_i64();
        let cur = Self::read_schedule_lease(db).await?;
        if let Some(c) = &cur {
            if c.term > term {
                return Ok(None); // 租约 term 不可能回退；等待集群换届。
            }
            if c.term == term && c.node != epoch.leader_i64() {
                return Ok(None); // 同 term 双 leader 不可能；保守等待。
            }
            if c.term < term && now < c.expires_at {
                return Ok(None); // 接管必须等旧 leader 租约到期。
            }
        }
        let row = match &cur {
            Some(c) if c.term == term => {
                if now.saturating_add(ttl / 2) < c.expires_at {
                    return Ok(Some(epoch)); // 剩余超过半 TTL：不续约。
                }
                ScheduleLeaseRow {
                    term: c.term,
                    node: c.node,
                    expires_at: now.saturating_add(ttl),
                    acquired_at: c.acquired_at,
                }
            }
            _ => ScheduleLeaseRow {
                term,
                node: epoch.leader_i64(),
                expires_at: now.saturating_add(ttl),
                acquired_at: now,
            },
        };
        if lease_row_ok(cur.as_ref(), &row, now).is_err() {
            return Ok(None);
        }
        match control
            .write(ControlCommand::ScheduleLeaseAcquire { lease: row, now })
            .await
        {
            Ok(ControlResponse::Ok) => Ok(Some(epoch)),
            Ok(ControlResponse::Conflict) => Ok(None),
            Ok(other) => Err(format!("schedule lease rejected: {other:?}")),
            Err(e) => Err(e.to_string()),
        }
    }
}
