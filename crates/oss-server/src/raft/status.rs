//! S5 运行时状态 JSON：`GET /api/v1/site/raft/status`（S6 路由接线）。
//!
//! `production_ready` 按计划 §2.6：少于 3 个 voter 只允许开发/测试，状态里显式标 false。
//! S7.1：`role` 是配置角色（voter/witness），运行态在 `state`；另报成员/leader 地址。
//!
//! `lag` = 本机已追加、尚未 apply 的日志条数（apply 积压；无值按 0，**不是**与 leader 的
//! 差距）。`stale` = 本节点非 leader 且 `lag > 0`，只说明本机有未 apply 的已追加日志。
//! 注意：分区/断连期间本节点收不到新日志，`lag` 仍为 0、`stale=false`，因此
//! **`stale=false` 不代表已追上 leader**，不能作为新鲜度保证。follower 的读是本机已
//! apply 状态，**不是**线性一致读。

use serde_json::{Value, json};

use super::compat;
use super::{RAFT_READY, RaftHandle, STRETCH_READY, SYNC_READY};

fn log_id_json(id: &openraft::LogId<u64>) -> Value {
    json!({ "term": id.leader_id.term, "index": id.index })
}

/// 本机 apply 积压：`stale` 仅表示非 leader 且本地有未 apply 日志；未收到的日志不计。
fn apply_backlog(
    is_leader: bool,
    last_log_index: Option<u64>,
    last_applied: Option<u64>,
) -> (u64, bool) {
    let lag = last_log_index
        .unwrap_or(0)
        .saturating_sub(last_applied.unwrap_or(0));
    (lag, !is_leader && lag > 0)
}

/// `configured` = `cfg.raft.is_some()`：未配置 `[raft]` 时能力位一律 false
/// （与 `/api/v1/site` 同口径），避免 console 在 M7 下显示 sync 可用。
pub fn status_json(handle: &RaftHandle, configured: bool) -> Value {
    let m = handle.raft().metrics();
    let m = m.borrow();
    let membership = m.membership_config.membership();
    let voters: Vec<u64> = m.membership_config.voter_ids().collect();
    let learners: Vec<u64> = membership.learner_ids().collect();
    let nodes: serde_json::Map<String, Value> = membership
        .nodes()
        .map(|(id, n)| (id.to_string(), Value::String(n.addr.clone())))
        .collect();
    let state = format!("{:?}", m.state).to_lowercase();
    // 与 [`super::LeaderGate`] 同口径：节点已停止（fatal）不算可执行 leader。
    let is_leader = m.running_state.is_ok() && m.current_leader == Some(m.id);
    let (lag, stale) = apply_backlog(
        is_leader,
        m.last_log_index,
        m.last_applied.as_ref().map(|l| l.index),
    );
    let leader_endpoint = m
        .current_leader
        .and_then(|l| membership.get_node(&l).map(|n| n.addr.clone()));
    let role = match handle.settings().role {
        crate::config::RaftRole::Voter => "voter",
        crate::config::RaftRole::Witness => "witness",
    };
    // leader 才计算生效等级：follower 没有当前 term 的 peer 记录，报 null 更诚实。
    let effective = is_leader.then(|| {
        compat::effective_feature_level(
            handle.settings().feature_level,
            m.id,
            &voters,
            &learners,
            m.current_term,
            handle.peers(),
        )
    });
    // 滚动升级诊断：peer 握手结果带 term（旧 term 记录只展示、不参与门控）。
    let peer_versions: serde_json::Map<String, Value> = handle
        .peers()
        .snapshot()
        .iter()
        .map(|(id, p)| {
            (
                id.to_string(),
                json!({"wire": p.wire, "feature": p.feature, "term": p.term}),
            )
        })
        .collect();
    json!({
        "enabled": m.running_state.is_ok(),
        "node_id": m.id,
        "role": role,
        "bootstrap": handle.settings().bootstrap,
        "initialized": !voters.is_empty(),
        "leader": m.current_leader,
        // M8-1：单例后台任务（作业/清单/tier/生命周期）只在本节点为 leader 时执行。
        "is_leader": is_leader,
        // M8-1 fencing token：状态转换按 (term, leader) 单调拒绝旧 leader 的写入。
        // null = 当前不可执行（follower/选举中/已停止）。
        "fencing_epoch": handle.fencing_epoch().map(|e| json!({"term": e.term, "leader": e.leader})),
        "fencing_token": handle.fencing_epoch().map(|e| format!("{}-{}", e.term, e.leader)),
        "leader_endpoint": leader_endpoint,
        "term": m.current_term,
        "state": state,
        "applied": m.last_applied.as_ref().map(log_id_json),
        // follower 读 = 本机 applied 状态（非线性一致）；lag 只是本地 apply 积压，
        // stale=false 不代表已追上 leader（未收到的日志不体现在 lag 中）。
        "lag": lag,
        "stale": stale,
        "snapshot": m.snapshot.as_ref().map(log_id_json),
        "purged": m.purged.as_ref().map(log_id_json),
        "voters": voters,
        "members": voters,
        "learners": learners,
        "nodes": nodes,
        "production_ready": voters.len() >= 3,
        // ---- Level B 滚动升级握手/诊断（纯增量字段；旧节点无这些键） ----
        "wire_version": compat::WIRE_VERSION,
        "feature_level": handle.advertised_feature(),
        "activation_level": handle.activation_level(),
        "effective_feature_level": effective.as_ref().map(|e| e.level),
        "compat": {
            "min_wire_version": compat::MIN_COMPAT_WIRE_VERSION,
            "min_membership_wire_version": compat::MIN_MEMBERSHIP_WIRE_VERSION,
            "peers": peer_versions,
            "mixed": handle.peers().mixed(m.current_term),
            "blocked_reason": effective.and_then(|e| e.blocked_reason),
        },
        "capabilities": {
            "raft": configured && RAFT_READY,
            "sync": configured && SYNC_READY,
            "stretch": configured && STRETCH_READY,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::apply_backlog;

    #[test]
    fn leader_backlog_is_not_stale() {
        assert_eq!(apply_backlog(true, Some(5), Some(3)), (2, false));
    }

    #[test]
    fn follower_backlog_is_stale() {
        assert_eq!(apply_backlog(false, Some(5), Some(3)), (2, true));
    }

    #[test]
    fn caught_up_follower_is_not_stale() {
        assert_eq!(apply_backlog(false, Some(5), Some(5)), (0, false));
    }

    #[test]
    fn missing_metrics_are_zero_and_not_stale() {
        assert_eq!(apply_backlog(false, None, None), (0, false));
    }

    #[test]
    fn applied_ahead_saturates_to_zero() {
        assert_eq!(apply_backlog(false, Some(2), Some(5)), (0, false));
    }
}
