//! S5 运行时状态 JSON：`GET /api/v1/site/raft/status`（S6 路由接线）。
//!
//! `production_ready` 按计划 §2.6：少于 3 个 voter 只允许开发/测试，状态里显式标 false。
//! S7.1：`role` 是配置角色（voter/witness），运行态在 `state`；另报成员/leader 地址。
//!
//! `lag` = `last_log_index - last_applied.index`（无值按 0）；`stale` = 本节点非 leader
//! 且 `lag > 0`。follower 的读是本机已 apply 状态，**不是**线性一致读：`stale=true` 只表示
//! 至少晚于 leader 一条已提交日志，读到的旧值仍需业务侧按版本/条件判断。

use serde_json::{Value, json};

use super::{RAFT_READY, RaftHandle, STRETCH_READY, SYNC_READY};

fn log_id_json(id: &openraft::LogId<u64>) -> Value {
    json!({ "term": id.leader_id.term, "index": id.index })
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
    let is_leader = m.current_leader == Some(m.id);
    let lag = m
        .last_log_index
        .unwrap_or(0)
        .saturating_sub(m.last_applied.as_ref().map(|l| l.index).unwrap_or(0));
    let leader_endpoint = m
        .current_leader
        .and_then(|l| membership.get_node(&l).map(|n| n.addr.clone()));
    let role = match handle.settings().role {
        crate::config::RaftRole::Voter => "voter",
        crate::config::RaftRole::Witness => "witness",
    };
    json!({
        "enabled": m.running_state.is_ok(),
        "node_id": m.id,
        "role": role,
        "bootstrap": handle.settings().bootstrap,
        "initialized": !voters.is_empty(),
        "leader": m.current_leader,
        "leader_endpoint": leader_endpoint,
        "term": m.current_term,
        "state": state,
        "applied": m.last_applied.as_ref().map(log_id_json),
        // follower 读 = 本机 applied 状态（非线性一致）；stale 只标记「落后 > 0」。
        "lag": lag,
        "stale": !is_leader && lag > 0,
        "snapshot": m.snapshot.as_ref().map(log_id_json),
        "purged": m.purged.as_ref().map(log_id_json),
        "voters": voters,
        "members": voters,
        "learners": learners,
        "nodes": nodes,
        "production_ready": voters.len() >= 3,
        "capabilities": {
            "raft": configured && RAFT_READY,
            "sync": configured && SYNC_READY,
            "stretch": configured && STRETCH_READY,
        },
    })
}
