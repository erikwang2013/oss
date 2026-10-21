//! S5 运行时状态 JSON：`GET /api/v1/site/raft/status`（S6 路由接线）。
//!
//! `production_ready` 按计划 §2.6：少于 3 个 voter 只允许开发/测试，状态里显式标 false。
//! S7.1：`role` 是配置角色（voter/witness），运行态在 `state`；另报成员/leader 地址。

use serde_json::{Value, json};

use super::{RAFT_READY, RaftHandle, STRETCH_READY, SYNC_READY};

fn log_id_json(id: &openraft::LogId<u64>) -> Value {
    json!({ "term": id.leader_id.term, "index": id.index })
}

pub fn status_json(handle: &RaftHandle) -> Value {
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
        "snapshot": m.snapshot.as_ref().map(log_id_json),
        "purged": m.purged.as_ref().map(log_id_json),
        "voters": voters,
        "members": voters,
        "learners": learners,
        "nodes": nodes,
        "production_ready": voters.len() >= 3,
        "capabilities": {
            "raft": RAFT_READY,
            "sync": SYNC_READY,
            "stretch": STRETCH_READY,
        },
    })
}
