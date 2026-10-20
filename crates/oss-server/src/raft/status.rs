//! S5 运行时状态 JSON：`GET /api/v1/site/raft/status`（S6 路由接线）。
//!
//! `production_ready` 按计划 §2.6：少于 3 个 voter 只允许开发/测试，状态里显式标 false。

use serde_json::{Value, json};

use super::{RAFT_READY, RaftHandle, STRETCH_READY, SYNC_READY};

fn log_id_json(id: &openraft::LogId<u64>) -> Value {
    json!({ "term": id.leader_id.term, "index": id.index })
}

pub fn status_json(handle: &RaftHandle) -> Value {
    let m = handle.raft().metrics();
    let m = m.borrow();
    let voters: Vec<u64> = m.membership_config.voter_ids().collect();
    json!({
        "enabled": m.running_state.is_ok(),
        "node_id": m.id,
        "leader": m.current_leader,
        "term": m.current_term,
        "state": format!("{:?}", m.state).to_lowercase(),
        "applied": m.last_applied.as_ref().map(log_id_json),
        "snapshot": m.snapshot.as_ref().map(log_id_json),
        "purged": m.purged.as_ref().map(log_id_json),
        "voters": voters,
        "production_ready": voters.len() >= 3,
        "capabilities": {
            "raft": RAFT_READY,
            "sync": SYNC_READY,
            "stretch": STRETCH_READY,
        },
    })
}
