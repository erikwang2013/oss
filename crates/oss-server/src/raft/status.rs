//! S5 运行时状态 JSON：`GET /api/v1/site/raft/status`（S6 路由接线）。
//!
//! `production_ready` 按计划 §2.6：少于 3 个 voter 只允许开发/测试，状态里显式标 false。

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
    let voters: Vec<u64> = m.membership_config.voter_ids().collect();
    let role = format!("{:?}", m.state).to_lowercase();
    json!({
        "enabled": m.running_state.is_ok(),
        "node_id": m.id,
        "role": role,
        "leader": m.current_leader,
        "term": m.current_term,
        "state": role,
        "applied": m.last_applied.as_ref().map(log_id_json),
        "snapshot": m.snapshot.as_ref().map(log_id_json),
        "purged": m.purged.as_ref().map(log_id_json),
        "voters": voters,
        "production_ready": voters.len() >= 3,
        "capabilities": {
            "raft": configured && RAFT_READY,
            "sync": configured && SYNC_READY,
            "stretch": configured && STRETCH_READY,
        },
    })
}
