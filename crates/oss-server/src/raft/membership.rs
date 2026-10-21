//! S7.3：成员变更（learners → voters、移除、替换）与 leader 感知的 HTTP 客户端。
//!
//! 唯一事实来源是 `metrics().membership_config` + `current_leader`（openraft 0.9 没有
//! `Raft::membership()`）。所有变更先做 leader 检查；新节点入列前先尽力建快照——若快照
//! 都建不出来，宁可失败也不添加一个永远追不上的 learner。

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers};
use serde::{Deserialize, Serialize};

use super::RaftHandle;

/// 入列前等待快照构建完成的上限（快照失败 = 不添加 learner）。
const SNAPSHOT_WAIT: Duration = Duration::from_secs(10);
/// 成员变更请求（follower → leader）超时。
/// 覆盖 leader 侧 `ensure_snapshot`（10s）+ 成员写（write_timeout）的转发上限。
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipOp {
    AddWitness {
        node_id: u64,
        endpoint: String,
    },
    RemoveNode {
        node_id: u64,
    },
    ReplaceVoter {
        node_id: u64,
        endpoint: String,
        old_node_id: u64,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemberView {
    pub leader: Option<u64>,
    pub leader_endpoint: Option<String>,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub nodes: BTreeMap<u64, String>,
}

#[derive(Debug)]
pub enum MembershipError {
    /// 本节点不是 leader（携带当前视图，路由据此回 409 或转发）。
    NotLeader { view: MemberView },
    /// 非法变更（会破坏 quorum、endpoint 非法、目标 id 冲突等）。
    Invalid(String),
    /// openraft 运行时错误（存储/致命）。
    Raft(String),
    /// 没有 leader / 无法联系 leader：控制面无 quorum。
    NoQuorum,
}

impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { view } => write!(f, "not leader (leader={:?})", view.leader),
            Self::Invalid(m) => write!(f, "invalid membership op: {m}"),
            Self::Raft(m) => write!(f, "membership raft error: {m}"),
            Self::NoQuorum => write!(f, "no leader/quorum"),
        }
    }
}

impl std::error::Error for MembershipError {}

/// `metrics().membership_config` + `current_leader` 的只读快照。
pub fn view(handle: &RaftHandle) -> MemberView {
    let m = handle.raft().metrics();
    let m = m.borrow();
    let membership = m.membership_config.membership();
    let mut voters: Vec<u64> = m.membership_config.voter_ids().collect();
    voters.sort_unstable();
    let mut learners: Vec<u64> = membership.learner_ids().collect();
    learners.sort_unstable();
    MemberView {
        leader: m.current_leader,
        leader_endpoint: m
            .current_leader
            .and_then(|l| membership.get_node(&l).map(|n| n.addr.clone())),
        voters,
        learners,
        nodes: membership
            .nodes()
            .map(|(id, n)| (*id, n.addr.clone()))
            .collect(),
    }
}

/// 新节点入列前先建快照：快照失败 → 错误（不添加 learner，避免永远追不上）。
///
/// 无 applied log 时视为 OK（没有需要快照的内容）。
pub async fn ensure_snapshot(handle: &RaftHandle) -> Result<(), MembershipError> {
    let applied = {
        let m = handle.raft().metrics();
        let m = m.borrow();
        m.last_applied.map(|l| l.index)
    };
    let Some(applied) = applied else {
        return Ok(());
    };
    handle
        .raft()
        .trigger()
        .snapshot()
        .await
        .map_err(|e| MembershipError::Raft(e.to_string()))?;
    let deadline = tokio::time::Instant::now() + SNAPSHOT_WAIT;
    loop {
        let snap = {
            let m = handle.raft().metrics();
            let m = m.borrow();
            m.snapshot.map(|l| l.index)
        };
        if snap.is_some_and(|i| i >= applied) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(MembershipError::Raft(format!(
                "snapshot not built within {SNAPSHOT_WAIT:?} (applied={applied}, snapshot={snap:?})"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn map_client_err(handle: &RaftHandle, e: ClientWriteError<u64, BasicNode>) -> MembershipError {
    match e {
        ClientWriteError::ForwardToLeader(_) => MembershipError::NotLeader { view: view(handle) },
        other => MembershipError::Raft(other.to_string()),
    }
}

fn map_raft_err(
    handle: &RaftHandle,
    e: RaftError<u64, ClientWriteError<u64, BasicNode>>,
) -> MembershipError {
    match e {
        RaftError::APIError(e) => map_client_err(handle, e),
        RaftError::Fatal(e) => MembershipError::Raft(e.to_string()),
    }
}

fn require_leader(handle: &RaftHandle) -> Result<MemberView, MembershipError> {
    let v = view(handle);
    if v.leader != Some(handle.node_id()) {
        return Err(MembershipError::NotLeader { view: v });
    }
    Ok(v)
}

fn valid_endpoint(endpoint: &str) -> Result<(), MembershipError> {
    crate::site_client::validate_endpoint(endpoint)
        .map(|_| ())
        .map_err(MembershipError::Invalid)
}

/// learner 入列（blocking：等到日志追平才返回）。已入列则跳过。
async fn add_learner(
    handle: &RaftHandle,
    node_id: u64,
    endpoint: &str,
) -> Result<(), MembershipError> {
    ensure_snapshot(handle).await?;
    handle
        .raft()
        .add_learner(node_id, BasicNode::new(endpoint), true)
        .await
        .map_err(|e| map_raft_err(handle, e))?;
    Ok(())
}

/// learner → voter；已是 voter 则幂等返回。
pub async fn promote_voter(
    handle: &RaftHandle,
    node_id: u64,
    endpoint: String,
) -> Result<MemberView, MembershipError> {
    require_leader(handle)?;
    valid_endpoint(&endpoint)?;
    let v = view(handle);
    if v.voters.contains(&node_id) {
        return Ok(v);
    }
    if !v.learners.contains(&node_id) {
        add_learner(handle, node_id, &endpoint).await?;
    }
    let mut voters: BTreeSet<u64> = v.voters.iter().copied().collect();
    voters.insert(node_id);
    handle
        .raft()
        .change_membership(voters, false)
        .await
        .map_err(|e| map_raft_err(handle, e))?;
    let v = view(handle);
    if !v.voters.contains(&node_id) || !v.voters.contains(&handle.node_id()) {
        return Err(MembershipError::Raft(format!(
            "membership change did not take effect: voters={:?}",
            v.voters
        )));
    }
    Ok(v)
}

async fn remove_node(
    handle: &RaftHandle,
    v: MemberView,
    node_id: u64,
) -> Result<MemberView, MembershipError> {
    if node_id == handle.node_id() {
        return Err(MembershipError::Invalid(
            "cannot remove the local node".into(),
        ));
    }
    if !v.voters.contains(&node_id) {
        // 只在 learners 里（上次 add_learner 后失败）：清掉 learner 条目。
        if v.learners.contains(&node_id) {
            handle
                .raft()
                .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
                .await
                .map_err(|e| map_raft_err(handle, e))?;
            return Ok(view(handle));
        }
        return Ok(v);
    }
    let mut voters: BTreeSet<u64> = v.voters.iter().copied().collect();
    voters.remove(&node_id);
    if voters.len() < 2 {
        return Err(MembershipError::Invalid(
            "would leave <2 voters; add a witness before removing".into(),
        ));
    }
    handle
        .raft()
        .change_membership(voters, false)
        .await
        .map_err(|e| map_raft_err(handle, e))?;
    Ok(view(handle))
}

async fn replace_voter(
    handle: &RaftHandle,
    v: MemberView,
    node_id: u64,
    endpoint: String,
    old_node_id: u64,
) -> Result<MemberView, MembershipError> {
    if old_node_id == handle.node_id() {
        return Err(MembershipError::Invalid(
            "cannot replace the local node".into(),
        ));
    }
    if node_id == old_node_id || node_id == handle.node_id() {
        return Err(MembershipError::Invalid(
            "replacement node id must be new and not the local node".into(),
        ));
    }
    if !v.voters.contains(&old_node_id) {
        // 已替换过：新 id 是 voter → 幂等。
        if v.voters.contains(&node_id) {
            return Ok(v);
        }
        return Err(MembershipError::Invalid(format!(
            "old node {old_node_id} is not a voter"
        )));
    }
    if v.voters.contains(&node_id) {
        return Err(MembershipError::Invalid(format!(
            "node {node_id} is already a voter"
        )));
    }
    valid_endpoint(&endpoint)?;
    if !v.learners.contains(&node_id) {
        add_learner(handle, node_id, &endpoint).await?;
    }
    let mut voters: BTreeSet<u64> = v.voters.iter().copied().collect();
    voters.remove(&old_node_id);
    voters.insert(node_id);
    handle
        .raft()
        .change_membership(voters, false)
        .await
        .map_err(|e| map_raft_err(handle, e))?;
    let v = view(handle);
    if !v.voters.contains(&node_id) || v.voters.contains(&old_node_id) {
        return Err(MembershipError::Raft(format!(
            "replace did not take effect: voters={:?}",
            v.voters
        )));
    }
    Ok(v)
}

/// 本节点执行变更（必须是 leader；否则返回当前视图）。
pub async fn apply_local(
    handle: &RaftHandle,
    op: &MembershipOp,
) -> Result<MemberView, MembershipError> {
    let v = require_leader(handle)?;
    match op {
        MembershipOp::AddWitness { node_id, endpoint } => {
            promote_voter(handle, *node_id, endpoint.clone()).await
        }
        MembershipOp::RemoveNode { node_id } => remove_node(handle, v, *node_id).await,
        MembershipOp::ReplaceVoter {
            node_id,
            endpoint,
            old_node_id,
        } => replace_voter(handle, v, *node_id, endpoint.clone(), *old_node_id).await,
    }
}

#[derive(Deserialize)]
struct LeaderHint {
    #[serde(default)]
    leader: Option<u64>,
    #[serde(default)]
    leader_endpoint: Option<String>,
}

/// 客户端（CLI/测试）向 leader 发一次成员变更；跟随者可自行转发到 leader。
pub async fn request(
    endpoint: &str,
    token: &str,
    op: &MembershipOp,
) -> Result<MemberView, MembershipError> {
    request_inner(endpoint, token, op, false).await
}

/// 节点内部转发：带单跳标记，目标若已非 leader 直接 409，不再二次转发。
pub async fn request_forwarded(
    endpoint: &str,
    token: &str,
    op: &MembershipOp,
) -> Result<MemberView, MembershipError> {
    request_inner(endpoint, token, op, true).await
}

async fn request_inner(
    endpoint: &str,
    token: &str,
    op: &MembershipOp,
    forwarded: bool,
) -> Result<MemberView, MembershipError> {
    let body = serde_json::to_vec(op).map_err(|e| MembershipError::Raft(e.to_string()))?;
    let (status, bytes) = crate::site_client::post_raft_membership(
        endpoint,
        token,
        body.into(),
        REQUEST_TIMEOUT,
        forwarded,
    )
    .await
    .map_err(MembershipError::Raft)?;
    let lossy = || {
        String::from_utf8_lossy(&bytes)
            .chars()
            .take(300)
            .collect::<String>()
    };
    match status {
        200 => serde_json::from_slice(&bytes)
            .map_err(|e| MembershipError::Raft(format!("member view decode: {e}"))),
        409 => {
            let hint: LeaderHint = serde_json::from_slice(&bytes).unwrap_or(LeaderHint {
                leader: None,
                leader_endpoint: None,
            });
            Err(MembershipError::NotLeader {
                view: MemberView {
                    leader: hint.leader,
                    leader_endpoint: hint.leader_endpoint,
                    ..MemberView::default()
                },
            })
        }
        503 => Err(MembershipError::NoQuorum),
        other => Err(MembershipError::Raft(format!(
            "membership HTTP {other}: {}",
            lossy()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_op_json_is_tagged_and_strict() {
        let op: MembershipOp =
            serde_json::from_str(r#"{"op":"add_witness","node_id":3,"endpoint":"http://c:9"}"#)
                .unwrap();
        assert!(matches!(op, MembershipOp::AddWitness { node_id: 3, .. }));
        let op: MembershipOp = serde_json::from_str(
            r#"{"op":"replace_voter","node_id":4,"endpoint":"http://b:9","old_node_id":2}"#,
        )
        .unwrap();
        assert!(matches!(
            op,
            MembershipOp::ReplaceVoter {
                node_id: 4,
                old_node_id: 2,
                ..
            }
        ));
        assert!(
            serde_json::from_str::<MembershipOp>(r#"{"op":"add_witness","node_id":3}"#).is_err()
        );
        assert!(
            serde_json::from_str::<MembershipOp>(
                r#"{"op":"add_witness","node_id":3,"endpoint":"http://c:9","x":1}"#
            )
            .is_err()
        );
    }
}
