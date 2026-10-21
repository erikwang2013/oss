//! S7.1：Raft HTTP 入口 `/api/v1/site/raft/*`（数据站点与 witness 共用）。
//!
//! 路由不依赖 `AppState`（witness 没有），状态经 [`RaftExt`] 注入。鉴权是共享集群令牌
//! 常量时间比较（[`super::network::token_matches`]）。写路由 follower 单跳转发到 leader，
//! 无 leader → 503 + hint，所有等待都有界。

use std::sync::Arc;

use axum::Extension;
use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use serde_json::{Value, json};

use super::command::{ControlCommand, SizeError};
use super::membership::{self, MembershipError, MembershipOp};
use super::{
    COMMAND_MAX_BYTES, RPC_BODY_LIMIT, RPC_TARGET_HEADER, RPC_TOKEN_HEADER, RaftHandle, TypeConfig,
    WriteError,
};

/// follower → leader 单跳标记：带此头仍非 leader 不再转发（防环）。
pub const FORWARD_HEADER: &str = "x-oss-raft-forwarded";
/// `/write` 体上限（ruling 11：24 MiB 命令 + JSON 余量）。
pub const WRITE_BODY_LIMIT: usize = 32 * 1024 * 1024;
/// `/membership` 体上限（S7.3 变更请求很小）。
const MEMBERSHIP_BODY_LIMIT: usize = 64 * 1024;

#[derive(Clone)]
pub struct RaftExt {
    pub handle: Arc<RaftHandle>,
    pub cluster_token: String,
    /// 本节点是否配置 `[site]`（sync 能力）与 `[stretch]`；witness/测试为 false。
    pub sync_configured: bool,
    pub stretch_configured: bool,
}

/// 挂到数据站点/witness 路由上（`Router<()>`；无需 AppState）。
pub fn rpc_router(
    handle: Arc<RaftHandle>,
    cluster_token: String,
    sync_configured: bool,
    stretch_configured: bool,
) -> Router {
    let ext = RaftExt {
        handle,
        cluster_token,
        sync_configured,
        stretch_configured,
    };
    Router::new()
        .route(
            "/api/v1/site/raft/append",
            post(append).layer(DefaultBodyLimit::max(RPC_BODY_LIMIT)),
        )
        .route(
            "/api/v1/site/raft/vote",
            post(vote).layer(DefaultBodyLimit::max(RPC_BODY_LIMIT)),
        )
        .route(
            "/api/v1/site/raft/snapshot",
            post(snapshot).layer(DefaultBodyLimit::max(RPC_BODY_LIMIT)),
        )
        .route("/api/v1/site/raft/status", get(status))
        .route(
            "/api/v1/site/raft/membership",
            post(membership).layer(DefaultBodyLimit::max(MEMBERSHIP_BODY_LIMIT)),
        )
        .route(
            "/api/v1/site/raft/write",
            post(write).layer(DefaultBodyLimit::max(WRITE_BODY_LIMIT)),
        )
        .layer(Extension(ext.clone()))
        .route_layer(middleware::from_fn_with_state(ext, require_token))
}

/// 缺令牌 401、错令牌 403（均不泄漏期望值）。
async fn require_token(State(ext): State<RaftExt>, req: Request, next: Next) -> Response {
    let provided = req
        .headers()
        .get(RPC_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok());
    match provided {
        None => (StatusCode::UNAUTHORIZED, "missing raft token").into_response(),
        Some(t) if !super::network::token_matches(&ext.cluster_token, Some(t)) => {
            (StatusCode::FORBIDDEN, "invalid raft token").into_response()
        }
        Some(_) => next.run(req).await,
    }
}

fn raft_err<E: serde::Serialize>(e: E) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(e)).into_response()
}

/// Raft RPC 只应答发给本 node_id 的请求。旧成员（endpoint 相同）尚未从 membership
/// 移除时，新进程绝不能以“异日志 follower”响应，否则 openraft 会 panic。
fn target_matches(ext: &RaftExt, headers: &HeaderMap) -> bool {
    headers
        .get(RPC_TARGET_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        == Some(ext.handle.node_id())
}

fn wrong_target() -> Response {
    (
        StatusCode::MISDIRECTED_REQUEST,
        "raft RPC target node id mismatch",
    )
        .into_response()
}

async fn append(
    Extension(ext): Extension<RaftExt>,
    headers: HeaderMap,
    Json(rpc): Json<AppendEntriesRequest<TypeConfig>>,
) -> Response {
    if !target_matches(&ext, &headers) {
        return wrong_target();
    }
    match ext.handle.raft().append_entries(rpc).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => raft_err(e),
    }
}

async fn vote(
    Extension(ext): Extension<RaftExt>,
    headers: HeaderMap,
    Json(rpc): Json<VoteRequest<u64>>,
) -> Response {
    if !target_matches(&ext, &headers) {
        return wrong_target();
    }
    match ext.handle.raft().vote(rpc).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => raft_err(e),
    }
}

async fn snapshot(
    Extension(ext): Extension<RaftExt>,
    headers: HeaderMap,
    Json(rpc): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Response {
    if !target_matches(&ext, &headers) {
        return wrong_target();
    }
    match ext.handle.raft().install_snapshot(rpc).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => raft_err::<RaftError<u64, InstallSnapshotError>>(e),
    }
}

async fn status(Extension(ext): Extension<RaftExt>) -> Json<Value> {
    // rpc_router 只在配置了 [raft] 时挂载；sync/stretch 能力位还要求对应配置存在。
    let mut v = ext.handle.status_json(true);
    if let Some(caps) = v.get_mut("capabilities") {
        caps["sync"] = serde_json::json!(super::SYNC_READY && ext.sync_configured);
        caps["stretch"] = serde_json::json!(super::STRETCH_READY && ext.stretch_configured);
    }
    Json(v)
}

/// 成员变更（S7.3）：token → 解析 → leader 本地执行；follower 单跳转发；无 leader 503。
async fn membership(
    Extension(ext): Extension<RaftExt>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let op: MembershipOp = match serde_json::from_slice(&body) {
        Ok(op) => op,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("invalid membership op: {e}")})),
            )
                .into_response();
        }
    };
    let view = migration_view(&ext.handle);
    if view.leader == Some(ext.handle.node_id()) {
        return match membership::apply_local(&ext.handle, &op).await {
            Ok(v) => Json(v).into_response(),
            Err(e) => membership_error(e),
        };
    }
    if headers.contains_key(FORWARD_HEADER) {
        return not_leader_409(view.leader, view.leader_endpoint.clone());
    }
    let Some(endpoint) = view.leader_endpoint.clone() else {
        return no_leader("leader unknown or address missing from membership");
    };
    match membership::request_forwarded(&endpoint, &ext.cluster_token, &op).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => membership_error(e),
    }
}

fn migration_view(handle: &RaftHandle) -> membership::MemberView {
    membership::view(handle)
}

fn membership_error(e: MembershipError) -> Response {
    match e {
        MembershipError::NotLeader { view } => not_leader_409(view.leader, view.leader_endpoint),
        MembershipError::Invalid(msg) => {
            (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
        }
        MembershipError::Raft(msg) => {
            (StatusCode::BAD_GATEWAY, Json(json!({"error": msg}))).into_response()
        }
        MembershipError::NoQuorum => no_leader("control plane has no quorum"),
    }
}

fn not_leader_409(leader: Option<u64>, leader_endpoint: Option<String>) -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": "not leader",
            "leader": leader,
            "leader_endpoint": leader_endpoint,
        })),
    )
        .into_response()
}

async fn write(
    Extension(ext): Extension<RaftExt>,
    headers: HeaderMap,
    Json(cmd): Json<ControlCommand>,
) -> Response {
    if let Err(e) = cmd.check_size() {
        return match e {
            SizeError::TooLarge(bytes) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({
                    "error": format!("command {bytes} bytes exceeds raft limit {COMMAND_MAX_BYTES}"),
                })),
            )
                .into_response(),
            SizeError::Encode(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("command encode: {e}")})),
            )
                .into_response(),
        };
    }
    // 带转发标记的请求只做本地写（它是别人转来的，禁止再转）。
    let forwarded = headers.contains_key(FORWARD_HEADER);
    let result = if forwarded {
        ext.handle.write(cmd).await
    } else {
        ext.handle.client_write(cmd).await
    };
    match result {
        Ok(response) => {
            let applied_index = {
                let m = ext.handle.raft().metrics();
                let m = m.borrow();
                m.last_applied.map(|l| l.index).unwrap_or(0)
            };
            Json(json!({"response": response, "applied_index": applied_index})).into_response()
        }
        Err(WriteError::Timeout(d)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "no quorum",
                "hint": format!("write may have committed after {d:?}; retry is idempotent"),
            })),
        )
            .into_response(),
        Err(WriteError::NoQuorum) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "no quorum",
                "hint": "write may have committed; retry is idempotent",
            })),
        )
            .into_response(),
        Err(WriteError::ForwardToLeader { leader, endpoint }) => match (leader, endpoint) {
            (Some(_), Some(endpoint)) => not_leader_409(leader, Some(endpoint)),
            _ => no_leader("control plane has no quorum"),
        },
        Err(WriteError::Raft(e)) => {
            (StatusCode::BAD_GATEWAY, Json(json!({"error": e}))).into_response()
        }
    }
}

fn no_leader(reason: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": "no leader",
            "hint": format!("{reason}; retry after leader election or join"),
        })),
    )
        .into_response()
}
