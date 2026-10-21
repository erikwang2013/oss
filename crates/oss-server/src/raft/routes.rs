//! S7.1：Raft HTTP 入口 `/api/v1/site/raft/*`（数据站点与 witness 共用）。
//!
//! 路由不依赖 `AppState`（witness 没有），状态经 [`RaftExt`] 注入。鉴权是共享集群令牌
//! 常量时间比较（[`super::network::token_matches`]）。写路由 follower 单跳转发到 leader，
//! 无 leader → 503 + hint，所有等待都有界。

use std::sync::Arc;
use std::time::Duration;

use axum::Extension;
use axum::Json;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use serde_json::{Value, json};

use super::command::ControlCommand;
use super::{RPC_BODY_LIMIT, RPC_TOKEN_HEADER, RaftHandle, TypeConfig, WriteError};

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
}

/// 挂到数据站点/witness 路由上（`Router<()>`；无需 AppState）。
pub fn rpc_router(handle: Arc<RaftHandle>, cluster_token: String) -> Router {
    let ext = RaftExt {
        handle,
        cluster_token,
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

async fn append(
    Extension(ext): Extension<RaftExt>,
    Json(rpc): Json<AppendEntriesRequest<TypeConfig>>,
) -> Response {
    match ext.handle.raft().append_entries(rpc).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => raft_err(e),
    }
}

async fn vote(Extension(ext): Extension<RaftExt>, Json(rpc): Json<VoteRequest<u64>>) -> Response {
    match ext.handle.raft().vote(rpc).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => raft_err(e),
    }
}

async fn snapshot(
    Extension(ext): Extension<RaftExt>,
    Json(rpc): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Response {
    match ext.handle.raft().install_snapshot(rpc).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => raft_err::<RaftError<u64, InstallSnapshotError>>(e),
    }
}

async fn status(Extension(ext): Extension<RaftExt>) -> Json<Value> {
    Json(ext.handle.status_json())
}

/// S7.3 拥有成员变更；S7.1 只固定路由/鉴权/体上限形状（CLI 客户端已按 HTTP 走）。
async fn membership(_body: Bytes) -> Response {
    // 先读完体：S7.1 靠这里触发 64 KiB 上限，S7.3 换成真正的变更请求解析。
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({"error": "membership changes are implemented in S7.3"})),
    )
        .into_response()
}

async fn write(
    Extension(ext): Extension<RaftExt>,
    headers: HeaderMap,
    Json(cmd): Json<ControlCommand>,
) -> Response {
    let forwarded = headers.contains_key(FORWARD_HEADER);
    let (leader, leader_endpoint) = {
        let m = ext.handle.raft().metrics();
        let m = m.borrow();
        let leader = m.current_leader;
        let endpoint = leader.and_then(|l| {
            m.membership_config
                .membership()
                .get_node(&l)
                .map(|n| n.addr.clone())
        });
        (leader, endpoint)
    };
    if leader == Some(ext.handle.node_id()) {
        return local_write(&ext.handle, cmd).await;
    }
    if forwarded {
        return no_leader("request already forwarded once");
    }
    let Some(endpoint) = leader_endpoint else {
        return no_leader("leader unknown or address missing from membership");
    };
    forward_write(&ext, &endpoint, cmd).await
}

async fn local_write(handle: &RaftHandle, cmd: ControlCommand) -> Response {
    match handle.write(cmd).await {
        Ok(response) => {
            let applied_index = {
                let m = handle.raft().metrics();
                let m = m.borrow();
                m.last_applied.map(|l| l.index).unwrap_or(0)
            };
            Json(json!({"response": response, "applied_index": applied_index})).into_response()
        }
        Err(WriteError::Timeout(d)) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({
                "error": format!("raft write timed out after {d:?}"),
                "hint": "write may have committed; retry reads before resubmitting",
            })),
        )
            .into_response(),
        Err(WriteError::Raft(e)) => no_leader(&format!("local write failed: {e}")),
    }
}

async fn forward_write(ext: &RaftExt, endpoint: &str, cmd: ControlCommand) -> Response {
    let timeout = Duration::from_millis(ext.handle.settings().write_timeout_ms);
    let client = reqwest::Client::new();
    let sent = client
        .post(format!(
            "{}/api/v1/site/raft/write",
            endpoint.trim_end_matches('/')
        ))
        .header(RPC_TOKEN_HEADER, &ext.cluster_token)
        .header(FORWARD_HEADER, "1")
        .timeout(timeout)
        .json(&cmd)
        .send()
        .await;
    match sent {
        Ok(resp) => {
            let status = resp.status();
            match resp.bytes().await {
                Ok(bytes) => Response::builder()
                    .status(status)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(bytes))
                    .unwrap_or_else(|_| no_leader("failed to relay leader response")),
                Err(e) => no_leader(&format!("leader response read failed: {e}")),
            }
        }
        Err(e) => no_leader(&format!("leader forward failed: {e}")),
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
