//! 一个 Router 承载全部 HTTP 面：控制台 + aetherupload + S3 fallback（M1 后续任务往这里加）。

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::error_handling::HandleError;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use jwt_rust::integrations::axum::JwtLayer;
use s3s::service::S3ServiceBuilder;

use crate::state::AppState;
use xl_s3::auth::SigV4Auth;
use xl_s3::service::OssS3;

/// 组装全部路由。`build_runtime` 失败（暂存盘不可写等）向上传播：
/// serve 与运维命令统一 exit 1 + stderr，不从启动路径 panic。
pub fn build_router(state: AppState) -> Result<Router, Box<dyn std::error::Error + Send + Sync>> {
    // s3s 0.17 的 builder 是 `&mut self` 风格，不能链式调用
    // 授权门：Acl 同时给 service（ListBuckets 过滤/建桶记账）与 access 层（set_access）
    // Task 9：集群时 Acl/SigV4Auth 接控制面读扇出（本地 miss → peers；写仍本地，M9 收口）
    let acl = state.acl();
    let mut s3_builder = S3ServiceBuilder::new(OssS3::new(
        Arc::clone(&state.store),
        acl.clone(),
        state.cfg.max_put_bytes,
    ));
    s3_builder.set_auth(SigV4Auth::with_mesh(
        state.db.pool().clone(),
        state.control_mesh(),
    ));
    s3_builder.set_access(acl);
    let s3 = s3_builder.build();

    // 控制面：`/api/v1` 整体走 jwt-rust 的 JwtLayer，`/login` 在 except 名单里
    // （名单与 12h 过期都在 AppState::new；nest 会先剥掉 `/api/v1` 前缀，层看到的是 `/login`）。
    let console = JwtLayer::new(Arc::clone(&state.jwt)).attach(crate::console::routes());

    // aetherupload：四条路由（预处理 / 分块 / 展示 / 下载）。
    // 不用 crate 自带的 `integrations::ecat::routes`——它的 handler 读不到身份，
    // 无法按子账户权限执行「分组 → 桶」的门；改用 `aether_gate`（同一内核，形态逐字对齐）。
    //
    // 鉴权层（规格 §0.1「会话 Cookie/JWT 覆盖层」）：cookie 垫片在最外层把 `oss_token`
    // 写成 Authorization，再交 JwtLayer 校验（与控制面同一个 JwtAuth 实例 → token 两面通用）。
    // 层序：后挂的 `.layer` 在更外层，先跑；JwtLayer 把 payload 放进类型化 extension，
    // aether_gate 的 handler 用 `Extension<JwtPayload>` 取。
    let aether = crate::aether_gate::routes(crate::aether::build_runtime(&state)?, state.clone())
        .layer(JwtLayer::new(Arc::clone(&state.jwt)))
        .layer(middleware::from_fn(cookie_to_bearer));

    let app = Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .route("/health/cluster", get(health_cluster))
        .route("/health/ready", get(health_ready))
        .nest("/api/v1", console)
        .merge(aether.with_state(()))
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state);
    Ok(app)
}

/// 集群健康视图：`/health/cluster` 的结构化体与 `/health/ready` 的判定共用一个实现。
/// 返回 `(view, ready, reason)`。单节点（无 cluster）时域 = 自身。
async fn cluster_view(state: &AppState) -> (serde_json::Value, bool, String) {
    let local = state.store.mesh_health().await;
    let self_node_id = local.node_id.clone();
    let (unmet_write, unmet_lock) = state.store.quorum_unmet_counts();
    let up_slots = |h: &xl_store::mesh::NodeHealth| h.slots.iter().filter(|(_, up)| *up).count();
    let mut nodes = vec![serde_json::json!({
        "id": self_node_id,
        "up": true,
        "slots": up_slots(&local),
    })];
    let mut reachable = 1usize;
    let mut expected = 1usize; // 无 cluster = 单节点域（自身即多数）
    if let Some(c) = state.store.cluster() {
        expected = c.topo.nodes.len();
        for n in &c.topo.nodes {
            if n.id == self_node_id {
                continue;
            }
            // 单点探测 800ms：就绪探针会被 LB 轮询，要短平快；掉队节点标 down 即可
            match tokio::time::timeout(Duration::from_millis(800), c.transport.health(&n.id)).await
            {
                Ok(Ok(h)) if h.node_id == n.id => {
                    reachable += 1;
                    nodes
                        .push(serde_json::json!({ "id": n.id, "up": true, "slots": up_slots(&h) }));
                }
                Ok(Ok(h)) => {
                    // 错接线：地址通了但不是声明的节点——按 down 处理并留下显性日志
                    tracing::error!(node = %n.id, reported = %h.node_id, "mesh 节点身份不符（错接线？）");
                    nodes.push(serde_json::json!({ "id": n.id, "up": false, "slots": 0 }));
                }
                _ => nodes.push(serde_json::json!({ "id": n.id, "up": false, "slots": 0 })),
            }
        }
    }
    let qn = expected / 2 + 1;
    let slot0_up = local.slots.first().is_some_and(|(_, up)| *up);
    let (ready, reason) = if !slot0_up {
        (false, "本机 slot0 不在册".to_string())
    } else if reachable < qn {
        (
            false,
            format!("可达节点 {reachable}/{expected} < 节点多数 {qn}"),
        )
    } else {
        (true, "ok".to_string())
    };
    let view = serde_json::json!({
        "node_id": self_node_id,
        "version": local.version,
        "nodes": nodes,
        "quorum_unmet": { "write": unmet_write, "lock": unmet_lock },
        "ready": ready,
    });
    (view, ready, reason)
}

/// 集群健康端点（设计 §19 M3「集群健康端点」）：本节点 + 各 peer 槽位在册状态 +
/// quorum 未达计数。peer 不可达/身份不符标 down；HTTP 恒 200（结构化体自描述）。
async fn health_cluster(State(state): State<AppState>) -> Json<serde_json::Value> {
    let (view, _, _) = cluster_view(&state).await;
    Json(view)
}

/// 就绪探针（设计 §18.4 强语义）：本机 slot0 可用 **且** 可达节点数 ≥ 节点多数 Q_n——
/// 分区/少数派节点返回 503（LB 摘除；弱版「本机一份可用」会把流量继续送进来）。
async fn health_ready(State(state): State<AppState>) -> (StatusCode, String) {
    let (_, ready, reason) = cluster_view(&state).await;
    if ready {
        (StatusCode::OK, "ready".to_string())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {reason}"),
        )
    }
}

/// cookie → Authorization 垫片：aetherupload 自带脚本的分块 XHR 只带 cookie（不开
/// Authorization 头），JwtLayer 只认 Bearer。仅当请求没有 Authorization 头时才注入。
async fn cookie_to_bearer(mut req: Request, next: Next) -> Response {
    if !req.headers().contains_key(header::AUTHORIZATION) {
        let bearer = req
            .headers()
            .get(header::COOKIE)
            .and_then(|v| v.to_str().ok())
            .and_then(cookie_token)
            .and_then(|v| HeaderValue::from_str(&format!("Bearer {v}")).ok());
        if let Some(bearer) = bearer {
            req.headers_mut().insert(header::AUTHORIZATION, bearer);
        }
    }
    next.run(req).await
}

/// 手写分号切分：只取 `oss_token` 一个字段，不引 cookie 库。
/// 空值（`oss_token=`）视为没有 cookie —— 不遮蔽 Authorization 回退。
fn cookie_token(cookies: &str) -> Option<&str> {
    cookies
        .split(';')
        .filter_map(|part| part.trim().strip_prefix("oss_token="))
        .find(|v| !v.is_empty())
}

/// 只有 s3s 连错误响应都序列化失败时才到达，近乎不可达的兜底 —— 包括 body 超限、
/// 格式非法在内的 HTTP 层错误都已在 s3s 的 `ops::call` 内 XML 化了。
async fn handle_s3_error(err: s3s::HttpError) -> (StatusCode, String) {
    tracing::error!(error = ?err, "s3s 错误响应失败（兜底 500）");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal error".to_string(),
    )
}
