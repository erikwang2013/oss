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
    let oss_s3 = OssS3::new(
        Arc::clone(&state.store),
        acl.clone(),
        state.cfg.max_put_bytes,
    )
    .with_events(Some(
        Arc::clone(&state.events) as Arc<dyn xl_s3::events::EventSink>
    ))
    .with_replication(
        Some(Arc::clone(&state.repl_sink) as Arc<dyn xl_s3::replication::ReplicationSink>),
        Some(Arc::clone(&state.repl_store) as Arc<dyn xl_s3::replication::ReplicationStore>),
    )
    .with_bucket_cleanup(Some(Arc::clone(&state.bucket_cleanup)))
    .with_lifecycle(Some(Arc::clone(&state.lifecycle)))
    .with_restore(Some(Arc::clone(&state.restore_sink)))
    .with_tier_gc(Some(Arc::clone(&state.tier_gc)))
    .with_inventory(Some(Arc::clone(&state.inventory)))
    // M9 S4：stretch 禁止 sync 桶（对象内容不得依赖单站点异步副本）；
    // M9 S3（I3）：S3 面的 sync 建桶要求 SYNC_READY + RAFT_READY + [raft]（与 console 同口径）。
    // “恰好 1 个数据 peer”是运行期 DB 条件（sites 行会变），在建桶入口按请求校验：
    // service.rs 的 site_peer_count / console 的 list_sites。
    .with_sync_ready(
        crate::raft::SYNC_READY
            && crate::raft::RAFT_READY
            && state.cfg.stretch.is_none()
            && state.cfg.raft.is_some(),
    )
    // M9：sync 摄入 token 校验（无 [site] = None → 一律 403）
    .with_site_token(state.site_enabled.then(|| state.site.token.clone()))
    // M9 S3：S3/lifecycle/batch/console 删除统一走远端优先服务。
    .with_sync_delete(Some(
        Arc::clone(&state.sync_delete) as Arc<dyn xl_s3::sync_delete::SyncDeleteSink>
    ))
    // M9 S6c：仅 Raft 模式注入桶控制面 sink（None = M7 直写路径逐字节不变）。
    .with_bucket_control(state.control.bucket_sink());
    // M8 S1（N1）：内层服务无 auth/access/route，仅供 AP 重写后调用。
    let inner = Arc::new(S3ServiceBuilder::new(oss_s3.clone()).build());
    let mut s3_builder = S3ServiceBuilder::new(oss_s3);
    s3_builder.set_auth(
        SigV4Auth::with_mesh(state.db.pool().clone(), state.control_mesh())
            .with_vault(state.vault.clone()) // M5 切片 6：SK 解密/透明重写
            .with_raft_mode(state.control.is_raft()), // M9 S6b H2：Raft 下禁止 apply 外重封
    );
    s3_builder.set_access(acl.clone());
    // M8 S1（N2）：AP 与 STS 共用一个 route 槽：先 AP，未命中委托 STS。
    let sts: Arc<dyn s3s::route::S3Route> = Arc::new(
        xl_s3::sts::StsRoute::new(state.db.pool().clone()).with_vault(state.vault.clone()),
    );
    let ap = xl_s3::ap::ApRoute::new(
        Arc::clone(&state.ap_cache),
        Arc::new(crate::access_points_api::DbApSource {
            db: Arc::clone(&state.db),
            region: state.ap_region.clone(),
        }),
        acl,
        Arc::clone(&state.olap_cache),
        inner,
    )
    .with_fallback(sts);
    s3_builder.set_route(ap);
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
        // Prometheus 文本面（M4 观测面）。公开口径与 /health/* 相同：只暴露节点 id 与
        // 计数（/health/cluster 本就公开同量级信息）；要收严时在外层套鉴权即可。
        .route("/metrics", get(metrics))
        // M9 S0 骨架：S5 补 leader/term/applied；B 经 advertise_endpoint 访问。
        .route("/api/v1/raft/status", get(raft_status))
        .nest("/api/v1", console)
        .merge(aether.with_state(()))
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state.clone());
    // S7.1：Raft RPC 路由只在 `[raft]` 下挂载；走集群令牌，不经 console JWT 层。
    let app = match state.raft.clone() {
        Some(handle) => {
            let token = state
                .cfg
                .raft
                .as_ref()
                .expect("raft handle implies [raft]")
                .cluster_token
                .clone();
            app.merge(crate::raft::routes::rpc_router(handle, token))
        }
        None => app,
    };
    // 层在路由之后挂：同时包住 fallback（S3 面）。
    let app = app.layer(middleware::from_fn(restore_accepted));
    Ok(app)
}

async fn raft_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(state.control.status_json())
}

/// s3s 0.17 的 RestoreObject 生成代码固定按 `200 OK` 序列化并丢弃 `S3Response.status`，
/// handler 里设的 202 到不了线上；这里在 HTTP 层补回 S3 语义（仅 POST `?restore`，
/// 且只改写 200，错误状态原样透传）。`x-amz-restore` 服务已设置，entry 避免重复。
async fn restore_accepted(req: Request, next: Next) -> Response {
    let is_restore = req.method() == axum::http::Method::POST
        && req.uri().query().is_some_and(|q| {
            q.split('&')
                .any(|kv| kv.split('=').next() == Some("restore"))
        });
    let mut res = next.run(req).await;
    if is_restore && res.status() == StatusCode::OK {
        *res.status_mut() = StatusCode::ACCEPTED;
        res.headers_mut()
            .entry(header::HeaderName::from_static("x-amz-restore"))
            .or_insert(HeaderValue::from_static("ongoing-request=\"true\""));
    }
    res
}

/// peer 探测（单点 800ms 超时；错接线按 down + 显性日志）。
/// `/health/cluster`、`/metrics`、`/api/v1/cluster/status` 三处共用一份探测逻辑。
/// 返回每 peer `(id, up, 在册槽数)`；顺序 = 拓扑顺序（不含本节点；单机为空表）。
pub(crate) async fn probe_peers(state: &AppState) -> Vec<(String, bool, usize)> {
    let mut out = Vec::new();
    let Some(c) = state.store.cluster() else {
        return out;
    };
    let self_node_id = c.sets.flat.self_node().id.clone();
    for n in &c.sets.flat.nodes {
        if n.id == self_node_id {
            continue;
        }
        // 单点探测 800ms：就绪探针会被 LB 轮询，要短平快；掉队节点标 down 即可
        match tokio::time::timeout(Duration::from_millis(800), c.transport.health(&n.id)).await {
            Ok(Ok(h)) if h.node_id == n.id => out.push((
                n.id.clone(),
                true,
                h.slots.iter().filter(|(_, up)| *up).count(),
            )),
            Ok(Ok(h)) => {
                // 错接线：地址通了但不是声明的节点——按 down 处理并留下显性日志
                tracing::error!(node = %n.id, reported = %h.node_id, "mesh 节点身份不符（错接线？）");
                out.push((n.id.clone(), false, 0));
            }
            _ => out.push((n.id.clone(), false, 0)),
        }
    }
    out
}

/// Prometheus 文本面（手写 exposition——十来个指标不值一个依赖树）。
/// 指标：quorum 未达计数（写/锁）、各节点可达性、本机盘数、全局槽位数。
async fn metrics(State(state): State<AppState>) -> impl axum::response::IntoResponse {
    let local = state.store.mesh_health().await;
    let (unmet_write, unmet_lock) = state.store.quorum_unmet_counts();
    let drive_count = local.slots.len();
    let slot_count = state
        .store
        .cluster()
        .map(|c| c.sets.flat.slot_count())
        .unwrap_or(drive_count as u16);

    let mut out = String::with_capacity(512);
    out.push_str("# HELP oss_quorum_unmet_total quorum 未达成次数（写 quorum / 租约锁分列）\n");
    out.push_str("# TYPE oss_quorum_unmet_total counter\n");
    out.push_str(&format!(
        "oss_quorum_unmet_total{{op=\"write\"}} {unmet_write}\n"
    ));
    out.push_str(&format!(
        "oss_quorum_unmet_total{{op=\"lock\"}} {unmet_lock}\n"
    ));
    out.push_str("# HELP oss_node_up 本节点视角各节点可达性（自身恒 1）\n");
    out.push_str("# TYPE oss_node_up gauge\n");
    out.push_str(&format!(
        "oss_node_up{{node=\"{}\"}} 1\n",
        label_escape(&local.node_id)
    ));
    for (id, up, _) in probe_peers(&state).await {
        out.push_str(&format!(
            "oss_node_up{{node=\"{}\"}} {}\n",
            label_escape(&id),
            u8::from(up)
        ));
    }
    out.push_str("# HELP oss_drive_count 本机盘数\n# TYPE oss_drive_count gauge\n");
    out.push_str(&format!("oss_drive_count {drive_count}\n"));
    out.push_str(
        "# HELP oss_slot_count 全局槽位数（单机 = 本机盘数）\n# TYPE oss_slot_count gauge\n",
    );
    out.push_str(&format!("oss_slot_count {slot_count}\n"));
    out.push_str(
        "# HELP oss_sync_delete_partial_total DeleteObjects 远端失败整体 503 次数（部分键已提交）\n\
         # TYPE oss_sync_delete_partial_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_delete_partial_total {}\n",
        state.sync_delete.partial_total()
    ));
    out.push_str(
        "# HELP oss_sync_delete_recovered_total 周期 worker 收敛的 sync delete intent 行数\n\
         # TYPE oss_sync_delete_recovered_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_delete_recovered_total {}\n",
        state.sync_delete.recovered_total()
    ));
    out.push_str(
        "# HELP oss_sync_ambiguous_total 远端删除超时且 HEAD 探测无法定位（执行与否不可判定）次数\n\
         # TYPE oss_sync_ambiguous_total counter\n",
    );
    out.push_str(&format!(
        "oss_sync_ambiguous_total {}\n",
        state.sync_delete.ambiguous_total()
    ));
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
}

/// Prometheus 标签值转义（配置里的 node id 属半信任输入；坏配置不该产出坏样本行）。
fn label_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
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
    let peers = probe_peers(state).await;
    // 单机 = 1（自身即多数）；集群 = 1 + peers
    let expected = 1 + peers.len();
    let mut reachable = 1usize;
    for (id, up, slots) in &peers {
        if *up {
            reachable += 1;
        }
        nodes.push(serde_json::json!({ "id": id, "up": up, "slots": slots }));
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

#[cfg(test)]
mod tests {
    use super::label_escape;
    use axum::http::StatusCode;

    /// RestoreObject 线上状态修复：仅 POST `?restore` 的 200 → 202 + `x-amz-restore`；
    /// 普通 POST/GET 与形似查询（`x-restore`）原样通过。
    #[tokio::test]
    async fn restore_post_is_rewritten_to_202() {
        async fn ok() -> StatusCode {
            StatusCode::OK
        }
        let app = axum::Router::new()
            .fallback(ok)
            .layer(axum::middleware::from_fn(super::restore_accepted));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = reqwest::Client::new();

        let res = http
            .post(format!("http://{addr}/b/k?restore"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 202);
        assert_eq!(res.headers()["x-amz-restore"], "ongoing-request=\"true\"");
        assert_eq!(res.headers().get_all("x-amz-restore").iter().count(), 1);

        let res = http
            .post(format!("http://{addr}/b/k?restore=1"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 202, "?restore=<v> 也是 RestoreObject");

        for (method, path) in [
            ("POST", "/b/k"),
            ("POST", "/b/k?x-restore=1"),
            ("GET", "/b/k?restore"),
        ] {
            let res = http
                .request(
                    reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                    format!("http://{addr}{path}"),
                )
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "{method} {path} 不应被改写");
            assert!(
                res.headers().get("x-amz-restore").is_none(),
                "{method} {path}"
            );
        }
    }

    /// 标签值转义：坏配置（node id 带引号/反斜杠/换行）不得产出坏样本行。
    /// 反斜杠必须先转义（先换引号会把 `\"` 里的反斜杠二次转义错）。
    #[test]
    fn label_escape_covers_prometheus_metacharacters() {
        assert_eq!(label_escape("plain"), "plain");
        assert_eq!(label_escape("a\"b"), "a\\\"b");
        assert_eq!(label_escape("a\\b"), "a\\\\b");
        assert_eq!(label_escape("a\nb"), "a\\nb");
        assert_eq!(label_escape("\\\""), "\\\\\\\"");
    }
}
