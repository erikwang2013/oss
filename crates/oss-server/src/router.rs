//! 一个 Router 承载全部 HTTP 面：控制台 + aetherupload + S3 fallback（M1 后续任务往这里加）。

use std::sync::Arc;

use axum::Router;
use axum::error_handling::HandleError;
use axum::extract::Request;
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
    let acl = xl_s3::access::Acl::new(state.db.pool().clone());
    let mut s3_builder = S3ServiceBuilder::new(OssS3::new(
        Arc::clone(&state.store),
        acl.clone(),
        state.cfg.max_put_bytes,
    ));
    s3_builder.set_auth(SigV4Auth::new(state.db.pool().clone()));
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
        .nest("/api/v1", console)
        .merge(aether.with_state(()))
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state);
    Ok(app)
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
