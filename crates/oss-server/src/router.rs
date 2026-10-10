//! 一个 Router 承载全部 HTTP 面：控制台 + aetherupload + S3 fallback（M1 后续任务往这里加）。

use std::sync::Arc;

use axum::Router;
use axum::error_handling::HandleError;
use axum::routing::get;
use s3s::service::S3ServiceBuilder;

use crate::state::AppState;
use xl_s3::auth::SigV4Auth;
use xl_s3::service::OssS3;

pub fn build_router(state: AppState) -> Router {
    // s3s 0.17 的 builder 是 `&mut self` 风格，不能链式调用
    let mut s3_builder = S3ServiceBuilder::new(OssS3::new(
        Arc::clone(&state.store),
        state.cfg.max_put_bytes,
    ));
    s3_builder.set_auth(SigV4Auth::new(state.db.pool().clone()));
    let s3 = s3_builder.build();

    // aetherupload：四条路由（预处理 / 分块 / 展示 / 下载）。
    // `routes` 返回的是 Router<()>（runtime 已 state 进各 handler）；`.with_state(())`
    // 只是把 marker 参数转回本函数的 Router<AppState>，为后续 `.nest` 控制台 API 留位。
    let aether = aetherupload::integrations::ecat::routes(crate::aether::build_runtime(&state));

    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .merge(aether.with_state(()))
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state)
}

/// 只有 s3s 连错误响应都序列化失败时才到达，近乎不可达的兜底 —— 包括 body 超限、
/// 格式非法在内的 HTTP 层错误都已在 s3s 的 `ops::call` 内 XML 化了。
async fn handle_s3_error(err: s3s::HttpError) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        err.into_std_error().to_string(),
    )
}
