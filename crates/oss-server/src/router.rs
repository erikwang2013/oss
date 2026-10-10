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

    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state)
}

/// S3 业务错误（NoSuchBucket 等）由 s3s 在 ops 层序列化成带正确状态码的 XML 响应，不会走到这里；
/// 到这里的是 HTTP 层错误（body 过大、格式非法等）—— 兜底 500。
async fn handle_s3_error(err: s3s::HttpError) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        err.into_std_error().to_string(),
    )
}
