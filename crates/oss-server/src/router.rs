//! 一个 Router 承载全部 HTTP 面：控制台 + aetherupload + S3 fallback（M1 后续任务往这里加）。

use axum::Router;
use axum::routing::get;

use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .with_state(state)
}
