//! 一个 Router 承载全部 HTTP 面：控制台 + aetherupload + S3 fallback（M1 后续任务往这里加）。

use std::sync::Arc;

use axum::Router;
use axum::error_handling::HandleError;
use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::get;
use s3s::service::S3ServiceBuilder;

use crate::console::Claims;
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
    //
    // 鉴权层（规格 §0.1「会话 Cookie/JWT 覆盖层」）：闭包捕获 jwt_secret，不把 aether
    // 路由挂上 AppState —— 上面的 `with_state(())` 口径不变。
    let jwt_secret = state.cfg.jwt_secret.clone();
    let aether = aetherupload::integrations::ecat::routes(crate::aether::build_runtime(&state))
        .layer(middleware::from_fn(move |req: Request, next: Next| {
            let secret = jwt_secret.clone();
            async move {
                if !aether_authorized(req.headers(), &secret) {
                    // 纯文本、不区分「缺 / 坏 / 过期」，不泄漏细节
                    return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
                }
                next.run(req).await
            }
        }));

    Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .nest("/api/v1", crate::console::routes())
        .merge(aether.with_state(()))
        .fallback_service(HandleError::new(s3, handle_s3_error))
        .with_state(state)
}

/// 只有 s3s 连错误响应都序列化失败时才到达，近乎不可达的兜底 —— 包括 body 超限、
/// 格式非法在内的 HTTP 层错误都已在 s3s 的 `ops::call` 内 XML 化了。
async fn handle_s3_error(err: s3s::HttpError) -> (axum::http::StatusCode, String) {
    tracing::error!(error = ?err, "s3s 错误响应失败（兜底 500）");
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "internal error".to_string(),
    )
}

/// aether 路由的凭据来源：cookie `oss_token` 优先，其次 `Authorization: Bearer <jwt>`
/// （curl/工具便利）。cookie 手写分号切分 —— 只取一个字段，不引 cookie 库。
/// 空值（`oss_token=`）等同于没有 cookie：不遮蔽 Bearer 回退。
fn aether_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|cookies| {
            cookies
                .split(';')
                .filter_map(|part| part.trim().strip_prefix("oss_token="))
                .find(|v| !v.is_empty())
        })
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
        })
}

/// 与 console.rs 同款校验（同 `Claims`、同 secret、同 `Validation::default()`）。
fn aether_authorized(headers: &HeaderMap, secret: &str) -> bool {
    aether_token(headers).is_some_and(|raw| {
        jsonwebtoken::decode::<Claims>(
            raw,
            &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
            &jsonwebtoken::Validation::default(),
        )
        .is_ok()
    })
}
