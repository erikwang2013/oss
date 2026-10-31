//! 多密钥 JWT 鉴权层：先验当前密钥，再按配置顺序试轮换期旧密钥。
//!
//! 路径/401 语义与 jwt-rust 的 axum `JwtLayer` 逐条对齐（except 白名单、Bearer 提取、
//! `JwtPayload` extension、`unauthorized_body`），只是把单实例换成「当前 + 旧密钥」。
//! 每个 `JwtAuth` 共用同一黑名单目录，旧密钥签发的已撤销 token 同样被拒。

use axum::Json;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jwt_rust::native::unauthorized_body;
use jwt_rust::{JwtError, bearer_token};

use crate::state::AppState;

pub async fn multi_jwt(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    if state.jwt.matches_except(req.uri().path()) {
        return next.run(req).await;
    }
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(bearer_token);
    // 失败时保留当前密钥的错误：单密钥部署的 401 body 与替换前逐字相同。
    let mut current_err: Option<JwtError> = None;
    for auth in std::iter::once(&state.jwt).chain(&state.jwt_previous) {
        match auth.authenticate(token.as_deref()) {
            Ok(payload) => {
                req.extensions_mut().insert(payload);
                return next.run(req).await;
            }
            Err(e) => {
                if current_err.is_none() {
                    current_err = Some(e);
                }
            }
        }
    }
    let err = current_err.expect("state.jwt 至少尝试一次");
    tracing::info!(
        "JWT middleware rejected ({} {}): {err}",
        req.method(),
        req.uri().path()
    );
    (StatusCode::UNAUTHORIZED, Json(unauthorized_body(&err))).into_response()
}
