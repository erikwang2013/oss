//! `accounts_api` 的请求校验、授权与 id 生成辅助（拆分以保持 handler 文件 <500 行）。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::console::{forbidden, internal_error};
use crate::state::AppState;

fn is_manager(role: Option<&str>) -> bool {
    matches!(role, Some("owner" | "admin"))
}

/// 全局管理员或本账户 manager。
#[allow(clippy::result_large_err)]
pub(crate) async fn require_manager(
    state: &AppState,
    uid: i64,
    is_admin: bool,
    account_id: &str,
) -> Result<(), Response> {
    if is_admin {
        return Ok(());
    }
    match state.db.member_role(account_id, uid).await {
        Ok(role) if is_manager(role.as_deref()) => Ok(()),
        Ok(_) => Err(forbidden("需要账户管理员权限")),
        Err(e) => {
            tracing::error!(error = %e, "查询账户角色失败");
            Err(internal_error())
        }
    }
}

/// 全局管理员或本账户任意成员（只读视角）。
#[allow(clippy::result_large_err)]
pub(crate) async fn require_member(
    state: &AppState,
    uid: i64,
    is_admin: bool,
    account_id: &str,
) -> Result<(), Response> {
    if is_admin {
        return Ok(());
    }
    match state.db.member_role(account_id, uid).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(forbidden("无权访问该账户")),
        Err(e) => {
            tracing::error!(error = %e, "查询账户角色失败");
            Err(internal_error())
        }
    }
}

/// 旧直写 API 的账户前置检查：保留 404 / “账户已停用” 的状态与文案。
/// 只读本地副本，apply 仍是最终权威（并发停用会由 apply 拒绝）。
#[allow(clippy::result_large_err)]
pub(crate) async fn require_active_account(state: &AppState, id: &str) -> Result<(), Response> {
    match state.db.get_account(id).await {
        Ok(None) => Err(StatusCode::NOT_FOUND.into_response()),
        Ok(Some(a)) if a.status != "active" => Err(bad("账户已停用")),
        Ok(Some(_)) => Ok(()),
        Err(e) => {
            tracing::error!(error = %e, "查询账户失败");
            Err(internal_error())
        }
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn valid_name(name: &str) -> Result<&str, Response> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 128 {
        return Err((StatusCode::BAD_REQUEST, "账户名长度必须为 1..=128").into_response());
    }
    Ok(name)
}

pub(crate) fn bad(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, msg.to_string()).into_response()
}

/// 12 位账户 id：与 `Db::create_account` 同分布；命令只携带算好的值。
pub(crate) async fn new_account_id(state: &AppState) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT printf('%012d', 100000000000 + ((random() & 0x7fffffffffffffff) % 900000000000))",
    )
    .fetch_one(state.db.pool())
    .await
}
