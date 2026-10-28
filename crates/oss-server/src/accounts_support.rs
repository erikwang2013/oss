//! `accounts_api` 的请求校验、授权与 id 生成辅助（拆分以保持 handler 文件 <500 行）。

use std::time::Duration;

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
    let role = member_role_caught_up(state, uid, account_id, is_manager).await?;
    if is_manager(role.as_deref()) {
        Ok(())
    } else {
        Err(forbidden("需要账户管理员权限"))
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
    let role = member_role_caught_up(state, uid, account_id, |r| r.is_some()).await?;
    if role.is_some() {
        Ok(())
    } else {
        Err(forbidden("无权访问该账户"))
    }
}

/// Raft 下本地成员行可能落后于 leader 已提交的挂载/改角色：本地 miss 或角色不足时，
/// 等本地 apply 追上 leader 已应用点后重查一次；standalone 不等待，行为不变。
/// 权限判定必须用这里返回的（已追赶的）行——追赶后仍不足则调用方照旧拒绝。
#[allow(clippy::result_large_err)]
async fn member_role_caught_up(
    state: &AppState,
    uid: i64,
    account_id: &str,
    sufficient: impl Fn(Option<&str>) -> bool,
) -> Result<Option<String>, Response> {
    let mut role = local_member_role(state, account_id, uid).await?;
    if !sufficient(role.as_deref())
        && state.control.is_raft()
        && state.control.wait_caught_up(USER_LOOKUP_CATCH_UP).await
    {
        role = local_member_role(state, account_id, uid).await?;
    }
    Ok(role)
}

#[allow(clippy::result_large_err)]
async fn local_member_role(
    state: &AppState,
    account_id: &str,
    uid: i64,
) -> Result<Option<String>, Response> {
    state.db.member_role(account_id, uid).await.map_err(|e| {
        tracing::error!(error = %e, "查询账户角色失败");
        internal_error()
    })
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

/// 滞后 follower 的本地副本重查上限（用户/成员行）：只等一次、有界；超时按本地结果拒绝（可重试）。
const USER_LOOKUP_CATCH_UP: Duration = Duration::from_secs(2);

#[allow(clippy::result_large_err)]
async fn local_attachable_username(state: &AppState, uid: i64) -> Result<Option<String>, Response> {
    match state.db.get_user(uid).await {
        Ok(Some((username, false, _))) => Ok(Some(username)),
        Ok(_) => Ok(None),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            Err(internal_error())
        }
    }
}

/// attach_member 的 uid→username 解析：只接受非管理员用户（其余一律 404）。
/// Raft 下本地 users 副本可能落后于已提交日志：本地 miss 时等本地 apply 追上
/// leader 已应用点后重查一次；standalone 不等待，行为不变。
#[allow(clippy::result_large_err)]
pub(crate) async fn attachable_username(state: &AppState, uid: i64) -> Result<String, Response> {
    if let Some(username) = local_attachable_username(state, uid).await? {
        return Ok(username);
    }
    if state.control.is_raft()
        && state.control.wait_caught_up(USER_LOOKUP_CATCH_UP).await
        && let Some(username) = local_attachable_username(state, uid).await?
    {
        return Ok(username);
    }
    Err(StatusCode::NOT_FOUND.into_response())
}

/// set_member_status 的目标解析：(role, username, is_admin)。
/// 与 [`attachable_username`] 同理，Raft 下本地副本可能落后：本地 member/user 任一
/// miss 时等本地 apply 追上 leader 已应用点后重查一次；standalone 不等待，行为不变。
/// 两次都缺 → 404；权限判定必须用这里返回的（已追赶的）行。
#[allow(clippy::result_large_err)]
pub(crate) async fn member_status_target(
    state: &AppState,
    account_id: &str,
    uid: i64,
) -> Result<(String, String, bool), Response> {
    let mut local = local_member_status(state, account_id, uid).await?;
    if (local.0.is_none() || local.1.is_none())
        && state.control.is_raft()
        && state.control.wait_caught_up(USER_LOOKUP_CATCH_UP).await
    {
        local = local_member_status(state, account_id, uid).await?;
    }
    match local {
        (Some(role), Some((username, is_admin))) => Ok((role, username, is_admin)),
        _ => Err(StatusCode::NOT_FOUND.into_response()),
    }
}

#[allow(clippy::result_large_err)]
async fn local_member_status(
    state: &AppState,
    account_id: &str,
    uid: i64,
) -> Result<(Option<String>, Option<(String, bool)>), Response> {
    let role = state.db.member_role(account_id, uid).await.map_err(|e| {
        tracing::error!(error = %e, "查询账户成员失败");
        internal_error()
    })?;
    let user = state
        .db
        .get_user(uid)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "查询用户失败");
            internal_error()
        })?
        .map(|(username, is_admin, _)| (username, is_admin));
    Ok((role, user))
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
