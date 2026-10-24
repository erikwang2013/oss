//! AC-0/AC-1/AC-2：多租户账户控制台 API（standalone 模式）。
//!
//! - 全局管理员：账户 CRUD、配额、启停、成员挂载；
//! - 账户管理员（role=owner/admin）：仅本账户——列成员、建用户（恒为 member）、停用 member；
//! - 其它成员：只读本账户与成员列表。
//!
//! 账户/成员表不参与 Raft 复制（0005 迁移头注），因此**所有写路由在 Raft 模式一律 503**，
//! 读路由使用启动期回填的本地派生数据。授权失败一律 403（纯文本，与 console 同形）。

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use jwt_rust::JwtPayload;
use serde::Deserialize;

use crate::console::{forbidden, internal_error, who};
use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/accounts", get(list_accounts).post(create_account))
        .route(
            "/accounts/{id}",
            get(get_account).put(update_account).delete(delete_account),
        )
        .route("/accounts/{id}/quota", put(set_quota))
        .route(
            "/accounts/{id}/members",
            get(list_members).post(attach_member),
        )
        .route("/accounts/{id}/users", post(create_user))
        .route(
            "/accounts/{id}/members/{uid}/status",
            put(set_member_status),
        )
}

/// 账户写接口只在 standalone 落地（Raft 快照不含本表，跨站点写会分叉）。
fn require_standalone(state: &AppState) -> Option<Response> {
    state.control.is_raft().then(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "accounts API is not available in raft mode",
        )
            .into_response()
    })
}

fn is_manager(role: Option<&str>) -> bool {
    matches!(role, Some("owner" | "admin"))
}

/// 全局管理员或本账户 manager。
#[allow(clippy::result_large_err)]
async fn require_manager(
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
async fn require_member(
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

#[allow(clippy::result_large_err)]
fn valid_name(name: &str) -> Result<&str, Response> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 128 {
        return Err((StatusCode::BAD_REQUEST, "账户名长度必须为 1..=128").into_response());
    }
    Ok(name)
}

fn bad(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, msg.to_string()).into_response()
}

async fn list_accounts(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match state.db.list_accounts((!is_admin).then_some(uid)).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询账户列表失败");
            internal_error()
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateAccountBody {
    name: String,
    #[serde(default)]
    bucket_quota: Option<i64>,
}

async fn create_account(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<CreateAccountBody>,
) -> Response {
    if let Some(r) = require_standalone(&state) {
        return r;
    }
    let (_, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !is_admin {
        return forbidden("需要管理员权限");
    }
    let name = match valid_name(&body.name) {
        Ok(n) => n,
        Err(r) => return r,
    };
    if body.bucket_quota.is_some_and(|q| q < 0) {
        return bad("bucket_quota 必须 >= 0 或省略");
    }
    match state.db.create_account(name, body.bucket_quota).await {
        Ok(id) => Json(serde_json::json!({ "id": id })).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "创建账户失败");
            internal_error()
        }
    }
}

async fn get_account(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
) -> Response {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_member(&state, uid, is_admin, &id).await {
        return r;
    }
    match state.db.get_account(&id).await {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询账户失败");
            internal_error()
        }
    }
}

#[derive(Debug, Deserialize)]
struct UpdateAccountBody {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

async fn update_account(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
    Json(body): Json<UpdateAccountBody>,
) -> Response {
    if let Some(r) = require_standalone(&state) {
        return r;
    }
    let (_, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !is_admin {
        return forbidden("需要管理员权限");
    }
    if let Some(name) = &body.name {
        let name = match valid_name(name) {
            Ok(n) => n,
            Err(r) => return r,
        };
        match state.db.rename_account(&id, name).await {
            Ok(true) => {}
            Ok(false) => return StatusCode::NOT_FOUND.into_response(),
            Err(e) => {
                tracing::error!(error = %e, "重命名账户失败");
                return internal_error();
            }
        }
    }
    if let Some(status) = &body.status {
        if status != "active" && status != "disabled" {
            return bad("status 只能是 active 或 disabled");
        }
        match state.db.set_account_status(&id, status).await {
            Ok(true) => {}
            // 条件更新失败：不存在，或账户内仍有在册全局管理员（防锁死）
            Ok(false) => {
                return match state.db.get_account(&id).await {
                    Ok(None) => StatusCode::NOT_FOUND.into_response(),
                    Ok(Some(_)) => bad("账户内仍有在册全局管理员，不能停用"),
                    Err(e) => {
                        tracing::error!(error = %e, "查询账户失败");
                        internal_error()
                    }
                };
            }
            Err(e) => {
                tracing::error!(error = %e, "更新账户状态失败");
                return internal_error();
            }
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Debug, Deserialize)]
struct QuotaBody {
    /// `null` = 取消配额（不限）。
    #[serde(default)]
    bucket_quota: Option<i64>,
}

async fn set_quota(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
    Json(body): Json<QuotaBody>,
) -> Response {
    if let Some(r) = require_standalone(&state) {
        return r;
    }
    let (_, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !is_admin {
        return forbidden("需要管理员权限");
    }
    if body.bucket_quota.is_some_and(|q| q < 0) {
        return bad("bucket_quota 必须 >= 0 或 null");
    }
    match state.db.set_account_quota(&id, body.bucket_quota).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "更新账户配额失败");
            internal_error()
        }
    }
}

async fn delete_account(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
) -> Response {
    if let Some(r) = require_standalone(&state) {
        return r;
    }
    let (_, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !is_admin {
        return forbidden("需要管理员权限");
    }
    if id == xl_s3::access::LEGACY_ACCOUNT {
        return bad("系统账户不可删除");
    }
    match state.db.delete_account(&id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => match state.db.get_account(&id).await {
            Ok(None) => StatusCode::NOT_FOUND.into_response(),
            Ok(Some(_)) => (StatusCode::CONFLICT, "账户仍有成员或桶，不能删除").into_response(),
            Err(e) => {
                tracing::error!(error = %e, "查询账户失败");
                internal_error()
            }
        },
        Err(e) => {
            tracing::error!(error = %e, "删除账户失败");
            internal_error()
        }
    }
}

async fn list_members(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
) -> Response {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_member(&state, uid, is_admin, &id).await {
        return r;
    }
    match state.db.list_account_members(&id).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询账户成员失败");
            internal_error()
        }
    }
}

#[derive(Debug, Deserialize)]
struct AttachMemberBody {
    uid: i64,
    /// admin | member（owner 只留给回填的派生账户）
    #[serde(default = "default_role")]
    role: String,
}

fn default_role() -> String {
    "member".to_string()
}

async fn attach_member(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
    Json(body): Json<AttachMemberBody>,
) -> Response {
    if let Some(r) = require_standalone(&state) {
        return r;
    }
    let (_, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    // 挂载已有用户会改变其账户根 ARN 的解析范围 → 只允许全局管理员（防账户管理员拉人提权）。
    if !is_admin {
        return forbidden("仅全局管理员可挂载已有用户");
    }
    if id == xl_s3::access::LEGACY_ACCOUNT {
        return bad("系统账户不能挂载成员");
    }
    if body.role != "admin" && body.role != "member" {
        return bad("role 只能是 admin 或 member");
    }
    match state.db.get_account(&id).await {
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Ok(Some(a)) if a.status != "active" => return bad("账户已停用"),
        Ok(Some(_)) => {}
        Err(e) => {
            tracing::error!(error = %e, "查询账户失败");
            return internal_error();
        }
    }
    match state
        .db
        .attach_account_member(&id, body.uid, &body.role)
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "挂载账户成员失败");
            internal_error()
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateUserBody {
    username: String,
    password: String,
}

async fn create_user(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
    Json(body): Json<CreateUserBody>,
) -> Response {
    if let Some(r) = require_standalone(&state) {
        return r;
    }
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_manager(&state, uid, is_admin, &id).await {
        return r;
    }
    if id == xl_s3::access::LEGACY_ACCOUNT {
        return bad("系统账户不能创建用户");
    }
    let username = body.username.trim();
    if username.is_empty() {
        return bad("用户名不能为空");
    }
    if body.password.len() < 8 {
        return bad("口令过短（至少 8 位）");
    }
    match state.db.get_account(&id).await {
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Ok(Some(a)) if a.status != "active" => return bad("账户已停用"),
        Ok(Some(_)) => {}
        Err(e) => {
            tracing::error!(error = %e, "查询账户失败");
            return internal_error();
        }
    }
    // 账户内新建用户恒为 member + 非全局管理员（禁提权）；角色升级由全局管理员挂载接口完成。
    match state
        .db
        .create_user_in_account(&id, username, &body.password, "member")
        .await
    {
        Ok(new_uid) => Json(serde_json::json!({ "id": new_uid })).into_response(),
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            bad("用户名已存在").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "账户内创建用户失败");
            internal_error()
        }
    }
}

#[derive(Debug, Deserialize)]
struct MemberStatusBody {
    active: bool,
}

async fn set_member_status(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path((id, target)): Path<(String, i64)>,
    Json(body): Json<MemberStatusBody>,
) -> Response {
    if let Some(r) = require_standalone(&state) {
        return r;
    }
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_manager(&state, uid, is_admin, &id).await {
        return r;
    }
    let role = match state.db.member_role(&id, target).await {
        Ok(Some(r)) => r,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询账户成员失败");
            return internal_error();
        }
    };
    let (_, target_is_admin, _) = match state.db.get_user(target).await {
        Ok(Some(v)) => v,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    };
    // 账户管理员不能碰全局管理员、同级 manager 或自己（防账户内互锁/越权）。
    if !is_admin && (target == uid || target_is_admin || role != "member") {
        return forbidden("账户管理员只能停用本账户的 member");
    }
    match state.db.set_user_status(target, body.active).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => {
            if matches!(state.db.get_user(target).await, Ok(Some(_))) {
                bad("不能停用最后一个在册管理员").into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "更新成员状态失败");
            internal_error()
        }
    }
}
