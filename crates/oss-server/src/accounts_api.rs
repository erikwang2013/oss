//! AC-0/AC-1/AC-2：多租户账户控制台 API。
//!
//! - 全局管理员：账户 CRUD、配额、启停、成员挂载；
//! - 账户管理员（role=owner/admin）：仅本账户——列成员、建用户（恒为 member）、停用 member；
//! - 其它成员：只读本账户与成员列表。
//!
//! 写路由一律经 `ControlPlane`（Raft = 复制命令；standalone = `Local` apply 同一事务），
//! 读路由使用本地已复制/已回填的 SQLite。授权失败一律 403（纯文本，与 console 同形）。

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use jwt_rust::JwtPayload;
use serde::Deserialize;

use crate::accounts_support::{
    bad, new_account_id, require_active_account, require_manager, require_member, valid_name,
};
use crate::accounts_usage::account_usage;
use crate::console::{
    control_failure, control_rejection, control_write_status, forbidden, internal_error, who,
};
use crate::raft::{ControlCommand, ControlResponse};
use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/accounts", get(list_accounts).post(create_account))
        .route(
            "/accounts/{id}",
            get(get_account).put(update_account).delete(delete_account),
        )
        .route("/accounts/{id}/quota", put(set_quota))
        .route("/accounts/{id}/usage", get(account_usage))
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
    let created_at = crate::db::now();
    // 与 db.rs 相同：id 冲突概率可忽略，最多重试 8 次（命令只带预生成 id）。
    for _ in 0..8 {
        let id = match new_account_id(&state).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %e, "生成账户 id 失败");
                return internal_error();
            }
        };
        let cmd = ControlCommand::AccountCreate {
            id: id.clone(),
            name: name.to_string(),
            bucket_quota: body.bucket_quota,
            created_at,
        };
        match state.control.write(cmd).await {
            Ok(ControlResponse::Ok) => {
                return Json(serde_json::json!({ "id": id })).into_response();
            }
            Ok(ControlResponse::Conflict) => continue,
            Ok(resp) => return control_rejection(resp, "账户创建冲突"),
            Err(e) => return control_failure(e),
        }
    }
    tracing::error!("account id generation collision (retry)");
    internal_error()
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
    let (_, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !is_admin {
        return forbidden("需要管理员权限");
    }
    let name = match &body.name {
        Some(name) => match valid_name(name) {
            Ok(n) => Some(n.to_string()),
            Err(r) => return r,
        },
        None => None,
    };
    if body
        .status
        .as_deref()
        .is_some_and(|s| s != "active" && s != "disabled")
    {
        return bad("status 只能是 active 或 disabled");
    }
    if name.is_none() && body.status.is_none() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let cmd = ControlCommand::AccountUpdate {
        id,
        name,
        status: body.status,
        updated_at: crate::db::now(),
    };
    match state.control.write(cmd).await {
        Ok(ControlResponse::Ok) => StatusCode::NO_CONTENT.into_response(),
        Ok(ControlResponse::NotFound) => StatusCode::NOT_FOUND.into_response(),
        // 旧直写路径：停用条件更新失败且账户存在 = 含在册全局管理员 → 400。
        Ok(ControlResponse::Conflict) => bad("账户内仍有在册全局管理员，不能停用"),
        Ok(resp) => control_rejection(resp, "账户更新冲突"),
        Err(e) => control_failure(e),
    }
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
    let cmd = ControlCommand::AccountSetQuota {
        id,
        bucket_quota: body.bucket_quota,
        updated_at: crate::db::now(),
    };
    control_write_status(state.control.write(cmd).await, "配额更新冲突")
}

async fn delete_account(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
) -> Response {
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
    control_write_status(
        state
            .control
            .write(ControlCommand::AccountDelete { id })
            .await,
        "账户仍有成员或桶，不能删除",
    )
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
    // Raft 下本节点副本可能落后：本地前置检查会在转发前误拒（Leader 上已生效的
    // 账户/用户）。只做 standalone 的文案兼容；Raft 交给命令走 leader 并返回其权威结果。
    if !state.control.is_raft()
        && let Err(r) = require_active_account(&state, &id).await
    {
        return r;
    }
    let username = match state.db.get_user(body.uid).await {
        Ok(Some((username, false, _))) => username,
        Ok(_) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    };
    let cmd = ControlCommand::AccountMemberAttach {
        account_id: id,
        username,
        role: body.role,
        created_at: crate::db::now(),
    };
    control_write_status(state.control.write(cmd).await, "成员挂载冲突")
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
    // 同 attach_member：Raft 副本滞后时本地预检会误拒；standalone 保留旧文案，
    // Raft 由 apply 的唯一键/账户状态做权威判定（重复名仍映射 400）。
    if !state.control.is_raft() {
        if let Err(r) = require_active_account(&state, &id).await {
            return r;
        }
        match state.db.user_exists(username).await {
            Ok(false) => {}
            Ok(true) => return bad("用户名已存在"),
            Err(e) => {
                tracing::error!(error = %e, "查询用户失败");
                return internal_error();
            }
        }
    }
    // 账户内新建用户恒为 member + 非全局管理员（禁提权）；角色升级由全局管理员挂载接口完成。
    let cmd = ControlCommand::AccountUserCreate {
        account_id: id,
        username: username.to_string(),
        password_hash: crate::db::hash_password(&body.password),
        role: "member".to_string(),
        created_at: crate::db::now(),
    };
    match state.control.write(cmd).await {
        Ok(ControlResponse::Ok) => {}
        // 与旧直写路径一致：唯一键冲突是 400，不是 409（apply 竞态同样收敛到这里）。
        Ok(ControlResponse::Conflict) => return bad("用户名已存在"),
        Ok(resp) => return control_rejection(resp, "用户名已存在"),
        Err(e) => return control_failure(e),
    }
    // 命令只带自然键；apply 后按 username 取本地 uid（follower 已在 client_write 内等 apply）。
    match sqlx::query_scalar::<_, i64>("SELECT id FROM users WHERE username = ?")
        .bind(username)
        .fetch_optional(state.db.pool())
        .await
    {
        Ok(Some(new_uid)) => Json(serde_json::json!({ "id": new_uid })).into_response(),
        Ok(None) => {
            tracing::error!(username, "账户内用户已建但回读失败");
            internal_error()
        }
        Err(e) => {
            tracing::error!(error = %e, "回读账户内用户失败");
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
    let (username, target_is_admin, _) = match state.db.get_user(target).await {
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
    let cmd = ControlCommand::AccountMemberSetStatus {
        account_id: id,
        username,
        active: body.active,
    };
    match state.control.write(cmd).await {
        Ok(ControlResponse::Ok) => StatusCode::NO_CONTENT.into_response(),
        Ok(ControlResponse::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Ok(ControlResponse::Conflict) => bad("不能停用最后一个在册管理员").into_response(),
        Ok(resp) => control_rejection(resp, "成员状态冲突"),
        Err(e) => control_failure(e),
    }
}
