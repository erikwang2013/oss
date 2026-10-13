//! 控制台 API：登录、`/me`、桶/对象、上传记录、访问密钥，以及**子账户与权限管理**（管理员）。
//!
//! 鉴权由 `router.rs` 在 `/api/v1` 上挂的 `jwt-rust` `JwtLayer` 统一做（`/login` 在 except
//! 名单内），handler 从 extensions 取 payload；除 `/login` 外无匿名可达路径。
//! 授权（谁能做什么）走三面共用的 `xl_s3::access::Acl`：管理员全量；子账户按
//! permissions 三档（读/写/删）+ 自有桶全权；403 用纯文本（前端 `rawApi` 直接展示 resp.text）。

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use jwt_rust::JwtPayload;
use serde::Deserialize;
use xl_s3::access::{Acl, Action, AuthedUser};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// 取 JWT 载荷里的用户 id（登录签发时 `sub` = uid 十进制字符串）。
/// 签名/过期已由 JwtLayer 校验；这里只剩形状兜底（理论上到不了）。
/// 兜底的 401 与 JwtLayer 用同一 JSON 形态，避免同面两种 401 body。
// Err 是 axum 的 `Response`（handler 惯例，`?` 直接把它变成响应）；装箱只会把这点体量
// 挪去别处并让调用点解箱。http 1.5 起 `Response<Body>` 越过 clippy 的 128 字节阈值，
// 每请求至多构造一次，不值得为它改签名。
#[allow(clippy::result_large_err)]
fn uid_of(payload: &JwtPayload) -> Result<i64, Response> {
    payload.sub().and_then(|s| s.parse().ok()).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"code": 401, "msg": "invalid token payload", "data": null})),
        )
            .into_response()
    })
}

/// 403（纯文本——前端 `rawApi` 直接把 resp.text() 当错误消息展示，零解析）。
fn forbidden(msg: &str) -> Response {
    (StatusCode::FORBIDDEN, msg.to_string()).into_response()
}

fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

/// JWT 有效但用户在库里没了（被删）：401 与 JwtLayer 同形，前端会清 token 回登录页。
fn stale_user() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({"code": 401, "msg": "用户不存在或已失效", "data": null})),
    )
        .into_response()
}

/// uid + is_admin 一次取出（普通登录路由用）。**被停用的账号视同失效**（旧 token 立即作废）。
#[allow(clippy::result_large_err)]
async fn who(state: &AppState, payload: &JwtPayload) -> Result<(i64, bool), Response> {
    let uid = uid_of(payload)?;
    match state.db.get_user(uid).await {
        Ok(Some((_, is_admin, true))) => Ok((uid, is_admin)),
        Ok(Some((_, _, false))) => Err(stale_user()),
        Ok(None) => Err(stale_user()),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            Err(internal_error())
        }
    }
}

/// 管理员专属路由的收口（返回 uid）。
#[allow(clippy::result_large_err)]
async fn admin_of(state: &AppState, payload: &JwtPayload) -> Result<i64, Response> {
    let (uid, is_admin) = who(state, payload).await?;
    if is_admin {
        Ok(uid)
    } else {
        Err(forbidden("需要管理员权限"))
    }
}

/// 桶级权限门（与 S3 面、aether 面共用同一 Acl）。
#[allow(clippy::result_large_err)]
async fn require_perm(
    state: &AppState,
    uid: i64,
    is_admin: bool,
    bucket: &str,
    action: Action,
) -> Result<(), Response> {
    let acl = Acl::new(state.db.pool().clone());
    match acl.can(AuthedUser { uid, is_admin }, action, bucket).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(forbidden("无权访问该桶")),
        Err(e) => {
            tracing::error!(error = %e, "判权失败");
            Err(internal_error())
        }
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/login", post(login))
        .route("/me", get(me))
        .route("/me/password", post(change_my_password))
        .route("/groups", get(list_groups))
        .route("/buckets", get(list_buckets).post(create_bucket))
        .route("/buckets/{bucket}", delete(delete_bucket))
        .route("/buckets/{bucket}/objects", get(list_objects))
        .route(
            "/buckets/{bucket}/objects/{*key}",
            get(get_object).delete(delete_object),
        )
        .route("/uploads", get(list_uploads))
        .route("/uploads/register", post(register_upload))
        .route(
            "/access-keys",
            get(list_access_keys).post(create_access_key),
        )
        // 子账户管理（管理员）
        .route("/users", get(list_users).post(create_user))
        .route("/users/{id}", delete(delete_user))
        .route("/users/{id}/password", post(reset_password))
        .route("/users/{id}/status", put(set_user_status))
        .route(
            "/users/{id}/permissions",
            get(get_user_permissions).put(set_user_permissions),
        )
}

async fn login(State(state): State<AppState>, Json(body): Json<LoginRequest>) -> impl IntoResponse {
    match state.db.verify_user(&body.username, &body.password).await {
        Ok(Some(uid)) => {
            // 12h 过期在 JwtConfig.default_expire（state.rs）；sub=uid 供各 handler 还原
            let token = state
                .jwt
                .jwt()
                .encode(&serde_json::json!({ "sub": uid.to_string() }), None);
            match token {
                Ok(token) => {
                    (StatusCode::OK, Json(serde_json::json!({ "token": token }))).into_response()
                }
                Err(e) => {
                    tracing::error!(error = %e, "token 签发失败");
                    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
                }
            }
        }
        Ok(None) => (StatusCode::UNAUTHORIZED, "用户名或密码错误").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "登录查询失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

/// store 错误分档：可纠正的保留原消息，服务端内部的记日志 + 通用文案（5xx 不泄漏内部细节）。
fn store_error(e: xl_store::error::Error) -> (StatusCode, String) {
    use xl_store::error::Error;
    match &e {
        Error::NoSuchBucket(_) | Error::NoSuchKey(_) => (StatusCode::NOT_FOUND, e.to_string()),
        Error::InvalidBucketName(_) | Error::InvalidKey(_) => {
            (StatusCode::BAD_REQUEST, e.to_string())
        }
        // 这两个语义是「客户端可纠正的冲突」，S3 本身也用 409；不落 5xx
        Error::BucketAlreadyExists(_) | Error::BucketNotEmpty(_) => {
            (StatusCode::CONFLICT, e.to_string())
        }
        _ => {
            tracing::error!(error = %e, "控制台 store 操作失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        }
    }
}

/// 当前登录者（前端角色/菜单的唯一依据）。
async fn me(State(state): State<AppState>, Extension(payload): Extension<JwtPayload>) -> Response {
    let uid = match uid_of(&payload) {
        Ok(u) => u,
        Err(r) => return r,
    };
    match state.db.get_user(uid).await {
        Ok(Some((username, is_admin, true))) => Json(serde_json::json!({
            "id": uid, "username": username, "is_admin": is_admin
        }))
        .into_response(),
        Ok(Some((_, _, false))) => stale_user(),
        Ok(None) => stale_user(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            internal_error()
        }
    }
}

/// 上传分组（oss.toml 的 `[[groups]]`），供上传面板的分组下拉使用。
async fn list_groups(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.cfg.groups.clone()).into_response()
}

/// 桶列表（带能力位，前端按位显隐按钮）：管理员全量；子账户 = 可见集（自有 ∪ 任一授权位）
/// 与磁盘列表求交。**磁盘是权威**——历史桶可能没有 DB 行（owner NULL ⇒ 仅管理员可见）。
async fn list_buckets(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let buckets = match state.store.list_buckets().await {
        Ok(l) => l,
        Err(e) => return store_error(e).into_response(),
    };
    let owned: std::collections::HashSet<String> = match state.db.owned_buckets(uid).await {
        Ok(v) => v.into_iter().collect(),
        Err(e) => {
            tracing::error!(error = %e, "查询桶归属失败");
            return internal_error();
        }
    };
    let perms: std::collections::HashMap<String, (bool, bool, bool)> =
        match state.db.get_permissions(uid).await {
            Ok(rows) => rows
                .into_iter()
                .map(|p| (p.bucket, (p.read, p.write, p.delete)))
                .collect(),
            Err(e) => {
                tracing::error!(error = %e, "查询权限失败");
                return internal_error();
            }
        };
    let out: Vec<serde_json::Value> = buckets
        .into_iter()
        .filter_map(|b| {
            let own = owned.contains(&b.name);
            let (r, w, d) = perms.get(&b.name).copied().unwrap_or((false, false, false));
            let can_read = is_admin || own || r;
            let can_write = is_admin || own || w;
            let can_delete = is_admin || own || d;
            // 可见性：管理员全部；子账户 = 自有 ∪ 任一能力位（与可见集口径一致）
            if !(is_admin || own || r || w || d) {
                return None;
            }
            Some(serde_json::json!({
                "name": b.name,
                "is_owner": own,
                "can_read": can_read,
                "can_write": can_write,
                "can_delete": can_delete,
            }))
        })
        .collect();
    Json(out).into_response()
}

#[derive(Deserialize)]
struct CreateBucketBody {
    name: String,
}

/// 任何登录用户可建桶（建桶者即桶主，自动全权）。
async fn create_bucket(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<CreateBucketBody>,
) -> impl IntoResponse {
    let uid = match uid_of(&payload) {
        Ok(u) => u,
        Err(r) => return r,
    };
    if let Err(e) = state.store.create_bucket(&body.name).await {
        return store_error(e).into_response();
    }
    if let Err(e) = state.db.insert_bucket(&body.name, Some(uid)).await {
        // 桶已建成：控制面行缺失只影响列表展示，不失败请求
        tracing::warn!(error = %e, "写入桶行失败");
    }
    StatusCode::NO_CONTENT.into_response()
}

/// 删桶限桶主或管理员；删后清记账（授权行 + 桶行，防重建同名桶继承旧授权）。
async fn delete_bucket(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !is_admin {
        match Acl::new(state.db.pool().clone())
            .is_owner(&bucket, uid)
            .await
        {
            Ok(true) => {}
            Ok(false) => return forbidden("只有桶主或管理员可以删除桶"),
            Err(e) => {
                tracing::error!(error = %e, "查询桶主失败");
                return internal_error();
            }
        }
    }
    match state.store.delete_bucket(&bucket).await {
        Ok(()) => {
            if let Err(e) = Acl::new(state.db.pool().clone())
                .forget_bucket(&bucket)
                .await
            {
                // 桶已删除：记账行残留只影响列表展示，不失败请求
                tracing::warn!(error = ?e, "清理桶记账失败");
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => store_error(e).into_response(),
    }
}

#[derive(Deserialize)]
struct ListQuery {
    prefix: Option<String>,
    delimiter: Option<String>,
    /// 翻页参数按 spec §0.3 命名，透传给 store 的 max_keys / continuation_token
    limit: Option<usize>,
    cursor: Option<String>,
}

async fn list_objects(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
    axum::extract::Query(q): axum::extract::Query<ListQuery>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Read).await {
        return r;
    }
    let req = xl_store::store::ListRequest {
        prefix: q.prefix,
        delimiter: q.delimiter,
        max_keys: q.limit,
        continuation_token: q.cursor,
    };
    match state.store.list_objects(&bucket, &req).await {
        Ok(r) => Json(serde_json::json!({
            "objects": r.objects.iter().map(|o| serde_json::json!({
                "key": o.key, "size": o.size, "etag": o.etag, "content_type": o.content_type
            })).collect::<Vec<_>>(),
            "common_prefixes": r.common_prefixes,
            "is_truncated": r.is_truncated,
            // next_cursor 是 spec §0.3 的契约名；next_token 为既有字段，保留不删
            "next_token": r.next_token.clone(),
            "next_cursor": r.next_token,
        }))
        .into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

async fn get_object(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Read).await {
        return r;
    }
    match state.store.get_object(&bucket, &key).await {
        Ok((info, bytes)) => {
            let ct = info
                .content_type
                .unwrap_or_else(|| "application/octet-stream".into());
            ([(axum::http::header::CONTENT_TYPE, ct)], bytes).into_response()
        }
        Err(e) => store_error(e).into_response(),
    }
}

async fn delete_object(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Delete).await {
        return r;
    }
    match state.store.delete_object(&bucket, &key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

/// 上传记录：管理员全部；子账户仅自己的（owner_id = uid）。
async fn list_uploads(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let rows = if is_admin {
        state.db.list_uploads(100).await
    } else {
        state.db.list_uploads_for(uid, 100).await
    };
    match rows {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询上传记录失败");
            internal_error()
        }
    }
}

#[derive(Deserialize)]
struct RegisterBody {
    saved_path: String,
    original_name: String,
}

async fn register_upload(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<RegisterBody>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    // 登记前判权：必须对被登记记录的目标桶有写权限（修「任意挂名」洞的前半；
    // 后半 = 非管理员只能认领无归属/自己的记录，见 db.register_upload）
    let bucket = match state.db.upload_bucket(&body.saved_path).await {
        Ok(Some(b)) => b,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询上传记录失败");
            return internal_error();
        }
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Write).await {
        return r;
    }
    match state
        .db
        .register_upload(&body.saved_path, &body.original_name, uid, is_admin)
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        // DB 故障是服务端故障（与 store_error 的 5xx 分档一致）
        Err(e) => {
            tracing::error!(error = %e, "登记上传记录失败");
            internal_error()
        }
    }
}

async fn list_access_keys(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> impl IntoResponse {
    let uid = match uid_of(&payload) {
        Ok(u) => u,
        Err(r) => return r,
    };
    match state.db.list_access_keys(uid).await {
        Ok(keys) => Json(keys).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询访问密钥失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

#[derive(Deserialize)]
struct CreateKeyBody {
    access_key: Option<String>,
    secret_key: Option<String>,
}

/// 用户自定 AK/SK 的最小校验：≥16 字节、ASCII 字母数字与 `_-+/=`（覆盖 S3 形态的
/// 字母数字 AK 与 base64 形态的 SK）；不满足直接 400，不落到 DB 层。
fn valid_credential(s: &str) -> bool {
    s.len() >= 16
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'+' | b'/' | b'='))
}

async fn create_access_key(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<CreateKeyBody>,
) -> impl IntoResponse {
    let uid = match uid_of(&payload) {
        Ok(u) => u,
        Err(r) => return r,
    };
    if let Some(ak) = body.access_key.as_deref().filter(|s| !valid_credential(s)) {
        return (
            StatusCode::BAD_REQUEST,
            format!("access key 非法：需 ≥16 位且仅限字母数字与 _-+/=（{ak}）"),
        )
            .into_response();
    }
    if body
        .secret_key
        .as_deref()
        .is_some_and(|s| !valid_credential(s))
    {
        return (
            StatusCode::BAD_REQUEST,
            "secret key 非法：需 ≥16 位且仅限字母数字与 _-+/=".to_string(),
        )
            .into_response();
    }
    let ak = body
        .access_key
        .unwrap_or_else(|| format!("OSS{}", uuid::Uuid::new_v4().simple()));
    let sk = body
        .secret_key
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    match state.db.create_access_key(&ak, &sk, uid).await {
        Ok(()) => Json(serde_json::json!({ "access_key": ak, "secret_key": sk })).into_response(),
        // UNIQUE 冲突说人话；不落 sqlite 原文
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            (StatusCode::BAD_REQUEST, "access key 已存在").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "写入访问密钥失败");
            internal_error()
        }
    }
}

// ---- 子账户管理（仅管理员）----

/// 用户列表（含各自权限摘要）。
async fn list_users(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    match state.db.list_users().await {
        Ok(users) => Json(users).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户列表失败");
            internal_error()
        }
    }
}

#[derive(Deserialize)]
struct CreateUserBody {
    username: String,
    password: String,
}

/// 建子账户。**API 恒 is_admin=false**（禁经 API 造管理员，防提权）；
/// 口令 ≥8 位（空/短口令会被登录路径命中——引导段已有教训）。
async fn create_user(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<CreateUserBody>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    let username = body.username.trim();
    if username.is_empty() {
        return (StatusCode::BAD_REQUEST, "用户名不能为空").into_response();
    }
    if body.password.len() < 8 {
        return (StatusCode::BAD_REQUEST, "口令过短（至少 8 位）").into_response();
    }
    match state.db.create_user(username, &body.password, false).await {
        Ok(id) => Json(serde_json::json!({ "id": id })).into_response(),
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            (StatusCode::BAD_REQUEST, "用户名已存在").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "创建用户失败");
            internal_error()
        }
    }
}

/// 删子账户：不能删自己、不能删最后一个管理员；级联清 keys/permissions（owner 置 NULL）。
async fn delete_user(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<i64>,
) -> Response {
    let me = match admin_of(&state, &payload).await {
        Ok(u) => u,
        Err(r) => return r,
    };
    if id == me {
        return (StatusCode::BAD_REQUEST, "不能删除自己").into_response();
    }
    match state.db.get_user(id).await {
        Ok(Some((_, true, _))) => match state.db.count_admins().await {
            Ok(n) if n <= 1 => {
                return (StatusCode::BAD_REQUEST, "不能删除最后一个管理员").into_response();
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, "统计管理员失败");
                return internal_error();
            }
        },
        Ok(Some((_, false, _))) => {}
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    }
    match state.db.delete_user(id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "删除用户失败");
            internal_error()
        }
    }
}

#[derive(Deserialize)]
struct ResetPasswordBody {
    password: String,
}

/// 重置子账户口令。
async fn reset_password(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<i64>,
    Json(body): Json<ResetPasswordBody>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    if body.password.len() < 8 {
        return (StatusCode::BAD_REQUEST, "口令过短（至少 8 位）").into_response();
    }
    match state.db.set_password(id, &body.password).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "重置口令失败");
            internal_error()
        }
    }
}

/// 某子账户的权限行（全量）。
async fn get_user_permissions(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<i64>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    match state.db.get_permissions(id).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询权限失败");
            internal_error()
        }
    }
}

#[derive(Deserialize)]
struct PermBody {
    bucket: String,
    #[serde(default)]
    read: bool,
    #[serde(default)]
    write: bool,
    #[serde(default)]
    delete: bool,
}

/// 全量替换某子账户的权限行（管理员的 PUT 语义）。
async fn set_user_permissions(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<i64>,
    Json(body): Json<Vec<PermBody>>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    match state.db.get_user(id).await {
        Ok(Some(_)) => {}
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    }
    let rows: Vec<crate::db::PermissionRow> = body
        .into_iter()
        .map(|p| crate::db::PermissionRow {
            bucket: p.bucket,
            read: p.read,
            write: p.write,
            delete: p.delete,
        })
        .collect();
    match state.db.set_permissions(id, &rows).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "写入权限失败");
            internal_error()
        }
    }
}

// ---- 自助改密与账号启停（遗留清单清理）----

#[derive(Deserialize)]
struct ChangePasswordBody {
    old_password: String,
    new_password: String,
}

/// 自助改密（任何登录者，需验旧口令）。改密后旧 JWT 不失效（sub 未变）——接受，
/// 必要时可由管理员停用账号强制全员重登。
async fn change_my_password(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<ChangePasswordBody>,
) -> Response {
    let (uid, _) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if body.new_password.len() < 8 {
        return (StatusCode::BAD_REQUEST, "新口令过短（至少 8 位）").into_response();
    }
    let username = match state.db.get_user(uid).await {
        Ok(Some((name, _, _))) => name,
        _ => return stale_user(),
    };
    match state.db.verify_user(&username, &body.old_password).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::BAD_REQUEST, "旧口令不正确").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "校验旧口令失败");
            return internal_error();
        }
    }
    match state.db.set_password(uid, &body.new_password).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "改密失败");
            internal_error()
        }
    }
}

#[derive(Deserialize)]
struct UserStatusBody {
    active: bool,
}

/// 启停用账号（管理员）。停用即全链路失效：登录 401、既有 JWT 下一请求 401、
/// S3 的 AK 403、上传面 401。防呆：不能停用自己、不能停用最后一个在册管理员。
async fn set_user_status(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<i64>,
    Json(body): Json<UserStatusBody>,
) -> Response {
    let me = match admin_of(&state, &payload).await {
        Ok(u) => u,
        Err(r) => return r,
    };
    if id == me && !body.active {
        return (StatusCode::BAD_REQUEST, "不能停用自己").into_response();
    }
    match state.db.get_user(id).await {
        Ok(Some((_, true, _))) if !body.active => match state.db.count_active_admins().await {
            Ok(n) if n <= 1 => {
                return (StatusCode::BAD_REQUEST, "不能停用最后一个在册管理员").into_response();
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, "统计管理员失败");
                return internal_error();
            }
        },
        Ok(Some(_)) => {}
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    }
    match state.db.set_user_status(id, body.active).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "更新用户状态失败");
            internal_error()
        }
    }
}
