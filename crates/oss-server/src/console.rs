//! 控制台 API：登录、`/me`、桶/对象、上传记录、访问密钥，以及**子账户与权限管理**（管理员）。
//!
//! 鉴权由 `router.rs` 在 `/api/v1` 上挂的 `jwt-rust` `JwtLayer` 统一做（`/login` 在 except
//! 名单内），handler 从 extensions 取 payload；除 `/login` 外无匿名可达路径。
//! 授权（谁能做什么）走三面共用的 `xl_s3::access::Acl`：管理员全量；子账户按
//! permissions 三档（读/写/删）+ 自有桶全权；403 用纯文本（前端 `rawApi` 直接展示 resp.text）。

use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, post, put};
use axum::{Json, Router};
use jwt_rust::JwtPayload;
use serde::Deserialize;
use xl_s3::access::{Action, AuthedUser};
use xl_s3::events::EventSink as _;
use xl_s3::sync_delete::SyncDeleteSink as _;
use xl_store::store::VersioningMode;

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

/// M9 S6b：控制写失败 → HTTP（413/503 带原因；500 不泄漏内部）。
/// 503 的文本含 raft/forward 提示（3 节点转发与 leader hint 由 S7 接线）。
pub(crate) fn control_failure(e: crate::raft::control::ControlError) -> Response {
    match e.http_status() {
        413 => (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()).into_response(),
        503 => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
        _ => {
            tracing::error!(error = %e, "控制写失败");
            internal_error()
        }
    }
}

/// M9 S6b：apply 业务拒绝 → 与 `raft::control::response_status` 完全一致
/// （Ok 200 / BucketNotEmpty+Conflict 409 / NotFound 404 / Invalid 400），绝不静默 204。
/// `Ok` 由调用方处理，传进来按 500 兜底（不应发生）。
pub(crate) fn control_rejection(
    resp: crate::raft::ControlResponse,
    conflict: &'static str,
) -> Response {
    match resp {
        crate::raft::ControlResponse::NotFound => StatusCode::NOT_FOUND.into_response(),
        crate::raft::ControlResponse::BucketNotEmpty | crate::raft::ControlResponse::Conflict => {
            (StatusCode::CONFLICT, conflict).into_response()
        }
        crate::raft::ControlResponse::Invalid => {
            (StatusCode::BAD_REQUEST, "invalid control command").into_response()
        }
        crate::raft::ControlResponse::Ok => internal_error(),
    }
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
pub(crate) async fn who(state: &AppState, payload: &JwtPayload) -> Result<(i64, bool), Response> {
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
    let acl = state.acl();
    match acl
        .can(
            AuthedUser {
                uid,
                is_admin,
                ak: String::new(),
            },
            action,
            bucket,
        )
        .await
    {
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
        // 集群状态（管理员；单机优雅降级为 mode:"single"）
        .route("/cluster/status", get(cluster_status))
        // M8 S6 缺口 A：加密/KMS 只读状态（管理员；无变更端点）
        .route("/encryption", get(encryption_status))
        // M4 切片 6：作业记录（管理员）与桶生命周期规则（桶主或管理员）
        .route("/jobs", get(list_jobs))
        // M8 S0：作业详情/控制（admin 或 requested_by）
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/{action}", post(job_action))
        // M8 S3：Batch 创建（POST /jobs，与上面的 GET 合并）
        .merge(crate::job_actions_api::routes())
        // M8 S1：访问点 CRUD/策略
        .merge(crate::access_points_api::routes())
        // M8 S5：Inventory 配置（owner/admin）
        .merge(crate::inventory_api::routes())
        .route(
            "/buckets/{bucket}/lifecycle",
            get(get_lifecycle).put(put_lifecycle),
        )
        // M8 S4b：admin 强制跑一轮 Transition sweep；tier 只读状态（Task 6a 后端预备）
        .route("/tiers", get(list_tiers))
        .route("/tiers/sweep", post(force_tier_sweep))
        .route("/tiers/gc", post(tier_gc))
        // Task 6a：桶默认加密读/写/清（与 S3 面同源）
        .route(
            "/buckets/{bucket}/encryption",
            get(get_bucket_encryption)
                .put(put_bucket_encryption)
                .delete(delete_bucket_encryption),
        )
        // M4 切片 7：桶策略（桶主或管理员）与审计（管理员）
        .route("/buckets/{bucket}/policy", get(get_policy).put(put_policy))
        // M8 S6 缺口 B：桶事件通知配置（S3 面同源）与一次性 Select 代理
        .route(
            "/buckets/{bucket}/notifications",
            get(get_notifications)
                .put(put_notifications)
                .delete(delete_notifications),
        )
        .route("/buckets/{bucket}/select", post(select_object))
        // M8 S6 缺口 B：死信可见性/重投（管理员）
        .route("/events/dead", get(list_dead_events))
        .route("/events/{id}/redeliver", post(redeliver_event))
        // M8 S6 缺口 B：IAM 预设模板（登录即可读；不含敏感信息）
        .route("/policy-templates", get(list_policy_templates))
        .route("/audit", get(list_audit))
        .route("/buckets", get(list_buckets).post(create_bucket))
        .route("/buckets/{bucket}", delete(delete_bucket))
        .route("/buckets/{bucket}/objects", get(list_objects))
        .route(
            "/buckets/{bucket}/objects/{*key}",
            get(get_object).delete(delete_object),
        )
        // M8 S6 缺口 A：对象标签（链尾版本）
        .route(
            "/buckets/{bucket}/tags/{*key}",
            get(get_object_tags)
                .put(put_object_tags)
                .delete(delete_object_tags),
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
        // M7：站点身份与关系（`[site]` 未配置时 handlers 404；identity 走 token 而非 JWT）
        .route("/site", get(site_info))
        .route("/site/identity", get(site_identity))
        // M9：sync 补偿的本地定点删除（token 面；只删本站，永不转发）
        .route("/site/delete-object", post(site_delete_object))
        .route("/site/delete-bucket", post(site_delete_bucket))
        .route("/site/head-object", head(site_head_object))
        .route("/site/bucket-empty", get(site_bucket_empty))
        .route("/site/bucket-quiesce", post(site_bucket_quiesce))
        .route(
            "/site/raft/join",
            post(site_raft_join).layer(axum::extract::DefaultBodyLimit::max(256 * 1024)),
        )
        .route("/sites", get(list_sites).post(create_site))
        .route("/sites/{peer_id}", delete(delete_site))
        .route("/sites/{peer_id}/ping", post(ping_site))
        .route("/sites/{peer_id}/status", get(site_status))
        .route(
            "/site/changelog",
            get(site_changelog_get)
                .post(site_changelog)
                .layer(axum::extract::DefaultBodyLimit::max(8 * 1024 * 1024)),
        )
}

async fn login(State(state): State<AppState>, Json(body): Json<LoginRequest>) -> impl IntoResponse {
    // C1 登录限速：账号锁定 + 全局失败上限（Argon2 资源放大面）。429 + Retry-After。
    if let Err(wait) = state.login_limiter.check(&body.username) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(axum::http::header::RETRY_AFTER, wait.to_string())],
            "登录尝试过多，请稍后再试",
        )
            .into_response();
    }
    match state.db.verify_user(&body.username, &body.password).await {
        Ok(Some(uid)) => {
            state.login_limiter.on_success(&body.username);
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
        Ok(None) => {
            state.login_limiter.on_failure(&body.username);
            (StatusCode::UNAUTHORIZED, "用户名或密码错误").into_response()
        }
        // DB 故障不算爆破信号（不计入限速——限量的是凭据尝试，不是基础设施故障）
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
        // 这几个语义是「客户端可纠正的冲突」，S3 本身也用 409；不落 5xx
        Error::BucketAlreadyExists(_) | Error::BucketNotEmpty(_) | Error::Conflict(_) => {
            (StatusCode::CONFLICT, e.to_string())
        }
        // M9：sync 远端不可达/intent 保留 → 503（与 S3 面同口径）。
        Error::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        _ => {
            tracing::error!(error = %e, "控制台 store 操作失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
        }
    }
}

/// M9 S6d：控制写成功 → 204；业务拒绝/门面错误统一走 S6b 的两个映射 helper
/// （503 保留 `ControlError::to_string()` 的 raft/forward 提示，绝不静默 204）。
fn control_write_status(
    r: Result<crate::raft::ControlResponse, crate::raft::control::ControlError>,
    conflict: &'static str,
) -> Response {
    match r {
        Ok(crate::raft::ControlResponse::Ok) => StatusCode::NO_CONTENT.into_response(),
        Ok(resp) => control_rejection(resp, conflict),
        Err(e) => control_failure(e),
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

/// 集群状态（admin）：本节点视角的拓扑快照 + 可达性 + quorum 计数。
/// 单机（无 `[cluster]`）优雅降级：`mode:"single"`，节点表只列本机（不走 mesh 探测）。
async fn cluster_status(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    let (unmet_write, unmet_lock) = state.store.quorum_unmet_counts();
    let quorum_unmet = serde_json::json!({ "write": unmet_write, "lock": unmet_lock });
    match state.store.cluster() {
        Some(c) => {
            let peers = crate::router::probe_peers(&state).await;
            let me = c.sets.flat.self_node();
            let mut nodes = vec![serde_json::json!({
                "id": me.id, "addr": me.addr, "drive_count": me.drive_count, "up": true,
            })];
            for n in &c.sets.flat.nodes {
                if n.id == me.id {
                    continue;
                }
                let up = peers
                    .iter()
                    .find(|(id, _, _)| id == &n.id)
                    .is_some_and(|(_, up, _)| *up);
                nodes.push(serde_json::json!({
                    "id": n.id, "addr": n.addr, "drive_count": n.drive_count, "up": up,
                }));
            }
            // M4 切片 8：多 set 视图——每 set 独立槽域（R3：跨 set 槽号会重号，必须分组呈现）；
            // topo_fingerprint = 本节点**首盘**（data_dirs[0] = set0 段首）的记录串，与写盘共用
            // 同一构造器（xl_store::store::topo_record），可直接与盘上文件等值比对；
            // set0 恒 v2、set>0 恒 v3（切片 8 口径）；切片 5 前的旧盘保留 v1 记录（读侧兼容）
            let sets_view: Vec<serde_json::Value> = c
                .sets
                .sets
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    serde_json::json!({
                        "set": i,
                        "slot_count": t.slot_count(),
                        "self": c.sets.self_ranges.iter().any(|(s, _, _)| *s as usize == i),
                        "nodes": t.nodes.iter().map(|n| serde_json::json!({
                            "id": n.id, "addr": n.addr, "drive_count": n.drive_count,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();
            let (first_set, first_off, _) = c.sets.self_ranges[0];
            let topo_fingerprint =
                xl_store::store::topo_record(first_set, &me.id, first_off as u16);
            Json(serde_json::json!({
                "mode": "cluster",
                "node_id": me.id,
                "slot_count": c.sets.flat.slot_count(),
                "topo_fingerprint": topo_fingerprint,
                "sets": sets_view,
                "nodes": nodes,
                "quorum_unmet": quorum_unmet,
            }))
            .into_response()
        }
        None => {
            let drive_count = state.cfg.data_dirs.as_ref().map(Vec::len).unwrap_or(1);
            Json(serde_json::json!({
                "mode": "single",
                "node_id": serde_json::Value::Null,
                "slot_count": drive_count,
                "topo_fingerprint": serde_json::Value::Null,
                "nodes": [{
                    "id": "local",
                    "addr": state.cfg.listen.clone(),
                    "drive_count": drive_count,
                    "up": true,
                }],
                "quorum_unmet": quorum_unmet,
            }))
            .into_response()
        }
    }
}

/// M8 S6 缺口 A：加密/KMS 只读状态（管理员）。配置仍只在 env（OSS_MASTER_KEY_FILE /
/// OSS_KMS_DIR）；此处只回布尔与当前主密钥编号，**绝不回密钥材料**，也无变更端点。
async fn encryption_status(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    let es = state.store.enc_state();
    let (sse_s3, sse_kms, kms_current_key_id) = match es {
        Some(es) => (
            es.file.is_some(),
            es.kms.is_some(),
            es.kms.as_ref().map(|k| format!("k{}", k.current_id())),
        ),
        None => (false, false, None),
    };
    Json(serde_json::json!({
        "sse_s3": sse_s3,
        "sse_kms": sse_kms,
        "kms_current_key_id": kms_current_key_id,
    }))
    .into_response()
}

/// 上传分组（oss.toml 的 `[[groups]]`），供上传面板的分组下拉使用。
/// 登录即可读，但被停用账号的旧 JWT 同样失效（过 who()）。
async fn list_groups(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> impl IntoResponse {
    if let Err(r) = who(&state, &payload).await {
        return r;
    }
    Json(state.cfg.groups.clone()).into_response()
}

/// 作业记录（M4 切片 6；管理员）：最近 100 条，最新在前。
async fn list_jobs(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    match state.db.list_jobs(100).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询 jobs 失败");
            internal_error()
        }
    }
}

#[derive(Deserialize)]
struct JobDetailQuery {
    #[serde(default)]
    offset: i64,
    #[serde(default = "default_task_limit")]
    limit: i64,
}

fn default_task_limit() -> i64 {
    50
}

/// 作业读权限：admin，或该作业的 `requested_by`。返回 uid（控制面审计用）。
#[allow(clippy::result_large_err)]
async fn job_access(state: &AppState, payload: &JwtPayload, id: i64) -> Result<i64, Response> {
    let (uid, is_admin) = who(state, payload).await?;
    match state.db.job_owner(id).await {
        Ok(Some((owner, _))) if is_admin || owner == Some(uid) => Ok(uid),
        Ok(Some(_)) => Err(forbidden("需要作业所有者或管理员")),
        Ok(None) => Err((StatusCode::NOT_FOUND, "作业不存在").into_response()),
        Err(e) => {
            tracing::error!(error = %e, "查作业归属失败");
            Err(internal_error())
        }
    }
}

/// 作业详情 + 任务分页。
async fn get_job(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<i64>,
    Query(q): Query<JobDetailQuery>,
) -> Response {
    if let Err(r) = job_access(&state, &payload, id).await {
        return r;
    }
    let job = match state.db.job_get(id).await {
        Ok(Some(j)) => j,
        Ok(None) => return (StatusCode::NOT_FOUND, "作业不存在").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查作业详情失败");
            return internal_error();
        }
    };
    match state.db.job_tasks(id, q.offset, q.limit).await {
        Ok(tasks) => Json(serde_json::json!({
            "job": job, "tasks": tasks, "offset": q.offset, "limit": q.limit,
        }))
        .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查作业任务失败");
            internal_error()
        }
    }
}

/// `pause` / `resume` / `cancel` / `retry-failed`。
async fn job_action(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path((id, action)): Path<(i64, String)>,
) -> Response {
    let uid = match job_access(&state, &payload, id).await {
        Ok(u) => u,
        Err(r) => return r,
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    if action == "retry-failed" {
        return match state.db.job_retry_failed(id, now).await {
            Ok(Some(new_id)) => {
                state
                    .audit(
                        uid,
                        "control:job-retry",
                        None,
                        None,
                        Some(&format!("job={id};child={new_id}")),
                    )
                    .await;
                Json(serde_json::json!({"job_id": new_id})).into_response()
            }
            Ok(None) => (StatusCode::CONFLICT, "没有可重试的失败任务").into_response(),
            Err(e) => {
                tracing::error!(error = %e, "重试失败任务出错");
                internal_error()
            }
        };
    }
    if !matches!(action.as_str(), "pause" | "resume" | "cancel") {
        return (StatusCode::BAD_REQUEST, "未知作业操作").into_response();
    }
    match state.db.job_set_state(id, &action, now).await {
        Ok(true) => {
            state
                .audit(
                    uid,
                    &format!("control:job-{action}"),
                    None,
                    None,
                    Some(&format!("job={id}")),
                )
                .await;
            // 取消后立即落一次报告（未执行任务被 started_at 过滤；后续运行中任务收尾会重写）。
            if action == "cancel" {
                crate::jobs::write_report_now(&state.db, &state.store, id).await;
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => (StatusCode::CONFLICT, "作业状态不允许该操作").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "作业状态操作失败");
            internal_error()
        }
    }
}

/// 归属门（桶主或管理员）——policy/lifecycle 共用；桶必须存在（404 护栏：防给不存在桶
/// 预置策略/规则、同名桶日后创建即继承——verifier 非阻断 3）。
#[allow(clippy::result_large_err)]
pub(crate) async fn require_owner_or_admin(
    state: &AppState,
    payload: &JwtPayload,
    bucket: &str,
) -> Result<(), Response> {
    let (uid, is_admin) = who(state, payload).await?;
    match state.acl().is_owner(bucket, uid).await {
        Ok(owner) if owner || is_admin => {} // 桶主或管理员 → 过归属门
        Ok(_) => return Err(forbidden("需要桶主或管理员")),
        Err(e) => {
            tracing::error!(error = %e, "归属查询失败");
            return Err(internal_error());
        }
    }
    match state.db.bucket_exists(bucket).await {
        Ok(true) => Ok(()),
        Ok(false) => Err((StatusCode::NOT_FOUND, "NoSuchBucket").into_response()),
        Err(e) => {
            tracing::error!(error = %e, "桶存在性查询失败");
            Err(internal_error())
        }
    }
}

/// 桶策略（M4 切片 7）：读 —— 桶主或管理员；返回原文 JSON 或 null。
async fn get_policy(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    match state.db.bucket_policy(&bucket).await {
        Ok(Some(json)) => match serde_json::from_str::<serde_json::Value>(&json) {
            Ok(v) => Json(v).into_response(),
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "stored policy corrupt").into_response(),
        },
        Ok(None) => Json(serde_json::Value::Null).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "读桶策略失败");
            internal_error()
        }
    }
}

/// 桶策略全量替换：写 —— 桶主或管理员。body 必须能按策略子集解析（400 否则）；
/// 上限 64 KiB / ≤100 条 statement（防呆）。
async fn put_policy(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
    body: String,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    if body.len() > 64 * 1024 {
        return (StatusCode::BAD_REQUEST, "策略过大（≤64KiB）").into_response();
    }
    // 顶层必须为对象（serde 的位置形式会把裸 `[]`/`[...]` 吞成「空/错位策略」）
    match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(v) if v.is_object() => {}
        Ok(_) => return (StatusCode::BAD_REQUEST, "策略顶层必须是对象").into_response(),
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("策略解析失败: {e}")).into_response();
        }
    }
    match serde_json::from_str::<xl_s3::policy::PolicyDoc>(&body) {
        Ok(doc) => {
            if doc.statement.len() > 100 {
                return (StatusCode::BAD_REQUEST, "statement 过多（≤100）").into_response();
            }
            // 语义校验（Effect 拼错/缺字段 = 语句静默空转，verifier 非阻断 1）
            if let Err(m) = doc.validate() {
                return (StatusCode::BAD_REQUEST, format!("策略校验失败: {m}")).into_response();
            }
        }
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("策略解析失败: {e}")).into_response();
        }
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    if state.control.is_raft() {
        return control_write_status(
            state
                .control
                .write(crate::raft::ControlCommand::BucketSetPolicy {
                    bucket,
                    policy_json: body,
                    updated_at: now,
                })
                .await,
            "policy 写入冲突",
        );
    }
    match state.db.set_bucket_policy(&bucket, &body, now).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            if let Some(r) = site_conflict(&e) {
                return r;
            }
            tracing::error!(error = %e, "写桶策略失败");
            internal_error()
        }
    }
}

// ---------- M8 S6 缺口 B：桶事件通知配置（与 S3 面同一状态） ----------

/// GET：Read 权限；返回与 S3 GetBucketNotificationConfiguration 同形的配置（未配置 = 空列表）。
async fn get_notifications(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
) -> Response {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Read).await {
        return r;
    }
    // Acl::can 对管理员短路，补桶存在性检查（与 get_lifecycle 同口径）
    match state.db.bucket_exists(&bucket).await {
        Ok(true) => {}
        Ok(false) => return (StatusCode::NOT_FOUND, "NoSuchBucket").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "桶存在性查询失败");
            return internal_error();
        }
    }
    match state.acl().bucket_notification(&bucket).await {
        Ok(Some(json)) => match serde_json::from_str::<Vec<xl_s3::notify::EventTarget>>(&json) {
            Ok(targets) => Json(xl_s3::notify::notification_config_out(&targets)).into_response(),
            Err(e) => {
                tracing::error!(error = %e, bucket = %bucket, "stored notification config corrupt");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "stored notification config corrupt",
                )
                    .into_response()
            }
        },
        Ok(None) => Json(xl_s3::notify::notification_config_out(&[])).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "读通知配置失败");
            internal_error()
        }
    }
}

/// PUT：全量替换（桶主或管理员）；S3 线格式 JSON → `notify::parse_notification_config`
/// 校验（≤100 targets / 事件白名单 / ARN 白名单），存归一化配置。
async fn put_notifications(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
    body: String,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    if body.len() > 64 * 1024 {
        return (StatusCode::BAD_REQUEST, "通知配置过大（≤64KiB）").into_response();
    }
    let cfg: s3s::dto::NotificationConfiguration = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("通知配置解析失败: {e}")).into_response();
        }
    };
    let targets = match xl_s3::notify::parse_notification_config(&cfg) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                e.message()
                    .unwrap_or("invalid notification configuration")
                    .to_string(),
            )
                .into_response();
        }
    };
    let json = match serde_json::to_string(&targets) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "通知配置序列化失败");
            return internal_error();
        }
    };
    if state.control.is_raft() {
        let cmd = if targets.is_empty() {
            crate::raft::ControlCommand::BucketClearNotification {
                bucket: bucket.clone(),
            }
        } else {
            crate::raft::ControlCommand::BucketSetNotification {
                bucket: bucket.clone(),
                config_json: json,
                updated_at: crate::db::now(),
            }
        };
        return control_write_status(state.control.write(cmd).await, "通知配置写入冲突");
    }
    match state.acl().set_bucket_notification(&bucket, &json).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "写通知配置失败");
            internal_error()
        }
    }
}

/// DELETE：清空配置（桶主或管理员）。
async fn delete_notifications(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    if state.control.is_raft() {
        return control_write_status(
            state
                .control
                .write(crate::raft::ControlCommand::BucketClearNotification { bucket })
                .await,
            "通知配置写入冲突",
        );
    }
    match state.acl().clear_bucket_notification(&bucket).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "清通知配置失败");
            internal_error()
        }
    }
}

// ---------- M8 S6 缺口 B：死信可见性与重投（管理员） ----------

#[derive(Debug, Deserialize)]
struct DeadQuery {
    #[serde(default)]
    limit: Option<i64>,
}

/// GET：`state='dead'` 的行，最新在前；limit 与 /audit 同口径钳位 1..=1000。
async fn list_dead_events(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    axum::extract::Query(q): axum::extract::Query<DeadQuery>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    use sqlx::Row as _;
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let rows = sqlx::query(
        "SELECT id, bucket, event_json, attempts, last_error, created_at FROM event_outbox \
         WHERE state='dead' ORDER BY id DESC LIMIT ?",
    )
    .bind(limit)
    .fetch_all(state.db.pool())
    .await;
    match rows {
        Ok(rows) => Json(
            rows.iter()
                .map(|r| {
                    let raw: String = r.get("event_json");
                    // event_outbox 无对象键/事件名列——两者都在事件载荷 JSON 里
                    let ev: serde_json::Value =
                        serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
                    serde_json::json!({
                        "id": r.get::<i64, _>("id"),
                        "bucket": r.get::<String, _>("bucket"),
                        // 载荷里是 AWS URL 编码键（space→+，/→%2F）；展示解码后的原件
                        "object_key": ev.pointer("/s3/object/key").and_then(|v| v.as_str())
                            .map(crate::events::aws_decode_key)
                            .map(serde_json::Value::String)
                            .unwrap_or(serde_json::Value::Null),
                        "event": ev.pointer("/eventName")
                            .cloned().unwrap_or(serde_json::Value::Null),
                        "attempts": r.get::<i64, _>("attempts"),
                        "last_error": r.get::<Option<String>, _>("last_error"),
                        "created_at": r.get::<i64, _>("created_at"),
                    })
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询死信失败");
            internal_error()
        }
    }
}

/// POST：dead → pending（复位 attempts/claimed_at/next_retry_at/last_error），交给原 worker
/// 重试；0 行（不存在或不是 dead）→ 404。不删除/重插，保持 at-least-once 语义。
async fn redeliver_event(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<i64>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    match sqlx::query(
        "UPDATE event_outbox SET state='pending', claimed_at=NULL, next_retry_at=?, attempts=0, \
         last_error=NULL WHERE id=? AND state='dead'",
    )
    .bind(now)
    .bind(id)
    .execute(state.db.pool())
    .await
    {
        Ok(r) if r.rows_affected() == 1 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => (StatusCode::NOT_FOUND, "死信事件不存在").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "重投死信失败");
            internal_error()
        }
    }
}

// ---------- M8 S6 缺口 B：IAM 预设模板（静态目录，登录即可读） ----------

async fn list_policy_templates(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    if let Err(r) = who(&state, &payload).await {
        return r;
    }
    Json(xl_s3::policy::policy_templates()).into_response()
}

// ---------- M8 S6 缺口 B：一次性 Select 代理 ----------

/// 输入上限与 S3 面一致；输出上限保护控制台单次 JSON 响应（越限 400，不静默截断）。
const SELECT_MAX_INPUT: u64 = 256 * 1024 * 1024;
const SELECT_MAX_RECORDS: usize = 10_000;
const SELECT_MAX_OUT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Deserialize)]
struct SelectBody {
    key: String,
    expression: String,
    input: SelectInput,
    output: SelectOutput,
    /// 点名版本（缺省 = 链尾）。
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    scan_range: Option<SelectScanRange>,
}

#[derive(Debug, Deserialize)]
struct SelectInput {
    /// "CSV" | "JSON"（大小写不敏感）。
    format: String,
    #[serde(default)]
    csv: Option<SelectCsvIn>,
    #[serde(default)]
    json: Option<SelectJsonIn>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SelectCsvIn {
    field_delimiter: Option<String>,
    quote_character: Option<String>,
    quote_escape_character: Option<String>,
    record_delimiter: Option<String>,
    comments: Option<String>,
    file_header_info: Option<String>,
    allow_quoted_record_delimiter: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
struct SelectJsonIn {
    #[serde(rename = "type", default)]
    kind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SelectOutput {
    format: String,
    #[serde(default)]
    csv: Option<SelectCsvOut>,
    #[serde(default)]
    json: Option<SelectJsonOut>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SelectCsvOut {
    field_delimiter: Option<String>,
    quote_character: Option<String>,
    quote_escape_character: Option<String>,
    record_delimiter: Option<String>,
    quote_fields: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SelectJsonOut {
    record_delimiter: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SelectScanRange {
    start: Option<i64>,
    end: Option<i64>,
}

type SelectErr = (StatusCode, String);

fn select_byte(v: Option<&str>, default: u8, name: &str) -> Result<u8, SelectErr> {
    match v {
        None => Ok(default),
        Some(s) if s.len() == 1 => Ok(s.as_bytes()[0]),
        Some(_) => Err((StatusCode::BAD_REQUEST, format!("{name} 必须是单字节字符"))),
    }
}

/// 请求 DTO → 引擎格式（与 service.rs::select_object_content 同口径；不含压缩）。
fn select_formats(
    body: &SelectBody,
) -> Result<(xl_s3::select::InFormat, xl_s3::select::OutFormat), SelectErr> {
    use xl_s3::select::{CsvIn, CsvOut, HeaderMode, InFormat, OutFormat};
    let in_fmt = if body.input.format.eq_ignore_ascii_case("CSV") {
        if body.input.json.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "input.format=CSV 时不得提供 json 选项".into(),
            ));
        }
        let c = body.input.csv.clone().unwrap_or_default();
        let header = match c.file_header_info.as_deref().unwrap_or("NONE") {
            "USE" => HeaderMode::Use,
            "IGNORE" => HeaderMode::Ignore,
            "NONE" => HeaderMode::None,
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "file_header_info 只支持 USE/IGNORE/NONE".into(),
                ));
            }
        };
        InFormat::Csv(CsvIn {
            field: select_byte(c.field_delimiter.as_deref(), b',', "field_delimiter")?,
            quote: select_byte(c.quote_character.as_deref(), b'"', "quote_character")?,
            escape: select_byte(
                c.quote_escape_character.as_deref(),
                b'"',
                "quote_escape_character",
            )?,
            record: select_byte(c.record_delimiter.as_deref(), b'\n', "record_delimiter")?,
            comments: Some(select_byte(c.comments.as_deref(), b'#', "comments")?),
            header,
            allow_quoted_record_delimiter: c.allow_quoted_record_delimiter.unwrap_or(false),
        })
    } else if body.input.format.eq_ignore_ascii_case("JSON") {
        if body.input.csv.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "input.format=JSON 时不得提供 csv 选项".into(),
            ));
        }
        let lines = match body
            .input
            .json
            .as_ref()
            .and_then(|j| j.kind.as_deref())
            .unwrap_or("DOCUMENT")
        {
            "LINES" => true,
            "DOCUMENT" => false,
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "json.type 只支持 DOCUMENT/LINES".into(),
                ));
            }
        };
        InFormat::Json { lines }
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "input.format 只支持 CSV/JSON".into(),
        ));
    };

    let out_fmt = if body.output.format.eq_ignore_ascii_case("CSV") {
        if body.output.json.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "output.format=CSV 时不得提供 json 选项".into(),
            ));
        }
        let c = body.output.csv.clone().unwrap_or_default();
        OutFormat::Csv(CsvOut {
            field: select_byte(c.field_delimiter.as_deref(), b',', "field_delimiter")?,
            quote: select_byte(c.quote_character.as_deref(), b'"', "quote_character")?,
            escape: select_byte(
                c.quote_escape_character.as_deref(),
                b'"',
                "quote_escape_character",
            )?,
            record: select_byte(c.record_delimiter.as_deref(), b'\n', "record_delimiter")?,
            always_quote: match c.quote_fields.as_deref() {
                None => false,
                Some(v) if v.eq_ignore_ascii_case("ALWAYS") => true,
                Some(v) if v.eq_ignore_ascii_case("ASNEEDED") => false,
                Some(_) => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "quote_fields 只支持 ALWAYS/ASNEEDED".into(),
                    ));
                }
            },
        })
    } else if body.output.format.eq_ignore_ascii_case("JSON") {
        if body.output.csv.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "output.format=JSON 时不得提供 csv 选项".into(),
            ));
        }
        let record = select_byte(
            body.output
                .json
                .as_ref()
                .and_then(|j| j.record_delimiter.as_deref()),
            b'\n',
            "record_delimiter",
        )?;
        // 控制台把 records 转成 JSON 数组返回：靠 serde_json 流式解析，分隔符须为空白
        if !record.is_ascii_whitespace() {
            return Err((
                StatusCode::BAD_REQUEST,
                "JSON 输出的 record_delimiter 必须是空白字符".into(),
            ));
        }
        OutFormat::Json { record }
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "output.format 只支持 CSV/JSON".into(),
        ));
    };
    Ok((in_fmt, out_fmt))
}

/// Store 读错误分档：SSE-C（控制台无客户钥）/ 客户端可纠正参数 → 400；归档中 → 403。
fn select_store_error(e: xl_store::error::Error) -> Response {
    use xl_store::error::Error;
    match &e {
        Error::InvalidArgument(m) => (StatusCode::BAD_REQUEST, m.clone()).into_response(),
        Error::InvalidObjectState(m) => (StatusCode::FORBIDDEN, m.clone()).into_response(),
        Error::DeleteMarker(m) => (StatusCode::NOT_FOUND, m.clone()).into_response(),
        _ => store_error(e).into_response(),
    }
}

/// 一次性 Select：桶 Read 权限；读取走与 S3 SelectObjectContent 相同的 store 路径；
/// 返回 `{records, stats}`（CSV → 字段数组数组；JSON → JSON 值数组）。
async fn select_object(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
    Json(body): Json<SelectBody>,
) -> Response {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Read).await {
        return r;
    }
    let plan = match xl_s3::select::parse(&body.expression) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                e.message().unwrap_or("invalid SQL").to_string(),
            )
                .into_response();
        }
    };
    let (in_fmt, out_fmt) = match select_formats(&body) {
        Ok(v) => v,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    if body.scan_range.is_some() && matches!(in_fmt, xl_s3::select::InFormat::Json { lines: false })
    {
        return (
            StatusCode::BAD_REQUEST,
            "ScanRange 不支持 JSON DOCUMENT 输入",
        )
            .into_response();
    }

    // 控制台不保存客户密钥：SSE-C 对象在 head 阶段即被 store 拒为 InvalidArgument → 400。
    let vid = body.version_id.as_deref().filter(|v| !v.is_empty());
    let head = match vid {
        Some(v) => {
            state
                .store
                .head_object_version_ext(&bucket, &body.key, v, None)
                .await
        }
        None => state.store.head_object_ext(&bucket, &body.key, None).await,
    };
    let info = match head {
        Ok(i) => i,
        Err(e) => return select_store_error(e),
    };
    if info.size > SELECT_MAX_INPUT {
        return (
            StatusCode::BAD_REQUEST,
            "对象超过 S3 Select 的 256MiB 输入上限",
        )
            .into_response();
    }
    let got = match vid {
        Some(v) => {
            state
                .store
                .get_object_version_ext(&bucket, &body.key, v, None)
                .await
        }
        None => state.store.get_object_ext(&bucket, &body.key, None).await,
    };
    let bytes = match got {
        Ok((_, b)) if b.len() as u64 <= SELECT_MAX_INPUT => b,
        Ok(_) => {
            return (
                StatusCode::BAD_REQUEST,
                "对象超过 S3 Select 的 256MiB 输入上限",
            )
                .into_response();
        }
        Err(e) => return select_store_error(e),
    };

    // ScanRange：AWS 语义 = 记录首字节落在区间内则整条处理；仅 end = 末尾 N 字节。
    let len = bytes.len() as i64;
    let mut scan = None;
    let mut scanned = len;
    if let Some(sr) = body.scan_range.as_ref() {
        if sr.start.is_some_and(|v| v < 0) || sr.end.is_some_and(|v| v < 0) {
            return (StatusCode::BAD_REQUEST, "ScanRange 不允许负值").into_response();
        }
        let (start, end) = match (sr.start, sr.end) {
            (None, None) => (0, len.saturating_sub(1)),
            (Some(s), None) => (s, len.saturating_sub(1)),
            (Some(s), Some(e)) => {
                if s > e {
                    return (StatusCode::BAD_REQUEST, "ScanRange Start 不得大于 End")
                        .into_response();
                }
                (s, e)
            }
            (None, Some(n)) => ((len - n).max(0), len.saturating_sub(1)),
        };
        scan = Some((start, end));
        scanned = if len == 0 {
            0
        } else {
            let lo = start.clamp(0, len);
            let hi = end.clamp(0, len - 1);
            if lo <= hi { hi - lo + 1 } else { 0 }
        };
    }

    let events = match xl_s3::select::evaluate_capped(
        &plan,
        &bytes,
        &in_fmt,
        &out_fmt,
        &xl_s3::select::EvalOptions {
            request_progress: false,
            scanned: Some(scanned),
            scan,
        },
        SELECT_MAX_RECORDS,
        SELECT_MAX_OUT_BYTES,
    ) {
        Ok(evs) => evs,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                e.message().unwrap_or("select failed").to_string(),
            )
                .into_response();
        }
    };
    let mut out: Vec<u8> = Vec::new();
    let mut stats = xl_s3::select::StatsData::default();
    for ev in events {
        match ev {
            xl_s3::select::SelectEvent::Records(b) => out.extend_from_slice(&b),
            xl_s3::select::SelectEvent::Stats(s) => stats = s,
            _ => {}
        }
    }
    let records: Vec<serde_json::Value> = match &out_fmt {
        xl_s3::select::OutFormat::Json { .. } => {
            let text = match std::str::from_utf8(&out) {
                Ok(t) => t,
                Err(_) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "select output is not UTF-8",
                    )
                        .into_response();
                }
            };
            let mut vals = Vec::new();
            for v in serde_json::Deserializer::from_str(text).into_iter::<serde_json::Value>() {
                match v {
                    Ok(v) => vals.push(v),
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            "JSON 输出无法按记录解析（record_delimiter 须为空白字符）",
                        )
                            .into_response();
                    }
                }
            }
            vals
        }
        xl_s3::select::OutFormat::Csv(c) => {
            let cin = xl_s3::select::CsvIn {
                field: c.field,
                quote: c.quote,
                escape: c.escape,
                record: c.record,
                comments: None,
                header: xl_s3::select::HeaderMode::None,
                allow_quoted_record_delimiter: true,
            };
            match xl_s3::select::parse_csv(&out, &cin) {
                Ok(p) => p
                    .records
                    .into_iter()
                    .map(|(fields, _)| serde_json::json!(fields))
                    .collect(),
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        e.message().unwrap_or("CSV 输出解析失败").to_string(),
                    )
                        .into_response();
                }
            }
        }
    };
    Json(serde_json::json!({
        "records": records,
        "stats": {
            "bytes_scanned": stats.bytes_scanned,
            "bytes_processed": stats.bytes_processed,
            "bytes_returned": stats.bytes_returned,
        },
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct AuditQuery {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    decision: Option<String>,
    /// 时间窗（unix 秒，闭区间）；`key` = object_key 前缀。
    #[serde(default)]
    from: Option<i64>,
    #[serde(default)]
    to: Option<i64>,
    #[serde(default)]
    uid: Option<i64>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    key: Option<String>,
}

/// 审计查询（M4 切片 7；管理员）：`?limit=100&decision=deny|allow`，以及
/// `from/to/uid/op/bucket/key` 过滤（空串视为未给）。
async fn list_audit(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    axum::extract::Query(q): axum::extract::Query<AuditQuery>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    if q.from.is_some_and(|v| v < 0) || q.to.is_some_and(|v| v < 0) || q.uid.is_some_and(|v| v < 0)
    {
        return (StatusCode::BAD_REQUEST, "时间/uid 不得为负").into_response();
    }
    if let (Some(from), Some(to)) = (q.from, q.to)
        && from > to
    {
        return (StatusCode::BAD_REQUEST, "from 不得晚于 to").into_response();
    }
    let nonempty = |s: Option<String>| s.filter(|v| !v.is_empty());
    let (op, bucket, key) = (nonempty(q.op), nonempty(q.bucket), nonempty(q.key));
    let filter = crate::db::AuditFilter {
        limit: q.limit.unwrap_or(100).clamp(1, 1000),
        decision: q
            .decision
            .as_deref()
            .filter(|d| *d == "allow" || *d == "deny"),
        from: q.from,
        to: q.to,
        uid: q.uid,
        op: op.as_deref(),
        bucket: bucket.as_deref(),
        key_prefix: key.as_deref(),
    };
    match state.db.list_audit(&filter).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询审计失败");
            internal_error()
        }
    }
}

/// 桶生命周期规则（M4 切片 6）：读 —— 桶主或管理员。
async fn get_lifecycle(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    match state.lifecycle.get_rules(&bucket).await {
        Ok(None) => Json(Vec::<serde_json::Value>::new()).into_response(),
        Ok(Some(rules)) => {
            Json(rules.iter().map(console_rule_json).collect::<Vec<_>>()).into_response()
        }
        Err(e) => lifecycle_err(e),
    }
}

/// 规则 → console JSON：保留旧扁平字段（旧前端），同时投影完整结构化模型。
fn console_rule_json(r: &xl_s3::lifecycle::LifecycleRuleSpec) -> serde_json::Value {
    use xl_s3::lifecycle::ExpirationAction;
    let expiration_days = r.expiration_days().map(i64::from);
    let first = r.transitions.first();
    let (expiration_date, eodm) = match &r.expiration {
        Some(ExpirationAction::Date { date }) => (Some(date_string(*date)), false),
        Some(ExpirationAction::ExpiredObjectDeleteMarker) => (None, true),
        _ => (None, false),
    };
    let transitions: Vec<serde_json::Value> = r
        .transitions
        .iter()
        .map(|t| {
            serde_json::json!({
                "days": t.days,
                "date": t.date.map(date_string),
                "storage_class": t.class,
                "tier": t.tier,
            })
        })
        .collect();
    let noncurrent_expiration = r.noncurrent_expiration.as_ref().map(|n| {
        serde_json::json!({
            "noncurrent_days": n.days,
            "newer_noncurrent_versions": n.newer,
        })
    });
    let noncurrent_transitions: Vec<serde_json::Value> = r
        .noncurrent_transitions
        .iter()
        .map(|t| {
            serde_json::json!({
                "noncurrent_days": t.days,
                "newer_noncurrent_versions": t.newer,
                "storage_class": t.class,
                "tier": t.tier,
            })
        })
        .collect();
    serde_json::json!({
        "rule_id": r.rule_id, "prefix": r.prefix(),
        // 旧前端字段：无过期 = 哨兵 -1（与 S0 行口径一致）
        "expire_days": expiration_days.unwrap_or(-1),
        "expiration_days": expiration_days,
        "transition_days": first.and_then(|t| t.days).map(i64::from),
        "transition_class": first.map(|t| t.class.clone()),
        "transition_tier": first.and_then(|t| t.tier.clone()),
        "enabled": r.enabled,
        "filter": {
            "prefix": r.filter.prefix,
            "tags": r.filter.tags,
            "object_size_greater_than": r.filter.size_gt,
            "object_size_less_than": r.filter.size_lt,
        },
        "expiration_date": expiration_date,
        "expired_object_delete_marker": eodm,
        "transitions": transitions,
        "noncurrent_version_expiration": noncurrent_expiration,
        "noncurrent_version_transitions": noncurrent_transitions,
    })
}

/// unix 秒 → `YYYY-MM-DD`（Date 恒为 UTC 午夜）。
fn date_string(secs: i64) -> String {
    let odt =
        time::OffsetDateTime::from_unix_timestamp(secs).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}",
        odt.year(),
        u8::from(odt.month()),
        odt.day()
    )
}

/// `YYYY-MM-DD`（或 RFC3339）→ UTC 午夜 unix 秒。
fn parse_midnight(s: &str) -> Result<i64, String> {
    let date = if let Ok(odt) =
        time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
    {
        odt.date()
    } else {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return Err(format!("日期格式应为 YYYY-MM-DD: {s}"));
        }
        let year: i32 = parts[0].parse().map_err(|_| format!("年份非法: {s}"))?;
        let month: u8 = parts[1].parse().map_err(|_| format!("月份非法: {s}"))?;
        let day: u8 = parts[2].parse().map_err(|_| format!("日期非法: {s}"))?;
        time::Date::from_calendar_date(
            year,
            time::Month::try_from(month).map_err(|_| format!("月份非法: {s}"))?,
            day,
        )
        .map_err(|_| format!("日期非法: {s}"))?
    };
    Ok(date.midnight().assume_utc().unix_timestamp())
}

/// M8 S4b：console 与 S3 共用 `LifecycleError` 的状态码映射。
fn lifecycle_err(e: xl_s3::lifecycle::LifecycleError) -> Response {
    use xl_s3::lifecycle::LifecycleError as E;
    let status = match &e {
        E::NoSuchBucket | E::NoSuchConfiguration => StatusCode::NOT_FOUND,
        E::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
        E::Invalid(_) | E::InvalidClass(_) | E::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        E::Internal(_) => {
            tracing::error!(error = %e, "lifecycle 内部错误");
            return internal_error();
        }
    };
    (status, e.to_string()).into_response()
}

#[derive(Debug, Deserialize)]
struct LifecycleRuleIn {
    #[serde(default)]
    rule_id: Option<String>,
    /// 旧前端扁平字段；与 `filter.prefix` 等价（同时给且不同 → 400）。
    #[serde(default)]
    prefix: String,
    /// 旧前端字段；`-1` = 无过期（GET 回读口径）。
    #[serde(default)]
    expire_days: Option<i64>,
    #[serde(default)]
    expiration_days: Option<i64>,
    #[serde(default)]
    transition_days: Option<u32>,
    #[serde(default)]
    transition_class: Option<String>,
    #[serde(default)]
    transition_tier: Option<String>,
    #[serde(default = "default_true_bool")]
    enabled: bool,
    // ---- P1 结构化字段（旧字段仍可用；新字段优先且互斥）----
    #[serde(default)]
    filter: Option<LifecycleFilterIn>,
    /// `YYYY-MM-DD`（UTC 午夜）。
    #[serde(default)]
    expiration_date: Option<String>,
    #[serde(default)]
    expired_object_delete_marker: Option<bool>,
    #[serde(default)]
    transitions: Option<Vec<TransitionIn>>,
    #[serde(default)]
    noncurrent_version_expiration: Option<NoncurrentExpirationIn>,
    #[serde(default)]
    noncurrent_version_transitions: Option<Vec<NoncurrentTransitionIn>>,
}

#[derive(Debug, Deserialize)]
struct LifecycleFilterIn {
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    tags: Option<Vec<TagIn>>,
    #[serde(default)]
    object_size_greater_than: Option<u64>,
    #[serde(default)]
    object_size_less_than: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TagIn {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct TransitionIn {
    #[serde(default)]
    days: Option<u32>,
    #[serde(default)]
    date: Option<String>,
    storage_class: String,
    #[serde(default)]
    tier: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NoncurrentExpirationIn {
    noncurrent_days: u32,
    #[serde(default)]
    newer_noncurrent_versions: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct NoncurrentTransitionIn {
    noncurrent_days: u32,
    #[serde(default)]
    newer_noncurrent_versions: Option<u32>,
    storage_class: String,
    #[serde(default)]
    tier: Option<String>,
}

fn default_true_bool() -> bool {
    true
}

/// console JSON → 规则（结构校验统一在 `put_rules` 的 `validate_rules`）。
fn console_spec(r: LifecycleRuleIn) -> Result<xl_s3::lifecycle::LifecycleRuleSpec, String> {
    use xl_s3::lifecycle::{
        ExpirationAction, LifecycleRuleSpec, NoncurrentExpiration, NoncurrentTransition,
        TransitionAction,
    };
    let LifecycleRuleIn {
        rule_id,
        prefix,
        expire_days,
        expiration_days,
        transition_days,
        transition_class,
        transition_tier,
        enabled,
        filter,
        expiration_date,
        expired_object_delete_marker,
        transitions,
        noncurrent_version_expiration,
        noncurrent_version_transitions,
    } = r;
    let filter = build_filter(filter, &prefix)?;
    // 旧前端只发 expire_days；两个字段都给时以 expiration_days 为准。
    // -1 是 GET 回读的「无过期」哨兵；其它负数仍拒绝（与 S0 口径一致）。
    let days = match (expiration_days, expire_days) {
        (Some(d), _) => Some(d),
        (None, Some(-1)) | (None, None) => None,
        (None, Some(d)) if d >= 0 => Some(d),
        (None, Some(_)) => return Err("expire_days 须在 0..=36500".into()),
    };
    let days = days
        .map(|d| u32::try_from(d).map_err(|_| "expire_days 越界".to_string()))
        .transpose()?;
    let expiration = if let Some(s) = expiration_date {
        if days.is_some() || expired_object_delete_marker == Some(true) {
            return Err("expiration_date 与 expiration_days/ExpiredObjectDeleteMarker 互斥".into());
        }
        Some(ExpirationAction::Date {
            date: parse_midnight(&s)?,
        })
    } else if expired_object_delete_marker == Some(true) {
        if days.is_some() {
            return Err("ExpiredObjectDeleteMarker 与 Expiration.Days 互斥".into());
        }
        Some(ExpirationAction::ExpiredObjectDeleteMarker)
    } else {
        days.map(|days| ExpirationAction::Days { days })
    };
    let transitions = match transitions {
        Some(v) if !v.is_empty() => {
            if transition_days.is_some() || transition_class.is_some() || transition_tier.is_some()
            {
                return Err("transitions 与 transition_days/class/tier 不能同时提供".into());
            }
            v.into_iter()
                .map(|t| {
                    let date = t.date.as_deref().map(parse_midnight).transpose()?;
                    if t.days.is_some() == date.is_some() {
                        return Err("Transition 需要且只能有 days 或 date".into());
                    }
                    Ok(TransitionAction {
                        days: t.days,
                        date,
                        class: t.storage_class,
                        tier: t.tier,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?
        }
        _ => match (transition_days, transition_class) {
            (Some(days), Some(class)) => vec![TransitionAction {
                days: Some(days),
                date: None,
                class,
                tier: transition_tier,
            }],
            (None, None) => {
                if transition_tier.is_some() {
                    return Err("transition_tier 需要 transition_class".into());
                }
                vec![]
            }
            _ => return Err("transition_days 与 transition_class 必须同时提供".into()),
        },
    };
    Ok(LifecycleRuleSpec {
        rule_id,
        filter,
        expiration,
        transitions,
        noncurrent_expiration: noncurrent_version_expiration.map(|n| NoncurrentExpiration {
            days: n.noncurrent_days,
            newer: n.newer_noncurrent_versions,
        }),
        noncurrent_transitions: noncurrent_version_transitions
            .unwrap_or_default()
            .into_iter()
            .map(|t| NoncurrentTransition {
                days: t.noncurrent_days,
                newer: t.newer_noncurrent_versions,
                class: t.storage_class,
                tier: t.tier,
            })
            .collect(),
        enabled,
    })
}

fn build_filter(
    inp: Option<LifecycleFilterIn>,
    legacy_prefix: &str,
) -> Result<xl_s3::lifecycle::LifecycleFilter, String> {
    let mut f = xl_s3::lifecycle::LifecycleFilter::default();
    if let Some(i) = inp {
        f.prefix = i.prefix.unwrap_or_default();
        f.tags = i
            .tags
            .unwrap_or_default()
            .into_iter()
            .map(|t| xl_store::meta::ObjectTag {
                key: t.key,
                value: t.value,
            })
            .collect();
        f.size_gt = i.object_size_greater_than;
        f.size_lt = i.object_size_less_than;
    }
    if !legacy_prefix.is_empty() {
        if !f.prefix.is_empty() && f.prefix != legacy_prefix {
            return Err("prefix 与 filter.prefix 冲突".into());
        }
        f.prefix = legacy_prefix.to_string();
    }
    Ok(f)
}

/// 桶生命周期规则全量替换（M4 切片 6）：写 —— 桶主或管理员。
/// 旧扁平字段与 P1 结构化字段都接受；校验统一由 `validate_rules` 完成。
async fn put_lifecycle(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
    Json(rules): Json<Vec<LifecycleRuleIn>>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    if rules.len() > 100 {
        return (StatusCode::BAD_REQUEST, "规则过多（≤100）").into_response();
    }
    let mut specs = Vec::with_capacity(rules.len());
    for r in rules {
        match console_spec(r) {
            Ok(s) => specs.push(s),
            Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
        }
    }
    match state.lifecycle.put_rules(&bucket, &specs).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => lifecycle_err(e),
    }
}

/// Task 6a：`[[tiers]]` 只读状态（管理员）。`healthy` = 哨兵键 HEAD 可达
/// （404/无键也算可达——远端活着）；凭证字段绝不投影。
async fn list_tiers(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    let mut out = Vec::with_capacity(state.tiers.targets().len());
    for t in state.tiers.targets() {
        let healthy = match state.tiers.get(&t.id) {
            Some(b) => b.head(".console-healthcheck").await.is_ok(),
            None => false,
        };
        out.push(serde_json::json!({
            "id": t.id.as_str(),
            "kind": t.kind.as_str(),
            "path": t.path.as_ref().map(|p| p.display().to_string()),
            "endpoint": t.endpoint.as_str(),
            "bucket": t.bucket.as_str(),
            "healthy": healthy,
        }));
    }
    Json(out).into_response()
}

#[derive(Deserialize)]
struct BucketEncryptionBody {
    algorithm: String,
}

/// Task 6a：桶默认加密只读（读权限；磁盘桶不存在 → 404；未配置 → algorithm:null）。
async fn get_bucket_encryption(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
) -> Response {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Read).await {
        return r;
    }
    match state.store.bucket_exists(&bucket).await {
        Ok(true) => {}
        Ok(false) => return (StatusCode::NOT_FOUND, "NoSuchBucket").into_response(),
        Err(e) => return store_error(e).into_response(),
    }
    match state.acl().bucket_default_encryption(&bucket).await {
        Ok(algorithm) => Json(serde_json::json!({ "algorithm": algorithm })).into_response(),
        Err(e) => {
            tracing::error!(error = %e, bucket, "读桶默认加密失败");
            internal_error()
        }
    }
}

/// Task 6a：设置桶默认加密（桶主/管理员）。仅 AES256 / aws:kms；0 行 → 500。
async fn put_bucket_encryption(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
    Json(body): Json<BucketEncryptionBody>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    if !matches!(body.algorithm.as_str(), "AES256" | "aws:kms") {
        return (StatusCode::BAD_REQUEST, "algorithm 须为 AES256 或 aws:kms").into_response();
    }
    if state.control.is_raft() {
        return control_write_status(
            state
                .control
                .write(crate::raft::ControlCommand::BucketSetDefaultEncryption {
                    bucket,
                    algorithm: body.algorithm,
                })
                .await,
            "加密配置写入冲突",
        );
    }
    match state
        .acl()
        .set_bucket_default_encryption(&bucket, &body.algorithm)
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => {
            tracing::error!(bucket, "写桶默认加密无行（buckets 记录缺失）");
            internal_error()
        }
        Err(e) => {
            tracing::error!(error = %e, bucket, "写桶默认加密失败");
            internal_error()
        }
    }
}

/// Task 6a：清除桶默认加密（桶主/管理员，幂等）。
async fn delete_bucket_encryption(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    if state.control.is_raft() {
        return control_write_status(
            state
                .control
                .write(crate::raft::ControlCommand::BucketClearDefaultEncryption { bucket })
                .await,
            "加密配置写入冲突",
        );
    }
    match state.acl().clear_bucket_default_encryption(&bucket).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, bucket, "清桶默认加密失败");
            internal_error()
        }
    }
}

/// M8 S4b：admin 强制 Transition sweep（返回本轮入队数）。
async fn force_tier_sweep(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    let uid = match admin_of(&state, &payload).await {
        Ok(u) => u,
        Err(r) => return r,
    };
    match crate::tier_jobs::sweep_transitions(&state.db, &state.store, state.lifecycle.as_ref())
        .await
    {
        Ok(queued) => {
            state
                .audit(
                    uid,
                    "control:tier-sweep",
                    None,
                    None,
                    Some(&format!("queued={queued}")),
                )
                .await;
            Json(serde_json::json!({ "queued": queued })).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "强制 tier sweep 失败");
            internal_error()
        }
    }
}

/// M8：tier 远端 GC/对账请求体。缺省 dry-run；真删必须显式 `"dry_run": false`。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct TierGcBody {
    dry_run: bool,
    grace_days: u32,
    max_deletes: usize,
    /// None = 全部已配置 tier。
    tier: Option<String>,
}

impl Default for TierGcBody {
    fn default() -> Self {
        Self {
            dry_run: true,
            grace_days: crate::tier_gc_sweep::DEFAULT_GRACE_DAYS,
            max_deletes: crate::tier_gc_sweep::DEFAULT_MAX_DELETES,
            tier: None,
        }
    }
}

/// M8：admin 触发 tier 远端 GC/对账（dry-run 优先；live 引用与 grace 内对象永不删除）。
async fn tier_gc(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    body: Option<Json<TierGcBody>>,
) -> Response {
    let uid = match admin_of(&state, &payload).await {
        Ok(u) => u,
        Err(r) => return r,
    };
    let b = body.map(|Json(b)| b).unwrap_or_default();
    if let Some(id) = &b.tier
        && state.tiers.get(id).is_none()
    {
        return (StatusCode::BAD_REQUEST, format!("tier 不存在：{id}")).into_response();
    }
    let opts = crate::tier_gc_sweep::GcOptions {
        tier: b.tier,
        dry_run: b.dry_run,
        grace_days: b.grace_days,
        max_deletes: b.max_deletes,
    };
    if let Err(e) = crate::tier_gc_sweep::validate_opts(&opts) {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    match crate::tier_gc_sweep::run_gc(&state.store, &state.tiers, &opts).await {
        Ok(resp) => {
            let deleted: usize = resp.reports.iter().map(|r| r.deleted).sum();
            let failed: usize = resp.reports.iter().map(|r| r.failed).sum();
            state
                .audit(
                    uid,
                    "control:tier-gc",
                    None,
                    None,
                    Some(&format!(
                        "dry_run={} grace_days={} deleted={deleted} failed={failed}",
                        resp.dry_run, resp.grace_days
                    )),
                )
                .await;
            Json(resp).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "tier GC 失败");
            internal_error()
        }
    }
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
    let modes = match state.db.bucket_replication_modes().await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "查询桶复制模式失败");
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
            let replication_mode = modes
                .get(&b.name)
                .cloned()
                .unwrap_or_else(|| "async".into());
            Some(serde_json::json!({
                "name": b.name,
                "is_owner": own,
                "can_read": can_read,
                "can_write": can_write,
                "can_delete": can_delete,
                "replication_mode": replication_mode,
            }))
        })
        .collect();
    Json(out).into_response()
}

#[derive(Deserialize)]
struct CreateBucketBody {
    name: String,
    /// M4 切片 8：目标 set（缺省 0；校验 < 组数——单 set 下非 0 一律 400）
    #[serde(default)]
    set: Option<u16>,
    /// M9：`async`（缺省）/ `sync`；sync 需能力开关与 Raft 控制面就绪。
    #[serde(default)]
    replication_mode: Option<String>,
}

/// 任何**在册**用户可建桶（建桶者即桶主，自动全权；被停用账号的旧 JWT 在此同样失效）。
async fn create_bucket(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<CreateBucketBody>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    // M9 S0：Raft 控制写通道尚未接线（S6 翻转 RAFT_READY）。
    if state.cfg.raft.is_some() && !crate::raft::RAFT_READY {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "raft control plane is not ready",
        )
            .into_response();
    }
    let replication_mode = body.replication_mode.as_deref().unwrap_or("async");
    match replication_mode {
        "async" => {}
        "sync" => {
            // 无 [raft]：形态错误（400），先于“未就绪”判定。
            if state.cfg.raft.is_none() {
                return (
                    StatusCode::BAD_REQUEST,
                    "sync replication mode requires [raft]",
                )
                    .into_response();
            }
            // M9 S3（I3）：sync 建桶硬前置 = 控制面已就绪（外层 [raft] 检查之外的显式兜底）。
            if !crate::raft::RAFT_READY {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "raft control plane is not ready",
                )
                    .into_response();
            }
            // M9 S4：stretch 禁止 sync 桶（与 S3 面 router 的 sync_ready 同口径）。
            if !crate::raft::SYNC_READY || state.cfg.stretch.is_some() {
                return (
                    StatusCode::BAD_REQUEST,
                    "sync replication mode is not available",
                )
                    .into_response();
            }
            // M9 S3：sync 依赖唯一数据 peer（witness 不写 sites 行）。
            match state.db.list_sites().await {
                Ok(sites) if sites.len() == 1 => {}
                Ok(sites) => {
                    return (
                        StatusCode::CONFLICT,
                        format!("sync requires exactly one site peer (have {})", sites.len()),
                    )
                        .into_response();
                }
                Err(e) => {
                    tracing::error!(error = %e, "读站点 peer 数失败");
                    return internal_error();
                }
            }
        }
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid replication mode: {other}"),
            )
                .into_response();
        }
    }
    // M9：同名桶重建不允许改变复制模式（陈旧行清理不能静默改写语义）。
    if let Ok(Some(existing)) =
        sqlx::query_scalar::<_, String>("SELECT replication_mode FROM buckets WHERE name = ?")
            .bind(&body.name)
            .fetch_optional(state.db.pool())
            .await
        && existing != replication_mode
    {
        return (
            StatusCode::CONFLICT,
            "bucket replication mode cannot be changed on recreate",
        )
            .into_response();
    }
    // M9 S6c：Raft 一条命令建桶（行+物理目录 409 预检；apply 只做 create-or-converge）。
    if state.control.is_raft() {
        match sqlx::query_scalar::<_, i64>("SELECT 1 FROM buckets WHERE name = ?")
            .bind(&body.name)
            .fetch_optional(state.db.pool())
            .await
        {
            Ok(Some(_)) => {
                return (StatusCode::CONFLICT, "BucketAlreadyExists").into_response();
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!(error = %e, "读桶行失败");
                return internal_error();
            }
        }
        match state.store.bucket_exists(&body.name).await {
            Ok(true) => return (StatusCode::CONFLICT, "BucketAlreadyExists").into_response(),
            Ok(false) => {}
            Err(e) => return store_error(e).into_response(),
        }
        // 属主 uid → username 在 append 前解析（管理员 = None → owner_id NULL）。
        let owner = if is_admin {
            None
        } else {
            match state.db.get_user(uid).await {
                Ok(Some((name, _, _))) => Some(name),
                Ok(None) => return internal_error(),
                Err(e) => {
                    tracing::error!(error = %e, "查询桶主失败");
                    return internal_error();
                }
            }
        };
        match state
            .control
            .write(crate::raft::ControlCommand::BucketCreate {
                name: body.name.clone(),
                owner,
                created_at: crate::db::now(),
                replication_mode: replication_mode.to_string(),
                versioning: if replication_mode == "sync" {
                    "Enabled"
                } else {
                    "Off"
                }
                .to_string(),
                object_lock: false,
                object_lock_default_mode: None,
                object_lock_default_days: None,
                default_encryption: None,
            })
            .await
        {
            Ok(crate::raft::ControlResponse::Ok) => {}
            Ok(resp) => return control_rejection(resp, "BucketAlreadyExists"),
            Err(e) => return control_failure(e),
        }
        state.store.invalidate_gate(&body.name);
        state.store.broadcast_bucket_row(&body.name, Some(0)).await;
        return StatusCode::NO_CONTENT.into_response();
    }
    let set_count = state
        .store
        .cluster()
        .map_or(1, |c| c.sets.sets.len() as u16);
    // C3：显式 set 校验；缺省走自动指派（桶数最少的 set；单 set 恒 0）
    let set = match body.set {
        Some(s) => {
            if s >= set_count {
                return (StatusCode::BAD_REQUEST, "set 越界").into_response();
            }
            s
        }
        None => match state.acl().pick_least_loaded_set(set_count).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "set 指派失败，回落 set0");
                0
            }
        },
    };
    if let Err(e) = state.store.create_bucket(&body.name).await {
        return store_error(e).into_response();
    }
    // M5 切片 0：覆盖语义——陈旧行（删桶记账容忍失败残留）随重建收敛 set 与 owner
    if let Err(e) = state
        .db
        .upsert_bucket_owner_set_mode(&body.name, Some(uid), set, replication_mode)
        .await
    {
        // M7 P2：站点 LWW 门控拒绝时物理桶已建——按**最终行**补偿后返回 409：
        // 远端 upsert 胜出（行存在）保留物理桶；远端 delete 胜出（无行）才删空目录。
        if let Some(r) = site_conflict(&e) {
            let has_row = match sqlx::query_scalar::<_, i64>("SELECT 1 FROM buckets WHERE name = ?")
                .bind(&body.name)
                .fetch_optional(state.db.pool())
                .await
            {
                Ok(v) => v.is_some(),
                Err(re) => {
                    // 行状态未知：保留刚建的物理桶（宁可孤儿目录，不误删对端数据）。
                    tracing::warn!(error = %re, bucket = %body.name, "冲突补偿读桶行失败（保留物理桶）");
                    return r;
                }
            };
            if has_row {
                if !state.store.bucket_exists(&body.name).await.unwrap_or(false)
                    && let Err(re) = state.store.create_bucket(&body.name).await
                {
                    tracing::warn!(error = %re, bucket = %body.name, "冲突补偿补建物理桶失败");
                }
            } else if let Err(re) = state.store.delete_bucket(&body.name).await {
                tracing::warn!(error = %re, bucket = %body.name, "冲突回滚物理桶失败");
            }
            return r;
        }
        // M9：行写失败（含 guard ABORT）不得以 204 静默成功——回滚刚建的物理桶并 5xx。
        tracing::error!(error = %e, bucket = %body.name, "写入桶行失败");
        if let Err(re) = state.store.delete_bucket(&body.name).await {
            tracing::warn!(error = %re, bucket = %body.name, "行写失败回滚物理桶失败");
        }
        return internal_error();
    }
    // M9：门模式缓存失效（行已写入——新建 sync 桶首次写即生效）。
    state.store.invalidate_gate(&body.name);
    // M5 切片 0（P4-1）：行变更广播（best-effort）
    state
        .store
        .broadcast_bucket_row(&body.name, Some(set))
        .await;
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
        match state.acl().is_owner(&bucket, uid).await {
            Ok(true) => {}
            Ok(false) => return forbidden("只有桶主或管理员可以删除桶"),
            Err(e) => {
                tracing::error!(error = %e, "查询桶主失败");
                return internal_error();
            }
        }
    }
    // M9 S3：sync 桶远端优先（双站 quiesce/查空）；async 桶走原本地删除。
    match state.sync_delete.delete_bucket(&bucket).await {
        Ok(()) => {
            // M9 S6c：Raft 的 BucketDelete 已在 apply 级联清表；不再直写受 guard 的表。
            if state.control.is_raft() {
                state.bucket_cleanup.invalidate_ap_cache(&bucket);
                state.store.invalidate_gate(&bucket);
                return StatusCode::NO_CONTENT.into_response();
            }
            // B10：物理删除已生效——清 M8 控制面状态并失效 AP 缓存（失败只告警，不补偿桶）。
            // 放在分支之前：站点冲突补偿重建空桶时同样不得继承旧 AP/Inventory 配置。
            if let Err(e) = state.bucket_cleanup.clear_bucket_m8(&bucket).await {
                tracing::warn!(error = %e, bucket = %bucket, "删桶 M8 表清理失败");
            }
            state.bucket_cleanup.invalidate_ap_cache(&bucket);
            // M9：物理桶已删——无论行清理是否成功都不再把写当 sync 拦截。
            state.store.invalidate_gate(&bucket);
            if let Err(e) = state.acl().forget_bucket(&bucket).await {
                // M7 H1：门控 ABORT 时物理桶已删、行/授权未动——恢复空桶（删除前为空）
                // 并返回 409，绝不能让“物理已删 + 行残留 + 无 delete changelog”静默成功。
                if let Some(r) = site_conflict(&e) {
                    // 按最终行补偿：远端 delete 已胜出（无行）→ 本地删除同样已生效，204；
                    // 行仍在（远端 upsert/权限胜出）→ 恢复空桶并 409。
                    let has_row = match sqlx::query_scalar::<_, i64>(
                        "SELECT 1 FROM buckets WHERE name = ?",
                    )
                    .bind(&bucket)
                    .fetch_optional(state.db.pool())
                    .await
                    {
                        Ok(v) => v.is_some(),
                        Err(re) => {
                            // 行状态未知：恢复空桶，不让行/物理不一致。
                            tracing::warn!(error = %re, bucket = %bucket, "冲突补偿读桶行失败（恢复空桶）");
                            if let Err(x) = state.store.create_bucket(&bucket).await {
                                tracing::warn!(error = %x, bucket = %bucket, "冲突补偿恢复空桶失败");
                            }
                            return r;
                        }
                    };
                    if !has_row {
                        return StatusCode::NO_CONTENT.into_response();
                    }
                    if let Err(re) = state.store.create_bucket(&bucket).await {
                        tracing::warn!(error = %re, bucket = %bucket, "冲突补偿重建空桶失败");
                    }
                    return r;
                }
                // 桶已删除：记账行残留只影响列表展示，不失败请求
                tracing::warn!(error = ?e, "清理桶记账失败");
            }
            // S6-1：生命周期规则一并清——桶名可复用，旧规则复活会误删新桶对象
            if let Err(e) = state.acl().clear_lifecycle_rules(&bucket).await {
                tracing::warn!(error = ?e, "清理桶生命周期规则失败");
            }
            if let Err(e) = state.acl().clear_bucket_notification(&bucket).await {
                tracing::warn!(error = %e, bucket = %bucket, "删桶清通知配置失败");
            }
            if let Err(e) = state.acl().clear_bucket_policy(&bucket).await {
                tracing::warn!(error = ?e, "清理桶策略失败");
            }
            // M5 切片 0（P4-1）：删行广播（best-effort）
            state.store.broadcast_bucket_row(&bucket, None).await;
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
        marker: None,
    };
    match state.store.list_objects(&bucket, &req).await {
        Ok(r) => Json(serde_json::json!({
            "objects": r.objects.iter().map(|o| serde_json::json!({
                "key": o.key, "size": o.size, "etag": o.etag, "content_type": o.content_type,
                // Task 6a：Objects 页直接展示存储类/归档阶段/恢复到期，避免逐对象 HEAD
                "storage_class": o.storage_class.as_deref(),
                "tier_phase": o.tier_phase.map(|p| format!("{p:?}")),
                "restore_expires_at": o.restore_expires_at,
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
    // M6 切片 0（P2-1 复查补）：store 在锁内返回「是否真删」——空删幂等，只有真删才发事件
    // M9 S3：sync 桶远端优先（远端先清全部版本）；async 桶走原本地硬删。
    match state
        .sync_delete
        .delete_key_all_versions(&bucket, &key)
        .await
    {
        Ok(deleted) => {
            if deleted {
                // M6 切片 0（R5）：控制台删对象事件（硬删整键 → ObjectRemoved:Delete）
                state
                    .events
                    .emit(
                        xl_s3::events::S3EventOp::RemovedDelete,
                        &bucket,
                        &key,
                        None,
                        None,
                        None,
                        xl_s3::events::EventSource::internal("oss-console"),
                    )
                    .await;
                // M6 切片 4（V3②）：控制台整键硬删 → 目标 DELETE（无源版本号，状态面不维护）
                use xl_s3::replication::ReplicationSink as _;
                state
                    .repl_sink
                    .on_write(xl_s3::replication::WriteInfo {
                        bucket: &bucket,
                        key: &key,
                        version_id: None,
                        etag: "",
                        size: 0,
                        is_delete_marker: true,
                        is_replica: false,
                    })
                    .await;
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => store_error(e).into_response(),
    }
}

/// M8 S6 缺口 A：控制台对象标签（仅链尾版本，v1 不收版本参数）。
/// GET 需 Read；PUT/DELETE 需 Write（桶主/管理员在 require_perm 内已放行）。
async fn get_object_tags(
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
    match state.store.get_object_tags(&bucket, &key, None).await {
        Ok((_vid, tags)) => Json(serde_json::json!({ "tags": tags })).into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

#[derive(Deserialize)]
struct ObjectTagsBody {
    tags: Vec<xl_store::meta::ObjectTag>,
}

async fn put_object_tags(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path((bucket, key)): Path<(String, String)>,
    Json(body): Json<ObjectTagsBody>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Write).await {
        return r;
    }
    // M9：标签变更不跨站——sync 桶明确 501（S3 Tagging API 同口径）。
    match state.store.sync_mode(&bucket).await {
        Ok(true) => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                "object tagging is not supported on sync buckets",
            )
                .into_response();
        }
        Ok(false) => {}
        Err(e) => return store_error(e).into_response(),
    }
    // store 对 InvalidArgument 落 5xx（那是通用错误面），这里先校验回 400
    if let Err(msg) = xl_store::meta::validate_tags(&body.tags) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    match state
        .store
        .set_object_tags(&bucket, &key, None, body.tags)
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

async fn delete_object_tags(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if let Err(r) = require_perm(&state, uid, is_admin, &bucket, Action::Write).await {
        return r;
    }
    // M9：同 put_object_tags。
    match state.store.sync_mode(&bucket).await {
        Ok(true) => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                "object tagging is not supported on sync buckets",
            )
                .into_response();
        }
        Ok(false) => {}
        Err(e) => return store_error(e).into_response(),
    }
    match state
        .store
        .set_object_tags(&bucket, &key, None, Vec::new())
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
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
    let (uid, _) = match who(&state, &payload).await {
        Ok(v) => v,
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
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
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
    // M5 切片 6（T5）：库启用时封存入库；响应照旧回明文一次
    let stored = match &state.vault {
        Some(v) => match v.seal(&sk).await {
            Ok(sealed) => sealed,
            Err(e) => {
                tracing::error!(error = %e, "SK 加密失败——拒绝创建（fail-closed）");
                return (StatusCode::INTERNAL_SERVER_ERROR, "secret key 加密失败").into_response();
            }
        },
        None => sk.clone(),
    };
    // M9 S6b：仅 Raft 模式且非管理员属主走控制命令（K1）；M7/standalone 与 K2（管理员
    // 属主，guard 例外）保持原直写路径不变——ak/sk 与 created_at 在 append 前算好。
    if !state.control.is_raft() || is_admin {
        return match state.db.create_access_key(&ak, &stored, uid).await {
            Ok(()) => {
                Json(serde_json::json!({ "access_key": ak, "secret_key": sk })).into_response()
            }
            // UNIQUE 冲突说人话；不落 sqlite 原文
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                (StatusCode::BAD_REQUEST, "access key 已存在").into_response()
            }
            Err(e) => {
                tracing::error!(error = %e, "写入访问密钥失败");
                internal_error()
            }
        };
    }
    let username = match state.db.get_user(uid).await {
        Ok(Some((name, _, _))) => name,
        _ => return stale_user(),
    };
    let cmd = crate::raft::ControlCommand::AccessKeyCreate {
        access_key: ak.clone(),
        secret_key: stored,
        username,
        kind: "long-term".into(),
        parent_ak: None,
        session_policy: None,
        created_at: crate::db::now(),
    };
    match state.control.write(cmd).await {
        Ok(crate::raft::ControlResponse::Ok) => {
            Json(serde_json::json!({ "access_key": ak, "secret_key": sk })).into_response()
        }
        Ok(resp) => control_rejection(resp, "access key 已存在"),
        Err(e) => control_failure(e),
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
    // M9 S6b：M7/standalone 保持原直写路径不变；只有 Raft 模式走控制命令。
    if !state.control.is_raft() {
        return match state.db.create_user(username, &body.password, false).await {
            Ok(id) => Json(serde_json::json!({ "id": id })).into_response(),
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                (StatusCode::BAD_REQUEST, "用户名已存在").into_response()
            }
            Err(e) => {
                tracing::error!(error = %e, "创建用户失败");
                internal_error()
            }
        };
    }
    // M9 S6b U1：Argon2 与 created_at 在 append 前算好；apply 只写行（is_admin 恒 0）。
    let cmd = crate::raft::ControlCommand::UserCreate {
        username: username.to_string(),
        password_hash: crate::db::hash_password(&body.password),
        status: "active".into(),
        created_at: crate::db::now(),
    };
    match state.control.write(cmd).await {
        Ok(crate::raft::ControlResponse::Ok) => {
            // 命令不带本地自增 id（跨站点自然键是 username），提交后回查一次。
            match sqlx::query_scalar::<_, i64>("SELECT id FROM users WHERE username = ?")
                .bind(username)
                .fetch_optional(state.db.pool())
                .await
            {
                Ok(Some(id)) => Json(serde_json::json!({ "id": id })).into_response(),
                Ok(None) => {
                    tracing::error!(user = %username, "建用户已提交但回查不到行");
                    internal_error()
                }
                Err(e) => {
                    tracing::error!(error = %e, "回查新用户失败");
                    internal_error()
                }
            }
        }
        Ok(resp) => control_rejection(resp, "用户名已存在"),
        Err(e) => control_failure(e),
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
    let (username, is_admin) = match state.db.get_user(id).await {
        Ok(Some((name, true, _))) => match state.db.count_admins().await {
            Ok(n) if n <= 1 => {
                return (StatusCode::BAD_REQUEST, "不能删除最后一个管理员").into_response();
            }
            Ok(_) => (name, true),
            Err(e) => {
                tracing::error!(error = %e, "统计管理员失败");
                return internal_error();
            }
        },
        Ok(Some((name, false, _))) => (name, false),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    };
    // M9 S6b：管理员账号是本地 guard 例外，且 M7/standalone 保持原路径不变。
    if !state.control.is_raft() || is_admin {
        return match state.db.delete_user(id).await {
            Ok(true) => StatusCode::NO_CONTENT.into_response(),
            // 预检给友好文案；false 且用户仍在 = 并发下守卫触发（评审 P2 的原子兜底）
            Ok(false) => {
                if matches!(state.db.get_user(id).await, Ok(Some(_))) {
                    (StatusCode::BAD_REQUEST, "不能删除最后一个在册管理员").into_response()
                } else {
                    StatusCode::NOT_FOUND.into_response()
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "删除用户失败");
                internal_error()
            }
        };
    }
    // M9 S6b U2：命令以 username 为业务键；apply 事务内重跑最后管理员守卫与全量级联
    // （AKs/permissions/buckets.owner_id/uploads.owner_id）。
    match state
        .control
        .write(crate::raft::ControlCommand::UserDelete { username })
        .await
    {
        Ok(crate::raft::ControlResponse::Ok) => StatusCode::NO_CONTENT.into_response(),
        // 预检给友好文案；Conflict = 并发下 apply 守卫触发（评审 P2 的原子兜底）
        Ok(resp) => control_rejection(resp, "不能删除最后一个在册管理员"),
        Err(e) => control_failure(e),
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
    if !state.control.is_raft() {
        return match state.db.set_password(id, &body.password).await {
            Ok(true) => StatusCode::NO_CONTENT.into_response(),
            Ok(false) => StatusCode::NOT_FOUND.into_response(),
            Err(e) => {
                tracing::error!(error = %e, "重置口令失败");
                internal_error()
            }
        };
    }
    let (username, is_admin) = match state.db.get_user(id).await {
        Ok(Some((name, is_admin, _))) => (name, is_admin),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    };
    // M9 S6b：管理员账号是本地 guard 例外（不进复制集）。
    if is_admin {
        return match state.db.set_password(id, &body.password).await {
            Ok(true) => StatusCode::NO_CONTENT.into_response(),
            Ok(false) => StatusCode::NOT_FOUND.into_response(),
            Err(e) => {
                tracing::error!(error = %e, "重置口令失败");
                internal_error()
            }
        };
    }
    // M9 S6b U3：hash 在 append 前算好；apply 只更新 password_hash。
    match state
        .control
        .write(crate::raft::ControlCommand::UserSetPassword {
            username,
            password_hash: crate::db::hash_password(&body.password),
        })
        .await
    {
        Ok(crate::raft::ControlResponse::Ok) => StatusCode::NO_CONTENT.into_response(),
        Ok(resp) => control_rejection(resp, "重置口令冲突"),
        Err(e) => control_failure(e),
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
    let (username, is_admin) = match state.db.get_user(id).await {
        Ok(Some((name, is_admin, _))) => (name, is_admin),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    };
    // M9 S6b：管理员目标与 M7/standalone 保持原直写路径不变。
    if !state.control.is_raft() || is_admin {
        let rows: Vec<crate::db::PermissionRow> = body
            .into_iter()
            .map(|p| crate::db::PermissionRow {
                bucket: p.bucket,
                read: p.read,
                write: p.write,
                delete: p.delete,
            })
            .collect();
        return match state.db.set_permissions(id, &rows).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => {
                tracing::error!(error = %e, "写入权限失败");
                internal_error()
            }
        };
    }
    // M9 S6b U6：自然键 (username,bucket,3 bits)；apply 全量替换并丢弃全 0 行
    //（无行 = 无权限，与旧 set_permissions 一致）。本地 id 不进命令。
    let permissions: Vec<crate::raft::PermissionBits> = body
        .into_iter()
        .map(|p| crate::raft::PermissionBits {
            bucket: p.bucket,
            read: p.read,
            write: p.write,
            delete: p.delete,
        })
        .collect();
    match state
        .control
        .write(crate::raft::ControlCommand::UserSetPermissions {
            username,
            permissions,
        })
        .await
    {
        Ok(crate::raft::ControlResponse::Ok) => StatusCode::NO_CONTENT.into_response(),
        Ok(resp) => control_rejection(resp, "权限写入冲突"),
        Err(e) => control_failure(e),
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
    let (uid, is_admin) = match who(&state, &payload).await {
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
    // M9 S6b：管理员账号是本地 guard 例外；M7/standalone 走原直写路径。
    if !state.control.is_raft() || is_admin {
        return match state.db.set_password(uid, &body.new_password).await {
            Ok(_) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => {
                tracing::error!(error = %e, "改密失败");
                internal_error()
            }
        };
    }
    // M9 S6b U4：verify 只读（H1 已抑制 Argon2 透明升级）；改密本身必须走控制命令。
    match state
        .control
        .write(crate::raft::ControlCommand::UserSetPassword {
            username,
            password_hash: crate::db::hash_password(&body.new_password),
        })
        .await
    {
        Ok(crate::raft::ControlResponse::Ok) => StatusCode::NO_CONTENT.into_response(),
        Ok(crate::raft::ControlResponse::NotFound) => stale_user(),
        Ok(resp) => control_rejection(resp, "改密冲突"),
        Err(e) => control_failure(e),
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
    let (username, is_admin) = match state.db.get_user(id).await {
        Ok(Some((name, true, _))) if !body.active => match state.db.count_active_admins().await {
            Ok(n) if n <= 1 => {
                return (StatusCode::BAD_REQUEST, "不能停用最后一个在册管理员").into_response();
            }
            Ok(_) => (name, true),
            Err(e) => {
                tracing::error!(error = %e, "统计管理员失败");
                return internal_error();
            }
        },
        Ok(Some((name, is_admin, _))) => (name, is_admin),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询用户失败");
            return internal_error();
        }
    };
    // M9 S6b：管理员账号是本地 guard 例外；M7/standalone 走原直写路径。
    if !state.control.is_raft() || is_admin {
        return match state.db.set_user_status(id, body.active).await {
            Ok(true) => StatusCode::NO_CONTENT.into_response(),
            // 上面的预检给友好文案；false 且用户仍在 = 并发下守卫触发（评审 P2 的原子兜底）
            Ok(false) => {
                if matches!(state.db.get_user(id).await, Ok(Some(_))) {
                    (StatusCode::BAD_REQUEST, "不能停用最后一个在册管理员").into_response()
                } else {
                    StatusCode::NOT_FOUND.into_response()
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "更新用户状态失败");
                internal_error()
            }
        };
    }
    // M9 S6b U5：预检保留（友好文案），最后管理员守卫在 apply 内原子重检。
    let status = if body.active { "active" } else { "disabled" };
    match state
        .control
        .write(crate::raft::ControlCommand::UserSetStatus {
            username,
            status: status.into(),
        })
        .await
    {
        Ok(crate::raft::ControlResponse::Ok) => StatusCode::NO_CONTENT.into_response(),
        Ok(resp) => control_rejection(resp, "不能停用最后一个在册管理员"),
        Err(e) => control_failure(e),
    }
}

// ---------------- M7：站点身份与关系（单节点站点对） ----------------

#[derive(Debug, Deserialize)]
struct CreateSiteBody {
    peer_id: String,
    #[serde(default)]
    name: Option<String>,
    endpoint: String,
    region: String,
    #[serde(default)]
    priority: i64,
    s3_access_key: String,
    s3_secret_key: String,
    peer_site_token: String,
}

fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut d = 0u8;
    for i in 0..a.len() {
        d |= a[i] ^ b[i];
    }
    d == 0
}

fn site_disabled() -> Response {
    (StatusCode::NOT_FOUND, "site replication is not enabled").into_response()
}

/// M7：触发器 BEFORE 门控（`RAISE(ABORT,'site replication conflict')`）→ 409。
fn site_conflict(e: impl std::fmt::Display) -> Option<Response> {
    if e.to_string().contains("site replication conflict") {
        Some((StatusCode::CONFLICT, "site replication conflict").into_response())
    } else {
        None
    }
}

fn site_json(state: &AppState, with_token: bool) -> serde_json::Value {
    let mut v = serde_json::json!({
        "id": state.site.id,
        "name": state.site.name,
        "region": state.site.region,
        "priority": state.site.priority,
        "has_vault": state.vault.is_some(),
        "enabled": state.site_enabled,
        "raft_node_id": state.cfg.raft.as_ref().map(|r| r.node_id),
        "raft_enabled": state.cfg.raft.is_some(),
        "raft_role": state.cfg.raft.as_ref().map(|r| match r.role {
            crate::config::RaftRole::Voter => "voter",
            crate::config::RaftRole::Witness => "witness",
        }),
        "raft_bootstrap": state.cfg.raft.as_ref().map(|r| r.bootstrap),
        "raft_initialized": state.raft.as_ref().map(|h| h.membership_nonempty()),
        "admin_username": state.admin_username,
        "vault_probe": state.vault_probe,
        "version": env!("CARGO_PKG_VERSION"),
        "advertise_endpoint": state.cfg.raft.as_ref().map(|r| r.advertise_endpoint.clone()),
        "capabilities": {
            "raft": crate::raft::RAFT_READY && state.cfg.raft.is_some(),
            "sync": crate::raft::SYNC_READY && state.cfg.raft.is_some(),
            "stretch": crate::raft::STRETCH_READY && state.cfg.stretch.is_some(),
        },
    });
    // S4/S7 共用形状；fingerprint 只暴露 SHA-256，不回传 secret。
    if let Some(s) = &state.cfg.stretch {
        v["stretch"] = serde_json::json!({
            "node_id": s.node_id,
            "self_addr": s.self_addr,
            "peer_node_id": s.peer_node_id,
            "peer_addr": s.peer_addr,
            // 本端实盘数；对端声明值不可信（B2）。
            "drive_count": state.cfg.data_dirs.as_ref().map_or(1, |d| d.len()),
            "ec": { "data": state.cfg.ec.data, "parity": state.cfg.ec.parity },
            "placement": s.placement,
            "mesh_secret_fingerprint": hex::encode({
                use sha2::Digest as _;
                sha2::Sha256::digest(s.mesh_secret.as_bytes())
            }),
        });
    }
    if with_token {
        v["site_token"] = serde_json::Value::String(state.site.token.clone());
    }
    v
}

/// `GET /api/v1/site`（管理员）：本站身份 + 配对用 token。
async fn site_info(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    Json(site_json(&state, true)).into_response()
}

/// `GET /api/v1/site/identity`（token 面；JWT except）：供对端 ping/配对校验。
async fn site_identity(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    let Some(tok) = headers
        .get(crate::site_client::SITE_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
    else {
        return (StatusCode::UNAUTHORIZED, "missing site token").into_response();
    };
    if !ct_eq(tok, &state.site.token) {
        return (StatusCode::UNAUTHORIZED, "invalid site token").into_response();
    }
    Json(site_json(&state, false)).into_response()
}

fn peer_json(p: &crate::db::SitePeer) -> serde_json::Value {
    serde_json::json!({
        "peer_id": p.peer_id,
        "remote_site_id": p.remote_site_id,
        "name": p.name,
        "endpoint": p.endpoint,
        "region": p.region,
        "priority": p.priority,
        "last_seen": p.last_seen,
        "last_error": p.last_error,
        "raft_node_id": p.raft_node_id,
        "reconcile_required": p.reconcile_required,
        "has_credentials": true,
    })
}

/// `GET /api/v1/sites`（管理员；脱敏——绝不回显 AK/SK/token）。
async fn list_sites(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    match state.db.list_sites().await {
        Ok(rows) => Json(rows.iter().map(peer_json).collect::<Vec<_>>()).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "列出站点失败");
            internal_error()
        }
    }
}

const VAULT_PROBE_PLAINTEXT: &str = "oss-vault-probe-v1";

fn local_stretch_identity(state: &AppState) -> Option<crate::site_client::StretchIdentity> {
    let s = state.cfg.stretch.as_ref()?;
    Some(crate::site_client::StretchIdentity {
        node_id: s.node_id.clone(),
        self_addr: s.self_addr.clone(),
        peer_node_id: s.peer_node_id.clone(),
        peer_addr: s.peer_addr.clone(),
        drive_count: state.cfg.data_dirs.as_ref().map_or(1, |d| d.len()) as u16,
        ec: crate::site_client::EcIdentity {
            data: state.cfg.ec.data as u16,
            parity: state.cfg.ec.parity as u16,
        },
        placement: s.placement.clone(),
        mesh_secret_fingerprint: {
            use sha2::Digest as _;
            hex::encode(sha2::Sha256::digest(s.mesh_secret.as_bytes()))
        },
    })
}

fn stretch_mismatches(
    local: &Option<crate::site_client::StretchIdentity>,
    peer: &Option<crate::site_client::StretchIdentity>,
) -> Option<Vec<&'static str>> {
    match (local, peer) {
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => Some(vec!["stretch_presence"]),
        (Some(a), Some(b)) => {
            let mut m = Vec::new();
            // 两端配置互为镜像：本端 node/self_addr 对应对端 peer_*。
            if a.node_id != b.peer_node_id {
                m.push("node_id");
            }
            if a.peer_node_id != b.node_id {
                m.push("peer_node_id");
            }
            if a.self_addr != b.peer_addr {
                m.push("self_addr");
            }
            if a.peer_addr != b.self_addr {
                m.push("peer_addr");
            }
            if a.drive_count != b.drive_count {
                m.push("drive_count");
            }
            if a.ec != b.ec {
                m.push("ec");
            }
            if a.placement != b.placement {
                m.push("placement");
            }
            if a.mesh_secret_fingerprint != b.mesh_secret_fingerprint {
                m.push("mesh_secret_fingerprint");
            }
            (!m.is_empty()).then_some(m)
        }
    }
}

#[allow(clippy::result_large_err)]
async fn vault_probe_matches(state: &AppState, peer_probe: Option<&str>) -> Result<bool, Response> {
    let Some(vault) = state.vault.as_ref() else {
        return Ok(peer_probe.is_none());
    };
    let Some(probe) = peer_probe else {
        return Ok(false);
    };
    match vault.open(probe).await {
        Ok(Some(p)) => Ok(p == VAULT_PROBE_PLAINTEXT),
        Ok(None) => Ok(false),
        Err(e) => {
            tracing::warn!(error = %e, "vault probe unseal failed");
            Ok(false)
        }
    }
}

async fn membership_apply_any(
    state: &AppState,
    op: &crate::raft::membership::MembershipOp,
) -> Result<crate::raft::membership::MemberView, crate::raft::membership::MembershipError> {
    use crate::raft::membership::{self, MembershipError};
    let Some(handle) = state.raft.as_ref() else {
        return Err(MembershipError::NoQuorum);
    };
    let view = membership::view(handle);
    if view.leader == Some(handle.node_id()) {
        membership::apply_local(handle, op).await
    } else if let Some(endpoint) = view.leader_endpoint.clone() {
        let token = state
            .cfg
            .raft
            .as_ref()
            .map(|r| r.cluster_token.clone())
            .unwrap_or_default();
        membership::request_tls(&endpoint, &token, op, Some(state.tls.as_ref())).await
    } else {
        Err(MembershipError::NoQuorum)
    }
}

/// Raft 模式配对：预检 → 本地行 → 对端 /join → 成员变更。M7 路径不经过此函数。
async fn create_site_raft(state: AppState, body: CreateSiteBody, endpoint: String) -> Response {
    use crate::raft::membership::MembershipOp;
    let bad = |m: String| (StatusCode::BAD_REQUEST, m).into_response();
    let conflict = |m: String| (StatusCode::CONFLICT, m).into_response();
    let Some(local_raft) = state.cfg.raft.as_ref() else {
        return conflict("local node has no [raft]".into());
    };
    let ident = match crate::site_client::get_identity_typed_tls(
        &endpoint,
        &body.peer_site_token,
        Some(state.tls.as_ref()),
    )
    .await
    {
        Ok(i) => i,
        Err(crate::site_client::IdentityError::Unauthorized) => {
            return (StatusCode::FORBIDDEN, "invalid peer site token").into_response();
        }
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("peer identity: {e:?}")).into_response();
        }
    };
    if ident.id == state.site.id {
        return bad("对端 identity 与本机相同（endpoint 填错？）".into());
    }
    if ident.region != body.region {
        return bad(format!(
            "region 不一致：对端 {} / 配置 {}",
            ident.region, body.region
        ));
    }
    if ident.has_vault != state.vault.is_some() {
        return bad("主密钥库状态不一致（两端必须同时启用或同时未启用 vault）".into());
    }
    if !ident.raft_enabled {
        return conflict("peer has no [raft]".into());
    }
    let peer_bootstrap = ident.raft_bootstrap.unwrap_or(false);
    if peer_bootstrap == local_raft.bootstrap {
        return conflict("exactly one side must have [raft].bootstrap=true".into());
    }
    let Some(peer_node) = ident.raft_node_id.filter(|n| *n > 0) else {
        return conflict("raft node id conflict".into());
    };
    if peer_node == local_raft.node_id {
        return conflict("raft node id conflict".into());
    }
    match state.db.list_sites().await {
        Ok(rows)
            if rows
                .iter()
                .any(|r| r.raft_node_id == Some(peer_node) && r.remote_site_id != ident.id) =>
        {
            return conflict("raft node id conflict".into());
        }
        Ok(_) => {}
        Err(e) => {
            tracing::error!(error = %e, "列出站点失败");
            return internal_error();
        }
    }
    if ident.admin_username.is_none() || ident.admin_username != state.admin_username {
        return conflict("admin username mismatch".into());
    }
    if state.vault.is_some() {
        let ok = match vault_probe_matches(&state, ident.vault_probe.as_deref()).await {
            Ok(ok) => ok,
            Err(r) => return r,
        };
        if !ok {
            return conflict("vault master key mismatch".into());
        }
    }
    if let Some(m) = stretch_mismatches(&local_stretch_identity(&state), &ident.stretch) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "stretch identity mismatch", "mismatches": m})),
        )
            .into_response();
    }
    let peer_raft_endpoint = match ident.advertise_endpoint.as_deref() {
        Some(e) => match crate::site_client::validate_endpoint(e) {
            Ok(e) => e,
            Err(e) => return bad(format!("peer advertise_endpoint: {e}")),
        },
        None => return conflict("peer advertise_endpoint missing".into()),
    };

    // 现有行：同 remote_site_id 采用 M7 标签；node id 变化 = B′ 替换。
    let existing = state
        .db
        .list_sites()
        .await
        .ok()
        .and_then(|rows| rows.into_iter().find(|r| r.remote_site_id == ident.id));
    let old_node = existing.as_ref().and_then(|r| r.raft_node_id);
    let row_id = existing
        .as_ref()
        .map(|r| r.peer_id.clone())
        .unwrap_or_else(|| body.peer_id.clone());
    let mut peer = crate::db::SitePeer {
        peer_id: row_id.clone(),
        remote_site_id: ident.id.clone(),
        name: body.name.clone().unwrap_or_else(|| ident.name.clone()),
        endpoint,
        region: body.region,
        priority: body.priority,
        s3_access_key: body.s3_access_key,
        s3_secret_key: body.s3_secret_key,
        peer_site_token: body.peer_site_token,
        created_at: crate::db::now(),
        last_seen: Some(crate::db::now()),
        last_error: None,
        raft_node_id: Some(peer_node),
        reconcile_required: false,
    };

    // 管理员长时 AK（join body 需要与对方同主密钥封装的 SK）。
    let creds: Option<(String, String)> = sqlx::query_as(
        "SELECT a.access_key, a.secret_key FROM access_keys a JOIN users u ON u.id = a.user_id \
         WHERE u.is_admin = 1 AND a.kind = 'long-term' AND a.parent_ak IS NULL \
           AND a.status = 'active' ORDER BY a.created_at LIMIT 1",
    )
    .fetch_optional(state.db.pool())
    .await
    .ok()
    .flatten();
    let Some((admin_ak, sealed_sk)) = creds else {
        return conflict("pairing requires an active long-term admin access key".into());
    };
    let admin_sk = match &state.vault {
        Some(v) => match v.open(&sealed_sk).await {
            Ok(Some(s)) => s,
            _ => return conflict("cannot unseal admin access key".into()),
        },
        None => sealed_sk,
    };
    let local_stretch = local_stretch_identity(&state);
    let join_body = serde_json::json!({
        "site_id": state.site.id,
        "name": state.site.name,
        "region": state.site.region,
        "priority": state.site.priority,
        "endpoint": local_raft.advertise_endpoint,
        "raft_endpoint": local_raft.advertise_endpoint,
        "raft_node_id": local_raft.node_id,
        "raft_bootstrap": local_raft.bootstrap,
        "site_token": state.site.token,
        "admin_username": state.admin_username,
        "has_vault": state.vault.is_some(),
        "vault_probe": state.vault_probe,
        "s3_access_key": admin_ak,
        "s3_secret_key": admin_sk,
        "stretch": local_stretch,
    });
    let (status, resp_body) = match crate::site_client::post_raft_join_tls(
        &peer_raft_endpoint,
        &local_raft.cluster_token,
        &join_body,
        std::time::Duration::from_secs(20),
        Some(state.tls.as_ref()),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("peer join: {e}")).into_response();
        }
    };
    if status == 403 {
        return conflict("cluster token mismatch".into());
    }
    if status != 200 {
        return (
            StatusCode::BAD_GATEWAY,
            format!(
                "peer join returned {status}: {}",
                String::from_utf8_lossy(&resp_body)
                    .chars()
                    .take(300)
                    .collect::<String>()
            ),
        )
            .into_response();
    }

    let replaced = old_node.is_some_and(|old| old != peer_node);
    let op = if replaced {
        MembershipOp::ReplaceVoter {
            node_id: peer_node,
            endpoint: peer_raft_endpoint,
            old_node_id: old_node.unwrap(),
        }
    } else {
        MembershipOp::AddWitness {
            node_id: peer_node,
            endpoint: peer_raft_endpoint,
        }
    };
    if local_raft.bootstrap {
        if let Err(e) = membership_apply_any(&state, &op).await {
            return (StatusCode::SERVICE_UNAVAILABLE, format!("membership: {e}")).into_response();
        }
        if replaced {
            let _ = state.db.set_site_reconcile_required(&row_id, true).await;
        }
    } else {
        // 非 bootstrap 侧等待 bootstrap 侧完成成员变更（30 s）。
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let ok = state.raft.as_ref().is_some_and(|h| {
                let v = crate::raft::membership::view(h);
                v.voters.contains(&local_raft.node_id) && v.voters.contains(&peer_node)
            });
            if ok {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "membership not established by bootstrap peer; retry",
                )
                    .into_response();
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    // 成员变更成功后才更新本地行：若 ReplaceVoter/AddWitness 失败，旧 node id 保留在行里，
    // 重试仍能识别 B′ 替换并再次执行（S7.4 review I1）。
    if let Err(e) = state.db.upsert_pair_peer(&peer).await {
        tracing::error!(error = %e, "写站点行失败");
        return internal_error();
    }
    if replaced {
        let _ = state.db.set_site_reconcile_required(&row_id, true).await;
        peer.reconcile_required = true;
    }
    let initialized = state.raft.as_ref().is_some_and(|h| h.membership_nonempty());
    let _ = crate::raft::identity::record_initialized(&state.db, initialized).await;
    Json(peer_json(&peer)).into_response()
}

/// `POST /api/v1/site/raft/join`（集群令牌；配对对端调用）：写本地 sites 行，bootstrap 侧加成员。
#[derive(serde::Deserialize)]
struct RaftJoinBody {
    site_id: String,
    name: String,
    region: String,
    #[serde(default)]
    priority: i64,
    endpoint: String,
    raft_endpoint: String,
    raft_node_id: u64,
    raft_bootstrap: bool,
    site_token: String,
    admin_username: String,
    #[serde(default)]
    has_vault: bool,
    #[serde(default)]
    vault_probe: Option<String>,
    s3_access_key: String,
    s3_secret_key: String,
    #[serde(default)]
    stretch: Option<crate::site_client::StretchIdentity>,
}

async fn site_raft_join(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Response {
    let bad = |m: &str| (StatusCode::BAD_REQUEST, m.to_string()).into_response();
    let conflict = |m: &str| (StatusCode::CONFLICT, m.to_string()).into_response();
    let Some(local_raft) = state.cfg.raft.as_ref() else {
        return conflict("local node has no [raft]");
    };
    let provided = headers
        .get(crate::raft::RPC_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ct_eq(provided, &local_raft.cluster_token) {
        return (StatusCode::FORBIDDEN, "invalid raft token").into_response();
    }
    if !state.site_enabled {
        return site_disabled();
    }
    let body: RaftJoinBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => return bad(&format!("invalid join body: {e}")),
    };
    if !(32..=128).contains(&body.site_token.len())
        || !body.site_token.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return bad("site_token invalid");
    }
    let raft_endpoint = match crate::site_client::validate_endpoint(&body.raft_endpoint) {
        Ok(e) => e,
        Err(e) => return bad(&e),
    };
    let data_endpoint = match crate::site_client::validate_endpoint(&body.endpoint) {
        Ok(e) => e,
        Err(e) => return bad(&e),
    };
    // 反向校验：用对方站点令牌拉 identity，确认声明的 id/node/admin 一致。
    let ident = match crate::site_client::get_identity_typed_tls(
        &raft_endpoint,
        &body.site_token,
        Some(state.tls.as_ref()),
    )
    .await
    {
        Ok(i) => i,
        Err(crate::site_client::IdentityError::Unauthorized) => {
            return (StatusCode::FORBIDDEN, "invalid peer site token").into_response();
        }
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("peer identity: {e:?}")).into_response();
        }
    };
    if ident.id != body.site_id {
        return conflict("site id mismatch");
    }
    if ident.raft_node_id != Some(body.raft_node_id) {
        return conflict("raft node id conflict");
    }
    if ident.admin_username.as_deref() != Some(body.admin_username.as_str())
        || state.admin_username.as_deref() != Some(body.admin_username.as_str())
    {
        return conflict("admin username mismatch");
    }
    if !ident.raft_enabled {
        return conflict("peer has no [raft]");
    }
    if ident.region != body.region {
        return bad("region mismatch");
    }
    if ident.has_vault != body.has_vault || body.has_vault != state.vault.is_some() {
        return bad("vault state mismatch");
    }
    if let Some(ad) = ident.advertise_endpoint.as_deref()
        && ad.trim_end_matches('/') != raft_endpoint
    {
        return conflict("raft endpoint mismatch");
    }
    if ident.stretch != body.stretch {
        return conflict("stretch identity mismatch");
    }
    if state.vault.is_some() {
        let ok = match vault_probe_matches(&state, body.vault_probe.as_deref()).await {
            Ok(ok) => ok,
            Err(r) => return r,
        };
        if !ok {
            return conflict("vault master key mismatch");
        }
    }
    if body.raft_bootstrap == local_raft.bootstrap {
        return conflict("exactly one bootstrap");
    }
    if body.raft_node_id == 0 || body.raft_node_id == local_raft.node_id {
        return conflict("raft node id conflict");
    }
    match state.db.list_sites().await {
        Ok(rows)
            if rows.iter().any(|r| {
                r.raft_node_id == Some(body.raft_node_id) && r.remote_site_id != body.site_id
            }) =>
        {
            return conflict("raft node id conflict");
        }
        Ok(_) => {}
        Err(e) => {
            tracing::error!(error = %e, "列出站点失败");
            return internal_error();
        }
    }
    if let Some(m) = stretch_mismatches(&local_stretch_identity(&state), &body.stretch) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "stretch identity mismatch", "mismatches": m})),
        )
            .into_response();
    }
    // B′ 识别：沿用同 remote_site_id 的旧行标签；node id 变化 = 替换（两侧发起者同规则）。
    let existing = state
        .db
        .list_sites()
        .await
        .ok()
        .and_then(|rows| rows.into_iter().find(|r| r.remote_site_id == body.site_id));
    let old_node = existing.as_ref().and_then(|r| r.raft_node_id);
    let row_id = existing
        .as_ref()
        .map(|r| r.peer_id.clone())
        .unwrap_or_else(|| body.site_id.clone());
    let replaced = old_node.is_some_and(|old| old != body.raft_node_id);
    let peer = crate::db::SitePeer {
        peer_id: row_id.clone(),
        remote_site_id: body.site_id.clone(),
        name: body.name,
        endpoint: data_endpoint,
        region: body.region,
        priority: body.priority,
        s3_access_key: body.s3_access_key,
        s3_secret_key: body.s3_secret_key,
        peer_site_token: body.site_token,
        created_at: crate::db::now(),
        last_seen: Some(crate::db::now()),
        last_error: None,
        raft_node_id: Some(body.raft_node_id),
        reconcile_required: false,
    };
    // 成员变更先于本地行更新：失败时旧 node id 仍在，重试可重新识别 B′。
    let membership_op = if replaced {
        Some(crate::raft::membership::MembershipOp::ReplaceVoter {
            node_id: body.raft_node_id,
            endpoint: raft_endpoint.clone(),
            old_node_id: old_node.unwrap(),
        })
    } else if local_raft.bootstrap {
        Some(crate::raft::membership::MembershipOp::AddWitness {
            node_id: body.raft_node_id,
            endpoint: raft_endpoint.clone(),
        })
    } else {
        None
    };
    if let Some(op) = membership_op
        && let Err(e) = membership_apply_any(&state, &op).await
    {
        return (StatusCode::SERVICE_UNAVAILABLE, format!("membership: {e}")).into_response();
    }
    if let Err(e) = state.db.upsert_pair_peer(&peer).await {
        tracing::error!(error = %e, "join 写站点行失败");
        return internal_error();
    }
    if replaced {
        let _ = state.db.set_site_reconcile_required(&row_id, true).await;
    }
    let view = state
        .raft
        .as_ref()
        .map(|h| crate::raft::membership::view(h))
        .unwrap_or_default();
    Json(serde_json::json!({
        "site_id": state.site.id,
        "raft_node_id": local_raft.node_id,
        "leader": view.leader,
        "voters": view.voters,
        "learners": view.learners,
        "nodes": view.nodes,
    }))
    .into_response()
}

/// `POST /api/v1/sites`（管理员）：建立（至多一个）站点对。同步校验对端可达与 region 一致性。
async fn create_site(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<CreateSiteBody>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    let bad = |m: String| (StatusCode::BAD_REQUEST, m).into_response();
    if body.peer_id.is_empty()
        || body.peer_id.len() > 64
        || !body
            .peer_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return bad("peer_id 非法（[A-Za-z0-9._-]，≤64）".into());
    }
    if body.peer_id == state.site.id {
        return bad("peer_id 不能等于本机站点 id".into());
    }
    if !(32..=128).contains(&body.peer_site_token.len())
        || !body.peer_site_token.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return bad("peer_site_token 非法（32–128 位十六进制）".into());
    }
    if body.s3_access_key.trim().is_empty() || body.s3_secret_key.trim().is_empty() {
        return bad("s3_access_key/secret_key 不能为空".into());
    }
    if body.region.trim().is_empty() || body.region.len() > 64 {
        return bad("[site].region 非法".into());
    }
    let endpoint = match crate::site_client::validate_endpoint(&body.endpoint) {
        Ok(e) => e,
        Err(e) => return bad(e),
    };
    if state.control.is_raft() {
        return create_site_raft(state, body, endpoint).await;
    }
    match state.db.count_sites().await {
        Ok(n) if n >= 1 => return bad("M7 仅支持单站点对（已存在一个 peer）".into()),
        Ok(_) => {}
        Err(e) => {
            tracing::error!(error = %e, "统计站点失败");
            return internal_error();
        }
    }
    // 同步校验对端可达 + region 一致 + 非本机
    let ident = match crate::site_client::get_identity_tls(
        &endpoint,
        &body.peer_site_token,
        Some(state.tls.as_ref()),
    )
    .await
    {
        Ok(i) => i,
        Err(e) => return bad(format!("对端 identity 校验失败：{e}")),
    };
    if ident.id == state.site.id {
        return bad("对端 identity 与本机相同（endpoint 填错？）".into());
    }
    if ident.region != body.region {
        return bad(format!(
            "region 不一致：对端 {} / 配置 {}",
            ident.region, body.region
        ));
    }
    // M7：AK 密文按各自主密钥封装——两端 vault 状态必须一致（否则运行期认证 500）。
    if ident.has_vault != state.vault.is_some() {
        return bad(
            "主密钥库状态不一致：两端必须同时启用或同时未启用 vault（AK 密文需同主密钥）".into(),
        );
    }
    let peer = crate::db::SitePeer {
        peer_id: body.peer_id.clone(),
        remote_site_id: ident.id.clone(),
        name: body.name.unwrap_or_else(|| body.peer_id.clone()),
        endpoint,
        region: body.region,
        priority: body.priority,
        s3_access_key: body.s3_access_key,
        s3_secret_key: body.s3_secret_key,
        peer_site_token: body.peer_site_token,
        created_at: crate::db::now(),
        last_seen: Some(crate::db::now()),
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    };
    match crate::site::insert_peer_with_snapshot(&state, &peer).await {
        Ok(()) => Json(peer_json(&peer)).into_response(),
        Err(e) if e.contains("UNIQUE") || e.contains("已存在站点对") => {
            bad("peer 已存在（M7 单站点对）".into())
        }
        Err(e) => {
            tracing::error!(error = %e, "写站点/快照失败");
            internal_error()
        }
    }
}

/// `GET /api/v1/sites/{peer_id}/status`（管理员）：游标 lag / outbox 积压 / 死信。
async fn site_status(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(peer_id): Path<String>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    let Some(peer) = state.db.get_site(&peer_id).await.ok().flatten() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let pool = state.db.pool();
    let max_seq: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq),0) FROM site_changelog")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let cursor: i64 = sqlx::query_scalar("SELECT last_seq FROM site_cursors WHERE peer_id = ?")
        .bind(&peer_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or(0);
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE target_kind='site' AND remote_id=? \
         AND state='pending'",
    )
    .bind(&peer_id)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let sending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE target_kind='site' AND remote_id=? \
         AND state='sending'",
    )
    .bind(&peer_id)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let dead_objs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE target_kind='site' AND remote_id=? \
         AND state='dead'",
    )
    .bind(&peer_id)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let dead_ctrl = crate::site::dead_count(pool, &peer.remote_site_id)
        .await
        .unwrap_or(0);
    Json(serde_json::json!({
        "peer_id": peer.peer_id,
        "remote_site_id": peer.remote_site_id,
        "control_lag": (max_seq - cursor).max(0),
        "control_dead": dead_ctrl,
        "object_pending": pending,
        "object_sending": sending,
        "object_dead": dead_objs,
        "last_seen": peer.last_seen,
        "last_error": peer.last_error,
    }))
    .into_response()
}

/// `DELETE /api/v1/sites/{peer_id}`（管理员）：解除站点对（同事务丢弃 site 出站行与游标）。
async fn delete_site(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(peer_id): Path<String>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    // M9 S7.3：Raft 模式下先做成员移除；任何失败都不得删本地对端行。
    if state.control.is_raft() {
        let sync_buckets: i64 = match sqlx::query_scalar(
            "SELECT COUNT(*) FROM buckets WHERE replication_mode = 'sync'",
        )
        .fetch_one(state.db.pool())
        .await
        {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(error = %e, "统计 sync 桶失败");
                return internal_error();
            }
        };
        if sync_buckets > 0 {
            return (
                StatusCode::CONFLICT,
                "sync buckets present; remove or convert them before unpairing",
            )
                .into_response();
        }
        let peer = match state.db.get_site(&peer_id).await {
            Ok(Some(p)) => p,
            Ok(None) => return StatusCode::NOT_FOUND.into_response(),
            Err(e) => {
                tracing::error!(error = %e, "读取站点失败");
                return internal_error();
            }
        };
        if let Some(node_id) = peer.raft_node_id {
            let Some(handle) = state.raft.as_ref() else {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "raft handle unavailable; cannot remove voter",
                )
                    .into_response();
            };
            let op = crate::raft::membership::MembershipOp::RemoveNode { node_id };
            let view = crate::raft::membership::view(handle);
            let result = if view.leader == Some(handle.node_id()) {
                crate::raft::membership::apply_local(handle, &op).await
            } else if let Some(endpoint) = view.leader_endpoint.clone() {
                let token = state
                    .cfg
                    .raft
                    .as_ref()
                    .map(|r| r.cluster_token.clone())
                    .unwrap_or_default();
                crate::raft::membership::request_tls(
                    &endpoint,
                    &token,
                    &op,
                    Some(state.tls.as_ref()),
                )
                .await
            } else {
                Err(crate::raft::membership::MembershipError::NoQuorum)
            };
            use crate::raft::membership::MembershipError;
            match result {
                Ok(_) => {}
                Err(MembershipError::Invalid(msg)) => {
                    return (StatusCode::CONFLICT, msg).into_response();
                }
                Err(MembershipError::NotLeader { .. }) => {
                    return (StatusCode::SERVICE_UNAVAILABLE, "no leader").into_response();
                }
                Err(MembershipError::NoQuorum) => {
                    return (StatusCode::SERVICE_UNAVAILABLE, "no quorum").into_response();
                }
                Err(MembershipError::Raft(msg)) => {
                    return (StatusCode::BAD_GATEWAY, msg).into_response();
                }
            }
        }
    }
    // P1-3：解配在事务内用 `DELETE ... RETURNING` 收集被丢弃的非空版本，删后逐个结算。
    match state.db.delete_site(&peer_id).await {
        Ok(Some(affected)) => {
            for (bucket, key, version_id) in affected {
                crate::replication::settle_version_after_drop(
                    state.db.pool(),
                    &state.store,
                    &bucket,
                    &key,
                    &version_id,
                )
                .await;
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "删站点失败");
            internal_error()
        }
    }
}

/// `POST /api/v1/sites/{peer_id}/ping`（管理员）：token 面校验对端并记录 last_seen/last_error。
async fn ping_site(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(peer_id): Path<String>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if let Err(r) = admin_of(&state, &payload).await {
        return r;
    }
    let Some(peer) = state.db.get_site(&peer_id).await.ok().flatten() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match crate::site_client::get_identity_tls(
        &peer.endpoint,
        &peer.peer_site_token,
        Some(state.tls.as_ref()),
    )
    .await
    {
        Ok(ident) => {
            let now = crate::db::now();
            if let Err(e) = state.db.update_site_ping(&peer_id, now, None).await {
                tracing::warn!(error = %e, "记录 ping 失败");
            }
            if ident.region != peer.region {
                return (
                    StatusCode::BAD_REQUEST,
                    format!(
                        "region 不一致：对端 {} / 配置 {}",
                        ident.region, peer.region
                    ),
                )
                    .into_response();
            }
            Json(serde_json::json!({
                "ok": true,
                "id": ident.id,
                "name": ident.name,
                "region": ident.region,
                "priority": ident.priority,
                "has_vault": ident.has_vault,
            }))
            .into_response()
        }
        Err(e) => {
            let now = crate::db::now();
            let _ = state.db.update_site_ping(&peer_id, now, Some(&e)).await;
            (StatusCode::BAD_GATEWAY, format!("ping 失败：{e}")).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct ChangelogBody {
    entries: Vec<crate::site::Entry>,
}

/// M9 S6d：Raft 模式不复用 M7 changelog（控制面走 Raft 日志，S7 的 `/site/raft/*`）。
/// M7（`[raft]` off）下 GET 仍是 405（原先只有 POST），POST 行为逐字节不变。
fn changelog_raft_conflict(state: &AppState) -> Option<Response> {
    state.control.is_raft().then(|| {
        (
            StatusCode::CONFLICT,
            "M7 site changelog is disabled in Raft mode",
        )
            .into_response()
    })
}

async fn site_changelog_get(State(state): State<AppState>) -> Response {
    if let Some(r) = changelog_raft_conflict(&state) {
        return r;
    }
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(axum::http::header::ALLOW, "POST")],
    )
        .into_response()
}

/// `POST /api/v1/site/changelog`（token 面；JWT except）：应用对端控制面变更批次。
async fn site_changelog(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ChangelogBody>,
) -> Response {
    if let Some(r) = changelog_raft_conflict(&state) {
        return r;
    }
    if !state.site_enabled {
        return site_disabled();
    }
    let Some(tok) = headers
        .get(crate::site_client::SITE_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
    else {
        return (StatusCode::UNAUTHORIZED, "missing site token").into_response();
    };
    if !ct_eq(tok, &state.site.token) {
        return (StatusCode::UNAUTHORIZED, "invalid site token").into_response();
    }
    // L8：token 不能脱离“当前配对关系”单独生效——解配后旧 token 立即失效；
    // 每条 origin 必须是当前唯一 peer 的 remote_site_id（禁止伪造归属）。
    let remote: Option<String> =
        match sqlx::query_scalar("SELECT remote_site_id FROM sites LIMIT 1")
            .fetch_optional(state.db.pool())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "读站点对失败");
                return internal_error();
            }
        };
    let Some(remote) = remote else {
        return (StatusCode::UNAUTHORIZED, "site pair not established").into_response();
    };
    if body.entries.iter().any(|e| e.origin_site_id != remote) {
        return (
            StatusCode::UNAUTHORIZED,
            "origin does not match paired site",
        )
            .into_response();
    }
    match crate::site::apply_changelog(&state, body.entries).await {
        Ok(st) => Json(serde_json::json!({
            "applied": st.applied, "skipped": st.skipped, "dead": st.dead,
        }))
        .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "changelog 应用失败（整批重试）");
            (StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct SiteDeleteBody {
    bucket: String,
    key: String,
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    marker_id: Option<String>,
    /// 控制台整键硬删（sync：远端先清全部版本）。
    #[serde(default)]
    all_versions: bool,
}

#[derive(Debug, Deserialize)]
struct SiteHeadQuery {
    bucket: String,
    key: String,
    version_id: String,
}

#[derive(Debug, Deserialize)]
struct SiteBucketQuery {
    bucket: String,
}

#[derive(Debug, Deserialize)]
struct SiteBucketBody {
    bucket: String,
    /// quiesce 回滚（超时/非空路径）。
    #[serde(default)]
    release: bool,
}

/// 内部路由的 store 错误分档（503/409 语义必须透传；其余 500 不外泄细节）。
fn site_store_error(e: xl_store::error::Error) -> Response {
    use xl_store::error::Error;
    let status = match &e {
        Error::NoSuchBucket(_) | Error::NoSuchKey(_) => StatusCode::NOT_FOUND,
        Error::BucketNotEmpty(_) | Error::Conflict(_) => StatusCode::CONFLICT,
        Error::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        Error::InvalidBucketName(_) | Error::InvalidKey(_) | Error::InvalidArgument(_) => {
            StatusCode::BAD_REQUEST
        }
        _ => {
            tracing::error!(error = %e, "站点内部路由 store 操作失败");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    (status, e.to_string()).into_response()
}

/// token 校验（常量时间；与既有站点路由同口径）。
fn site_token_ok(state: &AppState, headers: &HeaderMap) -> bool {
    headers
        .get(crate::site_client::SITE_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|tok| ct_eq(tok, &state.site.token))
}

/// m3：内部站点路由只服务 sync 桶；否则持有 site token 的对端可强制 local-only
/// 删除 async 桶数据。行不存在（桶已在接收端消失）同样拒绝。
async fn site_require_sync(state: &AppState, bucket: &str) -> Option<Response> {
    match state.db.bucket_sync_state_row(bucket).await {
        Ok(Some((mode, _, _))) if mode == "sync" => None,
        Ok(_) => Some((StatusCode::CONFLICT, "not a sync bucket").into_response()),
        Err(e) => {
            tracing::error!(error = %e, bucket, "读桶复制模式失败");
            Some(internal_error())
        }
    }
}

/// `POST /api/v1/site/delete-object`（token 面；JWT except）：sync 补偿的本地定点删除。
/// 只删本站版本，永不转发——普通 S3 DELETE 是 remote-first 路径，走它会 A→B→A 回环。
async fn site_delete_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SiteDeleteBody>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if !site_token_ok(&state, &headers) {
        return (StatusCode::FORBIDDEN, "invalid site token").into_response();
    }
    if let Some(r) = site_require_sync(&state, &body.bucket).await {
        return r;
    }
    // 接收端 local-only：只做本地删除/marker，永不转发（B4）。
    if body.all_versions {
        return match state.store.delete_object(&body.bucket, &body.key).await {
            Ok(_) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => site_store_error(e),
        };
    }
    if let Some(marker) = body.marker_id {
        let spec = xl_store::store::DeleteMarkerSpec {
            marker_uuid: marker.clone(),
            remote_version_id: Some(marker),
        };
        return match state
            .store
            .delete_object_versioned_with_marker(
                &body.bucket,
                &body.key,
                None,
                VersioningMode::Enabled,
                false,
                Some(spec),
            )
            .await
        {
            Ok(_) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => site_store_error(e),
        };
    }
    let Some(vid) = body.version_id else {
        return (
            StatusCode::BAD_REQUEST,
            "version_id, marker_id or all_versions is required",
        )
            .into_response();
    };
    // sync 桶创建即 Enabled；显式版本删除对不存在版本幂等成功（DeleteOutcome::NotFound）。
    match state
        .store
        .delete_object_versioned_ext(
            &body.bucket,
            &body.key,
            Some(&vid),
            VersioningMode::Enabled,
            false,
        )
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => site_store_error(e),
    }
}

/// `HEAD /api/v1/site/head-object`（token 面）：远端版本存在性探询。
/// 200 = 版本存在；405 = delete marker；404 = 不存在（恢复规则的三态）。
async fn site_head_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<SiteHeadQuery>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if !site_token_ok(&state, &headers) {
        return (StatusCode::FORBIDDEN, "invalid site token").into_response();
    }
    match state
        .store
        .head_object_version(&q.bucket, &q.key, &q.version_id)
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(xl_store::error::Error::DeleteMarker(_)) => {
            StatusCode::METHOD_NOT_ALLOWED.into_response()
        }
        Err(
            e @ (xl_store::error::Error::NoSuchKey(_) | xl_store::error::Error::NoSuchBucket(_)),
        ) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
        Err(e) => site_store_error(e),
    }
}

/// `GET /api/v1/site/bucket-empty?bucket=`：204 空 / 409 非空 / 404 桶不存在。
async fn site_bucket_empty(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<SiteBucketQuery>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if !site_token_ok(&state, &headers) {
        return (StatusCode::FORBIDDEN, "invalid site token").into_response();
    }
    match state.store.bucket_exists(&q.bucket).await {
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Ok(true) => {}
        Err(e) => return site_store_error(e),
    }
    match state.store.bucket_is_empty(&q.bucket).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (StatusCode::CONFLICT, "bucket not empty").into_response(),
        Err(e) => site_store_error(e),
    }
}

/// `POST /api/v1/site/bucket-quiesce {bucket[,release]}`：置 deleting 并等本站写者归零
/// （30s 上限），成功保持标志直到 delete-bucket/释放。`release=true` 回滚。
async fn site_bucket_quiesce(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SiteBucketBody>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if !site_token_ok(&state, &headers) {
        return (StatusCode::FORBIDDEN, "invalid site token").into_response();
    }
    if body.release {
        state.store.unmark_deleting(&body.bucket);
        return StatusCode::NO_CONTENT.into_response();
    }
    match state.store.bucket_exists(&body.bucket).await {
        // 桶已不存在 = 无写者可等：视作已 quiesce（对端删除后的 intent 重试幂等）。
        Ok(false) => return StatusCode::NO_CONTENT.into_response(),
        Ok(true) => {}
        Err(e) => return site_store_error(e),
    }
    if let Some(r) = site_require_sync(&state, &body.bucket).await {
        return r;
    }
    state.store.mark_deleting(&body.bucket);
    if state
        .store
        .wait_writers_idle(&body.bucket, std::time::Duration::from_secs(30))
        .await
    {
        StatusCode::NO_CONTENT.into_response()
    } else {
        state.store.unmark_deleting(&body.bucket);
        (StatusCode::CONFLICT, "timed out waiting for active writers").into_response()
    }
}

/// `POST /api/v1/site/delete-bucket {bucket}`：接收端本地删桶（只删本站，永不转发）。
async fn site_delete_bucket(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SiteBucketBody>,
) -> Response {
    if !state.site_enabled {
        return site_disabled();
    }
    if !site_token_ok(&state, &headers) {
        return (StatusCode::FORBIDDEN, "invalid site token").into_response();
    }
    if let Some(r) = site_require_sync(&state, &body.bucket).await {
        return r;
    }
    // m1：原子 tombstone rename + 重试清理，避免逐盘 remove 中途失败留半删目录。
    match state.store.delete_bucket_sync(&body.bucket).await {
        Ok(()) => {
            state.store.unmark_deleting(&body.bucket);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => site_store_error(e),
    }
}

#[cfg(test)]
mod stretch_identity_tests {
    use super::*;
    use crate::site_client::{EcIdentity, StretchIdentity};

    fn id(node: &str, self_addr: &str, peer: &str, peer_addr: &str) -> StretchIdentity {
        StretchIdentity {
            node_id: node.into(),
            self_addr: self_addr.into(),
            peer_node_id: peer.into(),
            peer_addr: peer_addr.into(),
            drive_count: 6,
            ec: EcIdentity { data: 8, parity: 4 },
            placement: "v2".into(),
            mesh_secret_fingerprint: "f".repeat(64),
        }
    }

    #[test]
    fn mirrored_stretch_identities_match() {
        let a = id("A", "a:9100", "B", "b:9100");
        let b = id("B", "b:9100", "A", "a:9100");
        assert_eq!(stretch_mismatches(&Some(a), &Some(b)), None);
    }

    #[test]
    fn non_mirrored_stretch_identities_mismatch() {
        let a = id("A", "a:9100", "B", "b:9100");
        let same = a.clone();
        let m = stretch_mismatches(&Some(a), &Some(same)).unwrap();
        assert!(m.contains(&"node_id"));
        assert!(m.contains(&"peer_node_id"));
        assert!(m.contains(&"self_addr"));
        assert!(m.contains(&"peer_addr"));
    }
}
