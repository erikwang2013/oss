//! M8 S1：访问点控制面（AP 来源实现 + CRUD API）。

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use base64::Engine as _;
use jwt_rust::JwtPayload;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use crate::console::{control_failure, control_rejection};
use crate::db::Db;
use crate::raft::{ControlCommand, ControlResponse};
use crate::state::AppState;
use xl_s3::ap::{AccessPoint, ApSource, valid_ap_name};
use xl_s3::olap::OlapTransform;

/// `/api/v1/access-points`（挂 console nest 下；JWT 已由外层 JwtLayer 校验）。
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/access-points", get(list).post(create))
        .route("/access-points/{name}", get(get_one).delete(delete_one))
        .route("/access-points/{name}/policy", put(put_policy))
        .route(
            "/access-points/{name}/transform",
            // wasm 模块 ≤16 MiB，base64 后 ~22 MiB；axum Json 默认 2 MiB 不够。
            get(get_transform)
                .put(put_transform)
                .layer(DefaultBodyLimit::max(24 * 1024 * 1024)),
        )
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[allow(clippy::result_large_err)]
async fn who(state: &AppState, payload: &JwtPayload) -> Result<(i64, bool), Response> {
    let uid: i64 = payload
        .sub()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "invalid token payload").into_response())?;
    match state.db.get_user(uid).await {
        Ok(Some((_, admin, true))) => Ok((uid, admin)),
        Ok(Some(_)) | Ok(None) => {
            Err((StatusCode::UNAUTHORIZED, "用户不存在或已失效").into_response())
        }
        Err(e) => {
            tracing::error!(error = %e, "AP 鉴权查询失败");
            Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response())
        }
    }
}

fn bad(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, msg.to_string()).into_response()
}

fn row_json(r: &crate::db::AccessPointRow) -> serde_json::Value {
    serde_json::json!({
        "name": r.name, "bucket": r.bucket, "owner_id": r.owner_id,
        // 别名确定性地由名字派生，不落库（改名不存在，删建即换）。
        "alias": format!("{}{}", r.name, xl_s3::ap::ALIAS_SUFFIX),
        "policy": r.policy_json.as_ref().and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
        "created_at": r.created_at, "updated_at": r.updated_at,
    })
}

async fn list(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
) -> Response {
    let (uid, admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match state.db.list_access_points(None).await {
        Ok(rows) => Json(
            rows.iter()
                .filter(|r| admin || r.owner_id == uid)
                .map(row_json)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "列 AP 失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

#[derive(Deserialize)]
struct CreateAp {
    name: String,
    bucket: String,
    #[serde(default)]
    policy: Option<serde_json::Value>,
}

async fn create(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(body): Json<CreateAp>,
) -> Response {
    let (uid, admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !valid_ap_name(&body.name) {
        return bad("AP 名称非法（3..=63，小写字母/数字/-）");
    }
    let is_owner = match state.acl().is_owner(&body.bucket, uid).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "AP 建桶归属查询失败");
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    if !admin && !is_owner {
        return (StatusCode::FORBIDDEN, "需要桶主或管理员").into_response();
    }
    if !matches!(state.store.bucket_exists(&body.bucket).await, Ok(true)) {
        return (StatusCode::NOT_FOUND, "桶不存在").into_response();
    }
    // 严格策略校验（保存 400）
    let policy_json = match &body.policy {
        None | Some(serde_json::Value::Null) => None,
        Some(v) => {
            let raw = v.to_string();
            match AccessPoint::from_row(
                body.name.clone(),
                body.bucket.clone(),
                uid,
                Some(raw.clone()),
                &state.ap_region,
            ) {
                Ok(_) => Some(raw),
                Err(e) => return bad(&e),
            }
        }
    };
    let ts = now();
    // M9 S6d：Raft 一条命令（属主 uid→username 预解析；apply 只做确定性映射）。
    // 缓存由提交后 SideEffect::ApReload 刷新。
    if state.control.is_raft() {
        let owner = if admin {
            None
        } else {
            match state.db.get_user(uid).await {
                Ok(Some((name, _, _))) => Some(name),
                Ok(None) => {
                    return (StatusCode::UNAUTHORIZED, "用户不存在或已失效").into_response();
                }
                Err(e) => {
                    tracing::error!(error = %e, "查询 AP 属主失败");
                    return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
                }
            }
        };
        return match state
            .control
            .write(ControlCommand::AccessPointPut {
                name: body.name.clone(),
                bucket: body.bucket.clone(),
                owner,
                policy_json: policy_json.clone(),
                created_at: ts,
                updated_at: ts,
            })
            .await
        {
            Ok(ControlResponse::Ok) => {
                state
                    .audit(
                        uid,
                        "control:ap-create",
                        Some(&body.bucket),
                        Some(&body.name),
                        Some(&format!(
                            "policy={}",
                            if policy_json.is_some() { "set" } else { "none" }
                        )),
                    )
                    .await;
                (
                    StatusCode::CREATED,
                    Json(serde_json::json!({"name": body.name})),
                )
                    .into_response()
            }
            Ok(resp) => control_rejection(resp, "AP 已存在"),
            Err(e) => control_failure(e),
        };
    }
    match state
        .db
        .create_access_point(&body.name, &body.bucket, uid, policy_json.as_deref(), ts)
        .await
    {
        Ok(()) => {
            state
                .audit(
                    uid,
                    "control:ap-create",
                    Some(&body.bucket),
                    Some(&body.name),
                    Some(&format!(
                        "policy={}",
                        if policy_json.is_some() { "set" } else { "none" }
                    )),
                )
                .await;
            if let Ok(ap) = AccessPoint::from_row(
                body.name.clone(),
                body.bucket.clone(),
                uid,
                policy_json,
                &state.ap_region,
            ) {
                state.ap_cache.put(ap);
            }
            (
                StatusCode::CREATED,
                Json(serde_json::json!({"name": body.name})),
            )
                .into_response()
        }
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            (StatusCode::CONFLICT, "AP 已存在").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "建 AP 失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

#[allow(clippy::result_large_err)]
async fn owned(
    state: &AppState,
    payload: &JwtPayload,
    name: &str,
) -> Result<(crate::db::AccessPointRow, i64), Response> {
    let (uid, admin) = who(state, payload).await?;
    let Some(row) = (match state.db.get_access_point(name).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "查 AP 失败");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response());
        }
    }) else {
        return Err((StatusCode::NOT_FOUND, "AP 不存在").into_response());
    };
    let is_owner = state
        .acl()
        .is_owner(&row.bucket, uid)
        .await
        .unwrap_or(false);
    if !admin && !is_owner {
        return Err((StatusCode::FORBIDDEN, "需要桶主或管理员").into_response());
    }
    Ok((row, uid))
}

async fn get_one(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(name): Path<String>,
) -> Response {
    match owned(&state, &payload, &name).await {
        Ok((row, _)) => Json(row_json(&row)).into_response(),
        Err(r) => r,
    }
}

async fn delete_one(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(name): Path<String>,
) -> Response {
    let (row, uid) = match owned(&state, &payload, &name).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    // M9 S6d：apply 级联删 OLAP；缓存由 SideEffect::{ApRemove,OlapRemove} 清理。
    if state.control.is_raft() {
        return match state
            .control
            .write(ControlCommand::AccessPointDelete { name: name.clone() })
            .await
        {
            Ok(ControlResponse::Ok) => {
                state
                    .audit(
                        uid,
                        "control:ap-delete",
                        Some(&row.bucket),
                        Some(&row.name),
                        None,
                    )
                    .await;
                StatusCode::NO_CONTENT.into_response()
            }
            Ok(resp) => control_rejection(resp, "AP 删除冲突"),
            Err(e) => control_failure(e),
        };
    }
    match state.db.delete_access_point(&name).await {
        Ok(true) => {
            state.ap_cache.remove(&name);
            state.olap_cache.remove(&name);
            state
                .audit(
                    uid,
                    "control:ap-delete",
                    Some(&row.bucket),
                    Some(&row.name),
                    None,
                )
                .await;
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => (StatusCode::NOT_FOUND, "AP 不存在").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "删 AP 失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

async fn put_policy(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(name): Path<String>,
    Json(policy): Json<serde_json::Value>,
) -> Response {
    let (row, uid) = match owned(&state, &payload, &name).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let policy_json = if policy.is_null() {
        None
    } else {
        let raw = policy.to_string();
        match AccessPoint::from_row(
            row.name.clone(),
            row.bucket.clone(),
            row.owner_id,
            Some(raw.clone()),
            &state.ap_region,
        ) {
            Ok(_) => Some(raw),
            Err(e) => return bad(&e),
        }
    };
    // M9 S6d：Raft 一条命令；缓存由 SideEffect::ApReload 刷新。
    if state.control.is_raft() {
        return match state
            .control
            .write(ControlCommand::AccessPointSetPolicy {
                name: name.clone(),
                policy_json: policy_json.clone(),
                updated_at: now(),
            })
            .await
        {
            Ok(ControlResponse::Ok) => {
                state
                    .audit(
                        uid,
                        "control:ap-policy",
                        Some(&row.bucket),
                        Some(&row.name),
                        Some(if policy_json.is_some() {
                            "policy=set"
                        } else {
                            "policy=clear"
                        }),
                    )
                    .await;
                StatusCode::NO_CONTENT.into_response()
            }
            Ok(resp) => control_rejection(resp, "AP 策略写入冲突"),
            Err(e) => control_failure(e),
        };
    }
    match state
        .db
        .set_access_point_policy(&name, policy_json.as_deref(), now())
        .await
    {
        Ok(true) => {
            state
                .audit(
                    uid,
                    "control:ap-policy",
                    Some(&row.bucket),
                    Some(&row.name),
                    Some(if policy_json.is_some() {
                        "policy=set"
                    } else {
                        "policy=clear"
                    }),
                )
                .await;
            if let Ok(ap) = AccessPoint::from_row(
                row.name,
                row.bucket,
                row.owner_id,
                policy_json,
                &state.ap_region,
            ) {
                state.ap_cache.put(ap);
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => (StatusCode::NOT_FOUND, "AP 不存在").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "更新 AP 策略失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

/// M8 S2：变换器配置（module 以标准 base64 传入；响应不含模块字节，只回摘要/大小）。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutTransform {
    kind: String,
    endpoint: Option<String>,
    module_b64: Option<String>,
    timeout_ms: Option<i64>,
    max_response_bytes: Option<i64>,
}

async fn get_transform(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(name): Path<String>,
) -> Response {
    if let Err(r) = owned(&state, &payload, &name).await {
        return r;
    }
    match state.db.get_olap_config(&name).await {
        Ok(Some(c)) => Json(serde_json::json!({
            "kind": c.kind,
            "endpoint": c.endpoint,
            "module_sha256": c.module_sha256,
            "module_size": c.module.as_ref().map(Vec::len),
            "timeout_ms": c.timeout_ms,
            "max_response_bytes": c.max_response_bytes,
            "updated_at": c.updated_at,
        }))
        .into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "未配置变换器").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查变换器配置失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

async fn put_transform(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(name): Path<String>,
    Json(body): Json<PutTransform>,
) -> Response {
    let (row, uid) = match owned(&state, &payload, &name).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    match body.kind.as_str() {
        "http" if body.module_b64.is_some() => {
            return bad("kind=http 不接受 module_b64");
        }
        "http" if body.endpoint.is_none() => return bad("kind=http 必须提供 endpoint"),
        "wasm" if body.endpoint.is_some() => return bad("kind=wasm 不接受 endpoint"),
        "wasm" if body.module_b64.is_none() => return bad("kind=wasm 必须提供 module_b64"),
        "http" | "wasm" => {}
        _ => return bad("kind 必须是 http/wasm"),
    }
    let module = match body.module_b64.as_deref() {
        Some(b64) => match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(m) => Some(m),
            Err(_) => return bad("module_b64 不是合法 base64"),
        },
        None => None,
    };
    if let Some(m) = &module
        && m.len() > xl_s3::olap::MAX_MODULE_BYTES
    {
        return bad("wasm 模块超过 16 MiB");
    }
    let timeout_ms = body.timeout_ms.unwrap_or(5000);
    let max_response_bytes = body
        .max_response_bytes
        .unwrap_or(xl_s3::olap::DEFAULT_MAX_RESPONSE_BYTES);
    // 保存即编译（WASM）并校验 URL 形状/限额；失败 400。
    let transform = match OlapTransform::prepare(
        &body.kind,
        body.endpoint.clone(),
        module.as_deref(),
        timeout_ms,
        max_response_bytes,
    ) {
        Ok(t) => t,
        Err(e) => return bad(&e),
    };
    let sha = module.as_ref().map(|m| hex::encode(Sha256::digest(m)));
    // M9 S6d：Raft 一条命令（module base64 + sha 已在 append 前算好；apply 只解码写行）。
    // 缓存由 SideEffect::OlapReload 重新预加载。
    if state.control.is_raft() {
        return match state
            .control
            .write(ControlCommand::OlapPut {
                access_point: name.clone(),
                kind: body.kind.clone(),
                endpoint: body.endpoint.clone(),
                module: body.module_b64.clone(),
                module_sha256: sha,
                timeout_ms,
                max_response_bytes,
                updated_at: now(),
            })
            .await
        {
            Ok(ControlResponse::Ok) => {
                state
                    .audit(
                        uid,
                        "control:ap-transform",
                        Some(&row.bucket),
                        Some(&row.name),
                        Some(&format!("kind={}", body.kind)),
                    )
                    .await;
                StatusCode::NO_CONTENT.into_response()
            }
            Ok(resp) => control_rejection(resp, "变换器写入冲突"),
            Err(e) => control_failure(e),
        };
    }
    match state
        .db
        .set_olap_config(
            &name,
            &body.kind,
            body.endpoint.as_deref(),
            module.as_deref(),
            sha.as_deref(),
            timeout_ms,
            max_response_bytes,
            now(),
        )
        .await
    {
        Ok(()) => {
            state
                .audit(
                    uid,
                    "control:ap-transform",
                    Some(&row.bucket),
                    Some(&row.name),
                    Some(&format!("kind={}", body.kind)),
                )
                .await;
            state.olap_cache.put(&name, &row.bucket, transform);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "保存变换器配置失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

/// 启动预加载：DB 行 → 编译产物入缓存。坏配置存 broken 占位（GET 500，fail-closed）。
pub async fn preload_olap(db: &Db, ap_cache: &xl_s3::ap::ApCache, cache: &xl_s3::olap::OlapCache) {
    let rows = match db.list_olap_configs().await {
        Ok(rows) => rows,
        Err(e) => {
            // 无法得知哪些 AP 有变换器：保持未加载，AP 请求 500（不得静默直通原文）。
            tracing::error!(error = %e, "变换器预加载失败（AP 请求将 500，直到重载成功）");
            cache.mark_failed();
            return;
        }
    };
    for r in rows {
        // AP 预加载可能失败/未含此 AP；直接回查 DB 拿桶名，不能因此跳过变换器。
        let bucket = match ap_cache.get(&r.access_point) {
            Some(ap) => ap.bucket,
            None => match db.get_access_point(&r.access_point).await {
                Ok(Some(row)) => row.bucket,
                Ok(None) => {
                    tracing::error!(ap = %r.access_point, "变换器预加载失败：AP 不存在");
                    cache.mark_failed();
                    return;
                }
                Err(e) => {
                    tracing::error!(error = %e, ap = %r.access_point, "变换器预加载回查 AP 失败");
                    cache.mark_failed();
                    return;
                }
            },
        };
        match OlapTransform::prepare(
            &r.kind,
            r.endpoint.clone(),
            r.module.as_deref(),
            r.timeout_ms,
            r.max_response_bytes,
        ) {
            Ok(t) => cache.put(&r.access_point, &bucket, t),
            Err(e) => {
                tracing::error!(error = %e, ap = %r.access_point, "变换器预加载失败（GET 将 500）");
                cache.put(&r.access_point, &bucket, OlapTransform::broken(e));
            }
        }
    }
    cache.mark_loaded();
}

/// `ApSource` 的 DB 实现（缓存未命中时查询；非法策略 → Err → 路由 500 fail-closed）。
pub struct DbApSource {
    pub db: Arc<Db>,
    pub region: String,
}

#[async_trait::async_trait]
impl ApSource for DbApSource {
    async fn lookup(&self, name: &str) -> Result<Option<AccessPoint>, String> {
        let row = self
            .db
            .get_access_point(name)
            .await
            .map_err(|e| e.to_string())?;
        row.map(|r| {
            AccessPoint::from_row(r.name, r.bucket, r.owner_id, r.policy_json, &self.region)
        })
        .transpose()
    }
}
