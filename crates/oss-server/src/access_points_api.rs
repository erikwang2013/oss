//! M8 S1：访问点控制面（AP 来源实现 + CRUD API）。

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use jwt_rust::JwtPayload;
use serde::Deserialize;

use crate::db::Db;
use crate::state::AppState;
use xl_s3::ap::{AccessPoint, ApSource};

/// `/api/v1/access-points`（挂 console nest 下；JWT 已由外层 JwtLayer 校验）。
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/access-points", get(list).post(create))
        .route("/access-points/{name}", get(get_one).delete(delete_one))
        .route("/access-points/{name}/policy", put(put_policy))
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

fn valid_ap_name(name: &str) -> bool {
    (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
}

fn row_json(r: &crate::db::AccessPointRow) -> serde_json::Value {
    serde_json::json!({
        "name": r.name, "bucket": r.bucket, "owner_id": r.owner_id,
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
    match state
        .db
        .create_access_point(&body.name, &body.bucket, uid, policy_json.as_deref(), ts)
        .await
    {
        Ok(()) => {
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
) -> Result<crate::db::AccessPointRow, Response> {
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
    Ok(row)
}

async fn get_one(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(name): Path<String>,
) -> Response {
    match owned(&state, &payload, &name).await {
        Ok(row) => Json(row_json(&row)).into_response(),
        Err(r) => r,
    }
}

async fn delete_one(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(name): Path<String>,
) -> Response {
    if let Err(r) = owned(&state, &payload, &name).await {
        return r;
    }
    match state.db.delete_access_point(&name).await {
        Ok(true) => {
            state.ap_cache.remove(&name);
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
    let row = match owned(&state, &payload, &name).await {
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
    match state
        .db
        .set_access_point_policy(&name, policy_json.as_deref(), now())
        .await
    {
        Ok(true) => {
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
