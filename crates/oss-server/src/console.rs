//! 控制台 API。M1 只有：登录、桶列表、对象列表、下载、删除、上传记录、访问密钥。
//! 授权模型（谁能做什么）在 M4；M1 只区分「登录 / 未登录」。

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
}

/// 控制台与 aether 鉴权层（`router.rs`）共用的 token 载荷。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Claims {
    sub: String,
    exp: usize,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/login", post(login))
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
}

async fn login(State(state): State<AppState>, Json(body): Json<LoginRequest>) -> impl IntoResponse {
    match state.db.verify_user(&body.username, &body.password).await {
        Ok(Some(uid)) => {
            let exp = now_secs() as usize + 12 * 3600;
            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &Claims {
                    sub: uid.to_string(),
                    exp,
                },
                &jsonwebtoken::EncodingKey::from_secret(state.cfg.jwt_secret.as_bytes()),
            );
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

/// 从 Authorization: Bearer 解出用户 id。M1 的最小鉴权。
/// ponytail: 12 行自写中间件；M4 引入策略模型时换 ecat-auth 的 JwtAuthLayer。
fn auth(state: &AppState, headers: &HeaderMap) -> Result<i64, StatusCode> {
    let raw = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let decoded = jsonwebtoken::decode::<Claims>(
        raw,
        &jsonwebtoken::DecodingKey::from_secret(state.cfg.jwt_secret.as_bytes()),
        &jsonwebtoken::Validation::default(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?;

    decoded
        .claims
        .sub
        .parse::<i64>()
        .map_err(|_| StatusCode::UNAUTHORIZED)
}

/// 上传分组（oss.toml 的 `[[groups]]`），供上传面板的分组下拉使用。
async fn list_groups(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    Json(state.cfg.groups.clone()).into_response()
}

async fn list_buckets(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.store.list_buckets().await {
        Ok(list) => Json(list.into_iter().map(|b| b.name).collect::<Vec<_>>()).into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

#[derive(Deserialize)]
struct CreateBucketBody {
    name: String,
}

async fn create_bucket(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateBucketBody>,
) -> impl IntoResponse {
    let uid = match auth(&state, &headers) {
        Ok(u) => u,
        Err(s) => return s.into_response(),
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

async fn delete_bucket(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.store.delete_bucket(&bucket).await {
        Ok(()) => {
            if let Err(e) = state.db.delete_bucket_row(&bucket).await {
                // 桶已删除：控制面行残留只影响列表展示，不失败请求
                tracing::warn!(error = %e, "清理桶行失败");
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
    headers: HeaderMap,
    Path(bucket): Path<String>,
    axum::extract::Query(q): axum::extract::Query<ListQuery>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
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
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
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
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.store.delete_object(&bucket, &key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => store_error(e).into_response(),
    }
}

async fn list_uploads(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) {
        return s.into_response();
    }
    match state.db.list_uploads(100).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询上传记录失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
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
    headers: HeaderMap,
    Json(body): Json<RegisterBody>,
) -> impl IntoResponse {
    let uid = match auth(&state, &headers) {
        Ok(u) => u,
        Err(s) => return s.into_response(),
    };
    match state
        .db
        .register_upload(&body.saved_path, &body.original_name, Some(uid))
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        // DB 故障是服务端故障（与 store_error 的 5xx 分档一致）
        Err(e) => {
            tracing::error!(error = %e, "登记上传记录失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

async fn list_access_keys(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let uid = match auth(&state, &headers) {
        Ok(u) => u,
        Err(s) => return s.into_response(),
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
    headers: HeaderMap,
    Json(body): Json<CreateKeyBody>,
) -> impl IntoResponse {
    let uid = match auth(&state, &headers) {
        Ok(u) => u,
        Err(s) => return s.into_response(),
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
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
