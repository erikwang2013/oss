//! M8 S3：Batch 创建的控制台面（`POST /api/v1/jobs`，kind=batch）。
//!
//! 创建期按创建者身份逐行授权，并把清单内容快照为 `job_tasks`（之后改清单不影响作业）。
//! 动作执行在 [`crate::job_actions`]。

use std::collections::{BTreeMap, HashSet};

use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use jwt_rust::JwtPayload;
use serde::{Deserialize, Serialize};
use xl_s3::access::AuthedUser;
use xl_store::meta::{self, ObjectTag};

use crate::db::{JobSpec, NewJobTask};
use crate::job_actions::{
    BatchActionPayload, BatchError, BatchOp, MAX_MANIFEST_BYTES, parse_manifest,
};
use crate::state::AppState;

/// `POST /api/v1/jobs` 请求体（kind 缺省 = batch）。
#[derive(Debug, Deserialize, Serialize)]
pub struct BatchCreateRequest {
    #[serde(default)]
    pub kind: Option<String>,
    pub manifest_bucket: String,
    pub manifest_key: String,
    pub operation: String,
    #[serde(default)]
    pub target: Option<BatchTarget>,
    #[serde(default)]
    pub tags: Option<TagsBody>,
    #[serde(default)]
    pub restore: Option<RestoreBody>,
    #[serde(default)]
    pub transform_access_point: Option<String>,
    #[serde(default)]
    pub report_bucket: Option<String>,
    #[serde(default)]
    pub report_prefix: Option<String>,
    #[serde(default)]
    pub priority: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BatchTarget {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
}

/// 标签两种写法都收：对象映射 `{"k":"v"}` 或 `[{"key","value"}]`。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TagsBody {
    Map(BTreeMap<String, String>),
    List(Vec<TagPair>),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TagPair {
    pub key: String,
    pub value: String,
}

impl TagsBody {
    fn into_tags(self) -> Vec<ObjectTag> {
        match self {
            TagsBody::Map(m) => m
                .into_iter()
                .map(|(key, value)| ObjectTag { key, value })
                .collect(),
            TagsBody::List(v) => v
                .into_iter()
                .map(|t| ObjectTag {
                    key: t.key,
                    value: t.value,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RestoreBody {
    #[serde(default = "one_day")]
    pub days: u32,
}

fn one_day() -> u32 {
    1
}

pub struct BatchCreated {
    pub job_id: i64,
    pub total: usize,
}

fn invalid(msg: impl Into<String>) -> BatchError {
    BatchError::Invalid(msg.into())
}

fn quota_limits() -> (i64, i64) {
    let get = |k: &str, d: i64| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    (
        get("OSS_JOB_MAX_INFLIGHT_PER_USER", 100),
        get("OSS_JOB_MAX_INFLIGHT_GLOBAL", 1000),
    )
}

/// 创建者身份判权（`check_op` 融表三档 + 桶策略 + 会话策略；copy 时源轴独立）。
async fn check(
    state: &AppState,
    user: &AuthedUser,
    op: &str,
    bucket: &str,
    key: &str,
    copy_src: Option<(&str, &str)>,
) -> Result<(), BatchError> {
    match state
        .acl()
        .check_op(user, "", op, Some(bucket), Some(key), copy_src)
        .await
    {
        Ok((true, _)) => Ok(()),
        Ok((false, detail)) => Err(BatchError::Denied(
            detail.unwrap_or_else(|| format!("无权 {op} {bucket}/{key}")),
        )),
        Err(e) => Err(BatchError::Internal(format!("判权失败: {e}"))),
    }
}

/// 校验并快照清单 → 建 `kind=batch` 作业。所有授权在落库前完成。
pub async fn create_batch(
    state: &AppState,
    uid: i64,
    is_admin: bool,
    req: BatchCreateRequest,
) -> Result<BatchCreated, BatchError> {
    if req.kind.as_deref().is_some_and(|k| k != "batch") {
        return Err(invalid("POST /jobs 仅支持 kind=batch"));
    }
    let op = BatchOp::parse(&req.operation)
        .ok_or_else(|| invalid(format!("未知 operation: {}", req.operation)))?;
    xl_store::layout::validate_bucket(&req.manifest_bucket)
        .map_err(|e| invalid(format!("manifest_bucket: {e}")))?;
    xl_store::layout::validate_key(&req.manifest_key)
        .map_err(|e| invalid(format!("manifest_key: {e}")))?;

    let tags = match op {
        BatchOp::ReplaceTags => {
            let tags = req
                .tags
                .clone()
                .ok_or_else(|| invalid("ReplaceTags 必须提供 tags"))?
                .into_tags();
            meta::validate_tags(&tags).map_err(|e| invalid(format!("tags: {e}")))?;
            tags
        }
        _ => Vec::new(),
    };
    let restore_days = match op {
        BatchOp::Restore => {
            let days = req
                .restore
                .as_ref()
                .ok_or_else(|| invalid("RestoreObject 必须提供 restore.days"))?
                .days;
            if !(1..=365).contains(&days) {
                return Err(invalid("restore.days 必须在 1..=365"));
            }
            Some(days)
        }
        _ => None,
    };
    let target = match op {
        BatchOp::Copy => {
            let t = req
                .target
                .as_ref()
                .ok_or_else(|| invalid("CopyObject 必须提供 target.bucket"))?;
            xl_store::layout::validate_bucket(&t.bucket)
                .map_err(|e| invalid(format!("target.bucket: {e}")))?;
            Some(t)
        }
        _ => None, // 其余动作作用于清单对象自身
    };
    if let Some(b) = &req.report_bucket {
        xl_store::layout::validate_bucket(b).map_err(|e| invalid(format!("report_bucket: {e}")))?;
    }
    match (op, &req.transform_access_point) {
        (BatchOp::Invoke, None) => {
            return Err(invalid("InvokeLambda 必须提供 transform_access_point"));
        }
        (BatchOp::Invoke, Some(ap)) => {
            let Some(row) = state
                .db
                .get_access_point(ap)
                .await
                .map_err(|e| BatchError::Internal(e.to_string()))?
            else {
                return Err(BatchError::NotFound(format!("access point {ap} 不存在")));
            };
            if !is_admin && row.owner_id != uid {
                return Err(BatchError::Denied(
                    "transform access point 需 owner/admin".into(),
                ));
            }
            let cfg = state
                .db
                .get_olap_config(ap)
                .await
                .map_err(|e| BatchError::Internal(e.to_string()))?
                .ok_or_else(|| invalid("transform access point 未配置变换器"))?;
            if cfg.kind != "http" {
                return Err(invalid("仅支持 http 变换器（WASM 不支持）"));
            }
        }
        (_, Some(_)) => return Err(invalid("transform_access_point 仅用于 InvokeLambda")),
        _ => {}
    }

    // 配额：控制台建作业前挡（queued/running/paused 计数；非原子但足够防呆）。
    let (per_user, global) = quota_limits();
    let (u, g) = state
        .db
        .job_inflight_counts(Some(uid))
        .await
        .map_err(|e| BatchError::Internal(e.to_string()))?;
    if u >= per_user || g >= global {
        return Err(BatchError::Quota(format!(
            "in-flight 作业超配额（用户 {u}/{per_user}，全局 {g}/{global}）"
        )));
    }

    let user = AuthedUser {
        uid,
        is_admin,
        ak: String::new(),
    };
    check(
        state,
        &user,
        "GetObject",
        &req.manifest_bucket,
        &req.manifest_key,
        None,
    )
    .await?;
    let (_, bytes) = state
        .store
        .get_object_ext(&req.manifest_bucket, &req.manifest_key, None)
        .await
        .map_err(|e| match e {
            xl_store::error::Error::NoSuchKey(_) => BatchError::NotFound("清单对象不存在".into()),
            other => invalid(format!("清单读取失败: {other}")),
        })?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(invalid(format!(
            "清单超过 {} MiB 上限",
            MAX_MANIFEST_BYTES >> 20
        )));
    }
    let rows = parse_manifest(&bytes)?;

    // 逐行创建期授权（同桶/键去重——1000 行同桶时省掉重复 SQL）。
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for r in &rows {
        let dst_key = match op {
            BatchOp::Copy => {
                let key = format!(
                    "{}{}",
                    target.map(|t| t.prefix.as_str()).unwrap_or(""),
                    r.key
                );
                xl_store::layout::validate_key(&key)
                    .map_err(|e| invalid(format!("目标键非法（{}）: {e}", r.key)))?;
                key
            }
            _ => r.key.clone(),
        };
        if !seen.insert((r.bucket.clone(), r.key.clone())) {
            continue;
        }
        match op {
            BatchOp::Copy => {
                let t = target.expect("Copy 已在上面校验");
                check(
                    state,
                    &user,
                    "CopyObject",
                    &t.bucket,
                    &dst_key,
                    Some((&r.bucket, &r.key)),
                )
                .await?
            }
            BatchOp::Delete => check(state, &user, "DeleteObject", &r.bucket, &r.key, None).await?,
            BatchOp::ReplaceTags => {
                check(state, &user, "PutObjectTagging", &r.bucket, &r.key, None).await?
            }
            BatchOp::Restore => {
                check(state, &user, "RestoreObject", &r.bucket, &r.key, None).await?
            }
            BatchOp::Invoke => check(state, &user, "GetObject", &r.bucket, &r.key, None).await?,
        }
    }
    if let Some(b) = &req.report_bucket {
        let key = format!("{}/results.csv", req.report_prefix.as_deref().unwrap_or(""));
        check(
            state,
            &user,
            "PutObject",
            b,
            key.trim_start_matches('/'),
            None,
        )
        .await?;
    }

    let mut tasks = Vec::with_capacity(rows.len());
    for r in rows {
        let payload = BatchActionPayload {
            bucket: r.bucket,
            key: r.key,
            version_id: r.version_id,
            uid,
            target_bucket: target.map(|t| t.bucket.clone()),
            target_prefix: target.map(|t| t.prefix.clone()),
            tags: tags.clone(),
            restore_days,
            transform_access_point: req.transform_access_point.clone(),
        };
        tasks.push(NewJobTask {
            action: op.action().to_string(),
            payload: serde_json::to_string(&payload)
                .map_err(|e| BatchError::Internal(e.to_string()))?,
            dedupe_key: None,
        });
    }
    let total = tasks.len();
    let spec = JobSpec {
        kind: "batch".to_string(),
        bucket: req.manifest_bucket.clone(),
        prefix: req.manifest_key.clone(),
        requested_by: Some(uid),
        priority: req.priority,
        parent_job_id: None,
        report_bucket: req.report_bucket.clone(),
        report_prefix: req.report_prefix.clone(),
        spec_json: Some(
            serde_json::to_string(&req).map_err(|e| BatchError::Internal(e.to_string()))?,
        ),
        tasks,
    };
    let job_id = state
        .db
        .create_job(&spec, crate::db::now())
        .await
        .map_err(|e| BatchError::Internal(e.to_string()))?;
    Ok(BatchCreated { job_id, total })
}

/// 控制台路由（与 `GET /jobs` 合并到同一路径）。
pub fn routes() -> Router<AppState> {
    Router::new().route("/jobs", post(create))
}

async fn create(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Json(req): Json<BatchCreateRequest>,
) -> Response {
    let (uid, is_admin) = match crate::console::who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match create_batch(&state, uid, is_admin, req).await {
        Ok(c) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"job_id": c.job_id, "total": c.total})),
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}
