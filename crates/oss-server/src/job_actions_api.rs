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
    BatchActionPayload, BatchError, BatchOp, MAX_MANIFEST_BYTES, check_invoke_kind, parse_manifest,
};
use crate::state::AppState;

/// M-2：`priority` 完全由用户控制。限定小范围，避免用极端值（如 `i64::MAX`）在
/// 协作式抢占中跨请求者压制其他租户的作业。
const PRIORITY_MIN: i64 = -100;
const PRIORITY_MAX: i64 = 100;

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

/// 清单桶跨账户门（账户 = 用户命名空间 v1）：非管理员只能使用**本账户**的清单桶。
/// 账户由桶主派生（无主/管理员桶 = 系统账户，见 `xl_s3::access::bucket_account_id`）；
/// admin 可用跨账户清单。读权限仍由随后的 `GetObject` 判权兜底（缺失/撤销 → 403）。
async fn check_manifest_account(
    state: &AppState,
    uid: i64,
    is_admin: bool,
    bucket: &str,
) -> Result<(), BatchError> {
    if is_admin {
        return Ok(());
    }
    let account = state
        .db
        .bucket_account(bucket)
        .await
        .map_err(|e| BatchError::Internal(format!("清单桶归属查询失败: {e}")))?;
    // uid 无法派生账户（越界/0）→ fail-closed，不得与系统账户同名放行。
    let own = xl_s3::access::account_id_of(uid);
    if own.as_deref() != Some(account.as_str()) {
        return Err(BatchError::Denied(format!(
            "清单桶 {bucket} 属于账户 {account}，跨账户清单仅管理员可用"
        )));
    }
    Ok(())
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
    if !(PRIORITY_MIN..=PRIORITY_MAX).contains(&req.priority) {
        return Err(invalid(format!(
            "priority 必须在 {PRIORITY_MIN}..={PRIORITY_MAX}"
        )));
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
            check_invoke_kind(&cfg.kind).map_err(BatchError::Invalid)?;
        }
        (_, Some(_)) => return Err(invalid("transform_access_point 仅用于 InvokeLambda")),
        _ => {}
    }

    // 配额：控制台建作业前挡（queued/running/paused/preempted 计数——预占仍是 inflight；
    // 非原子但足够防呆）。
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
    // 跨账户清单门必须在读取对象之前判定（避免为无权重账户读内容/探测存在性）。
    check_manifest_account(state, uid, is_admin, &req.manifest_bucket).await?;
    check(
        state,
        &user,
        "GetObject",
        &req.manifest_bucket,
        &req.manifest_key,
        None,
    )
    .await?;
    let head = state
        .store
        .head_object(&req.manifest_bucket, &req.manifest_key)
        .await
        .map_err(|e| match e {
            xl_store::error::Error::NoSuchKey(_) => BatchError::NotFound("清单对象不存在".into()),
            other => invalid(format!("清单读取失败: {other}")),
        })?;
    // 先按 HEAD 大小挡 16 MiB，避免为超限清单分配整块内存；读入后再兜一次。
    if head.size > MAX_MANIFEST_BYTES as u64 {
        return Err(invalid(format!(
            "清单超过 {} MiB 上限",
            MAX_MANIFEST_BYTES >> 20
        )));
    }
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
        .jobs
        .create_job(&state.db, &spec, crate::db::now())
        .await
        .map_err(|e| BatchError::Internal(e.to_string()))?;
    state
        .audit(
            uid,
            "control:job-create",
            Some(&req.manifest_bucket),
            Some(&req.manifest_key),
            Some(&format!("job={job_id};tasks={total}")),
        )
        .await;
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
