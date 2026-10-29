//! M8 S5：Inventory 配置的控制台薄封装（owner/admin）。
//!
//! S3 与 console 共用 `InventoryStore`，校验只在 store 的 `put_config` 做一次。

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use jwt_rust::JwtPayload;
use s3s::dto::{
    InventoryConfiguration, InventoryDestination, InventoryFilter, InventoryFormat,
    InventoryFrequency, InventoryIncludedObjectVersions, InventoryOptionalField,
    InventoryS3BucketDestination, InventorySchedule,
};
use serde::{Deserialize, Serialize};
use xl_s3::access::AuthedUser;
use xl_s3::inventory::InventoryError;

use crate::console::{require_owner_or_admin, who};
use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/buckets/{bucket}/inventory", get(list).put(upsert))
        .route("/buckets/{bucket}/inventory/{id}", delete(remove))
        .route("/buckets/{bucket}/inventory/{id}/run", post(run_now))
}

#[derive(Debug, Deserialize)]
struct ConfigBody {
    id: String,
    #[serde(default = "yes")]
    is_enabled: bool,
    frequency: String,
    destination_bucket: String,
    /// 3.7：目标桶账户（可选；须等于目标桶当前账户）。跨账户目标仅管理员可配置。
    #[serde(default)]
    destination_account_id: Option<String>,
    #[serde(default)]
    destination_prefix: Option<String>,
    #[serde(default = "csv")]
    format: String,
    #[serde(default)]
    filter_prefix: Option<String>,
    #[serde(default = "current")]
    included_object_versions: String,
    #[serde(default)]
    optional_fields: Vec<String>,
    /// 3.6：增量模式（非 S3 标准；默认 false = 全量快照）。
    #[serde(default)]
    incremental: bool,
}

fn yes() -> bool {
    true
}
fn csv() -> String {
    "CSV".to_string()
}
fn current() -> String {
    "Current".to_string()
}

impl ConfigBody {
    fn into_config(self) -> InventoryConfiguration {
        let prefix = self.destination_prefix.filter(|p| !p.is_empty());
        let filter = self
            .filter_prefix
            .filter(|p| !p.is_empty())
            .map(|prefix| InventoryFilter { prefix });
        InventoryConfiguration {
            destination: InventoryDestination {
                s3_bucket_destination: InventoryS3BucketDestination {
                    account_id: self.destination_account_id.filter(|a| !a.is_empty()),
                    bucket: self.destination_bucket,
                    encryption: None,
                    format: InventoryFormat::from(self.format),
                    prefix,
                },
            },
            filter,
            id: self.id,
            included_object_versions: InventoryIncludedObjectVersions::from(
                self.included_object_versions,
            ),
            is_enabled: self.is_enabled,
            optional_fields: (!self.optional_fields.is_empty()).then(|| {
                self.optional_fields
                    .into_iter()
                    .map(InventoryOptionalField::from)
                    .collect()
            }),
            schedule: InventorySchedule {
                frequency: InventoryFrequency::from(self.frequency),
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct ConfigView {
    id: String,
    is_enabled: bool,
    frequency: String,
    destination_bucket: String,
    destination_account_id: Option<String>,
    destination_prefix: Option<String>,
    source_account_id: Option<String>,
    format: String,
    filter_prefix: Option<String>,
    included_object_versions: String,
    optional_fields: Vec<String>,
    incremental: bool,
}

impl ConfigView {
    fn new(
        c: &InventoryConfiguration,
        incremental: bool,
        source_account_id: Option<String>,
    ) -> Self {
        let d = &c.destination.s3_bucket_destination;
        Self {
            id: c.id.clone(),
            is_enabled: c.is_enabled,
            frequency: c.schedule.frequency.as_str().to_string(),
            destination_bucket: d.bucket.clone(),
            destination_account_id: d.account_id.clone(),
            destination_prefix: d.prefix.clone(),
            source_account_id,
            format: d.format.as_str().to_string(),
            filter_prefix: c.filter.as_ref().map(|f| f.prefix.clone()),
            included_object_versions: c.included_object_versions.as_str().to_string(),
            optional_fields: c
                .optional_fields
                .iter()
                .flatten()
                .map(|f| f.as_str().to_string())
                .collect(),
            incremental,
        }
    }
}

fn err(e: InventoryError) -> Response {
    match e {
        InventoryError::NoSuchBucket => (StatusCode::NOT_FOUND, "NoSuchBucket").into_response(),
        InventoryError::NoSuchConfiguration => {
            (StatusCode::NOT_FOUND, "NoSuchConfiguration").into_response()
        }
        InventoryError::InvalidArgument(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        InventoryError::AccessDenied(m) => (StatusCode::FORBIDDEN, m).into_response(),
        InventoryError::NotImplemented(m) => (StatusCode::NOT_IMPLEMENTED, m).into_response(),
        InventoryError::Internal(m) => {
            tracing::error!(error = %m, "inventory 配置读写失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

async fn list(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    match state.inventory.list_configs(&bucket).await {
        Ok(list) => {
            // 账户 = 用户命名空间 v1；配置期已记录，展示当前桶账户（属主变更由运行期复核兜底）。
            let source_account = state.db.bucket_account(&bucket).await.ok();
            let mut views = Vec::with_capacity(list.len());
            for c in &list {
                // feature 5 起开关/水位随 Raft 复制；低级别仍是节点本地行（读本地即可）。
                let incremental = state
                    .db
                    .inventory_incremental_get(&bucket, &c.id)
                    .await
                    .unwrap_or(false);
                views.push(ConfigView::new(c, incremental, source_account.clone()));
            }
            Json(views).into_response()
        }
        Err(e) => err(e),
    }
}

async fn upsert(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(bucket): Path<String>,
    Json(body): Json<ConfigBody>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let owner = AuthedUser {
        uid,
        is_admin,
        ak: String::new(),
    };
    let incremental = body.incremental;
    let id = body.id.clone();
    // 复制模式下开关只能由 leader 生成；先预检，避免配置已提交而开关失败（P2-1）。
    if incremental && let Err(e) = state.jobs.inventory_incremental_preflight().await {
        let status = if e.contains("not leader") {
            StatusCode::CONFLICT
        } else if e.contains("feature") {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        return (status, e).into_response();
    }
    match state
        .inventory
        .put_config(&bucket, &owner, &body.into_config())
        .await
    {
        // InventoryPut apply 已复位游标与开关；incremental=false 无需再写。
        Ok(()) if !incremental => StatusCode::NO_CONTENT.into_response(),
        Ok(()) => {
            // put_config 复位后，console 请求按请求值重新开启。
            match state
                .jobs
                .inventory_incremental_set(&state.db, &bucket, &id, true, crate::db::now())
                .await
            {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(e) => {
                    tracing::error!(error = %e, "inventory 增量模式写入失败，回滚开关");
                    let _ = state
                        .jobs
                        .inventory_incremental_set(&state.db, &bucket, &id, false, crate::db::now())
                        .await;
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "config saved but incremental mode failed; retry the update",
                    )
                        .into_response()
                }
            }
        }
        Err(e) => err(e),
    }
}

async fn remove(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path((bucket, id)): Path<(String, String)>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    match state.inventory.delete_config(&bucket, &id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err(e),
    }
}

async fn run_now(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path((bucket, id)): Path<(String, String)>,
) -> Response {
    if let Err(r) = require_owner_or_admin(&state, &payload, &bucket).await {
        return r;
    }
    let (uid, _) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match state.inventory.get_config(&bucket, &id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "NoSuchConfiguration").into_response(),
        Err(e) => return err(e),
    }
    let now = crate::db::now();
    let run_key = format!("manual-{now}");
    match crate::inventory::enqueue_run_with(
        &state.jobs,
        &state.db,
        &bucket,
        &id,
        &run_key,
        Some(uid),
        now,
    )
    .await
    {
        Ok(Some(job_id)) => {
            state
                .audit(
                    uid,
                    "control:inventory-run",
                    Some(&bucket),
                    Some(&id),
                    Some(&format!("run_key={run_key};job={job_id}")),
                )
                .await;
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({"run_key": run_key, "job_id": job_id})),
            )
                .into_response()
        }
        Ok(None) => (StatusCode::CONFLICT, "run already queued").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "inventory force-run 入队失败");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}
