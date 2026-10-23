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
                    account_id: None,
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
    destination_prefix: Option<String>,
    format: String,
    filter_prefix: Option<String>,
    included_object_versions: String,
    optional_fields: Vec<String>,
    incremental: bool,
}

impl ConfigView {
    fn new(c: &InventoryConfiguration, incremental: bool) -> Self {
        let d = &c.destination.s3_bucket_destination;
        Self {
            id: c.id.clone(),
            is_enabled: c.is_enabled,
            frequency: c.schedule.frequency.as_str().to_string(),
            destination_bucket: d.bucket.clone(),
            destination_prefix: d.prefix.clone(),
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
            let mut views = Vec::with_capacity(list.len());
            for c in &list {
                // M5：增量开关/水位是节点本地表，Raft 多节点下不可用，统一报 false。
                let incremental = if state.control.is_raft() {
                    false
                } else {
                    state
                        .db
                        .inventory_incremental_get(&bucket, &c.id)
                        .await
                        .unwrap_or(false)
                };
                views.push(ConfigView::new(c, incremental));
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
    // M5：模式/水位是本地表，各节点各写各的会让同一配置交替全量/增量；Raft 下直接拒绝。
    if incremental && state.control.is_raft() {
        return (
            StatusCode::BAD_REQUEST,
            "incremental 仅支持单节点（非 Raft）部署",
        )
            .into_response();
    }
    let id = body.id.clone();
    match state
        .inventory
        .put_config(&bucket, &owner, &body.into_config())
        .await
    {
        Ok(()) => {
            // put_config 会复位增量模式；console 请求随后按请求值重新开启。
            match state
                .db
                .inventory_incremental_set(&bucket, &id, incremental, crate::db::now())
                .await
            {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(e) => {
                    tracing::error!(error = %e, "inventory 增量模式写入失败");
                    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
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
    match crate::inventory::enqueue_run(&state.db, &bucket, &id, &run_key, Some(uid), now).await {
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
