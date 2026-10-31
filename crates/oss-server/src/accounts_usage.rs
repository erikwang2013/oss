//! AC-2：账户软字节配额用量（只读、只告警，不阻塞写入）。
//!
//! 用量 = 账户当前全部桶中**当前版本对象**的字节和（列举口径天然排除 delete marker、
//! 非当前版本与未完成 MPU）。集群模式走 `Store::list_objects` 的既有 mesh 合并列举；
//! 丢 peer 时结果是下界（`complete=false`，头回 `partial`），不冒充精确值。
//! 逐桶分页列举，成本 O(对象数/1000)，只读管理端点，不在 S3 热路径上。
//! 同模块含 AC-3 硬配额配置路由（经 `ControlPlane` 复制；standalone 走 Local apply）。

use axum::Json;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use jwt_rust::JwtPayload;
use serde::{Deserialize, Serialize};
use xl_store::store::ListRequest;

use std::time::Duration;

use crate::accounts_support::{bad, require_member};
use crate::console::{control_write_status, forbidden, internal_error, who};
use crate::raft::ControlCommand;
use crate::state::AppState;

/// 硬配额用量采样间隔：跨节点缓存的最大陈旧度（另有写入路径 30s 惰性刷新兜底）。
const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

/// serve worker：本节点周期重算配置了硬配额的账户用量（每节点各持本地缓存）。
pub(crate) fn spawn_sampler(state: AppState) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = xl_s3::account_quota::refresh_all(state.db.pool(), &state.store).await {
                tracing::warn!(error = %e, "账户硬配额用量采样失败（保留旧缓存）");
            }
            tokio::time::sleep(SAMPLE_INTERVAL).await;
        }
    });
}

/// 列举页大小（S3 上限；列举失败即返回 500，不完整的用量不能报 ok）。
const MAX_KEYS: usize = 1000;

#[derive(Debug, Serialize)]
struct AccountUsage {
    account_id: String,
    used_bytes: u64,
    /// false = 集群有不可达 peer，`used_bytes` 只是下界。
    complete: bool,
    soft_byte_quota: Option<i64>,
    exceeded: bool,
    bucket_count: usize,
}

pub(crate) async fn account_usage(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
) -> Response {
    let (uid, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    // 与 GET /accounts/{id} 同口径：先鉴权（非成员一律 403，不泄露账户存在性），
    // 全局管理员再取账户行（缺失 404）。
    if let Err(r) = require_member(&state, uid, is_admin, &id).await {
        return r;
    }
    let account = match state.db.get_account(&id).await {
        Ok(Some(a)) => a,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询账户失败");
            return internal_error();
        }
    };
    let buckets = match state.db.account_bucket_names(&id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, account = %id, "查询账户桶失败");
            return internal_error();
        }
    };
    let mut used_bytes: u64 = 0;
    let mut incomplete = false;
    for bucket in &buckets {
        let mut token: Option<String> = None;
        loop {
            let req = ListRequest {
                max_keys: Some(MAX_KEYS),
                continuation_token: token.clone(),
                ..Default::default()
            };
            let page = match state.store.list_objects(bucket, &req).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(error = %e, bucket, "账户用量列举失败");
                    return internal_error();
                }
            };
            used_bytes = page
                .objects
                .iter()
                .fold(used_bytes, |acc, o| acc.saturating_add(o.size));
            // 集群丢 peer 时列举是下界；仍返回 200，但不得声称 ok。
            incomplete |= page.incomplete;
            match (page.is_truncated, page.next_token) {
                (false, _) => break,
                (true, Some(t)) => token = Some(t),
                // 截断却没有续页 token = 列举不完整，不能给出「ok」。
                (true, None) => {
                    tracing::error!(bucket, "列举截断但缺少续页 token");
                    return internal_error();
                }
            }
        }
    }
    // 负配额沿用 bucket_quota 的「负数 = 不限」约定；NULL 同样不限。
    let effective_quota = account.soft_byte_quota.filter(|q| *q >= 0);
    let exceeded = effective_quota.is_some_and(|q| used_bytes > q as u64);
    let mut resp = Json(AccountUsage {
        account_id: id,
        used_bytes,
        complete: !incomplete,
        soft_byte_quota: account.soft_byte_quota,
        exceeded,
        bucket_count: buckets.len(),
    })
    .into_response();
    // 配了配额才带告警头；集群下界未超限时回 partial，不能把下界当 ok。
    let signal = match (effective_quota.is_some(), exceeded, incomplete) {
        (false, _, _) => None,
        (true, true, _) => Some("exceeded"),
        (true, false, true) => Some("partial"),
        (true, false, false) => Some("ok"),
    };
    if let Some(signal) = signal {
        resp.headers_mut()
            .insert("x-oss-account-soft-quota", HeaderValue::from_static(signal));
    }
    resp
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HardQuotaBody {
    /// 必填：`null` = 取消；非负整数 = 设置（同软配额的三态语义）。
    hard_byte_quota: serde_json::Value,
    object_quota: serde_json::Value,
}

/// `null` → None；非负整数 → Some；其余非法。
fn optional_limit(v: &serde_json::Value) -> Option<Option<i64>> {
    match v {
        serde_json::Value::Null => Some(None),
        serde_json::Value::Number(n) => n.as_i64().filter(|q| *q >= 0).map(Some),
        _ => None,
    }
}

/// 硬字节/对象数配额：经 `ControlPlane` 写（Raft = level-8 复制命令，standalone =
/// Local apply）；绝不只改 leader 本地库。
pub(crate) async fn set_hard_quota(
    State(state): State<AppState>,
    Extension(payload): Extension<JwtPayload>,
    Path(id): Path<String>,
    Json(body): Json<HardQuotaBody>,
) -> Response {
    let (_, is_admin) = match who(&state, &payload).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !is_admin {
        return forbidden("需要管理员权限");
    }
    let Some(hard_byte_quota) = optional_limit(&body.hard_byte_quota) else {
        return bad("hard_byte_quota 必须 >= 0 或 null");
    };
    let Some(object_quota) = optional_limit(&body.object_quota) else {
        return bad("object_quota 必须 >= 0 或 null");
    };
    control_write_status(
        state
            .control
            .write(ControlCommand::AccountHardQuotaSet {
                id,
                hard_byte_quota,
                object_quota,
                updated_at: crate::db::now(),
            })
            .await,
        "硬配额更新冲突",
    )
}
