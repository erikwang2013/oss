//! AC-2：账户软字节配额用量（只读、只告警，不阻塞写入）。
//!
//! 用量 = 账户当前全部桶中**当前版本对象**的字节和（列举口径天然排除 delete marker、
//! 非当前版本与未完成 MPU）。集群模式走 `Store::list_objects` 的既有 mesh 合并列举。

use axum::Json;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use jwt_rust::JwtPayload;
use serde::Serialize;
use xl_store::store::ListRequest;

use crate::accounts_support::require_member;
use crate::console::{internal_error, who};
use crate::state::AppState;

/// 列举页大小（S3 上限；列举失败即返回 500，不完整的用量不能报 ok）。
const MAX_KEYS: usize = 1000;

#[derive(Debug, Serialize)]
struct AccountUsage {
    account_id: String,
    used_bytes: u64,
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
    // 缺失账户一律 404（先于成员判定）；账户行同时给出软配额。
    let account = match state.db.get_account(&id).await {
        Ok(Some(a)) => a,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "查询账户失败");
            return internal_error();
        }
    };
    if let Err(r) = require_member(&state, uid, is_admin, &id).await {
        return r;
    }
    let buckets = match state.db.account_bucket_names(&id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, account = %id, "查询账户桶失败");
            return internal_error();
        }
    };
    let mut used_bytes: u64 = 0;
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
    let exceeded = account
        .soft_byte_quota
        .is_some_and(|q| used_bytes > u64::try_from(q).unwrap_or(0));
    let mut resp = Json(AccountUsage {
        account_id: id,
        used_bytes,
        soft_byte_quota: account.soft_byte_quota,
        exceeded,
        bucket_count: buckets.len(),
    })
    .into_response();
    // 配了配额才带告警头：ok / exceeded；NULL = 不限，则无头。
    let signal = match (account.soft_byte_quota.is_some(), exceeded) {
        (false, _) => None,
        (true, true) => Some("exceeded"),
        (true, false) => Some("ok"),
    };
    if let Some(signal) = signal {
        resp.headers_mut()
            .insert("x-oss-account-soft-quota", HeaderValue::from_static(signal));
    }
    resp
}
