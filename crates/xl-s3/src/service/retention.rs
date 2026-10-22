//! M5 遗留补齐：`Put/GetObjectRetention`（改保留期）。
//!
//! 语义（与 AWS 对齐面记档）：
//! - 桶须已开 Object Lock；未开锁 → 400 `InvalidRequest`（AWS「Bucket is missing Object
//!   Lock Configuration」口径），桶不存在 → 404 `NoSuchBucket`。
//! - PUT 须点名 `versionId`（沿用本仓 legal-hold 约束；AWS 允许 latest 的差异记档）。
//! - 未到期 COMPLIANCE 只能延长（缩短/清除/降级一律 403，含 bypass）；
//!   GOVERNANCE 缩短/清除需 `x-amz-bypass-governance-retention: true`（沿用删除路径的
//!   鉴权口径：能过 PutObjectRetention 的 Write 门即可用头，未单设 policy action，记档）。
//! - 对象无 retention → 404 `NoSuchObjectLockConfiguration`。写 retention 不触碰 legal hold。
//!
//! 注意：Rust 不允许同一类型有多个 `impl S3`（trait impl 不能按模块拆分，只有 inherent
//! impl 可以）。本模块提供 free function，`service.rs` 的 `impl S3` 里有两个转发方法。
//! 主体逻辑在 `xl-store`（`set/get_object_retention`），本模块只管线格式与错误映射。

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};
use xl_store::meta::ObjectLockRecord;

use crate::convert::to_s3_error;
use crate::lock::{parse_put_lock, ts_from_nanos};

use super::OssS3;

/// 桶锁前置：桶在但未开锁 → 400（AWS Put/GetObjectRetention 口径）；桶亡 → 404。
async fn require_lock_enabled(s3: &OssS3, bucket: &str) -> S3Result<()> {
    let row = s3.acl.bucket_lock(bucket).await.map_err(|e| {
        tracing::error!(error = %e, "读取桶对象锁状态失败");
        s3s::s3_error!(InternalError, "internal error")
    })?;
    let enabled = match row {
        Some((enabled, _, _)) => enabled,
        None => {
            if !s3.store.bucket_exists(bucket).await.map_err(to_s3_error)? {
                return Err(s3s::s3_error!(NoSuchBucket, "bucket not found"));
            }
            false
        }
    };
    if !enabled {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "bucket is missing object lock configuration"
        ));
    }
    Ok(())
}

pub(super) async fn get_object_retention(
    s3: &OssS3,
    req: S3Request<GetObjectRetentionInput>,
) -> S3Result<S3Response<GetObjectRetentionOutput>> {
    require_lock_enabled(s3, &req.input.bucket).await?;
    let rec = s3
        .store
        .get_object_retention(
            &req.input.bucket,
            &req.input.key,
            req.input.version_id.as_deref(),
        )
        .await
        .map_err(to_s3_error)?;
    // 只剩 legal hold 的记录不算 retention → 与「从未设置」同判。
    let Some(ObjectLockRecord {
        mode: Some(mode),
        retain_until_nanos: Some(until),
        ..
    }) = rec
    else {
        return Err(s3s::s3_error!(
            NoSuchObjectLockConfiguration,
            "no retention settings exist for this object"
        ));
    };
    Ok(S3Response::new(GetObjectRetentionOutput {
        retention: Some(ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from(mode)),
            retain_until_date: Some(ts_from_nanos(until)),
        }),
    }))
}

pub(super) async fn put_object_retention(
    s3: &OssS3,
    req: S3Request<PutObjectRetentionInput>,
) -> S3Result<S3Response<PutObjectRetentionOutput>> {
    let PutObjectRetentionInput {
        bucket,
        key,
        version_id,
        bypass_governance_retention,
        retention,
        ..
    } = req.input;
    let Some(vid) = version_id else {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "version id is required for object retention"
        ));
    };
    require_lock_enabled(s3, &bucket).await?;
    let Some(retention) = retention else {
        return Err(s3s::s3_error!(InvalidRequest, "missing retention"));
    };
    // 复用 lock.rs：mode 白名单、mode/date 成对、until 必须在将来；两字段皆空 = 清除。
    let parsed = parse_put_lock(
        retention.mode.as_ref().map(|m| m.as_str()),
        retention.retain_until_date.as_ref(),
        None,
    )?;
    let requested = match parsed {
        None => None,
        Some(ObjectLockRecord {
            mode: Some(mode),
            retain_until_nanos: Some(until),
            ..
        }) => Some((mode, until)),
        Some(_) => {
            return Err(s3s::s3_error!(InvalidRequest, "invalid object retention"));
        }
    };
    s3.store
        .set_object_retention(
            &bucket,
            &key,
            &vid,
            requested,
            bypass_governance_retention.unwrap_or(false),
        )
        .await
        .map_err(to_s3_error)?;
    Ok(S3Response::new(PutObjectRetentionOutput::default()))
}
