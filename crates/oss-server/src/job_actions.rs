//! M8 S3：Batch Operations（清单解析 + 五种动作 + 副作用钩子）。
//!
//! 创建期授权/快照在 [`crate::job_actions_api`]；本模块只负责：清单 CSV、单任务载荷、
//! [`BatchExecutor`]（执行前按**当时的**身份/策略再授权一次，失败留给引擎按重试上限收敛
//! 为 PermanentFailure，不中断作业）。Batch 动作不经过 S3 service，桶通知按**对象所在桶**
//! 在此补发（Copy→目标桶 CreatedCopy；Delete→RemovedDelete/DeleteMarkerCreated；
//! Restore→RestorePost；ReplaceTags/InvokeLambda 无对应 S3 事件类型，不发）；副作用只走
//! [`BatchMutationHooks`]（复制入队 + tier 远端回收），与在线写路径同一组 sink。
//!
//! 跨账户边界：通知目标是**桶级配置**（`event_configs` 按桶名，见
//! [`crate::events::SqlEventSink`]），没有账户级订阅/转发模型。事件只投递到动作发生桶的
//! 配置——Copy 进目标桶、其余进对象桶；源账户桶与作业创建者的其它桶都收不到。跨账户
//! 清单/复制因此不会把事件泄漏到另一账户的桶；AWS 那种「通知目标位于另一账户」需要目标
//! 侧资源策略，本仓明确不支持（也不模拟），这是刻意的安全边界。
//!
//! 动作语义：Copy 只读源（可带 versionId），目标键 = `target.prefix + 原 key`，标签/元数据
//! 按 Copy 语义复制；ReplaceTags 对清单对象自身整体替换标签；Delete 的 tier refs 交 GC；
//! Restore 等价 S3 RestoreObject（Restoring/Restored → 幂等成功）；InvokeLambda 支持 HTTP
//! 变换器，构建开启 `olap-wasm` 时也支持 WASM（输出丢弃）。

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use xl_s3::access::{Acl, AuthedUser};
use xl_s3::events::{EventSink, EventSource, S3EventOp};
use xl_s3::olap::{OlapCache, OlapInput, acquire_transform_slot};
use xl_s3::replication::{ReplicationSink, WriteInfo};
use xl_s3::restore::RestoreSink;
use xl_s3::sse::enc_spec_from_default;
use xl_s3::tier_gc::TierGcSink;
use xl_store::meta::ObjectTag;
use xl_store::store::{DeleteOutcome, Store, VersioningMode};

use crate::db::Db;
use crate::jobs::TaskExecutor;

/// 清单对象上限（与 S3 Batch 一致）。
pub(crate) const MAX_MANIFEST_BYTES: usize = 16 << 20;
/// 单作业任务上限。
pub(crate) const MAX_MANIFEST_ROWS: usize = 10_000;

pub const BATCH_COPY: &str = "BatchCopyObject";
pub const BATCH_DELETE: &str = "BatchDeleteObject";
pub const BATCH_TAGS: &str = "BatchReplaceTags";
pub const BATCH_RESTORE: &str = "BatchRestoreObject";
pub const BATCH_INVOKE: &str = "BatchInvokeLambda";

/// 创建/执行前复核变换器类型。默认构建（未开 `olap-wasm`）明确拒绝 wasm，
/// 避免落到“变换器未加载”这种误导错误；开启后交给 `OlapTransform` 执行。
pub(crate) fn check_invoke_kind(kind: &str) -> Result<(), String> {
    match kind {
        "http" => Ok(()),
        "wasm" if cfg!(feature = "olap-wasm") => Ok(()),
        "wasm" => Err("wasm 变换器未启用（本次构建未开启 olap-wasm）".into()),
        other => Err(format!("不支持的变换器类型: {other}")),
    }
}

/// 创建期错误 → HTTP（与控制台其余 handler 同口径：403 纯文本、500 泛化）。
#[derive(Debug)]
pub enum BatchError {
    Invalid(String),
    Denied(String),
    NotFound(String),
    Quota(String),
    Internal(String),
}

impl IntoResponse for BatchError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            BatchError::Invalid(m) => (StatusCode::BAD_REQUEST, m),
            BatchError::Denied(m) => (StatusCode::FORBIDDEN, m),
            BatchError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            BatchError::Quota(m) => (StatusCode::TOO_MANY_REQUESTS, m),
            BatchError::Internal(m) => {
                tracing::error!(error = %m, "batch 创建失败");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            }
        };
        (code, msg).into_response()
    }
}

/// 清单一行（创建时快照进 `job_tasks.payload_json`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRow {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

/// 解析 `bucket,key[,versionId]` CSV：容忍 BOM/表头/CRLF，允许引号字段；
/// 空行跳过；键逐行 `validate_key`；非法行整单 400。
pub fn parse_manifest(bytes: &[u8]) -> Result<Vec<ManifestRow>, BatchError> {
    let bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(bytes);
    let mut rows: Vec<ManifestRow> = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| BatchError::Invalid(format!("清单 CSV 解析失败: {e}")))?;
        if rec.iter().all(|f| f.trim().is_empty()) {
            continue;
        }
        let line = rec.position().map(|p| p.line()).unwrap_or(0);
        if rows.is_empty()
            && rec
                .get(0)
                .is_some_and(|f| f.trim().eq_ignore_ascii_case("bucket"))
            && rec
                .get(1)
                .is_some_and(|f| f.trim().eq_ignore_ascii_case("key"))
        {
            continue; // 表头只容首行
        }
        if rec.len() < 2 || rec.len() > 3 {
            return Err(BatchError::Invalid(format!(
                "清单第 {line} 行：需要 bucket,key[,versionId]"
            )));
        }
        let bucket = rec[0].trim();
        let key = &rec[1];
        if bucket.is_empty() || key.is_empty() {
            return Err(BatchError::Invalid(format!("清单第 {line} 行：桶或键为空")));
        }
        xl_store::layout::validate_bucket(bucket)
            .map_err(|e| BatchError::Invalid(format!("清单第 {line} 行：{e}")))?;
        xl_store::layout::validate_key(key)
            .map_err(|e| BatchError::Invalid(format!("清单第 {line} 行：{e}")))?;
        let version_id = rec
            .get(2)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string);
        rows.push(ManifestRow {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
        });
        if rows.len() > MAX_MANIFEST_ROWS {
            return Err(BatchError::Invalid(format!(
                "清单超过 {MAX_MANIFEST_ROWS} 行上限"
            )));
        }
    }
    if rows.is_empty() {
        return Err(BatchError::Invalid("清单为空".into()));
    }
    Ok(rows)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOp {
    Copy,
    Delete,
    ReplaceTags,
    Restore,
    Invoke,
}

impl BatchOp {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "CopyObject" | "Copy" => Self::Copy,
            "DeleteObject" | "Delete" => Self::Delete,
            "ReplaceTags" | "PutObjectTagging" => Self::ReplaceTags,
            "RestoreObject" | "Restore" => Self::Restore,
            "InvokeLambda" => Self::Invoke,
            _ => return None,
        })
    }

    pub(crate) fn action(self) -> &'static str {
        match self {
            Self::Copy => BATCH_COPY,
            Self::Delete => BATCH_DELETE,
            Self::ReplaceTags => BATCH_TAGS,
            Self::Restore => BATCH_RESTORE,
            Self::Invoke => BATCH_INVOKE,
        }
    }
}

/// 单任务载荷：`TaskExecutor` 拿不到 job_id/请求体，所有参数都在这里。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BatchActionPayload {
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub version_id: Option<String>,
    /// 创建者 uid（执行前复验身份，见模块头注）。
    pub uid: i64,
    #[serde(default)]
    pub target_bucket: Option<String>,
    #[serde(default)]
    pub target_prefix: Option<String>,
    #[serde(default)]
    pub tags: Vec<ObjectTag>,
    #[serde(default)]
    pub restore_days: Option<u32>,
    #[serde(default)]
    pub transform_access_point: Option<String>,
}

/// 写路径副作用（N13）：复制入队与 tier 远端回收都复用在线路径的同一组 sink。
pub struct BatchMutationHooks {
    pub repl: Arc<dyn ReplicationSink>,
    pub tier_gc: Arc<dyn TierGcSink>,
    /// M9 S3：共享删除入口（sync 桶远端优先；batch 不直连 store.delete_*）。
    pub sync_delete: Arc<dyn xl_s3::sync_delete::SyncDeleteSink>,
}

/// `Batch*` 动作执行器。
pub struct BatchExecutor {
    db: Arc<Db>,
    store: Arc<Store>,
    acl: Acl,
    restore: Arc<dyn RestoreSink>,
    olap: Arc<OlapCache>,
    events: Arc<dyn EventSink>,
    hooks: BatchMutationHooks,
}

impl BatchExecutor {
    pub fn new(
        db: Arc<Db>,
        store: Arc<Store>,
        acl: Acl,
        restore: Arc<dyn RestoreSink>,
        olap: Arc<OlapCache>,
        events: Arc<dyn EventSink>,
        hooks: BatchMutationHooks,
    ) -> Self {
        Self {
            db,
            store,
            acl,
            restore,
            olap,
            events,
            hooks,
        }
    }

    /// 桶通知补发：Batch 不经过 S3 service，事件在这里按对象所在桶（Copy = 目标桶）发出。
    /// `EventSink` 自带「失败只 warn」语义，绝不改变任务结果（至少一次 outbox）。
    async fn emit_event(
        &self,
        op: S3EventOp,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: Option<u64>,
        etag: Option<&str>,
    ) {
        self.events
            .emit(
                op,
                bucket,
                key,
                version_id,
                size,
                etag,
                EventSource::internal("oss-batch"),
            )
            .await;
    }

    async fn versioning_mode(&self, bucket: &str) -> Result<VersioningMode, String> {
        self.db
            .get_bucket_versioning(bucket)
            .await
            .map(|s| VersioningMode::from_status(&s))
            .map_err(|e| e.to_string())
    }

    /// 执行前复验：创建者仍有效 + 当前策略仍允许。失败即 Err（引擎收敛为 PermanentFailure）。
    async fn author(
        &self,
        p: &BatchActionPayload,
        op: &str,
        bucket: &str,
        key: &str,
        copy_src: Option<(&str, &str)>,
    ) -> Result<AuthedUser, String> {
        let Some((_, is_admin, active)) =
            self.db.get_user(p.uid).await.map_err(|e| e.to_string())?
        else {
            return Err("创建者账号已不存在".into());
        };
        if !active {
            return Err("创建者账号已停用".into());
        }
        let user = AuthedUser {
            uid: p.uid,
            is_admin,
            ak: String::new(),
        };
        match self
            .acl
            .check_op(&user, "", op, Some(bucket), Some(key), copy_src)
            .await
        {
            Ok((true, _)) => Ok(user),
            Ok((false, detail)) => {
                Err(detail.unwrap_or_else(|| format!("无权 {op} {bucket}/{key}")))
            }
            Err(e) => Err(format!("判权失败: {e}")),
        }
    }

    async fn copy(&self, p: &BatchActionPayload) -> Result<String, String> {
        let dst_bucket = p.target_bucket.as_deref().ok_or("缺少目标桶")?;
        let dst_key = format!("{}{}", p.target_prefix.as_deref().unwrap_or(""), p.key);
        self.author(
            p,
            "CopyObject",
            dst_bucket,
            &dst_key,
            Some((&p.bucket, &p.key)),
        )
        .await?;
        let (info, bytes) = match &p.version_id {
            Some(v) => {
                self.store
                    .get_object_version_ext(&p.bucket, &p.key, v, None)
                    .await
            }
            None => self.store.get_object_ext(&p.bucket, &p.key, None).await,
        }
        .map_err(|e| e.to_string())?;
        let mode = self.versioning_mode(dst_bucket).await?;
        // M8 缺口 C：Batch Copy 无显式 SSE 入口，直接套目标桶默认加密
        let encryption = match self
            .acl
            .bucket_default_encryption(dst_bucket)
            .await
            .map_err(|e| e.to_string())?
            .as_deref()
        {
            Some(v) => Some(
                enc_spec_from_default(v).ok_or_else(|| format!("目标桶默认加密记录非法: {v}"))?,
            ),
            None => None,
        };
        let tags = (!info.tags.is_empty()).then(|| info.tags.clone());
        let out = self
            .store
            .put_object_stream_with_meta(
                dst_bucket,
                &dst_key,
                Box::pin(futures::stream::once(async move { Ok(bytes) })),
                info.content_type.clone(),
                info.user_metadata.clone(),
                None,
                mode,
                encryption,
                None,
                tags,
                // 目标固定 STANDARD（在线 CopyObject 语义）；源存储类不继承——Restored
                // 归档源会把 GLACIER 写成一个无 tier 的非法目标状态。
                None,
            )
            .await
            .map_err(|e| e.to_string())?;
        self.emit_event(
            S3EventOp::CreatedCopy,
            dst_bucket,
            &dst_key,
            out.version_id.as_deref(),
            Some(out.size),
            Some(&out.etag),
        )
        .await;
        self.hooks
            .repl
            .on_write(WriteInfo {
                bucket: dst_bucket,
                key: &dst_key,
                version_id: out.version_id.as_deref(),
                etag: &out.etag,
                size: out.size,
                is_delete_marker: false,
                is_version_delete: false,
                is_replica: false,
            })
            .await;
        Ok("Succeeded".into())
    }

    async fn delete(&self, p: &BatchActionPayload) -> Result<String, String> {
        self.author(p, "DeleteObject", &p.bucket, &p.key, None)
            .await?;
        let mode = self.versioning_mode(&p.bucket).await?;
        // M9 S3：sync 桶远端优先（batch 不直连 store.delete_*）；async 桶行为不变。
        let outcome = self
            .hooks
            .sync_delete
            .delete_versioned(&p.bucket, &p.key, p.version_id.as_deref(), mode, false)
            .await
            .map_err(|e| e.to_string())?;
        let refs = match &outcome {
            DeleteOutcome::Version { tier: Some(t), .. } => vec![t.clone()],
            DeleteOutcome::Version { tier: None, .. } => Vec::new(),
            DeleteOutcome::Object { tiers } => tiers.clone(),
            DeleteOutcome::Marker { .. } | DeleteOutcome::NotFound => Vec::new(),
        };
        match &outcome {
            DeleteOutcome::Version { version_id, .. } => {
                self.emit_event(
                    S3EventOp::RemovedDelete,
                    &p.bucket,
                    &p.key,
                    Some(version_id),
                    None,
                    None,
                )
                .await;
                // 4.16：指定版本删除按同 id 定点复制（不造 marker）。
                self.hooks
                    .repl
                    .on_write(WriteInfo {
                        bucket: &p.bucket,
                        key: &p.key,
                        version_id: Some(version_id),
                        etag: "",
                        size: 0,
                        is_delete_marker: false,
                        is_version_delete: true,
                        is_replica: false,
                    })
                    .await;
            }
            DeleteOutcome::Object { .. } => {
                self.emit_event(
                    S3EventOp::RemovedDelete,
                    &p.bucket,
                    &p.key,
                    None,
                    None,
                    None,
                )
                .await;
            }
            DeleteOutcome::Marker { version_id } => {
                self.emit_event(
                    S3EventOp::RemovedDeleteMarkerCreated,
                    &p.bucket,
                    &p.key,
                    Some(version_id),
                    None,
                    None,
                )
                .await;
                self.hooks
                    .repl
                    .on_write(WriteInfo {
                        bucket: &p.bucket,
                        key: &p.key,
                        version_id: Some(version_id),
                        etag: "",
                        size: 0,
                        is_delete_marker: true,
                        is_version_delete: false,
                        is_replica: false,
                    })
                    .await;
            }
            DeleteOutcome::NotFound => {}
        }
        if !refs.is_empty()
            && let Err(e) = self.hooks.tier_gc.delete(&refs).await
        {
            tracing::warn!(bucket = %p.bucket, key = %p.key, error = %e, "batch 删对象后清 tier 失败");
        }
        Ok("Succeeded".into())
    }

    async fn replace_tags(&self, p: &BatchActionPayload) -> Result<String, String> {
        self.author(p, "PutObjectTagging", &p.bucket, &p.key, None)
            .await?;
        // M9：标签不跨站复制——sync 桶明确拒绝（与 S3 Tagging API 同口径）。
        if self
            .store
            .sync_mode(&p.bucket)
            .await
            .map_err(|e| e.to_string())?
        {
            return Err("NotImplemented: object tagging is not supported on sync buckets".into());
        }
        self.store
            .set_object_tags(&p.bucket, &p.key, p.version_id.as_deref(), p.tags.clone())
            .await
            .map_err(|e| e.to_string())?;
        // S3 没有对象标签事件类型：不发桶通知，不发明事件名。
        Ok("Succeeded".into())
    }

    async fn restore(&self, p: &BatchActionPayload) -> Result<String, String> {
        self.author(p, "RestoreObject", &p.bucket, &p.key, None)
            .await?;
        let version_id = match &p.version_id {
            Some(v) => v.clone(),
            None => self
                .store
                .head_object(&p.bucket, &p.key)
                .await
                .map_err(|e| e.to_string())?
                .version_id
                .unwrap_or_else(|| "null".to_string()),
        };
        match self
            .store
            .mark_restore_pending(&p.bucket, &p.key, &version_id)
            .await
        {
            // Restoring/Restored：幂等成功（在线路径此处 409；Batch 语义记档）。
            Err(xl_store::error::Error::RestoreAlreadyInProgress(_)) => Ok("Succeeded".into()),
            Err(e) => Err(e.to_string()),
            Ok(()) => {
                if let Err(e) = self
                    .restore
                    .enqueue_restore(&p.bucket, &p.key, &version_id, p.restore_days.unwrap_or(1))
                    .await
                {
                    let _ = self
                        .store
                        .abort_restore(&p.bucket, &p.key, &version_id)
                        .await;
                    return Err(format!("Restore 入队失败: {e}"));
                }
                // 只在受理时发（Restoring/Restored 的幂等分支不发，与在线 409 口径一致）。
                self.emit_event(
                    S3EventOp::RestorePost,
                    &p.bucket,
                    &p.key,
                    Some(&version_id),
                    None,
                    None,
                )
                .await;
                Ok("Succeeded".into())
            }
        }
    }

    async fn invoke(&self, p: &BatchActionPayload) -> Result<String, String> {
        let user = self.author(p, "GetObject", &p.bucket, &p.key, None).await?;
        let ap = p
            .transform_access_point
            .as_deref()
            .ok_or("缺少 transform_access_point")?;
        let Some(row) = self
            .db
            .get_access_point(ap)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Err(format!("access point {ap} 不存在"));
        };
        if !user.is_admin && row.owner_id != user.uid {
            return Err("transform access point 需 owner/admin".into());
        }
        // 配置可能在创建后被改成 wasm：执行前复核一次。
        let cfg = self
            .db
            .get_olap_config(ap)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("transform access point 未配置变换器")?;
        check_invoke_kind(&cfg.kind)?;
        let t = self
            .olap
            .get(ap)
            .map_err(|e| e.to_string())?
            .ok_or("变换器未加载（配置更新后重试）")?;
        let (info, bytes) = match &p.version_id {
            Some(v) => {
                self.store
                    .get_object_version_ext(&p.bucket, &p.key, v, None)
                    .await
            }
            None => self.store.get_object_ext(&p.bucket, &p.key, None).await,
        }
        .map_err(|e| e.to_string())?;
        let slot = acquire_transform_slot().await?;
        t.transform(
            OlapInput {
                bucket: &p.bucket,
                key: &p.key,
                version_id: info.version_id.as_deref(),
                etag: Some(&info.etag),
                bytes,
            },
            &slot,
        )
        .await?; // 输出丢弃：不修改对象，不发桶通知
        Ok("Succeeded".into())
    }
}

#[async_trait::async_trait]
impl TaskExecutor for BatchExecutor {
    async fn execute(&self, action: &str, payload: &str) -> Result<String, String> {
        let p: BatchActionPayload =
            serde_json::from_str(payload).map_err(|e| format!("payload 非法: {e}"))?;
        match action {
            BATCH_COPY => self.copy(&p).await,
            BATCH_DELETE => self.delete(&p).await,
            BATCH_TAGS => self.replace_tags(&p).await,
            BATCH_RESTORE => self.restore(&p).await,
            BATCH_INVOKE => self.invoke(&p).await,
            other => Err(format!("未注册的作业动作: {other}")),
        }
    }
}
