//! s3s 的 `S3` trait 实现。M1 覆盖：桶 CRUD、对象 CRUD、ListObjectsV2、CopyObject、批量删除。
//! Multipart / Range / 版本 / 加密 / 锁 / 复制分别是 M2/M5/M6 的事。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::StatusCode;
use s3s::checksum::ChecksumHasher;
use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result};
use xl_store::error::Error as StoreError;
use xl_store::meta::{ChecksumRecord, ObjectTag};
use xl_store::store::{DeleteOutcome, ListRequest, ObjectInfo, Store, VersioningMode};

use crate::bucket_control::{BucketControlError, BucketControlSink, BucketCreateSpec};
use crate::convert::to_s3_error;
use crate::events::{EventSink, EventSource, S3EventOp};
use crate::lock::{apply_lock_out, bucket_lock_out, parse_bucket_lock_rule, parse_put_lock};
use crate::sse::{
    SseReq, enc_spec_from_default, parse_sse, sse_c_headers, sse_c_key_md5_header, sse_out,
    sse_out_from_spec,
};

mod list_multipart_uploads;
mod list_v1;
mod retention;
mod upload_part_copy;

#[derive(Clone)]
pub struct OssS3 {
    store: Arc<Store>,
    /// 判权/记账（桶归属、可见性过滤）；授权门本体在 `S3ServiceBuilder::set_access`
    /// （同一个 [`Acl`] 实例的克隆）。
    acl: crate::access::Acl,
    /// 单次 PUT 的整份读入上限，见 `put_object` 的 ponytail 取舍。
    max_put_bytes: usize,
    /// M6 切片 0：事件接收方（oss-server 注入；None = 不发事件，测试/未接线形态）。
    events: Option<Arc<dyn EventSink>>,
    /// M6 切片 4：复制写入钩子（post-commit 入队；None = 未接线）
    repl_sink: Option<Arc<dyn crate::replication::ReplicationSink>>,
    /// M6 切片 4：复制配置读存（Put/Get/DeleteBucketReplication）
    repl_store: Option<Arc<dyn crate::replication::ReplicationStore>>,
    /// M8 S0：删桶后的控制面清理（oss-server 注入；None = 未接线）
    bucket_cleanup: Option<Arc<dyn crate::bucket_cleanup::BucketCleanupSink>>,
    /// M8 S4a：删除归档版本的远端 tier 清理（S4b 注入真实实现；None = 不清理）
    tier_gc: Option<Arc<dyn crate::tier_gc::TierGcSink>>,
    /// 归档对象 Range 直读（默认关：None = 既有 403 行为）。
    tier_direct: Option<Arc<dyn crate::tier_direct::TierDirectRead>>,
    /// M8 S4b：生命周期规则读写（None = 501）。
    lifecycle: Option<Arc<dyn crate::lifecycle::LifecycleStore>>,
    /// M8 S4b：Restore 入队（None = 501）。
    restore: Option<Arc<dyn crate::restore::RestoreSink>>,
    /// M8 S5：Inventory 配置读写（None = 501）。
    inventory: Option<Arc<dyn crate::inventory::InventoryStore>>,
    /// M9：sync 桶能力开关（oss-server 注入 `SYNC_READY`；false 时创建 sync 桶 400）。
    sync_ready: bool,
    /// M9：本机站点 token（sync 摄入的 `x-oss-site-token` 校验；None = 拒收）。
    site_token: Option<String>,
    /// M9 S3：共享删除服务（None = 未接线/测试：直接 store，M7 行为不变）。
    sync_delete: Option<Arc<dyn crate::sync_delete::SyncDeleteSink>>,
    /// M9 S6c：桶控制写通道（仅 `[raft]` 注入；None = M7/standalone 直写路径不变）。
    bucket_control: Option<Arc<dyn BucketControlSink>>,
}

impl OssS3 {
    /// 站点 token 校验（常量时间；token 来自 `[site].token`/站点身份；
    /// 本站未启用站点模式或请求未携带 → false）。
    fn site_token_ok(&self, headers: &http::HeaderMap) -> bool {
        match (
            self.site_token.as_deref(),
            headers
                .get("x-oss-site-token")
                .and_then(|v| v.to_str().ok()),
        ) {
            (Some(expected), Some(got)) if !expected.is_empty() => ct_eq(expected, got),
            _ => false,
        }
    }

    /// 请求是否声明复制摄入（`x-amz-replication-status: REPLICA`，大小写不敏感）。
    fn replica_requested(headers: &http::HeaderMap) -> bool {
        headers
            .get("x-amz-replication-status")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("REPLICA"))
    }

    /// M8 缺口 C：请求未带任何显式 SSE（含 SSE-C）时套用桶默认加密；显式请求永远优先。
    /// 库内未知值属记录损坏 → 500（不静默降级为明文）。
    async fn apply_default_encryption(&self, bucket: &str, sse: &mut SseReq) -> S3Result<()> {
        if sse.spec.is_some() {
            return Ok(());
        }
        let stored = self
            .acl
            .bucket_default_encryption(bucket)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, bucket, "读取桶默认加密失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
        if let Some(v) = stored {
            let Some(spec) = enc_spec_from_default(&v) else {
                tracing::error!(bucket, value = %v, "桶默认加密记录非法");
                return Err(s3s::s3_error!(InternalError, "internal error"));
            };
            sse.spec = Some(spec);
        }
        Ok(())
    }

    /// 3.16：显式 CMK 必须存在于本节点 KMS 密钥环（客户端选错名 → 400，fail-closed）。
    /// 未配置 KMS provider = 部署错误 → 500（绝不静默降级到默认钥/明文）。
    fn ensure_cmk_available(&self, sse: &SseReq) -> S3Result<()> {
        let Some(xl_store::crypto::EncSpec::SseKmsCmk(name)) = &sse.spec else {
            return Ok(());
        };
        self.ensure_cmk_named(name)
    }

    fn ensure_cmk_named(&self, name: &str) -> S3Result<()> {
        let kms = self
            .store
            .enc_state()
            .and_then(|es| es.kms.as_ref())
            .ok_or_else(|| {
                tracing::error!(
                    cmk = name,
                    "显式 CMK 请求但未配置 KMS provider（OSS_KMS_DIR）"
                );
                s3s::s3_error!(InternalError, "internal error")
            })?;
        kms.ensure_cmk(name).map_err(to_s3_error)?;
        Ok(())
    }

    /// 桶版本控制三态（读 DB；查库失败上抛 500——状态不可知时不写数据）。
    async fn versioning_mode(&self, bucket: &str) -> S3Result<VersioningMode> {
        match self.acl.bucket_versioning(bucket).await {
            Ok(s) => Ok(VersioningMode::from_status(&s)),
            Err(e) => {
                tracing::error!(error = %e, "读取桶版本控制状态失败");
                Err(s3s::s3_error!(InternalError, "internal error"))
            }
        }
    }

    pub fn new(store: Arc<Store>, acl: crate::access::Acl, max_put_bytes: usize) -> Self {
        Self {
            store,
            acl,
            max_put_bytes,
            events: None,
            repl_sink: None,
            repl_store: None,
            bucket_cleanup: None,
            tier_gc: None,
            tier_direct: None,
            lifecycle: None,
            restore: None,
            inventory: None,
            sync_ready: false,
            site_token: None,
            sync_delete: None,
            bucket_control: None,
        }
    }

    /// M9 S6c：注入 Raft 控制写通道（`None` = 保持 M7 直写）。
    pub fn with_bucket_control(
        mut self,
        sink: Option<Arc<dyn crate::bucket_control::BucketControlSink>>,
    ) -> Self {
        self.bucket_control = sink;
        self
    }

    /// M9 S3：注入共享删除服务（sync 桶 remote-first；None 保持 store 直连）。
    pub fn with_sync_delete(
        mut self,
        sink: Option<Arc<dyn crate::sync_delete::SyncDeleteSink>>,
    ) -> Self {
        self.sync_delete = sink;
        self
    }

    /// 删除统一入口：四类外部 caller 都不直接调 `store.delete_*`。
    async fn delete_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        mode: VersioningMode,
        bypass: bool,
    ) -> xl_store::error::Result<DeleteOutcome> {
        match &self.sync_delete {
            Some(s) => {
                s.delete_versioned(bucket, key, version_id, mode, bypass)
                    .await
            }
            None => {
                self.store
                    .delete_object_versioned_ext(bucket, key, version_id, mode, bypass)
                    .await
            }
        }
    }

    /// M9：sync 桶能力（由 oss-server 按 `SYNC_READY` 注入）。
    pub fn with_sync_ready(mut self, ready: bool) -> Self {
        self.sync_ready = ready;
        self
    }

    /// M9：本机站点 token（`[site].token`；sync 摄入校验用）。
    pub fn with_site_token(mut self, token: Option<String>) -> Self {
        self.site_token = token;
        self
    }

    /// M8 S4a：注入 tier 清理实现（S4b 装配真实 sink；本片可 fake/None）。
    pub fn with_tier_gc(mut self, sink: Option<Arc<dyn crate::tier_gc::TierGcSink>>) -> Self {
        self.tier_gc = sink;
        self
    }

    /// 归档对象 Range 直读（None = 关闭，保持 403 InvalidObjectState）。
    pub fn with_tier_direct(
        mut self,
        direct: Option<Arc<dyn crate::tier_direct::TierDirectRead>>,
    ) -> Self {
        self.tier_direct = direct;
        self
    }

    /// 直读归档 Range；不可用 → None（回退既有 403），完整性失败 → 500。
    async fn tier_direct_range(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        info: &ObjectInfo,
        range: std::ops::Range<u64>,
    ) -> S3Result<Option<Bytes>> {
        let Some(direct) = &self.tier_direct else {
            return Ok(None);
        };
        match direct
            .read_range(bucket, key, version_id, info, range)
            .await
        {
            Ok(b) => Ok(Some(b)),
            Err(crate::tier_direct::TierDirectError::Unavailable(why)) => {
                tracing::debug!(bucket, key, why, "tier 直读不可用，回退 InvalidObjectState");
                Ok(None)
            }
            Err(crate::tier_direct::TierDirectError::Integrity(why)) => {
                tracing::error!(bucket, key, error = %why, "tier 直读完整性校验失败");
                Err(s3s::s3_error!(
                    InternalError,
                    "tier object integrity check failed"
                ))
            }
        }
    }

    /// M8 S4b：注入生命周期规则存储（console 与 S3 共用同一实例）。
    pub fn with_lifecycle(
        mut self,
        store: Option<Arc<dyn crate::lifecycle::LifecycleStore>>,
    ) -> Self {
        self.lifecycle = store;
        self
    }

    /// M8 S4b：注入 Restore 入队 sink（None = RestoreObject 501）。
    pub fn with_restore(mut self, sink: Option<Arc<dyn crate::restore::RestoreSink>>) -> Self {
        self.restore = sink;
        self
    }

    fn lifecycle_store(&self) -> S3Result<&Arc<dyn crate::lifecycle::LifecycleStore>> {
        self.lifecycle
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(NotImplemented, "lifecycle configuration is not wired"))
    }

    fn restore_sink(&self) -> S3Result<&Arc<dyn crate::restore::RestoreSink>> {
        self.restore
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(NotImplemented, "restore is not wired"))
    }

    /// M8 S4b：读/HEAD 发现恢复已过期 → 与 reaper 同路 best-effort 回收本地副本并删行。
    /// 失败留给 reaper（Version phase/expiry 始终是真相）。
    async fn cleanup_expired_restore(&self, bucket: &str, key: &str, info: &ObjectInfo) {
        if info.tier_phase != Some(xl_store::meta::TierPhase::Restored) {
            return;
        }
        let Some(exp) = info.restore_expires_at else {
            return;
        };
        let now = now_secs();
        if exp > now {
            return;
        }
        let vid = info
            .version_id
            .clone()
            .unwrap_or_else(|| "null".to_string());
        match self.store.expire_restore(bucket, key, &vid, now).await {
            Ok(true) => {}
            Ok(false) => return, // 竞态：别人已处理（或实际未到期），行留给 reaper
            Err(e) => {
                tracing::warn!(error = %e, bucket, key, "读路径回收过期恢复失败（留给 reaper）");
                return;
            }
        }
        if let Some(sink) = &self.restore
            && let Err(e) = sink.forget_restore(bucket, key, &vid).await
        {
            tracing::warn!(error = %e, bucket, key, "读路径删除过期恢复行失败");
        }
    }

    /// M8 S5：注入 Inventory 配置存储（console 与 S3 共用同一实例）。
    pub fn with_inventory(
        mut self,
        store: Option<Arc<dyn crate::inventory::InventoryStore>>,
    ) -> Self {
        self.inventory = store;
        self
    }

    fn inventory_store(&self) -> S3Result<&Arc<dyn crate::inventory::InventoryStore>> {
        self.inventory
            .as_ref()
            .ok_or_else(|| s3s::s3_error!(NotImplemented, "inventory configuration is not wired"))
    }

    /// M8 S4a：删除已成功的归档版本 → 远端清理（best-effort）。
    async fn tier_gc_delete(&self, refs: &[xl_store::store::TierRef]) {
        if refs.is_empty() {
            return;
        }
        if let Some(sink) = &self.tier_gc
            && let Err(e) = sink.delete(refs).await
        {
            tracing::warn!(error = %e, n = refs.len(), "tier 清理失败（本地删除已生效）");
        }
    }

    /// M8 S0：注入删桶清理实现（builder 风格；S1 装配 ApCache 后接线）。
    pub fn with_bucket_cleanup(
        mut self,
        sink: Option<Arc<dyn crate::bucket_cleanup::BucketCleanupSink>>,
    ) -> Self {
        self.bucket_cleanup = sink;
        self
    }

    /// M8 S0：删桶后 best-effort 清 M8 表并总是失效 AP 缓存。
    async fn cleanup_m8(&self, bucket: &str) {
        if let Some(sink) = &self.bucket_cleanup {
            if let Err(e) = sink.clear_bucket_m8(bucket).await {
                tracing::warn!(error = %e, bucket, "删桶 M8 表清理失败");
            }
            sink.invalidate_ap_cache(bucket);
        }
    }

    /// M6 切片 0：注入事件接收方（builder 风格；router 装配处调用）。
    pub fn with_events(mut self, sink: Option<Arc<dyn EventSink>>) -> Self {
        self.events = sink;
        self
    }

    /// M6 切片 4：注入复制写入钩子 + 配置存储（builder 风格；router 装配处调用）。
    pub fn with_replication(
        mut self,
        sink: Option<Arc<dyn crate::replication::ReplicationSink>>,
        store: Option<Arc<dyn crate::replication::ReplicationStore>>,
    ) -> Self {
        self.repl_sink = sink;
        self.repl_store = store;
        self
    }

    /// M7：建桶记账冲突后的补偿——按**最终行**判断：远端 upsert 胜出（行存在）时保留
    /// 物理桶；远端 delete 胜出（无行）才删除刚建的空目录（不可误删对端已建桶）。
    async fn rollback_created_bucket(&self, bucket: &str) {
        match self.acl.bucket_lock(bucket).await {
            Ok(Some(_)) => {
                // 行存在：物理桶应存在（若并发窗口里被删则补建空桶）。
                match self.store.bucket_exists(bucket).await {
                    Ok(true) => {}
                    Ok(false) => {
                        if let Err(e) = self.store.create_bucket(bucket).await {
                            tracing::warn!(error = %e, bucket, "冲突补偿补建物理桶失败");
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, bucket, "冲突补偿查桶失败"),
                }
            }
            Ok(None) => {
                if let Err(e) = self.store.delete_bucket(bucket).await {
                    tracing::warn!(error = %e, bucket, "冲突补偿删除空桶失败");
                }
            }
            Err(e) => tracing::warn!(error = %e, bucket, "冲突补偿读桶行失败（保守保留物理桶）"),
        }
    }

    /// 复制写提交回调（post-commit 尽力而为；实现自吞错误）。
    async fn repl_on_write(&self, info: crate::replication::WriteInfo<'_>) {
        if let Some(sink) = &self.repl_sink {
            sink.on_write(info).await;
        }
    }

    /// 事件发射（post-commit 尽力而为；接收方自吞错误，绝不影响请求结果）。
    #[allow(clippy::too_many_arguments)] // 事件字段面平铺（同 EventSink::emit）
    async fn emit(
        &self,
        op: S3EventOp,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        size: Option<u64>,
        etag: Option<&str>,
        source: EventSource,
    ) {
        if let Some(sink) = &self.events {
            sink.emit(op, bucket, key, version_id, size, etag, source)
                .await;
        }
    }

    /// M5 切片 4（L2/L3）：桶锁状态 + 请求锁 → 生效记录。
    /// 桶行不存在且带锁头 → 404 NoSuchBucket（N4）；未开锁带锁头 → 400；
    /// 无头 + 开锁桶有默认规则 → 按默认规则计算；余 None（存量无行桶保持既有行为）。
    async fn resolve_lock(
        &self,
        bucket: &str,
        req: Option<xl_store::meta::ObjectLockRecord>,
    ) -> S3Result<Option<xl_store::meta::ObjectLockRecord>> {
        let row = self.acl.bucket_lock(bucket).await.map_err(|e| {
            tracing::error!(error = %e, "读取桶对象锁状态失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        match (row, req) {
            (Some((false, _, _)), Some(_)) => Err(s3s::s3_error!(
                InvalidRequest,
                "bucket object lock is not enabled"
            )),
            (Some((_, _, _)), Some(l)) => Ok(Some(l)),
            (None, Some(_)) => {
                // N4 补：行缺失 ≠ 桶不存在。有些桶行可能先于记账缺失（测试/控制台路径），
                // 真桶按「未开锁」400，真不存在的桶才 404（store 为存在性权威）。
                let exists = self
                    .store
                    .bucket_exists(bucket)
                    .await
                    .map_err(to_s3_error)?;
                if exists {
                    Err(s3s::s3_error!(
                        InvalidRequest,
                        "bucket object lock is not enabled"
                    ))
                } else {
                    Err(s3s::s3_error!(NoSuchBucket, "bucket not found"))
                }
            }
            (None, None) => Ok(None),
            (Some((true, mode, days)), None) => {
                if let (Some(m), Some(d)) = (mode, days) {
                    Ok(Some(crate::lock::default_retention_record(&m, d)))
                } else {
                    Ok(None)
                }
            }
            (Some((false, _, _)), None) => Ok(None),
        }
    }
}

/// nanos → S3 时间戳：`LastModified` 与 `CreationDate` 同为 `Timestamp` 别名，
/// 对象用 `mod_time_nanos`、桶用 `creation_nanos`。不要用 now() 冒充。
fn last_modified(nanos: i64) -> LastModified {
    LastModified::from(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(nanos.max(0) as u64))
}

/// 流上限包装：累计超限置 `exceeded`、读取出错置 `body_err`——随后由 [`stream_err_to_s3`]
/// 换成稳定错误码（不把 413 / IncompleteBody 的语义沉进内核错误类型）。
type LimitedStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, xl_store::error::Error>> + Send>>;

fn limit_stream(
    body: StreamingBlob,
    max: usize,
) -> (LimitedStream, Arc<AtomicBool>, Arc<AtomicBool>) {
    let exceeded = Arc::new(AtomicBool::new(false));
    let exceeded_in_stream = Arc::clone(&exceeded);
    let body_err = Arc::new(AtomicBool::new(false));
    let body_err_in_stream = Arc::clone(&body_err);
    let mut seen = 0usize;
    let stream = futures::StreamExt::map(body, move |chunk| {
        let chunk = chunk.map_err(|e| {
            tracing::warn!(error = ?e, "读取请求体失败");
            body_err_in_stream.store(true, Ordering::Relaxed);
            xl_store::error::Error::Io(std::io::Error::other(format!("request body: {e}")))
        })?;
        seen += chunk.len();
        if seen > max {
            exceeded_in_stream.store(true, Ordering::Relaxed);
            return Err(xl_store::error::Error::Io(std::io::Error::other(
                "request body exceeds max_put_bytes",
            )));
        }
        Ok(chunk)
    });
    (Box::pin(stream), exceeded, body_err)
}

/// [`limit_stream`] 的标志位 → 稳定错误码；无标志时走通用映射。
fn stream_err_to_s3(
    e: xl_store::error::Error,
    exceeded: &AtomicBool,
    body_err: &AtomicBool,
) -> s3s::S3Error {
    if exceeded.load(Ordering::Relaxed) {
        return s3s::s3_error!(EntityTooLarge, "object exceeds max_put_bytes");
    }
    if body_err.load(Ordering::Relaxed) {
        // 与 M1 口径一致：请求体读取失败 = 客户端侧不完整（而不是 500）
        return s3s::s3_error!(IncompleteBody, "failed to read request body");
    }
    to_s3_error(e)
}

/// M7：SQLite BEFORE 门控的 `RAISE(ABORT,'site replication conflict')` 探测。
fn is_site_conflict(e: &(impl std::fmt::Display + ?Sized)) -> bool {
    e.to_string().contains("site replication conflict")
}

/// 4.1：`x-oss-part-sizes` = 逗号分隔的各段明文长度（REPLICA 摄入专用）。
fn parse_part_sizes(h: Option<&http::HeaderValue>) -> S3Result<Option<Vec<u64>>> {
    let Some(h) = h else { return Ok(None) };
    let raw = h
        .to_str()
        .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid x-oss-part-sizes"))?;
    raw.split(',')
        .map(|s| {
            s.trim()
                .parse::<u64>()
                .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid x-oss-part-sizes"))
        })
        .collect::<S3Result<Vec<_>>>()
        .map(Some)
}

/// M8 前置：`x-amz-tagging`（URL query 串）→ 校验后的标签集合。
fn parse_tagging_header(raw: &str) -> S3Result<Vec<ObjectTag>> {
    let tags: Vec<ObjectTag> = crate::sts::parse_form(raw.as_bytes())
        .into_iter()
        .map(|(key, value)| ObjectTag { key, value })
        .collect();
    xl_store::meta::validate_tags(&tags).map_err(|e| s3s::s3_error!(InvalidTag, "{}", e))?;
    Ok(tags)
}

/// M8 S4a：存储类解析/校验。`allow_archive=false`（直写 PUT/MPU/Copy 目标）拒绝归档类。
fn parse_storage_class(raw: Option<&str>, allow_archive: bool) -> S3Result<Option<String>> {
    let Some(raw) = raw else { return Ok(None) };
    let sc = raw.to_ascii_uppercase();
    if !xl_store::meta::is_valid_storage_class(&sc) {
        return Err(s3s::s3_error!(InvalidStorageClass, "invalid storage class"));
    }
    if !allow_archive && xl_store::meta::is_archive_class(&sc) {
        return Err(s3s::s3_error!(
            InvalidStorageClass,
            "archive storage class requires lifecycle transition"
        ));
    }
    Ok(xl_store::meta::normalize_storage_class(Some(&sc)))
}

/// M8 S4b：生命周期错误矩阵 → S3 错误码。
fn lifecycle_error(e: crate::lifecycle::LifecycleError) -> s3s::S3Error {
    use crate::lifecycle::LifecycleError as E;
    match e {
        E::NoSuchBucket => s3s::s3_error!(NoSuchBucket),
        E::NoSuchConfiguration => s3s::s3_error!(NoSuchLifecycleConfiguration),
        E::Invalid(m) => s3s::s3_error!(InvalidRequest, "{}", m),
        E::InvalidClass(m) => s3s::s3_error!(InvalidStorageClass, "{}", m),
        E::InvalidArgument(m) => s3s::s3_error!(InvalidArgument, "{}", m),
        E::NotImplemented(m) => s3s::s3_error!(NotImplemented, "{}", m),
        // 503 细节（required/effective level）原样返回，便于运维定位滚动窗口。
        E::Unavailable(m) => {
            tracing::warn!(error = %m, "生命周期控制面不可用");
            s3s::s3_error!(ServiceUnavailable, "{}", m)
        }
        E::Internal(m) => {
            tracing::error!(error = %m, "生命周期存储失败");
            s3s::s3_error!(InternalError, "internal error")
        }
    }
}

/// M8 S5：Inventory 错误矩阵 → S3 错误码（AWS 无 NoSuchConfiguration 码，用自定义码 + 404）。
fn inventory_error(e: crate::inventory::InventoryError) -> s3s::S3Error {
    use crate::inventory::InventoryError as E;
    match e {
        E::NoSuchBucket => s3s::s3_error!(NoSuchBucket),
        E::NoSuchConfiguration => {
            let mut err = s3s::S3Error::with_message(
                s3s::S3ErrorCode::Custom("NoSuchConfiguration".into()),
                "the inventory configuration does not exist",
            );
            err.set_status_code(StatusCode::NOT_FOUND);
            err
        }
        E::InvalidArgument(m) => s3s::s3_error!(InvalidArgument, "{}", m),
        E::AccessDenied(m) => s3s::s3_error!(AccessDenied, "{}", m),
        E::NotImplemented(m) => s3s::s3_error!(NotImplemented, "{}", m),
        E::Internal(m) => {
            tracing::error!(error = %m, "inventory 存储失败");
            s3s::s3_error!(InternalError, "internal error")
        }
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// M9 S6c：控制写拒绝 → s3s 错误（不静默降级）。
fn bucket_control_error(e: BucketControlError) -> s3s::S3Error {
    match e {
        BucketControlError::NotFound => s3s::s3_error!(NoSuchBucket, "bucket not found"),
        // M-3：非建桶路径的 Conflict 是状态冲突，不是「同名桶已存在」。
        BucketControlError::Conflict => {
            s3s::s3_error!(
                OperationAborted,
                "bucket request conflicts with current state"
            )
        }
        BucketControlError::NotEmpty => s3s::s3_error!(BucketNotEmpty, "bucket is not empty"),
        BucketControlError::Invalid => s3s::s3_error!(InvalidRequest, "invalid bucket request"),
        BucketControlError::TooLarge => {
            s3s::s3_error!(EntityTooLarge, "control command exceeds raft limit")
        }
        BucketControlError::Unavailable => {
            s3s::s3_error!(ServiceUnavailable, "control plane unavailable")
        }
        // M8-0：SigV4 钉死 host，S3 面不重定向；503 + leader 提示在消息里。
        BucketControlError::NotLeader { endpoint } => {
            let msg = match endpoint {
                Some(endpoint) => {
                    format!("control plane not leader; retry on the leader at {endpoint}")
                }
                None => "control plane not leader; retry on the Raft leader".to_string(),
            };
            s3s::s3_error!(ServiceUnavailable, "{}", msg)
        }
        BucketControlError::Internal => {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InternalError, "internal error")
        }
    }
}

/// 建桶专用：apply 的 Conflict = 同名桶已存在（模型冲突在 apply 前已预检 409）。
fn bucket_create_error(e: BucketControlError) -> s3s::S3Error {
    match e {
        BucketControlError::Conflict => {
            s3s::s3_error!(BucketAlreadyExists, "bucket already exists")
        }
        other => bucket_control_error(other),
    }
}

/// 桶策略校验与 console `put_policy` 同口径：≤64 KiB、顶层对象、`PolicyDoc` 可解析、
/// ≤100 条 statement、语义校验通过。
fn validate_bucket_policy(body: &str) -> S3Result<()> {
    if body.len() > 64 * 1024 {
        return Err(s3s::s3_error!(InvalidRequest, "policy exceeds 64 KiB"));
    }
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(v) if v.is_object() => {}
        Ok(_) => {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "policy must be a JSON object"
            ));
        }
        Err(e) => {
            return Err(s3s::s3_error!(InvalidRequest, "policy parse error: {e}"));
        }
    }
    match serde_json::from_str::<crate::policy::PolicyDoc>(body) {
        Ok(doc) => {
            if doc.statement.len() > 100 {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "policy has too many statements (max 100)"
                ));
            }
            if let Err(m) = doc.validate() {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "policy validation failed: {m}"
                ));
            }
        }
        Err(e) => {
            return Err(s3s::s3_error!(InvalidRequest, "policy parse error: {e}"));
        }
    }
    Ok(())
}

/// M8 S4b：GET/HEAD 的 `x-amz-restore` 三态（Archived 无头；Restoring ongoing；
/// Restored 带到期时刻）。
fn restore_header(info: &ObjectInfo) -> Option<String> {
    match info.tier_phase? {
        xl_store::meta::TierPhase::Archived => None,
        xl_store::meta::TierPhase::Restoring => Some("ongoing-request=\"true\"".to_string()),
        xl_store::meta::TierPhase::Restored => {
            let exp = info.restore_expires_at?;
            let odt = time::OffsetDateTime::from_unix_timestamp(exp).ok()?;
            let mut buf = Vec::new();
            Timestamp::from(odt)
                .format(TimestampFormat::HttpDate, &mut buf)
                .ok()?;
            Some(format!(
                "ongoing-request=\"false\", expiry-date=\"{}\"",
                String::from_utf8(buf).ok()?
            ))
        }
    }
}

/// M8 前置：PutObjectTagging 的 XML TagSet → 校验后的标签集合。
fn tagging_from_dto(t: &Tagging) -> S3Result<Vec<ObjectTag>> {
    let tags: Vec<ObjectTag> = t
        .tag_set
        .iter()
        .map(|tag| ObjectTag {
            key: tag.key.clone().unwrap_or_default(),
            value: tag.value.clone().unwrap_or_default(),
        })
        .collect();
    xl_store::meta::validate_tags(&tags).map_err(|e| s3s::s3_error!(InvalidTag, "{}", e))?;
    Ok(tags)
}

/// 校验和算法（本计划覆盖 s3s 输入里显式声明的那几种；组合语义留 M2b）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChecksumAlgo {
    Md5,
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
    Sha512,
    Crc64Nvme,
}

impl ChecksumAlgo {
    fn name(self) -> &'static str {
        match self {
            Self::Md5 => "MD5",
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Sha512 => "SHA512",
            Self::Crc64Nvme => "CRC64NVME",
        }
    }

    fn enable(self, h: &mut ChecksumHasher) {
        match self {
            Self::Md5 => h.md5 = Some(Default::default()),
            Self::Crc32 => h.crc32 = Some(Default::default()),
            Self::Crc32c => h.crc32c = Some(Default::default()),
            Self::Sha1 => h.sha1 = Some(Default::default()),
            Self::Sha256 => h.sha256 = Some(Default::default()),
            Self::Sha512 => h.sha512 = Some(Default::default()),
            Self::Crc64Nvme => h.crc64nvme = Some(Default::default()),
        }
    }

    fn extract(self, c: &Checksum) -> Option<String> {
        match self {
            Self::Md5 => c.checksum_md5.clone(),
            Self::Crc32 => c.checksum_crc32.clone(),
            Self::Crc32c => c.checksum_crc32c.clone(),
            Self::Sha1 => c.checksum_sha1.clone(),
            Self::Sha256 => c.checksum_sha256.clone(),
            Self::Sha512 => c.checksum_sha512.clone(),
            Self::Crc64Nvme => c.checksum_crc64nvme.clone(),
        }
    }
}

/// 从请求里摘出「唯一」声明的校验和（Content-MD5 与 `x-amz-checksum-*` 家族）。
/// 多值并存且不同 → InvalidRequest（S3 未定义交叉校验，明确拒绝优于静默取一）；
/// 只声明算法而无值 → InvalidRequest（CLI 恒带值；无值无法校验，不猜）。
fn declared_checksum(
    input: &PutObjectInput,
) -> Result<Option<(ChecksumAlgo, String)>, s3s::S3Error> {
    let mut declared: Vec<(ChecksumAlgo, String)> = Vec::new();
    let mut push = |algo: ChecksumAlgo, v: &Option<String>| {
        if let Some(v) = v {
            declared.push((algo, v.clone()));
        }
    };
    push(ChecksumAlgo::Md5, &input.content_md5);
    push(ChecksumAlgo::Md5, &input.checksum_md5);
    push(ChecksumAlgo::Crc32, &input.checksum_crc32);
    push(ChecksumAlgo::Crc32c, &input.checksum_crc32c);
    push(ChecksumAlgo::Sha1, &input.checksum_sha1);
    push(ChecksumAlgo::Sha256, &input.checksum_sha256);
    push(ChecksumAlgo::Sha512, &input.checksum_sha512);
    push(ChecksumAlgo::Crc64Nvme, &input.checksum_crc64nvme);

    match declared.len() {
        0 => {
            if input.checksum_algorithm.is_some() {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "checksum algorithm declared without a value"
                ));
            }
            Ok(None)
        }
        1 => Ok(Some(declared.remove(0))),
        // 多个声明：**完全同值**（如 content_md5 与 x-amz-checksum-md5 同值）视为一份；
        // 任一不同 → 拒绝（S3 未定义交叉校验，明确拒绝优于静默取一；算法名进日志便于排查）
        _ => {
            let first = declared[0].clone();
            if declared.iter().all(|d| *d == first) {
                return Ok(Some(first));
            }
            tracing::warn!(
                algorithms = ?declared.iter().map(|(a, _)| a.name()).collect::<Vec<_>>(),
                "请求声明了多个不一致的校验和，拒绝"
            );
            Err(s3s::s3_error!(
                InvalidRequest,
                "conflicting checksums declared"
            ))
        }
    }
}

/// 校验和尾流：边转发边算声明算法的摘要；流结束后比对，不匹配在**流末尾注入错误**，
/// 让内核在提交点之前 fail-closed（tmp 清理走既有失败路径，提交协议零改动）。
struct ChecksumTailStream<S> {
    inner: S,
    algo: ChecksumAlgo,
    hasher: ChecksumHasher,
    expected_base64: String,
    done: bool,
}

impl<S> ChecksumTailStream<S> {
    fn new(inner: S, algo: ChecksumAlgo, expected_base64: String) -> Self {
        let mut hasher = ChecksumHasher::default();
        algo.enable(&mut hasher);
        Self {
            inner,
            algo,
            hasher,
            expected_base64,
            done: false,
        }
    }
}

impl<S> futures::Stream for ChecksumTailStream<S>
where
    S: futures::Stream<Item = Result<Bytes, xl_store::error::Error>> + Unpin,
{
    type Item = Result<Bytes, xl_store::error::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.hasher.update(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.done = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                this.done = true;
                let hasher = std::mem::take(&mut this.hasher);
                let got = this.algo.extract(&hasher.finalize()).unwrap_or_default();
                if got != this.expected_base64 {
                    return Poll::Ready(Some(Err(xl_store::error::Error::BadDigest(format!(
                        "checksum mismatch: expected {}, got {}",
                        this.expected_base64, got
                    )))));
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 校验和记录 → GET 输出字段（GET/HEAD 两条回写路径各一份，类型不同不强行抽象）。
fn apply_checksum_get(out: &mut GetObjectOutput, rec: &xl_store::meta::ChecksumRecord) {
    match rec.algorithm.as_str() {
        "MD5" => out.checksum_md5 = Some(rec.value_base64.clone()),
        "CRC32" => out.checksum_crc32 = Some(rec.value_base64.clone()),
        "CRC32C" => out.checksum_crc32c = Some(rec.value_base64.clone()),
        "SHA1" => out.checksum_sha1 = Some(rec.value_base64.clone()),
        "SHA256" => out.checksum_sha256 = Some(rec.value_base64.clone()),
        "SHA512" => out.checksum_sha512 = Some(rec.value_base64.clone()),
        "CRC64NVME" => out.checksum_crc64nvme = Some(rec.value_base64.clone()),
        other => tracing::warn!(algorithm = other, "落盘校验和算法未知，GET 不回写"),
    }
}

fn apply_checksum_head(out: &mut HeadObjectOutput, rec: &xl_store::meta::ChecksumRecord) {
    match rec.algorithm.as_str() {
        "MD5" => out.checksum_md5 = Some(rec.value_base64.clone()),
        "CRC32" => out.checksum_crc32 = Some(rec.value_base64.clone()),
        "CRC32C" => out.checksum_crc32c = Some(rec.value_base64.clone()),
        "SHA1" => out.checksum_sha1 = Some(rec.value_base64.clone()),
        "SHA256" => out.checksum_sha256 = Some(rec.value_base64.clone()),
        "SHA512" => out.checksum_sha512 = Some(rec.value_base64.clone()),
        "CRC64NVME" => out.checksum_crc64nvme = Some(rec.value_base64.clone()),
        other => tracing::warn!(algorithm = other, "落盘校验和算法未知，HEAD 不回写"),
    }
}

/// 条件请求求值（RFC 9110 §13.2.2 顺序）：If-Match → If-Unmodified-Since →
/// If-None-Match → If-Modified-Since，任一有决定即短路。
/// 返回 `Err(NOT_MODIFIED)`（304，调用方回空 body）或 `Err(PRECONDITION_FAILED)`（412）。
///
/// ponytail: 求值基于 head 快照——与随后读盘之间是 TOCTOU 窗（并发覆写时条件可能对着
/// 旧版本判、数据读到新版本）；无版本控制语义下接受，M4 版本控制落地时改为对版本求值。
fn eval_preconditions(
    info: &ObjectInfo,
    if_match: Option<&ETagCondition>,
    if_unmodified_since: Option<&Timestamp>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
) -> Result<(), StatusCode> {
    let etag_matches = |c: &ETagCondition| match c {
        // 能走到求值处，实体必存在 → `*` 恒真
        ETagCondition::Any => true,
        // 值比较、不区分强弱：我们的 ETag 恒为 Strong 无引号 hex，客户端回传任意强弱按值比。
        // ponytail: 弱校验语义（W/ 差异）不细分；有真实互操作诉求时按 RFC 9110 §8.8.3 收严
        ETagCondition::ETag(e) => e.value() == info.etag,
    };
    // ponytail: 时间条件按存盘纳秒与客户端时间直比——客户端回显的 Last-Modified 是秒精度，
    // 毫秒级零头会让 If-Modified-Since 偶发多回一次 200；RFC 按秒比，行为无害，留痕即可
    let last = last_modified(info.last_modified_nanos);
    if let Some(c) = if_match {
        if !etag_matches(c) {
            return Err(StatusCode::PRECONDITION_FAILED);
        }
    } else if let Some(t) = if_unmodified_since
        && last > *t
    {
        return Err(StatusCode::PRECONDITION_FAILED);
    }
    if let Some(c) = if_none_match {
        if etag_matches(c) {
            return Err(StatusCode::NOT_MODIFIED);
        }
    } else if let Some(t) = if_modified_since
        && last <= *t
    {
        return Err(StatusCode::NOT_MODIFIED);
    }
    Ok(())
}

#[async_trait::async_trait]
impl S3 for OssS3 {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = req.input.bucket;
        // M9：复制模式头（缺省 async）；sync 需能力开关就绪。
        // S3（I3）：`sync_ready` 由 oss-server 注入为
        // SYNC_READY && RAFT_READY && [raft] && !stretch（xl-s3 不反依赖该常量）。
        let replication_mode = match req.headers.get("x-oss-replication-mode") {
            None => "async",
            Some(v) => match v.to_str().unwrap_or("").trim() {
                "async" => "async",
                "sync" if self.sync_ready => "sync",
                "sync" => {
                    return Err(s3s::s3_error!(
                        InvalidRequest,
                        "sync replication mode is not available"
                    ));
                }
                other => {
                    return Err(s3s::s3_error!(
                        InvalidRequest,
                        "invalid replication mode: {}",
                        other
                    ));
                }
            },
        };
        // M9：对象锁与 sync 无跨站语义（retention/legal hold 只写本站）——建桶即拒。
        if replication_mode == "sync" && req.input.object_lock_enabled_for_bucket == Some(true) {
            return Err(s3s::s3_error!(
                ConditionalRequestConflict,
                "object lock is not supported on sync buckets"
            ));
        }
        // M9 S3：sync 依赖唯一数据 peer（0/多行 = 站点配对未就绪）。
        if replication_mode == "sync" {
            match self.acl.site_peer_count().await {
                Ok(1) => {}
                Ok(n) => {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "sync requires exactly one site peer (have {})",
                        n
                    ));
                }
                Err(e) => {
                    tracing::error!(error = %e, bucket, "读站点 peer 数失败");
                    return Err(s3s::s3_error!(InternalError, "internal error"));
                }
            }
        }
        // M9 S6c：Raft 模式一条命令建桶（owner+mode+versioning+lock 原子）。
        if let Some(sink) = &self.bucket_control {
            // 已存在（行或目录）→ 409 BucketAlreadyExists；apply 侧只做 create-or-converge。
            match self.acl.bucket_replication_mode_row(&bucket).await {
                Ok(Some(_)) => {
                    return Err(s3s::s3_error!(BucketAlreadyExists, "bucket already exists"));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, bucket, "读桶复制模式失败");
                    return Err(s3s::s3_error!(InternalError, "internal error"));
                }
            }
            if self
                .store
                .bucket_exists(&bucket)
                .await
                .map_err(to_s3_error)?
            {
                return Err(s3s::s3_error!(BucketAlreadyExists, "bucket already exists"));
            }
            let owner_uid = req
                .extensions
                .get::<crate::access::AuthedUser>()
                .and_then(|u| (!u.is_admin).then_some(u.uid));
            // 开锁建桶顺带 Enabled（与 enable_bucket_lock 的列效果一致）；sync 恒 Enabled。
            let (object_lock, versioning) =
                if req.input.object_lock_enabled_for_bucket == Some(true) {
                    (true, "Enabled")
                } else if replication_mode == "sync" {
                    (false, "Enabled")
                } else {
                    (false, "Off")
                };
            sink.create_bucket(BucketCreateSpec {
                name: bucket.clone(),
                owner_uid,
                created_at: now_secs(),
                replication_mode: replication_mode.to_string(),
                versioning: versioning.to_string(),
                object_lock,
                object_lock_default_mode: None,
                object_lock_default_days: None,
                default_encryption: None,
            })
            .await
            .map_err(bucket_create_error)?;
            self.store.invalidate_gate(&bucket);
            self.store.broadcast_bucket_row(&bucket, Some(0)).await;
            return Ok(S3Response::new(CreateBucketOutput::default()));
        }
        // M9：陈旧行重建不得静默改模式（与 console 同口径，先于物理建桶）。
        match self.acl.bucket_replication_mode_row(&bucket).await {
            Ok(Some(existing)) if existing != replication_mode => {
                return Err(s3s::s3_error!(
                    OperationAborted,
                    "bucket replication mode cannot be changed on recreate"
                ));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, bucket, "读桶复制模式失败");
                return Err(s3s::s3_error!(InternalError, "internal error"));
            }
        }
        self.store
            .create_bucket(&bucket)
            .await
            .map_err(to_s3_error)?;
        // 桶归属记账（授权门已确认调用者身份并塞了 AuthedUser）：建桶者即桶主。
        // C3：多 set 下缺省**自动指派**（桶数最少的 set；单 set 恒 0 零查询）。
        // 记账失败必须回滚物理桶并显式失败（M7 M1：不允许「物理已建 + 行缺失/门控拒」静默成功）。
        if let Some(user) = req.extensions.get::<crate::access::AuthedUser>().cloned() {
            let k = self.store.cluster().map_or(1, |c| c.sets.sets.len() as u16);
            let set = match self.acl.pick_least_loaded_set(k).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = ?e, bucket, "set 指派失败，回落 set0");
                    0
                }
            };
            if let Err(e) = self
                .acl
                .record_bucket_owner_set_mode(&bucket, user.uid, set, replication_mode)
                .await
            {
                self.rollback_created_bucket(&bucket).await;
                if is_site_conflict(&e) {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "site replication conflict"
                    ));
                }
                tracing::error!(error = ?e, bucket, "桶归属记账失败");
                return Err(s3s::s3_error!(InternalError, "internal error"));
            }
            // M5 切片 0（P4-1）：行变更广播（best-effort；关「别节点陈旧行」窗口）
            self.store.broadcast_bucket_row(&bucket, Some(set)).await;
        }
        // M9：门模式缓存失效必须在行写入之后（新建 sync 桶后首次写即生效）。
        self.store.invalidate_gate(&bucket);
        // M5 切片 4（L2）：建桶即开锁（AWS：x-amz-bucket-object-lock-enabled 顺带开版本控制）。
        // 必须在全列归属写之后：开锁只改 versioning/object_lock，归属写不会把它重置。
        if req.input.object_lock_enabled_for_bucket == Some(true) {
            if let Err(e) = self.acl.enable_bucket_lock(&bucket).await {
                // M7 M1：记账门控失败时按最终行补偿（远端 upsert 胜出则保留物理桶）。
                self.rollback_created_bucket(&bucket).await;
                if is_site_conflict(&e) {
                    return Err(s3s::s3_error!(
                        OperationAborted,
                        "site replication conflict"
                    ));
                }
                tracing::error!(error = %e, bucket, "开桶锁失败");
                return Err(s3s::s3_error!(InternalError, "internal error"));
            }
            // 行变更广播（lock/versioning 与归属行同批；best-effort，与 C3 同口径）
            self.store.broadcast_bucket_row(&bucket, None).await;
        }
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let bucket = req.input.bucket;
        match self.store.bucket_exists(&bucket).await {
            Ok(true) => {
                let mode = self
                    .acl
                    .bucket_replication_mode(&bucket)
                    .await
                    .map_err(|e| {
                        tracing::error!(error = %e, bucket, "读桶复制模式失败");
                        s3s::s3_error!(InternalError, "internal error")
                    })?;
                let mut resp = S3Response::new(HeadBucketOutput::default());
                if let Ok(v) = http::HeaderValue::from_str(&mode) {
                    resp.headers.insert(
                        http::header::HeaderName::from_static("x-oss-replication-mode"),
                        v,
                    );
                }
                Ok(resp)
            }
            Ok(false) => Err(s3s::s3_error!(NoSuchBucket)),
            Err(e) => Err(to_s3_error(e)),
        }
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = req.input.bucket;
        // M9 S3：sync 桶走双站 quiesce/查空/远端删除，async 桶保持 M7 本地删除。
        match &self.sync_delete {
            Some(s) => s.delete_bucket(&bucket).await,
            None => self.store.delete_bucket(&bucket).await,
        }
        .map_err(to_s3_error)?;
        // M9 S6c：Raft 的 BucketDelete 已在 apply 级联清表；这里只做本地缓存失效，
        // 不再直写受 guard 保护的表（否则 ABORT → 已提交删除被误报为失败）。
        if self.bucket_control.is_some() {
            self.store.invalidate_gate(&bucket);
            if let Some(sink) = &self.bucket_cleanup {
                sink.invalidate_ap_cache(&bucket);
            }
            return Ok(S3Response::new(DeleteBucketOutput::default()));
        }
        // 记账清理（授权门已确认桶主/管理员）：清授权行 + 桶行 + 生命周期规则
        if let Err(e) = self.acl.forget_bucket(&bucket).await {
            // M7 H1：门控 ABORT 时物理桶已删、行/授权未动——按最终行补偿后显式失败，
            // 不允许“物理已删 + 行残留 + 无 delete changelog”静默成功。
            if is_site_conflict(&e) {
                match self.acl.bucket_lock(&bucket).await {
                    // 远端 delete 已胜出（无行）：本地删除同样已生效，按幂等成功返回。
                    Ok(None) => {
                        self.store.invalidate_gate(&bucket);
                        self.cleanup_m8(&bucket).await;
                        return Ok(S3Response::new(DeleteBucketOutput::default()));
                    }
                    // 行仍在（远端 upsert/权限胜出）：恢复空桶并 409。
                    Ok(Some(_)) => {
                        if let Err(re) = self.store.create_bucket(&bucket).await {
                            tracing::warn!(error = %re, bucket, "冲突补偿重建空桶失败");
                        }
                    }
                    Err(re) => {
                        // 行状态未知：恢复空桶（宁可多一个空目录，也不让行/物理不一致）。
                        tracing::warn!(error = %re, bucket, "冲突补偿读桶行失败（恢复空桶）");
                        if let Err(x) = self.store.create_bucket(&bucket).await {
                            tracing::warn!(error = %x, bucket, "冲突补偿恢复空桶失败");
                        }
                    }
                }
                return Err(s3s::s3_error!(
                    OperationAborted,
                    "site replication conflict"
                ));
            }
            tracing::warn!(error = ?e, bucket, "删桶记账清理失败");
        }
        if let Err(e) = self.acl.clear_lifecycle_rules(&bucket).await {
            tracing::warn!(error = ?e, bucket, "删桶生命周期规则清理失败");
        }
        if let Err(e) = self.acl.clear_bucket_notification(&bucket).await {
            tracing::warn!(error = %e, bucket, "删桶清通知配置失败（同名重建可能继承）");
        }
        if let Err(e) = self.acl.clear_bucket_policy(&bucket).await {
            tracing::warn!(error = ?e, bucket, "删桶策略清理失败");
        }
        // M5 切片 0（P4-1）：删行广播（best-effort）
        self.store.broadcast_bucket_row(&bucket, None).await;
        // M9：删桶后门模式缓存失效（10s 内不再把写当 sync 拦截）
        self.store.invalidate_gate(&bucket);
        // M8 S0：控制面状态清理 + AP 缓存失效（best-effort）
        self.cleanup_m8(&bucket).await;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.store.list_buckets().await.map_err(to_s3_error)?;
        // 磁盘列表是权威（历史桶可能没有 DB 行）；非管理员与可见集求交
        let visible: Option<std::collections::HashSet<String>> =
            match req.extensions.get::<crate::access::AuthedUser>().cloned() {
                Some(u) if !u.is_admin => Some(
                    self.acl
                        .visible(u.uid)
                        .await
                        .map_err(crate::access::acl_err)?
                        .into_iter()
                        .collect(),
                ),
                _ => None,
            };
        let list = buckets
            .into_iter()
            .filter(|b| visible.as_ref().is_none_or(|v| v.contains(&b.name)))
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(last_modified(b.creation_nanos)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(list),
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        // 先于解构求值（只借 input）：解构会部分搬走 metadata 等字段
        let declared = declared_checksum(&req.input)?;
        // M5 切片 3：SSE 头解析（R1/R2）；密钥对象随之进入 store（绝不落盘）
        let mut sse = parse_sse(
            req.input.sse_customer_algorithm.as_deref(),
            req.input.sse_customer_key.as_deref(),
            req.input.sse_customer_key_md5.as_deref(),
            req.input
                .server_side_encryption
                .as_ref()
                .map(|s| s.as_str()),
            req.input.ssekms_key_id.as_deref(),
        )?;
        // M8 缺口 C：无显式 SSE 时套用桶默认（显式头永远优先）
        self.apply_default_encryption(&req.input.bucket, &mut sse)
            .await?;
        // 3.16：显式/默认 CMK 都必须在本节点密钥环（未知名 400）
        self.ensure_cmk_available(&sse)?;
        // M5 切片 4（L3）：对象锁头（桶未开锁带锁头 → resolve_lock 400）
        let lock_req = parse_put_lock(
            req.input.object_lock_mode.as_ref().map(|m| m.as_str()),
            req.input.object_lock_retain_until_date.as_ref(),
            req.input
                .object_lock_legal_hold_status
                .as_ref()
                .map(|s| s.as_str()),
        )?;
        let lock = self.resolve_lock(&req.input.bucket, lock_req).await?;
        // M6 切片 0：事件来源（解构 req.input 前自 headers 取）
        let event_source = EventSource::from_headers(&req.headers);
        // M6 切片 3：REPLICA 摄入（复制源 PutObject 带 x-amz-replication-status: REPLICA）。
        // O-2：REPLICA 状态会抑制复制（防环），因此只信 site token 认证的内部请求；
        // 普通 S3 客户端的同名头按普通写入处理，不能借此抑制复制。
        let replica_hdr = Self::replica_requested(&req.headers);
        let token_ok = self.site_token_ok(&req.headers);
        let replica = replica_hdr && token_ok;
        if replica_hdr && !token_ok {
            tracing::warn!(bucket = %req.input.bucket, "忽略未认证的 x-amz-replication-status: REPLICA");
        }
        // 4.1：复制摄入可带 MPU 段长表（仅 REPLICA 接受；store 校验总和 == 实际大小）
        let part_sizes = if replica {
            parse_part_sizes(req.headers.get("x-oss-part-sizes"))?
        } else {
            None
        };
        // M9：sync 桶写边界（SSE-C 承诺客户端密钥双站可读；跨站对象锁语义未定义）。
        let sync = self
            .store
            .sync_mode(&req.input.bucket)
            .await
            .map_err(to_s3_error)?;
        // O-2/M9：sync 桶上未认证的 REPLICA 仍按原边界 403（不静默降级为普通写入）；
        // 该检查先于下面的非摄入限制，保持原有错误优先级。
        if sync && replica_hdr && !token_ok {
            return Err(s3s::s3_error!(AccessDenied, "site token required"));
        }
        if sync && !replica {
            if matches!(sse.spec, Some(xl_store::crypto::EncSpec::SseC(_))) {
                return Err(s3s::s3_error!(
                    InvalidArgument,
                    "SSE-C is not supported on sync buckets"
                ));
            }
            if lock.as_ref().is_some_and(|l| !l.is_empty()) {
                return Err(s3s::s3_error!(
                    ConditionalRequestConflict,
                    "object lock is not supported on sync buckets"
                ));
            }
        }
        // M9：sync 桶的 REPLICA 摄入必须带匹配 site token + origin id；采用 origin UUID
        // 作本地版本 id（双站同 id），且不再 gate/再复制（防环）。
        // 4.16：异步站点复制（非 sync 桶）带 origin id 时同样强制同 id（版本删除才能定点映射），
        // 但必须由 site token 认证——否则任意 REPLICA 客户端可伪造版本 id。
        let origin_id = req
            .headers
            .get("x-oss-origin-version-id")
            .and_then(|v| v.to_str().ok());
        let mut forced_version_id = None;
        if replica && sync {
            forced_version_id = Some(
                origin_id
                    .ok_or_else(|| s3s::s3_error!(InvalidRequest, "origin version id required"))?
                    .to_string(),
            );
        } else if replica_hdr && origin_id.is_some() {
            // 混合版本保护：带同源 id 却无有效令牌 → 明确拒绝（不静默丢弃 id 产生错位版本）。
            if !token_ok {
                return Err(s3s::s3_error!(AccessDenied, "site token required"));
            }
            forced_version_id = origin_id.map(str::to_string);
        }
        // M8 S4a：存储类随写保留（4.19：M6 远端复制同样携带；归档类直写拒）。
        let storage_class =
            parse_storage_class(req.input.storage_class.as_ref().map(|s| s.as_str()), false)?;
        let PutObjectInput {
            bucket,
            key,
            body,
            content_type,
            content_length,
            metadata,
            tagging,
            ..
        } = req.input;
        let body = body.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing body"))?;
        // M8 前置：x-amz-tagging（URL query 串）。4.19：M6 远端复制也携带标签（与本地版本一致）。
        let tags = match tagging.as_deref() {
            Some(raw) if !raw.is_empty() => Some(parse_tagging_header(raw)?),
            _ => None,
        };

        // 声明长度已知且超限：不读体直接 413
        if let Some(cl) = content_length {
            if cl < 0 {
                return Err(s3s::s3_error!(InvalidRequest, "negative content-length"));
            }
            if cl as usize > self.max_put_bytes {
                return Err(s3s::s3_error!(
                    EntityTooLarge,
                    "object exceeds max_put_bytes"
                ));
            }
        }

        // 流式直通内核（不再整份进内存）；content-length 可缺省/不可信，流上计数兜底。
        let (stream, exceeded, body_err) = limit_stream(body, self.max_put_bytes);
        // 校验和包装在限流之外：限流先标注 413/断流，校验和流再验证；
        // 注入的 BadDigest 不会被限流标志吞掉（标志只由限流层的 map 置位）。
        let (stream, checksum_record): (LimitedStream, Option<ChecksumRecord>) = match declared {
            Some((algo, expected)) => {
                let rec = ChecksumRecord {
                    algorithm: algo.name().to_string(),
                    value_base64: expected.clone(),
                };
                (
                    Box::pin(ChecksumTailStream::new(stream, algo, expected)),
                    Some(rec),
                )
            }
            None => (stream, None),
        };

        let mode = self.versioning_mode(&bucket).await?;
        let result = self
            .store
            .put_object_stream_with_meta_ext(
                &bucket,
                &key,
                stream,
                content_type,
                metadata.unwrap_or_default().into_iter().collect(),
                checksum_record,
                mode,
                sse.spec, // M5 切片 3：SSE 头映射（R1/R2）
                lock,     // M5 切片 4（L3）
                tags,     // M8 前置：写入版本标签
                storage_class,
                xl_store::store::PutExt {
                    replica,
                    forced_version_id,
                    part_sizes,
                },
            )
            .await;
        let info = match result {
            Ok(info) => info,
            Err(e) => return Err(stream_err_to_s3(e, &exceeded, &body_err)),
        };
        self.emit(
            S3EventOp::CreatedPut,
            &bucket,
            &key,
            info.version_id.as_deref(),
            Some(info.size),
            Some(&info.etag),
            event_source,
        )
        .await;
        if replica {
            self.store
                .set_replication_status(
                    &bucket,
                    &key,
                    info.version_id.as_deref().unwrap_or("null"),
                    Some("REPLICA"),
                )
                .await
                .map_err(to_s3_error)?;
        }
        // M6 切片 4：复制入队（REPLICA 摄入不再复制——防环）
        self.repl_on_write(crate::replication::WriteInfo {
            bucket: &bucket,
            key: &key,
            version_id: info.version_id.as_deref(),
            etag: &info.etag,
            size: info.size,
            is_delete_marker: false,
            is_version_delete: false,
            is_replica: replica,
        })
        .await;

        let so = sse_out(info.encryption.as_ref());
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(info.etag)),
            version_id: info.version_id.clone(),
            server_side_encryption: so
                .server_side_encryption
                .map(ServerSideEncryption::from_static),
            ssekms_key_id: so.kms_key_id,
            sse_customer_algorithm: so.sse_c_algorithm.map(str::to_string),
            sse_customer_key_md5: so.sse_c_key_md5, // P2-a 复验补：PUT 响应回 key-MD5
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        // M5 切片 3（R5）：SSE-C 只在暂存 meta 记 md5（钥绝不落盘）；其余模式在 complete 定。
        let mut sse = parse_sse(
            req.input.sse_customer_algorithm.as_deref(),
            req.input.sse_customer_key.as_deref(),
            req.input.sse_customer_key_md5.as_deref(),
            req.input
                .server_side_encryption
                .as_ref()
                .map(|s| s.as_str()),
            req.input.ssekms_key_id.as_deref(),
        )?;
        // M8 缺口 C：无显式 SSE 时套用桶默认（create 记档，complete 落地）
        self.apply_default_encryption(&req.input.bucket, &mut sse)
            .await?;
        // 3.16：CMK 在 create 即须存在（AWS 语义；complete 只按 sse_kind 落地）
        self.ensure_cmk_available(&sse)?;
        // M5 切片 4（L3）：对象锁头（桶未开锁带锁头 → resolve_lock 400）
        let lock_req = parse_put_lock(
            req.input.object_lock_mode.as_ref().map(|m| m.as_str()),
            req.input.object_lock_retain_until_date.as_ref(),
            req.input
                .object_lock_legal_hold_status
                .as_ref()
                .map(|s| s.as_str()),
        )?;
        let lock = self.resolve_lock(&req.input.bucket, lock_req).await?;
        // M9：sync 桶写边界（同 PUT；SSE-C/锁意图都不得进入 mpu.meta）
        if self
            .store
            .sync_mode(&req.input.bucket)
            .await
            .map_err(to_s3_error)?
        {
            if matches!(sse.spec, Some(xl_store::crypto::EncSpec::SseC(_))) {
                return Err(s3s::s3_error!(
                    InvalidArgument,
                    "SSE-C is not supported on sync buckets"
                ));
            }
            if lock.as_ref().is_some_and(|l| !l.is_empty()) {
                return Err(s3s::s3_error!(
                    ConditionalRequestConflict,
                    "object lock is not supported on sync buckets"
                ));
            }
        }
        // M8 S4a（B12）：Create 声明的存储类随 mpu.meta 持久化，Complete 落地；归档类拒
        let storage_class =
            parse_storage_class(req.input.storage_class.as_ref().map(|s| s.as_str()), false)?;
        let CreateMultipartUploadInput {
            bucket,
            key,
            content_type,
            metadata,
            tagging,
            ..
        } = req.input;
        // M8 缺口 A：Create 的 x-amz-tagging 随 mpu.meta 持久化，complete 落地（同 PUT 口径）
        let tags = match tagging.as_deref() {
            Some(raw) if !raw.is_empty() => Some(parse_tagging_header(raw)?),
            _ => None,
        };
        // R5 扩展（P2-b）：SSE-S3/KMS 意图持久化，complete 落地；3.16 显式 CMK 编码为
        // `sse-kms:<name>`（旧二进制按未知 kind 拒绝 = fail-closed）。
        let sse_kind = sse.sse_kind();
        let upload_id = self
            .store
            .create_multipart_upload_ext(
                &bucket,
                &key,
                content_type,
                metadata.unwrap_or_default().into_iter().collect(),
                sse.sse_c_md5(),
                sse_kind.as_deref(),
                lock.as_ref(), // M5 切片 4（L3）：锁意图持久化，complete 落地
                storage_class,
                tags,
            )
            .await
            .map_err(to_s3_error)?;
        let so = sse_out_from_spec(&sse);
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            server_side_encryption: so
                .server_side_encryption
                .map(ServerSideEncryption::from_static),
            ssekms_key_id: so.kms_key_id,
            sse_customer_algorithm: so.sse_c_algorithm.map(str::to_string),
            sse_customer_key_md5: so.sse_c_key_md5,
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        // M5 切片 3（R5）：UploadPart 携带的 SSE-C 只做一致性核对（md5 vs create 记录）；
        // 该 DTO 无 server_side_encryption/ssekms_key_id 字段（AWS 调用惯例亦然）。
        let sse = parse_sse(
            req.input.sse_customer_algorithm.as_deref(),
            req.input.sse_customer_key.as_deref(),
            req.input.sse_customer_key_md5.as_deref(),
            None,
            None,
        )?;
        let UploadPartInput {
            bucket,
            key,
            upload_id,
            part_number,
            body,
            content_length,
            ..
        } = req.input;
        if !(1..=10_000).contains(&part_number) {
            return Err(s3s::s3_error!(InvalidArgument, "part number out of range"));
        }
        let body = body.ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing body"))?;
        // 单 part 上限沿用 max_put_bytes（multipart 大对象整体不受它约束、按 part 计）
        if let Some(cl) = content_length {
            if cl < 0 {
                return Err(s3s::s3_error!(InvalidRequest, "negative content-length"));
            }
            if cl as usize > self.max_put_bytes {
                return Err(s3s::s3_error!(EntityTooLarge, "part exceeds max_put_bytes"));
            }
        }
        let (stream, exceeded, body_err) = limit_stream(body, self.max_put_bytes);
        let etag = match self
            .store
            .upload_part_ext(
                &bucket,
                &key,
                &upload_id,
                part_number as u16,
                stream,
                sse.sse_c_md5(),
            )
            .await
        {
            Ok(etag) => etag,
            Err(e) => return Err(stream_err_to_s3(e, &exceeded, &body_err)),
        };
        let so = sse_out_from_spec(&sse);
        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(ETag::Strong(etag)),
            server_side_encryption: so
                .server_side_encryption
                .map(ServerSideEncryption::from_static),
            ssekms_key_id: so.kms_key_id,
            sse_customer_algorithm: so.sse_c_algorithm.map(str::to_string),
            sse_customer_key_md5: so.sse_c_key_md5, // P2-a 复验补
            ..Default::default()
        }))
    }

    /// UploadPartCopy：主体在 `upload_part_copy.rs`（同上，只转发）。
    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        upload_part_copy::upload_part_copy(self, req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let event_source = EventSource::from_headers(&req.headers);
        // M5 切片 3（R5）：complete 的 SSE-C 决定最终加密（单流）且须与 create 同钥；
        // 该 DTO 无 server_side_encryption/ssekms_key_id 字段（SSE-S3/KMS 由 create 头记档，
        // complete 以 SSE-C md5 一致性为主——R5 记档）。
        let sse = parse_sse(
            req.input.sse_customer_algorithm.as_deref(),
            req.input.sse_customer_key.as_deref(),
            req.input.sse_customer_key_md5.as_deref(),
            None,
            None,
        )?;
        let CompleteMultipartUploadInput {
            bucket,
            key,
            upload_id,
            multipart_upload,
            ..
        } = req.input;
        // M9：sync 桶不接受 SSE-C（create 已拒；此处兜底 complete 携带密钥的直调）
        if matches!(sse.spec, Some(xl_store::crypto::EncSpec::SseC(_)))
            && self.store.sync_mode(&bucket).await.map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "SSE-C is not supported on sync buckets"
            ));
        }
        let Some(mu) = multipart_upload else {
            return Err(s3s::s3_error!(InvalidRequest, "missing multipart_upload"));
        };
        let Some(parts) = mu.parts else {
            return Err(s3s::s3_error!(InvalidRequest, "missing parts"));
        };
        let mut pairs = Vec::with_capacity(parts.len());
        for p in &parts {
            // 缺字段一并按 InvalidPart（规格只写了 ETag 不一致的情形，缺字段同属客户端报错）
            let (Some(pn), Some(etag)) = (p.part_number, p.e_tag.as_ref()) else {
                return Err(s3s::s3_error!(InvalidPart, "part missing number or etag"));
            };
            if !(1..=10_000).contains(&pn) {
                return Err(s3s::s3_error!(InvalidPart, "part number out of range"));
            }
            pairs.push((pn as u16, etag.value().to_string()));
        }
        let mode = self.versioning_mode(&bucket).await?;
        let info = self
            .store
            .complete_multipart_upload_versioned(
                &bucket,
                &key,
                &upload_id,
                &pairs,
                mode,
                sse.spec.clone(),
                None,
            )
            .await
            .map_err(to_s3_error)?;
        self.emit(
            S3EventOp::CreatedCompleteMultipartUpload,
            &bucket,
            &key,
            info.version_id.as_deref(),
            Some(info.size),
            Some(&info.etag),
            event_source,
        )
        .await;
        // M6 切片 4：MPU 提交同样入队（MPU 源 ETag 非 MD5，目标侧重算——断言按单段源跳过）
        self.repl_on_write(crate::replication::WriteInfo {
            bucket: &bucket,
            key: &key,
            version_id: info.version_id.as_deref(),
            etag: &info.etag,
            size: info.size,
            is_delete_marker: false,
            is_version_delete: false,
            is_replica: false,
        })
        .await;
        let so = sse_out(info.encryption.as_ref());
        let mut resp = S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(ETag::Strong(info.etag)),
            version_id: info.version_id.clone(),
            server_side_encryption: so
                .server_side_encryption
                .map(ServerSideEncryption::from_static),
            ssekms_key_id: so.kms_key_id,
            ..Default::default()
        });
        // CompleteMultipartUploadOutput 无 SSE-C 字段 → 头补 algorithm + key-MD5
        if let Some(md5) = so.sse_c_key_md5 {
            resp.headers = sse_c_headers(&md5);
        }
        Ok(resp)
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.store
            .abort_multipart_upload(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let bucket = req.input.bucket;
        let key = req.input.key;
        let upload_id = req.input.upload_id;
        let rows = self
            .store
            .list_parts(&bucket, &key, &upload_id)
            .await
            .map_err(to_s3_error)?;
        let parts = rows
            .into_iter()
            .map(|p| Part {
                part_number: Some(p.part_number as i32),
                size: Some(p.size as i64),
                e_tag: Some(ETag::Strong(p.etag)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            parts: Some(parts),
            // 分页不做：≤10000 全量
            is_truncated: Some(false),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let bucket = req.input.bucket;
        let key = req.input.key;
        let version_id = req.input.version_id.clone();
        let part_number = req.input.part_number;
        // M5 切片 3：SSE-C 读钥（GET 只有 customer 三件套；服务端校验在 store 读路径）
        let sse = parse_sse(
            req.input.sse_customer_algorithm.as_deref(),
            req.input.sse_customer_key.as_deref(),
            req.input.sse_customer_key_md5.as_deref(),
            None,
            None,
        )?;
        // 冻结裁决：partNumber + Range 组合不做（part 内相对区间留后续）
        if part_number.is_some() && req.input.range.is_some() {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "partNumber and Range cannot be used together"
            ));
        }

        // 条件求值需要 info：统一先取一次 head（Range 分支本来也要 size）；条件失败不读盘上数据。
        // M4：带 versionId 时点读该版本（marker 版本 → 405 DeleteMarker，经 to_s3_error）。
        let mut info = match &version_id {
            Some(v) => {
                self.store
                    .head_object_version_ext(&bucket, &key, v, sse.key.as_ref())
                    .await
            }
            None => {
                self.store
                    .head_object_ext(&bucket, &key, sse.key.as_ref())
                    .await
            }
        }
        .map_err(to_s3_error)?;
        // M8 S4b：过期恢复在此兜底回收（GET 仍按 InvalidObjectState 403）。
        self.cleanup_expired_restore(&bucket, &key, &info).await;
        if let Err(status) = eval_preconditions(
            &info,
            req.input.if_match.as_ref(),
            req.input.if_unmodified_since.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since.as_ref(),
        ) {
            if status == StatusCode::NOT_MODIFIED {
                // 304 无 body（空 output 即可）
                return Ok(S3Response::with_status(
                    GetObjectOutput::default(),
                    StatusCode::NOT_MODIFIED,
                ));
            }
            return Err(s3s::s3_error!(PreconditionFailed, "precondition failed"));
        }

        // partNumber → 段区间读（206 + Content-Range + parts_count）；Range → 有界读 +
        // 206/416；无 Range → 整读 + 200。三条路共用同一份响应组装。
        let mut parts_count = None;
        let (bytes, content_range, status) = if let Some(pn) = part_number {
            let (part_info, bytes, span, n) = self
                .store
                .get_object_part_ext(&bucket, &key, version_id.as_deref(), pn, sse.key.as_ref())
                .await
                .map_err(to_s3_error)?;
            info = part_info;
            parts_count = Some(n as i32);
            let cr = format!("bytes {}-{}/{}", span.start, span.end - 1, info.size);
            (bytes, Some(cr), StatusCode::PARTIAL_CONTENT)
        } else {
            match req.input.range {
                Some(range) => {
                    // 越界 → RangeNotSatisfiable → InvalidRange(416)
                    let rr = range.check(info.size).map_err(s3s::S3Error::from)?;
                    let (start, end) = (rr.start, rr.end);
                    // 归档对象：先试 Range 直读（仅 Archived；不可用回退既有 403）。
                    // 用 HEAD 得到的有效版本号（无 versionId 的 GET 也指向当前版本）。
                    let direct = if info.tier_phase == Some(xl_store::meta::TierPhase::Archived) {
                        self.tier_direct_range(
                            &bucket,
                            &key,
                            info.version_id.as_deref(),
                            &info,
                            start..end,
                        )
                        .await?
                    } else {
                        None
                    };
                    let bytes = if let Some(b) = direct {
                        b
                    } else if start == 0 && end == info.size {
                        // 显式全段 Range：仍回 206（S3 语义），数据走整读
                        match &version_id {
                            Some(v) => {
                                self.store
                                    .get_object_version_ext(&bucket, &key, v, sse.key.as_ref())
                                    .await
                            }
                            None => {
                                self.store
                                    .get_object_ext(&bucket, &key, sse.key.as_ref())
                                    .await
                            }
                        }
                        .map_err(to_s3_error)?
                        .1
                    } else {
                        match &version_id {
                            Some(v) => {
                                self.store
                                    .get_object_range_version_ext(
                                        &bucket,
                                        &key,
                                        v,
                                        start..end,
                                        sse.key.as_ref(),
                                    )
                                    .await
                            }
                            None => {
                                self.store
                                    .get_object_range_ext(
                                        &bucket,
                                        &key,
                                        start..end,
                                        sse.key.as_ref(),
                                    )
                                    .await
                            }
                        }
                        .map_err(to_s3_error)?
                    };
                    // check 保证 end >= 1（start <= last < full_length 且区间非空）
                    let cr = format!("bytes {}-{}/{}", start, end - 1, info.size);
                    (bytes, Some(cr), StatusCode::PARTIAL_CONTENT)
                }
                None => {
                    let bytes = match &version_id {
                        Some(v) => {
                            self.store
                                .get_object_version_ext(&bucket, &key, v, sse.key.as_ref())
                                .await
                        }
                        None => {
                            self.store
                                .get_object_ext(&bucket, &key, sse.key.as_ref())
                                .await
                        }
                    }
                    .map_err(to_s3_error)?
                    .1;
                    (bytes, None, StatusCode::OK)
                }
            }
        };
        let len = bytes.len() as i64;
        let restore = restore_header(&info);
        // 读路径补全：成功 200/206 才发（304/416/错误已在上面提前返回）
        self.emit(
            S3EventOp::AccessedGet,
            &bucket,
            &key,
            info.version_id.as_deref(),
            Some(info.size),
            Some(&info.etag),
            EventSource::from_headers(&req.headers),
        )
        .await;

        let mut out = GetObjectOutput {
            body: Some(StreamingBlob::from_bytes(bytes)),
            content_length: Some(len),
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(last_modified(info.last_modified_nanos)),
            accept_ranges: Some("bytes".to_string()),
            content_range,
            parts_count,
            // M8 S4b：x-amz-restore 三态
            restore,
            // 空集也回 Some(空)：AWS 语义下与 None 无差别，客户端按「无自定义元数据」处理
            metadata: Some(info.user_metadata.into_iter().collect()),
            // 透出实际读到的版本号（点名版本 = 该版本；未点名 = 当前最新）
            version_id: info.version_id.clone(),
            // M8 前置：有标签时回 x-amz-tagging-count（HeadObject 同）
            tag_count: (!info.tags.is_empty()).then_some(info.tags.len() as i32),
            // M8 S4a：存储类（None = STANDARD，s3s 序列化缺省）
            storage_class: info
                .storage_class
                .as_ref()
                .map(|s| StorageClass::from(s.clone())),
            ..Default::default()
        };
        // response-content-* 是响应头覆盖：客户端给什么值就原样下发（含 &filename= 的 RFC 5987
        // 编码由客户端负责——服务端无编码逻辑，见规格 §3.2）。
        if let Some(v) = req.input.response_content_type {
            out.content_type = Some(v);
        }
        if let Some(v) = req.input.response_content_disposition {
            out.content_disposition = Some(v);
        }
        if let Some(v) = req.input.response_cache_control {
            out.cache_control = Some(v);
        }
        if let Some(v) = req.input.response_content_language {
            out.content_language = Some(v);
        }
        if let Some(v) = req.input.response_content_encoding {
            out.content_encoding = Some(v);
        }
        if let Some(ts) = req.input.response_expires {
            // Timestamp::format 吃 io::Write；String 只有 fmt::Write——用 Vec<u8> 缓冲再转
            let mut buf = Vec::new();
            ts.format(TimestampFormat::HttpDate, &mut buf)
                .expect("HttpDate 格式化到 Vec 不会失败");
            out.expires = Some(String::from_utf8(buf).expect("HttpDate 只含 ASCII"));
        }

        // 校验和回写（GET）：仅整读（200）——206 的 body 只是片段，回整对象校验和会让
        // 客户端的响应校验（新版 AWS SDK 默认开启）必然失败；S3 对分段响应也不返回整对象校验和。
        if status != StatusCode::PARTIAL_CONTENT
            && let Some(rec) = &info.checksum
        {
            apply_checksum_get(&mut out, rec);
        }

        // M5 切片 4（L6）：对象锁响应头（GET）
        apply_lock_out(
            info.lock.as_ref(),
            &mut out.object_lock_mode,
            &mut out.object_lock_retain_until_date,
            &mut out.object_lock_legal_hold_status,
        );

        // M6 切片 3：复制状态（x-amz-replication-status）
        out.replication_status = info.replication_status.clone().map(ReplicationStatus::from);
        // M5 切片 3（R6）：SSE 响应头（S3/KMS 走字段；SSE-C 的 key-MD5 走 headers 补）
        let so = sse_out(info.encryption.as_ref());
        out.server_side_encryption = so
            .server_side_encryption
            .map(ServerSideEncryption::from_static);
        out.ssekms_key_id = so.kms_key_id;
        out.sse_customer_algorithm = so.sse_c_algorithm.map(str::to_string);
        let mut resp = S3Response::with_status(out, status);
        if let Some(md5) = so.sse_c_key_md5 {
            resp.headers = sse_c_key_md5_header(&md5);
        }
        Ok(resp)
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let version_id = req.input.version_id.clone();
        // M5 切片 3：SSE-C 校验（HEAD 不解密，只对记录校 key-MD5——由 store 读路径执行）
        let sse = parse_sse(
            req.input.sse_customer_algorithm.as_deref(),
            req.input.sse_customer_key.as_deref(),
            req.input.sse_customer_key_md5.as_deref(),
            None,
            None,
        )?;
        // 冻结裁决：partNumber + Range 组合不做（与 GET 同口径）
        if req.input.part_number.is_some() && req.input.range.is_some() {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "partNumber and Range cannot be used together"
            ));
        }
        let mut info = match &version_id {
            Some(v) => {
                self.store
                    .head_object_version_ext(&req.input.bucket, &req.input.key, v, sse.key.as_ref())
                    .await
            }
            None => {
                self.store
                    .head_object_ext(&req.input.bucket, &req.input.key, sse.key.as_ref())
                    .await
            }
        }
        .map_err(to_s3_error)?;
        // M8 S4b：过期恢复兜底（HEAD 元数据仍可读，响应头按过期态给 ongoing=false）。
        self.cleanup_expired_restore(&req.input.bucket, &req.input.key, &info)
            .await;
        // 条件与 GET 同口径（§3.3 的「错误一律 404」只罩读错误；304/412 是条件结果，照常返回）
        if let Err(status) = eval_preconditions(
            &info,
            req.input.if_match.as_ref(),
            req.input.if_unmodified_since.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since.as_ref(),
        ) {
            if status == StatusCode::NOT_MODIFIED {
                return Ok(S3Response::with_status(
                    HeadObjectOutput::default(),
                    StatusCode::NOT_MODIFIED,
                ));
            }
            return Err(s3s::s3_error!(PreconditionFailed, "precondition failed"));
        }
        // partNumber → 段区间（200 + 段 Content-Length/Content-Range + parts_count）。
        let mut content_length = info.size as i64;
        let mut content_range = None;
        let mut parts_count = None;
        if let Some(pn) = req.input.part_number {
            let (part_info, span, n) = self
                .store
                .object_part_span(
                    &req.input.bucket,
                    &req.input.key,
                    version_id.as_deref(),
                    pn,
                    sse.key.as_ref(),
                )
                .await
                .map_err(to_s3_error)?;
            info = part_info;
            content_length = (span.end - span.start) as i64;
            content_range = Some(format!(
                "bytes {}-{}/{}",
                span.start,
                span.end - 1,
                info.size
            ));
            parts_count = Some(n as i32);
        } else if let Some(range) = &req.input.range {
            // HEAD + Range：仍 200（HEAD 无 body/无 206），但头部按区间收窄；越界 416。
            let rr = range.check(info.size).map_err(s3s::S3Error::from)?;
            content_length = (rr.end - rr.start) as i64;
            content_range = Some(format!("bytes {}-{}/{}", rr.start, rr.end - 1, info.size));
        }
        let restore = restore_header(&info);
        // 读路径补全：HEAD 成功 200 才发（304/条件失败/错误已提前返回）
        self.emit(
            S3EventOp::AccessedHead,
            &req.input.bucket,
            &req.input.key,
            info.version_id.as_deref(),
            Some(info.size),
            Some(&info.etag),
            EventSource::from_headers(&req.headers),
        )
        .await;
        let mut out = HeadObjectOutput {
            content_length: Some(content_length),
            content_range,
            parts_count,
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(last_modified(info.last_modified_nanos)),
            // M8 S4b：x-amz-restore 三态
            restore,
            metadata: Some(info.user_metadata.into_iter().collect()),
            version_id: info.version_id.clone(),
            tag_count: (!info.tags.is_empty()).then_some(info.tags.len() as i32),
            storage_class: info
                .storage_class
                .as_ref()
                .map(|s| StorageClass::from(s.clone())),
            ..Default::default()
        };
        // 校验和回写（HEAD）：仅整对象（Range/partNumber 的头部按段收窄，回整对象校验和会误导客户端）
        if out.content_range.is_none()
            && let Some(rec) = &info.checksum
        {
            apply_checksum_head(&mut out, rec);
        }
        // M5 切片 4（L6）：对象锁响应头（HEAD）
        apply_lock_out(
            info.lock.as_ref(),
            &mut out.object_lock_mode,
            &mut out.object_lock_retain_until_date,
            &mut out.object_lock_legal_hold_status,
        );
        // M6 切片 3：复制状态（x-amz-replication-status）
        out.replication_status = info.replication_status.clone().map(ReplicationStatus::from);
        // M5 切片 3（R6）：SSE 响应头（同 GET）
        let so = sse_out(info.encryption.as_ref());
        out.server_side_encryption = so
            .server_side_encryption
            .map(ServerSideEncryption::from_static);
        out.ssekms_key_id = so.kms_key_id;
        out.sse_customer_algorithm = so.sse_c_algorithm.map(str::to_string);
        let mut resp = S3Response::new(out);
        if let Some(md5) = so.sse_c_key_md5 {
            resp.headers = sse_c_key_md5_header(&md5);
        }
        Ok(resp)
    }

    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        let bucket = req.input.bucket;
        let row = self.acl.bucket_lock(&bucket).await.map_err(|e| {
            tracing::error!(error = %e, "读取桶对象锁状态失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        let Some((enabled, mode, days)) = row else {
            // N4：桶不存在 → 404（区别于「存在但未开锁」的 ObjectLockConfigurationNotFoundError）；
            // 行缺失但桶在（记账缺）→ 按未开锁处理。
            let exists = self
                .store
                .bucket_exists(&bucket)
                .await
                .map_err(to_s3_error)?;
            return if exists {
                Err(s3s::s3_error!(
                    ObjectLockConfigurationNotFoundError,
                    "object lock configuration not found"
                ))
            } else {
                Err(s3s::s3_error!(NoSuchBucket, "bucket not found"))
            };
        };
        if !enabled {
            return Err(s3s::s3_error!(
                ObjectLockConfigurationNotFoundError,
                "object lock configuration not found"
            ));
        }
        Ok(S3Response::new(GetObjectLockConfigurationOutput {
            object_lock_configuration: Some(bucket_lock_out(mode.as_deref(), days)),
        }))
    }

    /// M5 切片 4（N2 复验补）：开关某版本 legal hold（ON/OFF）。
    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        let PutObjectLegalHoldInput {
            bucket,
            key,
            version_id,
            legal_hold,
            ..
        } = req.input;
        let Some(vid) = version_id else {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "version id is required for object legal hold"
            ));
        };
        // P3-a 复验补：须开锁桶（AWS：无对象锁配置 → 400；N4 同口径分层——桶在 = 未开锁 400，
        // 桶亡 = 404）。防「非 WORM 语境下产出不可删版本」。
        {
            let row = self.acl.bucket_lock(&bucket).await.map_err(|e| {
                tracing::error!(error = %e, "读取桶对象锁状态失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
            let enabled = match row {
                Some((enabled, _, _)) => enabled,
                None => {
                    if !self
                        .store
                        .bucket_exists(&bucket)
                        .await
                        .map_err(to_s3_error)?
                    {
                        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found"));
                    }
                    false
                }
            };
            if !enabled {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "bucket object lock is not enabled"
                ));
            }
        }
        let on = match legal_hold
            .as_ref()
            .and_then(|h| h.status.as_ref())
            .map(|s| s.as_str())
        {
            Some(ObjectLockLegalHoldStatus::ON) => true,
            Some(ObjectLockLegalHoldStatus::OFF) => false,
            _ => {
                return Err(s3s::s3_error!(InvalidRequest, "invalid legal hold status"));
            }
        };
        self.store
            .set_object_legal_hold(&bucket, &key, &vid, on)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(PutObjectLegalHoldOutput::default()))
    }

    /// M5 切片 4（N2 复验补）：读某版本 legal hold 状态。
    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        // P3-b 复验补：元数据面读（不经 head_object 的 SSE-C 客户钥校验——SSE-C 对象「能设
        // 不能读」的缺口收口；锁状态非秘密，与 AWS 须带数据钥的差异记档）。
        let on = self
            .store
            .get_legal_hold(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
            )
            .await
            .map_err(to_s3_error)?;
        let status = if on {
            ObjectLockLegalHoldStatus::from_static("ON")
        } else {
            ObjectLockLegalHoldStatus::from_static("OFF")
        };
        Ok(S3Response::new(GetObjectLegalHoldOutput {
            legal_hold: Some(ObjectLockLegalHold {
                status: Some(status),
            }),
        }))
    }

    /// M5 遗留补齐：主体在 `retention.rs`（trait impl 不能按模块拆分，这里只转发）。
    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        retention::get_object_retention(self, req).await
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        retention::put_object_retention(self, req).await
    }

    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        let bucket = req.input.bucket;
        // M9：sync 桶不允许对象锁（写侧锁意图已 409；桶级默认锁同样拒绝）。
        if self.store.sync_mode(&bucket).await.map_err(to_s3_error)? {
            return Err(s3s::s3_error!(
                ConditionalRequestConflict,
                "object lock is not supported on sync buckets"
            ));
        }
        let (mode, days) = parse_bucket_lock_rule(req.input.object_lock_configuration.as_ref())?;
        let row = self.acl.bucket_lock(&bucket).await.map_err(|e| {
            tracing::error!(error = %e, "读取桶对象锁状态失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        // N4：桶不存在 → 404；行缺失但桶在（记账缺）→ 按未开锁走版本检查
        let enabled = match row {
            Some((enabled, _, _)) => enabled,
            None => {
                if !self
                    .store
                    .bucket_exists(&bucket)
                    .await
                    .map_err(to_s3_error)?
                {
                    return Err(s3s::s3_error!(NoSuchBucket, "bucket not found"));
                }
                false
            }
        };
        if !enabled {
            // 首次开启：AWS 要求版本控制已 Enabled（锁须版本化）；开启即保持 Enabled（L2：不可关）
            let ver = self.acl.bucket_versioning(&bucket).await.map_err(|e| {
                tracing::error!(error = %e, "读取桶版本状态失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
            if ver != "Enabled" {
                return Err(s3s::s3_error!(
                    InvalidBucketState,
                    "object lock requires bucket versioning enabled"
                ));
            }
        }
        // M9 S6c：Raft 一条命令写 lock+规则两列（versioning 之前已 Enabled）。
        if let Some(sink) = &self.bucket_control {
            return sink
                .set_object_lock(&bucket, true, mode, days)
                .await
                .map(|()| S3Response::new(PutObjectLockConfigurationOutput::default()))
                .map_err(bucket_control_error);
        }
        if !enabled {
            self.acl.enable_bucket_lock(&bucket).await.map_err(|e| {
                tracing::error!(error = %e, bucket, "开桶锁失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
            self.store.broadcast_bucket_row(&bucket, None).await;
        }
        self.acl
            .set_bucket_lock_rule(&bucket, mode.as_deref(), days)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, bucket, "写桶锁默认规则失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let event_source = EventSource::from_headers(&req.headers);
        // P0-2：复制出的 DELETE 不得在目标端再生 marker 回环。
        // O-2：REPLICA 状态只信 site token 认证的内部请求；普通客户端的同名头被忽略，
        // 其删除照常进入复制钩子（不能用该头抑制复制）。
        let replica_hdr = Self::replica_requested(&req.headers);
        let token_ok = self.site_token_ok(&req.headers);
        let replica = replica_hdr && token_ok;
        if replica_hdr && !token_ok {
            tracing::warn!(bucket = %req.input.bucket, "忽略未认证的 x-amz-replication-status: REPLICA");
        }
        let bucket = req.input.bucket;
        let key = req.input.key;
        let vid = req.input.version_id.clone();
        // M5 切片 4（L4）：Governance bypass 头（调用方均经鉴权链——「非匿名」成立）
        let bypass = req.input.bypass_governance_retention.unwrap_or(false);
        let mode = self.versioning_mode(&bucket).await?;
        let hdr = |n: &str| req.headers.get(n).and_then(|v| v.to_str().ok());
        let origin_id = hdr("x-oss-origin-version-id");
        // 4.16：站点对 marker 同 id——REPLICA DELETE 在 site token 认证下可用
        // `x-oss-origin-version-id` 指定 marker 版本 id（异步站点复制；sync 桶走 M9 内部路由）。
        // 未认证却携带 origin id → 明确 403（不静默丢弃 id 产生错位 marker）。
        let forced_marker = if vid.is_none() && replica_hdr {
            match origin_id {
                Some(origin) => {
                    if !token_ok {
                        return Err(s3s::s3_error!(AccessDenied, "site token required"));
                    }
                    Some(origin.to_string())
                }
                None => None,
            }
        } else {
            None
        };
        if forced_marker.is_some() && self.store.sync_mode(&bucket).await.map_err(to_s3_error)? {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "origin marker id is not supported on sync buckets"
            ));
        }
        let outcome = match forced_marker {
            Some(marker) if mode == VersioningMode::Enabled => {
                let spec = xl_store::store::DeleteMarkerSpec {
                    marker_uuid: marker.clone(),
                    remote_version_id: Some(marker),
                };
                self.store
                    .delete_object_versioned_with_marker(
                        &bucket,
                        &key,
                        None,
                        mode,
                        bypass,
                        Some(spec),
                    )
                    .await
                    .map_err(to_s3_error)?
            }
            Some(_) => {
                return Err(s3s::s3_error!(
                    InvalidBucketState,
                    "origin marker id requires versioning Enabled"
                ));
            }
            None => self
                .delete_versioned(&bucket, &key, vid.as_deref(), mode, bypass)
                .await
                .map_err(to_s3_error)?,
        };
        // M-1 安全修正：绝不按内容指纹删除未映射（remote_version_id=None）的历史 REPLICA。
        // 这类副本不携带源站/源桶身份，共享目标上多个源的同内容副本无法区分，按内容删除
        // 会误删他源活副本。源 id 未命中时保持幂等 204；历史副本需人工对账后清理。
        // 原记档「s3s 无 VersionId 字段」不实（verifier 纠错）：DeleteObjectOutput 有
        // version_id 且 s3s 序列化 x-amz-version-id——marker/被删版本都把 id 回给客户端。
        let mut tier_refs = Vec::new();
        let (dm, vid) = match outcome {
            DeleteOutcome::Marker { version_id } => {
                self.emit(
                    S3EventOp::RemovedDeleteMarkerCreated,
                    &bucket,
                    &key,
                    Some(&version_id),
                    None,
                    None,
                    event_source,
                )
                .await;
                // M6 切片 4：marker 入队受 DeleteMarkerReplication 控制（sink 侧过滤）
                self.repl_on_write(crate::replication::WriteInfo {
                    bucket: &bucket,
                    key: &key,
                    version_id: Some(&version_id),
                    etag: "",
                    size: 0,
                    is_delete_marker: true,
                    is_version_delete: false,
                    is_replica: replica,
                })
                .await;
                (Some(true), Some(version_id))
            }
            DeleteOutcome::Version { version_id, tier } => {
                tier_refs.extend(tier);
                self.emit(
                    S3EventOp::RemovedDelete,
                    &bucket,
                    &key,
                    Some(&version_id),
                    None,
                    None,
                    event_source,
                )
                .await;
                // 4.16：版本定向删除也要复制——站点对同源版本 id，目标做定点删除（不造 marker）。
                // M-1：不再随行内容指纹；未映射历史副本无源身份，禁止按内容自动清理。
                self.repl_on_write(crate::replication::WriteInfo {
                    bucket: &bucket,
                    key: &key,
                    version_id: Some(&version_id),
                    etag: "",
                    size: 0,
                    is_delete_marker: false,
                    is_version_delete: true,
                    is_replica: replica,
                })
                .await;
                (None, Some(version_id))
            }
            DeleteOutcome::Object { tiers } => {
                tier_refs = tiers;
                self.emit(
                    S3EventOp::RemovedDelete,
                    &bucket,
                    &key,
                    None,
                    None,
                    None,
                    event_source,
                )
                .await;
                (None, None)
            }
            // P2-1：幂等空删（未发生删除）——204 但不发事件
            DeleteOutcome::NotFound => (None, None),
        };
        self.tier_gc_delete(&tier_refs).await;
        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: dm,
            version_id: vid,
            ..Default::default()
        }))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let event_source = EventSource::from_headers(&req.headers);
        // P0-2：批量删除的复制请求同样防回环。
        // O-2：同单键删除——未认证的 REPLICA 头被忽略，照常进入复制钩子。
        let replica_hdr = Self::replica_requested(&req.headers);
        let token_ok = self.site_token_ok(&req.headers);
        let replica = replica_hdr && token_ok;
        if replica_hdr && !token_ok {
            tracing::warn!(bucket = %req.input.bucket, "忽略未认证的 x-amz-replication-status: REPLICA");
        }
        let bucket = req.input.bucket;
        // §3.4：一次最多 1000 个
        if req.input.delete.objects.len() > 1000 {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "too many objects in a single delete request"
            ));
        }

        // §3.4 部分失败语义：逐键删，成功的进 deleted、键级错误进 errors 后继续；
        // 桶级错误（桶不存在/桶名不合法）是请求级错误，整体返回 Err。
        let mode = self.versioning_mode(&bucket).await?;
        // M9 §2.4 N2：sync 桶任一远端失败 → 整体 503（成功键已提交；对象可见性幂等）。
        // async 桶保持逐键错误口径（M7 不变）。
        let sync_bucket = self
            .acl
            .bucket_replication_mode(&bucket)
            .await
            .map(|m| m == "sync")
            .unwrap_or(false);
        let mut remote_failure: Option<StoreError> = None;
        // M5 切片 4（L4）：Governance bypass 头（请求级，作用于全部键——AWS 语义）
        let bypass = req.input.bypass_governance_retention.unwrap_or(false);
        // 逐键授权（M4 切片 7 / verifier 阻断 1c）：批量删的键在请求体里、check() 阶段不可见，
        // 授权在此逐键完成（策略 Deny/Allow/回落表三档，与单键删同口径；AWS 亦逐键授权）。
        // 判定基础设施失败 = 该键 fail-closed；被拒键记 AccessDenied 进 errors 后继续（§3.4 部分失败）。
        let user = req.extensions.get::<crate::access::AuthedUser>().cloned();
        // 匿名（无凭据）批量删：check 只验「有桶策略」，逐键仍按桶策略复评（无表三档回落）。
        let anonymous = req
            .extensions
            .get::<crate::access::AnonymousAccess>()
            .is_some();
        // M8 AP：经访问点的批量删由 ApRoute::check_access 挂上下文，逐键叠 AP 策略交集。
        let ap_delete = req.extensions.get::<crate::ap::ApDeletePolicy>();
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for obj in req.input.delete.objects {
            // AP 拒键先于基线判定；拒键绝不进入 delete_versioned（安全边界）。
            if let Some(p) = ap_delete {
                let account = crate::ap::ApAccount {
                    id: p.account_id.as_deref(),
                    active: p.account_active,
                };
                let proof = crate::ap::ApSourceProof {
                    ip: p.source_ip,
                    vpc: p.source_vpc.as_deref(),
                    vpce: p.source_vpce.as_deref(),
                };
                let denied = match &user {
                    Some(u) => {
                        crate::ap::ap_decision(
                            &p.ap,
                            u,
                            account,
                            "DeleteObjects",
                            Some(&obj.key),
                            proof,
                        ) == Some(false)
                    }
                    // 匿名 AP 批量删：逐键只认 Principal "*"，无策略默认拒绝。
                    None if anonymous => {
                        crate::ap::ap_decision_anonymous(
                            &p.ap,
                            "DeleteObjects",
                            Some(&obj.key),
                            proof,
                        ) == Some(false)
                    }
                    None => true, // 防御性 fail-closed
                };
                if denied {
                    let uid = user.as_ref().map_or(0, |u| u.uid);
                    if let Err(e) = self
                        .acl
                        .write_audit(
                            uid,
                            "DeleteObjects",
                            Some(&bucket),
                            Some(&obj.key),
                            "deny",
                            Some("ap:deny"),
                        )
                        .await
                    {
                        tracing::warn!(error = %e, "审计落行失败");
                    }
                    errors.push(Error {
                        key: Some(obj.key),
                        code: Some("AccessDenied".to_string()),
                        message: Some("access denied".to_string()),
                        ..Default::default()
                    });
                    continue;
                }
            }
            let decision = match (&user, anonymous) {
                (Some(u), _) => self.acl.key_delete_decision(u, &bucket, &obj.key).await,
                (None, true) => self.acl.anonymous_delete_decision(&bucket, &obj.key).await,
                (None, false) => Ok((false, None)), // check 恒塞身份；防御性 fail-closed
            };
            let (allowed, deny_detail) = match decision {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, key = %obj.key, "批量删逐键判权失败——fail-closed");
                    (false, None)
                }
            };
            if !allowed {
                // M1：匿名拒绝走限流审计（键可放大到 1000/请求）；已认证仍全量。
                let audited = match &user {
                    Some(u) => {
                        self.acl
                            .write_audit(
                                u.uid,
                                "DeleteObjects",
                                Some(&bucket),
                                Some(&obj.key),
                                "deny",
                                deny_detail,
                            )
                            .await
                    }
                    None => {
                        self.acl
                            .write_anonymous_deny_audit(
                                "DeleteObjects",
                                Some(&bucket),
                                Some(&obj.key),
                                deny_detail,
                            )
                            .await
                    }
                };
                if let Err(e) = audited {
                    tracing::warn!(error = %e, "审计落行失败");
                }
                errors.push(Error {
                    key: Some(obj.key),
                    code: Some("AccessDenied".to_string()),
                    message: Some("access denied".to_string()),
                    ..Default::default()
                });
                continue;
            }
            match self
                .delete_versioned(&bucket, &obj.key, obj.version_id.as_deref(), mode, bypass)
                .await
            {
                Ok(outcome) => {
                    let mut tier_refs = Vec::new();
                    let (dm, dmv, vid) = match outcome {
                        DeleteOutcome::Marker { version_id } => {
                            self.emit(
                                S3EventOp::RemovedDeleteMarkerCreated,
                                &bucket,
                                &obj.key,
                                Some(&version_id),
                                None,
                                None,
                                event_source.clone(),
                            )
                            .await;
                            // M6 切片 4（V3②）：批量删除的 marker 同样入队
                            self.repl_on_write(crate::replication::WriteInfo {
                                bucket: &bucket,
                                key: &obj.key,
                                version_id: Some(&version_id),
                                etag: "",
                                size: 0,
                                is_delete_marker: true,
                                is_version_delete: false,
                                is_replica: replica,
                            })
                            .await;
                            (Some(true), Some(version_id), None)
                        }
                        DeleteOutcome::Version { version_id, tier } => {
                            tier_refs.extend(tier);
                            self.emit(
                                S3EventOp::RemovedDelete,
                                &bucket,
                                &obj.key,
                                Some(&version_id),
                                None,
                                None,
                                event_source.clone(),
                            )
                            .await;
                            // 4.16：批量版本定向删除同样复制（目标定点删同 id，不造 marker）。
                            // M-1：不随行内容指纹；未映射历史副本无源身份，禁止按内容自动清理。
                            self.repl_on_write(crate::replication::WriteInfo {
                                bucket: &bucket,
                                key: &obj.key,
                                version_id: Some(&version_id),
                                etag: "",
                                size: 0,
                                is_delete_marker: false,
                                is_version_delete: true,
                                is_replica: replica,
                            })
                            .await;
                            (None, None, Some(version_id))
                        }
                        DeleteOutcome::Object { tiers } => {
                            tier_refs = tiers;
                            self.emit(
                                S3EventOp::RemovedDelete,
                                &bucket,
                                &obj.key,
                                None,
                                None,
                                None,
                                event_source.clone(),
                            )
                            .await;
                            (None, None, None)
                        }
                        // P2-1：幂等空删——进 deleted[]（S3 语义）但不发事件
                        DeleteOutcome::NotFound => (None, None, None),
                    };
                    self.tier_gc_delete(&tier_refs).await;
                    deleted.push(DeletedObject {
                        key: Some(obj.key),
                        delete_marker: dm,
                        delete_marker_version_id: dmv,
                        version_id: vid,
                    })
                }
                Err(e @ (StoreError::NoSuchBucket(_) | StoreError::InvalidBucketName(_))) => {
                    return Err(to_s3_error(e));
                }
                // 远端不可达/超时：记住并继续逐键（其余键仍各自尝试），最后整体 503。
                Err(e @ StoreError::Unavailable(_)) if sync_bucket => {
                    remote_failure.get_or_insert(e);
                }
                Err(e) => {
                    // 经 to_s3_error：4xx 保留原文，5xx 细节只进日志
                    let s3err = to_s3_error(e);
                    errors.push(Error {
                        key: Some(obj.key),
                        code: Some(s3err.code().as_str().to_string()),
                        message: Some(s3err.message().unwrap_or_default().to_string()),
                        ..Default::default()
                    });
                }
            }
        }

        if let Some(e) = remote_failure {
            if let Some(s) = &self.sync_delete {
                s.record_partial();
            }
            return Err(to_s3_error(e));
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: (!deleted.is_empty()).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..Default::default()
        }))
    }

    /// M6 切片 0：桶通知配置（全量替换；空配置 = 清空）。校验失败 400 不静默。
    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let bucket = req.input.bucket;
        // Raft：存在性由 apply 判定（不短路本地物理读，防 follower 误 404）。
        if self.bucket_control.is_none()
            && !self
                .store
                .bucket_exists(&bucket)
                .await
                .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(NoSuchBucket));
        }
        let targets =
            crate::notify::parse_notification_config(&req.input.notification_configuration)?;
        let json = serde_json::to_string(&targets).map_err(|e| {
            tracing::error!(error = %e, "通知配置序列化失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        if let Some(sink) = &self.bucket_control {
            // 空配置 = 清空（两站点/快照同口径）。
            let cfg = if targets.is_empty() { None } else { Some(json) };
            return sink
                .set_notification(&bucket, cfg, now_secs())
                .await
                .map(|()| S3Response::new(PutBucketNotificationConfigurationOutput::default()))
                .map_err(bucket_control_error);
        }
        self.acl
            .set_bucket_notification(&bucket, &json)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, bucket, "写通知配置失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
        Ok(S3Response::new(
            PutBucketNotificationConfigurationOutput::default(),
        ))
    }

    /// M9 S6c（B16）：桶策略全量替换。Raft 一条 `BucketSetPolicy`；M7 保持 s3s 的
    /// NotImplemented（本服务此前未实现 S3 桶策略写，行为不变）。
    async fn put_bucket_policy(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let Some(sink) = &self.bucket_control else {
            return Err(s3s::s3_error!(
                NotImplemented,
                "PutBucketPolicy is not implemented yet"
            ));
        };
        validate_bucket_policy(&req.input.policy)?;
        sink.set_policy(&req.input.bucket, &req.input.policy, now_secs())
            .await
            .map_err(bucket_control_error)?;
        Ok(S3Response::new(PutBucketPolicyOutput::default()))
    }

    /// M9 S6c（B17）：删除桶策略。Raft 一条 `BucketDeletePolicy`；M7 同上。
    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let Some(sink) = &self.bucket_control else {
            return Err(s3s::s3_error!(
                NotImplemented,
                "DeleteBucketPolicy is not implemented yet"
            ));
        };
        sink.delete_policy(&req.input.bucket)
            .await
            .map_err(bucket_control_error)?;
        Ok(S3Response::new(DeleteBucketPolicyOutput::default()))
    }

    // ---------- M6 切片 4：桶复制配置 API ----------

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let bucket = req.input.bucket;
        if !self
            .store
            .bucket_exists(&bucket)
            .await
            .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(NoSuchBucket, "bucket not found"));
        }
        // AWS：复制要求版本控制 Enabled
        let ver = self.acl.bucket_versioning(&bucket).await.map_err(|e| {
            tracing::error!(error = %e, bucket, "读取桶版本状态失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        if ver != "Enabled" {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "bucket versioning must be Enabled for replication"
            ));
        }
        let Some(store) = &self.repl_store else {
            return Err(s3s::s3_error!(
                NotImplemented,
                "replication is not configured on this server"
            ));
        };
        let ids = store.remote_ids().await;
        let rules = crate::replication::parse_replication_config(
            &req.input.replication_configuration,
            &ids,
        )?;
        let json = serde_json::to_string(&rules).map_err(|e| {
            tracing::error!(error = %e, "序列化复制规则失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        store.save_rules(&bucket, &json).await.map_err(|e| {
            tracing::error!(error = %e, bucket, "写复制规则失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        Ok(S3Response::new(PutBucketReplicationOutput::default()))
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        let bucket = req.input.bucket;
        if !self
            .store
            .bucket_exists(&bucket)
            .await
            .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(NoSuchBucket, "bucket not found"));
        }
        let Some(store) = &self.repl_store else {
            return Err(s3s::s3_error!(
                NotImplemented,
                "replication is not configured on this server"
            ));
        };
        let Some(json) = store.load_rules(&bucket).await.map_err(|e| {
            tracing::error!(error = %e, bucket, "读复制规则失败");
            s3s::s3_error!(InternalError, "internal error")
        })?
        else {
            return Err(s3s::s3_error!(
                ReplicationConfigurationNotFoundError,
                "replication configuration not found"
            ));
        };
        let rules = crate::replication::rules_from_json(&json).map_err(|e| {
            tracing::error!(error = %e, bucket, "复制规则记录损坏");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        let cfg = ReplicationConfiguration {
            role: "arn:oss:iam::replication".to_string(),
            rules: rules.iter().map(repl_rule_to_dto).collect(),
        };
        Ok(S3Response::new(GetBucketReplicationOutput {
            replication_configuration: Some(cfg),
        }))
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let bucket = req.input.bucket;
        if !self
            .store
            .bucket_exists(&bucket)
            .await
            .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(NoSuchBucket, "bucket not found"));
        }
        if let Some(store) = &self.repl_store {
            store.remove_rules(&bucket).await.map_err(|e| {
                tracing::error!(error = %e, bucket, "删复制规则失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
        }
        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }

    /// M6 切片 2：S3 Select（规格 §12；Records/Progress/Stats/End 事件序列 → s3s 成帧）。
    async fn select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        use crate::select as sel;
        // 全量缓冲上限（README 记档）；解压另设上限防压缩炸弹
        const MAX_INPUT: usize = 256 * 1024 * 1024;
        const MAX_INFLATED: usize = 512 * 1024 * 1024;

        let SelectObjectContentInput {
            bucket,
            key,
            request,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ..
        } = req.input;
        // 1) 请求校验先行（白名单/格式/压缩/ScanRange），不支持的语法不付读取成本
        if request.expression_type.as_str() != "SQL" {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "only SQL expression type is supported"
            ));
        }
        let plan = sel::parse(&request.expression)?;
        let comp = request
            .input_serialization
            .compression_type
            .as_ref()
            .map(|c| c.as_str().to_string())
            .unwrap_or_else(|| "NONE".to_string());
        match comp.as_str() {
            "NONE" | "GZIP" | "BZIP2" => {}
            other => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "不支持的压缩类型：{}",
                    other
                ));
            }
        }
        // ScanRange 是解压前字节区间语义，压缩流无法按记录偏移取——均 400 拒绝
        if matches!(comp.as_str(), "GZIP" | "BZIP2") && request.scan_range.is_some() {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "ScanRange is not supported for {} objects",
                comp
            ));
        }
        // Parquet：AWS 只允许 CompressionType NONE（列内压缩由文件自身声明）；
        // ScanRange 是字节区间语义，Parquet 列式无行内偏移——均 400 拒绝。
        if request.input_serialization.parquet.is_some() {
            if comp != "NONE" {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "CompressionType must be NONE with Parquet input"
                ));
            }
            if request.scan_range.is_some() {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "ScanRange is not supported for Parquet input"
                ));
            }
        }

        // 输入/输出格式
        let cf = |v: &str| -> S3Result<u8> {
            let b = v.as_bytes();
            if b.len() != 1 {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "CSV delimiter must be a single byte"
                ));
            }
            Ok(b[0])
        };
        let in_fmt = match (
            request.input_serialization.csv.as_ref(),
            request.input_serialization.json.as_ref(),
            request.input_serialization.parquet.as_ref(),
        ) {
            (Some(c), None, None) => {
                let d = sel::CsvIn {
                    field: cf(c.field_delimiter.as_deref().unwrap_or(","))?,
                    quote: cf(c.quote_character.as_deref().unwrap_or("\""))?,
                    escape: cf(c.quote_escape_character.as_deref().unwrap_or("\""))?,
                    record: cf(c.record_delimiter.as_deref().unwrap_or("\n"))?,
                    // AWS 缺省注释字符 #
                    comments: Some(cf(c.comments.as_deref().unwrap_or("#"))?),
                    header: match c
                        .file_header_info
                        .as_ref()
                        .map(|x| x.as_str())
                        .unwrap_or("NONE")
                    {
                        "USE" => sel::HeaderMode::Use,
                        "IGNORE" => sel::HeaderMode::Ignore,
                        "NONE" => sel::HeaderMode::None,
                        _ => return Err(s3s::s3_error!(InvalidRequest, "invalid FileHeaderInfo")),
                    },
                    allow_quoted_record_delimiter: c.allow_quoted_record_delimiter.unwrap_or(false),
                };
                sel::InFormat::Csv(d)
            }
            (None, Some(j), None) => {
                let lines = match j.type_.as_ref().map(|x| x.as_str()).unwrap_or("DOCUMENT") {
                    "LINES" => true,
                    "DOCUMENT" => false,
                    _ => return Err(s3s::s3_error!(InvalidRequest, "invalid JSON type")),
                };
                sel::InFormat::Json { lines }
            }
            (None, None, Some(_)) => sel::InFormat::Parquet,
            _ => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "exactly one of CSV/JSON/Parquet input serialization is required"
                ));
            }
        };
        let out_fmt = match (
            request.output_serialization.csv.as_ref(),
            request.output_serialization.json.as_ref(),
        ) {
            (Some(c), None) => sel::OutFormat::Csv(sel::CsvOut {
                field: cf(c.field_delimiter.as_deref().unwrap_or(","))?,
                quote: cf(c.quote_character.as_deref().unwrap_or("\""))?,
                escape: cf(c.quote_escape_character.as_deref().unwrap_or("\""))?,
                always_quote: c.quote_fields.as_ref().map(|x| x.as_str()) == Some("ALWAYS"),
                record: cf(c.record_delimiter.as_deref().unwrap_or("\n"))?,
            }),
            (None, Some(j)) => sel::OutFormat::Json {
                record: cf(j.record_delimiter.as_deref().unwrap_or("\n"))?,
            },
            _ => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "exactly one of CSV/JSON output serialization is required"
                ));
            }
        };
        // JSON DOCUMENT 无法按记录偏移做 ScanRange —— 读对象前拒绝
        if request.scan_range.is_some() && matches!(in_fmt, sel::InFormat::Json { lines: false }) {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "ScanRange 不支持 JSON DOCUMENT 输入"
            ));
        }
        let progress = request
            .request_progress
            .as_ref()
            .and_then(|p| p.enabled)
            .unwrap_or(false);
        // SSE-C 对象读取（钥来自 Select 请求头三件套）
        let sse = crate::sse::parse_sse(
            sse_customer_algorithm.as_deref(),
            sse_customer_key.as_deref(),
            sse_customer_key_md5.as_deref(),
            None,
            None,
        )?;

        // 2) 读取 + 解压（先 HEAD 量体积，避免为超大对象付全量读/解密）
        let info = self
            .store
            .head_object_ext(&bucket, &key, sse.key.as_ref())
            .await
            .map_err(to_s3_error)?;
        if info.size > MAX_INPUT as u64 {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "对象超过 S3 Select 的 256MiB 输入上限"
            ));
        }
        let (_info, bytes) = self
            .store
            .get_object_ext(&bucket, &key, sse.key.as_ref())
            .await
            .map_err(to_s3_error)?;
        if bytes.len() > MAX_INPUT {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "对象超过 S3 Select 的 256MiB 输入上限"
            ));
        }
        let input_owned: Vec<u8> = match comp.as_str() {
            "GZIP" => {
                use std::io::Read as _;
                let mut out = Vec::new();
                flate2::read::GzDecoder::new(&bytes[..])
                    .take(MAX_INFLATED as u64 + 1)
                    .read_to_end(&mut out)
                    .map_err(|_| s3s::s3_error!(InvalidRequest, "GZIP decompress failed"))?;
                if out.len() > MAX_INFLATED {
                    return Err(s3s::s3_error!(
                        InvalidRequest,
                        "GZIP 解压后超过 512MiB 上限"
                    ));
                }
                out
            }
            "BZIP2" => sel::inflate_bzip2(&bytes, MAX_INFLATED)?,
            _ => bytes.to_vec(),
        };

        // 3) ScanRange：AWS 语义 = 记录首字节落在区间内则整条处理；仅 End = 末尾 N 字节
        let len = input_owned.len() as i64;
        let mut scan: Option<(i64, i64)> = None;
        if let Some(sr) = request.scan_range.as_ref() {
            let (start, end) = match (sr.start, sr.end) {
                (None, None) => (0, len.saturating_sub(1)),
                (Some(s), None) => (s, len.saturating_sub(1)),
                (Some(s), Some(e)) => {
                    if s > e {
                        return Err(s3s::s3_error!(
                            InvalidRequest,
                            "ScanRange Start 不得大于 End"
                        ));
                    }
                    (s, e)
                }
                (None, Some(n)) => {
                    if n < 0 {
                        return Err(s3s::s3_error!(InvalidRequest, "ScanRange 不允许负值"));
                    }
                    ((len - n).max(0), len.saturating_sub(1))
                }
            };
            if start < 0 || end < 0 {
                return Err(s3s::s3_error!(InvalidRequest, "ScanRange 不允许负值"));
            }
            scan = Some((start, end));
        }
        let scan_bytes = scan.map(|(lo, hi)| {
            if len == 0 {
                return 0;
            }
            let lo = lo.clamp(0, len);
            let hi = hi.clamp(0, len - 1);
            if lo <= hi { hi - lo + 1 } else { 0 }
        });
        let default_scanned = if matches!(comp.as_str(), "GZIP" | "BZIP2") {
            bytes.len() as i64
        } else {
            len
        };

        let events = sel::evaluate(
            &plan,
            &input_owned,
            &in_fmt,
            &out_fmt,
            &sel::EvalOptions {
                request_progress: progress,
                scanned: Some(scan_bytes.unwrap_or(default_scanned)),
                scan,
            },
        )?;

        let mut evs = Vec::with_capacity(events.len());
        for e in events {
            evs.push(Ok(match e {
                sel::SelectEvent::Records(b) => SelectObjectContentEvent::Records(RecordsEvent {
                    payload: Some(Body::from(b)),
                }),
                sel::SelectEvent::Progress(s) => {
                    SelectObjectContentEvent::Progress(ProgressEvent {
                        details: Some(Progress {
                            bytes_scanned: Some(s.bytes_scanned),
                            bytes_processed: Some(s.bytes_processed),
                            bytes_returned: Some(s.bytes_returned),
                        }),
                    })
                }
                sel::SelectEvent::Stats(s) => SelectObjectContentEvent::Stats(StatsEvent {
                    details: Some(Stats {
                        bytes_scanned: Some(s.bytes_scanned),
                        bytes_processed: Some(s.bytes_processed),
                        bytes_returned: Some(s.bytes_returned),
                    }),
                }),
                sel::SelectEvent::End => SelectObjectContentEvent::End(EndEvent {}),
            }));
        }
        let stream = SelectObjectContentEventStream::new(futures::stream::iter(evs));
        Ok(S3Response::new(SelectObjectContentOutput {
            payload: Some(stream),
        }))
    }

    /// M6 切片 0：读桶通知配置（未配置 = 空配置）。
    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        let bucket = req.input.bucket;
        if !self
            .store
            .bucket_exists(&bucket)
            .await
            .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(NoSuchBucket));
        }
        let row = self.acl.bucket_notification(&bucket).await.map_err(|e| {
            tracing::error!(error = %e, bucket, "读通知配置失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        let cfg = match row {
            Some(json) => {
                let targets: Vec<crate::notify::EventTarget> = serde_json::from_str(&json)
                    .map_err(|e| {
                        tracing::error!(error = %e, bucket, "通知配置解析失败（fail-closed 500）");
                        s3s::s3_error!(InternalError, "internal error")
                    })?;
                crate::notify::notification_config_out(&targets)
            }
            None => NotificationConfiguration::default(),
        };
        Ok(S3Response::new(GetBucketNotificationConfigurationOutput {
            event_bridge_configuration: cfg.event_bridge_configuration,
            lambda_function_configurations: cfg.lambda_function_configurations,
            queue_configurations: cfg.queue_configurations,
            topic_configurations: cfg.topic_configurations,
        }))
    }

    // ---------- M8 缺口 C：桶默认加密 API ----------

    /// 只接受单条 SSE-S3/SSE-KMS 默认规则；SSE-C 与显式 CMK 均拒（后者属非目标）。
    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let bucket = req.input.bucket;
        if self.bucket_control.is_none()
            && !self
                .store
                .bucket_exists(&bucket)
                .await
                .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(NoSuchBucket));
        }
        let [rule] = req
            .input
            .server_side_encryption_configuration
            .rules
            .as_slice()
        else {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "exactly one encryption rule is required"
            ));
        };
        let Some(default) = &rule.apply_server_side_encryption_by_default else {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "missing apply_server_side_encryption_by_default"
            ));
        };
        let algo = match default.sse_algorithm.as_str() {
            a @ ("AES256" | "aws:kms") => a,
            _ => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "unsupported server-side encryption"
                ));
            }
        };
        // 3.16：`kms_master_key_id` 仅在 aws:kms 下合法；值须是已配置的命名 CMK。
        // 存储编码 `aws:kms:<name>`（沿用 TEXT 列）。
        let stored = match (default.kms_master_key_id.as_deref(), algo) {
            (Some(_), "AES256") => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "kms key id requires aws:kms"
                ));
            }
            (Some(raw), _) => {
                let raw = raw.trim();
                let name = xl_store::kms::parse_cmk_id(raw).map_err(to_s3_error)?;
                self.ensure_cmk_named(name)?;
                // 原样保留用户态 id（名或 ARN）；响应回显同一形态
                format!("aws:kms:{raw}")
            }
            (None, a) => a.to_string(),
        };
        if let Some(sink) = &self.bucket_control {
            return sink
                .set_default_encryption(&bucket, Some(stored))
                .await
                .map(|()| S3Response::new(PutBucketEncryptionOutput {}))
                .map_err(bucket_control_error);
        }
        let updated = self
            .acl
            .set_bucket_default_encryption(&bucket, &stored)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, bucket, "写桶默认加密失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
        if !updated {
            tracing::error!(bucket, "桶默认加密写入无行（buckets 记录缺失）");
            return Err(s3s::s3_error!(InternalError, "internal error"));
        }
        Ok(S3Response::new(PutBucketEncryptionOutput {}))
    }

    /// 未配置 → `ServerSideEncryptionConfigurationNotFoundError`（s3s 定义的状态码）。
    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        let bucket = req.input.bucket;
        if !self
            .store
            .bucket_exists(&bucket)
            .await
            .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(NoSuchBucket));
        }
        let Some(algo) = self
            .acl
            .bucket_default_encryption(&bucket)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, bucket, "读桶默认加密失败");
                s3s::s3_error!(InternalError, "internal error")
            })?
        else {
            return Err(s3s::s3_error!(
                ServerSideEncryptionConfigurationNotFoundError,
                "server-side encryption configuration not found"
            ));
        };
        // 写路径已校验；库内出现非法值属记录损坏 → 500（与 apply 路径同口径）
        let Some(spec) = enc_spec_from_default(&algo) else {
            tracing::error!(bucket, value = %algo, "桶默认加密记录非法");
            return Err(s3s::s3_error!(InternalError, "internal error"));
        };
        let (algorithm, kms_key_id) = match spec {
            xl_store::crypto::EncSpec::SseS3 => ("AES256", None),
            xl_store::crypto::EncSpec::SseKms => ("aws:kms", None),
            // 回显写入时的用户态 id（名或 ARN），与 PutBucketEncryption 入参同形
            xl_store::crypto::EncSpec::SseKmsCmk(_) => {
                ("aws:kms", algo.strip_prefix("aws:kms:").map(str::to_string))
            }
            xl_store::crypto::EncSpec::SseC(_) => {
                tracing::error!(bucket, "桶默认加密记录出现 SSE-C");
                return Err(s3s::s3_error!(InternalError, "internal error"));
            }
        };
        Ok(S3Response::new(GetBucketEncryptionOutput {
            server_side_encryption_configuration: Some(ServerSideEncryptionConfiguration {
                rules: vec![ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                        kms_master_key_id: kms_key_id,
                        sse_algorithm: ServerSideEncryption::from_static(algorithm),
                    }),
                    ..Default::default()
                }],
            }),
        }))
    }

    /// 幂等：无配置时删除同样成功（AWS 语义）。
    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let bucket = req.input.bucket;
        if self.bucket_control.is_none()
            && !self
                .store
                .bucket_exists(&bucket)
                .await
                .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(NoSuchBucket));
        }
        if let Some(sink) = &self.bucket_control {
            return sink
                .set_default_encryption(&bucket, None)
                .await
                .map(|()| S3Response::new(DeleteBucketEncryptionOutput {}))
                .map_err(bucket_control_error);
        }
        self.acl
            .clear_bucket_default_encryption(&bucket)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, bucket, "清桶默认加密失败");
                s3s::s3_error!(InternalError, "internal error")
            })?;
        Ok(S3Response::new(DeleteBucketEncryptionOutput {}))
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let bucket = req.input.bucket;
        let cfg = req.input.versioning_configuration;
        let current = match self.acl.bucket_versioning(&bucket).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "读桶版本控制状态失败");
                return Err(s3s::s3_error!(InternalError, "internal error"));
            }
        };
        let stored = match cfg.status.map(|s| s.as_str().to_string()).as_deref() {
            Some("Enabled") => "Enabled",
            Some("Off") => {
                // 单向：启用过（Enabled/Suspended）的桶不接受改回 Off——写路径会把整链替换成
                // 单版本 → 旧版本目录成孤儿被 GC 按龄删除（静默丢历史）。
                if current != "Off" {
                    return Err(s3s::s3_error!(
                        NotImplemented,
                        "disabling versioning is not supported"
                    ));
                }
                "Off"
            }
            Some("Suspended") => {
                // Object Lock 桶必须保持 Enabled，否则锁定版本的链语义被破坏。
                match self.acl.bucket_lock(&bucket).await {
                    Ok(Some((true, ..))) => {
                        return Err(s3s::s3_error!(
                            InvalidBucketState,
                            "object lock requires bucket versioning enabled"
                        ));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "读桶锁状态失败");
                        return Err(s3s::s3_error!(InternalError, "internal error"));
                    }
                }
                // M9 sync 桶依赖真 UUID 版本链做双站同 id；Suspended 的 null 版本会破坏
                // 复制/删除对齐（sync 删除路径对此 fail-closed 报 Conflict）。
                match self.acl.bucket_replication_mode(&bucket).await {
                    Ok(m) if m == "sync" => {
                        return Err(s3s::s3_error!(
                            InvalidBucketState,
                            "sync bucket requires versioning enabled"
                        ));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "读桶复制模式失败");
                        return Err(s3s::s3_error!(InternalError, "internal error"));
                    }
                }
                "Suspended"
            }
            _ => {
                return Err(s3s::s3_error!(InvalidArgument, "invalid versioning status"));
            }
        };
        if let Some(sink) = &self.bucket_control {
            return sink
                .set_versioning(&bucket, stored)
                .await
                .map(|()| S3Response::new(PutBucketVersioningOutput::default()))
                .map_err(bucket_control_error);
        }
        match self.acl.set_bucket_versioning(&bucket, stored).await {
            Ok(true) => Ok(S3Response::new(PutBucketVersioningOutput::default())),
            Ok(false) => Err(s3s::s3_error!(NoSuchBucket)),
            Err(e) => {
                tracing::error!(error = %e, "写桶版本控制状态失败");
                Err(s3s::s3_error!(InternalError, "internal error"))
            }
        }
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        match self.acl.bucket_versioning(&req.input.bucket).await {
            // 口径注记：未开过版本的桶本系统回 "Off"（AWS 留空）——值域 Off/Enabled/Suspended，
            // aws-cli 展示不受影响（记档边界）
            Ok(s) => {
                let status = match s.as_str() {
                    "Enabled" => {
                        BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)
                    }
                    "Suspended" => BucketVersioningStatus::from_static("Suspended"),
                    _ => BucketVersioningStatus::from_static("Off"),
                };
                let mut resp = S3Response::new(GetBucketVersioningOutput {
                    status: Some(status),
                    ..Default::default()
                });
                if let Ok(mode) = self.acl.bucket_replication_mode(&req.input.bucket).await
                    && let Ok(v) = http::HeaderValue::from_str(&mode)
                {
                    resp.headers.insert(
                        http::header::HeaderName::from_static("x-oss-replication-mode"),
                        v,
                    );
                }
                Ok(resp)
            }
            Err(e) => {
                tracing::error!(error = %e, "读桶版本控制状态失败");
                Err(s3s::s3_error!(InternalError, "internal error"))
            }
        }
    }

    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let store = self.lifecycle_store()?;
        let Some(cfg) = req.input.lifecycle_configuration else {
            return Err(s3s::s3_error!(
                MalformedXML,
                "lifecycle configuration is required"
            ));
        };
        if cfg.rules.is_empty() {
            return Err(s3s::s3_error!(
                MalformedXML,
                "lifecycle rules must not be empty"
            ));
        }
        let specs = crate::lifecycle::parse_lifecycle_config(&cfg).map_err(lifecycle_error)?;
        store
            .put_rules(&req.input.bucket, &specs)
            .await
            .map_err(lifecycle_error)?;
        Ok(S3Response::new(
            PutBucketLifecycleConfigurationOutput::default(),
        ))
    }

    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        let store = self.lifecycle_store()?;
        let rules = store
            .get_rules(&req.input.bucket)
            .await
            .map_err(lifecycle_error)?
            .ok_or_else(|| {
                s3s::s3_error!(
                    NoSuchLifecycleConfiguration,
                    "the lifecycle configuration does not exist"
                )
            })?;
        Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
            rules: Some(crate::lifecycle::rules_to_dto(&rules)),
            ..Default::default()
        }))
    }

    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let store = self.lifecycle_store()?;
        store
            .delete_rules(&req.input.bucket)
            .await
            .map_err(lifecycle_error)?;
        Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
    }

    async fn put_bucket_inventory_configuration(
        &self,
        req: S3Request<PutBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketInventoryConfigurationOutput>> {
        let store = self.inventory_store()?;
        let owner = req
            .extensions
            .get::<crate::access::AuthedUser>()
            .cloned()
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "access denied"))?;
        let input = req.input;
        if input.id != input.inventory_configuration.id {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "id in URL and body must match"
            ));
        }
        store
            .put_config(&input.bucket, &owner, &input.inventory_configuration)
            .await
            .map_err(inventory_error)?;
        Ok(S3Response::new(
            PutBucketInventoryConfigurationOutput::default(),
        ))
    }

    async fn get_bucket_inventory_configuration(
        &self,
        req: S3Request<GetBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketInventoryConfigurationOutput>> {
        let store = self.inventory_store()?;
        let cfg = store
            .get_config(&req.input.bucket, &req.input.id)
            .await
            .map_err(inventory_error)?
            .ok_or_else(|| {
                inventory_error(crate::inventory::InventoryError::NoSuchConfiguration)
            })?;
        Ok(S3Response::new(GetBucketInventoryConfigurationOutput {
            inventory_configuration: Some(cfg),
        }))
    }

    async fn delete_bucket_inventory_configuration(
        &self,
        req: S3Request<DeleteBucketInventoryConfigurationInput>,
    ) -> S3Result<S3Response<DeleteBucketInventoryConfigurationOutput>> {
        let store = self.inventory_store()?;
        store
            .delete_config(&req.input.bucket, &req.input.id)
            .await
            .map_err(inventory_error)?;
        Ok(S3Response::new(
            DeleteBucketInventoryConfigurationOutput::default(),
        ))
    }

    async fn list_bucket_inventory_configurations(
        &self,
        req: S3Request<ListBucketInventoryConfigurationsInput>,
    ) -> S3Result<S3Response<ListBucketInventoryConfigurationsOutput>> {
        let store = self.inventory_store()?;
        let list = store
            .list_configs(&req.input.bucket)
            .await
            .map_err(inventory_error)?;
        Ok(S3Response::new(ListBucketInventoryConfigurationsOutput {
            inventory_configuration_list: Some(list),
            is_truncated: Some(false),
            ..Default::default()
        }))
    }

    async fn restore_object(
        &self,
        req: S3Request<RestoreObjectInput>,
    ) -> S3Result<S3Response<RestoreObjectOutput>> {
        let sink = self.restore_sink()?;
        let bucket = req.input.bucket;
        let key = req.input.key;
        // versionId 可缺省：解析为当前版本（非版本桶 = "null"）。
        let version_id = match req.input.version_id {
            Some(v) => v,
            None => self
                .store
                .head_object(&bucket, &key)
                .await
                .map_err(to_s3_error)?
                .version_id
                .unwrap_or_else(|| "null".to_string()),
        };
        let header = req
            .headers
            .get("x-amz-restore-request")
            .and_then(|v| v.to_str().ok());
        let days =
            crate::restore::parse_restore_request(req.input.restore_request.as_ref(), header)
                .map_err(|e| match e {
                    crate::restore::RestoreError::InvalidRequest(m) => {
                        s3s::s3_error!(InvalidRequest, "{}", m)
                    }
                    crate::restore::RestoreError::InvalidArgument(m) => {
                        s3s::s3_error!(InvalidArgument, "{}", m)
                    }
                })?;
        // B9：xl-s3 只标记 pending + 交付 sink；sink 失败回滚 pending。
        self.store
            .mark_restore_pending(&bucket, &key, &version_id)
            .await
            .map_err(to_s3_error)?;
        if let Err(e) = sink.enqueue_restore(&bucket, &key, &version_id, days).await {
            tracing::error!(error = %e, bucket, key, "Restore 入队失败，回滚 pending");
            if let Err(re) = self.store.abort_restore(&bucket, &key, &version_id).await {
                tracing::warn!(error = %re, bucket, key, "Restore 回滚失败");
            }
            return Err(s3s::s3_error!(InternalError, "restore enqueue failed"));
        }
        self.emit(
            S3EventOp::RestorePost,
            &bucket,
            &key,
            Some(&version_id),
            None,
            None,
            EventSource::from_headers(&req.headers),
        )
        .await;
        let mut resp =
            S3Response::with_status(RestoreObjectOutput::default(), StatusCode::ACCEPTED);
        resp.headers.insert(
            "x-amz-restore",
            http::HeaderValue::from_static("ongoing-request=\"true\""),
        );
        Ok(resp)
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let ListObjectVersionsInput {
            bucket,
            prefix,
            max_keys,
            key_marker,
            version_id_marker,
            ..
        } = req.input;
        // max-keys<0 同 list_objects_v2 口径（i32 → usize 的 as 会回绕，不猜客户端意图）
        let max_keys = match max_keys {
            Some(m) if m < 0 => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "max-keys must not be negative"
                ));
            }
            other => other.map(|m| m as usize),
        };
        let vl = self
            .store
            .list_object_versions(
                &bucket,
                prefix.as_deref().unwrap_or(""),
                max_keys.unwrap_or(1000),
                key_marker.as_deref(),
                version_id_marker.as_deref(),
            )
            .await
            .map_err(to_s3_error)?;

        let versions: Vec<ObjectVersion> = vl
            .entries
            .iter()
            .filter(|e| !e.is_marker)
            .map(|e| ObjectVersion {
                key: Some(e.key.clone()),
                version_id: Some(e.version_id.clone()),
                is_latest: Some(e.is_latest),
                e_tag: Some(ETag::Strong(e.etag.clone())),
                size: Some(e.size as i64),
                last_modified: Some(last_modified(e.mod_time_nanos)),
                storage_class: Some(ObjectVersionStorageClass::from(
                    e.storage_class
                        .clone()
                        .unwrap_or_else(|| "STANDARD".to_string()),
                )),
                ..Default::default()
            })
            .collect();
        let markers: Vec<DeleteMarkerEntry> = vl
            .entries
            .iter()
            .filter(|e| e.is_marker)
            .map(|e| DeleteMarkerEntry {
                key: Some(e.key.clone()),
                version_id: Some(e.version_id.clone()),
                is_latest: Some(e.is_latest),
                last_modified: Some(last_modified(e.mod_time_nanos)),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListObjectVersionsOutput {
            name: Some(bucket),
            prefix,
            is_truncated: Some(vl.is_truncated),
            next_key_marker: vl.next_key_marker,
            next_version_id_marker: vl.next_version_id_marker,
            versions: (!versions.is_empty()).then_some(versions),
            delete_markers: (!markers.is_empty()).then_some(markers),
            ..Default::default()
        }))
    }

    /// ListObjects v1：主体在 `list_v1.rs`（trait impl 不能按模块拆分，这里只转发）。
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        list_v1::list_objects(self, req).await
    }

    /// ListMultipartUploads：主体在 `list_multipart_uploads.rs`（同上，只转发）。
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        list_multipart_uploads::list_multipart_uploads(self, req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let ListObjectsV2Input {
            bucket,
            prefix,
            delimiter,
            max_keys,
            continuation_token,
            ..
        } = req.input;

        // max-keys < 0：i32 → usize 的 `as` 会回绕成巨值、再被 store 的 .min(1000) 钳成 1000，
        // 非法参数被静默当默认值。报 InvalidRequest，不猜客户端意图。
        let max_keys = match max_keys {
            Some(m) if m < 0 => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "max-keys must not be negative"
                ));
            }
            other => other.map(|m| m as usize),
        };

        let result = self
            .store
            .list_objects(
                &bucket,
                &ListRequest {
                    prefix,
                    delimiter,
                    max_keys,
                    continuation_token,
                    ..Default::default()
                },
            )
            .await
            .map_err(to_s3_error)?;

        let contents: ObjectList = result
            .objects
            .iter()
            .map(|o| Object {
                key: Some(o.key.clone()),
                size: Some(o.size as i64),
                e_tag: Some(ETag::Strong(o.etag.clone())),
                last_modified: Some(last_modified(o.last_modified_nanos)),
                storage_class: Some(ObjectStorageClass::from(
                    o.storage_class
                        .clone()
                        .unwrap_or_else(|| "STANDARD".to_string()),
                )),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(bucket),
            contents: Some(contents),
            key_count: Some(result.objects.len() as i32),
            common_prefixes: Some(
                result
                    .common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect(),
            ),
            is_truncated: Some(result.is_truncated),
            next_continuation_token: result.next_token,
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let event_source = EventSource::from_headers(&req.headers);
        // s3s 已将 x-amz-copy-source 解码、去掉前导 '/' 并解析成枚举；这里只接受桶形式
        let CopySource::Bucket {
            ref bucket,
            ref key,
            ref version_id,
            ..
        } = req.input.copy_source
        else {
            return Err(s3s::s3_error!(InvalidRequest, "unsupported copy source"));
        };

        // M8 前置：两条指令轴（默认 COPY）。s3s 的指令类型解析宽松，非法值在此拒。
        let md = req
            .input
            .metadata_directive
            .as_ref()
            .map(|d| d.as_str())
            .unwrap_or("COPY");
        let td = req
            .input
            .tagging_directive
            .as_ref()
            .map(|d| d.as_str())
            .unwrap_or("COPY");
        if !matches!(md, "COPY" | "REPLACE") {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "invalid x-amz-metadata-directive"
            ));
        }
        if !matches!(td, "COPY" | "REPLACE") {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "invalid x-amz-tagging-directive"
            ));
        }

        // 规格 §3.5 边界：同桶同键且**无源版本号**一律拒（S3 语义：InvalidRequest）。
        // M4 版本控制：`?versionId=` 的同键 copy 是标准回滚惯用法（旧版本 → 新版本），放行。
        // M8 前置：metadata REPLACE 解锁同键 copy（AWS 语义：原地替换元数据/标签）。
        if version_id.is_none()
            && bucket.as_ref() == req.input.bucket.as_str()
            && key.as_ref() == req.input.key.as_str()
            && md != "REPLACE"
        {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "copy source and destination are the same"
            ));
        }

        // M5 切片 3（R4）：源钥（copy-source-sse-c*，源为 SSE-C 必带否则 store 读必拒）
        // 与目标 SSE（sse_customer_*/server_side_encryption；未给 = 明文目标——AWS 无桶默认时同义）
        let src_sse = parse_sse(
            req.input.copy_source_sse_customer_algorithm.as_deref(),
            req.input.copy_source_sse_customer_key.as_deref(),
            req.input.copy_source_sse_customer_key_md5.as_deref(),
            None,
            None,
        )?;
        let mut dst_sse = parse_sse(
            req.input.sse_customer_algorithm.as_deref(),
            req.input.sse_customer_key.as_deref(),
            req.input.sse_customer_key_md5.as_deref(),
            req.input
                .server_side_encryption
                .as_ref()
                .map(|s| s.as_str()),
            req.input.ssekms_key_id.as_deref(),
        )?;
        // M8 缺口 C：目标桶默认加密（仅目标；源钥/源加密不受影响）
        self.apply_default_encryption(&req.input.bucket, &mut dst_sse)
            .await?;
        // 3.16：目标 CMK（显式或桶默认）须存在于本节点密钥环
        self.ensure_cmk_available(&dst_sse)?;
        // M5 切片 4（L3）：对象锁头（桶未开锁带锁头 → resolve_lock 400）
        let dst_lock_req = parse_put_lock(
            req.input.object_lock_mode.as_ref().map(|m| m.as_str()),
            req.input.object_lock_retain_until_date.as_ref(),
            req.input
                .object_lock_legal_hold_status
                .as_ref()
                .map(|s| s.as_str()),
        )?;
        let dst_lock = self.resolve_lock(&req.input.bucket, dst_lock_req).await?;
        // M9：sync 目标桶写边界（SSE-C 目标 400；对象锁 409）
        if self
            .store
            .sync_mode(&req.input.bucket)
            .await
            .map_err(to_s3_error)?
        {
            if matches!(dst_sse.spec, Some(xl_store::crypto::EncSpec::SseC(_))) {
                return Err(s3s::s3_error!(
                    InvalidArgument,
                    "SSE-C is not supported on sync buckets"
                ));
            }
            if dst_lock.as_ref().is_some_and(|l| !l.is_empty()) {
                return Err(s3s::s3_error!(
                    ConditionalRequestConflict,
                    "object lock is not supported on sync buckets"
                ));
            }
        }

        // 源：点名版本（回滚路径）或最新
        let (src_info, bytes) = match version_id.as_deref() {
            Some(vid) => self
                .store
                .get_object_version_ext(bucket, key, vid, src_sse.key.as_ref())
                .await
                .map_err(to_s3_error)?,
            None => self
                .store
                .get_object_ext(bucket, key, src_sse.key.as_ref())
                .await
                .map_err(to_s3_error)?,
        };
        // M8 S4a：目标存储类（未给 = STANDARD；归档类直写拒）
        let storage_class =
            parse_storage_class(req.input.storage_class.as_ref().map(|s| s.as_str()), false)?;
        // M8 前置：两轴独立选择目标元数据/标签（REPLACE 未提供 = 清空）
        let (content_type, user_metadata): (
            Option<String>,
            std::collections::BTreeMap<String, String>,
        ) = if md == "REPLACE" {
            (
                req.input.content_type.clone(),
                req.input
                    .metadata
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            )
        } else {
            (
                src_info.content_type.clone(),
                src_info.user_metadata.clone(),
            )
        };
        let tags = if td == "REPLACE" {
            // 缺省 = 空集 → 清空
            Some(parse_tagging_header(
                req.input.tagging.as_deref().unwrap_or(""),
            )?)
        } else {
            // COPY 时携带 x-amz-tagging 一律忽略（AWS 语义）
            Some(src_info.tags.clone())
        };
        // 目标桶版本策略随 copy 生效（版本桶 copy 出的是新版本）
        let dst_mode = self.versioning_mode(&req.input.bucket).await?;
        let info = self
            .store
            .put_object_stream_with_meta(
                &req.input.bucket,
                &req.input.key,
                Box::pin(futures::stream::once(async move { Ok(bytes) })),
                content_type,
                user_metadata,
                // M9：Copy 继承源校验和（随门 PUT 透传对端，A/B 元数据一致）
                src_info.checksum.clone(),
                dst_mode,
                dst_sse.spec, // M5 切片 3（R4）：目标 SSE 显式头（未给 = 明文目标）
                dst_lock,     // M5 切片 4（L3）
                tags,
                storage_class,
            )
            .await
            .map_err(to_s3_error)?;
        self.emit(
            S3EventOp::CreatedCopy,
            &req.input.bucket,
            &req.input.key,
            info.version_id.as_deref(),
            Some(info.size),
            Some(&info.etag),
            event_source,
        )
        .await;
        // M6 切片 4（V3②）：复制入队覆盖 CopyObject
        self.repl_on_write(crate::replication::WriteInfo {
            bucket: &req.input.bucket,
            key: &req.input.key,
            version_id: info.version_id.as_deref(),
            etag: &info.etag,
            size: info.size,
            is_delete_marker: false,
            is_version_delete: false,
            is_replica: false,
        })
        .await;

        let so = sse_out(info.encryption.as_ref());
        Ok(S3Response::new(CopyObjectOutput {
            version_id: info.version_id.clone(),
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(info.etag)),
                // 与 get/head_object 同一翻译：返回 dst 对象的真实 mtime，不用 now() 冒充
                last_modified: Some(last_modified(info.last_modified_nanos)),
                ..Default::default()
            }),
            server_side_encryption: so
                .server_side_encryption
                .map(ServerSideEncryption::from_static),
            ssekms_key_id: so.kms_key_id,
            sse_customer_algorithm: so.sse_c_algorithm.map(str::to_string),
            sse_customer_key_md5: so.sse_c_key_md5,
            ..Default::default()
        }))
    }

    async fn put_object_tagging(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        // M9：标签 API 不复制（sync 写只带 Create/PUT 时的 x-amz-tagging）。
        if self
            .store
            .sync_mode(&req.input.bucket)
            .await
            .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(
                NotImplemented,
                "object tagging is not supported on sync buckets"
            ));
        }
        let tags = tagging_from_dto(&req.input.tagging)?;
        let vid = self
            .store
            .set_object_tags(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
                tags,
            )
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(PutObjectTaggingOutput {
            version_id: Some(vid),
        }))
    }

    async fn get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        let (vid, tags) = self
            .store
            .get_object_tags(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
            )
            .await
            .map_err(to_s3_error)?;
        let tag_set = tags
            .into_iter()
            .map(|t| Tag {
                key: Some(t.key),
                value: Some(t.value),
            })
            .collect();
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set,
            version_id: Some(vid),
        }))
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        // M9：同 put_object_tagging——标签变更不跨站，sync 桶明确拒绝。
        if self
            .store
            .sync_mode(&req.input.bucket)
            .await
            .map_err(to_s3_error)?
        {
            return Err(s3s::s3_error!(
                NotImplemented,
                "object tagging is not supported on sync buckets"
            ));
        }
        let vid = self
            .store
            .set_object_tags(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
                Vec::new(),
            )
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(DeleteObjectTaggingOutput {
            version_id: Some(vid),
        }))
    }
}

/// 规则 IR → DTO（GetBucketReplication 回读）。
fn repl_rule_to_dto(r: &crate::replication::ReplRule) -> ReplicationRule {
    ReplicationRule {
        delete_marker_replication: Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(
                if r.delete_marker {
                    "Enabled"
                } else {
                    "Disabled"
                },
            )),
        }),
        destination: Destination {
            access_control_translation: None,
            account: None,
            bucket: format!("{}/{}", r.remote_id, r.dest_bucket),
            encryption_configuration: None,
            metrics: None,
            replication_time: None,
            storage_class: None,
        },
        existing_object_replication: None,
        filter: r.prefix.as_ref().map(|p| ReplicationRuleFilter {
            and: None,
            prefix: Some(p.clone()),
            tag: None,
        }),
        id: Some(r.id.clone()),
        prefix: None,
        priority: None,
        source_selection_criteria: None,
        status: ReplicationRuleStatus::from_static(if r.enabled { "Enabled" } else { "Disabled" }),
    }
}

/// 常数时间比较（sync 站点 token；长度不等短路径返回 false 不泄漏内容）。
fn ct_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_control_error_mapping_is_specific() {
        assert_eq!(
            bucket_control_error(BucketControlError::NotFound).code(),
            &s3s::S3ErrorCode::NoSuchBucket
        );
        assert_eq!(
            bucket_control_error(BucketControlError::Conflict).code(),
            &s3s::S3ErrorCode::OperationAborted
        );
        assert_eq!(
            bucket_control_error(BucketControlError::NotEmpty).code(),
            &s3s::S3ErrorCode::BucketNotEmpty
        );
        // 仅建桶把 Conflict 映射为 BucketAlreadyExists（M-3）。
        assert_eq!(
            bucket_create_error(BucketControlError::Conflict).code(),
            &s3s::S3ErrorCode::BucketAlreadyExists
        );
        assert_eq!(
            bucket_control_error(BucketControlError::TooLarge).code(),
            &s3s::S3ErrorCode::EntityTooLarge
        );
        assert_eq!(
            bucket_control_error(BucketControlError::Unavailable).code(),
            &s3s::S3ErrorCode::ServiceUnavailable
        );
        // M8-0：not-leader 是 503，且消息带成员表 leader endpoint（S3 不重定向）。
        let not_leader = bucket_control_error(BucketControlError::NotLeader {
            endpoint: Some("http://10.0.0.2:9000".into()),
        });
        assert_eq!(not_leader.code(), &s3s::S3ErrorCode::ServiceUnavailable);
        assert_eq!(
            not_leader.status_code(),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert!(
            not_leader
                .message()
                .unwrap_or_default()
                .contains("http://10.0.0.2:9000")
        );
        assert_eq!(
            bucket_control_error(BucketControlError::NotLeader { endpoint: None }).code(),
            &s3s::S3ErrorCode::ServiceUnavailable
        );
        assert_eq!(
            bucket_control_error(BucketControlError::Internal).code(),
            &s3s::S3ErrorCode::InternalError
        );
    }

    #[test]
    fn bucket_policy_validation_mirrors_console() {
        assert!(validate_bucket_policy("{\"Version\":\"2012-10-17\",\"Statement\":[]}").is_ok());
        assert_eq!(
            validate_bucket_policy("[]").unwrap_err().code(),
            &s3s::S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            validate_bucket_policy("not json").unwrap_err().code(),
            &s3s::S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            validate_bucket_policy(&format!("{{\"x\":\"{}\"}}", "a".repeat(64 * 1024)))
                .unwrap_err()
                .code(),
            &s3s::S3ErrorCode::InvalidRequest
        );
    }
}
