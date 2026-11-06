//! 授权核心（子账户 × 桶 三档权限：读/写/删）与 s3s 的 `S3Access` 实现。
//!
//! 三面共用同一份判权：S3 面经 `s3s::access::S3Access`（`check` 反序列化前统一入口）；
//! 控制台与 aetherupload 上传面（oss-server）直接调用 [`Acl`] 的 `can/is_owner/visible`
//! ——SQL 只有这一处，防两套实现漂移。
//!
//! 求值规则（fail-closed）：
//!   匿名 → **只**评估桶策略（显式 Allow 必需、Deny 优先），且仅放行数据面对象操作
//!   Get/Head/Put/DeleteObject + DeleteObjects（其余 op 即使策略 Allow 也拒）；
//!   `is_admin` → 全允许；`buckets.owner_id == uid`（桶主）→ 全允许；
//!   否则查 `permissions` 行，**无行 = 无权限**。
//! CreateBucket：任何已认证用户（建后 `record_bucket_owner` 记账）；DeleteBucket：桶主或管理员。

use std::sync::Arc;

use http::HeaderMap;

use xl_store::control_store::ControlError;

use s3s::access::{S3Access, S3AccessContext};
use s3s::dto::CopySource;
use s3s::header::X_AMZ_COPY_SOURCE;
use s3s::{S3Error, S3ErrorCode, S3Result};

/// 建桶行写入结果：配额最终判定（写事务内 count+insert）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketRecordOutcome {
    Written,
    QuotaExceeded,
}

/// 已认证用户身份：`check` 里解析 AK 后塞进请求扩展（s3s 官方机制
/// `S3AccessContext::extensions_mut`），后续 handler 用 `req.extensions.get::<AuthedUser>()` 读取。
/// 带 `ak`：handler 级逐键策略复评（DeleteObjects）需要与 check 同源的 Principal 匹配输入；
/// 控制台/aether 合成身份给空串（各自面不走桶策略）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthedUser {
    pub uid: i64,
    pub is_admin: bool,
    pub ak: String,
}

/// 无凭据请求通过桶策略放行后的请求扩展标记（handler 用：DeleteObjects 逐键复评）。
/// 不复用 `AuthedUser`（uid=0 哨兵不得被当成真实用户/桶主参与表三档判定）。
#[derive(Debug, Clone, Copy)]
pub struct AnonymousAccess;

/// DeleteObjects 的请求级授权上下文；逐 key 只做策略求值，不再查库。
#[derive(Debug, Clone)]
pub struct DeleteBatchAuth {
    account_id: Option<String>,
    bucket_policy: Option<crate::policy::PolicyDoc>,
    session_policy: Option<crate::policy::PolicyDoc>,
    can_delete: bool,
}

impl DeleteBatchAuth {
    pub fn decision(
        &self,
        user: &AuthedUser,
        bucket: &str,
        key: &str,
    ) -> (bool, Option<&'static str>) {
        let request = crate::policy::PolicyRequest {
            op: "DeleteObjects",
            ak: &user.ak,
            uid: user.uid,
            account_id: self.account_id.as_deref(),
            bucket,
            key: Some(key),
            anonymous: false,
        };
        let policy = self.bucket_policy.as_ref().and_then(|doc| {
            crate::policy::s3_action("DeleteObject")
                .and_then(|a| crate::policy::evaluate(doc, &request, a))
        });
        let (allowed, detail) = match policy {
            Some(true) => (true, None),
            Some(false) => (false, Some("policy:deny")),
            None => (self.can_delete, None),
        };
        if allowed
            && let Some(sdoc) = &self.session_policy
            && crate::policy::s3_action("DeleteObject")
                .and_then(|a| crate::policy::evaluate(sdoc, &request, a))
                != Some(true)
        {
            return (false, Some("session:deny"));
        }
        (allowed, detail)
    }
}

/// 匿名 DeleteObjects 的请求级上下文（仅桶策略）。
#[derive(Debug, Clone)]
pub struct AnonymousDeleteAuth {
    policy: Option<crate::policy::PolicyDoc>,
}

impl AnonymousDeleteAuth {
    pub fn decision(&self, bucket: &str, key: &str) -> (bool, Option<&'static str>) {
        let Some(doc) = &self.policy else {
            return (false, Some("anonymous:no-policy"));
        };
        let request = crate::policy::PolicyRequest {
            op: "DeleteObjects",
            ak: "",
            uid: 0,
            account_id: None,
            bucket,
            key: Some(key),
            anonymous: true,
        };
        match crate::policy::s3_action("DeleteObject")
            .and_then(|a| crate::policy::evaluate(doc, &request, a))
        {
            Some(true) => (true, None),
            Some(false) => (false, Some("policy:deny")),
            None => (false, Some("anonymous:no-match")),
        }
    }
}

/// 账户命名空间 v1：uid 派生的 12 位十进制账户 ID（AWS 线上形态）。
///
/// AC-0 起账户成为实体（名称/状态/配额/成员，见 `oss-server` 0005 迁移），但**派生 ID
/// 仍是存量兼容基线**：无成员行（新用户、Raft 命令建的用户、迁移前用户）按此解析，
/// 保证既有 `arn:aws:iam::<12位>:root` 主体不漂移。`0`/负数/超出 12 位 → `None`
/// （不能匹配任何账户 ARN 主体）。
pub fn account_id_of(uid: i64) -> Option<String> {
    (1..=999_999_999_999)
        .contains(&uid)
        .then(|| format!("{uid:012}"))
}

/// 本变更前 AP ARN 的固定账户段；也是 admin/NULL 桶（系统账户）的账户 ID。
/// 主体匹配**永不**命中它（只有派生 ID 才能匹配），仅作 AP 资源 ARN 的历史别名。
pub const LEGACY_ACCOUNT: &str = "000000000000";

/// 账户 ID 的唯一边界校验：恰好 12 位 ASCII 数字。
/// 新的 DB/API/命令入口用它；`LEGACY_ACCOUNT` 也是合法 12 位数字，语义拒绝由调用方负责。
pub fn valid_account_id(s: &str) -> bool {
    s.len() == 12 && s.bytes().all(|b| b.is_ascii_digit())
}

/// 桶账户 = 桶主的**派生**账户；无主/管理员桶归系统账户（LEGACY_ACCOUNT）。
/// AC-0 有意保持该派生口径（AP 资源 ARN 不变）；成员关系只影响策略主体、状态与配额，
/// AP 跨账户属于后续切片。
pub fn bucket_account_id(owner_id: Option<i64>, owner_is_admin: bool) -> String {
    match owner_id {
        Some(uid) if !owner_is_admin => {
            account_id_of(uid).unwrap_or_else(|| LEGACY_ACCOUNT.to_string())
        }
        _ => LEGACY_ACCOUNT.to_string(),
    }
}

/// `arn:aws:iam::<12位>:root` 是否对应用户 uid（v1：账户 = 用户）。
/// 匿名/uid=0/越界 uid 恒 false；LEGACY_ACCOUNT 永不匹配任何真实用户。
pub fn account_arn_matches(arn: &str, uid: i64) -> bool {
    account_principal_matches(arn, uid, None)
}

/// 账户 ARN 主体匹配（AC-0/AC-3 的账户根语义）：
/// - 用户有成员行（`account_id` = Some）→ 账户根 ARN 映射到该账户的**全部成员**；
/// - 无成员行 → 沿用 uid 派生账户（存量/新用户的 legacy 兼容路径）。
///
/// 匿名、LEGACY 系统账户、uid=0 恒不命中：账户根只表达「账户成员」，不是通配。
pub fn account_principal_matches(arn: &str, uid: i64, account_id: Option<&str>) -> bool {
    if uid <= 0 {
        return false;
    }
    let Some(acct) = arn
        .strip_prefix("arn:aws:iam::")
        .and_then(|r| r.strip_suffix(":root"))
    else {
        return false;
    };
    if acct == LEGACY_ACCOUNT {
        return false;
    }
    match account_id {
        Some(current) => current == acct,
        None => account_id_of(uid).as_deref() == Some(acct),
    }
}

/// 权限动作（与 `permissions` 表三列一一对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    Delete,
}

/// 审计接收方（S3 请求路径只入队，不碰 DB）。实现方须自行持有字符串并保证有界；
/// `false` = 丢弃（队列满/已关闭）——审计 best-effort，调用方不得据此改变判定。
#[async_trait::async_trait]
pub trait AuditFallback: Send + Sync {
    /// Synchronous-audit equivalent for callers without an async queue (tests,
    /// offline commands, jobs): write one row through the control seam.
    #[allow(clippy::too_many_arguments)]
    async fn write_audit(
        &self,
        uid: i64,
        op: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
        decision: &str,
        detail: Option<&str>,
    ) -> Result<(), ControlError>;
}

pub trait AuditSink: Send + Sync {
    fn enqueue(
        &self,
        uid: i64,
        op: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
        decision: &str,
        detail: Option<&str>,
    ) -> bool;
}

#[derive(Clone)]
pub struct Acl {
    /// 账户硬配额/用量后端（serve 路径由 oss-server 注入；默认 [`crate::account_quota::NoQuota`]）。
    quota: Arc<dyn crate::account_quota::QuotaBackend>,
    /// M1：匿名拒绝审计限流器（进程级；Clone 共享同一桶）。
    anon_deny: Arc<AnonDenyLimiter>,
    /// 异步审计接收方（serve 路径必注入；缺失 = 审计写 fail-closed）。
    audit: Option<Arc<dyn AuditSink>>,
    /// 对象后端写缝（唯一写路径；缺失 = fail-closed）。
    writes: Option<Arc<dyn crate::control::AclWrites>>,
    /// Background/non-request reads fallback (a captured hot-view generation).
    control: Option<Arc<dyn crate::control::Control>>,
    /// Dynamic fallback: called per read so long-lived workers see new generations.
    control_provider: Option<Arc<dyn Fn() -> Arc<dyn crate::control::Control> + Send + Sync>>,
    /// Synchronous audit fallback when no async queue is installed.
    audit_fallback: Option<Arc<dyn AuditFallback>>,
}

/// 匿名拒绝审计的令牌桶：未认证请求可无限灌入，拒绝审计不能线性写存储。
/// 匿名 **允许** 仍全量审计（审计价值最高）；拒绝默认最多突发 [`ANON_DENY_BURST`] 条、
/// 每秒补 [`ANON_DENY_PER_SEC`] 条，丢弃计数每 1000 条告警一次。
/// 三个旋钮（均进程级、有界）：`OSS_ANON_DENY_AUDIT_BURST`（1..=10000）、
/// `OSS_ANON_DENY_AUDIT_PER_SEC`（0..=10000）、`OSS_ANON_DENY_AUDIT_DEDUPE_SECS`
/// （0 = 关闭；>0 时同 (op,bucket,key) 窗口内只记一次）。非法值回落默认并告警。
struct AnonDenyLimiter {
    burst: f64,
    per_sec: f64,
    dedupe: Option<std::time::Duration>,
    state: std::sync::Mutex<AnonDenyLimiterState>,
}

struct AnonDenyLimiterState {
    tokens: f64,
    last: std::time::Instant,
    dropped: u64,
    /// 去重窗口内的 (op, bucket, key)；超上限整体清空（有界，不做 LRU）。
    seen: std::collections::HashMap<(String, Option<String>, Option<String>), std::time::Instant>,
}

const ANON_DENY_BURST: f64 = 20.0;
const ANON_DENY_PER_SEC: f64 = 5.0;
const ANON_DEDUPE_MAX_KEYS: usize = 1024;

impl AnonDenyLimiter {
    fn new() -> Self {
        Self::with_config(
            env_f64("OSS_ANON_DENY_AUDIT_BURST", ANON_DENY_BURST, 1.0, 10_000.0),
            env_f64(
                "OSS_ANON_DENY_AUDIT_PER_SEC",
                ANON_DENY_PER_SEC,
                0.0,
                10_000.0,
            ),
            env_f64("OSS_ANON_DENY_AUDIT_DEDUPE_SECS", 0.0, 0.0, 86_400.0),
        )
    }

    fn with_config(burst: f64, per_sec: f64, dedupe_secs: f64) -> Self {
        let dedupe = (dedupe_secs > 0.0).then(|| std::time::Duration::from_secs_f64(dedupe_secs));
        Self {
            burst,
            per_sec,
            dedupe,
            state: std::sync::Mutex::new(AnonDenyLimiterState {
                tokens: burst,
                last: std::time::Instant::now(),
                dropped: 0,
                seen: std::collections::HashMap::new(),
            }),
        }
    }

    /// true = 落这一行；false = 丢弃。锁中毒按丢弃处理（不写审计是安全侧）。
    fn try_acquire(&self, op: &str, bucket: Option<&str>, key: Option<&str>) -> bool {
        let Ok(mut s) = self.state.lock() else {
            return false;
        };
        let now = std::time::Instant::now();
        let dedupe_key = self.dedupe.map(|_| {
            (
                op.to_string(),
                bucket.map(str::to_string),
                key.map(str::to_string),
            )
        });
        if let (Some(window), Some(k)) = (self.dedupe, dedupe_key.as_ref())
            && s.seen
                .get(k)
                .is_some_and(|last| now.duration_since(*last) < window)
        {
            return false;
        }
        let elapsed = now.duration_since(s.last).as_secs_f64();
        s.last = now;
        s.tokens = (s.tokens + elapsed * self.per_sec).min(self.burst);
        if s.tokens >= 1.0 {
            s.tokens -= 1.0;
            // 只有真正落行才登记去重窗口；令牌耗尽丢弃时不登记，避免窗口内永久静默。
            if let Some(k) = dedupe_key {
                if s.seen.len() >= ANON_DEDUPE_MAX_KEYS {
                    s.seen.clear();
                }
                s.seen.insert(k, now);
            }
            return true;
        }
        s.dropped += 1;
        if s.dropped % 1000 == 0 {
            tracing::warn!(dropped = s.dropped, "匿名拒绝审计持续限流，已丢弃累计条数");
        }
        false
    }
}

/// 有界数值环境变量：解析失败/非有限/越界 → 默认值 + 告警（限流侧安全默认不阻断）。
fn env_f64(name: &str, default: f64, min: f64, max: f64) -> f64 {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().parse::<f64>() {
            Ok(v) if v.is_finite() && (min..=max).contains(&v) => v,
            _ => {
                tracing::warn!(var = name, value = %raw, "匿名审计限流配置非法，使用默认值");
                default
            }
        },
        Err(_) => default,
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 当前请求捕获的热视图（对象后端唯一读路径；缺失 = fail-closed）。
impl Acl {
    /// 默认无配额后端（测试/离线命令）；serve 路径用 [`Self::with_quota`] 注入。
    pub fn new() -> Self {
        Self {
            quota: Arc::new(crate::account_quota::NoQuota),
            anon_deny: Arc::new(AnonDenyLimiter::new()),
            audit: None,
            writes: None,
            control: None,
            audit_fallback: None,
            control_provider: None,
        }
    }

    /// Synchronous audit writer used when [`Self::audit`] is absent.
    pub fn with_audit_fallback(mut self, writer: Arc<dyn AuditFallback>) -> Self {
        self.audit_fallback = Some(writer);
        self
    }

    /// Fallback control snapshot for callers outside a request scope (job workers,
    /// offline operations). Request-scoped views take precedence.
    pub fn with_control(mut self, control: Arc<dyn crate::control::Control>) -> Self {
        self.control = Some(control);
        self
    }

    /// Per-read provider for long-lived workers (captures a fresh generation).
    pub fn with_control_provider(
        mut self,
        provider: Arc<dyn Fn() -> Arc<dyn crate::control::Control> + Send + Sync>,
    ) -> Self {
        self.control_provider = Some(provider);
        self
    }

    fn control(&self) -> Result<Arc<dyn crate::control::Control>, ControlError> {
        crate::control::request_view()
            .or_else(|| self.control_provider.as_ref().map(|f| f()))
            .or_else(|| self.control.clone())
            .ok_or_else(|| ControlError::Backend("control view missing".into()))
    }

    /// 默认构造（无配额/无审计/无写缝）：仅用于测试与显式装配。
    pub fn default_unwired() -> Self {
        Self::new()
    }

    /// 注入账户硬配额后端（对象后端复制账本）。
    pub fn with_quota(mut self, quota: Arc<dyn crate::account_quota::QuotaBackend>) -> Self {
        self.quota = quota;
        self
    }

    /// S3 服务层取配额后端（不暴露控制库连接）。
    pub(crate) fn quota(&self) -> &dyn crate::account_quota::QuotaBackend {
        self.quota.as_ref()
    }

    /// 注入异步审计接收方（serve 路径）；None = 保持同步直写。
    pub fn with_audit_sink(mut self, sink: Option<Arc<dyn AuditSink>>) -> Self {
        self.audit = sink;
        self
    }

    /// 注入对象后端写缝（唯一写路径；缺失 = fail-closed）。
    pub fn with_writes(mut self, writes: Option<Arc<dyn crate::control::AclWrites>>) -> Self {
        self.writes = writes;
        self
    }

    /// 对象后端写缝（唯一写路径；缺失 = fail-closed）。
    fn writes(&self) -> Result<&dyn crate::control::AclWrites, ControlError> {
        self.writes
            .as_deref()
            .ok_or_else(|| ControlError::Backend("control write seam missing".into()))
    }

    /// 桶版本控制状态（M4 切片 3；`buckets.versioning`）：无行/旧库缺列值 → "Off"（fail-closed 兼容）。
    pub async fn bucket_versioning(&self, bucket: &str) -> Result<String, ControlError> {
        self.control()?.bucket_versioning(bucket).await
    }
    /// M9：桶复制模式行；行不存在 = None（建桶前判「重建不得改模式」）。
    pub async fn bucket_replication_mode_row(
        &self,
        bucket: &str,
    ) -> Result<Option<String>, ControlError> {
        self.control()?.bucket_replication_mode_row(bucket).await
    }
    /// M9：桶复制模式（行不存在 = "async"，与缺行旧桶同口径）。
    pub async fn bucket_replication_mode(&self, bucket: &str) -> Result<String, ControlError> {
        self.control()?.bucket_replication_mode(bucket).await
    }
    /// M9 S3：sync 建桶前置——数据站点 peer 数必须恰为 1（witness 不写 `sites` 行）。
    pub async fn site_peer_count(&self) -> Result<usize, ControlError> {
        self.control()?.site_peer_count().await
    }
    /// M5 切片 4：桶对象锁状态 → `Some((enabled, 默认模式, 默认天数))`；**行不存在 = None**
    /// （N4 复验补：调用方可据此区分「桶不存在(404)」与「锁未开(400)」）。
    pub async fn bucket_lock(
        &self,
        bucket: &str,
    ) -> Result<Option<(bool, Option<String>, Option<i64>)>, ControlError> {
        self.control()?.bucket_lock(bucket).await
    }
    /// M5 切片 4：开桶锁——`object_lock=1` **且自动开版本控制**（AWS：锁须版本化，建桶带
    /// `x-amz-bucket-object-lock-enabled` 时二者一并生效）。**Upsert**：建桶旗标可能先于记账
    /// 行到达（两条调用序都安全）；冲突时只改锁/版本列，不碰 owner/set/creation_date。
    pub async fn enable_bucket_lock(&self, bucket: &str) -> Result<(), ControlError> {
        self.writes()?.enable_bucket_lock(bucket).await
    }
    /// M5 切片 4：设/改默认保留规则（仅锁已开启桶；调用方先校验；None = 清空规则）。
    pub async fn set_bucket_lock_rule(
        &self,
        bucket: &str,
        mode: Option<&str>,
        days: Option<i64>,
    ) -> Result<(), ControlError> {
        self.writes()?
            .set_bucket_lock_rule(bucket, mode, days)
            .await
    }
    /// 桶策略读取（M4 切片 7）：None = 未配；**解析失败 = Err**（check 里 fail-closed 拒）。
    pub async fn bucket_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<crate::policy::PolicyDoc>, ControlError> {
        self.control()?.bucket_policy(bucket).await
    }
    /// 审计落行（判定后；允许与拒绝都记；best-effort 由调用方 warn）。
    /// 只入队到 [`AuditSink`]；未注入 sink = fail-closed（不允许静默丢弃审计）。
    pub async fn write_audit(
        &self,
        uid: i64,
        op: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
        decision: &str,
        detail: Option<&str>,
    ) -> Result<(), ControlError> {
        if let Some(sink) = &self.audit {
            // 丢弃计数与告警由 sink 负责（每次请求 warn 会在过载时洪水式刷日志）。
            sink.enqueue(uid, op, bucket, object_key, decision, detail);
            return Ok(());
        }
        if let Some(writer) = &self.audit_fallback {
            return writer
                .write_audit(uid, op, bucket, object_key, decision, detail)
                .await;
        }
        Err(ControlError::Backend(
            "audit sink missing: refusing synchronous audit write".into(),
        ))
    }
    /// 匿名拒绝审计（限流；uid=0 哨兵）。返回 Ok 不代表已落行——洪水期会丢弃。
    pub async fn write_anonymous_deny_audit(
        &self,
        op: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
        detail: Option<&str>,
    ) -> Result<(), ControlError> {
        if !self.anon_deny.try_acquire(op, bucket, object_key) {
            return Ok(());
        }
        self.write_audit(0, op, bucket, object_key, "deny", detail)
            .await
    }
    /// 清桶策略（删桶时调用——与生命周期规则同款 S6-1 语义）。
    pub async fn clear_bucket_policy(&self, bucket: &str) -> Result<(), ControlError> {
        self.writes()?.clear_bucket_policy(bucket).await
    }
    /// M6 切片 0：桶通知配置（None = 未配置；json 原文，解析在 notify 模块）。
    pub async fn bucket_notification(&self, bucket: &str) -> Result<Option<String>, ControlError> {
        self.control()?.bucket_notification(bucket).await
    }
    /// M6 切片 0：写桶通知配置（全量替换）。
    pub async fn set_bucket_notification(
        &self,
        bucket: &str,
        config_json: &str,
    ) -> Result<(), ControlError> {
        self.writes()?
            .set_bucket_notification(bucket, config_json)
            .await
    }
    /// M6 切片 0：删桶时清通知配置（防同名重建继承旧规则）。
    pub async fn clear_bucket_notification(&self, bucket: &str) -> Result<(), ControlError> {
        self.writes()?.clear_bucket_notification(bucket).await
    }
    /// M8 缺口 C：桶默认加密（"AES256"/"aws:kms"；None = 未配置）。
    pub async fn bucket_default_encryption(
        &self,
        bucket: &str,
    ) -> Result<Option<String>, ControlError> {
        self.control()?.bucket_default_encryption(bucket).await
    }
    /// M8 缺口 C：写桶默认加密（全量替换；桶行不存在 → false）。
    pub async fn set_bucket_default_encryption(
        &self,
        bucket: &str,
        algorithm: &str,
    ) -> Result<bool, ControlError> {
        self.writes()?
            .set_bucket_default_encryption(bucket, algorithm)
            .await
    }
    /// M8 缺口 C：清桶默认加密（幂等；返回是否命中桶行）。
    pub async fn clear_bucket_default_encryption(
        &self,
        bucket: &str,
    ) -> Result<bool, ControlError> {
        self.writes()?.clear_bucket_default_encryption(bucket).await
    }
    /// 清桶生命周期规则（M4 切片 6：删桶时调用——桶名可复用，旧规则复活会误删新桶
    /// 对象；verifier S6-1）。表在共享 DB（migrations 0001），S3 面与控制面同口径。
    pub async fn clear_lifecycle_rules(&self, bucket: &str) -> Result<(), ControlError> {
        self.writes()?.clear_lifecycle_rules(bucket).await
    }
    /// DeleteObjects **逐键**授权（handler 用；verifier 阻断 1c 的修复点）。批量删的对象键在
    /// 请求体、check() 阶段不可见——若不逐键复评，「前缀 Deny」可被一条批量删请求整体绕过，
    /// 而「桶级 ARN Deny」又会误压整个请求。判定与单键删在 check() 里的融合完全同口径：
    /// 策略显式 Deny → 拒；策略 Allow → 放；无匹配 → 回落表三档。
    /// 返回 `(allowed, deny_detail)`：deny_detail 仅拒绝时有值（`policy:deny` = 策略拒，
    /// `None` = 表三档拒），供 handler 落审计。策略/表读取失败 → 调用方 fail-closed。
    pub async fn key_delete_decision(
        &self,
        user: &AuthedUser,
        bucket: &str,
        key: &str,
    ) -> Result<(bool, Option<&'static str>), ControlError> {
        let ctx = self.delete_batch_auth(user, bucket).await?;
        Ok(ctx.decision(user, bucket, key))
    }
    /// 预加载 DeleteObjects 请求级授权上下文：账户、桶策略、会话策略与表三档权限
    /// 都只依赖 `(user, bucket)`，不应在每个 key 上重复查询。
    pub async fn delete_batch_auth(
        &self,
        user: &AuthedUser,
        bucket: &str,
    ) -> Result<DeleteBatchAuth, ControlError> {
        let account_id = self.account_id_of_user(user.uid).await?;
        let bucket_policy = self.bucket_policy(bucket).await?;
        let session_policy = self.session_policy_of(&user.ak).await?;
        let can_delete = self.can(user.clone(), Action::Delete, bucket).await?;
        Ok(DeleteBatchAuth {
            account_id,
            bucket_policy,
            session_policy,
            can_delete,
        })
    }
    /// 匿名 DeleteObjects 的桶策略预加载（匿名没有账户/表三档）。
    pub async fn anonymous_delete_batch_auth(
        &self,
        bucket: &str,
    ) -> Result<AnonymousDeleteAuth, ControlError> {
        Ok(AnonymousDeleteAuth {
            policy: self.bucket_policy(bucket).await?,
        })
    }
    /// 匿名 DeleteObjects 逐键判定：**仅**桶策略，无表三档回落（匿名不继承任何用户权限）。
    /// 与签名路径同口径：显式 Deny 优先、无匹配即拒。
    pub async fn anonymous_delete_decision(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(bool, Option<&'static str>), ControlError> {
        let ctx = self.anonymous_delete_batch_auth(bucket).await?;
        Ok(ctx.decision(bucket, key))
    }
    /// M5 切片 5（S8）：会话策略交集融合——在表三档 × 桶策略之后叠会话判定：
    /// 会话 Deny → 拒；**无匹配 → 拒**（未 Allow 即拒）；Allow → 保留基线（不能抬升）。
    /// CopyObject 双轴分别求值（dst=`PutObject` 映射 / src=`GetObject`），与桶策略融合同构。
    /// `s3_action` 未映射 op：无 Allow 可依 → 拒（fail-closed；旧实现静默跳过 = 会话免检）。
    /// 返回 `((allow_dst, allow_src), detail)`；无会话策略 = 原样返回 `((..), None)`。
    /// 抽出独立方法以便单测直钉（S3AccessContext 不可外部构造，check() 本体无法在单测驱动）。
    async fn fuse_session(
        &self,
        pr: crate::policy::PolicyRequest<'_>,
        src: Option<crate::policy::PolicyRequest<'_>>,
        mut allow_dst: bool,
        mut allow_src: bool,
    ) -> Result<((bool, bool), Option<&'static str>), ControlError> {
        let op = pr.op;
        let Some(sdoc) = self.session_policy_of(pr.ak).await? else {
            return Ok(((allow_dst, allow_src), None));
        };
        let dst_act = if is_copy(op) {
            crate::policy::s3_action("PutObject")
        } else {
            crate::policy::s3_action(op)
        };
        let mut denied = false;
        // None（无映射）同样落 deny：会话策略是身份上限，词表缺项不得等于免检。
        if dst_act.and_then(|a| crate::policy::evaluate(&sdoc, &pr, a)) != Some(true) {
            allow_dst = false;
            denied = true;
        }
        if let Some(spr) = src
            && let Some(a) = crate::policy::s3_action("GetObject")
            && crate::policy::evaluate(&sdoc, &spr, a) != Some(true)
        {
            allow_src = false;
            denied = true;
        }
        Ok((
            (allow_dst, allow_src),
            Some(if denied {
                "session:deny"
            } else {
                "session:allow"
            }),
        ))
    }
    /// 写桶版本控制状态（PutBucketVersioning；授权门已确认桶主/管理员）。
    /// 返回 false = 桶无 DB 行（S3 层按 NoSuchBucket 处置）。
    pub async fn set_bucket_versioning(
        &self,
        bucket: &str,
        status: &str,
    ) -> Result<bool, ControlError> {
        self.writes()?.set_bucket_versioning(bucket, status).await
    }
    /// AK → (uid, is_admin)。status/expires 条件与 `SigV4Auth::get_secret_key` 同口径
    ///（认证面与授权面查的是同一行，任一处撤销即同时失效）；**账号停用（users.status）
    /// 同样使 AK 失效**——停用即全链路出局。
    pub async fn user_of_key(&self, access_key: &str) -> Result<Option<AuthedUser>, ControlError> {
        self.control()?.user_of_key(access_key, now()).await
    }
    /// AC-0：用户当前账户（成员行优先，无行回落 uid 派生账户）。
    /// 策略主体把账户根 ARN 映射到账户成员集合时使用；查询失败由调用方 fail-closed。
    pub async fn account_id_of_user(&self, uid: i64) -> Result<Option<String>, ControlError> {
        self.control()?.account_id_of_user(uid).await
    }
    /// AP 跨账户：请求者账户 + 有效状态（成员行优先，无成员行回落 uid 派生账户）。
    /// - 成员行：账户行必须存在且 `status='active'`（缺失/停用 → false，fail-closed）；
    /// - 无成员行：派生账户实体行存在时按状态，不存在按 legacy 兼容视为有效。
    ///
    /// 查询失败由调用方 fail-closed（5xx）。
    pub async fn account_identity_of_user(
        &self,
        uid: i64,
    ) -> Result<(Option<String>, bool), ControlError> {
        self.control()?.account_identity_of_user(uid).await
    }
    /// AC-2：账户桶数硬配额（`accounts.bucket_quota`；NULL/负 = 不限）。
    /// 返回 true = 已到上限，拒绝建桶。查询失败由调用方 fail-closed（5xx）。
    /// 这是快速检查（文案/错误码兼容）；权威判定在 standalone 写事务内、Raft leader
    /// append 前，并发建桶不会越过 `accounts.bucket_quota`。
    pub async fn bucket_quota_exceeded(&self, uid: i64) -> Result<bool, ControlError> {
        self.control()?.bucket_quota_exceeded(uid).await
    }
    /// M5 切片 5（S8）：AK 的会话策略（`kind='session'` 且 `session_policy` 非空时）。
    /// 本地查无 → 扇出（远端应答带 `session_policy`；NULL/空 = 无约束，与线上口径一致）。
    /// 解析失败 → Err（调用方 fail-closed 拒）。非会话键 → `None`。
    pub async fn session_policy_of(
        &self,
        access_key: &str,
    ) -> Result<Option<crate::policy::PolicyDoc>, ControlError> {
        self.control()?.session_policy_of(access_key).await
    }
    /// 桶主？（`buckets.owner_id == uid`）
    pub async fn is_owner(&self, bucket: &str, uid: i64) -> Result<bool, ControlError> {
        self.control()?.is_owner(bucket, uid).await
    }
    /// ListObjects 的 Owner 回写：桶主 username；管理员桶/无 DB 行回落管理员。
    /// 返回 `(id, display_name)`，id 用本地 uid 的十进制串（本模型的稳定主体标识）。
    pub async fn bucket_owner_display(
        &self,
        bucket: &str,
    ) -> Result<Option<(String, String)>, ControlError> {
        self.control()?.bucket_owner_display(bucket).await
    }
    /// 主判权：admin / 桶主 全允许；否则查对应能力位（无行 = 拒）。
    pub async fn can(
        &self,
        user: AuthedUser,
        action: Action,
        bucket: &str,
    ) -> Result<bool, ControlError> {
        self.control()?.can(&user, action, bucket).await
    }
    /// 可见桶集合（列举过滤）：自有桶 ∪ 在 `permissions` 任一能力位为 1 的桶。
    /// 与磁盘列表**求交**使用（磁盘是权威——历史桶可能没有 DB 行）。
    pub async fn visible(&self, uid: i64) -> Result<Vec<String>, ControlError> {
        self.control()?.visible(uid).await
    }
    /// 桶主账户硬配额（E1）：由注入的配额后端负责请求视图/本地缓存选择。
    pub async fn account_quota_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Option<crate::account_quota::AccountQuota>, ControlError> {
        self.quota.account_quota_for_bucket(bucket).await
    }
    /// 桶归属登记（历史形态，无 set；幂等——已存在不覆盖，测试钉）。
    pub async fn record_bucket_owner(&self, bucket: &str, uid: i64) -> Result<(), ControlError> {
        self.writes()?.record_bucket_owner(bucket, uid).await
    }
    /// 带 set 的归属登记（C3：S3 面建桶缺省走自动指派；单 set 恒 0）。
    /// M5 切片 0：**覆盖**语义（原 INSERT OR IGNORE）——删桶记账曾被容忍失败留下的陈旧行
    /// 必须在重建时收敛 set 与 owner，否则本节点按旧 set 路由、按旧 owner 判权，与已收广播
    /// 的 peer 静默分叉（verifier 阻断 1）。
    pub async fn record_bucket_owner_set(
        &self,
        bucket: &str,
        uid: i64,
        set: u16,
    ) -> Result<BucketRecordOutcome, ControlError> {
        self.record_bucket_owner_set_mode(bucket, uid, set, "async")
            .await
    }
    /// M9：同 [`Self::record_bucket_owner_set`]，并写 `replication_mode`。
    /// ON CONFLICT 全列重置（set 随重建收敛，M5 阻断 1）；creation_date 保持首建。
    /// **新建行**在 `BEGIN IMMEDIATE` 写锁内 count+insert——并发建桶只有一个能越过账户
    /// 桶数配额（快速检查在 `check_op`，最终判定在这里）；已存在行是重建收敛，不判配额。
    pub async fn record_bucket_owner_set_mode(
        &self,
        bucket: &str,
        uid: i64,
        set: u16,
        replication_mode: &str,
    ) -> Result<BucketRecordOutcome, ControlError> {
        self.writes()?
            .record_bucket_owner_set_mode(bucket, uid, set, replication_mode)
            .await
    }
    /// C3：建桶自动指派——选**当前桶数最少**的 set（并列取小号）；无行数据按 0 计。
    /// 单 set/单机 k=1 恒 0（零查询）。指派只影响**新桶**；已有桶不迁（S-L 口径）。
    pub async fn pick_least_loaded_set(&self, k: u16) -> Result<u16, ControlError> {
        if k <= 1 {
            return Ok(0);
        }
        self.writes()?.pick_least_loaded_set(k).await
    }
    /// 删桶后的记账清理（S3 与控制台两条删桶路径共用同一实现）：
    /// 清该桶的授权行（防重建同名桶继承旧授权）+ 清桶行。
    pub async fn forget_bucket(&self, bucket: &str) -> Result<(), ControlError> {
        self.writes()?.forget_bucket(bucket).await
    }
    /// 带 `Option<bucket>` 判权的便捷形态（Root 路径的 op 直接拒）。
    async fn can_opt(
        &self,
        user: AuthedUser,
        action: Action,
        bucket: Option<&str>,
    ) -> Result<bool, S3Error> {
        match bucket {
            Some(b) => self.can(user, action, b).await.map_err(acl_err),
            None => Ok(false),
        }
    }
}

/// 判权查询失败的统一映射（详情进日志，客户端只见 500 泛化文案）。
pub(crate) fn acl_err(e: ControlError) -> S3Error {
    tracing::error!(error = ?e, "acl lookup failed");
    S3Error::with_message(S3ErrorCode::InternalError, "internal error")
}
/// L1：匿名请求携带的对象锁/治理旁路头（返回头名用于审计；值不记录）。
pub(crate) fn anonymous_lock_header(headers: &HeaderMap) -> Option<&'static str> {
    [
        (
            "x-amz-object-lock-mode",
            s3s::header::X_AMZ_OBJECT_LOCK_MODE,
        ),
        (
            "x-amz-object-lock-retain-until-date",
            s3s::header::X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
        ),
        (
            "x-amz-object-lock-legal-hold",
            s3s::header::X_AMZ_OBJECT_LOCK_LEGAL_HOLD,
        ),
        (
            "x-amz-bypass-governance-retention",
            s3s::header::X_AMZ_BYPASS_GOVERNANCE_RETENTION,
        ),
    ]
    .into_iter()
    .find(|(_, h)| headers.contains_key(h.clone()))
    .map(|(name, _)| name)
}

/// s3s 的授权入口：`check` 在反序列化**前**对每个请求执行（我们已 `set_auth` → 恒走；
/// per-op 方法默认 Ok 是叠加非替代，无需实现）。判据与三面共用的 [`Acl::can`] 同源；
/// 匿名走 [`Acl::anonymous_decision`]（仅桶策略 + 数据面白名单），不进入表三档。
impl Acl {
    /// 匿名（无凭据）判权：**只**允许数据面对象操作，且**只**由桶策略显式 Allow。
    /// 桶/控制面 op（列表、建删桶、策略/ACL/版本控制/MPU/tag/lock…）即使策略写了 Allow
    /// 也拒——否则匿名策略可自举成提权；无策略/无匹配/策略读取失败一律拒（fail-closed）。
    /// DeleteObjects 的键在请求体里 → 此处只要求存在桶策略，逐键判定下放 handler。
    /// 返回 `(allow, detail)`；detail 用于审计（不含任何凭据）。
    pub async fn anonymous_decision(
        &self,
        op: &str,
        bucket: Option<&str>,
        key: Option<&str>,
    ) -> S3Result<(bool, Option<String>)> {
        let data_plane = matches!(
            op,
            "GetObject" | "HeadObject" | "PutObject" | "DeleteObject" | "DeleteObjects"
        );
        if !data_plane {
            return Ok((false, Some("anonymous:not-data-plane".to_string())));
        }
        let Some(b) = bucket else {
            return Ok((false, Some("anonymous:no-bucket".to_string())));
        };
        let doc = match self.bucket_policy(b).await {
            Ok(Some(doc)) => doc,
            Ok(None) => return Ok((false, Some("anonymous:no-policy".to_string()))),
            Err(e) => {
                tracing::error!(error = %e, bucket = %b, "匿名请求读桶策略失败——fail-closed 拒绝");
                return Ok((false, Some("policy:error".to_string())));
            }
        };
        // DeleteObjects：check 阶段看不到键 → 下放 handler 逐键复评（与签名路径同构）。
        if op == "DeleteObjects" {
            return Ok((true, Some("anonymous:delegated".to_string())));
        }
        let Some(action) = crate::policy::s3_action(op) else {
            return Ok((false, Some("anonymous:not-data-plane".to_string())));
        };
        let pr = crate::policy::PolicyRequest {
            op,
            ak: "",
            uid: 0,
            account_id: None,
            bucket: b,
            key,
            anonymous: true,
        };
        Ok(match crate::policy::evaluate(&doc, &pr, action) {
            Some(true) => (true, Some("policy:allow".to_string())),
            Some(false) => (false, Some("policy:deny".to_string())),
            None => (false, Some("anonymous:no-match".to_string())),
        })
    }

    /// 匿名判权 + 审计；放行时塞 [`AnonymousAccess`] 扩展供 handler 逐键复评。
    /// 审计 uid=0（anonymous 哨兵；users 自增从 1 起，不写 AK/凭据）。
    async fn check_anonymous(
        &self,
        cx: &mut S3AccessContext<'_>,
        op: &str,
        bucket: Option<&str>,
        key: Option<&str>,
    ) -> S3Result<()> {
        // L1：匿名请求不得携带对象锁/治理旁路头。匿名策略没有可表达的对应动作，
        // 无法判断保留策略是否允许 → fail-closed（写/删都可能改锁语义）。
        let (allow, detail) = match anonymous_lock_header(cx.headers()) {
            Some(h) => (false, Some(format!("anonymous:object-lock-header:{h}"))),
            None => self.anonymous_decision(op, bucket, key).await?,
        };
        // M1：允许全量审计；拒绝经进程级令牌桶限流，避免未认证洪水撑爆 SQLite。
        let audited = if allow {
            self.write_audit(0, op, bucket, key, "allow", detail.as_deref())
                .await
        } else {
            self.write_anonymous_deny_audit(op, bucket, key, detail.as_deref())
                .await
        };
        if let Err(e) = audited {
            tracing::warn!(error = %e, "审计落行失败");
        }
        if allow {
            cx.extensions_mut().insert(AnonymousAccess);
            Ok(())
        } else {
            tracing::warn!(uid = 0, op, bucket = ?bucket, "匿名授权拒绝");
            Err(s3s::s3_error!(AccessDenied, "access denied"))
        }
    }

    /// M8 S1：把 `check` 的判定主体抽成可复用方法（AP 路由用；`check` 包装后照常审计）。
    /// `copy_src` = 已解析的 (源桶, 源键)，仅 CopyObject 使用；返回 (allow, detail)。
    #[allow(clippy::too_many_arguments)]
    pub async fn check_op(
        &self,
        user: &AuthedUser,
        ak: &str,
        op: &str,
        bucket: Option<&str>,
        key: Option<&str>,
        copy_src: Option<(&str, &str)>,
    ) -> S3Result<(bool, Option<String>)> {
        let is_admin = user.is_admin;
        // AC-0：账户根 ARN 主体按「用户当前账户」解析（成员行优先，无行回落派生账户）。
        // 多一次 PK 查询换取策略主体与账户模型同源；失败 fail-closed。
        let account_id = self.account_id_of_user(user.uid).await.map_err(acl_err)?;
        // AC-2：账户桶数硬配额在写边界（CreateBucket）fail-closed；管理员的桶归系统账户，
        // 不受任何用户账户配额约束。放在 check_op（而非 check）使 S3/AP/作业入口同口径。
        if op == "CreateBucket"
            && !is_admin
            && self
                .bucket_quota_exceeded(user.uid)
                .await
                .map_err(acl_err)?
        {
            return Err(s3s::s3_error!(
                TooManyBuckets,
                "account bucket quota exceeded"
            ));
        }

        // DeleteObjects：对象键在请求体里，check 阶段拿不到 → 授权**整体下放 handler 逐键**
        //（service.rs::delete_objects 对每个键跑 policy×表三档，与单键删同口径；AWS 亦逐键
        // 授权）。此处只保身份与下面审计，避免「批量删绕过前缀 Deny」（verifier 阻断 1c）
        // 与反向「桶级 ARN Deny 误压整个批量请求」两种偏差。
        let delegated = op == "DeleteObjects";
        // 表级基线拆两轴：dst（目标/主体动作）与 src（仅 CopyObject 的源读）。两轴独立 =
        // 策略融合时不会互相抬升/塌缩（旧实现 src Allow 能抬掉 dst 表拒、dst Allow(Put) 能
        // 抬掉 src 表读——verifier 阻断 1b）；最终 allow = 两轴合取。
        let (mut allow_dst, mut allow_src) = if is_admin || delegated {
            (true, true)
        } else {
            // can()/can_opt() 取 by-value；user 后面还要用（策略 Principal / 审计）→ 克隆一份
            let gate_user = user.clone();
            match classify(op) {
                None => {
                    tracing::warn!(uid = user.uid, op, "授权拒绝（未映射 op，fail-closed）");
                    return Err(s3s::s3_error!(AccessDenied, "access denied"));
                }
                Some(gate) => match gate {
                    // 列表由 handler 过滤；建桶任何已认证用户（建后记账 owner）
                    Gate::Open => (true, true),
                    Gate::OwnerBucket => match &bucket {
                        Some(b) => (self.is_owner(b, user.uid).await.map_err(acl_err)?, true),
                        None => (false, true),
                    },
                    Gate::Need(action) => (self.can_opt(gate_user, action, bucket).await?, true),
                    Gate::Copy => {
                        // 目标写 + 源读（源在 header；用 s3s 的解析器——header 是 URL 编码
                        // 且有 / 前导与 ?versionId 形态，解析失败与反序列化同判，直接拒绝）
                        let dst = self
                            .can_opt(gate_user.clone(), Action::Write, bucket)
                            .await?;
                        let src = match copy_src {
                            Some((src_bucket, _)) => self
                                .can(gate_user, Action::Read, src_bucket)
                                .await
                                .map_err(acl_err)?,
                            None => false,
                        };
                        (dst, src)
                    }
                },
            }
        };
        // —— 桶策略融合（M4 切片 7）：显式 Deny 优先 → Allow 提升 → 无匹配回落三档 ——
        // 两轴**各自**融合：dst 轴所有 op；src 轴仅 CopyObject 且**始终**评估（与目标桶有无
        // 策略无关——旧实现把 src 评估嵌在 dst 有策略分支内，dst 无策略时 src Deny 被完全
        // 跳过：verifier 阻断 1a）。策略读/解析失败按轴 fail-closed 拒（不静默放行）。
        // 分类内 op 若无 s3_action 映射则 fail-closed（不能把桶策略静默当成不存在）。
        // 匿名请求不进本方法（`check` 已分流，见上）。
        let (mut saw_deny, mut saw_error, mut saw_allow) = (false, false, false);
        if !delegated && let Some(b) = &bucket {
            match self.bucket_policy(b).await {
                Ok(Some(doc)) => {
                    let pr = crate::policy::PolicyRequest {
                        op,
                        ak,
                        uid: user.uid,
                        account_id: account_id.as_deref(),
                        bucket: b,
                        key,
                        anonymous: false,
                    };
                    // CopyObject 的目标桶策略管目标写（不走 s3_action 通用映射）
                    let act = if is_copy(op) {
                        crate::policy::s3_action("PutObject")
                    } else {
                        crate::policy::s3_action(op)
                    };
                    if let Some(a) = act {
                        match crate::policy::evaluate(&doc, &pr, a) {
                            Some(false) => {
                                allow_dst = false;
                                saw_deny = true;
                            }
                            Some(true) => {
                                allow_dst = true;
                                saw_allow = true;
                            }
                            None => {} // 无匹配：回落既有判定
                        }
                    } else if classify(op).is_some() {
                        // 分类表内 op 必须有映射（不变量）；缺映射 = 桶策略无法求值 → 拒
                        tracing::error!(op, bucket = %b, "op 无 s3_action 映射——桶策略 fail-closed");
                        allow_dst = false;
                        saw_error = true;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, bucket = %b, "桶策略读取/解析失败——fail-closed 拒绝");
                    allow_dst = false;
                    saw_error = true;
                }
            }
        }
        if is_copy(op) {
            // 源桶策略管源读，**独立于目标桶策略**评估（Deny 优先：src Deny 压过 dst Allow
            // ——否则有目标桶写权者可绕过源桶 Deny 拷贝机密）。源桶策略读取失败同样 fail-closed。
            if let Some((src_bucket, src_key)) = copy_src {
                match self.bucket_policy(src_bucket).await {
                    Ok(Some(sdoc)) => {
                        let spr = crate::policy::PolicyRequest {
                            op,
                            ak,
                            uid: user.uid,
                            account_id: account_id.as_deref(),
                            bucket: src_bucket,
                            key: Some(src_key),
                            anonymous: false,
                        };
                        if let Some(sa) = crate::policy::s3_action("GetObject") {
                            match crate::policy::evaluate(&sdoc, &spr, sa) {
                                Some(false) => {
                                    allow_src = false;
                                    saw_deny = true;
                                }
                                Some(true) => {
                                    allow_src = true;
                                    saw_allow = true;
                                }
                                None => {}
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!(error = %e, bucket = %src_bucket, "源桶策略读取失败——fail-closed");
                        allow_src = false;
                        saw_error = true;
                    }
                }
            }
        }
        // —— 会话策略融合（M5 切片 5 / S8）：在表三档 × 桶策略融合之后叠**交集** ——
        // 语义本体在 `fuse_session`（抽出以便单测直钉——S3AccessContext 不可外部构造）。
        // **P2-1 复验修复**：CopyObject 的 src 轴必须用**源桶/源键**构造 PolicyRequest 求值
        // （与上面桶策略 src 轴同构）——曾复用 dst 的 pr，会话对源读的收窄可被 copy 绕过。
        let mut session_detail: Option<&'static str> = None;
        // 会话策略是身份上限：ListBuckets/CreateBucket 无桶也必须过交集（旧实现只在请求
        // 带桶时融合，会话键因此可列桶/建桶）。无桶路径用通配资源求值。
        if !delegated && (bucket.is_some() || matches!(op, "ListBuckets" | "CreateBucket")) {
            let session_bucket = bucket.unwrap_or("*");
            // CopyObject 源解析一次持有（String 落外层作用域，PolicyRequest 借之）
            let src_pr = copy_src.map(|(src_bucket, src_key)| crate::policy::PolicyRequest {
                op,
                ak,
                uid: user.uid,
                account_id: account_id.as_deref(),
                bucket: src_bucket,
                key: Some(src_key),
                anonymous: false,
            });
            let ((d, s), detail) = self
                .fuse_session(
                    crate::policy::PolicyRequest {
                        op,
                        ak,
                        uid: user.uid,
                        account_id: account_id.as_deref(),
                        bucket: session_bucket,
                        key,
                        anonymous: false,
                    },
                    src_pr,
                    allow_dst,
                    allow_src,
                )
                .await
                .map_err(acl_err)?;
            allow_dst = d;
            allow_src = s;
            session_detail = detail;
        }

        let allow = allow_dst && allow_src;
        // 审计注解：只标「策略实际决定了方向」的判定；拒绝侧 Deny > error 优先归类
        let policy_detail: Option<&str> = if allow {
            if saw_allow {
                Some("policy:allow")
            } else {
                None
            }
        } else if saw_deny {
            Some("policy:deny")
        } else if saw_error {
            Some("policy:error")
        } else {
            None
        };
        // 会话判定缀后（policy 在前、session 在后；「;」分隔供审计查询 LIKE 匹配）
        let detail: Option<String> = match (policy_detail, session_detail) {
            (Some(p), Some(s)) => Some(format!("{p};{s}")),
            (Some(p), None) => Some(p.to_string()),
            (None, Some(s)) => Some(s.to_string()),
            (None, None) => None,
        };
        Ok((allow_dst && allow_src, detail))
    }
}

#[async_trait::async_trait]
impl S3Access for Acl {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // op 先取成 owned：下文匿名分支需要 &mut cx 塞扩展，不能持有 cx 的借用。
        let op = cx.s3_op().name().to_string();
        let bucket: Option<String> = cx.s3_path().get_bucket_name().map(str::to_owned);
        let key: Option<String> = cx.s3_path().as_object().map(|(_, k)| k.to_string());
        // 先克隆 AK（credentials() 借自 cx，与随后的 extensions_mut 冲突）。
        // 无凭据 = 匿名：仅桶策略数据面白名单，显式 Allow 必需、Deny 优先。
        let Some(ak) = cx.credentials().map(|c| c.access_key.clone()) else {
            return self
                .check_anonymous(cx, &op, bucket.as_deref(), key.as_deref())
                .await;
        };
        let user = self
            .user_of_key(&ak)
            .await
            .map_err(acl_err)?
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "unknown access key"))?;
        // handler 读取：ListBuckets 过滤 / CreateBucket 记账 / DeleteObjects 逐键复评
        //（随请求搬进 S3Request.extensions）
        cx.extensions_mut().insert(user.clone());
        // 管理员旁路**表级三档**，但不再提前 return：桶策略的显式 Deny 对管理员同样生效
        // （AWS 语义），且管理员请求照常落审计。旧实现此处直接 Ok，Deny 与审计双双落空
        // （verifier 阻断 2——两侧都要，admin 与子账户各自被检查项覆盖）。
        let op = op.as_str();
        let copy_src: Option<(String, String)> = if is_copy(op) {
            match cx
                .headers()
                .get(X_AMZ_COPY_SOURCE)
                .and_then(|v| v.to_str().ok())
                .map(CopySource::parse)
            {
                Some(Ok(CopySource::Bucket { bucket, key, .. })) => {
                    Some((bucket.to_string(), key.to_string()))
                }
                _ => None,
            }
        } else {
            None
        };
        let (allow, detail) = self
            .check_op(
                &user,
                &ak,
                op,
                bucket.as_deref(),
                key.as_deref(),
                copy_src.as_ref().map(|(b, k)| (b.as_str(), k.as_str())),
            )
            .await?;

        // 审计（允许与拒绝都记；failed 只 warn——审计不可用时不得反向影响判定）
        if let Err(e) = self
            .write_audit(
                user.uid,
                op,
                bucket.as_deref(),
                key.as_deref(),
                if allow { "allow" } else { "deny" },
                detail.as_deref(),
            )
            .await
        {
            tracing::warn!(error = %e, "审计落行失败");
        }

        if allow {
            Ok(())
        } else {
            tracing::warn!(uid = user.uid, op, bucket = ?bucket, "授权拒绝");
            Err(s3s::s3_error!(AccessDenied, "access denied"))
        }
    }
}

/// op → 门槛类型的纯映射（独立成函数以便表驱动单测；`None` = 未映射 op，fail-closed）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// 已认证即可（ListBuckets 由 handler 过滤；CreateBucket 建后记账 owner）
    Open,
    /// 桶主或管理员
    OwnerBucket,
    /// 桶级 读/写/删 三档
    Need(Action),
    /// 目标写 + 源读
    Copy,
}

/// 复制类 op（目标写 + 源读双轴；CopyObject / UploadPartCopy）。
pub(crate) fn is_copy(op: &str) -> bool {
    matches!(op, "CopyObject" | "UploadPartCopy")
}

fn classify(op: &str) -> Option<Gate> {
    Some(match op {
        "ListBuckets" | "CreateBucket" => Gate::Open,
        "DeleteBucket" => Gate::OwnerBucket,
        // M4 版本控制：桶级状态改成开版本控制 = 桶主/管理员；读状态与列版本归读
        "PutBucketVersioning" => Gate::OwnerBucket,
        "GetBucketVersioning" => Gate::Need(Action::Read),
        // M6 切片 0/4：桶通知与复制配置（写配置归桶主，读归读档）
        "PutBucketNotificationConfiguration" => Gate::OwnerBucket,
        "GetBucketNotificationConfiguration" => Gate::Need(Action::Read),
        "PutBucketReplication" => Gate::OwnerBucket,
        "GetBucketReplication" => Gate::Need(Action::Read),
        "DeleteBucketReplication" => Gate::OwnerBucket,
        // M8 S4b：生命周期配置（写配置 = 桶主，读 = 读档）；RestoreObject 是写动作
        "PutBucketLifecycleConfiguration" | "DeleteBucketLifecycle" => Gate::OwnerBucket,
        "GetBucketLifecycleConfiguration" => Gate::Need(Action::Read),
        // M8 缺口 C：桶默认加密（写配置 = 桶主，读 = 读档）
        "PutBucketEncryption" | "DeleteBucketEncryption" => Gate::OwnerBucket,
        "GetBucketEncryption" => Gate::Need(Action::Read),
        // M9 S6c（B16/B17）：桶策略写入 = 桶主/管理员（与 console put_policy 同口径）
        "PutBucketPolicy" | "DeleteBucketPolicy" => Gate::OwnerBucket,
        // M8 S5：Inventory 配置（写配置 = 桶主，读 = 读档）
        "PutBucketInventoryConfiguration" | "DeleteBucketInventoryConfiguration" => {
            Gate::OwnerBucket
        }
        "GetBucketInventoryConfiguration" | "ListBucketInventoryConfigurations" => {
            Gate::Need(Action::Read)
        }
        "RestoreObject" => Gate::Need(Action::Write),
        // 读（HeadBucket 归读：AWS 语义绑 ListBucket）
        "HeadBucket"
        | "ListObjects"
        | "ListObjectsV2"
        | "ListMultipartUploads"
        | "ListParts"
        | "GetObject"
        | "HeadObject"
        | "GetObjectTagging"
        | "GetObjectRetention"
        | "GetObjectLegalHold"
        | "ListObjectVersions"
        | "SelectObjectContent" => Gate::Need(Action::Read),
        // 写（Abort 归写——它是写路径的清理；PostObject 的 trait 默认实现委托
        // put_object，op 名看到的是 "PostObject"，不映射它就是浏览器表单直传的漏检点）
        "PutObject"
        | "PostObject"
        | "PutObjectTagging"
        | "DeleteObjectTagging"
        | "PutObjectRetention"
        | "PutObjectLegalHold"
        | "CreateMultipartUpload"
        | "UploadPart"
        | "CompleteMultipartUpload"
        | "AbortMultipartUpload" => Gate::Need(Action::Write),
        // 删
        "DeleteObject" | "DeleteObjects" => Gate::Need(Action::Delete),
        "CopyObject" | "UploadPartCopy" => Gate::Copy,
        // 未实现/未知 op：fail-closed（下游本来也是 501；这里统一 403）
        _ => return None,
    })
}

impl Default for Acl {
    fn default() -> Self {
        Self::new()
    }
}
