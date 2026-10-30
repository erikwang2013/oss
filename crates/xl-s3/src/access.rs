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

use futures::StreamExt as _;
use http::HeaderMap;
use sqlx::{Row, SqlitePool};

use xl_store::mesh::{ControlQuery, ControlRow, MeshTransport};

use s3s::access::{S3Access, S3AccessContext};
use s3s::dto::CopySource;
use s3s::header::X_AMZ_COPY_SOURCE;
use s3s::{S3Error, S3ErrorCode, S3Result};

/// 最小 xl-s3 夹具/未迁移旧库没有账户表：调用方按 uid 派生账户兼容处理。
fn missing_accounts_table(e: &sqlx::Error) -> bool {
    e.as_database_error().is_some_and(|d| {
        let m = d.message();
        m.contains("no such table") && (m.contains("account_members") || m.contains("accounts"))
    })
}

/// 最小夹具/旧库没有 users 表：Owner 是可选元数据，退化为不回写而不是 list 失败。
fn missing_users_table(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .is_some_and(|d| d.message().contains("no such table") && d.message().contains("users"))
}

/// 账户桶数计数 SQL（与 oss-server `Db::account_buckets_from` 同口径）：桶主**当前**
/// 账户（成员行优先，无行 = uid 派生账户）；管理员桶不计入任何用户账户。
const ACCOUNT_BUCKET_COUNT_SQL: &str = "SELECT COUNT(*) FROM buckets b \
     JOIN users u ON u.id = b.owner_id \
     LEFT JOIN account_members bm ON bm.uid = u.id \
     WHERE u.is_admin = 0 AND COALESCE(bm.account_id, printf('%012d', u.id)) = ?";

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

/// 控制面读扇出（Task 9）：本地未命中时向 peers 尽力扇出、**首答胜**（不缓存起步，
/// ponytail：加 5s TTL 缓存的升级点与撤销延迟权衡记档于计划）。
#[derive(Clone)]
pub struct ControlMesh {
    pub transport: Arc<dyn MeshTransport>,
    /// 本节点 id（仅日志/防呆用）
    pub self_id: String,
    /// 除本机外的节点 id
    pub peers: Vec<String>,
}

impl ControlMesh {
    /// 并发扇出，首个 Some 胜；全 miss / 全错 → None（调用方 fail-closed）。
    /// 失败落 warn（不许静默——排查现场全靠它）。
    pub async fn ask(&self, q: ControlQuery) -> Option<ControlRow> {
        let mut futs = futures::stream::FuturesUnordered::new();
        for p in &self.peers {
            let t = self.transport.clone();
            let n = p.clone();
            let q = q.clone();
            futs.push(async move {
                match t.control_query(&n, q).await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(node = %n, error = %e, "控制面扇出失败");
                        None
                    }
                }
            });
        }
        while let Some(r) = futs.next().await {
            if let Some(row) = r {
                return Some(row);
            }
        }
        None
    }
}

/// 审计接收方（S3 请求路径只入队，不碰 DB）。实现方须自行持有字符串并保证有界；
/// `false` = 丢弃（队列满/已关闭）——审计 best-effort，调用方不得据此改变判定。
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
    pool: SqlitePool,
    /// 集群控制面扇出（None = 单节点：本地查无即拒，fail-closed 语义不变）
    mesh: Option<ControlMesh>,
    /// M1：匿名拒绝审计限流器（进程级；Clone 共享同一桶）。
    anon_deny: Arc<AnonDenyLimiter>,
    /// 异步审计接收方（None = 同步直写，测试/离线命令语义）。
    audit: Option<Arc<dyn AuditSink>>,
}

/// 匿名拒绝审计的令牌桶：未认证请求可无限灌入，拒绝审计不能线性写 SQLite。
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

impl Acl {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            mesh: None,
            anon_deny: Arc::new(AnonDenyLimiter::new()),
            audit: None,
        }
    }

    /// 硬配额记账/采样复用同一控制库连接池（同 crate 服务层）。
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// 集群形态（Task 9）：读路径本地 miss → peers 扇出（写入仍只落本节点；强一致归 M9）。
    pub fn with_mesh(pool: SqlitePool, mesh: Option<ControlMesh>) -> Self {
        Self {
            pool,
            mesh,
            anon_deny: Arc::new(AnonDenyLimiter::new()),
            audit: None,
        }
    }

    /// 注入异步审计接收方（serve 路径）；None = 保持同步直写。
    pub fn with_audit_sink(mut self, sink: Option<Arc<dyn AuditSink>>) -> Self {
        self.audit = sink;
        self
    }

    /// 桶版本控制状态（M4 切片 3；`buckets.versioning`）：无行/旧库缺列值 → "Off"（fail-closed 兼容）。
    pub async fn bucket_versioning(&self, bucket: &str) -> Result<String, sqlx::Error> {
        let row = sqlx::query("SELECT versioning FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(|r| r.get::<String, _>("versioning"))
            .unwrap_or_else(|| "Off".to_string()))
    }

    /// M9：桶复制模式行；行不存在 = None（建桶前判「重建不得改模式」）。
    pub async fn bucket_replication_mode_row(
        &self,
        bucket: &str,
    ) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query("SELECT replication_mode FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get::<String, _>("replication_mode")))
    }

    /// M9：桶复制模式（行不存在 = "async"，与缺行旧桶同口径）。
    pub async fn bucket_replication_mode(&self, bucket: &str) -> Result<String, sqlx::Error> {
        Ok(self
            .bucket_replication_mode_row(bucket)
            .await?
            .unwrap_or_else(|| "async".to_string()))
    }

    /// M9 S3：sync 建桶前置——数据站点 peer 数必须恰为 1（witness 不写 `sites` 行）。
    pub async fn site_peer_count(&self) -> Result<usize, sqlx::Error> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sites")
            .fetch_one(&self.pool)
            .await?;
        Ok(n as usize)
    }

    /// M5 切片 4：桶对象锁状态 → `Some((enabled, 默认模式, 默认天数))`；**行不存在 = None**
    /// （N4 复验补：调用方可据此区分「桶不存在(404)」与「锁未开(400)」）。
    pub async fn bucket_lock(
        &self,
        bucket: &str,
    ) -> Result<Option<(bool, Option<String>, Option<i64>)>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT object_lock, object_lock_default_mode, object_lock_default_days \
             FROM buckets WHERE name = ?",
        )
        .bind(bucket)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                r.get::<i64, _>("object_lock") != 0,
                r.get::<Option<String>, _>("object_lock_default_mode"),
                r.get::<Option<i64>, _>("object_lock_default_days"),
            )
        }))
    }

    /// M5 切片 4：开桶锁——`object_lock=1` **且自动开版本控制**（AWS：锁须版本化，建桶带
    /// `x-amz-bucket-object-lock-enabled` 时二者一并生效）。**Upsert**：建桶旗标可能先于记账
    /// 行到达（两条调用序都安全）；冲突时只改锁/版本列，不碰 owner/set/creation_date。
    pub async fn enable_bucket_lock(&self, bucket: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, set_id, object_lock) \
             VALUES (?, NULL, ?, 'Enabled', 0, 1) \
             ON CONFLICT(name) DO UPDATE SET object_lock = 1, versioning = 'Enabled'",
        )
        .bind(bucket)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// M5 切片 4：设/改默认保留规则（仅锁已开启桶；调用方先校验；None = 清空规则）。
    pub async fn set_bucket_lock_rule(
        &self,
        bucket: &str,
        mode: Option<&str>,
        days: Option<i64>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE buckets SET object_lock_default_mode = ?, object_lock_default_days = ? \
             WHERE name = ?",
        )
        .bind(mode)
        .bind(days)
        .bind(bucket)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 桶策略读取（M4 切片 7）：None = 未配；**解析失败 = Err**（check 里 fail-closed 拒）。
    pub async fn bucket_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<crate::policy::PolicyDoc>, sqlx::Error> {
        let row = sqlx::query("SELECT policy_json FROM bucket_policies WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        let Some(r) = row else { return Ok(None) };
        let json: String = r.get("policy_json");
        serde_json::from_str(&json)
            .map(Some)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))
    }

    /// 审计落行（判定后；允许与拒绝都记；best-effort 由调用方 warn）。
    /// 注入 [`AuditSink`] 时只入队（同步、非阻塞）；否则直接写 SQLite（测试/离线命令）。
    pub async fn write_audit(
        &self,
        uid: i64,
        op: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
        decision: &str,
        detail: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        if let Some(sink) = &self.audit {
            // 丢弃计数与告警由 sink 负责（每次请求 warn 会在过载时洪水式刷日志）。
            sink.enqueue(uid, op, bucket, object_key, decision, detail);
            return Ok(());
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        sqlx::query(
            "INSERT INTO audit_log (ts, uid, op, bucket, object_key, decision, detail) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(now)
        .bind(uid)
        .bind(op)
        .bind(bucket)
        .bind(object_key)
        .bind(decision)
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 匿名拒绝审计（限流；uid=0 哨兵）。返回 Ok 不代表已落行——洪水期会丢弃。
    pub async fn write_anonymous_deny_audit(
        &self,
        op: &str,
        bucket: Option<&str>,
        object_key: Option<&str>,
        detail: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        if !self.anon_deny.try_acquire(op, bucket, object_key) {
            return Ok(());
        }
        self.write_audit(0, op, bucket, object_key, "deny", detail)
            .await
    }

    /// 清桶策略（删桶时调用——与生命周期规则同款 S6-1 语义）。
    pub async fn clear_bucket_policy(&self, bucket: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// M6 切片 0：桶通知配置（None = 未配置；json 原文，解析在 notify 模块）。
    pub async fn bucket_notification(&self, bucket: &str) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query("SELECT config_json FROM event_configs WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("config_json")))
    }

    /// M6 切片 0：写桶通知配置（全量替换）。
    pub async fn set_bucket_notification(
        &self,
        bucket: &str,
        config_json: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET config_json = excluded.config_json, \
             updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(config_json)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// M6 切片 0：删桶时清通知配置（防同名重建继承旧规则）。
    pub async fn clear_bucket_notification(&self, bucket: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM event_configs WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// M8 缺口 C：桶默认加密（"AES256"/"aws:kms"；None = 未配置）。
    pub async fn bucket_default_encryption(
        &self,
        bucket: &str,
    ) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query("SELECT default_encryption FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| r.get::<Option<String>, _>("default_encryption")))
    }

    /// M8 缺口 C：写桶默认加密（全量替换；桶行不存在 → false）。
    pub async fn set_bucket_default_encryption(
        &self,
        bucket: &str,
        algorithm: &str,
    ) -> Result<bool, sqlx::Error> {
        let res = sqlx::query("UPDATE buckets SET default_encryption = ? WHERE name = ?")
            .bind(algorithm)
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// M8 缺口 C：清桶默认加密（幂等；返回是否命中桶行）。
    pub async fn clear_bucket_default_encryption(&self, bucket: &str) -> Result<bool, sqlx::Error> {
        let res = sqlx::query("UPDATE buckets SET default_encryption = NULL WHERE name = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// 清桶生命周期规则（M4 切片 6：删桶时调用——桶名可复用，旧规则复活会误删新桶
    /// 对象；verifier S6-1）。表在共享 DB（migrations 0001），S3 面与控制面同口径。
    pub async fn clear_lifecycle_rules(&self, bucket: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(())
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
    ) -> Result<(bool, Option<&'static str>), sqlx::Error> {
        let account_id = self.account_id_of_user(user.uid).await?;
        let decision = match self.bucket_policy(bucket).await? {
            Some(doc) => {
                let pr = crate::policy::PolicyRequest {
                    op: "DeleteObjects",
                    ak: &user.ak,
                    uid: user.uid,
                    account_id: account_id.as_deref(),
                    bucket,
                    key: Some(key),
                    anonymous: false,
                };
                crate::policy::s3_action("DeleteObject")
                    .and_then(|a| crate::policy::evaluate(&doc, &pr, a))
            }
            None => None,
        };
        let (allowed, detail) = match decision {
            Some(true) => (true, None),
            Some(false) => (false, Some("policy:deny")),
            None => (self.can(user.clone(), Action::Delete, bucket).await?, None),
        };
        // M5 切片 5（S8）：会话策略在这里同步叠交集——批量删逐键判定与 check() 的单键
        // 口径必须同源，否则会话策略可被一条批量删整体绕过。会话 Deny/无匹配 → 拒。
        if allowed && let Some(sdoc) = self.session_policy_of(&user.ak).await? {
            let pr = crate::policy::PolicyRequest {
                op: "DeleteObjects",
                ak: &user.ak,
                uid: user.uid,
                account_id: account_id.as_deref(),
                bucket,
                key: Some(key),
                anonymous: false,
            };
            match crate::policy::s3_action("DeleteObject")
                .and_then(|a| crate::policy::evaluate(&sdoc, &pr, a))
            {
                Some(true) => {}
                _ => return Ok((false, Some("session:deny"))),
            }
        }
        Ok((allowed, detail))
    }

    /// 匿名 DeleteObjects 逐键判定：**仅**桶策略，无表三档回落（匿名不继承任何用户权限）。
    /// 与签名路径同口径：显式 Deny 优先、无匹配即拒。
    pub async fn anonymous_delete_decision(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(bool, Option<&'static str>), sqlx::Error> {
        let Some(doc) = self.bucket_policy(bucket).await? else {
            return Ok((false, Some("anonymous:no-policy")));
        };
        let pr = crate::policy::PolicyRequest {
            op: "DeleteObjects",
            ak: "",
            uid: 0,
            account_id: None,
            bucket,
            key: Some(key),
            anonymous: true,
        };
        Ok(
            match crate::policy::s3_action("DeleteObject")
                .and_then(|a| crate::policy::evaluate(&doc, &pr, a))
            {
                Some(true) => (true, None),
                Some(false) => (false, Some("policy:deny")),
                None => (false, Some("anonymous:no-match")),
            },
        )
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
    ) -> Result<((bool, bool), Option<&'static str>), sqlx::Error> {
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
        // P2-1 复验修复：src 轴在**源桶/源键**的 pr 上求值（调用方自 CopySource 构造；
        // 解析失败 → None，此时 Gate::Copy 基线已置 allow_src=false，fail-closed 不塌）
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
    ) -> Result<bool, sqlx::Error> {
        let res = sqlx::query("UPDATE buckets SET versioning = ? WHERE name = ?")
            .bind(status)
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// AK → (uid, is_admin)。status/expires 条件与 `SigV4Auth::get_secret_key` 同口径
    ///（认证面与授权面查的是同一行，任一处撤销即同时失效）；**账号停用（users.status）
    /// 同样使 AK 失效**——停用即全链路出局。
    pub async fn user_of_key(&self, access_key: &str) -> Result<Option<AuthedUser>, sqlx::Error> {
        // M5 切片 5：与 SigV4Auth::get_secret_key 同口径（含会话父链条件——三处 SQL 逐字
        // 一致：auth.rs / 此处 / cluster_control AccessKey；P3-1 补父**用户**停用同样传播）。
        let row = sqlx::query(
            "SELECT ak.user_id AS user_id, u.is_admin AS is_admin \
             FROM access_keys ak JOIN users u ON u.id = ak.user_id \
             LEFT JOIN access_keys p ON p.access_key = ak.parent_ak \
             LEFT JOIN users pu ON pu.id = p.user_id \
             LEFT JOIN account_members am ON am.uid = u.id \
             LEFT JOIN accounts a ON a.id = am.account_id \
             WHERE ak.access_key = ? AND ak.status = 'active' AND u.status = 'active' \
               AND (am.uid IS NULL OR a.status = 'active') \
               AND (ak.expires_at IS NULL OR ak.expires_at > ?) \
               AND (ak.parent_ak IS NULL OR (p.kind = 'long-term' \
                    AND p.status = 'active' AND (p.expires_at IS NULL OR p.expires_at > ?) \
                    AND pu.status = 'active'))",
        )
        .bind(access_key)
        .bind(now())
        .bind(now())
        .fetch_optional(&self.pool)
        .await;
        // 最小 xl-s3 夹具/未迁移旧库：无账户表时按旧语义（账户状态不参与）认证。
        let row = match row {
            Ok(row) => row,
            Err(e) if missing_accounts_table(&e) => {
                sqlx::query(
                    "SELECT ak.user_id AS user_id, u.is_admin AS is_admin \
                 FROM access_keys ak JOIN users u ON u.id = ak.user_id \
                 LEFT JOIN access_keys p ON p.access_key = ak.parent_ak \
                 LEFT JOIN users pu ON pu.id = p.user_id \
                 WHERE ak.access_key = ? AND ak.status = 'active' AND u.status = 'active' \
                   AND (ak.expires_at IS NULL OR ak.expires_at > ?) \
                   AND (ak.parent_ak IS NULL OR (p.kind = 'long-term' \
                        AND p.status = 'active' AND (p.expires_at IS NULL OR p.expires_at > ?) \
                        AND pu.status = 'active'))",
                )
                .bind(access_key)
                .bind(now())
                .bind(now())
                .fetch_optional(&self.pool)
                .await?
            }
            Err(e) => return Err(e),
        };
        if let Some(r) = row {
            return Ok(Some(AuthedUser {
                uid: r.get("user_id"),
                is_admin: r.get::<i64, _>("is_admin") != 0,
                ak: access_key.to_string(),
            }));
        }
        // 集群（Task 9）：本地查无 → 扇出（节点 B 建的号/签的 AK，节点 A 也认）
        if let Some(m) = &self.mesh
            && let Some(row) = m.ask(ControlQuery::AccessKey(access_key.to_string())).await
        {
            return Ok(Some(AuthedUser {
                uid: row.uid.unwrap_or(0),
                is_admin: row.is_admin.unwrap_or(false),
                ak: access_key.to_string(),
            }));
        }
        Ok(None)
    }

    /// AC-0：用户当前账户（成员行优先，无行回落 uid 派生账户）。
    /// 策略主体把账户根 ARN 映射到账户成员集合时使用；查询失败由调用方 fail-closed。
    pub async fn account_id_of_user(&self, uid: i64) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query("SELECT account_id FROM account_members WHERE uid = ?")
            .bind(uid)
            .fetch_optional(&self.pool)
            .await;
        match row {
            Ok(row) => Ok(row
                .map(|r| r.get::<String, _>("account_id"))
                .or_else(|| account_id_of(uid))),
            // 最小 xl-s3 夹具/未迁移旧库没有账户表：保持派生账户兼容语义。
            Err(e) if missing_accounts_table(&e) => Ok(account_id_of(uid)),
            Err(e) => Err(e),
        }
    }

    /// AP 跨账户：请求者账户 + 有效状态（成员行优先，无成员行回落 uid 派生账户）。
    /// - 成员行：账户行必须存在且 `status='active'`（缺失/停用 → false，fail-closed）；
    /// - 无成员行：派生账户实体行存在时按状态，不存在按 legacy 兼容视为有效。
    ///
    /// 查询失败由调用方 fail-closed（5xx）。
    pub async fn account_identity_of_user(
        &self,
        uid: i64,
    ) -> Result<(Option<String>, bool), sqlx::Error> {
        let row = sqlx::query(
            "SELECT am.account_id AS account_id, a.status AS status \
             FROM account_members am LEFT JOIN accounts a ON a.id = am.account_id \
             WHERE am.uid = ?",
        )
        .bind(uid)
        .fetch_optional(&self.pool)
        .await;
        let row = match row {
            Ok(row) => row,
            Err(e) if missing_accounts_table(&e) => {
                return Ok((account_id_of(uid), true));
            }
            Err(e) => return Err(e),
        };
        if let Some(r) = row {
            let account: String = r.get("account_id");
            let status: Option<String> = r.get("status");
            return Ok((Some(account), status.as_deref() == Some("active")));
        }
        let Some(account) = account_id_of(uid) else {
            return Ok((None, false));
        };
        let status: Option<String> = sqlx::query_scalar("SELECT status FROM accounts WHERE id = ?")
            .bind(&account)
            .fetch_optional(&self.pool)
            .await?;
        Ok((
            Some(account),
            status.as_deref().is_none_or(|s| s == "active"),
        ))
    }

    /// AC-2：账户桶数硬配额（`accounts.bucket_quota`；NULL/负 = 不限）。
    /// 返回 true = 已到上限，拒绝建桶。查询失败由调用方 fail-closed（5xx）。
    /// 这是快速检查（文案/错误码兼容）；权威判定在 standalone 写事务内、Raft leader
    /// append 前，并发建桶不会越过 `accounts.bucket_quota`。
    pub async fn bucket_quota_exceeded(&self, uid: i64) -> Result<bool, sqlx::Error> {
        let row = sqlx::query(
            "SELECT a.id AS id, a.status AS status, a.bucket_quota AS quota \
             FROM account_members am JOIN accounts a ON a.id = am.account_id \
             WHERE am.uid = ?",
        )
        .bind(uid)
        .fetch_optional(&self.pool)
        .await;
        let row = match row {
            Ok(row) => row,
            Err(e) if missing_accounts_table(&e) => return Ok(false),
            Err(e) => return Err(e),
        };
        // 无成员行 → uid 派生账户；该账户行可能不存在（新用户/未回填）→ 无配额。
        let (acct, status, quota) = match row {
            Some(r) => (
                r.get::<String, _>("id"),
                r.get::<String, _>("status"),
                r.get::<Option<i64>, _>("quota"),
            ),
            None => {
                let Some(acct) = account_id_of(uid) else {
                    return Ok(false);
                };
                let Some(r) = sqlx::query("SELECT status, bucket_quota FROM accounts WHERE id = ?")
                    .bind(&acct)
                    .fetch_optional(&self.pool)
                    .await?
                else {
                    return Ok(false);
                };
                (
                    acct,
                    r.get::<String, _>("status"),
                    r.get::<Option<i64>, _>("bucket_quota"),
                )
            }
        };
        // 账户停用：认证层已拒；此处兜底（fail-closed）。
        if status != "active" {
            return Ok(true);
        }
        let Some(quota) = quota.filter(|q| *q >= 0) else {
            return Ok(false);
        };
        let n: i64 = sqlx::query_scalar(ACCOUNT_BUCKET_COUNT_SQL)
            .bind(&acct)
            .fetch_one(&self.pool)
            .await?;
        Ok(n >= quota)
    }

    /// 写事务内配额判定（调用方已持 `BEGIN IMMEDIATE` 写锁）：与
    /// [`Self::bucket_quota_exceeded`] 同口径（成员行优先，无行回落 uid 派生账户）；
    /// NULL/负配额、无账户行 = 不限；停用账户 fail-closed。
    async fn quota_reached_conn(
        conn: &mut sqlx::SqliteConnection,
        uid: i64,
    ) -> Result<bool, sqlx::Error> {
        let account = match sqlx::query_scalar::<_, String>(
            "SELECT account_id FROM account_members WHERE uid = ?",
        )
        .bind(uid)
        .fetch_optional(&mut *conn)
        .await
        {
            Ok(row) => row.or_else(|| account_id_of(uid)),
            Err(e) if missing_accounts_table(&e) => return Ok(false),
            Err(e) => return Err(e),
        };
        let Some(account) = account else {
            return Ok(false);
        };
        let row = sqlx::query("SELECT status, bucket_quota FROM accounts WHERE id = ?")
            .bind(&account)
            .fetch_optional(&mut *conn)
            .await;
        let row = match row {
            Ok(row) => row,
            Err(e) if missing_accounts_table(&e) => return Ok(false),
            Err(e) => return Err(e),
        };
        let Some(r) = row else {
            return Ok(false);
        };
        if r.get::<String, _>("status") != "active" {
            return Ok(true);
        }
        let Some(quota) = r.get::<Option<i64>, _>("bucket_quota").filter(|q| *q >= 0) else {
            return Ok(false);
        };
        let n: i64 = sqlx::query_scalar(ACCOUNT_BUCKET_COUNT_SQL)
            .bind(&account)
            .fetch_one(&mut *conn)
            .await?;
        Ok(n >= quota)
    }

    /// M5 切片 5（S8）：AK 的会话策略（`kind='session'` 且 `session_policy` 非空时）。
    /// 本地查无 → 扇出（远端应答带 `session_policy`；NULL/空 = 无约束，与线上口径一致）。
    /// 解析失败 → Err（调用方 fail-closed 拒）。非会话键 → `None`。
    pub async fn session_policy_of(
        &self,
        access_key: &str,
    ) -> Result<Option<crate::policy::PolicyDoc>, sqlx::Error> {
        // 空串 = 控制台/aether 合成身份（AuthedUser 口径——各自面不走策略）→ 无会话
        if access_key.is_empty() {
            return Ok(None);
        }
        let local: Option<Option<String>> =
            sqlx::query_scalar("SELECT session_policy FROM access_keys WHERE access_key = ?")
                .bind(access_key)
                .fetch_optional(&self.pool)
                .await?;
        let raw = match local {
            Some(sp) => sp,
            None => {
                // 远端铸的会话键：扇出取策略原文（ControlRow.session_policy）。
                // P3-2 复验修复：本地无行 + 扇出**无答**（全不可达）→ Err（fail-closed）——
                // 「答了但无策略」与「没人答得上来」不可混同，后者静默放行即 fail-open
                // （user_of_key 此前已答过，本处再答失败 = 竞态窗口）。
                match &self.mesh {
                    Some(m) => match m.ask(ControlQuery::AccessKey(access_key.to_string())).await {
                        Some(row) => row.session_policy,
                        None => {
                            return Err(sqlx::Error::Protocol(
                                "会话策略扇出不可达（fail-closed）".into(),
                            ));
                        }
                    },
                    None => None,
                }
            }
        };
        let Some(raw) = raw.filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        let doc: crate::policy::PolicyDoc =
            serde_json::from_str(&raw).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(Some(doc))
    }

    /// 桶主？（`buckets.owner_id == uid`）
    pub async fn is_owner(&self, bucket: &str, uid: i64) -> Result<bool, sqlx::Error> {
        let row = sqlx::query("SELECT 1 FROM buckets WHERE name = ? AND owner_id = ?")
            .bind(bucket)
            .bind(uid)
            .fetch_optional(&self.pool)
            .await?;
        if row.is_some() {
            return Ok(true);
        }
        // 集群：归属记账可能在对端（本地无行）→ 扇出比对
        if let Some(m) = &self.mesh
            && let Some(row) = m.ask(ControlQuery::BucketOwner(bucket.to_string())).await
        {
            return Ok(row.uid == Some(uid));
        }
        Ok(false)
    }

    /// ListObjects 的 Owner 回写：桶主 username；管理员桶/无 DB 行回落管理员。
    /// 返回 `(id, display_name)`，id 用本地 uid 的十进制串（本模型的稳定主体标识）。
    pub async fn bucket_owner_display(
        &self,
        bucket: &str,
    ) -> Result<Option<(String, String)>, sqlx::Error> {
        let row = match sqlx::query(
            "SELECT b.owner_id AS owner_id, u.username AS username FROM buckets b \
             LEFT JOIN users u ON u.id = b.owner_id WHERE b.name = ?",
        )
        .bind(bucket)
        .fetch_optional(&self.pool)
        .await
        {
            Ok(r) => r,
            Err(e) if missing_users_table(&e) => return Ok(None),
            Err(e) => return Err(e),
        };
        if let Some(r) = row {
            let id: Option<i64> = r.get("owner_id");
            let name: Option<String> = r.get("username");
            if let (Some(id), Some(name)) = (id, name) {
                return Ok(Some((id.to_string(), name)));
            }
        }
        let admin = match sqlx::query(
            "SELECT id, username FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        {
            Ok(r) => r,
            Err(e) if missing_users_table(&e) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(admin.map(|r| {
            let id: i64 = r.get("id");
            let name: String = r.get("username");
            (id.to_string(), name)
        }))
    }

    /// 主判权：admin / 桶主 全允许；否则查对应能力位（无行 = 拒）。
    pub async fn can(
        &self,
        user: AuthedUser,
        action: Action,
        bucket: &str,
    ) -> Result<bool, sqlx::Error> {
        if user.is_admin {
            return Ok(true);
        }
        if self.is_owner(bucket, user.uid).await? {
            return Ok(true);
        }
        // 三份静态字面量（sqlx 0.9 拒绝动态 SQL 字符串；字面量也更利于审计）
        let sql = match action {
            Action::Read => {
                "SELECT 1 FROM permissions WHERE user_id = ? AND bucket = ? AND can_read = 1"
            }
            Action::Write => {
                "SELECT 1 FROM permissions WHERE user_id = ? AND bucket = ? AND can_write = 1"
            }
            Action::Delete => {
                "SELECT 1 FROM permissions WHERE user_id = ? AND bucket = ? AND can_delete = 1"
            }
        };
        let row = sqlx::query(sql)
            .bind(user.uid)
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        if row.is_some() {
            return Ok(true);
        }
        // 集群（Task 9）：本地无行 → 扇出（对端本地语义 = owner ∨ permission 位；
        // admin 已在发起端判定，对端无需重判）
        if let Some(m) = &self.mesh
            && let Some(r) = m
                .ask(ControlQuery::Permission {
                    uid: user.uid,
                    bucket: bucket.to_string(),
                    action: action as u8,
                })
                .await
        {
            return Ok(r.flag.unwrap_or(false));
        }
        Ok(false)
    }

    /// 可见桶集合（列举过滤）：自有桶 ∪ 在 `permissions` 任一能力位为 1 的桶。
    /// 与磁盘列表**求交**使用（磁盘是权威——历史桶可能没有 DB 行）。
    pub async fn visible(&self, uid: i64) -> Result<Vec<String>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT name AS b FROM buckets WHERE owner_id = ? \
             UNION \
             SELECT bucket AS b FROM permissions WHERE user_id = ? \
               AND (can_read = 1 OR can_write = 1 OR can_delete = 1)",
        )
        .bind(uid)
        .bind(uid)
        .fetch_all(&self.pool)
        .await?;
        let mut names: Vec<String> = rows.into_iter().map(|r| r.get("b")).collect();
        // 集群：并集对端可见集（去重）
        if let Some(m) = &self.mesh
            && let Some(r) = m.ask(ControlQuery::VisibleBuckets(uid)).await
            && let Some(peers) = r.names
        {
            for n in peers {
                if !names.contains(&n) {
                    names.push(n);
                }
            }
        }
        Ok(names)
    }

    /// 桶归属登记（历史形态，无 set；幂等——已存在不覆盖，测试钉）。
    pub async fn record_bucket_owner(&self, bucket: &str, uid: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT OR IGNORE INTO buckets \
             (name, owner_id, creation_date, versioning, set_id, object_lock, \
              object_lock_default_mode, object_lock_default_days, default_encryption, \
              replication_mode, deleting) \
             VALUES (?, ?, ?, 'Off', 0, 0, NULL, NULL, NULL, 'async', 0)",
        )
        .bind(bucket)
        .bind(uid)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
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
    ) -> Result<BucketRecordOutcome, sqlx::Error> {
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
    ) -> Result<BucketRecordOutcome, sqlx::Error> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&mut *tx)
            .await?;
        if exists.is_none() && Self::quota_reached_conn(&mut tx, uid).await? {
            return Ok(BucketRecordOutcome::QuotaExceeded);
        }
        sqlx::query(
            "INSERT INTO buckets \
             (name, owner_id, creation_date, versioning, set_id, object_lock, \
              object_lock_default_mode, object_lock_default_days, default_encryption, \
              replication_mode, deleting) \
             VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, NULL, ?, 0) \
             ON CONFLICT(name) DO UPDATE SET \
              owner_id = excluded.owner_id, set_id = excluded.set_id, \
              versioning = excluded.versioning, \
              object_lock = excluded.object_lock, \
              object_lock_default_mode = excluded.object_lock_default_mode, \
              object_lock_default_days = excluded.object_lock_default_days, \
              default_encryption = excluded.default_encryption, \
              replication_mode = excluded.replication_mode, deleting = excluded.deleting",
        )
        .bind(bucket)
        .bind(uid)
        .bind(now())
        // M9：sync 桶建桶即 Enabled（双站同 id 依赖版本链）；async 保持 Off。
        .bind(if replication_mode == "sync" {
            "Enabled"
        } else {
            "Off"
        })
        .bind(set as i64)
        .bind(replication_mode)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(BucketRecordOutcome::Written)
    }

    /// C3：建桶自动指派——选**当前桶数最少**的 set（并列取小号）；无行数据按 0 计。
    /// 单 set/单机 k=1 恒 0（零查询）。指派只影响**新桶**；已有桶不迁（S-L 口径）。
    pub async fn pick_least_loaded_set(&self, k: u16) -> Result<u16, sqlx::Error> {
        if k <= 1 {
            return Ok(0);
        }
        let rows = sqlx::query("SELECT set_id, COUNT(*) AS c FROM buckets GROUP BY set_id")
            .fetch_all(&self.pool)
            .await?;
        let mut counts: std::collections::HashMap<u16, i64> = std::collections::HashMap::new();
        for r in &rows {
            let s = r.get::<i64, _>("set_id");
            if let Ok(s) = u16::try_from(s) {
                counts.insert(s, r.get::<i64, _>("c"));
            }
        }
        let mut best = 0u16;
        let mut best_c = i64::MAX;
        for i in 0..k {
            let c = counts.get(&i).copied().unwrap_or(0);
            if c < best_c {
                best = i;
                best_c = c;
            }
        }
        Ok(best)
    }

    /// 删桶后的记账清理（S3 与控制台两条删桶路径共用同一实现）：
    /// 清该桶的授权行（防重建同名桶继承旧授权）+ 清桶行。
    pub async fn forget_bucket(&self, bucket: &str) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM permissions WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
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
pub(crate) fn acl_err(e: sqlx::Error) -> S3Error {
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

/// Task 9 测试支撑：假传输（只答 `control_query`；其余方法显式未实现）。
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use xl_store::error::{Error, Result};
    use xl_store::mesh::{ControlQuery, ControlRow, MeshTransport};

    /// 节点 → 应答（`None` = 该节点查无；不在表内 = RPC 失败——模拟不可达）。
    #[derive(Debug, Default)]
    pub struct FakeMesh {
        pub answers: Mutex<HashMap<String, Option<ControlRow>>>,
    }

    impl FakeMesh {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }
        pub fn answer(&self, node: &str, row: Option<ControlRow>) {
            self.answers.lock().unwrap().insert(node.to_string(), row);
        }
    }

    fn unimpl(op: &str) -> Error {
        Error::Encode(format!("FakeMesh 未实现：{op}"))
    }

    #[async_trait::async_trait]
    impl MeshTransport for FakeMesh {
        async fn control_query(&self, node: &str, _q: ControlQuery) -> Result<Option<ControlRow>> {
            match self.answers.lock().unwrap().get(node) {
                Some(row) => Ok(row.clone()),
                None => Err(unimpl("control_query（该节点不可达）")),
            }
        }
        async fn write_shard(&self, _n: &str, _w: xl_store::mesh::ShardWrite) -> Result<()> {
            Err(unimpl("write_shard"))
        }
        async fn read_shard(
            &self,
            _n: &str,
            _r: xl_store::mesh::ShardRead,
        ) -> Result<Option<bytes::Bytes>> {
            Err(unimpl("read_shard"))
        }
        async fn delete_version(&self, _n: &str, _d: xl_store::mesh::VersionDirRef) -> Result<()> {
            Err(unimpl("delete_version"))
        }
        async fn recycle_version(&self, _n: &str, _d: xl_store::mesh::VersionDirRef) -> Result<()> {
            Err(unimpl("recycle_version"))
        }
        async fn delete_shard(&self, _n: &str, _d: xl_store::mesh::ShardDelete) -> Result<()> {
            Err(unimpl("delete_shard"))
        }
        async fn prepare_meta(
            &self,
            _n: &str,
            _m: xl_store::mesh::MetaWrite,
        ) -> Result<xl_store::mesh::MetaAck> {
            Err(unimpl("prepare_meta"))
        }
        async fn commit_meta(
            &self,
            _n: &str,
            _m: xl_store::mesh::MetaWrite,
        ) -> Result<xl_store::mesh::MetaAck> {
            Err(unimpl("commit_meta"))
        }
        async fn abort_meta(&self, _n: &str, _m: xl_store::mesh::MetaWrite) -> Result<()> {
            Err(unimpl("abort_meta"))
        }
        async fn read_meta(
            &self,
            _n: &str,
            _r: xl_store::mesh::MetaRead,
        ) -> Result<xl_store::mesh::MetaSnapshot> {
            Err(unimpl("read_meta"))
        }
        async fn delete_meta(&self, _n: &str, _r: xl_store::mesh::MetaRead) -> Result<()> {
            Err(unimpl("delete_meta"))
        }
        async fn ensure_bucket(&self, _n: &str, _b: &str) -> Result<()> {
            Err(unimpl("ensure_bucket"))
        }
        async fn delete_bucket_dir(&self, _n: &str, _b: &str) -> Result<()> {
            Err(unimpl("delete_bucket_dir"))
        }
        async fn list_bucket_dirs(&self, _n: &str) -> Result<Vec<String>> {
            Err(unimpl("list_bucket_dirs"))
        }
        async fn list_local(
            &self,
            _n: &str,
            _l: xl_store::mesh::ListLocal,
        ) -> Result<xl_store::mesh::ListLocalResp> {
            Err(unimpl("list_local"))
        }
        async fn health(&self, _n: &str) -> Result<xl_store::mesh::NodeHealth> {
            Err(unimpl("health"))
        }
        async fn lock(
            &self,
            _n: &str,
            _r: xl_store::mesh::LockReq,
        ) -> Result<xl_store::mesh::LockResp> {
            Err(unimpl("lock"))
        }
    }
}

#[cfg(test)]
mod pick_set_tests {
    use super::*;

    /// C3：最少桶数优先；并列取小号；k=1 恒 0 且零查询。
    #[tokio::test]
    async fn pick_least_loaded_set() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, set_id INTEGER NOT NULL DEFAULT 0);",
        )
        .execute(&pool)
        .await
        .unwrap();
        let acl = Acl::new(pool.clone());
        assert_eq!(acl.pick_least_loaded_set(1).await.unwrap(), 0, "k=1 恒 0");
        // 空库 → 0（并列取小号）
        assert_eq!(acl.pick_least_loaded_set(3).await.unwrap(), 0);
        for n in ["a", "b"] {
            sqlx::query("INSERT INTO buckets (name, creation_date, set_id) VALUES (?, 0, 0)")
                .bind(n)
                .execute(&pool)
                .await
                .unwrap();
        }
        // set0=2, set1=0, set2=0 → 取 1
        assert_eq!(acl.pick_least_loaded_set(3).await.unwrap(), 1);
        sqlx::query("INSERT INTO buckets (name, creation_date, set_id) VALUES ('c', 0, 1)")
            .execute(&pool)
            .await
            .unwrap();
        // set0=2, set1=1, set2=0 → 取 2
        assert_eq!(acl.pick_least_loaded_set(3).await.unwrap(), 2);
        sqlx::query("INSERT INTO buckets (name, creation_date, set_id) VALUES ('d', 0, 2)")
            .execute(&pool)
            .await
            .unwrap();
        // 2/1/1 → 取 1（并列取小号）
        assert_eq!(acl.pick_least_loaded_set(3).await.unwrap(), 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Task 1：账户 ID 边界 = 恰好 12 位 ASCII 数字（全角/符号/长度偏差均拒绝）。
    #[test]
    fn valid_account_id_requires_exactly_12_ascii_digits() {
        assert!(valid_account_id("000000000000"));
        assert!(valid_account_id("123456789012"));
        assert!(!valid_account_id(""));
        assert!(!valid_account_id("12345678901"), "11 位");
        assert!(!valid_account_id("1234567890123"), "13 位");
        assert!(!valid_account_id("12345678901a"));
        assert!(!valid_account_id(" 12345678901"));
        assert!(!valid_account_id("-12345678901"));
        assert!(!valid_account_id("+12345678901"));
        assert!(!valid_account_id("１２３４５６７８９０１２"), "全角数字");
    }

    /// 最小 schema 的内联建库（列与 oss-server/migrations/0001_init.sql 对齐；
    /// xl-s3 不能反向依赖 oss-server，模式同 auth.rs 的测试 helper）。
    pub(super) async fn acl() -> Acl {
        acl_with_url("sqlite::memory:").await
    }

    /// 并发写事务用例用文件库（WAL/busy_timeout）；内存 shared-cache 库在并发
    /// `BEGIN IMMEDIATE` 下以 `SQLITE_LOCKED` 立即失败，测不出写锁排队语义。
    async fn acl_with_url(url: &str) -> Acl {
        let pool = SqlitePool::connect(url).await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, is_admin INTEGER NOT NULL DEFAULT 0, \
              status TEXT NOT NULL DEFAULT 'active');\
             CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, user_id INTEGER NOT NULL, \
              status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL, \
              parent_ak TEXT, session_policy TEXT, kind TEXT NOT NULL DEFAULT 'long-term');\
             CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, \
              versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0, \
              object_lock INTEGER NOT NULL DEFAULT 0, object_lock_default_mode TEXT, \
              object_lock_default_days INTEGER, default_encryption TEXT, \
              replication_mode TEXT NOT NULL DEFAULT 'async', deleting INTEGER NOT NULL DEFAULT 0);\
             CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
              can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, can_delete INTEGER NOT NULL DEFAULT 0, \
              PRIMARY KEY (user_id, bucket));\
             CREATE TABLE bucket_policies (bucket TEXT PRIMARY KEY, policy_json TEXT NOT NULL);\
             CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, \
              uid INTEGER NOT NULL, op TEXT NOT NULL, bucket TEXT, object_key TEXT, \
              decision TEXT NOT NULL, detail TEXT);\
             CREATE TABLE accounts (id TEXT PRIMARY KEY, name TEXT NOT NULL, \
              status TEXT NOT NULL DEFAULT 'active', admin_uid INTEGER, bucket_quota INTEGER, \
              created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);\
             CREATE TABLE account_members (account_id TEXT NOT NULL, uid INTEGER NOT NULL UNIQUE, \
              role TEXT NOT NULL DEFAULT 'member', created_at INTEGER NOT NULL, \
              PRIMARY KEY (account_id, uid));",
        )
        .execute(&pool)
        .await
        .unwrap();
        Acl::new(pool)
    }

    pub(super) async fn add_user(a: &Acl, id: i64, admin: bool) {
        sqlx::query("INSERT INTO users (id, username, is_admin) VALUES (?, ?, ?)")
            .bind(id)
            .bind(format!("u{id}"))
            .bind(admin as i64)
            .execute(&a.pool)
            .await
            .unwrap();
    }

    pub(super) async fn add_key(a: &Acl, ak: &str, uid: i64, expires_at: Option<i64>) {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, expires_at, created_at) VALUES (?, 'sk', ?, ?, 0)",
        )
        .bind(ak)
        .bind(uid)
        .bind(expires_at)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    pub(super) async fn add_bucket(a: &Acl, name: &str, owner: Option<i64>) {
        sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES (?, ?, 0)")
            .bind(name)
            .bind(owner)
            .execute(&a.pool)
            .await
            .unwrap();
    }

    async fn add_account(a: &Acl, id: &str, status: &str, quota: Option<i64>) {
        sqlx::query(
            "INSERT INTO accounts (id, name, status, bucket_quota, created_at, updated_at) \
             VALUES (?, ?, ?, ?, 0, 0)",
        )
        .bind(id)
        .bind(format!("acct-{id}"))
        .bind(status)
        .bind(quota)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    async fn add_member(a: &Acl, account_id: &str, uid: i64, role: &str) {
        sqlx::query(
            "INSERT INTO account_members (account_id, uid, role, created_at) VALUES (?, ?, ?, 0)",
        )
        .bind(account_id)
        .bind(uid)
        .bind(role)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    async fn set_policy(a: &Acl, bucket: &str, json: &str) {
        sqlx::query(
            "INSERT INTO bucket_policies (bucket, policy_json) VALUES (?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET policy_json = excluded.policy_json",
        )
        .bind(bucket)
        .bind(json)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    async fn grant(a: &Acl, uid: i64, bucket: &str, r: bool, w: bool, d: bool) {
        sqlx::query(
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(uid)
        .bind(bucket)
        .bind(r as i64)
        .bind(w as i64)
        .bind(d as i64)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    const SUB: AuthedUser = AuthedUser {
        uid: 2,
        is_admin: false,
        ak: String::new(),
    };
    const ADMIN: AuthedUser = AuthedUser {
        uid: 1,
        is_admin: true,
        ak: String::new(),
    };

    #[test]
    fn op_classification_table() {
        // 全量映射 + 三个易错点（PostObject 漏检、Abort 归写、HeadBucket 归读）
        // + fail-closed 默认臂。**S3 面 403 事故的回归**：对象级 op 必须命中这张表——
        // 曾经的 bug 在 check 的桶名解析（as_bucket 对 Object 路径返回 None），
        // 该段依赖 s3s 上下文、无法在此构造，由 e2e_s3.sh 的 15a 实弹钉住。
        assert_eq!(classify("ListBuckets"), Some(Gate::Open));
        assert_eq!(classify("CreateBucket"), Some(Gate::Open));
        assert_eq!(classify("DeleteBucket"), Some(Gate::OwnerBucket));
        // M4 切片 3：开版本控制 = 桶主/管理员；查状态归读
        assert_eq!(classify("PutBucketVersioning"), Some(Gate::OwnerBucket));
        assert_eq!(
            classify("GetBucketVersioning"),
            Some(Gate::Need(Action::Read))
        );
        for op in [
            "HeadBucket",
            "ListObjects",
            "ListObjectsV2",
            "ListMultipartUploads",
            "ListParts",
            "ListObjectVersions",
            "GetObject",
            "HeadObject",
            "SelectObjectContent",
            "GetObjectTagging",
            "GetObjectRetention",
            "GetObjectLegalHold",
            "GetBucketLifecycleConfiguration",
            "GetBucketInventoryConfiguration",
            "ListBucketInventoryConfigurations",
            "GetBucketEncryption",
        ] {
            assert_eq!(classify(op), Some(Gate::Need(Action::Read)), "{op}");
        }
        for op in [
            "PutObject",
            "PostObject",
            "CreateMultipartUpload",
            "UploadPart",
            "CompleteMultipartUpload",
            "AbortMultipartUpload",
            "PutObjectTagging",
            "DeleteObjectTagging",
            "PutObjectRetention",
            "PutObjectLegalHold",
            "RestoreObject",
        ] {
            assert_eq!(classify(op), Some(Gate::Need(Action::Write)), "{op}");
        }
        for op in ["DeleteObject", "DeleteObjects"] {
            assert_eq!(classify(op), Some(Gate::Need(Action::Delete)), "{op}");
        }
        for op in ["CopyObject", "UploadPartCopy"] {
            assert_eq!(classify(op), Some(Gate::Copy), "{op}");
        }
        for op in [
            "PutBucketLifecycleConfiguration",
            "DeleteBucketLifecycle",
            "PutBucketInventoryConfiguration",
            "DeleteBucketInventoryConfiguration",
            "PutBucketEncryption",
            "DeleteBucketEncryption",
            // M9 S6c：S3 桶策略写已实现（Raft 一条命令；M7 仍 NotImplemented）
            "PutBucketPolicy",
            "DeleteBucketPolicy",
        ] {
            assert_eq!(classify(op), Some(Gate::OwnerBucket), "{op}");
        }
        // 未映射 op → fail-closed
        assert_eq!(classify("Frobnicate"), None);
    }

    /// 覆盖不变量：`classify` 接受的每个 op 都必须有 `s3_action` 映射，否则桶策略/会话
    /// 策略在 check_op 里无法求值（授权路径 fail-closed）。CopyObject 是唯一例外——目标
    /// 写/源读双轴由调用方拆成 PutObject/GetObject（见 `is_copy` 分支）。
    /// 本表是全量清单：给 classify 加分支时必须同步补这里。
    #[test]
    fn every_classified_op_has_action_mapping() {
        const CLASSIFIED: &[&str] = &[
            "ListBuckets",
            "CreateBucket",
            "DeleteBucket",
            "PutBucketVersioning",
            "GetBucketVersioning",
            "PutBucketNotificationConfiguration",
            "GetBucketNotificationConfiguration",
            "PutBucketReplication",
            "GetBucketReplication",
            "DeleteBucketReplication",
            "PutBucketLifecycleConfiguration",
            "DeleteBucketLifecycle",
            "GetBucketLifecycleConfiguration",
            "PutBucketEncryption",
            "DeleteBucketEncryption",
            "GetBucketEncryption",
            "PutBucketPolicy",
            "DeleteBucketPolicy",
            "PutBucketInventoryConfiguration",
            "DeleteBucketInventoryConfiguration",
            "GetBucketInventoryConfiguration",
            "ListBucketInventoryConfigurations",
            "RestoreObject",
            "HeadBucket",
            "ListObjects",
            "ListObjectsV2",
            "ListMultipartUploads",
            "ListParts",
            "ListObjectVersions",
            "GetObject",
            "HeadObject",
            "GetObjectTagging",
            "GetObjectRetention",
            "GetObjectLegalHold",
            "SelectObjectContent",
            "PutObject",
            "PostObject",
            "PutObjectTagging",
            "DeleteObjectTagging",
            "PutObjectRetention",
            "PutObjectLegalHold",
            "CreateMultipartUpload",
            "UploadPart",
            "CompleteMultipartUpload",
            "AbortMultipartUpload",
            "DeleteObject",
            "DeleteObjects",
            "CopyObject",
            "UploadPartCopy",
        ];
        for op in CLASSIFIED {
            assert!(classify(op).is_some(), "{op} 不在 classify 表中");
            assert!(
                *op == "CopyObject" || crate::policy::s3_action(op).is_some(),
                "classify 接受的 {op} 缺少 s3_action 映射"
            );
        }
    }

    /// M1：匿名拒绝审计限流（允许不限；拒绝突发 20 后丢弃）。
    #[test]
    fn anonymous_deny_limiter_burst_then_drop() {
        let l = AnonDenyLimiter::with_config(ANON_DENY_BURST, ANON_DENY_PER_SEC, 0.0);
        for _ in 0..ANON_DENY_BURST as usize {
            assert!(l.try_acquire("GetObject", Some("b"), Some("k")));
        }
        assert!(
            !l.try_acquire("GetObject", Some("b"), Some("k")),
            "突发额度耗尽后必须丢弃"
        );
        // 不同键仍受同一令牌桶约束（去重关闭）
        assert!(!l.try_acquire("GetObject", Some("b"), Some("k2")));
    }

    /// 4.6：去重窗口内同 (op,bucket,key) 只记一次；窗口关闭时不去重。
    #[test]
    fn anonymous_deny_limiter_dedupe_window() {
        let l = AnonDenyLimiter::with_config(10.0, 10.0, 60.0);
        assert!(l.try_acquire("GetObject", Some("b"), Some("k")));
        assert!(!l.try_acquire("GetObject", Some("b"), Some("k")));
        assert!(l.try_acquire("GetObject", Some("b"), Some("other")));
        assert!(l.try_acquire("PutObject", Some("b"), Some("k")));

        let off = AnonDenyLimiter::with_config(10.0, 10.0, 0.0);
        assert!(off.try_acquire("GetObject", Some("b"), Some("k")));
        assert!(off.try_acquire("GetObject", Some("b"), Some("k")));
    }

    /// 4.6：界外/非法配置回落默认值（不改变限流安全默认）。
    #[test]
    fn anonymous_deny_limiter_bounds() {
        assert_eq!(
            super::env_f64("OSS_TEST_UNSET_VAR_S3H", 5.0, 1.0, 10.0),
            5.0
        );
    }

    /// M1：写入路径确实经限流（20 条后不再增长；放行 1 个 token 的时钟余量）。
    #[tokio::test]
    async fn anonymous_deny_audit_writes_are_capped() {
        let a = acl().await;
        for _ in 0..30 {
            a.write_anonymous_deny_audit("GetObject", Some("b"), Some("k"), None)
                .await
                .unwrap();
        }
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
            .fetch_one(&a.pool)
            .await
            .unwrap();
        assert!((20..=21).contains(&n), "匿名拒绝审计被限流：{n}");
    }

    #[derive(Debug, PartialEq)]
    struct AuditRow {
        uid: i64,
        op: String,
        bucket: Option<String>,
        key: Option<String>,
        decision: String,
        detail: Option<String>,
    }

    /// 假 sink：记录入队行；`accept=false` 模拟队列满/关闭。
    struct RecordingSink {
        rows: std::sync::Mutex<Vec<AuditRow>>,
        accept: bool,
    }

    impl AuditSink for RecordingSink {
        fn enqueue(
            &self,
            uid: i64,
            op: &str,
            bucket: Option<&str>,
            object_key: Option<&str>,
            decision: &str,
            detail: Option<&str>,
        ) -> bool {
            self.rows.lock().unwrap().push(AuditRow {
                uid,
                op: op.to_string(),
                bucket: bucket.map(str::to_string),
                key: object_key.map(str::to_string),
                decision: decision.to_string(),
                detail: detail.map(str::to_string),
            });
            self.accept
        }
    }

    /// 注入 sink 后允许/拒绝都只入队，不同步落库。
    #[tokio::test]
    async fn audit_sink_enqueues_instead_of_sqlite() {
        let a = acl().await;
        let sink = Arc::new(RecordingSink {
            rows: std::sync::Mutex::new(Vec::new()),
            accept: true,
        });
        let a = a.with_audit_sink(Some(sink.clone()));
        a.write_audit(
            7,
            "PutObject",
            Some("b"),
            Some("k"),
            "allow",
            Some("policy:allow"),
        )
        .await
        .unwrap();
        a.write_anonymous_deny_audit(
            "GetObject",
            Some("b"),
            Some("k"),
            Some("anonymous:no-policy"),
        )
        .await
        .unwrap();
        {
            let rows = sink.rows.lock().unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].uid, 7);
            assert_eq!(rows[0].op, "PutObject");
            assert_eq!(rows[0].decision, "allow");
            assert_eq!(rows[1].uid, 0, "匿名哨兵");
            assert_eq!(rows[1].decision, "deny");
        }
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
            .fetch_one(&a.pool)
            .await
            .unwrap();
        assert_eq!(n, 0, "注入 sink 后不应同步写 audit_log");
    }

    /// sink 拒收（队列满）时 `write_audit` 仍 Ok——审计不得影响判定。
    #[tokio::test]
    async fn audit_sink_rejection_does_not_fail_request() {
        let a = acl().await;
        let a = a.with_audit_sink(Some(Arc::new(RecordingSink {
            rows: std::sync::Mutex::new(Vec::new()),
            accept: false,
        })));
        a.write_audit(1, "GetObject", Some("b"), Some("k"), "deny", None)
            .await
            .unwrap();
        a.write_anonymous_deny_audit("GetObject", Some("b"), Some("k"), None)
            .await
            .unwrap();
    }

    /// L1：匿名请求的对象锁/治理旁路头被识别（大小写不敏感由 HeaderMap 归一）。
    #[test]
    fn anonymous_lock_headers_are_rejected() {
        let mut h = HeaderMap::new();
        assert_eq!(anonymous_lock_header(&h), None);
        h.insert("x-amz-object-lock-mode", "GOVERNANCE".parse().unwrap());
        assert_eq!(anonymous_lock_header(&h), Some("x-amz-object-lock-mode"));
        h.clear();
        h.insert("x-amz-bypass-governance-retention", "true".parse().unwrap());
        assert_eq!(
            anonymous_lock_header(&h),
            Some("x-amz-bypass-governance-retention")
        );
    }

    #[tokio::test]
    async fn admin_and_owner_allow_everything() {
        let a = acl().await;
        add_user(&a, 1, true).await;
        add_user(&a, 2, false).await;
        add_bucket(&a, "b", Some(2)).await;
        for act in [Action::Read, Action::Write, Action::Delete] {
            assert!(a.can(ADMIN, act, "b").await.unwrap(), "admin 全允许");
            assert!(a.can(SUB, act, "b").await.unwrap(), "桶主全允许");
        }
    }

    #[tokio::test]
    async fn three_tiers_are_independent_and_fail_closed() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_bucket(&a, "b", None).await;
        grant(&a, 2, "b", true, false, false).await;
        assert!(a.can(SUB, Action::Read, "b").await.unwrap());
        assert!(!a.can(SUB, Action::Write, "b").await.unwrap());
        assert!(!a.can(SUB, Action::Delete, "b").await.unwrap());

        grant(&a, 2, "c", false, true, false).await;
        assert!(!a.can(SUB, Action::Read, "c").await.unwrap(), "写不含读");
        assert!(a.can(SUB, Action::Write, "c").await.unwrap());

        grant(&a, 2, "d", false, false, true).await;
        assert!(a.can(SUB, Action::Delete, "d").await.unwrap());

        // 无行 = 无权限（fail-closed）
        assert!(!a.can(SUB, Action::Read, "nope").await.unwrap());
    }

    /// 匿名放行面：显式 Allow 必需、资源前缀生效；写/删各自需要对应 action；
    /// 桶/控制面 op 即使策略 `s3:*` 也拒（fail-closed）。
    #[tokio::test]
    async fn anonymous_requires_explicit_allow_and_data_plane_only() {
        let a = acl().await;
        add_bucket(&a, "pub", None).await;
        // 无策略：数据面也拒
        let (allow, detail) = a
            .anonymous_decision("GetObject", Some("pub"), Some("a"))
            .await
            .unwrap();
        assert!(!allow);
        assert_eq!(detail.as_deref(), Some("anonymous:no-policy"));

        // Allow "*" 只覆盖 public/* 的读；HEAD 走同一 s3:GetObject
        set_policy(
            &a,
            "pub",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
                "Resource":"arn:aws:s3:::pub/public/*"}]}"#,
        )
        .await;
        for (op, key) in [
            ("GetObject", "public/a"),
            ("HeadObject", "public/a"),
            ("GetObject", "private/a"),
            ("PutObject", "public/a"),
            ("DeleteObject", "public/a"),
        ] {
            let (allow, _) = a
                .anonymous_decision(op, Some("pub"), Some(key))
                .await
                .unwrap();
            let read = matches!(op, "GetObject" | "HeadObject");
            assert_eq!(allow, read && key == "public/a", "{op} {key}");
        }
        // 前缀外 HEAD 同样拒
        assert!(
            !a.anonymous_decision("HeadObject", Some("pub"), Some("private/a"))
                .await
                .unwrap()
                .0
        );

        // 写/删动作各自需要显式 Allow
        set_policy(
            &a,
            "pub",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
                "Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],
                "Resource":"arn:aws:s3:::pub/public/*"}]}"#,
        )
        .await;
        for op in ["GetObject", "HeadObject", "PutObject", "DeleteObject"] {
            assert!(
                a.anonymous_decision(op, Some("pub"), Some("public/a"))
                    .await
                    .unwrap()
                    .0,
                "{op}"
            );
        }
        // DeleteObjects：check 阶段只要求存在策略，键级判定在 handler
        assert!(
            a.anonymous_decision("DeleteObjects", Some("pub"), None)
                .await
                .unwrap()
                .0
        );
        add_bucket(&a, "empty", None).await;
        assert!(
            !a.anonymous_decision("DeleteObjects", Some("empty"), None)
                .await
                .unwrap()
                .0,
            "无策略桶的匿名批量删必须在 check 层拒"
        );
        // 无桶路径
        assert!(
            !a.anonymous_decision("GetObject", None, Some("a"))
                .await
                .unwrap()
                .0
        );

        // 桶/控制面 op：策略全通配也一律拒
        set_policy(
            &a,
            "pub",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*",
                "Resource":["arn:aws:s3:::pub","arn:aws:s3:::pub/*"]}]}"#,
        )
        .await;
        let control_plane = [
            "ListBuckets",
            "ListObjects",
            "ListObjectsV2",
            "HeadBucket",
            "CreateBucket",
            "DeleteBucket",
            "PutBucketPolicy",
            "DeleteBucketPolicy",
            "GetBucketVersioning",
            "PutBucketVersioning",
            "CreateMultipartUpload",
            "UploadPart",
            "CompleteMultipartUpload",
            "AbortMultipartUpload",
            "ListParts",
            "ListMultipartUploads",
            "GetObjectTagging",
            "PutObjectTagging",
            "DeleteObjectTagging",
            "GetObjectRetention",
            "PutObjectRetention",
            "CopyObject",
            "UploadPartCopy",
            "PostObject",
            "RestoreObject",
            "SelectObjectContent",
            "GetBucketLifecycleConfiguration",
            "PutBucketLifecycleConfiguration",
            "DeleteBucketLifecycle",
            "GetBucketEncryption",
            "PutBucketEncryption",
            "PutBucketNotificationConfiguration",
            "PutBucketReplication",
            "GetBucketInventoryConfiguration",
            "PutBucketInventoryConfiguration",
            "Frobnicate",
        ];
        for op in control_plane {
            let (allow, detail) = a
                .anonymous_decision(op, Some("pub"), Some("public/a"))
                .await
                .unwrap();
            assert!(!allow, "匿名不得放行 {op}");
            assert_eq!(detail.as_deref(), Some("anonymous:not-data-plane"), "{op}");
        }

        // 策略读取/解析失败 → fail-closed 拒（不是 500）
        set_policy(&a, "pub", "{not-json").await;
        let (allow, detail) = a
            .anonymous_decision("GetObject", Some("pub"), Some("public/a"))
            .await
            .unwrap();
        assert!(!allow);
        assert_eq!(detail.as_deref(), Some("policy:error"));
    }

    /// 匿名 Deny 优先 + Principal 只认 "*"（uid:0/空 AK/具体 AK 不得命中）。
    #[tokio::test]
    async fn anonymous_deny_wins_and_principal_is_wildcard_only() {
        let a = acl().await;
        add_bucket(&a, "pub", None).await;
        set_policy(
            &a,
            "pub",
            r#"{"Statement":[
                {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
                 "Resource":"arn:aws:s3:::pub/*"},
                {"Effect":"Deny","Principal":{"AWS":["*"]},"Action":"s3:GetObject",
                 "Resource":"arn:aws:s3:::pub/secret*"}]}"#,
        )
        .await;
        assert!(
            a.anonymous_decision("GetObject", Some("pub"), Some("open"))
                .await
                .unwrap()
                .0
        );
        let (allow, detail) = a
            .anonymous_decision("GetObject", Some("pub"), Some("secret/x"))
            .await
            .unwrap();
        assert!(!allow);
        assert_eq!(detail.as_deref(), Some("policy:deny"));

        // 非通配 Principal 不因 uid=0 哨兵或空 AK 命中
        for principal in [r#""uid:0""#, r#""""#, r#""AKIA_OTHER""#] {
            set_policy(
                &a,
                "pub",
                &format!(
                    r#"{{"Statement":[{{"Effect":"Allow","Principal":{{"AWS":{principal}}},
                    "Action":"s3:GetObject","Resource":"arn:aws:s3:::pub/*"}}]}}"#
                ),
            )
            .await;
            let (allow, detail) = a
                .anonymous_decision("GetObject", Some("pub"), Some("open"))
                .await
                .unwrap();
            assert!(!allow, "匿名不得命中 Principal {principal}");
            assert_eq!(detail.as_deref(), Some("anonymous:no-match"));
        }
    }

    /// 匿名 DeleteObjects 逐键：策略 Allow 才放；Deny 优先；无匹配/无策略拒；不回落表三档。
    #[tokio::test]
    async fn anonymous_delete_objects_per_key_matrix() {
        let a = acl().await;
        add_bucket(&a, "pub", None).await;
        assert_eq!(
            a.anonymous_delete_decision("pub", "a").await.unwrap(),
            (false, Some("anonymous:no-policy"))
        );
        set_policy(
            &a,
            "pub",
            r#"{"Statement":[
                {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:DeleteObject",
                 "Resource":"arn:aws:s3:::pub/*"},
                {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:DeleteObject",
                 "Resource":"arn:aws:s3:::pub/secret*"}]}"#,
        )
        .await;
        assert_eq!(
            a.anonymous_delete_decision("pub", "public/a")
                .await
                .unwrap(),
            (true, None)
        );
        assert_eq!(
            a.anonymous_delete_decision("pub", "secret/a")
                .await
                .unwrap(),
            (false, Some("policy:deny"))
        );
        // 前缀式 Allow + 前缀外键：无匹配即拒
        set_policy(
            &a,
            "pub",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:DeleteObject",
                "Resource":"arn:aws:s3:::pub/public/*"}]}"#,
        )
        .await;
        assert_eq!(
            a.anonymous_delete_decision("pub", "other/a").await.unwrap(),
            (false, Some("anonymous:no-match"))
        );
        // uid:0 专属 Principal 不得放行匿名
        set_policy(
            &a,
            "pub",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"uid:0"},"Action":"s3:DeleteObject",
                "Resource":"arn:aws:s3:::pub/*"}]}"#,
        )
        .await;
        assert!(!a.anonymous_delete_decision("pub", "a").await.unwrap().0);
    }

    /// 签名路径不因匿名分支改变：桶主/策略提升/表三档口径保持原样。
    #[tokio::test]
    async fn signed_path_unaffected_by_anonymous_branch() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_user(&a, 3, false).await;
        add_bucket(&a, "owned", Some(2)).await;
        let owner = AuthedUser {
            uid: 2,
            is_admin: false,
            ak: "AK2".into(),
        };
        // 桶主无策略：签名放行；匿名同一桶无策略拒
        assert!(
            a.check_op(&owner, "AK2", "GetObject", Some("owned"), Some("k"), None)
                .await
                .unwrap()
                .0
        );
        assert!(
            !a.anonymous_decision("GetObject", Some("owned"), Some("k"))
                .await
                .unwrap()
                .0
        );

        // 非桶主无表权限：uid 专属策略 Allow 仍可提升签名用户（既有语义）；匿名不命中
        add_bucket(&a, "shared", None).await;
        set_policy(
            &a,
            "shared",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"uid:3"},"Action":"s3:GetObject",
                "Resource":"arn:aws:s3:::shared/*"}]}"#,
        )
        .await;
        let sub = AuthedUser {
            uid: 3,
            is_admin: false,
            ak: "AK3".into(),
        };
        assert!(
            a.check_op(&sub, "AK3", "GetObject", Some("shared"), Some("k"), None)
                .await
                .unwrap()
                .0
        );
        assert!(
            !a.anonymous_decision("GetObject", Some("shared"), Some("k"))
                .await
                .unwrap()
                .0
        );

        // 通配策略对签名用户语义不变（无表权限也可被策略 Allow 提升）；匿名同时放行
        set_policy(
            &a,
            "shared",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
                "Resource":"arn:aws:s3:::shared/*"}]}"#,
        )
        .await;
        assert!(
            a.check_op(&sub, "AK3", "GetObject", Some("shared"), Some("k"), None)
                .await
                .unwrap()
                .0
        );
        assert!(
            a.anonymous_decision("GetObject", Some("shared"), Some("k"))
                .await
                .unwrap()
                .0
        );
    }

    /// 桶策略 `Deny s3:*` 必须压住全部 classify 内 op——含此前无 s3_action 映射而
    /// 被静默跳过策略的 SelectObjectContent / GetBucketEncryption。
    #[tokio::test]
    async fn bucket_policy_deny_all_covers_previously_unmapped_ops() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_bucket(&a, "b", None).await;
        grant(&a, 2, "b", true, false, false).await;
        let u = AuthedUser {
            uid: 2,
            is_admin: false,
            ak: "AK2".into(),
        };
        // 基线：表三档读权放行这两个 op（证明随后拒绝来自桶策略）
        for (op, key) in [
            ("SelectObjectContent", Some("k")),
            ("GetBucketEncryption", None),
        ] {
            assert!(
                a.check_op(&u, "AK2", op, Some("b"), key, None)
                    .await
                    .unwrap()
                    .0,
                "{op} 基线应放行"
            );
        }
        set_policy(
            &a,
            "b",
            r#"{"Statement":[{"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:*",
                "Resource":["arn:aws:s3:::b","arn:aws:s3:::b/*"]}]}"#,
        )
        .await;
        for (op, key) in [
            ("SelectObjectContent", Some("k")),
            ("GetBucketEncryption", None),
        ] {
            let (allow, detail) = a
                .check_op(&u, "AK2", op, Some("b"), key, None)
                .await
                .unwrap();
            assert!(!allow, "Deny s3:* 必须压住 {op}");
            assert_eq!(detail.as_deref(), Some("policy:deny"), "{op}");
        }
    }

    #[tokio::test]
    async fn visible_is_owned_union_any_bit() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_bucket(&a, "owned", Some(2)).await;
        add_bucket(&a, "wr", None).await;
        add_bucket(&a, "none", None).await;
        grant(&a, 2, "wr", false, true, false).await;
        grant(&a, 2, "alloff", false, false, false).await;
        let mut v = a.visible(2).await.unwrap();
        v.sort();
        assert_eq!(v, vec!["owned".to_string(), "wr".to_string()]);
    }

    #[tokio::test]
    async fn user_of_key_respects_status_and_expiry() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_user(&a, 9, true).await;
        add_key(&a, "AK_OK", 2, None).await;
        add_key(&a, "AK_EXPIRED", 2, Some(1)).await; // 1970 → 已过期
        add_key(&a, "AK_ADMIN", 9, None).await;
        assert_eq!(
            a.user_of_key("AK_OK").await.unwrap(),
            Some(AuthedUser {
                uid: 2,
                is_admin: false,
                ak: "AK_OK".to_string()
            })
        );
        assert_eq!(a.user_of_key("AK_EXPIRED").await.unwrap(), None);
        assert_eq!(
            a.user_of_key("AK_ADMIN").await.unwrap(),
            Some(AuthedUser {
                uid: 9,
                is_admin: true,
                ak: "AK_ADMIN".to_string()
            })
        );
        assert_eq!(a.user_of_key("AK_UNKNOWN").await.unwrap(), None);

        // 账号停用 → AK 立即失效（全链路出局）
        sqlx::query("UPDATE users SET status = 'disabled' WHERE id = 2")
            .execute(&a.pool)
            .await
            .unwrap();
        assert_eq!(a.user_of_key("AK_OK").await.unwrap(), None);
    }

    /// AC-1：账户停用 → AK 在认证面立即失效（与 users.status 同口径）。
    #[tokio::test]
    async fn user_of_key_rejects_disabled_account() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_key(&a, "AK_OK", 2, None).await;
        add_account(&a, "000000000002", "disabled", None).await;
        add_member(&a, "000000000002", 2, "owner").await;
        assert_eq!(a.user_of_key("AK_OK").await.unwrap(), None);

        sqlx::query("UPDATE accounts SET status = 'active' WHERE id = '000000000002'")
            .execute(&a.pool)
            .await
            .unwrap();
        assert!(a.user_of_key("AK_OK").await.unwrap().is_some());
    }

    /// AC-0：账户根 ARN = 账户成员集合；无成员行回落 uid 派生账户；LEGACY 永不通配。
    #[tokio::test]
    async fn account_principal_matching_uses_membership_then_derived_fallback() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_user(&a, 3, false).await;
        add_account(&a, "123456789012", "active", None).await;
        add_member(&a, "123456789012", 2, "member").await;

        let team_arn = "arn:aws:iam::123456789012:root";
        assert_eq!(
            a.account_id_of_user(2).await.unwrap().as_deref(),
            Some("123456789012")
        );
        assert!(account_principal_matches(team_arn, 2, Some("123456789012")));
        assert!(!account_principal_matches(
            team_arn,
            3,
            Some("999999999999")
        ));
        // 无成员行 → 派生账户（存量用户 ARN 不变）
        assert!(account_principal_matches(
            "arn:aws:iam::000000000003:root",
            3,
            None
        ));
        assert!(!account_principal_matches(team_arn, 3, None));
        // 系统账户与 uid=0 永不匹配
        assert!(!account_principal_matches(
            "arn:aws:iam::000000000000:root",
            3,
            Some(LEGACY_ACCOUNT)
        ));
        assert!(!account_principal_matches(team_arn, 0, None));
        // 非 root 形态不命中
        assert!(!account_principal_matches(
            "arn:aws:iam::123456789012:user/x",
            2,
            Some("123456789012")
        ));

        // AP 跨账户用的账户身份快照：成员行优先 + 账户状态
        assert_eq!(
            a.account_identity_of_user(2).await.unwrap(),
            (Some("123456789012".to_string()), true)
        );
        sqlx::query("UPDATE accounts SET status = 'disabled' WHERE id = '123456789012'")
            .execute(&a.pool)
            .await
            .unwrap();
        assert_eq!(
            a.account_identity_of_user(2).await.unwrap(),
            (Some("123456789012".to_string()), false),
            "停用账户 → active=false"
        );
        // 无成员行 → 派生账户；实体行存在则按状态，缺失按 legacy 兼容有效
        assert_eq!(
            a.account_identity_of_user(3).await.unwrap(),
            (Some("000000000003".to_string()), true)
        );
        add_account(&a, "000000000003", "disabled", None).await;
        assert_eq!(
            a.account_identity_of_user(3).await.unwrap(),
            (Some("000000000003".to_string()), false),
            "派生的实体账户停用 → false"
        );
        // 成员行存在但账户行缺失 → fail-closed
        add_user(&a, 4, false).await;
        add_member(&a, "555555555555", 4, "member").await;
        assert_eq!(
            a.account_identity_of_user(4).await.unwrap(),
            (Some("555555555555".to_string()), false)
        );
    }

    /// AC-2：账户桶数硬配额（NULL/负 = 不限；停用账户 fail-closed 拒建桶）。
    #[tokio::test]
    async fn bucket_quota_counts_owned_buckets_and_rejects_at_limit() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_account(&a, "000000000002", "active", Some(1)).await;
        add_member(&a, "000000000002", 2, "owner").await;
        assert!(!a.bucket_quota_exceeded(2).await.unwrap(), "尚未建桶");
        add_bucket(&a, "one", Some(2)).await;
        assert!(a.bucket_quota_exceeded(2).await.unwrap(), "1/1 已满");

        sqlx::query("UPDATE accounts SET bucket_quota = 2 WHERE id = '000000000002'")
            .execute(&a.pool)
            .await
            .unwrap();
        assert!(!a.bucket_quota_exceeded(2).await.unwrap());
        sqlx::query("UPDATE accounts SET bucket_quota = NULL WHERE id = '000000000002'")
            .execute(&a.pool)
            .await
            .unwrap();
        assert!(!a.bucket_quota_exceeded(2).await.unwrap(), "NULL = 不限");

        sqlx::query("UPDATE accounts SET status = 'disabled' WHERE id = '000000000002'")
            .execute(&a.pool)
            .await
            .unwrap();
        assert!(
            a.bucket_quota_exceeded(2).await.unwrap(),
            "停用账户 fail-closed"
        );
    }

    /// AC-2：无成员行的用户按派生账户计数（legacy fallback）。
    #[tokio::test]
    async fn bucket_quota_falls_back_to_derived_account() {
        let a = acl().await;
        add_user(&a, 4, false).await;
        add_account(&a, "000000000004", "active", Some(1)).await;
        add_bucket(&a, "legacy", Some(4)).await;
        assert!(a.bucket_quota_exceeded(4).await.unwrap());
    }

    /// AC-2：S3/AP 共用的 `check_op` 在 CreateBucket 上执行配额门（管理员豁免）。
    #[tokio::test]
    async fn create_bucket_quota_gate_in_check_op() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_account(&a, "000000000002", "active", Some(0)).await;
        add_member(&a, "000000000002", 2, "owner").await;
        let sub = AuthedUser {
            uid: 2,
            is_admin: false,
            ak: "AK2".into(),
        };
        let err = a
            .check_op(&sub, "AK2", "CreateBucket", Some("new"), None, None)
            .await
            .unwrap_err();
        assert_eq!(*err.code(), S3ErrorCode::TooManyBuckets);

        // 管理员不受用户账户配额约束
        let admin = AuthedUser {
            uid: 2,
            is_admin: true,
            ak: "AK2".into(),
        };
        let (allow, _) = a
            .check_op(&admin, "AK2", "CreateBucket", Some("new"), None, None)
            .await
            .unwrap();
        assert!(allow);

        // 取消配额后放行
        sqlx::query("UPDATE accounts SET bucket_quota = NULL WHERE id = '000000000002'")
            .execute(&a.pool)
            .await
            .unwrap();
        let (allow, _) = a
            .check_op(&sub, "AK2", "CreateBucket", Some("new"), None, None)
            .await
            .unwrap();
        assert!(allow);
    }

    /// AC-2：建桶行写入在写锁内 count+insert——N+1 个并发建桶恰好 N 个越过配额。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_bucket_records_stop_at_quota() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("acl.db").display());
        let a = Arc::new(acl_with_url(&url).await);
        add_user(&a, 2, false).await;
        add_account(&a, "000000000002", "active", Some(2)).await;
        add_member(&a, "000000000002", 2, "owner").await;
        let mut tasks = Vec::new();
        for i in 0..3 {
            let a = Arc::clone(&a);
            tasks.push(tokio::spawn(async move {
                a.record_bucket_owner_set_mode(&format!("q-{i}"), 2, 0, "async")
                    .await
                    .unwrap()
            }));
        }
        let mut written = 0;
        for t in tasks {
            if t.await.unwrap() == BucketRecordOutcome::Written {
                written += 1;
            }
        }
        assert_eq!(written, 2, "N+1 并发恰好 N 个成功");
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets")
            .fetch_one(&a.pool)
            .await
            .unwrap();
        assert_eq!(n, 2, "落库桶数不得超过配额");
    }

    /// AC-2：无成员行的用户按 uid 派生账户在写事务内同样计数。
    #[tokio::test]
    async fn record_bucket_owner_quota_uses_derived_account() {
        let a = acl().await;
        add_user(&a, 4, false).await;
        add_account(&a, "000000000004", "active", Some(1)).await;
        assert_eq!(
            a.record_bucket_owner_set_mode("d-1", 4, 0, "async")
                .await
                .unwrap(),
            BucketRecordOutcome::Written
        );
        assert_eq!(
            a.record_bucket_owner_set_mode("d-2", 4, 0, "async")
                .await
                .unwrap(),
            BucketRecordOutcome::QuotaExceeded
        );
    }

    /// AC-2：NULL/负配额 = 不限；已存在行是重建收敛，不增加桶数、不判配额。
    #[tokio::test]
    async fn record_bucket_owner_quota_null_negative_and_recreate() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_account(&a, "000000000002", "active", None).await;
        add_member(&a, "000000000002", 2, "owner").await;
        for i in 0..3 {
            assert_eq!(
                a.record_bucket_owner_set_mode(&format!("u-{i}"), 2, 0, "async")
                    .await
                    .unwrap(),
                BucketRecordOutcome::Written,
                "NULL = 不限"
            );
        }
        sqlx::query("UPDATE accounts SET bucket_quota = -1 WHERE id = '000000000002'")
            .execute(&a.pool)
            .await
            .unwrap();
        assert_eq!(
            a.record_bucket_owner_set_mode("u-3", 2, 0, "async")
                .await
                .unwrap(),
            BucketRecordOutcome::Written,
            "负数 = 不限"
        );
        sqlx::query("UPDATE accounts SET bucket_quota = 3 WHERE id = '000000000002'")
            .execute(&a.pool)
            .await
            .unwrap();
        assert_eq!(
            a.record_bucket_owner_set_mode("u-0", 2, 1, "sync")
                .await
                .unwrap(),
            BucketRecordOutcome::Written,
            "重建已有桶不增加桶数"
        );
    }

    #[tokio::test]
    async fn record_bucket_owner_is_idempotent_and_preserves_existing() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_user(&a, 3, false).await;
        a.record_bucket_owner("b", 2).await.unwrap();
        a.record_bucket_owner("b", 3).await.unwrap(); // 二次不覆盖
        assert!(a.is_owner("b", 2).await.unwrap());
        assert!(!a.is_owner("b", 3).await.unwrap());
    }

    /// M5 切片 0（verifier 阻断 1 修复）：带 set 的建桶记账**覆盖**语义——删桶记账容忍失败
    /// 留下的陈旧行随重建收敛 owner/set（旧 INSERT OR IGNORE 整条 no-op → 旧 owner/set 分叉）。
    #[tokio::test]
    async fn record_bucket_owner_set_overrides_stale_row() {
        let a = acl().await;
        add_user(&a, 2, false).await;
        add_user(&a, 3, false).await;
        a.record_bucket_owner_set("b", 2, 0).await.unwrap();
        a.record_bucket_owner_set("b", 3, 1).await.unwrap(); // 重建：覆盖
        assert!(!a.is_owner("b", 2).await.unwrap(), "旧 owner 不得继承");
        assert!(a.is_owner("b", 3).await.unwrap());
        let row = sqlx::query("SELECT set_id FROM buckets WHERE name = 'b'")
            .fetch_one(&a.pool)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("set_id"), 1, "set 应收敛到重建值");
    }

    // ---------- Task 9：控制面读扇出 ----------

    pub(super) fn mesh_with(peer_row: Option<ControlRow>) -> ControlMesh {
        let mesh = test_support::FakeMesh::new();
        mesh.answer("peer1", peer_row);
        ControlMesh {
            transport: mesh,
            self_id: "n1".into(),
            peers: vec!["peer1".into()],
        }
    }

    /// 权限位在对端：本地无行 → 扇出 granted。
    #[tokio::test]
    async fn permission_fanout_grants_from_peer() {
        let base = acl().await;
        add_user(&base, 7, false).await;
        let a = Acl::with_mesh(
            base.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: Some(7),
                is_admin: Some(false),
                active: Some(true),
                secret: None,
                flag: Some(true),
                names: None,
                set: None,
                session_policy: None,
            }))),
        );
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: String::new(),
        };
        assert!(a.can(u, Action::Read, "bkt").await.unwrap());
    }

    /// 对端不可达/查无 → fail-closed（拒），与单机同口径。
    #[tokio::test]
    async fn fanout_miss_or_peer_down_denies() {
        let base = acl().await;
        add_user(&base, 7, false).await;
        let a = Acl::with_mesh(base.pool.clone(), Some(mesh_with(None)));
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: String::new(),
        };
        assert!(!a.can(u, Action::Read, "bkt").await.unwrap());
        assert_eq!(a.user_of_key("AK_PEER").await.unwrap(), None);
        assert!(!a.is_owner("bkt", 7).await.unwrap());
    }

    /// 归属与可见集的扇出：owner 比对 + 并集去重。
    #[tokio::test]
    async fn owner_and_visible_fanout() {
        let base = acl().await;
        add_user(&base, 7, false).await;
        // 同一 FakeMesh 同一应答：owner=7。先测 owner。
        let a = Acl::with_mesh(
            base.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: Some(7),
                is_admin: None,
                active: None,
                secret: None,
                flag: None,
                names: None,
                set: None,
                session_policy: None,
            }))),
        );
        assert!(a.is_owner("bkt", 7).await.unwrap());
        assert!(!a.is_owner("bkt", 8).await.unwrap());
        // 可见集：远端应答复用 names 字段（换 mesh）
        let base2 = acl().await;
        add_user(&base2, 7, false).await;
        let a2 = Acl::with_mesh(
            base2.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: None,
                is_admin: None,
                active: None,
                secret: None,
                flag: None,
                names: Some(vec!["beta".into(), "gamma".into()]),
                set: None,
                session_policy: None,
            }))),
        );
        let v = a2.visible(7).await.unwrap();
        assert!(v.contains(&"beta".to_string()) && v.contains(&"gamma".to_string()));
    }
}

#[cfg(test)]
mod session_tests {
    use super::tests::{acl, add_bucket, add_key, add_user, mesh_with};
    use super::*;

    /// 会话键行（测试装置）：kind='session' + parent + 可选 policy。
    async fn add_session(a: &Acl, ak: &str, uid: i64, parent: &str, policy: Option<&str>) {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, parent_ak, session_policy, kind) \
             VALUES (?, 'SK_SESS', ?, 'active', 4000000000, 0, ?, ?, 'session')",
        )
        .bind(ak)
        .bind(uid)
        .bind(parent)
        .bind(policy)
        .execute(&a.pool)
        .await
        .unwrap();
    }

    const GET_ONLY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}"#;

    /// session_policy_of：本地命中 / 非会话键 None / 坏 JSON Err / 扇出取原文。
    #[tokio::test]
    async fn session_policy_of_sources() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        add_session(&a, "ASIA_S1", 7, "AKIA_LONG", Some(GET_ONLY)).await;
        add_session(&a, "ASIA_S2", 7, "AKIA_LONG", None).await;
        assert!(a.session_policy_of("ASIA_S1").await.unwrap().is_some());
        assert!(a.session_policy_of("ASIA_S2").await.unwrap().is_none());
        assert!(a.session_policy_of("AKIA_LONG").await.unwrap().is_none());
        // 坏 JSON → Err（调用方 fail-closed）
        add_session(&a, "ASIA_BAD", 7, "AKIA_LONG", Some("{not-json")).await;
        assert!(a.session_policy_of("ASIA_BAD").await.is_err());
        // 扇出：本地无行 → 远端应答带 session_policy
        let b = Acl::with_mesh(
            acl().await.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: Some(7),
                is_admin: Some(false),
                active: Some(true),
                secret: Some("SK_PEER".into()),
                session_policy: Some(GET_ONLY.to_string()),
                ..Default::default()
            }))),
        );
        assert!(b.session_policy_of("ASIA_REMOTE").await.unwrap().is_some());
        // 远端无策略（None）= 无约束
        let c = Acl::with_mesh(
            acl().await.pool.clone(),
            Some(mesh_with(Some(ControlRow {
                uid: Some(7),
                secret: Some("SK".into()),
                session_policy: None,
                ..Default::default()
            }))),
        );
        assert!(c.session_policy_of("ASIA_REMOTE2").await.unwrap().is_none());
    }

    /// fuse_session 调用包装（测试便利）：构造 dst PolicyRequest；`src` = CopyObject 源（桶, 键）。
    #[allow(clippy::too_many_arguments)] // 测试装置：平铺语义参（同 rebalance 先例）
    async fn fuse(
        a: &Acl,
        ak: &str,
        op: &str,
        bucket: &str,
        key: Option<&str>,
        src: Option<(&str, &str)>,
        d: bool,
        s: bool,
    ) -> ((bool, bool), Option<&'static str>) {
        a.fuse_session(
            crate::policy::PolicyRequest {
                op,
                ak,
                uid: 7,
                account_id: None,
                bucket,
                key,
                anonymous: false,
            },
            src.map(|(sb, sk)| crate::policy::PolicyRequest {
                op,
                ak,
                uid: 7,
                account_id: None,
                bucket: sb,
                key: Some(sk),
                anonymous: false,
            }),
            d,
            s,
        )
        .await
        .unwrap()
    }

    /// fuse_session 交集矩阵：Allow 保留 / Deny 拒 / 无匹配拒 / CopyObject 双轴 /
    /// 未映射 op fail-closed / 非会话无痕。
    #[tokio::test]
    async fn fuse_session_intersection_matrix() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        add_session(&a, "ASIA_G", 7, "AKIA_LONG", Some(GET_ONLY)).await;
        // 会话 Allow：基线保留
        let ((d, s), det) = fuse(
            &a,
            "ASIA_G",
            "GetObject",
            "bkt",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (true, true, Some("session:allow")));
        // 无匹配（PutObject 不在策略里）→ 交集拒
        let ((d, _), det) = fuse(
            &a,
            "ASIA_G",
            "PutObject",
            "bkt",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!(!d && det == Some("session:deny"));
        // 资源不匹配（别的桶）→ 拒
        let ((d, _), det) = fuse(
            &a,
            "ASIA_G",
            "GetObject",
            "other",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!(!d && det == Some("session:deny"));
        // CopyObject：dst 轴（PutObject 映射）无匹配 → 拒；src 轴（GetObject）在策略内 → 保留
        let ((d, s), det) = fuse(
            &a,
            "ASIA_G",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("bkt", "k")),
            true,
            true,
        )
        .await;
        assert!(!d && s && det == Some("session:deny"));
        // 反向：策略只允许 PutObject 时，CopyObject 的 src 轴（GetObject）红、dst 轴绿
        add_session(
            &a,
            "ASIA_P",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bkt/*"}]}"#),
        )
        .await;
        let ((d, s), det) = fuse(
            &a,
            "ASIA_P",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("bkt", "k")),
            true,
            true,
        )
        .await;
        assert!(d && !s && det == Some("session:deny"));
        // 未映射 op（如 PutObjectLockConfiguration）：无显式 Allow 可依 → 拒（fail-closed）
        let ((d, s), det) = fuse(
            &a,
            "ASIA_G",
            "PutObjectLockConfiguration",
            "bkt",
            None,
            None,
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (false, true, Some("session:deny")));
        // 非会话键：原样返回、无 detail
        let ((d, s), det) = fuse(
            &a,
            "AKIA_LONG",
            "PutObject",
            "bkt",
            Some("k"),
            None,
            true,
            false,
        )
        .await;
        assert!((d, s, det) == (true, false, None));
        // 显式 Deny 语句 → 拒（Deny 优先于同策略 Allow）
        add_session(
            &a,
            "ASIA_D",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"},{"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*"}]}"#),
        )
        .await;
        let ((d, _), det) = fuse(
            &a,
            "ASIA_D",
            "GetObject",
            "bkt",
            Some("secret.txt"),
            None,
            true,
            true,
        )
        .await;
        assert!(!d && det == Some("session:deny"));
    }

    /// 会话交集覆盖无桶/桶主 op：GET_ONLY 会话不得建桶、列桶，也不得以桶主身份改桶加密；
    /// 显式 Allow `s3:ListAllMyBuckets` / `s3:CreateBucket` 时才放行（交集而非一律拒）。
    #[tokio::test]
    async fn session_policy_covers_bucketless_ops_and_owner_config() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        add_session(&a, "ASIA_G", 7, "AKIA_LONG", Some(GET_ONLY)).await;
        add_bucket(&a, "bkt", Some(7)).await; // 桶主：表三档本应全放
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: "ASIA_G".into(),
        };
        for (op, bucket) in [
            ("CreateBucket", Some("newb")),
            ("CreateBucket", None),
            ("ListBuckets", None),
            ("PutBucketEncryption", Some("bkt")),
        ] {
            let (allow, detail) = a
                .check_op(&u, "ASIA_G", op, bucket, None, None)
                .await
                .unwrap();
            assert!(!allow, "GET_ONLY 会话不得放行 {op} ({bucket:?})");
            assert_eq!(detail.as_deref(), Some("session:deny"), "{op}");
        }
        // 显式 Allow（Resource "*" 覆盖无桶通配资源）才放行
        add_session(
            &a,
            "ASIA_ALLOW",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:ListAllMyBuckets","s3:CreateBucket"],"Resource":"*"}]}"#),
        )
        .await;
        let v = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: "ASIA_ALLOW".into(),
        };
        for (op, bucket) in [
            ("CreateBucket", Some("newb")),
            ("CreateBucket", None),
            ("ListBuckets", None),
        ] {
            let (allow, detail) = a
                .check_op(&v, "ASIA_ALLOW", op, bucket, None, None)
                .await
                .unwrap();
            assert!(allow, "显式 Allow 应放行 {op} ({bucket:?})");
            assert_eq!(detail.as_deref(), Some("session:allow"), "{op}");
        }
    }

    /// 批量删逐键判定叠会话：会话只放 GetObject → 删键被会话拒（防批量删绕过）。
    #[tokio::test]
    async fn key_delete_decision_applies_session() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        // 表三档放行（can_delete=1）
        sqlx::query("INSERT INTO permissions (user_id, bucket, can_delete) VALUES (7, 'bkt', 1)")
            .execute(&a.pool)
            .await
            .unwrap();
        add_session(&a, "ASIA_G", 7, "AKIA_LONG", Some(GET_ONLY)).await;
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: "ASIA_G".into(),
        };
        assert_eq!(
            a.key_delete_decision(&u, "bkt", "k").await.unwrap(),
            (false, Some("session:deny"))
        );
        // 会话未覆盖 DeleteObject → 恒拒；换成放行 DeleteObject 的会话 → 表三档生效
        add_session(
            &a,
            "ASIA_DEL",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::bkt/*"}]}"#),
        )
        .await;
        let u2 = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: "ASIA_DEL".into(),
        };
        assert_eq!(
            a.key_delete_decision(&u2, "bkt", "k").await.unwrap(),
            (true, None)
        );
    }

    /// P2-1 复验修复判别：CopyObject 的 src 轴用**源桶/源键**求值——曾复用 dst pr，
    /// 会话对源读的收窄可被 copy 绕过（src 轴在目标资源上误命中 Allow）。
    #[tokio::test]
    async fn fuse_session_tagging_folds_into_object_actions() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        add_session(&a, "ASIA_G", 7, "AKIA_LONG", Some(GET_ONLY)).await;
        // GetObjectTagging 折叠到 s3:GetObject → 只读会话放行
        let ((d, s), det) = fuse(
            &a,
            "ASIA_G",
            "GetObjectTagging",
            "bkt",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (true, true, Some("session:allow")));
        // Put/DeleteObjectTagging 折叠到 s3:PutObject → 只读会话拒
        for op in ["PutObjectTagging", "DeleteObjectTagging"] {
            let ((d, _), det) = fuse(&a, "ASIA_G", op, "bkt", Some("k"), None, true, true).await;
            assert!(!d && det == Some("session:deny"), "{op}");
        }
        // 只写会话：PutObjectTagging 放行、GetObjectTagging 拒
        add_session(
            &a,
            "ASIA_P",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bkt/*"}]}"#),
        )
        .await;
        let ((d, _), det) = fuse(
            &a,
            "ASIA_P",
            "PutObjectTagging",
            "bkt",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!(d && det == Some("session:allow"));
        let ((d, _), det) = fuse(
            &a,
            "ASIA_P",
            "GetObjectTagging",
            "bkt",
            Some("k"),
            None,
            true,
            true,
        )
        .await;
        assert!(!d && det == Some("session:deny"));
    }

    #[tokio::test]
    async fn fuse_session_copy_src_axis_uses_source_resource() {
        let a = acl().await;
        add_user(&a, 7, false).await;
        add_key(&a, "AKIA_LONG", 7, None).await;
        // 会话收窄：bkt/* 可写、srcbkt/* 可读（「只许从 srcbkt 拷入 bkt」）
        add_session(
            &a,
            "ASIA_CP",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:PutObject"],"Resource":"arn:aws:s3:::bkt/*"},{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject"],"Resource":"arn:aws:s3:::srcbkt/*"}]}"#),
        )
        .await;
        // 合法拷贝：dst=bkt src=srcbkt → 双轴过
        let ((d, s), det) = fuse(
            &a,
            "ASIA_CP",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("srcbkt", "o")),
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (true, true, Some("session:allow")));
        // 未覆盖源：src=other → src 轴在源资源上无匹配 → 拒（本用例对旧码亦红/绿不可判别，
        // 判别核心在下一段「安全向」）
        let ((d, s), det) = fuse(
            &a,
            "ASIA_CP",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("other", "o")),
            true,
            true,
        )
        .await;
        assert!(
            (d, s, det) == (true, false, Some("session:deny")),
            "src 轴必须用源桶求值（P2-1）"
        );
        // 安全向判别（verifier 复现口径）：会话在 **dst 资源**上放行 Get+Put 时，
        // 「从任意源桶拷入」必须仍被 src 轴拒——旧实现（src 轴复用 dst pr）会在此误放行。
        add_session(
            &a,
            "ASIA_OPEN",
            7,
            "AKIA_LONG",
            Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bkt/*"}]}"#),
        )
        .await;
        let ((d, s), det) = fuse(
            &a,
            "ASIA_OPEN",
            "CopyObject",
            "bkt",
            Some("k"),
            Some(("secret", "o")),
            true,
            true,
        )
        .await;
        assert!(
            (d, s, det) == (true, false, Some("session:deny")),
            "src≠dst 桶时 src 轴不得在目标资源上误命中（P2-1 安全向）"
        );
        // 同桶拷贝（dst 即源）→ 双轴允许（策略覆盖 bkt/*）
        let ((d, s), det) = fuse(
            &a,
            "ASIA_OPEN",
            "CopyObject",
            "bkt",
            Some("k2"),
            Some(("bkt", "k1")),
            true,
            true,
        )
        .await;
        assert!((d, s, det) == (true, true, Some("session:allow")));
        // UploadPartCopy 与 CopyObject 同属双轴：源桶不在会话授权范围 → src 轴拒
        let ((d, s), det) = fuse(
            &a,
            "ASIA_CP",
            "UploadPartCopy",
            "bkt",
            Some("k"),
            Some(("other", "o")),
            true,
            true,
        )
        .await;
        assert!(
            (d, s, det) == (true, false, Some("session:deny")),
            "UploadPartCopy 源轴必须走 GetObject 交集"
        );
    }

    /// P3-2 复验修复判别：本地无行 + 扇出无答（全不可达）→ Err（fail-closed），
    /// 不得把「没人答得上来」静默当「无策略」放行。
    #[tokio::test]
    async fn session_policy_of_mesh_no_answer_is_error() {
        let d = Acl::with_mesh(acl().await.pool.clone(), Some(mesh_with(None)));
        assert!(d.session_policy_of("ASIA_REMOTE_X").await.is_err());
    }
}
