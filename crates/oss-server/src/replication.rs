//! M6 切片 4：桶复制——SQL 规则存储、post-commit 入队（`repl_outbox`）、出站 worker。
//!
//! 语义（计划 R1/R2 + V3）：
//! - 仅版本化提交入队（`version_id` 非 "null"；REPLICA 摄入不再复制，防环）；
//! - 永久错超 10 次 → `state='dead'` 且版本状态 FAILED；连接类错误永不 dead（退避封顶 300s）；
//! - 成功 → 版本状态 COMPLETED 并删行；marker 成功同样 COMPLETED。

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::SqlitePool;
use xl_s3::replication::{ReplRule, ReplicationSink, ReplicationStore, WriteInfo};
use xl_store::store::Store;

use crate::events::{backoff_secs, now};
use crate::repl_client::{ErrClass, ReplClient, ReplError};

const CLAIM_BATCH: i64 = 16;
/// 复制作业租约（计划 R2：长于事件 60s——整读+跨机 PUT 更慢）
const REPL_LEASE_SECS: i64 = 300;
const MAX_ATTEMPTS: i64 = 10;

// ---------- 规则存储（Put/Get/DeleteBucketReplication 背后） ----------

pub struct SqlReplStore {
    pool: SqlitePool,
    remote_ids: Vec<String>,
    /// M9 S6c：仅 Raft 模式路由控制写（None/Local = M7 直写）。
    control: Option<Arc<crate::raft::control::ControlPlane>>,
}

impl SqlReplStore {
    pub fn new(pool: SqlitePool, remote_ids: Vec<String>) -> Self {
        Self {
            pool,
            remote_ids,
            control: None,
        }
    }

    pub fn with_control(mut self, control: Arc<crate::raft::control::ControlPlane>) -> Self {
        self.control = Some(control);
        self
    }

    fn raft_control(&self) -> Option<&Arc<crate::raft::control::ControlPlane>> {
        self.control.as_ref().filter(|c| c.is_raft())
    }
}

fn control_result(
    r: Result<crate::raft::ControlResponse, crate::raft::control::ControlError>,
) -> Result<(), String> {
    match r {
        Ok(crate::raft::ControlResponse::Ok) => Ok(()),
        Ok(other) => Err(format!("control write rejected: {other:?}")),
        Err(e) => Err(e.to_string()),
    }
}

#[async_trait]
impl ReplicationStore for SqlReplStore {
    async fn load_rules(&self, bucket: &str) -> Result<Option<String>, String> {
        sqlx::query_scalar("SELECT rules_json FROM replication_rules WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())
    }

    async fn save_rules(&self, bucket: &str, rules_json: &str) -> Result<(), String> {
        if let Some(control) = self.raft_control() {
            return control_result(
                control
                    .write(crate::raft::ControlCommand::BucketSetReplication {
                        bucket: bucket.to_string(),
                        rules_json: rules_json.to_string(),
                        updated_at: now(),
                    })
                    .await,
            );
        }
        sqlx::query(
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET rules_json=excluded.rules_json, \
             updated_at=excluded.updated_at",
        )
        .bind(bucket)
        .bind(rules_json)
        .bind(now())
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn remove_rules(&self, bucket: &str) -> Result<(), String> {
        if let Some(control) = self.raft_control() {
            return control_result(
                control
                    .write(crate::raft::ControlCommand::BucketClearReplication {
                        bucket: bucket.to_string(),
                    })
                    .await,
            );
        }
        sqlx::query("DELETE FROM replication_rules WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn remote_ids(&self) -> Vec<String> {
        self.remote_ids.clone()
    }
}

// ---------- 写入钩子：规则匹配 → repl_outbox + PENDING ----------

pub struct SqlReplSink {
    pool: SqlitePool,
    store: Arc<Store>,
    /// M9 S4：stretch 站点关闭 M7 异步**站点**对象复制（数据面已由 EC 双站点承担，
    /// 再复制一份是浪费且单站点本就写不成功）；M6 规则复制不受影响。
    site_data_replication: bool,
    /// 4.19：显式配置 `site_token` 的远端——版本定向删除只投递给这些目标（同源 id）。
    versioned_remotes: Vec<String>,
}

impl SqlReplSink {
    pub fn new(pool: SqlitePool, store: Arc<Store>) -> Self {
        Self {
            pool,
            store,
            site_data_replication: true,
            versioned_remotes: Vec::new(),
        }
    }

    /// M9 S4：`[stretch]` 传 false——`enqueue_sites` 整体短路（不写 `target_kind='site'` 行）。
    pub fn with_site_data_replication(mut self, on: bool) -> Self {
        self.site_data_replication = on;
        self
    }

    /// 4.19：`[[remotes]]` 中配置了 `site_token` 的远端 id。
    pub fn with_versioned_remotes(mut self, ids: Vec<String>) -> Self {
        self.versioned_remotes = ids;
        self
    }

    /// 源桶当前是否 versioning=Enabled（版本删除/同源 id 的前提）。
    async fn versioning_enabled(&self, bucket: &str) -> bool {
        match sqlx::query_scalar::<_, String>("SELECT versioning FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(Some(v)) => v == "Enabled",
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(error = %e, bucket, "读桶 versioning 失败——版本删除不入队");
                false
            }
        }
    }
}

/// 规则是否命中该写事件（enabled + 前缀 + marker 开关）。
fn rule_matches(r: &ReplRule, info: &WriteInfo<'_>) -> bool {
    if !r.enabled {
        return false;
    }
    if info.is_delete_marker && !r.delete_marker {
        return false;
    }
    match &r.prefix {
        Some(p) => info.key.starts_with(p),
        None => true,
    }
}

#[async_trait]
impl ReplicationSink for SqlReplSink {
    async fn on_write(&self, info: WriteInfo<'_>) {
        // REPLICA 不再复制（防环）。数据写仅版本化提交（"null" = 未开/暂停版本）；
        // 删除操作（marker）允许无版本号（V3②：写路径全覆盖；顶点状态仅在有版本时维护）。
        if info.is_replica {
            return;
        }
        // M9：sync 桶无异步 outbox（remote-first 已在写路径确认；失败即 503，不降级）。
        // 模式未知时同样不入队（fail-closed：宁可少一条异步复制，不可给 sync 桶造假队列）。
        match self.store.sync_mode(info.bucket).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(error = %e, bucket = info.bucket, "读复制模式失败——跳过异步入队");
                return;
            }
        }
        // 无版本号的删除（硬删）：只走站点路径（M6 规则侧需要版本读，无法处理）。
        let is_hard_delete = !info.is_delete_marker && info.version_id.is_none();
        if !info.is_delete_marker && !is_hard_delete {
            let Some(vid) = info.version_id else { return };
            if vid == "null" {
                return;
            }
        }
        let mut matched = false;
        // M6 桶复制规则（与 M7 站点对相互独立：没有规则也仍可能命中站点）。
        // 4.16：版本定向删除只投递「同源 id」远端（配置 `site_token`）；旧版 M6 远端版本 id
        // 由目标生成，无法定点映射 → 不入队（原语义不变）。
        // 4.19：与站点路径同口径，源桶 versioning 非 Enabled 时版本删除不入队。
        let m6_delivery = !is_hard_delete
            && (!info.is_version_delete
                || (!self.versioned_remotes.is_empty()
                    && self.versioning_enabled(info.bucket).await));
        if m6_delivery {
            match sqlx::query_scalar::<_, String>(
                "SELECT rules_json FROM replication_rules WHERE bucket = ?",
            )
            .bind(info.bucket)
            .fetch_optional(&self.pool)
            .await
            {
                Ok(Some(json)) => match xl_s3::replication::rules_from_json(&json) {
                    Ok(rules) => {
                        for r in rules.iter().filter(|r| rule_matches(r, &info)) {
                            if info.is_version_delete
                                && !self.versioned_remotes.iter().any(|id| id == &r.remote_id)
                            {
                                continue;
                            }
                            matched = true;
                            if let Err(e) = sqlx::query(
                                "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, \
                                 dest_bucket, is_delete_marker, is_version_delete, target_kind, state, attempts, next_retry_at, created_at) \
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'remote', 'pending', 0, 0, ?)",
                            )
                            .bind(info.bucket)
                            .bind(info.key)
                            .bind(info.version_id.unwrap_or(""))
                            .bind(info.etag)
                            .bind(info.size as i64)
                            .bind(&r.remote_id)
                            .bind(&r.dest_bucket)
                            .bind(i64::from(info.is_delete_marker))
                            .bind(i64::from(info.is_version_delete))
                            .bind(now())
                            .execute(&self.pool)
                            .await
                            {
                                tracing::warn!(error = %e, bucket = info.bucket, key = info.key, "复制入队失败");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, bucket = info.bucket, "复制规则记录损坏——跳过");
                    }
                },
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(error = %e, bucket = info.bucket, "读复制规则失败——本写不复制（不影响请求）");
                }
            }
        }
        // M7：站点对入队（独立于桶复制规则；非版本桶删除不跨界）
        let site_matched = self.enqueue_sites(&info).await;
        // 单调 PENDING：并发 worker 可能已 COMPLETED（P2-2，不降级终态）；无版本号（硬删）跳过
        if (matched || site_matched)
            && !info.is_version_delete
            && let Some(vid) = info.version_id
            && let Err(e) = self
                .store
                .mark_replication_pending(info.bucket, info.key, vid)
                .await
        {
            tracing::warn!(error = %e, bucket = info.bucket, key = info.key, "置 PENDING 失败");
        }
    }
}

impl SqlReplSink {
    /// M7 切片 2a：逐 peer 入队。删除写要求源桶 versioning=Enabled
    /// （Off/Suspended 不入队——记档；marker 带版本，硬删版本列为空）。
    async fn enqueue_sites(&self, info: &WriteInfo<'_>) -> bool {
        if !self.site_data_replication {
            return false;
        }
        // 版本定向删除必须是真实 UUID 版本（站点 PUT 强制同源 id）；
        // "null"/空版本无法在目标定点命中，不跨界（fail-closed）。
        if info.is_version_delete && info.version_id.is_none_or(|v| v.is_empty() || v == "null") {
            return false;
        }
        if info.is_delete_marker || info.version_id.is_none() || info.is_version_delete {
            let v: Option<String> = match sqlx::query_scalar(
                "SELECT versioning FROM buckets WHERE name = ?",
            )
            .bind(info.bucket)
            .fetch_optional(&self.pool)
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, bucket = info.bucket, "读桶 versioning 失败——站点删除不入队");
                    return false;
                }
            };
            if v.as_deref() != Some("Enabled") {
                return false;
            }
        }
        let peers: Vec<String> =
            match sqlx::query_scalar("SELECT peer_id FROM sites ORDER BY peer_id")
                .fetch_all(&self.pool)
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "读站点对失败——本写不复制到站点");
                    return false;
                }
            };
        if peers.is_empty() {
            return false;
        }
        let mut matched = false;
        for peer in peers {
            if let Err(e) = sqlx::query(
                "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, \
                 dest_bucket, is_delete_marker, is_version_delete, target_kind, state, attempts, next_retry_at, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'site', 'pending', 0, 0, ?)",
            )
            .bind(info.bucket)
            .bind(info.key)
            .bind(info.version_id.unwrap_or(""))
            .bind(info.etag)
            .bind(info.size as i64)
            .bind(&peer)
            .bind(info.bucket)
            .bind(i64::from(info.is_delete_marker))
            .bind(i64::from(info.is_version_delete))
            .bind(now())
            .execute(&self.pool)
            .await
            {
                tracing::warn!(error = %e, bucket = info.bucket, key = info.key, peer = %peer, "站点复制入队失败");
            } else {
                matched = true;
            }
        }
        matched
    }
}

// ---------- worker（R2 租约 + R1 分级） ----------

struct ReplRow {
    seq: i64,
    bucket: String,
    key: String,
    version_id: String,
    etag: String,
    remote_id: String,
    dest_bucket: String,
    is_delete_marker: bool,
    /// 4.16：目标按同源 version_id 定点删除（不造 marker）。
    is_version_delete: bool,
    target_kind: String,
    attempts: i64,
    claimed_at: Option<i64>,
}

/// M7 R6：目标桶 versioning 探询缓存（按 peer+bucket，10s）。
static VERSIONING_CACHE: LazyLock<Mutex<HashMap<String, (Instant, String)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
const VERSIONING_TTL: Duration = Duration::from_secs(10);

/// `cache_key` 含 endpoint（`endpoint\u{1f}bucket`）——解配/换端点后不会命中旧目标缓存。
async fn versioning_ready(
    client: &ReplClient,
    remote_id: &str,
    cache_key: &str,
    bucket: &str,
) -> Result<(), ReplError> {
    {
        let cache = VERSIONING_CACHE.lock().unwrap();
        if let Some((at, st)) = cache.get(cache_key)
            && at.elapsed() < VERSIONING_TTL
        {
            return if st == "Enabled" {
                Ok(())
            } else {
                Err(ReplError::retryable(format!(
                    "目标桶 versioning={st}（等待就绪）"
                )))
            };
        }
    }
    let st = client.get_bucket_versioning(remote_id, bucket).await?;
    {
        let mut cache = VERSIONING_CACHE.lock().unwrap();
        cache.insert(cache_key.to_string(), (Instant::now(), st.clone()));
    }
    if st == "Enabled" {
        Ok(())
    } else {
        Err(ReplError::retryable(format!(
            "目标桶 versioning={st}（等待就绪）"
        )))
    }
}

fn invalidate_versioning_cache(cache_key: &str) {
    VERSIONING_CACHE.lock().unwrap().remove(cache_key);
}

/// site PUT/DELETE 的 404：桶可能在预检后被删/重建——驱逐缓存并按可重试处理（P2-4）。
fn site_404_retry(e: ReplError, cache_key: &str, op: &str) -> ReplError {
    if e.status == Some(404) {
        invalidate_versioning_cache(cache_key);
        ReplError::retryable(format!("目标桶不存在（{op}）：{}", e.message))
    } else {
        e
    }
}

async fn site_peer(
    pool: &SqlitePool,
    peer_id: &str,
) -> Result<Option<(String, String, String, String, String)>, sqlx::Error> {
    use sqlx::Row as _;
    let row = sqlx::query(
        "SELECT endpoint, s3_access_key, s3_secret_key, region, peer_site_token \
         FROM sites WHERE peer_id = ?",
    )
    .bind(peer_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| {
        (
            r.get("endpoint"),
            r.get("s3_access_key"),
            r.get("s3_secret_key"),
            r.get("region"),
            r.get("peer_site_token"),
        )
    }))
}

/// 4.16：同 (bucket,key,version) 更早的出站行仍在途 → 版本删除必须先等待。
async fn earlier_version_rows(pool: &SqlitePool, r: &ReplRow) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE bucket=? AND key=? AND version_id=? \
         AND seq < ? AND state IN ('pending','sending')",
    )
    .bind(&r.bucket)
    .bind(&r.key)
    .bind(&r.version_id)
    .bind(r.seq)
    .fetch_one(pool)
    .await
}

async fn renew_claim(pool: &SqlitePool, seq: i64, old: Option<i64>) -> Option<i64> {
    let now_s = now();
    match sqlx::query(
        "UPDATE repl_outbox SET claimed_at=? WHERE seq=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(now_s)
    .bind(seq)
    .bind(old)
    .execute(pool)
    .await
    {
        Ok(r) if r.rows_affected() == 1 => Some(now_s),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(error = %e, seq, "复制续租失败（跳过本轮）");
            None
        }
    }
}

async fn finish_ok(pool: &SqlitePool, seq: i64, token: Option<i64>) -> bool {
    match sqlx::query("DELETE FROM repl_outbox WHERE seq=? AND claimed_at IS ? AND state='sending'")
        .bind(seq)
        .bind(token)
        .execute(pool)
        .await
    {
        Ok(r) => r.rows_affected() == 1,
        Err(e) => {
            tracing::warn!(error = %e, seq, "复制完成删行失败");
            false
        }
    }
}

async fn requeue_or_bury(
    pool: &SqlitePool,
    seq: i64,
    token: Option<i64>,
    attempts: i64,
    failure: &ReplError,
    now_s: i64,
) -> bool {
    let bury = failure.class == ErrClass::Permanent && attempts > MAX_ATTEMPTS;
    let res = if bury {
        sqlx::query(
            "UPDATE repl_outbox SET state='dead', attempts=?, claimed_at=NULL, last_error=? \
             WHERE seq=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(&failure.message)
        .bind(seq)
        .bind(token)
        .execute(pool)
        .await
    } else {
        sqlx::query(
            "UPDATE repl_outbox SET state='pending', attempts=?, claimed_at=NULL, \
             next_retry_at=?, last_error=? WHERE seq=? AND claimed_at IS ? AND state='sending'",
        )
        .bind(attempts)
        .bind(now_s + backoff_secs(attempts))
        .bind(&failure.message)
        .bind(seq)
        .bind(token)
        .execute(pool)
        .await
    };
    match res {
        Ok(r) => r.rows_affected() == 1,
        Err(e) => {
            tracing::warn!(error = %e, seq, "复制失败状态回写失败");
            false
        }
    }
}

/// 一行复制：marker → 目标 DELETE；数据版本 → 读源 + PUT（带 REPLICA）。
/// `cache_key`：版本化远端（站点对 / 配置 `site_token` 的 M6 远端）的 versioning
/// 就绪探询缓存；`site_token`：同源 id 与版本删除的认证令牌（None = 旧 M6 语义）。
async fn send_row(
    store: &Store,
    client: &ReplClient,
    remote_id: &str,
    r: &ReplRow,
    cache_key: Option<&str>,
    site_token: Option<&str>,
) -> Result<(), ReplError> {
    if let Some(key) = cache_key {
        versioning_ready(client, remote_id, key, &r.dest_bucket).await?;
    }
    // 4.16：版本定向删除——标准 S3 `DELETE ?versionId=`（带 REPLICA），目标删同 id
    // 版本/marker，绝不创建 marker。无同源 id 的远端不入队（入队侧已跳过）。
    if r.is_version_delete {
        let Some(token) = site_token else {
            return Err(ReplError::permanent(
                "版本定向删除仅支持同源 id 远端（未配置 site_token）",
            ));
        };
        // M-1：只带源 id 定点删除；绝不按内容指纹清理未映射历史副本（无源身份 → 会误删）。
        let res = client
            .delete_object_version_s3(
                remote_id,
                &r.dest_bucket,
                &r.key,
                &r.version_id,
                Some(token),
            )
            .await;
        return match (res, cache_key) {
            (Err(e), Some(key)) => Err(site_404_retry(e, key, "DELETE version")),
            (r, _) => r,
        };
    }
    // marker 或硬删（版本列为空）都以 DELETE 表达（P2-5）。
    // 4.16：同源 id 远端的 marker 用源 marker id 创建（目标随机 id 会让后续 marker
    // 版本删除失效）；旧版 M6 远端仍用普通 DELETE（无 id 映射，记档）。
    if r.is_delete_marker || r.version_id.is_empty() {
        let res = match site_token {
            Some(token) if r.is_delete_marker && !r.version_id.is_empty() => {
                client
                    .delete_marker_s3(remote_id, &r.dest_bucket, &r.key, &r.version_id, token)
                    .await
            }
            _ => {
                client
                    .delete_object(remote_id, &r.dest_bucket, &r.key)
                    .await
            }
        };
        return match (res, cache_key) {
            (Err(e), Some(key)) => Err(site_404_retry(e, key, "DELETE")),
            (r, _) => r,
        };
    }
    // 必须读行内记录的**那一个版本**（对象可能已被覆写；读 latest 会复制错内容）
    let info = store
        .head_object_version(&r.bucket, &r.key, &r.version_id)
        .await
        .map_err(|e| match e {
            xl_store::error::Error::NoSuchKey(_) => {
                ReplError::permanent(format!("源版本已不存在：{}@{}", r.key, r.version_id))
            }
            other => ReplError::retryable(format!("读源元数据失败：{other}")),
        })?;
    // 4.19：SSE-C（客户钥不落盘）与 SSE-KMS（CMK 为源站点本地主密钥）无法保语义 →
    // 所有远端永久死信，且在发送目标请求前拒绝，绝不静默降级为明文或换钥。
    if let Some(enc) = &info.encryption {
        if enc.sse_c_key_md5.is_some() || enc.key_id == "sse-c" {
            return Err(ReplError::permanent(
                "SSE-C 源对象不支持复制（客户钥不落盘）",
            ));
        }
        if enc.key_id != "file" {
            return Err(ReplError::permanent(format!(
                "SSE-KMS 源对象不支持复制（key_id={} 为站点本地主密钥，目标不接受显式 CMK）",
                enc.key_id
            )));
        }
    }
    let (_info, bytes) = store
        .get_object_version(&r.bucket, &r.key, &r.version_id)
        .await
        .map_err(|e| ReplError::retryable(format!("读源对象失败：{e}")))?;
    // 4.17/4.19：标签/存储类/校验和/SSE/对象锁随版本复制（旧 M6 远端同样携带）；
    // 过期 retention 不再下发（目标按同值 WORM 记录判定，过期即允许删），legal hold 始终保留。
    let now_ns = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;
    let lock = info
        .lock
        .as_ref()
        .filter(|l| l.legal_hold || l.retain_until_nanos.is_some_and(|n| n > now_ns));
    // 4.16/4.19：带 token 的远端要求同源版本 id（目标版本删除才能定点）；旧 M6 远端
    // 不带 origin id，由目标自生成（原语义）。
    let res = client
        .put_object_with_meta(
            remote_id,
            &r.dest_bucket,
            &r.key,
            bytes,
            &r.etag,
            info.content_type.as_deref(),
            &info.user_metadata,
            Some(info.tags.as_slice()).filter(|t| !t.is_empty()),
            info.storage_class.as_deref(),
            info.checksum.as_ref(),
            site_token.is_some(),
            site_token,
            site_token.map(|_| r.version_id.as_str()),
            info.part_sizes.as_deref(), // 4.1：段长表随副本，partNumber 可用
            info.encryption.as_ref(),
            lock,
        )
        .await;
    let res = match (res, site_token) {
        (Ok(vid), Some(_)) => match vid.as_deref() {
            Some(v) if v == r.version_id => Ok(()),
            Some(v) => Err(ReplError::permanent(format!(
                "目标未按同源版本 id 落库（期望 {}，实得 {v}；对端版本过旧？）",
                r.version_id
            ))),
            None => Err(ReplError::permanent("目标未返回 version id")),
        },
        (r, None) => r.map(|_| ()),
        (Err(e), Some(_)) => Err(e),
    };
    match (res, cache_key) {
        (Err(e), Some(key)) => Err(site_404_retry(e, key, "PUT")),
        (r, _) => r,
    }
}

/// P2-1：一个版本的复制终态聚合——仍有 pending/sending 行则保持 PENDING；
/// 存在 dead 行则 FAILED；否则 COMPLETED。`dropped=true` 表示解配丢弃了剩余行：
/// 无剩余行时清除状态（`None`，不再有复制义务），而不是谎报 COMPLETED。
async fn settle_version_status(pool: &SqlitePool, store: &Store, r: &ReplRow, dropped: bool) {
    use sqlx::Row as _;
    // 单条聚合：三次独立 COUNT 之间的窗口可能把仍有 pending 行误判为已完成。
    let counts = sqlx::query(
        "SELECT COALESCE(SUM(state='pending'),0) AS pending, \
                COALESCE(SUM(state='sending'),0) AS sending, \
                COALESCE(SUM(state='dead'),0) AS dead \
         FROM repl_outbox WHERE bucket=? AND key=? AND version_id=?",
    )
    .bind(&r.bucket)
    .bind(&r.key)
    .bind(&r.version_id)
    .fetch_one(pool)
    .await;
    let (pending, sending, dead) = match counts {
        Ok(counts) => (
            counts.get::<i64, _>("pending"),
            counts.get::<i64, _>("sending"),
            counts.get::<i64, _>("dead"),
        ),
        Err(e) => {
            tracing::warn!(error = %e, "复制终态聚合计数失败（本轮不写状态）");
            return;
        }
    };
    if pending > 0 || sending > 0 {
        return; // 其他规则仍在途：保持 PENDING
    }
    let status = if dead > 0 {
        Some("FAILED")
    } else if dropped {
        None
    } else {
        Some("COMPLETED")
    };
    if let Err(e) = store
        .set_replication_status(&r.bucket, &r.key, &r.version_id, status)
        .await
    {
        tracing::warn!(error = %e, ?status, "复制终态写入失败（对象可能已被删除）");
    }
}

/// 解配后结算（console `DELETE /sites/{id}` 用）：该版本可能只剩被删掉的 site 行。
pub async fn settle_version_after_drop(
    pool: &SqlitePool,
    store: &Store,
    bucket: &str,
    key: &str,
    version_id: &str,
) {
    let r = ReplRow {
        seq: 0,
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: version_id.to_string(),
        etag: String::new(),
        remote_id: String::new(),
        dest_bucket: String::new(),
        is_delete_marker: false,
        is_version_delete: false,
        target_kind: "site".into(),
        attempts: 0,
        claimed_at: None,
    };
    settle_version_status(pool, store, &r, true).await;
}

/// 一轮：回收过期租约 → claim → 逐行续租/发送/完成或重试。返回 (成功, 失败)。
pub async fn process_repl_once(
    pool: &SqlitePool,
    store: &Store,
    client: &ReplClient,
) -> (usize, usize) {
    let now_s = now();
    if let Err(e) = sqlx::query(
        "UPDATE repl_outbox SET state='pending', claimed_at=NULL, next_retry_at=? \
         WHERE state='sending' AND claimed_at < ?",
    )
    .bind(now_s)
    .bind(now_s - REPL_LEASE_SECS)
    .execute(pool)
    .await
    {
        tracing::warn!(error = %e, "repl_outbox 租约回收失败");
        return (0, 0);
    }
    let rows = match sqlx::query(
        "UPDATE repl_outbox SET state='sending', claimed_at=? \
         WHERE state='pending' AND seq IN (SELECT seq FROM repl_outbox WHERE state='pending' \
                       AND next_retry_at <= ? ORDER BY seq LIMIT ?) \
         RETURNING seq, bucket, key, version_id, etag, remote_id, dest_bucket, \
         is_delete_marker, is_version_delete, target_kind, attempts, claimed_at",
    )
    .bind(now_s)
    .bind(now_s)
    .bind(CLAIM_BATCH)
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "repl_outbox claim 失败");
            return (0, 0);
        }
    };
    let (mut sent, mut failed) = (0usize, 0usize);
    for row in rows {
        use sqlx::Row as _;
        let r = ReplRow {
            seq: row.get("seq"),
            bucket: row.get("bucket"),
            key: row.get("key"),
            version_id: row.get("version_id"),
            etag: row.get("etag"),
            remote_id: row.get("remote_id"),
            dest_bucket: row.get("dest_bucket"),
            is_delete_marker: row.get::<i64, _>("is_delete_marker") != 0,
            is_version_delete: row.get::<i64, _>("is_version_delete") != 0,
            target_kind: row.get("target_kind"),
            attempts: row.get("attempts"),
            claimed_at: row.get("claimed_at"),
        };
        let Some(token) = renew_claim(pool, r.seq, r.claimed_at).await else {
            continue;
        };
        let outcome: Result<(), ReplError> = if r.target_kind == "site" {
            match site_peer(pool, &r.remote_id).await {
                Ok(Some((endpoint, ak, sk, region, token))) => {
                    match ReplClient::single(&endpoint, &ak, &sk, &region)
                        .map(|c| c.with_tls(client.tls()))
                    {
                        Ok(c) => {
                            let cache_key = format!("{endpoint}\u{1f}{}", r.dest_bucket);
                            // 4.16：同版本更早的行（PUT）仍在途时先等待——否则删除先到、
                            // PUT 重投成功会把已删版本在目标复活。
                            let earlier = if r.is_version_delete {
                                earlier_version_rows(pool, &r).await
                            } else {
                                Ok(0)
                            };
                            match earlier {
                                Ok(0) => {
                                    send_row(store, &c, "site", &r, Some(&cache_key), Some(&token))
                                        .await
                                }
                                Ok(_) => Err(ReplError::retryable(
                                    "同版本更早的复制作业尚未完成（保序等待）",
                                )),
                                Err(e) => {
                                    Err(ReplError::retryable(format!("读同版本出站行失败：{e}")))
                                }
                            }
                        }
                        Err(f) => Err(f),
                    }
                }
                Ok(None) => {
                    // 已解除配对：丢行并结算版本状态（否则只有该 site 行的版本永久 PENDING——P1-3）。
                    let _ = sqlx::query(
                        "DELETE FROM repl_outbox WHERE seq=? AND claimed_at IS ? AND state='sending'",
                    )
                    .bind(r.seq)
                    .bind(Some(token))
                    .execute(pool)
                    .await;
                    if !r.version_id.is_empty() && !r.is_version_delete {
                        settle_version_status(pool, store, &r, true).await;
                    }
                    tracing::warn!(seq = r.seq, peer = %r.remote_id, "站点对已解除——丢弃出站行");
                    continue;
                }
                Err(e) => Err(ReplError::retryable(format!("读站点对失败：{e}"))),
            }
        } else {
            // 4.19：配置 `site_token` 的远端与站点同口径——先探询目标 versioning，
            // PUT 携带同源版本 id；版本删除还需等同一版本更早的 PUT 行完成（防复活）。
            let token = client.site_token(&r.remote_id);
            let cache_key =
                token.and_then(|_| client.target_cache_key(&r.remote_id, &r.dest_bucket));
            let earlier = if r.is_version_delete {
                earlier_version_rows(pool, &r).await
            } else {
                Ok(0)
            };
            match earlier {
                Ok(0) => {
                    send_row(store, client, &r.remote_id, &r, cache_key.as_deref(), token).await
                }
                Ok(_) => Err(ReplError::retryable(
                    "同版本更早的复制作业尚未完成（保序等待）",
                )),
                Err(e) => Err(ReplError::retryable(format!("读同版本出站行失败：{e}"))),
            }
        };
        match outcome {
            Ok(()) => {
                let claimed = finish_ok(pool, r.seq, Some(token)).await;
                if claimed {
                    sent += 1;
                } else {
                    tracing::warn!(seq = r.seq, "复制完成时 claim token 不符（跳过状态写）");
                }
                // P2-1：多规则聚合——仍有在途行则保持 PENDING；有死信则 FAILED；否则 COMPLETED。
                // P3-5：token 失配时不得写状态（该行可能已被新 worker 接管）。
                // 版本删除后源版本已不在，不写复制终态（无对象可标）。
                if claimed && !r.version_id.is_empty() && !r.is_version_delete {
                    settle_version_status(pool, store, &r, false).await;
                }
            }
            Err(f) => {
                failed += 1;
                let a = r.attempts + 1;
                let bury = f.class == ErrClass::Permanent && a > MAX_ATTEMPTS;
                let wrote = requeue_or_bury(pool, r.seq, Some(token), a, &f, now_s).await;
                if !wrote {
                    tracing::warn!(
                        seq = r.seq,
                        "复制失败回写时 claim token 不符（已被回收重投？）"
                    );
                }
                if bury && wrote {
                    tracing::error!(seq = r.seq, error = %f.message, "复制作业进入死信（永久错超限）");
                    if !r.version_id.is_empty()
                        && !r.is_version_delete
                        && let Err(e) = store
                            .set_replication_status(
                                &r.bucket,
                                &r.key,
                                &r.version_id,
                                Some("FAILED"),
                            )
                            .await
                    {
                        tracing::warn!(error = %e, "置 FAILED 失败（对象可能已被删除）");
                    }
                }
            }
        }
    }
    (sent, failed)
}

/// serve 挂载（`OSS_REPL_INTERVAL_SECS` 默认 1s；显式 0 = 关闭）。
pub fn spawn_repl_worker(pool: SqlitePool, store: Arc<Store>, client: Arc<ReplClient>) {
    let secs: u64 = std::env::var("OSS_REPL_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    if secs == 0 {
        tracing::info!("复制 worker 已关闭（OSS_REPL_INTERVAL_SECS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            let _ = process_repl_once(&pool, &store, &client).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(prefix: Option<&str>, dm: bool, enabled: bool) -> ReplRule {
        ReplRule {
            id: "r".into(),
            prefix: prefix.map(str::to_string),
            remote_id: "r1".into(),
            dest_bucket: "dst".into(),
            delete_marker: dm,
            enabled,
        }
    }

    fn info<'a>(key: &'a str, marker: bool, replica: bool) -> WriteInfo<'a> {
        WriteInfo {
            bucket: "b",
            key,
            version_id: Some("v1"),
            etag: "e",
            size: 1,
            is_delete_marker: marker,
            is_version_delete: false,
            is_replica: replica,
        }
    }

    #[test]
    fn rule_matching_matrix() {
        assert!(rule_matches(
            &rule(None, false, true),
            &info("a", false, false)
        ));
        assert!(rule_matches(
            &rule(Some("logs/"), false, true),
            &info("logs/x", false, false)
        ));
        assert!(!rule_matches(
            &rule(Some("logs/"), false, true),
            &info("data/x", false, false)
        ));
        // marker：仅 DeleteMarkerReplication=Enabled 的规则命中
        assert!(!rule_matches(
            &rule(None, false, true),
            &info("a", true, false)
        ));
        assert!(rule_matches(
            &rule(None, true, true),
            &info("a", true, false)
        ));
        // Disabled 规则永不命中
        assert!(!rule_matches(
            &rule(None, true, false),
            &info("a", false, false)
        ));
    }
}
