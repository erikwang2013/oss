//! M9 S1：sync 桶写提交门实现（store 调用的远端确认）。
//!
//! 顺序：store staging 完成（不可见）→ 本门对唯一站点 peer 做 token-auth PUT
//! （要求远端 version id == 本地预生成 UUID）→ store 翻转本地 meta。
//! 失败一律 fail-closed（503）；无降级、无后台重试。

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sqlx::Row as _;
use xl_s3::sync_delete::SyncDeleteSink;
use xl_store::error::Error as StoreError;
use xl_store::gate::{GateError, GateRequest, GateTicket, PutCommitGate};
use xl_store::store::{DeleteMarkerSpec, DeleteOutcome, Store};

use crate::db::Db;
use crate::repl_client::{ReplClient, ReplError};
use crate::state::AppState;

/// 模式缓存 TTL（建桶/删桶/apply 会显式失效；TTL 只兜底漏网）。
const CACHE_TTL: Duration = Duration::from_secs(10);
/// 删桶 quiesce 上限（两端各 30s；客户端留 5s 裕量）。
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);
const QUIESCE_CALL_TIMEOUT: Duration = Duration::from_secs(35);
/// 写 intent 状态：远端调用前 / 远端已确认（本地未提交）。
const INTENT_PENDING: &str = "remote_pending";
const INTENT_DONE: &str = "remote_done";
/// 写 intent 恢复周期（启动时先跑一轮）。
const INTENT_RECOVER_SECS: u64 = 30;
/// 恢复每轮最多处理行数 / 连续无进展短路阈值（远端黑洞时避免一轮拖数分钟）。
const RECOVER_BATCH: i64 = 32;
const RECOVER_MAX_CONSECUTIVE_FAILURES: usize = 8;

/// 远端 4xx 中 409/400 是确定性拒绝，保留语义避免客户端对不可重试错误无限重试；
/// 其余（403 本站凭证/配置问题、5xx、连接错误）一律 503 fail-closed。
fn remote_gate_error(e: crate::repl_client::ReplError) -> GateError {
    match e.status {
        Some(409) => GateError::Conflict(format!("remote conflict: {}", e.message)),
        Some(400) => GateError::Rejected(format!("remote rejected: {}", e.message)),
        _ => GateError::Unavailable(format!("remote put failed: {}", e.message)),
    }
}

#[derive(Debug, Clone, Copy)]
struct BucketState {
    sync: bool,
    deleting: bool,
    versioned: bool,
}

struct Cached {
    at: Instant,
    state: BucketState,
}

/// `/metrics` 的 sync 提交门计数（AppState 与门共享一份 `Arc`）。
#[derive(Debug, Default)]
pub struct SyncMetrics {
    acked: AtomicU64,
    /// 门（`before_commit`）返回错误的次数，含前置校验失败。
    failed: AtomicU64,
    write_intent_recovered: AtomicU64,
}

impl SyncMetrics {
    pub fn acked_total(&self) -> u64 {
        self.acked.load(Ordering::Relaxed)
    }

    pub fn failed_total(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    pub fn write_intent_recovered_total(&self) -> u64 {
        self.write_intent_recovered.load(Ordering::Relaxed)
    }
}

/// 站点 sync 门：`buckets.replication_mode='sync'` 的写必须远端先确认。
pub struct SiteSyncGate {
    db: Arc<Db>,
    timeout: Duration,
    max_bytes: u64,
    /// `[site]` 已配置（未配置时 sync 写不可用——残留 sites 行不足以授权远端写）。
    site_enabled: bool,
    cache: Mutex<HashMap<String, Cached>>,
    metrics: Arc<SyncMetrics>,
}

impl std::fmt::Debug for SiteSyncGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SiteSyncGate")
            .field("timeout", &self.timeout)
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}

impl SiteSyncGate {
    pub fn new(db: Arc<Db>, timeout: Duration, max_bytes: u64, site_enabled: bool) -> Self {
        Self {
            db,
            timeout,
            max_bytes,
            site_enabled,
            cache: Mutex::new(HashMap::new()),
            metrics: Arc::new(SyncMetrics::default()),
        }
    }

    /// 与 `AppState` 共享 `/metrics` 计数（默认独立一份，测试/恢复用）。
    pub fn with_metrics(mut self, metrics: Arc<SyncMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    async fn state(&self, bucket: &str) -> Result<BucketState, GateError> {
        if let Ok(c) = self.cache.lock()
            && let Some(e) = c.get(bucket)
            && e.at.elapsed() < CACHE_TTL
        {
            return Ok(e.state);
        }
        let (mode, deleting, versioning) = self
            .db
            .bucket_sync_state(bucket)
            .await
            .map_err(|e| GateError::Unavailable(format!("read bucket mode: {e}")))?;
        let state = BucketState {
            sync: mode == "sync",
            deleting,
            versioned: versioning == "Enabled",
        };
        if let Ok(mut c) = self.cache.lock() {
            c.insert(
                bucket.to_string(),
                Cached {
                    at: Instant::now(),
                    state,
                },
            );
        }
        Ok(state)
    }

    /// 唯一数据 peer（witness 不写 `sites` 行；0/多行 = sync 前置不满足 → 503）。
    async fn peer(&self) -> Result<crate::db::SitePeer, GateError> {
        peer(&self.db).await
    }

    fn client(peer: &crate::db::SitePeer) -> Result<ReplClient, GateError> {
        client(peer)
    }

    /// 远端 PUT 前落盘写 intent（kill -9 后恢复的唯一凭据）。
    /// 连接级显式 `synchronous=FULL`：WAL 提交必须落盘后才允许远端写。
    async fn intent_begin(&self, req: &GateRequest<'_>) -> Result<(), GateError> {
        let now = crate::db::now();
        let tee_path = req.plaintext.to_string_lossy().into_owned();
        let commit_json = serde_json::json!({ "etag": req.etag, "size": req.size }).to_string();
        let mut conn = self
            .db
            .pool()
            .acquire()
            .await
            .map_err(|e| GateError::Unavailable(format!("intent acquire: {e}")))?;
        sqlx::query("PRAGMA synchronous = FULL")
            .execute(&mut *conn)
            .await
            .map_err(|e| GateError::Unavailable(format!("intent pragma: {e}")))?;
        sqlx::query(
            "INSERT INTO sync_write_intents \
             (id, bucket, key, local_version_id, remote_version_id, tee_path, commit_json, \
              state, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(req.local_version_id)
        .bind(req.bucket)
        .bind(req.key)
        .bind(req.local_version_id)
        .bind(req.local_version_id)
        .bind(tee_path)
        .bind(commit_json)
        .bind(INTENT_PENDING)
        .bind(now)
        .bind(now)
        .execute(&mut *conn)
        .await
        .map_err(|e| GateError::Unavailable(format!("write intent: {e}")))?;
        Ok(())
    }

    async fn intent_done(&self, id: &str) {
        if let Err(e) =
            sqlx::query("UPDATE sync_write_intents SET state = ?, updated_at = ? WHERE id = ?")
                .bind(INTENT_DONE)
                .bind(crate::db::now())
                .bind(id)
                .execute(self.db.pool())
                .await
        {
            // 状态只是提示；行仍在，恢复按「本地无 meta → 删远端同 id」处理。
            tracing::warn!(id, error = %e, "sync intent 状态更新失败");
        }
    }

    /// 删除远端同 id 版本（404 视为已删）。恢复与补偿共用。
    async fn delete_remote_version(&self, bucket: &str, key: &str, id: &str) -> Result<(), String> {
        let peer = self.peer().await.map_err(|e| e.to_string())?;
        let client = Self::client(&peer).map_err(|e| e.to_string())?;
        let call =
            client.delete_object_version("site", bucket, key, id, Some(&peer.peer_site_token));
        match tokio::time::timeout(self.timeout, call).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.message),
            Err(_) => Err("remote delete timeout".into()),
        }
    }
}

/// 清理一条写 intent：先删 tee，再删行（中途崩溃时行仍在，下轮恢复）。
/// 返回 `true` 仅当 intent 行已删除；tee 删除失败仍算清理成功（行是恢复的依据）。
async fn clean_intent(db: &Db, id: &str) -> bool {
    let tee: Option<String> =
        sqlx::query_scalar("SELECT tee_path FROM sync_write_intents WHERE id = ?")
            .bind(id)
            .fetch_optional(db.pool())
            .await
            .ok()
            .flatten();
    if let Some(p) = tee {
        let _ = tokio::fs::remove_file(p).await;
    }
    match sqlx::query("DELETE FROM sync_write_intents WHERE id = ?")
        .bind(id)
        .execute(db.pool())
        .await
    {
        Ok(_) => true,
        Err(e) => {
            tracing::warn!(id, error = %e, "sync intent 删除失败（下轮恢复重试）");
            false
        }
    }
}

/// M9 S1 N12 / S2 M4：清理无 intent 引用的陈旧 tee 明文（>1h）。
/// 有 intent 的由恢复统一处理（不能抢在恢复前删掉补偿/补提交的凭据）。
async fn clean_orphan_tees(state: &AppState) {
    let dir = state.store.tee_dir();
    let referenced: std::collections::HashSet<String> =
        match sqlx::query_scalar::<_, String>("SELECT tee_path FROM sync_write_intents")
            .fetch_all(state.db.pool())
            .await
        {
            Ok(v) => v.into_iter().collect(),
            Err(e) => {
                tracing::warn!(error = %e, "tee 清理：读取 intent 失败（本轮跳过）");
                return;
            }
        };
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(_) => return, // 目录未创建 = 无 tee
    };
    let now = std::time::SystemTime::now();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("plain") {
            continue;
        }
        if referenced.contains(&path.to_string_lossy().into_owned()) {
            continue;
        }
        let old = entry
            .metadata()
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|m| now.duration_since(m).ok())
            .is_some_and(|age| age > Duration::from_secs(3600));
        if old {
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
}

/// 启动 + 周期恢复：本地 meta 有该 id → 只清 intent；否则删远端同 id。
/// 本地查询失败/远端不可达 → 保留，30s 后重试。
pub async fn recover_write_intents(state: &AppState) {
    clean_orphan_tees(state).await;
    // 只恢复“已完成/已崩溃”的陈旧行：在途写从插行到本地 meta 翻转必然短于
    // sync.timeout_ms + 提交耗时；60s 裕量覆盖病态 fsync。否则 worker 可能删掉
    // 仍在途、但远端已提交的版本（客户端 200、远端无副本）。
    let cutoff = crate::db::now() - (state.cfg.sync.timeout_ms as i64 / 1000 + 60);
    let rows =
        match sqlx::query("SELECT id, bucket, key FROM sync_write_intents WHERE updated_at < ?")
            .bind(cutoff)
            .fetch_all(state.db.pool())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "sync intent 恢复：读取失败");
                return;
            }
        };
    if rows.is_empty() {
        return;
    }
    let gate = SiteSyncGate::new(
        Arc::clone(&state.db),
        Duration::from_millis(state.cfg.sync.timeout_ms),
        state.cfg.sync.max_object_bytes,
        state.site_enabled,
    );
    let (mut cleaned, mut kept) = (0usize, 0usize);
    for row in rows {
        let id: String = row.get("id");
        let bucket: String = row.get("bucket");
        let key: String = row.get("key");
        match state.store.head_object_version(&bucket, &key, &id).await {
            Ok(_) => {
                if clean_intent(&state.db, &id).await {
                    cleaned += 1;
                }
            }
            Err(
                StoreError::NoSuchKey(_)
                | StoreError::DeleteMarker(_)
                | StoreError::NoSuchBucket(_),
            ) => match gate.delete_remote_version(&bucket, &key, &id).await {
                Ok(()) => {
                    if clean_intent(&state.db, &id).await {
                        cleaned += 1;
                    }
                }
                Err(e) => {
                    kept += 1;
                    tracing::warn!(bucket = %bucket, key = %key, id = %id, error = %e,
                            "sync intent 恢复：远端删除失败，保留重试");
                }
            },
            Err(e) => {
                kept += 1;
                tracing::warn!(bucket = %bucket, key = %key, id = %id, error = %e,
                    "sync intent 恢复：本地版本查询失败，保留重试");
            }
        }
    }
    if cleaned > 0 {
        state
            .sync_metrics
            .write_intent_recovered
            .fetch_add(cleaned as u64, Ordering::Relaxed);
    }
    tracing::info!(cleaned, kept, "sync intent 恢复完成");
}

/// 30s 周期恢复 worker（启动轮由 `recover_write_intents` 先跑）。
pub fn spawn_intent_worker(state: AppState) {
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(INTENT_RECOVER_SECS));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        t.tick().await; // 首跳立即：启动时已跑过一轮
        loop {
            t.tick().await;
            recover_write_intents(&state).await;
            recover_delete_intents(&state).await;
        }
    });
}

#[async_trait]
impl PutCommitGate for SiteSyncGate {
    async fn is_sync(&self, bucket: &str) -> Result<bool, GateError> {
        let st = self.state(bucket).await?;
        if !st.sync {
            return Ok(false);
        }
        // fail-closed：删桶中/本地版本化未就绪时不得写（sync 依赖双站同 id）。
        if st.deleting {
            return Err(GateError::Unavailable("bucket is being deleted".into()));
        }
        if !st.versioned {
            // M9 §2.4：sync 桶 Suspended/Off → 409（可纠正的桶状态，不是暂时不可用）。
            return Err(GateError::Conflict(
                "sync bucket requires versioning Enabled".into(),
            ));
        }
        Ok(true)
    }

    async fn before_commit(&self, req: GateRequest<'_>) -> Result<GateTicket, GateError> {
        let result: Result<_, GateError> = async {
            if !self.site_enabled {
                return Err(GateError::Unavailable(
                    "[site] not configured: sync writes unavailable".into(),
                ));
            }
            if req.size > self.max_bytes {
                return Err(GateError::TooLarge);
            }
            let peer = self.peer().await?;
            let client = Self::client(&peer)?;
            let body = tokio::fs::read(req.plaintext).await.map_err(|e| {
                GateError::Internal(format!("read tee {}: {e}", req.plaintext.display()))
            })?;
            if body.len() as u64 != req.size {
                return Err(GateError::Internal(format!(
                    "tee size mismatch: {} != {}",
                    body.len(),
                    req.size
                )));
            }
            // 远端调用之前落 intent：此处返回 Err 时行保留，恢复会删远端可能的同 id 版本。
            self.intent_begin(&req).await?;
            let call = client.put_object_with_meta(
                "site",
                req.bucket,
                req.key,
                Bytes::from(body),
                req.etag,
                req.content_type,
                req.user_metadata,
                req.tags,
                req.storage_class,
                req.checksum,
                true, // require_version：远端 versioning 必须已 Enabled
                Some(&peer.peer_site_token),
                Some(req.local_version_id),
            );
            let remote = tokio::time::timeout(self.timeout, call)
                .await
                .map_err(|_| GateError::Unavailable("remote ack timeout".into()))?
                .map_err(remote_gate_error)?
                .ok_or_else(|| GateError::Unavailable("remote returned no version id".into()))?;
            if remote != req.local_version_id {
                // 双站同 id 的硬契约破坏：502。恢复只按本地 id 补偿，远端异 id 版本无法自动定位；
                // 记下远端 id 供 S3 sync-reconcile 修复（intent 保留不删）。
                tracing::error!(
                    bucket = req.bucket, key = req.key,
                    local = req.local_version_id, remote = %remote,
                    "sync version id mismatch：远端异 id 版本需 repair"
                );
                return Err(GateError::VersionMismatch(format!(
                    "sync version id mismatch: local {}, remote {remote}",
                    req.local_version_id
                )));
            }
            self.intent_done(req.local_version_id).await;
            Ok(GateTicket {
                remote_version_id: Some(remote),
                bucket: req.bucket.to_string(),
                key: req.key.to_string(),
                local_version_id: req.local_version_id.to_string(),
            })
        }
        .await;
        if result.is_ok() {
            self.metrics.acked.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.failed.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn abort(&self, ticket: &GateTicket) {
        match self
            .delete_remote_version(&ticket.bucket, &ticket.key, &ticket.local_version_id)
            .await
        {
            Ok(()) => clean_intent(&self.db, &ticket.local_version_id).await,
            Err(e) => tracing::warn!(
                bucket = %ticket.bucket, key = %ticket.key, error = %e,
                "sync abort：远端补偿删除失败（intent 保留，恢复重试）"
            ),
        }
    }

    async fn committed(&self, ticket: &GateTicket) {
        clean_intent(&self.db, &ticket.local_version_id).await;
    }

    fn invalidate_all(&self) {
        if let Ok(mut c) = self.cache.lock() {
            c.clear();
        }
    }

    fn invalidate(&self, bucket: &str) {
        if let Ok(mut c) = self.cache.lock() {
            c.remove(bucket);
        }
    }
}

// ———————————————— M9 S3：remote-first 删除服务 ————————————————

/// 唯一数据 peer（witness 不写 `sites` 行；0/多行 = sync 前置不满足 → 503）。
async fn peer(db: &Db) -> Result<crate::db::SitePeer, GateError> {
    let sites = db
        .list_sites()
        .await
        .map_err(|e| GateError::Unavailable(format!("read sites: {e}")))?;
    if sites.len() != 1 {
        return Err(GateError::Unavailable(format!(
            "sync requires exactly one site peer (have {})",
            sites.len()
        )));
    }
    let peer = sites.into_iter().next().expect("len == 1");
    if peer.reconcile_required {
        return Err(GateError::Unavailable(
            "peer requires sync-reconcile --repair (B' replacement not complete)".into(),
        ));
    }
    Ok(peer)
}

fn client(peer: &crate::db::SitePeer) -> Result<ReplClient, GateError> {
    ReplClient::single(
        &peer.endpoint,
        &peer.s3_access_key,
        &peer.s3_secret_key,
        &peer.region,
    )
    .map_err(|e| GateError::Unavailable(format!("site endpoint invalid: {}", e.message)))
}

/// 远端删除错误 → store 错误：409/400 保留确定性语义，其余 fail-closed 503。
fn remote_delete_error(e: ReplError) -> StoreError {
    match e.status {
        Some(409) => StoreError::Conflict(format!("remote conflict: {}", e.message)),
        Some(400) => StoreError::InvalidArgument(format!("remote rejected: {}", e.message)),
        _ => StoreError::Unavailable(format!("remote delete failed: {}", e.message)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadStatus {
    Found,
    Marker,
    NotFound,
}

/// 共享删除服务：sync 桶远端优先（intent → 对端 token 路由 → 本地提交）；
/// 非 sync 桶原样委托 store（M7 行为不变）。
pub struct SiteSyncDelete {
    db: Arc<Db>,
    store: Arc<Store>,
    timeout: Duration,
    site_enabled: bool,
    /// M9 S6c：仅 Raft 模式路由控制写（None/Local = M7 直写）。
    control: Option<Arc<crate::raft::control::ControlPlane>>,
    /// DeleteObjects 因远端失败整体 503 的次数（部分键已提交）。
    partial_total: AtomicU64,
    /// 周期 worker 收敛的 delete intent 行数。
    recovered_total: AtomicU64,
    /// 远端删除超时且 HEAD 探测无法定位（未执行/已执行不可判定）的次数。
    ambiguous_total: AtomicU64,
    /// 正在执行的删桶 flow（API 或恢复）；恢复跳过，防与在途 API 并发。
    in_flight: Mutex<HashSet<String>>,
}

/// 删桶 flow 的在途标记（drop 时移除；panic/early return 不泄漏）。
struct FlowGuard<'a> {
    svc: &'a SiteSyncDelete,
    bucket: String,
}

impl Drop for FlowGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut set) = self.svc.in_flight.lock() {
            set.remove(&self.bucket);
        }
    }
}

impl SiteSyncDelete {
    pub fn new(db: Arc<Db>, store: Arc<Store>, timeout: Duration, site_enabled: bool) -> Self {
        Self {
            db,
            store,
            timeout,
            site_enabled,
            control: None,
            partial_total: Default::default(),
            recovered_total: Default::default(),
            ambiguous_total: Default::default(),
            in_flight: Mutex::new(HashSet::new()),
        }
    }

    pub fn with_control(mut self, control: Arc<crate::raft::control::ControlPlane>) -> Self {
        self.control = Some(control);
        self
    }

    fn raft_control(&self) -> Option<&Arc<crate::raft::control::ControlPlane>> {
        self.control.as_ref().filter(|c| c.is_raft())
    }

    /// 控制写 → 删桶错误（M7 的 store 错误口径）。
    fn control_bucket_error(
        bucket: &str,
        r: Result<crate::raft::ControlResponse, crate::raft::control::ControlError>,
        missing_ok: bool,
    ) -> Result<(), StoreError> {
        use crate::raft::ControlResponse as R;
        match r {
            Ok(R::Ok) => Ok(()),
            Ok(R::NotFound) if missing_ok => Ok(()),
            Ok(R::NotFound) => Err(StoreError::NoSuchBucket(bucket.to_string())),
            Ok(R::BucketNotEmpty) => Err(StoreError::BucketNotEmpty(bucket.to_string())),
            Ok(R::Conflict) => Err(StoreError::Conflict(format!(
                "bucket delete conflict: {bucket}"
            ))),
            Ok(R::Invalid) => Err(StoreError::InvalidArgument(format!(
                "bucket delete invalid: {bucket}"
            ))),
            Err(e) => Err(StoreError::Unavailable(e.to_string())),
        }
    }

    pub fn partial_total(&self) -> u64 {
        self.partial_total.load(Ordering::Relaxed)
    }

    pub fn recovered_total(&self) -> u64 {
        self.recovered_total.load(Ordering::Relaxed)
    }

    pub fn ambiguous_total(&self) -> u64 {
        self.ambiguous_total.load(Ordering::Relaxed)
    }

    fn count_ambiguous(&self) {
        self.ambiguous_total.fetch_add(1, Ordering::Relaxed);
    }

    /// 标记删桶 flow 在途；同桶已有 flow（API 或恢复）→ None。
    fn try_begin_flow(&self, bucket: &str) -> Option<FlowGuard<'_>> {
        let mut set = self.in_flight.lock().ok()?;
        if !set.insert(bucket.to_string()) {
            return None;
        }
        Some(FlowGuard {
            svc: self,
            bucket: bucket.to_string(),
        })
    }

    /// (is_sync, deleting, versioning Enabled)。
    async fn state(&self, bucket: &str) -> Result<(bool, bool, bool), StoreError> {
        self.db
            .bucket_sync_state(bucket)
            .await
            .map(|(mode, deleting, versioning)| (mode == "sync", deleting, versioning == "Enabled"))
            .map_err(|e| StoreError::Unavailable(format!("read bucket mode: {e}")))
    }

    async fn peer(&self) -> Result<crate::db::SitePeer, StoreError> {
        peer(&self.db).await.map_err(StoreError::from)
    }

    fn client(&self, peer: &crate::db::SitePeer) -> Result<ReplClient, StoreError> {
        client(peer).map_err(StoreError::from)
    }

    /// 远端调用前落盘 intent（kill -9 后恢复的唯一凭据；FULL 同步确保先于远端可见）。
    #[allow(clippy::too_many_arguments)]
    async fn intent_insert(
        &self,
        op: &str,
        bucket: &str,
        key: &str,
        local_version_id: &str,
        remote_version_id: &str,
        marker_version_id: &str,
    ) -> Result<String, StoreError> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = crate::db::now();
        let mut conn = self
            .db
            .pool()
            .acquire()
            .await
            .map_err(|e| StoreError::Unavailable(format!("intent acquire: {e}")))?;
        sqlx::query("PRAGMA synchronous = FULL")
            .execute(&mut *conn)
            .await
            .map_err(|e| StoreError::Unavailable(format!("intent pragma: {e}")))?;
        sqlx::query(
            "INSERT INTO sync_delete_intents \
             (id, bucket, key, op, local_version_id, remote_version_id, marker_version_id, \
              state, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(bucket)
        .bind(key)
        .bind(op)
        .bind(local_version_id)
        .bind(remote_version_id)
        .bind(marker_version_id)
        .bind(INTENT_PENDING)
        .bind(now)
        .bind(now)
        .execute(&mut *conn)
        .await
        .map_err(|e| StoreError::Unavailable(format!("delete intent: {e}")))?;
        Ok(id)
    }

    async fn intent_done(&self, id: &str) {
        if let Err(e) =
            sqlx::query("UPDATE sync_delete_intents SET state = ?, updated_at = ? WHERE id = ?")
                .bind(INTENT_DONE)
                .bind(crate::db::now())
                .bind(id)
                .execute(self.db.pool())
                .await
        {
            tracing::warn!(id, error = %e, "delete intent 状态更新失败（恢复按 pending 重放）");
        }
    }

    async fn intent_clean(&self, id: &str) {
        if let Err(e) = sqlx::query("DELETE FROM sync_delete_intents WHERE id = ?")
            .bind(id)
            .execute(self.db.pool())
            .await
        {
            tracing::warn!(id, error = %e, "delete intent 删除失败（下轮恢复重试）");
        }
    }

    async fn remote_delete_version(
        &self,
        client: &ReplClient,
        peer: &crate::db::SitePeer,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(), StoreError> {
        let call = client.delete_object_version(
            "site",
            bucket,
            key,
            version_id,
            Some(&peer.peer_site_token),
        );
        match tokio::time::timeout(self.timeout, call).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(remote_delete_error(e)),
            Err(_) => Err(StoreError::Unavailable("remote delete timeout".into())),
        }
    }

    async fn remote_head(
        &self,
        client: &ReplClient,
        peer: &crate::db::SitePeer,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<HeadStatus, StoreError> {
        let call = client.head_object_version(
            "site",
            bucket,
            key,
            version_id,
            Some(&peer.peer_site_token),
        );
        match tokio::time::timeout(self.timeout, call).await {
            Ok(Ok(true)) => Ok(HeadStatus::Found),
            Ok(Ok(false)) => Ok(HeadStatus::Marker),
            Ok(Err(e)) if e.status == Some(404) => Ok(HeadStatus::NotFound),
            Ok(Err(e)) => Err(remote_delete_error(e)),
            Err(_) => Err(StoreError::Unavailable("remote head timeout".into())),
        }
    }

    /// N2：远端删版本失败（超时/不可达）后 HEAD 探测一次（同一超时预算）：
    /// 404 → 已删，继续本地提交；仍在 → 重试删除一次；探测失败/重试仍失败 → ambiguous。
    async fn remote_delete_version_checked(
        &self,
        client: &ReplClient,
        peer: &crate::db::SitePeer,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(), StoreError> {
        let first = self
            .remote_delete_version(client, peer, bucket, key, version_id)
            .await;
        let Err(e) = first else { return Ok(()) };
        if !matches!(e, StoreError::Unavailable(_)) {
            return Err(e);
        }
        match self
            .remote_head(client, peer, bucket, key, version_id)
            .await
        {
            Ok(HeadStatus::NotFound) => Ok(()),
            // 版本仍在（或同 id marker）：删除结果不确定，重试一次幂等删除。
            Ok(HeadStatus::Found | HeadStatus::Marker) => {
                match self
                    .remote_delete_version(client, peer, bucket, key, version_id)
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(e2) => {
                        if matches!(e2, StoreError::Unavailable(_)) {
                            self.count_ambiguous();
                        }
                        Err(e2)
                    }
                }
            }
            Err(_) => {
                self.count_ambiguous();
                Err(e)
            }
        }
    }

    /// N2：远端建 marker 超时后 HEAD 探测：405 → 已建，继续本地；404 → 重试一次；
    /// 200/探测失败 → ambiguous。
    async fn remote_create_marker_checked(
        &self,
        client: &ReplClient,
        peer: &crate::db::SitePeer,
        bucket: &str,
        key: &str,
        marker: &str,
    ) -> Result<(), StoreError> {
        let call = client.delete_marker("site", bucket, key, marker, Some(&peer.peer_site_token));
        match tokio::time::timeout(self.timeout, call).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(remote_delete_error(e)),
            Err(_) => match self.remote_head(client, peer, bucket, key, marker).await {
                Ok(HeadStatus::Marker) => Ok(()),
                Ok(HeadStatus::NotFound) => {
                    let retry = client.delete_marker(
                        "site",
                        bucket,
                        key,
                        marker,
                        Some(&peer.peer_site_token),
                    );
                    match tokio::time::timeout(self.timeout, retry).await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(remote_delete_error(e)),
                        Err(_) => {
                            self.count_ambiguous();
                            Err(StoreError::Unavailable(
                                "remote delete marker timeout".into(),
                            ))
                        }
                    }
                }
                Ok(HeadStatus::Found) | Err(_) => {
                    self.count_ambiguous();
                    Err(StoreError::Unavailable(
                        "remote delete marker timeout".into(),
                    ))
                }
            },
        }
    }

    /// 点名删版本：远端同 id 删除 → 本地删。M9 前无映射对象 → 409（需 --repair）。
    async fn delete_version_remote(
        &self,
        client: &ReplClient,
        peer: &crate::db::SitePeer,
        bucket: &str,
        key: &str,
        vid: &str,
        bypass: bool,
    ) -> Result<DeleteOutcome, StoreError> {
        match self.store.head_object_version(bucket, key, vid).await {
            Ok(info) => match info.remote_version_id.as_deref() {
                Some(rid) if rid == vid => {}
                Some(rid) => {
                    return Err(StoreError::Conflict(format!(
                        "sync mapping mismatch (local {vid}, remote {rid}); run sync-reconcile --repair"
                    )));
                }
                None => {
                    return Err(StoreError::Conflict(
                        "object has no remote mapping; run sync-reconcile --repair".into(),
                    ));
                }
            },
            // marker 自身 id 即远端 id（双站同 id 契约）；本地已无 = 幂等继续。
            Err(StoreError::DeleteMarker(_))
            | Err(StoreError::NoSuchKey(_))
            | Err(StoreError::NoSuchBucket(_)) => {}
            Err(e) => return Err(e),
        }
        let id = self
            .intent_insert("delete_version", bucket, key, vid, vid, "")
            .await?;
        if let Err(e) = self
            .remote_delete_version_checked(client, peer, bucket, key, vid)
            .await
        {
            tracing::warn!(bucket, key, vid, error = %e, "sync 版本删：远端失败（intent 保留）");
            return Err(e);
        }
        self.intent_done(&id).await;
        match self
            .store
            .delete_object_versioned_ext(bucket, key, Some(vid), true, bypass)
            .await
        {
            Ok(out) => {
                self.intent_clean(&id).await;
                Ok(out)
            }
            Err(e) => {
                tracing::warn!(bucket, key, vid, error = %e, "sync 版本删：本地失败（intent 保留，worker 重试）");
                Err(e)
            }
        }
    }

    /// 无版本号删：预生成 marker UUID，远端先建同 id marker，本地再落同 id。
    async fn create_marker_remote(
        &self,
        client: &ReplClient,
        peer: &crate::db::SitePeer,
        bucket: &str,
        key: &str,
    ) -> Result<DeleteOutcome, StoreError> {
        let marker = uuid::Uuid::new_v4().to_string();
        let id = self
            .intent_insert("create_marker", bucket, key, "", &marker, &marker)
            .await?;
        if let Err(e) = self
            .remote_create_marker_checked(client, peer, bucket, key, &marker)
            .await
        {
            tracing::warn!(bucket, key, error = %e, "sync marker：远端失败（intent 保留）");
            return Err(e);
        }
        self.intent_done(&id).await;
        let spec = DeleteMarkerSpec {
            marker_uuid: marker.clone(),
            remote_version_id: Some(marker),
        };
        match self
            .store
            .delete_object_versioned_with_marker(bucket, key, None, true, false, Some(spec))
            .await
        {
            Ok(out) => {
                self.intent_clean(&id).await;
                Ok(out)
            }
            Err(e) => {
                tracing::warn!(bucket, key, error = %e, "sync marker：本地失败（intent 保留，worker 补齐）");
                Err(e)
            }
        }
    }

    /// 删桶：两站 quiesce → 两站查空 → 远端删 → 本地删。
    /// 失败保留 deleting + intent（worker 重试）；非空/超时回滚并 409。
    async fn delete_bucket_flow(&self, bucket: &str, intent_id: &str) -> Result<(), StoreError> {
        if !self.site_enabled {
            return Err(StoreError::Unavailable(
                "[site] not configured: sync deletes unavailable".into(),
            ));
        }
        let peer = self.peer().await?;
        let client = self.client(&peer)?;
        if let Some(control) = self.raft_control() {
            match control
                .write(crate::raft::ControlCommand::BucketSetDeleting {
                    name: bucket.to_string(),
                    deleting: true,
                })
                .await
            {
                Ok(crate::raft::ControlResponse::Ok) => {}
                Ok(crate::raft::ControlResponse::NotFound) => {
                    // 行已不存在：重试撞上已提交的删桶（intent 未清）。物理残留补 tombstone 后收敛。
                    if let Err(e) = self.store.delete_bucket_sync(bucket).await {
                        tracing::warn!(bucket, error = %e, "删桶重试：tombstone 残留目录失败");
                    }
                    self.store.unmark_deleting(bucket);
                    self.store.invalidate_gate(bucket);
                    self.intent_clean(intent_id).await;
                    return Ok(());
                }
                Ok(other) => {
                    return Err(StoreError::Conflict(format!(
                        "set deleting rejected: {other:?}"
                    )));
                }
                Err(e) => return Err(StoreError::Unavailable(format!("set deleting: {e}"))),
            }
        } else {
            self.db
                .set_bucket_deleting(bucket, true)
                .await
                .map_err(|e| StoreError::Unavailable(format!("set deleting: {e}")))?;
        }
        self.store.invalidate_gate(bucket);
        self.store.mark_deleting(bucket);
        if !self.store.wait_writers_idle(bucket, QUIESCE_TIMEOUT).await {
            self.rollback_bucket(&client, &peer, bucket).await;
            return Err(StoreError::Unavailable(
                "timed out waiting for active writers".into(),
            ));
        }
        // 对端 quiesce：等本站计数归零后才 200；超时/非空 → 409/503 并回滚两端。
        let token = Some(peer.peer_site_token.as_str());
        let call = client.bucket_quiesce("site", bucket, token);
        match tokio::time::timeout(QUIESCE_CALL_TIMEOUT, call).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.rollback_bucket(&client, &peer, bucket).await;
                let e = remote_delete_error(e);
                self.intent_clean(intent_id).await;
                return Err(e);
            }
            Err(_) => {
                self.rollback_bucket(&client, &peer, bucket).await;
                self.intent_clean(intent_id).await;
                return Err(StoreError::Unavailable("remote quiesce timeout".into()));
            }
        }
        let empty_local = match self.store.bucket_is_empty(bucket).await {
            Ok(v) => v,
            Err(e) => {
                self.rollback_bucket(&client, &peer, bucket).await;
                self.intent_clean(intent_id).await;
                return Err(e);
            }
        };
        let empty_remote = match tokio::time::timeout(
            QUIESCE_CALL_TIMEOUT,
            client.bucket_is_empty("site", bucket, token),
        )
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                self.rollback_bucket(&client, &peer, bucket).await;
                let e = remote_delete_error(e);
                self.intent_clean(intent_id).await;
                return Err(e);
            }
            Err(_) => {
                self.rollback_bucket(&client, &peer, bucket).await;
                self.intent_clean(intent_id).await;
                return Err(StoreError::Unavailable("remote empty check timeout".into()));
            }
        };
        if !empty_local || !empty_remote {
            self.rollback_bucket(&client, &peer, bucket).await;
            self.intent_clean(intent_id).await;
            return Err(StoreError::BucketNotEmpty(bucket.to_string()));
        }
        // 超时/失败：保留 deleting + intent，worker 重试（远端可能已删，重试幂等）。
        match tokio::time::timeout(
            QUIESCE_CALL_TIMEOUT,
            client.delete_bucket("site", bucket, token),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(remote_delete_error(e)),
            Err(_) => {
                return Err(StoreError::Unavailable(
                    "remote delete bucket timeout".into(),
                ));
            }
        }
        // 远端已删：本地失败同样保留 intent（重试先撞远端 404 = 已删）。
        if let Some(control) = self.raft_control() {
            // 一条 BucketDelete：apply 级联清表，post-commit 以 delete_bucket_sync 移走物理目录。
            // NotFound = 已删（重试幂等）；行已不在，无需再写 deleting=0。
            Self::control_bucket_error(
                bucket,
                control
                    .write(crate::raft::ControlCommand::BucketDelete {
                        name: bucket.to_string(),
                    })
                    .await,
                true,
            )?;
        } else {
            // 本地已无桶（删除已生效/恢复重跑）= 成功，继续清标志与 intent（I4）。
            match self.store.delete_bucket_sync(bucket).await {
                Ok(()) | Err(StoreError::NoSuchBucket(_)) => {}
                Err(e) => return Err(e),
            }
            if let Err(e) = self.db.set_bucket_deleting(bucket, false).await {
                tracing::warn!(bucket, error = %e, "清 deleting 标志失败（worker 会重试删桶）");
            }
        }
        self.store.unmark_deleting(bucket);
        self.store.invalidate_gate(bucket);
        self.intent_clean(intent_id).await;
        Ok(())
    }

    /// quiesce 回滚：释放对端 + 本地 deleting + DB deleting=0。
    async fn rollback_bucket(&self, client: &ReplClient, peer: &crate::db::SitePeer, bucket: &str) {
        let _ = tokio::time::timeout(
            QUIESCE_CALL_TIMEOUT,
            client.bucket_quiesce_release("site", bucket, Some(&peer.peer_site_token)),
        )
        .await;
        self.store.unmark_deleting(bucket);
        if let Some(control) = self.raft_control() {
            match control
                .write(crate::raft::ControlCommand::BucketSetDeleting {
                    name: bucket.to_string(),
                    deleting: false,
                })
                .await
            {
                Ok(crate::raft::ControlResponse::Ok | crate::raft::ControlResponse::NotFound) => {}
                Ok(other) => tracing::warn!(bucket, ?other, "回滚 deleting 标志被拒"),
                Err(e) => tracing::warn!(bucket, error = %e, "回滚 deleting 标志失败"),
            }
        } else if let Err(e) = self.db.set_bucket_deleting(bucket, false).await {
            tracing::warn!(bucket, error = %e, "回滚 deleting 标志失败");
        }
        self.store.invalidate_gate(bucket);
    }
}

#[async_trait]
impl SyncDeleteSink for SiteSyncDelete {
    async fn delete_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        versioned: bool,
        bypass_governance: bool,
    ) -> Result<DeleteOutcome, StoreError> {
        let (sync, deleting, versioning) = self.state(bucket).await?;
        if !sync {
            return self
                .store
                .delete_object_versioned_ext(bucket, key, version_id, versioned, bypass_governance)
                .await;
        }
        // I1：删桶 quiesce 窗口内拒绝新删除（先于任何远端调用，避免远端已删/本地 503 分叉）。
        if deleting {
            return Err(StoreError::Unavailable("bucket is being deleted".into()));
        }
        if !self.site_enabled {
            return Err(StoreError::Unavailable(
                "[site] not configured: sync deletes unavailable".into(),
            ));
        }
        if !versioning {
            return Err(StoreError::Conflict(
                "sync bucket requires versioning Enabled".into(),
            ));
        }
        let peer = self.peer().await?;
        let client = self.client(&peer)?;
        match version_id {
            Some(vid) => {
                self.delete_version_remote(&client, &peer, bucket, key, vid, bypass_governance)
                    .await
            }
            None if versioned => self.create_marker_remote(&client, &peer, bucket, key).await,
            None => Err(StoreError::Conflict(
                "sync bucket requires versioning Enabled".into(),
            )),
        }
    }

    async fn delete_key_all_versions(&self, bucket: &str, key: &str) -> Result<bool, StoreError> {
        let (sync, deleting, _versioning) = self.state(bucket).await?;
        if !sync {
            return self.store.delete_object(bucket, key).await;
        }
        if deleting {
            return Err(StoreError::Unavailable("bucket is being deleted".into()));
        }
        if !self.site_enabled {
            return Err(StoreError::Unavailable(
                "[site] not configured: sync deletes unavailable".into(),
            ));
        }
        let peer = self.peer().await?;
        let client = self.client(&peer)?;
        let id = self
            .intent_insert("delete_key", bucket, key, "", "", "")
            .await?;
        match tokio::time::timeout(
            self.timeout,
            client.delete_object_all("site", bucket, key, Some(&peer.peer_site_token)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let e = remote_delete_error(e);
                tracing::warn!(bucket, key, error = %e, "sync 整键删：远端失败（intent 保留）");
                return Err(e);
            }
            Err(_) => {
                // 整键删无版本 id 可 HEAD 定位：保留 intent，计为歧义，下轮重试幂等。
                self.count_ambiguous();
                return Err(StoreError::Unavailable("remote delete timeout".into()));
            }
        }
        self.intent_done(&id).await;
        match self.store.delete_object(bucket, key).await {
            Ok(existed) => {
                self.intent_clean(&id).await;
                Ok(existed)
            }
            Err(e) => {
                tracing::warn!(bucket, key, error = %e, "sync 整键删：本地失败（intent 保留）");
                Err(e)
            }
        }
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let (sync, deleting, _versioning) = self.state(bucket).await?;
        if !sync {
            if let Some(control) = self.raft_control() {
                // 非 sync：本地空预检后一条 BucketDelete（物理目录由 post-commit 幂等删除）。
                if !self.store.bucket_exists(bucket).await? {
                    return Err(StoreError::NoSuchBucket(bucket.to_string()));
                }
                if !self.store.bucket_is_empty(bucket).await? {
                    return Err(StoreError::BucketNotEmpty(bucket.to_string()));
                }
                return Self::control_bucket_error(
                    bucket,
                    control
                        .write(crate::raft::ControlCommand::BucketDelete {
                            name: bucket.to_string(),
                        })
                        .await,
                    false,
                );
            }
            return self.store.delete_bucket(bucket).await;
        }
        // I1：已在删除中 → 409；在途 flow 由 in_flight 串行化（第二个请求不触远端）。
        if deleting {
            return Err(StoreError::Conflict(
                "bucket delete already in progress".into(),
            ));
        }
        let Some(_flow) = self.try_begin_flow(bucket) else {
            return Err(StoreError::Conflict(
                "bucket delete already in progress".into(),
            ));
        };
        let id = self
            .intent_insert("delete_bucket", bucket, "", "", "", "")
            .await?;
        self.delete_bucket_flow(bucket, &id).await
    }

    fn record_partial(&self) {
        self.partial_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// 启动 + 周期恢复：`remote_pending` 先按远端 HEAD 收敛（200/405=在、404=不在），
/// `remote_done` 只补本地；本地失败保留行下一轮重试（不重启也恢复）。
pub async fn recover_delete_intents(state: &AppState) {
    let cutoff = crate::db::now() - (state.cfg.sync.timeout_ms as i64 / 1000 + 60);
    let rows = match sqlx::query(
        "SELECT id, bucket, key, op, local_version_id, remote_version_id, marker_version_id, state \
         FROM sync_delete_intents WHERE updated_at < ? ORDER BY updated_at LIMIT ?",
    )
    .bind(cutoff)
    .bind(RECOVER_BATCH)
    .fetch_all(state.db.pool())
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "delete intent 恢复：读取失败");
            return;
        }
    };
    if rows.is_empty() {
        return;
    }
    let svc = &*state.sync_delete;
    let mut recovered = 0usize;
    let mut consecutive = 0usize;
    for row in rows {
        let id: String = row.get("id");
        let bucket: String = row.get("bucket");
        let key: String = row.get("key");
        let op: String = row.get("op");
        let local_vid: Option<String> = row.get("local_version_id");
        let marker_vid: Option<String> = row.get("marker_version_id");
        let done = row.get::<String, _>("state") == INTENT_DONE;
        if recover_row(svc, &id, &bucket, &key, &op, local_vid, marker_vid, done).await {
            recovered += 1;
            consecutive = 0;
        } else {
            consecutive += 1;
            if consecutive >= RECOVER_MAX_CONSECUTIVE_FAILURES {
                tracing::warn!(consecutive, "delete intent 恢复：连续无进展，本轮提前结束");
                break;
            }
        }
    }
    if recovered > 0 {
        svc.recovered_total
            .fetch_add(recovered as u64, Ordering::Relaxed);
        tracing::info!(recovered, "sync delete intent 恢复完成");
    }
}

/// 恢复单行；返回 true 表示本轮已有进展（行已清理）。
#[allow(clippy::too_many_arguments)]
async fn recover_row(
    svc: &SiteSyncDelete,
    id: &str,
    bucket: &str,
    key: &str,
    op: &str,
    local_vid: Option<String>,
    marker_vid: Option<String>,
    done: bool,
) -> bool {
    match op {
        "delete_bucket" => {
            // C1：只有该桶当前仍是“deleting 的 sync 桶”才允许重跑 flow；
            // 行缺失或 deleting=0（已重建同名桶/已回滚）只清 intent，不碰数据面与远端。
            match svc.db.bucket_sync_state_row(bucket).await {
                Ok(Some((mode, true, _))) if mode == "sync" => {}
                Ok(_) => {
                    svc.intent_clean(id).await;
                    return true;
                }
                Err(e) => {
                    tracing::warn!(id, bucket, error = %e, "delete intent 恢复：读桶状态失败");
                    return false;
                }
            }
            let Some(_flow) = svc.try_begin_flow(bucket) else {
                return false; // API flow 在途，下轮再看
            };
            svc.delete_bucket_flow(bucket, id).await.is_ok()
        }
        "delete_version" => {
            let Some(vid) = local_vid.filter(|s| !s.is_empty()) else {
                tracing::warn!(id, bucket, key, "delete intent 缺 local_version_id（清理）");
                svc.intent_clean(id).await;
                return true;
            };
            let remote_ok = if done {
                true
            } else {
                match svc.peer().await {
                    Ok(peer) => match svc.client(&peer) {
                        Ok(client) => {
                            match svc.remote_head(&client, &peer, bucket, key, &vid).await {
                                Ok(HeadStatus::NotFound) => true,
                                Ok(_) => svc
                                    .remote_delete_version(&client, &peer, bucket, key, &vid)
                                    .await
                                    .is_ok(),
                                Err(_) => false,
                            }
                        }
                        Err(_) => false,
                    },
                    Err(_) => false,
                }
            };
            if !remote_ok {
                return false;
            }
            svc.intent_done(id).await;
            if svc
                .store
                .delete_object_versioned_ext(bucket, key, Some(&vid), true, false)
                .await
                .is_ok()
            {
                svc.intent_clean(id).await;
                return true;
            }
            false
        }
        "create_marker" => {
            let Some(marker) = marker_vid.filter(|s| !s.is_empty()) else {
                tracing::warn!(
                    id,
                    bucket,
                    key,
                    "delete intent 缺 marker_version_id（清理）"
                );
                svc.intent_clean(id).await;
                return true;
            };
            let remote_ok = if done {
                true
            } else {
                match svc.peer().await {
                    Ok(peer) => match svc.client(&peer) {
                        Ok(client) => {
                            match svc.remote_head(&client, &peer, bucket, key, &marker).await {
                                Ok(HeadStatus::NotFound) => {
                                    let call = client.delete_marker(
                                        "site",
                                        bucket,
                                        key,
                                        &marker,
                                        Some(&peer.peer_site_token),
                                    );
                                    tokio::time::timeout(svc.timeout, call)
                                        .await
                                        .is_ok_and(|r| r.is_ok())
                                }
                                Ok(_) => true,
                                Err(_) => false,
                            }
                        }
                        Err(_) => false,
                    },
                    Err(_) => false,
                }
            };
            if !remote_ok {
                return false;
            }
            svc.intent_done(id).await;
            // 本地已有同 id（marker 或版本）→ 只清 intent，不重造（at-least-once）。
            match svc.store.head_object_version(bucket, key, &marker).await {
                Ok(_) | Err(StoreError::DeleteMarker(_)) => {
                    svc.intent_clean(id).await;
                    true
                }
                Err(StoreError::NoSuchKey(_)) => {
                    let spec = DeleteMarkerSpec {
                        marker_uuid: marker.clone(),
                        remote_version_id: Some(marker),
                    };
                    if svc
                        .store
                        .delete_object_versioned_with_marker(
                            bucket,
                            key,
                            None,
                            true,
                            false,
                            Some(spec),
                        )
                        .await
                        .is_ok()
                    {
                        svc.intent_clean(id).await;
                        true
                    } else {
                        false
                    }
                }
                Err(_) => false,
            }
        }
        "delete_key" => {
            let remote_ok = match svc.peer().await {
                Ok(peer) => match svc.client(&peer) {
                    Ok(client) => {
                        let call = client.delete_object_all(
                            "site",
                            bucket,
                            key,
                            Some(&peer.peer_site_token),
                        );
                        tokio::time::timeout(svc.timeout, call)
                            .await
                            .is_ok_and(|r| r.is_ok())
                    }
                    Err(_) => false,
                },
                Err(_) => false,
            };
            if !remote_ok {
                return false;
            }
            if svc.store.delete_object(bucket, key).await.is_ok() {
                svc.intent_clean(id).await;
                return true;
            }
            false
        }
        other => {
            tracing::warn!(op = other, id, "未知 delete intent op（保留）");
            false
        }
    }
}
