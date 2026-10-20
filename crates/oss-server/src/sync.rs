//! M9 S1：sync 桶写提交门实现（store 调用的远端确认）。
//!
//! 顺序：store staging 完成（不可见）→ 本门对唯一站点 peer 做 token-auth PUT
//! （要求远端 version id == 本地预生成 UUID）→ store 翻转本地 meta。
//! 失败一律 fail-closed（503）；无降级、无后台重试。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sqlx::Row as _;
use xl_store::error::Error as StoreError;
use xl_store::gate::{GateError, GateRequest, GateTicket, PutCommitGate};

use crate::db::Db;
use crate::repl_client::ReplClient;
use crate::state::AppState;

/// 模式缓存 TTL（建桶/删桶/apply 会显式失效；TTL 只兜底漏网）。
const CACHE_TTL: Duration = Duration::from_secs(10);
/// 写 intent 状态：远端调用前 / 远端已确认（本地未提交）。
const INTENT_PENDING: &str = "remote_pending";
const INTENT_DONE: &str = "remote_done";
/// 写 intent 恢复周期（启动时先跑一轮）。
const INTENT_RECOVER_SECS: u64 = 30;

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

/// 站点 sync 门：`buckets.replication_mode='sync'` 的写必须远端先确认。
pub struct SiteSyncGate {
    db: Arc<Db>,
    timeout: Duration,
    max_bytes: u64,
    /// `[site]` 已配置（未配置时 sync 写不可用——残留 sites 行不足以授权远端写）。
    site_enabled: bool,
    cache: Mutex<HashMap<String, Cached>>,
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
        }
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
        let sites = self
            .db
            .list_sites()
            .await
            .map_err(|e| GateError::Unavailable(format!("read sites: {e}")))?;
        if sites.len() != 1 {
            return Err(GateError::Unavailable(format!(
                "sync requires exactly one site peer (have {})",
                sites.len()
            )));
        }
        Ok(sites.into_iter().next().expect("len == 1"))
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
async fn clean_intent(db: &Db, id: &str) {
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
    if let Err(e) = sqlx::query("DELETE FROM sync_write_intents WHERE id = ?")
        .bind(id)
        .execute(db.pool())
        .await
    {
        tracing::warn!(id, error = %e, "sync intent 删除失败（下轮恢复重试）");
    }
}

/// 启动 + 周期恢复：本地 meta 有该 id → 只清 intent；否则删远端同 id。
/// 本地查询失败/远端不可达 → 保留，30s 后重试。
pub async fn recover_write_intents(state: &AppState) {
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
                clean_intent(&state.db, &id).await;
                cleaned += 1;
            }
            Err(
                StoreError::NoSuchKey(_)
                | StoreError::DeleteMarker(_)
                | StoreError::NoSuchBucket(_),
            ) => match gate.delete_remote_version(&bucket, &key, &id).await {
                Ok(()) => {
                    clean_intent(&state.db, &id).await;
                    cleaned += 1;
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
            GateError::Unavailable(format!("read tee {}: {e}", req.plaintext.display()))
        })?;
        if body.len() as u64 != req.size {
            return Err(GateError::Unavailable(format!(
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
            .map_err(|e| GateError::Unavailable(format!("remote put failed: {}", e.message)))?
            .ok_or_else(|| GateError::Unavailable("remote returned no version id".into()))?;
        if remote != req.local_version_id {
            // 双站同 id 的硬契约破坏：502（不是重试可解的暂时不可用），intent 保留待 repair。
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

    fn invalidate(&self, bucket: &str) {
        if let Ok(mut c) = self.cache.lock() {
            c.remove(bucket);
        }
    }
}
