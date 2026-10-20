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
use xl_store::gate::{GateError, GateRequest, GateTicket, PutCommitGate};

use crate::db::Db;
use crate::repl_client::ReplClient;

/// 模式缓存 TTL（建桶/删桶/apply 会显式失效；TTL 只兜底漏网）。
const CACHE_TTL: Duration = Duration::from_secs(10);

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
    pub fn new(db: Arc<Db>, timeout: Duration, max_bytes: u64) -> Self {
        Self {
            db,
            timeout,
            max_bytes,
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
            return Err(GateError::Unavailable(
                "sync bucket requires versioning Enabled".into(),
            ));
        }
        Ok(true)
    }

    async fn before_commit(&self, req: GateRequest<'_>) -> Result<GateTicket, GateError> {
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
        let call = client.put_object_with_meta(
            "site",
            req.bucket,
            req.key,
            Bytes::from(body),
            req.etag,
            req.content_type,
            req.user_metadata,
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
            return Err(GateError::Unavailable(format!(
                "remote version id mismatch: local {}, remote {remote}",
                req.local_version_id
            )));
        }
        Ok(GateTicket {
            remote_version_id: Some(remote),
            bucket: req.bucket.to_string(),
            key: req.key.to_string(),
            local_version_id: req.local_version_id.to_string(),
        })
    }

    async fn abort(&self, ticket: &GateTicket) {
        let peer = match self.peer().await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(bucket = %ticket.bucket, key = %ticket.key, error = %e,
                    "sync abort：读站点对失败，远端版本可能残留");
                return;
            }
        };
        let client = match Self::client(&peer) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "sync abort：站点客户端构造失败");
                return;
            }
        };
        let call = client.delete_object_version(
            "site",
            &ticket.bucket,
            &ticket.key,
            &ticket.local_version_id,
            Some(&peer.peer_site_token),
        );
        match tokio::time::timeout(self.timeout, call).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(
                bucket = %ticket.bucket, key = %ticket.key, error = %e.message,
                "sync abort：远端补偿删除失败（远端孤儿，需对账）"
            ),
            Err(_) => tracing::warn!(
                bucket = %ticket.bucket, key = %ticket.key,
                "sync abort：远端补偿删除超时（远端孤儿，需对账）"
            ),
        }
    }

    fn invalidate(&self, bucket: &str) {
        if let Ok(mut c) = self.cache.lock() {
            c.remove(bucket);
        }
    }
}
