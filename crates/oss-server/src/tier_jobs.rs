//! M8 S4b：Transition / Restore 作业执行、sweep、reaper 与启动对账。
//!
//! 恢复顺序（C7）：prepare 本地数据（不可见）→ 行 restored/expiry → meta Restored。
//! 任一步崩溃由 [`reconcile_restores`]（启动）与 [`reap_once`] 按 Version 为准修正。

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use xl_s3::events::{EventSink, EventSource, S3EventOp};
use xl_s3::restore::RestoreSink;
use xl_store::meta::{TierPhase, is_archive_class};
use xl_store::store::Store;

use crate::db::{Db, JobSpec, NewJobTask};
use crate::jobs::TaskExecutor;
use crate::tier::{TierBackends, tier_key};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TransitionPayload {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) version_id: String,
    pub(crate) class: String,
    #[serde(default)]
    pub(crate) tier: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RestorePayload {
    bucket: String,
    key: String,
    version_id: String,
    days: u32,
}

/// Restore 入队：单事务写 `tier_restores`(pending) + 作业。
pub struct SqlRestoreSink {
    db: Arc<Db>,
}

impl SqlRestoreSink {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RestoreSink for SqlRestoreSink {
    async fn enqueue_restore(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        days: u32,
    ) -> Result<i64, String> {
        let payload = serde_json::to_string(&RestorePayload {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.to_string(),
            days,
        })
        .map_err(|e| e.to_string())?;
        let spec = JobSpec {
            kind: "restore".to_string(),
            bucket: bucket.to_string(),
            prefix: key.to_string(),
            // 不做 dedupe：Restore 去重由 store 的 phase（Restoring/Restored → 409）保证；
            // 同键任务若被 IGNORE，会留下一条无任务的空作业。
            tasks: vec![NewJobTask {
                action: "RestoreObject".to_string(),
                payload,
                dedupe_key: None,
            }],
            ..Default::default()
        };
        self.db
            .create_restore_job(&spec, bucket, key, version_id, crate::db::now())
            .await
            .map(|(id, _)| id)
            .map_err(|e| e.to_string())
    }
}

/// 作业执行器：`Transition` / `RestoreObject`。
pub struct M8TaskExecutor {
    db: Arc<Db>,
    store: Arc<Store>,
    tiers: Arc<TierBackends>,
    events: Arc<crate::events::SqlEventSink>,
    max_tier_bytes: u64,
}

impl M8TaskExecutor {
    pub fn new(
        db: Arc<Db>,
        store: Arc<Store>,
        tiers: Arc<TierBackends>,
        events: Arc<crate::events::SqlEventSink>,
    ) -> Self {
        let max_tier_bytes = std::env::var("OSS_TIER_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1 << 30);
        Self {
            db,
            store,
            tiers,
            events,
            max_tier_bytes,
        }
    }

    fn emit(
        &self,
        op: S3EventOp,
        bucket: &str,
        key: &str,
        vid: Option<&str>,
        size: Option<u64>,
        etag: Option<&str>,
    ) {
        let events = Arc::clone(&self.events);
        let (bucket, key, vid, etag) = (
            bucket.to_string(),
            key.to_string(),
            vid.map(str::to_string),
            etag.map(str::to_string),
        );
        tokio::spawn(async move {
            events
                .emit(
                    op,
                    &bucket,
                    &key,
                    vid.as_deref(),
                    size,
                    etag.as_deref(),
                    EventSource::internal("oss-tier"),
                )
                .await;
        });
    }

    async fn transition(&self, p: &TransitionPayload) -> Result<String, String> {
        let Some(v) = self
            .store
            .tier_version(&p.bucket, &p.key, &p.version_id)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok("missing".into());
        };
        if v.tier.is_some() {
            if v.storage_class.as_deref() == Some(p.class.as_str()) {
                return Ok("archived".into());
            }
            return Err(format!(
                "版本已是 {:?} 分层对象，不能转为 {}",
                v.storage_class, p.class
            ));
        }
        if is_archive_class(&p.class) {
            let tier_id = p
                .tier
                .as_deref()
                .ok_or_else(|| format!("归档类 {} 缺 tier", p.class))?;
            let backend = self
                .tiers
                .get(tier_id)
                .ok_or_else(|| format!("tier {tier_id:?} 未配置"))?;
            // 先按元数据拒绝超限/加密对象，再读入内存（避免为拒绝而整份加载）。
            if v.size > self.max_tier_bytes {
                return Err(format!(
                    "对象 {} 字节超过 OSS_TIER_MAX_BYTES={}",
                    v.size, self.max_tier_bytes
                ));
            }
            if v.encryption.is_some() {
                return Err("SSE 对象不参与 Transition".to_string());
            }
            let (info, bytes) = self
                .store
                .get_object_version_ext(&p.bucket, &p.key, &p.version_id, None)
                .await
                .map_err(|e| e.to_string())?;
            if bytes.len() as u64 != info.size {
                return Err(format!(
                    "读取长度 {} ≠ 元数据 {} 字节",
                    bytes.len(),
                    info.size
                ));
            }
            let md5 = hex::encode(md5::Md5::digest(&bytes));
            let sha256 = hex::encode(sha2::Sha256::digest(&bytes));
            let key = tier_key(tier_id, &p.bucket, &p.version_id, &p.key);
            let remote_etag = backend.put(&key, bytes, &md5).await?;
            self.store
                .archive_version(
                    &p.bucket,
                    &p.key,
                    &p.version_id,
                    xl_store::store::ArchiveSpec {
                        backend: tier_id.to_string(),
                        key,
                        size: info.size,
                        source_etag: info.etag.clone(),
                        etag: remote_etag,
                        sha256,
                        class: p.class.clone(),
                    },
                    crate::db::now(),
                )
                .await
                .map_err(|e| e.to_string())?;
            self.emit(
                S3EventOp::LifecycleTransition,
                &p.bucket,
                &p.key,
                Some(&p.version_id),
                Some(info.size),
                Some(&info.etag),
            );
            Ok("archived".into())
        } else {
            self.store
                .set_storage_class(&p.bucket, &p.key, &p.version_id, &p.class)
                .await
                .map_err(|e| e.to_string())?;
            self.emit(
                S3EventOp::LifecycleTransition,
                &p.bucket,
                &p.key,
                Some(&p.version_id),
                Some(v.size),
                Some(&v.etag),
            );
            Ok("ok".into())
        }
    }

    async fn restore(&self, p: &RestorePayload) -> Result<String, String> {
        let Some(v) = self
            .store
            .tier_version(&p.bucket, &p.key, &p.version_id)
            .await
            .map_err(|e| e.to_string())?
        else {
            let _ = self
                .db
                .delete_restore(&p.bucket, &p.key, &p.version_id)
                .await;
            return Ok("missing".into());
        };
        let Some(t) = v.tier else {
            let _ = self
                .db
                .delete_restore(&p.bucket, &p.key, &p.version_id)
                .await;
            return Ok("missing".into());
        };
        if t.phase == TierPhase::Restored {
            return Ok("restored".into());
        }
        if t.phase == TierPhase::Archived {
            self.store
                .mark_restore_pending(&p.bucket, &p.key, &p.version_id)
                .await
                .map_err(|e| e.to_string())?;
        }
        let backend = self
            .tiers
            .get(&t.backend)
            .ok_or_else(|| format!("tier {:?} 未配置", t.backend))?;
        let bytes = backend.get(&t.key).await?;
        if bytes.len() as u64 != t.size {
            return Err(format!("恢复数据长度 {} ≠ {}", bytes.len(), t.size));
        }
        let sha256 = hex::encode(sha2::Sha256::digest(&bytes));
        if sha256 != t.sha256 {
            return Err("恢复数据 SHA-256 不符".to_string());
        }
        self.store
            .restore_prepare(&p.bucket, &p.key, &p.version_id, &bytes)
            .await
            .map_err(|e| e.to_string())?;
        let expires = crate::db::now() + i64::from(p.days) * 86_400;
        self.db
            .set_restore_state(&p.bucket, &p.key, &p.version_id, "restored", expires)
            .await
            .map_err(|e| e.to_string())?;
        if let Err(e) = self
            .store
            .restore_commit(&p.bucket, &p.key, &p.version_id, expires)
            .await
        {
            let _ = self
                .db
                .set_restore_state(&p.bucket, &p.key, &p.version_id, "pending", 0)
                .await;
            return Err(e.to_string());
        }
        self.emit(
            S3EventOp::RestoreCompleted,
            &p.bucket,
            &p.key,
            Some(&p.version_id),
            Some(v.size),
            Some(&v.etag),
        );
        Ok("restored".into())
    }
}

#[async_trait::async_trait]
impl TaskExecutor for M8TaskExecutor {
    async fn execute(&self, action: &str, payload: &str) -> Result<String, String> {
        match action {
            "Transition" => {
                let p: TransitionPayload =
                    serde_json::from_str(payload).map_err(|e| format!("payload 非法: {e}"))?;
                self.transition(&p).await
            }
            "RestoreObject" => {
                let p: RestorePayload =
                    serde_json::from_str(payload).map_err(|e| format!("payload 非法: {e}"))?;
                match self.restore(&p).await {
                    Err(e) => {
                        // 失败回 Archived：重试从干净状态开始；重试耗尽也不会卡在 Restoring。
                        let _ = self
                            .store
                            .abort_restore(&p.bucket, &p.key, &p.version_id)
                            .await;
                        Err(e)
                    }
                    ok => ok,
                }
            }
            other => Err(format!("未注册的作业动作: {other}")),
        }
    }
}

pub use crate::tier_sweep::{reap_once, reconcile_restores, spawn, sweep_transitions};
