//! M8 S4b：Transition / Restore 作业执行、sweep、reaper 与启动对账。
//!
//! 恢复顺序（C7）：prepare 本地数据（不可见）→ 行 restored/expiry → meta Restored。
//! 任一步崩溃由 [`reconcile_restores`]（启动）与 [`reap_once`] 按 Version 为准修正。

use std::sync::{Arc, Mutex};

use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use tokio::io::AsyncWriteExt as _;
use xl_s3::events::{EventSink, EventSource, S3EventOp};
use xl_s3::restore::RestoreSink;
use xl_store::meta::{TierChunks, TierPhase, is_archive_class};
use xl_store::store::Store;

use crate::db::{Db, JobSpec, NewJobTask};
use crate::job_state::JobState;
use crate::jobs::TaskExecutor;
use crate::raft::{FencingEpoch, LeaderGate};
use crate::tier::{StageGuard, TierBackends, TierByteStream, reserve_free_space, tier_key};

/// 过渡/恢复的搬运窗口：内存驻留上限 = 一个窗口（+ 后端自己的 256 KiB 块）。
pub const TIER_CHUNK: u64 = 8 << 20;

/// 恢复暂存默认余量：宁可拒绝也不把数据盘写满。
const RESTORE_FREE_RESERVE: u64 = 64 << 20;

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

/// Restore 入队：作业行与 `tier_restores`(pending) 行同批走 `JobState`（feature 4 经复制
/// 日志按主键 upsert；非复制模式仍为本地直写）。
pub struct SqlRestoreSink {
    db: Arc<Db>,
    jobs: Arc<JobState>,
    gate: LeaderGate,
}

impl SqlRestoreSink {
    pub fn new(db: Arc<Db>, jobs: Arc<JobState>, gate: LeaderGate) -> Self {
        Self { db, jobs, gate }
    }

    /// 入队时刻的 token：leader 用当前 epoch；follower/M7 用 UNFENCED（本地行仍可写）。
    fn fence(&self) -> FencingEpoch {
        self.gate.epoch().unwrap_or(FencingEpoch::UNFENCED)
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
        self.jobs
            .create_restore(
                &self.db,
                &spec,
                bucket,
                key,
                version_id,
                crate::db::now(),
                self.fence(),
            )
            .await
    }

    async fn forget_restore(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(), String> {
        self.jobs
            .tier_restore_delete(&self.db, bucket, key, version_id, self.fence())
            .await
    }
}

/// 按 8 MiB 窗口读取指定版本，作为 tier 上传流（内存不随对象大小增长）。
/// 每个窗口的 SHA-256 记入返回的共享表——上传流与直读分块表来自同一份字节。
fn version_stream(
    store: Arc<Store>,
    bucket: String,
    key: String,
    version_id: String,
    size: u64,
) -> (TierByteStream, Arc<Mutex<Vec<String>>>) {
    let hashes = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&hashes);
    let stream = futures::stream::unfold(0u64, move |offset| {
        let store = Arc::clone(&store);
        let sink = Arc::clone(&sink);
        let (bucket, key, version_id) = (bucket.clone(), key.clone(), version_id.clone());
        async move {
            if offset >= size {
                return None;
            }
            let end = (offset + TIER_CHUNK).min(size);
            match store
                .get_object_range_version_ext(&bucket, &key, &version_id, offset..end, None)
                .await
            {
                Ok(chunk) if chunk.len() as u64 == end - offset => {
                    sink.lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(hex::encode(sha2::Sha256::digest(&chunk)));
                    Some((Ok(chunk), end))
                }
                Ok(chunk) => Some((
                    Err(format!(
                        "源版本窗口读取长度 {} ≠ {} 字节",
                        chunk.len(),
                        end - offset
                    )),
                    size,
                )),
                Err(e) => Some((Err(format!("读取源版本失败：{e}")), size)),
            }
        }
    });
    (Box::pin(stream), hashes)
}

/// 按与 Transition 相同的 8 MiB canonical 边界累计 SHA-256（旧对象 Restore 时 backfill）。
struct ChunkHasher {
    h: sha2::Sha256,
    filled: u64,
    out: Vec<String>,
}

impl ChunkHasher {
    fn new() -> Self {
        Self {
            h: sha2::Sha256::new(),
            filled: 0,
            out: Vec::new(),
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let take = ((TIER_CHUNK - self.filled) as usize).min(data.len());
            self.h.update(&data[..take]);
            self.filled += take as u64;
            data = &data[take..];
            if self.filled == TIER_CHUNK {
                self.flush();
            }
        }
    }

    fn flush(&mut self) {
        let h = std::mem::replace(&mut self.h, sha2::Sha256::new());
        self.out.push(hex::encode(h.finalize()));
        self.filled = 0;
    }

    fn finish(mut self) -> TierChunks {
        if self.filled > 0 {
            self.flush();
        }
        TierChunks {
            chunk_size: TIER_CHUNK,
            sha256: self.out,
        }
    }
}

/// 作业执行器：`Transition` / `RestoreObject`。
pub struct M8TaskExecutor {
    db: Arc<Db>,
    store: Arc<Store>,
    tiers: Arc<TierBackends>,
    events: Arc<crate::events::SqlEventSink>,
    /// Restore 后续状态写入门面；测试/单机默认本地直写。
    jobs: Arc<JobState>,
    max_tier_bytes: u64,
    /// 恢复暂存所需空闲余量（测试可放大以覆盖 fail-closed 分支）。
    restore_free_reserve: u64,
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
            jobs: Arc::new(JobState::local()),
            max_tier_bytes,
            restore_free_reserve: RESTORE_FREE_RESERVE,
        }
    }

    /// 生产路径：Restore 后续状态经共享 [`JobState`]（feature 7 进复制日志）。
    pub fn with_jobs(mut self, jobs: Arc<JobState>) -> Self {
        self.jobs = jobs;
        self
    }

    /// 测试/运维：覆盖单对象上限（默认 `OSS_TIER_MAX_BYTES` 或 1 GiB）。
    pub fn with_max_tier_bytes(mut self, max: u64) -> Self {
        self.max_tier_bytes = max;
        self
    }

    /// 测试/运维：覆盖恢复暂存空闲余量（默认 64 MiB）。
    pub fn with_restore_free_reserve(mut self, reserve: u64) -> Self {
        self.restore_free_reserve = reserve;
        self
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
        // SSE 对象不参与任何 Transition（sweep 已跳过；手工/批量入队在此拒绝）。
        if v.encryption.is_some() {
            return Err("SSE 对象不参与 Transition".to_string());
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
            // 先按元数据拒绝超限/加密对象，再流式读取（避免为拒绝而搬运）。
            if v.size > self.max_tier_bytes {
                return Err(format!(
                    "对象 {} 字节超过 OSS_TIER_MAX_BYTES={}",
                    v.size, self.max_tier_bytes
                ));
            }
            let key = tier_key(tier_id, &p.bucket, &p.version_id, &p.key);
            let (body, hashes) = version_stream(
                Arc::clone(&self.store),
                p.bucket.clone(),
                p.key.clone(),
                p.version_id.clone(),
                v.size,
            );
            // 后端在流上增量算 MD5/SHA-256；S3 单 PUT 响应 ETag 仍断言等于 MD5。
            let put = backend.put(&key, body, v.size).await?;
            // 归档副本必须绑定源已提交的整对象摘要（put 已算好，零额外 IO）。
            // legacy parts=Some / 空 part.sha256 跳过；MPU 组合 ETag 只比 SHA-256，不比 MD5。
            if v.parts.is_none() && !v.part.sha256.is_empty() {
                if put.sha256 != v.part.sha256 {
                    return Err(format!(
                        "源版本 SHA-256 与归档流不符：{} ≠ {}",
                        v.part.sha256, put.sha256
                    ));
                }
                let md5_etag = v.etag.len() == 32
                    && v.etag
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
                if md5_etag && put.md5 != v.etag {
                    return Err(format!(
                        "源 ETag(MD5) 与归档流不符：{} ≠ {}",
                        v.etag, put.md5
                    ));
                }
            }
            let chunks = TierChunks {
                chunk_size: TIER_CHUNK,
                sha256: hashes.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            };
            chunks
                .validate(v.size)
                .map_err(|e| format!("Transition 分块哈希表不自洽：{e}"))?;
            self.store
                .archive_version(
                    &p.bucket,
                    &p.key,
                    &p.version_id,
                    xl_store::store::ArchiveSpec {
                        backend: tier_id.to_string(),
                        key,
                        size: v.size,
                        source_etag: v.etag.clone(),
                        etag: put.etag,
                        sha256: put.sha256,
                        class: p.class.clone(),
                        chunks: Some(chunks),
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
                Some(v.size),
                Some(&v.etag),
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

    async fn restore(&self, p: &RestorePayload, fence: FencingEpoch) -> Result<String, String> {
        let Some(v) = self
            .store
            .tier_version(&p.bucket, &p.key, &p.version_id)
            .await
            .map_err(|e| e.to_string())?
        else {
            let _ = self
                .jobs
                .tier_restore_delete(&self.db, &p.bucket, &p.key, &p.version_id, fence)
                .await;
            return Ok("missing".into());
        };
        let Some(t) = v.tier else {
            let _ = self
                .jobs
                .tier_restore_delete(&self.db, &p.bucket, &p.key, &p.version_id, fence)
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
        // 元数据先挡一次，GET 回来再挡一次（后端可能撒谎/配置漂移）。
        if t.size > self.max_tier_bytes {
            return Err(format!(
                "恢复对象 {} 字节超过 OSS_TIER_MAX_BYTES={}",
                t.size, self.max_tier_bytes
            ));
        }
        // 恢复下载：先查空间，再边写暂存文件边算 SHA-256；失败/取消由 guard 清理。
        let stage = self
            .store
            .restore_stage_path(&p.bucket, &p.key, &v.part.uuid)
            .map_err(|e| e.to_string())?;
        let dir = stage
            .parent()
            .ok_or_else(|| "恢复暂存路径无父目录".to_string())?;
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| format!("恢复暂存目录创建失败：{e}"))?;
        // 预留登记与检查同锁：并发恢复不会都通过检查后一起把数据盘写满。
        let space = reserve_free_space(dir, t.size, self.restore_free_reserve)?;
        let guard = StageGuard::new(stage.clone()).with_reservation(space);
        let mut body = backend.get(&t.key).await?;
        let mut f = crate::tier::create_private_stage_file(&stage)
            .await
            .map_err(|e| format!("恢复暂存文件创建失败：{e}"))?;
        let mut hasher = sha2::Sha256::new();
        let mut chunker = ChunkHasher::new();
        let mut n: u64 = 0;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            n += chunk.len() as u64;
            // 元数据尺寸是信任边界：远端/后端撒谎时立刻停，不写满磁盘。
            if n > self.max_tier_bytes {
                return Err(format!(
                    "恢复数据 {n} 字节超过 OSS_TIER_MAX_BYTES={}",
                    self.max_tier_bytes
                ));
            }
            if n > t.size {
                return Err(format!("恢复数据长度超过元数据 {} 字节", t.size));
            }
            hasher.update(&chunk);
            chunker.update(&chunk);
            f.write_all(&chunk)
                .await
                .map_err(|e| format!("恢复暂存写入失败：{e}"))?;
        }
        f.flush()
            .await
            .map_err(|e| format!("恢复暂存写入失败：{e}"))?;
        drop(f);
        if n != t.size {
            return Err(format!("恢复数据长度 {n} ≠ {}", t.size));
        }
        let sha256 = hex::encode(hasher.finalize());
        if sha256 != t.sha256 {
            return Err("恢复数据 SHA-256 不符".to_string());
        }
        // 旧对象无分块表：本次恢复的字节即 Canonical 8 MiB 表，一并 backfill（直读依赖）。
        let chunks = chunker.finish();
        chunks
            .validate(t.size)
            .map_err(|e| format!("恢复分块哈希表不自洽：{e}"))?;
        self.store
            .restore_prepare_file(&p.bucket, &p.key, &p.version_id, &stage)
            .await
            .map_err(|e| e.to_string())?;
        guard.disarm();
        let expires = crate::db::now() + i64::from(p.days) * 86_400;
        self.jobs
            .tier_restore_set_state(
                &self.db,
                &p.bucket,
                &p.key,
                &p.version_id,
                "restored",
                expires,
                fence,
            )
            .await?;
        if let Err(e) = self
            .store
            .restore_commit(&p.bucket, &p.key, &p.version_id, expires, Some(chunks))
            .await
        {
            let _ = self
                .jobs
                .tier_restore_set_state(
                    &self.db,
                    &p.bucket,
                    &p.key,
                    &p.version_id,
                    "pending",
                    0,
                    fence,
                )
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
                match self.restore(&p, FencingEpoch::UNFENCED).await {
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

    /// M8-1：`RestoreObject` 的状态转换带 worker 的 fencing token；`Transition` 只写
    /// 对象元数据（版本级、可重放），无单例行可写。
    async fn execute_fenced(
        &self,
        action: &str,
        payload: &str,
        fence: FencingEpoch,
    ) -> Result<String, String> {
        if action != "RestoreObject" {
            return self.execute(action, payload).await;
        }
        let p: RestorePayload =
            serde_json::from_str(payload).map_err(|e| format!("payload 非法: {e}"))?;
        match self.restore(&p, fence).await {
            Err(e) => {
                let _ = self
                    .store
                    .abort_restore(&p.bucket, &p.key, &p.version_id)
                    .await;
                Err(e)
            }
            ok => ok,
        }
    }
}

pub use crate::tier_sweep::{
    reap_once, reconcile_restores, spawn, sweep_transitions, sweep_transitions_at,
};
