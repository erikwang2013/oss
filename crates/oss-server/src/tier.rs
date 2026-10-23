//! M8 S4b：远端 tier 后端注册表 + 删除回收。
//!
//! 远端 key 口径：`<tier_id>/<bucket>/<version_id>/<raw key>`，raw key 不预编码；
//! 编码只发生在 [`ReplClient`] 的 canonical path（N3）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt as _;
use md5::Digest as _;

use crate::config::{TierKind, TierTarget};
use crate::local_tier::LocalTierBackend;
use crate::repl_client::ReplClient;

/// tier 搬运流：块大小有界（[`crate::tier_jobs::TIER_CHUNK`]），不得整份驻留内存。
pub type TierByteStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, String>> + Send + 'static>>;

/// 上传结果：远端 ETag + 流上增量计算的摘要（恢复校验口径）。
#[derive(Debug, Clone)]
pub struct TierPutResult {
    /// 远端 PUT 响应 ETag；本地 tier = 内容 MD5。S3 实现断言其等于 `md5`。
    pub etag: String,
    pub md5: String,
    pub sha256: String,
}

/// 上传暂存余量：spool/restore staging 前要求磁盘至少多出这么多空闲字节。
pub const STAGING_FREE_RESERVE: u64 = 64 << 20;

/// 免费空间检查（fail closed）：`f_bavail × f_frsize ≥ need + reserve`。
pub fn ensure_free_space(dir: &Path, need: u64, reserve: u64) -> Result<(), String> {
    let vfs = rustix::fs::statvfs(dir)
        .map_err(|e| format!("读取 {:?} 剩余空间失败：{e}", dir.display()))?;
    let avail = vfs.f_bavail.saturating_mul(vfs.f_frsize);
    let want = need.saturating_add(reserve);
    if avail < want {
        return Err(format!(
            "磁盘剩余空间不足（{:?}：可用 {avail} 字节 < 需要 {want}）",
            dir.display()
        ));
    }
    Ok(())
}

/// 暂存文件守卫：任何错误/取消路径（含 future drop）都会删除暂存文件。
pub struct StageGuard {
    path: Option<PathBuf>,
}

impl StageGuard {
    pub fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    /// 暂存文件已被 rename 到最终位置：不再尝试删除。
    pub fn disarm(mut self) {
        self.path = None;
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        if let Some(p) = self.path.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// GC 列表项（远端 key、最后修改秒、字节数）。
#[derive(Debug, Clone)]
pub struct TierObject {
    pub key: String,
    pub last_modified: i64,
    pub size: u64,
}

/// 单个 tier 的远端对象操作。`key` = 完整远端 key（含 tier_id 前缀，未编码）。
#[async_trait::async_trait]
pub trait TierBackend: Send + Sync {
    fn id(&self) -> &str;
    /// 流式上传：`size` 为字节数（实现须校验流长度恰好等于它）；返回远端 ETag
    /// 与流上增量计算的 MD5/SHA-256（下载后恢复校验用）。
    async fn put(
        &self,
        key: &str,
        body: TierByteStream,
        size: u64,
    ) -> Result<TierPutResult, String>;
    /// 流式下载：调用方负责按元数据校验长度与 SHA-256。
    async fn get(&self, key: &str) -> Result<TierByteStream, String>;
    /// 远端大小；键不存在 → None。
    async fn head(&self, key: &str) -> Result<Option<u64>, String>;
    async fn delete(&self, key: &str) -> Result<(), String>;
    /// 列出该 tier 前缀下的全部对象（GC 对账用）；不支持的实现返回 Err。
    async fn list(&self) -> Result<Vec<TierObject>, String> {
        Err("tier 后端不支持 list".to_string())
    }
}

/// 远端 key 构造（N3：raw key 原样拼接）。
pub fn tier_key(tier_id: &str, bucket: &str, version_id: &str, key: &str) -> String {
    format!("{tier_id}/{bucket}/{version_id}/{key}")
}

/// S3 兼容远端（经 [`ReplClient`]；http + SigV4）。
pub struct S3TierBackend {
    id: String,
    bucket: String,
    client: Arc<ReplClient>,
    /// SigV4 需要预计算的 payload hash：先落暂存文件，再流式发送。
    staging_dir: PathBuf,
}

impl S3TierBackend {
    pub fn new(target: &TierTarget, client: Arc<ReplClient>) -> Self {
        Self {
            id: target.id.clone(),
            bucket: target.bucket.clone(),
            client,
            staging_dir: std::env::temp_dir(),
        }
    }

    /// 进程配置的暂存目录（`staging_dir`）；None 保持系统临时目录。
    pub fn with_staging_dir(mut self, dir: Option<PathBuf>) -> Self {
        if let Some(dir) = dir {
            self.staging_dir = dir;
        }
        self
    }

    /// 流式读到暂存文件并增量算 MD5/SHA-256（S3 SigV4 必须预知 payload hash）。
    async fn spool(
        &self,
        mut body: TierByteStream,
        size: u64,
    ) -> Result<(StageGuard, PathBuf, String, String), String> {
        tokio::fs::create_dir_all(&self.staging_dir)
            .await
            .map_err(|e| format!("tier 暂存目录不可用：{e}"))?;
        ensure_free_space(&self.staging_dir, size, STAGING_FREE_RESERVE)?;
        let path = self
            .staging_dir
            .join(format!("tier-{}.stage", uuid::Uuid::new_v4()));
        let guard = StageGuard::new(path.clone());
        let mut f = tokio::fs::File::create(&path)
            .await
            .map_err(|e| format!("tier 暂存文件创建失败：{e}"))?;
        let mut md5 = md5::Md5::new();
        let mut sha = sha2::Sha256::new();
        let mut n: u64 = 0;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            n += chunk.len() as u64;
            if n > size {
                return Err(format!("tier 上传流超过声明长度 {size} 字节"));
            }
            md5.update(&chunk);
            sha.update(&chunk);
            tokio::io::AsyncWriteExt::write_all(&mut f, &chunk)
                .await
                .map_err(|e| format!("tier 暂存写入失败：{e}"))?;
        }
        tokio::io::AsyncWriteExt::flush(&mut f)
            .await
            .map_err(|e| format!("tier 暂存写入失败：{e}"))?;
        drop(f);
        if n != size {
            return Err(format!("tier 上传流长度 {n} ≠ 声明 {size} 字节"));
        }
        Ok((
            guard,
            path,
            hex::encode(md5.finalize()),
            hex::encode(sha.finalize()),
        ))
    }
}

#[async_trait::async_trait]
impl TierBackend for S3TierBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn put(
        &self,
        key: &str,
        body: TierByteStream,
        size: u64,
    ) -> Result<TierPutResult, String> {
        let (_guard, path, md5, sha256) = self.spool(body, size).await?;
        // 流式发送暂存文件：Content-Length 与 x-amz-content-sha256 均为预计算值。
        let etag = self
            .client
            .tier_put_file(&self.id, &self.bucket, key, &path, size, &sha256, &md5)
            .await
            .map_err(|e| e.message)?;
        Ok(TierPutResult { etag, md5, sha256 })
    }

    async fn get(&self, key: &str) -> Result<TierByteStream, String> {
        self.client
            .tier_get_stream(&self.id, &self.bucket, key)
            .await
            .map_err(|e| e.message)
    }

    async fn head(&self, key: &str) -> Result<Option<u64>, String> {
        self.client
            .tier_head(&self.id, &self.bucket, key)
            .await
            .map_err(|e| e.message)
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.client
            .tier_delete(&self.id, &self.bucket, key)
            .await
            .map_err(|e| e.message)
    }

    async fn list(&self) -> Result<Vec<TierObject>, String> {
        self.client
            .tier_list(&self.id, &self.bucket, &format!("{}/", self.id))
            .await
            .map_err(|e| e.message)
    }
}

/// tier id → 后端（启动期固定；规则校验只认这里的 id）。
#[derive(Clone, Default)]
pub struct TierBackends {
    map: HashMap<String, Arc<dyn TierBackend>>,
    /// 控制台只读状态元数据；只投影 id/endpoint/bucket，绝不带凭证。
    targets: Vec<TierTarget>,
}

impl TierBackends {
    pub fn new(backends: Vec<Arc<dyn TierBackend>>) -> Self {
        Self {
            map: backends
                .into_iter()
                .map(|b| (b.id().to_string(), b))
                .collect(),
            targets: Vec::new(),
        }
    }

    pub fn from_config(tiers: &[TierTarget]) -> Self {
        Self::from_config_tls(tiers, None)
    }

    /// 带进程 TLS 物料（自定义 CA/mTLS 身份；None = 平台根 + 明文端点不变）。
    pub fn from_config_tls(tiers: &[TierTarget], tls: Option<Arc<crate::tls::Tls>>) -> Self {
        Self::from_config_tls_staging(tiers, tls, None)
    }

    /// 同上，另指定 S3 上传暂存目录（None = 系统临时目录）。
    pub fn from_config_tls_staging(
        tiers: &[TierTarget],
        tls: Option<Arc<crate::tls::Tls>>,
        staging_dir: Option<PathBuf>,
    ) -> Self {
        let s3: Vec<TierTarget> = tiers
            .iter()
            .filter(|t| t.kind == TierKind::S3)
            .cloned()
            .collect();
        let mut client = ReplClient::new_tiers(&s3);
        if let Some(t) = tls {
            client = client.with_tls(t);
        }
        let client = Arc::new(client);
        let map = tiers
            .iter()
            .filter_map(|t| match t.kind {
                TierKind::S3 => Some((
                    t.id.clone(),
                    Arc::new(
                        S3TierBackend::new(t, Arc::clone(&client))
                            .with_staging_dir(staging_dir.clone()),
                    ) as Arc<dyn TierBackend>,
                )),
                TierKind::Local => match t.path.clone() {
                    Some(path) => Some((
                        t.id.clone(),
                        Arc::new(LocalTierBackend::new(t.id.clone(), path)) as Arc<dyn TierBackend>,
                    )),
                    None => {
                        tracing::warn!(id = %t.id, "local tier 缺 path，已跳过");
                        None
                    }
                },
            })
            .collect();
        Self {
            map,
            targets: tiers.to_vec(),
        }
    }

    /// 配置的 tier 目标（顺序同 `[[tiers]]`；调用方只可读出展示字段）。
    pub fn targets(&self) -> &[TierTarget] {
        &self.targets
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn TierBackend>> {
        self.map.get(id).cloned()
    }

    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.map.keys().cloned().collect();
        ids.sort();
        ids
    }
}

/// `TierGcSink` 的真实实现：按 ref.backend 查后端并 DELETE（best-effort，逐条尝试）。
pub struct S3TierGc {
    backends: Arc<TierBackends>,
}

impl S3TierGc {
    pub fn new(backends: Arc<TierBackends>) -> Self {
        Self { backends }
    }
}

#[async_trait::async_trait]
impl xl_s3::tier_gc::TierGcSink for S3TierGc {
    async fn delete(&self, refs: &[xl_store::store::TierRef]) -> Result<(), String> {
        let mut errors = Vec::new();
        for r in refs {
            let Some(b) = self.backends.get(&r.backend) else {
                tracing::warn!(backend = %r.backend, key = %r.key, "tier 已不在配置中，跳过远端删除");
                continue;
            };
            if let Err(e) = b.delete(&r.key).await {
                tracing::warn!(backend = %r.backend, key = %r.key, error = %e, "tier 远端 DELETE 失败");
                errors.push(format!("{}: {e}", r.backend));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}
