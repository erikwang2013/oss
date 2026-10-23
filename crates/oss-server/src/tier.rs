//! M8 S4b：远端 tier 后端注册表 + 删除回收。
//!
//! 远端 key 口径：`<tier_id>/<bucket>/<version_id>/<raw key>`，raw key 不预编码；
//! 编码只发生在 [`ReplClient`] 的 canonical path（N3）。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

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

/// 进程内磁盘预留：key = 文件系统设备号，value = 已登记字节数。
///
/// Restore/Transition 先登记再写盘；否则并发作业各自 statvfs 通过后会一起写爆。
/// 只覆盖本进程（同一数据目录不允许多进程写，与对象锁口径一致）。
static SPACE_RESERVATIONS: LazyLock<Mutex<HashMap<u64, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// 创建 0600 的 tier 暂存文件（tier-*.stage / restore-stage）。
///
/// 暂存文件在落盘完成前是明文：权限必须从创建瞬间就是 0600，不能依赖 umask。
pub(crate) async fn create_private_stage_file(
    path: &Path,
) -> std::io::Result<tokio::fs::File> {
    let mut opts = tokio::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true).mode(0o600);
    opts.open(path).await
}

/// 预留登记；Drop 即释放（成功、失败、future 取消统一走这里）。
pub struct SpaceReservation {
    dev: u64,
    bytes: u64,
}

impl Drop for SpaceReservation {
    fn drop(&mut self) {
        let mut map = SPACE_RESERVATIONS.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(held) = map.get_mut(&self.dev) {
            *held = held.saturating_sub(self.bytes);
            if *held == 0 {
                map.remove(&self.dev);
            }
        }
    }
}

fn device_id(dir: &Path) -> Result<u64, String> {
    use std::os::unix::fs::MetadataExt as _;
    std::fs::metadata(dir)
        .map(|m| m.dev())
        .map_err(|e| format!("读取 {:?} 设备号失败：{e}", dir.display()))
}

/// 预留判定（纯函数）：`held` 为同设备已登记量，成功后累加 `need`。
/// 仍留 `reserve` 空闲余量；`held` 偏大时按已登记量保守拒绝（宁可 fail closed）。
fn try_reserve(held: &mut u64, avail: u64, need: u64, reserve: u64) -> Result<(), String> {
    let want = held.saturating_add(need).saturating_add(reserve);
    if avail < want {
        return Err(format!(
            "磁盘剩余空间不足（可用 {avail} 字节 < 需要 {want}）"
        ));
    }
    *held = held.saturating_add(need);
    Ok(())
}

/// 在 `dir` 所在文件系统登记 `need` 字节；检查与登记在同一把锁内，并发不会超卖。
pub fn reserve_free_space(dir: &Path, need: u64, reserve: u64) -> Result<SpaceReservation, String> {
    let dev = device_id(dir)?;
    let vfs = rustix::fs::statvfs(dir)
        .map_err(|e| format!("读取 {:?} 剩余空间失败：{e}", dir.display()))?;
    let avail = vfs.f_bavail.saturating_mul(vfs.f_frsize);
    let mut map = SPACE_RESERVATIONS.lock().unwrap_or_else(|e| e.into_inner());
    let mut held = map.get(&dev).copied().unwrap_or(0);
    try_reserve(&mut held, avail, need, reserve)
        .map_err(|e| format!("{e}（{}）", dir.display()))?;
    map.insert(dev, held);
    Ok(SpaceReservation { dev, bytes: need })
}

/// 进程内活跃暂存文件：正在写的 spool 登记在此，sweeper 一律跳过。
/// （age 阈值之外的第二道保险；扫描持锁，避免「检查后登记」竞态。）
static ACTIVE_STAGES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// 崩溃残留 S3 spool 暂存文件（`tier-*.stage`）的默认保留时长 / 清理周期。
pub const STAGE_SWEEP_AGE_SECS: u64 = 3600;
pub const STAGE_SWEEP_INTERVAL_SECS: u64 = 3600;
/// 安全下界：age 太小会与共享暂存盘的另一进程 spool 打架；周期太小纯耗 IO。
const STAGE_SWEEP_MIN_AGE_SECS: u64 = 300;
const STAGE_SWEEP_MAX_AGE_SECS: u64 = 7 * 86_400;
const STAGE_SWEEP_MIN_INTERVAL_SECS: u64 = 60;

/// 清理 `dir` 下崩溃残留的 `tier-*.stage`（非递归）：只删 mtime 年龄
/// ≥ `min_age_secs`、不在活跃登记里、且是普通文件的条目；链接/目录不碰。
/// 返回删除数。目录不存在 = 0。
pub fn sweep_stale_stages(dir: &Path, min_age_secs: u64) -> Result<usize, String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(format!("tier 暂存目录不可读（{}）：{e}", dir.display())),
    };
    let active = ACTIVE_STAGES.lock().unwrap_or_else(|e| e.into_inner());
    let now = std::time::SystemTime::now();
    let mut removed = 0usize;
    for ent in entries {
        let Ok(ent) = ent else { continue };
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("tier-") || !name.ends_with(".stage") {
            continue;
        }
        let path = ent.path();
        if active.contains(&path) {
            continue;
        }
        // DirEntry::metadata 不跟随符号链接（lstat 语义）。
        let Ok(md) = ent.metadata() else { continue };
        if !md.is_file() {
            continue;
        }
        let old = md
            .modified()
            .ok()
            .and_then(|m| now.duration_since(m).ok())
            .is_some_and(|age| age.as_secs() >= min_age_secs);
        if !old {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => removed += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "tier 暂存文件清理失败")
            }
        }
    }
    Ok(removed)
}

fn env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn sweep_stages_once(dir: &Path, min_age_secs: u64) {
    let d = dir.to_path_buf();
    let r = tokio::task::spawn_blocking(move || sweep_stale_stages(&d, min_age_secs)).await;
    match r {
        Ok(Ok(n)) if n > 0 => tracing::info!(n, "tier 暂存清理：删除陈旧 stage 文件"),
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "tier 暂存清理失败"),
        Err(e) => tracing::warn!(error = %e, "tier 暂存清理任务失败"),
    }
}

/// 启动即扫一轮，之后按 `OSS_TIER_STAGE_SWEEP_INTERVAL_SECS`（默认 3600，0 = 只扫启动轮）
/// 周期清理；年龄 `OSS_TIER_STAGE_SWEEP_AGE_SECS` 默认 3600，夹在 300..=7d。
pub fn spawn_stage_sweeper(dir: PathBuf) {
    let age = env_secs("OSS_TIER_STAGE_SWEEP_AGE_SECS", STAGE_SWEEP_AGE_SECS)
        .clamp(STAGE_SWEEP_MIN_AGE_SECS, STAGE_SWEEP_MAX_AGE_SECS);
    let interval = env_secs(
        "OSS_TIER_STAGE_SWEEP_INTERVAL_SECS",
        STAGE_SWEEP_INTERVAL_SECS,
    );
    tokio::spawn(async move {
        sweep_stages_once(&dir, age).await;
        if interval == 0 {
            tracing::info!("tier 暂存清理：仅启动轮（OSS_TIER_STAGE_SWEEP_INTERVAL_SECS=0）");
            return;
        }
        let mut t = tokio::time::interval(std::time::Duration::from_secs(
            interval.max(STAGE_SWEEP_MIN_INTERVAL_SECS),
        ));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        t.tick().await; // 消耗立即触发的首 tick：启动轮已经扫过
        loop {
            t.tick().await;
            sweep_stages_once(&dir, age).await;
        }
    });
}

/// 暂存文件守卫：任何错误/取消路径（含 future drop）都会删除暂存文件；
/// 附带磁盘预留（若登记过）也在同一生命周期内释放。
pub struct StageGuard {
    path: Option<PathBuf>,
    // 只为 Drop 持有；显式下划线避免「字段未读」告警。
    _space: Option<SpaceReservation>,
}

impl StageGuard {
    pub fn new(path: PathBuf) -> Self {
        ACTIVE_STAGES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(path.clone());
        Self {
            path: Some(path),
            _space: None,
        }
    }

    /// 把在写盘前登记的预留交给守卫：成功/失败/取消都由守卫统一释放。
    pub fn with_reservation(mut self, space: SpaceReservation) -> Self {
        self._space = Some(space);
        self
    }

    /// 暂存文件已被 rename 到最终位置：不再尝试删除（预留照常释放）。
    pub fn disarm(mut self) {
        if let Some(p) = self.path.take() {
            ACTIVE_STAGES
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&p);
        }
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        if let Some(p) = self.path.take() {
            let _ = std::fs::remove_file(&p);
            ACTIVE_STAGES
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&p);
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
        // 先登记预留再落盘：并发 spool 不会都通过 statvfs 后一起写爆。
        let space = reserve_free_space(&self.staging_dir, size, STAGING_FREE_RESERVE)?;
        let path = self
            .staging_dir
            .join(format!("tier-{}.stage", uuid::Uuid::new_v4()));
        let guard = StageGuard::new(path.clone()).with_reservation(space);
        let mut f = create_private_stage_file(&path)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn age_file(path: &Path, secs: u64) {
        let f = std::fs::File::options().write(true).open(path).unwrap();
        f.set_modified(SystemTime::now() - Duration::from_secs(secs))
            .unwrap();
    }

    /// 并发超卖边界：已登记量计入检查，失败不改账；饱和运算不 panic。
    #[test]
    fn reservation_refuses_overcommit_and_counts_held() {
        let mut held = 0u64;
        try_reserve(&mut held, 1000, 400, 100).unwrap();
        assert_eq!(held, 400);
        let err = try_reserve(&mut held, 1000, 501, 100).unwrap_err();
        assert!(err.contains("磁盘剩余空间不足"), "{err}");
        assert_eq!(held, 400, "失败不得改变已登记量");
        // 恰好用满（400+500+100 = 1000）仍允许。
        try_reserve(&mut held, 1000, 500, 100).unwrap();
        assert_eq!(held, 900);

        // need+reserve 饱和到 u64::MAX：可用 MAX-1 时必须拒绝，且不改账。
        let mut h = 0u64;
        assert!(try_reserve(&mut h, u64::MAX - 1, u64::MAX, 1).is_err());
        assert_eq!(h, 0);
    }

    /// 预留随 Drop 释放（成功/失败/取消同一路径），不泄漏设备账。
    /// 合成设备号避开并行单测的真实设备（local_tier put 也会登记）。
    #[test]
    fn reservation_released_on_drop() {
        let dev = u64::MAX;
        SPACE_RESERVATIONS.lock().unwrap().insert(dev, 123);
        drop(SpaceReservation { dev, bytes: 123 });
        assert!(!SPACE_RESERVATIONS.lock().unwrap().contains_key(&dev));
        // 超量释放饱和到 0，不 panic、不留账。
        SPACE_RESERVATIONS.lock().unwrap().insert(dev, 5);
        drop(SpaceReservation { dev, bytes: 9 });
        assert!(!SPACE_RESERVATIONS.lock().unwrap().contains_key(&dev));
    }

    /// 暂存文件从创建瞬间就是 0600（明文落盘不依赖 umask）。
    #[tokio::test]
    async fn private_stage_file_is_0600() {
        use std::os::unix::fs::PermissionsExt as _;
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("tier-x.stage");
        drop(create_private_stage_file(&p).await.unwrap());
        let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "mode={mode:o}");
    }

    /// 只删「旧 + 非活跃 + 普通文件 + tier-*.stage」；新文件、活跃文件、
    /// 非匹配名、目录、符号链接一律保留。
    #[test]
    fn sweeps_only_old_inactive_stage_files() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path();
        for name in [
            "tier-old.stage",
            "tier-fresh.stage",
            "tier-active.stage",
            "other.stage",
            "tier-x.tmp",
        ] {
            std::fs::write(dir.join(name), b"x").unwrap();
        }
        age_file(&dir.join("tier-old.stage"), 7200);
        age_file(&dir.join("tier-active.stage"), 7200);
        age_file(&dir.join("other.stage"), 7200);
        age_file(&dir.join("tier-x.tmp"), 7200);
        std::fs::create_dir(dir.join("tier-dir.stage")).unwrap();
        std::os::unix::fs::symlink("/nonexistent-target", dir.join("tier-link.stage")).unwrap();

        // 活跃文件：由 StageGuard 登记（崩溃残留本身不会有 guard）。
        let guard = StageGuard::new(dir.join("tier-active.stage"));
        assert_eq!(sweep_stale_stages(dir, 3600).unwrap(), 1);
        assert!(!dir.join("tier-old.stage").exists());
        assert!(dir.join("tier-fresh.stage").exists(), "新文件不得删除");
        assert!(dir.join("tier-active.stage").exists(), "活跃文件不得删除");
        assert!(dir.join("other.stage").exists(), "非 tier-*.stage 不得删除");
        assert!(dir.join("tier-x.tmp").exists(), "非 .stage 后缀不得删除");
        assert!(dir.join("tier-dir.stage").is_dir(), "目录不得删除");
        assert!(
            std::fs::symlink_metadata(dir.join("tier-link.stage")).is_ok(),
            "符号链接不得删除/跟随"
        );

        // 摘掉活跃登记后，同一文件才可被清理（证明跳过确因活跃）。
        guard.disarm();
        assert_eq!(sweep_stale_stages(dir, 3600).unwrap(), 1);
        assert!(!dir.join("tier-active.stage").exists());
    }

    #[test]
    fn sweep_missing_dir_is_noop() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(sweep_stale_stages(&d.path().join("nope"), 0).unwrap(), 0);
    }
}
