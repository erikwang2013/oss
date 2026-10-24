//! CMK 生命周期管理（管理员/离线面）：引用扫描、re-wrap 重加密、删除隔离。
//!
//! **为什么 rewrap 必须重写数据而不是只换信封**：DARE 单元 AAD = `b"oss-m5|" ‖ key_id`，
//! 只改 meta 里的 key_id/wrapped DEK 会让旧密文解不开。故本模块按版本重读明文、用目标
//! CMK 当前版本重新加密（新 DEK/新 seed/新版本目录），再单点翻转 `xl.meta`——字节、ETag、
//! 元数据、version_id、mod_time 全保留；旧目录在提交点后进 trash。
//!
//! 安全边界（写进契约，勿静默放宽）：
//! - 引用扫描只覆盖**本机盘**（`collect_keys`）与本机/离线可见的桶默认加密配置；集群面必须
//!   逐节点执行，删除在集群下额外要求 `force`（离线确认），因本机 writer 屏障无法静默对端
//!   在途写。**这不是全局分布式扫描**：CLI 不汇总各节点结果，操作员须自行逐节点执行并核对。
//! - 桶默认加密同样算引用：删除后若新写入仍会套用该 CMK，会立刻产生不可解对象（strand）。
//!   配置引用由调用方经 [`CmkConfigRefs`] 注入（xl-store 不依赖 SQL）；隔离后复扫关闭竞态。
//! - 逐版本失败不中断批次（原数据不动，可重跑）；扫描/meta 读取错误 → 整批 fail-closed。
//! - 删除 = 引用扫描（对象版本 + 桶默认，有引用即拒）→ 原子隔离（此后新 wrap 失败）→
//!   等本机写者归零 → 复扫（竞态出现引用则恢复）→ 清理。任一步失败都不会留下
//!   「钥没了但引用还在」的静默态。删除中断的残骸用 `cmk trash list/restore/purge` 处置。
//! - 旧版本保持 decrypt-only：本模块绝不删除/改写任何 key 文件（删除走隔离销毁）。
//! - 审计只记名/版本/数量，绝不记钥字节。

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use serde::Serialize;

use crate::crypto::{self, EncSpec};
use crate::error::{Error, Result};
use crate::kms::{self, KmsFileProvider};
use crate::layout;
use crate::meta::{self, Version};

use super::{ByteStream, Store};

/// 单次 rewrap 处理上限（bounded batching；续页用 cursor）。
pub const REWRAP_MAX_BATCH: usize = 1000;
/// 引用报告里最多列出的条目数（总数另记，避免巨桶响应爆量）。
pub const REFS_REPORT_CAP: usize = 100;
/// 删除隔离后等待本机在途写者归零的上限。
const DELETE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
/// 逐版本顺序读的块大小（内存上限）。
const READ_CHUNK: u64 = 8 * 1024 * 1024;

/// 一条「某对象版本仍引用该 CMK」的记录（无秘密）。
#[derive(Debug, Clone, Serialize)]
pub struct CmkRef {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub key_id: String,
    pub size: u64,
}

/// 桶默认加密配置对 CMK 的引用（非对象版本；`setting` 为库内存储值，如
/// `aws:kms:tenant-a` 或 `aws:kms:arn:...`，不含秘密）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CmkBucketRef {
    pub bucket: String,
    pub setting: String,
}

/// 删除/扫描的**外部配置引用源**（oss-server 的 `buckets.default_encryption`）。
/// 在线控制台与离线 CLI 注入同一实现；扫描失败必须上抛（删除 guard fail-closed）。
#[async_trait::async_trait]
pub trait CmkConfigRefs: Send + Sync {
    async fn cmk_config_refs(&self, cmk: &str) -> Result<Vec<CmkBucketRef>>;
}

/// 引用扫描结果（管理员只读面）。
#[derive(Debug, Clone, Default, Serialize)]
pub struct CmkRefScan {
    pub cmk: String,
    pub references: usize,
    pub refs: Vec<CmkRef>,
    pub refs_truncated: bool,
    /// 把该 CMK 设为桶默认加密的桶（离线 CLI 用本机库副本填充；空 = 无配置引用）。
    pub bucket_defaults: Vec<CmkBucketRef>,
}

/// 删除结果。`deleted=false` 是**受控拒绝**（有引用/集群需 force/写者未归零/复扫竞态），
/// 不是内部错误；`reason` 给出机器可读原因。
#[derive(Debug, Clone, Serialize)]
pub struct CmkDeleteReport {
    pub name: String,
    pub versions: Vec<u32>,
    pub force: bool,
    pub deleted: bool,
    /// 钥材料是否已从磁盘彻底移除；false = 已隔离但残骸需人工清理（provider 已不可用）。
    pub purged: bool,
    pub references: usize,
    pub refs: Vec<CmkRef>,
    pub refs_truncated: bool,
    /// 把该 CMK 设为桶默认加密的桶（同样阻止删除——否则新写入会被 strand）。
    pub bucket_defaults: Vec<CmkBucketRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// `.cmk-trash` 清理结果。`deleted=false` 是受控拒绝（引用未清/集群需 force/无残骸）。
#[derive(Debug, Clone, Serialize)]
pub struct CmkTrashPurgeReport {
    pub name: String,
    pub purged: Vec<kms::CmkTrashEntry>,
    pub deleted: bool,
    pub references: usize,
    pub refs: Vec<CmkRef>,
    pub refs_truncated: bool,
    pub bucket_defaults: Vec<CmkBucketRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// rewrap 参数。`apply=false` = 只读预览（默认安全态）。
#[derive(Debug, Clone)]
pub struct RewrapOptions {
    pub apply: bool,
    /// 本页最多处理的候选版本数（1..=`REWRAP_MAX_BATCH`）。
    pub limit: usize,
    /// 上一页返回的 `next_cursor`（不透明串）。
    pub cursor: Option<String>,
    /// 只处理该桶（None = 本机全部桶）。
    pub bucket: Option<String>,
}

/// 单版本 rewrap 结果项。
#[derive(Debug, Clone, Serialize)]
pub struct RewrapItem {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub old_key_id: String,
    /// 预览态/失败态可能为 None。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_key_id: Option<String>,
    pub size: u64,
    /// `preview` / `rewrapped` / `skipped` / `blocked` / `failed`。
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RewrapReport {
    pub cmk: String,
    /// 开始时的当前版本（新写入目标）。
    pub current_version: u32,
    pub apply: bool,
    pub scanned_objects: usize,
    /// 本页旧版本引用数（`truncated` 时后续页仍有）。
    pub candidates: usize,
    pub rewrapped: usize,
    pub skipped: usize,
    pub blocked: usize,
    pub failed: usize,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub items: Vec<RewrapItem>,
}

#[derive(Debug, Clone)]
struct Cursor {
    bucket: String,
    key: String,
    version_id: String,
}

struct Candidate {
    r: CmkRef,
    /// 预览即可判定的阻塞原因（锁定/分层/缺 sha256）。
    block: Option<String>,
}

#[derive(Default)]
struct ScanOut {
    scanned_objects: usize,
    /// 命中总数（引用守卫口径）；rewrap 下 = 本页候选数（截断时再多算一个探测命中）。
    matched: usize,
    candidates: Vec<Candidate>,
    truncated: bool,
}

/// 记录是否（以任意形态）引用名为 `cmk` 的 CMK：显式字段、规范 key_id，或畸形 `cmk:` 前缀的
/// 名段。删除守卫取「任一命中即拒」，宁可多拦不可漏放。
fn record_mentions(rec: &meta::EncryptionRecord, cmk: &str) -> bool {
    if rec.kms_key_id.as_deref() == Some(cmk) {
        return true;
    }
    if let Some((name, _)) = kms::parse_cmk_key_id(&rec.key_id) {
        return name == cmk;
    }
    rec.key_id
        .strip_prefix("cmk:")
        .and_then(|r| r.rsplit_once(':'))
        .is_some_and(|(name, _)| name == cmk)
}

/// 预览可判定的阻塞项（apply 时会再校验一遍）。
fn block_reason(v: &Version) -> Option<String> {
    if let Some(why) = super::version::active_lock_reason(v) {
        return Some(format!("object locked: {why}"));
    }
    if v.tier.is_some() {
        return Some("tiered version: remote copy may be stale; warm it back before rewrap".into());
    }
    if v.part.sha256.is_empty() {
        return Some("version lacks plaintext sha256; cannot verify rewrap integrity".into());
    }
    None
}

fn b64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

fn encode_cursor(r: &CmkRef) -> String {
    b64().encode(format!("{}\0{}\0{}", r.bucket, r.key, r.version_id))
}

fn decode_cursor(s: &str) -> Result<Cursor> {
    let bad = || Error::InvalidArgument("cursor 非法（应为上一页返回的不透明串）".into());
    let raw = b64().decode(s).map_err(|_| bad())?;
    let text = String::from_utf8(raw).map_err(|_| bad())?;
    let mut it = text.splitn(3, '\0');
    match (it.next(), it.next(), it.next()) {
        (Some(b), Some(k), Some(v)) if !k.is_empty() && !v.is_empty() => Ok(Cursor {
            bucket: b.to_string(),
            key: k.to_string(),
            version_id: v.to_string(),
        }),
        _ => Err(bad()),
    }
}

/// 版本数据顺序流（逐 `READ_CHUNK` 读明文）：管道有界 → 大对象内存恒定；
/// 消费端失败/取消 → 接收端关闭 → 生产任务自然退出。
fn version_stream(store: Arc<Store>, bucket: String, key: String, v: Version) -> ByteStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(4);
    tokio::spawn(async move {
        let mut pos = 0u64;
        while pos < v.size {
            let end = (pos + READ_CHUNK).min(v.size);
            let item = match store
                .read_version_data(&bucket, &key, &v, Some(pos..end), None)
                .await
            {
                Ok(Some(b)) => Ok(b),
                Ok(None) => Err(Error::CorruptMeta(format!(
                    "{bucket}/{key}: 版本数据缺失（无法重加密）"
                ))),
                Err(e) => Err(e),
            };
            if tx.send(item).await.is_err() {
                return; // 消费端已放弃
            }
            pos = end;
        }
    });
    Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

impl Store {
    fn cmk_provider(&self) -> Result<Arc<KmsFileProvider>> {
        self.maybe_enc_state()
            .and_then(|e| e.kms.clone())
            .ok_or_else(|| Error::InvalidArgument("未配置 KMS（OSS_KMS_DIR）".into()))
    }

    /// 本机全量引用扫描（删除守卫）。任何 meta 读取错误都上抛（fail-closed）。
    /// `config` 生产调用方必须传入（桶默认加密引用）；None 仅供无 SQL 面的 store 测试。
    pub async fn cmk_references(
        &self,
        cmk: &str,
        config: Option<&dyn CmkConfigRefs>,
    ) -> Result<CmkRefScan> {
        let kms = self.cmk_provider()?;
        kms.ensure_cmk(cmk)?; // 未知名 → InvalidArgument(400)，不静默空表
        let scan = self.scan_cmk_refs(cmk, None, None, None, None).await?;
        let bucket_defaults = self.scan_config_refs(cmk, config).await?;
        let refs: Vec<CmkRef> = scan.candidates.into_iter().map(|c| c.r).collect();
        let refs_truncated = scan.matched > refs.len();
        Ok(CmkRefScan {
            cmk: cmk.to_string(),
            references: scan.matched,
            refs,
            refs_truncated,
            bucket_defaults,
        })
    }

    /// 集群安全口径：在线 = 有拓扑；离线 CLI = 仅标记部署形态（无拓扑/传输）。
    fn cluster_mode(&self) -> bool {
        self.cfg.cluster.is_some() || self.cfg.cluster_deployment
    }

    async fn scan_config_refs(
        &self,
        cmk: &str,
        config: Option<&dyn CmkConfigRefs>,
    ) -> Result<Vec<CmkBucketRef>> {
        match config {
            Some(c) => c.cmk_config_refs(cmk).await,
            None => Ok(Vec::new()),
        }
    }

    /// 删除命名 CMK（管理员/离线）。流程见模块头注；返回受控拒绝而非 Err。
    /// `config` 生产调用方必须传入（桶默认加密引用）；None 仅供无 SQL 面的 store 测试。
    pub async fn delete_cmk(
        &self,
        cmk: &str,
        force: bool,
        config: Option<&dyn CmkConfigRefs>,
    ) -> Result<CmkDeleteReport> {
        let kms = self.cmk_provider()?;
        let versions = kms
            .list_cmks()?
            .into_iter()
            .find(|c| c.name == cmk)
            .map(|c| c.versions)
            .ok_or_else(|| Error::InvalidArgument(format!("KMS CMK {cmk:?} 不存在")))?;
        let base = |deleted: bool,
                    purged: bool,
                    scan: &ScanOut,
                    cfg_refs: &[CmkBucketRef],
                    reason: Option<&str>| CmkDeleteReport {
            name: cmk.to_string(),
            versions: versions.clone(),
            force,
            deleted,
            purged,
            references: scan.matched,
            refs: scan.candidates.iter().map(|c| c.r.clone()).collect(),
            refs_truncated: scan.matched > scan.candidates.len(),
            bucket_defaults: cfg_refs.to_vec(),
            reason: reason.map(str::to_string),
            warning: None,
        };
        let scan = self.scan_cmk_refs(cmk, None, None, None, None).await?;
        let cfg_refs = self.scan_config_refs(cmk, config).await?;
        if scan.matched > 0 || !cfg_refs.is_empty() {
            // force 绝不放宽已知引用；桶默认加密同属引用（否则删除后新写入即不可解）。
            let reason = if scan.matched > 0 {
                "references"
            } else {
                "bucket_default"
            };
            return Ok(base(false, false, &scan, &cfg_refs, Some(reason)));
        }
        if self.cluster_mode() && !force {
            return Ok(base(
                false,
                false,
                &scan,
                &cfg_refs,
                Some("cluster_requires_force"),
            ));
        }
        let q = kms.quarantine_cmk(cmk).await?;
        if !self.wait_writers_idle_all(DELETE_DRAIN_TIMEOUT).await {
            kms.restore_cmk(&q).await?;
            return Ok(base(false, false, &scan, &cfg_refs, Some("writers_busy")));
        }
        // 隔离后复扫：对象版本 + 桶默认。隔离后新的桶默认写入会被 put 路径的
        // ensure_cmk 拒绝，但窗口内已落库的记录只能在这里看到。
        let after = async {
            let s = self.scan_cmk_refs(cmk, None, None, None, None).await?;
            let c = self.scan_config_refs(cmk, config).await?;
            Ok::<_, Error>((s, c))
        }
        .await;
        match after {
            Ok((s, c)) if s.matched == 0 && c.is_empty() => {}
            Ok((s, c)) => {
                kms.restore_cmk(&q).await?;
                return Ok(base(false, false, &s, &c, Some("race_detected")));
            }
            Err(e) => {
                if let Err(re) = kms.restore_cmk(&q).await {
                    return Err(Error::Crypto(format!(
                        "删除复扫失败（{e}）且恢复 CMK 失败（{re}）：隔离钥在 .cmk-trash，需人工恢复"
                    )));
                }
                return Err(e);
            }
        }
        let purged = kms.purge_quarantine(&q).await?;
        let mut report = base(true, purged, &scan, &cfg_refs, None);
        if force && self.cluster_mode() {
            report.warning = Some(
                "force 仅跳过本机引用/写者屏障的集群限制：对端节点可能仍引用该 CMK（共享 KMS 目录时）——仅在确认全站离线后使用"
                    .into(),
            );
        }
        if !purged {
            report.warning = Some(
                "隔离目录清理失败：钥已不可被 provider 使用，但 .cmk-trash 残骸需人工删除".into(),
            );
        }
        Ok(report)
    }

    /// `.cmk-trash` 残留清单（本机 KMS 目录；无钥字节）。
    pub fn cmk_trash_list(&self) -> Result<Vec<kms::CmkTrashEntry>> {
        self.cmk_provider()?.list_trash()
    }

    /// `.cmk-trash` 清理（删除中断/清理失败后的重试）：对象与桶默认引用都为空才永久销毁。
    /// 与 `delete_cmk` 同口径——`force` 只放宽集群本机屏障，绝不放宽已知引用。
    pub async fn cmk_trash_purge(
        &self,
        cmk: &str,
        force: bool,
        config: Option<&dyn CmkConfigRefs>,
    ) -> Result<CmkTrashPurgeReport> {
        let kms = self.cmk_provider()?;
        let scan = self.scan_cmk_refs(cmk, None, None, None, None).await?;
        let cfg_refs = self.scan_config_refs(cmk, config).await?;
        let report = |deleted: bool,
                      purged: Vec<kms::CmkTrashEntry>,
                      scan: &ScanOut,
                      cfg_refs: &[CmkBucketRef],
                      reason: Option<&str>| CmkTrashPurgeReport {
            name: cmk.to_string(),
            purged,
            deleted,
            references: scan.matched,
            refs: scan.candidates.iter().map(|c| c.r.clone()).collect(),
            refs_truncated: scan.matched > scan.candidates.len(),
            bucket_defaults: cfg_refs.to_vec(),
            reason: reason.map(str::to_string),
        };
        if scan.matched > 0 || !cfg_refs.is_empty() {
            let reason = if scan.matched > 0 {
                "references"
            } else {
                "bucket_default"
            };
            return Ok(report(false, Vec::new(), &scan, &cfg_refs, Some(reason)));
        }
        if self.cluster_mode() && !force {
            return Ok(report(
                false,
                Vec::new(),
                &scan,
                &cfg_refs,
                Some("cluster_requires_force"),
            ));
        }
        let purged = kms.purge_trash(cmk).await?;
        if purged.is_empty() {
            return Ok(report(
                false,
                purged,
                &scan,
                &cfg_refs,
                Some("no_quarantine"),
            ));
        }
        Ok(report(true, purged, &scan, &cfg_refs, None))
    }

    /// `.cmk-trash` 安全恢复（purge 的安全方向；活动 CMK 已存在 → 拒绝合并）。
    pub async fn cmk_trash_restore(&self, cmk: &str) -> Result<kms::CmkTrashEntry> {
        self.cmk_provider()?.restore_trash(cmk).await
    }

    /// re-wrap：把 `cmk` 旧版本加密的版本重加密到当前版本（逐版本原子提交）。
    pub async fn rewrap_cmk(
        self: Arc<Self>,
        cmk: &str,
        opts: &RewrapOptions,
    ) -> Result<RewrapReport> {
        if !self.cfg.ec.enabled {
            return Err(Error::Encode("rewrap 需要 ec.enabled（加密数据面）".into()));
        }
        if self.cfg.cluster_deployment {
            // 离线 store 没有集群拓扑/传输：分片放置会按单机口径算，写出的新版本可能
            // 落到错误槽位。集群部署请走在线控制台（transport 在位）。
            return Err(Error::Unsupported(
                "离线 rewrap 不支持集群部署（[cluster]/[stretch]）：分片放置需在线拓扑；请在服务在线时经控制台执行"
                    .into(),
            ));
        }
        if opts.limit == 0 || opts.limit > REWRAP_MAX_BATCH {
            return Err(Error::InvalidArgument(format!(
                "limit 须在 1..={REWRAP_MAX_BATCH}（实得 {}）",
                opts.limit
            )));
        }
        let kms = self.cmk_provider()?;
        let current = kms.ensure_cmk(cmk)?;
        let cursor = opts.cursor.as_deref().map(decode_cursor).transpose()?;
        let scan = self
            .scan_cmk_refs(
                cmk,
                Some(current),
                cursor.as_ref(),
                opts.bucket.as_deref(),
                Some(opts.limit),
            )
            .await?;
        let mut report = RewrapReport {
            cmk: cmk.to_string(),
            current_version: current,
            apply: opts.apply,
            scanned_objects: scan.scanned_objects,
            candidates: scan.candidates.len(),
            rewrapped: 0,
            skipped: 0,
            blocked: 0,
            failed: 0,
            truncated: scan.truncated,
            next_cursor: scan
                .truncated
                .then(|| scan.candidates.last().map(|c| encode_cursor(&c.r)))
                .flatten(),
            items: Vec::with_capacity(scan.candidates.len()),
        };
        for cand in &scan.candidates {
            let mut item = RewrapItem {
                bucket: cand.r.bucket.clone(),
                key: cand.r.key.clone(),
                version_id: cand.r.version_id.clone(),
                old_key_id: cand.r.key_id.clone(),
                new_key_id: None,
                size: cand.r.size,
                status: "preview".into(),
                error: None,
            };
            if let Some(why) = &cand.block {
                item.status = "blocked".into();
                item.error = Some(why.clone());
                report.blocked += 1;
            } else if opts.apply {
                match Arc::clone(&self).rewrap_one(cmk, &cand.r).await {
                    Ok(Some(new_key_id)) => {
                        item.status = "rewrapped".into();
                        item.new_key_id = Some(new_key_id);
                        report.rewrapped += 1;
                    }
                    Ok(None) => {
                        item.status = "skipped".into();
                        item.error =
                            Some("already at current CMK version (or version gone)".into());
                        report.skipped += 1;
                    }
                    Err(e) => {
                        item.status = "failed".into();
                        item.error = Some(e.to_string());
                        report.failed += 1;
                    }
                }
            }
            report.items.push(item);
        }
        Ok(report)
    }

    /// 扫描本机桶/键。`current=None` = 引用守卫（全量计数）；`Some` = rewrap 候选（版本 < current）。
    async fn scan_cmk_refs(
        &self,
        cmk: &str,
        current: Option<u32>,
        cursor: Option<&Cursor>,
        bucket: Option<&str>,
        limit: Option<usize>,
    ) -> Result<ScanOut> {
        let mut buckets: Vec<String> = match bucket {
            Some(b) => {
                self.ensure_bucket(b).await?;
                vec![b.to_string()]
            }
            None => self
                .list_buckets()
                .await?
                .into_iter()
                .map(|b| b.name)
                .collect(),
        };
        buckets.sort();
        let mut out = ScanOut::default();
        'buckets: for b in buckets {
            if let Some(c) = cursor
                && b.as_str() < c.bucket.as_str()
            {
                continue;
            }
            let same_bucket = cursor.is_some_and(|c| b == c.bucket);
            let mut keys = match self.collect_keys(&b).await {
                Ok(k) => k,
                // 集群（在线或离线标记）：本机无该桶目录（远端独有桶）→ 本机无对象可扫
                Err(Error::NoSuchBucket(_)) if self.cluster_mode() => continue,
                Err(e) => return Err(e),
            };
            keys.sort();
            for k in keys {
                if let Some(c) = cursor
                    && same_bucket
                    && k.as_str() < c.key.as_str()
                {
                    continue;
                }
                let same_key = cursor.is_some_and(|c| b == c.bucket && k == c.key);
                let Some(m) = self.read_meta(&b, &k).await? else {
                    continue;
                };
                out.scanned_objects += 1;
                // cursor 版本已不在链上（并发删除）→ 从链首重扫（幂等，已改版本不再命中）
                let start = match cursor.filter(|_| same_key) {
                    Some(c) => m
                        .versions
                        .iter()
                        .position(|v| v.version_id_str() == c.version_id)
                        .map_or(0, |i| i + 1),
                    None => 0,
                };
                for v in m.versions.iter().skip(start) {
                    if v.delete_marker() {
                        continue;
                    }
                    let Some(rec) = &v.encryption else { continue };
                    match current {
                        // 引用守卫：任何形态提及都算引用
                        None => {
                            if !record_mentions(rec, cmk) {
                                continue;
                            }
                        }
                        // rewrap：只认规范 key_id 且版本 < 当前
                        Some(cur) => {
                            let Some((name, n)) = kms::parse_cmk_key_id(&rec.key_id) else {
                                continue;
                            };
                            if name != cmk || n >= cur {
                                continue;
                            }
                        }
                    }
                    // 本页已满 → 再多看到一个命中即证明有续页
                    if let Some(lim) = limit
                        && out.candidates.len() >= lim
                    {
                        out.matched += 1;
                        out.truncated = true;
                        break 'buckets;
                    }
                    out.matched += 1;
                    if limit.is_none() && out.candidates.len() >= REFS_REPORT_CAP {
                        continue; // 守卫只计数，不再攒列表
                    }
                    out.candidates.push(Candidate {
                        r: CmkRef {
                            bucket: b.clone(),
                            key: k.clone(),
                            version_id: v.version_id_str(),
                            key_id: rec.key_id.clone(),
                            size: v.size,
                        },
                        block: block_reason(v),
                    });
                }
            }
        }
        Ok(out)
    }

    /// 单版本重加密：读旧明文（旧 CMK 版本解密）→ 目标 CMK 当前版本重加密 → 新版本目录 →
    /// 单点 meta 翻转 → 旧目录回收。`Ok(None)` = 版本已不在/已被改到当前版本（幂等跳过）。
    async fn rewrap_one(self: Arc<Self>, cmk: &str, cand: &CmkRef) -> Result<Option<String>> {
        let _guard = self.lock_object(&cand.bucket, &cand.key).await?;
        let Some(old) = self.read_meta(&cand.bucket, &cand.key).await? else {
            return Ok(None);
        };
        let cur = self.cmk_provider()?.ensure_cmk(cmk)?;
        let Some(idx) = old
            .versions
            .iter()
            .position(|v| !v.delete_marker() && v.version_id_str() == cand.version_id)
        else {
            return Ok(None);
        };
        let v = &old.versions[idx];
        let Some(rec) = &v.encryption else {
            return Err(Error::CorruptMeta(format!(
                "{}/{}@{}: 版本无加密记录",
                cand.bucket, cand.key, cand.version_id
            )));
        };
        if rec.key_id != cand.key_id {
            return Ok(None); // 并发/上一页已改到新记录
        }
        let Some((name, n)) = kms::parse_cmk_key_id(&rec.key_id) else {
            return Err(Error::CorruptMeta(format!(
                "key_id 形态非法：{:?}",
                rec.key_id
            )));
        };
        if name != cmk {
            return Err(Error::InvalidArgument(format!(
                "版本 key_id 属于 CMK {name:?}，与目标 {cmk:?} 不符"
            )));
        }
        if n >= cur {
            return Ok(None);
        }
        if let Some(why) = block_reason(v) {
            return Err(Error::InvalidArgument(format!("版本不可重加密：{why}")));
        }
        let old_uuid = v.part.uuid.clone();
        let body = version_stream(
            Arc::clone(&self),
            cand.bucket.clone(),
            cand.key.clone(),
            v.clone(),
        );
        let (new_ctx, new_rec) =
            crypto::UnitCtx::for_write(EncSpec::SseKmsCmk(cmk.to_string()), self.maybe_enc_state())
                .await?;
        let new_uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(self.cfg.root(), "tmp").join(&new_uuid);
        let multi = self.cfg.cluster.is_some() || self.cfg.multi_drive();
        type Staged = (
            u64,
            String,
            String,
            Option<meta::DataLayout>,
            Option<meta::InlineData>,
        );
        let staged: Result<Staged> = if multi {
            self.stage_payload_multi_tee(
                &cand.bucket,
                &cand.key,
                &new_uuid,
                &tmp,
                body,
                Some(&new_ctx),
                None,
                None,
            )
            .await
            .map(|(s, e, sh, l, i, _)| (s, e, sh, l, i))
        } else {
            self.stage_payload_tee(&tmp, body, Some(&new_ctx), None, None)
                .await
        };
        let (size, etag, sha256, data_layout, inline) = match staged {
            Ok(x) => x,
            Err(e) => {
                self.discard_rewrap_stage(&cand.bucket, &cand.key, &new_uuid, &tmp)
                    .await;
                return Err(e);
            }
        };
        if size != v.size || sha256 != v.part.sha256 {
            self.discard_rewrap_stage(&cand.bucket, &cand.key, &new_uuid, &tmp)
                .await;
            return Err(Error::Crypto(format!(
                "{}/{}@{}: 重加密校验失败（size {size}/{}, sha256 不一致）——原数据可能损坏或并发变更",
                cand.bucket, cand.key, cand.version_id, v.size
            )));
        }
        if !v.etag.contains('-') && etag != v.etag {
            self.discard_rewrap_stage(&cand.bucket, &cand.key, &new_uuid, &tmp)
                .await;
            return Err(Error::Crypto(format!(
                "{}/{}@{}: 重加密 ETag 不一致（{etag} ≠ {}）",
                cand.bucket, cand.key, cand.version_id, v.etag
            )));
        }
        if !multi {
            let obj_dir = layout::object_dir(self.cfg.root(), &cand.bucket, &cand.key)?;
            let moved = async {
                tokio::fs::create_dir_all(&obj_dir).await?;
                tokio::fs::rename(&tmp, obj_dir.join(&new_uuid)).await?;
                if self.cfg.fsync {
                    Store::sync_dir(&obj_dir).await;
                }
                Ok::<(), Error>(())
            }
            .await;
            if let Err(e) = moved {
                self.discard_rewrap_stage(&cand.bucket, &cand.key, &new_uuid, &tmp)
                    .await;
                return Err(e);
            }
        }
        let mut m = old.clone();
        let mut nv = m.versions[idx].clone();
        nv.part.uuid = new_uuid.clone();
        nv.part.part = 1;
        nv.part.size = size;
        nv.part.sha256 = sha256;
        nv.data_layout = data_layout;
        nv.inline = inline;
        nv.parts = None; // 新形态恒为 inline/EC；旧 parts 加密形态不存在（读路径已拒绝）
        nv.encryption = Some(new_rec.clone());
        m.versions[idx] = nv;
        if let Some(last) = m.versions.last_mut() {
            last.epoch = Some(old.max_epoch() + 1);
        }
        m.refresh_latest_flags();
        if let Err(e) = self
            .write_meta(&cand.bucket, &cand.key, &m, Some(&old))
            .await
        {
            self.discard_rewrap_stage(&cand.bucket, &cand.key, &new_uuid, &tmp)
                .await;
            return Err(e);
        }
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        self.recycle_version_dir(&cand.bucket, &cand.key, &old_uuid)
            .await;
        Ok(Some(new_rec.key_id))
    }

    /// 未提交的新版本目录/临时目录清理（幂等）。
    async fn discard_rewrap_stage(
        &self,
        bucket: &str,
        key: &str,
        uuid: &str,
        tmp: &std::path::Path,
    ) {
        let _ = tokio::fs::remove_dir_all(tmp).await;
        self.rollback_versions(bucket, key, uuid).await;
    }
}
