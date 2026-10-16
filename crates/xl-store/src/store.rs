//! 存储内核。M1：单盘、无纠删码，但**提交协议与 xl.meta 格式按最终设计落**，
//! M2 只是把「一个 part 一个文件」换成「一个 part 若干分片」。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncWriteExt;

use crate::crypto;
use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, ChecksumRecord, EncryptionRecord, XlMeta};

mod drives;
pub mod ec;
mod fsck;
mod gc;
mod list;
mod lock;
mod mesh_api;
mod mpu;
mod put;
mod read;
mod rebalance;
mod version;
pub use rebalance::RebalanceReport;
pub use version::{DeleteOutcome, VersionEntry, VersionList};

// M3：集群拓扑类型经 store 面公开（drives 模块本身保持私有）
pub use drives::{ClusterConfig, ClusterSets, ClusterTopology, NodeSpec};

pub type ByteStream = std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// 数据盘数组（M2b-2 起多盘；单盘形态 = 长度 1）。`drives[0]` 兼作命名空间枚举源与暂存盘。
    pub drives: Vec<PathBuf>,
    /// 提交点是否 fsync。测试可关（快），生产必须开。
    pub fsync: bool,
    /// M2b EC 配置（默认 8+4、启用）。读路径按 meta 里的 `DataLayout` 重建，不读这里。
    pub ec: ec::EcConfig,
    /// 小于此长度的对象内联进 meta（不落 part 文件）。默认 128 KiB。
    /// 不得越过 [`meta::MAX_INLINE`]（meta 解码信任边界），`open` 会拒绝。
    pub inline_threshold: u64,
    /// M3 集群：静态拓扑 + 节点间传输。**None = 单节点**（v1.4.0 行为逐字节一致）。
    pub cluster: Option<Arc<drives::ClusterConfig>>,
    /// M3 集群锁（Task 8）：租约 TTL（生产默认 10s；测试注小值跑快）。
    pub cluster_lock_ttl_ms: u64,
    /// 获取重试次数与退避抖动区间 `(lo, hi)` ms（生产默认 10 次 / 50–200ms）。
    pub cluster_lock_retries: u32,
    pub cluster_lock_backoff_ms: (u64, u64),
    /// M3 控制面读扇出的服务端执行器（Task 9；oss-server 注入 sqlx 实现）。
    pub control: Option<Arc<dyn crate::mesh::ControlBackend>>,
}

impl StoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            drives: vec![root.into()],
            fsync: true,
            ec: ec::EcConfig::default(),
            inline_threshold: meta::MAX_INLINE,
            cluster: None,
            cluster_lock_ttl_ms: 10_000,
            cluster_lock_retries: 10,
            cluster_lock_backoff_ms: (50, 200),
            control: None,
        }
    }

    /// 命名空间源（列举/暂存/旧形态数据所在盘）：drives[0]。
    pub fn root(&self) -> &Path {
        &self.drives[0]
    }

    /// 多盘分发是否启用（EC 开启且盘数 > 1）。单盘（含 M2b-1 单盘 EC）走既有 tmp→rename 协议。
    pub fn multi_drive(&self) -> bool {
        self.ec.enabled && self.drives.len() > 1
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BucketInfo {
    pub name: String,
    pub creation_nanos: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub last_modified_nanos: i64,
    pub user_metadata: std::collections::BTreeMap<String, String>,
    /// 客户端声明的校验和（已通过；仅回写 GET/HEAD 用）
    pub checksum: Option<ChecksumRecord>,
    /// S3 线格式版本号（"null" = 无版本控制条目）；列表并集的降级条目为 None
    pub version_id: Option<String>,
    /// M5 切片 2：加密记录（切片 3 回 `x-amz-server-side-encryption*` 响应头用；None = 明文对象）
    pub encryption: Option<EncryptionRecord>,
}

#[derive(Debug, Clone, Default)]
pub struct ListRequest {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<usize>,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ListResult {
    pub objects: Vec<ObjectInfo>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_token: Option<String>,
}

pub struct Store {
    cfg: StoreConfig,
    /// 进程内 per-object 互斥，`bucket/key` → 锁：同键的并发写必须串行，
    /// 否则两个写者会把对方的版本目录当「老版本」扔进回收站。键前缀 `bucket:` 承载桶级锁
    /// （M2b-2 Task 10，见 `lock_bucket`）。
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// quorum 未达计数（M3 最小信号，写/锁分列）：`/health/cluster` 暴露——
    /// 分区故障必须线上有信号源（完整 Prometheus 面归 M4）。
    quorum_unmet_write: AtomicU64,
    quorum_unmet_lock: AtomicU64,
    /// M3 集群锁表（节点侧；`mesh_lock` 与协调者本地授予共用）。
    cluster_locks: Arc<lock::LockTable>,
    /// M5 切片 2：密钥库状态（`init_encryption` 一次性置入；未启用 = 空，零行为变化）。
    encryption: std::sync::OnceLock<crypto::EncState>,
}

/// 对象锁守卫：进程内 tokio 互斥 + 跨进程 flock（`File` drop 即释放 flock）
/// + 集群 quorum 租约锁（M3 Task 8；仅 cluster 配置时第三层，顺序恒定防死锁）。
pub(in crate::store) struct LockGuard {
    _mem: tokio::sync::OwnedMutexGuard<()>,
    _file: std::fs::File,
    _cluster: Option<lock::ClusterLockGuard>,
}

impl LockGuard {
    /// 集群租约是否丢失（单机恒 false）——写入口在提交点前检查（无 fencing 窗口收窄）。
    pub(in crate::store) fn lease_lost(&self) -> bool {
        self._cluster.as_ref().is_some_and(|c| c.lease_lost())
    }
}

impl Store {
    pub async fn open(cfg: StoreConfig) -> Result<Self> {
        // 信任边界：内联阈值不得超过 meta 解码上限，否则写出的 meta 重启后会被自己拒收
        if cfg.inline_threshold > meta::MAX_INLINE {
            return Err(Error::Encode(format!(
                "inline_threshold {} 超过 MAX_INLINE {}",
                cfg.inline_threshold,
                meta::MAX_INLINE
            )));
        }
        // M2b-2 盘数裁决（升级不破 + 多盘大声失败）：
        //   - enabled=false：M1 旧形态，只支持单盘（多盘旧形态无语义，Err 拒配）
        //   - enabled=true 单盘：M2b-1 形态（分片全在 drive 0），合法但无冗余——warn
        //   - enabled=true 多盘：盘数须 ≥ data+parity（8+4 即 ≥12）；2..11 盘是「半冗余」误配 → Err
        let n = cfg.drives.len();
        if n == 0 {
            return Err(Error::Encode("drives 为空".into()));
        }
        if n > 255 {
            return Err(Error::Encode(format!(
                "盘数 {n} 超过 255（ShardRef.drive 为 u8）"
            )));
        }
        // EC 数字域校验（评审 R4：配置是信任边界；u8 相加在 debug 会溢出 panic）
        if cfg.ec.data == 0 || cfg.ec.parity == 0 {
            return Err(Error::Encode("EC data/parity 必须 ≥ 1".into()));
        }
        if cfg.ec.data as u16 + cfg.ec.parity as u16 > 255 {
            return Err(Error::Encode(format!(
                "EC {}+{} 超过 GF(2^8) 上限 255",
                cfg.ec.data, cfg.ec.parity
            )));
        }
        if !cfg.ec.enabled {
            if n != 1 {
                return Err(Error::Encode(format!(
                    "ec.enabled=false（M1 旧形态）只支持单盘，当前 {n} 盘"
                )));
            }
            tracing::warn!("ec.enabled=false：单文件旧形态，无冗余、无内联");
        } else if cfg.cluster.is_some() {
            // M3 集群：本节点盘数只做 self 一致性（本地 3 盘 < 12 是合法节点形态）；
            // data+parity 的全局校验在 ClusterTopology::new（槽位数 ≥ 12）。
            // M4 切片 8：多 set 下自节点盘数 = 各 set 段之和（顺序契约见 ClusterSets::new）。
            let cl = cfg.cluster.as_ref().unwrap();
            let declared: usize = cl.sets.self_ranges.iter().map(|(_, _, len)| len).sum();
            let flat_self = cl.sets.flat.self_node();
            if declared != n || flat_self.drive_count as usize != n {
                return Err(Error::Encode(format!(
                    "cluster：本节点 {} 声明 {} 盘（各 set 段合计 {declared}），实际 {n} 盘",
                    flat_self.id, flat_self.drive_count
                )));
            }
        } else if n == 1 {
            tracing::warn!("单盘 EC：所有分片同盘，无冗余（多盘冗余需 data_dirs ≥ 12）");
        } else if n < (cfg.ec.data as usize + cfg.ec.parity as usize) {
            return Err(Error::Encode(format!(
                "盘数 {n} 不足 EC {}+{}（多盘须 ≥ {}；单盘可运行但无冗余）",
                cfg.ec.data,
                cfg.ec.parity,
                cfg.ec.data as usize + cfg.ec.parity as usize
            )));
        }

        // 盘身份与初始化（评审 R1）：`.oss.sys` 的存在 = 「已初始化的真盘」，open **不再无条件自建**——
        // 挂载点掉线后挂载点目录仍留在父 fs 上，自建等于把 impostor 当在册盘（写进根盘、假 quorum）。
        // 规则：
        //   - 全部盘都无 `.oss.sys`（真首启）→ 全盘初始化并盖同一部署标识 `.oss.sys/drive-id`；
        //   - 已有在册盘（存在任一 `.oss.sys`）→ 无 sys 的盘一律**按不在册处理**（不创建、warn），
        //     插回/恢复后凭其后 sys 目录自动重新在册（无需重启）；
        //   - 已启动过但缺 drive-id 的盘（旧部署升级）→ 补盖当前部署标识；
        //   - 各盘标识不一致（插错盘/混入别的部署）→ 大声失败。
        // 已知盲区（如实记录）：**首启时**全部盘同时掉线无法与「全新盘」区分，会初始化 impostor——
        // 部署文档要求「首启时确认各盘已挂载」。
        let mut initialized = 0usize;
        let mut ids: Vec<String> = Vec::new();
        for drive in &cfg.drives {
            if matches!(
                tokio::fs::try_exists(layout::sys_dir(drive, "")).await,
                Ok(true)
            ) {
                initialized += 1;
                if let Ok(s) = tokio::fs::read_to_string(layout::sys_dir(drive, "drive-id")).await {
                    let s = s.trim().to_string();
                    if !s.is_empty() && !ids.contains(&s) {
                        ids.push(s);
                    }
                }
            }
        }
        if ids.len() > 1 {
            return Err(Error::Encode(format!(
                "盘部署标识不一致（插错盘？）：{ids:?}"
            )));
        }
        let deployment_id = ids
            .first()
            .cloned()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        if initialized == 0 {
            tracing::warn!("全盘无 .oss.sys：按真首启初始化（新部署标识 {deployment_id}）");
        }
        for (di, drive) in cfg.drives.iter().enumerate() {
            let sys = layout::sys_dir(drive, "");
            let has_sys = matches!(tokio::fs::try_exists(&sys).await, Ok(true));
            if !has_sys {
                if initialized == 0 {
                    // 真首启：初始化
                } else {
                    tracing::warn!(
                        drive = %drive.display(),
                        "盘未初始化或未挂载，按不在册处理（不创建 .oss.sys）"
                    );
                    continue;
                }
            }
            for sub in ["tmp", "trash", "locks"] {
                tokio::fs::create_dir_all(layout::sys_dir(drive, sub)).await?;
            }
            // 盖/补部署标识（写失败上抛：标识是防插错的根，写不进去宁可不启动）
            let id_path = layout::sys_dir(drive, "drive-id");
            if !matches!(tokio::fs::try_exists(&id_path).await, Ok(true)) {
                tokio::fs::write(&id_path, &deployment_id).await?;
            }
            // M3/M4 拓扑指纹（data_dirs 顺序 = 拓扑的一部分；错位 = 分片静默错位 → 大声失败）。
            // 切片 5：记录格式 v2 = `v2 node=X slot=Y`（**去掉全局 slot_count**——追加式扩容
            // 后旧盘 (node,slot) 不变）；读时兼容 v1（忽略 `slots=` 段）。
            // 切片 8：set==0 恒写 v2（单 set 与多 set 的 set0 段逐字节不变）；set>0 写
            // `v3 set=Z node=X slot=Y`（slot=组内槽）；v3+set=0 = 拒启（写路径永不产出）。
            // 中间节点/中间 set 加盘会位移其后段 → 记录与现算不符 → 在此拒启（设计 G）。
            if let Some(cl) = cfg.cluster.as_deref() {
                let topo_path = layout::sys_dir(drive, "topo");
                let cur_node = cl.sets.flat.self_node().id.clone();
                // 该盘属于哪个 set 段：di 落在 self_ranges 的哪一段
                let (cur_set, off, _) = cl
                    .sets
                    .self_ranges
                    .iter()
                    .find(|&(_, off, len)| di >= *off && di < off + len)
                    .copied()
                    .ok_or_else(|| Error::Encode("盘下标不在任何 set 段（内部不一致）".into()))?;
                let cur_slot = (di - off) as u16;
                let want = topo_record(cur_set, &cur_node, cur_slot);
                match tokio::fs::read_to_string(&topo_path).await {
                    Ok(have) => {
                        let ok = parse_topo_record(have.trim()).is_some_and(|(set, n, s)| {
                            n == cur_node && s == cur_slot && set == cur_set
                        });
                        if !ok {
                            return Err(Error::Encode(format!(
                                "拓扑指纹不符（data_dirs 顺序变化/中间节点加盘？）：盘 {} 记录 «{}»，当前 set={} node={} slot={}",
                                drive.display(),
                                have.trim(),
                                cur_set,
                                cur_node,
                                cur_slot
                            )));
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        tokio::fs::write(&topo_path, &want).await?;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }

        let store = Self {
            cfg,
            locks: Mutex::new(HashMap::new()),
            quorum_unmet_write: AtomicU64::new(0),
            quorum_unmet_lock: AtomicU64::new(0),
            cluster_locks: Arc::new(lock::LockTable::default()),
            encryption: std::sync::OnceLock::new(),
        };
        // 启动自检：清 tmp/trash 的崩溃残留（对象目录不碰——在途 MPU 暂存区按其 mtime 由 clean 回收）
        store.reap_transient().await?;
        Ok(store)
    }

    /// M5 切片 2：密钥库接线（**前提块**：须在 `open` 成功后调用——掉盘首启若先生成新主密钥，
    /// 盘恢复后既有加密对象全部静默报废；且 provider 先跑会往盘在册扫描里插「无 drive-id 的
    /// `.oss.sys`」污染降级判定）。
    /// 门闩：root 盘 `drive-id` 在册才允许构造/生成；未在册 → Err **且不落任何钥文件**。
    /// 两参数均 None → Err（未调用本函数 = 不启用加密，Store 行为零变化）。
    pub async fn init_encryption(
        &self,
        file_path: Option<PathBuf>,
        kms_dir: Option<PathBuf>,
    ) -> Result<()> {
        if file_path.is_none() && kms_dir.is_none() {
            return Err(Error::Encode(
                "init_encryption：file/kms 至少给一个路径".into(),
            ));
        }
        let marker = layout::sys_dir(self.cfg.root(), "drive-id");
        if !matches!(tokio::fs::try_exists(&marker).await, Ok(true)) {
            return Err(Error::Crypto(format!(
                "根盘未在册（缺 {}）——拒绝初始化密钥库（防掉盘首启生成新主密钥）",
                marker.display()
            )));
        }
        let file = match file_path {
            Some(p) => Some(Arc::new(
                crate::kms::FileKeyProvider::open_or_init(&p).await?,
            )),
            None => None,
        };
        let kms = match kms_dir {
            Some(d) => Some(Arc::new(
                crate::kms::KmsFileProvider::open_or_init(&d).await?,
            )),
            None => None,
        };
        self.encryption
            .set(crypto::EncState { file, kms })
            .map_err(|_| Error::Encode("加密已初始化（init_encryption 不可重复）".into()))
    }

    /// 加密状态（未启用 = None）。**所有**写读路径经此取 provider——SSE-C 分支不需要
    /// provider（`for_write/for_read` 只在 S3/KMS 分支对 None fail-closed），若在此直接
    /// 报错会把「未接线」误伤为 SSE-C 不可用（切片 3 实测踩过）。
    pub(in crate::store) fn maybe_enc_state(&self) -> Option<&crypto::EncState> {
        self.encryption.get()
    }

    pub fn root(&self) -> &Path {
        self.cfg.root()
    }

    /// 集群面访问（None = 单节点，v1.4.0 行为）。oss-server 用它取拓扑与传输。
    pub fn cluster(&self) -> Option<&drives::ClusterConfig> {
        self.cfg.cluster.as_deref()
    }

    /// quorum 未达计数快照（写/锁分列；`/health/cluster` 的最小信号）。
    pub fn quorum_unmet_counts(&self) -> (u64, u64) {
        (
            self.quorum_unmet_write.load(Ordering::Relaxed),
            self.quorum_unmet_lock.load(Ordering::Relaxed),
        )
    }

    /// 写路径 quorum 未达（客户端可见的写失败）时递增；Task 6 的集群提交路径同口径。
    pub(crate) fn bump_quorum_unmet_write(&self) {
        self.quorum_unmet_write.fetch_add(1, Ordering::Relaxed);
    }

    /// 锁 quorum 未达时递增（Task 8 接入）。
    #[allow(dead_code)] // Task 8 消费
    pub(crate) fn bump_quorum_unmet_lock(&self) {
        self.quorum_unmet_lock.fetch_add(1, Ordering::Relaxed);
    }

    fn lock_for(&self, bucket: &str, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.lock_named(format!("{bucket}/{key}"))
    }

    /// 桶级互斥（M2b-2 Task 10）：挡「并发 delete_bucket + put 让桶复活」。
    /// 键前缀 `bucket:` 与对象锁键（`{bucket}/{key}`）不可能同串（桶名/键名均无 `:` 于该位置）。
    fn lock_bucket(&self, bucket: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.lock_named(format!("bucket:{bucket}"))
    }

    fn lock_named(&self, id: String) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().unwrap();
        let lock = locks
            .entry(id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        // 回收无人持有的旧条目，锁表不随历史写过的键无限增长
        //（本键刚 clone 出来，count ≥ 2 必留；其余条目只为 map 自己持有时删掉）
        locks.retain(|_, v| Arc::strong_count(v) > 1);
        lock
    }

    /// 对象锁的完整形态（**所有写路径的统一入口**）：进程内 tokio 互斥 → 跨进程 flock。
    /// 顺序恒定（内存锁在前）避免死锁；flock 文件固定锁 drive0 的
    /// `.oss.sys/locks/<sha256(bucket/key)[..16]>.lock`（锁是互斥信号不是数据，无需每盘一份）。
    ///
    /// `File::lock()` 是阻塞调用：放进 `spawn_blocking`——多任务争用同一把锁时，
    /// 挂起的是阻塞池线程而不是 async 执行器线程（否则工作线程被 parked 可能饿死持锁任务）。
    pub(in crate::store) async fn lock_object(&self, bucket: &str, key: &str) -> Result<LockGuard> {
        let mem = self.lock_for(bucket, key).lock_owned().await;
        let id = {
            use sha2::Digest as _;
            hex::encode(sha2::Sha256::digest(format!("{bucket}/{key}").as_bytes()))
        };
        let path = layout::sys_dir(self.cfg.root(), "locks").join(format!("{}.lock", &id[..16]));
        let file = tokio::fs::File::create(&path).await?.into_std().await;
        let file = tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
            file.lock()?;
            Ok(file)
        })
        .await
        .map_err(|e| Error::Encode(format!("锁等待任务异常: {e}")))??;
        // M3 集群（Task 8）：第三层 quorum 租约锁（顺序恒定防死锁：进程内 → flock → 集群）
        let cluster = if self.cfg.cluster.is_some() {
            Some(self.acquire_cluster_lock(bucket, key).await?)
        } else {
            None
        };
        Ok(LockGuard {
            _mem: mem,
            _file: file,
            _cluster: cluster,
        })
    }

    /// 桶级互斥的守卫（不做 flock——桶级操作已有对象锁覆盖写者，这是进程内的窗口收口）。
    fn lock_bucket_owned(
        &self,
        bucket: &str,
    ) -> impl std::future::Future<Output = tokio::sync::OwnedMutexGuard<()>> + '_ {
        self.lock_bucket(bucket).lock_owned()
    }

    /// 桶操作的统一前置。名字校验在此收口：桶目录是 `drive.join(bucket)`，
    /// 不过滤 `..`/绝对路径就会让删桶、列举跑到盘根之外。
    /// 多盘（M2b-2）：任一在册盘上有桶目录即视为存在（drive0 掉线不误报 NoSuchBucket）。
    async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        layout::validate_bucket(bucket)?;
        for drive in &self.cfg.drives {
            if tokio::fs::try_exists(layout::bucket_dir(drive, bucket)).await? {
                return Ok(());
            }
        }
        Err(Error::NoSuchBucket(bucket.to_string()))
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        layout::validate_bucket(bucket)?;
        let _bguard = self.lock_bucket_owned(bucket).await;
        // 集群：先查全节点（评审：静默部分建会让缺桶节点 404——分片分布确定（shard % slot_count），
        // 落不到该节点的桶永不靠懒自愈修复；任一节点不可达 → 上抛 fail-closed）
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                let names = cl.transport.list_bucket_dirs(&n.id).await?;
                if names.iter().any(|b| b == bucket) {
                    return Err(Error::BucketAlreadyExists(bucket.to_string()));
                }
            }
        }
        // 多盘：命名空间镜像——每盘各建一份；已存在（任一盘）= BucketAlreadyExists
        let mut created: Vec<&PathBuf> = Vec::new();
        for drive in &self.cfg.drives {
            let dir = layout::bucket_dir(drive, bucket);
            match tokio::fs::create_dir(&dir).await {
                Ok(()) => created.push(drive),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(Error::BucketAlreadyExists(bucket.to_string()));
                }
                Err(e) => {
                    // 部分建失败：回滚已建目录，保持命名空间镜像一致
                    for d in &created {
                        let _ = tokio::fs::remove_dir(layout::bucket_dir(d, bucket)).await;
                    }
                    return Err(e.into());
                }
            }
        }
        // 集群：广播建目录（幂等）；失败上抛（fail-closed——部分建会让缺桶节点 404，
        // 运维出路：删空桶重试或让写路径懒自愈补齐）
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                cl.transport.ensure_bucket(&n.id, bucket).await?;
            }
        }
        Ok(())
    }

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        layout::validate_bucket(bucket)?;
        for drive in &self.cfg.drives {
            if tokio::fs::try_exists(layout::bucket_dir(drive, bucket)).await? {
                return Ok(true);
            }
        }
        // 集群：任一节点有即存在；不可达 → 上抛（fail-closed，不把「查不到」伪装成「不存在」）
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                let names = cl.transport.list_bucket_dirs(&n.id).await?;
                if names.iter().any(|b| b == bucket) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        // 桶级互斥（M2b-2 Task 10 收口）：与「写者的桶窗口」互斥——写者窗口内先落对象目录，
        // 此后本检查见非空必拒；窗口外删除成功 → 迟到的写者 ensure 失败 NoSuchBucket。不复活的证明在测试。
        let _bguard = self.lock_bucket_owned(bucket).await;
        self.ensure_bucket(bucket).await?;
        // 评审 R2：不在册盘上的副本删不到——静默当空/成功会让「插回后桶（连同对象）复活」。
        // fail-closed：任一配置盘不在册 → 拒绝删桶（而不是把缺席盘当空桶）。
        for drive in &self.cfg.drives {
            if !drives::drive_up(drive).await {
                return Err(Error::QuorumUnmet(format!(
                    "盘 {} 不在册，删桶中止（防插回复活）",
                    drive.display()
                )));
            }
        }
        // 任一在册盘上还有对象目录就拒绝（S3 语义）
        for drive in &self.cfg.drives {
            let dir = layout::bucket_dir(drive, bucket);
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let non_empty = entries.next_entry().await?.is_some();
            drop(entries);
            if non_empty {
                return Err(Error::BucketNotEmpty(bucket.to_string()));
            }
        }
        // 集群：逐节点「空检查 + 删」（服务端原子做）；任一节点不可达/非空 → 中止。
        // 部分节点已删是自洽的：全局存在性 = 任一节点有（本函数上方的 bucket_exists 同口径）；
        // 缺桶节点的目录随对象写入懒自愈，重试删桶幂等。
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                cl.transport.delete_bucket_dir(&n.id, bucket).await?;
            }
        }
        for drive in &self.cfg.drives {
            match tokio::fs::remove_dir(layout::bucket_dir(drive, bucket)).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                    return Err(Error::BucketNotEmpty(bucket.to_string()));
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        // 评审 R3：各盘取并集（drive0 掉线不得让列举静默变空）；创建时间取首个见到该桶的盘。
        let mut out: Vec<BucketInfo> = Vec::new();
        for drive in &self.cfg.drives {
            let mut entries = match tokio::fs::read_dir(drive).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                if name == layout::SYS_DIR || out.iter().any(|b| b.name == name) {
                    continue;
                }
                let nanos = entry
                    .metadata()
                    .await
                    .ok()
                    .and_then(|m| m.created().or_else(|_| m.modified()).ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0);
                out.push(BucketInfo {
                    name,
                    creation_nanos: nanos,
                });
            }
        }
        // 集群：并集各节点桶目录（桶是广播镜像，掉线节点少一路并集不丢项；不可达 warn 跳过）。
        // 远端独有名字的 creation_nanos 记 0（「首个见到」口径：本机没有它的 mtime）。
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                match cl.transport.list_bucket_dirs(&n.id).await {
                    Ok(names) => {
                        for name in names {
                            if name == layout::SYS_DIR || out.iter().any(|b| b.name == name) {
                                continue;
                            }
                            out.push(BucketInfo {
                                name,
                                creation_nanos: 0,
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!(node = %n.id, error = %e, "远端桶列表不可达（少一路并集）")
                    }
                }
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// 读 meta：逐盘读取，**秩最大者胜**（`XlMeta::rank() = (max_epoch, 链尾 mod_time)`——
    /// epoch 单调优先，墙钟回拨不再让旧副本翻盘；老 meta 无 epoch 按 0 参与排秩）。
    ///
    /// 多盘契约（铁律②读侧）：meta 在每盘各一份，多数情况内容相同；跨盘不一致 = 撕裂写
    /// 的中间态（某盘已翻转、某盘未翻转）——旧的那份必然指向旧版本，取最新即取已提交版本。
    /// 单盘 meta 损坏/键不一致 → warn 跳过（其余盘可服务）；**一个可用副本都没有但见过损坏
    /// 副本** → `Err(CorruptMeta)`（fail-closed，不把「盘上有坏 meta」伪装成 404）。
    async fn read_meta(&self, bucket: &str, key: &str) -> Result<Option<XlMeta>> {
        if self.cfg.cluster.is_some() {
            return self.read_meta_cluster(bucket, key, false).await;
        }
        let mut best: Option<((u64, i64), XlMeta)> = None;
        let mut corrupt: Option<String> = None;
        for drive in &self.cfg.drives {
            let path = layout::object_dir(drive, bucket, key)?.join(meta::META_FILE);
            match tokio::fs::read(&path).await {
                Ok(bytes) => match XlMeta::decode(&bytes) {
                    Ok(m) if m.object_key == key => {
                        let t = m.rank();
                        if best.as_ref().is_none_or(|(bt, _)| t > *bt) {
                            best = Some((t, m));
                        }
                    }
                    Ok(m) => {
                        // 键一致性：目录名与 xl.meta 记录的键不符 = 元数据错位，不服务错误内容
                        corrupt = Some(format!(
                            "键不一致：请求 {key:?}，xl.meta 中为 {:?}",
                            m.object_key
                        ));
                        tracing::warn!(path = %path.display(), "meta 键不一致，跳过该盘");
                    }
                    Err(e) => {
                        corrupt = Some(e.to_string());
                        tracing::warn!(path = %path.display(), error = %e, "meta 解码失败，跳过该盘");
                    }
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    // 评审 R6：单盘 IO 错误（EIO/EACCES）降级为跳过该盘——与解码损坏同口径，
                    // 其余副本仍可服务；全盘不可用且见过错误 → 仍 fail-closed（corrupt 分支）
                    corrupt = Some(e.to_string());
                    tracing::warn!(path = %path.display(), error = %e, "meta 读取失败，跳过该盘");
                }
            }
        }
        match best {
            Some((_, m)) => Ok(Some(m)),
            None => match corrupt {
                Some(why) => Err(Error::CorruptMeta(why)),
                None => Ok(None),
            },
        }
    }

    /// 集群 meta 读 = 覆盖率快路径（Task 6 / 计划 D1）：并发问本机各盘 + 全 peer，
    /// **覆盖 ≥ Q_s 即取最新并停**（取消掉队者——SIGSTOP 节点不再拖尾延迟）；
    /// **收完全部仍覆盖 < Q_s → Err(CorruptMeta)**（fail-closed；内联对象的数据也在 meta
    /// 里，覆盖不足时返回旧副本等于破坏 quorum 读语义——验收⑤的 500 口径同此）。
    /// 相交证明：提交 ≥Q_s 与读覆盖 ≥Q_s 在槽位域必相交 → 最新已提交版本必在覆盖内；
    /// 覆盖 ≥Q_s 且无副本 → 缺席可证明（任何已提交副本都落在覆盖交集里）。
    /// `full=false` 快路径提前停；`full=true` 收满全部响应（get_object 的回退一次）。
    async fn read_meta_cluster(
        &self,
        bucket: &str,
        key: &str,
        full: bool,
    ) -> Result<Option<XlMeta>> {
        use futures::StreamExt as _;
        let cl = self.cfg.cluster.as_deref().expect("cluster 分支");
        let q_s = self.cfg.ec.data as usize + 1;
        let self_id = cl.sets.flat.self_node().id.clone();

        let mut best: Option<((u64, i64), XlMeta)> = None;
        let mut corrupt: Option<String> = None;
        let mut covered = 0usize;

        // 本机各盘扫描（就地计数：NotFound 也是确定性观察；IO/解码错误不计）
        for drive in &self.cfg.drives {
            if !drives::drive_up(drive).await {
                continue;
            }
            let path = layout::object_dir(drive, bucket, key)?.join(meta::META_FILE);
            match tokio::fs::read(&path).await {
                Ok(bytes) => match XlMeta::decode(&bytes) {
                    Ok(m) if m.object_key == key => {
                        covered += 1;
                        let t = m.rank();
                        if best.as_ref().is_none_or(|(bt, _)| t > *bt) {
                            best = Some((t, m));
                        }
                    }
                    Ok(m) => {
                        corrupt = Some(format!(
                            "键不一致：请求 {key:?}，xl.meta 中为 {:?}",
                            m.object_key
                        ));
                    }
                    Err(e) => corrupt = Some(e.to_string()),
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => covered += 1,
                Err(e) => corrupt = Some(e.to_string()),
            }
        }

        // 远端并发；每到一个响应即累计覆盖，够数立刻出循环（未完成的 future 直接丢弃）
        if covered < q_s {
            let mut futs = futures::stream::FuturesUnordered::new();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                let r = crate::mesh::MetaRead {
                    bucket: bucket.into(),
                    key: key.into(),
                };
                let node = n.id.clone();
                futs.push(async move {
                    let res = cl.transport.read_meta(&node, r).await;
                    (node, res)
                });
            }
            while let Some((node, res)) = futs.next().await {
                match res {
                    Ok(snap) => {
                        covered += snap.slots_ok as usize;
                        if let Some(meta_bytes) = snap.meta {
                            match XlMeta::decode(&meta_bytes) {
                                Ok(m) if m.object_key == key => {
                                    let t = m.rank();
                                    if best.as_ref().is_none_or(|(bt, _)| t > *bt) {
                                        best = Some((t, m));
                                    }
                                }
                                Ok(m) => {
                                    corrupt = Some(format!(
                                        "远端 {node} 键不一致：xl.meta 中为 {:?}",
                                        m.object_key
                                    ));
                                }
                                Err(e) => corrupt = Some(format!("远端 {node} 解码失败: {e}")),
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(node = %node, error = %e, "远端 meta 读取失败（覆盖不计）")
                    }
                }
                if covered >= q_s && !full {
                    break; // 取消掉队者（回退模式收满全部）
                }
            }
        }

        if covered < q_s {
            return Err(Error::CorruptMeta(format!(
                "meta 覆盖不足 quorum：{covered}/{q_s}（分区/节点不可达时拒绝读，fail-closed）"
            )));
        }
        match best {
            Some((_, m)) => Ok(Some(m)),
            None => match corrupt {
                Some(why) => Err(Error::CorruptMeta(why)),
                None => Ok(None),
            },
        }
    }

    /// 全量扇出版（不提前停）：get_object 在快路径集合的数据轮失败后的**回退一次**——
    /// 防「提前停时恰有掉队者持最新已提交版本」的小窗口（Task 6 末位评审项）。
    /// 单节点退化为普通 read_meta。
    async fn read_meta_full(&self, bucket: &str, key: &str) -> Result<Option<XlMeta>> {
        if self.cfg.cluster.is_some() {
            return self.read_meta_cluster(bucket, key, true).await;
        }
        self.read_meta(bucket, key).await
    }

    /// 单盘写 meta：`xl.meta.new` 写 + fsync，再 rename 覆盖 `xl.meta`（该盘的提交点）。
    /// `fsync=true` 时在 rename 之后补**目录 fsync**（M2b-2 Task 9：rename 的持久化屏障）。
    pub(in crate::store) async fn write_meta_on(
        &self,
        drive: &Path,
        bucket: &str,
        key: &str,
        m: &XlMeta,
    ) -> Result<()> {
        let dir = layout::object_dir(drive, bucket, key)?;
        // 目录可能不存在：版本控制 delete marker 可在「键从未有过数据目录」时落 meta
        // （put/mpu 都在桶窗口预建目录；marker 路径没有）——与多盘/集群分支对齐
        tokio::fs::create_dir_all(&dir).await?;
        let new_path = dir.join(meta::META_NEW_FILE);
        let mut f = tokio::fs::File::create(&new_path).await?;
        f.write_all(&m.encode()?).await?;
        f.flush().await?;
        if self.cfg.fsync {
            f.sync_all().await?;
        }
        drop(f);
        tokio::fs::rename(&new_path, dir.join(meta::META_FILE)).await?;
        if self.cfg.fsync {
            Self::sync_dir(&dir).await;
        }
        Ok(())
    }

    /// 目录 fsync（rename 的持久化屏障）。部分文件系统对目录 O_RDONLY fsync 返回 EINVAL——
    /// 忽略并 warn：目录 fsync 的物理保证需按盘上文件系统复核（ext4/XFS 之外的语义不在 M2b 范围）。
    pub(in crate::store) async fn sync_dir(dir: &Path) {
        if let Ok(f) = tokio::fs::File::open(dir).await
            && let Err(e) = f.sync_all().await
        {
            tracing::warn!(dir = %dir.display(), error = %e, "目录 fsync 失败（容忍；断电保证需按文件系统复核）");
        }
    }

    /// 写 meta（提交点），返回成功翻转的槽位数。
    ///
    /// 三分支（Task 6）：
    /// - **集群**（cluster Some）：两阶段 + **翻转 ≥ Q_s 才提交**（评审 P1：对端 RPC
    ///   超时/崩溃是常态——「≥1 翻转即提交」会让已 ack 的写从读视角消失）+ 失败重试
    ///   一次 + 不足**补偿回滚**（写者持 `old`）。见 [`Self::write_meta_cluster`]。
    /// - 单盘（drives.len()==1，未配集群）：唯一一份 meta 就是提交点（M2b-1 原样）。
    /// - 单节点多盘：M2b-2 两阶段（prepare 全盘 ≥ data+1 → flip；**≥1 翻转即已提交**，
    ///   本机读扫全部副本因此可见；kill9 的 `FLIP_K` 测试口径依赖此语义，勿动）。
    pub(in crate::store) async fn write_meta(
        &self,
        bucket: &str,
        key: &str,
        m: &XlMeta,
        old: Option<&XlMeta>,
    ) -> Result<usize> {
        if self.cfg.cluster.is_some() {
            return self.write_meta_cluster(bucket, key, m, old).await;
        }
        if self.cfg.drives.len() == 1 {
            self.write_meta_on(&self.cfg.drives[0], bucket, key, m)
                .await?;
            return Ok(1);
        }
        let quorum = self.cfg.ec.data as usize + 1;
        let bytes = m.encode()?;

        // 阶段一：准备（掉线盘先探测——挂载点目录仍在父 fs 上，盲写会填满根盘）
        let mut prepared: Vec<&PathBuf> = Vec::new();
        for drive in &self.cfg.drives {
            if !drives::drive_up(drive).await {
                tracing::warn!(drive = %drive.display(), "盘不在册，跳过 meta 准备");
                continue;
            }
            let dir = layout::object_dir(drive, bucket, key)?;
            let res: Result<()> = async {
                tokio::fs::create_dir_all(&dir).await?;
                let mut f = tokio::fs::File::create(dir.join(meta::META_NEW_FILE)).await?;
                f.write_all(&bytes).await?;
                f.flush().await?;
                if self.cfg.fsync {
                    f.sync_all().await?;
                }
                Ok(())
            }
            .await;
            match res {
                Ok(()) => prepared.push(drive),
                Err(e) => tracing::warn!(drive = %drive.display(), error = %e, "meta 准备失败"),
            }
        }
        if prepared.len() < quorum {
            for drive in &prepared {
                if let Ok(dir) = layout::object_dir(drive, bucket, key) {
                    let _ = tokio::fs::remove_file(dir.join(meta::META_NEW_FILE)).await;
                }
            }
            self.bump_quorum_unmet_write();
            return Err(Error::QuorumUnmet(format!(
                "meta 准备 {}/{quorum}，未提交",
                prepared.len()
            )));
        }

        // 阶段二：翻转
        // 测试注入（Task 9 扩展）：准备完成即崩 / 翻到第 k 盘即崩——覆盖两阶段协议的两个核心窗口
        let flip_abort: Option<usize> = std::env::var("OSS_TEST_ABORT_META_FLIP_K")
            .ok()
            .and_then(|v| v.parse().ok());
        if std::env::var("OSS_TEST_ABORT_META_PREPARE").is_ok() {
            std::process::abort();
        }
        let mut flipped = 0usize;
        for drive in &prepared {
            let dir = layout::object_dir(drive, bucket, key)?;
            match tokio::fs::rename(dir.join(meta::META_NEW_FILE), dir.join(meta::META_FILE)).await
            {
                Ok(()) => {
                    flipped += 1;
                    if flip_abort == Some(flipped) {
                        std::process::abort();
                    }
                }
                Err(e) => tracing::warn!(drive = %drive.display(), error = %e, "meta 翻转失败"),
            }
        }
        Ok(flipped)
    }

    /// 集群 meta 两阶段（Task 6，计划 C3 伪代码逐条落）：
    /// prepare 全可达槽位 ≥ Q_s → commit（失败重试一次）→ **翻转 ≥ Q_s 即已提交**；
    /// 仍不足 → **补偿回滚**（写者持 old：有 → 恢复旧 meta；无 → 删 meta）。
    /// hash 只计「内容是自己的」的翻转（无 fencing 交错写下 .new 可能被他人覆盖）。
    async fn write_meta_cluster(
        &self,
        bucket: &str,
        key: &str,
        m: &XlMeta,
        old: Option<&XlMeta>,
    ) -> Result<usize> {
        let cl = self.cfg.cluster.as_deref().expect("cluster 分支");
        let self_id = cl.sets.flat.self_node().id.clone();
        let q_s = self.cfg.ec.data as usize + 1;
        let bytes = bytes::Bytes::from(m.encode()?);
        let self_hash = crate::mesh::content_hash(&bytes);
        let base = self.slot_base();

        // ---- 阶段一：prepare ----
        let mut prepared_local: Vec<u16> = Vec::new();
        for (d, drive) in self.cfg.drives.iter().enumerate() {
            if !drives::drive_up(drive).await {
                tracing::warn!(drive = %drive.display(), "盘不在册，跳过 meta 准备");
                continue;
            }
            match self.write_meta_new_on(drive, bucket, key, &bytes).await {
                Ok(()) => prepared_local.push(base + d as u16),
                Err(e) => tracing::warn!(drive = %drive.display(), error = %e, "meta 准备失败"),
            }
        }
        let mw = crate::mesh::MetaWrite {
            bucket: bucket.into(),
            key: key.into(),
            meta: bytes.clone(),
        };
        let mut prepared_remote = 0usize;
        for n in &cl.sets.flat.nodes {
            if n.id == self_id {
                continue;
            }
            match cl.transport.prepare_meta(&n.id, mw.clone()).await {
                Ok(ack) => prepared_remote += ack.slots.iter().filter(|s| s.ok).count(),
                Err(e) => tracing::warn!(node = %n.id, error = %e, "meta 准备 RPC 失败"),
            }
        }
        let prepared = prepared_local.len() + prepared_remote;
        if prepared < q_s {
            // 一盘未翻转：清 .new（best-effort）→ QuorumUnmet，调用方回滚数据目录
            for drive in &self.cfg.drives {
                if let Ok(dir) = layout::object_dir(drive, bucket, key) {
                    let _ = tokio::fs::remove_file(dir.join(meta::META_NEW_FILE)).await;
                }
            }
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                let _ = cl.transport.abort_meta(&n.id, mw.clone()).await;
            }
            self.bump_quorum_unmet_write();
            return Err(Error::QuorumUnmet(format!(
                "meta 集群准备 {prepared}/{q_s}，未提交"
            )));
        }

        // ---- 阶段二：commit（本机单次；远端每节点至多两次机会——rename 幂等，.new 仍在） ----
        let mut flipped_local: Vec<u16> = Vec::new();
        for (d, drive) in self.cfg.drives.iter().enumerate() {
            let slot = base + d as u16;
            if !prepared_local.contains(&slot) {
                continue;
            }
            match self.commit_meta_on(drive, bucket, key).await {
                Ok((true, h)) if h == self_hash => flipped_local.push(slot),
                Ok((true, _)) => {
                    tracing::warn!(slot, "meta 翻转内容是他人写入（hash 不符），不计入")
                }
                Ok((false, _)) => tracing::warn!(slot, "meta 翻转失败（无 .new）"),
                Err(e) => tracing::warn!(slot, error = %e, "meta 翻转失败"),
            }
        }
        let mut flipped_remote: std::collections::BTreeSet<u16> = std::collections::BTreeSet::new();
        for n in &cl.sets.flat.nodes {
            if n.id == self_id {
                continue;
            }
            for attempt in 0..2 {
                match cl.transport.commit_meta(&n.id, mw.clone()).await {
                    Ok(ack) => {
                        let mut got = false;
                        for s in ack.slots.iter().filter(|s| s.ok) {
                            if s.hash == self_hash {
                                flipped_remote.insert(s.slot);
                                got = true;
                            } else {
                                tracing::warn!(slot = s.slot, node = %n.id, "meta 翻转内容是他人写入（hash 不符），不计入");
                            }
                        }
                        if got || attempt == 1 {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(node = %n.id, error = %e, "meta commit RPC 失败");
                        if attempt == 1 {
                            break;
                        }
                    }
                }
            }
        }
        let flipped = flipped_local.len() + flipped_remote.len();
        if flipped >= q_s {
            if flipped < prepared {
                tracing::warn!(
                    flipped,
                    prepared,
                    "meta 集群翻转 < 已准备（已提交，M4 heal 补副本）"
                );
            }
            return Ok(flipped);
        }

        // ---- 补偿回滚：**必须做**——已翻 ≥1 时任一 ≥Q_s 读集必含已翻副本，
        // 不补偿会让「已返回失败」的写被读到（评审 P1）
        // 先清全节点的 .new 残骸（未翻槽位的暂存；翻过的槽位 .new 已被 rename 消费）
        for drive in &self.cfg.drives {
            if let Ok(dir) = layout::object_dir(drive, bucket, key) {
                let _ = tokio::fs::remove_file(dir.join(meta::META_NEW_FILE)).await;
            }
        }
        for n in &cl.sets.flat.nodes {
            if n.id == self_id {
                continue;
            }
            let _ = cl.transport.abort_meta(&n.id, mw.clone()).await;
        }
        for slot in &flipped_local {
            let d = (slot - base) as usize;
            let Some(drive) = self.cfg.drives.get(d) else {
                continue;
            };
            let restored = match old {
                Some(om) => self.write_meta_on(drive, bucket, key, om).await,
                None => {
                    let dir = layout::object_dir(drive, bucket, key)?;
                    let _ = tokio::fs::remove_file(dir.join(meta::META_FILE)).await;
                    Ok(())
                }
            };
            if let Err(e) = restored {
                tracing::error!(slot, error = %e, "meta 补偿恢复失败（留痕待 fsck）");
            }
        }
        for slot in &flipped_remote {
            let node_idx = cl.sets.flat.node_of_slot(*slot);
            let Some(node) = cl.sets.flat.nodes.get(node_idx) else {
                continue;
            };
            let restored: Result<()> = match old {
                Some(om) => {
                    // 用旧内容重跑 prepare+commit（rename 幂等；已翻槽位被恢复，未翻槽位 no-op）
                    let ow = crate::mesh::MetaWrite {
                        bucket: bucket.into(),
                        key: key.into(),
                        meta: bytes::Bytes::from(om.encode()?),
                    };
                    match cl.transport.prepare_meta(&node.id, ow.clone()).await {
                        Ok(_) => cl.transport.commit_meta(&node.id, ow).await.map(|_| ()),
                        Err(e) => Err(e),
                    }
                }
                None => {
                    cl.transport
                        .delete_meta(
                            &node.id,
                            crate::mesh::MetaRead {
                                bucket: bucket.into(),
                                key: key.into(),
                            },
                        )
                        .await
                }
            };
            if let Err(e) = restored {
                tracing::error!(node = %node.id, slot, error = %e, "meta 补偿恢复失败（留痕待 fsck）");
            }
        }
        self.bump_quorum_unmet_write();
        Err(Error::QuorumUnmet(format!(
            "meta 集群翻转 {flipped}/{q_s}，已补偿回滚"
        )))
    }

    /// 清掉各盘对象目录里的 `xl.meta.new` 残骸（评审 R7：翻转全失败/半途遗留的暂存 meta）。
    /// best-effort——残骸无害（永不被读为 meta），这里只是顺手收干净。
    pub(in crate::store) async fn clear_meta_new(&self, bucket: &str, key: &str) {
        for drive in &self.cfg.drives {
            if let Ok(dir) = layout::object_dir(drive, bucket, key) {
                let _ = tokio::fs::remove_file(dir.join(meta::META_NEW_FILE)).await;
            }
        }
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        // 与 put 同键串行：删除进行中不得有写者把新版本 rename 进来（否则版本目录会被连带删掉）
        let _guard = self.lock_object(bucket, key).await?;
        self.purge_key(bucket, key).await
    }

    /// 整键清理（meta 可见性提交 + 数据目录）：[`Self::delete_object`] 与版本链清空
    /// （M4 切片 3 的「删最后一个版本」）共用。**调用方须已持对象锁**。
    pub(in crate::store) async fn purge_key(&self, bucket: &str, key: &str) -> Result<()> {
        // 评审 R2：不在册盘上的副本删不到——静默成功会让「插回后对象复活」（内联对象直接
        // 200 返回已删字节，EC 对象转永久 500）。fail-closed：任一配置盘不在册 → 拒绝删除。
        for drive in &self.cfg.drives {
            if !drives::drive_up(drive).await {
                return Err(Error::QuorumUnmet(format!(
                    "盘 {} 不在册，删除中止（防插回复活）",
                    drive.display()
                )));
            }
        }
        // ① 可见性提交点（与写入协议对偶——读只信 xl.meta，删也以 meta 为准）：
        // 各盘依次删 xl.meta（连同 .new 残骸）。此前读者读到旧 meta 仍见旧对象（读己之写），
        // 此后一律 NoSuchKey——**不会**再出现「meta 还在、分片被逐个删掉」的中间态
        // （那会让严格轮读报 CorruptMeta，并发删除的裂口由此关闭）。失败即中止（可重试）。
        for drive in &self.cfg.drives {
            let dir = layout::object_dir(drive, bucket, key)?;
            for f in [meta::META_FILE, meta::META_NEW_FILE] {
                match tokio::fs::remove_file(dir.join(f)).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        // ①' 集群：远端 meta 删除是可见性提交点的远端分派——**失败即上抛**（评审：
        //     best-effort 吞掉会让对象在部分节点「复活」而删除已报成功；不可达同样上抛=fail-closed）
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                cl.transport
                    .delete_meta(
                        &n.id,
                        crate::mesh::MetaRead {
                            bucket: bucket.into(),
                            key: key.into(),
                        },
                    )
                    .await?;
            }
        }
        // ② 数据清理：删各盘对象目录（meta 已除，失败只留不可见残骸，由 GC 按 mtime 回收）
        for drive in &self.cfg.drives {
            let dir = layout::object_dir(drive, bucket, key)?;
            match tokio::fs::remove_dir_all(&dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(dir = %dir.display(), error = %e, "删除数据目录失败（对象已不可见，残骸由 GC 回收）");
                }
            }
        }
        // ②' 集群：远端对象目录清理（幂等；失败 warn——对象已不可见，残骸由该节点 GC 按龄回收）
        if let Some(cl) = self.cfg.cluster.as_deref() {
            let self_id = cl.sets.flat.self_node().id.clone();
            for n in &cl.sets.flat.nodes {
                if n.id == self_id {
                    continue;
                }
                let d = crate::mesh::VersionDirRef {
                    bucket: bucket.into(),
                    key: key.into(),
                    uuid: String::new(), // 空 = 整对象目录
                };
                if let Err(e) = cl.transport.delete_version(&n.id, d).await {
                    tracing::warn!(node = %n.id, error = %e, "远端对象目录清理失败（残骸由该节点 GC 回收）");
                }
            }
        }
        Ok(())
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<ObjectInfo> {
        // ponytail: 读出整份字节再写回（内存中介）。EC 读已分块流式，但 copy 的 Bytes API
        // 仍物化整份；分片级服务端复制（不过内存）是后续独立项。
        let (info, bytes) = self.get_object(src_bucket, src_key).await?;
        // 默认 metadata-directive=COPY：元数据随对象走（REPLACE 指令不在 M2a 切片）
        self.put_object_with_meta(
            dst_bucket,
            dst_key,
            bytes,
            info.content_type,
            info.user_metadata,
        )
        .await
    }
}

/// 拓扑记录的唯一构造器（M4 切片 5；切片 8 加 set）：写盘（`Store::open`）与控制台展示
/// （`/api/v1/cluster/status` 的 topo_fingerprint）共用——两处再漂移即回归（verifier F9）。
/// **set==0 恒写 v2（逐字节兼容）；set>0 写 v3**（slot = **组内槽**）。
/// 读侧 `parse_topo_record` 兼容 v1/v2（均视为 set 0）。
pub fn topo_record(set: u16, node: &str, slot: u16) -> String {
    if set == 0 {
        format!("v2 node={node} slot={slot}")
    } else {
        format!("v3 set={set} node={node} slot={slot}")
    }
}

/// 拓扑记录解析（v1/v2/v3 兼容）：返回 `(set, node, slot)`；v1/v2 均视为 set 0。
/// v1 形如 `v1 slots=12 node=n1 slot=0`（slots 段被忽略）；v2 = set0；v3 形如
/// `v3 set=1 node=n1 slot=0`。**`v3` 且 set=0 = 拒启**（写路径永不产出该形态）。
/// 不把版本前缀当防线（R2）：全等比对按解析出的三元组。
fn parse_topo_record(s: &str) -> Option<(u16, String, u16)> {
    let mut node = None;
    let mut slot = None;
    let mut set: Option<u16> = None;
    let mut is_v3 = false;
    for part in s.split_whitespace() {
        if part == "v3" {
            is_v3 = true;
        }
        if let Some(v) = part.strip_prefix("node=") {
            node = Some(v.to_string());
        }
        if let Some(v) = part.strip_prefix("slot=") {
            slot = v.parse::<u16>().ok();
        }
        if let Some(v) = part.strip_prefix("set=") {
            set = v.parse::<u16>().ok();
        }
    }
    // P3-2 不变量：写路径 **set= 字段存在 ⟺ v3**——v1/v2 带 set=（或 v3 缺 set=）都是
    // 人为错改的形态，一律拒收（不再只靠「v3+set0」单点判）。全等比对仍按解析三元组（R2）。
    if set.is_some() != is_v3 {
        return None;
    }
    let set = set.unwrap_or(0);
    if is_v3 && set == 0 {
        return None; // 拒启：v3+set0 是人为错改的形态
    }
    Some((set, node?, slot?))
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_crypto;
