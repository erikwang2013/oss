//! 存储内核。M1：单盘、无纠删码，但**提交协议与 xl.meta 格式按最终设计落**，
//! M2 只是把「一个 part 一个文件」换成「一个 part 若干分片」。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, ChecksumRecord, XlMeta};

mod drives;
pub mod ec;
mod fsck;
mod gc;
mod list;
mod mesh_api;
mod mpu;
mod put;
mod read;

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
}

impl StoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            drives: vec![root.into()],
            fsync: true,
            ec: ec::EcConfig::default(),
            inline_threshold: meta::MAX_INLINE,
            cluster: None,
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
}

/// 对象锁守卫：进程内 tokio 互斥 + 跨进程 flock（`File` drop 即释放 flock）。
pub(in crate::store) struct LockGuard {
    _mem: tokio::sync::OwnedMutexGuard<()>,
    _file: std::fs::File,
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
            let n_self = cfg.cluster.as_ref().unwrap().topo.self_node();
            if n_self.drive_count as usize != n {
                return Err(Error::Encode(format!(
                    "cluster：本节点 {} 声明 {} 盘，实际 {n} 盘",
                    n_self.id, n_self.drive_count
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
        for drive in &cfg.drives {
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
        }

        let store = Self {
            cfg,
            locks: Mutex::new(HashMap::new()),
        };
        // 启动自检：清 tmp/trash 的崩溃残留（对象目录不碰——在途 MPU 暂存区按其 mtime 由 clean 回收）
        store.reap_transient().await?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        self.cfg.root()
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
        Ok(LockGuard {
            _mem: mem,
            _file: file,
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
        Ok(())
    }

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        layout::validate_bucket(bucket)?;
        for drive in &self.cfg.drives {
            if tokio::fs::try_exists(layout::bucket_dir(drive, bucket)).await? {
                return Ok(true);
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
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// 读 meta：逐盘读取，**最新（latest().mod_time_nanos 最大）者胜**。
    ///
    /// 多盘契约（铁律②读侧）：meta 在每盘各一份，多数情况内容相同；跨盘不一致 = 撕裂写
    /// 的中间态（某盘已翻转、某盘未翻转）——旧的那份必然指向旧版本，取最新即取已提交版本。
    /// 单盘 meta 损坏/键不一致 → warn 跳过（其余盘可服务）；**一个可用副本都没有但见过损坏
    /// 副本** → `Err(CorruptMeta)`（fail-closed，不把「盘上有坏 meta」伪装成 404）。
    async fn read_meta(&self, bucket: &str, key: &str) -> Result<Option<XlMeta>> {
        let mut best: Option<(i64, XlMeta)> = None;
        let mut corrupt: Option<String> = None;
        for drive in &self.cfg.drives {
            let path = layout::object_dir(drive, bucket, key)?.join(meta::META_FILE);
            match tokio::fs::read(&path).await {
                Ok(bytes) => match XlMeta::decode(&bytes) {
                    Ok(m) if m.object_key == key => {
                        let t = m.latest().map_or(i64::MIN, |v| v.mod_time_nanos);
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

    /// 写 meta（提交点），返回成功翻转的盘数。
    ///
    /// 单盘形态：唯一一份 meta 就是提交点，失败直接上抛（与 M2b-1 行为逐字节一致）。
    /// 多盘形态 = **两阶段提交**（M2b-2 裁决，取代「尽力而为计数」——那会让 quorum 不足时
    /// 出现「已翻转的 meta 不可回滚」的死角）：
    ///   阶段一（准备）：全盘写 `xl.meta.new` + fsync；准备数 < data+1 → 清 .new →
    ///     `Err(QuorumUnmet)`——**此时一盘未翻转**，调用方回滚数据目录即回到干净态。
    ///   阶段二（翻转）：对已准备的盘 rename `.new` → `xl.meta`；rename 近乎瞬时，
    ///     失败仅 warn（≥1 翻转即已提交，不可逆；全失败视同未提交，由调用方回滚）。
    pub(in crate::store) async fn write_meta(
        &self,
        bucket: &str,
        key: &str,
        m: &XlMeta,
    ) -> Result<usize> {
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
        Ok(())
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<ObjectInfo> {
        // ponytail: M1 读出整份字节再写回（内存中介，上限同 put）；M2 换分片级服务端复制，不过内存。
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

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
