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
mod gc;
mod list;
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
}

impl StoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            drives: vec![root.into()],
            fsync: true,
            ec: ec::EcConfig::default(),
            inline_threshold: meta::MAX_INLINE,
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
    /// 否则两个写者会把对方的版本目录当「老版本」扔进回收站。
    /// ponytail: 单进程锁；跨进程用 `.oss.sys/locks/<sha1(key)>.lock` 的 flock（设计 §4.4，M2/M3）
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
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
        if !cfg.ec.enabled {
            if n != 1 {
                return Err(Error::Encode(format!(
                    "ec.enabled=false（M1 旧形态）只支持单盘，当前 {n} 盘"
                )));
            }
            tracing::warn!("ec.enabled=false：单文件旧形态，无冗余、无内联");
        } else if n == 1 {
            tracing::warn!("单盘 EC：所有分片同盘，无冗余（多盘冗余需 data_dirs ≥ 12）");
        } else if n < (cfg.ec.data + cfg.ec.parity) as usize {
            return Err(Error::Encode(format!(
                "盘数 {n} 不足 EC {}+{}（多盘须 ≥ {}；单盘可运行但无冗余）",
                cfg.ec.data,
                cfg.ec.parity,
                cfg.ec.data + cfg.ec.parity
            )));
        }
        for drive in &cfg.drives {
            for sub in ["tmp", "trash", "locks"] {
                tokio::fs::create_dir_all(layout::sys_dir(drive, sub)).await?;
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
        let id = format!("{bucket}/{key}");
        let mut locks = self.locks.lock().unwrap();
        let lock = locks
            .entry(id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        // 回收无人持有的旧条目，锁表不随历史写过的键无限增长
        //（本键刚 clone 出来，count ≥ 2 必留；其余条目只为 map 自己持有时删掉）
        locks.retain(|_, v| Arc::strong_count(v) > 1);
        lock
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
        self.ensure_bucket(bucket).await?;
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
        // ponytail: 桶级互斥未实现——并发 delete_bucket + put 可让桶「复活」，M2 处理
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
        let mut out = Vec::new();
        let mut entries = match tokio::fs::read_dir(self.cfg.root()).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == layout::SYS_DIR {
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
                Err(e) => return Err(e.into()),
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
        let mut flipped = 0usize;
        for drive in &prepared {
            let dir = layout::object_dir(drive, bucket, key)?;
            match tokio::fs::rename(dir.join(meta::META_NEW_FILE), dir.join(meta::META_FILE)).await
            {
                Ok(()) => flipped += 1,
                Err(e) => tracing::warn!(drive = %drive.display(), error = %e, "meta 翻转失败"),
            }
        }
        Ok(flipped)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        // 与 put 同键串行：删除进行中不得有写者把新版本 rename 进来（否则版本目录会被连带删掉）
        let _guard = self.lock_for(bucket, key).lock_owned().await;
        // 多盘：每盘的完整对象目录（meta + 本盘分片）一并删；单盘即既有行为
        for drive in &self.cfg.drives {
            let dir = layout::object_dir(drive, bucket, key)?;
            match tokio::fs::remove_dir_all(&dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // S3 语义：删不存在的键也返回成功
                Err(e) => return Err(e.into()),
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
