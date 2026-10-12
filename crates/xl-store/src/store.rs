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

pub mod ec;
mod gc;
mod list;
mod mpu;
mod put;
mod read;

pub type ByteStream = std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub root: PathBuf,
    /// 提交点是否 fsync。测试可关（快），生产必须开。
    pub fsync: bool,
    /// M2b 写路径 EC 配置（默认 8+4）。读路径按 meta 里的 `DataLayout` 重建，不读这里。
    pub ec: ec::EcConfig,
    /// 小于此长度的对象内联进 meta（不落 part 文件）。默认 128 KiB。
    /// 不得越过 [`meta::MAX_INLINE`]（meta 解码信任边界），`open` 会拒绝。
    pub inline_threshold: u64,
}

impl StoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            fsync: true,
            ec: ec::EcConfig::default(),
            inline_threshold: meta::MAX_INLINE,
        }
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
        for sub in ["tmp", "trash", "locks"] {
            tokio::fs::create_dir_all(layout::sys_dir(&cfg.root, sub)).await?;
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
        &self.cfg.root
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

    /// 桶操作的统一前置。名字校验在此收口：桶目录是 `root.join(bucket)`，
    /// 不过滤 `..`/绝对路径就会让删桶、列举跑到盘根之外。
    async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        layout::validate_bucket(bucket)?;
        if tokio::fs::try_exists(layout::bucket_dir(&self.cfg.root, bucket)).await? {
            Ok(())
        } else {
            Err(Error::NoSuchBucket(bucket.to_string()))
        }
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        layout::validate_bucket(bucket)?;
        let dir = layout::bucket_dir(&self.cfg.root, bucket);
        match tokio::fs::create_dir(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(Error::BucketAlreadyExists(bucket.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        layout::validate_bucket(bucket)?;
        Ok(tokio::fs::try_exists(layout::bucket_dir(&self.cfg.root, bucket)).await?)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::bucket_dir(&self.cfg.root, bucket);
        let mut entries = tokio::fs::read_dir(&dir).await?;
        let non_empty = entries.next_entry().await?.is_some();
        drop(entries);
        // 桶里只要还有对象目录就拒绝（S3 语义）
        if non_empty {
            return Err(Error::BucketNotEmpty(bucket.to_string()));
        }
        // ponytail: 桶级互斥未实现——并发 delete_bucket + put 可让桶「复活」，M2 处理
        match tokio::fs::remove_dir(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                Err(Error::BucketNotEmpty(bucket.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let mut out = Vec::new();
        let mut entries = match tokio::fs::read_dir(&self.cfg.root).await {
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

    async fn read_meta(&self, bucket: &str, key: &str) -> Result<Option<XlMeta>> {
        let path = layout::object_dir(&self.cfg.root, bucket, key)?.join(meta::META_FILE);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let m = XlMeta::decode(&bytes)?;
                // 键一致性：目录名与 xl.meta 记录的键不符 = 元数据错位，宁可报损坏也不服务错误内容
                if m.object_key != key {
                    return Err(Error::CorruptMeta(format!(
                        "键不一致：请求 {key:?}，xl.meta 中为 {:?}",
                        m.object_key
                    )));
                }
                Ok(Some(m))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_meta(&self, bucket: &str, key: &str, m: &XlMeta) -> Result<()> {
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        let new_path = dir.join(meta::META_NEW_FILE);
        let mut f = tokio::fs::File::create(&new_path).await?;
        f.write_all(&m.encode()?).await?;
        f.flush().await?;
        if self.cfg.fsync {
            f.sync_all().await?;
        }
        drop(f);
        tokio::fs::rename(&new_path, dir.join(meta::META_FILE)).await?;
        Ok(())
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket(bucket).await?;
        // 与 put 同键串行：删除进行中不得有写者把新版本 rename 进来（否则版本目录会被连带删掉）
        let _guard = self.lock_for(bucket, key).lock_owned().await;
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // S3 语义：删不存在的键也返回成功
            Err(e) => Err(e.into()),
        }
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
