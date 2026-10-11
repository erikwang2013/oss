//! 存储内核。M1：单盘、无纠删码，但**提交协议与 xl.meta 格式按最终设计落**，
//! M2 只是把「一个 part 一个文件」换成「一个 part 若干分片」。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::Stream;
use md5::{Digest as _, Md5};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, PartRef, Version, XlMeta};

mod gc;
mod list;

pub type ByteStream = std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub root: PathBuf,
    /// 提交点是否 fsync。测试可关（快），生产必须开。
    pub fsync: bool,
}

impl StoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            fsync: true,
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

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<String>,
    ) -> Result<ObjectInfo> {
        self.put_object_with_meta(bucket, key, body, content_type, Default::default())
            .await
    }

    /// 带用户元数据的整份 PUT。metadata 是**全量替换**（S3 语义，不是合并）。
    pub async fn put_object_with_meta(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<ObjectInfo> {
        self.put_object_stream_with_meta(
            bucket,
            key,
            Box::pin(futures::stream::once(async move { Ok(body) })) as ByteStream,
            content_type,
            user_metadata,
        )
        .await
    }

    /// 提交协议（顺序不可变）：
    ///   1. 分片/数据写 `.oss.sys/tmp/<uuid>/part.1`，边写边算 MD5+SHA256+size
    ///   2. 候选 `xl.meta` 写进同一个 tmp 目录
    ///   3. **同盘 rename** tmp/<uuid> → <bucket>/<obj>/<uuid>     ← 数据就位
    ///   4. 写 `<obj>/xl.meta.new` 并 fsync，再 rename 覆盖 `xl.meta` ← **提交点**
    ///   5. 老版本目录 → `.oss.sys/trash/`（移入 trash，由 GC 回收——M1 未实现回收器）
    ///
    /// 3 与 4 之间崩溃 = 留下一个孤儿版本目录（不可见，M1 无回收器，残留只占空间）；
    /// 4 之后崩溃 = 老版本目录残留（不可见，同上）。读路径只信 xl.meta。
    ///
    /// M1 持久化假设：`fsync=true` 时数据文件与 `xl.meta.new` 都先做 file fsync；
    /// ext4/XFS 隐含提交先前的 rename，故提交点一次 rename 即可。**没有目录 fsync**，
    /// 换文件系统（或要求断电零丢失）需复核这两条假设。
    pub async fn put_object_stream(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        content_type: Option<String>,
    ) -> Result<ObjectInfo> {
        self.put_object_stream_with_meta(bucket, key, body, content_type, Default::default())
            .await
    }

    /// 流式 PUT 的完整形态（含用户元数据）；`put_object_stream` 是空元数据的薄壳。
    pub async fn put_object_stream_with_meta(
        &self,
        bucket: &str,
        key: &str,
        mut body: ByteStream,
        content_type: Option<String>,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        layout::validate_key(key)?;

        let _guard = self.lock_for(bucket, key).lock_owned().await;

        // M-6 裁决：损坏的 meta 让 PUT fail-closed（报错），不静默当作空对象覆盖——盘上可能还有旧数据，宁可失败也不毁
        let old_meta = self.read_meta(bucket, key).await?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let tmp = layout::sys_dir(&self.cfg.root, "tmp").join(&uuid);
        let now = now_nanos();

        // 1)+2) 数据与候选 meta 落 tmp；任何写失败/流错误都 best-effort 清掉本轮 tmp
        //（未提交前 tmp 不被任何 meta 引用，失败即弃；残留只占空间，由 `oss-server clean` 按 mtime 回收）
        let staged: Result<(u64, String, XlMeta)> = async {
            tokio::fs::create_dir_all(&tmp).await?;

            // 1) 流式写数据 + 增量哈希
            let part_path = tmp.join("part.1");
            let mut file = tokio::fs::File::create(&part_path).await?;
            let mut md5 = Md5::new();
            let mut sha = Sha256::new();
            let mut size: u64 = 0;
            while let Some(chunk) = futures::StreamExt::next(&mut body).await {
                let chunk = chunk?;
                md5.update(&chunk);
                sha.update(&chunk);
                size += chunk.len() as u64;
                file.write_all(&chunk).await?;
            }
            file.flush().await?;
            if self.cfg.fsync {
                file.sync_all().await?;
            }
            drop(file);

            let etag = hex::encode(md5.finalize());
            let version = Version {
                version_id: uuid::Uuid::nil(), // M1 无版本控制：nil = S3 的 "null" 版本
                mod_time_nanos: now,
                size,
                etag: etag.clone(),
                content_type: content_type.clone(),
                user_metadata: user_metadata.clone(),
                part: PartRef {
                    uuid: uuid.clone(),
                    part: 1,
                    size,
                    sha256: hex::encode(sha.finalize()),
                },
            };
            let meta = XlMeta {
                object_key: key.to_string(),
                versions: vec![version],
            };

            // 2) 候选 meta 落 tmp
            tokio::fs::write(tmp.join(meta::META_FILE), meta.encode()?).await?;
            Ok((size, etag, meta))
        }
        .await;
        let (size, etag, meta) = match staged {
            Ok(v) => v,
            Err(e) => {
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                return Err(e);
            }
        };

        // 3) 同盘 rename：数据就位（此时对读路径仍不可见）
        let obj_dir = layout::object_dir(&self.cfg.root, bucket, key)?;
        tokio::fs::create_dir_all(&obj_dir).await?;
        tokio::fs::rename(&tmp, obj_dir.join(&uuid)).await?;

        // 4) 提交点：写 xl.meta.new 再原子覆盖
        self.write_meta(bucket, key, &meta).await?;

        // 5) 老版本进回收站
        if let Some(old) = old_meta.as_ref().and_then(|m| m.latest()) {
            let old_dir = obj_dir.join(&old.part.uuid);
            let trash = layout::sys_dir(&self.cfg.root, "trash").join(&old.part.uuid);
            // best-effort：提交点已过，回收站的任何错误都不该让已提交的 PUT 报失败
            if matches!(tokio::fs::try_exists(&trash).await, Ok(true)) {
                let _ = tokio::fs::remove_dir_all(&trash).await;
            }
            let _ = tokio::fs::rename(&old_dir, &trash).await;
        }

        Ok(ObjectInfo {
            key: key.to_string(),
            size,
            etag,
            content_type,
            last_modified_nanos: now,
            user_metadata,
        })
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

    fn load_object_info(&self, bucket: &str, key: &str, m: &XlMeta) -> Result<ObjectInfo> {
        let v = m
            .latest()
            .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;
        Ok(ObjectInfo {
            key: key.to_string(),
            size: v.size,
            etag: v.etag.clone(),
            content_type: v.content_type.clone(),
            last_modified_nanos: v.mod_time_nanos,
            user_metadata: v.user_metadata.clone(),
        })
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;
        let m = self
            .read_meta(bucket, key)
            .await?
            .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
        self.load_object_info(bucket, key, &m)
    }

    /// 读对象：一次 meta 读取定 info 与数据文件位置。
    ///
    /// 读路径不加锁，meta 与数据文件之间隔着一次 open：覆写者在提交点之后才把老版本目录移进
    /// trash，所以先读到的 meta 可能指向刚被移走的版本目录（数据文件 ENOENT）。此时重读一次
    /// meta 拿新版本再试；两轮都读不到 = 对象确实不在（或被并发删除）→ NoSuchKey。
    /// ponytail: 固定重试一次；持续覆写风暴下不保证收敛（M2 快照读解决）。
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, Bytes)> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;

        for _ in 0..2 {
            let m = self
                .read_meta(bucket, key)
                .await?
                .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
            let v = m
                .latest()
                .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;
            let info = self.load_object_info(bucket, key, &m)?;
            let path = dir.join(&v.part.uuid).join(format!("part.{}", v.part.part));
            // ponytail: M1 整份读进内存；M2 换流式（大对象与 Range 是 M2 的事）。
            match tokio::fs::read(&path).await {
                Ok(bytes) => return Ok((info, Bytes::from(bytes))),
                // 旧版本目录已被并发覆写者移走：下一轮重读 meta 拿新版本
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            }
        }
        // 两轮 meta → 数据文件都 ENOENT：按 S3 语义报 NoSuchKey，而不是把裸 IO 错误抛给上层
        Err(Error::NoSuchKey(key.to_string()))
    }

    /// 有界读：`range` 为半开区间 `[start, end)`，须满足 `start <= end <= size`
    ///（越界由调用方——s3s 的 `Range::check`——先拦；内核仍防呆，防呆错误是内部错误）。
    /// 重试语义与 `get_object` 相同（并发覆写时重读 meta 一轮）。
    pub async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes> {
        self.ensure_bucket(bucket).await?;
        let dir = layout::object_dir(&self.cfg.root, bucket, key)?;

        for _ in 0..2 {
            let m = self
                .read_meta(bucket, key)
                .await?
                .ok_or_else(|| Error::NoSuchKey(key.to_string()))?;
            let v = m
                .latest()
                .ok_or_else(|| Error::CorruptMeta(format!("{bucket}/{key}: 无版本")))?;

            if range.start > range.end || range.end > v.size {
                return Err(Error::InvalidKey(format!(
                    "range {range:?} 越界（对象 {} 字节）",
                    v.size
                )));
            }

            // ponytail: M2a 单 part（Task 5 起按 parts 序列跨 part 映射）；seek + 有界读，不整份进内存。
            let path = dir.join(&v.part.uuid).join(format!("part.{}", v.part.part));
            let mut file = match tokio::fs::File::open(&path).await {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            file.seek(std::io::SeekFrom::Start(range.start)).await?;
            let mut buf = vec![0u8; (range.end - range.start) as usize];
            match file.read_exact(&mut buf).await {
                // tokio 的 read_exact 返回 io::Result<usize>（与 std 的 Result<()> 不同）
                Ok(_) => return Ok(Bytes::from(buf)),
                // 版本目录被并发覆写者移走/截断：下一轮重读 meta 拿新版本
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::NoSuchKey(key.to_string()))
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
