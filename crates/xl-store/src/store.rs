//! 存储内核。M1：单盘、无纠删码，但**提交协议与 xl.meta 格式按最终设计落**，
//! M2 只是把「一个 part 一个文件」换成「一个 part 若干分片」。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::Stream;
use md5::{Digest as _, Md5};
use sha2::Sha256;
use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, PartRef, Version, XlMeta};

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
        Ok(Self {
            cfg,
            locks: Mutex::new(HashMap::new()),
        })
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
        // ponytail: 桶级互斥未实现——并发 delete_bucket + put 可让桶「复活」，Task 6/M2 处理
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
        self.put_object_stream(
            bucket,
            key,
            Box::pin(futures::stream::once(async move { Ok(body) })) as ByteStream,
            content_type,
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
    /// 3 与 4 之间崩溃 = 留下一个孤儿版本目录（不可见，启动自检回收）；
    /// 4 之后崩溃 = 老版本目录残留（不可见，clean 回收）。读路径只信 xl.meta。
    ///
    /// M1 持久化假设：`fsync=true` 时数据文件与 `xl.meta.new` 都先做 file fsync；
    /// ext4/XFS 隐含提交先前的 rename，故提交点一次 rename 即可。**没有目录 fsync**，
    /// 换文件系统（或要求断电零丢失）需复核这两条假设。
    pub async fn put_object_stream(
        &self,
        bucket: &str,
        key: &str,
        mut body: ByteStream,
        content_type: Option<String>,
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
        //（未提交前 tmp 不被任何 meta 引用，失败即弃；残留也只占空间，由启动自检回收）
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
                user_metadata: Default::default(),
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
            user_metadata: Default::default(),
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

    // Task 5 的测试要一个「提交后是否可见」的探针，head_object 与其组装函数随本 task 落地；
    // get / delete / list / copy 仍留给 Task 6。
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
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
        (s, dir)
    }

    #[tokio::test]
    async fn open_creates_sys_dirs() {
        let (_, dir) = store().await;
        for sub in ["tmp", "trash", "locks"] {
            assert!(dir.path().join(layout::SYS_DIR).join(sub).is_dir());
        }
    }

    #[tokio::test]
    async fn open_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        Store::open(StoreConfig::new(dir.path())).await.unwrap();
        Store::open(StoreConfig::new(dir.path())).await.unwrap();
    }

    #[tokio::test]
    async fn create_and_list_buckets() {
        let (s, _d) = store().await;
        // 倒序创建：断言要证明的是「按名字排序」，不是创建顺序
        s.create_bucket("beta").await.unwrap();
        s.create_bucket("alpha").await.unwrap();
        let names: Vec<String> = s
            .list_buckets()
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.name)
            .collect();
        assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[tokio::test]
    async fn list_buckets_missing_root_is_empty() {
        // 盘根整个消失（挂载点没了/被带外清掉）：列举返回空，不报错
        let (s, dir) = store().await;
        std::fs::remove_dir_all(dir.path()).unwrap();
        assert!(s.list_buckets().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_bucket_is_idempotent_error() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        assert!(matches!(
            s.create_bucket("alpha").await,
            Err(Error::BucketAlreadyExists(_))
        ));
    }

    #[tokio::test]
    async fn create_bucket_rejects_bad_name() {
        let (s, _d) = store().await;
        assert!(matches!(
            s.create_bucket("A_B").await,
            Err(Error::InvalidBucketName(_))
        ));
    }

    #[tokio::test]
    async fn delete_bucket_requires_empty() {
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        // Task 5 前没有 put_object：直接摆一个对象目录（含内容）来制造「非空」。
        let obj_dir = layout::object_dir(d.path(), "alpha", "k.txt").unwrap();
        tokio::fs::create_dir_all(&obj_dir).await.unwrap();
        tokio::fs::write(obj_dir.join("part.1"), b"hi")
            .await
            .unwrap();
        assert!(matches!(
            s.delete_bucket("alpha").await,
            Err(Error::BucketNotEmpty(_))
        ));
        tokio::fs::remove_dir_all(&obj_dir).await.unwrap();
        s.delete_bucket("alpha").await.unwrap();
        assert!(!s.bucket_exists("alpha").await.unwrap());
    }

    #[tokio::test]
    async fn missing_bucket_is_reported() {
        let (s, _d) = store().await;
        // put_object 属 Task 5；ensure_bucket 是同一前置，先用 delete_bucket 验它。
        assert!(matches!(
            s.delete_bucket("nope").await,
            Err(Error::NoSuchBucket(_))
        ));
    }

    #[tokio::test]
    async fn ensure_bucket_rejects_traversal() {
        // ensure_bucket 内的桶名校验是删桶/列举的收口点：`..` 若放行，delete_bucket 会去删盘根之外
        let (s, _d) = store().await;
        assert!(matches!(
            s.delete_bucket("..").await,
            Err(Error::InvalidBucketName(_))
        ));
        assert!(matches!(
            s.delete_bucket("A_B").await,
            Err(Error::InvalidBucketName(_))
        ));
    }

    // ---- Task 5: put_object 提交协议 ----

    #[tokio::test]
    async fn put_then_head() {
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let info = s
            .put_object(
                "alpha",
                "dir/hello.txt",
                bytes::Bytes::from_static(b"hello world"),
                Some("text/plain".into()),
            )
            .await
            .unwrap();
        assert_eq!(info.size, 11);
        // 单段 PUT 的 ETag 就是整份 MD5
        assert_eq!(info.etag, "5eb63bbbe01eeed093cb22bb8f5acdc3");
        assert_eq!(info.content_type.as_deref(), Some("text/plain"));

        let head = s.head_object("alpha", "dir/hello.txt").await.unwrap();
        assert_eq!(head.etag, info.etag);

        // 盘上数据校验：meta 记录的 part.uuid 目录里，part.1 必须就是写进去的字节
        let obj_dir = layout::object_dir(d.path(), "alpha", "dir/hello.txt").unwrap();
        let on_disk = XlMeta::decode(
            &tokio::fs::read(obj_dir.join(meta::META_FILE))
                .await
                .unwrap(),
        )
        .unwrap();
        let part = obj_dir
            .join(&on_disk.latest().unwrap().part.uuid)
            .join("part.1");
        assert_eq!(
            tokio::fs::read(&part).await.unwrap(),
            b"hello world".to_vec()
        );
    }

    #[tokio::test]
    async fn put_with_fsync_disabled_still_commits() {
        // cfg.fsync=false 是提交路径上的另一条分支（测试快、生产必须开），必须同样可提交可读
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = StoreConfig::new(dir.path());
        cfg.fsync = false;
        let s = Store::open(cfg).await.unwrap();
        s.create_bucket("alpha").await.unwrap();
        let info = s
            .put_object("alpha", "k", Bytes::from_static(b"no-fsync"), None)
            .await
            .unwrap();
        assert_eq!(info.size, 8);
        assert_eq!(s.head_object("alpha", "k").await.unwrap().size, 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_streams_without_buffering_whole_body() {
        // 握手式断言：第一块发出后，盘上 tmp/<uuid>/part.1 必须已含它的字节，之后才放行剩余块。
        // 若实现把整份 body 缓冲到流结束再写，这里的轮询必然等不到而失败——这才是真流式的判别点。
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let s = std::sync::Arc::new(s);
        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes>>(1);
        let put = tokio::spawn({
            let s = std::sync::Arc::clone(&s);
            async move {
                s.put_object_stream("alpha", "big.bin", Box::pin(rx), None)
                    .await
            }
        });

        let first = Bytes::from(vec![b'a'; 1024]);
        futures::SinkExt::send(&mut tx, Ok(first.clone()))
            .await
            .unwrap();
        let tmp_root = layout::sys_dir(d.path(), "tmp");
        let mut on_disk = false;
        for _ in 0..200 {
            if let Ok(mut entries) = tokio::fs::read_dir(&tmp_root).await {
                while let Ok(Some(e)) = entries.next_entry().await {
                    if let Ok(bytes) = tokio::fs::read(e.path().join("part.1")).await
                        && bytes == first.as_ref()
                    {
                        on_disk = true;
                        break;
                    }
                }
            }
            if on_disk {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            on_disk,
            "第一块在 body 结束前没有落盘：实现把整份 body 缓冲了"
        );

        // 放行剩余块并收尾
        futures::SinkExt::send(&mut tx, Ok(Bytes::from(vec![b'a'; 1024])))
            .await
            .unwrap();
        futures::SinkExt::send(&mut tx, Ok(Bytes::from(vec![b'a'; 1024])))
            .await
            .unwrap();
        drop(tx);
        let info = put.await.unwrap().unwrap();
        assert_eq!(info.size, 3072);
    }

    #[tokio::test]
    async fn overwrite_is_atomic_and_old_version_gone() {
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        s.put_object("alpha", "k", bytes::Bytes::from_static(b"one"), None)
            .await
            .unwrap();
        s.put_object("alpha", "k", bytes::Bytes::from_static(b"two"), None)
            .await
            .unwrap();

        let info = s.head_object("alpha", "k").await.unwrap();
        assert_eq!(info.size, 3);

        // 对象目录下应当只剩一个新版本目录 + xl.meta（老版本进了 trash）
        let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
        let mut names: Vec<String> = Vec::new();
        let mut entries = tokio::fs::read_dir(&obj_dir).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            names.push(e.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        assert_eq!(
            names.len(),
            2,
            "对象目录下应只有 xl.meta 与一个新的版本目录: {names:?}"
        );
        assert!(names.iter().any(|n| n == "xl.meta"));
    }

    #[tokio::test]
    async fn crash_between_steps_leaves_no_visible_object() {
        // 崩在 3/4 之间的现场：tmp 孤儿 + 一个已被 rename 进对象目录、但无 meta 引用的版本目录。
        // 读者必须仍看到旧版本（这里 k 已有提交版本），绝不能看到未提交的数据。
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let old = s
            .put_object("alpha", "k", Bytes::from_static(b"one"), None)
            .await
            .unwrap();

        let orphan = layout::sys_dir(d.path(), "tmp").join("orphan-uuid");
        tokio::fs::create_dir_all(&orphan).await.unwrap();
        tokio::fs::write(orphan.join("part.1"), b"junk")
            .await
            .unwrap();
        let dangling = layout::object_dir(d.path(), "alpha", "k")
            .unwrap()
            .join("dangling-uuid");
        tokio::fs::create_dir_all(&dangling).await.unwrap();
        tokio::fs::write(dangling.join("part.1"), b"uncommitted")
            .await
            .unwrap();

        let head = s.head_object("alpha", "k").await.unwrap();
        assert_eq!(head.etag, old.etag);
        assert_eq!(head.size, 3);
    }

    #[tokio::test]
    async fn concurrent_puts_to_same_key_serialize() {
        let (s, _d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let s = std::sync::Arc::new(s);
        let mut handles = Vec::new();
        for i in 0..8u8 {
            let s = std::sync::Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                s.put_object("alpha", "same", bytes::Bytes::from(vec![i; 64]), None)
                    .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        // 必须能读出一份完整的、单一写入者的内容（不能是交错损坏）
        let info = s.head_object("alpha", "same").await.unwrap();
        assert_eq!(info.size, 64);
    }

    #[tokio::test]
    async fn part_written_but_not_committed_is_invisible() {
        // 直接把一个完整版本目录摆进对象目录、但不写 xl.meta，
        // 读路径必须报 NoSuchKey（证明「只有 xl.meta 是提交点」）
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
        tokio::fs::create_dir_all(obj_dir.join("dangling-uuid"))
            .await
            .unwrap();
        tokio::fs::write(obj_dir.join("dangling-uuid").join("part.1"), b"data")
            .await
            .unwrap();
        assert!(matches!(
            s.head_object("alpha", "k").await,
            Err(Error::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn read_meta_rejects_mismatched_object_key() {
        // 内容可解码、但 object_key 与目录名不符的 xl.meta：读路径必须报 CorruptMeta，
        // 而不是把「张冠李戴」的元数据当成请求的键（错位会静默服务错误内容）
        let (s, d) = store().await;
        s.create_bucket("alpha").await.unwrap();
        let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
        tokio::fs::create_dir_all(&obj_dir).await.unwrap();
        let m = XlMeta {
            object_key: "other".into(),
            versions: vec![],
        };
        tokio::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap())
            .await
            .unwrap();

        let err = s.head_object("alpha", "k").await.unwrap_err();
        assert!(matches!(err, Error::CorruptMeta(_)));
        let msg = err.to_string();
        assert!(msg.contains("\"k\"") && msg.contains("\"other\""), "{msg}");
    }
}
