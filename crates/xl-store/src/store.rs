//! 存储内核。M1：单盘、无纠删码，但**提交协议与 xl.meta 格式按最终设计落**，
//! M2 只是把「一个 part 一个文件」换成「一个 part 若干分片」。

use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures::Stream;

use crate::error::{Error, Result};
use crate::layout;

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
}

impl Store {
    pub async fn open(cfg: StoreConfig) -> Result<Self> {
        for sub in ["tmp", "trash", "locks"] {
            tokio::fs::create_dir_all(layout::sys_dir(&cfg.root, sub)).await?;
        }
        Ok(Self { cfg })
    }

    pub fn root(&self) -> &Path {
        &self.cfg.root
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
        // ponytail: check-then-remove 窗口在 Task 5 加锁后收窄；M1 单进程下仅 tokio 任务间竞态
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
}
