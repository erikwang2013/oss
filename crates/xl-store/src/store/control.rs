//! Phase 0：控制对象（`.oss.sys` 保留命名空间内的 system-object）。
//!
//! CAS = 串行化（`lock_object`：进程内 per-key mutex + `flock`）+ 读旧 meta +
//! `expected_version`/`fence_token` 校验 + fenced `write_meta` 提交（提交侧再按已提交 token
//! 拒绝过期写）。只要两个 Store 打开同一 root（同进程或不同进程），同一 key 的读-检-写互斥；
//! `flock` 是本地文件系统上的 advisory 锁，不覆盖网络文件系统/跨机器。
//! put/delete/get/list 的集群分支整体在 Task 5 接入（当前均 Unsupported）。

use md5::Digest as _;

use crate::error::{Error, Result};
use crate::layout;
use crate::meta::{self, InlineData, PartRef, Version, XlMeta};

use super::Store;

/// 控制对象键前缀：`object_dir_system` 只拒绝与系统子项同名的首段，`control` 不在其列。
const CONTROL_PREFIX: &str = "control/";

#[derive(Debug, Clone)]
pub struct SystemObject {
    pub key: String,
    pub data: Vec<u8>,
    pub version: u64,
    pub fence_token: u64,
    pub deleted: bool,
}

impl Store {
    /// 写控制对象（CAS）：`expected_version`/`fence_token` 与当前 meta 不符 → `Error::Conflict`。
    /// 返回新版本号（每次成功写入 +1）。
    pub async fn put_system_object(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<u64>,
        fence_token: u64,
    ) -> Result<u64> {
        self.ensure_control_single_node()?;
        validate_control_key(key)?;
        if data.len() as u64 > meta::MAX_INLINE {
            return Err(Error::InvalidArgument(format!(
                "控制对象 {} 超过内联上限 {}：{} 字节",
                key.escape_debug(),
                meta::MAX_INLINE,
                data.len()
            )));
        }
        let _guard = self.control_lock(key).await?;
        // 获取后即时自检（与数据面写入口一致；集群分支在 Task 5 放开后不再是死代码）
        if _guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约在获取后即失效".into()));
        }
        let old = self.read_meta(layout::SYS_DIR, key).await?;
        let (cur_version, cur_fence) = control_state(old.as_ref());
        check_cas(cur_version, cur_fence, expected_version, fence_token)?;
        let version = cur_version + 1;
        let m = control_meta(key, version, fence_token, Some(data), false);
        self.commit_control_meta(key, &m, old.as_ref(), fence_token)
            .await?;
        Ok(version)
    }

    /// 读控制对象（tombstone → None）。
    pub async fn get_system_object(&self, key: &str) -> Result<Option<SystemObject>> {
        self.ensure_control_single_node()?;
        validate_control_key(key)?;
        let Some(m) = self.read_meta(layout::SYS_DIR, key).await? else {
            return Ok(None);
        };
        if m.control_deleted == Some(true) {
            return Ok(None);
        }
        Ok(Some(SystemObject {
            key: key.to_string(),
            data: m
                .latest()
                .and_then(|v| v.inline.as_ref())
                .map(|i| i.bytes.clone())
                .unwrap_or_default(),
            version: m.control_version.unwrap_or(0),
            fence_token: m.fence_token.unwrap_or(0),
            deleted: false,
        }))
    }

    /// 删控制对象（CAS 同 put）：写 tombstone 版本。返回是否真的发生了删除
    /// （不存在/已删除 → false，幂等）。
    pub async fn delete_system_object(
        &self,
        key: &str,
        expected_version: Option<u64>,
        fence_token: u64,
    ) -> Result<bool> {
        self.ensure_control_single_node()?;
        validate_control_key(key)?;
        let _guard = self.control_lock(key).await?;
        if _guard.lease_lost() {
            return Err(Error::QuorumUnmet("集群锁租约在获取后即失效".into()));
        }
        let old = self.read_meta(layout::SYS_DIR, key).await?;
        let (cur_version, cur_fence) = control_state(old.as_ref());
        check_cas(cur_version, cur_fence, expected_version, fence_token)?;
        let Some(old) = old else {
            return Ok(false);
        };
        if old.control_deleted == Some(true) {
            return Ok(false);
        }
        let m = control_meta(key, cur_version + 1, fence_token, None, true);
        self.commit_control_meta(key, &m, Some(&old), fence_token)
            .await?;
        Ok(true)
    }

    /// 列控制对象键（前缀过滤；tombstone 不出现）。排序保证结果稳定。
    pub async fn list_system_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.ensure_control_single_node()?;
        if !prefix.starts_with(CONTROL_PREFIX) {
            return Err(Error::InvalidKey(format!(
                "控制对象前缀必须以 {CONTROL_PREFIX} 开头：{:?}",
                prefix.escape_debug()
            )));
        }
        // 全盘并集（同 scan_keys 口径）：掉线盘/半翻盘不得让 put 可写的键从列举里消失。
        let mut names = std::collections::BTreeSet::new();
        for drive in &self.cfg.drives {
            let mut entries = match tokio::fs::read_dir(layout::sys_dir(drive, "")).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Ok(key) = layout::key_from_dir_name(&name)
                    && key.starts_with(prefix)
                {
                    names.insert(key);
                }
            }
        }
        let mut out = Vec::new();
        for key in names {
            // tombstone 仍占目录：读 meta 过滤（控制对象量级小，逐键读可接受）
            if let Some(m) = self.read_meta(layout::SYS_DIR, &key).await?
                && m.control_deleted != Some(true)
            {
                out.push(key);
            }
        }
        Ok(out)
    }

    fn ensure_control_single_node(&self) -> Result<()> {
        if self.cfg.cluster.is_some() {
            return Err(Error::Unsupported(
                "system-object API 的集群分支在 Task 5 接入（当前单机）".into(),
            ));
        }
        Ok(())
    }

    /// CAS 串行化：数据面同款跨进程锁（进程内 mutex + `flock`），不同进程打开同一
    /// root 的两个 Store 也能互斥。集群子分支在上游已拒绝，故这里只落到本地两层。
    async fn control_lock(&self, key: &str) -> Result<super::LockGuard> {
        self.lock_object(layout::SYS_DIR, key).await
    }

    /// 提交控制对象 meta：单盘/多盘走 fenced 提交（调用方持有 fence token；持久化 token
    /// 发放为 Task 4）。当前 put/delete 由 `ensure_control_single_node` 限制为单机，集群
    /// 分支在 Task 5 放开；过期 token 由提交侧返回 `Error::Conflict`。
    async fn commit_control_meta(
        &self,
        key: &str,
        m: &XlMeta,
        old: Option<&XlMeta>,
        fence_token: u64,
    ) -> Result<()> {
        let flipped = self
            .write_meta_fenced(layout::SYS_DIR, key, m, old, Some(fence_token))
            .await?;
        if flipped == 0 {
            return Err(Error::QuorumUnmet(format!(
                "控制对象 {key:?} meta 未提交（0 槽翻转）"
            )));
        }
        Ok(())
    }
}

fn validate_control_key(key: &str) -> Result<()> {
    layout::validate_key(key)?;
    if !key.starts_with(CONTROL_PREFIX) {
        return Err(Error::InvalidKey(format!(
            "{}: 不以 {CONTROL_PREFIX} 开头",
            key.escape_debug()
        )));
    }
    Ok(())
}

fn control_state(old: Option<&XlMeta>) -> (u64, u64) {
    old.map_or((0, 0), |m| {
        (m.control_version.unwrap_or(0), m.fence_token.unwrap_or(0))
    })
}

fn check_cas(
    cur_version: u64,
    cur_fence: u64,
    expected_version: Option<u64>,
    fence_token: u64,
) -> Result<()> {
    if expected_version.unwrap_or(0) != cur_version {
        return Err(Error::Conflict(format!(
            "控制对象版本不匹配：期望 {}，当前 {cur_version}",
            expected_version.unwrap_or(0)
        )));
    }
    if fence_token <= cur_fence {
        return Err(Error::Conflict(format!(
            "fence token 已过期：{fence_token} ≤ {cur_fence}"
        )));
    }
    Ok(())
}

/// 单版本控制对象 meta：数据内联（数据面读路径不经过它，PartRef 仅满足解码/兼容契约）。
fn control_meta(
    key: &str,
    version: u64,
    fence_token: u64,
    data: Option<&[u8]>,
    deleted: bool,
) -> XlMeta {
    let bytes = data.unwrap_or_default();
    let version_entry = Version {
        version_id: uuid::Uuid::nil(),
        mod_time_nanos: super::now_nanos(),
        size: bytes.len() as u64,
        etag: hex::encode(md5::Md5::digest(bytes)),
        content_type: None,
        user_metadata: Default::default(),
        part: PartRef {
            uuid: uuid::Uuid::new_v4().to_string(),
            part: 1,
            size: bytes.len() as u64,
            sha256: hex::encode(sha2::Sha256::digest(bytes)),
        },
        parts: None,
        part_sizes: None,
        checksum: None,
        data_layout: None,
        inline: data.map(|d| InlineData { bytes: d.to_vec() }),
        is_delete_marker: None,
        is_latest: Some(true),
        epoch: Some(version),
        encryption: None,
        lock: None,
        replication_status: None,
        remote_version_id: None,
        tags: None,
        storage_class: None,
        tier: None,
    };
    XlMeta {
        object_key: key.to_string(),
        versions: vec![version_entry],
        control_version: Some(version),
        fence_token: Some(fence_token),
        control_deleted: deleted.then_some(true),
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::store::{Store, StoreConfig};

    async fn test_store() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
        (s, dir)
    }

    fn assert_conflict<T: std::fmt::Debug>(r: crate::error::Result<T>) {
        match r {
            Err(Error::Conflict(_)) => {}
            other => panic!("应为 Conflict，实际：{other:?}"),
        }
    }

    #[tokio::test]
    async fn system_object_cas_and_tombstone() {
        let (s, _d) = test_store().await;
        assert_eq!(
            s.put_system_object("control/a", b"one", None, 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            &s.get_system_object("control/a")
                .await
                .unwrap()
                .unwrap()
                .data[..],
            b"one"
        );
        assert_conflict(s.put_system_object("control/a", b"two", Some(0), 2).await);
        assert_eq!(
            s.put_system_object("control/a", b"two", Some(1), 2)
                .await
                .unwrap(),
            2
        );
        assert!(
            s.delete_system_object("control/a", Some(2), 3)
                .await
                .unwrap()
        );
        assert!(s.get_system_object("control/a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn system_object_fence_and_key_validation() {
        let (s, _d) = test_store().await;
        // fence token 必须严格递增（缺失按 0，故首个 token 必须 ≥ 1）
        assert_conflict(s.put_system_object("control/a", b"x", None, 0).await);
        s.put_system_object("control/a", b"x", None, 2)
            .await
            .unwrap();
        assert_conflict(s.put_system_object("control/a", b"y", Some(1), 2).await);
        // CAS 版本不匹配
        assert_conflict(s.put_system_object("control/a", b"y", Some(0), 3).await);
        // 只允许 control/ 前缀
        assert!(matches!(
            s.put_system_object("other/a", b"x", None, 9).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            s.get_system_object("other/a").await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            s.list_system_objects("other/").await,
            Err(Error::InvalidKey(_))
        ));
        // 内联上限
        let big = vec![0u8; crate::meta::MAX_INLINE as usize + 1];
        assert!(matches!(
            s.put_system_object("control/big", &big, None, 1).await,
            Err(Error::InvalidArgument(_))
        ));
    }

    /// 同一 expected_version 的并发写者只能有一个成功。两个独立 Store 句柄共用一个
    /// root（≈ 两个进程各开一个 Store）：per-Store 内存锁不共享，互斥必须由 flock 提供。
    #[tokio::test]
    async fn system_object_concurrent_cas_allows_exactly_one_writer() {
        let dir = tempfile::tempdir().unwrap();
        let s1 = std::sync::Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
        let s2 = std::sync::Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
        let handles = [s1.clone(), s2.clone()];
        let results = futures::future::join_all((1..=8u64).map(|token| {
            let s = handles[(token as usize) % 2].clone();
            async move { s.put_system_object("control/race", b"x", None, token).await }
        }))
        .await;
        assert_eq!(
            results.iter().filter(|r| r.is_ok()).count(),
            1,
            "只允许一个写者成功：{results:?}"
        );
        for r in results.iter().filter(|r| r.is_err()) {
            assert!(
                matches!(r, Err(Error::Conflict(_))),
                "其余必须是 Conflict：{r:?}"
            );
        }
        assert_eq!(
            s1.get_system_object("control/race")
                .await
                .unwrap()
                .unwrap()
                .version,
            1
        );
    }

    /// tombstone 后重建：按 tombstone 的版本 CAS 继续写，数据/版本/token 全部前进。
    #[tokio::test]
    async fn system_object_delete_then_put_resurrects() {
        let (s, _d) = test_store().await;
        assert_eq!(
            s.put_system_object("control/r", b"one", None, 1)
                .await
                .unwrap(),
            1
        );
        assert!(
            s.delete_system_object("control/r", Some(1), 2)
                .await
                .unwrap()
        );
        assert!(s.get_system_object("control/r").await.unwrap().is_none());
        // tombstone 占版本 2（delete 只回 bool，版本按契约 +1 推算）
        assert_eq!(
            s.put_system_object("control/r", b"two", Some(2), 3)
                .await
                .unwrap(),
            3
        );
        let obj = s.get_system_object("control/r").await.unwrap().unwrap();
        assert_eq!(&obj.data[..], b"two");
        assert_eq!((obj.version, obj.fence_token, obj.deleted), (3, 3, false));
    }

    #[tokio::test]
    async fn system_object_list_excludes_tombstones_and_filters_prefix() {
        let (s, _d) = test_store().await;
        s.put_system_object("control/a", b"1", None, 1)
            .await
            .unwrap();
        s.put_system_object("control/dir/b", b"2", None, 1)
            .await
            .unwrap();
        s.put_system_object("control/c", b"3", None, 1)
            .await
            .unwrap();
        assert_eq!(
            s.list_system_objects("control/").await.unwrap(),
            vec!["control/a", "control/c", "control/dir/b"]
        );
        assert_eq!(
            s.list_system_objects("control/dir/").await.unwrap(),
            vec!["control/dir/b"]
        );
        s.delete_system_object("control/c", Some(1), 2)
            .await
            .unwrap();
        assert_eq!(
            s.list_system_objects("control/").await.unwrap(),
            vec!["control/a", "control/dir/b"]
        );
    }
}
