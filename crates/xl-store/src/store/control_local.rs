//! 节点本地控制对象 IO（设计 §1.2）：Phase 0 单机/本地盘提交路径，**不扇出**。
//!
//! - 串行化：per-key 进程内 mutex + `flock`（[`Store::lock_object_local`]），跨进程互斥。
//! - 提交：`expected` 版本 CAS + 严格递增 fence token → 本机盘 quorum
//!   （`Q_s = ec.data + 1`）→ 提交证书；tombstone 与 `Indeterminate` 语义沿用 Phase 0。
//! - 读：只认带提交证书的副本；无证书残骸/损坏副本 fail-closed，覆盖不足 `CorruptMeta`。
//! - 集群配置下也不取 fenced 集群锁、不做 peer fanout；集群一致性由上层 Raft 负责。
//!
//! 启动互斥：`lock_local_control_root` 对 `<root>/.oss.sys/locks/control-root.lock`
//! 取独占 advisory `flock`（非阻塞）。同一 root 的第二个进程必须在启动时失败；该锁
//! 由调用方（server startup）持有到进程结束，drop 即释放。

use crate::error::{Error, Result};
use crate::{layout, meta};

use super::Store;
use super::control::{SystemObject, check_cas, control_meta, control_state, validate_control_key};

/// 节点控制根独占锁守卫（drop = 解锁）。
pub struct LocalControlRootGuard {
    _file: std::fs::File,
}

impl Store {
    /// 读节点本地控制对象。tombstone → `None`；无提交证书但有副本/损坏副本 →
    /// `CorruptMeta`（fail-closed，绝不把「无法判定」伪装成缺席）。
    pub async fn get_local_control(&self, key: &str) -> Result<Option<SystemObject>> {
        validate_control_key(key)?;
        let st = self.read_control_local(layout::SYS_DIR, key).await?;
        let Some(m) = st.certified else {
            if st.copies > 0 || st.corrupt > 0 {
                return Err(Error::CorruptMeta(format!(
                    "控制对象 {key:?} 无提交证书但存在 {} 份副本（{} 份损坏）：拒绝读取",
                    st.copies, st.corrupt
                )));
            }
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

    /// 写节点本地控制对象（CAS）：`expected` 必须等于当前已提交版本（缺失/`None` = 0），
    /// `fence` 必须严格大于已观察到的最大 token。返回新版本（每次成功 +1）。
    /// 本机盘 quorum 不足 → `QuorumUnmet`/`CorruptMeta`，绝不部分成功。
    pub async fn put_local_control(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<u64>,
        fence: u64,
    ) -> Result<u64> {
        validate_control_key(key)?;
        if data.len() as u64 > meta::MAX_INLINE {
            return Err(Error::InvalidArgument(format!(
                "控制对象 {} 超过内联上限 {}：{} 字节",
                key.escape_debug(),
                meta::MAX_INLINE,
                data.len()
            )));
        }
        let guard = self.control_lock_local(key).await?;
        // CAS 只认带提交证书的版本：未提交半翻不可见，也不能把后续合法 CAS 顶成 Conflict。
        let st = self.read_control_local(layout::SYS_DIR, key).await?;
        if st.certified.is_none() && st.corrupt > 0 {
            return Err(Error::CorruptMeta(format!(
                "控制对象 {key:?} 无提交证书且存在 {} 份损坏副本：拒绝推进版本",
                st.corrupt
            )));
        }
        let (cur_version, cur_fence) = control_state(st.certified.as_ref());
        check_cas(cur_version, cur_fence.max(st.max_token), expected, fence)?;
        // 版本推进越过所有已观察副本（含未提交残骸）：崩溃写者的版本绝不重用。
        let version = st
            .max_version
            .checked_add(1)
            .ok_or_else(|| Error::CorruptMeta(format!("控制对象 {key:?} 版本号溢出")))?;
        let m = control_meta(key, version, fence, Some(data), false);
        self.commit_control_meta_local(
            key,
            &m,
            st.certified.as_ref(),
            fence,
            &guard,
            &st.certified_slots,
        )
        .await?;
        Ok(version)
    }

    /// 节点控制根启动互斥（设计 §1.2）：`<root>/.oss.sys/locks/control-root.lock` 上的
    /// 独占 `flock`。第二个持有者立即失败；守卫 drop 即释放。
    pub fn lock_local_control_root(&self) -> Result<LocalControlRootGuard> {
        let dir = layout::sys_dir(self.cfg.root(), "locks");
        std::fs::create_dir_all(&dir)?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("control-root.lock"))?;
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
            |_| {
                Error::Encode(
                    "控制根已被另一进程占用（.oss.sys/locks/control-root.lock）：拒绝第二个写者"
                        .into(),
                )
            },
        )?;
        Ok(LocalControlRootGuard { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::store::ec::EcConfig;
    use crate::store::tests::InProcessMesh;
    use crate::store::{ClusterConfig, ClusterSets, NodeSpec, StoreConfig};

    const KEY: &str = "control/state/root";

    /// 3 节点 × 4 盘，EC 3+4：Q_s = 4 = 每节点盘数，节点本地控制写在本机可达 quorum。
    async fn cluster() -> (Vec<Arc<Store>>, Vec<Vec<tempfile::TempDir>>) {
        let ec = EcConfig {
            data: 3,
            parity: 4,
            enabled: true,
        };
        let ids = ["n1", "n2", "n3"];
        let specs: Vec<NodeSpec> = ids
            .iter()
            .map(|id| NodeSpec {
                id: (*id).into(),
                addr: format!("127.0.0.1:1930{}", &id[1..]),
                drive_count: 4,
            })
            .collect();
        let mesh = InProcessMesh::new();
        let mut stores = Vec::new();
        let mut dirs = Vec::new();
        for id in ids {
            let tds: Vec<tempfile::TempDir> =
                (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
            let paths: Vec<std::path::PathBuf> =
                tds.iter().map(|d| d.path().to_path_buf()).collect();
            let sets = ClusterSets::single(id, specs.clone(), &ec).unwrap();
            let mut cfg = StoreConfig::new(&paths[0]);
            cfg.drives = paths;
            cfg.ec = ec;
            cfg.fsync = false;
            cfg.cluster = Some(Arc::new(ClusterConfig {
                sets,
                transport: mesh.clone(),
                placement_v2: false,
                resolver: None,
                stretch: false,
            }));
            stores.push(Arc::new(Store::open(cfg).await.unwrap()));
            dirs.push(tds);
        }
        for (id, store) in ids.iter().zip(&stores) {
            mesh.register(id, store);
        }
        (stores, dirs)
    }

    /// 集群配置的 Store：本地控制 IO 只落本机盘，不写 peer、不取 fenced 集群锁。
    #[tokio::test]
    async fn cluster_configured_local_control_never_fans_out() {
        let (stores, dirs) = cluster().await;
        let s = &stores[0];
        assert_eq!(s.put_local_control(KEY, b"root", None, 1).await.unwrap(), 1);
        let obj = s.get_local_control(KEY).await.unwrap().unwrap();
        assert_eq!((obj.version, obj.fence_token), (1, 1));

        for (node, node_dirs) in dirs.iter().enumerate().skip(1) {
            for drive in node_dirs {
                let path = layout::object_dir_system(drive.path(), KEY)
                    .unwrap()
                    .join(meta::META_FILE);
                assert!(
                    !path.exists(),
                    "本地控制写污染了 peer node{}：{}",
                    node + 1,
                    path.display()
                );
            }
        }
        // fenced 集群锁会落 `.fence` 高水位并推进提交 token；本地路径必须没有。
        let locks = layout::sys_dir(s.cfg.root(), "locks");
        let mut entries = tokio::fs::read_dir(&locks).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                !name.ends_with(".fence"),
                "本地控制写不得取 fenced 集群锁：{name}"
            );
        }
    }
}
