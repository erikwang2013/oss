//! Task 6 故障注入共享夹具：kill9 式子进程重入 + 单节点集群（12 槽全本机）开关。
//! 真实进程/真实盘：崩溃靠 SIGABRT 注入，分区靠摘 `.oss.sys`（`drive_up` 判定）。

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::Arc;

use bytes::Bytes;
use xl_store::error::Result;
use xl_store::layout;
use xl_store::mesh::{
    ControlQuery, ControlRow, ListLocal, ListLocalResp, LockReq, LockResp, MeshTransport, MetaAck,
    MetaRead, MetaSnapshot, MetaWrite, NodeHealth, ShardDelete, ShardRead, ShardWrite,
    VersionDirRef,
};
use xl_store::meta::XlMeta;
use xl_store::store::{ClusterConfig, ClusterSets, NodeSpec, Store, StoreConfig};

pub const N_DRIVES: usize = 12;
/// 控制面 quorum：`Q_s = ec.data + 1`（默认 8+4）。
pub const Q_S: usize = 9;

pub const CHILD: &str = "OSS_FAULT_CHILD";
pub const PHASE: &str = "OSS_FAULT_PHASE";
pub const DIRS: &str = "OSS_FAULT_DIRS";
pub const ABORT_PREPARE: &str = "OSS_TEST_ABORT_META_PREPARE";
pub const ABORT_CONTROL_COMMIT_K: &str = "OSS_TEST_ABORT_CONTROL_COMMIT_K";

/// 单节点集群的传输：12 槽全在本机 → 永无 peer RPC。调用即缺陷，直接炸测试
/// （不用静默 Ok：把未实现伪装成成功会掩盖真实回归）。
#[derive(Debug)]
struct NoPeers;

#[async_trait::async_trait]
impl MeshTransport for NoPeers {
    async fn write_shard(&self, _: &str, _: ShardWrite) -> Result<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn read_shard(&self, _: &str, _: ShardRead) -> Result<Option<Bytes>> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn delete_version(&self, _: &str, _: VersionDirRef) -> Result<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn recycle_version(&self, _: &str, _: VersionDirRef) -> Result<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn delete_shard(&self, _: &str, _: ShardDelete) -> Result<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn prepare_meta(&self, _: &str, _: MetaWrite) -> Result<MetaAck> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn commit_meta(&self, _: &str, _: MetaWrite) -> Result<MetaAck> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn abort_meta(&self, _: &str, _: MetaWrite) -> Result<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn read_meta(&self, _: &str, _: MetaRead) -> Result<MetaSnapshot> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn delete_meta(&self, _: &str, _: MetaRead) -> Result<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn ensure_bucket(&self, _: &str, _: &str) -> Result<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn delete_bucket_dir(&self, _: &str, _: &str) -> Result<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn list_bucket_dirs(&self, _: &str) -> Result<Vec<String>> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn list_local(&self, _: &str, _: ListLocal) -> Result<ListLocalResp> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn health(&self, _: &str) -> Result<NodeHealth> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn lock(&self, _: &str, _: LockReq) -> Result<LockResp> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn control_query(&self, _: &str, _: ControlQuery) -> Result<Option<ControlRow>> {
        unreachable!("单节点集群不应有 peer RPC")
    }
}

pub async fn open_store(dirs: &[PathBuf], inline_threshold: Option<u64>, cluster: bool) -> Store {
    let mut cfg = StoreConfig::new(&dirs[0]);
    cfg.drives = dirs.to_vec();
    cfg.fsync = false; // 测试提速；注入点/可见性与 fsync 无关（kill9 同口径）
    if let Some(t) = inline_threshold {
        cfg.inline_threshold = t;
    }
    if cluster {
        // 12 槽全归本节点：真实 quorum 代码路径，无 peer、无网络替身。
        let ec = StoreConfig::new(&dirs[0]).ec;
        let nodes = vec![NodeSpec {
            id: "n1".into(),
            addr: "127.0.0.1:19001".into(),
            drive_count: N_DRIVES as u16,
        }];
        let sets = ClusterSets::single("n1", nodes, &ec).expect("单节点集群拓扑");
        cfg.cluster = Some(Arc::new(ClusterConfig {
            sets,
            transport: Arc::new(NoPeers),
            placement_v2: false,
            resolver: None,
            stretch: false,
        }));
    }
    Store::open(cfg).await.unwrap()
}

pub fn temp_dirs() -> (tempfile::TempDir, Vec<PathBuf>) {
    let base = tempfile::tempdir().unwrap();
    let dirs = (0..N_DRIVES)
        .map(|i| {
            let p = base.path().join(format!("d{i}"));
            std::fs::create_dir_all(&p).unwrap();
            p
        })
        .collect();
    (base, dirs)
}

pub fn dirs_from_env() -> Vec<PathBuf> {
    std::env::var(DIRS)
        .expect("OSS_FAULT_DIRS")
        .split(':')
        .map(PathBuf::from)
        .collect()
}

pub fn spawn(
    exe: &Path,
    test: &str,
    phase: &str,
    dirs: &[PathBuf],
    envs: &[(&str, &str)],
) -> ExitStatus {
    let mut c = Command::new(exe);
    c.arg("--exact")
        .arg(test)
        .env(CHILD, "1")
        .env(PHASE, phase)
        .env(
            DIRS,
            dirs.iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(":"),
        );
    for (k, v) in envs {
        c.env(k, v);
    }
    c.status().unwrap()
}

#[cfg(unix)]
pub fn assert_aborted(status: ExitStatus) {
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(6),
        "子进程应死于 abort()（SIGABRT=6），实际 {status:?}"
    );
}
#[cfg(not(unix))]
pub fn assert_aborted(status: ExitStatus) {
    assert!(!status.success());
}

pub fn control_dir(drive: &Path, key: &str) -> PathBuf {
    layout::object_dir_system(drive, key).unwrap()
}

/// 统计「已提交 meta 的槽数」与「暂存 `.new` 的槽数」——注入点/提交点的盘上证据。
pub fn slot_counts(dirs: &[PathBuf], key: &str) -> (usize, usize) {
    let mut committed = 0;
    let mut staged = 0;
    for d in dirs {
        let dir = control_dir(d, key);
        if dir.join("xl.meta").exists() {
            committed += 1;
        }
        if dir.join("xl.meta.new").exists() {
            staged += 1;
        }
    }
    (committed, staged)
}

pub fn disk_meta(drive: &Path, key: &str) -> XlMeta {
    let bytes = std::fs::read(control_dir(drive, key).join("xl.meta")).unwrap();
    XlMeta::decode(&bytes).unwrap()
}

/// 直接写盘上 `xl.meta`（绕过 Store 的两阶段；仅测试构造 legacy 现场）。
pub fn write_disk_meta(drive: &Path, key: &str, m: &XlMeta) {
    let dir = control_dir(drive, key);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("xl.meta"), m.encode().unwrap()).unwrap();
}

/// 盘上 `xl.meta` 原始字节（缺失 → None；零写入断言用）。
pub fn disk_meta_bytes(drive: &Path, key: &str) -> Option<Vec<u8>> {
    std::fs::read(control_dir(drive, key).join("xl.meta")).ok()
}
