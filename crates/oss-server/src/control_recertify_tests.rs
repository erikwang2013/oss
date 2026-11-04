//! `control-recertify` CLI 行为测试：单节点拒绝、missing no-op、apply 失败退出码、
//! 逐键继续与 `--json` 形状。集群基座 = 12 盘全归本机的单节点集群（真实 quorum 路径，
//! 无 peer RPC），与 `xl-store` 故障注入夹具同口径。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use xl_store::error::Result as StoreResult;
use xl_store::layout;
use xl_store::mesh::{
    ControlQuery, ControlRow, ListLocal, ListLocalResp, LockReq, LockResp, MeshTransport, MetaAck,
    MetaRead, MetaSnapshot, MetaWrite, NodeHealth, ShardDelete, ShardRead, ShardWrite,
    VersionDirRef,
};
use xl_store::meta::XlMeta;
use xl_store::store::{ClusterConfig, ClusterSets, NodeSpec, Store, StoreConfig};

use super::*;

const N_DRIVES: usize = 12;
/// `Q_s = ec.data + 1`（默认 8+4）。
const Q_S: usize = 9;

/// 单节点集群传输：12 槽全在本机 → 永无 peer RPC；调用即缺陷（不得静默伪装成功）。
#[derive(Debug)]
struct NoPeers;

#[async_trait::async_trait]
impl MeshTransport for NoPeers {
    async fn write_shard(&self, _: &str, _: ShardWrite) -> StoreResult<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn read_shard(&self, _: &str, _: ShardRead) -> StoreResult<Option<Bytes>> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn delete_version(&self, _: &str, _: VersionDirRef) -> StoreResult<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn recycle_version(&self, _: &str, _: VersionDirRef) -> StoreResult<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn delete_shard(&self, _: &str, _: ShardDelete) -> StoreResult<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn prepare_meta(&self, _: &str, _: MetaWrite) -> StoreResult<MetaAck> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn commit_meta(&self, _: &str, _: MetaWrite) -> StoreResult<MetaAck> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn abort_meta(&self, _: &str, _: MetaWrite) -> StoreResult<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn read_meta(&self, _: &str, _: MetaRead) -> StoreResult<MetaSnapshot> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn delete_meta(&self, _: &str, _: MetaRead) -> StoreResult<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn ensure_bucket(&self, _: &str, _: &str) -> StoreResult<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn delete_bucket_dir(&self, _: &str, _: &str) -> StoreResult<()> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn list_bucket_dirs(&self, _: &str) -> StoreResult<Vec<String>> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn list_local(&self, _: &str, _: ListLocal) -> StoreResult<ListLocalResp> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn health(&self, _: &str) -> StoreResult<NodeHealth> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn lock(&self, _: &str, _: LockReq) -> StoreResult<LockResp> {
        unreachable!("单节点集群不应有 peer RPC")
    }
    async fn control_query(&self, _: &str, _: ControlQuery) -> StoreResult<Option<ControlRow>> {
        unreachable!("单节点集群不应有 peer RPC")
    }
}

/// 12 盘单节点集群（Q_s=9、无 peer）；返回保活的临时目录与各盘路径。
async fn cluster_store() -> (tempfile::TempDir, Vec<PathBuf>, Store) {
    let tmp = tempfile::tempdir().unwrap();
    let dirs: Vec<PathBuf> = (0..N_DRIVES)
        .map(|i| {
            let p = tmp.path().join(format!("d{i}"));
            std::fs::create_dir_all(&p).unwrap();
            p
        })
        .collect();
    let mut cfg = StoreConfig::new(&dirs[0]);
    cfg.drives = dirs.clone();
    cfg.fsync = false;
    let nodes = vec![NodeSpec {
        id: "n1".into(),
        addr: "127.0.0.1:19001".into(),
        drive_count: N_DRIVES as u16,
    }];
    let sets = ClusterSets::single("n1", nodes, &cfg.ec).expect("单节点集群拓扑");
    cfg.cluster = Some(Arc::new(ClusterConfig {
        sets,
        transport: Arc::new(NoPeers),
        placement_v2: false,
        resolver: None,
        stretch: false,
    }));
    let store = Store::open(cfg).await.unwrap();
    (tmp, dirs, store)
}

fn control_dir(drive: &Path, key: &str) -> PathBuf {
    layout::object_dir_for(drive, layout::SYS_DIR, key).unwrap()
}

fn disk_meta(drive: &Path, key: &str) -> Option<XlMeta> {
    let bytes = std::fs::read(control_dir(drive, key).join("xl.meta")).ok()?;
    Some(XlMeta::decode(&bytes).unwrap())
}

fn write_disk_meta(drive: &Path, key: &str, m: &XlMeta) {
    std::fs::create_dir_all(control_dir(drive, key)).unwrap();
    std::fs::write(control_dir(drive, key).join("xl.meta"), m.encode().unwrap()).unwrap();
}

/// 真实两阶段写入种子后剥掉证书，模拟迁移前现场；`copies` 限定保留的 legacy 份数。
async fn seed_legacy(store: &Store, dirs: &[PathBuf], key: &str, copies: usize) {
    store.put_system_object(key, b"v1", None, 1).await.unwrap();
    let mut legacy = disk_meta(&dirs[0], key).expect("种子副本");
    assert_eq!(legacy.control_committed, Some(true));
    legacy.control_committed = None;
    for drive in dirs.iter().take(copies) {
        write_disk_meta(drive, key, &legacy);
    }
    // 未保留的种子槽必须整目录删除：残留证书会变成“可补齐”而不是“不可认证”。
    for drive in dirs.iter().take(Q_S).skip(copies) {
        std::fs::remove_dir_all(control_dir(drive, key)).unwrap();
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DiskState {
    meta: Option<Vec<u8>>,
    staged: Option<Vec<u8>>,
}

/// 全部槽的 `xl.meta` / `.new` 字节快照（失败路径零写入断言）。
fn snapshot(dirs: &[PathBuf], key: &str) -> Vec<DiskState> {
    dirs.iter()
        .map(|drive| {
            let dir = control_dir(drive, key);
            DiskState {
                meta: std::fs::read(dir.join("xl.meta")).ok(),
                staged: std::fs::read(dir.join("xl.meta.new")).ok(),
            }
        })
        .collect()
}

/// 返回 (保活的临时目录, 键列表路径)。
fn keys_file(keys: &[&str]) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keys.txt");
    std::fs::write(&path, keys.join("\n")).unwrap();
    (dir, path)
}

fn sorted_fields(v: &serde_json::Value) -> Vec<&str> {
    let mut fields: Vec<&str> = v
        .as_object()
        .expect("JSON 对象")
        .keys()
        .map(String::as_str)
        .collect();
    fields.sort_unstable();
    fields
}

/// 全部键缺席 → no-op，退出 0，摘要显式说明 nothing to do。
#[tokio::test]
async fn missing_keys_exit_zero_with_nothing_to_do() {
    let (_tmp, _dirs, store) = cluster_store().await;
    let (_kd, kf) = keys_file(&["control/absent"]);
    assert_eq!(run(&store, &kf, true, false).await.unwrap(), 0);

    let out = execute(&store, &["control/absent".into()], true, false).await;
    assert_eq!(out.code, 0);
    assert!(out.stdout.contains("missing"), "{}", out.stdout);
    assert!(out.stdout.contains("nothing to do"), "{}", out.stdout);
    assert!(out.stderr.is_empty(), "{}", out.stderr);
}

/// `--apply` 失败（8 份 < Q_s）→ 退出 1，且磁盘零写入。
#[tokio::test]
async fn apply_failure_exits_one_and_writes_nothing() {
    let (_tmp, dirs, store) = cluster_store().await;
    seed_legacy(&store, &dirs, "control/below", 8).await;
    let before = snapshot(&dirs, "control/below");
    let (_kd, kf) = keys_file(&["control/below"]);
    assert_eq!(run(&store, &kf, true, false).await.unwrap(), 1);

    let out = execute(&store, &["control/below".into()], true, false).await;
    assert_eq!(out.code, 1);
    assert!(
        out.stderr.contains("Error: control/below"),
        "{}",
        out.stderr
    );
    assert_eq!(snapshot(&dirs, "control/below"), before, "失败必须零写入");
}

/// 单节点部署不提供该命令：明确指向库 API。
#[tokio::test]
async fn single_node_deployment_is_unsupported() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(tmp.path())).await.unwrap();
    let (_kd, kf) = keys_file(&["control/a"]);
    let err = run(&store, &kf, false, false).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("单节点"), "{msg}");
    assert!(msg.contains("recertify_system_object"), "{msg}");
}

/// 一个键失败不中断后续键：失败进 `keys[]`，后续键照常扫描/汇总。
#[tokio::test]
async fn per_key_continue_after_failure() {
    let (_tmp, dirs, store) = cluster_store().await;
    seed_legacy(&store, &dirs, "control/bad", 8).await;
    let keys = vec!["control/bad".to_string(), "control/absent".to_string()];
    let out = execute(&store, &keys, false, true).await;
    assert_eq!(out.code, 1);
    assert!(out.stderr.is_empty(), "{}", out.stderr);
    let v: serde_json::Value = serde_json::from_str(out.stdout.trim()).unwrap();
    let entries = v["keys"].as_array().unwrap();
    assert_eq!(entries.len(), 2, "单键失败不得中断后续键");
    assert_eq!(entries[0]["key"], "control/bad");
    assert!(!entries[0]["ok"].as_bool().unwrap());
    assert_eq!(entries[1]["key"], "control/absent");
    assert!(entries[1]["ok"].as_bool().unwrap());
    assert_eq!(entries[1]["state"], "missing");
    assert!(!v["ok"].as_bool().unwrap());
    assert_eq!(v["missing"], 1);
}

/// `--json` 报告形状（dry-run 与 apply 两态）与 missing 计数。
#[tokio::test]
async fn json_report_shape() {
    let (_tmp, dirs, store) = cluster_store().await;
    seed_legacy(&store, &dirs, "control/ok", 9).await;
    let keys = vec!["control/ok".to_string()];

    let dry = execute(&store, &keys, false, true).await;
    assert_eq!(dry.code, 0);
    let v: serde_json::Value = serde_json::from_str(dry.stdout.trim()).unwrap();
    assert_eq!(sorted_fields(&v), ["applied", "keys", "missing", "ok"]);
    assert!(!v["applied"].as_bool().unwrap());
    assert!(v["ok"].as_bool().unwrap());
    assert_eq!(v["missing"], 0);
    let e = &v["keys"][0];
    assert_eq!(
        sorted_fields(e),
        [
            "certified",
            "copies",
            "groups",
            "key",
            "ok",
            "q_s",
            "state",
            "token",
            "version"
        ]
    );
    assert_eq!(e["key"], "control/ok");
    assert_eq!(e["state"], "certifiable");
    assert_eq!(e["version"], 1);
    assert_eq!(e["q_s"], 9);
    assert_eq!(e["copies"], 9);
    assert_eq!(e["certified"], 0);
    assert!(e["token"].is_null());
    let g = &e["groups"][0];
    assert_eq!(
        sorted_fields(g),
        ["certified", "copies", "max_token", "version"]
    );
    assert_eq!(g["version"], 1);
    assert_eq!(g["copies"], 9);
    assert_eq!(g["certified"], 0);
    assert!(g["max_token"].as_u64().unwrap() > 0);

    let applied = execute(&store, &keys, true, true).await;
    assert_eq!(applied.code, 0);
    let v: serde_json::Value = serde_json::from_str(applied.stdout.trim()).unwrap();
    assert!(v["applied"].as_bool().unwrap());
    assert_eq!(v["keys"][0]["state"], "certified");
    assert!(v["keys"][0]["token"].as_u64().unwrap() > 0);
    assert!(v["keys"][0]["certified"].as_u64().unwrap() >= Q_S as u64);
}

/// 键列表解析：注释/空行/CRLF 忽略；空列表、重复、非 `control/`、非法键整份拒绝。
#[test]
fn keys_file_ignores_comments_and_rejects_empty_duplicate_non_control() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("keys.txt");
    std::fs::write(&p, "# 注释\r\n\r\ncontrol/a\r\ncontrol/b\r\n# tail\r\n").unwrap();
    assert_eq!(load_keys(&p).unwrap(), vec!["control/a", "control/b"]);

    std::fs::write(&p, "# 只有注释\n\n").unwrap();
    assert!(load_keys(&p).is_err(), "空列表必须拒绝");

    std::fs::write(&p, "control/a\ncontrol/a\n").unwrap();
    assert!(load_keys(&p).is_err(), "重复键必须拒绝");

    std::fs::write(&p, "objects/a\n").unwrap();
    assert!(load_keys(&p).is_err(), "非 control/ 前缀必须拒绝");

    std::fs::write(&p, "control/../a\n").unwrap();
    assert!(load_keys(&p).is_err(), "非法键必须拒绝");
}
