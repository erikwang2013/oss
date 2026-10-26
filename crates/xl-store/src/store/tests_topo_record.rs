//! 拓扑记录启动语义：v4 只读 exact-match；legacy 记录只校验三元组、绝不改写；
//! 缺记录补 v1.19 同字节的 v2/v3（v4 只能由 `cluster-shrink commit` 写）。

use std::path::PathBuf;

use super::drives::{ClusterSets, SlotMap, SlotTarget};
use super::ec::EcConfig;
use super::tests_shrink_apply::{n, open_with, store_err};

/// 单节点 12 槽全在 0 号盘：只用于拓扑记录格式/启动校验（与集群 fixture 解耦）。
fn one_node(epoch: u64) -> ClusterSets {
    let slots = std::iter::repeat_n(
        SlotTarget {
            node: "n1".into(),
            drive: 0,
        },
        12,
    )
    .collect();
    ClusterSets::with_slot_maps(
        "n1",
        vec![vec![n("n1", 1)]],
        vec![Some(SlotMap { epoch, slots })],
        &EcConfig::default(),
    )
    .unwrap()
}

/// 1 盘目录 + 预置 topo 记录（记录格式测试用）。
async fn record_dir(rec: &str) -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    tokio::fs::create_dir_all(crate::layout::sys_dir(d.path(), ""))
        .await
        .unwrap();
    tokio::fs::write(crate::layout::sys_dir(d.path(), "topo"), rec)
        .await
        .unwrap();
    d
}

fn topo_at(dir: &std::path::Path) -> String {
    std::fs::read_to_string(crate::layout::sys_dir(dir, "topo")).unwrap()
}

async fn open_record(rec: &str) -> String {
    let d = record_dir(rec).await;
    let sets = one_node(2);
    store_err(
        open_with(
            vec![d.path().to_path_buf()],
            "n1",
            sets,
            EcConfig::default(),
        )
        .await,
    )
}

/// v4 启动校验：epoch/hash/node/slot 任一不符 → 拒启，错误要能指向 commit/配置。
#[tokio::test]
async fn v4_startup_exact_match_rejects_each_field_mismatch() {
    let good_hash = one_node(2).topology_hash();
    let cases = [
        format!("v4 set=0 node=n1 slot=0 epoch=3 hash={good_hash}"),
        "v4 set=0 node=n1 slot=0 epoch=2 hash=0123456789abcdef".to_string(),
        format!("v4 set=0 node=n2 slot=0 epoch=2 hash={good_hash}"),
        format!("v4 set=0 node=n1 slot=1 epoch=2 hash={good_hash}"),
    ];
    for (i, rec) in cases.iter().enumerate() {
        let e = open_record(rec).await;
        assert!(
            e.contains("v4 与配置不符") || e.contains("无法识别"),
            "case {i}: {e}"
        );
    }
    // 未知更高版本 v5 → fail-closed。
    let e = open_record("v5 set=0 node=n1 slot=0 epoch=2 hash=0123456789abcdef").await;
    assert!(e.contains("无法识别"), "{e}");
    // 本机 0 号盘 = slot 0；上面 4 个 case 已覆盖 epoch/hash/node/slot。
}

#[tokio::test]
async fn legacy_records_survive_explicit_start() {
    // v1/v2：显式配置只校验三元组、不改写；记录保持 v1.19 字节 → 可退回旧二进制。
    for rec in ["v1 slots=12 node=n1 slot=0", "v2 node=n1 slot=0"] {
        let dir = record_dir(rec).await;
        let sets = one_node(2);
        let s = open_with(
            vec![dir.path().to_path_buf()],
            "n1",
            sets,
            EcConfig::default(),
        )
        .await
        .unwrap();
        drop(s);
        assert_eq!(topo_at(dir.path()), rec);
    }
    // 缺记录 + 显式配置：补写 legacy v2（set0），仍不是 v4。
    let d = tempfile::tempdir().unwrap();
    let sets = one_node(2);
    let s = open_with(
        vec![d.path().to_path_buf()],
        "n1",
        sets,
        EcConfig::default(),
    )
    .await
    .unwrap();
    drop(s);
    assert_eq!(topo_at(d.path()), "v2 node=n1 slot=0");
    // v3（set>0）：本机只在 set 1，记录不改写；删掉后重开补写同字节 v3。
    let ec = EcConfig {
        data: 2,
        parity: 1,
        enabled: true,
    };
    let ts: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let dirs: Vec<PathBuf> = ts.iter().map(|t| t.path().to_path_buf()).collect();
    tokio::fs::create_dir_all(crate::layout::sys_dir(&dirs[0], ""))
        .await
        .unwrap();
    let path = crate::layout::sys_dir(&dirs[0], "topo");
    tokio::fs::write(&path, "v3 set=1 node=n1 slot=0")
        .await
        .unwrap();
    let m = |node: &str, epoch: u64| SlotMap {
        epoch,
        slots: (0..3)
            .map(|_| SlotTarget {
                node: node.into(),
                drive: 0,
            })
            .collect(),
    };
    let sets = ClusterSets::with_slot_maps(
        "n1",
        vec![vec![n("n2", 3)], vec![n("n1", 3)]],
        vec![Some(m("n2", 2)), Some(m("n1", 2))],
        &ec,
    )
    .unwrap();
    let s = open_with(dirs.clone(), "n1", sets.clone(), ec)
        .await
        .unwrap();
    drop(s);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "v3 set=1 node=n1 slot=0"
    );
    std::fs::remove_file(&path).unwrap();
    let s = open_with(dirs, "n1", sets, ec).await.unwrap();
    drop(s);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "v3 set=1 node=n1 slot=0"
    );
}
