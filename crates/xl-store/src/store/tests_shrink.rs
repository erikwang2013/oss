//! M3 shrink T1：显式槽表（拓扑 v3）的解析与校验。

use super::drives::{ClusterSets, NodeSpec, SlotMap, SlotTarget};
use super::ec::EcConfig;

fn n(id: &str, dc: u16) -> NodeSpec {
    NodeSpec {
        id: id.into(),
        addr: String::new(),
        drive_count: dc,
    }
}

fn ec84() -> EcConfig {
    EcConfig::default()
}

fn map(epoch: u64, slots: &[(&str, u16)]) -> SlotMap {
    SlotMap {
        epoch,
        slots: slots
            .iter()
            .map(|&(node, drive)| SlotTarget {
                node: node.into(),
                drive,
            })
            .collect(),
    }
}

/// 4 节点 × 3 盘的 12 槽（n1 d0..2, n2 d0..2, n3 d0..2, n4 d0..2）。
fn four_nodes() -> Vec<NodeSpec> {
    vec![n("n1", 3), n("n2", 3), n("n3", 3), n("n4", 3)]
}

fn positional_slots() -> Vec<(&'static str, u16)> {
    ["n1", "n2", "n3", "n4"]
        .into_iter()
        .flat_map(|id| (0..3u16).map(move |d| (id, d)))
        .collect()
}

fn err_of(nodes: Vec<Vec<NodeSpec>>, maps: Vec<Option<SlotMap>>) -> String {
    ClusterSets::with_slot_maps("n1", nodes, maps, &ec84())
        .unwrap_err()
        .to_string()
}

/// 槽 9/10/11 从 n4 改属 n1/n2/n3 的 drive 0：解析、物理盘映射与 place 不变性。
#[test]
fn slot_map_overrides_positional_resolution() {
    let old = ClusterSets::new("n1", vec![four_nodes()], &ec84()).unwrap();
    assert_eq!(
        old.sets[0].slots_on_drive(0, 0),
        vec![0],
        "positional：一槽一盘"
    );
    assert_eq!(old.sets[0].slot_count_of_node(0), 3);
    let mut slots = positional_slots();
    slots[9] = ("n1", 0);
    slots[10] = ("n2", 0);
    slots[11] = ("n3", 0);
    let slot_map = map(1, &slots);
    let new = ClusterSets::with_slot_maps(
        "n1",
        vec![four_nodes()],
        vec![Some(slot_map.clone())],
        &ec84(),
    )
    .unwrap();
    let t = &new.sets[0];
    assert_eq!(t.slot_map().unwrap().epoch, 1);
    assert_eq!(t.slot_count(), 12);
    assert_eq!(t.node_of_slot(9), 0);
    assert_eq!(t.node_of_slot(10), 1);
    assert_eq!(t.node_of_slot(11), 2);
    assert_eq!(t.local_drive_of_slot(9), Some(0), "槽 9 在 n1 的 drive 0");
    assert_eq!(t.local_drive_of_slot(10), None, "槽 10 属 n2（远端）");
    assert_eq!(t.slots_on_drive(0, 0), vec![0, 9]);
    assert_eq!(t.slots_on_drive(1, 0), vec![3, 10]);
    assert_eq!(t.slots_on_drive(0, 2), vec![2], "positional 之外的盘不变");
    assert_eq!(t.slot_count_of_node(0), 4);
    assert_eq!(t.slot_count_of_node(3), 0, "n4 不再承载槽位");
    // place 只看槽数：与旧 positional 拓扑逐点相等（meta 槽号不漂移）
    for h in [0u64, 1, 0xdead_beef] {
        for shard in 0..12u8 {
            assert_eq!(t.place(h, shard, true), old.sets[0].place(h, shard, true));
        }
    }
    // flat/self_ranges 仍是物理盘语义（命名空间镜像不随槽表变）
    assert_eq!(new.self_ranges, old.self_ranges);
    assert_eq!(new.flat.slot_count(), 12);
    assert_eq!(new.flat_of(0, 2), Some(2));
    // epoch / hash 暴露：SlotMap::hash 含 epoch，topology_hash 跨 set
    assert_eq!(new.topology_epoch(), 1);
    assert_eq!(old.topology_epoch(), 0);
    assert_eq!(slot_map.hash(), t.slot_map().unwrap().hash());
    assert_ne!(map(2, &slots).hash(), slot_map.hash());
    assert_eq!(new.topology_hash().len(), 16);
    assert_ne!(new.topology_hash(), old.topology_hash());
    // 指纹与节点身份无关：同配置在不同节点上必须同值（v4 启动 fail-closed 的依据）
    let n2_view =
        ClusterSets::with_slot_maps("n2", vec![four_nodes()], vec![Some(slot_map)], &ec84())
            .unwrap();
    assert_eq!(new.topology_hash(), n2_view.topology_hash());
}

/// shrink 形态：3 节点 × 3 盘（9 物理盘）承载 12 槽——set 按槽表 12 槽，flat 仍按物理盘 9 槽。
#[test]
fn slot_map_allows_fewer_physical_drives_than_slots() {
    let nodes = vec![n("n1", 3), n("n2", 3), n("n3", 3)];
    let slots: Vec<(&str, u16)> = [
        ("n1", 0),
        ("n1", 1),
        ("n1", 2),
        ("n1", 0),
        ("n2", 0),
        ("n2", 1),
        ("n2", 2),
        ("n2", 0),
        ("n3", 0),
        ("n3", 1),
        ("n3", 2),
        ("n3", 0),
    ]
    .to_vec();
    let cs = ClusterSets::with_slot_maps("n1", vec![nodes], vec![Some(map(1, &slots))], &ec84())
        .unwrap();
    let t = &cs.sets[0];
    assert_eq!(t.slot_count(), 12);
    assert_eq!(t.slots_on_drive(0, 0), vec![0, 3]);
    assert_eq!(t.slot_count_of_node(0), 4);
    assert_eq!(t.local_drive_of_slot(3), Some(0));
    // flat（命名空间面）按物理盘：9 槽 = 3 节点 × 3 盘
    assert_eq!(cs.flat.slot_count(), 9);
    assert_eq!(cs.self_ranges, vec![(0u16, 0usize, 3usize)]);
    assert_eq!(cs.flat_of(0, 2), Some(2));
}

#[test]
fn slot_map_rejects_unknown_node_or_drive() {
    let mut slots = positional_slots();
    slots[4] = ("n9", 0);
    let e = err_of(vec![four_nodes()], vec![Some(map(1, &slots))]);
    assert!(e.contains("n9") && e.contains("未知节点"), "{e}");

    let mut slots = positional_slots();
    slots[4] = ("n2", 3); // n2 仅 3 盘（0..2）
    let e = err_of(vec![four_nodes()], vec![Some(map(1, &slots))]);
    assert!(e.contains("盘 3 越界") && e.contains("n2"), "{e}");
}

#[test]
fn slot_map_rejects_parity_violation() {
    // n1 拿 5 槽 > parity 4：丢 n1 后只剩 7 槽 < data=8，不可重建
    let mut slots = positional_slots();
    slots[3] = ("n1", 0);
    slots[6] = ("n1", 0);
    let e = err_of(vec![four_nodes()], vec![Some(map(1, &slots))]);
    assert!(e.contains("n1") && e.contains("超过 parity 4"), "{e}");

    // 单节点豁免：12 槽全归唯一节点（丢节点 = 全丢，无节点级可保护项）
    let all_n1: Vec<(&str, u16)> = (0..12u16).map(|i| ("n1", i % 3)).collect();
    let ok = ClusterSets::with_slot_maps(
        "n1",
        vec![vec![n("n1", 3)]],
        vec![Some(map(1, &all_n1))],
        &ec84(),
    );
    assert!(ok.is_ok(), "{:?}", ok.err());
}

#[test]
fn slot_map_rejects_too_few_slots() {
    let slots = positional_slots();
    let e = err_of(vec![four_nodes()], vec![Some(map(1, &slots[..11]))]);
    assert!(e.contains("11") && e.contains("12"), "{e}");
}

#[test]
fn slot_map_rejects_too_many_slots() {
    let slots: Vec<(&str, u16)> = (0..256u16).map(|i| ("n1", i % 3)).collect();
    let e = err_of(vec![vec![n("n1", 3)]], vec![Some(map(1, &slots))]);
    assert!(e.contains("256") && e.contains("255"), "{e}");
}

/// None/Some 混用、map 数与 set 数不符、跨 set epoch 不一致均拒绝。
#[test]
fn slot_map_rejects_mixed_and_epoch_mismatch() {
    let two_sets = || vec![four_nodes(), four_nodes()];
    let e = ClusterSets::with_slot_maps(
        "n1",
        two_sets(),
        vec![Some(map(1, &positional_slots())), None],
        &ec84(),
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("混用"), "{e}");

    let e = ClusterSets::with_slot_maps(
        "n1",
        two_sets(),
        vec![
            Some(map(1, &positional_slots())),
            Some(map(2, &positional_slots())),
        ],
        &ec84(),
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("epoch 不一致"), "{e}");

    let e = ClusterSets::with_slot_maps(
        "n1",
        two_sets(),
        vec![Some(map(1, &positional_slots()))],
        &ec84(),
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("与 set 数 2 不一致"), "{e}");
}

// ---------- T4：cluster-shrink plan（只读校验/报告） ----------

use super::VersioningMode;
use super::shrink::acquire_shrink_lock;
use super::tests::Cluster;
use crate::mesh::{DriveSpace, NodeHealth};
use bytes::Bytes;

/// 旧拓扑 4×3 positional（槽 0..8 = n1/n2/n3，9..11 = n4）；新拓扑退役 n4，
/// 槽 9/10/11 改归 n1/n2/n3 的 drive 0——槽数与保留节点盘序都不变。
fn retired_n4_target(epoch: u64) -> ClusterSets {
    let mut slots = positional_slots();
    slots[9] = ("n1", 0);
    slots[10] = ("n2", 0);
    slots[11] = ("n3", 0);
    ClusterSets::with_slot_maps(
        "n1",
        vec![vec![n("n1", 3), n("n2", 3), n("n3", 3)]],
        vec![Some(map(epoch, &slots))],
        &ec84(),
    )
    .unwrap()
}

fn target_with_slots(nodes: Vec<NodeSpec>, slots: &[(&str, u16)]) -> ClusterSets {
    ClusterSets::with_slot_maps("n1", vec![nodes], vec![Some(map(2, slots))], &ec84()).unwrap()
}

#[tokio::test]
async fn shrink_plan_reports_affected_slots_and_bytes() {
    let c = Cluster::new_in_maintenance(4, 3).await;
    let s = &c.stores[0];
    s.create_bucket("alpha").await.unwrap();
    // EC 对象（> inline_threshold）：12 片按 identity place 落 12 槽
    s.put_object("alpha", "ec", Bytes::from(vec![7u8; 300 * 1024]), None)
        .await
        .unwrap();
    // inline 对象：计入 objects/versions，但无分片、不得计入 shards/bytes
    s.put_object("alpha", "small", Bytes::from_static(b"inline"), None)
        .await
        .unwrap();
    // 3 版本 EC 对象：versions=3，全部版本都影响槽 9–11
    for _ in 0..3 {
        s.put_object_stream_with_meta(
            "alpha",
            "ver",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from(vec![9u8; 300 * 1024]))
            })),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    }

    let report = s.shrink_plan(&retired_n4_target(2)).await.unwrap();
    assert_eq!(report.plan.old_epoch, 1, "positional v1 → epoch 1");
    assert_eq!(report.plan.new_epoch, 2);
    assert_eq!(report.plan.old_hash.len(), 16);
    assert_eq!(report.plan.new_hash.len(), 16);
    assert_eq!(report.plan.slot_counts, vec![12], "槽数不变量");
    assert_eq!(report.plan.moves.len(), 3, "{:?}", report.plan.moves);
    assert!(
        report.plan.moves.iter().all(|m| m.from_node == "n4"),
        "{:?}",
        report.plan.moves
    );
    assert_eq!(report.plan.retired_nodes, vec!["n4".to_string()]);
    assert_eq!(report.plan.objects, 3, "含 inline 对象");
    assert_eq!(report.plan.versions, 5, "1 EC + 1 inline + 3 版本");
    assert_eq!(report.plan.namespace_digest.len(), 64);
    // 只报被迁移的 3 个槽；inline 对象不计 shards，但 objects 按命中槽计 2（ec + ver）
    assert_eq!(
        report
            .slots
            .iter()
            .map(|st| (st.set, st.slot, st.objects, st.versions, st.shards))
            .collect::<Vec<_>>(),
        vec![(0, 9, 2, 4, 4), (0, 10, 2, 4, 4), (0, 11, 2, 4, 4)]
    );
    assert!(report.slots.iter().all(|st| st.bytes > 0));
    // 12 槽 - 单节点最多 4 槽 = 8 < data+1=9：停一节点后写无余量（只告警不阻断）
    assert!(
        report.warnings.iter().any(|w| w.contains("写余量")),
        "{:?}",
        report.warnings
    );
}

/// 单机（无 [cluster]）→ 只支持重新灌入；shrink 必须拒绝且文案指路 README。
#[tokio::test]
async fn shrink_plan_rejects_single_node_store() {
    let dir = tempfile::tempdir().unwrap();
    let s = super::Store::open(super::StoreConfig::new(dir.path()))
        .await
        .unwrap();
    let target = ClusterSets::new("n1", vec![four_nodes()], &ec84()).unwrap();
    let e = s.shrink_plan(&target).await.unwrap_err().to_string();
    assert!(e.contains("重新灌入") && e.contains("[cluster]"), "{e}");
}

#[tokio::test]
async fn shrink_plan_requires_maintenance() {
    let c = Cluster::new(4, 3).await;
    let e = c.stores[0]
        .shrink_plan(&retired_n4_target(2))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("maintenance=true"), "{e}");
}

#[tokio::test]
async fn shrink_plan_rejects_slot_count_change() {
    let c = Cluster::new_in_maintenance(4, 3).await;
    let ids = ["n1", "n2", "n3", "n4"];
    // 16 槽、每节点仍 ≤ parity 4：构造器接受，plan 必须拒绝（槽数变化会让 place 错片）
    let slots: Vec<(&str, u16)> = (0..16)
        .map(|i| (ids[i % 4], ((i / 4) % 3) as u16))
        .collect();
    let target = target_with_slots(vec![n("n1", 3), n("n2", 3), n("n3", 3), n("n4", 3)], &slots);
    let e = c.stores[0]
        .shrink_plan(&target)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("槽数变化 12") && e.contains("16"), "{e}");
}

#[tokio::test]
async fn shrink_plan_rejects_drive_count_change() {
    let c = Cluster::new_in_maintenance(4, 3).await;
    let ids = ["n1", "n2", "n3"];
    let slots: Vec<(&str, u16)> = (0..12)
        .map(|i| (ids[i % 3], ((i / 3) % 2) as u16))
        .collect();
    let target = target_with_slots(vec![n("n1", 2), n("n2", 2), n("n3", 2)], &slots);
    let e = c.stores[0]
        .shrink_plan(&target)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("盘数变化 3 → 2"), "{e}");
}

#[test]
fn shrink_target_parity_violation_rejected_at_build() {
    // 目标每节点 > parity 的槽表在构造期即拒；plan 内部还有一道防御性复核（不可达分支）。
    let ids = ["n1", "n2", "n3", "n4"];
    let mut slots: Vec<(&str, u16)> = (0..12).map(|i| (ids[i % 4], (i / 4) as u16)).collect();
    slots[3] = ("n1", 1);
    slots[7] = ("n1", 2); // n1 拿 5 槽 > parity 4
    let e = err_of(vec![four_nodes()], vec![Some(map(1, &slots))]);
    assert!(e.contains("n1") && e.contains("超过 parity 4"), "{e}");
}

#[tokio::test]
async fn shrink_plan_rejects_inflight_mpu_on_retired_node() {
    let c = Cluster::new_in_maintenance(4, 3).await;
    c.stores[0].create_bucket("alpha").await.unwrap();
    c.stores[3]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let e = c.stores[0]
        .shrink_plan(&retired_n4_target(2))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("未完成 MPU") && e.contains("n4"), "{e}");
}

#[tokio::test]
async fn shrink_plan_rejects_active_lock() {
    let c = Cluster::new_in_maintenance(4, 3).await;
    let _lock = acquire_shrink_lock(c.stores[0].root()).unwrap();
    let e = c.stores[0]
        .shrink_plan(&retired_n4_target(2))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("active.lock"), "{e}");
}

fn health(node: &str, epoch: u64, hash: &str, maintenance: bool, free: u64) -> NodeHealth {
    NodeHealth {
        node_id: node.into(),
        version: String::new(),
        slots: vec![],
        buckets: 0,
        data_wire_version: 1,
        topo_epoch: epoch,
        topo_hash: hash.into(),
        drives: vec![DriveSpace {
            set: 0,
            drive: 0,
            free_bytes: free,
        }],
        maintenance,
    }
}

#[test]
fn peer_health_requires_maintenance_and_matching_fingerprint() {
    let ok = [health("n1", 2, "h", true, 0), health("n2", 2, "h", true, 0)];
    super::shrink::validate_peer_health(&ok, 2, "h").unwrap();

    let mut no_maint = ok.clone();
    no_maint[1].maintenance = false;
    let e = super::shrink::validate_peer_health(&no_maint, 2, "h")
        .unwrap_err()
        .to_string();
    assert!(e.contains("n2") && e.contains("maintenance"), "{e}");

    let mut mixed_epoch = ok.clone();
    mixed_epoch[0].topo_epoch = 1;
    let e = super::shrink::validate_peer_health(&mixed_epoch, 2, "h")
        .unwrap_err()
        .to_string();
    assert!(e.contains("epoch") && e.contains("n1"), "{e}");

    let mut mixed_hash = ok.clone();
    mixed_hash[1].topo_hash = "other".into();
    let e = super::shrink::validate_peer_health(&mixed_hash, 2, "h")
        .unwrap_err()
        .to_string();
    assert!(e.contains("hash") && e.contains("n2"), "{e}");
}

#[test]
fn capacity_allows_exact_fit_and_rejects_shortfall_or_missing_drive() {
    let peers = [health("n1", 2, "h", true, 100)];
    let need = [(0u16, "n1".to_string(), 0u16, 100u64)];
    super::shrink::validate_capacity(&need, &peers).unwrap();

    let need = [(0u16, "n1".to_string(), 0u16, 101u64)];
    let e = super::shrink::validate_capacity(&need, &peers)
        .unwrap_err()
        .to_string();
    assert!(e.contains("101") && e.contains("100"), "{e}");

    let need = [(0u16, "n1".to_string(), 1u16, 1u64)];
    let e = super::shrink::validate_capacity(&need, &peers)
        .unwrap_err()
        .to_string();
    assert!(e.contains("盘 1") && e.contains("可用空间"), "{e}");

    let need = [(0u16, "n9".to_string(), 0u16, 0u64)];
    let e = super::shrink::validate_capacity(&need, &peers)
        .unwrap_err()
        .to_string();
    assert!(e.contains("n9") && e.contains("不可达"), "{e}");
}
