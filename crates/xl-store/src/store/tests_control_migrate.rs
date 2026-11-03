//! Phase 3 前置：集群 legacy 控制对象补证书。
//!
//! Task 1：只读 raw-copy RPC——返回每盘原始 `xl.meta` 字节，不解码、不选举、不过滤证书。

use super::*;
use crate::layout;
use crate::mesh::{MeshTransport as _, MetaRead};
use crate::meta::XlMeta;

/// 所有副本原样返回（含 `control_committed=None`），忽略 `.new` 暂存；命名空间外拒绝。
#[tokio::test]
async fn mesh_read_control_raw_returns_all_copies_ignoring_certificate() {
    let c = Cluster::new(4, 3).await; // 12 槽 → 8+4，Q_s = 9；节点 0 拥有槽 0..3
    let k = "control/raw-all-copies";
    assert_eq!(
        c.stores[0]
            .put_system_object(k, b"v1", None, 1)
            .await
            .unwrap(),
        1
    );
    // slot 0 退回 legacy 形态：raw 扫描必须仍能看到它（ReadMeta 不会返回未认证副本）。
    overwrite_control_slot(&c, 0, k, |m| m.control_committed = None).await;
    // 伪 `.new`：内容非法，若被读取会解码失败；raw 读取只能碰 `xl.meta`。
    let dir0 = layout::object_dir_for(&c.dirs[0][0], layout::SYS_DIR, k).unwrap();
    std::fs::write(dir0.join("xl.meta.new"), b"not-an-xlmeta").unwrap();

    let r = MetaRead {
        bucket: layout::SYS_DIR.into(),
        key: k.into(),
    };
    let resp = c.stores[0].mesh_read_control_raw(r).await.unwrap();
    assert_eq!(resp.slots_ok, 3, "本节点 3 盘全部被确定性观察");
    assert_eq!(resp.copies.len(), 3);
    let mut slots: Vec<u16> = resp.copies.iter().map(|c| c.slot).collect();
    slots.sort_unstable();
    assert_eq!(slots, vec![0, 1, 2]);

    for copy in &resp.copies {
        let drive = &c.dirs[0][copy.slot as usize];
        let path = layout::object_dir_for(drive, layout::SYS_DIR, k)
            .unwrap()
            .join("xl.meta");
        let disk = tokio::fs::read(&path).await.unwrap();
        assert_eq!(
            &copy.meta[..],
            &disk[..],
            "槽 {} 字节必须与盘上一致",
            copy.slot
        );
    }
    let slot0 = XlMeta::decode(
        &resp
            .copies
            .iter()
            .find(|c| c.slot == 0)
            .expect("slot 0 副本")
            .meta,
    )
    .unwrap();
    assert_eq!(slot0.control_committed, None, "raw 不得过滤未认证副本");

    // 命名空间门禁：非 `.oss.sys` bucket、非 `control/` 键均拒绝。
    for bad in [
        MetaRead {
            bucket: "buck".into(),
            key: k.into(),
        },
        MetaRead {
            bucket: layout::SYS_DIR.into(),
            key: "other/key".into(),
        },
    ] {
        let err = c.stores[0].mesh_read_control_raw(bad).await.unwrap_err();
        assert!(
            matches!(err, crate::error::Error::InvalidArgument(_)),
            "实得：{err:?}"
        );
    }
}

/// 混版门禁：任一 peer 未宣告 `control_raw` → `PeerIncompatible`（旧节点会静默忽略
/// 新 RPC，能力位是唯一防线）；恢复真实值后放行。
#[tokio::test]
async fn control_raw_gate_rejects_missing_capability() {
    let c = Cluster::new(4, 3).await;
    c.mesh.set_control_raw("n2", Some(false));
    let err = c.stores[0]
        .ensure_control_raw_compatible()
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::PeerIncompatible(_)),
        "实得：{err:?}"
    );
    c.mesh.set_control_raw("n2", None);
    c.stores[0]
        .ensure_control_raw_compatible()
        .await
        .expect("能力恢复后必须放行");
}

/// InProcessMesh 传输转发：节点 0 的 transport 读节点 2（n3，槽 6..8）的原始副本。
#[tokio::test]
async fn in_process_mesh_forwards_control_raw() {
    let c = Cluster::new(4, 3).await;
    let k = "control/transport-forward";
    c.stores[2]
        .put_system_object(k, b"v1", None, 1)
        .await
        .unwrap();
    let r = MetaRead {
        bucket: layout::SYS_DIR.into(),
        key: k.into(),
    };
    let resp = c.mesh.read_control_raw("n3", r).await.unwrap();
    assert_eq!(resp.slots_ok, 3, "n3 的 3 盘全部被观察");
    let mut slots: Vec<u16> = resp.copies.iter().map(|c| c.slot).collect();
    slots.sort_unstable();
    assert_eq!(slots, vec![6, 7, 8]);
}

// ---------- Task 3：全槽扫描、规范分组与状态判定（只读） ----------

/// 生产形态的 legacy 控制 meta：已提交前的第一阶段被剥离，只剩 `control_committed=None`。
pub(super) fn legacy_meta(key: &str, version: u64, token: u64, data: &[u8]) -> XlMeta {
    let mut m = crate::store::control::control_meta(key, version, token, Some(data), false);
    m.control_committed = None;
    m
}

/// 带提交证书的控制 meta（`Some(true)`）。
pub(super) fn certified_meta(key: &str, version: u64, token: u64, data: &[u8]) -> XlMeta {
    let mut m = crate::store::control::control_meta(key, version, token, Some(data), false);
    m.control_committed = Some(true);
    m
}

/// 全部槽的 `xl.meta` / `.new` 字节快照（失败路径零写入断言）。
pub(super) async fn snapshot(c: &Cluster, key: &str) -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> {
    let mut out = Vec::new();
    for drive in c.dirs.iter().flatten() {
        let dir = layout::object_dir_for(drive, layout::SYS_DIR, key).unwrap();
        out.push((
            tokio::fs::read(dir.join("xl.meta")).await.ok(),
            tokio::fs::read(dir.join("xl.meta.new")).await.ok(),
        ));
    }
    out
}

/// 规范内容只剥 `fence_token`/`control_committed`：token 与证书不同 → 同组；
/// 数据/版本/tombstone 不同 → 不同组。
#[test]
fn legacy_scan_groups_ignoring_token_and_certificate() {
    use super::super::control_migrate::canonical_content;
    let a = legacy_meta("control/g", 1, 5, b"v1");
    // 副本是同一份 meta 的复制：只允许 token/证书不同。
    let mut b = a.clone();
    b.fence_token = Some(9);
    b.control_committed = Some(true);
    assert_eq!(
        canonical_content(&a).unwrap(),
        canonical_content(&b).unwrap(),
        "token/证书不参与规范内容"
    );
    let mut c = a.clone();
    c.versions[0].inline.as_mut().unwrap().bytes = b"v2".to_vec();
    assert_ne!(
        canonical_content(&a).unwrap(),
        canonical_content(&c).unwrap()
    );
    let mut d = a.clone();
    d.control_version = Some(2);
    assert_ne!(
        canonical_content(&a).unwrap(),
        canonical_content(&d).unwrap()
    );
    let mut e = a.clone();
    e.control_deleted = Some(true);
    assert_ne!(
        canonical_content(&a).unwrap(),
        canonical_content(&e).unwrap()
    );
}

/// 12 槽（Q_s=9）：唯一同内容组恰 9 份 → Certifiable，chosen 指向该组。
#[tokio::test]
async fn legacy_scan_certifiable_at_twelve_slots() {
    let c = Cluster::new(4, 3).await;
    let k = "control/scan-certifiable";
    let m = legacy_meta(k, 1, 1, b"v1");
    for slot in 0..9 {
        write_control_slot(&c, slot, k, &m).await;
    }
    let before = snapshot(&c, k).await;
    let scan = c.stores[0].scan_legacy_control_object(k).await.unwrap();
    assert_eq!(scan.state, crate::store::LegacyControlState::Certifiable);
    assert_eq!((scan.n, scan.q_s, scan.observed), (12, 9, 12));
    assert_eq!(scan.copies.len(), 9);
    assert_eq!(scan.groups.len(), 1);
    let chosen = scan.chosen.expect("Certifiable 必有候选组");
    assert_eq!(scan.groups[chosen].copies, 9);
    assert_eq!(scan.groups[chosen].certified, 0);
    assert_eq!(snapshot(&c, k).await, before, "扫描必须零写入");
}

/// 8 份 < Q_s=9 → CorruptMeta，磁盘字节不变。
#[tokio::test]
async fn legacy_scan_below_quorum_fails() {
    let c = Cluster::new(4, 3).await;
    let k = "control/scan-below-quorum";
    let m = legacy_meta(k, 1, 1, b"v1");
    for slot in 0..8 {
        write_control_slot(&c, slot, k, &m).await;
    }
    let before = snapshot(&c, k).await;
    let err = c.stores[0].scan_legacy_control_object(k).await.unwrap_err();
    assert!(
        matches!(err, crate::error::Error::CorruptMeta(_)),
        "实得：{err:?}"
    );
    assert_eq!(snapshot(&c, k).await, before);
}

/// 任何 `Some(false)` 第一阶段残骸 / 非控制形态 / 非法键都整键拒绝。
#[tokio::test]
async fn legacy_scan_refuses_phase1_residue_and_non_control() {
    let c = Cluster::new(4, 3).await;
    let k = "control/scan-residue";
    let m = legacy_meta(k, 1, 1, b"v1");
    for slot in 0..9 {
        write_control_slot(&c, slot, k, &m).await;
    }
    let mut residue = m.clone();
    residue.control_committed = Some(false);
    write_control_slot(&c, 0, k, &residue).await;
    let before = snapshot(&c, k).await;
    let err = c.stores[0].scan_legacy_control_object(k).await.unwrap_err();
    assert!(
        matches!(err, crate::error::Error::CorruptMeta(_)),
        "实得：{err:?}"
    );
    assert_eq!(snapshot(&c, k).await, before);

    let k2 = "control/scan-non-control";
    let plain = XlMeta {
        object_key: k2.into(),
        versions: Vec::new(),
        control_version: None,
        fence_token: None,
        control_deleted: None,
        control_committed: None,
    };
    for slot in 0..9 {
        write_control_slot(&c, slot, k2, &plain).await;
    }
    let err = c.stores[0]
        .scan_legacy_control_object(k2)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::CorruptMeta(_)),
        "实得：{err:?}"
    );
    let err = c.stores[0]
        .scan_legacy_control_object("objects/not-control")
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::InvalidKey(_)),
        "实得：{err:?}"
    );
}

/// 不完备观察（节点宕机 / 本机盘摘除）→ fail-closed，不得用部分副本下结论。
#[tokio::test]
async fn legacy_scan_requires_all_slots() {
    let c = Cluster::new(4, 3).await;
    let k = "control/scan-coverage";
    let m = legacy_meta(k, 1, 1, b"v1");
    for slot in 0..9 {
        write_control_slot(&c, slot, k, &m).await;
    }
    let before = snapshot(&c, k).await;
    c.mesh.down("n3");
    // 不可达节点既可能在能力探测（PeerIncompatible）也可能在覆盖计数（CorruptMeta）
    // 处失败；两者都必须 fail-closed。
    assert!(
        c.stores[0].scan_legacy_control_object(k).await.is_err(),
        "节点宕机时扫描必须失败"
    );
    c.mesh.up("n3");

    let sys = c.dirs[0][0].join(layout::SYS_DIR);
    let hidden = c.dirs[0][0].join(".oss.sys.offline");
    std::fs::rename(&sys, &hidden).unwrap();
    let err = c.stores[0].scan_legacy_control_object(k).await.unwrap_err();
    assert!(
        matches!(err, crate::error::Error::CorruptMeta(_)),
        "实得：{err:?}"
    );
    std::fs::rename(&hidden, &sys).unwrap();
    assert_eq!(snapshot(&c, k).await, before);
}

/// 拓扑无法证明唯一性（2Q_s <= N）→ Unsupported，不读不写。
#[tokio::test]
async fn legacy_scan_refuses_small_topology() {
    let c = Cluster::new(3, 2).await; // 6 槽、data=2、Q_s=3 → 2*3 <= 6
    let err = c.stores[0]
        .scan_legacy_control_object("control/small")
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::Unsupported(_)),
        "实得：{err:?}"
    );
}

/// 混版：peer 缺 control_raw → PeerIncompatible（旧节点会静默忽略新 RPC）。
#[tokio::test]
async fn legacy_scan_refuses_missing_control_raw() {
    let c = Cluster::new(4, 3).await;
    c.mesh.set_control_raw("n2", Some(false));
    let err = c.stores[0]
        .scan_legacy_control_object("control/mixed")
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::PeerIncompatible(_)),
        "实得：{err:?}"
    );
}

/// 有证书时按 rank 取最高证书组：v1 保留证书 1 份被忽略，v2 证书 4 份（Q_s=4）
/// → AlreadyCommitted(v2)。
#[tokio::test]
async fn legacy_scan_ranks_preserved_old_certificate() {
    let c = Cluster::new(7, 1).await; // 7 槽、data=3、Q_s=4、2Q_s=8>7
    let k = "control/scan-rank";
    let v2 = certified_meta(k, 2, 10, b"v2");
    for slot in 0..4 {
        write_control_slot(&c, slot, k, &v2).await;
    }
    write_control_slot(&c, 4, k, &certified_meta(k, 1, 5, b"v1")).await;
    let scan = c.stores[0].scan_legacy_control_object(k).await.unwrap();
    assert_eq!(
        scan.state,
        crate::store::LegacyControlState::AlreadyCommitted
    );
    let chosen = scan.chosen.expect("已提交也应有候选组");
    assert_eq!(scan.groups[chosen].version, 2);
    assert_eq!(scan.groups[chosen].certified, 4);
    assert_eq!(scan.q_s, 4);
}

/// 纯分类：多个同内容组都达到 Q_s（2Q_s>N 下理论不可达）→ 防御性拒绝。
#[test]
fn classify_rejects_multiple_quorum_groups() {
    use crate::store::control_migrate::{LegacyContentGroup, classify};
    let g = |version: u64, byte: u8| LegacyContentGroup {
        version,
        content: [byte; 32],
        copies: 9,
        certified: 0,
        max_token: 1,
    };
    let err = classify(&[g(1, 1), g(2, 2)], 9).unwrap_err();
    assert!(
        matches!(err, crate::error::Error::CorruptMeta(_)),
        "实得：{err:?}"
    );
}
