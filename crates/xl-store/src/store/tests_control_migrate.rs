//! Phase 3 前置：集群 legacy 控制对象补证书。
//!
//! Task 1：只读 raw-copy RPC——返回每盘原始 `xl.meta` 字节，不解码、不选举、不过滤证书。

use super::*;
use crate::layout;
use crate::mesh::MetaRead;
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
        assert_eq!(&copy.meta[..], &disk[..], "槽 {} 字节必须与盘上一致", copy.slot);
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
