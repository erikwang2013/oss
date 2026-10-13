//! 多盘寻址与分发（M2b-2）。
//!
//! 铁律⑤：分片位置只存**拓扑下标**——分片 → 盘的映射是确定性函数
//! （`drive_index`），盘路径由下标解引用 drives[i]，换挂载点/换盘不影响寻址。
//!
//! 每盘持有完整命名空间：对象目录（含 xl.meta 副本）在各盘同构存在；
//! 版本目录 `<drive>/<bucket>/<展平键>/<uuid>/` 里放**落到本盘的那些分片**。
//! 单盘形态（drives.len() == 1）下所有分片都在 drive 0，与 M2b-1 兼容。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::layout;
use crate::mesh::MeshTransport;
use crate::meta::ShardRef;

use super::Store;

/// 分片 → 盘下标（确定性；单盘恒 0）。M2b 单 set = 全部盘；多 set 是 M4 的事。
pub fn drive_index(shard: u8, n_drives: usize) -> usize {
    (shard as usize) % n_drives.max(1)
}

/// 对象 → set 映射（设计 §5.3 的确定性哈希）。
/// **留形不留行为**：M2b 单 set（sets == 1）取模恒 0；M4 引入多 set / rebalance 时本公式直接生效。
#[allow(dead_code)]
pub fn set_index(bucket: &str, key: &str, sets: usize) -> usize {
    let h = crc32c::crc32c(format!("{bucket}/{key}").as_bytes());
    (h as usize) % sets.max(1)
}

/// 分片在指定盘上的路径：`<drive>/<bucket>/<展平键>/<uuid>/part.<part>.<shard>`。
pub fn shard_path(
    drive: &Path,
    bucket: &str,
    key: &str,
    uuid: &str,
    s: &ShardRef,
) -> Result<PathBuf> {
    Ok(layout::object_dir(drive, bucket, key)?
        .join(uuid)
        .join(format!("part.{}.{}", s.part, s.shard)))
}

/// 盘下标 → 盘根（越界返回 None——meta 里的 drive 下标是外部输入，须防呆）。
pub fn drive_root(drives: &[PathBuf], index: u8) -> Option<&Path> {
    drives.get(index as usize).map(PathBuf::as_path)
}

/// 盘存活探测（探针最小版）：`.oss.sys` 目录存在即存活——它在 `Store::open` 时建于每盘。
///
/// 为什么写盘前必须探测：挂载点掉线后，挂载点**目录**仍留在父文件系统上，
/// 直接 `create_dir_all` + 写会把数据写进根盘（并误当写成功）。探测为「不在册」的盘
/// 一律跳过（不创建任何目录），由 quorum 计数消化。
pub async fn drive_up(drive: &Path) -> bool {
    matches!(
        tokio::fs::try_exists(drive.join(layout::SYS_DIR)).await,
        Ok(true)
    )
}

// ---------- M3：全局槽位表与落点解析（集群模式；单节点走退化路径） ----------

/// 集群节点声明（静态拓扑：扁平 node × drive，**无 DC/Region/AZ**——设计 §5.3）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSpec {
    pub id: String,
    pub addr: String,
    pub drive_count: u16,
}

/// 全局槽位表：按配置顺序拼接各节点的盘，`slot = 前序节点盘数之和 + 本地盘序`。
/// **单节点（未配 cluster）不构造本表**——Store 走「槽位 = 本机盘下标」的退化路径，
/// 与 M2b 逐字节一致（`ShardRef.drive` 语义升级不动 xl.meta 格式）。
#[derive(Clone, Debug)]
pub struct ClusterTopology {
    pub self_idx: usize,
    pub nodes: Vec<NodeSpec>,
    /// bases[i] = 节点 i 的首个全局槽位（升序；node_of_slot 二分用）
    bases: Vec<u16>,
    slot_count: u16,
}

impl ClusterTopology {
    /// `ec` 提供 data+parity：槽位数不足 `data+parity` 的集群连 EC 8+4 都摆不下（oss.toml 同口径）。
    pub fn new(self_idx: usize, nodes: Vec<NodeSpec>, ec: &super::ec::EcConfig) -> Result<Self> {
        if nodes.is_empty() {
            return Err(Error::Encode("集群节点列表为空".into()));
        }
        if self_idx >= nodes.len() {
            return Err(Error::Encode(format!(
                "self_idx {self_idx} 越界（共 {} 节点）",
                nodes.len()
            )));
        }
        let mut bases = Vec::with_capacity(nodes.len());
        let mut total: u16 = 0;
        for (i, n) in nodes.iter().enumerate() {
            if n.id.is_empty() {
                return Err(Error::Encode(format!("节点 #{i} id 为空")));
            }
            if nodes[..i].iter().any(|m| m.id == n.id) {
                return Err(Error::Encode(format!("集群节点 id 重复：{}", n.id)));
            }
            if n.drive_count == 0 {
                return Err(Error::Encode(format!("节点 {} drive_count 为 0", n.id)));
            }
            bases.push(total);
            total = total
                .checked_add(n.drive_count)
                .ok_or_else(|| Error::Encode("槽位数溢出 u16".into()))?;
        }
        let min = ec.data as u16 + ec.parity as u16;
        if total > 255 {
            return Err(Error::Encode(format!(
                "槽位数 {total} 超过 255（ShardRef.drive 为 u8）"
            )));
        }
        if total < min {
            return Err(Error::Encode(format!(
                "槽位数 {total} 不足 EC {}+{}（集群须 ≥ {min}）",
                ec.data, ec.parity
            )));
        }
        Ok(Self {
            self_idx,
            nodes,
            bases,
            slot_count: total,
        })
    }

    pub fn slot_count(&self) -> u16 {
        self.slot_count
    }

    /// 槽位 → 节点下标（bases 升序 + partition_point）。
    pub fn node_of_slot(&self, slot: u16) -> usize {
        self.bases.partition_point(|&b| b <= slot) - 1
    }

    /// 槽位归属本节点 → Some(本地盘序)；否则 None（= 远端）。越界也 None。
    pub fn local_drive_of_slot(&self, slot: u16) -> Option<usize> {
        if slot >= self.slot_count {
            return None;
        }
        let node = self.node_of_slot(slot);
        (node == self.self_idx).then(|| (slot - self.bases[node]) as usize)
    }

    pub fn local_slot_base(&self) -> u16 {
        self.bases[self.self_idx]
    }

    pub fn self_node(&self) -> &NodeSpec {
        &self.nodes[self.self_idx]
    }

    /// 分片 → 全局槽位（`shard % slot_count`；单 set 内的确定性哈希）。
    pub fn slot_of(&self, shard: u8) -> u16 {
        (shard as u16) % self.slot_count
    }
}

/// 集群运行面：静态拓扑 + 节点间传输（trait 见 [`crate::mesh`]；tonic 实现由 xl-client 注入）。
pub struct ClusterConfig {
    pub topo: ClusterTopology,
    pub transport: Arc<dyn MeshTransport>,
}

impl std::fmt::Debug for ClusterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterConfig")
            .field("topo", &self.topo)
            .finish_non_exhaustive()
    }
}

/// 分片落点：单节点恒 Local（与 M2b 同路径）；集群按槽位归属本机/远端。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardTarget<'a> {
    Local { drive: &'a Path, slot: u16 },
    Remote { node: &'a str, slot: u16 },
}

impl Store {
    /// 分片下标（0..data+parity）→ 槽位。写路径在生成 ShardRef 前用它；
    /// 单机 = 本机盘下标（drive_index，与 M2b 逐字节一致），集群 = 全局槽位。
    pub(in crate::store) fn slot_of_shard(&self, shard: u8) -> u16 {
        match self.cfg.cluster.as_deref() {
            None => drive_index(shard, self.cfg.drives.len()) as u16,
            Some(cl) => cl.topo.slot_of(shard),
        }
    }

    /// 槽位 → 落点。越界返回 None（meta 里的 drive 是外部输入，须防呆）。
    pub(in crate::store) fn slot_target(&self, slot: u16) -> Option<ShardTarget<'_>> {
        match self.cfg.cluster.as_deref() {
            None => Some(ShardTarget::Local {
                drive: drive_root(&self.cfg.drives, slot as u8)?,
                slot,
            }),
            Some(cl) => {
                if slot >= cl.topo.slot_count() {
                    return None;
                }
                match cl.topo.local_drive_of_slot(slot) {
                    Some(d) => Some(ShardTarget::Local {
                        drive: drive_root(&self.cfg.drives, d as u8)?,
                        slot,
                    }),
                    None => Some(ShardTarget::Remote {
                        node: &cl.topo.nodes[cl.topo.node_of_slot(slot)].id,
                        slot,
                    }),
                }
            }
        }
    }

    /// meta 的 ShardRef → 落点（读路径统一入口）。
    pub(in crate::store) fn shard_target(&self, s: &ShardRef) -> Option<ShardTarget<'_>> {
        self.slot_target(s.drive as u16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drive_index_is_deterministic_and_single_drive_is_zero() {
        // 单盘：全部分片落 0（M2b-1 兼容）
        for i in 0..12u8 {
            assert_eq!(drive_index(i, 1), 0);
        }
        // 12 盘：一片一盘
        for i in 0..12u8 {
            assert_eq!(drive_index(i, 12), i as usize);
        }
        // 13 盘：分片铺满前 12 盘（第 13 盘不用）
        assert_eq!(drive_index(12, 13), 12);
        assert_eq!(drive_index(0, 13), 0);
    }

    #[test]
    fn set_index_single_set_is_zero_and_deterministic() {
        assert_eq!(set_index("bucket", "a/b.txt", 1), 0);
        // 多 set 时同键恒同 set（rebalance 之外不允许漂移）
        let a = set_index("bucket", "a/b.txt", 7);
        assert_eq!(a, set_index("bucket", "a/b.txt", 7));
        assert!(a < 7);
    }

    #[test]
    fn shard_path_uses_topology_index_only() {
        let p = shard_path(
            Path::new("/d0"),
            "bucket",
            "a/b.txt",
            "uuid-1",
            &ShardRef {
                drive: 3,
                part: 1,
                shard: 5,
                size: 10,
                crc32c: 0,
            },
        )
        .unwrap();
        assert_eq!(p, Path::new("/d0/bucket/a__XLDIR__b.txt/uuid-1/part.1.5"));
        // drive 下标是数据不是路径：shard_path 的盘由调用方解引用，这里不接受 drive 字段拼路径
        assert!(drive_root(&[PathBuf::from("/d0")], 3).is_none());
    }

    fn n(id: &str, dc: u16) -> NodeSpec {
        NodeSpec {
            id: id.into(),
            addr: String::new(),
            drive_count: dc,
        }
    }

    fn ec84() -> super::super::ec::EcConfig {
        super::super::ec::EcConfig::default()
    }

    #[test]
    fn topology_slot_truth_table_4x3() {
        let t = ClusterTopology::new(
            0,
            vec![n("n1", 3), n("n2", 3), n("n3", 3), n("n4", 3)],
            &ec84(),
        )
        .unwrap();
        assert_eq!(t.slot_count(), 12);
        // 4×3 = 12 槽：shard % 12 恰 1 片/槽（分布均匀、无空转节点）
        for i in 0..12u8 {
            assert_eq!(t.slot_of(i), i as u16);
        }
        // 槽位 → 节点归属（此配置下 slot 单调递增铺满）
        assert_eq!(t.node_of_slot(0), 0);
        assert_eq!(t.node_of_slot(2), 0);
        assert_eq!(t.node_of_slot(3), 1);
        assert_eq!(t.node_of_slot(11), 3);
        // 本机（n1）视角：slot 0..2 是本地盘 0..2，其余远端
        assert_eq!(t.local_slot_base(), 0);
        assert_eq!(t.local_drive_of_slot(2), Some(2));
        assert_eq!(t.local_drive_of_slot(3), None);
        assert_eq!(t.local_drive_of_slot(12), None, "越界不算本机");
    }

    #[test]
    fn topology_4x4_leaves_last_slots_empty() {
        // 配置反例（评审否决 4×4 的机制）：EC 只有 12 分片，`shard % 16` 只落槽 0..11，
        // 第 4 节点（槽 12..15）一个数据分片都没有——纯占位，停任一持片节点容错反更差。
        let t = ClusterTopology::new(0, vec![n("a", 4), n("b", 4), n("c", 4), n("d", 4)], &ec84())
            .unwrap();
        assert_eq!(t.slot_count(), 16);
        assert_eq!(t.slot_of(11), 11);
        assert!(
            (0..12u8).all(|i| t.slot_of(i) < 12),
            "12 个分片永远落不进槽 12..15"
        );
        assert_eq!(
            t.local_drive_of_slot(12),
            None,
            "槽 12 归属节点 d（远端），且永无分片"
        );
    }

    #[test]
    fn topology_rejects_bad_config() {
        let ec = ec84();
        // 槽位数不足 data+parity（3×3=9 < 12）：错误须指明两项数值
        let e = ClusterTopology::new(0, vec![n("a", 3), n("b", 3), n("c", 3)], &ec).unwrap_err();
        let msg = e.to_string();
        assert!(
            msg.contains('9') && msg.contains("12"),
            "应含实际值与下限：{msg}"
        );
        // self_idx 越界
        assert!(ClusterTopology::new(4, vec![n("a", 12)], &ec).is_err());
        // id 重复
        assert!(ClusterTopology::new(0, vec![n("a", 6), n("a", 6)], &ec).is_err());
        // drive_count 0
        assert!(ClusterTopology::new(0, vec![n("a", 0)], &ec).is_err());
        // 空节点表
        assert!(ClusterTopology::new(0, vec![], &ec).is_err());
    }
}
