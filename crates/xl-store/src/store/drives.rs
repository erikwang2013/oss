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
        Self::build(self_idx, nodes, ec, true)
    }

    /// `check_per_node_parity`：多节点拓扑须逐节点 `drive_count ≤ parity`（防丢节点不可重建）；
    /// **单节点拓扑豁免**（slice 5 规则把合法单节点集群一刀切拒了——xl-client 往返测试自切片 5
    /// 起静默红：单节点丢节点 = 全丢，与单机多盘同语义，无「节点级」可保护项）；
    /// flat 命名空间拓扑（不承载放置）同样不适用。build 为唯一入口（new/flat 共用）。
    fn build(
        self_idx: usize,
        nodes: Vec<NodeSpec>,
        ec: &super::ec::EcConfig,
        check_per_node_parity: bool,
    ) -> Result<Self> {
        if nodes.is_empty() {
            return Err(Error::Encode("集群节点列表为空".into()));
        }
        // M4 切片 5 校验：槽位总数 ≤ 255（`ShardRef.drive: u8` 的硬约束——超了会静默截断）；
        // node id 禁空白（拓扑记录 `node=X` 的解析边界）；drive_count ≤ parity（放置 v2 下
        // 保证「单节点丢失 ≤ parity 片」——>4 盘/节点时单节点可承载 5+ 片，丢节点即不可重建）
        let total_slots: u32 = nodes.iter().map(|n| n.drive_count as u32).sum();
        if total_slots > 255 {
            return Err(Error::Encode(format!(
                "槽位总数 {total_slots} 超过 255（ShardRef.drive 为 u8）"
            )));
        }
        for n in &nodes {
            if n.id.is_empty() || n.id.chars().any(char::is_whitespace) {
                return Err(Error::Encode(format!(
                    "节点 id 非法（含空白/空串）: {:?}",
                    n.id
                )));
            }
            if check_per_node_parity && nodes.len() > 1 && n.drive_count > ec.parity as u16 {
                return Err(Error::Encode(format!(
                    "节点 {} 盘数 {} 超过 parity {}（多节点集群里单节点可承载分片数须 ≤ parity，丢节点才可重建）",
                    n.id, n.drive_count, ec.parity
                )));
            }
        }
        // M4 切片 8：`self_idx == usize::MAX` = 占位「本节点不在此拓扑」（多 set 里非自属 set 的
        // 组内拓扑）；此时 local_drive_of_slot 天然全 None（node != sentinel），
        // 但 **self_node()/local_slot_base() 不得调用**（见方法注）。
        if self_idx != usize::MAX && self_idx >= nodes.len() {
            return Err(Error::Encode(format!(
                "self_idx {self_idx} 越界（共 {} 节点）",
                nodes.len()
            )));
        }
        let mut bases = Vec::with_capacity(nodes.len());
        let mut total: u16 = 0;
        for (i, n) in nodes.iter().enumerate() {
            // id 空/空白已在前段统一校验（含空串）；此处只查重复与盘数
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
        // （total>255 已在前段 u32 求和处校验；此处只需 checked_add 的溢出下不变量）
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

    /// 本节点首槽（仅对 self_idx 有效的拓扑调用；sentinel 占位会 panic——调用前用 self_idx_opt）。
    pub fn local_slot_base(&self) -> u16 {
        self.bases[self.self_idx]
    }

    /// 本节点在此拓扑中（非 sentinel 占位）。
    pub fn self_idx_opt(&self) -> Option<usize> {
        (self.self_idx < self.nodes.len()).then_some(self.self_idx)
    }

    /// 仅对 self_idx 有效的拓扑调用（sentinel 占位会 panic）。
    pub fn self_node(&self) -> &NodeSpec {
        &self.nodes[self.self_idx]
    }

    /// 分片 → 全局槽位（`shard % slot_count`；单 set 内的确定性哈希）。
    /// 分片放置（M4 切片 5）：`v2` 时 `(hash + shard) % slot_count` 全域分散（i 两两互异）；
    /// 否则 v1 恒等（`shard % slot_count`——≤12 槽逐字节零变化）。hash 见
    /// [`Store::placement_hash`]（sha256 前 8 字节：crc32c 线性，恶意 key 可定向聚集）。
    pub fn place(&self, hash: u64, shard: u8, v2: bool) -> u16 {
        if v2 {
            ((hash.wrapping_add(shard as u64)) % self.slot_count as u64) as u16
        } else {
            (shard as u16) % self.slot_count
        }
    }
}

/// 多 set 拓扑集合（M4 切片 8；子设计 v3）。**两个槽号域，绝不可混用**：
///
/// - `sets[i]` = set i 的**组内**槽空间（分片面：放置公式、ShardRef.drive、分片 RPC 的 set+slot）；
/// - `flat` = **全局扁平编号**（命名空间面：meta ack 链 `slot_base`/prepared_local/`node_of_slot`
///   回滚路由要求全集群唯一编号）——按 id 去重的全节点并集，每节点 drive_count = Σ 各 set 盘数；
///   k=1 时与单 set 的旧 topo 逐字节同构。
///
/// 命名空间面（meta/桶目录/版本目录）跨全部 set 镜像（S-L 口径），其广播循环一律用 `flat.nodes`。
#[derive(Clone, Debug)]
pub struct ClusterSets {
    pub sets: Vec<ClusterTopology>,
    pub flat: ClusterTopology,
    /// 自节点在 `data_dirs` 的 (set, offset, len) 段表（set 升序；顺序契约 = 子设计 S-A）。
    pub self_ranges: Vec<(u16, usize, usize)>,
}

impl ClusterSets {
    /// `sets_nodes[i]` = set i 的节点条目（同物理节点可在多个 set 各出一条，id 相同）。
    /// 校验：每 set 走 ClusterTopology::new 全套 + **同 id 跨 set 的 addr 必须一致**（RPC 只按
    /// id 路由——不一致则目标无定义，verifier M3）+ self 至少在一个 set + 总盘数 ≤255（口径 d）。
    pub fn new(
        self_id: &str,
        sets_nodes: Vec<Vec<NodeSpec>>,
        ec: &super::ec::EcConfig,
    ) -> Result<Self> {
        if sets_nodes.is_empty() {
            return Err(Error::Encode("cluster.sets 为空".into()));
        }
        // addr 一致性（跨 set 同 id）
        let mut addr_of: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
        for nodes in &sets_nodes {
            for n in nodes {
                match addr_of.get(n.id.as_str()) {
                    Some(a) if *a != n.addr => {
                        return Err(Error::Encode(format!(
                            "节点 {} 跨 set 的 addr 不一致（«{a}» vs «{}»）——RPC 只按 id 路由",
                            n.id, n.addr
                        )));
                    }
                    _ => {
                        addr_of.insert(&n.id, &n.addr);
                    }
                }
            }
        }
        let mut sets = Vec::with_capacity(sets_nodes.len());
        let mut self_ranges = Vec::new();
        let mut offset = 0usize;
        let mut self_seen = false;
        for (set_i, nodes) in sets_nodes.iter().enumerate() {
            let self_idx = nodes
                .iter()
                .position(|n| n.id == self_id)
                .unwrap_or(usize::MAX); // 非自属 set：sentinel 占位
            let topo = ClusterTopology::new(self_idx, nodes.clone(), ec)?;
            if let Some(si) = topo.self_idx_opt() {
                self_seen = true;
                self_ranges.push((set_i as u16, offset, nodes[si].drive_count as usize));
                offset += nodes[si].drive_count as usize;
            }
            sets.push(topo);
        }
        if !self_seen {
            return Err(Error::Encode(format!(
                "本节点 «{self_id}» 不在任何 set 的节点表内"
            )));
        }
        // flat：按 id 去重的全节点并集（首见顺序），drive_count = Σ
        let mut flat_nodes: Vec<NodeSpec> = Vec::new();
        for nodes in &sets_nodes {
            for n in nodes {
                match flat_nodes.iter_mut().find(|m| m.id == n.id) {
                    Some(m) => {
                        m.drive_count = m
                            .drive_count
                            .checked_add(n.drive_count)
                            .ok_or_else(|| Error::Encode(format!("节点 {} 盘数溢出 u16", n.id)))?
                    }
                    None => flat_nodes.push(n.clone()),
                }
            }
        }
        let self_flat = flat_nodes
            .iter()
            .position(|n| n.id == self_id)
            .ok_or_else(|| Error::Encode("flat：本节点缺席（不可达）".into()))?;
        // flat 不承载放置（命名空间面镜像）→ 不适用逐节点 parity 规则；≤255/≥min 仍在
        let flat = ClusterTopology::build(self_flat, flat_nodes, ec, false)?;
        Ok(Self {
            sets,
            flat,
            self_ranges,
        })
    }

    /// 单 set 便捷构造（k=1：flat 与 set0 同构）——单组部署与测试用。
    pub fn single(self_id: &str, nodes: Vec<NodeSpec>, ec: &super::ec::EcConfig) -> Result<Self> {
        Self::new(self_id, vec![nodes], ec)
    }

    /// (set, 组内盘序) → cfg.drives 扁平下标（= 自节点该 set 段 offset + 组内序）。
    /// `None` = 越界/该盘不在本节点。**洞 1 修法 (a)：一切物理盘寻址经此映射**。
    pub fn flat_of(&self, set: u16, ordinal: usize) -> Option<usize> {
        self.self_ranges
            .iter()
            .find(|(s, _, len)| *s == set && ordinal < *len)
            .map(|(_, off, _)| off + ordinal)
    }

    pub fn set_topo(&self, set: u16) -> Option<&ClusterTopology> {
        self.sets.get(set as usize)
    }
}

/// 集群运行面：静态拓扑 + 节点间传输（trait 见 [`crate::mesh`]；tonic 实现由 xl-client 注入）。
pub struct ClusterConfig {
    pub sets: ClusterSets,
    pub transport: Arc<dyn MeshTransport>,
    /// M4 切片 5 放置开关（显式；混版集群里未开的旧节点会按恒等公式服务 → fail-closed
    /// 可诊断——滚动升级需「全节点升级（仍 v1）→ 全节点开启 → rebalance」）。
    pub placement_v2: bool,
    /// M4 切片 8：桶 → set 解析。`None` = 未注入：单 set 由 Store 短路（不调用）；
    /// 多 set 命中 = fail-closed Err（配置错误不得静默）。
    pub resolver: Option<Arc<dyn crate::mesh::SetResolver>>,
}

impl std::fmt::Debug for ClusterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterConfig")
            .field("sets", &self.sets)
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
    /// 对象放置哈希（M4 切片 5）：sha256(bucket ‖ '/' ‖ key) 前 8 字节。
    /// 不用 crc32c——它是线性函数，恶意 key 能定向把对象聚集到指定节点/盘。
    pub(in crate::store) fn placement_hash(bucket: &str, key: &str) -> u64 {
        use sha2::Digest as _;
        let mut h = sha2::Sha256::new();
        h.update(bucket.as_bytes());
        h.update(b"/");
        h.update(key.as_bytes());
        let d = h.finalize();
        // 大端取值是**跨节点/跨版本的放置约定**（rebalance 目标槽由任一节点算，必须同式）——勿改
        u64::from_be_bytes(d[..8].try_into().expect("sha256 ≥ 8 字节"))
    }

    /// 放置 v2 是否启用：集群 = 显式开关（混版 fail-closed）；单机多盘 = 槽位超分片数即
    /// 自动（全本地，无混版问题：13 盘起步即分散）。
    pub(in crate::store) fn placement_v2(&self) -> bool {
        match self.cfg.cluster.as_deref() {
            Some(cl) => cl.placement_v2,
            None => {
                self.cfg.drives.len() > (self.cfg.ec.data as usize + self.cfg.ec.parity as usize)
            }
        }
    }

    /// 桶 → set（M4 切片 8；**数据面每 op 一次**）。单 set（含单机）短路静态 0（零新增查询，
    /// verifier Q6③）；多 set 走注入 resolver——`Ok(None)`（全可达且确无行）→ NoSuchBucket，
    /// `Err`（传输错/行分叉/超界）上抛 fail-closed，**绝不静默回落 0**（S-C）。
    pub(in crate::store) async fn set_of_bucket(&self, bucket: &str) -> Result<u16> {
        match self.resolve_bucket_set(bucket).await? {
            Some(s) => Ok(s),
            // 「全部可达且确无行」→ 桶不存在（放行会把分片写进错误组）
            None => Err(Error::NoSuchBucket(bucket.to_string())),
        }
    }

    /// 桶 → set 探测（**控制面/启动回填用**；与 set_of_bucket 同源、None 不转 NoSuchBucket）：
    /// 单机/单 set → `Some(0)`（零查询短路）；多 set → resolver（`Ok(None)` = 全部可达且确无
    /// 行——回填只在此态插 set=0，**任一传输错是不插行**，verifier 终审洞 2）。
    pub async fn resolve_bucket_set(&self, bucket: &str) -> Result<Option<u16>> {
        let Some(cl) = self.cfg.cluster.as_deref() else {
            return Ok(Some(0));
        };
        if cl.sets.sets.len() == 1 {
            return Ok(Some(0));
        }
        let resolver = cl
            .resolver
            .as_ref()
            .ok_or_else(|| Error::Encode("多 set 集群未注入 SetResolver".into()))?;
        match resolver.resolve_bucket_set(bucket).await? {
            Some(s) => {
                let s = u16::try_from(s).map_err(|_| Error::Encode(format!("set {s} 越界")))?;
                if cl.sets.set_topo(s).is_none() {
                    return Err(Error::Encode(format!(
                        "set {s} 不在拓扑内（共 {} 组）",
                        cl.sets.sets.len()
                    )));
                }
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    /// 槽位 → 本机盘下标（None = 不在本机）。**分片面语义：slot 为组内槽，经 `flat_of` 映射到
    /// cfg.drives 扁平下标**（洞 1 修法 (a)——组内序 ≠ 扁平序，多 set 直接透传会写错盘）。
    /// 单机：槽位即盘下标（v1 恒等/放置语义一致）。
    pub(in crate::store) fn local_drive_idx_of_slot(&self, set: u16, slot: u16) -> Option<usize> {
        match self.cfg.cluster.as_deref() {
            None => (slot as usize)
                .lt(&self.cfg.drives.len())
                .then_some(slot as usize),
            Some(cl) => {
                let topo = cl.sets.set_topo(set)?;
                let ord = topo.local_drive_of_slot(slot)?;
                cl.sets.flat_of(set, ord)
            }
        }
    }

    /// 分片放置（v1/v2 分派，见 [`ClusterTopology::place`] 与 [`Self::placement_hash`]）。
    /// 集群：在**桶所属 set** 的组内槽空间算（set 越界 → Err，fail-closed）。
    pub(in crate::store) fn slot_of_shard(&self, set: u16, hash: u64, shard: u8) -> Result<u16> {
        match self.cfg.cluster.as_deref() {
            None => {
                let n = self.cfg.drives.len();
                Ok(if self.placement_v2() {
                    ((hash.wrapping_add(shard as u64)) % n as u64) as u16
                } else {
                    drive_index(shard, n) as u16
                })
            }
            Some(cl) => {
                let topo = cl
                    .sets
                    .set_topo(set)
                    .ok_or_else(|| Error::Encode(format!("set {set} 不在拓扑内")))?;
                Ok(topo.place(hash, shard, cl.placement_v2))
            }
        }
    }

    /// 槽位 → 落点（slot = **桶所属 set 的组内槽**）。越界返回 None（meta 里的 drive 是外部输入，须防呆）。
    pub(in crate::store) fn slot_target(&self, set: u16, slot: u16) -> Option<ShardTarget<'_>> {
        match self.cfg.cluster.as_deref() {
            None => Some(ShardTarget::Local {
                drive: drive_root(&self.cfg.drives, slot as u8)?,
                slot,
            }),
            Some(cl) => {
                let topo = cl.sets.set_topo(set)?;
                if slot >= topo.slot_count() {
                    return None;
                }
                match topo.local_drive_of_slot(slot) {
                    Some(ord) => {
                        let flat = cl.sets.flat_of(set, ord)?;
                        Some(ShardTarget::Local {
                            drive: drive_root(&self.cfg.drives, flat as u8)?,
                            slot,
                        })
                    }
                    None => Some(ShardTarget::Remote {
                        node: &topo.nodes[topo.node_of_slot(slot)].id,
                        slot,
                    }),
                }
            }
        }
    }

    /// meta 的 ShardRef → 落点（读路径统一入口；drive = 桶所属 set 的组内槽）。
    pub(in crate::store) fn shard_target(&self, set: u16, s: &ShardRef) -> Option<ShardTarget<'_>> {
        self.slot_target(set, s.drive as u16)
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
        // 4×3 = 12 槽：v1 恒等（shard % 12 恰 1 片/槽）；v2 对任意 hash 也两两互异
        for i in 0..12u8 {
            assert_eq!(t.place(0, i, false), i as u16);
        }
        // v2 互异性（切片 5 放置不变量）：12 分片落 12 个不同槽位
        let mut slots: Vec<u16> = (0..12u8)
            .map(|i| t.place(0x1234_5678_9abc_def0, i, true))
            .collect();
        slots.sort_unstable();
        slots.dedup();
        assert_eq!(slots.len(), 12, "v2 放置必须两两互异");
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
        assert_eq!(t.place(0, 11, false), 11);
        assert!(
            (0..12u8).all(|i| t.place(0, i, false) < 12),
            "v1：12 个分片永远落不进槽 12..15"
        );
        assert!(
            (0..12u8).any(|i| t.place(0xabc, i, true) >= 12),
            "v2：分片必须能落进槽 12..15（扩容的意义）"
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
