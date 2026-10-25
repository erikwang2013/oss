//! M3 shrink T4：`cluster-shrink plan` 的只读校验与报告。
//!
//! 铁律：`ShardRef.drive` 是槽号，shrink 只改「槽位 → (node, drive)」解析。
//! 因此 plan 的第一道防线是**逐 set 槽数必须与旧拓扑相等**（槽数变了 `place()` 会
//! 静默改所有对象放置，meta 仍存旧槽号 → 跨对象错片）。parity/容量/MPU/epoch/hash
//! 任一不符即拒绝；不提供 `--force`，plan 不写任何数据或拓扑文件（只落报告）。

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::error::{Error, Result};
use crate::layout;
use crate::mesh::{ListUploadsLocal, NodeHealth};

use super::Store;
use super::drives::{ClusterSets, ClusterTopology};

/// 槽位迁移（同一槽号，新旧物理归属不同）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotMove {
    pub set: u16,
    pub slot: u16,
    pub from_node: String,
    pub from_drive: u16,
    pub to_node: String,
    pub to_drive: u16,
}

/// 机器可读计划（apply/verify/commit/rollback 的收据）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShrinkPlan {
    pub old_epoch: u64,
    pub new_epoch: u64,
    /// 旧拓扑的本地 canonical 指纹（`ClusterSets::topology_hash`，含槽位/物理序）。
    /// apply/verify 用它与本地旧拓扑比较；**不要**用它与 peer health 的 wire hash 比较。
    pub old_hash: String,
    /// 旧拓扑的 NodeHealth wire 指纹（`TopologyAttestation.topo_hash`），apply 阶段
    /// 用于校验 peer health 一致；positional 与显式槽表使用不同渲染，故单独存放。
    pub old_wire_hash: String,
    /// 新拓扑的本地 canonical 指纹（`ClusterSets::topology_hash`）。
    pub new_hash: String,
    /// 新拓扑逐 set 槽数（必须与旧拓扑逐 set 相等）。
    pub slot_counts: Vec<u16>,
    pub moves: Vec<SlotMove>,
    pub retired_nodes: Vec<String>,
    /// 全部键（含 delete marker；meta 镜像完整性口径）。
    pub objects: u64,
    pub versions: u64,
    /// sha256(sorted "bucket/key") 全 hex。
    pub namespace_digest: String,
}

/// 单个受影响槽位的统计。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShrinkSlotStat {
    pub set: u16,
    pub slot: u16,
    pub objects: u64,
    pub versions: u64,
    pub shards: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShrinkPlanReport {
    pub plan: ShrinkPlan,
    /// 只含被迁移槽位（未迁移槽位不出现）。
    pub slots: Vec<ShrinkSlotStat>,
    pub warnings: Vec<String>,
    pub elapsed_ms: u64,
}

/// 跨进程 shrink 互斥（`.oss.sys/shrink/active.lock`，flock；崩溃即自动释放）。
/// 同一检查也用于 fsck/heal/scrub/rebalance 入口（与 spec 5.6 的互斥要求对齐）。
pub fn acquire_shrink_lock(root: &Path) -> Result<std::fs::File> {
    let dir = layout::sys_dir(root, "shrink");
    std::fs::create_dir_all(&dir)?;
    let f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(dir.join("active.lock"))?;
    rustix::fs::flock(&f, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(|_| {
        Error::Encode("已有 shrink 在进行（.oss.sys/shrink/active.lock 被占用）".into())
    })?;
    Ok(f)
}

/// 只探测（拿到锁即释放）：有别的进程在 shrink 就返回 true。
pub fn shrink_lock_busy(root: &Path) -> bool {
    acquire_shrink_lock(root).is_err()
}

/// 对端拓扑指纹一致性：全节点 maintenance 且 epoch/hash 与本地一致（混版 fail-closed）。
pub fn validate_peer_health(peers: &[NodeHealth], epoch: u64, hash: &str) -> Result<()> {
    if peers.is_empty() {
        return Err(Error::Encode("拓扑为空：无法校验对端健康".into()));
    }
    for h in peers {
        if !h.maintenance {
            return Err(Error::Encode(format!(
                "节点 {} 未处于 maintenance（shrink 必须全节点停写）",
                h.node_id
            )));
        }
        if h.topo_epoch != epoch {
            return Err(Error::Encode(format!(
                "节点 {} 拓扑 epoch {} 与本地 {epoch} 不一致（混 epoch，拒绝 plan）",
                h.node_id, h.topo_epoch
            )));
        }
        if h.topo_hash != hash {
            return Err(Error::Encode(format!(
                "节点 {} 拓扑 hash «{}» 与本地 «{hash}» 不一致（混拓扑，拒绝 plan）",
                h.node_id, h.topo_hash
            )));
        }
    }
    Ok(())
}

/// 目标盘容量校验：`need` = (set, node, drive, 字节)。逐目标汇总后与 peer health
/// 上报的 free_bytes 比较；缺该盘信息 / 不足一律拒绝（fail-closed）。
pub fn validate_capacity(need: &[(u16, String, u16, u64)], peers: &[NodeHealth]) -> Result<()> {
    let mut sums: BTreeMap<(String, u16, u16), u64> = BTreeMap::new();
    for (set, node, drive, bytes) in need {
        let e = sums.entry((node.clone(), *set, *drive)).or_default();
        *e = e.saturating_add(*bytes);
    }
    for ((node, set, drive), bytes) in sums {
        let h = peers
            .iter()
            .find(|h| h.node_id == node)
            .ok_or_else(|| Error::Encode(format!("节点 {node} 不可达/未上报健康：无法确认容量")))?;
        let d = h
            .drives
            .iter()
            .find(|d| d.set == set && d.drive == drive)
            .ok_or_else(|| {
                Error::Encode(format!(
                    "节点 {node} 未上报 set {set} 盘 {drive} 的可用空间（无法确认容量）"
                ))
            })?;
        if d.free_bytes < bytes {
            return Err(Error::Encode(format!(
                "节点 {node} set {set} 盘 {drive} 可用 {} 字节 < 需复制 {bytes} 字节（不得 2× 覆盖旧片）",
                d.free_bytes
            )));
        }
    }
    Ok(())
}

impl Store {
    /// 只读生成 shrink 计划（不写数据/拓扑；仅持 active.lock）。任一校验失败即 `Err`。
    pub async fn shrink_plan(&self, new: &ClusterSets) -> Result<ShrinkPlanReport> {
        let started = Instant::now();
        let _lock = acquire_shrink_lock(self.root())?;
        let cl = self.cfg.cluster.as_deref().ok_or_else(|| {
            Error::Encode(
                "单机→多节点仅支持重新灌入（见 README 集群模式）；shrink 需要 [cluster] 拓扑"
                    .into(),
            )
        })?;
        if cl.stretch {
            return Err(Error::Encode("stretch 2×6 不支持 shrink".into()));
        }
        if !self.cfg.maintenance {
            return Err(Error::Encode(
                "shrink plan 要求本节点 maintenance=true（先停写；health 也会校验对端）".into(),
            ));
        }
        let caps = self.local_caps();
        let old = &cl.sets;
        let ec = &self.cfg.ec;

        // 目标必须是显式槽表（v3+）；positional 目标没有 epoch，无法建立 v4 指纹。
        if new.sets.iter().any(|t| t.slot_map().is_none()) {
            return Err(Error::Encode(
                "目标拓扑必须配置显式槽表（[cluster].epoch + [[cluster.slots]]）".into(),
            ));
        }
        let new_epoch = new.topology_epoch();
        if new_epoch != caps.topo_epoch + 1 {
            return Err(Error::Encode(format!(
                "新拓扑 epoch {new_epoch} 必须 = 旧 epoch {} + 1",
                caps.topo_epoch
            )));
        }
        if new.sets.len() != old.sets.len() {
            return Err(Error::Encode(format!(
                "set 数变化 {} → {}（shrink 只改变槽位归属）",
                old.sets.len(),
                new.sets.len()
            )));
        }

        let mut slot_counts = Vec::with_capacity(old.sets.len());
        let mut retired: BTreeSet<String> = BTreeSet::new();
        let mut moves = Vec::new();
        for (si, (o, n)) in old.sets.iter().zip(&new.sets).enumerate() {
            if o.slot_count() != n.slot_count() {
                return Err(Error::Encode(format!(
                    "set {si} 槽数变化 {} → {}（槽数变化会让 place() 静默错片；拒绝）",
                    o.slot_count(),
                    n.slot_count()
                )));
            }
            slot_counts.push(n.slot_count());
            validate_nodes(o, n, si)?;
            for on in &o.nodes {
                if !n.nodes.iter().any(|nn| nn.id == on.id) {
                    retired.insert(on.id.clone());
                }
            }
            // 每节点每 set 槽数 ≤ parity（单节点豁免；构造器已查，此处防御性复核）。
            if n.nodes.len() > 1 {
                for (i, nn) in n.nodes.iter().enumerate() {
                    let c = n.slot_count_of_node(i);
                    if c > ec.parity as u16 {
                        return Err(Error::Encode(format!(
                            "节点 «{}» 承载 {c} 槽超过 parity {}（丢节点即不可重建）",
                            nn.id, ec.parity
                        )));
                    }
                }
            }
            for slot in 0..o.slot_count() {
                let from = o.slot_target(slot).ok_or_else(|| {
                    Error::Encode(format!("旧拓扑 set {si} 槽 {slot} 越界（内部不一致）"))
                })?;
                let to = n.slot_target(slot).ok_or_else(|| {
                    Error::Encode(format!("新拓扑 set {si} 槽 {slot} 越界（内部不一致）"))
                })?;
                if from != to {
                    moves.push(SlotMove {
                        set: si as u16,
                        slot,
                        from_node: from.node,
                        from_drive: from.drive,
                        to_node: to.node,
                        to_drive: to.drive,
                    });
                }
            }
        }

        // 全节点可达 + maintenance + 指纹一致（退役节点也要可达：MPU 归属检查）。
        let peers = self.collect_healths(cl).await?;
        validate_peer_health(&peers, caps.topo_epoch, &caps.topo_hash)?;

        // 只读扫描命名空间；受影响槽位逐槽统计 + 目标盘容量需求。
        let (keys, buckets) = self.collect_namespace().await?;
        let moved: HashSet<(u16, u16)> = moves.iter().map(|m| (m.set, m.slot)).collect();
        let mut stats: BTreeMap<(u16, u16), ShrinkSlotStat> = moved
            .iter()
            .map(|&(set, slot)| {
                (
                    (set, slot),
                    ShrinkSlotStat {
                        set,
                        slot,
                        objects: 0,
                        versions: 0,
                        shards: 0,
                        bytes: 0,
                    },
                )
            })
            .collect();
        let mut need: Vec<(u16, String, u16, u64)> = Vec::new();
        let mut digest = sha2::Sha256::new();
        let mut objects = 0u64;
        let mut versions = 0u64;
        let mut cur_bucket: Option<String> = None;
        let mut cur_set: u16 = 0;
        for (bucket, key) in &keys {
            digest.update(format!("{bucket}/{key}\n").as_bytes());
            if cur_bucket.as_deref() != Some(bucket.as_str()) {
                cur_set = self
                    .set_of_bucket(bucket)
                    .await
                    .map_err(|e| Error::Encode(format!("桶 {bucket} 的 set 解析失败: {e}")))?;
                cur_bucket = Some(bucket.clone());
            }
            let Some(m) = self.read_meta(bucket, key).await? else {
                continue;
            };
            objects += 1;
            versions += m.versions.len() as u64;
            let mut object_slots: BTreeSet<(u16, u16)> = BTreeSet::new();
            for v in &m.versions {
                let Some(l) = &v.data_layout else { continue };
                let mut per_slot: BTreeMap<u16, (u64, u64)> = BTreeMap::new(); // slot → (shards, bytes)
                for s in &l.shards {
                    let slot = s.drive as u16;
                    if !moved.contains(&(cur_set, slot)) {
                        continue;
                    }
                    let to = new
                        .sets
                        .get(cur_set as usize)
                        .and_then(|t| t.slot_target(slot))
                        .ok_or_else(|| {
                            Error::Encode(format!(
                                "新拓扑 set {cur_set} 槽 {slot} 无法解析目标（内部不一致）"
                            ))
                        })?;
                    need.push((cur_set, to.node, to.drive, s.size));
                    let e = per_slot.entry(slot).or_default();
                    e.0 += 1;
                    e.1 = e.1.saturating_add(s.size);
                    object_slots.insert((cur_set, slot));
                }
                for (slot, (shards, bytes)) in per_slot {
                    let st = stats
                        .get_mut(&(cur_set, slot))
                        .expect("moved 槽位必在统计表内");
                    st.versions += 1;
                    st.shards += shards;
                    st.bytes = st.bytes.saturating_add(bytes);
                }
            }
            for k in object_slots {
                stats.get_mut(&k).expect("moved 槽位必在统计表内").objects += 1;
            }
        }
        validate_capacity(&need, &peers)?;

        // 退役节点不得有未完成 MPU（Complete 的目标归属会在 commit 后不可路由）。
        for node in &retired {
            for bucket in &buckets {
                check_retired_mpu(cl, node, bucket).await?;
            }
        }

        let mut warnings = Vec::new();
        let min_slots = slot_counts.iter().copied().min().unwrap_or(0);
        let max_per_node = new
            .sets
            .iter()
            .flat_map(|t| (0..t.nodes.len()).map(move |i| (t, i)))
            .map(|(t, i)| t.slot_count_of_node(i))
            .max()
            .unwrap_or(0);
        if min_slots.saturating_sub(max_per_node) < ec.data as u16 + 1 {
            warnings.push(format!(
                "写余量不足：最少 set 槽数 {min_slots} − 单节点最多 {max_per_node} 槽 = {} < data+1={}（停一节点后写入无余量）",
                min_slots.saturating_sub(max_per_node),
                ec.data as u16 + 1
            ));
        }
        // 目标盘 2× 余量（旧片保留到 commit 后）：不足仅告警，硬门槛是 need ≤ free。
        let mut cap_sums: BTreeMap<(String, u16, u16), u64> = BTreeMap::new();
        for (set, node, drive, bytes) in &need {
            let e = cap_sums.entry((node.clone(), *set, *drive)).or_default();
            *e = e.saturating_add(*bytes);
        }
        for ((node, set, drive), bytes) in &cap_sums {
            if let Some(free) = peers
                .iter()
                .find(|h| &h.node_id == node)
                .and_then(|h| h.drives.iter().find(|d| d.set == *set && d.drive == *drive))
                .map(|d| d.free_bytes)
                && free < bytes.saturating_mul(2)
            {
                warnings.push(format!(
                    "2× 余量不足：节点 {node} set {set} 盘 {drive} 可用 {free} < 复制量 {bytes} 的两倍"
                ));
            }
        }

        let mut slots: Vec<ShrinkSlotStat> = stats.into_values().collect();
        slots.sort_by_key(|s| (s.set, s.slot));
        let report = ShrinkPlanReport {
            plan: ShrinkPlan {
                old_epoch: caps.topo_epoch,
                new_epoch,
                old_hash: old.topology_hash(),
                old_wire_hash: caps.topo_hash,
                new_hash: new.topology_hash(),
                slot_counts,
                moves,
                retired_nodes: retired.into_iter().collect(),
                objects,
                versions,
                namespace_digest: hex::encode(digest.finalize()),
            },
            slots,
            warnings,
            elapsed_ms: started.elapsed().as_millis() as u64,
        };
        Ok(report)
    }

    /// 旧拓扑全部去重节点的健康（本机走本地实现；退役节点也查，失败即 Err）。
    async fn collect_healths(&self, cl: &super::drives::ClusterConfig) -> Result<Vec<NodeHealth>> {
        let me = self.mesh_health().await;
        let mut out = Vec::with_capacity(cl.sets.flat.nodes.len());
        for n in &cl.sets.flat.nodes {
            let h = if n.id == me.node_id {
                me.clone()
            } else {
                cl.transport.health(&n.id).await.map_err(|e| {
                    Error::Encode(format!(
                        "节点 {} 不可达（shrink 要求全节点在线）: {e}",
                        n.id
                    ))
                })?
            };
            out.push(h);
        }
        Ok(out)
    }
}

/// 保留节点：必须存在于旧拓扑、drive_count 不变、相对顺序不变（flat 编号稳定）。
fn validate_nodes(old: &ClusterTopology, new: &ClusterTopology, set: usize) -> Result<()> {
    for nn in &new.nodes {
        let on = old.nodes.iter().find(|o| o.id == nn.id).ok_or_else(|| {
            Error::Encode(format!(
                "set {set} 新拓扑出现旧拓扑没有的节点 «{}»（shrink 不增节点）",
                nn.id
            ))
        })?;
        if nn.drive_count != on.drive_count {
            return Err(Error::Encode(format!(
                "节点 «{}» 盘数变化 {} → {}（shrink 不增删盘）",
                nn.id, on.drive_count, nn.drive_count
            )));
        }
    }
    let expected: Vec<&str> = old
        .nodes
        .iter()
        .filter(|o| new.nodes.iter().any(|n| n.id == o.id))
        .map(|o| o.id.as_str())
        .collect();
    let actual: Vec<&str> = new.nodes.iter().map(|n| n.id.as_str()).collect();
    if expected != actual {
        return Err(Error::Encode(format!(
            "set {set} 保留节点顺序变化（{expected:?} → {actual:?}）；flat 编号必须稳定"
        )));
    }
    Ok(())
}

/// 退役节点逐桶查未完成 MPU；任一 upload 即拒绝（含 key/upload_id）。
async fn check_retired_mpu(
    cl: &super::drives::ClusterConfig,
    node: &str,
    bucket: &str,
) -> Result<()> {
    let resp = cl
        .transport
        .list_uploads_local(
            node,
            ListUploadsLocal {
                bucket: bucket.to_string(),
                prefix: String::new(),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                limit: 1,
            },
        )
        .await
        .map_err(|e| {
            Error::Encode(format!(
                "无法检查退役节点 {node} 的未完成 MPU（桶 {bucket}）: {e}"
            ))
        })?;
    if let Some(u) = resp.uploads.first() {
        return Err(Error::Encode(format!(
            "退役节点 {node} 存在未完成 MPU：{bucket}/{} upload_id={}（先 abort/complete 再 plan）",
            u.key, u.upload_id
        )));
    }
    Ok(())
}
