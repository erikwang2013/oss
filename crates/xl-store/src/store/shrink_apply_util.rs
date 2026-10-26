//! shrink T5–T7 内部件（计划对账/分片扫描/重建/落盘/收据）；公开命令面在 `shrink_apply.rs`。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::error::{Error, Result};
use crate::layout;
use crate::mesh::ShardRead;
use crate::meta::{DataLayout, ShardRef, Version};

use super::Store;
use super::drives::{ClusterConfig, ClusterSets, NodeSpec, ShardTarget, SlotMap, SlotTarget};
use super::ec;
use super::shrink::{PLAN_SCHEMA, ShrinkPlan, SlotMove, validate_peer_health};

/// 计划校验后的上下文。
pub(in crate::store) struct PlanCtx {
    pub(in crate::store) self_id: String,
    /// 本机为目标槽位的迁移（set, slot）。
    pub(in crate::store) local_moves: BTreeMap<(u16, u16), SlotMove>,
}

#[derive(Clone)]
pub(in crate::store) struct MovedShard {
    pub(in crate::store) bucket: String,
    pub(in crate::store) key: String,
    pub(in crate::store) set: u16,
    pub(in crate::store) slot: u16,
    pub(in crate::store) uuid: String,
    layout: DataLayout,
    shard_idx: usize,
    pub(in crate::store) shard: ShardRef,
}

pub(in crate::store) struct MovedData {
    pub(in crate::store) shards: Vec<MovedShard>,
    /// 含本机迁移槽位分片的版本（verify 全读用）。
    pub(in crate::store) versions: Vec<(String, String, Version)>,
}

/// apply/verify 的收据（commit 只认本机同时有 apply + verify 收据）。
#[derive(Serialize, Deserialize)]
pub(in crate::store) struct Receipt {
    schema: u32,
    action: String,
    node_id: String,
    old_epoch: u64,
    new_epoch: u64,
    old_hash: String,
    new_hash: String,
    files: u64,
    bytes: u64,
    reconstructed: u64,
}

impl Receipt {
    pub(in crate::store) fn new(
        action: &str,
        node: &str,
        plan: &ShrinkPlan,
        files: u64,
        bytes: u64,
        reconstructed: u64,
    ) -> Self {
        Self {
            schema: PLAN_SCHEMA,
            action: action.into(),
            node_id: node.into(),
            old_epoch: plan.old_epoch,
            new_epoch: plan.new_epoch,
            old_hash: plan.old_hash.clone(),
            new_hash: plan.new_hash.clone(),
            files,
            bytes,
            reconstructed,
        }
    }
}

impl Store {
    pub(in crate::store) fn cluster_ctx(&self) -> Result<&std::sync::Arc<ClusterConfig>> {
        self.cfg.cluster.as_ref().ok_or_else(|| {
            Error::Encode("shrink 需要 [cluster] 拓扑（单机→多节点须重新灌入）".into())
        })
    }

    /// 计划与本地旧拓扑逐项对账，并由 moves 重建新拓扑（epoch/hash 必须与计划一致）。
    pub(in crate::store) fn plan_ctx(&self, plan: &ShrinkPlan) -> Result<PlanCtx> {
        if plan.schema != PLAN_SCHEMA {
            return Err(Error::Encode(format!(
                "plan schema {} 不受支持（本版只认 {PLAN_SCHEMA}）",
                plan.schema
            )));
        }
        let cl = self.cluster_ctx()?;
        if cl.stretch {
            return Err(Error::Encode("stretch 2×6 不支持 shrink".into()));
        }
        if !self.cfg.maintenance {
            return Err(Error::Encode(
                "shrink 要求本节点 maintenance=true（先停写）".into(),
            ));
        }
        let caps = self.local_caps();
        if caps.topo_epoch != plan.old_epoch || caps.topo_hash != plan.old_wire_hash {
            return Err(Error::Encode(format!(
                "本地拓扑 epoch/hash 与 plan 不一致：本地 {}/{}，plan {}/{}（重新生成 plan？）",
                caps.topo_epoch, caps.topo_hash, plan.old_epoch, plan.old_wire_hash
            )));
        }
        if cl.sets.topology_hash() != plan.old_hash {
            return Err(Error::Encode(format!(
                "本地 canonical 拓扑指纹 «{}» ≠ plan.old_hash «{}»",
                cl.sets.topology_hash(),
                plan.old_hash
            )));
        }
        if plan.new_epoch != plan.old_epoch + 1 {
            return Err(Error::Encode(format!(
                "plan 新 epoch {} 必须 = 旧 epoch {} + 1",
                plan.new_epoch, plan.old_epoch
            )));
        }
        let old_counts: Vec<u16> = cl.sets.sets.iter().map(|t| t.slot_count()).collect();
        if plan.slot_counts != old_counts {
            return Err(Error::Encode(format!(
                "plan 槽数 {:?} ≠ 本地 {:?}（槽数变化会让 place() 静默错片）",
                plan.slot_counts, old_counts
            )));
        }
        let retired: BTreeSet<&str> = plan.retired_nodes.iter().map(String::as_str).collect();
        for r in &retired {
            if !cl
                .sets
                .sets
                .iter()
                .any(|t| t.nodes.iter().any(|n| n.id == *r))
            {
                return Err(Error::Encode(format!("plan 退役节点 «{r}» 不在旧拓扑内")));
            }
        }
        let self_id = cl.sets.flat.self_node().id.clone();
        let mut new_nodes: Vec<Vec<NodeSpec>> = Vec::with_capacity(cl.sets.sets.len());
        for t in &cl.sets.sets {
            let nodes: Vec<NodeSpec> = t
                .nodes
                .iter()
                .filter(|n| !retired.contains(n.id.as_str()))
                .cloned()
                .collect();
            if nodes.is_empty() {
                return Err(Error::Encode("plan 把某个 set 的节点全部退役".into()));
            }
            new_nodes.push(nodes);
        }
        if !new_nodes.iter().flatten().any(|n| n.id == self_id) {
            return Err(Error::Encode(format!(
                "本节点 «{self_id}» 在计划中退役：apply/verify/commit/rollback 只由保留节点执行"
            )));
        }
        let mut by_slot: BTreeMap<(u16, u16), &SlotMove> = BTreeMap::new();
        for m in &plan.moves {
            let t = cl
                .sets
                .sets
                .get(m.set as usize)
                .ok_or_else(|| Error::Encode(format!("move set {} 越界", m.set)))?;
            let old = t
                .slot_target(m.slot)
                .ok_or_else(|| Error::Encode(format!("move set {} slot {} 越界", m.set, m.slot)))?;
            if old.node != m.from_node || old.drive != m.from_drive {
                return Err(Error::Encode(format!(
                    "move set {} slot {} 源 {}/{} 与旧拓扑 {}/{} 不符",
                    m.set, m.slot, m.from_node, m.from_drive, old.node, old.drive
                )));
            }
            let tn = new_nodes[m.set as usize]
                .iter()
                .find(|n| n.id == m.to_node)
                .ok_or_else(|| {
                    Error::Encode(format!(
                        "move set {} slot {} 目标节点 «{}» 不在保留节点内",
                        m.set, m.slot, m.to_node
                    ))
                })?;
            if m.to_drive >= tn.drive_count {
                return Err(Error::Encode(format!(
                    "move set {} slot {} 目标盘 {} 越界（节点 «{}» 只有 {} 盘）",
                    m.set, m.slot, m.to_drive, m.to_node, tn.drive_count
                )));
            }
            if m.from_node == m.to_node && m.from_drive == m.to_drive {
                return Err(Error::Encode(format!(
                    "move set {} slot {} 是同盘空迁移",
                    m.set, m.slot
                )));
            }
            if by_slot.insert((m.set, m.slot), m).is_some() {
                return Err(Error::Encode(format!(
                    "move set {} slot {} 重复",
                    m.set, m.slot
                )));
            }
        }
        let mut maps = Vec::with_capacity(cl.sets.sets.len());
        for (si, t) in cl.sets.sets.iter().enumerate() {
            let mut slots = Vec::with_capacity(t.slot_count() as usize);
            for slot in 0..t.slot_count() {
                match by_slot.get(&(si as u16, slot)) {
                    Some(m) => slots.push(SlotTarget {
                        node: m.to_node.clone(),
                        drive: m.to_drive,
                    }),
                    None => slots.push(t.slot_target(slot).ok_or_else(|| {
                        Error::Encode(format!("旧拓扑 set {si} slot {slot} 越界"))
                    })?),
                }
            }
            maps.push(Some(SlotMap {
                epoch: plan.new_epoch,
                slots,
            }));
        }
        // 指纹与节点身份无关：任取一个保留节点作为 self 占位。
        let anchor = new_nodes[0][0].id.clone();
        let new_sets = ClusterSets::with_slot_maps(&anchor, new_nodes, maps, &self.cfg.ec)?;
        if new_sets.topology_epoch() != plan.new_epoch || new_sets.topology_hash() != plan.new_hash
        {
            return Err(Error::Encode(format!(
                "由 plan.moves 重建的新拓扑 {}/«{}» ≠ plan.new {}/«{}»（计划不完整/被改动）",
                new_sets.topology_epoch(),
                new_sets.topology_hash(),
                plan.new_epoch,
                plan.new_hash
            )));
        }
        let local_moves = plan
            .moves
            .iter()
            .filter(|m| m.to_node == self_id)
            .map(|m| ((m.set, m.slot), m.clone()))
            .collect();
        Ok(PlanCtx {
            self_id,
            local_moves,
        })
    }

    /// 全节点 maintenance + 旧 epoch/wire hash 一致（混版/混拓扑 fail-closed）。
    pub(in crate::store) async fn validate_live(&self, plan: &ShrinkPlan) -> Result<()> {
        let cl = self.cluster_ctx()?;
        let peers = self.collect_healths(cl).await?;
        validate_peer_health(&peers, plan.old_epoch, &plan.old_wire_hash)
    }

    pub(in crate::store) async fn namespace_digest(&self) -> Result<String> {
        let (keys, _) = self.collect_namespace().await?;
        let mut h = sha2::Sha256::new();
        for (bucket, key) in &keys {
            h.update(format!("{bucket}/{key}\n").as_bytes());
        }
        Ok(hex::encode(h.finalize()))
    }

    /// 扫描本机目标槽位的全部分片（meta 只读，不改）。
    pub(in crate::store) async fn collect_moved(
        &self,
        moves: &BTreeMap<(u16, u16), SlotMove>,
    ) -> Result<MovedData> {
        let (keys, _) = self.collect_namespace().await?;
        let mut sets: BTreeMap<String, u16> = BTreeMap::new();
        let mut out = MovedData {
            shards: Vec::new(),
            versions: Vec::new(),
        };
        for (bucket, key) in &keys {
            let Some(m) = self.read_meta(bucket, key).await? else {
                continue;
            };
            let set = match sets.get(bucket) {
                Some(s) => *s,
                None => {
                    let s = self.set_of_bucket(bucket).await?;
                    sets.insert(bucket.clone(), s);
                    s
                }
            };
            if !moves.keys().any(|(s, _)| *s == set) {
                continue;
            }
            for v in &m.versions {
                let Some(l) = &v.data_layout else { continue };
                let mut hit = false;
                for (i, s) in l.shards.iter().enumerate() {
                    let slot = s.drive as u16;
                    if !moves.contains_key(&(set, slot)) {
                        continue;
                    }
                    hit = true;
                    out.shards.push(MovedShard {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        set,
                        slot,
                        uuid: v.part.uuid.clone(),
                        layout: l.clone(),
                        shard_idx: i,
                        shard: s.clone(),
                    });
                }
                if hit {
                    out.versions.push((bucket.clone(), key.clone(), v.clone()));
                }
            }
        }
        Ok(out)
    }

    pub(in crate::store) fn target_drive(
        &self,
        cl: &ClusterConfig,
        set: u16,
        drive: u16,
    ) -> Result<&Path> {
        let flat = cl.sets.flat_of(set, drive as usize).ok_or_else(|| {
            Error::Encode(format!(
                "本机 set {set} 盘 {drive} 无扁平映射（内部不一致）"
            ))
        })?;
        self.cfg
            .drives
            .get(flat)
            .map(std::path::PathBuf::as_path)
            .ok_or_else(|| Error::Encode(format!("扁平盘下标 {flat} 越界")))
    }

    /// 整片读（本机直读 / 远端 read_shard）；缺失/不可达 → None（交重建）。
    pub(in crate::store) async fn read_shard(
        &self,
        bucket: &str,
        key: &str,
        uuid: &str,
        s: &ShardRef,
        target: &ShardTarget<'_>,
        set: u16,
    ) -> Option<Vec<u8>> {
        match target {
            ShardTarget::Local { drive, .. } => {
                let p = super::drives::shard_path(drive, bucket, key, uuid, s).ok()?;
                tokio::fs::read(p).await.ok()
            }
            ShardTarget::Remote { node, .. } => {
                let t = self.mesh_transport()?;
                let r = ShardRead {
                    bucket: bucket.into(),
                    key: key.into(),
                    uuid: uuid.into(),
                    part: s.part as u32,
                    shard: s.shard as u32,
                    offset: 0,
                    length: 0,
                    slot: s.drive as u32,
                    set: set as u32,
                };
                t.read_shard(node, r).await.ok()?.map(|b| b.to_vec())
            }
        }
    }

    /// 源片缺失时从同版本其它片 EC 重建本片（含 parity 片；分块写产物按列独立，整片重建等价）。
    pub(in crate::store) async fn reconstruct_shard(&self, ms: &MovedShard) -> Result<Vec<u8>> {
        let total = (ms.layout.data + ms.layout.parity) as usize;
        let mut got: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
        for s in &ms.layout.shards {
            let target = self.slot_target(ms.set, s.drive as u16).ok_or_else(|| {
                Error::Encode(format!("重建：set {} slot {} 越界", ms.set, s.drive))
            })?;
            let b = self
                .read_shard(&ms.bucket, &ms.key, &ms.uuid, s, &target, ms.set)
                .await
                .filter(|b| ec::crc32c_of(b) == s.crc32c);
            got.push(b);
        }
        let cfg = ec::EcConfig {
            data: ms.layout.data,
            parity: ms.layout.parity,
            enabled: true,
        };
        let all = ec::reconstruct_shards(&got, cfg)?;
        all.get(ms.shard_idx)
            .cloned()
            .ok_or_else(|| Error::Encode("EC 重建结果缺目标片（内部不一致）".into()))
    }

    pub(in crate::store) async fn write_target(
        &self,
        drive: &Path,
        ms: &MovedShard,
        bytes: &[u8],
    ) -> Result<()> {
        let dir = layout::object_dir(drive, &ms.bucket, &ms.key)?.join(&ms.uuid);
        tokio::fs::create_dir_all(&dir).await?;
        let name = format!("part.{}.{}", ms.shard.part, ms.shard.shard);
        let tmp = dir.join(format!("{name}.new")); // .new = fsck/clean 已知的半截件形态
        {
            use tokio::io::AsyncWriteExt as _;
            let mut f = tokio::fs::File::create(&tmp).await?;
            f.write_all(bytes).await?;
            f.flush().await?;
            if self.cfg.fsync {
                f.sync_all().await?;
            }
        }
        tokio::fs::rename(&tmp, dir.join(&name)).await?;
        if self.cfg.fsync {
            Store::sync_dir(&dir).await;
        }
        Ok(())
    }

    /// 盘上是否存在本次 commit 的新 v4 记录（rollback 的 commit 后闸门）。
    pub(in crate::store) fn local_any_new_topo(
        &self,
        sets: &ClusterSets,
        plan: &ShrinkPlan,
    ) -> Result<bool> {
        for (di, _, _) in super::local_topo_entries(sets, self.cfg.drives.len())? {
            let p = layout::sys_dir(&self.cfg.drives[di], "topo");
            match std::fs::read_to_string(&p) {
                Ok(s) => {
                    if let Some(r) = super::parse_topo_record_v4(s.trim())
                        && r.epoch == plan.new_epoch
                        && r.hash == plan.new_hash
                    {
                        return Ok(true);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(false)
    }
}

pub(in crate::store) fn work_dir(root: &Path, plan: &ShrinkPlan) -> PathBuf {
    layout::sys_dir(root, "shrink").join(format!("{}-{}", plan.old_hash, plan.new_hash))
}

pub(in crate::store) fn write_receipt(path: &Path, r: &Receipt) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| Error::Encode("收据路径无父目录".into()))?;
    std::fs::create_dir_all(dir)?;
    let tmp = path.with_extension("receipt.tmp");
    let body =
        rmp_serde::to_vec_named(r).map_err(|e| Error::Encode(format!("收据序列化失败: {e}")))?;
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)?;
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

pub(in crate::store) fn read_receipt(path: &Path) -> Result<Receipt> {
    let b = std::fs::read(path).map_err(|e| {
        Error::Encode(format!(
            "缺少 {}（先在本机执行 cluster-shrink apply 与 verify）: {e}",
            path.display()
        ))
    })?;
    rmp_serde::from_slice(&b)
        .map_err(|e| Error::Encode(format!("收据 {} 解析失败: {e}", path.display())))
}

pub(in crate::store) fn check_receipt(
    r: &Receipt,
    action: &str,
    plan: &ShrinkPlan,
    node: &str,
) -> Result<()> {
    if r.schema != PLAN_SCHEMA
        || r.action != action
        || r.node_id != node
        || r.old_epoch != plan.old_epoch
        || r.new_epoch != plan.new_epoch
        || r.old_hash != plan.old_hash
        || r.new_hash != plan.new_hash
    {
        return Err(Error::Encode(format!(
            "{action} 收据与当前 plan/节点不一致（收据被改动或来自其它计划）"
        )));
    }
    Ok(())
}
