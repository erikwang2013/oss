//! DP-0/DP-1 数据面滚动升级：能力握手 + 混版操作矩阵闸门。
//!
//! 线格式窗口 `{1, 2}`（本版 = 2）：旧节点缺字段（proto3 缺省 0）按 1；未知/未来值
//! （>2）与拓扑指纹不匹配一律 fail-closed（未知语义不得当成本版或旧版放行）。
//!
//! 分层：
//! - [`NodeHealth`] 携带 `data_wire_version` + `topo_epoch/topo_hash`；
//! - [`PeerCapabilities`] = 进程内 TTL 缓存；
//! - [`GatedMesh`] = 装配层装饰器：按**操作类**判定——base 数据面（分片/meta/桶/
//!   删除/回收/LMU）允许旧版；MPU 转发与带语义标志的 ListLocal 要求现版。
//!
//! 读路径的降级/失败语义在 store 层（`degraded_marker` / `VersionList::require_complete`），
//! 本模块只决定「能不能发给这个 peer」。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::mesh::{
    ControlQuery, ControlRow, ControlWrite, ListLocal, ListLocalResp, ListUploadsLocal,
    ListUploadsLocalResp, LockReq, LockResp, MeshTransport, MetaAck, MetaRead, MetaSnapshot,
    MetaWrite, MpuComplete, MpuPartRow, MpuPartUpload, MpuRef, NodeHealth, ShardDelete, ShardRead,
    ShardWrite, VersionDirRef,
};
use crate::store::ObjectInfo;

/// 本版数据面线格式。任何「旧端会理解错」的数据面语义新增都必须递增此值。
/// 滚动升级期间**不得递增**：v1.17 只认 2，抬到 3 会让新旧两端互相拒绝（死锁）。
pub const DATA_WIRE_VERSION: u32 = 2;
/// 旧节点缺字段（proto3 缺省 0）的保守取值：绝不把「未知」当成本版。
pub const LEGACY_DATA_WIRE_VERSION: u32 = 1;

/// 生产 TonicMesh 的能力缓存 TTL：滚动重启后最多一个 TTL 的陈旧窗口。
pub const DEFAULT_TTL: Duration = Duration::from_secs(10);

/// 本机拓扑指纹（Wave 0 命名 `TopologyAttestation`；placement v2 与 shrink topo v4 共用）。
pub use crate::mesh::TopologyAttestation as LocalCaps;

/// 拓扑指纹 canonical string：`topo_epoch;data;parity;enabled;`，然后每个 set 的
/// `set_index;` + 该 set 节点按 id 升序的 `id,addr,drive_count;`；SHA-256 前 8 字节 hex。
pub fn slot_map_hash(
    sets: &crate::store::ClusterSets,
    ec: &crate::store::ec::EcConfig,
    topo_epoch: u64,
) -> String {
    crate::store::mesh_api::topo_fingerprint(sets, ec, topo_epoch)
}

/// 本机能力快照（与 [`crate::store::Store::local_caps`] 同源）。
///
/// 显式槽表（v3/v4+）必须使用 SlotMap epoch + 含槽位的 `topology_hash`，否则
/// GatedMesh 与实际 health/Store 指纹不一致，所有跨节点数据 RPC 会被误拒。
pub fn local_caps(
    sets: &crate::store::ClusterSets,
    ec: &crate::store::ec::EcConfig,
    topo_epoch: u64,
) -> LocalCaps {
    if sets.sets.iter().any(|t| t.slot_map().is_some()) {
        LocalCaps {
            topo_epoch: sets.topology_epoch(),
            topo_hash: sets.topology_hash(),
        }
    } else {
        LocalCaps {
            topo_epoch,
            topo_hash: slot_map_hash(sets, ec, topo_epoch),
        }
    }
}

impl NodeHealth {
    /// 对端数据面版本：缺失/0 的旧节点按 1。
    pub fn peer_data_wire_version(&self) -> u32 {
        if self.data_wire_version == 0 {
            LEGACY_DATA_WIRE_VERSION
        } else {
            self.data_wire_version
        }
    }
}

/// 对端能力快照。`wire_version` 已归一（0 → 1）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerCaps {
    pub wire_version: u32,
    pub topo_epoch: u64,
    pub topo_hash: String,
}

impl PeerCaps {
    pub fn from_health(h: &NodeHealth) -> Self {
        Self {
            wire_version: h.peer_data_wire_version(),
            topo_epoch: h.topo_epoch,
            topo_hash: h.topo_hash.clone(),
        }
    }
}

/// 混版分类：Legacy 只允许 base 数据面；Current 全操作可用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerClass {
    Legacy,
    Current,
}

/// 操作类：决定 legacy peer 是否放行。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpClass {
    /// 分片/meta/桶/删除/回收/LMU：旧版字段兼容（混版窗口允许）。
    Data,
    /// MPU 跨节点转发：上传暂存区/段语义跨版本不可靠 → 只允许现版。
    MpuForward,
}

/// 能力判定（唯一策略出处）：
/// - wire 窗口 `{1, 2}`，其余（含 0/3+）fail-closed；
/// - 放置 v2+/显式槽表（local epoch ≥ 2）要求 peer 宣告 wire 2 + 相同 epoch + 相同非空 hash；
/// - placement v1 下，peer 宣告了 epoch 就必须一致；双方都有 hash 就必须一致。
pub fn check_peer(node: &str, peer: &PeerCaps, local: &LocalCaps) -> Result<PeerClass> {
    if !(LEGACY_DATA_WIRE_VERSION..=DATA_WIRE_VERSION).contains(&peer.wire_version) {
        return Err(Error::PeerIncompatible(format!(
            "节点 {node} 数据面线格式 {} 不在支持窗口 {LEGACY_DATA_WIRE_VERSION}..={DATA_WIRE_VERSION}",
            peer.wire_version
        )));
    }
    if local.topo_epoch >= 2
        && (peer.wire_version < 2
            || peer.topo_epoch != local.topo_epoch
            || peer.topo_hash.is_empty()
            || peer.topo_hash != local.topo_hash)
    {
        return Err(Error::PeerIncompatible(format!(
            "placement v2+/shrink 要求节点 {node} 宣告 wire2/epoch{}/相同拓扑指纹（实得 wire={} epoch={} hash={}，本机 hash={}）",
            local.topo_epoch, peer.wire_version, peer.topo_epoch, peer.topo_hash, local.topo_hash
        )));
    }
    if peer.topo_epoch != 0 && peer.topo_epoch != local.topo_epoch {
        return Err(Error::PeerIncompatible(format!(
            "节点 {node} 拓扑 epoch {} 与本机 {} 不一致（混版窗口只支持 placement v1）",
            peer.topo_epoch, local.topo_epoch
        )));
    }
    if !local.topo_hash.is_empty()
        && !peer.topo_hash.is_empty()
        && peer.topo_hash != local.topo_hash
    {
        return Err(Error::PeerIncompatible(format!(
            "节点 {node} 拓扑指纹 {} 与本机 {} 不一致",
            peer.topo_hash, local.topo_hash
        )));
    }
    Ok(if peer.wire_version < 2 {
        PeerClass::Legacy
    } else {
        PeerClass::Current
    })
}

/// 对端能力缓存。只缓存探测成功的结果；探测失败不缓存——不可达仍走既有
/// 「降级/quorum」语义，不伪装成版本不兼容。
pub struct PeerCapabilities {
    ttl: Duration,
    cache: Mutex<HashMap<String, (PeerCaps, Instant)>>,
}

impl std::fmt::Debug for PeerCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerCapabilities")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl PeerCapabilities {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// 对端能力（缓存优先）。`inner` 必须是**非闸门**实现，health 才不会递归。
    pub async fn caps(&self, inner: &dyn MeshTransport, node: &str) -> Result<PeerCaps> {
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(node)
            .cloned();
        if let Some((caps, at)) = cached
            && at.elapsed() < self.ttl
        {
            return Ok(caps);
        }
        let caps = inner.peer_caps(node).await?;
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(node.to_string(), (caps.clone(), Instant::now()));
        Ok(caps)
    }

    /// 对端线格式（缓存口径，供 `data_wire_version` 使用）。
    pub async fn wire_version(&self, inner: &dyn MeshTransport, node: &str) -> Result<u32> {
        Ok(self.caps(inner, node).await?.wire_version)
    }
}

/// 数据面闸门装饰器（装配层包住生产 TonicMesh）。
///
/// 直通面：`health`（握手/状态探测自身不能被闸）、`lock` 与 `control_query/control_write`
/// ——控制面有自己的 Level B 兼容门控，数据面闸门不应连带阻断鉴权与锁。
pub struct GatedMesh {
    inner: Arc<dyn MeshTransport>,
    local: LocalCaps,
    caps: PeerCapabilities,
}

/// `GatedMesh::new/with_ttl` 的缺省本机能力：无拓扑宣告（单测/旧装配）。
fn default_local() -> LocalCaps {
    LocalCaps {
        topo_epoch: 1,
        topo_hash: String::new(),
    }
}

impl GatedMesh {
    pub fn new(inner: Arc<dyn MeshTransport>) -> Self {
        Self::with_local(inner, default_local(), DEFAULT_TTL)
    }

    /// 测试夹具用 `Duration::ZERO`：每次判定都探测，版本切换立即可见。
    pub fn with_ttl(inner: Arc<dyn MeshTransport>, ttl: Duration) -> Self {
        Self::with_local(inner, default_local(), ttl)
    }

    /// 生产装配：本机能力参与策略（placement v2 必须由 `local_caps()` 构造）。
    pub fn with_local(inner: Arc<dyn MeshTransport>, local: LocalCaps, ttl: Duration) -> Self {
        Self {
            inner,
            local,
            caps: PeerCapabilities::new(ttl),
        }
    }

    /// 逐 peer 判定。探测失败（不可达）放行给实际 RPC——对端宕机必须保持既有错误映射
    /// （MPU → Unavailable、分片 → 缺失/quorum、锁 → 计数），不能把网络故障伪装成混版。
    async fn gate(&self, node: &str, op: OpClass) -> Result<()> {
        let caps = match self.caps.caps(&*self.inner, node).await {
            Ok(c) => c,
            Err(_) => return Ok(()),
        };
        let class = check_peer(node, &caps, &self.local)?;
        if op == OpClass::MpuForward && class == PeerClass::Legacy {
            return Err(Error::PeerIncompatible(format!(
                "混版窗口不跨版本转发 MPU：节点 {node} 数据面线格式 {} < {DATA_WIRE_VERSION}",
                caps.wire_version
            )));
        }
        Ok(())
    }
}

impl std::fmt::Debug for GatedMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatedMesh")
            .field("inner", &self.inner)
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl MeshTransport for GatedMesh {
    async fn write_shard(&self, node: &str, w: ShardWrite) -> Result<()> {
        self.gate(node, OpClass::Data).await?;
        self.inner.write_shard(node, w).await
    }
    async fn read_shard(&self, node: &str, r: ShardRead) -> Result<Option<Bytes>> {
        self.gate(node, OpClass::Data).await?;
        self.inner.read_shard(node, r).await
    }
    async fn delete_version(&self, node: &str, d: VersionDirRef) -> Result<()> {
        self.gate(node, OpClass::Data).await?;
        self.inner.delete_version(node, d).await
    }
    async fn recycle_version(&self, node: &str, d: VersionDirRef) -> Result<()> {
        self.gate(node, OpClass::Data).await?;
        self.inner.recycle_version(node, d).await
    }
    async fn delete_shard(&self, node: &str, d: ShardDelete) -> Result<()> {
        self.gate(node, OpClass::Data).await?;
        self.inner.delete_shard(node, d).await
    }
    async fn prepare_meta(&self, node: &str, m: MetaWrite) -> Result<MetaAck> {
        self.gate(node, OpClass::Data).await?;
        self.inner.prepare_meta(node, m).await
    }
    async fn commit_meta(&self, node: &str, m: MetaWrite) -> Result<MetaAck> {
        self.gate(node, OpClass::Data).await?;
        self.inner.commit_meta(node, m).await
    }
    async fn abort_meta(&self, node: &str, m: MetaWrite) -> Result<()> {
        self.gate(node, OpClass::Data).await?;
        self.inner.abort_meta(node, m).await
    }
    async fn read_meta(&self, node: &str, r: MetaRead) -> Result<MetaSnapshot> {
        self.gate(node, OpClass::Data).await?;
        self.inner.read_meta(node, r).await
    }
    async fn delete_meta(&self, node: &str, r: MetaRead) -> Result<()> {
        self.gate(node, OpClass::Data).await?;
        self.inner.delete_meta(node, r).await
    }
    async fn ensure_bucket(&self, node: &str, bucket: &str) -> Result<()> {
        self.gate(node, OpClass::Data).await?;
        self.inner.ensure_bucket(node, bucket).await
    }
    async fn delete_bucket_dir(&self, node: &str, bucket: &str) -> Result<()> {
        self.gate(node, OpClass::Data).await?;
        self.inner.delete_bucket_dir(node, bucket).await
    }
    async fn list_bucket_dirs(&self, node: &str) -> Result<Vec<String>> {
        self.gate(node, OpClass::Data).await?;
        self.inner.list_bucket_dirs(node).await
    }
    async fn list_local(&self, node: &str, l: ListLocal) -> Result<ListLocalResp> {
        // 旧版忽略 marker_exclusive/include_deleted（会让并集静默缺项/重复边界键），
        // 带这两个语义标志的请求必须现版对端。探测失败仍按不可达放行给实际 RPC。
        if let Ok(caps) = self.caps.caps(&*self.inner, node).await
            && check_peer(node, &caps, &self.local)? == PeerClass::Legacy
            && (l.marker_exclusive || l.include_deleted)
        {
            return Err(Error::PeerIncompatible(format!(
                "节点 {node} 为旧数据面，不支持 ListLocal 语义标志（marker_exclusive={} include_deleted={}）",
                l.marker_exclusive, l.include_deleted
            )));
        }
        self.inner.list_local(node, l).await
    }
    async fn list_uploads_local(
        &self,
        node: &str,
        l: ListUploadsLocal,
    ) -> Result<ListUploadsLocalResp> {
        self.gate(node, OpClass::Data).await?;
        self.inner.list_uploads_local(node, l).await
    }
    async fn mpu_upload_part(&self, node: &str, req: MpuPartUpload) -> Result<String> {
        self.gate(node, OpClass::MpuForward).await?;
        self.inner.mpu_upload_part(node, req).await
    }
    async fn mpu_complete(&self, node: &str, req: MpuComplete) -> Result<ObjectInfo> {
        self.gate(node, OpClass::MpuForward).await?;
        self.inner.mpu_complete(node, req).await
    }
    async fn mpu_abort(&self, node: &str, req: MpuRef) -> Result<()> {
        self.gate(node, OpClass::MpuForward).await?;
        self.inner.mpu_abort(node, req).await
    }
    async fn mpu_list_parts(&self, node: &str, req: MpuRef) -> Result<Vec<MpuPartRow>> {
        self.gate(node, OpClass::MpuForward).await?;
        self.inner.mpu_list_parts(node, req).await
    }

    // ---------- 直通面（非数据面） ----------

    async fn health(&self, node: &str) -> Result<NodeHealth> {
        self.inner.health(node).await
    }

    async fn data_wire_version(&self, node: &str) -> Result<u32> {
        self.caps.wire_version(&*self.inner, node).await
    }

    async fn peer_caps(&self, node: &str) -> Result<PeerCaps> {
        self.caps.caps(&*self.inner, node).await
    }

    async fn lock(&self, node: &str, req: LockReq) -> Result<LockResp> {
        self.inner.lock(node, req).await
    }

    async fn control_query(&self, node: &str, q: ControlQuery) -> Result<Option<ControlRow>> {
        self.inner.control_query(node, q).await
    }

    async fn control_write(&self, node: &str, w: ControlWrite) -> Result<()> {
        self.inner.control_write(node, w).await
    }
}

#[cfg(test)]
#[path = "mesh_gate_tests.rs"]
mod tests;
