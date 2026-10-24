//! DP-0 数据面滚动升级：能力握手 + 混版 fail-closed 闸门。
//!
//! 本切片只保证「同版本数据面放行、混版数据面明确拒绝」。**完整滚动升级仍被
//! 能力特定读路径（DP-1 操作矩阵，如 List marker/版本列表语义）与 mesh TLS 热轮换
//! （DP-2）阻塞**——本模块不是「可滚动升级」的完成声明，只是把混版从「静默错读/
//! 部分写」变成可诊断的 503（`peer_incompatible`）。
//!
//! 分层：
//! - [`NodeHealth`] 携带 `data_wire_version`（proto3 缺省 0 = 旧节点 → 按 1）；
//! - [`PeerCapabilities`] = 进程内 TTL 缓存（生产 TonicMesh 的 health 探测很轻）；
//! - [`GatedMesh`] = 装配层装饰器：数据面 RPC 发出前逐 peer 判定，控制面/健康/锁直通。

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
pub const DATA_WIRE_VERSION: u32 = 2;
/// 旧节点缺字段（proto3 缺省 0）的保守取值：绝不把「未知」当成本版。
pub const LEGACY_DATA_WIRE_VERSION: u32 = 1;

/// 生产 TonicMesh 的能力缓存 TTL：滚动重启后最多一个 TTL 的陈旧窗口。
const DEFAULT_TTL: Duration = Duration::from_secs(10);

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

/// 混版判定：只放行与本机完全一致的版本（未来版本也不放行——没有逐能力协商时，
/// 「比本机新」同样是未知语义）。错误固定为 [`Error::PeerIncompatible`]（S3 503）。
pub fn check_compatible(node: &str, peer_version: u32) -> Result<()> {
    if peer_version == DATA_WIRE_VERSION {
        return Ok(());
    }
    Err(Error::PeerIncompatible(format!(
        "节点 {node} 数据面版本 {peer_version} 与本机 {DATA_WIRE_VERSION} 不一致\
         （混版集群数据面 fail-closed，拒绝读写）"
    )))
}

/// 对端能力缓存。只缓存探测成功的结果；探测失败不缓存——不可达仍走既有
/// 「降级/quorum」语义，不伪装成版本不兼容。
pub struct PeerCapabilities {
    ttl: Duration,
    cache: Mutex<HashMap<String, (u32, Instant)>>,
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

    /// 对端版本（缓存优先）。`inner` 必须是**非闸门**实现，health 才不会递归。
    pub async fn version(&self, inner: &dyn MeshTransport, node: &str) -> Result<u32> {
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(node)
            .copied();
        if let Some((v, at)) = cached
            && at.elapsed() < self.ttl
        {
            return Ok(v);
        }
        let v = inner.health(node).await?.peer_data_wire_version();
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(node.to_string(), (v, Instant::now()));
        Ok(v)
    }

    /// 数据面前置判定：**可达且不兼容**才拒；探测失败（不可达）放行给实际 RPC——
    /// 对端宕机必须保持既有错误映射（MPU → Unavailable、分片 → 缺失/quorum、锁 → 计数），
    /// 不能把网络故障伪装成混版。
    pub async fn require(&self, inner: &dyn MeshTransport, node: &str) -> Result<()> {
        match self.version(inner, node).await {
            Ok(v) => check_compatible(node, v),
            Err(_) => Ok(()),
        }
    }
}

/// 数据面闸门装饰器（装配层包住生产 TonicMesh）。
///
/// 直通面：`health`（握手/状态探测自身不能被闸）、`lock` 与 `control_query/control_write`
/// ——控制面有自己的 Level B 兼容门控，数据面闸门不应连带阻断鉴权与锁。
pub struct GatedMesh {
    inner: Arc<dyn MeshTransport>,
    caps: PeerCapabilities,
}

impl GatedMesh {
    pub fn new(inner: Arc<dyn MeshTransport>) -> Self {
        Self::with_ttl(inner, DEFAULT_TTL)
    }

    /// 测试夹具用 `Duration::ZERO`：每次判定都探测，版本切换立即可见。
    pub fn with_ttl(inner: Arc<dyn MeshTransport>, ttl: Duration) -> Self {
        Self {
            inner,
            caps: PeerCapabilities::new(ttl),
        }
    }

    async fn gate(&self, node: &str) -> Result<()> {
        self.caps.require(&*self.inner, node).await
    }
}

impl std::fmt::Debug for GatedMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatedMesh")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl MeshTransport for GatedMesh {
    async fn write_shard(&self, node: &str, w: ShardWrite) -> Result<()> {
        self.gate(node).await?;
        self.inner.write_shard(node, w).await
    }
    async fn read_shard(&self, node: &str, r: ShardRead) -> Result<Option<Bytes>> {
        self.gate(node).await?;
        self.inner.read_shard(node, r).await
    }
    async fn delete_version(&self, node: &str, d: VersionDirRef) -> Result<()> {
        self.gate(node).await?;
        self.inner.delete_version(node, d).await
    }
    async fn recycle_version(&self, node: &str, d: VersionDirRef) -> Result<()> {
        self.gate(node).await?;
        self.inner.recycle_version(node, d).await
    }
    async fn delete_shard(&self, node: &str, d: ShardDelete) -> Result<()> {
        self.gate(node).await?;
        self.inner.delete_shard(node, d).await
    }
    async fn prepare_meta(&self, node: &str, m: MetaWrite) -> Result<MetaAck> {
        self.gate(node).await?;
        self.inner.prepare_meta(node, m).await
    }
    async fn commit_meta(&self, node: &str, m: MetaWrite) -> Result<MetaAck> {
        self.gate(node).await?;
        self.inner.commit_meta(node, m).await
    }
    async fn abort_meta(&self, node: &str, m: MetaWrite) -> Result<()> {
        self.gate(node).await?;
        self.inner.abort_meta(node, m).await
    }
    async fn read_meta(&self, node: &str, r: MetaRead) -> Result<MetaSnapshot> {
        self.gate(node).await?;
        self.inner.read_meta(node, r).await
    }
    async fn delete_meta(&self, node: &str, r: MetaRead) -> Result<()> {
        self.gate(node).await?;
        self.inner.delete_meta(node, r).await
    }
    async fn ensure_bucket(&self, node: &str, bucket: &str) -> Result<()> {
        self.gate(node).await?;
        self.inner.ensure_bucket(node, bucket).await
    }
    async fn delete_bucket_dir(&self, node: &str, bucket: &str) -> Result<()> {
        self.gate(node).await?;
        self.inner.delete_bucket_dir(node, bucket).await
    }
    async fn list_bucket_dirs(&self, node: &str) -> Result<Vec<String>> {
        self.gate(node).await?;
        self.inner.list_bucket_dirs(node).await
    }
    async fn list_local(&self, node: &str, l: ListLocal) -> Result<ListLocalResp> {
        self.gate(node).await?;
        self.inner.list_local(node, l).await
    }
    async fn list_uploads_local(
        &self,
        node: &str,
        l: ListUploadsLocal,
    ) -> Result<ListUploadsLocalResp> {
        self.gate(node).await?;
        self.inner.list_uploads_local(node, l).await
    }
    async fn mpu_upload_part(&self, node: &str, req: MpuPartUpload) -> Result<String> {
        self.gate(node).await?;
        self.inner.mpu_upload_part(node, req).await
    }
    async fn mpu_complete(&self, node: &str, req: MpuComplete) -> Result<ObjectInfo> {
        self.gate(node).await?;
        self.inner.mpu_complete(node, req).await
    }
    async fn mpu_abort(&self, node: &str, req: MpuRef) -> Result<()> {
        self.gate(node).await?;
        self.inner.mpu_abort(node, req).await
    }
    async fn mpu_list_parts(&self, node: &str, req: MpuRef) -> Result<Vec<MpuPartRow>> {
        self.gate(node).await?;
        self.inner.mpu_list_parts(node, req).await
    }

    // ---------- 直通面（非数据面） ----------

    async fn health(&self, node: &str) -> Result<NodeHealth> {
        self.inner.health(node).await
    }

    async fn data_wire_version(&self, node: &str) -> Result<u32> {
        self.caps.version(&*self.inner, node).await
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
