//! `mesh_gate` 单测（拆分文件以守住 <500 行规则）。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::mesh::{ListLocalResp, MetaAck, MetaSnapshot};

/// 只覆盖测试用到的面：health 返回可配版本，数据面 RPC 计数。
#[derive(Debug, Default)]
struct FakeMesh {
    version: u32,
    /// true = health 探测失败（模拟不可达），数据面仍可被调用以复用既有错误语义。
    health_err: bool,
    health_calls: AtomicUsize,
    writes: AtomicUsize,
    reads: AtomicUsize,
    mpu_parts: AtomicUsize,
}

#[async_trait::async_trait]
impl MeshTransport for FakeMesh {
    async fn write_shard(&self, _: &str, _: ShardWrite) -> Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        if self.health_err {
            return Err(Error::Unavailable("模拟不可达".into()));
        }
        Ok(())
    }
    async fn read_shard(&self, _: &str, _: ShardRead) -> Result<Option<Bytes>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
    async fn delete_version(&self, _: &str, _: VersionDirRef) -> Result<()> {
        Ok(())
    }
    async fn recycle_version(&self, _: &str, _: VersionDirRef) -> Result<()> {
        Ok(())
    }
    async fn delete_shard(&self, _: &str, _: ShardDelete) -> Result<()> {
        Ok(())
    }
    async fn prepare_meta(&self, _: &str, _: MetaWrite) -> Result<MetaAck> {
        Ok(MetaAck {
            ok: true,
            msg: String::new(),
            slots: vec![],
            slots_ok: 0,
        })
    }
    async fn commit_meta(&self, _: &str, _: MetaWrite) -> Result<MetaAck> {
        Ok(MetaAck {
            ok: true,
            msg: String::new(),
            slots: vec![],
            slots_ok: 0,
        })
    }
    async fn abort_meta(&self, _: &str, _: MetaWrite) -> Result<()> {
        Ok(())
    }
    async fn read_meta(&self, _: &str, _: MetaRead) -> Result<MetaSnapshot> {
        Ok(MetaSnapshot {
            meta: None,
            mod_time_nanos: 0,
            slots_ok: 0,
        })
    }
    async fn delete_meta(&self, _: &str, _: MetaRead) -> Result<()> {
        Ok(())
    }
    async fn ensure_bucket(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    async fn delete_bucket_dir(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    async fn list_bucket_dirs(&self, _: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn list_local(&self, _: &str, _: ListLocal) -> Result<ListLocalResp> {
        Ok(ListLocalResp {
            entries: vec![],
            prefixes: vec![],
            slots_ok: 0,
        })
    }
    async fn health(&self, node: &str) -> Result<NodeHealth> {
        self.health_calls.fetch_add(1, Ordering::SeqCst);
        if self.health_err {
            return Err(Error::Unavailable("模拟不可达".into()));
        }
        Ok(NodeHealth {
            node_id: node.to_string(),
            version: "test".into(),
            slots: vec![],
            buckets: 0,
            data_wire_version: self.version,
            topo_epoch: 1,
            topo_hash: String::new(),
        })
    }
    async fn lock(&self, _: &str, _: LockReq) -> Result<LockResp> {
        Ok(LockResp { granted: true })
    }
    async fn control_query(&self, _: &str, _: ControlQuery) -> Result<Option<ControlRow>> {
        Ok(None)
    }
    async fn mpu_upload_part(&self, _: &str, _: MpuPartUpload) -> Result<String> {
        self.mpu_parts.fetch_add(1, Ordering::SeqCst);
        Ok("etag".into())
    }
}

fn gate(version: u32) -> (GatedMesh, Arc<FakeMesh>) {
    let fake = Arc::new(FakeMesh {
        version,
        ..Default::default()
    });
    (GatedMesh::with_ttl(fake.clone(), Duration::ZERO), fake)
}

fn shard_write() -> ShardWrite {
    ShardWrite {
        bucket: "b".into(),
        key: "k".into(),
        uuid: "u".into(),
        part: 1,
        shard: 0,
        crc32c: 0,
        data: Bytes::new(),
        chunked: false,
        offset: 0,
        chunk_crc32c: 0,
        is_final: true,
        slot: 0,
        set: 0,
    }
}

fn shard_read() -> ShardRead {
    ShardRead {
        bucket: "b".into(),
        key: "k".into(),
        uuid: "u".into(),
        part: 1,
        shard: 0,
        offset: 0,
        length: 0,
        slot: 0,
        set: 0,
    }
}

fn mpu_upload() -> MpuPartUpload {
    MpuPartUpload {
        bucket: "b".into(),
        key: "k".into(),
        upload_id: "n1.u".into(),
        part_number: 1,
        sse_c_md5: None,
        body: Box::pin(futures::stream::empty()),
    }
}

/// 旧节点（含缺字段 = 0）一律按 1；本版 = 2。
#[test]
fn legacy_or_missing_version_is_one() {
    let h = |v| NodeHealth {
        node_id: "n1".into(),
        version: "x".into(),
        slots: vec![],
        buckets: 0,
        data_wire_version: v,
        topo_epoch: 1,
        topo_hash: String::new(),
    };
    assert_eq!(h(0).peer_data_wire_version(), 1);
    assert_eq!(h(1).peer_data_wire_version(), 1);
    assert_eq!(h(2).peer_data_wire_version(), 2);
    assert_eq!(DATA_WIRE_VERSION, 2);
    assert!(check_compatible("n1", 2).is_ok());
    for bad in [0, 1, 3] {
        let e = check_compatible("n1", bad).unwrap_err();
        assert!(matches!(e, Error::PeerIncompatible(_)), "{bad}");
        assert!(e.to_string().contains("peer_incompatible"));
    }
}

/// 混版写/读：在发出任何数据面 RPC 前拒绝。
#[tokio::test]
async fn mixed_version_rejects_shard_write_and_read() {
    let (g, fake) = gate(1);
    assert!(matches!(
        g.write_shard("n2", shard_write()).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert!(matches!(
        g.read_shard("n2", shard_read()).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert_eq!(fake.writes.load(Ordering::SeqCst), 0);
    assert_eq!(fake.reads.load(Ordering::SeqCst), 0);
}

/// 同版本：行为逐字节保留（转发到内层）。
#[tokio::test]
async fn same_version_forwards_data_plane() {
    let (g, fake) = gate(2);
    g.write_shard("n2", shard_write()).await.unwrap();
    g.read_shard("n2", shard_read()).await.unwrap();
    assert_eq!(g.mpu_upload_part("n2", mpu_upload()).await.unwrap(), "etag");
    assert_eq!(fake.writes.load(Ordering::SeqCst), 1);
    assert_eq!(fake.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fake.mpu_parts.load(Ordering::SeqCst), 1);
}

/// MPU 转发（UploadPart/Abort/ListParts）混版拒绝。
#[tokio::test]
async fn mpu_forwarding_rejected_on_mixed_version() {
    let (g, fake) = gate(1);
    assert!(matches!(
        g.mpu_upload_part("n2", mpu_upload()).await,
        Err(Error::PeerIncompatible(_))
    ));
    let r = MpuRef {
        bucket: "b".into(),
        key: "k".into(),
        upload_id: "n1.u".into(),
    };
    assert!(matches!(
        g.mpu_abort("n2", r.clone()).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert!(matches!(
        g.mpu_list_parts("n2", r).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert_eq!(fake.mpu_parts.load(Ordering::SeqCst), 0);
}

/// health/控制面/锁不被数据面闸门阻断（否则混版下连诊断与鉴权都不可用）。
#[tokio::test]
async fn health_and_control_plane_stay_reachable() {
    let (g, _) = gate(1);
    assert_eq!(g.health("n2").await.unwrap().peer_data_wire_version(), 1);
    assert_eq!(g.data_wire_version("n2").await.unwrap(), 1);
    let lock = LockReq {
        bucket: "b".into(),
        key: "k".into(),
        epoch_nanos: 0,
        epoch_node: "n1".into(),
        holder: "h".into(),
        ttl_ms: 1000,
        op: crate::mesh::LockOp::Acquire,
    };
    assert!(g.lock("n2", lock).await.unwrap().granted);
    assert!(
        g.control_query("n2", ControlQuery::BucketSet("b".into()))
            .await
            .unwrap()
            .is_none()
    );
}

/// 不可达 ≠ 混版：health 探测失败时放行实际 RPC（保住 MPU→Unavailable、分片→缺失/
/// quorum 等既有错误语义），且版本探测对外如实报错（状态面判「不可达」）。
#[tokio::test]
async fn unreachable_peer_is_not_incompatible() {
    let fake = Arc::new(FakeMesh {
        version: 2,
        health_err: true,
        ..Default::default()
    });
    let g = GatedMesh::with_ttl(fake.clone(), Duration::ZERO);
    let e = g.write_shard("n2", shard_write()).await.unwrap_err();
    assert!(!matches!(e, Error::PeerIncompatible(_)), "{e:?}");
    // 实际 RPC 的错误原样上抛（这里模拟传输层 Unavailable）
    assert!(matches!(e, Error::Unavailable(_)), "{e:?}");
    assert_eq!(fake.writes.load(Ordering::SeqCst), 1, "必须尝试实际 RPC");
    assert!(g.data_wire_version("n2").await.is_err());
}

/// 能力结果按 TTL 缓存（生产热路径不逐 RPC 探测）；TTL=0 = 每次探测（测试口径）。
#[tokio::test]
async fn capabilities_are_cached_within_ttl() {
    let fake = Arc::new(FakeMesh {
        version: 2,
        ..Default::default()
    });
    let cached = GatedMesh::with_ttl(fake.clone(), Duration::from_secs(3600));
    assert_eq!(cached.data_wire_version("n2").await.unwrap(), 2);
    assert_eq!(cached.data_wire_version("n2").await.unwrap(), 2);
    assert_eq!(fake.health_calls.load(Ordering::SeqCst), 1);
    let uncached = GatedMesh::with_ttl(fake.clone(), Duration::ZERO);
    assert_eq!(uncached.data_wire_version("n2").await.unwrap(), 2);
    assert_eq!(uncached.data_wire_version("n2").await.unwrap(), 2);
    assert_eq!(fake.health_calls.load(Ordering::SeqCst), 3);
}
