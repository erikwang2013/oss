//! `mesh_gate` 单测（拆分文件以守住 <500 行规则）。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::mesh::{ListLocalResp, ListUploadsLocalResp, MetaAck, MetaSnapshot};

/// 只覆盖测试用到的面：health 返回可配版本/拓扑，数据面 RPC 计数。
#[derive(Debug, Default)]
struct FakeMesh {
    version: u32,
    topo_epoch: u64,
    topo_hash: String,
    /// true = health 探测失败（模拟不可达），数据面仍可被调用以复用既有错误语义。
    health_err: bool,
    health_calls: AtomicUsize,
    writes: AtomicUsize,
    reads: AtomicUsize,
    list_calls: AtomicUsize,
    uploads_listed: AtomicUsize,
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
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ListLocalResp {
            entries: vec![],
            prefixes: vec![],
            slots_ok: 0,
        })
    }
    async fn list_uploads_local(
        &self,
        _: &str,
        _: ListUploadsLocal,
    ) -> Result<ListUploadsLocalResp> {
        self.uploads_listed.fetch_add(1, Ordering::SeqCst);
        Ok(ListUploadsLocalResp {
            uploads: vec![],
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
            topo_epoch: self.topo_epoch,
            topo_hash: self.topo_hash.clone(),
            drives: vec![],
            maintenance: false,
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

fn list_local() -> ListLocal {
    ListLocal {
        bucket: "b".into(),
        prefix: String::new(),
        delimiter: None,
        marker: None,
        marker_exclusive: false,
        limit: 10,
        include_deleted: false,
    }
}

fn caps(wire: u32, epoch: u64, hash: &str) -> PeerCaps {
    PeerCaps {
        wire_version: wire,
        topo_epoch: epoch,
        topo_hash: hash.into(),
    }
}

fn local(epoch: u64, hash: &str) -> LocalCaps {
    LocalCaps {
        topo_epoch: epoch,
        topo_hash: hash.into(),
    }
}

/// 旧节点（含缺字段 = 0）一律按 1；未知/未来 wire fail-closed。
#[test]
fn peer_wire_version_normalizes_legacy_and_unknown_fails_closed() {
    let h = |v| NodeHealth {
        node_id: "n1".into(),
        version: "x".into(),
        slots: vec![],
        buckets: 0,
        data_wire_version: v,
        topo_epoch: 1,
        topo_hash: String::new(),
        drives: vec![],
        maintenance: false,
    };
    assert_eq!(h(0).peer_data_wire_version(), 1);
    assert_eq!(h(1).peer_data_wire_version(), 1);
    assert_eq!(h(2).peer_data_wire_version(), 2);
    assert_eq!(DATA_WIRE_VERSION, 2);
    assert_eq!(PeerCaps::from_health(&h(0)).wire_version, 1);

    let l = local(1, "");
    assert_eq!(
        check_peer("n1", &PeerCaps::from_health(&h(1)), &l).unwrap(),
        PeerClass::Legacy
    );
    assert_eq!(
        check_peer("n1", &PeerCaps::from_health(&h(2)), &l).unwrap(),
        PeerClass::Current
    );
    // check_peer 只看已归一值：0 未归一 = 未知，必须拒绝
    for bad in [0, 3] {
        let e = check_peer("n1", &caps(bad, 1, ""), &l).unwrap_err();
        assert!(matches!(e, Error::PeerIncompatible(_)), "{bad}");
        assert!(e.to_string().contains("peer_incompatible"));
    }
}

/// 放置 v2+/显式槽表（local epoch ≥ 2）要求 peer 宣告 wire2 + 相同 epoch + 相同非空指纹；
/// placement v1 下 epoch/hash 一旦宣告就必须一致。
#[test]
fn check_peer_rejects_placement_and_hash_mismatch() {
    let v1 = local(1, "aaaa");
    assert_eq!(
        check_peer("n", &caps(2, 1, "aaaa"), &v1).unwrap(),
        PeerClass::Current
    );
    // 旧节点未宣告拓扑：仍按 legacy 可服务
    assert_eq!(
        check_peer("n", &caps(1, 0, ""), &v1).unwrap(),
        PeerClass::Legacy
    );
    // 宣告了就必须一致
    assert!(check_peer("n", &caps(2, 1, "bbbb"), &v1).is_err());
    assert!(check_peer("n", &caps(2, 2, "aaaa"), &v1).is_err());

    let v2 = local(2, "aaaa");
    assert_eq!(
        check_peer("n", &caps(2, 2, "aaaa"), &v2).unwrap(),
        PeerClass::Current
    );
    assert!(check_peer("n", &caps(1, 2, "aaaa"), &v2).is_err());
    assert!(check_peer("n", &caps(2, 1, "aaaa"), &v2).is_err());
    assert!(check_peer("n", &caps(2, 0, ""), &v2).is_err());
    assert!(check_peer("n", &caps(2, 2, "bbbb"), &v2).is_err());
}

/// 混版窗口：base 数据面（分片/meta/LMU）允许旧 peer，逐字节转发。
#[tokio::test]
async fn mixed_version_allows_base_data_plane() {
    let (g, fake) = gate(1);
    g.write_shard("n2", shard_write()).await.unwrap();
    g.read_shard("n2", shard_read()).await.unwrap();
    g.list_uploads_local(
        "n2",
        ListUploadsLocal {
            bucket: "b".into(),
            prefix: String::new(),
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            limit: 10,
        },
    )
    .await
    .unwrap();
    assert_eq!(fake.writes.load(Ordering::SeqCst), 1);
    assert_eq!(fake.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fake.uploads_listed.load(Ordering::SeqCst), 1);
}

/// MPU 分片流对旧 peer 在发出任何 RPC 前拒绝；收尾/查询（ListParts/Complete/Abort）
/// 自 v1.16.0 起线格式未变 → 放行到旧 peer 执行（这里传输层未实现 → Unavailable）。
#[tokio::test]
async fn mpu_streaming_rejected_but_finish_allowed_on_mixed_version() {
    let (g, fake) = gate(1);
    assert!(matches!(
        g.mpu_upload_part("n2", mpu_upload()).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert_eq!(fake.mpu_parts.load(Ordering::SeqCst), 0);
    let r = MpuRef {
        bucket: "b".into(),
        key: "k".into(),
        upload_id: "n1.u".into(),
    };
    for e in [
        g.mpu_abort("n2", r.clone()).await.err().unwrap(),
        g.mpu_list_parts("n2", r).await.err().unwrap(),
        g.mpu_complete(
            "n2",
            MpuComplete {
                bucket: "b".into(),
                key: "k".into(),
                upload_id: "n1.u".into(),
                parts: vec![],
                mode: crate::store::VersioningMode::Off,
                encryption: None,
                lock: None,
            },
        )
        .await
        .err()
        .unwrap(),
    ] {
        assert!(matches!(e, Error::Unavailable(_)), "实得 {e:?}");
    }
}

/// 有界回退操作类：wire-1 peer 放行（调用方已预落盘/限长/核对 MD5），但**流式**
/// UploadPart 仍只发往现版；未知 wire（3）连有界回退也 fail-closed。
#[tokio::test]
async fn bounded_mpu_forward_allowed_but_streaming_still_gated() {
    let (g, fake) = gate(1);
    assert_eq!(
        g.mpu_upload_part_bounded("n2", mpu_upload()).await.unwrap(),
        "etag"
    );
    assert_eq!(fake.mpu_parts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        g.mpu_upload_part("n2", mpu_upload()).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert_eq!(fake.mpu_parts.load(Ordering::SeqCst), 1, "流式不得发出");

    let (g3, fake3) = gate(3);
    assert!(matches!(
        g3.mpu_upload_part_bounded("n2", mpu_upload()).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert_eq!(fake3.mpu_parts.load(Ordering::SeqCst), 0);
}

/// 带语义标志的 ListLocal 只发往现版 peer（旧版会忽略 → 静默缺项/重复边界键）。
#[tokio::test]
async fn list_local_semantic_flags_require_current_peer() {
    let (g, fake) = gate(1);
    g.list_local("n2", list_local()).await.unwrap();
    for flags in [(true, false), (false, true)] {
        let l = ListLocal {
            marker_exclusive: flags.0,
            include_deleted: flags.1,
            ..list_local()
        };
        assert!(matches!(
            g.list_local("n2", l).await,
            Err(Error::PeerIncompatible(_))
        ));
    }
    assert_eq!(
        fake.list_calls.load(Ordering::SeqCst),
        1,
        "标志不得发往旧 peer"
    );

    let (g2, fake2) = gate(2);
    for flags in [(true, false), (false, true)] {
        let l = ListLocal {
            marker_exclusive: flags.0,
            include_deleted: flags.1,
            ..list_local()
        };
        g2.list_local("n2", l).await.unwrap();
    }
    assert_eq!(fake2.list_calls.load(Ordering::SeqCst), 2);

    // 未知 wire：连普通页也 fail-closed（不得当成本版）
    let (g3, fake3) = gate(3);
    assert!(matches!(
        g3.list_local("n2", list_local()).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert_eq!(fake3.list_calls.load(Ordering::SeqCst), 0);
}

/// placement v2：未宣告 v2 指纹的 peer 一律拒绝，且不发出 RPC。
#[tokio::test]
async fn placement_v2_gate_rejects_unattested_peer() {
    let legacy = Arc::new(FakeMesh {
        version: 2,
        topo_epoch: 1,
        ..Default::default()
    });
    let g = GatedMesh::with_local(legacy.clone(), local(2, "aaaa"), Duration::ZERO);
    assert!(matches!(
        g.write_shard("n2", shard_write()).await,
        Err(Error::PeerIncompatible(_))
    ));
    assert_eq!(legacy.writes.load(Ordering::SeqCst), 0);

    let current = Arc::new(FakeMesh {
        version: 2,
        topo_epoch: 2,
        topo_hash: "aaaa".into(),
        ..Default::default()
    });
    let g = GatedMesh::with_local(current.clone(), local(2, "aaaa"), Duration::ZERO);
    g.write_shard("n2", shard_write()).await.unwrap();
    assert_eq!(current.writes.load(Ordering::SeqCst), 1);
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
