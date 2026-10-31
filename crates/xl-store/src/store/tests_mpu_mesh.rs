//! M3 MPU 会话粘性（2 节点 in-process mesh）：属主发现 / 转发 / 失败封口 /
//! 本地兼容。传输层用 `tests::Cluster` 的 InProcessMesh（真网络流式路径见 xl-client
//! 的 tonic 往返测试）；此处验证 Store 层语义，不重跑网络。

use super::*;

fn stream_of(chunks: &[&'static [u8]]) -> ByteStream {
    let parts: Vec<Bytes> = chunks.iter().map(|b| Bytes::from_static(b)).collect();
    Box::pin(futures::stream::iter(parts.into_iter().map(Ok)))
}

async fn two_node() -> super::tests::Cluster {
    let c = super::tests::Cluster::new(2, 3).await;
    c.stores[0].create_bucket("alpha").await.unwrap();
    c
}

fn owner_of(id: &str) -> Option<String> {
    super::mpu_route::split_upload_id(id).unwrap().0
}

/// Create 在 n1；n2 收 UploadPart/ListParts/Complete 全部转发到属主，
/// 数据落在 n1 暂存区，提交后 n1 可读。同 parts 的重复 complete 幂等，迟到 abort
/// → NoSuchUpload。
#[tokio::test]
async fn mpu_mesh_forward_upload_list_complete() {
    let c = two_node().await;
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let (owner, uuid) = super::mpu_route::split_upload_id(&id).unwrap();
    assert_eq!(owner.as_deref(), Some("n1"));
    assert!(id.contains('.'), "集群 upload_id 必须带属主前缀：{id}");
    let n1_dir = layout::object_dir(&c.dirs[0][0], "alpha", "k")
        .unwrap()
        .join(&uuid);
    assert!(
        tokio::fs::try_exists(n1_dir.join("mpu.meta"))
            .await
            .unwrap()
    );
    let n2_dir = layout::object_dir(&c.dirs[1][0], "alpha", "k")
        .unwrap()
        .join(&uuid);
    assert!(
        !tokio::fs::try_exists(&n2_dir).await.unwrap(),
        "暂存区不得出现在非属主节点"
    );

    // n2 收 part → 属主 n1 落盘（body 分多块，仍按流处理）
    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"hello ", b"mesh"]))
        .await
        .unwrap();

    // 两节点读 ListParts 都走属主，结果一致
    for s in [&c.stores[0], &c.stores[1]] {
        let parts = s.list_parts("alpha", "k", &id).await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].size, 10);
        assert_eq!(parts[0].etag, etag);
    }

    // n2 收 Complete → 属主提交；n1 读得到完整内容
    let info = c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag.clone())])
        .await
        .unwrap();
    assert_eq!(info.size, 10);
    assert!(info.etag.ends_with("-1"));
    let (got, body) = c.stores[0].get_object("alpha", "k").await.unwrap();
    assert_eq!(body, Bytes::from_static(b"hello mesh"));
    assert_eq!(got.size, 10);
    assert!(
        !tokio::fs::try_exists(&n1_dir).await.unwrap(),
        "提交后暂存区应清理"
    );

    // 二次提交（网络丢响应的重试）：同一 parts → 幂等回查返回原结果，不重复提交；
    // 换了 parts 的迟到 complete 与迟到 abort → NoSuchUpload
    let again = c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag.clone())])
        .await
        .unwrap();
    assert_eq!(again.etag, info.etag);
    assert_eq!(again.version_id, info.version_id);
    assert_eq!(again.part_sizes, info.part_sizes);
    assert!(matches!(
        c.stores[1]
            .complete_multipart_upload("alpha", "k", &id, &[(1, "00".repeat(16))])
            .await,
        Err(Error::NoSuchUpload(_))
    ));
    assert!(matches!(
        c.stores[1].abort_multipart_upload("alpha", "k", &id).await,
        Err(Error::NoSuchUpload(_))
    ));
}

/// DP-1 跨版本：属主是旧数据面（缺字段 → 1）时分片流式转发按策略拒绝，但 store
/// 自动改走有界回退（预落盘 + MD5 核对）→ 分片可落地；ListParts/Complete/Abort 的
/// RPC 与暂存格式自 v1.16.0 未变 → 放行到旧属主执行，语义错误原样穿透。
/// 恢复同版后流式转发恢复（有界回退不再使用）。
#[tokio::test]
async fn mpu_mesh_mixed_version_bounded_upload_and_finish() {
    let c = two_node().await;
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    c.mesh.set_data_wire_version("n1", Some(0)); // 旧节点：health 缺字段

    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"x"]))
        .await
        .unwrap();
    assert_eq!(c.mesh.mpu_bounded_calls(), 1, "wire-1 必须走有界回退");
    assert_eq!(c.mesh.mpu_stream_calls(), 0);
    // 收尾类可达：分片已在属主暂存区
    let parts = c.stores[1].list_parts("alpha", "k", &id).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].etag, etag);
    // complete 的业务校验错误（ETag 不符）穿透旧属主，不再是版本拒绝
    let err = c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, "deadbeef".into())])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidPart(_)), "{err:?}");

    // 恢复同版 → 流式转发优先恢复（覆盖同号 part）
    c.mesh.set_data_wire_version("n1", None);
    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"ok"]))
        .await
        .unwrap();
    assert_eq!(c.mesh.mpu_stream_calls(), 1);
    assert_eq!(c.mesh.mpu_bounded_calls(), 1);
    c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
        .await
        .unwrap();

    // 旧属主上的 abort 也可达
    let id2 = c.stores[0]
        .create_multipart_upload("alpha", "k2", None, Default::default())
        .await
        .unwrap();
    c.mesh.set_data_wire_version("n1", Some(0));
    c.stores[1]
        .abort_multipart_upload("alpha", "k2", &id2)
        .await
        .unwrap();
    assert!(matches!(
        c.stores[1].list_parts("alpha", "k2", &id2).await,
        Err(Error::NoSuchUpload(_))
    ));
}

/// 属主宕机：四个转发操作全部确定性 Unavailable（S3 503），不得降级成本地
/// 成功/NoSuchUpload；恢复后可继续用宕机前已上传的 part 完成。
#[tokio::test]
async fn mpu_mesh_owner_down_is_unavailable() {
    let c = two_node().await;
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"before-down"]))
        .await
        .unwrap();

    c.mesh.down("n1");
    let upload = c.stores[1]
        .upload_part("alpha", "k", &id, 2, stream_of(&[b"x"]))
        .await
        .unwrap_err();
    assert!(matches!(upload, Error::Unavailable(_)), "实得 {upload:?}");
    assert!(matches!(
        c.stores[1].list_parts("alpha", "k", &id).await,
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        c.stores[1]
            .complete_multipart_upload("alpha", "k", &id, &[(1, etag.clone())])
            .await,
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        c.stores[1].abort_multipart_upload("alpha", "k", &id).await,
        Err(Error::Unavailable(_))
    ));
    // 失败期间非属主节点不得留下 part.2 残影
    let (_, uuid) = super::mpu_route::split_upload_id(&id).unwrap();
    let n2_dir = layout::object_dir(&c.dirs[1][0], "alpha", "k")
        .unwrap()
        .join(&uuid);
    assert!(!tokio::fs::try_exists(n2_dir.join("part.2")).await.unwrap());

    // 回线：宕机前 part 未丢，可正常列举并提交
    c.mesh.up("n1");
    assert_eq!(
        c.stores[1]
            .list_parts("alpha", "k", &id)
            .await
            .unwrap()
            .len(),
        1
    );
    let info = c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
        .await
        .unwrap();
    assert_eq!(info.size, 11);
}

/// 直投错节点（mesh handler 收到非本地属主的 id）→ NoSuchUpload，绝不碰属主暂存区；
/// 未知属主（不在拓扑内）→ Unavailable（503，而不是 404）。
#[tokio::test]
async fn mpu_mesh_wrong_node_and_unknown_owner() {
    use crate::mesh::{MpuPartUpload, MpuRef};

    let c = two_node().await;
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let uuid = super::mpu_route::split_upload_id(&id).unwrap().1;
    // 在 n1 上放一个 part，用于证明错投不会动它
    c.stores[0]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"owner-data"]))
        .await
        .unwrap();

    let wrong = MpuRef {
        bucket: "alpha".into(),
        key: "k".into(),
        upload_id: id.clone(),
    };
    assert!(matches!(
        c.stores[1].mesh_mpu_list_parts(wrong.clone()).await,
        Err(Error::NoSuchUpload(_))
    ));
    assert!(matches!(
        c.stores[1].mesh_mpu_abort(wrong.clone()).await,
        Err(Error::NoSuchUpload(_))
    ));
    assert!(matches!(
        c.stores[1]
            .mesh_mpu_complete(crate::mesh::MpuComplete {
                bucket: "alpha".into(),
                key: "k".into(),
                upload_id: id.clone(),
                parts: vec![(1, "whatever".into())],
                mode: VersioningMode::Off,
                encryption: None,
                lock: None,
            })
            .await,
        Err(Error::NoSuchUpload(_))
    ));
    assert!(matches!(
        c.stores[1]
            .mesh_mpu_upload_part(MpuPartUpload {
                bucket: "alpha".into(),
                key: "k".into(),
                upload_id: id.clone(),
                part_number: 1,
                sse_c_md5: None,
                body: stream_of(&[b"x"]),
            })
            .await,
        Err(Error::NoSuchUpload(_))
    ));
    // 错投后属主 part 仍在
    assert_eq!(
        c.stores[0]
            .list_parts("alpha", "k", &id)
            .await
            .unwrap()
            .len(),
        1
    );

    // 语法合法但拓扑中不存在的属主 → 所有操作 503
    let ghost = super::mpu_route::format_upload_id("n9", &uuid);
    assert!(matches!(
        c.stores[1]
            .upload_part("alpha", "k", &ghost, 1, stream_of(&[b"x"]))
            .await,
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        c.stores[1].list_parts("alpha", "k", &ghost).await,
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        c.stores[1]
            .abort_multipart_upload("alpha", "k", &ghost)
            .await,
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        c.stores[1]
            .complete_multipart_upload("alpha", "k", &ghost, &[(1, "e".into())])
            .await,
        Err(Error::Unavailable(_))
    ));
    // 语法非法（非 UUID 右半 / 超长前缀）→ NoSuchUpload
    for bad in ["n1.not-a-uuid", "!!!.bad", ""] {
        assert!(matches!(
            c.stores[1].list_parts("alpha", "k", bad).await,
            Err(Error::NoSuchUpload(_))
        ));
    }
}

/// LMU 并集：集群属主前缀重建后与单机口径兼容；任节点列举 = 全集群；
/// 经非属主节点 abort 生效。
#[tokio::test]
async fn mpu_mesh_lmu_union_and_abort_route() {
    let c = two_node().await;
    let a = c.stores[0]
        .create_multipart_upload("alpha", "same", None, Default::default())
        .await
        .unwrap();
    let b = c.stores[1]
        .create_multipart_upload("alpha", "same", None, Default::default())
        .await
        .unwrap();
    assert_eq!(owner_of(&a).as_deref(), Some("n1"));
    assert_eq!(owner_of(&b).as_deref(), Some("n2"));

    let full = c.stores[0]
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    let mut expect = vec![
        ("same".to_string(), a.clone()),
        ("same".to_string(), b.clone()),
    ];
    expect.sort();
    let got: Vec<_> = full
        .uploads
        .iter()
        .map(|u| (u.key.clone(), u.upload_id.clone()))
        .collect();
    assert_eq!(got, expect);
    assert!(!full.is_truncated);

    // 逐页（max=1）跨两源仍无重无漏
    let mut paged = Vec::new();
    let mut req = UploadListRequest {
        max_uploads: Some(1),
        ..Default::default()
    };
    for _ in 0..6 {
        let p = c.stores[1]
            .list_multipart_uploads("alpha", &req)
            .await
            .unwrap();
        paged.extend(
            p.uploads
                .iter()
                .map(|u| (u.key.clone(), u.upload_id.clone())),
        );
        if !p.is_truncated {
            break;
        }
        req.key_marker = p.next_key_marker;
        req.upload_id_marker = p.next_upload_id_marker;
    }
    assert_eq!(paged, expect);

    // n2 abort n1 属主的 MPU → 转发到 n1；并集只剩 n2 的
    c.stores[1]
        .abort_multipart_upload("alpha", "same", &a)
        .await
        .unwrap();
    let r = c.stores[0]
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(r.uploads.len(), 1);
    assert_eq!(r.uploads[0].upload_id, b);
}

/// 语义错误穿透转发：ETag 不符仍是 InvalidPart（不是笼统传输错），
/// 且失败不提交、暂存区仍在。
#[tokio::test]
async fn mpu_mesh_error_fidelity_and_no_partial_commit() {
    let c = two_node().await;
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"payload"]))
        .await
        .unwrap();
    let e = c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, "deadbeef".into())])
        .await
        .unwrap_err();
    assert!(matches!(e, Error::InvalidPart(_)), "实得 {e:?}");
    assert!(c.stores[0].get_object("alpha", "k").await.is_err());
    assert_eq!(
        c.stores[0]
            .list_parts("alpha", "k", &id)
            .await
            .unwrap()
            .len(),
        1
    );
    // 正确 etag 仍可提交（失败未污染暂存区）
    c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
        .await
        .unwrap();
}

/// 升级前创建的旧格式 MPU：裸 UUID upload_id + 旧 mpu.meta 字段集 + 旧属主只宣告
/// wire=1（无 DP-0 握手）。新节点须能定位属主并 ListParts/Complete/Abort；complete
/// 转发保留 part 元数据，丢响应后的重试幂等；分片流仍 fail-closed。
///
/// 属主在本测试里仍实现 MPU mesh RPC（= v1.16.0+ 的旧二进制；v1.15- 无 RPC 的缺口
/// 见 `mpu_remote_owner` 注释）。
#[tokio::test]
async fn mpu_legacy_bare_uuid_cross_version_finish() {
    // 升级前二进制写出的 mpu.meta：只有当时存在的字段（新增字段靠 serde(default)）
    #[derive(serde::Serialize)]
    struct LegacyMpuMeta {
        content_type: Option<String>,
        user_metadata: std::collections::BTreeMap<String, String>,
        created_nanos: i64,
        sse_c_md5: Option<String>,
    }

    async fn legacy_stage(c: &super::tests::Cluster, key: &str) -> String {
        let uuid = uuid::Uuid::new_v4().to_string();
        let dir = layout::object_dir(&c.dirs[0][0], "alpha", key)
            .unwrap()
            .join(&uuid);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let legacy = LegacyMpuMeta {
            content_type: Some("text/plain".into()),
            user_metadata: Default::default(),
            created_nanos: 0,
            sse_c_md5: None,
        };
        tokio::fs::write(
            dir.join("mpu.meta"),
            rmp_serde::to_vec_named(&legacy).unwrap(),
        )
        .await
        .unwrap();
        uuid
    }

    let c = two_node().await;
    let uuid = legacy_stage(&c, "k").await;
    // 旧二进制：health 缺 data_wire_version → wire 1
    c.mesh.set_data_wire_version("n1", Some(0));

    // 旧属主本地收分片
    let etag = c.stores[0]
        .upload_part("alpha", "k", &uuid, 1, stream_of(&[b"legacy-payload"]))
        .await
        .unwrap();

    // 新节点：裸 UUID 属主发现（ListParts 探测）+ 跨版本 ListParts
    let parts = c.stores[1].list_parts("alpha", "k", &uuid).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].etag, etag);
    // 混版窗口 LMU 仍能列出（裸 UUID 原样，显式 degraded）
    let lmu = c.stores[1]
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert!(lmu.uploads.iter().any(|u| u.upload_id == uuid));
    assert!(lmu.degraded);

    // 分片：新节点经有界回退落到旧属主暂存区（裸 UUID 属主发现 + 同一 RPC），
    // 字节一致 → ETag 与本地 MD5 相符
    let etag2 = c.stores[1]
        .upload_part("alpha", "k", &uuid, 2, stream_of(&[b"wire1"]))
        .await
        .unwrap();
    assert_eq!(c.mesh.mpu_bounded_calls(), 1);
    assert_eq!(c.mesh.mpu_stream_calls(), 0, "旧属主不得走流式路径");
    let expected2 = {
        use md5::Digest as _;
        let mut h = md5::Md5::new();
        h.update(b"wire1");
        hex::encode(h.finalize())
    };
    assert_eq!(etag2, expected2);
    assert_eq!(
        c.stores[0]
            .list_parts("alpha", "k", &uuid)
            .await
            .unwrap()
            .len(),
        2
    );

    // 新节点 Complete → 旧属主提交；part 元数据（part_sizes）随转发保留
    let info = c.stores[1]
        .complete_multipart_upload("alpha", "k", &uuid, &[(1, etag.clone())])
        .await
        .unwrap();
    assert_eq!(info.size, 14);
    let expected_etag = {
        use md5::Digest as _;
        let mut h = md5::Md5::new();
        h.update(hex::decode(&etag).unwrap());
        format!("{}-1", hex::encode(h.finalize()))
    };
    assert_eq!(info.etag, expected_etag);
    assert_eq!(info.part_sizes, Some(vec![14]));
    assert_eq!(info.content_type.as_deref(), Some("text/plain"));
    assert_eq!(info.version_id.as_deref(), Some("null"));
    let (_, body) = c.stores[1].get_object("alpha", "k").await.unwrap();
    assert_eq!(body, Bytes::from_static(b"legacy-payload"));

    // 响应丢失后的重试：幂等返回同一版本（mod_time 不变 = 未重复提交）
    let again = c.stores[1]
        .complete_multipart_upload("alpha", "k", &uuid, &[(1, etag)])
        .await
        .unwrap();
    assert_eq!(again.etag, info.etag);
    assert_eq!(again.version_id, info.version_id);
    assert_eq!(again.part_sizes, info.part_sizes);
    assert_eq!(again.last_modified_nanos, info.last_modified_nanos);

    // 旧格式 MPU 也可从新节点 Abort
    let uuid2 = legacy_stage(&c, "k2").await;
    c.stores[1]
        .abort_multipart_upload("alpha", "k2", &uuid2)
        .await
        .unwrap();
    let dir2 = layout::object_dir(&c.dirs[0][0], "alpha", "k2")
        .unwrap()
        .join(&uuid2);
    assert!(!tokio::fs::try_exists(&dir2).await.unwrap());
    assert!(matches!(
        c.stores[1]
            .abort_multipart_upload("alpha", "k2", &uuid2)
            .await,
        Err(Error::NoSuchUpload(_))
    ));
}

/// wire-1 有界回退的预落盘残件（`<tmp>/mpu-*.part`）；空 = 清理到位。
fn spool_files(drive: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(layout::sys_dir(drive, "tmp")) else {
        return Vec::new();
    };
    rd.filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("mpu-"))
        })
        .collect()
}

/// wire-1 属主：有界回退逐字节转发（分块输入），属主 ETag/大小与本地 MD5 一致；
/// complete 后对象字节与原分片完全相同；协调者暂存文件不残留。
#[tokio::test]
async fn mpu_mesh_wire1_bounded_forward_byte_identical() {
    const CHUNKS: [&[u8]; 3] = [b"hello ", b"wire-1 ", b"fallback"];
    let c = two_node().await;
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    c.mesh.set_data_wire_version("n1", Some(0));

    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&CHUNKS))
        .await
        .unwrap();
    let expected = {
        use md5::Digest as _;
        let mut h = md5::Md5::new();
        for b in CHUNKS {
            h.update(b);
        }
        hex::encode(h.finalize())
    };
    assert_eq!(etag, expected, "ETag 必须是原始分片字节的 MD5");
    assert_eq!(c.mesh.mpu_bounded_calls(), 1);
    assert_eq!(c.mesh.mpu_stream_calls(), 0);
    let parts = c.stores[0].list_parts("alpha", "k", &id).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].size, 21);
    assert_eq!(parts[0].etag, expected);
    assert!(spool_files(&c.dirs[1][0]).is_empty(), "预落盘必须清理");

    let info = c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
        .await
        .unwrap();
    assert_eq!(info.size, 21);
    let (_, body) = c.stores[0].get_object("alpha", "k").await.unwrap();
    assert_eq!(body, Bytes::from_static(b"hello wire-1 fallback"));
}

/// 超有界回退上限：在**发出任何 RPC 前**拒绝，属主无暂存、协调者无残件；
/// 上限内的同一条路径仍可传。
#[tokio::test]
async fn mpu_mesh_wire1_bounded_over_limit_rejected() {
    let c = super::tests::Cluster::new_with_mpu_forward_max(2, 3, 8).await;
    c.stores[0].create_bucket("alpha").await.unwrap();
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    c.mesh.set_data_wire_version("n1", Some(0));

    let err = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"0123456789"]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidPart(_)), "{err:?}");
    assert_eq!(c.mesh.mpu_bounded_calls(), 0, "超限不得发出转发 RPC");
    assert!(
        c.stores[0]
            .list_parts("alpha", "k", &id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(spool_files(&c.dirs[1][0]).is_empty());

    // 恰好等于上限 → 正常转发
    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"12345678"]))
        .await
        .unwrap();
    assert_eq!(c.mesh.mpu_bounded_calls(), 1);
    assert_eq!(
        c.stores[0].list_parts("alpha", "k", &id).await.unwrap()[0].etag,
        etag
    );
}

/// 转发中途失败：协调者预落盘文件清理、属主无半截 part；随后重试可成功
/// （失败未污染暂存区，也不留可复用的半截数据）。
#[tokio::test]
async fn mpu_mesh_wire1_bounded_mid_forward_failure_cleans_up() {
    let c = two_node().await;
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    c.mesh.set_data_wire_version("n1", Some(0));
    c.mesh.fail_once("n1", "mpu_upload_part_bounded");

    let err = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"payload"]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unavailable(_)), "{err:?}");
    assert_eq!(c.mesh.mpu_bounded_calls(), 1);
    assert!(
        c.stores[0]
            .list_parts("alpha", "k", &id)
            .await
            .unwrap()
            .is_empty(),
        "属主不得留下半截 part"
    );
    assert!(spool_files(&c.dirs[1][0]).is_empty(), "预落盘必须清理");

    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"payload"]))
        .await
        .unwrap();
    assert_eq!(c.mesh.mpu_bounded_calls(), 2);
    c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
        .await
        .unwrap();
    let (_, body) = c.stores[0].get_object("alpha", "k").await.unwrap();
    assert_eq!(body, Bytes::from_static(b"payload"));
}

/// 同版（wire 2）仍走流式路径：有界回退不得成为默认路径。
#[tokio::test]
async fn mpu_mesh_wire2_streaming_preferred() {
    let c = two_node().await;
    let id = c.stores[0]
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();

    let etag = c.stores[1]
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"stream"]))
        .await
        .unwrap();
    assert_eq!(c.mesh.mpu_stream_calls(), 1);
    assert_eq!(c.mesh.mpu_bounded_calls(), 0);
    assert!(spool_files(&c.dirs[1][0]).is_empty(), "流式不产生预落盘");
    c.stores[1]
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
        .await
        .unwrap();
}

/// 单机回归：非集群 Create 仍是裸 UUID（无前缀），本地 MPU 全流程不变。
#[tokio::test]
async fn mpu_single_node_legacy_upload_id_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let id = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    assert!(!id.contains('.'), "单机 upload_id 必须是裸 UUID：{id}");
    assert!(uuid::Uuid::parse_str(&id).is_ok());

    let etag = s
        .upload_part("alpha", "k", &id, 1, stream_of(&[b"single"]))
        .await
        .unwrap();
    let info = s
        .complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
        .await
        .unwrap();
    assert_eq!(info.size, 6);
    let (_, body) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(body, Bytes::from_static(b"single"));

    // 非集群进程收到带前缀 id → NoSuchUpload（不越界访问其他节点暂存区）
    let prefixed = super::mpu_route::format_upload_id("n1", &uuid::Uuid::new_v4().to_string());
    assert!(matches!(
        s.list_parts("alpha", "k", &prefixed).await,
        Err(Error::NoSuchUpload(_))
    ));
}
