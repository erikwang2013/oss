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
/// 数据落在 n1 暂存区，提交后 n1 可读。重复 complete/abort → NoSuchUpload。
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

    // 二次提交/迟到 abort：属主暂存区已无 mpu.meta → NoSuchUpload（不重复提交）
    assert!(matches!(
        c.stores[1]
            .complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
            .await,
        Err(Error::NoSuchUpload(_))
    ));
    assert!(matches!(
        c.stores[1].abort_multipart_upload("alpha", "k", &id).await,
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
