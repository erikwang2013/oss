//! DP-1 混版窗口（placement v1）读/写矩阵：new→old（本版协调者 → legacy peer）。
//!
//! 覆盖行：PUT/GET/Range/EC 重建、无版本 delete、metadata copy、ListV2 token、
//! ListV1 marker（降级重复边界）、ListVersions（degraded + require_complete 503）、
//! LMU 并集降级、MPU 转发 503 / 本地 MPU 完成、语义标志不得下发给 legacy peer。
//! 已知降级：旧节点重写 meta 可能丢新字段（tier/CMK 本就不在混版窗口内）。
//! placement v2 不属于混版窗口：未宣告 v2 指纹（含不可达）一律写前拒绝。

use super::tests::Cluster;
use super::*;

fn stream_of(b: &'static [u8]) -> ByteStream {
    Box::pin(futures::stream::once(
        async move { Ok(Bytes::from_static(b)) },
    ))
}

fn has_dot_new(dir: &std::path::Path) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.file_name()
            .is_some_and(|n| n.to_string_lossy().ends_with(".new"))
        {
            return true;
        }
        if p.is_dir() && has_dot_new(&p) {
            return true;
        }
    }
    false
}

async fn put_keys(c: &Cluster, keys: &[&str]) {
    let s = &c.stores[0];
    s.create_bucket("bkt").await.unwrap();
    for k in keys {
        s.put_object("bkt", k, Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }
}

/// Task 3：V1 marker 页在 legacy peer 上改发包含式 marker——边界键重复（不丢键），
/// 并置 degraded_marker；无 marker 首页不标降级。
#[tokio::test]
async fn v1_list_marker_with_legacy_peer_degrades_and_repeats_boundary() {
    let c = Cluster::new(4, 3).await;
    put_keys(&c, &["a", "b", "c"]).await;
    let s = &c.stores[0];
    c.mesh.set_data_wire_version("n2", Some(0));

    let first = s
        .list_objects(
            "bkt",
            &ListRequest {
                max_keys: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!first.degraded_marker);
    assert_eq!(first.objects.last().unwrap().key, "b");

    let page = s
        .list_objects(
            "bkt",
            &ListRequest {
                max_keys: Some(2),
                marker: Some("b".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(page.degraded_marker);
    assert!(
        page.objects.iter().any(|o| o.key == "b"),
        "legacy inclusive marker repeats boundary: {:?}",
        page.objects.iter().map(|o| &o.key).collect::<Vec<_>>()
    );
}

/// Task 4：任一 peer 为 legacy → degraded；用户可见版本列表必须 fail closed。
#[tokio::test]
async fn version_list_with_legacy_peer_is_degraded_and_fails_closed() {
    let c = Cluster::new(4, 3).await;
    put_keys(&c, &["k"]).await;
    let s = &c.stores[0];
    c.mesh.set_data_wire_version("n3", Some(0));
    let vl = s
        .list_object_versions("bkt", "", 100, None, None)
        .await
        .unwrap();
    assert!(vl.degraded);
    assert!(matches!(
        vl.require_complete(),
        Err(Error::PeerIncompatible(_))
    ));
}

/// Task 5（同时是 DP-1 LMU 行）：legacy peer 的可达并集仍返回，标记 degraded。
#[tokio::test]
async fn list_uploads_with_legacy_peer_degrades_and_unions() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.create_multipart_upload("bkt", "from-n1", None, Default::default())
        .await
        .unwrap();
    c.stores[1]
        .create_multipart_upload("bkt", "from-n2", None, Default::default())
        .await
        .unwrap();
    c.mesh.set_data_wire_version("n2", Some(0));

    let r = s1
        .list_multipart_uploads("bkt", &UploadListRequest::default())
        .await
        .unwrap();
    assert!(r.degraded);
    let keys: Vec<&str> = r.uploads.iter().map(|u| u.key.as_str()).collect();
    assert!(keys.contains(&"from-n1"), "{keys:?}");
    assert!(keys.contains(&"from-n2"), "{keys:?}");
}

/// Task 6：placement v2 下 legacy 或不可达 peer 均在本地 `.new` 落盘前拒绝；
/// 恢复同版后写入成功。
#[tokio::test]
async fn placement_v2_with_legacy_peer_rejected_before_local_write() {
    let c = Cluster::new_with_placement(4, 3, true).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();

    c.mesh.set_data_wire_version("n2", Some(0));
    let e = s1
        .put_object("bkt", "k", Bytes::from_static(b"x"), None)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::PeerIncompatible(_)), "{e:?}");
    assert!(
        !c.dirs.iter().any(|ds| ds.iter().any(|d| has_dot_new(d))),
        "placement v2 混版必须在任何本地 .new 前拒绝"
    );

    // 不可达 = 无法确认能力，placement v2 同样拒绝（不得当成兼容）
    c.mesh.set_data_wire_version("n2", None);
    c.mesh.down("n2");
    let e = s1
        .put_object("bkt", "k2", Bytes::from_static(b"x"), None)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::PeerIncompatible(_)), "{e:?}");

    c.mesh.up("n2");
    s1.put_object("bkt", "k3", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    assert_eq!(s1.get_object("bkt", "k3").await.unwrap().1.len(), 1);
}

/// DP-1：PUT/GET/Range 在 legacy peer 在场时可用；摘节点后 EC 重建读仍一致。
#[tokio::test]
async fn dp1_put_get_range_ec_new_to_old() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    c.mesh.set_data_wire_version("n2", Some(0));
    let body = vec![9u8; 200 * 1024];
    s1.put_object("bkt", "k", Bytes::from(body.clone()), None)
        .await
        .unwrap();
    assert_eq!(s1.get_object("bkt", "k").await.unwrap().1.to_vec(), body);
    assert_eq!(
        s1.get_object_range("bkt", "k", 7..77).await.unwrap().len(),
        70
    );

    // 9/12 槽存活（data=8）→ 重建读
    c.mesh.down("n2");
    assert_eq!(s1.get_object("bkt", "k").await.unwrap().1.to_vec(), body);
}

/// DP-1：无版本 delete 经 legacy peer 生效。
#[tokio::test]
async fn dp1_unversioned_delete_new_to_old() {
    let c = Cluster::new(4, 3).await;
    put_keys(&c, &["k"]).await;
    let s1 = &c.stores[0];
    c.mesh.set_data_wire_version("n2", Some(0));
    assert!(s1.delete_object("bkt", "k").await.unwrap());
    assert!(matches!(
        s1.get_object("bkt", "k").await,
        Err(Error::NoSuchKey(_))
    ));
    // 旧节点视角同样不可见
    assert!(matches!(
        c.stores[1].get_object("bkt", "k").await,
        Err(Error::NoSuchKey(_))
    ));
}

/// DP-1：metadata copy（content-type + user metadata）跨混版窗口保持。
#[tokio::test]
async fn dp1_metadata_copy_new_to_old() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    let mut meta = std::collections::BTreeMap::new();
    meta.insert("k1".to_string(), "v1".to_string());
    s1.put_object_with_meta(
        "bkt",
        "src",
        Bytes::from_static(b"copy me"),
        Some("text/plain".into()),
        meta.clone(),
    )
    .await
    .unwrap();
    c.mesh.set_data_wire_version("n2", Some(0));
    let info = s1.copy_object("bkt", "src", "bkt", "dst").await.unwrap();
    assert_eq!(info.content_type.as_deref(), Some("text/plain"));
    assert_eq!(info.user_metadata, meta);
    let (_, got) = c.stores[1].get_object("bkt", "dst").await.unwrap();
    assert_eq!(got, Bytes::from_static(b"copy me"));
}

/// DP-1：V2 token 分页在混版下不丢键、不重复，且不标 V1 降级。
#[tokio::test]
async fn dp1_list_v2_token_new_to_old() {
    let c = Cluster::new(4, 3).await;
    put_keys(&c, &["a", "b", "c", "d"]).await;
    let s1 = &c.stores[0];
    c.mesh.set_data_wire_version("n2", Some(0));

    let p1 = s1
        .list_objects(
            "bkt",
            &ListRequest {
                max_keys: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!p1.degraded_marker);
    assert_eq!(
        p1.objects
            .iter()
            .map(|o| o.key.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    let token = p1.next_token.expect("truncated V2 page has next_token");
    let p2 = s1
        .list_objects(
            "bkt",
            &ListRequest {
                max_keys: Some(2),
                continuation_token: Some(token),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        p2.objects
            .iter()
            .map(|o| o.key.as_str())
            .collect::<Vec<_>>(),
        vec!["c", "d"]
    );
    assert!(!p2.degraded_marker);
}

/// DP-1：版本列表（new→old）不返回部分并集。
#[tokio::test]
async fn dp1_list_versions_new_to_old() {
    let c = Cluster::new(4, 3).await;
    put_keys(&c, &["k"]).await;
    c.mesh.set_data_wire_version("n2", Some(0));
    let e = c.stores[0]
        .list_object_versions("bkt", "", 100, None, None)
        .await
        .unwrap()
        .require_complete()
        .unwrap_err();
    assert!(matches!(e, Error::PeerIncompatible(_)), "{e:?}");
}

/// DP-1：MPU 转发（协调者 ≠ 属主）在 legacy 属主上 503；本地 MPU 仍可完成。
#[tokio::test]
async fn dp1_mpu_forward_new_to_old_fails_closed() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    let remote = c.stores[1]
        .create_multipart_upload("bkt", "k", None, Default::default())
        .await
        .unwrap();
    c.mesh.set_data_wire_version("n2", Some(0));
    let e = s1
        .upload_part("bkt", "k", &remote, 1, stream_of(b"data"))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::PeerIncompatible(_)), "{e:?}");
}

/// DP-1：本地属主 MPU 在混版窗口内正常完成（不跨版本转发）。
#[tokio::test]
async fn dp1_local_mpu_completes_new_to_old() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    let id = s1
        .create_multipart_upload("bkt", "k", None, Default::default())
        .await
        .unwrap();
    c.mesh.set_data_wire_version("n3", Some(0));
    let etag = s1
        .upload_part("bkt", "k", &id, 1, stream_of(b"mpu-body"))
        .await
        .unwrap();
    s1.complete_multipart_upload("bkt", "k", &id, &[(1, etag)])
        .await
        .unwrap();
    assert_eq!(
        s1.get_object("bkt", "k").await.unwrap().1,
        Bytes::from_static(b"mpu-body")
    );
}

/// DP-1：new→old 的 ListLocal 绝不携带 legacy 不认识的语义标志；同请求对现版
/// peer 必须携带（否则测试会因两标志恒 false 而空转）。
#[tokio::test]
async fn dp1_new_to_old_requests_strip_semantic_flags() {
    let c = Cluster::new(4, 3).await;
    put_keys(&c, &["a", "b", "c"]).await;
    let s1 = &c.stores[0];
    c.mesh.set_data_wire_version("n2", Some(0));

    s1.list_objects(
        "bkt",
        &ListRequest {
            max_keys: Some(2),
            marker: Some("b".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    s1.list_object_versions("bkt", "", 100, None, None)
        .await
        .unwrap();

    let reqs = c.mesh.list_local_requests();
    let n2: Vec<_> = reqs.iter().filter(|(id, _)| id == "n2").collect();
    assert!(!n2.is_empty(), "legacy peer 仍应收到基础 ListLocal");
    assert!(
        n2.iter()
            .all(|(_, r)| !r.marker_exclusive && !r.include_deleted),
        "legacy peer 收到语义标志: {n2:?}"
    );
    let n3: Vec<_> = reqs.iter().filter(|(id, _)| id == "n3").collect();
    assert!(
        n3.iter().any(|(_, r)| r.marker_exclusive),
        "现版 peer 应收到 V1 排他 marker"
    );
    assert!(
        n3.iter().any(|(_, r)| r.include_deleted),
        "现版 peer 应收到 include_deleted"
    );
}
