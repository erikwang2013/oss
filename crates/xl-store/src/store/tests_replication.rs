//! M6 切片 3：复制状态面（store 级）——版本级 set/get 往返、清除、白名单、epoch 抬升。

use super::*;

async fn store() -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    (s, dir)
}

fn stream_of(b: &'static [u8]) -> ByteStream {
    Box::pin(futures::stream::once(
        async move { Ok(Bytes::from_static(b)) },
    ))
}

async fn put(s: &Store, key: &str, body: &'static [u8], mode: VersioningMode) -> ObjectInfo {
    s.put_object_stream_with_meta(
        "alpha",
        key,
        stream_of(body),
        None,
        Default::default(),
        None,
        mode,
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap()
}

/// V3①：delete marker 版本允许设置/清除复制状态（切片 4 DeleteMarkerReplication 依赖）。
#[tokio::test]
async fn replication_status_on_delete_marker() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let v = put(&s, "mk", b"x", VersioningMode::Enabled).await;
    assert_eq!(v.version_id.as_deref().map(str::len), Some(36));
    // 删除生成 marker（版本桶）
    let out = s
        .delete_object_versioned("alpha", "mk", None, VersioningMode::Enabled)
        .await
        .unwrap();
    let marker_vid = match out {
        DeleteOutcome::Marker { version_id } => version_id,
        other => panic!("应生成 marker：{other:?}"),
    };
    let m = s.read_meta("alpha", "mk").await.unwrap().unwrap();
    assert!(
        m.find_version(&marker_vid).unwrap().delete_marker(),
        "前置：目标应为 marker"
    );
    // marker 可置状态 + 可清除
    s.set_replication_status("alpha", "mk", &marker_vid, Some("COMPLETED"))
        .await
        .unwrap();
    let m = s.read_meta("alpha", "mk").await.unwrap().unwrap();
    assert_eq!(
        m.find_version(&marker_vid)
            .unwrap()
            .replication_status
            .as_deref(),
        Some("COMPLETED")
    );
    s.set_replication_status("alpha", "mk", &marker_vid, None)
        .await
        .unwrap();
    let m = s.read_meta("alpha", "mk").await.unwrap().unwrap();
    assert_eq!(
        m.find_version(&marker_vid).unwrap().replication_status,
        None
    );
    // 版本不存在仍拒
    let e = s
        .set_replication_status("alpha", "mk", "missing", Some("PENDING"))
        .await;
    assert!(matches!(e, Err(Error::NoSuchKey(_))), "{e:?}");
}

/// P2-2：PENDING 写入不得把终态降级（并发 worker 已完成后的迟到 PENDING）。
#[tokio::test]
async fn pending_write_does_not_downgrade_terminal_status() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let v = put(&s, "k", b"x", VersioningMode::Enabled).await;
    let vid = v.version_id.unwrap();
    // 初次：None → PENDING（true）
    assert!(
        s.mark_replication_pending("alpha", "k", &vid)
            .await
            .unwrap()
    );
    assert_eq!(
        s.head_object_version("alpha", "k", &vid)
            .await
            .unwrap()
            .replication_status
            .as_deref(),
        Some("PENDING")
    );
    // 终态后迟到 PENDING：不降级
    s.set_replication_status("alpha", "k", &vid, Some("COMPLETED"))
        .await
        .unwrap();
    assert!(
        !s.mark_replication_pending("alpha", "k", &vid)
            .await
            .unwrap()
    );
    assert_eq!(
        s.head_object_version("alpha", "k", &vid)
            .await
            .unwrap()
            .replication_status
            .as_deref(),
        Some("COMPLETED")
    );
    // FAILED/REPLICA 同样不降级
    s.set_replication_status("alpha", "k", &vid, Some("FAILED"))
        .await
        .unwrap();
    assert!(
        !s.mark_replication_pending("alpha", "k", &vid)
            .await
            .unwrap()
    );
    s.set_replication_status("alpha", "k", &vid, Some("REPLICA"))
        .await
        .unwrap();
    assert!(
        !s.mark_replication_pending("alpha", "k", &vid)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn replication_status_roundtrip_clear_and_whitelist() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();

    // 版本桶：两版本，状态互不影响
    let v1 = put(&s, "k", b"one", VersioningMode::Enabled).await;
    let v2 = put(&s, "k", b"two", VersioningMode::Enabled).await;
    let vid1 = v1.version_id.unwrap();
    s.set_replication_status("alpha", "k", &vid1, Some("PENDING"))
        .await
        .unwrap();
    let a = s.head_object_version("alpha", "k", &vid1).await.unwrap();
    assert_eq!(a.replication_status.as_deref(), Some("PENDING"));
    // 链尾（v2）不受影响
    let b = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(b.replication_status, None);
    assert_ne!(b.version_id, Some(vid1.clone()));

    // 非版本化对象：版本号 "null"
    let nv = put(&s, "nk", b"x", VersioningMode::Off).await;
    assert_eq!(nv.version_id.as_deref(), Some("null"));
    s.set_replication_status("alpha", "nk", "null", Some("COMPLETED"))
        .await
        .unwrap();
    assert_eq!(
        s.head_object("alpha", "nk")
            .await
            .unwrap()
            .replication_status
            .as_deref(),
        Some("COMPLETED")
    );
    // 清除
    s.set_replication_status("alpha", "nk", "null", None)
        .await
        .unwrap();
    assert_eq!(
        s.head_object("alpha", "nk")
            .await
            .unwrap()
            .replication_status,
        None
    );

    // 白名单（set 入口）
    let e = s
        .set_replication_status("alpha", "nk", "null", Some("BOGUS"))
        .await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");
    // 未知版本
    let e = s
        .set_replication_status("alpha", "k", "missing", Some("PENDING"))
        .await;
    assert!(matches!(e, Err(Error::NoSuchKey(_))), "{e:?}");

    // epoch 抬升：设置后链尾 epoch 严格大于设置前
    let before = s
        .read_meta("alpha", "k")
        .await
        .unwrap()
        .unwrap()
        .max_epoch();
    s.set_replication_status("alpha", "k", &v2.version_id.unwrap(), Some("FAILED"))
        .await
        .unwrap();
    let after = s
        .read_meta("alpha", "k")
        .await
        .unwrap()
        .unwrap()
        .max_epoch();
    assert!(after > before, "epoch 必须抬升：{before} → {after}");
}
