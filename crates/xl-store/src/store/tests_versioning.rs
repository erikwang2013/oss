//! 版本控制三态（S3 Suspended）与版本列表分页回归。

use super::*;

async fn store() -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
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

/// Enabled→Suspended→Enabled：Suspended 写只替换 null 版本，真 UUID 历史保留。
#[tokio::test]
async fn suspended_puts_replace_null_and_keep_history() {
    let (s, _d) = store().await;
    let v1 = put(&s, "k", b"v1", VersioningMode::Enabled).await;
    let vid1 = v1.version_id.clone().unwrap();
    assert_ne!(vid1, "null");

    let v2 = put(&s, "k", b"v2", VersioningMode::Suspended).await;
    assert_eq!(v2.version_id.as_deref(), Some("null"));
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert_eq!(m.versions.len(), 2);
    assert_eq!(m.versions[0].version_id_str(), vid1);
    assert_eq!(m.versions[1].version_id_str(), "null");
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], b"v2");

    // 再次 Suspended 写：null 版本原地替换，链长度不变
    let v3 = put(&s, "k", b"v3", VersioningMode::Suspended).await;
    assert_eq!(v3.version_id.as_deref(), Some("null"));
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert_eq!(m.versions.len(), 2);
    assert_eq!(m.versions[1].version_id_str(), "null");
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], b"v3");

    // 回到 Enabled：追加真 UUID，null 版本成为历史
    let v4 = put(&s, "k", b"v4", VersioningMode::Enabled).await;
    let vid4 = v4.version_id.clone().unwrap();
    assert_ne!(vid4, "null");
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert_eq!(m.versions.len(), 3);
    assert_eq!(m.latest().unwrap().version_id_str(), vid4);
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], b"v4");
    // null 版本仍可点名读（历史不丢）
    let (_, got) = s.get_object_version("alpha", "k", "null").await.unwrap();
    assert_eq!(&got[..], b"v3");
}

/// Suspended 删除：移除链中 null 版本及旧 null marker，追加新的 null marker；真历史不动。
#[tokio::test]
async fn suspended_delete_appends_null_marker_and_removes_null() {
    let (s, _d) = store().await;
    let v1 = put(&s, "k", b"v1", VersioningMode::Enabled).await;
    let vid1 = v1.version_id.unwrap();
    put(&s, "k", b"v2", VersioningMode::Suspended).await;

    let out = s
        .delete_object_versioned("alpha", "k", None, VersioningMode::Suspended)
        .await
        .unwrap();
    assert_eq!(
        out,
        DeleteOutcome::Marker {
            version_id: "null".into()
        }
    );
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert_eq!(m.versions.len(), 2);
    assert_eq!(m.versions[0].version_id_str(), vid1);
    assert!(m.latest().unwrap().delete_marker());
    assert_eq!(m.latest().unwrap().version_id_str(), "null");

    // 再删：旧 null marker 被替换，链长度仍为 2
    let out = s
        .delete_object_versioned("alpha", "k", None, VersioningMode::Suspended)
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::Marker { .. }));
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert_eq!(m.versions.len(), 2);
    assert!(m.latest().unwrap().delete_marker());

    // 点名删 null marker → 恢复可见（真历史 v1）
    let out = s
        .delete_object_versioned("alpha", "k", Some("null"), VersioningMode::Suspended)
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::Version { .. }));
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], b"v1");

    // 版本列表含 null marker 条目（is_marker/is_latest）
    s.delete_object_versioned("alpha", "k", None, VersioningMode::Suspended)
        .await
        .unwrap();
    let vl = s
        .list_object_versions("alpha", "", 1000, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries.len(), 2);
    assert!(vl.entries[0].is_marker && vl.entries[0].is_latest);
    assert_eq!(vl.entries[0].version_id, "null");
}

/// Suspended 桶上对不存在的键删除 = 创建 null marker（AWS 语义）。
#[tokio::test]
async fn suspended_delete_on_missing_key_creates_null_marker() {
    let (s, _d) = store().await;
    let out = s
        .delete_object_versioned("alpha", "fresh", None, VersioningMode::Suspended)
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::Marker { .. }));
    let m = s.read_meta("alpha", "fresh").await.unwrap().unwrap();
    assert_eq!(m.versions.len(), 1);
    assert!(m.latest().unwrap().delete_marker());
    assert_eq!(m.latest().unwrap().version_id_str(), "null");
    // 版本列表可见（delete-marker-latest 键不得被隐藏）
    let vl = s
        .list_object_versions("alpha", "", 1000, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries.len(), 1);
    assert!(vl.entries[0].is_marker);
}

/// key-marker 必须在本地截断**之前**生效：>1000 键的桶从中间续页不得为空。
#[tokio::test]
async fn list_versions_key_marker_before_limit() {
    let (s, _d) = store().await;
    const N: usize = 1005;
    for i in 0..N {
        let key = format!("k{i:04}");
        s.put_object("alpha", &key, Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }
    let page = s
        .list_object_versions("alpha", "", 100, Some("k0500"), None)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 100);
    assert!(page.is_truncated);
    assert_eq!(page.entries[0].key, "k0500");
    assert_eq!(page.entries[99].key, "k0599");
    assert_eq!(page.next_key_marker.as_deref(), Some("k0599"));
    assert_eq!(page.next_version_id_marker.as_deref(), Some("null"));

    // 完整翻页：1005 键恰好一次、键序升序
    let mut seen = Vec::new();
    let (mut km, mut vm) = (None, None);
    for _ in 0..20 {
        let p = s
            .list_object_versions("alpha", "", 100, km.as_deref(), vm.as_deref())
            .await
            .unwrap();
        seen.extend(p.entries.iter().map(|e| e.key.clone()));
        if !p.is_truncated {
            break;
        }
        km = p.next_key_marker;
        vm = p.next_version_id_marker;
    }
    assert_eq!(seen.len(), N);
    assert!(seen.windows(2).all(|w| w[0] <= w[1]), "键序必须升序");
    let mut uniq = seen.clone();
    uniq.dedup();
    assert_eq!(uniq.len(), N, "分页不得重复");
}

/// 4.19：版本删除的内容指纹仅在链中再无同 (etag,size) 版本时给出——目标据此按内容
/// 清理两端 UUID 不同的历史副本；同内容仍有活版本时必须不给（否则可能删掉活副本）。
#[tokio::test]
async fn version_delete_content_fingerprint_only_when_unique() {
    let (s, _d) = store().await;
    let a1 = put(&s, "k", b"same", VersioningMode::Enabled).await;
    let a2 = put(&s, "k", b"same", VersioningMode::Enabled).await;
    let b = put(&s, "k", b"other", VersioningMode::Enabled).await;
    assert_eq!(a1.etag, a2.etag, "同内容必须同 etag");

    let fingerprint = |out: DeleteOutcome| match out {
        DeleteOutcome::Version { content, .. } => content,
        other => panic!("unexpected: {other:?}"),
    };
    // 同内容的 a2 仍在链中 → 不给指纹。
    let out = s
        .delete_object_versioned(
            "alpha",
            "k",
            a1.version_id.as_deref(),
            VersioningMode::Enabled,
        )
        .await
        .unwrap();
    assert_eq!(fingerprint(out), None);
    // 最后一份同内容 → 给出指纹。
    let out = s
        .delete_object_versioned(
            "alpha",
            "k",
            a2.version_id.as_deref(),
            VersioningMode::Enabled,
        )
        .await
        .unwrap();
    assert_eq!(fingerprint(out), Some((a2.etag, a2.size)));
    // 内容本就唯一 → 给出指纹。
    let out = s
        .delete_object_versioned(
            "alpha",
            "k",
            b.version_id.as_deref(),
            VersioningMode::Enabled,
        )
        .await
        .unwrap();
    assert_eq!(fingerprint(out), Some((b.etag, b.size)));
}
