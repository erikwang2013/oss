//! M8 S4a：存储类/分层元数据——往返、phase gate、MPU class、DeleteOutcome tier refs。

use super::*;
use crate::meta::{TierPhase, TierState};

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

async fn put(s: &Store, key: &str, versioned: bool, class: Option<&str>) -> ObjectInfo {
    s.put_object_stream_with_meta(
        "alpha",
        key,
        stream_of(b"tier-data"),
        None,
        Default::default(),
        None,
        versioned,
        None,
        None,
        None,
        class.map(str::to_string),
    )
    .await
    .unwrap()
}

fn tier(phase: TierPhase) -> TierState {
    TierState {
        backend: "s3".into(),
        key: "cold/alpha/k".into(),
        size: 9,
        etag: "e".into(),
        sha256: "s".into(),
        phase,
        archived_at: 100,
        restore_expires_at: None,
    }
}

#[tokio::test]
async fn storage_class_roundtrip_and_normalization() {
    let (s, _d) = store().await;
    let info = put(&s, "k", true, Some("STANDARD_IA")).await;
    assert_eq!(info.storage_class.as_deref(), Some("STANDARD_IA"));
    let head = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(head.storage_class.as_deref(), Some("STANDARD_IA"));
    let (got, _) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(got.storage_class.as_deref(), Some("STANDARD_IA"));
    let listed = s
        .list_objects("alpha", &ListRequest::default())
        .await
        .unwrap();
    assert_eq!(
        listed.objects[0].storage_class.as_deref(),
        Some("STANDARD_IA")
    );
    let vl = s
        .list_object_versions("alpha", "", 10, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries[0].storage_class.as_deref(), Some("STANDARD_IA"));

    // STANDARD 归一为 None；未知类拒
    let i = put(&s, "std", false, Some("STANDARD")).await;
    assert_eq!(i.storage_class, None);
    let e = s
        .put_object_stream_with_meta(
            "alpha",
            "bad",
            stream_of(b"x"),
            None,
            Default::default(),
            None,
            false,
            None,
            None,
            None,
            Some("WARM".into()),
        )
        .await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");
}

#[tokio::test]
async fn archived_phase_gates_reads_but_head_works() {
    let (s, _d) = store().await;
    put(&s, "k", false, Some("GLACIER")).await;
    let mut m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    m.versions.last_mut().unwrap().tier = Some(tier(TierPhase::Archived));
    s.write_meta("alpha", "k", &m, None).await.unwrap();

    // HEAD 可读元数据；GET 403 InvalidObjectState
    let head = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(head.tier_phase, Some(TierPhase::Archived));
    let e = s.get_object("alpha", "k").await;
    assert!(matches!(e, Err(Error::InvalidObjectState(_))), "{e:?}");
    let e = s.get_object_range_ext("alpha", "k", 0..1, None).await;
    assert!(matches!(e, Err(Error::InvalidObjectState(_))), "{e:?}");

    // effective_parts 为空 → fsck 不报缺片
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert!(m.latest().unwrap().effective_parts().is_empty());
    let rep = s.fsck(false).await.unwrap();
    assert_eq!(rep.missing_shards, 0, "{rep:?}");

    // Restored：本地数据可读（数据仍在盘上）
    let mut m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    m.versions.last_mut().unwrap().tier = Some(tier(TierPhase::Restored));
    s.write_meta("alpha", "k", &m, None).await.unwrap();
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&bytes[..], b"tier-data");
}

#[tokio::test]
async fn delete_outcome_carries_tier_refs() {
    let (s, _d) = store().await;
    // 非版本桶：整键删带 tiers
    put(&s, "plain", false, Some("GLACIER")).await;
    let mut m = s.read_meta("alpha", "plain").await.unwrap().unwrap();
    m.versions.last_mut().unwrap().tier = Some(tier(TierPhase::Archived));
    s.write_meta("alpha", "plain", &m, None).await.unwrap();
    match s
        .delete_object_versioned("alpha", "plain", None, false)
        .await
        .unwrap()
    {
        DeleteOutcome::Object { tiers } => {
            assert_eq!(tiers.len(), 1);
            assert_eq!(tiers[0].backend, "s3");
            assert_eq!(tiers[0].key, "cold/alpha/k");
        }
        other => panic!("应为 Object：{other:?}"),
    }

    // 版本桶：点名删版本带 tier
    let v = put(&s, "ver", true, Some("GLACIER")).await;
    let vid = v.version_id.unwrap();
    let mut m = s.read_meta("alpha", "ver").await.unwrap().unwrap();
    m.versions.last_mut().unwrap().tier = Some(tier(TierPhase::Archived));
    s.write_meta("alpha", "ver", &m, None).await.unwrap();
    match s
        .delete_object_versioned("alpha", "ver", Some(&vid), true)
        .await
        .unwrap()
    {
        DeleteOutcome::Version {
            version_id,
            tier: t,
        } => {
            assert_eq!(version_id, vid);
            assert_eq!(t.unwrap().key, "cold/alpha/k");
        }
        other => panic!("应为 Version：{other:?}"),
    }
}

#[tokio::test]
async fn mpu_storage_class_lands_on_completed_version() {
    let (s, _d) = store().await;
    let id = s
        .create_multipart_upload_ext(
            "alpha",
            "mp",
            Some("text/plain".into()),
            Default::default(),
            None,
            None,
            None,
            Some("ONEZONE_IA".into()),
        )
        .await
        .unwrap();
    let etag = s
        .upload_part("alpha", "mp", &id, 1, stream_of(b"hi"))
        .await
        .unwrap();
    let info = s
        .complete_multipart_upload("alpha", "mp", &id, &[(1, etag)])
        .await
        .unwrap();
    assert_eq!(info.storage_class.as_deref(), Some("ONEZONE_IA"));
    // 归档类 Create 拒（直写）
    let e = s
        .create_multipart_upload_ext(
            "alpha",
            "mp2",
            None,
            Default::default(),
            None,
            None,
            None,
            Some("GLACIER".into()),
        )
        .await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");
}

#[tokio::test]
async fn meta_decode_rejects_bad_tier_and_class() {
    let (s, _d) = store().await;
    put(&s, "k", false, None).await;
    let mut m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    m.versions.last_mut().unwrap().storage_class = Some("WARM".into());
    let bytes = m.encode().unwrap();
    assert!(matches!(xl_meta_decode(&bytes), Err(Error::CorruptMeta(_))));
    let mut m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    m.versions.last_mut().unwrap().tier = Some(TierState {
        backend: String::new(),
        key: "k".into(),
        size: 1,
        etag: "e".into(),
        sha256: "s".into(),
        phase: TierPhase::Archived,
        archived_at: 1,
        restore_expires_at: None,
    });
    let bytes = m.encode().unwrap();
    assert!(matches!(xl_meta_decode(&bytes), Err(Error::CorruptMeta(_))));
}

fn xl_meta_decode(bytes: &[u8]) -> crate::error::Result<crate::meta::XlMeta> {
    crate::meta::XlMeta::decode(bytes)
}
