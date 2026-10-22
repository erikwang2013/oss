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

async fn put(s: &Store, key: &str, mode: VersioningMode, class: Option<&str>) -> ObjectInfo {
    s.put_object_stream_with_meta(
        "alpha",
        key,
        stream_of(b"tier-data"),
        None,
        Default::default(),
        None,
        mode,
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
    let info = put(&s, "k", VersioningMode::Enabled, Some("STANDARD_IA")).await;
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
    let i = put(&s, "std", VersioningMode::Off, Some("STANDARD")).await;
    assert_eq!(i.storage_class, None);
    let e = s
        .put_object_stream_with_meta(
            "alpha",
            "bad",
            stream_of(b"x"),
            None,
            Default::default(),
            None,
            VersioningMode::Off,
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
    put(&s, "k", VersioningMode::Off, Some("GLACIER")).await;
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
    put(&s, "plain", VersioningMode::Off, Some("GLACIER")).await;
    let mut m = s.read_meta("alpha", "plain").await.unwrap().unwrap();
    m.versions.last_mut().unwrap().tier = Some(tier(TierPhase::Archived));
    s.write_meta("alpha", "plain", &m, None).await.unwrap();
    match s
        .delete_object_versioned("alpha", "plain", None, VersioningMode::Off)
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
    let v = put(&s, "ver", VersioningMode::Enabled, Some("GLACIER")).await;
    let vid = v.version_id.unwrap();
    let mut m = s.read_meta("alpha", "ver").await.unwrap().unwrap();
    m.versions.last_mut().unwrap().tier = Some(tier(TierPhase::Archived));
    s.write_meta("alpha", "ver", &m, None).await.unwrap();
    match s
        .delete_object_versioned("alpha", "ver", Some(&vid), VersioningMode::Enabled)
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
            None,
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
            None,
        )
        .await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");
}

#[tokio::test]
async fn meta_decode_rejects_bad_tier_and_class() {
    let (s, _d) = store().await;
    put(&s, "k", VersioningMode::Off, None).await;
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

/// M8 S4b：归档幂等/换类拒绝、恢复两阶段顺序、过期回收。
#[tokio::test]
async fn archive_restore_state_machine() {
    let (s, _d) = store().await;
    let info = put(&s, "k", VersioningMode::Enabled, None).await;
    let vid = info.version_id.clone().unwrap();
    let data = b"tier-data";
    let sha = hex::encode(crate::mesh::content_hash(data));
    let spec = |class: &str| ArchiveSpec {
        backend: "s3".into(),
        key: "cold/alpha/k".into(),
        size: data.len() as u64,
        source_etag: info.etag.clone(),
        etag: "remote-etag".into(),
        sha256: sha.clone(),
        class: class.into(),
    };
    s.archive_version("alpha", "k", &vid, spec("GLACIER"), 100)
        .await
        .unwrap();
    // 同 class 幂等；不同 class 400
    s.archive_version("alpha", "k", &vid, spec("GLACIER"), 101)
        .await
        .unwrap();
    let e = s
        .archive_version("alpha", "k", &vid, spec("DEEP_ARCHIVE"), 102)
        .await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");

    let v = s.tier_version("alpha", "k", &vid).await.unwrap().unwrap();
    let t = v.tier.as_ref().unwrap();
    assert_eq!(t.phase, TierPhase::Archived);
    assert_eq!(t.etag, "remote-etag", "远端 ETag 与源 ETag 分开存");
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::InvalidObjectState(_))
    ));

    // Restoring：读仍 403；重复受理 409
    s.mark_restore_pending("alpha", "k", &vid).await.unwrap();
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::InvalidObjectState(_))
    ));
    let e = s.mark_restore_pending("alpha", "k", &vid).await;
    assert!(
        matches!(e, Err(Error::RestoreAlreadyInProgress(_))),
        "{e:?}"
    );

    // abort 回 Archived；再受理
    s.abort_restore("alpha", "k", &vid).await.unwrap();
    s.mark_restore_pending("alpha", "k", &vid).await.unwrap();
    // prepare 长度校验；prepare 后、commit 前不可见
    let e = s.restore_prepare("alpha", "k", &vid, b"short").await;
    assert!(matches!(e, Err(Error::CorruptMeta(_))), "{e:?}");
    s.restore_prepare("alpha", "k", &vid, data).await.unwrap();
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::InvalidObjectState(_))
    ));

    let expires = now_secs() + 1000;
    s.restore_commit("alpha", "k", &vid, expires).await.unwrap();
    let (head, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], data);
    assert_eq!(head.tier_phase, Some(TierPhase::Restored));
    assert_eq!(head.restore_expires_at, Some(expires));

    // 未到期不回收；到期回收后本地数据不可读、phase 回 Archived
    assert!(
        !s.expire_restore("alpha", "k", &vid, expires - 1)
            .await
            .unwrap()
    );
    assert!(s.expire_restore("alpha", "k", &vid, expires).await.unwrap());
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::InvalidObjectState(_))
    ));
    let v = s.tier_version("alpha", "k", &vid).await.unwrap().unwrap();
    assert_eq!(v.tier.unwrap().phase, TierPhase::Archived);
}

fn version_dir(s: &Store, key: &str, uuid: &str) -> std::path::PathBuf {
    crate::layout::object_dir(s.cfg.root(), "alpha", key)
        .unwrap()
        .join(uuid)
}

async fn uuid_of(s: &Store, key: &str, version_id: &str) -> String {
    s.read_meta("alpha", key)
        .await
        .unwrap()
        .unwrap()
        .versions
        .iter()
        .find(|v| v.version_id_str() == version_id)
        .unwrap()
        .part
        .uuid
        .clone()
}

fn archive_spec(key: &str, info: &ObjectInfo) -> ArchiveSpec {
    ArchiveSpec {
        backend: "s3".into(),
        key: format!("cold/alpha/{key}"),
        size: info.size,
        source_etag: info.etag.clone(),
        etag: "remote-etag".into(),
        sha256: hex::encode(crate::mesh::content_hash(b"tier-data")),
        class: "GLACIER".into(),
    }
}

/// 归档提交后回收版本目录；同一 uuid 仍被其它版本引用时保留。
#[tokio::test]
async fn archive_reclaims_physical_dir_unless_uuid_shared() {
    let (s, _d) = store().await;
    let info = put(&s, "k", VersioningMode::Enabled, None).await;
    let vid = info.version_id.clone().unwrap();
    let uuid = uuid_of(&s, "k", &vid).await;
    let dir = version_dir(&s, "k", &uuid);
    assert!(dir.exists(), "写入后应有版本目录");
    s.archive_version("alpha", "k", &vid, archive_spec("k", &info), 100)
        .await
        .unwrap();
    assert!(!dir.exists(), "归档后版本目录应被回收");

    // 第二个版本引用同一 uuid → 目录必须保留
    let (s, _d) = store().await;
    let a = put(&s, "shared", VersioningMode::Enabled, None).await;
    let b = put(&s, "shared", VersioningMode::Enabled, None).await;
    let a_vid = a.version_id.clone().unwrap();
    let b_vid = b.version_id.clone().unwrap();
    let uuid = uuid_of(&s, "shared", &a_vid).await;
    let mut m = s.read_meta("alpha", "shared").await.unwrap().unwrap();
    for v in &mut m.versions {
        if v.version_id_str() == b_vid {
            v.part.uuid = uuid.clone();
        }
    }
    s.write_meta("alpha", "shared", &m, None).await.unwrap();
    let dir = version_dir(&s, "shared", &uuid);
    s.archive_version("alpha", "shared", &a_vid, archive_spec("shared", &a), 100)
        .await
        .unwrap();
    assert!(dir.exists(), "共享 uuid 的版本目录不得回收");
}

/// prepare 已写 part.1、随后行更新失败 → abort 必须清掉 prepared 副本。
#[tokio::test]
async fn abort_restore_removes_prepared_data() {
    let (s, _d) = store().await;
    let info = put(&s, "k", VersioningMode::Enabled, None).await;
    let vid = info.version_id.clone().unwrap();
    let uuid = uuid_of(&s, "k", &vid).await;
    let dir = version_dir(&s, "k", &uuid);
    s.archive_version("alpha", "k", &vid, archive_spec("k", &info), 100)
        .await
        .unwrap();
    s.mark_restore_pending("alpha", "k", &vid).await.unwrap();
    s.restore_prepare("alpha", "k", &vid, b"tier-data")
        .await
        .unwrap();
    assert!(dir.join("part.1").exists());
    s.abort_restore("alpha", "k", &vid).await.unwrap();
    assert!(
        !dir.join("part.1").exists(),
        "abort 后不得残留 prepared 数据"
    );
    let v = s.tier_version("alpha", "k", &vid).await.unwrap().unwrap();
    assert_eq!(v.tier.unwrap().phase, TierPhase::Archived);
}

/// 归档提交前源版本已被改写（etag 不符）→ 拒绝且本地数据保持可读（meta 失败不毁本地）。
#[tokio::test]
async fn archive_meta_failure_keeps_local() {
    let (s, _d) = store().await;
    let info = put(&s, "keep", VersioningMode::Enabled, None).await;
    let vid = info.version_id.clone().unwrap();
    let e = s
        .archive_version(
            "alpha",
            "keep",
            &vid,
            ArchiveSpec {
                backend: "s3".into(),
                key: "cold/alpha/keep".into(),
                size: info.size,
                source_etag: "stale-etag".into(),
                etag: "remote".into(),
                sha256: "00".into(),
                class: "GLACIER".into(),
            },
            100,
        )
        .await;
    assert!(matches!(e, Err(Error::CorruptMeta(_))), "{e:?}");
    let (head, bytes) = s.get_object("alpha", "keep").await.unwrap();
    assert_eq!(head.tier_phase, None);
    assert_eq!(&bytes[..], b"tier-data");
}
