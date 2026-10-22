//! M5 切片 4：对象锁（store 级）——删除守卫、MPU 意图落地、decode 信任边界。

use super::*;
use crate::crypto::EncSpec;
use crate::meta::ObjectLockRecord;

async fn store() -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    (s, dir)
}

fn payload(n: usize) -> Bytes {
    Bytes::from(vec![7u8; n])
}

fn stream_of(b: Bytes) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(b) }))
}

fn lock(mode: &str, until_nanos: i64, legal_hold: bool) -> ObjectLockRecord {
    ObjectLockRecord {
        mode: Some(mode.to_string()),
        retain_until_nanos: Some(until_nanos),
        legal_hold,
    }
}

fn now_plus(secs: i64) -> i64 {
    super::now_nanos() + secs * 1_000_000_000
}

/// 写一个带锁版本（versioned 桶），返回 version_id。
async fn put_locked(s: &Store, key: &str, l: Option<ObjectLockRecord>) -> (String, ObjectInfo) {
    let info = s
        .put_object_stream_with_meta(
            "alpha",
            key,
            stream_of(payload(200_000)),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled, // versioned：锁对象只存在于版本桶（L7）
            None,
            l,
            None,
            None,
        )
        .await
        .unwrap();
    (info.version_id.clone().unwrap(), info)
}

#[tokio::test]
async fn lock_guards_matrix() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();

    // (a) COMPLIANCE 未到期：无名删与 bypass 皆拒（含 admin——store 无 admin 概念，一律拒）
    let (vid_c, info) = put_locked(&s, "c", Some(lock("COMPLIANCE", now_plus(3600), false))).await;
    assert!(info.lock.is_some());
    let e = s
        .delete_object_versioned("alpha", "c", Some(&vid_c), VersioningMode::Enabled)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)), "{e:?}");
    let e = s
        .delete_object_versioned_ext("alpha", "c", Some(&vid_c), VersioningMode::Enabled, true)
        .await
        .unwrap_err();
    assert!(
        matches!(e, Error::ObjectLocked(_)),
        "COMPLIANCE 不受 bypass 影响：{e:?}"
    );
    // 锁字面量在盘上（点名读版本仍可读内容）
    let (_, got) = s.get_object_version("alpha", "c", &vid_c).await.unwrap();
    assert_eq!(got.len(), 200_000);

    // (b) GOVERNANCE 未到期：默认拒；bypass=true 放行
    let (vid, _) = put_locked(&s, "g", Some(lock("GOVERNANCE", now_plus(3600), false))).await;
    let e = s
        .delete_object_versioned("alpha", "g", Some(&vid), VersioningMode::Enabled)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)), "{e:?}");
    let out = s
        .delete_object_versioned_ext("alpha", "g", Some(&vid), VersioningMode::Enabled, true)
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::Version { .. }));

    // (c) 到期（until 在过去）：可直接删
    let (vid, _) = put_locked(&s, "e", Some(lock("COMPLIANCE", now_plus(-10), false))).await;
    let out = s
        .delete_object_versioned("alpha", "e", Some(&vid), VersioningMode::Enabled)
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::Version { .. }));

    // (d) legal hold（无 retention）：一律拒，bypass 无效
    let hold = ObjectLockRecord {
        mode: None,
        retain_until_nanos: None,
        legal_hold: true,
    };
    let (vid, _) = put_locked(&s, "h", Some(hold)).await;
    let e = s
        .delete_object_versioned("alpha", "h", Some(&vid), VersioningMode::Enabled)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)), "{e:?}");
    let e = s
        .delete_object_versioned_ext("alpha", "h", Some(&vid), VersioningMode::Enabled, true)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)), "{e:?}");

    // (e) delete-marker（无版本号删）不受锁约束；锁定版本仍在
    let out = s
        .delete_object_versioned("alpha", "c", None, VersioningMode::Enabled)
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::Marker { .. }));
    let (_, still) = s.get_object_version("alpha", "c", &vid_c).await.unwrap();
    assert_eq!(still.len(), 200_000);
    // latest 是 marker → 普通读 404；点名读仍可（锁 = 保留，不是隐藏）
    assert!(matches!(
        s.get_object("alpha", "c").await,
        Err(Error::NoSuchKey(_))
    ));
}

#[tokio::test]
async fn mpu_lock_lands_at_complete() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let l = lock("COMPLIANCE", now_plus(3600), true); // 保留 + hold 同时
    let up = s
        .create_multipart_upload_ext(
            "alpha",
            "k",
            None,
            Default::default(),
            None,
            None,
            Some(&l),
            None,
            None,
        )
        .await
        .unwrap();
    let e1 = s
        .upload_part(
            "alpha",
            "k",
            &up,
            1,
            stream_of(payload(5 * 1024 * 1024 + 1)),
        )
        .await
        .unwrap();
    // complete 不带锁参数 → 由 create 记录落地（AWS：锁头只出现在 create）
    let info = s
        .complete_multipart_upload_versioned(
            "alpha",
            "k",
            &up,
            &[(1, e1)],
            VersioningMode::Off,
            None,
            None,
        )
        .await
        .unwrap();
    let rec = info
        .lock
        .as_ref()
        .expect("create 的锁意图须在 complete 落地");
    assert_eq!(rec.mode.as_deref(), Some("COMPLIANCE"));
    assert!(rec.legal_hold);
    // 守卫生效
    let vid = info.version_id.clone().unwrap();
    let e = s
        .delete_object_versioned("alpha", "k", Some(&vid), VersioningMode::Enabled)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)));
    // create 未声明 + complete 携带 → 400（fail-closed）
    let up2 = s
        .create_multipart_upload("alpha", "k2", None, Default::default())
        .await
        .unwrap();
    let e1 = s
        .upload_part(
            "alpha",
            "k2",
            &up2,
            1,
            stream_of(payload(5 * 1024 * 1024 + 1)),
        )
        .await
        .unwrap();
    let e = s
        .complete_multipart_upload_versioned(
            "alpha",
            "k2",
            &up2,
            &[(1, e1)],
            VersioningMode::Off,
            None,
            Some(lock("GOVERNANCE", now_plus(60), false)),
        )
        .await
        .unwrap_err();
    assert!(matches!(e, Error::InvalidArgument(_)), "{e:?}");
}

#[tokio::test]
async fn lock_record_decode_trust_boundary() {
    let mk = |mode: Option<&str>, until: Option<i64>, hold: bool| {
        let mut m = XlMeta {
            object_key: "k".into(),
            versions: vec![meta::Version::default()],
        };
        m.versions[0].part.uuid = "u".into();
        m.versions[0].lock = Some(ObjectLockRecord {
            mode: mode.map(str::to_string),
            retain_until_nanos: until,
            legal_hold: hold,
        });
        m
    };
    // 合法：成对 / 纯 hold / 空记录
    for ok in [
        mk(Some("GOVERNANCE"), Some(1), false),
        mk(Some("COMPLIANCE"), Some(1), true),
        mk(None, None, true),
        mk(None, None, false),
    ] {
        let back = XlMeta::decode(&ok.encode().unwrap()).unwrap();
        assert_eq!(back, ok);
    }
    // 非法：模式白名单 / 不成对 / 负 until
    for bad in [
        mk(Some("WORM"), Some(1), false),
        mk(Some("GOVERNANCE"), None, false),
        mk(None, Some(1), false),
        mk(Some("COMPLIANCE"), Some(-5), false),
    ] {
        assert!(
            matches!(
                XlMeta::decode(&bad.encode().unwrap()),
                Err(Error::CorruptMeta(_))
            ),
            "非法锁记录应解码即拒"
        );
    }
}

#[tokio::test]
async fn empty_lock_is_not_stored() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let empty = ObjectLockRecord {
        mode: None,
        retain_until_nanos: None,
        legal_hold: false,
    };
    let info = s
        .put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(payload(1000)),
            None,
            Default::default(),
            None,
            VersioningMode::Off,
            None,
            Some(empty),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(info.lock.is_none(), "空锁记录不应落字段");
    // SSE/SseC 回归面：锁不影响加密路径
    let key = crypto::SseCKey::from_key([9u8; 32]);
    let info = s
        .put_object_stream_with_meta(
            "alpha",
            "k2",
            stream_of(payload(1000)),
            None,
            Default::default(),
            None,
            VersioningMode::Off,
            Some(EncSpec::SseC(key.clone())),
            Some(lock("GOVERNANCE", now_plus(60), false)),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(info.encryption.is_some() && info.lock.is_some());
    let (_, got) = s.get_object_ext("alpha", "k2", Some(&key)).await.unwrap();
    assert_eq!(got.len(), 1000);
}

#[tokio::test]
async fn purge_and_overwrite_guards_block_locked_versions() {
    // B1/B2 复验补：整键硬删（控制台路径）/陈旧节点非版本化删与覆盖写——全链查锁 fail-closed
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let (_vid, _) = put_locked(&s, "k", Some(lock("COMPLIANCE", now_plus(3600), false))).await;

    // B1：硬删（store.delete_object = 控制台对象删除路径）
    assert!(matches!(
        s.delete_object("alpha", "k").await,
        Err(Error::ObjectLocked(_))
    ));
    // B2-a：陈旧节点非版本化整键删
    assert!(matches!(
        s.delete_object_versioned("alpha", "k", None, VersioningMode::Off)
            .await,
        Err(Error::ObjectLocked(_))
    ));
    // B2-b：陈旧节点非版本化覆盖写（recycle 会回收锁定版本）
    let e = s
        .put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(payload(1000)),
            None,
            Default::default(),
            None,
            VersioningMode::Off,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)), "{e:?}");
    // B2-c：MPU 非版本化 complete 同拒
    let up = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let e1 = s
        .upload_part(
            "alpha",
            "k",
            &up,
            1,
            stream_of(payload(5 * 1024 * 1024 + 1)),
        )
        .await
        .unwrap();
    let e = s
        .complete_multipart_upload_versioned(
            "alpha",
            "k",
            &up,
            &[(1, e1)],
            VersioningMode::Off,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)), "{e:?}");
    // 版本化追加写不受锁约束（锁=保留旧版本，不禁新版本）
    let (_, _) = put_locked(&s, "k", Some(lock("GOVERNANCE", now_plus(60), false))).await;
    assert!(s.delete_object("alpha", "k").await.is_err(), "仍有锁定版本");

    // N2：legal hold 开关——ON 挡删（含硬删）；OFF 后放行；空记录不残留
    let hold = ObjectLockRecord {
        mode: None,
        retain_until_nanos: None,
        legal_hold: true,
    };
    let (hvid, _) = put_locked(&s, "h", Some(hold)).await;
    assert!(matches!(
        s.delete_object("alpha", "h").await,
        Err(Error::ObjectLocked(_))
    ));
    s.set_object_legal_hold("alpha", "h", &hvid, false)
        .await
        .unwrap();
    // OFF 后 meta 不留空锁记录
    let m = s.read_meta("alpha", "h").await.unwrap().unwrap();
    assert!(m.latest().unwrap().lock.is_none(), "空锁记录不残留");
    assert!(
        s.delete_object("alpha", "h").await.is_ok(),
        "OFF 后硬删放行"
    );
    // ON 幂等重开 → 再拒
    let (h2, _) = put_locked(&s, "h2", None).await;
    s.set_object_legal_hold("alpha", "h2", &h2, true)
        .await
        .unwrap();
    assert!(matches!(
        s.delete_object_versioned("alpha", "h2", Some(&h2), VersioningMode::Enabled)
            .await,
        Err(Error::ObjectLocked(_))
    ));
    // 关闭后按版本删放行
    s.set_object_legal_hold("alpha", "h2", &h2, false)
        .await
        .unwrap();
    assert!(
        s.delete_object_versioned("alpha", "h2", Some(&h2), VersioningMode::Enabled)
            .await
            .is_ok()
    );
    // marker 版本不支持 legal hold；不存在的版本 NoSuchKey
    let out = s
        .delete_object_versioned("alpha", "k", None, VersioningMode::Enabled)
        .await
        .unwrap();
    let vid_marker = match out {
        DeleteOutcome::Marker { version_id } => version_id,
        _ => unreachable!(),
    };
    assert!(matches!(
        s.set_object_legal_hold("alpha", "k", &vid_marker, true)
            .await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        s.set_object_legal_hold("alpha", "k", "00000000-0000-0000-0000-000000000000", true)
            .await,
        Err(Error::NoSuchKey(_))
    ));

    // P3-c 复验补（判别力）：到期锁 → 整链守卫放行（硬删/非版本化覆盖写）。删掉
    // active_lock_reason 的 `until > now` 判定会让本段红（不再只是 fail-closed 长拒）。
    let (_vid, _) = put_locked(&s, "exp", Some(lock("COMPLIANCE", now_plus(-10), false))).await;
    assert!(
        s.delete_object("alpha", "exp").await.is_ok(),
        "到期 COMPLIANCE 硬删应放行"
    );
    let (_vid, _) = put_locked(&s, "exp2", Some(lock("GOVERNANCE", now_plus(-5), false))).await;
    assert!(
        s.delete_object_versioned("alpha", "exp2", None, VersioningMode::Off)
            .await
            .is_ok(),
        "到期 GOVERNANCE 非版本化删应放行（无需 bypass）"
    );
    // 到期 retention + hold ON：hold 仍拦（到期只解 retention）
    let mix = ObjectLockRecord {
        mode: Some("GOVERNANCE".into()),
        retain_until_nanos: Some(now_plus(-1)),
        legal_hold: true,
    };
    let (mvid, _) = put_locked(&s, "mix", Some(mix)).await;
    assert!(matches!(
        s.delete_object("alpha", "mix").await,
        Err(Error::ObjectLocked(_))
    ));
    s.set_object_legal_hold("alpha", "mix", &mvid, false)
        .await
        .unwrap();
    assert!(s.delete_object("alpha", "mix").await.is_ok());

    // P3-b 侧同步钉：get_legal_hold 元数据读（点名 / 无版本号 / marker / 无锁）
    let (gvid, _) = put_locked(&s, "gh", Some(lock("GOVERNANCE", now_plus(60), true))).await;
    assert!(s.get_legal_hold("alpha", "gh", Some(&gvid)).await.unwrap());
    assert!(
        s.get_legal_hold("alpha", "gh", None).await.unwrap(),
        "无版本号 = latest"
    );
    let (pvid, _) = put_locked(&s, "gp", None).await;
    assert!(
        !s.get_legal_hold("alpha", "gp", Some(&pvid)).await.unwrap(),
        "无锁 = false"
    );
    assert!(matches!(
        s.get_legal_hold("alpha", "k", Some(&vid_marker)).await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        s.get_legal_hold("alpha", "nope", None).await,
        Err(Error::NoSuchKey(_))
    ));
}

/// Suspended：覆盖写/无版本号删除都只动 null 版本——null 版本的锁必须生效。
#[tokio::test]
async fn suspended_write_and_delete_guard_locked_null_version() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let info = s
        .put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(payload(200_000)),
            None,
            Default::default(),
            None,
            VersioningMode::Suspended,
            None,
            Some(lock("GOVERNANCE", now_plus(3600), false)),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(info.version_id.as_deref(), Some("null"));

    let e = s
        .put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(payload(200_000)),
            None,
            Default::default(),
            None,
            VersioningMode::Suspended,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)), "{e:?}");

    let e = s
        .delete_object_versioned("alpha", "k", None, VersioningMode::Suspended)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::ObjectLocked(_)), "{e:?}");

    // GOVERNANCE bypass 放行（与点名删版本同口径）
    let out = s
        .delete_object_versioned_ext("alpha", "k", None, VersioningMode::Suspended, true)
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::Marker { .. }));
}
