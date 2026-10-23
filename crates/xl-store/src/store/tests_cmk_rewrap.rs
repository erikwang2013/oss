//! CMK rewrap / 删除隔离的 store 级测试（单盘；集群面在 console 集成测覆盖到路由/权限）。

use std::sync::Arc;

use super::*;
use crate::crypto::EncSpec;
use crate::kms::{KeyProvider as _, KmsFileProvider};

fn payload(n: usize) -> Bytes {
    Bytes::from(
        (0..n)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect::<Vec<u8>>(),
    )
}

fn stream_of(b: Bytes) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(b) }))
}

async fn enc_store() -> (Arc<Store>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    let sys = layout::sys_dir(dir.path(), "");
    s.init_encryption(Some(sys.join("master.key")), Some(sys.join("kms")))
        .await
        .unwrap();
    (Arc::new(s), dir)
}

fn kms(s: &Store) -> Arc<KmsFileProvider> {
    s.enc_state().unwrap().kms.clone().unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn put(
    s: &Store,
    key: &str,
    body: Bytes,
    mode: VersioningMode,
    enc: Option<EncSpec>,
) -> ObjectInfo {
    s.put_object_stream_with_meta(
        "alpha",
        key,
        stream_of(body),
        None,
        Default::default(),
        None,
        mode,
        enc,
        None,
        None,
        None,
    )
    .await
    .unwrap()
}

fn cmk(name: &str) -> Option<EncSpec> {
    Some(EncSpec::SseKmsCmk(name.to_string()))
}

/// 预览只读；apply 逐版本重加密（inline + EC 两条形态），版本身份/元数据/字节全保留；
/// 非目标加密形态不动。
#[tokio::test]
async fn rewrap_old_versions_preserves_identity_and_readability() {
    let (s, _d) = enc_store().await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let big = payload(300_000); // EC 形态
    let inline = payload(120_000); // < MAX_INLINE，内联形态
    let v1 = put(
        &s,
        "k",
        big.clone(),
        VersioningMode::Enabled,
        cmk("tenant-a"),
    )
    .await;
    let mut meta1: std::collections::BTreeMap<String, String> = Default::default();
    meta1.insert("who".to_string(), "v1".to_string());
    // 同键第二版本（真版本链）：带 user metadata/标签，验证 rewrap 不丢
    let v2 = s
        .put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(inline.clone()),
            Some("text/plain".into()),
            meta1,
            None,
            VersioningMode::Enabled,
            cmk("tenant-a"),
            None,
            Some(vec![meta::ObjectTag {
                key: "env".into(),
                value: "test".into(),
            }]),
            None,
        )
        .await
        .unwrap();
    // 非目标：未加密 / 默认 KMS 环——rewrap 不得触碰
    let plain = put(&s, "plain", payload(20_000), VersioningMode::Off, None).await;
    let dflt = put(
        &s,
        "dflt",
        payload(20_000),
        VersioningMode::Off,
        Some(EncSpec::SseKms),
    )
    .await;
    kms.rotate_cmk("tenant-a").await.unwrap();

    let before = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let preview = Arc::clone(&s)
        .rewrap_cmk(
            "tenant-a",
            &RewrapOptions {
                apply: false,
                limit: 100,
                cursor: None,
                bucket: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(preview.candidates, 2, "两个旧版本各算一条");
    assert_eq!(preview.rewrapped, 0, "预览绝不写");
    assert!(preview.items.iter().all(|i| i.status == "preview"));
    assert_eq!(
        s.read_meta("alpha", "k").await.unwrap().unwrap(),
        before,
        "dry-run 后 meta 不变"
    );

    let applied = Arc::clone(&s)
        .rewrap_cmk(
            "tenant-a",
            &RewrapOptions {
                apply: true,
                limit: 100,
                cursor: None,
                bucket: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(applied.rewrapped, 2);
    assert_eq!(applied.failed, 0);
    assert!(
        applied
            .items
            .iter()
            .all(|i| i.new_key_id.as_deref() == Some("cmk:tenant-a:k1"))
    );

    let after = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert_eq!(after.versions.len(), 2);
    for old in &before.versions {
        let new = after.find_version(&old.version_id_str()).unwrap();
        assert_eq!(new.version_id, old.version_id, "version id 保留");
        assert_eq!(new.size, old.size);
        assert_eq!(new.etag, old.etag, "ETag 保留");
        assert_eq!(new.mod_time_nanos, old.mod_time_nanos, "Last-Modified 保留");
        assert_eq!(new.part.sha256, old.part.sha256);
        assert_eq!(new.user_metadata, old.user_metadata);
        assert_eq!(new.tags, old.tags);
        assert_eq!(new.content_type, old.content_type);
        assert_eq!(new.encryption.as_ref().unwrap().key_id, "cmk:tenant-a:k1");
        assert_ne!(
            new.part.uuid, old.part.uuid,
            "数据目录必须换新（AAD 绑定 key_id）"
        );
    }
    let (_, got1) = s
        .get_object_version("alpha", "k", &v1.version_id.clone().unwrap())
        .await
        .unwrap();
    assert_eq!(got1, big, "旧版本重加密后字节不变");
    let (_, got2) = s
        .get_object_version("alpha", "k", &v2.version_id.clone().unwrap())
        .await
        .unwrap();
    assert_eq!(got2, inline);
    // 非目标对象原样可读，加密记录未被改写
    assert_eq!(
        s.get_object("alpha", "plain").await.unwrap().1,
        payload(20_000)
    );
    assert_eq!(
        s.get_object("alpha", "dflt").await.unwrap().1,
        payload(20_000)
    );
    let dnow = s.read_meta("alpha", "dflt").await.unwrap().unwrap();
    assert_eq!(
        dnow.versions[0].encryption.as_ref().unwrap().key_id,
        dflt.encryption.unwrap().key_id
    );
    // 再跑一次：无候选（幂等）
    let again = Arc::clone(&s)
        .rewrap_cmk(
            "tenant-a",
            &RewrapOptions {
                apply: true,
                limit: 100,
                cursor: None,
                bucket: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(again.candidates, 0);
    assert_eq!(plain.size, 20_000);
}

/// 删除 fail-closed：任何版本引用（含当前版本）→ 拒；force 不能绕过已知引用；
/// 引用清零后隔离销毁，同名可重建。
#[tokio::test]
async fn delete_cmk_refuses_references_then_purges() {
    let (s, _d) = enc_store().await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    put(
        &s,
        "k",
        payload(300_000),
        VersioningMode::Off,
        cmk("tenant-a"),
    )
    .await;
    kms.rotate_cmk("tenant-a").await.unwrap();

    let r = s.delete_cmk("tenant-a", false).await.unwrap();
    assert!(!r.deleted);
    assert_eq!(r.reason.as_deref(), Some("references"));
    assert_eq!(r.references, 1);
    assert_eq!(r.refs[0].key_id, "cmk:tenant-a:k0");
    // force 只放宽集群扫描限制，绝不放宽「已知引用」fail-closed
    let r = s.delete_cmk("tenant-a", true).await.unwrap();
    assert!(!r.deleted, "force 不得删除仍有引用的 CMK");
    assert_eq!(r.references, 1);

    // rewrap 到当前版本后仍是同一 CMK 的引用 → 仍拒
    let rep = Arc::clone(&s)
        .rewrap_cmk(
            "tenant-a",
            &RewrapOptions {
                apply: true,
                limit: 10,
                cursor: None,
                bucket: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(rep.rewrapped, 1);
    let r = s.delete_cmk("tenant-a", false).await.unwrap();
    assert!(!r.deleted);
    assert_eq!(r.references, 1);
    assert_eq!(r.refs[0].key_id, "cmk:tenant-a:k1");

    // 对象删除 → 引用清零 → 删除成功
    assert!(s.delete_object("alpha", "k").await.unwrap());
    let r = s.delete_cmk("tenant-a", false).await.unwrap();
    assert!(r.deleted, "无引用应删除：{r:?}");
    assert!(r.purged);
    assert_eq!(r.references, 0);
    assert!(kms.list_cmks().unwrap().is_empty());
    assert!(
        !layout::sys_dir(s.cfg.root(), "kms")
            .join("cmk/tenant-a")
            .exists(),
        "活动命名空间不得残留"
    );
    // 删除后 provider 不可再用；同名可重新创建（隔离目录已清）
    assert!(matches!(
        kms.wrap_dek_named(&crate::kms::new_dek(), Some("tenant-a"))
            .await,
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(kms.create_cmk("tenant-a").await.unwrap(), 0);
    // 未知 CMK / 未知桶：客户端可纠正
    assert!(matches!(
        s.delete_cmk("ghost", false).await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        Arc::clone(&s)
            .rewrap_cmk(
                "tenant-a",
                &RewrapOptions {
                    apply: false,
                    limit: 10,
                    cursor: None,
                    bucket: Some("nope".into()),
                },
            )
            .await,
        Err(Error::NoSuchBucket(_))
    ));
}

/// 超过 READ_CHUNK（8 MiB）的大对象：逐块读旧密文/写新密文的 streaming 路径，
/// 重加密后字节与校验仍必须一致。
#[tokio::test]
async fn rewrap_streams_large_object_across_chunks() {
    let (s, _d) = enc_store().await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let big = payload(8 * 1024 * 1024 + 4096);
    put(&s, "big", big.clone(), VersioningMode::Off, cmk("tenant-a")).await;
    kms.rotate_cmk("tenant-a").await.unwrap();
    let rep = Arc::clone(&s)
        .rewrap_cmk(
            "tenant-a",
            &RewrapOptions {
                apply: true,
                limit: 10,
                cursor: None,
                bucket: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(rep.rewrapped, 1);
    assert_eq!(rep.failed, 0, "{:?}", rep.items);
    let (_, got) = s.get_object("alpha", "big").await.unwrap();
    assert_eq!(got, big);
}

/// 有界批处理 + cursor 续页：预览与 apply 都能跨页且不漏项。
#[tokio::test]
async fn rewrap_batches_with_cursor() {
    let (s, _d) = enc_store().await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    for key in ["a", "b", "c"] {
        put(&s, key, payload(1000), VersioningMode::Off, cmk("tenant-a")).await;
    }
    kms.rotate_cmk("tenant-a").await.unwrap();
    let run = |apply: bool, limit: usize, cursor: Option<String>| {
        let s = Arc::clone(&s);
        async move {
            s.rewrap_cmk(
                "tenant-a",
                &RewrapOptions {
                    apply,
                    limit,
                    cursor,
                    bucket: None,
                },
            )
            .await
            .unwrap()
        }
    };
    let p1 = run(false, 2, None).await;
    assert_eq!(p1.candidates, 2);
    assert!(p1.truncated);
    let cur = p1.next_cursor.clone().expect("续页 cursor");
    let p2 = run(false, 2, Some(cur.clone())).await;
    assert_eq!(p2.candidates, 1);
    assert!(!p2.truncated);
    assert_eq!(p2.next_cursor, None);
    let keys: Vec<_> = p1
        .items
        .iter()
        .chain(&p2.items)
        .map(|i| i.key.as_str())
        .collect();
    assert_eq!(keys, vec!["a", "b", "c"]);

    let a1 = run(true, 2, None).await;
    assert_eq!(a1.rewrapped, 2);
    assert!(a1.truncated);
    let a2 = run(true, 2, a1.next_cursor.clone()).await;
    assert_eq!(a2.rewrapped, 1);
    assert!(!a2.truncated);
    let left = run(false, 10, None).await;
    assert_eq!(left.candidates, 0);
    for key in ["a", "b", "c"] {
        assert_eq!(s.get_object("alpha", key).await.unwrap().1, payload(1000));
    }
    // limit 边界值
    let bad = Arc::clone(&s)
        .rewrap_cmk(
            "tenant-a",
            &RewrapOptions {
                apply: false,
                limit: 0,
                cursor: None,
                bucket: None,
            },
        )
        .await;
    assert!(matches!(bad, Err(Error::InvalidArgument(_))));
}
