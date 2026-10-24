//! CMK rewrap / 删除隔离的 store 级测试（单盘；集群面在 console 集成测覆盖到路由/权限）。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::crypto::EncSpec;
use crate::kms::{KeyProvider as _, KmsFileProvider};

/// 固定配置引用源：模拟 `buckets.default_encryption` 指向某 CMK。
struct FixedRefs(Vec<CmkBucketRef>);

#[async_trait::async_trait]
impl CmkConfigRefs for FixedRefs {
    async fn cmk_config_refs(&self, _cmk: &str) -> crate::error::Result<Vec<CmkBucketRef>> {
        Ok(self.0.clone())
    }
}

/// 前 `misses` 次返回空、之后返回引用：模拟「扫描后、隔离前」落库的桶默认配置。
struct LateRefs {
    misses: usize,
    calls: AtomicUsize,
    bucket: String,
}

#[async_trait::async_trait]
impl CmkConfigRefs for LateRefs {
    async fn cmk_config_refs(&self, _cmk: &str) -> crate::error::Result<Vec<CmkBucketRef>> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(if n < self.misses {
            Vec::new()
        } else {
            vec![CmkBucketRef {
                bucket: self.bucket.clone(),
                setting: "aws:kms:tenant-a".into(),
            }]
        })
    }
}

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
    enc_store_with(false).await
}

async fn enc_store_with(cluster_deployment: bool) -> (Arc<Store>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.cluster_deployment = cluster_deployment;
    let s = Store::open(cfg).await.unwrap();
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

    let r = s.delete_cmk("tenant-a", false, None).await.unwrap();
    assert!(!r.deleted);
    assert_eq!(r.reason.as_deref(), Some("references"));
    assert_eq!(r.references, 1);
    assert_eq!(r.refs[0].key_id, "cmk:tenant-a:k0");
    // force 只放宽集群扫描限制，绝不放宽「已知引用」fail-closed
    let r = s.delete_cmk("tenant-a", true, None).await.unwrap();
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
    let r = s.delete_cmk("tenant-a", false, None).await.unwrap();
    assert!(!r.deleted);
    assert_eq!(r.references, 1);
    assert_eq!(r.refs[0].key_id, "cmk:tenant-a:k1");

    // 对象删除 → 引用清零 → 删除成功
    assert!(s.delete_object("alpha", "k").await.unwrap());
    let r = s.delete_cmk("tenant-a", false, None).await.unwrap();
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
        s.delete_cmk("ghost", false, None).await,
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

/// 桶默认加密也是引用：删除必须拒（force 不例外），否则新写入会被 strand。
#[tokio::test]
async fn delete_cmk_refuses_bucket_default_configs() {
    let (s, _d) = enc_store().await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let refs = FixedRefs(vec![CmkBucketRef {
        bucket: "alpha".into(),
        setting: "aws:kms:tenant-a".into(),
    }]);
    for force in [false, true] {
        let r = s.delete_cmk("tenant-a", force, Some(&refs)).await.unwrap();
        assert!(!r.deleted, "桶默认仍是引用（force={force}）");
        assert_eq!(r.reason.as_deref(), Some("bucket_default"));
        assert_eq!(r.references, 0);
        assert_eq!(r.bucket_defaults, refs.0);
        assert!(!r.purged);
    }
    // 拒删不得隔离：CMK 仍可用，且没有 .cmk-trash 残骸
    assert_eq!(kms.ensure_cmk("tenant-a").unwrap(), 0);
    assert!(s.cmk_trash_list().unwrap().is_empty());
    // 配置引用清零后才允许删除
    let r = s
        .delete_cmk("tenant-a", false, Some(&FixedRefs(Vec::new())))
        .await
        .unwrap();
    assert!(r.deleted, "{r:?}");
    assert!(r.bucket_defaults.is_empty());
}

/// 隔离后复扫发现窗口内落库的桶默认配置 → race_detected 并原样恢复 CMK。
#[tokio::test]
async fn delete_cmk_restores_on_late_bucket_default_race() {
    let (s, _d) = enc_store().await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let refs = LateRefs {
        misses: 1,
        calls: AtomicUsize::new(0),
        bucket: "alpha".into(),
    };
    let r = s.delete_cmk("tenant-a", false, Some(&refs)).await.unwrap();
    assert_eq!(refs.calls.load(Ordering::SeqCst), 2, "隔离前/后各复扫一次");
    assert!(!r.deleted);
    assert_eq!(r.reason.as_deref(), Some("race_detected"));
    assert_eq!(r.bucket_defaults.len(), 1);
    assert_eq!(r.bucket_defaults[0].bucket, "alpha");
    // 恢复成功：无残骸，CMK 可继续加密新写入并读出
    assert!(s.cmk_trash_list().unwrap().is_empty());
    let info = put(&s, "k", payload(2048), VersioningMode::Off, cmk("tenant-a")).await;
    assert_eq!(info.encryption.as_ref().unwrap().key_id, "cmk:tenant-a:k0");
    assert_eq!(s.get_object("alpha", "k").await.unwrap().1, payload(2048));
}

/// `.cmk-trash` 清理：对象/桶默认引用都拒清且保留残骸；restore 安全回滚；
/// 引用清零后 purge 永久销毁。
#[tokio::test]
async fn trash_purge_guards_restore_and_cleanup() {
    let (s, _d) = enc_store().await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    put(&s, "k", payload(4096), VersioningMode::Off, cmk("tenant-a")).await;
    // 手工隔离：模拟删除中断/清理失败后的残骸
    kms.quarantine_cmk("tenant-a").await.unwrap();
    let trash = s.cmk_trash_list().unwrap();
    assert_eq!(trash.len(), 1);
    assert_eq!(trash[0].name, "tenant-a");
    assert_eq!(trash[0].versions, vec![0]);
    // 仍有对象引用 → 拒清且残骸原样保留
    let r = s.cmk_trash_purge("tenant-a", false, None).await.unwrap();
    assert!(!r.deleted);
    assert_eq!(r.reason.as_deref(), Some("references"));
    assert_eq!(r.references, 1);
    assert!(r.purged.is_empty());
    assert_eq!(s.cmk_trash_list().unwrap().len(), 1);
    // 对象引用清零、但桶默认仍指向该 CMK → 同样拒清
    assert!(s.delete_object("alpha", "k").await.unwrap());
    let refs = FixedRefs(vec![CmkBucketRef {
        bucket: "alpha".into(),
        setting: "aws:kms:tenant-a".into(),
    }]);
    let r = s
        .cmk_trash_purge("tenant-a", false, Some(&refs))
        .await
        .unwrap();
    assert!(!r.deleted);
    assert_eq!(r.reason.as_deref(), Some("bucket_default"));
    assert_eq!(s.cmk_trash_list().unwrap().len(), 1);
    // 安全恢复：唯一残骸原样回活动命名空间，可再次加密写入
    let e = s.cmk_trash_restore("tenant-a").await.unwrap();
    assert_eq!(e.versions, vec![0]);
    assert!(s.cmk_trash_list().unwrap().is_empty());
    put(&s, "k2", payload(16), VersioningMode::Off, cmk("tenant-a")).await;
    assert_eq!(s.get_object("alpha", "k2").await.unwrap().1, payload(16));
    // 引用全部清零 → 清理成功，钥材料永久消失（provider 不可再用）
    assert!(s.delete_object("alpha", "k2").await.unwrap());
    kms.quarantine_cmk("tenant-a").await.unwrap();
    let r = s.cmk_trash_purge("tenant-a", false, None).await.unwrap();
    assert!(r.deleted, "{r:?}");
    assert_eq!(r.purged.len(), 1);
    assert_eq!(r.purged[0].versions, vec![0]);
    assert!(s.cmk_trash_list().unwrap().is_empty());
    assert!(matches!(
        kms.wrap_dek_named(&crate::kms::new_dek(), Some("tenant-a"))
            .await,
        Err(Error::InvalidArgument(_))
    ));
}

/// 集群部署（含离线标记 `cluster_deployment`）：离线 rewrap 直接拒绝（无拓扑无法
/// 安全放置分片）；删除要求显式 force，force 不绕过已知引用且报告带警告。
#[tokio::test]
async fn cluster_deployment_requires_force_and_refuses_offline_rewrap() {
    let (s, _d) = enc_store_with(true).await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    put(&s, "k", payload(2048), VersioningMode::Off, cmk("tenant-a")).await;
    assert!(matches!(
        Arc::clone(&s)
            .rewrap_cmk(
                "tenant-a",
                &RewrapOptions {
                    apply: false,
                    limit: 10,
                    cursor: None,
                    bucket: None,
                },
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    assert!(s.delete_object("alpha", "k").await.unwrap());
    let r = s.delete_cmk("tenant-a", false, None).await.unwrap();
    assert!(!r.deleted);
    assert_eq!(r.reason.as_deref(), Some("cluster_requires_force"));
    let r = s.delete_cmk("tenant-a", true, None).await.unwrap();
    assert!(r.deleted, "{r:?}");
    assert!(r.purged);
    assert!(
        r.warning.as_deref().unwrap_or("").contains("force"),
        "{:?}",
        r.warning
    );
}

/// `.cmk-trash` 清理同样受集群 force 闸门约束；无残骸 → no_quarantine（受控拒绝）。
#[tokio::test]
async fn trash_purge_cluster_requires_force() {
    let (s, _d) = enc_store_with(true).await;
    let kms = kms(&s);
    kms.create_cmk("tenant-a").await.unwrap();
    kms.quarantine_cmk("tenant-a").await.unwrap();
    let r = s.cmk_trash_purge("tenant-a", false, None).await.unwrap();
    assert!(!r.deleted);
    assert_eq!(r.reason.as_deref(), Some("cluster_requires_force"));
    assert_eq!(s.cmk_trash_list().unwrap().len(), 1, "拒清不得动残骸");
    let r = s.cmk_trash_purge("tenant-a", true, None).await.unwrap();
    assert!(r.deleted, "{r:?}");
    assert!(s.cmk_trash_list().unwrap().is_empty());
    let r = s.cmk_trash_purge("tenant-a", true, None).await.unwrap();
    assert!(!r.deleted);
    assert_eq!(r.reason.as_deref(), Some("no_quarantine"));
}
