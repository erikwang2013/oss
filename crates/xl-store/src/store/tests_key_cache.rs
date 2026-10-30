//! 命名空间键缓存 + 在途 MPU 索引：加载/分页/增量维护/TTL 重建/上限回退/钩子。

use super::*;

const MANY: usize = 1_000_000;

async fn store_with(max_keys: usize, ttl_secs: u64) -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    // 显式覆写：测试不依赖进程环境
    cfg.key_cache_max_keys = max_keys;
    cfg.key_cache_ttl_secs = ttl_secs;
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    (s, dir)
}

async fn store() -> (Store, tempfile::TempDir) {
    store_with(MANY, 300).await
}

async fn put(s: &Store, key: &str) {
    s.put_object("alpha", key, Bytes::from_static(b"x"), None)
        .await
        .unwrap();
}

fn keys(r: &ListResult) -> Vec<String> {
    r.objects.iter().map(|o| o.key.clone()).collect()
}

fn stream_of(b: &'static [u8]) -> ByteStream {
    Box::pin(futures::stream::once(
        async move { Ok(Bytes::from_static(b)) },
    ))
}

fn ids(r: &UploadListResult) -> Vec<(String, String)> {
    r.uploads
        .iter()
        .map(|u| (u.key.clone(), u.upload_id.clone()))
        .collect()
}

/// 分页走完全部键（V2 token 续页），返回看到的键序列。
async fn list_all(s: &Store, max_keys: usize) -> Vec<String> {
    let mut req = ListRequest {
        max_keys: Some(max_keys),
        ..Default::default()
    };
    let mut out = Vec::new();
    for _ in 0..100 {
        let p = s.list_objects("alpha", &req).await.unwrap();
        out.extend(keys(&p));
        if !p.is_truncated {
            break;
        }
        req.continuation_token = p.next_token;
    }
    out
}

/// 缓存加载后的分页/prefix/delimiter/V1 marker 与 put/delete 增量。
#[tokio::test]
async fn cache_listing_pagination_prefix_delimiter_and_deletes() {
    let (s, _d) = store().await;
    for k in ["a.txt", "b.txt", "dir/c.txt", "dir/d.txt", "e.txt"] {
        put(&s, k).await;
    }
    // 首次列举触发懒加载；之后按批取键
    assert_eq!(
        list_all(&s, 2).await,
        ["a.txt", "b.txt", "dir/c.txt", "dir/d.txt", "e.txt"]
    );
    assert!(s.key_cache_contains("alpha", "dir/c.txt"));

    let r = s
        .list_objects(
            "alpha",
            &ListRequest {
                prefix: Some("dir/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(keys(&r), ["dir/c.txt", "dir/d.txt"]);

    let r = s
        .list_objects(
            "alpha",
            &ListRequest {
                delimiter: Some("/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(keys(&r), ["a.txt", "b.txt", "e.txt"]);
    assert_eq!(r.common_prefixes, ["dir/"]);

    // V1 marker 排他
    let r = s
        .list_objects(
            "alpha",
            &ListRequest {
                marker: Some("b.txt".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(keys(&r), ["dir/c.txt", "dir/d.txt", "e.txt"]);

    // delete 后的增量（forget 钩子）：缓存立即可见
    s.delete_object("alpha", "dir/c.txt").await.unwrap();
    assert!(!s.key_cache_contains("alpha", "dir/c.txt"));
    let r = s
        .list_objects(
            "alpha",
            &ListRequest {
                prefix: Some("dir/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(keys(&r), ["dir/d.txt"]);

    // put 后的增量（note 钩子）
    put(&s, "f.txt").await;
    assert!(s.key_cache_contains("alpha", "f.txt"));
}

/// 钩子覆盖 put/purge 与 mesh meta 写入/删除（服务端提交路径）。
#[tokio::test]
async fn cache_hooks_put_purge_and_mesh_meta() {
    let (s, d) = store().await;
    put(&s, "k1").await;
    // 加载缓存
    let _ = s.list_objects("alpha", &Default::default()).await.unwrap();
    assert!(s.key_cache_contains("alpha", "k1"));

    put(&s, "k2").await;
    assert!(s.key_cache_contains("alpha", "k2"));
    s.delete_object("alpha", "k2").await.unwrap();
    assert!(!s.key_cache_contains("alpha", "k2"));

    // 取 k1 的 meta 字节，模拟 mesh 服务端 prepare+commit（远端节点提交点）
    let meta_bytes = tokio::fs::read(
        layout::object_dir(d.path(), "alpha", "k1")
            .unwrap()
            .join(crate::meta::META_FILE),
    )
    .await
    .unwrap();
    s.mesh_delete_meta(crate::mesh::MetaRead {
        bucket: "alpha".into(),
        key: "k1".into(),
    })
    .await
    .unwrap();
    assert!(!s.key_cache_contains("alpha", "k1"));

    let mw = crate::mesh::MetaWrite {
        bucket: "alpha".into(),
        key: "k3".into(),
        meta: bytes::Bytes::from(meta_bytes),
    };
    s.mesh_prepare_meta(mw.clone()).await.unwrap();
    s.mesh_commit_meta(mw).await.unwrap();
    assert!(s.key_cache_contains("alpha", "k3"));
    s.mesh_delete_meta(crate::mesh::MetaRead {
        bucket: "alpha".into(),
        key: "k3".into(),
    })
    .await
    .unwrap();
    assert!(!s.key_cache_contains("alpha", "k3"));
}

/// 键数超上限 → 该桶不缓存，回退扫描仍给出正确（分页）页。
#[tokio::test]
async fn cache_cap_exceeded_falls_back_to_scan() {
    let (s, _d) = store_with(2, 300).await;
    for k in ["a", "b", "c", "d"] {
        put(&s, k).await;
    }
    assert_eq!(list_all(&s, 2).await, ["a", "b", "c", "d"]);
    // 桶被禁用：没有键集缓存
    assert!(!s.key_cache_contains("alpha", "a"));
    // 再来一页仍是扫描结果（不受上限影响）
    let r = s
        .list_objects(
            "alpha",
            &ListRequest {
                prefix: Some("c".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(keys(&r), ["c"]);
}

/// TTL 到点整桶重建：另一个 Store 实例（代表外部写/手工写）新增的键被拾取。
#[tokio::test]
async fn cache_ttl_reload_picks_up_external_key() {
    let (s1, dir) = store_with(MANY, 1).await;
    put(&s1, "k1").await;
    assert_eq!(list_all(&s1, 10).await, ["k1"]);
    assert!(s1.key_cache_contains("alpha", "k1"));

    let s2 = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    put(&s2, "k2").await;
    // TTL 内：advisory 缓存看不到外部写（最终一致窗口）
    assert_eq!(list_all(&s1, 10).await, ["k1"]);
    assert!(!s1.key_cache_contains("alpha", "k2"));

    s1.key_cache_expire("alpha");
    assert_eq!(list_all(&s1, 10).await, ["k1", "k2"]);
    assert!(s1.key_cache_contains("alpha", "k2"));
}

/// MPU 索引：首次列举构建后，create/abort/complete 增量维护。
#[tokio::test]
async fn mpu_index_tracks_lifecycle_after_load() {
    let (s, _d) = store().await;
    let a = s
        .create_multipart_upload("alpha", "a", None, Default::default())
        .await
        .unwrap();
    // 首次列举：索引从盘上暂存区构建（create 钩子此时尚未加载 → no-op）
    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(ids(&r), vec![("a".to_string(), a.clone())]);

    // 加载后新增
    let b = s
        .create_multipart_upload("alpha", "b", None, Default::default())
        .await
        .unwrap();
    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(
        ids(&r),
        vec![("a".to_string(), a.clone()), ("b".to_string(), b.clone())]
    );

    // 加载后 abort
    s.abort_multipart_upload("alpha", "a", &a).await.unwrap();
    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(ids(&r), vec![("b".to_string(), b.clone())]);

    // 加载后 complete
    let etag = s
        .upload_part("alpha", "b", &b, 1, stream_of(b"hello"))
        .await
        .unwrap();
    s.complete_multipart_upload("alpha", "b", &b, &[(1, etag)])
        .await
        .unwrap();
    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert!(r.uploads.is_empty() && r.common_prefixes.is_empty());
    assert!(!r.is_truncated);
}

/// MPU 索引 TTL 到点整桶重建：另一实例（代表外部 create/手工写）新增的 upload 被拾取。
#[tokio::test]
async fn mpu_index_ttl_reload_picks_up_external_upload() {
    let (s1, dir) = store().await;
    let a = s1
        .create_multipart_upload("alpha", "a", None, Default::default())
        .await
        .unwrap();
    // 首次列举：索引从盘上暂存区构建
    let r = s1
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(ids(&r), vec![("a".to_string(), a.clone())]);

    let s2 = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    let b = s2
        .create_multipart_upload("alpha", "b", None, Default::default())
        .await
        .unwrap();
    // TTL 内：advisory 索引看不到外部 create（最终一致窗口）
    let r = s1
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(ids(&r), vec![("a".to_string(), a.clone())]);

    s1.mpu_index_expire("alpha");
    let r = s1
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(
        ids(&r),
        vec![("a".to_string(), a), ("b".to_string(), b)],
        "TTL 重建应拾取外部新增 upload"
    );
}

/// 索引增量后的 delimiter/分页边界：cp 先于生成键、续页不重复。
#[tokio::test]
async fn mpu_index_delimiter_and_page_boundaries_after_incremental_adds() {
    let (s, _d) = store().await;
    // 先加载一次空索引，后续 create 全走增量
    let empty = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert!(empty.uploads.is_empty());
    for k in ["a/1", "a/2", "b", "c/d"] {
        s.create_multipart_upload("alpha", k, None, Default::default())
            .await
            .unwrap();
    }

    let mut req = UploadListRequest {
        delimiter: Some("/".into()),
        max_uploads: Some(1),
        ..Default::default()
    };
    let mut got = Vec::new();
    for _ in 0..10 {
        let p = s.list_multipart_uploads("alpha", &req).await.unwrap();
        got.push((
            p.uploads.iter().map(|u| u.key.clone()).collect::<Vec<_>>(),
            p.common_prefixes.clone(),
        ));
        if !p.is_truncated {
            break;
        }
        req.key_marker = p.next_key_marker;
        req.upload_id_marker = p.next_upload_id_marker;
    }
    assert_eq!(
        got,
        vec![
            (vec![], vec!["a/".to_string()]),
            (vec!["b".to_string()], vec![]),
            (vec![], vec!["c/".to_string()]),
        ]
    );
}

/// `clean` 按 mtime 回收暂存区后，LMU 索引同步移除（不再列举已回收 upload）。
#[tokio::test]
async fn clean_removes_stale_upload_from_index() {
    let (s, _d) = store().await;
    let _ = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(r.uploads.len(), 1, "先加载索引");
    // due 设在未来：未引用暂存区全部视为过期
    let removed = s
        .reap_stale(std::time::SystemTime::now() + std::time::Duration::from_secs(3600))
        .await
        .unwrap();
    assert!(removed >= 1);
    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert!(r.uploads.is_empty(), "回收后索引不得残留");
}

/// 删桶后键缓存/MPU 索引失效（同名桶重建不继承旧键）。
#[tokio::test]
async fn delete_bucket_invalidates_caches() {
    let (s, _d) = store().await;
    put(&s, "k").await;
    let _ = s.list_objects("alpha", &Default::default()).await.unwrap();
    assert!(s.key_cache_contains("alpha", "k"));
    s.delete_object("alpha", "k").await.unwrap();
    s.delete_bucket("alpha").await.unwrap();
    assert!(!s.key_cache_contains("alpha", "k"));

    s.create_bucket("alpha").await.unwrap();
    let r = s.list_objects("alpha", &Default::default()).await.unwrap();
    assert!(r.objects.is_empty());
}

/// 全进程总量上限：后加载的桶超限时禁用缓存并回退扫描，已缓存桶保留。
#[tokio::test]
async fn cache_total_cap_disables_late_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.key_cache_max_keys = 1000;
    cfg.key_cache_max_total_keys = 3;
    cfg.key_cache_ttl_secs = 300;
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    s.create_bucket("beta").await.unwrap();
    for k in ["a", "b"] {
        s.put_object("alpha", k, Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }
    let _ = s.list_objects("alpha", &Default::default()).await.unwrap();
    assert!(s.key_cache_contains("alpha", "a"));

    for k in ["c", "d"] {
        s.put_object("beta", k, Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }
    let p = s.list_objects("beta", &Default::default()).await.unwrap();
    assert_eq!(
        p.objects.iter().map(|o| o.key.clone()).collect::<Vec<_>>(),
        ["c", "d"]
    );
    assert!(!s.key_cache_contains("beta", "c"), "超总上限应禁用该桶缓存");
    assert!(s.key_cache_contains("alpha", "a"), "已缓存桶不受影响");
}
