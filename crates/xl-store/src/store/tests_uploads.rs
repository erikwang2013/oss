//! ListMultipartUploads 内核：分页 / delimiter / 集群并集 / 暂存区残骸过滤。

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

async fn create(s: &Store, key: &str) -> String {
    s.create_multipart_upload("alpha", key, None, Default::default())
        .await
        .unwrap()
}

fn ids(r: &UploadListResult) -> Vec<(String, String)> {
    r.uploads
        .iter()
        .map(|u| (u.key.clone(), u.upload_id.clone()))
        .collect()
}

/// 基本列举：按 (key, upload_id) 有序；abort/complete 后消失；非暂存区目录不混入。
#[tokio::test]
async fn uploads_basic_abort_complete_and_residue() {
    let (s, dir) = store().await;
    let k1a = create(&s, "k1").await;
    let k1b = create(&s, "k1").await;
    let k2 = create(&s, "k2").await;
    // 崩溃残骸：mpu.meta 已改名（complete 途中）→ 不得列举
    let residue = create(&s, "res").await;
    let res_dir = layout::object_dir(dir.path(), "alpha", "res")
        .unwrap()
        .join(&residue);
    tokio::fs::rename(res_dir.join("mpu.meta"), res_dir.join("mpu.meta.done"))
        .await
        .unwrap();

    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(r.uploads.len(), 3);
    assert!(!r.is_truncated);
    assert!(r.next_key_marker.is_none() && r.next_upload_id_marker.is_none());
    assert!(r.common_prefixes.is_empty());
    assert_eq!(
        r.uploads.iter().map(|u| u.key.as_str()).collect::<Vec<_>>(),
        ["k1", "k1", "k2"]
    );
    assert!(r.uploads.iter().all(|u| u.initiated_nanos > 0));
    assert!(r.uploads.iter().all(|u| u.storage_class.is_none()));
    let mut k1_ids = vec![k1a.clone(), k1b.clone()];
    k1_ids.sort();
    assert_eq!(
        r.uploads
            .iter()
            .filter(|u| u.key == "k1")
            .map(|u| u.upload_id.clone())
            .collect::<Vec<_>>(),
        k1_ids
    );

    // abort 后不再出现
    s.abort_multipart_upload("alpha", "k1", &k1b).await.unwrap();
    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(r.uploads.len(), 2);
    assert!(!r.uploads.iter().any(|u| u.upload_id == k1b));

    // complete 后暂存区转正 → 不再出现；k2 仍在
    let etag = s
        .upload_part("alpha", "k1", &k1a, 1, stream_of(b"hello"))
        .await
        .unwrap();
    s.complete_multipart_upload("alpha", "k1", &k1a, &[(1, etag)])
        .await
        .unwrap();
    let r = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(ids(&r), vec![("k2".to_string(), k2)]);
}

/// 同键多 upload 的分页：NextKeyMarker/NextUploadIdMarker 续页无重无漏；
/// 单独 key_marker 排他、单独 upload_id_marker 被忽略。
#[tokio::test]
async fn uploads_pagination_same_key() {
    let (s, _d) = store().await;
    for _ in 0..3 {
        create(&s, "k").await;
    }
    let full = s
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    let ordered = ids(&full);
    assert_eq!(ordered.len(), 3);
    assert!(ordered.windows(2).all(|w| w[0].1 < w[1].1));

    let mut got = Vec::new();
    let mut req = UploadListRequest {
        max_uploads: Some(1),
        ..Default::default()
    };
    for _ in 0..10 {
        let p = s.list_multipart_uploads("alpha", &req).await.unwrap();
        assert!(p.uploads.len() <= 1);
        got.extend(ids(&p));
        if !p.is_truncated {
            break;
        }
        req.key_marker = p.next_key_marker;
        req.upload_id_marker = p.next_upload_id_marker;
        assert!(req.upload_id_marker.is_some(), "截断页末项必为 upload");
    }
    assert_eq!(got, ordered);

    // 只给 key_marker → 同键 upload 全部跳过
    let r = s
        .list_multipart_uploads(
            "alpha",
            &UploadListRequest {
                key_marker: Some("k".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(r.uploads.is_empty() && !r.is_truncated);

    // 单独 upload_id_marker（无 key_marker）被忽略
    let r = s
        .list_multipart_uploads(
            "alpha",
            &UploadListRequest {
                upload_id_marker: Some(ordered[0].1.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(ids(&r), ordered);
}

/// delimiter 折叠 / prefix / cp <= key_marker 跳过（分页边界不重复）。
#[tokio::test]
async fn uploads_delimiter_prefix_and_page_boundary() {
    let (s, _d) = store().await;
    for k in ["a/1", "a/2", "b", "c/d"] {
        create(&s, k).await;
    }
    let r = s
        .list_multipart_uploads(
            "alpha",
            &UploadListRequest {
                delimiter: Some("/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.common_prefixes, ["a/", "c/"]);
    assert_eq!(r.uploads.len(), 1);
    assert_eq!(r.uploads[0].key, "b");
    assert!(!r.is_truncated);

    // max=1：依次返回 a/（cp）、b（upload）、c/（cp），续页不重复已发 cp
    let mut req = UploadListRequest {
        delimiter: Some("/".into()),
        max_uploads: Some(1),
        ..Default::default()
    };
    let p1 = s.list_multipart_uploads("alpha", &req).await.unwrap();
    assert_eq!(p1.common_prefixes, ["a/"]);
    assert!(p1.is_truncated);
    assert_eq!(p1.next_key_marker.as_deref(), Some("a/"));
    assert!(p1.next_upload_id_marker.is_none());

    req.key_marker = p1.next_key_marker;
    req.upload_id_marker = p1.next_upload_id_marker;
    let p2 = s.list_multipart_uploads("alpha", &req).await.unwrap();
    assert_eq!(p2.uploads.len(), 1);
    assert_eq!(p2.uploads[0].key, "b");
    assert!(p2.common_prefixes.is_empty());
    assert!(p2.is_truncated);
    assert_eq!(p2.next_key_marker.as_deref(), Some("b"));

    req.key_marker = p2.next_key_marker;
    req.upload_id_marker = p2.next_upload_id_marker;
    let p3 = s.list_multipart_uploads("alpha", &req).await.unwrap();
    assert_eq!(p3.common_prefixes, ["c/"]);
    assert!(p3.uploads.is_empty());
    assert!(!p3.is_truncated);

    // prefix 限定
    let r = s
        .list_multipart_uploads(
            "alpha",
            &UploadListRequest {
                prefix: Some("a/".into()),
                delimiter: Some("/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.uploads.len(), 2);
    assert!(r.common_prefixes.is_empty());
}

/// cp 必须排在「字典序介于 cp 与生成键之间」的键之前：键 "a0" < 生成键 "a/zzz"，
/// 但 cp "a/" < "a0"。若先发 "a0"，续页按 `cp <= key_marker` 会永久丢掉 "a/"。
#[tokio::test]
async fn uploads_delimiter_prefix_sorts_before_interleaved_keys() {
    let (s, _d) = store().await;
    for k in ["a/zzz", "a0", "a1"] {
        create(&s, k).await;
    }
    let mut req = UploadListRequest {
        delimiter: Some("/".into()),
        max_uploads: Some(1),
        ..Default::default()
    };
    let mut got = Vec::new();
    for _ in 0..6 {
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
            (vec!["a0".to_string()], vec![]),
            (vec!["a1".to_string()], vec![]),
        ]
    );
}

/// max-uploads=0 → 空页不截断；>1000 → 夹到 1000。
#[tokio::test]
async fn uploads_max_zero_and_clamp() {
    let (s, _d) = store().await;
    // 1001 个同键 upload：max>1000 必须夹到 1000
    for _ in 0..1001 {
        create(&s, "k").await;
    }
    let r = s
        .list_multipart_uploads(
            "alpha",
            &UploadListRequest {
                max_uploads: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(r.uploads.is_empty() && r.common_prefixes.is_empty());
    assert!(!r.is_truncated);

    let r = s
        .list_multipart_uploads(
            "alpha",
            &UploadListRequest {
                max_uploads: Some(5000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.uploads.len(), 1000);
    assert!(r.is_truncated);
    assert!(r.next_upload_id_marker.is_some());
}

/// 空桶 → 空页不截断；不存在的桶 → NoSuchBucket。
#[tokio::test]
async fn uploads_empty_and_missing_bucket() {
    let (s, _d) = store().await;
    s.create_bucket("empty").await.unwrap();
    let r = s
        .list_multipart_uploads("empty", &Default::default())
        .await
        .unwrap();
    assert!(r.uploads.is_empty() && r.common_prefixes.is_empty());
    assert!(!r.is_truncated);
    let e = s
        .list_multipart_uploads("missing", &Default::default())
        .await
        .unwrap_err();
    assert!(matches!(e, Error::NoSuchBucket(b) if b == "missing"));
}

/// 集群：暂存区分散在多个节点，任一节点列举 = 全集群并集；分页跨节点仍连续。
#[tokio::test]
async fn uploads_cluster_union_and_pagination() {
    let c = super::tests::Cluster::new(4, 3).await;
    c.stores[0].create_bucket("alpha").await.unwrap();
    let a0 = c.stores[0]
        .create_multipart_upload("alpha", "a", None, Default::default())
        .await
        .unwrap();
    let b1 = c.stores[1]
        .create_multipart_upload("alpha", "b", None, Default::default())
        .await
        .unwrap();
    let a1 = c.stores[1]
        .create_multipart_upload("alpha", "a", None, Default::default())
        .await
        .unwrap();

    let full = c.stores[2]
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    let ordered = ids(&full);
    let mut expect = vec![
        ("a".to_string(), a0.clone()),
        ("a".to_string(), a1.clone()),
        ("b".to_string(), b1.clone()),
    ];
    expect.sort();
    assert_eq!(ordered, expect);
    assert!(!full.is_truncated);

    // 跨节点逐页走完（每次 limit=2，两路源各返回自身的 max+1）
    let mut got = Vec::new();
    let mut req = UploadListRequest {
        max_uploads: Some(1),
        ..Default::default()
    };
    for _ in 0..10 {
        let p = c.stores[1]
            .list_multipart_uploads("alpha", &req)
            .await
            .unwrap();
        got.extend(ids(&p));
        if !p.is_truncated {
            break;
        }
        req.key_marker = p.next_key_marker;
        req.upload_id_marker = p.next_upload_id_marker;
    }
    assert_eq!(got, ordered);

    // 远端 abort 后并集少一项
    c.stores[1]
        .abort_multipart_upload("alpha", "b", &b1)
        .await
        .unwrap();
    let r = c.stores[0]
        .list_multipart_uploads("alpha", &Default::default())
        .await
        .unwrap();
    assert_eq!(r.uploads.len(), 2);
    assert!(!r.uploads.iter().any(|u| u.upload_id == b1));
}
