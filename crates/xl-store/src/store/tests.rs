use super::*;
// 拆分后 tests 不再经 store.rs 蹭到 Digest trait（md5/sha2 的 new/finalize 需要）
use md5::Digest as _;

async fn store() -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    (s, dir)
}

#[tokio::test]
async fn open_creates_sys_dirs() {
    let (_, dir) = store().await;
    for sub in ["tmp", "trash", "locks"] {
        assert!(dir.path().join(layout::SYS_DIR).join(sub).is_dir());
    }
}

#[tokio::test]
async fn open_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    Store::open(StoreConfig::new(dir.path())).await.unwrap();
    Store::open(StoreConfig::new(dir.path())).await.unwrap();
}

#[tokio::test]
async fn create_and_list_buckets() {
    let (s, _d) = store().await;
    // 倒序创建：断言要证明的是「按名字排序」，不是创建顺序
    s.create_bucket("beta").await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let names: Vec<String> = s
        .list_buckets()
        .await
        .unwrap()
        .into_iter()
        .map(|b| b.name)
        .collect();
    assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
}

#[tokio::test]
async fn list_buckets_missing_root_is_empty() {
    // 盘根整个消失（挂载点没了/被带外清掉）：列举返回空，不报错
    let (s, dir) = store().await;
    std::fs::remove_dir_all(dir.path()).unwrap();
    assert!(s.list_buckets().await.unwrap().is_empty());
}

#[tokio::test]
async fn create_bucket_is_idempotent_error() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    assert!(matches!(
        s.create_bucket("alpha").await,
        Err(Error::BucketAlreadyExists(_))
    ));
}

#[tokio::test]
async fn create_bucket_rejects_bad_name() {
    let (s, _d) = store().await;
    assert!(matches!(
        s.create_bucket("A_B").await,
        Err(Error::InvalidBucketName(_))
    ));
}

#[tokio::test]
async fn delete_bucket_requires_empty() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    // Task 5 前没有 put_object：直接摆一个对象目录（含内容）来制造「非空」。
    let obj_dir = layout::object_dir(d.path(), "alpha", "k.txt").unwrap();
    tokio::fs::create_dir_all(&obj_dir).await.unwrap();
    tokio::fs::write(obj_dir.join("part.1"), b"hi")
        .await
        .unwrap();
    assert!(matches!(
        s.delete_bucket("alpha").await,
        Err(Error::BucketNotEmpty(_))
    ));
    tokio::fs::remove_dir_all(&obj_dir).await.unwrap();
    s.delete_bucket("alpha").await.unwrap();
    assert!(!s.bucket_exists("alpha").await.unwrap());
}

#[tokio::test]
async fn missing_bucket_is_reported() {
    let (s, _d) = store().await;
    // put_object 属 Task 5；ensure_bucket 是同一前置，先用 delete_bucket 验它。
    assert!(matches!(
        s.delete_bucket("nope").await,
        Err(Error::NoSuchBucket(_))
    ));
}

#[tokio::test]
async fn ensure_bucket_rejects_traversal() {
    // ensure_bucket 内的桶名校验是删桶/列举的收口点：`..` 若放行，delete_bucket 会去删盘根之外
    let (s, _d) = store().await;
    assert!(matches!(
        s.delete_bucket("..").await,
        Err(Error::InvalidBucketName(_))
    ));
    assert!(matches!(
        s.delete_bucket("A_B").await,
        Err(Error::InvalidBucketName(_))
    ));
}

// ---- Task 5: put_object 提交协议 ----

#[tokio::test]
async fn put_then_head() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let info = s
        .put_object(
            "alpha",
            "dir/hello.txt",
            bytes::Bytes::from_static(b"hello world"),
            Some("text/plain".into()),
        )
        .await
        .unwrap();
    assert_eq!(info.size, 11);
    // 单段 PUT 的 ETag 就是整份 MD5
    assert_eq!(info.etag, "5eb63bbbe01eeed093cb22bb8f5acdc3");
    assert_eq!(info.content_type.as_deref(), Some("text/plain"));

    let head = s.head_object("alpha", "dir/hello.txt").await.unwrap();
    assert_eq!(head.etag, info.etag);

    // 盘上数据校验：meta 记录的 part.uuid 目录里，part.1 必须就是写进去的字节
    let obj_dir = layout::object_dir(d.path(), "alpha", "dir/hello.txt").unwrap();
    let on_disk = XlMeta::decode(
        &tokio::fs::read(obj_dir.join(meta::META_FILE))
            .await
            .unwrap(),
    )
    .unwrap();
    let part = obj_dir
        .join(&on_disk.latest().unwrap().part.uuid)
        .join("part.1");
    assert_eq!(
        tokio::fs::read(&part).await.unwrap(),
        b"hello world".to_vec()
    );
}

#[tokio::test]
async fn put_with_fsync_disabled_still_commits() {
    // cfg.fsync=false 是提交路径上的另一条分支（测试快、生产必须开），必须同样可提交可读
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let info = s
        .put_object("alpha", "k", Bytes::from_static(b"no-fsync"), None)
        .await
        .unwrap();
    assert_eq!(info.size, 8);
    assert_eq!(s.head_object("alpha", "k").await.unwrap().size, 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_streams_without_buffering_whole_body() {
    // 握手式断言：第一块发出后，盘上 tmp/<uuid>/part.1 必须已含它的字节，之后才放行剩余块。
    // 若实现把整份 body 缓冲到流结束再写，这里的轮询必然等不到而失败——这才是真流式的判别点。
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let s = std::sync::Arc::new(s);
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes>>(1);
    let put = tokio::spawn({
        let s = std::sync::Arc::clone(&s);
        async move {
            s.put_object_stream("alpha", "big.bin", Box::pin(rx), None)
                .await
        }
    });

    let first = Bytes::from(vec![b'a'; 1024]);
    futures::SinkExt::send(&mut tx, Ok(first.clone()))
        .await
        .unwrap();
    let tmp_root = layout::sys_dir(d.path(), "tmp");
    let mut on_disk = false;
    for _ in 0..200 {
        if let Ok(mut entries) = tokio::fs::read_dir(&tmp_root).await {
            while let Ok(Some(e)) = entries.next_entry().await {
                if let Ok(bytes) = tokio::fs::read(e.path().join("part.1")).await
                    && bytes == first.as_ref()
                {
                    on_disk = true;
                    break;
                }
            }
        }
        if on_disk {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        on_disk,
        "第一块在 body 结束前没有落盘：实现把整份 body 缓冲了"
    );

    // 放行剩余块并收尾
    futures::SinkExt::send(&mut tx, Ok(Bytes::from(vec![b'a'; 1024])))
        .await
        .unwrap();
    futures::SinkExt::send(&mut tx, Ok(Bytes::from(vec![b'a'; 1024])))
        .await
        .unwrap();
    drop(tx);
    let info = put.await.unwrap().unwrap();
    assert_eq!(info.size, 3072);
}

#[tokio::test]
async fn overwrite_is_atomic_and_old_version_gone() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", bytes::Bytes::from_static(b"one"), None)
        .await
        .unwrap();
    s.put_object("alpha", "k", bytes::Bytes::from_static(b"two"), None)
        .await
        .unwrap();

    let info = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(info.size, 3);

    // 对象目录下应当只剩一个新版本目录 + xl.meta（老版本进了 trash）
    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    let mut names: Vec<String> = Vec::new();
    let mut entries = tokio::fs::read_dir(&obj_dir).await.unwrap();
    while let Some(e) = entries.next_entry().await.unwrap() {
        names.push(e.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    assert_eq!(
        names.len(),
        2,
        "对象目录下应只有 xl.meta 与一个新的版本目录: {names:?}"
    );
    assert!(names.iter().any(|n| n == "xl.meta"));
}

#[tokio::test]
async fn crash_between_steps_leaves_no_visible_object() {
    // 崩在 3/4 之间的现场：tmp 孤儿 + 一个已被 rename 进对象目录、但无 meta 引用的版本目录。
    // 读者必须仍看到旧版本（这里 k 已有提交版本），绝不能看到未提交的数据。
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let old = s
        .put_object("alpha", "k", Bytes::from_static(b"one"), None)
        .await
        .unwrap();

    let orphan = layout::sys_dir(d.path(), "tmp").join("orphan-uuid");
    tokio::fs::create_dir_all(&orphan).await.unwrap();
    tokio::fs::write(orphan.join("part.1"), b"junk")
        .await
        .unwrap();
    let dangling = layout::object_dir(d.path(), "alpha", "k")
        .unwrap()
        .join("dangling-uuid");
    tokio::fs::create_dir_all(&dangling).await.unwrap();
    tokio::fs::write(dangling.join("part.1"), b"uncommitted")
        .await
        .unwrap();

    let head = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(head.etag, old.etag);
    assert_eq!(head.size, 3);
}

#[tokio::test]
async fn concurrent_puts_to_same_key_serialize() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let s = std::sync::Arc::new(s);
    let mut handles = Vec::new();
    for i in 0..8u8 {
        let s = std::sync::Arc::clone(&s);
        handles.push(tokio::spawn(async move {
            s.put_object("alpha", "same", bytes::Bytes::from(vec![i; 64]), None)
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    // 必须能读出一份完整的、单一写入者的内容（不能是交错损坏）
    let info = s.head_object("alpha", "same").await.unwrap();
    assert_eq!(info.size, 64);

    let (info, body) = s.get_object("alpha", "same").await.unwrap();
    assert_eq!(info.size, 64);
    assert_eq!(body.len(), 64);
    // 读回内容必须是某个写入者一次写入的 64 字节（单一重复值），不能是拼接/交错/截断产生的碎片
    assert!(
        body.iter().all(|b| *b == body[0]),
        "读回内容不是单次完整写入: {body:?}"
    );
}

#[tokio::test]
async fn part_written_but_not_committed_is_invisible() {
    // 直接把一个完整版本目录摆进对象目录、但不写 xl.meta，
    // 读路径必须报 NoSuchKey（证明「只有 xl.meta 是提交点」）
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    tokio::fs::create_dir_all(obj_dir.join("dangling-uuid"))
        .await
        .unwrap();
    tokio::fs::write(obj_dir.join("dangling-uuid").join("part.1"), b"data")
        .await
        .unwrap();
    assert!(matches!(
        s.head_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
}

#[tokio::test]
async fn read_meta_rejects_mismatched_object_key() {
    // 内容可解码、但 object_key 与目录名不符的 xl.meta：读路径必须报 CorruptMeta，
    // 而不是把「张冠李戴」的元数据当成请求的键（错位会静默服务错误内容）
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    tokio::fs::create_dir_all(&obj_dir).await.unwrap();
    let m = XlMeta {
        object_key: "other".into(),
        versions: vec![],
    };
    tokio::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap())
        .await
        .unwrap();

    let err = s.head_object("alpha", "k").await.unwrap_err();
    assert!(matches!(err, Error::CorruptMeta(_)));
    let msg = err.to_string();
    assert!(msg.contains("\"k\"") && msg.contains("\"other\""), "{msg}");
}

// ---- Task 6: 读 / 删 / 列举 / 复制 ----

#[tokio::test]
async fn get_reads_back_exact_bytes() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", bytes::Bytes::from_static(b"payload"), None)
        .await
        .unwrap();
    let (info, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(bytes, bytes::Bytes::from_static(b"payload"));
    assert_eq!(info.etag, {
        let mut h = md5::Md5::new();
        md5::Digest::update(&mut h, b"payload");
        hex::encode(md5::Digest::finalize(h))
    });
}

#[tokio::test]
async fn get_missing_key() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    assert!(matches!(
        s.get_object("alpha", "nope").await,
        Err(Error::NoSuchKey(_))
    ));
}

#[tokio::test]
async fn get_dangling_meta_version_reports_no_such_key() {
    // 并发覆写窗口的现实形态：xl.meta 指向的版本目录已被移走（覆写者提交后把它扔进 trash）。
    // 读路径重读 meta 重试一次后必须报 NoSuchKey —— 不得 panic，也不得把裸 IO 错误抛给上层。
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    let m = XlMeta::decode(
        &tokio::fs::read(obj_dir.join(meta::META_FILE))
            .await
            .unwrap(),
    )
    .unwrap();
    tokio::fs::remove_dir_all(obj_dir.join(&m.latest().unwrap().part.uuid))
        .await
        .unwrap();

    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
}

#[tokio::test]
async fn list_with_prefix_and_delimiter() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    for k in [
        "a.txt",
        "docs/1.txt",
        "docs/2.txt",
        "docs/deep/3.txt",
        "z.txt",
    ] {
        s.put_object("alpha", k, bytes::Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }

    let all = s
        .list_objects("alpha", &ListRequest::default())
        .await
        .unwrap();
    assert_eq!(all.objects.len(), 5);
    assert_eq!(all.objects[0].key, "a.txt"); // 字典序
    assert!(!all.is_truncated);

    let prefixed = s
        .list_objects(
            "alpha",
            &ListRequest {
                prefix: Some("docs/".into()),
                delimiter: Some("/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(prefixed.objects.len(), 2);
    assert_eq!(prefixed.common_prefixes, vec!["docs/deep/".to_string()]);
}

#[tokio::test]
async fn list_pagination() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    for i in 0..5 {
        s.put_object(
            "alpha",
            &format!("k{i}"),
            bytes::Bytes::from_static(b"x"),
            None,
        )
        .await
        .unwrap();
    }
    let page1 = s
        .list_objects(
            "alpha",
            &ListRequest {
                max_keys: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page1.objects.len(), 2);
    assert!(page1.is_truncated);
    let token = page1.next_token.clone().unwrap();

    let page2 = s
        .list_objects(
            "alpha",
            &ListRequest {
                max_keys: Some(2),
                continuation_token: Some(token),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page2.objects[0].key, "k2");
}

#[tokio::test]
async fn copy_object_duplicates_content() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object(
        "alpha",
        "src",
        bytes::Bytes::from_static(b"copy me"),
        Some("text/plain".into()),
    )
    .await
    .unwrap();
    let info = s.copy_object("alpha", "src", "alpha", "dst").await.unwrap();
    assert_eq!(info.size, 7);
    let (_, bytes) = s.get_object("alpha", "dst").await.unwrap();
    assert_eq!(bytes, bytes::Bytes::from_static(b"copy me"));
}

// ---- Task 6 收尾轮：delete 语义 / 列举边界 / 桶级竞态 ----

#[tokio::test]
async fn delete_then_get_and_head_are_no_such_key() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    s.delete_object("alpha", "k").await.unwrap();
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
    assert!(matches!(
        s.head_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
}

#[tokio::test]
async fn delete_missing_key_is_ok_and_leaves_others() {
    // S3 语义：删不存在的键返回成功；且不得误伤桶内其他键
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "keep", Bytes::from_static(b"keep"), None)
        .await
        .unwrap();
    s.delete_object("alpha", "nope").await.unwrap();
    let (_, bytes) = s.get_object("alpha", "keep").await.unwrap();
    assert_eq!(bytes, Bytes::from_static(b"keep"));
}

#[tokio::test]
async fn delete_then_put_same_key_reads_back() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", Bytes::from_static(b"one"), None)
        .await
        .unwrap();
    s.delete_object("alpha", "k").await.unwrap();
    let info = s
        .put_object("alpha", "k", Bytes::from_static(b"two"), None)
        .await
        .unwrap();
    assert_eq!(info.size, 3);
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(bytes, Bytes::from_static(b"two"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_puts_and_deletes_never_observe_torn_state() {
    // put/delete 并发下读路径的不变式：每次读要么是某个写入者一次写入的完整 64 字节，
    // 要么 NoSuchKey —— 不得出现其他 Err（IO 错误），也不得读出碎片内容。
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let s = std::sync::Arc::new(s);
    let mut handles = Vec::new();
    for i in 0..8u8 {
        let s = std::sync::Arc::clone(&s);
        handles.push(tokio::spawn(async move {
            if i % 2 == 0 {
                s.put_object("alpha", "same", Bytes::from(vec![i; 64]), None)
                    .await
                    .map(|_| ())
            } else {
                s.delete_object("alpha", "same").await
            }
        }));
    }
    for _ in 0..200 {
        match s.get_object("alpha", "same").await {
            Ok((info, body)) => {
                assert_eq!(info.size, 64);
                assert_eq!(body.len(), 64);
                assert!(body.iter().all(|b| *b == body[0]), "碎片内容: {body:?}");
            }
            Err(Error::NoSuchKey(_)) => {}
            Err(e) => panic!("读路径不应出现其他错误: {e}"),
        }
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn list_delimiter_with_pagination_no_duplicate_prefix() {
    // 复现（评审）：max=1 时，已发过的 common prefix 若仍计入容量会被再次截断，
    // token 原地打转（下一页空页或重复 cp）。去重检查必须在容量判断之前。
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    for k in ["a", "b/x", "b/y"] {
        s.put_object("alpha", k, bytes::Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }
    let req = |token: Option<String>| ListRequest {
        max_keys: Some(1),
        delimiter: Some("/".into()),
        continuation_token: token,
        ..Default::default()
    };

    let page1 = s.list_objects("alpha", &req(None)).await.unwrap();
    assert_eq!(page1.objects.len(), 1);
    assert!(page1.common_prefixes.is_empty());
    assert!(page1.is_truncated);
    let token = page1.next_token.clone().unwrap();

    let page2 = s.list_objects("alpha", &req(Some(token))).await.unwrap();
    assert!(page2.objects.is_empty());
    assert_eq!(page2.common_prefixes, vec!["b/".to_string()]);
    assert!(
        !page2.is_truncated && page2.next_token.is_none(),
        "已发过的 cp 不应再触发截断（否则客户端拿同一个 token 空转）"
    );
}

#[tokio::test]
async fn list_max_keys_zero_is_empty_page() {
    // max-keys=0：空页且不截断（报截断会让客户端拿同一个 token 空转）
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    let r = s
        .list_objects(
            "alpha",
            &ListRequest {
                max_keys: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(r.objects.is_empty() && r.common_prefixes.is_empty());
    assert!(!r.is_truncated);
    assert!(r.next_token.is_none());
}

#[tokio::test]
async fn list_empty_delimiter_is_ignored() {
    // delimiter="" 视同未指定：否则 find("") 命中每个键的首字节，整份键都被当 common prefix
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    for k in ["a/b", "c"] {
        s.put_object("alpha", k, bytes::Bytes::from_static(b"x"), None)
            .await
            .unwrap();
    }
    let r = s
        .list_objects(
            "alpha",
            &ListRequest {
                delimiter: Some(String::new()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.objects.len(), 2);
    assert!(r.common_prefixes.is_empty());
}

#[tokio::test]
async fn list_skips_invalid_object_dir_names() {
    // 手工塞入桶目录的杂物目录（键重建为 `../..`）不得让整桶列举失败，只跳过该目录
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "real.txt", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    std::fs::create_dir(layout::bucket_dir(d.path(), "alpha").join("..__XLDIR__..")).unwrap();
    let r = s
        .list_objects("alpha", &ListRequest::default())
        .await
        .unwrap();
    let keys: Vec<&str> = r.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(keys, ["real.txt"]);
}

#[tokio::test]
async fn collect_keys_on_vanished_bucket_is_no_such_bucket() {
    // 桶被并发删除的窗口：read_dir 命中 NotFound 必须映射 NoSuchBucket，而不是裸 IO 错误
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    std::fs::remove_dir(layout::bucket_dir(d.path(), "alpha")).unwrap();
    assert!(matches!(
        s.collect_keys("alpha").await,
        Err(Error::NoSuchBucket(_))
    ));
}

// ---- M2a Task 1: x-amz-meta-* 用户元数据往返 ----

#[tokio::test]
async fn put_and_read_back_user_metadata() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let mut md = std::collections::BTreeMap::new();
    md.insert("foo".to_string(), "bar".to_string());
    let info = s
        .put_object_with_meta("alpha", "k", Bytes::from_static(b"x"), None, md.clone())
        .await
        .unwrap();
    assert_eq!(info.user_metadata, md);
    let head = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(head.user_metadata, md);
    // 覆写不带元数据 = 清空（S3 PUT 语义：metadata 全量替换，不是合并）
    let info2 = s
        .put_object_with_meta(
            "alpha",
            "k",
            Bytes::from_static(b"y"),
            None,
            Default::default(),
        )
        .await
        .unwrap();
    assert!(info2.user_metadata.is_empty());
    assert!(
        s.head_object("alpha", "k")
            .await
            .unwrap()
            .user_metadata
            .is_empty()
    );
}

#[tokio::test]
async fn copy_carries_user_metadata() {
    // copy 默认 metadata-directive=COPY：元数据随对象走
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let mut md = std::collections::BTreeMap::new();
    md.insert("k".to_string(), "v".to_string());
    s.put_object_with_meta("alpha", "src", Bytes::from_static(b"x"), None, md)
        .await
        .unwrap();
    s.copy_object("alpha", "src", "alpha", "dst").await.unwrap();
    assert_eq!(
        s.head_object("alpha", "dst")
            .await
            .unwrap()
            .user_metadata
            .get("k"),
        Some(&"v".to_string())
    );
}

// ---- M2a Task 2: Range 有界读 ----

#[tokio::test]
async fn get_object_range_reads_exact_slice() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", Bytes::from_static(b"0123456789"), None)
        .await
        .unwrap();
    // 半开区间 [2,5) -> "234"
    assert_eq!(
        &s.get_object_range("alpha", "k", 2..5).await.unwrap()[..],
        b"234"
    );
    assert_eq!(
        &s.get_object_range("alpha", "k", 0..10).await.unwrap()[..],
        b"0123456789"
    );
    // 空区间（start == end）合法，返回 0 字节
    assert!(
        s.get_object_range("alpha", "k", 4..4)
            .await
            .unwrap()
            .is_empty()
    );
    // 出界由调用方（s3s range.check）先拦；内核仍防呆：start >= size 或 end > size 或倒序 → Err
    assert!(s.get_object_range("alpha", "k", 10..11).await.is_err());
    assert!(s.get_object_range("alpha", "k", 0..11).await.is_err());
    // 倒序区间用结构体构造：字面量 `5..3` 会触发 clippy::reversed_empty_ranges（deny by default）
    let reversed = std::ops::Range { start: 5, end: 3 };
    assert!(s.get_object_range("alpha", "k", reversed).await.is_err());
}

// ---- M2a Task 4: 孤儿 / 垃圾回收 ----

use std::time::{Duration, SystemTime};

fn set_mtime(path: &std::path::Path, t: SystemTime) {
    std::fs::File::open(path).unwrap().set_modified(t).unwrap();
}

#[tokio::test]
async fn open_reaps_tmp_and_trash_transient() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", Bytes::from_static(b"one"), None)
        .await
        .unwrap();
    // 摆 tmp 孤儿与 trash 残留（含一个「活引用」的对照：活版本目录在对象目录里，不受影响）
    let tmp = layout::sys_dir(d.path(), "tmp").join("orphan");
    let trash = layout::sys_dir(d.path(), "trash").join("old");
    tokio::fs::create_dir_all(&tmp).await.unwrap();
    tokio::fs::write(tmp.join("part.1"), b"junk").await.unwrap();
    tokio::fs::create_dir_all(&trash).await.unwrap();

    let s2 = Store::open(StoreConfig::new(d.path())).await.unwrap(); // 重开 = 重启
    assert!(!tmp.exists() && !trash.exists(), "启动自检应清空 tmp/trash");
    assert_eq!(s2.head_object("alpha", "k").await.unwrap().size, 3); // 活数据不动
    drop(s);
}

#[tokio::test]
async fn reap_stale_removes_unreferenced_dirs_by_mtime() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", Bytes::from_static(b"one"), None)
        .await
        .unwrap();
    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();

    // ① 无引用旧目录（mtime 拨老）→ 删 ② 无引用新目录（模拟在途 MPU 暂存区）→ 留
    let stale = obj_dir.join("dangling-old");
    let fresh = obj_dir.join("mpu-in-flight");
    tokio::fs::create_dir_all(&stale).await.unwrap();
    tokio::fs::write(stale.join("part.1"), b"junk")
        .await
        .unwrap();
    tokio::fs::create_dir_all(&fresh).await.unwrap();
    set_mtime(&stale, SystemTime::now() - Duration::from_secs(8 * 86_400));

    let due = SystemTime::now() - Duration::from_secs(7 * 86_400);
    assert_eq!(s.reap_stale(due).await.unwrap(), 1);
    assert!(!stale.exists(), "过期的未引用目录应被删");
    assert!(fresh.exists(), "新鲜目录（可能在途 MPU）不得删");
    assert_eq!(
        s.head_object("alpha", "k").await.unwrap().size,
        3,
        "活版本不受影响"
    );
}

#[tokio::test]
async fn reap_stale_skips_object_with_corrupt_meta() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    let dangling = obj_dir.join("dangling-old");
    tokio::fs::create_dir_all(&dangling).await.unwrap();
    // 损坏的 xl.meta：内容不可解码
    tokio::fs::write(obj_dir.join(meta::META_FILE), b"\x01garbage")
        .await
        .unwrap();
    set_mtime(
        &dangling,
        SystemTime::now() - Duration::from_secs(8 * 86_400),
    );

    let due = SystemTime::now() - Duration::from_secs(7 * 86_400);
    assert_eq!(s.reap_stale(due).await.unwrap(), 0);
    assert!(
        dangling.exists(),
        "读 meta 失败的对象应整体跳过（保守不删）"
    );
}

// ---- M2a Task 5: Multipart Upload ----

fn bytes_stream(v: Vec<u8>) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(Bytes::from(v)) }))
}

#[tokio::test]
async fn multipart_single_small_part_completes() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let id = s
        .create_multipart_upload(
            "alpha",
            "big",
            Some("text/plain".into()),
            Default::default(),
        )
        .await
        .unwrap();
    let etag = s
        .upload_part("alpha", "big", &id, 1, bytes_stream(b"hello".to_vec()))
        .await
        .unwrap();
    assert_eq!(etag, "5d41402abc4b2a76b9719d911017c592"); // md5("hello")
    let info = s
        .complete_multipart_upload("alpha", "big", &id, &[(1, etag)])
        .await
        .unwrap();
    assert_eq!(info.size, 5);
    // AWS multipart ETag = hex(md5(逐段原始 md5 字节拼接)) + "-N"——单段也是「组合」值，
    // 不等于明文 md5（手算核对：md5(md5("hello").digest()) = 62109206...）
    assert_eq!(info.etag, "62109206880d38a4010a98e11243924a-1");
    assert_eq!(info.content_type.as_deref(), Some("text/plain"));

    let (head, body) = s.get_object("alpha", "big").await.unwrap();
    assert_eq!(head.size, 5);
    assert_eq!(&body[..], b"hello");
}

#[tokio::test]
async fn multipart_multi_part_etag_and_cross_part_range() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let id = s
        .create_multipart_upload("alpha", "multi", None, Default::default())
        .await
        .unwrap();

    // 5 MiB + 5 MiB + 1 MiB：非末段 ≥5 MiB；三段内容取不同值以便验证跨段边界
    let mib = 1024 * 1024;
    let e1 = s
        .upload_part("alpha", "multi", &id, 1, bytes_stream(vec![1u8; 5 * mib]))
        .await
        .unwrap();
    let e2 = s
        .upload_part("alpha", "multi", &id, 2, bytes_stream(vec![2u8; 5 * mib]))
        .await
        .unwrap();
    let e3 = s
        .upload_part("alpha", "multi", &id, 3, bytes_stream(vec![3u8; mib]))
        .await
        .unwrap();

    let info = s
        .complete_multipart_upload("alpha", "multi", &id, &[(1, e1), (2, e2), (3, e3)])
        .await
        .unwrap();
    assert_eq!(info.size, (11 * mib) as u64);
    assert!(info.etag.ends_with("-3"), "etag: {}", info.etag);

    // 跨 part 边界 Range：[5MiB-2, 5MiB+3) = [1,1,2,2,2]
    let r = s
        .get_object_range(
            "alpha",
            "multi",
            ((5 * mib - 2) as u64)..((5 * mib + 3) as u64),
        )
        .await
        .unwrap();
    assert_eq!(&r[..], &[1u8, 1, 2, 2, 2]);
    // 末段整读
    let last = s
        .get_object_range("alpha", "multi", (10 * mib as u64)..(11 * mib as u64))
        .await
        .unwrap();
    assert!(last.iter().all(|b| *b == 3), "末段应全来自 part3");

    // complete 之后 abort 不得删掉已转正的版本目录（mpu.meta 生死门）
    assert!(matches!(
        s.abort_multipart_upload("alpha", "multi", &id).await,
        Err(Error::NoSuchUpload(_))
    ));
    assert_eq!(
        s.head_object("alpha", "multi").await.unwrap().size,
        (11 * mib) as u64
    );
}

#[tokio::test]
async fn multipart_validation_errors() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let id = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let mib = 1024 * 1024;
    let e1 = s
        .upload_part("alpha", "k", &id, 1, bytes_stream(vec![1u8; 5 * mib]))
        .await
        .unwrap();
    let e2 = s
        .upload_part("alpha", "k", &id, 2, bytes_stream(vec![2u8; 1024]))
        .await
        .unwrap();

    // 乱序（非严格升序）
    assert!(matches!(
        s.complete_multipart_upload("alpha", "k", &id, &[(2, e2.clone()), (1, e1.clone())])
            .await,
        Err(Error::InvalidPartOrder(_))
    ));
    // ETag 不一致
    assert!(matches!(
        s.complete_multipart_upload("alpha", "k", &id, &[(1, "deadbeef".into())])
            .await,
        Err(Error::InvalidPart(_))
    ));
    // 中间段 <5 MiB（末段不受限：加第三段让 part2 成为「中间段」）
    let e3 = s
        .upload_part("alpha", "k", &id, 3, bytes_stream(vec![3u8; 1024]))
        .await
        .unwrap();
    assert!(matches!(
        s.complete_multipart_upload(
            "alpha",
            "k",
            &id,
            &[(1, e1.clone()), (2, e2.clone()), (3, e3)]
        )
        .await,
        Err(Error::EntityTooSmall(_))
    ));
    // 未上传的 part
    assert!(matches!(
        s.complete_multipart_upload("alpha", "k", &id, &[(9, "x".into())])
            .await,
        Err(Error::InvalidPart(_))
    ));
    // upload_id 含路径穿越 → 直接不让进目录（Uuid 解析拒）
    assert!(matches!(
        s.upload_part("alpha", "k", "../x", 1, bytes_stream(vec![]))
            .await,
        Err(Error::NoSuchUpload(_))
    ));
    // 不存在的 upload
    let missing = uuid::Uuid::new_v4().to_string();
    assert!(matches!(
        s.complete_multipart_upload("alpha", "k", &missing, &[])
            .await,
        Err(Error::NoSuchUpload(_))
    ));
}

#[tokio::test]
async fn multipart_abort_removes_staging_and_is_idempotent_error() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let id = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    s.upload_part("alpha", "k", &id, 1, bytes_stream(vec![1u8; 16]))
        .await
        .unwrap();
    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    assert!(obj_dir.join(&id).is_dir(), "暂存区应落在对象目录下");

    s.abort_multipart_upload("alpha", "k", &id).await.unwrap();
    assert!(!obj_dir.join(&id).exists(), "abort 应删掉整个暂存区");
    assert!(matches!(
        s.abort_multipart_upload("alpha", "k", &id).await,
        Err(Error::NoSuchUpload(_))
    ));
    // 未 complete：对象不可见
    assert!(matches!(
        s.head_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
}

#[tokio::test]
async fn multipart_list_parts_ascending() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let id = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    // 逆序上传 2、1：列举必须按 part 号升序
    s.upload_part("alpha", "k", &id, 2, bytes_stream(vec![2u8; 7]))
        .await
        .unwrap();
    s.upload_part("alpha", "k", &id, 1, bytes_stream(vec![1u8; 5]))
        .await
        .unwrap();
    let ps = s.list_parts("alpha", "k", &id).await.unwrap();
    assert_eq!(ps.len(), 2);
    assert_eq!(ps[0].part_number, 1);
    assert_eq!(ps[0].size, 5);
    assert_eq!(ps[1].part_number, 2);
    assert_eq!(ps[1].size, 7);
}

// ---- 协议补全 Task 3: ChecksumRecord 往返 ----

#[tokio::test]
async fn meta_roundtrips_checksum_record() {
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let rec = meta::ChecksumRecord {
        algorithm: "SHA256".into(),
        value_base64: "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=".into(),
    };
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        Box::pin(futures::stream::once(async {
            Ok(Bytes::from_static(b"payload-123"))
        })),
        None,
        Default::default(),
        Some(rec.clone()),
    )
    .await
    .unwrap();
    let head = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(head.checksum, Some(rec.clone()));
    let (get, _body) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(get.checksum, Some(rec));
}
