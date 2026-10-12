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

    // 盘上形态校验：11 字节 < inline_threshold → 内联进 meta，版本目录只有 xl.meta，无 part 文件
    let obj_dir = layout::object_dir(d.path(), "alpha", "dir/hello.txt").unwrap();
    let on_disk = XlMeta::decode(
        &tokio::fs::read(obj_dir.join(meta::META_FILE))
            .await
            .unwrap(),
    )
    .unwrap();
    let v = on_disk.latest().unwrap();
    assert_eq!(v.inline.as_ref().unwrap().bytes.as_slice(), b"hello world");
    let vdir = obj_dir.join(&v.part.uuid);
    assert_eq!(shard_files(&vdir), 0);
    assert!(!vdir.join("part.1").exists());
}

// ---- M2b：EC 与内联形态（Task 4/5） ----

/// 版本目录下 `part.*` 数据文件计数（内联 = 0，EC = data+parity，旧形态 = 1）
fn shard_files(version_dir: &std::path::Path) -> usize {
    std::fs::read_dir(version_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("part."))
        .count()
}

/// 低可压缩性的确定性伪随机字节（全零数据会掩盖编码错位）
fn payload(n: usize) -> Bytes {
    Bytes::from(
        (0..n)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect::<Vec<u8>>(),
    )
}

async fn read_disk_meta(s: &Store, bucket: &str, key: &str) -> XlMeta {
    s.read_meta(bucket, key).await.unwrap().unwrap()
}

#[tokio::test]
async fn ec_large_object_writes_twelve_shards_and_roundtrips() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let data = payload(1 << 20);
    let info = s
        .put_object("alpha", "big", data.clone(), None)
        .await
        .unwrap();
    assert_eq!(info.size, 1 << 20);

    let m = read_disk_meta(&s, "alpha", "big").await;
    let v = m.latest().unwrap();
    assert!(v.inline.is_none());
    let l = v.data_layout.as_ref().unwrap();
    assert_eq!((l.data, l.parity), (8, 4));
    assert_eq!(l.shards.len(), 12);
    let vdir = layout::object_dir(d.path(), "alpha", "big")
        .unwrap()
        .join(&v.part.uuid);
    assert_eq!(shard_files(&vdir), 12);
    assert!(!vdir.join("part.1").exists(), "转化后不得残留 part.1");

    // 读回一致 + EC 对象上的 Range（整份重建后切片）
    let (got, bytes) = s.get_object("alpha", "big").await.unwrap();
    assert_eq!(got.size, 1 << 20);
    assert_eq!(&bytes[..], &data[..]);
    let r = s
        .get_object_range("alpha", "big", 1_048_000..1_048_500)
        .await
        .unwrap();
    assert_eq!(&r[..], &data[1_048_000..1_048_500]);
}

#[tokio::test]
async fn inline_threshold_boundary() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();

    // 127 KiB < 128 KiB 阈值 → 内联：版本目录零数据文件
    let small = payload(127 * 1024);
    s.put_object("alpha", "small", small.clone(), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "small").await;
    let v = m.latest().unwrap();
    assert_eq!(v.inline.as_ref().unwrap().bytes.as_slice(), &small[..]);
    assert!(v.data_layout.is_none());
    let vdir = layout::object_dir(d.path(), "alpha", "small")
        .unwrap()
        .join(&v.part.uuid);
    assert_eq!(shard_files(&vdir), 0);
    let (_, bytes) = s.get_object("alpha", "small").await.unwrap();
    assert_eq!(&bytes[..], &small[..]);
    let r = s
        .get_object_range("alpha", "small", 100..200)
        .await
        .unwrap();
    assert_eq!(&r[..], &small[100..200]);

    // 129 KiB > 阈值 → EC
    let big = payload(129 * 1024);
    s.put_object("alpha", "big", big.clone(), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "big").await;
    let v = m.latest().unwrap();
    assert!(v.inline.is_none());
    assert_eq!(v.data_layout.as_ref().unwrap().shards.len(), 12);
    let (_, bytes) = s.get_object("alpha", "big").await.unwrap();
    assert_eq!(&bytes[..], &big[..]);

    // 形态切换：同键覆写内联 → EC → 内联，读到的始终是最新形态，旧版本进 trash
    let mid = payload(200 * 1024);
    s.put_object("alpha", "small", mid.clone(), None)
        .await
        .unwrap();
    let (_, bytes) = s.get_object("alpha", "small").await.unwrap();
    assert_eq!(&bytes[..], &mid[..]);
    s.put_object("alpha", "small", small.clone(), None)
        .await
        .unwrap();
    let (_, bytes) = s.get_object("alpha", "small").await.unwrap();
    assert_eq!(&bytes[..], &small[..]);
    assert_eq!(
        read_disk_meta(&s, "alpha", "small")
            .await
            .latest()
            .unwrap()
            .inline
            .as_ref()
            .unwrap()
            .bytes
            .as_slice(),
        &small[..]
    );
}

#[tokio::test]
async fn ec_read_survives_shard_loss_and_corruption() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();

    // 丢 4 片（= parity 上限）仍可重建
    let data = payload(300_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();
    let vdir = layout::object_dir(d.path(), "alpha", "k").unwrap().join(
        &read_disk_meta(&s, "alpha", "k")
            .await
            .latest()
            .unwrap()
            .part
            .uuid,
    );
    for i in [0, 3, 7, 11] {
        std::fs::remove_file(vdir.join(format!("part.1.{i}"))).unwrap();
    }
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&bytes[..], &data[..], "丢 4 片必须能原样重建");

    // 坏片（翻转一字节）被 CRC32C 抓到 → 视同缺失，同样可重建
    let data2 = payload(200_000);
    s.put_object("alpha", "flip", data2.clone(), None)
        .await
        .unwrap();
    let vdir2 = layout::object_dir(d.path(), "alpha", "flip").unwrap().join(
        &read_disk_meta(&s, "alpha", "flip")
            .await
            .latest()
            .unwrap()
            .part
            .uuid,
    );
    let spath = vdir2.join("part.1.2");
    let mut shard = std::fs::read(&spath).unwrap();
    shard[0] ^= 0x01;
    std::fs::write(&spath, &shard).unwrap();
    let (_, bytes) = s.get_object("alpha", "flip").await.unwrap();
    assert_eq!(&bytes[..], &data2[..], "坏片必须被 CRC 拦下并重建");

    // 丢 5 片（> parity）：fail-closed 报损坏，不吐坏数据
    let data3 = payload(200_000);
    s.put_object("alpha", "gone", data3, None).await.unwrap();
    let vdir3 = layout::object_dir(d.path(), "alpha", "gone").unwrap().join(
        &read_disk_meta(&s, "alpha", "gone")
            .await
            .latest()
            .unwrap()
            .part
            .uuid,
    );
    for i in 0..5 {
        std::fs::remove_file(vdir3.join(format!("part.1.{i}"))).unwrap();
    }
    assert!(matches!(
        s.get_object("alpha", "gone").await,
        Err(Error::CorruptMeta(_))
    ));
}

#[tokio::test]
async fn ec_sha_mismatch_fails_closed() {
    // 能过 CRC（坏片字节被改 + meta 里 CRC 被同步改掉）但整份数据错：SHA256 终验必须拦下
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", payload(200_000), None)
        .await
        .unwrap();

    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    let mut m = XlMeta::decode(
        &tokio::fs::read(obj_dir.join(meta::META_FILE))
            .await
            .unwrap(),
    )
    .unwrap();
    let v = m.versions.last_mut().unwrap();
    let spath = obj_dir.join(&v.part.uuid).join("part.1.2");
    let mut shard = tokio::fs::read(&spath).await.unwrap();
    shard[0] ^= 0xff;
    tokio::fs::write(&spath, &shard).await.unwrap();
    v.data_layout.as_mut().unwrap().shards[2].crc32c = ec::crc32c_of(&shard);
    tokio::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap())
        .await
        .unwrap();

    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::CorruptMeta(_))
    ));
}

#[tokio::test]
async fn zero_byte_object_inlines_even_with_zero_threshold() {
    // 评审 F4：inline_threshold=0（「永不内联」的意图）时，0 字节对象不得进 EC
    //（rs 拒收空片 → 500）；0 字节恒内联。非空对象仍按阈值走 EC。
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.inline_threshold = 0;
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();

    let info = s
        .put_object("alpha", "empty", Bytes::new(), None)
        .await
        .unwrap();
    assert_eq!(info.size, 0);
    assert!(
        read_disk_meta(&s, "alpha", "empty")
            .await
            .latest()
            .unwrap()
            .inline
            .is_some()
    );
    let (_, body) = s.get_object("alpha", "empty").await.unwrap();
    assert!(body.is_empty());

    s.put_object("alpha", "one", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "one").await;
    assert_eq!(
        m.latest()
            .unwrap()
            .data_layout
            .as_ref()
            .unwrap()
            .shards
            .len(),
        12
    );
    let (_, body) = s.get_object("alpha", "one").await.unwrap();
    assert_eq!(&body[..], b"x");
}

#[tokio::test]
async fn all_shards_corrupt_is_reported_as_corruption_not_404() {
    // 评审 F5：12 片全读得到但 CRC 全不符 = 真实损坏，必须走 500 谱系（CorruptMeta），
    // 不得与「全片消失」混同伪装成 404
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", payload(200_000), None)
        .await
        .unwrap();
    let vdir = layout::object_dir(d.path(), "alpha", "k").unwrap().join(
        &read_disk_meta(&s, "alpha", "k")
            .await
            .latest()
            .unwrap()
            .part
            .uuid,
    );
    for i in 0..12 {
        let p = vdir.join(format!("part.1.{i}"));
        let mut b = std::fs::read(&p).unwrap();
        b[0] ^= 0xff;
        std::fs::write(&p, &b).unwrap();
    }
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::CorruptMeta(_))
    ));
}

#[tokio::test]
async fn multipart_complete_rejects_tampered_part_bytes() {
    // 评审 F2：upload_part 不持锁，complete 拼装期间同号 part 可能被并发重传。
    // 直接复现「sidecar 快照之后、字节被同长度改写」→ complete 必须 InvalidPart（fail-closed），
    // 且可重试：重传正确数据后 complete 成功
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let id = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let etag = s
        .upload_part("alpha", "k", &id, 1, bytes_stream(vec![7u8; 2048]))
        .await
        .unwrap();

    let staging = layout::object_dir(d.path(), "alpha", "k")
        .unwrap()
        .join(&id);
    tokio::fs::write(staging.join("part.1"), vec![9u8; 2048])
        .await
        .unwrap();

    assert!(matches!(
        s.complete_multipart_upload("alpha", "k", &id, &[(1, etag.clone())])
            .await,
        Err(Error::InvalidPart(_))
    ));
    assert!(matches!(
        s.head_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));

    // 重传正确数据（同号覆盖）→ complete 可成功
    let etag = s
        .upload_part("alpha", "k", &id, 1, bytes_stream(vec![7u8; 2048]))
        .await
        .unwrap();
    s.complete_multipart_upload("alpha", "k", &id, &[(1, etag)])
        .await
        .unwrap();
    let (_, body) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(body.len(), 2048);
    assert!(body.iter().all(|b| *b == 7));
}

#[tokio::test]
async fn inline_length_mismatch_fails_closed() {
    // 信任边界：手工改坏的 meta（内联长度 ≠ 对象长度）不得 panic，宁可报损坏
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", payload(100), None)
        .await
        .unwrap();

    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    let mut m = XlMeta::decode(
        &tokio::fs::read(obj_dir.join(meta::META_FILE))
            .await
            .unwrap(),
    )
    .unwrap();
    let inline = m.versions.last_mut().unwrap().inline.as_mut().unwrap();
    inline.bytes.truncate(5);
    tokio::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap())
        .await
        .unwrap();

    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::CorruptMeta(_))
    ));
    assert!(matches!(
        s.get_object_range("alpha", "k", 0..5).await,
        Err(Error::CorruptMeta(_))
    ));
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
    // 数据形态须依赖版本目录：EC 对象（≥ 阈值）。内联对象数据在 meta 里，与目录无关
    let data = payload(129 * 1024);
    s.put_object("alpha", "k", data, None).await.unwrap();
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

    // 内联的镜像性质：数据在 meta 里，版本目录被移走也照读（无跨文件竞态窗口）
    let inline_data = payload(64);
    s.put_object("alpha", "i", inline_data.clone(), None)
        .await
        .unwrap();
    let vi = read_disk_meta(&s, "alpha", "i").await;
    let idir = layout::object_dir(d.path(), "alpha", "i")
        .unwrap()
        .join(&vi.latest().unwrap().part.uuid);
    tokio::fs::remove_dir_all(idir).await.unwrap();
    let (_, bytes) = s.get_object("alpha", "i").await.unwrap();
    assert_eq!(&bytes[..], &inline_data[..]);
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
    // put/delete 并发下读路径的不变式：每次读要么是某个写入者一次写入的完整对象，
    // 要么 NoSuchKey —— 不得出现其他 Err（IO 错误），也不得读出碎片内容。
    // 对象取 ≥ 128 KiB（EC 形态）：数据跨 12 个分片文件，才真正压到「删除/覆写的中间态」
    // 这个跨文件竞态（评审 F1；64 B 会走内联、数据在 meta 里，该用例会退化为空转）。
    let n = 129 * 1024;
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let s = std::sync::Arc::new(s);
    let mut handles = Vec::new();
    for i in 0..8u8 {
        let s = std::sync::Arc::clone(&s);
        handles.push(tokio::spawn(async move {
            if i % 2 == 0 {
                s.put_object("alpha", "same", Bytes::from(vec![i; n]), None)
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
                assert_eq!(info.size, n as u64);
                assert_eq!(body.len(), n);
                assert!(body.iter().all(|b| *b == body[0]), "碎片内容");
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
    let (s, d) = store().await;
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

    // Task 6：小对象 complete 后走内联（版本目录只有 xl.meta），暂存区整目录清理
    let obj_dir = layout::object_dir(d.path(), "alpha", "big").unwrap();
    let m = read_disk_meta(&s, "alpha", "big").await;
    let v = m.latest().unwrap();
    assert_eq!(v.inline.as_ref().unwrap().bytes.as_slice(), b"hello");
    assert!(v.parts.is_none(), "新写入不再使用 parts");
    assert_eq!(shard_files(&obj_dir.join(&v.part.uuid)), 0);
    assert!(
        !tokio::fs::try_exists(obj_dir.join(&id)).await.unwrap(),
        "complete 后暂存区应被清理"
    );
}

#[tokio::test]
async fn multipart_complete_over_threshold_writes_ec() {
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let id = s
        .create_multipart_upload("alpha", "ec", None, Default::default())
        .await
        .unwrap();
    let data = payload(200 * 1024);
    let etag = s
        .upload_part("alpha", "ec", &id, 1, bytes_stream(data.to_vec()))
        .await
        .unwrap();
    let info = s
        .complete_multipart_upload("alpha", "ec", &id, &[(1, etag)])
        .await
        .unwrap();
    assert_eq!(info.size, (200 * 1024) as u64);
    assert!(info.etag.ends_with("-1"));

    let obj_dir = layout::object_dir(d.path(), "alpha", "ec").unwrap();
    let m = read_disk_meta(&s, "alpha", "ec").await;
    let v = m.latest().unwrap();
    assert!(v.inline.is_none());
    assert!(v.parts.is_none(), "新写入不再使用 parts");
    assert_eq!(v.data_layout.as_ref().unwrap().shards.len(), 12);
    assert_eq!(shard_files(&obj_dir.join(&v.part.uuid)), 12);
    assert!(
        !tokio::fs::try_exists(obj_dir.join(&id)).await.unwrap(),
        "complete 后暂存区应被清理"
    );

    let (_, bytes) = s.get_object("alpha", "ec").await.unwrap();
    assert_eq!(&bytes[..], &data[..]);
    // EC 形态在 MPU 路径同样成立：丢 4 片仍可重建
    let vdir = obj_dir.join(&v.part.uuid);
    for i in [0, 3, 7, 11] {
        std::fs::remove_file(vdir.join(format!("part.1.{i}"))).unwrap();
    }
    let (_, bytes) = s.get_object("alpha", "ec").await.unwrap();
    assert_eq!(&bytes[..], &data[..]);
}

// ---- M2b-2：多盘（Task 7/8） ----

/// 12 盘 Store（每盘一个 tempdir；EC 8+4 的最小全冗余形态）。
async fn store_12() -> (Store, Vec<tempfile::TempDir>) {
    let dirs: Vec<_> = (0..12).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut cfg = StoreConfig::new(dirs[0].path());
    cfg.drives = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    let s = Store::open(cfg).await.unwrap();
    (s, dirs)
}

#[tokio::test]
async fn multi_drive_ec_spreads_one_shard_per_drive_and_roundtrips() {
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    let data = payload(300_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();

    let m = read_disk_meta(&s, "alpha", "k").await;
    let v = m.latest().unwrap();
    let l = v.data_layout.as_ref().unwrap();
    assert_eq!(l.shards.len(), 12);
    // 12 盘 × 每盘恰 1 片；每盘都有 meta 副本
    for (d, dir) in dirs.iter().enumerate() {
        let obj_dir = layout::object_dir(dir.path(), "alpha", "k").unwrap();
        assert!(
            tokio::fs::try_exists(obj_dir.join(meta::META_FILE))
                .await
                .unwrap(),
            "盘 {d} 应有 meta 副本"
        );
        let vdir = obj_dir.join(&v.part.uuid);
        assert_eq!(shard_files(&vdir), 1, "盘 {d} 应恰有 1 片");
        assert_eq!(l.shards[d].drive as usize, d, "分片 {d} 应落盘 {d}");
    }
    // 读回 + Range
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&bytes[..], &data[..]);
    let r = s
        .get_object_range("alpha", "k", 100_000..100_010)
        .await
        .unwrap();
    assert_eq!(&r[..], &data[100_000..100_010]);
}

#[tokio::test]
async fn multi_drive_pull_two_then_five_drives() {
    // 设计验收 #1 的本地版：拔盘（整目录 rename 走）后读写行为
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    let stash = tempfile::tempdir().unwrap();

    let pull = |i: usize| {
        let from = dirs[i].path().to_path_buf();
        let to = stash.path().join(format!("pulled-{i}"));
        std::fs::rename(&from, &to).unwrap();
    };

    let data = payload(300_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();
    pull(2);
    pull(5);
    // 拔 2 盘：读成功（重建 2 片）；再写成功（健康盘 10 ≥ 9 quorum）
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&bytes[..], &data[..], "拔 2 盘读必须成功");
    let data2 = payload(200_000);
    s.put_object("alpha", "k2", data2.clone(), None)
        .await
        .unwrap();
    let (_, bytes) = s.get_object("alpha", "k2").await.unwrap();
    assert_eq!(&bytes[..], &data2[..], "拔 2 盘写必须成功");

    // 再拔 2 盘（共 4 拔）：读 k 仍成功（8 片在 = data 边界）；写必须 QuorumUnmet 且无残留
    pull(7);
    pull(9);
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&bytes[..], &data[..], "拔 4 盘读必须成功（8 = data）");
    let err = s.put_object("alpha", "k3", payload(200_000), None).await;
    assert!(
        matches!(err, Err(Error::QuorumUnmet(_))),
        "8 < 9 必须 QuorumUnmet: {err:?}"
    );
    // 回滚：k3 未提交（head 报 NoSuchKey），健康盘上无 k3 的版本目录残留
    assert!(matches!(
        s.head_object("alpha", "k3").await,
        Err(Error::NoSuchKey(_))
    ));
    for i in [0, 1, 3, 4] {
        let obj_dir = layout::object_dir(dirs[i].path(), "alpha", "k3").unwrap();
        if tokio::fs::try_exists(&obj_dir).await.unwrap() {
            let mut entries = tokio::fs::read_dir(&obj_dir).await.unwrap();
            assert!(
                entries.next_entry().await.unwrap().is_none(),
                "盘 {i} 上 k3 目录应无残留版本"
            );
        }
    }

    // 拔第 5 盘：7 < 8 → 读 fail-closed
    pull(11);
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::CorruptMeta(_))
    ));
}

#[tokio::test]
async fn multi_drive_read_repairs_missing_shard() {
    // 读时修（M2b-2 最小版）：缺 1 片 → 重建成功 + best-effort 回写
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    let data = payload(300_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "k").await;
    let v = m.latest().unwrap();
    let shard2 = layout::object_dir(dirs[2].path(), "alpha", "k")
        .unwrap()
        .join(&v.part.uuid)
        .join("part.1.2");
    std::fs::remove_file(&shard2).unwrap();

    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&bytes[..], &data[..]);
    assert!(
        tokio::fs::try_exists(&shard2).await.unwrap(),
        "读时修应把缺失分片写回"
    );
    // 回写的片内容与 CRC 与原片一致（再次读不再依赖重建）
    let sref = &m.latest().unwrap().data_layout.as_ref().unwrap().shards[2];
    let repaired = std::fs::read(&shard2).unwrap();
    assert_eq!(ec::crc32c_of(&repaired), sref.crc32c);
}

#[tokio::test]
async fn multi_drive_inline_replication_survives_all_but_one_drive() {
    // 内联对象：数据在 meta 里，12 盘各一份副本——只剩 1 盘也能读（M2b-1 记录的「内联无冗余」边界在此收口）
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    let stash = tempfile::tempdir().unwrap();
    let data = payload(4096);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "k").await;
    let v = m.latest().unwrap();
    assert!(v.inline.is_some());
    for (d, dir) in dirs.iter().enumerate() {
        // 内联多盘：数据只在 meta 里——连版本目录都不建，没有任何数据文件
        let vdir = layout::object_dir(dir.path(), "alpha", "k")
            .unwrap()
            .join(&v.part.uuid);
        assert!(
            !tokio::fs::try_exists(&vdir).await.unwrap(),
            "内联不应建版本目录（盘 {d}）"
        );
    }
    for (i, dir) in dirs.iter().enumerate().take(11) {
        std::fs::rename(dir.path(), stash.path().join(format!("pulled-{i}"))).unwrap();
    }
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(
        &bytes[..],
        &data[..],
        "11 盘拔掉，最后 1 盘的 meta 副本仍可服务"
    );
}

#[tokio::test]
async fn multi_drive_meta_latest_wins_on_torn_replicas() {
    // 撕裂缝中的读侧契约（铁律②读侧）：各盘 meta 不一致时 latest.mod_time 最新者胜
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    let mk = |t: i64, content: &[u8]| {
        let bytes = content.to_vec();
        XlMeta {
            object_key: "torn".into(),
            versions: vec![meta::Version {
                version_id: uuid::Uuid::nil(),
                mod_time_nanos: t,
                size: bytes.len() as u64,
                etag: "e".into(),
                content_type: None,
                user_metadata: Default::default(),
                part: meta::PartRef {
                    uuid: format!("uuid-t{t}"),
                    part: 1,
                    size: bytes.len() as u64,
                    sha256: String::new(),
                },
                parts: None,
                checksum: None,
                data_layout: None,
                inline: Some(meta::InlineData { bytes }),
            }],
        }
    };
    for (i, dir) in dirs.iter().enumerate() {
        let obj_dir = layout::object_dir(dir.path(), "alpha", "torn").unwrap();
        tokio::fs::create_dir_all(&obj_dir).await.unwrap();
        let m = if i < 6 {
            mk(100, b"old")
        } else {
            mk(200, b"new")
        };
        tokio::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap())
            .await
            .unwrap();
    }
    let (_, bytes) = s.get_object("alpha", "torn").await.unwrap();
    assert_eq!(&bytes[..], b"new", "mod_time 最新者胜");
}

#[tokio::test]
async fn multi_drive_delete_removes_all_replicas() {
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", payload(200_000), None)
        .await
        .unwrap();
    s.delete_object("alpha", "k").await.unwrap();
    for (i, dir) in dirs.iter().enumerate() {
        let obj_dir = layout::object_dir(dir.path(), "alpha", "k").unwrap();
        assert!(
            !tokio::fs::try_exists(&obj_dir).await.unwrap(),
            "盘 {i} 上对象目录应被删除"
        );
    }
}

#[tokio::test]
async fn open_rejects_misconfigured_drive_counts() {
    // 2..11 盘（半冗余误配）→ Err；enabled=false + 多盘 → Err
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut cfg = StoreConfig::new(dirs[0].path());
    cfg.drives = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    assert!(matches!(Store::open(cfg).await, Err(Error::Encode(_))));

    let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut cfg = StoreConfig::new(dirs[0].path());
    cfg.drives = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    cfg.ec.enabled = false;
    assert!(matches!(Store::open(cfg).await, Err(Error::Encode(_))));
}

#[tokio::test]
async fn ec_disabled_single_drive_writes_legacy_form() {
    // enabled=false：M1 旧形态（单文件 part.1、无内联、无分片），读路径走旧分支
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.ec.enabled = false;
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    let data = payload(300_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "k").await;
    let v = m.latest().unwrap();
    assert!(v.data_layout.is_none() && v.inline.is_none());
    let vdir = layout::object_dir(dir.path(), "alpha", "k")
        .unwrap()
        .join(&v.part.uuid);
    assert_eq!(shard_files(&vdir), 1, "旧形态：恰一个 part.1");
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&bytes[..], &data[..]);
}

// ---- M2b-2 Task 10：桶级互斥 / flock / fsck ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_delete_race_never_revives_bucket() {
    // 并发 delete_bucket + put 的不变式：二者不可能同时成功（桶锁窗口收口）。
    // 可能结局只有两种：put 先（桶非空 → 删被拒）或 delete 先（put 撞 NoSuchBucket）。
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false; // 30 轮竞态下的提速，与锁语义无关
    let s = Store::open(cfg).await.unwrap();
    let s = std::sync::Arc::new(s);
    for round in 0..30 {
        let bucket = format!("race-{round}");
        s.create_bucket(&bucket).await.unwrap();
        let (put_res, del_res) = tokio::join!(
            {
                let s = std::sync::Arc::clone(&s);
                let b = bucket.clone();
                async move { s.put_object(&b, "k", payload(1024), None).await }
            },
            {
                let s = std::sync::Arc::clone(&s);
                let b = bucket.clone();
                async move { s.delete_bucket(&b).await }
            }
        );
        assert!(
            !(put_res.is_ok() && del_res.is_ok()),
            "round {round}: put 与 delete_bucket 不得同时成功（桶复活）"
        );
        if del_res.is_ok() {
            assert!(
                !s.bucket_exists(&bucket).await.unwrap(),
                "round {round}: 删桶成功后桶必须消失"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flock_serializes_across_store_instances() {
    // 两个 Store 实例 = 跨进程语义的代表：进程内锁表各自独立，只剩 flock 兜底。
    // 不变式：并发写/删交错下，读只允许「某个写者的完整字节」或 NoSuchKey。
    let dirs: Vec<_> = (0..12).map(|_| tempfile::tempdir().unwrap()).collect();
    let mk_cfg = || {
        let mut cfg = StoreConfig::new(dirs[0].path());
        cfg.drives = dirs.iter().map(|d| d.path().to_path_buf()).collect();
        cfg.fsync = false; // 并发语义与 fsync 无关；高负载机器上 30 写 × 48 次 fsync 会拖成分钟级
        cfg
    };
    let s1 = std::sync::Arc::new(Store::open(mk_cfg()).await.unwrap());
    let s2 = std::sync::Arc::new(Store::open(mk_cfg()).await.unwrap());
    s1.create_bucket("alpha").await.unwrap();

    let n = 129 * 1024;
    let put_task = |s: std::sync::Arc<Store>, byte: u8| async move {
        for _ in 0..15 {
            s.put_object("alpha", "k", Bytes::from(vec![byte; n]), None)
                .await
                .unwrap();
        }
    };
    let del_task = {
        let s = std::sync::Arc::clone(&s2);
        async move {
            for _ in 0..15 {
                let _ = s.delete_object("alpha", "k").await;
            }
        }
    };
    let read_task = {
        let s = std::sync::Arc::clone(&s1);
        async move {
            for _ in 0..120 {
                match s.get_object("alpha", "k").await {
                    Ok((info, body)) => {
                        assert_eq!(info.size, n as u64);
                        assert_eq!(body.len(), n);
                        assert!(body.iter().all(|b| *b == body[0]), "撕裂内容");
                    }
                    Err(Error::NoSuchKey(_)) => {}
                    Err(e) => panic!("读路径不应出现其他错误: {e}"),
                }
            }
        }
    };
    let (r1, r2, r3) = tokio::join!(
        put_task(std::sync::Arc::clone(&s1), 1),
        put_task(std::sync::Arc::clone(&s2), 2),
        async { tokio::join!(del_task, read_task) }
    );
    let _ = (r1, r2, r3);
}

#[tokio::test]
async fn fsck_reports_and_repairs() {
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", payload(200_000), None)
        .await
        .unwrap();
    s.put_object("alpha", "small", payload(64), None)
        .await
        .unwrap();

    // 植放：drive 3 上一个孤儿版本目录 + drive 0 的 tmp 一个残骸
    let obj_dir = layout::object_dir(dirs[3].path(), "alpha", "k").unwrap();
    let orphan = obj_dir.join("orphan-uuid");
    tokio::fs::create_dir_all(&orphan).await.unwrap();
    tokio::fs::write(orphan.join("part.1.3"), b"junk")
        .await
        .unwrap();
    let tmp = layout::sys_dir(dirs[0].path(), "tmp");
    tokio::fs::write(tmp.join("junk"), b"x").await.unwrap();
    // 植坏：drive 2 上删掉 k 的分片
    let m = read_disk_meta(&s, "alpha", "k").await;
    let v = m.latest().unwrap();
    let shard2 = layout::object_dir(dirs[2].path(), "alpha", "k")
        .unwrap()
        .join(&v.part.uuid)
        .join("part.1.2");
    std::fs::remove_file(&shard2).unwrap();

    // 只读巡检：孤儿/缺片/tmp 都被报出
    let r = s.fsck(false).await.unwrap();
    assert_eq!(r.drives, 12);
    assert!(r.objects_scanned >= 2);
    assert_eq!(r.orphan_dirs.len(), 1, "{:?}", r.orphan_dirs);
    assert!(r.orphan_dirs[0].starts_with("3#alpha/k/orphan-uuid"));
    assert_eq!(r.missing_shards, 1);
    assert_eq!(r.bad_shards, 0);
    assert!(r.tmp_entries >= 1);
    assert!(!r.repaired);
    // 只读：孤儿仍在（报告不等于修复）
    assert!(tokio::fs::try_exists(&orphan).await.unwrap());

    // --repair：删孤儿 + 读时修分片
    let r = s.fsck(true).await.unwrap();
    assert!(r.repaired);
    assert!(!tokio::fs::try_exists(&orphan).await.unwrap(), "孤儿应被删");
    assert!(
        tokio::fs::try_exists(&shard2).await.unwrap(),
        "缺片应被读时修回写"
    );

    // 复检：缺片与孤儿清零，对象仍可读
    let r = s.fsck(false).await.unwrap();
    assert_eq!(r.missing_shards, 0);
    assert_eq!(r.orphan_dirs.len(), 0);
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(bytes.len(), 200_000);
}

// ---- M2b-2 评审修复轮（R1/R2/R3/R4）----

fn id_path(d: &tempfile::TempDir) -> std::path::PathBuf {
    d.path().join(layout::SYS_DIR).join("drive-id")
}

fn drive_cfg(dirs: &[tempfile::TempDir]) -> StoreConfig {
    let mut cfg = StoreConfig::new(dirs[0].path());
    cfg.drives = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    cfg
}

#[tokio::test]
async fn open_stamps_and_verifies_drive_identity() {
    // R1：首启全盘盖同一部署标识；插错盘（标识不一致）大声失败
    let (_, dirs) = store_12().await;
    let first = tokio::fs::read_to_string(id_path(&dirs[0])).await.unwrap();
    assert!(!first.trim().is_empty());
    for d in &dirs {
        assert_eq!(
            tokio::fs::read_to_string(id_path(d)).await.unwrap(),
            first,
            "全盘标识必须一致"
        );
    }
    tokio::fs::write(id_path(&dirs[3]), "foreign-deployment")
        .await
        .unwrap();
    assert!(matches!(
        Store::open(drive_cfg(&dirs)).await,
        Err(Error::Encode(_))
    ));
    // 还原后可开
    tokio::fs::write(id_path(&dirs[3]), &first).await.unwrap();
    Store::open(drive_cfg(&dirs)).await.unwrap();
}

#[tokio::test]
async fn open_treats_uninitialized_drive_as_offline_and_adopts_on_return() {
    // R1 核心：掉线盘的 `.oss.sys` 不可见时，open 不得替它重建（impostor 防线的本体）；
    // 恢复后凭 sys 目录自动重新在册，无需重启
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k", payload(300_000), None)
        .await
        .unwrap();
    drop(s);

    // 模拟掉线：drive5 的 .oss.sys 整体挪走（挂载点掉线 = 该路径看不到真盘内容）
    let sys5 = dirs[5].path().join(layout::SYS_DIR);
    let stash = tempfile::tempdir().unwrap();
    std::fs::rename(&sys5, stash.path().join("sys5")).unwrap();

    let s = Store::open(drive_cfg(&dirs)).await.unwrap();
    assert!(
        !tokio::fs::try_exists(&sys5).await.unwrap(),
        "open 不得为掉线盘重建 .oss.sys（impostor 防线）"
    );
    // 11 盘在册：写照常（≥ quorum），且新对象不在 drive5 落片
    let data = payload(200_000);
    s.put_object("alpha", "k2", data.clone(), None)
        .await
        .unwrap();
    let (_, bytes) = s.get_object("alpha", "k2").await.unwrap();
    assert_eq!(&bytes[..], &data[..]);
    let m = read_disk_meta(&s, "alpha", "k2").await;
    let v = m.latest().unwrap();
    let v5 = layout::object_dir(dirs[5].path(), "alpha", "k2")
        .unwrap()
        .join(&v.part.uuid);
    assert!(
        !tokio::fs::try_exists(&v5).await.unwrap(),
        "掉线盘不应有新对象的版本目录"
    );

    // 插回：恢复 .oss.sys → 无需重开即在册
    std::fs::rename(stash.path().join("sys5"), &sys5).unwrap();
    s.put_object("alpha", "k3", payload(200_000), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "k3").await;
    let v = m.latest().unwrap();
    let v5 = layout::object_dir(dirs[5].path(), "alpha", "k3")
        .unwrap()
        .join(&v.part.uuid);
    assert_eq!(shard_files(&v5), 1, "插回后新写入应重新用满 12 盘");
    s.get_object("alpha", "k").await.unwrap(); // 旧对象照常可读
}

#[tokio::test]
async fn delete_fails_closed_when_drive_absent() {
    // R2：拔盘时删除不得静默成功（插回即复活）；插回后可删
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    let data = payload(200_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();
    let stash = tempfile::tempdir().unwrap();
    std::fs::rename(dirs[3].path(), stash.path().join("d3")).unwrap();

    assert!(
        matches!(
            s.delete_object("alpha", "k").await,
            Err(Error::QuorumUnmet(_))
        ),
        "拔盘时删除必须 fail-closed"
    );
    // 对象完好
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&bytes[..], &data[..]);
    assert!(matches!(
        s.delete_bucket("alpha").await,
        Err(Error::QuorumUnmet(_))
    ));

    // 插回 → 可删
    std::fs::rename(stash.path().join("d3"), dirs[3].path()).unwrap();
    s.delete_object("alpha", "k").await.unwrap();
    assert!(matches!(
        s.head_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
}

#[tokio::test]
async fn list_union_survives_drive0_pull() {
    // R3：拔 drive0 后列举不得误报 NoSuchBucket/空
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    for k in ["a", "b"] {
        s.put_object("alpha", k, payload(64), None).await.unwrap();
    }
    let stash = tempfile::tempdir().unwrap();
    std::fs::rename(dirs[0].path(), stash.path().join("d0")).unwrap();

    let r = s
        .list_objects("alpha", &ListRequest::default())
        .await
        .unwrap();
    let keys: Vec<String> = r.objects.iter().map(|o| o.key.clone()).collect();
    assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
    let buckets = s.list_buckets().await.unwrap();
    assert!(buckets.iter().any(|b| b.name == "alpha"));
    s.get_object("alpha", "a").await.unwrap();
}

#[tokio::test]
async fn open_rejects_degenerate_ec_numbers() {
    // R4：data/parity 的 0 值与 u8 相加溢出在 open 收口（不得 panic）
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.ec.data = 0;
    assert!(matches!(Store::open(cfg).await, Err(Error::Encode(_))));

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.ec.parity = 0;
    assert!(matches!(Store::open(cfg).await, Err(Error::Encode(_))));

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.ec.data = 200;
    cfg.ec.parity = 100; // 相加 300 > 255：u16 运算不得溢出 panic
    assert!(matches!(Store::open(cfg).await, Err(Error::Encode(_))));
}

#[tokio::test]
async fn legacy_parts_form_still_readable() {
    // M2a 旧对象回归：parts: Some + 多段 part.N 文件的旧形态读路径必须继续可读
    //（Task 6 裁决后新写入不再产生此形态，但升级不得让旧对象失读）
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let obj_dir = layout::object_dir(d.path(), "alpha", "legacy").unwrap();
    let vdir = obj_dir.join("legacy-uuid");
    tokio::fs::create_dir_all(&vdir).await.unwrap();
    tokio::fs::write(vdir.join("part.1"), b"AAA").await.unwrap();
    tokio::fs::write(vdir.join("part.2"), b"BBBB")
        .await
        .unwrap();
    let mk = |part: u16, size: u64| meta::PartRef {
        uuid: "legacy-uuid".into(),
        part,
        size,
        sha256: String::new(),
    };
    let version = meta::Version {
        version_id: uuid::Uuid::nil(),
        mod_time_nanos: 1,
        size: 7,
        etag: "legacy".into(),
        content_type: None,
        user_metadata: Default::default(),
        part: mk(1, 3),
        parts: Some(vec![mk(1, 3), mk(2, 4)]),
        checksum: None,
        data_layout: None,
        inline: None,
    };
    let m = XlMeta {
        object_key: "legacy".into(),
        versions: vec![version],
    };
    tokio::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap())
        .await
        .unwrap();

    let (info, bytes) = s.get_object("alpha", "legacy").await.unwrap();
    assert_eq!(info.size, 7);
    assert_eq!(&bytes[..], b"AAABBBB");
    let r = s.get_object_range("alpha", "legacy", 2..6).await.unwrap();
    assert_eq!(&r[..], b"ABBB");
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
