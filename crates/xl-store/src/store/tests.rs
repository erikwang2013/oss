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
async fn ec_old_segment_layout_reads_chunked() {
    // 「旧写新读」兼容：手工按旧格式落盘（整份 ec::encode 的段式布局、shard_size =
    // ceil(len/data)、**非 W 对齐**），新分块读路径必须原样读出（读端不假设 W 对齐）。
    // 「新写旧读」在编解码层已证：分块产物经整份 `reconstruct` 直读（ec.rs 测试），
    // 且 meta 的 ShardRef.crc32c 与整片重算一致（旧读端的整片 CRC 闸可过）。
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let data = payload(300_001);
    s.put_object("alpha", "old", data.clone(), None)
        .await
        .unwrap();

    let obj_dir = layout::object_dir(d.path(), "alpha", "old").unwrap();
    let mut m = XlMeta::decode(
        &tokio::fs::read(obj_dir.join(meta::META_FILE))
            .await
            .unwrap(),
    )
    .unwrap();
    let v = m.versions.last_mut().unwrap();
    let uuid = v.part.uuid.clone();
    let ss_old = (data.len() as u64).div_ceil(8);
    assert_ne!(
        ss_old % ec::CHUNK_W as u64,
        0,
        "前置：旧布局须非 W 对齐（否则测不到边界）"
    );
    let cfg84 = ec::EcConfig {
        data: 8,
        parity: 4,
        ..Default::default()
    };
    let shards = ec::encode(&data, cfg84).unwrap();
    let vdir = obj_dir.join(&uuid);
    for (i, sh) in shards.iter().enumerate() {
        std::fs::write(vdir.join(format!("part.1.{i}")), sh).unwrap();
        v.data_layout.as_mut().unwrap().shards[i] = meta::ShardRef {
            drive: 0,
            part: 1,
            shard: i as u8,
            size: ss_old,
            crc32c: ec::crc32c_of(sh),
        };
    }
    v.data_layout.as_mut().unwrap().shard_size = ss_old;
    std::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap()).unwrap();

    let (_, got) = s.get_object("alpha", "old").await.unwrap();
    assert_eq!(&got[..], &data[..], "旧布局整读");
    // 跨块边界区间（末块短于 W 的读点）
    let w = ec::CHUNK_W;
    let r = s
        .get_object_range("alpha", "old", (w - 3) as u64..(w + 5) as u64)
        .await
        .unwrap();
    assert_eq!(&r[..], &data[w - 3..w + 5], "旧布局跨块区间");

    // 第二例：ss_old > W 且多块（ss=75000、blocks=2、末块短 9464）——覆盖段内跨块对与
    // **跨片段**区间（s1>s0 的 (s,t) 迭代分支；第一例 ss<W 走不到）
    let data2 = payload(600_000);
    s.put_object("alpha", "old2", data2.clone(), None)
        .await
        .unwrap();
    let obj_dir2 = layout::object_dir(d.path(), "alpha", "old2").unwrap();
    let mut m2 = XlMeta::decode(
        &tokio::fs::read(obj_dir2.join(meta::META_FILE))
            .await
            .unwrap(),
    )
    .unwrap();
    let v2 = m2.versions.last_mut().unwrap();
    let uuid2 = v2.part.uuid.clone();
    let ss2 = (data2.len() as u64).div_ceil(8);
    assert!(ss2 > ec::CHUNK_W as u64, "前置：第二例须 ss>W（ss2={ss2}）");
    let shards2 = ec::encode(&data2, cfg84).unwrap();
    let vdir2 = obj_dir2.join(&uuid2);
    for (i, sh) in shards2.iter().enumerate() {
        std::fs::write(vdir2.join(format!("part.1.{i}")), sh).unwrap();
        v2.data_layout.as_mut().unwrap().shards[i] = meta::ShardRef {
            drive: 0,
            part: 1,
            shard: i as u8,
            size: ss2,
            crc32c: ec::crc32c_of(sh),
        };
    }
    v2.data_layout.as_mut().unwrap().shard_size = ss2;
    std::fs::write(obj_dir2.join(meta::META_FILE), m2.encode().unwrap()).unwrap();

    let (_, got2) = s.get_object("alpha", "old2").await.unwrap();
    assert_eq!(&got2[..], &data2[..], "旧布局多块整读");
    let wu = ec::CHUNK_W as u64;
    let r2 = s
        .get_object_range("alpha", "old2", (wu - 3)..(wu + 5))
        .await
        .unwrap();
    assert_eq!(
        &r2[..],
        &data2[(wu - 3) as usize..(wu + 5) as usize],
        "段内跨块区间"
    );
    let r3 = s
        .get_object_range("alpha", "old2", (ss2 - 2)..(ss2 + 2))
        .await
        .unwrap();
    assert_eq!(
        &r3[..],
        &data2[(ss2 - 2) as usize..(ss2 + 2) as usize],
        "跨片段区间（s1>s0）"
    );
}

/// 流式内存峰值锚（96 MiB 对象）：分块路径的内核额外峰值应为 MB 级。
/// **必须单独跑**（VmHWM 是进程峰值）：`cargo test -p xl-store --lib
/// ec_streaming_memory_peak -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn ec_streaming_memory_peak_stays_flat() {
    fn vmhwm_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find(|l| l.starts_with("VmHWM:"))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap()
    }
    let (s, _d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let mib = 1024 * 1024usize;
    let chunk = vec![0xABu8; mib];
    let stream = futures::stream::iter((0..96).map(move |_| Ok(bytes::Bytes::from(chunk.clone()))));
    let info = s
        .put_object_stream("alpha", "huge", Box::pin(stream), None)
        .await
        .unwrap();
    assert_eq!(info.size, 96 * mib as u64);
    let (_, back) = s.get_object("alpha", "huge").await.unwrap();
    assert_eq!(back.len(), 96 * mib);
    assert!(back[..].iter().all(|b| *b == 0xAB));
    let peak = vmhwm_kb();
    // 阈值 200 MiB（verifier 校准）：delta 含读回缓冲 96 MiB（Bytes API 固有）；分块路径
    // 实测 ≈107 MiB。旧整份路径 ≈96(payload)+144(12 片)+96(读回) ≈ 240 MiB 以上——
    // 320 阈值放它过，200 才能真的拦回归。
    eprintln!("VmHWM peak={peak} KiB");
    assert!(
        peak < 200 * 1024,
        "峰值 {peak} KiB 超过 200 MiB（分块路径回归？）"
    );
}

#[tokio::test]
async fn ec_sha_mismatch_fails_closed() {
    // 数据完好但**提交记录被篡改**（meta 的 part.sha256 改掉）：SHA256 终验必须拦下——
    // 分块路径的最后一闸（逐块对偶管不到「记录本身是假的」）。
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
    m.versions.last_mut().unwrap().part.sha256 = "00".repeat(32);
    std::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap()).unwrap();
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::CorruptMeta(_))
    ));
}

#[tokio::test]
async fn ec_forged_crc_heals_one_shard_and_fails_closed_at_two() {
    // 能过旧整片 CRC 的伪造（坏片字节被改 + meta 里 CRC 被同步改掉）在 M4 分块路径下
    // 由 12×W 对偶校验接管：**单片**可唯一定位 → 剔除 + parity 重建（EC 自愈，读到正确
    // 数据）；**两片同时**无法唯一定位 → fail-closed。
    // 备注：自愈会以正确字节重写坏片文件，而 meta 里的 CRC 是被伪造的——fsck 的全片
    // CRC 比对会把这个（对抗性）残局标为坏片，属已知 fail-safe 方向（绝不吐坏数据）。
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let data = payload(200_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();

    let obj_dir = layout::object_dir(d.path(), "alpha", "k").unwrap();
    async fn read_meta(dir: &std::path::Path) -> XlMeta {
        XlMeta::decode(&tokio::fs::read(dir.join(meta::META_FILE)).await.unwrap()).unwrap()
    }
    let mut m = read_meta(&obj_dir).await;
    let uuid = m.versions.last().unwrap().part.uuid.clone();
    let spath = |i: usize| obj_dir.join(&uuid).join(format!("part.1.{i}"));
    // 先伪造一片：应自愈
    {
        let mut shard = tokio::fs::read(spath(2)).await.unwrap();
        shard[0] ^= 0xff;
        tokio::fs::write(spath(2), &shard).await.unwrap();
        let v = m.versions.last_mut().unwrap();
        v.data_layout.as_mut().unwrap().shards[2].crc32c = ec::crc32c_of(&shard);
        tokio::fs::write(obj_dir.join(meta::META_FILE), m.encode().unwrap())
            .await
            .unwrap();
    }
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], &data[..], "单片伪造应由 parity 自愈出正确数据");
    // 读时修已把 part.1.2 重写回正确内容（与提交的 sha 一致，见上方备注）
    let healed = tokio::fs::read(spath(2)).await.unwrap();
    assert_eq!(healed, {
        let mut m2 = read_meta(&obj_dir).await;
        let v = m2.versions.last_mut().unwrap();
        let ss = v.data_layout.as_ref().unwrap().shard_size as usize;
        let start = 2 * ss;
        // 片 2 = payload 第 2 段 + 补零
        let end = (start + ss).min(data.len());
        let mut seg = data[start..end].to_vec();
        seg.resize(ss, 0);
        seg
    });

    // 两片**同时**伪造（片 2 前一轮已被自愈重写，需重新伪造）：无法唯一定位 → CorruptMeta
    {
        let mut m2 = read_meta(&obj_dir).await;
        for i in [2usize, 5] {
            let mut shard = tokio::fs::read(spath(i)).await.unwrap();
            shard[0] ^= 0xff;
            tokio::fs::write(spath(i), &shard).await.unwrap();
            m2.versions
                .last_mut()
                .unwrap()
                .data_layout
                .as_mut()
                .unwrap()
                .shards[i]
                .crc32c = ec::crc32c_of(&shard);
        }
        tokio::fs::write(obj_dir.join(meta::META_FILE), m2.encode().unwrap())
            .await
            .unwrap();
    }
    assert!(
        matches!(s.get_object("alpha", "k").await, Err(Error::CorruptMeta(_))),
        "两片同时伪造必须 fail-closed"
    );
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
                s.delete_object("alpha", "same").await.map(|_| ())
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
                is_delete_marker: None,
                is_latest: None,
                epoch: None,
                encryption: None,
                lock: None,
                replication_status: None,
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
        is_delete_marker: None,
        is_latest: None,
        epoch: None,
        encryption: None,
        lock: None,
        replication_status: None,
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
        false,
        None,
        None,
    )
    .await
    .unwrap();
    let head = s.head_object("alpha", "k").await.unwrap();
    assert_eq!(head.checksum, Some(rec.clone()));
    let (get, _body) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(get.checksum, Some(rec));
}

// ---------- M3 in-process 集群基座（Task 3） ----------
//
// 两条写死约定：
// 1) **in-process handler 不得再向外扇出**——真 RPC 只是超时，in-process 是循环 await 死锁；
//    对端 Store 的 mesh_* 一律只碰本地槽。
// 2) fixture 不变量：`slot_count == data + parity`（parity 恒 4、data 由槽位数推导）——
//    后人把配置调成 3×2/4×4 会把算术静默搞坏（4×4 + 硬编码 8+4 尤其坑：`shard % 16`
//    只落槽 0..11，第 4 节点零分片、容错反更差——见 drives.rs 的否决反例测试）。

/// in-process 传输：直调对端 `Store::mesh_*`（测试专用；真网络走 xl-client 的 tonic 实现）。
pub struct InProcessMesh {
    peers: std::sync::Mutex<std::collections::HashMap<String, std::sync::Weak<Store>>>,
    down: std::sync::Mutex<std::collections::HashSet<String>>,
    delays: std::sync::Mutex<std::collections::HashMap<String, std::time::Duration>>,
    fail_once: std::sync::Mutex<std::collections::HashMap<String, u32>>,
}

impl std::fmt::Debug for InProcessMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessMesh").finish_non_exhaustive()
    }
}

impl InProcessMesh {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            peers: Default::default(),
            down: Default::default(),
            delays: Default::default(),
            fail_once: Default::default(),
        })
    }

    /// 构建后回填（Store↔transport 循环：Store 构造需要 transport，transport 又指回 Store）。
    pub fn register(&self, id: &str, store: &Arc<Store>) {
        self.peers
            .lock()
            .unwrap()
            .insert(id.to_string(), Arc::downgrade(store));
    }

    /// 摘除/恢复节点（模拟宕机/回线；多个 down = 分区）。
    pub fn down(&self, node: &str) {
        self.down.lock().unwrap().insert(node.to_string());
    }
    pub fn up(&self, node: &str) {
        self.down.lock().unwrap().remove(node);
    }
    /// 慢节点注入：该节点所有 RPC 先睡 d（读快路径/取消掉队者测试用）。
    pub fn delay(&self, node: &str, d: std::time::Duration) {
        self.delays.lock().unwrap().insert(node.to_string(), d);
    }
    /// 计数式失败注入（"node:op"，op ∈ prepare_meta/commit_meta/delete_version/…）：
    /// 调 N 次挡 N 发（重试两发都要挡时注册 2 次）。
    pub fn fail_once(&self, node: &str, op: &str) {
        *self
            .fail_once
            .lock()
            .unwrap()
            .entry(format!("{node}:{op}"))
            .or_insert(0) += 1;
    }

    async fn peer(&self, node: &str) -> crate::error::Result<Arc<Store>> {
        // 先落值再判：if-let 的临时 MutexGuard 会活到语句尾，跨 await 就不是 Send
        let delay = self.delays.lock().unwrap().get(node).copied();
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        if self.down.lock().unwrap().contains(node) {
            return Err(crate::error::Error::Encode(format!(
                "in-process：节点 {node} 不可达（模拟宕机）"
            )));
        }
        self.peers
            .lock()
            .unwrap()
            .get(node)
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| crate::error::Error::Encode(format!("in-process：未知节点 {node}")))
    }

    #[allow(dead_code)] // Task 8/9 的注入面
    fn take_fail(&self, node: &str, op: &str) -> bool {
        let mut m = self.fail_once.lock().unwrap();
        let k = format!("{node}:{op}");
        match m.get_mut(&k) {
            Some(c) if *c > 0 => {
                *c -= 1;
                if *c == 0 {
                    m.remove(&k);
                }
                true
            }
            _ => false,
        }
    }
}

// （in-process 传输的 14 个方法已全部落地——此前的 `unimpl` 占位助手已删）

#[async_trait::async_trait]
impl crate::mesh::MeshTransport for InProcessMesh {
    async fn health(&self, node: &str) -> crate::error::Result<crate::mesh::NodeHealth> {
        Ok(self.peer(node).await?.mesh_health().await)
    }
    // 以下 RPC 随所属 Task 落地（5a/5b/6/8/9）；在此之前显式报未实现，不静默成功。
    async fn write_shard(&self, n: &str, w: crate::mesh::ShardWrite) -> crate::error::Result<()> {
        if self.take_fail(n, "write_shard") {
            return Err(crate::error::Error::Encode(format!(
                "in-process 注入：{n} write_shard 失败"
            )));
        }
        self.peer(n).await?.mesh_write_shard(w).await
    }
    async fn read_shard(
        &self,
        n: &str,
        r: crate::mesh::ShardRead,
    ) -> crate::error::Result<Option<Bytes>> {
        self.peer(n).await?.mesh_read_shard(r).await
    }
    async fn delete_version(
        &self,
        n: &str,
        d: crate::mesh::VersionDirRef,
    ) -> crate::error::Result<()> {
        if self.take_fail(n, "delete_version") {
            return Err(crate::error::Error::Encode(format!(
                "in-process 注入：{n} delete_version 失败"
            )));
        }
        self.peer(n).await?.mesh_delete_version(d).await
    }
    async fn delete_shard(&self, n: &str, d: crate::mesh::ShardDelete) -> crate::error::Result<()> {
        self.peer(n).await?.mesh_delete_shard(d).await
    }
    async fn recycle_version(
        &self,
        n: &str,
        d: crate::mesh::VersionDirRef,
    ) -> crate::error::Result<()> {
        self.peer(n).await?.mesh_recycle_version(d).await
    }
    async fn prepare_meta(
        &self,
        n: &str,
        m: crate::mesh::MetaWrite,
    ) -> crate::error::Result<crate::mesh::MetaAck> {
        if self.take_fail(n, "prepare_meta") {
            return Err(crate::error::Error::Encode(format!(
                "in-process 注入：{n} prepare_meta 失败"
            )));
        }
        self.peer(n).await?.mesh_prepare_meta(m).await
    }
    async fn commit_meta(
        &self,
        n: &str,
        m: crate::mesh::MetaWrite,
    ) -> crate::error::Result<crate::mesh::MetaAck> {
        if self.take_fail(n, "commit_meta") {
            return Err(crate::error::Error::Encode(format!(
                "in-process 注入：{n} commit_meta 失败"
            )));
        }
        self.peer(n).await?.mesh_commit_meta(m).await
    }
    async fn abort_meta(&self, n: &str, m: crate::mesh::MetaWrite) -> crate::error::Result<()> {
        self.peer(n).await?.mesh_abort_meta(m).await
    }
    async fn read_meta(
        &self,
        n: &str,
        r: crate::mesh::MetaRead,
    ) -> crate::error::Result<crate::mesh::MetaSnapshot> {
        self.peer(n).await?.mesh_read_meta(r).await
    }
    async fn delete_meta(&self, n: &str, r: crate::mesh::MetaRead) -> crate::error::Result<()> {
        self.peer(n).await?.mesh_delete_meta(r).await
    }
    async fn ensure_bucket(&self, n: &str, b: &str) -> crate::error::Result<()> {
        self.peer(n).await?.mesh_ensure_bucket(b).await
    }
    async fn delete_bucket_dir(&self, n: &str, b: &str) -> crate::error::Result<()> {
        self.peer(n).await?.mesh_delete_bucket(b).await
    }
    async fn list_bucket_dirs(&self, n: &str) -> crate::error::Result<Vec<String>> {
        self.peer(n).await?.mesh_list_buckets().await
    }
    async fn list_local(
        &self,
        n: &str,
        l: crate::mesh::ListLocal,
    ) -> crate::error::Result<crate::mesh::ListLocalResp> {
        self.peer(n).await?.mesh_list_local(l).await
    }
    async fn lock(
        &self,
        n: &str,
        r: crate::mesh::LockReq,
    ) -> crate::error::Result<crate::mesh::LockResp> {
        if self.take_fail(n, "lock") {
            return Err(crate::error::Error::Encode(format!(
                "in-process 注入：{n} lock 失败"
            )));
        }
        self.peer(n).await?.mesh_lock(r).await
    }
    async fn control_write(
        &self,
        node: &str,
        w: crate::mesh::ControlWrite,
    ) -> crate::error::Result<()> {
        let st = self.peer(node).await?;
        st.mesh_control_write(w).await
    }

    async fn control_query(
        &self,
        n: &str,
        q: crate::mesh::ControlQuery,
    ) -> crate::error::Result<Option<crate::mesh::ControlRow>> {
        self.peer(n).await?.mesh_control_query(q).await
    }
}

/// 4×3 = 12 槽 in-process 集群（与 e2e 统一）。EC 由槽位数推导：parity 恒 4、data = 槽数 − 4。
pub struct Cluster {
    pub stores: Vec<Arc<Store>>,
    pub mesh: Arc<InProcessMesh>,
    pub ids: Vec<String>,
    /// 每节点的盘路径（分片落位断言用）
    pub dirs: Vec<Vec<std::path::PathBuf>>,
    _tmpdirs: Vec<tempfile::TempDir>, // 保活：drop 即删目录
}

impl Cluster {
    pub async fn new(nodes: usize, drives_per_node: usize) -> Cluster {
        Self::new_with_placement(nodes, drives_per_node, false).await
    }

    pub async fn new_with_placement(
        nodes: usize,
        drives_per_node: usize,
        placement_v2: bool,
    ) -> Cluster {
        let mut tmpdirs = Vec::new();
        let mut node_dirs = Vec::new();
        for _ in 0..nodes {
            let mut dirs = Vec::new();
            for _ in 0..drives_per_node {
                let td = tempfile::TempDir::new().unwrap();
                dirs.push(td.path().to_path_buf());
                tmpdirs.push(td);
            }
            node_dirs.push(dirs);
        }
        Self::open_over(node_dirs, tmpdirs, placement_v2).await
    }

    /// 在同一组盘上重开（扩容/换放置旗测试）：`dirs` 的保活由调用方（原 Cluster）负责。
    pub async fn reopen_over(dirs: Vec<Vec<std::path::PathBuf>>, placement_v2: bool) -> Cluster {
        Self::open_over(dirs, Vec::new(), placement_v2).await
    }

    async fn open_over(
        node_dirs: Vec<Vec<std::path::PathBuf>>,
        tmpdirs: Vec<tempfile::TempDir>,
        placement_v2: bool,
    ) -> Cluster {
        let nodes = node_dirs.len();
        let drives_per_node = node_dirs[0].len();
        let slot_count = nodes * drives_per_node;
        // 不变量：parity 恒 4（12 槽 → 8+4，与生产默认一致）；EC 由槽位数推导，勿硬编码
        let data = (slot_count - 4) as u8;
        assert!((1..=251).contains(&data), "槽位数 {slot_count} 非法");
        let ec = ec::EcConfig {
            data,
            parity: 4,
            ..Default::default()
        };

        let ids: Vec<String> = (0..nodes).map(|i| format!("n{}", i + 1)).collect();
        let specs: Vec<drives::NodeSpec> = ids
            .iter()
            .map(|id| drives::NodeSpec {
                id: id.clone(),
                addr: format!("127.0.0.1:1910{}", &id[1..]),
                drive_count: drives_per_node as u16,
            })
            .collect();

        let mesh = InProcessMesh::new();
        let mut stores = Vec::new();
        for (i, _id) in ids.iter().enumerate() {
            let dirs = node_dirs[i].clone();
            std::fs::create_dir_all(&dirs[0]).unwrap();
            let sets = drives::ClusterSets::single(&ids[i], specs.clone(), &ec).unwrap();
            let mut cfg = StoreConfig::new(&dirs[0]);
            cfg.drives = dirs.clone();
            cfg.ec = ec;
            cfg.fsync = false; // 测试提速（既有测试同款）
            // 集群锁参数注小值（Task 8）：TTL 400ms、3 次重试、5–10ms 抖动——速度与语义两全
            cfg.cluster_lock_ttl_ms = 400;
            cfg.cluster_lock_retries = 3;
            cfg.cluster_lock_backoff_ms = (5, 10);
            cfg.cluster = Some(Arc::new(drives::ClusterConfig {
                sets,
                transport: mesh.clone(),
                placement_v2,
                resolver: None,
            }));
            stores.push(Arc::new(Store::open(cfg).await.unwrap()));
        }
        for (id, s) in ids.iter().zip(&stores) {
            mesh.register(id, s);
        }
        Cluster {
            stores,
            mesh,
            ids,
            dirs: node_dirs,
            _tmpdirs: tmpdirs,
        }
    }
}

#[tokio::test]
async fn cluster_fixture_health_and_injections() {
    use crate::mesh::MeshTransport as _; // 方法调用需要 trait 在作用域

    let c = Cluster::new(4, 3).await;
    assert_eq!(c.ids, vec!["n1", "n2", "n3", "n4"]);
    assert_eq!(c.stores.len(), 4);

    // 健康面：本机槽位窗口 + 节点 id + 桶计数
    let h1 = c.stores[0].mesh_health().await;
    assert_eq!(h1.node_id, "n1");
    assert_eq!(h1.slots, vec![(0, true), (1, true), (2, true)]);
    assert_eq!(h1.buckets, 0);
    let h2 = c.stores[1].mesh_health().await;
    assert_eq!(h2.node_id, "n2");
    assert_eq!(h2.slots, vec![(3, true), (4, true), (5, true)]);

    // RPC 往返（经 in-process 传输）
    let peer = c.mesh.health("n2").await.unwrap();
    assert_eq!(peer.node_id, "n2");

    // 摘除 → 不可达；恢复 → 通
    c.mesh.down("n2");
    assert!(c.mesh.health("n2").await.is_err());
    c.mesh.up("n2");
    assert!(c.mesh.health("n2").await.is_ok());

    // 慢节点注入（Task 6 读快路径测试的注入面）
    c.mesh.delay("n3", std::time::Duration::from_millis(60));
    let t0 = std::time::Instant::now();
    c.mesh.health("n3").await.unwrap();
    assert!(t0.elapsed() >= std::time::Duration::from_millis(60));
}

/// 对象目录下版本目录里的全部文件名（分片落位断言用）。
async fn version_files(obj_dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(mut rd) = tokio::fs::read_dir(obj_dir).await else {
        return out;
    };
    while let Ok(Some(e)) = rd.next_entry().await {
        let name = e.file_name().to_string_lossy().into_owned();
        if name == crate::meta::META_FILE || name == crate::meta::META_NEW_FILE {
            continue;
        }
        if let Ok(mut inner) = tokio::fs::read_dir(e.path()).await {
            while let Ok(Some(f)) = inner.next_entry().await {
                out.push(f.file_name().to_string_lossy().into_owned());
            }
        }
    }
    out.sort();
    out
}

/// 对象目录下的第一个版本目录名（uuid）。
async fn first_version_dir(obj_dir: &std::path::Path) -> String {
    let mut rd = tokio::fs::read_dir(obj_dir).await.unwrap();
    while let Some(e) = rd.next_entry().await.unwrap() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name != crate::meta::META_FILE
            && name != crate::meta::META_NEW_FILE
            && e.file_type().await.unwrap().is_dir()
        {
            return name;
        }
    }
    panic!("无版本目录");
}

/// Task 5a：4×3 下 shard i → 槽 i（恰 1 片/槽），落位到对应节点的对应盘；读回一致。
#[tokio::test]
async fn cluster_write_distributes_by_slot() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    let body = vec![7u8; 200 * 1024]; // > inline 阈值 → EC 分片
    s1.put_object("bkt", "k", Bytes::from(body.clone()), None)
        .await
        .unwrap();

    for (ni, dirs) in c.dirs.iter().enumerate() {
        for (di, d) in dirs.iter().enumerate() {
            let slot = ni * 3 + di;
            let obj = crate::layout::object_dir(d, "bkt", "k").unwrap();
            let files = version_files(&obj).await;
            assert!(
                files.contains(&format!("part.1.{slot}")),
                "节点 n{} 盘 {di} 应有 part.1.{slot}，实得 {files:?}",
                ni + 1
            );
            assert_eq!(files.len(), 1, "每槽恰 1 片（实得 {files:?}）");
        }
    }
    let (info, got) = s1.get_object("bkt", "k").await.unwrap();
    assert_eq!(info.size, body.len() as u64);
    assert_eq!(got.to_vec(), body);
}

/// M4 切片 8 核心验收：2 set（**同 id 跨 set**——直接压 flat_of 组内序→扁平盘映射，洞 1）。
/// ec 8+4：每 set 12 槽（3 节点 × 4 盘 ≤ parity 4）；节点 n1..n3 在两 set 各 4 盘（共 8 盘/节点，
/// data_dirs = set0 段[0..4] + set1 段[4..8]）。断言：
/// ① set1 桶（b1）分片只落 set1 段（含逐槽号）；② set0 桶（b0）反之；
/// ③ 跨节点读一致（含从同物理节点的另一段读）；④ 命名空间镜像：meta 落全部 8 盘（S-L 口径）。
#[tokio::test]
async fn multiset_isolation_flat_mapping_and_mirror() {
    #[derive(Debug)]
    struct FixedResolver;
    #[async_trait::async_trait]
    impl crate::mesh::SetResolver for FixedResolver {
        async fn resolve_bucket_set(&self, bucket: &str) -> crate::error::Result<Option<u32>> {
            // bkt1 → set1；bkt0 → set0；其余 → 「全可达且确无行」
            Ok(match bucket {
                "bkt1" => Some(1),
                "bkt0" => Some(0),
                _ => None,
            })
        }
    }

    let ec = ec::EcConfig {
        data: 8,
        parity: 4,
        enabled: true,
    };
    let n_nodes = 3usize;
    let ids: Vec<String> = (1..=n_nodes).map(|i| format!("n{i}")).collect();
    let mut tmpdirs = Vec::new();
    let mut node_dirs: Vec<Vec<std::path::PathBuf>> = Vec::new();
    for _ in 0..n_nodes {
        let mut dirs = Vec::new();
        for _ in 0..8 {
            let td = tempfile::TempDir::new().unwrap();
            dirs.push(td.path().to_path_buf());
            tmpdirs.push(td);
        }
        node_dirs.push(dirs);
    }
    let mk = |i: usize| drives::NodeSpec {
        id: ids[i].clone(),
        addr: format!("127.0.0.1:{}", 26000 + i),
        drive_count: 4,
    };
    let sets_nodes = vec![vec![mk(0), mk(1), mk(2)], vec![mk(0), mk(1), mk(2)]];

    let mesh = InProcessMesh::new();
    let mut stores = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        std::fs::create_dir_all(&node_dirs[i][0]).unwrap();
        let sets = drives::ClusterSets::new(id, sets_nodes.clone(), &ec).unwrap();
        let mut cfg = StoreConfig::new(&node_dirs[i][0]);
        cfg.drives = node_dirs[i].clone();
        cfg.ec = ec;
        cfg.fsync = false;
        cfg.cluster_lock_ttl_ms = 400;
        cfg.cluster_lock_retries = 3;
        cfg.cluster_lock_backoff_ms = (5, 10);
        cfg.cluster = Some(Arc::new(drives::ClusterConfig {
            sets,
            // v1 恒等放置：slot = shard % 12 —— 每片 shard i 落槽 i，断言可逐步推导
            placement_v2: false,
            transport: mesh.clone(),
            resolver: Some(Arc::new(FixedResolver)),
        }));
        let st = Arc::new(Store::open(cfg).await.unwrap());
        mesh.register(id, &st);
        stores.push(st);
    }
    // 建两桶（命名空间面：目录镜像全盘全节点）
    stores[0].create_bucket("bkt0").await.unwrap();
    stores[0].create_bucket("bkt1").await.unwrap();
    let body = vec![5u8; 200 * 1024]; // > 内联阈值 → EC 分片
    stores[0]
        .put_object("bkt1", "k1", Bytes::from(body.clone()), None)
        .await
        .unwrap();
    stores[1]
        .put_object("bkt0", "k0", Bytes::from(body.clone()), None)
        .await
        .unwrap();

    for (ni, dirs) in node_dirs.iter().enumerate() {
        for (di, d) in dirs.iter().enumerate() {
            let seg_is_set1 = di >= 4;
            let obj1 = crate::layout::object_dir(d, "bkt1", "k1").unwrap();
            let obj0 = crate::layout::object_dir(d, "bkt0", "k0").unwrap();
            let f1 = version_files(&obj1).await;
            let f0 = version_files(&obj0).await;
            let meta1 = tokio::fs::try_exists(obj1.join("xl.meta")).await.unwrap();
            let meta0 = tokio::fs::try_exists(obj0.join("xl.meta")).await.unwrap();
            if seg_is_set1 {
                // b1 属 set1：本段应持有槽 ni*4+ord 的那片
                let slot = ni * 4 + (di - 4);
                assert!(
                    f1.contains(&format!("part.1.{slot}")),
                    "bkt1：n{} set1 段盘 {di} 应有 part.1.{slot}，实得 {f1:?}",
                    ni + 1
                );
                assert_eq!(
                    f1.len(),
                    1,
                    "bkt1：每槽恰 1 片（n{} 盘 {di}：{f1:?}）",
                    ni + 1
                );
                assert!(
                    f0.is_empty(),
                    "bkt0(属 set0)：set1 段盘 {di} 不应有分片：{f0:?}"
                );
            } else {
                let slot = ni * 4 + di;
                assert!(
                    f0.contains(&format!("part.1.{slot}")),
                    "bkt0：n{} set0 段盘 {di} 应有 part.1.{slot}，实得 {f0:?}",
                    ni + 1
                );
                assert_eq!(
                    f0.len(),
                    1,
                    "bkt0：每槽恰 1 片（n{} 盘 {di}：{f0:?}）",
                    ni + 1
                );
                assert!(
                    f1.is_empty(),
                    "bkt1(属 set1)：set0 段盘 {di} 不应有分片：{f1:?}"
                );
            }
            // ④ 命名空间镜像：meta 应落全部 8 盘（S-L：目录/meta 镜像，隔离 = 分片不落别 set）
            assert!(meta1 && meta0, "meta 应镜像到 n{} 盘 {di}", ni + 1);
        }
    }

    // ③ 跨节点读一致（从 n3 读两桶：b1 的分片散在 set1 段，含 n3 本机 set1 段 flat_of 命中）
    let (_, got1) = stores[2].get_object("bkt1", "k1").await.unwrap();
    assert_eq!(got1.to_vec(), body);
    let (_, got0) = stores[2].get_object("bkt0", "k0").await.unwrap();
    assert_eq!(got0.to_vec(), body);

    // ⑤ 指纹盘上内容（S-G 口径）：set0 段盘 = v2、set1 段盘 = v3 set=1
    let tf0 = tokio::fs::read_to_string(crate::layout::sys_dir(&node_dirs[0][0], "topo"))
        .await
        .unwrap();
    let tf1 = tokio::fs::read_to_string(crate::layout::sys_dir(&node_dirs[0][4], "topo"))
        .await
        .unwrap();
    assert_eq!(tf0.trim(), "v2 node=n1 slot=0", "set0 段盘记录");
    assert_eq!(
        tf1.trim(),
        "v3 set=1 node=n1 slot=0",
        "set1 段盘记录（组内槽 0）"
    );

    // ⑥ resolver 返回 None（全可达且确无行）→ 放置 fail-closed 为 NoSuchBucket
    //（桶目录已建：ensure_bucket 过关后由 set 解析兜底——绝不静默回落 set0）
    stores[0].create_bucket("bkt-miss").await.unwrap();
    let err = stores[0]
        .put_object("bkt-miss", "kx", Bytes::from(vec![1u8; 200 * 1024]), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::NoSuchBucket(_)),
        "无行桶应 NoSuchBucket（实得 {err:?}）"
    );
}

/// M5 切片 0（P4-1）：桶行广播——create/delete 后各 peer 的控制面后端立即收到写（免重启收敛）。
#[tokio::test]
async fn bucket_row_broadcast_reaches_all_peers() {
    #[derive(Debug, Default)]
    struct CollectBackend {
        log: std::sync::Mutex<Vec<(String, Option<u16>)>>,
    }
    #[async_trait::async_trait]
    impl crate::mesh::ControlBackend for CollectBackend {
        async fn control_query(
            &self,
            _q: crate::mesh::ControlQuery,
        ) -> crate::error::Result<Option<crate::mesh::ControlRow>> {
            Ok(None)
        }
        async fn control_write(&self, w: crate::mesh::ControlWrite) -> crate::error::Result<()> {
            // 单变体枚举：直接解构；加新变体时编译器会强制此桩更新（白名单语义）
            let crate::mesh::ControlWrite::BucketRowSet { bucket, set } = w;
            self.log.lock().unwrap().push((bucket, set));
            Ok(())
        }
    }

    let ec = ec::EcConfig {
        data: 1,
        parity: 1,
        enabled: true,
    };
    let ids: Vec<String> = (1..=3).map(|i| format!("n{i}")).collect();
    let specs: Vec<drives::NodeSpec> = ids
        .iter()
        .map(|id| drives::NodeSpec {
            id: id.clone(),
            addr: format!("127.0.0.1:2710{}", &id[1..]),
            drive_count: 1,
        })
        .collect();
    let mesh = InProcessMesh::new();
    let mut stores = Vec::new();
    let mut backends: Vec<std::sync::Arc<CollectBackend>> = Vec::new();
    let mut tmpdirs = Vec::new();
    for id in &ids {
        let td = tempfile::TempDir::new().unwrap();
        let dir = td.path().to_path_buf();
        tmpdirs.push(td);
        std::fs::create_dir_all(&dir).unwrap();
        let back = std::sync::Arc::new(CollectBackend::default());
        let sets = drives::ClusterSets::single(id, specs.clone(), &ec).unwrap();
        let mut cfg = StoreConfig::new(&dir);
        cfg.ec = ec;
        cfg.fsync = false;
        cfg.cluster_lock_ttl_ms = 400;
        cfg.cluster_lock_retries = 3;
        cfg.cluster_lock_backoff_ms = (5, 10);
        cfg.control = Some(back.clone());
        cfg.cluster = Some(Arc::new(drives::ClusterConfig {
            sets,
            placement_v2: false,
            transport: mesh.clone(),
            resolver: None,
        }));
        let st = Arc::new(Store::open(cfg).await.unwrap());
        mesh.register(id, &st);
        stores.push(st);
        backends.push(back);
    }

    // 机制本体：broadcast_bucket_row 应把写投到全部 peer 的后端（best-effort 面）。
    // 注：S3 面/控制面的 4 个调用点（create/delete × 两面）是薄封装，归 verifier 读码 +
    // 后续 e2e 断言；本测钉传输与分发机制。
    stores[0].broadcast_bucket_row("bkt-row", Some(0)).await;
    for b in &backends[1..] {
        let log = b.log.lock().unwrap();
        assert!(
            log.iter().any(|(n, s)| n == "bkt-row" && *s == Some(0)),
            "peer 应立即收到建桶行（单 set → set0）：{log:?}"
        );
    }
    // 本节点不发给自己（发起端直写本地 DB）
    assert!(backends[0].log.lock().unwrap().is_empty());

    stores[0].broadcast_bucket_row("bkt-row", None).await;
    for b in &backends[1..] {
        let log = b.log.lock().unwrap();
        assert!(
            log.iter().any(|(n, s)| n == "bkt-row" && s.is_none()),
            "peer 应立即收到删行：{log:?}"
        );
    }
    // 宕机 peer：广播 best-effort，不 panic 不上抛（回填对账兜底）
    mesh.down("n2");
    stores[0].broadcast_bucket_row("bkt-dead", Some(1)).await;
    assert!(
        backends[2]
            .log
            .lock()
            .unwrap()
            .iter()
            .any(|(n, s)| n == "bkt-dead" && *s == Some(1)),
        "其余 peer 不受单点不可达影响"
    );
}

/// Task 5a/5b：摘 1 节点（剩 9 槽=恰好 quorum）→ 写成功；读经重建成功。
#[tokio::test]
async fn cluster_write_with_node_down_and_read_reconstructs() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    c.mesh.down("n2"); // 槽 3,4,5 离线
    let body = vec![9u8; 150 * 1024];
    s1.put_object("bkt", "k", Bytes::from(body.clone()), None)
        .await
        .unwrap();
    let (_, got) = s1.get_object("bkt", "k").await.unwrap();
    assert_eq!(got.to_vec(), body);
    for (ni, dirs) in c.dirs.iter().enumerate() {
        for d in dirs {
            let files = version_files(&crate::layout::object_dir(d, "bkt", "k").unwrap()).await;
            if ni == 1 {
                assert!(files.is_empty(), "n2 离线不应有分片");
            } else {
                assert_eq!(files.len(), 1);
            }
        }
    }
}

/// Task 5a：覆盖写后远端旧版本进该节点 trash（先决链：提交 quorum 成立回收才安全）。
#[tokio::test]
async fn overwrite_recycles_remote_old_version() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from(vec![1u8; 200 * 1024]), None)
        .await
        .unwrap();
    let obj = crate::layout::object_dir(&c.dirs[0][0], "bkt", "k").unwrap();
    let v1 = first_version_dir(&obj).await;
    s1.put_object("bkt", "k", Bytes::from(vec![2u8; 200 * 1024]), None)
        .await
        .unwrap();
    for d in &c.dirs[1] {
        let trash = crate::layout::sys_dir(d, "trash").join(&v1);
        assert!(
            tokio::fs::try_exists(&trash).await.unwrap(),
            "远端旧版本未进 trash: {}",
            trash.display()
        );
    }
    // 读回的是新版本
    let (_, got) = s1.get_object("bkt", "k").await.unwrap();
    assert_eq!(got.to_vec(), vec![2u8; 200 * 1024]);
}

/// Task 6（C4）：连续覆盖写，mod_time 严格递增（与墙钟解耦）。
#[tokio::test]
async fn cluster_mod_time_is_monotonic() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from_static(b"v1"), None)
        .await
        .unwrap();
    let (i1, _) = s1.get_object("bkt", "k").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from_static(b"v2"), None)
        .await
        .unwrap();
    let (i2, _) = s1.get_object("bkt", "k").await.unwrap();
    assert!(
        i2.last_modified_nanos > i1.last_modified_nanos,
        "mod_time 必须严格递增：{} -> {}",
        i1.last_modified_nanos,
        i2.last_modified_nanos
    );
}

/// Task 6（评审 P1）：部分翻转（3 远端两发全挡 → flipped=3 < Q_s=9）→ 5xx 且全节点零残留。
#[tokio::test]
async fn partial_flip_compensates_and_leaves_no_residue() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    for n in ["n2", "n3", "n4"] {
        c.mesh.fail_once(n, "commit_meta");
        c.mesh.fail_once(n, "commit_meta"); // 重试的一发也要挡
    }
    let err = s1
        .put_object("bkt", "k", Bytes::from(vec![5u8; 128 * 1024]), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::QuorumUnmet(_)),
        "应 QuorumUnmet：{err:?}"
    );
    // 任何节点都不该见到这次失败的写（补偿 + 数据回滚干净）
    for s in &c.stores {
        let r = s.get_object("bkt", "k").await;
        assert!(
            matches!(r, Err(crate::error::Error::NoSuchKey(_))),
            "失败写泄漏可见：{r:?}"
        );
    }
    for dirs in &c.dirs {
        for d in dirs {
            let files = version_files(&crate::layout::object_dir(d, "bkt", "k").unwrap()).await;
            assert!(files.is_empty(), "残留：{files:?}");
        }
    }
}

/// Task 6（D1）：覆盖不足 quorum → fail-closed（Err(CorruptMeta)，验收⑤的 500 口径）。
#[tokio::test]
async fn read_coverage_below_quorum_fails_closed() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from(vec![3u8; 200 * 1024]), None)
        .await
        .unwrap();
    c.mesh.down("n3");
    c.mesh.down("n4"); // 覆盖 6 < 9
    let err = s1.get_object("bkt", "k").await.unwrap_err();
    assert!(
        matches!(err, crate::error::Error::CorruptMeta(_)),
        "应 CorruptMeta（fail-closed，非 404）：{err:?}"
    );
}

/// Task 6（D1）：meta 读快路径取消掉队者（慢节点不拖读延迟）。
#[tokio::test]
async fn read_meta_fast_path_cancels_straggler() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from(vec![4u8; 200 * 1024]), None)
        .await
        .unwrap();
    c.mesh.delay("n4", std::time::Duration::from_millis(800));
    let t0 = std::time::Instant::now();
    let m = s1.read_meta("bkt", "k").await.unwrap();
    let dt = t0.elapsed();
    assert!(m.is_some());
    // 覆盖 3+3+3=9 即停，不等 n4（否则 ≥800ms）
    assert!(
        dt < std::time::Duration::from_millis(500),
        "快路径被掉队者拖累：{dt:?}"
    );
}

/// Task 6：建桶广播全节点 + 全局存在性（任一节点视角一致）+ fail-closed（不可达即拒）。
#[tokio::test]
async fn cluster_bucket_create_fanout_exists_and_fail_closed() {
    let c = Cluster::new(4, 3).await;
    c.stores[0].create_bucket("bkt").await.unwrap();
    // 每个节点的每块盘上都有桶目录
    for dirs in &c.dirs {
        for d in dirs {
            assert!(
                tokio::fs::try_exists(crate::layout::bucket_dir(d, "bkt"))
                    .await
                    .unwrap()
            );
        }
    }
    // 全局存在性：任一节点视角都为 true
    for s in &c.stores {
        assert!(s.bucket_exists("bkt").await.unwrap());
    }
    // 重复建桶（从另一节点视角）→ BucketAlreadyExists
    let err = c.stores[2].create_bucket("bkt").await.unwrap_err();
    assert!(
        matches!(err, crate::error::Error::BucketAlreadyExists(_)),
        "{err:?}"
    );
    // 不可达 → fail-closed（静默部分建会让缺桶节点 404）
    c.mesh.down("n3");
    let err = c.stores[0].create_bucket("bkt2").await.unwrap_err();
    assert!(
        format!("{err}").contains("不可达") || format!("{err}").contains("in-process"),
        "{err:?}"
    );
}

/// Task 6：删桶全节点空检 + 全局删除；非空/不可达都拒。
#[tokio::test]
async fn cluster_delete_bucket_all_nodes_and_guards() {
    let c = Cluster::new(4, 3).await;
    c.stores[0].create_bucket("bkt").await.unwrap();
    // 非空 → BucketNotEmpty
    c.stores[0]
        .put_object("bkt", "k", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    let err = c.stores[1].delete_bucket("bkt").await.unwrap_err();
    assert!(
        matches!(err, crate::error::Error::BucketNotEmpty(_)),
        "{err:?}"
    );
    c.stores[0].delete_object("bkt", "k").await.unwrap();
    // 不可达 → 拒
    c.mesh.down("n4");
    let err = c.stores[0].delete_bucket("bkt").await.unwrap_err();
    assert!(
        format!("{err}").contains("in-process") || format!("{err}").contains("不可达"),
        "{err:?}"
    );
    c.mesh.up("n4");
    // 全上 → 空桶删成功，全节点消失
    c.stores[0].delete_bucket("bkt").await.unwrap();
    for s in &c.stores {
        assert!(!s.bucket_exists("bkt").await.unwrap());
    }
}

/// Task 6：删对象跨节点清 meta 与目录（全部节点不可见）。
#[tokio::test]
async fn cluster_delete_object_clears_all_nodes() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from(vec![6u8; 200 * 1024]), None)
        .await
        .unwrap();
    s1.delete_object("bkt", "k").await.unwrap();
    for s in &c.stores {
        let r = s.get_object("bkt", "k").await;
        assert!(
            matches!(r, Err(crate::error::Error::NoSuchKey(_))),
            "未全清：{r:?}"
        );
    }
    for dirs in &c.dirs {
        for d in dirs {
            let files = version_files(&crate::layout::object_dir(d, "bkt", "k").unwrap()).await;
            assert!(files.is_empty(), "残留：{files:?}");
        }
    }
}

/// Task 6（D4）：掉线期写入的对象，回归节点 LIST 仍见（并集）；分页不丢键。
#[tokio::test]
async fn cluster_list_union_and_pagination() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    // n2 掉线期写入：n2 本地无任何痕迹
    c.mesh.down("n2");
    s1.put_object("bkt", "missed", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    c.mesh.up("n2");
    // n2 视角 LIST 也能见到（并集；本机页为空 → 远端条目）
    let from_n2 = c.stores[1]
        .list_objects("bkt", &Default::default())
        .await
        .unwrap();
    assert!(
        from_n2.objects.iter().any(|o| o.key == "missed"),
        "并集应见掉线期写入：{:?}",
        from_n2.objects.iter().map(|o| &o.key).collect::<Vec<_>>()
    );
    // 分页完备性：5 个键、max_keys=2 逐页枚举，恰好一次、无丢失
    for i in 0..5 {
        s1.put_object("bkt", &format!("k{i}"), Bytes::from_static(b"y"), None)
            .await
            .unwrap();
    }
    let mut seen: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    for _page in 0..10 {
        let req = crate::store::ListRequest {
            max_keys: Some(2),
            continuation_token: token.clone(),
            ..Default::default()
        };
        let page = s1.list_objects("bkt", &req).await.unwrap();
        seen.extend(page.objects.iter().map(|o| o.key.clone()));
        if !page.is_truncated {
            break;
        }
        token = page.next_token.clone();
        assert!(token.is_some(), "截断必须有续页 token");
    }
    seen.sort();
    let mut expect: Vec<String> = (0..5).map(|i| format!("k{i}")).collect();
    expect.push("missed".into());
    expect.sort();
    assert_eq!(seen, expect, "分页枚举应恰好一次全覆盖");
}

/// Task 7：fsck 差集检测 + 缺键补建闭环（验收② in-process 版）。
#[tokio::test]
async fn cluster_fsck_detects_and_repairs_missing_meta() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    // n2 掉线期写入：n2 本地无任何痕迹
    c.mesh.down("n2");
    s1.put_object("bkt", "missed", Bytes::from(vec![8u8; 200 * 1024]), None)
        .await
        .unwrap();
    c.mesh.up("n2");

    // 巡检（不修）：从**缺键的一方（n2）**跑——差集应报出缺键；n2 的盘上确实没有 meta
    let rep = c.stores[1].fsck(false).await.unwrap();
    assert_eq!(rep.unreachable_nodes, Vec::<String>::new());
    assert_eq!(rep.missing_meta_keys, vec!["bkt/missed".to_string()]);
    assert_eq!(rep.repaired_meta_keys, 0);
    for d in &c.dirs[1] {
        let meta_path = crate::layout::object_dir(d, "bkt", "missed")
            .unwrap()
            .join("xl.meta");
        assert!(
            !tokio::fs::try_exists(&meta_path).await.unwrap(),
            "修复前不该有 meta"
        );
    }

    // --repair：补建 meta 到本机各槽；随后 n2 的读路径自持（分片补建靠读时修）
    let rep = c.stores[1].fsck(true).await.unwrap();
    assert_eq!(rep.repaired_meta_keys, 1);
    for d in &c.dirs[1] {
        let meta_path = crate::layout::object_dir(d, "bkt", "missed")
            .unwrap()
            .join("xl.meta");
        assert!(
            tokio::fs::try_exists(&meta_path).await.unwrap(),
            "修复后应有 meta"
        );
    }
    // 再巡检：差集归零
    let rep = c.stores[1].fsck(false).await.unwrap();
    assert_eq!(rep.missing_meta_keys, Vec::<String>::new());
    // 从 n2 读也通（触发其读时修分片）
    let (_, got) = c.stores[1].get_object("bkt", "missed").await.unwrap();
    assert_eq!(got.len(), 200 * 1024);
}

/// Task 7：gc 保守门——任一节点不可达，对象目录回收整体跳过（trash/tmp 照常）。
#[tokio::test]
async fn cluster_gc_skips_object_sweep_when_node_down() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from(vec![1u8; 200 * 1024]), None)
        .await
        .unwrap();
    // 造一个孤儿版本目录（mtime 新鲜 → due 取未来即可回收）
    let obj = crate::layout::object_dir(&c.dirs[0][0], "bkt", "k").unwrap();
    let orphan = obj.join("deadbeef-orphan");
    tokio::fs::create_dir_all(&orphan).await.unwrap();
    let due = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);

    c.mesh.down("n3");
    let n = s1.reap_stale(due).await.unwrap();
    assert!(
        tokio::fs::try_exists(&orphan).await.unwrap(),
        "节点不可达时不得删孤儿"
    );
    let _ = n;

    c.mesh.up("n3");
    s1.reap_stale(due).await.unwrap();
    assert!(
        !tokio::fs::try_exists(&orphan).await.unwrap(),
        "全上后孤儿应被回收"
    );
}

/// Task 6 微测（延期补齐）：`.new` 被他人内容覆盖 → commit 回报的 hash 不是我们的。
#[tokio::test]
async fn foreign_new_content_hash_not_counted() {
    use crate::mesh::MeshTransport as _;
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from_static(b"v1"), None)
        .await
        .unwrap();
    // 取一份合法 meta 字节当「我们的新内容」
    let meta_bytes = tokio::fs::read(
        crate::layout::object_dir(&c.dirs[1][0], "bkt", "k")
            .unwrap()
            .join("xl.meta"),
    )
    .await
    .unwrap();
    let mw = crate::mesh::MetaWrite {
        bucket: "bkt".into(),
        key: "k".into(),
        meta: Bytes::from(meta_bytes),
    };
    // prepare 到 n2（写 .new = 我们的内容）
    let ack = c.mesh.prepare_meta("n2", mw.clone()).await.unwrap();
    assert!(ack.slots.iter().any(|s| s.ok));
    // 篡改：把 n2 各盘 .new 换成他人内容（无 fencing 交错写的模拟）
    for d in &c.dirs[1] {
        let p = crate::layout::object_dir(d, "bkt", "k")
            .unwrap()
            .join("xl.meta.new");
        if tokio::fs::try_exists(&p).await.unwrap() {
            tokio::fs::write(&p, b"foreign-content").await.unwrap();
        }
    }
    // commit：翻成功但内容是他人写入 → hash 不符（协调者不计入 quorum）
    let ack = c.mesh.commit_meta("n2", mw.clone()).await.unwrap();
    let ours = crate::mesh::content_hash(&mw.meta);
    assert!(
        ack.slots.iter().any(|s| s.ok),
        "翻转本身应成功（翻的是他人内容）"
    );
    assert!(
        ack.slots.iter().all(|s| !s.ok || s.hash != ours),
        "他人内容不得计为我们的翻转"
    );
}

/// Task 6 微测（延期补齐）：full 模式收满全部源（回退一次的机制面）。
#[tokio::test]
async fn read_meta_full_waits_all_sources() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    s1.put_object("bkt", "k", Bytes::from(vec![2u8; 200 * 1024]), None)
        .await
        .unwrap();
    c.mesh.delay("n4", std::time::Duration::from_millis(300));
    let t0 = std::time::Instant::now();
    let fast = s1.read_meta("bkt", "k").await.unwrap();
    assert!(fast.is_some());
    assert!(
        t0.elapsed() < std::time::Duration::from_millis(200),
        "快路径不等 n4"
    );
    let t0 = std::time::Instant::now();
    let full = s1.read_meta_full("bkt", "k").await.unwrap();
    assert!(full.is_some());
    assert!(
        t0.elapsed() >= std::time::Duration::from_millis(300),
        "full 应收满全部源"
    );
}

/// Task 2 回补：拓扑指纹——data_dirs 顺序变化 = 槽位静默错位 → 拒开。
#[tokio::test]
async fn cluster_topology_fingerprint_detects_reorder() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut dirs: Vec<std::path::PathBuf> = Vec::new();
    for i in 0..3 {
        let d = tmp.path().join(format!("d{i}"));
        std::fs::create_dir_all(&d).unwrap();
        dirs.push(d);
    }
    let specs = vec![
        drives::NodeSpec {
            id: "n1".into(),
            addr: "a".into(),
            drive_count: 3,
        },
        drives::NodeSpec {
            id: "n2".into(),
            addr: "b".into(),
            drive_count: 3,
        },
        drives::NodeSpec {
            id: "n3".into(),
            addr: "c".into(),
            drive_count: 3,
        },
        drives::NodeSpec {
            id: "n4".into(),
            addr: "d".into(),
            drive_count: 3,
        },
    ];
    let mk_cfg = |order: Vec<std::path::PathBuf>| {
        let sets =
            drives::ClusterSets::single("n1", specs.clone(), &ec::EcConfig::default()).unwrap();
        let mesh = InProcessMesh::new();
        let mut cfg = StoreConfig::new(&order[0]);
        cfg.drives = order;
        cfg.fsync = false;
        cfg.cluster = Some(Arc::new(drives::ClusterConfig {
            sets,
            transport: mesh,
            placement_v2: false,
            resolver: None,
        }));
        cfg
    };
    // 首开：写指纹
    Store::open(mk_cfg(dirs.clone())).await.unwrap();
    // 顺序交换 → 大声失败
    let mut swapped = dirs.clone();
    swapped.swap(0, 1);
    let err = match Store::open(mk_cfg(swapped)).await {
        Ok(_) => panic!("顺序交换后不应打开成功"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("拓扑指纹"), "实得：{err}");
}

/// Task 8：两方抢同一键——恰一方持有（节点域 Q_n），另一方重试耗尽；释放后可取得。
#[tokio::test]
async fn cluster_lock_single_winner_then_takeover() {
    let c = Cluster::new(4, 3).await;
    let a = c.stores[0].clone();
    let b = c.stores[1].clone();
    let ha = a.acquire_cluster_lock("bkt", "k").await.unwrap();
    // 竞争者重试耗尽 → QuorumUnmet（互斥：a 持 3 票，剩 1 票拿不到 Q_n）
    let err = match b.acquire_cluster_lock("bkt", "k").await {
        Ok(_) => panic!("竞争者不应拿到锁"),
        Err(e) => e,
    };
    assert!(
        matches!(err, crate::error::Error::QuorumUnmet(_)),
        "{err:?}"
    );
    let (_, lock_unmet) = b.quorum_unmet_counts();
    assert!(lock_unmet >= 1);
    drop(ha); // 释放：本机表同步清 + 广播
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let hb = b.acquire_cluster_lock("bkt", "k").await.unwrap();
    drop(hb);
}

/// Task 8：授予规则原语——活租约不可被更大 epoch 抢占；过期按 epoch 定序；迟到旧 epoch 不抢回。
#[tokio::test]
async fn lock_grant_rules_primitive() {
    use crate::mesh::LockOp;
    let t = crate::store::lock::LockTable::default();
    let mk = |holder: &str, epoch: i64, op: LockOp| crate::mesh::LockReq {
        bucket: "b".into(),
        key: "k".into(),
        epoch_nanos: epoch,
        epoch_node: holder.into(),
        holder: holder.into(),
        ttl_ms: 1000,
        op,
    };
    let ttl = std::time::Duration::from_millis(1000);
    let zero = std::time::Duration::ZERO;
    // 首授 → 活租约 + 更大 epoch 竞争 → 拒（绝不抢活锁）
    assert!(t.apply("b/k", &mk("A", 100, LockOp::Acquire), ttl));
    assert!(!t.apply("b/k", &mk("B", 10_000, LockOp::Acquire), ttl));
    // 持有者续期 → 过期（zero ttl）→ 更大 epoch 替换
    assert!(t.apply("b/k", &mk("A", 100, LockOp::Renew), ttl));
    assert!(t.apply("b/k", &mk("A", 100, LockOp::Renew), zero)); // 续成但即刻过期
    assert!(t.apply("b/k", &mk("C", 200, LockOp::Acquire), zero));
    // 过期态下迟到的旧 epoch（150 < 200）不得抢回；更大 epoch（300）可替换
    assert!(t.apply("b/k", &mk("C", 200, LockOp::Renew), zero));
    assert!(!t.apply("b/k", &mk("D", 150, LockOp::Acquire), zero));
    assert!(t.apply("b/k", &mk("D", 300, LockOp::Acquire), zero));
    // 非持有者 Release 忽略；持有者 Release 释放
    assert!(!t.apply("b/k", &mk("E", 400, LockOp::Release), zero));
    assert!(t.apply("b/k", &mk("D", 300, LockOp::Release), zero));
    assert_eq!(t.holder("b/k"), None);
}

/// Task 8：续租保锁——持有者慢写（超 TTL）期间租约被续住，竞争者拿不到。
#[tokio::test]
async fn slow_holder_keeps_lease_via_renew() {
    let c = Cluster::new(4, 3).await;
    let a = c.stores[0].clone();
    let b = c.stores[2].clone();
    let ha = a.acquire_cluster_lock("bkt", "k").await.unwrap();
    // TTL=400ms（fixture），睡 700ms（跨多个续租周期 133ms）——续租任务应保锁
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    assert!(!ha.lease_lost(), "续租应成功（慢写持锁）");
    let err = match b.acquire_cluster_lock("bkt", "k").await {
        Ok(_) => panic!("租约被续住时竞争者不应拿到锁"),
        Err(e) => e,
    };
    assert!(matches!(err, crate::error::Error::QuorumUnmet(_)));
    drop(ha);
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    drop(b.acquire_cluster_lock("bkt", "k").await.unwrap());
}

/// Task 8：脑裂——多数不可达时 acquire 重试耗尽拒绝写（验收⑤ in-process 版）。
#[tokio::test]
async fn split_brain_rejects_writes() {
    let c = Cluster::new(4, 3).await;
    let s1 = &c.stores[0];
    s1.create_bucket("bkt").await.unwrap();
    c.mesh.down("n3");
    c.mesh.down("n4"); // 只剩 n1+n2 = 2 < Q_n=3
    let err = s1
        .put_object("bkt", "k", Bytes::from_static(b"x"), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::error::Error::QuorumUnmet(_)),
        "分区下写必须被拒：{err:?}"
    );
    let (_, lock_unmet) = s1.quorum_unmet_counts();
    assert!(lock_unmet >= 1);
    // 恢复多数 → 写恢复
    c.mesh.up("n3");
    c.mesh.up("n4");
    s1.put_object("bkt", "k", Bytes::from_static(b"y"), None)
        .await
        .unwrap();
}

/// Task 8：Q_n 数学——严格多数（任意两个 quorum 集必相交，评审「Renew 同口径」的依据）。
#[tokio::test]
async fn qn_is_strict_majority() {
    let c = Cluster::new(4, 3).await;
    assert_eq!(c.stores[0].q_n(), 3); // 4 → 4/2+1 = 3
    for n in 4..=7usize {
        let q = n / 2 + 1;
        assert!(2 * q > n, "n={n} q={q} 必须严格多数（交集非空）");
    }
}

/// 版本控制链全流程（M4 切片 3）：PUT×3 → 点名读/区间读 → delete marker →
/// 删 marker 恢复 → 点名删版本（目录进 trash）→ 分页 → 链清空整键清理 → 幂等。
#[tokio::test]
async fn versioning_chain_full_flow() {
    use super::version::DeleteOutcome;
    fn stream_of(b: Bytes) -> ByteStream {
        Box::pin(futures::stream::once(async move { Ok(b) }))
    }
    let (s, d) = store().await;
    s.create_bucket("alpha").await.unwrap();
    let a = payload(300_000);
    let b = payload(310_001);
    let c = payload(320_002);
    for body in [a.clone(), b.clone(), c.clone()] {
        s.put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(body),
            None,
            Default::default(),
            None,
            true, // versioned
            None,
            None,
        )
        .await
        .unwrap();
    }
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], &c[..], "普通读 = 最新版本");

    let vl = s
        .list_object_versions("alpha", "", 1000, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries.len(), 3);
    assert_eq!(
        vl.entries.iter().map(|e| e.size).collect::<Vec<_>>(),
        vec![c.len() as u64, b.len() as u64, a.len() as u64],
        "新→旧"
    );
    assert!(vl.entries[0].is_latest && !vl.entries[1].is_latest);
    let v_ids: Vec<String> = vl.entries.iter().map(|e| e.version_id.clone()).collect();
    assert!(
        v_ids.iter().all(|v| v != "null" && v.len() == 36),
        "版本控制版本号应为真 uuid：{v_ids:?}"
    );

    // 点名读 v2（整读 + 区间）
    let (info2, got2) = s.get_object_version("alpha", "k", &v_ids[1]).await.unwrap();
    assert_eq!(info2.size, b.len() as u64);
    assert_eq!(&got2[..], &b[..]);
    let r = s
        .get_object_range_version("alpha", "k", &v_ids[1], 5..25)
        .await
        .unwrap();
    assert_eq!(&r[..], &b[5..25]);
    // head 指定版本
    let h1 = s
        .head_object_version("alpha", "k", &v_ids[2])
        .await
        .unwrap();
    assert_eq!(h1.size, a.len() as u64);

    // delete marker：普通读 404、对象列表不见、版本列表见 marker 为 latest
    let out = s
        .delete_object_versioned("alpha", "k", None, true)
        .await
        .unwrap();
    let marker_id = match out {
        DeleteOutcome::Marker { version_id } => version_id,
        other => panic!("应得 Marker，实得 {other:?}"),
    };
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
    let ol = s
        .list_objects("alpha", &ListRequest::default())
        .await
        .unwrap();
    assert_eq!(ol.objects.len(), 0, "marker 遮住整键");
    let vl = s
        .list_object_versions("alpha", "", 1000, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries.len(), 4);
    assert!(vl.entries[0].is_marker && vl.entries[0].is_latest);
    assert!(matches!(
        s.get_object_version("alpha", "k", &marker_id).await,
        Err(Error::DeleteMarker(_))
    ));

    // 删 marker → 恢复可见（读回 c）；同时验证 rank 防倒退（verifier #2）：
    // 链重写必须把修订序号抬到旧链 max+1——否则掉线盘的旧链会凭平/高秩在选举中翻盘
    let pre_dm = read_disk_meta(&s, "alpha", "k").await.max_epoch();
    s.delete_object_versioned("alpha", "k", Some(&marker_id), true)
        .await
        .unwrap();
    let post_dm = read_disk_meta(&s, "alpha", "k").await.max_epoch();
    assert!(
        post_dm > pre_dm,
        "删 marker 的链重写不得让修订序号倒退：{pre_dm} → {post_dm}"
    );
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], &c[..]);

    // 分页：max=2 截断 + 续点重放
    let p1 = s
        .list_object_versions("alpha", "", 2, None, None)
        .await
        .unwrap();
    assert_eq!(p1.entries.len(), 2);
    assert!(p1.is_truncated);
    let p2 = s
        .list_object_versions(
            "alpha",
            "",
            2,
            p1.next_key_marker.as_deref(),
            p1.next_version_id_marker.as_deref(),
        )
        .await
        .unwrap();
    assert_eq!(p2.entries.len(), 1);
    assert!(!p2.is_truncated);
    assert_eq!(p2.entries[0].version_id, v_ids[2], "续页无重叠无遗漏");

    // 点名删 v1：读 404、目录进 trash
    s.delete_object_versioned("alpha", "k", Some(&v_ids[2]), true)
        .await
        .unwrap();
    assert!(matches!(
        s.get_object_version("alpha", "k", &v_ids[2]).await,
        Err(Error::NoSuchKey(_))
    ));
    let trash = layout::sys_dir(d.path(), "trash").join(&v_ids[2]);
    assert!(
        tokio::fs::try_exists(&trash).await.unwrap(),
        "被删版本目录应进 trash"
    );

    // 幂等：删不存在的版本 → Ok(NotFound)（S3 语义；M6 P2-1：空删与真删可判别，事件层据此不发事件）
    let out = s
        .delete_object_versioned(
            "alpha",
            "k",
            Some("00000000-0000-0000-0000-000000000000"),
            true,
        )
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::NotFound));

    // 删光 → 整键清理
    s.delete_object_versioned("alpha", "k", Some(&v_ids[0]), true)
        .await
        .unwrap();
    s.delete_object_versioned("alpha", "k", Some(&v_ids[1]), true)
        .await
        .unwrap();
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
    let vl = s
        .list_object_versions("alpha", "", 1000, None, None)
        .await
        .unwrap();
    assert_eq!(vl.entries.len(), 0);
    assert!(
        !tokio::fs::try_exists(layout::object_dir(d.path(), "alpha", "k").unwrap())
            .await
            .unwrap(),
        "链清空后整键目录应清理"
    );

    // 不存在的键：版本桶 delete 也创建 marker（S3 语义）——单盘 write_meta_on 需自建目录
    // （verifier 抓的回归：曾因对象目录缺失 500）
    let out = s
        .delete_object_versioned("alpha", "fresh", None, true)
        .await
        .unwrap();
    assert!(matches!(out, DeleteOutcome::Marker { .. }));
    assert!(matches!(
        s.get_object("alpha", "fresh").await,
        Err(Error::NoSuchKey(_))
    ));
    // 续点不在链内 → InvalidArgument（fail-loud，不静默跳键）
    let e = s
        .list_object_versions("alpha", "fresh", 10, Some("fresh"), Some("deadbeef"))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::InvalidArgument(_)), "{e}");
    // 只给 version-id-marker（无 key-marker）→ InvalidArgument
    let e = s
        .list_object_versions("alpha", "", 10, None, Some("x"))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::InvalidArgument(_)), "{e}");

    // 非版本桶回归：delete_object_versioned(None, false) = M2b 整键删除
    s.put_object("alpha", "plain", payload(200_000), None)
        .await
        .unwrap();
    let out = s
        .delete_object_versioned("alpha", "plain", None, false)
        .await
        .unwrap();
    assert_eq!(out, DeleteOutcome::Object);
    assert!(matches!(
        s.get_object("alpha", "plain").await,
        Err(Error::NoSuchKey(_))
    ));
}

/// 切片 4：heal 批量补建——掉线期欠账（缺片）与 fsck 差集闭环；修复后复检清零。
#[tokio::test]
async fn heal_repairs_missing_shards_and_reports_clean() {
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    let data = payload(600_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "k").await;
    let uuid = m.latest().unwrap().part.uuid.clone();
    // 模拟掉线期欠账：删两片本地文件（12 盘各持一片：shard i 落 dirs[i]）
    for i in [2usize, 5] {
        let p = layout::object_dir(dirs[i].path(), "alpha", "k")
            .unwrap()
            .join(&uuid)
            .join(format!("part.1.{i}"));
        std::fs::remove_file(&p).unwrap();
    }
    let pre = s.fsck(false).await.unwrap();
    assert!(pre.missing_shards >= 2, "{pre:?}");
    let healed = s.heal().await.unwrap();
    assert_eq!(healed.repaired_bucket_dirs, 0, "单机无桶目录差集");
    let post = s.fsck(false).await.unwrap();
    assert_eq!(post.missing_shards, 0, "heal 后应清账：{post:?}");
    assert_eq!(post.bad_shards, 0);
    let (_, back) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&back[..], &data[..]);
}

/// 切片 4：scrub 深度巡检——健康全绿；坏片定位计数；缺块计数；**全链覆盖**（历史版本也巡）。
#[tokio::test]
async fn scrub_detects_bad_and_missing_shards_full_chain() {
    fn stream_of(b: Bytes) -> ByteStream {
        Box::pin(futures::stream::once(async move { Ok(b) }))
    }
    let (s, dirs) = store_12().await;
    s.create_bucket("alpha").await.unwrap();
    let data = payload(600_000);
    s.put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();

    let r = s.scrub().await.unwrap();
    assert_eq!(r.versions_scanned, 1);
    assert_eq!(r.ok_versions, 1, "{r:?}");
    assert!(r.failed_versions.is_empty(), "{r:?}");

    // 坏一字节：块级对偶可定位 → 计 bad_shards，版本仍 ok（读路径会自愈）
    let m = read_disk_meta(&s, "alpha", "k").await;
    let uuid = m.latest().unwrap().part.uuid.clone();
    let p = layout::object_dir(dirs[3].path(), "alpha", "k")
        .unwrap()
        .join(&uuid)
        .join("part.1.3");
    let mut sh = std::fs::read(&p).unwrap();
    sh[10] ^= 0xff;
    std::fs::write(&p, &sh).unwrap();
    let r = s.scrub().await.unwrap();
    assert!(r.bad_shards >= 1, "{r:?}");
    assert!(
        r.failed_versions.is_empty(),
        "单片坏应定位为 bad 而非失败：{r:?}"
    );

    // 删一片：缺块计数（存活 11 ≥ data，仍可重建 → 不算失败）
    std::fs::remove_file(&p).unwrap();
    let r = s.scrub().await.unwrap();
    assert!(r.missing_shards >= 1, "{r:?}");
    assert!(r.failed_versions.is_empty(), "{r:?}");

    // 全链覆盖：版本桶两版 → versions_scanned = 2（旧版本坏片同样被巡到）
    s.delete_object("alpha", "k").await.unwrap();
    let a = payload(300_000);
    let b = payload(310_000);
    for body in [a.clone(), b.clone()] {
        s.put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(body),
            None,
            Default::default(),
            None,
            true,
            None,
            None,
        )
        .await
        .unwrap();
    }
    let m = read_disk_meta(&s, "alpha", "k").await;
    let v1 = m.versions.first().unwrap();
    let p1 = layout::object_dir(dirs[1].path(), "alpha", "k")
        .unwrap()
        .join(&v1.part.uuid)
        .join("part.1.1");
    let mut sh = std::fs::read(&p1).unwrap();
    sh[0] ^= 0xff;
    std::fs::write(&p1, &sh).unwrap();
    let r = s.scrub().await.unwrap();
    assert_eq!(r.versions_scanned, 2, "{r:?}");
    assert!(r.bad_shards >= 1, "历史版本的坏片必须被巡到：{r:?}");
}

/// 切片 5：追加式扩容（12→13 盘）+ rebalance——展开放置、搬迁闭环、幂等、fsck/stray 清。
#[tokio::test]
async fn rebalance_expands_from_12_to_13_disks() {
    let dir = tempfile::tempdir().unwrap();
    let mut dirs: Vec<std::path::PathBuf> =
        (0..12).map(|i| dir.path().join(format!("d{i}"))).collect();
    for d in &dirs {
        std::fs::create_dir_all(d).unwrap();
    }
    let data = payload(600_000);

    // 1) 12 盘（v1 恒等放置）落两个对象
    let mut cfg = StoreConfig::new(&dirs[0]);
    cfg.drives = dirs.clone();
    let s = Store::open(cfg).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
    s.put_object("alpha", "k1", data.clone(), None)
        .await
        .unwrap();
    s.put_object("alpha", "inline-small", payload(1000), None)
        .await
        .unwrap();
    let m = read_disk_meta(&s, "alpha", "k1").await;
    let l = m.latest().unwrap().data_layout.clone().unwrap();
    assert!(
        l.shards
            .iter()
            .enumerate()
            .all(|(i, sh)| sh.drive as usize == i),
        "12 盘 v1 恒等：片 i 落盘 i"
    );
    drop(s);

    // 2) 追加第 13 盘重开 → 放置 v2 自动启用（13 > 12）；旧对象自描述可读。
    //    新盘须先「入册」（真实运维步骤：在**已挂载**的新盘上建 .oss.sys 并盖部署标识——
    //    防「遮挂载点 impostor」的既有策略不允许 open 自动初始化非首启盘，README 记档）
    dirs.push(dir.path().join("d12"));
    let new_sys = dirs[12].join(layout::SYS_DIR);
    std::fs::create_dir_all(&new_sys).unwrap();
    std::fs::copy(
        dirs[0].join(layout::SYS_DIR).join("drive-id"),
        new_sys.join("drive-id"),
    )
    .unwrap();
    let mut cfg = StoreConfig::new(&dirs[0]);
    cfg.drives = dirs.clone();
    let s = Store::open(cfg).await.unwrap();
    let (_, got) = s.get_object("alpha", "k1").await.unwrap();
    assert_eq!(&got[..], &data[..], "扩容后旧对象仍可读");

    // dry-run 只报告
    let rep = s.rebalance(true).await.unwrap();
    assert!(rep.placement_v2);
    assert_eq!(
        rep.objects_moved, 1,
        "EC 对象需搬；inline 不在范围：{rep:?}"
    );
    assert!(rep.shards_moved >= 1);

    // apply
    let rep = s.rebalance(false).await.unwrap();
    assert_eq!(rep.objects_moved, 1, "{rep:?}");
    assert!(rep.skipped.is_empty(), "{rep:?}");

    // 分布达标：片 i → (hash + i) % 13
    let hash = Store::placement_hash("alpha", "k1");
    let m = read_disk_meta(&s, "alpha", "k1").await;
    let l = m.latest().unwrap().data_layout.as_ref().unwrap();
    for (i, sh) in l.shards.iter().enumerate() {
        assert_eq!(
            sh.drive as u16,
            ((hash.wrapping_add(i as u64)) % 13) as u16,
            "片 {i} 目标槽"
        );
    }
    // 读全绿 + fsck 零（老片已清、无 stray）
    let (_, got) = s.get_object("alpha", "k1").await.unwrap();
    assert_eq!(&got[..], &data[..], "搬迁后读一致");
    let r = s.fsck(false).await.unwrap();
    assert_eq!(r.missing_shards, 0, "{r:?}");
    assert_eq!(r.stray_files.len(), 0, "老片应已清理：{r:?}");

    // 幂等：二次 apply 0 移动
    let rep = s.rebalance(false).await.unwrap();
    assert_eq!(rep.objects_moved, 0, "{rep:?}");

    // stray 检测与修复：手工放一个多余分片文件
    let m = read_disk_meta(&s, "alpha", "k1").await;
    let uuid = m.latest().unwrap().part.uuid.clone();
    let stray = layout::object_dir(&dirs[0], "alpha", "k1")
        .unwrap()
        .join(&uuid)
        .join("part.1.99");
    std::fs::write(&stray, b"x").unwrap();
    let r = s.fsck(false).await.unwrap();
    assert!(
        r.stray_files.iter().any(|f| f.contains("part.1.99")),
        "stray 应被检出：{r:?}"
    );
    let r = s.fsck(true).await.unwrap();
    assert!(!r.stray_files.is_empty());
    assert!(
        !tokio::fs::try_exists(&stray).await.unwrap(),
        "--repair 应删除 stray"
    );
}

/// 切片 5：集群跨节点 rebalance——v1 写入（片 i → 节点 i/3）→ 同一组盘重开 v2 →
/// 搬迁跨节点搬片 → 每节点读一致 + fsck 零 + 幂等。
#[tokio::test]
async fn rebalance_moves_shards_across_nodes_in_cluster() {
    let c1 = Cluster::new(4, 3).await;
    c1.stores[0].create_bucket("alpha").await.unwrap();
    let data = payload(600_000);
    c1.stores[0]
        .put_object("alpha", "k", data.clone(), None)
        .await
        .unwrap();
    // v1 恒等：片 i → 槽 i（节点 i/3）
    let m = read_disk_meta(&c1.stores[0], "alpha", "k").await;
    let l = m.latest().unwrap().data_layout.clone().unwrap();
    assert!(
        l.shards
            .iter()
            .enumerate()
            .all(|(i, sh)| sh.drive as usize == i),
        "v1 恒等落位"
    );

    // 同一组盘重开 v2（c1 保活 TempDir——勿 drop，目录在其 _tmpdirs 里）
    let c2 = Cluster::reopen_over(c1.dirs.clone(), true).await;

    let rep = c2.stores[0].rebalance(true).await.unwrap();
    assert!(rep.placement_v2);
    assert_eq!(rep.objects_moved, 1, "{rep:?}");
    let rep = c2.stores[0].rebalance(false).await.unwrap();
    assert_eq!(rep.objects_moved, 1, "{rep:?}");
    assert!(rep.skipped.is_empty(), "{rep:?}");

    // 目标槽逐片断言（(hash+i)%12 会跨节点）
    let hash = Store::placement_hash("alpha", "k");
    let m = read_disk_meta(&c2.stores[0], "alpha", "k").await;
    let l = m.latest().unwrap().data_layout.as_ref().unwrap();
    for (i, sh) in l.shards.iter().enumerate() {
        assert_eq!(
            sh.drive as u16,
            ((hash.wrapping_add(i as u64)) % 12) as u16,
            "片 {i} 目标槽"
        );
    }
    // 每节点读一致 + fsck 零
    for s in &c2.stores {
        let (_, got) = s.get_object("alpha", "k").await.unwrap();
        assert_eq!(&got[..], &data[..], "搬迁后跨节点读一致");
        let r = s.fsck(false).await.unwrap();
        assert_eq!(r.missing_shards, 0, "{r:?}");
        assert_eq!(r.stray_files.len(), 0, "老片应已清：{r:?}");
    }
    // 幂等：二次 apply 0 移动
    let rep = c2.stores[0].rebalance(false).await.unwrap();
    assert_eq!(rep.objects_moved, 0, "{rep:?}");
}

/// 切片 5/8（verifier F5-a）：拓扑记录 v1→v2→v3 兼容解析与畸形 fail-closed。
#[test]
fn topo_record_parses_v1_and_v2_and_rejects_garbage() {
    assert_eq!(
        super::parse_topo_record("v2 node=n1 slot=0"),
        Some((0, "n1".to_string(), 0))
    );
    // v1 旧记录：slots= 段被忽略，按 (node,slot) 取出（set 视为 0）
    assert_eq!(
        super::parse_topo_record("v1 slots=12 node=n2 slot=5"),
        Some((0, "n2".to_string(), 5))
    );
    assert_eq!(super::parse_topo_record("garbage"), None);
    assert_eq!(super::parse_topo_record("v2 node=n1"), None, "缺 slot 段");
    assert_eq!(
        super::parse_topo_record("v2 node=n1 slot=abc"),
        None,
        "slot 非数字"
    );
    // 切片 8：v3 带 set；v3+set=0 = 拒启（写路径永不产出该形态）
    assert_eq!(
        super::parse_topo_record("v3 set=1 node=n2 slot=3"),
        Some((1, "n2".to_string(), 3))
    );
    assert_eq!(
        super::parse_topo_record("v3 set=0 node=n1 slot=0"),
        None,
        "v3+set0 非法"
    );
    // P3-2 不变量：set= 字段 ⟺ v3——v1/v2 携带 set= 一律拒收（人为错改形态）
    assert_eq!(
        super::parse_topo_record("v2 set=1 node=n1 slot=0"),
        None,
        "v2 带 set= 非法"
    );
    assert_eq!(
        super::parse_topo_record("v1 slots=12 set=1 node=n1 slot=0"),
        None,
        "v1 带 set= 非法"
    );
    assert_eq!(
        super::parse_topo_record("v3 node=n1 slot=0"),
        None,
        "v3 缺 set= 非法"
    );
    // 构造器与解析器互逆（写盘与 console 共用的唯一出处）；set0 恒 v2、set>0 恒 v3
    assert_eq!(
        super::parse_topo_record(&super::topo_record(0, "n3", 7)),
        Some((0, "n3".to_string(), 7))
    );
    assert_eq!(
        super::parse_topo_record(&super::topo_record(2, "n3", 7)),
        Some((2, "n3".to_string(), 7))
    );
}

/// 切片 5（verifier F5-b）：追加式扩容不改变既有节点的槽位基址（H 面的不变量）。
#[test]
fn appending_nodes_keeps_existing_bases() {
    let ec = ec::EcConfig {
        data: 8,
        parity: 4,
        ..Default::default()
    };
    let mk = |n: &str| drives::NodeSpec {
        id: n.into(),
        addr: "127.0.0.1:1".into(),
        drive_count: 3,
    };
    let n4 = vec![mk("n1"), mk("n2"), mk("n3"), mk("n4")];
    let mut n5 = n4.clone();
    n5.push(mk("n5"));
    for i in 0..4 {
        let t4 = drives::ClusterTopology::new(i, n4.clone(), &ec).unwrap();
        let t5 = drives::ClusterTopology::new(i, n5.clone(), &ec).unwrap();
        assert_eq!(
            t4.local_slot_base(),
            t5.local_slot_base(),
            "节点 {} 的前缀基址在尾部追加下不变",
            i
        );
        let r4 = super::topo_record(0, &t4.self_node().id, t4.local_slot_base());
        let r5 = super::topo_record(0, &t5.self_node().id, t5.local_slot_base());
        assert_eq!(
            r4, r5,
            "既有盘的 (node,slot) 记录在追加下不变（指纹兼容的根）"
        );
    }
    assert_eq!(
        drives::ClusterTopology::new(4, n5.clone(), &ec)
            .unwrap()
            .local_slot_base(),
        12,
        "新节点接在末尾"
    );
}
