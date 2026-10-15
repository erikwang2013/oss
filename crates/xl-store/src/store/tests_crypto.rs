//! M5 切片 2：加密数据面集成测试（store 级；子设计 D12 清单 ①-⑥）。
//! 单元级几何/AEAD 测试在 `crate::crypto::tests`；本文件覆盖写读管线、Range、MPU、版本链、
//! 篡改/泄露面、门闩与零回归。

use super::*;
use crate::crypto::{self, EncSpec};

// ---------- 测试助手（本模块自持，避免跨 mod tests 私有面） ----------

async fn store() -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    (s, dir)
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

/// 已接线加密的 store（file + kms 两个 provider 都接上；drive-id 由 open 盖）。
async fn enc_store() -> (Store, tempfile::TempDir) {
    let (s, d) = store().await;
    let sys = layout::sys_dir(d.path(), "");
    s.init_encryption(Some(sys.join("master.key")), Some(sys.join("kms")))
        .await
        .unwrap();
    (s, d)
}

/// 对象目录（单盘）：`<drive>/alpha/<key>/`。
fn key_dir(d: &std::path::Path, key: &str) -> std::path::PathBuf {
    layout::object_dir(d, "alpha", key).unwrap()
}

/// 提交点 meta 路径：**对象目录根**（非版本目录内）。
fn meta_path(d: &std::path::Path, key: &str) -> std::path::PathBuf {
    key_dir(d, key).join(meta::META_FILE)
}

/// 12 个分片文件路径（`<key>/<uuid>/part.1.<i>`）。
fn shard_paths(d: &std::path::Path, key: &str, uuid: &str) -> Vec<std::path::PathBuf> {
    (0..12)
        .map(|i| key_dir(d, key).join(uuid).join(format!("part.1.{i}")))
        .collect()
}

/// 独立重建密文流 C（确定性：SEAL 只依赖 DEK/seed/单元号）。
async fn rebuild_c(s: &Store, rec: &meta::EncryptionRecord, plain: &[u8]) -> Vec<u8> {
    let ctx = crypto::UnitCtx::for_read(rec, s.enc_state().unwrap())
        .await
        .unwrap();
    crypto::seal_all(&ctx, plain).unwrap()
}

/// 期望全部 data+parity 片：按 D2 独立构造单元（数据片 s 的块 t 输入 = 单元 u = s·blocks_ec + t，
/// 越界条带全零）→ 走 `ChunkWriter` 逐块编码——**parity 由 EC 生成，不是 C 窗口**。
fn expected_shards_via_ec(c: &[u8], geo: &crypto::Geometry, blocks: u64) -> Vec<Vec<u8>> {
    let cfg = ec::EcConfig::default();
    let data = cfg.data as usize;
    let mut w = ec::ChunkWriter::new(cfg).unwrap();
    let mut out: Vec<Vec<u8>> = vec![Vec::new(); (cfg.data + cfg.parity) as usize];
    for t in 0..blocks {
        let mut ins: Vec<Vec<u8>> = Vec::with_capacity(data);
        for s in 0..data as u64 {
            let u = s * blocks + t;
            let mut block = vec![0u8; ec::CHUNK_W];
            if u < geo.units {
                let start = (u * crypto::UNIT_W as u64) as usize;
                let len = geo.unit_cipher_len(u) as usize;
                block[..len].copy_from_slice(&c[start..start + len]);
            }
            ins.push(block);
        }
        for (i, b) in w.encode_block(&ins).unwrap().into_iter().enumerate() {
            out[i].extend_from_slice(&b);
        }
    }
    out
}

fn contains_window(hay: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && hay.len() >= needle.len()
        && hay.windows(needle.len()).any(|w| w == needle)
}

// ---------- ① 往返 / 几何 / Range ----------

#[tokio::test]
async fn encrypted_roundtrip_geometry_boundaries() {
    let (s, _d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let sizes = [
        0usize, 1, 65519, 65520, 65521, 131039, 131040, 131041, 300_000, 1_048_575, 1_048_577,
    ];
    for &n in &sizes {
        let body = payload(n);
        let info = s
            .put_object_stream_with_meta(
                "alpha",
                "k",
                stream_of(body.clone()),
                None,
                Default::default(),
                None,
                false,
                Some(EncSpec::SseS3),
            )
            .await
            .unwrap_or_else(|e| panic!("size={n} put 失败: {e}"));
        assert_eq!(info.size, n as u64);
        assert!(info.encryption.is_some(), "size={n} 须有加密记录");
        let (head, got) = s.get_object("alpha", "k").await.unwrap();
        assert_eq!(got, body, "size={n} 整读不等");
        assert_eq!(head.encryption.as_ref().unwrap().algorithm, crypto::ALGO);

        if n > 0 {
            let ranges: Vec<(u64, u64)> = vec![
                (0, 0), // 空区间（M3 判别：短路删则 b−1 下溢）
                (0, 1),
                (n as u64 - 1, n as u64),
                (65510.min(n as u64 - 1), 65530.min(n as u64)),
                (n as u64 / 2, (n as u64 / 2 + 7).min(n as u64)),
                (n as u64 / 3, n as u64 / 3), // 空区间（x>0，非判别）
                (196_550, 196_570),           // 跨 t 回绕点（P=3·65520，仅大档命中）
            ];
            for (a, b) in ranges {
                if a > b || b > n as u64 {
                    continue;
                }
                let got = s.get_object_range("alpha", "k", a..b).await.unwrap();
                assert_eq!(
                    &got[..],
                    &body[a as usize..b as usize],
                    "size={n} range={a}..{b}"
                );
            }
        } else {
            let got = s.get_object_range("alpha", "k", 0..0).await.unwrap();
            assert!(got.is_empty());
        }
    }
}

#[tokio::test]
async fn encrypted_disk_layout_contract_and_crc() {
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let body = payload(300_000); // units=5, blocks_ec=1, ss=W
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();

    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let v = m.latest().unwrap();
    let rec = v.encryption.clone().unwrap();
    let l = v.data_layout.as_ref().unwrap().clone();
    let c = rebuild_c(&s, &rec, &body).await;
    let geo = crypto::Geometry::new(body.len() as u64).unwrap();
    assert_eq!(c.len() as u64, geo.cipher_len);
    let blocks = l.shard_size / ec::CHUNK_W as u64;
    assert_eq!(blocks, 1);

    // 逐片契约：盘上分片 == 按 D2 独立构造单元后 EC 编码的产物（含 parity）；CRC 同口径
    let want = expected_shards_via_ec(&c, &geo, blocks);
    let paths = shard_paths(d.path(), "k", &v.part.uuid);
    for i in 0..12usize {
        let on_disk = tokio::fs::read(&paths[i]).await.unwrap();
        assert_eq!(on_disk, want[i], "分片 {i} 与 EC 布局契约不符");
        assert_eq!(
            ec::crc32c_of(&on_disk),
            l.shards[i].crc32c,
            "分片 {i} CRC 不符"
        );
    }
}

// ---------- ② 篡改 / 错钥 fail-closed ----------

#[tokio::test]
async fn tamper_two_shards_fails_closed() {
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let body = payload(300_000);
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let uuid = m.latest().unwrap().part.uuid.clone();
    // 同块两片同时翻字节：坏片定位失败 → fail-closed（单坏片会被 EC 自愈，不是本测目标）
    let paths = shard_paths(d.path(), "k", &uuid);
    for i in [0usize, 5usize] {
        let mut b = tokio::fs::read(&paths[i]).await.unwrap();
        b[123] ^= 0x01;
        tokio::fs::write(&paths[i], &b).await.unwrap();
    }
    let e = s.get_object("alpha", "k").await.unwrap_err();
    assert!(
        matches!(e, Error::CorruptMeta(_) | Error::Crypto(_)),
        "双片篡改须 fail-closed，实得 {e:?}"
    );
}

#[tokio::test]
async fn tamper_inline_ciphertext_and_wrapped_dek_fail_closed() {
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    // (a) inline 密文翻 1 字节 → GCM tag 失配 → Crypto
    let body = payload(100_000);
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let mut mm = m.clone();
    let inline = mm.versions[0].inline.as_mut().unwrap();
    inline.bytes[10] ^= 0x01;
    let bytes = mm.encode().unwrap();
    // meta 提交点在对象目录根（不在版本目录内）
    tokio::fs::write(meta_path(d.path(), "k"), &bytes)
        .await
        .unwrap();
    let e = s.get_object("alpha", "k").await.unwrap_err();
    assert!(
        matches!(e, Error::Crypto(_)),
        "inline 篡改应 Crypto，得 {e:?}"
    );

    // (b) wrapped_dek 篡改 → unwrap 失败 → Crypto
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let mut mm = m.clone();
    let rec = mm.versions[0].encryption.as_mut().unwrap();
    let mut w = hex::decode(&rec.wrapped_dek_hex).unwrap();
    w[0] ^= 0x01;
    rec.wrapped_dek_hex = hex::encode(&w);
    let bytes = mm.encode().unwrap();
    tokio::fs::write(meta_path(d.path(), "k"), &bytes)
        .await
        .unwrap();
    let e = s.get_object("alpha", "k").await.unwrap_err();
    assert!(
        matches!(e, Error::Crypto(_)),
        "wrapped 篡改应 Crypto，得 {e:?}"
    );
}

#[tokio::test]
async fn wrong_or_missing_provider_fails_closed() {
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let body = payload(200_000);
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();

    // (a) 同目录另一个 store，用**另一把**主密钥 → unwrap 失败
    let s2 = Store::open(StoreConfig::new(d.path())).await.unwrap();
    s2.init_encryption(Some(d.path().join("other-master.key")), None)
        .await
        .unwrap();
    let e = s2.get_object("alpha", "k").await.unwrap_err();
    assert!(matches!(e, Error::Crypto(_)), "错钥应 Crypto，得 {e:?}");

    // (b) 未接线（无 provider）→ Crypto
    let s3 = Store::open(StoreConfig::new(d.path())).await.unwrap();
    let e = s3.get_object("alpha", "k").await.unwrap_err();
    assert!(
        matches!(e, Error::Crypto(_)),
        "无 provider 应 Crypto，得 {e:?}"
    );
}

// ---------- ③ 泄露面（验收 ② 的单测级锚） ----------

#[tokio::test]
async fn shards_and_inline_leak_no_plaintext_window() {
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let marker = b"OSS-M5-SECRET-MARKER-0123456789ABCDEF"; // 36B ASCII

    // (a) EC 对象：全部 12 片（含 parity）不得含 16B 明文窗
    let n = 300_000;
    let body = Bytes::from(marker.iter().copied().cycle().take(n).collect::<Vec<u8>>());
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let uuid = m.latest().unwrap().part.uuid.clone();
    for (i, p) in shard_paths(d.path(), "k", &uuid).iter().enumerate() {
        let b = tokio::fs::read(p).await.unwrap();
        assert!(
            !contains_window(&b, &marker[..16]),
            "分片 {i} 含明文窗（验收②失败）"
        );
    }

    // (b) 两单元全零明文 → 两段密文不等（nonce 位置相关：等则 nonce 复用）
    let zeros = vec![0u8; 3 * 65520];
    s.put_object_stream_with_meta(
        "alpha",
        "z",
        stream_of(Bytes::from(zeros.clone())),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    let mz = s.read_meta("alpha", "z").await.unwrap().unwrap();
    let zv = mz.latest().unwrap();
    let zp = shard_paths(d.path(), "z", &zv.part.uuid);
    let s0 = tokio::fs::read(&zp[0]).await.unwrap();
    let s1 = tokio::fs::read(&zp[1]).await.unwrap();
    assert_ne!(s0, s1, "全零两单元密文不得相等（nonce 复用/派生失效）");

    // (c) inline 加密：盘上 meta 文件内不得有明文窗
    let body2 = Bytes::from(
        marker
            .iter()
            .copied()
            .cycle()
            .take(100_000)
            .collect::<Vec<u8>>(),
    );
    s.put_object_stream_with_meta(
        "alpha",
        "i",
        stream_of(body2.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    let meta_bytes = tokio::fs::read(meta_path(d.path(), "i")).await.unwrap();
    assert!(
        !contains_window(&meta_bytes, &marker[..16]),
        "inline 密文的 meta 文件含明文窗"
    );
}

// ---------- ④ MPU / 版本链 ----------

#[tokio::test]
async fn mpu_complete_encrypted_roundtrip() {
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let p1 = payload(5 * 1024 * 1024 + 123);
    let p2 = payload(300_000);
    let upload = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let e1 = s
        .upload_part("alpha", "k", &upload, 1, stream_of(p1.clone()))
        .await
        .unwrap();
    let e2 = s
        .upload_part("alpha", "k", &upload, 2, stream_of(p2.clone()))
        .await
        .unwrap();
    // 暂存区断言（D12⑥/D8 已知边界）：complete 前 part.N 为**明文**
    let staging = key_dir(d.path(), "k").join(&upload);
    let st1 = tokio::fs::read(staging.join("part.1")).await.unwrap();
    assert_eq!(&st1[..], &p1[..], "暂存 part.1 应为明文（提交前不可见）");
    let st2 = tokio::fs::read(staging.join("part.2")).await.unwrap();
    assert_eq!(&st2[..], &p2[..], "暂存 part.2 应为明文");
    let info = s
        .complete_multipart_upload_versioned(
            "alpha",
            "k",
            &upload,
            &[(1, e1), (2, e2)],
            false,
            Some(EncSpec::SseS3),
        )
        .await
        .unwrap();
    assert!(info.etag.ends_with("-2"));
    assert!(info.encryption.is_some());
    let concat: Vec<u8> = p1.iter().chain(p2.iter()).copied().collect();
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(&got[..], &concat[..]);
    let mid = s
        .get_object_range("alpha", "k", 5_240_000..5_240_100)
        .await
        .unwrap();
    assert_eq!(&mid[..], &concat[5_240_000..5_240_100]);
    // complete 后：暂存区已清、版本目录分片为密文（无明文窗）
    assert!(!staging.exists(), "complete 后暂存区应清理");
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let sh0 = shard_paths(d.path(), "k", &m.latest().unwrap().part.uuid);
    let b0 = tokio::fs::read(&sh0[0]).await.unwrap();
    assert!(!contains_window(&b0, &p1[..16]), "MPU 版本目录分片含明文窗");
}

#[tokio::test]
async fn sse_kms_roundtrip_and_key_id_routing() {
    // M1（复验补）：SseKms 写→读全链 + provider_for_key_id 的 kms 分支 + 路由负例
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let body = payload(300_000);
    let info = s
        .put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(body.clone()),
            None,
            Default::default(),
            None,
            false,
            Some(EncSpec::SseKms),
        )
        .await
        .unwrap();
    let rec = info.encryption.as_ref().unwrap();
    assert!(rec.key_id.starts_with('k'), "KMS key_id 应为 k<n>：{rec:?}");
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(got, body, "SSE-KMS 往返（unwrap 走 kms 路由）");
    // 同记录再读一遍（点读路径覆盖）
    let r = s
        .get_object_range("alpha", "k", 65_000..65_100)
        .await
        .unwrap();
    assert_eq!(&r[..], &body[65_000..65_100]);

    // 负例一：仅 file provider 的 store 读 k<n> 记录 → 路由到未配置的 kms → Crypto
    let s2 = Store::open(StoreConfig::new(d.path())).await.unwrap();
    s2.init_encryption(Some(d.path().join("only-file.key")), None)
        .await
        .unwrap();
    let e = s2.get_object("alpha", "k").await.unwrap_err();
    assert!(matches!(e, Error::Crypto(_)), "kms 未配置应 Crypto：{e:?}");
    // 负例二：未配置 kms provider 时 SseKms 写 → Crypto
    let e = s2
        .put_object_stream_with_meta(
            "alpha",
            "k2",
            stream_of(payload(100)),
            None,
            Default::default(),
            None,
            false,
            Some(EncSpec::SseKms),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(e, Error::Crypto(_)),
        "SseKms 写无 provider 应 Crypto：{e:?}"
    );
}

#[tokio::test]
async fn version_chain_keeps_old_encrypted_versions_readable() {
    let (s, _d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let v1 = payload(200_000);
    let info1 = s
        .put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(v1.clone()),
            None,
            Default::default(),
            None,
            true,
            Some(EncSpec::SseS3),
        )
        .await
        .unwrap();
    let id1 = info1.version_id.clone().unwrap();
    let v2 = payload(300_001);
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(v2.clone()),
        None,
        Default::default(),
        None,
        true,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();

    let (_, latest) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(latest, v2, "链尾 = v2");
    // 点名读 v1（get / head / range 三面）
    let (head1, got1) = s.get_object_version("alpha", "k", &id1).await.unwrap();
    assert_eq!(got1, v1);
    assert!(head1.encryption.is_some());
    let h = s.head_object_version("alpha", "k", &id1).await.unwrap();
    assert_eq!(h.size, 200_000);
    let r = s
        .get_object_range_version("alpha", "k", &id1, 65_500..65_540)
        .await
        .unwrap();
    assert_eq!(&r[..], &v1[65_500..65_540]);

    // delete marker 不受影响：无版本号删除 → 404；版本链仍可点名读
    s.delete_object_versioned("alpha", "k", None, true)
        .await
        .unwrap();
    assert!(matches!(
        s.get_object("alpha", "k").await,
        Err(Error::NoSuchKey(_))
    ));
    let (_, got1b) = s.get_object_version("alpha", "k", &id1).await.unwrap();
    assert_eq!(got1b, v1);
}

// ---------- ⑤ 零回归 / ec.enabled=false / 门闩 ----------

#[tokio::test]
async fn plaintext_objects_coexist_with_encrypted() {
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let plain = payload(300_000);
    s.put_object("alpha", "p", plain.clone(), None)
        .await
        .unwrap();
    let enc = payload(300_000);
    s.put_object_stream_with_meta(
        "alpha",
        "e",
        stream_of(enc.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();

    let (hp, gp) = s.get_object("alpha", "p").await.unwrap();
    assert_eq!(gp, plain);
    assert!(hp.encryption.is_none(), "明文对象无加密记录");
    let (_, ge) = s.get_object("alpha", "e").await.unwrap();
    assert_eq!(ge, enc);

    // 明文对象盘上分片 == 明文域的 EC 编码（与加密路径隔离，零回归）
    let mp = s.read_meta("alpha", "p").await.unwrap().unwrap();
    let l = mp.latest().unwrap().data_layout.as_ref().unwrap().clone();
    let blocks = l.shard_size / ec::CHUNK_W as u64;
    // 明文不做单元切分：直接以 payload 构造（用与加密同形的编码核对全 12 片）
    let cfg = ec::EcConfig::default();
    let data = cfg.data as usize;
    let mut w = ec::ChunkWriter::new(cfg).unwrap();
    let mut want0: Vec<u8> = Vec::new();
    for t in 0..blocks {
        let mut ins: Vec<Vec<u8>> = Vec::with_capacity(data);
        for s in 0..data as u64 {
            let start = s * l.shard_size + t * ec::CHUNK_W as u64;
            let mut block = vec![0u8; ec::CHUNK_W];
            if start < plain.len() as u64 {
                let take = ((plain.len() as u64 - start).min(ec::CHUNK_W as u64)) as usize;
                block[..take].copy_from_slice(&plain[start as usize..start as usize + take]);
            }
            ins.push(block);
        }
        want0.extend_from_slice(&w.encode_block(&ins).unwrap()[0]);
    }
    let paths = shard_paths(d.path(), "p", &mp.latest().unwrap().part.uuid);
    let on_disk0 = tokio::fs::read(&paths[0]).await.unwrap();
    assert_eq!(on_disk0, want0, "明文对象分片应逐字节 = 明文域 EC 编码");
}

#[tokio::test]
async fn ec_disabled_encryption_rejected_at_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.ec.enabled = false;
    let s = Store::open(cfg).await.unwrap();
    s.init_encryption(
        Some(layout::sys_dir(dir.path(), "").join("master.key")),
        None,
    )
    .await
    .unwrap();
    s.create_bucket("alpha").await.unwrap();
    let e = s
        .put_object_stream_with_meta(
            "alpha",
            "k",
            stream_of(payload(1000)),
            None,
            Default::default(),
            None,
            false,
            Some(EncSpec::SseS3),
        )
        .await
        .unwrap_err();
    match e {
        Error::Encode(m) => assert!(m.contains("ec.enabled"), "文案应含 ec.enabled：{m}"),
        other => panic!("应 Encode 拒绝，得 {other:?}"),
    }
}

#[tokio::test]
async fn init_gate_and_idempotence() {
    // 正常路径 + 重复 init
    let (s, d) = store().await;
    let key = layout::sys_dir(d.path(), "").join("master.key");
    s.init_encryption(Some(key.clone()), None).await.unwrap();
    assert!(key.exists());
    let e = s
        .init_encryption(Some(key.clone()), None)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Encode(_)), "重复 init 应拒：{e:?}");

    // 门闩：drive-id 缺失（未在册）→ 拒且**不落任何钥文件**
    let (s2, d2) = store().await;
    tokio::fs::remove_file(layout::sys_dir(d2.path(), "drive-id"))
        .await
        .unwrap();
    let key2 = layout::sys_dir(d2.path(), "").join("master.key");
    let e = s2
        .init_encryption(Some(key2.clone()), None)
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Crypto(_)), "未在册应 Crypto 拒：{e:?}");
    assert!(!key2.exists(), "被拒路径不得生成主密钥文件");
}

#[tokio::test]
async fn decode_rejects_bad_encryption_records() {
    let mk = |algo: &str, kid: &str, wrapped: &str, seed: &str| {
        let mut m = XlMeta {
            object_key: "k".into(),
            versions: vec![meta::Version::default()],
        };
        m.versions[0].part.uuid = "u".into(); // 解码白名单要求 uuid 安全
        m.versions[0].encryption = Some(meta::EncryptionRecord {
            algorithm: algo.into(),
            key_id: kid.into(),
            wrapped_dek_hex: wrapped.into(),
            nonce_seed_hex: seed.into(),
        });
        m
    };
    let good_seed = hex::encode([1u8; 8]);
    let good_wrapped = hex::encode([0u8; 60]);
    // 合法（含 SSE-C 预留：空 wrapped）→ 往返
    let ok = mk(crypto::ALGO, "file", &good_wrapped, &good_seed);
    assert_eq!(XlMeta::decode(&ok.encode().unwrap()).unwrap(), ok);
    let ok_c = mk(crypto::ALGO, "file", "", &good_seed);
    assert_eq!(XlMeta::decode(&ok_c.encode().unwrap()).unwrap(), ok_c);
    // 非法：算法 / wrapped 长度 / seed 长度 / key_id 控制字符
    for bad in [
        mk("AES256-GCM", "file", &good_wrapped, &good_seed),
        mk(crypto::ALGO, "file", &hex::encode([0u8; 59]), &good_seed),
        mk(crypto::ALGO, "file", &good_wrapped, &hex::encode([1u8; 7])),
        mk(crypto::ALGO, "bad\nid", &good_wrapped, &good_seed),
    ] {
        assert!(
            matches!(
                XlMeta::decode(&bad.encode().unwrap()),
                Err(Error::CorruptMeta(_))
            ),
            "非法记录应解码即拒"
        );
    }

    // 读侧：SSE-C 预留形态（空 wrapped）落盘 → read 必拒 Crypto
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    let mut m = mk(crypto::ALGO, "file", "", &good_seed);
    let v = &mut m.versions[0];
    v.size = 10;
    v.inline = Some(meta::InlineData {
        bytes: vec![0u8; 26], // c_len(10) = 26
    });
    v.part = meta::PartRef {
        uuid: "c_rec".into(),
        part: 1,
        size: 10,
        sha256: String::new(),
    };
    tokio::fs::create_dir_all(key_dir(d.path(), "k"))
        .await
        .unwrap();
    tokio::fs::write(meta_path(d.path(), "k"), m.encode().unwrap())
        .await
        .unwrap();
    let e = s.get_object("alpha", "k").await.unwrap_err();
    assert!(
        matches!(e, Error::Crypto(_)),
        "空 wrapped 读应 Crypto：{e:?}"
    );

    // 非法记录落盘 → 解码即拒（read_meta）
    let mut bad = mk("AES256-GCM", "file", &good_wrapped, &good_seed);
    bad.object_key = "b".into();
    let v = &mut bad.versions[0];
    v.part = meta::PartRef {
        uuid: "b_rec".into(),
        part: 1,
        size: 1,
        sha256: String::new(),
    };
    tokio::fs::create_dir_all(key_dir(d.path(), "b"))
        .await
        .unwrap();
    tokio::fs::write(meta_path(d.path(), "b"), bad.encode().unwrap())
        .await
        .unwrap();
    let e = s.read_meta("alpha", "b").await.unwrap_err();
    assert!(
        matches!(e, Error::CorruptMeta(_)),
        "非法记录应解码拒：{e:?}"
    );
}

#[tokio::test]
async fn fsck_and_repair_ok_on_encrypted_objects() {
    let (s, d) = enc_store().await;
    s.create_bucket("alpha").await.unwrap();
    // inline 加密对象（B5：不得误报 failed）
    let inline_body = payload(64_000);
    s.put_object_stream_with_meta(
        "alpha",
        "i",
        stream_of(inline_body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    // EC 加密对象 + 丢两片（≤parity）→ 整读重建 + 读时修
    let body = payload(300_000);
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let v = m.latest().unwrap();
    let rec = v.encryption.clone().unwrap();
    let uuid = v.part.uuid.clone();
    let l = v.data_layout.as_ref().unwrap().clone();
    let paths = shard_paths(d.path(), "k", &uuid);
    tokio::fs::remove_file(&paths[0]).await.unwrap();
    tokio::fs::remove_file(&paths[1]).await.unwrap();

    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(got, body, "丢 ≤parity 片应重建");

    // 读时修：从 **C** 重写回的分片与写侧契约逐字节一致（从 P 重编码必红）
    let c = rebuild_c(&s, &rec, &body).await;
    let geo = crypto::Geometry::new(body.len() as u64).unwrap();
    let blocks = l.shard_size / ec::CHUNK_W as u64;
    let want = expected_shards_via_ec(&c, &geo, blocks);
    for i in [0usize, 1usize] {
        let on_disk = tokio::fs::read(&paths[i]).await.unwrap();
        assert_eq!(on_disk, want[i], "修复片 {i} 非 C 口径");
    }
    // 修复后再次整读通过
    let (_, again) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(again, body);

    // scrub：两类加密对象都 ok（inline 长度按 c_len 口径——B5 落点）
    let rep = s.scrub().await.unwrap();
    assert!(
        rep.failed_versions.is_empty(),
        "scrub 误报：{:?}",
        rep.failed_versions
    );
    assert!(
        rep.ok_versions >= 2,
        "scrub ok_versions={}",
        rep.ok_versions
    );
}

#[tokio::test]
async fn multi_drive_encrypted_roundtrip_and_leak() {
    // 12 盘（stage_payload_multi 加密路径）+ 片级泄露面
    let dirs: Vec<_> = (0..12).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut cfg = StoreConfig::new(dirs[0].path());
    cfg.drives = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    let s = Store::open(cfg).await.unwrap();
    s.init_encryption(
        Some(layout::sys_dir(dirs[0].path(), "").join("master.key")),
        None,
    )
    .await
    .unwrap();
    s.create_bucket("alpha").await.unwrap();
    let marker = b"OSS-M5-MULTI-MARKER-0123456789ABCDEF";
    let body = Bytes::from(
        marker
            .iter()
            .copied()
            .cycle()
            .take(300_000)
            .collect::<Vec<u8>>(),
    );
    s.put_object_stream_with_meta(
        "alpha",
        "k",
        stream_of(body.clone()),
        None,
        Default::default(),
        None,
        false,
        Some(EncSpec::SseS3),
    )
    .await
    .unwrap();
    let (_, got) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(got, body);
    let r = s
        .get_object_range("alpha", "k", 65_530..65_560)
        .await
        .unwrap();
    assert_eq!(&r[..], &body[65_530..65_560]);
    // 各盘分片无明文窗（多盘路径同样按 C 落盘）
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let v = m.latest().unwrap();
    let l = v.data_layout.as_ref().unwrap();
    for (i, sh) in l.shards.iter().enumerate() {
        let p = layout::object_dir(dirs[sh.drive as usize].path(), "alpha", "k")
            .unwrap()
            .join(&v.part.uuid)
            .join(format!("part.1.{i}"));
        let b = tokio::fs::read(&p).await.unwrap();
        assert!(!contains_window(&b, &marker[..16]), "多盘分片 {i} 含明文窗");
    }
}
