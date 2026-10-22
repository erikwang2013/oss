//! GET/HEAD `?partNumber`：MPU complete 落盘的 part_sizes 偏移表与分段读（含版本/SSE-C）。

use super::*;
use crate::crypto::{self, EncSpec};

async fn store() -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(StoreConfig::new(dir.path())).await.unwrap();
    s.create_bucket("alpha").await.unwrap();
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

async fn put(s: &Store, key: &str, body: Bytes, mode: VersioningMode) -> ObjectInfo {
    s.put_object_stream_with_meta(
        "alpha",
        key,
        stream_of(body),
        None,
        Default::default(),
        None,
        mode,
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap()
}

/// complete 写 part_sizes；段读内容/区间/段数；版本点读；越界 416。
#[tokio::test]
async fn complete_records_part_sizes_and_reads_parts() {
    let (s, _d) = store().await;
    // 非末段须 ≥ 5 MiB（store 侧 EntityTooSmall 校验）
    let p1 = payload(5 * 1024 * 1024 + 7);
    let p2 = payload(5 * 1024 * 1024 + 3);
    let p3 = payload(11);
    let (s1, s2, s3) = (p1.len() as u64, p2.len() as u64, p3.len() as u64);

    let up = s
        .create_multipart_upload("alpha", "k", None, Default::default())
        .await
        .unwrap();
    let e1 = s
        .upload_part("alpha", "k", &up, 1, stream_of(p1.clone()))
        .await
        .unwrap();
    let e2 = s
        .upload_part("alpha", "k", &up, 2, stream_of(p2.clone()))
        .await
        .unwrap();
    let e3 = s
        .upload_part("alpha", "k", &up, 3, stream_of(p3.clone()))
        .await
        .unwrap();
    let info = s
        .complete_multipart_upload_versioned(
            "alpha",
            "k",
            &up,
            &[(1, e1), (2, e2), (3, e3)],
            VersioningMode::Enabled,
            None,
            None,
        )
        .await
        .unwrap();
    let vid = info.version_id.clone().unwrap();

    // part_sizes 从磁盘 meta 解码（msgpack 往返）且与上传段长一致
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let v = m
        .versions
        .iter()
        .find(|v| v.version_id_str() == vid)
        .unwrap();
    assert_eq!(v.part_sizes, Some(vec![s1, s2, s3]));

    // GET part2：内容、绝对区间、parts_count
    let (pi, bytes, span, n) = s
        .get_object_part_ext("alpha", "k", None, 2, None)
        .await
        .unwrap();
    assert_eq!(pi.size, s1 + s2 + s3);
    assert_eq!(bytes, p2);
    assert_eq!(span, s1..s1 + s2);
    assert_eq!(n, 3);

    // HEAD part3
    let (_, span, n) = s
        .object_part_span("alpha", "k", None, 3, None)
        .await
        .unwrap();
    assert_eq!(span, s1 + s2..s1 + s2 + s3);
    assert_eq!(n, 3);

    // 越界 / 非法段号
    assert!(matches!(
        s.get_object_part_ext("alpha", "k", None, 4, None).await,
        Err(Error::InvalidPartRange(_))
    ));
    assert!(matches!(
        s.object_part_span("alpha", "k", None, 0, None).await,
        Err(Error::InvalidPart(_))
    ));

    // 版本点读：新 PUT 成为 latest 后，旧版本的段边界仍可定位
    put(&s, "k", Bytes::from_static(b"new"), VersioningMode::Enabled).await;
    let (_, bytes, span, n) = s
        .get_object_part_ext("alpha", "k", Some(&vid), 1, None)
        .await
        .unwrap();
    assert_eq!(bytes, p1);
    assert_eq!(span, 0..s1);
    assert_eq!(n, 3);
    // latest 是单段 PUT（无 part_sizes）：只认 part 1
    assert!(matches!(
        s.get_object_part_ext("alpha", "k", None, 2, None).await,
        Err(Error::InvalidPartRange(_))
    ));
}

/// 旧对象（无 part_sizes）：part 1 = 整对象，其余 416。
#[tokio::test]
async fn legacy_object_only_part_one() {
    let (s, _d) = store().await;
    put(&s, "k", Bytes::from_static(b"abc"), VersioningMode::Off).await;

    let (_, bytes, span, n) = s
        .get_object_part_ext("alpha", "k", None, 1, None)
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"abc");
    assert_eq!(span, 0..3);
    assert_eq!(n, 1);

    assert!(matches!(
        s.get_object_part_ext("alpha", "k", None, 2, None).await,
        Err(Error::InvalidPartRange(_))
    ));
    assert!(matches!(
        s.object_part_span("alpha", "k", None, 2, None).await,
        Err(Error::InvalidPartRange(_))
    ));
}

/// SSE-C：段读与整读同口径校验客户端密钥（缺钥/错钥 → InvalidArgument）。
#[tokio::test]
async fn sse_c_part_read_checks_key() {
    let (s, _d) = store().await;
    let k = crypto::SseCKey::from_key([21u8; 32]);
    let wrong = crypto::SseCKey::from_key([22u8; 32]);
    let p1 = payload(5 * 1024 * 1024 + 1);
    let p2 = payload(300_000);

    let up = s
        .create_multipart_upload_ext(
            "alpha",
            "k",
            None,
            Default::default(),
            Some(k.md5_b64()),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let e1 = s
        .upload_part_ext(
            "alpha",
            "k",
            &up,
            1,
            stream_of(p1.clone()),
            Some(k.md5_b64()),
        )
        .await
        .unwrap();
    let e2 = s
        .upload_part_ext(
            "alpha",
            "k",
            &up,
            2,
            stream_of(p2.clone()),
            Some(k.md5_b64()),
        )
        .await
        .unwrap();
    s.complete_multipart_upload_versioned(
        "alpha",
        "k",
        &up,
        &[(1, e1), (2, e2)],
        VersioningMode::Off,
        Some(EncSpec::SseC(k.clone())),
        None,
    )
    .await
    .unwrap();

    let (_, bytes, span, n) = s
        .get_object_part_ext("alpha", "k", None, 2, Some(&k))
        .await
        .unwrap();
    assert_eq!(bytes, p2);
    assert_eq!(span, p1.len() as u64..p1.len() as u64 + p2.len() as u64);
    assert_eq!(n, 2);

    assert!(matches!(
        s.get_object_part_ext("alpha", "k", None, 1, None).await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        s.object_part_span("alpha", "k", None, 1, Some(&wrong))
            .await,
        Err(Error::InvalidArgument(_))
    ));
}
