//! M8 前置：对象标签（store 级）——逐版本往返、覆盖清零、marker 语义、epoch、共存字段。

use super::*;
use crate::meta::ObjectTag;

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

fn tags(pairs: &[(&str, &str)]) -> Vec<ObjectTag> {
    pairs
        .iter()
        .map(|(k, v)| ObjectTag {
            key: (*k).into(),
            value: (*v).into(),
        })
        .collect()
}

async fn put(
    s: &Store,
    key: &str,
    body: &'static [u8],
    versioned: bool,
    t: Option<Vec<ObjectTag>>,
) -> ObjectInfo {
    s.put_object_stream_with_meta(
        "alpha",
        key,
        stream_of(body),
        None,
        Default::default(),
        None,
        versioned,
        None,
        None,
        t,
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn versioned_tags_are_isolated_and_listed() {
    let (s, _d) = store().await;
    let v1 = put(&s, "k", b"1", true, Some(tags(&[("a", "1")]))).await;
    let v2 = put(&s, "k", b"2", true, Some(tags(&[("b", "2")]))).await;
    let vid1 = v1.version_id.unwrap();
    let vid2 = v2.version_id.unwrap();
    // 默认 = 链尾
    let (vid, t) = s.get_object_tags("alpha", "k", None).await.unwrap();
    assert_eq!(vid, vid2);
    assert_eq!(t, tags(&[("b", "2")]));
    // 点名旧版本
    let (vid, t) = s.get_object_tags("alpha", "k", Some(&vid1)).await.unwrap();
    assert_eq!(vid, vid1);
    assert_eq!(t, tags(&[("a", "1")]));
    // 不新增版本
    assert_eq!(
        s.read_meta("alpha", "k")
            .await
            .unwrap()
            .unwrap()
            .versions
            .len(),
        2
    );
    // head/get/list 都回填 tags
    assert_eq!(
        s.head_object("alpha", "k").await.unwrap().tags,
        tags(&[("b", "2")])
    );
    let (info, _) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(info.tags, tags(&[("b", "2")]));
    let listed = s
        .list_objects("alpha", &ListRequest::default())
        .await
        .unwrap();
    assert_eq!(listed.objects[0].tags, tags(&[("b", "2")]));
}

#[tokio::test]
async fn non_versioned_overwrite_clears_tags() {
    let (s, _d) = store().await;
    put(&s, "k", b"1", false, Some(tags(&[("a", "1")]))).await;
    let (_, t) = s.get_object_tags("alpha", "k", None).await.unwrap();
    assert_eq!(t, tags(&[("a", "1")]));
    // 覆盖写不带标签 → 旧标签不残留（单版本）
    put(&s, "k", b"2", false, None).await;
    let (_, t) = s.get_object_tags("alpha", "k", None).await.unwrap();
    assert!(t.is_empty(), "{t:?}");
    assert_eq!(
        s.read_meta("alpha", "k")
            .await
            .unwrap()
            .unwrap()
            .versions
            .len(),
        1
    );
}

#[tokio::test]
async fn null_version_tags_roundtrip() {
    let (s, _d) = store().await;
    put(&s, "k", b"1", false, Some(tags(&[("a", "1")]))).await;
    let (vid, t) = s.get_object_tags("alpha", "k", Some("null")).await.unwrap();
    assert_eq!(vid, "null");
    assert_eq!(t, tags(&[("a", "1")]));
    s.set_object_tags("alpha", "k", Some("null"), tags(&[("z", "9")]))
        .await
        .unwrap();
    let (_, t) = s.get_object_tags("alpha", "k", None).await.unwrap();
    assert_eq!(t, tags(&[("z", "9")]));
}

#[tokio::test]
async fn tags_write_bumps_epoch_and_clear_works() {
    let (s, _d) = store().await;
    put(&s, "k", b"1", true, Some(tags(&[("a", "1")]))).await;
    let before = s
        .read_meta("alpha", "k")
        .await
        .unwrap()
        .unwrap()
        .max_epoch();
    s.set_object_tags("alpha", "k", None, tags(&[("b", "2")]))
        .await
        .unwrap();
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert!(m.max_epoch() > before, "标签写必须抬 epoch");
    assert_eq!(m.versions.len(), 1, "标签写不新增版本");
    s.set_object_tags("alpha", "k", None, Vec::new())
        .await
        .unwrap();
    let (_, t) = s.get_object_tags("alpha", "k", None).await.unwrap();
    assert!(t.is_empty());
    assert!(
        s.read_meta("alpha", "k").await.unwrap().unwrap().versions[0]
            .tags
            .is_none(),
        "空集合归一为 None"
    );
}

#[tokio::test]
async fn marker_semantics() {
    let (s, _d) = store().await;
    put(&s, "k", b"1", true, Some(tags(&[("a", "1")]))).await;
    let out = s
        .delete_object_versioned("alpha", "k", None, true)
        .await
        .unwrap();
    let marker = match out {
        DeleteOutcome::Marker { version_id } => version_id,
        other => panic!("应生成 marker：{other:?}"),
    };
    // 默认解析视同对象不存在
    assert!(matches!(
        s.get_object_tags("alpha", "k", None).await,
        Err(Error::NoSuchKey(_))
    ));
    assert!(matches!(
        s.set_object_tags("alpha", "k", None, tags(&[("b", "2")]))
            .await,
        Err(Error::NoSuchKey(_))
    ));
    // 点名 marker → 405（DeleteMarker）
    assert!(matches!(
        s.get_object_tags("alpha", "k", Some(&marker)).await,
        Err(Error::DeleteMarker(_))
    ));
    assert!(matches!(
        s.set_object_tags("alpha", "k", Some(&marker), Vec::new())
            .await,
        Err(Error::DeleteMarker(_))
    ));
    // 未知版本
    assert!(matches!(
        s.get_object_tags("alpha", "k", Some("missing")).await,
        Err(Error::NoSuchKey(_))
    ));
    // marker 自身无标签
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    assert!(m.find_version(&marker).unwrap().tags.is_none());
}

#[tokio::test]
async fn tags_coexist_with_lock_and_replication_status() {
    let (s, _d) = store().await;
    let v = put(&s, "k", b"1", true, Some(tags(&[("a", "1")]))).await;
    let vid = v.version_id.unwrap();
    s.set_object_legal_hold("alpha", "k", &vid, true)
        .await
        .unwrap();
    s.set_replication_status("alpha", "k", &vid, Some("PENDING"))
        .await
        .unwrap();
    s.set_object_tags("alpha", "k", Some(&vid), tags(&[("b", "2")]))
        .await
        .unwrap();
    let m = s.read_meta("alpha", "k").await.unwrap().unwrap();
    let ver = m.find_version(&vid).unwrap();
    assert_eq!(ver.tags, Some(tags(&[("b", "2")])));
    assert!(ver.lock.as_ref().is_some_and(|l| l.legal_hold));
    assert_eq!(ver.replication_status.as_deref(), Some("PENDING"));
}

#[tokio::test]
async fn invalid_tags_rejected_at_put_and_set() {
    let (s, _d) = store().await;
    // put：重复键
    let e = s
        .put_object_stream_with_meta(
            "alpha",
            "bad",
            stream_of(b"x"),
            None,
            Default::default(),
            None,
            true,
            None,
            None,
            Some(tags(&[("k", "1"), ("k", "2")])),
            None,
        )
        .await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");
    // set：11 条
    put(&s, "k", b"1", true, None).await;
    let many: Vec<ObjectTag> = (0..11)
        .map(|i| ObjectTag {
            key: format!("k{i}"),
            value: "v".into(),
        })
        .collect();
    let e = s.set_object_tags("alpha", "k", None, many).await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");
}

/// 单 part MPU 全流程（create → upload → complete）；`t` 走 Create 的标签参数。
async fn mpu(
    s: &Store,
    key: &str,
    t: Option<Vec<ObjectTag>>,
    versioned: bool,
) -> Result<ObjectInfo> {
    let id = s
        .create_multipart_upload_ext(
            "alpha",
            key,
            None,
            Default::default(),
            None,
            None,
            None,
            None,
            t,
        )
        .await?;
    let etag = s
        .upload_part("alpha", key, &id, 1, stream_of(b"mpu-body"))
        .await?;
    s.complete_multipart_upload_versioned("alpha", key, &id, &[(1, etag)], versioned, None, None)
        .await
}

#[tokio::test]
async fn mpu_create_tags_land_on_complete() {
    let (s, _d) = store().await;
    let want = tags(&[("env", "prod"), ("team", "core")]);
    let info = mpu(&s, "mp", Some(want.clone()), false).await.unwrap();
    // ObjectInfo 与版本 meta 同源；GET 标签读的就是落地版本
    assert_eq!(info.tags, want);
    let (_, got) = s.get_object_tags("alpha", "mp", None).await.unwrap();
    assert_eq!(got, want);
    let m = s.read_meta("alpha", "mp").await.unwrap().unwrap();
    assert_eq!(m.latest().unwrap().tags, Some(want));

    // 版本控制桶同样落到新版本（链尾）
    let info = mpu(&s, "mpv", Some(tags(&[("v", "1")])), true)
        .await
        .unwrap();
    assert_eq!(info.tags, tags(&[("v", "1")]));
    let (_, got) = s.get_object_tags("alpha", "mpv", None).await.unwrap();
    assert_eq!(got, tags(&[("v", "1")]));
}

#[tokio::test]
async fn mpu_without_tags_stays_none() {
    let (s, _d) = store().await;
    let info = mpu(&s, "mp", None, false).await.unwrap();
    assert!(info.tags.is_empty());
    let (_, got) = s.get_object_tags("alpha", "mp", None).await.unwrap();
    assert!(got.is_empty());
    let m = s.read_meta("alpha", "mp").await.unwrap().unwrap();
    assert!(m.latest().unwrap().tags.is_none());
}

#[tokio::test]
async fn mpu_create_rejects_invalid_tags() {
    let (s, _d) = store().await;
    let many: Vec<ObjectTag> = (0..11)
        .map(|i| ObjectTag {
            key: format!("k{i}"),
            value: "v".into(),
        })
        .collect();
    let e = mpu(&s, "mp", Some(many), false).await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");
    let e = mpu(&s, "mp2", Some(tags(&[("bad!", "v")])), false).await;
    assert!(matches!(e, Err(Error::InvalidArgument(_))), "{e:?}");
}
