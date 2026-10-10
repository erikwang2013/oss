// 直接测 sink 与 instant store，不经 HTTP（HTTP 链路交给 Task 16 的端到端脚本）。

mod common;

use aetherupload::events::EventSink;
use aetherupload::resource::Resource;
use common::state;

#[tokio::test(flavor = "multi_thread")] // block_on 用了 block_in_place，单线程 runtime 会 panic
async fn ingest_sink_puts_finished_file_into_store() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("files").await.unwrap();

    // 造一个「已落盘的成品」：aetherupload 落盘后是 <root>/file/<subdir>/<md5>.txt
    let sub = "2026-10";
    let group_dir = st.cfg.staging_dir.join("file").join(sub);
    std::fs::create_dir_all(&group_dir).unwrap();
    let body = b"finished upload";
    let md5 = format!("{:x}", {
        use md5::Digest as _;
        let mut h = md5::Md5::new();
        h.update(body);
        h.finalize()
    });
    let path = group_dir.join(format!("{md5}.txt"));
    std::fs::write(&path, body).unwrap();

    let resource = Resource::new(
        &st.cfg.staging_dir,
        "file",
        "file",
        sub,
        &format!("{md5}.txt"),
    );

    st.ingest.upload_complete(&resource);

    // 对象应当已经把文件装进内核，ETag 就是 aetherupload 已经算好的整份 md5
    let info = st
        .store
        .head_object("files", &format!("{sub}/{md5}.txt"))
        .await
        .unwrap();
    assert_eq!(info.size, body.len() as u64);
    assert_eq!(info.etag, md5);

    // uploads 表留下记录，供控制台展示
    let rows = st.db.list_uploads(10).await.unwrap();
    assert_eq!(rows[0].bucket, "files");
}

/// staging_dir 已存在（启动自举会先建它）时，list_groups 不建 `_header/` ——
/// 断言 build_runtime 自己补上，否则预处理必然 "fail to write head resource"。
#[tokio::test]
async fn build_runtime_prepares_staging_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    let _rt = oss_server::aether::build_runtime(&st);

    assert!(
        st.cfg.staging_dir.join("_header").is_dir(),
        "预处理要往 _header/ 写断点文件"
    );
    assert!(st.cfg.staging_dir.join("file").is_dir(), "分组目录要先建好");
}
