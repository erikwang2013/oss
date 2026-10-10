// 直接测 sink 与 instant store，不经 HTTP（HTTP 链路交给 Task 16 的端到端脚本）。

mod common;

use std::sync::Arc;

use aetherupload::events::EventSink;
use aetherupload::instant::KEY_PREFIX;
use aetherupload::resource::Resource;
use aetherupload::saved_path::SavedPath;
use common::state;
use oss_server::aether::IngestSink;

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

    // 成品文件留在暂存盘不动（秒传重传与人工排查都靠它）
    assert!(path.exists(), "ingest 不应删除暂存盘的成品文件");

    // 对象应当已经把文件装进内核，ETag 就是 aetherupload 已经算好的整份 md5
    let name = format!("{md5}.txt");
    let key = format!("{sub}/{name}");
    let info = st.store.head_object("files", &key).await.unwrap();
    assert_eq!(info.size, body.len() as u64);
    assert_eq!(info.etag, md5);

    // uploads 表留下记录，供控制台展示
    let rows = st.db.list_uploads(10).await.unwrap();
    assert_eq!(rows[0].bucket, "files");
    assert_eq!(rows[0].key, key);
    assert_eq!(rows[0].size, body.len() as i64);
    assert_eq!(rows[0].etag, md5);
    assert_eq!(
        rows[0].saved_path,
        SavedPath::encode("file", sub, &name),
        "saved_path 必须走 aetherupload 的三段式编码（register_upload 靠它回填 original_name）"
    );
}

/// I-1：ingest 失败（此处用「桶不存在」触发）必须把内容从秒传索引里摘掉 ——
/// 否则同内容重传会命中秒传、拿到一个根本没进桶的 savedPath。panic 是刻意的
/// （controller 侧 catch_unwind 吞掉，见 aether.rs 注释）。
#[tokio::test(flavor = "multi_thread")]
async fn ingest_failure_clears_instant_index() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    // 故意不建 "files" 桶 —— put 必失败（NoSuchBucket）

    let sub = "2026-10";
    let group_dir = st.cfg.staging_dir.join("file").join(sub);
    std::fs::create_dir_all(&group_dir).unwrap();
    let body = b"doomed upload";
    let md5 = format!("{:x}", {
        use md5::Digest as _;
        let mut h = md5::Md5::new();
        h.update(body);
        h.finalize()
    });
    let name = format!("{md5}.txt");
    std::fs::write(group_dir.join(&name), body).unwrap();
    let resource = Resource::new(&st.cfg.staging_dir, "file", "file", sub, &name);

    // 模拟控制器在写索引之后才触发事件（upload.rs:396 先 set 再 emit）
    let index_key = format!("{KEY_PREFIX}file_{md5}"); // KEY_PREFIX = "aetherupload:resource:"
    st.db
        .instant_set(&index_key, &SavedPath::encode("file", sub, &name), 3600)
        .await
        .unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        st.ingest.upload_complete(&resource);
    }));

    assert!(result.is_err(), "ingest 失败必须以 panic 让服务端可见");
    assert_eq!(
        st.db.instant_get(&index_key).await.unwrap(),
        None,
        "失败后索引必须被摘掉，否则同内容重传被钉死"
    );
    assert!(st.db.list_uploads(10).await.unwrap().is_empty());
}

/// 分组没配目标桶：跳过 ingest（只告警），不 panic、不落对象、不写上传表。
/// 用单线程 runtime 正是要证明这条早返回路径不碰 `block_in_place`。
#[tokio::test]
async fn sink_without_bucket_skips_ingest() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.store.create_bucket("files").await.unwrap();

    let mut cfg = (*st.cfg).clone();
    cfg.groups.clear();
    let sink = IngestSink {
        store: Arc::clone(&st.store),
        db: Arc::clone(&st.db),
        cfg: Arc::new(cfg),
    };

    let sub = "2026-10";
    let group_dir = st.cfg.staging_dir.join("file").join(sub);
    std::fs::create_dir_all(&group_dir).unwrap();
    let body = b"no bucket for this group";
    let md5 = format!("{:x}", {
        use md5::Digest as _;
        let mut h = md5::Md5::new();
        h.update(body);
        h.finalize()
    });
    let name = format!("{md5}.txt");
    std::fs::write(group_dir.join(&name), body).unwrap();

    let resource = Resource::new(&st.cfg.staging_dir, "file", "file", sub, &name);
    sink.upload_complete(&resource);

    assert!(
        st.store
            .head_object("files", &format!("{sub}/{name}"))
            .await
            .is_err()
    );
    assert!(st.db.list_uploads(10).await.unwrap().is_empty());
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
