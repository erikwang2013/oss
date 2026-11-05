//! B4 启动断言：对象根引导/锁/mode 校验，SQLite 默认路径零改动。

use super::*;
use xl_store::control_store::sites::SiteStore;
use xl_store::store::StoreConfig;

async fn open_store(root: &std::path::Path) -> Arc<Store> {
    std::fs::create_dir_all(root).unwrap();
    let mut cfg = StoreConfig::new(root);
    cfg.fsync = false;
    Arc::new(Store::open(cfg).await.unwrap())
}

#[tokio::test]
async fn object_startup_bootstraps_root_and_holds_exclusive_lock() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    let store = open_store(&root).await;

    let first = ObjectControl::open(Arc::clone(&store), Mode::Standalone, None)
        .await
        .unwrap();
    let pinned = first.control.pin().await.unwrap();
    assert_eq!(pinned.root.meta.mode, Mode::Standalone);
    let site_id = first
        .control
        .site_identity()
        .await
        .unwrap()
        .expect("对象首启必须种子 site_config 身份行")
        .id;
    assert!(
        local_state_dir(&root).is_dir(),
        "local_store 状态目录必须就绪"
    );
    assert!(!root.join("oss.db").exists(), "对象模式不得创建 oss.db");

    // 同一 root 的第二个进程（独立 open 文件描述符）必须在启动锁处失败。
    let err = ObjectControl::open(Arc::clone(&store), Mode::Standalone, None)
        .await
        .err()
        .expect("第二个写者必须失败");
    assert!(err.to_string().contains("启动锁"), "实际错误：{err}");
    drop(first);

    // 锁释放后可再次启动；已有根与身份行保持不变。
    let again = ObjectControl::open(Arc::clone(&store), Mode::Standalone, None)
        .await
        .unwrap();
    assert_eq!(
        again.control.site_identity().await.unwrap().unwrap().id,
        site_id,
        "重启必须复用已有身份，不得重新生成"
    );
}

#[tokio::test]
async fn object_mode_mismatch_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    let store = open_store(&root).await;

    let first = ObjectControl::open(Arc::clone(&store), Mode::Standalone, Some("site-a"))
        .await
        .unwrap();
    let site = first.control.site_identity().await.unwrap().unwrap();
    assert_eq!(site.id, "site-a");
    drop(first);

    let err = ObjectControl::open(Arc::clone(&store), Mode::Raft, Some("site-a"))
        .await
        .err()
        .expect("standalone 根 + raft 配置必须拒绝");
    assert!(err.to_string().contains("不一致"), "实际错误：{err}");

    let err = ObjectControl::open(Arc::clone(&store), Mode::Standalone, Some("site-b"))
        .await
        .err()
        .expect("站点 id 变更必须拒绝");
    assert!(err.to_string().contains("site-b"), "实际错误：{err}");
}

#[tokio::test]
async fn fresh_object_root_bootstraps_in_requested_mode() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    let store = open_store(&root).await;
    let oc = ObjectControl::open(Arc::clone(&store), Mode::Raft, None)
        .await
        .unwrap();
    assert_eq!(oc.control.pin().await.unwrap().root.meta.mode, Mode::Raft);
}

#[test]
fn expected_mode_maps_raft_config() {
    assert_eq!(expected_mode(false), Mode::Standalone);
    assert_eq!(expected_mode(true), Mode::Raft);
}
