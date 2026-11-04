//! A5: node-local control-object IO (`Store::{get_local_control, put_local_control}`).
//!
//! Parity with the Phase 0 single-node/local-drive path: CAS (`expected` version + strict
//! fence token), inline cap, commit certificates, tombstones, restart, crash residue and
//! drive-quorum fail-closed behavior. The cluster-configured no-fanout case needs the
//! in-process mesh fixture and lives in `store::control_local`'s unit tests.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::join_all;
use xl_store::error::Error;
use xl_store::layout;
use xl_store::meta::{self, InlineData, XlMeta};
use xl_store::store::{Store, StoreConfig};

const KEY: &str = "control/state/root";

fn config(root: &Path) -> StoreConfig {
    let mut cfg = StoreConfig::new(root);
    cfg.fsync = false; // 可见性与 fsync 无关（与既有控制面测试同口径）
    cfg
}

async fn open(root: &Path) -> Store {
    Store::open(config(root)).await.unwrap()
}

/// 12 盘 = 默认 EC 8+4（Q_s = 9），走单节点多盘提交路径。
fn multi_config(dirs: &[PathBuf]) -> StoreConfig {
    let mut cfg = StoreConfig::new(&dirs[0]);
    cfg.drives = dirs.to_vec();
    cfg.fsync = false;
    cfg
}

async fn open_multi(dirs: &[PathBuf]) -> Store {
    Store::open(multi_config(dirs)).await.unwrap()
}

fn temp_drives(n: usize) -> (Vec<tempfile::TempDir>, Vec<PathBuf>) {
    let dirs: Vec<tempfile::TempDir> = (0..n).map(|_| tempfile::tempdir().unwrap()).collect();
    let paths = dirs.iter().map(|d| d.path().to_path_buf()).collect();
    (dirs, paths)
}

fn drive_meta_path(drive: &Path) -> PathBuf {
    layout::object_dir_system(drive, KEY)
        .unwrap()
        .join(meta::META_FILE)
}

fn assert_conflict<T: std::fmt::Debug>(r: Result<T, Error>) {
    match r {
        Err(Error::Conflict(_)) => {}
        other => panic!("应为 Conflict，实际：{other:?}"),
    }
}

#[tokio::test]
async fn control_local_cas_fence_creation_inline_cap_and_key_validation() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(dir.path()).await;

    // 缺席 + expected None = 创建；fence 必须严格递增（缺失按 0）。
    assert!(s.get_local_control(KEY).await.unwrap().is_none());
    assert_conflict(s.put_local_control(KEY, b"x", Some(1), 1).await);
    assert_conflict(s.put_local_control(KEY, b"x", None, 0).await);
    assert_eq!(s.put_local_control(KEY, b"one", None, 1).await.unwrap(), 1);
    let obj = s.get_local_control(KEY).await.unwrap().unwrap();
    assert_eq!(&obj.data[..], b"one");
    assert_eq!((obj.version, obj.fence_token, obj.deleted), (1, 1, false));

    // 版本不匹配 / fence 回归 → Conflict，且失败不改数据。
    assert_conflict(s.put_local_control(KEY, b"x", Some(0), 2).await);
    assert_conflict(s.put_local_control(KEY, b"x", Some(1), 1).await);
    let obj = s.get_local_control(KEY).await.unwrap().unwrap();
    assert_eq!((obj.version, &obj.data[..]), (1, &b"one"[..]));

    // CAS 成功：返回新版本，token 前进。
    assert_eq!(
        s.put_local_control(KEY, b"two", Some(1), 2).await.unwrap(),
        2
    );
    let obj = s.get_local_control(KEY).await.unwrap().unwrap();
    assert_eq!((obj.version, obj.fence_token), (2, 2));

    // 内联上限（与 put_system_object 同错误；对象后端在树层分块，不在这里）。
    let big = vec![0u8; meta::MAX_INLINE as usize + 1];
    assert!(matches!(
        s.put_local_control("control/big", &big, None, 1).await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(s.get_local_control("control/big").await.unwrap().is_none());

    // 只允许 control/ 前缀（与 Phase 0 同键语法）。
    assert!(matches!(
        s.put_local_control("other/a", b"x", None, 1).await,
        Err(Error::InvalidKey(_))
    ));
    assert!(matches!(
        s.get_local_control("other/a").await,
        Err(Error::InvalidKey(_))
    ));
}

/// 两个独立 Store 句柄共用一个 root（≈ 两个进程各开一个 Store）：进程内锁不共享，
/// 互斥必须由 per-key flock + CAS 提供；同一轮只允许一个写者成功。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_local_two_store_handles_allow_exactly_one_writer() {
    let dir = tempfile::tempdir().unwrap();
    let s1 = Arc::new(open(dir.path()).await);
    let s2 = Arc::new(open(dir.path()).await);
    let handles = [s1.clone(), s2.clone()];
    let results = join_all((1..=8u64).map(|token| {
        let s = handles[(token as usize) % 2].clone();
        async move { s.put_local_control("control/race", b"x", None, token).await }
    }))
    .await;
    assert_eq!(
        results.iter().filter(|r| r.is_ok()).count(),
        1,
        "只允许一个写者成功：{results:?}"
    );
    for r in results.iter().filter(|r| r.is_err()) {
        assert!(
            matches!(r, Err(Error::Conflict(_))),
            "其余必须 Conflict：{r:?}"
        );
    }
    let obj = s1.get_local_control("control/race").await.unwrap().unwrap();
    assert_eq!(obj.version, 1);
}

/// 启动互斥：节点控制根上的独占 flock 被第一个进程持有后，第二个 Store 句柄
/// （= 第二个进程的 open fd，flock 按 open file description 计）必须失败。
#[tokio::test]
async fn control_local_root_flock_excludes_second_process() {
    let dir = tempfile::tempdir().unwrap();
    let s1 = open(dir.path()).await;
    let s2 = open(dir.path()).await;
    let guard = s1.lock_local_control_root().unwrap();
    assert!(matches!(
        s2.lock_local_control_root(),
        Err(Error::Encode(_))
    ));
    drop(guard);
    assert!(s2.lock_local_control_root().is_ok());
}

#[tokio::test]
async fn control_local_restart_reopens_committed_root() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = open(dir.path()).await;
        assert_eq!(s.put_local_control(KEY, b"v1", None, 1).await.unwrap(), 1);
        assert_eq!(
            s.put_local_control(KEY, b"v2", Some(1), 2).await.unwrap(),
            2
        );
    }
    let s = open(dir.path()).await;
    let obj = s.get_local_control(KEY).await.unwrap().unwrap();
    assert_eq!(
        (obj.version, obj.fence_token, &obj.data[..]),
        (2, 2, &b"v2"[..])
    );
    assert_eq!(
        s.put_local_control(KEY, b"v3", Some(2), 3).await.unwrap(),
        3
    );
}

/// 第一阶段崩溃残骸（有数据、无提交证书）：读必须 fail-closed（绝不返回 ghost，也不谎报
/// 缺席），后续写必须越过 `max_version`，绝不复用残骸版本号。
#[tokio::test]
async fn control_local_uncommitted_crash_residue_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let s = open(root).await;
    assert_eq!(s.put_local_control(KEY, b"v1", None, 1).await.unwrap(), 1);

    let path = drive_meta_path(root);
    let mut ghost = XlMeta::decode(&std::fs::read(&path).unwrap()).unwrap();
    ghost.control_version = Some(2);
    ghost.fence_token = Some(2);
    ghost.control_committed = Some(false);
    ghost.versions[0].inline = Some(InlineData {
        bytes: b"ghost".to_vec(),
    });
    std::fs::write(&path, ghost.encode().unwrap()).unwrap();

    assert!(matches!(
        s.get_local_control(KEY).await,
        Err(Error::CorruptMeta(_))
    ));
    assert_eq!(s.put_local_control(KEY, b"v3", None, 3).await.unwrap(), 3);
    let obj = s.get_local_control(KEY).await.unwrap().unwrap();
    assert_eq!((obj.version, &obj.data[..]), (3, &b"v3"[..]));
}

/// 多盘半翻：12 盘 W=7，仅 W-1=6 盘写下一代数据、都没有提交证书 → 读只能看到
/// 已提交的旧代，绝不混合两代内容。
#[tokio::test]
async fn control_local_partial_crash_returns_old_generation() {
    let (_dirs, paths) = temp_drives(12);
    let s = open_multi(&paths).await;
    assert_eq!(s.put_local_control(KEY, b"old", None, 1).await.unwrap(), 1);

    let mut ghost = XlMeta::decode(&std::fs::read(drive_meta_path(&paths[0])).unwrap()).unwrap();
    ghost.control_version = Some(2);
    ghost.fence_token = Some(2);
    ghost.control_committed = Some(false);
    ghost.versions[0].inline = Some(InlineData {
        bytes: b"ghost".to_vec(),
    });
    let bytes = ghost.encode().unwrap();
    for path in paths.iter().take(6) {
        std::fs::write(drive_meta_path(path), &bytes).unwrap();
    }

    let obj = s.get_local_control(KEY).await.unwrap().unwrap();
    assert_eq!(
        (obj.version, &obj.data[..]),
        (1, &b"old"[..]),
        "只能看到已提交的旧代，绝不能返回混合内容"
    );
    assert_eq!(
        s.put_local_control(KEY, b"new", Some(1), 3).await.unwrap(),
        3
    );
}

/// 本机盘覆盖 < W（12 盘 → W=7；摘掉 6 盘后剩 6 < 7）：读写都 fail-closed；
/// 恢复盘后旧值原样，绝无部分成功。
#[tokio::test]
async fn control_local_insufficient_drive_coverage_fails_closed() {
    let (_dirs, paths) = temp_drives(12);
    let s = open_multi(&paths).await;
    assert_eq!(s.put_local_control(KEY, b"v1", None, 1).await.unwrap(), 1);

    // 保留 drive0（锁文件所在盘），摘掉其余 6 盘 → 6 盘在线 < W=7。
    for path in paths.iter().skip(1).take(6) {
        std::fs::remove_dir_all(path.join(layout::SYS_DIR)).unwrap();
    }
    assert!(matches!(
        s.get_local_control(KEY).await,
        Err(Error::QuorumUnmet(_))
    ));
    assert!(matches!(
        s.put_local_control(KEY, b"v2", Some(1), 2).await,
        Err(Error::QuorumUnmet(_))
    ));

    for path in paths.iter().skip(1).take(6) {
        std::fs::create_dir_all(path.join(layout::SYS_DIR)).unwrap();
    }
    let obj = s.get_local_control(KEY).await.unwrap().unwrap();
    assert_eq!((obj.version, &obj.data[..]), (1, &b"v1"[..]));
}

/// tombstone 与 Phase 0 共用键形态：已删除 → None；重建按 tombstone 版本 CAS 继续。
#[tokio::test]
async fn control_local_tombstone_reads_none_and_put_resurrects() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(dir.path()).await;
    s.put_system_object(KEY, b"one", None, 1).await.unwrap();
    assert!(s.delete_system_object(KEY, Some(1), 2).await.unwrap());
    assert!(s.get_local_control(KEY).await.unwrap().is_none());
    assert_eq!(
        s.put_local_control(KEY, b"two", Some(2), 3).await.unwrap(),
        3
    );
    let obj = s.get_local_control(KEY).await.unwrap().unwrap();
    assert_eq!((obj.version, &obj.data[..]), (3, &b"two"[..]));
}
