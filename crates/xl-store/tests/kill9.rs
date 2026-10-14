//! Task 9：断电一致性（kill -9 的确定性版 = SIGABRT 注入）进程级用例。
//!
//! 四个注入点（评审扩充：覆盖两阶段 meta 提交的两个核心窗口），全部走同一测试二进制重入：
//!   PHASE=1：建桶 + 写 v1（k / k3 / k4 三个对象），正常退出；
//!   PHASE=2：写 k 的 v2，`OSS_TEST_ABORT_AFTER_SHARD=8` → 第 8 片**全部块**落盘后 abort
//!            （M4 分块化后注入点=该片末块；窗口：其余片半写/未写、meta 未碰）；
//!   PHASE=3：写 k3 的 v2，`OSS_TEST_ABORT_META_PREPARE=1` → 两阶段提交「准备完成、一盘未翻」时 abort；
//!   PHASE=4：写 k4 的 v2，`OSS_TEST_ABORT_META_FLIP_K=3` → 翻到第 3 盘时 abort（部分盘已提交）。
//! 父进程断言见各段注释。

use std::path::PathBuf;
use std::process::Command;

use xl_store::store::{Store, StoreConfig};

const CHILD: &str = "OSS_KILL9_CHILD";
const PHASE: &str = "OSS_KILL9_PHASE";
const DIRS: &str = "OSS_KILL9_DIRS";
const ABORT_SHARD: &str = "OSS_TEST_ABORT_AFTER_SHARD";
const ABORT_PREPARE: &str = "OSS_TEST_ABORT_META_PREPARE";
const ABORT_FLIP_K: &str = "OSS_TEST_ABORT_META_FLIP_K";

fn dirs_from_env() -> Vec<PathBuf> {
    std::env::var(DIRS)
        .expect("OSS_KILL9_DIRS")
        .split(':')
        .map(PathBuf::from)
        .collect()
}

async fn open_store() -> Store {
    let mut cfg = StoreConfig::new(&dirs_from_env()[0]);
    cfg.drives = dirs_from_env();
    cfg.fsync = false; // 提速；被测的是「提交点前后的可见性」，与 fsync 开关无关
    Store::open(cfg).await.unwrap()
}

async fn child() {
    let s = open_store().await;
    let _ = s.create_bucket("alpha").await; // 幂等（第二段起桶已存在）
    let v = |b: u8| bytes::Bytes::from(vec![b; 300_000]);
    match std::env::var(PHASE).unwrap().as_str() {
        "1" => {
            s.put_object("alpha", "k", v(1), None).await.unwrap();
            s.put_object("alpha", "k3", v(1), None).await.unwrap();
            s.put_object("alpha", "k4", v(1), None).await.unwrap();
        }
        "2" => {
            let _ = s.put_object("alpha", "k", v(2), None).await;
            unreachable!("abort 注入（分片）未生效：进程存活到了 put 返回");
        }
        "3" => {
            let _ = s.put_object("alpha", "k3", v(2), None).await;
            unreachable!("abort 注入（prepare）未生效");
        }
        "4" => {
            let _ = s.put_object("alpha", "k4", v(2), None).await;
            unreachable!("abort 注入（flip）未生效");
        }
        other => unreachable!("未知 phase {other}"),
    }
}

#[tokio::test]
async fn kill9_before_commit_keeps_old_version_and_no_leak() {
    if std::env::var(CHILD).is_ok() {
        child().await;
        return;
    }

    // 12 盘目录
    let base = tempfile::tempdir().unwrap();
    let dirs: Vec<PathBuf> = (0..12)
        .map(|i| {
            let p = base.path().join(format!("d{i}"));
            std::fs::create_dir_all(&p).unwrap();
            p
        })
        .collect();
    let dirs_env = dirs
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(":");
    let exe = std::env::current_exe().unwrap();
    let spawn = |phase: &str, envs: &[(&str, &str)]| {
        let mut c = Command::new(&exe);
        c.arg("--exact")
            .arg("kill9_before_commit_keeps_old_version_and_no_leak")
            .env(CHILD, "1")
            .env(PHASE, phase)
            .env(DIRS, &dirs_env);
        for (k, v) in envs {
            c.env(k, v);
        }
        c.status().unwrap()
    };
    #[cfg(unix)]
    fn assert_aborted(status: std::process::ExitStatus) {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(6),
            "子进程应死于 abort()（SIGABRT=6），实际 {status:?}"
        );
    }
    #[cfg(not(unix))]
    fn assert_aborted(status: std::process::ExitStatus) {
        assert!(!status.success());
    }

    // ---- 段 1：v1 正常落盘 ----
    assert!(spawn("1", &[]).success(), "v1 写入子进程应正常退出");

    // ---- 段 2：分片阶段崩（第 8 片后）----
    assert_aborted(spawn("2", &[(ABORT_SHARD, "8")]));
    let reopen = || async {
        let mut cfg = StoreConfig::new(&dirs[0]);
        cfg.drives = dirs.clone();
        cfg.fsync = false;
        Store::open(cfg).await.unwrap()
    };
    let s = reopen().await;
    // 注入生效的证据：d0 上 k 的对象目录里有 v2 的版本目录（含已落分片）
    let kd0 = xl_store::layout::object_dir(&dirs[0], "alpha", "k").unwrap();
    let mut subdirs = tokio::fs::read_dir(&kd0).await.unwrap();
    let mut n_dirs = 0;
    while let Some(e) = subdirs.next_entry().await.unwrap() {
        if e.file_type().await.unwrap().is_dir() {
            n_dirs += 1; // 只数版本目录（同目录里还有 xl.meta 文件）
        }
    }
    assert_eq!(n_dirs, 2, "应有 v1/v2 两个版本目录（v2 为半提交残骸）");
    // v1 可见、v2 无影
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert!(bytes.iter().all(|b| *b == 1), "断电恢复后必须是 v1");
    // 启动自检已清 tmp
    let tmp = dirs[0].join(xl_store::layout::SYS_DIR).join("tmp");
    let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
    assert!(
        entries.next_entry().await.unwrap().is_none(),
        "tmp 应被清空"
    );
    // 半提交残骸可回收
    let due = std::time::SystemTime::now() + std::time::Duration::from_secs(1);
    assert!(s.reap_stale(due).await.unwrap() >= 1);
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert!(bytes.iter().all(|b| *b == 1));

    // ---- 段 3：两阶段提交「准备完成、未翻转」时崩 ----
    assert_aborted(spawn("3", &[(ABORT_PREPARE, "1")]));
    let s = reopen().await;
    // v1 可见（v2 的 .new 只是暂存，永不被读为 meta）
    let (_, bytes) = s.get_object("alpha", "k3").await.unwrap();
    assert!(
        bytes.iter().all(|b| *b == 1),
        "prepare 后崩溃：v1 必须仍然可见"
    );
    // 残骸证据：部分盘上留有 xl.meta.new
    let new_exists = {
        let mut found = false;
        for d in &dirs {
            if let Ok(od) = xl_store::layout::object_dir(d, "alpha", "k3") {
                found |= tokio::fs::try_exists(od.join("xl.meta.new"))
                    .await
                    .unwrap_or(false);
            }
        }
        found
    };
    assert!(new_exists, "应留有 .new 暂存 meta（证明注入点在准备之后）");
    // 同键重写自愈：.new 被覆盖、提交正常
    s.put_object("alpha", "k3", bytes::Bytes::from(vec![7u8; 300_000]), None)
        .await
        .unwrap();
    let (_, bytes) = s.get_object("alpha", "k3").await.unwrap();
    assert!(bytes.iter().all(|b| *b == 7));

    // ---- 段 4：翻转中途崩（第 3 盘后）----
    assert_aborted(spawn("4", &[(ABORT_FLIP_K, "3")]));
    let s = reopen().await;
    // 裁决语义：≥1 盘翻转 = 已提交（commit point 不可逆）→ 读侧 latest 胜 → v2
    let (_, bytes) = s.get_object("alpha", "k4").await.unwrap();
    assert!(
        bytes.iter().all(|b| *b == 2),
        "翻转中途崩溃：已翻转的 meta 必须胜出（v2 已提交）"
    );
}
