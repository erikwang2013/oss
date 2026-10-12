//! Task 9：断电一致性（kill -9 的确定性版 = SIGABRT 注入）进程级用例。
//!
//! 子进程模式（同一测试二进制重入，两段）：
//!   PHASE=1：建桶 + 写 v1（正常退出）；
//!   PHASE=2：写 v2，`OSS_TEST_ABORT_AFTER_SHARD=8` 使进程在第 8 片落盘后 abort（提交点之前）。
//! 父进程断言：重开 Store → v1 可见（v2 不可见）、启动自检清掉 tmp、半提交分片无 meta 引用
//! 且可被 `reap_stale` 回收。

use std::path::PathBuf;
use std::process::Command;

use xl_store::store::{Store, StoreConfig};

const CHILD: &str = "OSS_KILL9_CHILD";
const PHASE: &str = "OSS_KILL9_PHASE";
const DIRS: &str = "OSS_KILL9_DIRS";
const ABORT: &str = "OSS_TEST_ABORT_AFTER_SHARD";

fn dirs_from_env() -> Vec<PathBuf> {
    std::env::var(DIRS)
        .expect("OSS_KILL9_DIRS")
        .split(':')
        .map(PathBuf::from)
        .collect()
}

async fn child() {
    let mut cfg = StoreConfig::new(&dirs_from_env()[0]);
    cfg.drives = dirs_from_env();
    cfg.fsync = false; // 提速；被测的是「提交点前的可见性」，与 fsync 开关无关
    let s = Store::open(cfg).await.unwrap();
    // 第二段重入时桶已存在（第一段建的）——幂等容忍
    let _ = s.create_bucket("alpha").await;
    let phase = std::env::var(PHASE).unwrap();
    if phase == "2" {
        // abort 注入在 stage_payload_multi 中生效（env 已在 spawn 时注入）
        let _ = s
            .put_object("alpha", "k", bytes::Bytes::from(vec![2u8; 300_000]), None)
            .await;
        unreachable!("abort 注入未生效：进程存活到了 put 返回");
    }
    s.put_object("alpha", "k", bytes::Bytes::from(vec![1u8; 300_000]), None)
        .await
        .unwrap();
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
    let spawn = |phase: &str, abort: Option<&str>| {
        let mut c = Command::new(&exe);
        c.arg("--exact")
            .arg("kill9_before_commit_keeps_old_version_and_no_leak")
            .env(CHILD, "1")
            .env(PHASE, phase)
            .env(DIRS, &dirs_env);
        if let Some(a) = abort {
            c.env(ABORT, a);
        }
        c.status().unwrap()
    };

    // 第一段：v1 正常落盘
    assert!(spawn("1", None).success(), "v1 写入子进程应正常退出");
    // 第二段：v2 写到第 8 片后 SIGABRT
    let status = spawn("2", Some("8"));
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(6),
            "子进程应死于 abort()（SIGABRT=6），实际 {status:?}"
        );
    }
    #[cfg(not(unix))]
    assert!(!status.success());

    // 重启（= 断电恢复）：v1 可见，v2 无影
    let mut cfg = StoreConfig::new(&dirs[0]);
    cfg.drives = dirs.clone();
    cfg.fsync = false;
    let s = Store::open(cfg).await.unwrap();
    let (info, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert_eq!(info.size, 300_000);
    assert!(
        bytes.iter().all(|b| *b == 1),
        "断电恢复后必须是 v1 的字节（v2 从未提交）"
    );

    // 启动自检已清空 drive0 的 tmp（v2 的流式收全残留）
    let tmp = dirs[0].join(".oss.sys").join("tmp");
    let mut entries = tokio::fs::read_dir(&tmp).await.unwrap();
    assert!(
        entries.next_entry().await.unwrap().is_none(),
        "tmp 应被 reap_transient 清空"
    );

    // 半提交分片（8 片已落盘、meta 未翻转）：不被任何 meta 引用，clean 可回收
    let due = std::time::SystemTime::now() + std::time::Duration::from_secs(1);
    let removed = s.reap_stale(due).await.unwrap();
    assert!(
        removed >= 1,
        "半提交残骸应被 reap_stale 回收，实际 {removed}"
    );
    // 回收后再读仍是 v1
    let (_, bytes) = s.get_object("alpha", "k").await.unwrap();
    assert!(bytes.iter().all(|b| *b == 1));
}
