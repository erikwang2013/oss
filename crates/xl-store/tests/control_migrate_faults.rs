//! Task 6：legacy 控制对象补证书的崩溃/中止注入与收敛。
//!
//! 子进程只调用公开 `recertify_system_object_cluster`；父进程用真实盘快照断言中间态
//! 与最终态。两个窗口：证书翻第 1 槽即崩（Indeterminate 现场）、prepare 完成即崩
//! （零翻转、legacy 证据原样）。重跑必须收敛到 ≥Q_s 份同 token 证书。

mod common;

use std::path::{Path, PathBuf};

use common::*;
use xl_store::meta::XlMeta;
use xl_store::store::RecertifyOutcome;

/// 直接写盘上 `xl.meta`（绕过 Store 的两阶段；仅测试构造 legacy 现场）。
fn write_disk_meta(drive: &Path, key: &str, m: &XlMeta) {
    let dir = control_dir(drive, key);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("xl.meta"), m.encode().unwrap()).unwrap();
}

/// 盘上 `xl.meta` 原始字节（缺失 → None；零写入断言用）。
fn disk_meta_bytes(drive: &Path, key: &str) -> Option<Vec<u8>> {
    std::fs::read(control_dir(drive, key).join("xl.meta")).ok()
}

/// 生产形态的 legacy 现场：先用真实两阶段写建立 v1/token1，再把所有副本剥掉证书
/// 直接写盘（模拟迁移前状态，9 份同内容、无 `Some(false)`）。
async fn seed_legacy(dirs: &[PathBuf], key: &str) -> XlMeta {
    let s = open_store(dirs, None, true).await;
    assert_eq!(s.put_system_object(key, b"v1", None, 1).await.unwrap(), 1);
    let mut m = disk_meta(&dirs[0], key);
    assert_eq!(m.control_committed, Some(true), "种子必须是已提交副本");
    m.control_committed = None;
    for d in dirs.iter().take(Q_S) {
        assert!(
            control_dir(d, key).join("xl.meta").exists(),
            "{}：quorum 槽应有种子副本",
            d.display()
        );
        write_disk_meta(d, key, &m);
    }
    drop(s);
    m
}

/// 已提交槽数与其中最大 token。
fn certificate_stats(dirs: &[PathBuf], key: &str) -> (usize, u64) {
    let mut n = 0;
    let mut max = 0;
    for d in dirs {
        if let Some(bytes) = disk_meta_bytes(d, key) {
            let m = XlMeta::decode(&bytes).unwrap();
            if m.control_committed == Some(true) {
                n += 1;
                max = max.max(m.fence_token.unwrap_or(0));
            }
        }
    }
    (n, max)
}

/// 翻第 1 份证书即 SIGABRT：盘上 1 份 `Some(true)` + 8 份 `None`，单份证书已对读可见
/// （这正是 Indeterminate 不得假设回滚的原因）；重跑补齐到 ≥Q_s 份同 token 证书。
#[tokio::test]
async fn legacy_recertify_crash_after_first_certificate_converges() {
    const KEY: &str = "control/migrate-crash-cert";
    const TEST: &str = "legacy_recertify_crash_after_first_certificate_converges";

    if std::env::var(CHILD).is_ok() {
        assert_eq!(std::env::var(PHASE).unwrap(), "flip1");
        let dirs = dirs_from_env();
        let s = open_store(&dirs, None, true).await;
        let r = s.recertify_system_object_cluster(KEY).await;
        unreachable!("abort 注入（control commit K=1）未生效：apply 返回 {r:?}");
    }

    let (_tmp, dirs) = temp_dirs();
    let exe = std::env::current_exe().unwrap();
    let legacy = seed_legacy(&dirs, KEY).await;
    let old_token = legacy.fence_token.unwrap();

    assert_aborted(spawn(
        &exe,
        TEST,
        "flip1",
        &dirs,
        &[(ABORT_CONTROL_COMMIT_K, "1")],
    ));

    // 崩溃现场：1 份证书 + 8 份 legacy；其余 11 槽只剩 `.new`（首槽已 rename）。
    let mut certified = 0;
    let mut legacy_n = 0;
    let mut crash_token = 0;
    for d in &dirs {
        let Some(bytes) = disk_meta_bytes(d, KEY) else {
            continue;
        };
        let m = XlMeta::decode(&bytes).unwrap();
        match m.control_committed {
            Some(true) => {
                certified += 1;
                crash_token = m.fence_token.unwrap();
            }
            None => legacy_n += 1,
            other => panic!("意外证书状态 {other:?}"),
        }
    }
    assert_eq!((certified, legacy_n), (1, 8));
    assert!(crash_token > old_token, "证书 token {crash_token} 未推进");
    assert_eq!(slot_counts(&dirs, KEY), (N_DRIVES - 3, N_DRIVES - 1));

    // 单份证书已 elect 可见（协议：控制面 ≥1 份证书 = 已提交）。
    let s = open_store(&dirs, None, true).await;
    let obj = s
        .get_system_object(KEY)
        .await
        .unwrap()
        .expect("崩溃落盘的 1 份证书必须可见");
    assert_eq!((obj.version, &obj.data[..]), (1, &b"v1"[..]));
    assert_eq!(obj.fence_token, crash_token);

    // 重跑走“最高 rank 证书组总副本 ≥Q_s”的补齐分支，覆盖崩溃证书与 legacy 副本。
    let out = s.recertify_system_object_cluster(KEY).await.unwrap();
    let RecertifyOutcome::Certified {
        version,
        certified: flipped,
        token,
    } = out
    else {
        panic!("重跑实得：{out:?}");
    };
    assert_eq!(version, 1);
    assert!(flipped >= Q_S, "补齐翻转 {flipped} < Q_s");
    assert!(token > crash_token, "重跑 token {token} 未严格递增");
    let (certs, max) = certificate_stats(&dirs, KEY);
    assert!(certs >= Q_S, "最终证书 {certs} < Q_s");
    assert_eq!(max, token);
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!((obj.version, &obj.data[..]), (1, &b"v1"[..]));
}

/// prepare 完成即 SIGABRT：9 份 legacy `xl.meta` 字节逐一不变（仅 12 槽 `.new` 暂存）；
/// 重跑仍能认证并收敛。
#[tokio::test]
async fn legacy_recertify_crash_before_marker_keeps_legacy_and_reruns() {
    const KEY: &str = "control/migrate-crash-prepare";
    const TEST: &str = "legacy_recertify_crash_before_marker_keeps_legacy_and_reruns";

    if std::env::var(CHILD).is_ok() {
        assert_eq!(std::env::var(PHASE).unwrap(), "prepare");
        let dirs = dirs_from_env();
        let s = open_store(&dirs, None, true).await;
        let _ = s.recertify_system_object_cluster(KEY).await;
        unreachable!("abort 注入（meta prepare）未生效：apply 返回了");
    }

    let (_tmp, dirs) = temp_dirs();
    let exe = std::env::current_exe().unwrap();
    let legacy = seed_legacy(&dirs, KEY).await;
    let old_token = legacy.fence_token.unwrap();
    let before: Vec<Option<Vec<u8>>> = dirs.iter().map(|d| disk_meta_bytes(d, KEY)).collect();

    assert_aborted(spawn(&exe, TEST, "prepare", &dirs, &[(ABORT_PREPARE, "1")]));

    // prepare 崩溃不得改写任何已提交字节；12 槽全部只有 `.new` 暂存。
    let after: Vec<Option<Vec<u8>>> = dirs.iter().map(|d| disk_meta_bytes(d, KEY)).collect();
    assert_eq!(after, before, "prepare 崩溃必须保留全部 legacy xl.meta");
    assert_eq!(slot_counts(&dirs, KEY), (Q_S, N_DRIVES));

    let s = open_store(&dirs, None, true).await;
    let out = s.recertify_system_object_cluster(KEY).await.unwrap();
    let RecertifyOutcome::Certified {
        version,
        certified: flipped,
        token,
    } = out
    else {
        panic!("重跑实得：{out:?}");
    };
    assert_eq!(version, 1);
    assert!(flipped >= Q_S, "补齐翻转 {flipped} < Q_s");
    assert!(token > old_token, "重跑 token {token} 未严格递增");
    let (certs, max) = certificate_stats(&dirs, KEY);
    assert!(certs >= Q_S, "最终证书 {certs} < Q_s");
    assert_eq!(max, token);
    let obj = s.get_system_object(KEY).await.unwrap().unwrap();
    assert_eq!((obj.version, &obj.data[..]), (1, &b"v1"[..]));
}
