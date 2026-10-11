// 运维命令（groups / build / clean）的进程内冒烟：
// 参数解析在 src/cli.rs 的单测里，这里验证三条命令真跑一遍的落盘效果。
// 进程级调用（`cargo run -- build` 等）属人工冒烟，见 README「运维命令」。

mod common;

use std::path::Path;
use std::time::{Duration, SystemTime};

use common::state;
use oss_server::cli::{self, Command};

#[tokio::test(flavor = "multi_thread")] // build 走 SqlInstantStore → block_in_place，单线程 runtime 会 panic
async fn ops_commands_groups_build_clean() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;

    // groups：建出分组目录；幂等，再跑一次仍是 0
    assert_eq!(cli::run(Command::Groups, st.clone()).await.unwrap(), 0);
    assert!(st.cfg.staging_dir.join("file").is_dir());
    assert_eq!(cli::run(Command::Groups, st.clone()).await.unwrap(), 0);

    // build：磁盘上一个成品 + 一个分块残留 → 只有成品进秒传索引
    let group_dir = st.cfg.staging_dir.join("file/202610");
    std::fs::create_dir_all(&group_dir).unwrap();
    let finished = group_dir.join("d41d8cd98f00b204e9800998ecf8427e.jpg");
    std::fs::write(&finished, b"finished").unwrap();
    std::fs::write(group_dir.join("deadbeef.part"), b"chunk").unwrap();

    assert_eq!(cli::run(Command::Build, st.clone()).await.unwrap(), 0);
    assert_eq!(
        st.db
            .instant_get("aetherupload:resource:file_d41d8cd98f00b204e9800998ecf8427e")
            .await
            .unwrap()
            .as_deref(),
        Some("file_202610_d41d8cd98f00b204e9800998ecf8427e.jpg")
    );
    assert!(
        st.db
            .instant_get("aetherupload:resource:file_deadbeef")
            .await
            .unwrap()
            .is_none(),
        "分块文件不进索引"
    );

    // clean：拨老 mtime 的断点 / 分块 / 内核 tmp 孤儿目录被删，成品与新鲜文件留下
    let header_dir = st.cfg.staging_dir.join("_header");
    std::fs::create_dir_all(&header_dir).unwrap();
    let old_header = header_dir.join("0123456789abcdef");
    let old_part = group_dir.join("0123456789abcdef.jpg.part");
    let fresh_part = group_dir.join("fresh.part");
    let tmp_orphan = st.cfg.data_dir.join(".oss.sys/tmp/orphan-uuid");
    for path in [&old_header, &old_part, &fresh_part] {
        std::fs::write(path, b"x").unwrap();
    }
    std::fs::create_dir_all(&tmp_orphan).unwrap();
    std::fs::write(tmp_orphan.join("part.1"), b"x").unwrap();

    // 拨老 mtime 必须在写入之后：往目录里写文件会刷新目录自身的 mtime
    let three_days_ago = SystemTime::now() - Duration::from_secs(3 * 86_400);
    for path in [&old_header, &old_part, &tmp_orphan] {
        set_mtime(path, three_days_ago);
    }

    assert_eq!(
        cli::run(Command::Clean { days: 2 }, st.clone())
            .await
            .unwrap(),
        0
    );
    assert!(!old_header.exists(), "过期断点应被删");
    assert!(!old_part.exists(), "过期分块应被删");
    assert!(!tmp_orphan.exists(), "过期 tmp 孤儿目录应被删");
    assert!(fresh_part.exists(), "新鲜分块不能被删");
    assert!(finished.exists(), "成品不能被清");
}

/// 用 `touch` 造旧 mtime（与 src/cli.rs 的单测同款，避免为测试引第三方 crate）。
fn set_mtime(path: &Path, when: SystemTime) {
    let secs = when
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let ok = std::process::Command::new("touch")
        .arg("-d")
        .arg(format!("@{secs}"))
        .arg(path)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    assert!(ok, "touch 失败");
}
