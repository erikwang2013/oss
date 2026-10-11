//! 命令行入口：`oss-server [serve|groups|build|clean <days>]`。
//!
//! 功能规格 §18.1 的 M1 三条运维命令；三条动作本身在 aetherupload 内核里已有实现
//! （crate 的 `console.rs`），本模块只做「解析参数 → 走同一条启动自举 → 引出 → 退出码」，
//! 外加一处内核管不到的补充：`.oss.sys/tmp/` 的孤儿上传目录（见 [`run`] 的 clean 分支）。

use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::state::AppState;

/// 固定读当前目录的 `oss.toml`（与 M1 的既有行为一致，运维命令也没有别的配置来源）。
pub const CONFIG_PATH: &str = "oss.toml";

pub const USAGE: &str = "\
用法：oss-server [serve|groups|build|clean <days>]

  serve          启动服务（默认，无参数时同此）
  groups         建分组目录（幂等；不建就一定上传失败）
  build          按磁盘现状重建秒传索引
  clean <days>   按 mtime 回收超过 days 天的临时文件（_header 与 *.part，外加内核 tmp 孤儿目录）
";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Serve,
    Groups,
    Build,
    Clean { days: i64 },
}

/// 解析子命令（入参为去掉 argv[0] 后的参数）。未知子命令或多余参数 → `Err(用法)`（调用方 exit 2）。
///
/// 解析从严、不接受多余参数：`clean 1 --dry-run` 之类的输入宁可直接报错，
/// 也不能因为忽略了多余参数而当真去删。
pub fn parse_args(args: &[String]) -> Result<Command, &'static str> {
    let (cmd, rest) = match args.split_first() {
        None => return Ok(Command::Serve),
        Some(pair) => pair,
    };

    match (cmd.as_str(), rest) {
        ("serve", []) => Ok(Command::Serve),
        ("groups", []) => Ok(Command::Groups),
        ("build", []) => Ok(Command::Build),
        // 上限 100 年：远超任何真实用法，同时挡掉按天换算秒时的乘法溢出。
        ("clean", [days]) => match days.parse::<i64>() {
            Ok(days) if (1..=36_525).contains(&days) => Ok(Command::Clean { days }),
            _ => Err(USAGE),
        },
        _ => Err(USAGE),
    }
}

/// 跑一条命令，返回进程退出码（0 成功 / 1 失败 / serve 正常退出为 0）。
pub async fn run(
    cmd: Command,
    state: AppState,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    match cmd {
        Command::Serve => {
            let listen = state.cfg.listen.clone();
            let router = crate::router::build_router(state);
            let mut app = ecat::App::builder()
                .name("oss")
                .server(ecat::transport_http::HttpServer::new(listen).router(router))
                .build()?;
            app.run().await?;
            Ok(0)
        }
        // 注：`build_runtime` 自己已经跑过一次 `list_groups`（把分组目录建出来），
        // 所以这里再调时通常只输出「Group-Directory List:」——目录确已就位。
        Command::Groups => Ok(exit_code(aetherupload::console::list_groups(
            &crate::aether::build_runtime(&state),
            &mut |m| println!("{m}"),
        ))),
        Command::Build => Ok(exit_code(aetherupload::console::build_redis_hashes(
            &crate::aether::build_runtime(&state),
            &mut |m| println!("{m}"),
        ))),
        Command::Clean { days } => {
            let runtime = crate::aether::build_runtime(&state);
            let code =
                aetherupload::console::clean_up_directory(&runtime, &mut |m| println!("{m}"), days);

            // 内核 clean 只扫 aether 的 `_header/` 与各分组子目录下的 `*.part`；
            // 内核 tmp 区（`.oss.sys/tmp/<uuid>/`，ingest 写盘中途失败的残留）由这里补扫。
            let due = SystemTime::now() - Duration::from_secs(days as u64 * 86_400);
            let tmp = xl_store::layout::sys_dir(state.store.root(), "tmp");

            match reap_stale_tmp(&tmp, due) {
                Ok(n) => {
                    println!("{n} orphan tmp entries have been deleted.");
                    Ok(exit_code(code))
                }
                Err(e) => {
                    eprintln!("Error: 清理 {} 失败: {e}", tmp.display());
                    Ok(1)
                }
            }
        }
    }
}

/// 内核命令的返回码（0/1）→ 进程退出码：非 0 一律记 1。
fn exit_code(code: i32) -> i32 {
    if code == 0 { 0 } else { 1 }
}

/// 回收 `.oss.sys/tmp/` 下 mtime 早于 `due` 的孤儿条目，返回删除个数。
///
/// 为什么可以只按 mtime 判定：tmp 目录在写盘开始时（`tmp/<uuid>/` 刚创建、mtime 是新
/// 的）才存在，正在进行的上传不可能「早于 due」；早于 due 的必是崩溃/失败留下的残骸，
/// 没有任何 meta 引用它。目录形态是 `tmp/<uuid>/part.1`，散落的非目录条目一并清掉。
fn reap_stale_tmp(dir: &Path, due: SystemTime) -> std::io::Result<usize> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        // tmp 目录还没建（存储从未写过）不是错误
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut removed = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let stale = std::fs::metadata(&path)
            .and_then(|meta| meta.modified())
            .is_ok_and(|modified| modified < due);
        if !stale {
            continue;
        }

        let result = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        result.map_err(|e| std::io::Error::other(format!("删除 {} 失败: {e}", path.display())))?;
        removed += 1;
    }

    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_args_defaults_to_serve() {
        assert_eq!(parse_args(&args(&[])), Ok(Command::Serve));
        assert_eq!(parse_args(&args(&["serve"])), Ok(Command::Serve));
    }

    #[test]
    fn parse_args_reads_ops_commands() {
        assert_eq!(parse_args(&args(&["groups"])), Ok(Command::Groups));
        assert_eq!(parse_args(&args(&["build"])), Ok(Command::Build));
        assert_eq!(
            parse_args(&args(&["clean", "3"])),
            Ok(Command::Clean { days: 3 })
        );
    }

    #[test]
    fn parse_args_rejects_bad_input() {
        for bad in [
            &["frobnicate"][..],
            &["serve", "多余参数"][..], // 多余参数一律拒绝
            &["clean"][..],             // 缺 days
            &["clean", "0"][..],        // 非正数
            &["clean", "-1"][..],
            &["clean", "abc"][..],
            &["clean", "1", "x"][..], // 多余参数不算 clean 的 days
            &["clean", "999999999999999999999"][..], // 超出 i64
            &["/etc/oss.toml"][..],   // 旧的「首参=配置路径」不再是合法用法
        ] {
            assert_eq!(parse_args(&args(bad)), Err(USAGE), "应拒绝: {bad:?}");
        }
        // 上限之外的天数也拒绝（挡住按天换算时的乘法溢出）
        assert_eq!(parse_args(&args(&["clean", "36526"])), Err(USAGE));
    }

    #[test]
    fn reap_removes_only_stale_entries() {
        let root = std::env::temp_dir().join(format!("oss-cli-reap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let tmp = root.join(".oss.sys/tmp");

        std::fs::create_dir_all(tmp.join("stale-uuid")).unwrap();
        std::fs::create_dir_all(tmp.join("fresh-uuid")).unwrap();
        std::fs::write(tmp.join("stale-uuid/part.1"), b"x").unwrap();
        std::fs::write(tmp.join("stale-file"), b"x").unwrap();

        // 拨老 mtime 必须在写入之后：往目录里写文件会刷新目录自身的 mtime
        let three_days_ago = SystemTime::now() - Duration::from_secs(3 * 86_400);
        set_mtime(&tmp.join("stale-uuid"), three_days_ago);
        set_mtime(&tmp.join("stale-file"), three_days_ago);

        let due = SystemTime::now() - Duration::from_secs(86_400); // 1 天
        assert_eq!(reap_stale_tmp(&tmp, due).unwrap(), 2);
        assert!(!tmp.join("stale-uuid").exists(), "过期孤儿目录应被删");
        assert!(!tmp.join("stale-file").exists(), "过期散落条目应被删");
        assert!(
            tmp.join("fresh-uuid").exists(),
            "刚创建（进行中上传）的目录不能动"
        );

        // tmp 目录不存在不是错误
        assert_eq!(reap_stale_tmp(&root.join("nope"), due).unwrap(), 0);

        let _ = std::fs::remove_dir_all(&root);
    }

    /// 用 `touch` 造旧 mtime（与 aetherupload 自身测试同款，避免为测试引第三方 crate）。
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
}
