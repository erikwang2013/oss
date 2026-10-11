//! 命令行入口：`oss-server [serve|groups|build|clean <days>]`。
//!
//! 功能规格 §18.1 的 M1 三条运维命令；三条动作本身在 aetherupload 内核里已有实现
//! （crate 的 `console.rs`），本模块只做「解析参数 → 走同一条启动自举 → 引出 → 退出码」，
//! 外加一处内核管不到的补充：`.oss.sys/tmp/` 的孤儿上传目录（见 [`run`] 的 clean 分支）。

use std::time::{Duration, SystemTime};

use crate::state::AppState;

/// 固定读当前目录的 `oss.toml`（与 M1 的既有行为一致，运维命令也没有别的配置来源）。
pub const CONFIG_PATH: &str = "oss.toml";

pub const USAGE: &str = "\
用法：oss-server [serve|groups|build|clean <days>]

  serve          启动服务（默认，无参数时同此）
  groups         建分组目录（幂等；不建就一定上传失败）
  build          按磁盘现状重建秒传索引
  clean <days>   按 mtime 回收超过 days 天的残骸（暂存盘 _header 与 *.part；数据盘 trash/tmp 孤儿与未引用的版本目录；days 限 1..=36525）
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

/// 内核（`aetherupload::console`）把错误也经同一回调报出（固定前缀 `Error: `）——
/// 分流到 stderr，cron/监控只盯 stderr 时也能看到失败原因。
fn say(m: &str) {
    if m.starts_with("Error: ") {
        eprintln!("{m}");
    } else {
        println!("{m}");
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
            let router = crate::router::build_router(state)?;
            let mut app = ecat::App::builder()
                .name("oss")
                .server(ecat::transport_http::HttpServer::new(listen).router(router))
                .build()?;
            app.run().await?;
            Ok(0)
        }
        // 注：`build_runtime` 自己已经跑过一次 `list_groups`（把分组目录建出来），
        // 所以这里再调时通常只输出「Group-Directory List:」——目录确已就位。
        Command::Groups => {
            let runtime = crate::aether::build_runtime(&state)?;
            Ok(exit_code(aetherupload::console::list_groups(
                &runtime, &mut say,
            )))
        }
        Command::Build => {
            let runtime = crate::aether::build_runtime(&state)?;
            Ok(exit_code(aetherupload::console::build_redis_hashes(
                &runtime, &mut say,
            )))
        }
        Command::Clean { days } => {
            let runtime = crate::aether::build_runtime(&state)?;
            let code =
                aetherupload::console::clean_up_directory(&runtime, &mut |m| println!("{m}"), days);

            // aether 内核只扫暂存盘的 `_header/` 与各分组子目录下的 `*.part`；
            // 数据盘侧（`.oss.sys/{tmp,trash}` 与各对象目录下未被引用且过期的子目录，
            // 含未完成 MPU 暂存区）由 xl-store 的按龄回收负责。
            let due = SystemTime::now() - Duration::from_secs(days as u64 * 86_400);
            match state.store.reap_stale(due).await {
                Ok(n) => {
                    println!("{n} orphan entries have been deleted.");
                    Ok(exit_code(code))
                }
                Err(e) => {
                    eprintln!("Error: 内核回收失败: {e}");
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
}
