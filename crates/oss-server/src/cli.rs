//! 命令行入口：`oss-server [serve|groups|build|clean <days>|fsck [--repair]]`。
//!
//! 功能规格 §18.1 的 M1 三条运维命令；三条动作本身在 aetherupload 内核里已有实现
//! （crate 的 `console.rs`），本模块只做「解析参数 → 走同一条启动自举 → 引出 → 退出码」，
//! 外加内核管不到的部分：`.oss.sys/tmp/` 的孤儿上传目录（[`run`] 的 clean 分支）与
//! xl-store 的盘上巡检（fsck 分支，M2b-2 Task 10）。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::state::AppState;

/// 固定读当前目录的 `oss.toml`（与 M1 的既有行为一致，运维命令也没有别的配置来源）。
pub const CONFIG_PATH: &str = "oss.toml";

pub const USAGE: &str = "\
用法：oss-server [serve|groups|build|clean <days>|fsck [--repair]]

  serve          启动服务（默认，无参数时同此）
  groups         建分组目录（幂等；不建就一定上传失败）
  build          按磁盘现状重建秒传索引
  clean <days>   按 mtime 回收超过 days 天的残骸（暂存盘 _header 与 *.part；数据盘 trash/tmp 孤儿与未引用的版本目录；days 限 1..=36525）
  fsck [--repair] 全盘巡检（孤儿版本目录、EC 分片缺/坏、tmp/trash 计数）；--repair 执行最小修复（删孤儿 + 读时修分片）；报告落 .oss.sys/fsck/<时间戳>.json
";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Serve,
    Groups,
    Build,
    Clean { days: i64 },
    Fsck { repair: bool },
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
        ("fsck", []) => Ok(Command::Fsck { repair: false }),
        ("fsck", [flag]) if flag == "--repair" => Ok(Command::Fsck { repair: true }),
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
            // 绑定期检（遗留清单修复）：ecat 把 server 丢进 tokio::spawn，start() 失败
            // （如 EADDRINUSE）只记一条 error 日志，而 run() 继续 wait_for_shutdown 等信号
            // → 端口被占时进程「假活」（活着但无监听，systemd 视作健康）。这里先探测绑定
            // 一次：失败直接上抛 → main 非零退出；成功立即释放，随后 ecat 复绑。
            // 释放与复绑之间有毫秒级竞态窗口（他人恰好抢到）——命中时退化为原假活行为，
            // 由 ecat 的 error 日志暴露；根治需 transport 支持外部 listener（上游增强项）。
            let probe = tokio::net::TcpListener::bind(&listen)
                .await
                .map_err(|e| format!("监听地址 {listen} 绑定失败: {e}"))?;
            drop(probe);

            // M3 集群：起节点间 gRPC（独立端口）。绑定期检同 HTTP——失败即退出，不假活。
            if let Some(cl) = state.cfg.cluster.as_ref() {
                let self_node = cl
                    .nodes
                    .iter()
                    .find(|n| n.id == cl.node_id)
                    .ok_or_else(|| format!("cluster.node_id «{}» 不在节点表内", cl.node_id))?;
                // 地址支持主机名（docker compose 服务名 / DNS 名）：先按 IP:port 解析，
                // 失败走一次解析取首个结果绑定；对端拨号仍用配置原串（tonic 自带 DNS）。
                let addr: std::net::SocketAddr = match self_node.addr.parse() {
                    Ok(a) => a,
                    Err(_) => tokio::net::lookup_host(&self_node.addr)
                        .await
                        .map_err(|e| {
                            format!("cluster 节点地址解析失败（{}）: {e}", self_node.addr)
                        })?
                        .next()
                        .ok_or_else(|| {
                            format!("cluster 节点地址无解析结果（{}）", self_node.addr)
                        })?,
                };
                let probe = tokio::net::TcpListener::bind(addr)
                    .await
                    .map_err(|e| format!("mesh 监听 {addr} 绑定失败: {e}"))?;
                drop(probe);

                // 启动探活（防错接线）：可达且 node_id 不符 → 拒启；不可达只 warn——
                // 滚动启动时对端可能尚未就绪，RPC 时由 health 面持续暴露。
                if let Some(c) = state.store.cluster() {
                    for n in &cl.nodes {
                        if n.id == cl.node_id {
                            continue;
                        }
                        match tokio::time::timeout(
                            Duration::from_millis(800),
                            c.transport.health(&n.id),
                        )
                        .await
                        {
                            Ok(Ok(h)) if h.node_id == n.id => {}
                            Ok(Ok(h)) => {
                                return Err(format!(
                                    "对端 {} 实报 node_id «{}»（错接线？）",
                                    n.id, h.node_id
                                )
                                .into());
                            }
                            Ok(Err(e)) => {
                                tracing::warn!(node = %n.id, error = %e, "启动探活失败（滚动启动时正常）")
                            }
                            Err(_) => {
                                tracing::warn!(node = %n.id, "启动探活超时（滚动启动时正常）")
                            }
                        }
                    }
                }

                let store = Arc::clone(&state.store);
                let secret = cl.secret.clone();
                tokio::spawn(async move {
                    if let Err(e) = xl_client::serve(store, addr, secret).await {
                        // 进程不随 gRPC 退（HTTP 面继续）：对端会将其标 down、quorum 收窄；
                        // 监督/自动重启归 M4 运维面。
                        tracing::error!(error = %e, "mesh gRPC server 退出");
                    }
                });
            }

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
        Command::Fsck { repair } => {
            let report = match state.store.fsck(repair).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Error: fsck 巡检失败: {e}");
                    return Ok(1);
                }
            };
            // 报告落盘：`.oss.sys/fsck/<时间戳>.json`（设计 §4.1 预留目录；写失败不掩结论）
            let dir = state
                .store
                .root()
                .join(xl_store::layout::SYS_DIR)
                .join("fsck");
            if let Err(e) = std::fs::create_dir_all(&dir) {
                eprintln!("警告: 无法创建 fsck 报告目录 {}: {e}", dir.display());
            } else {
                let ts = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let path = dir.join(format!("{ts}.json"));
                match serde_json::to_string_pretty(&report) {
                    Ok(json) => {
                        if let Err(e) = std::fs::write(&path, json) {
                            eprintln!("警告: fsck 报告写入失败 {}: {e}", path.display());
                        } else {
                            println!("fsck 报告: {}", path.display());
                        }
                    }
                    Err(e) => eprintln!("警告: fsck 报告序列化失败: {e}"),
                }
            }
            println!(
                "fsck: {} 盘 / {} 对象 / meta 损坏 {} / 孤儿目录 {} / 分片缺 {} 坏 {} / tmp {} trash {}{}",
                report.drives,
                report.objects_scanned,
                report.corrupt_meta,
                report.orphan_dirs.len(),
                report.missing_shards,
                report.bad_shards,
                report.tmp_entries,
                report.trash_entries,
                if repair { "（已修复）" } else { "" }
            );
            Ok(0)
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
        assert_eq!(
            parse_args(&args(&["fsck"])),
            Ok(Command::Fsck { repair: false })
        );
        assert_eq!(
            parse_args(&args(&["fsck", "--repair"])),
            Ok(Command::Fsck { repair: true })
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
            &["fsck", "--dry-run"][..], // fsck 只认 --repair
            &["fsck", "--repair", "x"][..],
            &["/etc/oss.toml"][..], // 旧的「首参=配置路径」不再是合法用法
        ] {
            assert_eq!(parse_args(&args(bad)), Err(USAGE), "应拒绝: {bad:?}");
        }
        // 上限之外的天数也拒绝（挡住按天换算时的乘法溢出）
        assert_eq!(parse_args(&args(&["clean", "36526"])), Err(USAGE));
    }
}
