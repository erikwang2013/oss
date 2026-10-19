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
用法：oss-server [serve|groups|build|clean <days>|fsck [--repair]|heal|scrub|rebalance [--dry-run|--apply]]

  serve          启动服务（默认，无参数时同此）
  groups         建分组目录（幂等；不建就一定上传失败）
  build          按磁盘现状重建秒传索引
  clean <days>   按 mtime 回收超过 days 天的残骸（暂存盘 _header 与 *.part；数据盘 trash/tmp 孤儿与未引用的版本目录；days 限 1..=36525）
  fsck [--repair] 全盘巡检（全版本链：孤儿版本目录、EC 分片缺/坏、tmp/trash 计数）；--repair 执行最小修复（删孤儿 + 逐版本读时修）；报告落 .oss.sys/fsck/<时间戳>-fsck.json
  heal           fsck --repair 全量 + 缺失桶目录补建（集群差集；每节点各自执行）；报告 <时间戳>-heal.json
  scrub          全量深度巡检（只读）：逐块 12×W 对偶校验可定位坏片、历史版本全覆盖；深度失败 exit 1（可重建的坏/缺块不退出非零——口径见 README）；报告 <时间戳>-scrub.json
  rebalance [--dry-run|--apply]
                 扩容搬迁（默认 dry-run）：把存量对象迁到展开放置（placement=v2；未开则空跑）；
                 逐对象 copy→锁内翻转→清老片，幂等可重跑；报告 <时间戳>-rebalance[-dry].json
";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Serve,
    Groups,
    Build,
    Clean {
        days: i64,
    },
    Fsck {
        repair: bool,
    },
    /// M4 切片 4：fsck --repair 的强化（额外补建集群差集报出的缺失桶目录）
    Heal,
    /// M4 切片 4：全量深度巡检（逐块对偶校验，只读不修）
    Scrub,
    /// M4 切片 5：扩容后搬迁（dry_run 只报告）
    Rebalance {
        dry_run: bool,
    },
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
        ("heal", []) => Ok(Command::Heal),
        ("scrub", []) => Ok(Command::Scrub),
        ("rebalance", []) => Ok(Command::Rebalance { dry_run: true }),
        ("rebalance", [flag]) if flag == "--dry-run" => Ok(Command::Rebalance { dry_run: true }),
        ("rebalance", [flag]) if flag == "--apply" => Ok(Command::Rebalance { dry_run: false }),
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

/// serve 的作业动作分派：Inventory 走 S5 生成器，Batch* 走 S3 执行器，其余落 M8 S4b。
struct ServeTaskExecutor {
    tier: std::sync::Arc<crate::tier_jobs::M8TaskExecutor>,
    inventory: std::sync::Arc<crate::inventory::InventoryRunner>,
    batch: std::sync::Arc<crate::job_actions::BatchExecutor>,
}

#[async_trait::async_trait]
impl crate::jobs::TaskExecutor for ServeTaskExecutor {
    async fn execute(&self, action: &str, payload: &str) -> Result<String, String> {
        if action == "Inventory" {
            crate::jobs::TaskExecutor::execute(self.inventory.as_ref(), action, payload).await
        } else if action.starts_with("Batch") {
            crate::jobs::TaskExecutor::execute(self.batch.as_ref(), action, payload).await
        } else {
            crate::jobs::TaskExecutor::execute(self.tier.as_ref(), action, payload).await
        }
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

            // M4 切片 6：生命周期 worker（serve 专属；运维命令不挂）。
            // 间隔 OSS_LIFECYCLE_INTERVAL_SECS（默认 60；0=关闭）
            crate::lifecycle::spawn_worker(state.clone());
            // M6 切片 0：事件 outbox worker（OSS_EVENT_INTERVAL_SECS，0=关）
            crate::events::spawn_worker(
                std::sync::Arc::clone(&state.events),
                state.db.pool().clone(),
            );
            // M7：站点控制面 changelog worker（OSS_SITE_INTERVAL_SECS，0=关）
            if state.site_enabled {
                crate::site::spawn_site_worker(state.db.pool().clone());
            }
            // M6 切片 4：复制 outbox worker（OSS_REPL_INTERVAL_SECS，0=关）
            crate::replication::spawn_repl_worker(
                state.db.pool().clone(),
                std::sync::Arc::clone(&state.store),
                std::sync::Arc::clone(&state.repl_client),
            );
            // C5：审计保留清理（默认关闭；OSS_AUDIT_RETENTION_DAYS=N 天）
            crate::lifecycle::spawn_audit_cleanup(state.clone());
            // M8 S0：通用作业引擎（S4b 的 Transition/Restore + S3 Batch + S5 Inventory；0 关闭）
            let executor = std::sync::Arc::new(ServeTaskExecutor {
                tier: std::sync::Arc::new(crate::tier_jobs::M8TaskExecutor::new(
                    std::sync::Arc::clone(&state.db),
                    std::sync::Arc::clone(&state.store),
                    std::sync::Arc::clone(&state.tiers),
                    std::sync::Arc::clone(&state.events),
                )),
                inventory: std::sync::Arc::clone(&state.inventory_runner),
                // Batch：与在线写路径共用同一组 sink（不发桶通知）
                batch: std::sync::Arc::new(crate::job_actions::BatchExecutor::new(
                    std::sync::Arc::clone(&state.db),
                    std::sync::Arc::clone(&state.store),
                    state.acl(),
                    std::sync::Arc::clone(&state.restore_sink),
                    std::sync::Arc::clone(&state.olap_cache),
                    crate::job_actions::BatchMutationHooks {
                        repl: state.repl_sink.clone()
                            as std::sync::Arc<dyn xl_s3::replication::ReplicationSink>,
                        tier_gc: std::sync::Arc::clone(&state.tier_gc),
                    },
                )),
            });
            crate::jobs::spawn_workers(
                std::sync::Arc::clone(&state.db),
                std::sync::Arc::clone(&state.store),
                executor,
            );
            // M8 S4b：restore 启动对账 + Transition sweep / 过期 reaper 周期
            crate::tier_jobs::spawn(
                std::sync::Arc::clone(&state.db),
                std::sync::Arc::clone(&state.store),
                std::sync::Arc::clone(&state.lifecycle),
                std::sync::Arc::clone(&state.restore_sink),
            );
            // M8 S5：Inventory 调度（OSS_INVENTORY_INTERVAL_SECS，0=关）
            crate::inventory::spawn_worker(std::sync::Arc::clone(&state.db));

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
            // 报告落盘：`.oss.sys/fsck/<时间戳>-<kind>.json`（设计 §4.1 预留目录；写失败不掩结论）
            dump_report(&state, "fsck", &report);
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
        Command::Heal => {
            let report = match state.store.heal().await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Error: heal 失败: {e}");
                    return Ok(1);
                }
            };
            dump_report(&state, "heal", &report);
            println!(
                "heal: 缺键补建 {} / 桶目录补建 {} / 孤儿目录 {}（已删）/ 分片缺 {} 坏 {}{}",
                report.repaired_meta_keys,
                report.repaired_bucket_dirs,
                report.orphan_dirs.len(),
                report.missing_shards,
                report.bad_shards,
                if report.unreachable_nodes.is_empty() {
                    String::new()
                } else {
                    format!(" / 不可达节点 {:?}", report.unreachable_nodes)
                }
            );
            Ok(0)
        }
        Command::Rebalance { dry_run } => {
            let report = match state.store.rebalance(dry_run).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Error: rebalance 失败: {e}");
                    return Ok(1);
                }
            };
            dump_report(
                &state,
                if dry_run {
                    "rebalance-dry"
                } else {
                    "rebalance"
                },
                &report,
            );
            println!(
                "rebalance{}: v2={} / 扫描 {} 对象 / 搬迁 {} 对象 {} 片 {:.1} MiB / 跳过 {}{}",
                if dry_run { "（dry-run）" } else { "" },
                report.placement_v2,
                report.objects_scanned,
                report.objects_moved,
                report.shards_moved,
                report.bytes_moved as f64 / 1048576.0,
                report.skipped.len(),
                if report.skipped.is_empty() {
                    String::new()
                } else {
                    format!(
                        "（下一轮重跑可续）：{:?}",
                        &report.skipped[..report.skipped.len().min(5)]
                    )
                }
            );
            Ok(0)
        }
        Command::Scrub => {
            let report = match state.store.scrub().await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Error: scrub 失败: {e}");
                    return Ok(1);
                }
            };
            dump_report(&state, "scrub", &report);
            println!(
                "scrub: {} 盘 / {} 键 / {} 版本（健康 {}）/ 深度失败 {} / 缺块 {} 坏块 {}{}",
                report.drives,
                report.keys_scanned,
                report.versions_scanned,
                report.ok_versions,
                report.failed_versions.len(),
                report.missing_shards,
                report.bad_shards,
                if report.unreachable_nodes.is_empty() {
                    String::new()
                } else {
                    format!(" / 不可达节点 {:?}", report.unreachable_nodes)
                }
            );
            for f in report.failed_versions.iter().take(20) {
                println!("  失败: {f}");
            }
            // 深度失败 = 非零退出（cron/监控可盯）
            Ok(if report.failed_versions.is_empty() {
                0
            } else {
                1
            })
        }
    }
}

/// 巡检报告落盘（fsck/heal/scrub 共用）：`.oss.sys/fsck/<时间戳>-<kind>.json`；
/// 写失败只警告，不掩命令结论。
fn dump_report(state: &AppState, kind: &str, report: &impl serde::Serialize) {
    let dir = state
        .store
        .root()
        .join(xl_store::layout::SYS_DIR)
        .join("fsck");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("警告: 无法创建报告目录 {}: {e}", dir.display());
        return;
    }
    let ts = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let path = dir.join(format!("{ts}-{kind}.json"));
    match serde_json::to_string_pretty(report) {
        Ok(json) => match std::fs::write(&path, json) {
            Ok(()) => println!("{kind} 报告: {}", path.display()),
            Err(e) => eprintln!("警告: {kind} 报告写入失败 {}: {e}", path.display()),
        },
        Err(e) => eprintln!("警告: {kind} 报告序列化失败: {e}"),
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
            &["heal", "x"][..], // heal/scrub 不收参数
            &["scrub", "--deep"][..],
            &["rebalance", "--go"][..], // rebalance 只认 --dry-run/--apply
            &["rebalance", "x", "y"][..],
            &["/etc/oss.toml"][..], // 旧的「首参=配置路径」不再是合法用法
        ] {
            assert_eq!(parse_args(&args(bad)), Err(USAGE), "应拒绝: {bad:?}");
        }
        // 上限之外的天数也拒绝（挡住按天换算时的乘法溢出）
        assert_eq!(parse_args(&args(&["clean", "36526"])), Err(USAGE));
    }
}
