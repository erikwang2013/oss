//! 命令行入口：`oss-server [serve|groups|build|clean <days>|fsck [--repair]]`。
//!
//! 功能规格 §18.1 的 M1 三条运维命令；三条动作本身在 aetherupload 内核里已有实现
//! （crate 的 `console.rs`），本模块只做「解析参数 → 走同一条启动自举 → 引出 → 退出码」，
//! 外加内核管不到的部分：`.oss.sys/tmp/` 的孤儿上传目录（[`run`] 的 clean 分支）与
//! xl-store 的盘上巡检（fsck 分支，M2b-2 Task 10）。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::state::AppState;

/// 默认读当前目录的 `oss.toml`；`--config <path>` 可覆盖（离线/管理命令必须能换库）。
pub const CONFIG_PATH: &str = "oss.toml";

pub const USAGE: &str = "\
用法：oss-server [--config <path>] <命令> [选项]

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
  witness        启动 witness 控制面节点（读 --config 的 [raft]；无数据目录/S3 面）
  raft-add-witness --node-id N --endpoint <advertise_url> [--replace OLD]
                 通过本机 advertise_endpoint 的 HTTP 成员路由加 witness（不二次打开 raft.db）
  raft-remove-node --node-id N
                 同上，移除成员
  raft-export [--out PATH] [--witness]
                 导出复制子集（默认 stdout；--witness 读 [raft].state_db）
  raft-cutover --from PATH [--peer-config PATH] [--dry-run] [--overwrite]
                 以导出文件割接控制面（S7.5）
  raft-force-recover --from PATH --node-id N --yes
                 从导出文件重建 N 的单节点集群（数据站点或 witness 配置；S7.5）
  sync-reconcile [--repair] [--bucket B] [--json]
                 B′ 站点对账（S7.5）
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
    /// M9 S7.1：witness 控制面节点（独立运行时，不建 AppState）。
    Witness,
    /// M9 S7.1：经本机 Raft HTTP 路由加 witness（成员变更由 S7.3 实现）。
    RaftAddWitness {
        node_id: u64,
        endpoint: String,
        replace: Option<u64>,
    },
    RaftRemoveNode {
        node_id: u64,
    },
    /// M9 S7.5（S7.1 只固定 CLI 形状）
    RaftExport {
        out: Option<std::path::PathBuf>,
        witness: bool,
    },
    RaftCutover {
        from: std::path::PathBuf,
        peer_config: Option<std::path::PathBuf>,
        dry_run: bool,
        overwrite: bool,
    },
    RaftForceRecover {
        from: std::path::PathBuf,
        new_node_id: u64,
        yes: bool,
    },
    SyncReconcile {
        repair: bool,
        bucket: Option<String>,
        json: bool,
    },
}

/// 取出 `--config <path>` / `--config=<path>`（恰好一次；缺省 `oss.toml`）。
/// 其余参数原样返回给 [`parse_args`]。
pub fn split_config(args: &[String]) -> Result<(std::path::PathBuf, Vec<String>), &'static str> {
    let mut out = Vec::with_capacity(args.len());
    let mut cfg: Option<std::path::PathBuf> = None;
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        let value = if a == "--config" {
            i += 1;
            args.get(i).ok_or(USAGE)?
        } else if let Some(v) = a.strip_prefix("--config=") {
            v
        } else {
            out.push(a.clone());
            i += 1;
            continue;
        };
        if value.is_empty() || cfg.is_some() {
            return Err(USAGE);
        }
        cfg = Some(std::path::PathBuf::from(value));
        i += 1;
    }
    Ok((
        cfg.unwrap_or_else(|| std::path::PathBuf::from(CONFIG_PATH)),
        out,
    ))
}

/// 命令是否需要完整 `AppState::load`。离线/管理命令必须绕开（否则二次打开 raft.db）。
pub fn needs_full_state(cmd: &Command) -> bool {
    !matches!(
        cmd,
        Command::Witness
            | Command::RaftAddWitness { .. }
            | Command::RaftRemoveNode { .. }
            | Command::RaftExport { .. }
            | Command::RaftCutover { .. }
            | Command::RaftForceRecover { .. }
            | Command::SyncReconcile { .. }
    )
}

/// 严格标志解析：`--name value` / `--name=value`，重复或未知 → 用法错误。
fn parse_flags(
    args: &[String],
    value_flags: &[&str],
    bool_flags: &[&str],
) -> Result<
    (
        std::collections::BTreeMap<String, String>,
        std::collections::BTreeSet<String>,
    ),
    &'static str,
> {
    let mut values = std::collections::BTreeMap::new();
    let mut flags = std::collections::BTreeSet::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if bool_flags.contains(&a.as_str()) {
            if !flags.insert(a.clone()) {
                return Err(USAGE);
            }
            i += 1;
        } else if value_flags.contains(&a.as_str()) {
            let v = args
                .get(i + 1)
                .filter(|v| !v.starts_with("--"))
                .ok_or(USAGE)?;
            if values.insert(a.clone(), v.clone()).is_some() {
                return Err(USAGE);
            }
            i += 2;
        } else if let Some((k, v)) = a.split_once('=') {
            if v.is_empty() || !value_flags.contains(&k) {
                return Err(USAGE);
            }
            if values.insert(k.to_string(), v.to_string()).is_some() {
                return Err(USAGE);
            }
            i += 1;
        } else {
            return Err(USAGE);
        }
    }
    Ok((values, flags))
}

fn req_u64(
    values: &std::collections::BTreeMap<String, String>,
    name: &str,
) -> Result<u64, &'static str> {
    values
        .get(name)
        .ok_or(USAGE)?
        .parse::<u64>()
        .map_err(|_| USAGE)
}

fn opt_u64(
    values: &std::collections::BTreeMap<String, String>,
    name: &str,
) -> Result<Option<u64>, &'static str> {
    values
        .get(name)
        .map(|v| v.parse::<u64>().map_err(|_| USAGE))
        .transpose()
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
        ("witness", []) => Ok(Command::Witness),
        ("raft-add-witness", rest) => {
            let (v, _) = parse_flags(rest, &["--node-id", "--endpoint", "--replace"], &[])?;
            let node_id = req_u64(&v, "--node-id")?;
            let endpoint = v
                .get("--endpoint")
                .filter(|e| e.starts_with("http://"))
                .cloned()
                .ok_or(USAGE)?;
            Ok(Command::RaftAddWitness {
                node_id,
                endpoint,
                replace: opt_u64(&v, "--replace")?,
            })
        }
        ("raft-remove-node", rest) => {
            let (v, _) = parse_flags(rest, &["--node-id"], &[])?;
            Ok(Command::RaftRemoveNode {
                node_id: req_u64(&v, "--node-id")?,
            })
        }
        ("raft-export", rest) => {
            let (v, flags) = parse_flags(rest, &["--out"], &["--witness"])?;
            Ok(Command::RaftExport {
                out: v.get("--out").map(std::path::PathBuf::from),
                witness: flags.contains("--witness"),
            })
        }
        ("raft-cutover", rest) => {
            let (v, flags) = parse_flags(
                rest,
                &["--from", "--peer-config"],
                &["--dry-run", "--overwrite"],
            )?;
            Ok(Command::RaftCutover {
                from: v.get("--from").map(std::path::PathBuf::from).ok_or(USAGE)?,
                peer_config: v.get("--peer-config").map(std::path::PathBuf::from),
                dry_run: flags.contains("--dry-run"),
                overwrite: flags.contains("--overwrite"),
            })
        }
        ("raft-force-recover", rest) => {
            let (v, flags) = parse_flags(rest, &["--from", "--node-id"], &["--yes"])?;
            Ok(Command::RaftForceRecover {
                from: v.get("--from").map(std::path::PathBuf::from).ok_or(USAGE)?,
                new_node_id: req_u64(&v, "--node-id")?,
                yes: flags.contains("--yes"),
            })
        }
        ("sync-reconcile", rest) => {
            let (v, flags) = parse_flags(rest, &["--bucket"], &["--repair", "--json"])?;
            Ok(Command::SyncReconcile {
                repair: flags.contains("--repair"),
                bucket: v.get("--bucket").cloned(),
                json: flags.contains("--json"),
            })
        }
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

/// M3/M9：`host:port`（可带 `http://` 前缀）→ 可绑定地址，并做绑定期检（失败即启动失败，
/// 不假活）。地址支持主机名（docker compose 服务名 / DNS 名）：先按 IP:port 解析，
/// 失败走一次解析取首个结果。
async fn bind_addr(
    raw: &str,
    what: &str,
) -> Result<std::net::SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let raw = raw
        .strip_prefix("http://")
        .or_else(|| raw.strip_prefix("https://"))
        .unwrap_or(raw);
    let addr: std::net::SocketAddr = match raw.parse() {
        Ok(a) => a,
        Err(_) => tokio::net::lookup_host(raw)
            .await
            .map_err(|e| format!("{what} 地址解析失败（{raw}）: {e}"))?
            .next()
            .ok_or_else(|| format!("{what} 地址无解析结果（{raw}）"))?,
    };
    let probe = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("{what} 监听 {addr} 绑定失败: {e}"))?;
    drop(probe);
    Ok(addr)
}

/// 跑一条命令，返回进程退出码（0 成功 / 1 失败 / serve 正常退出为 0）。
pub async fn run(
    cmd: Command,
    state: AppState,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    match cmd {
        Command::Serve => {
            let listen = state.cfg.listen.clone();
            // listen 允许带 http(s):// 前缀（协议由 `[tls.server]` 决定）；绑定前剥离。
            let listen_bind = listen
                .strip_prefix("http://")
                .or_else(|| listen.strip_prefix("https://"))
                .unwrap_or(&listen)
                .to_string();
            // 绑定期检（遗留清单修复）：ecat 把 server 丢进 tokio::spawn，start() 失败
            // （如 EADDRINUSE）只记一条 error 日志，而 run() 继续 wait_for_shutdown 等信号
            // → 端口被占时进程「假活」（活着但无监听，systemd 视作健康）。这里先探测绑定
            // 一次：失败直接上抛 → main 非零退出；成功立即释放，随后 ecat 复绑。
            // 释放与复绑之间有毫秒级竞态窗口（他人恰好抢到）——命中时退化为原假活行为，
            // 由 ecat 的 error 日志暴露；根治需 transport 支持外部 listener（上游增强项）。
            let probe = tokio::net::TcpListener::bind(&listen_bind)
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
                // 对端拨号仍用配置原串（tonic 自带 DNS）。
                let addr = bind_addr(&self_node.addr, "cluster 节点").await?;

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
                let tls = state.tls.tonic_server();
                tokio::spawn(async move {
                    if let Err(e) = xl_client::serve_tls(store, addr, secret, tls).await {
                        // 进程不随 gRPC 退（HTTP 面继续）：对端会将其标 down、quorum 收窄；
                        // 监督/自动重启归 M4 运维面。
                        tracing::error!(error = %e, "mesh gRPC server 退出");
                    }
                });
            }

            // M9 S4：stretch 的 mesh gRPC 独立监听 `self_addr`（与主 HTTP/Raft 端口分离），
            // 与 [cluster] 互斥不会双起；secret 走 `x-oss-cluster-token` 拦截器。
            if let Some(st) = state.cfg.stretch.as_ref() {
                let addr = bind_addr(&st.self_addr, "[stretch].self_addr").await?;
                // 启动探活（防错接线）：可达且 node_id 不符 → 拒启；不可达只 warn。
                if let Some(c) = state.store.cluster() {
                    match tokio::time::timeout(
                        Duration::from_millis(800),
                        c.transport.health(&st.peer_node_id),
                    )
                    .await
                    {
                        Ok(Ok(h)) if h.node_id == st.peer_node_id => {}
                        Ok(Ok(h)) => {
                            return Err(format!(
                                "stretch 对端 {} 实报 node_id «{}»（错接线？）",
                                st.peer_node_id, h.node_id
                            )
                            .into());
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(node = %st.peer_node_id, error = %e, "stretch 启动探活失败（滚动启动时正常）")
                        }
                        Err(_) => {
                            tracing::warn!(node = %st.peer_node_id, "stretch 启动探活超时（滚动启动时正常）")
                        }
                    }
                }
                let store = Arc::clone(&state.store);
                let secret = st.mesh_secret.clone();
                let tls = state.tls.tonic_server();
                tokio::spawn(async move {
                    if let Err(e) = xl_client::serve_tls(store, addr, secret, tls).await {
                        tracing::error!(error = %e, "stretch mesh gRPC server 退出");
                    }
                });
            }

            // M9 S1/S3：sync 写/删 intent 恢复（先跑启动轮，再挂 30s worker）。
            crate::sync::recover_write_intents(&state).await;
            crate::sync::recover_delete_intents(&state).await;
            if state.site_enabled {
                crate::sync::spawn_intent_worker(state.clone());
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
            // M9 S6a：Raft 模式不挂站点 worker（控制写不经 changelog；S6d 再决定替代）。
            if state.site_enabled && state.cfg.raft.is_none() {
                crate::site::spawn_site_worker(
                    state.db.pool().clone(),
                    std::sync::Arc::clone(&state.tls),
                );
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
                // Batch：与在线写路径共用同一组 sink；桶通知由执行器按对象所在桶补发。
                batch: std::sync::Arc::new(crate::job_actions::BatchExecutor::new(
                    std::sync::Arc::clone(&state.db),
                    std::sync::Arc::clone(&state.store),
                    state.acl(),
                    std::sync::Arc::clone(&state.restore_sink),
                    std::sync::Arc::clone(&state.olap_cache),
                    std::sync::Arc::clone(&state.events)
                        as std::sync::Arc<dyn xl_s3::events::EventSink>,
                    crate::job_actions::BatchMutationHooks {
                        repl: state.repl_sink.clone()
                            as std::sync::Arc<dyn xl_s3::replication::ReplicationSink>,
                        tier_gc: std::sync::Arc::clone(&state.tier_gc),
                        sync_delete: state.sync_delete.clone()
                            as std::sync::Arc<dyn xl_s3::sync_delete::SyncDeleteSink>,
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

            // TLS 主监听器走可热加载实现（每个新连接取配置快照）；明文路径保持
            // ecat HttpServer 不变（无 [tls] 时行为与旧版一致）。
            let tls = std::sync::Arc::clone(&state.tls);
            let reload_secs = state.cfg.tls.reload_interval_secs.unwrap_or(30);
            let router = crate::router::build_router(state)?;
            let mut app = if tls.peer_tls() {
                let server = crate::tls_server::ReloadableHttpServer::new(
                    listen_bind,
                    router,
                    std::sync::Arc::clone(&tls),
                );
                ecat::App::builder().name("oss").server(server).build()?
            } else {
                let server = ecat::transport_http::HttpServer::new(listen_bind).router(router);
                ecat::App::builder().name("oss").server(server).build()?
            };
            // SIGHUP 立即触发 + reload_interval_secs 轮询；无 TLS 物料时直接返回。
            crate::tls_server::spawn_reload_watchers(tls, reload_secs);
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
        // main 只对 needs_full_state 的命令调用 run；离线命令误入是内部接线错误。
        Command::Witness
        | Command::RaftAddWitness { .. }
        | Command::RaftRemoveNode { .. }
        | Command::RaftExport { .. }
        | Command::RaftCutover { .. }
        | Command::RaftForceRecover { .. }
        | Command::SyncReconcile { .. } => {
            Err("内部错误：离线命令不得走 AppState::run（应走 run_offline）".into())
        }
    }
}

/// 不经 `AppState::load` 的命令入口：witness 运行时与 HTTP 成员客户端。
/// 关键：这些命令**不得**再构造第二个 `Raft`/打开同一 `raft.db`。
pub async fn run_offline(
    cmd: Command,
    cfg_path: &std::path::Path,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    match cmd {
        Command::Witness => crate::witness::run(cfg_path).await,
        Command::RaftAddWitness {
            node_id,
            endpoint,
            replace,
        } => {
            let cfg = crate::config::AppConfig::load(cfg_path)?;
            if cfg.raft.is_none() {
                return Err("[raft] 未配置：无法定位本机 advertise_endpoint".into());
            }
            let body = match replace {
                Some(old_node_id) => serde_json::json!({
                    "op": "replace_voter",
                    "node_id": node_id,
                    "endpoint": endpoint,
                    "old_node_id": old_node_id,
                }),
                None => serde_json::json!({
                    "op": "add_witness",
                    "node_id": node_id,
                    "endpoint": endpoint,
                }),
            };
            membership_rpc(&cfg, body).await
        }
        Command::RaftRemoveNode { node_id } => {
            let cfg = crate::config::AppConfig::load(cfg_path)?;
            if cfg.raft.is_none() {
                return Err("[raft] 未配置：无法定位本机 advertise_endpoint".into());
            }
            membership_rpc(
                &cfg,
                serde_json::json!({"op": "remove_node", "node_id": node_id}),
            )
            .await
        }
        Command::RaftExport { out, witness } => export_cmd(cfg_path, out, witness).await,
        Command::RaftCutover {
            from,
            peer_config,
            dry_run,
            overwrite,
        } => cutover_cmd(cfg_path, &from, peer_config.as_deref(), dry_run, overwrite).await,
        Command::RaftForceRecover {
            from,
            new_node_id,
            yes,
        } => force_recover_cmd(cfg_path, &from, new_node_id, yes).await,
        Command::SyncReconcile {
            repair,
            bucket,
            json,
        } => sync_reconcile_cmd(cfg_path, repair, bucket, json).await,
        other => Err(format!("{other:?} 不是离线命令（内部接线错误）").into()),
    }
}

/// 向本机 `[raft].advertise_endpoint` 的成员路由发一次有界 HTTP 请求。
async fn membership_rpc(
    cfg: &crate::config::AppConfig,
    body: serde_json::Value,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let raft = cfg.raft.as_ref().ok_or("[raft] 未配置")?;
    let tls = crate::tls::Tls::load(&cfg.tls)?;
    let endpoint = tls.peer_url(&raft.advertise_endpoint)?;
    let url = format!(
        "{}/api/v1/site/raft/membership",
        endpoint.trim_end_matches('/')
    );
    let resp = tls
        .reqwest_client()?
        .post(&url)
        .header(crate::raft::RPC_TOKEN_HEADER, &raft.cluster_token)
        // 服务端 leader 转发上限 30s（快照 10s + 成员写），CLI 必须覆盖该窗口。
        .timeout(Duration::from_secs(40))
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("连接本机 Raft HTTP（{}）失败: {e}", raft.advertise_endpoint))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    println!("{text}");
    Ok(if status.is_success() { 0 } else { 1 })
}

fn sqlite_url(path: &std::path::Path) -> String {
    format!("sqlite://{}?mode=rw", path.display())
}

fn env_has_vault() -> bool {
    ["OSS_MASTER_KEY_FILE", "OSS_KMS_DIR"].iter().any(|k| {
        std::env::var(k)
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
    })
}

async fn export_cmd(
    cfg_path: &std::path::Path,
    out: Option<std::path::PathBuf>,
    witness: bool,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let (db, source_site, node_id, role, admin, has_vault) = if witness {
        let w = crate::config::WitnessToml::load(cfg_path)?;
        let state_db = w.raft.state_db.as_ref().ok_or("witness 缺少 state_db")?;
        let db = crate::db::Db::connect_offline(&sqlite_url(state_db)).await?;
        (
            db,
            "witness".to_string(),
            Some(w.raft.node_id),
            "witness".to_string(),
            None,
            None,
        )
    } else {
        let cfg = crate::config::AppConfig::load(cfg_path)?;
        let raft = cfg.raft.as_ref().ok_or("[raft] 未配置")?;
        let db = crate::db::Db::connect_offline(&cfg.database_url).await?;
        let site = db
            .schema_meta("raft_site_id")
            .await?
            .unwrap_or_else(|| "site".into());
        let admin = db.schema_meta("admin_username").await?;
        (
            db,
            site,
            Some(raft.node_id),
            "voter".to_string(),
            admin,
            Some(env_has_vault()),
        )
    };
    let export = crate::raft::cutover::build_export(
        &db,
        source_site.clone(),
        node_id,
        &role,
        admin,
        has_vault,
    )
    .await?;
    match out.as_deref() {
        Some(p) if p != std::path::Path::new("-") => {
            crate::raft::cutover::write_export(p, &export)?;
            println!("导出完成: {}", p.display());
        }
        _ => println!("{}", serde_json::to_string_pretty(&export)?),
    }
    Ok(0)
}

async fn cutover_cmd(
    cfg_path: &std::path::Path,
    from: &std::path::Path,
    peer_config: Option<&std::path::Path>,
    dry_run: bool,
    overwrite: bool,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let export = crate::raft::cutover::read_export(from)?;
    let mut paths = vec![cfg_path.to_path_buf()];
    if let Some(p) = peer_config {
        paths.push(p.to_path_buf());
    }
    let mut targets = Vec::new();
    for p in &paths {
        let cfg = crate::config::AppConfig::load(p)?;
        let raft = cfg.raft.as_ref().ok_or("[raft] 未配置")?;
        let db = crate::db::Db::connect_offline(&cfg.database_url).await?;
        let facts = crate::raft::cutover::TargetFacts {
            version: env!("CARGO_PKG_VERSION").to_string(),
            has_vault: env_has_vault(),
            admin_username: db.schema_meta("admin_username").await?,
            has_cluster: cfg.cluster.is_some(),
        };
        let _ = raft;
        targets.push((p.clone(), db, facts));
    }
    // 先 dry-run 全部目标，再写；避免第二个目标失败时第一个已切模式。
    for (_, db, facts) in &targets {
        let report = match crate::raft::cutover::cutover(
            db,
            facts,
            &export,
            &crate::raft::cutover::CutoverOptions {
                dry_run: true,
                overwrite,
            },
        )
        .await
        {
            Ok(report) => report,
            Err(crate::raft::cutover::CutoverError::Conflict(diffs)) => {
                // 非 --overwrite 的冲突在返回 Error 前也打印字段级明细，便于审计定位。
                for d in &diffs {
                    eprintln!("conflict {} {} {}", d.table, d.key, d.field);
                }
                return Err(crate::raft::cutover::CutoverError::Conflict(diffs).into());
            }
            Err(e) => return Err(e.into()),
        };
        if !report.conflicts.is_empty() {
            for d in &report.conflicts {
                eprintln!("conflict {} {} {}", d.table, d.key, d.field);
            }
        }
    }
    if dry_run {
        println!("dry-run: 校验通过（{} 个目标）", targets.len());
        return Ok(0);
    }
    for (path, db, facts) in &targets {
        let report = crate::raft::cutover::cutover(
            db,
            facts,
            &export,
            &crate::raft::cutover::CutoverOptions {
                dry_run: false,
                overwrite,
            },
        )
        .await?;
        println!(
            "cutover 完成: {} ({} 张表)",
            path.display(),
            report.rows.len()
        );
    }
    println!("下一步：A 以 bootstrap=true、B 以 false 启动，配对后 raft-add-witness C");
    Ok(0)
}

async fn force_recover_cmd(
    cfg_path: &std::path::Path,
    from: &std::path::Path,
    new_node_id: u64,
    yes: bool,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    if !yes {
        return Err("raft-force-recover 需要 --yes 确认".into());
    }
    let export = crate::raft::cutover::read_export(from)?;
    // 数据站点配置优先；否则按 witness 配置（仅 [raft]）打开 state_db。
    let (db, settings, raft_db) = match crate::config::AppConfig::load(cfg_path) {
        Ok(cfg) => {
            let raft = cfg.raft.as_ref().ok_or("[raft] 未配置")?;
            let settings = crate::raft::RaftSettings::from_toml(raft)?;
            let raft_db = settings.db_path.clone();
            let db = crate::db::Db::connect_raft(&cfg.database_url).await?;
            (db, settings, raft_db)
        }
        Err(_) => {
            let w = crate::config::WitnessToml::load(cfg_path)?;
            let state_db = w.raft.state_db.as_ref().ok_or("witness 缺少 state_db")?;
            let settings = crate::raft::RaftSettings::from_toml(&w.raft)?;
            let raft_db = settings.db_path.clone();
            for p in [state_db.as_path(), raft_db.as_path()] {
                if let Some(dir) = p.parent().filter(|d| !d.as_os_str().is_empty()) {
                    std::fs::create_dir_all(dir)?;
                }
            }
            let url = format!("sqlite://{}?mode=rwc", state_db.display());
            let db = crate::db::Db::connect_witness(&url).await?;
            (db, settings, raft_db)
        }
    };
    let report =
        crate::raft::cutover::force_recover(&db, &raft_db, &settings, &export, new_node_id).await?;
    println!(
        "force-recover 完成: node={} voters={:?} raft_db={}",
        report.node_id,
        report.voters,
        report.raft_db.display()
    );
    println!("以 bootstrap=false 启动本节点；恢复其他站点后配对；对象数据从备份恢复");
    Ok(0)
}

async fn sync_reconcile_cmd(
    cfg_path: &std::path::Path,
    repair: bool,
    bucket: Option<String>,
    json: bool,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    // 离线：绝不 AppState::load（会二次打开 raft.db 并启动运行时/启动对账）。
    let cfg = crate::config::AppConfig::load(cfg_path)?;
    let db = crate::db::Db::connect_offline(&cfg.database_url).await?;
    let drives = cfg
        .data_dirs
        .clone()
        .unwrap_or_else(|| vec![cfg.data_dir.clone()]);
    let mut sc = xl_store::store::StoreConfig::new(&drives[0]);
    sc.drives = drives;
    sc.ec = xl_store::store::ec::EcConfig {
        data: cfg.ec.data,
        parity: cfg.ec.parity,
        enabled: cfg.ec.enabled,
    };
    sc.inline_threshold = cfg.ec.inline_threshold;
    sc.sync_max_bytes = cfg.sync.max_object_bytes;
    // sync-reconcile 是运维修数据路径：不注入写门（只读对象 + 定点映射写）。
    sc.gate = None;
    let store = xl_store::store::Store::open(sc).await?;
    let master_file = std::env::var("OSS_MASTER_KEY_FILE")
        .ok()
        .filter(|s| !s.is_empty());
    let kms_dir = std::env::var("OSS_KMS_DIR").ok().filter(|s| !s.is_empty());
    if master_file.is_some() || kms_dir.is_some() {
        store
            .init_encryption(
                master_file.map(std::path::PathBuf::from),
                kms_dir.map(std::path::PathBuf::from),
            )
            .await?;
    }
    let vault = xl_s3::skvault::SecretVault::from_enc_state(store.enc_state()).map(Arc::new);
    let tls = crate::tls::Tls::load(&cfg.tls)?;
    let report = crate::raft::repair::run_tls(
        &db,
        &store,
        vault.as_deref(),
        cfg.sync.max_object_bytes,
        &crate::raft::repair::RepairOptions { bucket, repair },
        Some(tls),
    )
    .await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "buckets={} versions={} already_ok={} mappings_written={} pushed={} markers_pushed={} markers_missing={} intents_cleared={} outbox_cleared={}",
            report.buckets,
            report.versions,
            report.already_ok,
            report.mappings_written,
            report.pushed,
            report.markers_pushed,
            report.markers_missing,
            report.intents_cleared,
            report.outbox_cleared
        );
        for e in &report.errors {
            eprintln!("{} {}@{}: {}", e.bucket, e.key, e.version_id, e.message);
        }
    }
    Ok(if report.has_errors() { 1 } else { 0 })
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
    fn parse_args_reads_offline_commands() {
        assert_eq!(parse_args(&args(&["witness"])), Ok(Command::Witness));
        assert_eq!(
            parse_args(&args(&[
                "raft-add-witness",
                "--node-id",
                "3",
                "--endpoint",
                "http://127.0.0.1:9000",
            ])),
            Ok(Command::RaftAddWitness {
                node_id: 3,
                endpoint: "http://127.0.0.1:9000".into(),
                replace: None,
            })
        );
        assert_eq!(
            parse_args(&args(&[
                "raft-add-witness",
                "--node-id=3",
                "--endpoint=http://w:9",
                "--replace=2",
            ])),
            Ok(Command::RaftAddWitness {
                node_id: 3,
                endpoint: "http://w:9".into(),
                replace: Some(2),
            })
        );
        assert_eq!(
            parse_args(&args(&["raft-remove-node", "--node-id", "2"])),
            Ok(Command::RaftRemoveNode { node_id: 2 })
        );
        assert_eq!(
            parse_args(&args(&["raft-export", "--witness", "--out", "/x.json"])),
            Ok(Command::RaftExport {
                out: Some("/x.json".into()),
                witness: true,
            })
        );
        assert_eq!(
            parse_args(&args(&[
                "raft-cutover",
                "--from",
                "/e.json",
                "--peer-config=/b.toml",
                "--dry-run",
            ])),
            Ok(Command::RaftCutover {
                from: "/e.json".into(),
                peer_config: Some("/b.toml".into()),
                dry_run: true,
                overwrite: false,
            })
        );
        assert_eq!(
            parse_args(&args(&[
                "raft-force-recover",
                "--from",
                "/e.json",
                "--node-id",
                "1",
                "--yes",
            ])),
            Ok(Command::RaftForceRecover {
                from: "/e.json".into(),
                new_node_id: 1,
                yes: true,
            })
        );
        assert_eq!(
            parse_args(&args(&["sync-reconcile", "--repair", "--bucket", "b"])),
            Ok(Command::SyncReconcile {
                repair: true,
                bucket: Some("b".into()),
                json: false,
            })
        );
    }

    #[test]
    fn split_config_extracts_exactly_one() {
        use std::path::PathBuf;
        let (p, rest) = split_config(&args(&["witness"])).unwrap();
        assert_eq!(p, PathBuf::from(CONFIG_PATH));
        assert_eq!(rest, args(&["witness"]));
        let (p, rest) = split_config(&args(&["--config", "/x.toml", "witness"])).unwrap();
        assert_eq!(p, PathBuf::from("/x.toml"));
        assert_eq!(rest, args(&["witness"]));
        let (p, rest) = split_config(&args(&["raft-export", "--config=/y.toml"])).unwrap();
        assert_eq!(p, PathBuf::from("/y.toml"));
        assert_eq!(rest, args(&["raft-export"]));
        for bad in [
            &["--config"][..],
            &["--config="][..],
            &["--config", "a", "--config", "b"][..],
            &["--config=a", "--config=b"][..],
        ] {
            assert!(split_config(&args(bad)).is_err(), "应拒绝: {bad:?}");
        }
    }

    #[test]
    fn needs_full_state_classifies_offline_commands() {
        for cmd in [
            Command::Serve,
            Command::Groups,
            Command::Build,
            Command::Clean { days: 1 },
            Command::Fsck { repair: false },
            Command::Heal,
            Command::Scrub,
            Command::Rebalance { dry_run: true },
        ] {
            assert!(needs_full_state(&cmd), "{cmd:?}");
        }
        for cmd in [
            Command::Witness,
            Command::RaftAddWitness {
                node_id: 1,
                endpoint: "http://w:1".into(),
                replace: None,
            },
            Command::RaftRemoveNode { node_id: 1 },
            Command::RaftExport {
                out: None,
                witness: false,
            },
            Command::RaftCutover {
                from: "e".into(),
                peer_config: None,
                dry_run: true,
                overwrite: false,
            },
            Command::RaftForceRecover {
                from: "e".into(),
                new_node_id: 1,
                yes: true,
            },
            Command::SyncReconcile {
                repair: false,
                bucket: None,
                json: false,
            },
        ] {
            assert!(!needs_full_state(&cmd), "{cmd:?}");
        }
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
            &["witness", "x"][..],
            &["raft-add-witness", "--node-id", "3"][..], // 缺 endpoint
            &[
                "raft-add-witness",
                "--node-id",
                "x",
                "--endpoint",
                "http://w:1",
            ][..],
            &[
                "raft-add-witness",
                "--node-id",
                "3",
                "--endpoint",
                "https://w:1",
            ][..],
            &[
                "raft-add-witness",
                "--node-id",
                "3",
                "--endpoint",
                "http://w:1",
                "--go",
            ][..],
            &[
                "raft-add-witness",
                "--node-id",
                "3",
                "--node-id",
                "4",
                "--endpoint",
                "http://w:1",
            ][..],
            &["raft-cutover", "--dry-run"][..],  // 缺 --from
            &["sync-reconcile", "--bucket"][..], // 缺值
        ] {
            assert_eq!(parse_args(&args(bad)), Err(USAGE), "应拒绝: {bad:?}");
        }
        // 上限之外的天数也拒绝（挡住按天换算时的乘法溢出）
        assert_eq!(parse_args(&args(&["clean", "36526"])), Err(USAGE));
    }
}
