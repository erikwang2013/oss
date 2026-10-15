//! 请求间共享的应用状态。

use std::path::PathBuf;
use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::Db;
use jwt_rust::config::JwtConfig;
use jwt_rust::integrations::JwtAuth;
use xl_store::store::{Store, StoreConfig};

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<AppConfig>,
    pub db: Arc<Db>,
    pub store: Arc<Store>,
    pub ingest: Arc<crate::aether::IngestSink>,
    /// 控制面（`/api/v1`）与上传口（`/aetherupload`）共用的 JWT 门面：
    /// 登录用它签发，路由层用同一实例的 `JwtLayer` 校验（跨面 token 因此天然互通）。
    pub jwt: Arc<JwtAuth>,
}

impl AppState {
    /// 启动自举：读配置、建目录、连库、开存储、引导管理员与桶。
    /// serve 与运维命令（`cli::run`）共用这一条序列。
    pub async fn load(
        cfg_path: &std::path::Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut cfg = AppConfig::load(cfg_path)?;
        // 密钥兜底解析（docker/无人值守）：空值 → OSS_SECRETS_FILE 复用/生成落盘（见 config.rs）
        let secrets = cfg.resolve_secrets()?;

        // 盘数组：data_dirs 优先（多盘），否则 data_dir 单盘兼容
        let drives: Vec<PathBuf> = cfg
            .data_dirs
            .clone()
            .unwrap_or_else(|| vec![cfg.data_dir.clone()]);
        // 信任边界（评审 R4）：空数组/重复路径在此收口——空数组后续会越界、重复路径会让
        // quorum 按条目假达成（12 条指同一物理盘 = 零冗余且无告警）
        if drives.is_empty() {
            return Err("data_dirs 不能为空（不配 data_dirs 即用 data_dir 单盘）".into());
        }
        let mut seen = std::collections::HashSet::new();
        for d in &drives {
            let canon = d.canonicalize().unwrap_or_else(|_| d.clone());
            if !seen.insert(canon.clone()) {
                return Err(format!("data_dirs 含重复路径: {}", canon.display()).into());
            }
        }
        for d in &drives {
            std::fs::create_dir_all(d)
                .map_err(|e| format!("创建数据目录 {} 失败: {e}", d.display()))?;
        }
        std::fs::create_dir_all(&cfg.staging_dir)
            .map_err(|e| format!("创建暂存目录 {} 失败: {e}", cfg.staging_dir.display()))?;

        let db = Db::connect(&cfg.database_url).await?;
        let mut store_cfg = StoreConfig::new(&drives[0]);
        store_cfg.drives = drives;
        store_cfg.ec = xl_store::store::ec::EcConfig {
            data: cfg.ec.data,
            parity: cfg.ec.parity,
            enabled: cfg.ec.enabled,
        };
        store_cfg.inline_threshold = cfg.ec.inline_threshold;
        // M3 集群（配了 [cluster] 才走）：静态拓扑 + 节点间传输注入存储内核。
        // 单节点（None）行为与 v1.4.0 逐字节一致；拓扑与盘数的硬校验在
        // ClusterTopology::new 与 Store::open（self 一致性）。
        if let Some(cl) = cfg.cluster.as_ref() {
            let sets_nodes = sets_nodes_by_set(&cl.nodes)?;
            let sets = xl_store::store::ClusterSets::new(&cl.node_id, sets_nodes, &store_cfg.ec)?;
            // 路由表用 flat 去重节点集（同 id 多 set 各出一条时不得重复注册）
            let pairs: Vec<(String, String)> = sets
                .flat
                .nodes
                .iter()
                .map(|n| (n.id.clone(), n.addr.clone()))
                .collect();
            let mesh = Arc::new(xl_client::TonicMesh::new(&cl.node_id, &cl.secret, &pairs)?);
            // 放置开关：只认 None/"v1"/"v2"——未知值 fail-closed（静默降级为 v1 会让
            // 「以为开了 v2」的运维在 rebalance 里得到空跑报告）
            let placement_v2 = match cl.placement.as_deref() {
                None | Some("v1") => false,
                Some("v2") => true,
                Some(other) => {
                    return Err(format!("cluster.placement 非法值 «{other}»（只认 v1/v2）").into());
                }
            };
            // M4 切片 8：多 set 注入 SetResolver（单 set 由 Store 侧短路——零新增查询）；
            // 扇出面 = 除本机外的去重节点并集（verifier 口径 b）
            let resolver: Option<Arc<dyn xl_store::mesh::SetResolver>> = (sets.sets.len() > 1)
                .then(|| {
                    let peers: Vec<String> = sets
                        .flat
                        .nodes
                        .iter()
                        .filter(|n| n.id != cl.node_id)
                        .map(|n| n.id.clone())
                        .collect();
                    let transport: Arc<dyn xl_store::mesh::MeshTransport> = mesh.clone();
                    Arc::new(crate::cluster_control::DbSetResolver::new(
                        db.pool().clone(),
                        peers,
                        transport,
                    )) as Arc<dyn xl_store::mesh::SetResolver>
                });
            store_cfg.cluster = Some(Arc::new(xl_store::store::ClusterConfig {
                sets,
                transport: mesh,
                placement_v2,
                resolver,
            }));
            // Task 9：控制面读扇出的本节点执行器（判权复用 xl-s3::Acl；只查本地 DB）
            store_cfg.control = Some(Arc::new(crate::cluster_control::SqliteControlBackend::new(
                db.pool().clone(),
            )));
        }
        let store = Store::open(store_cfg).await?;

        // 历史桶回填（权限体系）：盘上有、buckets 表里没有的桶补一行（owner NULL ⇒
        // 仅管理员可见/可管）。INSERT OR IGNORE 幂等，已存在（含已有 owner）不覆盖。
        match store.list_buckets().await {
            Ok(buckets) => {
                for b in buckets {
                    // M4 切片 8：INSERT OR IGNORE 不覆盖已有行（新桶在创建时已带 owner/set）；
                    // 无行者先扇出问 set：有答按答插；「全部可达且全无」→ set0+warn（存量语义）；
                    // **任一传输错 → 不插行**（下次启动重试；「不可达」不得写死 set0——
                    // verifier 终审洞 2：升级后 set=1 桶的行在不可达窗口被写死 0 即分片错组）
                    match store.resolve_bucket_set(&b.name).await {
                        // P3-3：权威 set **对账**（不止补缺）——陈旧行（删桶重建后别节点残留）在此收敛
                        Ok(Some(s)) => {
                            if let Err(e) = db.upsert_bucket_set(&b.name, s).await {
                                tracing::warn!(error = %e, bucket = %b.name, "回填对账失败");
                            }
                        }
                        Ok(None) => {
                            // 全可达且确无行：仅缺行时插 set=0（**不覆盖**已有行——None 无权威值）
                            tracing::warn!(bucket = %b.name, "回填：全集群确无该桶行，按 set=0（存量语义）");
                            if let Err(e) = db.insert_bucket(&b.name, None).await {
                                tracing::warn!(error = %e, bucket = %b.name, "历史桶回填失败");
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, bucket = %b.name, "回填：set 解析不可达，本轮跳过（下次启动重试）");
                        }
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "启动回填：列举桶失败"),
        }

        // M1 还没有注册界面：用环境变量引导出第一个管理员（幂等，已存在则跳过）。
        // 口令来源：OSS_ADMIN_PASSWORD 非空优先；空/缺时用密钥文件解析值（配了
        // OSS_SECRETS_FILE 才可能有——docker 栈即此路径，口令随文件落盘）。
        // 空凭据一律拒绝建号：空口令账号会被空口令登录命中。
        match (
            std::env::var("OSS_ADMIN_USER")
                .ok()
                .filter(|u| !u.is_empty()),
            std::env::var("OSS_ADMIN_PASSWORD")
                .ok()
                .filter(|p| !p.is_empty())
                .or_else(|| secrets.admin_password.clone()),
        ) {
            (Some(user), Some(pass)) => {
                // 已存在的同名账号一律不覆盖（含口令不同的情况）
                if !db.user_exists(&user).await? {
                    db.create_user(&user, &pass, true).await?;
                    tracing::info!(
                        user = %user,
                        secrets_file = ?secrets.secrets_file,
                        "创建管理员账号（口令取自环境变量或密钥文件）"
                    );
                }
            }
            (Some(_), None) => {
                tracing::warn!(
                    "OSS_ADMIN_USER 已设但无可用口令（环境与密钥文件都没有），跳过管理员引导"
                );
            }
            _ => {}
        }

        // 每个上传分组对应的桶，启动时确保存在（内核 + 控制面各一份记录）。
        // 集群：滚动启动时对端可能尚未就绪——**本地确保 + 广播尽力**（严格 fail-closed
        // 留给运行时的用户请求；缺口由写路径懒自愈与 fsck 差集补齐兜底）。
        // 实弹教训：docker 集群栈里 n1 先于对端启动，严格扇出会让启动即退。
        for g in &cfg.groups {
            if cfg.cluster.is_some() {
                store.mesh_ensure_bucket(&g.bucket).await?;
                if let Some(cl) = store.cluster() {
                    let self_id = cl.sets.flat.self_node().id.clone();
                    for n in &cl.sets.flat.nodes {
                        if n.id == self_id {
                            continue;
                        }
                        if let Err(e) = cl.transport.ensure_bucket(&n.id, &g.bucket).await {
                            tracing::warn!(node = %n.id, bucket = %g.bucket, error = %e, "启动建桶广播未达（滚动启动正常；懒自愈/fsck 兜底）");
                        }
                    }
                }
            } else if !store.bucket_exists(&g.bucket).await? {
                store.create_bucket(&g.bucket).await?;
            }
            // M4 切片 8：groups 桶钉 set 0（多 set 下不参与新组——目录/行按 set0 口径收口，C3）
            db.insert_bucket(&g.bucket, None).await?;
        }

        Ok(Self::new(cfg, db, store))
    }

    /// 集群控制面扇出面（Task 9；None = 单节点——三面 Acl/SigV4Auth 共用此构造）。
    pub fn control_mesh(&self) -> Option<xl_s3::access::ControlMesh> {
        self.store.cluster().map(|c| {
            let self_id = c.sets.flat.self_node().id.clone();
            xl_s3::access::ControlMesh {
                transport: c.transport.clone(),
                peers: c
                    .sets
                    .flat
                    .nodes
                    .iter()
                    .filter(|n| n.id != self_id)
                    .map(|n| n.id.clone())
                    .collect(),
                self_id,
            }
        })
    }

    /// 三面共用的判权实例（Task 9：集群时自动带读扇出；SQL 唯一事实源在 xl-s3::Acl）。
    pub fn acl(&self) -> xl_s3::access::Acl {
        xl_s3::access::Acl::with_mesh(self.db.pool().clone(), self.control_mesh())
    }

    pub fn new(cfg: AppConfig, db: Db, store: Store) -> Self {
        // except 只有 `/login`：console 路由经 nest 时 axum 的 StripPrefix 会先把
        // `/api/v1` 前缀剥掉，JwtLayer 看到的是 `/login`；aether 面路径不含 `/login` 段
        // （`/aetherupload/display/login` 之类整段不匹配），照常校验。
        // secret 短于 32 字节（jwt-rust 的 256 位下限）、黑名单目录不可建，都启动即失败。
        let jwt = Arc::new(
            JwtAuth::from_config(JwtConfig {
                secret_key: cfg.jwt_secret.clone(),
                default_expire: 12 * 3600,
                // 黑名单（M1 无 logout 调用；未来撤销用）落数据盘，不用默认 /tmp：
                // /tmp 会被 tmpfiles 清理（静默丢撤销记录）、多实例不共享，且预建同名目录
                // 的 0700 只在本进程创建时设。FileTokenStorage 会递归建目录。
                // 挂 `.oss.sys/` 下：store 的 list_buckets 跳过 SYS_DIR，直接放 data_dir 根
                // 会被当成一个桶列出来（撞过测试 `login_then_list_buckets`）。
                storage: jwt_rust::config::StorageConfig {
                    path: Some(
                        cfg.data_dir
                            .join(xl_store::layout::SYS_DIR)
                            .join("jwt_blacklist")
                            .to_string_lossy()
                            .into_owned(),
                    ),
                    ..Default::default()
                },
                middleware: jwt_rust::config::MiddlewareConfig {
                    except: vec!["/login".into()],
                },
                ..Default::default()
            })
            .unwrap_or_else(|e| panic!("jwt-rust 初始化失败: {e}")),
        );
        let cfg = Arc::new(cfg);
        let db = Arc::new(db);
        let ingest = Arc::new(crate::aether::IngestSink {
            store: Arc::new(store),
            db: Arc::clone(&db),
            cfg: Arc::clone(&cfg),
        });
        Self {
            cfg,
            db,
            store: Arc::clone(&ingest.store),
            ingest,
            jwt,
        }
    }
}

/// M4 切片 8：节点按 set 分组（`set` 缺省 0；set 号须从 0 连续——缺号会让拓扑数组下标错位）。
/// 独立成函数以便单测（配置是信任边界，fail-closed）。
fn sets_nodes_by_set(
    nodes: &[crate::config::NodeToml],
) -> Result<Vec<Vec<xl_store::store::NodeSpec>>, String> {
    let mut by_set: std::collections::BTreeMap<u16, Vec<xl_store::store::NodeSpec>> =
        std::collections::BTreeMap::new();
    for n in nodes {
        by_set
            .entry(n.set.unwrap_or(0))
            .or_default()
            .push(xl_store::store::NodeSpec {
                id: n.id.clone(),
                addr: n.addr.clone(),
                drive_count: n.drive_count,
            });
    }
    let k = by_set.len() as u16;
    if by_set.keys().copied().ne(0..k) {
        return Err(format!(
            "cluster.nodes 的 set 号必须从 0 连续（现有 {:?}）",
            by_set.keys().collect::<Vec<_>>()
        ));
    }
    Ok(by_set.into_values().collect())
}

#[cfg(test)]
mod set_group_tests {
    use super::*;

    fn node(id: &str, set: Option<u16>) -> crate::config::NodeToml {
        crate::config::NodeToml {
            id: id.into(),
            addr: "127.0.0.1:1".into(),
            drive_count: 4,
            set,
        }
    }

    #[test]
    fn groups_by_set_with_default_zero() {
        let groups = sets_nodes_by_set(&[
            node("n1", None),
            node("n2", None),
            node("n1", Some(1)),
            node("n2", Some(1)),
        ])
        .unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 2);
        assert_eq!(groups[0][0].id, "n1");
        assert_eq!(groups[1][0].id, "n1", "同 id 跨 set 各一条");
    }

    #[test]
    fn non_contiguous_sets_rejected() {
        // 只有 set=1：缺 set0 → fail-closed（拓扑数组下标会错位）
        let err = sets_nodes_by_set(&[node("n1", Some(1))]).unwrap_err();
        assert!(err.contains("连续"), "{err}");
        let err = sets_nodes_by_set(&[node("n1", None), node("n2", Some(2))]).unwrap_err();
        assert!(err.contains("连续"), "{err}");
    }
}
