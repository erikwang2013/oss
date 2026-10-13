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
        let cfg = AppConfig::load(cfg_path)?;

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
            let self_idx = cl
                .nodes
                .iter()
                .position(|n| n.id == cl.node_id)
                .ok_or_else(|| {
                    format!(
                        "cluster.node_id «{}» 不在 [[cluster.nodes]] 表内",
                        cl.node_id
                    )
                })?;
            let specs: Vec<xl_store::store::NodeSpec> = cl
                .nodes
                .iter()
                .map(|n| xl_store::store::NodeSpec {
                    id: n.id.clone(),
                    addr: n.addr.clone(),
                    drive_count: n.drive_count,
                })
                .collect();
            let topo = xl_store::store::ClusterTopology::new(self_idx, specs, &store_cfg.ec)?;
            let pairs: Vec<(String, String)> = cl
                .nodes
                .iter()
                .map(|n| (n.id.clone(), n.addr.clone()))
                .collect();
            let mesh = Arc::new(xl_client::TonicMesh::new(&cl.node_id, &cl.secret, &pairs)?);
            store_cfg.cluster = Some(Arc::new(xl_store::store::ClusterConfig {
                topo,
                transport: mesh,
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
                    if let Err(e) = db.insert_bucket(&b.name, None).await {
                        tracing::warn!(error = %e, bucket = %b.name, "历史桶回填失败");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "启动回填：列举桶失败"),
        }

        // M1 还没有注册界面：用环境变量引导出第一个管理员（幂等，已存在则跳过）。
        // OSS_ADMIN_USER / OSS_ADMIN_PASSWORD 未设置时不做任何事。
        // 空凭据一律拒绝建号：空口令账号会被空口令登录命中（M1 的 verify_user 只做哈希比对，无口令强度校验）。
        match (
            std::env::var("OSS_ADMIN_USER"),
            std::env::var("OSS_ADMIN_PASSWORD"),
        ) {
            (Ok(user), Ok(pass)) if !user.is_empty() && !pass.is_empty() => {
                // 已存在的同名账号一律不覆盖（含口令不同的情况）
                if !db.user_exists(&user).await? {
                    db.create_user(&user, &pass, true).await?;
                    tracing::info!(user = %user, "创建管理员账号");
                }
            }
            (Ok(user), Ok(pass)) => {
                tracing::warn!(
                    user_empty = user.is_empty(),
                    pass_empty = pass.is_empty(),
                    "管理员凭据为空字符串，跳过引导（空口令账号会被空登录命中）"
                );
            }
            (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
                tracing::warn!("OSS_ADMIN_USER / OSS_ADMIN_PASSWORD 只设置了一个，跳过管理员引导");
            }
            (Err(_), Err(_)) => {}
        }

        // 每个上传分组对应的桶，启动时确保存在（内核 + 控制面各一份记录）。
        // 集群：滚动启动时对端可能尚未就绪——**本地确保 + 广播尽力**（严格 fail-closed
        // 留给运行时的用户请求；缺口由写路径懒自愈与 fsck 差集补齐兜底）。
        // 实弹教训：docker 集群栈里 n1 先于对端启动，严格扇出会让启动即退。
        for g in &cfg.groups {
            if cfg.cluster.is_some() {
                store.mesh_ensure_bucket(&g.bucket).await?;
                if let Some(cl) = store.cluster() {
                    let self_id = cl.topo.self_node().id.clone();
                    for n in &cl.topo.nodes {
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
            db.insert_bucket(&g.bucket, None).await?;
        }

        Ok(Self::new(cfg, db, store))
    }

    /// 集群控制面扇出面（Task 9；None = 单节点——三面 Acl/SigV4Auth 共用此构造）。
    pub fn control_mesh(&self) -> Option<xl_s3::access::ControlMesh> {
        self.store.cluster().map(|c| {
            let self_id = c.topo.self_node().id.clone();
            xl_s3::access::ControlMesh {
                transport: c.transport.clone(),
                peers: c
                    .topo
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
