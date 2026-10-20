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
    /// 登录限速（C1）：账号失败锁定 + 全局失败上限（Argon2 ~19MiB/次 的资源放大面封口）。
    pub login_limiter: Arc<LoginLimiter>,
    /// M5 切片 6：SK 加密库（`OSS_MASTER_KEY_FILE`/`OSS_KMS_DIR` 未配 = None = 明文直存）。
    pub vault: Option<Arc<xl_s3::skvault::SecretVault>>,
    /// M6 切片 0：事件总线接收方 + worker（event_outbox）。
    pub events: Arc<crate::events::SqlEventSink>,
    /// M6 切片 4：复制写入钩子（repl_outbox 入队）
    pub repl_sink: Arc<crate::replication::SqlReplSink>,
    /// M6 切片 4：复制规则存储（Put/Get/DeleteBucketReplication）
    pub repl_store: Arc<crate::replication::SqlReplStore>,
    /// M6 切片 4：复制出站客户端（worker 用）
    pub repl_client: Arc<crate::repl_client::ReplClient>,
    /// M7：本站身份（`site_config` 单行，启动时读取；token 不下发给未授权面）
    pub site: Arc<crate::db::SiteIdentity>,
    /// M7：`[site]` 是否配置（唯一开关；无 `[site]` 不挂站点路由语义、不可配对）
    pub site_enabled: bool,
    /// M8 S1：访问点缓存（CRUD/桶删除负责失效；路由读路径 fail-closed）
    pub ap_cache: Arc<xl_s3::ap::ApCache>,
    /// M8 S2：变换器缓存（AP 名 → 编译产物；控制面 PUT/删除/桶删除失效）
    pub olap_cache: Arc<xl_s3::olap::OlapCache>,
    /// M8 S1：AP ARN 的 region（取 `[site].region`，缺省 us-east-1）
    pub ap_region: String,
    /// M8 S0/B10：所有删桶路径共用的 M8 清理（Db 清表 + AP 缓存失效）。
    pub bucket_cleanup: Arc<dyn xl_s3::bucket_cleanup::BucketCleanupSink>,
    /// M8 S4b：生命周期规则存储（S3 协议面 + console 共用）。
    pub lifecycle: Arc<dyn xl_s3::lifecycle::LifecycleStore>,
    /// M8 S4b：远端 tier 后端（Transition 归档 / Restore 取回 / GC）。
    pub tiers: Arc<crate::tier::TierBackends>,
    /// M8 S4b：RestoreObject 入队。
    pub restore_sink: Arc<dyn xl_s3::restore::RestoreSink>,
    /// M8 S4b：删除对象时回收远端归档副本。
    pub tier_gc: Arc<dyn xl_s3::tier_gc::TierGcSink>,
    /// M8 S5：Inventory 配置存储（S3 与 console 共用）。
    pub inventory: Arc<dyn xl_s3::inventory::InventoryStore>,
    /// M8 S5：Inventory 生成执行器（作业引擎的 `Inventory` 动作）。
    pub inventory_runner: Arc<crate::inventory::InventoryRunner>,
}

fn mesh_of(store: &Store) -> Option<xl_s3::access::ControlMesh> {
    store.cluster().map(|c| {
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

/// M8 S0/S1：删桶清理的跨 crate 实现（Db 清表 + AP 缓存失效）。
pub struct SqlBucketCleanup {
    db: Arc<Db>,
    ap_cache: Arc<xl_s3::ap::ApCache>,
    olap_cache: Arc<xl_s3::olap::OlapCache>,
}

impl SqlBucketCleanup {
    pub fn new(
        db: Arc<Db>,
        ap_cache: Arc<xl_s3::ap::ApCache>,
        olap_cache: Arc<xl_s3::olap::OlapCache>,
    ) -> Self {
        Self {
            db,
            ap_cache,
            olap_cache,
        }
    }
}

#[async_trait::async_trait]
impl xl_s3::bucket_cleanup::BucketCleanupSink for SqlBucketCleanup {
    async fn clear_bucket_m8(&self, bucket: &str) -> Result<(), String> {
        self.db
            .clear_bucket_m8(bucket)
            .await
            .map_err(|e| e.to_string())
    }

    fn invalidate_ap_cache(&self, bucket: &str) {
        self.ap_cache.invalidate_bucket(bucket);
        self.olap_cache.invalidate_bucket(bucket);
    }
}

/// 登录限速（C1）。**单进程内存态**（每节点独立，重启清零——限速非持久配额，重启放宽可接受）：
/// - 账号维度：10 分钟窗口内失败 ≥5 → 锁 10 分钟（429 + Retry-After）——防定向爆破；
/// - 全局维度：60 秒窗口内失败 ≥120（跨用户名）→ 全体暂拒——防「随机用户名洪水」刷 Argon2
///   （每次失败无论用户名是否存在都付一次 dummy_verify 的同价成本）。
///
/// 防护面：登录只；已认证的 `/me/password` 不在此列（攻击者已持凭据，改密需旧密校验）。
pub struct LoginLimiter {
    inner: std::sync::Mutex<LimiterState>,
}

struct LimiterState {
    per_user: std::collections::HashMap<String, UserWindow>,
    /// (窗口内失败数, 窗口起点秒)
    global: (u32, i64),
}

#[derive(Clone, Copy)]
struct UserWindow {
    fails: u32,
    window_start: i64,
    locked_until: i64,
}

const USER_FAILS_MAX: u32 = 5;
const USER_WINDOW_SECS: i64 = 600;
const USER_LOCK_SECS: i64 = 600;
const GLOBAL_FAILS_MAX: u32 = 120;
const GLOBAL_WINDOW_SECS: i64 = 60;

fn limiter_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl LoginLimiter {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(LimiterState {
                per_user: std::collections::HashMap::new(),
                global: (0, 0),
            }),
        }
    }

    /// 尝试前检查。`Err(秒)` = 被拒（Retry-After）。
    pub fn check(&self, username: &str) -> Result<(), u64> {
        let now = limiter_now();
        let st = self.inner.lock().unwrap();
        if let Some(w) = st.per_user.get(username)
            && w.locked_until > now
        {
            return Err((w.locked_until - now) as u64);
        }
        let (gf, gw) = st.global;
        if now - gw < GLOBAL_WINDOW_SECS && gf >= GLOBAL_FAILS_MAX {
            return Err((GLOBAL_WINDOW_SECS - (now - gw)).max(1) as u64);
        }
        Ok(())
    }

    /// 失败记账（凭据错误路径）。
    pub fn on_failure(&self, username: &str) {
        let now = limiter_now();
        let mut st = self.inner.lock().unwrap();
        // 全局窗口
        if now - st.global.1 >= GLOBAL_WINDOW_SECS {
            st.global = (1, now);
        } else {
            st.global.0 = st.global.0.saturating_add(1);
        }
        // 账号窗口
        let w = st
            .per_user
            .entry(username.to_string())
            .or_insert(UserWindow {
                fails: 0,
                window_start: now,
                locked_until: 0,
            });
        if now - w.window_start >= USER_WINDOW_SECS {
            w.fails = 0;
            w.window_start = now;
        }
        w.fails = w.fails.saturating_add(1);
        if w.fails >= USER_FAILS_MAX {
            w.locked_until = now + USER_LOCK_SECS;
            w.fails = 0; // 锁定期间不再累加；解锁后窗口从头计
            w.window_start = now + USER_LOCK_SECS;
        }
        // 防内存无界：条目过多时清掉已过期窗口（简陋但足够——ponytail）
        if st.per_user.len() > 10_000 {
            st.per_user
                .retain(|_, w| w.locked_until > now || now - w.window_start < USER_WINDOW_SECS);
        }
    }

    /// 成功即清账号窗口（全局窗口不清——一次成功不洗白洪水信号）。
    pub fn on_success(&self, username: &str) {
        let mut st = self.inner.lock().unwrap();
        st.per_user.remove(username);
    }
}

impl Default for LoginLimiter {
    fn default() -> Self {
        Self::new()
    }
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

        let db = Arc::new(match cfg.raft.as_ref() {
            Some(r) if r.role == crate::config::RaftRole::Witness => {
                Db::connect_witness(&cfg.database_url).await?
            }
            Some(_) => Db::connect_raft(&cfg.database_url).await?,
            None => Db::connect(&cfg.database_url).await?,
        });
        // M7（R2）：`[site]` 覆盖必须早于任何控制面写（桶对账/管理员引导）；测试路径
        // 经 `AppState::new` 同样会再应用一次（幂等）。
        if let Some(site) = cfg.site.as_ref() {
            db.apply_site_config(
                site.name.as_deref(),
                Some(site.region.as_str()),
                Some(site.priority),
                site.token.as_deref(),
            )
            .await?;
        }
        // M9：无条件模式守卫（connect_* 已执行一次；这里按计划顺序在 site 配置后复验）。
        db.prepare_raft_mode(cfg.raft.is_some()).await?;
        let mut store_cfg = StoreConfig::new(&drives[0]);
        // M9 S1：sync 写提交门（必须在 Store::open 前注入——open 克隆配置）。
        store_cfg.gate = Some(Arc::new(crate::sync::SiteSyncGate::new(
            Arc::clone(&db),
            std::time::Duration::from_millis(cfg.sync.timeout_ms),
            cfg.sync.max_object_bytes,
            cfg.site.is_some(),
        )));
        store_cfg.sync_max_bytes = cfg.sync.max_object_bytes;
        store_cfg.drives = drives;
        store_cfg.ec = xl_store::store::ec::EcConfig {
            data: cfg.ec.data,
            parity: cfg.ec.parity,
            enabled: cfg.ec.enabled,
        };
        store_cfg.inline_threshold = cfg.ec.inline_threshold;
        // B10：控制面删桶路径的清理实现须在 Store::open 前注入（集群控制面写）。
        let ap_cache = Arc::new(xl_s3::ap::ApCache::new());
        let olap_cache = Arc::new(xl_s3::olap::OlapCache::new());
        let bucket_cleanup: Arc<dyn xl_s3::bucket_cleanup::BucketCleanupSink> =
            Arc::new(SqlBucketCleanup::new(
                Arc::clone(&db),
                Arc::clone(&ap_cache),
                Arc::clone(&olap_cache),
            ));
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
            store_cfg.control = Some(Arc::new(
                crate::cluster_control::SqliteControlBackend::new(db.pool().clone())
                    .with_bucket_cleanup(Some(Arc::clone(&bucket_cleanup))),
            ));
        }
        let store = Store::open(store_cfg).await?;

        // M5 切片 2：密钥库接线（**前提块**：须在 open 成功后——掉盘首启不得生成新主密钥；
        // 未在册时 init_encryption 内部门闩会拒启）。env 一个不设 = 不启用加密（零行为变化）：
        // - OSS_MASTER_KEY_FILE：SSE-S3 单钥文件路径（provider file/"file"）
        // - OSS_KMS_DIR：SSE-KMS 多钥目录路径（provider kms/"k<n>"）
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

        // M7（P3）：两轮启动对账期间抑制站点触发器——维护写（set_id 收敛/补行）不得
        // 产生 changelog；结束后复位 0。进程若中途退出，下次 connect 的 site_bootstrap 复位。
        if let Err(e) = db.set_apply_ctx(true).await {
            tracing::warn!(error = %e, "启动对账抑制开关失败（继续）");
        }
        // 历史桶回填（权限体系）：盘上有、buckets 表里没有的桶补一行（owner NULL ⇒
        // 仅管理员可见/可管）。INSERT OR IGNORE 幂等，已存在（含已有 owner）不覆盖。
        match store.list_buckets().await {
            Ok(buckets) => {
                for b in buckets {
                    // M4 切片 8：INSERT OR IGNORE 不覆盖已有行（新桶在创建时已带 owner/set）；
                    // 无行者先扇出问 set：有答按答插；「全部可达且全无」→ set0+warn（存量语义）；
                    // **任一传输错 → 不插行**（下次启动重试；「不可达」不得写死 set0——
                    // verifier 终审洞 2：升级后 set=1 桶的行在不可达窗口被写死 0 即分片错组）
                    match store.resolve_bucket_set_authoritative(&b.name).await {
                        // P3-3：权威 set **对账**（不止补缺）——陈旧行（删桶重建后别节点残留）在此收敛；
                        // M5 切片 0：改走**权威**解析（跳过本地短路——原实现把本地陈旧值原样写回，收敛不成立）
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

        // M5 切片 0（P4-1）第二对账面：表行全量——上面的磁盘循环看不见「行在而目录不在」的
        // 陈旧行（删桶广播未达该节点 + 目录删除已生效）。对端有行 → 权威值收敛；对端全无行
        // → 保守不动（可能是本节点建的桶而对端漏收广播——删行会把本节点的活桶 404 掉；
        // 该残窗随重建广播/下轮重启收敛，彻底强一致归 M9）。
        match db.list_bucket_rows().await {
            Ok(names) => {
                for name in names {
                    match store.resolve_bucket_set_authoritative(&name).await {
                        Ok(Some(s)) => {
                            if let Err(e) = db.upsert_bucket_set(&name, s).await {
                                tracing::warn!(error = %e, bucket = %name, "行对账收敛失败");
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, bucket = %name, "行对账：权威解析不可达，本轮跳过");
                        }
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "启动对账：列举桶行失败"),
        }
        if let Err(e) = db.set_apply_ctx(false).await {
            tracing::warn!(error = %e, "启动对账抑制复位失败");
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
                // M9 N8：identity/配对使用首个管理员名；已写入不覆盖。
                db.schema_meta_set_if_absent("admin_username", &user)
                    .await?;
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
        // M7（L2）：groups 桶的记账写也属于启动维护，必须抑制站点触发器（否则时钟领先时
        // INSERT OR IGNORE 仍会触发 BEFORE 门控 ABORT，进程起不来）。
        if let Err(e) = db.set_apply_ctx(true).await {
            tracing::warn!(error = %e, "groups 回填抑制开关失败（继续）");
        }
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
        if let Err(e) = db.set_apply_ctx(false).await {
            tracing::warn!(error = %e, "groups 回填抑制复位失败");
        }

        Ok(Self::new_with(cfg, db, store, ap_cache, olap_cache, bucket_cleanup).await)
    }

    /// 集群控制面扇出面（Task 9；None = 单节点——三面 Acl/SigV4Auth 共用此构造）。
    pub fn control_mesh(&self) -> Option<xl_s3::access::ControlMesh> {
        mesh_of(&self.store)
    }

    /// 三面共用的判权实例（Task 9：集群时自动带读扇出；SQL 唯一事实源在 xl-s3::Acl）。
    pub fn acl(&self) -> xl_s3::access::Acl {
        xl_s3::access::Acl::with_mesh(self.db.pool().clone(), self.control_mesh())
    }

    /// M8 S6：控制面写操作审计（best-effort——落行失败只 warn，绝不改变动作结果）。
    pub(crate) async fn audit(
        &self,
        uid: i64,
        op: &str,
        bucket: Option<&str>,
        key: Option<&str>,
        detail: Option<&str>,
    ) {
        if let Err(e) = self
            .acl()
            .write_audit(uid, op, bucket, key, "allow", detail)
            .await
        {
            tracing::warn!(error = %e, op, "控制面审计落行失败");
        }
    }

    pub async fn new(cfg: AppConfig, db: Db, store: Store) -> Self {
        let db = Arc::new(db);
        let ap_cache = Arc::new(xl_s3::ap::ApCache::new());
        let olap_cache = Arc::new(xl_s3::olap::OlapCache::new());
        let bucket_cleanup: Arc<dyn xl_s3::bucket_cleanup::BucketCleanupSink> =
            Arc::new(SqlBucketCleanup::new(
                Arc::clone(&db),
                Arc::clone(&ap_cache),
                Arc::clone(&olap_cache),
            ));
        Self::new_with(cfg, db, store, ap_cache, olap_cache, bucket_cleanup).await
    }

    /// `load` 预建 AP 缓存与清理实现（控制面写要在 `Store::open` 前注入），
    /// tests 走 [`AppState::new`] 同一装配语义。
    async fn new_with(
        cfg: AppConfig,
        db: Arc<Db>,
        store: Store,
        ap_cache: Arc<xl_s3::ap::ApCache>,
        olap_cache: Arc<xl_s3::olap::OlapCache>,
        bucket_cleanup: Arc<dyn xl_s3::bucket_cleanup::BucketCleanupSink>,
    ) -> Self {
        // M7：`[site]` 覆盖种子（先于任何控制面写；load 与 tests 共用此路径）
        if let Some(site) = cfg.site.as_ref() {
            db.apply_site_config(
                site.name.as_deref(),
                Some(site.region.as_str()),
                Some(site.priority),
                site.token.as_deref(),
            )
            .await
            .unwrap_or_else(|e| panic!("[site] 写入失败: {e}"));
        }
        let site = Arc::new(
            db.site_identity()
                .await
                .unwrap_or_else(|e| panic!("site_config 种子缺失/不可读: {e}")),
        );
        let site_enabled = cfg.site.is_some();
        // M8 S1：AP 缓存预加载（坏策略行跳过并告警；运行期 source 查询会再 fail-closed）
        match db.list_access_points(None).await {
            Ok(rows) => {
                for r in rows {
                    match xl_s3::ap::AccessPoint::from_row(
                        r.name,
                        r.bucket,
                        r.owner_id,
                        r.policy_json,
                        &site.region,
                    ) {
                        Ok(ap) => ap_cache.put(ap),
                        Err(e) => tracing::warn!(error = %e, "AP 预加载跳过非法策略行"),
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "AP 预加载失败（路由将按需查询）"),
        }
        // M8 S2：变换器预加载（须在 AP 之后——缓存条目要按 AP 查桶名做级联失效）
        crate::access_points_api::preload_olap(&db, &ap_cache, &olap_cache).await;
        let ap_region = site.region.clone();
        // except 只有 `/login`：console 路由经 nest 时 axum 的 StripPrefix 会先把
        // `/api/v1` 前缀剥掉，JwtLayer 看到的是 `/login`；aether 面路径不含 `/login` 段
        // （`/aetherupload/display/login` 之类整段不匹配），照常校验。
        // secret 短于 32 字节（jwt-rust 的 256 位下限）、黑名单目录不可建，都启动即失败。
        let login_limiter = Arc::new(LoginLimiter::new());
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
                    // M7：站点控制面通道（token / token 头）两条不走 JWT
                    except: vec![
                        "/login".into(),
                        "/site/identity".into(),
                        "/site/changelog".into(),
                        "/site/delete-object".into(),
                    ],
                },
                ..Default::default()
            })
            .unwrap_or_else(|e| panic!("jwt-rust 初始化失败: {e}")),
        );
        // M5 切片 6：SK 库自 Store 的密钥库状态（须在 init_encryption 之后——本函数由
        // 启动路径在 store open+init 完成后调用）。
        let vault = xl_s3::skvault::SecretVault::from_enc_state(store.enc_state()).map(Arc::new);
        // M6 切片 0/1：事件接收方（worker 在 serve 路径挂载；Kafka 目标优先，否则 env 文件 sink）
        let notifier = crate::events::notifier_from_config(&cfg.kafka.targets);
        let events = Arc::new(crate::events::SqlEventSink::new(
            db.pool().clone(),
            notifier,
        ));
        // M6 切片 4：复制（规则存储 + 写入钩子 + 出站客户端；worker 在 serve 路径挂载）
        let remote_ids: Vec<String> = cfg.remotes.iter().map(|r| r.id.clone()).collect();
        let repl_store = Arc::new(crate::replication::SqlReplStore::new(
            db.pool().clone(),
            remote_ids,
        ));
        let store = Arc::new(store);
        let repl_sink = Arc::new(crate::replication::SqlReplSink::new(
            db.pool().clone(),
            Arc::clone(&store),
        ));
        let repl_client = Arc::new(crate::repl_client::ReplClient::new(&cfg.remotes));
        // M8 S4b：tier 后端固定于启动配置；规则校验只认这些 id。
        let tiers = Arc::new(crate::tier::TierBackends::from_config(&cfg.tiers));
        let lifecycle: Arc<dyn xl_s3::lifecycle::LifecycleStore> = Arc::new(
            crate::lifecycle_store::SqlLifecycleStore::new(Arc::clone(&db), Arc::new(tiers.ids())),
        );
        let restore_sink: Arc<dyn xl_s3::restore::RestoreSink> =
            Arc::new(crate::tier_jobs::SqlRestoreSink::new(Arc::clone(&db)));
        let tier_gc: Arc<dyn xl_s3::tier_gc::TierGcSink> =
            Arc::new(crate::tier::S3TierGc::new(Arc::clone(&tiers)));
        // M8 S5：Inventory 配置存储与执行器（共用三面判权实例语义）。
        let inv_acl = xl_s3::access::Acl::with_mesh(db.pool().clone(), mesh_of(&store));
        let inventory: Arc<dyn xl_s3::inventory::InventoryStore> = Arc::new(
            crate::inventory_store::SqlInventoryStore::new(Arc::clone(&db), inv_acl.clone()),
        );
        let inventory_runner = Arc::new(crate::inventory::InventoryRunner::new(
            Arc::clone(&db),
            Arc::clone(&store),
            inv_acl,
            cfg.staging_dir.clone(),
        ));
        let cfg = Arc::new(cfg);
        let ingest = Arc::new(crate::aether::IngestSink {
            store: Arc::clone(&store),
            db: Arc::clone(&db),
            cfg: Arc::clone(&cfg),
            events: Some(Arc::clone(&events)),
            repl: Some(Arc::clone(&repl_sink)),
        });
        Self {
            cfg,
            db,
            store,
            ingest,
            jwt,
            login_limiter,
            vault,
            events,
            repl_sink,
            repl_store,
            repl_client,
            site,
            site_enabled,
            ap_cache,
            olap_cache,
            ap_region,
            bucket_cleanup,
            lifecycle,
            tiers,
            restore_sink,
            tier_gc,
            inventory,
            inventory_runner,
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
mod login_limiter_tests {
    use super::*;

    /// 5 次失败 → 锁定；锁定期内即使口令正确也拒（check 先于校验）；成功清零。
    #[test]
    fn user_lockout_and_reset() {
        let l = LoginLimiter::new();
        for _ in 0..USER_FAILS_MAX {
            assert!(l.check("u").is_ok());
            l.on_failure("u");
        }
        assert!(
            l.check("u").is_err(),
            "第 {} 次失败后应锁定",
            USER_FAILS_MAX
        );
        // 别的账号不受影响
        assert!(l.check("v").is_ok());
        // 过期解锁（直接拨表：把锁定窗口拨到过去）
        {
            let mut st = l.inner.lock().unwrap();
            let w = st.per_user.get_mut("u").unwrap();
            w.locked_until = limiter_now() - 1;
            w.window_start = limiter_now() - 1;
        }
        assert!(l.check("u").is_ok(), "锁定过期应放行");
        // 成功后窗口清零
        l.on_failure("w");
        l.on_success("w");
        assert!(l.check("w").is_ok());
        assert!(!l.inner.lock().unwrap().per_user.contains_key("w"));
    }

    /// 全局失败上限：跨账号洪水被整体暂拒；窗口滑动后恢复。
    #[test]
    fn global_flood_ceiling() {
        let l = LoginLimiter::new();
        for i in 0..GLOBAL_FAILS_MAX {
            l.on_failure(&format!("flood{i}"));
        }
        assert!(l.check("anyone").is_err(), "全局失败上限应暂拒全体");
        // 拨回窗口 → 恢复
        {
            let mut st = l.inner.lock().unwrap();
            st.global.1 = limiter_now() - GLOBAL_WINDOW_SECS - 1;
        }
        assert!(l.check("anyone").is_ok(), "窗口滑过后应恢复");
    }
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
