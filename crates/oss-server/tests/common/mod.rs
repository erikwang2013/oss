pub mod tls;
use oss_server::config::{AppConfig, GroupTarget, SiteToml, TierTarget};
use oss_server::db::Db;
use oss_server::state::AppState;
use xl_store::store::{Store, StoreConfig};

pub mod raft_harness;

/// 测试共用 secret（jwt-rust 要求 ≥32 字符）；测试自己造令牌时也用它签名。
pub const JWT_SECRET: &str = "0123456789abcdef0123456789abcdef";

/// 部分测试 target 只用 `state_with_site`；每个 target 独立编译 common，避免 dead_code 告警
/// 在 `clippy -D warnings` 下误杀。
#[allow(dead_code)]
pub async fn state(dir: &std::path::Path) -> AppState {
    state_with_site(dir, None).await
}

/// M7 测试用：带 `[site]` 的 AppState（None = M7 关闭，站点路由 404）。
pub async fn state_with_site(dir: &std::path::Path, site: Option<SiteToml>) -> AppState {
    state_with_targets(dir, site, vec![]).await
}

/// M8 S4b 测试用：带 `[[tiers]]` 的 AppState（后端由用例替换为 mock）。
#[allow(dead_code)]
pub async fn state_with_tiers(dir: &std::path::Path, tiers: Vec<TierTarget>) -> AppState {
    state_with_targets(dir, None, tiers).await
}

/// M8 AP 测试用：放大单 PUT 上限以覆盖 >1 MiB 的 AP 自定义路由请求。
#[allow(dead_code)]
pub async fn state_with_max_put(dir: &std::path::Path, max_put_bytes: usize) -> AppState {
    state_with_targets_max(dir, None, vec![], max_put_bytes).await
}

async fn state_with_targets(
    dir: &std::path::Path,
    site: Option<SiteToml>,
    tiers: Vec<TierTarget>,
) -> AppState {
    state_with_targets_max(dir, site, tiers, 1024 * 1024).await
}

async fn state_with_targets_max(
    dir: &std::path::Path,
    site: Option<SiteToml>,
    tiers: Vec<TierTarget>,
    max_put_bytes: usize,
) -> AppState {
    let cfg = AppConfig {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.join("data"),
        data_dirs: None,
        ec: Default::default(),
        staging_dir: dir.join("staging"),
        database_url: "sqlite::memory:".into(),
        jwt_secret: JWT_SECRET.into(),
        max_put_bytes,
        groups: vec![GroupTarget {
            group: "file".into(),
            bucket: "files".into(),
            max_size: 0,
            extensions: vec![],
        }],
        cluster: None, // 单节点测试形态；集群 fixture 走 in-process 基座（xl-store::store::tests）
        kafka: Default::default(),
        remotes: vec![],
        tiers,
        site,
        raft: None,
        sync: Default::default(),
        stretch: None,
        tls: Default::default(),
        sts: Default::default(),
    };
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let store = Store::open(StoreConfig::new(&cfg.data_dir)).await.unwrap();
    let db = Db::connect(&cfg.database_url).await.unwrap();
    AppState::new(cfg, db, store).await
}
