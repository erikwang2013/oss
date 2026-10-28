pub mod tls;
use std::sync::Arc;

use oss_server::config::{AppConfig, GroupTarget, SiteToml, TierTarget};

/// 直接改写盘上 xl.meta（`Store::write_meta` 不对外）：模拟 meta 篡改/旧形态对象。
#[allow(dead_code)]
pub fn rewrite_meta(
    data_dir: &std::path::Path,
    bucket: &str,
    key: &str,
    f: impl FnOnce(&mut xl_store::meta::XlMeta),
) {
    let path = xl_store::layout::object_dir(data_dir, bucket, key)
        .unwrap()
        .join(xl_store::meta::META_FILE);
    let mut m = xl_store::meta::XlMeta::decode(&std::fs::read(&path).unwrap()).unwrap();
    f(&mut m);
    std::fs::write(&path, m.encode().unwrap()).unwrap();
}

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
    state_with_targets_max(dir, None, vec![], max_put_bytes, false).await
}

/// M3 shrink T3：maintenance=true 的 HTTP 面（非读方法应 503）。
#[allow(dead_code)]
pub async fn state_in_maintenance(dir: &std::path::Path) -> AppState {
    state_with_targets_max(dir, None, vec![], 1024 * 1024, true).await
}

/// AP Slice A 测试用：注入可变的端点 registry（构造后、`build_router` 前设置，
/// 测试可先播种账户再补行；避免并行用例改进程 env）。
#[allow(dead_code)]
pub async fn state_with_endpoint_source(
    dir: &std::path::Path,
    source: Arc<dyn xl_s3::ap::ApEndpointSource>,
) -> AppState {
    let mut st = state(dir).await;
    st.ap_endpoints = source;
    st
}

async fn state_with_targets(
    dir: &std::path::Path,
    site: Option<SiteToml>,
    tiers: Vec<TierTarget>,
) -> AppState {
    state_with_targets_max(dir, site, tiers, 1024 * 1024, false).await
}

async fn state_with_targets_max(
    dir: &std::path::Path,
    site: Option<SiteToml>,
    tiers: Vec<TierTarget>,
    max_put_bytes: usize,
    maintenance: bool,
) -> AppState {
    state_with_targets_url(
        dir,
        site,
        tiers,
        max_put_bytes,
        maintenance,
        "sqlite::memory:",
    )
    .await
}

/// 长跑用例专用：文件库。`sqlite::memory:` 是命名共享缓存内存库，sqlx 默认
/// `idle_timeout=600s` 回收最后一个连接后库即销毁；高负载下单用例 >600s 不碰 DB，
/// 后续访问报 `no such table`。文件库在连接重建后 schema 仍在（不改生产默认值）。
#[allow(dead_code)]
pub async fn state_with_tiers_file_db(dir: &std::path::Path, tiers: Vec<TierTarget>) -> AppState {
    let url = format!("sqlite://{}?mode=rwc", dir.join("test.db").display());
    state_with_targets_url(dir, None, tiers, 1024 * 1024, false, &url).await
}

async fn state_with_targets_url(
    dir: &std::path::Path,
    site: Option<SiteToml>,
    tiers: Vec<TierTarget>,
    max_put_bytes: usize,
    maintenance: bool,
    database_url: &str,
) -> AppState {
    let cfg = AppConfig {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.join("data"),
        data_dirs: None,
        ec: Default::default(),
        maintenance,
        staging_dir: dir.join("staging"),
        database_url: database_url.into(),
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
        ap: Default::default(),
    };
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let store = Store::open(StoreConfig::new(&cfg.data_dir)).await.unwrap();
    let db = Db::connect(&cfg.database_url).await.unwrap();
    AppState::new(cfg, db, store).await
}
