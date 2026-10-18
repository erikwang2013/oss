use oss_server::config::{AppConfig, GroupTarget};
use oss_server::db::Db;
use oss_server::state::AppState;
use xl_store::store::{Store, StoreConfig};

/// 测试共用 secret（jwt-rust 要求 ≥32 字符）；测试自己造令牌时也用它签名。
pub const JWT_SECRET: &str = "0123456789abcdef0123456789abcdef";

pub async fn state(dir: &std::path::Path) -> AppState {
    let cfg = AppConfig {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.join("data"),
        data_dirs: None,
        ec: Default::default(),
        staging_dir: dir.join("staging"),
        database_url: "sqlite::memory:".into(),
        jwt_secret: JWT_SECRET.into(),
        max_put_bytes: 1024 * 1024,
        groups: vec![GroupTarget {
            group: "file".into(),
            bucket: "files".into(),
            max_size: 0,
            extensions: vec![],
        }],
        cluster: None, // 单节点测试形态；集群 fixture 走 in-process 基座（xl-store::store::tests）
        kafka: Default::default(),
        remotes: vec![],
    };
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let store = Store::open(StoreConfig::new(&cfg.data_dir)).await.unwrap();
    let db = Db::connect(&cfg.database_url).await.unwrap();
    AppState::new(cfg, db, store)
}
