//! 请求间共享的应用状态。

use std::sync::Arc;

use crate::config::AppConfig;
use crate::db::Db;
use xl_store::store::Store;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<AppConfig>,
    pub db: Arc<Db>,
    pub store: Arc<Store>,
}

impl AppState {
    pub fn new(cfg: AppConfig, db: Db, store: Store) -> Self {
        Self {
            cfg: Arc::new(cfg),
            db: Arc::new(db),
            store: Arc::new(store),
        }
    }
}
