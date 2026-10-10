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
    pub ingest: Arc<crate::aether::IngestSink>,
}

impl AppState {
    pub fn new(cfg: AppConfig, db: Db, store: Store) -> Self {
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
        }
    }
}
