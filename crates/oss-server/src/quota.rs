//! 账户硬配额后端选择：SQLite 本地缓存（H1 前不变）或对象后端复制账本。

use std::sync::Arc;

use xl_s3::account_quota::QuotaBackend;

use crate::config::ControlBackend;
use crate::db::Db;
use crate::quota_object::ObjectQuota;
use crate::quota_sqlite::SqliteQuota;

/// 按 `Db` 已选控制后端装配配额实现（AppState 装配一次，请求间共享）。
pub(crate) fn for_db(db: &Arc<Db>) -> Arc<dyn QuotaBackend> {
    match db.control_backend() {
        ControlBackend::Sqlite => Arc::new(SqliteQuota::new(db.pool().clone())),
        ControlBackend::Objects => Arc::new(ObjectQuota::new(Arc::clone(db))),
    }
}

/// strict 账本命令的播种基线（不经过 `Acl` 的独立调用点）。
pub(crate) async fn usage_baseline(db: &Db, account: &str) -> (i64, i64) {
    match db.control_backend() {
        ControlBackend::Sqlite => crate::quota_sqlite::usage_baseline(db.pool(), account).await,
        ControlBackend::Objects => crate::quota_object::usage_baseline(db, account).await,
    }
}
