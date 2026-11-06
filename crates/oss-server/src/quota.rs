//! 账户硬配额后端：对象后端复制账本。

use std::sync::Arc;

use xl_s3::account_quota::QuotaBackend;

use crate::db::Db;
use crate::quota_object::ObjectQuota;

/// 装配对象配额实现（AppState 装配一次，请求间共享）。
pub(crate) fn for_db(db: &Arc<Db>) -> Arc<dyn QuotaBackend> {
    Arc::new(ObjectQuota::new(Arc::clone(db)))
}

/// strict 账本命令的播种基线（不经过 `Acl` 的独立调用点）。
pub(crate) async fn usage_baseline(db: &Db, account: &str) -> (i64, i64) {
    crate::quota_object::usage_baseline(db, account).await
}
