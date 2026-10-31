//! AP Slice B：动态 VPC 端点 registry（feature 8）。
//!
//! 查询优先走复制表 `vpc_endpoints`（Raft apply/快照安装维护），未命中回退静态
//! `[[ap.vpc_endpoints]]` 配置；DB 查询失败返回 `Err`，路由层按 500 fail-closed。
//! 表行仍是**注册断言**，不是密码学端点身份（与 Slice A 同口径）。

use std::collections::HashMap;
use std::sync::Arc;

use xl_s3::ap::{ApEndpoint, ApEndpointSource};

use crate::db::Db;

pub struct DbApEndpointSource {
    db: Arc<Db>,
    static_rows: HashMap<String, ApEndpoint>,
}

impl DbApEndpointSource {
    pub fn new(db: Arc<Db>, static_rows: impl IntoIterator<Item = ApEndpoint>) -> Self {
        Self {
            db,
            static_rows: static_rows
                .into_iter()
                .map(|e| (e.vpce_id.clone(), e))
                .collect(),
        }
    }
}

#[async_trait::async_trait]
impl ApEndpointSource for DbApEndpointSource {
    async fn lookup(&self, vpce: &str) -> Result<Option<ApEndpoint>, String> {
        let row = self
            .db
            .vpc_endpoint_get(vpce)
            .await
            .map_err(|e| format!("vpc_endpoints lookup: {e}"))?;
        if let Some(r) = row {
            return Ok(Some(ApEndpoint {
                vpce_id: r.vpce_id,
                vpc_id: r.vpc_id,
                account_id: r.account_id,
                state: r.state,
            }));
        }
        Ok(self.static_rows.get(vpce).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn db_overrides_and_static_falls_back() {
        let db = Arc::new(Db::connect("sqlite::memory:").await.unwrap());
        let src = DbApEndpointSource::new(
            Arc::clone(&db),
            [ApEndpoint {
                vpce_id: "vpce-static".into(),
                vpc_id: "vpc-s".into(),
                account_id: "000000000001".into(),
                state: "active".into(),
            }],
        );
        assert_eq!(
            src.lookup("vpce-static").await.unwrap().unwrap().vpc_id,
            "vpc-s"
        );
        assert!(src.lookup("vpce-dyn").await.unwrap().is_none());
        sqlx::query(
            "INSERT INTO vpc_endpoints \
             (vpce_id, vpc_id, account_id, state, created_at, updated_at) \
             VALUES ('vpce-dyn', 'vpc-d', '000000000001', 'active', 1, 1)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let got = src.lookup("vpce-dyn").await.unwrap().unwrap();
        assert_eq!(
            (got.vpc_id.as_str(), got.state.as_str()),
            ("vpc-d", "active")
        );
    }
}
