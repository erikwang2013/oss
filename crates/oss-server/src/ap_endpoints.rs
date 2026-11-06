//! AP Slice B：动态 VPC 端点 registry（feature 8）。
//!
//! 查询优先走复制表 `vpc_endpoints`（Raft apply/快照安装维护），未命中回退静态
//! `[[ap.vpc_endpoints]]` 配置；DB 查询失败返回 `Err`，路由层按 500 fail-closed。
//! 表行仍是**注册断言**，不是密码学端点身份（与 Slice A 同口径）。

use std::collections::HashMap;
use std::sync::Arc;

use xl_s3::ap::{ApEndpoint, ApEndpointSource};
use xl_store::control_store::ControlStore;

use crate::db::Db;

pub struct DbApEndpointSource {
    store: Arc<dyn ControlStore>,
    static_rows: HashMap<String, ApEndpoint>,
}

impl DbApEndpointSource {
    pub fn new(db: Arc<Db>, static_rows: impl IntoIterator<Item = ApEndpoint>) -> Self {
        Self {
            store: db.control_store(),
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
            .store
            .vpc_endpoint(vpce)
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
    use xl_store::control_state::root::Mode;
    use xl_store::control_store::VpcEndpoint;
    use xl_store::store::{Store, StoreConfig};

    async fn object_db() -> (tempfile::TempDir, Arc<Db>) {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = StoreConfig::new(dir.path());
        cfg.fsync = false;
        let store = Arc::new(Store::open(cfg).await.unwrap());
        let object = crate::control_backend::ObjectControl::open(store, Mode::Standalone, None)
            .await
            .unwrap();
        let db = Arc::new(Db::connect_object().await.unwrap());
        db.attach_object_control(object).unwrap();
        (dir, db)
    }

    async fn put(db: &Arc<Db>, row: VpcEndpoint) {
        db.control_write("ap-endpoints-test", move |txn| {
            let row = row.clone();
            Box::pin(async move { txn.vpc_endpoint_upsert(&row).await })
        })
        .await
        .unwrap();
    }

    /// 列表按 `vpce_id` 升序；动态行覆盖静态行，静态回退只在查无时发生。
    #[tokio::test]
    async fn db_overrides_and_static_falls_back() {
        let (_dir, db) = object_db().await;
        let src = DbApEndpointSource::new(
            Arc::clone(&db),
            [ApEndpoint {
                vpce_id: "vpce-static".into(),
                vpc_id: "vpc-s".into(),
                account_id: "000000000001".into(),
                state: "active".into(),
            }],
        );
        put(
            &db,
            VpcEndpoint {
                vpce_id: "vpce-b".into(),
                vpc_id: "vpc-b".into(),
                account_id: "000000000001".into(),
                state: "disabled".into(),
                created_at: 20,
                updated_at: 21,
            },
        )
        .await;
        put(
            &db,
            VpcEndpoint {
                vpce_id: "vpce-a".into(),
                vpc_id: "vpc-a".into(),
                account_id: "000000000001".into(),
                state: "active".into(),
                created_at: 10,
                updated_at: 11,
            },
        )
        .await;

        let ap = src.lookup("vpce-a").await.unwrap().unwrap();
        assert_eq!(
            (
                ap.vpce_id.as_str(),
                ap.vpc_id.as_str(),
                ap.account_id.as_str(),
                ap.state.as_str(),
            ),
            ("vpce-a", "vpc-a", "000000000001", "active")
        );
        let ids: Vec<_> = db
            .control_store()
            .vpc_endpoints()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.vpce_id)
            .collect();
        assert_eq!(ids, ["vpce-a", "vpce-b"]);

        let fallback = src.lookup("vpce-static").await.unwrap().unwrap();
        assert_eq!(fallback.vpc_id, "vpc-s");
        assert!(src.lookup("vpce-missing").await.unwrap().is_none());

        put(
            &db,
            VpcEndpoint {
                vpce_id: "vpce-static".into(),
                vpc_id: "vpc-d".into(),
                account_id: "000000000001".into(),
                state: "disabled".into(),
                created_at: 1,
                updated_at: 1,
            },
        )
        .await;
        let got = src.lookup("vpce-static").await.unwrap().unwrap();
        assert_eq!(
            (got.vpc_id.as_str(), got.state.as_str()),
            ("vpc-d", "disabled")
        );
    }
}
