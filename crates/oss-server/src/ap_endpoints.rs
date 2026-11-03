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
    use sqlx::Row as _;

    /// 差分：`SqliteControlStore` 读出的完整行必须与旧 raw SQL 读取一致；
    /// 列表按 `vpce_id` 升序；静态回退只在 `None`，DB 错误必须上抛。
    #[tokio::test]
    async fn vpc_endpoint_reads_match_legacy_rows_and_fallback_only_on_none() {
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
        for (vpce, vpc, state, created, updated) in [
            ("vpce-b", "vpc-b", "disabled", 20, 21),
            ("vpce-a", "vpc-a", "active", 10, 11),
        ] {
            sqlx::query(
                "INSERT INTO vpc_endpoints \
                 (vpce_id, vpc_id, account_id, state, created_at, updated_at) \
                 VALUES (?, ?, '000000000001', ?, ?, ?)",
            )
            .bind(vpce)
            .bind(vpc)
            .bind(state)
            .bind(created)
            .bind(updated)
            .execute(db.pool())
            .await
            .unwrap();
        }

        // 点查：与旧 raw SQL 读取逐字段一致。
        let legacy = sqlx::query(
            "SELECT vpce_id, vpc_id, account_id, state, created_at, updated_at \
             FROM vpc_endpoints WHERE vpce_id = ?",
        )
        .bind("vpce-a")
        .fetch_one(db.pool())
        .await
        .unwrap();
        let got = db
            .control_store()
            .vpc_endpoint("vpce-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                got.vpce_id.as_str(),
                got.vpc_id.as_str(),
                got.account_id.as_str(),
                got.state.as_str(),
                got.created_at,
                got.updated_at,
            ),
            (
                legacy.get::<String, _>("vpce_id").as_str(),
                legacy.get::<String, _>("vpc_id").as_str(),
                legacy.get::<String, _>("account_id").as_str(),
                legacy.get::<String, _>("state").as_str(),
                legacy.get::<i64, _>("created_at"),
                legacy.get::<i64, _>("updated_at"),
            )
        );
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

        // 列表按 `vpce_id` 升序（插入顺序相反）。
        let ids: Vec<_> = db
            .control_store()
            .vpc_endpoints()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.vpce_id)
            .collect();
        assert_eq!(ids, ["vpce-a", "vpce-b"]);

        // 静态回退只在 DB 查无（None）时发生。
        let fallback = src.lookup("vpce-static").await.unwrap().unwrap();
        assert_eq!(fallback.vpc_id, "vpc-s");
        assert!(src.lookup("vpce-missing").await.unwrap().is_none());

        // DB 错误：Err 上抛，绝不回退静态配置。
        db.pool().close().await;
        let err = src.lookup("vpce-static").await.unwrap_err();
        assert!(
            err.starts_with("vpc_endpoints lookup: control store backend: "),
            "{err}"
        );
    }

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
