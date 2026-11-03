//! `access_points` / `olap_configs` 双后端差分的共享辅助：legacy raw SQL 参考实现
//! （测试内重新实现，不调用生产读方法）。

use std::sync::Arc;

use oss_server::db::Db;
use sqlx::Row;
use xl_store::control_store::{AccessPointEntry, OlapConfigEntry, OwnerRef};

pub async fn fresh() -> Arc<Db> {
    Arc::new(Db::connect("sqlite::memory:").await.unwrap())
}

pub async fn raw_ap(
    db: &Db,
    name: &str,
    bucket: &str,
    owner_id: i64,
    policy: Option<&str>,
    ts: i64,
) {
    sqlx::query(
        "INSERT INTO access_points (name, bucket, owner_id, policy_json, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(name)
    .bind(bucket)
    .bind(owner_id)
    .bind(policy)
    .bind(ts)
    .bind(ts)
    .execute(db.pool())
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
pub async fn raw_olap(
    db: &Db,
    access_point: &str,
    kind: &str,
    endpoint: Option<&str>,
    module: Option<&[u8]>,
    module_sha256: Option<&str>,
    timeout_ms: i64,
    max_response_bytes: i64,
    ts: i64,
) {
    sqlx::query(
        "INSERT INTO olap_configs (access_point, kind, endpoint, module, module_sha256, \
         timeout_ms, max_response_bytes, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(access_point)
    .bind(kind)
    .bind(endpoint)
    .bind(module)
    .bind(module_sha256)
    .bind(timeout_ms)
    .bind(max_response_bytes)
    .bind(ts)
    .execute(db.pool())
    .await
    .unwrap();
}

pub type RawAp = (String, String, i64, Option<String>, i64, i64);
pub type RawOlap = (
    String,
    String,
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
    i64,
    i64,
    i64,
);

pub async fn raw_aps(db: &Db) -> Vec<RawAp> {
    sqlx::query(
        "SELECT name, bucket, owner_id, policy_json, created_at, updated_at \
         FROM access_points ORDER BY name",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("name"),
            r.get("bucket"),
            r.get("owner_id"),
            r.get("policy_json"),
            r.get("created_at"),
            r.get("updated_at"),
        )
    })
    .collect()
}

pub async fn raw_olaps(db: &Db) -> Vec<RawOlap> {
    sqlx::query(
        "SELECT access_point, kind, endpoint, module, module_sha256, timeout_ms, \
         max_response_bytes, updated_at FROM olap_configs ORDER BY access_point",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("access_point"),
            r.get("kind"),
            r.get("endpoint"),
            r.get("module"),
            r.get("module_sha256"),
            r.get("timeout_ms"),
            r.get("max_response_bytes"),
            r.get("updated_at"),
        )
    })
    .collect()
}

/// 旧运行期读口径：owner_id=0 或用户行缺失 → Admin；用户行存在 → User（管理员亦然）。
pub async fn legacy_runtime_aps(db: &Db) -> Vec<AccessPointEntry> {
    sqlx::query(
        "SELECT a.name, a.bucket, a.owner_id, a.policy_json, a.created_at, a.updated_at, \
         u.username AS owner_username FROM access_points a \
         LEFT JOIN users u ON u.id = a.owner_id ORDER BY a.name",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        let owner_id: i64 = r.get("owner_id");
        let username: Option<String> = r.get("owner_username");
        AccessPointEntry {
            name: r.get("name"),
            bucket: r.get("bucket"),
            owner: match (owner_id, username) {
                (0, _) | (_, None) => OwnerRef::Admin,
                (_, Some(u)) => OwnerRef::User(u),
            },
            policy_json: r.get("policy_json"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        }
    })
    .collect()
}

pub async fn legacy_olap_configs(db: &Db) -> Vec<OlapConfigEntry> {
    sqlx::query(
        "SELECT access_point, kind, endpoint, module, module_sha256, timeout_ms, \
         max_response_bytes, updated_at FROM olap_configs ORDER BY access_point",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| OlapConfigEntry {
        access_point: r.get("access_point"),
        kind: r.get("kind"),
        endpoint: r.get("endpoint"),
        module: r.get("module"),
        module_sha256: r.get("module_sha256"),
        timeout_ms: r.get("timeout_ms"),
        max_response_bytes: r.get("max_response_bytes"),
        updated_at: r.get("updated_at"),
    })
    .collect()
}

/// 旧快照导出映射：owner_id=0 / is_admin=1 → Admin；用户行存在 → User；缺失 → 错误。
pub async fn legacy_export_aps(db: &Db) -> Vec<AccessPointEntry> {
    sqlx::query(
        "SELECT a.name, a.bucket, a.owner_id, a.policy_json, a.created_at, a.updated_at, \
         u.username AS owner_username, u.is_admin AS owner_is_admin FROM access_points a \
         LEFT JOIN users u ON u.id = a.owner_id ORDER BY a.name",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        let owner_id: i64 = r.get("owner_id");
        let username: Option<String> = r.get("owner_username");
        let is_admin: Option<i64> = r.get("owner_is_admin");
        AccessPointEntry {
            name: r.get("name"),
            bucket: r.get("bucket"),
            owner: if owner_id == 0 {
                OwnerRef::Admin
            } else {
                match (username, is_admin) {
                    (Some(_), Some(1)) => OwnerRef::Admin,
                    (Some(u), _) => OwnerRef::User(u),
                    (None, _) => panic!("legacy export: owner user id {owner_id} missing"),
                }
            },
            policy_json: r.get("policy_json"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        }
    })
    .collect()
}

/// 旧快照布局的独立参考编码：字段顺序不依赖生产 `Serialize` 实现。
#[derive(serde::Serialize)]
pub struct LegacyApRow {
    pub name: String,
    pub bucket: String,
    pub owner: OwnerRef,
    pub policy_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

pub fn legacy_ap_json(rows: &[AccessPointEntry]) -> Vec<u8> {
    let legacy: Vec<LegacyApRow> = rows
        .iter()
        .map(|r| LegacyApRow {
            name: r.name.clone(),
            bucket: r.bucket.clone(),
            owner: r.owner.clone(),
            policy_json: r.policy_json.clone(),
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
        .collect();
    serde_json::to_vec(&legacy).unwrap()
}
