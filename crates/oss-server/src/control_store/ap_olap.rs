//! AP/OLAP 的 SQLite 读写实现。
//!
//! 运行期属主口径（`ControlStore`）：`owner_id == 0` 或用户行缺失 → `Admin`
//! （宽松；悬空属主等价 legacy“管理员可见”，且不会让整批预加载失败）。
//! 快照导出口径（`ControlTxn::access_points`，严格 fail-closed）见 Task 4。

use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use xl_store::control_store::{AccessPointEntry, ControlError, OlapConfigEntry, OwnerRef};

use super::SqliteControlStore;

fn backend(e: sqlx::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

fn access_point_from(r: &SqliteRow) -> AccessPointEntry {
    let owner_id: i64 = r.get("owner_id");
    let username: Option<String> = r.get("owner_username");
    let owner = match (owner_id, username) {
        (0, _) | (_, None) => OwnerRef::Admin,
        (_, Some(u)) => OwnerRef::User(u),
    };
    AccessPointEntry {
        name: r.get("name"),
        bucket: r.get("bucket"),
        owner,
        policy_json: r.get("policy_json"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

fn olap_config_from(r: &SqliteRow) -> OlapConfigEntry {
    OlapConfigEntry {
        access_point: r.get("access_point"),
        kind: r.get("kind"),
        endpoint: r.get("endpoint"),
        module: r.get("module"),
        module_sha256: r.get("module_sha256"),
        timeout_ms: r.get("timeout_ms"),
        max_response_bytes: r.get("max_response_bytes"),
        updated_at: r.get("updated_at"),
    }
}

const AP_SELECT: &str =
    "SELECT a.name, a.bucket, a.owner_id, a.policy_json, a.created_at, a.updated_at, \
     u.username AS owner_username \
     FROM access_points a LEFT JOIN users u ON u.id = a.owner_id";

const OLAP_SELECT: &str =
    "SELECT access_point, kind, endpoint, module, module_sha256, timeout_ms, \
     max_response_bytes, updated_at FROM olap_configs";

pub(super) async fn access_point(
    store: &SqliteControlStore,
    name: &str,
) -> Result<Option<AccessPointEntry>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("{AP_SELECT} WHERE a.name = ?")))
        .bind(name)
        .fetch_optional(&store.pool)
        .await
        .map_err(backend)?;
    Ok(row.as_ref().map(access_point_from))
}

pub(super) async fn access_points(
    store: &SqliteControlStore,
) -> Result<Vec<AccessPointEntry>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!("{AP_SELECT} ORDER BY a.name")))
        .fetch_all(&store.pool)
        .await
        .map_err(backend)?;
    Ok(rows.iter().map(access_point_from).collect())
}

pub(super) async fn olap_config(
    store: &SqliteControlStore,
    access_point: &str,
) -> Result<Option<OlapConfigEntry>, ControlError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{OLAP_SELECT} WHERE access_point = ?"
    )))
    .bind(access_point)
    .fetch_optional(&store.pool)
    .await
    .map_err(backend)?;
    Ok(row.as_ref().map(olap_config_from))
}

pub(super) async fn olap_configs(
    store: &SqliteControlStore,
) -> Result<Vec<OlapConfigEntry>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{OLAP_SELECT} ORDER BY access_point"
    )))
    .fetch_all(&store.pool)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(olap_config_from).collect())
}
