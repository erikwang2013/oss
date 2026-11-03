//! AP/OLAP 的 SQLite 读写实现。
//!
//! 运行期属主口径（`ControlStore`）：`owner_id == 0` 或用户行缺失 → `Admin`
//! （宽松；悬空属主等价 legacy“管理员可见”，且不会让整批预加载失败）。
//! 快照导出口径（`ControlTxn::access_points`，严格 fail-closed）见 Task 4。

use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqliteConnection};
use xl_store::control_store::{AccessPointEntry, ControlError, OlapConfigEntry, OwnerRef};

use super::{SqliteControlStore, SqliteControlTxn};

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

const AP_SELECT: &str = "SELECT a.name, a.bucket, a.owner_id, a.policy_json, a.created_at, a.updated_at, \
     u.username AS owner_username \
     FROM access_points a LEFT JOIN users u ON u.id = a.owner_id";

const OLAP_SELECT: &str = "SELECT access_point, kind, endpoint, module, module_sha256, timeout_ms, \
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

// ---------- 写路径（调用方事务内） ----------

/// `Admin` → 首个管理员 id，无管理员 → 0（witness 哨兵）；`User(name)` → users.id，
/// 缺失且存在管理员 → Invalid，缺失且无管理员 → 0（与 apply::admin_owner_id 等价）。
async fn resolve_owner(conn: &mut SqliteConnection, owner: &OwnerRef) -> Result<i64, ControlError> {
    let admin: Option<i64> =
        sqlx::query_scalar("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1")
            .fetch_optional(&mut *conn)
            .await
            .map_err(backend)?;
    match owner {
        OwnerRef::Admin => Ok(admin.unwrap_or(0)),
        OwnerRef::User(username) => {
            let id: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE username = ?")
                .bind(username)
                .fetch_optional(&mut *conn)
                .await
                .map_err(backend)?;
            match id {
                Some(id) => Ok(id),
                None if admin.is_some() => Err(ControlError::Invalid(format!(
                    "owner user {username} not found"
                ))),
                None => Ok(0),
            }
        }
    }
}

const AP_EXPORT_SELECT: &str = "SELECT a.name, a.bucket, a.owner_id, a.policy_json, a.created_at, a.updated_at, \
     u.username AS owner_username, u.is_admin AS owner_is_admin \
     FROM access_points a LEFT JOIN users u ON u.id = a.owner_id";

/// 严格导出映射：`owner_id == 0` 或 `is_admin == 1` → Admin；用户行缺失 → Invalid。
fn export_access_point_from(r: &SqliteRow) -> Result<AccessPointEntry, ControlError> {
    let owner_id: i64 = r.get("owner_id");
    let username: Option<String> = r.get("owner_username");
    let is_admin: Option<i64> = r.get("owner_is_admin");
    let owner = if owner_id == 0 {
        OwnerRef::Admin
    } else {
        match (username, is_admin) {
            (Some(_), Some(1)) => OwnerRef::Admin,
            (Some(u), _) => OwnerRef::User(u),
            (None, _) => {
                return Err(ControlError::Invalid(format!(
                    "owner user id {owner_id} missing"
                )));
            }
        }
    };
    Ok(AccessPointEntry {
        name: r.get("name"),
        bucket: r.get("bucket"),
        owner,
        policy_json: r.get("policy_json"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    })
}

async fn insert_access_point(
    conn: &mut SqliteConnection,
    row: &AccessPointEntry,
) -> Result<(), ControlError> {
    let owner_id = resolve_owner(conn, &row.owner).await?;
    sqlx::query(
        "INSERT INTO access_points \
         (name, bucket, owner_id, policy_json, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(&row.name)
    .bind(&row.bucket)
    .bind(owner_id)
    .bind(&row.policy_json)
    .bind(row.created_at)
    .bind(row.updated_at)
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_access_point_create(
    txn: &mut SqliteControlTxn<'_>,
    row: &AccessPointEntry,
) -> Result<bool, ControlError> {
    let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM access_points WHERE name = ?")
        .bind(&row.name)
        .fetch_optional(&mut *txn.conn)
        .await
        .map_err(backend)?;
    if exists.is_some() {
        return Ok(false);
    }
    insert_access_point(txn.conn, row).await?;
    Ok(true)
}

pub(super) async fn txn_access_point_set_policy(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
    policy_json: Option<&str>,
    updated_at: i64,
) -> Result<bool, ControlError> {
    let res =
        sqlx::query("UPDATE access_points SET policy_json = ?, updated_at = ? WHERE name = ?")
            .bind(policy_json)
            .bind(updated_at)
            .bind(name)
            .execute(&mut *txn.conn)
            .await
            .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn txn_access_point_delete(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
) -> Result<bool, ControlError> {
    sqlx::query("DELETE FROM olap_configs WHERE access_point = ?")
        .bind(name)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    let res = sqlx::query("DELETE FROM access_points WHERE name = ?")
        .bind(name)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn txn_access_point_exists(
    txn: &mut SqliteControlTxn<'_>,
    name: &str,
) -> Result<bool, ControlError> {
    let row: Option<i64> = sqlx::query_scalar("SELECT 1 FROM access_points WHERE name = ?")
        .bind(name)
        .fetch_optional(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(row.is_some())
}

pub(super) async fn txn_access_points(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<AccessPointEntry>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{AP_EXPORT_SELECT} ORDER BY a.name"
    )))
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    rows.iter().map(export_access_point_from).collect()
}

pub(super) async fn txn_replace_access_points(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[AccessPointEntry],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM olap_configs")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    sqlx::query("DELETE FROM access_points")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        insert_access_point(txn.conn, row).await?;
    }
    Ok(())
}

pub(super) async fn txn_olap_config_upsert(
    txn: &mut SqliteControlTxn<'_>,
    row: &OlapConfigEntry,
) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO olap_configs (access_point, kind, endpoint, module, module_sha256, \
         timeout_ms, max_response_bytes, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(access_point) DO UPDATE SET kind = excluded.kind, \
         endpoint = excluded.endpoint, module = excluded.module, \
         module_sha256 = excluded.module_sha256, timeout_ms = excluded.timeout_ms, \
         max_response_bytes = excluded.max_response_bytes, updated_at = excluded.updated_at",
    )
    .bind(&row.access_point)
    .bind(&row.kind)
    .bind(&row.endpoint)
    .bind(&row.module)
    .bind(&row.module_sha256)
    .bind(row.timeout_ms)
    .bind(row.max_response_bytes)
    .bind(row.updated_at)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(())
}

pub(super) async fn txn_olap_config_delete(
    txn: &mut SqliteControlTxn<'_>,
    access_point: &str,
) -> Result<bool, ControlError> {
    let res = sqlx::query("DELETE FROM olap_configs WHERE access_point = ?")
        .bind(access_point)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(res.rows_affected() > 0)
}

pub(super) async fn txn_olap_configs(
    txn: &mut SqliteControlTxn<'_>,
) -> Result<Vec<OlapConfigEntry>, ControlError> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "{OLAP_SELECT} ORDER BY access_point"
    )))
    .fetch_all(&mut *txn.conn)
    .await
    .map_err(backend)?;
    Ok(rows.iter().map(olap_config_from).collect())
}

pub(super) async fn txn_replace_olap_configs(
    txn: &mut SqliteControlTxn<'_>,
    rows: &[OlapConfigEntry],
) -> Result<(), ControlError> {
    sqlx::query("DELETE FROM olap_configs")
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    for row in rows {
        txn_olap_config_upsert(txn, row).await?;
    }
    Ok(())
}

pub(super) async fn txn_delete_access_points_for_bucket(
    txn: &mut SqliteControlTxn<'_>,
    bucket: &str,
) -> Result<(), ControlError> {
    sqlx::query(
        "DELETE FROM olap_configs WHERE access_point IN \
         (SELECT name FROM access_points WHERE bucket = ?)",
    )
    .bind(bucket)
    .execute(&mut *txn.conn)
    .await
    .map_err(backend)?;
    sqlx::query("DELETE FROM access_points WHERE bucket = ?")
        .bind(bucket)
        .execute(&mut *txn.conn)
        .await
        .map_err(backend)?;
    Ok(())
}
