//! 33-cell raft guard matrix: 11 replicated tables × INSERT/UPDATE/DELETE.
//! Each cell is denied outside apply with the exact SQLite error, then succeeds
//! inside apply. Exceptions (admin user, session/admin key, admin permission)
//! are exercised separately and must always be allowed.

use serde_json::{Value, json};

use oss_server::db::Db;

use super::{error_json, exec};
use crate::golden;

const TABLES: [&str; 11] = [
    "users",
    "access_keys",
    "permissions",
    "buckets",
    "bucket_policies",
    "lifecycle_rules",
    "event_configs",
    "replication_rules",
    "access_points",
    "olap_configs",
    "inventory_configs",
];

const OPS: [&str; 3] = ["insert", "update", "delete"];

const DENIED: &str = "raft: control write outside apply";

pub async fn capture() {
    let mut cells = Vec::new();
    for table in TABLES {
        for op in OPS {
            let dir = tempfile::tempdir().unwrap();
            let url = format!("sqlite://{}?mode=rwc", dir.path().join("raft.db").display());
            let db = Db::connect_raft(&url).await.unwrap();
            seed(&db, table, op).await;

            let err = attempt(&db, table, op)
                .await
                .expect_err("write outside apply must be denied");
            let denied = error_json(&err);
            assert!(
                denied["error"].as_str().unwrap().contains(DENIED),
                "{table}.{op}: {denied}"
            );

            set_apply(&db, true).await;
            attempt(&db, table, op)
                .await
                .unwrap_or_else(|e| panic!("{table}.{op} inside apply: {e}"));
            set_apply(&db, false).await;

            cells.push(json!({
                "table": table,
                "op": op,
                "outside_apply": denied,
                "inside_apply": "allowed",
            }));
        }
    }
    assert_eq!(cells.len(), 33);
    golden::write_serde(
        "guards/matrix.json",
        &json!({
            "denied_error": DENIED,
            "cases": cells,
            "exceptions": exceptions().await,
        }),
    );
}

async fn set_apply(db: &Db, active: bool) {
    sqlx::query("UPDATE raft_apply_ctx SET active = ? WHERE id = 1")
        .bind(active as i64)
        .execute(db.pool())
        .await
        .unwrap();
}

async fn seed(db: &Db, table: &str, op: &str) {
    set_apply(db, true).await;
    let pool = db.pool();
    exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (1, 'guard-root', 'h', 1, 'active', 1)").await;
    exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (10, 'guard-user', 'h', 0, 'active', 1)").await;
    exec(
        pool,
        "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('guard-b', 1, 1)",
    )
    .await;
    exec(pool, "INSERT INTO access_points (name, bucket, owner_id, created_at, updated_at) VALUES ('guard-ap', 'guard-b', 1, 1, 1)").await;
    match table {
        "users" if op != "insert" => {
            exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (20, 'guard-target', 'h', 0, 'active', 1)").await;
        }
        "access_keys" if op != "insert" => {
            exec(pool, "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKGUARD', 'SKGUARD', 10, 'active', NULL, 1, 'long-term')").await;
        }
        "permissions" if op != "insert" => {
            exec(pool, "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (10, 'guard-b', 1, 0, 0)").await;
        }
        "buckets" if op != "insert" => {
            exec(pool, "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('guard-target-b', 1, 1)").await;
        }
        "bucket_policies" if op != "insert" => {
            exec(pool, "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES ('guard-b', '{}', 1)").await;
        }
        "lifecycle_rules" if op != "insert" => {
            exec(pool, "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at, rule_id) VALUES ('guard-b', 'logs/', 1, 1, 1, 'r1')").await;
        }
        "event_configs" if op != "insert" => {
            exec(pool, "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES ('guard-b', '[]', 1)").await;
        }
        "replication_rules" if op != "insert" => {
            exec(pool, "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES ('guard-b', '[]', 1)").await;
        }
        "access_points" if op != "insert" => {
            exec(pool, "INSERT INTO access_points (name, bucket, owner_id, created_at, updated_at) VALUES ('guard-target-ap', 'guard-b', 1, 1, 1)").await;
        }
        "olap_configs" if op != "insert" => {
            exec(pool, "INSERT INTO olap_configs (access_point, kind, endpoint, updated_at) VALUES ('guard-ap', 'http', 'http://127.0.0.1:9', 1)").await;
        }
        "inventory_configs" if op != "insert" => {
            exec(pool, "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) VALUES ('guard-b', 'inv', 1, '{}', 1)").await;
        }
        _ => {}
    }
    set_apply(db, false).await;
}

async fn attempt(db: &Db, table: &str, op: &str) -> Result<(), sqlx::Error> {
    let sql = sql_for(table, op);
    sqlx::query(sql).execute(db.pool()).await?;
    Ok(())
}

fn sql_for(table: &str, op: &str) -> &'static str {
    match (table, op) {
        ("users", "insert") => {
            "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (21, 'guard-new', 'h', 0, 'active', 1)"
        }
        ("users", "update") => "UPDATE users SET status = 'disabled' WHERE id = 20",
        ("users", "delete") => "DELETE FROM users WHERE id = 20",
        ("access_keys", "insert") => {
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKGUARD-NEW', 'SK', 10, 'active', NULL, 1, 'long-term')"
        }
        ("access_keys", "update") => {
            "UPDATE access_keys SET status = 'disabled' WHERE access_key = 'AKGUARD'"
        }
        ("access_keys", "delete") => "DELETE FROM access_keys WHERE access_key = 'AKGUARD'",
        ("permissions", "insert") => {
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (10, 'guard-b', 1, 0, 0)"
        }
        ("permissions", "update") => {
            "UPDATE permissions SET can_read = 0 WHERE user_id = 10 AND bucket = 'guard-b'"
        }
        ("permissions", "delete") => {
            "DELETE FROM permissions WHERE user_id = 10 AND bucket = 'guard-b'"
        }
        ("buckets", "insert") => {
            "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('guard-target-b', 1, 1)"
        }
        ("buckets", "update") => {
            "UPDATE buckets SET versioning = 'Enabled' WHERE name = 'guard-target-b'"
        }
        ("buckets", "delete") => "DELETE FROM buckets WHERE name = 'guard-target-b'",
        ("bucket_policies", "insert") => {
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES ('guard-b', '{}', 1)"
        }
        ("bucket_policies", "update") => {
            "UPDATE bucket_policies SET policy_json = '{\"v\":2}' WHERE bucket = 'guard-b'"
        }
        ("bucket_policies", "delete") => "DELETE FROM bucket_policies WHERE bucket = 'guard-b'",
        ("lifecycle_rules", "insert") => {
            "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at, rule_id) VALUES ('guard-b', 'logs/', 1, 1, 1, 'r1')"
        }
        ("lifecycle_rules", "update") => {
            "UPDATE lifecycle_rules SET expire_days = 2 WHERE bucket = 'guard-b'"
        }
        ("lifecycle_rules", "delete") => "DELETE FROM lifecycle_rules WHERE bucket = 'guard-b'",
        ("event_configs", "insert") => {
            "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES ('guard-b', '[]', 1)"
        }
        ("event_configs", "update") => {
            "UPDATE event_configs SET config_json = '[{\"v\":2}]' WHERE bucket = 'guard-b'"
        }
        ("event_configs", "delete") => "DELETE FROM event_configs WHERE bucket = 'guard-b'",
        ("replication_rules", "insert") => {
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES ('guard-b', '[]', 1)"
        }
        ("replication_rules", "update") => {
            "UPDATE replication_rules SET rules_json = '[]' WHERE bucket = 'guard-b'"
        }
        ("replication_rules", "delete") => "DELETE FROM replication_rules WHERE bucket = 'guard-b'",
        ("access_points", "insert") => {
            "INSERT INTO access_points (name, bucket, owner_id, created_at, updated_at) VALUES ('guard-target-ap', 'guard-b', 1, 1, 1)"
        }
        ("access_points", "update") => {
            "UPDATE access_points SET updated_at = 2 WHERE name = 'guard-target-ap'"
        }
        ("access_points", "delete") => "DELETE FROM access_points WHERE name = 'guard-target-ap'",
        ("olap_configs", "insert") => {
            "INSERT INTO olap_configs (access_point, kind, endpoint, updated_at) VALUES ('guard-ap', 'http', 'http://127.0.0.1:9', 1)"
        }
        ("olap_configs", "update") => {
            "UPDATE olap_configs SET updated_at = 2 WHERE access_point = 'guard-ap'"
        }
        ("olap_configs", "delete") => "DELETE FROM olap_configs WHERE access_point = 'guard-ap'",
        ("inventory_configs", "insert") => {
            "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) VALUES ('guard-b', 'inv', 1, '{}', 1)"
        }
        ("inventory_configs", "update") => {
            "UPDATE inventory_configs SET updated_at = 2 WHERE bucket = 'guard-b'"
        }
        ("inventory_configs", "delete") => "DELETE FROM inventory_configs WHERE bucket = 'guard-b'",
        other => panic!("missing guard SQL for {other:?}"),
    }
}

async fn exceptions() -> Value {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("raft.db").display());
    let db = Db::connect_raft(&url).await.unwrap();
    set_apply(&db, true).await;
    let pool = db.pool();
    exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (1, 'guard-root', 'h', 1, 'active', 1)").await;
    exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (10, 'guard-user', 'h', 0, 'active', 1)").await;
    exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (11, 'guard-admin', 'h', 1, 'active', 1)").await;
    exec(
        pool,
        "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('ex-b', 1, 1)",
    )
    .await;
    exec(pool, "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKSESS-EX', 'SK', 10, 'active', NULL, 1, 'session')").await;
    exec(pool, "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKADMIN-EX', 'SK', 1, 'active', NULL, 1, 'long-term')").await;
    exec(pool, "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (1, 'ex-b', 1, 0, 0)").await;
    set_apply(&db, false).await;

    let mut actions = Vec::new();
    for (table, action, sql) in [
        (
            "users",
            "insert admin user",
            "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (12, 'guard-admin-2', 'h', 1, 'active', 1)",
        ),
        (
            "users",
            "update admin user",
            "UPDATE users SET status = 'disabled' WHERE id = 12",
        ),
        (
            "users",
            "delete admin user",
            "DELETE FROM users WHERE id = 12",
        ),
        (
            "access_keys",
            "insert session key",
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKSESS-EX2', 'SK', 10, 'active', NULL, 1, 'session')",
        ),
        (
            "access_keys",
            "update session key",
            "UPDATE access_keys SET status = 'disabled' WHERE access_key = 'AKSESS-EX2'",
        ),
        (
            "access_keys",
            "delete session key",
            "DELETE FROM access_keys WHERE access_key = 'AKSESS-EX2'",
        ),
        (
            "access_keys",
            "insert admin key",
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKADMIN-EX2', 'SK', 1, 'active', NULL, 1, 'long-term')",
        ),
        (
            "access_keys",
            "update admin key",
            "UPDATE access_keys SET status = 'disabled' WHERE access_key = 'AKADMIN-EX2'",
        ),
        (
            "access_keys",
            "delete admin key",
            "DELETE FROM access_keys WHERE access_key = 'AKADMIN-EX2'",
        ),
        (
            "permissions",
            "insert admin permission",
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (1, 'ex-b2', 0, 1, 0)",
        ),
        (
            "permissions",
            "update admin permission",
            "UPDATE permissions SET can_write = 0 WHERE user_id = 1 AND bucket = 'ex-b'",
        ),
        (
            "permissions",
            "delete admin permission",
            "DELETE FROM permissions WHERE user_id = 1 AND bucket = 'ex-b'",
        ),
    ] {
        sqlx::query(sql)
            .execute(db.pool())
            .await
            .unwrap_or_else(|e| panic!("{table}: {action}: {e}"));
        actions.push(action);
    }
    json!({"all_allowed": true, "actions": actions})
}
