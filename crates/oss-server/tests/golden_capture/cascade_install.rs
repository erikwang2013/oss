//! Bucket-delete cascade order and snapshot-install predicates, both through
//! production paths: `ControlCommand::BucketDelete` and the raft state-machine
//! `install_snapshot`.

use std::io::Cursor;

use openraft::storage::RaftStateMachine;
use openraft::{SnapshotMeta, StoredMembership};
use serde_json::{Value, json};
use sqlx::{Column, Row, SqlitePool, TypeInfo, ValueRef};

use oss_server::db::Db;
use oss_server::raft::data;
use oss_server::raft::store::SqliteStateMachine;
use oss_server::raft::{ControlCommand, ControlResponse, TypeConfig};

use super::fixtures::{self, BUCKET, USER};
use super::{exec, fix_site_id, peer, site_config};
use crate::common::state_with_site;
use crate::golden;

/// 13 dependent tables in the order production deletes them; `buckets` last.
const CASCADE_ORDER: [&str; 14] = [
    "olap_configs",
    "access_points",
    "inventory_runs",
    "inventory_configs",
    "inventory_cursors",
    "inventory_incremental",
    "tier_restores",
    "lifecycle_rules",
    "lifecycle_cursors",
    "permissions",
    "bucket_policies",
    "event_configs",
    "replication_rules",
    "buckets",
];

pub async fn capture() {
    bucket_delete_order().await;
    install_predicates().await;
}

async fn bucket_delete_order() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_site(dir.path(), Some(site_config())).await;
    fix_site_id(st.db.pool()).await;
    exec(
        st.db.pool(),
        "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
         VALUES (1, 'golden-root', 'root-hash', 1, 'active', 100)",
    )
    .await;
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
         VALUES (10, ?, ?, 0, 'active', 111)",
    )
    .bind(USER)
    .bind(fixtures::PASSWORD_HASH)
    .execute(st.db.pool())
    .await
    .unwrap();
    for bucket in ["b1", "b2"] {
        seed_bucket_rows(st.db.pool(), bucket).await;
    }
    // Capture-only AFTER DELETE triggers record the exact production order.
    sqlx::raw_sql(
        "CREATE TABLE cascade_trace (seq INTEGER PRIMARY KEY AUTOINCREMENT, table_name TEXT NOT NULL);
         CREATE TRIGGER trace_olap_configs_del AFTER DELETE ON olap_configs BEGIN INSERT INTO cascade_trace(table_name) VALUES ('olap_configs'); END;
         CREATE TRIGGER trace_access_points_del AFTER DELETE ON access_points BEGIN INSERT INTO cascade_trace(table_name) VALUES ('access_points'); END;
         CREATE TRIGGER trace_inventory_runs_del AFTER DELETE ON inventory_runs BEGIN INSERT INTO cascade_trace(table_name) VALUES ('inventory_runs'); END;
         CREATE TRIGGER trace_inventory_configs_del AFTER DELETE ON inventory_configs BEGIN INSERT INTO cascade_trace(table_name) VALUES ('inventory_configs'); END;
         CREATE TRIGGER trace_inventory_cursors_del AFTER DELETE ON inventory_cursors BEGIN INSERT INTO cascade_trace(table_name) VALUES ('inventory_cursors'); END;
         CREATE TRIGGER trace_inventory_incremental_del AFTER DELETE ON inventory_incremental BEGIN INSERT INTO cascade_trace(table_name) VALUES ('inventory_incremental'); END;
         CREATE TRIGGER trace_tier_restores_del AFTER DELETE ON tier_restores BEGIN INSERT INTO cascade_trace(table_name) VALUES ('tier_restores'); END;
         CREATE TRIGGER trace_lifecycle_rules_del AFTER DELETE ON lifecycle_rules BEGIN INSERT INTO cascade_trace(table_name) VALUES ('lifecycle_rules'); END;
         CREATE TRIGGER trace_lifecycle_cursors_del AFTER DELETE ON lifecycle_cursors BEGIN INSERT INTO cascade_trace(table_name) VALUES ('lifecycle_cursors'); END;
         CREATE TRIGGER trace_permissions_del AFTER DELETE ON permissions BEGIN INSERT INTO cascade_trace(table_name) VALUES ('permissions'); END;
         CREATE TRIGGER trace_bucket_policies_del AFTER DELETE ON bucket_policies BEGIN INSERT INTO cascade_trace(table_name) VALUES ('bucket_policies'); END;
         CREATE TRIGGER trace_event_configs_del AFTER DELETE ON event_configs BEGIN INSERT INTO cascade_trace(table_name) VALUES ('event_configs'); END;
         CREATE TRIGGER trace_replication_rules_del AFTER DELETE ON replication_rules BEGIN INSERT INTO cascade_trace(table_name) VALUES ('replication_rules'); END;
         CREATE TRIGGER trace_buckets_del AFTER DELETE ON buckets BEGIN INSERT INTO cascade_trace(table_name) VALUES ('buckets'); END;",
    )
    .execute(st.db.pool())
    .await
    .unwrap();
    st.db.insert_site(&peer()).await.unwrap();

    let response = st
        .control
        .write(ControlCommand::BucketDelete {
            name: "b1".to_string(),
        })
        .await
        .unwrap();
    assert!(matches!(response, ControlResponse::Ok));

    let trace: Vec<String> =
        sqlx::query_scalar("SELECT table_name FROM cascade_trace ORDER BY seq")
            .fetch_all(st.db.pool())
            .await
            .unwrap();
    assert_eq!(trace, CASCADE_ORDER, "cascade delete order");

    let mut b1 = serde_json::Map::new();
    let mut b2 = serde_json::Map::new();
    for table in CASCADE_ORDER[2..13].iter() {
        b1.insert(
            (*table).into(),
            json!(count_bucket(st.db.pool(), table, "b1").await),
        );
        b2.insert(
            (*table).into(),
            json!(count_bucket(st.db.pool(), table, "b2").await),
        );
    }
    let b1_ap = count_where(
        st.db.pool(),
        "SELECT COUNT(*) FROM access_points WHERE bucket = 'b1'",
    )
    .await;
    let b2_ap = count_where(
        st.db.pool(),
        "SELECT COUNT(*) FROM access_points WHERE bucket = 'b2'",
    )
    .await;
    b1.insert("access_points".into(), json!(b1_ap));
    b2.insert("access_points".into(), json!(b2_ap));
    let b1_olap = count_where(
        st.db.pool(),
        "SELECT COUNT(*) FROM olap_configs WHERE access_point IN (SELECT name FROM access_points WHERE bucket = 'b1')",
    )
    .await;
    let b2_olap = count_where(
        st.db.pool(),
        "SELECT COUNT(*) FROM olap_configs WHERE access_point IN (SELECT name FROM access_points WHERE bucket = 'b2')",
    )
    .await;
    b1.insert("olap_configs".into(), json!(b1_olap));
    b2.insert("olap_configs".into(), json!(b2_olap));
    assert!(b1.values().all(|v| v.as_i64() == Some(0)));
    assert!(b2.values().all(|v| v.as_i64() == Some(1)));

    let buckets: Vec<String> = sqlx::query_scalar("SELECT name FROM buckets ORDER BY name")
        .fetch_all(st.db.pool())
        .await
        .unwrap();
    assert_eq!(buckets, ["b2"]);
    let changelog: Vec<String> =
        sqlx::query("SELECT entity || '.' || op FROM site_changelog ORDER BY seq")
            .fetch_all(st.db.pool())
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<String, _>(0))
            .collect();
    assert_eq!(
        changelog,
        ["permission.delete", "bucket_policy.delete", "bucket.delete"]
    );

    golden::write_serde(
        "cascade/bucket_delete_order.json",
        &json!({
            "deleted_bucket": "b1",
            "response": "Ok",
            "order": trace,
            "b1_remaining": b1,
            "b2_remaining": b2,
            "buckets_remaining": buckets,
            "site_changelog": changelog,
        }),
    );
}

async fn seed_bucket_rows(pool: &SqlitePool, bucket: &str) {
    let ap = format!("ap-{bucket}");
    for sql in [
        "INSERT INTO buckets (name, owner_id, creation_date) VALUES (?, 1, 1)",
        "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) VALUES (?, 'inv', 1, '{}', 1)",
        "INSERT INTO inventory_runs (bucket, config_id, run_key, started_at, state, created_at) VALUES (?, 'inv', 'r1', 1, 'done', 1)",
        "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, updated_at) VALUES (?, 'inv', 'c1', 1, 1)",
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) VALUES (?, 'inv', 1, 1)",
        "INSERT INTO tier_restores (bucket, key, version_id, expires_at, state, created_at) VALUES (?, 'k', 'v', 1, 'done', 1)",
        "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at, rule_id) VALUES (?, 'logs/', 1, 1, 1, 'r1')",
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at) VALUES (?, 'r1', 'expire', 'o1', 1)",
        "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (10, ?, 1, 1, 1)",
        "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, '{}', 1)",
        "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, '[]', 1)",
        "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, '[]', 1)",
    ] {
        sqlx::query(sql).bind(bucket).execute(pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO access_points (name, bucket, owner_id, created_at, updated_at) \
         VALUES (?, ?, 1, 1, 1)",
    )
    .bind(&ap)
    .bind(bucket)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO olap_configs (access_point, kind, endpoint, updated_at) \
         VALUES (?, 'http', 'http://127.0.0.1:9', 1)",
    )
    .bind(&ap)
    .execute(pool)
    .await
    .unwrap();
}

async fn count_bucket(pool: &SqlitePool, table: &str, bucket: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {table} WHERE bucket = ?"
    )))
    .bind(bucket)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn count_where(pool: &SqlitePool, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

async fn install_predicates() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("install.db").display()
    );
    let db = Db::connect_raft(&url).await.unwrap();
    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(db.pool())
        .await
        .unwrap();
    exec(db.pool(), "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (1, 'golden-admin', 'admin-hash', 1, 'active', 1)").await;
    exec(db.pool(), "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (10, 'old-user', 'old-hash', 0, 'active', 1)").await;
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date, set_id) VALUES (?, 10, 1, 7)")
        .bind(BUCKET)
        .execute(db.pool())
        .await
        .unwrap();
    exec(
        db.pool(),
        "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('target-only', 10, 1)",
    )
    .await;
    exec(db.pool(), "INSERT INTO access_keys (access_key, secret_key, user_id, status, created_at, kind) VALUES ('AK-ADMIN-KEEP', 'SK', 1, 'active', 1, 'long-term')").await;
    exec(db.pool(), "INSERT INTO access_keys (access_key, secret_key, user_id, status, created_at, kind) VALUES ('AK-OLD-DROP', 'SK', 10, 'active', 1, 'long-term')").await;
    exec(db.pool(), "INSERT INTO access_keys (access_key, secret_key, user_id, status, created_at, kind) VALUES ('AK-SESSION-DROP', 'SK', 10, 'active', 1, 'session')").await;
    sqlx::query("INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (1, ?, 1, 1, 1)")
        .bind(BUCKET)
        .execute(db.pool())
        .await
        .unwrap();
    exec(db.pool(), "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (10, 'target-only', 1, 0, 0)").await;
    exec(db.pool(), "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, original_name, owner_id, created_at) VALUES ('u/old', 'target-only', 'k', 'file', 1, 'e', 'n', 10, 1)").await;
    exec(db.pool(), "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, original_name, owner_id, created_at) VALUES ('u/admin', 'target-only', 'k', 'file', 1, 'e', 'n', 1, 1)").await;
    sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
        .execute(db.pool())
        .await
        .unwrap();

    let bytes = data::encode_snapshot_at(&fixtures::level_one(), 1).unwrap();
    let mut sm =
        SqliteStateMachine::open_with_pool(db.pool().clone(), 4 * 1024 * 1024, false, None, 1)
            .await
            .unwrap();
    let meta = SnapshotMeta {
        last_log_id: None,
        last_membership: StoredMembership::default(),
        snapshot_id: "golden-install-predicates".into(),
    };
    RaftStateMachine::<TypeConfig>::install_snapshot(&mut sm, &meta, Box::new(Cursor::new(bytes)))
        .await
        .unwrap();

    let users: Vec<Value> = rows(
        db.pool(),
        "SELECT id, username, is_admin FROM users ORDER BY id",
    )
    .await;
    let admin_preserved = users
        .iter()
        .any(|u| u["username"] == "golden-admin" && u["is_admin"] == 1);
    let old_user_gone = !users.iter().any(|u| u["username"] == "old-user");
    let installed_user = users.iter().find(|u| u["username"] == USER).cloned();
    assert!(
        admin_preserved && old_user_gone && installed_user.is_some(),
        "{users:?}"
    );

    let bucket: Value = rows(
        db.pool(),
        "SELECT b.name, b.set_id, u.username AS owner FROM buckets b \
         LEFT JOIN users u ON u.id = b.owner_id ORDER BY b.name",
    )
    .await
    .into_iter()
    .find(|b| b["name"] == BUCKET)
    .unwrap();
    assert_eq!(bucket["set_id"], 7);
    assert_eq!(bucket["owner"], USER);
    let target_only: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name = 'target-only'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(target_only, 0);

    let keys: Vec<Value> = rows(
        db.pool(),
        "SELECT access_key, user_id, kind FROM access_keys ORDER BY access_key",
    )
    .await;
    let key_names: Vec<&str> = keys
        .iter()
        .map(|k| k["access_key"].as_str().unwrap())
        .collect();
    assert!(!key_names.contains(&"AK-OLD-DROP"));
    assert!(!key_names.contains(&"AK-SESSION-DROP"));
    assert!(key_names.contains(&"AK-ADMIN-KEEP"));
    assert!(key_names.contains(&fixtures::AK));

    let perms: Vec<Value> = rows(
        db.pool(),
        "SELECT user_id, bucket, can_read, can_write, can_delete FROM permissions ORDER BY user_id, bucket",
    )
    .await;
    assert!(!perms.iter().any(|p| p["user_id"] == 1));
    assert!(
        perms
            .iter()
            .any(|p| p["bucket"] == BUCKET && p["user_id"] != 1)
    );

    let uploads: Vec<Value> = rows(
        db.pool(),
        "SELECT saved_path, owner_id FROM uploads ORDER BY saved_path",
    )
    .await;
    assert_eq!(uploads[0]["saved_path"], "u/admin");
    assert_eq!(uploads[0]["owner_id"], 1);
    assert!(uploads[1]["owner_id"].is_null());

    golden::write_serde(
        "cascade/install_predicates.json",
        &json!({
            "snapshot": "OSSCTL1 level 1",
            "users": users,
            "bucket": bucket,
            "target_only_buckets": target_only,
            "access_keys": keys,
            "permissions": perms,
            "uploads": uploads,
        }),
    );
}

async fn rows(pool: &SqlitePool, sql: &'static str) -> Vec<serde_json::Value> {
    let rows = sqlx::query(sql).fetch_all(pool).await.unwrap();
    rows.iter()
        .map(|r| {
            let mut obj = serde_json::Map::new();
            for col in r.columns() {
                let v: sqlx::sqlite::SqliteValueRef = r.try_get_raw(col.name()).unwrap();
                let value = if v.is_null() {
                    serde_json::Value::Null
                } else {
                    match v.type_info().name() {
                        "INTEGER" => json!(r.get::<i64, _>(col.name())),
                        "REAL" => json!(r.get::<f64, _>(col.name())),
                        _ => json!(r.get::<String, _>(col.name())),
                    }
                };
                obj.insert(col.name().to_string(), value);
            }
            serde_json::Value::Object(obj)
        })
        .collect()
}
