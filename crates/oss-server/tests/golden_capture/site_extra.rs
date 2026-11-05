//! Site-trigger extras: admin/session exclusions, apply suppression, raft-mode
//! inactivity, and the ordering/payloads of the user-delete cascade.

use serde_json::{Value, json};
use sqlx::{Row, SqlitePool};

use oss_server::db::Db;
use oss_server::state::AppState;

use super::fixtures::{self, USER};
use super::{NORMALIZED_TS, exec, fix_site_id, peer, site_config};
use crate::common::state_with_site;
use crate::golden;

pub async fn capture() {
    exclusions().await;
    apply_suppression().await;
    raft_mode_inactive().await;
    user_delete_cascade().await;
}

async fn base_state(dir: &std::path::Path) -> AppState {
    let st = state_with_site(dir, Some(site_config())).await;
    fix_site_id(st.db.pool()).await;
    let pool = st.db.pool();
    exec(
        pool,
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
    .execute(pool)
    .await
    .unwrap();
    st
}

async fn exclusions() {
    let dir = tempfile::tempdir().unwrap();
    let st = base_state(dir.path()).await;
    st.db.insert_site(&peer()).await.unwrap();
    let pool = st.db.pool();
    let mut actions = Vec::new();

    // users: admin rows are node-local.
    exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (11, 'admin-two', 'h', 1, 'active', 112)").await;
    actions.push("insert admin user");
    exec(pool, "UPDATE users SET status = 'disabled' WHERE id = 11").await;
    actions.push("update admin user");
    exec(pool, "DELETE FROM users WHERE id = 11").await;
    actions.push("delete admin user");

    // access_keys: session keys and admin-owned keys are local.
    exec(pool, "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKSESS-GOLDEN', 'SK-SESS', 10, 'active', 999, 556, 'session')").await;
    actions.push("insert session access key");
    exec(
        pool,
        "UPDATE access_keys SET status = 'disabled' WHERE access_key = 'AKSESS-GOLDEN'",
    )
    .await;
    actions.push("update session access key");
    exec(
        pool,
        "DELETE FROM access_keys WHERE access_key = 'AKSESS-GOLDEN'",
    )
    .await;
    actions.push("delete session access key");

    exec(pool, "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKADMIN-GOLDEN', 'SK-ADMIN', 1, 'active', NULL, 557, 'long-term')").await;
    actions.push("insert admin-owned access key");
    exec(
        pool,
        "UPDATE access_keys SET status = 'disabled' WHERE access_key = 'AKADMIN-GOLDEN'",
    )
    .await;
    actions.push("update admin-owned access key");
    exec(
        pool,
        "DELETE FROM access_keys WHERE access_key = 'AKADMIN-GOLDEN'",
    )
    .await;
    actions.push("delete admin-owned access key");

    // permissions: rows targeting an admin are local.
    exec(pool, "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (1, 'ex-b', 1, 0, 0)").await;
    actions.push("insert admin-target permission");
    exec(
        pool,
        "UPDATE permissions SET can_read = 0 WHERE user_id = 1 AND bucket = 'ex-b'",
    )
    .await;
    actions.push("update admin-target permission");
    exec(
        pool,
        "DELETE FROM permissions WHERE user_id = 1 AND bucket = 'ex-b'",
    )
    .await;
    actions.push("delete admin-target permission");

    let log = count(pool, "site_changelog").await;
    let clock = count(pool, "site_entity_clock").await;
    assert_eq!((log, clock), (0, 0));
    golden::write_serde(
        "site_triggers/exclusions.json",
        &json!({"actions": actions, "changelog_rows": log, "clock_rows": clock}),
    );
}

async fn apply_suppression() {
    let dir = tempfile::tempdir().unwrap();
    let st = base_state(dir.path()).await;
    let pool = st.db.pool();
    exec(
        pool,
        "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('ex-b', 10, 1)",
    )
    .await;
    st.db.insert_site(&peer()).await.unwrap();
    st.db.set_apply_ctx(true).await.unwrap();

    let mut actions = Vec::new();
    exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (11, 'suppressed-user', 'h', 0, 'active', 113)").await;
    actions.push("insert user");
    exec(
        pool,
        "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('ex-b2', 10, 2)",
    )
    .await;
    actions.push("insert bucket");
    exec(
        pool,
        "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES ('ex-b', '{}', 1)",
    )
    .await;
    actions.push("insert policy");
    exec(pool, "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (10, 'ex-b', 1, 1, 1)").await;
    actions.push("insert permission");
    exec(pool, "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKSUP-GOLDEN', 'SK-SUP', 10, 'active', NULL, 558, 'long-term')").await;
    actions.push("insert access key");

    let suppressed_log = count(pool, "site_changelog").await;
    let suppressed_clock = count(pool, "site_entity_clock").await;
    assert_eq!((suppressed_log, suppressed_clock), (0, 0));

    st.db.set_apply_ctx(false).await.unwrap();
    exec(pool, "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) VALUES (12, 'after-apply', 'h', 0, 'active', 114)").await;
    let after_reset_log = count(pool, "site_changelog").await;
    assert_eq!(after_reset_log, 1);
    golden::write_serde(
        "site_triggers/apply_suppression.json",
        &json!({
            "actions": actions,
            "suppressed_changelog_rows": suppressed_log,
            "suppressed_clock_rows": suppressed_clock,
            "after_reset_changelog_rows": after_reset_log,
        }),
    );
}

async fn raft_mode_inactive() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("raft.db").display());
    let db = Db::connect_raft(&url).await.unwrap();
    let site_triggers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'site_%'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let raft_guards: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'raft_guard_%'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(site_triggers, 0);
    assert_eq!(raft_guards, 33);

    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(db.pool())
        .await
        .unwrap();
    exec(
        db.pool(),
        "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
         VALUES (10, 'raft-user', 'h', 0, 'active', 1)",
    )
    .await;
    exec(
        db.pool(),
        "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('raft-b', 10, 1)",
    )
    .await;
    exec(
        db.pool(),
        "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES ('raft-b', '{}', 1)",
    )
    .await;
    exec(
        db.pool(),
        "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
         VALUES (10, 'raft-b', 1, 1, 1)",
    )
    .await;
    exec(
        db.pool(),
        "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) \
         VALUES ('AKRAFT-GOLDEN', 'SK-RAFT', 10, 'active', NULL, 1, 'long-term')",
    )
    .await;
    sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
        .execute(db.pool())
        .await
        .unwrap();

    let log = count(db.pool(), "site_changelog").await;
    let clock = count(db.pool(), "site_entity_clock").await;
    assert_eq!((log, clock), (0, 0));
    golden::write_serde(
        "site_triggers/raft_mode_inactive.json",
        &json!({
            "site_trigger_count": site_triggers,
            "raft_guard_trigger_count": raft_guards,
            "changelog_rows": log,
            "clock_rows": clock,
        }),
    );
}

async fn user_delete_cascade() {
    let dir = tempfile::tempdir().unwrap();
    let st = base_state(dir.path()).await;
    let pool = st.db.pool();
    exec(
        pool,
        "INSERT INTO buckets (name, owner_id, creation_date) VALUES ('cascade-b', 10, 1)",
    )
    .await;
    exec(pool, "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (10, 'cascade-b', 1, 1, 1)").await;
    exec(pool, "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) VALUES ('AKCASCADE-GOLDEN', 'SK-CAS', 10, 'active', NULL, 559, 'long-term')").await;
    exec(pool, "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, original_name, owner_id, created_at) VALUES ('p/q/r', 'cascade-b', 'k', 'file', 1, 'e', 'n', 10, 2)").await;
    st.db.insert_site(&peer()).await.unwrap();

    st.db.delete_user(10).await.unwrap();
    let log = read_log(pool).await;
    let clock = read_clock(pool).await;
    let order: Vec<String> = log
        .iter()
        .map(|r| {
            format!(
                "{}.{}",
                r["entity"].as_str().unwrap(),
                r["op"].as_str().unwrap()
            )
        })
        .collect();
    assert_eq!(
        order,
        [
            "access_key.delete",
            "permission.delete",
            "bucket.upsert",
            "user.delete"
        ]
    );

    let remaining_user: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id = 10")
        .fetch_one(pool)
        .await
        .unwrap();
    let remaining_ak: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM access_keys WHERE access_key = 'AKCASCADE-GOLDEN'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let remaining_perm: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM permissions WHERE user_id = 10")
            .fetch_one(pool)
            .await
            .unwrap();
    let bucket_owner: Option<i64> =
        sqlx::query_scalar("SELECT owner_id FROM buckets WHERE name = 'cascade-b'")
            .fetch_one(pool)
            .await
            .unwrap();
    let upload_owner: Option<i64> =
        sqlx::query_scalar("SELECT owner_id FROM uploads WHERE saved_path = 'p/q/r'")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        (
            remaining_user,
            remaining_ak,
            remaining_perm,
            bucket_owner,
            upload_owner
        ),
        (0, 0, 0, None, None)
    );
    golden::write_serde(
        "site_triggers/user_delete_cascade.json",
        &json!({
            "order": order,
            "changelog": log,
            "clock": clock,
            "final": {
                "user_rows": remaining_user,
                "access_key_rows": remaining_ak,
                "permission_rows": remaining_perm,
                "bucket_owner_id": bucket_owner,
                "upload_owner_id": upload_owner,
            },
        }),
    );
}

async fn read_log(pool: &SqlitePool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT seq, origin_site_id, entity, entity_id, op, payload, priority, ts, created_at \
         FROM site_changelog ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.iter()
        .map(|r| {
            json!({
                "seq": r.get::<i64, _>("seq"),
                "origin_site_id": r.get::<String, _>("origin_site_id"),
                "entity": r.get::<String, _>("entity"),
                "entity_id": r.get::<String, _>("entity_id"),
                "op": r.get::<String, _>("op"),
                "payload": r.get::<String, _>("payload"),
                "priority": r.get::<i64, _>("priority"),
                "ts": NORMALIZED_TS,
                "created_at": NORMALIZED_TS,
            })
        })
        .collect()
}

async fn read_clock(pool: &SqlitePool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT entity, entity_id, ts, priority, origin_site_id, seq \
         FROM site_entity_clock ORDER BY entity, entity_id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.iter()
        .map(|r| {
            json!({
                "entity": r.get::<String, _>("entity"),
                "entity_id": r.get::<String, _>("entity_id"),
                "ts": NORMALIZED_TS,
                "priority": r.get::<i64, _>("priority"),
                "origin_site_id": r.get::<String, _>("origin_site_id"),
                "seq": r.get::<i64, _>("seq"),
            })
        })
        .collect()
}

async fn count(pool: &SqlitePool, table: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(pool)
        .await
        .unwrap()
}
