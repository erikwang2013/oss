//! All 15 site-trigger mutations, each exercised twice: a future clock row
//! makes the gate reject before any mutation, then the clock is removed and the
//! same mutation logs one payload + clock row. That is 30 trigger cases.
//!
//! Only `ts`/`created_at` (SQLite `strftime('%s','now')`) are normalized to 0;
//! everything else, including payload escaping, is captured byte-for-byte.

use serde_json::json;
use sqlx::{Row, SqlitePool};

use oss_server::state::AppState;

use super::fixtures::{self, BUCKET, USER};
use super::{NORMALIZED_TS, error_json, fix_site_id, now_secs, peer, site_config};
use crate::common::state_with_site;
use crate::golden;

#[derive(Clone, Copy)]
enum Entity {
    Bucket,
    Policy,
    Permission,
    User,
    AccessKey,
}

#[derive(Clone, Copy)]
enum Op {
    Insert,
    Update,
    Delete,
}

const CASES: [(&str, Entity, Op); 15] = [
    ("bucket_ins", Entity::Bucket, Op::Insert),
    ("bucket_upd", Entity::Bucket, Op::Update),
    ("bucket_del", Entity::Bucket, Op::Delete),
    ("policy_ins", Entity::Policy, Op::Insert),
    ("policy_upd", Entity::Policy, Op::Update),
    ("policy_del", Entity::Policy, Op::Delete),
    ("perm_ins", Entity::Permission, Op::Insert),
    ("perm_upd", Entity::Permission, Op::Update),
    ("perm_del", Entity::Permission, Op::Delete),
    ("user_ins", Entity::User, Op::Insert),
    ("user_upd", Entity::User, Op::Update),
    ("user_del", Entity::User, Op::Delete),
    ("ak_ins", Entity::AccessKey, Op::Insert),
    ("ak_upd", Entity::AccessKey, Op::Update),
    ("ak_del", Entity::AccessKey, Op::Delete),
];

pub async fn capture() {
    for (case, entity, op) in CASES {
        capture_case(case, entity, op).await;
    }
}

async fn capture_case(case: &str, entity: Entity, op: Op) {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_site(dir.path(), Some(site_config())).await;
    fix_site_id(st.db.pool()).await;
    seed(&st, entity, op).await;
    st.db.insert_site(&peer()).await.unwrap();

    insert_future_clock(st.db.pool(), entity, op).await;
    let err = mutate(st.db.pool(), entity, op)
        .await
        .expect_err("future clock must reject the write");
    golden::write_serde(
        &format!("site_triggers/gate/{case}.json"),
        &json!({
            "case": case,
            "entity": entity_name(entity),
            "op": op_name(op),
            "probe": {"ts": "now+3600 (normalized)", "priority": 9, "origin_site_id": "remote-z"},
            "rejected": error_json(&err),
        }),
    );

    sqlx::query("DELETE FROM site_entity_clock WHERE entity = ? AND entity_id = ?")
        .bind(entity_name(entity))
        .bind(entity_id(entity))
        .execute(st.db.pool())
        .await
        .unwrap();

    mutate(st.db.pool(), entity, op)
        .await
        .unwrap_or_else(|e| panic!("{case}: allowed write failed after gate clear: {e}"));

    let row = sqlx::query(
        "SELECT seq, origin_site_id, entity, entity_id, op, payload, priority, ts, created_at \
         FROM site_changelog ORDER BY seq",
    )
    .fetch_all(st.db.pool())
    .await
    .unwrap();
    assert_eq!(row.len(), 1, "{case}: expected exactly one changelog row");
    let r = &row[0];
    let payload: String = r.get("payload");
    let log_ts: i64 = r.get("ts");
    let log_created: i64 = r.get("created_at");
    assert!(log_ts > 0 && log_created > 0, "{case}: trigger timestamp");
    let seq: i64 = r.get("seq");

    let clock = sqlx::query(
        "SELECT entity, entity_id, ts, priority, origin_site_id, seq FROM site_entity_clock \
         WHERE entity = ? AND entity_id = ?",
    )
    .bind(entity_name(entity))
    .bind(entity_id(entity))
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    let clock_ts: i64 = clock.get("ts");
    assert!(clock_ts > 0, "{case}: clock timestamp");
    assert_eq!(clock.get::<i64, _>("seq"), seq, "{case}: clock seq");

    golden::write_serde(
        &format!("site_triggers/log/{case}.json"),
        &json!({
            "case": case,
            "entity": entity_name(entity),
            "entity_id": r.get::<String, _>("entity_id"),
            "op": r.get::<String, _>("op"),
            "payload": payload,
            "changelog": {
                "seq": seq,
                "origin_site_id": r.get::<String, _>("origin_site_id"),
                "entity": r.get::<String, _>("entity"),
                "entity_id": r.get::<String, _>("entity_id"),
                "op": r.get::<String, _>("op"),
                "priority": r.get::<i64, _>("priority"),
                "ts": NORMALIZED_TS,
                "created_at": NORMALIZED_TS,
            },
            "clock": {
                "entity": clock.get::<String, _>("entity"),
                "entity_id": clock.get::<String, _>("entity_id"),
                "ts": NORMALIZED_TS,
                "priority": clock.get::<i64, _>("priority"),
                "origin_site_id": clock.get::<String, _>("origin_site_id"),
                "seq": clock.get::<i64, _>("seq"),
            },
        }),
    );
}

async fn seed(st: &AppState, entity: Entity, op: Op) {
    let pool = st.db.pool();
    super::exec(
        pool,
        "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
         VALUES (1, 'golden-root', 'root-hash', 1, 'active', 100)",
    )
    .await;
    if !matches!((entity, op), (Entity::User, Op::Insert)) {
        seed_user(pool).await;
    }
    match entity {
        Entity::Bucket if !matches!(op, Op::Insert) => seed_bucket(pool).await,
        Entity::Policy if !matches!(op, Op::Insert) => seed_policy(pool).await,
        Entity::Permission if !matches!(op, Op::Insert) => seed_permission(pool).await,
        Entity::AccessKey if !matches!(op, Op::Insert) => seed_access_key(pool).await,
        _ => {}
    }
}

async fn seed_user(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
         VALUES (10, ?, ?, 0, 'active', 111)",
    )
    .bind(USER)
    .bind(fixtures::PASSWORD_HASH)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_bucket(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock) \
         VALUES (?, 10, 222, 'Off', 0)",
    )
    .bind(BUCKET)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_policy(pool: &SqlitePool) {
    sqlx::query("INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, 333)")
        .bind(BUCKET)
        .bind(fixtures::POLICY_JSON)
        .execute(pool)
        .await
        .unwrap();
}

async fn seed_permission(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
         VALUES (10, ?, 1, 0, 1)",
    )
    .bind(BUCKET)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_access_key(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO access_keys \
         (access_key, secret_key, user_id, status, expires_at, created_at, kind) \
         VALUES (?, ?, 10, 'active', 444, 555, 'long-term')",
    )
    .bind(fixtures::AK)
    .bind(fixtures::SK)
    .execute(pool)
    .await
    .unwrap();
}

async fn mutate(pool: &SqlitePool, entity: Entity, op: Op) -> Result<(), sqlx::Error> {
    match (entity, op) {
        (Entity::Bucket, Op::Insert) => {
            sqlx::query(
                "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock) \
                 VALUES (?, 10, 222, 'Off', 0)",
            )
            .bind(BUCKET)
            .execute(pool)
            .await?;
        }
        (Entity::Bucket, Op::Update) => {
            sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = ?")
                .bind(BUCKET)
                .execute(pool)
                .await?;
        }
        (Entity::Bucket, Op::Delete) => {
            sqlx::query("DELETE FROM buckets WHERE name = ?")
                .bind(BUCKET)
                .execute(pool)
                .await?;
        }
        (Entity::Policy, Op::Insert) => {
            sqlx::query(
                "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, 333)",
            )
            .bind(BUCKET)
            .bind(fixtures::POLICY_JSON)
            .execute(pool)
            .await?;
        }
        (Entity::Policy, Op::Update) => {
            sqlx::query(
                "UPDATE bucket_policies SET policy_json = ?, updated_at = 334 WHERE bucket = ?",
            )
            .bind(fixtures::POLICY_JSON)
            .bind(BUCKET)
            .execute(pool)
            .await?;
        }
        (Entity::Policy, Op::Delete) => {
            sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
                .bind(BUCKET)
                .execute(pool)
                .await?;
        }
        (Entity::Permission, Op::Insert) => {
            sqlx::query(
                "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
                 VALUES (10, ?, 1, 0, 1)",
            )
            .bind(BUCKET)
            .execute(pool)
            .await?;
        }
        (Entity::Permission, Op::Update) => {
            sqlx::query("UPDATE permissions SET can_read = 0 WHERE user_id = 10 AND bucket = ?")
                .bind(BUCKET)
                .execute(pool)
                .await?;
        }
        (Entity::Permission, Op::Delete) => {
            sqlx::query("DELETE FROM permissions WHERE user_id = 10 AND bucket = ?")
                .bind(BUCKET)
                .execute(pool)
                .await?;
        }
        (Entity::User, Op::Insert) => {
            sqlx::query(
                "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
                 VALUES (11, ?, ?, 0, 'active', 111)",
            )
            .bind(USER)
            .bind(fixtures::PASSWORD_HASH)
            .execute(pool)
            .await?;
        }
        (Entity::User, Op::Update) => {
            sqlx::query("UPDATE users SET status = 'disabled' WHERE username = ?")
                .bind(USER)
                .execute(pool)
                .await?;
        }
        (Entity::User, Op::Delete) => {
            sqlx::query("DELETE FROM users WHERE username = ?")
                .bind(USER)
                .execute(pool)
                .await?;
        }
        (Entity::AccessKey, Op::Insert) => {
            sqlx::query(
                "INSERT INTO access_keys \
                 (access_key, secret_key, user_id, status, expires_at, created_at, kind) \
                 VALUES (?, ?, 10, 'active', 444, 555, 'long-term')",
            )
            .bind(fixtures::AK)
            .bind(fixtures::SK)
            .execute(pool)
            .await?;
        }
        (Entity::AccessKey, Op::Update) => {
            sqlx::query("UPDATE access_keys SET status = 'disabled' WHERE access_key = ?")
                .bind(fixtures::AK)
                .execute(pool)
                .await?;
        }
        (Entity::AccessKey, Op::Delete) => {
            sqlx::query("DELETE FROM access_keys WHERE access_key = ?")
                .bind(fixtures::AK)
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}

async fn insert_future_clock(pool: &SqlitePool, entity: Entity, op: Op) {
    let (entity, id) = (entity_name(entity), entity_id_for(entity, op));
    sqlx::query(
        "INSERT INTO site_entity_clock \
         (entity, entity_id, ts, priority, origin_site_id, seq) \
         VALUES (?, ?, ?, 9, 'remote-z', 1)",
    )
    .bind(entity)
    .bind(id)
    .bind(now_secs() + 3600)
    .execute(pool)
    .await
    .unwrap();
}

fn entity_id(entity: Entity) -> String {
    entity_id_for(entity, Op::Update)
}

fn entity_id_for(entity: Entity, _op: Op) -> String {
    match entity {
        Entity::Bucket | Entity::Policy => BUCKET.into(),
        Entity::Permission => format!("{USER}\x1f{BUCKET}"),
        Entity::User => USER.into(),
        Entity::AccessKey => fixtures::AK.into(),
    }
}

fn entity_name(entity: Entity) -> &'static str {
    match entity {
        Entity::Bucket => "bucket",
        Entity::Policy => "bucket_policy",
        Entity::Permission => "permission",
        Entity::User => "user",
        Entity::AccessKey => "access_key",
    }
}

fn op_name(op: Op) -> &'static str {
    match op {
        Op::Insert => "insert",
        Op::Update => "update",
        Op::Delete => "delete",
    }
}
