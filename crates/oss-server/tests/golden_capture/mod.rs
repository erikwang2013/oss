//! A8 capture harness. Runs only through `scripts/capture_control_goldens.sh`
//! (`--ignored`, `CAPTURE_CONTROL_GOLDENS=1`) against the SQLite backend.

mod cascade_install;
mod fixtures;
mod guards;
mod local_rows;
mod rows;
mod site_extra;
mod site_triggers;
mod snapshots;

use oss_server::config::SiteToml;
use oss_server::db::SitePeer;
use serde_json::json;
use sqlx::SqlitePool;

use crate::golden;

pub const SITE_ID: &str = "site-golden";
pub const SITE_PRIORITY: i64 = 7;

/// All nondeterministic trigger columns (`ts`/`created_at` from
/// `strftime('%s','now')`) are written as zero in goldens.
pub const NORMALIZED_TS: i64 = 0;

#[tokio::test]
#[ignore = "writes goldens; run scripts/capture_control_goldens.sh"]
async fn capture_control_goldens() {
    assert_eq!(
        std::env::var("CAPTURE_CONTROL_GOLDENS").as_deref(),
        Ok("1"),
        "refusing to rewrite goldens without CAPTURE_CONTROL_GOLDENS=1"
    );
    for sub in ["rows", "snapshots", "site_triggers", "guards", "cascade"] {
        let p = golden::root().join(sub);
        if p.exists() {
            std::fs::remove_dir_all(&p).unwrap();
        }
    }

    rows::capture();
    local_rows::capture();
    snapshots::capture().await;
    site_triggers::capture().await;
    site_extra::capture().await;
    guards::capture().await;
    cascade_install::capture().await;
}

pub fn site_config() -> SiteToml {
    SiteToml {
        id: Some(SITE_ID.into()),
        name: Some("golden-a".into()),
        region: "us-east-1".into(),
        priority: SITE_PRIORITY,
        token: Some("a".repeat(32)),
    }
}

pub fn peer() -> SitePeer {
    SitePeer {
        peer_id: "peer-b".into(),
        remote_site_id: "remote-b".into(),
        name: "peer-b".into(),
        endpoint: "http://127.0.0.1:9".into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "AKIAGOLDEN00000001".into(),
        s3_secret_key: "SKGOLDEN00000000001".into(),
        peer_site_token: "b".repeat(32),
        created_at: 1_700_000_000,
        last_seen: Some(1_700_000_100),
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// `Db::connect` seeds `site_config.id` with a random UUID; pin it before any
/// trigger can copy it into `site_changelog`/`site_entity_clock`.
pub async fn fix_site_id(pool: &SqlitePool) {
    sqlx::query("UPDATE site_config SET id = ? WHERE rowid = (SELECT MIN(rowid) FROM site_config)")
        .bind(SITE_ID)
        .execute(pool)
        .await
        .unwrap();
}

pub async fn exec(pool: &SqlitePool, sql: &'static str) {
    sqlx::query(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

pub fn error_json(e: &sqlx::Error) -> serde_json::Value {
    json!({
        "error": e.to_string(),
        "sqlite_message": e.as_database_error().map(|d| d.message().to_string()),
    })
}
