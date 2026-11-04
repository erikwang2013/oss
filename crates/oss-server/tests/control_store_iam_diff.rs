//! IAM / 账户写路径的 site 触发器与差分回归。

mod common;

use common::state_with_site;
use oss_server::config::SiteToml;
use oss_server::control_store::SqliteControlTxn;
use oss_server::db::SitePeer;
use xl_store::control_store::iam::IamTxn;

fn site() -> SiteToml {
    SiteToml {
        id: None,
        name: Some("a".into()),
        region: "us-east-1".into(),
        priority: 0,
        token: Some("a".repeat(32)),
    }
}

/// 行已存在时第二次 upsert 会走 site UPDATE 触发器：旧 UPSERT 实现在 SQLite
/// 下对 `site_entity_clock` 的 `INSERT OR IGNORE` 误报 UNIQUE。
#[tokio::test]
async fn site_triggers_accept_double_upsert_for_permissions_and_access_keys() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_site(dir.path(), Some(site())).await;
    // 触发器要求 sites 与 site_config 同时存在（配对后才捕获变更）。
    st.db
        .insert_site(&SitePeer {
            peer_id: "peer".into(),
            remote_site_id: "b".into(),
            name: "b".into(),
            endpoint: "http://127.0.0.1:9".into(),
            region: "us-east-1".into(),
            priority: 0,
            s3_access_key: "AKIATEST000000001".into(),
            s3_secret_key: "SKTEST000000000001".into(),
            peer_site_token: "b".repeat(32),
            created_at: 1,
            last_seen: None,
            last_error: None,
            raft_node_id: None,
            reconcile_required: false,
        })
        .await
        .unwrap();
    st.db.create_user("root", "pw", true).await.unwrap();
    let alice = st.db.create_user("alice", "pw", false).await.unwrap();

    let mut conn = st.db.pool().acquire().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut conn);
        txn.permission_upsert(alice, "bkt", true, false, false)
            .await
            .unwrap();
        txn.permission_upsert(alice, "bkt", false, true, true)
            .await
            .unwrap();
        txn.access_key_site_upsert("AKALICE000000000001", "SK1", alice, "active", None, 100)
            .await
            .unwrap();
        txn.access_key_site_upsert(
            "AKALICE000000000001",
            "SK2",
            alice,
            "disabled",
            Some(200),
            999,
        )
        .await
        .unwrap();
    }
    drop(conn);

    let perms: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM site_changelog WHERE entity = 'permission'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(perms, 2, "insert + update 各一条 upsert changelog");
    let aks: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM site_changelog WHERE entity = 'access_key'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(aks, 2);
    let clock: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM site_entity_clock \
         WHERE entity IN ('permission', 'access_key')",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(clock, 2, "每个实体只保留一行时钟");

    let perms: (i64, i64, i64) =
        sqlx::query_as("SELECT can_read, can_write, can_delete FROM permissions")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(perms, (0, 1, 1));
    let ak: (String, String, Option<i64>, i64) = sqlx::query_as(
        "SELECT secret_key, status, expires_at, created_at FROM access_keys \
         WHERE access_key = 'AKALICE000000000001'",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(ak, ("SK2".into(), "disabled".into(), Some(200), 100));
}
