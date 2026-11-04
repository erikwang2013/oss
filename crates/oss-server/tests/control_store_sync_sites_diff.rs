//! sync intents 与 sites/site_cursors/site_config 迁移差分：CRUD、状态更新、恢复
//! 扫描、UPDATE-then-INSERT 与游标 lag 计算同迁移前 SQL 一致。

use oss_server::control_store::{SqliteControlStore, SqliteControlTxn};
use oss_server::db::Db;
use sqlx::SqlitePool;
use xl_store::control_store::sites::{SitePeerRow, SiteStore, SiteTxn};
use xl_store::control_store::sync_intents::{
    NewSyncDeleteIntent, NewSyncWriteIntent, SyncIntentStore, SyncIntentTxn,
};

async fn pair() -> (Db, Db) {
    (
        Db::connect("sqlite::memory:").await.unwrap(),
        Db::connect("sqlite::memory:").await.unwrap(),
    )
}

async fn dump(pool: &SqlitePool, table: &str, cols: &str) -> Vec<String> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT json_array({cols}) FROM {table} ORDER BY id"
    )))
    .fetch_all(pool)
    .await
    .unwrap()
}

fn new_write(id: &str, updated_at: i64) -> NewSyncWriteIntent {
    NewSyncWriteIntent {
        id: id.into(),
        bucket: "b".into(),
        key: format!("k-{id}"),
        local_version_id: format!("lv-{id}"),
        remote_version_id: format!("rv-{id}"),
        tee_path: format!("/tee/{id}"),
        commit_json: "{}".into(),
        state: "remote_pending".into(),
        created_at: 10,
        updated_at,
    }
}

fn new_delete(id: &str, op: &str, updated_at: i64) -> NewSyncDeleteIntent {
    NewSyncDeleteIntent {
        id: id.into(),
        bucket: "b".into(),
        key: format!("k-{id}"),
        op: op.into(),
        local_version_id: format!("lv-{id}"),
        remote_version_id: format!("rv-{id}"),
        marker_version_id: String::new(),
        state: "remote_pending".into(),
        created_at: 10,
        updated_at,
    }
}

async fn legacy_write_insert(pool: &SqlitePool, n: &NewSyncWriteIntent) {
    sqlx::query(
        "INSERT INTO sync_write_intents \
         (id, bucket, key, local_version_id, remote_version_id, tee_path, commit_json, \
          state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&n.id)
    .bind(&n.bucket)
    .bind(&n.key)
    .bind(&n.local_version_id)
    .bind(&n.remote_version_id)
    .bind(&n.tee_path)
    .bind(&n.commit_json)
    .bind(&n.state)
    .bind(n.created_at)
    .bind(n.updated_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn legacy_delete_insert(pool: &SqlitePool, n: &NewSyncDeleteIntent) {
    sqlx::query(
        "INSERT INTO sync_delete_intents \
         (id, bucket, key, op, local_version_id, remote_version_id, marker_version_id, \
          state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&n.id)
    .bind(&n.bucket)
    .bind(&n.key)
    .bind(&n.op)
    .bind(&n.local_version_id)
    .bind(&n.remote_version_id)
    .bind(&n.marker_version_id)
    .bind(&n.state)
    .bind(n.created_at)
    .bind(n.updated_at)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn sync_intents_insert_status_recovery_matches_legacy() {
    let (s, l) = pair().await;
    for (id, at) in [("a", 100), ("b", 200), ("c", 300)] {
        let w = new_write(id, at);
        let mut conn = s.pool().acquire().await.unwrap();
        SqliteControlTxn::new(&mut conn)
            .sync_write_intent_insert(&w)
            .await
            .unwrap();
        drop(conn);
        legacy_write_insert(l.pool(), &w).await;
        let d = new_delete(id, "delete_version", at);
        let mut conn = s.pool().acquire().await.unwrap();
        SqliteControlTxn::new(&mut conn)
            .sync_delete_intent_insert(&d)
            .await
            .unwrap();
        drop(conn);
        legacy_delete_insert(l.pool(), &d).await;
    }
    let wcols = "id, bucket, key, local_version_id, remote_version_id, tee_path, commit_json, \
                 state, created_at, updated_at";
    let dcols = "id, bucket, key, op, local_version_id, remote_version_id, marker_version_id, \
                 state, created_at, updated_at";
    assert_eq!(
        dump(s.pool(), "sync_write_intents", wcols).await,
        dump(l.pool(), "sync_write_intents", wcols).await
    );
    assert_eq!(
        dump(s.pool(), "sync_delete_intents", dcols).await,
        dump(l.pool(), "sync_delete_intents", dcols).await
    );

    // remote_done 状态更新（含 updated_at）。
    let mut conn = s.pool().acquire().await.unwrap();
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .sync_write_intent_mark_done("b", 400)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .sync_delete_intent_mark_done("b", 400)
            .await
            .unwrap(),
        1
    );
    drop(conn);
    sqlx::query(
        "UPDATE sync_write_intents SET state = 'remote_done', updated_at = 400 WHERE id = 'b'",
    )
    .execute(l.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE sync_delete_intents SET state = 'remote_done', updated_at = 400 WHERE id = 'b'",
    )
    .execute(l.pool())
    .await
    .unwrap();
    assert_eq!(
        dump(s.pool(), "sync_write_intents", wcols).await,
        dump(l.pool(), "sync_write_intents", wcols).await
    );
    assert_eq!(
        dump(s.pool(), "sync_delete_intents", dcols).await,
        dump(l.pool(), "sync_delete_intents", dcols).await
    );

    let store = SqliteControlStore::new(s.pool().clone());
    let stale: Vec<String> = store
        .sync_write_intents_stale(250)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(stale, vec!["a"]);
    assert_eq!(
        store.sync_write_intent_tee("a").await.unwrap().as_deref(),
        Some("/tee/a")
    );
    let mut tees = store.sync_write_intent_tees().await.unwrap();
    tees.sort();
    assert_eq!(tees, vec!["/tee/a", "/tee/b", "/tee/c"]);
    let stale = store.sync_delete_intents_stale(250, 10).await.unwrap();
    assert_eq!(
        stale.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        vec!["a"]
    );
    assert_eq!(stale[0].local_version_id.as_deref(), Some("lv-a"));
    assert_eq!(stale[0].marker_version_id.as_deref(), Some(""));

    let mut conn = s.pool().acquire().await.unwrap();
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .sync_write_intent_delete("a")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .sync_write_intent_count()
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .sync_delete_intent_delete("a")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .sync_delete_intent_count()
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .sync_write_intent_delete_all()
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .sync_delete_intent_delete_all()
            .await
            .unwrap(),
        2
    );
    drop(conn);
    assert!(
        SqliteControlStore::new(s.pool().clone())
            .sync_write_intent_tees()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(dump(s.pool(), "sync_write_intents", wcols).await.is_empty());
    assert!(
        dump(s.pool(), "sync_delete_intents", dcols)
            .await
            .is_empty()
    );
}

fn peer(peer_id: &str, remote: &str, created_at: i64) -> SitePeerRow {
    SitePeerRow {
        peer_id: peer_id.into(),
        remote_site_id: remote.into(),
        name: format!("name-{peer_id}"),
        endpoint: format!("https://{peer_id}"),
        region: "us-east-1".into(),
        priority: 3,
        s3_access_key: "ak".into(),
        s3_secret_key: "sk".into(),
        peer_site_token: format!("tok-{peer_id}"),
        created_at,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

async fn dump_sites(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(peer_id, remote_site_id, name, endpoint, region, priority, \
         s3_access_key, s3_secret_key, peer_site_token, created_at, last_seen, last_error, \
         raft_node_id, reconcile_required) FROM sites ORDER BY peer_id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn dump_cursors(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(peer_id, last_seq, updated_at) FROM site_cursors ORDER BY peer_id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn legacy_insert_site(pool: &SqlitePool, p: &SitePeerRow, now: i64) {
    sqlx::query(
        "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, region, priority, \
         s3_access_key, s3_secret_key, peer_site_token, created_at, raft_node_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&p.peer_id)
    .bind(&p.remote_site_id)
    .bind(&p.name)
    .bind(&p.endpoint)
    .bind(&p.region)
    .bind(p.priority)
    .bind(&p.s3_access_key)
    .bind(&p.s3_secret_key)
    .bind(&p.peer_site_token)
    .bind(p.created_at)
    .bind(p.raft_node_id.map(|v| v as i64))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT OR IGNORE INTO site_cursors (peer_id, last_seq, updated_at) VALUES (?, 0, ?)",
    )
    .bind(&p.peer_id)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}

async fn legacy_upsert_site(pool: &SqlitePool, p: &SitePeerRow) {
    let updated = sqlx::query(
        "UPDATE sites SET remote_site_id = ?, name = ?, endpoint = ?, region = ?, priority = ?, \
         s3_access_key = ?, s3_secret_key = ?, peer_site_token = ?, raft_node_id = ? \
         WHERE peer_id = ? OR remote_site_id = ?",
    )
    .bind(&p.remote_site_id)
    .bind(&p.name)
    .bind(&p.endpoint)
    .bind(&p.region)
    .bind(p.priority)
    .bind(&p.s3_access_key)
    .bind(&p.s3_secret_key)
    .bind(&p.peer_site_token)
    .bind(p.raft_node_id.map(|v| v as i64))
    .bind(&p.peer_id)
    .bind(&p.remote_site_id)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected();
    if updated == 0 {
        sqlx::query(
            "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, region, priority, \
             s3_access_key, s3_secret_key, peer_site_token, created_at, raft_node_id, \
             reconcile_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&p.peer_id)
        .bind(&p.remote_site_id)
        .bind(&p.name)
        .bind(&p.endpoint)
        .bind(&p.region)
        .bind(p.priority)
        .bind(&p.s3_access_key)
        .bind(&p.s3_secret_key)
        .bind(&p.peer_site_token)
        .bind(p.created_at)
        .bind(p.raft_node_id.map(|v| v as i64))
        .bind(i64::from(p.reconcile_required))
        .execute(pool)
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn sites_insert_upsert_ping_delete_matches_legacy_sql() {
    let (s, l) = pair().await;
    let mut a = peer("p1", "r1", 10);
    let b = peer("p2", "r2", 20);
    for p in [&a, &b] {
        let mut tx = s.pool().begin().await.unwrap();
        SqliteControlTxn::new(&mut tx)
            .site_insert(p, 500)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        legacy_insert_site(l.pool(), p, 500).await;
    }
    assert_eq!(dump_sites(s.pool()).await, dump_sites(l.pool()).await);
    assert_eq!(dump_cursors(s.pool()).await, dump_cursors(l.pool()).await);

    // UPDATE 命中，且不得清除 repair 门。
    a.name = "renamed".into();
    a.reconcile_required = true;
    let mut tx = s.pool().begin().await.unwrap();
    SqliteControlTxn::new(&mut tx)
        .site_pair_upsert(&a)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    legacy_upsert_site(l.pool(), &a).await;
    assert_eq!(dump_sites(s.pool()).await, dump_sites(l.pool()).await);
    // 未命中 → INSERT。
    let c = peer("p3", "r3", 30);
    let mut tx = s.pool().begin().await.unwrap();
    SqliteControlTxn::new(&mut tx)
        .site_pair_upsert(&c)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    legacy_upsert_site(l.pool(), &c).await;
    assert_eq!(dump_sites(s.pool()).await, dump_sites(l.pool()).await);
    // remote_site_id 命中同一行（不新增）。
    let mut alias = peer("p4", "r1", 40);
    alias.name = "alias".into();
    let mut tx = s.pool().begin().await.unwrap();
    SqliteControlTxn::new(&mut tx)
        .site_pair_upsert(&alias)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    legacy_upsert_site(l.pool(), &alias).await;
    assert_eq!(
        dump_sites(s.pool()).await,
        dump_sites(l.pool()).await,
        "按 remote_site_id 命中"
    );

    let mut conn = s.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .site_update_ping("p1", 777, Some("down"))
        .await
        .unwrap();
    drop(conn);
    sqlx::query("UPDATE sites SET last_seen = 777, last_error = 'down' WHERE peer_id = 'p1'")
        .execute(l.pool())
        .await
        .unwrap();
    assert_eq!(dump_sites(s.pool()).await, dump_sites(l.pool()).await);

    let mut conn = s.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .site_set_reconcile_required("p1", false)
        .await
        .unwrap();
    drop(conn);
    sqlx::query("UPDATE sites SET reconcile_required = 0 WHERE peer_id = 'p1'")
        .execute(l.pool())
        .await
        .unwrap();
    assert_eq!(dump_sites(s.pool()).await, dump_sites(l.pool()).await);

    let store = SqliteControlStore::new(s.pool().clone());
    assert_eq!(store.site_peer_ids().await.unwrap(), vec!["p1", "p2", "p3"]);
    assert_eq!(store.site_count().await.unwrap(), 3);
    assert_eq!(
        store.site_remote_id("p1").await.unwrap().as_deref(),
        Some("r1")
    );
    let credentials = store.site_credentials("p2").await.unwrap().unwrap();
    assert_eq!(credentials.endpoint, "https://p2");
    assert_eq!(credentials.peer_site_token, "tok-p2");
    let sites = store.sites().await.unwrap();
    assert_eq!(
        sites.iter().map(|r| r.peer_id.as_str()).collect::<Vec<_>>(),
        vec!["p1", "p2", "p3"]
    );

    let mut conn = s.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .site_cursor_delete("p2")
        .await
        .unwrap();
    SqliteControlTxn::new(&mut conn)
        .site_delete("p2")
        .await
        .unwrap();
    drop(conn);
    sqlx::query("DELETE FROM site_cursors WHERE peer_id = 'p2'")
        .execute(l.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM sites WHERE peer_id = 'p2'")
        .execute(l.pool())
        .await
        .unwrap();
    assert_eq!(dump_sites(s.pool()).await, dump_sites(l.pool()).await);
    assert_eq!(dump_cursors(s.pool()).await, dump_cursors(l.pool()).await);
}

#[tokio::test]
async fn site_config_trim_apply_and_lag_rows() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    let id = db.site_identity().await.unwrap().id;
    // 老库多行缺陷：trim 保留最早一行。
    sqlx::query(
        "INSERT INTO site_config (id, name, region, priority, site_token, created_at) \
         VALUES ('dup', 'x', 'x', 9, 'x', 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let mut conn = db.pool().acquire().await.unwrap();
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .site_config_trim()
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .site_config_id()
            .await
            .unwrap()
            .as_deref(),
        Some(id.as_str())
    );
    SqliteControlTxn::new(&mut conn)
        .site_config_apply(Some("site-x"), Some("eu-west-1"), Some(5), None)
        .await
        .unwrap();
    drop(conn);
    let identity = SqliteControlStore::new(db.pool().clone())
        .site_identity()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(identity.name, "site-x");
    assert_eq!(identity.region, "eu-west-1");
    assert_eq!(identity.priority, 5);
    assert_eq!(identity.id, id);

    // 出站 lag：两个 peer，一个游标追平 changelog、一个落后。
    for p in [peer("p1", "r1", 10), peer("p2", "r2", 20)] {
        let mut tx = db.pool().begin().await.unwrap();
        SqliteControlTxn::new(&mut tx)
            .site_insert(&p, 500)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    sqlx::query(
        "INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts) \
         VALUES ('x', 'bucket', 'b', 'upsert', '{}', 1, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let max_seq: i64 = sqlx::query_scalar("SELECT MAX(seq) FROM site_changelog")
        .fetch_one(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE site_cursors SET last_seq = ? WHERE peer_id = 'p1'")
        .bind(max_seq)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        SqliteControlStore::new(db.pool().clone())
            .site_lag_rows()
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        SqliteControlStore::new(db.pool().clone())
            .site_cursor("p1")
            .await
            .unwrap(),
        Some(max_seq)
    );
    assert_eq!(
        SqliteControlStore::new(db.pool().clone())
            .first_site_remote_id()
            .await
            .unwrap()
            .as_deref(),
        Some("r1")
    );
}
