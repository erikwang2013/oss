//! D2 parity: object `site_config` / `sites` vs the SQLite seam.
//!
//! Cursors are local files in the object backend (design §7), so cursor parity
//! is deliberately not asserted here.

use futures::future::BoxFuture;
use xl_store::control_store::ControlError;
use xl_store::control_store::sites::{SitePeerRow, SiteStore, SiteTxn};

use super::{local, real_store};
use crate::control_store::{SqliteControlStore, SqliteControlTxn};
use crate::control_store_obj::{ObjectControlTxn, TableId};

type Sqlite = (crate::db::Db, SqliteControlStore);

async fn sqlite() -> Sqlite {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    let store = SqliteControlStore::new(db.pool().clone());
    (db, store)
}

async fn in_sqlite<R>(
    db: &crate::db::Db,
    f: impl for<'a> FnOnce(&'a mut SqliteControlTxn<'a>) -> BoxFuture<'a, R>,
) -> R {
    let mut conn = db.pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();
    let out = {
        let mut txn = SqliteControlTxn::new(&mut conn);
        f(&mut txn).await
    };
    sqlx::query("COMMIT").execute(&mut *conn).await.unwrap();
    out
}

fn se(e: ControlError) -> xl_store::error::Error {
    xl_store::error::Error::InvalidArgument(e.to_string())
}

fn peer(peer_id: &str, remote: &str, created_at: i64) -> SitePeerRow {
    SitePeerRow {
        peer_id: peer_id.into(),
        remote_site_id: remote.into(),
        name: format!("name-{peer_id}"),
        endpoint: format!("http://{peer_id}:9000"),
        region: "us-east-1".into(),
        priority: 3,
        s3_access_key: format!("AK-{peer_id}"),
        s3_secret_key: format!("SK-{peer_id}"),
        peer_site_token: format!("TOK-{peer_id}"),
        created_at,
        last_seen: None,
        last_error: None,
        raft_node_id: Some(42),
        reconcile_required: false,
    }
}

async fn sqlite_insert_peer(db: &crate::db::Db, p: &SitePeerRow) {
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
    .execute(db.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn config_and_peer_rows_match_sqlite() {
    let (db, sqlite_store) = sqlite().await;
    let real = real_store().await;
    let identity = sqlite_store.site_identity().await.unwrap().unwrap();

    // SQLite: seed/apply/insert/update in one explicit transaction.
    let p1 = peer("peer-a", "remote-a", 10);
    let p2 = peer("peer-b", "remote-b", 5);
    in_sqlite(&db, |txn| {
        let p1 = p1.clone();
        let p2 = p2.clone();
        Box::pin(async move {
            txn.site_config_apply(Some("east"), Some("eu-west-1"), Some(7), Some("tok-2"))
                .await
                .unwrap();
            txn.site_insert(&p1, 100).await.unwrap();
            txn.site_insert(&p2, 101).await.unwrap();
        })
    })
    .await;
    sqlx::query("UPDATE sites SET reconcile_required = 1 WHERE peer_id = 'peer-a'")
        .execute(db.pool())
        .await
        .unwrap();

    real.control
        .transaction(local(), |txn| {
            let identity = identity.clone();
            let p1 = p1.clone();
            let p2 = p2.clone();
            Box::pin(async move {
                txn.site_config_seed(&identity.id, &identity.token, 1)
                    .await
                    .map_err(se)?;
                txn.site_config_apply(Some("east"), Some("eu-west-1"), Some(7), Some("tok-2"))
                    .await
                    .map_err(se)?;
                txn.site_insert(&p1, 100).await.map_err(se)?;
                txn.site_insert(&p2, 101).await.map_err(se)?;
                txn.site_set_reconcile_required("peer-a", true)
                    .await
                    .map_err(se)?;
                Ok(())
            })
        })
        .await
        .unwrap();

    let s_identity = sqlite_store.site_identity().await.unwrap();
    let o_identity = SiteStore::site_identity(&*real.control).await.unwrap();
    assert_eq!(s_identity, o_identity);

    let s_sites = sqlite_store.sites().await.unwrap();
    let o_sites = SiteStore::sites(&*real.control).await.unwrap();
    assert_eq!(s_sites, o_sites, "created_at, peer_id order");
    assert_eq!(o_sites[0].peer_id, "peer-b");

    assert_eq!(
        sqlite_store.site_count().await.unwrap(),
        SiteStore::site_count(&*real.control).await.unwrap()
    );
    assert_eq!(
        sqlite_store.site_peer_ids().await.unwrap(),
        SiteStore::site_peer_ids(&*real.control).await.unwrap()
    );
    assert_eq!(
        sqlite_store.first_site_remote_id().await.unwrap(),
        SiteStore::first_site_remote_id(&*real.control)
            .await
            .unwrap()
    );
    for id in ["peer-a", "peer-b", "missing"] {
        assert_eq!(
            sqlite_store.site(id).await.unwrap(),
            SiteStore::site(&*real.control, id).await.unwrap()
        );
        assert_eq!(
            sqlite_store.site_remote_id(id).await.unwrap(),
            SiteStore::site_remote_id(&*real.control, id).await.unwrap()
        );
        assert_eq!(
            sqlite_store.site_credentials(id).await.unwrap(),
            SiteStore::site_credentials(&*real.control, id)
                .await
                .unwrap()
        );
    }
    assert_eq!(
        sqlite_store.site_lag_rows().await.unwrap(),
        SiteStore::site_lag_rows(&*real.control).await.unwrap(),
        "no changelog: both report no lag"
    );

    // UPDATE-then-INSERT by remote id: peer_id/created_at/reconcile_required kept.
    let update = peer("peer-x", "remote-a", 99);
    in_sqlite(&db, |txn| {
        let update = update.clone();
        Box::pin(async move { txn.site_pair_upsert(&update).await.unwrap() })
    })
    .await;
    real.control
        .transaction(local(), |txn| {
            let update = update.clone();
            Box::pin(async move { txn.site_pair_upsert(&update).await.map_err(se) })
        })
        .await
        .unwrap();
    assert_eq!(
        sqlite_store.site("peer-a").await.unwrap(),
        SiteStore::site(&*real.control, "peer-a").await.unwrap()
    );
    assert_eq!(
        SiteStore::site(&*real.control, "peer-x").await.unwrap(),
        None
    );

    // Insert path.
    let p3 = peer("peer-c", "remote-c", 20);
    in_sqlite(&db, |txn| {
        let p3 = p3.clone();
        Box::pin(async move { txn.site_pair_upsert(&p3).await.unwrap() })
    })
    .await;
    real.control
        .transaction(local(), |txn| {
            let p3 = p3.clone();
            Box::pin(async move { txn.site_pair_upsert(&p3).await.map_err(se) })
        })
        .await
        .unwrap();
    assert_eq!(
        sqlite_store.sites().await.unwrap(),
        SiteStore::sites(&*real.control).await.unwrap()
    );

    // Ping/reconcile/delete.
    in_sqlite(&db, |txn| {
        Box::pin(async move {
            txn.site_update_ping("peer-b", 500, Some("boom"))
                .await
                .unwrap();
            txn.site_set_reconcile_required("peer-b", true)
                .await
                .unwrap();
            assert_eq!(txn.site_delete("peer-b").await.unwrap(), 1);
        })
    })
    .await;
    real.control
        .transaction(local(), |txn| {
            Box::pin(async move {
                txn.site_update_ping("peer-b", 500, Some("boom"))
                    .await
                    .map_err(se)?;
                txn.site_set_reconcile_required("peer-b", true)
                    .await
                    .map_err(se)?;
                assert_eq!(txn.site_delete("peer-b").await.map_err(se)?, 1);
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(
        sqlite_store.sites().await.unwrap(),
        SiteStore::sites(&*real.control).await.unwrap()
    );
}

#[tokio::test]
async fn duplicate_peer_insert_is_rejected_without_committing() {
    let real = real_store().await;
    let identity = {
        real.control
            .transaction(local(), |txn| {
                Box::pin(async move {
                    txn.site_config_seed("site-a", "tok", 1).await.map_err(se)?;
                    txn.site_config_id().await.map_err(se)
                })
            })
            .await
            .unwrap()
    };
    assert_eq!(identity.as_deref(), Some("site-a"));
    let p = peer("peer-a", "remote-a", 1);
    real.control
        .transaction(local(), |txn| {
            let p = p.clone();
            Box::pin(async move { txn.site_insert(&p, 1).await.map_err(se) })
        })
        .await
        .unwrap();
    let err = real
        .control
        .transaction(local(), |txn| {
            let p = p.clone();
            Box::pin(async move { txn.site_insert(&p, 2).await.map_err(se) })
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed"),
        "{err}"
    );
    assert_eq!(SiteStore::site_count(&*real.control).await.unwrap(), 1);
}

#[tokio::test]
async fn seed_is_idempotent_and_trim_is_a_noop() {
    let (db, sqlite_store) = sqlite().await;
    let real = real_store().await;
    let s_id = sqlite_store.site_identity().await.unwrap().unwrap().id;
    in_sqlite(&db, |txn| {
        Box::pin(async move {
            txn.site_config_seed("ignored", "tok", 1).await.unwrap();
            assert_eq!(txn.site_config_trim().await.unwrap(), 0);
        })
    })
    .await;
    real.control
        .transaction(local(), |txn| {
            let s_id = s_id.clone();
            Box::pin(async move {
                txn.site_config_seed(&s_id, "tok", 1).await.map_err(se)?;
                txn.site_config_seed("ignored", "tok", 1)
                    .await
                    .map_err(se)?;
                assert_eq!(txn.site_config_trim().await.map_err(se)?, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
    let sql_id = sqlite_store.site_identity().await.unwrap().unwrap();
    let obj_id = SiteStore::site_identity(&*real.control)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(obj_id.id, s_id);
    assert_eq!((&obj_id.name, &obj_id.region, obj_id.priority), (&sql_id.name, &sql_id.region, sql_id.priority));
    assert_eq!(obj_id.token, "tok");
    assert_eq!(obj_id.id, s_id);
}

#[tokio::test]
async fn site_rows_live_in_the_site_tables() {
    let real = real_store().await;
    real.control
        .transaction(local(), |txn| {
            Box::pin(async move {
                assert_eq!(txn.site_config_id().await.map_err(se)?, None);
                txn.site_config_seed("site-a", "tok", 1).await.map_err(se)?;
                txn.site_insert(&peer("peer-a", "remote-a", 1), 1)
                    .await
                    .map_err(se)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let pinned = real.control.pin().await.unwrap();
    assert!(pinned.root.table(TableId::SiteConfig.as_u16()).is_some());
    assert!(pinned.root.table(TableId::Sites.as_u16()).is_some());
}
