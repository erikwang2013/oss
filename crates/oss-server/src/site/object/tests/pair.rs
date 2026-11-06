//! Object-backend pairing: snapshot rows, single-peer guard, Db seam
//! regression (placeholder pool must stay untouched), and pair→ship→apply.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use xl_store::control_state::root::Mode;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketEntry, BucketSnapshotTxn, BucketTxn};
use xl_store::control_store::iam::{IamStore, IamTxn, UserEntry};
use xl_store::control_store::sites::{SiteIdentityRow, SiteStore};
use xl_store::store::{Store, StoreConfig};

use super::super::{apply_changelog, insert_peer_with_snapshot, ship_once};
use super::{CLEANUP, Fx, empty_fixture};
use crate::control_backend::ObjectControl;
use crate::control_store_obj::{ObjectControlStore, WriteOrigin};
use crate::db::{Db, SitePeer};
use crate::local_store::DeadLetter;
use crate::site::Entry;

fn peer(endpoint: &str) -> SitePeer {
    SitePeer {
        peer_id: "peer-b".into(),
        remote_site_id: "remote-b".into(),
        name: "b".into(),
        endpoint: endpoint.into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "ak".into(),
        s3_secret_key: "sk".into(),
        peer_site_token: "tok".into(),
        created_at: 42,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

async fn seed_rows(fx: &Fx) -> i64 {
    let mut txn = fx.control.begin(WriteOrigin::local("seed")).await.unwrap();
    let alice = txn
        .user_create(&UserEntry {
            username: "alice".into(),
            password_hash: "hash".into(),
            status: "active".into(),
            created_at: 5,
        })
        .await
        .unwrap();
    let bob = txn
        .user_create(&UserEntry {
            username: "bob".into(),
            password_hash: "bob-hash".into(),
            status: "active".into(),
            created_at: 5,
        })
        .await
        .unwrap();
    txn.access_key_site_upsert("AKALICE000000000001", "secret", alice, "active", None, 5)
        .await
        .unwrap();
    txn.permission_upsert(alice, "bkt", true, false, false)
        .await
        .unwrap();
    seed_bucket(&mut txn, "bkt", "alice").await;
    txn.bucket_policy_set("bkt", r#"{"Version":"2012-10-17"}"#, 5)
        .await
        .unwrap();
    // Bob's winning value came from the remote site: never echoed back.
    txn.site_put_clock("user", "bob", 50, 0, "remote-b", 1)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    bob
}

async fn seed_bucket(
    txn: &mut crate::control_store_obj::ObjectControlTxn,
    name: &str,
    owner: &str,
) {
    txn.bucket_upsert_install(&BucketEntry {
        name: name.into(),
        owner: OwnerRef::User(owner.into()),
        creation_date: 5,
        versioning: "Enabled".into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "None".into(),
        deleting: false,
    })
    .await
    .unwrap();
}

fn payload(row: &xl_store::control_store::sites::SiteChangelogRow) -> serde_json::Value {
    serde_json::from_str(&row.payload).unwrap()
}

#[tokio::test]
async fn pairing_snapshots_local_rows_and_creates_cursor() {
    let fx = empty_fixture("site-a", 3).await;
    let _bob = seed_rows(&fx).await;
    insert_peer_with_snapshot(&fx.control, false, &peer("http://127.0.0.1:9"))
        .await
        .unwrap();

    let sites = fx.control.sites().await.unwrap();
    assert_eq!(sites.len(), 1);
    assert_eq!(sites[0].peer_id, "peer-b");
    assert_eq!(sites[0].remote_site_id, "remote-b");

    let rows = fx.control.site_changelog_after(0).await.unwrap();
    let entities: Vec<&str> = rows.iter().map(|r| r.entity.as_str()).collect();
    assert_eq!(
        entities,
        vec![
            "user",
            "access_key",
            "permission",
            "bucket",
            "bucket_policy"
        ]
    );
    assert!(rows.windows(2).all(|w| w[0].seq < w[1].seq));
    for row in &rows {
        assert_eq!(row.origin_site_id, "site-a");
        assert_eq!(row.priority, 3);
        assert_eq!(row.op, "upsert");
        assert!(row.ts > 0);
    }
    assert_eq!(payload(&rows[0])["password_hash"], "hash");
    assert_eq!(payload(&rows[1])["access_key"], "AKALICE000000000001");
    assert_eq!(payload(&rows[3])["owner_username"], "alice");
    assert!(
        rows.iter().all(|r| r.entity_id != "bob"),
        "remote value echoed"
    );

    let clock = fx
        .control
        .site_clock("user", "alice")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(clock.origin_site_id, "site-a");
    assert_eq!(clock.priority, 3);
    assert_eq!(clock.seq, rows[0].seq);
    let bob = fx.control.site_clock("user", "bob").await.unwrap().unwrap();
    assert_eq!(bob.origin_site_id, "remote-b");
    assert_eq!(bob.ts, 50);

    let cursor = fx.local.cursor("peer-b").unwrap().load().unwrap().unwrap();
    assert_eq!(cursor.last_seq, 0);
}

#[tokio::test]
async fn duplicate_pairing_is_rejected_without_new_rows() {
    let fx = empty_fixture("site-a", 0).await;
    insert_peer_with_snapshot(&fx.control, false, &peer("http://127.0.0.1:9"))
        .await
        .unwrap();
    let before = fx.control.site_changelog_max_seq().await.unwrap();

    let err = insert_peer_with_snapshot(&fx.control, false, &peer("http://127.0.0.1:10"))
        .await
        .unwrap_err();
    assert!(err.contains("已存在站点对"), "{err}");
    assert_eq!(fx.control.sites().await.unwrap().len(), 1);
    assert_eq!(fx.control.site_changelog_max_seq().await.unwrap(), before);
}

struct DbFx {
    _dir: tempfile::TempDir,
    db: Arc<Db>,
}

async fn object_db() -> DbFx {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let object = ObjectControl::open(store, Mode::Standalone, None)
        .await
        .unwrap();
    let db = Arc::new(Db::connect_object().await.unwrap());
    db.attach_object_control(object).unwrap();
    DbFx { _dir: dir, db }
}

#[tokio::test]
async fn db_site_ops_stay_on_object_seam_and_clean_local_files() {
    let f = object_db().await;
    let p = peer("http://127.0.0.1:9");
    f.db.insert_site(&p).await.unwrap();
    assert_eq!(f.db.count_sites().await.unwrap(), 1);
    assert_eq!(
        f.db.get_site("peer-b").await.unwrap().unwrap().created_at,
        42
    );

    // S7.4: upsert converges by peer_id and by remote_site_id, keeps
    // created_at/reconcile_required, and clears the gate only via repair.
    f.db.set_site_reconcile_required("peer-b", true)
        .await
        .unwrap();
    let mut updated = p.clone();
    updated.name = "renamed".into();
    updated.endpoint = "http://127.0.0.1:10".into();
    updated.created_at = 999;
    updated.raft_node_id = Some(9);
    updated.reconcile_required = false;
    f.db.upsert_pair_peer(&updated).await.unwrap();
    let mut other = updated.clone();
    other.peer_id = "peer-b2".into();
    f.db.upsert_pair_peer(&other).await.unwrap();
    let sites = f.db.list_sites().await.unwrap();
    assert_eq!(sites.len(), 1, "remote_site_id must converge the same row");
    assert_eq!(sites[0].peer_id, "peer-b");
    assert_eq!(sites[0].name, "renamed");
    assert_eq!(sites[0].endpoint, "http://127.0.0.1:10");
    assert_eq!(sites[0].created_at, 42);
    assert_eq!(sites[0].raft_node_id, Some(9));
    assert!(sites[0].reconcile_required);
    f.db.set_site_reconcile_required("peer-b", false)
        .await
        .unwrap();
    assert!(
        !f.db
            .get_site("peer-b")
            .await
            .unwrap()
            .unwrap()
            .reconcile_required
    );

    f.db.update_site_ping("peer-b", 123, Some("boom"))
        .await
        .unwrap();
    let got = f.db.get_site("peer-b").await.unwrap().unwrap();
    assert_eq!(got.last_seen, Some(123));
    assert_eq!(got.last_error.as_deref(), Some("boom"));
    f.db.update_site_ping("peer-b", 124, None).await.unwrap();
    let got = f.db.get_site("peer-b").await.unwrap().unwrap();
    assert_eq!(got.last_seen, Some(124));
    assert_eq!(got.last_error, None);

    let local =
        f.db.object_control()
            .unwrap()
            .control
            .local_store()
            .unwrap();
    local.cursor("peer-b").unwrap().store(7, 1).unwrap();
    local
        .dead("remote-b")
        .unwrap()
        .append(&DeadLetter {
            peer_id: "remote-b".into(),
            seq: 1,
            entity: "user".into(),
            entity_id: "alice".into(),
            error: "dead".into(),
            payload: "{}".into(),
            created_at: 1,
        })
        .unwrap();

    assert!(f.db.delete_site("peer-b").await.unwrap().is_some());
    assert!(f.db.get_site("peer-b").await.unwrap().is_none());
    assert_eq!(f.db.count_sites().await.unwrap(), 0);
    assert!(local.cursor("peer-b").unwrap().load().unwrap().is_none());
    assert_eq!(local.dead("remote-b").unwrap().count().unwrap(), 0);
    assert!(f.db.delete_site("peer-b").await.unwrap().is_none());
}

#[derive(Clone)]
struct ApplyCfg {
    control: Arc<ObjectControlStore>,
    store: Arc<Store>,
}

async fn apply_remote(State(cfg): State<ApplyCfg>, body: String) -> (StatusCode, &'static str) {
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    let entries: Vec<Entry> = serde_json::from_value(value["entries"].clone()).unwrap();
    apply_changelog(&cfg.control, &cfg.store, "site-b", &CLEANUP, &entries)
        .await
        .unwrap();
    (StatusCode::OK, "ok")
}

#[tokio::test]
async fn paired_sites_ship_snapshot_and_apply_on_peer() {
    let a = empty_fixture("site-a", 0).await;
    let b = empty_fixture("site-b", 0).await;
    {
        let mut txn = a.control.begin(WriteOrigin::local("seed")).await.unwrap();
        txn.user_create(&UserEntry {
            username: "alice".into(),
            password_hash: "hash".into(),
            status: "active".into(),
            created_at: 5,
        })
        .await
        .unwrap();
        seed_bucket(&mut txn, "bkt", "alice").await;
        txn.commit().await.unwrap();
    }

    let app = Router::new()
        .route("/api/v1/site/changelog", post(apply_remote))
        .with_state(ApplyCfg {
            control: Arc::clone(&b.control),
            store: Arc::clone(&b.store),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let p = peer(&format!("http://{addr}"));
    insert_peer_with_snapshot(&a.control, false, &p)
        .await
        .unwrap();
    let max = a.control.site_changelog_max_seq().await.unwrap();
    assert_eq!(max, 2);
    assert_eq!(ship_once(&a.control, None).await.unwrap(), (1, 2));

    assert!(b.control.user("alice").await.unwrap().is_some());
    assert!(b.store.bucket_exists("bkt").await.unwrap());
    let clock: SiteIdentityRow = b.control.site_identity().await.unwrap().unwrap();
    assert_eq!(clock.id, "site-b");
    let user_clock = b
        .control
        .site_clock("user", "alice")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user_clock.origin_site_id, "site-a");
    assert_eq!(
        a.local
            .cursor("peer-b")
            .unwrap()
            .load()
            .unwrap()
            .unwrap()
            .last_seq,
        max
    );
}
