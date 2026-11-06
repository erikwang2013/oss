//! Object-backend site apply tests: LWW, five entities, clocks, dead letters,
//! idempotent replay, and physical bucket reconciliation.

use std::sync::Arc;

use tempfile::TempDir;
use xl_s3::bucket_cleanup::BucketCleanupSink;
use xl_store::control_state::root::Mode;
use xl_store::control_store::bucket::BucketStore;
use xl_store::control_store::iam::IamStore;
use xl_store::control_store::sites::{SitePeerRow, SiteStore, SiteTxn};
use xl_store::store::{Store, StoreConfig};

use crate::control_store_obj::{ObjectControlStore, RootIo, WriteOrigin};
use crate::local_store::LocalStore;
use crate::site::{ApplyStats, Entry};

mod pair;
mod ship;

struct Noop;

#[async_trait::async_trait]
impl BucketCleanupSink for Noop {
    async fn clear_bucket_m8(&self, _bucket: &str) -> Result<(), String> {
        Ok(())
    }
    fn invalidate_ap_cache(&self, _bucket: &str) {}
}

static CLEANUP: Noop = Noop;

pub(super) struct Fx {
    pub _dir: TempDir,
    pub store: Arc<Store>,
    pub control: Arc<ObjectControlStore>,
    pub local: LocalStore,
}

pub(super) async fn empty_fixture(site_id: &str, priority: i64) -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let local = LocalStore::open(dir.path().join("local")).unwrap();
    let control = Arc::new(
        ObjectControlStore::new(Arc::new(RootIo::new(Arc::clone(&store))))
            .with_local_store(local.clone()),
    );
    control.bootstrap(Mode::Standalone, 1).await.unwrap();

    let mut txn = control.begin(WriteOrigin::local("seed")).await.unwrap();
    txn.site_config_seed(site_id, "tok", 1).await.unwrap();
    txn.site_config_apply(None, None, Some(priority), None)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    Fx {
        _dir: dir,
        store,
        control,
        local,
    }
}

pub(super) async fn fixture(endpoint: &str) -> Fx {
    let fx = empty_fixture("site-a", 0).await;
    let peer = SitePeerRow {
        peer_id: "peer-b".into(),
        remote_site_id: "remote-b".into(),
        name: "b".into(),
        endpoint: endpoint.into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "ak".into(),
        s3_secret_key: "sk".into(),
        peer_site_token: "tok".into(),
        created_at: 0,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    };
    let mut txn = fx.control.begin(WriteOrigin::local("seed")).await.unwrap();
    txn.site_insert(&peer, 0).await.unwrap();
    txn.commit().await.unwrap();
    fx
}

fn entry(seq: i64, entity: &str, id: &str, op: &str, payload: serde_json::Value, ts: i64) -> Entry {
    Entry {
        seq,
        origin_site_id: "remote-b".into(),
        entity: entity.into(),
        entity_id: id.into(),
        op: op.into(),
        payload,
        priority: 0,
        ts,
    }
}

fn win(seq: i64, entity: &str, id: &str, payload: serde_json::Value) -> Entry {
    entry(seq, entity, id, "upsert", payload, 100)
}

fn user_entry(seq: i64) -> Entry {
    win(
        seq,
        "user",
        "alice",
        serde_json::json!({
            "username": "alice",
            "password_hash": "hash",
            "status": "active",
            "created_at": 5,
        }),
    )
}

fn ak_entry(seq: i64) -> Entry {
    win(
        seq,
        "access_key",
        "AKALICE000000000001",
        serde_json::json!({
            "access_key": "AKALICE000000000001",
            "secret_key": "secret",
            "username": "alice",
            "status": "active",
            "expires_at": null,
            "created_at": 5,
        }),
    )
}

fn perm_entry(seq: i64) -> Entry {
    win(
        seq,
        "permission",
        "alice\u{1f}bkt",
        serde_json::json!({
            "username": "alice",
            "bucket": "bkt",
            "can_read": 1,
            "can_write": 0,
            "can_delete": 0,
        }),
    )
}

fn bucket_entry(seq: i64) -> Entry {
    win(
        seq,
        "bucket",
        "bkt",
        serde_json::json!({
            "name": "bkt",
            "owner_username": "alice",
            "creation_date": 5,
            "versioning": "Enabled",
            "object_lock": 0,
        }),
    )
}

fn policy_entry(seq: i64) -> Entry {
    win(
        seq,
        "bucket_policy",
        "bkt",
        serde_json::json!({
            "bucket": "bkt",
            "policy_json": r#"{"Version":"2012-10-17","Statement":[]}"#,
            "updated_at": 5,
        }),
    )
}

fn all_wins() -> Vec<Entry> {
    vec![
        user_entry(1),
        ak_entry(2),
        perm_entry(3),
        bucket_entry(4),
        policy_entry(5),
    ]
}

async fn apply(fx: &Fx, entries: &[Entry]) -> ApplyStats {
    super::apply_changelog(&fx.control, &fx.store, "site-a", &CLEANUP, entries)
        .await
        .unwrap()
}

async fn alice_uid(fx: &Fx) -> i64 {
    fx.control.user("alice").await.unwrap().unwrap().uid
}

#[tokio::test]
async fn winning_upserts_apply_all_five_entities_and_store_clocks() {
    let fx = fixture("http://127.0.0.1:9").await;
    let stats = apply(&fx, &all_wins()).await;
    assert_eq!((stats.applied, stats.skipped, stats.dead), (5, 0, 0));

    let uid = alice_uid(&fx).await;
    assert_eq!(
        fx.control.access_keys_for_user(uid).await.unwrap(),
        vec!["AKALICE000000000001".to_string()]
    );
    let perms = fx.control.permissions_for_user(uid).await.unwrap();
    assert_eq!(perms.len(), 1);
    assert_eq!((perms[0].bucket.as_str(), perms[0].read), ("bkt", true));

    let bucket = fx.control.bucket("bkt").await.unwrap().unwrap();
    assert_eq!(bucket.versioning, "Enabled");
    assert_eq!(bucket.owner_uid, Some(uid));
    assert!(fx.store.bucket_exists("bkt").await.unwrap());
    assert_eq!(
        fx.control
            .bucket_policy("bkt")
            .await
            .unwrap()
            .unwrap()
            .policy_json,
        r#"{"Version":"2012-10-17","Statement":[]}"#
    );

    for (entity, id, seq) in [
        ("user", "alice", 1),
        ("access_key", "AKALICE000000000001", 2),
        ("permission", "alice\u{1f}bkt", 3),
        ("bucket", "bkt", 4),
        ("bucket_policy", "bkt", 5),
    ] {
        let clock = fx.control.site_clock(entity, id).await.unwrap().unwrap();
        assert_eq!((clock.ts, clock.seq), (100, seq), "{entity}");
        assert_eq!(clock.origin_site_id, "remote-b");
    }
}

#[tokio::test]
async fn winning_deletes_apply_all_five_entities() {
    let fx = fixture("http://127.0.0.1:9").await;
    apply(&fx, &all_wins()).await;
    let uid = alice_uid(&fx).await;

    // User last: its delete cascades any remaining AK/permission rows.
    let deletes = vec![
        entry(
            6,
            "access_key",
            "AKALICE000000000001",
            "delete",
            serde_json::json!({}),
            200,
        ),
        entry(
            7,
            "permission",
            "alice\u{1f}bkt",
            "delete",
            serde_json::json!({"username": "alice", "bucket": "bkt"}),
            200,
        ),
        entry(
            8,
            "bucket_policy",
            "bkt",
            "delete",
            serde_json::json!({"bucket": "bkt"}),
            200,
        ),
        entry(
            9,
            "bucket",
            "bkt",
            "delete",
            serde_json::json!({"name": "bkt"}),
            200,
        ),
        entry(
            10,
            "user",
            "alice",
            "delete",
            serde_json::json!({"username": "alice"}),
            200,
        ),
    ];
    let stats = apply(&fx, &deletes).await;
    assert_eq!((stats.applied, stats.skipped, stats.dead), (5, 0, 0));

    assert!(fx.control.user("alice").await.unwrap().is_none());
    assert!(fx.control.bucket("bkt").await.unwrap().is_none());
    assert!(fx.control.bucket_policy("bkt").await.unwrap().is_none());
    assert!(
        fx.control
            .access_keys_for_user(uid)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!fx.store.bucket_exists("bkt").await.unwrap());
}

#[tokio::test]
async fn losing_entry_is_skipped_without_mutation_or_clock_change() {
    let fx = fixture("http://127.0.0.1:9").await;
    apply(&fx, &all_wins()).await;
    // Create-then-remove physical bucket, so the losing entry also exercises R9
    // reconciliation (row survives; physical must be recreated after the CAS).
    fx.store.delete_bucket("bkt").await.unwrap();
    assert!(!fx.store.bucket_exists("bkt").await.unwrap());

    let mut losing = bucket_entry(6);
    losing.ts = 50;
    losing.payload = serde_json::json!({
        "name": "bkt",
        "owner_username": "alice",
        "creation_date": 1,
        "versioning": "Off",
        "object_lock": 1,
    });
    let stats = apply(&fx, &[losing]).await;
    assert_eq!((stats.applied, stats.skipped, stats.dead), (0, 1, 0));
    let bucket = fx.control.bucket("bkt").await.unwrap().unwrap();
    assert_eq!(bucket.versioning, "Enabled");
    assert!(!bucket.object_lock);
    let clock = fx
        .control
        .site_clock("bucket", "bkt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((clock.ts, clock.seq), (100, 4));
    assert!(fx.store.bucket_exists("bkt").await.unwrap());
}

#[tokio::test]
async fn bad_entries_dead_letter_and_set_peer_error() {
    let fx = fixture("http://127.0.0.1:9").await;
    let now = crate::db::now();
    let huge = "x".repeat(257 * 1024);
    let entries = vec![
        entry(
            11,
            "user",
            "u1",
            "replace",
            serde_json::json!({"username": "u1"}),
            100,
        ),
        win(12, "mystery", "m1", serde_json::json!({"x": 1})),
        win(
            13,
            "user",
            "u2",
            serde_json::json!({"username": "u2", "password_hash": huge}),
        ),
        entry(
            14,
            "user",
            "u3",
            "upsert",
            serde_json::json!({"username": "u3", "password_hash": "h"}),
            now + 400,
        ),
    ];
    let stats = apply(&fx, &entries).await;
    assert_eq!((stats.applied, stats.skipped, stats.dead), (0, 0, 4));

    let dead = fx.local.dead("remote-b").unwrap().scan().unwrap();
    let reasons: Vec<&str> = dead.iter().map(|d| d.error.as_str()).collect();
    assert_eq!(
        reasons,
        vec![
            "未知 op",
            "未知 entity：mystery",
            "payload 超限",
            "ts 超时钟偏移上限",
        ]
    );
    assert_eq!(
        dead[2].payload.len(),
        crate::site::MAX_DEAD_PAYLOAD,
        "dead payload is truncated at a UTF-8 boundary"
    );
    assert!(fx.control.user("u1").await.unwrap().is_none());
    assert!(fx.control.user("u3").await.unwrap().is_none());
    let peer = fx.control.site("peer-b").await.unwrap().unwrap();
    assert_eq!(peer.last_error.as_deref(), Some("ts 超时钟偏移上限"));
}

#[tokio::test]
async fn duplicate_peer_seq_is_idempotent() {
    let fx = fixture("http://127.0.0.1:9").await;
    let first = apply(&fx, &[user_entry(21)]).await;
    let again = apply(&fx, &[user_entry(21)]).await;
    assert_eq!((first.applied, again.applied, again.skipped), (1, 0, 1));
    assert_eq!(fx.control.users().await.unwrap().len(), 1);
    let clock = fx
        .control
        .site_clock("user", "alice")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((clock.ts, clock.seq), (100, 21));

    let bad = entry(
        22,
        "user",
        "x",
        "nope",
        serde_json::json!({"username": "x"}),
        100,
    );
    apply(&fx, std::slice::from_ref(&bad)).await;
    apply(&fx, &[bad]).await;
    assert_eq!(fx.local.dead("remote-b").unwrap().count().unwrap(), 1);
}

#[tokio::test]
async fn same_site_entries_are_skipped() {
    let fx = fixture("http://127.0.0.1:9").await;
    let mut own = user_entry(30);
    own.origin_site_id = "site-a".into();
    let stats = apply(&fx, &[own]).await;
    assert_eq!((stats.applied, stats.skipped), (0, 1));
    assert!(fx.control.user("alice").await.unwrap().is_none());
}
