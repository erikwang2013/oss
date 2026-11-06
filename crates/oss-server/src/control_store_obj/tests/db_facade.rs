//! G3/H1: `Db` façade writers must reach the object seam or the node-local
//! store, never the placeholder SQLite pool. Each assertion reads back through
//! `control_store()` / the local store, so a regression to `self.pool` fails
//! with "no such table" or a missing row.

use std::sync::Arc;

use xl_store::control_state::root::Mode;
use xl_store::control_store::audit::{AuditTxn, NewAuditRecord};
use xl_store::store::{Store, StoreConfig};

use crate::control_backend::ObjectControl;
use crate::control_store_obj::{BucketRow, TableId, decode_row, key1};
use crate::db::{AuditFilter, BucketRowOutcome, Db};

struct Fixture {
    _dir: tempfile::TempDir,
    db: Arc<Db>,
}

async fn object_db() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let object = ObjectControl::open(store, Mode::Standalone, None)
        .await
        .unwrap();
    let db = Arc::new(Db::connect_object().await.unwrap());
    db.attach_object_control(object).unwrap();
    Fixture { _dir: dir, db }
}

async fn bucket_set_id(db: &Db, name: &str) -> i64 {
    let name = name.to_owned();
    let raw = db
        .object_local_write("test", move |txn| {
            Box::pin(async move {
                txn.get(TableId::Buckets, &key1(&name))
                    .await
                    .map_err(|e| xl_store::control_store::ControlError::Backend(e.to_string()))
            })
        })
        .await
        .unwrap()
        .expect("bucket row must exist");
    decode_row::<BucketRow>(&raw).unwrap().set_id
}

fn legacy_hash(password: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"oss-m1-salt:");
    h.update(password.as_bytes());
    hex::encode(h.finalize())
}

#[tokio::test]
async fn object_db_user_writers_and_delete_cascade() {
    let f = object_db().await;
    let db = &*f.db;
    let uid = db.create_user("alice", "old-pw-123", false).await.unwrap();

    db.create_access_key("AKIA_FACADE", "SK_FACADE", uid)
        .await
        .unwrap();
    assert_eq!(
        db.control_store().access_keys_for_user(uid).await.unwrap(),
        vec!["AKIA_FACADE"]
    );

    assert!(db.set_password(uid, "new-pw-123").await.unwrap());
    assert_eq!(
        db.verify_user("alice", "new-pw-123").await.unwrap(),
        Some(uid)
    );

    // Legacy hash is upgraded through the object seam (root marker mode is
    // standalone); raft object roots must keep the hash untouched.
    let legacy = legacy_hash("legacy-pw");
    db.control_write("test", move |txn| {
        let legacy = legacy.clone();
        Box::pin(async move { txn.user_set_password(uid, &legacy).await.map(|_| ()) })
    })
    .await
    .unwrap();
    assert_eq!(
        db.verify_user("alice", "legacy-pw").await.unwrap(),
        Some(uid)
    );
    let hash = db
        .control_store()
        .login_user("alice")
        .await
        .unwrap()
        .unwrap()
        .password_hash;
    assert!(hash.starts_with("$argon2"), "legacy hash must be upgraded");

    assert!(db.set_user_status(uid, false).await.unwrap());
    assert!(!db.get_user(uid).await.unwrap().unwrap().2);
    assert!(db.set_user_status(uid, true).await.unwrap());
    assert!(db.get_user(uid).await.unwrap().unwrap().2);

    // Delete cascade: access keys, bucket owner, local upload owner.
    db.insert_bucket_full("owned", Some(uid), 0, "async")
        .await
        .unwrap();
    db.record_upload("p1", "owned", "k", "g", 1, "e")
        .await
        .unwrap();
    assert!(db.register_upload("p1", "name", uid, false).await.unwrap());
    assert!(db.delete_user(uid).await.unwrap());
    assert!(db.get_user(uid).await.unwrap().is_none());
    assert!(
        db.control_store()
            .access_keys_for_user(uid)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.control_store()
            .bucket("owned")
            .await
            .unwrap()
            .unwrap()
            .owner,
        Some(xl_store::control_store::OwnerRef::Admin),
        "NULL owner_id projects to the admin owner"
    );
    let upload = db
        .object_control()
        .unwrap()
        .control
        .local_store()
        .unwrap()
        .uploads()
        .unwrap()
        .get("p1")
        .unwrap()
        .unwrap();
    assert_eq!(upload.owner_uid, None);
    assert!(
        !db.delete_user(uid).await.unwrap(),
        "missing user is a no-op"
    );

    // Last active admin cannot be deleted.
    let admin = db.create_user("root", "root-pw-123", true).await.unwrap();
    assert!(!db.delete_user(admin).await.unwrap());
}

#[tokio::test]
async fn object_db_account_writers_roundtrip() {
    let f = object_db().await;
    let db = &*f.db;
    let id = db.create_account("acme", Some(5)).await.unwrap();
    assert_eq!(id.len(), 12);
    assert!(id.bytes().all(|b| b.is_ascii_digit()));

    let acct = db.control_store().account(&id).await.unwrap().unwrap();
    assert_eq!(acct.name, "acme");
    assert_eq!(acct.bucket_quota, Some(5));

    assert!(db.rename_account(&id, "acme-2").await.unwrap());
    assert_eq!(
        db.control_store().account(&id).await.unwrap().unwrap().name,
        "acme-2"
    );

    assert!(db.set_account_quota(&id, Some(9)).await.unwrap());
    assert_eq!(
        db.control_store().account_bucket_quota(&id).await.unwrap(),
        Some(("active".into(), Some(9)))
    );

    assert!(
        db.set_account_hard_limits(&id, Some(1000), Some(7))
            .await
            .unwrap()
    );
    assert_eq!(
        db.control_store().account_hard_limits(&id).await.unwrap(),
        Some((Some(1000), Some(7)))
    );

    assert!(db.set_account_status(&id, "disabled").await.unwrap());
    assert_eq!(
        db.control_store().account_status(&id).await.unwrap(),
        Some("disabled".into())
    );
    assert!(db.set_account_status(&id, "active").await.unwrap());

    // Membership: global admins are never attachable; a regular user moves
    // from its derived account and becomes the account admin.
    let bob = db.create_user("bob", "bob-pw-123", false).await.unwrap();
    assert!(
        !db.attach_account_member(&id, 999_999, "member")
            .await
            .unwrap()
    );
    let admin = db.create_user("root", "root-pw-123", true).await.unwrap();
    assert!(
        !db.attach_account_member(&id, admin, "member")
            .await
            .unwrap()
    );
    assert!(db.attach_account_member(&id, bob, "admin").await.unwrap());
    assert_eq!(
        db.member_role(&id, bob).await.unwrap().as_deref(),
        Some("admin")
    );
    assert_eq!(
        db.control_store()
            .account(&id)
            .await
            .unwrap()
            .unwrap()
            .admin_uid,
        Some(bob)
    );

    let carol = db
        .create_user_in_account(&id, "carol", "carol-pw-123", "member")
        .await
        .unwrap();
    assert_eq!(
        db.control_store()
            .user_by_id(carol)
            .await
            .unwrap()
            .unwrap()
            .username,
        "carol"
    );
    assert_eq!(
        db.member_role(&id, carol).await.unwrap().as_deref(),
        Some("member")
    );

    // Non-empty account is not deletable; empty account is.
    assert!(!db.delete_account(&id).await.unwrap());
    let empty = db.create_account("empty", None).await.unwrap();
    assert!(db.delete_account(&empty).await.unwrap());
    assert!(db.control_store().account(&empty).await.unwrap().is_none());
    assert!(
        !db.delete_account(xl_s3::access::LEGACY_ACCOUNT)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn object_db_bucket_row_and_config_writers() {
    let f = object_db().await;
    let db = &*f.db;
    let uid = db
        .create_user("alice", "alice-pw-123", false)
        .await
        .unwrap();

    db.insert_bucket_full("b1", Some(uid), 2, "sync")
        .await
        .unwrap();
    let view = db.control_store().bucket("b1").await.unwrap().unwrap();
    assert_eq!(
        view.owner,
        Some(xl_store::control_store::OwnerRef::User("alice".into()))
    );
    assert_eq!(view.replication_mode, "sync");
    // INSERT OR IGNORE keeps the legacy "Off" default even for sync mode.
    assert_eq!(view.versioning, "Off");
    assert_eq!(bucket_set_id(db, "b1").await, 2);

    assert!(matches!(
        db.upsert_bucket_owner_set_mode("b1", None, 3, "async")
            .await
            .unwrap(),
        BucketRowOutcome::Written
    ));
    let view = db.control_store().bucket("b1").await.unwrap().unwrap();
    assert_eq!(view.owner, Some(xl_store::control_store::OwnerRef::Admin));
    assert_eq!(view.replication_mode, "async");
    assert_eq!(view.versioning, "Off");
    assert_eq!(bucket_set_id(db, "b1").await, 3);

    db.upsert_bucket_set("b1", 5).await.unwrap();
    assert_eq!(bucket_set_id(db, "b1").await, 5);

    db.set_bucket_deleting("b1", true).await.unwrap();
    assert!(
        db.bucket_sync_state_row("b1")
            .await
            .unwrap()
            .is_some_and(|v| v.1)
    );
    db.set_bucket_deleting("b1", false).await.unwrap();

    assert!(
        db.set_bucket_default_encryption("b1", "AES256")
            .await
            .unwrap()
    );
    assert_eq!(
        db.get_bucket_default_encryption("b1").await.unwrap(),
        Some("AES256".into())
    );
    assert!(
        db.list_bucket_default_encryptions()
            .await
            .unwrap()
            .contains(&("b1".into(), "AES256".into()))
    );
    assert!(db.clear_bucket_default_encryption("b1").await.unwrap());
    assert_eq!(db.get_bucket_default_encryption("b1").await.unwrap(), None);

    db.set_bucket_policy("b1", r#"{"Statement":[]}"#, 1)
        .await
        .unwrap();
    assert_eq!(
        db.bucket_policy("b1").await.unwrap().as_deref(),
        Some(r#"{"Statement":[]}"#)
    );
    db.clear_bucket_policy("b1").await.unwrap();
    assert!(db.bucket_policy("b1").await.unwrap().is_none());

    // Quota gates only new rows; existing rows converge.
    let account = xl_s3::access::account_id_of(uid).unwrap();
    assert!(db.set_account_quota(&account, Some(0)).await.unwrap());
    assert!(matches!(
        db.upsert_bucket_owner_set_mode("fresh", Some(uid), 0, "async")
            .await
            .unwrap(),
        BucketRowOutcome::QuotaExceeded
    ));
    assert!(matches!(
        db.upsert_bucket_owner_set_mode("b1", None, 6, "async")
            .await
            .unwrap(),
        BucketRowOutcome::Written
    ));
}

#[tokio::test]
async fn object_db_schema_meta_site_and_audit_facades() {
    let f = object_db().await;
    let db = &*f.db;

    db.schema_meta_set("k", "v1").await.unwrap();
    assert_eq!(db.schema_meta("k").await.unwrap().as_deref(), Some("v1"));
    assert!(!db.schema_meta_set_if_absent("k", "v2").await.unwrap());
    assert!(db.schema_meta_set_if_absent("new", "v2").await.unwrap());
    db.upsert_schema_meta("k", "v3").await.unwrap();
    assert_eq!(db.schema_meta("k").await.unwrap().as_deref(), Some("v3"));

    db.apply_site_config(Some("edge"), Some("cn"), Some(7), None)
        .await
        .unwrap();
    let id = db.site_identity().await.unwrap();
    assert_eq!(id.name, "edge");
    assert_eq!(id.region, "cn");
    assert_eq!(id.priority, 7);

    db.object_local_write("test", |txn| {
        Box::pin(async move {
            txn.audit_insert_batch(&[NewAuditRecord {
                ts: 100,
                uid: 7,
                op: "GetObject".into(),
                bucket: Some("b".into()),
                object_key: Some("k".into()),
                decision: "allow".into(),
                detail: None,
            }])
            .await
        })
    })
    .await
    .unwrap();
    let rows = db
        .list_audit(&AuditFilter {
            limit: 10,
            decision: None,
            from: None,
            to: None,
            uid: Some(7),
            op: Some("GetObject"),
            bucket: None,
            key_prefix: None,
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["bucket"], "b");
}
