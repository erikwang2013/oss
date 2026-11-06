//! `AclWrites`/`StsWrites` over the object root: every ported S3 write lands in
//! the control store while the placeholder SQLite pool stays schema-less.

use std::sync::Arc;

use xl_s3::access::{Acl, BucketRecordOutcome};
use xl_s3::control::{ControlAuth, KeyLookup};
use xl_store::control_store::bucket::{BucketStore, BucketTxn};
use xl_store::control_store::iam::{IamStore, IamTxn};
use xl_store::control_store::lifecycle::{LifecycleRuleEntry, LifecycleStore, LifecycleTxn};

use super::real_store;
use crate::control_store_obj::{
    AccessKeyRow, BucketRow, ObjectControlStore, TableId, WriteOrigin, decode_row,
    insert_access_key, key1,
};
use crate::control_view::HotView;
use crate::control_writes::ObjectControlWrites;

/// `set_id` is a node-local projection, absent from `BucketView`.
async fn stored_set_id(control: &ObjectControlStore, name: &str) -> i64 {
    let txn = control.begin(WriteOrigin::local("read")).await.unwrap();
    let bytes = txn
        .get(TableId::Buckets, &key1(name))
        .await
        .unwrap()
        .unwrap();
    decode_row::<BucketRow>(&bytes).unwrap().set_id
}

struct Fixture {
    _dir: tempfile::TempDir,
    control: Arc<ObjectControlStore>,
    acl: Acl,
}

async fn fixture() -> Fixture {
    let real = real_store().await;
    let writes = ObjectControlWrites::new(Arc::clone(&real.control));
    let acl = Acl::new().with_writes(Some(Arc::new(writes)));
    Fixture {
        _dir: real._dir,
        control: real.control,
        acl,
    }
}

#[tokio::test]
async fn missing_write_seam_fails_closed() {
    let acl = Acl::new();
    assert!(
        acl.set_bucket_versioning("b", "Enabled").await.is_err(),
        "无写缝必须失败，不得静默返回成功"
    );
}

#[tokio::test]
async fn lock_and_versioning_writes() {
    let fx = fixture().await;
    let before = crate::db::now();
    fx.acl.enable_bucket_lock("locked").await.unwrap();
    let after = crate::db::now();
    let row = BucketStore::bucket(fx.control.as_ref(), "locked")
        .await
        .unwrap()
        .unwrap();
    assert!(row.object_lock);
    assert_eq!(row.versioning, "Enabled");
    assert_eq!(row.owner_uid, None);
    assert_eq!(stored_set_id(fx.control.as_ref(), "locked").await, 0);
    assert!(
        (before..=after).contains(&row.creation_date),
        "creation_date {} outside [{before}, {after}]",
        row.creation_date
    );

    fx.acl
        .set_bucket_lock_rule("locked", Some("GOVERNANCE"), Some(3))
        .await
        .unwrap();
    let row = BucketStore::bucket(fx.control.as_ref(), "locked")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.object_lock_default_mode.as_deref(), Some("GOVERNANCE"));
    assert_eq!(row.object_lock_default_days, Some(3));

    fx.acl
        .set_bucket_lock_rule("locked", None, None)
        .await
        .unwrap();
    let row = BucketStore::bucket(fx.control.as_ref(), "locked")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.object_lock_default_mode, None);
    assert_eq!(row.object_lock_default_days, None);

    assert!(
        fx.acl
            .set_bucket_versioning("locked", "Suspended")
            .await
            .unwrap()
    );
    let row = BucketStore::bucket(fx.control.as_ref(), "locked")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.versioning, "Suspended");
    assert!(
        !fx.acl
            .set_bucket_versioning("missing", "Enabled")
            .await
            .unwrap()
    );

    // Lock on an existing row keeps the owner (upsert touches lock/versioning only).
    fx.acl.record_bucket_owner("owned", 7).await.unwrap();
    fx.acl.enable_bucket_lock("owned").await.unwrap();
    let row = BucketStore::bucket(fx.control.as_ref(), "owned")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.owner_uid, Some(7));
    assert!(row.object_lock);
    assert_eq!(row.versioning, "Enabled");
}

#[tokio::test]
async fn bucket_config_writes() {
    let fx = fixture().await;
    fx.acl.record_bucket_owner("cfg", 1).await.unwrap();

    assert!(
        fx.acl
            .set_bucket_default_encryption("cfg", "AES256")
            .await
            .unwrap()
    );
    assert_eq!(
        BucketStore::bucket(fx.control.as_ref(), "cfg")
            .await
            .unwrap()
            .unwrap()
            .default_encryption
            .as_deref(),
        Some("AES256")
    );
    assert!(fx.acl.clear_bucket_default_encryption("cfg").await.unwrap());
    assert_eq!(
        BucketStore::bucket(fx.control.as_ref(), "cfg")
            .await
            .unwrap()
            .unwrap()
            .default_encryption,
        None
    );
    assert!(
        !fx.acl
            .set_bucket_default_encryption("missing", "AES256")
            .await
            .unwrap()
    );
    assert!(
        !fx.acl
            .clear_bucket_default_encryption("missing")
            .await
            .unwrap()
    );

    fx.acl
        .set_bucket_notification("cfg", r#"{"queue":[]}"#)
        .await
        .unwrap();
    assert_eq!(
        BucketStore::bucket_notification(fx.control.as_ref(), "cfg")
            .await
            .unwrap()
            .unwrap()
            .config_json,
        r#"{"queue":[]}"#
    );
    fx.acl.clear_bucket_notification("cfg").await.unwrap();
    assert!(
        BucketStore::bucket_notification(fx.control.as_ref(), "cfg")
            .await
            .unwrap()
            .is_none()
    );

    // Seed a policy and a lifecycle rule, then clear both through the seam.
    let mut txn = fx.control.begin(WriteOrigin::local("seed")).await.unwrap();
    BucketTxn::bucket_policy_set(&mut txn, "cfg", "{}", 1)
        .await
        .unwrap();
    LifecycleTxn::replace_lifecycle_rules(
        &mut txn,
        "cfg",
        &[LifecycleRuleEntry {
            bucket: "cfg".into(),
            prefix: "logs/".into(),
            rule_id: Some("r1".into()),
            expire_days: 7,
            expiration_days: Some(7),
            transition_days: None,
            transition_class: None,
            transition_tier: None,
            enabled: true,
            updated_at: 1,
            config_json: None,
        }],
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert!(
        BucketStore::bucket_policy(fx.control.as_ref(), "cfg")
            .await
            .unwrap()
            .is_some()
    );

    fx.acl.clear_bucket_policy("cfg").await.unwrap();
    assert!(
        BucketStore::bucket_policy(fx.control.as_ref(), "cfg")
            .await
            .unwrap()
            .is_none()
    );
    fx.acl.clear_lifecycle_rules("cfg").await.unwrap();
    assert!(
        LifecycleStore::lifecycle_rules(fx.control.as_ref(), "cfg")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn owner_set_and_forget_writes() {
    let fx = fixture().await;
    fx.acl.record_bucket_owner("o1", 5).await.unwrap();
    let row = BucketStore::bucket(fx.control.as_ref(), "o1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.owner_uid, Some(5));
    assert_eq!(row.versioning, "Off");
    assert_eq!(stored_set_id(fx.control.as_ref(), "o1").await, 0);
    assert_eq!(row.replication_mode, "async");
    // INSERT OR IGNORE: an existing row keeps its owner.
    fx.acl.record_bucket_owner("o1", 9).await.unwrap();
    assert_eq!(
        BucketStore::bucket(fx.control.as_ref(), "o1")
            .await
            .unwrap()
            .unwrap()
            .owner_uid,
        Some(5)
    );

    assert_eq!(
        fx.acl
            .record_bucket_owner_set_mode("o2", 8, 2, "sync")
            .await
            .unwrap(),
        BucketRecordOutcome::Written
    );
    let first = BucketStore::bucket(fx.control.as_ref(), "o2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.owner_uid, Some(8));
    assert_eq!(stored_set_id(fx.control.as_ref(), "o2").await, 2);
    assert_eq!(first.versioning, "Enabled");
    assert_eq!(first.replication_mode, "sync");

    // Re-record converges set/owner/versioning but keeps creation_date.
    fx.acl
        .record_bucket_owner_set_mode("o2", 8, 1, "async")
        .await
        .unwrap();
    let second = BucketStore::bucket(fx.control.as_ref(), "o2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_set_id(fx.control.as_ref(), "o2").await, 1);
    assert_eq!(second.versioning, "Off");
    assert_eq!(second.creation_date, first.creation_date);

    // sets: o1→0, o2→1; ties keep the lowest index, so set 2 is least loaded.
    assert_eq!(fx.acl.pick_least_loaded_set(3).await.unwrap(), 2);
    fx.acl
        .record_bucket_owner_set_mode("o3", 8, 2, "async")
        .await
        .unwrap();
    assert_eq!(fx.acl.pick_least_loaded_set(3).await.unwrap(), 0);
    assert_eq!(fx.acl.pick_least_loaded_set(1).await.unwrap(), 0);

    let mut txn = fx.control.begin(WriteOrigin::local("seed")).await.unwrap();
    IamTxn::permission_upsert(&mut txn, 5, "o1", true, true, false)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        IamStore::permissions_for_user(fx.control.as_ref(), 5)
            .await
            .unwrap()
            .len(),
        1
    );
    fx.acl.forget_bucket("o1").await.unwrap();
    assert!(
        BucketStore::bucket(fx.control.as_ref(), "o1")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        IamStore::permissions_for_user(fx.control.as_ref(), 5)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn sts_session_lands_in_root_with_parent_chain_and_expiry() {
    let fx = fixture().await;
    let mut txn = fx.control.begin(WriteOrigin::local("seed")).await.unwrap();
    let uid = IamTxn::user_create_default_status(&mut txn, "alice", "hash", false, 100)
        .await
        .unwrap();
    insert_access_key(
        &mut txn,
        &AccessKeyRow {
            access_key: "AKIA_PARENT".into(),
            secret_key: "parent-secret".into(),
            user_id: uid,
            status: "active".into(),
            expires_at: None,
            created_at: 100,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    let writes = ObjectControlWrites::new(Arc::clone(&fx.control));
    let now = 1_000;
    xl_s3::control::StsWrites::issue_session(
        &writes,
        "ASIA_SESSION",
        "sealed-secret",
        uid,
        now + 900,
        now,
        Some("AKIA_PARENT"),
        Some(r#"{"Version":"2012-10-17"}"#),
    )
    .await
    .unwrap();

    let view = HotView::load(fx.control.as_ref()).await.unwrap().capture();
    match ControlAuth::key_auth(&view, "ASIA_SESSION", now)
        .await
        .unwrap()
    {
        KeyLookup::Valid(auth) => {
            assert_eq!(auth.uid, uid);
            assert!(!auth.is_admin);
            assert_eq!(auth.secret_key, "sealed-secret");
        }
        other => panic!("expected a valid session, got {other:?}"),
    }
    assert_eq!(
        ControlAuth::key_auth(&view, "ASIA_SESSION", now + 901)
            .await
            .unwrap(),
        KeyLookup::ExpiredSession
    );

    // A session whose parent chain is missing must never authenticate.
    xl_s3::control::StsWrites::issue_session(
        &writes,
        "ASIA_ORPHAN",
        "sealed-secret",
        uid,
        now + 900,
        now,
        Some("AKIA_MISSING"),
        None,
    )
    .await
    .unwrap();
    let view = HotView::load(fx.control.as_ref()).await.unwrap().capture();
    assert_eq!(
        ControlAuth::key_auth(&view, "ASIA_ORPHAN", now)
            .await
            .unwrap(),
        KeyLookup::Invalid
    );
}
