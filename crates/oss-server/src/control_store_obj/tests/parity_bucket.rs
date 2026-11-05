//! Bucket domain parity: the same scenario runs through the SQLite adapter and
//! the object backend; full rows, ordering, owner scopes and snapshot bytes must
//! match.

use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{
    BucketCreateOutcome, BucketEntry, BucketSnapshotTxn, BucketStore, BucketTxn,
};
use xl_store::error::Error as StoreError;

use super::super::TableId;
use super::super::bucket::{BucketRow, decode_row, encode_row, key1};
use super::local;
use super::parity_util::{Parity, err, object, parity, sqlite};

#[derive(Debug, PartialEq)]
struct Writes {
    creates: Vec<BucketCreateOutcome>,
    flags: Vec<bool>,
}

fn entry(
    name: &str,
    owner: OwnerRef,
    creation_date: i64,
    versioning: &str,
    object_lock: bool,
    deleting: bool,
) -> BucketEntry {
    BucketEntry {
        name: name.into(),
        owner,
        creation_date,
        versioning: versioning.into(),
        object_lock,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting,
    }
}

async fn write_scenario<T: BucketTxn>(txn: &mut T) -> Result<Writes, StoreError> {
    let mut creates = Vec::new();
    creates.push(
        txn.bucket_create(&entry(
            "b1",
            OwnerRef::User("alice".into()),
            100,
            "Enabled",
            false,
            false,
        ))
        .await
        .map_err(err)?,
    );
    // Same mode: update path keeps creation_date and set_id, clears deleting.
    creates.push(
        txn.bucket_create(&entry(
            "b1",
            OwnerRef::User("alice".into()),
            999,
            "Suspended",
            true,
            true,
        ))
        .await
        .map_err(err)?,
    );
    creates.push(
        txn.bucket_create(&entry("b2", OwnerRef::Admin, 101, "Off", false, false))
            .await
            .map_err(err)?,
    );
    creates.push(
        txn.bucket_create(&entry(
            "b3",
            OwnerRef::User("root".into()),
            102,
            "Off",
            false,
            false,
        ))
        .await
        .map_err(err)?,
    );
    creates.push(
        txn.bucket_create(&entry(
            "b4",
            OwnerRef::User("bob".into()),
            103,
            "Off",
            false,
            false,
        ))
        .await
        .map_err(err)?,
    );
    let mut conflict = entry("b2", OwnerRef::Admin, 999, "Enabled", true, true);
    conflict.replication_mode = "sync".into();
    creates.push(txn.bucket_create(&conflict).await.map_err(err)?);

    txn.bucket_policy_set("b1", r#"{"p":1}"#, 10)
        .await
        .map_err(err)?;
    txn.bucket_policy_set("b2", "{}", 11).await.map_err(err)?;
    txn.bucket_notification_set("b1", r#"{"e":1}"#, 12)
        .await
        .map_err(err)?;
    txn.bucket_replication_rules_set("b1", r#"{"r":1}"#, 13)
        .await
        .map_err(err)?;

    let mut flags = Vec::new();
    flags.push(
        txn.bucket_set_versioning("b1", "Enabled")
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.bucket_set_object_lock("b3", true, Some("GOVERNANCE"), Some(7))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.bucket_set_default_encryption("b4", "AES256")
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.bucket_clear_default_encryption("b4")
            .await
            .map_err(err)?,
    );
    flags.push(txn.bucket_set_deleting("b3", true).await.map_err(err)?);
    flags.push(
        txn.bucket_set_deleting("missing", true)
            .await
            .map_err(err)?,
    );
    txn.bucket_policy_delete("b2").await.map_err(err)?;
    txn.bucket_replication_rules_delete("b1")
        .await
        .map_err(err)?;
    flags.push(txn.bucket_exists("b1").await.map_err(err)?);
    flags.push(txn.bucket_exists("b9").await.map_err(err)?);
    Ok(Writes { creates, flags })
}

async fn assert_reads(p: &Parity, names: &[&str]) {
    let sq = p.db.control_store();
    let ob = p.real.control.as_ref();
    assert_eq!(sq.buckets().await.unwrap(), ob.buckets().await.unwrap());
    assert_eq!(
        sq.bucket_policies().await.unwrap(),
        ob.bucket_policies().await.unwrap()
    );
    assert_eq!(
        sq.event_configs().await.unwrap(),
        ob.event_configs().await.unwrap()
    );
    assert_eq!(
        sq.replication_rules().await.unwrap(),
        ob.replication_rules().await.unwrap()
    );
    for name in names {
        assert_eq!(
            sq.bucket(name).await.unwrap(),
            ob.bucket(name).await.unwrap(),
            "bucket {name}"
        );
        assert_eq!(
            sq.bucket_policy(name).await.unwrap(),
            ob.bucket_policy(name).await.unwrap(),
            "policy {name}"
        );
        assert_eq!(
            sq.bucket_notification(name).await.unwrap(),
            ob.bucket_notification(name).await.unwrap(),
            "notification {name}"
        );
        assert_eq!(
            sq.bucket_replication_rules(name).await.unwrap(),
            ob.bucket_replication_rules(name).await.unwrap(),
            "replication {name}"
        );
    }
}

#[tokio::test]
async fn bucket_writes_and_reads_match() {
    let p = parity(&[("root", true), ("alice", false), ("bob", false)]).await;
    let sq = sqlite(&p, |t| Box::pin(write_scenario(t))).await.unwrap();
    let ob = object(&p, |t| Box::pin(write_scenario(t))).await.unwrap();
    assert_eq!(sq, ob);
    assert_eq!(
        sq.creates,
        vec![
            BucketCreateOutcome::Created,
            BucketCreateOutcome::Updated,
            BucketCreateOutcome::Created,
            BucketCreateOutcome::Created,
            BucketCreateOutcome::Created,
            BucketCreateOutcome::ModeConflict,
        ]
    );
    assert_eq!(
        sq.flags,
        vec![true, true, true, true, true, false, true, false]
    );

    assert_reads(&p, &["b1", "b2", "b3", "b4", "b9"]).await;
    let buckets = p.real.control.buckets().await.unwrap();
    assert_eq!(
        buckets.iter().map(|b| b.name.as_str()).collect::<Vec<_>>(),
        ["b1", "b2", "b3", "b4"]
    );
    let b1 = &buckets[0];
    assert_eq!(
        b1.creation_date, 100,
        "update must not rewrite creation_date"
    );
    assert_eq!(b1.versioning, "Enabled");
    assert!(b1.object_lock, "update writes object_lock");
    assert!(!b1.deleting, "update clears deleting");
    assert_eq!(b1.owner, Some(OwnerRef::User("alice".into())));
    assert!(!b1.owner_is_admin);
    let b2 = &buckets[1];
    assert_eq!(b2.owner, Some(OwnerRef::Admin));
    assert!(
        b2.deleting == false && b2.versioning == "Off",
        "mode conflict is zero-write"
    );
    let b3 = &buckets[2];
    assert_eq!(b3.owner, Some(OwnerRef::User("root".into())));
    assert!(b3.owner_is_admin, "runtime keeps User and flags admin");
    assert!(b3.object_lock && b3.deleting);
    let b4 = &buckets[3];
    assert_eq!(b4.default_encryption, None);
    assert_eq!(
        p.real.control.bucket("b2").await.unwrap().unwrap().owner,
        Some(OwnerRef::Admin)
    );
}

#[tokio::test]
async fn bucket_owner_errors_and_witness_match() {
    let p = parity(&[("root", true), ("alice", false)]).await;
    let missing = entry("b1", OwnerRef::User("ghost".into()), 1, "Off", false, false);
    let sq_err = sqlite(&p, |t| {
        let missing = missing.clone();
        Box::pin(async move {
            t.bucket_create(&missing).await.map_err(err)?;
            Ok(())
        })
    })
    .await
    .unwrap_err();
    let ob_err = object(&p, |t| {
        let missing = missing.clone();
        Box::pin(async move {
            t.bucket_create(&missing).await.map_err(err)?;
            Ok(())
        })
    })
    .await
    .unwrap_err();
    assert_eq!(sq_err, ob_err);
    assert_eq!(
        sq_err,
        "invalid control data: bucket owner user ghost not found"
    );
    assert!(p.db.control_store().buckets().await.unwrap().is_empty());
    assert!(p.real.control.buckets().await.unwrap().is_empty());

    // Witness (no admin): a missing user resolves to NULL instead of failing.
    let witness = parity(&[("alice", false)]).await;
    let got = object(&witness, |t| {
        let missing = missing.clone();
        Box::pin(async move { t.bucket_create(&missing).await.map_err(err) })
    })
    .await
    .unwrap();
    assert_eq!(got, BucketCreateOutcome::Created);
    let view = witness.real.control.bucket("b1").await.unwrap().unwrap();
    assert_eq!(view.owner, Some(OwnerRef::Admin));
    assert_eq!(view.owner_uid, None);
}

async fn put_object_bucket(p: &Parity, row: &BucketRow) {
    let bytes = encode_row(row).unwrap();
    let key = key1(&row.name);
    p.real
        .control
        .transaction(local(), move |txn| {
            let key = key.clone();
            let bytes = bytes.clone();
            Box::pin(async move { txn.put(TableId::Buckets, &key, &bytes).await })
        })
        .await
        .unwrap();
}

async fn seed_targets(p: &Parity) {
    sqlx::query(
        "INSERT INTO buckets (name, owner_id, creation_date, set_id) VALUES \
         ('b1', NULL, 1, 7), ('b-extra', NULL, 2, 9)",
    )
    .execute(p.db.pool())
    .await
    .unwrap();
    for (name, set_id) in [("b1", 7), ("b-extra", 9)] {
        put_object_bucket(
            p,
            &BucketRow {
                name: name.into(),
                owner_uid: None,
                set_id,
                creation_date: if name == "b1" { 1 } else { 2 },
                versioning: "Off".into(),
                object_lock: false,
                default_encryption: None,
                object_lock_default_mode: None,
                object_lock_default_days: None,
                replication_mode: "async".into(),
                deleting: false,
            },
        )
        .await;
    }
}

async fn install_scenario<T: BucketSnapshotTxn>(
    txn: &mut T,
) -> Result<Vec<BucketEntry>, StoreError> {
    txn.bucket_install_prepare(&["b1".into(), "b3".into()])
        .await
        .map_err(err)?;
    txn.bucket_upsert_install(&entry(
        "b1",
        OwnerRef::User("alice".into()),
        55,
        "Enabled",
        true,
        false,
    ))
    .await
    .map_err(err)?;
    txn.bucket_upsert_install(&entry("b3", OwnerRef::Admin, 56, "Off", false, false))
        .await
        .map_err(err)?;
    let exported = txn.buckets().await.map_err(err)?;
    txn.replace_bucket_policies(&[xl_store::control_store::bucket::BucketPolicyEntry {
        bucket: "b1".into(),
        policy_json: r#"{"installed":true}"#.into(),
        updated_at: 77,
    }])
    .await
    .map_err(err)?;
    Ok(exported)
}

#[tokio::test]
async fn bucket_snapshot_install_preserves_set_id() {
    let p = parity(&[("root", true), ("alice", false)]).await;
    seed_targets(&p).await;
    let sq = sqlite(&p, |t| Box::pin(install_scenario(t))).await.unwrap();
    let ob = object(&p, |t| Box::pin(install_scenario(t))).await.unwrap();
    assert_eq!(sq, ob, "same strict export after install");
    assert_eq!(
        serde_json::to_vec(&sq).unwrap(),
        serde_json::to_vec(&ob).unwrap()
    );
    assert_eq!(
        sq.iter().map(|b| b.name.as_str()).collect::<Vec<_>>(),
        ["b1", "b3"]
    );
    assert_eq!(sq[0].creation_date, 55, "install updates creation_date");
    assert_eq!(sq[0].owner, OwnerRef::User("alice".into()));

    let (sqlite_set_id,): (i64,) = sqlx::query_as("SELECT set_id FROM buckets WHERE name = 'b1'")
        .fetch_one(p.db.pool())
        .await
        .unwrap();
    assert_eq!(sqlite_set_id, 7);
    let bytes = p
        .real
        .control
        .read_row(TableId::Buckets, &key1("b1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decode_row::<BucketRow>(&bytes).unwrap().set_id, 7);
    assert!(p.real.control.bucket("b-extra").await.unwrap().is_none());
    assert!(
        p.db.control_store()
            .bucket("b-extra")
            .await
            .unwrap()
            .is_none()
    );
    assert_reads(&p, &["b1", "b3", "b-extra"]).await;
    assert_eq!(
        p.real
            .control
            .bucket_policy("b1")
            .await
            .unwrap()
            .unwrap()
            .updated_at,
        77
    );
}

#[tokio::test]
async fn bucket_export_rejects_dangling_owner() {
    let p = parity(&[("alice", false)]).await;
    p.real
        .control
        .transaction(local(), |txn| {
            Box::pin(async move {
                let row = BucketRow {
                    name: "dangling".into(),
                    owner_uid: Some(999),
                    set_id: 0,
                    creation_date: 1,
                    versioning: "Off".into(),
                    object_lock: false,
                    default_encryption: None,
                    object_lock_default_mode: None,
                    object_lock_default_days: None,
                    replication_mode: "async".into(),
                    deleting: false,
                };
                txn.put(
                    TableId::Buckets,
                    &key1("dangling"),
                    &encode_row(&row).unwrap(),
                )
                .await
            })
        })
        .await
        .unwrap();
    let mut conn = p.db.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES ('dangling', 999, 1)")
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);

    let sq_err = sqlite(&p, |t| {
        Box::pin(async move { t.buckets().await.map_err(err).map(|_| ()) })
    })
    .await
    .unwrap_err();
    let ob_err = object(&p, |t| {
        Box::pin(async move { t.buckets().await.map_err(err).map(|_| ()) })
    })
    .await
    .unwrap_err();
    assert_eq!(sq_err, ob_err);
    assert_eq!(sq_err, "owner user id 999 missing");
    // A failed export leaves the current row visible to runtime reads as Admin.
    let view = p.real.control.bucket("dangling").await.unwrap().unwrap();
    assert_eq!(view.owner, Some(OwnerRef::Admin));
}
