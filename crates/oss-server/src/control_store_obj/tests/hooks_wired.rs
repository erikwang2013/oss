//! Site hooks must fire from the public domain writes (not only direct hook
//! calls). These tests are the production-path counterpart of `site_hooks.rs`.

use xl_store::control_state::root::AppliedLog;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketEntry, BucketTxn};
use xl_store::control_store::iam::{IamTxn, PermissionRow};

use crate::control_store_obj::WriteOrigin;

use super::site_common::{obj_rows, object_site, run_hook, run_origin};

fn bucket(name: &str, owner: OwnerRef) -> BucketEntry {
    BucketEntry {
        name: name.into(),
        owner,
        creation_date: 10,
        versioning: "Off".into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting: false,
    }
}

fn entities(rows: &[crate::control_store_obj::site::SiteChangelogRow]) -> Vec<(String, String)> {
    rows.iter()
        .map(|r| (r.entity.clone(), r.op.clone()))
        .collect()
}

#[tokio::test]
async fn domain_inserts_and_updates_emit_changelog() {
    let real = object_site("site-a").await;
    run_hook(&real, |txn| {
        Box::pin(async move {
            let uid = txn
                .user_create_default_status("alice", "h", false, 10)
                .await?;
            txn.bucket_create(&bucket("b1", OwnerRef::User("alice".into())))
                .await?;
            txn.bucket_set_versioning("b1", "Enabled").await?;
            txn.permission_upsert(uid, "b1", true, false, true).await?;
            txn.access_key_create_legacy("AK1", "SK1", uid, 10).await?;
            txn.bucket_policy_set("b1", "{\"Statement\":[]}", 11)
                .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let rows = obj_rows(&real).await;
    assert_eq!(
        entities(&rows),
        vec![
            ("user".into(), "upsert".into()),
            ("bucket".into(), "upsert".into()),
            ("bucket".into(), "upsert".into()),
            ("permission".into(), "upsert".into()),
            ("access_key".into(), "upsert".into()),
            ("bucket_policy".into(), "upsert".into()),
        ]
    );
    assert_eq!(rows[0].entity_id, "alice");
    assert_eq!(rows[1].entity_id, "b1");
    assert!(rows[1].payload.contains("\"owner_username\":\"alice\""));
    assert!(rows[2].payload.contains("\"versioning\":\"Enabled\""));
    assert_eq!(rows[3].entity_id, "alice\u{1f}b1");
    assert_eq!(rows[4].entity_id, "AK1");
    assert_eq!(rows[5].entity_id, "b1");
}

#[tokio::test]
async fn domain_deletes_and_user_cascade_emit_changelog() {
    let real = object_site("site-a").await;
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.user_create_default_status("alice", "h", false, 10)
                .await?;
            let uid = txn.user_uid("alice").await?.unwrap();
            txn.bucket_create(&bucket("b1", OwnerRef::User("alice".into())))
                .await?;
            txn.permission_upsert(uid, "b1", true, false, true).await?;
            txn.access_key_create_legacy("AK1", "SK1", uid, 10).await?;
            txn.bucket_policy_set("b1", "{}", 11).await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    let before = obj_rows(&real).await.len();

    run_hook(&real, |txn| {
        Box::pin(async move {
            let uid = txn.user_uid("alice").await?.unwrap();
            txn.user_delete_access_keys_permissions(uid).await?;
            txn.bucket_clear_owner(uid).await?;
            txn.user_delete(uid).await?;
            txn.bucket_policy_delete("b1").await?;
            txn.bucket_delete("b1").await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    let rows = obj_rows(&real).await;
    let tail: Vec<_> = rows[before..]
        .iter()
        .map(|r| (r.entity.as_str(), r.op.as_str()))
        .collect();
    assert_eq!(
        tail,
        vec![
            ("access_key", "delete"),
            ("permission", "delete"),
            ("bucket", "upsert"), // owner cleared
            ("user", "delete"),
            ("bucket_policy", "delete"),
            ("bucket", "delete"),
        ]
    );
}

#[tokio::test]
async fn replace_and_status_updates_emit() {
    let real = object_site("site-a").await;
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.user_create_default_status("alice", "h", false, 10)
                .await?;
            let uid = txn.user_uid("alice").await?.unwrap();
            txn.permission_upsert(uid, "b1", true, false, false).await?;
            txn.permissions_replace(
                uid,
                &[PermissionRow {
                    bucket: "b2".into(),
                    read: true,
                    write: true,
                    delete: false,
                }],
            )
            .await?;
            txn.user_set_status(uid, false).await?;
            txn.access_key_create_legacy("AK1", "SK1", uid, 10).await?;
            txn.access_key_set_status("AK1", "inactive").await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    let rows = obj_rows(&real).await;
    let tail: Vec<_> = rows[rows.len() - 6..]
        .iter()
        .map(|r| (r.entity.as_str(), r.op.as_str()))
        .collect();
    assert_eq!(
        tail,
        vec![
            ("permission", "upsert"), // original upsert
            ("permission", "delete"), // replace: delete old
            ("permission", "upsert"), // replace: insert new
            ("user", "upsert"),       // status change
            ("access_key", "upsert"), // create
            ("access_key", "upsert"), // status change
        ]
    );
}

#[tokio::test]
async fn non_local_origins_do_not_emit() {
    let real = object_site("site-a").await;
    run_origin(
        &real,
        WriteOrigin::raft_apply(AppliedLog { term: 1, index: 7 }),
        |txn| {
            Box::pin(async move {
                txn.user_create_default_status("bob", "h", false, 10)
                    .await?;
                txn.bucket_create(&bucket("raft-b", OwnerRef::User("bob".into())))
                    .await?;
                Ok(())
            })
        },
    )
    .await
    .unwrap();
    assert!(obj_rows(&real).await.is_empty());
}

#[tokio::test]
async fn local_write_loses_to_a_newer_clock() {
    let real = object_site("site-a").await;
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_put_clock(
                "bucket",
                "gated",
                crate::db::now() + 3_600,
                99,
                "remote-z",
                1,
            )
            .await
        })
    })
    .await
    .unwrap();
    let err = run_hook(&real, |txn| {
        Box::pin(async move {
            txn.bucket_create(&bucket("gated", OwnerRef::Admin))
                .await
                .map(|_| ())
        })
    })
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    assert!(obj_rows(&real).await.is_empty());
}
