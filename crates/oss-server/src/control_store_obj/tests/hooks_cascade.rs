//! D2 cascades (user/bucket delete) and origin suppression / clock ownership.

use xl_store::control_state::root::AppliedLog;
use xl_store::control_store::sites::{SitePeerRow, SiteTxn};

use crate::control_store_obj::WriteOrigin;
use crate::control_store_obj::site_apply::{
    AccessKeySiteRow, BucketSiteRow, PermissionSiteRow, PolicySiteRow, UserSiteRow,
};

use super::real_store;
use super::site_common::{
    assert_rows, now, obj_rows, object_site, run_hook, run_origin, sql_rows, sqlite_site,
};

async fn users(db: &crate::db::Db) {
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, is_admin, status, created_at) \
         VALUES (1, 'root', 'h', 1, 'active', 1), \
                (2, 'alice', 'h1', 0, 'active', 10), \
                (3, 'bob', 'h1', 0, 'active', 11)",
    )
    .execute(db.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn user_delete_cascade_matches() {
    let (db, id) = sqlite_site().await;
    let real = object_site(&id).await;
    users(&db).await;
    sqlx::query(
        "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) \
         VALUES ('AKU', 'SKU', 2, 'active', NULL, 10, 'long-term'), \
                ('AKS', 'SKS', 2, 'active', 20, 11, 'session'), \
                ('AKB', 'SKB', 3, 'active', NULL, 12, 'long-term')",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
         VALUES (2, 'cb1', 1, 0, 1), (3, 'cb2', 1, 0, 0)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock) \
         VALUES ('cb1', 2, 1, 'Off', 0), ('cb2', 2, 2, 'Off', 0), ('cb3', 3, 3, 'Off', 0)",
    )
    .execute(db.pool())
    .await
    .unwrap();

    let mut tx = db.pool().begin().await.unwrap();
    for sql in [
        "DELETE FROM access_keys WHERE user_id = 2",
        "DELETE FROM permissions WHERE user_id = 2",
        "UPDATE buckets SET owner_id = NULL WHERE owner_id = 2",
        "DELETE FROM users WHERE id = 2",
    ] {
        sqlx::query(sql).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();

    let alice = UserSiteRow {
        username: "alice".into(),
        password_hash: "h1".into(),
        status: "active".into(),
        created_at: 10,
        is_admin: false,
    };
    let aku = AccessKeySiteRow {
        access_key: "AKU".into(),
        secret_key: "SKU".into(),
        username: Some("alice".into()),
        status: "active".into(),
        expires_at: None,
        created_at: 10,
        kind: "long-term".into(),
        owner_is_admin: Some(false),
    };
    let perm = PermissionSiteRow {
        username: Some("alice".into()),
        bucket: "cb1".into(),
        can_read: true,
        can_write: false,
        can_delete: true,
        owner_is_admin: Some(false),
    };
    let cb1 = BucketSiteRow {
        name: "cb1".into(),
        owner_uid: Some(2),
        owner_username: Some("alice".into()),
        creation_date: 1,
        versioning: "Off".into(),
        object_lock: false,
    };
    let cb2 = BucketSiteRow {
        name: "cb2".into(),
        owner_uid: Some(2),
        owner_username: Some("alice".into()),
        creation_date: 2,
        versioning: "Off".into(),
        object_lock: false,
    };
    let cb1_null = BucketSiteRow {
        owner_uid: None,
        owner_username: None,
        ..cb1.clone()
    };
    let cb2_null = BucketSiteRow {
        owner_uid: None,
        owner_username: None,
        ..cb2.clone()
    };
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_access_key(Some(&aku), None, 2_000).await?;
            txn.site_hook_permission(Some(&perm), None, 2_000).await?;
            txn.site_hook_bucket(Some(&cb1), Some(&cb1_null), 2_000)
                .await?;
            txn.site_hook_bucket(Some(&cb2), Some(&cb2_null), 2_000)
                .await?;
            txn.site_hook_user(Some(&alice), None, 2_000).await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);
    let rows = obj_rows(&real).await;
    assert_eq!(rows.len(), 5, "{rows:?}");
    assert_eq!(
        rows.iter().map(|r| r.entity.as_str()).collect::<Vec<_>>(),
        ["access_key", "permission", "bucket", "bucket", "user"]
    );
}

#[tokio::test]
async fn bucket_delete_cascade_matches() {
    let (db, id) = sqlite_site().await;
    let real = object_site(&id).await;
    users(&db).await;
    sqlx::query(
        "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock) \
         VALUES ('dk', 3, 1, 'Off', 0)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES ('dk', '{}', 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO permissions (user_id, bucket, can_read) VALUES (3, 'dk', 1)")
        .execute(db.pool())
        .await
        .unwrap();

    let mut tx = db.pool().begin().await.unwrap();
    for sql in [
        "DELETE FROM permissions WHERE bucket = 'dk'",
        "DELETE FROM bucket_policies WHERE bucket = 'dk'",
        "DELETE FROM buckets WHERE name = 'dk'",
    ] {
        sqlx::query(sql).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();

    let perm = PermissionSiteRow {
        username: Some("bob".into()),
        bucket: "dk".into(),
        can_read: true,
        owner_is_admin: Some(false),
        ..PermissionSiteRow::default()
    };
    let policy = PolicySiteRow {
        bucket: "dk".into(),
        policy_json: "{}".into(),
        updated_at: 1,
    };
    let bucket = BucketSiteRow {
        name: "dk".into(),
        owner_uid: Some(3),
        owner_username: Some("bob".into()),
        creation_date: 1,
        versioning: "Off".into(),
        object_lock: false,
    };
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_permission(Some(&perm), None, 3_000).await?;
            txn.site_hook_policy(Some(&policy), None, 3_000).await?;
            txn.site_hook_bucket(Some(&bucket), None, 3_000).await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);
}

#[tokio::test]
async fn site_apply_bypasses_hooks_but_owns_its_clock() {
    let (db, id) = sqlite_site().await;
    let real = object_site(&id).await;
    users(&db).await;
    let bucket = BucketSiteRow {
        name: "applyb".into(),
        owner_uid: None,
        owner_username: None,
        creation_date: 1,
        versioning: "Off".into(),
        object_lock: false,
    };

    run_origin(&real, WriteOrigin::site_apply("remote-x"), |txn| {
        let bucket = bucket.clone();
        Box::pin(async move {
            txn.site_hook_bucket(None, Some(&bucket), 100).await?;
            txn.site_hook_user(
                None,
                Some(&UserSiteRow {
                    username: "remote-u".into(),
                    is_admin: false,
                    ..UserSiteRow::default()
                }),
                100,
            )
            .await
        })
    })
    .await
    .unwrap();
    assert!(obj_rows(&real).await.is_empty());
    assert!(
        real.control
            .site_clock("bucket", "applyb")
            .await
            .unwrap()
            .is_none()
    );

    // SiteApply records the winning remote clock explicitly; still no log row.
    run_origin(&real, WriteOrigin::site_apply("remote-x"), |txn| {
        Box::pin(async move {
            txn.site_put_clock("bucket", "applyb", 100, 7, "!remote", 5)
                .await
        })
    })
    .await
    .unwrap();
    let clock = real
        .control
        .site_clock("bucket", "applyb")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            clock.ts,
            clock.priority,
            clock.origin_site_id.as_str(),
            clock.seq
        ),
        (100, 7, "!remote", 5)
    );
    assert!(obj_rows(&real).await.is_empty());

    // Equal ts/priority wins locally when the local id sorts higher; seq breaks ties.
    run_hook(&real, |txn| {
        let bucket = bucket.clone();
        Box::pin(async move { txn.site_hook_bucket(None, Some(&bucket), 100).await })
    })
    .await
    .unwrap();
    let clock = real
        .control
        .site_clock("bucket", "applyb")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (clock.ts, clock.origin_site_id.as_str()),
        (100, id.as_str())
    );
    assert_eq!(obj_rows(&real).await.len(), 1);

    // A lower tuple loses and aborts; a higher tuple wins and moves the clock.
    let err = run_hook(&real, |txn| {
        let bucket = bucket.clone();
        Box::pin(async move { txn.site_hook_bucket(None, Some(&bucket), 50).await })
    })
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    assert_eq!(obj_rows(&real).await.len(), 1);
    run_hook(&real, |txn| {
        let bucket = bucket.clone();
        Box::pin(async move { txn.site_hook_bucket(None, Some(&bucket), 101).await })
    })
    .await
    .unwrap();
    let clock = real
        .control
        .site_clock("bucket", "applyb")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(clock.ts, 101);

    // A lower explicit clock never overwrites the winner.
    run_origin(&real, WriteOrigin::site_apply("remote-x"), |txn| {
        Box::pin(async move { txn.site_put_clock("bucket", "applyb", 1, 9, "Z", 99).await })
    })
    .await
    .unwrap();
    let clock = real
        .control
        .site_clock("bucket", "applyb")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(clock.ts, 101);

    // RaftApply / SnapshotInstall never run the hooks.
    for origin in [
        WriteOrigin::raft_apply(AppliedLog { term: 1, index: 1 }),
        WriteOrigin::snapshot_install(),
    ] {
        run_origin(&real, origin, |txn| {
            let bucket = bucket.clone();
            Box::pin(async move { txn.site_hook_bucket(None, Some(&bucket), 10_000).await })
        })
        .await
        .unwrap();
    }
    assert_eq!(obj_rows(&real).await.len(), 2);
    assert_eq!(
        real.control
            .site_clock("bucket", "applyb")
            .await
            .unwrap()
            .unwrap()
            .ts,
        101
    );
}

#[tokio::test]
async fn local_hooks_stay_inactive_until_paired_and_seeded() {
    let real = real_store().await;
    let bucket = BucketSiteRow {
        name: "unpaired".into(),
        ..BucketSiteRow::default()
    };
    run_hook(&real, |txn| {
        let bucket = bucket.clone();
        Box::pin(async move { txn.site_hook_bucket(None, Some(&bucket), now()).await })
    })
    .await
    .unwrap();
    assert!(
        real.control
            .site_changelog_after(0)
            .await
            .unwrap()
            .is_empty()
    );

    // A paired row without site_config is still inactive.
    let peer = SitePeerRow {
        peer_id: "p".into(),
        remote_site_id: "r".into(),
        name: "p".into(),
        endpoint: "http://p".into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "a".into(),
        s3_secret_key: "s".into(),
        peer_site_token: "t".into(),
        created_at: 0,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    };
    real.control
        .transaction(WriteOrigin::local("test"), |txn| {
            Box::pin(async move {
                txn.site_insert(&peer, 1)
                    .await
                    .map_err(super::site_common::se)
            })
        })
        .await
        .unwrap();
    run_hook(&real, |txn| {
        let bucket = bucket.clone();
        Box::pin(async move { txn.site_hook_bucket(None, Some(&bucket), now()).await })
    })
    .await
    .unwrap();
    assert!(
        real.control
            .site_changelog_after(0)
            .await
            .unwrap()
            .is_empty()
    );
}
