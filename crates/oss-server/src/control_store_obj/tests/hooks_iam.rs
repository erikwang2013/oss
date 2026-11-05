//! D2 golden matrix: permission / user / access_key hook cells vs the triggers.

use crate::control_store_obj::site_apply::{AccessKeySiteRow, PermissionSiteRow, UserSiteRow};

use super::site_common::{
    assert_rows, block, now, obj_rows, object_site, run_hook, sql_rows, sqlite_site,
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
async fn permission_trigger_cells_match() {
    let (db, id) = sqlite_site().await;
    let real = object_site(&id).await;
    users(&db).await;

    sqlx::query(
        "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
         VALUES (2, 'pb', 1, 0, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let row = PermissionSiteRow {
        username: Some("alice".into()),
        bucket: "pb".into(),
        can_read: true,
        can_write: false,
        can_delete: true,
        owner_is_admin: Some(false),
    };
    run_hook(&real, |txn| {
        let row = row.clone();
        Box::pin(async move { txn.site_hook_permission(None, Some(&row), 1_000).await })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);
    assert!(obj_rows(&real).await[0].entity_id.contains('\u{1f}'));

    sqlx::query("UPDATE permissions SET can_read = 0 WHERE user_id = 2 AND bucket = 'pb'")
        .execute(db.pool())
        .await
        .unwrap();
    let mut changed = row.clone();
    changed.can_read = false;
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_permission(Some(&row), Some(&changed), 1_001)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    // admin target, missing user: both are excluded exactly like the subquery.
    sqlx::query(
        "INSERT INTO permissions (user_id, bucket, can_read) VALUES (1, 'pb2', 1), (999, 'pb3', 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let admin = PermissionSiteRow {
        username: Some("root".into()),
        bucket: "pb2".into(),
        can_read: true,
        owner_is_admin: Some(true),
        ..PermissionSiteRow::default()
    };
    run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_permission(None, Some(&admin), 1_100).await })
    })
    .await
    .unwrap();
    let dangling = PermissionSiteRow {
        bucket: "pb3".into(),
        can_read: true,
        ..PermissionSiteRow::default()
    };
    run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_permission(None, Some(&dangling), 1_101).await })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    let before = sql_rows(&db).await.len();
    block(&db, &real, "permission", "alice\u{1f}gpb").await;
    let err =
        sqlx::query("INSERT INTO permissions (user_id, bucket, can_read) VALUES (2, 'gpb', 1)")
            .execute(db.pool())
            .await
            .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let gated = PermissionSiteRow {
        username: Some("alice".into()),
        bucket: "gpb".into(),
        can_read: true,
        owner_is_admin: Some(false),
        ..PermissionSiteRow::default()
    };
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_permission(None, Some(&gated), now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );

    block(&db, &real, "permission", "alice\u{1f}pb").await;
    let err =
        sqlx::query("UPDATE permissions SET can_write = 1 WHERE user_id = 2 AND bucket = 'pb'")
            .execute(db.pool())
            .await
            .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let mut blocked = changed.clone();
    blocked.can_write = true;
    let obj_err = run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_permission(Some(&changed), Some(&blocked), now())
                .await
        })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );

    let err = sqlx::query("DELETE FROM permissions WHERE user_id = 2 AND bucket = 'pb'")
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_permission(Some(&changed), None, now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );
    assert_eq!(sql_rows(&db).await.len(), before);
    assert_eq!(obj_rows(&real).await.len(), before);
}

#[tokio::test]
async fn user_trigger_cells_match() {
    let (db, id) = sqlite_site().await;
    let real = object_site(&id).await;
    users(&db).await;
    let alice = UserSiteRow {
        username: "alice".into(),
        password_hash: "h1".into(),
        status: "active".into(),
        created_at: 10,
        is_admin: false,
    };

    sqlx::query("UPDATE users SET password_hash = 'h2' WHERE id = 2")
        .execute(db.pool())
        .await
        .unwrap();
    let mut changed = alice.clone();
    changed.password_hash = "h2".into();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_user(Some(&alice), Some(&changed), 1_000)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    sqlx::query("UPDATE users SET is_admin = 0 WHERE id = 2")
        .execute(db.pool())
        .await
        .unwrap();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_user(Some(&changed), Some(&changed), 1_001)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    // admin insert/update/delete and 0->1->0 transitions are excluded.
    let root = UserSiteRow {
        username: "root".into(),
        password_hash: "h".into(),
        status: "active".into(),
        created_at: 1,
        is_admin: true,
    };
    run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_user(None, Some(&root), 1_100).await })
    })
    .await
    .unwrap();
    sqlx::query("UPDATE users SET is_admin = 1 WHERE id = 3")
        .execute(db.pool())
        .await
        .unwrap();
    let bob = UserSiteRow {
        username: "bob".into(),
        password_hash: "h1".into(),
        status: "active".into(),
        created_at: 11,
        is_admin: false,
    };
    let mut bob_admin = bob.clone();
    bob_admin.is_admin = true;
    run_hook(&real, |txn| {
        let bob_admin = bob_admin.clone();
        Box::pin(async move {
            txn.site_hook_user(Some(&bob), Some(&bob_admin), 1_101)
                .await
        })
    })
    .await
    .unwrap();
    sqlx::query("UPDATE users SET is_admin = 0 WHERE id = 3")
        .execute(db.pool())
        .await
        .unwrap();
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_user(Some(&bob_admin), Some(&bob), 1_102)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    let before = sql_rows(&db).await.len();
    block(&db, &real, "user", "gated-user").await;
    let err = sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
         VALUES ('gated-user', 'h', 0, 'active', 20)",
    )
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let gated = UserSiteRow {
        username: "gated-user".into(),
        password_hash: "h".into(),
        status: "active".into(),
        created_at: 20,
        is_admin: false,
    };
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_user(None, Some(&gated), now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );

    block(&db, &real, "user", "alice").await;
    let err = sqlx::query("UPDATE users SET password_hash = 'h3' WHERE id = 2")
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let mut blocked = changed.clone();
    blocked.password_hash = "h3".into();
    let obj_err = run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_user(Some(&changed), Some(&blocked), now())
                .await
        })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );

    let err = sqlx::query("DELETE FROM users WHERE id = 2")
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_user(Some(&changed), None, now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );
    assert_eq!(sql_rows(&db).await.len(), before);
    assert_eq!(obj_rows(&real).await.len(), before);
}

#[tokio::test]
async fn access_key_trigger_cells_match() {
    let (db, id) = sqlite_site().await;
    let real = object_site(&id).await;
    users(&db).await;
    sqlx::query(
        "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, created_at, kind) \
         VALUES ('AK1', 'SK1', 2, 'active', NULL, 10, 'long-term'), \
                ('AKS', 'SKS', 2, 'active', 20, 11, 'session'), \
                ('AKR', 'SKR', 1, 'active', NULL, 12, 'long-term'), \
                ('AK2', 'SK2', 2, 'active', NULL, 13, 'long-term')",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let ak1 = AccessKeySiteRow {
        access_key: "AK1".into(),
        secret_key: "SK1".into(),
        username: Some("alice".into()),
        status: "active".into(),
        expires_at: None,
        created_at: 10,
        kind: "long-term".into(),
        owner_is_admin: Some(false),
    };
    let session = AccessKeySiteRow {
        access_key: "AKS".into(),
        secret_key: "SKS".into(),
        username: Some("alice".into()),
        status: "active".into(),
        expires_at: Some(20),
        created_at: 11,
        kind: "session".into(),
        owner_is_admin: Some(false),
    };
    let admin = AccessKeySiteRow {
        access_key: "AKR".into(),
        username: Some("root".into()),
        owner_is_admin: Some(true),
        kind: "long-term".into(),
        ..AccessKeySiteRow::default()
    };
    for row in [&ak1, &session, &admin] {
        run_hook(&real, |txn| {
            Box::pin(async move { txn.site_hook_access_key(None, Some(row), 1_000).await })
        })
        .await
        .unwrap();
    }
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    // Any UPDATE of a replicable long-term key logs; kind transitions do not.
    sqlx::query("UPDATE access_keys SET status = 'disabled' WHERE access_key = 'AK1'")
        .execute(db.pool())
        .await
        .unwrap();
    let mut changed = ak1.clone();
    changed.status = "disabled".into();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_access_key(Some(&ak1), Some(&changed), 1_001)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    sqlx::query("UPDATE access_keys SET kind = 'session' WHERE access_key = 'AK1'")
        .execute(db.pool())
        .await
        .unwrap();
    let mut to_session = changed.clone();
    to_session.kind = "session".into();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        let to_session = to_session.clone();
        Box::pin(async move {
            txn.site_hook_access_key(Some(&changed), Some(&to_session), 1_002)
                .await
        })
    })
    .await
    .unwrap();
    sqlx::query("UPDATE access_keys SET kind = 'long-term' WHERE access_key = 'AK1'")
        .execute(db.pool())
        .await
        .unwrap();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_access_key(Some(&to_session), Some(&changed), 1_003)
                .await
        })
    })
    .await
    .unwrap();
    sqlx::query("UPDATE access_keys SET status = 'disabled' WHERE access_key = 'AKS'")
        .execute(db.pool())
        .await
        .unwrap();
    let mut session_changed = session.clone();
    session_changed.status = "disabled".into();
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_access_key(Some(&session), Some(&session_changed), 1_004)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    // delete cell (payload without status/expires_at).
    sqlx::query("DELETE FROM access_keys WHERE access_key = 'AK1'")
        .execute(db.pool())
        .await
        .unwrap();
    run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_access_key(Some(&changed), None, 1_100).await })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    let before = sql_rows(&db).await.len();
    block(&db, &real, "access_key", "AKG").await;
    let err = sqlx::query(
        "INSERT INTO access_keys (access_key, secret_key, user_id, status, created_at, kind) \
         VALUES ('AKG', 'SKG', 2, 'active', 30, 'long-term')",
    )
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let gated = AccessKeySiteRow {
        access_key: "AKG".into(),
        secret_key: "SKG".into(),
        username: Some("alice".into()),
        status: "active".into(),
        created_at: 30,
        kind: "long-term".into(),
        owner_is_admin: Some(false),
        ..AccessKeySiteRow::default()
    };
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_access_key(None, Some(&gated), now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );

    block(&db, &real, "access_key", "AK2").await;
    let ak2 = AccessKeySiteRow {
        access_key: "AK2".into(),
        secret_key: "SK2".into(),
        username: Some("alice".into()),
        status: "active".into(),
        created_at: 13,
        kind: "long-term".into(),
        owner_is_admin: Some(false),
        ..AccessKeySiteRow::default()
    };
    let mut ak2_disabled = ak2.clone();
    ak2_disabled.status = "disabled".into();
    let err = sqlx::query("UPDATE access_keys SET status = 'disabled' WHERE access_key = 'AK2'")
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let obj_err = run_hook(&real, |txn| {
        let ak2 = ak2.clone();
        Box::pin(async move {
            txn.site_hook_access_key(Some(&ak2), Some(&ak2_disabled), now())
                .await
        })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );

    let err = sqlx::query("DELETE FROM access_keys WHERE access_key = 'AK2'")
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_access_key(Some(&ak2), None, now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );
    assert_eq!(sql_rows(&db).await.len(), before);
    assert_eq!(obj_rows(&real).await.len(), before);
}
