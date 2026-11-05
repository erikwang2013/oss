//! C1 parity: object IAM reads/writes and the raft-guard matrix vs SQLite.

use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{AccessKeyEntry, IamStore, PermissionRow, UserEntry};

use super::local;
use super::support::{self, both_iam, same_err, same_ok};

const RAFT_GUARD: &str = "raft: control write outside apply";

fn gate<T>(result: Result<T, ControlError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(e) if e.to_string().contains(RAFT_GUARD) => "guard",
        Err(_) => "other",
    }
}

#[tokio::test]
async fn reads_match_sqlite() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    support::sqlite_iam(&db, |t| Box::pin(async move { support::seed_iam(t).await }))
        .await
        .unwrap();
    support::object_iam(&obj.control, local(), |t| {
        Box::pin(async move { support::seed_iam(t).await })
    })
    .await
    .unwrap();
    support::dump_iam(&db, &obj).await;

    let oracle = support::sqlite_store(&db);
    let object = &obj.control;
    support::dump_pair(
        "users()",
        oracle.users().await.unwrap(),
        object.users().await.unwrap(),
    );
    support::dump_pair(
        "user(alice)",
        oracle.user("alice").await.unwrap(),
        object.user("alice").await.unwrap(),
    );
    support::dump_pair(
        "user_by_id(3)",
        oracle.user_by_id(3).await.unwrap(),
        object.user_by_id(3).await.unwrap(),
    );
    assert_eq!(oracle.user_by_id(999).await.unwrap(), None);
    assert_eq!(object.user_by_id(999).await.unwrap(), None);
    support::dump_pair(
        "user_exists",
        oracle.user_exists("alice").await.unwrap(),
        object.user_exists("alice").await.unwrap(),
    );
    support::dump_pair(
        "login_user(alice)",
        oracle.login_user("alice").await.unwrap(),
        object.login_user("alice").await.unwrap(),
    );
    support::dump_pair(
        "login_user(bob)",
        oracle.login_user("bob").await.unwrap(),
        object.login_user("bob").await.unwrap(),
    );
    assert_eq!(object.login_user("nobody").await.unwrap(), None);
    support::dump_pair(
        "admin_id",
        oracle.admin_id().await.unwrap(),
        object.admin_id().await.unwrap(),
    );
    support::dump_pair(
        "admin_counts",
        oracle.admin_counts().await.unwrap(),
        object.admin_counts().await.unwrap(),
    );
    support::dump_pair(
        "permissions_for_user",
        oracle.permissions_for_user(2).await.unwrap(),
        object.permissions_for_user(2).await.unwrap(),
    );
    support::dump_pair(
        "access_keys_for_user",
        oracle.access_keys_for_user(2).await.unwrap(),
        object.access_keys_for_user(2).await.unwrap(),
    );
    assert_eq!(
        object.access_keys_for_user(2).await.unwrap(),
        vec!["AKALICE"]
    );
}

#[tokio::test]
async fn writes_match_sqlite() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    support::sqlite_iam(&db, |t| Box::pin(async move { support::seed_iam(t).await }))
        .await
        .unwrap();
    support::object_iam(&obj.control, local(), |t| {
        Box::pin(async move { support::seed_iam(t).await })
    })
    .await
    .unwrap();

    // Users: explicit status, duplicate, password/status updates.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.user_create(&UserEntry {
                username: "dave".into(),
                password_hash: "h-dave".into(),
                status: "disabled".into(),
                created_at: 50,
            })
            .await
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), 5);
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.user_create(&UserEntry {
                username: "alice".into(),
                password_hash: "dup".into(),
                status: "active".into(),
                created_at: 1,
            })
            .await
        })
    })
    .await;
    let (s, o) = same_err((s, o));
    assert!(matches!(s, ControlError::Backend(_)));
    assert_eq!(
        o.to_string(),
        "control store backend: UNIQUE constraint failed: users.username"
    );

    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.user_set_password(2, "h-alice-2").await })
    })
    .await;
    assert!(same_ok((s, o)));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.user_set_password(999, "x").await })
    })
    .await;
    assert!(!same_ok((s, o)));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.user_set_status(3, true).await })
    })
    .await;
    assert!(same_ok((s, o)));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.user_set_status(1, false).await })
    })
    .await;
    assert!(!same_ok((s, o)), "last active admin cannot be disabled");
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.user_set_status_value(4, "disabled").await })
    })
    .await;
    assert!(same_ok((s, o)));

    // Permissions: upsert/delete/replace/bucket cascade.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.permission_upsert(2, "b1", false, true, true).await?;
            t.permission_upsert(2, "b2", true, false, false).await?;
            t.permission_delete(2, "b0").await?;
            t.permission_delete(2, "missing").await?;
            t.permission_upsert(3, "b9", true, true, true).await
        })
    })
    .await;
    same_ok((s, o));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.permissions_replace(
                2,
                &[
                    PermissionRow {
                        bucket: "b1".into(),
                        read: true,
                        write: true,
                        delete: true,
                    },
                    PermissionRow {
                        bucket: "b3".into(),
                        read: true,
                        write: false,
                        delete: false,
                    },
                ],
            )
            .await
        })
    })
    .await;
    same_ok((s, o));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.permissions_delete_for_bucket("b1").await })
    })
    .await;
    same_ok((s, o));
    support::dump_iam(&db, &obj).await;

    // Access keys: apply/legacy create, status, delete, lookups, site upsert.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.access_key_create(&AccessKeyEntry {
                access_key: "AKDAVE".into(),
                secret_key: "sk-dave".into(),
                username: "dave".into(),
                status: "ignored".into(),
                expires_at: Some(7),
                created_at: 55,
                parent_ak: None,
                session_policy: None,
                kind: "long-term".into(),
            })
            .await
        })
    })
    .await;
    same_ok((s, o));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.access_key_create(&AccessKeyEntry {
                access_key: "AKMISS".into(),
                username: "nobody".into(),
                secret_key: "sk".into(),
                status: "active".into(),
                expires_at: None,
                created_at: 56,
                parent_ak: None,
                session_policy: None,
                kind: "long-term".into(),
            })
            .await
        })
    })
    .await;
    let (s, o) = same_err((s, o));
    assert_eq!(s.to_string(), o.to_string());
    assert_eq!(
        o.to_string(),
        "invalid control data: access key owner nobody not found"
    );

    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.access_key_create_legacy("AKC", "sk-c", 3, 40).await?;
            assert!(t.access_key_exists("AKC").await?);
            assert!(!t.access_key_exists("AKNONE").await?);
            assert!(t.access_key_set_status("AKC", "disabled").await?);
            assert!(!t.access_key_set_status("AKNONE", "disabled").await?);
            let pair: Option<(i64, String)> = t.access_key_user_and_kind("SESSALICE").await?;
            assert_eq!(pair, Some((2, "session".into())));
            assert!(t.access_key_owner_is_admin("AKADMIN").await?);
            assert!(!t.access_key_owner_is_admin("AKALICE").await?);
            assert!(t.access_key_long_term_owner_is_admin("AKADMIN").await?);
            assert!(!t.access_key_long_term_owner_is_admin("SESSALICE").await?);
            assert!(!t.access_key_delete_long_term("SESSALICE").await?);
            t.access_key_delete("AKC").await
        })
    })
    .await;
    assert!(same_ok((s, o)));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.access_key_site_upsert("AKSITE", "sk-1", 2, "active", Some(99), 50)
                .await?;
            t.access_key_site_upsert("AKSITE", "sk-2", 3, "disabled", None, 60)
                .await
        })
    })
    .await;
    same_ok((s, o));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.access_key_delete_long_term("AKALICE").await })
    })
    .await;
    assert!(same_ok((s, o)));
    support::dump_iam(&db, &obj).await;

    // User delete cascade (keys + permissions + user) and last-admin guard.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            assert!(t.user_delete_guard(3).await?);
            t.user_delete_access_keys_permissions(3).await?;
            assert!(t.user_delete(3).await?);
            assert!(!t.user_delete(3).await?);
            assert!(t.user_delete_guard(2).await?);
            t.user_delete_access_keys_permissions(2).await?;
            t.user_delete(2).await
        })
    })
    .await;
    assert!(same_ok((s, o)));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.user_delete_guard(1).await })
    })
    .await;
    assert!(!same_ok((s, o)), "sole active admin cannot be deleted");
    support::dump_iam(&db, &obj).await;

    // Reads agree after the mutation script.
    let oracle = support::sqlite_store(&db);
    support::dump_pair(
        "users() after writes",
        oracle.users().await.unwrap(),
        obj.control.users().await.unwrap(),
    );
    support::dump_pair(
        "login_user(alice) after writes",
        oracle.login_user("alice").await.unwrap(),
        obj.control.login_user("alice").await.unwrap(),
    );
    support::dump_pair(
        "access_keys_for_user(carol)",
        oracle.access_keys_for_user(4).await.unwrap(),
        obj.control.access_keys_for_user(4).await.unwrap(),
    );
}

#[tokio::test]
async fn standalone_local_writes_are_unguarded() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    support::sqlite_iam(&db, |t| Box::pin(async move { support::seed_iam(t).await }))
        .await
        .unwrap();
    support::object_iam(&obj.control, local(), |t| {
        Box::pin(async move { support::seed_iam(t).await })
    })
    .await
    .unwrap();
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.user_create_default_status("free", "h", false, 80).await?;
            t.permission_upsert(3, "free", true, true, true).await?;
            t.access_key_create_legacy("AKFREE", "sk", 4, 81).await
        })
    })
    .await;
    same_ok((s, o));
    support::dump_iam(&db, &obj).await;
}
