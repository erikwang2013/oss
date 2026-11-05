//! C1 strict snapshot export/install parity vs SQLite.

use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{AccessKeyEntry, PermissionEntry, UserEntry};

use super::local;
use super::support::{self, both_iam, same_err, same_ok};
use crate::control_store_obj::WriteOrigin;

type Snap = (Vec<UserEntry>, Vec<AccessKeyEntry>, Vec<PermissionEntry>);

async fn seed(db: &crate::db::Db, obj: &support::Obj) {
    support::sqlite_iam(db, |t| Box::pin(async move { support::seed_iam(t).await }))
        .await
        .unwrap();
    support::object_iam(&obj.control, local(), |t| {
        Box::pin(async move { support::seed_iam(t).await })
    })
    .await
    .unwrap();
    let (s, o) = both_iam(db, obj, |t| {
        Box::pin(async move {
            // Snapshot rows are ordered by natural key, not insertion order.
            t.user_create(&UserEntry {
                username: "zoe".into(),
                password_hash: "h-zoe".into(),
                status: "active".into(),
                created_at: 25,
            })
            .await?;
            t.user_create(&UserEntry {
                username: "anna".into(),
                password_hash: "h-anna".into(),
                status: "disabled".into(),
                created_at: 26,
            })
            .await?;
            t.permission_upsert(5, "zz", true, false, false).await?;
            t.permission_upsert(6, "aa", false, true, false).await?;
            t.access_key_create(&AccessKeyEntry {
                access_key: "AKZOE".into(),
                secret_key: "sk".into(),
                username: "zoe".into(),
                status: "active".into(),
                expires_at: None,
                created_at: 27,
                parent_ak: None,
                session_policy: None,
                kind: "long-term".into(),
            })
            .await?;
            t.access_key_create(&AccessKeyEntry {
                access_key: "AKANNA".into(),
                secret_key: "sk".into(),
                username: "anna".into(),
                status: "active".into(),
                expires_at: None,
                created_at: 28,
                parent_ak: None,
                session_policy: None,
                kind: "long-term".into(),
            })
            .await?;
            Ok(())
        })
    })
    .await;
    same_ok((s, o));
}

#[tokio::test]
async fn strict_export_matches_sqlite_and_serializes_identically() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    seed(&db, &obj).await;

    let s: Snap = support::sqlite_iam(&db, |t| {
        Box::pin(async move {
            Ok((
                t.snapshot_users().await?,
                t.snapshot_access_keys().await?,
                t.snapshot_permissions().await?,
            ))
        })
    })
    .await
    .unwrap();
    let o: Snap = support::object_iam(&obj.control, WriteOrigin::snapshot_install(), |t| {
        Box::pin(async move {
            Ok((
                t.snapshot_users().await?,
                t.snapshot_access_keys().await?,
                t.snapshot_permissions().await?,
            ))
        })
    })
    .await
    .unwrap();
    assert_eq!(s, o, "strict snapshot entries");
    assert_eq!(
        rmp_serde::to_vec_named(&s).unwrap(),
        rmp_serde::to_vec_named(&o).unwrap(),
        "strict snapshot bytes"
    );

    let usernames: Vec<&str> = o.0.iter().map(|u| u.username.as_str()).collect();
    assert_eq!(usernames, ["alice", "anna", "bob", "carol", "zoe"]);
    assert!(o.0.iter().all(|u| !u.username.is_empty()));
    let keys: Vec<&str> = o.1.iter().map(|k| k.access_key.as_str()).collect();
    assert_eq!(keys, ["AKALICE", "AKANNA", "AKBOB", "AKZOE"]);
    assert!(o.1.iter().all(|k| k.username != "admin"));
    let perms: Vec<(&str, &str)> =
        o.2.iter()
            .map(|p| (p.username.as_str(), p.bucket.as_str()))
            .collect();
    assert_eq!(
        perms,
        [
            ("alice", "b0"),
            ("alice", "b1"),
            ("anna", "aa"),
            ("zoe", "zz"),
        ],
        "all-zero permission rows are still exported"
    );
}

#[tokio::test]
async fn install_predicates_match_sqlite() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    seed(&db, &obj).await;

    // SQLite FKs force the real install order: permissions, non-admin keys,
    // then non-admin users.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.permissions_clear().await })
    })
    .await;
    same_ok((s, o));
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.access_keys_delete_non_admin().await })
    })
    .await;
    same_ok((s, o));
    support::dump_iam(&db, &obj).await;
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.users_delete_non_admin().await })
    })
    .await;
    same_ok((s, o));
    support::dump_iam(&db, &obj).await;

    // Install users in a different order; local uids must not collide.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            let anna = t
                .user_install(&UserEntry {
                    username: "anna".into(),
                    password_hash: "h-anna".into(),
                    status: "active".into(),
                    created_at: 40,
                })
                .await?;
            let zoe = t
                .user_install(&UserEntry {
                    username: "zoe".into(),
                    password_hash: "h-zoe".into(),
                    status: "disabled".into(),
                    created_at: 41,
                })
                .await?;
            Ok((anna, zoe))
        })
    })
    .await;
    assert_eq!(
        same_ok((s, o)),
        (7, 8),
        "install uids continue after the largest ever uid (6)"
    );
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.user_install(&UserEntry {
                username: "admin".into(),
                password_hash: "h".into(),
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

    // Key + permission install, duplicate predicates, and admin cleanup.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            let row = AccessKeyEntry {
                access_key: "AKANNA".into(),
                secret_key: "sk".into(),
                username: "anna".into(),
                status: "active".into(),
                expires_at: Some(9),
                created_at: 42,
                parent_ak: None,
                session_policy: None,
                kind: "long-term".into(),
            };
            t.access_key_install(&row, 7).await?;
            let perm = PermissionEntry {
                username: "anna".into(),
                bucket: "inst".into(),
                can_read: true,
                can_write: true,
                can_delete: false,
            };
            t.permission_install(&perm, 7).await?;
            t.permission_install(
                &PermissionEntry {
                    username: "admin".into(),
                    bucket: "adm".into(),
                    can_read: true,
                    can_write: true,
                    can_delete: true,
                },
                1,
            )
            .await?;
            Ok(())
        })
    })
    .await;
    same_ok((s, o));
    support::dump_iam(&db, &obj).await;
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.permission_install(
                &PermissionEntry {
                    username: "anna".into(),
                    bucket: "inst".into(),
                    can_read: false,
                    can_write: false,
                    can_delete: false,
                },
                7,
            )
            .await
        })
    })
    .await;
    let (s, o) = same_err((s, o));
    assert!(matches!(s, ControlError::Backend(_)));
    assert_eq!(
        o.to_string(),
        "control store backend: UNIQUE constraint failed: permissions.user_id, permissions.bucket"
    );
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move { t.permissions_delete_admins().await })
    })
    .await;
    same_ok((s, o));
    support::dump_iam(&db, &obj).await;

    // Session/long-term install rows survive orphan cleanup while their owner
    // exists (SQLite FKs make a true orphan unreachable through the API).
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.access_key_install(
                &AccessKeyEntry {
                    access_key: "ANNA-SESS".into(),
                    secret_key: "sk".into(),
                    username: "anna".into(),
                    status: "active".into(),
                    expires_at: None,
                    created_at: 43,
                    parent_ak: None,
                    session_policy: None,
                    kind: "session".into(),
                },
                7,
            )
            .await?;
            t.access_key_install(
                &AccessKeyEntry {
                    access_key: "ANNA-LT".into(),
                    secret_key: "sk".into(),
                    username: "anna".into(),
                    status: "active".into(),
                    expires_at: None,
                    created_at: 44,
                    parent_ak: None,
                    session_policy: None,
                    kind: "long-term".into(),
                },
                7,
            )
            .await?;
            t.access_keys_delete_orphan_sessions().await
        })
    })
    .await;
    same_ok((s, o));
    support::dump_iam(&db, &obj).await;
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.permissions_clear().await?;
            t.permissions_clear().await
        })
    })
    .await;
    same_ok((s, o));
    support::dump_iam(&db, &obj).await;
}
