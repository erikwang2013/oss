//! C1 raft-guard matrix, last-admin zero-write/re-evaluation, and uid stability.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use xl_store::control_state::root::AppliedLog;
use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{AccessKeyEntry, IamTxn, UserEntry};
use xl_store::error::Error;

use super::support::{self, both_iam, same_ok};
use super::{ObjectControlStore, RootIo, local};
use crate::control_store_obj::WriteOrigin;

const RAFT_GUARD: &str = "raft: control write outside apply";

fn gate<T>(result: Result<T, ControlError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(e) if e.to_string().contains(RAFT_GUARD) => "guard",
        Err(_) => "other",
    }
}

fn to_error(e: ControlError) -> Error {
    Error::InvalidArgument(e.to_string())
}

#[tokio::test]
async fn raft_guard_matrix_matches_sqlite() {
    let db = support::sqlite(true).await;
    let obj = support::object_store(Mode::Raft).await;
    support::set_apply_ctx(&db, true).await;
    support::sqlite_iam(&db, |t| Box::pin(async move { support::seed_iam(t).await }))
        .await
        .unwrap();
    support::object_iam(
        &obj.control,
        WriteOrigin::raft_apply(AppliedLog { term: 1, index: 1 }),
        |t| Box::pin(async move { support::seed_iam(t).await }),
    )
    .await
    .unwrap();
    support::set_apply_ctx(&db, false).await;

    let mut oracle = Vec::new();
    let mut object = Vec::new();
    macro_rules! step {
        ($body:expr) => {{
            let (s, o) = both_iam(&db, &obj, $body).await;
            oracle.push(gate(s));
            object.push(gate(o));
        }};
    }
    step!(|t| Box::pin(
        async move { t.user_create_default_status("newbie", "h", false, 50).await }
    ));
    step!(|t| Box::pin(async move { t.user_create_default_status("admin2", "h", true, 51).await }));
    step!(|t| Box::pin(async move { t.user_set_password(2, "x").await }));
    step!(|t| Box::pin(async move { t.user_set_password(1, "x").await }));
    step!(|t| Box::pin(async move { t.user_delete(4).await }));
    step!(|t| Box::pin(async move { t.user_delete(5).await }));
    step!(|t| Box::pin(async move { t.permission_upsert(4, "g", true, true, true).await }));
    step!(|t| Box::pin(async move { t.permission_upsert(1, "g", true, true, true).await }));
    step!(|t| Box::pin(async move { t.permission_delete(1, "b1").await }));
    step!(|t| Box::pin(async move {
        t.access_key_create(&AccessKeyEntry {
            access_key: "AKALICE2".into(),
            secret_key: "sk".into(),
            username: "alice".into(),
            status: "active".into(),
            expires_at: None,
            created_at: 60,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        })
        .await
    }));
    step!(|t| Box::pin(async move {
        t.access_key_create(&AccessKeyEntry {
            access_key: "SESS2".into(),
            secret_key: "sk".into(),
            username: "alice".into(),
            status: "active".into(),
            expires_at: None,
            created_at: 61,
            parent_ak: None,
            session_policy: None,
            kind: "session".into(),
        })
        .await
    }));
    step!(|t| Box::pin(async move {
        t.access_key_create(&AccessKeyEntry {
            access_key: "AKADMIN2".into(),
            secret_key: "sk".into(),
            username: "admin".into(),
            status: "active".into(),
            expires_at: None,
            created_at: 62,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        })
        .await
    }));
    step!(|t| Box::pin(async move { t.access_key_set_status("AKALICE", "disabled").await }));
    step!(|t| Box::pin(async move { t.access_key_set_status("SESSALICE", "disabled").await }));
    step!(|t| Box::pin(async move { t.access_key_delete("AKALICE").await }));
    step!(|t| Box::pin(async move { t.access_key_delete("SESSALICE").await }));
    step!(|t| Box::pin(async move { t.access_key_delete("AKADMIN").await }));

    let expected = vec![
        "guard", "ok", "guard", "ok", "guard", "ok", "guard", "ok", "ok", "guard", "ok", "ok",
        "guard", "ok", "guard", "ok", "ok",
    ];
    assert_eq!(object, expected, "object guard matrix");
    assert_eq!(oracle, expected, "sqlite guard matrix");
    support::dump_iam(&db, &obj).await;

    // Replicated origins bypass the guards: SnapshotInstall and RaftApply.
    support::set_apply_ctx(&db, true).await;
    let s = support::sqlite_iam(&db, |t| {
        Box::pin(async move {
            t.user_install(&UserEntry {
                username: "snapuser".into(),
                password_hash: "h".into(),
                status: "active".into(),
                created_at: 70,
            })
            .await
        })
    })
    .await
    .unwrap();
    let o = support::object_iam(&obj.control, WriteOrigin::snapshot_install(), |t| {
        Box::pin(async move {
            t.user_install(&UserEntry {
                username: "snapuser".into(),
                password_hash: "h".into(),
                status: "active".into(),
                created_at: 70,
            })
            .await
        })
    })
    .await
    .unwrap();
    assert_eq!(s, o, "uid after snapshot install");
    let s = support::sqlite_iam(&db, |t| {
        Box::pin(async move {
            t.user_create(&UserEntry {
                username: "raftuser".into(),
                password_hash: "h".into(),
                status: "active".into(),
                created_at: 71,
            })
            .await
        })
    })
    .await
    .unwrap();
    let o = support::object_iam(
        &obj.control,
        WriteOrigin::raft_apply(AppliedLog { term: 1, index: 9 }),
        |t| {
            Box::pin(async move {
                t.user_create(&UserEntry {
                    username: "raftuser".into(),
                    password_hash: "h".into(),
                    status: "active".into(),
                    created_at: 71,
                })
                .await
            })
        },
    )
    .await
    .unwrap();
    assert_eq!(s, o, "uid after raft apply");
    support::dump_iam(&db, &obj).await;
}

#[tokio::test]
async fn last_admin_guard_is_zero_write() {
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
        Box::pin(async move { t.user_create_default_status("admin2", "h", true, 90).await })
    })
    .await;
    assert_eq!(same_ok((s, o)), 5);
    support::dump_iam(&db, &obj).await;

    // Deleting admin 1 and then asking for the last-admin guard on admin 2 in
    // the same transaction is refused without staging any write for admin 2.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            // Keys/permissions first: SQLite FKs reject deleting a referenced
            // user, and the runtime delete_user does the same.
            t.user_delete_access_keys_permissions(1).await?;
            assert!(t.user_delete(1).await?);
            t.user_delete_guard(5).await
        })
    })
    .await;
    assert!(!same_ok((s, o)));
    support::dump_iam(&db, &obj).await;
}

#[tokio::test]
async fn last_admin_guard_reevaluates_after_conflict() {
    let obj = support::object_store(Mode::Standalone).await;
    support::object_iam(&obj.control, local(), |t| {
        Box::pin(async move {
            support::seed_iam(t).await?;
            t.user_create_default_status("admin2", "h", true, 90).await
        })
    })
    .await
    .unwrap();

    // First closure call pins two admins and another writer deletes admin 1;
    // the staged delete of admin 2 conflicts. The retry re-reads one admin and
    // refuses, so the last admin is never removed by replay of the overlay.
    let control = Arc::clone(&obj.control);
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let writer = Arc::clone(&control);
    let result: Result<(), Error> = control
        .transaction(local(), move |txn| {
            let writer = Arc::clone(&writer);
            let attempt = seen.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt == 0 {
                    let mut other = writer.begin(WriteOrigin::local("other")).await?;
                    assert!(other.user_delete_guard(1).await.map_err(to_error)?);
                    other.user_delete(1).await.map_err(to_error)?;
                    other.commit().await?;
                }
                let allowed = txn.user_delete_guard(5).await.map_err(to_error)?;
                if !allowed {
                    return Err(Error::InvalidArgument("last admin".into()));
                }
                txn.user_delete(5).await.map_err(to_error)?;
                Ok(())
            })
        })
        .await;
    assert!(
        matches!(result, Err(Error::InvalidArgument(_))),
        "{result:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "closure must be retried");
    let mut txn = control.begin(local()).await.unwrap();
    assert!(txn.user_uid("admin").await.unwrap().is_none());
    drop(txn);
    let mut txn = control.begin(local()).await.unwrap();
    assert_eq!(txn.user_uid("admin2").await.unwrap(), Some(5));
}

#[tokio::test]
async fn uid_never_reused_across_restart_and_install() {
    let obj = support::object_store(Mode::Standalone).await;
    let uids = support::object_iam(&obj.control, local(), |t| {
        Box::pin(async move {
            let admin = t.user_create_default_status("admin", "h", true, 1).await?;
            let u2 = t
                .user_create(&UserEntry {
                    username: "u2".into(),
                    password_hash: "h".into(),
                    status: "active".into(),
                    created_at: 2,
                })
                .await?;
            let u3 = t
                .user_create(&UserEntry {
                    username: "u3".into(),
                    password_hash: "h".into(),
                    status: "active".into(),
                    created_at: 3,
                })
                .await?;
            Ok((admin, u2, u3))
        })
    })
    .await
    .unwrap();
    assert_eq!(uids, (1, 2, 3));

    // A new store handle over the same root continues the persisted sequence.
    let restarted = Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(Arc::clone(
        &obj.store,
    )))));
    let next = support::object_iam(&restarted, local(), |t| {
        Box::pin(async move {
            t.user_create(&UserEntry {
                username: "u4".into(),
                password_hash: "h".into(),
                status: "active".into(),
                created_at: 4,
            })
            .await
        })
    })
    .await
    .unwrap();
    assert_eq!(next, 4);

    // Snapshot install deletes non-admin users but never reuses their uids.
    let installed = support::object_iam(&restarted, WriteOrigin::snapshot_install(), |t| {
        Box::pin(async move {
            t.users_delete_non_admin().await?;
            let one = t
                .user_install(&UserEntry {
                    username: "fresh1".into(),
                    password_hash: "h".into(),
                    status: "active".into(),
                    created_at: 5,
                })
                .await?;
            let two = t
                .user_install(&UserEntry {
                    username: "fresh2".into(),
                    password_hash: "h".into(),
                    status: "active".into(),
                    created_at: 6,
                })
                .await?;
            Ok((one, two))
        })
    })
    .await
    .unwrap();
    assert_eq!(installed, (5, 6));
    let mut txn = restarted.begin(local()).await.unwrap();
    assert_eq!(txn.user_uid("admin").await.unwrap(), Some(1));
    drop(txn);
    let mut txn = restarted.begin(local()).await.unwrap();
    assert_eq!(txn.user_uid("fresh1").await.unwrap(), Some(5));
}
