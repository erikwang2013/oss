//! E2: the 12 golden raft-guard exceptions. Admin users, admin-owned keys and
//! permissions, and session keys stay writable from Local origins in raft mode;
//! every action in `guards/matrix.json` must be allowed and reach a commit.

use xl_store::control_state::root::Mode;
use xl_store::control_store::iam::AccessKeyEntry;

use super::control_guards::{exception_actions, iam_v, raft, seed_alice, seed_user};
use super::support::{self, Obj};
use crate::control_store_obj::WriteOrigin;

fn entry(kind: &str, username: &str) -> AccessKeyEntry {
    AccessKeyEntry {
        access_key: "AKEXC".into(),
        secret_key: "sk".into(),
        username: username.into(),
        status: "active".into(),
        expires_at: None,
        created_at: 30,
        parent_ak: None,
        session_policy: None,
        kind: kind.into(),
    }
}

async fn seed_key(real: &Obj, origin: WriteOrigin, e: AccessKeyEntry) -> Result<(), String> {
    iam_v(real, origin, move |t| {
        Box::pin(async move { t.access_key_create(&e).await })
    })
    .await
}

async fn seed_perm(real: &Obj, origin: WriteOrigin, uid: i64) -> Result<(), String> {
    iam_v(real, origin, move |t| {
        Box::pin(async move { t.permission_upsert(uid, "bexc", true, false, true).await })
    })
    .await
}

fn ok<T>(action: &str, result: Result<T, String>) -> T {
    result.unwrap_or_else(|e| panic!("golden exception {action} must be allowed: {e}"))
}

#[tokio::test]
async fn local_exceptions_match_golden() {
    let actions = exception_actions();
    assert_eq!(actions.len(), 12);
    for action in actions {
        match action.as_str() {
            "insert admin user" => {
                let real = support::object_store(Mode::Raft).await;
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), |t| {
                        Box::pin(
                            async move { t.user_create_default_status("root", "h", true, 1).await },
                        )
                    })
                    .await,
                );
            }
            "update admin user" => {
                let real = support::object_store(Mode::Raft).await;
                let uid = ok(&action, seed_user(&real, raft(1, 1), "root", true).await);
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), move |t| {
                        Box::pin(async move { t.user_set_password(uid, "h2").await })
                    })
                    .await,
                );
            }
            "delete admin user" => {
                let real = support::object_store(Mode::Raft).await;
                let uid = ok(&action, seed_user(&real, raft(1, 1), "root", true).await);
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), move |t| {
                        Box::pin(async move { t.user_delete(uid).await })
                    })
                    .await,
                );
            }
            "insert session key" => {
                let real = support::object_store(Mode::Raft).await;
                ok(&action, seed_alice(&real, raft(1, 1)).await);
                ok(
                    &action,
                    seed_key(&real, WriteOrigin::local("test"), entry("session", "alice")).await,
                );
            }
            "update session key" => {
                let real = support::object_store(Mode::Raft).await;
                ok(&action, seed_alice(&real, raft(1, 1)).await);
                ok(
                    &action,
                    seed_key(&real, raft(1, 2), entry("session", "alice")).await,
                );
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), |t| {
                        Box::pin(async move { t.access_key_set_status("AKEXC", "disabled").await })
                    })
                    .await,
                );
            }
            "delete session key" => {
                let real = support::object_store(Mode::Raft).await;
                ok(&action, seed_alice(&real, raft(1, 1)).await);
                ok(
                    &action,
                    seed_key(&real, raft(1, 2), entry("session", "alice")).await,
                );
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), |t| {
                        Box::pin(async move { t.access_key_delete("AKEXC").await })
                    })
                    .await,
                );
            }
            "insert admin key" => {
                let real = support::object_store(Mode::Raft).await;
                ok(&action, seed_user(&real, raft(1, 1), "root", true).await);
                ok(
                    &action,
                    seed_key(
                        &real,
                        WriteOrigin::local("test"),
                        entry("long-term", "root"),
                    )
                    .await,
                );
            }
            "update admin key" => {
                let real = support::object_store(Mode::Raft).await;
                ok(&action, seed_user(&real, raft(1, 1), "root", true).await);
                ok(
                    &action,
                    seed_key(&real, raft(1, 2), entry("long-term", "root")).await,
                );
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), |t| {
                        Box::pin(async move { t.access_key_set_status("AKEXC", "disabled").await })
                    })
                    .await,
                );
            }
            "delete admin key" => {
                let real = support::object_store(Mode::Raft).await;
                ok(&action, seed_user(&real, raft(1, 1), "root", true).await);
                ok(
                    &action,
                    seed_key(&real, raft(1, 2), entry("long-term", "root")).await,
                );
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), |t| {
                        Box::pin(async move { t.access_key_delete("AKEXC").await })
                    })
                    .await,
                );
            }
            "insert admin permission" => {
                let real = support::object_store(Mode::Raft).await;
                let uid = ok(&action, seed_user(&real, raft(1, 1), "root", true).await);
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), move |t| {
                        Box::pin(async move {
                            t.permission_upsert(uid, "bexc", true, false, false).await
                        })
                    })
                    .await,
                );
            }
            "update admin permission" => {
                let real = support::object_store(Mode::Raft).await;
                let uid = ok(&action, seed_user(&real, raft(1, 1), "root", true).await);
                ok(&action, seed_perm(&real, raft(1, 2), uid).await);
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), move |t| {
                        Box::pin(async move {
                            t.permission_upsert(uid, "bexc", false, false, true).await
                        })
                    })
                    .await,
                );
            }
            "delete admin permission" => {
                let real = support::object_store(Mode::Raft).await;
                let uid = ok(&action, seed_user(&real, raft(1, 1), "root", true).await);
                ok(&action, seed_perm(&real, raft(1, 2), uid).await);
                ok(
                    &action,
                    iam_v(&real, WriteOrigin::local("test"), move |t| {
                        Box::pin(async move { t.permission_delete(uid, "bexc").await })
                    })
                    .await,
                );
            }
            other => panic!("golden names unknown exception action {other}"),
        }
    }
}
