//! B3 parity: object account/member/ledger writes and quota semantics.

use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;
use xl_store::control_store::account::AccountStore;

use super::account_fixtures::{A, ADMIN_ACCT, B, DELETE_ACCT, DISABLED, EVE_ACCT, seed};
use super::support::{self, both_account, same_err, same_ok};
use crate::db::Db;

async fn check_quota(db: &Db, obj: &support::Obj, owner: Option<i64>) -> bool {
    let (s, o) = both_account(db, obj, |t| {
        Box::pin(async move { t.bucket_quota_reached(owner).await })
    })
    .await;
    same_ok((s, o))
}

#[tokio::test]
async fn writes_and_quota_match_sqlite() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    seed(&db, &obj).await;

    // Insert-or-ignore shapes (plain, system, derived).
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            let dup = t.account_insert_if_absent(A, "dup", None, 1, 1).await?;
            let sys = t
                .account_insert_if_absent("300000000009", "system-ish", None, 2, 2)
                .await?;
            let sys2 = t
                .account_system_insert_if_absent("300000000009", "system-ish", 3, 3)
                .await?;
            let der = t
                .account_insert_derived_if_absent("300000000010", "derived", 2, 4, 4)
                .await?;
            let der2 = t
                .account_insert_derived_if_absent("300000000010", "derived", 3, 5, 5)
                .await?;
            Ok((dup, sys, sys2, der, der2))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (false, true, false, true, false));
    support::dump_accounts(&db, &obj).await;

    // Scalar column updates.
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            t.account_set_name(A, "alice-renamed", 300).await?;
            t.account_set_bucket_quota(A, Some(5), 301).await?;
            t.account_set_soft_quota(A, Some(7), 302).await?;
            t.account_set_hard_limits(A, Some(1000), Some(9), 303).await
        })
    })
    .await;
    assert!(same_ok((s, o)));
    support::dump_accounts(&db, &obj).await;

    // Disable guard: an active global-admin member pins the account, a
    // non-admin member does not, and derived admin accounts go once empty.
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            let active = t.account_set_status(A, "active", 304).await?;
            let pinned = t.account_set_status(ADMIN_ACCT, "disabled", 305).await?;
            t.account_members_delete_admins().await?;
            let released = t.account_set_status(ADMIN_ACCT, "disabled", 306).await?;
            t.accounts_delete_admin_derived().await?;
            let disabled = t.account_set_status(DISABLED, "disabled", 307).await?;
            Ok((active, pinned, released, disabled))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (true, false, true, true));
    support::dump_accounts(&db, &obj).await;

    // Member moves preserve created_at; duplicate uid uses the exact error.
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            t.account_member_upsert(B, 4, "owner", 999).await?;
            let count = t.account_member_count(B).await?;
            let uid = t.member_uid(B, "carol").await?;
            Ok((count, uid))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (2, Some(4)));
    assert!(
        obj.control
            .account_members(B)
            .await
            .unwrap()
            .iter()
            .any(|m| m.uid == 4 && m.role == "owner")
    );
    support::dump_accounts(&db, &obj).await;
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { t.account_member_insert(B, 2, "member", 210).await })
    })
    .await;
    let (s, o) = same_err((s, o));
    assert!(matches!(s, ControlError::Backend(_)));
    assert_eq!(
        o.to_string(),
        "control store backend: UNIQUE constraint failed: account_members.uid"
    );
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            let present = t
                .account_member_insert_if_absent(B, 2, "member", 211)
                .await?;
            let absent = t
                .account_member_insert_if_absent(B, 6, "member", 212)
                .await?;
            Ok((present, absent))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (false, true));
    support::dump_accounts(&db, &obj).await;

    // Account admin is filled once and never overwritten.
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            t.account_set_admin_if_unset(DELETE_ACCT, 2).await?;
            t.account_set_admin_if_unset(DELETE_ACCT, 3).await
        })
    })
    .await;
    same_ok((s, o));
    support::dump_accounts(&db, &obj).await;
    assert_eq!(
        obj.control
            .account(DELETE_ACCT)
            .await
            .unwrap()
            .unwrap()
            .admin_uid,
        Some(2)
    );

    // Delete drops members but keeps the ledger row (no FK there).
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            let first = t.account_delete(DELETE_ACCT).await?;
            let second = t.account_delete(DELETE_ACCT).await?;
            let ledger = t.account_ledger(DELETE_ACCT).await?;
            Ok((first, second, ledger))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (true, false, Some((0, 0))));
    support::dump_accounts(&db, &obj).await;

    // Ledger upsert/read/clear.
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            t.account_ledger_upsert(A, 2000, 10).await?;
            t.account_ledger_upsert(A, 2500, 12).await?;
            let read = t.account_ledger(A).await?;
            let missing = t.account_ledger("999999999999").await?;
            let count = t.account_ledger_count().await?;
            t.account_ledger_clear().await?;
            let after = t.account_ledger_count().await?;
            Ok((read, missing, count, after))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (Some((2500, 12)), None, 3, 0));
    support::dump_accounts(&db, &obj).await;

    // Quota semantics: member row first, derived fallback second, disabled
    // accounts fail closed, missing accounts and non-positive quotas unlimited.
    assert!(!check_quota(&db, &obj, None).await);
    assert!(!check_quota(&db, &obj, Some(99)).await);
    assert!(check_quota(&db, &obj, Some(5)).await, "disabled account");
    assert!(
        !check_quota(&db, &obj, Some(6)).await,
        "account without quota"
    );
    assert!(!check_quota(&db, &obj, Some(7)).await, "missing account");

    // Carol already moved to B, so A has one bucket (b-alice) and B has two.
    for (quota, expected) in [
        (Some(1), true),
        (Some(2), false),
        (Some(0), true),
        (Some(-1), false),
        (None, false),
    ] {
        let (s, o) = both_account(&db, &obj, |t| {
            Box::pin(async move { t.account_set_bucket_quota(A, quota, 400).await })
        })
        .await;
        assert!(same_ok((s, o)));
        assert_eq!(
            check_quota(&db, &obj, Some(2)).await,
            expected,
            "A {quota:?}"
        );
    }
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            t.account_set_bucket_quota(B, Some(1), 401).await?;
            t.account_set_bucket_quota(EVE_ACCT, Some(0), 402).await
        })
    })
    .await;
    assert!(same_ok((s, o)));
    assert!(
        check_quota(&db, &obj, Some(3)).await,
        "derived B at 1 bucket"
    );
    assert!(
        check_quota(&db, &obj, Some(6)).await,
        "zero quota with no buckets"
    );

    // Moving carol to B moves her bucket's count (member row wins).
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { t.account_member_upsert(B, 4, "member", 999).await })
    })
    .await;
    same_ok((s, o));
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { t.account_set_bucket_quota(A, Some(1), 403).await })
    })
    .await;
    assert!(same_ok((s, o)));
    assert!(
        check_quota(&db, &obj, Some(2)).await,
        "A now has one bucket"
    );
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { t.account_set_bucket_quota(B, Some(2), 404).await })
    })
    .await;
    assert!(same_ok((s, o)));
    assert!(
        check_quota(&db, &obj, Some(3)).await,
        "B now has two buckets"
    );
    support::dump_accounts(&db, &obj).await;

    // Bucket counts in the runtime view agree with the SQL projection.
    let oracle = support::sqlite_store(&db);
    support::dump_pair(
        "account(A) after quota writes",
        oracle.account(A).await.unwrap(),
        obj.control.account(A).await.unwrap(),
    );
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move {
            Ok((
                t.account_bucket_count(A).await?,
                t.account_bucket_count(B).await?,
            ))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (1, 2));
}
