//! B3 parity: object account/member/ledger reads and user-delete cascades.

use xl_store::control_state::root::Mode;
use xl_store::control_store::account::{AccountStore, AccountTxn};

use super::account_fixtures::{A, ADMIN_ACCT, B, DISABLED, EVE_ACCT, TestBucket, put_bucket, seed};
use super::local;
use super::support::{self, both_iam, same_ok};

#[tokio::test]
async fn reads_match_sqlite() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    seed(&db, &obj).await;
    let oracle = support::sqlite_store(&db);
    let object = &obj.control;

    support::dump_pair(
        "account(A)",
        oracle.account(A).await.unwrap(),
        object.account(A).await.unwrap(),
    );
    support::dump_pair(
        "account(missing)",
        oracle.account("999999999999").await.unwrap(),
        object.account("999999999999").await.unwrap(),
    );
    support::dump_pair(
        "accounts(None)",
        oracle.accounts(None).await.unwrap(),
        object.accounts(None).await.unwrap(),
    );
    support::dump_pair(
        "accounts(Some(4))",
        oracle.accounts(Some(4)).await.unwrap(),
        object.accounts(Some(4)).await.unwrap(),
    );
    support::dump_pair(
        "accounts(Some(99))",
        oracle.accounts(Some(99)).await.unwrap(),
        object.accounts(Some(99)).await.unwrap(),
    );
    support::dump_pair(
        "account_status",
        oracle.account_status(A).await.unwrap(),
        object.account_status(A).await.unwrap(),
    );
    assert_eq!(object.account_status("999999999999").await.unwrap(), None);
    support::dump_pair(
        "account_bucket_quota",
        oracle.account_bucket_quota(A).await.unwrap(),
        object.account_bucket_quota(A).await.unwrap(),
    );
    support::dump_pair(
        "account_hard_limits",
        oracle.account_hard_limits(A).await.unwrap(),
        object.account_hard_limits(A).await.unwrap(),
    );
    for uid in [2, 3, 4, 5, 99] {
        support::dump_pair(
            "account_member_account",
            oracle.account_member_account(uid).await.unwrap(),
            object.account_member_account(uid).await.unwrap(),
        );
    }
    for uid in [1, 2, 5, 99] {
        support::dump_pair(
            "account_active",
            oracle.account_active(uid).await.unwrap(),
            object.account_active(uid).await.unwrap(),
        );
    }
    assert!(!object.account_active(5).await.unwrap(), "disabled account");
    for (account, name) in [(A, "alice"), (A, "admin"), (A, "nobody"), (B, "bob")] {
        support::dump_pair(
            "member_uid",
            oracle.member_uid(account, name).await.unwrap(),
            object.member_uid(account, name).await.unwrap(),
        );
    }
    support::dump_pair(
        "member_role",
        oracle.member_role(A, 4).await.unwrap(),
        object.member_role(A, 4).await.unwrap(),
    );
    assert_eq!(object.member_role(A, 3).await.unwrap(), None);
    support::dump_pair(
        "account_members(A)",
        oracle.account_members(A).await.unwrap(),
        object.account_members(A).await.unwrap(),
    );
    support::dump_pair(
        "legacy_users_missing_accounts",
        oracle.legacy_users_missing_accounts().await.unwrap(),
        object.legacy_users_missing_accounts().await.unwrap(),
    );
    assert_eq!(
        object.legacy_users_missing_accounts().await.unwrap(),
        vec![(6, "eve".to_owned())]
    );
    assert_eq!(
        object.account_bucket_names(A).await.unwrap(),
        ["b-alice", "b-carol"]
    );
    assert_eq!(object.account_bucket_names(B).await.unwrap(), ["b-bob"]);
    assert_eq!(
        object.account_bucket_names(DISABLED).await.unwrap(),
        ["b-dave"],
        "the member row wins over the derived account id"
    );
    assert!(
        object
            .account_bucket_names("000000000005")
            .await
            .unwrap()
            .is_empty(),
        "a user with a member row is not also counted under the derived id"
    );
    assert!(
        object
            .account_bucket_names(ADMIN_ACCT)
            .await
            .unwrap()
            .is_empty()
    );
    support::dump_pair(
        "account_counts",
        oracle.account_counts().await.unwrap(),
        object.account_counts().await.unwrap(),
    );
    support::dump_pair(
        "account_ledger_count",
        oracle.account_ledger_count().await.unwrap(),
        object.account_ledger_count().await.unwrap(),
    );
    // Column parity for every table after the seed script.
    support::dump_accounts(&db, &obj).await;
}

#[tokio::test]
async fn user_delete_cascades_match_sqlite() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    seed(&db, &obj).await;

    // carol is an account member; eve is a non-admin account's admin_uid.
    let (s, o) = both_iam(&db, &obj, |t| {
        Box::pin(async move {
            t.user_delete_access_keys_permissions(4).await?;
            let carol = t.user_delete(4).await?;
            let eve = t.user_delete(6).await?;
            Ok((carol, eve))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (true, true));
    support::dump_accounts(&db, &obj).await;
    support::dump_iam(&db, &obj).await;

    let oracle = support::sqlite_store(&db);
    support::dump_pair(
        "account(A) after user delete",
        oracle.account(A).await.unwrap(),
        obj.control.account(A).await.unwrap(),
    );
    support::dump_pair(
        "account(EVE_ACCT) after user delete",
        oracle.account(EVE_ACCT).await.unwrap(),
        obj.control.account(EVE_ACCT).await.unwrap(),
    );
    assert_eq!(
        obj.control.account_members(A).await.unwrap().len(),
        1,
        "carol's member row is gone"
    );
    assert_eq!(
        obj.control
            .account(EVE_ACCT)
            .await
            .unwrap()
            .unwrap()
            .admin_uid,
        None,
        "admin_uid is cleared on user delete"
    );
    // carol's bucket loses its now-dangling owner in both account projections.
    assert_eq!(
        obj.control.account_bucket_names(A).await.unwrap(),
        ["b-alice"]
    );
    assert!(
        obj.control
            .legacy_users_missing_accounts()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn bucket_owner_fallbacks_are_consistent() {
    let obj = support::object_store(Mode::Standalone).await;
    support::object_iam(&obj.control, local(), |t| {
        Box::pin(async move { support::seed_iam(t).await })
    })
    .await
    .unwrap();

    // Local FK projection, legacy owner_id column, and natural username all
    // resolve to the same non-admin uid; a dangling uid resolves to nothing.
    put_bucket(
        &obj,
        "by-uid",
        TestBucket {
            owner_uid: Some(2),
            owner_id: None,
            owner_username: None,
        },
    )
    .await;
    put_bucket(
        &obj,
        "by-id",
        TestBucket {
            owner_uid: None,
            owner_id: Some(2),
            owner_username: None,
        },
    )
    .await;
    put_bucket(
        &obj,
        "by-name",
        TestBucket {
            owner_uid: None,
            owner_id: None,
            owner_username: Some("alice".into()),
        },
    )
    .await;
    put_bucket(
        &obj,
        "dangling",
        TestBucket {
            owner_uid: Some(999),
            owner_id: None,
            owner_username: None,
        },
    )
    .await;
    put_bucket(
        &obj,
        "admin-owned",
        TestBucket {
            owner_uid: Some(1),
            owner_id: None,
            owner_username: None,
        },
    )
    .await;

    assert_eq!(
        obj.control.account_bucket_names(A).await.unwrap(),
        ["by-id", "by-name", "by-uid"]
    );
    assert!(
        obj.control
            .account_bucket_names("000000000999")
            .await
            .unwrap()
            .is_empty(),
        "dangling owner is not attributed to its derived id"
    );
    assert!(
        obj.control
            .account_bucket_names(ADMIN_ACCT)
            .await
            .unwrap()
            .is_empty(),
        "admin-owned buckets count for no account"
    );

    let mut txn = obj.control.begin(local()).await.unwrap();
    txn.account_insert_if_absent(A, "alice", Some(4), 1, 1)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let reached = support::object_account(&obj.control, local(), |t| {
        Box::pin(async move { t.bucket_quota_reached(Some(2)).await })
    })
    .await
    .unwrap();
    assert!(!reached, "three buckets below quota 4");
    let mut txn = obj.control.begin(local()).await.unwrap();
    txn.account_set_bucket_quota(A, Some(2), 2).await.unwrap();
    txn.commit().await.unwrap();
    let reached = support::object_account(&obj.control, local(), |t| {
        Box::pin(async move { t.bucket_quota_reached(Some(2)).await })
    })
    .await
    .unwrap();
    assert!(reached);
}
