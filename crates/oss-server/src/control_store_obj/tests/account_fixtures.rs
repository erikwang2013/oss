//! Shared seed helpers for the B3 account parity tests.

use serde::Serialize;
use xl_store::control_store::iam::UserEntry;

use super::local;
use super::support::{self, both_account, both_iam, same_ok};
use crate::control_store_obj::TableId;
use crate::db::Db;

pub(super) const A: &str = "000000000002"; // derived id of alice (uid 2)
pub(super) const B: &str = "000000000003"; // derived id of bob (uid 3)
pub(super) const ADMIN_ACCT: &str = "000000000001";
pub(super) const DISABLED: &str = "200000000001";
pub(super) const DELETE_ACCT: &str = "200000000002";
pub(super) const EVE_ACCT: &str = "000000000006";

#[derive(Serialize)]
pub(super) struct TestBucket {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) owner_uid: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) owner_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) owner_username: Option<String>,
}

/// Seed the SQLite bucket row and the object row from the same owner uid.
pub(super) async fn bucket(db: &Db, obj: &support::Obj, name: &str, owner: Option<i64>) {
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES (?, ?, 10)")
        .bind(name)
        .bind(owner)
        .execute(db.pool())
        .await
        .unwrap();
    put_bucket(
        obj,
        name,
        TestBucket {
            owner_uid: owner,
            owner_id: None,
            owner_username: None,
        },
    )
    .await;
}

pub(super) async fn put_bucket(obj: &support::Obj, name: &str, row: TestBucket) {
    let mut txn = obj.control.begin(local()).await.unwrap();
    let bytes = rmp_serde::to_vec_named(&row).unwrap();
    txn.put(TableId::Buckets, name.as_bytes(), &bytes)
        .await
        .unwrap();
    txn.commit().await.unwrap();
}

async fn seed_users(db: &Db, obj: &support::Obj) {
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
            let dave = t.user_create_default_status("dave", "h", false, 25).await?;
            let eve = t
                .user_create(&UserEntry {
                    username: "eve".into(),
                    password_hash: "h".into(),
                    status: "active".into(),
                    created_at: 26,
                })
                .await?;
            Ok((dave, eve))
        })
    })
    .await;
    assert_eq!(same_ok((s, o)), (5, 6));
}

pub(super) async fn seed(db: &Db, obj: &support::Obj) {
    seed_users(db, obj).await;
    // alice and carol are members of A; bob is a member of B; dave belongs to a
    // disabled account; the admin member exists only to exercise the guards.
    for (name, owner) in [
        ("b-alice", Some(2)),
        ("b-carol", Some(4)),
        ("b-bob", Some(3)),
        ("b-dave", Some(5)),
        ("b-admin", Some(1)),
        ("b-null", None),
    ] {
        bucket(db, obj, name, owner).await;
    }
    let (s, o) = both_account(db, obj, |t| {
        Box::pin(async move {
            t.account_insert_if_absent(A, "alice-acct", Some(2), 100, 100)
                .await?;
            t.account_insert_derived_if_absent(B, "bob-derived", 3, 101, 101)
                .await?;
            t.account_insert_derived_if_absent(ADMIN_ACCT, "admin-derived", 1, 102, 102)
                .await?;
            t.account_insert_if_absent(DISABLED, "disabled-acct", None, 103, 103)
                .await?;
            t.account_set_status(DISABLED, "disabled", 104).await?;
            t.account_insert_derived_if_absent(EVE_ACCT, "eve-derived", 6, 105, 105)
                .await?;
            t.account_insert_if_absent(DELETE_ACCT, "delete-me", None, 106, 106)
                .await?;
            t.account_member_upsert(A, 2, "owner", 200).await?;
            t.account_member_upsert(A, 4, "member", 201).await?;
            t.account_member_upsert(B, 3, "owner", 202).await?;
            t.account_member_upsert(DISABLED, 5, "member", 203).await?;
            t.account_member_upsert(ADMIN_ACCT, 1, "owner", 204).await?;
            t.account_ledger_upsert(A, 1000, 5).await?;
            t.account_ledger_upsert(ADMIN_ACCT, 0, 0).await?;
            t.account_ledger_upsert(DELETE_ACCT, 0, 0).await
        })
    })
    .await;
    same_ok((s, o));
}
