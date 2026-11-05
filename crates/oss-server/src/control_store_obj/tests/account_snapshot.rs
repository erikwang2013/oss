//! B3 strict account snapshot export/install parity vs SQLite.

use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;
use xl_store::control_store::account::{
    AccountEntry, AccountMemberEntry, AccountStore, AccountUsageLedgerEntry,
};

use super::super::account::AccountRow;
use super::support::{self, both_account, same_err, same_ok};
use super::{ObjectControlStore, local};
use crate::control_store_obj::{TableId, WriteOrigin};
use crate::db::Db;

type Snap = (
    Vec<AccountEntry>,
    Vec<AccountMemberEntry>,
    Vec<AccountUsageLedgerEntry>,
);

async fn seed_users(db: &Db, obj: &support::Obj) {
    support::sqlite_iam(db, |t| Box::pin(async move { support::seed_iam(t).await }))
        .await
        .unwrap();
    support::object_iam(&obj.control, local(), |t| {
        Box::pin(async move { support::seed_iam(t).await })
    })
    .await
    .unwrap();
}

async fn seed_accounts(t: &mut dyn support::Account) -> Result<(), ControlError> {
    t.account_insert_derived_if_absent("000000000002", "alice-acct", 2, 100, 101)
        .await?;
    t.account_set_bucket_quota("000000000002", Some(3), 102)
        .await?;
    t.account_set_soft_quota("000000000002", Some(10), 103)
        .await?;
    t.account_insert_if_absent("300000000001", "plain", None, 104, 105)
        .await?;
    t.account_set_soft_quota("300000000001", Some(5), 106)
        .await?;
    t.account_insert_if_absent("300000000002", "off", None, 107, 108)
        .await?;
    t.account_set_status("300000000002", "disabled", 109)
        .await?;
    t.account_member_upsert("000000000002", 2, "owner", 200)
        .await?;
    t.account_member_upsert("000000000002", 3, "admin", 201)
        .await?;
    t.account_member_upsert("300000000001", 4, "member", 202)
        .await?;
    t.account_ledger_upsert("000000000002", 123, 4).await?;
    t.account_ledger_upsert("300000000001", 0, 0).await
}

async fn seed(db: &Db, obj: &support::Obj) {
    support::mirror_sqlite_baseline(db, obj).await;
    seed_users(db, obj).await;
    let (s, o) = both_account(db, obj, |t| Box::pin(async move { seed_accounts(t).await })).await;
    same_ok((s, o));
}

async fn sqlite_snapshot(db: &Db) -> Result<Snap, ControlError> {
    support::sqlite_account(db, |t| {
        Box::pin(async move {
            Ok((
                t.snapshot_accounts().await?,
                t.snapshot_account_members().await?,
                t.snapshot_account_usage_ledger().await?,
            ))
        })
    })
    .await
}

async fn object_snapshot(obj: &support::Obj) -> Result<Snap, ControlError> {
    support::object_account(&obj.control, WriteOrigin::snapshot_install(), |t| {
        Box::pin(async move {
            Ok((
                t.snapshot_accounts().await?,
                t.snapshot_account_members().await?,
                t.snapshot_account_usage_ledger().await?,
            ))
        })
    })
    .await
}

async fn marker(control: &ObjectControlStore) -> Option<String> {
    control
        .pin()
        .await
        .unwrap()
        .root
        .meta
        .markers
        .get(crate::db::META_ACCOUNTS_CANONICAL)
        .cloned()
}

#[tokio::test]
async fn strict_export_matches_sqlite_and_serializes_identically() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    seed(&db, &obj).await;

    let s = sqlite_snapshot(&db).await.unwrap();
    let o = object_snapshot(&obj).await.unwrap();
    assert_eq!(s, o, "strict snapshot entries");
    assert_eq!(
        rmp_serde::to_vec_named(&s).unwrap(),
        rmp_serde::to_vec_named(&o).unwrap(),
        "strict snapshot bytes"
    );

    let ids: Vec<&str> = o.0.iter().map(|a| a.id.as_str()).collect();
    assert_eq!(ids, ["000000000002", "300000000001", "300000000002"]);
    assert_eq!(o.0[0].admin.as_deref(), Some("alice"));
    assert_eq!(
        (o.0[0].bucket_quota, o.0[0].soft_byte_quota),
        (Some(3), Some(10))
    );
    assert_eq!((o.0[0].created_at, o.0[0].updated_at), (100, 103));
    assert_eq!(o.0[1].admin, None);
    assert_eq!(o.0[2].status, "disabled");
    let members: Vec<(&str, &str, &str)> =
        o.1.iter()
            .map(|m| (m.account_id.as_str(), m.username.as_str(), m.role.as_str()))
            .collect();
    assert_eq!(
        members,
        [
            ("000000000002", "alice", "owner"),
            ("000000000002", "bob", "admin"),
            ("300000000001", "carol", "member"),
        ]
    );
    let ledger: Vec<(&str, i64, i64)> =
        o.2.iter()
            .map(|l| (l.account_id.as_str(), l.used_bytes, l.object_count))
            .collect();
    assert_eq!(ledger, [("000000000002", 123, 4), ("300000000001", 0, 0)]);
}

async fn install_valid(t: &mut dyn support::Account, canonical: bool) -> Result<(), ControlError> {
    let accounts = [
        AccountEntry {
            id: "400000000001".into(),
            name: "new".into(),
            status: "active".into(),
            admin: Some("alice".into()),
            bucket_quota: Some(1),
            soft_byte_quota: None,
            created_at: 300,
            updated_at: 301,
        },
        AccountEntry {
            id: "400000000002".into(),
            name: "new-off".into(),
            status: "disabled".into(),
            admin: None,
            bucket_quota: None,
            soft_byte_quota: Some(2),
            created_at: 302,
            updated_at: 303,
        },
    ];
    let members = [
        AccountMemberEntry {
            account_id: "400000000001".into(),
            username: "alice".into(),
            role: "owner".into(),
            created_at: 400,
        },
        AccountMemberEntry {
            account_id: "400000000002".into(),
            username: "bob".into(),
            role: "member".into(),
            created_at: 401,
        },
    ];
    t.replace_accounts(&accounts, &members, canonical).await
}

#[tokio::test]
async fn install_replaces_tables_and_sets_canonical_marker() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    seed(&db, &obj).await;

    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { install_valid(t, false).await })
    })
    .await;
    same_ok((s, o));
    support::dump_accounts(&db, &obj).await;
    // Db::connect canonicalizes at startup; a non-canonical install must not
    // change whatever marker the baseline already had.
    assert_eq!(
        db.schema_meta(crate::db::META_ACCOUNTS_CANONICAL)
            .await
            .unwrap(),
        marker(&obj.control).await,
        "non-canonical install keeps the baseline marker"
    );

    // The ledger is not touched by the account install.
    assert_eq!(
        obj.control.account_ledger_count().await.unwrap(),
        2,
        "ledger rows survive replace_accounts"
    );

    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { install_valid(t, true).await })
    })
    .await;
    same_ok((s, o));
    support::dump_accounts(&db, &obj).await;
    assert_eq!(
        db.schema_meta(crate::db::META_ACCOUNTS_CANONICAL)
            .await
            .unwrap(),
        Some("done".into())
    );
    assert_eq!(marker(&obj.control).await, Some("done".into()));

    // Repeat install with the same rows is idempotent (marker stays set).
    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { install_valid(t, true).await })
    })
    .await;
    same_ok((s, o));
    assert_eq!(marker(&obj.control).await, Some("done".into()));
}

async fn replace_bad_id(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [AccountEntry {
        id: "123".into(),
        name: "bad".into(),
        status: "active".into(),
        admin: None,
        bucket_quota: None,
        soft_byte_quota: None,
        created_at: 1,
        updated_at: 1,
    }];
    t.replace_accounts(&accounts, &[], false).await
}

async fn replace_bad_status(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [AccountEntry {
        id: "300000000003".into(),
        name: "bad".into(),
        status: "gone".into(),
        admin: None,
        bucket_quota: None,
        soft_byte_quota: None,
        created_at: 1,
        updated_at: 1,
    }];
    t.replace_accounts(&accounts, &[], false).await
}

fn account_entry(id: &str, admin: Option<&str>) -> AccountEntry {
    AccountEntry {
        id: id.into(),
        name: "acct".into(),
        status: "active".into(),
        admin: admin.map(str::to_owned),
        bucket_quota: None,
        soft_byte_quota: None,
        created_at: 1,
        updated_at: 1,
    }
}

async fn replace_bad_role(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [account_entry("300000000001", None)];
    let members = [AccountMemberEntry {
        account_id: "300000000001".into(),
        username: "alice".into(),
        role: "boss".into(),
        created_at: 2,
    }];
    t.replace_accounts(&accounts, &members, false).await
}

async fn replace_bad_admin(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [account_entry("300000000001", Some("nobody"))];
    t.replace_accounts(&accounts, &[], false).await
}

async fn replace_global_admin(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [account_entry("300000000001", Some("admin"))];
    t.replace_accounts(&accounts, &[], false).await
}

async fn replace_bad_member(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [account_entry("300000000001", None)];
    let members = [AccountMemberEntry {
        account_id: "300000000001".into(),
        username: "nobody".into(),
        role: "member".into(),
        created_at: 2,
    }];
    t.replace_accounts(&accounts, &members, false).await
}

async fn replace_global_admin_member(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [account_entry("300000000001", None)];
    let members = [AccountMemberEntry {
        account_id: "300000000001".into(),
        username: "admin".into(),
        role: "member".into(),
        created_at: 2,
    }];
    t.replace_accounts(&accounts, &members, false).await
}

async fn replace_duplicate_account(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [
        account_entry("300000000001", None),
        account_entry("300000000001", None),
    ];
    t.replace_accounts(&accounts, &[], false).await
}

async fn replace_duplicate_member(t: &mut dyn support::Account) -> Result<(), ControlError> {
    let accounts = [
        account_entry("300000000001", None),
        account_entry("300000000002", None),
    ];
    let members = [
        AccountMemberEntry {
            account_id: "300000000001".into(),
            username: "alice".into(),
            role: "owner".into(),
            created_at: 2,
        },
        AccountMemberEntry {
            account_id: "300000000002".into(),
            username: "alice".into(),
            role: "member".into(),
            created_at: 3,
        },
    ];
    t.replace_accounts(&accounts, &members, false).await
}

fn assert_invalid(pair: (Result<(), ControlError>, Result<(), ControlError>), expected: &str) {
    let (s, o) = same_err(pair);
    assert!(matches!(s, ControlError::Invalid(_)), "{s:?}");
    assert_eq!(s.to_string(), o.to_string(), "invalid text parity");
    assert_eq!(o.to_string(), format!("invalid control data: {expected}"));
}

#[tokio::test]
async fn install_errors_match_sqlite() {
    let db = support::sqlite(false).await;
    let obj = support::object_store(Mode::Standalone).await;
    seed_users(&db, &obj).await;

    let pair = both_account(&db, &obj, |t| Box::pin(replace_bad_id(t))).await;
    assert_invalid(pair, "account id \"123\" is not 12 ASCII digits");
    let pair = both_account(&db, &obj, |t| Box::pin(replace_bad_status(t))).await;
    assert_invalid(pair, "account 300000000003 status \"gone\" invalid");
    let pair = both_account(&db, &obj, |t| Box::pin(replace_bad_role(t))).await;
    assert_invalid(
        pair,
        "account 300000000001 member alice role \"boss\" invalid",
    );
    let pair = both_account(&db, &obj, |t| Box::pin(replace_bad_admin(t))).await;
    assert_invalid(
        pair,
        "account admin \"nobody\" is not a replicated non-admin user",
    );
    let pair = both_account(&db, &obj, |t| Box::pin(replace_global_admin(t))).await;
    assert_invalid(
        pair,
        "account admin \"admin\" is not a replicated non-admin user",
    );
    let pair = both_account(&db, &obj, |t| Box::pin(replace_bad_member(t))).await;
    assert_invalid(
        pair,
        "account member \"nobody\" is not a replicated non-admin user",
    );
    let pair = both_account(&db, &obj, |t| Box::pin(replace_global_admin_member(t))).await;
    assert_invalid(
        pair,
        "account member \"admin\" is not a replicated non-admin user",
    );
    // All failures rolled back.
    assert_eq!(obj.control.account_counts().await.unwrap(), (0, 0));
    support::dump_accounts(&db, &obj).await;

    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { replace_duplicate_account(t).await })
    })
    .await;
    let (s, o) = same_err((s, o));
    assert!(matches!(s, ControlError::Backend(_)));
    assert_eq!(
        o.to_string(),
        "control store backend: UNIQUE constraint failed: accounts.id"
    );

    let (s, o) = both_account(&db, &obj, |t| {
        Box::pin(async move { replace_duplicate_member(t).await })
    })
    .await;
    let (s, o) = same_err((s, o));
    assert!(matches!(s, ControlError::Backend(_)));
    assert_eq!(
        o.to_string(),
        "control store backend: UNIQUE constraint failed: account_members.uid"
    );
    support::dump_accounts(&db, &obj).await;
}

#[tokio::test]
async fn dangling_admin_uid_is_not_exportable() {
    let obj = support::object_store(Mode::Standalone).await;
    let mut txn = obj.control.begin(local()).await.unwrap();
    let row = AccountRow {
        id: "500000000001".into(),
        name: "dangling".into(),
        status: "active".into(),
        admin_uid: Some(999),
        bucket_quota: None,
        soft_byte_quota: None,
        hard_byte_quota: None,
        object_quota: None,
        created_at: 1,
        updated_at: 1,
    };
    txn.put(
        TableId::Accounts,
        row.id.as_bytes(),
        &rmp_serde::to_vec_named(&row).unwrap(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    let err = object_snapshot(&obj).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid control data: account 500000000001 admin_uid 999 is missing or a global admin (not exportable)"
    );
}
