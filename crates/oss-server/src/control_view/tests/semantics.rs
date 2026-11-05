//! SQLite-parity semantics: auth predicates, ACL, quotas, bucket config.

use xl_s3::access::Action;
use xl_s3::control::{ControlAcl, ControlAuth, KeyLookup};

use super::super::*;
use super::helpers::*;

const NOW: i64 = 100;

fn expect_valid(lookup: KeyLookup, uid: i64, admin: bool) {
    match lookup {
        KeyLookup::Valid(k) => {
            assert_eq!((k.uid, k.is_admin), (uid, admin));
        }
        other => panic!("expected valid key, got {other:?}"),
    }
}

async fn seed_auth(fx: &Fx) {
    let mut t = txn(fx).await;
    for row in [
        user(1, "alice", false, "active"),
        user(2, "bob", false, "disabled"),
        user(3, "root", true, "active"),
        user(4, "carol", false, "active"),
        user(5, "orphan", false, "active"),
    ] {
        put(&mut t, TableId::Users, &uid_key(row.uid), &row).await;
    }
    put(
        &mut t,
        TableId::Accounts,
        b"acct-active",
        &account("acct-active", "active", None, None, None),
    )
    .await;
    put(
        &mut t,
        TableId::Accounts,
        b"acct-disabled",
        &account("acct-disabled", "disabled", None, None, None),
    )
    .await;
    put(
        &mut t,
        TableId::AccountMembers,
        &uid_key(1),
        &member("acct-active", 1),
    )
    .await;
    put(
        &mut t,
        TableId::AccountMembers,
        &uid_key(4),
        &member("acct-disabled", 4),
    )
    .await;
    put(
        &mut t,
        TableId::AccountMembers,
        &uid_key(5),
        &member("acct-missing", 5),
    )
    .await;

    let keys = [
        key("AK_OK", "sk-ok", 1, "long-term", "active", None, None, None),
        key(
            "AK_DISABLED",
            "sk-d",
            1,
            "long-term",
            "inactive",
            None,
            None,
            None,
        ),
        key(
            "AK_EXPIRED",
            "sk-e",
            1,
            "long-term",
            "active",
            Some(50),
            None,
            None,
        ),
        key(
            "AK_BOB",
            "sk-bob",
            2,
            "long-term",
            "active",
            None,
            None,
            None,
        ),
        key(
            "AK_CAROL",
            "sk-carol",
            4,
            "long-term",
            "active",
            None,
            None,
            None,
        ),
        key(
            "AK_ORPHAN",
            "sk-o",
            5,
            "long-term",
            "active",
            None,
            None,
            None,
        ),
        key(
            "AK_ROOT",
            "sk-root",
            3,
            "long-term",
            "active",
            None,
            None,
            None,
        ),
        key(
            "AK_PARENT_EXP",
            "sk-pe",
            1,
            "long-term",
            "active",
            Some(50),
            None,
            None,
        ),
        key(
            "ASIA_OK",
            "sk-s1",
            1,
            "session",
            "active",
            Some(500),
            Some("AK_OK"),
            None,
        ),
        key(
            "ASIA_MISSING",
            "sk-s2",
            1,
            "session",
            "active",
            Some(500),
            Some("AK_NOPE"),
            None,
        ),
        key(
            "ASIA_BOB",
            "sk-s3",
            1,
            "session",
            "active",
            Some(500),
            Some("AK_BOB"),
            None,
        ),
        key(
            "ASIA_PARENT_EXPIRED",
            "sk-s4",
            1,
            "session",
            "active",
            Some(500),
            Some("AK_PARENT_EXP"),
            None,
        ),
        key(
            "ASIA_EXPIRED",
            "sk-s5",
            1,
            "session",
            "active",
            Some(50),
            Some("AK_OK"),
            None,
        ),
        key(
            "ASIA_INACTIVE",
            "sk-s6",
            1,
            "session",
            "inactive",
            Some(50),
            Some("AK_OK"),
            None,
        ),
    ];
    for row in keys {
        put(&mut t, TableId::AccessKeys, row.access_key.as_bytes(), &row).await;
    }
    t.commit().await.unwrap();
}

#[tokio::test]
async fn auth_matches_sqlite_predicates() {
    let fx = fixture().await;
    seed_auth(&fx).await;
    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();
    let v = cache.capture();
    let now = NOW;

    expect_valid(v.key_auth("AK_OK", now).await.unwrap(), 1, false);
    expect_valid(v.key_auth("AK_ROOT", now).await.unwrap(), 3, true);
    expect_valid(v.key_auth("ASIA_OK", now).await.unwrap(), 1, false);
    assert_eq!(v.key_auth("NOPE", now).await.unwrap(), KeyLookup::Invalid);
    for invalid in [
        "AK_DISABLED",
        "AK_EXPIRED",
        "AK_BOB",
        "AK_CAROL",
        "AK_ORPHAN",
        "ASIA_MISSING",
        "ASIA_BOB",
        "ASIA_PARENT_EXPIRED",
        "ASIA_INACTIVE",
    ] {
        assert_eq!(
            v.key_auth(invalid, now).await.unwrap(),
            KeyLookup::Invalid,
            "{invalid}"
        );
    }
    assert_eq!(
        v.key_auth("ASIA_EXPIRED", now).await.unwrap(),
        KeyLookup::ExpiredSession
    );

    let sts = v.sts_caller("AK_OK", now).await.unwrap().unwrap();
    assert_eq!(
        (sts.uid, sts.is_admin, sts.ak.as_str()),
        (1, false, "AK_OK")
    );
    assert!(v.sts_caller("ASIA_OK", now).await.unwrap().is_none());
    assert!(v.sts_caller("AK_EXPIRED", now).await.unwrap().is_none());
    assert_eq!(v.sts_caller("AK_ROOT", now).await.unwrap().unwrap().uid, 3);

    assert_eq!(v.active_user_uid("alice").await.unwrap(), Some(1));
    assert_eq!(v.active_user_uid("root").await.unwrap(), Some(3));
    assert_eq!(v.active_user_uid("bob").await.unwrap(), None);
    assert_eq!(v.active_user_uid("nobody").await.unwrap(), None);

    let user = v.user_of_key("ASIA_OK", now).await.unwrap().unwrap();
    assert_eq!(user.ak, "ASIA_OK");
    assert!(v.user_of_key("ASIA_EXPIRED", now).await.unwrap().is_none());
}

#[tokio::test]
async fn acl_owner_admin_bits_and_visibility() {
    let fx = fixture().await;
    let mut t = txn(&fx).await;
    for row in [
        user(1, "alice", false, "active"),
        user(2, "bob", false, "active"),
        user(3, "root", true, "active"),
    ] {
        put(&mut t, TableId::Users, &uid_key(row.uid), &row).await;
    }
    put(
        &mut t,
        TableId::Buckets,
        b"owned",
        &bucket("owned", Some(1)),
    )
    .await;
    put(
        &mut t,
        TableId::Buckets,
        b"shared",
        &bucket("shared", Some(2)),
    )
    .await;
    put(
        &mut t,
        TableId::Buckets,
        b"only-me",
        &bucket("only-me", Some(1)),
    )
    .await;
    put(
        &mut t,
        TableId::Permissions,
        b"shared",
        &perm(1, "shared", true, false, false),
    )
    .await;
    put(
        &mut t,
        TableId::Permissions,
        b"no-perm",
        &perm(1, "no-perm", false, false, false),
    )
    .await;
    put(
        &mut t,
        TableId::Buckets,
        b"no-perm",
        &bucket("no-perm", Some(2)),
    )
    .await;
    t.commit().await.unwrap();

    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();
    let v = cache.capture();
    let alice = v.user_of_key("", 0).await.unwrap(); // no such key
    assert!(alice.is_none());
    let alice = xl_s3::access::AuthedUser {
        uid: 1,
        is_admin: false,
        ak: "AK1".into(),
    };
    let bob = xl_s3::access::AuthedUser {
        uid: 2,
        is_admin: false,
        ak: "AK2".into(),
    };
    let root = xl_s3::access::AuthedUser {
        uid: 3,
        is_admin: true,
        ak: "AK3".into(),
    };

    for action in [Action::Read, Action::Write, Action::Delete] {
        assert!(v.can(&alice, action, "owned").await.unwrap());
        assert!(v.can(&bob, action, "shared").await.unwrap());
        assert!(v.can(&root, action, "shared").await.unwrap());
    }
    assert!(v.can(&alice, Action::Read, "shared").await.unwrap());
    assert!(!v.can(&alice, Action::Write, "shared").await.unwrap());
    assert!(!v.can(&alice, Action::Delete, "shared").await.unwrap());
    assert!(!v.can(&alice, Action::Read, "no-perm").await.unwrap());
    assert!(!v.can(&alice, Action::Read, "missing").await.unwrap());

    assert!(v.is_owner("owned", 1).await.unwrap());
    assert!(!v.is_owner("owned", 2).await.unwrap());
    assert!(!v.is_owner("missing", 1).await.unwrap());

    assert_eq!(
        v.visible(1).await.unwrap(),
        vec!["only-me".to_string(), "owned".to_string(), "shared".into()]
    );
    assert_eq!(
        v.visible(2).await.unwrap(),
        vec!["no-perm".to_string(), "shared".into()]
    );
    assert!(v.visible(9).await.unwrap().is_empty());
}

#[tokio::test]
async fn account_membership_quota_and_ledger() {
    let fx = fixture().await;
    let mut t = txn(&fx).await;
    for (uid, name) in [
        (7, "user7"),
        (8, "admin"),
        (9, "nine"),
        (12, "twelve"),
        (13, "off"),
        (14, "fourteen"),
    ] {
        let admin = uid == 8;
        put(
            &mut t,
            TableId::Users,
            &uid_key(uid),
            &user(uid, name, admin, "active"),
        )
        .await;
    }
    put(
        &mut t,
        TableId::Accounts,
        b"acct-team",
        &account("acct-team", "active", Some(1), Some(500), None),
    )
    .await;
    put(
        &mut t,
        TableId::Accounts,
        b"000000000007",
        &account("000000000007", "active", Some(99), None, None),
    )
    .await;
    put(
        &mut t,
        TableId::Accounts,
        b"000000000009",
        &account("000000000009", "disabled", None, Some(9), None),
    )
    .await;
    put(
        &mut t,
        TableId::Accounts,
        b"acct-off",
        &account("acct-off", "disabled", Some(0), Some(100), None),
    )
    .await;
    put(
        &mut t,
        TableId::Accounts,
        b"000000000014",
        &account("000000000014", "active", Some(0), None, None),
    )
    .await;
    put(
        &mut t,
        TableId::AccountMembers,
        &uid_key(7),
        &member("acct-team", 7),
    )
    .await;
    put(
        &mut t,
        TableId::AccountMembers,
        &uid_key(13),
        &member("acct-off", 13),
    )
    .await;
    for (name, owner) in [
        ("b1", Some(7)),
        ("b2", Some(7)),
        ("b3", Some(13)),
        ("b4", Some(8)),
        ("b5", Some(11)),
        ("b6", Some(12)),
        ("b7", Some(14)),
        ("b-none", None),
    ] {
        put(
            &mut t,
            TableId::Buckets,
            name.as_bytes(),
            &bucket(name, owner),
        )
        .await;
    }
    put(
        &mut t,
        TableId::AccountUsageLedger,
        b"acct-team",
        &ledger("acct-team", 1234, 5),
    )
    .await;
    t.commit().await.unwrap();

    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();
    let v = cache.capture();

    assert_eq!(
        v.account_id_of_user(7).await.unwrap().as_deref(),
        Some("acct-team")
    );
    assert_eq!(
        v.account_id_of_user(9).await.unwrap().as_deref(),
        Some("000000000009")
    );
    assert_eq!(v.account_id_of_user(0).await.unwrap(), None);
    assert_eq!(
        v.account_identity_of_user(7).await.unwrap(),
        (Some("acct-team".into()), true)
    );
    assert_eq!(
        v.account_identity_of_user(13).await.unwrap(),
        (Some("acct-off".into()), false)
    );
    assert_eq!(
        v.account_identity_of_user(9).await.unwrap(),
        (Some("000000000009".into()), false)
    );
    assert_eq!(
        v.account_identity_of_user(10).await.unwrap(),
        (Some("000000000010".into()), true)
    );
    assert_eq!(v.account_identity_of_user(0).await.unwrap(), (None, false));

    assert_eq!(
        v.account_status("acct-team").await.unwrap().as_deref(),
        Some("active")
    );
    assert_eq!(v.account_status("nope").await.unwrap(), None);

    assert!(v.bucket_quota_exceeded(7).await.unwrap());
    assert!(v.bucket_quota_exceeded(13).await.unwrap());
    assert!(v.bucket_quota_exceeded(14).await.unwrap());
    assert!(!v.bucket_quota_exceeded(12).await.unwrap());
    assert!(!v.bucket_quota_exceeded(0).await.unwrap());
    assert!(!v.bucket_quota_exceeded(8).await.unwrap());

    let quota = v.account_quota_for_bucket("b1").await.unwrap().unwrap();
    assert_eq!(quota.account, "acct-team");
    assert!(!quota.disabled);
    assert_eq!(quota.limits.hard_bytes, Some(500));
    assert_eq!(quota.limits.objects, None);
    let quota = v.account_quota_for_bucket("b3").await.unwrap().unwrap();
    assert_eq!(quota.account, "acct-off");
    assert!(quota.disabled);
    assert!(v.account_quota_for_bucket("b4").await.unwrap().is_none());
    assert!(v.account_quota_for_bucket("b5").await.unwrap().is_none());
    assert!(v.account_quota_for_bucket("b6").await.unwrap().is_none());
    assert!(
        v.account_quota_for_bucket("b-none")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        v.account_quota_for_bucket("missing")
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        v.account_ledger("acct-team").await.unwrap(),
        Some((1234, 5))
    );
    assert_eq!(v.account_ledger("nope").await.unwrap(), None);

    assert_eq!(
        v.bucket_owner_display("b1").await.unwrap(),
        Some(("7".into(), "user7".into()))
    );
    assert_eq!(
        v.bucket_owner_display("b-none").await.unwrap(),
        Some(("8".into(), "admin".into()))
    );
    assert_eq!(
        v.bucket_owner_display("missing").await.unwrap(),
        Some(("8".into(), "admin".into()))
    );
    assert_eq!(v.view.bucket_count("acct-team"), 2);
    assert_eq!(v.view.bucket_count("acct-off"), 1);
}
