//! E2 site-trigger extras: no-op update predicates, admin/session exclusions,
//! SiteApply suppression and raft-mode inactivity, all pinned by goldens.

use xl_store::control_state::root::AppliedLog;
use xl_store::control_store::ControlError;

use super::control_site_triggers::{
    Ent, Op, access_key, bucket, call, clock_rows, obj_rows, permission, policy, raft_site,
    run_hook, run_origin, standalone_site, user,
};
use super::golden;
use super::support::Obj;
use crate::control_store_obj::WriteOrigin;
use crate::control_store_obj::site_apply::{AccessKeySiteRow, PermissionSiteRow, UserSiteRow};

#[tokio::test]
async fn update_predicates_match_trigger_shape() {
    let real = standalone_site().await;
    let b = bucket("Off");
    run_hook(&real, move |txn| {
        Box::pin(async move { txn.site_hook_bucket(Some(&b), Some(&b), 1_000).await })
    })
    .await
    .unwrap();
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_bucket(Some(&bucket("Off")), Some(&bucket("Enabled")), 1_001)
                .await
        })
    })
    .await
    .unwrap();
    let p = policy(333);
    run_hook(&real, move |txn| {
        Box::pin(async move { txn.site_hook_policy(Some(&p), Some(&p), 1_002).await })
    })
    .await
    .unwrap();
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_policy(Some(&policy(333)), Some(&policy(334)), 1_003)
                .await
        })
    })
    .await
    .unwrap();
    let u = user("active");
    run_hook(&real, move |txn| {
        Box::pin(async move { txn.site_hook_user(Some(&u), Some(&u), 1_004).await })
    })
    .await
    .unwrap();
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_user(Some(&user("active")), Some(&user("disabled")), 1_005)
                .await
        })
    })
    .await
    .unwrap();
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_permission(Some(&permission(true)), Some(&permission(true)), 1_006)
                .await
        })
    })
    .await
    .unwrap();
    run_hook(&real, |txn| {
        Box::pin(async move {
            txn.site_hook_access_key(
                Some(&access_key("active")),
                Some(&access_key("active")),
                1_007,
            )
            .await
        })
    })
    .await
    .unwrap();
    let rows = obj_rows(&real).await;
    let entities: Vec<&str> = rows.iter().map(|r| r.entity.as_str()).collect();
    assert_eq!(
        entities,
        [
            "bucket",
            "bucket_policy",
            "user",
            "permission",
            "access_key"
        ]
    );
}

const EXCLUSIONS: [&str; 12] = [
    "insert admin user",
    "update admin user",
    "delete admin user",
    "insert session access key",
    "update session access key",
    "delete session access key",
    "insert admin-owned access key",
    "update admin-owned access key",
    "delete admin-owned access key",
    "insert admin-target permission",
    "update admin-target permission",
    "delete admin-target permission",
];

async fn exclude(real: &Obj, action: &str) {
    let admin = UserSiteRow {
        username: "admin-two".into(),
        password_hash: "h".into(),
        status: "active".into(),
        created_at: 112,
        is_admin: true,
    };
    let mut admin_off = admin.clone();
    admin_off.status = "disabled".into();
    let session = AccessKeySiteRow {
        access_key: "AKSESS-GOLDEN".into(),
        secret_key: "SK-SESS".into(),
        username: Some(super::control_site_triggers::USER.into()),
        status: "active".into(),
        expires_at: Some(999),
        created_at: 556,
        kind: "session".into(),
        owner_is_admin: Some(false),
    };
    let mut session_off = session.clone();
    session_off.status = "disabled".into();
    let admin_key = AccessKeySiteRow {
        access_key: "AKADMIN-GOLDEN".into(),
        secret_key: "SK-ADMIN".into(),
        username: Some("golden-root".into()),
        status: "active".into(),
        expires_at: None,
        created_at: 557,
        kind: "long-term".into(),
        owner_is_admin: Some(true),
    };
    let mut admin_key_off = admin_key.clone();
    admin_key_off.status = "disabled".into();
    let admin_perm = PermissionSiteRow {
        username: Some("golden-root".into()),
        bucket: "ex-b".into(),
        can_read: true,
        can_write: false,
        can_delete: false,
        owner_is_admin: Some(true),
    };
    let mut admin_perm_off = admin_perm.clone();
    admin_perm_off.can_read = false;
    let result = match action {
        "insert admin user" => hook_user(real, None, Some(admin)).await,
        "update admin user" => hook_user(real, Some(admin), Some(admin_off)).await,
        "delete admin user" => hook_user(real, Some(admin), None).await,
        "insert session access key" => hook_key(real, None, Some(session)).await,
        "update session access key" => hook_key(real, Some(session), Some(session_off)).await,
        "delete session access key" => hook_key(real, Some(session), None).await,
        "insert admin-owned access key" => hook_key(real, None, Some(admin_key)).await,
        "update admin-owned access key" => {
            hook_key(real, Some(admin_key), Some(admin_key_off)).await
        }
        "delete admin-owned access key" => hook_key(real, Some(admin_key), None).await,
        "insert admin-target permission" => hook_perm(real, None, Some(admin_perm)).await,
        "update admin-target permission" => {
            hook_perm(real, Some(admin_perm), Some(admin_perm_off)).await
        }
        "delete admin-target permission" => hook_perm(real, Some(admin_perm), None).await,
        other => panic!("unhandled golden exclusion action {other}"),
    };
    result.unwrap();
}

async fn hook_user(
    real: &Obj,
    old: Option<UserSiteRow>,
    new: Option<UserSiteRow>,
) -> Result<(), ControlError> {
    run_hook(real, move |txn| {
        Box::pin(async move { txn.site_hook_user(old.as_ref(), new.as_ref(), 1_000).await })
    })
    .await
}

async fn hook_key(
    real: &Obj,
    old: Option<AccessKeySiteRow>,
    new: Option<AccessKeySiteRow>,
) -> Result<(), ControlError> {
    run_hook(real, move |txn| {
        Box::pin(async move {
            txn.site_hook_access_key(old.as_ref(), new.as_ref(), 1_000)
                .await
        })
    })
    .await
}

async fn hook_perm(
    real: &Obj,
    old: Option<PermissionSiteRow>,
    new: Option<PermissionSiteRow>,
) -> Result<(), ControlError> {
    run_hook(real, move |txn| {
        Box::pin(async move {
            txn.site_hook_permission(old.as_ref(), new.as_ref(), 1_000)
                .await
        })
    })
    .await
}

#[tokio::test]
async fn exclusions_match_golden() {
    let golden = golden::load_json("site_triggers/exclusions.json");
    let actions: Vec<&str> = golden["actions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(actions, EXCLUSIONS);
    let real = standalone_site().await;
    for action in EXCLUSIONS {
        exclude(&real, action).await;
    }
    assert_eq!(obj_rows(&real).await.len(), 0);
    assert_eq!(clock_rows(&real).await.len(), 0);
    assert_eq!(golden["changelog_rows"], 0);
    assert_eq!(golden["clock_rows"], 0);
}

const SUPPRESSED: [&str; 5] = [
    "insert user",
    "insert bucket",
    "insert policy",
    "insert permission",
    "insert access key",
];

#[tokio::test]
async fn apply_suppression_matches_golden() {
    let golden = golden::load_json("site_triggers/apply_suppression.json");
    let actions: Vec<&str> = golden["actions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(actions, SUPPRESSED);
    let real = raft_site().await;
    run_origin(&real, WriteOrigin::site_apply("peer-b"), |txn| {
        Box::pin(async move {
            for (ent, op) in [
                (Ent::User, Op::Insert),
                (Ent::Bucket, Op::Insert),
                (Ent::Policy, Op::Insert),
                (Ent::Permission, Op::Insert),
                (Ent::AccessKey, Op::Insert),
            ] {
                call(txn, ent, op, 100).await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(obj_rows(&real).await.len(), 0);
    assert_eq!(clock_rows(&real).await.len(), 0);
    assert_eq!(golden["suppressed_changelog_rows"], 0);
    assert_eq!(golden["suppressed_clock_rows"], 0);

    hook_user(&real, None, Some(user("active"))).await.unwrap();
    assert_eq!(obj_rows(&real).await.len(), 1);
    assert_eq!(golden["after_reset_changelog_rows"], 1);
}

#[tokio::test]
async fn raft_mode_inactive_matches_golden() {
    let golden = golden::load_json("site_triggers/raft_mode_inactive.json");
    let matrix = golden::load_json("guards/matrix.json");
    let guards = matrix["cases"].as_array().unwrap().len() as i64;
    assert_eq!(guards, golden["raft_guard_trigger_count"].as_i64().unwrap());
    assert_eq!(guards, 33);
    assert_eq!(golden["site_trigger_count"], 0);

    let real = raft_site().await;
    run_origin(
        &real,
        WriteOrigin::raft_apply(AppliedLog { term: 1, index: 2 }),
        |txn| {
            Box::pin(async move {
                txn.site_hook_user(None, Some(&user("active")), 100).await?;
                txn.site_hook_bucket(None, Some(&bucket("Off")), 100)
                    .await?;
                txn.site_hook_policy(None, Some(&policy(333)), 100).await?;
                txn.site_hook_permission(None, Some(&permission(true)), 100)
                    .await?;
                txn.site_hook_access_key(None, Some(&access_key("active")), 100)
                    .await
            })
        },
    )
    .await
    .unwrap();
    assert_eq!(obj_rows(&real).await.len(), 0);
    assert_eq!(clock_rows(&real).await.len(), 0);
    assert_eq!(golden["changelog_rows"], 0);
    assert_eq!(golden["clock_rows"], 0);
}
