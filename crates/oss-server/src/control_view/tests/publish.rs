//! Publication: revocation visibility, snapshot isolation, gap rebuild, cap.

use xl_s3::control::{ControlAuth, KeyLookup};

use crate::control_store_obj::ObjectControlTxn;

use super::super::*;
use super::helpers::*;

const POLICY_JSON: &str = r#"{"Statement":[]}"#;

#[tokio::test]
async fn revocation_and_user_delete_visible_on_next_request() {
    let fx = fixture().await;
    let mut t = txn(&fx).await;
    put(
        &mut t,
        TableId::Users,
        &uid_key(1),
        &user(1, "alice", false, "active"),
    )
    .await;
    put(
        &mut t,
        TableId::AccessKeys,
        b"AK1",
        &key("AK1", "sk", 1, "long-term", "active", None, None, None),
    )
    .await;
    t.commit().await.unwrap();

    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();
    let before = cache.capture();
    assert!(matches!(
        before.key_auth("AK1", 100).await.unwrap(),
        KeyLookup::Valid(_)
    ));

    let mut t = txn(&fx).await;
    put(
        &mut t,
        TableId::AccessKeys,
        b"AK1",
        &key("AK1", "sk", 1, "long-term", "inactive", None, None, None),
    )
    .await;
    let stamp = t.commit().await.unwrap();
    cache.publish(CacheDelta::from_stamp(&stamp)).await.unwrap();
    let after = cache.capture();
    assert_eq!(
        after.key_auth("AK1", 100).await.unwrap(),
        KeyLookup::Invalid
    );
    assert!(matches!(
        before.key_auth("AK1", 100).await.unwrap(),
        KeyLookup::Valid(_)
    ));
    assert!(after.generation > before.generation);
    assert!(Arc::ptr_eq(&before.view.users, &after.view.users));
    assert!(!Arc::ptr_eq(&before.view.keys, &after.view.keys));

    let mut t = txn(&fx).await;
    put(
        &mut t,
        TableId::AccessKeys,
        b"AK1",
        &key("AK1", "sk", 1, "long-term", "active", None, None, None),
    )
    .await;
    let stamp = t.commit().await.unwrap();
    cache.publish(CacheDelta::from_stamp(&stamp)).await.unwrap();
    assert!(matches!(
        cache.capture().key_auth("AK1", 100).await.unwrap(),
        KeyLookup::Valid(_)
    ));

    let mut t = txn(&fx).await;
    assert!(t.delete(TableId::Users, &uid_key(1)).await.unwrap());
    let stamp = t.commit().await.unwrap();
    cache.publish(CacheDelta::from_stamp(&stamp)).await.unwrap();
    assert_eq!(
        cache.capture().key_auth("AK1", 100).await.unwrap(),
        KeyLookup::Invalid
    );
}

#[tokio::test]
async fn publish_gap_and_snapshot_install_rebuild_all() {
    let fx = fixture().await;
    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();

    let mut t = txn(&fx).await;
    put(
        &mut t,
        TableId::Users,
        &uid_key(1),
        &user(1, "alice", false, "active"),
    )
    .await;
    let _missed = t.commit().await.unwrap();

    let mut t = txn(&fx).await;
    put(&mut t, TableId::Buckets, b"b1", &bucket("b1", Some(1))).await;
    let stamp = t.commit().await.unwrap();
    cache.publish(CacheDelta::from_stamp(&stamp)).await.unwrap();
    assert_eq!(
        *cache.capture().view,
        *HotView::load(&fx.control).await.unwrap()
    );

    let mut t = txn(&fx).await;
    put(&mut t, TableId::Buckets, b"b2", &bucket("b2", Some(1))).await;
    t.commit().await.unwrap();
    cache.rebuild().await.unwrap();
    assert_eq!(
        *cache.capture().view,
        *HotView::load(&fx.control).await.unwrap()
    );

    let generation = cache.capture().generation;
    let stamp = txn(&fx).await.commit().await.unwrap();
    if !stamp.committed {
        cache.publish(CacheDelta::from_stamp(&stamp)).await.unwrap();
        assert_eq!(cache.capture().generation, generation);
    }
}

#[tokio::test]
async fn empty_commit_advances_generation_and_next_data_commit_merges() {
    let fx = fixture().await;
    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();
    let mut t = txn(&fx).await;
    t.set_applied(Some(xl_store::control_state::root::AppliedLog {
        term: 1,
        index: 1,
    }));
    let empty = t.commit().await.unwrap();
    assert!(
        empty.committed && empty.dirty_tables.is_empty(),
        "{empty:?}"
    );
    let before = cache.capture();
    cache.publish(CacheDelta::from_stamp(&empty)).await.unwrap();
    let after = cache.capture();
    assert_eq!(after.generation, empty.generation);
    assert!(Arc::ptr_eq(&before.view.users, &after.view.users));

    let mut t = txn(&fx).await;
    put(&mut t, TableId::Buckets, b"b1", &bucket("b1", Some(1))).await;
    let data = t.commit().await.unwrap();
    assert!(data.committed && !data.dirty_tables.is_empty(), "{data:?}");
    cache.publish(CacheDelta::from_stamp(&data)).await.unwrap();
    let merged = cache.capture();
    assert_eq!(merged.generation, data.generation);
    assert!(merged.view.buckets.contains_key("b1"));
    assert!(
        Arc::ptr_eq(&after.view.users, &merged.view.users),
        "the data publish must merge domains, not full-reload"
    );
}

#[tokio::test]
async fn memory_cap_fails_closed() {
    let fx = fixture().await;
    let err = HotView::load_with_cap(&fx.control, 1).await.unwrap_err();
    assert!(matches!(err, ControlError::Invalid(_)), "{err:?}");
}

async fn random_put(t: &mut ObjectControlTxn, s: &mut u64) {
    let uid = (lcg(s) % 5) as i64 + 1;
    let bucket_name = format!("b{}", lcg(s) % 5);
    let status = if lcg(s).is_multiple_of(3) {
        "disabled"
    } else {
        "active"
    };
    match lcg(s) % 14 {
        0 => {
            let row = user(
                uid,
                &format!("u{uid}"),
                lcg(s).is_multiple_of(4),
                if lcg(s).is_multiple_of(4) {
                    "disabled"
                } else {
                    status
                },
            );
            put(t, TableId::Users, &uid_key(uid), &row).await;
        }
        1 => {
            let ak = format!("k{}", lcg(s) % 5);
            let kind = if lcg(s).is_multiple_of(2) {
                "long-term"
            } else {
                "session"
            };
            let row = AccessKeyRow {
                access_key: ak.clone(),
                secret_key: format!("enc:v1:s{}", lcg(s) % 7),
                user_id: uid,
                status: if lcg(s).is_multiple_of(4) {
                    "inactive".into()
                } else {
                    "active".into()
                },
                expires_at: if lcg(s).is_multiple_of(5) {
                    Some(50)
                } else {
                    None
                },
                created_at: 1,
                parent_ak: (kind == "session").then(|| format!("k{}", lcg(s) % 5)),
                session_policy: (lcg(s).is_multiple_of(3)).then(|| POLICY_JSON.to_string()),
                kind: kind.into(),
            };
            put(t, TableId::AccessKeys, ak.as_bytes(), &row).await;
        }
        2 => {
            let row = perm(
                uid,
                &bucket_name,
                lcg(s).is_multiple_of(2),
                lcg(s).is_multiple_of(2),
                lcg(s).is_multiple_of(2),
            );
            put(t, TableId::Permissions, bucket_name.as_bytes(), &row).await;
        }
        3 => {
            let id = if lcg(s).is_multiple_of(2) {
                format!("{uid:012}")
            } else {
                format!("acct{}", lcg(s) % 3)
            };
            let row = account(
                &id,
                status,
                (lcg(s).is_multiple_of(3)).then(|| lcg(s) as i64 % 3),
                (lcg(s).is_multiple_of(3)).then_some(1000),
                None,
            );
            put(t, TableId::Accounts, id.as_bytes(), &row).await;
        }
        4 => {
            let id = if lcg(s).is_multiple_of(2) {
                format!("{uid:012}")
            } else {
                format!("acct{}", lcg(s) % 3)
            };
            put(t, TableId::AccountMembers, &uid_key(uid), &member(&id, uid)).await;
        }
        5 => {
            let id = format!("acct{}", lcg(s) % 3);
            put(
                t,
                TableId::AccountUsageLedger,
                id.as_bytes(),
                &ledger(&id, lcg(s) as i64, lcg(s) as i64),
            )
            .await;
        }
        6 => {
            let mut b = bucket(&bucket_name, (!lcg(s).is_multiple_of(3)).then_some(uid));
            b.versioning = if lcg(s).is_multiple_of(2) {
                "Enabled"
            } else {
                "Off"
            }
            .into();
            b.object_lock = lcg(s).is_multiple_of(2);
            b.default_encryption = (lcg(s).is_multiple_of(2)).then(|| "AES256".to_string());
            b.object_lock_default_mode =
                (lcg(s).is_multiple_of(2)).then(|| "GOVERNANCE".to_string());
            b.object_lock_default_days = (lcg(s).is_multiple_of(2)).then_some(7);
            b.replication_mode = if lcg(s).is_multiple_of(2) {
                "sync"
            } else {
                "async"
            }
            .into();
            put(t, TableId::Buckets, bucket_name.as_bytes(), &b).await;
        }
        7 => {
            put(
                t,
                TableId::BucketPolicies,
                bucket_name.as_bytes(),
                &policy(&bucket_name, POLICY_JSON),
            )
            .await;
        }
        8 => {
            put(
                t,
                TableId::EventConfigs,
                bucket_name.as_bytes(),
                &event(&bucket_name, r#"{"Queue":[]}"#),
            )
            .await;
        }
        9 => {
            let name = format!("ap{}", lcg(s) % 3);
            let row = StoredAp {
                name: name.clone(),
                bucket: bucket_name,
                owner_id: if lcg(s).is_multiple_of(3) { 0 } else { uid },
                policy_json: None,
                created_at: 1,
                updated_at: 1,
            };
            put(t, TableId::AccessPoints, name.as_bytes(), &row).await;
        }
        10 => {
            let name = format!("ap{}", lcg(s) % 3);
            let row = OlapConfigEntry {
                access_point: name.clone(),
                kind: "python".into(),
                endpoint: Some("http://olap".into()),
                module: Some(vec![1]),
                module_sha256: None,
                timeout_ms: 10,
                max_response_bytes: 10,
                updated_at: 1,
            };
            put(t, TableId::OlapConfigs, name.as_bytes(), &row).await;
        }
        11 => {
            let id = format!("vpce-{}", lcg(s) % 3);
            let row = VpcEndpoint {
                vpce_id: id.clone(),
                vpc_id: "vpc".into(),
                account_id: format!("{uid:012}"),
                state: "available".into(),
                created_at: 1,
                updated_at: 1,
            };
            put(t, TableId::VpcEndpoints, id.as_bytes(), &row).await;
        }
        12 => {
            let peer = format!("peer{}", lcg(s) % 3);
            put(t, TableId::Sites, peer.as_bytes(), &site(&peer)).await;
        }
        _ => {
            let row = StoredSiteConfig {
                id: "site".into(),
                name: "site".into(),
                region: "us-east-1".into(),
                priority: 1,
                site_token: "tok".into(),
            };
            put(t, TableId::SiteConfig, &[1u8], &row).await;
        }
    }
}

async fn random_delete(t: &mut ObjectControlTxn, s: &mut u64) {
    let uid = (lcg(s) % 5) as i64 + 1;
    let bucket_name = format!("b{}", lcg(s) % 5);
    let existed = match lcg(s) % 10 {
        0 => t.delete(TableId::Users, &uid_key(uid)).await,
        1 => {
            let key = format!("k{}", lcg(s) % 5);
            t.delete(TableId::AccessKeys, key.as_bytes()).await
        }
        2 => t.delete(TableId::Permissions, bucket_name.as_bytes()).await,
        3 => t.delete(TableId::AccountMembers, &uid_key(uid)).await,
        4 => t.delete(TableId::Buckets, bucket_name.as_bytes()).await,
        5 => {
            let key = format!("{uid:012}");
            t.delete(TableId::Accounts, key.as_bytes()).await
        }
        6 => {
            t.delete(TableId::BucketPolicies, bucket_name.as_bytes())
                .await
        }
        7 => {
            t.delete(TableId::EventConfigs, bucket_name.as_bytes())
                .await
        }
        8 => {
            let key = format!("ap{}", lcg(s) % 3);
            t.delete(TableId::AccessPoints, key.as_bytes()).await
        }
        _ => {
            let key = format!("peer{}", lcg(s) % 3);
            t.delete(TableId::Sites, key.as_bytes()).await
        }
    };
    existed.unwrap();
}

#[tokio::test]
async fn published_cache_equals_fresh_scan_after_random_transactions() {
    let fx = fixture().await;
    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    for i in 0..30 {
        let mut t = txn(&fx).await;
        for _ in 0..1 + lcg(&mut seed) % 4 {
            if lcg(&mut seed).is_multiple_of(5) {
                random_delete(&mut t, &mut seed).await;
            } else {
                random_put(&mut t, &mut seed).await;
            }
        }
        let stamp = t.commit().await.unwrap();
        if stamp.committed {
            cache.publish(CacheDelta::from_stamp(&stamp)).await.unwrap();
        }
        let fresh = HotView::load(&fx.control).await.unwrap();
        let (l, r) = (&*cache.capture().view, &*fresh);
        assert_eq!(l.generation, r.generation, "generation {i}");
        assert_eq!(l.users, r.users, "users {i}");
        assert_eq!(l.usernames, r.usernames, "usernames {i}");
        assert_eq!(l.keys, r.keys, "keys {i}");
        assert_eq!(l.accounts, r.accounts, "accounts {i}");
        assert_eq!(l.members, r.members, "members {i}");
        assert_eq!(l.ledger, r.ledger, "ledger {i}");
        assert_eq!(l.buckets, r.buckets, "buckets {i}");
        assert_eq!(l.policies, r.policies, "policies {i}");
        assert_eq!(l.events, r.events, "events {i}");
        assert_eq!(l.permissions, r.permissions, "permissions {i}");
        assert_eq!(l.aps, r.aps, "aps {i}");
        assert_eq!(l.olap, r.olap, "olap {i}");
        assert_eq!(l.vpcs, r.vpcs, "vpcs {i}");
        assert_eq!(l.site_config, r.site_config, "site_config {i}");
        assert_eq!(l.sites, r.sites, "sites {i}");
        assert_eq!(l.bucket_counts, r.bucket_counts, "bucket_counts {i}");
        assert_eq!(l.admin_uid, r.admin_uid, "admin_uid {i}");
    }
}
