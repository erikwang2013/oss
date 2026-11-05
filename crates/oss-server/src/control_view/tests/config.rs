//! Bucket config getters, policies, AP/OLAP/VPC registries, sealed SKs.

use xl_s3::control::{ControlAcl, ControlAuth, KeyLookup};

use super::super::*;
use super::helpers::*;

#[tokio::test]
async fn bucket_config_policies_and_registries() {
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
        TableId::Users,
        &uid_key(8),
        &user(8, "admin", true, "active"),
    )
    .await;
    let mut cfg = bucket("cfg", Some(1));
    cfg.versioning = "Enabled".into();
    cfg.object_lock = true;
    cfg.object_lock_default_mode = Some("GOVERNANCE".into());
    cfg.object_lock_default_days = Some(7);
    cfg.default_encryption = Some("AES256".into());
    cfg.replication_mode = "sync".into();
    put(&mut t, TableId::Buckets, b"cfg", &cfg).await;
    put(
        &mut t,
        TableId::Buckets,
        b"plain",
        &bucket("plain", Some(1)),
    )
    .await;
    put(
        &mut t,
        TableId::BucketPolicies,
        b"cfg",
        &policy("cfg", r#"{"Statement":[]}"#),
    )
    .await;
    put(
        &mut t,
        TableId::BucketPolicies,
        b"plain",
        &policy("plain", "{bad"),
    )
    .await;
    put(
        &mut t,
        TableId::EventConfigs,
        b"cfg",
        &event("cfg", r#"{"Queue":[]}"#),
    )
    .await;
    put(
        &mut t,
        TableId::AccessKeys,
        b"ASIA_POLICY",
        &key(
            "ASIA_POLICY",
            "sk",
            1,
            "session",
            "active",
            None,
            None,
            Some(r#"{"Statement":[]}"#),
        ),
    )
    .await;
    put(
        &mut t,
        TableId::AccessKeys,
        b"ASIA_BAD_POLICY",
        &key(
            "ASIA_BAD_POLICY",
            "sk",
            1,
            "session",
            "active",
            None,
            None,
            Some("{bad"),
        ),
    )
    .await;
    put(
        &mut t,
        TableId::AccessKeys,
        b"AK_LONG",
        &key("AK_LONG", "sk", 1, "long-term", "active", None, None, None),
    )
    .await;

    put(
        &mut t,
        TableId::AccessPoints,
        b"ap-b",
        &StoredAp {
            name: "ap-b".into(),
            bucket: "cfg".into(),
            owner_id: 1,
            policy_json: None,
            created_at: 1,
            updated_at: 1,
        },
    )
    .await;
    put(
        &mut t,
        TableId::AccessPoints,
        b"ap-a",
        &StoredAp {
            name: "ap-a".into(),
            bucket: "plain".into(),
            owner_id: 0,
            policy_json: Some(r#"{"Statement":[]}"#.into()),
            created_at: 1,
            updated_at: 1,
        },
    )
    .await;
    put(
        &mut t,
        TableId::AccessPoints,
        b"ap-c",
        &StoredAp {
            name: "ap-c".into(),
            bucket: "plain".into(),
            owner_id: 999,
            policy_json: None,
            created_at: 1,
            updated_at: 1,
        },
    )
    .await;
    put(
        &mut t,
        TableId::OlapConfigs,
        b"ap-b",
        &OlapConfigEntry {
            access_point: "ap-b".into(),
            kind: "python".into(),
            endpoint: Some("http://olap".into()),
            module: Some(vec![1, 2]),
            module_sha256: Some("sha".into()),
            timeout_ms: 1000,
            max_response_bytes: 1024,
            updated_at: 1,
        },
    )
    .await;
    put(
        &mut t,
        TableId::VpcEndpoints,
        b"vpce-1",
        &VpcEndpoint {
            vpce_id: "vpce-1".into(),
            vpc_id: "vpc-1".into(),
            account_id: "000000000001".into(),
            state: "available".into(),
            created_at: 1,
            updated_at: 1,
        },
    )
    .await;
    put(&mut t, TableId::Sites, b"peer1", &site("peer1")).await;
    put(
        &mut t,
        TableId::SiteConfig,
        &[1u8],
        &StoredSiteConfig {
            id: "site-a".into(),
            name: "Site A".into(),
            region: "us-east-1".into(),
            priority: 1,
            site_token: "secret-token".into(),
        },
    )
    .await;
    t.commit().await.unwrap();

    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();
    let v = cache.capture();

    assert_eq!(v.bucket_versioning("cfg").await.unwrap(), "Enabled");
    assert_eq!(v.bucket_versioning("missing").await.unwrap(), "Off");
    assert_eq!(
        v.bucket_replication_mode_row("cfg")
            .await
            .unwrap()
            .as_deref(),
        Some("sync")
    );
    assert_eq!(
        v.bucket_replication_mode_row("missing").await.unwrap(),
        None
    );
    assert_eq!(v.bucket_replication_mode("missing").await.unwrap(), "async");
    assert_eq!(
        v.bucket_lock("cfg").await.unwrap(),
        Some((true, Some("GOVERNANCE".into()), Some(7)))
    );
    assert_eq!(v.bucket_lock("missing").await.unwrap(), None);
    assert_eq!(
        v.bucket_policy("cfg")
            .await
            .unwrap()
            .unwrap()
            .statement
            .len(),
        0
    );
    assert!(v.bucket_policy("plain").await.is_err());
    assert!(v.bucket_policy("missing").await.unwrap().is_none());
    assert_eq!(
        v.bucket_notification("cfg").await.unwrap().as_deref(),
        Some(r#"{"Queue":[]}"#)
    );
    assert_eq!(
        v.bucket_default_encryption("cfg").await.unwrap().as_deref(),
        Some("AES256")
    );
    assert_eq!(v.bucket_default_encryption("plain").await.unwrap(), None);
    assert!(v.session_policy_of("ASIA_POLICY").await.unwrap().is_some());
    assert!(v.session_policy_of("ASIA_BAD_POLICY").await.is_err());
    assert!(v.session_policy_of("AK_LONG").await.unwrap().is_none());
    assert!(v.session_policy_of("").await.unwrap().is_none());
    assert!(v.session_policy_of("missing").await.unwrap().is_none());

    let aps = v.view.access_points();
    assert_eq!(
        aps.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(),
        vec!["ap-a", "ap-b", "ap-c"]
    );
    assert_eq!(aps[0].owner, OwnerRef::Admin);
    assert_eq!(aps[1].owner, OwnerRef::User("alice".into()));
    assert_eq!(aps[2].owner, OwnerRef::Admin);
    assert_eq!(v.view.access_point("ap-b").unwrap().bucket, "cfg");
    assert!(v.view.access_point("missing").is_none());
    let olap = v.view.olap_config("ap-b").unwrap();
    assert_eq!((olap.kind.as_str(), olap.timeout_ms), ("python", 1000));
    assert!(v.view.olap_config("missing").is_none());
    assert_eq!(v.view.vpc_endpoint("vpce-1").unwrap().state, "available");
    assert!(v.view.vpc_endpoint("missing").is_none());
    assert_eq!(v.site_peer_count().await.unwrap(), 1);
    assert_eq!(v.view.site_config.as_ref().unwrap().id, "site-a");
}

#[tokio::test]
async fn sealed_secret_is_returned_verbatim_and_debug_redacts() {
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
        b"AK_SEALED",
        &key(
            "AK_SEALED",
            "enc:v1:c2VhbGVk",
            1,
            "long-term",
            "active",
            None,
            None,
            None,
        ),
    )
    .await;
    t.commit().await.unwrap();
    let cache = ViewCache::load(Arc::clone(&fx.control)).await.unwrap();
    let v = cache.capture();
    let lookup = v.key_auth("AK_SEALED", 100).await.unwrap();
    match &lookup {
        KeyLookup::Valid(k) => assert_eq!(k.secret_key, "enc:v1:c2VhbGVk"),
        other => panic!("expected valid key, got {other:?}"),
    }
    let debug = format!("{lookup:?} {:?}", v.view);
    assert!(!debug.contains("c2VhbGVk"), "{debug}");
    assert!(debug.contains("redacted"));
}
