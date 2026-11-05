//! 13-table bucket-delete cascade parity: AP+OLAP (2), lifecycle/inventory/tier
//! (7), bucket configs (3), buckets last. The other bucket (`b2`) must survive.

use xl_store::control_store::bucket::{BucketEntry, BucketStore, BucketTxn};
use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow,
    InventoryTxn,
};
use xl_store::control_store::lifecycle::{LifecycleCursorRow, LifecycleRuleEntry, LifecycleTxn};
use xl_store::control_store::tier::{TierRestoreRow, TierTxn};
use xl_store::control_store::{AccessPointEntry, OlapConfigEntry, OwnerRef};
use xl_store::error::Error as StoreError;

use super::super::{AccessPointTxn, BucketDomainCleanup};
use super::parity_util::{err, object, parity, sqlite};

fn bucket(name: &str) -> BucketEntry {
    BucketEntry {
        name: name.into(),
        owner: OwnerRef::Admin,
        creation_date: 1,
        versioning: "Off".into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting: false,
    }
}

fn ap(bucket: &str) -> AccessPointEntry {
    AccessPointEntry {
        name: format!("ap-{bucket}"),
        bucket: bucket.into(),
        owner: OwnerRef::Admin,
        policy_json: None,
        created_at: 1,
        updated_at: 1,
    }
}

fn olap(access_point: &str) -> OlapConfigEntry {
    OlapConfigEntry {
        access_point: access_point.into(),
        kind: "http".into(),
        endpoint: Some("https://x".into()),
        module: None,
        module_sha256: None,
        timeout_ms: 1000,
        max_response_bytes: 1024,
        updated_at: 1,
    }
}

async fn seed<T>(txn: &mut T) -> Result<(), StoreError>
where
    T: BucketTxn + AccessPointTxn + LifecycleTxn + InventoryTxn + TierTxn,
{
    for b in ["b1", "b2"] {
        txn.bucket_create(&bucket(b)).await.map_err(err)?;
        txn.bucket_policy_set(b, "{}", 1).await.map_err(err)?;
        txn.bucket_notification_set(b, "{}", 1).await.map_err(err)?;
        txn.bucket_replication_rules_set(b, "{}", 1)
            .await
            .map_err(err)?;
        txn.access_point_create(&ap(b)).await.map_err(err)?;
        txn.olap_config_upsert(&olap(&format!("ap-{b}")))
            .await
            .map_err(err)?;
        txn.inventory_config_upsert(&InventoryConfigEntry {
            bucket: b.into(),
            id: "c1".into(),
            owner: OwnerRef::Admin,
            config_json: "{}".into(),
            updated_at: 1,
        })
        .await
        .map_err(err)?;
        txn.inventory_run_create(b, "c1", "run1", 1, 1, 1)
            .await
            .map_err(err)?;
        txn.inventory_cursor_upsert(&InventoryCursorRow {
            bucket: b.into(),
            config_id: "c1".into(),
            cursor: "0".into(),
            config_updated_at: 1,
            updated_at: 1,
            fence_term: 1,
            fence_leader: 1,
        })
        .await
        .map_err(err)?;
        txn.inventory_incremental_upsert(&InventoryIncrementalRow {
            bucket: b.into(),
            config_id: "c1".into(),
            enabled: true,
            updated_at: 1,
        })
        .await
        .map_err(err)?;
        txn.tier_restore_upsert(&TierRestoreRow {
            bucket: b.into(),
            key: "k".into(),
            version_id: "v1".into(),
            expires_at: 1,
            job_id: None,
            state: "pending".into(),
            created_at: 1,
            fence_term: 1,
            fence_leader: 1,
        })
        .await
        .map_err(err)?;
        txn.replace_lifecycle_rules(
            b,
            &[LifecycleRuleEntry {
                bucket: b.into(),
                prefix: "".into(),
                rule_id: Some("r1".into()),
                expire_days: 1,
                expiration_days: None,
                transition_days: None,
                transition_class: None,
                transition_tier: None,
                enabled: true,
                updated_at: 1,
                config_json: None,
            }],
        )
        .await
        .map_err(err)?;
        txn.lifecycle_cursor_upsert(&LifecycleCursorRow {
            bucket: b.into(),
            rule_id: "r1".into(),
            stage: "expire".into(),
            cursor: "0".into(),
            updated_at: 1,
            fence_term: 1,
            fence_leader: 1,
        })
        .await
        .map_err(err)?;
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
struct After {
    order: Vec<&'static str>,
    deleted: bool,
    aps: Vec<String>,
    olaps: Vec<String>,
    configs: Vec<String>,
    runs: Vec<InventoryRunRow>,
    cursors: Vec<InventoryCursorRow>,
    incrementals: Vec<InventoryIncrementalRow>,
    tiers: Vec<String>,
    rules: Vec<String>,
    lifecycle_cursors: Vec<String>,
}

async fn cascade<T>(txn: &mut T) -> Result<After, StoreError>
where
    T: BucketTxn + AccessPointTxn + LifecycleTxn + InventoryTxn + TierTxn + BucketDomainCleanup,
{
    let mut order = Vec::new();
    txn.delete_access_points_for_bucket("b1")
        .await
        .map_err(err)?;
    order.push("ap");
    txn.delete_lifecycle_inventory_tier_for_bucket("b1")
        .await
        .map_err(err)?;
    order.push("lit");
    txn.delete_bucket_configs_for_bucket("b1")
        .await
        .map_err(err)?;
    order.push("configs");
    let deleted = txn.bucket_delete("b1").await.map_err(err)?;
    order.push("bucket");
    Ok(After {
        order,
        deleted,
        aps: txn
            .access_points()
            .await
            .map_err(err)?
            .into_iter()
            .map(|r| r.name)
            .collect(),
        olaps: txn
            .olap_configs()
            .await
            .map_err(err)?
            .into_iter()
            .map(|r| r.access_point)
            .collect(),
        configs: txn
            .inventory_configs()
            .await
            .map_err(err)?
            .into_iter()
            .map(|r| r.bucket)
            .collect(),
        runs: txn.inventory_runs().await.map_err(err)?,
        cursors: txn.inventory_cursors().await.map_err(err)?,
        incrementals: txn.inventory_incrementals().await.map_err(err)?,
        tiers: txn
            .tier_restores()
            .await
            .map_err(err)?
            .into_iter()
            .map(|r| r.bucket)
            .collect(),
        rules: txn
            .all_lifecycle_rules()
            .await
            .map_err(err)?
            .into_iter()
            .map(|r| r.bucket)
            .collect(),
        lifecycle_cursors: txn
            .lifecycle_cursors()
            .await
            .map_err(err)?
            .into_iter()
            .map(|r| r.bucket)
            .collect(),
    })
}

#[tokio::test]
async fn bucket_delete_cascade_removes_thirteen_tables_in_order() {
    let p = parity(&[("alice", false)]).await;
    sqlite(&p, |t| Box::pin(seed(t))).await.unwrap();
    object(&p, |t| Box::pin(seed(t))).await.unwrap();
    let sq = sqlite(&p, |t| Box::pin(cascade(t))).await.unwrap();
    let ob = object(&p, |t| Box::pin(cascade(t))).await.unwrap();
    assert_eq!(sq, ob);
    assert_eq!(sq.order, ["ap", "lit", "configs", "bucket"], "buckets last");
    assert!(sq.deleted);
    assert_eq!(sq.aps, ["ap-b2"]);
    assert_eq!(sq.olaps, ["ap-b2"]);
    assert_eq!(sq.configs, ["b2"]);
    assert_eq!(
        sq.runs
            .iter()
            .map(|r| r.bucket.as_str())
            .collect::<Vec<_>>(),
        ["b2"]
    );
    assert_eq!(
        sq.cursors
            .iter()
            .map(|r| r.bucket.as_str())
            .collect::<Vec<_>>(),
        ["b2"]
    );
    assert_eq!(
        sq.incrementals
            .iter()
            .map(|r| r.bucket.as_str())
            .collect::<Vec<_>>(),
        ["b2"]
    );
    assert_eq!(sq.tiers, ["b2"]);
    assert_eq!(sq.rules, ["b2"]);
    assert_eq!(sq.lifecycle_cursors, ["b2"]);

    let sq_store = p.db.control_store();
    let ob_store = p.real.control.as_ref();
    assert_eq!(
        sq_store.buckets().await.unwrap(),
        ob_store.buckets().await.unwrap()
    );
    assert_eq!(
        ob_store
            .buckets()
            .await
            .unwrap()
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        ["b2"]
    );
    assert_eq!(
        sq_store.bucket_policy("b1").await.unwrap(),
        ob_store.bucket_policy("b1").await.unwrap()
    );
    assert_eq!(
        sq_store.bucket_notification("b1").await.unwrap(),
        ob_store.bucket_notification("b1").await.unwrap()
    );
    assert_eq!(
        sq_store.bucket_replication_rules("b1").await.unwrap(),
        ob_store.bucket_replication_rules("b1").await.unwrap()
    );
    assert!(ob_store.bucket_policy("b1").await.unwrap().is_none());
    assert_eq!(
        sq_store.bucket_policies().await.unwrap(),
        ob_store.bucket_policies().await.unwrap()
    );
    assert_eq!(
        sq_store.event_configs().await.unwrap(),
        ob_store.event_configs().await.unwrap()
    );
    assert_eq!(
        sq_store.replication_rules().await.unwrap(),
        ob_store.replication_rules().await.unwrap()
    );
    assert!(ob_store.bucket("b1").await.unwrap().is_none());
    assert!(ob_store.bucket("b2").await.unwrap().is_some());
}
