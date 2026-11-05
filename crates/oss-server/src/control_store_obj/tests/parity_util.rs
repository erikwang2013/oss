//! Shared harness for B2 object-vs-SQLite parity tests.
//!
//! Both backends are seeded with the same users: SQLite creates them with its
//! normal allocator, the object side writes the same uids into the B2 user
//! directory. Scenario closures return `Result<T, String>` so a domain error is
//! compared by message and aborts the transaction on both sides.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use xl_store::control_store::{
    AccessPointEntry, ControlError, ControlStore, ControlTxn, OlapConfigEntry, VpcEndpoint,
};
use xl_store::error::Error as StoreError;

use super::super::bucket::{UserRec, encode_row, user_key};
use super::super::{
    AccessPointStore, AccessPointTxn, BucketDomainCleanup, ObjectControlTxn, TableId, VpcStore,
    VpcTxn,
};
use super::{Real, local, real_store};
use crate::control_store::{SqliteControlStore, SqliteControlTxn};
use crate::db::Db;

pub(super) struct Parity {
    pub(super) db: Arc<Db>,
    pub(super) real: Real,
    pub(super) uids: BTreeMap<String, i64>,
}

impl Parity {
    pub(super) fn uid(&self, name: &str) -> i64 {
        self.uids[name]
    }
}

/// Sentinel so a scenario's domain error survives the object transaction's
/// `xl_store::error::Error` return type without a string prefix.
pub(super) fn err(e: ControlError) -> StoreError {
    StoreError::CorruptMeta(e.to_string())
}

fn message(e: StoreError) -> String {
    match e {
        StoreError::CorruptMeta(message) => message,
        other => other.to_string(),
    }
}

/// Create users in SQLite order, then mirror the same ids into the object dir.
pub(super) async fn parity(users: &[(&str, bool)]) -> Parity {
    let db = Arc::new(Db::connect("sqlite::memory:").await.unwrap());
    let real = real_store().await;
    let mut uids = BTreeMap::new();
    let mut recs = Vec::new();
    for (name, is_admin) in users {
        let uid = db.create_user(name, "pw", *is_admin).await.unwrap();
        uids.insert((*name).to_string(), uid);
        recs.push((uid, (*name).to_string(), *is_admin));
    }
    real.control
        .transaction(local(), move |txn| {
            let recs = recs.clone();
            Box::pin(async move {
                for (uid, username, is_admin) in recs {
                    let row = UserRec { username, is_admin };
                    txn.put(TableId::Users, &user_key(uid), &encode_row(&row).unwrap())
                        .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    Parity { db, real, uids }
}

/// Run one scenario in a SQLite transaction and commit only on success.
pub(super) async fn sqlite<T, F>(p: &Parity, mut f: F) -> Result<T, String>
where
    T: Send,
    F: Send
        + for<'a, 'b> FnMut(&'a mut SqliteControlTxn<'b>) -> BoxFuture<'a, Result<T, StoreError>>,
{
    let mut tx = p.db.pool().begin().await.unwrap();
    let out = {
        let mut txn = SqliteControlTxn::new(&mut tx);
        f(&mut txn).await
    };
    match out {
        Ok(value) => {
            tx.commit().await.unwrap();
            Ok(value)
        }
        Err(e) => {
            tx.rollback().await.unwrap();
            Err(message(e))
        }
    }
}

/// Run one scenario through `ObjectControlStore::transaction` (retry-on-conflict).
pub(super) async fn object<T, F>(p: &Parity, f: F) -> Result<T, String>
where
    T: Send,
    F: Send + for<'a> FnMut(&'a mut ObjectControlTxn) -> BoxFuture<'a, Result<T, StoreError>>,
{
    p.real
        .control
        .transaction(local(), f)
        .await
        .map_err(message)
}

// ---------- SQLite adapters for the B2-only AP/VPC traits ----------
// The seam exposes these on `ControlStore`/`ControlTxn`; B2 dispatched them as
// domain traits, so the parity harness delegates the SQLite side onto the seam.

#[async_trait::async_trait]
impl AccessPointStore for SqliteControlStore {
    async fn access_point(&self, name: &str) -> Result<Option<AccessPointEntry>, ControlError> {
        ControlStore::access_point(self, name).await
    }
    async fn access_points(&self) -> Result<Vec<AccessPointEntry>, ControlError> {
        ControlStore::access_points(self).await
    }
    async fn olap_config(
        &self,
        access_point: &str,
    ) -> Result<Option<OlapConfigEntry>, ControlError> {
        ControlStore::olap_config(self, access_point).await
    }
    async fn olap_configs(&self) -> Result<Vec<OlapConfigEntry>, ControlError> {
        ControlStore::olap_configs(self).await
    }
}

#[async_trait::async_trait]
impl AccessPointTxn for SqliteControlTxn<'_> {
    async fn access_point_create(&mut self, row: &AccessPointEntry) -> Result<bool, ControlError> {
        ControlTxn::access_point_create(self, row).await
    }
    async fn access_point_set_policy(
        &mut self,
        name: &str,
        policy_json: Option<&str>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        ControlTxn::access_point_set_policy(self, name, policy_json, updated_at).await
    }
    async fn access_point_delete(&mut self, name: &str) -> Result<bool, ControlError> {
        ControlTxn::access_point_delete(self, name).await
    }
    async fn access_point_exists(&mut self, name: &str) -> Result<bool, ControlError> {
        ControlTxn::access_point_exists(self, name).await
    }
    async fn access_points(&mut self) -> Result<Vec<AccessPointEntry>, ControlError> {
        ControlTxn::access_points(self).await
    }
    async fn replace_access_points(
        &mut self,
        rows: &[AccessPointEntry],
    ) -> Result<(), ControlError> {
        ControlTxn::replace_access_points(self, rows).await
    }
    async fn olap_config_upsert(&mut self, row: &OlapConfigEntry) -> Result<(), ControlError> {
        ControlTxn::olap_config_upsert(self, row).await
    }
    async fn olap_config_delete(&mut self, access_point: &str) -> Result<bool, ControlError> {
        ControlTxn::olap_config_delete(self, access_point).await
    }
    async fn olap_configs(&mut self) -> Result<Vec<OlapConfigEntry>, ControlError> {
        ControlTxn::olap_configs(self).await
    }
    async fn replace_olap_configs(&mut self, rows: &[OlapConfigEntry]) -> Result<(), ControlError> {
        ControlTxn::replace_olap_configs(self, rows).await
    }
    async fn delete_access_points_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError> {
        ControlTxn::delete_access_points_for_bucket(self, bucket).await
    }
}

#[async_trait::async_trait]
impl BucketDomainCleanup for SqliteControlTxn<'_> {
    async fn delete_lifecycle_inventory_tier_for_bucket(
        &mut self,
        bucket: &str,
    ) -> Result<(), ControlError> {
        ControlTxn::delete_lifecycle_inventory_tier_for_bucket(self, bucket).await
    }
}

#[async_trait::async_trait]
impl VpcStore for SqliteControlStore {
    async fn vpc_endpoint(&self, vpce_id: &str) -> Result<Option<VpcEndpoint>, ControlError> {
        ControlStore::vpc_endpoint(self, vpce_id).await
    }
    async fn vpc_endpoints(&self) -> Result<Vec<VpcEndpoint>, ControlError> {
        ControlStore::vpc_endpoints(self).await
    }
}

#[async_trait::async_trait]
impl VpcTxn for SqliteControlTxn<'_> {
    async fn vpc_endpoint_upsert(&mut self, row: &VpcEndpoint) -> Result<(), ControlError> {
        ControlTxn::vpc_endpoint_upsert(self, row).await
    }
    async fn vpc_endpoint_delete(&mut self, vpce_id: &str) -> Result<bool, ControlError> {
        ControlTxn::vpc_endpoint_delete(self, vpce_id).await
    }
    async fn vpc_endpoints(&mut self) -> Result<Vec<VpcEndpoint>, ControlError> {
        ControlTxn::vpc_endpoints(self).await
    }
    async fn replace_vpc_endpoints(&mut self, rows: &[VpcEndpoint]) -> Result<(), ControlError> {
        ControlTxn::replace_vpc_endpoints(self, rows).await
    }
}
