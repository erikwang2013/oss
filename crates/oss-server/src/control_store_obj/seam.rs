//! B4 dispatch seam: expose the D3 object adapters through the backend-neutral
//! `ControlStore`/`ControlTxn` traits (the same traits the SQLite adapters use).
//!
//! This file only delegates; domain semantics stay in the D3 modules. The
//! transaction impl is the `ControlTxn` view of one `ObjectControlTxn`; the
//! caller owns the commit/retry boundary (`ObjectControlStore::transaction`).

use xl_store::control_store::{
    AccessPointEntry, ControlError, ControlStore, ControlTxn, OlapConfigEntry, VpcEndpoint,
};

use super::{
    AccessPointStore, AccessPointTxn, BucketDomainCleanup, ObjectControlStore, ObjectControlTxn,
    VpcStore, VpcTxn,
};

#[async_trait::async_trait]
impl ControlStore for ObjectControlStore {
    async fn vpc_endpoint(&self, vpce_id: &str) -> Result<Option<VpcEndpoint>, ControlError> {
        VpcStore::vpc_endpoint(self, vpce_id).await
    }

    async fn vpc_endpoints(&self) -> Result<Vec<VpcEndpoint>, ControlError> {
        VpcStore::vpc_endpoints(self).await
    }

    async fn access_point(&self, name: &str) -> Result<Option<AccessPointEntry>, ControlError> {
        AccessPointStore::access_point(self, name).await
    }

    async fn access_points(&self) -> Result<Vec<AccessPointEntry>, ControlError> {
        AccessPointStore::access_points(self).await
    }

    async fn olap_config(
        &self,
        access_point: &str,
    ) -> Result<Option<OlapConfigEntry>, ControlError> {
        AccessPointStore::olap_config(self, access_point).await
    }

    async fn olap_configs(&self) -> Result<Vec<OlapConfigEntry>, ControlError> {
        AccessPointStore::olap_configs(self).await
    }
}

#[async_trait::async_trait]
impl ControlTxn for ObjectControlTxn {
    async fn vpc_endpoint_upsert(&mut self, row: &VpcEndpoint) -> Result<(), ControlError> {
        VpcTxn::vpc_endpoint_upsert(self, row).await
    }

    async fn vpc_endpoint_delete(&mut self, vpce_id: &str) -> Result<bool, ControlError> {
        VpcTxn::vpc_endpoint_delete(self, vpce_id).await
    }

    async fn vpc_endpoints(&mut self) -> Result<Vec<VpcEndpoint>, ControlError> {
        VpcTxn::vpc_endpoints(self).await
    }

    async fn replace_vpc_endpoints(&mut self, rows: &[VpcEndpoint]) -> Result<(), ControlError> {
        VpcTxn::replace_vpc_endpoints(self, rows).await
    }

    async fn access_point_create(&mut self, row: &AccessPointEntry) -> Result<bool, ControlError> {
        AccessPointTxn::access_point_create(self, row).await
    }

    async fn access_point_set_policy(
        &mut self,
        name: &str,
        policy_json: Option<&str>,
        updated_at: i64,
    ) -> Result<bool, ControlError> {
        AccessPointTxn::access_point_set_policy(self, name, policy_json, updated_at).await
    }

    async fn access_point_delete(&mut self, name: &str) -> Result<bool, ControlError> {
        AccessPointTxn::access_point_delete(self, name).await
    }

    async fn access_point_exists(&mut self, name: &str) -> Result<bool, ControlError> {
        AccessPointTxn::access_point_exists(self, name).await
    }

    async fn access_points(&mut self) -> Result<Vec<AccessPointEntry>, ControlError> {
        AccessPointTxn::access_points(self).await
    }

    async fn replace_access_points(
        &mut self,
        rows: &[AccessPointEntry],
    ) -> Result<(), ControlError> {
        AccessPointTxn::replace_access_points(self, rows).await
    }

    async fn olap_config_upsert(&mut self, row: &OlapConfigEntry) -> Result<(), ControlError> {
        AccessPointTxn::olap_config_upsert(self, row).await
    }

    async fn olap_config_delete(&mut self, access_point: &str) -> Result<bool, ControlError> {
        AccessPointTxn::olap_config_delete(self, access_point).await
    }

    async fn olap_configs(&mut self) -> Result<Vec<OlapConfigEntry>, ControlError> {
        AccessPointTxn::olap_configs(self).await
    }

    async fn replace_olap_configs(&mut self, rows: &[OlapConfigEntry]) -> Result<(), ControlError> {
        AccessPointTxn::replace_olap_configs(self, rows).await
    }

    async fn delete_access_points_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError> {
        AccessPointTxn::delete_access_points_for_bucket(self, bucket).await
    }

    async fn delete_lifecycle_inventory_tier_for_bucket(
        &mut self,
        bucket: &str,
    ) -> Result<(), ControlError> {
        BucketDomainCleanup::delete_lifecycle_inventory_tier_for_bucket(self, bucket).await
    }

    async fn schema_meta_set(&mut self, key: &str, value: &str) -> Result<(), ControlError> {
        self.set_marker(key, value);
        Ok(())
    }

    async fn schema_meta_set_if_absent(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<bool, ControlError> {
        if self.meta().markers.contains_key(key) {
            return Ok(false);
        }
        self.set_marker(key, value);
        Ok(true)
    }
}
