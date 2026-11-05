//! Object-backed inventory configs, runs, cursors and incremental switches.
//!
//! Runs are keyed by their natural key `(bucket, config_id, run_key)`; the local
//! `id` is a row attribute (snapshot/executor API), so point-by-id reads scan and
//! the allocator is advanced past explicit snapshot ids.

use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryConfigView, InventoryCursorRow, InventoryIncrementalRow,
    InventoryRunRow, InventoryStore,
};
use xl_store::control_store::{ControlError, OwnerRef};

use super::bucket::{
    CResult, UserLookup, backend, decode_row, key1, key2, key3, prefix_end, store_scan_all,
};
use super::{ObjectControlStore, TableId};

#[async_trait::async_trait]
pub trait BucketDomainCleanup: Send {
    /// Delete the seven per-bucket tables in the SQLite cascade order.
    async fn delete_lifecycle_inventory_tier_for_bucket(&mut self, bucket: &str) -> CResult<()>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct ConfigRow {
    pub(super) bucket: String,
    pub(super) id: String,
    pub(super) owner_id: i64,
    pub(super) config_json: String,
    pub(super) updated_at: i64,
}

async fn runtime_view<L: UserLookup + ?Sized>(
    dir: &L,
    row: ConfigRow,
) -> CResult<InventoryConfigView> {
    let owner = if row.owner_id == 0 {
        Some(OwnerRef::Admin)
    } else {
        match dir.user_by_uid(row.owner_id).await? {
            Some(user) => Some(OwnerRef::User(user.username)),
            None => None,
        }
    };
    Ok(InventoryConfigView {
        bucket: row.bucket,
        id: row.id,
        owner,
        owner_uid: row.owner_id,
        config_json: row.config_json,
        updated_at: row.updated_at,
    })
}

pub(super) async fn strict_entry<L: UserLookup + ?Sized>(
    dir: &L,
    row: ConfigRow,
) -> CResult<InventoryConfigEntry> {
    let owner = if row.owner_id == 0 {
        OwnerRef::Admin
    } else {
        match dir.user_by_uid(row.owner_id).await? {
            Some(user) if user.is_admin => OwnerRef::Admin,
            Some(user) => OwnerRef::User(user.username),
            None => {
                return Err(ControlError::Invalid(format!(
                    "owner user id {} missing",
                    row.owner_id
                )));
            }
        }
    };
    Ok(InventoryConfigEntry {
        bucket: row.bucket,
        id: row.id,
        owner,
        config_json: row.config_json,
        updated_at: row.updated_at,
    })
}

#[async_trait::async_trait]
impl InventoryStore for ObjectControlStore {
    async fn inventory_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> CResult<Option<InventoryConfigView>> {
        let Some(bytes) = self
            .read_row(TableId::InventoryConfigs, &key2(bucket, id))
            .await
            .map_err(backend)?
        else {
            return Ok(None);
        };
        runtime_view(self, decode_row(&bytes)?).await.map(Some)
    }

    async fn inventory_configs(&self, bucket: &str) -> CResult<Vec<InventoryConfigView>> {
        let start = key1(bucket);
        let end = prefix_end(&start);
        let mut out = Vec::new();
        for (_, value) in self
            .read_range(TableId::InventoryConfigs, &start[..]..&end[..])
            .await
            .map_err(backend)?
        {
            out.push(runtime_view(self, decode_row(&value)?).await?);
        }
        Ok(out)
    }

    async fn inventory_configs_all(&self) -> CResult<Vec<InventoryConfigView>> {
        let mut out = Vec::new();
        for (_, value) in store_scan_all(self, TableId::InventoryConfigs).await? {
            out.push(runtime_view(self, decode_row(&value)?).await?);
        }
        Ok(out)
    }

    async fn inventory_config_count(&self, bucket: &str) -> CResult<i64> {
        Ok(self.inventory_configs(bucket).await?.len() as i64)
    }

    async fn inventory_run(&self, id: i64) -> CResult<Option<InventoryRunRow>> {
        for (_, value) in store_scan_all(self, TableId::InventoryRuns).await? {
            let row: InventoryRunRow = decode_row(&value)?;
            if row.id == id {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    async fn inventory_run_by_key(
        &self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> CResult<Option<InventoryRunRow>> {
        let bytes = self
            .read_row(TableId::InventoryRuns, &key3(bucket, config_id, run_key))
            .await
            .map_err(backend)?;
        bytes.as_deref().map(decode_row).transpose()
    }

    async fn inventory_cursor(
        &self,
        bucket: &str,
        config_id: &str,
    ) -> CResult<Option<InventoryCursorRow>> {
        let bytes = self
            .read_row(TableId::InventoryCursors, &key2(bucket, config_id))
            .await
            .map_err(backend)?;
        bytes.as_deref().map(decode_row).transpose()
    }

    async fn inventory_incremental(
        &self,
        bucket: &str,
        config_id: &str,
    ) -> CResult<Option<InventoryIncrementalRow>> {
        let bytes = self
            .read_row(TableId::InventoryIncremental, &key2(bucket, config_id))
            .await
            .map_err(backend)?;
        bytes.as_deref().map(decode_row).transpose()
    }
}
