//! Object-backed dynamic VPC endpoint adapters.

use xl_store::control_store::VpcEndpoint;

use super::bucket::{
    CResult, backend, clear_table, decode_row, encode_row, key1, store_scan_all, txn_scan_all,
};
use super::{ObjectControlStore, ObjectControlTxn, TableId};

#[async_trait::async_trait]
pub trait VpcStore: Send + Sync {
    async fn vpc_endpoint(&self, vpce_id: &str) -> CResult<Option<VpcEndpoint>>;
    /// `vpce_id` ascending.
    async fn vpc_endpoints(&self) -> CResult<Vec<VpcEndpoint>>;
}

#[async_trait::async_trait]
pub trait VpcTxn: Send {
    async fn vpc_endpoint_upsert(&mut self, row: &VpcEndpoint) -> CResult<()>;
    async fn vpc_endpoint_delete(&mut self, vpce_id: &str) -> CResult<bool>;
    /// `vpce_id` ascending.
    async fn vpc_endpoints(&mut self) -> CResult<Vec<VpcEndpoint>>;
    /// Full replace (clear, then insert in input order).
    async fn replace_vpc_endpoints(&mut self, rows: &[VpcEndpoint]) -> CResult<()>;
}

#[async_trait::async_trait]
impl VpcStore for ObjectControlStore {
    async fn vpc_endpoint(&self, vpce_id: &str) -> CResult<Option<VpcEndpoint>> {
        let Some(bytes) = self
            .read_row(TableId::VpcEndpoints, &key1(vpce_id))
            .await
            .map_err(backend)?
        else {
            return Ok(None);
        };
        decode_row(&bytes).map(Some)
    }

    async fn vpc_endpoints(&self) -> CResult<Vec<VpcEndpoint>> {
        store_scan_all(self, TableId::VpcEndpoints)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }
}

#[async_trait::async_trait]
impl VpcTxn for ObjectControlTxn {
    async fn vpc_endpoint_upsert(&mut self, row: &VpcEndpoint) -> CResult<()> {
        let mut stored = row.clone();
        if let Some(bytes) = self
            .get(TableId::VpcEndpoints, &key1(&row.vpce_id))
            .await
            .map_err(backend)?
        {
            stored.created_at = decode_row::<VpcEndpoint>(&bytes)?.created_at;
        }
        let bytes = encode_row(&stored)?;
        self.put(TableId::VpcEndpoints, &key1(&row.vpce_id), &bytes)
            .await
            .map_err(backend)
    }

    async fn vpc_endpoint_delete(&mut self, vpce_id: &str) -> CResult<bool> {
        self.delete(TableId::VpcEndpoints, &key1(vpce_id))
            .await
            .map_err(backend)
    }

    async fn vpc_endpoints(&mut self) -> CResult<Vec<VpcEndpoint>> {
        txn_scan_all(self, TableId::VpcEndpoints)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_vpc_endpoints(&mut self, rows: &[VpcEndpoint]) -> CResult<()> {
        clear_table(self, TableId::VpcEndpoints).await?;
        for row in rows {
            let bytes = encode_row(row)?;
            self.put(TableId::VpcEndpoints, &key1(&row.vpce_id), &bytes)
                .await
                .map_err(backend)?;
        }
        Ok(())
    }
}
