//! Object-backed tier-restore rows (`m8.runstate` extension semantics).
//!
//! Key = `(bucket, key, version_id)` (snapshot export order). Upsert, delete and
//! state transitions are fence-gated; a fenced-off transition is a zero write.

use xl_store::control_store::tier::{TierRestoreRow, TierStore, TierTxn};

use super::bucket::{
    CResult, backend, clear_table, decode_row, encode_row, key3, store_scan_all, txn_scan_all,
};
use super::{ObjectControlStore, ObjectControlTxn, TableId};

async fn get_row(
    txn: &ObjectControlTxn,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> CResult<Option<TierRestoreRow>> {
    let row = txn
        .get(TableId::TierRestores, &key3(bucket, key, version_id))
        .await
        .map_err(backend)?;
    row.as_deref().map(decode_row).transpose()
}

async fn put_row(txn: &mut ObjectControlTxn, row: &TierRestoreRow) -> CResult<()> {
    let bytes = encode_row(row)?;
    txn.put(
        TableId::TierRestores,
        &key3(&row.bucket, &row.key, &row.version_id),
        &bytes,
    )
    .await
    .map_err(backend)
}

#[async_trait::async_trait]
impl TierStore for ObjectControlStore {
    async fn tier_restore(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> CResult<Option<TierRestoreRow>> {
        let bytes = self
            .read_row(TableId::TierRestores, &key3(bucket, key, version_id))
            .await
            .map_err(backend)?;
        bytes.as_deref().map(decode_row).transpose()
    }

    async fn tier_restores(&self, state: &str) -> CResult<Vec<TierRestoreRow>> {
        let mut rows: Vec<TierRestoreRow> = store_scan_all(self, TableId::TierRestores)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect::<CResult<_>>()?;
        rows.retain(|row| row.state == state);
        rows.sort_by_key(|row| row.created_at);
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl TierTxn for ObjectControlTxn {
    async fn tier_restore(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> CResult<Option<TierRestoreRow>> {
        get_row(self, bucket, key, version_id).await
    }

    async fn tier_restores(&mut self) -> CResult<Vec<TierRestoreRow>> {
        txn_scan_all(self, TableId::TierRestores)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_tier_restores(&mut self, rows: &[TierRestoreRow]) -> CResult<()> {
        clear_table(self, TableId::TierRestores).await?;
        for row in rows {
            put_row(self, row).await?;
        }
        Ok(())
    }

    async fn tier_restore_upsert(&mut self, row: &TierRestoreRow) -> CResult<bool> {
        if let Some(existing) = get_row(self, &row.bucket, &row.key, &row.version_id).await?
            && existing.fence_term > row.fence_term
        {
            return Ok(false);
        }
        put_row(self, row).await?;
        Ok(true)
    }

    async fn tier_restore_delete(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
        fence_term: i64,
    ) -> CResult<()> {
        if let Some(existing) = get_row(self, bucket, key, version_id).await?
            && existing.fence_term <= fence_term
        {
            self.delete(TableId::TierRestores, &key3(bucket, key, version_id))
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn tier_restore_set_state(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
        state: &str,
        expires_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> CResult<()> {
        let Some(mut row) = get_row(self, bucket, key, version_id).await? else {
            return Ok(());
        };
        if row.fence_term > fence_term {
            return Ok(());
        }
        row.state = state.into();
        row.expires_at = expires_at;
        row.fence_term = fence_term;
        row.fence_leader = fence_leader;
        put_row(self, &row).await
    }
}
