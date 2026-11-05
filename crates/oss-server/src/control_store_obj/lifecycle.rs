//! Object-backed lifecycle rules and scan cursors.
//!
//! Rule key = `(bucket, nullable rule_id, prefix)`, which reproduces the seam's
//! `ORDER BY bucket, rule_id, prefix` with NULL rule ids first. Cursors are keyed
//! `(bucket, rule_id, stage)` and every upsert/delete is fence-gated.

use xl_store::control_store::lifecycle::{
    LifecycleCursorRow, LifecycleRuleEntry, LifecycleStore, LifecycleTxn,
};

use super::bucket::{
    CResult, backend, clear_table, decode_row, delete_prefix, encode_row, key_opt2, key1, key3,
    prefix_end, store_scan_all, txn_scan_all,
};
use super::{ObjectControlStore, ObjectControlTxn, TableId};

async fn get_cursor(
    txn: &ObjectControlTxn,
    bucket: &str,
    rule_id: &str,
    stage: &str,
) -> CResult<Option<LifecycleCursorRow>> {
    let row = txn
        .get(TableId::LifecycleCursors, &key3(bucket, rule_id, stage))
        .await
        .map_err(backend)?;
    row.as_deref().map(decode_row).transpose()
}

async fn put_cursor(txn: &mut ObjectControlTxn, row: &LifecycleCursorRow) -> CResult<()> {
    let bytes = encode_row(row)?;
    txn.put(
        TableId::LifecycleCursors,
        &key3(&row.bucket, &row.rule_id, &row.stage),
        &bytes,
    )
    .await
    .map_err(backend)
}

async fn put_rule(txn: &mut ObjectControlTxn, row: &LifecycleRuleEntry) -> CResult<()> {
    let bytes = encode_row(row)?;
    txn.put(
        TableId::LifecycleRules,
        &key_opt2(&row.bucket, row.rule_id.as_deref(), &row.prefix),
        &bytes,
    )
    .await
    .map_err(backend)
}

#[async_trait::async_trait]
impl LifecycleStore for ObjectControlStore {
    async fn lifecycle_rules(&self, bucket: &str) -> CResult<Vec<LifecycleRuleEntry>> {
        let start = key1(bucket);
        let end = prefix_end(&start);
        self.read_range(TableId::LifecycleRules, &start[..]..&end[..])
            .await
            .map_err(backend)?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn enabled_lifecycle_rules(&self) -> CResult<Vec<LifecycleRuleEntry>> {
        let rows = store_scan_all(self, TableId::LifecycleRules)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row::<LifecycleRuleEntry>(&value))
            .collect::<CResult<Vec<_>>>()?;
        Ok(rows.into_iter().filter(|row| row.enabled).collect())
    }

    async fn lifecycle_cursor(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> CResult<Option<String>> {
        let bytes = self
            .read_row(TableId::LifecycleCursors, &key3(bucket, rule_id, stage))
            .await
            .map_err(backend)?;
        bytes
            .as_deref()
            .map(decode_row::<LifecycleCursorRow>)
            .transpose()
            .map(|row| row.map(|row| row.cursor))
    }

    async fn lifecycle_cursor_row(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> CResult<Option<LifecycleCursorRow>> {
        let bytes = self
            .read_row(TableId::LifecycleCursors, &key3(bucket, rule_id, stage))
            .await
            .map_err(backend)?;
        bytes.as_deref().map(decode_row).transpose()
    }
}

#[async_trait::async_trait]
impl LifecycleTxn for ObjectControlTxn {
    async fn replace_lifecycle_rules(
        &mut self,
        bucket: &str,
        rows: &[LifecycleRuleEntry],
    ) -> CResult<()> {
        delete_prefix(self, TableId::LifecycleRules, bucket).await?;
        delete_prefix(self, TableId::LifecycleCursors, bucket).await?;
        for row in rows {
            // The seam stores the caller-supplied bucket, not `row.bucket`.
            let mut stored = row.clone();
            stored.bucket = bucket.into();
            put_rule(self, &stored).await?;
        }
        Ok(())
    }

    async fn replace_all_lifecycle_rules(&mut self, rows: &[LifecycleRuleEntry]) -> CResult<()> {
        clear_table(self, TableId::LifecycleRules).await?;
        for row in rows {
            put_rule(self, row).await?;
        }
        Ok(())
    }

    async fn clear_lifecycle_rules(&mut self, bucket: &str) -> CResult<()> {
        delete_prefix(self, TableId::LifecycleRules, bucket).await?;
        delete_prefix(self, TableId::LifecycleCursors, bucket).await
    }

    async fn delete_lifecycle_rules_only(&mut self, bucket: &str) -> CResult<()> {
        delete_prefix(self, TableId::LifecycleRules, bucket).await
    }

    async fn all_lifecycle_rules(&mut self) -> CResult<Vec<LifecycleRuleEntry>> {
        txn_scan_all(self, TableId::LifecycleRules)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn lifecycle_cursor(
        &mut self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> CResult<Option<LifecycleCursorRow>> {
        get_cursor(self, bucket, rule_id, stage).await
    }

    async fn lifecycle_cursor_upsert(&mut self, row: &LifecycleCursorRow) -> CResult<bool> {
        if let Some(existing) = get_cursor(self, &row.bucket, &row.rule_id, &row.stage).await?
            && existing.fence_term > row.fence_term
        {
            return Ok(false);
        }
        put_cursor(self, row).await?;
        Ok(true)
    }

    async fn lifecycle_cursor_delete(
        &mut self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
        fence_term: i64,
    ) -> CResult<()> {
        if let Some(existing) = get_cursor(self, bucket, rule_id, stage).await?
            && existing.fence_term <= fence_term
        {
            self.delete(TableId::LifecycleCursors, &key3(bucket, rule_id, stage))
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn lifecycle_cursors(&mut self) -> CResult<Vec<LifecycleCursorRow>> {
        txn_scan_all(self, TableId::LifecycleCursors)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_lifecycle_cursors(&mut self, rows: &[LifecycleCursorRow]) -> CResult<()> {
        clear_table(self, TableId::LifecycleCursors).await?;
        for row in rows {
            put_cursor(self, row).await?;
        }
        Ok(())
    }
}
