//! Object-backed inventory writes (`InventoryTxn`) and the per-bucket cascade.

use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow,
    InventoryTxn,
};
use xl_store::control_store::{ControlError, OwnerRef};

use super::bucket::{
    CResult, admin_uid, backend, clear_table, decode_row, delete_prefix, encode_row, key2, key3,
    txn_scan_all, user_by_name,
};
use super::inventory::{BucketDomainCleanup, ConfigRow, strict_entry};
use super::{ObjectControlTxn, TableId};

/// Same owner mapping as AP writes.
async fn resolve_owner(txn: &ObjectControlTxn, owner: &OwnerRef) -> CResult<i64> {
    let admin = admin_uid(txn).await?;
    match owner {
        OwnerRef::Admin => Ok(admin.unwrap_or(0)),
        OwnerRef::User(username) => match user_by_name(txn, username).await? {
            Some((uid, _)) => Ok(uid),
            None if admin.is_some() => Err(ControlError::Invalid(format!(
                "owner user {username} not found"
            ))),
            None => Ok(0),
        },
    }
}

fn run_key(row: &InventoryRunRow) -> Vec<u8> {
    key3(&row.bucket, &row.config_id, &row.run_key)
}

async fn find_run(txn: &ObjectControlTxn, id: i64) -> CResult<Option<InventoryRunRow>> {
    let mut found = None;
    for (_, value) in txn_scan_all(txn, TableId::InventoryRuns).await? {
        let row: InventoryRunRow = decode_row(&value)?;
        if row.id == id {
            found = Some(row);
            break;
        }
    }
    Ok(found)
}

async fn put_run(txn: &mut ObjectControlTxn, row: &InventoryRunRow) -> CResult<()> {
    let bytes = encode_row(row)?;
    txn.put(TableId::InventoryRuns, &run_key(row), &bytes)
        .await
        .map_err(backend)
}

async fn mutate_run(
    txn: &mut ObjectControlTxn,
    id: i64,
    fence_term: i64,
    fence_leader: i64,
    f: impl FnOnce(&mut InventoryRunRow),
) -> CResult<()> {
    let Some(mut row) = find_run(txn, id).await? else {
        return Ok(());
    };
    if row.fence_term > fence_term {
        return Ok(());
    }
    f(&mut row);
    row.fence_term = fence_term;
    row.fence_leader = fence_leader;
    put_run(txn, &row).await
}

async fn get_cursor(
    txn: &ObjectControlTxn,
    bucket: &str,
    config_id: &str,
) -> CResult<Option<InventoryCursorRow>> {
    let row = txn
        .get(TableId::InventoryCursors, &key2(bucket, config_id))
        .await
        .map_err(backend)?;
    row.as_deref().map(decode_row).transpose()
}

async fn get_incremental(
    txn: &ObjectControlTxn,
    bucket: &str,
    config_id: &str,
) -> CResult<Option<InventoryIncrementalRow>> {
    let row = txn
        .get(TableId::InventoryIncremental, &key2(bucket, config_id))
        .await
        .map_err(backend)?;
    row.as_deref().map(decode_row).transpose()
}

#[async_trait::async_trait]
impl InventoryTxn for ObjectControlTxn {
    async fn inventory_config_upsert(&mut self, row: &InventoryConfigEntry) -> CResult<()> {
        let owner_id = resolve_owner(self, &row.owner).await?;
        let key = key2(&row.bucket, &row.id);
        self.delete(TableId::InventoryCursors, &key)
            .await
            .map_err(backend)?;
        self.delete(TableId::InventoryIncremental, &key)
            .await
            .map_err(backend)?;
        let stored = ConfigRow {
            bucket: row.bucket.clone(),
            id: row.id.clone(),
            owner_id,
            config_json: row.config_json.clone(),
            updated_at: row.updated_at,
        };
        let bytes = encode_row(&stored)?;
        self.put(TableId::InventoryConfigs, &key, &bytes)
            .await
            .map_err(backend)
    }

    async fn inventory_config_delete(&mut self, bucket: &str, id: &str) -> CResult<bool> {
        let key = key2(bucket, id);
        self.delete(TableId::InventoryCursors, &key)
            .await
            .map_err(backend)?;
        self.delete(TableId::InventoryIncremental, &key)
            .await
            .map_err(backend)?;
        self.delete(TableId::InventoryConfigs, &key)
            .await
            .map_err(backend)
    }

    async fn inventory_configs(&mut self) -> CResult<Vec<InventoryConfigEntry>> {
        let mut out = Vec::new();
        for (_, value) in txn_scan_all(self, TableId::InventoryConfigs).await? {
            out.push(strict_entry(self, decode_row(&value)?).await?);
        }
        Ok(out)
    }

    async fn replace_inventory_configs(&mut self, rows: &[InventoryConfigEntry]) -> CResult<()> {
        clear_table(self, TableId::InventoryConfigs).await?;
        for row in rows {
            let owner_id = resolve_owner(self, &row.owner).await?;
            let stored = ConfigRow {
                bucket: row.bucket.clone(),
                id: row.id.clone(),
                owner_id,
                config_json: row.config_json.clone(),
                updated_at: row.updated_at,
            };
            let bytes = encode_row(&stored)?;
            self.put(
                TableId::InventoryConfigs,
                &key2(&row.bucket, &row.id),
                &bytes,
            )
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn inventory_runs(&mut self) -> CResult<Vec<InventoryRunRow>> {
        let mut rows: Vec<InventoryRunRow> = txn_scan_all(self, TableId::InventoryRuns)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect::<CResult<_>>()?;
        rows.sort_by_key(|row| row.id);
        Ok(rows)
    }

    async fn replace_inventory_runs(&mut self, rows: &[InventoryRunRow]) -> CResult<()> {
        clear_table(self, TableId::InventoryRuns).await?;
        for row in rows {
            put_run(self, row).await?;
            if row.id > 0 {
                self.meta_mut()
                    .observe_run_id(row.id as u64)
                    .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn inventory_cursors(&mut self) -> CResult<Vec<InventoryCursorRow>> {
        txn_scan_all(self, TableId::InventoryCursors)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_inventory_cursors(&mut self, rows: &[InventoryCursorRow]) -> CResult<()> {
        clear_table(self, TableId::InventoryCursors).await?;
        for row in rows {
            let bytes = encode_row(row)?;
            self.put(
                TableId::InventoryCursors,
                &key2(&row.bucket, &row.config_id),
                &bytes,
            )
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn inventory_incrementals(&mut self) -> CResult<Vec<InventoryIncrementalRow>> {
        txn_scan_all(self, TableId::InventoryIncremental)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_inventory_incrementals(
        &mut self,
        rows: &[InventoryIncrementalRow],
    ) -> CResult<()> {
        clear_table(self, TableId::InventoryIncremental).await?;
        for row in rows {
            let bytes = encode_row(row)?;
            self.put(
                TableId::InventoryIncremental,
                &key2(&row.bucket, &row.config_id),
                &bytes,
            )
            .await
            .map_err(backend)?;
        }
        Ok(())
    }

    async fn inventory_run(&mut self, id: i64) -> CResult<Option<InventoryRunRow>> {
        find_run(self, id).await
    }

    async fn inventory_run_by_key(
        &mut self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> CResult<Option<InventoryRunRow>> {
        let bytes = self
            .get(TableId::InventoryRuns, &key3(bucket, config_id, run_key))
            .await
            .map_err(backend)?;
        bytes.as_deref().map(decode_row).transpose()
    }

    async fn inventory_run_upsert(&mut self, row: &InventoryRunRow) -> CResult<bool> {
        put_run(self, row).await?;
        Ok(true)
    }

    async fn inventory_run_create(
        &mut self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
        created_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> CResult<Option<i64>> {
        let key = key3(bucket, config_id, run_key);
        if self
            .get(TableId::InventoryRuns, &key)
            .await
            .map_err(backend)?
            .is_some()
        {
            // `INSERT OR IGNORE` still consumes an AUTOINCREMENT id; burn one so
            // subsequent ids match the SQLite adapter.
            self.alloc_run_id().map_err(backend)?;
            return Ok(None);
        }
        let id = i64::try_from(self.alloc_run_id().map_err(backend)?)
            .map_err(|_| backend("run id overflow"))?;
        let row = InventoryRunRow {
            id,
            bucket: bucket.into(),
            config_id: config_id.into(),
            run_key: run_key.into(),
            started_at: 0,
            state: "running".into(),
            job_id: None,
            manifest_key: None,
            error: None,
            created_at,
            fence_term,
            fence_leader,
        };
        put_run(self, &row).await?;
        Ok(Some(id))
    }

    async fn inventory_run_set_job(
        &mut self,
        run_id: i64,
        job_id: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> CResult<()> {
        mutate_run(self, run_id, fence_term, fence_leader, |row| {
            row.job_id = Some(job_id)
        })
        .await
    }

    async fn inventory_run_started(
        &mut self,
        run_id: i64,
        started_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> CResult<()> {
        mutate_run(self, run_id, fence_term, fence_leader, |row| {
            if row.started_at == 0 {
                row.started_at = started_at;
            }
        })
        .await
    }

    async fn inventory_run_finish(
        &mut self,
        run_id: i64,
        state: &str,
        manifest_key: Option<&str>,
        error: Option<&str>,
        fence_term: i64,
        fence_leader: i64,
    ) -> CResult<()> {
        mutate_run(self, run_id, fence_term, fence_leader, |row| {
            row.state = state.into();
            row.manifest_key = manifest_key.map(Into::into);
            row.error = error.map(Into::into);
        })
        .await
    }

    async fn inventory_cursor(
        &mut self,
        bucket: &str,
        config_id: &str,
    ) -> CResult<Option<InventoryCursorRow>> {
        get_cursor(self, bucket, config_id).await
    }

    async fn inventory_cursor_upsert(&mut self, row: &InventoryCursorRow) -> CResult<bool> {
        if let Some(existing) = get_cursor(self, &row.bucket, &row.config_id).await?
            && existing.fence_term > row.fence_term
        {
            return Ok(false);
        }
        let bytes = encode_row(row)?;
        self.put(
            TableId::InventoryCursors,
            &key2(&row.bucket, &row.config_id),
            &bytes,
        )
        .await
        .map_err(backend)?;
        Ok(true)
    }

    async fn inventory_cursor_delete(
        &mut self,
        bucket: &str,
        config_id: &str,
        fence_term: Option<i64>,
    ) -> CResult<()> {
        let keep = match (fence_term, get_cursor(self, bucket, config_id).await?) {
            (None, _) => false,
            (Some(term), Some(row)) => row.fence_term > term,
            (Some(_), None) => return Ok(()),
        };
        if !keep {
            self.delete(TableId::InventoryCursors, &key2(bucket, config_id))
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn inventory_incremental(
        &mut self,
        bucket: &str,
        config_id: &str,
    ) -> CResult<Option<InventoryIncrementalRow>> {
        get_incremental(self, bucket, config_id).await
    }

    async fn inventory_incremental_upsert(
        &mut self,
        row: &InventoryIncrementalRow,
    ) -> CResult<bool> {
        let bytes = encode_row(row)?;
        self.put(
            TableId::InventoryIncremental,
            &key2(&row.bucket, &row.config_id),
            &bytes,
        )
        .await
        .map_err(backend)?;
        Ok(true)
    }

    async fn inventory_incremental_delete(&mut self, bucket: &str, config_id: &str) -> CResult<()> {
        self.delete(TableId::InventoryIncremental, &key2(bucket, config_id))
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn inventory_clear(&mut self, bucket: &str, config_id: &str) -> CResult<()> {
        self.inventory_cursor_delete(bucket, config_id, None)
            .await?;
        self.inventory_incremental_delete(bucket, config_id).await
    }
}

#[async_trait::async_trait]
impl BucketDomainCleanup for ObjectControlTxn {
    async fn delete_lifecycle_inventory_tier_for_bucket(&mut self, bucket: &str) -> CResult<()> {
        for table in [
            TableId::InventoryRuns,
            TableId::InventoryConfigs,
            TableId::InventoryCursors,
            TableId::InventoryIncremental,
            TableId::TierRestores,
            TableId::LifecycleRules,
            TableId::LifecycleCursors,
        ] {
            delete_prefix(self, table, bucket).await?;
        }
        Ok(())
    }
}
