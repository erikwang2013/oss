//! Object-backed bucket snapshot export/install (`BucketSnapshotTxn`).

use std::collections::HashSet;

use xl_store::control_store::bucket::{
    BucketEntry, BucketPolicyEntry, BucketSnapshotTxn, EventConfigEntry, ReplicationRuleEntry,
};
use xl_store::control_store::{ControlError, OwnerRef};

use super::bucket::{
    BucketRow, CResult, UserLookup, backend, clear_table, decode_row, encode_row, key1,
    non_admin_uid, txn_scan_all,
};
use super::{ObjectControlTxn, TableId};

/// Strict export mapping: NULL/0 or `is_admin` → `Admin`; non-admin user → `User`;
/// dangling non-zero id → `MissingOwner`.
async fn strict_entry<L: UserLookup + ?Sized>(dir: &L, row: BucketRow) -> CResult<BucketEntry> {
    let owner = match row.owner_uid {
        None | Some(0) => OwnerRef::Admin,
        Some(uid) => match dir.user_by_uid(uid).await? {
            Some(user) if user.is_admin => OwnerRef::Admin,
            Some(user) => OwnerRef::User(user.username),
            None => return Err(ControlError::MissingOwner(uid)),
        },
    };
    Ok(BucketEntry {
        name: row.name,
        owner,
        creation_date: row.creation_date,
        versioning: row.versioning,
        object_lock: row.object_lock,
        default_encryption: row.default_encryption,
        object_lock_default_mode: row.object_lock_default_mode,
        object_lock_default_days: row.object_lock_default_days,
        replication_mode: row.replication_mode,
        deleting: row.deleting,
    })
}

async fn put<T: serde::Serialize>(
    txn: &mut ObjectControlTxn,
    table: TableId,
    key: &str,
    row: &T,
) -> CResult<()> {
    let bytes = encode_row(row)?;
    txn.put(table, &key1(key), &bytes).await.map_err(backend)
}

#[async_trait::async_trait]
impl BucketSnapshotTxn for ObjectControlTxn {
    async fn buckets(&mut self) -> CResult<Vec<BucketEntry>> {
        let mut out = Vec::new();
        for (_, value) in txn_scan_all(self, TableId::Buckets).await? {
            out.push(strict_entry(self, decode_row(&value)?).await?);
        }
        Ok(out)
    }

    async fn bucket_policies(&mut self) -> CResult<Vec<BucketPolicyEntry>> {
        txn_scan_all(self, TableId::BucketPolicies)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_bucket_policies(&mut self, rows: &[BucketPolicyEntry]) -> CResult<()> {
        clear_table(self, TableId::BucketPolicies).await?;
        for row in rows {
            put(self, TableId::BucketPolicies, &row.bucket, row).await?;
        }
        Ok(())
    }

    async fn event_configs(&mut self) -> CResult<Vec<EventConfigEntry>> {
        txn_scan_all(self, TableId::EventConfigs)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_event_configs(&mut self, rows: &[EventConfigEntry]) -> CResult<()> {
        clear_table(self, TableId::EventConfigs).await?;
        for row in rows {
            put(self, TableId::EventConfigs, &row.bucket, row).await?;
        }
        Ok(())
    }

    async fn replication_rules(&mut self) -> CResult<Vec<ReplicationRuleEntry>> {
        txn_scan_all(self, TableId::ReplicationRules)
            .await?
            .into_iter()
            .map(|(_, value)| decode_row(&value))
            .collect()
    }

    async fn replace_replication_rules(&mut self, rows: &[ReplicationRuleEntry]) -> CResult<()> {
        clear_table(self, TableId::ReplicationRules).await?;
        for row in rows {
            put(self, TableId::ReplicationRules, &row.bucket, row).await?;
        }
        Ok(())
    }

    async fn bucket_install_prepare(&mut self, wanted: &[String]) -> CResult<()> {
        let wanted: HashSet<&str> = wanted.iter().map(String::as_str).collect();
        let rows: Vec<(Vec<u8>, BucketRow)> = txn_scan_all(self, TableId::Buckets)
            .await?
            .into_iter()
            .map(|(key, value)| Ok((key, decode_row(&value)?)))
            .collect::<CResult<_>>()?;
        for (key, row) in rows {
            if wanted.contains(row.name.as_str()) {
                // The SQL path clears every surviving bucket's owner before the
                // per-row upsert restores it from the snapshot.
                let mut cleared = row;
                cleared.owner_uid = None;
                let bytes = encode_row(&cleared)?;
                self.put(TableId::Buckets, &key, &bytes)
                    .await
                    .map_err(backend)?;
            } else {
                self.delete(TableId::Buckets, &key).await.map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn bucket_upsert_install(&mut self, row: &BucketEntry) -> CResult<()> {
        let owner_uid = match &row.owner {
            OwnerRef::Admin => None,
            OwnerRef::User(name) => Some(non_admin_uid(self, name).await?.ok_or_else(|| {
                ControlError::Invalid(format!("bucket owner {name} not in snapshot"))
            })?),
        };
        let set_id = match self
            .get(TableId::Buckets, &key1(&row.name))
            .await
            .map_err(backend)?
        {
            Some(bytes) => decode_row::<BucketRow>(&bytes)?.set_id,
            None => 0,
        };
        let installed = BucketRow::new(row, owner_uid, set_id);
        let bytes = encode_row(&installed)?;
        self.put(TableId::Buckets, &key1(&row.name), &bytes)
            .await
            .map_err(backend)
    }
}
