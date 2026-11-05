//! Object-backed bucket writes (`BucketTxn`).

use xl_store::control_store::bucket::{BucketCreateOutcome, BucketEntry, BucketTxn};
use xl_store::control_store::{ControlError, OwnerRef};

use super::bucket::{
    BucketRow, CResult, admin_uid, backend, decode_row, encode_row, key1, txn_scan_all,
    user_by_name,
};
use super::{ObjectControlTxn, TableId};

async fn get_bucket(txn: &ObjectControlTxn, name: &str) -> CResult<Option<BucketRow>> {
    let row = txn
        .get(TableId::Buckets, &key1(name))
        .await
        .map_err(backend)?;
    row.as_deref().map(decode_row).transpose()
}

async fn put_bucket(txn: &mut ObjectControlTxn, row: &BucketRow) -> CResult<()> {
    let bytes = encode_row(row)?;
    txn.put(TableId::Buckets, &key1(&row.name), &bytes)
        .await
        .map_err(backend)
}

/// `Admin` → NULL; existing user (admin included) → local uid; missing user with
/// an admin present → `Invalid`; witness (no admin) → NULL.
async fn resolve_owner(txn: &ObjectControlTxn, owner: &OwnerRef) -> CResult<Option<i64>> {
    let admin = admin_uid(txn).await?;
    match owner {
        OwnerRef::Admin => Ok(None),
        OwnerRef::User(username) => match user_by_name(txn, username).await? {
            Some((uid, _)) => Ok(Some(uid)),
            None if admin.is_some() => Err(ControlError::Invalid(format!(
                "bucket owner user {username} not found"
            ))),
            None => Ok(None),
        },
    }
}

/// UserDelete cascade: buckets owned by `uid` become admin-owned (SQLite's
/// `UPDATE buckets SET owner_id = NULL`), inside the enclosing root CAS.
pub(crate) async fn bucket_clear_owner(txn: &mut ObjectControlTxn, uid: i64) -> CResult<()> {
    for (_, value) in txn_scan_all(txn, TableId::Buckets).await? {
        let mut row: BucketRow = decode_row(&value)?;
        if row.owner_uid == Some(uid) {
            row.owner_uid = None;
            put_bucket(txn, &row).await?;
        }
    }
    Ok(())
}

async fn mutate_bucket(
    txn: &mut ObjectControlTxn,
    name: &str,
    f: impl FnOnce(&mut BucketRow),
) -> CResult<bool> {
    let Some(mut row) = get_bucket(txn, name).await? else {
        return Ok(false);
    };
    f(&mut row);
    put_bucket(txn, &row).await?;
    Ok(true)
}

#[async_trait::async_trait]
impl BucketTxn for ObjectControlTxn {
    async fn bucket_create(&mut self, row: &BucketEntry) -> CResult<BucketCreateOutcome> {
        let owner_uid = resolve_owner(self, &row.owner).await?;
        if let Some(bytes) = self
            .get(TableId::Buckets, &key1(&row.name))
            .await
            .map_err(backend)?
        {
            let mut existing: BucketRow = decode_row(&bytes)?;
            if existing.replication_mode != row.replication_mode {
                return Ok(BucketCreateOutcome::ModeConflict);
            }
            existing.owner_uid = owner_uid;
            existing.versioning = row.versioning.clone();
            existing.object_lock = row.object_lock;
            existing.object_lock_default_mode = row.object_lock_default_mode.clone();
            existing.object_lock_default_days = row.object_lock_default_days;
            existing.default_encryption = row.default_encryption.clone();
            existing.replication_mode = row.replication_mode.clone();
            existing.deleting = false;
            put_bucket(self, &existing).await?;
            return Ok(BucketCreateOutcome::Updated);
        }
        let mut created = BucketRow::new(row, owner_uid, 0);
        created.deleting = false;
        put_bucket(self, &created).await?;
        Ok(BucketCreateOutcome::Created)
    }

    async fn bucket_delete(&mut self, name: &str) -> CResult<bool> {
        self.delete(TableId::Buckets, &key1(name))
            .await
            .map_err(backend)
    }

    async fn bucket_set_deleting(&mut self, name: &str, deleting: bool) -> CResult<bool> {
        mutate_bucket(self, name, |row| row.deleting = deleting).await
    }

    async fn bucket_set_versioning(&mut self, name: &str, versioning: &str) -> CResult<bool> {
        mutate_bucket(self, name, |row| row.versioning = versioning.into()).await
    }

    async fn bucket_set_object_lock(
        &mut self,
        name: &str,
        object_lock: bool,
        default_mode: Option<&str>,
        default_days: Option<i64>,
    ) -> CResult<bool> {
        mutate_bucket(self, name, |row| {
            row.object_lock = object_lock;
            row.object_lock_default_mode = default_mode.map(Into::into);
            row.object_lock_default_days = default_days;
        })
        .await
    }

    async fn bucket_set_default_encryption(
        &mut self,
        name: &str,
        algorithm: &str,
    ) -> CResult<bool> {
        mutate_bucket(self, name, |row| {
            row.default_encryption = Some(algorithm.into())
        })
        .await
    }

    async fn bucket_clear_default_encryption(&mut self, name: &str) -> CResult<bool> {
        mutate_bucket(self, name, |row| row.default_encryption = None).await
    }

    async fn bucket_policy_set(
        &mut self,
        bucket: &str,
        policy_json: &str,
        updated_at: i64,
    ) -> CResult<()> {
        let mut row = match self
            .get(TableId::BucketPolicies, &key1(bucket))
            .await
            .map_err(backend)?
        {
            Some(bytes) => decode_row(&bytes)?,
            None => xl_store::control_store::bucket::BucketPolicyEntry {
                bucket: bucket.into(),
                policy_json: String::new(),
                updated_at: 0,
            },
        };
        row.policy_json = policy_json.into();
        row.updated_at = updated_at;
        let bytes = encode_row(&row)?;
        self.put(TableId::BucketPolicies, &key1(bucket), &bytes)
            .await
            .map_err(backend)
    }

    async fn bucket_policy_delete(&mut self, bucket: &str) -> CResult<()> {
        self.delete(TableId::BucketPolicies, &key1(bucket))
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn bucket_notification_set(
        &mut self,
        bucket: &str,
        config_json: &str,
        updated_at: i64,
    ) -> CResult<()> {
        let row = xl_store::control_store::bucket::EventConfigEntry {
            bucket: bucket.into(),
            config_json: config_json.into(),
            updated_at,
        };
        let bytes = encode_row(&row)?;
        self.put(TableId::EventConfigs, &key1(bucket), &bytes)
            .await
            .map_err(backend)
    }

    async fn bucket_notification_delete(&mut self, bucket: &str) -> CResult<()> {
        self.delete(TableId::EventConfigs, &key1(bucket))
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn bucket_replication_rules_set(
        &mut self,
        bucket: &str,
        rules_json: &str,
        updated_at: i64,
    ) -> CResult<()> {
        let row = xl_store::control_store::bucket::ReplicationRuleEntry {
            bucket: bucket.into(),
            rules_json: rules_json.into(),
            updated_at,
        };
        let bytes = encode_row(&row)?;
        self.put(TableId::ReplicationRules, &key1(bucket), &bytes)
            .await
            .map_err(backend)
    }

    async fn bucket_replication_rules_delete(&mut self, bucket: &str) -> CResult<()> {
        self.delete(TableId::ReplicationRules, &key1(bucket))
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn bucket_exists(&mut self, name: &str) -> CResult<bool> {
        Ok(self
            .get(TableId::Buckets, &key1(name))
            .await
            .map_err(backend)?
            .is_some())
    }

    async fn delete_bucket_configs_for_bucket(&mut self, bucket: &str) -> CResult<()> {
        for table in [
            TableId::BucketPolicies,
            TableId::EventConfigs,
            TableId::ReplicationRules,
        ] {
            self.delete(table, &key1(bucket)).await.map_err(backend)?;
        }
        Ok(())
    }
}
