//! Object-backed bucket writes (`BucketTxn`).

use xl_store::control_store::bucket::{BucketCreateOutcome, BucketEntry, BucketTxn};
use xl_store::control_store::{ControlError, OwnerRef};

use super::bucket::{
    BucketRow, CResult, UserLookup, admin_uid, backend, decode_row, encode_row, key1, txn_scan_all,
    user_by_name,
};
use super::site_apply::{BucketSiteRow, PolicySiteRow};
use super::{ObjectControlTxn, TableId};

async fn bucket_site_row(txn: &ObjectControlTxn, row: &BucketRow) -> CResult<BucketSiteRow> {
    let owner_username = match row.owner_uid {
        Some(uid) => UserLookup::user_by_uid(txn, uid).await?.map(|u| u.username),
        None => None,
    };
    Ok(BucketSiteRow {
        name: row.name.clone(),
        owner_uid: row.owner_uid,
        owner_username,
        creation_date: row.creation_date,
        versioning: row.versioning.clone(),
        object_lock: row.object_lock,
    })
}

/// M7 trigger `site_log_bucket_{ins,upd,del}` (update only for
/// versioning/owner/object-lock — the hook applies that predicate).
async fn emit_bucket(
    txn: &mut ObjectControlTxn,
    old: Option<&BucketRow>,
    new: Option<&BucketRow>,
) -> CResult<()> {
    let old = match old {
        Some(row) => Some(bucket_site_row(txn, row).await?),
        None => None,
    };
    let new = match new {
        Some(row) => Some(bucket_site_row(txn, row).await?),
        None => None,
    };
    txn.site_hook_bucket(old.as_ref(), new.as_ref(), crate::db::now())
        .await
}

async fn policy_site_row(txn: &ObjectControlTxn, bucket: &str) -> CResult<Option<PolicySiteRow>> {
    let Some(bytes) = txn
        .get(TableId::BucketPolicies, &key1(bucket))
        .await
        .map_err(backend)?
    else {
        return Ok(None);
    };
    let row: xl_store::control_store::bucket::BucketPolicyEntry = decode_row(&bytes)?;
    Ok(Some(PolicySiteRow {
        bucket: row.bucket,
        policy_json: row.policy_json,
        updated_at: row.updated_at,
    }))
}

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
            let old = row.clone();
            row.owner_uid = None;
            put_bucket(txn, &row).await?;
            emit_bucket(txn, Some(&old), Some(&row)).await?;
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
    let old = row.clone();
    f(&mut row);
    put_bucket(txn, &row).await?;
    emit_bucket(txn, Some(&old), Some(&row)).await?;
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
            let old = existing.clone();
            existing.owner_uid = owner_uid;
            existing.versioning = row.versioning.clone();
            existing.object_lock = row.object_lock;
            existing.object_lock_default_mode = row.object_lock_default_mode.clone();
            existing.object_lock_default_days = row.object_lock_default_days;
            existing.default_encryption = row.default_encryption.clone();
            existing.replication_mode = row.replication_mode.clone();
            existing.deleting = false;
            put_bucket(self, &existing).await?;
            emit_bucket(self, Some(&old), Some(&existing)).await?;
            return Ok(BucketCreateOutcome::Updated);
        }
        let mut created = BucketRow::new(row, owner_uid, 0);
        created.deleting = false;
        put_bucket(self, &created).await?;
        emit_bucket(self, None, Some(&created)).await?;
        Ok(BucketCreateOutcome::Created)
    }

    async fn bucket_delete(&mut self, name: &str) -> CResult<bool> {
        let old = get_bucket(self, name).await?;
        let deleted = self
            .delete(TableId::Buckets, &key1(name))
            .await
            .map_err(backend)?;
        if deleted && let Some(old) = old {
            emit_bucket(self, Some(&old), None).await?;
        }
        Ok(deleted)
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
        let old = policy_site_row(self, bucket).await?;
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
            .map_err(backend)?;
        let new = PolicySiteRow {
            bucket: row.bucket,
            policy_json: row.policy_json,
            updated_at: row.updated_at,
        };
        self.site_hook_policy(old.as_ref(), Some(&new), crate::db::now())
            .await
    }

    async fn bucket_policy_delete(&mut self, bucket: &str) -> CResult<()> {
        let old = policy_site_row(self, bucket).await?;
        self.delete(TableId::BucketPolicies, &key1(bucket))
            .await
            .map_err(backend)?;
        if let Some(old) = old {
            self.site_hook_policy(Some(&old), None, crate::db::now())
                .await?;
        }
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
        // Only bucket_policies is a site-replicated entity; the trigger emits a
        // delete for an existing row.
        let old = policy_site_row(self, bucket).await?;
        for table in [
            TableId::BucketPolicies,
            TableId::EventConfigs,
            TableId::ReplicationRules,
        ] {
            self.delete(table, &key1(bucket)).await.map_err(backend)?;
        }
        if let Some(old) = old {
            self.site_hook_policy(Some(&old), None, crate::db::now())
                .await?;
        }
        Ok(())
    }

    async fn bucket_clear_owner(&mut self, uid: i64) -> CResult<()> {
        bucket_clear_owner(self, uid).await
    }

    async fn bucket_insert_ignore(
        &mut self,
        name: &str,
        owner_id: Option<i64>,
        set_id: i64,
        replication_mode: &str,
        creation_date: i64,
    ) -> CResult<()> {
        if get_bucket(self, name).await?.is_some() {
            return Ok(());
        }
        let row = BucketRow {
            name: name.into(),
            owner_uid: owner_id,
            set_id,
            creation_date,
            versioning: "Off".into(),
            object_lock: false,
            default_encryption: None,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            replication_mode: replication_mode.into(),
            deleting: false,
        };
        put_bucket(self, &row).await?;
        emit_bucket(self, None, Some(&row)).await
    }

    async fn bucket_upsert_owner_set_mode(
        &mut self,
        name: &str,
        owner_id: Option<i64>,
        set_id: i64,
        replication_mode: &str,
        creation_date: i64,
    ) -> CResult<()> {
        let old = get_bucket(self, name).await?;
        let mut row = match old.clone() {
            Some(row) => row,
            None => BucketRow {
                name: name.into(),
                owner_uid: owner_id,
                set_id,
                creation_date,
                versioning: "Off".into(),
                object_lock: false,
                default_encryption: None,
                object_lock_default_mode: None,
                object_lock_default_days: None,
                replication_mode: replication_mode.into(),
                deleting: false,
            },
        };
        // ON CONFLICT 全列重置（creation_date 保留首建）。
        row.owner_uid = owner_id;
        row.set_id = set_id;
        row.versioning = if replication_mode == "sync" {
            "Enabled".into()
        } else {
            "Off".into()
        };
        row.object_lock = false;
        row.object_lock_default_mode = None;
        row.object_lock_default_days = None;
        row.default_encryption = None;
        row.replication_mode = replication_mode.into();
        row.deleting = false;
        put_bucket(self, &row).await?;
        emit_bucket(self, old.as_ref(), Some(&row)).await
    }

    async fn bucket_upsert_set(
        &mut self,
        name: &str,
        set_id: i64,
        creation_date: i64,
    ) -> CResult<()> {
        let mut row = match get_bucket(self, name).await? {
            Some(row) => row,
            None => BucketRow {
                name: name.into(),
                owner_uid: None,
                set_id,
                creation_date,
                versioning: "Off".into(),
                object_lock: false,
                default_encryption: None,
                object_lock_default_mode: None,
                object_lock_default_days: None,
                replication_mode: "async".into(),
                deleting: false,
            },
        };
        row.set_id = set_id;
        put_bucket(self, &row).await
    }
}
