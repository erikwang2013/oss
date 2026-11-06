//! Object-backed implementations of the `xl-s3` write seams (`AclWrites`,
//! `StsWrites`). Every method is one control-root transaction; on a local root
//! CAS conflict the whole closure re-runs. SQLite mode never constructs this
//! type, so `xl-s3`'s existing SQL stays the sole SQLite path byte for byte.

use std::sync::Arc;

use futures::future::BoxFuture;
use xl_store::control_store::ControlError;
use xl_store::control_store::account::AccountTxn;
use xl_store::control_store::bucket::BucketTxn;
use xl_store::control_store::iam::IamTxn;
use xl_store::control_store::lifecycle::LifecycleTxn;

use xl_s3::access::BucketRecordOutcome;
use xl_s3::control::{AclWrites, StsWrites};

use crate::control_backend::store_error;
use crate::control_store_obj::{
    AccessKeyRow, BucketRow, ObjectControlStore, ObjectControlTxn, TableId, WriteOrigin,
    decode_row, encode_row, insert_access_key, key1, txn_scan_all,
};

/// All writes go through one shared control store handle.
pub(crate) struct ObjectControlWrites {
    store: Arc<ObjectControlStore>,
}

impl ObjectControlWrites {
    pub(crate) fn new(store: Arc<ObjectControlStore>) -> Self {
        Self { store }
    }
}

fn cerr(e: impl std::fmt::Display) -> ControlError {
    ControlError::Backend(e.to_string())
}

/// One root-CAS transaction; `f` re-runs on conflict, so it must be `FnMut`.
async fn write<T: Send + 'static>(
    store: &Arc<ObjectControlStore>,
    actor: &str,
    mut f: impl for<'a> FnMut(&'a mut ObjectControlTxn) -> BoxFuture<'a, Result<T, ControlError>>,
) -> Result<T, ControlError> {
    store
        .transaction(WriteOrigin::local(actor), |txn| {
            let fut = f(txn);
            Box::pin(async move { fut.await.map_err(store_error) })
        })
        .await
        .map_err(cerr)
}

async fn bucket_row(txn: &ObjectControlTxn, name: &str) -> Result<Option<BucketRow>, ControlError> {
    match txn.get(TableId::Buckets, &key1(name)).await {
        Ok(Some(bytes)) => decode_row(&bytes).map(Some),
        Ok(None) => Ok(None),
        Err(e) => Err(cerr(e)),
    }
}

async fn put_bucket(txn: &mut ObjectControlTxn, row: &BucketRow) -> Result<(), ControlError> {
    let bytes = encode_row(row)?;
    txn.put(TableId::Buckets, &key1(&row.name), &bytes)
        .await
        .map_err(cerr)
}

/// The row `INSERT INTO buckets (..., object_lock) VALUES (?, NULL, ?, 'Enabled', 0, 1)`
/// creates when no row exists: every unspecified column takes its schema default.
fn fresh_locked_bucket(name: &str) -> BucketRow {
    BucketRow {
        name: name.into(),
        owner_uid: None,
        set_id: 0,
        creation_date: crate::db::now(),
        versioning: "Enabled".into(),
        object_lock: true,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting: false,
    }
}

#[async_trait::async_trait]
impl AclWrites for ObjectControlWrites {
    async fn enable_bucket_lock(&self, bucket: &str) -> Result<(), ControlError> {
        let bucket = bucket.to_owned();
        write(&self.store, "acl-lock", |txn| {
            let bucket = bucket.clone();
            Box::pin(async move {
                match bucket_row(txn, &bucket).await? {
                    Some(mut row) => {
                        row.object_lock = true;
                        row.versioning = "Enabled".into();
                        put_bucket(txn, &row).await
                    }
                    None => put_bucket(txn, &fresh_locked_bucket(&bucket)).await,
                }
            })
        })
        .await
    }

    async fn set_bucket_lock_rule(
        &self,
        bucket: &str,
        mode: Option<&str>,
        days: Option<i64>,
    ) -> Result<(), ControlError> {
        let (bucket, mode) = (bucket.to_owned(), mode.map(str::to_owned));
        write(&self.store, "acl-lock-rule", |txn| {
            let (bucket, mode) = (bucket.clone(), mode.clone());
            Box::pin(async move {
                let Some(mut row) = bucket_row(txn, &bucket).await? else {
                    return Ok(());
                };
                row.object_lock_default_mode = mode;
                row.object_lock_default_days = days;
                put_bucket(txn, &row).await
            })
        })
        .await
    }

    async fn clear_bucket_policy(&self, bucket: &str) -> Result<(), ControlError> {
        let bucket = bucket.to_owned();
        write(&self.store, "acl-policy-clear", |txn| {
            let bucket = bucket.clone();
            Box::pin(async move { BucketTxn::bucket_policy_delete(txn, &bucket).await })
        })
        .await
    }

    async fn set_bucket_notification(
        &self,
        bucket: &str,
        config_json: &str,
    ) -> Result<(), ControlError> {
        let (bucket, config_json) = (bucket.to_owned(), config_json.to_owned());
        write(&self.store, "acl-notification", |txn| {
            let (bucket, config_json) = (bucket.clone(), config_json.clone());
            Box::pin(async move {
                BucketTxn::bucket_notification_set(txn, &bucket, &config_json, crate::db::now())
                    .await
            })
        })
        .await
    }

    async fn clear_bucket_notification(&self, bucket: &str) -> Result<(), ControlError> {
        let bucket = bucket.to_owned();
        write(&self.store, "acl-notification-clear", |txn| {
            let bucket = bucket.clone();
            Box::pin(async move { BucketTxn::bucket_notification_delete(txn, &bucket).await })
        })
        .await
    }

    async fn set_bucket_default_encryption(
        &self,
        bucket: &str,
        algorithm: &str,
    ) -> Result<bool, ControlError> {
        let (bucket, algorithm) = (bucket.to_owned(), algorithm.to_owned());
        write(&self.store, "acl-encryption", |txn| {
            let (bucket, algorithm) = (bucket.clone(), algorithm.clone());
            Box::pin(async move {
                BucketTxn::bucket_set_default_encryption(txn, &bucket, &algorithm).await
            })
        })
        .await
    }

    async fn clear_bucket_default_encryption(&self, bucket: &str) -> Result<bool, ControlError> {
        let bucket = bucket.to_owned();
        write(&self.store, "acl-encryption-clear", |txn| {
            let bucket = bucket.clone();
            Box::pin(async move { BucketTxn::bucket_clear_default_encryption(txn, &bucket).await })
        })
        .await
    }

    async fn clear_lifecycle_rules(&self, bucket: &str) -> Result<(), ControlError> {
        let bucket = bucket.to_owned();
        write(&self.store, "acl-lifecycle-clear", |txn| {
            let bucket = bucket.clone();
            Box::pin(async move { LifecycleTxn::clear_lifecycle_rules(txn, &bucket).await })
        })
        .await
    }

    async fn set_bucket_versioning(
        &self,
        bucket: &str,
        status: &str,
    ) -> Result<bool, ControlError> {
        let (bucket, status) = (bucket.to_owned(), status.to_owned());
        write(&self.store, "acl-versioning", |txn| {
            let (bucket, status) = (bucket.clone(), status.clone());
            Box::pin(async move { BucketTxn::bucket_set_versioning(txn, &bucket, &status).await })
        })
        .await
    }

    async fn record_bucket_owner(&self, bucket: &str, uid: i64) -> Result<(), ControlError> {
        let bucket = bucket.to_owned();
        write(&self.store, "acl-owner", |txn| {
            let bucket = bucket.clone();
            Box::pin(async move {
                if bucket_row(txn, &bucket).await?.is_some() {
                    return Ok(());
                }
                let row = BucketRow {
                    name: bucket,
                    owner_uid: Some(uid),
                    set_id: 0,
                    creation_date: crate::db::now(),
                    versioning: "Off".into(),
                    object_lock: false,
                    default_encryption: None,
                    object_lock_default_mode: None,
                    object_lock_default_days: None,
                    replication_mode: "async".into(),
                    deleting: false,
                };
                put_bucket(txn, &row).await
            })
        })
        .await
    }

    async fn record_bucket_owner_set_mode(
        &self,
        bucket: &str,
        uid: i64,
        set: u16,
        replication_mode: &str,
    ) -> Result<BucketRecordOutcome, ControlError> {
        let (bucket, replication_mode) = (bucket.to_owned(), replication_mode.to_owned());
        write(&self.store, "acl-owner-set", |txn| {
            let (bucket, replication_mode) = (bucket.clone(), replication_mode.clone());
            Box::pin(async move {
                let existing = bucket_row(txn, &bucket).await?;
                if existing.is_none() && AccountTxn::bucket_quota_reached(txn, Some(uid)).await? {
                    return Ok(BucketRecordOutcome::QuotaExceeded);
                }
                let versioning = if replication_mode == "sync" {
                    "Enabled"
                } else {
                    "Off"
                };
                let row = match existing {
                    Some(mut row) => {
                        // ON CONFLICT resets every column but creation_date.
                        row.owner_uid = Some(uid);
                        row.set_id = set as i64;
                        row.versioning = versioning.into();
                        row.object_lock = false;
                        row.default_encryption = None;
                        row.object_lock_default_mode = None;
                        row.object_lock_default_days = None;
                        row.replication_mode = replication_mode;
                        row.deleting = false;
                        row
                    }
                    None => BucketRow {
                        name: bucket,
                        owner_uid: Some(uid),
                        set_id: set as i64,
                        creation_date: crate::db::now(),
                        versioning: versioning.into(),
                        object_lock: false,
                        default_encryption: None,
                        object_lock_default_mode: None,
                        object_lock_default_days: None,
                        replication_mode,
                        deleting: false,
                    },
                };
                put_bucket(txn, &row).await?;
                Ok(BucketRecordOutcome::Written)
            })
        })
        .await
    }

    async fn pick_least_loaded_set(&self, k: u16) -> Result<u16, ControlError> {
        if k <= 1 {
            return Ok(0);
        }
        let txn = self
            .store
            .begin(WriteOrigin::local("acl-set"))
            .await
            .map_err(cerr)?;
        let mut counts: std::collections::HashMap<u16, i64> = std::collections::HashMap::new();
        for (_, value) in txn_scan_all(&txn, TableId::Buckets).await? {
            let row: BucketRow = decode_row(&value)?;
            if let Ok(set) = u16::try_from(row.set_id) {
                *counts.entry(set).or_insert(0) += 1;
            }
        }
        let mut best = 0u16;
        let mut best_c = i64::MAX;
        for i in 0..k {
            let c = counts.get(&i).copied().unwrap_or(0);
            if c < best_c {
                best = i;
                best_c = c;
            }
        }
        Ok(best)
    }

    async fn forget_bucket(&self, bucket: &str) -> Result<(), ControlError> {
        let bucket = bucket.to_owned();
        write(&self.store, "acl-forget", |txn| {
            let bucket = bucket.clone();
            Box::pin(async move {
                IamTxn::permissions_delete_for_bucket(txn, &bucket).await?;
                BucketTxn::bucket_delete(txn, &bucket).await?;
                Ok(())
            })
        })
        .await
    }
}

#[async_trait::async_trait]
impl StsWrites for ObjectControlWrites {
    async fn issue_session(
        &self,
        access_key: &str,
        secret_key: &str,
        uid: i64,
        expires_at: i64,
        created_at: i64,
        parent_ak: Option<&str>,
        session_policy: Option<&str>,
    ) -> Result<(), ControlError> {
        let access_key = access_key.to_owned();
        let secret_key = secret_key.to_owned();
        let parent_ak = parent_ak.map(str::to_owned);
        let session_policy = session_policy.map(str::to_owned);
        write(&self.store, "sts-issue", |txn| {
            let access_key = access_key.clone();
            let secret_key = secret_key.clone();
            let parent_ak = parent_ak.clone();
            let session_policy = session_policy.clone();
            Box::pin(async move {
                let row = AccessKeyRow {
                    access_key,
                    secret_key,
                    user_id: uid,
                    status: "active".into(),
                    expires_at: Some(expires_at),
                    created_at,
                    parent_ak,
                    session_policy,
                    kind: "session".into(),
                };
                insert_access_key(txn, &row).await
            })
        })
        .await
    }
}
