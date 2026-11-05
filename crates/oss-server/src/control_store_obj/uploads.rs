//! Local-file `uploads` adapter (design D4 / §7).
//!
//! Records are KV entries keyed by `saved_path`; the local-store record carries
//! the same columns plus owner/expiry used by the multipart cleanup paths.
//! `saved_path` conflicts keep the first row, matching
//! `INSERT ... ON CONFLICT(saved_path) DO NOTHING`.
//!
//! shortcut: `upload_register`/`upload_claim` are read-modify-write with no
//! cross-process CAS (the SQL `UPDATE ... WHERE` is atomic). One control-plane
//! writer per node is the local-store model; a second writer can overwrite the
//! other's owner. Upgrade to a lock or CAS record if multi-writer uploads return.

use xl_store::control_store::ControlError;
use xl_store::control_store::uploads::{NewUpload, UploadRow, UploadStore, UploadTxn};

use super::local_ops::{backend, limit_of};
use super::{ObjectControlStore, ObjectControlTxn};
use crate::local_store::UploadRecord;

fn row(record: UploadRecord) -> UploadRow {
    UploadRow {
        saved_path: record.saved_path,
        bucket: record.bucket,
        key: record.key,
        group_name: record.group_name,
        size: record.size,
        etag: record.etag,
        original_name: record.original_name,
        created_at: record.created_at,
    }
}

fn ordered(mut rows: Vec<UploadRecord>, limit: i64) -> Vec<UploadRow> {
    rows.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.saved_path.cmp(&b.saved_path))
    });
    if let Some(limit) = limit_of(limit) {
        rows.truncate(limit);
    }
    rows.into_iter().map(row).collect()
}

#[async_trait::async_trait]
impl UploadStore for ObjectControlStore {
    async fn upload(&self, saved_path: &str) -> Result<Option<UploadRow>, ControlError> {
        let uploads = self.local_store()?.uploads().map_err(backend)?;
        Ok(uploads.get(saved_path).map_err(backend)?.map(row))
    }

    async fn uploads(&self, limit: i64) -> Result<Vec<UploadRow>, ControlError> {
        let uploads = self.local_store()?.uploads().map_err(backend)?;
        Ok(ordered(uploads.all().map_err(backend)?, limit))
    }

    async fn uploads_for(&self, owner_id: i64, limit: i64) -> Result<Vec<UploadRow>, ControlError> {
        let uploads = self.local_store()?.uploads().map_err(backend)?;
        let mut rows = uploads.all().map_err(backend)?;
        rows.retain(|r| r.owner_uid == Some(owner_id));
        Ok(ordered(rows, limit))
    }
}

#[async_trait::async_trait]
impl UploadTxn for ObjectControlTxn {
    async fn upload_record(&mut self, new: &NewUpload) -> Result<(), ControlError> {
        let uploads = self.local_store()?.uploads().map_err(backend)?;
        uploads
            .put_if_absent(&UploadRecord {
                saved_path: new.saved_path.clone(),
                bucket: new.bucket.clone(),
                key: new.key.clone(),
                group_name: new.group_name.clone(),
                size: new.size,
                etag: new.etag.clone(),
                original_name: None,
                owner_uid: None,
                created_at: new.created_at,
                expires_at: None,
            })
            .map_err(backend)?;
        Ok(())
    }

    async fn upload_register(
        &mut self,
        saved_path: &str,
        original_name: &str,
        owner_id: i64,
        is_admin: bool,
    ) -> Result<bool, ControlError> {
        let uploads = self.local_store()?.uploads().map_err(backend)?;
        let Some(mut record) = uploads.get(saved_path).map_err(backend)? else {
            return Ok(false);
        };
        if !is_admin && record.owner_uid.is_some_and(|owner| owner != owner_id) {
            return Ok(false);
        }
        record.original_name = Some(original_name.to_string());
        record.owner_uid = Some(owner_id);
        uploads.put(&record).map_err(backend)?;
        Ok(true)
    }

    async fn upload_claim(
        &mut self,
        saved_path: &str,
        owner_id: i64,
    ) -> Result<bool, ControlError> {
        let uploads = self.local_store()?.uploads().map_err(backend)?;
        let Some(mut record) = uploads.get(saved_path).map_err(backend)? else {
            return Ok(false);
        };
        if record.owner_uid.is_some() {
            return Ok(false);
        }
        record.owner_uid = Some(owner_id);
        uploads.put(&record).map_err(backend)?;
        Ok(true)
    }
}
