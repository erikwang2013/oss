//! KV-style multipart upload records (the former `uploads` SQL table).
//!
//! `saved_path` is the key. The owner is stored as a numeric uid: uids are never
//! reused, so a dangling reference cannot silently attach to a later account.

use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{LocalKv, Result, invalid_input};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadRecord {
    pub saved_path: String,
    pub bucket: String,
    pub key: String,
    pub group_name: String,
    pub size: i64,
    pub etag: String,
    pub original_name: Option<String>,
    /// Numeric uid is stable because uids are never reused.
    pub owner_uid: Option<i64>,
    pub created_at: i64,
    /// Unix seconds; `None` = no expiry.
    pub expires_at: Option<i64>,
}

pub struct UploadStore {
    kv: LocalKv,
}

impl UploadStore {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            kv: LocalKv::open(dir)?,
        })
    }

    pub fn put(&self, record: &UploadRecord) -> Result<()> {
        if record.saved_path.is_empty() {
            return Err(invalid_input("upload saved_path must not be empty"));
        }
        let bytes = rmp_serde::to_vec(record).map_err(io::Error::other)?;
        self.kv.put(&record.saved_path, &bytes)?;
        Ok(())
    }

    pub fn get(&self, saved_path: &str) -> Result<Option<UploadRecord>> {
        match self.kv.get(saved_path)? {
            None => Ok(None),
            Some(bytes) => decode(&bytes).map(Some),
        }
    }

    pub fn delete(&self, saved_path: &str) -> Result<bool> {
        self.kv.delete(saved_path)
    }

    /// Live (non-expired) records, newest first, then `saved_path` ascending.
    pub fn list(&self, now: i64, limit: usize) -> Result<Vec<UploadRecord>> {
        let mut all = self.all()?;
        all.retain(|r| r.expires_at.is_none_or(|expires| expires > now));
        all.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.saved_path.cmp(&b.saved_path))
        });
        all.truncate(limit);
        Ok(all)
    }

    /// Records whose expiry is at or before `now` (for periodic cleanup).
    pub fn expired(&self, now: i64) -> Result<Vec<UploadRecord>> {
        let mut all = self.all()?;
        all.retain(|r| r.expires_at.is_some_and(|expires| expires <= now));
        Ok(all)
    }

    fn all(&self) -> Result<Vec<UploadRecord>> {
        self.kv
            .scan()?
            .into_iter()
            .map(|entry| decode(&entry.value))
            .collect()
    }
}

fn decode(bytes: &[u8]) -> Result<UploadRecord> {
    rmp_serde::from_slice(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::super::LocalStore;
    use super::UploadRecord;

    fn record(saved_path: &str, created_at: i64, expires_at: Option<i64>) -> UploadRecord {
        UploadRecord {
            saved_path: saved_path.into(),
            bucket: "b".into(),
            key: format!("k/{saved_path}"),
            group_name: "g".into(),
            size: 42,
            etag: "etag".into(),
            original_name: Some("file.bin".into()),
            owner_uid: Some(7),
            created_at,
            expires_at,
        }
    }

    #[test]
    fn put_get_list_delete() {
        let dir = tempfile::tempdir().unwrap();
        let uploads = LocalStore::open(dir.path()).unwrap().uploads().unwrap();
        assert_eq!(uploads.get("a").unwrap(), None);

        uploads.put(&record("a", 100, None)).unwrap();
        uploads.put(&record("b", 200, None)).unwrap();
        uploads.put(&record("c", 300, None)).unwrap();
        assert_eq!(uploads.get("a").unwrap().unwrap().owner_uid, Some(7));

        let paths: Vec<_> = uploads
            .list(1_000, 10)
            .unwrap()
            .into_iter()
            .map(|r| r.saved_path)
            .collect();
        assert_eq!(paths, ["c", "b", "a"]);
        assert_eq!(uploads.list(1_000, 2).unwrap().len(), 2);

        assert!(uploads.delete("b").unwrap());
        assert!(!uploads.delete("b").unwrap());
        assert_eq!(uploads.list(1_000, 10).unwrap().len(), 2);
    }

    #[test]
    fn expiry_scan_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let uploads = LocalStore::open(dir.path()).unwrap().uploads().unwrap();
        uploads.put(&record("live", 100, Some(2_000))).unwrap();
        uploads.put(&record("gone", 100, Some(500))).unwrap();
        uploads.put(&record("forever", 100, None)).unwrap();

        let live: Vec<_> = uploads
            .list(1_000, 10)
            .unwrap()
            .into_iter()
            .map(|r| r.saved_path)
            .collect();
        assert_eq!(live, ["forever", "live"]);

        let expired: Vec<_> = uploads
            .expired(1_000)
            .unwrap()
            .into_iter()
            .map(|r| r.saved_path)
            .collect();
        assert_eq!(expired, ["gone"]);
        for path in expired {
            assert!(uploads.delete(&path).unwrap());
        }
        assert_eq!(uploads.list(1_000, 10).unwrap().len(), 2);
    }
}
