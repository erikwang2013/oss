//! Node-local `instant_index` adapter (design D4).
//!
//! The SQLite table is node-local operational state, so the object backend keeps
//! it in the local KV under the same namespace and record shape the migrator
//! writes (`migrate::local_sink::INSTANT_KV`, `SourceInstant`), letting migrated
//! rows stay readable. Expiry is compared against the caller's `now`, matching
//! the SQL `expires_at > now` / `expires_at <= now` predicates.

use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;

use super::ObjectControlStore;
use super::local_ops::{backend, decode, encode, kv};

/// KV namespace; same value as `migrate::local_sink::INSTANT_KV`.
pub(crate) const INSTANT_KV: &str = "instant_index";

/// Migrator-compatible record (`SourceInstant` field names).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StoredInstant {
    pub(crate) k: String,
    pub(crate) v: String,
    pub(crate) expires_at: i64,
}

impl ObjectControlStore {
    fn instant_kv(&self) -> Result<crate::local_store::LocalKv, ControlError> {
        kv(self.local_store()?, INSTANT_KV)
    }

    /// Expired rows read as missing, like the SQL `expires_at > now` predicate.
    pub fn instant_get(&self, k: &str, now: i64) -> Result<Option<String>, ControlError> {
        let Some(bytes) = self.instant_kv()?.get(k).map_err(backend)? else {
            return Ok(None);
        };
        let row: StoredInstant = decode(&bytes)?;
        Ok((row.expires_at > now).then_some(row.v))
    }

    pub fn instant_set(&self, k: &str, v: &str, expires_at: i64) -> Result<(), ControlError> {
        let bytes = encode(&StoredInstant {
            k: k.to_string(),
            v: v.to_string(),
            expires_at,
        })?;
        self.instant_kv()?.put(k, &bytes).map_err(backend)?;
        Ok(())
    }

    pub fn instant_del(&self, k: &str) -> Result<(), ControlError> {
        self.instant_kv()?.delete(k).map_err(backend)?;
        Ok(())
    }

    /// Byte prefix; the sole caller passes the fixed lowercase
    /// `aetherupload:resource:` constant, where SQLite LIKE folding is moot.
    pub fn instant_delete_prefix(&self, prefix: &str) -> Result<(), ControlError> {
        let kv = self.instant_kv()?;
        for entry in kv.scan().map_err(backend)? {
            if entry.key.starts_with(prefix) {
                kv.delete(&entry.key).map_err(backend)?;
            }
        }
        Ok(())
    }

    /// Deletes at most `limit` expired rows, earliest expiry first (SQL
    /// `ORDER BY expires_at LIMIT ?`), and returns the deleted count.
    pub fn instant_purge_expired(&self, now: i64, limit: i64) -> Result<u64, ControlError> {
        let kv = self.instant_kv()?;
        let mut expired = Vec::new();
        for entry in kv.scan().map_err(backend)? {
            let row: StoredInstant = decode(&entry.value)?;
            if row.expires_at <= now {
                expired.push((row.expires_at, entry.key));
            }
        }
        expired.sort();
        expired.truncate(limit.max(1) as usize);
        let mut removed = 0u64;
        for (_, key) in expired {
            if kv.delete(&key).map_err(backend)? {
                removed += 1;
            }
        }
        Ok(removed)
    }
}
