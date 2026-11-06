//! Node-local account-usage cache (design D4): one `LocalKv` record per account.
//!
//! Same record shape as the SQLite `account_usage` row and the same trust model:
//! a value is a lower bound while `complete == false`; the sampler corrects it
//! from object listings. Writes are temp+fsync+rename, so a successful admit is
//! durable; a crash before the rename loses only that in-flight delta (the
//! sampler reconciles it). Corruption is fail-closed: quotas must not silently
//! over-admit on a value we cannot read.

use serde::{Deserialize, Serialize};
use xl_store::control_store::ControlError;

use crate::local_store::{LocalKv, LocalStore};

pub(crate) const NAMESPACE: &str = "account_usage";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Usage {
    pub used_bytes: i64,
    pub object_count: i64,
    pub complete: bool,
    pub computed_at: i64,
}

impl Usage {
    pub(crate) fn empty() -> Self {
        Self {
            used_bytes: 0,
            object_count: 0,
            complete: false,
            computed_at: 0,
        }
    }

    pub(crate) fn is_fresh(&self, now: i64, ttl_secs: i64) -> bool {
        now.saturating_sub(self.computed_at) <= ttl_secs
    }
}

pub(crate) struct UsageCache {
    local: LocalStore,
}

impl UsageCache {
    pub(crate) fn new(local: LocalStore) -> Self {
        Self { local }
    }

    fn kv(&self) -> Result<LocalKv, ControlError> {
        self.local
            .kv(NAMESPACE)
            .map_err(|e| ControlError::Backend(format!("local account usage: {e}")))
    }

    pub(crate) fn load(&self, account: &str) -> Result<Option<Usage>, ControlError> {
        let bytes = self
            .kv()?
            .get(account)
            .map_err(|e| ControlError::Backend(format!("local account usage: {e}")))?;
        match bytes {
            None => Ok(None),
            Some(bytes) => rmp_serde::from_slice(&bytes)
                .map(Some)
                .map_err(|e| ControlError::Backend(format!("local account usage decode: {e}"))),
        }
    }

    pub(crate) fn store(&self, account: &str, usage: &Usage) -> Result<(), ControlError> {
        let bytes = rmp_serde::to_vec_named(usage)
            .map_err(|e| ControlError::Backend(format!("local account usage encode: {e}")))?;
        self.kv()?
            .put(account, &bytes)
            .map_err(|e| ControlError::Backend(format!("local account usage: {e}")))?;
        Ok(())
    }

    /// Every cached account id (sampler removes accounts that stopped qualifying).
    pub(crate) fn accounts(&self) -> Result<Vec<String>, ControlError> {
        Ok(self
            .kv()?
            .scan()
            .map_err(|e| ControlError::Backend(format!("local account usage: {e}")))?
            .into_iter()
            .map(|e| e.key)
            .collect())
    }

    pub(crate) fn remove(&self, account: &str) -> Result<(), ControlError> {
        self.kv()?
            .delete(account)
            .map_err(|e| ControlError::Backend(format!("local account usage: {e}")))?;
        Ok(())
    }

    /// Snapshot install rebuilds the cache from listings; drop all stale rows.
    pub(crate) fn clear(&self) -> Result<(), ControlError> {
        let kv = self.kv()?;
        for account in self.accounts()? {
            kv.delete(&account)
                .map_err(|e| ControlError::Backend(format!("local account usage: {e}")))?;
        }
        Ok(())
    }
}
