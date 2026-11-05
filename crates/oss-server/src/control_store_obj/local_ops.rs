//! Shared plumbing for the node-local operational domain adapters (design D4).
//!
//! These domains live in `crate::local_store`, not in the root: a root CAS per
//! event/upload/audit write would be far too expensive and they share no
//! atomicity boundary with control state. Writes are applied immediately and
//! are therefore not rolled back when an `ObjectControlTxn` is dropped; callers
//! must not run them inside a retried [`super::ObjectControlStore::transaction`]
//! closure (see the slice report for the remaining B4 wiring note).

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use xl_store::control_store::ControlError;

use super::{ObjectControlStore, ObjectControlTxn};
use crate::local_store::{LocalKv, LocalQueue, LocalStore};

const MISSING_LOCAL: &str = "object control store has no local store";

impl ObjectControlStore {
    /// Attach an already-open node-local store (design D4).
    pub fn with_local_store(mut self, local: LocalStore) -> Self {
        self.local = Some(Arc::new(local));
        self
    }

    /// Open (creating directories as needed) the node-local store under `dir`.
    pub fn with_local_dir(self, dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        Ok(self.with_local_store(LocalStore::open(dir)?))
    }

    /// Local handle for the operational domains; fails closed when the backend
    /// was built without one instead of silently dropping state.
    pub fn local_store(&self) -> Result<&LocalStore, ControlError> {
        self.local
            .as_deref()
            .ok_or_else(|| ControlError::Backend(MISSING_LOCAL.into()))
    }
}

impl ObjectControlTxn {
    /// Local handle for the operational domains; writes there are immediate
    /// and are not rolled back with this root transaction (design D4).
    pub fn local_store(&self) -> Result<&LocalStore, ControlError> {
        self.local
            .as_deref()
            .ok_or_else(|| ControlError::Backend(MISSING_LOCAL.into()))
    }
}

pub(crate) const EVENT_QUEUE: &str = "event_outbox";
pub(crate) const REPL_QUEUE: &str = "repl_outbox";
pub(crate) const SYNC_WRITE_KV: &str = "sync_write_intents";
pub(crate) const SYNC_DELETE_KV: &str = "sync_delete_intents";

/// The SQL outbox tables carry no lease at claim time; expiry is decided by the
/// caller's constants at reclaim (`events::EVENT_LEASE_SECS` = 60s,
/// `replication::REPL_LEASE_SECS` = 300s). The local queue records a lease so
/// its own bookkeeping stays consistent; reclaim still uses `claimed_at` plus
/// the caller's `lease_secs`, matching the SQL `claimed_at < now - lease_secs`.
pub(crate) const EVENT_LEASE_SECS: i64 = 60;
pub(crate) const REPL_LEASE_SECS: i64 = 300;

pub(super) fn backend(e: io::Error) -> ControlError {
    ControlError::Backend(format!("local store: {e}"))
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ControlError> {
    rmp_serde::to_vec_named(value).map_err(|e| ControlError::Backend(format!("local encode: {e}")))
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ControlError> {
    rmp_serde::from_slice(bytes).map_err(|e| ControlError::Backend(format!("local decode: {e}")))
}

pub(super) fn queue(local: &LocalStore, name: &str) -> Result<LocalQueue, ControlError> {
    local.queue(name).map_err(backend)
}

pub(super) fn kv(local: &LocalStore, name: &str) -> Result<LocalKv, ControlError> {
    local.kv(name).map_err(backend)
}

/// SQL outbox tokens are the `claimed_at` unix second; the queue stores them as
/// its opaque lease token so stale-token guards behave identically.
pub(super) fn token(claimed_at: i64) -> String {
    claimed_at.to_string()
}

pub(super) fn token_str(value: Option<i64>) -> Option<String> {
    value.map(token)
}

/// Outbox attempt counters are `i64` in SQL; the local queue stores `u32`.
pub(super) fn attempts_of(value: i64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// SQLite `LIMIT` with a negative value means "no limit".
pub(super) fn limit_of(value: i64) -> Option<usize> {
    if value < 0 {
        None
    } else {
        Some(value as usize)
    }
}

/// Sequence ids are `u64` in the queue and `i64` in the SQL traits.
pub(super) fn seq_of(id: i64) -> Option<u64> {
    u64::try_from(id).ok()
}
