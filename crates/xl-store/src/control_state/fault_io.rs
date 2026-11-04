//! In-memory NodeIo and root-CAS doubles with injectable faults.
//!
//! Compiled for unit tests and behind the `test-fault-io` feature so crash and
//! retry tests in later layers can share one deterministic harness.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::io;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::control_state::io::NodeIo;
use crate::error::{Error, Result};

#[derive(Default)]
struct FaultState {
    objects: BTreeMap<String, Vec<u8>>,
    puts: usize,
    fail_after: Option<usize>,
}

/// Ordered in-memory object map with create-once and put failpoints.
#[derive(Default)]
pub struct FaultIo {
    state: Mutex<FaultState>,
}

impl FaultIo {
    pub fn new() -> Self {
        Self::default()
    }

    /// Failed puts at or after this many successful puts; `None` clears it.
    pub fn fail_after(&self, limit: Option<usize>) {
        self.state.lock().unwrap().fail_after = limit;
    }

    pub fn puts(&self) -> usize {
        self.state.lock().unwrap().puts
    }

    pub fn keys(&self) -> Vec<String> {
        self.state.lock().unwrap().objects.keys().cloned().collect()
    }

    /// Bypass create-once to simulate an object damaged after it was written.
    pub fn force_put(&self, key: &str, bytes: &[u8]) {
        self.state
            .lock()
            .unwrap()
            .objects
            .insert(key.to_owned(), bytes.to_vec());
    }
}

#[async_trait]
impl NodeIo for FaultIo {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.state.lock().unwrap().objects.get(key).cloned())
    }

    async fn put_create(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.fail_after.is_some_and(|limit| state.puts >= limit) {
            return Err(Error::Io(io::Error::other("injected put failure")));
        }
        if let Some(existing) = state.objects.get(key) {
            if existing.as_slice() != bytes {
                return Err(Error::CorruptMeta(format!(
                    "duplicate create of {key} with different bytes"
                )));
            }
            return Ok(());
        }
        state.objects.insert(key.to_owned(), bytes.to_vec());
        state.puts += 1;
        Ok(())
    }
}

#[derive(Default)]
struct RootState {
    bytes: Option<Vec<u8>>,
    version: u64,
    partial_next: bool,
}

/// Committed root generation returned by [`FaultRootCas::load`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootSlot {
    pub version: u64,
    pub bytes: Vec<u8>,
}

/// In-memory root CAS with an injectable partial-write fault.
#[derive(Default)]
pub struct FaultRootCas {
    state: Mutex<RootState>,
}

impl FaultRootCas {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(&self) -> Option<RootSlot> {
        let state = self.state.lock().unwrap();
        state.bytes.as_ref().map(|bytes| RootSlot {
            version: state.version,
            bytes: bytes.clone(),
        })
    }

    /// Compare-and-swap the root. Returns the new committed version.
    pub fn cas(&self, expected: u64, bytes: &[u8]) -> Result<u64> {
        let mut state = self.state.lock().unwrap();
        if expected != state.version {
            return Err(Error::Conflict(format!(
                "stale root version {expected}, current {}",
                state.version
            )));
        }
        state.version += 1;
        state.bytes = Some(bytes.to_vec());
        if std::mem::take(&mut state.partial_next) {
            // The write landed but the caller cannot know: it must re-read.
            return Err(Error::Indeterminate("injected partial root CAS".into()));
        }
        Ok(state.version)
    }

    /// Next CAS stores the new root, then reports `Indeterminate`.
    pub fn fail_next_cas_partially(&self) {
        self.state.lock().unwrap().partial_next = true;
    }
}
