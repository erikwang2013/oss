#![allow(dead_code)] // Wired into RaftHandle/startup by F2.

//! Object-backed control state machine (implementation plan F1).
//!
//! One root CAS per log entry (design §2.4): the command's row mutations,
//! feature/runstate/inventory activation, the new applied LogId + membership
//! and the root allocators commit together. A business rejection
//! (`Invalid`/`Conflict`) is still an applied entry; only an internal error
//! aborts the CAS (fail-stop). Replaying an already-applied entry is a no-op.
//!
//! Data mode only. F2 still supplies the metadata-only witness state machine
//! (`witness_sm.rs`) and the file snapshot store; the snapshot kept here is
//! in memory.

use std::io::Cursor;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use openraft::entry::RaftPayload;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId,
    OptionalSend, Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use xl_store::control_state::root::{AppliedLog, RootMeta};
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::IamTxn;

use super::apply;
use super::compat;
use super::data;
use super::metrics::Metrics;
use super::store::storage_err;
use super::{ControlResponse, TypeConfig};
use crate::control_store_obj::{ObjectControlStore, WriteOrigin, bucket_clear_owner};

mod snapshot;
#[cfg(test)]
mod snapshot_crash_tests;
#[cfg(test)]
mod snapshot_gate_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;

/// Full applied `LogId` (including its node id) next to [`RootMeta::applied`],
/// which only carries `(term, index)`.
const META_APPLIED_LOG_ID: &str = "raft_applied_log_id";

fn store_err(e: ControlError) -> xl_store::error::Error {
    match e {
        ControlError::Invalid(m) => xl_store::error::Error::InvalidArgument(m),
        ControlError::Backend(m) => xl_store::error::Error::CorruptMeta(m),
        ControlError::MissingOwner(id) => {
            xl_store::error::Error::InvalidArgument(format!("owner user id {id} missing"))
        }
    }
}

fn data_err(e: data::DataError) -> StorageError<u64> {
    storage_err(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        std::io::Error::other(e.to_string()),
    )
}

fn sm_err(e: impl std::fmt::Display) -> StorageError<u64> {
    storage_err(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        std::io::Error::other(e.to_string()),
    )
}

fn activation_of(meta: &RootMeta) -> u8 {
    u8::try_from(meta.feature_level).unwrap_or(u8::MAX)
}

/// `(applied LogId, stored membership)` as committed in the root metadata.
type AppliedState = (Option<LogId<u64>>, StoredMembership<u64, BasicNode>);

fn applied_of(meta: &RootMeta) -> Result<AppliedState, StorageError<u64>> {
    let applied = match meta.markers.get(META_APPLIED_LOG_ID) {
        Some(json) => Some(
            serde_json::from_str(json)
                .map_err(|e| sm_err(format!("{META_APPLIED_LOG_ID} marker: {e}")))?,
        ),
        None => meta.applied.map(|a| {
            // Standard leader-id mode orders by term; the node id is not part
            // of `RootMeta` and only needs to be internally consistent.
            LogId::new(CommittedLeaderId::new(a.term, 0), a.index)
        }),
    };
    let membership = match &meta.membership_json {
        Some(json) => {
            serde_json::from_str(json).map_err(|e| sm_err(format!("raft membership json: {e}")))?
        }
        None => StoredMembership::default(),
    };
    Ok((applied, membership))
}

/// Clear owner references in the node-local upload store (D3: local files are
/// not part of the root CAS, so this runs after a committed apply/install).
fn scrub_local_uploads(
    store: &ObjectControlStore,
    remove: impl Fn(Option<i64>) -> bool,
) -> Result<(), StorageError<u64>> {
    let uploads = store
        .local_store()
        .map_err(sm_err)?
        .uploads()
        .map_err(sm_err)?;
    for mut record in uploads.all().map_err(sm_err)? {
        if remove(record.owner_uid) {
            record.owner_uid = None;
            uploads.put(&record).map_err(sm_err)?;
        }
    }
    Ok(())
}

/// Object-backed control-memory state machine.
#[derive(Clone)]
pub struct ObjectStateMachine {
    store: Arc<ObjectControlStore>,
    max_snapshot_bytes: u64,
    /// Configured `[raft].feature_level`; entries above it fail-stop (R10).
    feature_level: u8,
    activation: Arc<AtomicU8>,
    snapshot_lock: Arc<tokio::sync::Mutex<()>>,
    /// F2 replaces this with `FileSnapshotStore`; see the module docs.
    last_snapshot: Arc<Mutex<SnapshotPair>>,
    install_notify: Option<tokio::sync::watch::Sender<u64>>,
    counters: Arc<Metrics>,
}

/// Snapshot slot shared until F2 replaces it with `FileSnapshotStore`.
type SnapshotPair = Option<(SnapshotMeta<u64, BasicNode>, Vec<u8>)>;

impl ObjectStateMachine {
    pub async fn open(
        store: Arc<ObjectControlStore>,
        max_snapshot_bytes: u64,
        install_notify: Option<tokio::sync::watch::Sender<u64>>,
        feature_level: u8,
    ) -> Result<Self, StorageError<u64>> {
        let pinned = store
            .pin()
            .await
            .map_err(|e| sm_err(format!("control root: {e}")))?;
        Ok(Self {
            store,
            max_snapshot_bytes,
            feature_level,
            activation: Arc::new(AtomicU8::new(activation_of(&pinned.root.meta))),
            snapshot_lock: Arc::new(tokio::sync::Mutex::new(())),
            last_snapshot: Arc::new(Mutex::new(None)),
            install_notify,
            counters: Arc::new(Metrics::default()),
        })
    }

    /// Committed activation level (R8 startup guard / `/status`).
    pub fn activation_level(&self) -> u8 {
        self.activation.load(Ordering::Relaxed)
    }

    pub fn counters(&self) -> &Arc<Metrics> {
        &self.counters
    }

    /// 已应用 LogId 与成员（测试/F2 用）。
    pub async fn applied(
        &self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let pinned = self
            .store
            .pin()
            .await
            .map_err(|e| sm_err(format!("control root: {e}")))?;
        applied_of(&pinned.root.meta)
    }

    fn snapshot_slot(&self) -> std::sync::MutexGuard<'_, SnapshotPair> {
        self.last_snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Keep the newest snapshot only (mirrors SQLite's compare-and-write).
    fn remember_snapshot(&self, meta: &SnapshotMeta<u64, BasicNode>, bytes: &[u8]) {
        let mut slot = self.snapshot_slot();
        if slot
            .as_ref()
            .is_some_and(|(old, _)| old.last_log_id >= meta.last_log_id)
        {
            return;
        }
        *slot = Some((meta.clone(), bytes.to_vec()));
    }
}

impl RaftStateMachine<TypeConfig> for ObjectStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        self.applied().await
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ControlResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let result: Result<_, StorageError<u64>> = async {
            let entries: Vec<_> = entries.into_iter().collect();
            let (_, mut membership) = self.applied().await?;
            let mut responses = Vec::with_capacity(entries.len());
            for entry in &entries {
                let log_id = entry.log_id;
                if let Some(m) = entry.get_membership() {
                    membership = StoredMembership::new(Some(log_id), m.clone());
                }
                let required = match &entry.payload {
                    EntryPayload::Normal(cmd) => cmd.min_feature_level(),
                    EntryPayload::Blank | EntryPayload::Membership(_) => compat::MIN_FEATURE_LEVEL,
                };
                if required > self.feature_level {
                    return Err(storage_err(
                        ErrorSubject::Apply(log_id),
                        ErrorVerb::Write,
                        std::io::Error::other(format!(
                            "control command requires raft feature level {required}, but \
                             [raft].feature_level = {}; refusing to apply",
                            self.feature_level
                        )),
                    ));
                }
                let target = AppliedLog {
                    term: log_id.leader_id.term,
                    index: log_id.index,
                };
                let membership_json = serde_json::to_string(&membership)
                    .map_err(|e| storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e))?;
                let applied_json = serde_json::to_string(&log_id)
                    .map_err(|e| storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e))?;
                let payload = entry.payload.clone();
                let (response, has_effects, cleared_uid) = self
                    .store
                    .transaction(WriteOrigin::raft_apply(target), |txn| {
                        let payload = payload.clone();
                        let membership_json = membership_json.clone();
                        let applied_json = applied_json.clone();
                        Box::pin(async move {
                            if let Some(cur) = txn.meta().applied
                                && (cur.term, cur.index) >= (target.term, target.index)
                            {
                                return Ok((ControlResponse::Ok, false, None));
                            }
                            let (response, effects, uid) = match &payload {
                                EntryPayload::Normal(cmd) => {
                                    let (applied, uid) = apply::apply_command_txn(txn, cmd)
                                        .await
                                        .map_err(store_err)?;
                                    if required > activation_of(txn.meta()) {
                                        txn.meta_mut().feature_level = required as u16;
                                    }
                                    if required >= compat::FEATURE_LEVEL_4 {
                                        txn.set_marker(compat::META_RUNSTATE_ACTIVATED, "1");
                                    }
                                    if required >= compat::FEATURE_LEVEL_5 {
                                        txn.set_marker(compat::META_INVENTORY_ACTIVATED, "1");
                                    }
                                    (applied.response, !applied.effects.is_empty(), uid)
                                }
                                EntryPayload::Blank | EntryPayload::Membership(_) => {
                                    (ControlResponse::Ok, false, None)
                                }
                            };
                            // UserDelete: clear bucket owners, then the user row,
                            // in the same CAS (SQLite does this in its wrapper).
                            if let Some(uid) = uid {
                                bucket_clear_owner(txn, uid).await.map_err(store_err)?;
                                txn.user_delete(uid).await.map_err(store_err)?;
                            }
                            txn.set_membership(Some(membership_json));
                            txn.set_applied(Some(target));
                            txn.set_marker(META_APPLIED_LOG_ID, applied_json);
                            Ok((response, effects, uid))
                        })
                    })
                    .await
                    .map_err(|e| storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e))?;
                if has_effects && let Some(tx) = &self.install_notify {
                    let _ = tx.send(log_id.index);
                }
                if let Some(uid) = cleared_uid {
                    scrub_local_uploads(&self.store, |owner| owner == Some(uid))?;
                }
                self.activation.fetch_max(required, Ordering::Relaxed);
                responses.push(response);
            }
            Ok(responses)
        }
        .await;
        if result.is_err() {
            self.counters
                .apply_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        snapshot::install(self, meta, snapshot).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let slot = self.snapshot_slot();
        Ok(slot.as_ref().map(|(meta, bytes)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(bytes.clone())),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for ObjectStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        snapshot::build(self).await
    }
}
