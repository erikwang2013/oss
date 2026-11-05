//! Metadata-only witness state machine (design D9).
//!
//! A witness has no data dir and never serves control reads, so it stores only
//! identity + applied LogId + membership in `<db_path>.d/witness/state` (atomic
//! tmp + fsync + rename). Snapshots reuse [`FileSnapshotStore`]; install accepts
//! a voter's opaque snapshot bytes and advances the metadata from `SnapshotMeta`
//! (a later promotion to voter can replay that snapshot through the object SM).

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::entry::RaftPayload;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, Snapshot,
    SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use super::file_log::raft_dir;
use super::file_meta::sync_dir;
use super::file_snapshot::FileSnapshotStore;
use super::identity;
use super::metrics::Metrics;
use super::store::storage_err;
use super::{ControlResponse, RaftSettings, TypeConfig, compat};

const STATE_DIR: &str = "witness";
const STATE_FILE: &str = "state";
const STATE_FORMAT: u32 = 1;

type AppliedState = (Option<LogId<u64>>, StoredMembership<u64, BasicNode>);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WitnessFile {
    format: u32,
    identity: BTreeMap<String, String>,
    applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
}

impl Default for WitnessFile {
    fn default() -> Self {
        Self {
            format: STATE_FORMAT,
            identity: BTreeMap::new(),
            applied: None,
            membership: StoredMembership::default(),
        }
    }
}

/// Snapshot payload built by a witness (metadata only; install ignores payloads).
#[derive(Serialize, Deserialize)]
struct WitnessSnapshot {
    format: u32,
    applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
}

#[derive(Clone)]
pub struct WitnessStateMachine {
    path: PathBuf,
    snapshots: Arc<FileSnapshotStore>,
    state: Arc<Mutex<WitnessFile>>,
    max_snapshot_bytes: u64,
    feature_level: u8,
    counters: Arc<Metrics>,
}

impl WitnessStateMachine {
    /// Seed/verify identity, adopt an existing snapshot's applied position, and
    /// refuse an already-initialized witness whose log and membership are gone.
    pub async fn open(db_path: &Path, settings: &RaftSettings) -> Result<Self, String> {
        let dir = raft_dir(db_path).join(STATE_DIR);
        fs::create_dir_all(&dir).map_err(|e| format!("创建 {} 失败: {e}", dir.display()))?;
        let path = dir.join(STATE_FILE);
        let mut state = match fs::read(&path) {
            Ok(bytes) => {
                let state: WitnessFile = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("解析 {} 失败: {e}", path.display()))?;
                if state.format != STATE_FORMAT {
                    return Err(format!(
                        "{} 格式 {} 不受支持（期望 {STATE_FORMAT}）",
                        path.display(),
                        state.format
                    ));
                }
                state
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => WitnessFile::default(),
            Err(e) => return Err(format!("读取 {} 失败: {e}", path.display())),
        };
        seed(
            &mut state,
            identity::META_NODE_ID,
            &settings.node_id.to_string(),
        )?;
        seed(&mut state, identity::META_ROLE, "witness")?;
        seed(&mut state, identity::META_BOOTSTRAP, "0")?;
        seed(
            &mut state,
            identity::META_ADVERTISE,
            settings.advertise_endpoint.trim_end_matches('/'),
        )?;
        if !state.identity.contains_key(identity::META_INITIALIZED) {
            state
                .identity
                .insert(identity::META_INITIALIZED.into(), "0".into());
        }
        let snapshots = Arc::new(
            FileSnapshotStore::open(db_path).map_err(|e| format!("打开快照目录失败: {e}"))?,
        );
        // A crash between snapshot write and state persist must not leave applied
        // behind the snapshot the log was purged to.
        if let Some(meta) = snapshots
            .latest_meta()
            .await
            .map_err(|e| format!("读取快照清单失败: {e}"))?
            && state.applied < meta.last_log_id
        {
            state.applied = meta.last_log_id;
            state.membership = meta.last_membership;
        }
        persist(&path, &state)?;
        Ok(Self {
            path,
            snapshots,
            state: Arc::new(Mutex::new(state)),
            max_snapshot_bytes: settings.snapshot_max_bytes,
            feature_level: settings.feature_level,
            counters: Arc::new(Metrics::default()),
        })
    }

    pub fn counters(&self) -> &Arc<Metrics> {
        &self.counters
    }

    pub fn activation_level(&self) -> u8 {
        compat::MIN_FEATURE_LEVEL
    }

    pub fn applied(&self) -> Result<AppliedState, StorageError<u64>> {
        let state = self.lock();
        Ok((state.applied, state.membership.clone()))
    }

    /// `raft_initialized=1` + no log + no voter = state lost (same guard as SQLite).
    pub fn verify_not_lost(&self, has_logs: bool) -> Result<(), String> {
        let state = self.lock();
        let initialized = state
            .identity
            .get(identity::META_INITIALIZED)
            .is_some_and(|v| v == "1");
        if initialized && !has_logs && state.membership.voter_ids().next().is_none() {
            return Err(
                "raft_initialized=1 但 witness 文件无日志且成员为空：控制面状态已丢失；\
                 使用 raft-force-recover 重建（见 runbook）"
                    .into(),
            );
        }
        Ok(())
    }

    /// 0 → 1 only after a real membership appears (same rule as the SQLite witness).
    pub fn record_initialized(&self, initialized: bool) -> Result<(), String> {
        let mut state = self.lock();
        let next = match state
            .identity
            .get(identity::META_INITIALIZED)
            .map(String::as_str)
        {
            None => {
                if initialized {
                    "1"
                } else {
                    "0"
                }
            }
            Some("0") if initialized => "1",
            _ => return Ok(()),
        };
        state
            .identity
            .insert(identity::META_INITIALIZED.into(), next.into());
        persist(&self.path, &state)
    }

    pub async fn snapshot_bytes(&self) -> u64 {
        self.snapshots.latest_bytes().await
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WitnessFile> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn seed(state: &mut WitnessFile, key: &str, value: &str) -> Result<(), String> {
    match state.identity.get(key) {
        Some(old) if old != value => Err(format!(
            "Raft 身份 {key} 已持久化为 {old:?}，配置为 {value:?}：节点身份不可变更\
             （丢失/迁移见 raft-force-recover runbook）"
        )),
        Some(_) => Ok(()),
        None => {
            state.identity.insert(key.to_string(), value.to_string());
            Ok(())
        }
    }
}

fn persist(path: &Path, state: &WitnessFile) -> Result<(), String> {
    let bytes = serde_json::to_vec(state).map_err(|e| format!("编码 witness 状态失败: {e}"))?;
    let tmp = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .map_err(|e| format!("写 {} 失败: {e}", tmp.display()))?;
        file.write_all(&bytes)
            .map_err(|e| format!("写 {} 失败: {e}", tmp.display()))?;
        file.sync_all()
            .map_err(|e| format!("fsync {} 失败: {e}", tmp.display()))?;
    }
    fs::rename(&tmp, path).map_err(|e| format!("rename {} 失败: {e}", path.display()))?;
    if let Some(dir) = path.parent() {
        sync_dir(dir).map_err(|e| format!("fsync {} 失败: {e}", dir.display()))?;
    }
    Ok(())
}

impl RaftStateMachine<TypeConfig> for WitnessStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        self.applied()
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ControlResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let result: Result<_, StorageError<u64>> = async {
            let entries: Vec<_> = entries.into_iter().collect();
            let mut state = self.lock();
            let mut responses = Vec::with_capacity(entries.len());
            for entry in &entries {
                let log_id = entry.log_id;
                let required = match &entry.payload {
                    EntryPayload::Normal(cmd) => cmd.min_feature_level(),
                    EntryPayload::Blank | EntryPayload::Membership(_) => compat::MIN_FEATURE_LEVEL,
                };
                if required > self.feature_level {
                    return Err(storage_err(
                        ErrorSubject::Apply(log_id),
                        ErrorVerb::Write,
                        io::Error::other(format!(
                            "control command requires raft feature level {required}, but \
                             [raft].feature_level = {}; refusing to apply",
                            self.feature_level
                        )),
                    ));
                }
                state.applied = Some(log_id);
                if let Some(m) = entry.get_membership() {
                    state.membership = StoredMembership::new(Some(log_id), m.clone());
                }
                persist(&self.path, &state).map_err(|e| {
                    storage_err(
                        ErrorSubject::Apply(log_id),
                        ErrorVerb::Write,
                        io::Error::other(e),
                    )
                })?;
                responses.push(ControlResponse::Ok);
            }
            Ok(responses)
        }
        .await;
        if result.is_err() {
            self.counters
                .apply_errors_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            path: self.path.clone(),
            snapshots: Arc::clone(&self.snapshots),
            state: Arc::clone(&self.state),
            max_snapshot_bytes: self.max_snapshot_bytes,
            feature_level: self.feature_level,
            counters: Arc::clone(&self.counters),
        }
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
        let bytes = snapshot.get_ref().clone();
        if bytes.len() as u64 > self.max_snapshot_bytes {
            return Err(storage_err(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                io::Error::other(format!(
                    "snapshot {} bytes exceeds limit {} (raft.snapshot_max_bytes)",
                    bytes.len(),
                    self.max_snapshot_bytes
                )),
            ));
        }
        {
            let mut state = self.lock();
            state.applied = meta.last_log_id;
            state.membership = meta.last_membership.clone();
            persist(&self.path, &state).map_err(|e| {
                storage_err(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Write,
                    io::Error::other(e),
                )
            })?;
        }
        // A voter's payload is opaque here; keep it so promotion can replay it.
        self.snapshots.write(meta, &bytes).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        self.snapshots.latest().await
    }
}

impl RaftSnapshotBuilder<TypeConfig> for WitnessStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let (applied, membership) = {
            let state = self.lock();
            (state.applied, state.membership.clone())
        };
        let bytes = serde_json::to_vec(&WitnessSnapshot {
            format: STATE_FORMAT,
            applied,
            membership: membership.clone(),
        })
        .map_err(|e| {
            storage_err(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                io::Error::other(e),
            )
        })?;
        let snapshot_id = format!(
            "w{}-{}",
            applied.map(|l| l.index).unwrap_or(0),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        );
        let meta = SnapshotMeta {
            last_log_id: applied,
            last_membership: membership,
            snapshot_id,
        };
        self.snapshots.write(&meta, &bytes).await?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}
