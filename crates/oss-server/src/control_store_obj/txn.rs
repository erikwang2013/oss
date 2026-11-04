//! Object backend transaction: pinned root, overlay-aware reads, one root CAS
//! commit (design §2.1/§2.2). There is no begin/rollback: dropping the txn
//! discards the overlay.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use xl_store::control_state::blob::MAX_BLOB_BYTES;
use xl_store::control_state::node::{INLINE_MAX, LeafValue};
use xl_store::control_state::root::{AppliedLog, Mode, Root, RootMeta, TableRoot, encode_root};
use xl_store::control_state::tree::TableTree;
use xl_store::error::{Error, Result};

use super::change::{Overlay, StagedRow};
use super::origin::WriteOrigin;
use super::root_io::{CasOutcome, PinnedRoot, RootIo};
use super::table::{TableId, known_table_ids};

/// Bounded conflict retries (design §2.2 step 5).
pub const MAX_COMMIT_ATTEMPTS: usize = 8;

/// Cache publication callback, invoked once after a successful root CAS.
pub type CachePublish = Arc<dyn Fn(&CommitStamp) + Send + Sync>;

/// Result of a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitStamp {
    /// Committed root generation; unchanged for a no-op.
    pub generation: u64,
    /// False when no CAS was needed (empty/no-op transaction or idempotent replay).
    pub committed: bool,
    pub applied: Option<AppliedLog>,
    /// Table ids whose committed root changed.
    pub dirty_tables: Vec<u16>,
}

/// Read handle for a pinned root generation plus its commit gate.
pub struct ObjectControlStore {
    pub root: Arc<RootIo>,
    publisher: Option<CachePublish>,
}

impl ObjectControlStore {
    pub fn new(root: Arc<RootIo>) -> Self {
        Self {
            root,
            publisher: None,
        }
    }

    /// Install a cache-publication callback (called once per successful CAS).
    pub fn with_publisher(
        mut self,
        publish: impl Fn(&CommitStamp) + Send + Sync + 'static,
    ) -> Self {
        self.publisher = Some(Arc::new(publish));
        self
    }

    /// Bootstrap the empty root when the store has none; returns the pin.
    pub async fn bootstrap(&self, mode: Mode, created_at: i64) -> Result<PinnedRoot> {
        self.root.bootstrap(mode, created_at).await
    }

    /// Pin the current committed generation; fails closed when no root exists.
    pub async fn pin(&self) -> Result<PinnedRoot> {
        self.root
            .load()
            .await?
            .ok_or_else(|| Error::CorruptMeta("control root missing".into()))
    }

    /// Export/install gate (design §1.3): unknown tables or a newer required
    /// reader minor fail closed instead of silently dropping state.
    pub async fn check_compatible(&self) -> Result<()> {
        self.pin().await?.root.check_compatible(&known_table_ids())
    }

    /// Start a transaction at the current generation. The origin is mandatory.
    pub async fn begin(self: &Arc<Self>, origin: WriteOrigin) -> Result<ObjectControlTxn> {
        let pinned = self.pin().await?;
        let base_meta = pinned.root.meta.clone();
        let applied_target = origin.log_id();
        let mut txn = ObjectControlTxn {
            store: Arc::clone(self),
            origin,
            pinned,
            base_meta: base_meta.clone(),
            meta: base_meta,
            applied_target,
            meta_dirty: false,
            trees: BTreeMap::new(),
            overlay: BTreeMap::new(),
        };
        txn.restage();
        Ok(txn)
    }
}

/// A transaction over one pinned root generation.
pub struct ObjectControlTxn {
    store: Arc<ObjectControlStore>,
    origin: WriteOrigin,
    pinned: PinnedRoot,
    /// Root meta as seen at begin; used to replay only this txn's meta changes on retry.
    base_meta: RootMeta,
    meta: RootMeta,
    applied_target: Option<AppliedLog>,
    meta_dirty: bool,
    /// All known tables, rebuilt from `pinned` and the overlay on every attempt.
    trees: BTreeMap<u16, TableTree>,
    overlay: Overlay,
}

impl ObjectControlTxn {
    pub fn origin(&self) -> &WriteOrigin {
        &self.origin
    }

    pub fn meta(&self) -> &RootMeta {
        &self.meta
    }

    pub fn meta_mut(&mut self) -> &mut RootMeta {
        self.meta_dirty = true;
        &mut self.meta
    }

    pub fn alloc_uid(&mut self) -> Result<u64> {
        let id = self.meta.alloc_uid()?;
        self.meta_dirty = true;
        Ok(id)
    }

    pub fn alloc_job_id(&mut self) -> Result<u64> {
        let id = self.meta.alloc_job_id()?;
        self.meta_dirty = true;
        Ok(id)
    }

    pub fn alloc_run_id(&mut self) -> Result<u64> {
        let id = self.meta.alloc_run_id()?;
        self.meta_dirty = true;
        Ok(id)
    }

    pub fn alloc_site_seq(&mut self) -> Result<i64> {
        let id = self.meta.alloc_site_seq()?;
        self.meta_dirty = true;
        Ok(id)
    }

    pub fn set_marker(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.meta.markers.insert(key.into(), value.into());
        self.meta_dirty = true;
    }

    pub fn remove_marker(&mut self, key: &str) {
        self.meta.markers.remove(key);
        self.meta_dirty = true;
    }

    pub fn set_membership(&mut self, membership: Option<String>) {
        self.meta.membership_json = membership;
        self.meta_dirty = true;
    }

    /// Set the applied log position explicitly (snapshot install). The id is
    /// also used to recognize an idempotent replay after a conflict.
    pub fn set_applied(&mut self, applied: Option<AppliedLog>) {
        self.applied_target = applied;
        self.meta.applied = applied;
        self.meta_dirty = true;
    }

    /// Stage a row. Values above 16 KiB are written as chunks immediately (before
    /// any node that references them), so a later node flush sees an existing blob.
    pub async fn put(&mut self, table: TableId, key: &[u8], value: &[u8]) -> Result<()> {
        if value.len() > MAX_BLOB_BYTES {
            return Err(Error::InvalidArgument(format!(
                "control row is {} bytes, over the {MAX_BLOB_BYTES} byte cap",
                value.len()
            )));
        }
        let row = if value.len() > INLINE_MAX {
            StagedRow::PutBlob(self.store.root.put_blob(value).await?)
        } else {
            StagedRow::Put(value.to_vec())
        };
        self.stage(table, key.to_vec(), row);
        Ok(())
    }

    /// Stage a delete; returns whether the row was visible before the delete.
    pub async fn delete(&mut self, table: TableId, key: &[u8]) -> Result<bool> {
        let existed = self.get(table, key).await?.is_some();
        self.stage(table, key.to_vec(), StagedRow::Delete);
        Ok(existed)
    }

    /// Overlay-aware point read; blobs are reassembled and verified.
    pub async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.tree(table).get_leaf(&*self.store.root, key).await? {
            None | Some(LeafValue::Tombstone) => Ok(None),
            Some(LeafValue::Row(bytes)) => Ok(Some(bytes)),
            Some(LeafValue::Blob(blob)) => Ok(Some(self.store.root.get_blob(&blob).await?)),
        }
    }

    /// Overlay-aware ordered range scan (cascade enumeration).
    pub async fn scan(
        &self,
        table: TableId,
        range: Range<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        for (key, value) in self.tree(table).scan_leaf(&*self.store.root, range).await? {
            match value {
                LeafValue::Row(bytes) => out.push((key, bytes)),
                LeafValue::Blob(blob) => out.push((key, self.store.root.get_blob(&blob).await?)),
                LeafValue::Tombstone => {}
            }
        }
        Ok(out)
    }

    /// Commit the overlay with one root CAS (design §2.2). Returns the pinned
    /// generation unchanged when nothing changed.
    pub async fn commit(mut self) -> Result<CommitStamp> {
        let target = self.origin.log_id().or(self.applied_target);
        if let Some(target) = target {
            if applied_at_least(self.pinned.root.meta.applied, target) {
                // Already applied (replay): the entry's rows are in this generation.
                return Ok(self.noop_stamp());
            }
            if self.meta.applied != Some(target) {
                self.meta.applied = Some(target);
                self.meta_dirty = true;
            }
        }
        if self.overlay.is_empty() && !self.meta_dirty {
            return Ok(self.noop_stamp());
        }

        let mut attempts = 0;
        loop {
            attempts += 1;
            self.restage();
            let (new_root, dirty) = self.prepare().await?;
            if new_root == self.pinned.root {
                return Ok(self.noop_stamp());
            }
            let bytes = encode_root(&new_root)?;
            let fence = self
                .store
                .root
                .alloc_fence(self.pinned.version.saturating_add(1));
            match self
                .store
                .root
                .cas_root(self.pinned.version, fence, &bytes)
                .await
            {
                Ok(CasOutcome::Committed(generation)) => {
                    let stamp = CommitStamp {
                        generation,
                        committed: true,
                        applied: new_root.meta.applied,
                        dirty_tables: dirty,
                    };
                    if let Some(publish) = &self.store.publisher {
                        publish(&stamp);
                    }
                    return Ok(stamp);
                }
                Ok(CasOutcome::NotCommitted) => {
                    if attempts >= MAX_COMMIT_ATTEMPTS {
                        return Err(Error::Conflict(format!(
                            "control root commit contended after {attempts} attempts"
                        )));
                    }
                }
                Err(Error::Conflict(_)) => {
                    let Some(new_pinned) = self.store.root.load().await? else {
                        return Err(Error::CorruptMeta(
                            "control root vanished during commit".into(),
                        ));
                    };
                    if let Some(target) = self.applied_target
                        && applied_at_least(new_pinned.root.meta.applied, target)
                    {
                        // Another apply won with this txn's applied id.
                        return Ok(CommitStamp {
                            generation: new_pinned.version,
                            committed: false,
                            applied: new_pinned.root.meta.applied,
                            dirty_tables: Vec::new(),
                        });
                    }
                    if attempts >= MAX_COMMIT_ATTEMPTS {
                        return Err(Error::Conflict(format!(
                            "control root commit contended after {attempts} attempts"
                        )));
                    }
                    self.rebase_meta(&new_pinned.root.meta);
                    self.pinned = new_pinned;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn tree(&self, table: TableId) -> &TableTree {
        self.trees
            .get(&table.as_u16())
            .expect("every registry table has a tree")
    }

    fn stage(&mut self, table: TableId, key: Vec<u8>, row: StagedRow) {
        let tree = self
            .trees
            .get_mut(&table.as_u16())
            .expect("every registry table has a tree");
        match &row {
            StagedRow::Put(bytes) => tree.stage_put(&key, LeafValue::Row(bytes.clone())),
            StagedRow::PutBlob(blob) => tree.stage_put(&key, LeafValue::Blob(*blob)),
            StagedRow::Delete => tree.stage_delete(&key),
        }
        self.overlay
            .entry(table.as_u16())
            .or_default()
            .insert(key, row);
    }

    /// Rebuild every read tree from the current pin and replay the overlay.
    fn restage(&mut self) {
        let mut trees = BTreeMap::new();
        for table in TableId::ALL {
            let id = table.as_u16();
            let tree = match self.pinned.root.table(id) {
                Some(root) => TableTree::from_root(root.hash, root.rows, root.bytes),
                None => TableTree::new(),
            };
            trees.insert(id, tree);
        }
        for (id, rows) in &self.overlay {
            let Some(tree) = trees.get_mut(id) else {
                continue;
            };
            for (key, row) in rows {
                match row {
                    StagedRow::Put(bytes) => tree.stage_put(key, LeafValue::Row(bytes.clone())),
                    StagedRow::PutBlob(blob) => tree.stage_put(key, LeafValue::Blob(*blob)),
                    StagedRow::Delete => tree.stage_delete(key),
                }
            }
        }
        self.trees = trees;
    }

    /// Flush dirty tables bottom-up (the tree copies only changed paths) and
    /// serialize the resulting root image.
    async fn prepare(&mut self) -> Result<(Root, Vec<u16>)> {
        let mut root = self.pinned.root.clone();
        root.meta = self.meta.clone();
        let mut dirty = Vec::new();
        for (id, tree) in &mut self.trees {
            if tree.staged_len() == 0 {
                continue;
            }
            tree.flush(&*self.store.root).await?;
            let table = TableId::from_u16(*id).ok_or_else(|| {
                Error::CorruptMeta(format!("transaction staged unknown table {id}"))
            })?;
            match tree.root_hash() {
                Some(hash) => {
                    let table_root = TableRoot {
                        hash,
                        rows: tree.rows(),
                        bytes: tree.bytes(),
                        required_minor: table.required_minor(),
                    };
                    if root.table(*id) != Some(&table_root) {
                        dirty.push(*id);
                    }
                    root.set_table(*id, table_root);
                }
                None => {
                    if root.remove_table(*id).is_some() {
                        dirty.push(*id);
                    }
                }
            }
        }
        Ok((root, dirty))
    }

    /// On conflict, keep the new generation's metadata but replay this txn's
    /// changes; allocators merge monotonically so a retry never regresses them.
    fn rebase_meta(&mut self, new: &RootMeta) {
        let base = &self.base_meta;
        let mine = &self.meta;
        let mut merged = new.clone();
        merged.next_uid = merge_counter(merged.next_uid, base.next_uid, mine.next_uid);
        merged.next_job_id = merge_counter(merged.next_job_id, base.next_job_id, mine.next_job_id);
        merged.next_run_id = merge_counter(merged.next_run_id, base.next_run_id, mine.next_run_id);
        if mine.next_site_seq != base.next_site_seq {
            merged.next_site_seq = merged.next_site_seq.max(mine.next_site_seq);
        }
        if mine.applied != base.applied {
            merged.applied = match (merged.applied, mine.applied) {
                (Some(current), Some(mine)) => Some(if applied_at_least(Some(current), mine) {
                    current
                } else {
                    mine
                }),
                (None, mine) => mine,
                (current, None) => current,
            };
        }
        if mine.membership_json != base.membership_json {
            merged.membership_json = mine.membership_json.clone();
        }
        if mine.feature_level != base.feature_level {
            merged.feature_level = mine.feature_level;
        }
        if mine.mode != base.mode {
            merged.mode = mine.mode;
        }
        for key in base.markers.keys() {
            if !mine.markers.contains_key(key) {
                merged.markers.remove(key);
            }
        }
        for (key, value) in &mine.markers {
            if base.markers.get(key) != Some(value) {
                merged.markers.insert(key.clone(), value.clone());
            }
        }
        self.meta = merged;
    }

    fn noop_stamp(&self) -> CommitStamp {
        CommitStamp {
            generation: self.pinned.version,
            committed: false,
            applied: self.pinned.root.meta.applied,
            dirty_tables: Vec::new(),
        }
    }
}

fn merge_counter(current: u64, base: u64, mine: u64) -> u64 {
    if mine != base {
        current.max(mine)
    } else {
        current
    }
}

fn applied_at_least(current: Option<AppliedLog>, target: AppliedLog) -> bool {
    current.is_some_and(|c| (c.term, c.index) >= (target.term, target.index))
}
