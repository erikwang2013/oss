//! Copy-on-write B+tree for one control table (design D1/§1.3).
//!
//! Every mutation is staged in an in-memory overlay (`stage_put`/`stage_delete`)
//! and becomes visible to reads immediately. `flush` applies the overlay with
//! persistent path copying: only nodes on a changed key path are rewritten, and
//! nodes are content-addressed create-once objects. The tree root/row/byte
//! counters are updated only after the whole flush succeeds, so an interrupted
//! flush leaves the previous generation readable.
//!
//! Deletes compact to no entry at flush (reads return `None` either way); a
//! persisted tombstone from an older writer is dropped on the next rewrite.
//! Keys are compared through [`KeyComparator`]; the default is bytewise.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Range;

use super::io::{NodeIo, get_node};
use super::node::{InternalEntry, LeafEntry, LeafValue, NodeEntries};
use crate::error::{Error, Result};

mod update;

/// Typed per-table key order. Bytewise today; composite keys can plug in here.
pub trait KeyComparator: Send + Sync {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Bytewise;

impl KeyComparator for Bytewise {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeRef {
    pub hash: [u8; 32],
}

#[derive(Clone, Copy)]
enum Mutation<'a> {
    Put(&'a LeafValue),
    Delete,
}

pub struct TableTree {
    root: Option<NodeRef>,
    rows: u64,
    bytes: u64,
    staged: BTreeMap<Vec<u8>, LeafValue>,
    cmp: Box<dyn KeyComparator>,
}

impl Default for TableTree {
    fn default() -> Self {
        Self::new()
    }
}

impl TableTree {
    pub fn new() -> Self {
        Self::with_comparator(Box::new(Bytewise))
    }

    pub fn with_comparator(cmp: Box<dyn KeyComparator>) -> Self {
        Self {
            root: None,
            rows: 0,
            bytes: 0,
            staged: BTreeMap::new(),
            cmp,
        }
    }

    /// Reopen a table from its committed root (`TableRoot {hash, rows, bytes}`).
    pub fn from_root(hash: [u8; 32], rows: u64, bytes: u64) -> Self {
        let mut tree = Self::new();
        tree.root = Some(NodeRef { hash });
        tree.rows = rows;
        tree.bytes = bytes;
        tree
    }

    pub fn root(&self) -> Option<NodeRef> {
        self.root
    }

    pub fn root_hash(&self) -> Option<[u8; 32]> {
        self.root.map(|r| r.hash)
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Logical stored bytes (row/blob lengths), excluding keys and tombstones.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn staged_len(&self) -> usize {
        self.staged.len()
    }

    pub fn stage_put(&mut self, key: &[u8], value: LeafValue) {
        self.staged.insert(key.to_vec(), value);
    }

    pub fn stage_delete(&mut self, key: &[u8]) {
        self.staged.insert(key.to_vec(), LeafValue::Tombstone);
    }

    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.cmp.compare(a, b)
    }

    fn leaf_index(&self, entries: &[LeafEntry], key: &[u8]) -> std::result::Result<usize, usize> {
        entries.binary_search_by(|e| self.cmp(&e.key, key))
    }

    fn child_index(&self, entries: &[InternalEntry], key: &[u8]) -> Result<usize> {
        if entries.is_empty() {
            return Err(Error::CorruptMeta("empty internal node".into()));
        }
        let pos = entries.partition_point(|e| self.cmp(&e.first_key, key) != Ordering::Greater);
        Ok(pos.saturating_sub(1))
    }

    fn in_range(&self, key: &[u8], start: &[u8], end: &[u8]) -> bool {
        self.cmp(key, start) != Ordering::Less && self.cmp(key, end) == Ordering::Less
    }

    async fn lookup(&self, io: &dyn NodeIo, key: &[u8]) -> Result<Option<LeafValue>> {
        let Some(mut node_ref) = self.root else {
            return Ok(None);
        };
        loop {
            let node = get_node(io, &node_ref.hash).await?;
            match node.entries {
                NodeEntries::Leaf(entries) => {
                    return Ok(self
                        .leaf_index(&entries, key)
                        .ok()
                        .map(|i| entries[i].value.clone()));
                }
                NodeEntries::Internal(entries) => {
                    node_ref = NodeRef {
                        hash: entries[self.child_index(&entries, key)?].child,
                    };
                }
            }
        }
    }

    /// Full leaf value: distinguishes inline rows, blobs and (never returned) tombstones.
    pub async fn get_leaf(&self, io: &dyn NodeIo, key: &[u8]) -> Result<Option<LeafValue>> {
        if let Some(value) = self.staged.get(key) {
            return Ok((!value.is_tombstone()).then(|| value.clone()));
        }
        Ok(self.lookup(io, key).await?.filter(|v| !v.is_tombstone()))
    }

    /// Inline row read. Blob-backed leaves are a caller error here: use
    /// [`TableTree::get_leaf`] and reassemble the blob, never silently miss it.
    pub async fn get(&self, io: &dyn NodeIo, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.get_leaf(io, key).await? {
            Some(LeafValue::Row(bytes)) => Ok(Some(bytes)),
            Some(LeafValue::Blob(_)) => {
                Err(Error::Unsupported("blob leaf value; use get_leaf".into()))
            }
            Some(LeafValue::Tombstone) | None => Ok(None),
        }
    }

    async fn collect(
        &self,
        io: &dyn NodeIo,
        hash: &[u8; 32],
        start: &[u8],
        end: &[u8],
        out: &mut Vec<(Vec<u8>, LeafValue)>,
    ) -> Result<()> {
        let node = get_node(io, hash).await?;
        match node.entries {
            NodeEntries::Leaf(entries) => {
                for entry in entries {
                    if !entry.value.is_tombstone() && self.in_range(&entry.key, start, end) {
                        out.push((entry.key, entry.value));
                    }
                }
            }
            NodeEntries::Internal(entries) => {
                for (i, entry) in entries.iter().enumerate() {
                    let next = entries.get(i + 1).map(|e| e.first_key.as_slice());
                    let overlaps = self.cmp(&entry.first_key, end) == Ordering::Less
                        && next.is_none_or(|n| self.cmp(n, start) == Ordering::Greater);
                    if overlaps {
                        Box::pin(self.collect(io, &entry.child, start, end, out)).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Ordered range scan over full leaf values (including blobs, excluding tombstones).
    pub async fn scan_leaf(
        &self,
        io: &dyn NodeIo,
        range: Range<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, LeafValue)>> {
        let (start, end) = (range.start, range.end);
        let mut out = Vec::new();
        if self.cmp(start, end) != Ordering::Less {
            return Ok(out);
        }
        if let Some(root) = self.root {
            self.collect(io, &root.hash, start, end, &mut out).await?;
        }
        for (key, value) in &self.staged {
            if self.in_range(key, start, end) {
                match out.binary_search_by(|(k, _)| self.cmp(k, key)) {
                    Ok(i) => out[i].1 = value.clone(),
                    Err(i) => out.insert(i, (key.clone(), value.clone())),
                }
            }
        }
        out.retain(|(_, v)| !v.is_tombstone());
        Ok(out)
    }

    /// Ordered inline-row range scan. A blob in range fails closed; use `scan_leaf`.
    pub async fn scan(
        &self,
        io: &dyn NodeIo,
        range: Range<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_leaf(io, range)
            .await?
            .into_iter()
            .map(|(k, v)| match v {
                LeafValue::Row(bytes) => Ok((k, bytes)),
                LeafValue::Blob(_) => Err(Error::Unsupported(
                    "blob leaf value in range scan; use scan_leaf".into(),
                )),
                LeafValue::Tombstone => unreachable!("scan filters tombstones"),
            })
            .collect()
    }

    /// Number of node levels (0 = empty tree).
    pub async fn depth(&self, io: &dyn NodeIo) -> Result<u16> {
        let Some(mut node_ref) = self.root else {
            return Ok(0);
        };
        let mut depth = 1;
        loop {
            let node = get_node(io, &node_ref.hash).await?;
            match node.entries {
                NodeEntries::Leaf(_) => return Ok(depth),
                NodeEntries::Internal(entries) => {
                    if entries.is_empty() {
                        return Err(Error::CorruptMeta("empty internal node".into()));
                    }
                    node_ref = NodeRef {
                        hash: entries[0].child,
                    };
                    depth += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
