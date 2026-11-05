//! Whole-table rebuild: merge the overlay over the committed rows, then build
//! leaves and internals bottom-up and write each node once. Snapshot install
//! and migrator import stage a whole table; path copying would rewrite the root
//! path per row. The root is published only after every node write succeeded.

use std::cmp::Ordering;

use futures::{StreamExt, TryStreamExt};

use super::update::{
    first_key, internal_node, leaf_node, split_internal, split_leaf, validate_mutation,
};
use super::{Mutation, NodeRef, TableTree};
use crate::control_state::io::{NodeIo, get_node, put_node};
use crate::control_state::node::{InternalEntry, LeafEntry, LeafValue, Node, NodeEntries};
use crate::error::{Error, Result};

/// Node writes in flight. Each carries two fsyncs on a single-drive store, so a
/// bounded window hides most of the latency without opening 100k descriptors.
const WRITE_WINDOW: usize = 16;

impl TableTree {
    pub(super) async fn flush_bulk(&mut self, io: &dyn NodeIo) -> Result<()> {
        let mut staged: Vec<(Vec<u8>, LeafValue)> = self
            .staged
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        staged.sort_by(|(a, _), (b, _)| self.cmp(a, b));

        let mut base = Vec::new();
        if let Some(root) = self.root {
            collect_all(io, &root.hash, &mut base).await?;
        }

        let merged = self.merge(&base, &staged)?;
        let rows = merged.len() as u64;
        let bytes = merged_bytes(&merged)?;

        let leaves = split_leaf(merged)?.into_iter().map(leaf_node).collect();
        let mut children = write_nodes(io, leaves).await?;
        let mut level = 1u16;
        while children.len() > 1 {
            let entries = std::mem::take(&mut children)
                .into_iter()
                .map(|(first_key, r)| InternalEntry {
                    first_key,
                    child: r.hash,
                })
                .collect();
            let nodes = split_internal(entries)?
                .into_iter()
                .map(|group| internal_node(level, group))
                .collect();
            children = write_nodes(io, nodes).await?;
            level = level
                .checked_add(1)
                .ok_or_else(|| Error::CorruptMeta("table tree too deep".into()))?;
        }

        self.root = children.into_iter().next().map(|(_, r)| r);
        self.rows = rows;
        self.bytes = bytes;
        self.staged.clear();
        Ok(())
    }

    /// Ordered union of committed rows and the overlay; the overlay wins on an
    /// equal key and a tombstone drops the row.
    fn merge(
        &self,
        base: &[(Vec<u8>, LeafValue)],
        staged: &[(Vec<u8>, LeafValue)],
    ) -> Result<Vec<LeafEntry>> {
        let mut merged = Vec::with_capacity(base.len() + staged.len());
        let (mut i, mut j) = (0usize, 0usize);
        while i < base.len() || j < staged.len() {
            let order = match (base.get(i), staged.get(j)) {
                (Some((a, _)), Some((b, _))) => Some(self.cmp(a, b)),
                _ => None,
            };
            let from_staged = match order {
                Some(Ordering::Less) => false,
                Some(Ordering::Equal) => {
                    i += 1;
                    true
                }
                Some(Ordering::Greater) => true,
                None => base.get(i).is_none(),
            };
            let (key, value) = if from_staged {
                let (key, value) = staged[j].clone();
                j += 1;
                validate_mutation(Mutation::Put(&value))?;
                (key, value)
            } else {
                let (key, value) = base[i].clone();
                i += 1;
                (key, value)
            };
            if value.is_tombstone() {
                continue;
            }
            merged.push(LeafEntry { key, value });
        }
        Ok(merged)
    }
}

/// Write one node level, at most [`WRITE_WINDOW`] puts in flight. The nodes are
/// independent create-once objects; only the final root reference publishes them.
async fn write_nodes(io: &dyn NodeIo, nodes: Vec<Node>) -> Result<Vec<(Vec<u8>, NodeRef)>> {
    futures::stream::iter(nodes)
        .map(|node| async move {
            let first_key = first_key(&node)?;
            let hash = put_node(io, &node).await?;
            Ok((first_key, NodeRef { hash }))
        })
        .buffered(WRITE_WINDOW)
        .try_collect()
        .await
}

async fn collect_all(
    io: &dyn NodeIo,
    hash: &[u8; 32],
    out: &mut Vec<(Vec<u8>, LeafValue)>,
) -> Result<()> {
    let node = get_node(io, hash).await?;
    match node.entries {
        NodeEntries::Leaf(entries) => {
            for entry in entries {
                if !entry.value.is_tombstone() {
                    out.push((entry.key, entry.value));
                }
            }
        }
        NodeEntries::Internal(entries) => {
            for entry in entries {
                Box::pin(collect_all(io, &entry.child, out)).await?;
            }
        }
    }
    Ok(())
}

fn merged_bytes(entries: &[LeafEntry]) -> Result<u64> {
    entries.iter().try_fold(0u64, |acc, entry| {
        acc.checked_add(entry.value.stored_len())
            .ok_or_else(|| Error::CorruptMeta("table byte counter overflow".into()))
    })
}
