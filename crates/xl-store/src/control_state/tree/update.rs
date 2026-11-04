//! Write path: persistent path copying, splits and underfull merges.

use super::{Mutation, NodeRef, TableTree};
use crate::control_state::io::{NodeIo, get_node, put_node};
use crate::control_state::node::{
    INLINE_MAX, InternalEntry, LeafEntry, LeafValue, NODE_MIN_BYTES, NODE_SPLIT_BYTES,
    NODE_TARGET_BYTES, Node, NodeEntries, NodeKind, encoded_size,
};
use crate::error::{Error, Result};

impl TableTree {
    /// Apply the overlay with path copying, then publish the new root.
    pub async fn flush(&mut self, io: &dyn NodeIo) -> Result<()> {
        if self.staged.is_empty() {
            return Ok(());
        }
        let mut staged = std::mem::take(&mut self.staged);
        let mut applied: Vec<(Vec<u8>, LeafValue)> = Vec::with_capacity(staged.len());
        let mut root = self.root;
        let mut rows = self.rows;
        let mut bytes = self.bytes;

        while let Some((key, value)) = staged.pop_first() {
            let mutation = match value {
                LeafValue::Tombstone => Mutation::Delete,
                ref v => Mutation::Put(v),
            };
            let is_put = !matches!(mutation, Mutation::Delete);
            let outcome = self.update_top(io, root, &key, mutation).await;
            match outcome {
                Ok((new_root, old)) => match apply_counters(rows, bytes, &value, old, is_put) {
                    Ok((r, b)) => {
                        root = new_root;
                        rows = r;
                        bytes = b;
                        applied.push((key, value));
                    }
                    Err(e) => {
                        // The unpublished root is unchanged: every mutation in
                        // this batch must remain staged, including ones applied.
                        staged.extend(applied);
                        staged.insert(key, value);
                        self.staged = staged;
                        return Err(e);
                    }
                },
                Err(e) => {
                    staged.extend(applied);
                    staged.insert(key, value);
                    self.staged = staged;
                    return Err(e);
                }
            }
        }
        self.root = root;
        self.rows = rows;
        self.bytes = bytes;
        Ok(())
    }

    async fn update_top(
        &self,
        io: &dyn NodeIo,
        root: Option<NodeRef>,
        key: &[u8],
        mutation: Mutation<'_>,
    ) -> Result<(Option<NodeRef>, Option<LeafValue>)> {
        validate_mutation(mutation)?;
        let Some(root) = root else {
            if matches!(mutation, Mutation::Delete) {
                return Ok((None, None));
            }
            let Mutation::Put(value) = mutation else {
                unreachable!()
            };
            let node = leaf_node(vec![LeafEntry {
                key: key.to_vec(),
                value: value.clone(),
            }]);
            let hash = put_node(io, &node).await?;
            return Ok((Some(NodeRef { hash }), None));
        };

        let root_node = get_node(io, &root.hash).await?;
        let root_level = root_node.level;
        let (nodes, old) = self.update_node(io, root_node, key, mutation).await?;
        let new_root = match nodes.len() {
            0 => None,
            1 => {
                let hash = put_node(io, &nodes[0]).await?;
                Some(NodeRef { hash })
            }
            _ => {
                let refs = write_node_groups(io, nodes).await?;
                let entries = refs
                    .into_iter()
                    .map(|(first_key, r)| InternalEntry {
                        first_key,
                        child: r.hash,
                    })
                    .collect();
                let hash = put_node(io, &internal_node(root_level + 1, entries)).await?;
                Some(NodeRef { hash })
            }
        };
        Ok((self.collapse_root(io, new_root).await?, old))
    }

    /// A root internal node with a single child is collapsed (reads only).
    async fn collapse_root(
        &self,
        io: &dyn NodeIo,
        mut root: Option<NodeRef>,
    ) -> Result<Option<NodeRef>> {
        loop {
            let Some(node_ref) = root else {
                return Ok(None);
            };
            let node = get_node(io, &node_ref.hash).await?;
            match node.entries {
                NodeEntries::Internal(entries) if entries.len() == 1 => {
                    root = Some(NodeRef {
                        hash: entries[0].child,
                    });
                }
                _ => return Ok(root),
            }
        }
    }

    /// Update one fetched node and return its unwritten replacement nodes
    /// (1+ for put splits, 0/1 for deletes). Children are written here.
    async fn update_node(
        &self,
        io: &dyn NodeIo,
        node: Node,
        key: &[u8],
        mutation: Mutation<'_>,
    ) -> Result<(Vec<Node>, Option<LeafValue>)> {
        let level = node.level;
        match node.entries {
            NodeEntries::Leaf(mut entries) => {
                entries.retain(|e| !e.value.is_tombstone());
                let old = match mutation {
                    Mutation::Put(value) => match self.leaf_index(&entries, key) {
                        Ok(i) => Some(std::mem::replace(&mut entries[i].value, value.clone())),
                        Err(i) => {
                            entries.insert(
                                i,
                                LeafEntry {
                                    key: key.to_vec(),
                                    value: value.clone(),
                                },
                            );
                            None
                        }
                    },
                    Mutation::Delete => self
                        .leaf_index(&entries, key)
                        .ok()
                        .map(|i| entries.remove(i).value),
                };
                let old = old.filter(|v| !v.is_tombstone());
                if entries.is_empty() {
                    return Ok((Vec::new(), old));
                }
                let groups = match mutation {
                    Mutation::Put(_) => split_leaf(entries)?,
                    Mutation::Delete => vec![entries],
                };
                Ok((groups.into_iter().map(leaf_node).collect(), old))
            }
            NodeEntries::Internal(mut entries) => {
                let idx = self.child_index(&entries, key)?;
                let child = get_node(io, &entries[idx].child).await?;
                let (mut child_nodes, old) =
                    Box::pin(self.update_node(io, child, key, mutation)).await?;
                if child_nodes
                    .iter()
                    .any(|n| n.level + 1 != level || (n.level == 0) != (n.kind == NodeKind::Leaf))
                {
                    return Err(Error::CorruptMeta(
                        "child node level/kind does not fit parent".into(),
                    ));
                }

                if child_nodes.is_empty() {
                    entries.remove(idx);
                } else if matches!(mutation, Mutation::Delete)
                    && child_nodes.len() == 1
                    && entries.len() > 1
                    && encoded_size(&child_nodes[0])? < NODE_MIN_BYTES
                {
                    self.merge_underfull(io, &mut entries, idx, child_nodes.pop().unwrap())
                        .await?;
                } else {
                    let refs = write_node_groups(io, child_nodes).await?;
                    entries[idx].first_key = refs[0].0.clone();
                    entries[idx].child = refs[0].1.hash;
                    for (n, (first_key, r)) in refs.into_iter().skip(1).enumerate() {
                        entries.insert(
                            idx + 1 + n,
                            InternalEntry {
                                first_key,
                                child: r.hash,
                            },
                        );
                    }
                }

                if entries.is_empty() {
                    return Ok((Vec::new(), old));
                }
                let groups = split_internal(entries)?;
                Ok((
                    groups
                        .into_iter()
                        .map(|g| internal_node(level, g))
                        .collect(),
                    old,
                ))
            }
        }
    }

    /// Merge the underfull child at `idx` with an adjacent sibling, then write
    /// the resulting 1-2 nodes and splice their refs into the parent entries.
    async fn merge_underfull(
        &self,
        io: &dyn NodeIo,
        entries: &mut Vec<InternalEntry>,
        idx: usize,
        child: Node,
    ) -> Result<()> {
        let (li, ri) = if idx > 0 {
            (idx - 1, idx)
        } else {
            (idx, idx + 1)
        };
        let sibling_hash = if idx > 0 {
            entries[idx - 1].child
        } else {
            entries[idx + 1].child
        };
        let sibling = get_node(io, &sibling_hash).await?;
        let (left, right) = if idx == li {
            (child, sibling)
        } else {
            (sibling, child)
        };
        if left.kind != right.kind || left.level != right.level {
            return Err(Error::CorruptMeta("sibling level/kind mismatch".into()));
        }
        let merged = merge_nodes(left, right)?;
        let refs = write_node_groups(io, merged).await?;
        entries.remove(ri);
        entries.remove(li);
        for (n, (first_key, r)) in refs.into_iter().enumerate() {
            entries.insert(
                li + n,
                InternalEntry {
                    first_key,
                    child: r.hash,
                },
            );
        }
        Ok(())
    }
}

fn validate_mutation(mutation: Mutation<'_>) -> Result<()> {
    if let Mutation::Put(value) = mutation {
        match value {
            LeafValue::Row(bytes) if bytes.len() > INLINE_MAX => {
                return Err(Error::InvalidArgument(format!(
                    "inline row {} exceeds {INLINE_MAX}",
                    bytes.len()
                )));
            }
            LeafValue::Blob(blob) => blob.validate()?,
            LeafValue::Row(_) | LeafValue::Tombstone => {}
        }
    }
    Ok(())
}

fn apply_counters(
    rows: u64,
    bytes: u64,
    value: &LeafValue,
    old: Option<LeafValue>,
    is_put: bool,
) -> Result<(u64, u64)> {
    let checked_add = |v: u64, d: u64| {
        v.checked_add(d)
            .ok_or_else(|| Error::CorruptMeta("table byte counter overflow".into()))
    };
    let checked_sub = |v: u64, d: u64, what: &str| {
        v.checked_sub(d)
            .ok_or_else(|| Error::CorruptMeta(format!("table {what} counter underflow")))
    };
    match (is_put, old) {
        (true, Some(old)) => Ok((
            rows,
            checked_add(
                checked_sub(bytes, old.stored_len(), "bytes")?,
                value.stored_len(),
            )?,
        )),
        (true, None) => Ok((
            checked_add(rows, 1)?,
            checked_add(bytes, value.stored_len())?,
        )),
        (false, Some(old)) => Ok((
            checked_sub(rows, 1, "rows")?,
            checked_sub(bytes, old.stored_len(), "bytes")?,
        )),
        (false, None) => Ok((rows, bytes)),
    }
}

fn leaf_node(entries: Vec<LeafEntry>) -> Node {
    Node {
        kind: NodeKind::Leaf,
        level: 0,
        entries: NodeEntries::Leaf(entries),
    }
}

fn internal_node(level: u16, entries: Vec<InternalEntry>) -> Node {
    Node {
        kind: NodeKind::Internal,
        level,
        entries: NodeEntries::Internal(entries),
    }
}

fn first_key(node: &Node) -> Result<Vec<u8>> {
    match &node.entries {
        NodeEntries::Leaf(entries) => entries
            .first()
            .map(|e| e.key.clone())
            .ok_or_else(|| Error::CorruptMeta("empty leaf node".into())),
        NodeEntries::Internal(entries) => entries
            .first()
            .map(|e| e.first_key.clone())
            .ok_or_else(|| Error::CorruptMeta("empty internal node".into())),
    }
}

async fn write_node_groups(io: &dyn NodeIo, nodes: Vec<Node>) -> Result<Vec<(Vec<u8>, NodeRef)>> {
    let mut refs = Vec::with_capacity(nodes.len());
    for node in nodes {
        let first_key = first_key(&node)?;
        let hash = put_node(io, &node).await?;
        refs.push((first_key, NodeRef { hash }));
    }
    Ok(refs)
}

fn merge_nodes(left: Node, right: Node) -> Result<Vec<Node>> {
    if left.kind != right.kind || left.level != right.level {
        return Err(Error::CorruptMeta(
            "cannot merge nodes of different kind/level".into(),
        ));
    }
    let level = left.level;
    match (left.entries, right.entries) {
        (NodeEntries::Leaf(mut l), NodeEntries::Leaf(r)) => {
            l.extend(r);
            l.retain(|e| !e.value.is_tombstone());
            if l.is_empty() {
                return Ok(Vec::new());
            }
            Ok(split_leaf(l)?.into_iter().map(leaf_node).collect())
        }
        (NodeEntries::Internal(mut l), NodeEntries::Internal(r)) => {
            l.extend(r);
            Ok(split_internal(l)?
                .into_iter()
                .map(|g| internal_node(level, g))
                .collect())
        }
        _ => unreachable!("kind checked above"),
    }
}

fn entry_size<T: serde::Serialize>(entry: &T) -> Result<usize> {
    Ok(rmp_serde::to_vec_named(entry)
        .map_err(|e| Error::Encode(format!("rmp entry: {e}")))?
        .len()
        + 1)
}

/// Greedy target split, then hard-cap repair. Order is preserved.
fn split_leaf(entries: Vec<LeafEntry>) -> Result<Vec<Vec<LeafEntry>>> {
    let mut groups: Vec<Vec<LeafEntry>> = Vec::new();
    let mut cur = Vec::new();
    let mut acc = 0;
    for entry in entries {
        let size = entry_size(&entry)?;
        if !cur.is_empty() && acc + size > NODE_TARGET_BYTES {
            groups.push(std::mem::take(&mut cur));
            acc = 0;
        }
        acc += size;
        cur.push(entry);
    }
    if !cur.is_empty() {
        groups.push(cur);
    }
    let mut out = Vec::new();
    let mut stack: Vec<Vec<LeafEntry>> = groups.into_iter().rev().collect();
    while let Some(group) = stack.pop() {
        if group.len() > 1 && encoded_size(&leaf_node(group.clone()))? > NODE_SPLIT_BYTES {
            let mid = group.len() / 2;
            stack.push(group[mid..].to_vec());
            stack.push(group[..mid].to_vec());
        } else {
            out.push(group);
        }
    }
    Ok(out)
}

fn split_internal(entries: Vec<InternalEntry>) -> Result<Vec<Vec<InternalEntry>>> {
    let mut groups: Vec<Vec<InternalEntry>> = Vec::new();
    let mut cur = Vec::new();
    let mut acc = 0;
    for entry in entries {
        let size = entry_size(&entry)?;
        if !cur.is_empty() && acc + size > NODE_TARGET_BYTES {
            groups.push(std::mem::take(&mut cur));
            acc = 0;
        }
        acc += size;
        cur.push(entry);
    }
    if !cur.is_empty() {
        groups.push(cur);
    }
    let mut out = Vec::new();
    let mut stack: Vec<Vec<InternalEntry>> = groups.into_iter().rev().collect();
    while let Some(group) = stack.pop() {
        if group.len() > 1 && encoded_size(&internal_node(1, group.clone()))? > NODE_SPLIT_BYTES {
            let mid = group.len() / 2;
            stack.push(group[mid..].to_vec());
            stack.push(group[..mid].to_vec());
        } else {
            out.push(group);
        }
    }
    Ok(out)
}
