//! B+tree node codec (design §1.3). Leaves hold `key -> row | BlobRef | tombstone`;
//! internal nodes hold `first_key -> child hash`. Values above 16 KiB must be blobs,
//! and no encoded node may exceed 64 KiB. The tree splits at 64 KiB targeting 32 KiB
//! and merges below 8 KiB; see [`super::tree`].

use serde::{Deserialize, Serialize};

use super::blob::BlobRef;
use super::format::{
    self, HEADER_LEN, KNOWN_MAJOR, KNOWN_MINOR, ObjectHeader, ObjectKind, TRAILER_LEN,
};
use crate::error::{Error, Result};

/// Largest row stored inline in a leaf (design §1.3). Larger values use a blob.
pub const INLINE_MAX: usize = 16 * 1024;
pub const NODE_TARGET_BYTES: usize = 32 * 1024;
/// Nodes split above this bound and must never be written above it.
pub const NODE_SPLIT_BYTES: usize = 64 * 1024;
pub const NODE_MIN_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Leaf,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeafValue {
    Row(Vec<u8>),
    Blob(BlobRef),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeafEntry {
    pub key: Vec<u8>,
    pub value: LeafValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalEntry {
    /// Smallest key in the child subtree (separator for ordering).
    pub first_key: Vec<u8>,
    pub child: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeEntries {
    Leaf(Vec<LeafEntry>),
    Internal(Vec<InternalEntry>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub kind: NodeKind,
    pub level: u16,
    pub entries: NodeEntries,
}

impl LeafValue {
    /// Logical stored size used by `TableRoot.bytes` (tombstones cost zero).
    pub fn stored_len(&self) -> u64 {
        match self {
            Self::Row(bytes) => bytes.len() as u64,
            Self::Blob(blob) => blob.len,
            Self::Tombstone => 0,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone)
    }
}

fn validate(node: &Node) -> Result<()> {
    let empty = match &node.entries {
        NodeEntries::Leaf(entries) => entries.is_empty(),
        NodeEntries::Internal(entries) => entries.is_empty(),
    };
    if empty {
        return Err(Error::InvalidArgument("empty node".into()));
    }
    match (&node.kind, &node.entries) {
        (NodeKind::Leaf, NodeEntries::Leaf(entries)) => {
            if node.level != 0 {
                return Err(Error::InvalidArgument(format!(
                    "leaf level {} != 0",
                    node.level
                )));
            }
            for entry in entries {
                match &entry.value {
                    LeafValue::Row(bytes) if bytes.len() > INLINE_MAX => {
                        return Err(Error::InvalidArgument(format!(
                            "inline row {} exceeds {INLINE_MAX}",
                            bytes.len()
                        )));
                    }
                    LeafValue::Row(_) | LeafValue::Tombstone => {}
                    LeafValue::Blob(blob) => blob.validate()?,
                }
            }
        }
        (NodeKind::Internal, NodeEntries::Internal(_)) => {
            if node.level == 0 {
                return Err(Error::InvalidArgument(
                    "internal node level must be >= 1".into(),
                ));
            }
        }
        _ => {
            return Err(Error::InvalidArgument(
                "node kind does not match entries".into(),
            ));
        }
    }
    Ok(())
}

fn serialize_payload(node: &Node) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(node).map_err(|e| Error::Encode(format!("rmp encode node: {e}")))
}

/// Serialized size including the OSSC header and CRC trailer.
pub fn encoded_size(node: &Node) -> Result<usize> {
    Ok(HEADER_LEN + serialize_payload(node)?.len() + TRAILER_LEN)
}

pub fn encode_node(node: &Node) -> Result<Vec<u8>> {
    validate(node)?;
    let payload = serialize_payload(node)?;
    let size = HEADER_LEN + payload.len() + TRAILER_LEN;
    if size > NODE_SPLIT_BYTES {
        return Err(Error::InvalidArgument(format!(
            "node {size} exceeds {NODE_SPLIT_BYTES} bytes"
        )));
    }
    format::encode_object(
        &ObjectHeader {
            kind: ObjectKind::Node,
            major: KNOWN_MAJOR,
            min_reader: KNOWN_MINOR,
        },
        &payload,
    )
}

pub fn decode_node(bytes: &[u8]) -> Result<Node> {
    let (header, payload) = format::decode_object(bytes)?;
    if header.kind != ObjectKind::Node {
        return Err(Error::CorruptMeta("expected node object".into()));
    }
    if payload.len() > NODE_SPLIT_BYTES {
        return Err(Error::CorruptMeta(format!(
            "node payload {} exceeds {NODE_SPLIT_BYTES}",
            payload.len()
        )));
    }
    let node: Node = rmp_serde::from_slice(payload)
        .map_err(|e| Error::CorruptMeta(format!("rmp decode node: {e}")))?;
    validate(&node).map_err(|e| Error::CorruptMeta(format!("invalid node: {e}")))?;
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_state::blob::CHUNK_SIZE;
    use crate::error::Error;

    const GOLDEN_LEAF_HEX: &str = "4f53534302000100000000003583a46b696e64a46c656166a56c6576656c00a7656e747269657381a46c6561669182a36b65799161a576616c756581a3726f77917608933232";

    fn row(key: &[u8], value: &[u8]) -> LeafEntry {
        LeafEntry {
            key: key.to_vec(),
            value: LeafValue::Row(value.to_vec()),
        }
    }

    fn leaf(entries: Vec<LeafEntry>) -> Node {
        Node {
            kind: NodeKind::Leaf,
            level: 0,
            entries: NodeEntries::Leaf(entries),
        }
    }

    #[test]
    fn leaf_roundtrip_all_value_kinds() {
        let node = leaf(vec![
            row(b"a", b"one"),
            LeafEntry {
                key: b"b".to_vec(),
                value: LeafValue::Blob(BlobRef {
                    sha256: [3; 32],
                    len: CHUNK_SIZE as u64 + 1,
                    chunks: 2,
                }),
            },
            LeafEntry {
                key: b"c".to_vec(),
                value: LeafValue::Tombstone,
            },
        ]);
        let bytes = encode_node(&node).unwrap();
        assert_eq!(decode_node(&bytes).unwrap(), node);
        assert!(encoded_size(&node).unwrap() <= NODE_SPLIT_BYTES);
    }

    #[test]
    fn internal_roundtrip() {
        let node = Node {
            kind: NodeKind::Internal,
            level: 2,
            entries: NodeEntries::Internal(vec![
                InternalEntry {
                    first_key: b"a".to_vec(),
                    child: [1; 32],
                },
                InternalEntry {
                    first_key: b"m".to_vec(),
                    child: [2; 32],
                },
            ]),
        };
        let bytes = encode_node(&node).unwrap();
        assert_eq!(decode_node(&bytes).unwrap(), node);
    }

    #[test]
    fn golden_leaf_bytes() {
        let node = leaf(vec![row(b"a", b"v")]);
        let bytes = encode_node(&node).unwrap();
        assert_eq!(hex::encode(&bytes), GOLDEN_LEAF_HEX);
    }

    #[test]
    fn inline_cap_enforced() {
        let ok = leaf(vec![row(b"k", &vec![0u8; INLINE_MAX])]);
        assert!(encode_node(&ok).is_ok());
        let too_big = leaf(vec![row(b"k", &vec![0u8; INLINE_MAX + 1])]);
        assert!(matches!(
            encode_node(&too_big),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn max_node_size_enforced() {
        let entries: Vec<_> = (0..2000)
            .map(|i| row(&(i as u32).to_be_bytes(), &[9u8; 100]))
            .collect();
        let node = leaf(entries);
        assert!(encoded_size(&node).unwrap() > NODE_SPLIT_BYTES);
        assert!(matches!(encode_node(&node), Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn wrong_object_kind_rejected() {
        let bytes = crate::control_state::format::encode_object(
            &crate::control_state::format::ObjectHeader {
                kind: crate::control_state::format::ObjectKind::Root,
                major: crate::control_state::format::KNOWN_MAJOR,
                min_reader: crate::control_state::format::KNOWN_MINOR,
            },
            b"not-a-node",
        )
        .unwrap();
        assert!(matches!(decode_node(&bytes), Err(Error::CorruptMeta(_))));
    }

    #[test]
    fn structural_validation() {
        let bad_level = Node {
            kind: NodeKind::Leaf,
            level: 1,
            entries: NodeEntries::Leaf(vec![row(b"a", b"v")]),
        };
        assert!(matches!(
            encode_node(&bad_level),
            Err(Error::InvalidArgument(_))
        ));

        let empty = leaf(vec![]);
        assert!(matches!(
            encode_node(&empty),
            Err(Error::InvalidArgument(_))
        ));

        let kind_mismatch = Node {
            kind: NodeKind::Internal,
            level: 1,
            entries: NodeEntries::Leaf(vec![row(b"a", b"v")]),
        };
        assert!(matches!(
            encode_node(&kind_mismatch),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn invalid_blob_ref_rejected() {
        let node = leaf(vec![LeafEntry {
            key: b"k".to_vec(),
            value: LeafValue::Blob(BlobRef {
                sha256: [0; 32],
                len: CHUNK_SIZE as u64 + 1,
                chunks: 1, // should be 2
            }),
        }]);
        assert!(matches!(encode_node(&node), Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn oversized_payload_decode_rejected() {
        let payload = vec![0u8; NODE_SPLIT_BYTES + 1];
        let bytes = crate::control_state::format::encode_object(
            &crate::control_state::format::ObjectHeader {
                kind: crate::control_state::format::ObjectKind::Node,
                major: crate::control_state::format::KNOWN_MAJOR,
                min_reader: crate::control_state::format::KNOWN_MINOR,
            },
            &payload,
        )
        .unwrap();
        assert!(matches!(decode_node(&bytes), Err(Error::CorruptMeta(_))));
    }
}
