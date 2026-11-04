//! Node IO seam. Keys are `control/state/node/<sha256-hex>`; nodes are
//! content-addressed and create-once. `put_create` must accept an existing key
//! only when the stored bytes are identical (design §1.1, §2.2 step 2).

use async_trait::async_trait;

use super::hash;
use super::node::{self, Node};
use crate::error::{Error, Result};

#[async_trait]
pub trait NodeIo: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    /// Exists + same bytes = Ok; exists + different bytes = `CorruptMeta`.
    async fn put_create(&self, key: &str, bytes: &[u8]) -> Result<()>;
}

/// Frame, encode and create-once a node; returns its content hash.
pub async fn put_node(io: &dyn NodeIo, node: &Node) -> Result<[u8; 32]> {
    let bytes = node::encode_node(node)?;
    let hash = hash::sha256(&bytes);
    io.put_create(&hash::node_key(&hash), &bytes).await?;
    Ok(hash)
}

/// Read a node and verify its content address before decoding.
pub async fn get_node(io: &dyn NodeIo, hash: &[u8; 32]) -> Result<Node> {
    let key = hash::node_key(hash);
    let bytes = io
        .get(&key)
        .await?
        .ok_or_else(|| Error::CorruptMeta(format!("missing node {}", hex::encode(hash))))?;
    if hash::sha256(&bytes) != *hash {
        return Err(Error::CorruptMeta(format!(
            "node {} content hash mismatch",
            hex::encode(hash)
        )));
    }
    node::decode_node(&bytes)
}
