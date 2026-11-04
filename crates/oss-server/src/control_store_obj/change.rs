//! The per-transaction write overlay (design §2.1): table id → key → staged row.
//! Large rows are staged as already-written content-addressed blobs.

use std::collections::BTreeMap;

use xl_store::control_state::blob::BlobRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StagedRow {
    /// Inline row bytes (≤ 16 KiB).
    Put(Vec<u8>),
    /// Row stored as 64 KiB content-addressed chunks.
    PutBlob(BlobRef),
    Delete,
}

pub(crate) type Overlay = BTreeMap<u16, BTreeMap<Vec<u8>, StagedRow>>;
