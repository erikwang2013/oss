//! Root envelope: table registry, allocators and commit metadata.
//!
//! The root is the single compare-and-swap object of the control store. It is
//! small (<= 32 KiB), mentions only authenticated content hashes, and carries
//! the allocator high-water marks and applied log position.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::control_state::format::{self, ObjectHeader, ObjectKind};
pub use crate::control_state::format::{KNOWN_MAJOR, KNOWN_MINOR};
use crate::error::{Error, Result};

/// Hard upper bound for a framed root object.
pub const ROOT_MAX_BYTES: usize = 32 * 1024;
/// Feature level written by this version for a fresh store.
pub const DEFAULT_FEATURE_LEVEL: u16 = 1;

/// Per-table root pointer and committed counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRoot {
    pub hash: [u8; 32],
    pub rows: u64,
    pub bytes: u64,
    /// Reader minor this table needs. Defaults to 0 so older roots decode.
    #[serde(default)]
    pub required_minor: u16,
}

/// Deployment shape the store is running in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Standalone,
    Raft,
}

/// Last log entry applied to the state represented by this root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedLog {
    pub term: u64,
    pub index: u64,
}

/// Mutable control-plane metadata committed together with the tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootMeta {
    pub mode: Mode,
    pub markers: BTreeMap<String, String>,
    pub next_uid: u64,
    pub next_job_id: u64,
    pub next_run_id: u64,
    pub next_site_seq: i64,
    pub applied: Option<AppliedLog>,
    pub membership_json: Option<String>,
    pub feature_level: u16,
    pub created_at: i64,
}

impl RootMeta {
    pub fn alloc_uid(&mut self) -> Result<u64> {
        alloc_u64(&mut self.next_uid)
    }

    pub fn alloc_job_id(&mut self) -> Result<u64> {
        alloc_u64(&mut self.next_job_id)
    }

    pub fn alloc_run_id(&mut self) -> Result<u64> {
        alloc_u64(&mut self.next_run_id)
    }

    pub fn alloc_site_seq(&mut self) -> Result<i64> {
        alloc_i64(&mut self.next_site_seq)
    }

    /// Advance past an id that may already be visible outside the store.
    pub fn observe_uid(&mut self, used: u64) -> Result<()> {
        observe_u64(&mut self.next_uid, used)
    }

    pub fn observe_job_id(&mut self, used: u64) -> Result<()> {
        observe_u64(&mut self.next_job_id, used)
    }

    pub fn observe_run_id(&mut self, used: u64) -> Result<()> {
        observe_u64(&mut self.next_run_id, used)
    }

    pub fn observe_site_seq(&mut self, used: i64) -> Result<()> {
        observe_i64(&mut self.next_site_seq, used)
    }
}

fn alloc_u64(next: &mut u64) -> Result<u64> {
    let id = *next;
    *next = id
        .checked_add(1)
        .ok_or_else(|| Error::InvalidArgument("allocator overflow".into()))?;
    Ok(id)
}

fn observe_u64(next: &mut u64, used: u64) -> Result<()> {
    let following = used
        .checked_add(1)
        .ok_or_else(|| Error::InvalidArgument("allocator overflow".into()))?;
    *next = (*next).max(following);
    Ok(())
}

fn alloc_i64(next: &mut i64) -> Result<i64> {
    let id = *next;
    *next = id
        .checked_add(1)
        .ok_or_else(|| Error::InvalidArgument("allocator overflow".into()))?;
    Ok(id)
}

fn observe_i64(next: &mut i64, used: i64) -> Result<()> {
    let following = used
        .checked_add(1)
        .ok_or_else(|| Error::InvalidArgument("allocator overflow".into()))?;
    *next = (*next).max(following);
    Ok(())
}

/// The committed root payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Root {
    pub major: u16,
    pub min_reader: u16,
    pub tables: BTreeMap<u16, TableRoot>,
    pub meta: RootMeta,
}

impl Root {
    /// Bootstrap an empty store. Allocators start at 1 so 0 stays "unset".
    pub fn empty(mode: Mode, created_at: i64) -> Self {
        Root {
            major: KNOWN_MAJOR,
            min_reader: KNOWN_MINOR,
            tables: BTreeMap::new(),
            meta: RootMeta {
                mode,
                markers: BTreeMap::new(),
                next_uid: 1,
                next_job_id: 1,
                next_run_id: 1,
                next_site_seq: 1,
                applied: None,
                membership_json: None,
                feature_level: DEFAULT_FEATURE_LEVEL,
                created_at,
            },
        }
    }

    pub fn table(&self, id: u16) -> Option<&TableRoot> {
        self.tables.get(&id)
    }

    pub fn set_table(&mut self, id: u16, table: TableRoot) {
        self.tables.insert(id, table);
    }

    pub fn remove_table(&mut self, id: u16) -> Option<TableRoot> {
        self.tables.remove(&id)
    }

    /// Highest reader minor demanded by the envelope or any registered table.
    pub fn required_minor(&self) -> u16 {
        self.tables
            .values()
            .map(|t| t.required_minor)
            .max()
            .unwrap_or(0)
            .max(self.min_reader)
    }

    /// Fail closed when a table needs a newer reader than this binary.
    pub fn check_required_minor(&self) -> Result<()> {
        if self.required_minor() > KNOWN_MINOR {
            return Err(Error::Unsupported(format!(
                "control root requires reader minor {} (known {KNOWN_MINOR})",
                self.required_minor()
            )));
        }
        Ok(())
    }

    pub fn unknown_tables(&self, known: &[u16]) -> Vec<u16> {
        self.tables
            .keys()
            .copied()
            .filter(|id| !known.contains(id))
            .collect()
    }

    /// Gate export/install of a state written by a newer binary.
    pub fn check_compatible(&self, known: &[u16]) -> Result<()> {
        self.check_required_minor()?;
        if let Some(id) = self.unknown_tables(known).first() {
            return Err(Error::Unsupported(format!(
                "control root references unknown table {id}"
            )));
        }
        Ok(())
    }
}

/// Encode a framed root object, enforcing the 32 KiB cap and version gates.
pub fn encode_root(root: &Root) -> Result<Vec<u8>> {
    if root.major != KNOWN_MAJOR || root.min_reader > KNOWN_MINOR {
        return Err(Error::Unsupported(
            "refusing to write an unknown control root version".into(),
        ));
    }
    let payload = rmp_serde::to_vec_named(root)
        .map_err(|e| Error::InvalidArgument(format!("encode control root: {e}")))?;
    let framed = format::HEADER_LEN + payload.len() + format::TRAILER_LEN;
    if framed > ROOT_MAX_BYTES {
        return Err(Error::InvalidArgument(format!(
            "control root is {framed} bytes, over the {ROOT_MAX_BYTES} byte cap"
        )));
    }
    format::encode_object(
        &ObjectHeader {
            kind: ObjectKind::Root,
            major: root.major,
            min_reader: root.min_reader,
        },
        &payload,
    )
}

/// Decode and validate a framed root object.
pub fn decode_root(bytes: &[u8]) -> Result<Root> {
    if bytes.len() > ROOT_MAX_BYTES {
        return Err(Error::CorruptMeta(format!(
            "control root is {} bytes, over the {ROOT_MAX_BYTES} byte cap",
            bytes.len()
        )));
    }
    let (header, payload) = format::decode_object(bytes)?;
    if header.kind != ObjectKind::Root {
        return Err(Error::CorruptMeta("object is not a control root".into()));
    }
    let root: Root = rmp_serde::from_slice(payload)
        .map_err(|e| Error::CorruptMeta(format!("decode control root: {e}")))?;
    if root.major != header.major || root.min_reader != header.min_reader {
        return Err(Error::CorruptMeta(
            "control root header and payload versions disagree".into(),
        ));
    }
    Ok(root)
}
