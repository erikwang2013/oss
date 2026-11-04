//! Object-backed control state: immutable copy-on-write nodes under one CAS root.
//!
//! Storage primitives only (implementation plan A1-A4): framing, node/blob codecs,
//! a copy-on-write B+tree per table, the root envelope and an in-memory fault
//! NodeIo for crash tests. The transaction/commit layer lives above this module.

pub mod blob;
#[cfg(any(test, feature = "test-fault-io"))]
pub mod fault_io;
pub mod format;
pub mod hash;
pub mod io;
pub mod node;
pub mod root;
pub mod tree;
