//! Object-backed control store (implementation plan B1).
//!
//! One immutable copy-on-write B+tree per table; the only mutable object is the
//! root, whose local-control CAS is the commit point (design §2.2). This module
//! is storage/transaction plumbing only: domain adapters (B2+) stage rows here
//! and commit. Prefer [`ObjectControlStore::transaction`], which re-runs the
//! whole closure on a root CAS conflict so guards never replay on a stale root.
//! No begin/rollback is exposed; dropping a transaction discards its overlay.

mod ap_olap;
mod bucket;
mod bucket_snapshot;
mod bucket_write;
mod change;
mod inventory;
mod inventory_txn;
mod lifecycle;
mod origin;
mod root_io;
mod table;
mod tier;
mod txn;
mod vpc;

#[cfg(test)]
mod tests;

pub use ap_olap::{AccessPointStore, AccessPointTxn};
pub use inventory::BucketDomainCleanup;
pub use origin::WriteOrigin;
pub use root_io::{PinnedRoot, RootIo};
pub use table::{KNOWN_TABLES, TableId, known_table_ids};
pub use txn::{CachePublish, CommitStamp, ObjectControlStore, ObjectControlTxn};
pub use vpc::{VpcStore, VpcTxn};
