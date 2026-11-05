//! Object-backed control store (implementation plan B1).
//!
//! One immutable copy-on-write B+tree per table; the only mutable object is the
//! root, whose local-control CAS is the commit point (design §2.2). This module
//! is storage/transaction plumbing only: domain adapters (B2+) stage rows here
//! and commit. Prefer [`ObjectControlStore::transaction`], which re-runs the
//! whole closure on a root CAS conflict so guards never replay on a stale root.
//! No begin/rollback is exposed; dropping a transaction discards its overlay.

mod account;
mod account_snapshot;
mod account_write;
mod ap_olap;
mod audit;
mod bucket;
mod bucket_snapshot;
mod bucket_write;
mod change;
mod event_outbox;
mod iam;
mod iam_snapshot;
mod iam_txn;
mod iam_write;
mod inventory;
mod inventory_txn;
mod jobs;
mod jobs_claim;
mod jobs_read;
mod jobs_snapshot;
mod jobs_write;
mod lifecycle;
mod local_ops;
mod origin;
mod repl_outbox;
mod root_io;
mod site;
mod site_apply;
mod sync_intents;
mod table;
mod tier;
mod txn;
mod uploads;
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
