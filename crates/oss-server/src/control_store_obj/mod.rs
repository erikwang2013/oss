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
mod instant_index;
mod inventory;
mod inventory_txn;
pub(crate) mod jobs;
mod jobs_claim;
mod jobs_read;
mod jobs_snapshot;
mod jobs_write;
mod lifecycle;
mod local_ops;
mod origin;
mod repl_outbox;
mod root_io;
mod seam;
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
// Row shapes are re-exported for the generation-tagged read view (C2); the
// domain modules themselves stay private.
pub(crate) use account::{AccountRow, LedgerRow, MemberRow};
pub(crate) use bucket::{BucketRow, decode_row, encode_row, key1, txn_scan_all};
pub(crate) use bucket_write::bucket_clear_owner;
pub(crate) use iam::{
    AccessKeyRow, PermissionStorageRow, UserRow, permission_key, scan_permissions, scan_users,
};
pub(crate) use iam_write::insert_access_key;
pub use inventory::BucketDomainCleanup;
pub use origin::WriteOrigin;
#[cfg(test)]
pub(crate) use root_io::{CRASH_AFTER_ROOT_CAS, CRASH_BEFORE_ROOT_CAS, CrashIo};
pub use root_io::{PinnedRoot, RootIo};
pub(crate) use site::{SiteEntityClockRow, changelog_key, clock_key};
pub use table::{KNOWN_TABLES, TableId, known_table_ids};
pub use txn::{CachePublish, CommitStamp, ObjectControlStore, ObjectControlTxn};
pub use vpc::{VpcStore, VpcTxn};
