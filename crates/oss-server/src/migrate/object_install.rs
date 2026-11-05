//! Install the SQLite state into the object control root.
//!
//! All table nodes/blobs are written first; the root CAS is the last step, so an
//! interrupted apply before the commit leaves no visible root. Raw rows are
//! written through `migrate_support` to preserve local attributes (uids,
//! `is_admin`, bucket `set_id`, account admin FK) that the replicated snapshot
//! installers project away. Replicated configuration tables use the same
//! high-level installers as Raft snapshots.

use xl_store::control_state::root::{AppliedLog, Root};

use crate::control_store_obj::migrate_support::{
    put_access_key, put_bucket, put_ledger, put_permission, put_site_changelog, put_site_clock,
    put_site_config, put_site_peer, put_user,
};
use crate::control_store_obj::{
    AccessPointTxn, ObjectControlStore, PinnedRoot, VpcTxn, WriteOrigin,
};
use crate::raft::compat::MAX_FEATURE_LEVEL;
use xl_store::control_store::bucket::BucketSnapshotTxn;
use xl_store::control_store::inventory::InventoryTxn;
use xl_store::control_store::jobs::JobSnapshotTxn;
use xl_store::control_store::lifecycle::LifecycleTxn;
use xl_store::control_store::tier::TierTxn;

use super::export::olap_entry;
use super::source::{BoxError, Source, effective_feature, error, expected_allocators};

pub(crate) async fn install(
    control: &ObjectControlStore,
    source: &Source,
) -> Result<u64, BoxError> {
    install_with(control, source, || Ok(())).await
}

/// `before_commit` runs after every row is staged and before the root CAS; the
/// test hook injects an interruption here.
pub(crate) async fn install_with(
    control: &ObjectControlStore,
    source: &Source,
    before_commit: impl FnOnce() -> Result<(), BoxError>,
) -> Result<u64, BoxError> {
    let level = effective_feature(source);
    if level > MAX_FEATURE_LEVEL {
        return error(format!(
            "effective feature level {level} exceeds maximum {MAX_FEATURE_LEVEL}"
        ));
    }
    let now = crate::db::now();
    let pinned = PinnedRoot {
        version: 0,
        fence_token: 0,
        bytes: Vec::new(),
        // `created_at` is synthetic and never stored; the skew guarantees the
        // commit below even when every table and the meta fields are empty.
        root: Root::empty(source.mode, now.wrapping_sub(1)),
    };
    let mut txn = control
        .begin_at(pinned, WriteOrigin::snapshot_install())
        .await?;
    txn.bulk_install();

    for row in &source.raw.users {
        put_user(&mut txn, row).await?;
    }
    for row in &source.raw.access_keys {
        put_access_key(&mut txn, row).await?;
    }
    for row in &source.raw.permissions {
        put_permission(&mut txn, row).await?;
    }
    for row in &source.raw.buckets {
        put_bucket(&mut txn, row).await?;
    }
    for row in &source.raw.accounts {
        crate::control_store_obj::put_account(&mut txn, row).await?;
    }
    for row in &source.raw.members {
        crate::control_store_obj::install_member(&mut txn, row.clone()).await?;
    }
    for row in &source.raw.ledger {
        put_ledger(&mut txn, row).await?;
    }
    if let Some(row) = &source.raw.site_config {
        put_site_config(&mut txn, row).await?;
    }
    for row in &source.raw.sites {
        put_site_peer(&mut txn, row).await?;
    }
    for row in &source.raw.changelog {
        put_site_changelog(&mut txn, row).await?;
    }
    for row in &source.raw.clocks {
        put_site_clock(&mut txn, row).await?;
    }

    txn.replace_bucket_policies(&source.data.bucket_policies)
        .await?;
    txn.replace_all_lifecycle_rules(&source.data.lifecycle_rules)
        .await?;
    txn.replace_event_configs(&source.data.event_configs)
        .await?;
    txn.replace_replication_rules(&source.data.replication_rules)
        .await?;
    txn.replace_access_points(&source.data.access_points)
        .await?;
    let olap = source
        .data
        .olap_configs
        .iter()
        .map(olap_entry)
        .collect::<Result<Vec<_>, _>>()?;
    txn.replace_olap_configs(&olap).await?;
    txn.replace_inventory_configs(&source.data.inventory_configs)
        .await?;
    txn.replace_vpc_endpoints(&source.data.vpc_endpoints)
        .await?;

    txn.runstate_clear().await?;
    txn.insert_jobs(&source.runstate.jobs).await?;
    txn.insert_job_tasks(&source.runstate.job_tasks).await?;
    txn.replace_inventory_runs(&source.runstate.inventory_runs)
        .await?;
    txn.replace_inventory_cursors(&source.runstate.inventory_cursors)
        .await?;
    txn.replace_inventory_incrementals(&source.runstate.inventory_incremental)
        .await?;
    txn.replace_tier_restores(&source.runstate.tier_restores)
        .await?;
    txn.replace_lifecycle_cursors(&source.runstate.lifecycle_cursors)
        .await?;
    txn.insert_schedule_lease(source.runstate.schedule_lease.as_ref())
        .await?;
    txn.insert_runstate_meta("next_job_id", source.runstate.next_job_id)
        .await?;
    txn.insert_runstate_meta("next_run_id", source.runstate.next_run_id)
        .await?;

    // Applied position also marks the transaction dirty when every table is empty.
    txn.set_applied(source.applied.map(|l| AppliedLog {
        term: l.leader_id.term,
        index: l.index,
    }));
    let (uid, job, run, site) = expected_allocators(source);
    {
        let meta = txn.meta_mut();
        meta.feature_level = u16::from(level);
        meta.next_uid = uid;
        meta.next_job_id = job;
        meta.next_run_id = run;
        meta.next_site_seq = site;
        meta.membership_json = source.membership_json.clone();
        meta.created_at = now;
    }
    if source.accounts_canonical {
        txn.set_marker(crate::db::META_ACCOUNTS_CANONICAL, "done");
    }

    // On error the transaction is dropped uncommitted: no root CAS, no visibility.
    before_commit()?;
    let stamp = txn.commit().await?;
    Ok(stamp.generation)
}
