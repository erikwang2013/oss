//! Export the object control root through the live snapshot encoders and
//! compare it byte-for-byte with the SQLite source export. Raw local attributes
//! (uid/FK columns) are compared separately because the replicated snapshot
//! intentionally projects them to natural keys.

use base64::Engine as _;
use xl_store::control_store::OlapConfigEntry;
use xl_store::control_store::account::AccountSnapshotTxn;
use xl_store::control_store::bucket::BucketSnapshotTxn;
use xl_store::control_store::iam::IamSnapshotTxn;
use xl_store::control_store::inventory::InventoryTxn;
use xl_store::control_store::jobs::JobSnapshotTxn;
use xl_store::control_store::lifecycle::LifecycleTxn;
use xl_store::control_store::tier::TierTxn;

use crate::control_store_obj::migrate_support::{
    bucket_rows, scan_access_keys, scan_accounts, scan_ledger, scan_members, scan_permissions,
    scan_users, site_changelog_rows, site_clock_rows, site_config_rows, site_peer_rows,
};
use crate::control_store_obj::{
    AccessPointTxn, ObjectControlStore, TableId, VpcTxn, WriteOrigin, known_table_ids,
};
use crate::raft::data::{self, ControlData, OlapConfigRow};
use crate::raft::runstate::{self, RunState};

use super::source::{BoxError, Source, effective_feature, error, expected_allocators};

pub(crate) struct ObjectVerify {
    pub generation: u64,
    pub root_hash: String,
    pub snapshot_sha256: String,
    pub runstate_sha256: String,
    pub table_counts: std::collections::BTreeMap<String, u64>,
}

pub(crate) async fn read_object(
    control: &ObjectControlStore,
) -> Result<(ControlData, RunState), BoxError> {
    // Read-only use of a transaction: no commit, so no root CAS happens.
    let mut txn = control.begin(WriteOrigin::snapshot_install()).await?;
    let data = ControlData {
        users: txn.snapshot_users().await?,
        access_keys: txn.snapshot_access_keys().await?,
        permissions: txn.snapshot_permissions().await?,
        buckets: txn.buckets().await?,
        bucket_policies: txn.bucket_policies().await?,
        lifecycle_rules: txn.all_lifecycle_rules().await?,
        event_configs: txn.event_configs().await?,
        replication_rules: txn.replication_rules().await?,
        access_points: txn.access_points().await?,
        olap_configs: txn
            .olap_configs()
            .await?
            .into_iter()
            .map(|e| OlapConfigRow {
                access_point: e.access_point,
                kind: e.kind,
                endpoint: e.endpoint,
                module_b64: e
                    .module
                    .map(|m| base64::engine::general_purpose::STANDARD.encode(m)),
                module_sha256: e.module_sha256,
                timeout_ms: e.timeout_ms,
                max_response_bytes: e.max_response_bytes,
                updated_at: e.updated_at,
            })
            .collect(),
        inventory_configs: txn.inventory_configs().await?,
        accounts: txn.snapshot_accounts().await?,
        account_members: txn.snapshot_account_members().await?,
        vpc_endpoints: txn.vpc_endpoints().await?,
        account_usage_ledger: txn.snapshot_account_usage_ledger().await?,
    };

    let mut rs = RunState {
        jobs: txn.runstate_jobs().await?,
        job_tasks: txn.runstate_job_tasks().await?,
        inventory_runs: txn.inventory_runs().await?,
        inventory_cursors: txn.inventory_cursors().await?,
        inventory_incremental: txn.inventory_incrementals().await?,
        tier_restores: txn.tier_restores().await?,
        lifecycle_cursors: txn.lifecycle_cursors().await?,
        schedule_lease: txn.snapshot_schedule_lease().await?,
        next_job_id: txn
            .snapshot_runstate_meta("next_job_id")
            .await?
            .unwrap_or_default(),
        next_run_id: txn
            .snapshot_runstate_meta("next_run_id")
            .await?
            .unwrap_or_default(),
    };
    normalize_runstate(&mut rs);
    Ok((data, rs))
}

fn normalize_runstate(rs: &mut RunState) {
    rs.next_job_id = rs
        .next_job_id
        .max(rs.jobs.iter().map(|j| j.id + 1).max().unwrap_or(0));
    rs.next_run_id = rs.next_run_id.max(
        rs.inventory_runs
            .iter()
            .map(|r| r.id + 1)
            .max()
            .unwrap_or(0),
    );
    if !rs.is_empty() {
        rs.next_job_id = rs.next_job_id.max(1);
        rs.next_run_id = rs.next_run_id.max(1);
    }
}

pub(crate) async fn verify(
    control: &ObjectControlStore,
    source: &Source,
) -> Result<ObjectVerify, BoxError> {
    let pinned = control.pin().await?;
    pinned
        .root
        .check_compatible(&known_table_ids())
        .map_err(|e| std::io::Error::other(format!("object root incompatible: {e}")))?;
    let meta = &pinned.root.meta;
    let level = effective_feature(source);
    if meta.mode != source.mode {
        return error(format!(
            "root mode {:?} != source {:?}",
            meta.mode, source.mode
        ));
    }
    if meta.feature_level != u16::from(level) {
        return error(format!(
            "root feature level {} != source effective {level}",
            meta.feature_level
        ));
    }
    let want_marker = source.accounts_canonical.then_some("done");
    if meta
        .markers
        .get(crate::db::META_ACCOUNTS_CANONICAL)
        .map(String::as_str)
        != want_marker
    {
        return error("root accounts_canonical marker differs from source");
    }
    let want_applied = source
        .applied
        .map(|l| xl_store::control_state::root::AppliedLog {
            term: l.leader_id.term,
            index: l.index,
        });
    if meta.applied != want_applied {
        return error(format!(
            "root applied {:?} != source {want_applied:?}",
            meta.applied
        ));
    }
    if meta.membership_json != source.membership_json {
        return error("root membership_json differs from source");
    }
    let (data, runstate) = read_object(control).await?;
    let want = data::encode_snapshot_at(&source.data, level)
        .map_err(|e| std::io::Error::other(format!("source control export: {e}")))?;
    let got = data::encode_snapshot_at(&data, level)
        .map_err(|e| std::io::Error::other(format!("object control export: {e}")))?;
    if want != got {
        return error(format!(
            "control export mismatch: sqlite {} bytes vs object {} bytes",
            want.len(),
            got.len()
        ));
    }
    let want_run = runstate::encode_runstate_snapshot(&source.data, &source.runstate, level)
        .map_err(|e| std::io::Error::other(format!("source runstate export: {e}")))?;
    let got_run = runstate::encode_runstate_snapshot(&data, &runstate, level)
        .map_err(|e| std::io::Error::other(format!("object runstate export: {e}")))?;
    if want_run != got_run {
        return error(format!(
            "runstate export mismatch: sqlite {} bytes vs object {} bytes",
            want_run.len(),
            got_run.len()
        ));
    }
    compare_raw(control, source).await?;
    let table_counts = compare_counts(&pinned.root, source)?;
    let (uid, job, run, site) = expected_allocators(source);
    let meta = &pinned.root.meta;
    if (
        meta.next_uid,
        meta.next_job_id,
        meta.next_run_id,
        meta.next_site_seq,
    ) != (uid, job, run, site)
    {
        return error(format!(
            "root allocator mismatch: got uid={} job={} run={} site={}, want uid={uid} job={job} run={run} site={site}",
            meta.next_uid, meta.next_job_id, meta.next_run_id, meta.next_site_seq
        ));
    }
    let root_bytes = xl_store::control_state::root::encode_root(&pinned.root)
        .map_err(|e| std::io::Error::other(format!("encode root: {e}")))?;
    Ok(ObjectVerify {
        generation: pinned.version,
        root_hash: super::raft_source::sha256(&root_bytes),
        snapshot_sha256: super::raft_source::sha256(&got),
        runstate_sha256: super::raft_source::sha256(&got_run),
        table_counts,
    })
}

fn compare_counts(
    root: &xl_store::control_state::root::Root,
    source: &Source,
) -> Result<std::collections::BTreeMap<String, u64>, BoxError> {
    let mut out = std::collections::BTreeMap::new();
    for table in TableId::ALL {
        let name = table.name();
        let want = source.table_counts.get(name).copied().unwrap_or(0).max(0) as u64;
        let got = root.table(table.as_u16()).map(|t| t.rows).unwrap_or(0);
        if want != got {
            return error(format!(
                "table {name} row count: sqlite {want} vs object {got}"
            ));
        }
        out.insert(name.to_string(), got);
    }
    Ok(out)
}

async fn compare_raw(control: &ObjectControlStore, source: &Source) -> Result<(), BoxError> {
    let txn = control.begin(WriteOrigin::snapshot_install()).await?;
    let raw = &source.raw;
    let mismatch = |what: &str| format!("raw {what} rows differ between sqlite and object root");

    if scan_users(&txn).await? != raw.users {
        return error(mismatch("users"));
    }
    if scan_access_keys(&txn).await? != raw.access_keys {
        return error(mismatch("access_keys"));
    }
    if scan_permissions(&txn).await? != raw.permissions {
        return error(mismatch("permissions"));
    }
    if bucket_rows(&txn).await? != raw.buckets {
        return error(mismatch("buckets"));
    }
    if scan_accounts(&txn).await? != raw.accounts {
        return error(mismatch("accounts"));
    }
    let mut members = scan_members(&txn).await?;
    members.sort_by(|a, b| (&a.account_id, a.uid).cmp(&(&b.account_id, b.uid)));
    if members != raw.members {
        return error(mismatch("account_members"));
    }
    if scan_ledger(&txn).await? != raw.ledger {
        return error(mismatch("account_usage_ledger"));
    }
    if site_peer_rows(&txn).await? != raw.sites {
        return error(mismatch("sites"));
    }
    let config = site_config_rows(&txn).await?.into_iter().next();
    if config != raw.site_config {
        return error(mismatch("site_config"));
    }
    if site_changelog_rows(&txn).await? != raw.changelog {
        return error(mismatch("site_changelog"));
    }
    if site_clock_rows(&txn).await? != raw.clocks {
        return error(mismatch("site_entity_clock"));
    }
    Ok(())
}

/// Ensure a converted `OlapConfigEntry` module round-trips (used by install).
pub(crate) fn olap_entry(row: &OlapConfigRow) -> Result<OlapConfigEntry, BoxError> {
    let module = match &row.module_b64 {
        None => None,
        Some(b64) => Some(
            base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| std::io::Error::other(format!("olap module base64: {e}")))?,
        ),
    };
    Ok(OlapConfigEntry {
        access_point: row.access_point.clone(),
        kind: row.kind.clone(),
        endpoint: row.endpoint.clone(),
        module,
        module_sha256: row.module_sha256.clone(),
        timeout_ms: row.timeout_ms,
        max_response_bytes: row.max_response_bytes,
        updated_at: row.updated_at,
    })
}
