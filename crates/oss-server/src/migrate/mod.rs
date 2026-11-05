//! Offline SQLite → object control-store migration (G1).
//!
//! The source is opened read-only and read in one transaction, then exported
//! through the same [`crate::raft::data`] / [`crate::raft::runstate`] encoders
//! the object backend serves. `--apply` writes the node-local files and Raft
//! file stores first and the object root last (single CAS); `--dry-run` opens no
//! target path at all. Every apply is idempotent and every mismatch fails closed.

mod export;
mod local_rows;
mod local_sink;
mod object_install;
mod raft_sink;
mod raft_source;
mod source;
mod source_rows;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use xl_store::store::{Store, StoreConfig};

use crate::control_store_obj::{ObjectControlStore, RootIo};
use crate::raft::data;
use crate::raft::runstate;

use source::{BoxError, Source, effective_feature, error, required_feature};

pub(crate) type ReportResult = Result<Report, BoxError>;

#[derive(Serialize)]
pub(crate) struct Report {
    pub command: &'static str,
    pub dry_run: bool,
    pub status: &'static str,
    pub source: String,
    pub source_sha256: String,
    pub state_dir: String,
    pub raft_db: String,
    pub mode: String,
    pub feature: FeatureReport,
    pub schema_meta: BTreeMap<String, String>,
    pub sqlite_table_counts: BTreeMap<String, i64>,
    pub control_table_counts: BTreeMap<String, u64>,
    pub source_snapshot_sha256: String,
    pub source_runstate_sha256: String,
    pub object: Option<ObjectReport>,
    pub local: Option<LocalReport>,
    pub raft: Option<RaftReport>,
    pub warnings: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct FeatureReport {
    pub marker: u8,
    pub required: u8,
    pub effective: u8,
    pub max: u8,
}

#[derive(Serialize)]
pub(crate) struct ObjectReport {
    pub generation: u64,
    pub root_hash: String,
    pub snapshot_sha256: String,
    pub runstate_sha256: String,
    pub table_counts: BTreeMap<String, u64>,
}

#[derive(Serialize)]
pub(crate) struct LocalReport {
    pub uploads: u64,
    pub audit: u64,
    pub events: u64,
    pub repls: u64,
    pub write_intents: u64,
    pub delete_intents: u64,
    pub cursors: u64,
    pub dead: u64,
    pub usage: u64,
    pub instant: u64,
}

#[derive(Serialize)]
pub(crate) struct RaftReport {
    pub entries: u64,
    pub vote: bool,
    pub committed: bool,
    pub last_purged: Option<u64>,
    pub snapshot: bool,
    pub snapshot_sha256: Option<String>,
    pub entries_sha256: Option<String>,
    pub source_sha256: Option<String>,
}

pub async fn run_cli(
    from: &Path,
    state_dir: &Path,
    raft_db: &Path,
    dry_run: bool,
    json: bool,
) -> Result<i32, BoxError> {
    let report = run(from, state_dir, raft_db, dry_run).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_text(&report);
    }
    Ok(0)
}

pub(crate) async fn run(
    from: &Path,
    state_dir: &Path,
    raft_db: &Path,
    dry_run: bool,
) -> ReportResult {
    let source = source::read_control(from, raft_db).await?;
    let (snapshot_sha, runstate_sha) = source_hashes(&source)?;
    let warnings = source_warnings(&source);
    let feature = FeatureReport {
        marker: source.feature_marker,
        required: required_feature(&source),
        effective: effective_feature(&source),
        max: crate::raft::compat::MAX_FEATURE_LEVEL,
    };
    let mut report = Report {
        command: "control-migrate",
        dry_run,
        status: if dry_run { "dry-run" } else { "applied" },
        source: from.display().to_string(),
        source_sha256: source.checksum.clone(),
        state_dir: state_dir.display().to_string(),
        raft_db: raft_db.display().to_string(),
        mode: format!("{:?}", source.mode).to_lowercase(),
        feature,
        schema_meta: source.schema_meta.clone(),
        sqlite_table_counts: source.table_counts.clone(),
        control_table_counts: source_control_counts(&source),
        source_snapshot_sha256: snapshot_sha,
        source_runstate_sha256: runstate_sha,
        object: None,
        local: None,
        raft: Some(raft_report(&source)),
        warnings,
    };
    if dry_run {
        report.local = Some(local_report(&source));
        return Ok(report);
    }

    let store = open_store(state_dir).await?;
    let _lock = store
        .lock_local_control_root()
        .map_err(|e| std::io::Error::other(format!("root lock: {e}")))?;
    let local_dir = crate::control_backend::local_state_dir(state_dir);
    let control = ObjectControlStore::new(std::sync::Arc::new(RootIo::new(store)))
        .with_local_dir(&local_dir)?;

    let existing = control.root.load().await?;
    if existing.is_some() {
        report.status = "already-applied";
        let object = export::verify(&control, &source).await?;
        report.object = Some(object_report(object));
    }

    let local = local_sink::apply(&control, &source)?;
    report.warnings.extend(local.warnings.clone());
    report.local = Some(local_report_stats(&local));
    let _raft = raft_sink::apply(raft_db, &source).await?;

    if existing.is_none() {
        object_install::install(&control, &source).await?;
        let object = export::verify(&control, &source).await?;
        report.object = Some(object_report(object));
    }

    let local_check = local_sink::verify(&control, &source)?;
    report.local = Some(local_report_stats(&local_check));
    let raft_check = raft_sink::verify(raft_db, &source).await?;
    report.raft = Some(RaftReport {
        entries: raft_check.entries,
        vote: raft_check.vote,
        committed: raft_check.committed,
        last_purged: raft_check.last_purged,
        snapshot: raft_check.snapshot,
        snapshot_sha256: raft_check.snapshot_sha256,
        entries_sha256: raft_check.entries_sha256,
        source_sha256: raft_check.source_checksum,
    });

    let after = source::checksum(from)?;
    if after != source.checksum {
        return error(format!(
            "source database checksum changed during apply: {} -> {after}",
            source.checksum
        ));
    }
    if let Some(before) = &source.raft_checksum
        && source::checksum(raft_db)? != *before
    {
        return error("raft source database checksum changed during apply");
    }
    Ok(report)
}

async fn open_store(root: &Path) -> Result<std::sync::Arc<Store>, BoxError> {
    std::fs::create_dir_all(root)?;
    let mut cfg = StoreConfig::new(root);
    cfg.fsync = true;
    Ok(std::sync::Arc::new(Store::open(cfg).await?))
}

fn source_hashes(source: &Source) -> Result<(String, String), BoxError> {
    let level = effective_feature(source);
    let data = data::encode_snapshot_at(&source.data, level)
        .map_err(|e| std::io::Error::other(format!("source control export: {e}")))?;
    let runstate = runstate::encode_runstate_snapshot(&source.data, &source.runstate, level)
        .map_err(|e| std::io::Error::other(format!("source runstate export: {e}")))?;
    Ok((raft_source::sha256(&data), raft_source::sha256(&runstate)))
}

fn source_control_counts(source: &Source) -> BTreeMap<String, u64> {
    crate::control_store_obj::TableId::ALL
        .iter()
        .map(|t| {
            let count = source
                .table_counts
                .get(t.name())
                .copied()
                .unwrap_or(0)
                .max(0);
            (t.name().to_string(), count as u64)
        })
        .collect()
}

fn source_warnings(source: &Source) -> Vec<String> {
    let dropped = source
        .local
        .audit
        .iter()
        .filter(|r| r.principal_account.is_some() || r.target_account.is_some())
        .count();
    if dropped == 0 {
        Vec::new()
    } else {
        vec![format!(
            "{dropped} audit_log rows carry principal_account/target_account, which have no local-store field: dropped"
        )]
    }
}

fn local_report(source: &Source) -> LocalReport {
    LocalReport {
        uploads: source.local.uploads.len() as u64,
        audit: source.local.audit.len() as u64,
        events: source.local.events.len() as u64,
        repls: source.local.repls.len() as u64,
        write_intents: source.local.write_intents.len() as u64,
        delete_intents: source.local.delete_intents.len() as u64,
        cursors: source.local.cursors.len() as u64,
        dead: source.local.dead.len() as u64,
        usage: source.local.usage.len() as u64,
        instant: source.local.instant.len() as u64,
    }
}

fn local_report_stats(stats: &local_sink::LocalStats) -> LocalReport {
    LocalReport {
        uploads: stats.uploads,
        audit: stats.audit,
        events: stats.events,
        repls: stats.repls,
        write_intents: stats.write_intents,
        delete_intents: stats.delete_intents,
        cursors: stats.cursors,
        dead: stats.dead,
        usage: stats.usage,
        instant: stats.instant,
    }
}

fn raft_report(source: &Source) -> RaftReport {
    let raft = &source.raft;
    RaftReport {
        entries: raft.entries.len() as u64,
        vote: raft.vote.is_some(),
        committed: raft.committed.is_some(),
        last_purged: raft.last_purged.map(|l| l.index),
        snapshot: raft.snapshot.is_some(),
        snapshot_sha256: raft.snapshot_sha256.clone(),
        entries_sha256: raft.entries_sha256.clone(),
        source_sha256: raft.checksum.clone(),
    }
}

fn object_report(verify: export::ObjectVerify) -> ObjectReport {
    ObjectReport {
        generation: verify.generation,
        root_hash: verify.root_hash,
        snapshot_sha256: verify.snapshot_sha256,
        runstate_sha256: verify.runstate_sha256,
        table_counts: verify.table_counts,
    }
}

fn print_text(report: &Report) {
    println!(
        "control-migrate: {} (mode {}, feature marker/required/effective {}/{}/{})",
        report.status,
        report.mode,
        report.feature.marker,
        report.feature.required,
        report.feature.effective
    );
    println!(
        "source {} sha256={} tables={}",
        report.source,
        report.source_sha256,
        report.sqlite_table_counts.len()
    );
    println!("state dir {}  raft db {}", report.state_dir, report.raft_db);
    for (name, count) in &report.control_table_counts {
        println!("  table {name}: {count}");
    }
    if let Some(local) = &report.local {
        println!(
            "  local uploads={} audit={} events={} repls={} sync={}+{} cursors={} dead={} usage={} instant={}",
            local.uploads,
            local.audit,
            local.events,
            local.repls,
            local.write_intents,
            local.delete_intents,
            local.cursors,
            local.dead,
            local.usage,
            local.instant
        );
    }
    if let Some(raft) = &report.raft {
        println!(
            "  raft entries={} vote={} committed={} purged={:?} snapshot={}",
            raft.entries, raft.vote, raft.committed, raft.last_purged, raft.snapshot
        );
    }
    if let Some(object) = &report.object {
        println!(
            "  object generation={} root={} snapshot={} runstate={}",
            object.generation, object.root_hash, object.snapshot_sha256, object.runstate_sha256
        );
    }
    for warning in &report.warnings {
        println!("  warning: {warning}");
    }
}
