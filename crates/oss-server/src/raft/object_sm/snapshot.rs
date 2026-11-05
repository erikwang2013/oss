//! Snapshot build/install for the object-backed state machine (F1).
//!
//! Install keeps SQLite's exact one-way gates and install order, then stages the
//! whole new tree in one root CAS (design §2.4/§9): rows, runstate markers,
//! applied log + membership and the feature level switch together. A crash
//! before the CAS leaves the old root; after it, the new one. Local upload
//! owner cleanup (D3) runs after the commit.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::atomic::Ordering;

use openraft::{BasicNode, ErrorSubject, ErrorVerb, Snapshot, SnapshotMeta, StorageError};
use xl_store::control_state::root::AppliedLog;
use xl_store::control_store::account::AccountTxn;
use xl_store::control_store::iam::{IamStore, IamTxn};

use super::super::TypeConfig;
use super::super::compat;
use super::super::data::{self, InstallCtx};
use super::super::runstate;
use super::super::store::storage_err;
use super::{
    META_APPLIED_LOG_ID, ObjectStateMachine, activation_of, applied_of, data_err,
    scrub_local_uploads, sm_err, store_err,
};
use crate::control_store_obj::WriteOrigin;

pub(super) async fn install(
    sm: &mut ObjectStateMachine,
    meta: &SnapshotMeta<u64, BasicNode>,
    snapshot: Box<Cursor<Vec<u8>>>,
) -> Result<(), StorageError<u64>> {
    let bytes = snapshot.get_ref().clone();
    if bytes.len() as u64 > sm.max_snapshot_bytes {
        return Err(storage_err(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            std::io::Error::other(format!(
                "snapshot {} bytes exceeds limit {} (raft.snapshot_max_bytes)",
                bytes.len(),
                sm.max_snapshot_bytes
            )),
        ));
    }
    if !data::is_control_snapshot(&bytes) {
        return Err(storage_err(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            std::io::Error::other(
                "control state machine requires OSSCTL1/OSSCTL2/OSSCTL3 snapshot \
                 (legacy KV snapshot rejected)",
            ),
        ));
    }
    let snapshot = data::decode_snapshot(&bytes).map_err(data_err)?;
    // One-way doors: a level-3 activation/canonical rows must not be erased
    // by a lower-level install, and a level-9 ledger must not be wiped.
    let local_activation = sm.activation.load(Ordering::Relaxed);
    if snapshot.feature_level < compat::FEATURE_LEVEL_3
        || snapshot.feature_level < compat::FEATURE_LEVEL_9
    {
        let mut txn = sm
            .store
            .begin(WriteOrigin::snapshot_install())
            .await
            .map_err(|e| sm_err(format!("control root: {e}")))?;
        if snapshot.feature_level < compat::FEATURE_LEVEL_3 {
            let canonical = txn
                .meta()
                .markers
                .contains_key(crate::db::META_ACCOUNTS_CANONICAL);
            let canonical_rows = if canonical {
                let (accounts, members) = txn.account_counts().await.map_err(sm_err)?;
                accounts + members
            } else {
                0
            };
            if local_activation >= compat::FEATURE_LEVEL_3 || canonical_rows != 0 {
                return Err(data_err(data::DataError::Format(format!(
                    "node activation level {local_activation} (canonical accounts: {}, rows: \
                     {canonical_rows}) cannot install a level-{} snapshot",
                    canonical, snapshot.feature_level
                ))));
            }
        }
        if snapshot.feature_level < compat::FEATURE_LEVEL_9 {
            let ledger_rows = txn.account_ledger_count().await.map_err(sm_err)?;
            if ledger_rows != 0 {
                return Err(data_err(data::DataError::Format(format!(
                    "cannot install a level-{} snapshot: local account_usage_ledger has \
                     {ledger_rows} rows (level 9 required)",
                    snapshot.feature_level
                ))));
            }
        }
    }
    // Only `m8.runstate` is understood; unknown extensions fail closed.
    let mut runstate = None;
    for (name, ext) in &snapshot.extensions {
        if name == runstate::RUNSTATE_EXTENSION {
            runstate = Some(
                runstate::from_snapshot_extension(ext, runstate::RUNSTATE_EXTENSION_VERSION)
                    .map_err(data_err)?,
            );
        } else {
            return Err(data_err(data::DataError::Format(format!(
                "snapshot carries unsupported extension {name:?}; refusing to install and \
                 drop it"
            ))));
        }
    }
    let runstate = runstate.unwrap_or_default();
    let feature = snapshot.feature_level;
    if ((compat::FEATURE_LEVEL_3..compat::FEATURE_LEVEL_4).contains(&feature)
        && sm.feature_level >= compat::FEATURE_LEVEL_4
        && (!runstate.jobs.is_empty()
            || !runstate.job_tasks.is_empty()
            || !runstate.inventory_runs.is_empty()
            || !runstate.inventory_cursors.is_empty()
            || !runstate.inventory_incremental.is_empty()
            || !runstate.tier_restores.is_empty()))
        || ((compat::FEATURE_LEVEL_3..compat::FEATURE_LEVEL_5).contains(&feature)
            && sm.feature_level >= compat::FEATURE_LEVEL_5
            && (!runstate.inventory_runs.is_empty()
                || !runstate.inventory_cursors.is_empty()
                || !runstate.inventory_incremental.is_empty()))
    {
        return Err(data_err(data::DataError::Format(format!(
            "[raft].feature_level={} cannot install a level-{feature} snapshot carrying \
             non-log runstate rows; finish activation on the source or drain/clear and \
             re-export",
            sm.feature_level
        ))));
    }
    let canonical = feature >= compat::FEATURE_LEVEL_3;
    let applied = meta.last_log_id.map(|l| AppliedLog {
        term: l.leader_id.term,
        index: l.index,
    });
    let membership_json = serde_json::to_string(&meta.last_membership)
        .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
    let applied_json = meta
        .last_log_id
        .map(|l| serde_json::to_string(&l))
        .transpose()
        .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
    let data_snapshot = snapshot.data.clone();
    let runstate = runstate.clone();
    let _guard = sm.snapshot_lock.lock().await;
    sm.store
        .transaction(WriteOrigin::snapshot_install(), |txn| {
            let membership_json = membership_json.clone();
            let applied_json = applied_json.clone();
            let data_snapshot = data_snapshot.clone();
            let runstate = runstate.clone();
            Box::pin(async move {
                let admin_id = txn.admin_id().await.map_err(store_err)?;
                let ctx = InstallCtx {
                    admin_id,
                    witness: false,
                    canonical,
                };
                data::install_rows(txn, &data_snapshot, &ctx)
                    .await
                    .map_err(store_err)?;
                if feature >= compat::FEATURE_LEVEL_3 {
                    runstate::install_runstate_rows(txn, &runstate)
                        .await
                        .map_err(store_err)?;
                    // A marker only certifies a level-4/5 source; plain
                    // level-3/4 installs revoke it.
                    if feature >= compat::FEATURE_LEVEL_4 {
                        txn.set_marker(compat::META_RUNSTATE_ACTIVATED, "1");
                    } else {
                        txn.remove_marker(compat::META_RUNSTATE_ACTIVATED);
                    }
                    if feature >= compat::FEATURE_LEVEL_5 {
                        txn.set_marker(compat::META_INVENTORY_ACTIVATED, "1");
                    } else {
                        txn.remove_marker(compat::META_INVENTORY_ACTIVATED);
                    }
                }
                if feature > compat::MIN_FEATURE_LEVEL && feature > activation_of(txn.meta()) {
                    txn.meta_mut().feature_level = feature as u16;
                }
                txn.set_membership(Some(membership_json));
                txn.set_applied(applied);
                match &applied_json {
                    Some(json) => txn.set_marker(META_APPLIED_LOG_ID, json),
                    None => txn.remove_marker(META_APPLIED_LOG_ID),
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
    sm.activation.fetch_max(feature, Ordering::Relaxed);
    if let Some(tx) = &sm.install_notify {
        let _ = tx.send(meta.last_log_id.map(|l| l.index).unwrap_or(0));
    }
    // SQLite clears non-admin upload owners inside the install statement;
    // local uploads live outside the root CAS, so do it after the commit.
    let admins: BTreeSet<i64> = sm
        .store
        .users()
        .await
        .map_err(sm_err)?
        .into_iter()
        .filter(|u| u.is_admin)
        .map(|u| u.uid)
        .collect();
    scrub_local_uploads(&sm.store, |owner| {
        owner.is_some_and(|uid| !admins.contains(&uid))
    })?;
    // Root switch is the install commit; the snapshot file only feeds future
    // replication. A crash in between leaves the new root with an older snapshot
    // (normal lag), never the reverse.
    sm.snapshots.write(meta, &bytes).await?;
    Ok(())
}

pub(super) async fn build(
    sm: &mut ObjectStateMachine,
) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
    let mut txn = sm
        .store
        .begin(WriteOrigin::snapshot_install())
        .await
        .map_err(|e| sm_err(format!("control root: {e}")))?;
    let (applied, membership) = applied_of(txn.meta())?;
    let level = activation_of(txn.meta());
    let canonical = txn
        .meta()
        .markers
        .contains_key(crate::db::META_ACCOUNTS_CANONICAL);
    let control = data::read_txn(&mut txn, canonical).await.map_err(sm_err)?;
    let runstate = if level >= compat::FEATURE_LEVEL_3 {
        Some(
            runstate::read_runstate_txn(&mut txn)
                .await
                .map_err(sm_err)?,
        )
    } else {
        None
    };
    let bytes = match &runstate {
        Some(rs) => runstate::encode_runstate_snapshot(&control, rs, level),
        None => data::encode_snapshot_at(&control, level),
    }
    .map_err(data_err)?;
    if bytes.len() as u64 > sm.max_snapshot_bytes {
        return Err(storage_err(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            std::io::Error::other(format!(
                "snapshot {} bytes exceeds limit {} (raft.snapshot_max_bytes)",
                bytes.len(),
                sm.max_snapshot_bytes
            )),
        ));
    }
    let snapshot_id = format!(
        "{}-{}",
        applied.map(|l| l.index).unwrap_or(0),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );
    let meta = SnapshotMeta {
        last_log_id: applied,
        last_membership: membership,
        snapshot_id,
    };
    sm.snapshots.write(&meta, &bytes).await?;
    Ok(Snapshot {
        meta,
        snapshot: Box::new(Cursor::new(bytes)),
    })
}
