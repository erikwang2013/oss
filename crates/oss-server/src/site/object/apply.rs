//! Object-backend M7 apply path (no SQLite).
//!
//! Apply reuses the object transaction (one root CAS, `WriteOrigin::SiteApply`
//! so hooks stay quiet) and writes the LWW clock explicitly. Permanent bad
//! entries go to the node-local dead file after the CAS commits; node-local
//! writes cannot join the root transaction.

use std::collections::BTreeSet;

use xl_s3::bucket_cleanup::BucketCleanupSink;
use xl_store::control_store::bucket::BucketStore;
use xl_store::control_store::sites::SiteTxn;
use xl_store::store::Store;

use crate::control_store_obj::{ObjectControlStore, ObjectControlTxn, WriteOrigin};
use crate::local_store::DeadLetter;

use super::super::{
    ApplyStats, CLOCK_SKEW_SECS, Entry, MAX_DEAD_PAYLOAD, MAX_ENTRIES, MAX_PAYLOAD, bad, wins,
};
use super::entity::{
    apply_access_key, apply_bucket, apply_permission, apply_policy, apply_user, read_clock,
};

type ApplyResult<T> = Result<T, String>;

fn dead_payload(e: &Entry) -> String {
    let payload = e.payload.to_string();
    // UTF-8 boundary, matching the SQLite bury truncation.
    let mut cut = payload.len().min(MAX_DEAD_PAYLOAD);
    while cut > 0 && !payload.is_char_boundary(cut) {
        cut -= 1;
    }
    payload[..cut].to_string()
}

async fn dead(
    txn: &mut ObjectControlTxn,
    pending: &mut Vec<DeadLetter>,
    e: &Entry,
    why: &str,
) -> ApplyResult<()> {
    pending.push(DeadLetter {
        peer_id: e.origin_site_id.clone(),
        seq: e.seq,
        entity: e.entity.clone(),
        entity_id: e.entity_id.clone(),
        error: why.into(),
        payload: dead_payload(e),
        created_at: crate::db::now(),
    });
    txn.site_set_last_error_for_remote(&e.origin_site_id, Some(why))
        .await
        .map_err(|err| bad(err.to_string()))
}

/// Apply one batch. `Err` = caller returns 500 and the peer retries the batch.
pub(crate) async fn apply_changelog(
    control: &ObjectControlStore,
    store: &Store,
    site_id: &str,
    cleanup: &dyn BucketCleanupSink,
    entries: &[Entry],
) -> ApplyResult<ApplyStats> {
    if entries.len() > MAX_ENTRIES {
        return Err(bad(format!("批次超过 {MAX_ENTRIES} 条")));
    }
    let origin = entries
        .first()
        .map(|e| e.origin_site_id.as_str())
        .unwrap_or("");
    let mut txn = control
        .begin(WriteOrigin::site_apply(origin))
        .await
        .map_err(|err| bad(err.to_string()))?;
    let now = crate::db::now();
    let mut stats = ApplyStats::default();
    let mut dead_rows: Vec<DeadLetter> = Vec::new();
    let mut bucket_entities: BTreeSet<String> = BTreeSet::new();
    let mut deleted_buckets: Vec<String> = Vec::new();
    let mut deleted_uids: Vec<i64> = Vec::new();

    for e in entries {
        if e.payload.to_string().len() > MAX_PAYLOAD {
            dead(&mut txn, &mut dead_rows, e, "payload 超限").await?;
            stats.dead += 1;
            continue;
        }
        if e.origin_site_id == site_id {
            stats.skipped += 1;
            continue;
        }
        if e.ts > now + CLOCK_SKEW_SECS {
            dead(&mut txn, &mut dead_rows, e, "ts 超时钟偏移上限").await?;
            stats.dead += 1;
            continue;
        }
        if !matches!(e.op.as_str(), "upsert" | "delete") {
            dead(&mut txn, &mut dead_rows, e, "未知 op").await?;
            stats.dead += 1;
            continue;
        }
        let clock = read_clock(&txn, &e.entity, &e.entity_id).await?;
        if e.entity == "bucket" {
            bucket_entities.insert(e.entity_id.clone());
        }
        if !wins(e, clock) {
            stats.skipped += 1;
            continue;
        }
        let applied = match e.entity.as_str() {
            "bucket" => apply_bucket(&mut txn, store, e).await?,
            "bucket_policy" => apply_policy(&mut txn, e).await?,
            "permission" => apply_permission(&mut txn, e).await?,
            "user" => apply_user(&mut txn, e, &mut deleted_uids).await?,
            "access_key" => apply_access_key(&mut txn, e).await?,
            other => {
                dead(
                    &mut txn,
                    &mut dead_rows,
                    e,
                    &format!("未知 entity：{other}"),
                )
                .await?;
                stats.dead += 1;
                continue;
            }
        };
        match applied {
            Ok(()) => {
                if e.entity == "bucket" && e.op == "delete" {
                    deleted_buckets.push(e.entity_id.clone());
                }
                stats.applied += 1;
            }
            Err(why) => {
                dead(&mut txn, &mut dead_rows, e, &why).await?;
                stats.dead += 1;
                continue;
            }
        }
        txn.site_put_clock(
            &e.entity,
            &e.entity_id,
            e.ts,
            e.priority,
            &e.origin_site_id,
            e.seq,
        )
        .await
        .map_err(|err| bad(err.to_string()))?;
    }
    txn.commit().await.map_err(|err| bad(err.to_string()))?;

    if !dead_rows.is_empty() {
        let local = control.local_store().map_err(|err| bad(err.to_string()))?;
        for row in &dead_rows {
            local
                .dead(&row.peer_id)
                .map_err(|err| bad(err.to_string()))?
                .append(row)
                .map_err(|err| bad(err.to_string()))?;
        }
    }
    if !deleted_uids.is_empty() {
        // SQLite clears upload owners in the same statement batch; local files
        // cannot join the root CAS, so best-effort after commit.
        match clear_upload_owners(control, &deleted_uids) {
            Ok(()) => {}
            Err(err) => tracing::warn!(error = %err, "site apply：清理上传属主失败"),
        }
    }
    reconcile(
        store,
        control,
        cleanup,
        &bucket_entities,
        &deleted_buckets,
        stats,
    )
    .await
}

async fn reconcile(
    store: &Store,
    control: &ObjectControlStore,
    cleanup: &dyn BucketCleanupSink,
    bucket_entities: &BTreeSet<String>,
    deleted_buckets: &[String],
    stats: ApplyStats,
) -> ApplyResult<ApplyStats> {
    for name in deleted_buckets {
        cleanup.invalidate_ap_cache(name);
    }
    for name in bucket_entities {
        if xl_store::layout::validate_bucket(name).is_err() {
            continue;
        }
        let exists = control
            .bucket(name)
            .await
            .map_err(|err| bad(err.to_string()))?
            .is_some();
        if !exists {
            continue;
        }
        match store.bucket_exists(name).await {
            Ok(true) => {}
            Ok(false) => match store.create_bucket(name).await {
                Ok(()) | Err(xl_store::error::Error::BucketAlreadyExists(_)) => {}
                Err(err) => return Err(bad(format!("桶物理对账失败 {name}: {err}"))),
            },
            Err(err) => return Err(bad(format!("桶存在性查询失败 {name}: {err}"))),
        }
        let still = control
            .bucket(name)
            .await
            .map_err(|err| bad(err.to_string()))?
            .is_some();
        if !still
            && store.bucket_exists(name).await.unwrap_or(false)
            && let Err(err) = store.delete_bucket(name).await
        {
            tracing::warn!(error = %err, bucket = %name, "对账 TOCTOU 清理空桶失败（下次兜底）");
        }
    }
    Ok(stats)
}

fn clear_upload_owners(control: &ObjectControlStore, uids: &[i64]) -> ApplyResult<()> {
    let uploads = control
        .local_store()
        .map_err(|err| bad(err.to_string()))?
        .uploads()
        .map_err(|err| bad(err.to_string()))?;
    let mut records = uploads.all().map_err(|err| bad(err.to_string()))?;
    for record in &mut records {
        if record.owner_uid.is_some_and(|uid| uids.contains(&uid)) {
            record.owner_uid = None;
            uploads.put(record).map_err(|err| bad(err.to_string()))?;
        }
    }
    Ok(())
}
