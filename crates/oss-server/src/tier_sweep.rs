//! M8 S4b：Transition sweep、Restore reaper 与启动对账（executor 见 `tier_jobs`）。

use std::sync::Arc;

use xl_s3::lifecycle::LifecycleStore;
use xl_s3::restore::RestoreSink;
use xl_store::meta::{TierPhase, is_archive_class};
use xl_store::store::Store;

use crate::db::{Db, JobSpec, NewJobTask};
use crate::tier_jobs::TransitionPayload;

fn age_reached(mod_time_nanos: i64, days: u32, now: i64) -> bool {
    // 秒粒度比较：now 是整秒，纳秒 mtime 直接比会让「同秒写入」的 0 天规则漏扫。
    now - mod_time_nanos.div_euclid(1_000_000_000) >= i64::from(days) * 86_400
}

/// 每桶按规则入队 Transition（单次最多检查 10000 个版本；返回新入队任务数）。
pub async fn sweep_transitions(
    db: &Db,
    store: &Store,
    lifecycle: &dyn LifecycleStore,
) -> Result<usize, String> {
    let now = crate::db::now();
    let mut examined = 0usize;
    let mut queued = 0usize;
    'buckets: for bucket in db.list_bucket_rows().await.map_err(|e| e.to_string())? {
        let rules = match lifecycle.get_rules(&bucket).await {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(bucket, error = %e, "sweep：读取生命周期规则失败");
                continue;
            }
        };
        let active: Vec<_> = rules
            .iter()
            .filter(|r| r.enabled && r.transition_days.is_some() && r.transition_class.is_some())
            .collect();
        if active.is_empty() {
            continue;
        }
        let mut key_marker: Option<String> = None;
        let mut vid_marker: Option<String> = None;
        loop {
            let page = store
                .list_object_versions(
                    &bucket,
                    "",
                    1000,
                    key_marker.as_deref(),
                    vid_marker.as_deref(),
                )
                .await
                .map_err(|e| e.to_string())?;
            for e in &page.entries {
                examined += 1;
                if examined > 10_000 {
                    break 'buckets;
                }
                // NoncurrentVersionTransition 未支持（501）：只转当前版本。
                if !e.is_latest
                    || e.is_marker
                    || e.encrypted
                    || e.tier_phase.is_some()
                    || matches!(e.replication_status.as_deref(), Some("PENDING" | "FAILED"))
                {
                    continue;
                }
                if e.storage_class.as_deref().is_some_and(is_archive_class) {
                    continue;
                }
                // 重叠规则取最长前缀（S3 的 most-specific 口径）。
                let Some(rule) = active
                    .iter()
                    .filter(|r| e.key.starts_with(&r.prefix))
                    .max_by_key(|r| r.prefix.len())
                else {
                    continue;
                };
                let days = rule.transition_days.expect("active 已过滤");
                let class = rule.transition_class.as_ref().expect("active 已过滤");
                if e.storage_class.as_deref() == Some(class.as_str()) {
                    continue;
                }
                if !age_reached(e.mod_time_nanos, days, now) {
                    continue;
                }
                let payload = serde_json::to_string(&TransitionPayload {
                    bucket: bucket.clone(),
                    key: e.key.clone(),
                    version_id: e.version_id.clone(),
                    class: class.clone(),
                    tier: rule.transition_tier.clone(),
                })
                .map_err(|e| e.to_string())?;
                let spec = JobSpec {
                    kind: "transition".to_string(),
                    bucket: bucket.clone(),
                    prefix: e.key.clone(),
                    tasks: vec![NewJobTask {
                        action: "Transition".to_string(),
                        payload,
                        dedupe_key: Some(format!(
                            "tier:{}:{}:{}:{}",
                            bucket, e.key, e.version_id, class
                        )),
                    }],
                    ..Default::default()
                };
                match db.create_job_dedup(&spec, now).await {
                    Ok(Some(_)) => queued += 1,
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, bucket, key = %e.key, "Transition 入队失败")
                    }
                }
            }
            if !page.is_truncated {
                break;
            }
            key_marker = page.next_key_marker;
            vid_marker = page.next_version_id_marker;
        }
    }
    Ok(queued)
}

/// 过期回收：以 Version 的 phase/expiry 为准修正/清理 `tier_restores` 行。
pub async fn reap_once(db: &Db, store: &Store) -> Result<usize, String> {
    let now = crate::db::now();
    let rows = db
        .restore_rows("restored")
        .await
        .map_err(|e| e.to_string())?;
    let mut n = 0usize;
    for r in rows {
        if r.expires_at == 0 || r.expires_at > now {
            continue;
        }
        let v = match store.tier_version(&r.bucket, &r.key, &r.version_id).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, key = %r.key, "reaper：读版本失败");
                continue;
            }
        };
        let phase = v.as_ref().and_then(|v| v.tier.as_ref()).map(|t| t.phase);
        let version_expiry = v
            .as_ref()
            .and_then(|v| v.tier.as_ref())
            .and_then(|t| t.restore_expires_at);
        match phase {
            Some(TierPhase::Restored) => {
                if version_expiry.is_none_or(|e| e <= now) {
                    match store
                        .expire_restore(&r.bucket, &r.key, &r.version_id, now)
                        .await
                    {
                        Ok(true) => {
                            let _ = db.delete_restore(&r.bucket, &r.key, &r.version_id).await;
                            n += 1;
                        }
                        Ok(false) => {
                            if let Some(e) = version_expiry {
                                let _ = db
                                    .set_restore_state(
                                        &r.bucket,
                                        &r.key,
                                        &r.version_id,
                                        "restored",
                                        e,
                                    )
                                    .await;
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, key = %r.key, "reaper：过期回收失败"),
                    }
                } else if version_expiry != Some(r.expires_at)
                    && let Some(e) = version_expiry
                {
                    // 行过期时刻早于版本：以版本为准修正（不误删）。
                    let _ = db
                        .set_restore_state(&r.bucket, &r.key, &r.version_id, "restored", e)
                        .await;
                }
            }
            Some(TierPhase::Restoring) => {
                // 恢复进行中：行回到 pending，交给作业/对账。
                let _ = db
                    .set_restore_state(&r.bucket, &r.key, &r.version_id, "pending", 0)
                    .await;
            }
            Some(TierPhase::Archived) | None => {
                // 孤儿行：版本已归档/无 tier/已删除。
                let _ = db.delete_restore(&r.bucket, &r.key, &r.version_id).await;
                n += 1;
            }
        }
    }
    Ok(n)
}

/// 启动对账 + 重排恢复。返回处理条数。
pub async fn reconcile_restores(
    db: &Db,
    store: &Store,
    sink: &dyn RestoreSink,
) -> Result<usize, String> {
    let mut n = 0usize;
    // 1) Restoring 版本：按行状态提交/重排/回滚。
    for (bucket, key, vid) in store.scan_tier_pending().await.map_err(|e| e.to_string())? {
        let row = db
            .restore_row(&bucket, &key, &vid)
            .await
            .map_err(|e| e.to_string())?;
        match row {
            Some(r) if r.state == "restored" => {
                if let Err(e) = store
                    .restore_commit(&bucket, &key, &vid, r.expires_at)
                    .await
                {
                    tracing::warn!(error = %e, key, "对账：提交恢复失败，重排");
                    let _ = store.abort_restore(&bucket, &key, &vid).await;
                    let _ = sink.enqueue_restore(&bucket, &key, &vid, 1).await;
                }
                n += 1;
            }
            Some(r) => {
                let active = match r.job_id {
                    Some(j) => db.job_task_active(j).await.unwrap_or(false),
                    None => false,
                };
                if !active {
                    let _ = store.abort_restore(&bucket, &key, &vid).await;
                    if let Err(e) = sink.enqueue_restore(&bucket, &key, &vid, 1).await {
                        tracing::warn!(error = %e, key, "对账：重排恢复失败");
                    }
                    n += 1;
                }
            }
            None => {
                let _ = store.abort_restore(&bucket, &key, &vid).await;
                if let Err(e) = sink.enqueue_restore(&bucket, &key, &vid, 1).await {
                    tracing::warn!(error = %e, key, "对账：重排恢复失败");
                }
                n += 1;
            }
        }
    }
    // 2) 行存在但版本已归档/不存在：孤儿行清理；pending 且无活跃作业 → 重排。
    for state in ["pending", "restored"] {
        for r in db.restore_rows(state).await.map_err(|e| e.to_string())? {
            let v = match store.tier_version(&r.bucket, &r.key, &r.version_id).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            let phase = v.as_ref().and_then(|v| v.tier.as_ref()).map(|t| t.phase);
            if phase.is_none() {
                let _ = db.delete_restore(&r.bucket, &r.key, &r.version_id).await;
            } else if state == "pending" && phase == Some(TierPhase::Archived) {
                let active = match r.job_id {
                    Some(j) => db.job_task_active(j).await.unwrap_or(false),
                    None => false,
                };
                if !active {
                    let _ = sink
                        .enqueue_restore(&r.bucket, &r.key, &r.version_id, 1)
                        .await;
                }
            }
        }
    }
    Ok(n)
}

fn env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// 启动时对账一次，并挂 sweep / reaper 周期任务（`0` = 关闭对应周期）。
pub fn spawn(
    db: Arc<Db>,
    store: Arc<Store>,
    lifecycle: Arc<dyn LifecycleStore>,
    sink: Arc<dyn RestoreSink>,
) {
    let reconcile_db = Arc::clone(&db);
    let reconcile_store = Arc::clone(&store);
    tokio::spawn(async move {
        match reconcile_restores(&reconcile_db, &reconcile_store, sink.as_ref()).await {
            Ok(n) if n > 0 => tracing::info!(n, "restore 启动对账完成"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "restore 启动对账失败"),
        }
    });
    let sweep_secs = env_secs("OSS_TIER_SWEEP_INTERVAL_SECS", 3600);
    if sweep_secs > 0 {
        let db = Arc::clone(&db);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(std::time::Duration::from_secs(sweep_secs));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                t.tick().await;
                match sweep_transitions(&db, &store, lifecycle.as_ref()).await {
                    Ok(n) if n > 0 => tracing::info!(n, "tier sweep：入队 Transition"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "tier sweep 失败"),
                }
            }
        });
    }
    let reaper_secs = env_secs("OSS_TIER_REAPER_INTERVAL_SECS", 300);
    if reaper_secs > 0 {
        let db = Arc::clone(&db);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(std::time::Duration::from_secs(reaper_secs));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                t.tick().await;
                match reap_once(&db, &store).await {
                    Ok(n) if n > 0 => tracing::info!(n, "tier reaper：回收过期恢复"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "tier reaper 失败"),
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::age_reached;

    #[test]
    fn age_threshold() {
        let now = 1_000_000i64;
        assert!(age_reached((now - 86_400) * 1_000_000_000, 1, now));
        assert!(!age_reached((now - 86_399) * 1_000_000_000, 1, now));
        assert!(!age_reached(0, 36_500, now));
    }
}
