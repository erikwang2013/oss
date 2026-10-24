//! M8 S4b：Transition sweep、Restore reaper 与启动对账（executor 见 `tier_jobs`）。
//!
//! M8-1：Raft 模式下 sweep/reaper/启动对账只在 leader 跑（[`crate::raft::LeaderGate`]）；
//! `tier_restores`/作业表仍是节点本地表，门控只防重复执行，不复制运行态（backlog §4.3）。

use std::sync::Arc;

use xl_s3::lifecycle::{LifecycleFilter, LifecycleRuleSpec, LifecycleStore};
use xl_s3::restore::RestoreSink;
use xl_store::meta::{ObjectTag, TierPhase, is_archive_class};
use xl_store::store::{Store, VersionEntry};

use crate::db::{Db, JobSpec, NewJobTask};
use crate::raft::FencingEpoch;
use crate::tier_jobs::TransitionPayload;

fn age_reached(mod_time_nanos: i64, days: u32, now: i64) -> bool {
    // 秒粒度比较：now 是整秒，纳秒 mtime 直接比会让「同秒写入」的 0 天规则漏扫。
    now - mod_time_nanos.div_euclid(1_000_000_000) >= i64::from(days) * 86_400
}

/// Days 从 `clock_nanos` 起算；Date = 执行日之后清掉当日前已存在的版本。
fn due(days: Option<u32>, date: Option<i64>, clock_nanos: i64, now: i64) -> bool {
    match (days, date) {
        (Some(days), _) => age_reached(clock_nanos, days, now),
        (None, Some(date)) => now >= date && clock_nanos.div_euclid(1_000_000_000) < date,
        _ => false,
    }
}

/// Filter 需要读对象元数据（标签/大小）才有判定依据。
fn needs_meta(f: &LifecycleFilter) -> bool {
    !f.tags.is_empty() || f.size_gt.is_some() || f.size_lt.is_some()
}

/// 具体度排序：谓词多者优先，再比前缀长度（S3 most-specific 口径）。
fn specificity(spec: &LifecycleRuleSpec) -> (usize, usize) {
    (spec.filter.predicate_count(), spec.prefix().len())
}

/// 某版本的当前元数据；并发删除/墓碑 → None（跳过该版本）。
async fn fresh_version_meta(
    store: &Store,
    bucket: &str,
    e: &VersionEntry,
    current: bool,
) -> Result<Option<(u64, Vec<ObjectTag>)>, xl_store::error::Error> {
    if e.is_marker {
        return Ok(None);
    }
    let r = match store
        .head_object_version(bucket, &e.key, &e.version_id)
        .await
    {
        Ok(info) => Ok(info),
        // 非版本化条目的 version_id 是 "null"，个别路径按 key 头读。
        Err(xl_store::error::Error::NoSuchKey(_)) if current => {
            store.head_object(bucket, &e.key).await
        }
        Err(err) => Err(err),
    };
    match r {
        Ok(info) => Ok(Some((info.size, info.tags))),
        Err(xl_store::error::Error::NoSuchKey(_) | xl_store::error::Error::DeleteMarker(_)) => {
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

/// 每桶按规则入队 Transition（单次最多检查 10000 个版本；返回新入队任务数）。
///
/// 当前版本按 `transitions`、noncurrent 按 `noncurrent_transitions` 各取**最具体**规则；
/// 同一规则多条动作同时到期时取阈值最大的那条（离线跨过多个档位）。noncurrent 的
/// 新近程度以链上**更新版本**的创建时间为钟，`NewerNoncurrentVersions` 保留最新 N 个。
pub async fn sweep_transitions(
    db: &Db,
    store: &Store,
    lifecycle: &dyn LifecycleStore,
) -> Result<usize, String> {
    sweep_transitions_at(db, store, lifecycle, crate::db::now()).await
}

/// [`sweep_transitions`] 的注入时刻版本（集成测试模拟未来到期）。
pub async fn sweep_transitions_at(
    db: &Db,
    store: &Store,
    lifecycle: &dyn LifecycleStore,
    now: i64,
) -> Result<usize, String> {
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
            .filter(|r| {
                r.enabled && (!r.transitions.is_empty() || !r.noncurrent_transitions.is_empty())
            })
            .collect();
        if active.is_empty() {
            continue;
        }
        let mut key_marker: Option<String> = None;
        let mut vid_marker: Option<String> = None;
        // 同一 key 内版本新→旧；rank=0 为当前版本，>0 为第 N 个 noncurrent。
        let mut last_key: Option<String> = None;
        let mut rank = 0usize;
        let mut newer_nanos = 0i64;
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
                if last_key.as_deref() != Some(e.key.as_str()) {
                    last_key = Some(e.key.clone());
                    rank = 0;
                    newer_nanos = 0;
                } else {
                    rank += 1;
                }
                let clock = if rank == 0 {
                    e.mod_time_nanos
                } else {
                    newer_nanos
                };
                newer_nanos = e.mod_time_nanos;

                if e.is_marker
                    || e.encrypted
                    || e.tier_phase.is_some()
                    || matches!(e.replication_status.as_deref(), Some("PENDING" | "FAILED"))
                    || e.storage_class.as_deref().is_some_and(is_archive_class)
                {
                    continue;
                }
                // 前缀粗筛 + 元数据懒加载（标签/大小过滤）。
                let candidates: Vec<_> = active
                    .iter()
                    .copied()
                    .filter(|r| e.key.starts_with(r.prefix()))
                    .filter(|r| {
                        if rank == 0 {
                            !r.transitions.is_empty()
                        } else {
                            !r.noncurrent_transitions.is_empty()
                        }
                    })
                    .collect();
                if candidates.is_empty() {
                    continue;
                }
                let mut meta: Option<Option<(u64, Vec<ObjectTag>)>> = None;
                if candidates.iter().any(|r| needs_meta(&r.filter)) {
                    meta = Some(fresh_version_meta(store, &bucket, e, rank == 0).await.map_err(
                        |err| {
                            tracing::warn!(error = %err, bucket, key = %e.key, "tier sweep：元数据读取失败");
                            err.to_string()
                        },
                    )?);
                }
                if matches!(meta, Some(None)) {
                    continue; // 版本已被并发删除
                }
                let size = meta
                    .as_ref()
                    .and_then(|m| m.as_ref())
                    .map(|(s, _)| *s)
                    .unwrap_or(e.size);
                let tags: &[ObjectTag] = meta
                    .as_ref()
                    .and_then(|m| m.as_ref())
                    .map(|(_, t)| t.as_slice())
                    .unwrap_or(&[]);
                let Some(rule) = candidates
                    .into_iter()
                    .filter(|r| r.filter.matches(&e.key, size, tags))
                    .max_by_key(|r| specificity(r))
                else {
                    continue;
                };
                // 选到期动作：同规则多条同时到期取阈值最大者（最后一条）。
                let mut chosen: Option<(String, Option<String>)> = None;
                if rank == 0 {
                    for t in &rule.transitions {
                        if due(t.days, t.date, clock, now) {
                            chosen = Some((t.class.clone(), t.tier.clone()));
                        }
                    }
                } else {
                    for t in &rule.noncurrent_transitions {
                        if rank as u32 <= t.newer.unwrap_or(0) {
                            continue; // 保留最新 N 个 noncurrent
                        }
                        if due(Some(t.days), None, clock, now) {
                            chosen = Some((t.class.clone(), t.tier.clone()));
                        }
                    }
                }
                let Some((class, tier)) = chosen else {
                    continue;
                };
                if e.storage_class.as_deref() == Some(class.as_str()) {
                    continue;
                }
                let payload = serde_json::to_string(&TransitionPayload {
                    bucket: bucket.clone(),
                    key: e.key.clone(),
                    version_id: e.version_id.clone(),
                    class: class.clone(),
                    tier: tier.clone(),
                })
                .map_err(|err| err.to_string())?;
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

/// 单节点/M7 入口：不带 fencing token（见 [`reap_once_fenced`]）。
pub async fn reap_once(db: &Db, store: &Store) -> Result<usize, String> {
    reap_once_fenced(db, store, FencingEpoch::UNFENCED).await
}

/// 过期回收：以 Version 的 phase/expiry 为准修正/清理 `tier_restores` 行。
pub async fn reap_once_fenced(
    db: &Db,
    store: &Store,
    fence: FencingEpoch,
) -> Result<usize, String> {
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
                            let _ = db
                                .delete_restore_fenced(&r.bucket, &r.key, &r.version_id, fence)
                                .await;
                            n += 1;
                        }
                        Ok(false) => {
                            if let Some(e) = version_expiry {
                                let _ = db
                                    .set_restore_state_fenced(
                                        &r.bucket,
                                        &r.key,
                                        &r.version_id,
                                        "restored",
                                        e,
                                        fence,
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
                        .set_restore_state_fenced(
                            &r.bucket,
                            &r.key,
                            &r.version_id,
                            "restored",
                            e,
                            fence,
                        )
                        .await;
                }
            }
            Some(TierPhase::Restoring) => {
                // 恢复进行中：行回到 pending，交给作业/对账。
                let _ = db
                    .set_restore_state_fenced(&r.bucket, &r.key, &r.version_id, "pending", 0, fence)
                    .await;
            }
            Some(TierPhase::Archived) | None => {
                // 孤儿行：版本已归档/无 tier/已删除。
                let _ = db
                    .delete_restore_fenced(&r.bucket, &r.key, &r.version_id, fence)
                    .await;
                n += 1;
            }
        }
    }
    Ok(n)
}

/// 单节点/M7 入口：不带 fencing token（见 [`reconcile_restores_fenced`]）。
pub async fn reconcile_restores(
    db: &Db,
    store: &Store,
    sink: &dyn RestoreSink,
) -> Result<usize, String> {
    reconcile_restores_fenced(db, store, sink, FencingEpoch::UNFENCED).await
}

/// 启动对账 + 重排恢复。返回处理条数。
pub async fn reconcile_restores_fenced(
    db: &Db,
    store: &Store,
    sink: &dyn RestoreSink,
    fence: FencingEpoch,
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
                let _ = db
                    .delete_restore_fenced(&r.bucket, &r.key, &r.version_id, fence)
                    .await;
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
/// M8-1：对账与周期任务都只在 leader 执行。
pub fn spawn(
    db: Arc<Db>,
    store: Arc<Store>,
    lifecycle: Arc<dyn LifecycleStore>,
    sink: Arc<dyn RestoreSink>,
    gate: crate::raft::LeaderGate,
) {
    let reconcile_db = Arc::clone(&db);
    let reconcile_store = Arc::clone(&store);
    let reconcile_gate = gate.clone();
    tokio::spawn(async move {
        // 对账也是单例写：等到本节点成为 leader 再做（unknown 一直等）；写状态带当时的
        // token，等待期间又换届则放弃（交给新 leader 重新对账）。
        let fence = reconcile_gate.wait_until_leader().await;
        if !reconcile_gate.still_current(fence) {
            return;
        }
        match reconcile_restores_fenced(&reconcile_db, &reconcile_store, sink.as_ref(), fence).await
        {
            Ok(n) if n > 0 => tracing::info!(n, "restore 启动对账完成"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "restore 启动对账失败"),
        }
    });
    let sweep_secs = env_secs("OSS_TIER_SWEEP_INTERVAL_SECS", 3600);
    if sweep_secs > 0 {
        let db = Arc::clone(&db);
        let store = Arc::clone(&store);
        let gate = gate.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(std::time::Duration::from_secs(sweep_secs));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                t.tick().await;
                if !gate.is_leader() {
                    continue;
                }
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
                let Some(fence) = gate.epoch() else {
                    continue;
                };
                match reap_once_fenced(&db, &store, fence).await {
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
