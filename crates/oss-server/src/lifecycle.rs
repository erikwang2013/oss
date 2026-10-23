//! 生命周期 expiry sweep：当前版本 Days/Date、ExpiredObjectDeleteMarker、非当前版本过期。
//!
//! 验收口径（设计 ③ + P1/P2）：
//! - 当前版本：`now - last_modified >= Days*86400`；Date = 指定午夜后的首轮清掉当时已存在
//!   的对象（`lmt < date && now >= date`）。删前 `head_object` 复读（标签/大小过滤 + 并发覆盖）。
//! - 版本控制桶：无版本号删除 = 打 **delete marker**；noncurrent 过期/EODM 点名删版本。
//! - EODM：仅删「链上唯一版本且为 delete marker」的墓碑；删前以 2 条窗口复核。
//! - noncurrent：链内新→旧，新近程度以**更新版本**的创建时间为钟；`NewerNoncurrentVersions`
//!   保留最新 N 个 noncurrent；delete marker 作为 noncurrent 版本同样可删（AWS 语义）。
//!   删前复核候选不是链首，避免并发下把刚复活的版本删掉。
//! - 版本级删除**不**入复制队列（M9 复制无「删指定版本」语义，见 report 风险段）。
//! - worker 间隔 `OSS_LIFECYCLE_INTERVAL_SECS`（默认 60；0 = 关闭）。逐规则 best-effort。

use std::time::Duration;

use xl_s3::lifecycle::{ExpirationAction, LifecycleRuleSpec};
use xl_s3::sync_delete::SyncDeleteSink as _;
use xl_store::store::{ListRequest, VersionEntry, VersioningMode};

use crate::db::LifecycleRuleRow;
use crate::state::AppState;

/// 删除结果 → 生命周期事件（`None` = 并发空删，**不发事件**）。
/// 点名删版本（含 EODM 删 marker）带版本号，事件名沿用 AWS 既有集合。
pub(crate) fn lifecycle_event(
    outcome: &xl_store::store::DeleteOutcome,
) -> Option<(&'static str, Option<&str>)> {
    match outcome {
        xl_store::store::DeleteOutcome::Marker { version_id } => Some((
            "s3:LifecycleExpiration:DeleteMarkerCreated",
            Some(version_id.as_str()),
        )),
        xl_store::store::DeleteOutcome::Version { version_id, .. } => {
            Some(("s3:LifecycleExpiration:Delete", Some(version_id.as_str())))
        }
        xl_store::store::DeleteOutcome::Object { .. } => {
            Some(("s3:LifecycleExpiration:Delete", None))
        }
        xl_store::store::DeleteOutcome::NotFound => None,
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn secs(nanos: i64) -> i64 {
    nanos.div_euclid(1_000_000_000)
}

/// 单轮进度计数（单规则跨 pass 汇总）。
#[derive(Default)]
struct Progress {
    scanned: i64,
    deleted: i64,
    skipped: i64,
}

/// noncurrent 是否到期：保留最新 N 个 noncurrent；年龄以**更新版本**的创建时间为钟。
fn noncurrent_due(rank: usize, retain: Option<u32>, newer_nanos: i64, days: u32, now: i64) -> bool {
    rank as u32 > retain.unwrap_or(0) && now - secs(newer_nanos) >= i64::from(days) * 86_400
}

/// 规则包含的过期阶段（同一条规则可同时有当前版本与非当前版本动作）。
#[derive(Clone, Copy, Default)]
struct ExpiryKinds {
    current: bool,
    eodm: bool,
    noncurrent: bool,
}

impl ExpiryKinds {
    fn any(self) -> bool {
        self.current || self.eodm || self.noncurrent
    }
}

fn expiry_kinds(spec: &LifecycleRuleSpec) -> ExpiryKinds {
    ExpiryKinds {
        current: matches!(
            spec.expiration,
            Some(ExpirationAction::Days { .. } | ExpirationAction::Date { .. })
        ),
        eodm: matches!(
            spec.expiration,
            Some(ExpirationAction::ExpiredObjectDeleteMarker)
        ),
        noncurrent: spec.noncurrent_expiration.is_some(),
    }
}

/// 单次扫描：对全部启用规则各跑一轮，逐规则一条 jobs。返回处理的规则数。
pub async fn sweep_once(state: &AppState) -> usize {
    sweep_once_at(state, now_secs()).await
}

/// 单次扫描（注入判定时刻）：集成测试用它模拟「规则到期后」的时间点。
pub async fn sweep_once_at(state: &AppState, now: i64) -> usize {
    let rules = match state.db.enabled_lifecycle_rules().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "lifecycle：规则读取失败，本轮跳过");
            return 0;
        }
    };
    let mut ran = 0usize;
    for row in rules {
        let spec = match crate::lifecycle_store::spec_from_row(&row) {
            Ok(s) => s,
            Err(e) => {
                // 负值防呆：console 已拦 0..=36500；直写 DB 绕过时 fail-closed。
                let msg = match row.expiration_days.or(Some(row.expire_days)) {
                    Some(d) if d < 0 => format!("规则有效期天数为负（{d}），拒绝执行"),
                    _ => format!("规则解析失败，拒绝执行: {e}"),
                };
                record_bad_rule(state, &row, &msg).await;
                ran += 1;
                continue;
            }
        };
        let kinds = expiry_kinds(&spec);
        if !kinds.any() {
            continue; // Transition-only 规则归 tier sweep
        }
        ran += 1;
        run_rule(state, &row.bucket, &spec, kinds, now).await;
    }
    ran
}

/// 无法解析的规则：开单并记 error（可见性优先，不执行任何删除）。
async fn record_bad_rule(state: &AppState, row: &LifecycleRuleRow, msg: &str) {
    let now = now_secs();
    let job = match state
        .db
        .job_start("lifecycle", &row.bucket, &row.prefix, now)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(error = %e, bucket = %row.bucket, "lifecycle：job 开单失败");
            return;
        }
    };
    if let Err(e) = state
        .db
        .job_finish(job, "error", 0, 0, 0, Some(msg), now_secs())
        .await
    {
        tracing::warn!(error = %e, "lifecycle：job 收尾失败");
    }
}

async fn run_rule(
    state: &AppState,
    bucket: &str,
    spec: &LifecycleRuleSpec,
    kinds: ExpiryKinds,
    now: i64,
) {
    let mut progress = Progress::default();
    let mut err: Option<String> = None;
    let started = now_secs();
    let job = match state
        .db
        .job_start("lifecycle", bucket, spec.prefix(), started)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(error = %e, bucket, "lifecycle：job 开单失败");
            return;
        }
    };
    let mode = match state.db.get_bucket_versioning(bucket).await {
        Ok(s) => VersioningMode::from_status(&s),
        Err(e) => {
            err = Some(format!("版本状态读取失败: {e}"));
            VersioningMode::Off
        }
    };
    if err.is_none() {
        if kinds.current {
            err = expire_current(state, bucket, spec, mode, now, &mut progress).await;
        }
        if err.is_none() && kinds.eodm && mode != VersioningMode::Off {
            err = expire_eodm(state, bucket, spec, mode, now, &mut progress).await;
        }
        if err.is_none() && kinds.noncurrent && mode != VersioningMode::Off {
            err = expire_noncurrent(state, bucket, spec, mode, now, &mut progress).await;
        }
    }
    let (st, msg) = match &err {
        None => ("done", None),
        Some(m) => ("error", Some(m.as_str())),
    };
    if let Err(e) = state
        .db
        .job_finish(
            job,
            st,
            progress.scanned,
            progress.deleted,
            progress.skipped,
            msg,
            now_secs(),
        )
        .await
    {
        tracing::warn!(error = %e, "lifecycle：job 收尾失败");
    }
}

/// 当前版本过期（Days/Date）。
async fn expire_current(
    state: &AppState,
    bucket: &str,
    spec: &LifecycleRuleSpec,
    mode: VersioningMode,
    now: i64,
    progress: &mut Progress,
) -> Option<String> {
    let date = match &spec.expiration {
        Some(ExpirationAction::Days { days }) => Some(now - i64::from(*days) * 86_400),
        Some(ExpirationAction::Date { date }) => {
            if now < *date {
                return None; // 未到执行日
            }
            None
        }
        _ => return None,
    };
    let mut token: Option<String> = None;
    let mut pages = 0usize;
    loop {
        if pages >= 100 {
            return Some(format!(
                "单轮页上限截停（前缀下键过多），下轮续扫（scanned={}）",
                progress.scanned
            ));
        }
        pages += 1;
        let page = match state
            .store
            .list_objects(
                bucket,
                &ListRequest {
                    prefix: Some(spec.prefix().to_string()),
                    continuation_token: token.clone(),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(p) => p,
            Err(xl_store::error::Error::NoSuchBucket(_)) => return None,
            Err(e) => return Some(format!("列举失败: {e}")),
        };
        for o in &page.objects {
            progress.scanned += 1;
            // 删前复读：按列举快照直删会误删「列举后刚被覆盖」的新内容。
            let fresh = match state.store.head_object(bucket, &o.key).await {
                Ok(f) => f,
                Err(xl_store::error::Error::NoSuchKey(_)) => continue,
                Err(e) => {
                    tracing::warn!(bucket, key = %o.key, error = %e, "lifecycle：复读失败（计 skipped）");
                    progress.skipped += 1;
                    continue;
                }
            };
            if !spec.filter.matches(&o.key, fresh.size, &fresh.tags) {
                continue;
            }
            let lmt = secs(fresh.last_modified_nanos);
            let due = match &spec.expiration {
                Some(ExpirationAction::Days { .. }) => date.is_some_and(|c| lmt <= c),
                // Date：清掉执行日之前已存在的对象；之后创建的（lmt > date）不动。
                Some(ExpirationAction::Date { date }) => lmt < *date,
                _ => false,
            };
            if !due {
                continue;
            }
            match state
                .sync_delete
                .delete_versioned(bucket, &o.key, None, mode, false)
                .await
            {
                Ok(outcome) => settle_delete(state, bucket, &o.key, outcome, true, progress).await,
                Err(xl_store::error::Error::NoSuchKey(_)) => {}
                Err(e) => {
                    tracing::warn!(bucket, key = %o.key, error = %e, "lifecycle：删除失败（计 skipped）");
                    progress.skipped += 1;
                }
            }
        }
        match page.next_token {
            Some(t) if page.is_truncated => token = Some(t),
            _ => return None,
        }
    }
}

/// EODM：链上唯一版本且为 delete marker 的墓碑。
async fn expire_eodm(
    state: &AppState,
    bucket: &str,
    spec: &LifecycleRuleSpec,
    mode: VersioningMode,
    _now: i64,
    progress: &mut Progress,
) -> Option<String> {
    let mut last_key: Option<String> = None;
    let mut group: Vec<VersionEntry> = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut vid_marker: Option<String> = None;
    let mut pages = 0usize;
    loop {
        if pages >= 100 {
            return Some("单轮页上限截停（版本过多），下轮续扫".into());
        }
        pages += 1;
        let page = match state
            .store
            .list_object_versions(
                bucket,
                spec.prefix(),
                1000,
                key_marker.as_deref(),
                vid_marker.as_deref(),
            )
            .await
        {
            Ok(p) => p,
            Err(xl_store::error::Error::NoSuchBucket(_)) => return None,
            Err(e) => return Some(format!("版本列举失败: {e}")),
        };
        for e in &page.entries {
            if last_key.as_deref() != Some(e.key.as_str()) {
                if let Some(k) = last_key.take()
                    && let Some(m) =
                        eodm_group(state, bucket, spec, &k, &group, mode, progress).await
                {
                    return Some(m);
                }
                last_key = Some(e.key.clone());
                group.clear();
            }
            group.push(e.clone());
        }
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker;
        vid_marker = page.next_version_id_marker;
    }
    if let Some(k) = last_key {
        return eodm_group(state, bucket, spec, &k, &group, mode, progress).await;
    }
    None
}

async fn eodm_group(
    state: &AppState,
    bucket: &str,
    spec: &LifecycleRuleSpec,
    key: &str,
    group: &[VersionEntry],
    mode: VersioningMode,
    progress: &mut Progress,
) -> Option<String> {
    if group.len() != 1 || !group[0].is_marker {
        return None;
    }
    if !spec.filter.matches(key, group[0].size, &[]) {
        return None;
    }
    // 复核：仍须是「唯一且为 marker」；并发新增版本时放弃本轮。
    let fresh = match fresh_entries(state, bucket, key).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(bucket, key, error = %e, "lifecycle：EODM 复核失败");
            return None;
        }
    };
    if fresh.len() != 1 || fresh[0].version_id != group[0].version_id || !fresh[0].is_marker {
        return None;
    }
    progress.scanned += 1;
    match state
        .sync_delete
        .delete_versioned(bucket, key, Some(&fresh[0].version_id), mode, false)
        .await
    {
        Ok(outcome) => settle_delete(state, bucket, key, outcome, false, progress).await,
        Err(xl_store::error::Error::NoSuchKey(_)) => {}
        Err(e) => {
            tracing::warn!(bucket, key, error = %e, "lifecycle：EODM 删除失败（计 skipped）");
            progress.skipped += 1;
        }
    }
    None
}

/// 非当前版本过期（NoncurrentDays + 可选 NewerNoncurrentVersions）。
async fn expire_noncurrent(
    state: &AppState,
    bucket: &str,
    spec: &LifecycleRuleSpec,
    mode: VersioningMode,
    now: i64,
    progress: &mut Progress,
) -> Option<String> {
    let nc = spec.noncurrent_expiration.as_ref()?;
    let mut last_key: Option<String> = None;
    let mut group: Vec<VersionEntry> = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut vid_marker: Option<String> = None;
    let mut pages = 0usize;
    loop {
        if pages >= 100 {
            return Some("单轮页上限截停（版本过多），下轮续扫".into());
        }
        pages += 1;
        let page = match state
            .store
            .list_object_versions(
                bucket,
                spec.prefix(),
                1000,
                key_marker.as_deref(),
                vid_marker.as_deref(),
            )
            .await
        {
            Ok(p) => p,
            Err(xl_store::error::Error::NoSuchBucket(_)) => return None,
            Err(e) => return Some(format!("版本列举失败: {e}")),
        };
        for e in &page.entries {
            if last_key.as_deref() != Some(e.key.as_str()) {
                if let Some(k) = last_key.take()
                    && let Some(m) =
                        noncurrent_group(state, bucket, spec, nc, &k, &group, mode, now, progress)
                            .await
                {
                    return Some(m);
                }
                last_key = Some(e.key.clone());
                group.clear();
            }
            group.push(e.clone());
        }
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker;
        vid_marker = page.next_version_id_marker;
    }
    if let Some(k) = last_key {
        return noncurrent_group(state, bucket, spec, nc, &k, &group, mode, now, progress).await;
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn noncurrent_group(
    state: &AppState,
    bucket: &str,
    spec: &LifecycleRuleSpec,
    nc: &xl_s3::lifecycle::NoncurrentExpiration,
    key: &str,
    group: &[VersionEntry],
    mode: VersioningMode,
    now: i64,
    progress: &mut Progress,
) -> Option<String> {
    for i in 1..group.len() {
        progress.scanned += 1;
        // 新近程度以更新版本（链上前一条）的创建时间为钟。
        let newer = &group[i - 1];
        if !noncurrent_due(i, nc.newer, newer.mod_time_nanos, nc.days, now) {
            continue;
        }
        let e = &group[i];
        let matched = match version_meta(state, bucket, e).await {
            Ok(Some((size, tags))) => spec.filter.matches(key, size, &tags),
            Ok(None) => continue,
            Err(err) => {
                tracing::warn!(bucket, key, error = %err, "lifecycle：noncurrent 元数据读取失败");
                progress.skipped += 1;
                continue;
            }
        };
        if !matched {
            continue;
        }
        // 复核：候选不得是链首（并发下可能被复活为当前版本）。
        let fresh = match fresh_entries(state, bucket, key).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(bucket, key, error = %e, "lifecycle：noncurrent 复核失败");
                continue;
            }
        };
        if fresh.first().is_some_and(|v| v.version_id == e.version_id) {
            continue;
        }
        match state
            .sync_delete
            .delete_versioned(bucket, key, Some(&e.version_id), mode, false)
            .await
        {
            Ok(outcome) => settle_delete(state, bucket, key, outcome, false, progress).await,
            Err(xl_store::error::Error::NoSuchKey(_)) => {}
            Err(err) => {
                tracing::warn!(bucket, key, error = %err, "lifecycle：noncurrent 删除失败（计 skipped）");
                progress.skipped += 1;
            }
        }
    }
    None
}

/// 候选版本的新鲜元数据（size/tags）；marker 无 HEAD，按空标签/已有 size 判定。
async fn version_meta(
    state: &AppState,
    bucket: &str,
    e: &VersionEntry,
) -> Result<Option<(u64, Vec<xl_store::meta::ObjectTag>)>, xl_store::error::Error> {
    if e.is_marker {
        return Ok(Some((e.size, Vec::new())));
    }
    match state
        .store
        .head_object_version(bucket, &e.key, &e.version_id)
        .await
    {
        Ok(info) => Ok(Some((info.size, info.tags))),
        Err(xl_store::error::Error::NoSuchKey(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

/// 从 key 起取一页（≤2 条）该 key 的最新版本视图，用于删前复核。
async fn fresh_entries(
    state: &AppState,
    bucket: &str,
    key: &str,
) -> Result<Vec<VersionEntry>, xl_store::error::Error> {
    let page = state
        .store
        .list_object_versions(bucket, key, 2, Some(key), None)
        .await?;
    Ok(page
        .entries
        .into_iter()
        .filter(|e| e.key == key)
        .take(2)
        .collect())
}

/// 删除结果统一收尾：tier GC、事件、可选复制入队、计数。
async fn settle_delete(
    state: &AppState,
    bucket: &str,
    key: &str,
    outcome: xl_store::store::DeleteOutcome,
    replicate: bool,
    progress: &mut Progress,
) {
    let refs: Vec<xl_store::store::TierRef> = match &outcome {
        xl_store::store::DeleteOutcome::Version { tier, .. } => tier.iter().cloned().collect(),
        xl_store::store::DeleteOutcome::Object { tiers } => tiers.clone(),
        _ => vec![],
    };
    if !refs.is_empty()
        && let Err(e) = state.tier_gc.delete(&refs).await
    {
        tracing::warn!(error = %e, n = refs.len(), "lifecycle：tier 清理失败");
    }
    if let Some((name, vid)) = lifecycle_event(&outcome) {
        state
            .events
            .emit_named(
                name,
                bucket,
                key,
                vid,
                None,
                None,
                xl_s3::events::EventSource::internal("oss-lifecycle"),
            )
            .await;
        if replicate {
            use xl_s3::replication::ReplicationSink as _;
            state
                .repl_sink
                .on_write(xl_s3::replication::WriteInfo {
                    bucket,
                    key,
                    version_id: vid,
                    etag: "",
                    size: 0,
                    is_delete_marker: true,
                    is_replica: false,
                })
                .await;
        }
        progress.deleted += 1;
    }
}

/// 后台 worker（serve 路径挂载；显式 `OSS_LIFECYCLE_INTERVAL_SECS=0` 关闭）。
pub fn spawn_worker(state: AppState) {
    let secs: u64 = std::env::var("OSS_LIFECYCLE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    if secs == 0 {
        tracing::info!("lifecycle worker 已关闭（OSS_LIFECYCLE_INTERVAL_SECS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            sweep_once(&state).await;
        }
    });
    tracing::info!(interval_secs = secs, "lifecycle worker 已启动");
}

/// C5：审计保留清理 worker——`OSS_AUDIT_RETENTION_DAYS`（默认 0 = 不清理，保持现状）。
/// 每小时清理一次 `ts < now - days*86400` 的行；清理失败只 warn（保留策略非关键路径）。
pub fn spawn_audit_cleanup(state: AppState) {
    let days: i64 = std::env::var("OSS_AUDIT_RETENTION_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if days <= 0 {
        tracing::info!("审计保留清理未启用（OSS_AUDIT_RETENTION_DAYS=0）");
        return;
    }
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(3600));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            let cutoff = now_secs() - days * 86_400;
            match state.db.delete_audit_older_than(cutoff).await {
                Ok(n) if n > 0 => tracing::info!(deleted = n, "审计保留清理"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "审计保留清理失败"),
            }
        }
    });
    tracing::info!(days, "审计保留清理 worker 已启动（每小时一次）");
}

#[cfg(test)]
mod tests {
    use super::{lifecycle_event, noncurrent_due};
    use xl_store::store::DeleteOutcome;

    /// M6 切片 0（P2-1/P2-4 复查补）+ P2：删除结果 → 事件映射的确定性矩阵。
    #[test]
    fn lifecycle_event_mapping_matrix() {
        assert_eq!(
            lifecycle_event(&DeleteOutcome::Marker {
                version_id: "v9".into()
            }),
            Some(("s3:LifecycleExpiration:DeleteMarkerCreated", Some("v9")))
        );
        assert_eq!(
            lifecycle_event(&DeleteOutcome::Version {
                version_id: "v1".into(),
                tier: None,
            }),
            Some(("s3:LifecycleExpiration:Delete", Some("v1")))
        );
        assert_eq!(
            lifecycle_event(&DeleteOutcome::Object { tiers: vec![] }),
            Some(("s3:LifecycleExpiration:Delete", None))
        );
        assert_eq!(
            lifecycle_event(&DeleteOutcome::NotFound),
            None,
            "空删（并发窄窗）不得发事件"
        );
    }

    /// noncurrent 年龄以「更新版本」的创建时间为钟，且 NewerNoncurrentVersions 保留最新 N 个。
    #[test]
    fn noncurrent_due_uses_newer_clock_and_retention() {
        let day = 86_400i64;
        let now = 10 * day;
        let newer = 8 * day * 1_000_000_000; // 2 天前成为 noncurrent
        assert!(noncurrent_due(1, None, newer, 1, now));
        assert!(!noncurrent_due(1, None, newer, 3, now), "3 天阈值未到");
        assert!(!noncurrent_due(1, Some(1), newer, 1, now), "保留最新 1 个");
        assert!(noncurrent_due(2, Some(1), newer, 1, now));
        // 时钟取自更新版本而非候选版本：候选更老也一样由 newer 决定。
        assert!(!noncurrent_due(1, None, 9 * day * 1_000_000_000, 2, now));
    }
}
