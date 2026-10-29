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
use xl_store::store::{ListRequest, UploadListRequest, VersionEntry, VersioningMode};

use crate::db::LifecycleRuleRow;
use crate::raft::{FencingEpoch, LeaderGate};
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

/// 4.10：单阶段单轮页预算（默认 100 页 × 1000 条，测试注入小值验证跨轮续扫）。
/// H3：`max_key_versions` 是单键版本链硬上限——超长链不整链读入内存，跳过该键继续。
#[derive(Clone, Copy)]
pub struct SweepLimits {
    pub max_pages: usize,
    pub page_size: usize,
    pub max_key_versions: usize,
}

impl Default for SweepLimits {
    fn default() -> Self {
        Self {
            max_pages: 100,
            page_size: 1000,
            max_key_versions: 10_000,
        }
    }
}

/// H3：跳过整条版本链的排他 marker。键校验拒绝控制字符，`key\0` 严格大于该键、
/// 小于任何合法后继键，可让 ListObjectVersions 从下一个键开始（不会漏 delete-marker 键）。
fn past_key(key: &str) -> String {
    format!("{key}\u{0}")
}

const STAGE_CURRENT: &str = "current";
const STAGE_EODM: &str = "eodm";
const STAGE_NONCURRENT: &str = "noncurrent";
const STAGE_ABORT_MPU: &str = "abort-mpu";

/// 游标键：新规则用 rule_id；M4 遗留行（rule_id NULL）用 prefix（同桶内唯一）。
fn cursor_rule_id(spec: &LifecycleRuleSpec) -> String {
    spec.rule_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("legacy:{}", spec.prefix()))
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
    abort_mpu: bool,
}

impl ExpiryKinds {
    fn any(self) -> bool {
        self.current || self.eodm || self.noncurrent || self.abort_mpu
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
        abort_mpu: spec.abort_mpu_days.is_some(),
    }
}

/// 单次扫描：对全部启用规则各跑一轮，逐规则一条 jobs。返回处理的规则数。
pub async fn sweep_once(state: &AppState) -> usize {
    sweep_once_at(state, now_secs()).await
}

/// 单次扫描（注入判定时刻）：集成测试用它模拟「规则到期后」的时间点。
pub async fn sweep_once_at(state: &AppState, now: i64) -> usize {
    sweep_once_at_with(state, now, SweepLimits::default()).await
}

/// 4.10：可注入页预算的扫描入口（集成测试用小预算验证「截停→续扫」）。
pub async fn sweep_once_at_with(state: &AppState, now: i64, limits: SweepLimits) -> usize {
    // M8-1：Raft 模式下只有 leader 扫；本轮固定一个 token，交接后立即停（游标写入也带
    // token，旧 leader 不会推进新 leader 的进度）。
    let gate = LeaderGate::from_raft(state.raft.clone());
    let Some(fence) = gate.epoch() else {
        return 0;
    };
    let rules = match state.db.enabled_lifecycle_rules().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "lifecycle：规则读取失败，本轮跳过");
            return 0;
        }
    };
    let mut ran = 0usize;
    for row in rules {
        if !gate.still_current(fence) {
            tracing::warn!("lifecycle：leadership 已变更，本轮中止（新 leader 续扫）");
            break;
        }
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
        run_rule(state, &row.bucket, &spec, kinds, now, limits, &gate, fence).await;
    }
    ran
}

/// 无法解析的规则：开单并记 error（可见性优先，不执行任何删除）。
async fn record_bad_rule(state: &AppState, row: &LifecycleRuleRow, msg: &str) {
    let now = now_secs();
    let job = match state
        .jobs
        .job_start(&state.db, "lifecycle", &row.bucket, &row.prefix, now)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(error = %e, bucket = %row.bucket, "lifecycle：job 开单失败");
            return;
        }
    };
    if let Err(e) = state
        .jobs
        .job_finish(&state.db, job, "error", 0, 0, 0, Some(msg), now_secs())
        .await
    {
        tracing::warn!(error = %e, "lifecycle：job 收尾失败");
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_rule(
    state: &AppState,
    bucket: &str,
    spec: &LifecycleRuleSpec,
    kinds: ExpiryKinds,
    now: i64,
    limits: SweepLimits,
    gate: &LeaderGate,
    fence: FencingEpoch,
) {
    let rule_key = cursor_rule_id(spec);
    let mut progress = Progress::default();
    let mut err: Option<String> = None;
    let started = now_secs();
    let job = match state
        .jobs
        .job_start(&state.db, "lifecycle", bucket, spec.prefix(), started)
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
        // 4.10：各阶段独立推进（各自游标 + 页预算）；某阶段截停不阻塞其他阶段。
        if kinds.current {
            err = expire_current(
                state,
                bucket,
                &rule_key,
                spec,
                mode,
                now,
                job,
                &mut progress,
                limits,
                gate,
                fence,
            )
            .await;
        }
        if kinds.eodm && mode != VersioningMode::Off {
            let e = expire_eodm(
                state,
                bucket,
                &rule_key,
                spec,
                mode,
                now,
                job,
                &mut progress,
                limits,
                gate,
                fence,
            )
            .await;
            err = err.or(e);
        }
        if kinds.noncurrent && mode != VersioningMode::Off {
            let e = expire_noncurrent(
                state,
                bucket,
                &rule_key,
                spec,
                mode,
                now,
                job,
                &mut progress,
                limits,
                gate,
                fence,
            )
            .await;
            err = err.or(e);
        }
        if kinds.abort_mpu {
            let e = expire_abort_mpu(
                state,
                bucket,
                &rule_key,
                spec,
                now,
                job,
                &mut progress,
                limits,
                gate,
                fence,
            )
            .await;
            err = err.or(e);
        }
    }
    let (st, msg) = match &err {
        None => ("done", None),
        Some(m) => ("error", Some(m.as_str())),
    };
    if let Err(e) = state
        .jobs
        .job_finish(
            &state.db,
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

/// M2：长扫描心跳（每页一次），避免周期孤儿回收把仍在跑的 lifecycle 作业误标 error。
async fn heartbeat(state: &AppState, job: i64) {
    if let Err(e) = state.jobs.job_heartbeat(&state.db, job, now_secs()).await {
        tracing::warn!(job, error = %e, "lifecycle：作业心跳失败");
    }
}

/// 游标写入失败只告警：最坏是下轮重复扫/推迟到期，不影响删除安全。
async fn save_cursor(
    state: &AppState,
    bucket: &str,
    rule_id: &str,
    stage: &str,
    cursor: Option<&str>,
    fence: FencingEpoch,
) {
    if let Err(e) = state
        .db
        .lifecycle_cursor_set(bucket, rule_id, stage, cursor, now_secs(), fence)
        .await
    {
        tracing::warn!(bucket, rule_id, stage, error = %e, "lifecycle：游标写入失败");
    }
}

/// 当前版本过期（Days/Date）。
#[allow(clippy::too_many_arguments)]
async fn expire_current(
    state: &AppState,
    bucket: &str,
    rule_id: &str,
    spec: &LifecycleRuleSpec,
    mode: VersioningMode,
    now: i64,
    job: i64,
    progress: &mut Progress,
    limits: SweepLimits,
    gate: &LeaderGate,
    fence: FencingEpoch,
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
    // 4.10：从上轮截停位置续扫。读取失败从头扫（只重复工作，安全）。
    let mut cursor = match state
        .db
        .lifecycle_cursor_get(bucket, rule_id, STAGE_CURRENT)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(bucket, rule_id, error = %e, "lifecycle：游标读取失败，本轮从头扫");
            None
        }
    };
    let mut token: Option<String> = None;
    let mut pages = 0usize;
    loop {
        if !gate.still_current(fence) {
            tracing::warn!(
                bucket,
                rule_id,
                "lifecycle：leadership 已变更，中止当前版本扫描"
            );
            return None;
        }
        if pages >= limits.max_pages {
            // 截停点前整页已处理完：记页尾 key（marker 排他，下轮从其后继续）。
            save_cursor(
                state,
                bucket,
                rule_id,
                STAGE_CURRENT,
                cursor.as_deref(),
                fence,
            )
            .await;
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
                    max_keys: Some(limits.page_size),
                    continuation_token: token.clone(),
                    // 续页 token 与 marker 互斥；marker 只在首轮使用。
                    marker: if token.is_none() {
                        cursor.clone()
                    } else {
                        None
                    },
                    ..Default::default()
                },
            )
            .await
        {
            Ok(p) => p,
            Err(xl_store::error::Error::NoSuchBucket(_)) => return None,
            Err(e) => return Some(format!("列举失败: {e}")),
        };
        heartbeat(state, job).await;
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
        if let Some(last) = page.objects.last() {
            cursor = Some(last.key.clone());
        }
        match page.next_token {
            Some(t) if page.is_truncated => token = Some(t),
            _ => {
                // 扫完清游标：下轮从头开始，不跳过游标之前新插入的 key。
                save_cursor(state, bucket, rule_id, STAGE_CURRENT, None, fence).await;
                return None;
            }
        }
    }
}

/// EODM：链上唯一版本且为 delete marker 的墓碑。
#[allow(clippy::too_many_arguments)]
async fn expire_eodm(
    state: &AppState,
    bucket: &str,
    rule_id: &str,
    spec: &LifecycleRuleSpec,
    mode: VersioningMode,
    _now: i64,
    job: i64,
    progress: &mut Progress,
    limits: SweepLimits,
    gate: &LeaderGate,
    fence: FencingEpoch,
) -> Option<String> {
    let mut last_key: Option<String> = None;
    let mut group: Vec<VersionEntry> = Vec::new();
    // 4.10：续扫重读「上轮未处理完的 key」整条链（不带 vid marker，避免半条链参与判定）。
    let mut key_marker = match state
        .db
        .lifecycle_cursor_get(bucket, rule_id, STAGE_EODM)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(bucket, rule_id, error = %e, "lifecycle：EODM 游标读取失败，本轮从头扫");
            None
        }
    };
    let mut vid_marker: Option<String> = None;
    let mut pages = 0usize;
    let mut over_cap = 0usize;
    'pages: loop {
        if !gate.still_current(fence) {
            tracing::warn!(
                bucket,
                rule_id,
                "lifecycle：leadership 已变更，中止 EODM 扫描"
            );
            return None;
        }
        pages += 1;
        let page = match state
            .store
            .list_object_versions(
                bucket,
                spec.prefix(),
                limits.page_size,
                key_marker.as_deref(),
                vid_marker.as_deref(),
            )
            .await
        {
            Ok(p) => p,
            Err(xl_store::error::Error::NoSuchBucket(_)) => return None,
            Err(e) => return Some(format!("版本列举失败: {e}")),
        };
        heartbeat(state, job).await;
        for e in &page.entries {
            if last_key.as_deref() != Some(e.key.as_str()) {
                // 先在 key 边界把上一组处理完，再判断预算（整链必须读全才能排名）。
                if let Some(k) = last_key.take() {
                    if let Some(m) =
                        eodm_group(state, bucket, spec, &k, &group, mode, progress).await
                    {
                        return Some(m);
                    }
                    // 4.10：至少完成一个 key 后才按预算截停，否则会永远停在起点。
                    if pages >= limits.max_pages {
                        save_cursor(
                            state,
                            bucket,
                            rule_id,
                            STAGE_EODM,
                            Some(e.key.as_str()),
                            fence,
                        )
                        .await;
                        return Some("单轮页上限截停（版本过多），下轮续扫".into());
                    }
                }
                last_key = Some(e.key.clone());
                group.clear();
            }
            if group.len() >= limits.max_key_versions {
                // H3：单键链超限 → 丢弃已收条目，直接跳到下一个键（不整链读入、不空转）。
                over_cap += 1;
                progress.skipped += 1;
                tracing::warn!(bucket, key = %e.key, cap = limits.max_key_versions,
                    "lifecycle：单键版本链超过上限，跳过该键");
                last_key = None;
                group.clear();
                key_marker = Some(past_key(&e.key));
                vid_marker = None;
                if pages >= limits.max_pages {
                    save_cursor(
                        state,
                        bucket,
                        rule_id,
                        STAGE_EODM,
                        key_marker.as_deref(),
                        fence,
                    )
                    .await;
                    return Some("单轮页上限截停（版本过多），下轮续扫".into());
                }
                continue 'pages;
            }
            group.push(e.clone());
        }
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker;
        vid_marker = page.next_version_id_marker;
    }
    save_cursor(state, bucket, rule_id, STAGE_EODM, None, fence).await;
    let group_err = if let Some(k) = last_key {
        eodm_group(state, bucket, spec, &k, &group, mode, progress).await
    } else {
        None
    };
    if group_err.is_some() {
        return group_err;
    }
    if over_cap > 0 {
        return Some(format!(
            "{over_cap} 个键的版本链超过 {} 条，已跳过",
            limits.max_key_versions
        ));
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
#[allow(clippy::too_many_arguments)]
async fn expire_noncurrent(
    state: &AppState,
    bucket: &str,
    rule_id: &str,
    spec: &LifecycleRuleSpec,
    mode: VersioningMode,
    now: i64,
    job: i64,
    progress: &mut Progress,
    limits: SweepLimits,
    gate: &LeaderGate,
    fence: FencingEpoch,
) -> Option<String> {
    let nc = match spec.noncurrent_expiration.as_ref() {
        Some(nc) => nc,
        None => return None,
    };
    let mut last_key: Option<String> = None;
    let mut group: Vec<VersionEntry> = Vec::new();
    // 4.10：同 EODM——续扫整链重读，排名才正确。
    let mut key_marker = match state
        .db
        .lifecycle_cursor_get(bucket, rule_id, STAGE_NONCURRENT)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(bucket, rule_id, error = %e, "lifecycle：noncurrent 游标读取失败，本轮从头扫");
            None
        }
    };
    let mut vid_marker: Option<String> = None;
    let mut pages = 0usize;
    let mut over_cap = 0usize;
    'pages: loop {
        if !gate.still_current(fence) {
            tracing::warn!(
                bucket,
                rule_id,
                "lifecycle：leadership 已变更，中止 noncurrent 扫描"
            );
            return None;
        }
        pages += 1;
        let page = match state
            .store
            .list_object_versions(
                bucket,
                spec.prefix(),
                limits.page_size,
                key_marker.as_deref(),
                vid_marker.as_deref(),
            )
            .await
        {
            Ok(p) => p,
            Err(xl_store::error::Error::NoSuchBucket(_)) => return None,
            Err(e) => return Some(format!("版本列举失败: {e}")),
        };
        heartbeat(state, job).await;
        for e in &page.entries {
            if last_key.as_deref() != Some(e.key.as_str()) {
                // 同 EODM：先处理完上一组，再在 key 边界判断预算。
                if let Some(k) = last_key.take() {
                    if let Some(m) =
                        noncurrent_group(state, bucket, spec, nc, &k, &group, mode, now, progress)
                            .await
                    {
                        return Some(m);
                    }
                    if pages >= limits.max_pages {
                        save_cursor(
                            state,
                            bucket,
                            rule_id,
                            STAGE_NONCURRENT,
                            Some(e.key.as_str()),
                            fence,
                        )
                        .await;
                        return Some("单轮页上限截停（版本过多），下轮续扫".into());
                    }
                }
                last_key = Some(e.key.clone());
                group.clear();
            }
            if group.len() >= limits.max_key_versions {
                // H3：同 EODM——超长链不读入内存，跳过该键并直接续到下一个键。
                over_cap += 1;
                progress.skipped += 1;
                tracing::warn!(bucket, key = %e.key, cap = limits.max_key_versions,
                    "lifecycle：单键版本链超过上限，跳过该键");
                last_key = None;
                group.clear();
                key_marker = Some(past_key(&e.key));
                vid_marker = None;
                if pages >= limits.max_pages {
                    save_cursor(
                        state,
                        bucket,
                        rule_id,
                        STAGE_NONCURRENT,
                        key_marker.as_deref(),
                        fence,
                    )
                    .await;
                    return Some("单轮页上限截停（版本过多），下轮续扫".into());
                }
                continue 'pages;
            }
            group.push(e.clone());
        }
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker;
        vid_marker = page.next_version_id_marker;
    }
    save_cursor(state, bucket, rule_id, STAGE_NONCURRENT, None, fence).await;
    let group_err = if let Some(k) = last_key {
        noncurrent_group(state, bucket, spec, nc, &k, &group, mode, now, progress).await
    } else {
        None
    };
    if group_err.is_some() {
        return group_err;
    }
    if over_cap > 0 {
        return Some(format!(
            "{over_cap} 个键的版本链超过 {} 条，已跳过",
            limits.max_key_versions
        ));
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

/// 3.10：MPU 双 marker 游标（key + upload_id），JSON 存在 lifecycle_cursors.cursor。
#[derive(serde::Serialize, serde::Deserialize)]
struct MpuCursor {
    key: String,
    upload_id: String,
}

/// 未完成分段上传过期（AbortIncompleteMultipartUpload）：仅按前缀 + 创建天数。
/// MPU 没有对象事件/复制语义（不是已存在对象），中止后只计数。
#[allow(clippy::too_many_arguments)]
async fn expire_abort_mpu(
    state: &AppState,
    bucket: &str,
    rule_id: &str,
    spec: &LifecycleRuleSpec,
    now: i64,
    job: i64,
    progress: &mut Progress,
    limits: SweepLimits,
    gate: &LeaderGate,
    fence: FencingEpoch,
) -> Option<String> {
    let days = spec.abort_mpu_days?;
    let (mut key_marker, mut upload_id_marker) = match state
        .db
        .lifecycle_cursor_get(bucket, rule_id, STAGE_ABORT_MPU)
        .await
    {
        Ok(Some(raw)) => match serde_json::from_str::<MpuCursor>(&raw) {
            Ok(c) => (Some(c.key), Some(c.upload_id)),
            Err(e) => {
                // 坏游标从头重扫：最多重复列举，不会重复中止（NoSuchUpload 视为成功）。
                tracing::warn!(bucket, rule_id, error = %e, "lifecycle：MPU 游标损坏，从头扫描");
                (None, None)
            }
        },
        Ok(None) => (None, None),
        Err(e) => return Some(format!("MPU 游标读取失败: {e}")),
    };
    let mut pages = 0usize;
    loop {
        if !gate.still_current(fence) {
            tracing::warn!(
                bucket,
                rule_id,
                "lifecycle：leadership 已变更，中止 MPU 扫描"
            );
            return None;
        }
        let page = match state
            .store
            .list_multipart_uploads(
                bucket,
                &UploadListRequest {
                    prefix: Some(spec.prefix().to_string()),
                    delimiter: None,
                    key_marker: key_marker.clone(),
                    upload_id_marker: upload_id_marker.clone(),
                    max_uploads: Some(limits.page_size.max(1)),
                },
            )
            .await
        {
            Ok(p) => p,
            Err(e) => return Some(format!("MPU 列举失败: {e}")),
        };
        heartbeat(state, job).await;
        let mut last: Option<(String, String)> = None;
        for u in &page.uploads {
            last = Some((u.key.clone(), u.upload_id.clone()));
            // 创建时刻缺失/非法 → 年龄不可知，fail closed 跳过。
            if u.initiated_nanos <= 0 {
                progress.skipped += 1;
                continue;
            }
            if now - secs(u.initiated_nanos) < i64::from(days) * 86_400 {
                continue;
            }
            progress.scanned += 1;
            match state
                .store
                .abort_multipart_upload(bucket, &u.key, &u.upload_id)
                .await
            {
                Ok(()) => progress.deleted += 1,
                // 并发完成/已删：目标状态已达成。
                Err(xl_store::error::Error::NoSuchUpload(_)) => {}
                // M3：单个上传的持久错误不得卡死整条规则/后续 MPU；跳过并计数，游标照常前移。
                Err(e) => {
                    progress.skipped += 1;
                    tracing::warn!(bucket, key = %u.key, upload_id = %u.upload_id, error = %e,
                        "lifecycle：中止 MPU 失败，跳过该上传");
                }
            }
        }
        if !page.is_truncated {
            save_cursor(state, bucket, rule_id, STAGE_ABORT_MPU, None, fence).await;
            return None;
        }
        pages += 1;
        let next = match (page.next_key_marker, page.next_upload_id_marker) {
            (Some(key), Some(upload_id)) => Some(MpuCursor { key, upload_id }),
            _ => last.map(|(key, upload_id)| MpuCursor { key, upload_id }),
        };
        let Some(next) = next else {
            // 截断却无续页 marker：保守停下，下轮从原游标重扫。
            return Some("MPU 列举截断但缺续页 marker".to_string());
        };
        if pages >= limits.max_pages.max(1) {
            let raw = serde_json::to_string(&next).unwrap_or_default();
            save_cursor(state, bucket, rule_id, STAGE_ABORT_MPU, Some(&raw), fence).await;
            return Some("单轮页上限截停（MPU 过多），下轮续扫".into());
        }
        key_marker = Some(next.key);
        upload_id_marker = Some(next.upload_id);
    }
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
                    is_version_delete: false,
                    is_replica: false,
                })
                .await;
        }
        progress.deleted += 1;
    }
}

/// 后台 worker（serve 路径挂载；显式 `OSS_LIFECYCLE_INTERVAL_SECS=0` 关闭）。
/// M8-1：Raft 模式下只有 leader 执行（过期删除作用于全集群共享的对象数据）。
pub fn spawn_worker(state: AppState) {
    let secs: u64 = std::env::var("OSS_LIFECYCLE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    if secs == 0 {
        tracing::info!("lifecycle worker 已关闭（OSS_LIFECYCLE_INTERVAL_SECS=0）");
        return;
    }
    let gate = crate::raft::LeaderGate::from_raft(state.raft.clone());
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(secs));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            t.tick().await;
            if !gate.is_leader() {
                continue;
            }
            sweep_once(&state).await;
        }
    });
    tracing::info!(interval_secs = secs, "lifecycle worker 已启动");
}

/// 审计保留清理 worker——`OSS_AUDIT_RETENTION_DAYS`（默认 0 = 不清理，保持现状）。
/// 天数收敛到 1..=36500（0/负/非法 = 关闭，避免 `days*86400` 溢出）；每小时清理一次
/// `ts < now - days*86400`，单轮最多 [`AUDIT_PURGE_BATCH`] 行（积压多轮收敛，失败只 warn）。
pub fn spawn_audit_cleanup(state: AppState) {
    let days: i64 = std::env::var("OSS_AUDIT_RETENTION_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
        .clamp(0, 36_500);
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
            match state
                .db
                .delete_audit_older_than(cutoff, AUDIT_PURGE_BATCH)
                .await
            {
                Ok(n) if n > 0 => tracing::info!(deleted = n, "审计保留清理（单轮上限）"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "审计保留清理失败"),
            }
        }
    });
    tracing::info!(days, "审计保留清理 worker 已启动（每小时一次）");
}

/// 单轮保留清理上限：避免首次启用 / 长期积压时长时间持有 SQLite 写锁。
const AUDIT_PURGE_BATCH: i64 = 10_000;

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
