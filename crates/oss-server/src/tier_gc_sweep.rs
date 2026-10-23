//! M8：tier 远端 GC / 对账（管理员显式触发，默认 dry-run）。
//!
//! 安全口径：
//! 1. **先列远端、再全量扫本地版本 meta**：任何一次 list / 扫描失败 → 整轮报错，一个对象都不删。
//! 2. live `TierRef` 永不删除（按 backend + 完整 key 精确匹配）。
//! 3. 只处理 `<tier_id>/` 前缀内、年龄 ≥ grace 的对象；年龄未知（<=0）也保留。
//! 4. 删除失败逐条记录并继续；本轮最多删 `max_deletes` 个，剩余下轮处理。
//! 5. `missing_live` 只做对账提示（live 引用在远端不存在），不触发任何写操作。

use std::collections::{HashMap, HashSet};

use serde::Serialize;
use xl_store::store::Store;

use crate::tier::{TierBackends, TierObject};

pub const DEFAULT_GRACE_DAYS: u32 = 7;
pub const DEFAULT_MAX_DELETES: usize = 1000;
const MAX_GRACE_DAYS: u32 = 36_500;
const MAX_DELETES: usize = 10_000;
/// 全量版本扫描上限：超过就整轮失败（宁可不 GC，也不能在 live 集不完整时删除）。
/// 可用 `OSS_TIER_GC_MAX_LIVE_SCAN` 调整；非法/0 回落到默认值。
const MAX_LIVE_SCAN: usize = 1_000_000;
/// 4.8：有效 grace 下限。Transition 先 put 远端再写 meta 引用，`grace_days=0`
/// 会把这段窗口内的新对象当垃圾删掉；强制至少 1h 保护。
const DEFAULT_MIN_GRACE_SECS: i64 = 3600;
/// 下限本身再兜底：环境变量配 0/负数也只降到 60s，绝不回到「立刻可删」。
const MIN_ALLOWED_GRACE_SECS: i64 = 60;
const MAX_ERRORS: usize = 100;
const MAX_MISSING_SAMPLES: usize = 10;

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn min_grace_secs_from(raw: Option<&str>) -> i64 {
    raw.and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_MIN_GRACE_SECS)
        .max(MIN_ALLOWED_GRACE_SECS)
}

fn effective_grace_secs(grace_days: u32, min_grace: i64) -> i64 {
    (i64::from(grace_days) * 86_400).max(min_grace)
}

fn max_live_scan_from(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(MAX_LIVE_SCAN)
}

#[derive(Debug, Clone)]
pub struct GcOptions {
    /// None = 全部已配置 tier。
    pub tier: Option<String>,
    pub dry_run: bool,
    pub grace_days: u32,
    pub max_deletes: usize,
}

impl Default for GcOptions {
    fn default() -> Self {
        Self {
            tier: None,
            dry_run: true,
            grace_days: DEFAULT_GRACE_DAYS,
            max_deletes: DEFAULT_MAX_DELETES,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct GcError {
    pub key: String,
    pub error: String,
}

#[derive(Debug, Default, Serialize)]
pub struct TierGcReport {
    pub tier: String,
    pub listed: usize,
    /// 本轮扫描到的 live TierRef 数（该 tier）。
    pub live_refs: usize,
    /// 可删候选（非 live、前缀匹配、年龄 ≥ grace）。
    pub stale: usize,
    /// 本轮实际尝试/打算删除的数量（受 max_deletes 限制）。
    pub would_delete: usize,
    /// dry-run 时恒为 0。
    pub deleted: usize,
    pub kept_live: usize,
    pub kept_grace: usize,
    pub kept_foreign: usize,
    pub kept_unknown_age: usize,
    /// live 引用但远端 list 不见的 key 数（只提示，不写）。
    pub missing_live: usize,
    pub missing_samples: Vec<String>,
    pub failed: usize,
    /// 候选超过 max_deletes（本轮只处理前 N 个）。
    pub truncated: bool,
    pub errors: Vec<GcError>,
}

#[derive(Debug, Serialize)]
pub struct GcResponse {
    pub dry_run: bool,
    pub grace_days: u32,
    /// 实际生效的 grace（含 4.8 强制下限），秒。
    pub effective_grace_secs: i64,
    pub max_deletes: usize,
    pub reports: Vec<TierGcReport>,
}

/// 数值范围校验（HTTP 层用于把非法参数映射成 400）。
pub fn validate_opts(opts: &GcOptions) -> Result<(), String> {
    if opts.grace_days > MAX_GRACE_DAYS {
        return Err(format!("grace_days 须在 0..={MAX_GRACE_DAYS}"));
    }
    if opts.max_deletes == 0 || opts.max_deletes > MAX_DELETES {
        return Err(format!("max_deletes 须在 1..={MAX_DELETES}"));
    }
    Ok(())
}

/// 执行一轮 GC/对账。校验失败或任一 tier list / live 扫描失败 → Err（不删除任何对象）。
pub async fn run_gc(
    store: &Store,
    tiers: &TierBackends,
    opts: &GcOptions,
) -> Result<GcResponse, String> {
    validate_opts(opts)?;
    let min_grace = min_grace_secs_from(env_var("OSS_TIER_GC_MIN_GRACE_SECS").as_deref());
    let grace_secs = effective_grace_secs(opts.grace_days, min_grace);
    let scan_cap = max_live_scan_from(env_var("OSS_TIER_GC_MAX_LIVE_SCAN").as_deref());
    let ids: Vec<String> = match &opts.tier {
        Some(id) => {
            if tiers.get(id).is_none() {
                return Err(format!("tier 不存在：{id}"));
            }
            vec![id.clone()]
        }
        None => tiers.ids(),
    };

    // 1) 先列远端：任一失败即中止（还没扫本地，更没删除）。
    let mut listed: HashMap<String, Vec<TierObject>> = HashMap::new();
    for id in &ids {
        let Some(b) = tiers.get(id) else { continue };
        let objs = b
            .list()
            .await
            .map_err(|e| format!("tier {id} list 失败：{e}"))?;
        listed.insert(id.clone(), objs);
    }

    // 2) live 集：全量版本扫描；不完整就失败（fail closed）。
    let live = collect_live(store, scan_cap).await?;

    let now = crate::db::now();
    let mut reports = Vec::with_capacity(ids.len());
    let mut pending: Vec<(usize, String, Vec<TierObject>)> = Vec::new();
    for id in &ids {
        let objs = listed.remove(id).unwrap_or_default();
        let (report, candidates) = eval_tier(id, objs, live.get(id), opts, grace_secs, now);
        if !opts.dry_run && !candidates.is_empty() {
            pending.push((reports.len(), id.clone(), candidates));
        }
        reports.push(report);
    }

    if !opts.dry_run && !pending.is_empty() {
        // 4.8：真正删除前再取一次 live 快照，把第一轮快照到删除之间的新引用保下来。
        let fresh = collect_live(store, scan_cap).await?;
        for (idx, id, candidates) in pending {
            let Some(backend) = tiers.get(&id) else {
                continue;
            };
            let report = &mut reports[idx];
            let (mut candidates, newly_live) = split_newly_live(candidates, fresh.get(&id));
            report.kept_live += newly_live;
            report.stale = candidates.len();
            let take = candidates.len().min(opts.max_deletes);
            report.would_delete = take;
            report.truncated = candidates.len() > take;
            for o in candidates.drain(..take) {
                match backend.delete(&o.key).await {
                    Ok(()) => report.deleted += 1,
                    Err(e) => {
                        report.failed += 1;
                        if report.errors.len() < MAX_ERRORS {
                            report.errors.push(GcError {
                                key: o.key,
                                error: e,
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(GcResponse {
        dry_run: opts.dry_run,
        grace_days: opts.grace_days,
        effective_grace_secs: grace_secs,
        max_deletes: opts.max_deletes,
        reports,
    })
}

/// 第二轮 live 快照才出现的候选：保留，并从待删集移除。
fn split_newly_live(
    candidates: Vec<TierObject>,
    fresh: Option<&HashSet<String>>,
) -> (Vec<TierObject>, usize) {
    let Some(fresh) = fresh else {
        return (candidates, 0);
    };
    let mut kept = 0;
    let mut out = Vec::with_capacity(candidates.len());
    for o in candidates {
        if fresh.contains(&o.key) {
            kept += 1;
        } else {
            out.push(o);
        }
    }
    (out, kept)
}

fn eval_tier(
    id: &str,
    objs: Vec<TierObject>,
    live: Option<&HashSet<String>>,
    opts: &GcOptions,
    grace_secs: i64,
    now: i64,
) -> (TierGcReport, Vec<TierObject>) {
    let tier_live = live.cloned().unwrap_or_default();
    let prefix = format!("{id}/");
    let listed_keys: HashSet<&str> = objs
        .iter()
        .filter(|o| o.key.starts_with(&prefix))
        .map(|o| o.key.as_str())
        .collect();
    let mut report = TierGcReport {
        tier: id.to_string(),
        listed: objs.len(),
        live_refs: tier_live.len(),
        missing_live: tier_live
            .iter()
            .filter(|k| !listed_keys.contains(k.as_str()))
            .count(),
        ..TierGcReport::default()
    };
    report.missing_samples = tier_live
        .iter()
        .filter(|k| !listed_keys.contains(k.as_str()))
        .take(MAX_MISSING_SAMPLES)
        .cloned()
        .collect();

    let mut candidates: Vec<TierObject> = Vec::new();
    for o in objs {
        if !o.key.starts_with(&prefix) {
            report.kept_foreign += 1;
            continue;
        }
        if tier_live.contains(&o.key) {
            report.kept_live += 1;
            continue;
        }
        // LastModified 缺失/为 0 = 年龄未知：保留（fail closed）。
        if o.last_modified <= 0 {
            report.kept_unknown_age += 1;
            continue;
        }
        if now.saturating_sub(o.last_modified) < grace_secs {
            report.kept_grace += 1;
            continue;
        }
        candidates.push(o);
    }
    candidates.sort_by(|a, b| a.key.cmp(&b.key));
    report.stale = candidates.len();
    let take = candidates.len().min(opts.max_deletes);
    report.would_delete = take;
    report.truncated = candidates.len() > take;
    (report, candidates)
}

/// 全量扫描所有桶的所有版本，收集 (backend → 完整 tier key) 的 live 集。
async fn collect_live(
    store: &Store,
    scan_cap: usize,
) -> Result<HashMap<String, HashSet<String>>, String> {
    let mut live: HashMap<String, HashSet<String>> = HashMap::new();
    let mut scanned = 0usize;
    let buckets = store
        .list_buckets()
        .await
        .map_err(|e| format!("GC 扫描桶列表失败：{e}"))?;
    for b in buckets {
        let mut key_marker: Option<String> = None;
        let mut vid_marker: Option<String> = None;
        loop {
            let page = store
                .list_object_versions(
                    &b.name,
                    "",
                    1000,
                    key_marker.as_deref(),
                    vid_marker.as_deref(),
                )
                .await
                .map_err(|e| format!("GC 扫描桶 {} 版本失败：{e}", b.name))?;
            if page.degraded {
                return Err(format!(
                    "GC 扫描桶 {} 时有节点不可达，live 集不完整，已中止（未删除任何对象）",
                    b.name
                ));
            }
            for e in &page.entries {
                scanned += 1;
                if scanned > scan_cap {
                    return Err(format!(
                        "GC live 扫描超过 {scan_cap} 个版本，已中止（未删除任何对象）"
                    ));
                }
                if let (Some(backend), Some(key)) =
                    (e.tier_backend.as_deref(), e.tier_key.as_deref())
                {
                    live.entry(backend.to_string())
                        .or_default()
                        .insert(key.to_string());
                }
            }
            if !page.is_truncated {
                break;
            }
            if page.next_key_marker.is_none() {
                return Err(format!("GC 扫描桶 {} 分页缺少续点，已中止", b.name));
            }
            key_marker = page.next_key_marker;
            vid_marker = page.next_version_id_marker;
        }
    }
    Ok(live)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(key: &str) -> TierObject {
        TierObject {
            key: key.into(),
            last_modified: 1,
            size: 1,
        }
    }

    #[test]
    fn grace_zero_is_clamped_to_min_grace() {
        // 4.8：grace_days=0 不得退化成「新对象立刻可删」。
        assert_eq!(min_grace_secs_from(None), 3600);
        assert_eq!(min_grace_secs_from(Some("0")), 3600);
        assert_eq!(min_grace_secs_from(Some("-5")), 3600);
        assert_eq!(min_grace_secs_from(Some("30")), 60);
        assert_eq!(min_grace_secs_from(Some("7200")), 7200);
        assert_eq!(effective_grace_secs(0, 3600), 3600);
        assert_eq!(effective_grace_secs(7, 3600), 7 * 86_400);
    }

    #[test]
    fn live_scan_cap_is_configurable_with_safe_default() {
        assert_eq!(max_live_scan_from(Some("42")), 42);
        assert_eq!(max_live_scan_from(Some(" 42 ")), 42);
        assert_eq!(max_live_scan_from(Some("0")), MAX_LIVE_SCAN);
        assert_eq!(max_live_scan_from(Some("nope")), MAX_LIVE_SCAN);
        assert_eq!(max_live_scan_from(None), MAX_LIVE_SCAN);
    }

    #[test]
    fn fresh_live_ref_survives_second_snapshot_filter() {
        let a = obj("cold/bkt/v1/a");
        let b = obj("cold/bkt/v1/b");
        let fresh: HashSet<String> = [b.key.clone()].into_iter().collect();
        let (out, kept) = split_newly_live(vec![a.clone(), b], Some(&fresh));
        assert_eq!(kept, 1);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key, a.key);
        // 没有第二轮快照（理论上不会发生）时保持原候选。
        let (out, kept) = split_newly_live(vec![a], None);
        assert_eq!(kept, 0);
        assert_eq!(out.len(), 1);
    }
}
