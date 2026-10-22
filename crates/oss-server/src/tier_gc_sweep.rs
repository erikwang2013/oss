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
const MAX_LIVE_SCAN: usize = 1_000_000;
const MAX_ERRORS: usize = 100;
const MAX_MISSING_SAMPLES: usize = 10;

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
    let live = collect_live(store).await?;

    let now = crate::db::now();
    let mut reports = Vec::with_capacity(ids.len());
    for id in &ids {
        let objs = listed.remove(id).unwrap_or_default();
        reports.push(eval_tier(id, objs, live.get(id), tiers, opts, now).await);
    }
    Ok(GcResponse {
        dry_run: opts.dry_run,
        grace_days: opts.grace_days,
        max_deletes: opts.max_deletes,
        reports,
    })
}

async fn eval_tier(
    id: &str,
    objs: Vec<TierObject>,
    live: Option<&HashSet<String>>,
    tiers: &TierBackends,
    opts: &GcOptions,
    now: i64,
) -> TierGcReport {
    let grace_secs = i64::from(opts.grace_days) * 86_400;
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
    if opts.dry_run {
        return report;
    }
    let Some(backend) = tiers.get(id) else {
        return report;
    };
    for o in candidates.into_iter().take(take) {
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
    report
}

/// 全量扫描所有桶的所有版本，收集 (backend → 完整 tier key) 的 live 集。
async fn collect_live(store: &Store) -> Result<HashMap<String, HashSet<String>>, String> {
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
                if scanned > MAX_LIVE_SCAN {
                    return Err(format!(
                        "GC live 扫描超过 {MAX_LIVE_SCAN} 个版本，已中止（未删除任何对象）"
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
