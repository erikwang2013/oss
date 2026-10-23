//! M8 S4b：生命周期配置（Expiration + Transition + Noncurrent）——共享类型、校验与线格式映射。
//!
//! console 与 S3 两个入口共用 [`validate_rules`]（`SqlLifecycleStore::put_rules` 内调用），
//! 因此任何入口都无法绕过同一份约束。结构化动作经 `config_json` 持久化；旧列保持双写，
//! 老行（无 config_json）按旧列反解。

mod parse;

pub use parse::{parse_lifecycle_config, rules_to_dto};

use serde::{Deserialize, Serialize};
use xl_store::meta::{
    MAX_TAG_KEY_CHARS, MAX_TAG_VALUE_CHARS, MAX_TAGS, ObjectTag, is_archive_class,
};

pub const MAX_RULES: usize = 100;
pub const MAX_DAYS: u32 = 36_500;
pub const MAX_PREFIX_BYTES: usize = 1024;
pub const MAX_ID_CHARS: usize = 255;
/// NoncurrentVersionExpiration.NewerNoncurrentVersions 上限（AWS）。
pub const MAX_RETAINED_NONCURRENT: u32 = 100;

/// Transition 允许的存储类（S4a 白名单的子集：不含 STANDARD/REDUCED_REDUNDANCY）。
pub const TRANSITION_CLASSES: [&str; 6] = [
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER",
    "GLACIER_IR",
    "DEEP_ARCHIVE",
];

/// 过滤器（prefix / tag 集合 / 严格大小边界；全部条件同时满足才命中）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleFilter {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prefix: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<ObjectTag>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_gt: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_lt: Option<u64>,
}

impl LifecycleFilter {
    /// 无任何谓词 = 全桶（`NewerNoncurrentVersions` 要求非空 Filter）。
    pub fn is_empty(&self) -> bool {
        self.prefix.is_empty()
            && self.tags.is_empty()
            && self.size_gt.is_none()
            && self.size_lt.is_none()
    }

    /// 对象是否命中（严格大于/小于；标签全部存在且值相等）。
    pub fn matches(&self, key: &str, size: u64, tags: &[ObjectTag]) -> bool {
        key.starts_with(&self.prefix)
            && self.size_gt.is_none_or(|gt| size > gt)
            && self.size_lt.is_none_or(|lt| size < lt)
            && self
                .tags
                .iter()
                .all(|t| tags.iter().any(|o| o.key == t.key && o.value == t.value))
    }

    /// 谓词数量：规则重叠时“更具体者优先”。
    pub fn predicate_count(&self) -> usize {
        usize::from(!self.prefix.is_empty())
            + self.tags.len()
            + usize::from(self.size_gt.is_some())
            + usize::from(self.size_lt.is_some())
    }
}

/// Expiration 动作（三选一；枚举本身保证互斥）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExpirationAction {
    Days { days: u32 },
    Date { date: i64 },
    ExpiredObjectDeleteMarker,
}

/// 当前版本 Transition（Days 或 Date 二选一）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionAction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub days: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<i64>,
    pub class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
}

impl TransitionAction {
    /// 阈值到达时刻（Days 相对对象创建时间；Date 为绝对时刻）。
    pub fn threshold_secs(&self, created_secs: i64) -> i64 {
        match (self.days, self.date) {
            (Some(d), _) => created_secs + i64::from(d) * 86_400,
            (None, Some(date)) => date,
            (None, None) => i64::MAX,
        }
    }
}

/// 非当前版本过期；`newer` = 保留的较新非当前版本数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoncurrentExpiration {
    pub days: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newer: Option<u32>,
}

/// 非当前版本 Transition（仅 Days；`newer` 同语义）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoncurrentTransition {
    pub days: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newer: Option<u32>,
    pub class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
}

/// 归一化后的规则（console ↔ DB ↔ S3 共用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleRuleSpec {
    pub rule_id: Option<String>,
    pub filter: LifecycleFilter,
    pub expiration: Option<ExpirationAction>,
    pub transitions: Vec<TransitionAction>,
    pub noncurrent_expiration: Option<NoncurrentExpiration>,
    pub noncurrent_transitions: Vec<NoncurrentTransition>,
    pub enabled: bool,
}

impl LifecycleRuleSpec {
    pub fn prefix(&self) -> &str {
        &self.filter.prefix
    }

    /// 旧列 `expiration_days` 能表达的过期（仅 Days）。
    pub fn expiration_days(&self) -> Option<u32> {
        match self.expiration {
            Some(ExpirationAction::Days { days }) => Some(days),
            _ => None,
        }
    }

    pub fn has_action(&self) -> bool {
        self.expiration.is_some()
            || !self.transitions.is_empty()
            || self.noncurrent_expiration.is_some()
            || !self.noncurrent_transitions.is_empty()
    }

    /// 旧列（prefix/expiration_days/单条 transition）能完整表达时返回 None；
    /// 否则返回结构化 `config_json` 原文。
    pub fn config_json(&self) -> Result<Option<String>, LifecycleError> {
        let legacy = self.filter.tags.is_empty()
            && self.filter.size_gt.is_none()
            && self.filter.size_lt.is_none()
            && matches!(self.expiration, None | Some(ExpirationAction::Days { .. }))
            && self.transitions.len() <= 1
            && self
                .transitions
                .iter()
                .all(|t| t.days.is_some() && t.date.is_none())
            && self.noncurrent_expiration.is_none()
            && self.noncurrent_transitions.is_empty();
        if legacy {
            return Ok(None);
        }
        let blob = LifecycleConfigJson {
            filter: self.filter.clone(),
            expiration: self.expiration,
            transitions: self.transitions.clone(),
            noncurrent_expiration: self.noncurrent_expiration.clone(),
            noncurrent_transitions: self.noncurrent_transitions.clone(),
        };
        serde_json::to_string(&blob)
            .map(Some)
            .map_err(|e| LifecycleError::Internal(format!("config_json 序列化失败: {e}")))
    }

    /// 旧行（config_json 缺失）反解；`enabled`/`rule_id` 由行携带。
    pub fn from_legacy_row(
        rule_id: Option<String>,
        prefix: String,
        expiration_days: Option<i64>,
        transition_days: Option<i64>,
        transition_class: Option<String>,
        transition_tier: Option<String>,
        enabled: bool,
    ) -> Result<Self, LifecycleError> {
        let expiration = match expiration_days {
            Some(d) => Some(ExpirationAction::Days {
                days: u32::try_from(d)
                    .map_err(|_| LifecycleError::Invalid(format!("expiration_days 非法: {d}")))?,
            }),
            None => None,
        };
        let transitions =
            match (transition_days, transition_class) {
                (Some(d), Some(class)) => vec![TransitionAction {
                    days: Some(u32::try_from(d).map_err(|_| {
                        LifecycleError::Invalid(format!("transition_days 非法: {d}"))
                    })?),
                    date: None,
                    class,
                    tier: transition_tier,
                }],
                _ => vec![],
            };
        let spec = Self {
            rule_id,
            filter: LifecycleFilter {
                prefix,
                ..Default::default()
            },
            expiration,
            transitions,
            noncurrent_expiration: None,
            noncurrent_transitions: vec![],
            enabled,
        };
        validate_spec(&spec)?;
        Ok(spec)
    }
}

/// `config_json` 的线格式（版本化 blob；身份字段留在列上）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LifecycleConfigJson {
    filter: LifecycleFilter,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expiration: Option<ExpirationAction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    transitions: Vec<TransitionAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    noncurrent_expiration: Option<NoncurrentExpiration>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    noncurrent_transitions: Vec<NoncurrentTransition>,
}

/// 解析 `config_json` 并做安全校验（读路径 fail-closed：坏 JSON/越界 → Err，调用方跳过）。
pub fn from_config_json(
    json: &str,
    rule_id: Option<String>,
    enabled: bool,
) -> Result<LifecycleRuleSpec, LifecycleError> {
    let blob: LifecycleConfigJson = serde_json::from_str(json)
        .map_err(|e| LifecycleError::Invalid(format!("config_json 损坏: {e}")))?;
    let spec = LifecycleRuleSpec {
        rule_id,
        filter: blob.filter,
        expiration: blob.expiration,
        transitions: blob.transitions,
        noncurrent_expiration: blob.noncurrent_expiration,
        noncurrent_transitions: blob.noncurrent_transitions,
        enabled,
    };
    validate_spec(&spec)?;
    Ok(spec)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleError {
    NoSuchBucket,
    NoSuchConfiguration,
    /// 400 InvalidRequest（结构/组合错误）。
    Invalid(String),
    /// 400 InvalidStorageClass。
    InvalidClass(String),
    /// 400 InvalidArgument（days 越界 / ID 超长）。
    InvalidArgument(String),
    /// 501 NotImplemented（线格式中本实现不支持的字段）。
    NotImplemented(String),
    /// 503 ServiceUnavailable（控制面不可用/滚动升级未激活；fail-closed，重试幂等）。
    Unavailable(String),
    Internal(String),
}

impl std::fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoSuchBucket => write!(f, "no such bucket"),
            Self::NoSuchConfiguration => write!(f, "no such lifecycle configuration"),
            Self::Invalid(m)
            | Self::InvalidClass(m)
            | Self::InvalidArgument(m)
            | Self::NotImplemented(m)
            | Self::Unavailable(m)
            | Self::Internal(m) => write!(f, "{m}"),
        }
    }
}

/// 读取规则存储（oss-server 注入 SQL 实现；xl-s3 不写 jobs/业务库）。
#[async_trait::async_trait]
pub trait LifecycleStore: Send + Sync {
    async fn get_rules(
        &self,
        bucket: &str,
    ) -> Result<Option<Vec<LifecycleRuleSpec>>, LifecycleError>;
    async fn put_rules(
        &self,
        bucket: &str,
        rules: &[LifecycleRuleSpec],
    ) -> Result<(), LifecycleError>;
    async fn delete_rules(&self, bucket: &str) -> Result<(), LifecycleError>;
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 与时间/tier 配置无关的安全约束（PUT 校验与读路径共用）。
fn validate_spec(r: &LifecycleRuleSpec) -> Result<(), LifecycleError> {
    let f = &r.filter;
    if f.prefix.len() > MAX_PREFIX_BYTES {
        return Err(LifecycleError::InvalidArgument(format!(
            "prefix 超过 {MAX_PREFIX_BYTES} 字节"
        )));
    }
    if f.prefix.chars().any(char::is_control) {
        return Err(LifecycleError::InvalidArgument(
            "prefix 含控制字符".to_string(),
        ));
    }
    if f.tags.len() > MAX_TAGS {
        return Err(LifecycleError::Invalid(format!(
            "Filter 标签过多（{} > {MAX_TAGS}）",
            f.tags.len()
        )));
    }
    let mut keys = std::collections::HashSet::new();
    for t in &f.tags {
        if t.key.is_empty() || t.key.chars().count() > MAX_TAG_KEY_CHARS {
            return Err(LifecycleError::InvalidArgument(format!(
                "标签键长度非法: {:?}",
                t.key
            )));
        }
        if t.value.chars().count() > MAX_TAG_VALUE_CHARS {
            return Err(LifecycleError::InvalidArgument(format!(
                "标签值超长: {:?}",
                t.key
            )));
        }
        if !keys.insert(t.key.as_str()) {
            return Err(LifecycleError::Invalid(format!("标签键重复: {:?}", t.key)));
        }
    }
    if let (Some(gt), Some(lt)) = (f.size_gt, f.size_lt)
        && gt >= lt
    {
        return Err(LifecycleError::Invalid(
            "ObjectSizeGreaterThan 必须小于 ObjectSizeLessThan".to_string(),
        ));
    }
    if !r.has_action() {
        return Err(LifecycleError::Invalid(
            "每条规则至少要有 Expiration/Transition/Noncurrent 动作".to_string(),
        ));
    }
    if let Some(ExpirationAction::ExpiredObjectDeleteMarker) = r.expiration
        && !f.tags.is_empty()
    {
        return Err(LifecycleError::Invalid(
            "ExpiredObjectDeleteMarker 不能与 Tag 过滤器同时使用".to_string(),
        ));
    }
    match r.expiration {
        Some(ExpirationAction::Days { days }) if days > MAX_DAYS => {
            return Err(LifecycleError::InvalidArgument(format!(
                "Expiration.Days 越界（{days} > {MAX_DAYS}）"
            )));
        }
        Some(ExpirationAction::Date { date }) if date.rem_euclid(86_400) != 0 => {
            return Err(LifecycleError::InvalidArgument(
                "Expiration.Date 必须是 UTC 午夜".to_string(),
            ));
        }
        _ => {}
    }
    let mut classes = std::collections::HashSet::new();
    for t in &r.transitions {
        match (t.days, t.date) {
            (Some(d), None) if d <= MAX_DAYS => {}
            (None, Some(date)) if date.rem_euclid(86_400) == 0 => {}
            (Some(d), None) => {
                return Err(LifecycleError::InvalidArgument(format!(
                    "Transition.Days 越界（{d} > {MAX_DAYS}）"
                )));
            }
            (None, Some(_)) => {
                return Err(LifecycleError::InvalidArgument(
                    "Transition.Date 必须是 UTC 午夜".to_string(),
                ));
            }
            _ => {
                return Err(LifecycleError::Invalid(
                    "Transition 需要且只能有 Days 或 Date".to_string(),
                ));
            }
        }
        if !TRANSITION_CLASSES.contains(&t.class.as_str()) {
            return Err(LifecycleError::InvalidClass(format!(
                "非法 Transition 存储类：{}",
                t.class
            )));
        }
        if !classes.insert(t.class.as_str()) {
            return Err(LifecycleError::Invalid(format!(
                "Transition 存储类重复：{}",
                t.class
            )));
        }
    }
    if let Some(n) = &r.noncurrent_expiration {
        if n.days == 0 || n.days > MAX_DAYS {
            return Err(LifecycleError::InvalidArgument(format!(
                "NoncurrentDays 越界（{} 不在 1..={MAX_DAYS}）",
                n.days
            )));
        }
        if n.newer
            .is_some_and(|v| v == 0 || v > MAX_RETAINED_NONCURRENT)
        {
            return Err(LifecycleError::InvalidArgument(format!(
                "NewerNoncurrentVersions 越界（1..={MAX_RETAINED_NONCURRENT}）"
            )));
        }
        if n.newer.is_some() && f.is_empty() {
            return Err(LifecycleError::Invalid(
                "NewerNoncurrentVersions 需要指定 Filter".to_string(),
            ));
        }
    }
    for t in &r.noncurrent_transitions {
        if t.days == 0 || t.days > MAX_DAYS {
            return Err(LifecycleError::InvalidArgument(format!(
                "NoncurrentDays 越界（{} 不在 1..={MAX_DAYS}）",
                t.days
            )));
        }
        if t.newer
            .is_some_and(|v| v == 0 || v > MAX_RETAINED_NONCURRENT)
        {
            return Err(LifecycleError::InvalidArgument(format!(
                "NewerNoncurrentVersions 越界（1..={MAX_RETAINED_NONCURRENT}）"
            )));
        }
        if t.newer.is_some() && f.is_empty() {
            return Err(LifecycleError::Invalid(
                "NewerNoncurrentVersions 需要指定 Filter".to_string(),
            ));
        }
        if !TRANSITION_CLASSES.contains(&t.class.as_str()) {
            return Err(LifecycleError::InvalidClass(format!(
                "非法 Transition 存储类：{}",
                t.class
            )));
        }
    }
    Ok(())
}

/// 就地补全缺省 rule_id / 归档类 tier，并校验全部约束。
/// 归档类 tier 解析后存**实际 tier id**（之后改 `[[tiers]]` 不静默改目标）。
pub fn validate_rules(
    rules: &mut [LifecycleRuleSpec],
    tier_ids: &[String],
) -> Result<(), LifecycleError> {
    if rules.len() > MAX_RULES {
        return Err(LifecycleError::Invalid(format!(
            "规则过多（{} > {MAX_RULES}）",
            rules.len()
        )));
    }
    // 显式 id 先全部占位（重复显式 id 报错）；自动 id 跳过已占用编号，避免与显式 id 冲突。
    let mut ids = std::collections::HashSet::new();
    for r in rules.iter() {
        let Some(id) = r.rule_id.as_deref().filter(|s| !s.is_empty()) else {
            continue;
        };
        if id.chars().count() > MAX_ID_CHARS {
            return Err(LifecycleError::InvalidArgument(format!(
                "rule_id 超过 {MAX_ID_CHARS} 字符"
            )));
        }
        if !ids.insert(id.to_string()) {
            return Err(LifecycleError::Invalid(format!("rule_id 重复：{id}")));
        }
    }
    let now = now_secs();
    for (i, r) in rules.iter_mut().enumerate() {
        if r.rule_id.as_deref().is_none_or(str::is_empty) {
            let mut n = i + 1;
            while ids.contains(&format!("rule-{n}")) {
                n += 1;
            }
            let id = format!("rule-{n}");
            ids.insert(id.clone());
            r.rule_id = Some(id);
        }
        validate_spec(r)?;
        if let Some(ExpirationAction::Date { date }) = r.expiration
            && date <= now
        {
            return Err(LifecycleError::InvalidArgument(
                "Expiration.Date 必须是未来时间".to_string(),
            ));
        }
        // Transition 阈值严格递增（Days 按 now 折算为绝对时刻，混合形式也可比较）。
        let mut prev: Option<i64> = None;
        for t in &mut r.transitions {
            let threshold = t.threshold_secs(now);
            if prev.is_some_and(|p| threshold <= p) {
                return Err(LifecycleError::Invalid(
                    "Transition 阈值必须严格递增".to_string(),
                ));
            }
            prev = Some(threshold);
            resolve_tier(&mut t.class, &mut t.tier, tier_ids)?;
        }
        for t in &mut r.noncurrent_transitions {
            resolve_tier(&mut t.class, &mut t.tier, tier_ids)?;
        }
    }
    Ok(())
}

/// 归档类必须解析出唯一/显式 tier；非归档类忽略显式 tier（保持 S4b 口径）。
fn resolve_tier(
    class: &mut String,
    tier: &mut Option<String>,
    tier_ids: &[String],
) -> Result<(), LifecycleError> {
    if is_archive_class(class) {
        match tier.as_deref() {
            Some(t) if !t.is_empty() => {
                if !tier_ids.iter().any(|x| x == t) {
                    return Err(LifecycleError::Invalid(format!(
                        "transition_tier {t:?} 未在 [[tiers]] 中配置"
                    )));
                }
            }
            _ if tier_ids.len() == 1 => *tier = Some(tier_ids[0].clone()),
            _ => {
                return Err(LifecycleError::Invalid(format!(
                    "归档类 {class} 需要 transition_tier（已配置 {} 个 tier）",
                    tier_ids.len()
                )));
            }
        }
    } else {
        *tier = None;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
