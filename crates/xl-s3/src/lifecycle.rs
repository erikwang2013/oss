//! M8 S4b：生命周期配置（Expiration + Transition）——共享类型、校验与 s3s 线格式映射。
//!
//! console 与 S3 两个入口共用 [`validate_rules`]（`SqlLifecycleStore::put_rules` 内调用），
//! 因此任何入口都无法绕过同一份约束。

use s3s::dto::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
    LifecycleRuleFilter, Transition, TransitionStorageClass,
};

use xl_store::meta::is_archive_class;

pub const MAX_RULES: usize = 100;
pub const MAX_DAYS: u32 = 36_500;
pub const MAX_PREFIX_BYTES: usize = 1024;
pub const MAX_ID_CHARS: usize = 255;

/// Transition 允许的存储类（S4a 白名单的子集：不含 STANDARD/REDUCED_REDUNDANCY）。
pub const TRANSITION_CLASSES: [&str; 6] = [
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER",
    "GLACIER_IR",
    "DEEP_ARCHIVE",
];

/// 归一化后的规则（console ↔ DB ↔ S3 共用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleRuleSpec {
    pub rule_id: Option<String>,
    pub prefix: String,
    pub expiration_days: Option<u32>,
    pub transition_days: Option<u32>,
    pub transition_class: Option<String>,
    pub transition_tier: Option<String>,
    pub enabled: bool,
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
    let mut ids = std::collections::HashSet::new();
    let mut prefixes = std::collections::HashSet::new();
    for (i, r) in rules.iter_mut().enumerate() {
        if r.rule_id.as_deref().is_none_or(str::is_empty) {
            r.rule_id = Some(format!("rule-{}", i + 1));
        }
        let id = r.rule_id.as_deref().expect("上面已补全");
        if id.chars().count() > MAX_ID_CHARS {
            return Err(LifecycleError::InvalidArgument(format!(
                "rule_id 超过 {MAX_ID_CHARS} 字符"
            )));
        }
        if !ids.insert(id.to_string()) {
            return Err(LifecycleError::Invalid(format!("rule_id 重复：{id}")));
        }
        if r.prefix.len() > MAX_PREFIX_BYTES {
            return Err(LifecycleError::InvalidArgument(format!(
                "prefix 超过 {MAX_PREFIX_BYTES} 字节"
            )));
        }
        if r.prefix.chars().any(char::is_control) {
            return Err(LifecycleError::InvalidArgument(
                "prefix 含控制字符".to_string(),
            ));
        }
        if !prefixes.insert(r.prefix.clone()) {
            return Err(LifecycleError::Invalid(format!(
                "prefix 重复：{:?}",
                r.prefix
            )));
        }
        if r.expiration_days.is_none() && r.transition_days.is_none() {
            return Err(LifecycleError::Invalid(
                "每条规则至少要有 Expiration 或 Transition".to_string(),
            ));
        }
        for d in [r.expiration_days, r.transition_days].into_iter().flatten() {
            if d > MAX_DAYS {
                return Err(LifecycleError::InvalidArgument(format!(
                    "days 越界（{d} > {MAX_DAYS}）"
                )));
            }
        }
        match (&r.transition_days, &r.transition_class) {
            (None, None) => {}
            (Some(_), Some(class)) => {
                if !TRANSITION_CLASSES.contains(&class.as_str()) {
                    return Err(LifecycleError::InvalidClass(format!(
                        "非法 Transition 存储类：{class}"
                    )));
                }
                if is_archive_class(class) {
                    match r.transition_tier.as_deref() {
                        Some(t) if !t.is_empty() => {
                            if !tier_ids.iter().any(|x| x == t) {
                                return Err(LifecycleError::Invalid(format!(
                                    "transition_tier {t:?} 未在 [[tiers]] 中配置"
                                )));
                            }
                        }
                        _ if tier_ids.len() == 1 => {
                            r.transition_tier = Some(tier_ids[0].clone());
                        }
                        _ => {
                            return Err(LifecycleError::Invalid(format!(
                                "归档类 {class} 需要 transition_tier（已配置 {} 个 tier）",
                                tier_ids.len()
                            )));
                        }
                    }
                } else {
                    // 非归档类不需要远端 tier（显式给出也忽略）。
                    r.transition_tier = None;
                }
            }
            _ => {
                return Err(LifecycleError::Invalid(
                    "transition_days 与 transition_class 必须同时出现".to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// 不支持的线格式字段：先整体拒绝（501），再解析受支持动作（400）。
fn reject_unsupported(rule: &LifecycleRule) -> Result<(), LifecycleError> {
    if rule.abort_incomplete_multipart_upload.is_some() {
        return Err(LifecycleError::NotImplemented(
            "AbortIncompleteMultipartUpload".into(),
        ));
    }
    if rule.noncurrent_version_expiration.is_some() || rule.noncurrent_version_transitions.is_some()
    {
        return Err(LifecycleError::NotImplemented("NoncurrentVersion*".into()));
    }
    if let Some(f) = &rule.filter
        && (f.tag.is_some()
            || f.object_size_greater_than.is_some()
            || f.object_size_less_than.is_some()
            || f.and.is_some())
    {
        return Err(LifecycleError::NotImplemented(
            "Filter Tag/ObjectSize/And".into(),
        ));
    }
    if let Some(e) = &rule.expiration
        && (e.date.is_some() || e.expired_object_delete_marker.is_some())
    {
        return Err(LifecycleError::NotImplemented(
            "Expiration.Date/ExpiredObjectDeleteMarker".into(),
        ));
    }
    if let Some(ts) = &rule.transitions {
        if ts.len() > 1 {
            return Err(LifecycleError::NotImplemented("多条 Transition".into()));
        }
        if ts.iter().any(|t| t.date.is_some()) {
            return Err(LifecycleError::NotImplemented("Transition.Date".into()));
        }
    }
    Ok(())
}

fn prefix_of(rule: &LifecycleRule) -> Result<String, LifecycleError> {
    let legacy = rule.prefix.as_deref();
    let filtered = rule
        .filter
        .as_ref()
        .and_then(|f| f.prefix.as_ref())
        .map(|p| p.as_str());
    match (legacy, filtered) {
        (Some(a), Some(b)) if a != b => Err(LifecycleError::Invalid(
            "Rule.Prefix 与 Filter.Prefix 不一致".into(),
        )),
        (Some(p), _) | (None, Some(p)) => Ok(p.to_string()),
        (None, None) => Ok(String::new()),
    }
}

/// s3s 的配置体 → 规则列表（未注入 tier 配置；归档类 tier 由 `validate_rules` 解析）。
pub fn parse_lifecycle_config(
    cfg: &BucketLifecycleConfiguration,
) -> Result<Vec<LifecycleRuleSpec>, LifecycleError> {
    if cfg.rules.is_empty() {
        return Err(LifecycleError::Invalid("rules 为空".into()));
    }
    if cfg.rules.len() > MAX_RULES {
        return Err(LifecycleError::Invalid(format!(
            "规则过多（{} > {MAX_RULES}）",
            cfg.rules.len()
        )));
    }
    for r in &cfg.rules {
        reject_unsupported(r)?;
    }
    let mut out = Vec::with_capacity(cfg.rules.len());
    for r in &cfg.rules {
        let status = r.status.as_str();
        if status != ExpirationStatus::ENABLED && status != ExpirationStatus::DISABLED {
            return Err(LifecycleError::InvalidArgument(format!(
                "Status 非法：{status}"
            )));
        }
        let expiration_days = match &r.expiration {
            Some(LifecycleExpiration { days: Some(d), .. }) => {
                Some(u32::try_from(*d).map_err(|_| {
                    LifecycleError::InvalidArgument(format!("Expiration.Days 非法：{d}"))
                })?)
            }
            Some(_) => {
                return Err(LifecycleError::Invalid("Expiration 只支持 Days".into()));
            }
            None => None,
        };
        let (transition_days, transition_class) = match r.transitions.as_deref() {
            Some([t]) => {
                let d = t
                    .days
                    .ok_or_else(|| LifecycleError::Invalid("Transition 缺 Days".into()))?;
                let class = t
                    .storage_class
                    .as_ref()
                    .ok_or_else(|| LifecycleError::Invalid("Transition 缺 StorageClass".into()))?;
                (
                    Some(u32::try_from(d).map_err(|_| {
                        LifecycleError::InvalidArgument(format!("Transition.Days 非法：{d}"))
                    })?),
                    Some(class.as_str().to_string()),
                )
            }
            Some(_) => (None, None),
            None => (None, None),
        };
        out.push(LifecycleRuleSpec {
            rule_id: r.id.as_ref().map(|v| v.as_str().to_string()),
            prefix: prefix_of(r)?,
            expiration_days,
            transition_days,
            transition_class,
            transition_tier: None,
            enabled: status == ExpirationStatus::ENABLED,
        });
    }
    Ok(out)
}

/// 规则列表 → s3s 回读形态（按调用方给出的顺序；writer 只输出 filter.prefix）。
pub fn rules_to_dto(rules: &[LifecycleRuleSpec]) -> Vec<LifecycleRule> {
    rules
        .iter()
        .map(|r| LifecycleRule {
            abort_incomplete_multipart_upload: None,
            expiration: r.expiration_days.map(|d| LifecycleExpiration {
                days: Some(d as i32),
                ..Default::default()
            }),
            filter: Some(LifecycleRuleFilter {
                prefix: Some(r.prefix.clone()),
                ..Default::default()
            }),
            id: r.rule_id.clone(),
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            status: ExpirationStatus::from(if r.enabled {
                ExpirationStatus::ENABLED.to_string()
            } else {
                ExpirationStatus::DISABLED.to_string()
            }),
            transitions: r.transition_class.as_ref().map(|c| {
                vec![Transition {
                    days: r.transition_days.map(|d| d as i32),
                    storage_class: Some(TransitionStorageClass::from(c.clone())),
                    ..Default::default()
                }]
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests;
