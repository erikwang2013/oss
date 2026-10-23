//! S3 线格式 ↔ [`LifecycleRuleSpec`]：解析（PUT）与回读（GET）。

use s3s::dto::{
    AbortIncompleteMultipartUpload, LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator,
    LifecycleRuleFilter, NoncurrentVersionExpiration, NoncurrentVersionTransition, Timestamp,
    Transition, TransitionStorageClass,
};

use super::{
    ExpirationAction, LifecycleError, LifecycleFilter, LifecycleRuleSpec, MAX_RULES,
    NoncurrentExpiration, NoncurrentTransition, TransitionAction,
};
use xl_store::meta::ObjectTag;

use s3s::dto::BucketLifecycleConfiguration;

fn invalid(msg: impl Into<String>) -> LifecycleError {
    LifecycleError::Invalid(msg.into())
}

fn epoch_secs(ts: &Timestamp) -> i64 {
    time::OffsetDateTime::from(ts.clone()).unix_timestamp()
}

fn timestamp(secs: i64) -> Timestamp {
    match time::OffsetDateTime::from_unix_timestamp(secs) {
        Ok(t) => Timestamp::from(t),
        Err(_) => Timestamp::from(time::OffsetDateTime::UNIX_EPOCH),
    }
}

fn u32_of(v: i64, what: &str) -> Result<u32, LifecycleError> {
    u32::try_from(v).map_err(|_| LifecycleError::InvalidArgument(format!("{what} 非法：{v}")))
}

fn size_bound(v: Option<i64>, what: &str) -> Result<Option<u64>, LifecycleError> {
    v.map(|x| {
        u64::try_from(x)
            .map_err(|_| LifecycleError::InvalidArgument(format!("{what} 不能为负：{x}")))
    })
    .transpose()
}

fn parse_tag(t: &s3s::dto::Tag) -> Result<ObjectTag, LifecycleError> {
    let key = t
        .key
        .as_deref()
        .filter(|k| !k.is_empty())
        .ok_or_else(|| invalid("Tag 过滤缺 Key"))?;
    Ok(ObjectTag {
        key: key.to_string(),
        value: t.value.clone().unwrap_or_default(),
    })
}

fn prefix_of(rule: &LifecycleRule) -> Result<String, LifecycleError> {
    let legacy = rule.prefix.as_deref();
    let filtered = rule
        .filter
        .as_ref()
        .and_then(|f| f.prefix.as_ref())
        .map(|p| p.as_str());
    match (legacy, filtered) {
        (Some(a), Some(b)) if a != b => Err(invalid("Rule.Prefix 与 Filter.Prefix 不一致")),
        (Some(p), _) | (None, Some(p)) => Ok(p.to_string()),
        (None, None) => Ok(String::new()),
    }
}

/// `Filter` 的四种互斥形态（Prefix / Tag / ObjectSize* / And）。
fn parse_filter(rule: &LifecycleRule) -> Result<LifecycleFilter, LifecycleError> {
    let Some(f) = rule.filter.as_ref() else {
        return Ok(LifecycleFilter {
            prefix: prefix_of(rule)?,
            ..Default::default()
        });
    };
    if let Some(and) = f.and.as_ref() {
        if f.prefix.is_some()
            || f.tag.is_some()
            || f.object_size_greater_than.is_some()
            || f.object_size_less_than.is_some()
        {
            return Err(invalid(
                "Filter 只能指定 Prefix/Tag/ObjectSizeGreaterThan/ObjectSizeLessThan/And 之一",
            ));
        }
        let prefix = match (rule.prefix.as_deref(), and.prefix.as_deref()) {
            (Some(a), Some(b)) if a != b => {
                return Err(invalid("Rule.Prefix 与 Filter.And.Prefix 不一致"));
            }
            (Some(p), _) | (None, Some(p)) => p.to_string(),
            (None, None) => String::new(),
        };
        return Ok(LifecycleFilter {
            prefix,
            tags: and
                .tags
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(parse_tag)
                .collect::<Result<_, _>>()?,
            size_gt: size_bound(and.object_size_greater_than, "ObjectSizeGreaterThan")?,
            size_lt: size_bound(and.object_size_less_than, "ObjectSizeLessThan")?,
        });
    }
    Ok(LifecycleFilter {
        prefix: prefix_of(rule)?,
        tags: f
            .tag
            .as_ref()
            .map(parse_tag)
            .transpose()?
            .into_iter()
            .collect(),
        size_gt: size_bound(f.object_size_greater_than, "ObjectSizeGreaterThan")?,
        size_lt: size_bound(f.object_size_less_than, "ObjectSizeLessThan")?,
    })
}

fn parse_expiration(e: &LifecycleExpiration) -> Result<Option<ExpirationAction>, LifecycleError> {
    let eodm = e.expired_object_delete_marker.unwrap_or(false);
    match (e.days, e.date.as_ref(), eodm) {
        (Some(_), Some(_), _) => Err(invalid("Expiration 只能指定 Days 或 Date 之一")),
        (Some(_), None, true) => Err(invalid(
            "Expiration.Days 与 ExpiredObjectDeleteMarker 不能同时出现",
        )),
        (Some(d), None, false) => Ok(Some(ExpirationAction::Days {
            days: u32_of(i64::from(d), "Expiration.Days")?,
        })),
        (None, Some(date), false) => Ok(Some(ExpirationAction::Date {
            date: epoch_secs(date),
        })),
        (None, None, true) => Ok(Some(ExpirationAction::ExpiredObjectDeleteMarker)),
        (None, None, false) => Ok(None),
        (None, Some(_), true) => Err(invalid(
            "Expiration.Date 与 ExpiredObjectDeleteMarker 不能同时出现",
        )),
    }
}

fn transition_class(class: Option<&TransitionStorageClass>) -> Result<String, LifecycleError> {
    class
        .map(|c| c.as_str().to_string())
        .ok_or_else(|| invalid("Transition 缺 StorageClass"))
}

fn parse_transitions(rule: &LifecycleRule) -> Result<Vec<TransitionAction>, LifecycleError> {
    let mut out = Vec::new();
    for t in rule.transitions.as_deref().unwrap_or_default() {
        let days = t
            .days
            .map(|d| u32_of(i64::from(d), "Transition.Days"))
            .transpose()?;
        let date = t.date.as_ref().map(epoch_secs);
        match (days, date) {
            (Some(_), Some(_)) | (None, None) => {
                return Err(invalid("Transition 需要且只能有 Days 或 Date"));
            }
            _ => {}
        }
        out.push(TransitionAction {
            days,
            date,
            class: transition_class(t.storage_class.as_ref())?,
            tier: None,
        });
    }
    Ok(out)
}

fn parse_noncurrent_expiration(
    n: &NoncurrentVersionExpiration,
) -> Result<Option<NoncurrentExpiration>, LifecycleError> {
    let Some(days) = n.noncurrent_days else {
        if n.newer_noncurrent_versions.is_some() {
            return Err(invalid("NoncurrentVersionExpiration 缺 NoncurrentDays"));
        }
        return Ok(None);
    };
    Ok(Some(NoncurrentExpiration {
        days: u32_of(i64::from(days), "NoncurrentDays")?,
        newer: n
            .newer_noncurrent_versions
            .map(|v| u32_of(i64::from(v), "NewerNoncurrentVersions"))
            .transpose()?,
    }))
}

fn parse_noncurrent_transitions(
    rule: &LifecycleRule,
) -> Result<Vec<NoncurrentTransition>, LifecycleError> {
    let mut out = Vec::new();
    for t in rule
        .noncurrent_version_transitions
        .as_deref()
        .unwrap_or_default()
    {
        let days = t
            .noncurrent_days
            .ok_or_else(|| invalid("NoncurrentVersionTransition 缺 NoncurrentDays"))?;
        out.push(NoncurrentTransition {
            days: u32_of(i64::from(days), "NoncurrentDays")?,
            newer: t
                .newer_noncurrent_versions
                .map(|v| u32_of(i64::from(v), "NewerNoncurrentVersions"))
                .transpose()?,
            class: transition_class(t.storage_class.as_ref())?,
            tier: None,
        });
    }
    Ok(out)
}

/// s3s 的配置体 → 规则列表（未注入 tier 配置；归档类 tier 由 `validate_rules` 解析）。
pub fn parse_lifecycle_config(
    cfg: &BucketLifecycleConfiguration,
) -> Result<Vec<LifecycleRuleSpec>, LifecycleError> {
    if cfg.rules.is_empty() {
        return Err(invalid("rules 为空"));
    }
    if cfg.rules.len() > MAX_RULES {
        return Err(invalid(format!(
            "规则过多（{} > {MAX_RULES}）",
            cfg.rules.len()
        )));
    }
    let mut out = Vec::with_capacity(cfg.rules.len());
    for r in &cfg.rules {
        // 3.10：MPU 列表本身可枚举（Store::list_multipart_uploads），按天数直接支持。
        let abort_mpu_days = r
            .abort_incomplete_multipart_upload
            .as_ref()
            .map(|a| {
                let days = a.days_after_initiation.ok_or_else(|| {
                    invalid("AbortIncompleteMultipartUpload 缺 DaysAfterInitiation")
                })?;
                u32_of(i64::from(days), "DaysAfterInitiation")
            })
            .transpose()?;
        let status = r.status.as_str();
        if status != s3s::dto::ExpirationStatus::ENABLED
            && status != s3s::dto::ExpirationStatus::DISABLED
        {
            return Err(LifecycleError::InvalidArgument(format!(
                "Status 非法：{status}"
            )));
        }
        out.push(LifecycleRuleSpec {
            rule_id: r.id.as_ref().map(|v| v.as_str().to_string()),
            filter: parse_filter(r)?,
            expiration: r
                .expiration
                .as_ref()
                .map(parse_expiration)
                .transpose()?
                .flatten(),
            transitions: parse_transitions(r)?,
            noncurrent_expiration: r
                .noncurrent_version_expiration
                .as_ref()
                .map(parse_noncurrent_expiration)
                .transpose()?
                .flatten(),
            noncurrent_transitions: parse_noncurrent_transitions(r)?,
            abort_mpu_days,
            enabled: status == s3s::dto::ExpirationStatus::ENABLED,
        });
    }
    Ok(out)
}

/// 过滤器 → 精确回读形态（Prefix / Tag / ObjectSize* / And 互斥）。
fn filter_to_dto(f: &LifecycleFilter) -> LifecycleRuleFilter {
    let mut dto = LifecycleRuleFilter::default();
    let single_tag =
        f.tags.len() == 1 && f.prefix.is_empty() && f.size_gt.is_none() && f.size_lt.is_none();
    let single_size =
        f.tags.is_empty() && f.prefix.is_empty() && f.size_gt.is_some() != f.size_lt.is_some();
    if single_tag {
        let t = &f.tags[0];
        dto.tag = Some(s3s::dto::Tag {
            key: Some(t.key.clone()),
            value: Some(t.value.clone()),
        });
    } else if single_size {
        dto.object_size_greater_than = f.size_gt.map(|v| v as i64);
        dto.object_size_less_than = f.size_lt.map(|v| v as i64);
    } else if f.tags.is_empty() && f.size_gt.is_none() && f.size_lt.is_none() {
        dto.prefix = Some(f.prefix.clone());
    } else {
        dto.and = Some(LifecycleRuleAndOperator {
            object_size_greater_than: f.size_gt.map(|v| v as i64),
            object_size_less_than: f.size_lt.map(|v| v as i64),
            prefix: (!f.prefix.is_empty()).then(|| f.prefix.clone()),
            tags: (!f.tags.is_empty()).then(|| {
                f.tags
                    .iter()
                    .map(|t| s3s::dto::Tag {
                        key: Some(t.key.clone()),
                        value: Some(t.value.clone()),
                    })
                    .collect()
            }),
        });
    }
    dto
}

fn expiration_to_dto(a: &ExpirationAction) -> LifecycleExpiration {
    match a {
        ExpirationAction::Days { days } => LifecycleExpiration {
            days: Some(*days as i32),
            ..Default::default()
        },
        ExpirationAction::Date { date } => LifecycleExpiration {
            date: Some(timestamp(*date)),
            ..Default::default()
        },
        ExpirationAction::ExpiredObjectDeleteMarker => LifecycleExpiration {
            expired_object_delete_marker: Some(true),
            ..Default::default()
        },
    }
}

/// 规则列表 → s3s 回读形态（按调用方给出的顺序）。
pub fn rules_to_dto(rules: &[LifecycleRuleSpec]) -> Vec<LifecycleRule> {
    rules
        .iter()
        .map(|r| LifecycleRule {
            abort_incomplete_multipart_upload: r.abort_mpu_days.map(|d| {
                AbortIncompleteMultipartUpload {
                    days_after_initiation: Some(d as i32),
                }
            }),
            expiration: r.expiration.as_ref().map(expiration_to_dto),
            filter: Some(filter_to_dto(&r.filter)),
            id: r.rule_id.clone(),
            noncurrent_version_expiration: r.noncurrent_expiration.as_ref().map(|n| {
                NoncurrentVersionExpiration {
                    noncurrent_days: Some(n.days as i32),
                    newer_noncurrent_versions: n.newer.map(|v| v as i32),
                }
            }),
            noncurrent_version_transitions: (!r.noncurrent_transitions.is_empty()).then(|| {
                r.noncurrent_transitions
                    .iter()
                    .map(|t| NoncurrentVersionTransition {
                        noncurrent_days: Some(t.days as i32),
                        newer_noncurrent_versions: t.newer.map(|v| v as i32),
                        storage_class: Some(TransitionStorageClass::from(t.class.clone())),
                    })
                    .collect()
            }),
            prefix: None,
            status: s3s::dto::ExpirationStatus::from(if r.enabled {
                s3s::dto::ExpirationStatus::ENABLED.to_string()
            } else {
                s3s::dto::ExpirationStatus::DISABLED.to_string()
            }),
            transitions: (!r.transitions.is_empty()).then(|| {
                r.transitions
                    .iter()
                    .map(|t| Transition {
                        days: t.days.map(|d| d as i32),
                        date: t.date.map(timestamp),
                        storage_class: Some(TransitionStorageClass::from(t.class.clone())),
                    })
                    .collect()
            }),
        })
        .collect()
}
