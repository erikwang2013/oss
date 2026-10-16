//! M5 切片 4：对象锁的 S3 面——锁头解析、响应装配、桶级配置翻译（裁决 L1-L7）。
//! 守卫本体在 xl-store 删除路径（`check_lock_delete`）；本模块只管「线格式 ↔ 内核形态」。
//! 客户端可纠正形态 → 4xx（InvalidRequest/InvalidArgument）；桶状态类 → 400 InvalidBucketState。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3s::S3Result;
use s3s::dto::*;
use xl_store::meta::ObjectLockRecord;

const DAY_NANOS: i64 = 86_400_000_000_000;

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// 纳秒 → S3 Timestamp（穿 SystemTime；负值钳 0）。
pub fn ts_from_nanos(nanos: i64) -> Timestamp {
    Timestamp::from(UNIX_EPOCH + Duration::from_nanos(nanos.max(0) as u64))
}

/// S3 Timestamp → 纳秒（穿 time::OffsetDateTime；超 i64 钳 MAX——远未来值 saturate 安全）。
fn nanos_of(ts: &Timestamp) -> i64 {
    let odt: time::OffsetDateTime = ts.clone().into();
    i64::try_from(odt.unix_timestamp_nanos()).unwrap_or(i64::MAX)
}

/// 解析 Put/Copy/CreateMPU 的对象锁头 → 记录（L3）：
/// - legal hold 独立（ON/OFF）；mode 与 until 必须**成对**且 until 在将来；mode 白名单大小写严格
///   （aws-cli 恒大写；不宽容=「猜」的宽口径，记档）。
pub fn parse_put_lock(
    mode: Option<&str>,
    until: Option<&Timestamp>,
    legal_hold: Option<&str>,
) -> S3Result<Option<ObjectLockRecord>> {
    let legal = match legal_hold {
        None => false,
        Some(ObjectLockLegalHoldStatus::ON) => true,
        Some(ObjectLockLegalHoldStatus::OFF) => false,
        Some(_) => {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "invalid object lock legal hold status"
            ));
        }
    };
    match (mode, until) {
        (None, None) => Ok(legal.then_some(ObjectLockRecord {
            mode: None,
            retain_until_nanos: None,
            legal_hold: true,
        })),
        (Some(m), Some(t)) => {
            if m != ObjectLockMode::GOVERNANCE && m != ObjectLockMode::COMPLIANCE {
                return Err(s3s::s3_error!(InvalidRequest, "invalid object lock mode"));
            }
            let until_nanos = nanos_of(t);
            if until_nanos <= now_nanos() {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "object lock retain-until-date must be in the future"
                ));
            }
            Ok(Some(ObjectLockRecord {
                mode: Some(m.to_string()),
                retain_until_nanos: Some(until_nanos),
                legal_hold: legal,
            }))
        }
        _ => Err(s3s::s3_error!(
            InvalidRequest,
            "object lock mode and retain-until-date must be provided together"
        )),
    }
}

/// 默认保留天数上限（AWS 36500 天口径；N3 复验补：防 `now + days·86400e9` i64 溢出——
/// release 回绕产出负 until 会让对象 decode 即 CorruptMeta 自伤）。
pub const MAX_DEFAULT_RETENTION_DAYS: i64 = 36_500;

/// 桶默认保留规则的落地记录（L2；无显式头且桶开锁时的 put 默认）。saturating 双保险。
pub fn default_retention_record(mode: &str, days: i64) -> ObjectLockRecord {
    let add = days.max(0).saturating_mul(DAY_NANOS);
    ObjectLockRecord {
        mode: Some(mode.to_string()),
        retain_until_nanos: Some(now_nanos().saturating_add(add)),
        legal_hold: false,
    }
}

/// 记录 → Get/Head 响应三字段（L6 响应面）。
pub fn apply_lock_out(
    rec: Option<&ObjectLockRecord>,
    mode: &mut Option<ObjectLockMode>,
    until: &mut Option<ObjectLockRetainUntilDate>,
    legal: &mut Option<ObjectLockLegalHoldStatus>,
) {
    let Some(l) = rec else { return };
    if let Some(m) = &l.mode {
        *mode = Some(ObjectLockMode::from(m.clone()));
    }
    if let Some(u) = l.retain_until_nanos {
        *until = Some(ts_from_nanos(u));
    }
    if l.legal_hold {
        *legal = Some(ObjectLockLegalHoldStatus::from_static("ON"));
    }
}

/// 桶级配置请求 →（默认模式, 默认天数）：`PutObjectLockConfiguration` 的 rule 翻译。
/// years×365 归一为天（L2 记档）；rule 缺省 = 只有 Enabled、无默认规则。
pub fn parse_bucket_lock_rule(
    cfg: Option<&ObjectLockConfiguration>,
) -> S3Result<(Option<String>, Option<i64>)> {
    let Some(c) = cfg else {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "missing object lock configuration"
        ));
    };
    if let Some(e) = &c.object_lock_enabled
        && e.as_str() != ObjectLockEnabled::ENABLED
    {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "object lock can only be enabled, never disabled"
        ));
    }
    let Some(rule) = &c.rule else {
        return Ok((None, None));
    };
    let Some(dr) = &rule.default_retention else {
        return Ok((None, None));
    };
    let mode = match dr.mode.as_ref().map(|m| m.as_str().to_string()) {
        Some(m)
            if m == ObjectLockRetentionMode::GOVERNANCE
                || m == ObjectLockRetentionMode::COMPLIANCE =>
        {
            m
        }
        _ => {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "default retention mode must be GOVERNANCE or COMPLIANCE"
            ));
        }
    };
    let days = match (dr.days, dr.years) {
        (Some(d), None) if d > 0 => d as i64,
        (None, Some(y)) if y > 0 => (y as i64).saturating_mul(365),
        _ => {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "default retention requires exactly one of days/years (positive)"
            ));
        }
    };
    if days > MAX_DEFAULT_RETENTION_DAYS {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "default retention exceeds 36500 days (N3 上限)"
        ));
    }
    Ok((Some(mode), Some(days)))
}

/// （模式, 天数）→ `GetObjectLockConfiguration` 响应（L2；天数拆 days 输出，不折 years）。
pub fn bucket_lock_out(mode: Option<&str>, days: Option<i64>) -> ObjectLockConfiguration {
    let rule = mode.map(|m| ObjectLockRule {
        default_retention: Some(DefaultRetention {
            mode: Some(ObjectLockRetentionMode::from(m.to_string())),
            days: days.map(|d| d as i32),
            years: None,
        }),
    });
    ObjectLockConfiguration {
        object_lock_enabled: Some(ObjectLockEnabled::from_static("Enabled")),
        rule,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn future_ts(secs: i64) -> Timestamp {
        ts_from_nanos(now_nanos() + secs * 1_000_000_000)
    }

    #[test]
    fn parse_put_lock_matrix() {
        // 成对合规
        let r = parse_put_lock(Some("GOVERNANCE"), Some(&future_ts(3600)), None)
            .unwrap()
            .unwrap();
        assert_eq!(r.mode.as_deref(), Some("GOVERNANCE"));
        assert!(r.retain_until_nanos.unwrap() > now_nanos());
        // legal hold 独立
        let r = parse_put_lock(None, None, Some("ON")).unwrap().unwrap();
        assert!(r.legal_hold && r.mode.is_none());
        assert!(parse_put_lock(None, None, Some("OFF")).unwrap().is_none());
        // 不成对 / 过去时间 / 非法 mode / 非法 hold → 400
        assert!(parse_put_lock(Some("GOVERNANCE"), None, None).is_err());
        assert!(parse_put_lock(None, Some(&future_ts(1)), None).is_err());
        assert!(parse_put_lock(Some("governance"), Some(&future_ts(9)), None).is_err());
        let past = ts_from_nanos(now_nanos() - 1_000_000_000);
        assert!(parse_put_lock(Some("COMPLIANCE"), Some(&past), None).is_err());
        assert!(parse_put_lock(None, None, Some("MAYBE")).is_err());
    }

    #[test]
    fn bucket_rule_parse_and_out() {
        // days 直存
        let cfg = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static("Enabled")),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static("COMPLIANCE")),
                    days: Some(30),
                    years: None,
                }),
            }),
        };
        let (m, d) = parse_bucket_lock_rule(Some(&cfg)).unwrap();
        assert_eq!((m.as_deref(), d), (Some("COMPLIANCE"), Some(30)));
        // years 归一
        let cfg2 = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static("Enabled")),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static("GOVERNANCE")),
                    days: None,
                    years: Some(2),
                }),
            }),
        };
        let (m, d) = parse_bucket_lock_rule(Some(&cfg2)).unwrap();
        assert_eq!((m.as_deref(), d), (Some("GOVERNANCE"), Some(730)));
        // 双给 / 零值 / 非法 mode → 400
        let bad = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static("Enabled")),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static("GOVERNANCE")),
                    days: Some(1),
                    years: Some(1),
                }),
            }),
        };
        assert!(parse_bucket_lock_rule(Some(&bad)).is_err());
        // 无 rule → (None, None)；缺配置 → 400
        let no_rule = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static("Enabled")),
            rule: None,
        };
        assert_eq!(
            parse_bucket_lock_rule(Some(&no_rule)).unwrap(),
            (None, None)
        );
        assert!(parse_bucket_lock_rule(None).is_err());
        // 输出映射
        let out = bucket_lock_out(Some("COMPLIANCE"), Some(30));
        let dr = out.rule.unwrap().default_retention.unwrap();
        assert_eq!(dr.days, Some(30));
    }
}
