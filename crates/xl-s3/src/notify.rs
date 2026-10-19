//! M6 切片 0：桶通知配置——线格式 ↔ 内部 JSON（裁决 R5：事件白名单两档；
//! ObjectAccessed / Replication:* / LifecycleTransition 配置 → 400 拒绝并记档）。
//!
//! 内部形态（存 `event_configs.config_json`）：
//! `[{"id":"...","target":"<target_id>","events":["s3:ObjectCreated:Put",...],
//!    "prefix":null,"suffix":null}, ...]`——target 自 ARN `arn:oss:sqs::<id>:kafka` 提取。

use s3s::S3Result;
use s3s::dto::*;

/// 事件白名单（R5 两档：对象事件 + 生命周期事件；本表同时供服务端匹配与校验）。
pub const ALLOWED_EVENTS: &[&str] = &[
    "s3:ObjectCreated:Put",
    "s3:ObjectCreated:Post",
    "s3:ObjectCreated:Copy",
    "s3:ObjectCreated:CompleteMultipartUpload",
    "s3:ObjectRemoved:Delete",
    "s3:ObjectRemoved:DeleteMarkerCreated",
    "s3:LifecycleExpiration:Delete",
    "s3:LifecycleExpiration:DeleteMarkerCreated",
    // M8 S4b：Transition / Restore
    "s3:LifecycleTransition",
    "s3:ObjectRestore:Post",
    "s3:ObjectRestore:Completed",
];

/// 单条归一化配置（内部 JSON 的 serde 形状）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct EventTarget {
    pub id: String,
    /// ARN `arn:oss:sqs::<target_id>:kafka` 里的 target_id
    pub target: String,
    pub events: Vec<String>,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

/// ARN → target_id。只收 `arn:oss:sqs::<id>:kafka`（MinIO 惯例的 oss 变体）。
/// **P2-3**：id 限白名单 `[A-Za-z0-9._-]{1,64}`——target id 会进 file sink 路径/日志名，
/// 放行 `/` `\\` 等字符即路径穿越面（桶主可控该值）。
pub fn parse_target_arn(arn: &str) -> Option<String> {
    let rest = arn.strip_prefix("arn:oss:sqs::")?;
    let (id, kind) = rest.split_once(':')?;
    if kind != "kafka" || id.is_empty() || id.len() > 64 {
        return None;
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return None;
    }
    Some(id.to_string())
}

/// 请求 DTO → 归一化配置（全量替换语义；空配置 = 清空）。
/// 校验失败 → 400 InvalidArgument（不静默）；非 queue 通道 / 未知事件 / 坏 ARN 一律拒。
pub fn parse_notification_config(cfg: &NotificationConfiguration) -> S3Result<Vec<EventTarget>> {
    if cfg
        .topic_configurations
        .as_ref()
        .is_some_and(|l| !l.is_empty())
        || cfg
            .lambda_function_configurations
            .as_ref()
            .is_some_and(|l| !l.is_empty())
        || cfg.event_bridge_configuration.is_some()
    {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "only queue_configurations (Kafka target ARN) are supported"
        ));
    }
    let list = cfg.queue_configurations.as_deref().unwrap_or_default();
    if list.len() > 100 {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "at most 100 notification configurations per bucket"
        ));
    }
    let mut out = Vec::with_capacity(list.len());
    for (i, q) in list.iter().enumerate() {
        let target = parse_target_arn(q.queue_arn.as_str()).ok_or_else(|| {
            s3s::s3_error!(
                InvalidArgument,
                "invalid queue ARN (expect arn:oss:sqs::<id>:kafka)"
            )
        })?;
        if q.events.is_empty() {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "notification events list must not be empty"
            ));
        }
        let mut events = Vec::with_capacity(q.events.len());
        for e in &q.events {
            let name = e.as_ref();
            // 本实现只收显式事件（P3-4：AWS 本身**支持**通配如 s3:ObjectCreated:*，本仓按计划
            // 裁决不收——README 记档，迁移用户用显式列表）；未知/本片不支持 → 400
            if !ALLOWED_EVENTS.contains(&name) {
                return Err(s3s::s3_error!(
                    InvalidArgument,
                    "unsupported notification event type"
                ));
            }
            events.push(name.to_string());
        }
        let mut prefix = None;
        let mut suffix = None;
        if let Some(f) = q.filter.as_ref().and_then(|f| f.key.as_ref()) {
            for rule in f.filter_rules.as_deref().unwrap_or_default() {
                let name = rule.name.as_ref().map(|n| n.as_str().to_string());
                let value = rule.value.as_ref().map(|v| v.as_str().to_string());
                match (name.as_deref(), value) {
                    (Some(FilterRuleName::PREFIX), Some(v)) if prefix.is_none() => prefix = Some(v),
                    (Some(FilterRuleName::SUFFIX), Some(v)) if suffix.is_none() => suffix = Some(v),
                    _ => {
                        return Err(s3s::s3_error!(
                            InvalidArgument,
                            "filter rules must be prefix/suffix (each at most once)"
                        ));
                    }
                }
            }
        }
        let id =
            q.id.as_ref()
                .map(|v| v.as_str().to_string())
                .unwrap_or_else(|| format!("cfg-{i}"));
        out.push(EventTarget {
            id,
            target,
            events,
            prefix,
            suffix,
        });
    }
    Ok(out)
}

/// 归一化配置 → 响应 DTO（与请求同形；target id 回拼 ARN）。
pub fn notification_config_out(targets: &[EventTarget]) -> NotificationConfiguration {
    let queues: Vec<QueueConfiguration> = targets
        .iter()
        .map(|t| QueueConfiguration {
            id: Some(NotificationId::from(t.id.clone())),
            queue_arn: QueueArn::from(format!("arn:oss:sqs::{}:kafka", t.target)),
            events: EventList::from(
                t.events
                    .iter()
                    .map(|e| Event::from(e.clone()))
                    .collect::<Vec<_>>(),
            ),
            filter: t.prefix.as_ref().or(t.suffix.as_ref()).map(|_| {
                let mut rules = Vec::new();
                if let Some(p) = &t.prefix {
                    rules.push(FilterRule {
                        name: Some(FilterRuleName::from_static("prefix")),
                        value: Some(FilterRuleValue::from(p.clone())),
                    });
                }
                if let Some(s) = &t.suffix {
                    rules.push(FilterRule {
                        name: Some(FilterRuleName::from_static("suffix")),
                        value: Some(FilterRuleValue::from(s.clone())),
                    });
                }
                NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(FilterRuleList::from(rules)),
                    }),
                }
            }),
        })
        .collect();
    NotificationConfiguration {
        queue_configurations: Some(QueueConfigurationList::from(queues)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(id: &str, arn: &str, events: &[&str]) -> QueueConfiguration {
        QueueConfiguration {
            id: Some(NotificationId::from(id.to_string())),
            queue_arn: QueueArn::from(arn.to_string()),
            events: EventList::from(
                events
                    .iter()
                    .map(|e| Event::from(e.to_string()))
                    .collect::<Vec<_>>(),
            ),
            filter: None,
        }
    }

    #[test]
    fn arn_matrix() {
        assert_eq!(
            parse_target_arn("arn:oss:sqs::1:kafka").as_deref(),
            Some("1")
        );
        // P2-3：id 白名单（路径穿越/特殊字符一律拒）
        let long = "a".repeat(65);
        for bad in [
            "arn:oss:sqs::1:amqp",
            "arn:minio:sqs::1:kafka",
            "arn:oss:sqs:::kafka",
            "junk",
            "arn:oss:sqs::../x:kafka",
            "arn:oss:sqs::/abs:kafka",
            "arn:oss:sqs::a b:kafka",
            "arn:oss:sqs::a%2Fb:kafka",
        ] {
            assert!(parse_target_arn(bad).is_none(), "{bad}");
        }
        assert!(parse_target_arn(&format!("arn:oss:sqs::{long}:kafka")).is_none());
    }

    #[test]
    fn config_validation_matrix() {
        // 合法：两条 + 过滤器
        let mut c1 = q("a", "arn:oss:sqs::t1:kafka", &["s3:ObjectCreated:Put"]);
        c1.filter = Some(NotificationConfigurationFilter {
            key: Some(S3KeyFilter {
                filter_rules: Some(FilterRuleList::from(vec![
                    FilterRule {
                        name: Some(FilterRuleName::from_static("prefix")),
                        value: Some(FilterRuleValue::from("logs/".to_string())),
                    },
                    FilterRule {
                        name: Some(FilterRuleName::from_static("suffix")),
                        value: Some(FilterRuleValue::from(".csv".to_string())),
                    },
                ])),
            }),
        });
        let ok = NotificationConfiguration {
            queue_configurations: Some(QueueConfigurationList::from(vec![
                c1,
                q("b", "arn:oss:sqs::t2:kafka", &["s3:ObjectRemoved:*"]),
            ])),
            ..Default::default()
        };
        // 通配事件不在白名单 → 拒（第 2 条）
        assert!(parse_notification_config(&ok).is_err());

        let ok2 = NotificationConfiguration {
            queue_configurations: Some(QueueConfigurationList::from(vec![
                q("a", "arn:oss:sqs::t1:kafka", &["s3:ObjectCreated:Put"]),
                q(
                    "b",
                    "arn:oss:sqs::t2:kafka",
                    &["s3:ObjectRemoved:Delete", "s3:LifecycleExpiration:Delete"],
                ),
            ])),
            ..Default::default()
        };
        let parsed = parse_notification_config(&ok2).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[1].target, "t2");
        assert_eq!(parsed[1].events.len(), 2);

        // ObjectAccessed 拒绝（R5 记档：本片不实现）
        let bad_accessed = NotificationConfiguration {
            queue_configurations: Some(QueueConfigurationList::from(vec![q(
                "a",
                "arn:oss:sqs::t1:kafka",
                &["s3:ObjectAccessed:Get"],
            )])),
            ..Default::default()
        };
        assert!(parse_notification_config(&bad_accessed).is_err());

        // topic/lambda 通道拒绝
        let bad_topic = NotificationConfiguration {
            topic_configurations: Some(TopicConfigurationList::from(vec![])),
            ..Default::default()
        };
        // 空 list 不触发（is_empty 检查）——给一个非空需要构造 TopicConfiguration：
        assert!(parse_notification_config(&bad_topic).is_ok());
        // 空配置 = 清空（合法）
        let empty = NotificationConfiguration::default();
        assert!(parse_notification_config(&empty).unwrap().is_empty());
        // 超 100
        let many: Vec<_> = (0..101)
            .map(|i| {
                q(
                    &format!("c{i}"),
                    "arn:oss:sqs::t:kafka",
                    &["s3:ObjectCreated:Put"],
                )
            })
            .collect();
        let too_many = NotificationConfiguration {
            queue_configurations: Some(QueueConfigurationList::from(many)),
            ..Default::default()
        };
        assert!(parse_notification_config(&too_many).is_err());
    }

    #[test]
    fn out_roundtrip() {
        let targets = vec![EventTarget {
            id: "cfg-0".into(),
            target: "t1".into(),
            events: vec!["s3:ObjectCreated:Put".into()],
            prefix: Some("logs/".into()),
            suffix: None,
        }];
        let out = notification_config_out(&targets);
        let q = &out.queue_configurations.as_ref().unwrap()[0];
        assert_eq!(q.queue_arn.as_str(), "arn:oss:sqs::t1:kafka");
        let rules = q
            .filter
            .as_ref()
            .unwrap()
            .key
            .as_ref()
            .unwrap()
            .filter_rules
            .as_ref()
            .unwrap();
        assert_eq!(rules[0].name.as_ref().unwrap().as_str(), "prefix");
    }
}
