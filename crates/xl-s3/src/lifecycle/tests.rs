use super::*;
use s3s::dto::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
    LifecycleRuleFilter, Transition, TransitionStorageClass,
};

/// LifecycleRule 无 Default impl（s3s 生成代码）；给测试一个全空骨架。
fn rule() -> LifecycleRule {
    LifecycleRule {
        abort_incomplete_multipart_upload: None,
        expiration: None,
        filter: None,
        id: None,
        noncurrent_version_expiration: None,
        noncurrent_version_transitions: None,
        prefix: None,
        status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
        transitions: None,
    }
}

fn spec(prefix: &str) -> LifecycleRuleSpec {
    LifecycleRuleSpec {
        rule_id: None,
        filter: LifecycleFilter {
            prefix: prefix.into(),
            ..Default::default()
        },
        expiration: Some(ExpirationAction::Days { days: 1 }),
        transitions: vec![],
        noncurrent_expiration: None,
        noncurrent_transitions: vec![],
        enabled: true,
    }
}

fn tag(k: &str, v: &str) -> s3s::dto::Tag {
    s3s::dto::Tag {
        key: Some(k.into()),
        value: Some(v.into()),
    }
}

fn future_midnight(days: i64) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    (now / 86_400 + days) * 86_400
}

#[test]
fn validation_defaults_and_archive_tier_resolution() {
    let mut rules = vec![spec("a"), spec("b")];
    validate_rules(&mut rules, &[]).unwrap();
    assert_eq!(rules[0].rule_id.as_deref(), Some("rule-1"));
    assert_eq!(rules[1].rule_id.as_deref(), Some("rule-2"));

    let mut r = vec![LifecycleRuleSpec {
        transitions: vec![TransitionAction {
            days: Some(3),
            date: None,
            class: "GLACIER".into(),
            tier: None,
        }],
        ..spec("")
    }];
    assert!(matches!(
        validate_rules(&mut r, &[]),
        Err(LifecycleError::Invalid(_))
    ));
    let tiers = vec!["t1".to_string()];
    validate_rules(&mut r, &tiers).unwrap();
    assert_eq!(r[0].transitions[0].tier.as_deref(), Some("t1"));
    r[0].transitions[0].tier = None;
    let tiers2 = vec!["t1".to_string(), "t2".to_string()];
    assert!(matches!(
        validate_rules(&mut r, &tiers2),
        Err(LifecycleError::Invalid(_))
    ));
    let mut r = vec![LifecycleRuleSpec {
        transitions: vec![TransitionAction {
            days: Some(1),
            date: None,
            class: "STANDARD_IA".into(),
            tier: Some("t1".into()),
        }],
        ..spec("")
    }];
    validate_rules(&mut r, &tiers).unwrap();
    assert_eq!(r[0].transitions[0].tier, None);
    let mut r = vec![LifecycleRuleSpec {
        transitions: vec![TransitionAction {
            days: Some(1),
            date: None,
            class: "STANDARD".into(),
            tier: None,
        }],
        ..spec("")
    }];
    assert!(matches!(
        validate_rules(&mut r, &[]),
        Err(LifecycleError::InvalidClass(_))
    ));
}

#[test]
fn filter_shapes_roundtrip() {
    for filter in [
        LifecycleFilter {
            prefix: "logs/".into(),
            ..Default::default()
        },
        LifecycleFilter {
            tags: vec![ObjectTag {
                key: "env".into(),
                value: "prod".into(),
            }],
            ..Default::default()
        },
        LifecycleFilter {
            size_gt: Some(10),
            size_lt: Some(100),
            ..Default::default()
        },
        LifecycleFilter {
            prefix: "logs/".into(),
            tags: vec![ObjectTag {
                key: "env".into(),
                value: "prod".into(),
            }],
            size_gt: Some(10),
            ..Default::default()
        },
    ] {
        let dto = rules_to_dto(&[LifecycleRuleSpec {
            filter: filter.clone(),
            ..spec("")
        }]);
        let f = dto[0].filter.as_ref().unwrap();
        // 回读形态必须是四种互斥形态之一
        let shapes = [
            f.prefix.is_some() as u8,
            f.tag.is_some() as u8,
            (f.object_size_greater_than.is_some() || f.object_size_less_than.is_some()) as u8,
            f.and.is_some() as u8,
        ];
        assert_eq!(shapes.iter().filter(|x| **x == 1).count(), 1, "{f:?}");
        let back = parse_lifecycle_config(&BucketLifecycleConfiguration { rules: dto }).unwrap();
        assert_eq!(back[0].filter, filter);
    }
}

#[test]
fn and_filter_parses_and_rejects_legacy_conflict() {
    let cfg = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            filter: Some(LifecycleRuleFilter {
                and: Some(s3s::dto::LifecycleRuleAndOperator {
                    prefix: Some("p/".into()),
                    tags: Some(vec![tag("env", "prod"), tag("team", "x")]),
                    object_size_greater_than: Some(100),
                    object_size_less_than: Some(1000),
                }),
                ..Default::default()
            }),
            expiration: Some(LifecycleExpiration {
                days: Some(2),
                ..Default::default()
            }),
            ..rule()
        }],
    };
    let specs = parse_lifecycle_config(&cfg).unwrap();
    assert_eq!(specs[0].filter.prefix, "p/");
    assert_eq!(specs[0].filter.tags.len(), 2);
    assert_eq!(specs[0].filter.size_gt, Some(100));
    assert_eq!(specs[0].filter.size_lt, Some(1000));

    let mut bad = cfg.clone();
    bad.rules[0].prefix = Some("other/".into());
    assert!(matches!(
        parse_lifecycle_config(&bad),
        Err(LifecycleError::Invalid(_))
    ));
}

#[test]
fn expiration_date_and_delete_marker_rules() {
    let date = future_midnight(2);
    let cfg = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            expiration: Some(LifecycleExpiration {
                date: Some(s3s::dto::Date::from(
                    time::OffsetDateTime::from_unix_timestamp(date).unwrap(),
                )),
                ..Default::default()
            }),
            ..rule()
        }],
    };
    let mut specs = parse_lifecycle_config(&cfg).unwrap();
    validate_rules(&mut specs, &[]).unwrap();
    assert_eq!(specs[0].expiration, Some(ExpirationAction::Date { date }));

    // 过去时间 → 400；非午夜 → 400（在 parse 后由 validate_spec 拦）
    let mut past = specs.clone();
    past[0].expiration = Some(ExpirationAction::Date {
        date: date - 30 * 86_400,
    });
    assert!(matches!(
        validate_rules(&mut past, &[]),
        Err(LifecycleError::InvalidArgument(_))
    ));
    let mut odd = specs.clone();
    odd[0].expiration = Some(ExpirationAction::Date { date: date + 1 });
    assert!(matches!(
        validate_rules(&mut odd, &[]),
        Err(LifecycleError::InvalidArgument(_))
    ));

    let eodm = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            filter: Some(LifecycleRuleFilter {
                prefix: Some("p/".into()),
                ..Default::default()
            }),
            expiration: Some(LifecycleExpiration {
                expired_object_delete_marker: Some(true),
                ..Default::default()
            }),
            ..rule()
        }],
    };
    let mut specs = parse_lifecycle_config(&eodm).unwrap();
    validate_rules(&mut specs, &[]).unwrap();
    assert_eq!(
        specs[0].expiration,
        Some(ExpirationAction::ExpiredObjectDeleteMarker)
    );
    // EODM + Tag → 400
    let mut with_tag = specs.clone();
    with_tag[0].filter.tags.push(ObjectTag {
        key: "k".into(),
        value: "v".into(),
    });
    assert!(matches!(
        validate_rules(&mut with_tag, &[]),
        Err(LifecycleError::Invalid(_))
    ));
}

#[test]
fn multiple_transitions_order_and_classes() {
    let tr = |days: u32, class: &str| TransitionAction {
        days: Some(days),
        date: None,
        class: class.into(),
        tier: None,
    };
    let mut specs = vec![LifecycleRuleSpec {
        transitions: vec![tr(30, "STANDARD_IA"), tr(90, "GLACIER")],
        ..spec("")
    }];
    validate_rules(&mut specs, &["cold".into()]).unwrap();
    assert_eq!(specs[0].transitions[1].tier.as_deref(), Some("cold"));

    let mut unsorted = specs.clone();
    unsorted[0].transitions.reverse();
    assert!(matches!(
        validate_rules(&mut unsorted, &["cold".into()]),
        Err(LifecycleError::Invalid(_))
    ));
    let mut dup = specs.clone();
    dup[0].transitions[1].class = "STANDARD_IA".into();
    assert!(matches!(
        validate_rules(&mut dup, &["cold".into()]),
        Err(LifecycleError::Invalid(_))
    ));
    let mut dated = specs.clone();
    dated[0].transitions[1].date = Some(future_midnight(30));
    dated[0].transitions[1].days = None;
    assert!(
        matches!(
            validate_rules(&mut dated, &["cold".into()]),
            Err(LifecycleError::Invalid(_))
        ),
        "date 阈值早于前一条 days 阈值必须拒绝"
    );
}

#[test]
fn noncurrent_actions_parse_and_validate() {
    let cfg = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            filter: Some(LifecycleRuleFilter {
                prefix: Some("p/".into()),
                ..Default::default()
            }),
            noncurrent_version_expiration: Some(s3s::dto::NoncurrentVersionExpiration {
                noncurrent_days: Some(30),
                newer_noncurrent_versions: Some(5),
            }),
            noncurrent_version_transitions: Some(vec![s3s::dto::NoncurrentVersionTransition {
                noncurrent_days: Some(60),
                newer_noncurrent_versions: None,
                storage_class: Some(s3s::dto::TransitionStorageClass::from_static("GLACIER")),
            }]),
            ..rule()
        }],
    };
    let mut specs = parse_lifecycle_config(&cfg).unwrap();
    validate_rules(&mut specs, &["cold".into()]).unwrap();
    let n = specs[0].noncurrent_expiration.as_ref().unwrap();
    assert_eq!((n.days, n.newer), (30, Some(5)));
    assert_eq!(
        specs[0].noncurrent_transitions[0].tier.as_deref(),
        Some("cold")
    );

    // NewerNoncurrentVersions 需要非空 Filter
    let mut no_filter = specs.clone();
    no_filter[0].filter = LifecycleFilter::default();
    assert!(matches!(
        validate_rules(&mut no_filter, &["cold".into()]),
        Err(LifecycleError::Invalid(_))
    ));
    // 天数越界
    let mut zero = specs.clone();
    zero[0].noncurrent_expiration.as_mut().unwrap().days = 0;
    assert!(matches!(
        validate_rules(&mut zero, &["cold".into()]),
        Err(LifecycleError::InvalidArgument(_))
    ));
}

#[test]
fn unsupported_abort_mpu_still_501() {
    let cfg = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            abort_incomplete_multipart_upload: Some(Default::default()),
            ..rule()
        }],
    };
    assert!(matches!(
        parse_lifecycle_config(&cfg),
        Err(LifecycleError::NotImplemented(_))
    ));
}

#[test]
fn dto_roundtrip_keeps_supported_shape() {
    let cfg = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            id: Some("r1".into()),
            filter: Some(LifecycleRuleFilter {
                prefix: Some("p/".into()),
                ..Default::default()
            }),
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            expiration: Some(LifecycleExpiration {
                days: Some(2),
                ..Default::default()
            }),
            transitions: Some(vec![Transition {
                days: Some(5),
                storage_class: Some(TransitionStorageClass::from_static("GLACIER_IR")),
                ..Default::default()
            }]),
            ..rule()
        }],
    };
    let mut specs = parse_lifecycle_config(&cfg).unwrap();
    validate_rules(&mut specs, &["cold".into()]).unwrap();
    let back = rules_to_dto(&specs);
    assert_eq!(back.len(), 1);
    assert_eq!(
        back[0].filter.as_ref().unwrap().prefix.as_deref(),
        Some("p/")
    );
    assert_eq!(back[0].prefix, None);
    assert_eq!(back[0].expiration.as_ref().unwrap().days, Some(2));
    assert_eq!(
        back[0].transitions.as_ref().unwrap()[0]
            .storage_class
            .as_ref()
            .unwrap()
            .as_str(),
        "GLACIER_IR"
    );
    assert_eq!(specs[0].transitions[0].tier.as_deref(), Some("cold"));
}

#[test]
fn generated_ids_avoid_explicit_ids() {
    let mut rules = vec![
        spec("a"),
        LifecycleRuleSpec {
            rule_id: Some("rule-1".into()),
            ..spec("b")
        },
        spec("c"),
    ];
    validate_rules(&mut rules, &[]).unwrap();
    assert_eq!(rules[0].rule_id.as_deref(), Some("rule-2"));
    assert_eq!(rules[1].rule_id.as_deref(), Some("rule-1"));
    assert_eq!(rules[2].rule_id.as_deref(), Some("rule-3"));
}

#[test]
fn duplicate_prefix_allowed_but_prefix_conflict_rejected() {
    let cfg = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            prefix: Some("a".into()),
            filter: Some(LifecycleRuleFilter {
                prefix: Some("b".into()),
                ..Default::default()
            }),
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            expiration: Some(LifecycleExpiration {
                days: Some(1),
                ..Default::default()
            }),
            ..rule()
        }],
    };
    assert!(matches!(
        parse_lifecycle_config(&cfg),
        Err(LifecycleError::Invalid(_))
    ));
    // 同 prefix 两条规则（不同 ID）：AWS 允许，校验只查 ID 唯一
    let mut dups = vec![spec("a"), {
        let mut s = spec("a");
        s.rule_id = Some("x".into());
        s
    }];
    validate_rules(&mut dups, &[]).unwrap();
}

#[test]
fn config_json_roundtrip_and_legacy_fallback() {
    let structured = LifecycleRuleSpec {
        filter: LifecycleFilter {
            prefix: "p/".into(),
            tags: vec![ObjectTag {
                key: "env".into(),
                value: "prod".into(),
            }],
            size_gt: Some(1),
            size_lt: Some(2),
        },
        expiration: Some(ExpirationAction::Date {
            date: future_midnight(1),
        }),
        transitions: vec![TransitionAction {
            days: Some(3),
            date: None,
            class: "GLACIER".into(),
            tier: Some("cold".into()),
        }],
        noncurrent_expiration: Some(NoncurrentExpiration {
            days: 7,
            newer: Some(2),
        }),
        noncurrent_transitions: vec![],
        ..spec("")
    };
    let json = structured
        .config_json()
        .unwrap()
        .expect("结构化规则应写 blob");
    let back = from_config_json(&json, Some("r1".into()), true).unwrap();
    assert_eq!(back.filter, structured.filter);
    assert_eq!(back.expiration, structured.expiration);
    assert_eq!(back.noncurrent_expiration, structured.noncurrent_expiration);
    assert_eq!(back.transitions[0].tier.as_deref(), Some("cold"));

    let legacy = spec("logs/");
    assert!(legacy.config_json().unwrap().is_none());
    let back = LifecycleRuleSpec::from_legacy_row(
        Some("rule-1".into()),
        "logs/".into(),
        Some(30),
        None,
        None,
        None,
        true,
    )
    .unwrap();
    let mut expected = legacy_with_days("logs/", 30);
    expected.rule_id = Some("rule-1".into());
    assert_eq!(back, expected);
    // 坏 JSON fail-closed
    assert!(from_config_json("{not json", None, true).is_err());
    assert!(from_config_json(r#"{"filter":{"prefix":"p/"}}"#, None, true).is_err());
}

fn legacy_with_days(prefix: &str, days: u32) -> LifecycleRuleSpec {
    LifecycleRuleSpec {
        expiration: Some(ExpirationAction::Days { days }),
        ..spec(prefix)
    }
}
