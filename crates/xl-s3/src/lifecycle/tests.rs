use super::*;

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
        prefix: prefix.into(),
        expiration_days: Some(1),
        transition_days: None,
        transition_class: None,
        transition_tier: None,
        enabled: true,
    }
}

#[test]
fn validation_defaults_and_archive_tier_resolution() {
    let mut rules = vec![spec("a"), spec("b")];
    validate_rules(&mut rules, &[]).unwrap();
    assert_eq!(rules[0].rule_id.as_deref(), Some("rule-1"));
    assert_eq!(rules[1].rule_id.as_deref(), Some("rule-2"));

    // 归档类：0 个 tier → 400；恰一个 → 自动解析；多个 → 400
    let mut r = vec![LifecycleRuleSpec {
        transition_days: Some(3),
        transition_class: Some("GLACIER".into()),
        ..spec("")
    }];
    assert!(matches!(
        validate_rules(&mut r, &[]),
        Err(LifecycleError::Invalid(_))
    ));
    let tiers = vec!["t1".to_string()];
    validate_rules(&mut r, &tiers).unwrap();
    assert_eq!(r[0].transition_tier.as_deref(), Some("t1"));
    // 上一步已就地解析出 t1；清掉后验证「多个 tier 且未显式指定」→ 400
    r[0].transition_tier = None;
    let tiers2 = vec!["t1".to_string(), "t2".to_string()];
    assert!(matches!(
        validate_rules(&mut r, &tiers2),
        Err(LifecycleError::Invalid(_))
    ));
    // 非归档类：显式 tier 被忽略
    let mut r = vec![LifecycleRuleSpec {
        transition_days: Some(1),
        transition_class: Some("STANDARD_IA".into()),
        transition_tier: Some("t1".into()),
        ..spec("")
    }];
    validate_rules(&mut r, &tiers).unwrap();
    assert_eq!(r[0].transition_tier, None);
    // 非法 class
    let mut r = vec![LifecycleRuleSpec {
        transition_days: Some(1),
        transition_class: Some("STANDARD".into()),
        ..spec("")
    }];
    assert!(matches!(
        validate_rules(&mut r, &[]),
        Err(LifecycleError::InvalidClass(_))
    ));
}

#[test]
fn parse_rejects_unsupported_before_missing_action() {
    let bad = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            expiration: Some(LifecycleExpiration {
                date: Some(s3s::dto::Date::from(time::OffsetDateTime::UNIX_EPOCH)),
                ..Default::default()
            }),
            ..rule()
        }],
    };
    assert!(matches!(
        parse_lifecycle_config(&bad),
        Err(LifecycleError::NotImplemented(_))
    ));
    // 无任何动作 → 400（由 validate_rules 兜底）
    let empty = BucketLifecycleConfiguration {
        rules: vec![LifecycleRule {
            status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
            ..rule()
        }],
    };
    let mut specs = parse_lifecycle_config(&empty).unwrap();
    assert!(matches!(
        validate_rules(&mut specs, &[]),
        Err(LifecycleError::Invalid(_))
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
    assert_eq!(specs[0].transition_tier.as_deref(), Some("cold"));
}

#[test]
fn prefix_conflict_and_duplicates_rejected() {
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
    let mut dups = vec![spec("a"), {
        let mut s = spec("a");
        s.rule_id = Some("x".into());
        s
    }];
    assert!(matches!(
        validate_rules(&mut dups, &[]),
        Err(LifecycleError::Invalid(_))
    ));
}
