use super::*;

#[test]
fn missing_or_malformed_headers_mean_level_one() {
    assert_eq!(parse_wire(None), 1);
    assert_eq!(parse_wire(Some("garbage")), 1);
    assert_eq!(parse_wire(Some("0")), 1);
    assert_eq!(parse_wire(Some("")), 1);
    assert_eq!(parse_wire(Some("1")), 1);
    assert_eq!(parse_wire(Some("2")), 2);
    assert_eq!(parse_wire(Some("3")), 3);
    assert_eq!(parse_feature(None), 1);
    assert_eq!(parse_feature(Some("x")), 1);
    assert_eq!(parse_feature(Some("0")), 1);
    assert_eq!(parse_feature(Some("2")), 2);
}

#[test]
fn advertised_feature_is_willingness_capped_at_binary_max() {
    assert_eq!(advertised_feature(1), 1);
    assert_eq!(advertised_feature(2), 2);
    assert_eq!(advertised_feature(9), MAX_FEATURE_LEVEL);
}

#[test]
fn supports_is_bounded_by_binary_max_and_levels_are_fixed() {
    assert_eq!(MIN_FEATURE_LEVEL, 1);
    assert_eq!(FEATURE_STRUCTURED_LIFECYCLE, 2);
    assert_eq!(FEATURE_LEVEL_3, 3);
    assert_eq!(FEATURE_COMMAND_EXT, 3);
    assert_eq!(FEATURE_COMMAND_EXT, FEATURE_LEVEL_3);
    assert_eq!(FEATURE_LEVEL_4, 4, "M8-2 jobs 复制是新 level，不得复用 3");
    assert_eq!(
        FEATURE_LEVEL_5, 5,
        "v1.23.0 已 advertise 4 且不认识 Inventory*，必须新开 5"
    );
    assert_eq!(
        FEATURE_LEVEL_6, 6,
        "v1.24.0 及更早 advertise ≤5 且不认识 JobPurge，必须新开 6"
    );
    assert_eq!(
        FEATURE_LEVEL_7, 7,
        "v1.25.0 及更早 advertise ≤6 且不认识 TierRestore*/ScheduleLeaseAcquire，必须新开 7"
    );
    assert_eq!(
        FEATURE_LEVEL_8, 8,
        "v1.26.0 及更早 advertise ≤7 且不认识 AccountHardQuotaSet，必须新开 8"
    );
    assert_eq!(
        FEATURE_LEVEL_9, 9,
        "v1.27.0 及更早 advertise ≤8 且不认识账户用量账本命令/快照字段，必须新开 9"
    );
    assert_eq!(MAX_FEATURE_LEVEL, FEATURE_LEVEL_9);
    assert!(supports(1));
    assert!(supports(FEATURE_STRUCTURED_LIFECYCLE));
    assert!(supports(FEATURE_COMMAND_EXT));
    assert!(supports(FEATURE_LEVEL_4));
    assert!(supports(FEATURE_LEVEL_5));
    assert!(supports(FEATURE_LEVEL_6));
    assert!(supports(FEATURE_LEVEL_7));
    assert!(supports(FEATURE_LEVEL_8));
    assert!(supports(FEATURE_LEVEL_9));
    assert!(!supports(MAX_FEATURE_LEVEL + 1));
}

/// JobPurge 的 append 前闸门：混版集群里只要有一个 level-5 节点，effective 就是 5，
/// 低于命令要求的 6（`check_feature` 因此拒绝，不写日志、不激活 marker）。
#[test]
fn job_purge_gate_rejects_level_five_cluster_before_append() {
    let p = peers_all(7, &[1, 2], &[], 5);
    let e = effective_feature_level(FEATURE_LEVEL_6, 1, &[1, 2], &[], 7, &p);
    assert_eq!(e.level, 5);
    let purge = super::super::command::ControlCommand::JobPurge { job_ids: vec![1] };
    assert_eq!(purge.min_feature_level(), FEATURE_LEVEL_6);
    assert!(
        e.level < purge.min_feature_level(),
        "level-5 节点在场时 append 前拒绝 JobPurge"
    );
    assert!(e.blocked_reason.unwrap().contains("voter 2"));
}

/// level-7 的 append 前闸门：混版集群里只要有一个 level-6 节点，effective 就是 6，
/// 低于 restore 后续状态/租约命令要求的 7（`check_feature` 拒绝，不写日志）。
#[test]
fn level_seven_commands_rejected_on_level_six_cluster() {
    use super::super::command::ControlCommand;
    use crate::raft::runstate::ScheduleLeaseRow;

    let p = peers_all(7, &[1, 2], &[], FEATURE_LEVEL_6);
    let e = effective_feature_level(FEATURE_LEVEL_7, 1, &[1, 2], &[], 7, &p);
    assert_eq!(e.level, FEATURE_LEVEL_6);
    let lease = ControlCommand::ScheduleLeaseAcquire {
        lease: ScheduleLeaseRow {
            term: 1,
            node: 1,
            expires_at: 2,
            acquired_at: 1,
        },
        now: 1,
    };
    let restore = ControlCommand::TierRestoreDelete {
        bucket: "b".into(),
        key: "k".into(),
        version_id: "v".into(),
        fence_term: 1,
    };
    for cmd in [lease, restore] {
        assert_eq!(cmd.min_feature_level(), FEATURE_LEVEL_7);
        assert!(
            e.level < cmd.min_feature_level(),
            "level-6 节点在场时 append 前拒绝 {cmd:?}"
        );
    }

    // 硬配额配置是 level 8：level-7 集群同样必须在 append 前拒绝。
    let p7 = peers_all(8, &[1, 2], &[], FEATURE_LEVEL_7);
    let e7 = effective_feature_level(FEATURE_LEVEL_8, 1, &[1, 2], &[], 8, &p7);
    assert_eq!(e7.level, FEATURE_LEVEL_7);
    let quota = ControlCommand::AccountHardQuotaSet {
        id: "000000000001".into(),
        hard_byte_quota: Some(1),
        object_quota: None,
        updated_at: 1,
    };
    assert_eq!(quota.min_feature_level(), FEATURE_LEVEL_8);
    assert!(e7.level < quota.min_feature_level());
    assert!(e.blocked_reason.unwrap().contains("voter 2"));
}

fn peers_all(term: u64, voters: &[u64], learners: &[u64], feature: u8) -> PeerVersions {
    let p = PeerVersions::default();
    for id in voters.iter().chain(learners) {
        p.record(*id, term, WIRE_VERSION, feature);
    }
    p
}

#[test]
fn effective_is_min_over_voters_and_learners() {
    let p = peers_all(7, &[1, 2, 3], &[4], 2);
    assert_eq!(
        effective_feature_level(2, 1, &[1, 2, 3], &[4], 7, &p).level,
        2
    );

    // learner 掉到 1 也必须阻塞（它会收到 level-2 entry/snapshot）。
    p.record(4, 7, WIRE_VERSION, 1);
    let e = effective_feature_level(2, 1, &[1, 2, 3], &[4], 7, &p);
    assert_eq!(e.level, 1);
    assert!(e.blocked_reason.unwrap().contains("learner 4"));

    // voter 掉到 1 同理。
    let p = peers_all(7, &[1, 2, 3], &[], 2);
    p.record(3, 7, WIRE_VERSION, 1);
    let e = effective_feature_level(2, 1, &[1, 2, 3], &[], 7, &p);
    assert_eq!(e.level, 1);
    assert!(
        e.blocked_reason
            .unwrap()
            .contains("voter 3 wire 2 feature 1")
    );
}

fn ext(types: &[&str]) -> BTreeSet<String> {
    types.iter().map(|s| s.to_string()).collect()
}

#[test]
fn ext_type_header_is_parsed_conservatively() {
    assert!(parse_ext_types(None).is_empty());
    assert!(parse_ext_types(Some("")).is_empty());
    assert!(parse_ext_types(Some(" , ")).is_empty());
    assert_eq!(
        parse_ext_types(Some("a.b, c-d ,a.b,,")),
        ext(&["a.b", "c-d"]),
        "去空白且去重"
    );
    // 畸形条目绝不能变成「支持」。
    assert!(parse_ext_types(Some("ok\u{7f},bad\nname")).is_empty());
    assert!(parse_ext_types(Some(&"x".repeat(EXT_TYPE_ID_MAX + 1))).is_empty());
    assert_eq!(format_ext_types(&[]), "");
    assert_eq!(format_ext_types(&["a", "", "b"]), "a,b");
    assert_eq!(
        parse_ext_types(Some(&format_ext_types(&["a.b", "c"]))),
        ext(&["a.b", "c"])
    );
}

#[test]
fn ext_type_gate_requires_every_current_voter_and_learner() {
    let p = PeerVersions::default();
    p.record_ext(1, 7, WIRE_VERSION, 3, ext(&["test.noop"]));
    p.record_ext(2, 7, WIRE_VERSION, 8, ext(&["test.noop"]));
    p.record_ext(4, 7, WIRE_VERSION, 8, ext(&["test.noop"]));
    assert!(
        ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &p).is_empty(),
        "本机记录不参与（由注册表判定）"
    );

    // 当前 term 有握手但未宣告（旧节点默认空集）→ append 前拒绝并点名。
    p.record_ext(2, 7, WIRE_VERSION, 8, BTreeSet::new());
    let m = ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &p);
    assert_eq!(m.len(), 1, "{m:?}");
    assert!(
        m[0].contains("voter 2") && m[0].contains("no ext types"),
        "{m:?}"
    );

    // learner 同样计入，且错误里有它宣告的类型便于排查。
    p.record_ext(2, 7, WIRE_VERSION, 8, ext(&["test.noop"]));
    p.record_ext(4, 7, WIRE_VERSION, 8, ext(&["other"]));
    let m = ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &p);
    assert!(
        m[0].contains("learner 4") && m[0].contains("other"),
        "{m:?}"
    );

    // 旧 term 记录不可信；完全没握手的当前成员同样缺失。
    p.record_ext(4, 6, WIRE_VERSION, 8, ext(&["test.noop"]));
    let m = ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &p);
    assert!(m[0].contains("stale term 6"), "{m:?}");
    let m = ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &PeerVersions::default());
    assert_eq!(m.len(), 2);
    assert!(m.iter().all(|s| s.contains("not heard")), "{m:?}");
}

#[test]
fn effective_ignores_previous_term_records() {
    let p = peers_all(6, &[1, 2], &[], 2);
    let e = effective_feature_level(2, 1, &[1, 2], &[], 7, &p);
    assert_eq!(e.level, 1);
    assert!(e.blocked_reason.unwrap().contains("stale term 6"));
}

#[test]
fn unanswered_peer_contributes_one() {
    let p = PeerVersions::default();
    let e = effective_feature_level(2, 1, &[1, 2], &[], 7, &p);
    assert_eq!(e.level, 1);
    assert!(e.blocked_reason.unwrap().contains("not heard"));

    // 本地配置 1 时即便所有 peer 都是 2 也只能是 1。
    let p = peers_all(7, &[1, 2], &[], 2);
    assert_eq!(effective_feature_level(1, 1, &[1, 2], &[], 7, &p).level, 1);
}

#[test]
fn startup_guard_rejects_marker_above_config_or_binary() {
    assert!(guard_startup(1, 1).is_ok());
    assert!(guard_startup(2, 2).is_ok());
    // 已激活 2 的集群不能用 feature_level=1 启动。
    assert!(guard_startup(2, 1).is_err());
    // 二进制最高 2：marker 3 无从谈起，但必须拒绝而不是截断。
    assert!(guard_startup(3, 2).is_err());
    // feature 4 激活后：本二进制（max 4）显式配置 4 才放行。
    assert!(guard_startup(4, 4).is_ok());
    assert!(guard_startup(4, 3).is_err(), "配置低于 marker 必须拒绝");
    assert!(guard_startup(5, 5).is_ok());
    assert!(guard_startup(5, 4).is_err(), "配置低于 marker 必须拒绝");
    assert!(guard_startup(6, 6).is_ok());
    assert!(guard_startup(6, 5).is_err(), "配置低于 marker 必须拒绝");
    assert!(guard_startup(7, 7).is_ok());
    assert!(guard_startup(7, 6).is_err(), "配置低于 marker 必须拒绝");
    assert!(guard_startup(8, 7).is_err(), "未知更高 marker 必须拒绝");
    // v1.22（max 3）遇到 marker 4、v1.23.0（max 4）遇到 marker 5、
    // v1.24.0（max 5）遇到 marker 6、v1.25.0（max 6）遇到 marker 7 必须拒绝。
    assert!(guard_startup_with_max(4, 4, 3).is_err());
    assert!(guard_startup_with_max(3, 3, 3).is_ok());
    assert!(guard_startup_with_max(5, 5, 4).is_err());
    assert!(guard_startup_with_max(4, 4, 4).is_ok());
    assert!(guard_startup_with_max(6, 6, 5).is_err());
    assert!(guard_startup_with_max(5, 5, 5).is_ok());
    assert!(guard_startup_with_max(7, 7, 6).is_err());
    assert!(guard_startup_with_max(6, 6, 6).is_ok());
}
#[test]
fn mixed_only_counts_current_term_legacy_wire() {
    let p = PeerVersions::default();
    p.record(2, 7, 1, 1);
    assert!(p.mixed(7));
    assert!(!p.mixed(8), "stale-term legacy record must not alarm");
    p.record(2, 8, WIRE_VERSION, 2);
    assert!(!p.mixed(8));
}
