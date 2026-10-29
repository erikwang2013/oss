//! Level C M0：冻结 `ControlCommand` 的 serde 字段集（变体名 + 字段名即跨版本协议）。
//!
//! fixture `tests/fixtures/compat/control_commands.json` 形如
//! `{"VariantName": ["field1", ...]}`（字段名按 serde_json 的 BTreeMap 序）。patch 分支
//! 与后续升级必须复用同一份 fixture：任何变体/字段的增删改名都会让本测试变红——这正是
//! 旧节点 `deny_unknown_fields` fail-stop 的边界。
//!
//! `#[cfg(test)]` 的 `Set`/`Delete`/`FeatureProbe` 是二进制内部探针，不进生产命令集，
//! 因此也不进契约（集成测试构建的 lib 关闭 cfg(test)，看不到它们）。

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use oss_server::raft::runstate::{JobRow, JobTaskRow, TierRestoreRow};
use oss_server::raft::{ControlCommand, LifecycleRuleCmd, LifecycleRuleCmdV2, PermissionBits};

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compat/control_commands.json")
}

fn rule_v1() -> LifecycleRuleCmd {
    LifecycleRuleCmd {
        rule_id: Some("r1".into()),
        prefix: "logs/".into(),
        expire_days: 30,
        expiration_days: Some(30),
        transition_days: Some(7),
        transition_class: Some("GLACIER".into()),
        transition_tier: Some("cold".into()),
        enabled: true,
    }
}

fn rule_v2() -> LifecycleRuleCmdV2 {
    LifecycleRuleCmdV2 {
        rule_id: Some("r1".into()),
        prefix: "logs/".into(),
        expire_days: 30,
        expiration_days: Some(30),
        transition_days: Some(7),
        transition_class: Some("GLACIER".into()),
        transition_tier: Some("cold".into()),
        enabled: true,
        config_json: Some(r#"{"filter":{"prefix":"logs/"}}"#.into()),
    }
}

/// 生产命令集的每个变体各一条（字段值无关契约，只用可 roundtrip 的最小值）。
fn all_variants() -> Vec<ControlCommand> {
    vec![
        ControlCommand::UserCreate {
            username: "alice".into(),
            password_hash: "h".into(),
            status: "active".into(),
            created_at: 1,
        },
        ControlCommand::UserDelete {
            username: "alice".into(),
        },
        ControlCommand::UserSetPassword {
            username: "alice".into(),
            password_hash: "h2".into(),
        },
        ControlCommand::UserSetStatus {
            username: "alice".into(),
            status: "disabled".into(),
        },
        ControlCommand::UserSetPermissions {
            username: "alice".into(),
            permissions: vec![PermissionBits {
                bucket: "b1".into(),
                read: true,
                write: false,
                delete: false,
            }],
        },
        ControlCommand::AccessKeyCreate {
            access_key: "AK".into(),
            secret_key: "SK".into(),
            username: "alice".into(),
            kind: "long-term".into(),
            parent_ak: None,
            session_policy: None,
            created_at: 1,
        },
        ControlCommand::AccessKeyDelete {
            access_key: "AK".into(),
        },
        ControlCommand::AccessKeySetStatus {
            access_key: "AK".into(),
            status: "Inactive".into(),
        },
        ControlCommand::BucketCreate {
            name: "b1".into(),
            owner: Some("alice".into()),
            created_at: 1,
            replication_mode: "async".into(),
            versioning: "Off".into(),
            object_lock: false,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            default_encryption: None,
        },
        ControlCommand::BucketDelete { name: "b1".into() },
        ControlCommand::BucketSetDeleting {
            name: "b1".into(),
            deleting: true,
        },
        ControlCommand::BucketSetVersioning {
            name: "b1".into(),
            versioning: "Enabled".into(),
        },
        ControlCommand::BucketSetObjectLock {
            name: "b1".into(),
            enabled: true,
            default_mode: Some("GOVERNANCE".into()),
            default_days: Some(7),
        },
        ControlCommand::BucketSetNotification {
            bucket: "b1".into(),
            config_json: "{}".into(),
            updated_at: 1,
        },
        ControlCommand::BucketClearNotification {
            bucket: "b1".into(),
        },
        ControlCommand::BucketSetLifecycle {
            bucket: "b1".into(),
            rules: vec![rule_v1()],
            updated_at: 1,
        },
        ControlCommand::BucketSetLifecycleV2 {
            bucket: "b1".into(),
            rules: vec![rule_v2()],
            updated_at: 1,
        },
        ControlCommand::BucketClearLifecycle {
            bucket: "b1".into(),
        },
        ControlCommand::BucketSetReplication {
            bucket: "b1".into(),
            rules_json: "[]".into(),
            updated_at: 1,
        },
        ControlCommand::BucketClearReplication {
            bucket: "b1".into(),
        },
        ControlCommand::BucketSetDefaultEncryption {
            bucket: "b1".into(),
            algorithm: "AES256".into(),
        },
        ControlCommand::BucketClearDefaultEncryption {
            bucket: "b1".into(),
        },
        ControlCommand::BucketSetPolicy {
            bucket: "b1".into(),
            policy_json: "{}".into(),
            updated_at: 1,
        },
        ControlCommand::BucketDeletePolicy {
            bucket: "b1".into(),
        },
        ControlCommand::AccessPointPut {
            name: "ap1".into(),
            bucket: "b1".into(),
            owner: Some("alice".into()),
            policy_json: None,
            created_at: 1,
            updated_at: 1,
        },
        ControlCommand::AccessPointDelete { name: "ap1".into() },
        ControlCommand::AccessPointSetPolicy {
            name: "ap1".into(),
            policy_json: Some("{}".into()),
            updated_at: 1,
        },
        ControlCommand::OlapPut {
            access_point: "ap1".into(),
            kind: "http".into(),
            endpoint: Some("https://example.com/t".into()),
            module: Some("aGVsbG8=".into()),
            module_sha256: Some("abc".into()),
            timeout_ms: 5_000,
            max_response_bytes: 1_024,
            updated_at: 1,
        },
        ControlCommand::OlapDelete {
            access_point: "ap1".into(),
        },
        ControlCommand::InventoryPut {
            bucket: "b1".into(),
            id: "inv1".into(),
            owner: None,
            config_json: "{}".into(),
            updated_at: 1,
        },
        ControlCommand::InventoryDelete {
            bucket: "b1".into(),
            id: "inv1".into(),
        },
        // Level 3 账户/成员命令：自然键 + 预计算值。
        ControlCommand::AccountCreate {
            id: "000000000001".into(),
            name: "acme".into(),
            bucket_quota: Some(1),
            created_at: 1,
        },
        ControlCommand::AccountUpdate {
            id: "000000000001".into(),
            name: Some("acme2".into()),
            status: Some("disabled".into()),
            updated_at: 1,
        },
        ControlCommand::AccountSetQuota {
            id: "000000000001".into(),
            bucket_quota: None,
            updated_at: 1,
        },
        ControlCommand::AccountSetSoftQuota {
            id: "000000000001".into(),
            soft_byte_quota: Some(1024),
            updated_at: 1,
        },
        ControlCommand::AccountDelete {
            id: "000000000001".into(),
        },
        ControlCommand::AccountMemberAttach {
            account_id: "000000000001".into(),
            username: "alice".into(),
            role: "member".into(),
            created_at: 1,
        },
        ControlCommand::AccountMemberSetStatus {
            account_id: "000000000001".into(),
            username: "alice".into(),
            active: false,
        },
        ControlCommand::AccountUserCreate {
            account_id: "000000000001".into(),
            username: "bob".into(),
            password_hash: "h".into(),
            role: "member".into(),
            created_at: 1,
        },
        // Level C M1：命令演进容器（level 3）；type_id 由提交方定义，payload 任意 JSON。
        ControlCommand::Ext {
            type_id: "future.demo".into(),
            payload: serde_json::json!({"k": "v"}),
        },
        // M8-2：作业运行态完整行（level 3，apply 按主键 upsert）。
        ControlCommand::JobUpsert {
            job: JobRow {
                id: 1,
                kind: "batch".into(),
                bucket: "b1".into(),
                prefix: "p/".into(),
                state: "queued".into(),
                ..JobRow::default()
            },
        },
        ControlCommand::JobTaskUpsert {
            task: JobTaskRow {
                job_id: 1,
                task_no: 1,
                action: "DeleteObject".into(),
                payload_json: "{}".into(),
                state: "queued".into(),
                ..JobTaskRow::default()
            },
        },
        ControlCommand::JobBatch {
            jobs: vec![JobRow {
                id: 2,
                kind: "transition".into(),
                ..JobRow::default()
            }],
            tasks: vec![JobTaskRow {
                job_id: 2,
                task_no: 1,
                ..JobTaskRow::default()
            }],
            // P2-1：Restore 受理行与作业同批复制。
            restores: vec![TierRestoreRow {
                bucket: "b1".into(),
                key: "p/obj".into(),
                version_id: "v1".into(),
                job_id: Some(2),
                state: "pending".into(),
                ..TierRestoreRow::default()
            }],
        },
    ]
}

/// 从样本提取 `{变体名: [字段名...]}`（外部标记序 = serde 默认形态）。
fn observed_fields() -> BTreeMap<String, Vec<String>> {
    let mut map = BTreeMap::new();
    for cmd in all_variants() {
        let value = serde_json::to_value(&cmd).expect("command must serialize");
        let obj = value.as_object().expect("externally tagged enum");
        assert_eq!(obj.len(), 1, "每条命令恰一个变体键：{obj:?}");
        let (name, body) = obj.iter().next().unwrap();
        let fields: Vec<String> = body
            .as_object()
            .expect("struct variant")
            .keys()
            .cloned()
            .collect();
        let previous = map.insert(name.clone(), fields);
        assert!(previous.is_none(), "样本重复覆盖变体 {name}");
    }
    map
}

/// 契约测试：字段集与 fixture 完全一致，且每条命令 serde roundtrip 相等。
#[test]
fn control_command_field_set_matches_frozen_fixture() {
    let text = fs::read_to_string(fixture_path())
        .expect("fixture missing——先运行 --ignored regenerate_fixture 生成并提交");
    let frozen: BTreeMap<String, Vec<String>> =
        serde_json::from_str(&text).expect("fixture must be {variant: [fields]} JSON");

    let observed = observed_fields();
    for (name, fields) in &observed {
        let want = frozen
            .get(name)
            .unwrap_or_else(|| panic!("fixture 缺少变体 {name}——字段即协议，需显式评审"));
        assert_eq!(want, fields, "变体 {name} 的字段集漂移");
    }
    let extra: Vec<&String> = frozen
        .keys()
        .filter(|k| !observed.contains_key(*k))
        .collect();
    assert!(extra.is_empty(), "fixture 含样本未覆盖的变体：{extra:?}");

    // roundtrip：线上形态可解码回同一命令（字段值不参与契约比较以外的东西）。
    for cmd in all_variants() {
        let value = serde_json::to_value(&cmd).unwrap();
        let back: ControlCommand = serde_json::from_value(value).unwrap();
        assert_eq!(back, cmd);
    }
}

/// Level C M1：level-3 词汇表固定；生产命令集中只有 `Ext`、账户/成员与 M8-2 作业
/// 行命令要求 level 3，其余命令仍为 1/2（旧二进制必须能解析并应用）。
#[test]
fn feature_level_three_is_open_only_for_ext_accounts_and_jobs() {
    use oss_server::raft::compat::{
        FEATURE_COMMAND_EXT, FEATURE_LEVEL_3, FEATURE_OSSCTL2, FEATURE_STRUCTURED_LIFECYCLE,
        MAX_FEATURE_LEVEL, MIN_FEATURE_LEVEL, guard_startup, supports,
    };
    assert_eq!(MIN_FEATURE_LEVEL, 1);
    assert_eq!(FEATURE_STRUCTURED_LIFECYCLE, 2);
    assert_eq!(FEATURE_LEVEL_3, 3);
    assert_eq!(FEATURE_COMMAND_EXT, FEATURE_LEVEL_3);
    assert_eq!(
        FEATURE_OSSCTL2, FEATURE_LEVEL_3,
        "m8 别名与 level 3 同一个激活门"
    );
    assert_eq!(MAX_FEATURE_LEVEL, FEATURE_LEVEL_3);
    assert!(supports(1) && supports(2) && supports(3));
    assert!(!supports(4), "未知更高 level 一律不支持");

    // marker 4 超出本二进制上限：拒绝启动；已激活 3 不能配 2 启动。
    assert!(guard_startup(3, 3).is_ok());
    assert!(guard_startup(4, 3).is_err(), "未知更高 marker 必须拒绝");
    assert!(
        guard_startup(3, 2).is_err(),
        "配置低于已激活 marker 必须拒绝"
    );

    let (mut ext, mut accounts, mut jobs) = (0, 0, 0);
    for cmd in all_variants() {
        let level = cmd.min_feature_level();
        match cmd {
            ControlCommand::Ext { .. } => {
                ext += 1;
                assert_eq!(level, FEATURE_COMMAND_EXT);
            }
            ControlCommand::AccountCreate { .. }
            | ControlCommand::AccountUpdate { .. }
            | ControlCommand::AccountSetQuota { .. }
            | ControlCommand::AccountSetSoftQuota { .. }
            | ControlCommand::AccountDelete { .. }
            | ControlCommand::AccountMemberAttach { .. }
            | ControlCommand::AccountMemberSetStatus { .. }
            | ControlCommand::AccountUserCreate { .. } => {
                accounts += 1;
                assert_eq!(level, FEATURE_LEVEL_3);
            }
            ControlCommand::JobUpsert { .. }
            | ControlCommand::JobTaskUpsert { .. }
            | ControlCommand::JobBatch { .. } => {
                jobs += 1;
                assert_eq!(level, FEATURE_LEVEL_3);
            }
            _ => assert!(
                level <= FEATURE_STRUCTURED_LIFECYCLE,
                "{cmd:?} 要求 level {level}，但只有 Ext/账户/作业行命令可以是 level 3"
            ),
        }
    }
    assert_eq!(ext, 1, "生产命令集恰好一个 Ext 样本");
    assert_eq!(accounts, 8, "生产命令集恰好八个账户命令样本");
    assert_eq!(jobs, 3, "生产命令集恰好三个 M8-2 作业行样本");
}

/// `Ext` 是显式可选（externally tagged）变体：字段集冻结为 `type_id`+`payload`，
/// 变体级 `deny_unknown_fields`；旧命令集/未来变体都解码失败（fail-stop，不静默跳过）。
#[test]
fn ext_is_typed_and_unreadable_by_old_decoders() {
    let ext = ControlCommand::Ext {
        type_id: "future.module.op".into(),
        payload: serde_json::json!({"k": "v"}),
    };
    let wire = serde_json::to_value(&ext).unwrap();
    assert_eq!(
        wire,
        serde_json::json!({"Ext": {"type_id": "future.module.op", "payload": {"k": "v"}}})
    );
    assert_eq!(
        serde_json::from_value::<ControlCommand>(wire.clone()).unwrap(),
        ext
    );

    // 变体级未知字段/缺失字段：新语义只能新 type_id 或新变体，不能偷偷塞字段。
    assert!(
        serde_json::from_value::<ControlCommand>(
            serde_json::json!({"Ext": {"type_id": "x", "payload": {}, "extra": 1}})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<ControlCommand>(serde_json::json!({"Ext": {"type_id": "x"}}))
            .is_err()
    );

    // 未来变体：新二进制同样 fail-closed（绝不能把未知命令当空操作）。
    assert!(
        serde_json::from_value::<ControlCommand>(serde_json::json!({"FutureVariant": {}})).is_err()
    );

    // v1.13.1/v1.13.2 的命令集没有 Ext（deny_unknown_fields）→ 解码即失败，
    // apply 侧 fail-stop，绝不会把未知命令当空操作。
    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    enum LegacyCommand {
        BucketDelete { name: String },
    }
    match serde_json::from_value::<LegacyCommand>(
        serde_json::json!({"BucketDelete": {"name": "b"}}),
    ) {
        Ok(LegacyCommand::BucketDelete { name }) => assert_eq!(name, "b"),
        Err(e) => panic!("旧命令集必须仍能解码既有变体: {e}"),
    }
    let legacy = serde_json::from_value::<LegacyCommand>(wire);
    assert!(
        legacy.is_err(),
        "旧二进制必须拒绝 level-3 Ext，得到 {legacy:?}"
    );
}

/// 重新生成 fixture（人工核对后提交）。只允许在有意变更 wire 契约时运行。
#[test]
#[ignore = "regenerates the committed wire-contract fixture"]
fn regenerate_fixture() {
    let text = serde_json::to_string_pretty(&observed_fields()).unwrap() + "\n";
    fs::create_dir_all(fixture_path().parent().unwrap()).unwrap();
    fs::write(fixture_path(), text).unwrap();
}
