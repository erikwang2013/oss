//! Level C M0：冻结 OSSCTL1 / OSSCTL2 快照字节与 LC-0 导出封套（patch/旧二进制复用）。
//!
//! 快照 fixture 是**逐字节**契约：本二进制对同一 `ControlData` 重新编码必须字节相同
//! （checksum 头 + 行序 + 字段序全部固定）。导出 fixture 走 `verify_export`，钉住来源
//! provenance（snapshot_format=2 / feature_level=2 / wire_version=2）的接受口径。
//!
//! 生成：`cargo test -p oss-server --test compat_fixtures -- --ignored generate_fixtures`
//! 后人工核对 magic（v1=`OSSCTL1`、v2=`OSSCTL2`）再提交。

use std::fs;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use oss_server::raft::cutover::{self, ExportFile};
use oss_server::raft::data::{
    self, AccessKeyRow, AccessPointRow, BucketPolicyRow, BucketRow, ControlData, EventConfigRow,
    InventoryConfigRow, LifecycleRuleRow, OlapConfigRow, OwnerRef, PermissionRow,
    ReplicationRuleRow, UserRow,
};
use sha2::{Digest, Sha256};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compat")
}

fn read_b64(name: &str) -> Vec<u8> {
    let text = fs::read_to_string(fixture_dir().join(name))
        .unwrap_or_else(|e| panic!("fixture {name} missing: {e}"));
    base64::engine::general_purpose::STANDARD
        .decode(text.trim())
        .unwrap()
}

/// 11 张表各一行（自然键最小值）；level-2 版本只给生命周期规则加 `config_json`。
fn level_one() -> ControlData {
    ControlData {
        users: vec![UserRow {
            username: "alice".into(),
            password_hash: "h".into(),
            status: "active".into(),
            created_at: 10,
        }],
        access_keys: vec![AccessKeyRow {
            access_key: "AKALICE".into(),
            secret_key: "SK".into(),
            username: "alice".into(),
            status: "active".into(),
            expires_at: Some(99),
            created_at: 11,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        }],
        permissions: vec![PermissionRow {
            username: "alice".into(),
            bucket: "b1".into(),
            can_read: true,
            can_write: true,
            can_delete: false,
        }],
        buckets: vec![BucketRow {
            name: "b1".into(),
            owner: OwnerRef::User("alice".into()),
            creation_date: 12,
            versioning: "Enabled".into(),
            object_lock: false,
            default_encryption: Some("AES256".into()),
            object_lock_default_mode: None,
            object_lock_default_days: None,
            replication_mode: "async".into(),
            deleting: false,
        }],
        bucket_policies: vec![BucketPolicyRow {
            bucket: "b1".into(),
            policy_json: "{}".into(),
            updated_at: 13,
        }],
        lifecycle_rules: vec![LifecycleRuleRow {
            bucket: "b1".into(),
            prefix: "logs/".into(),
            rule_id: Some("r1".into()),
            expire_days: 30,
            expiration_days: Some(30),
            transition_days: Some(7),
            transition_class: Some("GLACIER".into()),
            transition_tier: Some("cold".into()),
            enabled: true,
            updated_at: 14,
            config_json: None,
        }],
        event_configs: vec![EventConfigRow {
            bucket: "b1".into(),
            config_json: "{\"topics\":[]}".into(),
            updated_at: 15,
        }],
        replication_rules: vec![ReplicationRuleRow {
            bucket: "b1".into(),
            rules_json: "[]".into(),
            updated_at: 16,
        }],
        access_points: vec![AccessPointRow {
            name: "ap1".into(),
            bucket: "b1".into(),
            owner: OwnerRef::Admin,
            policy_json: None,
            created_at: 17,
            updated_at: 18,
        }],
        olap_configs: vec![OlapConfigRow {
            access_point: "ap1".into(),
            kind: "http".into(),
            endpoint: Some("https://example.com/t".into()),
            module_b64: None,
            module_sha256: None,
            timeout_ms: 5_000,
            max_response_bytes: 1_024,
            updated_at: 19,
        }],
        inventory_configs: vec![InventoryConfigRow {
            bucket: "b1".into(),
            id: "inv1".into(),
            owner: OwnerRef::Admin,
            config_json: "{}".into(),
            updated_at: 20,
        }],
    }
}

fn level_two() -> ControlData {
    let mut data = level_one();
    data.lifecycle_rules[0].config_json = Some(r#"{"filter":{"prefix":"logs/"}}"#.into());
    data
}

/// LC-0 导出的稳定 fixture：来源激活 level 2（数据也要求 2），快照格式 2、wire 2。
fn export_fixture() -> ExportFile {
    let data = level_two();
    let payload = serde_json::to_vec(&data).unwrap();
    ExportFile {
        // LC-0 冻结 fixture 是跑的格式 1；新写出走 EXPORT_FORMAT=2。
        format: cutover::MIN_EXPORT_FORMAT,
        kind: cutover::EXPORT_KIND.to_string(),
        source_site: "site-a".into(),
        source_node_id: Some(1),
        source_role: "voter".into(),
        version: "1.17.0".into(),
        feature_level: Some(2),
        activation_level: Some(2),
        snapshot_format: Some(2),
        wire_version: Some(2),
        source_build: Some("wave0-track-a".into()),
        admin_username: Some("admin".into()),
        has_vault: Some(false),
        exported_at: 1_700_000_000,
        sha256: hex::encode(Sha256::digest(&payload)),
        runstate: None,
        data,
    }
}

/// OSSCTL1 逐字节冻结：解码回 level 1，重新编码字节相同。
#[test]
fn snapshot_v1_fixture_is_byte_identical() {
    let bytes = read_b64("snapshot_v1.b64");
    assert!(bytes.starts_with(b"OSSCTL1\n"), "v1 fixture magic");
    let decoded = data::decode_snapshot(&bytes).unwrap();
    assert_eq!(decoded.feature_level, 1);
    assert_eq!(decoded.data, level_one());
    assert!(decoded.extensions.is_empty(), "V1 无扩展区");
    assert_eq!(
        data::encode_snapshot_at(&decoded.data, 1).unwrap(),
        bytes,
        "V1 载荷/行序/checksum 必须逐字节不变"
    );
}

/// OSSCTL2 逐字节冻结：带 feature_level=2 与 config_json 行，重新编码字节相同。
#[test]
fn snapshot_v2_level2_fixture_is_byte_identical() {
    let bytes = read_b64("snapshot_v2_level2.b64");
    assert!(bytes.starts_with(b"OSSCTL2\n"), "v2 fixture magic");
    let decoded = data::decode_snapshot(&bytes).unwrap();
    assert_eq!(decoded.feature_level, 2);
    assert_eq!(decoded.data, level_two());
    assert!(
        decoded.data.lifecycle_rules[0].config_json.is_some(),
        "v2 fixture 必须覆盖 level-2 行"
    );
    assert!(decoded.extensions.is_empty(), "V2 无扩展区");
    assert_eq!(
        data::encode_snapshot_at(&decoded.data, 2).unwrap(),
        bytes,
        "V2 载荷/checksum 必须逐字节不变"
    );
}

/// LC-0 导出封套：provenance 完整，本二进制 verify_export 零 warning。
#[test]
fn export_v2_level2_fixture_verifies_clean() {
    let text = fs::read_to_string(fixture_dir().join("export_v2_level2.json"))
        .expect("export fixture missing");
    let export: ExportFile = serde_json::from_str(&text).unwrap();
    assert_eq!(
        export.format,
        cutover::MIN_EXPORT_FORMAT,
        "frozen LC-0 fixture predates runstate-bearing format 2"
    );
    assert_eq!(export.kind, cutover::EXPORT_KIND);
    assert_eq!(export.snapshot_format, Some(2));
    assert_eq!(export.feature_level, Some(2));
    assert_eq!(export.activation_level, Some(2));
    assert_eq!(export.wire_version, Some(2));
    assert_eq!(export.data, level_two());
    assert!(
        cutover::verify_export(&export).unwrap().is_empty(),
        "完整 provenance 的导出不得产生 warning"
    );
}

/// 重新生成三个 fixture（人工核对后提交）。
#[test]
#[ignore = "regenerates committed compat fixtures"]
fn generate_fixtures() {
    let dir = fixture_dir();
    fs::create_dir_all(&dir).unwrap();
    let b64 = base64::engine::general_purpose::STANDARD;
    fs::write(
        dir.join("snapshot_v1.b64"),
        b64.encode(data::encode_snapshot_at(&level_one(), 1).unwrap()),
    )
    .unwrap();
    fs::write(
        dir.join("snapshot_v2_level2.b64"),
        b64.encode(data::encode_snapshot_at(&level_two(), 2).unwrap()),
    )
    .unwrap();
    fs::write(
        dir.join("export_v2_level2.json"),
        serde_json::to_string_pretty(&export_fixture()).unwrap() + "\n",
    )
    .unwrap();
}
