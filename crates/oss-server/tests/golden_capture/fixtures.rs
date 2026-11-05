//! Fixed values shared by every golden domain. Timestamps/ids are literals so
//! capture never depends on wall-clock time.

use base64::Engine as _;
use sha2::{Digest, Sha256};

use oss_server::raft::data::{
    AccessKeyRow, AccountMemberRow, AccountRow, AccountUsageLedgerRow, BucketPolicyRow, BucketRow,
    ControlData, EventConfigRow, InventoryConfigRow, LifecycleRuleRow, OlapConfigRow, OwnerRef,
    PermissionRow, ReplicationRuleRow, UserRow, VpcEndpointRow,
};
use oss_server::raft::runstate::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, JobRow, JobTaskRow,
    LifecycleCursorRow, RunState, ScheduleLeaseRow, TierRestoreRow,
};
use xl_store::control_store::AccessPointEntry;

pub const USER: &str = "alice\"q\\b\u{1}";
pub const PASSWORD_HASH: &str = "hash\"q\\b\u{1}";
pub const BUCKET: &str = "bucket\"q\\b\u{1}";
pub const POLICY_JSON: &str = "{\"k\":\"q\\\"\\\\\\u0001\"}";
pub const EVENT_JSON: &str = r#"{"targets":[{"id":"t1","arn":"arn:aws:sns:us-east-1:1:t"}]}"#;
pub const RULES_JSON: &str = r#"[{"id":"r1","dest":"dst","prefix":"logs/","status":"enabled"}]"#;
pub const INVENTORY_JSON: &str = r#"{"format":"CSV","schedule":"daily"}"#;
pub const AP: &str = "ap\"q\\b\u{1}";
pub const AK: &str = "AK\"q\\b\u{1}";
pub const SK: &str = "SK\"q\\b\u{1}";
pub const ERROR_TEXT: &str = "err\"q\\b\u{1}";
pub const SPEC_JSON: &str = r#"{"op":"lifecycle","dry_run":false}"#;
pub const TASK_JSON: &str = r#"{"key":"logs/a b"}"#;

pub fn level_one() -> ControlData {
    let module = b"\x00\x01golden-module\xff";
    ControlData {
        users: vec![UserRow {
            username: USER.into(),
            password_hash: PASSWORD_HASH.into(),
            status: "active".into(),
            created_at: 10,
        }],
        access_keys: vec![AccessKeyRow {
            access_key: AK.into(),
            secret_key: SK.into(),
            username: USER.into(),
            status: "active".into(),
            expires_at: Some(20),
            created_at: 10,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        }],
        permissions: vec![PermissionRow {
            username: USER.into(),
            bucket: BUCKET.into(),
            can_read: true,
            can_write: false,
            can_delete: true,
        }],
        buckets: vec![BucketRow {
            name: BUCKET.into(),
            owner: OwnerRef::User(USER.into()),
            creation_date: 30,
            versioning: "Enabled".into(),
            object_lock: true,
            default_encryption: Some("AES256".into()),
            object_lock_default_mode: Some("GOVERNANCE".into()),
            object_lock_default_days: Some(7),
            replication_mode: "async".into(),
            deleting: false,
        }],
        bucket_policies: vec![BucketPolicyRow {
            bucket: BUCKET.into(),
            policy_json: POLICY_JSON.into(),
            updated_at: 40,
        }],
        lifecycle_rules: vec![LifecycleRuleRow {
            bucket: BUCKET.into(),
            prefix: "logs/".into(),
            rule_id: Some("rule-1".into()),
            expire_days: 30,
            expiration_days: Some(30),
            transition_days: Some(7),
            transition_class: Some("STANDARD_IA".into()),
            transition_tier: Some("tier-a".into()),
            enabled: true,
            updated_at: 50,
            config_json: None,
        }],
        event_configs: vec![EventConfigRow {
            bucket: BUCKET.into(),
            config_json: EVENT_JSON.into(),
            updated_at: 60,
        }],
        replication_rules: vec![ReplicationRuleRow {
            bucket: BUCKET.into(),
            rules_json: RULES_JSON.into(),
            updated_at: 70,
        }],
        access_points: vec![AccessPointEntry {
            name: AP.into(),
            bucket: BUCKET.into(),
            owner: OwnerRef::Admin,
            policy_json: Some(POLICY_JSON.into()),
            created_at: 80,
            updated_at: 90,
        }],
        olap_configs: vec![OlapConfigRow {
            access_point: AP.into(),
            kind: "http".into(),
            endpoint: Some("http://127.0.0.1:9/transform".into()),
            module_b64: Some(base64::engine::general_purpose::STANDARD.encode(module)),
            module_sha256: Some(hex::encode(Sha256::digest(module))),
            timeout_ms: 1500,
            max_response_bytes: 4096,
            updated_at: 100,
        }],
        inventory_configs: vec![InventoryConfigRow {
            bucket: BUCKET.into(),
            id: "inv-1".into(),
            owner: OwnerRef::User(USER.into()),
            config_json: INVENTORY_JSON.into(),
            updated_at: 110,
        }],
        ..ControlData::default()
    }
}

pub fn level_two() -> ControlData {
    let mut data = level_one();
    data.lifecycle_rules[0].config_json = Some(r#"{"Rules":[{"ID":"rule-1"}]}"#.into());
    data
}

pub fn level_three() -> ControlData {
    let mut data = level_two();
    data.accounts = vec![account()];
    data.account_members = vec![AccountMemberRow {
        account_id: "123456789012".into(),
        username: USER.into(),
        role: "admin".into(),
        created_at: 120,
    }];
    data
}

pub fn account() -> AccountRow {
    AccountRow {
        id: "123456789012".into(),
        name: "account\"q\\b\u{1}".into(),
        status: "active".into(),
        admin: Some(USER.into()),
        bucket_quota: Some(10),
        soft_byte_quota: Some(1_000_000),
        created_at: 120,
        updated_at: 121,
    }
}

pub fn vpc() -> VpcEndpointRow {
    VpcEndpointRow {
        vpce_id: "vpce-0123456789abcdef0".into(),
        vpc_id: "vpc-0123456789abcdef0".into(),
        account_id: "123456789012".into(),
        state: "available".into(),
        created_at: 130,
        updated_at: 131,
    }
}

pub fn ledger() -> AccountUsageLedgerRow {
    AccountUsageLedgerRow {
        account_id: "123456789012".into(),
        used_bytes: 4096,
        object_count: 3,
    }
}

pub fn runstate() -> RunState {
    RunState {
        jobs: vec![
            JobRow {
                id: 1,
                kind: "lifecycle".into(),
                bucket: BUCKET.into(),
                prefix: "logs/".into(),
                state: "running".into(),
                scanned: 5,
                deleted: 1,
                skipped: 2,
                error: Some(ERROR_TEXT.into()),
                created_at: 120,
                updated_at: 121,
                spec_json: Some(SPEC_JSON.into()),
                progress: 3,
                total: 5,
                succeeded: 2,
                failed: 1,
                report_bucket: Some("reports".into()),
                report_prefix: Some("r/".into()),
                priority: 2,
                requested_by: Some(9),
                parent_job_id: None,
                started_at: Some(120),
                finished_at: None,
            },
            JobRow {
                id: 2,
                kind: "inventory".into(),
                bucket: BUCKET.into(),
                prefix: String::new(),
                state: "done".into(),
                error: None,
                created_at: 122,
                updated_at: 123,
                parent_job_id: Some(1),
                finished_at: Some(123),
                ..JobRow::default()
            },
        ],
        job_tasks: vec![JobTaskRow {
            job_id: 1,
            task_no: 1,
            action: "delete".into(),
            payload_json: TASK_JSON.into(),
            state: "queued".into(),
            attempt: 1,
            next_at: 130,
            dedupe_key: Some("d1".into()),
            deadline_at: Some(999),
            result_code: None,
            result_string: None,
            started_at: None,
            finished_at: None,
            fence_term: 3,
            fence_leader: 2,
        }],
        inventory_runs: vec![InventoryRunRow {
            id: 1,
            bucket: BUCKET.into(),
            config_id: "inv-1".into(),
            run_key: "2026-11-05T00:00:00Z".into(),
            started_at: 140,
            state: "done".into(),
            job_id: Some(1),
            manifest_key: Some("m.json".into()),
            error: None,
            created_at: 140,
            fence_term: 3,
            fence_leader: 2,
        }],
        inventory_cursors: vec![InventoryCursorRow {
            bucket: BUCKET.into(),
            config_id: "inv-1".into(),
            cursor: "c1".into(),
            config_updated_at: 110,
            updated_at: 141,
            fence_term: 3,
            fence_leader: 2,
        }],
        inventory_incremental: vec![InventoryIncrementalRow {
            bucket: BUCKET.into(),
            config_id: "inv-1".into(),
            enabled: true,
            updated_at: 142,
        }],
        tier_restores: vec![TierRestoreRow {
            bucket: BUCKET.into(),
            key: "k".into(),
            version_id: "v1".into(),
            expires_at: 143,
            job_id: Some(1),
            state: "done".into(),
            created_at: 143,
            fence_term: 3,
            fence_leader: 2,
        }],
        lifecycle_cursors: vec![LifecycleCursorRow {
            bucket: BUCKET.into(),
            rule_id: "rule-1".into(),
            stage: "expire".into(),
            cursor: "o1".into(),
            updated_at: 144,
            fence_term: 3,
            fence_leader: 2,
        }],
        schedule_lease: Some(ScheduleLeaseRow {
            term: 3,
            node: 2,
            expires_at: 200,
            acquired_at: 150,
        }),
        next_job_id: 3,
        next_run_id: 2,
    }
}
