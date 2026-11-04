//! IAM / 账户快照行的字节顺序冻结（V1/V2/V3 已发布快照不可改写）。

use oss_server::raft::data::{
    AccessKeyRow, AccountMemberRow, AccountRow, AccountUsageLedgerRow, ControlData, PermissionRow,
    UserRow, VpcEndpointRow, decode_snapshot, encode_snapshot_at,
};

fn sample() -> ControlData {
    ControlData {
        users: vec![UserRow {
            username: "alice".into(),
            password_hash: "h".into(),
            status: "active".into(),
            created_at: 1,
        }],
        access_keys: vec![AccessKeyRow {
            access_key: "AK".into(),
            secret_key: "SK".into(),
            username: "alice".into(),
            status: "active".into(),
            expires_at: None,
            created_at: 2,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        }],
        permissions: vec![PermissionRow {
            username: "alice".into(),
            bucket: "b".into(),
            can_read: true,
            can_write: false,
            can_delete: true,
        }],
        accounts: vec![AccountRow {
            id: "000000000001".into(),
            name: "acme".into(),
            status: "active".into(),
            admin: Some("alice".into()),
            bucket_quota: Some(2),
            soft_byte_quota: None,
            created_at: 3,
            updated_at: 4,
        }],
        account_members: vec![AccountMemberRow {
            account_id: "000000000001".into(),
            username: "alice".into(),
            role: "owner".into(),
            created_at: 5,
        }],
        vpc_endpoints: vec![VpcEndpointRow {
            vpce_id: "vpce-1".into(),
            vpc_id: "vpc-1".into(),
            account_id: "000000000001".into(),
            state: "active".into(),
            created_at: 6,
            updated_at: 7,
        }],
        account_usage_ledger: vec![AccountUsageLedgerRow {
            account_id: "000000000001".into(),
            used_bytes: 8,
            object_count: 9,
        }],
        ..Default::default()
    }
}

#[test]
fn control_data_json_field_order_is_frozen() {
    let json = serde_json::to_string(&sample()).unwrap();
    let pos = |k: &str| {
        json.find(&format!("\"{k}\":"))
            .unwrap_or_else(|| panic!("missing {k}: {json}"))
    };
    let order = [
        "users",
        "access_keys",
        "permissions",
        "buckets",
        "bucket_policies",
        "lifecycle_rules",
        "event_configs",
        "replication_rules",
        "access_points",
        "olap_configs",
        "inventory_configs",
        "accounts",
        "account_members",
        "vpc_endpoints",
        "account_usage_ledger",
    ];
    for pair in order.windows(2) {
        assert!(
            pos(pair[0]) < pos(pair[1]),
            "{} 必须在 {} 之前",
            pair[0],
            pair[1]
        );
    }
    // 行内字段顺序同样是快照字节的一部分。
    let rows: [(&str, &[&str]); 6] = [
        (
            "\"users\"",
            &["username", "password_hash", "status", "created_at"],
        ),
        (
            "\"access_keys\"",
            &[
                "access_key",
                "secret_key",
                "username",
                "status",
                "expires_at",
                "created_at",
                "parent_ak",
                "session_policy",
                "kind",
            ],
        ),
        (
            "\"permissions\"",
            &["username", "bucket", "can_read", "can_write", "can_delete"],
        ),
        (
            "\"accounts\"",
            &[
                "id",
                "name",
                "status",
                "admin",
                "bucket_quota",
                "soft_byte_quota",
                "created_at",
                "updated_at",
            ],
        ),
        (
            "\"account_members\"",
            &["account_id", "username", "role", "created_at"],
        ),
        (
            "\"account_usage_ledger\"",
            &["account_id", "used_bytes", "object_count"],
        ),
    ];
    for (start, keys) in rows {
        let base = json.find(start).unwrap();
        let mut prev = base;
        for k in keys {
            let at = json[base..].find(&format!("\"{k}\":")).unwrap() + base;
            assert!(at >= prev, "{start} 内 {k} 顺序漂移");
            prev = at;
        }
    }
}

#[test]
fn empty_level3_vectors_stay_omitted_and_round_trip() {
    let empty = ControlData::default();
    let json = serde_json::to_string(&empty).unwrap();
    for k in [
        "accounts",
        "account_members",
        "vpc_endpoints",
        "account_usage_ledger",
    ] {
        assert!(!json.contains(&format!("\"{k}\"")), "空 {k} 不得序列化");
    }
    let data = sample();
    let bytes = encode_snapshot_at(&data, 1).unwrap();
    let decoded = decode_snapshot(&bytes).unwrap();
    assert_eq!(decoded.feature_level, 9, "账本行要求 level 9");
    assert_eq!(decoded.data, data);
    assert_eq!(encode_snapshot_at(&decoded.data, 1).unwrap(), bytes);
}
