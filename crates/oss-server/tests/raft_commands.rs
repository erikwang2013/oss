//! M9 S6a：控制命令 serde/尺寸闸、apply 自然键映射与级联、fail-stop 回滚。
//!
//! 全部走 `AppState::control`（`[raft]` 关闭 = Local 门面，同一个 apply 映射 + 事务）。

mod common;

use base64::Engine as _;
use oss_server::raft::control::ControlError;
use oss_server::raft::{
    COMMAND_MAX_BYTES, ControlCommand, ControlResponse, LifecycleRuleCmd, PermissionBits,
    RaftSettings, SizeError,
};
use oss_server::state::AppState;
use sqlx::Row;

fn user_create(username: &str) -> ControlCommand {
    ControlCommand::UserCreate {
        username: username.into(),
        password_hash: "hash".into(),
        status: "active".into(),
        created_at: 1,
    }
}

fn bucket_create(name: &str, owner: Option<&str>) -> ControlCommand {
    ControlCommand::BucketCreate {
        name: name.into(),
        owner: owner.map(str::to_string),
        created_at: 1,
        replication_mode: "async".into(),
        versioning: "Off".into(),
        object_lock: false,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        default_encryption: None,
    }
}

fn lifecycle_rule(prefix: &str, id: &str) -> LifecycleRuleCmd {
    LifecycleRuleCmd {
        rule_id: Some(id.into()),
        prefix: prefix.into(),
        expire_days: 1,
        expiration_days: None,
        transition_days: None,
        transition_class: None,
        transition_tier: None,
        enabled: true,
    }
}

async fn write(st: &AppState, cmd: ControlCommand) -> ControlResponse {
    st.control.write(cmd).await.unwrap()
}

async fn count(st: &AppState, sql: &'static str, bucket: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(bucket)
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

#[test]
fn command_serde_round_trip_every_variant_and_rejects_unknown_fields() {
    let commands = vec![
        user_create("u"),
        ControlCommand::UserDelete {
            username: "u".into(),
        },
        ControlCommand::UserSetPassword {
            username: "u".into(),
            password_hash: "h".into(),
        },
        ControlCommand::UserSetStatus {
            username: "u".into(),
            status: "disabled".into(),
        },
        ControlCommand::UserSetPermissions {
            username: "u".into(),
            permissions: vec![PermissionBits {
                bucket: "b".into(),
                read: true,
                write: false,
                delete: true,
            }],
        },
        ControlCommand::AccessKeyCreate {
            access_key: "ak".into(),
            secret_key: "sk".into(),
            username: "u".into(),
            kind: "long".into(),
            parent_ak: None,
            session_policy: None,
            created_at: 1,
        },
        ControlCommand::AccessKeyDelete {
            access_key: "ak".into(),
        },
        ControlCommand::AccessKeySetStatus {
            access_key: "ak".into(),
            status: "disabled".into(),
        },
        bucket_create("b", None),
        ControlCommand::BucketDelete { name: "b".into() },
        ControlCommand::BucketSetDeleting {
            name: "b".into(),
            deleting: true,
        },
        ControlCommand::BucketSetVersioning {
            name: "b".into(),
            versioning: "Enabled".into(),
        },
        ControlCommand::BucketSetObjectLock {
            name: "b".into(),
            enabled: true,
            default_mode: Some("GOVERNANCE".into()),
            default_days: Some(1),
        },
        ControlCommand::BucketSetNotification {
            bucket: "b".into(),
            config_json: "{}".into(),
            updated_at: 1,
        },
        ControlCommand::BucketClearNotification { bucket: "b".into() },
        ControlCommand::BucketSetLifecycle {
            bucket: "b".into(),
            rules: vec![lifecycle_rule("p", "r")],
            updated_at: 1,
        },
        ControlCommand::BucketClearLifecycle { bucket: "b".into() },
        ControlCommand::BucketSetReplication {
            bucket: "b".into(),
            rules_json: "[]".into(),
            updated_at: 1,
        },
        ControlCommand::BucketClearReplication { bucket: "b".into() },
        ControlCommand::BucketSetDefaultEncryption {
            bucket: "b".into(),
            algorithm: "AES256".into(),
        },
        ControlCommand::BucketClearDefaultEncryption { bucket: "b".into() },
        ControlCommand::BucketSetPolicy {
            bucket: "b".into(),
            policy_json: "{}".into(),
            updated_at: 1,
        },
        ControlCommand::BucketDeletePolicy { bucket: "b".into() },
        ControlCommand::AccessPointPut {
            name: "ap".into(),
            bucket: "b".into(),
            owner: None,
            policy_json: Some("{}".into()),
            created_at: 1,
            updated_at: 1,
        },
        ControlCommand::AccessPointDelete { name: "ap".into() },
        ControlCommand::AccessPointSetPolicy {
            name: "ap".into(),
            policy_json: None,
            updated_at: 1,
        },
        ControlCommand::OlapPut {
            access_point: "ap".into(),
            kind: "wasm".into(),
            endpoint: None,
            module: Some("AA==".into()),
            module_sha256: Some("00".into()),
            timeout_ms: 1000,
            max_response_bytes: 1024,
            updated_at: 1,
        },
        ControlCommand::OlapDelete {
            access_point: "ap".into(),
        },
        ControlCommand::InventoryPut {
            bucket: "b".into(),
            id: "i".into(),
            owner: Some("u".into()),
            config_json: "{}".into(),
            updated_at: 1,
        },
        ControlCommand::InventoryDelete {
            bucket: "b".into(),
            id: "i".into(),
        },
    ];
    for cmd in &commands {
        let json = serde_json::to_string(cmd).unwrap();
        let back: ControlCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(&back, cmd, "round-trip failed for {json}");
    }
    // 未知字段（变体级）与未知变体都必须拒绝，避免跨版本静默丢字段。
    let unknown_field = r#"{"UserCreate":{"username":"u","password_hash":"h","status":"active","created_at":0,"extra":1}}"#;
    assert!(serde_json::from_str::<ControlCommand>(unknown_field).is_err());
    assert!(serde_json::from_str::<ControlCommand>(r#"{"NoSuchCommand":{}}"#).is_err());
}

#[test]
fn command_size_gate_accepts_16mib_module_and_rejects_over_24mib() {
    let make = |bytes: usize| ControlCommand::OlapPut {
        access_point: "ap".into(),
        kind: "wasm".into(),
        endpoint: None,
        module: Some(base64::engine::general_purpose::STANDARD.encode(vec![0u8; bytes])),
        module_sha256: None,
        timeout_ms: 1,
        max_response_bytes: 1,
        updated_at: 1,
    };
    // 16 MiB 模块 → ~21.4 MiB base64，必须在 24 MiB 闸内。
    make(16 * 1024 * 1024).check_size().unwrap();
    // 19 MiB 模块 → ~25.4 MiB base64，append 前拒绝。
    match make(19 * 1024 * 1024).check_size() {
        Err(SizeError::TooLarge(n)) => assert!(n > COMMAND_MAX_BYTES),
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn owner_mapping_and_permission_bits() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    let admin_id = st.db.create_user("admin", "pw", true).await.unwrap();
    assert_eq!(write(&st, user_create("alice")).await, ControlResponse::Ok);
    let alice_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'alice'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();

    // 管理员属主 → buckets.owner_id NULL；普通用户 → 本地 id。
    assert_eq!(
        write(&st, bucket_create("admin-owned", None)).await,
        ControlResponse::Ok
    );
    let owner: Option<i64> = sqlx::query_scalar("SELECT owner_id FROM buckets WHERE name = ?")
        .bind("admin-owned")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(owner, None);
    assert_eq!(
        write(&st, bucket_create("alice-owned", Some("alice"))).await,
        ControlResponse::Ok
    );
    let owner: Option<i64> = sqlx::query_scalar("SELECT owner_id FROM buckets WHERE name = ?")
        .bind("alice-owned")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(owner, Some(alice_id));

    // AP/inventory 属主 NOT NULL：管理员 → 本地管理员 id。
    assert_eq!(
        write(
            &st,
            ControlCommand::AccessPointPut {
                name: "ap1".into(),
                bucket: "alice-owned".into(),
                owner: None,
                policy_json: None,
                created_at: 1,
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::Ok
    );
    let ap_owner: i64 = sqlx::query_scalar("SELECT owner_id FROM access_points WHERE name = 'ap1'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(ap_owner, admin_id);
    for owner in [None, Some("admin".to_string())] {
        assert_eq!(
            write(
                &st,
                ControlCommand::InventoryPut {
                    bucket: "alice-owned".into(),
                    id: "inv".into(),
                    owner,
                    config_json: "{}".into(),
                    updated_at: 1,
                }
            )
            .await,
            ControlResponse::Ok
        );
        let inv_owner: i64 = sqlx::query_scalar(
            "SELECT owner_id FROM inventory_configs WHERE bucket = ? AND id = ?",
        )
        .bind("alice-owned")
        .bind("inv")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
        assert_eq!(inv_owner, admin_id);
    }

    // 权限位：全量替换、按 bucket 排序、全零行丢弃。
    assert_eq!(
        write(
            &st,
            ControlCommand::UserSetPermissions {
                username: "alice".into(),
                permissions: vec![
                    PermissionBits {
                        bucket: "b".into(),
                        read: true,
                        write: false,
                        delete: true,
                    },
                    PermissionBits {
                        bucket: "a".into(),
                        read: true,
                        write: true,
                        delete: true,
                    },
                    PermissionBits {
                        bucket: "z".into(),
                        read: false,
                        write: false,
                        delete: false,
                    },
                ],
            }
        )
        .await,
        ControlResponse::Ok
    );
    let rows = sqlx::query(
        "SELECT bucket, can_read, can_write, can_delete FROM permissions \
         WHERE user_id = ? ORDER BY bucket",
    )
    .bind(alice_id)
    .fetch_all(st.db.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("bucket"), "a");
    assert_eq!(rows[0].get::<i64, _>("can_delete"), 1);
    assert_eq!(rows[1].get::<String, _>("bucket"), "b");
    assert_eq!(rows[1].get::<i64, _>("can_write"), 0);
    assert_eq!(
        write(
            &st,
            ControlCommand::UserSetPermissions {
                username: "alice".into(),
                permissions: vec![],
            }
        )
        .await,
        ControlResponse::Ok
    );
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM permissions WHERE bucket = ?",
            "a"
        )
        .await,
        0
    );
}

/// M1：管理员用户/管理员属主 AK 是站点本地状态，IAM 命令一律 Invalid（S6b 留本地）。
#[tokio::test]
async fn admin_target_iam_commands_are_invalid() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    let admin_id = st.db.create_user("admin", "pw", true).await.unwrap();
    st.db
        .create_access_key("ADMINAK", "sk", admin_id)
        .await
        .unwrap();
    let admin_hash: String =
        sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'admin'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    sqlx::query(
        "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
         VALUES (?, 'kept', 1, 0, 0)",
    )
    .bind(admin_id)
    .execute(st.db.pool())
    .await
    .unwrap();

    for cmd in [
        ControlCommand::UserDelete {
            username: "admin".into(),
        },
        ControlCommand::UserSetPassword {
            username: "admin".into(),
            password_hash: "new".into(),
        },
        ControlCommand::UserSetStatus {
            username: "admin".into(),
            status: "disabled".into(),
        },
        ControlCommand::UserSetPermissions {
            username: "admin".into(),
            permissions: vec![],
        },
        ControlCommand::AccessKeyCreate {
            access_key: "NEWAK".into(),
            secret_key: "sk".into(),
            username: "admin".into(),
            kind: "access_key".into(),
            parent_ak: None,
            session_policy: None,
            created_at: 1,
        },
        ControlCommand::AccessKeyDelete {
            access_key: "ADMINAK".into(),
        },
        ControlCommand::AccessKeySetStatus {
            access_key: "ADMINAK".into(),
            status: "disabled".into(),
        },
    ] {
        assert_eq!(write(&st, cmd).await, ControlResponse::Invalid);
    }

    let (status, n): (String, i64) =
        sqlx::query_as("SELECT status, COUNT(*) FROM users WHERE username = 'admin'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!((status.as_str(), n), ("active", 1));
    let stored_hash: String =
        sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'admin'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(
        stored_hash, admin_hash,
        "拒绝后管理员口令哈希必须逐字节不变"
    );
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM permissions WHERE user_id = ? AND bucket = 'kept' AND can_read = 1",
    )
    .bind(admin_id)
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(n, 1, "UserSetPermissions 拒绝不得清空管理员权限行");
    let (ak_status, n): (String, i64) =
        sqlx::query_as("SELECT status, COUNT(*) FROM access_keys WHERE access_key = 'ADMINAK'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!((ak_status.as_str(), n), ("active", 1));
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM access_keys WHERE access_key = 'NEWAK'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn lifecycle_and_replication_replace_deterministically() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert_eq!(
        write(&st, bucket_create("b", None)).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetLifecycle {
                bucket: "b".into(),
                // 故意乱序；apply 按 (prefix, rule_id) 排序落库。
                rules: vec![lifecycle_rule("b", "r2"), lifecycle_rule("a", "r1")],
                updated_at: 7,
            }
        )
        .await,
        ControlResponse::Ok
    );
    let rows = sqlx::query(
        "SELECT prefix, rule_id, updated_at FROM lifecycle_rules WHERE bucket = 'b' ORDER BY rowid",
    )
    .fetch_all(st.db.pool())
    .await
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.get::<String, _>("prefix"), r.get::<String, _>("rule_id")))
            .collect::<Vec<_>>(),
        vec![
            ("a".to_string(), "r1".to_string()),
            ("b".to_string(), "r2".to_string())
        ]
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetLifecycle {
                bucket: "b".into(),
                rules: vec![lifecycle_rule("c", "r3")],
                updated_at: 8,
            }
        )
        .await,
        ControlResponse::Ok
    );
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT prefix FROM lifecycle_rules WHERE bucket = 'b'")
            .fetch_all(st.db.pool())
            .await
            .unwrap();
    assert_eq!(rows, vec!["c"]);

    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetReplication {
                bucket: "b".into(),
                rules_json: r#"[{"id":"r1"},{"id":"r2"}]"#.into(),
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetReplication {
                bucket: "b".into(),
                rules_json: r#"[{"id":"r2"}]"#.into(),
                updated_at: 2,
            }
        )
        .await,
        ControlResponse::Ok
    );
    let (json, updated): (String, i64) =
        sqlx::query_as("SELECT rules_json, updated_at FROM replication_rules WHERE bucket = 'b'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(json, r#"[{"id":"r2"}]"#);
    assert_eq!(updated, 2);
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketClearReplication { bucket: "b".into() }
        )
        .await,
        ControlResponse::Ok
    );
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM replication_rules WHERE bucket = ?",
            "b"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn apply_conflicts_and_invalids() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert_eq!(write(&st, user_create("u")).await, ControlResponse::Ok);
    assert_eq!(
        write(&st, user_create("u")).await,
        ControlResponse::Conflict
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::UserSetStatus {
                username: "u".into(),
                status: "bogus".into(),
            }
        )
        .await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::UserSetStatus {
                username: "nobody".into(),
                status: "disabled".into(),
            }
        )
        .await,
        ControlResponse::NotFound
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::UserSetPassword {
                username: "u".into(),
                password_hash: String::new(),
            }
        )
        .await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::UserDelete {
                username: "no".into()
            }
        )
        .await,
        ControlResponse::NotFound
    );

    let ak = ControlCommand::AccessKeyCreate {
        access_key: "ak".into(),
        secret_key: "sk".into(),
        username: "u".into(),
        kind: "long".into(),
        parent_ak: None,
        session_policy: None,
        created_at: 1,
    };
    assert_eq!(write(&st, ak.clone()).await, ControlResponse::Ok);
    assert_eq!(write(&st, ak).await, ControlResponse::Conflict);
    assert_eq!(
        write(
            &st,
            ControlCommand::AccessKeySetStatus {
                access_key: "ak".into(),
                status: "bogus".into(),
            }
        )
        .await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::AccessKeySetStatus {
                access_key: "missing".into(),
                status: "disabled".into(),
            }
        )
        .await,
        ControlResponse::NotFound
    );

    assert_eq!(
        write(&st, bucket_create("b", None)).await,
        ControlResponse::Ok
    );
    // 同名同模式 = 幂等更新；不同模式 = 409。
    assert_eq!(
        write(&st, bucket_create("b", None)).await,
        ControlResponse::Ok
    );
    let mut sync = bucket_create("b", None);
    if let ControlCommand::BucketCreate {
        replication_mode, ..
    } = &mut sync
    {
        *replication_mode = "sync".into();
    }
    assert_eq!(write(&st, sync).await, ControlResponse::Conflict);
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetVersioning {
                name: "b".into(),
                versioning: "Bogus".into(),
            }
        )
        .await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetVersioning {
                name: "no".into(),
                versioning: "Enabled".into(),
            }
        )
        .await,
        ControlResponse::NotFound
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetLifecycle {
                bucket: "no".into(),
                rules: vec![lifecycle_rule("p", "r")],
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::NotFound
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetNotification {
                bucket: "b".into(),
                config_json: String::new(),
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetPolicy {
                bucket: "b".into(),
                policy_json: String::new(),
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(&st, ControlCommand::BucketDelete { name: "no".into() }).await,
        ControlResponse::NotFound
    );

    // OLAP：非法 kind / wasm 缺模块 / AP 不存在。
    let olap = |kind: &str, module: Option<String>| ControlCommand::OlapPut {
        access_point: "no-ap".into(),
        kind: kind.into(),
        endpoint: None,
        module,
        module_sha256: None,
        timeout_ms: 1,
        max_response_bytes: 1,
        updated_at: 1,
    };
    assert_eq!(
        write(&st, olap("bogus", None)).await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(&st, olap("wasm", None)).await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(&st, olap("http", None)).await,
        ControlResponse::NotFound
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::InventoryPut {
                bucket: "b".into(),
                id: String::new(),
                owner: None,
                config_json: "{}".into(),
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::Invalid
    );
}

#[tokio::test]
async fn internal_apply_error_rolls_back_partial_writes() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert_eq!(
        write(&st, bucket_create("b", None)).await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketSetLifecycle {
                bucket: "b".into(),
                rules: vec![lifecycle_rule("keep", "old")],
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::Ok
    );

    // 两条同 prefix 规则违反 (bucket,prefix) 主键：DELETE 已执行、首条 INSERT 已执行，
    // 第二条失败 → 事务回滚，旧行必须原样保留（fail-stop，无部分写）。
    let err = st
        .control
        .write(ControlCommand::BucketSetLifecycle {
            bucket: "b".into(),
            rules: vec![lifecycle_rule("dup", "r1"), lifecycle_rule("dup", "r2")],
            updated_at: 2,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::Internal(_)), "{err}");
    assert_eq!(err.http_status(), 500);
    let rows =
        sqlx::query("SELECT prefix, rule_id, updated_at FROM lifecycle_rules WHERE bucket = 'b'")
            .fetch_all(st.db.pool())
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("prefix"), "keep");
    assert_eq!(rows[0].get::<String, _>("rule_id"), "old");
    assert_eq!(rows[0].get::<i64, _>("updated_at"), 1);
}

#[tokio::test]
async fn bucket_delete_cascades_all_dependent_rows() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert_eq!(write(&st, user_create("u")).await, ControlResponse::Ok);
    assert_eq!(
        write(&st, bucket_create("cascade", Some("u"))).await,
        ControlResponse::Ok
    );
    for cmd in [
        ControlCommand::UserSetPermissions {
            username: "u".into(),
            permissions: vec![PermissionBits {
                bucket: "cascade".into(),
                read: true,
                write: false,
                delete: false,
            }],
        },
        ControlCommand::BucketSetLifecycle {
            bucket: "cascade".into(),
            rules: vec![lifecycle_rule("p", "r")],
            updated_at: 1,
        },
        ControlCommand::BucketSetNotification {
            bucket: "cascade".into(),
            config_json: "{}".into(),
            updated_at: 1,
        },
        ControlCommand::BucketSetPolicy {
            bucket: "cascade".into(),
            policy_json: "{}".into(),
            updated_at: 1,
        },
        ControlCommand::BucketSetReplication {
            bucket: "cascade".into(),
            rules_json: "[]".into(),
            updated_at: 1,
        },
        ControlCommand::BucketSetDefaultEncryption {
            bucket: "cascade".into(),
            algorithm: "AES256".into(),
        },
        ControlCommand::BucketSetDeleting {
            name: "cascade".into(),
            deleting: true,
        },
        ControlCommand::AccessPointPut {
            name: "cap".into(),
            bucket: "cascade".into(),
            owner: Some("u".into()),
            policy_json: None,
            created_at: 1,
            updated_at: 1,
        },
        ControlCommand::OlapPut {
            access_point: "cap".into(),
            kind: "http".into(),
            endpoint: Some("http://127.0.0.1:1".into()),
            module: None,
            module_sha256: None,
            timeout_ms: 10,
            max_response_bytes: 10,
            updated_at: 1,
        },
        ControlCommand::InventoryPut {
            bucket: "cascade".into(),
            id: "inv".into(),
            owner: None,
            config_json: "{}".into(),
            updated_at: 1,
        },
    ] {
        assert_eq!(write(&st, cmd).await, ControlResponse::Ok);
    }

    assert_eq!(
        write(
            &st,
            ControlCommand::BucketDelete {
                name: "cascade".into()
            }
        )
        .await,
        ControlResponse::Ok
    );
    for (sql, label) in [
        (
            "SELECT COUNT(*) FROM permissions WHERE bucket = ?",
            "permissions",
        ),
        (
            "SELECT COUNT(*) FROM lifecycle_rules WHERE bucket = ?",
            "lifecycle",
        ),
        (
            "SELECT COUNT(*) FROM bucket_policies WHERE bucket = ?",
            "policy",
        ),
        (
            "SELECT COUNT(*) FROM event_configs WHERE bucket = ?",
            "events",
        ),
        (
            "SELECT COUNT(*) FROM replication_rules WHERE bucket = ?",
            "replication",
        ),
        (
            "SELECT COUNT(*) FROM access_points WHERE bucket = ?",
            "access_points",
        ),
        (
            "SELECT COUNT(*) FROM inventory_configs WHERE bucket = ?",
            "inventory",
        ),
        ("SELECT COUNT(*) FROM buckets WHERE name = ?", "buckets"),
    ] {
        assert_eq!(count(&st, sql, "cascade").await, 0, "{label} must cascade");
    }
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM olap_configs WHERE access_point = ?",
            "cap"
        )
        .await,
        0
    );
}

/// 生产接线：状态机指向主控制库连接池，apply 写的是受 guard 保护的 11 张表。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_applies_control_commands_to_guarded_control_db() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("control.db").display()
    );
    let db = oss_server::db::Db::connect_raft(&url).await.unwrap();
    let settings = RaftSettings {
        node_id: 1,
        bootstrap: true,
        advertise_endpoint: "http://127.0.0.1:1".to_string(),
        cluster_token: "test-cluster-token".to_string(),
        db_path: dir.path().join("raft.db"),
        election_timeout_ms: 2_000,
        heartbeat_interval_ms: 200,
        install_snapshot_timeout_ms: 5_000,
        max_in_snapshot_log_to_keep: 0,
        purge_batch_size: 1024,
        write_timeout_ms: 20_000,
        snapshot_max_chunk_size: 4 * 1024 * 1024,
        snapshot_max_bytes: 512 * 1024 * 1024,
    };
    let handle = oss_server::raft::spawn_with_control(settings, Some(db.pool().clone()))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if handle.raft().metrics().borrow().current_leader.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader election timed out"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    assert_eq!(
        handle.write(user_create("raft-user")).await.unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        handle
            .write(bucket_create("raft-bucket", None))
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    let applied = handle.raft().metrics().borrow().last_applied.unwrap();
    assert!(
        handle
            .wait_applied(applied.index, std::time::Duration::from_secs(15))
            .await
    );
    let user: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE username = 'raft-user'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let bucket: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name = 'raft-bucket'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!((user, bucket), (1, 1));
    handle.shutdown().await;
}

#[tokio::test]
async fn user_delete_cascade_matches_db_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert_eq!(write(&st, user_create("u")).await, ControlResponse::Ok);
    let uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'u'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(
        write(
            &st,
            ControlCommand::AccessKeyCreate {
                access_key: "ak".into(),
                secret_key: "sk".into(),
                username: "u".into(),
                kind: "long".into(),
                parent_ak: None,
                session_policy: None,
                created_at: 1,
            }
        )
        .await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::UserSetPermissions {
                username: "u".into(),
                permissions: vec![PermissionBits {
                    bucket: "owned".into(),
                    read: true,
                    write: true,
                    delete: true,
                }],
            }
        )
        .await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(&st, bucket_create("owned", Some("u"))).await,
        ControlResponse::Ok
    );
    sqlx::query(
        "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, owner_id, created_at) \
         VALUES ('/tmp/x', 'owned', 'k', 'file', 1, 'etag', ?, 1)",
    )
    .bind(uid)
    .execute(st.db.pool())
    .await
    .unwrap();

    assert_eq!(
        write(
            &st,
            ControlCommand::UserDelete {
                username: "u".into()
            }
        )
        .await,
        ControlResponse::Ok
    );
    let left: (i64, i64, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT \
         (SELECT COUNT(*) FROM users WHERE id = ?), \
         (SELECT COUNT(*) FROM access_keys WHERE user_id = ?), \
         (SELECT owner_id FROM buckets WHERE name = 'owned'), \
         (SELECT owner_id FROM uploads WHERE saved_path = '/tmp/x')",
    )
    .bind(uid)
    .bind(uid)
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(left, (0, 0, None, None));
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM permissions WHERE bucket = ?",
            "owned"
        )
        .await,
        0
    );
}

/// M3（S6d review）：预检后桶被删的竞态——apply 内复核桶存在，不得留下悬挂行。
#[tokio::test]
async fn ap_and_inventory_put_recheck_bucket_existence_in_apply() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert_eq!(write(&st, user_create("u")).await, ControlResponse::Ok);
    assert_eq!(
        write(&st, bucket_create("race", Some("u"))).await,
        ControlResponse::Ok
    );
    // 模拟 handler 预检通过后、命令 apply 前桶被删。
    assert_eq!(
        write(
            &st,
            ControlCommand::BucketDelete {
                name: "race".into()
            }
        )
        .await,
        ControlResponse::Ok
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::AccessPointPut {
                name: "ap-race".into(),
                bucket: "race".into(),
                owner: Some("u".into()),
                policy_json: None,
                created_at: 1,
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::Invalid
    );
    assert_eq!(
        write(
            &st,
            ControlCommand::InventoryPut {
                bucket: "race".into(),
                id: "inv-race".into(),
                owner: Some("u".into()),
                config_json: "{}".into(),
                updated_at: 1,
            }
        )
        .await,
        ControlResponse::Invalid
    );
    for (sql, label) in [
        (
            "SELECT COUNT(*) FROM access_points WHERE name = 'ap-race'",
            "access_points",
        ),
        (
            "SELECT COUNT(*) FROM inventory_configs WHERE bucket = 'race'",
            "inventory_configs",
        ),
    ] {
        let n: i64 = sqlx::query_scalar(sql)
            .fetch_one(st.db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "{label} 不得留下悬挂行");
    }
}
