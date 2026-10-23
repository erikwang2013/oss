//! M9 S6c：桶控制写路由（仅 Raft）。每条路由写 = 恰好一条控制命令；
//! M7（无 sink）直写路径保持不变。用真实单节点 Raft + 真实 SQLite 控制库。

mod common;

use std::sync::Arc;
use std::time::Duration;

use oss_server::config::AppConfig;
use oss_server::db::{Db, SitePeer};
use oss_server::inventory_store::SqlInventoryStore;
use oss_server::lifecycle_store::SqlLifecycleStore;
use oss_server::raft::control::ControlPlane;
use oss_server::raft::{RaftHandle, RaftSettings};
use oss_server::replication::SqlReplStore;
use oss_server::state::AppState;
use oss_server::sync::SiteSyncDelete;
use s3s::dto::*;
use s3s::{S3, S3ErrorCode, S3Request};
use sqlx::Row as _;
use xl_s3::access::AuthedUser;
use xl_s3::lifecycle::{ExpirationAction, LifecycleRuleSpec, LifecycleStore};
use xl_s3::replication::ReplicationStore;
use xl_s3::service::OssS3;
use xl_s3::sync_delete::SyncDeleteSink;
use xl_store::store::{Store, StoreConfig};

const JWT_SECRET: &str = common::JWT_SECRET;

struct H {
    state: AppState,
    handle: Arc<RaftHandle>,
    dir: tempfile::TempDir,
    base: String,
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: axum::http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn as_user<T>(mut r: S3Request<T>, uid: i64, is_admin: bool) -> S3Request<T> {
    r.extensions.insert(AuthedUser {
        uid,
        is_admin,
        ak: String::new(),
    });
    r
}

async fn wait_leader(h: &RaftHandle) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = {
            let m = h.raft().metrics();
            let m = m.borrow();
            m.current_leader
        };
        if leader.is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader election timed out"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn applied(h: &H) -> u64 {
    let m = h.handle.raft().metrics();
    let m = m.borrow();
    m.last_applied.as_ref().map(|l| l.index).unwrap_or(0)
}

/// `client_write` 返回时 metrics 的 last_applied 可能尚未刷新——先等到目标 index。
async fn assert_applied(h: &H, target: u64) {
    assert!(
        h.handle.wait_applied(target, Duration::from_secs(10)).await,
        "Raft apply 超时（期望 {target}，实际 {}）",
        applied(h)
    );
    assert_eq!(applied(h), target, "控制命令条数不符");
}

/// 反向断言：等一个稳定窗口，确认没有追加命令（metrics 滞后不会造成假通过）。
async fn assert_no_write(h: &H, before: u64) {
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(applied(h), before, "不应追加控制命令");
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn setup() -> H {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}?mode=rwc", dir.path().join("m9.db").display());
    let cfg = AppConfig {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.path().join("data"),
        data_dirs: None,
        ec: Default::default(),
        staging_dir: dir.path().join("staging"),
        database_url: db_url.clone(),
        jwt_secret: JWT_SECRET.into(),
        max_put_bytes: 1024 * 1024,
        groups: vec![],
        cluster: None,
        kafka: Default::default(),
        remotes: vec![],
        tiers: vec![],
        site: None,
        // 关键：cfg.raft=None 让 console 的 S0 503 门放行；控制面由 state.control 强制为 Raft。
        raft: None,
        sync: Default::default(),
        stretch: None,
        tls: Default::default(),
    };
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let store = Store::open(StoreConfig::new(&cfg.data_dir)).await.unwrap();
    let db = Db::connect_raft(&db_url).await.unwrap();
    let st = AppState::new(cfg, db, store).await;

    let settings = RaftSettings {
        node_id: 1,
        role: oss_server::config::RaftRole::Voter,
        bootstrap: true,
        advertise_endpoint: "http://127.0.0.1:1".to_string(),
        cluster_token: "s6c-test".to_string(),
        db_path: dir.path().join("raft.db"),
        election_timeout_ms: 2_000,
        heartbeat_interval_ms: 200,
        install_snapshot_timeout_ms: 5_000,
        max_in_snapshot_log_to_keep: 0,
        purge_batch_size: 1024,
        write_timeout_ms: 20_000,
        snapshot_max_chunk_size: 4 * 1024 * 1024,
        snapshot_max_bytes: 512 * 1024 * 1024,
        feature_level: 1,
    };
    let handle = Arc::new(
        oss_server::raft::spawn_with_control(settings, Some(st.db.pool().clone()), None)
            .await
            .unwrap(),
    );
    wait_leader(&handle).await;

    let control = Arc::new(ControlPlane::raft(
        Arc::clone(&handle),
        Arc::clone(&st.db),
        Arc::clone(&st.store),
        Arc::clone(&st.ap_cache),
        Arc::clone(&st.olap_cache),
        st.ap_region.clone(),
    ));
    let sync_delete = Arc::new(
        SiteSyncDelete::new(
            Arc::clone(&st.db),
            Arc::clone(&st.store),
            Duration::from_secs(5),
            true,
        )
        .with_control(Arc::clone(&control)),
    );
    let repl_store = Arc::new(
        SqlReplStore::new(st.db.pool().clone(), vec!["remote-a".into()])
            .with_control(Arc::clone(&control)),
    );
    let lifecycle: Arc<dyn LifecycleStore> = Arc::new(
        SqlLifecycleStore::new(Arc::clone(&st.db), Arc::new(vec![]))
            .with_control(Arc::clone(&control)),
    );
    let inventory: Arc<dyn xl_s3::inventory::InventoryStore> = Arc::new(
        SqlInventoryStore::new(Arc::clone(&st.db), st.acl()).with_control(Arc::clone(&control)),
    );
    let state = AppState {
        control,
        sync_delete,
        repl_store,
        lifecycle,
        inventory,
        ..st
    };
    let base = serve(oss_server::router::build_router(state.clone()).unwrap()).await;
    // 管理员登录种子（admin 行不受 guard 约束）。
    state.db.create_user("erik", "pw", true).await.unwrap();
    H {
        state,
        handle,
        dir,
        base,
    }
}

async fn shutdown(h: H) {
    h.handle.shutdown().await;
}

fn s3(h: &H) -> OssS3 {
    OssS3::new(Arc::clone(&h.state.store), h.state.acl(), 1024 * 1024)
        .with_sync_delete(Some(
            Arc::clone(&h.state.sync_delete) as Arc<dyn SyncDeleteSink>
        ))
        .with_lifecycle(Some(Arc::clone(&h.state.lifecycle)))
        .with_bucket_control(h.state.control.bucket_sink())
}

fn bucket_create_cmd(name: &str, mode: &str, versioning: &str) -> oss_server::raft::ControlCommand {
    oss_server::raft::ControlCommand::BucketCreate {
        name: name.into(),
        owner: None,
        created_at: 1,
        replication_mode: mode.into(),
        versioning: versioning.into(),
        object_lock: false,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        default_encryption: None,
    }
}

/// 经控制面建桶并断言恰好一条命令。
async fn create_bucket(h: &H, name: &str, mode: &str, versioning: &str) {
    let before = applied(h);
    let r = h
        .state
        .control
        .write(bucket_create_cmd(name, mode, versioning))
        .await
        .unwrap();
    assert_eq!(r, oss_server::raft::ControlResponse::Ok);
    assert_applied(h, before + 1).await;
    assert!(h.state.store.bucket_exists(name).await.unwrap());
}

struct BucketRow {
    owner: Option<i64>,
    versioning: String,
    object_lock: i64,
    deleting: i64,
    encryption: Option<String>,
    lock_mode: Option<String>,
    lock_days: Option<i64>,
}

async fn bucket_row(h: &H, name: &str) -> Option<BucketRow> {
    sqlx::query(
        "SELECT owner_id, versioning, object_lock, deleting, replication_mode, \
         default_encryption, object_lock_default_mode, object_lock_default_days \
         FROM buckets WHERE name = ?",
    )
    .bind(name)
    .fetch_optional(h.state.db.pool())
    .await
    .unwrap()
    .map(|r| BucketRow {
        owner: r.get("owner_id"),
        versioning: r.get("versioning"),
        object_lock: r.get("object_lock"),
        deleting: r.get("deleting"),
        encryption: r.get("default_encryption"),
        lock_mode: r.get("object_lock_default_mode"),
        lock_days: r.get("object_lock_default_days"),
    })
}

macro_rules! count {
    ($h:expr, $tbl:literal, $bucket:expr) => {
        sqlx::query_scalar::<_, i64>(concat!("SELECT COUNT(*) FROM ", $tbl, " WHERE bucket = ?"))
            .bind($bucket)
            .fetch_one($h.state.db.pool())
            .await
            .unwrap()
    };
}

fn lock_cfg() -> ObjectLockConfiguration {
    ObjectLockConfiguration {
        object_lock_enabled: Some(ObjectLockEnabled::from_static("Enabled")),
        rule: Some(ObjectLockRule {
            default_retention: Some(DefaultRetention {
                mode: Some(ObjectLockRetentionMode::from_static("GOVERNANCE")),
                days: Some(1),
                years: None,
            }),
        }),
    }
}

fn notif_cfg() -> NotificationConfiguration {
    NotificationConfiguration {
        queue_configurations: Some(QueueConfigurationList::from(vec![QueueConfiguration {
            id: Some(NotificationId::from("cfg-0".to_string())),
            queue_arn: QueueArn::from("arn:oss:sqs::t1:kafka".to_string()),
            events: EventList::from(vec![Event::from("s3:ObjectCreated:Put".to_string())]),
            filter: None,
        }])),
        ..Default::default()
    }
}

fn enc_input(bucket: &str) -> PutBucketEncryptionInput {
    PutBucketEncryptionInput {
        bucket: bucket.into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        server_side_encryption_configuration: ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    kms_master_key_id: None,
                    sse_algorithm: ServerSideEncryption::from_static("AES256"),
                }),
                ..Default::default()
            }],
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_create_is_one_command_with_409_precheck() {
    let h = setup().await;
    // 非管理员属主：经控制命令创建（users 非 admin 行受 guard，直写会 ABORT）。
    let alice = h
        .state
        .control
        .write(oss_server::raft::ControlCommand::UserCreate {
            username: "alice".into(),
            password_hash: "hash".into(),
            status: "active".into(),
            created_at: 1,
        })
        .await
        .unwrap();
    assert_eq!(alice, oss_server::raft::ControlResponse::Ok);
    let alice_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'alice'")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();

    let svc = s3(&h);
    let before = applied(&h);
    svc.create_bucket(as_user(
        req(CreateBucketInput {
            bucket: "alpha".into(),
            object_lock_enabled_for_bucket: Some(true),
            ..Default::default()
        }),
        alice_id,
        false,
    ))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    let row = bucket_row(&h, "alpha").await.unwrap();
    assert_eq!(row.owner, Some(alice_id), "属主 uid 必须映射回本地行");
    assert_eq!(row.versioning, "Enabled", "开锁建桶顺带 Enabled");
    assert_eq!(row.object_lock, 1);
    assert!(h.state.store.bucket_exists("alpha").await.unwrap());

    // 重复建桶：行预检 409，零命令。
    let before = applied(&h);
    let e = svc
        .create_bucket(as_user(
            req(CreateBucketInput {
                bucket: "alpha".into(),
                ..Default::default()
            }),
            alice_id,
            false,
        ))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::BucketAlreadyExists);
    assert_no_write(&h, before).await;

    // 管理员建桶：owner_id NULL。
    svc.create_bucket(as_user(
        req(CreateBucketInput {
            bucket: "beta".into(),
            ..Default::default()
        }),
        1,
        true,
    ))
    .await
    .unwrap();
    assert_eq!(bucket_row(&h, "beta").await.unwrap().owner, None);

    // guard 仍在：绕过控制面的直写必须 ABORT。
    let err = sqlx::query("INSERT INTO buckets (name, creation_date) VALUES ('direct', 0)")
        .execute(h.state.db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("control write outside apply"),
        "guard 未生效: {err}"
    );
    shutdown(h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_bucket_writes_each_one_command() {
    let h = setup().await;
    create_bucket(&h, "wbk", "async", "Off").await;
    let svc = s3(&h);

    // versioning
    let before = applied(&h);
    svc.put_bucket_versioning(req(PutBucketVersioningInput {
        bucket: "wbk".into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        mfa: None,
        versioning_configuration: VersioningConfiguration {
            mfa_delete: None,
            status: Some(BucketVersioningStatus::from_static("Enabled")),
        },
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    let row = bucket_row(&h, "wbk").await.unwrap();
    assert_eq!(row.versioning, "Enabled");
    assert_eq!(row.object_lock, 0, "versioning 不得顺带开锁");

    // object lock（versioning 已 Enabled → 一条命令写 lock + 规则）
    let before = applied(&h);
    svc.put_object_lock_configuration(req(PutObjectLockConfigurationInput {
        bucket: "wbk".into(),
        object_lock_configuration: Some(lock_cfg()),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    let row = bucket_row(&h, "wbk").await.unwrap();
    assert_eq!(row.object_lock, 1);
    assert_eq!(row.lock_mode.as_deref(), Some("GOVERNANCE"));
    assert_eq!(row.lock_days, Some(1));

    // notification set / clear
    let before = applied(&h);
    svc.put_bucket_notification_configuration(req(
        PutBucketNotificationConfigurationInput::builder()
            .bucket("wbk".to_string())
            .notification_configuration(notif_cfg())
            .build()
            .unwrap(),
    ))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "event_configs", "wbk"), 1);
    let before = applied(&h);
    svc.put_bucket_notification_configuration(req(
        PutBucketNotificationConfigurationInput::builder()
            .bucket("wbk".to_string())
            .notification_configuration(NotificationConfiguration::default())
            .build()
            .unwrap(),
    ))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "event_configs", "wbk"), 0, "空配置 = 清空");

    // encryption set / delete
    let before = applied(&h);
    svc.put_bucket_encryption(req(enc_input("wbk")))
        .await
        .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(
        bucket_row(&h, "wbk").await.unwrap().encryption.as_deref(),
        Some("AES256")
    );
    let before = applied(&h);
    svc.delete_bucket_encryption(req(DeleteBucketEncryptionInput {
        bucket: "wbk".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(bucket_row(&h, "wbk").await.unwrap().encryption, None);
    shutdown(h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_bucket_cascades_union_in_one_command() {
    let h = setup().await;
    create_bucket(&h, "dbk", "async", "Off").await;
    // 每张级联表各播一行（全部经控制命令，guard 生效）。
    use oss_server::raft::ControlCommand as C;
    let seed = [
        // S6a 修复后管理员 IAM 写被拒；权限级联用普通用户验证。
        C::UserCreate {
            username: "bob".into(),
            password_hash: "hash".into(),
            status: "active".into(),
            created_at: 1,
        },
        C::BucketSetLifecycle {
            bucket: "dbk".into(),
            rules: vec![oss_server::raft::LifecycleRuleCmd {
                rule_id: Some("r1".into()),
                prefix: String::new(),
                expire_days: 1,
                expiration_days: Some(1),
                transition_days: None,
                transition_class: None,
                transition_tier: None,
                enabled: true,
            }],
            updated_at: 1,
        },
        C::BucketSetPolicy {
            bucket: "dbk".into(),
            policy_json: "{\"Version\":\"2012-10-17\",\"Statement\":[]}".into(),
            updated_at: 1,
        },
        C::BucketSetNotification {
            bucket: "dbk".into(),
            config_json: "[]".into(),
            updated_at: 1,
        },
        C::BucketSetReplication {
            bucket: "dbk".into(),
            rules_json: "[]".into(),
            updated_at: 1,
        },
        C::InventoryPut {
            bucket: "dbk".into(),
            id: "inv".into(),
            owner: None,
            config_json: "{}".into(),
            updated_at: 1,
        },
        C::AccessPointPut {
            name: "ap-d".into(),
            bucket: "dbk".into(),
            owner: None,
            policy_json: None,
            created_at: 1,
            updated_at: 1,
        },
        C::OlapPut {
            access_point: "ap-d".into(),
            kind: "http".into(),
            endpoint: Some("http://127.0.0.1:1/olap".into()),
            module: None,
            module_sha256: None,
            timeout_ms: 1000,
            max_response_bytes: 1024,
            updated_at: 1,
        },
        C::UserSetPermissions {
            username: "bob".into(),
            permissions: vec![oss_server::raft::PermissionBits {
                bucket: "dbk".into(),
                read: true,
                write: false,
                delete: false,
            }],
        },
    ];
    for cmd in seed {
        assert_eq!(
            h.state.control.write(cmd).await.unwrap(),
            oss_server::raft::ControlResponse::Ok
        );
    }
    assert_eq!(count!(h, "lifecycle_rules", "dbk"), 1);
    assert_eq!(count!(h, "bucket_policies", "dbk"), 1);
    assert_eq!(count!(h, "event_configs", "dbk"), 1);
    assert_eq!(count!(h, "replication_rules", "dbk"), 1);
    assert_eq!(count!(h, "inventory_configs", "dbk"), 1);
    assert_eq!(count!(h, "permissions", "dbk"), 1);
    assert_eq!(count!(h, "access_points", "dbk"), 1);
    let olap: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM olap_configs")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    assert_eq!(olap, 1);

    let svc = s3(&h);
    let before = applied(&h);
    svc.delete_bucket(req(DeleteBucketInput {
        bucket: "dbk".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "lifecycle_rules", "dbk"), 0, "lifecycle 未级联");
    assert_eq!(count!(h, "bucket_policies", "dbk"), 0, "policy 未级联");
    assert_eq!(count!(h, "event_configs", "dbk"), 0, "notification 未级联");
    assert_eq!(
        count!(h, "replication_rules", "dbk"),
        0,
        "replication 未级联"
    );
    assert_eq!(count!(h, "inventory_configs", "dbk"), 0, "inventory 未级联");
    assert_eq!(count!(h, "permissions", "dbk"), 0, "permissions 未级联");
    assert_eq!(count!(h, "access_points", "dbk"), 0, "access_points 未级联");
    let olap: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM olap_configs")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    assert_eq!(olap, 0, "olap_configs 未随 AP 级联");
    assert!(bucket_row(&h, "dbk").await.is_none());
    assert!(!h.state.store.bucket_exists("dbk").await.unwrap());
    shutdown(h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_delete_rolls_back_and_keeps_bucket_on_peer_failure() {
    let h = setup().await;
    create_bucket(&h, "syncb", "sync", "Enabled").await;
    h.state
        .db
        .insert_site(&SitePeer {
            peer_id: "site".into(),
            remote_site_id: "remote".into(),
            name: "peer".into(),
            endpoint: "http://127.0.0.1:1".into(),
            region: "us-east-1".into(),
            priority: 1,
            s3_access_key: "k".into(),
            s3_secret_key: "s".into(),
            peer_site_token: "tok".into(),
            created_at: 0,
            last_seen: None,
            last_error: None,
            raft_node_id: None,
            reconcile_required: false,
        })
        .await
        .unwrap();

    let svc = s3(&h);
    let before = applied(&h);
    let err = svc
        .delete_bucket(req(DeleteBucketInput {
            bucket: "syncb".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    // SetBucketDeleting(true) + 回滚 SetBucketDeleting(false)。
    assert_applied(&h, before + 2).await;
    let row = bucket_row(&h, "syncb").await.unwrap();
    assert_eq!(row.deleting, 0, "回滚要清 deleting");
    assert!(
        h.state.store.bucket_exists("syncb").await.unwrap(),
        "桶必须保留"
    );
    let intents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_delete_intents")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    assert_eq!(intents, 0, "失败回滚后 intent 必须清理");
    shutdown(h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_delete_success_tombstones_physical_dir() {
    let h = setup().await;
    let fake = axum::Router::new()
        .route(
            "/api/v1/site/bucket-empty",
            axum::routing::get(|| async { axum::http::StatusCode::NO_CONTENT }),
        )
        .route(
            "/api/v1/site/bucket-quiesce",
            axum::routing::post(|_: axum::Json<serde_json::Value>| async {
                axum::http::StatusCode::OK
            }),
        )
        .route(
            "/api/v1/site/delete-bucket",
            axum::routing::post(|_: axum::Json<serde_json::Value>| async {
                axum::http::StatusCode::NO_CONTENT
            }),
        );
    let endpoint = serve(fake).await;
    create_bucket(&h, "syncb", "sync", "Enabled").await;
    h.state
        .db
        .insert_site(&SitePeer {
            peer_id: "site".into(),
            remote_site_id: "remote".into(),
            name: "peer".into(),
            endpoint,
            region: "us-east-1".into(),
            priority: 1,
            s3_access_key: "k".into(),
            s3_secret_key: "s".into(),
            peer_site_token: "tok".into(),
            created_at: 0,
            last_seen: None,
            last_error: None,
            raft_node_id: None,
            reconcile_required: false,
        })
        .await
        .unwrap();

    let svc = s3(&h);
    let before = applied(&h);
    svc.delete_bucket(req(DeleteBucketInput {
        bucket: "syncb".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // SetBucketDeleting(true) + BucketDelete。
    assert_applied(&h, before + 2).await;
    assert!(bucket_row(&h, "syncb").await.is_none());
    assert!(!h.state.store.bucket_exists("syncb").await.unwrap());
    // sync 删桶的物理副作用是 tombstone：rename 进 `.oss.sys/trash/<bucket>/<uuid>`
    // 后尽力立即清；桶目录本身必须已消失（与 M7 共用 store::delete_bucket_sync）。
    let trash = h
        .dir
        .path()
        .join("data")
        .join(".oss.sys")
        .join("trash")
        .join("syncb");
    assert!(
        trash.is_dir(),
        "tombstone 目录未落到 trash: {}",
        trash.display()
    );
    assert_eq!(
        std::fs::read_dir(&trash).unwrap().count(),
        0,
        "tombstone 应已尽力清理（留空目录待下轮）"
    );
    let intents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_delete_intents")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    assert_eq!(intents, 0);
    shutdown(h).await;
}

async fn login(base: &str) -> String {
    let v: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["token"].as_str().unwrap().to_string()
}

async fn console(
    h: &H,
    method: reqwest::Method,
    path: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> reqwest::Response {
    let mut r = reqwest::Client::new()
        .request(method, format!("{}{path}", h.base))
        .bearer_auth(token);
    if let Some(b) = body {
        r = r.json(&b);
    }
    r.send().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_bucket_writes_route_one_command_each() {
    let h = setup().await;
    let token = login(&h.base).await;

    // create（204）：一条命令；重复 409 零命令。
    let before = applied(&h);
    let resp = console(
        &h,
        reqwest::Method::POST,
        "/api/v1/buckets",
        &token,
        Some(serde_json::json!({"name": "cbk"})),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_applied(&h, before + 1).await;
    assert!(bucket_row(&h, "cbk").await.is_some());
    let before = applied(&h);
    let resp = console(
        &h,
        reqwest::Method::POST,
        "/api/v1/buckets",
        &token,
        Some(serde_json::json!({"name": "cbk"})),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    assert_no_write(&h, before).await;

    // policy / notifications / encryption：各一条命令。
    let before = applied(&h);
    let resp = console(
        &h,
        reqwest::Method::PUT,
        "/api/v1/buckets/cbk/policy",
        &token,
        Some(serde_json::json!({"Version": "2012-10-17", "Statement": []})),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "bucket_policies", "cbk"), 1);

    let before = applied(&h);
    let resp = console(
        &h,
        reqwest::Method::PUT,
        "/api/v1/buckets/cbk/notifications",
        &token,
        Some(serde_json::json!({"queue_configurations": [{
            "id": "cfg-0",
            "queue_arn": "arn:oss:sqs::t1:kafka",
            "events": ["s3:ObjectCreated:Put"]
        }]})),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "event_configs", "cbk"), 1);

    let before = applied(&h);
    let resp = console(
        &h,
        reqwest::Method::PUT,
        "/api/v1/buckets/cbk/encryption",
        &token,
        Some(serde_json::json!({"algorithm": "AES256"})),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_applied(&h, before + 1).await;
    assert_eq!(
        bucket_row(&h, "cbk").await.unwrap().encryption.as_deref(),
        Some("AES256")
    );

    let before = applied(&h);
    let resp = console(
        &h,
        reqwest::Method::DELETE,
        "/api/v1/buckets/cbk/encryption",
        &token,
        None,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_applied(&h, before + 1).await;
    assert_eq!(bucket_row(&h, "cbk").await.unwrap().encryption, None);

    // delete（204）：一条 BucketDelete，级联清表 + 物理目录。
    let before = applied(&h);
    let resp = console(
        &h,
        reqwest::Method::DELETE,
        "/api/v1/buckets/cbk",
        &token,
        None,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert_applied(&h, before + 1).await;
    assert!(bucket_row(&h, "cbk").await.is_none());
    assert_eq!(count!(h, "bucket_policies", "cbk"), 0);
    assert_eq!(count!(h, "event_configs", "cbk"), 0);
    assert!(!h.state.store.bucket_exists("cbk").await.unwrap());
    shutdown(h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stores_route_control_writes_in_raft() {
    let h = setup().await;
    create_bucket(&h, "shbk", "async", "Off").await;
    create_bucket(&h, "dest", "async", "Off").await;

    // lifecycle put/delete
    let spec = LifecycleRuleSpec {
        rule_id: Some("r1".into()),
        filter: Default::default(),
        expiration: Some(ExpirationAction::Days { days: 1 }),
        transitions: vec![],
        noncurrent_expiration: None,
        noncurrent_transitions: vec![],
        enabled: true,
    };
    let before = applied(&h);
    h.state.lifecycle.put_rules("shbk", &[spec]).await.unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "lifecycle_rules", "shbk"), 1);
    let before = applied(&h);
    h.state.lifecycle.delete_rules("shbk").await.unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "lifecycle_rules", "shbk"), 0);

    // replication save/remove
    let before = applied(&h);
    h.state.repl_store.save_rules("shbk", "[]").await.unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "replication_rules", "shbk"), 1);
    let before = applied(&h);
    h.state.repl_store.remove_rules("shbk").await.unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "replication_rules", "shbk"), 0);

    // inventory put/delete（admin owner → NULL）
    let cfg = InventoryConfiguration {
        destination: InventoryDestination {
            s3_bucket_destination: InventoryS3BucketDestination {
                account_id: None,
                bucket: "dest".into(),
                encryption: None,
                format: InventoryFormat::from_static(InventoryFormat::CSV),
                prefix: None,
            },
        },
        filter: None,
        id: "inv1".into(),
        included_object_versions: InventoryIncludedObjectVersions::from_static(
            InventoryIncludedObjectVersions::CURRENT,
        ),
        is_enabled: true,
        optional_fields: None,
        schedule: InventorySchedule {
            frequency: InventoryFrequency::from_static(InventoryFrequency::DAILY),
        },
    };
    let admin = AuthedUser {
        uid: 1,
        is_admin: true,
        ak: String::new(),
    };
    let before = applied(&h);
    h.state
        .inventory
        .put_config("shbk", &admin, &cfg)
        .await
        .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "inventory_configs", "shbk"), 1);
    let before = applied(&h);
    h.state
        .inventory
        .delete_config("shbk", "inv1")
        .await
        .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "inventory_configs", "shbk"), 0);
    shutdown(h).await;
}

/// Level B：feature_level=1 时结构化生命周期 PUT 必须 503（带 required/effective 详情），
/// 且在 append 前拒绝（不追加控制命令、不落行）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_lifecycle_put_at_feature_level_one_is_503() {
    let h = setup().await;
    create_bucket(&h, "lvl1", "async", "Off").await;
    let before = applied(&h);
    let err = s3(&h)
        .put_bucket_lifecycle_configuration(req(PutBucketLifecycleConfigurationInput {
            bucket: "lvl1".into(),
            lifecycle_configuration: Some(BucketLifecycleConfiguration {
                rules: vec![LifecycleRule {
                    abort_incomplete_multipart_upload: None,
                    expiration: Some(LifecycleExpiration {
                        days: Some(30),
                        ..Default::default()
                    }),
                    // Tag filter 无法用旧列表达 → 产生 config_json → V2（level 2）。
                    filter: Some(LifecycleRuleFilter {
                        tag: Some(Tag {
                            key: Some("env".into()),
                            value: Some("prod".into()),
                        }),
                        ..Default::default()
                    }),
                    id: Some("tagged".into()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    transitions: None,
                }],
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(
        err.status_code(),
        Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
    );
    let msg = err.message().unwrap_or_default();
    assert!(
        msg.contains("feature level 2") && msg.contains("effective 1"),
        "503 detail missing required/effective: {msg}"
    );
    assert_no_write(&h, before).await;
    assert_eq!(count!(h, "lifecycle_rules", "lvl1"), 0);
    shutdown(h).await;
}

#[tokio::test]
async fn m7_without_sink_keeps_direct_store_path() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert!(!st.control.is_raft(), "M7/standalone 必须仍是 Local");
    // buckets.owner_id 有 FK：先落一个真实用户（普通直写路径）。
    let uid = st.db.create_user("m7user", "pw", false).await.unwrap();
    // 无 sink = M7 路径：建桶直写 store + 行，删桶直写清理。
    let svc = OssS3::new(Arc::clone(&st.store), st.acl(), 1024 * 1024);
    svc.create_bucket(as_user(
        req(CreateBucketInput {
            bucket: "m7b".into(),
            ..Default::default()
        }),
        uid,
        false,
    ))
    .await
    .unwrap();
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name = 'm7b'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(exists, 1);
    svc.delete_bucket(req(DeleteBucketInput {
        bucket: "m7b".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name = 'm7b'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(exists, 0);
    assert!(!st.store.bucket_exists("m7b").await.unwrap());
}
