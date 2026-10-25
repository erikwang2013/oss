//! M9 S6c 修复轮：启动对账（I-1）、物理副作用失败修复、sync 删桶回滚（非空/对端
//! quiesce 超时）、S3 共享存储/策略/本地 M8 清理的路由，以及真实 `[raft]` 装配。

mod common;

use std::sync::Arc;
use std::time::Duration;

use oss_server::config::AppConfig;
use oss_server::db::{Db, SitePeer};
use oss_server::inventory_store::SqlInventoryStore;
use oss_server::lifecycle_store::SqlLifecycleStore;
use oss_server::raft::control::ControlPlane;
use oss_server::raft::{ControlCommand, RaftHandle, RaftSettings};
use oss_server::replication::SqlReplStore;
use oss_server::state::AppState;
use oss_server::sync::SiteSyncDelete;
use s3s::dto::*;
use s3s::{S3, S3ErrorCode, S3Request};
use xl_s3::access::AuthedUser;
use xl_s3::bucket_control::{BucketControlError, BucketCreateSpec};
use xl_s3::inventory::InventoryStore;
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

async fn assert_applied(h: &H, target: u64) {
    assert!(
        h.handle.wait_applied(target, Duration::from_secs(10)).await,
        "Raft apply 超时（期望 {target}，实际 {}）",
        applied(h)
    );
    assert_eq!(applied(h), target, "控制命令条数不符");
}

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
        maintenance: false,
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
        raft: None,
        sync: Default::default(),
        stretch: None,
        tls: Default::default(),
        sts: Default::default(),
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
        cluster_token: "s6c-fix".to_string(),
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
    let inventory: Arc<dyn InventoryStore> = Arc::new(
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
    state.db.create_user("erik", "pw", true).await.unwrap();
    H { state, handle, dir }
}

async fn shutdown(h: H) {
    h.handle.shutdown().await;
}

fn s3(h: &H) -> OssS3 {
    OssS3::new(Arc::clone(&h.state.store), h.state.acl(), 1024 * 1024)
        .with_sync_delete(Some(
            Arc::clone(&h.state.sync_delete) as Arc<dyn SyncDeleteSink>
        ))
        .with_bucket_control(h.state.control.bucket_sink())
}

fn s3_full(h: &H) -> OssS3 {
    s3(h)
        .with_lifecycle(Some(Arc::clone(&h.state.lifecycle)))
        .with_replication(
            None,
            Some(Arc::clone(&h.state.repl_store) as Arc<dyn ReplicationStore>),
        )
        .with_inventory(Some(Arc::clone(&h.state.inventory)))
}

fn bucket_create_spec(name: &str, mode: &str, versioning: &str) -> BucketCreateSpec {
    BucketCreateSpec {
        name: name.into(),
        owner_uid: None,
        created_at: 1,
        replication_mode: mode.into(),
        versioning: versioning.into(),
        object_lock: false,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        default_encryption: None,
    }
}

async fn create_bucket(h: &H, name: &str, mode: &str, versioning: &str) {
    let before = applied(h);
    let r = h
        .state
        .control
        .write(ControlCommand::BucketCreate {
            name: name.into(),
            owner: None,
            created_at: 1,
            replication_mode: mode.into(),
            versioning: versioning.into(),
            object_lock: false,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            default_encryption: None,
        })
        .await
        .unwrap();
    assert_eq!(r, oss_server::raft::ControlResponse::Ok);
    assert_applied(h, before + 1).await;
}

async fn reconcile_now(h: &H) -> Vec<String> {
    let rows = h.state.db.list_bucket_rows().await.unwrap();
    h.state.store.reconcile_bucket_dirs(&rows).await;
    rows
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

async fn insert_peer(h: &H, endpoint: String) {
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
}

/// I-1/EX-1：行 → 目录补建不追加日志；无行空目录删除、非空目录改名 orphan；幂等。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconcile_repairs_rows_and_orphans_without_log_entry() {
    let h = setup().await;
    create_bucket(&h, "rbk", "async", "Off").await;
    let data = h.dir.path().join("data");
    assert!(data.join("rbk").is_dir());

    // apply 与 mkdir 之间崩溃：行在、目录不在 → 重启对账补建（零日志）。
    tokio::fs::remove_dir_all(data.join("rbk")).await.unwrap();
    let before = applied(&h);
    let rows = reconcile_now(&h).await;
    assert!(rows.iter().any(|r| r == "rbk"));
    assert!(data.join("rbk").is_dir(), "对账必须补建行对应目录");
    assert_no_write(&h, before).await;

    // 幂等：再跑一次无变化、无日志。
    reconcile_now(&h).await;
    assert_no_write(&h, before).await;

    // 无行空目录：删除。
    std::fs::create_dir(data.join("junkempty")).unwrap();
    // 无行非空目录：绝不删除，改名进 .oss.sys/orphan/<name>.<uuid>。
    std::fs::create_dir(data.join("junkfull")).unwrap();
    std::fs::write(data.join("junkfull/keep"), b"x").unwrap();
    reconcile_now(&h).await;
    assert!(!data.join("junkempty").exists(), "空孤儿目录应删除");
    assert!(!data.join("junkfull").exists(), "非空孤儿目录应移走");
    let orphan = data.join(".oss.sys/orphan");
    let moved: Vec<_> = std::fs::read_dir(&orphan)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("junkfull."))
        .collect();
    assert_eq!(moved.len(), 1, "非空孤儿目录应恰好移入一次: {moved:?}");
    assert!(orphan.join(&moved[0]).join("keep").is_file());

    // 幂等：已移走的 orphan 不再产生第二份。
    reconcile_now(&h).await;
    let again = std::fs::read_dir(&orphan)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("junkfull.")
        })
        .count();
    assert_eq!(again, 1);
    assert_no_write(&h, before).await;
    shutdown(h).await;
}

/// 提交成功但物理副作用失败（盘不可用）→ 命令仍成功；盘恢复后对账修复（幂等）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_commit_physical_failure_is_repaired_by_reconcile() {
    let h = setup().await;
    create_bucket(&h, "pfk", "async", "Off").await;
    let data = h.dir.path().join("data");
    let backup = h.dir.path().join("data.bak");
    tokio::fs::rename(&data, &backup).await.unwrap();
    tokio::fs::write(&data, b"not a dir").await.unwrap();

    let before = applied(&h);
    let sink = h.state.control.bucket_sink().unwrap();
    sink.create_bucket(bucket_create_spec("pfk2", "async", "Off"))
        .await
        .unwrap();
    assert_applied(&h, before + 1).await;
    assert!(!data.join("pfk2").exists(), "盘不可用时不建目录");

    tokio::fs::remove_file(&data).await.unwrap();
    tokio::fs::rename(&backup, &data).await.unwrap();
    reconcile_now(&h).await;
    assert!(data.join("pfk2").is_dir(), "盘恢复后对账必须补建目录");
    assert_no_write(&h, before + 1).await;
    shutdown(h).await;
}

/// sync 删桶：本地非空 → 回滚 deleting、保留桶与 intent 清理、409。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_delete_nonempty_rolls_back() {
    let h = setup().await;
    create_bucket(&h, "snbk", "sync", "Enabled").await;
    let fake = axum::Router::new()
        .route(
            "/api/v1/site/bucket-quiesce",
            axum::routing::post(|| async { axum::http::StatusCode::OK }),
        )
        .route(
            "/api/v1/site/bucket-empty",
            axum::routing::get(|| async { axum::http::StatusCode::NO_CONTENT }),
        )
        .route(
            "/api/v1/site/delete-bucket",
            axum::routing::post(|| async { axum::http::StatusCode::NO_CONTENT }),
        );
    insert_peer(&h, serve(fake).await).await;
    // 本地残留对象 → 两站查空阶段拒绝（远端已 quiesce 后回滚）。
    std::fs::write(h.dir.path().join("data/snbk/junk"), b"x").unwrap();

    let before = applied(&h);
    let err = s3(&h)
        .delete_bucket(req(DeleteBucketInput {
            bucket: "snbk".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::BucketNotEmpty);
    // SetBucketDeleting(true) + 回滚 SetBucketDeleting(false)。
    assert_applied(&h, before + 2).await;
    let deleting: i64 = sqlx::query_scalar("SELECT deleting FROM buckets WHERE name = 'snbk'")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    assert_eq!(deleting, 0, "回滚要清 deleting");
    assert!(h.state.store.bucket_exists("snbk").await.unwrap());
    assert!(h.dir.path().join("data/snbk/junk").exists());
    let intents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_delete_intents")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    assert_eq!(intents, 0, "失败回滚后 intent 必须清理");
    shutdown(h).await;
}

/// sync 删桶：对端 quiesce 超时（35s 上限）→ 503 回滚，deleting/intent 清理。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_delete_quiesce_timeout_rolls_back() {
    let h = setup().await;
    create_bucket(&h, "sqtb", "sync", "Enabled").await;
    let fake = axum::Router::new().route(
        "/api/v1/site/bucket-quiesce",
        axum::routing::post(|axum::Json(v): axum::Json<serde_json::Value>| async move {
            let release = v.get("release").and_then(|r| r.as_bool()).unwrap_or(false);
            if !release {
                // 超过 QUIESCE_CALL_TIMEOUT(35s)：客户端必须超时回滚。
                tokio::time::sleep(Duration::from_secs(40)).await;
            }
            axum::http::StatusCode::OK
        }),
    );
    insert_peer(&h, serve(fake).await).await;

    let before = applied(&h);
    let err = s3(&h)
        .delete_bucket(req(DeleteBucketInput {
            bucket: "sqtb".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    assert_applied(&h, before + 2).await;
    let deleting: i64 = sqlx::query_scalar("SELECT deleting FROM buckets WHERE name = 'sqtb'")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    assert_eq!(deleting, 0);
    assert!(h.state.store.bucket_exists("sqtb").await.unwrap());
    let intents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_delete_intents")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    assert_eq!(intents, 0);
    shutdown(h).await;
}

fn lifecycle_rule(id: &str) -> LifecycleRule {
    LifecycleRule {
        abort_incomplete_multipart_upload: None,
        expiration: Some(LifecycleExpiration {
            date: None,
            days: Some(1),
            expired_object_delete_marker: None,
        }),
        filter: Some(LifecycleRuleFilter {
            prefix: Some("logs/".into()),
            ..Default::default()
        }),
        id: Some(id.into()),
        noncurrent_version_expiration: None,
        noncurrent_version_transitions: None,
        prefix: None,
        status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
        transitions: None,
    }
}

fn repl_config(rules: Vec<ReplicationRule>) -> ReplicationConfiguration {
    ReplicationConfiguration {
        role: "arn:aws:iam::1:role/r".into(),
        rules,
    }
}

fn repl_rule(dest: &str) -> ReplicationRule {
    ReplicationRule {
        delete_marker_replication: Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static("Disabled")),
        }),
        destination: Destination {
            access_control_translation: None,
            account: None,
            bucket: dest.into(),
            encryption_configuration: None,
            metrics: None,
            replication_time: None,
            storage_class: None,
        },
        existing_object_replication: None,
        filter: None,
        id: Some("x".into()),
        prefix: None,
        priority: None,
        source_selection_criteria: None,
        status: ReplicationRuleStatus::from_static("Enabled"),
    }
}

fn inventory_cfg(id: &str, dest: &str) -> InventoryConfiguration {
    InventoryConfiguration {
        destination: InventoryDestination {
            s3_bucket_destination: InventoryS3BucketDestination {
                account_id: None,
                bucket: dest.into(),
                encryption: None,
                format: InventoryFormat::from_static(InventoryFormat::CSV),
                prefix: None,
            },
        },
        filter: None,
        id: id.into(),
        included_object_versions: InventoryIncludedObjectVersions::from_static(
            InventoryIncludedObjectVersions::CURRENT,
        ),
        is_enabled: true,
        optional_fields: None,
        schedule: InventorySchedule {
            frequency: InventoryFrequency::from_static(InventoryFrequency::DAILY),
        },
    }
}

/// S3 协议面 lifecycle/replication/inventory 每写各一条命令；非管理员属主解析为本地 id。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_shared_stores_route_one_command_each() {
    let h = setup().await;
    create_bucket(&h, "hbk", "async", "Enabled").await;
    create_bucket(&h, "dst", "async", "Off").await;
    let svc = s3_full(&h);

    let before = applied(&h);
    svc.put_bucket_lifecycle_configuration(req(PutBucketLifecycleConfigurationInput {
        bucket: "hbk".into(),
        lifecycle_configuration: Some(BucketLifecycleConfiguration {
            rules: vec![lifecycle_rule("r1")],
        }),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "lifecycle_rules", "hbk"), 1);

    let before = applied(&h);
    svc.delete_bucket_lifecycle(req(DeleteBucketLifecycleInput {
        bucket: "hbk".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "lifecycle_rules", "hbk"), 0);

    let before = applied(&h);
    svc.put_bucket_replication(req(PutBucketReplicationInput {
        bucket: "hbk".into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        replication_configuration: repl_config(vec![repl_rule("remote-a/dst")]),
        token: None,
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "replication_rules", "hbk"), 1);

    let before = applied(&h);
    svc.delete_bucket_replication(req(DeleteBucketReplicationInput {
        bucket: "hbk".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "replication_rules", "hbk"), 0);

    let before = applied(&h);
    svc.put_bucket_inventory_configuration(as_user(
        req(PutBucketInventoryConfigurationInput {
            bucket: "hbk".into(),
            id: "c1".into(),
            inventory_configuration: inventory_cfg("c1", "dst"),
            expected_bucket_owner: None,
        }),
        1,
        true,
    ))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "inventory_configs", "hbk"), 1);
    let owner: i64 = sqlx::query_scalar(
        "SELECT owner_id FROM inventory_configs WHERE bucket = 'hbk' AND id = 'c1'",
    )
    .fetch_one(h.state.db.pool())
    .await
    .unwrap();
    // apply 语义：owner=None → 首个管理员 id（本夹具 erik=1；无管理员时为 0）。
    assert_eq!(owner, 1, "管理员属主应解析为首个管理员 id");

    let before = applied(&h);
    svc.delete_bucket_inventory_configuration(req(DeleteBucketInventoryConfigurationInput {
        bucket: "hbk".into(),
        expected_bucket_owner: None,
        id: "c1".into(),
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "inventory_configs", "hbk"), 0);

    // 非管理员属主：append 前 uid → username，apply 再解析回本地 id。
    assert_eq!(
        h.state
            .control
            .write(ControlCommand::UserCreate {
                username: "alice".into(),
                password_hash: "hash".into(),
                status: "active".into(),
                created_at: 1,
            })
            .await
            .unwrap(),
        oss_server::raft::ControlResponse::Ok
    );
    let alice_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'alice'")
        .fetch_one(h.state.db.pool())
        .await
        .unwrap();
    svc.create_bucket(as_user(
        req(CreateBucketInput {
            bucket: "abk".into(),
            ..Default::default()
        }),
        alice_id,
        false,
    ))
    .await
    .unwrap();
    svc.create_bucket(as_user(
        req(CreateBucketInput {
            bucket: "adst".into(),
            ..Default::default()
        }),
        alice_id,
        false,
    ))
    .await
    .unwrap();
    let before = applied(&h);
    svc.put_bucket_inventory_configuration(as_user(
        req(PutBucketInventoryConfigurationInput {
            bucket: "abk".into(),
            id: "c2".into(),
            inventory_configuration: inventory_cfg("c2", "adst"),
            expected_bucket_owner: None,
        }),
        alice_id,
        false,
    ))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    let owner: i64 = sqlx::query_scalar(
        "SELECT owner_id FROM inventory_configs WHERE bucket = 'abk' AND id = 'c2'",
    )
    .fetch_one(h.state.db.pool())
    .await
    .unwrap();
    assert_eq!(owner, alice_id, "非管理员属主必须落本地用户 id");
    shutdown(h).await;
}

/// S3 Put/DeleteBucketPolicy：Raft 各一条命令；非法策略零命令；删除无策略幂等成功。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_policy_writes_route_one_command_each() {
    let h = setup().await;
    create_bucket(&h, "pbk", "async", "Off").await;
    let svc = s3(&h);
    let policy = r#"{"Version":"2012-10-17","Statement":[]}"#.to_string();

    let before = applied(&h);
    svc.put_bucket_policy(req(PutBucketPolicyInput {
        bucket: "pbk".into(),
        policy: policy.clone(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "bucket_policies", "pbk"), 1);

    // 非法 JSON：handler 先拒，零命令。
    let before = applied(&h);
    let err = svc
        .put_bucket_policy(req(PutBucketPolicyInput {
            bucket: "pbk".into(),
            policy: "[]".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    assert_no_write(&h, before).await;

    let before = applied(&h);
    svc.delete_bucket_policy(req(DeleteBucketPolicyInput {
        bucket: "pbk".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    assert_eq!(count!(h, "bucket_policies", "pbk"), 0);

    // 无策略行删除 = 幂等成功（仍是一条命令）。
    let before = applied(&h);
    svc.delete_bucket_policy(req(DeleteBucketPolicyInput {
        bucket: "pbk".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_applied(&h, before + 1).await;
    shutdown(h).await;
}

/// M-1：Raft 删桶后本地 inventory_runs/tier_restores 一并清理。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_delete_clears_local_m8_rows() {
    let h = setup().await;
    create_bucket(&h, "lbk", "async", "Off").await;
    sqlx::query(
        "INSERT INTO inventory_runs (bucket,config_id,run_key,started_at,state,created_at) \
         VALUES ('lbk','c1','r1',0,'done',0)",
    )
    .execute(h.state.db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tier_restores (bucket,key,version_id,expires_at,state,created_at) \
         VALUES ('lbk','k','null',0,'done',0)",
    )
    .execute(h.state.db.pool())
    .await
    .unwrap();

    s3(&h)
        .delete_bucket(req(DeleteBucketInput {
            bucket: "lbk".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(count!(h, "inventory_runs", "lbk"), 0);
    assert_eq!(count!(h, "tier_restores", "lbk"), 0);
    shutdown(h).await;
}

/// M7（无 sink）：策略 handler 保持 s3s 旧行为（NotImplemented + 原文案）。
#[tokio::test]
async fn m7_policy_handlers_stay_not_implemented() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    assert!(!st.control.is_raft());
    let svc = OssS3::new(Arc::clone(&st.store), st.acl(), 1024 * 1024);
    let err = svc
        .put_bucket_policy(req(PutBucketPolicyInput {
            bucket: "m7p".into(),
            policy: "{}".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    assert_eq!(
        err.message(),
        Some("PutBucketPolicy is not implemented yet")
    );
    let err = svc
        .delete_bucket_policy(req(DeleteBucketPolicyInput {
            bucket: "m7p".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    assert_eq!(
        err.message(),
        Some("DeleteBucketPolicy is not implemented yet")
    );
}

/// M-2：真实 `[raft]` 配置走 `AppState::load`——控制面注入 + 启动对账 + 三存储路由。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_appstate_load_wires_control_and_reconciles() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let staging = dir.path().join("staging");
    // 启动前残留：空目录应删、非空目录应移入 orphan（证明 reconcile 在 serve 前跑了）。
    std::fs::create_dir_all(data.join("legacy")).unwrap();
    std::fs::create_dir_all(data.join("stray")).unwrap();
    std::fs::write(data.join("stray/keep"), b"x").unwrap();
    let cfg_path = dir.path().join("oss.toml");
    std::fs::write(
        &cfg_path,
        format!(
            r#"listen = "127.0.0.1:0"
data_dir = "{}"
staging_dir = "{}"
database_url = "sqlite://{}?mode=rwc"
jwt_secret = "{JWT_SECRET}"

[site]
region = "us-east-1"

[raft]
node_id = 1
bootstrap = true
advertise_endpoint = "http://127.0.0.1:1"
cluster_token = "s6c-load"
db_path = "{}"
election_timeout_ms = 500
heartbeat_interval_ms = 100
"#,
            data.display(),
            staging.display(),
            dir.path().join("m9.db").display(),
            dir.path().join("raft.db").display(),
        ),
    )
    .unwrap();

    let state = AppState::load(&cfg_path).await.unwrap();
    assert!(state.control.is_raft(), "真实 [raft] 必须注入 Raft 门面");
    assert!(
        state.control.bucket_sink().is_some(),
        "Raft 必须暴露 bucket sink"
    );
    assert!(!data.join("legacy").exists(), "启动对账未删空孤儿目录");
    assert!(!data.join("stray").exists(), "启动对账未移走非空孤儿目录");
    let orphans: Vec<_> = std::fs::read_dir(data.join(".oss.sys/orphan"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(orphans.len(), 1);
    assert!(orphans[0].starts_with("stray."));

    // 选举窗口内首次写可能 Unavailable：重试；Conflict = 前次已提交（幂等）。
    let sink = state.control.bucket_sink().unwrap();
    let mut created = false;
    for _ in 0..150 {
        match sink
            .create_bucket(bucket_create_spec("lbk", "async", "Off"))
            .await
        {
            Ok(()) => {
                created = true;
                break;
            }
            Err(BucketControlError::Conflict) => {
                created = true;
                break;
            }
            Err(BucketControlError::Unavailable) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("create_bucket: {e:?}"),
        }
    }
    assert!(created, "控制面在选举后仍不可写");
    assert!(data.join("lbk").is_dir(), "post-commit 应建物理目录");

    // 三个共享存储都经生产装配的 control 路由（行落在控制库）。
    state
        .lifecycle
        .put_rules(
            "lbk",
            &[LifecycleRuleSpec {
                rule_id: Some("r1".into()),
                filter: Default::default(),
                expiration: Some(ExpirationAction::Days { days: 1 }),
                transitions: vec![],
                noncurrent_expiration: None,
                noncurrent_transitions: vec![],
                abort_mpu_days: None,
                enabled: true,
            }],
        )
        .await
        .unwrap();
    state.repl_store.save_rules("lbk", "[]").await.unwrap();
    state
        .inventory
        .put_config(
            "lbk",
            &AuthedUser {
                uid: 1,
                is_admin: true,
                ak: String::new(),
            },
            &inventory_cfg("c1", "lbk"),
        )
        .await
        .unwrap();
    for (sql, table) in [
        (
            "SELECT COUNT(*) FROM lifecycle_rules WHERE bucket = 'lbk'",
            "lifecycle_rules",
        ),
        (
            "SELECT COUNT(*) FROM replication_rules WHERE bucket = 'lbk'",
            "replication_rules",
        ),
        (
            "SELECT COUNT(*) FROM inventory_configs WHERE bucket = 'lbk'",
            "inventory_configs",
        ),
    ] {
        let n: i64 = sqlx::query_scalar(sql)
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(n, 1, "{table} 未经控制面写入");
    }
}
