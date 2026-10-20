//! M9 S0：模式契约（0004 重放/列迁移/降级守卫/guard 例外/能力开关/桶模式往返）。

mod common;

use common::JWT_SECRET;
use oss_server::config::{AppConfig, EcToml, GroupTarget, RaftToml, SiteToml, StretchToml};
use oss_server::db::Db;
use oss_server::router::build_router;
use oss_server::state::AppState;
use xl_store::store::{Store, StoreConfig};

fn db_url(dir: &std::path::Path) -> String {
    format!("sqlite://{}?mode=rwc", dir.join("m9.db").display())
}

async fn count_triggers(db: &Db, pattern: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE ?")
        .bind(pattern)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

fn raft_cfg(dir: &std::path::Path) -> AppConfig {
    let raft: RaftToml = toml::from_str(
        r#"
node_id = 1
bootstrap = true
advertise_endpoint = "http://127.0.0.1:9000"
cluster_token = "t"
"#,
    )
    .unwrap();
    AppConfig {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.join("data"),
        data_dirs: None,
        ec: Default::default(),
        staging_dir: dir.join("staging"),
        database_url: db_url(dir),
        jwt_secret: JWT_SECRET.into(),
        max_put_bytes: 1024 * 1024,
        groups: vec![GroupTarget {
            group: "file".into(),
            bucket: "files".into(),
            max_size: 0,
            extensions: vec![],
        }],
        cluster: None,
        kafka: Default::default(),
        remotes: vec![],
        site: Some(SiteToml::default()),
        tiers: vec![],
        raft: Some(raft),
        sync: Default::default(),
        stretch: None,
    }
}

fn stretch_cfg(dir: &std::path::Path) -> AppConfig {
    let mut cfg = raft_cfg(dir);
    cfg.groups = vec![];
    cfg.ec = EcToml {
        enabled: true,
        data: 8,
        parity: 4,
        inline_threshold: 0,
    };
    cfg.data_dirs = Some((0..6).map(|i| dir.join(format!("d{i}"))).collect());
    cfg.stretch = Some(StretchToml {
        node_id: "a".into(),
        self_addr: "a:9100".into(),
        peer_node_id: "b".into(),
        peer_addr: "b:9100".into(),
        peer_drives: 6,
        placement: "v2".into(),
        mesh_secret: "mesh-topsecret-0123456789abcdef".into(),
        mesh_timeout_ms: 5_000,
    });
    cfg
}

async fn raft_state(dir: &std::path::Path) -> AppState {
    let cfg = raft_cfg(dir);
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let store = Store::open(StoreConfig::new(&cfg.data_dir)).await.unwrap();
    let db = Db::connect_raft(&cfg.database_url).await.unwrap();
    AppState::new(cfg, db, store).await
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn login(base: &str) -> String {
    let login: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    login["token"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn m9_double_connect_and_columns() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(dir.path());
    let db = Db::connect(&url).await.unwrap();
    // 0004 表齐 + ctx 恰好一行
    for t in [
        "schema_meta",
        "raft_applied",
        "raft_apply_ctx",
        "sync_write_intents",
        "sync_delete_intents",
    ] {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(t)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(n, 1, "缺表 {t}");
    }
    let ctx: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raft_apply_ctx")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(ctx, 1);
    // M7 启动：guard 必须已 drop（M7 控制写不受影响）
    assert_eq!(count_triggers(&db, "raft_guard_%").await, 0);
    db.insert_bucket("m7-bkt", None).await.unwrap();
    drop(db);
    // 连续第二次 connect（0004 重放）
    let db = Db::connect(&url).await.unwrap();
    assert_eq!(count_triggers(&db, "raft_guard_%").await, 0);
    let mode: String =
        sqlx::query_scalar("SELECT replication_mode FROM buckets WHERE name='m7-bkt'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(mode, "async");
}

#[tokio::test]
async fn m9_old_db_gains_columns_idempotently() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(dir.path());
    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    // pre-M9 的 buckets：无 replication_mode/deleting
    sqlx::raw_sql(
        "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, \
         versioning TEXT NOT NULL DEFAULT 'Off', object_lock INTEGER NOT NULL DEFAULT 0, \
         set_id INTEGER NOT NULL DEFAULT 0, default_encryption TEXT);\
         INSERT INTO buckets (name, owner_id, creation_date) VALUES ('legacy', NULL, 0);",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
    let db = Db::connect(&url).await.unwrap();
    let mode: String =
        sqlx::query_scalar("SELECT replication_mode FROM buckets WHERE name='legacy'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(mode, "async");
    let deleting: i64 = sqlx::query_scalar("SELECT deleting FROM buckets WHERE name='legacy'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(deleting, 0);
    drop(db);
    Db::connect(&url).await.unwrap();
}

#[tokio::test]
async fn m9_downgrade_guard_rejects_m7_startup() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(dir.path());
    let raft = Db::connect_raft(&url).await.unwrap();
    assert_eq!(
        raft.schema_meta("raft_mode").await.unwrap().as_deref(),
        Some("on")
    );
    assert_eq!(count_triggers(&raft, "raft_guard_%").await, 33);
    drop(raft);
    let err = Db::connect(&url).await.err().expect("降级守卫必须拒绝启动");
    assert!(
        err.to_string().contains("Raft 模式"),
        "无 [raft] 启动必须被降级守卫拒绝：{err}"
    );
}

#[tokio::test]
async fn m9_guards_block_outside_apply_with_admin_exceptions() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::connect_raft(&db_url(dir.path())).await.unwrap();
    // apply 外：普通用户/桶写被 guard ABORT
    let err =
        sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES ('x', NULL, 0)")
            .execute(db.pool())
            .await
            .unwrap_err();
    assert!(
        err.to_string()
            .contains("raft: control write outside apply")
    );
    let err = sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES ('u','h',0,0)",
    )
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("raft: control write outside apply")
    );

    // apply 事务内造数据（非管理员用户 + AK + 管理员）
    db.set_raft_apply_ctx(true).await.unwrap();
    sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES ('u','h',0,0)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO users (username, password_hash, is_admin, created_at) VALUES ('admin','h',1,0)")
        .execute(db.pool())
        .await
        .unwrap();
    let uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username='u'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let admin: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username='admin'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    db.set_raft_apply_ctx(false).await.unwrap();

    // 例外：session AK、管理员属主 AK、管理员 permissions、admin 用户行
    for sql in [
        format!("INSERT INTO access_keys (access_key, secret_key, user_id, created_at, kind) VALUES ('sess','s',{uid},0,'session')"),
        format!("INSERT INTO access_keys (access_key, secret_key, user_id, created_at) VALUES ('adminkey','s',{admin},0)"),
        format!("INSERT INTO permissions (user_id, bucket) VALUES ({admin},'b')"),
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES ('admin2','h',1,0)".into(),
    ] {
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(db.pool())
            .await
            .unwrap();
    }
    // 非例外：普通用户长期 AK、普通用户 permissions 均 ABORT
    let err = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO access_keys (access_key, secret_key, user_id, created_at) VALUES ('uk','s',{uid},0)"
    )))
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("raft: control write outside apply")
    );
    let err = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO permissions (user_id, bucket) VALUES ({uid},'b')"
    )))
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("raft: control write outside apply")
    );

    // apply ctx 放行桶写
    db.set_raft_apply_ctx(true).await.unwrap();
    db.insert_bucket("applied", None).await.unwrap();
    db.set_raft_apply_ctx(false).await.unwrap();
    assert!(db.bucket_exists("applied").await.unwrap());
}

#[tokio::test]
async fn m9_console_sync_create_400_and_mode_readback() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let token = login(&base).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"name": "synced", "replication_mode": "sync"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "SYNC_READY=false 时 sync 创建必须 400");

    let resp = client
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"name": "plain", "replication_mode": "async"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let buckets: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let plain = buckets
        .iter()
        .find(|b| b["name"] == "plain")
        .expect("列表应含 plain");
    assert_eq!(plain["replication_mode"], "async");
}

#[tokio::test]
async fn m9_console_recreate_mode_change_409() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    // 陈旧行：sync 模式；物理桶不存在（删桶记账失败的形态）
    st.db
        .upsert_bucket_owner_set_mode("stale", None, 0, "sync")
        .await
        .unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let token = login(&base).await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"name": "stale"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409, "重建不得改复制模式");
}

#[tokio::test]
async fn m9_raft_mode_control_write_503_and_status_skeleton() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    // 状态骨架：无鉴权可达；S0 恒 enabled=false、sync 能力 false
    let status: serde_json::Value = reqwest::get(format!("{base}/api/v1/raft/status"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["enabled"], false);
    assert_eq!(status["capabilities"]["sync"], false);

    let token = login(&base).await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"name": "raftb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "RAFT_READY=false 控制写必须 503");
}

/// I1：stretch identity 用本端实盘 `drive_count`（不信任 peer 声明），且不泄漏 secret。
#[tokio::test]
async fn m9_stretch_identity_drive_count_and_no_secret() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = stretch_cfg(dir.path());
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let store = Store::open(StoreConfig::new(&cfg.data_dir)).await.unwrap();
    let db = Db::connect_raft(&cfg.database_url).await.unwrap();
    let st = AppState::new(cfg, db, store).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let token = login(&base).await;
    let v: serde_json::Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/site"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let s = &v["stretch"];
    assert_eq!(s["drive_count"], 6);
    assert_eq!(s["peer_node_id"], "b");
    assert_eq!(s["self_addr"], "a:9100");
    assert_eq!(s["ec"]["data"], 8);
    assert_eq!(s["ec"]["parity"], 4);
    assert_eq!(s["placement"], "v2");
    assert_eq!(s["mesh_secret_fingerprint"].as_str().unwrap().len(), 64);
    assert!(s.get("peer_drives").is_none(), "不得回传对端声明盘数");
    let body = serde_json::to_string(&v).unwrap();
    assert!(!body.contains("topsecret"), "identity 不得回显 mesh_secret");
}
