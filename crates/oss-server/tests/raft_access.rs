//! M9 S6d：AP/OLAP/inventory 路由、site/M7 409、真实 Raft 状态。
//!
//! 走**真实** `AppState::load`（单节点 openraft + 主控制库 + 启动断言）+ 真实 router；
//! 每条 HTTP 写用 `raft_applied.applied_log_id` 的前后差证明「恰好一条命令」。

mod common;

use std::time::Duration;

use oss_server::raft::control::ControlError;
use oss_server::raft::{ControlCommand, ControlResponse, RAFT_READY, STRETCH_READY, SYNC_READY};
use oss_server::router::build_router;
use oss_server::state::AppState;
use reqwest::StatusCode;
use serde_json::json;

const ADMIN: &str = "erik";
const ADMIN_PW: &str = "pw";

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

fn legacy_hash(password: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"oss-m1-salt:");
    h.update(password.as_bytes());
    hex::encode(h.finalize())
}

fn write_raft_config(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("oss.toml");
    let text = format!(
        r#"
listen = "127.0.0.1:0"
data_dir = "{data}"
staging_dir = "{staging}"
database_url = "sqlite://{db}?mode=rwc"
jwt_secret = "0123456789abcdef0123456789abcdef"

[site]
name = "s6d"
region = "us-east-1"

[raft]
node_id = 1
bootstrap = true
advertise_endpoint = "http://127.0.0.1:19001"
cluster_token = "s6d-cluster-token"
db_path = "{raft_db}"
election_timeout_ms = 500
heartbeat_interval_ms = 100
write_timeout_ms = 5000
"#,
        data = dir.join("data").display(),
        staging = dir.join("staging").display(),
        db = dir.join("control.db").display(),
        raft_db = dir.join("raft.db").display(),
    );
    std::fs::write(&path, text).unwrap();
    path
}

async fn raft_state(dir: &std::path::Path) -> AppState {
    let cfg = write_raft_config(dir);
    AppState::load(&cfg).await.unwrap()
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn login(base: &str, user: &str, pw: &str) -> String {
    let resp = client()
        .post(format!("{base}/api/v1/login"))
        .json(&json!({ "username": user, "password": pw }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "登录 {user} 失败");
    resp.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string()
}

/// `raft_applied.applied_log_id.index`（未 apply 过 = 0）。
async fn applied_index(st: &AppState) -> u64 {
    let raw: Option<String> =
        sqlx::query_scalar("SELECT applied_log_id FROM raft_applied WHERE id = 1")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    raw.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v["index"].as_u64())
        .unwrap_or(0)
}

/// 单节点选举完成前 `write` 会失败；用幂等探针（删不存在的用户）等 leader。
async fn wait_leader(st: &AppState) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        match st
            .control
            .write(ControlCommand::UserDelete {
                username: "__leader_probe__".into(),
            })
            .await
        {
            Ok(ControlResponse::NotFound) => return,
            Ok(other) => panic!("leader probe unexpected response: {other:?}"),
            Err(ControlError::Unavailable(_)) | Err(ControlError::Timeout(_)) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "raft leader election timed out"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("leader probe failed: {e}"),
        }
    }
}

async fn write_ok(st: &AppState, cmd: ControlCommand) {
    assert_eq!(
        st.control.write(cmd).await.unwrap(),
        ControlResponse::Ok,
        "控制命令应成功"
    );
}

async fn http(
    base: &str,
    token: Option<&str>,
    method: reqwest::Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> reqwest::Response {
    let mut r = client().request(method, format!("{base}{path}"));
    if let Some(t) = token {
        r = r.bearer_auth(t);
    }
    if let Some(b) = body {
        r = r.json(&b);
    }
    r.send().await.unwrap()
}

async fn wait_applied(st: &AppState, target: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while applied_index(st).await < target {
        assert!(
            tokio::time::Instant::now() < deadline,
            "apply 超时（期望 {target}，实际 {}）",
            applied_index(st).await
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(applied_index(st).await, target, "控制命令条数不符");
}

async fn assert_no_write(st: &AppState, before: u64) {
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(applied_index(st).await, before, "不应追加控制命令");
}

async fn count(st: &AppState, sql: &'static str, arg: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(arg)
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

/// 建普通用户 + 属主桶；返回 (uid, 用户名)。全部经控制命令（guard 生效）。
async fn seed_user_bucket(st: &AppState, user: &str, bucket: &str) -> i64 {
    write_ok(
        st,
        ControlCommand::UserCreate {
            username: user.into(),
            password_hash: legacy_hash("pw"),
            status: "active".into(),
            created_at: 1,
        },
    )
    .await;
    write_ok(
        st,
        ControlCommand::BucketCreate {
            name: bucket.into(),
            owner: Some(user.into()),
            created_at: 1,
            replication_mode: "async".into(),
            versioning: "Off".into(),
            object_lock: false,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            default_encryption: None,
        },
    )
    .await;
    sqlx::query_scalar("SELECT id FROM users WHERE username = ?")
        .bind(user)
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ap_and_olap_writes_route_one_command_and_refresh_caches() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;
    let alice = seed_user_bucket(&st, "alice", "apb").await;
    let base = serve(build_router(st.clone()).unwrap()).await;
    let token = login(&base, "alice", "pw").await;

    // create：一条命令；属主 username→本地行；提交后缓存可见。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&token),
        reqwest::Method::POST,
        "/api/v1/access-points",
        Some(json!({
            "name": "myap",
            "bucket": "apb",
            "policy": {"Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::apb/*"
            }]}
        })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    wait_applied(&st, before + 1).await;
    let owner: Option<i64> =
        sqlx::query_scalar("SELECT owner_id FROM access_points WHERE name = 'myap'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(owner, Some(alice), "非管理员属主必须映射回本地 id");
    let ap = st.ap_cache.get("myap").expect("create 后 AP 缓存应刷新");
    assert_eq!(ap.bucket, "apb");
    assert!(ap.policy.is_some());

    // 重复 create：仍是一条命令，apply 拒绝 → 409（行不变）。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&token),
        reqwest::Method::POST,
        "/api/v1/access-points",
        Some(json!({"name": "myap", "bucket": "apb"})),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    wait_applied(&st, before + 1).await;
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM access_points WHERE name = ?",
            "myap"
        )
        .await,
        1
    );

    // 400：非法策略在 append 前拒绝，零命令。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&token),
        reqwest::Method::POST,
        "/api/v1/access-points",
        Some(json!({
            "name": "bad-ap",
            "bucket": "apb",
            "policy": {"Statement": "nope"}
        })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_no_write(&st, before).await;

    // policy：一条命令；缓存随 SideEffect::ApReload 刷新。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&token),
        reqwest::Method::PUT,
        "/api/v1/access-points/myap/policy",
        Some(json!({"Statement": []})),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    wait_applied(&st, before + 1).await;
    let policy: Option<String> =
        sqlx::query_scalar("SELECT policy_json FROM access_points WHERE name = 'myap'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert!(policy.unwrap().contains("Statement"));

    // transform：一条 OlapPut；缓存由 SideEffect::OlapReload 全量重建。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&token),
        reqwest::Method::PUT,
        "/api/v1/access-points/myap/transform",
        Some(json!({"kind": "http", "endpoint": "https://olap.example.com/v1"})),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    wait_applied(&st, before + 1).await;
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM olap_configs WHERE access_point = ?",
            "myap"
        )
        .await,
        1
    );
    assert!(
        st.olap_cache.get("myap").unwrap().is_some(),
        "put_transform 后缓存应可见"
    );

    // delete：一条命令；级联删 OLAP，两个缓存都清。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&token),
        reqwest::Method::DELETE,
        "/api/v1/access-points/myap",
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    wait_applied(&st, before + 1).await;
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM access_points WHERE name = ?",
            "myap"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM olap_configs WHERE access_point = ?",
            "myap"
        )
        .await,
        0
    );
    assert!(st.ap_cache.get("myap").is_none());
    assert!(st.olap_cache.get("myap").unwrap().is_none());

    // 403：非属主非管理员不得建 AP（零命令）。
    write_ok(
        &st,
        ControlCommand::BucketCreate {
            name: "adminb".into(),
            owner: None,
            created_at: 1,
            replication_mode: "async".into(),
            versioning: "Off".into(),
            object_lock: false,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            default_encryption: None,
        },
    )
    .await;
    let resp = http(
        &base,
        Some(&token),
        reqwest::Method::POST,
        "/api/v1/access-points",
        Some(json!({"name": "nope-ap", "bucket": "adminb"})),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let before = applied_index(&st).await;
    assert_no_write(&st, before).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inventory_console_writes_one_command_and_run_stays_local() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;
    let alice = seed_user_bucket(&st, "alice", "invb").await;
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin_token = login(&base, ADMIN, ADMIN_PW).await;

    let cfg = json!({
        "id": "inv1",
        "frequency": "Daily",
        "destination_bucket": "invb"
    });
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&admin_token),
        reqwest::Method::PUT,
        "/api/v1/buckets/invb/inventory",
        Some(cfg),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    wait_applied(&st, before + 1).await;
    let owner: Option<i64> = sqlx::query_scalar(
        "SELECT owner_id FROM inventory_configs WHERE bucket = 'invb' AND id = 'inv1'",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(
        owner,
        Some(1),
        "管理员 owner 必须落本地 admin id（witness 0）"
    );

    // 覆盖同 id：仍是一条命令。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&admin_token),
        reqwest::Method::PUT,
        "/api/v1/buckets/invb/inventory",
        Some(json!({
            "id": "inv1",
            "frequency": "Weekly",
            "destination_bucket": "invb"
        })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    wait_applied(&st, before + 1).await;

    // run_now：本地入队（inventory_runs/jobs），不是控制写。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&admin_token),
        reqwest::Method::POST,
        "/api/v1/buckets/invb/inventory/inv1/run",
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    assert_no_write(&st, before).await;
    let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_runs WHERE bucket = 'invb'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(runs, 1, "run_now 应本地落一行");

    // 非管理员属主：username→id 映射。
    let alice_token = login(&base, "alice", "pw").await;
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&alice_token),
        reqwest::Method::PUT,
        "/api/v1/buckets/invb/inventory",
        Some(json!({
            "id": "alice-inv",
            "frequency": "Daily",
            "destination_bucket": "invb"
        })),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    wait_applied(&st, before + 1).await;
    let owner: Option<i64> = sqlx::query_scalar(
        "SELECT owner_id FROM inventory_configs WHERE bucket = 'invb' AND id = 'alice-inv'",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(owner, Some(alice));

    // delete：一条命令。
    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&admin_token),
        reqwest::Method::DELETE,
        "/api/v1/buckets/invb/inventory/inv1",
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    wait_applied(&st, before + 1).await;
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM inventory_configs WHERE bucket = ?",
            "invb"
        )
        .await,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn site_changelog_409_and_pairing_keeps_m7_state_empty() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;
    let base = serve(build_router(st.clone()).unwrap()).await;

    // GET 旧 M7 下是 405；Raft 下两种方法都 409（token 面，无 JWT）。
    let resp = http(
        &base,
        None,
        reqwest::Method::GET,
        "/api/v1/site/changelog",
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let resp = http(
        &base,
        None,
        reqwest::Method::POST,
        "/api/v1/site/changelog",
        Some(json!({"origin": "x", "entries": []})),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    // 配对行保持本地，但不得种 M7 changelog/游标（启动断言仍须通过）。
    let peer = oss_server::db::SitePeer {
        peer_id: "peer-1".into(),
        remote_site_id: "remote-1".into(),
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
    };
    oss_server::site::insert_peer_with_snapshot(&st, &peer)
        .await
        .unwrap();
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM sites WHERE peer_id = ?",
            "peer-1"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &st,
            "SELECT COUNT(*) FROM site_changelog WHERE origin_site_id = ?",
            "remote-1"
        )
        .await,
        0
    );
    for table in ["site_entity_clock", "site_cursors", "site_apply_dead"] {
        let n: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(st.db.pool())
                .await
                .unwrap();
        assert_eq!(n, 0, "{table} 在 Raft 下必须为空");
    }
    st.db.assert_mode_guards().await.unwrap();

    // 第二条 peer 仍被 M7 单 peer 约束拒绝（不在 S6d 改语义）。
    assert!(
        oss_server::site::insert_peer_with_snapshot(&st, &peer)
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reports_real_raft_state_and_ready_flags() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;
    let base = serve(build_router(st.clone()).unwrap()).await;

    let v: serde_json::Value = client()
        .get(format!("{base}/api/v1/raft/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["enabled"], true);
    assert_eq!(v["node_id"], 1);
    assert_eq!(v["leader"], 1);
    assert_eq!(v["role"], "voter");
    assert_eq!(v["state"], "leader");
    assert_eq!(v["initialized"], true);
    assert_eq!(v["members"], serde_json::json!([1]));
    assert!(v["term"].as_u64().unwrap() >= 1);
    assert_eq!(v["capabilities"]["raft"], RAFT_READY);
    assert_eq!(v["capabilities"]["sync"], SYNC_READY);
    // 本测试只有 [raft] 无 [stretch]：能力位按实际配置为 false（与 /api/v1/site 同口径）。
    assert_eq!(v["capabilities"]["stretch"], false);
    let _ = STRETCH_READY;

    // M7/Local（无 [raft]）：S0 骨架，能力位全 false（与 /api/v1/site 同口径）。
    let m7dir = tempfile::tempdir().unwrap();
    let m7 = common::state(m7dir.path()).await;
    let m7base = serve(build_router(m7).unwrap()).await;
    let v: serde_json::Value = client()
        .get(format!("{m7base}/api/v1/raft/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["enabled"], false);
    assert_eq!(v["capabilities"]["raft"], false);
    assert_eq!(v["capabilities"]["sync"], false);
    assert_eq!(v["capabilities"]["stretch"], false);
}

/// READY 翻转前的 503 与翻转后的单命令建桶用同一用例覆盖：断言跟着常量走。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_sync_create_gate_and_routing_follow_ready_flags() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;
    // sync 前置：唯一数据 peer。
    st.db
        .insert_site(&oss_server::db::SitePeer {
            peer_id: "peer-1".into(),
            remote_site_id: "remote-1".into(),
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
    let base = serve(build_router(st.clone()).unwrap()).await;
    let token = login(&base, ADMIN, ADMIN_PW).await;

    let before = applied_index(&st).await;
    let resp = http(
        &base,
        Some(&token),
        reqwest::Method::POST,
        "/api/v1/buckets",
        Some(json!({"name": "syncb", "replication_mode": "sync"})),
    )
    .await;
    if RAFT_READY && SYNC_READY {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        assert_eq!(status, StatusCode::NO_CONTENT, "sync 建桶失败: {body}");
        wait_applied(&st, before + 1).await;
        let mode: String =
            sqlx::query_scalar("SELECT replication_mode FROM buckets WHERE name = 'syncb'")
                .fetch_one(st.db.pool())
                .await
                .unwrap();
        assert_eq!(mode, "sync");
        assert!(st.store.bucket_exists("syncb").await.unwrap());
    } else if !RAFT_READY {
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_no_write(&st, before).await;
    } else {
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_no_write(&st, before).await;
    }
}
