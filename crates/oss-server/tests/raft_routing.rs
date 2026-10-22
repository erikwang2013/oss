//! M9 S6b：console IAM/AK/permissions 路由到 `state.control` 的端到端用例。
//!
//! Raft 变体走**真实** `AppState::load`（单节点 openraft + 主控制库）+ 真实 router；
//! 每条 HTTP 写用 `raft_applied.applied_log_id` 的前后差证明「恰好一条命令」。
//! M7 变体走 `common::state` 的 Local 门面，证明行为与既有直写路径一致。

mod common;

use std::time::Duration;

use oss_server::raft::control::{ControlError, ControlPlane};
use oss_server::raft::{ControlCommand, ControlResponse, RaftSettings};
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

/// 真实 `[raft]` 配置：单 voter + 允许快速选举/短写超时（测试机负载高，避免长等）。
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
name = "s6b"
region = "us-east-1"

[raft]
node_id = 1
bootstrap = true
advertise_endpoint = "http://127.0.0.1:19000"
cluster_token = "s6b-cluster-token"
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

/// K1 专用：与 `raft_state` 相同，但开启密钥库（M5 接线后从 `enc_state` 派生 vault）。
/// 不走 `OSS_MASTER_KEY_FILE` 环境变量，避免进程级 env 与其它并行用例互相污染。
async fn raft_state_with_vault(dir: &std::path::Path) -> AppState {
    let mut st = raft_state(dir).await;
    st.store
        .init_encryption(Some(dir.join("master.key")), None)
        .await
        .unwrap();
    st.vault =
        xl_s3::skvault::SecretVault::from_enc_state(st.store.enc_state()).map(std::sync::Arc::new);
    assert!(st.vault.is_some(), "测试夹具必须启用 vault");
    st
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// 登录成功返回 token；失败（含停用/口令错）返回 None。
async fn login(base: &str, user: &str, pw: &str) -> Option<String> {
    let resp = client()
        .post(format!("{base}/api/v1/login"))
        .json(&json!({ "username": user, "password": pw }))
        .send()
        .await
        .unwrap();
    if resp.status() != StatusCode::OK {
        return None;
    }
    let v: serde_json::Value = resp.json().await.unwrap();
    Some(v["token"].as_str().unwrap().to_string())
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

async fn create_sub(base: &str, token: &str, name: &str, pw: &str) -> i64 {
    let resp = client()
        .post(format!("{base}/api/v1/users"))
        .bearer_auth(token)
        .json(&json!({ "username": name, "password": pw }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "建子账户应成功");
    resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_i64()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_user_paths_write_one_command_each_and_map_responses() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    // 管理员引导是 guard 例外（本地，不复制）。
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login(&base, ADMIN, ADMIN_PW).await.unwrap();

    // U1：建子账户 = 一条命令，200 {"id"}；apply 行恒 is_admin=0、Argon2 已在 append 前算好。
    let before = applied_index(&st).await;
    let sub_id = create_sub(&base, &admin, "sub", "pw12345678").await;
    assert_eq!(applied_index(&st).await, before + 1, "U1 恰好一条命令");
    let (hash, is_admin): (String, i64) =
        sqlx::query_as("SELECT password_hash, is_admin FROM users WHERE id = ?")
            .bind(sub_id)
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert!(hash.starts_with("$argon2"), "口令必须已哈希: {hash}");
    assert_eq!(is_admin, 0);

    // 重名：apply 业务拒绝 Conflict → 409（`response_status` 映射）；命令仍是一条
    //（拒绝不回滚已提交的 entry）。
    let before = applied_index(&st).await;
    let resp = client()
        .post(format!("{base}/api/v1/users"))
        .bearer_auth(&admin)
        .json(&json!({ "username": "sub", "password": "pw12345678" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(applied_index(&st).await, before + 1, "重名也是一条命令");

    // 前置校验失败（口令过短）：400 且**不触达**控制面。
    let before = applied_index(&st).await;
    let resp = client()
        .post(format!("{base}/api/v1/users"))
        .bearer_auth(&admin)
        .json(&json!({ "username": "short", "password": "short" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(applied_index(&st).await, before, "校验失败不得 append");

    // U6：权限全量替换 = 一条命令；自然键落行。
    let before = applied_index(&st).await;
    let resp = client()
        .put(format!("{base}/api/v1/users/{sub_id}/permissions"))
        .bearer_auth(&admin)
        .json(&json!([{"bucket": "alpha", "read": true, "write": true, "delete": false}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(applied_index(&st).await, before + 1, "U6 恰好一条命令");
    let perms: (String, i64, i64, i64) = sqlx::query_as(
        "SELECT bucket, can_read, can_write, can_delete FROM permissions WHERE user_id = ?",
    )
    .bind(sub_id)
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(perms, ("alpha".into(), 1, 1, 0));

    // U3：重置口令 = 一条命令（hash 预计算）；旧口令失效。
    let before = applied_index(&st).await;
    let resp = client()
        .post(format!("{base}/api/v1/users/{sub_id}/password"))
        .bearer_auth(&admin)
        .json(&json!({ "password": "newpw1234" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(applied_index(&st).await, before + 1, "U3 恰好一条命令");
    assert!(login(&base, "sub", "pw12345678").await.is_none());
    let sub = login(&base, "sub", "newpw1234").await.unwrap();

    // U4：自助改密 = 一条命令（verify 只读，不产生 entry）。
    let before = applied_index(&st).await;
    let resp = client()
        .post(format!("{base}/api/v1/me/password"))
        .bearer_auth(&sub)
        .json(&json!({ "old_password": "newpw1234", "new_password": "selfpw1234" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(applied_index(&st).await, before + 1, "U4 恰好一条命令");
    assert!(login(&base, "sub", "newpw1234").await.is_none());
    assert!(login(&base, "sub", "selfpw1234").await.is_some());

    // 管理员自停用：预检 400，不 append。
    let me: serde_json::Value = client()
        .get(format!("{base}/api/v1/me"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let before = applied_index(&st).await;
    let resp = client()
        .put(format!("{base}/api/v1/users/{}/status", me["id"]))
        .bearer_auth(&admin)
        .json(&json!({ "active": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(applied_index(&st).await, before, "自停用预检不得 append");

    // U5：停用/启用各一条命令；停用后登录 401。
    let before = applied_index(&st).await;
    let resp = client()
        .put(format!("{base}/api/v1/users/{sub_id}/status"))
        .bearer_auth(&admin)
        .json(&json!({ "active": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(applied_index(&st).await, before + 1, "U5 停用恰好一条命令");
    assert!(login(&base, "sub", "selfpw1234").await.is_none());
    let before = applied_index(&st).await;
    let resp = client()
        .put(format!("{base}/api/v1/users/{sub_id}/status"))
        .bearer_auth(&admin)
        .json(&json!({ "active": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(applied_index(&st).await, before + 1, "U5 启用恰好一条命令");
    assert!(login(&base, "sub", "selfpw1234").await.is_some());

    // 非管理员不存在：404 且不 append。
    let before = applied_index(&st).await;
    let resp = client()
        .put(format!("{base}/api/v1/users/999/status"))
        .bearer_auth(&admin)
        .json(&json!({ "active": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(applied_index(&st).await, before, "404 不得 append");

    // U2：删子账户 = 一条命令；apply 事务级联（permissions/AKs/owner 置 NULL）。
    let before = applied_index(&st).await;
    let resp = client()
        .delete(format!("{base}/api/v1/users/{sub_id}"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(applied_index(&st).await, before + 1, "U2 恰好一条命令");
    let left: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM users WHERE id = ?), \
         (SELECT COUNT(*) FROM permissions WHERE user_id = ?)",
    )
    .bind(sub_id)
    .bind(sub_id)
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(left, (0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_access_keys_route_non_admin_and_keep_guard_exceptions_local() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state_with_vault(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login(&base, ADMIN, ADMIN_PW).await.unwrap();
    create_sub(&base, &admin, "sub", "pw12345678").await;
    let sub = login(&base, "sub", "pw12345678").await.unwrap();

    // K1：非管理员属主 AK = 一条命令；kind 落 long-term，属主按 username 解析；
    // 入日志/落库的是 **append 前封存** 的密文，响应仍一次性回明文。
    let before = applied_index(&st).await;
    let resp = client()
        .post(format!("{base}/api/v1/access-keys"))
        .bearer_auth(&sub)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let created: serde_json::Value = resp.json().await.unwrap();
    let ak = created["access_key"].as_str().unwrap().to_string();
    let plain_sk = created["secret_key"].as_str().unwrap().to_string();
    assert_eq!(applied_index(&st).await, before + 1, "K1 恰好一条命令");
    let row: (i64, String, String) = sqlx::query_as(
        "SELECT u.is_admin, ak.kind, ak.secret_key FROM access_keys ak \
         JOIN users u ON u.id = ak.user_id WHERE ak.access_key = ?",
    )
    .bind(&ak)
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!((row.0, row.1.as_str()), (0, "long-term"));
    assert!(
        xl_store::kms::is_sealed(&row.2),
        "复制的 secret_key 必须是封存密文: {}",
        row.2
    );
    assert_ne!(row.2, plain_sk, "落库/复制值不得等于响应明文 SK");

    // 显式 AK 重名：apply Conflict → 409；仍一条命令。
    let before = applied_index(&st).await;
    let resp = client()
        .post(format!("{base}/api/v1/access-keys"))
        .bearer_auth(&sub)
        .json(&json!({ "access_key": ak, "secret_key": "SK_DUP_0000000000001" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(applied_index(&st).await, before + 1, "重名也是一条命令");

    // K2：管理员属主 AK 是 guard 例外——本地写、**不产生** Raft entry。
    let before = applied_index(&st).await;
    let resp = client()
        .post(format!("{base}/api/v1/access-keys"))
        .bearer_auth(&admin)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let admin_ak: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(applied_index(&st).await, before, "K2 不得复制");
    let owner_admin: i64 = sqlx::query_scalar(
        "SELECT u.is_admin FROM access_keys ak JOIN users u ON u.id = ak.user_id \
         WHERE ak.access_key = ?",
    )
    .bind(admin_ak["access_key"].as_str().unwrap())
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(owner_admin, 1);

    // K3：STS 会话行同样是 guard 例外（本地 kind='session' 插入成功），也不进 Raft。
    let before = applied_index(&st).await;
    sqlx::query(
        "INSERT INTO access_keys (access_key, secret_key, user_id, kind, created_at) \
         VALUES ('ASIA_SESSION_0001', 'SK_SESSION_000001', \
                 (SELECT id FROM users WHERE username = 'sub'), 'session', 1)",
    )
    .execute(st.db.pool())
    .await
    .unwrap();
    assert_eq!(applied_index(&st).await, before, "K3 不得复制");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_hidden_writes_stay_outside_apply() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;

    // H1：旧 M1 哈希只能经命令进入控制库（apply 外非管理员写被 guard 拦）。
    let legacy = legacy_hash("legacypw");
    assert_eq!(
        st.control
            .write(ControlCommand::UserCreate {
                username: "legacy".into(),
                password_hash: legacy.clone(),
                status: "active".into(),
                created_at: 1,
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    let before = applied_index(&st).await;
    // 登录路径（verify_user）：校验通过，但 Raft 下**不得**透明升级 Argon2。
    assert!(
        st.db
            .verify_user("legacy", "legacypw")
            .await
            .unwrap()
            .is_some()
    );
    let stored: String =
        sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'legacy'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(stored, legacy, "H1：verify_user 不得在 apply 外重写 users");
    assert_eq!(applied_index(&st).await, before, "登录不得 append");

    // HTTP 登录同样只读；改密仍必须走 UserSetPassword（一条命令）。
    let base = serve(build_router(st.clone()).unwrap()).await;
    assert!(login(&base, "legacy", "legacypw").await.is_some());
    assert_eq!(applied_index(&st).await, before, "HTTP 登录不得 append");
    let token = login(&base, "legacy", "legacypw").await.unwrap();
    let resp = client()
        .post(format!("{base}/api/v1/me/password"))
        .bearer_auth(&token)
        .json(&json!({ "old_password": "legacypw", "new_password": "newpw12345" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(applied_index(&st).await, before + 1, "改密恰好一条命令");
    let upgraded: String =
        sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'legacy'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert!(
        upgraded.starts_with("$argon2"),
        "改密应写入预计算 Argon2: {upgraded}"
    );

    // 未路由的直写一律被 guard 拒绝 → handler 只能得到 Err（5xx），不可能静默成功/204。
    let legacy_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'legacy'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert!(
        st.db.set_password(legacy_id, "bypass123").await.is_err(),
        "apply 外的 users UPDATE 必须失败"
    );
    assert!(
        st.db
            .create_user("direct", "pw12345678", false)
            .await
            .is_err(),
        "apply 外的非管理员 INSERT 必须失败"
    );
    assert!(
        st.db
            .set_permissions(
                legacy_id,
                &[oss_server::db::PermissionRow {
                    bucket: "guard".into(),
                    read: true,
                    write: false,
                    delete: false,
                }],
            )
            .await
            .is_err(),
        "apply 外的 permissions 写必须失败"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_write_failure_maps_to_503_with_hint_not_silent_204() {
    let dir = tempfile::tempdir().unwrap();
    let st = raft_state(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    wait_leader(&st).await;
    let admin = login(
        &serve(build_router(st.clone()).unwrap()).await,
        ADMIN,
        ADMIN_PW,
    )
    .await
    .unwrap();

    // 无成员的节点（bootstrap=false）永远没有 leader：写入口只能返回错误。
    // S7 才接转发路由；S6b 钉住「失败 → 503 + 提示，绝不静默 204」。
    let dead = oss_server::raft::spawn_with_control(
        RaftSettings {
            node_id: 9,
            role: oss_server::config::RaftRole::Voter,
            bootstrap: false,
            advertise_endpoint: "http://127.0.0.1:19001".to_string(),
            cluster_token: "s6b-cluster-token".to_string(),
            db_path: dir.path().join("dead-raft.db"),
            election_timeout_ms: 2_000,
            heartbeat_interval_ms: 200,
            install_snapshot_timeout_ms: 5_000,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1024,
            write_timeout_ms: 2_000,
            snapshot_max_chunk_size: 4 * 1024 * 1024,
            snapshot_max_bytes: 512 * 1024 * 1024,
            feature_level: 1,
        },
        None,
        None,
    )
    .await
    .unwrap();
    let dead = std::sync::Arc::new(dead);

    let mut failing = st.clone();
    failing.control = std::sync::Arc::new(ControlPlane::raft(
        std::sync::Arc::clone(&dead),
        failing.db.clone(),
        failing.store.clone(),
        failing.ap_cache.clone(),
        failing.olap_cache.clone(),
        failing.ap_region.clone(),
    ));
    let base = serve(build_router(failing.clone()).unwrap()).await;
    let before = applied_index(&st).await;
    let resp = client()
        .post(format!("{base}/api/v1/users"))
        .bearer_auth(&admin)
        .json(&json!({ "username": "never", "password": "pw12345678" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "写失败必须 503"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("raft"),
        "503 应带 raft/转发提示，实得: {body}"
    );
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE username = 'never'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(exists, 0, "失败不得落任何行");
    assert_eq!(applied_index(&st).await, before, "失败不得 append");
    dead.shutdown().await;
}

#[tokio::test]
async fn local_control_matches_m7_behavior_for_users_and_keys() {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    st.db.create_user(ADMIN, ADMIN_PW, true).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login(&base, ADMIN, ADMIN_PW).await.unwrap();

    // U1/U6/U3/U4/U5/U2 在 Local 门面下与既有 API 语义一致。
    let sub_id = create_sub(&base, &admin, "sub", "pw12345678").await;
    let resp = client()
        .put(format!("{base}/api/v1/users/{sub_id}/permissions"))
        .bearer_auth(&admin)
        .json(&json!([{"bucket": "alpha", "read": true, "write": false, "delete": false}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = client()
        .post(format!("{base}/api/v1/users/{sub_id}/password"))
        .bearer_auth(&admin)
        .json(&json!({ "password": "newpw1234" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(login(&base, "sub", "newpw1234").await.is_some());
    let sub = login(&base, "sub", "newpw1234").await.unwrap();
    let resp = client()
        .post(format!("{base}/api/v1/me/password"))
        .bearer_auth(&sub)
        .json(&json!({ "old_password": "newpw1234", "new_password": "selfpw1234" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = client()
        .put(format!("{base}/api/v1/users/{sub_id}/status"))
        .bearer_auth(&admin)
        .json(&json!({ "active": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(login(&base, "sub", "selfpw1234").await.is_none());
    let resp = client()
        .delete(format!("{base}/api/v1/users/{sub_id}"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = client()
        .delete(format!("{base}/api/v1/users/{sub_id}"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // K1 非管理员 / K2 管理员都成功（Local 时后者同样走本地直写）。
    let sub_id = create_sub(&base, &admin, "sub2", "pw12345678").await;
    let sub = login(&base, "sub2", "pw12345678").await.unwrap();
    for token in [&sub, &admin] {
        let resp = client()
            .post(format!("{base}/api/v1/access-keys"))
            .bearer_auth(token)
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    assert!(
        st.db.get_user(sub_id).await.unwrap().is_some(),
        "子账户仍存在"
    );

    // H1 的 M7 对照：非 Raft 库上 verify_user 仍原地升级旧哈希。
    let legacy = legacy_hash("legacypw");
    st.db
        .create_user("legacy", "placeholder", false)
        .await
        .unwrap();
    sqlx::query("UPDATE users SET password_hash = ? WHERE username = 'legacy'")
        .bind(&legacy)
        .execute(st.db.pool())
        .await
        .unwrap();
    assert!(
        st.db
            .verify_user("legacy", "legacypw")
            .await
            .unwrap()
            .is_some()
    );
    let upgraded: String =
        sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'legacy'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert!(upgraded.starts_with("$argon2"), "M7 透明升级必须保留");
}
