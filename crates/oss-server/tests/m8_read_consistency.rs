//! M8-3：控制台读一致性（`?consistent=1`）与 `x-oss-applied-index`。
//!
//! 默认 follower 是本地弱读；`consistent=1` 先有界追赶 leader 请求时刻的 applied
//! index，再读本地；无 leader / 追赶超时 → 503（fail-closed）。

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use oss_server::raft::control::ControlPlane;
use oss_server::raft::{ControlCommand, ControlResponse};

const ACC: &str = "000000000001";
const APPLIED_HEADER: &str = "x-oss-applied-index";

async fn connect_raft(dir: &std::path::Path, name: &str) -> oss_server::db::Db {
    oss_server::db::Db::connect_raft(&format!(
        "sqlite://{}?mode=rwc",
        dir.join(format!("{name}.db")).display()
    ))
    .await
    .unwrap()
}

fn token(uid: i64) -> String {
    #[derive(serde::Serialize)]
    struct Claims {
        sub: String,
        exp: usize,
    }
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
        + 3600;
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &Claims {
            sub: uid.to_string(),
            exp,
        },
        &jsonwebtoken::EncodingKey::from_secret(common::JWT_SECRET.as_bytes()),
    )
    .unwrap()
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn with_raft_control(
    st: &mut oss_server::state::AppState,
    handle: &Arc<oss_server::raft::RaftHandle>,
) {
    let control = ControlPlane::raft(
        Arc::clone(handle),
        Arc::clone(&st.db),
        Arc::clone(&st.store),
        Arc::clone(&st.ap_cache),
        Arc::clone(&st.olap_cache),
        st.ap_region.clone(),
    );
    st.control = Arc::new(control);
}

fn applied_header(resp: &reqwest::Response) -> u64 {
    resp.headers()
        .get(APPLIED_HEADER)
        .unwrap_or_else(|| panic!("缺少 {APPLIED_HEADER}"))
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

/// 无 leader（bootstrap=false 且成员表为空）：`consistent=1` 必须 503；
/// 默认弱读保持 200。中间件判定与 fail-closed 分支不需要真实集群。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consistent_read_without_leader_is_503_and_default_read_is_200() {
    let dir = tempfile::tempdir().unwrap();
    let node =
        common::raft_harness::spawn_node(common::raft_harness::settings(dir.path(), 1, false))
            .await;
    assert!(!node.handle.is_leader(), "无成员节点不应是 leader");
    assert_eq!(node.handle.leader_endpoint(), None);

    let mut st = common::state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    with_raft_control(&mut st, &node.handle);
    let base = serve(oss_server::router::build_router(st).unwrap()).await;
    let http = reqwest::Client::new();
    let login: serde_json::Value = http
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tok = login["token"].as_str().unwrap().to_string();

    let weak = http
        .get(format!("{base}/api/v1/accounts"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(weak.status(), StatusCode::OK, "默认弱读保持 200");
    assert_eq!(applied_header(&weak), 0, "无 apply 记录时上报 0");

    let consistent = http
        .get(format!("{base}/api/v1/accounts?consistent=1"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(
        consistent.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "无 leader 时 consistent=1 必须 fail-closed 503"
    );
    assert_eq!(applied_header(&consistent), 0);

    node.shutdown().await;
}

/// 2 节点集群：leader 提交账户后挡住 follower 的 apply，弱读立即返回陈旧空列表；
/// `consistent=1` 等本地追上后返回 200，能看到 leader 已提交账户，且响应头
/// `x-oss-applied-index` 覆盖 leader 请求时刻的 applied。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_consistent_read_catches_up_and_carries_applied_index() {
    let dir = tempfile::tempdir().unwrap();

    // 真实 2-voter 集群：node 1 为 leader（真实 advertise/HTTP），node 2 只跟随。
    let a_db = connect_raft(dir.path(), "read-a").await;
    let mut a_settings = common::raft_harness::settings(dir.path(), 1, true);
    a_settings.feature_level = 3;
    let a = common::raft_harness::spawn_node_with_pool(a_settings, Some(a_db.pool().clone())).await;
    common::raft_harness::wait_leader(&a.handle).await;

    let b_db = connect_raft(dir.path(), "read-b").await;
    let mut b_settings = common::raft_harness::settings(dir.path(), 2, false);
    b_settings.feature_level = 3;
    let b = common::raft_harness::spawn_node_with_pool(b_settings, Some(b_db.pool().clone())).await;
    oss_server::raft::membership::promote_voter(&a.handle, 2, b.base.clone())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if b.handle.raft().metrics().borrow().current_leader == Some(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader 1 not observed on follower"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 本地管理员只用于控制台鉴权（绕过复制守卫；账户数据必须来自 leader 复制）。
    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(b_db.pool())
        .await
        .unwrap();
    let admin_uid = sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
         VALUES ('erik', 'x', 1, 'active', 1)",
    )
    .execute(b_db.pool())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
        .execute(b_db.pool())
        .await
        .unwrap();

    let mut st = common::state(dir.path()).await;
    let b_db = Arc::new(b_db);
    st.db = Arc::clone(&b_db);
    with_raft_control(&mut st, &b.handle);
    let base = serve(oss_server::router::build_router(st).unwrap()).await;
    let http = reqwest::Client::new();
    let tok = token(admin_uid);

    // 拿住 follower 控制库写锁：leader 提交的账户日志到达后也 apply 不了。
    let mut lock = b_db.pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *lock)
        .await
        .unwrap();
    assert_eq!(
        a.handle
            .write(ControlCommand::AccountCreate {
                id: ACC.into(),
                name: "acme".into(),
                bucket_quota: None,
                created_at: 10,
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    let a_applied = a.handle.applied_index();

    // 默认弱读：立即 200（陈旧空列表），响应头报本机 applied（落后于 leader）。
    let weak = http
        .get(format!("{base}/api/v1/accounts"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(weak.status(), StatusCode::OK);
    let weak_applied = applied_header(&weak);
    assert!(
        weak_applied < a_applied,
        "apply 被锁住时本机 applied({weak_applied}) 应落后 leader({a_applied})"
    );
    let weak_body: serde_json::Value = weak.json().await.unwrap();
    assert!(
        !weak_body.to_string().contains(ACC),
        "弱读不应看到未 apply 的账户"
    );

    // consistent=1：等本地 apply 追上 leader 后才读（不是提前返回陈旧副本）。
    let req = {
        let http = http.clone();
        let url = format!("{base}/api/v1/accounts?consistent=1");
        let tok = tok.clone();
        tokio::spawn(async move { http.get(url).bearer_auth(tok).send().await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !req.is_finished(),
        "consistent=1 必须在 lagging follower 上等待追赶"
    );
    sqlx::query("ROLLBACK").execute(&mut *lock).await.unwrap();
    drop(lock);
    let resp = req.await.unwrap().unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        applied_header(&resp) >= a_applied,
        "追赶后本机 applied 应覆盖 leader 请求时刻"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body.to_string().contains(ACC),
        "consistent=1 必须看到 leader 已提交账户"
    );

    // 追赶完成后不带参数仍是 200 弱读，行为不变。
    let after = http
        .get(format!("{base}/api/v1/accounts"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(after.status(), StatusCode::OK);

    b.shutdown().await;
    a.shutdown().await;
}
