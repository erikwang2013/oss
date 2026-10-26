//! follower 上的账户写：转发到假 leader 得到 409 → HTTP 503 + `x-oss-leader`。
//!
//! 与 `accounts_api.rs` 复用 console 的 `control_write_status` 映射；这里验证真实
//! Raft follower 路径，而不是直接构造 `ControlError`。

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::http::StatusCode;
use oss_server::db::Db;
use oss_server::raft::control::ControlPlane;

const ACC: &str = "000000000001";

async fn connect_raft(dir: &std::path::Path, name: &str) -> Db {
    Db::connect_raft(&format!(
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_account_write_is_503_with_leader_hint() {
    let dir = tempfile::tempdir().unwrap();
    // 假 leader：只回应转发写，让 follower 把它当成 leader 1 但拒绝写入。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stub = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().fallback(|| async {
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"error": "not leader", "leader": 1})),
                )
            }),
        )
        .await
        .unwrap();
    });

    // node 1 是真实 leader，但对外 advertise 假地址；node 2 只负责跟随与转发。
    let a_db = connect_raft(dir.path(), "a").await;
    let mut a_settings = common::raft_harness::settings(dir.path(), 1, true);
    a_settings.feature_level = 3;
    a_settings.cluster_token = "accounts-follower-token".into();
    a_settings.advertise_endpoint = stub;
    let a = Arc::new(
        oss_server::raft::spawn_with_control(a_settings, Some(a_db.pool().clone()), None)
            .await
            .unwrap(),
    );
    common::raft_harness::wait_leader(&a).await;

    let mut b_settings = common::raft_harness::settings(dir.path(), 2, false);
    b_settings.feature_level = 3;
    b_settings.cluster_token = "accounts-follower-token".into();
    let b_db = connect_raft(dir.path(), "b").await;
    let b = common::raft_harness::spawn_node_with_pool(b_settings, Some(b_db.pool().clone())).await;
    oss_server::raft::membership::promote_voter(&a, 2, b.base.clone())
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

    // 本地管理员直接落库（绕过复制守卫；账户路由只用它做 who()）。
    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(b_db.pool())
        .await
        .unwrap();
    let uid = sqlx::query(
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
    st.control = Arc::new(ControlPlane::raft(
        Arc::clone(&b.handle),
        Arc::clone(&b_db),
        Arc::clone(&st.store),
        Arc::clone(&st.ap_cache),
        Arc::clone(&st.olap_cache),
        st.ap_region.clone(),
    ));
    let base = serve(oss_server::router::build_router(st).unwrap()).await;
    let resp = reqwest::Client::new()
        .delete(format!("{base}/api/v1/accounts/{ACC}"))
        .bearer_auth(token(uid))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
    assert_eq!(
        resp.headers()
            .get("x-oss-leader")
            .and_then(|v| v.to_str().ok()),
        Some("1"),
        "NotLeader 必须带 leader id 回退提示"
    );

    b.shutdown().await;
    a.shutdown().await;
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}
