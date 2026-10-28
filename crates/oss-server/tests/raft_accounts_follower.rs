//! follower 上的账户写：转发到假 leader 得到 409 → HTTP 503 + `x-oss-leader`；
//! 以及 users 副本滞后的 follower 上 attach_member 的追赶重试（§14.3 残余）。
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
use oss_server::raft::{ControlCommand, ControlResponse};

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

/// §14.3 残余：follower 的 users 副本落后（leader 已提交 UserCreate，本地 apply 被
/// 写锁挡住）时，attach_member 不能凭本地 miss 直接 404；等本地 apply 追上 leader
/// 已应用点后重试一次并成功。真正不存在的 uid 仍 404。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attach_member_on_lagging_follower_waits_for_catch_up() {
    let dir = tempfile::tempdir().unwrap();

    // 真实 2-voter 集群：node 1 为 leader（带真实 advertise/HTTP），node 2 只跟随。
    let a_db = connect_raft(dir.path(), "lag-a").await;
    let mut a_settings = common::raft_harness::settings(dir.path(), 1, true);
    a_settings.feature_level = 3;
    let a = common::raft_harness::spawn_node_with_pool(a_settings, Some(a_db.pool().clone())).await;
    common::raft_harness::wait_leader(&a.handle).await;

    let b_db = connect_raft(dir.path(), "lag-b").await;
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

    // 本地管理员直接落库（只用于 who()），绕过复制守卫。
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

    // 账户先在 leader 提交并追上 follower，确保后面只差用户行。
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
    let a_applied = {
        let m = a.handle.raft().metrics();
        m.borrow().last_applied.map(|l| l.index).unwrap_or(0)
    };
    assert!(
        b.handle
            .wait_applied(a_applied, Duration::from_secs(10))
            .await
    );

    // 拿住 follower 控制库的写锁：之后 leader 提交的用户日志到达也 apply 不了。
    let mut lock = b_db.pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *lock)
        .await
        .unwrap();
    assert_eq!(
        a.handle
            .write(ControlCommand::UserCreate {
                username: "dave".into(),
                password_hash: oss_server::db::hash_password("dave-password"),
                status: "active".into(),
                created_at: 11,
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    // follower 本地：admin=1 → dave 将是 2；leader 上 dave=1（uid 跨节点不稳定，
    // 所以命令只能带 username，本地校验必须等本地副本追上）。
    let target_uid = admin_uid + 1;
    assert!(b_db.get_user(target_uid).await.unwrap().is_none());

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

    let http = reqwest::Client::new();
    let req = {
        let http = http.clone();
        let url = format!("{base}/api/v1/accounts/{ACC}/members");
        let tok = token(admin_uid);
        tokio::spawn(async move {
            http.post(url)
                .bearer_auth(tok)
                .json(&serde_json::json!({ "uid": target_uid, "role": "member" }))
                .send()
                .await
        })
    };
    // 给 handler 足够时间做第一次本地查询（旧代码此刻已返回 404），再放行 apply。
    tokio::time::sleep(Duration::from_millis(500)).await;
    sqlx::query("ROLLBACK").execute(&mut *lock).await.unwrap();
    drop(lock);
    let resp = req.await.unwrap().unwrap();
    assert_eq!(resp.status(), 204, "滞后 follower 的 attach 必须追上后成功");

    let member: Option<i64> = sqlx::query_scalar(
        "SELECT am.uid FROM account_members am JOIN users u ON u.id = am.uid \
         WHERE am.account_id = ? AND u.username = 'dave'",
    )
    .bind(ACC)
    .fetch_optional(b_db.pool())
    .await
    .unwrap();
    assert_eq!(member, Some(target_uid), "成员行按本地 uid 落库");

    // 真正不存在的 uid 仍 404（barrier 已完成，无长等待）。
    let missing = http
        .post(format!("{base}/api/v1/accounts/{ACC}/members"))
        .bearer_auth(token(admin_uid))
        .json(&serde_json::json!({ "uid": 999_999, "role": "member" }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    b.shutdown().await;
    a.shutdown().await;
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}
