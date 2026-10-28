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
use oss_server::raft::{ControlCommand, ControlResponse, RPC_TOKEN_HEADER};

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
/// item 10 残余：成员挂载/角色变更在本地滞后时，require_manager 同样必须追赶后判定
/// （旧代码会把刚挂载的 manager 误判 403），真正非 manager 追赶后仍 403。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catch_up_covers_member_attach_and_role_checks() {
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

    // §14.3 同类残余：leader 已提交 用户创建 + 成员挂载、follower 本地仍滞后时，
    // 停用该成员不能凭本地 member/user miss 误 404；追赶后按最新行判定并转发成功。
    let mut lock = b_db.pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *lock)
        .await
        .unwrap();
    assert_eq!(
        a.handle
            .write(ControlCommand::UserCreate {
                username: "erin".into(),
                password_hash: oss_server::db::hash_password("erin-password"),
                status: "active".into(),
                created_at: 12,
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    assert_eq!(
        a.handle
            .write(ControlCommand::AccountMemberAttach {
                account_id: ACC.into(),
                username: "erin".into(),
                role: "member".into(),
                created_at: 13,
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    let erin_uid = admin_uid + 2;
    assert!(b_db.get_user(erin_uid).await.unwrap().is_none());
    assert!(b_db.member_role(ACC, erin_uid).await.unwrap().is_none());

    let req = {
        let http = http.clone();
        let url = format!("{base}/api/v1/accounts/{ACC}/members/{erin_uid}/status");
        let tok = token(admin_uid);
        tokio::spawn(async move {
            http.put(url)
                .bearer_auth(tok)
                .json(&serde_json::json!({ "active": false }))
                .send()
                .await
        })
    };
    // 旧代码此刻已凭本地 miss 返回 404；放行 apply 后应追赶成功。
    tokio::time::sleep(Duration::from_millis(500)).await;
    sqlx::query("ROLLBACK").execute(&mut *lock).await.unwrap();
    drop(lock);
    let resp = req.await.unwrap().unwrap();
    assert_eq!(resp.status(), 204, "滞后 follower 的成员停用必须追上后成功");
    assert_eq!(
        b_db.get_user(erin_uid).await.unwrap().map(|u| u.2),
        Some(false),
        "停用已按 leader 命令 apply 到 follower"
    );

    // 真正不存在的 uid 停用仍 404。
    let missing_status = http
        .put(format!(
            "{base}/api/v1/accounts/{ACC}/members/999999/status"
        ))
        .bearer_auth(token(admin_uid))
        .json(&serde_json::json!({ "active": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_status.status(), 404);

    // item 10：leader 已提交成员挂载为 admin（manager）后，follower 本地成员行滞后。
    // 先把 frank 的用户行追上（who() 只读本地），再只让那条挂载日志卡在写锁后。
    assert_eq!(
        a.handle
            .write(ControlCommand::UserCreate {
                username: "frank".into(),
                password_hash: oss_server::db::hash_password("frank-password"),
                status: "active".into(),
                created_at: 14,
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
    let frank_uid = admin_uid + 3;
    assert_eq!(
        b_db.get_user(frank_uid).await.unwrap().map(|u| u.0),
        Some("frank".to_string())
    );

    let mut lock = b_db.pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *lock)
        .await
        .unwrap();
    assert_eq!(
        a.handle
            .write(ControlCommand::AccountMemberAttach {
                account_id: ACC.into(),
                username: "frank".into(),
                role: "admin".into(),
                created_at: 15,
            })
            .await
            .unwrap(),
        ControlResponse::Ok
    );
    assert!(b_db.member_role(ACC, frank_uid).await.unwrap().is_none());

    let req = {
        let http = http.clone();
        let url = format!("{base}/api/v1/accounts/{ACC}/users");
        let tok = token(frank_uid);
        tokio::spawn(async move {
            http.post(url)
                .bearer_auth(tok)
                .json(&serde_json::json!({
                    "username": "grace",
                    "password": "grace-password"
                }))
                .send()
                .await
        })
    };
    // 旧代码此刻已凭本地缺行返回 403；放行 apply 后应追赶成功。
    tokio::time::sleep(Duration::from_millis(500)).await;
    sqlx::query("ROLLBACK").execute(&mut *lock).await.unwrap();
    drop(lock);
    let resp = req.await.unwrap().unwrap();
    assert_eq!(
        resp.status(),
        200,
        "滞后 follower 的 manager 权限判定必须追上后放行（不能 403）"
    );

    // 真正非 manager（dave 本地角色仍为 member）：追赶后不让行，保持 403。
    let denied = http
        .post(format!("{base}/api/v1/accounts/{ACC}/users"))
        .bearer_auth(token(target_uid))
        .json(&serde_json::json!({
            "username": "heidi",
            "password": "heidi-password"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403, "追赶后仍非 manager 必须拒绝");

    b.shutdown().await;
    a.shutdown().await;
}

fn bucket_create(name: &str) -> ControlCommand {
    ControlCommand::BucketCreate {
        name: name.into(),
        owner: Some("alice".into()),
        created_at: 100,
        replication_mode: "async".into(),
        versioning: "Off".into(),
        object_lock: false,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        default_encryption: None,
    }
}

/// 2-voter 集群：node 1 leader、node 2 follower（均带真实控制库）。
async fn two_node_quota_cluster(
    dir: &std::path::Path,
) -> (
    common::raft_harness::TestNode,
    common::raft_harness::TestNode,
) {
    let a_db = connect_raft(dir, "quota-a").await;
    let mut sa = common::raft_harness::settings(dir, 1, true);
    sa.feature_level = 3;
    let a = common::raft_harness::spawn_node_with_pool(sa, Some(a_db.pool().clone())).await;
    common::raft_harness::wait_leader(&a.handle).await;

    let b_db = connect_raft(dir, "quota-b").await;
    let mut sb = common::raft_harness::settings(dir, 2, false);
    sb.feature_level = 3;
    let b = common::raft_harness::spawn_node_with_pool(sb, Some(b_db.pool().clone())).await;
    oss_server::raft::membership::promote_voter(&a.handle, 2, b.base.clone())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while b.handle.raft().metrics().borrow().current_leader != Some(1) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader 1 not observed on follower"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (a, b)
}

async fn seed_quota_account(a: &common::raft_harness::TestNode, quota: i64) {
    for cmd in [
        ControlCommand::AccountCreate {
            id: ACC.into(),
            name: "acme".into(),
            bucket_quota: Some(quota),
            created_at: 10,
        },
        ControlCommand::UserCreate {
            username: "alice".into(),
            password_hash: oss_server::db::hash_password("alice-password"),
            status: "active".into(),
            created_at: 11,
        },
        ControlCommand::AccountMemberAttach {
            account_id: ACC.into(),
            username: "alice".into(),
            role: "member".into(),
            created_at: 12,
        },
    ] {
        assert_eq!(
            a.handle.write(cmd).await.unwrap(),
            ControlResponse::Ok,
            "seed command"
        );
    }
}

async fn wait_bucket_count(pool: &sqlx::SqlitePool, want: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let got: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets")
            .fetch_one(pool)
            .await
            .unwrap();
        if got == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "bucket count {got} != {want}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn post_write(
    http: &reqwest::Client,
    base: &str,
    token: &str,
    cmd: ControlCommand,
) -> (u16, serde_json::Value) {
    let resp = http
        .post(format!("{base}/api/v1/site/raft/write"))
        .header(RPC_TOKEN_HEADER, token)
        .json(&cmd)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap())
}

/// 绕过复制守卫改 follower 本地副本（模拟混版窗口里落后的本地配额列）。
async fn set_follower_quota(pool: &sqlx::SqlitePool, quota: Option<i64>) {
    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE accounts SET bucket_quota = ? WHERE id = ?")
        .bind(quota)
        .bind(ACC)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
        .execute(pool)
        .await
        .unwrap();
}

/// AC-2 修订：经 follower `/site/raft/write` 转发的并发建桶由 **leader** 在 append 前
/// 串行判定——quota=N 时 N+1 个恰好 N 成功、1×403；两端桶表一致，无第 N+1 个桶。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_forwarded_bucket_creates_stop_at_leader_quota() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = two_node_quota_cluster(dir.path()).await;
    seed_quota_account(&a, 2).await;
    let idx = a.handle.applied_index();
    assert!(
        b.handle.wait_applied(idx, Duration::from_secs(10)).await,
        "follower 追上账户播种"
    );

    let http = reqwest::Client::new();
    let mut tasks = Vec::new();
    for i in 0..3 {
        let http = http.clone();
        let b_base = b.base.clone();
        let token = b.token.clone();
        tasks.push(tokio::spawn(async move {
            post_write(&http, &b_base, &token, bucket_create(&format!("f-{i}"))).await
        }));
    }
    let (mut ok, mut quota) = (0, 0);
    for t in tasks {
        match t.await.unwrap() {
            (200, body) => {
                assert_eq!(body["response"], "Ok");
                ok += 1;
            }
            (403, body) => {
                assert_eq!(body["error"], "quota_exceeded");
                quota += 1;
            }
            other => panic!("unexpected write response: {other:?}"),
        }
    }
    assert_eq!((ok, quota), (2, 1), "N+1 并发恰好 N 成功、1 拒绝");

    let a_pool = a.pool.as_ref().unwrap();
    let b_pool = b.pool.as_ref().unwrap();
    wait_bucket_count(a_pool, 2).await;
    wait_bucket_count(b_pool, 2).await;
    let names = |pool: &sqlx::SqlitePool| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT name FROM buckets ORDER BY name")
                .fetch_all(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(
        names(a_pool).await,
        names(b_pool).await,
        "两端状态机桶表必须一致"
    );

    a.shutdown().await;
    b.shutdown().await;
}

/// 混版回归：follower 只转发、不用本地副本判配额。follower 本地配额分别改成
/// 0（会误拒）与 NULL（会误放），都以 leader 的判定为准；apply 不做配额，桶表不分叉。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_stale_local_quota_does_not_decide() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = two_node_quota_cluster(dir.path()).await;
    seed_quota_account(&a, 1).await;
    let idx = a.handle.applied_index();
    assert!(
        b.handle.wait_applied(idx, Duration::from_secs(10)).await,
        "follower 追上账户播种"
    );
    let a_pool = a.pool.as_ref().unwrap();
    let b_pool = b.pool.as_ref().unwrap();
    let http = reqwest::Client::new();

    // follower 本地配额 0（陈旧/落后）→ 若本地判定会 403；leader 有额度，应放行。
    set_follower_quota(b_pool, Some(0)).await;
    let (status, body) = post_write(&http, &b.base, &b.token, bucket_create("stale-allow")).await;
    assert_eq!(status, 200, "leader 有额度必须放行：{body}");
    wait_bucket_count(a_pool, 1).await;
    wait_bucket_count(b_pool, 1).await;

    // follower 本地不限（与 leader 的 quota=1 相反）→ 若本地判定会误放；leader 已满应 403。
    set_follower_quota(b_pool, None).await;
    let (status, body) = post_write(&http, &b.base, &b.token, bucket_create("stale-deny")).await;
    assert_eq!(status, 403, "leader 配额已满必须拒绝：{body}");
    assert_eq!(body["error"], "quota_exceeded");
    wait_bucket_count(a_pool, 1).await;
    wait_bucket_count(b_pool, 1).await;

    a.shutdown().await;
    b.shutdown().await;
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}
