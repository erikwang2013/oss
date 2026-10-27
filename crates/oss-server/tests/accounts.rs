//! AC-0/AC-1/AC-2 账户控制面集成测试（standalone、in-memory SQLite）。
//!
//! 覆盖：管理员专属 CRUD/配额、账户管理员账户内自治与跨账户隔离、停用账户登录/存量 JWT
//! 失效、桶数配额在建桶入口生效、软字节配额用量只告警不阻写、以及「账户管理员不能挂载
//! 已有用户/提权/停用全局管理员」。

mod common;

use common::state;
use oss_server::router::build_router;
use reqwest::Method;
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::{Digest, Sha256};

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn login(base: &str, username: &str, password: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({ "username": username, "password": password }))
        .send()
        .await
        .unwrap()
}

async fn login_ok(base: &str, username: &str, password: &str) -> String {
    let resp = login(base, username, password).await;
    assert_eq!(resp.status(), 200, "登录 {username} 应成功");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["token"].as_str().unwrap().to_string()
}

/// 建好全局管理员 erik 并起服务（返回 tempdir 保活）。
async fn serve_with_admin() -> (tempfile::TempDir, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let admin = login_ok(&base, "erik", "pw").await;
    (dir, base, admin)
}

async fn create_account(base: &str, token: &str, name: &str, quota: Option<i64>) -> String {
    let body = match quota {
        Some(q) => serde_json::json!({ "name": name, "bucket_quota": q }),
        None => serde_json::json!({ "name": name }),
    };
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/accounts"))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "创建账户 {name}");
    let body: serde_json::Value = resp.json().await.unwrap();
    let id = body["id"].as_str().unwrap().to_string();
    assert!(
        id.len() == 12 && id.chars().all(|c| c.is_ascii_digit()),
        "账户 id 应为 12 位十进制，实得 {id}"
    );
    id
}

async fn create_console_user(base: &str, token: &str, username: &str) -> i64 {
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/users"))
        .bearer_auth(token)
        .json(&serde_json::json!({ "username": username, "password": format!("{username}-password") }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "创建用户 {username}");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["id"].as_i64().unwrap()
}

async fn account_users(base: &str, token: &str, account: &str, username: &str) -> i64 {
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/accounts/{account}/users"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "username": username,
            "password": format!("{username}-password")
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "账户内创建用户 {username}");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["id"].as_i64().unwrap()
}

async fn set_member_active(base: &str, token: &str, account: &str, uid: i64, active: bool) -> u16 {
    reqwest::Client::new()
        .put(format!(
            "{base}/api/v1/accounts/{account}/members/{uid}/status"
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({ "active": active }))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn members(base: &str, token: &str, account: &str) -> Vec<serde_json::Value> {
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/accounts/{account}/members"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.json().await.unwrap()
}

/// AC-1：账户 CRUD/配额/启停仅全局管理员；系统账户不可删/不可挂人；空账户可删。
#[tokio::test]
async fn account_crud_is_admin_only_and_legacy_account_is_protected() {
    let (_dir, base, admin) = serve_with_admin().await;
    let client = reqwest::Client::new();
    let bob_uid = create_console_user(&base, &admin, "bob").await;
    let bob = login_ok(&base, "bob", "bob-password").await;

    let id = create_account(&base, &admin, "team-a", Some(2)).await;

    let row: serde_json::Value = client
        .get(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(row["name"], "team-a");
    assert_eq!(row["status"], "active");
    assert_eq!(row["bucket_quota"], 2);
    assert_eq!(row["member_count"], 0);
    assert_eq!(row["bucket_count"], 0);

    let rows: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/accounts"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        rows.iter().any(|r| r["id"] == id.as_str()),
        "账户列表含新账户"
    );

    // 非管理员：写一律 403；不可见非成员账户
    let cases = [
        (
            "POST",
            format!("{base}/api/v1/accounts"),
            Some(serde_json::json!({ "name": "nope" })),
        ),
        (
            "PUT",
            format!("{base}/api/v1/accounts/{id}/quota"),
            Some(serde_json::json!({ "bucket_quota": 5 })),
        ),
        (
            "PUT",
            format!("{base}/api/v1/accounts/{id}"),
            Some(serde_json::json!({ "status": "disabled" })),
        ),
        ("DELETE", format!("{base}/api/v1/accounts/{id}"), None),
        (
            "POST",
            format!("{base}/api/v1/accounts/{id}/users"),
            Some(serde_json::json!({ "username": "x", "password": "x-password" })),
        ),
    ];
    for (method, url, body) in cases {
        let mut req = match method {
            "POST" => client.post(&url),
            "PUT" => client.put(&url),
            _ => client.delete(&url),
        }
        .bearer_auth(&bob);
        if let Some(b) = body {
            req = req.json(&b);
        }
        assert_eq!(req.send().await.unwrap().status(), 403, "{method} {url}");
    }
    let resp = client
        .get(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&bob)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "非成员不可读账户");
    let visible: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/accounts"))
        .bearer_auth(&bob)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(visible.len(), 1, "普通用户只看到自己的派生账户");
    assert_eq!(
        visible[0]["id"],
        xl_s3::access::account_id_of(bob_uid).unwrap()
    );

    // 边界校验
    let bad_quota = client
        .put(format!("{base}/api/v1/accounts/{id}/quota"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "bucket_quota": -1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_quota.status(), 400);
    let bad_status = client
        .put(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "status": "frozen" }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_status.status(), 400);
    let empty_name = client
        .post(format!("{base}/api/v1/accounts"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "name": "  " }))
        .send()
        .await
        .unwrap();
    assert_eq!(empty_name.status(), 400);

    // 系统账户：不可删、不可挂成员
    let legacy_delete = client
        .delete(format!(
            "{base}/api/v1/accounts/{}",
            xl_s3::access::LEGACY_ACCOUNT
        ))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(legacy_delete.status(), 400);
    let legacy_attach = client
        .post(format!(
            "{base}/api/v1/accounts/{}/members",
            xl_s3::access::LEGACY_ACCOUNT
        ))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "uid": bob_uid, "role": "member" }))
        .send()
        .await
        .unwrap();
    assert_eq!(legacy_attach.status(), 400);

    // 空账户可删；删除后 404
    let empty = create_account(&base, &admin, "empty", None).await;
    let deleted = client
        .delete(format!("{base}/api/v1/accounts/{empty}"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let gone = client
        .get(format!("{base}/api/v1/accounts/{empty}"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 404);

    // 有成员的账户不可删
    client
        .post(format!("{base}/api/v1/accounts/{id}/members"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "uid": bob_uid, "role": "member" }))
        .send()
        .await
        .unwrap();
    let conflict = client
        .delete(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), 409);
}

/// AC-2：账户桶数硬配额在建桶入口生效（console 面），管理员调高配额后立即放行。
#[tokio::test]
async fn account_bucket_quota_is_enforced_on_bucket_create() {
    let (_dir, base, admin) = serve_with_admin().await;
    let client = reqwest::Client::new();
    let id = create_account(&base, &admin, "quota", Some(1)).await;
    account_users(&base, &admin, &id, "carol").await;
    let carol = login_ok(&base, "carol", "carol-password").await;

    let first = client
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&carol)
        .json(&serde_json::json!({ "name": "q-one" }))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 204);

    let second = client
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&carol)
        .json(&serde_json::json!({ "name": "q-two" }))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 403);
    assert_eq!(second.text().await.unwrap(), "账户桶配额已用尽");

    let row: serde_json::Value = client
        .get(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&carol)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(row["bucket_count"], 1, "配额计数按成员当前账户聚合");

    let raised = client
        .put(format!("{base}/api/v1/accounts/{id}/quota"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "bucket_quota": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(raised.status(), 204);

    let retry = client
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&carol)
        .json(&serde_json::json!({ "name": "q-two" }))
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), 204, "提高配额后建桶放行");
}

/// AC-1：停用账户 → 登录被拒、存量 JWT 立即失效；重新启用后恢复。
#[tokio::test]
async fn disabling_account_blocks_login_and_existing_tokens() {
    let (_dir, base, admin) = serve_with_admin().await;
    let client = reqwest::Client::new();
    let id = create_account(&base, &admin, "freeze", None).await;
    account_users(&base, &admin, &id, "dave").await;
    let dave = login_ok(&base, "dave", "dave-password").await;
    assert_eq!(
        client
            .get(format!("{base}/api/v1/buckets"))
            .bearer_auth(&dave)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let disabled = client
        .put(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "status": "disabled" }))
        .send()
        .await
        .unwrap();
    assert_eq!(disabled.status(), 204);

    assert_eq!(
        login(&base, "dave", "dave-password").await.status(),
        401,
        "停用账户成员不能登录"
    );
    assert_eq!(
        client
            .get(format!("{base}/api/v1/buckets"))
            .bearer_auth(&dave)
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "存量 JWT 立即失效"
    );

    let enabled = client
        .put(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "status": "active" }))
        .send()
        .await
        .unwrap();
    assert_eq!(enabled.status(), 204);
    let dave2 = login_ok(&base, "dave", "dave-password").await;
    assert_eq!(
        client
            .get(format!("{base}/api/v1/buckets"))
            .bearer_auth(&dave2)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

/// 全局管理员不是任何账户的成员（Task 1 v2 回填起），也没有 uid 派生账户行：
/// 其派生地址不存在，因此无从停用（404）；防锁死由「管理员永不进 account_members」
/// 这一不变量保证，而非账户号上的条件更新。
#[tokio::test]
async fn account_with_active_global_admin_cannot_be_disabled() {
    let (_dir, base, admin) = serve_with_admin().await;
    // 首个用户 id=1 是全局管理员：v2 后不再有派生账户行。
    let admin_account = xl_s3::access::account_id_of(1).unwrap();
    let resp = reqwest::Client::new()
        .put(format!("{base}/api/v1/accounts/{admin_account}"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "status": "disabled" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// AC-1：账户管理员只能在本账户内建/列/停 member；跨账户一律 403；
/// 不能挂载已有用户、不能提权、不能停用自己/同级/全局管理员。
#[tokio::test]
async fn account_manager_is_confined_and_cannot_escalate() {
    let (_dir, base, admin) = serve_with_admin().await;
    let client = reqwest::Client::new();
    let a = create_account(&base, &admin, "team-a", None).await;
    let b = create_account(&base, &admin, "team-b", None).await;
    let mgr_uid = create_console_user(&base, &admin, "mgr").await;
    let attached = client
        .post(format!("{base}/api/v1/accounts/{a}/members"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "uid": mgr_uid, "role": "admin" }))
        .send()
        .await
        .unwrap();
    assert_eq!(attached.status(), 204);
    let mgr = login_ok(&base, "mgr", "mgr-password").await;

    // 账户列表只含本账户
    let visible: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/accounts"))
        .bearer_auth(&mgr)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0]["id"], a.as_str());

    // 跨账户读写 403
    for req in [
        client.get(format!("{base}/api/v1/accounts/{b}")),
        client.get(format!("{base}/api/v1/accounts/{b}/members")),
        client
            .post(format!("{base}/api/v1/accounts/{b}/users"))
            .json(&serde_json::json!({ "username": "intruder", "password": "intruder-pw" })),
        client
            .post(format!("{base}/api/v1/accounts/{b}/members"))
            .json(&serde_json::json!({ "uid": mgr_uid, "role": "admin" })),
    ] {
        assert_eq!(
            req.bearer_auth(&mgr).send().await.unwrap().status(),
            403,
            "账户管理员不得跨账户"
        );
    }
    // 全局账户写仍 403
    assert_eq!(
        client
            .post(format!("{base}/api/v1/accounts"))
            .bearer_auth(&mgr)
            .json(&serde_json::json!({ "name": "mgr-own" }))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .put(format!("{base}/api/v1/accounts/{a}/quota"))
            .bearer_auth(&mgr)
            .json(&serde_json::json!({ "bucket_quota": 100 }))
            .send()
            .await
            .unwrap()
            .status(),
        403,
        "配额仅全局管理员"
    );

    // 本账户内可建用户，成员恒为 member + 非全局管理员
    let worker_uid = account_users(&base, &mgr, &a, "worker").await;
    let rows = members(&base, &mgr, &a).await;
    let worker = rows.iter().find(|r| r["uid"] == worker_uid).unwrap();
    assert_eq!(worker["role"], "member");
    assert_eq!(worker["is_admin"], false);
    assert!(
        rows.iter()
            .any(|r| r["uid"] == mgr_uid && r["role"] == "admin")
    );

    // 普通 member 只读成员表，不能管理
    let worker = login_ok(&base, "worker", "worker-password").await;
    assert_eq!(
        client
            .get(format!("{base}/api/v1/accounts/{a}/members"))
            .bearer_auth(&worker)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{base}/api/v1/accounts/{a}/users"))
            .bearer_auth(&worker)
            .json(&serde_json::json!({ "username": "x2", "password": "x2-password" }))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        set_member_active(&base, &worker, &a, mgr_uid, false).await,
        403
    );

    // 账户管理员不能挂载已有用户（防拉人提权）
    assert_eq!(
        client
            .post(format!("{base}/api/v1/accounts/{a}/members"))
            .bearer_auth(&mgr)
            .json(&serde_json::json!({ "uid": worker_uid, "role": "admin" }))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    // 不能停用自己
    assert_eq!(
        set_member_active(&base, &mgr, &a, mgr_uid, false).await,
        403
    );

    // 可停用 member（存量 JWT 立即失效），也可恢复
    assert_eq!(
        set_member_active(&base, &mgr, &a, worker_uid, false).await,
        204
    );
    assert_eq!(
        login(&base, "worker", "worker-password").await.status(),
        401
    );
    assert_eq!(
        client
            .get(format!("{base}/api/v1/buckets"))
            .bearer_auth(&worker)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        set_member_active(&base, &mgr, &a, worker_uid, true).await,
        204
    );

    // 同级 manager 不可互停：把 worker 升为账户 admin 后，mgr 停 worker → 403
    let promoted = client
        .post(format!("{base}/api/v1/accounts/{a}/members"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "uid": worker_uid, "role": "admin" }))
        .send()
        .await
        .unwrap();
    assert_eq!(promoted.status(), 204);
    assert_eq!(
        set_member_active(&base, &mgr, &a, worker_uid, false).await,
        403
    );

    // 全局管理员不是账户成员：挂载被拒（404），对非成员停用同样是 404。
    let attach_admin = client
        .post(format!("{base}/api/v1/accounts/{a}/members"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "uid": 1, "role": "member" }))
        .send()
        .await
        .unwrap();
    assert_eq!(attach_admin.status(), 404);
    assert_eq!(set_member_active(&base, &mgr, &a, 1, false).await, 404);

    // 不存在的成员 → 404
    assert_eq!(
        set_member_active(&base, &mgr, &a, 999_999, false).await,
        404
    );
}

/// AC-1：停用账户后其账户管理员不能再建用户（账户状态在写入口同样生效）。
#[tokio::test]
async fn disabled_account_rejects_manager_writes() {
    let (_dir, base, admin) = serve_with_admin().await;
    let id = create_account(&base, &admin, "frozen2", None).await;
    let mgr_uid = create_console_user(&base, &admin, "mgr2").await;
    reqwest::Client::new()
        .post(format!("{base}/api/v1/accounts/{id}/members"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "uid": mgr_uid, "role": "admin" }))
        .send()
        .await
        .unwrap();
    let mgr = login_ok(&base, "mgr2", "mgr2-password").await;

    let resp = reqwest::Client::new()
        .put(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "status": "disabled" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 停用账户的 manager 的 JWT 已失效 → 401（先于业务校验）
    assert_eq!(
        reqwest::Client::new()
            .post(format!("{base}/api/v1/accounts/{id}/users"))
            .bearer_auth(&mgr)
            .json(&serde_json::json!({ "username": "late", "password": "late-password" }))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
}

/// 回归：写路径改走 ControlCommand 后仍保持旧 API 的 400 文案
/// （重复用户名、停用账户）与缺失账户 404。
#[tokio::test]
async fn account_write_rejections_keep_legacy_status_and_message() {
    let (_dir, base, admin) = serve_with_admin().await;
    let id = create_account(&base, &admin, "compat", None).await;
    let client = reqwest::Client::new();
    let create_user = |username: &str| {
        client
            .post(format!("{base}/api/v1/accounts/{id}/users"))
            .bearer_auth(&admin)
            .json(&serde_json::json!({ "username": username, "password": "compat-password" }))
            .send()
    };

    let first = create_user("dup").await.unwrap();
    assert_eq!(first.status(), 200);
    let uid = first.json::<serde_json::Value>().await.unwrap()["id"]
        .as_i64()
        .unwrap();

    let dup = create_user("dup").await.unwrap();
    assert_eq!(dup.status(), 400);
    assert_eq!(dup.text().await.unwrap(), "用户名已存在");

    let disabled = client
        .put(format!("{base}/api/v1/accounts/{id}"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "status": "disabled" }))
        .send()
        .await
        .unwrap();
    assert_eq!(disabled.status(), 204);

    // 全局管理员写停用账户：入口保留旧 400 文案（apply 仍是权威）。
    for (path, body) in [
        (
            format!("/api/v1/accounts/{id}/users"),
            serde_json::json!({ "username": "late", "password": "late-password" }),
        ),
        (
            format!("/api/v1/accounts/{id}/members"),
            serde_json::json!({ "uid": uid, "role": "member" }),
        ),
    ] {
        let resp = client
            .post(format!("{base}{path}"))
            .bearer_auth(&admin)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "{path}");
        assert_eq!(resp.text().await.unwrap(), "账户已停用", "{path}");
    }

    // 缺失账户仍 404。
    for (path, body) in [
        (
            "/api/v1/accounts/000000000099/users".to_string(),
            serde_json::json!({ "username": "ghost", "password": "ghost-password" }),
        ),
        (
            "/api/v1/accounts/000000000099/members".to_string(),
            serde_json::json!({ "uid": uid, "role": "member" }),
        ),
    ] {
        assert_eq!(
            client
                .post(format!("{base}{path}"))
                .bearer_auth(&admin)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            404,
            "{path}"
        );
    }
}

// ---------- AC-2：软字节配额用量（只读、只告警） ----------

fn amz_now() -> AmzDate {
    let t = time::OffsetDateTime::now_utc();
    let s = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    AmzDate::parse(&s).unwrap()
}

/// 最小 SigV4 header 签名 PUT（数据面真实写入口；不引生产签名器）。
async fn s3_put(base: &str, path: &str, ak: &str, sk: &str, body: &[u8]) -> reqwest::Response {
    const REGION: &str = "us-east-1";
    let url = format!("{base}{path}");
    let host = url
        .split("://")
        .nth(1)
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_string();
    let payload_hex = hex::encode(Sha256::digest(body));
    let amz = amz_now();
    let amz_iso = amz.fmt_iso8601().to_string();
    let amz_date = amz.fmt_date().to_string();
    let to_sign = [
        ("host".to_string(), host),
        ("x-amz-content-sha256".to_string(), payload_hex.clone()),
        ("x-amz-date".to_string(), amz_iso.clone()),
    ];
    let refs: Vec<(&str, &str)> = to_sign
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let canonical = create_canonical_request(
        Method::PUT.as_str(),
        path,
        &[] as &[(&str, &str)],
        &refs,
        Payload::SingleChunk(&payload_hex),
    );
    let sts = create_string_to_sign(&canonical, &amz, REGION, "s3");
    let sig = calculate_signature(&sts, sk, &amz, REGION, "s3");
    let names: Vec<&str> = to_sign.iter().map(|(k, _)| k.as_str()).collect();
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={ak}/{amz_date}/{REGION}/s3/aws4_request, \
         SignedHeaders={}, Signature={sig}",
        names.join(";")
    );
    let req = reqwest::Client::new()
        .put(&url)
        .header("x-amz-content-sha256", &payload_hex)
        .header("x-amz-date", amz_iso)
        .header("authorization", auth)
        .body(body.to_vec())
        .build()
        .unwrap();
    reqwest::Client::new().execute(req).await.unwrap()
}

async fn account_usage(base: &str, token: &str, id: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{base}/api/v1/accounts/{id}/usage"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
}

fn quota_header(resp: &reqwest::Response) -> Option<String> {
    resp.headers()
        .get("x-oss-account-soft-quota")
        .map(|v| v.to_str().unwrap().to_string())
}

/// 两个桶的当前对象字节求和、配额未超/超限/none 的 `exceeded` 与响应头、
/// 非成员 403 / 缺失账户 404，以及**超限后真实 S3 PUT 仍成功**（不阻写）。
#[tokio::test]
async fn soft_byte_quota_usage_is_advisory_and_never_blocks_put() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let probe = st.clone();
    let base = serve(build_router(st).unwrap()).await;
    let admin = login_ok(&base, "erik", "pw").await;
    let client = reqwest::Client::new();

    let id = create_account(&base, &admin, "soft", None).await;
    let erin_uid = account_users(&base, &admin, &id, "erin").await;
    let erin = login_ok(&base, "erin", "erin-password").await;
    for name in ["soft-one", "soft-two"] {
        let resp = client
            .post(format!("{base}/api/v1/buckets"))
            .bearer_auth(&erin)
            .json(&serde_json::json!({ "name": name }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 204, "建桶 {name}");
    }

    // 两个桶、含嵌套键：3 + 5 + 7 = 15 字节。
    probe
        .store
        .put_object("soft-one", "a.txt", bytes::Bytes::from_static(b"abc"), None)
        .await
        .unwrap();
    probe
        .store
        .put_object(
            "soft-one",
            "nested/b.bin",
            bytes::Bytes::from_static(b"12345"),
            None,
        )
        .await
        .unwrap();
    probe
        .store
        .put_object(
            "soft-two",
            "c.bin",
            bytes::Bytes::from_static(b"1234567"),
            None,
        )
        .await
        .unwrap();

    // 未配配额：真实用量照报，exceeded=false，且不带告警头。
    let resp = account_usage(&base, &erin, &id).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(quota_header(&resp), None, "soft_byte_quota=NULL 不带头");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["account_id"], id.as_str());
    assert_eq!(body["used_bytes"], 15);
    assert_eq!(body["bucket_count"], 2);
    assert!(body["soft_byte_quota"].is_null());
    assert_eq!(body["exceeded"], false);

    // 配额 10 < 15：超限。
    sqlx::query("UPDATE accounts SET soft_byte_quota = ? WHERE id = ?")
        .bind(10_i64)
        .bind(&id)
        .execute(probe.db.pool())
        .await
        .unwrap();
    let resp = account_usage(&base, &erin, &id).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(quota_header(&resp).as_deref(), Some("exceeded"));
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["used_bytes"], 15);
    assert_eq!(body["soft_byte_quota"], 10);
    assert_eq!(body["exceeded"], true);

    // 超限后真实 S3 PUT 仍成功（软配额只告警，绝不阻写）。
    probe
        .db
        .create_access_key(
            "AKIDSOFTQUOTA0000001",
            "sk-soft-quota-00000000001",
            erin_uid,
        )
        .await
        .unwrap();
    let put = s3_put(
        &base,
        "/soft-one/after-quota.txt",
        "AKIDSOFTQUOTA0000001",
        "sk-soft-quota-00000000001",
        b"xy",
    )
    .await;
    assert_eq!(put.status(), 200, "软配额超限后 PUT 必须成功");
    let resp = account_usage(&base, &erin, &id).await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["used_bytes"], 17, "超限后写入计入用量");
    assert_eq!(body["exceeded"], true);

    // 提高配额到 100：未超限，头为 ok。
    sqlx::query("UPDATE accounts SET soft_byte_quota = ? WHERE id = ?")
        .bind(100_i64)
        .bind(&id)
        .execute(probe.db.pool())
        .await
        .unwrap();
    let resp = account_usage(&base, &erin, &id).await;
    assert_eq!(quota_header(&resp).as_deref(), Some("ok"));
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["exceeded"], false);

    // 非成员 403；缺失账户 404（管理员与非成员同口径）。
    create_console_user(&base, &admin, "bob2").await;
    let bob = login_ok(&base, "bob2", "bob2-password").await;
    assert_eq!(account_usage(&base, &bob, &id).await.status(), 403);
    for token in [&admin, &bob] {
        assert_eq!(
            account_usage(&base, token, "000000000099").await.status(),
            404
        );
    }
}

/// 分页边界：单桶 >1000 键必须翻页累加（端点固定 max_keys=1000），再跨两个桶求和。
#[tokio::test]
async fn soft_byte_quota_usage_paginates_past_1000_keys() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let probe = st.clone();
    let base = serve(build_router(st).unwrap()).await;
    let admin = login_ok(&base, "erik", "pw").await;
    let client = reqwest::Client::new();

    let id = create_account(&base, &admin, "page", None).await;
    account_users(&base, &admin, &id, "frank").await;
    let frank = login_ok(&base, "frank", "frank-password").await;
    for name in ["page-one", "page-two"] {
        let resp = client
            .post(format!("{base}/api/v1/buckets"))
            .bearer_auth(&frank)
            .json(&serde_json::json!({ "name": name }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 204, "建桶 {name}");
    }

    // 1001 个 1 字节对象 → 第一页 1000 + 第二页 1。
    for i in 0..1001 {
        probe
            .store
            .put_object(
                "page-one",
                &format!("k{i:04}"),
                bytes::Bytes::from_static(b"x"),
                None,
            )
            .await
            .unwrap();
    }
    probe
        .store
        .put_object("page-two", "tail", bytes::Bytes::from_static(b"yy"), None)
        .await
        .unwrap();

    let resp = account_usage(&base, &frank, &id).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["used_bytes"], 1003);
    assert_eq!(body["bucket_count"], 2);
}
