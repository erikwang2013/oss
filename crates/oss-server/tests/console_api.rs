mod common;

use common::state;
use oss_server::router::build_router;

/// 起真实监听端口的服务，返回 base URL（新用例共用；Step 1 块内两个测试保持原样）。
async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// 用 common::state 的固定口令建的用户换一个 token（用例需先 create_user）。
async fn login_token(base: &str) -> String {
    login_as(base, "erik", "pw").await
}

async fn login_as(base: &str, username: &str, password: &str) -> String {
    let login: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": username, "password": password}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    login["token"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn login_then_list_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("alpha").await.unwrap();
    let app = build_router(st).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");

    // 未带 token 必须 401
    let resp = reqwest::get(format!("{base}/api/v1/buckets"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    let client = reqwest::Client::new();
    let login: serde_json::Value = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = login["token"].as_str().unwrap();

    let buckets: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // 权限体系起形状带能力位（管理员全 true）
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["name"], "alpha");
    assert_eq!(buckets[0]["can_write"], true);
    assert_eq!(buckets[0]["can_delete"], true);
}

#[tokio::test]
async fn wrong_password_is_401() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let app = build_router(st).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "WRONG"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    // /login 在 JwtLayer 的 except 名单外是反的：它在名单内，401 由 handler 自己给，
    // 保持纯文本（登录页直接把 body 当错误文案展示）；不是 jwt-rust 的 JSON 401 形态。
    assert_eq!(resp.text().await.unwrap(), "用户名或密码错误");
}

/// 钉 `register_upload` 的三分支口径（I-1，收尾轮修订）：命中 204、无行 404、DB 故障 500。
/// 本用例覆盖前两支；Err→500 见下方 `register_upload_db_failure_is_500`。
#[tokio::test]
async fn register_upload_hit_is_204_miss_is_404() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db
        .record_upload(
            "file_202610_abc.txt",
            "files",
            "abc.txt",
            "file",
            7,
            "etag1",
        )
        .await
        .unwrap();
    let app = build_router(st).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");

    let client = reqwest::Client::new();
    let login: serde_json::Value = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = login["token"].as_str().unwrap();

    let resp = client
        .post(format!("{base}/api/v1/uploads/register"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "saved_path": "file_202610_abc.txt", "original_name": "my.txt"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let uploads: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/uploads"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(uploads[0]["original_name"], "my.txt");

    let resp = client
        .post(format!("{base}/api/v1/uploads/register"))
        .bearer_auth(token)
        .json(&serde_json::json!({"saved_path": "nope.txt", "original_name": "my.txt"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// register Err→500（微调轮裁决）：DB 故障在 HTTP 面可构造——经池 drop 掉 uploads 表。
#[tokio::test]
async fn register_upload_db_failure_is_500() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    sqlx::query("DROP TABLE uploads")
        .execute(st.db.pool())
        .await
        .unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let token = login_token(&base).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/uploads/register"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"saved_path": "a.txt", "original_name": "b.txt"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);
    assert_eq!(
        resp.text().await.unwrap(),
        "internal error",
        "5xx 不泄内部细节"
    );
}

// ---- Task 15 收尾轮（I-1 / M-2 / M-3 / M-4）新增 ----

/// I-1②：12 条 method-route 去掉 login = 11 条，匿名一律 401 —— 鉴权是整面 `JwtLayer`（不是逐 handler
/// 的 `auth()`），路由表变化时本表跟着同步即可。POST 带可解析 body：401 现在来自层
/// （先于提取器），但仍钉住「匿名不给任何 handler 机会」。
#[tokio::test]
async fn all_routes_reject_anonymous() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve(build_router(state(dir.path()).await).unwrap()).await;
    let client = reqwest::Client::new();

    let cases: Vec<(reqwest::Method, &str, Option<serde_json::Value>)> = vec![
        (reqwest::Method::GET, "/api/v1/buckets", None),
        (
            reqwest::Method::POST,
            "/api/v1/buckets",
            Some(serde_json::json!({"name": "alpha"})),
        ),
        (reqwest::Method::DELETE, "/api/v1/buckets/alpha", None),
        (reqwest::Method::GET, "/api/v1/buckets/alpha/objects", None),
        (
            reqwest::Method::GET,
            "/api/v1/buckets/alpha/objects/k.txt",
            None,
        ),
        (
            reqwest::Method::DELETE,
            "/api/v1/buckets/alpha/objects/k.txt",
            None,
        ),
        (reqwest::Method::GET, "/api/v1/uploads", None),
        (
            reqwest::Method::POST,
            "/api/v1/uploads/register",
            Some(serde_json::json!({"saved_path": "a", "original_name": "b"})),
        ),
        (reqwest::Method::GET, "/api/v1/access-keys", None),
        (
            reqwest::Method::POST,
            "/api/v1/access-keys",
            Some(serde_json::json!({})),
        ),
        (reqwest::Method::GET, "/api/v1/groups", None),
        // 子账户与权限（管理员面；这里只验「无 token 一律 401」）
        (reqwest::Method::GET, "/api/v1/me", None),
        (
            reqwest::Method::POST,
            "/api/v1/me/password",
            Some(serde_json::json!({"old_password": "x", "new_password": "pw12345678"})),
        ),
        (reqwest::Method::GET, "/api/v1/users", None),
        (
            reqwest::Method::POST,
            "/api/v1/users",
            Some(serde_json::json!({"username": "sub", "password": "pw12345678"})),
        ),
        (reqwest::Method::DELETE, "/api/v1/users/999", None),
        (
            reqwest::Method::POST,
            "/api/v1/users/999/password",
            Some(serde_json::json!({"password": "pw12345678"})),
        ),
        (
            reqwest::Method::PUT,
            "/api/v1/users/999/status",
            Some(serde_json::json!({"active": false})),
        ),
        (reqwest::Method::GET, "/api/v1/users/999/permissions", None),
        (
            reqwest::Method::PUT,
            "/api/v1/users/999/permissions",
            Some(serde_json::json!([])),
        ),
    ];
    assert_eq!(cases.len(), 20, "路由表变化时同步此表（21 条 - login）");

    for (method, path, body) in cases {
        let mut req = client.request(method.clone(), format!("{base}{path}"));
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.unwrap();
        assert_eq!(resp.status(), 401, "{method} {path} 未带 token 必须 401");
    }
}

#[derive(serde::Serialize)]
struct TestClaims {
    sub: String,
    exp: usize,
}

fn sign_token(secret: &str, exp_secs: i64) -> String {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &TestClaims {
            sub: "1".into(),
            exp: exp_secs as usize,
        },
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// I-1①：换 secret 签发的 token（签名不过）与正确 secret 但已过期的 token，都必须 401。
#[tokio::test]
async fn forged_or_expired_token_is_401() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve(build_router(state(dir.path()).await).unwrap()).await;
    let client = reqwest::Client::new();

    let forged = sign_token("not-the-secret", unix_now() + 3600);
    // exp 早于 now 3600s，越过 jwt-rust 的 leeway（0，源头是 JwtConfig 默认值）
    let expired = sign_token(common::JWT_SECRET, unix_now() - 3600);

    for token in [forged, expired] {
        let resp = client
            .get(format!("{base}/api/v1/buckets"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
    }
}

/// M-2：状态码分档 —— NoSuchBucket → 404、InvalidBucketName → 400、BucketNotEmpty → 409。
#[tokio::test]
async fn store_error_status_codes() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("alpha").await.unwrap();
    st.store
        .put_object("alpha", "keep.txt", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let token = login_token(&base).await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("{base}/api/v1/buckets/missing"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "删不存在的桶");

    let resp = client
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"name": "Bad_Name"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "非法桶名");

    let resp = client
        .delete(format!("{base}/api/v1/buckets/alpha"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409, "删非空桶");
}

/// M-3：`?limit=&cursor=`（spec §0.3）透传翻页；响应带 `next_cursor`。
#[tokio::test]
async fn objects_pagination_follows_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("alpha").await.unwrap();
    st.store
        .put_object("alpha", "a.txt", bytes::Bytes::from_static(b"a"), None)
        .await
        .unwrap();
    st.store
        .put_object("alpha", "b.txt", bytes::Bytes::from_static(b"b"), None)
        .await
        .unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let token = login_token(&base).await;
    let client = reqwest::Client::new();

    let page1: serde_json::Value = client
        .get(format!("{base}/api/v1/buckets/alpha/objects?limit=1"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page1["objects"].as_array().unwrap().len(), 1);
    assert_eq!(page1["objects"][0]["key"], "a.txt");
    assert_eq!(page1["is_truncated"], true);
    let cursor = page1["next_cursor"]
        .as_str()
        .expect("截断时必须带 next_cursor");
    assert_eq!(page1["next_token"], page1["next_cursor"], "新老字段同值");

    let page2: serde_json::Value = client
        .get(format!(
            "{base}/api/v1/buckets/alpha/objects?limit=1&cursor={cursor}"
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page2["objects"].as_array().unwrap().len(), 1);
    assert_eq!(page2["objects"][0]["key"], "b.txt");
}

/// Task 7（M1b）：上传面板分组下拉的数据源 —— `oss.toml` 的 `[[groups]]` 原样透出。
#[tokio::test]
async fn list_groups_returns_configured_groups() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await; // common::state() 里已配 file → files
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let token = login_token(&base).await;

    let groups: serde_json::Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/groups"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(
        groups,
        serde_json::json!([{"group": "file", "bucket": "files", "max_size": 0, "extensions": []}])
    );
}

/// M-4：自定 AK/SK 的最小校验（400 自己的文案）与 UNIQUE 冲突说人话（不泄 sqlite 原文）。
#[tokio::test]
async fn access_key_validation_and_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let token = login_token(&base).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/v1/access-keys");

    let short = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({"access_key": "short", "secret_key": "s3cr3t-1234567890"}))
        .send()
        .await
        .unwrap();
    assert_eq!(short.status(), 400, "AK 短于 16");
    let text = short.text().await.unwrap();
    assert!(text.contains("access key 非法"), "自己的文案: {text}");

    let bad_chars = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "access_key": "AKIA_1234567890!!!!", "secret_key": "s3cr3t-1234567890abcdef"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_chars.status(), 400, "字符集外");

    let ok = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "access_key": "AKIA1234567890ABCD", "secret_key": "s3cr3t-1234567890abcdef"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);

    let keys: Vec<String> = client
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(keys.contains(&"AKIA1234567890ABCD".to_string()));

    let dup = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "access_key": "AKIA1234567890ABCD", "secret_key": "another-secret-1234567890"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(dup.status(), 400, "重复 AK");
    let text = dup.text().await.unwrap();
    assert!(text.contains("access key 已存在"), "文案: {text}");
    assert!(
        !text.contains("UNIQUE") && !text.contains("sqlite"),
        "不泄底层原文: {text}"
    );
}

/// M6 切片 0（P2-1 复查补）：控制台删**不存在**的对象 → 204 且不发事件。
#[tokio::test]
async fn console_delete_missing_emits_no_event() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let tok = login_token(&base).await;
    st.store.create_bucket("cmb").await.unwrap();
    st.db.insert_bucket("cmb", None).await.unwrap();
    sqlx::query("INSERT INTO event_configs (bucket, config_json) VALUES ('cmb', ?)")
        .bind(
            r#"[{"id":"a","target":"t1","events":["s3:ObjectRemoved:Delete"],"prefix":null,"suffix":null}]"#,
        )
        .execute(st.db.pool())
        .await
        .unwrap();
    let resp = reqwest::Client::new()
        .delete(format!("{base}/api/v1/buckets/cmb/objects/never.txt"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204, "空删幂等 204");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_outbox WHERE bucket='cmb'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    assert_eq!(n, 0, "空删不得发事件");
}

/// M6 切片 0（R5/P2-4）：控制台删对象发 `s3:ObjectRemoved:Delete`（source=oss-console）。
#[tokio::test]
async fn console_delete_emits_event() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let tok = login_token(&base).await;
    st.store.create_bucket("cdb").await.unwrap();
    st.db.insert_bucket("cdb", None).await.unwrap();
    st.store
        .put_object("cdb", "d.txt", bytes::Bytes::from_static(b"d"), None)
        .await
        .unwrap();
    sqlx::query("INSERT INTO event_configs (bucket, config_json) VALUES ('cdb', ?)")
        .bind(
            r#"[{"id":"a","target":"t1","events":["s3:ObjectRemoved:Delete"],"prefix":null,"suffix":null}]"#,
        )
        .execute(st.db.pool())
        .await
        .unwrap();

    // M6 切片 4（V3②）：控制台硬删同样入复制队列（无源版本号）
    sqlx::query(
        "INSERT INTO replication_rules (bucket, rules_json) VALUES ('cdb', ?) \
         ON CONFLICT(bucket) DO UPDATE SET rules_json=excluded.rules_json",
    )
    .bind(r#"[{"id":"r","prefix":null,"remote_id":"r1","dest_bucket":"dst","delete_marker":true,"enabled":true}]"#)
    .execute(st.db.pool())
    .await
    .unwrap();

    let resp = reqwest::Client::new()
        .delete(format!("{base}/api/v1/buckets/cdb/objects/d.txt"))
        .bearer_auth(tok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE bucket='cdb' AND is_delete_marker=1 AND version_id=''",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(n, 1, "控制台硬删应入复制队列");

    let json: String = sqlx::query_scalar("SELECT event_json FROM event_outbox WHERE bucket='cdb'")
        .fetch_one(st.db.pool())
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["eventName"], "s3:ObjectRemoved:Delete");
    assert_eq!(v["source"]["userAgent"], "oss-console");
    assert_eq!(v["s3"]["object"]["key"], "d.txt");
}

/// M8 S0：作业详情/控制 API——admin 全量、子账户仅自己的作业、状态操作。
#[tokio::test]
async fn jobs_api_ownership_and_actions() {
    use oss_server::db::{JobSpec, NewJobTask};
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let sub_uid = st.db.create_user("sub", "pw2", false).await.unwrap();
    // 系统作业（requested_by=None）：仅 admin 可见
    let sys = st
        .db
        .create_job(
            &JobSpec {
                kind: "batch".into(),
                tasks: vec![NewJobTask {
                    action: "Copy".into(),
                    payload: "{}".into(),
                    dedupe_key: None,
                }],
                ..Default::default()
            },
            1000,
        )
        .await
        .unwrap();
    // 子账户自己的作业
    let own = st
        .db
        .create_job(
            &JobSpec {
                kind: "batch".into(),
                requested_by: Some(sub_uid),
                tasks: vec![NewJobTask {
                    action: "Copy".into(),
                    payload: "{}".into(),
                    dedupe_key: None,
                }],
                ..Default::default()
            },
            1000,
        )
        .await
        .unwrap();
    let app = build_router(st.clone()).unwrap();
    let client = reqwest::Client::new();
    let base_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = base_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(base_listener, app).await.unwrap() });
    let base = format!("http://{addr}");
    let admin = login_token(&base).await;
    let sub: serde_json::Value = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "sub", "password": "pw2"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sub = sub["token"].as_str().unwrap().to_string();

    // admin 看系统作业；子账户 403
    let r = client
        .get(format!("{base}/api/v1/jobs/{sys}"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["job"]["id"], sys);
    assert_eq!(body["tasks"].as_array().unwrap().len(), 1);
    let r = client
        .get(format!("{base}/api/v1/jobs/{sys}"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // 子账户自己的作业可读；先领取一条任务 → pause/resume 可用
    let r = client
        .get(format!("{base}/api/v1/jobs/{own}"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // 取消系统作业，确保下一条领取落在 own 上
    st.db.job_set_state(sys, "cancel", 1001).await.unwrap();
    let claimed = st.db.claim_task(1001, 900).await.unwrap().unwrap();
    assert_eq!(claimed.job_id, own);
    for action in ["pause", "resume", "cancel"] {
        let r = client
            .post(format!("{base}/api/v1/jobs/{own}/{action}"))
            .bearer_auth(&sub)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 204, "{action}");
    }
    // 取消后 retry-failed 无失败任务 → 409
    let r = client
        .post(format!("{base}/api/v1/jobs/{own}/retry-failed"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    // 未授权动作/不存在作业
    let r = client
        .post(format!("{base}/api/v1/jobs/{own}/explode"))
        .bearer_auth(&sub)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let r = client
        .get(format!("{base}/api/v1/jobs/999999"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

/// M8 S1：访问点 CRUD/策略 API（owner/admin、严格策略 400、缓存同步）。
#[tokio::test]
async fn access_points_api_crud() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("apb").await.unwrap();
    st.db.insert_bucket("apb", None).await.unwrap();
    let app = build_router(st.clone()).unwrap();
    let client = reqwest::Client::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");
    let tok = login_token(&base).await;

    // 建 AP（带策略）
    let r = client
        .post(format!("{base}/api/v1/access-points"))
        .bearer_auth(&tok)
        .json(&serde_json::json!({
            "name": "ap-one", "bucket": "apb",
            "policy": {"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
                "Action":"s3:GetObject","Resource":"arn:aws:s3:::apb/*"}]}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    assert!(st.ap_cache.get("ap-one").is_some(), "缓存未更新");
    // 重名 409
    let r = client
        .post(format!("{base}/api/v1/access-points"))
        .bearer_auth(&tok)
        .json(&serde_json::json!({"name": "ap-one", "bucket": "apb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    // 非法名 400
    let r = client
        .post(format!("{base}/api/v1/access-points"))
        .bearer_auth(&tok)
        .json(&serde_json::json!({"name": "Bad Name", "bucket": "apb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    // 列表/详情
    let r = client
        .get(format!("{base}/api/v1/access-points"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let rows: serde_json::Value = r.json().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    let r = client
        .get(format!("{base}/api/v1/access-points/ap-one"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // 坏策略（Condition）→ 400
    let r = client
        .put(format!("{base}/api/v1/access-points/ap-one/policy"))
        .bearer_auth(&tok)
        .json(
            &serde_json::json!({"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
            "Action":"s3:GetObject","Resource":"arn:x","Condition":{}}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    // 合法清策略（null）→ 204
    let r = client
        .put(format!("{base}/api/v1/access-points/ap-one/policy"))
        .bearer_auth(&tok)
        .json(&serde_json::json!(null))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    // 删除 → 404 + 缓存移除
    let r = client
        .delete(format!("{base}/api/v1/access-points/ap-one"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(st.ap_cache.get("ap-one").is_none());
    let r = client
        .get(format!("{base}/api/v1/access-points/ap-one"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

/// M8 S2：变换器配置 API（owner/admin、参数校验、缓存同步、删 AP/删桶级联）。
#[tokio::test]
async fn access_point_transform_api() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("bob", "pw", false).await.unwrap();
    st.store.create_bucket("apb").await.unwrap();
    st.db.insert_bucket("apb", None).await.unwrap();
    let app = build_router(st.clone()).unwrap();
    let client = reqwest::Client::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;

    let r = client
        .post(format!("{base}/api/v1/access-points"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({"name": "ap-one", "bucket": "apb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    // 未配置 → 404
    let r = client
        .get(format!("{base}/api/v1/access-points/ap-one/transform"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    // 非桶主非管理员 → 403
    let r = client
        .put(format!("{base}/api/v1/access-points/ap-one/transform"))
        .bearer_auth(&bob)
        .json(&serde_json::json!({"kind": "http", "endpoint": "http://example.com/x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    // 未知字段由 serde 拒绝（axum JSON 数据错误 → 422）
    let r = client
        .put(format!("{base}/api/v1/access-points/ap-one/transform"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({"kind": "http", "endpoint": "http://example.com/x", "extra": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 422);
    // 参数校验：kind、kind 与字段组合、URL 形状、限额
    for bad in [
        serde_json::json!({"kind": "ftp", "endpoint": "http://example.com/x"}),
        serde_json::json!({"kind": "http", "endpoint": "http://example.com/x", "module_b64": "KG1vZHVsZSk="}),
        serde_json::json!({"kind": "http"}),
        serde_json::json!({"kind": "wasm", "endpoint": "http://example.com/x", "module_b64": "KG1vZHVsZSk="}),
        serde_json::json!({"kind": "wasm"}),
        serde_json::json!({"kind": "http", "endpoint": "ftp://example.com/x"}),
        serde_json::json!({"kind": "http", "endpoint": "http://u:p@example.com/x"}),
        serde_json::json!({"kind": "http", "endpoint": "http://example.com/x", "timeout_ms": 10}),
        serde_json::json!({"kind": "http", "endpoint": "http://example.com/x", "max_response_bytes": 0}),
    ] {
        let r = client
            .put(format!("{base}/api/v1/access-points/ap-one/transform"))
            .bearer_auth(&admin)
            .json(&bad)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400, "{bad}");
    }
    // 合法保存 → 204 + 缓存/DB 同步；GET 只回摘要（不回模块字节）
    let r = client
        .put(format!("{base}/api/v1/access-points/ap-one/transform"))
        .bearer_auth(&admin)
        .json(
            &serde_json::json!({"kind": "http", "endpoint": "http://example.com/x",
            "timeout_ms": 2000, "max_response_bytes": 1024}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(st.olap_cache.get("ap-one").is_some(), "缓存未更新");
    let row = st.db.get_olap_config("ap-one").await.unwrap().unwrap();
    assert_eq!(row.kind, "http");
    let r = client
        .get(format!("{base}/api/v1/access-points/ap-one/transform"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let cfg: serde_json::Value = r.json().await.unwrap();
    assert_eq!(cfg["kind"], "http");
    assert_eq!(cfg["endpoint"], "http://example.com/x");
    assert_eq!(cfg["timeout_ms"], 2000);
    assert_eq!(cfg["max_response_bytes"], 1024);
    assert!(cfg["module_sha256"].is_null());
    assert!(cfg["module_size"].is_null());
    // WASM：默认关闭时保存即拒；开启 feature 时合法空模块可编译（运行期导出缺失另行报错）
    let r = client
        .put(format!("{base}/api/v1/access-points/ap-one/transform"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({"kind": "wasm", "module_b64": "KG1vZHVsZSk="}))
        .send()
        .await
        .unwrap();
    #[cfg(not(feature = "olap-wasm"))]
    assert_eq!(r.status(), 400);
    #[cfg(feature = "olap-wasm")]
    assert_eq!(r.status(), 204);
    // 删 AP → 两缓存 + DB 行一并清
    let r = client
        .delete(format!("{base}/api/v1/access-points/ap-one"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(st.olap_cache.get("ap-one").is_none());
    assert!(st.db.get_olap_config("ap-one").await.unwrap().is_none());
    // 删桶级联：AP/变换器行与两缓存都失效
    let r = client
        .post(format!("{base}/api/v1/access-points"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({"name": "ap-two", "bucket": "apb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let r = client
        .put(format!("{base}/api/v1/access-points/ap-two/transform"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({"kind": "http", "endpoint": "http://example.com/y"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(st.olap_cache.get("ap-two").is_some());
    let r = client
        .delete(format!("{base}/api/v1/buckets/apb"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(st.ap_cache.get("ap-two").is_none());
    assert!(st.olap_cache.get("ap-two").is_none());
    assert!(st.db.get_olap_config("ap-two").await.unwrap().is_none());
}
