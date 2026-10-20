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
                priority: 7,
                parent_job_id: Some(sys),
                report_bucket: Some("reports".into()),
                report_prefix: Some("out".into()),
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
    let own_detail: serde_json::Value = r.json().await.unwrap();
    // GET /jobs 列表投影须与详情同值（Web Jobs 页列）
    let list: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/jobs"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let item = list
        .iter()
        .find(|j| j["id"] == own)
        .expect("own job in list");
    assert_eq!(item["priority"], 7);
    assert_eq!(item["total"], 1);
    assert_eq!(item["succeeded"], 0);
    assert_eq!(item["failed"], 0);
    assert_eq!(item["requested_by"], sub_uid);
    assert_eq!(item["report_bucket"], "reports");
    assert_eq!(item["report_prefix"], "out");
    assert_eq!(item["parent_job_id"], sys);
    assert!(item["started_at"].is_null());
    assert!(item["finished_at"].is_null());
    for f in [
        "priority",
        "total",
        "succeeded",
        "failed",
        "requested_by",
        "report_bucket",
        "report_prefix",
        "started_at",
        "finished_at",
        "parent_job_id",
    ] {
        assert_eq!(item[f], own_detail["job"][f], "{f}");
    }
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
    assert!(st.olap_cache.get("ap-one").unwrap().is_some(), "缓存未更新");
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
    assert!(st.olap_cache.get("ap-one").unwrap().is_none());
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
    assert!(st.olap_cache.get("ap-two").unwrap().is_some());
    let r = client
        .delete(format!("{base}/api/v1/buckets/apb"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert!(st.ap_cache.get("ap-two").is_none());
    assert!(st.olap_cache.get("ap-two").unwrap().is_none());
    assert!(st.db.get_olap_config("ap-two").await.unwrap().is_none());
}

/// 预加载失败注入：`olap_configs` 不可读 → 缓存保持“未加载”（AP 请求 fail-closed）；
/// 恢复后再次预加载成功 → 置为已加载。
#[tokio::test]
async fn transform_preload_failure_keeps_cache_unloaded() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    assert!(st.olap_cache.is_loaded(), "启动预加载成功应置为已加载");

    sqlx::query("DROP TABLE olap_configs")
        .execute(st.db.pool())
        .await
        .unwrap();
    let cache = xl_s3::olap::OlapCache::new();
    oss_server::access_points_api::preload_olap(&st.db, &st.ap_cache, &cache).await;
    assert!(!cache.is_loaded(), "列表失败必须退回未加载（fail-closed）");

    sqlx::raw_sql(
        "CREATE TABLE olap_configs (
          access_point TEXT PRIMARY KEY REFERENCES access_points(name) ON DELETE CASCADE,
          kind TEXT NOT NULL CHECK (kind IN ('http', 'wasm')),
          endpoint TEXT, module BLOB, module_sha256 TEXT,
          timeout_ms INTEGER NOT NULL DEFAULT 5000,
          max_response_bytes INTEGER NOT NULL DEFAULT 67108864,
          updated_at INTEGER NOT NULL);",
    )
    .execute(st.db.pool())
    .await
    .unwrap();
    let cache = xl_s3::olap::OlapCache::new();
    oss_server::access_points_api::preload_olap(&st.db, &st.ap_cache, &cache).await;
    assert!(cache.is_loaded(), "加载成功后应恢复");
}

/// AP 缓存未命中时，预加载必须回查 DB 解析桶名（不能跳过变换器）。
#[tokio::test]
async fn transform_preload_resolves_bucket_when_ap_cache_misses() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db
        .create_access_point("ap-pre", "preb", 1, None, 0)
        .await
        .unwrap();
    st.db
        .set_olap_config(
            "ap-pre",
            "http",
            Some("http://example.com/"),
            None,
            None,
            5000,
            1 << 20,
            0,
        )
        .await
        .unwrap();

    // state() 之后才建 AP，故 st.ap_cache 不含它；再传一个全新空缓存模拟 AP 预加载失败。
    let empty_ap_cache = xl_s3::ap::ApCache::new();
    let cache = xl_s3::olap::OlapCache::new();
    oss_server::access_points_api::preload_olap(&st.db, &empty_ap_cache, &cache).await;
    assert!(cache.is_loaded());
    assert!(cache.get("ap-pre").unwrap().is_some(), "不能跳过该行");
    cache.invalidate_bucket("other");
    assert!(cache.get("ap-pre").unwrap().is_some());
    cache.invalidate_bucket("preb");
    assert!(cache.get("ap-pre").unwrap().is_none(), "桶名应从 DB 回查");
}

/// M8 S6 缺口 A：控制台对象标签路由——set→get→delete 往返、校验、404、非写者 403。
#[tokio::test]
async fn object_tags_console_routes() {
    use oss_server::db::PermissionRow;
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let bob_uid = st.db.create_user("bob", "pw", false).await.unwrap();
    st.store.create_bucket("tagb").await.unwrap();
    st.db.insert_bucket("tagb", None).await.unwrap();
    st.store
        .put_object("tagb", "a/b.txt", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    // bob 只有读权限（非写者）
    st.db
        .set_permissions(
            bob_uid,
            &[PermissionRow {
                bucket: "tagb".into(),
                read: true,
                write: false,
                delete: false,
            }],
        )
        .await
        .unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/v1/buckets/tagb/tags/a/b.txt");

    // 初始为空
    let v: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, serde_json::json!({"tags": []}));

    // set → get
    let r = client
        .put(&url)
        .bearer_auth(&admin)
        .json(&serde_json::json!({"tags": [
            {"key": "env", "value": "prod"}, {"key": "team", "value": "core"}
        ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let v: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        v["tags"],
        serde_json::json!([
            {"key": "env", "value": "prod"}, {"key": "team", "value": "core"}
        ])
    );

    // 非写者：读 200、写/删 403
    let r = client.get(&url).bearer_auth(&bob).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let r = client
        .put(&url)
        .bearer_auth(&bob)
        .json(&serde_json::json!({"tags": [{"key": "a", "value": "1"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "put");
    let r = client.delete(&url).bearer_auth(&bob).send().await.unwrap();
    assert_eq!(r.status(), 403, "delete");

    // 校验：>10 条 / 非法字符 → 400
    let many: Vec<serde_json::Value> = (0..11)
        .map(|i| serde_json::json!({"key": format!("k{i}"), "value": "v"}))
        .collect();
    let r = client
        .put(&url)
        .bearer_auth(&admin)
        .json(&serde_json::json!({"tags": many}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let r = client
        .put(&url)
        .bearer_auth(&admin)
        .json(&serde_json::json!({"tags": [{"key": "bad!", "value": "v"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // 缺失对象/桶 → 404（标签写不得凭空建对象）
    let r = client
        .get(format!("{base}/api/v1/buckets/tagb/tags/nope"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let r = client
        .put(format!("{base}/api/v1/buckets/tagb/tags/nope"))
        .bearer_auth(&admin)
        .json(&serde_json::json!({"tags": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let r = client
        .get(format!("{base}/api/v1/buckets/nope/tags/a"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // delete → 清空
    let r = client
        .delete(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let v: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, serde_json::json!({"tags": []}));
}

/// M8 S6 缺口 A：加密/KMS 只读状态——admin 200、非 admin 403、仅三个字段（无钥材料）。
#[tokio::test]
async fn encryption_status_is_admin_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("bob", "pw", false).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/v1/encryption");

    // 未接线（env 未配置）：两 false + null
    let v: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        v,
        serde_json::json!({"sse_s3": false, "sse_kms": false, "kms_current_key_id": null})
    );

    // 接线后：只回布尔与当前主密钥编号（k0），不含任何钥字节
    st.store
        .init_encryption(
            Some(dir.path().join("master.key")),
            Some(dir.path().join("kms")),
        )
        .await
        .unwrap();
    let v: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        v,
        serde_json::json!({"sse_s3": true, "sse_kms": true, "kms_current_key_id": "k0"})
    );

    // 非管理员 403
    let r = client.get(&url).bearer_auth(&bob).send().await.unwrap();
    assert_eq!(r.status(), 403);
}

// ---------- M8 S6 缺口 B：通知 CRUD / 死信 / Select 代理 / IAM 预设 ----------

/// 通知配置：GET/PUT/DELETE 往返（与 S3 面同源）、非法 ARN 400、>100 targets 400、非桶主 403。
#[tokio::test]
async fn notifications_console_crud() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("bob", "pw", false).await.unwrap();
    st.store.create_bucket("notb").await.unwrap();
    st.db.insert_bucket("notb", None).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/v1/buckets/notb/notifications");

    // 初始 = 空配置
    let got: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["queue_configurations"], serde_json::json!([]));

    // 不存在的桶：管理员也不得因 Acl::can 短路而拿到 200 空配置
    let r = client
        .get(format!("{base}/api/v1/buckets/nope/notifications"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // PUT 全量替换 → 204；GET 回读同形（ARN 由 target 回拼）
    let cfg = serde_json::json!({"queue_configurations": [{
        "id": "cfg-0",
        "queue_arn": "arn:oss:sqs::t1:kafka",
        "events": ["s3:ObjectCreated:Put"],
        "filter": {"key": {"filter_rules": [{"name": "prefix", "value": "logs/"}]}}
    }]});
    let r = client
        .put(&url)
        .bearer_auth(&admin)
        .json(&cfg)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let got: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        got["queue_configurations"][0]["queue_arn"],
        "arn:oss:sqs::t1:kafka"
    );
    assert_eq!(
        got["queue_configurations"][0]["events"],
        serde_json::json!(["s3:ObjectCreated:Put"])
    );
    // 与 S3 面同源：存的是归一化内部形态
    let stored = st.acl().bucket_notification("notb").await.unwrap().unwrap();
    assert!(stored.contains(r#""target":"t1""#), "{stored}");

    // 非桶主写 → 403
    let r = client
        .put(&url)
        .bearer_auth(&bob)
        .json(&cfg)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // 非法 ARN → 400
    let bad = serde_json::json!({"queue_configurations": [{
        "queue_arn": "arn:aws:sqs:us-east-1:1:q", "events": ["s3:ObjectCreated:Put"]
    }]});
    let r = client
        .put(&url)
        .bearer_auth(&admin)
        .json(&bad)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // >100 targets → 400
    let many: Vec<serde_json::Value> = (0..101)
        .map(|i| {
            serde_json::json!({
                "id": format!("c{i}"), "queue_arn": "arn:oss:sqs::t:kafka",
                "events": ["s3:ObjectCreated:Put"]
            })
        })
        .collect();
    let r = client
        .put(&url)
        .bearer_auth(&admin)
        .json(&serde_json::json!({"queue_configurations": many}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // DELETE → 清空
    let r = client
        .delete(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let got: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["queue_configurations"], serde_json::json!([]));
    assert!(
        st.acl()
            .bucket_notification("notb")
            .await
            .unwrap()
            .is_none()
    );
}

/// 死信：list 只回 state='dead'、字段取自事件载荷；re-deliver 复位为 pending 且不可重复；非 admin 403。
#[tokio::test]
async fn dead_letter_list_and_redeliver() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("bob", "pw", false).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;
    let client = reqwest::Client::new();

    let payload = serde_json::json!({
        "eventName": "s3:ObjectCreated:Put",
        "s3": {"object": {"key": "dir%2Fa+b.txt"}}
    })
    .to_string();
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO event_outbox (bucket, target_id, event_json, state, attempts, created_at, \
         last_error) VALUES ('bkt', 't1', ?, 'dead', 5, 123, 'boom') RETURNING id",
    )
    .bind(&payload)
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    // pending 行不得出现在死信列表
    sqlx::query(
        "INSERT INTO event_outbox (bucket, target_id, event_json, state) \
         VALUES ('bkt', 't1', '{}', 'pending')",
    )
    .execute(st.db.pool())
    .await
    .unwrap();

    let list: Vec<serde_json::Value> = client
        .get(format!("{base}/api/v1/events/dead?limit=10"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["id"], id);
    assert_eq!(list[0]["bucket"], "bkt");
    // 载荷里是 URL 编码（%2F / +）；列表展示解码后的键
    assert_eq!(list[0]["object_key"], "dir/a b.txt");
    assert_eq!(list[0]["event"], "s3:ObjectCreated:Put");
    assert_eq!(list[0]["attempts"], 5);
    assert_eq!(list[0]["last_error"], "boom");
    assert_eq!(list[0]["created_at"], 123);

    // limit 与 /audit 同口径钳位（0 → 1，超大 → 1000，不报错）
    for limit in ["0", "99999"] {
        let r = client
            .get(format!("{base}/api/v1/events/dead?limit={limit}"))
            .bearer_auth(&admin)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "limit={limit}");
    }

    // 非 admin 403（列表与重投均拒）
    let r = client
        .get(format!("{base}/api/v1/events/dead"))
        .bearer_auth(&bob)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = client
        .post(format!("{base}/api/v1/events/{id}/redeliver"))
        .bearer_auth(&bob)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // 重投：dead → pending，attempts/claimed_at/last_error 复位
    let r = client
        .post(format!("{base}/api/v1/events/{id}/redeliver"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let row: (String, i64, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT state, attempts, claimed_at, last_error FROM event_outbox WHERE id = ?",
    )
    .bind(id)
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(row, ("pending".to_string(), 0, None, None));

    // 二次重投（已不是 dead）→ 404
    let r = client
        .post(format!("{base}/api/v1/events/{id}/redeliver"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

/// Select 代理：CSV 过滤 → records/stats；非法 SQL 400；无读 403；缺对象 404；SSE-C 400；记录上限。
#[tokio::test]
async fn select_proxy_csv_filter_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("bob", "pw", false).await.unwrap();
    st.store.create_bucket("selb").await.unwrap();
    st.store
        .put_object(
            "selb",
            "data.csv",
            bytes::Bytes::from_static(b"name,age\nalice,30\nbob,20\n"),
            None,
        )
        .await
        .unwrap();
    // SSE-C 对象：控制台不保存客户密钥 → 400
    {
        use xl_store::crypto::{EncSpec, SseCKey};
        let stream: xl_store::store::ByteStream = Box::pin(futures::stream::once(async {
            Ok(bytes::Bytes::from_static(b"a\n"))
        }));
        st.store
            .put_object_stream_with_meta(
                "selb",
                "secret.csv",
                stream,
                None,
                Default::default(),
                None,
                false,
                Some(EncSpec::SseC(SseCKey::from_key([7u8; 32]))),
                None,
                None,
                None,
            )
            .await
            .unwrap();
    }
    // 10,001 行 → 越过控制台记录上限
    st.store
        .put_object(
            "selb",
            "many.csv",
            bytes::Bytes::from("x\n".repeat(10_001)),
            None,
        )
        .await
        .unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/v1/buckets/selb/select");
    let run = |token: &str, body: serde_json::Value| {
        client
            .post(&url)
            .bearer_auth(token.to_string())
            .json(&body)
            .send()
    };

    // CSV(header) → JSON 输出：过滤出 1 条，stats 按输入字节
    let body = serde_json::json!({
        "key": "data.csv",
        "expression": "SELECT s.name, s.age FROM S3Object s WHERE s.age > 25",
        "input": {"format": "CSV", "csv": {"file_header_info": "USE"}},
        "output": {"format": "JSON"}
    });
    let resp = run(&admin, body).await.unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        v["records"],
        serde_json::json!([{"age": "30", "name": "alice"}])
    );
    assert_eq!(v["stats"]["bytes_scanned"], 25);
    assert!(v["stats"]["bytes_returned"].as_i64().unwrap() > 0);

    // CSV 输出：records = 字段数组
    let body = serde_json::json!({
        "key": "data.csv",
        "expression": "SELECT * FROM S3Object",
        "input": {"format": "CSV", "csv": {"file_header_info": "IGNORE"}},
        "output": {"format": "CSV"}
    });
    let v: serde_json::Value = run(&admin, body).await.unwrap().json().await.unwrap();
    // IGNORE = 跳过首行表头
    assert_eq!(
        v["records"],
        serde_json::json!([["alice", "30"], ["bob", "20"]])
    );

    // quote_fields 未知值 → 400（不得静默降级为 ASNEEDED）
    let body = serde_json::json!({
        "key": "data.csv",
        "expression": "SELECT * FROM S3Object",
        "input": {"format": "CSV"},
        "output": {"format": "CSV", "csv": {"quote_fields": "SOMETIMES"}}
    });
    let r = run(&admin, body).await.unwrap();
    assert_eq!(r.status(), 400);

    // 非法 SQL → 400
    let body = serde_json::json!({
        "key": "data.csv", "expression": "DELETE FROM S3Object",
        "input": {"format": "CSV"}, "output": {"format": "CSV"}
    });
    let r = run(&admin, body).await.unwrap();
    assert_eq!(r.status(), 400);
    // 缺对象 → 404
    let body = serde_json::json!({
        "key": "nope.csv", "expression": "SELECT * FROM S3Object",
        "input": {"format": "CSV"}, "output": {"format": "CSV"}
    });
    let r = run(&admin, body).await.unwrap();
    assert_eq!(r.status(), 404);
    // 无读权限 → 403
    let body = serde_json::json!({
        "key": "data.csv", "expression": "SELECT * FROM S3Object",
        "input": {"format": "CSV"}, "output": {"format": "CSV"}
    });
    let r = run(&bob, body).await.unwrap();
    assert_eq!(r.status(), 403);
    // SSE-C 对象 → 400
    let body = serde_json::json!({
        "key": "secret.csv", "expression": "SELECT * FROM S3Object",
        "input": {"format": "CSV"}, "output": {"format": "CSV"}
    });
    let r = run(&admin, body).await.unwrap();
    assert_eq!(r.status(), 400);
    assert!(
        r.text().await.unwrap().contains("SSE-C"),
        "SSE-C 拒绝需给出可理解文案"
    );
    // 记录上限 → 400（不静默截断）
    let body = serde_json::json!({
        "key": "many.csv", "expression": "SELECT * FROM S3Object",
        "input": {"format": "CSV"}, "output": {"format": "CSV"}
    });
    let r = run(&admin, body).await.unwrap();
    assert_eq!(r.status(), 400);
    assert!(r.text().await.unwrap().contains("记录上限"));
}

/// IAM 预设：登录即可读、数量稳定；每个 doc 过 PolicyDoc::validate 且无匿名 Principal。
#[tokio::test]
async fn policy_templates_listing() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("bob", "pw", false).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/v1/policy-templates");

    for token in [&admin, &bob] {
        let r = client.get(&url).bearer_auth(token).send().await.unwrap();
        assert_eq!(r.status(), 200, "普通登录用户与管理员都可读");
        let list: Vec<serde_json::Value> = r.json().await.unwrap();
        let ids: Vec<&str> = list.iter().map(|t| t["id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["read-only", "read-write", "prefix-isolated"]);
        for t in &list {
            let doc: xl_s3::policy::PolicyDoc = serde_json::from_value(t["doc"].clone()).unwrap();
            doc.validate()
                .unwrap_or_else(|e| panic!("{}: {e}", t["id"]));
            for st in t["doc"]["Statement"].as_array().unwrap() {
                let bad = match &st["Principal"]["AWS"] {
                    serde_json::Value::String(s) => s == "*",
                    serde_json::Value::Array(a) => a.iter().any(|v| v == "*"),
                    _ => true,
                };
                assert!(!bad, "{} 不得含匿名 Principal", t["id"]);
                assert!(
                    !st["Action"].to_string().contains("Delete"),
                    "{} 不得含删除动作",
                    t["id"]
                );
            }
        }
    }
    // 匿名 → 401（所有 console 路由由 JwtLayer 统一挡）
    let r = client.get(&url).send().await.unwrap();
    assert_eq!(r.status(), 401);
}

/// Task 6a：无 `[[tiers]]` → `[]`；普通用户 403。
#[tokio::test]
async fn tiers_list_empty_and_admin_only() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.db.create_user("bob", "pw", false).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;
    let client = reqwest::Client::new();

    let r = client
        .get(format!("{base}/api/v1/tiers"))
        .bearer_auth(&bob)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403, "仅管理员可读");

    let v: serde_json::Value = client
        .get(format!("{base}/api/v1/tiers"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, serde_json::json!([]));
}

/// Task 6a：tier 只读状态——404 哨兵 = 远端可达（healthy），连接失败 = false；不投影凭证。
#[tokio::test]
async fn tiers_list_health_without_credentials() {
    use oss_server::config::TierTarget;

    // 任意请求 404：HEAD 哨兵得 404 → tier_head 返回 Ok(None) → healthy
    let mock = axum::Router::new().fallback(|| async { axum::http::StatusCode::NOT_FOUND });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

    let target = |id: &str, endpoint: &str, bucket: &str| TierTarget {
        id: id.into(),
        endpoint: endpoint.into(),
        bucket: bucket.into(),
        access_key: "ak-secret".into(),
        secret_key: "sk-secret".into(),
        region: "us-east-1".into(),
    };
    let dir = tempfile::tempdir().unwrap();
    let st = common::state_with_tiers(
        dir.path(),
        vec![
            target("cold", &format!("http://{addr}"), "tb"),
            target("dead", "http://127.0.0.1:1", "tb2"),
        ],
    )
    .await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let admin = login_token(&base).await;

    let v: Vec<serde_json::Value> = reqwest::Client::new()
        .get(format!("{base}/api/v1/tiers"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v.len(), 2);
    assert_eq!(v[0]["id"], "cold");
    assert_eq!(v[0]["endpoint"], format!("http://{addr}"));
    assert_eq!(v[0]["bucket"], "tb");
    assert_eq!(v[0]["healthy"], true, "404 哨兵 = 远端可达");
    assert_eq!(v[1]["id"], "dead");
    assert_eq!(v[1]["healthy"], false, "连接失败 = 不健康");
    for t in &v {
        assert!(t.get("access_key").is_none() && t.get("secret_key").is_none());
    }
}

/// Task 6a：桶默认加密控制台路由——读权限 GET、桶主/管理员写、404/400/403。
#[tokio::test]
async fn bucket_encryption_console_routes() {
    use oss_server::db::PermissionRow;

    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let bob_uid = st.db.create_user("bob", "pw", false).await.unwrap();
    st.store.create_bucket("encb").await.unwrap();
    st.db.insert_bucket("encb", None).await.unwrap();
    st.db
        .set_permissions(
            bob_uid,
            &[PermissionRow {
                bucket: "encb".into(),
                read: true,
                write: false,
                delete: false,
            }],
        )
        .await
        .unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let admin = login_token(&base).await;
    let bob = login_as(&base, "bob", "pw").await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/v1/buckets/encb/encryption");

    // 未配置 → null；有读权限的子账户可读
    let v: serde_json::Value = client
        .get(&url)
        .bearer_auth(&bob)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, serde_json::json!({"algorithm": null}));

    // PUT AES256 → 204 → GET 往返
    let r = client
        .put(&url)
        .bearer_auth(&admin)
        .json(&serde_json::json!({"algorithm": "AES256"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let v: serde_json::Value = client
        .get(&url)
        .bearer_auth(&bob)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["algorithm"], "AES256");

    // 未知算法 400
    let r = client
        .put(&url)
        .bearer_auth(&admin)
        .json(&serde_json::json!({"algorithm": "aws:kms:dsse"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // 非桶主（仅有读权限）写/删 403
    let r = client
        .put(&url)
        .bearer_auth(&bob)
        .json(&serde_json::json!({"algorithm": "aws:kms"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = client.delete(&url).bearer_auth(&bob).send().await.unwrap();
    assert_eq!(r.status(), 403);

    // DELETE → 204，回到 null
    let r = client
        .delete(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let v: serde_json::Value = client
        .get(&url)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["algorithm"], serde_json::Value::Null);

    // 缺桶 → GET/PUT/DELETE 均 404
    let missing = format!("{base}/api/v1/buckets/ghost/encryption");
    let r = client
        .get(&missing)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let r = client
        .put(&missing)
        .bearer_auth(&admin)
        .json(&serde_json::json!({"algorithm": "AES256"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let r = client
        .delete(&missing)
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

/// Task 6a：对象列表带存储类/归档阶段/恢复到期（缺失为 null），供 Objects 页免 N+1。
#[tokio::test]
async fn object_list_includes_storage_and_tier_fields() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("alpha").await.unwrap();
    st.store
        .put_object("alpha", "plain.txt", bytes::Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    st.store
        .put_object_stream_with_meta(
            "alpha",
            "ia.txt",
            Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from_static(b"ia"))
            })),
            None,
            Default::default(),
            None,
            false,
            None,
            None,
            None,
            Some("STANDARD_IA".into()),
        )
        .await
        .unwrap();
    st.store
        .put_object("alpha", "cold.txt", bytes::Bytes::from_static(b"c"), None)
        .await
        .unwrap();
    let info = st.store.head_object("alpha", "cold.txt").await.unwrap();
    st.store
        .archive_version(
            "alpha",
            "cold.txt",
            info.version_id.as_deref().unwrap_or("null"),
            xl_store::store::ArchiveSpec {
                backend: "cold".into(),
                key: "k".into(),
                size: info.size,
                source_etag: info.etag.clone(),
                etag: "remote".into(),
                sha256: "00".into(),
                class: "GLACIER".into(),
            },
            1,
        )
        .await
        .unwrap();

    let base = serve(build_router(st).unwrap()).await;
    let admin = login_token(&base).await;
    let v: serde_json::Value = reqwest::Client::new()
        .get(format!("{base}/api/v1/buckets/alpha/objects?limit=10"))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let objs = v["objects"].as_array().unwrap();
    let find = |key: &str| objs.iter().find(|o| o["key"] == key).unwrap();

    let plain = find("plain.txt");
    assert_eq!(plain["storage_class"], serde_json::Value::Null);
    assert_eq!(plain["tier_phase"], serde_json::Value::Null);
    assert_eq!(plain["restore_expires_at"], serde_json::Value::Null);

    let ia = find("ia.txt");
    assert_eq!(ia["storage_class"], "STANDARD_IA");
    assert_eq!(ia["tier_phase"], serde_json::Value::Null);

    let cold = find("cold.txt");
    assert_eq!(cold["storage_class"], "GLACIER");
    assert_eq!(cold["tier_phase"], "Archived");
    assert_eq!(cold["restore_expires_at"], serde_json::Value::Null);
}
