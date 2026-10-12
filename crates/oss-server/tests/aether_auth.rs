// /aetherupload/* 的最小鉴权层（规格 §0.1「会话 Cookie/JWT 覆盖层」）：
// cookie `oss_token` 或 `Authorization: Bearer` 二者皆可，缺/坏/过期一律 401。
// 实现在 router.rs：cookie→Authorization 垫片（最外层）+ jwt-rust JwtLayer（校验）。
// 路由表变化时，本文件的「匿名 401 表」要跟着同步（四条路由全在两个层之内）。

mod common;

use common::state;
use oss_server::router::build_router;

/// 起真实监听端口的服务，返回 base URL。
async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

const BOUNDARY: &str = "X-AETHER-AUTH-TEST";

/// 给请求补上 preprocess 的 multipart 文本字段。手拼报文：reqwest 的 dev features 没开
/// multipart，为一条测试换算依赖特性（动 Cargo.lock）不划算。
fn with_form(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let mut body = String::new();
    for (k, v) in [
        ("resource_name", "a.txt"),
        ("resource_size", "11"),
        ("group", "file"),
        ("resource_hash", "5eb63bbbe01eeed093cb22bb8f5acdc3"),
    ] {
        body.push_str(&format!(
            "--{BOUNDARY}\r\ncontent-disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n"
        ));
    }
    body.push_str(&format!("--{BOUNDARY}--\r\n"));
    req.header(
        "content-type",
        format!("multipart/form-data; boundary={BOUNDARY}"),
    )
    .body(body)
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

/// 四条路由（预处理 / 分块 / 展示 / 下载）匿名一律 401；body 是 jwt-rust 的 JSON 形态
/// （`{"code":401,"msg":…,"data":null}`）。`msg` 随错误类型变化（缺头/过期/坏签名各走
/// `user_message()`，不再是旧实现统一的纯文本），故断言只钉 `code`/`data`、不钉 `msg`。
/// （POST 两条带合法 body，钉「鉴权先于提取器」，路由漏挂 layer 即红。）
#[tokio::test(flavor = "multi_thread")]
async fn aether_routes_reject_anonymous() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let client = reqwest::Client::new();

    let cases: Vec<(reqwest::Method, &str)> = vec![
        (reqwest::Method::POST, "/aetherupload/preprocess"),
        (reqwest::Method::POST, "/aetherupload/uploading"),
        (reqwest::Method::GET, "/aetherupload/display/x"),
        (reqwest::Method::GET, "/aetherupload/download/x/y"),
    ];

    for (method, path) in cases {
        let resp = with_form(client.request(method.clone(), format!("{base}{path}")))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401, "{method} {path} 未带 token 必须 401");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["code"], 401, "jwt-rust 401 形态");
        assert_eq!(body["data"], serde_json::Value::Null);
    }
}

/// 登录拿到的 token：cookie `oss_token`（放在多 cookie 串里，钉分号切分）与 Bearer 头都要过。
#[tokio::test(flavor = "multi_thread")]
async fn cookie_or_bearer_token_passes() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
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
    let token = login["token"].as_str().unwrap().to_string();
    let url = format!("{base}/aetherupload/preprocess");

    let resp = with_form(
        client
            .post(&url)
            .header("cookie", format!("theme=dark; oss_token={token}; other=1")),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(resp.status(), 200, "cookie 形态");
    let json: serde_json::Value = resp.json().await.unwrap();
    // 2.0 协议：成功响应是 snake_case 的上传结果、没有 error 字段
    assert!(
        json["chunk_size"].is_number(),
        "预处理应成功（拿到 chunk_size）: {json}"
    );
    assert!(
        json.get("error").is_none(),
        "2.0 成功体不应带 error 字段: {json}"
    );

    let resp = with_form(client.post(&url).bearer_auth(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "Bearer 形态");
    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json["chunk_size"].is_number(), "预处理应成功: {json}");

    // 空 cookie 值（`oss_token=`）不遮蔽 Bearer 回退 —— 本轮微收尾的修复点
    let resp = with_form(
        client
            .post(&url)
            .header("cookie", "oss_token=")
            .bearer_auth(&token),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(resp.status(), 200, "空 cookie 值应回退到 Bearer");
    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json["chunk_size"].is_number(), "预处理应成功: {json}");
}

/// 跨面共用：同一次登录的 token，控制面（Bearer 打 /api/v1）与上传口（cookie 打
/// /aetherupload）都要认 —— 两面是同一个 JwtAuth 实例、同一 secret。
#[tokio::test(flavor = "multi_thread")]
async fn login_token_works_on_both_surfaces() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
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
        .get(format!("{base}/api/v1/groups"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "控制面（Bearer）");

    let resp = with_form(
        client
            .post(format!("{base}/aetherupload/preprocess"))
            .header("cookie", format!("oss_token={token}")),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(resp.status(), 200, "上传口（同 token，cookie 形态）");
    let json: serde_json::Value = resp.json().await.unwrap();
    // 2.0 协议：成功体没有 error 字段（同一 token 两面通用）
    assert!(json["chunk_size"].is_number(), "预处理应成功: {json}");
}

/// 边界凭据一律 401：空 Bearer（`Authorization: Bearer `）、空 cookie 值且无 Bearer 可回退、
/// 含非法字符的 cookie 值。
#[tokio::test(flavor = "multi_thread")]
async fn empty_or_garbage_credential_is_401() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/aetherupload/preprocess");

    let cases: Vec<(Option<&str>, Option<&str>)> = vec![
        // 空 Bearer（尾随空格由 HTTP 层剥掉，等价于裸 "Bearer"：前缀不匹配）
        (None, Some("Bearer ")),
        (Some("oss_token="), None), // 空 cookie 值，无 Bearer 可回退
        (Some("oss_token=%%% not.a.jwt @@"), None), // 非法字符
    ];

    for (cookie, authorization) in cases {
        let mut req = client.post(&url);
        if let Some(c) = cookie {
            req = req.header("cookie", c);
        }
        if let Some(a) = authorization {
            req = req.header("authorization", a);
        }
        let resp = with_form(req).send().await.unwrap();
        assert_eq!(
            resp.status(),
            401,
            "cookie={cookie:?} authorization={authorization:?} 必须 401"
        );
    }
}

/// 垫片只在「无 Authorization 头」时注入 cookie：带不可用 Authorization（坏 Bearer）时，
/// 即便 cookie 有效也 401 —— Bearer 优先于 cookie（与旧 cookie 优先实现相反，有意定稿）。
#[tokio::test(flavor = "multi_thread")]
async fn bad_bearer_shadows_valid_cookie() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
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

    let resp = with_form(
        client
            .post(format!("{base}/aetherupload/preprocess"))
            .header("cookie", format!("oss_token={token}"))
            .header("authorization", "Bearer not.a.real.token"),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(resp.status(), 401, "不可用 Bearer 存在时不回退 cookie");
}

/// 伪造（换 secret 签名）与过期 token，两种来源都必须 401。
#[tokio::test(flavor = "multi_thread")]
async fn forged_or_expired_token_is_401() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let client = reqwest::Client::new();

    let forged = sign_token("not-the-secret", unix_now() + 3600);
    // exp 早于 now 3600s，越过 jwt-rust 的 leeway（0，源头是 JwtConfig 默认值）
    let expired = sign_token(common::JWT_SECRET, unix_now() - 3600);

    for token in [&forged, &expired] {
        for as_cookie in [true, false] {
            let req = client.post(format!("{base}/aetherupload/preprocess"));
            let req = if as_cookie {
                req.header("cookie", format!("oss_token={token}"))
            } else {
                req.bearer_auth(token)
            };
            let resp = with_form(req).send().await.unwrap();
            assert_eq!(
                resp.status(),
                401,
                "伪造/过期 token（cookie={as_cookie}）必须 401"
            );
        }
    }
}

/// 按任意 uid 签 token（权限门用例需要非 1 号子账户）；
/// 上面既有用例的 `sign_token` 恒 sub="1"，保持原样不动。
fn sign_for(secret: &str, uid: i64, exp_secs: i64) -> String {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &TestClaims {
            sub: uid.to_string(),
            exp: exp_secs as usize,
        },
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

/// 上传面权限门（权限体系 Step 4）：写路径按「分组 → 桶」判权。
/// - 子账户对未授权分组（file → files）preprocess → 403 problem+json（code=access_denied、
///   title 含「无权」——前端 JS 直接渲染 title）；
/// - 管理员令牌不受影响（既有用例已覆盖）；
/// - 授权 write 后同一请求 → 200 协议成功体；display 另按 read 门槛。
#[tokio::test(flavor = "multi_thread")]
async fn aether_gate_enforces_bucket_permission() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let sub = st.db.create_user("sub", "pw12345678", false).await.unwrap();
    let base = serve(build_router(st.clone()).unwrap()).await;
    let client = reqwest::Client::new();
    let sub_token = sign_for(common::JWT_SECRET, sub, unix_now() + 3600);

    // 未授权：403 problem+json
    let resp = with_form(
        client
            .post(format!("{base}/aetherupload/preprocess"))
            .bearer_auth(&sub_token),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(resp.status(), 403, "无写权限的分组必须 403");
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        ct.contains("application/problem+json"),
        "content-type: {ct}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "access_denied");
    assert!(
        body["title"].as_str().unwrap_or_default().contains("无权"),
        "title 应含「无权」（JS 直接渲染 title）: {body}"
    );

    // display 同样按读门槛：未授权 → 403
    let saved_path = aetherupload::SavedPath::encode("file", "202610", "x.txt");
    let resp = client
        .get(format!("{base}/aetherupload/display/{saved_path}"))
        .bearer_auth(&sub_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "display 无读权限必须 403");

    // 授权 files 读+写 → preprocess 200、display 不再 403（资源不存在交给内核报）
    st.db
        .set_permissions(
            sub,
            &[oss_server::db::PermissionRow {
                bucket: "files".into(),
                read: true,
                write: true,
                delete: false,
            }],
        )
        .await
        .unwrap();
    let resp = with_form(
        client
            .post(format!("{base}/aetherupload/preprocess"))
            .bearer_auth(&sub_token),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(resp.status(), 200, "授权后 preprocess 应 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["chunk_size"].is_number(), "协议成功体: {body}");

    let resp = client
        .get(format!("{base}/aetherupload/display/{saved_path}"))
        .bearer_auth(&sub_token)
        .send()
        .await
        .unwrap();
    assert_ne!(
        resp.status(),
        403,
        "授权读后 display 不再 403（资源缺失由内核报）"
    );
}
