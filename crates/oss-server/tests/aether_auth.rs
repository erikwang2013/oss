// /aetherupload/* 的最小鉴权层（规格 §0.1「会话 Cookie/JWT 覆盖层」）：
// cookie `oss_token` 或 `Authorization: Bearer` 二者皆可，缺/坏/过期一律 401。
// 路由表变化时，本文件的「匿名 401 表」要跟着同步（四条路由全在同一层 middleware 之内）。

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

/// 四条路由（预处理 / 分块 / 展示 / 下载）匿名一律 401 纯文本。
/// （POST 两条带合法 body，钉「auth 先于提取器」，路由漏挂 middleware 即红。）
#[tokio::test(flavor = "multi_thread")]
async fn aether_routes_reject_anonymous() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st)).await;
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
        assert_eq!(
            resp.text().await.unwrap(),
            "unauthorized",
            "纯文本、不泄细节"
        );
    }
}

/// 登录拿到的 token：cookie `oss_token`（放在多 cookie 串里，钉分号切分）与 Bearer 头都要过。
#[tokio::test(flavor = "multi_thread")]
async fn cookie_or_bearer_token_passes() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st)).await;
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
    assert_eq!(json["error"], 0, "预处理应成功（error 为数字 0）");

    let resp = with_form(client.post(&url).bearer_auth(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "Bearer 形态");
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["error"], 0);
}

/// 伪造（换 secret 签名）与过期 token，两种来源都必须 401。
#[tokio::test(flavor = "multi_thread")]
async fn forged_or_expired_token_is_401() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st)).await;
    let client = reqwest::Client::new();

    let forged = sign_token("not-the-secret", unix_now() + 3600);
    // exp 早于 now 3600s，越过 jsonwebtoken 默认 60s leeway
    let expired = sign_token("test", unix_now() - 3600);

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
