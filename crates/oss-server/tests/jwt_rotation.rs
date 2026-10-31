//! JWT 签名密钥轮换：当前密钥签发的 token 与 `jwt_previous_secrets` 中每个旧密钥签发的
//! token 都放行；未知密钥/匿名仍是 401；公共路径（`/login`）照旧绕过鉴权。

mod common;

use common::{JWT_SECRET, state_with_previous_secrets};
use oss_server::router::build_router;

const PREV_1: &str = "11111111111111111111111111111111";
const PREV_2: &str = "22222222222222222222222222222222";
const UNKNOWN: &str = "33333333333333333333333333333333";

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn sign(secret: &str, sub: i64) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &serde_json::json!({"sub": sub.to_string(), "exp": exp}),
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

async fn groups_status(client: &reqwest::Client, base: &str, token: &str) -> reqwest::StatusCode {
    client
        .get(format!("{base}/api/v1/groups"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .status()
}

/// 当前密钥 + 每个旧密钥都放行（控制面与上传口共用同一鉴权层）。
#[tokio::test(flavor = "multi_thread")]
async fn current_and_each_previous_secret_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_previous_secrets(dir.path(), vec![PREV_1.into(), PREV_2.into()]).await;
    let uid = st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let client = reqwest::Client::new();

    for secret in [JWT_SECRET, PREV_1, PREV_2] {
        assert_eq!(
            groups_status(&client, &base, &sign(secret, uid)).await,
            200,
            "secret 应被接受: {secret}"
        );
    }

    // 上传口：同一旧密钥 token 也要过（两层都换成了多密钥鉴权）。
    let resp = client
        .get(format!("{base}/aetherupload/display/x"))
        .bearer_auth(sign(PREV_1, uid))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), 401, "上传口旧密钥 token 不应被鉴权层拒绝");
}

/// 未知密钥一律 401，响应体保持 jwt-rust 形态（`code=401`、`data=null`）。
#[tokio::test(flavor = "multi_thread")]
async fn unknown_secret_rejected_with_401() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_previous_secrets(dir.path(), vec![PREV_1.into()]).await;
    let uid = st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/v1/groups"))
        .bearer_auth(sign(UNKNOWN, uid))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], 401);
    assert_eq!(body["data"], serde_json::Value::Null);
}

/// 旧密钥列表为空 = 原行为：当前密钥放行，旧密钥 401。
#[tokio::test(flavor = "multi_thread")]
async fn empty_previous_secrets_keeps_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_previous_secrets(dir.path(), vec![]).await;
    let uid = st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let client = reqwest::Client::new();

    assert_eq!(
        groups_status(&client, &base, &sign(JWT_SECRET, uid)).await,
        200
    );
    assert_eq!(groups_status(&client, &base, &sign(PREV_1, uid)).await, 401);
    assert_eq!(groups_status(&client, &base, "").await, 401, "匿名");
}

/// 公共路径仍绕过鉴权：未带 token 的 `/api/v1/login` 正常登录。
#[tokio::test(flavor = "multi_thread")]
async fn public_login_path_still_bypasses() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_previous_secrets(dir.path(), vec![PREV_1.into()]).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let base = serve(build_router(st).unwrap()).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "登录不应要求 JWT");
}
