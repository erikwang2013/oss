mod common;

use common::state;
use oss_server::router::build_router;

#[tokio::test]
async fn login_then_list_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    st.store.create_bucket("alpha").await.unwrap();
    let app = build_router(st);

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

    let buckets: Vec<String> = client
        .get(format!("{base}/api/v1/buckets"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(buckets, vec!["alpha".to_string()]);
}

#[tokio::test]
async fn wrong_password_is_401() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    let app = build_router(st);
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
}

/// 钉 `register_upload` 的三分支口径（I-1 定稿）：命中 204、无行 404、错误 400。
/// Err→400 分支需要 DB 故障，HTTP 面不构造（db.rs 单测已覆盖返回语义）。
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
    let app = build_router(st);
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
