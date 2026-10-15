//! C1 登录限速（429 + Retry-After；锁定期内正确口令同样拒）。

mod common;

use common::state;
use oss_server::router::build_router;

#[tokio::test]
async fn login_lockout_after_failures() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("erik", "pw-123456", true).await.unwrap();
    let base = {
        let app = build_router(st.clone()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    };
    let client = reqwest::Client::new();
    let login = |pw: &str| {
        client
            .post(format!("{base}/api/v1/login"))
            .json(&serde_json::json!({"username": "erik", "password": pw}))
            .send()
    };
    // 5 次错口令 → 401
    for i in 0..5 {
        let resp = login("wrong-pw").await.unwrap();
        assert_eq!(resp.status(), 401, "第 {i} 次错口令应 401");
    }
    // 第 6 次（哪怕口令正确）→ 429 + Retry-After
    let resp = login("pw-123456").await.unwrap();
    assert_eq!(resp.status(), 429);
    let ra = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(ra > 0 && ra <= 600, "Retry-After 应在 (0,600]：{ra}");
    // 另一账号不受影响（erik 锁定不影响 other）
    st.db
        .create_user("other", "pw-654321", false)
        .await
        .unwrap();
    let resp = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "other", "password": "pw-654321"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "未锁定账号应正常登录");
}
