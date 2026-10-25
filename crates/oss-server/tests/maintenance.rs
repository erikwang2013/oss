//! M3 shrink T3：maintenance 模式下 HTTP 写口一律 503（读口保持在线，供 mesh 拉源分片）。

mod common;

use common::state_in_maintenance;
use oss_server::router::build_router;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

#[tokio::test]
async fn maintenance_rejects_writes_with_retry_after() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve(build_router(state_in_maintenance(dir.path()).await).unwrap()).await;
    let http = reqwest::Client::new();

    // 写方法（含未鉴权请求）必须在鉴权/路由前被拦：503 + S3 XML + 可重试提示。
    let resp = http
        .put(format!("{base}/files/k"))
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
    assert_eq!(
        resp.headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some("60")
    );
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(ct.starts_with("application/xml"), "content-type: {ct}");
    let body = resp.text().await.unwrap();
    assert!(body.contains("<Code>ServiceUnavailable</Code>"), "{body}");

    // GET/HEAD/OPTIONS 放行（读路径 + 预检不能被 503 卡死）；具体路由状态不在此断言。
    for method in [reqwest::Method::GET, reqwest::Method::HEAD] {
        let resp = http
            .request(method.clone(), format!("{base}/files/k"))
            .send()
            .await
            .unwrap();
        assert_ne!(resp.status(), 503, "{method} 不应被 maintenance 拦截");
    }
    let resp = http
        .request(reqwest::Method::OPTIONS, format!("{base}/files/k"))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), 503, "OPTIONS 不应被 maintenance 拦截");

    // DELETE/POST 同样 503。
    for method in [reqwest::Method::DELETE, reqwest::Method::POST] {
        let resp = http
            .request(method.clone(), format!("{base}/files/k"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 503, "{method} 应被 maintenance 拦截");
    }
}
