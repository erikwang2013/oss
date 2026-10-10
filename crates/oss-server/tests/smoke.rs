mod common;

use common::state;
use oss_server::router::build_router;

#[tokio::test]
async fn health_endpoint_is_up() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    let app = build_router(st);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let resp = reqwest::get(format!("http://{addr}/health/live"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}
