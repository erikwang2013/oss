// 只验「挂载 + 鉴权接线」：无凭据请求被 403 = s3s 的 default_check 生效，
// 排除 setAuth 漏接的半成品形态；签名闭环（aws-cli / rclone）交 Task 16 的端到端脚本。

mod common;

use common::state;
use oss_server::router::build_router;

#[tokio::test]
async fn s3_fallback_is_mounted() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    let app = build_router(st).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // 无凭据的 S3 请求必须被拒（说明 s3s 的鉴权层确实挂上了，而不是 404）
    let resp = reqwest::get(format!("http://{addr}/alpha/whatever"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "未经签名的 S3 请求应被 s3s 拒绝");
}
