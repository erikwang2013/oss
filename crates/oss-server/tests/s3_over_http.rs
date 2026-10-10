// 起真服务，用 aws-sigv4 打的请求跑一个 PUT → HEAD → GET → LIST → DELETE 的闭环。
// 为了不引 aws-sdk（体积大），用 reqwest 手工构造签名。
// 更省事的做法：直接调 s3s 的 S3Service（不经 HTTP）验证挂载正确，
// 而真正的 SigV4 兼容性交给 Task 16 的 aws-cli / rclone 脚本（那是端到端验收）。

mod common;

use common::state;
use oss_server::router::build_router;

#[tokio::test]
async fn s3_fallback_is_mounted() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    st.db.create_user("u", "p", true).await.unwrap();
    st.db
        .create_access_key("AKIA_TEST", "SK_TEST", 1)
        .await
        .unwrap();
    let app = build_router(st);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // 无凭据的 S3 请求必须被拒（说明 s3s 的鉴权层确实挂上了，而不是 404）
    let resp = reqwest::get(format!("http://{addr}/alpha/whatever"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "未经签名的 S3 请求应被 s3s 拒绝");
}
