//! Level C 手工排空：`read_only` 节点拒绝客户端写（S3/控制台/上传面），
//! 读、登录、健康/指标与节点间 Raft RPC 保持在线。

mod common;

use common::state;
use oss_server::router::build_router;
use oss_server::state::AppState;
use reqwest::Method;
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// 等价于「重启时读入 read_only=true」：router 只读 `state.cfg`。
fn mark_read_only(mut st: AppState) -> AppState {
    let mut cfg = (*st.cfg).clone();
    cfg.read_only = true;
    st.cfg = Arc::new(cfg);
    st
}

async fn login_token(base: &str) -> String {
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({ "username": "erik", "password": "pw" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "登录应放行（运维要能登录看排空节点）");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["token"].as_str().unwrap().to_string()
}

fn amz_now() -> AmzDate {
    let t = time::OffsetDateTime::now_utc();
    let s = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    AmzDate::parse(&s).unwrap()
}

/// 最小 SigV4 header 签名请求（与 accounts.rs 同款）。
async fn s3_signed(
    method: Method,
    base: &str,
    path: &str,
    ak: &str,
    sk: &str,
    body: &[u8],
) -> reqwest::Response {
    const REGION: &str = "us-east-1";
    let url = format!("{base}{path}");
    let host = url
        .split("://")
        .nth(1)
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_string();
    let payload_hex = hex::encode(Sha256::digest(body));
    let amz = amz_now();
    let amz_iso = amz.fmt_iso8601().to_string();
    let amz_date = amz.fmt_date().to_string();
    let to_sign = [
        ("host".to_string(), host),
        ("x-amz-content-sha256".to_string(), payload_hex.clone()),
        ("x-amz-date".to_string(), amz_iso.clone()),
    ];
    let refs: Vec<(&str, &str)> = to_sign
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let canonical = create_canonical_request(
        method.as_str(),
        path,
        &[] as &[(&str, &str)],
        &refs,
        Payload::SingleChunk(&payload_hex),
    );
    let sts = create_string_to_sign(&canonical, &amz, REGION, "s3");
    let sig = calculate_signature(&sts, sk, &amz, REGION, "s3");
    let names: Vec<&str> = to_sign.iter().map(|(k, _)| k.as_str()).collect();
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={ak}/{amz_date}/{REGION}/s3/aws4_request, \
         SignedHeaders={}, Signature={sig}",
        names.join(";")
    );
    reqwest::Client::new()
        .request(method, &url)
        .header("x-amz-content-sha256", &payload_hex)
        .header("x-amz-date", amz_iso)
        .header("authorization", auth)
        .body(body.to_vec())
        .send()
        .await
        .unwrap()
}

/// 管理员 + S3 密钥；桶和对象由非只读面写入（排空前的存量数据）。
async fn seeded() -> (tempfile::TempDir, AppState, &'static str, &'static str) {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    let uid = st.db.create_user("erik", "pw", true).await.unwrap();
    let (ak, sk) = ("AKIDREADONLY00000001", "sk-read-only-0000000001");
    st.db.create_access_key(ak, sk, uid).await.unwrap();
    (dir, st, ak, sk)
}

#[tokio::test]
async fn read_only_blocks_client_writes_keeps_reads_login_and_status() {
    let (_dir, st, ak, sk) = seeded().await;
    let client = reqwest::Client::new();

    // 排空前：登录/建桶/写对象正常（read_only=false 行为不变）。
    let rw = serve(build_router(st.clone()).unwrap()).await;
    let token = login_token(&rw).await;
    let resp = client
        .post(format!("{rw}/api/v1/buckets"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "readonly-src" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204, "read_only=false 建桶应正常");
    let resp = s3_signed(Method::PUT, &rw, "/readonly-src/obj.txt", ak, sk, b"hello").await;
    assert_eq!(resp.status(), 200, "read_only=false PutObject 应正常");

    // 同一状态切换为排空（重启等价）。
    let ro = serve(build_router(mark_read_only(st)).unwrap()).await;

    // 登录与读面在线。
    let token = login_token(&ro).await;
    let resp = client
        .get(format!("{ro}/api/v1/buckets"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "控制台读应放行");
    let resp = s3_signed(Method::GET, &ro, "/readonly-src/obj.txt", ak, sk, b"").await;
    assert_eq!(resp.status(), 200, "GetObject 应放行");
    assert_eq!(resp.text().await.unwrap(), "hello");
    let resp = s3_signed(Method::GET, &ro, "/readonly-src", ak, sk, b"").await;
    assert_eq!(resp.status(), 200, "ListObjects 应放行");
    assert!(resp.text().await.unwrap().contains("obj.txt"));

    let resp = reqwest::get(format!("{ro}/health/live")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let resp = reqwest::get(format!("{ro}/metrics")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let resp = client
        .request(Method::OPTIONS, format!("{ro}/readonly-src/obj.txt"))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), 503, "OPTIONS 预检不得被排空拦截");
    // 状态面暴露排空标记。
    let view: serde_json::Value = reqwest::get(format!("{ro}/health/cluster"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(view["read_only"], true);

    // 客户端写一律 503（S3 XML + 显式头），先于鉴权。
    let resp = s3_signed(Method::PUT, &ro, "/readonly-src/blocked.txt", ak, sk, b"x").await;
    assert_eq!(resp.status(), 503, "PutObject 应被排空拦截");
    assert_eq!(resp.headers()["x-oss-read-only"], "1");
    assert!(resp.text().await.unwrap().contains("read-only"));
    let resp = s3_signed(Method::DELETE, &ro, "/readonly-src/obj.txt", ak, sk, b"").await;
    assert_eq!(resp.status(), 503, "DeleteObject 应被排空拦截");
    let resp = client
        .post(format!("{ro}/api/v1/buckets"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "readonly-2" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "控制台写应被排空拦截");

    // 登录放行只认精确路径/方法：编码斜杠与变体不能绕过，也不能误放行。
    for (method, path) in [
        (Method::POST, "/api/v1/%6Cogin"),
        (Method::POST, "/api/v1/login%2F..%2Fbuckets"),
        (Method::PUT, "/api/v1/login"),
    ] {
        let resp = client
            .request(method.clone(), format!("{ro}{path}"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 503, "{method} {path} 必须被拦截");
    }
    // 形似 raft RPC、但不再被 axum 路由到节点间 RPC 的路径（尾斜杠）属于客户端面，
    // 必须被拦截（fail-closed，不能落进 S3 fallback 当写请求）。
    let resp = client
        .post(format!("{ro}/api/v1/site/raft/append/"))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "非精确 raft 路径不得绕过排空");
}

/// read_only=false 保持原行为：控制台与 S3 写都不被 503 拦截。
#[tokio::test]
async fn read_only_off_keeps_writes() {
    let (_dir, st, ak, sk) = seeded().await;
    let base = serve(build_router(st).unwrap()).await;
    let token = login_token(&base).await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/buckets"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "readonly-rw" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let resp = s3_signed(Method::PUT, &base, "/readonly-rw/obj.txt", ak, sk, b"x").await;
    assert_eq!(resp.status(), 200);
}
