//! E1：`[server].control_backend = "objects"` 下的请求热视图端到端。
//!
//! 覆盖：S3 SigV4 鉴权与 ACL/桶查找走视图、同节点写后读（提交返回前已发布）、
//! 吊销立即生效、控制台登录/`/me` 走对象 IAM 适配器。SQLite 默认路径由既有
//! 测试保持逐字节不变（本文件全部显式启用 objects 后端）。

use std::path::{Path, PathBuf};

use oss_server::config::ControlBackend;
use oss_server::router::build_router;
use oss_server::state::AppState;
use reqwest::Method;
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::{Digest, Sha256};
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::BucketEntry;

const JWT_SECRET: &str = "0123456789abcdef0123456789abcdef";

fn config_path(dir: &Path) -> PathBuf {
    let text = format!(
        r#"
listen = "127.0.0.1:0"
data_dir = "{data}"
staging_dir = "{staging}"
database_url = "sqlite://{db}?mode=rwc"
jwt_secret = "{JWT_SECRET}"

[server]
control_backend = "objects"

[site]
id = "e1-site"
region = "us-east-1"
"#,
        data = dir.join("data").display(),
        staging = dir.join("staging").display(),
        db = dir.join("oss.db").display(),
    );
    let path = dir.join("oss.toml");
    std::fs::write(&path, text).unwrap();
    path
}

async fn object_state(dir: &Path) -> AppState {
    let state = AppState::load(&config_path(dir)).await.unwrap();
    assert_eq!(state.db.control_backend(), ControlBackend::Objects);
    assert!(state.control_view.is_some(), "对象后端必须装配请求视图");
    state
}

/// 直接在对象根里建用户 + 长时 AK（console 写路径尚未双路由，E1 只读热路径）。
async fn seed_user_key(st: &AppState, user: &str, password: &str, ak: &str, sk: &str) -> i64 {
    let hash = oss_server::db::hash_password(password);
    let (user, ak, sk) = (user.to_string(), ak.to_string(), sk.to_string());
    st.db
        .control_write("e1-test", move |txn| {
            let (user, hash, ak, sk) = (user.clone(), hash.clone(), ak.clone(), sk.clone());
            Box::pin(async move {
                let uid = txn
                    .user_create_default_status(&user, &hash, false, 1)
                    .await?;
                txn.access_key_create_legacy(&ak, &sk, uid, 1).await?;
                Ok(uid)
            })
        })
        .await
        .unwrap()
}

async fn seed_bucket(st: &AppState, name: &str, owner: &str) {
    let entry = BucketEntry {
        name: name.to_string(),
        owner: OwnerRef::User(owner.to_string()),
        creation_date: 1,
        versioning: "Off".into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting: false,
    };
    st.db
        .control_write("e1-test", move |txn| {
            let entry = entry.clone();
            Box::pin(async move { txn.bucket_create(&entry).await.map(|_| ()) })
        })
        .await
        .unwrap();
    std::fs::create_dir_all(xl_store::layout::bucket_dir(st.store.root(), name)).unwrap();
}

async fn add_key(st: &AppState, user: &str, ak: &str, sk: &str) {
    let uid = st.db.control_store().user(user).await.unwrap().unwrap().uid;
    let (ak, sk) = (ak.to_string(), sk.to_string());
    st.db
        .control_write("e1-test", move |txn| {
            let (ak, sk) = (ak.clone(), sk.clone());
            Box::pin(async move { txn.access_key_create_legacy(&ak, &sk, uid, 2).await })
        })
        .await
        .unwrap();
}

async fn delete_key(st: &AppState, ak: &str) {
    let ak = ak.to_string();
    st.db
        .control_write("e1-test", move |txn| {
            let ak = ak.clone();
            Box::pin(async move { txn.access_key_delete(&ak).await.map(|_| ()) })
        })
        .await
        .unwrap();
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
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

/// 最小 SigV4 header 签名（同 `read_only.rs`）。
async fn s3_signed(
    method: Method,
    base: &str,
    path: &str,
    ak: &str,
    sk: &str,
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
    let body: &[u8] = &[];
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

#[tokio::test]
async fn s3_auth_acl_and_bucket_lookup_use_the_captured_view() {
    let dir = tempfile::tempdir().unwrap();
    let st = object_state(dir.path()).await;
    seed_user_key(
        &st,
        "alice",
        "alice-pw",
        "AKIDALICE00000000001",
        "sk-alice-000000000001",
    )
    .await;
    seed_user_key(
        &st,
        "bob",
        "bob-pw",
        "AKIDBOB0000000000001",
        "sk-bob-00000000000001",
    )
    .await;
    seed_bucket(&st, "alpha", "alice").await;

    let base = serve(build_router(st).unwrap()).await;
    // 属主：视图鉴权 + ACL 放行 + 桶配置读取。
    let owner = s3_signed(
        Method::HEAD,
        &base,
        "/alpha",
        "AKIDALICE00000000001",
        "sk-alice-000000000001",
    )
    .await;
    assert_eq!(owner.status(), 200, "桶属主 HEAD 必须 200");
    // 既非属主也无权限：视图 `can()` 拒绝。
    let denied = s3_signed(
        Method::HEAD,
        &base,
        "/alpha",
        "AKIDBOB0000000000001",
        "sk-bob-00000000000001",
    )
    .await;
    assert_eq!(denied.status(), 403, "无权限用户必须 403");
    // 未知 AK：视图 `key_auth` 拒绝（不会回落到 SQLite）。
    let unknown = s3_signed(
        Method::HEAD,
        &base,
        "/alpha",
        "AKIDGHOST00000000001",
        "sk-ghost-00000000000001",
    )
    .await;
    assert_eq!(unknown.status(), 403, "未知 AK 必须 403");
}

#[tokio::test]
async fn write_then_read_same_node_and_revocation_are_immediate() {
    let dir = tempfile::tempdir().unwrap();
    let st = object_state(dir.path()).await;
    seed_user_key(
        &st,
        "alice",
        "alice-pw",
        "AKIDALICE00000000001",
        "sk-alice-000000000001",
    )
    .await;
    seed_bucket(&st, "alpha", "alice").await;
    let base = serve(build_router(st.clone()).unwrap()).await;

    let first = s3_signed(
        Method::HEAD,
        &base,
        "/alpha",
        "AKIDALICE00000000001",
        "sk-alice-000000000001",
    )
    .await;
    assert_eq!(first.status(), 200);

    // 提交返回前已发布：同节点下一个请求立即能用新 AK。
    add_key(
        &st,
        "alice",
        "AKIDALICE00000000002",
        "sk-alice-000000000002",
    )
    .await;
    let fresh = s3_signed(
        Method::HEAD,
        &base,
        "/alpha",
        "AKIDALICE00000000002",
        "sk-alice-000000000002",
    )
    .await;
    assert_eq!(fresh.status(), 200, "写后读：新 AK 必须立即可用");

    // 吊销立即生效（不缓存负结果、不从 SQLite 回落）。
    delete_key(&st, "AKIDALICE00000000002").await;
    let revoked = s3_signed(
        Method::HEAD,
        &base,
        "/alpha",
        "AKIDALICE00000000002",
        "sk-alice-000000000002",
    )
    .await;
    assert_eq!(revoked.status(), 403, "吊销必须立即生效");
    let survivor = s3_signed(
        Method::HEAD,
        &base,
        "/alpha",
        "AKIDALICE00000000001",
        "sk-alice-000000000001",
    )
    .await;
    assert_eq!(survivor.status(), 200, "未吊销的 AK 不受影响");
}

#[tokio::test]
async fn console_login_and_me_use_object_iam() {
    let dir = tempfile::tempdir().unwrap();
    let st = object_state(dir.path()).await;
    seed_user_key(
        &st,
        "alice",
        "alice-pw",
        "AKIDCONSOLE000000001",
        "sk-console-00000000001",
    )
    .await;
    let base = serve(build_router(st).unwrap()).await;

    let login = reqwest::Client::new()
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({ "username": "alice", "password": "alice-pw" }))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 200, "对象后端登录必须可用");
    let token = login.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    let me = reqwest::Client::new()
        .get(format!("{base}/api/v1/me"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(me.status(), 200, "/me 走对象 IAM 适配器");
    let body: serde_json::Value = me.json().await.unwrap();
    assert_eq!(body["username"], "alice");
}
