//! M8 S4b：tier 后端、Transition 归档、Restore 状态机/对账/reaper、远端 key 编码。

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use xl_store::store::VersioningMode;

use bytes::Bytes;
use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::service::OssS3;
use xl_store::meta::TierPhase;

use md5::Digest as _;
use oss_server::config::{TierKind, TierTarget};
use oss_server::jobs::TaskExecutor as _;
use oss_server::router::build_router;
use oss_server::state::AppState;
use oss_server::tier::{S3TierBackend, S3TierGc, TierBackend, TierBackends, tier_key};
use oss_server::tier_jobs::{M8TaskExecutor, reconcile_restores};
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::Sha256;

/// 内存 tier 后端：PUT 返回与源 ETag 不同的远端 ETag（钉「不比较源 ETag」）。
#[derive(Default)]
struct MockTier {
    objs: Mutex<HashMap<String, Bytes>>,
    deleted: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TierBackend for MockTier {
    fn id(&self) -> &str {
        "cold"
    }

    async fn put(&self, key: &str, body: Bytes, etag: &str) -> Result<String, String> {
        self.objs.lock().unwrap().insert(key.to_string(), body);
        Ok(format!("mock-{etag}"))
    }

    async fn get(&self, key: &str) -> Result<Bytes, String> {
        self.objs
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| format!("missing {key}"))
    }

    async fn head(&self, key: &str) -> Result<Option<u64>, String> {
        Ok(self.objs.lock().unwrap().get(key).map(|b| b.len() as u64))
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.deleted.lock().unwrap().push(key.to_string());
        self.objs.lock().unwrap().remove(key);
        Ok(())
    }
}

fn tier_target(endpoint: &str) -> TierTarget {
    TierTarget {
        id: "cold".into(),
        kind: TierKind::S3,
        path: None,
        endpoint: endpoint.into(),
        bucket: "tb".into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
        server_name: None,
    }
}

async fn setup() -> (AppState, Arc<MockTier>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut st =
        common::state_with_tiers(dir.path(), vec![tier_target("http://127.0.0.1:9")]).await;
    let mock = Arc::new(MockTier::default());
    let backends = Arc::new(TierBackends::new(vec![
        Arc::clone(&mock) as Arc<dyn TierBackend>
    ]));
    st.tiers = Arc::clone(&backends);
    st.tier_gc = Arc::new(S3TierGc::new(Arc::clone(&backends)));
    st.store.create_bucket("bkt").await.unwrap();
    st.db.insert_bucket("bkt", None).await.unwrap();
    (st, mock, dir)
}

fn service(st: &AppState) -> OssS3 {
    OssS3::new(Arc::clone(&st.store), st.acl(), st.cfg.max_put_bytes)
        .with_lifecycle(Some(Arc::clone(&st.lifecycle)))
        .with_restore(Some(Arc::clone(&st.restore_sink)))
        .with_tier_gc(Some(Arc::clone(&st.tier_gc)))
}

fn executor(st: &AppState) -> M8TaskExecutor {
    M8TaskExecutor::new(
        Arc::clone(&st.db),
        Arc::clone(&st.store),
        Arc::clone(&st.tiers),
        Arc::clone(&st.events),
    )
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: axum::http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// 对 `POST /<bucket>/<key>?restore` 发一个最小 SigV4 签名请求（只签本用例用到的头）。
async fn signed_restore_post(
    base: &str,
    bucket: &str,
    key: &str,
    ak: &str,
    sk: &str,
) -> reqwest::Response {
    const REGION: &str = "us-east-1";
    let path = format!("/{bucket}/{key}");
    let host = base.strip_prefix("http://").unwrap().to_string();
    let payload_hex = hex::encode(Sha256::digest(b""));
    let t = time::OffsetDateTime::now_utc();
    let amz = AmzDate::parse(&format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    ))
    .unwrap();
    let mut to_sign = [
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hex.clone()),
        ("x-amz-date".to_string(), amz.fmt_iso8601().to_string()),
        ("x-amz-restore-request".to_string(), "days=1".to_string()),
    ];
    to_sign.sort_by(|a, b| a.0.cmp(&b.0));
    let refs: Vec<(&str, &str)> = to_sign
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    // 线上 `?restore` 与 canonical 形态 `restore=` 等价（s3s 按空值 form 解析）。
    let pairs = [("restore".to_string(), String::new())];
    let canonical = create_canonical_request(
        "POST",
        &path,
        &pairs,
        &refs,
        Payload::SingleChunk(&payload_hex),
    );
    let sts = create_string_to_sign(&canonical, &amz, REGION, "s3");
    let sig = calculate_signature(&sts, sk, &amz, REGION, "s3");
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={ak}/{}/{REGION}/s3/aws4_request, SignedHeaders={}, Signature={sig}",
        amz.fmt_date(),
        refs.iter().map(|(k, _)| *k).collect::<Vec<_>>().join(";")
    );
    reqwest::Client::new()
        .post(format!("{base}{path}?restore"))
        .header("host", host)
        .header("x-amz-content-sha256", payload_hex)
        .header("x-amz-date", amz.fmt_iso8601().to_string())
        .header("x-amz-restore-request", "days=1")
        .header(reqwest::header::AUTHORIZATION, auth)
        .send()
        .await
        .expect("HTTP 请求发送失败")
}

fn payload(bucket: &str, key: &str, vid: &str, class: &str, tier: Option<&str>) -> String {
    serde_json::json!({
        "bucket": bucket, "key": key, "version_id": vid, "class": class, "tier": tier
    })
    .to_string()
}

async fn archive(st: &AppState, key: &str, body: &'static [u8]) -> String {
    let info = st
        .store
        .put_object("bkt", key, Bytes::from_static(body), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    let out = executor(st)
        .execute(
            "Transition",
            &payload("bkt", key, &vid, "GLACIER", Some("cold")),
        )
        .await
        .unwrap();
    assert_eq!(out, "archived");
    vid
}

#[tokio::test]
async fn reconcile_crash_windows() {
    let (st, _mock, _d) = setup().await;
    let vid = archive(&st, "r.txt", b"crash-data").await;

    // 窗口 1：Restoring 但无 row/job（sink 前崩溃）→ 回 Archived 并重排
    st.store
        .mark_restore_pending("bkt", "r.txt", &vid)
        .await
        .unwrap();
    st.store
        .restore_prepare("bkt", "r.txt", &vid, b"crash-data")
        .await
        .unwrap();
    let n = reconcile_restores(&st.db, &st.store, st.restore_sink.as_ref())
        .await
        .unwrap();
    assert!(n >= 1);
    let phase = st
        .store
        .tier_version("bkt", "r.txt", &vid)
        .await
        .unwrap()
        .unwrap()
        .tier
        .unwrap()
        .phase;
    assert_eq!(phase, TierPhase::Archived);
    let row = st
        .db
        .restore_row("bkt", "r.txt", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "pending");

    // 窗口 2：row=restored + 本地副本已写、meta 仍 Restoring（commit 前崩溃）→ 重放 commit
    st.store
        .mark_restore_pending("bkt", "r.txt", &vid)
        .await
        .unwrap();
    st.store
        .restore_prepare("bkt", "r.txt", &vid, b"crash-data")
        .await
        .unwrap();
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 3600;
    st.db
        .set_restore_state("bkt", "r.txt", &vid, "restored", expires)
        .await
        .unwrap();
    reconcile_restores(&st.db, &st.store, st.restore_sink.as_ref())
        .await
        .unwrap();
    let v = st
        .store
        .tier_version("bkt", "r.txt", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.tier.as_ref().unwrap().phase, TierPhase::Restored);
    assert_eq!(v.tier.as_ref().unwrap().restore_expires_at, Some(expires));
    assert_eq!(
        st.store
            .get_object("bkt", "r.txt")
            .await
            .unwrap()
            .1
            .to_vec(),
        b"crash-data"
    );
}

/// 缺省 versionId → 当前版本（版本桶）。
#[tokio::test]
async fn restore_without_version_id_targets_latest() {
    let (st, _mock, _d) = setup().await;
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = 'bkt'")
        .execute(st.db.pool())
        .await
        .unwrap();
    let stream: xl_store::store::ByteStream = Box::pin(futures::stream::once(async {
        Ok(Bytes::from_static(b"versioned"))
    }));
    let info = st
        .store
        .put_object_stream_with_meta(
            "bkt",
            "v.txt",
            stream,
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    executor(&st)
        .execute(
            "Transition",
            &payload("bkt", "v.txt", &vid, "GLACIER", Some("cold")),
        )
        .await
        .unwrap();
    let resp = service(&st)
        .restore_object(req(RestoreObjectInput {
            bucket: "bkt".into(),
            key: "v.txt".into(),
            version_id: None,
            restore_request: Some(RestoreRequest {
                days: Some(3),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(resp.status, Some(axum::http::StatusCode::ACCEPTED));
    let row = st
        .db
        .restore_row("bkt", "v.txt", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "pending");
}

/// 线上闭环：签名 POST `?restore` 经 build_router 返回 202 + `x-amz-restore`
/// （s3s 0.17 生成代码固定 200，router 层中间件补回 202）。
#[tokio::test]
async fn restore_object_over_http_returns_202_and_header() {
    let (st, _mock, _d) = setup().await;
    let vid = archive(&st, "archived.bin", b"arch-data").await;
    let uid = st.db.create_user("restorer", "pw", true).await.unwrap();
    st.db
        .create_access_key("AKIARESTORE00000001", "secret-restore", uid)
        .await
        .unwrap();
    let app = build_router(st.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let resp = signed_restore_post(
        &format!("http://{addr}"),
        "bkt",
        "archived.bin",
        "AKIARESTORE00000001",
        "secret-restore",
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(resp.headers()["x-amz-restore"], "ongoing-request=\"true\"");
    let row = st
        .db
        .restore_row("bkt", "archived.bin", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "pending");
}

/// M4 生命周期到期删除归档对象 → 远端副本同样回收。
#[tokio::test]
async fn lifecycle_delete_reclaims_remote() {
    let (st, mock, _d) = setup().await;
    let vid = archive(&st, "exp.txt", b"expire-me").await;
    st.db
        .set_lifecycle_rules(
            "bkt",
            &[oss_server::db::LifecycleRuleRow {
                bucket: "bkt".into(),
                rule_id: Some("r1".into()),
                prefix: String::new(),
                expire_days: 0,
                expiration_days: Some(0),
                transition_days: None,
                transition_class: None,
                transition_tier: None,
                enabled: true,
                updated_at: 0,
                config_json: None,
            }],
            0,
        )
        .await
        .unwrap();
    assert_eq!(oss_server::lifecycle::sweep_once(&st).await, 1);
    assert_eq!(
        mock.deleted.lock().unwrap().as_slice(),
        &[tier_key("cold", "bkt", &vid, "exp.txt")]
    );
}

/// 远端取回失败 → 版本回 Archived（重试/耗尽后都不会卡在 Restoring）。
#[tokio::test]
async fn restore_failure_returns_to_archived() {
    let (st, mock, _d) = setup().await;
    let vid = archive(&st, "fail.txt", b"fail-data").await;
    mock.objs.lock().unwrap().clear();
    let payload = serde_json::json!({
        "bucket": "bkt", "key": "fail.txt", "version_id": vid, "days": 1
    })
    .to_string();
    let err = executor(&st)
        .execute("RestoreObject", &payload)
        .await
        .unwrap_err();
    assert!(err.contains("missing"), "{err}");
    let v = st
        .store
        .tier_version("bkt", "fail.txt", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.tier.unwrap().phase, TierPhase::Archived);
}

/// N3：raw key 只编码一次；空格/`+`/`%2F`/中文可 PUT/GET 往返。
#[tokio::test]
async fn raw_key_encoded_once_at_repl_client() {
    use base64::Engine as _;

    type Store = Arc<Mutex<HashMap<String, Bytes>>>;
    async fn handler(
        axum::extract::State(objs): axum::extract::State<Store>,
        axum::extract::RawQuery(_q): axum::extract::RawQuery,
        method: axum::http::Method,
        uri: axum::http::Uri,
        headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> axum::response::Response {
        use axum::response::IntoResponse as _;
        let path = uri.path().to_string();
        match method {
            axum::http::Method::PUT => {
                let b64 = headers
                    .get("content-md5")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default();
                let md5 = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .unwrap();
                objs.lock().unwrap().insert(path, body);
                (
                    axum::http::StatusCode::OK,
                    [(
                        axum::http::header::ETAG,
                        format!("\"{}\"", hex::encode(md5)),
                    )],
                )
                    .into_response()
            }
            axum::http::Method::GET => match objs.lock().unwrap().get(&path) {
                Some(b) => (axum::http::StatusCode::OK, b.clone()).into_response(),
                None => axum::http::StatusCode::NOT_FOUND.into_response(),
            },
            axum::http::Method::HEAD => {
                let len = objs
                    .lock()
                    .unwrap()
                    .get(&path)
                    .map(|b| b.len())
                    .unwrap_or(0);
                axum::response::Response::builder()
                    .status(axum::http::StatusCode::OK)
                    .header(axum::http::header::CONTENT_LENGTH, len)
                    .body(axum::body::Body::empty())
                    .unwrap()
            }
            _ => axum::http::StatusCode::OK.into_response(),
        }
    }

    let objs: Store = Arc::new(Mutex::new(HashMap::new()));
    let app = axum::Router::new()
        .fallback(handler)
        .with_state(Arc::clone(&objs));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let target = tier_target(&format!("http://{addr}"));
    let client = Arc::new(oss_server::repl_client::ReplClient::new_tiers(
        std::slice::from_ref(&target),
    ));
    let backend = S3TierBackend::new(&target, client);
    let raw_key = "cold/bkt/null/dir/a b+c%2F中文.txt";
    let body = Bytes::from_static(b"encoded");
    let md5 = hex::encode(md5::Md5::digest(body.as_ref()));
    let etag = backend.put(raw_key, body.clone(), &md5).await.unwrap();
    assert_eq!(etag, md5);
    assert_eq!(backend.get(raw_key).await.unwrap(), body);
    assert_eq!(
        backend.head(raw_key).await.unwrap(),
        Some(body.len() as u64)
    );

    let expected = "/tb/cold/bkt/null/dir/a%20b%2Bc%252F%E4%B8%AD%E6%96%87.txt";
    assert!(
        objs.lock().unwrap().contains_key(expected),
        "远端路径应只编码一次：{expected}"
    );
}
