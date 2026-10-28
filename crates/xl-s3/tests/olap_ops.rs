//! M8 S2：Object Lambda——`/ap/<name>/<key>` 全链路（s3s S3Service → ApRoute → 内层 Op）。
//!
//! 变换器用进程内 axum mock；SSE/私网用例靠 `OSS_OLAP_ALLOW_PRIVATE=1`，SSRF 用例在锁内
//! 临时摘掉该变量验证请求期拒绝。所有会读写进程环境变量的用例共用一把异步互斥锁。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use base64::Engine as _;
use bytes::Bytes;
use s3s::S3Request;
use s3s::service::S3ServiceBuilder;
use xl_s3::ap::{AccessPoint, ApCache, ApPolicy, ApRoute, ApSource};
use xl_s3::olap::{OlapCache, OlapTransform};
use xl_s3::service::OssS3;
use xl_store::meta::{TierPhase, TierState};
use xl_store::store::{Store, StoreConfig, VersioningMode};

const BUCKET: &str = "bkt";
const AK: &str = "AKIA_TEST";

/// 环境变量是进程级；所有变换用例串行，SSRF/上限用例才能在锁内安全改 env。
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn env_guard(allow_private: bool) -> tokio::sync::MutexGuard<'static, ()> {
    let g = ENV_LOCK.lock().await;
    // SAFETY：仅测试进程；锁保证没有其它线程并发读写环境变量。
    unsafe {
        if allow_private {
            std::env::set_var("OSS_OLAP_ALLOW_PRIVATE", "1");
        } else {
            std::env::remove_var("OSS_OLAP_ALLOW_PRIVATE");
        }
        std::env::remove_var("OSS_OLAP_MAX_INPUT_BYTES");
        for k in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            std::env::remove_var(k);
        }
    }
    g
}

// ---------------- mock 变换器 ----------------

type Reply = Arc<dyn Fn(HeaderMap, Bytes) -> (StatusCode, HeaderMap, Bytes) + Send + Sync>;

#[derive(Clone)]
struct Mock {
    hits: Arc<AtomicUsize>,
    last: Arc<Mutex<Option<(HeaderMap, Bytes)>>>,
    reply: Reply,
    delay: Duration,
}

async fn mock_handler(State(m): State<Mock>, headers: HeaderMap, body: Bytes) -> Response {
    m.hits.fetch_add(1, Ordering::SeqCst);
    *m.last.lock().unwrap() = Some((headers.clone(), body.clone()));
    if !m.delay.is_zero() {
        tokio::time::sleep(m.delay).await;
    }
    let (status, h, b) = (m.reply)(headers, body);
    let mut resp = Response::new(axum::body::Body::from(b));
    *resp.status_mut() = status;
    *resp.headers_mut() = h;
    resp
}

async fn mock(
    reply: impl Fn(HeaderMap, Bytes) -> (StatusCode, HeaderMap, Bytes) + Send + Sync + 'static,
) -> (String, Mock) {
    mock_delayed(reply, Duration::ZERO).await
}

async fn mock_delayed(
    reply: impl Fn(HeaderMap, Bytes) -> (StatusCode, HeaderMap, Bytes) + Send + Sync + 'static,
    delay: Duration,
) -> (String, Mock) {
    let m = Mock {
        hits: Arc::new(AtomicUsize::new(0)),
        last: Arc::new(Mutex::new(None)),
        reply: Arc::new(reply),
        delay,
    };
    let app = Router::new()
        .route("/", post(mock_handler))
        .with_state(m.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/"), m)
}

fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(
            http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    h
}

fn upper_reply() -> impl Fn(HeaderMap, Bytes) -> (StatusCode, HeaderMap, Bytes) + Send + Sync {
    move |_, body| {
        let up: Vec<u8> = body.iter().map(u8::to_ascii_uppercase).collect();
        (
            StatusCode::OK,
            header_map(&[("content-type", "application/x-upper")]),
            up.into(),
        )
    }
}

// ---------------- 测试环境 ----------------

struct TestEnv {
    store: Arc<Store>,
    svc: s3s::service::S3Service,
    ap_cache: Arc<ApCache>,
    olap: Arc<OlapCache>,
    dir: tempfile::TempDir,
}

struct NoSource;

#[async_trait::async_trait]
impl ApSource for NoSource {
    async fn lookup(&self, _name: &str) -> Result<Option<AccessPoint>, String> {
        Ok(None)
    }
}

/// s3s 无 auth 时不会填 credentials；AP check_access 需要身份 → 测试路由注入固定 AK。
struct InjectCreds {
    inner: Arc<ApRoute>,
}

#[async_trait::async_trait]
impl s3s::route::S3Route for InjectCreds {
    fn is_match(
        &self,
        method: &http::Method,
        uri: &http::Uri,
        headers: &http::HeaderMap,
        extensions: &mut http::Extensions,
    ) -> bool {
        self.inner.is_match(method, uri, headers, extensions)
    }

    async fn check_access(&self, req: &mut S3Request<s3s::Body>) -> s3s::S3Result<()> {
        if req.credentials.is_none() {
            req.credentials = Some(s3s::auth::Credentials {
                access_key: AK.into(),
                secret_key: "s".into(),
            });
        }
        self.inner.check_access(req).await
    }

    async fn call(&self, req: S3Request<s3s::Body>) -> s3s::S3Result<s3s::S3Response<s3s::Body>> {
        self.inner.call(req).await
    }
}

const SCHEMA: &str = "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, password_hash TEXT, \
  is_admin INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT 0, \
  status TEXT NOT NULL DEFAULT 'active');\
 CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, \
  user_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, \
  created_at INTEGER NOT NULL DEFAULT 0, parent_ak TEXT, session_policy TEXT, \
  kind TEXT NOT NULL DEFAULT 'long-term');\
 CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL DEFAULT 0, \
  versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0, \
  object_lock INTEGER NOT NULL DEFAULT 0, object_lock_default_mode TEXT, \
  object_lock_default_days INTEGER, default_encryption TEXT, \
  replication_mode TEXT NOT NULL DEFAULT 'async', deleting INTEGER NOT NULL DEFAULT 0);\
 CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
  can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, \
  can_delete INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (user_id, bucket));\
 CREATE TABLE bucket_policies (bucket TEXT PRIMARY KEY, policy_json TEXT NOT NULL, \
  updated_at INTEGER NOT NULL DEFAULT 0);\
 CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, \
  uid INTEGER NOT NULL, op TEXT NOT NULL, bucket TEXT, object_key TEXT, \
  decision TEXT NOT NULL, detail TEXT);\
 INSERT INTO users (id,username,password_hash,is_admin) VALUES (1,'u','h',1);\
 INSERT INTO access_keys (access_key,secret_key,user_id) VALUES ('AKIA_TEST','s',1);\
 INSERT INTO buckets (name,owner_id,creation_date) VALUES ('bkt',1,0);";

async fn test_env() -> TestEnv {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    store.create_bucket(BUCKET).await.unwrap();
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
    let acl = xl_s3::access::Acl::new(pool);
    let s3 = OssS3::new(Arc::clone(&store), acl.clone(), 1 << 20);
    let ap_cache = Arc::new(ApCache::new());
    let olap = Arc::new(OlapCache::new());
    olap.mark_loaded(); // 模拟启动预加载成功（未加载态由 fail-closed 用例单独覆盖）
    let inner = Arc::new(S3ServiceBuilder::new(s3.clone()).build());
    let ap_route = Arc::new(ApRoute::new(
        Arc::clone(&ap_cache),
        Arc::new(NoSource),
        Arc::new(xl_s3::ap::StaticApEndpointSource::new([])),
        acl,
        Arc::clone(&olap),
        inner,
    ));
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_route(InjectCreds { inner: ap_route });
    TestEnv {
        store,
        svc: builder.build(),
        ap_cache,
        olap,
        dir,
    }
}

fn etag_hex(b: &[u8]) -> String {
    use md5::{Digest as _, Md5};
    hex::encode(Md5::digest(b))
}

fn get(path: &str, headers: &[(&str, &str)]) -> s3s::HttpRequest {
    let mut b = s3s::HttpRequest::builder().method("GET").uri(path);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    b.body(s3s::Body::empty()).unwrap()
}

fn method(m: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> s3s::HttpRequest {
    let mut b = s3s::HttpRequest::builder().method(m).uri(path);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    b.body(s3s::Body::from(Bytes::copy_from_slice(body)))
        .unwrap()
}

async fn send(env: &TestEnv, req: s3s::HttpRequest) -> (StatusCode, HeaderMap, Bytes) {
    let resp = env.svc.call(req).await.unwrap();
    let (parts, mut body) = resp.into_parts();
    let bytes = body.store_all_limited(8 << 20).await.unwrap_or_default();
    (parts.status, parts.headers, bytes)
}

async fn put_obj(
    env: &TestEnv,
    key: &str,
    body: &'static [u8],
    ct: Option<&str>,
    meta: &[(&str, &str)],
    sc: Option<&str>,
    mode: VersioningMode,
) -> xl_store::store::ObjectInfo {
    let metadata: BTreeMap<String, String> = meta
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    env.store
        .put_object_stream_with_meta(
            BUCKET,
            key,
            Box::pin(futures::stream::once(async move {
                Ok(Bytes::from_static(body))
            })),
            ct.map(str::to_string),
            metadata,
            None,
            mode,
            None,
            None,
            None,
            sc.map(str::to_string),
        )
        .await
        .unwrap()
}

fn add_ap(env: &TestEnv, name: &str) {
    // 4.4 起 AP 无策略默认拒绝；本套用例只测 OLAP 变换，显式放行数据面动作。
    let policy: ApPolicy = serde_json::from_value(serde_json::json!({"Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:*",
        "Resource": [format!("arn:aws:s3:::{BUCKET}/*"), format!("arn:aws:s3:::{BUCKET}")],
    }]}))
    .unwrap();
    env.ap_cache.put(AccessPoint {
        name: name.into(),
        bucket: BUCKET.into(),
        owner_id: 1,
        account: xl_s3::access::LEGACY_ACCOUNT.into(),
        policy: Some(policy),
        region: "us-east-1".into(),
    });
}

fn add_http_transform(env: &TestEnv, ap: &str, endpoint: &str, timeout_ms: i64, max: i64) {
    let t =
        OlapTransform::prepare("http", Some(endpoint.to_string()), None, timeout_ms, max).unwrap();
    env.olap.put(ap, BUCKET, t);
}

fn hv<'a>(h: &'a HeaderMap, name: &str) -> &'a str {
    h.get(name).and_then(|v| v.to_str().ok()).unwrap_or("")
}

fn key_b64(h: &HeaderMap) -> Vec<u8> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(hv(h, "x-oss-key-b64").as_bytes())
        .unwrap()
}

// ---------------- HTTP 变换器 ----------------

#[tokio::test]
async fn http_transform_full_path_headers_and_etag() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        Some("text/plain"),
        &[("foo", "bar")],
        Some("STANDARD_IA"),
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);

    let (st, h, body) = send(&env, get("/ap/ap1/hello", &[])).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&body[..], b"HELLO");
    assert_eq!(hv(&h, "content-length"), "5");
    assert_eq!(hv(&h, "etag"), format!("\"{}\"", etag_hex(b"HELLO")));
    assert_eq!(hv(&h, "content-type"), "application/x-upper");
    assert_eq!(hv(&h, "x-amz-meta-foo"), "bar");
    assert_eq!(hv(&h, "x-amz-storage-class"), "STANDARD_IA");
    assert!(h.contains_key("last-modified"));
    assert!(!h.contains_key("accept-ranges"));
    assert!(!h.contains_key("content-range"));

    assert_eq!(m.hits.load(Ordering::SeqCst), 1);
    let (mh, mb) = m.last.lock().unwrap().clone().unwrap();
    assert_eq!(hv(&mh, "x-oss-bucket"), BUCKET);
    assert_eq!(hv(&mh, "x-oss-size"), "5");
    assert_eq!(hv(&mh, "x-oss-etag"), etag_hex(b"hello"));
    assert_eq!(key_b64(&mh), b"hello");
    assert_eq!(&mb[..], b"hello");
}

#[tokio::test]
async fn version_id_and_encoded_key_forwarded() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(move |_, body| (StatusCode::OK, HeaderMap::new(), body)).await;
    let env = test_env().await;
    let info = put_obj(
        &env,
        "a b/中文.txt",
        b"v",
        Some("text/plain"),
        &[],
        None,
        VersioningMode::Enabled,
    )
    .await;
    let version_id = info.version_id.clone().unwrap();
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);

    let (st, h, body) = send(&env, get("/ap/ap1/a%20b/%E4%B8%AD%E6%96%87.txt", &[])).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&body[..], b"v");
    assert_eq!(hv(&h, "x-amz-version-id"), version_id);
    let (mh, _) = m.last.lock().unwrap().clone().unwrap();
    assert_eq!(hv(&mh, "x-oss-version-id"), version_id);
    assert_eq!(key_b64(&mh), "a b/中文.txt".as_bytes());
}

#[tokio::test]
async fn conditionals_short_circuit_before_transform() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);
    let etag = etag_hex(b"hello");

    let inm = format!("\"{etag}\"");
    let (st, _, _) = send(&env, get("/ap/ap1/hello", &[("if-none-match", &inm)])).await;
    assert_eq!(st, StatusCode::NOT_MODIFIED);

    let bad = "\"deadbeef\"";
    let (st, _, _) = send(&env, get("/ap/ap1/hello", &[("if-match", bad)])).await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED);
    assert_eq!(m.hits.load(Ordering::SeqCst), 0, "条件失败不得调用变换器");
}

#[tokio::test]
async fn range_and_part_number_are_400() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);

    let (st, _, body) = send(&env, get("/ap/ap1/hello", &[("range", "bytes=0-1")])).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("InvalidRequest"));

    let (st, _, _) = send(&env, get("/ap/ap1/hello?partNumber=1", &[])).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn head_passthrough_and_writes_are_405() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);

    let (st, h, _) = send(&env, method("HEAD", "/ap/ap1/hello", &[], b"")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hv(&h, "content-length"), "5", "HEAD 回原始元数据");
    assert_eq!(hv(&h, "etag"), format!("\"{}\"", etag_hex(b"hello")));

    let (st, _, _) = send(&env, method("PUT", "/ap/ap1/new", &[], b"x")).await;
    assert_eq!(st, StatusCode::METHOD_NOT_ALLOWED);
    let (st, _, _) = send(&env, method("DELETE", "/ap/ap1/hello", &[], b"")).await;
    assert_eq!(st, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ap_without_transform_passes_through() {
    let _g = env_guard(true).await;
    let (_, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "plain");
    let (st, _, body) = send(&env, get("/ap/plain/hello", &[])).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&body[..], b"hello");
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

/// 全量配置未成功加载时 fail-closed：AP 请求 500 且不直通原文；加载成功后恢复直通。
#[tokio::test]
async fn unloaded_transform_cache_fails_closed_until_loaded() {
    let _g = env_guard(true).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "plain");

    env.olap.mark_failed(); // 模拟预加载失败
    let (st, _, body) = send(&env, get("/ap/plain/hello", &[])).await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(String::from_utf8_lossy(&body).contains("InternalError"));
    assert!(
        !String::from_utf8_lossy(&body).contains("hello"),
        "未加载时不得直通原文: {body:?}"
    );

    env.olap.mark_loaded();
    let (st, _, body) = send(&env, get("/ap/plain/hello", &[])).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&body[..], b"hello", "加载成功后无变换器的 AP 应直通");
}

/// 钉住 304 还原所依赖的不变量：经 AP 的正常 200 GetObject 必带 ETag；条件 304 不调变换器。
#[tokio::test]
async fn success_200_carries_etag_and_304_skips_transform() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(&env, "empty", b"", None, &[], None, VersioningMode::Off).await;
    put_obj(&env, "text", b"abc", None, &[], None, VersioningMode::Off).await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);

    for key in ["empty", "text"] {
        let (st, h, _) = send(&env, get(&format!("/ap/ap1/{key}"), &[])).await;
        assert_eq!(st, StatusCode::OK);
        assert!(
            h.contains_key("etag"),
            "200 GetObject 必须带 ETag（304 还原依赖）: {key}"
        );
    }
    let hits = m.hits.load(Ordering::SeqCst);
    let inm = format!("\"{}\"", etag_hex(b"abc"));
    let (st, _, body) = send(&env, get("/ap/ap1/text", &[("if-none-match", &inm)])).await;
    assert_eq!(st, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
    assert_eq!(m.hits.load(Ordering::SeqCst), hits, "304 不得调用变换器");
}

#[tokio::test]
async fn upstream_error_is_500_without_leaking_endpoint() {
    let _g = env_guard(true).await;
    let (endpoint, _) = mock(|_, _| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            HeaderMap::new(),
            Bytes::from_static(b"upstream"),
        )
    })
    .await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);
    let (st, _, body) = send(&env, get("/ap/ap1/hello", &[])).await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("InternalError"), "{text}");
    assert!(
        !text.contains("127.0.0.1") && !text.contains(&endpoint),
        "{text}"
    );
    assert!(!text.contains("upstream"), "{text}");
}

#[tokio::test]
async fn timeout_is_enforced() {
    let _g = env_guard(true).await;
    let (endpoint, _) = mock_delayed(
        |_, body| (StatusCode::OK, HeaderMap::new(), body),
        Duration::from_secs(3),
    )
    .await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 1000, 1 << 20);
    let t0 = std::time::Instant::now();
    let (st, _, _) = send(&env, get("/ap/ap1/hello", &[])).await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        t0.elapsed() < Duration::from_millis(2500),
        "{:?}",
        t0.elapsed()
    );
}

#[tokio::test]
async fn output_limit_and_gzip_bomb_count_decompressed() {
    let _g = env_guard(true).await;
    let (endpoint, _) = mock(|_, _| {
        (
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::from(vec![b'x'; 2000]),
        )
    })
    .await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "big");
    add_http_transform(&env, "big", &endpoint, 5000, 100);
    let (st, _, _) = send(&env, get("/ap/big/hello", &[])).await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);

    // gzip：压缩体很小，解压后 1 MiB > 上限 1 KiB → 必须按解压后计数拒绝。
    use std::io::Write as _;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(&vec![0u8; 1 << 20]).unwrap();
    let gz = enc.finish().unwrap();
    let gz_len = gz.len();
    let (gz_endpoint, _) = mock(move |_, _| {
        (
            StatusCode::OK,
            header_map(&[
                ("content-encoding", "gzip"),
                ("content-length", &gz_len.to_string()),
            ]),
            Bytes::from(gz.clone()),
        )
    })
    .await;
    add_ap(&env, "bomb");
    add_http_transform(&env, "bomb", &gz_endpoint, 5000, 1024);
    let (st, _, _) = send(&env, get("/ap/bomb/hello", &[])).await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
}

/// gzip 由客户端解码后接受；其它编码无法安全透出 → 500（不得以原文 CT 发压缩字节）。
#[tokio::test]
async fn gzip_ok_and_unsupported_content_encoding_is_500() {
    let _g = env_guard(true).await;
    use std::io::Write as _;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(b"hello").unwrap();
    let gz = enc.finish().unwrap();
    let (gz_endpoint, _) = mock(move |_, _| {
        (
            StatusCode::OK,
            header_map(&[("content-encoding", "gzip")]),
            Bytes::from(gz.clone()),
        )
    })
    .await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "gz");
    add_http_transform(&env, "gz", &gz_endpoint, 5000, 1 << 20);
    let (st, h, body) = send(&env, get("/ap/gz/hello", &[])).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&body[..], b"hello");
    assert!(!h.contains_key("content-encoding"), "{h:?}");

    let (br_endpoint, _) = mock(|_, body| {
        (
            StatusCode::OK,
            header_map(&[("content-encoding", "br")]),
            body,
        )
    })
    .await;
    add_ap(&env, "br");
    add_http_transform(&env, "br", &br_endpoint, 5000, 1 << 20);
    let (st, _, body) = send(&env, get("/ap/br/hello", &[])).await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("InternalError"), "{text}");
    assert!(!text.contains("br"), "{text}");
}

/// 原对象带校验和时，变换后必须移除 `x-amz-checksum-*`（描述原文字节）。
#[tokio::test]
async fn checksum_headers_removed_after_transform() {
    let _g = env_guard(true).await;
    let (endpoint, _) = mock(upper_reply()).await;
    let env = test_env().await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);
    use md5::Digest as _;
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(b"hello"));
    let (st, _, _) = send(
        &env,
        method(
            "PUT",
            "/bkt/ck",
            &[("content-md5", &md5_b64), ("content-length", "5")],
            b"hello",
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    // 直读确认校验和头确实存在，避免断言落空。
    let (st, h, _) = send(&env, get("/bkt/ck", &[])).await;
    assert_eq!(st, StatusCode::OK);
    assert!(h.contains_key("x-amz-checksum-md5"), "{h:?}");

    let (st, h, body) = send(&env, get("/ap/ap1/ck", &[])).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&body[..], b"HELLO");
    assert!(
        !h.keys().any(|k| k.as_str().starts_with("x-amz-checksum-")),
        "变换后不得保留原校验和: {h:?}"
    );
}

#[tokio::test]
async fn redirect_is_not_followed() {
    let _g = env_guard(true).await;
    let (second, m2) = mock(|_, _| {
        (
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::from_static(b"SECOND"),
        )
    })
    .await;
    let (endpoint, _) = mock(move |_, _| {
        (
            StatusCode::FOUND,
            header_map(&[("location", second.as_str())]),
            Bytes::new(),
        )
    })
    .await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);
    let (st, _, _) = send(&env, get("/ap/ap1/hello", &[])).await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR, "3xx 不得当成功");
    assert_eq!(m2.hits.load(Ordering::SeqCst), 0, "不得跟随重定向");
}

#[tokio::test]
async fn private_mapped_and_localhost_rejected_at_request_time() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;

    // 保存时 ALLOW_PRIVATE=1 全放行；请求期摘掉变量后三类目标都必须拒。
    let mapped = endpoint.replace("127.0.0.1", "[::ffff:127.0.0.1]");
    let localhost = endpoint.replace("127.0.0.1", "localhost");
    for (ap, ep) in [
        ("ap1", endpoint.as_str()),
        ("ap2", mapped.as_str()),
        ("ap3", localhost.as_str()),
    ] {
        add_ap(&env, ap);
        add_http_transform(&env, ap, ep, 5000, 1 << 20);
    }
    unsafe { std::env::remove_var("OSS_OLAP_ALLOW_PRIVATE") };
    for ap in ["ap1", "ap2", "ap3"] {
        let (st, _, _) = send(&env, get(&format!("/ap/{ap}/hello"), &[])).await;
        assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR, "{ap}");
    }
    unsafe { std::env::set_var("OSS_OLAP_ALLOW_PRIVATE", "1") };
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn input_limit_is_enforced() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"01234567",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);
    unsafe { std::env::set_var("OSS_OLAP_MAX_INPUT_BYTES", "4") };
    let (st, _, body) = send(&env, get("/ap/ap1/hello", &[])).await;
    // 简报允许 400/413；s3s 的 EntityTooLarge 码在协议层固定映射 400。
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("EntityTooLarge"));
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn proxy_env_is_ignored() {
    let _g = env_guard(true).await;
    let (proxy, pm) = mock(|_, _| {
        (
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::from_static(b"PROXY"),
        )
    })
    .await;
    unsafe {
        std::env::set_var("HTTP_PROXY", proxy.trim_end_matches('/'));
        std::env::set_var("ALL_PROXY", proxy.trim_end_matches('/'));
    }
    let (endpoint, _) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "hello",
        b"hello",
        None,
        &[],
        None,
        VersioningMode::Off,
    )
    .await;
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);
    let (st, _, body) = send(&env, get("/ap/ap1/hello", &[])).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&body[..], b"HELLO");
    assert_eq!(pm.hits.load(Ordering::SeqCst), 0, "no_proxy 不得绕过钉扎");
}

#[tokio::test]
async fn archived_object_403_before_transform() {
    let _g = env_guard(true).await;
    let (endpoint, m) = mock(upper_reply()).await;
    let env = test_env().await;
    put_obj(
        &env,
        "cold",
        b"cold-data",
        None,
        &[],
        Some("GLACIER"),
        VersioningMode::Off,
    )
    .await;
    // 无公开 tier 变更 API（S4a 的 phase gate 只在存储内核单测里播种）——直接改盘上
    // xl.meta，走与 `Store::write_meta` 相同的 encode 格式。
    let meta_path = xl_store::layout::object_dir(env.dir.path(), BUCKET, "cold")
        .unwrap()
        .join(xl_store::meta::META_FILE);
    let mut md = xl_store::meta::XlMeta::decode(&std::fs::read(&meta_path).unwrap()).unwrap();
    md.versions.last_mut().unwrap().tier = Some(TierState {
        backend: "s3".into(),
        key: "cold/bkt/cold".into(),
        size: 9,
        etag: "e".into(),
        sha256: "s".into(),
        phase: TierPhase::Archived,
        archived_at: 100,
        restore_expires_at: None,
        chunks: None,
    });
    std::fs::write(&meta_path, md.encode().unwrap()).unwrap();
    add_ap(&env, "ap1");
    add_http_transform(&env, "ap1", &endpoint, 5000, 1 << 20);

    let (st, _, body) = send(&env, get("/ap/ap1/cold", &[])).await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert!(String::from_utf8_lossy(&body).contains("InvalidObjectState"));
    assert_eq!(m.hits.load(Ordering::SeqCst), 0);
}

// ---------------- WASM（feature 门控） ----------------

#[cfg(feature = "olap-wasm")]
mod wasm {
    use super::*;

    const IDENTITY: &str = r#"
        (module
          (memory (export "memory") 2)
          (global $next (mut i32) (i32.const 1024))
          (func $alloc (export "alloc") (param $len i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $next))
            (global.set $next (i32.add (global.get $next) (local.get $len)))
            (local.get $p))
          (func (export "dealloc") (param i32 i32))
          (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
            (local $out i32)
            (local.set $out (call $alloc (local.get $len)))
            (memory.copy (local.get $out) (local.get $ptr) (local.get $len))
            (i64.or
              (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
              (i64.extend_i32_u (local.get $len)))))
    "#;

    const FIXED: &str = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 8) "Z")
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "dealloc") (param i32 i32))
          (func (export "transform") (param i32 i32) (result i64)
            (i64.or (i64.shl (i64.const 8) (i64.const 32)) (i64.const 1))))
    "#;

    const LOOP: &str = r#"
        (module
          (memory (export "memory") 1)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "dealloc") (param i32 i32))
          (func (export "transform") (param i32 i32) (result i64)
            (loop $l (br $l))
            (i64.const 0)))
    "#;

    const BAD_ABI: &str = r#"(module (memory (export "memory") 1))"#;

    const MULTI_MEMORY: &str = r#"
        (module
          (memory (export "memory") 1)
          (memory 1)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "dealloc") (param i32 i32))
          (func (export "transform") (param i32 i32) (result i64) (i64.const 0)))
    "#;

    const HUGE_TABLE: &str = r#"
        (module
          (memory (export "memory") 1)
          (table 200000 funcref)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "dealloc") (param i32 i32))
          (func (export "transform") (param i32 i32) (result i64) (i64.const 0)))
    "#;

    const OOB: &str = r#"
        (module
          (memory (export "memory") 1)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "dealloc") (param i32 i32))
          (func (export "transform") (param i32 i32) (result i64)
            (i64.or (i64.shl (i64.const 2147483647) (i64.const 32)) (i64.const 1))))
    "#;

    fn add_wasm(env: &TestEnv, ap: &str, wat: &str, timeout_ms: i64) {
        let t = OlapTransform::prepare("wasm", None, Some(wat.as_bytes()), timeout_ms, 1 << 20)
            .unwrap();
        env.olap.put(ap, BUCKET, t);
    }

    async fn setup() -> TestEnv {
        let env = test_env().await;
        put_obj(
            &env,
            "hello",
            b"hello",
            Some("text/plain"),
            &[],
            None,
            VersioningMode::Off,
        )
        .await;
        add_ap(&env, "ap1");
        env
    }

    #[tokio::test]
    async fn identity_module_roundtrip_and_cache_replace() {
        let _g = env_guard(true).await;
        let env = setup().await;
        add_wasm(&env, "ap1", IDENTITY, 5000);
        let (st, h, body) = send(&env, get("/ap/ap1/hello", &[])).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(&body[..], b"hello");
        assert_eq!(hv(&h, "etag"), format!("\"{}\"", etag_hex(b"hello")));
        assert_eq!(hv(&h, "content-type"), "text/plain");

        // 更新变换器（新 Module 入缓存，旧 Module 失效）
        add_wasm(&env, "ap1", FIXED, 5000);
        let (st, _, body) = send(&env, get("/ap/ap1/hello", &[])).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(&body[..], b"Z");
    }

    #[tokio::test]
    async fn fuel_and_epoch_stop_runaway_module() {
        let _g = env_guard(true).await;
        let env = setup().await;
        add_wasm(&env, "ap1", LOOP, 1000);
        let t0 = std::time::Instant::now();
        let (st, _, _) = send(&env, get("/ap/ap1/hello", &[])).await;
        assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            t0.elapsed() < Duration::from_millis(2500),
            "{:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn bad_abi_and_out_of_bounds_fail_500() {
        let _g = env_guard(true).await;
        let env = setup().await;
        add_wasm(&env, "ap1", BAD_ABI, 5000);
        let (st, _, _) = send(&env, get("/ap/ap1/hello", &[])).await;
        assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
        add_wasm(&env, "ap1", OOB, 5000);
        let (st, _, _) = send(&env, get("/ap/ap1/hello", &[])).await;
        assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// 多 memory 在引擎层禁用；巨大 table 被 StoreLimits 拒绝 → 500。
    #[tokio::test]
    async fn multi_memory_and_huge_table_are_rejected() {
        let _g = env_guard(true).await;
        let e =
            match OlapTransform::prepare("wasm", None, Some(MULTI_MEMORY.as_bytes()), 5000, 1024) {
                Ok(_) => panic!("multi-memory 应被拒"),
                Err(e) => e,
            };
        assert!(!e.is_empty());
        let env = setup().await;
        add_wasm(&env, "ap1", HUGE_TABLE, 5000);
        let (st, _, _) = send(&env, get("/ap/ap1/hello", &[])).await;
        assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn oversized_module_rejected_at_prepare() {
        let _g = env_guard(true).await;
        let big = vec![0u8; xl_s3::olap::MAX_MODULE_BYTES + 1];
        let e = match OlapTransform::prepare("wasm", None, Some(&big), 5000, 1024) {
            Ok(_) => panic!("超大模块应被拒"),
            Err(e) => e,
        };
        assert!(e.contains("上限"), "{e}");
    }
}

#[cfg(not(feature = "olap-wasm"))]
#[test]
fn wasm_config_rejected_without_feature() {
    let e = match OlapTransform::prepare("wasm", None, Some(b"(module)"), 5000, 1024) {
        Ok(_) => panic!("无 feature 时 wasm 配置应被拒"),
        Err(e) => e,
    };
    assert!(e.contains("olap-wasm"), "{e}");
}
