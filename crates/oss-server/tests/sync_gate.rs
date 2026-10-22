//! M9 S1：sync 桶 remote-first PutObject 进程内验收。
//!
//! 覆盖：成功（远端收到同体/同 ETag/同 origin id，远端回同 id → 本地可见、
//! `COMPLETED`、无站点 outbox）；远端失败/超时/无版本 → 503，版本 id 不符 → 502，
//! 均本地不可见、tee 清理、无 outbox；0/2 peer、删桶中 → 503，versioning 未就绪 →
//! 409；sync 上限 → 400；async 桶路径逐字节不变（照常入队）；接收端 token 校验与
//! origin id 采纳/重放/冲突（409）；MPU Complete 与 CopyObject 的远端先确认与
//! 元数据透传；不支持组合的 4xx/501 边界；补偿删除走本地站点路由（不回环）。
//!
//! SigV4 用 s3s-sigv4 规范原语在测试内最小实现（同 m8_ap）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use md5::{Digest as _, Md5};
use oss_server::config::{AppConfig, SiteToml, SyncToml};
use oss_server::db::{Db, PermissionRow, SitePeer};
use oss_server::lifecycle::sweep_once;
use oss_server::state::AppState;
use oss_server::sync::{SiteSyncGate, SyncMetrics, recover_delete_intents, recover_write_intents};
use reqwest::Method;
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::Sha256;
use xl_store::store::{Store, StoreConfig};

const REGION: &str = "us-east-1";
const AK: &str = "AKIAALICE000000000001";
const SK: &str = "SKALICE000000000001";
const LOCAL_TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PEER_TOKEN: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
/// 门超时（毫秒）与 mock 慢响应（毫秒，> 超时即触发 503）。
const TIMEOUT_MS: u64 = 1000;
const SLOW_MS: u64 = 1400;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Ok,
    Fail,
    Slow,
    WrongId,
    NoVersion,
    Conflict,
    BadRequest,
}

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    uri: String,
    body: Vec<u8>,
    authorization: Option<String>,
    token: Option<String>,
    origin: Option<String>,
    replication: Option<String>,
    content_type: Option<String>,
    meta_foo: Option<String>,
    tagging: Option<String>,
    storage_class: Option<String>,
    checksum_sha256: Option<String>,
}

/// M9 S3 mock 对端状态：内部路由只按需回应（不是完整 site 模拟）。
#[derive(Default)]
struct MockState {
    /// version_id → HEAD 内部路由状态（200 存在 / 405 delete marker；缺省 404）。
    head: HashMap<String, u16>,
    /// 对端桶非空（`GET /site/bucket-empty` → 409）。
    bucket_nonempty: bool,
    /// 仅该键的 token 删除返回 500（DeleteObjects 部分失败）。
    fail_delete_key: Option<String>,
    /// 仅 PUT 的延迟（毫秒；in-flight 写者屏障测试）。
    put_delay_ms: u64,
    /// PUT 延迟结束后返回 500（造「在途但最终失败」的写者，不提交对象）。
    fail_put: bool,
    /// 站点 delete-object 路由延迟（毫秒；> TIMEOUT_MS 触发客户端超时，I2）。
    delete_delay_ms: u64,
    /// 站点 bucket-quiesce（非 release）延迟（毫秒；并发删桶测试）。
    quiesce_delay_ms: u64,
    /// 站点 bucket-quiesce（非 release）返回 500（远端 quiesce 失败回滚测试）。
    fail_quiesce: bool,
}

struct MockRemote {
    base: String,
    mode: Arc<Mutex<Mode>>,
    seen: Arc<Mutex<Vec<Seen>>>,
    state: Arc<Mutex<MockState>>,
}

impl MockRemote {
    fn puts(&self) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.method == "PUT")
            .cloned()
            .collect()
    }

    fn deletes(&self) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.method == "DELETE")
            .cloned()
            .collect()
    }

    fn calls(&self) -> usize {
        self.seen.lock().unwrap().len()
    }

    fn set_mode(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn set_head(&self, version_id: &str, status: u16) {
        self.state
            .lock()
            .unwrap()
            .head
            .insert(version_id.to_string(), status);
    }

    fn set_bucket_nonempty(&self, nonempty: bool) {
        self.state.lock().unwrap().bucket_nonempty = nonempty;
    }

    fn fail_delete_key(&self, key: &str) {
        self.state.lock().unwrap().fail_delete_key = Some(key.to_string());
    }

    fn set_put_delay_ms(&self, ms: u64) {
        self.state.lock().unwrap().put_delay_ms = ms;
    }

    fn fail_put(&self) {
        self.state.lock().unwrap().fail_put = true;
    }

    fn set_delete_delay_ms(&self, ms: u64) {
        self.state.lock().unwrap().delete_delay_ms = ms;
    }

    fn set_quiesce_delay_ms(&self, ms: u64) {
        self.state.lock().unwrap().quiesce_delay_ms = ms;
    }

    fn fail_quiesce(&self) {
        self.state.lock().unwrap().fail_quiesce = true;
    }

    /// 匹配某站点内部路由的 POST（delete-object / bucket-quiesce / delete-bucket）。
    fn site_posts(&self, path: &str) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.method == "POST" && s.uri.contains(path))
            .cloned()
            .collect()
    }

    /// M9 S2/S3：补偿删除只允许走站点内部路由（POST），不得发普通 S3 DELETE（会回环）。
    fn site_deletes(&self) -> Vec<Seen> {
        self.site_posts("/api/v1/site/delete-object")
    }

    fn quiesces(&self) -> Vec<Seen> {
        self.site_posts("/api/v1/site/bucket-quiesce")
            .into_iter()
            .filter(|s| !String::from_utf8_lossy(&s.body).contains("\"release\":true"))
            .collect()
    }

    fn quiesce_releases(&self) -> Vec<Seen> {
        self.site_posts("/api/v1/site/bucket-quiesce")
            .into_iter()
            .filter(|s| String::from_utf8_lossy(&s.body).contains("\"release\":true"))
            .collect()
    }
}

/// 测试用查询串取值（本文件只用简单 ASCII 键，不做 percent-decode）。
fn qparam(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == name).then(|| v.to_string())
    })
}

struct Env {
    st: AppState,
    base: String,
    http: reqwest::Client,
    remote: MockRemote,
    /// 与门/AppState 共享的 `/metrics` 计数（生产由 load 注入，测试门先建）。
    metrics: Arc<SyncMetrics>,
    tee_dir: std::path::PathBuf,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

async fn spawn_remote(mode: Mode) -> MockRemote {
    use hyper::service::service_fn;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mode = Arc::new(Mutex::new(mode));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(Mutex::new(MockState::default()));
    let (mode2, seen2, state2) = (Arc::clone(&mode), Arc::clone(&seen), Arc::clone(&state));
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let (mode, seen, state) = (Arc::clone(&mode2), Arc::clone(&seen2), Arc::clone(&state2));
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let (mode, seen, state) =
                        (Arc::clone(&mode), Arc::clone(&seen), Arc::clone(&state));
                    async move {
                        let method = req.method().to_string();
                        let hdr = |n: &str| {
                            req.headers()
                                .get(n)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string)
                        };
                        let uri = req.uri().to_string();
                        let path = req.uri().path().to_string();
                        let query = req.uri().query().unwrap_or("").to_string();
                        let authorization = hdr("authorization");
                        let token = hdr("x-oss-site-token");
                        let origin = hdr("x-oss-origin-version-id");
                        let replication = hdr("x-amz-replication-status");
                        let content_type = hdr("content-type");
                        let meta_foo = hdr("x-amz-meta-foo");
                        let tagging = hdr("x-amz-tagging");
                        let storage_class = hdr("x-amz-storage-class");
                        let checksum_sha256 = hdr("x-amz-checksum-sha256");
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        seen.lock().unwrap().push(Seen {
                            method: method.clone(),
                            uri,
                            body: body.to_vec(),
                            authorization,
                            token,
                            origin: origin.clone(),
                            replication,
                            content_type,
                            meta_foo,
                            tagging,
                            storage_class,
                            checksum_sha256,
                        });
                        let m = *mode.lock().unwrap();
                        let mut resp = Response::new(Full::new(Bytes::new()));
                        if m == Mode::Slow {
                            tokio::time::sleep(Duration::from_millis(SLOW_MS)).await;
                        }
                        let (put_delay, fail_put) = {
                            let st = state.lock().unwrap();
                            (st.put_delay_ms, st.fail_put)
                        };
                        if method == "PUT" && put_delay > 0 {
                            tokio::time::sleep(Duration::from_millis(put_delay)).await;
                        }
                        if method == "PUT" && fail_put {
                            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                        } else if method == "PUT" {
                            match m {
                                Mode::Fail => {
                                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR
                                }
                                Mode::Slow | Mode::Ok => {
                                    let etag = hex::encode(Md5::digest(&body));
                                    resp.headers_mut()
                                        .insert("etag", format!("\"{etag}\"").parse().unwrap());
                                    if let Some(id) = origin {
                                        resp.headers_mut()
                                            .insert("x-amz-version-id", id.parse().unwrap());
                                    }
                                }
                                Mode::WrongId => {
                                    let etag = hex::encode(Md5::digest(&body));
                                    resp.headers_mut()
                                        .insert("etag", format!("\"{etag}\"").parse().unwrap());
                                    resp.headers_mut().insert(
                                        "x-amz-version-id",
                                        "someone-else".parse().unwrap(),
                                    );
                                }
                                Mode::NoVersion => {
                                    let etag = hex::encode(Md5::digest(&body));
                                    resp.headers_mut()
                                        .insert("etag", format!("\"{etag}\"").parse().unwrap());
                                }
                                Mode::Conflict => *resp.status_mut() = StatusCode::CONFLICT,
                                Mode::BadRequest => *resp.status_mut() = StatusCode::BAD_REQUEST,
                            }
                        } else if path == "/api/v1/site/delete-object" {
                            // M9 S3：token 删除（版本/同 id marker/整键）。
                            let delay = state.lock().unwrap().delete_delay_ms;
                            if delay > 0 {
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                            }
                            let fail_key = state.lock().unwrap().fail_delete_key.clone();
                            let v: serde_json::Value =
                                serde_json::from_slice(&body).unwrap_or_default();
                            let key = v.get("key").and_then(|k| k.as_str()).unwrap_or("");
                            if m == Mode::Fail || fail_key.as_deref() == Some(key) {
                                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            } else {
                                *resp.status_mut() = StatusCode::NO_CONTENT;
                            }
                        } else if method == "HEAD" && path == "/api/v1/site/head-object" {
                            if m == Mode::Fail {
                                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            } else {
                                let vid = qparam(&query, "version_id").unwrap_or_default();
                                let code =
                                    state.lock().unwrap().head.get(&vid).copied().unwrap_or(404);
                                *resp.status_mut() = StatusCode::from_u16(code).unwrap();
                            }
                        } else if method == "GET" && path == "/api/v1/site/bucket-empty" {
                            if m == Mode::Fail {
                                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            } else if state.lock().unwrap().bucket_nonempty {
                                *resp.status_mut() = StatusCode::CONFLICT;
                            } else {
                                *resp.status_mut() = StatusCode::NO_CONTENT;
                            }
                        } else if path == "/api/v1/site/bucket-quiesce" {
                            let v: serde_json::Value =
                                serde_json::from_slice(&body).unwrap_or_default();
                            let release =
                                v.get("release").and_then(|b| b.as_bool()).unwrap_or(false);
                            let (delay, fail) = {
                                let st = state.lock().unwrap();
                                (st.quiesce_delay_ms, st.fail_quiesce)
                            };
                            if !release && delay > 0 {
                                tokio::time::sleep(Duration::from_millis(delay)).await;
                            }
                            if m == Mode::Fail || (!release && fail) {
                                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            } else {
                                *resp.status_mut() = StatusCode::NO_CONTENT;
                            }
                        } else if path == "/api/v1/site/delete-bucket" {
                            if m == Mode::Fail {
                                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            } else {
                                *resp.status_mut() = StatusCode::NO_CONTENT;
                            }
                        }
                        Ok::<_, std::convert::Infallible>(resp)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    MockRemote {
        base: format!("http://{addr}"),
        mode,
        seen,
        state,
    }
}

fn peer(endpoint: &str) -> SitePeer {
    SitePeer {
        peer_id: "b".into(),
        remote_site_id: "site-b".into(),
        name: "b".into(),
        endpoint: endpoint.into(),
        region: REGION.into(),
        priority: 0,
        s3_access_key: "AKIAPEEER00000000001".into(),
        s3_secret_key: "SKPEER0000000000001".into(),
        peer_site_token: PEER_TOKEN.into(),
        created_at: 0,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

/// 文件库（两个连接共享：门一个、AppState 一个；内存库不共享）。
async fn env(mode: Mode, sync_max: u64) -> Env {
    env_inner(mode, sync_max, true).await
}

async fn env_inner(mode: Mode, sync_max: u64, site_enabled: bool) -> Env {
    let remote = spawn_remote(mode).await;
    let dbdir = tempfile::tempdir().unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}?mode=rwc", dbdir.path().join("t.db").display());
    let gate_db = Arc::new(Db::connect(&db_url).await.unwrap());
    gate_db.insert_site(&peer(&remote.base)).await.unwrap();
    let tee_dir = datadir.path().join("tee");
    let metrics = Arc::new(SyncMetrics::default());
    let mut sc = StoreConfig::new(datadir.path());
    sc.gate = Some(Arc::new(
        SiteSyncGate::new(
            Arc::clone(&gate_db),
            Duration::from_millis(TIMEOUT_MS),
            sync_max,
            site_enabled,
        )
        .with_metrics(Arc::clone(&metrics)),
    ));
    sc.sync_max_bytes = sync_max;
    sc.tee_dir = Some(tee_dir.clone());
    let store = Store::open(sc).await.unwrap();
    let cfg = AppConfig {
        listen: "127.0.0.1:0".into(),
        data_dir: datadir.path().join("data"),
        data_dirs: None,
        ec: Default::default(),
        staging_dir: datadir.path().join("staging"),
        database_url: db_url.clone(),
        jwt_secret: "0123456789abcdef0123456789abcdef".into(),
        max_put_bytes: 1024 * 1024,
        groups: vec![],
        cluster: None,
        kafka: Default::default(),
        remotes: vec![],
        tiers: vec![],
        site: site_enabled.then(|| SiteToml {
            id: None,
            name: Some("a".into()),
            region: REGION.into(),
            priority: 0,
            token: Some(LOCAL_TOKEN.into()),
        }),
        raft: None,
        sync: SyncToml {
            max_object_bytes: sync_max,
            timeout_ms: TIMEOUT_MS,
        },
        stretch: None,
        tls: Default::default(),
    };
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let db = Db::connect(&db_url).await.unwrap();
    let mut st = AppState::new(cfg, db, store).await;
    // 与生产 `load` 同口径：/metrics 读的就是门里的同一份计数。
    st.sync_metrics = Arc::clone(&metrics);
    let uid = st.db.create_user("alice", "pw", false).await.unwrap();
    st.db.create_access_key(AK, SK, uid).await.unwrap();
    st.db
        .set_permissions(
            uid,
            &[
                PermissionRow {
                    bucket: "syn".into(),
                    read: true,
                    write: true,
                    delete: true,
                },
                PermissionRow {
                    bucket: "asy".into(),
                    read: true,
                    write: true,
                    delete: true,
                },
            ],
        )
        .await
        .unwrap();
    make_bucket(&st, "syn", uid, "sync").await;
    make_bucket(&st, "asy", uid, "async").await;
    let app = oss_server::router::build_router(st.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Env {
        st,
        base: format!("http://{addr}"),
        http: reqwest::Client::new(),
        remote,
        metrics,
        tee_dir,
        _dirs: (dbdir, datadir),
    }
}

async fn make_bucket(st: &AppState, name: &str, uid: i64, mode: &str) {
    st.store.create_bucket(name).await.unwrap();
    st.db.insert_bucket(name, Some(uid)).await.unwrap();
    set_bucket(st, name, "replication_mode", mode).await;
    // sync 依赖双站同 id → 必须 versioning Enabled；async 站点复制同理（M7 口径）。
    set_bucket(st, name, "versioning", "Enabled").await;
    st.store.invalidate_gate(name);
}

async fn set_bucket(st: &AppState, name: &str, col: &str, val: &str) {
    assert!(matches!(
        col,
        "replication_mode" | "versioning" | "deleting" | "object_lock"
    ));
    let sql = format!("UPDATE buckets SET {col} = ? WHERE name = ?");
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(val)
        .bind(name)
        .execute(st.db.pool())
        .await
        .unwrap();
}

async fn outbox(st: &AppState, bucket: &str, key: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE target_kind='site' AND bucket = ? AND key = ?",
    )
    .bind(bucket)
    .bind(key)
    .fetch_one(st.db.pool())
    .await
    .unwrap()
}

fn tee_entries(e: &Env) -> usize {
    std::fs::read_dir(&e.tee_dir)
        .map(|it| it.count())
        .unwrap_or(0)
}

// ---------- 最小 SigV4（header 签名；payload 单块哈希） ----------

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

async fn signed(
    e: &Env,
    method: Method,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> reqwest::Response {
    let url = format!("{}{}", e.base, path);
    let mut rb = e.http.request(method.clone(), &url);
    for (k, v) in headers {
        rb = rb.header(*k, *v);
    }
    let mut req = rb.body(body.to_vec()).build().unwrap();

    let payload_hex = hex::encode(Sha256::digest(body));
    let amz = amz_now();
    let host = url
        .split("://")
        .nth(1)
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_string();
    let mut to_sign: Vec<(String, String)> = vec![
        ("host".into(), host),
        ("x-amz-content-sha256".into(), payload_hex.clone()),
        ("x-amz-date".into(), amz.fmt_iso8601().to_string()),
    ];
    for (k, v) in headers {
        let name = k.to_ascii_lowercase();
        if !to_sign.iter().any(|(n, _)| *n == name) {
            to_sign.push((name, v.to_string()));
        }
    }
    to_sign.sort_by(|a, b| a.0.cmp(&b.0));
    let refs: Vec<(&str, &str)> = to_sign
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    // M9 S2：MPU/子资源请求带查询串，必须一起进规范请求。
    let (path_only, query) = path.split_once('?').unwrap_or((path, ""));
    let pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (kv.to_string(), String::new()),
        })
        .collect();
    let canonical = create_canonical_request(
        method.as_str(),
        path_only,
        &pairs,
        &refs,
        Payload::SingleChunk(&payload_hex),
    );
    let sts = create_string_to_sign(&canonical, &amz, REGION, "s3");
    let sig = calculate_signature(&sts, SK, &amz, REGION, "s3");
    let names: Vec<&str> = to_sign.iter().map(|(k, _)| k.as_str()).collect();
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={AK}/{}/{REGION}/s3/aws4_request, SignedHeaders={}, Signature={sig}",
        amz.fmt_date(),
        names.join(";")
    );
    req.headers_mut()
        .insert("x-amz-date", amz.fmt_iso8601().parse().unwrap());
    req.headers_mut()
        .insert("x-amz-content-sha256", payload_hex.parse().unwrap());
    req.headers_mut()
        .insert(reqwest::header::AUTHORIZATION, auth.parse().unwrap());
    e.http.execute(req).await.expect("HTTP 请求发送失败")
}

async fn put(e: &Env, path: &str, body: &[u8]) -> reqwest::Response {
    signed(
        e,
        Method::PUT,
        path,
        &[("content-type", "text/plain")],
        body,
    )
    .await
}

// ---------- 用例 ----------

#[tokio::test]
async fn sync_put_remote_first_success() {
    let e = env(Mode::Ok, 1 << 20).await;
    let body = b"hello sync".to_vec();
    let r = signed(
        &e,
        Method::PUT,
        "/syn/hello.txt",
        &[("content-type", "text/plain"), ("x-amz-meta-foo", "bar")],
        &body,
    )
    .await;
    assert_eq!(r.status(), 200);
    let vid = r
        .headers()
        .get("x-amz-version-id")
        .expect("sync PUT 必须返回 version id")
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        r.headers().get("etag").unwrap().to_str().unwrap(),
        format!("\"{}\"", hex::encode(Md5::digest(&body)))
    );
    // 远端先收到：同体、同 ETag 来源、同 origin id、站点 token、REPLICA 标记。
    let seen = e.remote.puts();
    assert_eq!(seen.len(), 1, "sync 写只允许一次远端 PUT");
    assert_eq!(seen[0].body, body);
    assert_eq!(seen[0].origin.as_deref(), Some(vid.as_str()));
    assert_eq!(seen[0].token.as_deref(), Some(PEER_TOKEN));
    // 自定义头必须进 SigV4 SignedHeaders（否则可被中间人改写）。
    let auth = seen[0]
        .authorization
        .as_deref()
        .expect("远端 PUT 必须带签名");
    assert!(
        auth.contains("x-oss-site-token") && auth.contains("x-oss-origin-version-id"),
        "SignedHeaders 缺站点头: {auth}"
    );
    assert_eq!(seen[0].replication.as_deref(), Some("REPLICA"));
    assert_eq!(seen[0].content_type.as_deref(), Some("text/plain"));
    assert_eq!(seen[0].meta_foo.as_deref(), Some("bar"));
    // 本地已翻转：可读、COMPLETED、无站点 outbox、tee 已清。
    let g = signed(&e, Method::GET, "/syn/hello.txt", &[], &[]).await;
    assert_eq!(g.status(), 200);
    assert_eq!(g.bytes().await.unwrap().as_ref(), body.as_slice());
    let h = signed(&e, Method::HEAD, "/syn/hello.txt", &[], &[]).await;
    assert_eq!(h.status(), 200);
    assert_eq!(
        h.headers()
            .get("x-amz-replication-status")
            .expect("HEAD 应带复制状态"),
        "COMPLETED"
    );
    assert_eq!(outbox(&e.st, "syn", "hello.txt").await, 0);
    assert_eq!(tee_entries(&e), 0);
}

/// post-release 观测：sync 提交门 acked/failed 随远端结果变化，且 `/metrics` 可见。
#[tokio::test]
async fn sync_gate_acked_and_failed_counters_move() {
    let e = env(Mode::Ok, 1 << 20).await;
    assert_eq!(e.metrics.acked_total(), 0);
    assert_eq!(e.metrics.failed_total(), 0);

    assert_eq!(put(&e, "/syn/acked.txt", b"ok").await.status(), 200);
    assert_eq!(e.metrics.acked_total(), 1);
    assert_eq!(e.metrics.failed_total(), 0);

    e.remote.fail_put();
    assert_eq!(put(&e, "/syn/failed.txt", b"x").await.status(), 503);
    assert_eq!(e.metrics.acked_total(), 1, "失败不得计入 acked");
    assert_eq!(e.metrics.failed_total(), 1);

    let body = e
        .http
        .get(format!("{}/metrics", e.base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("oss_sync_acked_total 1"), "{body}");
    assert!(body.contains("oss_sync_failed_total 1"), "{body}");
}

/// M9 S2：PutObject 的 tags/storage-class/checksum 必须随门 PUT 到达远端；
/// CopyObject 目标走同一门（远端先确认、同 id），并复制 tags/metadata/storage-class。
#[tokio::test]
async fn sync_copy_forwards_metadata_and_roundtrips() {
    let e = env(Mode::Ok, 1 << 20).await;
    let body = b"copy me".to_vec();
    let sum = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&body));
    let src_h = [
        ("content-type", "text/plain"),
        ("x-amz-tagging", "k1=v1&k2=v2"),
        ("x-amz-storage-class", "STANDARD_IA"),
        ("x-amz-checksum-sha256", sum.as_str()),
        ("x-amz-meta-foo", "bar"),
    ];
    let r = signed(&e, Method::PUT, "/syn/src.txt", &src_h, &body).await;
    assert_eq!(r.status(), 200);
    let src_vid = r
        .headers()
        .get("x-amz-version-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let seen = e.remote.puts();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].tagging.as_deref(), Some("k1=v1&k2=v2"));
    assert_eq!(seen[0].storage_class.as_deref(), Some("STANDARD_IA"));
    assert_eq!(seen[0].checksum_sha256.as_deref(), Some(sum.as_str()));

    let r = signed(
        &e,
        Method::PUT,
        "/syn/dst.txt",
        &[
            ("x-amz-copy-source", "/syn/src.txt"),
            // Copy 的存储类是目标属性（AWS：缺省 STANDARD，不继承源）；显式给出以验证透传。
            ("x-amz-storage-class", "STANDARD_IA"),
        ],
        &[],
    )
    .await;
    assert_eq!(r.status(), 200);
    let dst_vid = r
        .headers()
        .get("x-amz-version-id")
        .expect("sync copy 必须返回目标 version id")
        .to_str()
        .unwrap()
        .to_string();
    assert_ne!(dst_vid, src_vid);
    let seen = e.remote.puts();
    assert_eq!(seen.len(), 2, "copy 目标也必须远端先确认");
    assert_eq!(seen[1].body, body);
    assert_eq!(seen[1].origin.as_deref(), Some(dst_vid.as_str()));
    assert_eq!(seen[1].tagging.as_deref(), Some("k1=v1&k2=v2"));
    assert_eq!(seen[1].storage_class.as_deref(), Some("STANDARD_IA"));
    assert_eq!(seen[1].meta_foo.as_deref(), Some("bar"));
    // M9：Copy 目标 PUT 必须携带与源相同的校验和，且进 SigV4 SignedHeaders。
    assert_eq!(
        seen[1].checksum_sha256.as_deref(),
        Some(sum.as_str()),
        "Copy 远端 PUT 必须继承源校验和"
    );
    let auth = seen[1]
        .authorization
        .as_deref()
        .expect("copy PUT 必须带签名");
    assert!(
        auth.contains("x-amz-checksum-sha256"),
        "校验和必须进 SignedHeaders: {auth}"
    );

    // 本地目标字段与远端收到的字段一一对应（A/B 一致），且内容可读。
    let g = signed(&e, Method::GET, "/syn/dst.txt", &[], &[]).await;
    assert_eq!(g.status(), 200);
    assert_eq!(g.bytes().await.unwrap().as_ref(), body.as_slice());
    let h = signed(&e, Method::HEAD, "/syn/dst.txt", &[], &[]).await;
    assert_eq!(
        h.headers().get("x-amz-storage-class").unwrap(),
        "STANDARD_IA"
    );
    assert_eq!(h.headers().get("x-amz-meta-foo").unwrap(), "bar");
    assert_eq!(h.headers().get("x-amz-tagging-count").unwrap(), "2");
    let hs = signed(&e, Method::HEAD, "/syn/src.txt", &[], &[]).await;
    assert_eq!(
        hs.headers().get("x-amz-checksum-sha256").unwrap(),
        sum.as_str(),
        "源对象 HEAD 应保留校验和"
    );
    assert_eq!(
        h.headers().get("x-amz-checksum-sha256").unwrap(),
        hs.headers().get("x-amz-checksum-sha256").unwrap(),
        "A/B 对象校验和元数据必须一致"
    );
    let t = signed(&e, Method::GET, "/syn/dst.txt?tagging", &[], &[]).await;
    assert_eq!(t.status(), 200);
    let xml = t.text().await.unwrap();
    assert!(
        xml.contains("<Key>k1</Key>") && xml.contains("<Value>v1</Value>"),
        "{xml}"
    );
    assert_eq!(outbox(&e.st, "syn", "dst.txt").await, 0);
    assert_eq!(tee_entries(&e), 0);
}

/// M9：源对象无校验和时，Copy 目标不得凭空产生校验和（远端 PUT 与 A/B HEAD 均无）。
#[tokio::test]
async fn sync_copy_without_checksum_stays_absent() {
    let e = env(Mode::Ok, 1 << 20).await;
    let body = b"no checksum".to_vec();
    let r = signed(
        &e,
        Method::PUT,
        "/syn/plain-src.txt",
        &[("content-type", "text/plain")],
        &body,
    )
    .await;
    assert_eq!(r.status(), 200);
    let r = signed(
        &e,
        Method::PUT,
        "/syn/plain-dst.txt",
        &[("x-amz-copy-source", "/syn/plain-src.txt")],
        &[],
    )
    .await;
    assert_eq!(r.status(), 200);
    let seen = e.remote.puts();
    assert_eq!(seen.len(), 2, "copy 目标也必须远端先确认");
    assert_eq!(seen[1].body, body);
    assert_eq!(seen[1].checksum_sha256, None, "无源校验和不得伪造");
    for key in ["plain-src.txt", "plain-dst.txt"] {
        let h = signed(&e, Method::HEAD, &format!("/syn/{key}"), &[], &[]).await;
        assert_eq!(h.status(), 200);
        assert_eq!(h.headers().get("x-amz-checksum-sha256"), None, "{key}");
    }
}

/// M9 §2.4：sync 写不支持的组合必须显式 4xx/501，且不得触远端。
#[tokio::test]
async fn sync_write_boundaries_reject_unsupported_combinations() {
    let e = env(Mode::Ok, 1 << 20).await;
    // 直接置桶锁位（enable_bucket_lock 的 upsert 会撞站点时钟触发器，测试不测该路径）；
    // 桶锁配置 op 未映射到权限词表（非 admin fail-closed 403），临时提权以验证到 sync 边界。
    set_bucket(&e.st, "syn", "object_lock", "1").await;
    sqlx::query("UPDATE users SET is_admin = 1")
        .execute(e.st.db.pool())
        .await
        .unwrap();
    // SSE-C → 400
    let key = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
    let ssec = [
        ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ("x-amz-server-side-encryption-customer-key", key.as_str()),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/ssec.bin", &ssec, b"x")
            .await
            .status(),
        400
    );
    // 对象锁 → 409（桶锁已开，锁头能过 resolve_lock，才验证到 sync 边界）
    assert_eq!(
        signed(
            &e,
            Method::PUT,
            "/syn/lock.bin",
            &[("x-amz-object-lock-legal-hold", "ON")],
            b"x"
        )
        .await
        .status(),
        409
    );
    // MPU create / complete SSE-C → 400（与 PUT 同一 sync 边界）
    let mut mpu_ssec = vec![ssec[0], ssec[1]];
    mpu_ssec.push(("content-type", "text/plain"));
    assert_eq!(
        signed(
            &e,
            Method::POST,
            "/syn/ssec-mpu.bin?uploads",
            &mpu_ssec,
            &[]
        )
        .await
        .status(),
        400
    );
    // complete 的 SSE-C 是直调兜底：先建普通 MPU 并传一个 part。
    let r = signed(&e, Method::POST, "/syn/ssec-complete.bin?uploads", &[], &[]).await;
    assert_eq!(r.status(), 200);
    let xml = r.text().await.unwrap();
    let upload_id = xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .expect("create MPU 必须返回 UploadId")
        .to_string();
    let r = signed(
        &e,
        Method::PUT,
        &format!("/syn/ssec-complete.bin?partNumber=1&uploadId={upload_id}"),
        &[],
        b"x",
    )
    .await;
    assert_eq!(r.status(), 200);
    let etag = r
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
         <ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let complete_headers = [("content-type", "application/xml"), ssec[0], ssec[1]];
    assert_eq!(
        signed(
            &e,
            Method::POST,
            &format!("/syn/ssec-complete.bin?uploadId={upload_id}"),
            &complete_headers,
            complete.as_bytes()
        )
        .await
        .status(),
        400
    );
    // Copy dest SSE-C → 400 / dest 对象锁 → 409（源用 REPLICA 摄入，避免触远端）。
    let origin = uuid::Uuid::new_v4().to_string();
    let seed = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", LOCAL_TOKEN),
        ("x-oss-origin-version-id", origin.as_str()),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/copy-src.bin", &seed, b"src")
            .await
            .status(),
        200
    );
    let copy_ssec = [("x-amz-copy-source", "/syn/copy-src.bin"), ssec[0], ssec[1]];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/copy-ssec.bin", &copy_ssec, &[])
            .await
            .status(),
        400
    );
    let copy_lock = [
        ("x-amz-copy-source", "/syn/copy-src.bin"),
        ("x-amz-object-lock-legal-hold", "ON"),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/copy-lock.bin", &copy_lock, &[])
            .await
            .status(),
        409
    );
    // Tagging API → 501
    let tag = br#"<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>"#;
    assert_eq!(
        signed(
            &e,
            Method::PUT,
            "/syn/ssec.bin?tagging",
            &[("content-type", "application/xml")],
            tag
        )
        .await
        .status(),
        501
    );
    // 桶级默认锁 → 409
    let lock_cfg = br#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#;
    assert_eq!(
        signed(
            &e,
            Method::PUT,
            "/syn?object-lock",
            &[("content-type", "application/xml")],
            lock_cfg
        )
        .await
        .status(),
        409
    );
    // lifecycle Transition → 400（同步副本无法在远端做分层迁移）
    let lc = br#"<LifecycleConfiguration><Rule><ID>r</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>"#;
    assert_eq!(
        signed(
            &e,
            Method::PUT,
            "/syn?lifecycle",
            &[("content-type", "application/xml")],
            lc
        )
        .await
        .status(),
        400
    );
    assert_eq!(e.remote.calls(), 0, "被拒边界不得触远端");
}

#[tokio::test]
async fn remote_failure_and_unversioned_are_503_and_invisible() {
    let e = env(Mode::Fail, 1 << 20).await;
    for (mode, key) in [(Mode::Fail, "fail.txt"), (Mode::NoVersion, "nover.txt")] {
        *e.remote.mode.lock().unwrap() = mode;
        let path = format!("/syn/{key}");
        let r = put(&e, &path, b"body").await;
        assert_eq!(r.status(), 503, "{mode:?} 必须 503");
        let g = signed(&e, Method::GET, &path, &[], &[]).await;
        assert_eq!(g.status(), 404, "{mode:?} 失败后本地不得可见");
        assert_eq!(outbox(&e.st, "syn", key).await, 0, "{mode:?} 不得入 outbox");
        assert_eq!(tee_entries(&e), 0, "{mode:?} 后 tee 必须清理");
    }
}

/// M9 S2 M4：远端 409/400 是确定性拒绝，保留语义，不折叠成可重试的 503。
#[tokio::test]
async fn remote_4xx_preserves_conflict_and_bad_request() {
    let e = env(Mode::Conflict, 1 << 20).await;
    assert_eq!(put(&e, "/syn/conflict.txt", b"x").await.status(), 409);
    assert_eq!(
        signed(&e, Method::GET, "/syn/conflict.txt", &[], &[])
            .await
            .status(),
        404
    );
    *e.remote.mode.lock().unwrap() = Mode::BadRequest;
    assert_eq!(put(&e, "/syn/bad.txt", b"x").await.status(), 400);
    assert_eq!(
        signed(&e, Method::GET, "/syn/bad.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(tee_entries(&e), 0);
}

/// M9 §2.4：远端 id 与本地预生成 UUID 不符 = 双站同 id 契约破坏 → 502（不可重试）。
#[tokio::test]
async fn remote_version_id_mismatch_is_502_and_invisible() {
    let e = env(Mode::WrongId, 1 << 20).await;
    let r = put(&e, "/syn/wrong.txt", b"body").await;
    assert_eq!(r.status(), 502);
    assert_eq!(
        signed(&e, Method::GET, "/syn/wrong.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(tee_entries(&e), 0);
    assert_eq!(
        intent_rows(&e.st).await,
        1,
        "id 不符保留 intent 供 repair/恢复清理远端错 id 版本"
    );
}

#[tokio::test]
async fn remote_timeout_is_503_and_invisible() {
    let e = env(Mode::Slow, 1 << 20).await;
    let r = put(&e, "/syn/slow.bin", b"slow body").await;
    assert_eq!(r.status(), 503);
    assert_eq!(
        signed(&e, Method::GET, "/syn/slow.bin", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(outbox(&e.st, "syn", "slow.bin").await, 0);
    assert_eq!(tee_entries(&e), 0);
}

#[tokio::test]
async fn gates_require_single_peer_and_ready_bucket() {
    let e = env(Mode::Ok, 1 << 20).await;
    let pool = e.st.db.pool();
    // 0 peer
    sqlx::query("DELETE FROM sites")
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(put(&e, "/syn/gate1.txt", b"x").await.status(), 503);
    // 2 peer（第二个换 peer_id，避免主键冲突）
    let mut p2 = peer(&e.remote.base);
    p2.peer_id = "c".into();
    e.st.db.insert_site(&peer(&e.remote.base)).await.unwrap();
    e.st.db.insert_site(&p2).await.unwrap();
    assert_eq!(put(&e, "/syn/gate2.txt", b"x").await.status(), 503);
    // 回到 1 peer
    sqlx::query("DELETE FROM sites WHERE peer_id = 'c'")
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(put(&e, "/syn/gate3.txt", b"x").await.status(), 200);
    // 删桶中
    set_bucket(&e.st, "syn", "deleting", "1").await;
    e.st.store.invalidate_gate("syn");
    assert_eq!(put(&e, "/syn/gate4.txt", b"x").await.status(), 503);
    // 本地 versioning 未启用（sync 依赖双站同 id）→ 409（桶状态可纠正，非暂时不可用）
    set_bucket(&e.st, "syn", "deleting", "0").await;
    set_bucket(&e.st, "syn", "versioning", "Suspended").await;
    e.st.store.invalidate_gate("syn");
    assert_eq!(put(&e, "/syn/gate5.txt", b"x").await.status(), 409);
    // 被拒的写一次远端调用都没有；gate3 是唯一成功者。
    assert_eq!(e.remote.puts().len(), 1);
    assert_eq!(outbox(&e.st, "syn", "gate4.txt").await, 0);
}

#[tokio::test]
async fn async_bucket_put_unchanged() {
    let e = env(Mode::Ok, 1 << 20).await;
    let r = put(&e, "/asy/async.txt", b"async body").await;
    assert_eq!(r.status(), 200);
    assert_eq!(e.remote.puts().len(), 0, "async 桶不得走 sync 门");
    assert_eq!(
        outbox(&e.st, "asy", "async.txt").await,
        1,
        "async 桶照常入站点 outbox"
    );
    let g = signed(&e, Method::GET, "/asy/async.txt", &[], &[]).await;
    assert_eq!(g.status(), 200);
    assert_eq!(g.bytes().await.unwrap().as_ref(), b"async body");
}

#[tokio::test]
async fn sync_max_bytes_rejected_before_remote() {
    let e = env(Mode::Ok, 64).await;
    let r = put(&e, "/syn/max.bin", &[7u8; 100]).await;
    assert_eq!(r.status(), 400);
    assert_eq!(e.remote.puts().len(), 0);
    assert_eq!(
        signed(&e, Method::GET, "/syn/max.bin", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(tee_entries(&e), 0);
}

/// M9 S2：MPU Complete 与 PutObject 同一门——拼接后 tee 明文 → 整对象远端 PUT
/// （同 origin id）→ 本地 meta 翻转；失败则不可见且 tee 清理。
#[tokio::test]
async fn sync_mpu_complete_is_remote_first_and_roundtrips() {
    let e = env(Mode::Ok, 1 << 20).await;
    let r = signed(
        &e,
        Method::POST,
        "/syn/mpu.bin?uploads",
        &[
            ("content-type", "text/x-mpu"),
            ("x-amz-tagging", "mp=1"),
            ("x-amz-storage-class", "STANDARD_IA"),
        ],
        &[],
    )
    .await;
    assert_eq!(r.status(), 200);
    let xml = r.text().await.unwrap();
    let upload_id = xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .expect("create MPU 必须返回 UploadId")
        .to_string();
    let part = b"hello multipart".to_vec();
    let part_path = format!("/syn/mpu.bin?partNumber=1&uploadId={upload_id}");
    let r = signed(&e, Method::PUT, &part_path, &[], &part).await;
    assert_eq!(r.status(), 200);
    let etag = r
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
         <ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let r = signed(
        &e,
        Method::POST,
        &format!("/syn/mpu.bin?uploadId={upload_id}"),
        &[("content-type", "application/xml")],
        complete.as_bytes(),
    )
    .await;
    assert_eq!(r.status(), 200);
    let vid = r
        .headers()
        .get("x-amz-version-id")
        .expect("sync Complete 必须返回 version id")
        .to_str()
        .unwrap()
        .to_string();
    let puts = e.remote.puts();
    assert_eq!(puts.len(), 1, "Complete 只允许一次远端 PUT");
    assert_eq!(puts[0].body, part, "远端收到整对象明文");
    assert_eq!(puts[0].origin.as_deref(), Some(vid.as_str()));
    assert_eq!(puts[0].tagging.as_deref(), Some("mp=1"));
    assert_eq!(puts[0].storage_class.as_deref(), Some("STANDARD_IA"));
    assert_eq!(puts[0].content_type.as_deref(), Some("text/x-mpu"));
    let g = signed(&e, Method::GET, "/syn/mpu.bin", &[], &[]).await;
    assert_eq!(g.status(), 200);
    assert_eq!(g.bytes().await.unwrap().as_ref(), part.as_slice());
    assert_eq!(
        signed(&e, Method::HEAD, "/syn/mpu.bin", &[], &[])
            .await
            .headers()
            .get("x-amz-replication-status")
            .unwrap(),
        "COMPLETED"
    );
    assert_eq!(outbox(&e.st, "syn", "mpu.bin").await, 0);
    assert_eq!(tee_entries(&e), 0);

    // 失败：拼接+tee 之后远端 500 → 503，本地不可见，tee 清理。
    let r = signed(&e, Method::POST, "/syn/mpu2.bin?uploads", &[], &[]).await;
    let xml = r.text().await.unwrap();
    let uid2 = xml
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .unwrap()
        .to_string();
    let r = signed(
        &e,
        Method::PUT,
        &format!("/syn/mpu2.bin?partNumber=1&uploadId={uid2}"),
        &[],
        &part,
    )
    .await;
    let etag2 = r
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    *e.remote.mode.lock().unwrap() = Mode::Fail;
    let complete2 = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
         <ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"
    );
    let r = signed(
        &e,
        Method::POST,
        &format!("/syn/mpu2.bin?uploadId={uid2}"),
        &[("content-type", "application/xml")],
        complete2.as_bytes(),
    )
    .await;
    assert_eq!(r.status(), 503);
    assert_eq!(
        signed(&e, Method::GET, "/syn/mpu2.bin", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(outbox(&e.st, "syn", "mpu2.bin").await, 0);
    assert_eq!(tee_entries(&e), 0);
}

#[tokio::test]
async fn replica_intake_requires_token_and_adopts_origin_id() {
    let e = env(Mode::Ok, 1 << 20).await;
    let origin = uuid::Uuid::new_v4().to_string();
    let body = b"from-peer".to_vec();
    let sum = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&body));
    // 缺 token / 错 token → 403
    let missing = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/in1.txt", &missing, &body)
            .await
            .status(),
        403
    );
    let wrong = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", "cccccccccccccccccccccccccccccccc"),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/in2.txt", &wrong, &body)
            .await
            .status(),
        403
    );
    let h = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", LOCAL_TOKEN),
        ("x-oss-origin-version-id", origin.as_str()),
        ("x-amz-tagging", "k1=v1&k2=v2"),
        ("x-amz-storage-class", "STANDARD_IA"),
        ("x-amz-checksum-sha256", sum.as_str()),
    ];
    // 正确 token：采用 origin id，不调用远端门；tags/存储类/校验和随版本保留（A/B 一致）。
    let r = signed(&e, Method::PUT, "/syn/in3.txt", &h, &body).await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.headers()
            .get("x-amz-version-id")
            .unwrap()
            .to_str()
            .unwrap(),
        origin
    );
    assert_eq!(e.remote.puts().len(), 0, "REPLICA 摄入不得再 gate");
    assert_eq!(
        outbox(&e.st, "syn", "in3.txt").await,
        0,
        "REPLICA 摄入不再复制"
    );
    let head = signed(&e, Method::HEAD, "/syn/in3.txt", &[], &[]).await;
    assert_eq!(
        head.headers().get("x-amz-replication-status").unwrap(),
        "REPLICA"
    );
    assert_eq!(
        head.headers().get("x-amz-storage-class").unwrap(),
        "STANDARD_IA"
    );
    assert_eq!(head.headers().get("x-amz-tagging-count").unwrap(), "2");
    assert_eq!(
        head.headers().get("x-amz-checksum-sha256").unwrap(),
        sum.as_str()
    );
    let t = signed(&e, Method::GET, "/syn/in3.txt?tagging", &[], &[]).await;
    assert_eq!(t.status(), 200);
    let xml = t.text().await.unwrap();
    assert!(
        xml.contains("<Key>k1</Key>") && xml.contains("<Value>v1</Value>"),
        "{xml}"
    );
    // 同 id 同体重放幂等；同 id 异体冲突 → 409（M9 §2.4 强制 id 冲突）。
    assert_eq!(
        signed(&e, Method::PUT, "/syn/in3.txt", &h, &body)
            .await
            .status(),
        200
    );
    // 异体请求不带 checksum（否则先撞校验和 400，测不到强制 id 冲突）。
    let conflict = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", LOCAL_TOKEN),
        ("x-oss-origin-version-id", origin.as_str()),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/in3.txt", &conflict, b"different")
            .await
            .status(),
        409
    );
}

#[tokio::test]
async fn sync_write_requires_site_config() {
    let e = env_inner(Mode::Ok, 1 << 20, false).await;
    assert_eq!(put(&e, "/syn/nosite.txt", b"x").await.status(), 503);
    assert!(e.remote.puts().is_empty(), "无 [site] 不得触远端");
    assert_eq!(intent_rows(&e.st).await, 0, "前置拒绝不得留 intent");
}

#[tokio::test]
async fn replica_intake_requires_local_versioning() {
    let e = env(Mode::Ok, 1 << 20).await;
    set_bucket(&e.st, "syn", "versioning", "Suspended").await;
    e.st.store.invalidate_gate("syn");
    let origin = uuid::Uuid::new_v4().to_string();
    let h = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", LOCAL_TOKEN),
        ("x-oss-origin-version-id", origin.as_str()),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/noversion.txt", &h, b"body")
            .await
            .status(),
        409,
        "本端 versioning 未启用时不得摄入 REPLICA（可纠正的桶状态 → 409）"
    );
    assert_eq!(
        signed(&e, Method::GET, "/syn/noversion.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert!(e.remote.puts().is_empty());
}

/// M9 S2/M6：站点补偿删除路由只做本地删除（token 认证、JWT 豁免、永不转发）。
#[tokio::test]
async fn site_delete_route_is_local_only_and_token_authed() {
    let e = env(Mode::Ok, 1 << 20).await;
    // 用 REPLICA 摄入造一个已知 id 的本地版本（不触远端）。
    let origin = uuid::Uuid::new_v4().to_string();
    let h = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", LOCAL_TOKEN),
        ("x-oss-origin-version-id", origin.as_str()),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/site-del.txt", &h, b"bye")
            .await
            .status(),
        200
    );
    let url = format!("{}/api/v1/site/delete-object", e.base);
    let body = serde_json::json!({"bucket": "syn", "key": "site-del.txt", "version_id": origin});
    // 错 token → 403，对象仍在。
    let r = e
        .http
        .post(&url)
        .header("x-oss-site-token", "ffffffffffffffffffffffffffffffff")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    assert_eq!(
        signed(&e, Method::GET, "/syn/site-del.txt", &[], &[])
            .await
            .status(),
        200
    );
    // 正确 token → 204；重复调用幂等 204。
    for _ in 0..2 {
        let r = e
            .http
            .post(&url)
            .header("x-oss-site-token", LOCAL_TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 204);
    }
    assert_eq!(
        signed(&e, Method::GET, "/syn/site-del.txt", &[], &[])
            .await
            .status(),
        404
    );

    // marker_id 分支：本地落同 id marker（只本地；GET 404 / HEAD marker 405）。
    let origin2 = uuid::Uuid::new_v4().to_string();
    let h2 = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", LOCAL_TOKEN),
        ("x-oss-origin-version-id", origin2.as_str()),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/site-del2.txt", &h2, b"bye2")
            .await
            .status(),
        200
    );
    let marker = uuid::Uuid::new_v4().to_string();
    let body = serde_json::json!({
        "bucket": "syn", "key": "site-del2.txt", "marker_id": marker,
    });
    let r = e
        .http
        .post(&url)
        .header("x-oss-site-token", LOCAL_TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert_eq!(
        signed(&e, Method::GET, "/syn/site-del2.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(
        signed(
            &e,
            Method::HEAD,
            &format!("/syn/site-del2.txt?versionId={marker}"),
            &[],
            &[]
        )
        .await
        .status(),
        405
    );

    // all_versions 分支：本地整键硬删（含 marker 与旧版本）。
    let body = serde_json::json!({
        "bucket": "syn", "key": "site-del2.txt", "all_versions": true,
    });
    let r = e
        .http
        .post(&url)
        .header("x-oss-site-token", LOCAL_TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    assert_eq!(
        signed(&e, Method::GET, "/syn/site-del2.txt", &[], &[])
            .await
            .status(),
        404
    );
    // quiesce：桶不存在 → 204（删桶 intent 重试时对端已删，幂等继续）。
    let r = e
        .http
        .post(format!("{}/api/v1/site/bucket-quiesce", e.base))
        .header("x-oss-site-token", LOCAL_TOKEN)
        .json(&serde_json::json!({"bucket": "no-such-bucket"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);

    // m3：内部路由只接受 sync 桶——async 桶 409 且数据/物理桶不动。
    assert_eq!(put(&e, "/asy/m3.txt", b"keep").await.status(), 200);
    for (path, body) in [
        (
            "/api/v1/site/delete-object",
            serde_json::json!({"bucket": "asy", "key": "m3.txt", "all_versions": true}),
        ),
        (
            "/api/v1/site/bucket-quiesce",
            serde_json::json!({"bucket": "asy"}),
        ),
        (
            "/api/v1/site/delete-bucket",
            serde_json::json!({"bucket": "asy"}),
        ),
    ] {
        let r = e
            .http
            .post(format!("{}{path}", e.base))
            .header("x-oss-site-token", LOCAL_TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 409, "{path} 必须拒绝 async 桶");
    }
    assert_eq!(
        signed(&e, Method::GET, "/asy/m3.txt", &[], &[])
            .await
            .status(),
        200,
        "async 桶对象不得被站点路由删除"
    );
    assert!(e.st.store.bucket_exists("asy").await.unwrap());
    assert_eq!(e.remote.calls(), 0, "补偿路由不得转发远端（无回环）");
}

async fn intent_rows(st: &AppState) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM sync_write_intents")
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

async fn insert_intent(st: &AppState, id: &str, key: &str, tee: &std::path::Path) {
    sqlx::query(
        "INSERT INTO sync_write_intents \
         (id, bucket, key, local_version_id, remote_version_id, tee_path, commit_json, \
          state, created_at, updated_at) VALUES (?, 'syn', ?, ?, ?, ?, '{}', 'remote_done', 0, 0)",
    )
    .bind(id)
    .bind(key)
    .bind(id)
    .bind(id)
    .bind(tee.to_string_lossy().into_owned())
    .execute(st.db.pool())
    .await
    .unwrap();
}

/// I1：恢复三态——本地已提交只清行；远端孤儿删远端；不可达保留重试。
#[tokio::test]
async fn recovery_cleans_committed_and_orphan_intents() {
    let e = env(Mode::Ok, 1 << 20).await;
    std::fs::create_dir_all(&e.tee_dir).unwrap();

    // A) 本地 meta 已有该 id：只清 intent/tee，不删远端。
    let r = put(&e, "/syn/committed.txt", b"committed").await;
    assert_eq!(r.status(), 200);
    let vid = r
        .headers()
        .get("x-amz-version-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let tee = e.tee_dir.join(format!("{vid}.plain"));
    std::fs::write(&tee, b"x").unwrap();
    insert_intent(&e.st, &vid, "committed.txt", &tee).await;
    recover_write_intents(&e.st).await;
    assert_eq!(intent_rows(&e.st).await, 0);
    assert!(!tee.exists(), "恢复必须清 tee");
    assert!(e.remote.deletes().is_empty(), "本地已有版本，不得删远端");
    assert!(e.remote.site_deletes().is_empty());
    assert_eq!(
        signed(&e, Method::GET, "/syn/committed.txt", &[], &[])
            .await
            .status(),
        200
    );

    // B) 本地无该 id（远端可能已提交）：删远端同 id 后清行。
    let orphan = uuid::Uuid::new_v4().to_string();
    let tee2 = e.tee_dir.join(format!("{orphan}.plain"));
    std::fs::write(&tee2, b"y").unwrap();
    insert_intent(&e.st, &orphan, "orphan.txt", &tee2).await;
    recover_write_intents(&e.st).await;
    // M6/S2：补偿删除走站点内部路由（token 校验、只删本地、永不转发）；
    // 普通 S3 DELETE 会在双站间回环，必须零调用。
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), 1);
    let body = String::from_utf8_lossy(&dels[0].body);
    assert!(
        body.contains(&format!("\"version_id\":\"{orphan}\"")),
        "补偿删除必须点名同 id: {body}"
    );
    assert_eq!(dels[0].token.as_deref(), Some(PEER_TOKEN));
    assert!(
        e.remote.deletes().is_empty(),
        "补偿不得发普通 S3 DELETE（回环）"
    );
    assert_eq!(intent_rows(&e.st).await, 0);
    assert!(!tee2.exists());
    assert_eq!(
        e.metrics.write_intent_recovered_total(),
        2,
        "A/B 两轮各恢复 1 行"
    );

    // C) 远端不可达：保留；恢复可达后清掉。
    sqlx::query("UPDATE sites SET endpoint = 'http://127.0.0.1:1'")
        .execute(e.st.db.pool())
        .await
        .unwrap();
    let pending = uuid::Uuid::new_v4().to_string();
    let tee3 = e.tee_dir.join(format!("{pending}.plain"));
    std::fs::write(&tee3, b"z").unwrap();
    insert_intent(&e.st, &pending, "retry.txt", &tee3).await;
    recover_write_intents(&e.st).await;
    assert_eq!(intent_rows(&e.st).await, 1, "远端不可达必须保留 intent");
    assert!(tee3.exists(), "保留时不得删 tee");
    sqlx::query("UPDATE sites SET endpoint = ?")
        .bind(&e.remote.base)
        .execute(e.st.db.pool())
        .await
        .unwrap();
    recover_write_intents(&e.st).await;
    assert_eq!(intent_rows(&e.st).await, 0);
    assert!(!tee3.exists());

    // D) 在途写（updated_at=now）不得被恢复 worker 删除；否则会删掉远端已确认版本。
    let inflight = uuid::Uuid::new_v4().to_string();
    let tee4 = e.tee_dir.join(format!("{inflight}.plain"));
    std::fs::write(&tee4, b"w").unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    sqlx::query(
        "INSERT INTO sync_write_intents \
         (id, bucket, key, local_version_id, remote_version_id, tee_path, commit_json, \
          state, created_at, updated_at) VALUES (?, 'syn', 'inflight.txt', ?, ?, ?, '{}', \
          'remote_pending', 0, ?)",
    )
    .bind(&inflight)
    .bind(&inflight)
    .bind(&inflight)
    .bind(tee4.to_string_lossy().into_owned())
    .bind(now)
    .execute(e.st.db.pool())
    .await
    .unwrap();
    let deletes_before = e.remote.site_deletes().len();
    recover_write_intents(&e.st).await;
    assert_eq!(intent_rows(&e.st).await, 1, "在途 intent 必须保留");
    assert!(tee4.exists(), "在途 intent 的 tee 必须保留");
    assert_eq!(
        e.remote.site_deletes().len(),
        deletes_before,
        "在途写不得触发远端删除"
    );
}

async fn delete_intent_rows(st: &AppState) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM sync_delete_intents")
        .fetch_one(st.db.pool())
        .await
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn insert_delete_intent(
    st: &AppState,
    op: &str,
    key: &str,
    local_vid: &str,
    remote_vid: &str,
    marker_vid: &str,
) {
    sqlx::query(
        "INSERT INTO sync_delete_intents \
         (id, bucket, key, op, local_version_id, remote_version_id, marker_version_id, \
          state, created_at, updated_at) VALUES (?, 'syn', ?, ?, ?, ?, ?, 'remote_pending', 0, 0)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(key)
    .bind(op)
    .bind(local_vid)
    .bind(remote_vid)
    .bind(marker_vid)
    .execute(st.db.pool())
    .await
    .unwrap();
}

fn hdr(r: &reqwest::Response, name: &str) -> String {
    r.headers()
        .get(name)
        .unwrap_or_else(|| panic!("缺响应头 {name}"))
        .to_str()
        .unwrap()
        .to_string()
}

fn body_json(s: &Seen) -> serde_json::Value {
    serde_json::from_slice(&s.body).unwrap_or_default()
}

/// M9 S3：指名版本删除 remote-first——远端 token 删除成功后才本地删；无 outbox。
#[tokio::test]
async fn sync_version_delete_is_remote_first() {
    let e = env(Mode::Ok, 1 << 20).await;
    let r = put(&e, "/syn/v1.txt", b"v1").await;
    assert_eq!(r.status(), 200);
    let vid = hdr(&r, "x-amz-version-id");
    assert_eq!(outbox(&e.st, "syn", "v1.txt").await, 0);

    let d = signed(
        &e,
        Method::DELETE,
        &format!("/syn/v1.txt?versionId={vid}"),
        &[],
        &[],
    )
    .await;
    assert_eq!(d.status(), 204);
    assert_eq!(hdr(&d, "x-amz-version-id"), vid);
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), 1, "版本删必须走一次远端 token 路由");
    assert_eq!(
        body_json(&dels[0])
            .get("version_id")
            .and_then(|v| v.as_str()),
        Some(vid.as_str())
    );
    assert_eq!(dels[0].token.as_deref(), Some(PEER_TOKEN));
    assert_eq!(
        signed(&e, Method::GET, "/syn/v1.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(
        signed(
            &e,
            Method::GET,
            &format!("/syn/v1.txt?versionId={vid}"),
            &[],
            &[]
        )
        .await
        .status(),
        404
    );
    assert_eq!(
        outbox(&e.st, "syn", "v1.txt").await,
        0,
        "sync 删不得进 outbox"
    );
    assert_eq!(delete_intent_rows(&e.st).await, 0, "成功后 intent 必须清理");
}

/// M9 S3：无 versionId 删除 = 远端先建同 id marker，本地再落同一 id；
/// 对端与本地 DeleteMarkers 的 VersionId 相同（不比较整表）。
#[tokio::test]
async fn sync_marker_delete_uses_same_id_on_both_sites() {
    let e = env(Mode::Ok, 1 << 20).await;
    assert_eq!(put(&e, "/syn/m1.txt", b"m1").await.status(), 200);

    let d = signed(&e, Method::DELETE, "/syn/m1.txt", &[], &[]).await;
    assert_eq!(d.status(), 204);
    assert_eq!(hdr(&d, "x-amz-delete-marker"), "true");
    let mid = hdr(&d, "x-amz-version-id");
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), 1);
    assert_eq!(
        body_json(&dels[0])
            .get("marker_id")
            .and_then(|v| v.as_str()),
        Some(mid.as_str()),
        "双站 marker 必须同 id"
    );
    assert_eq!(
        signed(&e, Method::GET, "/syn/m1.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(
        signed(
            &e,
            Method::HEAD,
            &format!("/syn/m1.txt?versionId={mid}"),
            &[],
            &[]
        )
        .await
        .status(),
        405,
        "marker 版本 HEAD = 405"
    );
    assert_eq!(outbox(&e.st, "syn", "m1.txt").await, 0);
    assert_eq!(delete_intent_rows(&e.st).await, 0);
}

/// M9 S3：远端失败 → 503 且本地对象/版本原样（未删）；intent 保留供恢复。
#[tokio::test]
async fn sync_delete_remote_failure_is_503_and_local_unchanged() {
    let e = env(Mode::Ok, 1 << 20).await;
    let r = put(&e, "/syn/f1.txt", b"keep").await;
    assert_eq!(r.status(), 200);
    let vid = hdr(&r, "x-amz-version-id");

    e.remote.set_mode(Mode::Fail);
    let d = signed(
        &e,
        Method::DELETE,
        &format!("/syn/f1.txt?versionId={vid}"),
        &[],
        &[],
    )
    .await;
    assert_eq!(d.status(), 503, "远端失败必须 503（不降级）");
    assert_eq!(
        signed(
            &e,
            Method::GET,
            &format!("/syn/f1.txt?versionId={vid}"),
            &[],
            &[]
        )
        .await
        .status(),
        200,
        "远端未确认，本地版本必须仍在"
    );
    assert_eq!(delete_intent_rows(&e.st).await, 1, "失败必须保留 intent");

    let d = signed(&e, Method::DELETE, "/syn/f1.txt", &[], &[]).await;
    assert_eq!(d.status(), 503);
    assert_eq!(
        signed(&e, Method::GET, "/syn/f1.txt", &[], &[])
            .await
            .status(),
        200,
        "marker 远端失败，本地不得落 marker"
    );
    assert_eq!(delete_intent_rows(&e.st).await, 2);

    // 恢复远端后重试成功。
    e.remote.set_mode(Mode::Ok);
    let d = signed(
        &e,
        Method::DELETE,
        &format!("/syn/f1.txt?versionId={vid}"),
        &[],
        &[],
    )
    .await;
    assert_eq!(d.status(), 204);
    assert_eq!(
        signed(&e, Method::GET, "/syn/f1.txt", &[], &[])
            .await
            .status(),
        404
    );
}

/// M9 S3 N2：DeleteObjects 逐键 remote-first；任一远端失败 → 整体 503，
/// 已成功的键已提交（marker 可见性幂等）。
#[tokio::test]
async fn delete_objects_sync_is_remote_first_and_partial_failure_is_503() {
    let e = env(Mode::Ok, 1 << 20).await;
    for k in ["d1.txt", "d2.txt"] {
        assert_eq!(put(&e, &format!("/syn/{k}"), b"x").await.status(), 200);
    }
    let xml =
        b"<Delete><Object><Key>d1.txt</Key></Object><Object><Key>d2.txt</Key></Object></Delete>";
    let r = signed(
        &e,
        Method::POST,
        "/syn?delete",
        &[("content-type", "application/xml")],
        xml,
    )
    .await;
    assert_eq!(r.status(), 200);
    let text = r.text().await.unwrap();
    assert!(text.contains("d1.txt") && text.contains("d2.txt"), "{text}");
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), 2, "每个键一次远端 marker");
    let ids: Vec<String> = dels
        .iter()
        .filter_map(|s| body_json(s).get("marker_id")?.as_str().map(str::to_string))
        .collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1], "不同键的 marker id 不得复用");
    for k in ["d1.txt", "d2.txt"] {
        assert_eq!(
            signed(&e, Method::GET, &format!("/syn/{k}"), &[], &[])
                .await
                .status(),
            404
        );
        assert_eq!(outbox(&e.st, "syn", k).await, 0);
    }
    assert_eq!(delete_intent_rows(&e.st).await, 0);

    // 部分失败：d3 成功、d4 远端 500 → 整体 503；d3 已提交、d4 原样。
    for k in ["d3.txt", "d4.txt"] {
        assert_eq!(put(&e, &format!("/syn/{k}"), b"x").await.status(), 200);
    }
    e.remote.fail_delete_key("d4.txt");
    let xml =
        b"<Delete><Object><Key>d3.txt</Key></Object><Object><Key>d4.txt</Key></Object></Delete>";
    let r = signed(
        &e,
        Method::POST,
        "/syn?delete",
        &[("content-type", "application/xml")],
        xml,
    )
    .await;
    assert_eq!(r.status(), 503, "任一远端失败 → 整体 503（成功键已提交）");
    assert_eq!(
        signed(&e, Method::GET, "/syn/d3.txt", &[], &[])
            .await
            .status(),
        404,
        "先成功的键必须已提交"
    );
    assert_eq!(
        signed(&e, Method::GET, "/syn/d4.txt", &[], &[])
            .await
            .status(),
        200,
        "失败键不得本地删除"
    );
    assert_eq!(e.st.sync_delete.partial_total(), 1);
}

/// M9 S3：生命周期 Expiration 在 sync 桶同样 remote-first（同 id marker），无 outbox。
#[tokio::test]
async fn lifecycle_expiration_on_sync_bucket_is_remote_first() {
    let e = env(Mode::Ok, 1 << 20).await;
    assert_eq!(put(&e, "/syn/exp.txt", b"expire me").await.status(), 200);
    sqlx::query(
        "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at) \
         VALUES ('syn', '', 0, 1, 0)",
    )
    .execute(e.st.db.pool())
    .await
    .unwrap();
    assert_eq!(sweep_once(&e.st).await, 1);
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), 1, "生命周期删除必须先到远端");
    assert!(body_json(&dels[0]).get("marker_id").is_some());
    assert_eq!(
        signed(&e, Method::GET, "/syn/exp.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(outbox(&e.st, "syn", "exp.txt").await, 0);
    assert_eq!(delete_intent_rows(&e.st).await, 0);
}

/// M9 S3 B3：在途 PUT 期间删桶——quiesce 等写者归零后才删；新写 503 且不触远端。
#[tokio::test]
async fn delete_bucket_waits_for_inflight_writer_and_quiesces_remote() {
    let e = env(Mode::Ok, 1 << 20).await;
    e.remote.set_put_delay_ms(600);
    e.remote.fail_put(); // 在途写最终失败：不提交对象，桶仍为空可删。

    let put_fut = async {
        signed(
            &e,
            Method::PUT,
            "/syn/inflight.bin",
            &[("content-type", "text/plain")],
            b"data",
        )
        .await
    };
    let del_fut = async {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let t = std::time::Instant::now();
        let r = signed(&e, Method::DELETE, "/syn", &[], &[]).await;
        (r, t.elapsed())
    };
    let late_fut = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        signed(
            &e,
            Method::PUT,
            "/syn/late.bin",
            &[("content-type", "text/plain")],
            b"late",
        )
        .await
    };
    let (put_r, (del_r, waited), late_r) = tokio::join!(put_fut, del_fut, late_fut);
    assert_eq!(put_r.status(), 503, "在途写远端失败 → 503");
    assert_eq!(del_r.status(), 204);
    assert!(
        waited >= Duration::from_millis(300),
        "删桶必须在途写者归零后才继续（等了 {waited:?}）"
    );
    assert_eq!(late_r.status(), 503, "quiesce 期间新写必须拒绝");
    assert_eq!(e.remote.puts().len(), 1, "被拒的新写不得触远端");
    assert_eq!(e.remote.quiesces().len(), 1);
    assert!(
        e.remote.quiesce_releases().is_empty(),
        "成功路径不得 release"
    );
    assert_eq!(e.remote.site_posts("/api/v1/site/delete-bucket").len(), 1);
    let buckets = e.st.store.list_buckets().await.unwrap();
    assert!(!buckets.iter().any(|b| b.name == "syn"), "本地桶必须已删");
}

/// M9 S3：远端非空 → 409，回滚两端 deleting；桶仍可用（新写可进）。
#[tokio::test]
async fn delete_bucket_rolls_back_when_remote_nonempty() {
    let e = env(Mode::Ok, 1 << 20).await;
    e.remote.set_bucket_nonempty(true);
    let r = signed(&e, Method::DELETE, "/syn", &[], &[]).await;
    assert_eq!(r.status(), 409);
    assert_eq!(e.remote.quiesces().len(), 1);
    assert_eq!(
        e.remote.quiesce_releases().len(),
        1,
        "非空必须释放对端 quiesce"
    );
    assert!(e.remote.site_posts("/api/v1/site/delete-bucket").is_empty());
    let (_, deleting, _) = e.st.db.bucket_sync_state("syn").await.unwrap();
    assert!(!deleting, "失败必须回滚 deleting");
    assert_eq!(
        put(&e, "/syn/after.txt", b"ok").await.status(),
        200,
        "回滚后桶必须恢复可写"
    );
}

/// M9 S3：delete intent 恢复按远端 HEAD 三态收敛（create_marker 200/405/404，
/// delete_version 200/404），本地补提交且不重生 marker。
#[tokio::test]
async fn recovery_delete_intents_head_tristate() {
    let e = env(Mode::Ok, 1 << 20).await;
    let m_found = uuid::Uuid::new_v4().to_string();
    let m_marker = uuid::Uuid::new_v4().to_string();
    let m_missing = uuid::Uuid::new_v4().to_string();
    e.remote.set_head(&m_found, 200);
    e.remote.set_head(&m_marker, 405);
    insert_delete_intent(&e.st, "create_marker", "rm1.txt", "", &m_found, &m_found).await;
    insert_delete_intent(&e.st, "create_marker", "rm2.txt", "", &m_marker, &m_marker).await;
    insert_delete_intent(
        &e.st,
        "create_marker",
        "rm3.txt",
        "",
        &m_missing,
        &m_missing,
    )
    .await;
    let before = e.remote.site_deletes().len();
    recover_delete_intents(&e.st).await;
    let dels = e.remote.site_deletes();
    assert_eq!(
        dels.len(),
        before + 1,
        "只有 HEAD 404 的 marker 需要重发远端"
    );
    assert_eq!(
        body_json(&dels[before])
            .get("marker_id")
            .and_then(|v| v.as_str()),
        Some(m_missing.as_str())
    );
    for (key, mid) in [
        ("rm1.txt", &m_found),
        ("rm2.txt", &m_marker),
        ("rm3.txt", &m_missing),
    ] {
        assert!(
            matches!(
                e.st.store.head_object_version("syn", key, mid).await,
                Err(xl_store::error::Error::DeleteMarker(_))
            ),
            "{key} 本地必须补出同 id marker"
        );
    }
    assert_eq!(delete_intent_rows(&e.st).await, 0);

    // delete_version：HEAD 200 → 先重试远端删除；HEAD 404 → 只做本地删。
    let r = put(&e, "/syn/rd1.txt", b"d1").await;
    let v_found = hdr(&r, "x-amz-version-id");
    e.remote.set_head(&v_found, 200);
    insert_delete_intent(&e.st, "delete_version", "rd1.txt", &v_found, &v_found, "").await;
    let before = e.remote.site_deletes().len();
    recover_delete_intents(&e.st).await;
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), before + 1, "HEAD 200 → 重试远端删除");
    assert_eq!(
        body_json(&dels[before])
            .get("version_id")
            .and_then(|v| v.as_str()),
        Some(v_found.as_str())
    );
    assert_eq!(
        signed(
            &e,
            Method::GET,
            &format!("/syn/rd1.txt?versionId={v_found}"),
            &[],
            &[]
        )
        .await
        .status(),
        404
    );

    let r = put(&e, "/syn/rd2.txt", b"d2").await;
    let v_missing = hdr(&r, "x-amz-version-id");
    insert_delete_intent(
        &e.st,
        "delete_version",
        "rd2.txt",
        &v_missing,
        &v_missing,
        "",
    )
    .await;
    let before = e.remote.site_deletes().len();
    recover_delete_intents(&e.st).await;
    assert_eq!(
        e.remote.site_deletes().len(),
        before,
        "HEAD 404 → 远端已无，不得再发删除"
    );
    assert_eq!(
        signed(
            &e,
            Method::GET,
            &format!("/syn/rd2.txt?versionId={v_missing}"),
            &[],
            &[]
        )
        .await
        .status(),
        404
    );
    assert_eq!(delete_intent_rows(&e.st).await, 0);
    assert!(e.st.sync_delete.recovered_total() >= 5);

    // m7：缺必填版本 id 的行 warn + 清理，不得永久残留。
    insert_delete_intent(&e.st, "delete_version", "bad1.txt", "", "", "").await;
    insert_delete_intent(&e.st, "create_marker", "bad2.txt", "", "", "").await;
    recover_delete_intents(&e.st).await;
    assert_eq!(delete_intent_rows(&e.st).await, 0);
}

/// M9 S3：delete_bucket intent 周期恢复——远端 quiesce/查空/删除后本地删除，
/// intent 清理、deleting 回滚（远端桶已删时 quiesce 204 幂等，见站点路由用例）。
/// C1 后仅“仍 deleting 的 sync 桶”会重跑 flow：模拟 crash 于 flow 中（DB flag 已置）。
#[tokio::test]
async fn recovery_delete_bucket_intent_converges() {
    let e = env(Mode::Ok, 1 << 20).await;
    set_bucket(&e.st, "syn", "deleting", "1").await;
    e.st.store.mark_deleting("syn");
    insert_delete_intent(&e.st, "delete_bucket", "", "", "", "").await;
    recover_delete_intents(&e.st).await;
    assert!(
        !e.st.store.bucket_exists("syn").await.unwrap(),
        "本地桶必须删除"
    );
    assert_eq!(delete_intent_rows(&e.st).await, 0);
    assert_eq!(e.remote.quiesces().len(), 1, "远端必须先 quiesce");
    assert!(e.st.sync_delete.recovered_total() >= 1);
}

/// C1：成功删桶后残留的旧 delete_bucket intent 不得删除同名重建的新桶。
#[tokio::test]
async fn recovery_stale_delete_bucket_intent_spares_recreated_bucket() {
    let e = env(Mode::Ok, 1 << 20).await;
    assert_eq!(
        signed(&e, Method::DELETE, "/syn", &[], &[]).await.status(),
        204
    );
    // 模拟 intent_clean 未落盘（崩溃/SQLite busy）：旧行 updated_at=0 必被扫描。
    insert_delete_intent(&e.st, "delete_bucket", "", "", "", "").await;
    let uid: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'alice'")
        .fetch_one(e.st.db.pool())
        .await
        .unwrap();
    make_bucket(&e.st, "syn", uid, "sync").await;
    // 删桶已清授权行：同名重建需重新授权（否则 S3 面 403）。
    e.st.db
        .set_permissions(
            uid,
            &[PermissionRow {
                bucket: "syn".into(),
                read: true,
                write: true,
                delete: true,
            }],
        )
        .await
        .unwrap();
    assert_eq!(put(&e, "/syn/reborn.txt", b"new").await.status(), 200);

    let quiesces = e.remote.quiesces().len();
    let bucket_deletes = e.remote.site_posts("/api/v1/site/delete-bucket").len();
    recover_delete_intents(&e.st).await;

    assert_eq!(delete_intent_rows(&e.st).await, 0, "陈旧 intent 只清行");
    assert!(
        e.st.store.bucket_exists("syn").await.unwrap(),
        "同名新桶必须存活（数据目录不得被删）"
    );
    assert_eq!(e.remote.quiesces().len(), quiesces, "恢复不得再 quiesce");
    assert_eq!(
        e.remote.site_posts("/api/v1/site/delete-bucket").len(),
        bucket_deletes,
        "恢复不得再删远端桶"
    );
    assert_eq!(
        signed(&e, Method::GET, "/syn/reborn.txt", &[], &[])
            .await
            .status(),
        200,
        "新桶数据必须仍在"
    );
    assert_eq!(put(&e, "/syn/after.txt", b"ok").await.status(), 200);
}

/// I1：deleting=1 后所有删除入口在任何远端调用前拒绝；
/// 第二个 DeleteBucket 409。本地对象/版本原样。
#[tokio::test]
async fn deleting_bucket_rejects_deletes_before_remote() {
    let e = env(Mode::Ok, 1 << 20).await;
    let r = put(&e, "/syn/i1.txt", b"keep").await;
    let vid = hdr(&r, "x-amz-version-id");
    set_bucket(&e.st, "syn", "deleting", "1").await;
    e.st.store.mark_deleting("syn");
    let calls = e.remote.calls();

    let d = signed(
        &e,
        Method::DELETE,
        &format!("/syn/i1.txt?versionId={vid}"),
        &[],
        &[],
    )
    .await;
    assert_eq!(d.status(), 503, "deleting 中版本删必须 503");
    let d = signed(&e, Method::DELETE, "/syn/i1.txt", &[], &[]).await;
    assert_eq!(d.status(), 503, "deleting 中 marker 删必须 503");
    let xml = b"<Delete><Object><Key>i1.txt</Key></Object></Delete>";
    let r = signed(
        &e,
        Method::POST,
        "/syn?delete",
        &[("content-type", "application/xml")],
        xml,
    )
    .await;
    assert_eq!(r.status(), 503, "deleting 中 DeleteObjects 必须 503");
    let r = signed(&e, Method::DELETE, "/syn", &[], &[]).await;
    assert_eq!(r.status(), 409, "已删除中的桶再删必须 409");

    assert_eq!(e.remote.calls(), calls, "deleting 置位后不得触远端");
    assert_eq!(
        signed(
            &e,
            Method::GET,
            &format!("/syn/i1.txt?versionId={vid}"),
            &[],
            &[]
        )
        .await
        .status(),
        200,
        "被拒的删除不得影响本地版本"
    );
    assert_eq!(delete_intent_rows(&e.st).await, 0);
}

/// I2/N2：远端删超时 → HEAD 探测一次；404 = 已删继续本地，200 = 重试一次，
/// 重试仍超时 → 503 + ambiguous_total + intent 保留。
#[tokio::test]
async fn remote_delete_timeout_probes_head_before_ambiguous() {
    let e = env(Mode::Ok, 1 << 20).await;
    let r = put(&e, "/syn/i2a.txt", b"a").await;
    let vid = hdr(&r, "x-amz-version-id");
    e.remote.set_delete_delay_ms(SLOW_MS);
    // 默认 HEAD 404：远端已删（响应丢失）→ 本地提交成功，不记 ambiguous。
    let d = signed(
        &e,
        Method::DELETE,
        &format!("/syn/i2a.txt?versionId={vid}"),
        &[],
        &[],
    )
    .await;
    assert_eq!(d.status(), 204, "HEAD 404 = 已删，必须继续本地提交");
    assert_eq!(
        signed(
            &e,
            Method::GET,
            &format!("/syn/i2a.txt?versionId={vid}"),
            &[],
            &[]
        )
        .await
        .status(),
        404
    );
    assert_eq!(delete_intent_rows(&e.st).await, 0);
    assert_eq!(e.st.sync_delete.ambiguous_total(), 0);

    // HEAD 200：版本仍在 → 重试一次；重试仍超时 → ambiguous + 503，本地不改。
    let r = put(&e, "/syn/i2b.txt", b"b").await;
    let vid2 = hdr(&r, "x-amz-version-id");
    e.remote.set_head(&vid2, 200);
    let before = e.remote.site_deletes().len();
    let d = signed(
        &e,
        Method::DELETE,
        &format!("/syn/i2b.txt?versionId={vid2}"),
        &[],
        &[],
    )
    .await;
    assert_eq!(d.status(), 503);
    let dels = e.remote.site_deletes();
    let attempts = dels[before..]
        .iter()
        .filter(|s| body_json(s).get("version_id").and_then(|v| v.as_str()) == Some(vid2.as_str()))
        .count();
    assert_eq!(attempts, 2, "HEAD 200 → 必须重试删除一次");
    assert_eq!(e.st.sync_delete.ambiguous_total(), 1);
    assert_eq!(
        delete_intent_rows(&e.st).await,
        1,
        "ambiguous 必须保留 intent"
    );
    assert_eq!(
        signed(
            &e,
            Method::GET,
            &format!("/syn/i2b.txt?versionId={vid2}"),
            &[],
            &[]
        )
        .await
        .status(),
        200,
        "未确认远端删除时本地版本必须仍在"
    );
}

/// I4：本地桶已删 + 残留 delete_bucket intent（deleting=1）→ 恢复收敛并清行，不空转。
#[tokio::test]
async fn recovery_local_deleted_bucket_intent_converges() {
    let e = env(Mode::Ok, 1 << 20).await;
    e.st.store.delete_bucket("syn").await.unwrap();
    set_bucket(&e.st, "syn", "deleting", "1").await;
    e.st.store.mark_deleting("syn");
    insert_delete_intent(&e.st, "delete_bucket", "", "", "", "").await;
    recover_delete_intents(&e.st).await;
    assert_eq!(delete_intent_rows(&e.st).await, 0, "本地已删必须收敛清行");
    let (_, deleting, _) = e.st.db.bucket_sync_state("syn").await.unwrap();
    assert!(!deleting, "成功后 deleting 必须清零");
    assert!(!e.st.store.bucket_exists("syn").await.unwrap());
}

/// m5：两个 DeleteBucket 并发——第二个在 flow 在途时 409，且远端只 quiesce 一次。
#[tokio::test]
async fn concurrent_delete_bucket_second_is_409() {
    let e = env(Mode::Ok, 1 << 20).await;
    e.remote.set_quiesce_delay_ms(800);
    let first = async { signed(&e, Method::DELETE, "/syn", &[], &[]).await };
    let second = async {
        tokio::time::sleep(Duration::from_millis(150)).await;
        signed(&e, Method::DELETE, "/syn", &[], &[]).await
    };
    let (a, b) = tokio::join!(first, second);
    assert_eq!(a.status(), 204);
    assert_eq!(b.status(), 409, "在途删除中的桶必须 409");
    assert_eq!(e.remote.quiesces().len(), 1, "并发删桶不得二次 quiesce");
    assert_eq!(e.remote.site_posts("/api/v1/site/delete-bucket").len(), 1);
    assert!(!e.st.store.bucket_exists("syn").await.unwrap());
}

/// m5：远端 quiesce 失败 → 503、释放对端、deleting 回滚、桶恢复可写。
#[tokio::test]
async fn delete_bucket_rolls_back_when_remote_quiesce_fails() {
    let e = env(Mode::Ok, 1 << 20).await;
    e.remote.fail_quiesce();
    let r = signed(&e, Method::DELETE, "/syn", &[], &[]).await;
    assert_eq!(r.status(), 503);
    assert_eq!(e.remote.quiesce_releases().len(), 1, "失败必须释放对端");
    assert!(e.remote.site_posts("/api/v1/site/delete-bucket").is_empty());
    let (_, deleting, _) = e.st.db.bucket_sync_state("syn").await.unwrap();
    assert!(!deleting, "失败必须回滚 deleting");
    assert!(e.st.store.bucket_exists("syn").await.unwrap());
    assert_eq!(put(&e, "/syn/after.txt", b"ok").await.status(), 200);
}

/// m5：console 对象删（整键）同样走共享 sink：远端 all_versions 先删，本地后删。
#[tokio::test]
async fn console_object_delete_on_sync_bucket_is_remote_first() {
    let e = env(Mode::Ok, 1 << 20).await;
    assert_eq!(put(&e, "/syn/c1.txt", b"x").await.status(), 200);
    let login: serde_json::Value = e
        .http
        .post(format!("{}/api/v1/login", e.base))
        .json(&serde_json::json!({"username": "alice", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = login["token"].as_str().unwrap();
    let r = e
        .http
        .delete(format!("{}/api/v1/buckets/syn/objects/c1.txt", e.base))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), 1, "console 删必须远端优先");
    assert_eq!(
        body_json(&dels[0])
            .get("all_versions")
            .and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        signed(&e, Method::GET, "/syn/c1.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(outbox(&e.st, "syn", "c1.txt").await, 0);
}

/// M9 S3：从接收端（B 持有 REPLICA 副本）发起删除同样 remote-first，
/// 且两侧使用同一 origin 版本 id / marker id。
#[tokio::test]
async fn replica_site_delete_is_remote_first_with_same_ids() {
    let e = env(Mode::Ok, 1 << 20).await;
    let origin = uuid::Uuid::new_v4().to_string();
    let h = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", LOCAL_TOKEN),
        ("x-oss-origin-version-id", origin.as_str()),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/fromb.txt", &h, b"from-b")
            .await
            .status(),
        200
    );
    assert_eq!(e.remote.puts().len(), 0, "摄入端不得再 gate");

    let d = signed(
        &e,
        Method::DELETE,
        &format!("/syn/fromb.txt?versionId={origin}"),
        &[],
        &[],
    )
    .await;
    assert_eq!(d.status(), 204);
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), 1);
    assert_eq!(
        body_json(&dels[0])
            .get("version_id")
            .and_then(|v| v.as_str()),
        Some(origin.as_str()),
        "B 发起删除必须用同一 origin id"
    );
    assert_eq!(
        signed(&e, Method::GET, "/syn/fromb.txt", &[], &[])
            .await
            .status(),
        404
    );

    // B 上再摄入一版，无 versionId 删除：marker 同 id。
    let origin2 = uuid::Uuid::new_v4().to_string();
    let h2 = [
        ("content-type", "text/plain"),
        ("x-amz-replication-status", "REPLICA"),
        ("x-oss-site-token", LOCAL_TOKEN),
        ("x-oss-origin-version-id", origin2.as_str()),
    ];
    assert_eq!(
        signed(&e, Method::PUT, "/syn/fromb2.txt", &h2, b"from-b2")
            .await
            .status(),
        200
    );
    let d = signed(&e, Method::DELETE, "/syn/fromb2.txt", &[], &[]).await;
    assert_eq!(d.status(), 204);
    let mid = hdr(&d, "x-amz-version-id");
    let dels = e.remote.site_deletes();
    assert_eq!(dels.len(), 2);
    assert_eq!(
        body_json(&dels[1])
            .get("marker_id")
            .and_then(|v| v.as_str()),
        Some(mid.as_str())
    );
    assert_eq!(outbox(&e.st, "syn", "fromb2.txt").await, 0);
}

/// M7 回归：async 桶删除完全本地，不触远端、不走 quiesce。
#[tokio::test]
async fn async_bucket_delete_stays_local() {
    let e = env(Mode::Ok, 1 << 20).await;
    assert_eq!(put(&e, "/asy/a.txt", b"x").await.status(), 200);
    let calls = e.remote.calls();
    let d = signed(&e, Method::DELETE, "/asy/a.txt", &[], &[]).await;
    assert_eq!(d.status(), 204);
    assert_eq!(e.remote.calls(), calls, "async 删不得触远端");
    assert!(
        e.remote
            .site_posts("/api/v1/site/bucket-quiesce")
            .is_empty()
    );
    assert_eq!(
        signed(&e, Method::GET, "/asy/a.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert_eq!(delete_intent_rows(&e.st).await, 0);
}
