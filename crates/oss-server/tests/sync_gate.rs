//! M9 S1：sync 桶 remote-first PutObject 进程内验收。
//!
//! 覆盖：成功（远端收到同体/同 ETag/同 origin id，远端回同 id → 本地可见、
//! `COMPLETED`、无站点 outbox）；远端失败/超时/版本 id 不符/无版本 → 503 且本地
//! 不可见、tee 清理、无 outbox；0/2 peer、删桶中、本地/远端 versioning 未就绪 →
//! 503；sync 上限 → 400；async 桶路径逐字节不变（照常入队）；接收端 token 校验与
//! origin id 采纳/重放/冲突。
//!
//! SigV4 用 s3s-sigv4 规范原语在测试内最小实现（同 m8_ap）。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use md5::{Digest as _, Md5};
use oss_server::config::{AppConfig, SiteToml, SyncToml};
use oss_server::db::{Db, PermissionRow, SitePeer};
use oss_server::state::AppState;
use oss_server::sync::{SiteSyncGate, recover_write_intents};
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
}

struct MockRemote {
    base: String,
    mode: Arc<Mutex<Mode>>,
    seen: Arc<Mutex<Vec<Seen>>>,
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
}

struct Env {
    st: AppState,
    base: String,
    http: reqwest::Client,
    remote: MockRemote,
    tee_dir: std::path::PathBuf,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

async fn spawn_remote(mode: Mode) -> MockRemote {
    use hyper::service::service_fn;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mode = Arc::new(Mutex::new(mode));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (mode2, seen2) = (Arc::clone(&mode), Arc::clone(&seen));
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let (mode, seen) = (Arc::clone(&mode2), Arc::clone(&seen2));
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let (mode, seen) = (Arc::clone(&mode), Arc::clone(&seen));
                    async move {
                        let method = req.method().to_string();
                        let hdr = |n: &str| {
                            req.headers()
                                .get(n)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string)
                        };
                        let uri = req.uri().to_string();
                        let authorization = hdr("authorization");
                        let token = hdr("x-oss-site-token");
                        let origin = hdr("x-oss-origin-version-id");
                        let replication = hdr("x-amz-replication-status");
                        let content_type = hdr("content-type");
                        let meta_foo = hdr("x-amz-meta-foo");
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
                        });
                        let m = *mode.lock().unwrap();
                        let mut resp = Response::new(Full::new(Bytes::new()));
                        if m == Mode::Slow {
                            tokio::time::sleep(Duration::from_millis(SLOW_MS)).await;
                        }
                        if method == "PUT" {
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
    let mut sc = StoreConfig::new(datadir.path());
    sc.gate = Some(Arc::new(SiteSyncGate::new(
        Arc::clone(&gate_db),
        Duration::from_millis(TIMEOUT_MS),
        sync_max,
        site_enabled,
    )));
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
    };
    std::fs::create_dir_all(&cfg.data_dir).unwrap();
    std::fs::create_dir_all(&cfg.staging_dir).unwrap();
    let db = Db::connect(&db_url).await.unwrap();
    let st = AppState::new(cfg, db, store).await;
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
        "replication_mode" | "versioning" | "deleting"
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
    let pairs: Vec<(String, String)> = Vec::new();
    let canonical = create_canonical_request(
        method.as_str(),
        req.url().path(),
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

#[tokio::test]
async fn remote_failure_wrong_id_and_unversioned_are_503_and_invisible() {
    let e = env(Mode::Fail, 1 << 20).await;
    let cases = [
        (Mode::Fail, "fail.txt"),
        (Mode::WrongId, "wrong.txt"),
        (Mode::NoVersion, "nover.txt"),
    ];
    for (mode, key) in cases {
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
    // 本地 versioning 未启用（sync 依赖双站同 id）
    set_bucket(&e.st, "syn", "deleting", "0").await;
    set_bucket(&e.st, "syn", "versioning", "Suspended").await;
    e.st.store.invalidate_gate("syn");
    assert_eq!(put(&e, "/syn/gate5.txt", b"x").await.status(), 503);
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

#[tokio::test]
async fn replica_intake_requires_token_and_adopts_origin_id() {
    let e = env(Mode::Ok, 1 << 20).await;
    let origin = uuid::Uuid::new_v4().to_string();
    let body = b"from-peer".to_vec();
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
    ];
    // 正确 token：采用 origin id，不调用远端门。
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
    // 同 id 同体重放幂等；同 id 异体冲突 400。
    assert_eq!(
        signed(&e, Method::PUT, "/syn/in3.txt", &h, &body)
            .await
            .status(),
        200
    );
    assert_eq!(
        signed(&e, Method::PUT, "/syn/in3.txt", &h, b"different")
            .await
            .status(),
        400
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
        503,
        "本端 versioning 未启用时不得摄入 REPLICA"
    );
    assert_eq!(
        signed(&e, Method::GET, "/syn/noversion.txt", &[], &[])
            .await
            .status(),
        404
    );
    assert!(e.remote.puts().is_empty());
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
    let dels = e.remote.deletes();
    assert_eq!(dels.len(), 1);
    assert!(
        dels[0].uri.contains(&format!("versionId={orphan}")),
        "删除必须点名同 id: {}",
        dels[0].uri
    );
    assert_eq!(intent_rows(&e.st).await, 0);
    assert!(!tee2.exists());

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
}
