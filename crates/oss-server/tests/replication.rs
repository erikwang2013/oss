//! M6 切片 4：复制——入队矩阵 + worker（R2 租约/R1 分级）+ SigV4 出站（mock remote）。

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use oss_server::config::RemoteTarget;
use oss_server::repl_client::ReplClient;
use oss_server::replication::{SqlReplSink, SqlReplStore, process_repl_once};
use sqlx::SqlitePool;
use xl_s3::replication::{ReplicationSink, ReplicationStore, WriteInfo};
use xl_store::store::{Store, StoreConfig};

async fn fixture() -> (SqlitePool, Arc<Store>, tempfile::TempDir, tempfile::TempDir) {
    let dbdir = tempfile::tempdir().unwrap();
    let path = dbdir.path().join("repl.db");
    let pool = SqlitePool::connect(&format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE replication_rules (bucket TEXT PRIMARY KEY, rules_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE repl_outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, bucket TEXT NOT NULL, key TEXT NOT NULL, \
          version_id TEXT NOT NULL, etag TEXT NOT NULL, size INTEGER NOT NULL, remote_id TEXT NOT NULL, \
          dest_bucket TEXT NOT NULL, is_delete_marker INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending', \
          attempts INTEGER NOT NULL DEFAULT 0, claimed_at INTEGER, next_retry_at INTEGER NOT NULL DEFAULT 0, \
          created_at INTEGER NOT NULL DEFAULT 0, last_error TEXT, \
          target_kind TEXT NOT NULL DEFAULT 'remote');\
         CREATE TABLE buckets (name TEXT PRIMARY KEY, versioning TEXT NOT NULL DEFAULT 'Off');\
         CREATE TABLE sites (peer_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL DEFAULT '', \
          s3_access_key TEXT NOT NULL DEFAULT '', s3_secret_key TEXT NOT NULL DEFAULT '', \
          region TEXT NOT NULL DEFAULT 'us-east-1', last_error TEXT);",
    )
    .execute(&pool)
    .await
    .unwrap();
    let datadir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(datadir.path())).await.unwrap());
    store.create_bucket("rbk").await.unwrap();
    (pool, store, dbdir, datadir)
}

async fn put_versioned(store: &Store, key: &str, body: &'static [u8]) -> (String, String) {
    let info = store
        .put_object_stream_with_meta(
            "rbk",
            key,
            Box::pin(futures::stream::once(async move {
                Ok(Bytes::from_static(body))
            })),
            None,
            Default::default(),
            None,
            true,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    (info.version_id.unwrap(), info.etag)
}

fn rules_json(prefix: Option<&str>, dm: bool) -> String {
    let r = xl_s3::replication::ReplRule {
        id: "r".into(),
        prefix: prefix.map(str::to_string),
        remote_id: "r1".into(),
        dest_bucket: "dst".into(),
        delete_marker: dm,
        enabled: true,
    };
    serde_json::to_string(&vec![r]).unwrap()
}

async fn outbox_rows(pool: &SqlitePool) -> Vec<(String, i64, i64, Option<String>)> {
    use sqlx::Row as _;
    sqlx::query(
        "SELECT state, attempts, is_delete_marker, last_error FROM repl_outbox ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| {
        (
            r.get::<String, _>("state"),
            r.get::<i64, _>("attempts"),
            r.get::<i64, _>("is_delete_marker"),
            r.get::<Option<String>, _>("last_error"),
        )
    })
    .collect()
}

#[tokio::test]
async fn enqueue_matrix_and_pending() {
    let (pool, store, _a, _b) = fixture().await;
    let (vid, etag) = put_versioned(&store, "logs/a", b"hello").await;
    let rs = Arc::new(SqlReplStore::new(pool.clone(), vec!["r1".into()]));
    rs.save_rules("rbk", &rules_json(Some("logs/"), true))
        .await
        .unwrap();
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));

    // 命中前缀 → 入队 + PENDING
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "logs/a",
        version_id: Some(&vid),
        etag: &etag,
        size: 5,
        is_delete_marker: false,
        is_replica: false,
    })
    .await;
    assert_eq!(outbox_rows(&pool).await.len(), 1);
    let h = store
        .head_object_version("rbk", "logs/a", &vid)
        .await
        .unwrap();
    assert_eq!(h.replication_status.as_deref(), Some("PENDING"));

    // 前缀不命中 / REPLICA / null 版本 → 不入队
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "other",
        version_id: Some(&vid),
        etag: &etag,
        size: 5,
        is_delete_marker: false,
        is_replica: false,
    })
    .await;
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "logs/a",
        version_id: Some(&vid),
        etag: &etag,
        size: 5,
        is_delete_marker: false,
        is_replica: true,
    })
    .await;
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "logs/a",
        version_id: Some("null"),
        etag: &etag,
        size: 5,
        is_delete_marker: false,
        is_replica: false,
    })
    .await;
    assert_eq!(outbox_rows(&pool).await.len(), 1);

    // marker：规则 delete_marker=false → 不入队；true → 入队
    rs.save_rules("rbk", &rules_json(None, false))
        .await
        .unwrap();
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "logs/a",
        version_id: Some(&vid),
        etag: "",
        size: 0,
        is_delete_marker: true,
        is_replica: false,
    })
    .await;
    assert_eq!(outbox_rows(&pool).await.len(), 1);
    rs.save_rules("rbk", &rules_json(None, true)).await.unwrap();
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "logs/a",
        version_id: Some(&vid),
        etag: "",
        size: 0,
        is_delete_marker: true,
        is_replica: false,
    })
    .await;
    assert_eq!(outbox_rows(&pool).await.len(), 2);
}

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    replication: Option<String>,
    authorization: Option<String>,
    body: Bytes,
}

type Script = Arc<dyn Fn(&str) -> (StatusCode, Option<String>) + Send + Sync>;

/// mock remote：按路径脚本化 (status, etag)；记录请求。
async fn mock_server_scripted(
    script: Script,
) -> (String, Arc<Mutex<Vec<Seen>>>, tokio::task::JoinHandle<()>) {
    use hyper::service::service_fn;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = Arc::clone(&seen);
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let seen = Arc::clone(&seen2);
            let script = Arc::clone(&script);
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let seen = Arc::clone(&seen);
                    let script = Arc::clone(&script);
                    async move {
                        let method = req.method().to_string();
                        let path = req.uri().path().to_string();
                        let replication = req
                            .headers()
                            .get("x-amz-replication-status")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        let authorization = req
                            .headers()
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let (status, etag) = script(&path);
                        seen.lock().unwrap().push(Seen {
                            method,
                            path,
                            replication,
                            authorization,
                            body,
                        });
                        let mut resp = Response::new(Full::new(Bytes::from_static(b"ok")));
                        *resp.status_mut() = status;
                        if let Some(e) = etag {
                            resp.headers_mut().insert(
                                "etag",
                                hyper::header::HeaderValue::from_str(&format!("\"{e}\"")).unwrap(),
                            );
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
    (format!("http://{addr}"), seen, handle)
}

async fn mock_server(
    status: StatusCode,
    etag: Option<String>,
) -> (String, Arc<Mutex<Vec<Seen>>>, tokio::task::JoinHandle<()>) {
    mock_server_scripted(Arc::new(move |_p: &str| (status, etag.clone()))).await
}

fn client(endpoint: &str) -> ReplClient {
    ReplClient::new(&[RemoteTarget {
        id: "r1".into(),
        endpoint: endpoint.into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
    }])
}

async fn enqueue_one(pool: &SqlitePool, store: Arc<Store>, key: &str) -> String {
    let (vid, etag) = put_versioned(&store, key, b"payload").await;
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    sqlx::query(
        "INSERT INTO replication_rules (bucket, rules_json) VALUES ('rbk', ?) \
         ON CONFLICT(bucket) DO UPDATE SET rules_json=excluded.rules_json",
    )
    .bind(rules_json(None, true))
    .execute(pool)
    .await
    .unwrap();
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key,
        version_id: Some(&vid),
        etag: &etag,
        size: 7,
        is_delete_marker: false,
        is_replica: false,
    })
    .await;
    vid
}

#[tokio::test]
async fn worker_put_success_signs_replica_header() {
    let (pool, store, _a, _b) = fixture().await;
    let vid = enqueue_one(&pool, Arc::clone(&store), "k").await;
    let etag = store.head_object("rbk", "k").await.unwrap().etag;
    let (endpoint, seen, _h) = mock_server(StatusCode::OK, Some(etag)).await;
    let c = client(&endpoint);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (1, 0));
    assert!(outbox_rows(&pool).await.is_empty());
    let h = store.head_object_version("rbk", "k", &vid).await.unwrap();
    assert_eq!(h.replication_status.as_deref(), Some("COMPLETED"));
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].method, "PUT");
    assert_eq!(seen[0].path, "/dst/k");
    assert_eq!(seen[0].replication.as_deref(), Some("REPLICA"));
    assert_eq!(seen[0].body, Bytes::from_static(b"payload"));
    let auth = seen[0].authorization.as_deref().unwrap();
    assert!(
        auth.contains("x-amz-replication-status"),
        "REPLICA 头必须纳入 SignedHeaders：{auth}"
    );
}

/// 行内 version_id 必须被尊重：对象已覆写时复制的是旧版本内容（不是 latest）。
#[tokio::test]
async fn worker_reads_recorded_version_not_latest() {
    let (pool, store, _a, _b) = fixture().await;
    let vid = enqueue_one(&pool, Arc::clone(&store), "k").await;
    let old_etag = store
        .head_object_version("rbk", "k", &vid)
        .await
        .unwrap()
        .etag;
    // 覆写（新版本、新内容）
    let _ = put_versioned(&store, "k", b"NEWER").await;
    let (endpoint, seen, _h) = mock_server(StatusCode::OK, Some(old_etag)).await;
    let c = client(&endpoint);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (1, 0));
    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen[0].body,
        Bytes::from_static(b"payload"),
        "应复制记录的旧版本"
    );
}

/// P2-1：多规则聚合——一个目的地成功不得提前 COMPLETED；任一永久错 bury 后 FAILED。
#[tokio::test]
async fn multi_rule_status_aggregates_and_fails_on_any_dead() {
    let (pool, store, _a, _b) = fixture().await;
    let (vid, etag) = put_versioned(&store, "k", b"payload").await;
    let rules = vec![
        xl_s3::replication::ReplRule {
            id: "a".into(),
            prefix: None,
            remote_id: "r1".into(),
            dest_bucket: "dst".into(),
            delete_marker: false,
            enabled: true,
        },
        xl_s3::replication::ReplRule {
            id: "b".into(),
            prefix: None,
            remote_id: "r1".into(),
            dest_bucket: "bad".into(),
            delete_marker: false,
            enabled: true,
        },
    ];
    sqlx::query("INSERT INTO replication_rules (bucket, rules_json) VALUES ('rbk', ?)")
        .bind(serde_json::to_string(&rules).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "k",
        version_id: Some(&vid),
        etag: &etag,
        size: 7,
        is_delete_marker: false,
        is_replica: false,
    })
    .await;
    assert_eq!(outbox_rows(&pool).await.len(), 2);

    let etag2 = etag.clone();
    let script = Arc::new(move |p: &str| {
        if p.contains("/bad/") {
            (StatusCode::FORBIDDEN, None)
        } else {
            (StatusCode::OK, Some(etag2.clone()))
        }
    });
    let (endpoint, _seen, _h) = mock_server_scripted(script).await;
    let c = client(&endpoint);
    let _ = process_repl_once(&pool, &store, &c).await;
    // dst 成功、bad 仍待重试 → 状态必须保持 PENDING（不得提前 COMPLETED）
    let h = store.head_object_version("rbk", "k", &vid).await.unwrap();
    assert_eq!(h.replication_status.as_deref(), Some("PENDING"));

    // 把 bad 推到超限 → bury → FAILED
    sqlx::query("UPDATE repl_outbox SET attempts=10, next_retry_at=0 WHERE state='pending'")
        .execute(&pool)
        .await
        .unwrap();
    let etag3 = store
        .head_object_version("rbk", "k", &vid)
        .await
        .unwrap()
        .etag;
    let script = Arc::new(move |p: &str| {
        if p.contains("/bad/") {
            (StatusCode::FORBIDDEN, None)
        } else {
            (StatusCode::OK, Some(etag3.clone()))
        }
    });
    let (endpoint, _seen, _h) = mock_server_scripted(script).await;
    let c = client(&endpoint);
    let _ = process_repl_once(&pool, &store, &c).await;
    let h = store.head_object_version("rbk", "k", &vid).await.unwrap();
    assert_eq!(h.replication_status.as_deref(), Some("FAILED"));
}

/// R2 租约回收：过期 sending 行被置回 pending 并成功投递。
#[tokio::test]
async fn worker_recovers_expired_lease() {
    let (pool, store, _a, _b) = fixture().await;
    let vid = enqueue_one(&pool, Arc::clone(&store), "k").await;
    let old = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 400;
    sqlx::query("UPDATE repl_outbox SET state='sending', claimed_at=?")
        .bind(old)
        .execute(&pool)
        .await
        .unwrap();
    let etag = store.head_object("rbk", "k").await.unwrap().etag;
    let (endpoint, _seen, _h) = mock_server(StatusCode::OK, Some(etag)).await;
    let c = client(&endpoint);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (1, 0));
    assert!(outbox_rows(&pool).await.is_empty());
    let h = store.head_object_version("rbk", "k", &vid).await.unwrap();
    assert_eq!(h.replication_status.as_deref(), Some("COMPLETED"));
}

/// MPU 源（ETag 含 "-N"）跳过目标 ETag 断言。
#[tokio::test]
async fn worker_skips_etag_assertion_for_multipart_source() {
    let (pool, store, _a, _b) = fixture().await;
    let _vid = enqueue_one(&pool, Arc::clone(&store), "k").await;
    sqlx::query("UPDATE repl_outbox SET etag='deadbeef-2'")
        .execute(&pool)
        .await
        .unwrap();
    let (endpoint, _seen, _h) = mock_server(StatusCode::OK, Some("different-etag".into())).await;
    let c = client(&endpoint);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (1, 0));
}

#[tokio::test]
async fn worker_retry_then_dead_and_marker_delete() {
    let (pool, store, _a, _b) = fixture().await;
    let vid = enqueue_one(&pool, Arc::clone(&store), "k").await;
    let etag = store.head_object("rbk", "k").await.unwrap().etag;
    // 500 → 可重试：pending + attempts=1，状态仍 PENDING
    let (endpoint, _seen, _h) = mock_server(StatusCode::INTERNAL_SERVER_ERROR, Some(etag)).await;
    let c = client(&endpoint);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (0, 1));
    let rows = outbox_rows(&pool).await;
    assert_eq!(rows[0].0, "pending");
    assert_eq!(rows[0].1, 1);
    // 403 → 永久错；把 attempts 推到 10 再失败 → dead + FAILED
    sqlx::query("UPDATE repl_outbox SET next_retry_at=0, attempts=10")
        .execute(&pool)
        .await
        .unwrap();
    let (endpoint, _seen, _h) = mock_server(StatusCode::FORBIDDEN, None).await;
    let c = client(&endpoint);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (0, 1));
    let rows = outbox_rows(&pool).await;
    assert_eq!(rows[0].0, "dead");
    assert_eq!(rows[0].1, 11);
    let h = store.head_object_version("rbk", "k", &vid).await.unwrap();
    assert_eq!(h.replication_status.as_deref(), Some("FAILED"));

    // marker 行 → DELETE 请求
    sqlx::query(
        "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, dest_bucket, \
         is_delete_marker, state, attempts, next_retry_at, created_at) \
         VALUES ('rbk','k',?, '', 0, 'r1', 'dst', 1, 'pending', 0, 0, 0)",
    )
    .bind(&vid)
    .execute(&pool)
    .await
    .unwrap();
    let (endpoint, seen, _h) = mock_server(StatusCode::OK, None).await;
    let c = client(&endpoint);
    let _ = process_repl_once(&pool, &store, &c).await;
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].method, "DELETE");
    assert_eq!(seen[0].path, "/dst/k");
    assert_eq!(
        seen[0].replication.as_deref(),
        Some("REPLICA"),
        "DELETE 必须带 REPLICA 头（防双向回环——P0-2）"
    );
}

#[tokio::test]
async fn worker_backlog_unreachable_then_recovers() {
    let (pool, store, _a, _b) = fixture().await;
    let _vid = enqueue_one(&pool, Arc::clone(&store), "k").await;
    // 不可达端点：连接错误 = 可重试，永不 dead
    let dead_port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let c = client(&format!("http://127.0.0.1:{dead_port}"));
    let _ = process_repl_once(&pool, &store, &c).await;
    let rows = outbox_rows(&pool).await;
    assert_eq!(rows[0].0, "pending");
    assert_eq!(rows[0].1, 1);
    // 恢复：真实 mock → 追平
    let etag = store.head_object("rbk", "k").await.unwrap().etag;
    let (endpoint, _seen, _h) = mock_server(StatusCode::OK, Some(etag)).await;
    sqlx::query("UPDATE repl_outbox SET next_retry_at=0")
        .execute(&pool)
        .await
        .unwrap();
    let c = client(&endpoint);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (1, 0));
    assert!(outbox_rows(&pool).await.is_empty());
}

// ---------------- M7 切片 2a/2b：站点入队矩阵 + 动态 peer 出站 ----------------

async fn site_fixture() -> (SqlitePool, Arc<Store>, tempfile::TempDir, tempfile::TempDir) {
    let (pool, store, a, b) = fixture().await;
    sqlx::query("INSERT INTO sites (peer_id) VALUES ('p1')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO buckets (name, versioning) VALUES ('rbk', 'Enabled')")
        .execute(&pool)
        .await
        .unwrap();
    (pool, store, a, b)
}

async fn site_count(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM repl_outbox WHERE target_kind='site'")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn site_enqueue_matrix() {
    let (pool, store, _a, _b) = site_fixture().await;
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    let (vid, etag) = put_versioned(&store, "k", b"x").await;
    macro_rules! info {
        ($v:expr, $marker:expr, $replica:expr) => {
            WriteInfo {
                bucket: "rbk",
                key: "k",
                version_id: $v,
                etag: &etag,
                size: 1,
                is_delete_marker: $marker,
                is_replica: $replica,
            }
        };
    }
    // 数据写 → 每个 peer 一行（无 replication_rules 也入队）。
    sink.on_write(info!(Some(&vid), false, false)).await;
    assert_eq!(site_count(&pool).await, 1);
    let row = sqlx::query(
        "SELECT target_kind, remote_id, dest_bucket, is_delete_marker, version_id \
         FROM repl_outbox WHERE target_kind='site'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    use sqlx::Row as _;
    assert_eq!(row.get::<String, _>("target_kind"), "site");
    assert_eq!(row.get::<String, _>("remote_id"), "p1");
    assert_eq!(row.get::<String, _>("dest_bucket"), "rbk");
    assert_eq!(row.get::<i64, _>("is_delete_marker"), 0);
    assert_eq!(row.get::<String, _>("version_id"), vid);

    // marker → 入队（版本桶 Enabled）。
    sink.on_write(info!(Some("dm-1"), true, false)).await;
    assert_eq!(site_count(&pool).await, 2);
    // REPLICA / null 版本 → 不入队。
    sink.on_write(info!(Some("v-other"), false, true)).await;
    sink.on_write(info!(Some("null"), false, false)).await;
    assert_eq!(site_count(&pool).await, 2);
    // 硬删（无版本）在 Enabled 桶 → 入队且版本列为空。
    sink.on_write(info!(None, false, false)).await;
    assert_eq!(site_count(&pool).await, 3);
    let empty: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE version_id='' AND target_kind='site'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(empty, 1);
    // Suspended → 删除不入队。
    sqlx::query("UPDATE buckets SET versioning='Suspended' WHERE name='rbk'")
        .execute(&pool)
        .await
        .unwrap();
    sink.on_write(info!(Some("dm-2"), true, false)).await;
    assert_eq!(site_count(&pool).await, 3);
}

#[derive(Clone, Debug)]
struct SiteSeen {
    method: String,
    uri: String,
    replication: Option<String>,
    content_type: Option<String>,
    meta_foo: Option<String>,
}

/// 站点 mock：GET ?versioning 按共享状态回答；PUT 回 etag + version-id
/// （key 含 "nullver" 时故意不回 version-id——P2 重试用例）。
async fn site_mock(
    versioning: Arc<Mutex<String>>,
    etag: String,
) -> (String, Arc<Mutex<Vec<SiteSeen>>>) {
    use hyper::service::service_fn;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = Arc::clone(&seen);
    let etag = Arc::new(etag);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let seen = Arc::clone(&seen2);
            let versioning = Arc::clone(&versioning);
            let etag = Arc::clone(&etag);
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let seen = Arc::clone(&seen);
                    let versioning = Arc::clone(&versioning);
                    let etag = Arc::clone(&etag);
                    async move {
                        let method = req.method().to_string();
                        let uri = req.uri().to_string();
                        let replication = req
                            .headers()
                            .get("x-amz-replication-status")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        let content_type = req
                            .headers()
                            .get("content-type")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        let meta_foo = req
                            .headers()
                            .get("x-amz-meta-foo")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        seen.lock().unwrap().push(SiteSeen {
                            method: method.clone(),
                            uri: uri.clone(),
                            replication,
                            content_type,
                            meta_foo,
                        });
                        let mut resp = Response::new(Full::new(Bytes::new()));
                        if method == "GET" {
                            let st = versioning.lock().unwrap().clone();
                            if st == "404" {
                                *resp.status_mut() = StatusCode::NOT_FOUND;
                            } else {
                                let xml = format!(
                                    "<VersioningConfiguration><Status>{st}</Status></VersioningConfiguration>"
                                );
                                *resp.body_mut() = Full::new(Bytes::from(xml));
                            }
                        } else if method == "PUT" {
                            resp.headers_mut()
                                .insert("etag", format!("\"{}\"", *etag).parse().unwrap());
                            if !uri.contains("nullver") {
                                resp.headers_mut()
                                    .insert("x-amz-version-id", "v-remote-1".parse().unwrap());
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
    (format!("http://{addr}"), seen)
}

#[tokio::test]
async fn site_worker_preflight_metadata_retry_and_null_version() {
    use std::collections::BTreeMap;
    let (pool, store, _a, _b) = site_fixture().await;
    let info = store
        .put_object_stream_with_meta(
            "rbk",
            "k",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"payload"))
            })),
            Some("text/plain".into()),
            BTreeMap::from([("foo".to_string(), "Bar".to_string())]),
            None,
            true,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let vid = info.version_id.clone().unwrap();
    let etag = info.etag.clone();
    // 站点行：白名单凭据写入 sites。
    sqlx::query(
        "UPDATE sites SET endpoint='http://127.0.0.1:1', s3_access_key='ak', \
         s3_secret_key='sk', region='us-east-1' WHERE peer_id='p1'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "k",
        version_id: Some(&vid),
        etag: &etag,
        size: 7,
        is_delete_marker: false,
        is_replica: false,
    })
    .await;
    assert_eq!(site_count(&pool).await, 1);

    // 1) 目标 versioning 404 → 可重试（不 PUT）。
    let state = Arc::new(Mutex::new("404".to_string()));
    let (endpoint, seen) = site_mock(Arc::clone(&state), etag.clone()).await;
    sqlx::query("UPDATE sites SET endpoint=? WHERE peer_id='p1'")
        .bind(&endpoint)
        .execute(&pool)
        .await
        .unwrap();
    let outer = ReplClient::new(&[]);
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (0, 1));
    assert!(seen.lock().unwrap().iter().all(|s| s.method == "GET"));
    assert_eq!(outbox_rows(&pool).await[0].0, "pending");
    // P3-7：把 attempts 推到 bury 阈值——仍 pending 才证明 404 被归为可重试（而非 11 轮后 dead）。
    sqlx::query("UPDATE repl_outbox SET attempts=10, next_retry_at=0 WHERE target_kind='site'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (0, 1));
    let rows = outbox_rows(&pool).await;
    assert_eq!(rows[0].0, "pending", "404 预检必须是可重试：{rows:?}");
    assert_eq!(rows[0].1, 11);
    assert!(
        rows[0].3.as_deref().unwrap_or("").contains("versioning"),
        "失败原因应记录 versioning 探询：{:?}",
        rows[0].3
    );

    // 2) 就绪 → PUT 带 REPLICA/content-type/metadata，且要求真实 version-id。
    *state.lock().unwrap() = "Enabled".into();
    sqlx::query("UPDATE repl_outbox SET next_retry_at=0")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (1, 0));
    let seen_snapshot = seen.lock().unwrap().clone();
    let puts: Vec<&SiteSeen> = seen_snapshot.iter().filter(|s| s.method == "PUT").collect();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].replication.as_deref(), Some("REPLICA"));
    assert_eq!(puts[0].content_type.as_deref(), Some("text/plain"));
    assert_eq!(puts[0].meta_foo.as_deref(), Some("Bar"));
    let gets: Vec<&SiteSeen> = seen_snapshot.iter().filter(|s| s.method == "GET").collect();
    assert!(
        gets.iter().any(|g| g.uri.contains("versioning=")),
        "预检必须是 ?versioning：{gets:?}"
    );
    let h = store.head_object_version("rbk", "k", &vid).await.unwrap();
    assert_eq!(h.replication_status.as_deref(), Some("COMPLETED"));

    // 2b) marker 与硬删行都以 DELETE 发送（带 REPLICA 防回环——P0-2/P2-5）。
    let (dm_vid, dm_etag) = put_versioned(&store, "dm", b"dm").await;
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "dm",
        version_id: Some(&dm_vid),
        etag: &dm_etag,
        size: 0,
        is_delete_marker: true,
        is_replica: false,
    })
    .await;
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "hard",
        version_id: None,
        etag: "",
        size: 0,
        is_delete_marker: false,
        is_replica: false,
    })
    .await;
    sqlx::query("UPDATE repl_outbox SET next_retry_at=0 WHERE target_kind='site'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (2, 0));
    let seen = seen.lock().unwrap().clone();
    let dels: Vec<&SiteSeen> = seen.iter().filter(|s| s.method == "DELETE").collect();
    assert_eq!(dels.len(), 2, "marker 与硬删都走 DELETE");
    assert!(
        dels.iter()
            .all(|d| d.replication.as_deref() == Some("REPLICA")),
        "DELETE 必须带 REPLICA 头"
    );

    // 3) PUT 响应 version-id 为 null/缺失 → 可重试，不静默成功。
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    // 与 "k" 同内容 → ETag 与 mock 返回值一致，确保失败原因是 version-id 缺失而非 ETag 断言。
    let (vid2, etag2) = put_versioned(&store, "nullver", b"payload").await;
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "nullver",
        version_id: Some(&vid2),
        etag: &etag2,
        size: 1,
        is_delete_marker: false,
        is_replica: false,
    })
    .await;
    // 等预检缓存过期不必要：同名桶缓存 Enabled（10s）。PUT 缺 version-id → pending。
    sqlx::query("UPDATE repl_outbox SET next_retry_at=0 WHERE key='nullver'")
        .execute(&pool)
        .await
        .unwrap();
    let (sent, failed) = process_repl_once(&pool, &store, &outer).await;
    assert_eq!((sent, failed), (0, 1));
    sqlx::query("UPDATE repl_outbox SET attempts=10, next_retry_at=0 WHERE key='nullver'")
        .execute(&pool)
        .await
        .unwrap();
    let (sent, failed) = process_repl_once(&pool, &store, &outer).await;
    assert_eq!((sent, failed), (0, 1));
    let (st, err): (String, Option<String>) = sqlx::query_as(
        "SELECT state, last_error FROM repl_outbox WHERE key='nullver' AND target_kind='site'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        st, "pending",
        "null version-id 必须可重试（11 轮也不 dead）"
    );
    assert!(
        err.unwrap_or_default().contains("version-id"),
        "失败原因应指向 version-id"
    );
}
