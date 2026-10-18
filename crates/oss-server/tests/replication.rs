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
          created_at INTEGER NOT NULL DEFAULT 0, last_error TEXT);",
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
    assert!(seen[0].replication.is_none(), "DELETE 不带 REPLICA 头");
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
