//! M6 切片 4：复制——入队矩阵 + worker（R2 租约/R1 分级）+ SigV4 出站（mock remote）。

use std::sync::{Arc, Mutex};
use xl_store::store::VersioningMode;

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
          target_kind TEXT NOT NULL DEFAULT 'remote', \
          is_version_delete INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE buckets (name TEXT PRIMARY KEY, versioning TEXT NOT NULL DEFAULT 'Off');\
         CREATE TABLE sites (peer_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL DEFAULT '', \
          s3_access_key TEXT NOT NULL DEFAULT '', s3_secret_key TEXT NOT NULL DEFAULT '', \
          region TEXT NOT NULL DEFAULT 'us-east-1', peer_site_token TEXT NOT NULL DEFAULT '', \
          last_error TEXT);",
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
    put_versioned_owned(store, key, Bytes::from_static(body)).await
}

async fn put_versioned_owned(store: &Store, key: &str, body: Bytes) -> (String, String) {
    let info = store
        .put_object_stream_with_meta(
            "rbk",
            key,
            Box::pin(futures::stream::once(async move { Ok(body) })),
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
        is_version_delete: false,
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
        is_version_delete: false,
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
        is_version_delete: false,
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
        is_version_delete: false,
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
        is_version_delete: false,
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
        is_version_delete: false,
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

fn target(id: &str, endpoint: &str, max: Option<u64>) -> RemoteTarget {
    RemoteTarget {
        id: id.into(),
        endpoint: endpoint.into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
        max_bytes_per_sec: max,
        server_name: None,
        site_token: None,
    }
}

fn client(endpoint: &str) -> ReplClient {
    ReplClient::new(&[target("r1", endpoint, None)])
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
        is_version_delete: false,
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

/// M8 缺口 C：`max_bytes_per_sec` 对多 MiB 对象生效（对象级整形）。
/// 2 MiB @ 1 MiB/s：初始桶 1 MiB，再等 ~1s；下界取宽松的 700ms（高负载只增不减）。
#[tokio::test]
async fn bandwidth_limit_paces_large_put() {
    let (pool, store, _a, _b) = fixture().await;
    let body = Bytes::from(vec![7u8; 2 * 1024 * 1024]);
    let (vid, etag) = put_versioned_owned(&store, "big", body.clone()).await;
    sqlx::query(
        "INSERT INTO replication_rules (bucket, rules_json) VALUES ('rbk', ?) \
         ON CONFLICT(bucket) DO UPDATE SET rules_json=excluded.rules_json",
    )
    .bind(rules_json(None, false))
    .execute(&pool)
    .await
    .unwrap();
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "big",
        version_id: Some(&vid),
        etag: &etag,
        size: body.len() as u64,
        is_delete_marker: false,
        is_version_delete: false,
        is_replica: false,
    })
    .await;
    let (endpoint, seen, _h) = mock_server(StatusCode::OK, Some(etag)).await;
    let c = ReplClient::new(&[target("r1", &endpoint, Some(1024 * 1024))]);
    let start = std::time::Instant::now();
    assert_eq!(process_repl_once(&pool, &store, &c).await, (1, 0));
    let elapsed = start.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_millis(700),
        "限速未生效（应等待 ~1s）：{elapsed:?}"
    );
    assert_eq!(seen.lock().unwrap()[0].body.len(), body.len());
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
        is_version_delete: false,
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
    sqlx::query("INSERT INTO sites (peer_id, peer_site_token) VALUES ('p1', 'tok-p1')")
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
                is_version_delete: false,
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
    // 4.16：版本定向删除 → 站点一行（is_version_delete=1），M6 远端规则不入队。
    sqlx::query("INSERT INTO replication_rules (bucket, rules_json) VALUES ('rbk', ?)")
        .bind(rules_json(None, true))
        .execute(&pool)
        .await
        .unwrap();
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "k",
        version_id: Some("v-del"),
        etag: "",
        size: 0,
        is_delete_marker: false,
        is_version_delete: true,
        is_replica: false,
    })
    .await;
    assert_eq!(site_count(&pool).await, 4);
    let row = sqlx::query(
        "SELECT is_delete_marker, is_version_delete FROM repl_outbox \
         WHERE target_kind='site' AND version_id='v-del'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<i64, _>("is_delete_marker"), 0);
    assert_eq!(row.get::<i64, _>("is_version_delete"), 1);
    let remote: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repl_outbox WHERE target_kind='remote' AND version_id='v-del'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(remote, 0, "版本定向删除无 M6 远端 id 映射，不得入远端队列");
    // null 版本删除不跨界（站点 PUT 用真实 UUID 同源 id）。
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "k",
        version_id: Some("null"),
        etag: "",
        size: 0,
        is_delete_marker: false,
        is_version_delete: true,
        is_replica: false,
    })
    .await;
    assert_eq!(site_count(&pool).await, 4);
    // Suspended → 删除不入队。
    sqlx::query("UPDATE buckets SET versioning='Suspended' WHERE name='rbk'")
        .execute(&pool)
        .await
        .unwrap();
    sink.on_write(info!(Some("dm-2"), true, false)).await;
    assert_eq!(site_count(&pool).await, 4);
}

#[derive(Clone, Debug)]
struct SiteSeen {
    method: String,
    uri: String,
    replication: Option<String>,
    content_type: Option<String>,
    meta_foo: Option<String>,
    part_sizes: Option<String>,
    origin_version_id: Option<String>,
    origin_etag: Option<String>,
    origin_size: Option<String>,
    site_token: Option<String>,
    tags: Option<String>,
    storage_class: Option<String>,
    lock_mode: Option<String>,
    lock_until: Option<String>,
    lock_hold: Option<String>,
    bypass: Option<String>,
    sse: Option<String>,
    sse_kms: Option<String>,
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
                        let hdr = |n: &str| {
                            req.headers()
                                .get(n)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_string)
                        };
                        let part_sizes = hdr("x-oss-part-sizes");
                        let origin_version_id = hdr("x-oss-origin-version-id");
                        let origin_etag = hdr("x-oss-origin-etag");
                        let origin_size = hdr("x-oss-origin-size");
                        let site_token = hdr("x-oss-site-token");
                        let tags = hdr("x-amz-tagging");
                        let storage_class = hdr("x-amz-storage-class");
                        let lock_mode = hdr("x-amz-object-lock-mode");
                        let lock_until = hdr("x-amz-object-lock-retain-until-date");
                        let lock_hold = hdr("x-amz-object-lock-legal-hold");
                        let bypass = hdr("x-amz-bypass-governance-retention");
                        let sse = hdr("x-amz-server-side-encryption");
                        let sse_kms = hdr("x-amz-server-side-encryption-aws-kms-key-id");
                        seen.lock().unwrap().push(SiteSeen {
                            method: method.clone(),
                            uri: uri.clone(),
                            replication,
                            content_type,
                            meta_foo,
                            part_sizes,
                            origin_version_id: origin_version_id.clone(),
                            origin_etag,
                            origin_size,
                            site_token,
                            tags,
                            storage_class,
                            lock_mode,
                            lock_until,
                            lock_hold,
                            bypass,
                            sse,
                            sse_kms,
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
                                // 4.16：目标按同源版本 id 落库——回显请求头，模拟真实站点。
                                let vid =
                                    origin_version_id.unwrap_or_else(|| "v-remote-1".to_string());
                                resp.headers_mut()
                                    .insert("x-amz-version-id", vid.parse().unwrap());
                            }
                        } else if method == "DELETE" && uri.contains("already-gone") {
                            // 4.16：目标版本已不存在（404）——幂等已删。
                            *resp.status_mut() = StatusCode::NOT_FOUND;
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
        .put_object_stream_with_meta_ext(
            "rbk",
            "k",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"payload"))
            })),
            Some("text/plain".into()),
            BTreeMap::from([("foo".to_string(), "Bar".to_string())]),
            None,
            VersioningMode::Enabled,
            None,
            None,
            None,
            None,
            xl_store::store::PutExt {
                part_sizes: Some(vec![3, 4]),
                ..Default::default()
            },
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
        is_version_delete: false,
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
    // 4.1：MPU 段长表随站点复制透传（源 7B = 3+4）
    assert_eq!(puts[0].part_sizes.as_deref(), Some("3,4"));
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
        is_version_delete: false,
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
        is_version_delete: false,
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
    // 4.16：站点 marker 用源 marker id 创建（不带 versionId），后续版本删除才能命中。
    let dm_del = dels
        .iter()
        .find(|d| d.uri.ends_with("/dm"))
        .expect("marker 行");
    assert_eq!(dm_del.origin_version_id.as_deref(), Some(dm_vid.as_str()));
    assert_eq!(dm_del.site_token.as_deref(), Some("tok-p1"));
    assert!(!dm_del.uri.contains("versionId="));

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
        is_version_delete: false,
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

/// 4.16：站点版本定向删除走标准 `DELETE ?versionId=`（带 REPLICA），绝不 PUT/造 marker。
#[tokio::test]
async fn site_version_delete_is_replicated_as_versioned_delete() {
    let (pool, store, _a, _b) = site_fixture().await;
    let state = Arc::new(Mutex::new("Enabled".to_string()));
    let (endpoint, seen) = site_mock(Arc::clone(&state), "e".into()).await;
    sqlx::query(
        "UPDATE sites SET endpoint=?, s3_access_key='ak', s3_secret_key='sk' \
         WHERE peer_id='p1'",
    )
    .bind(&endpoint)
    .execute(&pool)
    .await
    .unwrap();
    let vid = "11111111-1111-4111-8111-111111111111";
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "gone",
        version_id: Some(vid),
        etag: "",
        size: 0,
        is_delete_marker: false,
        is_version_delete: true,
        is_replica: false,
    })
    .await;
    let outer = ReplClient::new(&[]);
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (1, 0));
    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.iter().all(|s| s.method != "PUT"),
        "版本删除绝不能以 PUT 表达（否则目标会复活/造新版本）：{seen:?}"
    );
    let dels: Vec<&SiteSeen> = seen.iter().filter(|s| s.method == "DELETE").collect();
    assert_eq!(dels.len(), 1);
    assert!(
        dels[0].uri.contains(&format!("versionId={vid}")),
        "必须是定点版本删除：{}",
        dels[0].uri
    );
    assert_eq!(dels[0].replication.as_deref(), Some("REPLICA"));
    assert_eq!(
        dels[0].bypass.as_deref(),
        Some("true"),
        "源端已授权，目标按同值复核"
    );
    assert_eq!(site_count(&pool).await, 0, "成功行应删除");
}

/// 4.16：目标版本已不存在（404）视为已删——幂等成功，不重试、不造 marker。
#[tokio::test]
async fn site_version_delete_treats_target_404_as_already_deleted() {
    let (pool, store, _a, _b) = site_fixture().await;
    let state = Arc::new(Mutex::new("Enabled".to_string()));
    let (endpoint, seen) = site_mock(Arc::clone(&state), "e".into()).await;
    sqlx::query("UPDATE sites SET endpoint=? WHERE peer_id='p1'")
        .bind(&endpoint)
        .execute(&pool)
        .await
        .unwrap();
    let vid = "33333333-3333-4333-8333-333333333333";
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "already-gone",
        version_id: Some(vid),
        etag: "",
        size: 0,
        is_delete_marker: false,
        is_version_delete: true,
        is_replica: false,
    })
    .await;
    let outer = ReplClient::new(&[]);
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (1, 0));
    assert_eq!(site_count(&pool).await, 0, "404 应按已删结算");
    assert!(
        seen.lock().unwrap().iter().all(|s| s.method != "PUT"),
        "删除不得退化成 PUT"
    );
}

/// 4.16：同版本更早的 PUT 行仍未完成时，版本删除必须等待（防目标复活）。
#[tokio::test]
async fn site_version_delete_waits_for_same_version_put() {
    let (pool, store, _a, _b) = site_fixture().await;
    let state = Arc::new(Mutex::new("Enabled".to_string()));
    let (endpoint, seen) = site_mock(Arc::clone(&state), "e".into()).await;
    sqlx::query("UPDATE sites SET endpoint=? WHERE peer_id='p1'")
        .bind(&endpoint)
        .execute(&pool)
        .await
        .unwrap();
    let vid = "22222222-2222-4222-8222-222222222222";
    let ins = "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, \
               dest_bucket, is_delete_marker, is_version_delete, target_kind, state, attempts, \
               next_retry_at, created_at) \
               VALUES ('rbk', 'ord', ?, 'e', 0, 'p1', 'rbk', 0, ?, 'site', 'pending', 0, ?, 0)";
    // 更早的 PUT 行（未到重试时刻）挡住版本删除。
    sqlx::query(ins)
        .bind(vid)
        .bind(0_i64)
        .bind(9_999_999_999_i64)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(ins)
        .bind(vid)
        .bind(1_i64)
        .bind(0_i64)
        .execute(&pool)
        .await
        .unwrap();
    let outer = ReplClient::new(&[]);
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (0, 1));
    assert!(
        seen.lock().unwrap().iter().all(|s| s.method == "GET"),
        "等待期间不得发送删除"
    );
    let (st, err): (String, Option<String>) =
        sqlx::query_as("SELECT state, last_error FROM repl_outbox WHERE is_version_delete=1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(st, "pending");
    let err = err.unwrap_or_default();
    assert!(err.contains("保序"), "{err}");
    // 更早行完成（删除）后放行。
    sqlx::query("DELETE FROM repl_outbox WHERE is_version_delete=0")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE repl_outbox SET next_retry_at=0 WHERE is_version_delete=1")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (1, 0));
    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.iter()
            .any(|s| s.method == "DELETE" && s.uri.contains(&format!("versionId={vid}")))
    );
}

/// 4.17：站点 PUT 随版本复制标签/存储类/锁，并用同源 id + 站点 token（4.16 前提）。
#[tokio::test]
async fn site_put_replicates_tags_storage_class_and_lock() {
    use std::collections::BTreeMap;
    use xl_store::meta::{ObjectLockRecord, ObjectTag};
    let (pool, store, _a, _b) = site_fixture().await;
    let until = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 + 3_600_000_000_000;
    let info = store
        .put_object_stream_with_meta_ext(
            "rbk",
            "meta",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"payload"))
            })),
            Some("text/plain".into()),
            BTreeMap::from([("foo".to_string(), "Bar".to_string())]),
            None,
            VersioningMode::Enabled,
            None,
            Some(ObjectLockRecord {
                mode: Some("GOVERNANCE".into()),
                retain_until_nanos: Some(until),
                legal_hold: true,
            }),
            Some(vec![ObjectTag {
                key: "k1".into(),
                value: "v1".into(),
            }]),
            Some("STANDARD_IA".into()),
            xl_store::store::PutExt::default(),
        )
        .await
        .unwrap();
    let vid = info.version_id.clone().unwrap();
    let state = Arc::new(Mutex::new("Enabled".to_string()));
    let (endpoint, seen) = site_mock(Arc::clone(&state), info.etag.clone()).await;
    sqlx::query(
        "UPDATE sites SET endpoint=?, s3_access_key='ak', s3_secret_key='sk' \
         WHERE peer_id='p1'",
    )
    .bind(&endpoint)
    .execute(&pool)
    .await
    .unwrap();
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "meta",
        version_id: Some(&vid),
        etag: &info.etag,
        size: info.size,
        is_delete_marker: false,
        is_version_delete: false,
        is_replica: false,
    })
    .await;
    let outer = ReplClient::new(&[]);
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (1, 0));
    let seen_put = seen.lock().unwrap().clone();
    let puts: Vec<&SiteSeen> = seen_put.iter().filter(|s| s.method == "PUT").collect();
    assert_eq!(puts.len(), 1);
    let p = puts[0];
    assert_eq!(p.origin_version_id.as_deref(), Some(vid.as_str()));
    assert_eq!(p.site_token.as_deref(), Some("tok-p1"));
    assert_eq!(p.tags.as_deref(), Some("k1=v1"));
    assert_eq!(p.storage_class.as_deref(), Some("STANDARD_IA"));
    assert_eq!(p.meta_foo.as_deref(), Some("Bar"));
    assert_eq!(p.lock_mode.as_deref(), Some("GOVERNANCE"));
    assert_eq!(p.lock_hold.as_deref(), Some("ON"));
    let got = time::OffsetDateTime::parse(
        p.lock_until.as_deref().unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    assert_eq!(got.unix_timestamp_nanos() as i64, until);

    // 随后版本删除：同一目标版本被定点删，且不再发 PUT。
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "meta",
        version_id: Some(&vid),
        etag: "",
        size: 0,
        is_delete_marker: false,
        is_version_delete: true,
        is_replica: false,
    })
    .await;
    assert_eq!(process_repl_once(&pool, &store, &outer).await, (1, 0));
    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen.iter().filter(|s| s.method == "PUT").count(),
        1,
        "版本删除不得产生新 PUT"
    );
    assert!(
        seen.iter()
            .any(|s| s.method == "DELETE" && s.uri.contains(&format!("versionId={vid}")))
    );
}

/// 4.17：SSE 方案随复制下传；SSE-C 无法转封装 → 客户端层 fail-closed。
#[tokio::test]
async fn put_with_meta_sends_sse_scheme_and_rejects_sse_c() {
    use std::collections::BTreeMap;
    use xl_store::meta::EncryptionRecord;
    let state = Arc::new(Mutex::new("Enabled".to_string()));
    let (endpoint, seen) = site_mock(Arc::clone(&state), "e".into()).await;
    let c = client(&endpoint);
    let enc = |key_id: &str| EncryptionRecord {
        algorithm: "AES256".into(),
        key_id: key_id.into(),
        wrapped_dek_hex: String::new(),
        nonce_seed_hex: String::new(),
        sse_c_key_md5: None,
        kms_key_id: None,
    };
    let empty = BTreeMap::new();
    c.put_object_with_meta(
        "r1",
        "dst",
        "s3",
        Bytes::from_static(b"x"),
        "e",
        None,
        &empty,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
        Some(&enc("file")),
        None,
    )
    .await
    .unwrap();
    let seen_snapshot = seen.lock().unwrap().clone();
    let s3 = seen_snapshot
        .iter()
        .find(|s| s.uri.ends_with("/s3"))
        .unwrap();
    assert_eq!(s3.sse.as_deref(), Some("AES256"));
    assert_eq!(s3.sse_kms, None);
    // SSE-KMS：key_id 是站点本地主密钥，目标拒绝显式 CMK → 永久错且不发请求，
    // 不得静默换钥或降级明文。
    let before = seen.lock().unwrap().len();
    let err = c
        .put_object_with_meta(
            "r1",
            "dst",
            "kms",
            Bytes::from_static(b"x"),
            "e",
            None,
            &empty,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            Some(&enc("k3")),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.class, oss_server::repl_client::ErrClass::Permanent);
    assert_eq!(seen.lock().unwrap().len(), before, "SSE-KMS 不得发请求");
    // SSE-C：目标无法用客户钥重加密（钥不落盘）→ 永久错且不发请求。
    let before = seen.lock().unwrap().len();
    let err = c
        .put_object_with_meta(
            "r1",
            "dst",
            "ssec",
            Bytes::from_static(b"x"),
            "e",
            None,
            &empty,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            Some(&enc("sse-c")),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.class, oss_server::repl_client::ErrClass::Permanent);
    assert_eq!(seen.lock().unwrap().len(), before, "SSE-C 不得发请求");
}

// ---------------- 4.19：M6 远端复用 M7 载荷/删除机制 ----------------

fn token_target(id: &str, endpoint: &str) -> RemoteTarget {
    RemoteTarget {
        site_token: Some("tok-m6".into()),
        ..target(id, endpoint, None)
    }
}

async fn remote_rows(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM repl_outbox WHERE target_kind='remote'")
        .fetch_one(pool)
        .await
        .unwrap()
}

/// 4.19：配置 `site_token` 的 M6 远端——完整元数据 + 同源版本 id（目标按 id 落库）。
#[tokio::test]
async fn m6_token_remote_replicates_metadata_and_same_origin_id() {
    use std::collections::BTreeMap;
    use xl_store::meta::{ObjectLockRecord, ObjectTag};
    let (pool, store, _a, _b) = fixture().await;
    let until = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 + 3_600_000_000_000;
    let info = store
        .put_object_stream_with_meta_ext(
            "rbk",
            "meta",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"payload"))
            })),
            Some("text/plain".into()),
            BTreeMap::from([("foo".to_string(), "Bar".to_string())]),
            None,
            VersioningMode::Enabled,
            None,
            Some(ObjectLockRecord {
                mode: Some("GOVERNANCE".into()),
                retain_until_nanos: Some(until),
                legal_hold: true,
            }),
            Some(vec![ObjectTag {
                key: "k1".into(),
                value: "v1".into(),
            }]),
            Some("STANDARD_IA".into()),
            xl_store::store::PutExt::default(),
        )
        .await
        .unwrap();
    let vid = info.version_id.clone().unwrap();
    let state = Arc::new(Mutex::new("Enabled".to_string()));
    let (endpoint, seen) = site_mock(Arc::clone(&state), info.etag.clone()).await;
    sqlx::query("INSERT INTO replication_rules (bucket, rules_json) VALUES ('rbk', ?)")
        .bind(rules_json(None, true))
        .execute(&pool)
        .await
        .unwrap();
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store))
        .with_versioned_remotes(vec!["r1".into()]);
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "meta",
        version_id: Some(&vid),
        etag: &info.etag,
        size: info.size,
        is_delete_marker: false,
        is_version_delete: false,
        is_replica: false,
    })
    .await;
    let c = ReplClient::new(&[token_target("r1", &endpoint)]);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (1, 0));
    let seen = seen.lock().unwrap().clone();
    let puts: Vec<&SiteSeen> = seen.iter().filter(|s| s.method == "PUT").collect();
    assert_eq!(puts.len(), 1);
    let p = puts[0];
    assert_eq!(p.origin_version_id.as_deref(), Some(vid.as_str()));
    assert_eq!(p.site_token.as_deref(), Some("tok-m6"));
    assert_eq!(p.replication.as_deref(), Some("REPLICA"));
    assert_eq!(p.content_type.as_deref(), Some("text/plain"));
    assert_eq!(p.meta_foo.as_deref(), Some("Bar"));
    assert_eq!(p.tags.as_deref(), Some("k1=v1"));
    assert_eq!(p.storage_class.as_deref(), Some("STANDARD_IA"));
    assert_eq!(p.lock_mode.as_deref(), Some("GOVERNANCE"));
    assert_eq!(p.lock_hold.as_deref(), Some("ON"));
    assert!(
        seen.iter()
            .any(|s| s.method == "GET" && s.uri.contains("versioning=")),
        "版本化远端必须先探询目标 versioning"
    );
    let h = store
        .head_object_version("rbk", "meta", &vid)
        .await
        .unwrap();
    assert_eq!(h.replication_status.as_deref(), Some("COMPLETED"));
}

/// 4.19：M6 版本删除只投递给同源 id 远端（源桶 Enabled），带内容指纹与幂等 404；
/// 旧 M6 远端（无 token）保持「版本删除不入队」原语义。
#[tokio::test]
async fn m6_token_remote_replicates_version_delete_with_fingerprint() {
    let (pool, store, _a, _b) = fixture().await;
    sqlx::query("INSERT INTO buckets (name, versioning) VALUES ('rbk', 'Enabled')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO replication_rules (bucket, rules_json) VALUES ('rbk', ?)")
        .bind(rules_json(None, true))
        .execute(&pool)
        .await
        .unwrap();
    // 真实删除以取得源端内容指纹。
    let (vid, _etag) = put_versioned(&store, "gone", b"payload").await;
    let out = store
        .delete_object_versioned("rbk", "gone", Some(&vid), VersioningMode::Enabled)
        .await
        .unwrap();
    let (fp_etag, fp_size) = match out {
        xl_store::store::DeleteOutcome::Version {
            content: Some(c), ..
        } => c,
        other => panic!("unexpected: {other:?}"),
    };
    let wi = |key: &'static str, vid: &'static str| WriteInfo {
        bucket: "rbk",
        key,
        version_id: Some(vid),
        etag: &fp_etag,
        size: fp_size,
        is_delete_marker: false,
        is_version_delete: true,
        is_replica: false,
    };
    // 旧 M6 sink（未配置 versioned remotes）→ 不入队。
    SqlReplSink::new(pool.clone(), Arc::clone(&store))
        .on_write(wi("gone", "v-old"))
        .await;
    assert_eq!(remote_rows(&pool).await, 0, "无 token 远端不得入队版本删除");

    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store))
        .with_versioned_remotes(vec!["r1".into()]);
    sink.on_write(wi("gone", "v-del")).await;
    assert_eq!(remote_rows(&pool).await, 1);
    // 源桶 Suspended → 不入队（与站点路径同口径）。
    sqlx::query("UPDATE buckets SET versioning='Suspended' WHERE name='rbk'")
        .execute(&pool)
        .await
        .unwrap();
    sink.on_write(wi("gone", "v-del2")).await;
    assert_eq!(remote_rows(&pool).await, 1);
    sqlx::query("UPDATE buckets SET versioning='Enabled' WHERE name='rbk'")
        .execute(&pool)
        .await
        .unwrap();
    // 目标已无该版本（404）→ 幂等成功。
    sink.on_write(wi("already-gone", "v-del3")).await;
    assert_eq!(remote_rows(&pool).await, 2);

    let state = Arc::new(Mutex::new("Enabled".to_string()));
    let (endpoint, seen) = site_mock(Arc::clone(&state), "e".into()).await;
    let c = ReplClient::new(&[token_target("r1", &endpoint)]);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (2, 0));
    assert_eq!(remote_rows(&pool).await, 0, "成功行应删除");
    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.iter().all(|s| s.method != "PUT"),
        "版本删除绝不能退化成 PUT：{seen:?}"
    );
    let del = seen
        .iter()
        .find(|s| s.method == "DELETE" && s.uri.contains("versionId=v-del"))
        .expect("定点删除");
    assert_eq!(del.origin_version_id.as_deref(), Some("v-del"));
    assert_eq!(del.site_token.as_deref(), Some("tok-m6"));
    assert_eq!(del.origin_etag.as_deref(), Some(fp_etag.as_str()));
    assert_eq!(
        del.origin_size.as_deref(),
        Some(fp_size.to_string().as_str())
    );
    assert_eq!(del.bypass.as_deref(), Some("true"));
    assert_eq!(del.replication.as_deref(), Some("REPLICA"));
}

/// 4.19：SSE-KMS 源不得以明文/换钥复制——永久错、不发任何目标请求。
#[tokio::test]
async fn m6_worker_dead_letters_sse_kms_without_request() {
    let (pool, store, _a, datadir) = fixture().await;
    let sys = xl_store::layout::sys_dir(datadir.path(), "");
    store
        .init_encryption(None, Some(sys.join("kms")))
        .await
        .unwrap();
    let info = store
        .put_object_stream_with_meta_ext(
            "rbk",
            "kms",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"payload"))
            })),
            None,
            Default::default(),
            None,
            VersioningMode::Enabled,
            Some(xl_store::crypto::EncSpec::SseKms),
            None,
            None,
            None,
            xl_store::store::PutExt::default(),
        )
        .await
        .unwrap();
    let vid = info.version_id.clone().unwrap();
    let (endpoint, seen, _h) = mock_server(StatusCode::OK, Some(info.etag.clone())).await;
    let sink = SqlReplSink::new(pool.clone(), Arc::clone(&store));
    sqlx::query("INSERT INTO replication_rules (bucket, rules_json) VALUES ('rbk', ?)")
        .bind(rules_json(None, true))
        .execute(&pool)
        .await
        .unwrap();
    sink.on_write(WriteInfo {
        bucket: "rbk",
        key: "kms",
        version_id: Some(&vid),
        etag: &info.etag,
        size: info.size,
        is_delete_marker: false,
        is_version_delete: false,
        is_replica: false,
    })
    .await;
    sqlx::query("UPDATE repl_outbox SET attempts=10")
        .execute(&pool)
        .await
        .unwrap();
    let c = client(&endpoint);
    assert_eq!(process_repl_once(&pool, &store, &c).await, (0, 1));
    let rows = outbox_rows(&pool).await;
    assert_eq!(rows[0].0, "dead", "SSE-KMS 必须是永久死信：{rows:?}");
    assert!(rows[0].3.as_deref().unwrap_or("").contains("SSE-KMS"));
    assert!(seen.lock().unwrap().is_empty(), "不得向目标发任何请求");
}
