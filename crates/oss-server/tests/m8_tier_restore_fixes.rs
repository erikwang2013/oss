//! M8 S4b 复核补测：SSE 拒绝、MPU 源归档、事件落库、过期行读路径清理、恢复大小上限。

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::events::EventSink;
use xl_s3::lifecycle::LifecycleRuleSpec;
use xl_s3::service::OssS3;
use xl_store::crypto::EncSpec;
use xl_store::meta::TierPhase;

use oss_server::config::TierTarget;
use oss_server::jobs::TaskExecutor as _;
use oss_server::state::AppState;
use oss_server::tier::{S3TierGc, TierBackend, TierBackends, tier_key};
use oss_server::tier_jobs::{M8TaskExecutor, sweep_transitions};

#[derive(Default)]
struct MockTier {
    objs: Mutex<HashMap<String, Bytes>>,
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
        self.objs.lock().unwrap().remove(key);
        Ok(())
    }
}

fn tier_target() -> TierTarget {
    TierTarget {
        id: "cold".into(),
        endpoint: "http://127.0.0.1:9".into(),
        bucket: "tb".into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
        server_name: None,
    }
}

async fn setup() -> (AppState, Arc<MockTier>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut st = common::state_with_tiers(dir.path(), vec![tier_target()]).await;
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

fn payload(bucket: &str, key: &str, vid: &str, class: &str, tier: Option<&str>) -> String {
    serde_json::json!({
        "bucket": bucket, "key": key, "version_id": vid, "class": class, "tier": tier
    })
    .to_string()
}

fn restore_payload(vid: &str, days: u32) -> String {
    serde_json::json!({
        "bucket": "bkt", "key": "ev.txt", "version_id": vid, "days": days
    })
    .to_string()
}

fn stream(b: &'static [u8]) -> xl_store::store::ByteStream {
    Box::pin(futures::stream::once(
        async move { Ok(Bytes::from_static(b)) },
    ))
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

async fn phase(st: &AppState, key: &str, vid: &str) -> TierPhase {
    st.store
        .tier_version("bkt", key, vid)
        .await
        .unwrap()
        .unwrap()
        .tier
        .unwrap()
        .phase
}

/// SSE 对象：执行器拒绝，sweep 也不选中。
#[tokio::test]
async fn sse_object_rejected_and_sweep_skips() {
    let (st, mock, _d) = setup().await;
    let info = st
        .store
        .put_object_stream_with_meta(
            "bkt",
            "sse.txt",
            stream(b"secret"),
            None,
            Default::default(),
            None,
            false,
            Some(EncSpec::SseC(xl_store::crypto::SseCKey::from_key(
                [11u8; 32],
            ))),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let vid = info.version_id.unwrap();

    let err = executor(&st)
        .execute(
            "Transition",
            &payload("bkt", "sse.txt", &vid, "GLACIER", Some("cold")),
        )
        .await
        .unwrap_err();
    assert!(err.contains("SSE"), "{err}");
    assert!(mock.objs.lock().unwrap().is_empty(), "SSE 对象不得上传远端");
    assert!(
        st.store
            .tier_version("bkt", "sse.txt", &vid)
            .await
            .unwrap()
            .unwrap()
            .tier
            .is_none()
    );

    st.lifecycle
        .put_rules(
            "bkt",
            &[LifecycleRuleSpec {
                rule_id: Some("t1".into()),
                prefix: String::new(),
                expiration_days: None,
                transition_days: Some(0),
                transition_class: Some("STANDARD_IA".into()),
                transition_tier: None,
                enabled: true,
            }],
        )
        .await
        .unwrap();
    let queued = sweep_transitions(&st.db, &st.store, st.lifecycle.as_ref())
        .await
        .unwrap();
    assert_eq!(queued, 0, "SSE 对象不得被 sweep 选中");
    assert!(st.db.list_jobs(10).await.unwrap().is_empty());
}

/// MPU 源：multipart ETag 与远端 ETag 不同也不影响归档/恢复；ETag 原样保留。
#[tokio::test]
async fn mpu_source_transition_keeps_multipart_etag() {
    let (st, mock, _d) = setup().await;
    let up = st
        .store
        .create_multipart_upload("bkt", "mp.bin", None, Default::default())
        .await
        .unwrap();
    let e1 = st
        .store
        .upload_part("bkt", "mp.bin", &up, 1, stream(b"part-one"))
        .await
        .unwrap();
    let info = st
        .store
        .complete_multipart_upload("bkt", "mp.bin", &up, &[(1, e1)])
        .await
        .unwrap();
    assert!(info.etag.contains('-'), "MPU ETag 应含 -N：{}", info.etag);
    let vid = info.version_id.clone().unwrap();

    assert_eq!(
        executor(&st)
            .execute(
                "Transition",
                &payload("bkt", "mp.bin", &vid, "GLACIER", Some("cold"))
            )
            .await
            .unwrap(),
        "archived"
    );
    let remote = tier_key("cold", "bkt", &vid, "mp.bin");
    assert_eq!(
        mock.objs.lock().unwrap().get(&remote).unwrap().as_ref(),
        b"part-one"
    );
    let v = st
        .store
        .tier_version("bkt", "mp.bin", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.etag, info.etag, "源 multipart ETag 不得被远端 ETag 改写");
    assert!(v.tier.as_ref().unwrap().etag.starts_with("mock-"));

    let p = serde_json::json!({
        "bucket": "bkt", "key": "mp.bin", "version_id": vid, "days": 1
    })
    .to_string();
    assert_eq!(
        executor(&st).execute("RestoreObject", &p).await.unwrap(),
        "restored"
    );
    let v = st
        .store
        .tier_version("bkt", "mp.bin", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.etag, info.etag, "恢复后 ETag 仍为 multipart ETag");
    let (_, bytes) = st.store.get_object("bkt", "mp.bin").await.unwrap();
    assert_eq!(&bytes[..], b"part-one");
}

/// 三个事件真实落到 event_outbox（不只构造枚举）。
#[tokio::test]
async fn tier_events_reach_outbox() {
    let (st, _mock, _d) = setup().await;
    sqlx::query("INSERT INTO event_configs (bucket, config_json) VALUES ('bkt', ?)")
        .bind(
            r#"[{"id":"t","target":"tier","events":["s3:LifecycleTransition","s3:ObjectRestore:Post","s3:ObjectRestore:Completed"],"prefix":null,"suffix":null}]"#,
        )
        .execute(st.db.pool())
        .await
        .unwrap();
    let vid = archive(&st, "ev.txt", b"event-data").await;

    let s3 = service(&st).with_events(Some(Arc::clone(&st.events) as Arc<dyn EventSink>));
    let resp = s3
        .restore_object(req(RestoreObjectInput {
            bucket: "bkt".into(),
            key: "ev.txt".into(),
            version_id: Some(vid.clone()),
            restore_request: Some(RestoreRequest {
                days: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(resp.status, Some(axum::http::StatusCode::ACCEPTED));
    assert_eq!(
        executor(&st)
            .execute("RestoreObject", &restore_payload(&vid, 1))
            .await
            .unwrap(),
        "restored"
    );

    // Transition/Completed 由 spawn 异步写库：轮询至三条事件齐（高负载下放宽到 ~15s）。
    let mut names: Vec<String> = Vec::new();
    for _ in 0..600 {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT event_json FROM event_outbox WHERE bucket='bkt' ORDER BY id",
        )
        .fetch_all(st.db.pool())
        .await
        .unwrap();
        names = rows
            .iter()
            .filter_map(|j| {
                serde_json::from_str::<serde_json::Value>(j)
                    .ok()
                    .and_then(|v| v["eventName"].as_str().map(str::to_string))
            })
            .collect();
        if names.len() >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    names.sort();
    assert_eq!(
        names,
        [
            "s3:LifecycleTransition",
            "s3:ObjectRestore:Completed",
            "s3:ObjectRestore:Post"
        ],
        "事件名必须精确落库"
    );
}

/// 读/HEAD 发现恢复已过期：回收本地副本并 best-effort 删除 tier_restores 行。
#[tokio::test]
async fn head_on_expired_restore_drops_row() {
    let (st, _mock, _d) = setup().await;
    let vid = archive(&st, "exp.txt", b"expired").await;
    // days=0：行与 meta 的到期时刻都是「现在」，无需等待。
    st.restore_sink
        .enqueue_restore("bkt", "exp.txt", &vid, 0)
        .await
        .unwrap();
    let task: String = sqlx::query_scalar(
        "SELECT payload_json FROM job_tasks WHERE action = 'RestoreObject' \
         ORDER BY job_id DESC LIMIT 1",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(
        executor(&st).execute("RestoreObject", &task).await.unwrap(),
        "restored"
    );
    assert!(
        st.db
            .restore_row("bkt", "exp.txt", &vid)
            .await
            .unwrap()
            .is_some()
    );

    let head = service(&st)
        .head_object(req(HeadObjectInput {
            bucket: "bkt".into(),
            key: "exp.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(head.status, None, "HEAD 元数据仍可读");
    assert!(
        st.db
            .restore_row("bkt", "exp.txt", &vid)
            .await
            .unwrap()
            .is_none(),
        "读路径发现过期后应删除 tier_restores 行"
    );
    assert_eq!(phase(&st, "exp.txt", &vid).await, TierPhase::Archived);
}

/// 恢复大小上限：元数据先挡、远端 GET 回来再挡，失败都干净回到 Archived。
#[tokio::test]
async fn restore_respects_max_tier_bytes() {
    let (st, mock, _d) = setup().await;
    let vid = archive(&st, "big.bin", b"0123456789").await;
    let p = serde_json::json!({
        "bucket": "bkt", "key": "big.bin", "version_id": vid, "days": 1
    })
    .to_string();

    let err = executor(&st)
        .with_max_tier_bytes(4)
        .execute("RestoreObject", &p)
        .await
        .unwrap_err();
    assert!(err.contains("OSS_TIER_MAX_BYTES"), "{err}");
    assert_eq!(phase(&st, "big.bin", &vid).await, TierPhase::Archived);

    // 后端撒谎：GET 体积超过上限（meta 记录的 10 字节本身合法）→ 同样失败。
    let remote = tier_key("cold", "bkt", &vid, "big.bin");
    mock.objs
        .lock()
        .unwrap()
        .insert(remote, Bytes::from_static(b"01234567890"));
    let err = executor(&st)
        .with_max_tier_bytes(10)
        .execute("RestoreObject", &p)
        .await
        .unwrap_err();
    assert!(err.contains("恢复数据 11 字节超过"), "{err}");
    assert_eq!(phase(&st, "big.bin", &vid).await, TierPhase::Archived);
}
