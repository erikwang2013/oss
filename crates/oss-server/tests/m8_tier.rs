//! M8 S4b：tier 后端、Transition 归档、Restore 状态机/对账/reaper、远端 key 编码。

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use s3s::dto::*;
use s3s::{S3, S3ErrorCode, S3Request, S3Response};
use xl_s3::service::OssS3;
use xl_store::meta::TierPhase;

use oss_server::config::TierTarget;
use oss_server::jobs::TaskExecutor as _;
use oss_server::state::AppState;
use oss_server::tier::{S3TierGc, TierBackend, TierBackends, tier_key};
use oss_server::tier_jobs::{M8TaskExecutor, reap_once};

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
        endpoint: endpoint.into(),
        bucket: "tb".into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
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

async fn body_of(resp: S3Response<GetObjectOutput>) -> Vec<u8> {
    let mut body = resp.output.body.unwrap();
    let mut buf = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut body).await {
        buf.extend_from_slice(&chunk.unwrap());
    }
    buf
}

#[tokio::test]
async fn transition_restore_roundtrip_and_reaper() {
    let (st, mock, _d) = setup().await;
    let s3 = service(&st);
    let key = "dir/a b+c%2F中文.txt";
    let vid = archive(&st, key, b"hello tier").await;

    // TierBackend 边界收到 raw key（N3：编码只在 ReplClient canonical path）
    let remote = tier_key("cold", "bkt", &vid, key);
    assert_eq!(
        mock.objs.lock().unwrap().keys().next().unwrap(),
        &remote,
        "远端 key 必须保留 raw 对象键"
    );

    let v = st
        .store
        .tier_version("bkt", key, &vid)
        .await
        .unwrap()
        .unwrap();
    let t = v.tier.as_ref().unwrap();
    assert_eq!(v.storage_class.as_deref(), Some("GLACIER"));
    assert_eq!(t.phase, TierPhase::Archived);
    assert!(
        t.etag.starts_with("mock-"),
        "tier.etag 存远端 ETag，源 ETag 另存：{}",
        t.etag
    );

    // Archived：HEAD 可读元数据、无 x-amz-restore；GET 403
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(head.status, None);
    assert_eq!(head.output.content_length, Some(10));
    assert!(head.output.restore.is_none());
    let e = s3
        .get_object(req(GetObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidObjectState);

    // RestoreObject → 202 + 状态头；重复 → 409
    let resp = s3
        .restore_object(req(RestoreObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
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
        resp.headers.get("x-amz-restore").unwrap(),
        "ongoing-request=\"true\""
    );
    let row = st.db.restore_row("bkt", key, &vid).await.unwrap().unwrap();
    assert_eq!(row.state, "pending");
    let e = s3
        .restore_object(req(RestoreObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            version_id: Some(vid.clone()),
            restore_request: Some(RestoreRequest {
                days: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::RestoreAlreadyInProgress);

    // Restoring：HEAD 带 ongoing=true，GET 仍 403
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        head.output.restore.as_deref(),
        Some("ongoing-request=\"true\"")
    );
    assert_eq!(
        s3.get_object(req(GetObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap_err()
        .code(),
        &S3ErrorCode::InvalidObjectState
    );

    // 执行作业：prepare → row restored → meta commit
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
    let row = st.db.restore_row("bkt", key, &vid).await.unwrap().unwrap();
    assert_eq!(row.state, "restored");
    assert!(row.expires_at > 0);

    // Restored：GET 可读、ETag 不变；HEAD ongoing=false + expiry
    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let etag = got.output.e_tag.clone();
    assert_eq!(body_of(got).await, b"hello tier");
    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(head.output.e_tag, etag);
    let restore = head.output.restore.unwrap();
    assert!(
        restore.starts_with("ongoing-request=\"false\""),
        "{restore}"
    );
    assert!(restore.contains("expiry-date="), "{restore}");

    // 已 Restored 再发起 → 409（与 Restoring 同口径）
    let e = s3
        .restore_object(req(RestoreObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            version_id: Some(vid.clone()),
            restore_request: Some(RestoreRequest {
                days: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::RestoreAlreadyInProgress);

    // reaper 以 Version 为准：行过期但版本未到期 → 只修正行，不回收
    st.db
        .set_restore_state("bkt", key, &vid, "restored", 1)
        .await
        .unwrap();
    assert_eq!(reap_once(&st.db, &st.store).await.unwrap(), 0);
    let row = st.db.restore_row("bkt", key, &vid).await.unwrap().unwrap();
    assert!(row.expires_at > 1, "行按版本 expiry 修正：{row:?}");
    assert_eq!(
        st.store
            .tier_version("bkt", key, &vid)
            .await
            .unwrap()
            .unwrap()
            .tier
            .unwrap()
            .phase,
        TierPhase::Restored
    );

    // 版本到期（reaper 的回收动作）→ 本地副本回收，行删除
    st.store
        .expire_restore("bkt", key, &vid, i64::MAX / 2)
        .await
        .unwrap();
    st.db
        .set_restore_state("bkt", key, &vid, "restored", 1)
        .await
        .unwrap();
    assert_eq!(reap_once(&st.db, &st.store).await.unwrap(), 1);
    assert!(st.db.restore_row("bkt", key, &vid).await.unwrap().is_none());
    assert_eq!(
        s3.get_object(req(GetObjectInput {
            bucket: "bkt".into(),
            key: key.into(),
            ..Default::default()
        }))
        .await
        .unwrap_err()
        .code(),
        &S3ErrorCode::InvalidObjectState,
        "过期后 GET 一律 403"
    );
}

#[tokio::test]
async fn non_archive_class_and_version_delete_gc() {
    let (st, mock, _d) = setup().await;
    let info = st
        .store
        .put_object("bkt", "ia.txt", Bytes::from_static(b"ia"), None)
        .await
        .unwrap();
    let vid = info.version_id.unwrap();
    assert_eq!(
        executor(&st)
            .execute(
                "Transition",
                &payload("bkt", "ia.txt", &vid, "STANDARD_IA", None)
            )
            .await
            .unwrap(),
        "ok"
    );
    let v = st
        .store
        .tier_version("bkt", "ia.txt", &vid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.storage_class.as_deref(), Some("STANDARD_IA"));
    assert!(v.tier.is_none());
    assert!(mock.objs.lock().unwrap().is_empty(), "非归档类不搬数据");

    // 归档幂等；不同 class 拒绝
    let avid = archive(&st, "cold.txt", b"cold").await;
    assert_eq!(
        executor(&st)
            .execute(
                "Transition",
                &payload("bkt", "cold.txt", &avid, "GLACIER", Some("cold"))
            )
            .await
            .unwrap(),
        "archived"
    );
    let err = executor(&st)
        .execute(
            "Transition",
            &payload("bkt", "cold.txt", &avid, "DEEP_ARCHIVE", Some("cold")),
        )
        .await
        .unwrap_err();
    assert!(err.contains("不能转为"), "{err}");

    // 删除对象（非版本桶整键删）→ TierGcSink 远端 DELETE
    service(&st)
        .delete_object(req(DeleteObjectInput {
            bucket: "bkt".into(),
            key: "cold.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        mock.deleted.lock().unwrap().as_slice(),
        &[tier_key("cold", "bkt", &avid, "cold.txt")]
    );

    // 非归档对象 RestoreObject → 403
    let e = service(&st)
        .restore_object(req(RestoreObjectInput {
            bucket: "bkt".into(),
            key: "ia.txt".into(),
            version_id: Some(vid),
            restore_request: Some(RestoreRequest {
                days: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidObjectState);
}
