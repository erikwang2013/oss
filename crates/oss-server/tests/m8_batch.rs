//! M8 S3：Batch Operations——清单解析、五种动作、钩子、权限、报告、取消/暂停/重试。

mod common;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use xl_store::store::VersioningMode;

use bytes::Bytes;
use oss_server::db::{Db, JobSpec, NewJobTask, PermissionRow};
use oss_server::job_actions::{
    BATCH_COPY, BATCH_DELETE, BATCH_INVOKE, BATCH_RESTORE, BATCH_TAGS, BatchActionPayload,
    BatchError, BatchExecutor, BatchMutationHooks, parse_manifest,
};
use oss_server::job_actions_api::{BatchCreateRequest, TagsBody, create_batch};
use oss_server::jobs::{JobEnv, TaskExecutor, worker_once};
use oss_server::state::AppState;
use sha2::Digest as _;
use xl_s3::olap::OlapTransform;
use xl_s3::replication::{ReplicationSink, WriteInfo};
use xl_s3::restore::RestoreSink;
use xl_s3::tier_gc::TierGcSink;
use xl_store::crypto::{EncSpec, SseCKey};
use xl_store::meta::{ObjectTag, TierPhase};
use xl_store::store::{ArchiveSpec, Store, TierRef};

#[derive(Debug, PartialEq)]
struct ReplWrite {
    bucket: String,
    key: String,
    version_id: Option<String>,
    is_delete_marker: bool,
    is_version_delete: bool,
}

#[derive(Default)]
struct FakeRepl {
    writes: Mutex<Vec<ReplWrite>>,
}

#[async_trait::async_trait]
impl ReplicationSink for FakeRepl {
    async fn on_write(&self, info: WriteInfo<'_>) {
        self.writes.lock().unwrap().push(ReplWrite {
            bucket: info.bucket.to_string(),
            key: info.key.to_string(),
            version_id: info.version_id.map(str::to_string),
            is_delete_marker: info.is_delete_marker,
            is_version_delete: info.is_version_delete,
        });
    }
}

#[derive(Default)]
struct FakeGc {
    refs: Mutex<Vec<(String, String)>>,
}

#[async_trait::async_trait]
impl TierGcSink for FakeGc {
    async fn delete(&self, refs: &[TierRef]) -> Result<(), String> {
        self.refs
            .lock()
            .unwrap()
            .extend(refs.iter().map(|r| (r.backend.clone(), r.key.clone())));
        Ok(())
    }
}

#[derive(Default)]
struct FakeRestore {
    calls: Mutex<Vec<(String, String, String, u32)>>,
}

#[async_trait::async_trait]
impl RestoreSink for FakeRestore {
    async fn enqueue_restore(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        days: u32,
    ) -> Result<i64, String> {
        self.calls.lock().unwrap().push((
            bucket.to_string(),
            key.to_string(),
            version_id.to_string(),
            days,
        ));
        Ok(1)
    }

    async fn forget_restore(&self, _b: &str, _k: &str, _v: &str) -> Result<(), String> {
        Ok(())
    }
}

struct Fx {
    st: AppState,
    _dir: tempfile::TempDir,
    admin: i64,
    repl: Arc<FakeRepl>,
    gc: Arc<FakeGc>,
    restore: Arc<FakeRestore>,
}

async fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    let admin = st.db.create_user("admin", "pw", true).await.unwrap();
    for b in ["src", "dst", "reports"] {
        st.store.create_bucket(b).await.unwrap();
        st.db.insert_bucket(b, Some(admin)).await.unwrap();
    }
    Fx {
        st,
        _dir: dir,
        admin,
        repl: Arc::new(FakeRepl::default()),
        gc: Arc::new(FakeGc::default()),
        restore: Arc::new(FakeRestore::default()),
    }
}

impl Fx {
    fn db(&self) -> &Db {
        &self.st.db
    }

    fn store(&self) -> &Store {
        &self.st.store
    }

    fn exec(&self) -> BatchExecutor {
        BatchExecutor::new(
            Arc::clone(&self.st.db),
            Arc::clone(&self.st.store),
            self.st.acl(),
            Arc::clone(&self.restore) as Arc<dyn RestoreSink>,
            Arc::clone(&self.st.olap_cache),
            Arc::clone(&self.st.events) as Arc<dyn xl_s3::events::EventSink>,
            BatchMutationHooks {
                repl: Arc::clone(&self.repl) as Arc<dyn ReplicationSink>,
                tier_gc: Arc::clone(&self.gc) as Arc<dyn TierGcSink>,
                sync_delete: Arc::clone(&self.st.sync_delete)
                    as Arc<dyn xl_s3::sync_delete::SyncDeleteSink>,
            },
        )
    }

    async fn put(&self, bucket: &str, key: &str, body: &'static [u8]) {
        self.st
            .store
            .put_object(bucket, key, Bytes::from_static(body), None)
            .await
            .unwrap();
    }

    async fn run(&self, action: &str, p: &BatchActionPayload) -> Result<String, String> {
        let payload = serde_json::to_string(p).unwrap();
        self.exec().execute(action, &payload).await
    }

    /// 桶通知配置（SqlEventSink 直接读 `event_configs`，不经 S3 路由）。
    async fn notify(&self, bucket: &str, events: &[&str]) {
        let cfg = serde_json::json!([{
            "id": format!("cfg-{bucket}"),
            "target": format!("t-{bucket}"),
            "events": events,
            "prefix": null,
            "suffix": null,
        }]);
        self.st
            .acl()
            .set_bucket_notification(bucket, &cfg.to_string())
            .await
            .unwrap();
    }

    async fn clear_outbox(&self) {
        sqlx::query("DELETE FROM event_outbox")
            .execute(self.db().pool())
            .await
            .unwrap();
    }

    /// outbox 行：(bucket, target, event JSON)，按写入序（worker 未启动，行不会被消费）。
    async fn outbox(&self) -> Vec<(String, String, serde_json::Value)> {
        use sqlx::Row as _;
        let rows =
            sqlx::query("SELECT bucket, target_id, event_json FROM event_outbox ORDER BY id")
                .fetch_all(self.db().pool())
                .await
                .unwrap();
        rows.iter()
            .map(|r| {
                (
                    r.get::<String, _>("bucket"),
                    r.get::<String, _>("target_id"),
                    serde_json::from_str(r.get("event_json")).unwrap(),
                )
            })
            .collect()
    }
}

fn payload(bucket: &str, key: &str, uid: i64) -> BatchActionPayload {
    BatchActionPayload {
        bucket: bucket.into(),
        key: key.into(),
        uid,
        ..Default::default()
    }
}

fn wall_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn env(max_attempts: i64) -> JobEnv {
    JobEnv {
        workers: 1,
        interval_secs: 1,
        max_attempts,
        timeout_secs: 30,
        tier_timeout_secs: 30,
    }
}

/// 从墙钟开始跑空队列（create_batch 的 next_at 是墙钟；每 tick +1）。
async fn drain(exec: &dyn TaskExecutor, db: &Db, store: &Store, max_attempts: i64) {
    let mut now = wall_now();
    while worker_once(db, store, exec, env(max_attempts), now)
        .await
        .unwrap()
    {
        now += 1;
    }
}

fn req(op: &str, manifest: &str) -> BatchCreateRequest {
    BatchCreateRequest {
        kind: Some("batch".into()),
        manifest_bucket: "src".into(),
        manifest_key: manifest.into(),
        operation: op.into(),
        target: None,
        tags: None,
        restore: None,
        transform_access_point: None,
        report_bucket: None,
        report_prefix: None,
        priority: 0,
    }
}

fn tags(pairs: &[(&str, &str)]) -> Option<TagsBody> {
    Some(TagsBody::Map(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    ))
}

fn assert_iso8601(s: &str) {
    let b = s.as_bytes();
    assert_eq!(b.len(), 20, "期望秒级 ISO8601: {s}");
    for (i, c) in [4, 7, 10, 13, 16].iter().zip(['-', '-', 'T', ':', ':']) {
        assert_eq!(b[*i], c as u8, "位置 {i}: {s}");
    }
    assert_eq!(b[19], b'Z');
}

#[test]
fn manifest_csv_tolerance_and_errors() {
    // BOM + CRLF + 表头 + 引号字段 + 空行
    let csv = b"\xEF\xBB\xBFbucket,key,versionId\r\nsrc,\"a,b\",v\"1\"\r\n\r\nsrc,c,\r\nsrc,d\r\n";
    let rows = parse_manifest(csv).unwrap();
    assert_eq!(
        rows,
        vec![
            oss_server::job_actions::ManifestRow {
                bucket: "src".into(),
                key: "a,b".into(),
                version_id: Some("v\"1\"".into()),
            },
            oss_server::job_actions::ManifestRow {
                bucket: "src".into(),
                key: "c".into(),
                version_id: None,
            },
            oss_server::job_actions::ManifestRow {
                bucket: "src".into(),
                key: "d".into(),
                version_id: None,
            },
        ]
    );
    assert!(matches!(
        parse_manifest(b"src,a,x,y\n"),
        Err(BatchError::Invalid(_))
    ));
    assert!(matches!(
        parse_manifest(b"src,\n"),
        Err(BatchError::Invalid(_))
    ));
    assert!(matches!(
        parse_manifest(b"src,../escape\n"),
        Err(BatchError::Invalid(_))
    ));
    assert!(matches!(parse_manifest(b""), Err(BatchError::Invalid(_))));
    // 行数上限：10001 行拒绝
    let mut big = String::new();
    for i in 0..=10_000 {
        big.push_str(&format!("src,k{i}\n"));
    }
    assert!(matches!(
        parse_manifest(big.as_bytes()),
        Err(BatchError::Invalid(_))
    ));
}

#[tokio::test]
async fn batch_1000_replace_tags_failure_retry_and_report() {
    let f = fx().await;
    for i in 0..1000 {
        f.put("src", &format!("obj/{i}"), b"x").await;
    }
    let mut manifest = String::from("bucket,key\r\n");
    for i in 0..1000 {
        manifest.push_str(&format!("src,obj/{i}\r\n"));
    }
    manifest.push_str("src,missing-key\r\n");
    f.put("src", "manifest.csv", b"placeholder").await;
    f.st.store
        .put_object(
            "src",
            "manifest.csv",
            Bytes::from(manifest.into_bytes()),
            Some("text/csv".into()),
        )
        .await
        .unwrap();

    let mut r = req("ReplaceTags", "manifest.csv");
    r.tags = tags(&[("batch", "1")]);
    r.report_bucket = Some("reports".into());
    r.report_prefix = Some("out".into());
    let created = create_batch(&f.st, f.admin, true, r).await.unwrap();
    assert_eq!(created.total, 1001);
    // I3：Batch 创建落控制面审计（allow；目标是清单对象）
    let (op, bucket, key, decision, detail): (
        String,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT op, bucket, object_key, decision, detail FROM audit_log \
         WHERE op='control:job-create'",
    )
    .fetch_one(f.db().pool())
    .await
    .unwrap();
    assert_eq!(op, "control:job-create");
    assert_eq!(bucket.as_deref(), Some("src"));
    assert_eq!(key.as_deref(), Some("manifest.csv"));
    assert_eq!(decision, "allow");
    assert!(
        detail
            .as_deref()
            .unwrap_or("")
            .contains(&format!("job={}", created.job_id)),
        "detail={detail:?}"
    );
    // 快照：改清单不影响已建任务
    f.put("src", "manifest.csv", b"src,only\r\n").await;
    assert_eq!(
        f.db().job_get(created.job_id).await.unwrap().unwrap()["total"],
        1001
    );

    drain(&f.exec(), f.db(), f.store(), 1).await;
    let job = f.db().job_get(created.job_id).await.unwrap().unwrap();
    assert_eq!(job["state"], "error");
    assert_eq!(job["total"], 1001);
    assert_eq!(job["succeeded"], 1000);
    assert_eq!(job["failed"], 1);

    let path = format!("out/{}/results.csv", created.job_id);
    let (_, bytes) = f.store().get_object("reports", &path).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    let mut rdr = csv::Reader::from_reader(text.as_bytes());
    let head: Vec<String> = rdr.headers().unwrap().iter().map(str::to_string).collect();
    assert_eq!(
        head,
        [
            "taskId",
            "bucket",
            "key",
            "versionId",
            "resultCode",
            "resultString",
            "startTime",
            "endTime"
        ]
    );
    let rows: Vec<csv::StringRecord> = rdr.records().map(|r| r.unwrap()).collect();
    assert_eq!(rows.len(), 1001, "失败任务也在报告里（已执行）");
    let mut ids = HashSet::new();
    let (mut ok, mut bad) = (0, 0);
    for row in &rows {
        assert!(
            ids.insert(row[0].to_string()),
            "taskId 必须唯一: {}",
            &row[0]
        );
        assert_eq!(row.get(1), Some("src"));
        if row.get(4) == Some("Succeeded") {
            ok += 1;
        } else if row.get(4) == Some("PermanentFailure") {
            bad += 1;
            assert_eq!(row.get(2), Some("missing-key"));
            assert!(row.get(5).is_some_and(|s| !s.is_empty()));
        }
        assert_iso8601(&row[6]);
        assert_iso8601(&row[7]);
    }
    assert_eq!((ok, bad), (1000, 1));

    // retry-failed：子作业只含失败任务，自己的报告目录；父报告不变
    let child = f
        .db()
        .job_retry_failed(created.job_id, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.db().job_get(child).await.unwrap().unwrap()["total"], 1);
    f.put("src", "missing-key", b"back").await;
    drain(&f.exec(), f.db(), f.store(), 1).await;
    let cj = f.db().job_get(child).await.unwrap().unwrap();
    assert_eq!(cj["state"], "done");
    assert_eq!(cj["succeeded"], 1);
    let (_, child_bytes) = f
        .store()
        .get_object("reports", &format!("out/{child}/results.csv"))
        .await
        .unwrap();
    let child_text = String::from_utf8(child_bytes.to_vec()).unwrap();
    assert_eq!(child_text.lines().count(), 2, "表头 + 1 行");
    let mut crdr = csv::Reader::from_reader(child_text.as_bytes());
    let child_rows: Vec<csv::StringRecord> = crdr.records().map(|r| r.unwrap()).collect();
    assert_eq!(child_rows[0].get(2), Some("missing-key"));
    assert_eq!(child_rows[0].get(4), Some("Succeeded"));
    let (_, parent_after) = f.store().get_object("reports", &path).await.unwrap();
    assert_eq!(parent_after, bytes, "父报告不得被子作业改写");
}

#[tokio::test]
async fn five_actions_and_hooks() {
    let f = fx().await;
    // ---- Copy：标签/元数据按 Copy 语义，写后复制入队 ----
    f.st.store
        .put_object_stream_with_meta(
            "src",
            "a",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"hello"))
            })),
            Some("text/plain".into()),
            [("meta".to_string(), "yes".to_string())].into(),
            None,
            VersioningMode::Off,
            None,
            None,
            Some(vec![ObjectTag {
                key: "t".into(),
                value: "v".into(),
            }]),
            None,
        )
        .await
        .unwrap();
    let mut cp = payload("src", "a", f.admin);
    cp.target_bucket = Some("dst".into());
    cp.target_prefix = Some("copies/".into());
    assert_eq!(f.run(BATCH_COPY, &cp).await.unwrap(), "Succeeded");
    let (info, body) = f.store().get_object("dst", "copies/a").await.unwrap();
    assert_eq!(body, Bytes::from_static(b"hello"));
    assert_eq!(info.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        info.user_metadata.get("meta").map(String::as_str),
        Some("yes")
    );
    let (_, got_tags) = f
        .store()
        .get_object_tags("dst", "copies/a", None)
        .await
        .unwrap();
    assert_eq!(
        got_tags,
        vec![ObjectTag {
            key: "t".into(),
            value: "v".into()
        }]
    );
    {
        let writes = f.repl.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(
            (writes[0].bucket.as_str(), writes[0].key.as_str()),
            ("dst", "copies/a")
        );
        assert!(!writes[0].is_delete_marker);
    }
    // 重复执行幂等（覆盖同一目标键）
    assert_eq!(f.run(BATCH_COPY, &cp).await.unwrap(), "Succeeded");
    assert_eq!(
        f.store().get_object("dst", "copies/a").await.unwrap().1,
        Bytes::from_static(b"hello")
    );

    // ---- Delete ----
    f.put("src", "del", b"gone").await;
    let d = payload("src", "del", f.admin);
    assert_eq!(f.run(BATCH_DELETE, &d).await.unwrap(), "Succeeded");
    assert!(matches!(
        f.store().head_object("src", "del").await,
        Err(xl_store::error::Error::NoSuchKey(_))
    ));

    // ---- ReplaceTags：整体替换（两次执行不合并） ----
    f.put("src", "tag", b"x").await;
    let mut t = payload("src", "tag", f.admin);
    t.tags = vec![ObjectTag {
        key: "k".into(),
        value: "1".into(),
    }];
    assert_eq!(f.run(BATCH_TAGS, &t).await.unwrap(), "Succeeded");
    t.tags.push(ObjectTag {
        key: "z".into(),
        value: "9".into(),
    });
    assert_eq!(f.run(BATCH_TAGS, &t).await.unwrap(), "Succeeded");
    let (_, got) = f.store().get_object_tags("src", "tag", None).await.unwrap();
    assert_eq!(got.len(), 2);
    // 对象不存在 → Err（引擎收敛 PermanentFailure）
    let missing = payload("src", "nope", f.admin);
    assert!(f.run(BATCH_TAGS, &missing).await.is_err());

    // ---- Restore：走 sink；第二次（Restoring）幂等成功且不重复入队 ----
    f.put("src", "arch", b"cold-data").await;
    let info = f.store().head_object("src", "arch").await.unwrap();
    f.store()
        .archive_version(
            "src",
            "arch",
            info.version_id.as_deref().unwrap_or("null"),
            ArchiveSpec {
                backend: "cold".into(),
                key: "tier-key".into(),
                size: info.size,
                source_etag: info.etag.clone(),
                etag: "remote".into(),
                sha256: "00".into(),
                class: "GLACIER".into(),
            },
            1,
        )
        .await
        .unwrap();
    let mut r = payload("src", "arch", f.admin);
    r.restore_days = Some(3);
    assert_eq!(f.run(BATCH_RESTORE, &r).await.unwrap(), "Succeeded");
    assert_eq!(f.restore.calls.lock().unwrap().len(), 1);
    assert_eq!(
        f.store()
            .tier_version("src", "arch", "null")
            .await
            .unwrap()
            .unwrap()
            .tier
            .unwrap()
            .phase,
        TierPhase::Restoring
    );
    assert_eq!(f.run(BATCH_RESTORE, &r).await.unwrap(), "Succeeded");
    assert_eq!(f.restore.calls.lock().unwrap().len(), 1, "已在恢复不再入队");

    // ---- InvokeLambda：POST 给 HTTP 变换器，输出丢弃 ----
    let hits = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let app = axum::Router::new()
        .route(
            "/t",
            axum::routing::post(
                |axum::extract::State(h): axum::extract::State<Arc<Mutex<Vec<Vec<u8>>>>>,
                 body: Bytes| async move {
                    h.lock().unwrap().push(body.to_vec());
                    "ok"
                },
            ),
        )
        .route(
            "/fail",
            axum::routing::post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .with_state(Arc::clone(&hits));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    f.db()
        .create_access_point("ap1", "src", f.admin, None, 1)
        .await
        .unwrap();
    f.db()
        .set_olap_config(
            "ap1",
            "http",
            Some(&format!("http://{addr}/t")),
            None,
            None,
            5_000,
            1 << 20,
            1,
        )
        .await
        .unwrap();
    // 固定 allow_private，避免测试进程级环境变量。
    let transform = |url: &str| {
        OlapTransform::prepare_with_allow_private(
            "http",
            Some(url.to_string()),
            None,
            5_000,
            1 << 20,
            Some(true),
        )
        .unwrap()
    };
    f.st.olap_cache
        .put("ap1", "src", transform(&format!("http://{addr}/t")));
    f.put("src", "invoke", b"payload-bytes").await;
    let mut iv = payload("src", "invoke", f.admin);
    iv.transform_access_point = Some("ap1".into());
    assert_eq!(f.run(BATCH_INVOKE, &iv).await.unwrap(), "Succeeded");
    assert_eq!(
        hits.lock().unwrap().as_slice(),
        &[b"payload-bytes".to_vec()]
    );

    // 清单 versionId 生效：读旧版本而非最新
    let old = f
        .store()
        .put_object_stream_with_meta(
            "src",
            "invoke-ver",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"old-bytes"))
            })),
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
        .unwrap()
        .version_id
        .unwrap();
    f.store()
        .put_object_stream_with_meta(
            "src",
            "invoke-ver",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"new-bytes"))
            })),
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
    let mut iv = payload("src", "invoke-ver", f.admin);
    iv.transform_access_point = Some("ap1".into());
    iv.version_id = Some(old);
    assert_eq!(f.run(BATCH_INVOKE, &iv).await.unwrap(), "Succeeded");
    assert_eq!(hits.lock().unwrap().last().unwrap(), &b"old-bytes".to_vec());

    // 变换器非 2xx → Err（引擎按上限收敛）
    f.db()
        .create_access_point("ap-fail", "src", f.admin, None, 1)
        .await
        .unwrap();
    f.db()
        .set_olap_config(
            "ap-fail",
            "http",
            Some(&format!("http://{addr}/fail")),
            None,
            None,
            5_000,
            1 << 20,
            1,
        )
        .await
        .unwrap();
    f.st.olap_cache
        .put("ap-fail", "src", transform(&format!("http://{addr}/fail")));
    let mut iv = payload("src", "invoke", f.admin);
    iv.transform_access_point = Some("ap-fail".into());
    assert!(f.run(BATCH_INVOKE, &iv).await.is_err(), "500 必须失败");
}

#[cfg(feature = "olap-wasm")]
mod batch_wasm {
    use super::*;

    /// guest ABI 恒等变换：`memory` + `alloc`/`dealloc`/`transform`。
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

    /// 定长输出（"Z"，长度 1 ≠ 输入长度）：验证输出由 guest 指定区域读取。
    const FIXED: &str = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 8) "Z")
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "dealloc") (param i32 i32))
          (func (export "transform") (param i32 i32) (result i64)
            (i64.or (i64.shl (i64.const 8) (i64.const 32)) (i64.const 1))))
    "#;

    /// 死循环，由 fuel/epoch 终止。
    const LOOP: &str = r#"
        (module
          (memory (export "memory") 1)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "dealloc") (param i32 i32))
          (func (export "transform") (param i32 i32) (result i64)
            (loop $l (br $l))
            (i64.const 0)))
    "#;

    /// 可编译但缺 alloc/dealloc/transform 导出。
    const BAD_ABI: &str = r#"(module (memory (export "memory") 1))"#;

    async fn wasm_ap_with(f: &Fx, name: &str, wat: &str, timeout_ms: i64, max_response_bytes: i64) {
        f.db()
            .create_access_point(name, "src", f.admin, None, 1)
            .await
            .unwrap();
        f.db()
            .set_olap_config(
                name,
                "wasm",
                None,
                Some(wat.as_bytes()),
                None,
                timeout_ms,
                max_response_bytes,
                1,
            )
            .await
            .unwrap();
        let t = OlapTransform::prepare(
            "wasm",
            None,
            Some(wat.as_bytes()),
            timeout_ms,
            max_response_bytes,
        )
        .unwrap();
        f.st.olap_cache.put(name, "src", t);
    }

    async fn wasm_ap(f: &Fx, name: &str, max_response_bytes: i64) {
        wasm_ap_with(f, name, IDENTITY, 5_000, max_response_bytes).await;
    }

    /// 创建期接受 WASM AP；执行成功即变换跑通（输出丢弃）。
    #[tokio::test]
    async fn invoke_lambda_wasm_transform_succeeds() {
        let f = fx().await;
        wasm_ap(&f, "wasm-ap", 1 << 20).await;
        f.put("src", "m.csv", b"src,wasm-key\n").await;
        f.put("src", "wasm-key", b"wasm-input").await;
        let mut r = req("InvokeLambda", "m.csv");
        r.transform_access_point = Some("wasm-ap".into());
        assert!(create_batch(&f.st, f.admin, true, r).await.is_ok());

        let mut p = payload("src", "wasm-key", f.admin);
        p.transform_access_point = Some("wasm-ap".into());
        assert_eq!(f.run(BATCH_INVOKE, &p).await.unwrap(), "Succeeded");
    }

    /// 输出超过 max_response_bytes → 任务失败。
    #[tokio::test]
    async fn invoke_lambda_wasm_output_limit_fails() {
        let f = fx().await;
        wasm_ap(&f, "wasm-ap", 4).await;
        f.put("src", "wasm-key", b"wasm-input").await;
        let mut p = payload("src", "wasm-key", f.admin);
        p.transform_access_point = Some("wasm-ap".into());
        let e = f.run(BATCH_INVOKE, &p).await.unwrap_err();
        assert!(e.contains("超限"), "{e}");
    }

    /// 变换体走通（输出丢弃，但 guest 输出区域被读取）。
    #[tokio::test]
    async fn invoke_lambda_wasm_fixed_output_succeeds() {
        let f = fx().await;
        wasm_ap_with(&f, "wasm-ap", FIXED, 5_000, 1 << 20).await;
        f.put("src", "fixed-key", b"longer-than-one-byte").await;
        let mut p = payload("src", "fixed-key", f.admin);
        p.transform_access_point = Some("wasm-ap".into());
        assert_eq!(f.run(BATCH_INVOKE, &p).await.unwrap(), "Succeeded");
    }

    /// 死循环模块被 fuel/epoch 终止：任务失败且有界耗时（timeout 下限 1000ms）。
    #[tokio::test]
    async fn invoke_lambda_wasm_timeout_fails() {
        let f = fx().await;
        wasm_ap_with(&f, "wasm-ap", LOOP, 1_000, 1 << 20).await;
        f.put("src", "loop-key", b"x").await;
        let mut p = payload("src", "loop-key", f.admin);
        p.transform_access_point = Some("wasm-ap".into());
        let t0 = std::time::Instant::now();
        let e = f.run(BATCH_INVOKE, &p).await.unwrap_err();
        assert!(e.contains("wasm"), "{e}");
        assert!(
            t0.elapsed() < std::time::Duration::from_millis(2500),
            "{:?}",
            t0.elapsed()
        );
    }

    /// 坏模块：编译期拒绝；绕过保存塞入坏 ABI / 配置在但未加载时执行期 fail-closed。
    #[tokio::test]
    async fn invoke_lambda_wasm_bad_module_fails_closed() {
        let f = fx().await;
        let e =
            match OlapTransform::prepare("wasm", None, Some(b"not wasm".as_slice()), 5_000, 1024) {
                Ok(_) => panic!("坏模块应被拒"),
                Err(e) => e,
            };
        assert!(e.contains("wasm 编译失败"), "{e}");

        wasm_ap_with(&f, "bad-abi", BAD_ABI, 5_000, 1 << 20).await;
        f.put("src", "bad-key", b"x").await;
        let mut p = payload("src", "bad-key", f.admin);
        p.transform_access_point = Some("bad-abi".into());
        let e = f.run(BATCH_INVOKE, &p).await.unwrap_err();
        assert!(e.contains("wasm 缺"), "{e}");

        // 保存失败/预加载失败的配置不应静默直通或伪成功。
        f.db()
            .create_access_point("unloaded", "src", f.admin, None, 1)
            .await
            .unwrap();
        f.db()
            .set_olap_config(
                "unloaded",
                "wasm",
                None,
                Some(b"not wasm".as_slice()),
                None,
                5_000,
                1024,
                1,
            )
            .await
            .unwrap();
        let mut p = payload("src", "bad-key", f.admin);
        p.transform_access_point = Some("unloaded".into());
        let e = f.run(BATCH_INVOKE, &p).await.unwrap_err();
        assert!(e.contains("未加载"), "{e}");
    }
}

#[cfg(not(feature = "olap-wasm"))]
mod batch_wasm_disabled {
    use super::*;

    /// 默认构建：DB 里的 wasm 配置（可能由开启 feature 的构建写入）在创建/执行期
    /// 都给出明确的 feature 未开启错误，而不是“未加载”。
    #[tokio::test]
    async fn wasm_transform_reports_disabled_feature() {
        let f = fx().await;
        f.db()
            .create_access_point("wasm-ap", "src", f.admin, None, 1)
            .await
            .unwrap();
        f.db()
            .set_olap_config(
                "wasm-ap",
                "wasm",
                None,
                Some(b"wasm".as_slice()),
                None,
                5_000,
                1024,
                1,
            )
            .await
            .unwrap();
        f.put("src", "m.csv", b"src,wasm-key\n").await;
        f.put("src", "wasm-key", b"x").await;

        let mut r = req("InvokeLambda", "m.csv");
        r.transform_access_point = Some("wasm-ap".into());
        let e = match create_batch(&f.st, f.admin, true, r).await {
            Err(BatchError::Invalid(e)) => e,
            Err(e) => panic!("期望 Invalid，得到 {e:?}"),
            Ok(_) => panic!("默认构建应拒绝 wasm 变换器"),
        };
        assert!(e.contains("olap-wasm"), "{e}");

        let mut p = payload("src", "wasm-key", f.admin);
        p.transform_access_point = Some("wasm-ap".into());
        let e = f.run(BATCH_INVOKE, &p).await.unwrap_err();
        assert!(e.contains("olap-wasm"), "{e}");
    }
}

/// I1：Batch Delete 产生 delete marker 时必须入复制队列（与在线 delete_object 同语义）；
/// 指定版本删除只入站点对队列（4.16；本夹具无站点，故观测量为 0）。
#[tokio::test]
async fn delete_marker_is_enqueued_for_replication() {
    let f = fx().await;
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = 'src'")
        .execute(f.db().pool())
        .await
        .unwrap();

    // 无版本删除 → marker 入队（is_delete_marker=true，带 marker 版本号）
    f.put("src", "marker", b"x").await;
    let d = payload("src", "marker", f.admin);
    assert_eq!(f.run(BATCH_DELETE, &d).await.unwrap(), "Succeeded");
    {
        let writes = f.repl.writes.lock().unwrap();
        assert_eq!(writes.len(), 1, "marker 必须入复制队列");
        assert_eq!(
            (writes[0].bucket.as_str(), writes[0].key.as_str()),
            ("src", "marker")
        );
        assert!(writes[0].is_delete_marker);
        assert!(!writes[0].is_version_delete);
        assert!(writes[0].version_id.is_some(), "marker 版本号必须带上");
    }

    // 指定版本删除 → M6 规则队列不入队（站点对另有队列，本夹具无站点）
    f.put("src", "ver", b"y").await;
    let vid = f
        .store()
        .head_object("src", "ver")
        .await
        .unwrap()
        .version_id;
    let mut vd = payload("src", "ver", f.admin);
    vd.version_id = Some(vid.unwrap_or_else(|| "null".into()));
    assert_eq!(f.run(BATCH_DELETE, &vd).await.unwrap(), "Succeeded");
    {
        let writes = f.repl.writes.lock().unwrap();
        assert_eq!(writes.len(), 2, "指定版本删除也入复制队列（4.16）");
        assert!(!writes[1].is_delete_marker);
        assert!(writes[1].is_version_delete, "必须是版本删除而非 marker");
    }
}

/// I3：控制面审计失败只 warn，不改变动作结果。
#[tokio::test]
async fn audit_failure_does_not_block_batch_create() {
    let f = fx().await;
    f.put("src", "m.csv", b"src,a\n").await;
    sqlx::query("DROP TABLE audit_log")
        .execute(f.db().pool())
        .await
        .unwrap();
    let mut r = req("ReplaceTags", "m.csv");
    r.tags = tags(&[("k", "v")]);
    let created = create_batch(&f.st, f.admin, true, r)
        .await
        .expect("审计失败不得阻断建作业");
    assert_eq!(created.total, 1);
}

/// M8 缺口 C：Batch Copy 必须套目标桶默认加密——HEAD 可见 SSE-S3 记录且可解密读回。
#[tokio::test]
async fn copy_applies_bucket_default_encryption() {
    let f = fx().await;
    f.st.store
        .init_encryption(Some(f._dir.path().join("master.key")), None)
        .await
        .unwrap();
    f.put("src", "secret", b"top-secret").await;
    f.db()
        .set_bucket_default_encryption("dst", "AES256")
        .await
        .unwrap();
    let mut cp = payload("src", "secret", f.admin);
    cp.target_bucket = Some("dst".into());
    assert_eq!(f.run(BATCH_COPY, &cp).await.unwrap(), "Succeeded");
    let info = f.store().head_object("dst", "secret").await.unwrap();
    let rec = info.encryption.expect("Batch Copy 目标应套用桶默认加密");
    assert_eq!(rec.key_id, "file", "AES256 默认 → SSE-S3（file provider）");
    assert_eq!(rec.sse_c_key_md5, None);
    let (_, body) = f.store().get_object("dst", "secret").await.unwrap();
    assert_eq!(
        body,
        Bytes::from_static(b"top-secret"),
        "加密对象应可解密读回"
    );
}

#[tokio::test]
async fn archived_and_sse_c_sources_are_rejected() {
    let f = fx().await;
    // 归档源 Copy → Err（InvalidObjectState；引擎按上限转 PermanentFailure）
    f.put("src", "cold", b"cold").await;
    let info = f.store().head_object("src", "cold").await.unwrap();
    f.store()
        .archive_version(
            "src",
            "cold",
            info.version_id.as_deref().unwrap_or("null"),
            ArchiveSpec {
                backend: "cold".into(),
                key: "k".into(),
                size: info.size,
                source_etag: info.etag.clone(),
                etag: "r".into(),
                sha256: "0".into(),
                class: "GLACIER".into(),
            },
            1,
        )
        .await
        .unwrap();
    let mut cp = payload("src", "cold", f.admin);
    cp.target_bucket = Some("dst".into());
    let err = f.run(BATCH_COPY, &cp).await.unwrap_err();
    assert!(
        err.contains("invalid object state") || err.contains("not readable"),
        "{err}"
    );

    // SSE-C 源 Copy → Err
    let sse = SseCKey::from_key([7u8; 32]);
    f.st.store
        .put_object_stream_with_meta(
            "src",
            "secret",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"secret"))
            })),
            None,
            Default::default(),
            None,
            VersioningMode::Off,
            Some(EncSpec::SseC(sse)),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let mut sc = payload("src", "secret", f.admin);
    sc.target_bucket = Some("dst".into());
    assert!(f.run(BATCH_COPY, &sc).await.is_err());

    // 归档 Delete → TierRef 交 GC
    let d = payload("src", "cold", f.admin);
    assert_eq!(f.run(BATCH_DELETE, &d).await.unwrap(), "Succeeded");
    assert_eq!(
        f.gc.refs.lock().unwrap().as_slice(),
        &[("cold".to_string(), "k".to_string())]
    );
}

#[tokio::test]
async fn versioned_source_copy_and_tag() {
    let f = fx().await;
    let v1 = f
        .store()
        .put_object_stream_with_meta(
            "src",
            "doc",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"one"))
            })),
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
        .unwrap()
        .version_id
        .unwrap();
    f.st.store
        .put_object_stream_with_meta(
            "src",
            "doc",
            Box::pin(futures::stream::once(async {
                Ok(Bytes::from_static(b"two"))
            })),
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
    let mut cp = payload("src", "doc", f.admin);
    cp.version_id = Some(v1.clone());
    cp.target_bucket = Some("dst".into());
    assert_eq!(f.run(BATCH_COPY, &cp).await.unwrap(), "Succeeded");
    assert_eq!(
        f.store().get_object("dst", "doc").await.unwrap().1,
        Bytes::from_static(b"one"),
        "必须读指定版本，而非最新版本"
    );

    let mut t = payload("src", "doc", f.admin);
    t.version_id = Some(v1.clone());
    t.tags = vec![ObjectTag {
        key: "old".into(),
        value: "1".into(),
    }];
    assert_eq!(f.run(BATCH_TAGS, &t).await.unwrap(), "Succeeded");
    let (_, old_tags) = f
        .store()
        .get_object_tags("src", "doc", Some(&v1))
        .await
        .unwrap();
    assert_eq!(old_tags, t.tags);
    let (_, latest_tags) = f.store().get_object_tags("src", "doc", None).await.unwrap();
    assert!(latest_tags.is_empty(), "最新版本不得被改");

    // Delete + versionId：只删指定版本，最新版本仍可读
    let mut d = payload("src", "doc", f.admin);
    d.version_id = Some(v1.clone());
    assert_eq!(f.run(BATCH_DELETE, &d).await.unwrap(), "Succeeded");
    assert!(
        f.store()
            .get_object_version_ext("src", "doc", &v1, None)
            .await
            .is_err(),
        "指定版本应已删除"
    );
    assert_eq!(
        f.store().get_object("src", "doc").await.unwrap().1,
        Bytes::from_static(b"two")
    );
}

/// 回归：Restored 归档源的 Batch Copy 不得继承 GLACIER（否则目标无 tier，非法态）；
/// 目标应为 STANDARD 且可读。顺带覆盖 Restored 态再次 Restore 的幂等。
#[tokio::test]
async fn restored_archive_copy_is_standard_and_readable() {
    let f = fx().await;
    let data = b"thawed-bytes";
    let info = f
        .store()
        .put_object("src", "thaw", Bytes::from_static(data), None)
        .await
        .unwrap();
    let vid = info.version_id.clone().unwrap_or_else(|| "null".into());
    f.store()
        .archive_version(
            "src",
            "thaw",
            &vid,
            ArchiveSpec {
                backend: "cold".into(),
                key: "thaw-key".into(),
                size: info.size,
                source_etag: info.etag.clone(),
                etag: "remote".into(),
                sha256: hex::encode(sha2::Sha256::digest(data)),
                class: "GLACIER".into(),
            },
            1,
        )
        .await
        .unwrap();
    f.store()
        .mark_restore_pending("src", "thaw", &vid)
        .await
        .unwrap();
    f.store()
        .restore_prepare("src", "thaw", &vid, data)
        .await
        .unwrap();
    f.store()
        .restore_commit("src", "thaw", &vid, wall_now() + 3600)
        .await
        .unwrap();
    assert_eq!(
        f.store()
            .head_object("src", "thaw")
            .await
            .unwrap()
            .storage_class
            .as_deref(),
        Some("GLACIER"),
        "源确为归档类"
    );

    let mut cp = payload("src", "thaw", f.admin);
    cp.target_bucket = Some("dst".into());
    assert_eq!(f.run(BATCH_COPY, &cp).await.unwrap(), "Succeeded");
    let (dst, body) = f.store().get_object("dst", "thaw").await.unwrap();
    assert_eq!(body, Bytes::from_static(data), "目标内容可读");
    assert_eq!(dst.storage_class, None, "目标固定 STANDARD，不继承 GLACIER");
    let dst_vid = dst.version_id.clone().unwrap_or_else(|| "null".into());
    assert!(
        f.store()
            .tier_version("dst", "thaw", &dst_vid)
            .await
            .unwrap()
            .unwrap()
            .tier
            .is_none(),
        "目标不得带 tier 记录"
    );

    // Restored 态再 Restore：幂等成功且不重复入队
    let mut r = payload("src", "thaw", f.admin);
    r.restore_days = Some(1);
    assert_eq!(f.run(BATCH_RESTORE, &r).await.unwrap(), "Succeeded");
    assert!(f.restore.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn execution_reauthorization_is_permanent_and_isolated() {
    let f = fx().await;
    let nobody = f.db().create_user("nobody", "pw", false).await.unwrap();
    f.put("src", "ok", b"x").await;
    f.put("src", "denied", b"x").await;
    // 同作业两条任务：创建者身份不同 → 一条成功、一条执行前判权失败
    let spec = JobSpec {
        kind: "batch".into(),
        bucket: "src".into(),
        requested_by: Some(f.admin),
        report_bucket: Some("reports".into()),
        report_prefix: Some("re".into()),
        tasks: vec![
            NewJobTask {
                action: BATCH_TAGS.into(),
                payload: serde_json::to_string(&BatchActionPayload {
                    tags: vec![ObjectTag {
                        key: "k".into(),
                        value: "v".into(),
                    }],
                    ..payload("src", "ok", f.admin)
                })
                .unwrap(),
                dedupe_key: None,
            },
            NewJobTask {
                action: BATCH_TAGS.into(),
                payload: serde_json::to_string(&BatchActionPayload {
                    tags: vec![ObjectTag {
                        key: "k".into(),
                        value: "v".into(),
                    }],
                    ..payload("src", "denied", nobody)
                })
                .unwrap(),
                dedupe_key: None,
            },
        ],
        ..Default::default()
    };
    let id = f.db().create_job(&spec, 1).await.unwrap();
    drain(&f.exec(), f.db(), f.store(), 1).await;
    let job = f.db().job_get(id).await.unwrap().unwrap();
    assert_eq!(job["state"], "error");
    assert_eq!(job["succeeded"], 1);
    assert_eq!(job["failed"], 1, "单任务判权失败不得中断作业");
    let rows = f.db().job_report_rows(id).await.unwrap();
    assert_eq!(rows[1]["result_code"], "PermanentFailure");
    assert!(
        rows[1]["result_string"].as_str().unwrap().contains("无权"),
        "{:?}",
        rows[1]["result_string"]
    );

    // 创建者被停用 → 执行前拒绝（先补一个在册管理员，避免最后管理员保护）
    f.db().create_user("admin2", "pw", true).await.unwrap();
    f.db().set_user_status(f.admin, false).await.unwrap();
    assert!(
        f.run(BATCH_TAGS, &payload("src", "ok", f.admin))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn creation_validation_permissions_and_quota() {
    let f = fx().await;
    f.put("src", "m.csv", b"src,a\n").await;
    let other = f.db().create_user("other", "pw", false).await.unwrap();

    // 清单超 16 MiB：HEAD 阶段即拒（不把整块对象读进内存）
    f.store()
        .put_object(
            "src",
            "big.csv",
            Bytes::from(vec![b'a'; (16 << 20) + 1]),
            None,
        )
        .await
        .unwrap();
    let mut big = req("ReplaceTags", "big.csv");
    big.tags = tags(&[("k", "v")]);
    assert!(matches!(
        create_batch(&f.st, f.admin, true, big).await,
        Err(BatchError::Invalid(_))
    ));

    // 清单桶不可读 → 403
    let mut r = req("ReplaceTags", "m.csv");
    r.tags = tags(&[("k", "v")]);
    assert!(matches!(
        create_batch(&f.st, other, false, r).await,
        Err(BatchError::Denied(_))
    ));

    // 非管理员自有清单/对象，但报告桶无写权 → 403
    f.store().create_bucket("u2b").await.unwrap();
    f.db().insert_bucket("u2b", Some(other)).await.unwrap();
    f.st.store
        .put_object("u2b", "m.csv", Bytes::from_static(b"u2b,a\n\n"), None)
        .await
        .unwrap();
    f.put("u2b", "a", b"x").await;
    let mut own = BatchCreateRequest {
        manifest_bucket: "u2b".into(),
        manifest_key: "m.csv".into(),
        report_bucket: Some("reports".into()),
        report_prefix: Some("r".into()),
        ..req("ReplaceTags", "m.csv")
    };
    own.tags = tags(&[("k", "v")]);
    assert!(matches!(
        create_batch(&f.st, other, false, own).await,
        Err(BatchError::Denied(_))
    ));

    // 校验：非法标签（>10）、未知动作、InvokeLambda 缺 AP/缺变换器
    let mut bad_tags = req("ReplaceTags", "m.csv");
    bad_tags.tags = tags(&[("k0", "v")]);
    let mut body = match bad_tags.tags.take().unwrap() {
        TagsBody::Map(_) => std::collections::BTreeMap::new(),
        TagsBody::List(_) => unreachable!(),
    };
    for i in 0..11 {
        body.insert(format!("k{i}"), "v".to_string());
    }
    bad_tags.tags = Some(TagsBody::Map(body));
    assert!(matches!(
        create_batch(&f.st, f.admin, true, bad_tags).await,
        Err(BatchError::Invalid(_))
    ));
    assert!(matches!(
        create_batch(&f.st, f.admin, true, req("Frobnicate", "m.csv")).await,
        Err(BatchError::Invalid(_))
    ));
    assert!(matches!(
        create_batch(&f.st, f.admin, true, req("InvokeLambda", "m.csv")).await,
        Err(BatchError::Invalid(_))
    ));
    f.db()
        .create_access_point("no-transform", "src", f.admin, None, 1)
        .await
        .unwrap();
    let mut n = req("InvokeLambda", "m.csv");
    n.transform_access_point = Some("no-transform".into());
    assert!(matches!(
        create_batch(&f.st, f.admin, true, n).await,
        Err(BatchError::Invalid(_))
    ));

    // M-2：priority 用户可控 → 限定 -100..=100，越界 400（含极端值）。
    for p in [i64::MAX, i64::MIN, 101, -101] {
        let mut r = req("ReplaceTags", "m.csv");
        r.tags = tags(&[("k", "v")]);
        r.priority = p;
        assert!(
            matches!(
                create_batch(&f.st, f.admin, true, r).await,
                Err(BatchError::Invalid(_))
            ),
            "priority {p} 必须 400"
        );
    }

    // 配额：已有 100 个 in-flight 作业 → 429
    for i in 0..100 {
        f.db()
            .create_job(
                &JobSpec {
                    kind: "batch".into(),
                    bucket: "src".into(),
                    requested_by: Some(f.admin),
                    tasks: vec![NewJobTask {
                        action: "Noop".into(),
                        payload: "{}".into(),
                        dedupe_key: None,
                    }],
                    ..Default::default()
                },
                i,
            )
            .await
            .unwrap();
    }
    let mut q = req("ReplaceTags", "m.csv");
    q.tags = tags(&[("k", "v")]);
    assert!(matches!(
        create_batch(&f.st, f.admin, true, q).await,
        Err(BatchError::Quota(_))
    ));
}

/// 清单桶跨账户门（账户 = 用户命名空间 v1）：非管理员只能用**本账户**清单桶，
/// 即使被授予跨账户读权限也不放行；admin 可用跨账户；同账户但读权限被桶策略
/// 撤销/缺失时 fail-closed。
#[tokio::test]
async fn manifest_cross_account_authorization() {
    let f = fx().await;
    let alice = f.db().create_user("alice", "pw", false).await.unwrap();
    let bob = f.db().create_user("bob", "pw", false).await.unwrap();
    for (b, owner) in [("alice-b", alice), ("bob-b", bob), ("alice-deny", alice)] {
        f.store().create_bucket(b).await.unwrap();
        f.db().insert_bucket(b, Some(owner)).await.unwrap();
    }
    f.store()
        .put_object(
            "alice-b",
            "m.csv",
            Bytes::from_static(b"alice-b,k1\n"),
            None,
        )
        .await
        .unwrap();
    f.put("alice-b", "k1", b"x").await;
    f.store()
        .put_object("bob-b", "m.csv", Bytes::from_static(b"bob-b,k1\n"), None)
        .await
        .unwrap();
    f.put("bob-b", "k1", b"x").await;
    f.store()
        .put_object(
            "alice-deny",
            "m.csv",
            Bytes::from_static(b"alice-deny,k1\n"),
            None,
        )
        .await
        .unwrap();
    f.put("alice-deny", "k1", b"x").await;

    // 同账户（自有桶）→ 允许
    let mut own = BatchCreateRequest {
        manifest_bucket: "alice-b".into(),
        manifest_key: "m.csv".into(),
        ..req("ReplaceTags", "m.csv")
    };
    own.tags = tags(&[("batch", "1")]);
    assert_eq!(
        create_batch(&f.st, alice, false, own).await.unwrap().total,
        1
    );

    // 授予跨账户读+写（旧实现本可通过）→ 新账户门仍拒绝，错误须指明跨账户。
    f.db()
        .set_permissions(
            alice,
            &[PermissionRow {
                bucket: "bob-b".into(),
                read: true,
                write: true,
                delete: true,
            }],
        )
        .await
        .unwrap();
    let mut cross = BatchCreateRequest {
        manifest_bucket: "bob-b".into(),
        manifest_key: "m.csv".into(),
        ..req("ReplaceTags", "m.csv")
    };
    cross.tags = tags(&[("batch", "1")]);
    match create_batch(&f.st, alice, false, cross).await {
        Err(BatchError::Denied(m)) => assert!(m.contains("跨账户"), "{m}"),
        Err(e) => panic!("跨账户清单非管理员必须拒绝: {e:?}"),
        Ok(c) => panic!("跨账户清单非管理员不得建作业（job {}）", c.job_id),
    }

    // admin 跨账户 → 允许
    let mut cross = BatchCreateRequest {
        manifest_bucket: "bob-b".into(),
        manifest_key: "m.csv".into(),
        ..req("ReplaceTags", "m.csv")
    };
    cross.tags = tags(&[("batch", "1")]);
    assert_eq!(
        create_batch(&f.st, f.admin, true, cross)
            .await
            .unwrap()
            .total,
        1
    );

    // 同账户但清单读权限被桶策略显式 Deny（读权限撤销/缺失）→ fail-closed。
    let deny = serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject",
        "Resource":"arn:aws:s3:::alice-deny/*"}]})
    .to_string();
    f.db()
        .set_bucket_policy("alice-deny", &deny, 0)
        .await
        .unwrap();
    let mut revoked = BatchCreateRequest {
        manifest_bucket: "alice-deny".into(),
        manifest_key: "m.csv".into(),
        ..req("ReplaceTags", "m.csv")
    };
    revoked.tags = tags(&[("batch", "1")]);
    assert!(
        matches!(
            create_batch(&f.st, alice, false, revoked).await,
            Err(BatchError::Denied(_))
        ),
        "读权限被撤销后必须 403"
    );
}

#[tokio::test]
async fn pause_resume_engine() {
    let f = fx().await;
    f.put("src", "a", b"x").await;
    f.put("src", "b", b"x").await;
    f.st.store
        .put_object("src", "m.csv", Bytes::from_static(b"src,a\nsrc,b\n"), None)
        .await
        .unwrap();
    let mut r = req("ReplaceTags", "m.csv");
    r.tags = tags(&[("k", "v")]);
    r.report_bucket = Some("reports".into());
    r.report_prefix = Some("p".into());
    let id = create_batch(&f.st, f.admin, true, r).await.unwrap().job_id;

    // 跑一条 → running；暂停后不再领取
    let t = wall_now();
    assert!(
        worker_once(f.db(), f.store(), &f.exec(), env(1), t)
            .await
            .unwrap()
    );
    assert!(f.db().job_set_state(id, "pause", t + 1).await.unwrap());
    assert!(
        !worker_once(f.db(), f.store(), &f.exec(), env(1), t + 2)
            .await
            .unwrap()
    );
    assert_eq!(
        f.db().job_tasks(id, 0, 10).await.unwrap()[1]["state"],
        "queued"
    );
    assert!(f.db().job_set_state(id, "resume", t + 3).await.unwrap());
    drain(&f.exec(), f.db(), f.store(), 1).await;
    assert_eq!(f.db().job_get(id).await.unwrap().unwrap()["state"], "done");
}

#[tokio::test]
async fn http_create_then_cancel() {
    use oss_server::router::build_router;

    let dir = tempfile::tempdir().unwrap();
    let st = common::state(dir.path()).await;
    let uid = st.db.create_user("erik", "pw", true).await.unwrap();
    for b in ["src", "reports"] {
        st.store.create_bucket(b).await.unwrap();
        st.db.insert_bucket(b, Some(uid)).await.unwrap();
    }
    st.store
        .put_object(
            "src",
            "m.csv",
            Bytes::from_static(b"src,a\n"),
            Some("text/csv".into()),
        )
        .await
        .unwrap();
    let store = Arc::clone(&st.store);
    let app = build_router(st).unwrap();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let base = format!("http://{addr}");
    let c = reqwest::Client::new();
    let login: serde_json::Value = c
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username":"erik","password":"pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = login["token"].as_str().unwrap();

    let resp = c
        .post(format!("{base}/api/v1/jobs"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "kind":"batch",
            "manifest_bucket":"src",
            "manifest_key":"m.csv",
            "operation":"ReplaceTags",
            "tags":{"k":"v"},
            "report_bucket":"reports",
            "report_prefix":"web"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let created: serde_json::Value = resp.json().await.unwrap();
    let id = created["job_id"].as_i64().unwrap();
    assert_eq!(created["total"], 1);

    let resp = c
        .post(format!("{base}/api/v1/jobs/{id}/cancel"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let (_, bytes) = store
        .get_object("reports", &format!("web/{id}/results.csv"))
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(bytes.to_vec()).unwrap().lines().count(),
        1
    );
}

/// M9 S3 m5：Batch Delete 对 sync 桶走共享 sink；站点未配对（sink 不可用）→ 任务失败，
/// 本地对象原样（不得绕过 sink 直连 store 删除）。
#[tokio::test]
async fn batch_delete_on_sync_bucket_refuses_without_remote() {
    let f = fx().await;
    f.put("src", "sync-del", b"keep").await;
    sqlx::query(
        "UPDATE buckets SET replication_mode = 'sync', versioning = 'Enabled' WHERE name = 'src'",
    )
    .execute(f.db().pool())
    .await
    .unwrap();
    let d = payload("src", "sync-del", f.admin);
    let err = f.run(BATCH_DELETE, &d).await.unwrap_err();
    assert!(
        err.contains("not configured"),
        "必须由共享 sink 拒绝：{err}"
    );
    assert!(
        f.store().head_object("src", "sync-del").await.is_ok(),
        "sync 删失败时本地对象必须原样"
    );
}

/// 每动作恰好一条桶通知：Copy→目标桶 CreatedCopy；Delete→RemovedDelete；Restore→RestorePost；
/// ReplaceTags 无事件类型不发；事件来源标记为 `oss-batch`。
#[tokio::test]
async fn batch_actions_emit_bucket_notifications() {
    let f = fx().await;
    let all = [
        "s3:ObjectCreated:Copy",
        "s3:ObjectRemoved:Delete",
        "s3:ObjectRestore:Post",
    ];
    f.notify("src", &all).await;
    f.notify("dst", &all).await;

    // Copy：只发目标桶，带新版本/大小/ETag。
    f.put("src", "a", b"hello").await;
    let mut cp = payload("src", "a", f.admin);
    cp.target_bucket = Some("dst".into());
    cp.target_prefix = Some("copies/".into());
    assert_eq!(f.run(BATCH_COPY, &cp).await.unwrap(), "Succeeded");
    let rows = f.outbox().await;
    assert_eq!(rows.len(), 1, "Copy 恰好一条事件");
    assert_eq!((rows[0].0.as_str(), rows[0].1.as_str()), ("dst", "t-dst"));
    let ev = &rows[0].2;
    assert_eq!(ev["eventName"], "s3:ObjectCreated:Copy");
    assert_eq!(ev["s3"]["bucket"]["name"], "dst");
    assert_eq!(ev["s3"]["object"]["key"], "copies%2Fa");
    assert_eq!(ev["s3"]["object"]["versionId"], "null");
    assert_eq!(ev["s3"]["object"]["size"], 5);
    let etag = f.store().head_object("dst", "copies/a").await.unwrap().etag;
    assert_eq!(ev["s3"]["object"]["eTag"], etag.as_str());
    assert_eq!(ev["source"]["host"], "internal");
    assert_eq!(ev["source"]["userAgent"], "oss-batch");

    // Delete（非版本桶）：RemovedDelete，无版本号。
    f.clear_outbox().await;
    let d = payload("dst", "copies/a", f.admin);
    assert_eq!(f.run(BATCH_DELETE, &d).await.unwrap(), "Succeeded");
    let rows = f.outbox().await;
    assert_eq!(rows.len(), 1, "Delete 恰好一条事件");
    assert_eq!(rows[0].2["eventName"], "s3:ObjectRemoved:Delete");
    assert_eq!(rows[0].2["s3"]["object"]["versionId"], "null");

    // NotFound：未发生删除 → 不发（与在线 delete_object 一致）。
    f.clear_outbox().await;
    let missing = payload("src", "nope", f.admin);
    assert_eq!(f.run(BATCH_DELETE, &missing).await.unwrap(), "Succeeded");
    assert!(f.outbox().await.is_empty(), "空删不得发事件");

    // ReplaceTags：S3 无标签事件类型 → 不发。
    f.put("src", "tag", b"x").await;
    f.clear_outbox().await;
    let mut t = payload("src", "tag", f.admin);
    t.tags = vec![ObjectTag {
        key: "k".into(),
        value: "1".into(),
    }];
    assert_eq!(f.run(BATCH_TAGS, &t).await.unwrap(), "Succeeded");
    assert!(f.outbox().await.is_empty(), "ReplaceTags 不发桶通知");

    // Restore：受理发 RestorePost；重复执行（Restoring）幂等且不重发。
    f.put("src", "arch", b"cold-data").await;
    let info = f.store().head_object("src", "arch").await.unwrap();
    f.store()
        .archive_version(
            "src",
            "arch",
            info.version_id.as_deref().unwrap_or("null"),
            ArchiveSpec {
                backend: "cold".into(),
                key: "tier-key".into(),
                size: info.size,
                source_etag: info.etag.clone(),
                etag: "remote".into(),
                sha256: "00".into(),
                class: "GLACIER".into(),
            },
            1,
        )
        .await
        .unwrap();
    f.clear_outbox().await;
    let mut r = payload("src", "arch", f.admin);
    r.restore_days = Some(1);
    assert_eq!(f.run(BATCH_RESTORE, &r).await.unwrap(), "Succeeded");
    let rows = f.outbox().await;
    assert_eq!(rows.len(), 1, "Restore 受理恰好一条事件");
    assert_eq!(rows[0].2["eventName"], "s3:ObjectRestore:Post");
    assert_eq!(rows[0].2["s3"]["bucket"]["name"], "src");
    assert_eq!(rows[0].2["s3"]["object"]["versionId"], "null");
    assert_eq!(f.run(BATCH_RESTORE, &r).await.unwrap(), "Succeeded");
    assert_eq!(f.outbox().await.len(), 1, "已在恢复不重发 RestorePost");
}

/// 版本化桶：无版本删除 → DeleteMarkerCreated（带 marker 版本）；点名删版本 → Delete（带该版本）。
#[tokio::test]
async fn batch_delete_events_are_version_aware() {
    let f = fx().await;
    f.notify(
        "src",
        &[
            "s3:ObjectRemoved:Delete",
            "s3:ObjectRemoved:DeleteMarkerCreated",
        ],
    )
    .await;
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = 'src'")
        .execute(f.db().pool())
        .await
        .unwrap();

    // 无版本号删除 → marker 事件（versionId 非 "null"）。
    f.put("src", "marker", b"x").await;
    let d = payload("src", "marker", f.admin);
    assert_eq!(f.run(BATCH_DELETE, &d).await.unwrap(), "Succeeded");
    let rows = f.outbox().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].2["eventName"],
        "s3:ObjectRemoved:DeleteMarkerCreated"
    );
    let marker_vid = rows[0].2["s3"]["object"]["versionId"].as_str().unwrap();
    assert_ne!(marker_vid, "null", "marker 事件必须带 marker 版本号");

    // 点名删版本 → RemovedDelete，版本号原样回显。
    f.clear_outbox().await;
    f.put("src", "ver", b"y").await;
    let vid = f
        .store()
        .head_object("src", "ver")
        .await
        .unwrap()
        .version_id
        .unwrap_or_else(|| "null".into());
    let mut vd = payload("src", "ver", f.admin);
    vd.version_id = Some(vid.clone());
    assert_eq!(f.run(BATCH_DELETE, &vd).await.unwrap(), "Succeeded");
    let rows = f.outbox().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].2["eventName"], "s3:ObjectRemoved:Delete");
    assert_eq!(rows[0].2["s3"]["object"]["versionId"], vid.as_str());
}

/// 双桶双配置：事件只进对象所在桶的配置，不泄漏到作业创建者/其它桶。
#[tokio::test]
async fn batch_events_stay_in_object_bucket() {
    let f = fx().await;
    for b in ["src", "dst"] {
        f.notify(b, &["s3:ObjectCreated:Copy", "s3:ObjectRemoved:Delete"])
            .await;
    }
    f.put("src", "x", b"data").await;
    let mut cp = payload("src", "x", f.admin);
    cp.target_bucket = Some("dst".into());
    assert_eq!(f.run(BATCH_COPY, &cp).await.unwrap(), "Succeeded");
    let rows = f.outbox().await;
    assert_eq!(rows.len(), 1, "Copy 只对目标桶发");
    assert_eq!(rows[0].0, "dst");
    assert_eq!(rows[0].2["s3"]["bucket"]["name"], "dst");

    f.clear_outbox().await;
    let d = payload("src", "x", f.admin);
    assert_eq!(f.run(BATCH_DELETE, &d).await.unwrap(), "Succeeded");
    let rows = f.outbox().await;
    assert_eq!(rows.len(), 1, "Delete 只对对象桶发");
    assert_eq!(rows[0].0, "src");
    assert_eq!(rows[0].2["s3"]["bucket"]["name"], "src");
}
