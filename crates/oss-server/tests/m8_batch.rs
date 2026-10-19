//! M8 S3：Batch Operations——清单解析、五种动作、钩子、权限、报告、取消/暂停/重试。

mod common;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use oss_server::db::{Db, JobSpec, NewJobTask};
use oss_server::job_actions::{
    BATCH_COPY, BATCH_DELETE, BATCH_INVOKE, BATCH_RESTORE, BATCH_TAGS, BatchActionPayload,
    BatchError, BatchExecutor, BatchMutationHooks, parse_manifest,
};
use oss_server::job_actions_api::{BatchCreateRequest, TagsBody, create_batch};
use oss_server::jobs::{JobEnv, TaskExecutor, worker_once};
use oss_server::state::AppState;
use xl_s3::olap::OlapTransform;
use xl_s3::replication::{ReplicationSink, WriteInfo};
use xl_s3::restore::RestoreSink;
use xl_s3::tier_gc::TierGcSink;
use xl_store::crypto::{EncSpec, SseCKey};
use xl_store::meta::{ObjectTag, TierPhase};
use xl_store::store::{ArchiveSpec, Store, TierRef};

#[derive(Default)]
struct FakeRepl {
    writes: Mutex<Vec<(String, String)>>,
}

#[async_trait::async_trait]
impl ReplicationSink for FakeRepl {
    async fn on_write(&self, info: WriteInfo<'_>) {
        self.writes
            .lock()
            .unwrap()
            .push((info.bucket.to_string(), info.key.to_string()));
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
            BatchMutationHooks {
                repl: Arc::clone(&self.repl) as Arc<dyn ReplicationSink>,
                tier_gc: Arc::clone(&self.gc) as Arc<dyn TierGcSink>,
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
            false,
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
    assert_eq!(
        f.repl.writes.lock().unwrap().as_slice(),
        &[("dst".to_string(), "copies/a".to_string())]
    );
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
    unsafe { std::env::set_var("OSS_OLAP_ALLOW_PRIVATE", "1") };
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
    f.st.olap_cache.put(
        "ap1",
        "src",
        OlapTransform::prepare(
            "http",
            Some(format!("http://{addr}/t")),
            None,
            5_000,
            1 << 20,
        )
        .unwrap(),
    );
    f.put("src", "invoke", b"payload-bytes").await;
    let mut iv = payload("src", "invoke", f.admin);
    iv.transform_access_point = Some("ap1".into());
    assert_eq!(f.run(BATCH_INVOKE, &iv).await.unwrap(), "Succeeded");
    assert_eq!(
        hits.lock().unwrap().as_slice(),
        &[b"payload-bytes".to_vec()]
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
            false,
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
            true,
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
            true,
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

    // 校验：非法标签（>10）、未知动作、InvokeLambda 缺 AP、WASM AP
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
    let mut w = req("InvokeLambda", "m.csv");
    w.transform_access_point = Some("wasm-ap".into());
    assert!(matches!(
        create_batch(&f.st, f.admin, true, w).await,
        Err(BatchError::Invalid(_))
    ));

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
