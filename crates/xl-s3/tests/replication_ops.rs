//! M6 切片 4：桶复制配置 API + 写入钩子（服务级；SQL/worker 在 oss-server 侧测）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::replication::{ReplicationSink, ReplicationStore, WriteInfo};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

async fn acl() -> xl_s3::access::Acl {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0, object_lock INTEGER NOT NULL DEFAULT 0, object_lock_default_mode TEXT, object_lock_default_days INTEGER, default_encryption TEXT, replication_mode TEXT NOT NULL DEFAULT 'async', deleting INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
          can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, can_delete INTEGER NOT NULL DEFAULT 0, \
          PRIMARY KEY (user_id, bucket));\
         CREATE TABLE bucket_policies (bucket TEXT PRIMARY KEY, policy_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, uid INTEGER NOT NULL, op TEXT NOT NULL, bucket TEXT, object_key TEXT, decision TEXT NOT NULL, detail TEXT);\
         CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, user_id INTEGER NOT NULL, \
          status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL, \
          parent_ak TEXT, session_policy TEXT, kind TEXT NOT NULL DEFAULT 'long-term');\
         CREATE TABLE event_configs (bucket TEXT PRIMARY KEY, config_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);",
    )
    .execute(&pool)
    .await
    .unwrap();
    xl_s3::access::Acl::new(pool)
}

/// REPLICA 摄入测试用 site token（O-2：仅 token 认证的内部请求可声明 REPLICA）。
const TEST_SITE_TOKEN: &str = "test-site-token";

async fn service(
    sink: Option<Arc<dyn ReplicationSink>>,
    store: Option<Arc<dyn ReplicationStore>>,
) -> (OssS3, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let st = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    (
        OssS3::new(st, acl().await, 16 * 1024 * 1024)
            .with_replication(sink, store)
            .with_site_token(Some(TEST_SITE_TOKEN.into())),
        dir,
    )
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

#[derive(Default)]
struct MemReplStore {
    rules: Mutex<HashMap<String, String>>,
    remotes: Vec<String>,
}

impl MemReplStore {
    fn new(remotes: &[&str]) -> Self {
        Self {
            rules: Mutex::new(HashMap::new()),
            remotes: remotes.iter().map(|r| r.to_string()).collect(),
        }
    }
}

#[async_trait]
impl ReplicationStore for MemReplStore {
    async fn load_rules(&self, bucket: &str) -> Result<Option<String>, String> {
        Ok(self.rules.lock().unwrap().get(bucket).cloned())
    }
    async fn save_rules(&self, bucket: &str, rules_json: &str) -> Result<(), String> {
        self.rules
            .lock()
            .unwrap()
            .insert(bucket.to_string(), rules_json.to_string());
        Ok(())
    }
    async fn remove_rules(&self, bucket: &str) -> Result<(), String> {
        self.rules.lock().unwrap().remove(bucket);
        Ok(())
    }
    async fn remote_ids(&self) -> Vec<String> {
        self.remotes.clone()
    }
}

#[derive(Debug, Clone)]
struct Recorded {
    bucket: String,
    key: String,
    version: Option<String>,
    marker: bool,
    replica: bool,
}

#[derive(Default)]
struct MockSink {
    got: Mutex<Vec<Recorded>>,
}

#[async_trait]
impl ReplicationSink for MockSink {
    async fn on_write(&self, info: WriteInfo<'_>) {
        self.got.lock().unwrap().push(Recorded {
            bucket: info.bucket.to_string(),
            key: info.key.to_string(),
            version: info.version_id.map(str::to_string),
            marker: info.is_delete_marker,
            replica: info.is_replica,
        });
    }
}

/// 带身份建桶（落 buckets 行——版本状态查询的前提）。
fn with_user<T>(input: T) -> S3Request<T> {
    let mut r = req(input);
    r.extensions.insert(xl_s3::access::AuthedUser {
        uid: 1,
        is_admin: true,
        ak: String::new(),
    });
    r
}

async fn versioned_bucket(s3: &OssS3, bucket: &str) {
    s3.create_bucket(with_user(CreateBucketInput {
        bucket: bucket.into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_bucket_versioning(req(PutBucketVersioningInput {
        bucket: bucket.into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        mfa: None,
        versioning_configuration: VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static("Enabled")),
            mfa_delete: None,
        },
    }))
    .await
    .unwrap();
}

fn rule(dest: &str, prefix: Option<&str>, dm: bool) -> ReplicationRule {
    ReplicationRule {
        delete_marker_replication: Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(if dm {
                "Enabled"
            } else {
                "Disabled"
            })),
        }),
        destination: Destination {
            access_control_translation: None,
            account: None,
            bucket: dest.into(),
            encryption_configuration: None,
            metrics: None,
            replication_time: None,
            storage_class: None,
        },
        existing_object_replication: None,
        filter: prefix.map(|p| ReplicationRuleFilter {
            and: None,
            prefix: Some(p.into()),
            tag: None,
        }),
        id: Some("x".into()),
        prefix: None,
        priority: None,
        source_selection_criteria: None,
        status: ReplicationRuleStatus::from_static("Enabled"),
    }
}

fn repl_cfg(rules: Vec<ReplicationRule>) -> PutBucketReplicationInput {
    PutBucketReplicationInput {
        bucket: "repapi".into(),
        checksum_algorithm: None,
        content_md5: None,
        expected_bucket_owner: None,
        replication_configuration: ReplicationConfiguration {
            role: "arn:aws:iam::1:role/r".into(),
            rules,
        },
        token: None,
    }
}

#[tokio::test]
async fn replication_api_roundtrip_and_errors() {
    let ms = Arc::new(MemReplStore::new(&["r1"]));
    let (s3, _d) = service(None, Some(ms.clone())).await;
    versioned_bucket(&s3, "repapi").await;

    // Put → Get 回读
    s3.put_bucket_replication(req(repl_cfg(vec![rule("r1/dst", Some("logs/"), true)])))
        .await
        .unwrap();
    let got = s3
        .get_bucket_replication(req(GetBucketReplicationInput {
            bucket: "repapi".into(),
            expected_bucket_owner: None,
        }))
        .await
        .unwrap()
        .output
        .replication_configuration
        .unwrap();
    assert_eq!(got.rules.len(), 1);
    let r = &got.rules[0];
    assert_eq!(r.id.as_deref(), Some("x"));
    assert_eq!(r.destination.bucket, "r1/dst");
    assert_eq!(r.filter.as_ref().unwrap().prefix.as_deref(), Some("logs/"));
    assert_eq!(
        r.delete_marker_replication
            .as_ref()
            .unwrap()
            .status
            .as_ref()
            .unwrap()
            .as_str(),
        "Enabled"
    );

    // Tag / And 过滤器 Put → Get 回读：单标签归一为 Tag，多条件归一为 And（可再次提交）。
    let tag = Tag {
        key: Some("env".into()),
        value: Some("prod".into()),
    };
    let mut t = rule("r1/dst", None, false);
    t.id = Some("tag".into());
    t.filter = Some(ReplicationRuleFilter {
        and: None,
        prefix: None,
        tag: Some(tag.clone()),
    });
    let mut a = rule("r1/dst", None, false);
    a.id = Some("and".into());
    a.filter = Some(ReplicationRuleFilter {
        and: Some(ReplicationRuleAndOperator {
            prefix: Some("logs/".into()),
            tags: Some(vec![tag]),
        }),
        prefix: None,
        tag: None,
    });
    s3.put_bucket_replication(req(repl_cfg(vec![t, a])))
        .await
        .unwrap();
    let got = s3
        .get_bucket_replication(req(GetBucketReplicationInput {
            bucket: "repapi".into(),
            expected_bucket_owner: None,
        }))
        .await
        .unwrap()
        .output
        .replication_configuration
        .unwrap();
    assert_eq!(
        got.rules[0]
            .filter
            .as_ref()
            .unwrap()
            .tag
            .as_ref()
            .unwrap()
            .key
            .as_deref(),
        Some("env")
    );
    let and = got.rules[1].filter.as_ref().unwrap().and.as_ref().unwrap();
    assert_eq!(and.prefix.as_deref(), Some("logs/"));
    assert_eq!(and.tags.as_ref().unwrap().len(), 1);

    // 未知 remote → 400
    let e = s3
        .put_bucket_replication(req(repl_cfg(vec![rule("nope/dst", None, false)])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    // And 过滤器 → 400
    let mut bad = rule("r1/dst", None, false);
    bad.filter = Some(ReplicationRuleFilter {
        and: Some(ReplicationRuleAndOperator {
            prefix: None,
            tags: None,
        }),
        prefix: None,
        tag: None,
    });
    let e = s3
        .put_bucket_replication(req(repl_cfg(vec![bad])))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    // 未开版本控制 → 400
    s3.create_bucket(req(CreateBucketInput {
        bucket: "repapi2".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let mut c = repl_cfg(vec![rule("r1/dst", None, false)]);
    c.bucket = "repapi2".into();
    let e = s3.put_bucket_replication(req(c)).await.unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);

    // Delete → Get 404
    s3.delete_bucket_replication(req(DeleteBucketReplicationInput {
        bucket: "repapi".into(),
        expected_bucket_owner: None,
    }))
    .await
    .unwrap();
    let e = s3
        .get_bucket_replication(req(GetBucketReplicationInput {
            bucket: "repapi".into(),
            expected_bucket_owner: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        &s3s::S3ErrorCode::ReplicationConfigurationNotFoundError
    );
}

#[tokio::test]
async fn replication_hooks_put_replica_marker_and_mpu() {
    let sink = Arc::new(MockSink::default());
    let (s3, _d) = service(Some(sink.clone()), None).await;
    versioned_bucket(&s3, "rephook").await;

    // 普通 PUT
    s3.put_object(req(PutObjectInput {
        bucket: "rephook".into(),
        key: "a".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"one"))),
        ..Default::default()
    }))
    .await
    .unwrap();
    // REPLICA 摄入 PUT
    let mut r = req(PutObjectInput {
        bucket: "rephook".into(),
        key: "rep".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"two"))),
        ..Default::default()
    });
    r.headers
        .insert("x-amz-replication-status", "REPLICA".parse().unwrap());
    r.headers
        .insert("x-oss-site-token", TEST_SITE_TOKEN.parse().unwrap());
    s3.put_object(r).await.unwrap();
    // MPU complete
    let up = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "rephook".into(),
            key: "mp".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let etag = s3
        .upload_part(req(UploadPartInput {
            bucket: "rephook".into(),
            key: "mp".into(),
            upload_id: up.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"part"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .e_tag
        .unwrap()
        .value()
        .to_string();
    s3.complete_multipart_upload(req(CompleteMultipartUploadInput {
        bucket: "rephook".into(),
        key: "mp".into(),
        upload_id: up,
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: Some(ETag::Strong(etag)),
                ..Default::default()
            }]),
        }),
        ..Default::default()
    }))
    .await
    .unwrap();
    // CopyObject（V3②：复制覆盖 CopyObject）
    s3.copy_object(req(CopyObjectInput::builder()
        .bucket("rephook".to_string())
        .key("copy".to_string())
        .copy_source(CopySource::parse("/rephook/a").unwrap())
        .build()
        .unwrap()))
        .await
        .unwrap();
    // 版本桶删除 → marker
    s3.delete_object(req(DeleteObjectInput {
        bucket: "rephook".into(),
        key: "a".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 批量删除 → marker（V3②：复制覆盖 DeleteObjects）
    s3.put_object(req(PutObjectInput {
        bucket: "rephook".into(),
        key: "batch".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"b"))),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.delete_objects(with_user(DeleteObjectsInput {
        bucket: "rephook".into(),
        bypass_governance_retention: None,
        checksum_algorithm: None,
        delete: Delete {
            objects: vec![ObjectIdentifier {
                e_tag: None,
                key: "batch".into(),
                last_modified_time: None,
                size: None,
                version_id: None,
            }],
            quiet: None,
        },
        expected_bucket_owner: None,
        mfa: None,
        request_payer: None,
    }))
    .await
    .unwrap();

    let got = sink.got.lock().unwrap().clone();
    let keys: Vec<&str> = got.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(keys, vec!["a", "rep", "mp", "copy", "a", "batch", "batch"]);
    assert_eq!(got[0].bucket, "rephook");
    assert!(!got[0].replica && !got[0].marker);
    assert!(got[0].version.as_deref().is_some_and(|v| v != "null"));
    assert!(got[1].replica, "REPLICA 摄入头应透传给 sink");
    assert!(!got[2].marker, "MPU complete 是数据提交");
    assert!(!got[3].marker, "CopyObject 是数据提交");
    assert!(got[4].marker, "版本桶删除应触发 marker 钩子");
    assert!(!got[5].marker, "批量前 PUT 是数据提交");
    assert!(got[6].marker, "批量删除应触发 marker 钩子");
}

/// O-2：普通客户端伪造 `x-amz-replication-status: REPLICA` 不能抑制复制。
/// 只有 site token 认证的内部请求才被当作副本摄入（并保留 M7 防环语义）。
#[tokio::test]
async fn replica_header_requires_site_token() {
    let sink = Arc::new(MockSink::default());
    let (s3, _d) = service(Some(sink.clone()), None).await;
    versioned_bucket(&s3, "sectok").await;

    for (key, token) in [
        ("spoof", None),
        ("spoof-wrong", Some("wrong-token")),
        ("trusted", Some(TEST_SITE_TOKEN)),
    ] {
        let mut r = req(PutObjectInput {
            bucket: "sectok".into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"x"))),
            ..Default::default()
        });
        r.headers
            .insert("x-amz-replication-status", "REPLICA".parse().unwrap());
        if let Some(t) = token {
            r.headers.insert("x-oss-site-token", t.parse().unwrap());
        }
        s3.put_object(r).await.unwrap();
    }
    let head = |key: &str| {
        let s3 = s3.clone();
        let key = key.to_string();
        async move {
            let h = s3
                .head_object(req(HeadObjectInput {
                    bucket: "sectok".into(),
                    key,
                    ..Default::default()
                }))
                .await
                .unwrap()
                .output;
            h.replication_status.map(|s| s.as_str().to_string())
        }
    };
    assert_eq!(head("spoof").await, None, "未认证 REPLICA 不得落复制状态");
    assert_eq!(head("spoof-wrong").await, None, "错 token 不得落复制状态");
    assert_eq!(
        head("trusted").await.as_deref(),
        Some("REPLICA"),
        "受信内部请求的 REPLICA 摄入语义不变"
    );

    // DELETE：未认证的 REPLICA 头被忽略（删除照常复制/入队）；受信请求维持 M7 防环。
    for (key, token) in [("spoof-del", None), ("trusted-del", Some(TEST_SITE_TOKEN))] {
        let mut r = req(DeleteObjectInput {
            bucket: "sectok".into(),
            key: key.into(),
            ..Default::default()
        });
        r.headers
            .insert("x-amz-replication-status", "REPLICA".parse().unwrap());
        if let Some(t) = token {
            r.headers.insert("x-oss-site-token", t.parse().unwrap());
        }
        s3.delete_object(r).await.unwrap();
    }

    let got = sink.got.lock().unwrap().clone();
    let keys: Vec<&str> = got.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            "spoof",
            "spoof-wrong",
            "trusted",
            "spoof-del",
            "trusted-del"
        ]
    );
    assert!(
        !got[0].replica && !got[1].replica,
        "伪造 REPLICA 必须照常复制"
    );
    assert!(got[2].replica, "受信 REPLICA 摄入仍标记 is_replica");
    assert!(!got[3].replica, "伪造 REPLICA 删除必须照常复制（防抑制）");
    assert!(got[4].replica, "受信副本删除保留 M7 防环");
}
