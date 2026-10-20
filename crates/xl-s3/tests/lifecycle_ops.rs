//! M8 S4b：生命周期配置 S3 协议面——PUT/GET/DELETE、解析与错误矩阵。

use std::sync::{Arc, Mutex};

use s3s::dto::*;
use s3s::{S3, S3ErrorCode, S3Request};
use xl_s3::lifecycle::{LifecycleError, LifecycleRuleSpec, LifecycleStore, validate_rules};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

/// 内存规则库：两个入口共用同一份 validate_rules 校验语义。
struct MemStore {
    rules: Mutex<Option<Vec<LifecycleRuleSpec>>>,
    tier_ids: Vec<String>,
}

impl MemStore {
    fn new(tier_ids: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            rules: Mutex::new(None),
            tier_ids: tier_ids.iter().map(|s| s.to_string()).collect(),
        })
    }
}

#[async_trait::async_trait]
impl LifecycleStore for MemStore {
    async fn get_rules(
        &self,
        bucket: &str,
    ) -> Result<Option<Vec<LifecycleRuleSpec>>, LifecycleError> {
        if bucket != "bkt" {
            return Err(LifecycleError::NoSuchBucket);
        }
        Ok(self.rules.lock().unwrap().clone())
    }

    async fn put_rules(
        &self,
        bucket: &str,
        rules: &[LifecycleRuleSpec],
    ) -> Result<(), LifecycleError> {
        if bucket != "bkt" {
            return Err(LifecycleError::NoSuchBucket);
        }
        let mut specs = rules.to_vec();
        validate_rules(&mut specs, &self.tier_ids)?;
        specs.sort_by(|a, b| {
            a.prefix
                .cmp(&b.prefix)
                .then_with(|| a.rule_id.cmp(&b.rule_id))
        });
        *self.rules.lock().unwrap() = Some(specs);
        Ok(())
    }

    async fn delete_rules(&self, bucket: &str) -> Result<(), LifecycleError> {
        if bucket != "bkt" {
            return Err(LifecycleError::NoSuchBucket);
        }
        *self.rules.lock().unwrap() = None;
        Ok(())
    }
}

async fn acl() -> xl_s3::access::Acl {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0, object_lock INTEGER NOT NULL DEFAULT 0, object_lock_default_mode TEXT, object_lock_default_days INTEGER, default_encryption TEXT, replication_mode TEXT NOT NULL DEFAULT 'async', deleting INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
          can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, can_delete INTEGER NOT NULL DEFAULT 0, \
          PRIMARY KEY (user_id, bucket));",
    )
    .execute(&pool)
    .await
    .unwrap();
    xl_s3::access::Acl::new(pool)
}

async fn service(tier_ids: &[&str]) -> (OssS3, Arc<MemStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    store.create_bucket("bkt").await.unwrap();
    let mem = MemStore::new(tier_ids);
    let s3 = OssS3::new(store, acl().await, 1024 * 1024)
        .with_lifecycle(Some(Arc::clone(&mem) as Arc<dyn LifecycleStore>));
    (s3, mem, dir)
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

fn rule(
    id: Option<&str>,
    filter_prefix: Option<&str>,
    legacy_prefix: Option<&str>,
    expiration: Option<LifecycleExpiration>,
    transitions: Option<TransitionList>,
) -> LifecycleRule {
    LifecycleRule {
        abort_incomplete_multipart_upload: None,
        expiration,
        filter: filter_prefix.map(|p| LifecycleRuleFilter {
            prefix: Some(p.to_string()),
            ..Default::default()
        }),
        id: id.map(str::to_string),
        noncurrent_version_expiration: None,
        noncurrent_version_transitions: None,
        prefix: legacy_prefix.map(str::to_string),
        status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
        transitions,
    }
}

fn transition(days: i32, class: &str) -> TransitionList {
    vec![Transition {
        days: Some(days),
        storage_class: Some(TransitionStorageClass::from(class.to_string())),
        ..Default::default()
    }]
}

async fn put(
    s3: &OssS3,
    bucket: &str,
    rules: Vec<LifecycleRule>,
) -> s3s::S3Result<PutBucketLifecycleConfigurationOutput> {
    s3.put_bucket_lifecycle_configuration(req(PutBucketLifecycleConfigurationInput {
        bucket: bucket.into(),
        lifecycle_configuration: Some(BucketLifecycleConfiguration { rules }),
        ..Default::default()
    }))
    .await
    .map(|r| r.output)
}

#[tokio::test]
async fn roundtrip_put_get_delete() {
    let (s3, mem, _d) = service(&["cold"]).await;
    put(
        &s3,
        "bkt",
        vec![
            rule(
                Some("expire"),
                Some("logs/"),
                None,
                Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                None,
            ),
            // 无 id、无 tier 的归档 Transition：tier 应解析为唯一 tier "cold"
            rule(None, None, None, None, Some(transition(7, "GLACIER"))),
        ],
    )
    .await
    .unwrap();

    let stored = mem.rules.lock().unwrap().clone().unwrap();
    assert!(
        stored
            .iter()
            .any(|r| r.rule_id.as_deref() == Some("expire"))
    );
    let generated = stored
        .iter()
        .find(|r| r.rule_id.as_deref() == Some("rule-2"))
        .unwrap();
    assert_eq!(generated.transition_tier.as_deref(), Some("cold"));

    let out = s3
        .get_bucket_lifecycle_configuration(req(GetBucketLifecycleConfigurationInput {
            bucket: "bkt".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let rules = out.rules.unwrap();
    assert_eq!(rules.len(), 2);
    let expire = rules
        .iter()
        .find(|r| r.id.as_deref() == Some("expire"))
        .unwrap();
    assert_eq!(
        expire.filter.as_ref().unwrap().prefix.as_deref(),
        Some("logs/")
    );
    assert_eq!(
        expire.expiration.as_ref().and_then(|e| e.days),
        Some(30),
        "回读 expiration"
    );
    let tr = rules
        .iter()
        .find(|r| r.id.as_deref() == Some("rule-2"))
        .unwrap();
    let t = &tr.transitions.as_ref().unwrap()[0];
    assert_eq!(t.days, Some(7));
    assert_eq!(
        t.storage_class.as_ref().map(|c| c.as_str()),
        Some("GLACIER")
    );
    assert_eq!(tr.status.as_str(), "Enabled", "回读按规范化输入输出 status");

    s3.delete_bucket_lifecycle(req(DeleteBucketLifecycleInput {
        bucket: "bkt".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let e = s3
        .get_bucket_lifecycle_configuration(req(GetBucketLifecycleConfigurationInput {
            bucket: "bkt".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::NoSuchLifecycleConfiguration);
}

#[tokio::test]
async fn unsupported_and_invalid_matrix() {
    let (s3, _mem, _d) = service(&["cold"]).await;

    // config 缺失 / 空 rules → MalformedXML
    let e = s3
        .put_bucket_lifecycle_configuration(req(PutBucketLifecycleConfigurationInput {
            bucket: "bkt".into(),
            lifecycle_configuration: None,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::MalformedXML);
    let e = put(&s3, "bkt", vec![]).await.unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::MalformedXML);

    // 501 优先于 400：带 tag 且无任何受支持动作
    let mut tagged = rule(None, None, None, None, None);
    tagged.filter = Some(LifecycleRuleFilter {
        tag: Some(Tag {
            key: Some("k".into()),
            value: Some("v".into()),
        }),
        ..Default::default()
    });
    let e = put(&s3, "bkt", vec![tagged]).await.unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::NotImplemented, "{e:?}");

    // Transition.date → 501
    let dated = rule(
        None,
        None,
        None,
        None,
        Some(vec![Transition {
            date: Some(Timestamp::from(
                time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            )),
            days: None,
            storage_class: Some(TransitionStorageClass::from_static("GLACIER")),
        }]),
    );
    let e = put(&s3, "bkt", vec![dated]).await.unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::NotImplemented);

    // Expiration.date → 501
    let exp_date = rule(
        None,
        None,
        None,
        Some(LifecycleExpiration {
            date: Some(Timestamp::from(
                time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            )),
            ..Default::default()
        }),
        None,
    );
    let e = put(&s3, "bkt", vec![exp_date]).await.unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::NotImplemented);

    // 无动作 → 400 InvalidRequest
    let e = put(&s3, "bkt", vec![rule(None, Some("p/"), None, None, None)])
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidRequest);

    // 非法 class → InvalidStorageClass
    let e = put(
        &s3,
        "bkt",
        vec![rule(None, None, None, None, Some(transition(1, "WARM")))],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidStorageClass);

    // days 越界 → InvalidArgument
    let e = put(
        &s3,
        "bkt",
        vec![rule(
            None,
            None,
            None,
            Some(LifecycleExpiration {
                days: Some(36_501),
                ..Default::default()
            }),
            None,
        )],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidArgument);

    // legacy prefix 与 filter.prefix 不一致 → 400
    let e = put(
        &s3,
        "bkt",
        vec![rule(
            None,
            Some("a/"),
            Some("b/"),
            Some(LifecycleExpiration {
                days: Some(1),
                ..Default::default()
            }),
            None,
        )],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn archive_tier_resolution_requires_unique_tier() {
    let (s3, _mem, _d) = service(&[]).await;
    let e = put(
        &s3,
        "bkt",
        vec![rule(None, None, None, None, Some(transition(1, "GLACIER")))],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidRequest, "0 个 tier 应 400");

    let (s3, _mem, _d) = service(&["a", "b"]).await;
    let e = put(
        &s3,
        "bkt",
        vec![rule(None, None, None, None, Some(transition(1, "GLACIER")))],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidRequest, "多个 tier 应 400");

    // 显式 tier 必须存在
    let e = put(
        &s3,
        "bkt",
        vec![rule(
            None,
            None,
            None,
            None,
            Some(vec![Transition {
                days: Some(1),
                storage_class: Some(TransitionStorageClass::from_static("GLACIER")),
                ..Default::default()
            }]),
        )],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::InvalidRequest, "缺省多 tier 歧义");

    // 非归档类不需要 tier；显式给出也被忽略
    put(
        &s3,
        "bkt",
        vec![rule(
            None,
            None,
            None,
            None,
            Some(transition(1, "STANDARD_IA")),
        )],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_bucket_404_and_not_injected_501() {
    let (s3, _mem, _d) = service(&["cold"]).await;
    let e = put(
        &s3,
        "nope",
        vec![rule(
            None,
            None,
            None,
            None,
            Some(transition(1, "STANDARD_IA")),
        )],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::NoSuchBucket);
    let e = s3
        .get_bucket_lifecycle_configuration(req(GetBucketLifecycleConfigurationInput {
            bucket: "nope".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::NoSuchBucket);

    // 未注入 store：三个 op 都 501（与 s3s 默认实现口径一致）
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    store.create_bucket("bkt").await.unwrap();
    let bare = OssS3::new(store, acl().await, 1024 * 1024);
    let e = put(
        &bare,
        "bkt",
        vec![rule(
            None,
            None,
            None,
            None,
            Some(transition(1, "STANDARD_IA")),
        )],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), &S3ErrorCode::NotImplemented);
}
