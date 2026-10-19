//! M8 S4b：远端 tier 后端注册表 + 删除回收。
//!
//! 远端 key 口径：`<tier_id>/<bucket>/<version_id>/<raw key>`，raw key 不预编码；
//! 编码只发生在 [`ReplClient`] 的 canonical path（N3）。

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::config::TierTarget;
use crate::repl_client::ReplClient;

/// 单个 tier 的远端对象操作。`key` = 完整远端 key（含 tier_id 前缀，未编码）。
#[async_trait::async_trait]
pub trait TierBackend: Send + Sync {
    fn id(&self) -> &str;
    /// 上传并返回远端 ETag（实现须校验响应 ETag 与 `etag` 一致）。
    async fn put(&self, key: &str, body: Bytes, etag: &str) -> Result<String, String>;
    async fn get(&self, key: &str) -> Result<Bytes, String>;
    /// 远端大小；键不存在 → None。
    async fn head(&self, key: &str) -> Result<Option<u64>, String>;
    async fn delete(&self, key: &str) -> Result<(), String>;
}

/// 远端 key 构造（N3：raw key 原样拼接）。
pub fn tier_key(tier_id: &str, bucket: &str, version_id: &str, key: &str) -> String {
    format!("{tier_id}/{bucket}/{version_id}/{key}")
}

/// S3 兼容远端（经 [`ReplClient`]；http + SigV4）。
pub struct S3TierBackend {
    id: String,
    bucket: String,
    client: Arc<ReplClient>,
}

impl S3TierBackend {
    pub fn new(target: &TierTarget, client: Arc<ReplClient>) -> Self {
        Self {
            id: target.id.clone(),
            bucket: target.bucket.clone(),
            client,
        }
    }
}

#[async_trait::async_trait]
impl TierBackend for S3TierBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn put(&self, key: &str, body: Bytes, etag: &str) -> Result<String, String> {
        self.client
            .tier_put(&self.id, &self.bucket, key, body, etag)
            .await
            .map_err(|e| e.message)
    }

    async fn get(&self, key: &str) -> Result<Bytes, String> {
        self.client
            .tier_get(&self.id, &self.bucket, key)
            .await
            .map_err(|e| e.message)
    }

    async fn head(&self, key: &str) -> Result<Option<u64>, String> {
        self.client
            .tier_head(&self.id, &self.bucket, key)
            .await
            .map_err(|e| e.message)
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.client
            .tier_delete(&self.id, &self.bucket, key)
            .await
            .map_err(|e| e.message)
    }
}

/// tier id → 后端（启动期固定；规则校验只认这里的 id）。
#[derive(Clone, Default)]
pub struct TierBackends {
    map: HashMap<String, Arc<dyn TierBackend>>,
    /// 控制台只读状态元数据；只投影 id/endpoint/bucket，绝不带凭证。
    targets: Vec<TierTarget>,
}

impl TierBackends {
    pub fn new(backends: Vec<Arc<dyn TierBackend>>) -> Self {
        Self {
            map: backends
                .into_iter()
                .map(|b| (b.id().to_string(), b))
                .collect(),
            targets: Vec::new(),
        }
    }

    pub fn from_config(tiers: &[TierTarget]) -> Self {
        let client = Arc::new(ReplClient::new_tiers(tiers));
        let map = tiers
            .iter()
            .map(|t| {
                (
                    t.id.clone(),
                    Arc::new(S3TierBackend::new(t, Arc::clone(&client))) as Arc<dyn TierBackend>,
                )
            })
            .collect();
        Self {
            map,
            targets: tiers.to_vec(),
        }
    }

    /// 配置的 tier 目标（顺序同 `[[tiers]]`；调用方只可读出展示字段）。
    pub fn targets(&self) -> &[TierTarget] {
        &self.targets
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn TierBackend>> {
        self.map.get(id).cloned()
    }

    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.map.keys().cloned().collect();
        ids.sort();
        ids
    }
}

/// `TierGcSink` 的真实实现：按 ref.backend 查后端并 DELETE（best-effort，逐条尝试）。
pub struct S3TierGc {
    backends: Arc<TierBackends>,
}

impl S3TierGc {
    pub fn new(backends: Arc<TierBackends>) -> Self {
        Self { backends }
    }
}

#[async_trait::async_trait]
impl xl_s3::tier_gc::TierGcSink for S3TierGc {
    async fn delete(&self, refs: &[xl_store::store::TierRef]) -> Result<(), String> {
        let mut errors = Vec::new();
        for r in refs {
            let Some(b) = self.backends.get(&r.backend) else {
                tracing::warn!(backend = %r.backend, key = %r.key, "tier 已不在配置中，跳过远端删除");
                continue;
            };
            if let Err(e) = b.delete(&r.key).await {
                tracing::warn!(backend = %r.backend, key = %r.key, error = %e, "tier 远端 DELETE 失败");
                errors.push(format!("{}: {e}", r.backend));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}
