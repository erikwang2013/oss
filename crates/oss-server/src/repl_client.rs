//! M6 切片 4：复制出站客户端——SigV4（s3s-sigv4 原语）+ hyper http1 直连。
//!
//! - 端点 http/https 均可；https 走 rustls（`[tls.client]` 自定义 CA/身份可选）。
//! - PUT 携带 `x-amz-replication-status: REPLICA`，且该头**纳入 SignedHeaders**（s3s 服务端
//!   对未签名的 x-amz-* 返回 403）。
//! - 单段源（32 hex ETag）断言响应 ETag；MPU 源 ETag 非 MD5，跳过断言（目标重算）。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt as _;
use http_body_util::{BodyExt as _, Full, StreamBody};
use hyper::body::Frame;
use hyper::{Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::Digest as _;
use tokio::io::AsyncReadExt as _;

use crate::config::{RemoteTarget, TierTarget};
use crate::tier::{TierByteStream, TierObject};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrClass {
    Retryable,
    Permanent,
}

#[derive(Debug)]
pub struct ReplError {
    pub class: ErrClass,
    pub message: String,
    /// HTTP 状态（仅远端响应错误有值；连接/构造错误为 None）。
    pub status: Option<u16>,
}

impl ReplError {
    pub fn retryable(msg: impl Into<String>) -> Self {
        Self {
            class: ErrClass::Retryable,
            message: msg.into(),
            status: None,
        }
    }
    pub fn permanent(msg: impl Into<String>) -> Self {
        Self {
            class: ErrClass::Permanent,
            message: msg.into(),
            status: None,
        }
    }
}

struct Remote {
    /// 原始端点（站点对等 remote 在 peer TLS 下由此升级 https）。
    endpoint: String,
    https: bool,
    host: String,
    port: u16,
    /// TLS SNI 覆写（仅 `[[remotes]]`/`[[tiers]]` 的 https 端点）。
    server_name: Option<String>,
    /// M7 站点对等 remote：地址遵循 `[tls.server]` 模式（http 存储、连接期升级）。
    peer: bool,
    access_key: String,
    secret_key: String,
    region: String,
    /// M8 缺口 C：`max_bytes_per_sec` 的令牌桶；None = 不限速（旧行为）。
    limiter: Option<RateLimiter>,
}

/// 令牌桶：容量 = 1 秒的量，空闲后最多攒 1 秒的突发。
struct TokenBucket {
    rate: u64,
    capacity: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(rate: u64) -> Self {
        let full = rate as f64;
        Self {
            rate,
            capacity: full,
            tokens: full,
            last: Instant::now(),
        }
    }

    /// 补桶后取走现有 token（至多 `n`），返回仍未满足的字节数。
    /// 必须部分取走：容量只有 1 秒的量，等「攒够整对象」会永远等不到。
    fn drain(&mut self, n: u64, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate as f64).min(self.capacity);
        self.last = now;
        let take = self.tokens.min(n as f64);
        self.tokens -= take;
        n - take as u64
    }
}

struct RateLimiter {
    rate: u64,
    bucket: Mutex<TokenBucket>,
}

impl RateLimiter {
    fn new(rate: u64) -> Self {
        Self {
            rate,
            bucket: Mutex::new(TokenBucket::new(rate)),
        }
    }

    /// shortcut: 对象级限速（整份配额等够再发，单对象内不成形）；需要逐块整形时升级为流式发送。
    async fn acquire(&self, mut remaining: u64) {
        while remaining > 0 {
            remaining = self.bucket.lock().unwrap().drain(remaining, Instant::now());
            if remaining > 0 {
                let wait = Duration::from_secs_f64(remaining as f64 / self.rate as f64);
                tokio::time::sleep(wait).await;
            }
        }
    }
}

fn limiter_of(target: &RemoteTarget) -> Option<RateLimiter> {
    target
        .max_bytes_per_sec
        // 启动期 remotes_validate 是唯一闸门（拒 <1024）；程序化构造的 Some(0) 视为不限
        .filter(|&r| r > 0)
        .map(RateLimiter::new)
}

fn remote_of(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    limiter: Option<RateLimiter>,
    server_name: Option<String>,
    peer: bool,
) -> Option<Remote> {
    let ep = crate::tls_config::parse_endpoint(endpoint, "remote endpoint").ok()?;
    Some(Remote {
        endpoint: endpoint.to_string(),
        https: ep.https,
        host: ep.host,
        port: ep.port,
        server_name,
        peer,
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
        region: region.to_string(),
        limiter,
    })
}

fn plain_tls() -> std::sync::Arc<crate::tls::Tls> {
    crate::tls::Tls::load(&crate::tls_config::TlsToml::default()).expect("空 TLS 配置不会失败")
}

/// 出站客户端（remotes 静态映射；连接每请求新建——复制频率低，无池化）。
pub struct ReplClient {
    remotes: HashMap<String, Remote>,
    tls: std::sync::Arc<crate::tls::Tls>,
}

impl ReplClient {
    pub fn new(targets: &[RemoteTarget]) -> Self {
        let remotes = targets
            .iter()
            .filter_map(|t| {
                Some((
                    t.id.clone(),
                    remote_of(
                        &t.endpoint,
                        &t.access_key,
                        &t.secret_key,
                        &t.region,
                        limiter_of(t),
                        t.server_name.clone(),
                        false,
                    )?,
                ))
            })
            .collect();
        Self {
            remotes,
            tls: plain_tls(),
        }
    }

    /// 注入进程 TLS 物料（自定义 CA / mTLS 身份 / peer 模式）。
    pub fn with_tls(mut self, tls: std::sync::Arc<crate::tls::Tls>) -> Self {
        self.tls = tls;
        self
    }

    /// 共享同一份 TLS 物料（复制 worker 的站点对等分支用）。
    pub fn tls(&self) -> std::sync::Arc<crate::tls::Tls> {
        std::sync::Arc::clone(&self.tls)
    }

    /// M8 S4b：tier 目标（同 `[[remotes]]` 形态 + 目标桶；id 即 tier id）。
    pub fn new_tiers(targets: &[TierTarget]) -> Self {
        let remotes = targets
            .iter()
            .filter_map(|t| {
                Some((
                    t.id.clone(),
                    remote_of(
                        &t.endpoint,
                        &t.access_key,
                        &t.secret_key,
                        &t.region,
                        None,
                        t.server_name.clone(),
                        false,
                    )?,
                ))
            })
            .collect();
        Self {
            remotes,
            tls: plain_tls(),
        }
    }

    /// tier PUT（流式）：发送预落盘的暂存文件；`sha256_hex` 是 SigV4 payload hash，
    /// 断言响应 ETag == 本地 MD5（远端 ETag 口径）。
    #[allow(clippy::too_many_arguments)]
    pub async fn tier_put_file(
        &self,
        remote_id: &str,
        bucket: &str,
        key: &str,
        path: &Path,
        size: u64,
        sha256_hex: &str,
        md5_hex: &str,
    ) -> Result<String, ReplError> {
        let meta = tokio::fs::metadata(path)
            .await
            .map_err(|e| ReplError::retryable(format!("tier 暂存文件不可读：{e}")))?;
        if meta.len() != size {
            return Err(ReplError::permanent(format!(
                "tier 暂存文件长度 {} ≠ 声明 {size} 字节",
                meta.len()
            )));
        }
        let raw = hex::decode(md5_hex).map_err(|_| ReplError::permanent("MD5 非法"))?;
        let md5_b64 =
            base64::engine::Engine::encode(&base64::engine::general_purpose::STANDARD, &raw);
        let headers = vec![("content-md5".to_string(), md5_b64)];
        let resp = self
            .send_path_impl(
                remote_id,
                "PUT",
                &format!("/{bucket}/{key}"),
                &[],
                OutBody::File {
                    path: path.to_path_buf(),
                    size,
                    sha256: sha256_hex.to_string(),
                },
                &headers,
                None,
            )
            .await?;
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_matches('"').to_string())
            .unwrap_or_default();
        if etag != md5_hex {
            return Err(ReplError::permanent(format!(
                "tier PUT ETag 不符（期望 {md5_hex}，得到 {etag:?}）"
            )));
        }
        Ok(etag)
    }

    /// tier GET（流式）：返回响应体块流（恢复作业边写边算 SHA-256）。
    pub async fn tier_get_stream(
        &self,
        remote_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<TierByteStream, ReplError> {
        let resp = self
            .send(remote_id, "GET", bucket, Some(key), &[], None, &[], None)
            .await?;
        let stream = resp
            .into_body()
            .into_data_stream()
            .map(|r| r.map_err(|e| format!("读取 tier 响应体失败：{e}")));
        Ok(Box::pin(stream))
    }

    /// tier HEAD：远端无此键 → Ok(None)。
    pub async fn tier_head(
        &self,
        remote_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<Option<u64>, ReplError> {
        match self
            .send(remote_id, "HEAD", bucket, Some(key), &[], None, &[], None)
            .await
        {
            Ok(resp) => Ok(Some(
                resp.headers()
                    .get("content-length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0),
            )),
            Err(e) if e.status == Some(404) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// tier DELETE（GC）：远端无此键 → Ok（幂等）。
    pub async fn tier_delete(
        &self,
        remote_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), ReplError> {
        match self
            .send(remote_id, "DELETE", bucket, Some(key), &[], None, &[], None)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.status == Some(404) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// tier ListObjectsV2（GC 对账）：按 prefix 翻页返回 key/LastModified/Size。
    pub async fn tier_list(
        &self,
        remote_id: &str,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<TierObject>, ReplError> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        // 防御远端不推进 continuation：上限 1000 页 × 1000 键。
        for _ in 0..1_000 {
            let mut query: Vec<(&str, &str)> =
                vec![("list-type", "2"), ("prefix", prefix), ("max-keys", "1000")];
            if let Some(t) = token.as_deref() {
                query.push(("continuation-token", t));
            }
            let resp = self
                .send(remote_id, "GET", bucket, None, &query, None, &[], None)
                .await?;
            let body = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| ReplError::retryable(format!("读取 tier list 响应体失败：{e}")))?
                .to_bytes();
            let page = parse_list_page(&String::from_utf8_lossy(&body))?;
            out.extend(page.contents);
            if !page.is_truncated {
                return Ok(out);
            }
            token = page.next_token;
            if token.is_none() {
                // 截断却没有续点 = 列表不完整；GC 必须 fail closed，不能拿它当全量。
                return Err(ReplError::permanent(
                    "tier list 响应截断但缺少 NextContinuationToken".to_string(),
                ));
            }
        }
        Err(ReplError::permanent("tier list 翻页超过上限".to_string()))
    }

    /// 单目标客户端（M7 站点对：worker 按 `sites` 行动态构造；id 固定为 "site"）。
    pub fn single(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Result<Self, ReplError> {
        let remote = remote_of(endpoint, access_key, secret_key, region, None, None, true)
            .ok_or_else(|| ReplError::permanent(format!("site endpoint 非法：{endpoint}")))?;
        let mut remotes = HashMap::new();
        remotes.insert("site".to_string(), remote);
        Ok(Self {
            remotes,
            tls: plain_tls(),
        })
    }

    pub async fn put_object(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        body: Bytes,
        expected_etag: &str,
    ) -> Result<(), ReplError> {
        self.put_object_with_meta(
            remote_id,
            dest_bucket,
            key,
            body,
            expected_etag,
            None,
            &BTreeMap::new(),
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .map(|_| ())
    }

    /// M7 站点 PUT：透传 content-type/user-metadata，并可要求响应带真实 version-id
    /// （Suspended/Off 目标返回 "null" → 按可重试处理——P2）。
    /// M9：`site_token`/`origin_version_id` 进 SigV4 SignedHeaders（sync 摄入校验+同 id），
    /// tags/storage-class/checksum 一并透传（远端重算校验后落同值）；
    /// 返回远端 `x-amz-version-id`（无则 None）。
    #[allow(clippy::too_many_arguments)]
    pub async fn put_object_with_meta(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        body: Bytes,
        expected_etag: &str,
        content_type: Option<&str>,
        user_metadata: &BTreeMap<String, String>,
        tags: Option<&[xl_store::meta::ObjectTag]>,
        storage_class: Option<&str>,
        checksum: Option<&xl_store::meta::ChecksumRecord>,
        require_version: bool,
        site_token: Option<&str>,
        origin_version_id: Option<&str>,
        part_sizes: Option<&[u64]>,
        sse: Option<&xl_store::meta::EncryptionRecord>,
        lock: Option<&xl_store::meta::ObjectLockRecord>,
    ) -> Result<Option<String>, ReplError> {
        let mut headers: Vec<(String, String)> = Vec::new();
        if let Some(ct) = content_type {
            headers.push(("content-type".into(), ct.to_string()));
        }
        if let Some(t) = site_token {
            headers.push(("x-oss-site-token".into(), t.to_string()));
        }
        if let Some(v) = origin_version_id {
            headers.push(("x-oss-origin-version-id".into(), v.to_string()));
        }
        // 4.1：MPU 段长表随对象到目标端，partNumber 在副本上仍可定位
        if let Some(sizes) = part_sizes.filter(|s| !s.is_empty()) {
            let v = sizes
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            headers.push(("x-oss-part-sizes".into(), v));
        }
        // 4.17：SSE-S3 以 AES256 下发，目标用本地主密钥重新封装（key 材料不跨站）。
        // SSE-C（客户钥不落盘）与 SSE-KMS（key_id 站点本地、目标 400 拒绝显式 CMK）无法
        // 保持语义 → fail closed，绝不静默降级为明文或换钥。
        if let Some(enc) = sse {
            match enc.key_id.as_str() {
                "file" => {
                    headers.push(("x-amz-server-side-encryption".into(), "AES256".to_string()))
                }
                "sse-c" => {
                    return Err(ReplError::permanent(
                        "SSE-C 源对象不支持复制（客户钥不落盘）",
                    ));
                }
                other => {
                    return Err(ReplError::permanent(format!(
                        "SSE-KMS 源对象不支持站点复制（key_id={other} 为站点本地主密钥，\
                         目标不接受显式 CMK）"
                    )));
                }
            }
        }
        // 4.17：对象锁随版本复制（目标桶锁配置由控制面同步；未开锁会 400 → 死信 fail-closed）。
        if let Some(l) = lock {
            if let (Some(mode), Some(until)) = (&l.mode, l.retain_until_nanos) {
                let dt = time::OffsetDateTime::from_unix_timestamp_nanos(until as i128)
                    .map_err(|e| ReplError::permanent(format!("retention 时间非法：{e}")))?;
                let date = dt
                    .format(&time::format_description::well_known::Rfc3339)
                    .map_err(|e| ReplError::permanent(format!("retention 格式化失败：{e}")))?;
                headers.push(("x-amz-object-lock-mode".into(), mode.clone()));
                headers.push(("x-amz-object-lock-retain-until-date".into(), date));
            }
            if l.legal_hold {
                headers.push(("x-amz-object-lock-legal-hold".into(), "ON".into()));
            }
        }
        if let Some(tags) = tags.filter(|t| !t.is_empty()) {
            let q = tags
                .iter()
                .map(|t| format!("{}={}", pct(&t.key), pct(&t.value)))
                .collect::<Vec<_>>()
                .join("&");
            headers.push(("x-amz-tagging".into(), q));
        }
        if let Some(sc) = storage_class {
            headers.push(("x-amz-storage-class".into(), sc.to_string()));
        }
        if let Some(c) = checksum {
            let h = if c.algorithm.eq_ignore_ascii_case("MD5") {
                "content-md5".to_string()
            } else {
                format!("x-amz-checksum-{}", c.algorithm.to_ascii_lowercase())
            };
            headers.push((h, c.value_base64.clone()));
        }
        for (k, v) in user_metadata {
            let k = k.to_ascii_lowercase();
            if k.is_empty()
                || !k
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
            {
                continue; // 非法元数据名：丢弃而非让整次复制永久失败
            }
            headers.push((format!("x-amz-meta-{k}"), v.clone()));
        }
        // P3-8：lowercase 后重名的头只保留一个（SignedHeaders 与线上头必须一致）。
        headers.sort_by(|a, b| a.0.cmp(&b.0));
        headers.dedup_by(|a, b| a.0 == b.0);
        let resp = self
            .send(
                remote_id,
                "PUT",
                dest_bucket,
                Some(key),
                &[],
                Some(body),
                &headers,
                Some("REPLICA"),
            )
            .await?;
        // 单段源（32 hex）断言目标 ETag；MPU 源跳过（重算口径，记档）
        if is_single_part_etag(expected_etag) {
            let got = resp
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim_matches('"'));
            if got != Some(expected_etag) {
                return Err(ReplError::permanent(format!(
                    "目标 ETag 不一致：期望 {expected_etag:?}，实得 {got:?}"
                )));
            }
        }
        let vid = resp
            .headers()
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        if require_version && vid.as_deref().is_none_or(|v| v.is_empty() || v == "null") {
            return Err(ReplError::retryable(
                "目标返回空/null version-id（versioning 未就绪，稍后重试）",
            ));
        }
        Ok(vid)
    }

    /// M9：按版本 id 定点删除（同步门补偿）。
    /// 走站点内部路由（token 认证、只做本地删除、永不转发——普通 S3 DELETE 会回环）；
    /// 404 = 路由/对象不存在 → 视为已删。
    pub async fn delete_object_version(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        version_id: &str,
        site_token: Option<&str>,
    ) -> Result<(), ReplError> {
        let token = site_token.ok_or_else(|| ReplError::permanent("site token required"))?;
        let body = serde_json::json!({
            "bucket": dest_bucket, "key": key, "version_id": version_id,
        })
        .to_string();
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("x-oss-site-token".to_string(), token.to_string()),
        ];
        match self
            .send_path(
                remote_id,
                "POST",
                "/api/v1/site/delete-object",
                &[],
                Some(Bytes::from(body)),
                &headers,
                None,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.status == Some(404) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// M9 S3：远端创建同 id delete marker（内部路由，只做本地 marker，永不转发）。
    /// 404 = 路由/版本不存在 → 视为已删（幂等）。
    pub async fn delete_marker(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        marker_id: &str,
        site_token: Option<&str>,
    ) -> Result<(), ReplError> {
        let token = site_token.ok_or_else(|| ReplError::permanent("site token required"))?;
        let body = serde_json::json!({
            "bucket": dest_bucket, "key": key, "marker_id": marker_id,
        })
        .to_string();
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("x-oss-site-token".to_string(), token.to_string()),
        ];
        match self
            .send_path(
                remote_id,
                "POST",
                "/api/v1/site/delete-object",
                &[],
                Some(Bytes::from(body)),
                &headers,
                None,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.status == Some(404) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// M9 S3：远端整键硬删（控制台对象删除；内部路由，只做本地清理）。
    pub async fn delete_object_all(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        site_token: Option<&str>,
    ) -> Result<(), ReplError> {
        let token = site_token.ok_or_else(|| ReplError::permanent("site token required"))?;
        let body = serde_json::json!({
            "bucket": dest_bucket, "key": key, "all_versions": true,
        })
        .to_string();
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("x-oss-site-token".to_string(), token.to_string()),
        ];
        match self
            .send_path(
                remote_id,
                "POST",
                "/api/v1/site/delete-object",
                &[],
                Some(Bytes::from(body)),
                &headers,
                None,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.status == Some(404) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// M9 S3：远端版本 HEAD（token 路由）。`Ok(true)` = 版本存在、`Ok(false)` = delete marker；
    /// 404 由调用方按「不存在」处理（错误对象带 status=404）。
    pub async fn head_object_version(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        version_id: &str,
        site_token: Option<&str>,
    ) -> Result<bool, ReplError> {
        let token = site_token.ok_or_else(|| ReplError::permanent("site token required"))?;
        let headers = vec![("x-oss-site-token".to_string(), token.to_string())];
        match self
            .send_path(
                remote_id,
                "HEAD",
                "/api/v1/site/head-object",
                &[
                    ("bucket", dest_bucket),
                    ("key", key),
                    ("version_id", version_id),
                ],
                None,
                &headers,
                None,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if e.status == Some(405) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// M9 S3：远端桶空检查（内部路由）。`Ok(true)` = 空；409 = 非空。
    pub async fn bucket_is_empty(
        &self,
        remote_id: &str,
        bucket: &str,
        site_token: Option<&str>,
    ) -> Result<bool, ReplError> {
        let token = site_token.ok_or_else(|| ReplError::permanent("site token required"))?;
        let headers = vec![("x-oss-site-token".to_string(), token.to_string())];
        match self
            .send_path(
                remote_id,
                "GET",
                "/api/v1/site/bucket-empty",
                &[("bucket", bucket)],
                None,
                &headers,
                None,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if e.status == Some(409) => Ok(false),
            // 桶已不存在也算空（删除目标已达成）。
            Err(e) if e.status == Some(404) => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// M9 S3：对端 quiesce（置 deleting + 等 active-writer 归零，30s 上限）。
    pub async fn bucket_quiesce(
        &self,
        remote_id: &str,
        bucket: &str,
        site_token: Option<&str>,
    ) -> Result<(), ReplError> {
        let token = site_token.ok_or_else(|| ReplError::permanent("site token required"))?;
        let body = serde_json::json!({ "bucket": bucket }).to_string();
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("x-oss-site-token".to_string(), token.to_string()),
        ];
        self.send_path(
            remote_id,
            "POST",
            "/api/v1/site/bucket-quiesce",
            &[],
            Some(Bytes::from(body)),
            &headers,
            None,
        )
        .await
        .map(|_| ())
    }

    /// 回滚对端 quiesce（超时/非空/失败的补偿路径）。
    pub async fn bucket_quiesce_release(
        &self,
        remote_id: &str,
        bucket: &str,
        site_token: Option<&str>,
    ) -> Result<(), ReplError> {
        let token = site_token.ok_or_else(|| ReplError::permanent("site token required"))?;
        let body = serde_json::json!({ "bucket": bucket, "release": true }).to_string();
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("x-oss-site-token".to_string(), token.to_string()),
        ];
        self.send_path(
            remote_id,
            "POST",
            "/api/v1/site/bucket-quiesce",
            &[],
            Some(Bytes::from(body)),
            &headers,
            None,
        )
        .await
        .map(|_| ())
    }

    /// M9 S3：远端本地删桶（内部路由；404 = 已删，幂等成功）。
    pub async fn delete_bucket(
        &self,
        remote_id: &str,
        bucket: &str,
        site_token: Option<&str>,
    ) -> Result<(), ReplError> {
        let token = site_token.ok_or_else(|| ReplError::permanent("site token required"))?;
        let body = serde_json::json!({ "bucket": bucket }).to_string();
        let headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("x-oss-site-token".to_string(), token.to_string()),
        ];
        match self
            .send_path(
                remote_id,
                "POST",
                "/api/v1/site/delete-bucket",
                &[],
                Some(Bytes::from(body)),
                &headers,
                None,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.status == Some(404) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn delete_object(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
    ) -> Result<(), ReplError> {
        // P0-2：DELETE 也带 REPLICA 头（双向配对时目标端不得回环再入队）。
        self.send(
            remote_id,
            "DELETE",
            dest_bucket,
            Some(key),
            &[],
            None,
            &[],
            Some("REPLICA"),
        )
        .await?;
        Ok(())
    }

    /// 4.16：站点对 marker 同 id 创建——标准 S3 DELETE（无 versionId）+ REPLICA +
    /// site token + origin id；目标服务在 token 认证下用 origin id 生成 marker。
    /// 这样源端后续 `DELETE ?versionId=<marker id>` 才能在目标定点命中。
    pub async fn delete_marker_s3(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        marker_id: &str,
        site_token: &str,
    ) -> Result<(), ReplError> {
        let headers = vec![
            ("x-oss-site-token".to_string(), site_token.to_string()),
            ("x-oss-origin-version-id".to_string(), marker_id.to_string()),
        ];
        self.send(
            remote_id,
            "DELETE",
            dest_bucket,
            Some(key),
            &[],
            None,
            &headers,
            Some("REPLICA"),
        )
        .await?;
        Ok(())
    }

    /// 4.16：站点对版本定向删除——标准 S3 `DELETE ?versionId=`，带 REPLICA（目标不再入队）。
    /// 目标版本不存在 = 已删（幂等；含升级前副本 id 不一致的旧版本，记日志由上层处理）。
    pub async fn delete_object_version_s3(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(), ReplError> {
        let vid = pct(version_id);
        // 源端删除已通过其锁校验（含 governance bypass），目标以同值 WORM 记录复核；
        // 带 bypass 头保证「源端允许的删除」不会因目标端 governance 二次拦截而死信。
        let headers = vec![(
            "x-amz-bypass-governance-retention".to_string(),
            "true".to_string(),
        )];
        match self
            .send(
                remote_id,
                "DELETE",
                dest_bucket,
                Some(key),
                &[("versionId", vid.as_str())],
                None,
                &headers,
                Some("REPLICA"),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.status == Some(404) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// M7 R6：目标桶 versioning 就绪探询（签名 GET `/{bucket}?versioning`）。
    /// 404 归一为可重试（控制面/物理桶尚未到达），其余错误按状态分类。
    pub async fn get_bucket_versioning(
        &self,
        remote_id: &str,
        bucket: &str,
    ) -> Result<String, ReplError> {
        let resp = self
            .send(
                remote_id,
                "GET",
                bucket,
                None,
                &[("versioning", "")],
                None,
                &[],
                None,
            )
            .await
            .map_err(|e| {
                if e.status == Some(404) {
                    ReplError::retryable(format!("目标桶不存在（versioning 探询）：{}", e.message))
                } else {
                    e
                }
            })?;
        let mut body = Vec::new();
        let mut incoming = resp.into_body();
        while body.len() < 4096 {
            match incoming.frame().await {
                Some(Ok(f)) => {
                    if let Some(d) = f.data_ref() {
                        let take = (4096 - body.len()).min(d.len());
                        body.extend_from_slice(&d[..take]);
                    }
                }
                _ => break,
            }
        }
        let text = String::from_utf8_lossy(&body);
        let status = text
            .split("<Status>")
            .nth(1)
            .and_then(|s| s.split("</Status>").next())
            .unwrap_or("Off")
            .trim();
        Ok(status.to_string())
    }

    #[allow(clippy::too_many_arguments)]
    async fn send(
        &self,
        remote_id: &str,
        method: &str,
        dest_bucket: &str,
        key: Option<&str>,
        query: &[(&str, &str)],
        body: Option<Bytes>,
        headers: &[(String, String)],
        replication_status: Option<&'static str>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, ReplError> {
        let decoded_path = match key {
            Some(k) => format!("/{dest_bucket}/{k}"),
            None => format!("/{dest_bucket}"),
        };
        self.send_path(
            remote_id,
            method,
            &decoded_path,
            query,
            body,
            headers,
            replication_status,
        )
        .await
    }

    /// 站点内部路由（非 S3 路径）用：签名/连接与 `send` 同源，路径由调用方给。
    #[allow(clippy::too_many_arguments)]
    async fn send_path(
        &self,
        remote_id: &str,
        method: &str,
        decoded_path: &str,
        query: &[(&str, &str)],
        body: Option<Bytes>,
        headers: &[(String, String)],
        replication_status: Option<&'static str>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, ReplError> {
        self.send_path_impl(
            remote_id,
            method,
            decoded_path,
            query,
            OutBody::Bytes(body.unwrap_or_default()),
            headers,
            replication_status,
        )
        .await
    }

    /// `send_path` 的流式/整块共用实现：签名只做一次，body 在最后一段才分派。
    #[allow(clippy::too_many_arguments)]
    async fn send_path_impl(
        &self,
        remote_id: &str,
        method: &str,
        decoded_path: &str,
        query: &[(&str, &str)],
        body: OutBody,
        headers: &[(String, String)],
        replication_status: Option<&'static str>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, ReplError> {
        let remote = self.remotes.get(remote_id).ok_or_else(|| {
            ReplError::permanent(format!("remote {remote_id:?} 未配置（规则校验应已拦截）"))
        })?;
        let (https, host, port) = if remote.peer {
            let url = self
                .tls
                .peer_url(&remote.endpoint)
                .map_err(ReplError::permanent)?;
            let ep = crate::tls_config::parse_endpoint(&url, "site endpoint")
                .map_err(ReplError::permanent)?;
            (ep.https, ep.host, ep.port)
        } else {
            (remote.https, remote.host.clone(), remote.port)
        };

        // M8 缺口 C：按远端独立令牌桶整形（GET/HEAD/DELETE 无 body 不限）
        let body_len = body.len();
        if body_len > 0
            && let Some(l) = &remote.limiter
        {
            l.acquire(body_len).await;
        }
        // 流式文件 body 的 SHA-256 已由调用方边写边算（SigV4 要求预先给出）。
        let payload_hex = body.sha256_hex();
        let amz = amz_date_now()?;
        let mut signed: Vec<(String, String)> = vec![
            ("host".into(), host.clone()),
            ("x-amz-content-sha256".into(), payload_hex.clone()),
            ("x-amz-date".into(), amz.fmt_iso8601().to_string()),
        ];
        if let Some(rs) = replication_status {
            signed.push(("x-amz-replication-status".into(), rs.to_string()));
        }
        signed.extend(headers.iter().cloned());
        signed.sort_by(|a, b| a.0.cmp(&b.0));
        let signed_refs: Vec<(&str, &str)> = signed
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let payload = Payload::SingleChunk(payload_hex.as_str());
        let canonical =
            create_canonical_request(method, decoded_path, query, &signed_refs, payload);
        // 规范请求的第 2 行是编码后的 path、第 3 行是编码后的 query（可为空）；
        // 实际请求 URI 必须把两者拼回去（M6 只取第 2 行——加 query 后会丢签名参数）。
        let mut clines = canonical.lines();
        let _ = clines.next();
        let enc_path = clines
            .next()
            .ok_or_else(|| ReplError::permanent("规范请求缺少 URI 行"))?;
        let enc_query = clines.next().unwrap_or("");
        let encoded_path = if enc_query.is_empty() {
            enc_path.to_string()
        } else {
            format!("{enc_path}?{enc_query}")
        };
        let sts = create_string_to_sign(&canonical, &amz, &remote.region, "s3");
        let sig = calculate_signature(&sts, &remote.secret_key, &amz, &remote.region, "s3");
        let signed_names: Vec<&str> = signed.iter().map(|(k, _)| k.as_str()).collect();
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request, SignedHeaders={}, Signature={}",
            remote.access_key,
            amz.fmt_date(),
            remote.region,
            signed_names.join(";"),
            sig
        );
        let scheme = if https { "https" } else { "http" };
        let uri = Uri::from_str(&format!("{scheme}://{host}:{port}{encoded_path}"))
            .map_err(|e| ReplError::permanent(format!("请求 URI 构造失败：{e}")))?;
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in &signed {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("authorization", auth);

        let ctx = SendCtx {
            https,
            host,
            port,
            server_name: remote.server_name.clone(),
        };
        match body {
            OutBody::Bytes(bytes) => {
                let req = req
                    .body(Full::new(bytes))
                    .map_err(|e| ReplError::permanent(format!("请求构造失败：{e}")))?;
                self.dispatch(ctx, req).await
            }
            OutBody::File { path, size, .. } => {
                // Content-Length 显式给出：hyper 才能用定长 body（S3 不依赖 chunked）。
                let body = file_body(path, size).await?;
                let req = req
                    .header("content-length", size.to_string())
                    .body(body)
                    .map_err(|e| ReplError::permanent(format!("请求构造失败：{e}")))?;
                self.dispatch(ctx, req).await
            }
        }
    }

    /// 连接 + 发送 + 状态分类（body 类型由调用方决定）。
    async fn dispatch<B>(
        &self,
        ctx: SendCtx,
        req: Request<B>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, ReplError>
    where
        B: hyper::body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let stream = self
            .tls
            .connect(ctx.https, &ctx.host, ctx.port, ctx.server_name.as_deref())
            .await
            .map_err(ReplError::retryable)?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| ReplError::retryable(format!("HTTP1 握手失败：{e}")))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let resp = sender
            .send_request(req)
            .await
            .map_err(|e| ReplError::retryable(format!("发送请求失败：{e}")))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        // 错误体最多读 300 字节（远端超大错误体不得撑内存）
        let mut body = Vec::new();
        let mut incoming = resp.into_body();
        while body.len() < 300 {
            match incoming.frame().await {
                Some(Ok(f)) => {
                    if let Some(d) = f.data_ref() {
                        let take = (300 - body.len()).min(d.len());
                        body.extend_from_slice(&d[..take]);
                    }
                }
                _ => break,
            }
        }
        let text = String::from_utf8_lossy(&body);
        Err(classify_status(status, &text))
    }
}

/// 请求体：整块 Bytes，或已落盘、带预计算哈希的暂存文件（tier 上传）。
enum OutBody {
    Bytes(Bytes),
    File {
        path: PathBuf,
        size: u64,
        sha256: String,
    },
}

impl OutBody {
    fn len(&self) -> u64 {
        match self {
            OutBody::Bytes(b) => b.len() as u64,
            OutBody::File { size, .. } => *size,
        }
    }

    fn sha256_hex(&self) -> String {
        match self {
            OutBody::Bytes(b) => hex::encode(sha2::Sha256::digest(b)),
            OutBody::File { sha256, .. } => sha256.clone(),
        }
    }
}

struct SendCtx {
    https: bool,
    host: String,
    port: u16,
    server_name: Option<String>,
}

/// 暂存文件 → hyper body 流（256 KiB 块；不整份驻留内存）。
async fn file_body(
    path: PathBuf,
    size: u64,
) -> Result<
    StreamBody<impl futures::Stream<Item = Result<Frame<Bytes>, std::io::Error>> + Send + 'static>,
    ReplError,
> {
    let f = tokio::fs::File::open(&path)
        .await
        .map_err(|e| ReplError::retryable(format!("tier 暂存文件不可读：{e}")))?;
    if f.metadata().await.map(|m| m.len()).unwrap_or(size) != size {
        return Err(ReplError::permanent("tier 暂存文件长度与声明不符"));
    }
    let stream = futures::stream::try_unfold(f, |mut f| async move {
        let mut buf = vec![0u8; 256 * 1024];
        match f.read(&mut buf).await {
            Ok(0) => Ok(None),
            Ok(n) => {
                buf.truncate(n);
                Ok(Some((Frame::data(Bytes::from(buf)), f)))
            }
            Err(e) => Err(e),
        }
    });
    Ok(StreamBody::new(stream))
}

fn classify_status(status: StatusCode, text: &str) -> ReplError {
    let msg = format!("目标返回 {}：{text}", status.as_u16());
    let class = if status.as_u16() == 408 || status.as_u16() == 429 || status.is_server_error() {
        ErrClass::Retryable
    } else {
        ErrClass::Permanent
    };
    ReplError {
        class,
        message: msg,
        status: Some(status.as_u16()),
    }
}

fn is_single_part_etag(etag: &str) -> bool {
    etag.len() == 32 && etag.bytes().all(|b| b.is_ascii_hexdigit())
}

/// `x-amz-tagging` 是 URL query 串；对端 `sts::parse_form` 解码 `%XX`/`+`。
fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn amz_date_now() -> Result<AmzDate, ReplError> {
    let now = time::OffsetDateTime::now_utc();
    let s = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );
    AmzDate::parse(&s).map_err(|_| ReplError::permanent("x-amz-date 构造失败"))
}

struct ListPage {
    contents: Vec<TierObject>,
    is_truncated: bool,
    next_token: Option<String>,
}

/// 最小 ListBucketResult 解析（不引 XML 依赖；只取 GC 需要的字段）。
fn parse_list_page(xml: &str) -> Result<ListPage, ReplError> {
    let mut contents = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find("<Contents>") {
        let after = &rest[i + "<Contents>".len()..];
        let Some(j) = after.find("</Contents>") else {
            break;
        };
        let block = &after[..j];
        let key = tag_text(block, "Key")
            .map(xml_unescape)
            .ok_or_else(|| ReplError::permanent("tier list 缺 Key".to_string()))?;
        let last_modified = tag_text(block, "LastModified")
            .and_then(parse_rfc3339)
            .ok_or_else(|| ReplError::permanent(format!("tier list LastModified 非法：{key}")))?;
        let size = tag_text(block, "Size")
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| ReplError::permanent(format!("tier list Size 非法：{key}")))?;
        contents.push(TierObject {
            key,
            last_modified,
            size,
        });
        rest = &after[j + "</Contents>".len()..];
    }
    Ok(ListPage {
        contents,
        is_truncated: tag_text(xml, "IsTruncated") == Some("true"),
        next_token: tag_text(xml, "NextContinuationToken").map(xml_unescape),
    })
}

fn tag_text<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn parse_rfc3339(s: &str) -> Option<i64> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|t| t.unix_timestamp())
}

/// 构造签名的纯函数（单测用，不联网）。
#[cfg(test)]
fn canonical_for_test(
    method: &str,
    path: &str,
    host: &str,
    payload_hex: &str,
    replication_status: Option<&str>,
) -> String {
    let mut signed: Vec<(&str, &str)> = vec![
        ("host", host),
        ("x-amz-content-sha256", payload_hex),
        ("x-amz-date", "20260101T000000Z"),
    ];
    if let Some(rs) = replication_status {
        signed.push(("x-amz-replication-status", rs));
    }
    signed.sort_by(|a, b| a.0.cmp(b.0));
    create_canonical_request(
        method,
        path,
        &[] as &[(&str, &str)],
        signed,
        Payload::SingleChunk(payload_hex),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(id: &str, max: Option<u64>) -> RemoteTarget {
        RemoteTarget {
            id: id.into(),
            endpoint: "http://127.0.0.1:1".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            region: "us-east-1".into(),
            max_bytes_per_sec: max,
            server_name: None,
        }
    }

    /// M8 缺口 C：令牌桶——初始 1s 满桶、按时间补桶、容量封顶 1s、超额部分取走剩余。
    #[test]
    fn token_bucket_refill_and_burst_cap() {
        let mut b = TokenBucket::new(1000);
        let t0 = Instant::now();
        assert_eq!(b.drain(1000, t0), 0, "初始满桶可整取");
        assert_eq!(b.drain(500, t0), 500, "空桶时全部欠账");
        let t1 = t0 + Duration::from_millis(250);
        assert_eq!(b.drain(500, t1), 250, "补 250 后仍欠 250");
        let t2 = t0 + Duration::from_secs(10);
        assert_eq!(b.drain(1000, t2), 0, "空闲再久也只攒 1 秒的量");
        assert_eq!(b.drain(1, t2), 1);
        // 大于容量的对象分段落账：先取走现有，再欠剩余（不会死等）
        let mut b = TokenBucket::new(1000);
        let t3 = Instant::now();
        assert_eq!(b.drain(2500, t3), 1500);
    }

    /// M8 缺口 C：每个 remote 独立桶——耗尽 r1 不影响 r2。
    #[test]
    fn rate_limiters_are_per_remote() {
        let c = ReplClient::new(&[target("r1", Some(1024)), target("r2", Some(1024))]);
        let r1 = c.remotes["r1"].limiter.as_ref().unwrap();
        let r2 = c.remotes["r2"].limiter.as_ref().unwrap();
        assert_eq!(
            r1.bucket.lock().unwrap().drain(1024, Instant::now()),
            0,
            "r1 一次取空 1 秒额度"
        );
        assert_eq!(
            r2.bucket.lock().unwrap().drain(1024, Instant::now()),
            0,
            "r1 的消耗不得影响 r2"
        );
        assert_eq!(
            r1.bucket.lock().unwrap().drain(1, Instant::now()),
            1,
            "r1 已空"
        );
        // 未配置 limit → 无桶
        let c = ReplClient::new(&[target("r3", None)]);
        assert!(c.remotes["r3"].limiter.is_none());
    }

    #[test]
    fn canonical_includes_replication_header() {
        let c = canonical_for_test(
            "PUT",
            "/dst/a b.txt",
            "127.0.0.1:9000",
            "abc",
            Some("REPLICA"),
        );
        assert!(c.contains("x-amz-replication-status:REPLICA"), "{c}");
        assert!(
            c.contains("host;x-amz-content-sha256;x-amz-date;x-amz-replication-status"),
            "{c}"
        );
        // URI 行已 AWS 编码（空格 → %20）
        assert!(c.lines().nth(1).unwrap().contains("a%20b.txt"), "{c}");
        // 未传入时省略该头（纯函数行为；线上 DELETE 现在也带 REPLICA——P0-2）
        let c = canonical_for_test("DELETE", "/dst/k", "h:1", "abc", None);
        assert!(!c.contains("x-amz-replication-status"), "{c}");
    }

    /// GC 依赖 list 的 key/时间/续点；XML 实体与下一页标记必须正确解析。
    #[test]
    fn tier_list_page_parses_contents_and_token() {
        let xml = r#"<?xml version="1.0"?>
<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>tok+1/2=</NextContinuationToken>
  <Contents>
    <Key>cold/bkt/v1/a&amp;b.txt</Key>
    <LastModified>2026-10-20T01:02:03.000Z</LastModified>
    <Size>42</Size>
  </Contents>
</ListBucketResult>"#;
        let page = parse_list_page(xml).unwrap();
        assert!(page.is_truncated);
        assert_eq!(page.next_token.as_deref(), Some("tok+1/2="));
        assert_eq!(page.contents.len(), 1);
        assert_eq!(page.contents[0].key, "cold/bkt/v1/a&b.txt");
        assert_eq!(page.contents[0].size, 42);
        assert_eq!(
            page.contents[0].last_modified,
            time::OffsetDateTime::parse(
                "2026-10-20T01:02:03Z",
                &time::format_description::well_known::Rfc3339
            )
            .unwrap()
            .unix_timestamp()
        );

        // 缺 Key/Size/LastModified = 非法响应（GC fail closed）。
        assert!(parse_list_page("<Contents><Size>1</Size></Contents>").is_err());
        assert!(parse_list_page("<Contents><Key>k</Key></Contents>").is_err());
    }

    #[test]
    fn status_classification_retryable_vs_permanent() {
        for s in [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::NOT_IMPLEMENTED,
        ] {
            assert_eq!(classify_status(s, "x").class, ErrClass::Retryable, "{s}");
        }
        for s in [
            StatusCode::BAD_REQUEST,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::METHOD_NOT_ALLOWED,
        ] {
            assert_eq!(classify_status(s, "x").class, ErrClass::Permanent, "{s}");
        }
    }
}
