//! M6 切片 4：复制出站客户端——SigV4（s3s-sigv4 原语）+ hyper http1 直连。
//!
//! - 仅 http（TLS 未接线，配置层拒绝 https——记档）。
//! - PUT 携带 `x-amz-replication-status: REPLICA`，且该头**纳入 SignedHeaders**（s3s 服务端
//!   对未签名的 x-amz-* 返回 403）。
//! - 单段源（32 hex ETag）断言响应 ETag；MPU 源 ETag 非 MD5，跳过断言（目标重算）。

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr as _;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::Digest as _;
use tokio::net::TcpStream;

use crate::config::{RemoteTarget, TierTarget};

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
    uri: Uri,
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

/// 出站客户端（remotes 静态映射；连接每请求新建——复制频率低，无池化）。
pub struct ReplClient {
    remotes: HashMap<String, Remote>,
}

impl ReplClient {
    pub fn new(targets: &[RemoteTarget]) -> Self {
        let remotes = targets
            .iter()
            .filter_map(|t| {
                let uri = t.endpoint.parse::<Uri>().ok()?;
                Some((
                    t.id.clone(),
                    Remote {
                        uri,
                        access_key: t.access_key.clone(),
                        secret_key: t.secret_key.clone(),
                        region: t.region.clone(),
                        limiter: limiter_of(t),
                    },
                ))
            })
            .collect();
        Self { remotes }
    }

    /// M8 S4b：tier 目标（同 `[[remotes]]` 形态 + 目标桶；id 即 tier id）。
    pub fn new_tiers(targets: &[TierTarget]) -> Self {
        let remotes = targets
            .iter()
            .filter_map(|t| {
                let uri = t.endpoint.parse::<Uri>().ok()?;
                Some((
                    t.id.clone(),
                    Remote {
                        uri,
                        access_key: t.access_key.clone(),
                        secret_key: t.secret_key.clone(),
                        region: t.region.clone(),
                        limiter: None,
                    },
                ))
            })
            .collect();
        Self { remotes }
    }

    /// tier PUT：`Content-MD5`（签入）并断言响应 ETag == 本地 MD5（远端 ETag 口径）。
    pub async fn tier_put(
        &self,
        remote_id: &str,
        bucket: &str,
        key: &str,
        body: Bytes,
        md5_hex: &str,
    ) -> Result<String, ReplError> {
        let raw = hex::decode(md5_hex).map_err(|_| ReplError::permanent("MD5 非法"))?;
        let md5_b64 =
            base64::engine::Engine::encode(&base64::engine::general_purpose::STANDARD, &raw);
        let headers = vec![("content-md5".to_string(), md5_b64)];
        let resp = self
            .send(
                remote_id,
                "PUT",
                bucket,
                Some(key),
                &[],
                Some(body),
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

    /// tier GET：整份读入（恢复作业随后校验 size/SHA-256）。
    pub async fn tier_get(
        &self,
        remote_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<Bytes, ReplError> {
        let resp = self
            .send(remote_id, "GET", bucket, Some(key), &[], None, &[], None)
            .await?;
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| ReplError::retryable(format!("读取 tier 响应体失败：{e}")))?;
        Ok(body.to_bytes())
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

    /// 单目标客户端（M7 站点对：worker 按 `sites` 行动态构造；id 固定为 "site"）。
    pub fn single(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Result<Self, ReplError> {
        let uri = endpoint
            .parse::<Uri>()
            .map_err(|e| ReplError::permanent(format!("site endpoint 非法：{e}")))?;
        let mut remotes = HashMap::new();
        remotes.insert(
            "site".to_string(),
            Remote {
                uri,
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                region: region.to_string(),
                limiter: None,
            },
        );
        Ok(Self { remotes })
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
            false,
        )
        .await
    }

    /// M7 站点 PUT：透传 content-type/user-metadata，并可要求响应带真实 version-id
    /// （Suspended/Off 目标返回 "null" → 按可重试处理——P2）。
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
        require_version: bool,
    ) -> Result<(), ReplError> {
        let mut headers: Vec<(String, String)> = Vec::new();
        if let Some(ct) = content_type {
            headers.push(("content-type".into(), ct.to_string()));
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
        if require_version {
            let vid = resp
                .headers()
                .get("x-amz-version-id")
                .and_then(|v| v.to_str().ok());
            if vid.is_none_or(|v| v.is_empty() || v == "null") {
                return Err(ReplError::retryable(
                    "目标返回空/null version-id（versioning 未就绪，稍后重试）",
                ));
            }
        }
        Ok(())
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
        let remote = self.remotes.get(remote_id).ok_or_else(|| {
            ReplError::permanent(format!("remote {remote_id:?} 未配置（规则校验应已拦截）"))
        })?;
        if remote.uri.scheme_str() != Some("http") {
            return Err(ReplError::permanent("仅支持 http remote（TLS 未接线）"));
        }
        let host = remote
            .uri
            .host()
            .ok_or_else(|| ReplError::permanent("remote endpoint 缺 host"))?
            .to_string();
        let port = remote.uri.port_u16().unwrap_or(80);

        // 签名
        let body_bytes = body.unwrap_or_default();
        // M8 缺口 C：按远端独立令牌桶整形（GET/HEAD/DELETE 无 body 不限）
        if !body_bytes.is_empty()
            && let Some(l) = &remote.limiter
        {
            l.acquire(body_bytes.len() as u64).await;
        }
        let payload_hex = hex::encode(sha2::Sha256::digest(&body_bytes));
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
        let decoded_path = match key {
            Some(k) => format!("/{dest_bucket}/{k}"),
            None => format!("/{dest_bucket}"),
        };
        let payload = Payload::SingleChunk(payload_hex.as_str());
        let canonical =
            create_canonical_request(method, &decoded_path, query, &signed_refs, payload);
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
        let uri = Uri::from_str(&format!("http://{host}:{port}{encoded_path}"))
            .map_err(|e| ReplError::permanent(format!("请求 URI 构造失败：{e}")))?;
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in &signed {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("authorization", auth);
        let req = req
            .body(Full::new(body_bytes))
            .map_err(|e| ReplError::permanent(format!("请求构造失败：{e}")))?;

        // 连接 + 发送
        let stream = TcpStream::connect(format!("{host}:{port}"))
            .await
            .map_err(|e| ReplError::retryable(format!("连接 {host}:{port} 失败：{e}")))?;
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
