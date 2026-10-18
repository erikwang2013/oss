//! M6 切片 4：复制出站客户端——SigV4（s3s-sigv4 原语）+ hyper http1 直连。
//!
//! - 仅 http（TLS 未接线，配置层拒绝 https——记档）。
//! - PUT 携带 `x-amz-replication-status: REPLICA`，且该头**纳入 SignedHeaders**（s3s 服务端
//!   对未签名的 x-amz-* 返回 403）。
//! - 单段源（32 hex ETag）断言响应 ETag；MPU 源 ETag 非 MD5，跳过断言（目标重算）。

use std::collections::HashMap;
use std::str::FromStr as _;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use s3s_sigv4::{
    AmzDate, Payload, calculate_signature, create_canonical_request, create_string_to_sign,
};
use sha2::Digest as _;
use tokio::net::TcpStream;

use crate::config::RemoteTarget;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrClass {
    Retryable,
    Permanent,
}

#[derive(Debug)]
pub struct ReplError {
    pub class: ErrClass,
    pub message: String,
}

impl ReplError {
    pub fn retryable(msg: impl Into<String>) -> Self {
        Self {
            class: ErrClass::Retryable,
            message: msg.into(),
        }
    }
    pub fn permanent(msg: impl Into<String>) -> Self {
        Self {
            class: ErrClass::Permanent,
            message: msg.into(),
        }
    }
}

struct Remote {
    uri: Uri,
    access_key: String,
    secret_key: String,
    region: String,
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
                    },
                ))
            })
            .collect();
        Self { remotes }
    }

    pub async fn put_object(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
        body: Bytes,
        expected_etag: &str,
    ) -> Result<(), ReplError> {
        let resp = self
            .send(
                remote_id,
                "PUT",
                dest_bucket,
                key,
                Some(body),
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
        Ok(())
    }

    pub async fn delete_object(
        &self,
        remote_id: &str,
        dest_bucket: &str,
        key: &str,
    ) -> Result<(), ReplError> {
        self.send(remote_id, "DELETE", dest_bucket, key, None, None)
            .await?;
        Ok(())
    }

    async fn send(
        &self,
        remote_id: &str,
        method: &str,
        dest_bucket: &str,
        key: &str,
        body: Option<Bytes>,
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
        let payload_hex = hex::encode(sha2::Sha256::digest(&body_bytes));
        let amz = amz_date_now()?;
        let mut signed: Vec<(&str, String)> = vec![
            ("host", host.clone()),
            ("x-amz-content-sha256", payload_hex.clone()),
            ("x-amz-date", amz.fmt_iso8601().to_string()),
        ];
        if let Some(rs) = replication_status {
            signed.push(("x-amz-replication-status", rs.to_string()));
        }
        signed.sort_by(|a, b| a.0.cmp(b.0));
        let signed_refs: Vec<(&str, &str)> = signed.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let decoded_path = format!("/{dest_bucket}/{key}");
        let payload = Payload::SingleChunk(payload_hex.as_str());
        let canonical = create_canonical_request(
            method,
            &decoded_path,
            &[] as &[(&str, &str)],
            &signed_refs,
            payload,
        );
        // 规范请求第二行即 AWS 编码后的 URI（与实际请求必须逐字节一致）
        let encoded_path = canonical
            .lines()
            .nth(1)
            .ok_or_else(|| ReplError::permanent("规范请求缺少 URI 行"))?
            .to_string();
        let sts = create_string_to_sign(&canonical, &amz, &remote.region, "s3");
        let sig = calculate_signature(&sts, &remote.secret_key, &amz, &remote.region, "s3");
        let signed_names: Vec<&str> = signed.iter().map(|(k, _)| *k).collect();
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
            req = req.header(*k, v);
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
    if status.as_u16() == 408 || status.as_u16() == 429 || status.is_server_error() {
        ReplError::retryable(msg)
    } else {
        ReplError::permanent(msg)
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
        // DELETE 不带该头
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
