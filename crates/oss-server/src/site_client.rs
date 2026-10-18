//! M7：站点控制面 HTTP 客户端（identity 校验 / ping；changelog 出货复用同一发送器）。
//! 仅 http（M7 信任模型同 `[[remotes]]`），每请求新建连接（控制面低频）。

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::Uri;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

pub const SITE_TOKEN_HEADER: &str = "x-oss-site-token";

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PeerIdentity {
    pub id: String,
    pub name: String,
    pub region: String,
    pub priority: i64,
    #[serde(default)]
    pub has_vault: bool,
}

/// 端点校验：仅 `http://host[:port]`，无 path/query/userinfo；返回去除尾斜杠的基址。
pub fn validate_endpoint(endpoint: &str) -> Result<String, String> {
    let trimmed = endpoint.trim_end_matches('/');
    let uri: Uri = trimmed
        .parse()
        .map_err(|_| format!("endpoint 非法：{endpoint}"))?;
    if uri.scheme_str() != Some("http") {
        return Err("endpoint 必须是 http://host[:port]（TLS 未接线）".into());
    }
    if uri.host().is_none() {
        return Err("endpoint 缺 host".into());
    }
    if uri.path() != "/" && !uri.path().is_empty() {
        return Err("endpoint 不得带 path".into());
    }
    if uri.query().is_some() {
        return Err("endpoint 不得带 query".into());
    }
    if uri.authority().is_some_and(|a| a.as_str().contains('@')) {
        return Err("endpoint 不得带 userinfo".into());
    }
    Ok(trimmed.to_string())
}

/// 控制面请求硬超时：单一出货 worker 不得被“接受 TCP 但不回响应”的对端永久挂起。
/// 30s 与 1MiB 批次预算匹配（慢链路可完成；坏对端仍能及时发现）。
const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

async fn send(
    endpoint: &str,
    method: &str,
    path: &str,
    token: &str,
    body: Option<Bytes>,
) -> Result<(u16, Bytes), String> {
    match tokio::time::timeout(
        SEND_TIMEOUT,
        send_inner(endpoint, method, path, token, body),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => Err(format!("站点控制面请求超时（{}s）", SEND_TIMEOUT.as_secs())),
    }
}

async fn send_inner(
    endpoint: &str,
    method: &str,
    path: &str,
    token: &str,
    body: Option<Bytes>,
) -> Result<(u16, Bytes), String> {
    let base = validate_endpoint(endpoint)?;
    let uri: Uri = format!("{base}{path}")
        .parse()
        .map_err(|e| format!("请求 URI 构造失败：{e}"))?;
    let host = uri
        .host()
        .ok_or_else(|| "endpoint 缺 host".to_string())?
        .to_string();
    let port = uri.port_u16().unwrap_or(80);
    let mut req = hyper::Request::builder()
        .method(method)
        .uri(uri)
        .header(SITE_TOKEN_HEADER, token);
    if body.is_some() {
        req = req.header("content-type", "application/json");
    }
    let req = req
        .body(Full::new(body.unwrap_or_default()))
        .map_err(|e| format!("请求构造失败：{e}"))?;

    let stream = TcpStream::connect(format!("{host}:{port}"))
        .await
        .map_err(|e| format!("连接 {host}:{port} 失败：{e}"))?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|e| format!("HTTP1 握手失败：{e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| format!("发送请求失败：{e}"))?;
    let status = resp.status().as_u16();
    // 错误体/响应体最多 64KiB
    let mut out = Vec::new();
    let mut incoming = resp.into_body();
    while out.len() < 64 * 1024 {
        match incoming.frame().await {
            Some(Ok(f)) => {
                if let Some(d) = f.data_ref() {
                    let take = (64 * 1024 - out.len()).min(d.len());
                    out.extend_from_slice(&d[..take]);
                }
            }
            _ => break,
        }
    }
    Ok((status, Bytes::from(out)))
}

pub async fn get_identity(endpoint: &str, token: &str) -> Result<PeerIdentity, String> {
    let (status, body) = send(endpoint, "GET", "/api/v1/site/identity", token, None).await?;
    if status != 200 {
        return Err(format!(
            "对端 identity 返回 {status}：{}",
            String::from_utf8_lossy(&body)
                .chars()
                .take(200)
                .collect::<String>()
        ));
    }
    serde_json::from_slice(&body).map_err(|e| format!("identity 解析失败：{e}"))
}

/// changelog 出货（切片 1b）：返回 (status, body)。
pub async fn post_changelog(
    endpoint: &str,
    token: &str,
    body: Bytes,
) -> Result<(u16, Bytes), String> {
    send(
        endpoint,
        "POST",
        "/api/v1/site/changelog",
        token,
        Some(body),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_validation() {
        assert_eq!(
            validate_endpoint("http://127.0.0.1:9001/").unwrap(),
            "http://127.0.0.1:9001"
        );
        assert!(validate_endpoint("https://h:1").is_err());
        assert!(validate_endpoint("http://h:1/path").is_err());
        assert!(validate_endpoint("http://h:1?x=1").is_err());
        assert!(validate_endpoint("http://u@h:1").is_err());
        assert!(validate_endpoint("ftp://h:1").is_err());
    }
}
