//! M7：站点控制面 HTTP 客户端（identity 校验 / ping；changelog 出货复用同一发送器）。
//! `[tls.server]` 存在时对端地址升级 https；每请求新建连接（控制面低频）。

use std::time::Duration;

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
    // ---- M9 S7.4 配对预检（缺省 = 旧 M7 identity） ----
    #[serde(default)]
    pub raft_enabled: bool,
    #[serde(default)]
    pub raft_role: Option<String>,
    #[serde(default)]
    pub raft_bootstrap: Option<bool>,
    #[serde(default)]
    pub raft_initialized: Option<bool>,
    #[serde(default)]
    pub raft_node_id: Option<u64>,
    #[serde(default)]
    pub advertise_endpoint: Option<String>,
    #[serde(default)]
    pub admin_username: Option<String>,
    #[serde(default)]
    pub vault_probe: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub stretch: Option<StretchIdentity>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct StretchIdentity {
    pub node_id: String,
    pub self_addr: String,
    pub peer_node_id: String,
    /// 对端声明的本端 mesh 地址；与 self_addr 交叉校验（旧版本缺省为空）。
    #[serde(default)]
    pub peer_addr: String,
    pub drive_count: u16,
    pub ec: EcIdentity,
    pub placement: String,
    pub mesh_secret_fingerprint: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct EcIdentity {
    pub data: u16,
    pub parity: u16,
}

#[derive(Debug)]
pub enum IdentityError {
    Unauthorized,
    Status(u16),
    Transport(String),
    Decode(String),
}

/// 端点校验：`http|https://host[:port]`，无 path/query/userinfo；返回去尾斜杠基址。
pub fn validate_endpoint(endpoint: &str) -> Result<String, String> {
    Ok(crate::tls_config::parse_endpoint(endpoint, "endpoint")?.base)
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
    tls: Option<&crate::tls::Tls>,
) -> Result<(u16, Bytes), String> {
    let headers = [(SITE_TOKEN_HEADER, token)];
    send_with_headers(endpoint, method, path, &headers, body, SEND_TIMEOUT, tls).await
}

/// 任意头部的控制面请求（成员变更走集群令牌 + 单跳标记，见 [`post_raft_membership`]）。
async fn send_with_headers(
    endpoint: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Bytes>,
    timeout: Duration,
    tls: Option<&crate::tls::Tls>,
) -> Result<(u16, Bytes), String> {
    match tokio::time::timeout(
        timeout,
        send_inner(endpoint, method, path, headers, body, tls),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => Err(format!("站点控制面请求超时（{}s）", timeout.as_secs())),
    }
}

async fn send_inner(
    endpoint: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Bytes>,
    tls: Option<&crate::tls::Tls>,
) -> Result<(u16, Bytes), String> {
    // peer TLS 模式下存储的 http:// 地址在连接期升级；无 TLS 物料的 https 直接拒绝。
    let base = match tls {
        Some(t) => t.peer_url(endpoint)?,
        None => validate_endpoint(endpoint)?,
    };
    let ep = crate::tls_config::parse_endpoint(&base, "endpoint")?;
    let uri: Uri = format!("{base}{path}")
        .parse()
        .map_err(|e| format!("请求 URI 构造失败：{e}"))?;
    let mut req = hyper::Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    if body.is_some() {
        req = req.header("content-type", "application/json");
    }
    let req = req
        .body(Full::new(body.unwrap_or_default()))
        .map_err(|e| format!("请求构造失败：{e}"))?;

    let stream: Box<dyn crate::tls::Io> = match tls {
        Some(t) => t.connect(ep.https, &ep.host, ep.port, None).await?,
        None if ep.https => {
            return Err(format!(
                "endpoint {endpoint} 为 https，但未配置 [tls.server]（拒绝明文发送）"
            ));
        }
        None => Box::new(
            TcpStream::connect(format!("{}:{}", ep.host, ep.port))
                .await
                .map_err(|e| format!("连接 {}:{} 失败：{e}", ep.host, ep.port))?,
        ),
    };
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
    get_identity_tls(endpoint, token, None).await
}

pub async fn get_identity_tls(
    endpoint: &str,
    token: &str,
    tls: Option<&crate::tls::Tls>,
) -> Result<PeerIdentity, String> {
    get_identity_typed_tls(endpoint, token, tls)
        .await
        .map_err(|e| match e {
            IdentityError::Unauthorized => "对端 identity 拒绝站点令牌（403）".to_string(),
            IdentityError::Status(s) => format!("对端 identity 返回 {s}"),
            IdentityError::Transport(m) => format!("对端 identity 连接失败：{m}"),
            IdentityError::Decode(m) => format!("identity 解析失败：{m}"),
        })
}

/// S7.4：配对需要区分 403（错站点令牌）与传输/解码错误。
pub async fn get_identity_typed(
    endpoint: &str,
    token: &str,
) -> Result<PeerIdentity, IdentityError> {
    get_identity_typed_tls(endpoint, token, None).await
}

pub async fn get_identity_typed_tls(
    endpoint: &str,
    token: &str,
    tls: Option<&crate::tls::Tls>,
) -> Result<PeerIdentity, IdentityError> {
    let (status, body) = send(endpoint, "GET", "/api/v1/site/identity", token, None, tls)
        .await
        .map_err(IdentityError::Transport)?;
    match status {
        200 => serde_json::from_slice(&body).map_err(|e| IdentityError::Decode(e.to_string())),
        401 | 403 => Err(IdentityError::Unauthorized),
        s => Err(IdentityError::Status(s)),
    }
}

/// S7.4 双边配对：向对端 `/join` 提交本站身份（集群令牌；站点令牌在 body 内）。
pub async fn post_raft_join(
    endpoint: &str,
    cluster_token: &str,
    body: &serde_json::Value,
    timeout: Duration,
) -> Result<(u16, Bytes), String> {
    post_raft_join_tls(endpoint, cluster_token, body, timeout, None).await
}

pub async fn post_raft_join_tls(
    endpoint: &str,
    cluster_token: &str,
    body: &serde_json::Value,
    timeout: Duration,
    tls: Option<&crate::tls::Tls>,
) -> Result<(u16, Bytes), String> {
    let bytes = Bytes::from(serde_json::to_vec(body).map_err(|e| e.to_string())?);
    send_with_headers(
        endpoint,
        "POST",
        "/api/v1/site/raft/join",
        &[(crate::raft::RPC_TOKEN_HEADER, cluster_token)],
        Some(bytes),
        timeout,
        tls,
    )
    .await
}

/// S7.3 成员变更：集群令牌；`forwarded` 只在节点内部转发时置位（单跳，防二次转发）。
pub(crate) async fn post_raft_membership(
    endpoint: &str,
    token: &str,
    body: Bytes,
    timeout: Duration,
    forwarded: bool,
    target: Option<u64>,
    tls: Option<&crate::tls::Tls>,
) -> Result<(u16, Bytes), String> {
    let target = target.map(|t| t.to_string());
    let mut headers = vec![(crate::raft::RPC_TOKEN_HEADER, token)];
    if forwarded {
        headers.push((crate::raft::routes::FORWARD_HEADER, "1"));
        if let Some(t) = target.as_deref() {
            headers.push((crate::raft::RPC_TARGET_HEADER, t));
        }
    }
    send_with_headers(
        endpoint,
        "POST",
        "/api/v1/site/raft/membership",
        &headers,
        Some(body),
        timeout,
        tls,
    )
    .await
}

/// changelog 出货（切片 1b）：返回 (status, body)。
pub async fn post_changelog(
    endpoint: &str,
    token: &str,
    body: Bytes,
) -> Result<(u16, Bytes), String> {
    post_changelog_tls(endpoint, token, body, None).await
}

pub async fn post_changelog_tls(
    endpoint: &str,
    token: &str,
    body: Bytes,
    tls: Option<&crate::tls::Tls>,
) -> Result<(u16, Bytes), String> {
    send(
        endpoint,
        "POST",
        "/api/v1/site/changelog",
        token,
        Some(body),
        tls,
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
        assert_eq!(validate_endpoint("https://h:1").unwrap(), "https://h:1");
        assert!(validate_endpoint("h:1").is_err());
        assert!(validate_endpoint("http://h:1/path").is_err());
        assert!(validate_endpoint("http://h:1?x=1").is_err());
        assert!(validate_endpoint("http://u@h:1").is_err());
        assert!(validate_endpoint("ftp://h:1").is_err());
    }
}
