//! AWS PrivateLink 端点身份验证器：直连源 IP → ENI → VPC endpoint（EC2 Query API）。
//!
//! 只信任直连 TCP 对端 IP（`ConnectInfo`），绝不读取客户端端点/账户头。未命中缓存时
//! 依次调用 `DescribeNetworkInterfaces`（按源 IP 过滤）与 `DescribeVpcEndpoints`
//! （按 ENI 的 VPC 过滤后匹配 `NetworkInterfaceIds`）；要求恰好一个 ENI、恰好一个关联
//! 端点且 `State=available`，从响应取 `VpcEndpointId/VpcId/OwnerId`。结果按源 IP + TTL
//! 缓存；API 超时/限流/错误一律 `Unavailable`（500，fail-closed），且不缓存失败。
//!
//! 部署要求：NLB/网关必须**保留客户端源 IP**（否则服务端只看到 NLB 地址，映射必然
//! 失败并 fail-closed）。真实 AWS 账号的端到端验收必须在具备 PrivateLink 的环境中
//! 完成；本仓库测试只用本地 mock HTTP 服务覆盖签名请求形状、响应解析、状态判定与
//! 缓存，不伪造 AWS 身份。

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use s3s_sigv4::{
    AmzDate, EMPTY_STRING_SHA256_HASH, Payload, calculate_signature, create_canonical_request,
    create_string_to_sign,
};
use xl_s3::ap::{
    ApEndpointClaim, ApEndpointContext, ApEndpointEvidence, ApEndpointIdentity,
    ApEndpointVerifyError, EndpointIdentityVerifier, valid_vpc_id, valid_vpce_id,
};

const DEFAULT_TTL: Duration = Duration::from_secs(60);
const API_TIMEOUT: Duration = Duration::from_secs(3);
const EC2_API_VERSION: &str = "2016-11-15";

/// SigV4 签名后的 EC2 Query GET：URL + 需发送的头。
type SignedRequestParts = (reqwest::Url, Vec<(&'static str, String)>);

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// `OSS_AP_ENDPOINT_VERIFIER=aws-api` 的启动配置。
///
/// 环境变量：`AWS_REGION`（或 `AWS_DEFAULT_REGION`）、`AWS_ACCESS_KEY_ID`、
/// `AWS_SECRET_ACCESS_KEY` 必填；`AWS_SESSION_TOKEN` 可选；`AWS_ENDPOINT_URL_EC2`
/// 仅用于测试/私有端点。可选校验：`OSS_AP_VPC_ID`（期望 VPC）、
/// `OSS_AP_AWS_ACCOUNT_ID`（期望端点 owner 账户）。
#[derive(Clone)]
pub struct AwsApiConfig {
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
    pub expected_vpc: Option<String>,
    pub expected_account: Option<String>,
    pub ttl: Duration,
}

/// 绝不把长期密钥/会话令牌打进日志/诊断。
impl std::fmt::Debug for AwsApiConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsApiConfig")
            .field("region", &self.region)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("endpoint", &self.endpoint)
            .field("expected_vpc", &self.expected_vpc)
            .field("expected_account", &self.expected_account)
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl AwsApiConfig {
    pub fn from_env() -> Result<Self, String> {
        let env = |key: &str| std::env::var(key).ok().filter(|v| !v.trim().is_empty());
        let region = env("AWS_REGION")
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .ok_or("OSS_AP_ENDPOINT_VERIFIER=aws-api 需要 AWS_REGION（或 AWS_DEFAULT_REGION）")?;
        let access_key = env("AWS_ACCESS_KEY_ID")
            .ok_or("OSS_AP_ENDPOINT_VERIFIER=aws-api 需要 AWS_ACCESS_KEY_ID")?;
        let secret_key = env("AWS_SECRET_ACCESS_KEY")
            .ok_or("OSS_AP_ENDPOINT_VERIFIER=aws-api 需要 AWS_SECRET_ACCESS_KEY")?;
        Ok(Self {
            region,
            access_key,
            secret_key,
            session_token: env("AWS_SESSION_TOKEN"),
            endpoint: env("AWS_ENDPOINT_URL_EC2"),
            expected_vpc: env("OSS_AP_VPC_ID"),
            expected_account: env("OSS_AP_AWS_ACCOUNT_ID"),
            ttl: DEFAULT_TTL,
        })
    }
}

/// 按源 IP 缓存的身份结果（含负结果）；API 错误不入缓存。
struct CacheEntry {
    expires: Instant,
    identity: Option<ApEndpointIdentity>,
}

pub struct AwsApiEndpointVerifier {
    client: reqwest::Client,
    config: AwsApiConfig,
    endpoint: String,
    cache: Mutex<HashMap<IpAddr, CacheEntry>>,
}

impl AwsApiEndpointVerifier {
    pub fn new(config: AwsApiConfig) -> Result<Self, String> {
        if config.region.trim().is_empty() {
            return Err("AWS 区域不能为空".into());
        }
        if config.access_key.trim().is_empty() || config.secret_key.trim().is_empty() {
            return Err("AWS 访问密钥不能为空".into());
        }
        if config
            .expected_vpc
            .as_deref()
            .is_some_and(|v| !valid_vpc_id(v))
        {
            return Err(format!("OSS_AP_VPC_ID 非法: {:?}", config.expected_vpc));
        }
        if config
            .expected_account
            .as_deref()
            .is_some_and(|a| !xl_s3::access::valid_account_id(a))
        {
            return Err(format!(
                "OSS_AP_AWS_ACCOUNT_ID 非法: {:?}",
                config.expected_account
            ));
        }
        let endpoint = config
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://ec2.{}.amazonaws.com", config.region));
        let url = reqwest::Url::parse(&endpoint)
            .map_err(|e| format!("AWS EC2 endpoint 非法 {endpoint:?}: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(format!(
                "AWS EC2 endpoint 必须是 http(s)://host: {endpoint:?}"
            ));
        }
        // SigV4 头含长期密钥派生签名；明文只允许回环 mock（测试/本地私有端点）。
        if url.scheme() == "http" && !url.host_str().is_some_and(is_loopback_host) {
            return Err(format!(
                "AWS EC2 endpoint 明文 http 仅允许 loopback: {endpoint:?}"
            ));
        }
        if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
            return Err(format!(
                "AWS EC2 endpoint 不得带 path/query/fragment: {endpoint:?}"
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(API_TIMEOUT)
            .build()
            .map_err(|e| format!("AWS EC2 HTTP 客户端构建失败: {e}"))?;
        Ok(Self {
            client,
            config,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            cache: Mutex::new(HashMap::new()),
        })
    }

    fn cached(&self, ip: IpAddr) -> Option<Option<ApEndpointIdentity>> {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        match cache.get(&ip) {
            Some(entry) if entry.expires > Instant::now() => Some(entry.identity.clone()),
            Some(_) => {
                cache.remove(&ip);
                None
            }
            None => None,
        }
    }

    fn store(&self, ip: IpAddr, identity: Option<ApEndpointIdentity>) {
        const MAX_CACHE: usize = 4096;
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if cache.len() >= MAX_CACHE {
            cache.retain(|_, entry| entry.expires > now);
            if cache.len() >= MAX_CACHE {
                cache.clear();
            }
        }
        cache.insert(
            ip,
            CacheEntry {
                expires: now + self.config.ttl,
                identity,
            },
        );
    }

    /// 源 IP → 端点身份；`Ok(None)` = 该 IP 不是 PrivateLink 端点（负结果也缓存）。
    async fn lookup(
        &self,
        ip: IpAddr,
    ) -> Result<Option<ApEndpointIdentity>, ApEndpointVerifyError> {
        if let Some(identity) = self.cached(ip) {
            return Ok(identity);
        }
        let result = self.load(ip).await;
        if let Ok(identity) = &result {
            self.store(ip, identity.clone());
        }
        result
    }

    async fn load(&self, ip: IpAddr) -> Result<Option<ApEndpointIdentity>, ApEndpointVerifyError> {
        let filter = if ip.is_ipv6() {
            "addresses.ipv6-address"
        } else {
            "addresses.private-ip-address"
        };
        let xml = self
            .call(
                "DescribeNetworkInterfaces",
                &[
                    ("Filter.1.Name", filter),
                    ("Filter.1.Value.1", &ip.to_string()),
                ],
            )
            .await?;
        let enis = parse_network_interfaces(&xml).map_err(unavailable)?;
        let [eni] = enis.as_slice() else {
            // 0（非端点流量）或多个（无法归属）都不产生身份。
            return Ok(None);
        };
        let xml = self
            .call(
                "DescribeVpcEndpoints",
                &[
                    ("Filter.1.Name", "vpc-id"),
                    ("Filter.1.Value.1", &eni.vpc_id),
                    ("Filter.2.Name", "vpc-endpoint-state"),
                    ("Filter.2.Value.1", "available"),
                ],
            )
            .await?;
        let endpoints = parse_vpc_endpoints(&xml).map_err(unavailable)?;
        let matching: Vec<&VpcEndpoint> = endpoints
            .iter()
            .filter(|e| e.state == "available" && e.interface_ids.contains(&eni.id))
            .collect();
        let [endpoint] = matching.as_slice() else {
            return Ok(None);
        };
        if endpoint.vpc_id != eni.vpc_id {
            return Err(ApEndpointVerifyError::Unavailable(
                "EC2 响应的 ENI 与 VPC endpoint 不在同一 VPC".into(),
            ));
        }
        if !valid_vpce_id(&endpoint.vpce_id)
            || !valid_vpc_id(&endpoint.vpc_id)
            || !xl_s3::access::valid_account_id(&endpoint.owner_id)
        {
            return Err(ApEndpointVerifyError::Unavailable(
                "EC2 响应含非法端点/VPC/账户 ID".into(),
            ));
        }
        if let Some(expected) = &self.config.expected_vpc
            && expected != &endpoint.vpc_id
        {
            return Err(ApEndpointVerifyError::Denied("vpc-mismatch"));
        }
        if let Some(expected) = &self.config.expected_account
            && expected != &endpoint.owner_id
        {
            return Err(ApEndpointVerifyError::Denied("account-mismatch"));
        }
        Ok(Some(ApEndpointIdentity {
            vpce_id: endpoint.vpce_id.clone(),
            vpc_id: Some(endpoint.vpc_id.clone()),
            account_id: Some(endpoint.owner_id.clone()),
            evidence: ApEndpointEvidence::AwsApi,
        }))
    }

    async fn call(
        &self,
        action: &str,
        filters: &[(&str, &str)],
    ) -> Result<String, ApEndpointVerifyError> {
        let mut pairs = vec![
            ("Action".to_string(), action.to_string()),
            ("Version".to_string(), EC2_API_VERSION.to_string()),
        ];
        pairs.extend(filters.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        let (url, headers) = self
            .signed_get(&pairs)
            .map_err(ApEndpointVerifyError::Unavailable)?;
        let mut request = self.client.get(url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await.map_err(|e| {
            ApEndpointVerifyError::Unavailable(format!("EC2 {action} 请求失败: {e}"))
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|e| {
            ApEndpointVerifyError::Unavailable(format!("EC2 {action} 响应读取失败: {e}"))
        })?;
        if !status.is_success() {
            let code = xml_tag(&body, "Code").unwrap_or("unknown");
            let message = xml_tag(&body, "Message").unwrap_or("");
            return Err(ApEndpointVerifyError::Unavailable(format!(
                "EC2 {action} {status}: {code} {message}"
            )));
        }
        Ok(body)
    }

    /// SigV4 签名的 EC2 Query GET（service=`ec2`）。返回 URL 与需发送的头。
    fn signed_get(
        &self,
        pairs: &[(String, String)],
    ) -> Result<SignedRequestParts, String> {
        let mut encoded: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (aws_encode(k), aws_encode(v)))
            .collect();
        encoded.sort();
        let query = encoded
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let url = reqwest::Url::parse(&format!("{}?{query}", self.endpoint))
            .map_err(|e| format!("EC2 请求 URL 非法: {e}"))?;
        let amz = now_amz_date();
        let amz_date = AmzDate::parse(&amz).map_err(|_| "内部 x-amz-date 生成失败".to_string())?;
        let mut headers: Vec<(&'static str, String)> =
            vec![("host", host_header(&url)), ("x-amz-date", amz)];
        if let Some(token) = &self.config.session_token {
            headers.push(("x-amz-security-token", token.clone()));
        }
        let signed: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        let canonical = create_canonical_request(
            "GET",
            "/",
            pairs,
            &signed,
            Payload::SingleChunk(EMPTY_STRING_SHA256_HASH),
        );
        let region = &self.config.region;
        let string_to_sign = create_string_to_sign(&canonical, &amz_date, region, "ec2");
        let signature = calculate_signature(
            &string_to_sign,
            &self.config.secret_key,
            &amz_date,
            region,
            "ec2",
        );
        let signed_names = signed
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(";");
        headers.push((
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{}/{region}/ec2/aws4_request, \
                 SignedHeaders={signed_names}, Signature={signature}",
                self.config.access_key,
                amz_date.fmt_date(),
            ),
        ));
        Ok((url, headers))
    }
}

#[async_trait::async_trait]
impl EndpointIdentityVerifier for AwsApiEndpointVerifier {
    async fn verify(
        &self,
        ctx: &ApEndpointContext,
    ) -> Result<Option<ApEndpointIdentity>, ApEndpointVerifyError> {
        // 静态部署证明优先，与其它验证器语义一致。
        if let Some(vpce) = &ctx.static_vpce {
            return Ok(Some(ApEndpointIdentity {
                vpce_id: vpce.clone(),
                vpc_id: None,
                account_id: None,
                evidence: ApEndpointEvidence::StaticConfig,
            }));
        }
        if matches!(&ctx.claim, ApEndpointClaim::Malformed) {
            return Err(ApEndpointVerifyError::Denied("malformed-claim"));
        }
        let Some(peer) = ctx.direct_peer else {
            return match &ctx.claim {
                ApEndpointClaim::Absent => Ok(None),
                _ => Err(ApEndpointVerifyError::Denied("untrusted-source")),
            };
        };
        let Some(identity) = self.lookup(peer).await? else {
            // 只信源 IP 映射；客户端声明的端点无法由 API 证明 → 拒绝。
            return match &ctx.claim {
                ApEndpointClaim::Absent => Ok(None),
                _ => Err(ApEndpointVerifyError::Denied("unmapped-source")),
            };
        };
        if let ApEndpointClaim::Header(vpce) = &ctx.claim
            && vpce != &identity.vpce_id
        {
            return Err(ApEndpointVerifyError::Denied("vpce-mismatch"));
        }
        Ok(Some(identity))
    }
}

fn unavailable(message: String) -> ApEndpointVerifyError {
    ApEndpointVerifyError::Unavailable(message)
}

/// AWS canonical 编码（unreserved 外一律 `%XX` 大写）；与 s3s-sigv4 的规范一致。
fn aws_encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'~' | b'.' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn host_header(url: &reqwest::Url) -> String {
    let host = url.host_str().unwrap_or_default();
    let default_port = if url.scheme() == "https" { 443 } else { 80 };
    match url.port() {
        Some(port) if port != default_port => format!("{host}:{port}"),
        _ => host.to_string(),
    }
}

/// `{YYYY}{MM}{DD}T{HH}{MM}{SS}Z`（UTC）。
fn now_amz_date() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Eni {
    id: String,
    vpc_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VpcEndpoint {
    vpce_id: String,
    vpc_id: String,
    owner_id: String,
    state: String,
    interface_ids: Vec<String>,
}

fn parse_network_interfaces(xml: &str) -> Result<Vec<Eni>, String> {
    xml_items(xml, "networkInterfaceSet")
        .into_iter()
        .map(|item| {
            Ok(Eni {
                id: xml_tag(item, "networkInterfaceId")
                    .ok_or("DescribeNetworkInterfaces 缺 networkInterfaceId")?
                    .to_string(),
                vpc_id: xml_tag(item, "vpcId")
                    .ok_or("DescribeNetworkInterfaces 缺 vpcId")?
                    .to_string(),
            })
        })
        .collect()
}

fn parse_vpc_endpoints(xml: &str) -> Result<Vec<VpcEndpoint>, String> {
    xml_items(xml, "vpcEndpointSet")
        .into_iter()
        .map(|item| {
            let interface_ids = xml_items(item, "networkInterfaceIdSet")
                .into_iter()
                .filter_map(|entry| xml_tag(entry, "networkInterfaceId"))
                .map(str::to_string)
                .collect();
            Ok(VpcEndpoint {
                vpce_id: xml_tag(item, "vpcEndpointId")
                    .ok_or("DescribeVpcEndpoints 缺 vpcEndpointId")?
                    .to_string(),
                vpc_id: xml_tag(item, "vpcId")
                    .ok_or("DescribeVpcEndpoints 缺 vpcId")?
                    .to_string(),
                owner_id: xml_tag(item, "ownerId")
                    .ok_or("DescribeVpcEndpoints 缺 ownerId")?
                    .to_string(),
                state: xml_tag(item, "state")
                    .ok_or("DescribeVpcEndpoints 缺 state")?
                    .to_string(),
                interface_ids,
            })
        })
        .collect()
}

/// 取 `<tag>...</tag>` 的首个内含文本。
fn xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    xml_block(xml, tag)
}

/// 取 `<tag>...</tag>` 的首个内含块（同名的第一对，不做嵌套匹配）。
fn xml_block<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

/// 取集合块内的顶层 `<item>`（按嵌套深度配对，EC2 的 `networkInterfaceIdSet` 等
/// 会嵌套 item）。
fn xml_items<'a>(xml: &'a str, set: &str) -> Vec<&'a str> {
    let Some(block) = xml_block(xml, set) else {
        return Vec::new();
    };
    let mut items = Vec::new();
    let mut rest = block;
    while let Some(open_at) = rest.find("<item>") {
        let inner = &rest[open_at + "<item>".len()..];
        let mut depth = 1usize;
        let mut cursor = 0usize;
        let mut close_at = None;
        while let Some(offset) = inner[cursor..].find('<') {
            let at = cursor + offset;
            if inner[at..].starts_with("<item>") {
                depth += 1;
                cursor = at + "<item>".len();
            } else if inner[at..].starts_with("</item>") {
                depth -= 1;
                if depth == 0 {
                    close_at = Some(at);
                    break;
                }
                cursor = at + "</item>".len();
            } else {
                cursor = at + 1;
            }
        }
        let Some(close_at) = close_at else {
            break;
        };
        items.push(&inner[..close_at]);
        rest = &inner[close_at + "</item>".len()..];
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::extract::{Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;

    #[derive(Clone, Default)]
    struct MockState {
        calls: Arc<AtomicUsize>,
        authorizations: Arc<Mutex<Vec<String>>>,
    }

    async fn mock(
        State(state): State<MockState>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
            state
                .authorizations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(auth.to_string());
        }
        let action = query.get("Action").map(String::as_str).unwrap_or_default();
        let value = query
            .get("Filter.1.Value.1")
            .map(String::as_str)
            .unwrap_or_default();
        let xml = match action {
            "DescribeNetworkInterfaces" => match value {
                "10.0.0.8" => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "<Response><Errors><Error><Code>RequestLimitExceeded</Code>\
                         <Message>slow down</Message></Error></Errors></Response>",
                    )
                        .into_response();
                }
                "10.0.0.9" => enis("vpc-disabled"),
                "10.0.0.10" => enis("vpc-two"),
                "10.0.0.11" => {
                    "<DescribeNetworkInterfacesResponse><networkInterfaceSet>\
                     <item><networkInterfaceId>eni-1</networkInterfaceId><vpcId>vpc-1</vpcId></item>\
                     <item><networkInterfaceId>eni-2</networkInterfaceId><vpcId>vpc-1</vpcId></item>\
                     </networkInterfaceSet></DescribeNetworkInterfacesResponse>"
                        .to_string()
                }
                _ => enis("vpc-1"),
            },
            "DescribeVpcEndpoints" => match value {
                "vpc-disabled" => endpoint_xml("vpce-abc", "vpc-disabled", "deleted"),
                "vpc-two" => {
                    let one = item("vpce-a", "vpc-two", "available");
                    let two = item("vpce-b", "vpc-two", "available");
                    format!(
                        "<DescribeVpcEndpointsResponse><vpcEndpointSet>{one}{two}\
                         </vpcEndpointSet></DescribeVpcEndpointsResponse>"
                    )
                }
                _ => endpoint_xml("vpce-abc", "vpc-1", "available"),
            },
            _ => String::new(),
        };
        (StatusCode::OK, xml).into_response()
    }

    fn enis(vpc: &str) -> String {
        format!(
            "<DescribeNetworkInterfacesResponse><networkInterfaceSet>\
             <item><networkInterfaceId>eni-1</networkInterfaceId><vpcId>{vpc}</vpcId></item>\
             </networkInterfaceSet></DescribeNetworkInterfacesResponse>"
        )
    }

    fn item(vpce: &str, vpc: &str, state: &str) -> String {
        format!(
            "<item><vpcEndpointId>{vpce}</vpcEndpointId><vpcId>{vpc}</vpcId>\
             <state>{state}</state>\
             <networkInterfaceIdSet><item><networkInterfaceId>eni-1</networkInterfaceId>\
             </item></networkInterfaceIdSet>\
             <ownerId>111111111111</ownerId></item>"
        )
    }

    fn endpoint_xml(vpce: &str, vpc: &str, state: &str) -> String {
        let item = item(vpce, vpc, state);
        format!(
            "<DescribeVpcEndpointsResponse><vpcEndpointSet>{item}\
             </vpcEndpointSet></DescribeVpcEndpointsResponse>"
        )
    }

    async fn mock_server() -> (String, MockState) {
        let state = MockState::default();
        let app = Router::new().fallback(get(mock)).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), state)
    }

    fn config(endpoint: String) -> AwsApiConfig {
        AwsApiConfig {
            region: "us-east-1".into(),
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "secret".into(),
            session_token: None,
            endpoint: Some(endpoint),
            expected_vpc: None,
            expected_account: None,
            ttl: Duration::from_secs(60),
        }
    }

    fn ctx(claim: ApEndpointClaim, peer: Option<&str>) -> ApEndpointContext {
        ApEndpointContext {
            claim,
            direct_peer: peer.map(|p| p.parse().unwrap()),
            ..Default::default()
        }
    }

    /// EC2 XML 含嵌套 `<item>`（networkInterfaceIdSet）且 ownerId 在其后：解析必须完整。
    #[test]
    fn parses_nested_ec2_xml() {
        let enis = parse_network_interfaces(&enis("vpc-1")).unwrap();
        assert_eq!(
            enis,
            vec![Eni {
                id: "eni-1".into(),
                vpc_id: "vpc-1".into()
            }]
        );
        let endpoints =
            parse_vpc_endpoints(&endpoint_xml("vpce-abc", "vpc-1", "available")).unwrap();
        assert_eq!(
            endpoints,
            vec![VpcEndpoint {
                vpce_id: "vpce-abc".into(),
                vpc_id: "vpc-1".into(),
                owner_id: "111111111111".into(),
                state: "available".into(),
                interface_ids: vec!["eni-1".into()],
            }]
        );
    }

    /// 源 IP 映射成功 → AwsApi 身份；声明头一致可省略/匹配，不一致拒绝；成功结果按 IP 缓存。
    #[tokio::test]
    async fn maps_source_ip_and_caches_result() {
        let (endpoint, mock) = mock_server().await;
        let verifier = AwsApiEndpointVerifier::new(config(endpoint)).unwrap();
        let identity = verifier
            .verify(&ctx(ApEndpointClaim::Absent, Some("10.0.0.5")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(identity.vpce_id, "vpce-abc");
        assert_eq!(identity.vpc_id.as_deref(), Some("vpc-1"));
        assert_eq!(identity.account_id.as_deref(), Some("111111111111"));
        assert_eq!(identity.evidence, ApEndpointEvidence::AwsApi);
        assert_eq!(mock.calls.load(Ordering::Relaxed), 2, "两次 EC2 调用");

        let cached = verifier
            .verify(&ctx(
                ApEndpointClaim::Header("vpce-abc".into()),
                Some("10.0.0.5"),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached.vpce_id, "vpce-abc");
        assert_eq!(mock.calls.load(Ordering::Relaxed), 2, "命中缓存不再调用");

        assert_eq!(
            verifier
                .verify(&ctx(
                    ApEndpointClaim::Header("vpce-other".into()),
                    Some("10.0.0.5")
                ))
                .await,
            Err(ApEndpointVerifyError::Denied("vpce-mismatch"))
        );
        let auth = mock.authorizations.lock().unwrap();
        assert!(
            auth.iter().all(
                |a| a.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
                    && a.contains("/us-east-1/ec2/aws4_request")
                    && a.contains("SignedHeaders=host;x-amz-date")
            ),
            "SigV4 头形状: {auth:?}"
        );
    }

    /// API 错误/无映射/歧义/缺对端都 fail-closed；负结果缓存；期望 VPC/账户不符拒绝。
    #[tokio::test]
    async fn fails_closed_on_errors_and_ambiguity() {
        let (endpoint, mock) = mock_server().await;
        let verifier = AwsApiEndpointVerifier::new(config(endpoint.clone())).unwrap();

        assert!(matches!(
            verifier
                .verify(&ctx(ApEndpointClaim::Absent, Some("10.0.0.8")))
                .await,
            Err(ApEndpointVerifyError::Unavailable(_))
        ));
        assert_eq!(
            verifier
                .verify(&ctx(ApEndpointClaim::Absent, Some("10.0.0.9")))
                .await,
            Ok(None),
            "端点非 available → 无身份"
        );
        assert_eq!(
            verifier
                .verify(&ctx(
                    ApEndpointClaim::Header("vpce-abc".into()),
                    Some("10.0.0.9")
                ))
                .await,
            Err(ApEndpointVerifyError::Denied("unmapped-source"))
        );
        // 负结果同样按 IP 缓存（10.0.0.9 已查询过）。
        let before = mock.calls.load(Ordering::Relaxed);
        assert_eq!(
            verifier
                .verify(&ctx(ApEndpointClaim::Absent, Some("10.0.0.9")))
                .await,
            Ok(None)
        );
        assert_eq!(mock.calls.load(Ordering::Relaxed), before);
        for ip in ["10.0.0.10", "10.0.0.11"] {
            assert_eq!(
                verifier
                    .verify(&ctx(ApEndpointClaim::Absent, Some(ip)))
                    .await,
                Ok(None),
                "{ip}: 多个端点/ENI 无法归属"
            );
        }
        assert_eq!(
            verifier.verify(&ctx(ApEndpointClaim::Absent, None)).await,
            Ok(None)
        );
        assert_eq!(
            verifier
                .verify(&ctx(ApEndpointClaim::Header("vpce-abc".into()), None))
                .await,
            Err(ApEndpointVerifyError::Denied("untrusted-source"))
        );
        assert_eq!(
            verifier
                .verify(&ctx(ApEndpointClaim::Malformed, Some("10.0.0.5")))
                .await,
            Err(ApEndpointVerifyError::Denied("malformed-claim"))
        );
        let id = verifier
            .verify(&ApEndpointContext {
                static_vpce: Some("vpce-static".into()),
                ..ctx(ApEndpointClaim::Absent, Some("10.0.0.8"))
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id.evidence, ApEndpointEvidence::StaticConfig);

        let mut expected = config(endpoint.clone());
        expected.expected_account = Some("222222222222".into());
        let verifier = AwsApiEndpointVerifier::new(expected).unwrap();
        assert_eq!(
            verifier
                .verify(&ctx(ApEndpointClaim::Absent, Some("10.0.0.5")))
                .await,
            Err(ApEndpointVerifyError::Denied("account-mismatch"))
        );
        let mut expected = config(endpoint);
        expected.expected_vpc = Some("vpc-other".into());
        let verifier = AwsApiEndpointVerifier::new(expected).unwrap();
        assert_eq!(
            verifier
                .verify(&ctx(ApEndpointClaim::Absent, Some("10.0.0.5")))
                .await,
            Err(ApEndpointVerifyError::Denied("vpc-mismatch"))
        );
    }

    #[test]
    fn rejects_invalid_config() {
        let mut cfg = config("http://127.0.0.1:1".into());
        cfg.region.clear();
        assert!(AwsApiEndpointVerifier::new(cfg).is_err());
        let mut cfg = config("http://127.0.0.1:1".into());
        cfg.expected_account = Some("123".into());
        assert!(AwsApiEndpointVerifier::new(cfg).is_err());
        assert!(AwsApiEndpointVerifier::new(config("http://h/path".into())).is_err());
        assert!(AwsApiEndpointVerifier::new(config("ftp://h".into())).is_err());
    }
}
