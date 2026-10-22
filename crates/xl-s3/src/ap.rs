//! M8 S1：访问点（Access Point）——模型、严格策略解析与 op 推导。
//!
//! 路由形态：`/ap/<name>/<key>`、别名路径 `/<name>-s3alias/<key>`、别名 Host
//! `<name>-s3alias.<endpoint>`。AP 只做**额外限制**（与身份/桶策略取交集），绝不放松
//! 既有授权。策略方言 = 现有 `policy` 子集，但解析更严格（未知字段/条件运算符一律拒）。
//!
//! 明确不支持（non-goals）：匿名访问点、跨账号、VPC 端点、经 AP 的桶级 Create/Delete/
//! ListBuckets、AP 缓存一致性。

use std::net::IpAddr;
use std::sync::Arc;

use s3s::{S3Request, S3Response, S3Result};
use serde::Deserialize;

use crate::policy::{StringOrVec, glob_match};

/// AWS 访问点别名后缀；桶名在建桶时保留（`xl_store::layout::validate_bucket`）。
pub const ALIAS_SUFFIX: &str = "-s3alias";

/// DeleteObjects 请求体硬上限（与 s3s 自定义路由默认体上限一致）。
const AP_DELETE_MAX_BODY: usize = 1024 * 1024;
const AP_DELETE_MAX_KEYS: usize = 1000;

/// AP 名（控制面与别名路由共用的唯一判定）：3..=63 位小写字母/数字/`-`，首尾非 `-`。
pub fn valid_ap_name(name: &str) -> bool {
    (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
}

/// AP 策略（严格子集：`Statement[].Effect/Principal.AWS/Action/Resource`）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApPolicy {
    #[serde(default, rename = "Statement")]
    pub statement: Vec<ApStatement>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApStatement {
    #[serde(rename = "Effect")]
    pub effect: String,
    #[serde(rename = "Principal")]
    pub principal: ApPrincipal,
    #[serde(rename = "Action")]
    pub action: StringOrVec,
    #[serde(rename = "Resource")]
    pub resource: StringOrVec,
    #[serde(default, rename = "Condition")]
    pub condition: Option<ApCondition>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApPrincipal {
    #[serde(default, rename = "AWS")]
    pub aws: StringOrVec,
}

/// 条件块：仅 IP 条件（VPC 端点的窄替代）。未知运算符由 `deny_unknown_fields` 拒绝。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApCondition {
    #[serde(default, rename = "IpAddress")]
    pub ip_address: Option<ApIpCondition>,
    #[serde(default, rename = "NotIpAddress")]
    pub not_ip_address: Option<ApIpCondition>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApIpCondition {
    #[serde(rename = "aws:SourceIp")]
    pub source_ip: StringOrVec,
}

impl ApCondition {
    fn validate(&self, i: usize) -> Result<(), String> {
        if self.ip_address.is_none() && self.not_ip_address.is_none() {
            return Err(format!("Statement[{i}].Condition 不能为空"));
        }
        for (op, cond) in [
            ("IpAddress", &self.ip_address),
            ("NotIpAddress", &self.not_ip_address),
        ] {
            if let Some(cond) = cond {
                cond.validate(i, op)?;
            }
        }
        Ok(())
    }

    /// 条件是否成立；`None` = 无法求值（缺对端/坏 CIDR）→ 语句不匹配，fail-closed。
    fn matches(&self, ip: Option<IpAddr>) -> Option<bool> {
        let ip = ip?;
        if let Some(c) = &self.ip_address
            && c.matches(ip) != Some(true)
        {
            return Some(false);
        }
        if let Some(c) = &self.not_ip_address
            && c.matches(ip) != Some(false)
        {
            return Some(false);
        }
        Some(true)
    }
}

impl ApIpCondition {
    fn validate(&self, i: usize, op: &str) -> Result<(), String> {
        if self.source_ip.iter().next().is_none() {
            return Err(format!(
                "Statement[{i}].Condition.{op}.aws:SourceIp 不能为空"
            ));
        }
        for raw in self.source_ip.iter() {
            if parse_cidr(raw).is_none() {
                return Err(format!(
                    "Statement[{i}].Condition.{op}.aws:SourceIp 非法 CIDR: {raw}"
                ));
            }
        }
        Ok(())
    }

    /// 列表内任一 CIDR 命中；空列表/坏 CIDR → `None`（fail-closed）。
    fn matches(&self, ip: IpAddr) -> Option<bool> {
        let mut any = false;
        for raw in self.source_ip.iter() {
            any = true;
            if parse_cidr(raw)?.contains(ip) {
                return Some(true);
            }
        }
        any.then_some(false)
    }
}

struct Cidr {
    net: IpAddr,
    /// 已按 `net` 家族归一（v4-mapped 会变 v4，故上限按归一后的家族算）。
    prefix: u8,
}

fn parse_cidr(raw: &str) -> Option<Cidr> {
    let raw = raw.trim();
    let (addr, prefix) = match raw.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (raw, None),
    };
    let net = addr.parse::<IpAddr>().ok()?.to_canonical();
    let max = match net {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    let prefix = match prefix {
        Some(p) => p.parse::<u8>().ok()?,
        None => max,
    };
    (prefix <= max).then_some(Cidr { net, prefix })
}

impl Cidr {
    fn contains(&self, ip: IpAddr) -> bool {
        match (self.net, ip.to_canonical()) {
            (IpAddr::V4(n), IpAddr::V4(i)) => prefix_eq(&n.octets(), &i.octets(), self.prefix),
            (IpAddr::V6(n), IpAddr::V6(i)) => prefix_eq(&n.octets(), &i.octets(), self.prefix),
            _ => false,
        }
    }
}

fn prefix_eq(net: &[u8], ip: &[u8], prefix: u8) -> bool {
    let full = usize::from(prefix / 8);
    let rem = prefix % 8;
    net[..full] == ip[..full] && (rem == 0 || (net[full] ^ ip[full]) >> (8 - rem) == 0)
}

impl ApPolicy {
    /// serde 形状 + 语义校验（保存 400；运行期解析失败 500）。
    pub fn validate(&self) -> Result<(), String> {
        for (i, st) in self.statement.iter().enumerate() {
            if !matches!(st.effect.as_str(), "Allow" | "Deny") {
                return Err(format!("Statement[{i}].Effect 必须是 Allow/Deny"));
            }
            if st.principal.aws.iter().next().is_none() {
                return Err(format!("Statement[{i}].Principal.AWS 不能为空"));
            }
            if st.action.iter().next().is_none() {
                return Err(format!("Statement[{i}].Action 不能为空"));
            }
            if st.resource.iter().next().is_none() {
                return Err(format!("Statement[{i}].Resource 不能为空"));
            }
            if let Some(c) = &st.condition {
                c.validate(i)?;
            }
        }
        Ok(())
    }

    /// 单动作判定（与桶策略同语义：显式 Deny 优先；Allow 命中 true；无匹配 None）。
    pub fn evaluate(&self, req: &ApEvalRequest<'_>, action: &str) -> Option<bool> {
        let mut allowed = false;
        let mut any = false;
        for st in &self.statement {
            if !principal_matches(&st.principal, req.ak, req.uid)
                || !st.action.iter().any(|p| glob_match(p, action))
                || !st
                    .resource
                    .iter()
                    .any(|p| req.resources.iter().any(|r| glob_match(p, r)))
                || st
                    .condition
                    .as_ref()
                    .is_some_and(|c| c.matches(req.source_ip) != Some(true))
            {
                continue;
            }
            any = true;
            match st.effect.as_str() {
                "Deny" => return Some(false),
                "Allow" => allowed = true,
                _ => {}
            }
        }
        (any && allowed).then_some(true)
    }
}

fn principal_matches(p: &ApPrincipal, ak: &str, uid: i64) -> bool {
    p.aws.iter().any(|v| {
        v == "*"
            || v == ak
            || v.strip_prefix("uid:").and_then(|n| n.parse::<i64>().ok()) == Some(uid)
    })
}

/// 求值输入（policy 层不依赖 HTTP 类型）。
pub struct ApEvalRequest<'a> {
    pub ak: &'a str,
    pub uid: i64,
    /// AP 对象/桶 ARN 与标准 ARN（两者都参与匹配）
    pub resources: Vec<String>,
    /// 连接对端（无则带 `SourceIp` 条件的语句一律不匹配，fail-closed）。
    pub source_ip: Option<IpAddr>,
}

/// AP 上下文（ARN 生成用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApContext {
    pub name: String,
    pub region: String,
    pub account: String,
}

impl ApContext {
    pub fn new(name: &str, region: &str) -> Self {
        Self {
            name: name.to_string(),
            region: region.to_string(),
            account: "000000000000".to_string(),
        }
    }

    /// 对象级 AP ARN。
    pub fn object_arn(&self, key: &str) -> String {
        format!(
            "arn:aws:s3:{}:{}:accesspoint/{}/object/{key}",
            self.region, self.account, self.name
        )
    }

    /// 桶级 AP ARN。
    pub fn bucket_arn(&self) -> String {
        format!(
            "arn:aws:s3:{}:{}:accesspoint/{}",
            self.region, self.account, self.name
        )
    }
}

/// 访问点（缓存/路由用；policy 已解析校验）。
#[derive(Debug, Clone)]
pub struct AccessPoint {
    pub name: String,
    pub bucket: String,
    pub owner_id: i64,
    pub policy: Option<ApPolicy>,
    pub region: String,
}

impl AccessPoint {
    pub fn context(&self) -> ApContext {
        ApContext::new(&self.name, &self.region)
    }

    /// 从 DB 行构造（策略 JSON 解析失败 → Err，调用方按 fail-closed 处理）。
    pub fn from_row(
        name: String,
        bucket: String,
        owner_id: i64,
        policy_json: Option<String>,
        region: &str,
    ) -> Result<Self, String> {
        let policy = match policy_json {
            Some(raw) if !raw.trim().is_empty() => {
                let doc: ApPolicy =
                    serde_json::from_str(&raw).map_err(|e| format!("AP 策略解析失败: {e}"))?;
                doc.validate()?;
                Some(doc)
            }
            _ => None,
        };
        Ok(Self {
            name,
            bucket,
            owner_id,
            policy,
            region: region.to_string(),
        })
    }
}

/// AP 策略对单请求的额外限制（与身份/桶策略取交集）：
/// `None` = 无策略（不额外限制）；`Some(true)` = 放行；`Some(false)` = 拒（含无匹配）。
pub fn ap_decision(
    ap: &AccessPoint,
    user: &crate::access::AuthedUser,
    op: &str,
    key: Option<&str>,
    source_ip: Option<IpAddr>,
) -> Option<bool> {
    let policy = ap.policy.as_ref()?;
    let action = if crate::access::is_copy(op) {
        crate::policy::s3_action("PutObject")
    } else {
        crate::policy::s3_action(op)
    }?;
    let ctx = ap.context();
    let mut resources = vec![];
    match key {
        Some(k) => {
            resources.push(format!("arn:aws:s3:::{}", ap.bucket)); // 兼容旧方言（对象请求少见）
            resources.push(format!("arn:aws:s3:::{}/{k}", ap.bucket));
            resources.push(ctx.object_arn(k));
        }
        None => {
            resources.push(format!("arn:aws:s3:::{}", ap.bucket));
            resources.push(ctx.bucket_arn());
        }
    }
    let req = ApEvalRequest {
        ak: &user.ak,
        uid: user.uid,
        resources,
        source_ip,
    };
    Some(policy.evaluate(&req, action) == Some(true))
}

/// AP 缓存（控制面写路径负责更新；路由读路径 fail-closed）。
#[derive(Default)]
pub struct ApCache {
    inner: std::sync::RwLock<std::collections::HashMap<String, AccessPoint>>,
}

impl ApCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, name: &str) -> Option<AccessPoint> {
        self.inner.read().ok()?.get(name).cloned()
    }

    pub fn put(&self, ap: AccessPoint) {
        if let Ok(mut w) = self.inner.write() {
            w.insert(ap.name.clone(), ap);
        }
    }

    pub fn remove(&self, name: &str) {
        if let Ok(mut w) = self.inner.write() {
            w.remove(name);
        }
    }

    /// 桶删除级联：移除该桶全部 AP。
    pub fn invalidate_bucket(&self, bucket: &str) {
        if let Ok(mut w) = self.inner.write() {
            w.retain(|_, ap| ap.bucket != bucket);
        }
    }

    /// 启动预加载/全量刷新。
    pub fn replace_all(&self, items: impl IntoIterator<Item = AccessPoint>) {
        if let Ok(mut w) = self.inner.write() {
            *w = items.into_iter().map(|a| (a.name.clone(), a)).collect();
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|w| w.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// AP 来源（缓存未命中时的权威查询；oss-server 用 Db 实现）。
#[async_trait::async_trait]
pub trait ApSource: Send + Sync {
    /// `Err` = 查询失败（路由按 500 fail-closed，不得当作无 AP）。
    async fn lookup(&self, name: &str) -> Result<Option<AccessPoint>, String>;
}

/// 路由写入 extensions 的调用意图（`call` 读取）。
#[derive(Debug, Clone)]
pub struct ApInvocation {
    pub ap_name: String,
    pub bucket: String,
    pub raw_suffix: String,
    pub canonical_key: Option<String>,
    pub op: &'static str,
}

/// HTTP 层注入的连接对端（SourceIp 条件求值用）。缺省 → 带 IP 条件的语句不匹配。
#[derive(Debug, Clone, Copy)]
pub struct ApPeer(pub IpAddr);

/// DeleteObjects 经 AP 的逐键策略上下文：键在请求体里，整体判定必然失真，
/// 由 `service.rs::delete_objects` 在逐键循环里叠交集（被拒键绝不进入删除路径）。
#[derive(Debug, Clone)]
pub struct ApDeletePolicy {
    pub ap: AccessPoint,
    pub source_ip: Option<IpAddr>,
}

/// `/ap/<name>/...` 前置路由：鉴权（身份/桶策略 × AP 策略交集）→ 重写到内层服务。
pub struct ApRoute {
    cache: Arc<ApCache>,
    source: Arc<dyn ApSource>,
    acl: crate::access::Acl,
    /// M8 S2 变换器缓存（AP 名 → 编译产物）；miss = 不改写响应体。
    olap: Arc<crate::olap::OlapCache>,
    /// 内层服务：无 auth/access/route（N1），共享同一 OssS3 与 sinks。
    inner: Arc<s3s::service::S3Service>,
    /// 同一槽位的既有路由（N2：STS）；非 AP 请求原样委托。
    fallback: Option<Arc<dyn s3s::route::S3Route>>,
}

/// is_match 的派发结果（extensions 在 check_access/call 间传递）。
#[derive(Clone, Copy)]
struct ApMatch(bool);

impl ApRoute {
    pub fn new(
        cache: Arc<ApCache>,
        source: Arc<dyn ApSource>,
        acl: crate::access::Acl,
        olap: Arc<crate::olap::OlapCache>,
        inner: Arc<s3s::service::S3Service>,
    ) -> Self {
        Self {
            cache,
            source,
            acl,
            olap,
            inner,
            fallback: None,
        }
    }

    /// 包装同一 `set_route` 槽的既有路由（STS）：先 AP，未命中委托。
    pub fn with_fallback(mut self, fallback: Arc<dyn s3s::route::S3Route>) -> Self {
        self.fallback = Some(fallback);
        self
    }

    async fn load_ap(&self, name: &str) -> S3Result<AccessPoint> {
        if let Some(ap) = self.cache.get(name) {
            return Ok(ap);
        }
        match self.source.lookup(name).await {
            Ok(Some(ap)) => {
                self.cache.put(ap.clone());
                Ok(ap)
            }
            Ok(None) => Err(s3s::s3_error!(NoSuchAccessPoint, "access point not found")),
            Err(e) => {
                tracing::error!(error = %e, ap = name, "AP 查询失败");
                Err(s3s::s3_error!(InternalError, "internal error"))
            }
        }
    }
}

fn parse_ap_path(uri: &http::Uri) -> S3Result<(String, String)> {
    let Some(rest) = uri.path().strip_prefix("/ap/") else {
        return Err(s3s::s3_error!(InvalidArgument, "invalid access point path"));
    };
    let (name, suffix) = rest.split_once('/').unwrap_or((rest, ""));
    if name.is_empty() {
        return Err(s3s::s3_error!(InvalidArgument, "missing access point name"));
    }
    Ok((name.to_string(), suffix.to_string()))
}

/// Host 形式别名：首标签（去端口、转小写、须含 `.` 即 `<alias>.<endpoint>`）以
/// `-s3alias` 结尾。IPv6 字面量不可能是别名。
fn host_alias(uri: &http::Uri, headers: &http::HeaderMap) -> Option<String> {
    let host = headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .map(str::to_string)
        .or_else(|| uri.authority().map(|a| a.as_str().to_string()))?;
    if host.starts_with('[') {
        return None;
    }
    let host = host.split(':').next().unwrap_or(host.as_str());
    if !host.contains('.') {
        return None;
    }
    let label = host
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    label.ends_with(ALIAS_SUFFIX).then_some(label)
}

/// 路径形式别名：首段（即桶位）以 `-s3alias` 结尾。
fn path_alias(uri: &http::Uri) -> Option<String> {
    let first = uri
        .path()
        .strip_prefix('/')?
        .split('/')
        .next()
        .unwrap_or("");
    first.ends_with(ALIAS_SUFFIX).then(|| first.to_string())
}

/// 剥后缀并校验 AP 名；非法名与未知别名同响应（404），绝不回落桶路由。
fn strip_alias(seg: &str) -> S3Result<String> {
    let name = seg.strip_suffix(ALIAS_SUFFIX).unwrap_or(seg);
    if !valid_ap_name(name) {
        return Err(s3s::s3_error!(NoSuchAccessPoint, "access point not found"));
    }
    Ok(name.to_string())
}

/// 解析三种形态为 `(ap_name, raw_suffix)`：`/ap/<name>/...`、别名的路径/Host 形式。
fn parse_ap_target(uri: &http::Uri, headers: &http::HeaderMap) -> S3Result<(String, String)> {
    if uri.path().starts_with("/ap/") {
        return parse_ap_path(uri);
    }
    if let Some(alias) = host_alias(uri, headers) {
        let raw_suffix = uri.path().strip_prefix('/').unwrap_or_default().to_string();
        return Ok((strip_alias(&alias)?, raw_suffix));
    }
    if let Some(alias) = path_alias(uri) {
        let rest = uri.path().strip_prefix('/').unwrap_or_default();
        let raw_suffix = rest.split_once('/').map_or("", |(_, s)| s).to_string();
        return Ok((strip_alias(&alias)?, raw_suffix));
    }
    Err(s3s::s3_error!(InvalidArgument, "invalid access point path"))
}

/// 严格百分号解码（拒绝坏转义/非法 UTF-8）。
fn percent_decode_strict(raw: &str) -> S3Result<String> {
    let b = raw.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            if i + 2 >= b.len() {
                return Err(s3s::s3_error!(
                    InvalidArgument,
                    "malformed percent-encoding"
                ));
            }
            let h = (b[i + 1] as char).to_digit(16);
            let l = (b[i + 2] as char).to_digit(16);
            match (h, l) {
                (Some(h), Some(l)) => out.push((h * 16 + l) as u8),
                _ => {
                    return Err(s3s::s3_error!(
                        InvalidArgument,
                        "malformed percent-encoding"
                    ));
                }
            }
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    let s =
        String::from_utf8(out).map_err(|_| s3s::s3_error!(InvalidArgument, "invalid UTF-8 key"))?;
    if s.contains('\0') || s.split('/').any(|seg| seg == "." || seg == "..") {
        return Err(s3s::s3_error!(InvalidArgument, "invalid key"));
    }
    Ok(s)
}

fn query_has(query: &str, key: &str) -> bool {
    query
        .split('&')
        .any(|p| p == key || p.starts_with(&format!("{key}=")))
}

fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .find_map(|p| p.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
}

/// `GET /{bucket}`（无 list-type）的 v1 ListObjects 参数白名单；未知子资源仍 fail-closed。
fn is_list_objects_v1(query: &str) -> bool {
    strip_signed_query(query).split('&').all(|p| {
        p.is_empty()
            || matches!(
                p.split('=').next().unwrap_or(""),
                "prefix" | "delimiter" | "marker" | "max-keys" | "encoding-type" | "x-id"
            )
    })
}

fn derive_op(
    method: &http::Method,
    key: Option<&str>,
    query: &str,
    copy_source: bool,
) -> S3Result<&'static str> {
    use http::Method;
    match *method {
        Method::GET => {
            if query_has(query, "versions") {
                Ok("ListObjectVersions")
            } else if query_has(query, "uploads") {
                Ok("ListMultipartUploads")
            } else if key.is_some() && query_has(query, "uploadId") {
                Ok("ListParts")
            } else if query_has(query, "tagging") {
                Ok("GetObjectTagging")
            } else if key.is_some() && query_has(query, "retention") {
                Ok("GetObjectRetention")
            } else if query_value(query, "list-type") == Some("2") {
                Ok("ListObjectsV2")
            } else if query_has(query, "versioning") {
                Ok("GetBucketVersioning")
            } else if key.is_some() {
                Ok("GetObject")
            } else if is_list_objects_v1(query) {
                Ok("ListObjects")
            } else {
                Err(s3s::s3_error!(
                    NotImplemented,
                    "unsupported access point operation"
                ))
            }
        }
        Method::HEAD => {
            if key.is_some() {
                Ok("HeadObject")
            } else {
                Ok("HeadBucket")
            }
        }
        Method::PUT => {
            if query_has(query, "uploadId") && query_has(query, "partNumber") {
                if copy_source {
                    Ok("UploadPartCopy")
                } else {
                    Ok("UploadPart")
                }
            } else if copy_source {
                Ok("CopyObject")
            } else if query_has(query, "tagging") {
                Ok("PutObjectTagging")
            } else if key.is_some() && query_has(query, "retention") {
                Ok("PutObjectRetention")
            } else if query_has(query, "versioning") {
                Ok("PutBucketVersioning")
            } else if key.is_some() {
                Ok("PutObject")
            } else {
                Err(s3s::s3_error!(
                    NotImplemented,
                    "unsupported access point operation"
                ))
            }
        }
        Method::POST => {
            if query_has(query, "uploads") {
                Ok("CreateMultipartUpload")
            } else if query_has(query, "uploadId") {
                Ok("CompleteMultipartUpload")
            } else if query_has(query, "restore") {
                // S4b：内层 RestoreObject 已启用（restore 查询随 call() 原样转发）
                Ok("RestoreObject")
            } else if query_has(query, "delete") {
                // 键在请求体：逐键授权在 service.rs 里叠 AP 策略（check_access 只校验形状）
                Ok("DeleteObjects")
            } else {
                Err(s3s::s3_error!(
                    NotImplemented,
                    "unsupported access point operation"
                ))
            }
        }
        Method::DELETE => {
            if query_has(query, "uploadId") {
                Ok("AbortMultipartUpload")
            } else if query_has(query, "tagging") {
                Ok("DeleteObjectTagging")
            } else if key.is_some() {
                Ok("DeleteObject")
            } else {
                Err(s3s::s3_error!(
                    NotImplemented,
                    "unsupported access point operation"
                ))
            }
        }
        _ => Err(s3s::s3_error!(
            NotImplemented,
            "unsupported access point operation"
        )),
    }
}

const SIGNED_QUERY: [&str; 7] = [
    "X-Amz-Signature",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Security-Token",
    "X-Amz-Algorithm",
];

fn strip_signed_query(query: &str) -> String {
    query
        .split('&')
        .filter(|p| {
            let k = p.split('=').next().unwrap_or("");
            !SIGNED_QUERY.contains(&k) && !k.starts_with("X-Amz-")
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// DeleteObjects 请求体：1 MiB / 1000 键硬上限。字节用 `store_all_limited` 存回 Body，
/// 内层 op 照常反序列化（s3s 的 Body 可重放），授权只看键、不改写请求。
async fn validate_delete_body(req: &mut S3Request<s3s::Body>) -> S3Result<()> {
    let bytes = req
        .input
        .store_all_limited(AP_DELETE_MAX_BODY)
        .await
        .map_err(|e| {
            if e.downcast_ref::<s3s::BodySizeLimitExceeded>().is_some() {
                s3s::s3_error!(EntityTooLarge, "delete request body exceeds 1 MiB")
            } else {
                tracing::error!(error = %e, "读取 DeleteObjects 请求体失败");
                s3s::s3_error!(InternalError, "internal error")
            }
        })?;
    parse_delete_xml(&bytes).map(|_| ())
}

/// 校验 Delete XML 形状；返回对象数。空体 = MissingRequestBodyError（AWS 语义）。
fn parse_delete_xml(bytes: &[u8]) -> S3Result<usize> {
    if bytes.is_empty() {
        return Err(s3s::s3_error!(
            MissingRequestBodyError,
            "request body is empty"
        ));
    }
    let mut d = s3s::xml::Deserializer::new(bytes);
    let delete = <s3s::dto::Delete as s3s::xml::Deserialize<'_>>::deserialize(&mut d)
        .map_err(|_| s3s::s3_error!(MalformedXML, "malformed XML"))?;
    d.expect_eof()
        .map_err(|_| s3s::s3_error!(MalformedXML, "malformed XML"))?;
    // AWS：>1000 键按 MalformedXML 拒绝（官方错误表如此，不是 InvalidRequest）。
    if delete.objects.len() > AP_DELETE_MAX_KEYS {
        return Err(s3s::s3_error!(
            MalformedXML,
            "the request has more than 1000 keys"
        ));
    }
    Ok(delete.objects.len())
}

#[async_trait::async_trait]
impl s3s::route::S3Route for ApRoute {
    fn is_match(
        &self,
        method: &http::Method,
        uri: &http::Uri,
        headers: &http::HeaderMap,
        extensions: &mut http::Extensions,
    ) -> bool {
        // 别名先于 fallback：未知别名必须 404，绝不能掉进桶路由（桶名已保留该后缀）。
        if uri.path().starts_with("/ap/")
            || host_alias(uri, headers).is_some()
            || path_alias(uri).is_some()
        {
            extensions.insert(ApMatch(true));
            return true;
        }
        if let Some(f) = &self.fallback
            && f.is_match(method, uri, headers, extensions)
        {
            extensions.insert(ApMatch(false));
            return true;
        }
        false
    }

    async fn check_access(&self, req: &mut S3Request<s3s::Body>) -> S3Result<()> {
        if !req.extensions.get::<ApMatch>().map(|m| m.0).unwrap_or(true) {
            return match &self.fallback {
                Some(f) => f.check_access(req).await,
                None => Err(s3s::s3_error!(NotImplemented)),
            };
        }
        let Some(cred) = req.credentials.as_ref() else {
            return Err(s3s::s3_error!(AccessDenied, "Authentication required"));
        };
        let ak = cred.access_key.clone();
        let user = self
            .acl
            .user_of_key(&ak)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "AP 鉴权查询失败");
                s3s::s3_error!(InternalError, "internal error")
            })?
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "unknown access key"))?;
        let (name, raw_suffix) = parse_ap_target(&req.uri, &req.headers)?;
        let ap = self.load_ap(&name).await?;
        let peer = req.extensions.get::<ApPeer>().map(|p| p.0);
        let canonical = percent_decode_strict(&raw_suffix)?;
        let key: Option<&str> = (!canonical.is_empty()).then_some(canonical.as_str());
        let copy_src: Option<(String, String)> = if req.method == http::Method::PUT {
            req.headers
                .get(s3s::header::X_AMZ_COPY_SOURCE)
                .and_then(|v| v.to_str().ok())
                .map(s3s::dto::CopySource::parse)
                .and_then(|r| match r {
                    Ok(s3s::dto::CopySource::Bucket { bucket, key, .. }) => {
                        Some((bucket.to_string(), key.to_string()))
                    }
                    _ => None,
                })
        } else {
            None
        };
        let op = derive_op(
            &req.method,
            key,
            req.uri.query().unwrap_or(""),
            copy_src.is_some(),
        )?;
        let (base_allow, base_detail) = self
            .acl
            .check_op(
                &user,
                &ak,
                op,
                Some(&ap.bucket),
                key,
                copy_src.as_ref().map(|(b, k)| (b.as_str(), k.as_str())),
            )
            .await?;
        // DeleteObjects 的键在请求体里：整体判定必然失真，逐键交集在 service 里做。
        let ap_policy = if op == "DeleteObjects" {
            None
        } else {
            ap_decision(&ap, &user, op, key, peer)
        };
        let allow = base_allow && ap_policy != Some(false);
        let mut parts: Vec<String> = Vec::new();
        if let Some(d) = base_detail {
            parts.push(d);
        }
        if let Some(v) = ap_policy {
            parts.push(if v { "ap:allow" } else { "ap:deny" }.to_string());
        } else if op == "DeleteObjects" && ap.policy.is_some() {
            parts.push("ap:per-key".to_string());
        }
        let detail = (!parts.is_empty()).then(|| parts.join(";"));
        if let Err(e) = self
            .acl
            .write_audit(
                user.uid,
                op,
                Some(&ap.bucket),
                key,
                if allow { "allow" } else { "deny" },
                detail.as_deref(),
            )
            .await
        {
            tracing::warn!(error = %e, "AP 审计落行失败");
        }
        if !allow {
            tracing::warn!(uid = user.uid, op, ap = %name, "AP 授权拒绝");
            return Err(s3s::s3_error!(AccessDenied, "access denied"));
        }
        if op == "DeleteObjects" {
            validate_delete_body(req).await?;
            req.extensions.insert(ApDeletePolicy {
                ap: ap.clone(),
                source_ip: peer,
            });
        }
        req.extensions.insert(user);
        req.extensions.insert(ApInvocation {
            ap_name: name,
            bucket: ap.bucket,
            raw_suffix,
            canonical_key: key.map(str::to_string),
            op,
        });
        Ok(())
    }

    async fn call(&self, req: S3Request<s3s::Body>) -> S3Result<S3Response<s3s::Body>> {
        if !req.extensions.get::<ApMatch>().map(|m| m.0).unwrap_or(true) {
            return match &self.fallback {
                Some(f) => f.call(req).await,
                None => Err(s3s::s3_error!(NotImplemented)),
            };
        }
        let Some(inv) = req.extensions.get::<ApInvocation>().cloned() else {
            return Err(s3s::s3_error!(
                InternalError,
                "access point context missing"
            ));
        };
        // 全量配置未加载/加载失败时无从得知该 AP 是否有变换器：fail-closed，绝不直通原文。
        if !self.olap.is_loaded() {
            return Err(s3s::s3_error!(InternalError, "transform cache unavailable"));
        }
        // S2：配置了变换器的 AP 只允许 GET/HEAD；写操作 405。Range/partNumber 会让
        // 内层返回局部字节，变换后 ETag/长度语义不再成立 → 400。
        let transform = match self.olap.get(&inv.ap_name) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(error = %e, ap = %inv.ap_name, "变换器缓存不可用");
                return Err(s3s::s3_error!(InternalError, "transform cache unavailable"));
            }
        };
        if transform.is_some() {
            if !matches!(req.method, http::Method::GET | http::Method::HEAD) {
                return Err(s3s::s3_error!(
                    MethodNotAllowed,
                    "object transform only supports GET/HEAD"
                ));
            }
            if inv.op == "GetObject"
                && (req.headers.contains_key(http::header::RANGE)
                    || query_has(req.uri.query().unwrap_or(""), "partNumber"))
            {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "Range/partNumber is not supported with object transform"
                ));
            }
        }
        let path = if inv.raw_suffix.is_empty() {
            format!("/{}", inv.bucket)
        } else {
            format!("/{}/{}", inv.bucket, inv.raw_suffix)
        };
        let query = strip_signed_query(req.uri.query().unwrap_or(""));
        let path_q = if query.is_empty() {
            path
        } else {
            format!("{path}?{query}")
        };
        let uri: http::Uri = path_q
            .parse()
            .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid rewritten URI"))?;
        let mut builder = http::Request::builder().method(req.method).uri(uri);
        // 转发前剥离身份头；业务 x-amz-* 头保留
        let mut headers = req.headers;
        headers.remove(http::header::AUTHORIZATION);
        *builder.headers_mut().expect("builder headers") = headers;
        let mut inner_req = builder
            .body(req.input)
            .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid request"))?;
        // AuthedUser/ApDeletePolicy 等扩展必须随请求进内层：批量删逐键授权靠它们。
        inner_req.extensions_mut().extend(req.extensions);
        let resp = self.inner.call(inner_req).await.map_err(|e| {
            tracing::error!(error = ?e, "AP 内层调用失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        let (mut parts, mut body) = resp.into_parts();
        // 仅 200 的 GetObject 体走变换；304/412/403/404 等原样回（条件求值已在内层完成）。
        if let Some(t) = &transform
            && parts.status == http::StatusCode::OK
            && inv.op == "GetObject"
        {
            // s3s 0.17 的 GetObject 生成层丢弃 S3Response.status：内层的条件 304 被序列化成
            // 200 + 默认空输出。成功对象必带 ETag，据此识别并还原 304，且不调用变换器。
            if !parts.headers.contains_key(http::header::ETAG) {
                return Ok(S3Response {
                    output: s3s::Body::empty(),
                    status: Some(http::StatusCode::NOT_MODIFIED),
                    headers: parts.headers,
                    extensions: parts.extensions,
                });
            }
            body = apply_transform(t, &inv, &mut parts, body).await?;
        }
        Ok(S3Response {
            output: body,
            status: Some(parts.status),
            headers: parts.headers,
            extensions: parts.extensions,
        })
    }
}

/// 读入原对象 → 变换 → 重写表示层头。原始 Last-Modified/meta/version-id/
/// storage-class/restore/锁/SSE-C 响应头保留；Content-Length/ETag/Content-Type 按变换结果重算。
async fn apply_transform(
    t: &crate::olap::OlapTransform,
    inv: &ApInvocation,
    parts: &mut http::response::Parts,
    mut body: s3s::Body,
) -> S3Result<s3s::Body> {
    // 先占并发槽再读体：输入缓冲、变换与输出都在 16 路闸内。
    let slot = crate::olap::acquire_transform_slot().await.map_err(|e| {
        tracing::error!(error = %e, "变换并发槽获取失败");
        s3s::s3_error!(InternalError, "object transform failed")
    })?;
    let limit = crate::olap::max_input_bytes();
    let input = body.store_all_limited(limit).await.map_err(|e| {
        if e.downcast_ref::<s3s::BodySizeLimitExceeded>().is_some() {
            s3s::s3_error!(EntityTooLarge, "object exceeds transform input limit")
        } else {
            tracing::error!(error = %e, "读取待变换对象失败");
            s3s::s3_error!(InternalError, "internal error")
        }
    })?;
    let etag =
        header_str(&parts.headers, http::header::ETAG).map(|s| s.trim_matches('"').to_string());
    let version_id = header_str(&parts.headers, s3s::header::X_AMZ_VERSION_ID).map(str::to_string);
    let out = t
        .transform(
            crate::olap::OlapInput {
                bucket: &inv.bucket,
                key: inv.canonical_key.as_deref().unwrap_or_default(),
                version_id: version_id.as_deref(),
                etag: etag.as_deref(),
                bytes: input,
            },
            &slot,
        )
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, ap = %inv.ap_name, "对象变换失败");
            s3s::s3_error!(InternalError, "object transform failed")
        })?;
    use md5::Digest as _;
    let etag = hex::encode(md5::Md5::digest(&out.bytes));
    let headers = &mut parts.headers;
    headers.insert(
        http::header::CONTENT_LENGTH,
        http::HeaderValue::from(out.bytes.len() as u64),
    );
    headers.insert(
        http::header::ETAG,
        http::HeaderValue::from_str(&format!("\"{etag}\"")).expect("hex ETag 恒合法"),
    );
    headers.remove(http::header::RANGE);
    headers.remove(http::header::ACCEPT_RANGES);
    headers.remove(http::header::CONTENT_RANGE);
    // 校验和头描述的是原文字节，变换后不再成立。
    let stale: Vec<http::HeaderName> = headers
        .keys()
        .filter(|k| k.as_str().starts_with("x-amz-checksum-"))
        .cloned()
        .collect();
    for k in stale {
        headers.remove(k);
    }
    if let Some(ct) = out.content_type
        && let Ok(v) = http::HeaderValue::from_str(&ct)
    {
        headers.insert(http::header::CONTENT_TYPE, v);
    }
    Ok(out.bytes.into())
}

fn header_str(headers: &http::HeaderMap, name: http::HeaderName) -> Option<&str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<ApPolicy, String> {
        let p: ApPolicy = serde_json::from_str(s).map_err(|e| e.to_string())?;
        p.validate()?;
        Ok(p)
    }

    #[test]
    fn strict_policy_rejects_unknown_shapes() {
        for bad in [
            r#"{"Version":"2012-10-17","Statement":[]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{}}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","NotAction":"s3:*"}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*","Service":"x"},"Action":"s3:GetObject","Resource":"arn:x"}]}"#,
            r#"{"Statement":[{"Effect":"Dney","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x"}]}"#,
            // 未知条件运算符/条件键：拒绝而非静默忽略
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"StringEquals":{"aws:SourceIp":"10.0.0.0/8"}}}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"IpAddress":{"aws:SourceVpc":"vpc-1"}}}]}"#,
            // 畸形 CIDR 在保存时即拒
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/33"}}}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"IpAddress":{"aws:SourceIp":[]}}}]}"#,
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    /// I2：POST `?restore` 推导为 RestoreObject（其余未知 POST 形状仍 501）。
    #[test]
    fn derive_op_post_shapes() {
        use http::Method;
        assert_eq!(
            derive_op(&Method::POST, Some("k"), "restore", false).unwrap(),
            "RestoreObject"
        );
        assert_eq!(
            derive_op(&Method::POST, Some("k"), "restore&versionId=v1", false).unwrap(),
            "RestoreObject"
        );
        assert_eq!(
            derive_op(&Method::POST, None, "uploads", false).unwrap(),
            "CreateMultipartUpload"
        );
        // 批量删：桶级 POST ?delete（SDK 两种写法）
        assert_eq!(
            derive_op(&Method::POST, None, "delete", false).unwrap(),
            "DeleteObjects"
        );
        assert_eq!(
            derive_op(&Method::POST, None, "delete=", false).unwrap(),
            "DeleteObjects"
        );
        assert!(derive_op(&Method::POST, None, "acl", false).is_err());
    }

    #[test]
    fn ap_evaluate_deny_wins_and_matching() {
        let p = parse(
            r#"{"Statement":[
              {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"},
              {"Effect":"Deny","Principal":{"AWS":"uid:7"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*"}
            ]}"#,
        )
        .unwrap();
        let req = |key: &str| ApEvalRequest {
            ak: "AKIA_X",
            uid: 7,
            resources: vec![format!("arn:aws:s3:::bkt/{key}")],
            source_ip: None,
        };
        assert_eq!(p.evaluate(&req("open"), "s3:GetObject"), Some(true));
        assert_eq!(p.evaluate(&req("secret/x"), "s3:GetObject"), Some(false));
        assert_eq!(p.evaluate(&req("open"), "s3:PutObject"), None);
    }

    #[test]
    fn source_ip_condition_allow_deny_and_fail_closed() {
        let p = parse(
            r#"{"Statement":[
              {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*",
               "Condition":{"IpAddress":{"aws:SourceIp":["10.0.0.0/8","2001:db8::/32"]}}},
              {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*",
               "Condition":{"NotIpAddress":{"aws:SourceIp":"10.1.0.0/16"}}}
            ]}"#,
        )
        .unwrap();
        let eval = |key: &str, ip: &str| {
            p.evaluate(
                &ApEvalRequest {
                    ak: "AKIA_X",
                    uid: 7,
                    resources: vec![format!("arn:aws:s3:::bkt/{key}")],
                    source_ip: ip.parse().ok(),
                },
                "s3:GetObject",
            )
        };
        assert_eq!(eval("open", "10.2.3.4"), Some(true));
        assert_eq!(eval("open", "2001:db8:1::1"), Some(true));
        // v4-mapped v6 归一后仍命中 v4 网段
        assert_eq!(eval("open", "::ffff:10.2.3.4"), Some(true));
        assert_eq!(eval("open", "192.168.1.1"), None, "源 IP 不匹配 → 无 Allow");
        assert_eq!(eval("open", ""), None, "无对端 → 条件无法求值，fail-closed");
        // NotIpAddress：不在名单内才满足 Deny 条件
        assert_eq!(eval("secret/x", "10.2.3.4"), Some(false));
        assert_eq!(
            eval("secret/x", "10.1.2.3"),
            Some(true),
            "在 NotIpAddress 名单内 → Deny 条件不成立"
        );
        assert_eq!(eval("secret/x", ""), None, "缺对端时带条件语句一律不匹配");
    }

    #[test]
    fn cidr_parse_and_prefix_boundaries() {
        let c = |s: &str| parse_cidr(s).unwrap();
        assert!(c("10.1.2.3/8").contains("10.200.0.1".parse().unwrap()));
        assert!(!c("10.1.2.3/8").contains("11.0.0.1".parse().unwrap()));
        assert!(c("10.0.0.0/0").contains("203.0.113.9".parse().unwrap()));
        assert!(c("2001:db8::/33").contains("2001:db8:7fff::1".parse().unwrap()));
        assert!(!c("2001:db8::/33").contains("2001:db8:8000::1".parse().unwrap()));
        assert!(c("192.168.1.1").contains("192.168.1.1".parse().unwrap()));
        assert!(!c("192.168.1.1").contains("192.168.1.2".parse().unwrap()));
        for bad in [
            "",
            "10.0.0.0/33",
            "::/129",
            "1.2.3.4/x",
            "::ffff:1.2.3.4/120", // 归一为 v4 后 /120 越界 → 畸形
            "not-an-ip",
        ] {
            assert!(parse_cidr(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn alias_target_forms_resolve_name_and_suffix() {
        let mk = |path: &str, host: Option<&str>| {
            let mut h = http::HeaderMap::new();
            if let Some(v) = host {
                h.insert(http::header::HOST, v.parse().unwrap());
            }
            parse_ap_target(&path.parse().unwrap(), &h)
        };
        let (n, s) = mk("/ap/ap1/dir/x", None).unwrap();
        assert_eq!((n.as_str(), s.as_str()), ("ap1", "dir/x"));
        // 别名路径（首段即桶位）
        let (n, s) = mk("/ap1-s3alias/dir/x", None).unwrap();
        assert_eq!((n.as_str(), s.as_str()), ("ap1", "dir/x"));
        let (n, s) = mk("/ap1-s3alias", None).unwrap();
        assert_eq!((n.as_str(), s.as_str()), ("ap1", ""));
        // Host 形式：端口/大小写/末尾点都归一
        let (n, s) = mk("/dir/x", Some("AP1-s3alias.example.com:9000")).unwrap();
        assert_eq!((n.as_str(), s.as_str()), ("ap1", "dir/x"));
        let (n, s) = mk("/", Some("ap1-s3alias.example.com.")).unwrap();
        assert_eq!((n.as_str(), s.as_str()), ("ap1", ""));
        // 形状合法的别名在此只解析名；是否存在由 load_ap 决定（未知 = 404，e2e 覆盖）。
        let (n, s) = mk("/nope-s3alias/k", None).unwrap();
        assert_eq!((n.as_str(), s.as_str()), ("nope", "k"));
        let (n, s) = mk("/k", Some("nope-s3alias.example.com")).unwrap();
        assert_eq!((n.as_str(), s.as_str()), ("nope", "k"));
        // 非法名（大写/太短）→ NoSuchAccessPoint（绝不回落桶路由）
        for (p, h) in [
            ("/X-s3alias/k", None),
            ("/k", Some("ap-s3alias.example.com")), // 剥离后名太短
        ] {
            let e = mk(p, h).unwrap_err();
            assert_eq!(e.code(), &s3s::S3ErrorCode::NoSuchAccessPoint, "{p} {h:?}");
        }
        // 普通桶路径不是 AP
        assert!(mk("/bucket/key", None).is_err());
        assert!(mk("/bucket/key", Some("bucket.example.com")).is_err());
    }

    #[test]
    fn delete_xml_validation_and_key_cap() {
        assert_eq!(
            parse_delete_xml(
                b"<Delete><Object><Key>a</Key></Object>\
                  <Object><Key>b</Key><VersionId>v1</VersionId></Object></Delete>"
            )
            .unwrap(),
            2
        );
        assert!(matches!(
            parse_delete_xml(b"").unwrap_err().code(),
            s3s::S3ErrorCode::MissingRequestBodyError
        ));
        assert_eq!(
            parse_delete_xml(b"<Delete><Object>").unwrap_err().code(),
            &s3s::S3ErrorCode::MalformedXML
        );
        assert_eq!(
            parse_delete_xml(b"<Delete></Delete><x/>")
                .unwrap_err()
                .code(),
            &s3s::S3ErrorCode::MalformedXML,
            "尾部杂质必须拒绝"
        );
        let build = |n: usize| {
            let mut xml = String::from("<Delete>");
            for i in 0..n {
                xml.push_str(&format!("<Object><Key>k{i}</Key></Object>"));
            }
            xml.push_str("</Delete>");
            xml
        };
        assert_eq!(parse_delete_xml(build(1000).as_bytes()).unwrap(), 1000);
        assert_eq!(
            parse_delete_xml(build(1001).as_bytes()).unwrap_err().code(),
            &s3s::S3ErrorCode::MalformedXML,
            "AWS 对 >1000 键返回 MalformedXML"
        );
    }

    async fn ap_acl() -> crate::access::Acl {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, password_hash TEXT, \
              is_admin INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT 0, \
              status TEXT NOT NULL DEFAULT 'active');\
             CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, \
              user_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, \
              created_at INTEGER NOT NULL DEFAULT 0, parent_ak TEXT, session_policy TEXT, \
              kind TEXT NOT NULL DEFAULT 'long-term');\
             CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL DEFAULT 0, \
              versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0, \
              object_lock INTEGER NOT NULL DEFAULT 0, object_lock_default_mode TEXT, \
              object_lock_default_days INTEGER, default_encryption TEXT, \
              replication_mode TEXT NOT NULL DEFAULT 'async', deleting INTEGER NOT NULL DEFAULT 0);\
             CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
              can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, \
              can_delete INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (user_id, bucket));\
             CREATE TABLE bucket_policies (bucket TEXT PRIMARY KEY, policy_json TEXT NOT NULL, \
              updated_at INTEGER NOT NULL DEFAULT 0);\
             CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, \
              uid INTEGER NOT NULL, op TEXT NOT NULL, bucket TEXT, object_key TEXT, \
              decision TEXT NOT NULL, detail TEXT);\
             INSERT INTO users (id,username,password_hash,is_admin) VALUES (7,'u','h',0);\
             INSERT INTO access_keys (access_key,secret_key,user_id) VALUES ('AKIA_X','s',7);\
             INSERT INTO buckets (name,owner_id,creation_date) VALUES ('bkt',7,0);",
        )
        .execute(&pool)
        .await
        .unwrap();
        crate::access::Acl::new(pool)
    }

    struct DummyS3;
    #[async_trait::async_trait]
    impl s3s::S3 for DummyS3 {}

    #[tokio::test]
    async fn route_check_access_canonicalizes_and_applies_ap_policy() {
        use s3s::route::S3Route as _;
        let acl = ap_acl().await;
        let cache = Arc::new(ApCache::new());
        let policy: ApPolicy = serde_json::from_str(
            r#"{"Statement":[
              {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"},
              {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:us-east-1:000000000000:accesspoint/ap1/object/secret/*"}
            ]}"#,
        )
        .unwrap();
        cache.put(AccessPoint {
            name: "ap1".into(),
            bucket: "bkt".into(),
            owner_id: 7,
            policy: Some(policy),
            region: "us-east-1".into(),
        });
        let inner = Arc::new(s3s::service::S3ServiceBuilder::new(DummyS3).build());
        struct NoSource;
        #[async_trait::async_trait]
        impl ApSource for NoSource {
            async fn lookup(&self, _n: &str) -> Result<Option<AccessPoint>, String> {
                Ok(None)
            }
        }
        let route = ApRoute::new(
            cache,
            Arc::new(NoSource),
            acl,
            Arc::new(crate::olap::OlapCache::new()),
            inner,
        );
        let mk = |path: &str| {
            let mut r = S3Request {
                input: s3s::Body::empty(),
                method: http::Method::GET,
                uri: path.parse().unwrap(),
                headers: http::HeaderMap::new(),
                extensions: http::Extensions::new(),
                credentials: Some(s3s::auth::Credentials {
                    access_key: "AKIA_X".into(),
                    secret_key: "s".into(),
                }),
                region: None,
                service: None,
                trailing_headers: None,
            };
            r.extensions.insert(ApMatch(true));
            r
        };
        let mut req = mk("/ap/ap1/open%2Ffile");
        route.check_access(&mut req).await.unwrap();
        let inv = req.extensions.get::<ApInvocation>().unwrap();
        assert_eq!(inv.bucket, "bkt");
        assert_eq!(inv.canonical_key.as_deref(), Some("open/file"));
        assert_eq!(inv.raw_suffix, "open%2Ffile");
        assert_eq!(inv.op, "GetObject");
        // AP Deny 前缀
        let mut req = mk("/ap/ap1/secret/x");
        assert!(matches!(
            route.check_access(&mut req).await,
            Err(e) if e.code() == &s3s::S3ErrorCode::AccessDenied
        ));
        // 未注册 AP → 404
        let mut req = mk("/ap/nope/k");
        assert!(matches!(
            route.check_access(&mut req).await,
            Err(e) if e.code() == &s3s::S3ErrorCode::NoSuchAccessPoint
        ));
        // 坏转义 → 400
        let mut req = mk("/ap/ap1/bad%zz");
        assert!(matches!(
            route.check_access(&mut req).await,
            Err(e) if e.code() == &s3s::S3ErrorCode::InvalidArgument
        ));
    }

    #[test]
    fn canonical_key_decoding_matrix() {
        assert_eq!(percent_decode_strict("a%2Fb").unwrap(), "a/b");
        assert_eq!(percent_decode_strict("%252F").unwrap(), "%2F");
        assert_eq!(percent_decode_strict("%E4%B8%AD%E6%96%87").unwrap(), "中文");
        assert!(percent_decode_strict("%zz").is_err());
        assert!(percent_decode_strict("%0").is_err());
        assert!(percent_decode_strict("%00").is_err());
        assert!(percent_decode_strict("a/%2E%2E/b").is_err());
        assert!(percent_decode_strict("%FF").is_err());
    }

    #[test]
    fn op_derivation_matrix() {
        use http::Method;
        assert_eq!(
            derive_op(&Method::PUT, Some("k"), "partNumber=1&uploadId=u", true).unwrap(),
            "UploadPartCopy"
        );
        assert_eq!(
            derive_op(&Method::PUT, Some("k"), "", true).unwrap(),
            "CopyObject"
        );
        assert_eq!(
            derive_op(&Method::GET, None, "", false).unwrap(),
            "ListObjects"
        );
        assert_eq!(
            derive_op(&Method::GET, None, "prefix=p&marker=m&max-keys=5", false).unwrap(),
            "ListObjects",
            "v1 列举参数不得被当作未知子资源"
        );
        assert_eq!(
            derive_op(&Method::GET, None, "x-id=ListObjects", false).unwrap(),
            "ListObjects",
            "SDK 的 x-id 查询参数不得误判"
        );
        assert_eq!(
            derive_op(&Method::GET, None, "uploads", false).unwrap(),
            "ListMultipartUploads"
        );
        assert_eq!(
            derive_op(&Method::GET, Some("k"), "uploadId=u", false).unwrap(),
            "ListParts"
        );
        assert_eq!(
            derive_op(&Method::GET, Some("k"), "retention", false).unwrap(),
            "GetObjectRetention"
        );
        assert_eq!(
            derive_op(&Method::PUT, Some("k"), "retention", false).unwrap(),
            "PutObjectRetention"
        );
        assert!(
            derive_op(&Method::GET, None, "acl", false).is_err(),
            "未知桶级子资源仍 fail-closed"
        );
        assert_eq!(
            derive_op(&Method::GET, None, "list-type=2", false).unwrap(),
            "ListObjectsV2"
        );
        assert_eq!(
            derive_op(&Method::GET, None, "versions", false).unwrap(),
            "ListObjectVersions"
        );
        assert_eq!(
            derive_op(&Method::PUT, Some("k"), "tagging", false).unwrap(),
            "PutObjectTagging"
        );
        assert_eq!(
            derive_op(&Method::DELETE, Some("k"), "uploadId=u", false).unwrap(),
            "AbortMultipartUpload"
        );
        assert_eq!(
            derive_op(&Method::POST, None, "uploads", false).unwrap(),
            "CreateMultipartUpload"
        );
        assert_eq!(
            derive_op(&Method::POST, None, "delete", false).unwrap(),
            "DeleteObjects",
            "批量删经 AP 走逐键授权"
        );
    }

    #[test]
    fn signed_query_stripped_business_query_kept() {
        let q = "versionId=v1&X-Amz-Signature=aa&X-Amz-Credential=b&X-Amz-Date=c&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Security-Token=t&X-Amz-Algorithm=AWS4-HMAC-SHA256&tagging";
        assert_eq!(strip_signed_query(q), "versionId=v1&tagging");
    }

    #[test]
    fn ap_decision_uses_ap_and_standard_arns() {
        use crate::access::AuthedUser;
        let ap = AccessPoint {
            name: "ap1".into(),
            bucket: "bkt".into(),
            owner_id: 7,
            policy: Some(
                parse(
                    r#"{"Statement":[
                      {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"},
                      {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:us-east-1:000000000000:accesspoint/ap1/object/secret/*"}
                    ]}"#,
                )
                .unwrap(),
            ),
            region: "us-east-1".into(),
        };
        let u = AuthedUser {
            uid: 7,
            is_admin: false,
            ak: "AKIA_X".into(),
        };
        assert_eq!(
            ap_decision(&ap, &u, "GetObject", Some("open"), None),
            Some(true)
        );
        assert_eq!(
            ap_decision(&ap, &u, "GetObject", Some("secret/x"), None),
            Some(false)
        );
        // 无匹配 action → 拒（有策略 = 默认收窄）
        assert_eq!(
            ap_decision(&ap, &u, "PutObject", Some("open"), None),
            Some(false)
        );
        let no_policy = AccessPoint { policy: None, ..ap };
        assert_eq!(
            ap_decision(&no_policy, &u, "PutObject", Some("open"), None),
            None
        );
    }

    #[test]
    fn cache_put_remove_and_bucket_invalidate() {
        let c = ApCache::new();
        let mk = |name: &str, bucket: &str| AccessPoint {
            name: name.into(),
            bucket: bucket.into(),
            owner_id: 1,
            policy: None,
            region: "us-east-1".into(),
        };
        c.put(mk("a1", "b1"));
        c.put(mk("a2", "b2"));
        c.put(mk("a3", "b1"));
        assert_eq!(c.len(), 3);
        assert!(c.get("a1").is_some());
        c.remove("a2");
        assert!(c.get("a2").is_none());
        c.invalidate_bucket("b1");
        assert!(c.get("a1").is_none() && c.get("a3").is_none());
        assert_eq!(c.len(), 0);
        c.replace_all([mk("x", "y")]);
        assert_eq!(c.get("x").unwrap().bucket, "y");
    }

    #[test]
    fn arn_shapes() {
        let c = ApContext::new("ap1", "eu-west-1");
        assert_eq!(
            c.object_arn("a/b"),
            "arn:aws:s3:eu-west-1:000000000000:accesspoint/ap1/object/a/b"
        );
        assert_eq!(
            c.bucket_arn(),
            "arn:aws:s3:eu-west-1:000000000000:accesspoint/ap1"
        );
    }

    /// 结构断言：并发槽在读取输入体期间已被持有（缓冲不得发生在闸外）。
    #[tokio::test]
    async fn permit_is_held_while_buffering_input() {
        use std::future::Future as _;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct Gated {
            started: Option<tokio::sync::oneshot::Sender<()>>,
            release: Option<tokio::sync::oneshot::Receiver<()>>,
            done: bool,
        }
        impl futures::Stream for Gated {
            type Item = Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>;
            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if let Some(tx) = self.started.take() {
                    let _ = tx.send(());
                }
                if self.done {
                    return Poll::Ready(None);
                }
                let release = self.release.as_mut().expect("release");
                match Pin::new(release).poll(cx) {
                    Poll::Ready(_) => {
                        self.done = true;
                        Poll::Ready(Some(Ok(bytes::Bytes::from_static(b"hello"))))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
        impl s3s::stream::ByteStream for Gated {}

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let stream = Gated {
            started: Some(started_tx),
            release: Some(release_rx),
            done: false,
        };
        let body = s3s::Body::from(Box::pin(stream) as s3s::stream::DynByteStream);
        let t = crate::olap::OlapTransform::broken("test".into());
        let inv = ApInvocation {
            ap_name: "ap1".into(),
            bucket: "bkt".into(),
            raw_suffix: "k".into(),
            canonical_key: Some("k".into()),
            op: "GetObject",
        };
        let mut parts = http::Response::new(()).into_parts().0;
        let before = crate::olap::available_transform_slots();
        let task = tokio::spawn(async move { apply_transform(&t, &inv, &mut parts, body).await });

        started_rx.await.unwrap();
        assert_eq!(
            crate::olap::available_transform_slots(),
            before - 1,
            "读体期间必须持有并发槽"
        );
        release_tx.send(()).unwrap();
        assert!(task.await.unwrap().is_err());
        assert_eq!(
            crate::olap::available_transform_slots(),
            before,
            "结束后归还槽"
        );
    }
}
