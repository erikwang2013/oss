//! M8 S1：访问点（Access Point）——模型、严格策略解析与 op 推导。
//!
//! 路由形态只支持路径式 `/ap/<name>/<key>`；AP 只做**额外限制**（与身份/桶策略取交集），
//! 绝不放松既有授权。策略方言 = 现有 `policy` 子集，但解析更严格（未知字段/条件一律拒）。

use std::sync::Arc;

use s3s::{S3Request, S3Response, S3Result};
use serde::Deserialize;

use crate::policy::{StringOrVec, glob_match};

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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApPrincipal {
    #[serde(default, rename = "AWS")]
    pub aws: StringOrVec,
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
) -> Option<bool> {
    let policy = ap.policy.as_ref()?;
    let action = if op == "CopyObject" {
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
            } else if query_has(query, "tagging") {
                Ok("GetObjectTagging")
            } else if query_value(query, "list-type") == Some("2") {
                Ok("ListObjectsV2")
            } else if query_has(query, "versioning") {
                Ok("GetBucketVersioning")
            } else if key.is_some() {
                Ok("GetObject")
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
                    Err(s3s::s3_error!(
                        NotImplemented,
                        "UploadPartCopy is not supported"
                    ))
                } else {
                    Ok("UploadPart")
                }
            } else if copy_source {
                Ok("CopyObject")
            } else if query_has(query, "tagging") {
                Ok("PutObjectTagging")
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
            } else {
                // restore（S4b 启用）/ delete（批量删不支持）/ 其余
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

#[async_trait::async_trait]
impl s3s::route::S3Route for ApRoute {
    fn is_match(
        &self,
        method: &http::Method,
        uri: &http::Uri,
        headers: &http::HeaderMap,
        extensions: &mut http::Extensions,
    ) -> bool {
        if uri.path().starts_with("/ap/") {
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
        let (name, raw_suffix) = parse_ap_path(&req.uri)?;
        let ap = self.load_ap(&name).await?;
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
        let ap_decision = ap_decision(&ap, &user, op, key);
        let allow = base_allow && ap_decision != Some(false);
        let mut parts: Vec<String> = Vec::new();
        if let Some(d) = base_detail {
            parts.push(d);
        }
        if let Some(v) = ap_decision {
            parts.push(if v { "ap:allow" } else { "ap:deny" }.to_string());
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
        // S2：配置了变换器的 AP 只允许 GET/HEAD；写操作 405。Range/partNumber 会让
        // 内层返回局部字节，变换后 ETag/长度语义不再成立 → 400。
        let transform = self.olap.get(&inv.ap_name);
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
        let inner_req = builder
            .body(req.input)
            .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid request"))?;
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
        .transform(crate::olap::OlapInput {
            bucket: &inv.bucket,
            key: inv.canonical_key.as_deref().unwrap_or_default(),
            version_id: version_id.as_deref(),
            etag: etag.as_deref(),
            bytes: input,
        })
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
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
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
        };
        assert_eq!(p.evaluate(&req("open"), "s3:GetObject"), Some(true));
        assert_eq!(p.evaluate(&req("secret/x"), "s3:GetObject"), Some(false));
        assert_eq!(p.evaluate(&req("open"), "s3:PutObject"), None);
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
              object_lock_default_days INTEGER);\
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
        let copy = |q: &str| {
            derive_op(&Method::PUT, Some("k"), q, true)
                .map_err(|e| format!("{:?}", e.code()))
                .unwrap_err()
        };
        assert_eq!(copy("partNumber=1&uploadId=u"), "NotImplemented");
        assert_eq!(
            derive_op(&Method::PUT, Some("k"), "", true).unwrap(),
            "CopyObject"
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
        assert!(
            derive_op(&Method::POST, Some("k"), "delete", false).is_err(),
            "批量删不支持"
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
        assert_eq!(ap_decision(&ap, &u, "GetObject", Some("open")), Some(true));
        assert_eq!(
            ap_decision(&ap, &u, "GetObject", Some("secret/x")),
            Some(false)
        );
        // 无匹配 action → 拒（有策略 = 默认收窄）
        assert_eq!(ap_decision(&ap, &u, "PutObject", Some("open")), Some(false));
        let no_policy = AccessPoint { policy: None, ..ap };
        assert_eq!(ap_decision(&no_policy, &u, "PutObject", Some("open")), None);
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
}
