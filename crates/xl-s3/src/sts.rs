//! M5 切片 5：STS 临时凭据——AssumeRole / GetSessionToken / AssumeRoleWithWebIdentity。
//!
//! 形态：s3s `S3Route` 前置拦截（POST `/` + form-urlencoded）；签名请求由管线先校验
//! SigV4。`check_access` 覆写为放行：`AssumeRoleWithWebIdentity` 的凭据是 OIDC token
//! （无需 AK/SK），其余 Action 在 `call` 内逐条要求签名（未启用 OIDC 时该 Action 拒绝）。
//! 响应 XML（`text/xml`）；错误走 s3s 的 S3 错误 XML（STS `ErrorResponse` 形态差异记档）。
//! 会话行落 `access_keys`（kind='session'、parent_ak、expires_at、session_policy）；
//! 父链条件在 auth.rs/user_of_key/cluster_control 三处同口径。WebIdentity 会话无父键
//! （匿名入口不继承任何调用者权限）。

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use http::Extensions;
use http::{HeaderMap, Method, Uri};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};

use crate::sts_web_identity::{WebIdentity, WebIdentityError};

/// 请求体上限（STS form 只有字段名值；64 KiB 富余）。
const MAX_BODY: usize = 64 * 1024;
/// 会话策略上限（AWS 4096 字符口径）。
const MAX_POLICY_LEN: usize = 4096;

/// 时长边界（AWS 口径）：GetSessionToken 900..=129600（默认 43200=12h）；
/// AssumeRole 900..=43200（默认 3600）。上限无角色策略约束（最小模型；S7 记档）。
const GST_MIN: i64 = 900;
const GST_MAX: i64 = 129_600;
const GST_DEFAULT: i64 = 43_200;
const AR_MIN: i64 = 900;
const AR_MAX: i64 = 43_200;
const AR_DEFAULT: i64 = 3600;

/// L-1：匿名 AssumeRoleWithWebIdentity 的全局令牌桶——每次 JWT 验签（RSA/ES256）
/// 远贵于拒绝一个未签名请求，限制放大面。签名 STS 路径不受影响。
/// 固定阈值（每实例）；集群多节点时总吞吐按实例数放大，运营方仍应在反代叠加限流。
const WI_RATE_PER_SEC: f64 = 20.0;
const WI_BURST: f64 = 40.0;
/// 按对端 IP 的分桶：全局桶之上再保证单个客户端不能吃掉全部配额。
/// 仅当 HTTP 层注入了对端身份（TLS listener 的 `ConnectInfo` → `ApPeer`）时生效；
/// 明文 ecat listener 无 ConnectInfo → 只有全局桶（运营方须在反代补限流）。
const WI_IP_RATE_PER_SEC: f64 = 2.0;
const WI_IP_BURST: f64 = 5.0;
/// 分桶表上限与空闲回收：超过上限先回收 60s 未活跃的桶，仍满则 fail-closed（503）。
/// 全局桶 20/s 封顶总增速，表满只可能来自分布式攻击；此时拒绝优于无界内存增长。
const WI_IP_BUCKETS_MAX: usize = 4096;
const WI_IP_BUCKET_IDLE: Duration = Duration::from_secs(60);

struct TokenBucket {
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(burst: f64) -> Self {
        Self {
            tokens: burst,
            last: Instant::now(),
        }
    }

    fn allow(&mut self, rate: f64, burst: f64) -> bool {
        let now = Instant::now();
        self.tokens = (self.tokens + now.duration_since(self.last).as_secs_f64() * rate).min(burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// 每个对端 IP 一个令牌桶；`last` 同时充当空闲时间游标（每次 `allow` 刷新）。
#[derive(Default)]
struct IpBuckets {
    buckets: HashMap<IpAddr, TokenBucket>,
}

impl IpBuckets {
    fn allow(&mut self, ip: IpAddr) -> bool {
        if self.buckets.len() >= WI_IP_BUCKETS_MAX && !self.buckets.contains_key(&ip) {
            self.buckets
                .retain(|_, b| b.last.elapsed() < WI_IP_BUCKET_IDLE);
            if self.buckets.len() >= WI_IP_BUCKETS_MAX {
                return false; // 表满且无可回收：fail-closed
            }
        }
        self.buckets
            .entry(ip)
            .or_insert_with(|| TokenBucket::new(WI_IP_BURST))
            .allow(WI_IP_RATE_PER_SEC, WI_IP_BURST)
    }
}

pub struct StsRoute {
    /// 对象后端会话写入缝（唯一写路径；缺失 = fail-closed）。
    writes: Option<std::sync::Arc<dyn crate::control::StsWrites>>,
    /// M5 切片 6（T5 校正）：SK 加密库——签发会话的 SK 与 console 同口径封存入库
    /// （`issue` 是 `access_keys` 的**第二个写点**，切片 6 核查轮补齐）。
    vault: Option<std::sync::Arc<crate::skvault::SecretVault>>,
    /// AssumeRoleWithWebIdentity 的 OIDC 验证器；`None` = 未启用（该 Action 一律拒绝）。
    web_identity: Option<WebIdentity>,
    /// L-1：匿名 WebIdentity 验签入口的限流桶（锁中毒按未中毒处理，不让 panic 拖垮 STS）。
    web_identity_limiter: Mutex<TokenBucket>,
    /// L-1b：按对端 IP 的子桶（仅有对端身份时生效）。
    web_identity_ip_limiter: Mutex<IpBuckets>,
}

impl StsRoute {
    pub fn new() -> Self {
        Self {
            writes: None,
            vault: None,
            web_identity: None,
            web_identity_limiter: Mutex::new(TokenBucket::new(WI_BURST)),
            web_identity_ip_limiter: Mutex::new(IpBuckets::default()),
        }
    }

    /// 注入对象后端会话写入缝（唯一写路径；缺失 = fail-closed）。
    pub fn with_writes(
        mut self,
        writes: Option<std::sync::Arc<dyn crate::control::StsWrites>>,
    ) -> Self {
        self.writes = writes;
        self
    }

    /// M5 切片 6：接 SK 加密库（builder 风格；router 装配处调用）。
    pub fn with_vault(
        mut self,
        vault: Option<std::sync::Arc<crate::skvault::SecretVault>>,
    ) -> Self {
        self.vault = vault;
        self
    }

    /// 接 OIDC 验证器（builder 风格；`None` 保持功能关闭）。配置在启动期已校验。
    pub fn with_web_identity(mut self, web_identity: Option<WebIdentity>) -> Self {
        self.web_identity = web_identity;
        self
    }

    /// AssumeRole（S6）：角色 = 用户别名（ARN `arn:...:iam::<acct>:role/<name>`）；
    /// **调用者须管理员**——无角色信任策略的最小模型里，这是不引入提权面的唯一取法
    /// （admin 已全权，铸造任何角色用户会话不构成提权；非 admin → AccessDenied，记档）。
    async fn assume_role(
        &self,
        caller_ak: &str,
        fields: &[(String, String)],
    ) -> S3Result<S3Response<Body>> {
        let (_, is_admin) = self
            .caller_of(caller_ak)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "unknown access key"))?;
        if !is_admin {
            return Err(s3s::s3_error!(
                AccessDenied,
                "only the administrator may assume a role"
            ));
        }
        let arn = field(fields, "RoleArn")
            .ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing RoleArn"))?;
        let role = parse_role_arn(&arn)
            .ok_or_else(|| s3s::s3_error!(InvalidRequest, "invalid RoleArn"))?;
        let role_uid = self.view()?.active_user_uid(&role).await.map_err(|e| {
            tracing::error!(error = %e, role, "control view role lookup failed");
            s3s::s3_error!(InternalError, "internal error")
        })?;
        let Some(role_uid) = role_uid else {
            return Err(s3s::s3_error!(AccessDenied, "role not found or disabled"));
        };
        let dur = duration(fields, "DurationSeconds", AR_DEFAULT, AR_MIN, AR_MAX)?;
        let policy = session_policy(fields)?;
        let session_name = field(fields, "RoleSessionName").unwrap_or_else(|| "session".into());
        let (ak, sk, token) = issue_with(
            self.writes.as_deref(),
            self.vault.as_deref(),
            role_uid,
            Some(caller_ak),
            dur,
            policy.as_deref(),
        )
        .await?;
        let expires = now_secs() + dur;
        let account = "oss"; // 单租户简化（S6）：acct 段不校验，回显固定
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <AssumeRoleResult>\
             <Credentials>\
             <AccessKeyId>{ak}</AccessKeyId>\
             <SecretAccessKey>{sk}</SecretAccessKey>\
             <SessionToken>{token}</SessionToken>\
             <Expiration>{exp}</Expiration>\
             </Credentials>\
             <AssumedRoleUser>\
             <Arn>arn:oss:iam::{account}:assumed-role/{role}/{name}</Arn>\
             <AssumedRoleId>AROA{role_uid}:{name}</AssumedRoleId>\
             </AssumedRoleUser>\
             </AssumeRoleResult>\
             <ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata>\
             </AssumeRoleResponse>",
            exp = iso8601(expires),
            rid = request_id(),
            name = xml_escape(&session_name),
        );
        Ok(xml_response(body))
    }

    /// GetSessionToken（S1）：自凭据派生更短 TTL 的会话；调用者须 long-term 长时键
    /// （AWS：临时凭据不可再签 GetSessionToken）。不收 Policy（AWS 无此参数；带了忽略）。
    async fn get_session_token(
        &self,
        caller_ak: &str,
        fields: &[(String, String)],
    ) -> S3Result<S3Response<Body>> {
        let (uid, _) = self
            .caller_of(caller_ak)
            .await?
            .ok_or_else(|| s3s::s3_error!(AccessDenied, "unknown access key"))?;
        let dur = duration(fields, "DurationSeconds", GST_DEFAULT, GST_MIN, GST_MAX)?;
        let (ak, sk, token) = issue_with(
            self.writes.as_deref(),
            self.vault.as_deref(),
            uid,
            Some(caller_ak),
            dur,
            None,
        )
        .await?;
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <GetSessionTokenResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <GetSessionTokenResult>\
             <Credentials>\
             <AccessKeyId>{ak}</AccessKeyId>\
             <SecretAccessKey>{sk}</SecretAccessKey>\
             <SessionToken>{token}</SessionToken>\
             <Expiration>{exp}</Expiration>\
             </Credentials>\
             </GetSessionTokenResult>\
             <ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata>\
             </GetSessionTokenResponse>",
            exp = iso8601(now_secs() + dur),
            rid = request_id(),
        );
        Ok(xml_response(body))
    }

    /// AssumeRoleWithWebIdentity：凭据来自 OIDC token，**不要求 AK/SK 签名**。
    /// 仅当 `[sts.web_identity] enabled=true` 时开放；token 校验、角色映射、时长全部
    /// fail-closed（见 `sts_web_identity`）。签发的会话 `parent_ak = NULL`：匿名入口
    /// 不继承任何调用者父链，权限只来自映射到的本地角色用户。
    async fn assume_role_with_web_identity(
        &self,
        fields: &[(String, String)],
        peer: Option<IpAddr>,
    ) -> S3Result<S3Response<Body>> {
        let Some(wi) = self.web_identity.as_ref() else {
            return Err(s3s::s3_error!(
                AccessDenied,
                "AssumeRoleWithWebIdentity is not enabled"
            ));
        };
        // L-1：匿名入口在验签前限流（CPU 放大防护）；仅此 Action，签名路径不变。
        let allowed = self
            .web_identity_limiter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .allow(WI_RATE_PER_SEC, WI_BURST);
        // L-1b：有对端身份时再扣单 IP 桶——一个客户端耗尽自己的配额不影响其他 IP。
        let allowed = allowed
            && peer.is_none_or(|ip| {
                self.web_identity_ip_limiter
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .allow(ip)
            });
        if !allowed {
            return Err(s3s::s3_error!(
                SlowDown,
                "AssumeRoleWithWebIdentity rate limit exceeded"
            ));
        }
        let token = field(fields, "WebIdentityToken")
            .ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing WebIdentityToken"))?;
        let arn = field(fields, "RoleArn")
            .ok_or_else(|| s3s::s3_error!(InvalidRequest, "missing RoleArn"))?;
        let requested = match field(fields, "DurationSeconds") {
            Some(raw) => Some(
                raw.parse::<i64>()
                    .map_err(|_| s3s::s3_error!(InvalidRequest, "invalid DurationSeconds"))?,
            ),
            None => None,
        };
        let verified = wi
            .verify(&token, &arn, requested)
            .map_err(web_identity_error)?;
        let role_uid = self
            .view()?
            .active_user_uid(&verified.role)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, role = %verified.role, "control view role lookup failed");
                s3s::s3_error!(InternalError, "internal error")
            })?;
        let Some(role_uid) = role_uid else {
            // 映射命中但本地角色不存在/停用：拒绝，绝不回退到其他用户。
            return Err(s3s::s3_error!(
                AccessDenied,
                "web identity role not found or disabled"
            ));
        };
        let policy = session_policy(fields)?;
        let session_name = web_identity_session_name(fields)?;
        let (ak, sk, session_token) = issue_with(
            self.writes.as_deref(),
            self.vault.as_deref(),
            role_uid,
            None,
            verified.duration_seconds,
            policy.as_deref(),
        )
        .await?;
        let expires = now_secs() + verified.duration_seconds;
        let account = "oss"; // 单租户简化：acct 段不校验，回显固定
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <AssumeRoleWithWebIdentityResult>\
             <SubjectFromWebIdentityToken>{subject}</SubjectFromWebIdentityToken>\
             <AssumedRoleUser>\
             <Arn>arn:oss:iam::{account}:assumed-role/{role}/{name}</Arn>\
             <AssumedRoleId>AROA{role_uid}:{name}</AssumedRoleId>\
             </AssumedRoleUser>\
             <Audience>{audience}</Audience>\
             <Provider>{provider}</Provider>\
             <Credentials>\
             <AccessKeyId>{ak}</AccessKeyId>\
             <SecretAccessKey>{sk}</SecretAccessKey>\
             <SessionToken>{session_token}</SessionToken>\
             <Expiration>{exp}</Expiration>\
             </Credentials>\
             </AssumeRoleWithWebIdentityResult>\
             <ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata>\
             </AssumeRoleWithWebIdentityResponse>",
            subject = xml_escape(&verified.subject),
            role = xml_escape(&verified.role),
            name = xml_escape(&session_name),
            audience = xml_escape(wi.audience()),
            provider = xml_escape(wi.issuer()),
            exp = iso8601(expires),
            rid = request_id(),
        );
        Ok(xml_response(body))
    }

    /// 请求热视图（STS 只在请求作用域内运行；缺失 = fail-closed）。
    fn view(&self) -> S3Result<std::sync::Arc<dyn crate::control::Control>> {
        crate::control::request_view().ok_or_else(|| {
            tracing::error!("control view missing outside request scope");
            s3s::s3_error!(InternalError, "internal error")
        })
    }

    /// 调用者（须长时键）：uid + is_admin。会话键不得作为调用者（父链只有一层；S2）。
    async fn caller_of(&self, ak: &str) -> S3Result<Option<(i64, bool)>> {
        self.view()?
            .sts_caller(ak, now_secs())
            .await
            .map(|u| u.map(|u| (u.uid, u.is_admin)))
            .map_err(|e| {
                tracing::error!(error = %e, ak, "control view STS caller lookup failed");
                s3s::s3_error!(InternalError, "internal error")
            })
    }
}

impl Default for StsRoute {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl S3Route for StsRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        method == Method::POST
            && uri.path() == "/"
            && headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("application/x-www-form-urlencoded"))
    }

    /// 放行到 `call`：`AssumeRoleWithWebIdentity` 的凭据是 OIDC token，匿名合法；
    /// 其余 Action 在 `call` 内按 Action 强制签名（未启用 OIDC 时 WebIdentity 也拒）。
    /// 带签名的请求仍先经 s3s 管线验签——本覆写不放宽任何签名校验。
    async fn check_access(&self, _req: &mut S3Request<Body>) -> S3Result<()> {
        Ok(())
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let caller_ak = req.credentials.as_ref().map(|c| c.access_key.clone());
        // 对端身份由 oss-server 的 HTTP 层注入（TLS listener 的 ConnectInfo；显式可信
        // 反代时换成 XFF 客户端）。明文 listener 无 ConnectInfo → None → 仅全局桶。
        let peer = req.extensions.get::<crate::ap::ApPeer>().map(|p| p.0);
        let mut body = req.input;
        let bytes = body
            .store_all_limited(MAX_BODY)
            .await
            .map_err(|_| s3s::s3_error!(InvalidRequest, "malformed request body"))?;
        let fields = parse_form(&bytes);
        match field(&fields, "Action").as_deref() {
            // OIDC token 即凭据：无需 AK/SK（是否启用由 assume_role_with_web_identity 判）
            Some("AssumeRoleWithWebIdentity") => {
                self.assume_role_with_web_identity(&fields, peer).await
            }
            Some("AssumeRole") => {
                let ak = signed_caller(caller_ak.as_deref())?;
                self.assume_role(&ak, &fields).await
            }
            Some("GetSessionToken") => {
                let ak = signed_caller(caller_ak.as_deref())?;
                self.get_session_token(&ak, &fields).await
            }
            Some(_) => Err(s3s::s3_error!(NotImplemented, "unsupported STS operation")),
            None => Err(s3s::s3_error!(InvalidRequest, "missing Action")),
        }
    }
}

/// 非 WebIdentity 的 STS Action 必须带（已验签的）AK/SK。
fn signed_caller(caller_ak: Option<&str>) -> S3Result<String> {
    caller_ak
        .map(str::to_string)
        .ok_or_else(|| s3s::s3_error!(AccessDenied, "Signature is required"))
}

/// 落一行会话（S1：AK `ASIA`+32hex / SK 32B hex / token 不落库）。
async fn issue_with(
    writes: Option<&dyn crate::control::StsWrites>,
    vault: Option<&crate::skvault::SecretVault>,
    uid: i64,
    parent_ak: Option<&str>,
    dur_secs: i64,
    session_policy: Option<&str>,
) -> S3Result<(String, String, String)> {
    let rng = |e: std::io::Error| {
        tracing::error!(error = %e, "STS 凭据随机源失败——拒绝签发（fail-closed）");
        s3s::s3_error!(InternalError, "internal error")
    };
    let ak = format!("ASIA{}", hex16().map_err(rng)?);
    let sk = hex32().map_err(rng)?;
    let token = hex32().map_err(rng)?;
    // M5 切片 6：库启用时封存入库（响应仍回明文一次——SDK 只在此刻拿到 SK）
    let stored = match vault {
        Some(v) => v.seal(&sk).await.map_err(|e| {
            tracing::error!(error = %e, "会话 SK 加密失败——拒绝签发（fail-closed）");
            s3s::s3_error!(InternalError, "internal error")
        })?,
        None => sk.clone(),
    };
    let now = now_secs();
    writes
        .ok_or_else(|| {
            tracing::error!("STS 会话写入缝缺失——拒绝签发（fail-closed）");
            s3s::s3_error!(InternalError, "internal error")
        })?
        .issue_session(
            &ak,
            &stored,
            uid,
            now + dur_secs,
            now,
            parent_ak,
            session_policy,
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "STS 会话落控制根失败");
            s3s::s3_error!(InternalError, "internal error")
        })?;
    Ok((ak, sk, token))
}

// ---------- 解析/格式（纯函数，单测直钉） ----------

fn field(fields: &[(String, String)], name: &str) -> Option<String> {
    fields
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
}

pub(crate) fn parse_role_arn(arn: &str) -> Option<String> {
    // arn:partition:service:region:account:resource —— 只约束 service=iam、resource=role/<name>
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() != 6 || parts[0] != "arn" || parts[2] != "iam" {
        return None;
    }
    let name = parts[5].strip_prefix("role/")?;
    if name.is_empty() || name.contains('/') || name.contains(' ') {
        return None;
    }
    Some(name.to_string())
}

fn duration(
    fields: &[(String, String)],
    name: &str,
    default: i64,
    min: i64,
    max: i64,
) -> S3Result<i64> {
    let Some(raw) = field(fields, name) else {
        return Ok(default);
    };
    let d: i64 = raw
        .parse()
        .map_err(|_| s3s::s3_error!(InvalidRequest, "invalid DurationSeconds"))?;
    if !(min..=max).contains(&d) {
        // AWS 的 ValidationError 码不在 s3s 词表（S7 记档）；400 InvalidArgument 语义等值
        return Err(s3s::s3_error!(
            InvalidArgument,
            "DurationSeconds out of range"
        ));
    }
    Ok(d)
}

/// WebIdentity 的 RoleSessionName：AWS 口径 `[\w+=,.@-]{1,64}`，缺省 `web-identity`。
fn web_identity_session_name(fields: &[(String, String)]) -> S3Result<String> {
    let Some(name) = field(fields, "RoleSessionName") else {
        return Ok("web-identity".to_string());
    };
    if name.is_empty()
        || name.len() > 64
        || !name.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'+' | b'=' | b',' | b'.' | b'@' | b'-' | b'_')
        })
    {
        return Err(s3s::s3_error!(InvalidArgument, "invalid RoleSessionName"));
    }
    Ok(name)
}

/// WebIdentity 验证错误 → S3 错误（文案不含 token/claims；不区分签名与 claim 失败）。
fn web_identity_error(e: WebIdentityError) -> s3s::S3Error {
    match e {
        WebIdentityError::Expired => {
            s3s::s3_error!(ExpiredToken, "web identity token expired")
        }
        WebIdentityError::DurationOutOfRange => {
            s3s::s3_error!(InvalidArgument, "DurationSeconds out of range")
        }
        WebIdentityError::RoleNotMapped => {
            s3s::s3_error!(AccessDenied, "RoleArn is not mapped for web identity")
        }
        WebIdentityError::InvalidToken => {
            s3s::s3_error!(InvalidToken, "invalid web identity token")
        }
    }
}

fn session_policy(fields: &[(String, String)]) -> S3Result<Option<String>> {
    let Some(raw) = field(fields, "Policy") else {
        return Ok(None);
    };
    if raw.len() > MAX_POLICY_LEN {
        return Err(s3s::s3_error!(InvalidArgument, "session policy too large"));
    }
    let doc: crate::policy::PolicyDoc = serde_json::from_str(&raw)
        .map_err(|_| s3s::s3_error!(InvalidArgument, "malformed session policy"))?;
    doc.validate()
        .map_err(|e| s3s::s3_error!(InvalidArgument, "malformed session policy: {e}"))?;
    Ok(Some(raw))
}

/// form-urlencoded 解析：`k=v&k=v`；`+`→空格、`%XX` 解码；坏转义保留原文（尽力解析）。
pub fn parse_form(bytes: &[u8]) -> Vec<(String, String)> {
    let s = String::from_utf8_lossy(bytes);
    s.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (url_decode(k), url_decode(v)),
            None => (url_decode(pair), String::new()),
        })
        .collect()
}

fn url_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < b.len() => match (hex_val(b[i + 1]), hex_val(b[i + 2])) {
                (Some(h), Some(l)) => {
                    out.push((h << 4) | l);
                    i += 3;
                }
                _ => {
                    out.push(b[i]);
                    i += 1;
                }
            },
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn xml_response(body: String) -> S3Response<Body> {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::header::HeaderValue::from_static("text/xml"),
    );
    S3Response::with_headers(Body::from(body), headers)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Unix 秒 → `2011-06-15T13:45:30Z`（AWS 形态；time crate 的 RFC3339 输出 `+00:00` 后缀，
/// 与 AWS `Z` 形态有差异——手排，零歧义）。
fn iso8601(secs: i64) -> String {
    use time::OffsetDateTime;
    let Ok(odt) = OffsetDateTime::from_unix_timestamp(secs) else {
        return "1970-01-01T00:00:00Z".to_string();
    };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        odt.year(),
        u8::from(odt.month()),
        odt.day(),
        odt.hour(),
        odt.minute(),
        odt.second()
    )
}

/// 16 随机字节 → 32 hex（/dev/urandom；AK 后缀）。
fn hex16() -> std::io::Result<String> {
    hex_rand(16)
}

fn hex32() -> std::io::Result<String> {
    hex_rand(32)
}

/// P3-3 复验修复：RNG 失败**必须上抛**（原实现静默全零兜底 → 可预测凭据）。
fn hex_rand(n: usize) -> std::io::Result<String> {
    use std::io::Read as _;
    let mut f = std::fs::File::open("/dev/urandom")?;
    let mut buf = vec![0u8; n];
    f.read_exact(&mut buf)?;
    let mut out = String::with_capacity(n * 2);
    for b in buf {
        out.push_str(&format!("{b:02x}"));
    }
    Ok(out)
}

/// RequestId 是展示性字段：RNG 失败留空串（凭证路径绝不走此处，见 `issue`）。
fn request_id() -> String {
    hex_rand(8).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L-1：桶耗尽后拒绝，按速率补充后恢复。
    #[test]
    fn web_identity_token_bucket_limits_and_refills() {
        let mut b = TokenBucket::new(2.0);
        assert!(b.allow(0.0, 2.0));
        assert!(b.allow(0.0, 2.0));
        assert!(!b.allow(0.0, 2.0), "耗尽后必须拒绝");
        std::thread::sleep(std::time::Duration::from_millis(5));
        // 5ms × 1000/s 补满 burst=2：连续两次放行，随后再次耗尽。
        assert!(b.allow(1000.0, 2.0), "补充后恢复");
        assert!(b.allow(0.0, 2.0));
        assert!(!b.allow(0.0, 2.0));
    }

    /// L-1b：单 IP 耗尽不影响其他 IP；按速率补充后恢复。
    #[test]
    fn web_identity_ip_buckets_isolate_and_refill() {
        let mut b = IpBuckets::default();
        let a: IpAddr = "192.0.2.10".parse().unwrap();
        let other: IpAddr = "192.0.2.11".parse().unwrap();
        for _ in 0..WI_IP_BURST as usize {
            assert!(b.allow(a));
        }
        assert!(!b.allow(a), "单 IP 耗尽后必须拒绝");
        assert!(b.allow(other), "其他 IP 不受影响");
        std::thread::sleep(Duration::from_millis(520));
        assert!(b.allow(a), "按 2/s 补充后恢复");
    }

    /// L-1b 全路径：同一 IP 超限 → 503 SlowDown；其他 IP / 无对端不受影响。
    #[tokio::test]
    async fn web_identity_per_ip_limit_returns_slow_down() {
        use crate::sts_web_identity::test_support as wi;

        let td = tempfile::tempdir().unwrap();
        let route = StsRoute::new().with_web_identity(Some(wi::web_identity(td.path(), 3600)));
        let body = "Action=AssumeRoleWithWebIdentity\
            &RoleArn=arn%3Aoss%3Aiam%3A%3Aoss%3Arole%2Fci-bot&WebIdentityToken=bad";
        let mk = |peer: Option<IpAddr>| {
            let mut extensions = http::Extensions::new();
            if let Some(ip) = peer {
                extensions.insert(crate::ap::ApPeer(ip));
            }
            S3Request::<Body> {
                input: Body::from(body.to_string()),
                method: Method::POST,
                uri: "/".parse().unwrap(),
                headers: HeaderMap::new(),
                extensions,
                credentials: None,
                region: None,
                service: None,
                trailing_headers: None,
            }
        };
        let ip: IpAddr = "192.0.2.50".parse().unwrap();
        for _ in 0..WI_IP_BURST as usize {
            let e = route.call(mk(Some(ip))).await.unwrap_err();
            assert_ne!(e.code(), &s3s::S3ErrorCode::SlowDown, "配额内不得限流");
        }
        let e = route.call(mk(Some(ip))).await.unwrap_err();
        assert_eq!(e.code(), &s3s::S3ErrorCode::SlowDown, "同一 IP 超限 → 503");
        // 另一个 IP 有自己的配额（全局桶未耗尽）。
        let e = route
            .call(mk(Some("192.0.2.51".parse().unwrap())))
            .await
            .unwrap_err();
        assert_ne!(e.code(), &s3s::S3ErrorCode::SlowDown);
        // 无对端身份（明文 listener）：退化为全局桶，仍可进入。
        let e = route.call(mk(None)).await.unwrap_err();
        assert_ne!(e.code(), &s3s::S3ErrorCode::SlowDown);
    }

    /// 最小控制面 schema（同 auth.rs 测试的口径 + kind 列）。
    #[test]
    fn form_parse_matrix() {
        let f = parse_form(b"Action=AssumeRole&DurationSeconds=900&Policy=%7B%22a%22%3A1%7D");
        assert_eq!(field(&f, "Action").as_deref(), Some("AssumeRole"));
        assert_eq!(field(&f, "DurationSeconds").as_deref(), Some("900"));
        assert_eq!(field(&f, "Policy").as_deref(), Some(r#"{"a":1}"#));
        // '+' → 空格；坏转义保留原文；空值
        let g = parse_form(b"k=a+b&bad=%zz&empty=");
        assert_eq!(field(&g, "k").as_deref(), Some("a b"));
        assert_eq!(field(&g, "bad").as_deref(), Some("%zz"));
        assert_eq!(field(&g, "empty").as_deref(), Some(""));
    }

    #[test]
    fn role_arn_matrix() {
        assert_eq!(
            parse_role_arn("arn:oss:iam::acct:role/erik").as_deref(),
            Some("erik")
        );
        assert_eq!(
            parse_role_arn("arn:aws:iam::123:role/backup-bot").as_deref(),
            Some("backup-bot")
        );
        for bad in [
            "arn:oss:iam::acct:user/erik",
            "arn:oss:s3:::bucket",
            "erik",
            "arn:oss:iam::acct:role/",
            "arn:oss:iam::acct:role/a/b",
        ] {
            assert!(parse_role_arn(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn duration_bounds() {
        let f = |d: &str| parse_form(format!("DurationSeconds={d}").as_bytes());
        assert_eq!(
            duration(&f("900"), "DurationSeconds", 3600, 900, 43200).unwrap(),
            900
        );
        assert_eq!(
            duration(&[], "DurationSeconds", 3600, 900, 43200).unwrap(),
            3600
        );
        assert!(duration(&f("899"), "DurationSeconds", 3600, 900, 43200).is_err());
        assert!(duration(&f("43201"), "DurationSeconds", 3600, 900, 43200).is_err());
        assert!(duration(&f("abc"), "DurationSeconds", 3600, 900, 43200).is_err());
    }

    #[test]
    fn session_policy_validation() {
        let ok = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#;
        let f = parse_form(format!("Policy={}", url_encode(ok)).as_bytes());
        assert!(session_policy(&f).unwrap().is_some());
        // 坏 JSON / 校验不过（Effect 拼错）
        let bad = parse_form(b"Policy=%7Bnot-json");
        assert!(session_policy(&bad).is_err());
        let bad2 = parse_form(
            b"Policy=%7B%22Statement%22%3A%5B%7B%22Effect%22%3A%22Permit%22%2C%22Principal%22%3A%7B%22AWS%22%3A%22*%22%7D%2C%22Action%22%3A%22s3%3AGetObject%22%2C%22Resource%22%3A%22arn%3Aaws%3As3%3A%3A%3Ab%2F*%22%7D%5D%7D",
        );
        assert!(session_policy(&bad2).is_err());
    }

    fn url_encode(s: &str) -> String {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect()
    }

    #[test]
    fn iso8601_shape() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso8601(1_760_000_000), "2025-10-09T08:53:20Z");
    }
}
