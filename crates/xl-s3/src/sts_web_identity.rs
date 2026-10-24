//! STS `AssumeRoleWithWebIdentity` 的 OIDC/JWT 验证与角色映射（默认关闭）。
//!
//! 安全边界（全部 fail-closed）：
//! - 只接受 RS256 / ES256；`none`、HS*、PS*、EdDSA 一律拒（alg 混淆由白名单 +
//!   jsonwebtoken 的「验证算法族必须与公钥族一致」双重拦截）。
//! - `kid` 必填且必须命中本地 JWKS 文件；JWKS 只从本地文件加载（不拉 URL，无 SSRF 面）。
//!   文件 mtime/长度变化后按 ≤1s 间隔懒加载（每次验签前至多 stat 一次），校验通过才
//!   整体替换内存 key；新文件非法则保留旧 key 并告警（不 fail-open、不影响在途验证）。
//! - `iss` / `aud` 精确匹配；`exp` / `nbf` / `iat` 由本模块 + jsonwebtoken 校验（60s 时钟容差）。
//! - 角色映射 = 精确 RoleArn 字符串 + 精确 claim 值（sub/email），无通配符；
//!   角色名再映射到现有 `users.username`（active），复用 STS 会话签发。
//!   `require_email_verified=true` 时 `claim="email"` 的角色还要求 `email_verified=true`。
//! - token 不超过 16 KiB；JWKS 文件不超过 1 MiB；不记录 token / 凭据。

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use base64::Engine as _;
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::Value;

/// WebIdentityToken 上限（JWT 通常 < 8 KiB；挡住解析放大攻击）。
pub const MAX_TOKEN_BYTES: usize = 16 * 1024;
/// JWKS 文件上限。
const MAX_JWKS_BYTES: u64 = 1024 * 1024;
/// DurationSeconds 边界（AWS 口径）。默认 3600；配置上限可收严，不得放宽到 43200 以上。
pub const MIN_DURATION_SECONDS: i64 = 900;
pub const DEFAULT_DURATION_SECONDS: i64 = 3_600;
pub const MAX_DURATION_SECONDS: i64 = 43_200;
/// 时钟容差（秒）——与 jsonwebtoken 默认一致；仅用于 exp/nbf/iat。
const LEEWAY_SECONDS: u64 = 60;
/// JWKS 文件 mtime 检查间隔：每请求至多一次 stat，足以在轮换后 1s 内生效。
const JWKS_RECHECK: Duration = Duration::from_secs(1);

const MAX_ISSUER_LEN: usize = 512;
const MAX_AUDIENCE_LEN: usize = 256;
const MAX_KID_LEN: usize = 128;
const MAX_CLAIM_VALUE_LEN: usize = 512;
const MAX_CLAIM_VALUES: usize = 32;
const MAX_ROLES: usize = 64;

/// `[sts]` 配置节（缺省 = 全关；现有部署零改动）。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StsToml {
    /// `[sts.web_identity]`：OIDC 联合身份。`enabled=false` 时整节不生效。
    #[serde(default)]
    pub web_identity: Option<WebIdentityToml>,
}

/// `[sts.web_identity]`：启用后 `AssumeRoleWithWebIdentity` 才可匿名调用。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebIdentityToml {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub issuer: String,
    #[serde(default)]
    pub audience: String,
    /// 本地 JWKS 文件（启动期读入内存；mtime/长度变化后 ≤1s 内热加载，无需重启）。
    #[serde(default)]
    pub jwks_file: Option<PathBuf>,
    /// 会话时长上限（900..=43200，默认 3600）；请求超过即拒，缺省值收敛到该上限。
    #[serde(default = "default_max_duration_seconds")]
    pub max_duration_seconds: i64,
    /// `claim="email"` 的角色是否要求 token 的 `email_verified=true`（默认 false）。
    #[serde(default)]
    pub require_email_verified: bool,
    #[serde(default)]
    pub roles: Vec<WebIdentityRoleToml>,
}

/// 一条 RoleArn → claim 值映射。claim 只支持 `sub` / `email`，值为精确匹配。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebIdentityRoleToml {
    /// 完整 ARN（`arn:partition:iam::acct:role/<name>`）；<name> 须是现有活跃用户。
    pub role_arn: String,
    pub claim: String,
    #[serde(default)]
    pub values: Vec<String>,
}

fn default_max_duration_seconds() -> i64 {
    DEFAULT_DURATION_SECONDS
}

impl StsToml {
    /// 启动期校验：启用时连 JWKS 文件一并读入解析（坏配置/坏文件 = 启动失败）。
    pub fn validate(&self) -> Result<(), String> {
        self.web_identity().map(drop)
    }

    /// 构建验证器；`None` = 未启用。已启用但配置非法 → Err（调用方须拒绝启动）。
    pub fn web_identity(&self) -> Result<Option<WebIdentity>, String> {
        match &self.web_identity {
            Some(c) if c.enabled => WebIdentity::from_toml(c).map(Some),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleClaim {
    Sub,
    Email,
}

impl RoleClaim {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "sub" => Ok(Self::Sub),
            "email" => Ok(Self::Email),
            other => Err(format!(
                "claim 只支持 \"sub\"/\"email\"，实际 {other:?}（不支持任意 claim/通配符）"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Sub => "sub",
            Self::Email => "email",
        }
    }
}

#[derive(Debug, Clone)]
struct RoleMapping {
    arn: String,
    /// `users.username`（由 role ARN 的 role/<name> 解析）。
    role: String,
    claim: RoleClaim,
    values: Vec<String>,
}

/// 已校验的 OIDC 配置（内存态；除 JWKS 热加载外不可变）。
#[derive(Debug)]
pub struct WebIdentity {
    issuer: String,
    audience: String,
    max_duration: i64,
    require_email_verified: bool,
    roles: Vec<RoleMapping>,
    keys: Mutex<KeyCache>,
}

/// 已载入 JWKS 的文件签名：mtime + 长度；两者不变则不重读。
type FileSignature = (SystemTime, u64);

/// JWKS 内存态 + 热加载游标。写锁只在整体替换 `keys` 时持有（原子换 key），
/// 验证路径持读等价锁最多到 `DecodingKey` 构造完成（无 await）。
#[derive(Debug)]
struct KeyCache {
    path: PathBuf,
    keys: JwkSet,
    /// 最近一次读过的文件签名（成功或失败）；相同 → 不重读。
    seen: Option<FileSignature>,
    /// 上次检查时刻：把 stat 限制为每 `JWKS_RECHECK` 最多一次。
    last_check: Instant,
    /// 最近一次失败文案：同一错误只告警一次（坏文件期间不刷日志）。
    last_error: Option<String>,
}

impl KeyCache {
    fn new(path: PathBuf, keys: JwkSet, sig: FileSignature) -> Self {
        Self {
            path,
            keys,
            seen: Some(sig),
            last_check: Instant::now(),
            last_error: None,
        }
    }

    fn maybe_reload(&mut self) {
        if self.last_check.elapsed() < JWKS_RECHECK {
            return;
        }
        self.reload();
    }

    fn reload(&mut self) {
        self.last_check = Instant::now();
        let sig = match file_signature(&self.path) {
            Ok(sig) => sig,
            // 文件暂时不可读/被删：保留旧 key（轮换期间的瞬时状态不应中断服务）。
            Err(e) => return self.fail(e),
        };
        if self.seen == Some(sig) {
            return;
        }
        match load_jwks(&self.path) {
            Ok((keys, sig)) => {
                let count = keys.keys.len();
                self.keys = keys;
                self.seen = Some(sig);
                self.last_error = None;
                tracing::info!(path = %self.path.display(), keys = count, "JWKS 已热加载");
            }
            Err(e) => {
                // 记住坏版本签名：文件再变之前不重复读盘；旧 key 继续生效。
                self.seen = Some(sig);
                self.fail(e);
            }
        }
    }

    fn fail(&mut self, e: String) {
        if self.last_error.as_deref() != Some(e.as_str()) {
            tracing::warn!(path = %self.path.display(), error = %e,
                "JWKS 热加载失败，保留旧 key");
            self.last_error = Some(e);
        }
    }
}

/// 验证通过后的会话参数（不含 token / 原始 claims）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedWebIdentity {
    pub subject: String,
    /// 映射到的本地角色用户名（`users.username`）。
    pub role: String,
    pub role_arn: String,
    pub duration_seconds: i64,
}

/// 验证失败类别；错误文案绝不含 token/claim 原文。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebIdentityError {
    /// 结构/签名/算法/kid/iss/aud/claim 任一不符。
    InvalidToken,
    Expired,
    DurationOutOfRange,
    RoleNotMapped,
}

impl std::fmt::Display for WebIdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidToken => "invalid web identity token",
            Self::Expired => "web identity token expired",
            Self::DurationOutOfRange => "DurationSeconds out of range",
            Self::RoleNotMapped => "RoleArn is not mapped for web identity",
        })
    }
}

impl WebIdentity {
    /// 配置的 issuer（仅用于响应 Provider 回显；token 的 iss 已精确匹配）。
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// 配置的 audience（仅用于响应回显；token 的 aud 已精确匹配）。
    pub fn audience(&self) -> &str {
        &self.audience
    }

    fn from_toml(c: &WebIdentityToml) -> Result<Self, String> {
        validate_issuer(&c.issuer)?;
        validate_audience(&c.audience)?;
        if !(MIN_DURATION_SECONDS..=MAX_DURATION_SECONDS).contains(&c.max_duration_seconds) {
            return Err(format!(
                "max_duration_seconds 必须在 {MIN_DURATION_SECONDS}..={MAX_DURATION_SECONDS}，实际 {}",
                c.max_duration_seconds
            ));
        }
        let Some(path) = c.jwks_file.as_ref() else {
            return Err("缺少 jwks_file（仅支持本地文件，不支持 URL）".into());
        };
        let (keys, sig) = load_jwks(path)?;
        if c.roles.is_empty() {
            return Err("roles 不能为空（无映射则无人可换取凭据）".into());
        }
        if c.roles.len() > MAX_ROLES {
            return Err(format!("roles 最多 {MAX_ROLES} 条"));
        }
        let mut seen = HashSet::new();
        let mut roles = Vec::with_capacity(c.roles.len());
        for r in &c.roles {
            if r.role_arn.bytes().any(|b| b.is_ascii_whitespace()) {
                return Err(format!("role_arn 含空白：{:?}", r.role_arn));
            }
            let role = crate::sts::parse_role_arn(&r.role_arn)
                .ok_or_else(|| format!("role_arn 非法：{:?}", r.role_arn))?;
            if !seen.insert(r.role_arn.clone()) {
                return Err(format!("role_arn 重复：{}", r.role_arn));
            }
            let claim = RoleClaim::parse(&r.claim)?;
            if r.values.is_empty() {
                return Err(format!("{} 的 values 不能为空", r.role_arn));
            }
            if r.values.len() > MAX_CLAIM_VALUES {
                return Err(format!(
                    "{} 的 values 最多 {MAX_CLAIM_VALUES} 条",
                    r.role_arn
                ));
            }
            let mut values = Vec::with_capacity(r.values.len());
            for v in &r.values {
                if v.is_empty()
                    || v.len() > MAX_CLAIM_VALUE_LEN
                    || v.bytes().any(|b| b.is_ascii_control())
                    || v.contains('*')
                {
                    return Err(format!(
                        "{} 的 claim 值非法（非空、≤{MAX_CLAIM_VALUE_LEN}、无控制符、无通配符）",
                        r.role_arn
                    ));
                }
                if !values.contains(v) {
                    values.push(v.clone());
                }
            }
            roles.push(RoleMapping {
                arn: r.role_arn.clone(),
                role,
                claim,
                values,
            });
        }
        Ok(Self {
            issuer: c.issuer.clone(),
            audience: c.audience.clone(),
            max_duration: c.max_duration_seconds,
            require_email_verified: c.require_email_verified,
            roles,
            keys: Mutex::new(KeyCache::new(path.clone(), keys, sig)),
        })
    }

    /// 测试钩子：跳过 1s 间隔（等价于已到期），走真实的 mtime 检测 + 重载路径。
    #[cfg(test)]
    pub(crate) fn reload_for_test(&self) {
        let mut cache = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        cache.last_check = Instant::now() - JWKS_RECHECK;
        cache.maybe_reload();
    }

    /// 验证 token 并按 RoleArn 映射；任何失败都 fail-closed。
    pub fn verify(
        &self,
        token: &str,
        role_arn: &str,
        requested_duration: Option<i64>,
    ) -> Result<VerifiedWebIdentity, WebIdentityError> {
        if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
            return Err(WebIdentityError::InvalidToken);
        }
        let mapping = self
            .roles
            .iter()
            .find(|r| r.arn == role_arn)
            .ok_or(WebIdentityError::RoleNotMapped)?;
        let duration = match requested_duration {
            Some(d) if (MIN_DURATION_SECONDS..=self.max_duration).contains(&d) => d,
            Some(_) => return Err(WebIdentityError::DurationOutOfRange),
            None => DEFAULT_DURATION_SECONDS.min(self.max_duration),
        };

        let header = decode_header(token).map_err(|_| WebIdentityError::InvalidToken)?;
        if !matches!(header.alg, Algorithm::RS256 | Algorithm::ES256) {
            return Err(WebIdentityError::InvalidToken);
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|k| !k.is_empty() && k.len() <= MAX_KID_LEN)
            .ok_or(WebIdentityError::InvalidToken)?;
        // 锁只覆盖查找 + DecodingKey 构造：热加载的写入点在两次验签之间整体换 key。
        let key = {
            let mut cache = self.keys.lock().unwrap_or_else(|e| e.into_inner());
            cache.maybe_reload();
            let jwk = cache.keys.find(kid).ok_or(WebIdentityError::InvalidToken)?;
            // JWKS 若声明 alg，必须与 token header 一致（防同一 kid 跨算法复用）。
            if let Some(declared) = jwk.common.key_algorithm {
                let declared =
                    Algorithm::try_from(declared).map_err(|_| WebIdentityError::InvalidToken)?;
                if declared != header.alg {
                    return Err(WebIdentityError::InvalidToken);
                }
            }
            DecodingKey::from_jwk(jwk).map_err(|_| WebIdentityError::InvalidToken)?
        };
        // 单算法 Validation：jsonwebtoken 要求验证算法族与公钥族一致（RS256↔RSA、
        // ES256↔EC），因此列表里只能放 header 声明的这一个算法。
        let mut validation = Validation::new(header.alg);
        validation.leeway = LEEWAY_SECONDS;
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.validate_nbf = true;
        let claims = decode::<Value>(token, &key, &validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => WebIdentityError::Expired,
                _ => WebIdentityError::InvalidToken,
            })?
            .claims;

        let subject = claims
            .get("sub")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= MAX_CLAIM_VALUE_LEN)
            .ok_or(WebIdentityError::InvalidToken)?;
        let exp = claims
            .get("exp")
            .and_then(Value::as_i64)
            .ok_or(WebIdentityError::InvalidToken)?;
        let iat = claims
            .get("iat")
            .and_then(Value::as_i64)
            .ok_or(WebIdentityError::InvalidToken)?;
        let now = now_secs();
        // jsonwebtoken 不校验 iat：拒绝未来签发（含容差）与 iat>exp。
        if iat > now + LEEWAY_SECONDS as i64 || iat > exp {
            return Err(WebIdentityError::InvalidToken);
        }
        let claim_value = claims
            .get(mapping.claim.name())
            .and_then(Value::as_str)
            .ok_or(WebIdentityError::InvalidToken)?;
        if !mapping.values.iter().any(|v| v == claim_value) {
            return Err(WebIdentityError::InvalidToken);
        }
        // 按 email 授权的角色可选强制 verified；未验证/缺失/非 true 一律拒。
        if self.require_email_verified
            && mapping.claim == RoleClaim::Email
            && !email_verified(&claims)
        {
            return Err(WebIdentityError::InvalidToken);
        }
        Ok(VerifiedWebIdentity {
            subject: subject.to_string(),
            role: mapping.role.clone(),
            role_arn: mapping.arn.clone(),
            duration_seconds: duration,
        })
    }
}

fn validate_issuer(raw: &str) -> Result<(), String> {
    if raw.is_empty() || raw.len() > MAX_ISSUER_LEN || raw.bytes().any(|b| b.is_ascii_control()) {
        return Err("issuer 非法（非空、≤512、无控制符）".into());
    }
    let uri: http::Uri = raw
        .parse()
        .map_err(|_| "issuer 必须是 https URL".to_string())?;
    if uri.scheme_str() != Some("https")
        || uri.authority().is_none()
        || uri.authority().is_some_and(|a| a.as_str().contains('@'))
        || uri.query().is_some()
        || raw.contains('#')
    {
        return Err(
            "issuer 必须是无 query/fragment/userinfo 的 https URL（精确匹配 iss claim）".into(),
        );
    }
    Ok(())
}

fn validate_audience(raw: &str) -> Result<(), String> {
    if raw.is_empty() || raw.len() > MAX_AUDIENCE_LEN || raw.bytes().any(|b| b.is_ascii_control()) {
        return Err("audience 非法（非空、≤256、无控制符）".into());
    }
    Ok(())
}

/// `email_verified` 只认布尔 `true` 或字符串 `"true"`（部分 IdP 用字符串形态）；
/// 缺失/`false`/其他类型一律视为未验证。
fn email_verified(claims: &Value) -> bool {
    match claims.get("email_verified") {
        Some(Value::Bool(v)) => *v,
        Some(Value::String(s)) => s == "true",
        _ => false,
    }
}

fn file_signature(path: &std::path::Path) -> Result<FileSignature, String> {
    let meta = std::fs::metadata(path)
        .map_err(|e| format!("读取 jwks_file {} 失败: {e}", path.display()))?;
    let modified = meta
        .modified()
        .map_err(|e| format!("读取 jwks_file {} 的 mtime 失败: {e}", path.display()))?;
    Ok((modified, meta.len()))
}

/// 从本地文件读 JWKS：大小上限 → JSON 解析 → 每把 key 白名单校验（kid/算法/曲线/长度）。
fn load_jwks(path: &std::path::Path) -> Result<(JwkSet, FileSignature), String> {
    let sig = file_signature(path)?;
    if sig.1 > MAX_JWKS_BYTES {
        return Err(format!("jwks_file 超过 {MAX_JWKS_BYTES} 字节上限"));
    }
    let bytes =
        std::fs::read(path).map_err(|e| format!("读取 jwks_file {} 失败: {e}", path.display()))?;
    let keys: JwkSet =
        serde_json::from_slice(&bytes).map_err(|e| format!("jwks_file 不是合法 JWKS JSON: {e}"))?;
    if keys.keys.is_empty() {
        return Err("jwks_file 不含任何 key".into());
    }
    let mut seen = HashSet::new();
    for jwk in &keys.keys {
        validate_jwk(jwk)?;
        let kid = jwk.common.key_id.as_deref().unwrap_or_default();
        if !seen.insert(kid.to_string()) {
            return Err(format!("jwks_file 中 kid 重复：{kid:?}"));
        }
    }
    Ok((keys, sig))
}

fn validate_jwk(jwk: &Jwk) -> Result<(), String> {
    let kid = jwk
        .common
        .key_id
        .as_deref()
        .filter(|k| !k.is_empty() && k.len() <= MAX_KID_LEN)
        .ok_or_else(|| "JWKS key 缺少合法 kid（kid 必填，用于查找）".to_string())?;
    if let Some(u) = &jwk.common.public_key_use
        && *u != jsonwebtoken::jwk::PublicKeyUse::Signature
    {
        return Err(format!("JWKS key {kid:?} 的 use 不是 sig"));
    }
    match &jwk.algorithm {
        AlgorithmParameters::RSA(p) => {
            if let Some(alg) = jwk.common.key_algorithm
                && alg != KeyAlgorithm::RS256
            {
                return Err(format!("JWKS key {kid:?} 只支持 RS256（实际声明 {alg:?}）"));
            }
            let n = b64url(&p.n).ok_or_else(|| format!("JWKS key {kid:?} 的 n 非法"))?;
            let e = b64url(&p.e).ok_or_else(|| format!("JWKS key {kid:?} 的 e 非法"))?;
            if modulus_bits(&n) < 2048 || e.is_empty() {
                return Err(format!(
                    "JWKS key {kid:?} 的 RSA 公钥不足 2048 位或指数为空"
                ));
            }
        }
        AlgorithmParameters::EllipticCurve(p) => {
            if let Some(alg) = jwk.common.key_algorithm
                && alg != KeyAlgorithm::ES256
            {
                return Err(format!("JWKS key {kid:?} 只支持 ES256（实际声明 {alg:?}）"));
            }
            if p.curve != EllipticCurve::P256 {
                return Err(format!("JWKS key {kid:?} 只支持 P-256 曲线"));
            }
            match (b64url(&p.x), b64url(&p.y)) {
                (Some(x), Some(y)) if x.len() == 32 && y.len() == 32 => {}
                _ => return Err(format!("JWKS key {kid:?} 的 P-256 坐标非法")),
            }
        }
        _ => return Err(format!("JWKS key {kid:?} 类型不受支持（只允许 RSA/EC）")),
    }
    Ok(())
}

fn b64url(s: &str) -> Option<Vec<u8>> {
    use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD
        .decode(s)
        .or_else(|_| URL_SAFE.decode(s))
        .ok()
}

fn modulus_bits(bytes: &[u8]) -> usize {
    let Some(first) = bytes.iter().position(|b| *b != 0) else {
        return 0;
    };
    (bytes.len() - first - 1) * 8 + (8 - bytes[first].leading_zeros() as usize)
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) mod test_support {
    //! 测试夹具：内嵌**测试专用**私钥 + 本地 JWKS 文件；不触网。
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::{Value, json};

    pub const ISSUER: &str = "https://issuer.example";
    pub const AUDIENCE: &str = "oss-test";
    pub const KID_RSA: &str = "test-rsa";
    pub const KID_EC: &str = "test-ec";

    /// 测试专用 RSA-2048 私钥（公开于仓库，仅限单测）。
    const RSA_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQCUWWLwzW+HcRyz
zQRqrSqR5lqfrt32+S8Yrx2LdMyZz3dq9JkNTDce6BNmv4ITKgy6LPqOjAM/4CQH
1xuw1+P8sMSPpYIkIIxjfvuYSX8q2rZHBvhOqNcB7Kibcwguj2wieweFfU554mOs
SyNaeHiFBGdGIGkoJ4iLgIoZmWBqY3MvJu9izOcCE2BGXRO4IqDAFb78oiyEvmte
Ua6uuQpBb+ip6THoM6tio0tJHWLtXQhT9XbeUOXsfCnq7SGcxhxn9SKfXUCAfzWg
dNQs+Mfw1BPWCyO20AFtzd/01gcxbrpWM5S41vNAXVU2p5ZgeMyqPSd//KwXTB4a
dizq+Gh9AgMBAAECgf8BLf3ttWtbVhQSEM9Kd5YMydFKGFniXOIMInzr8UHT0rcX
PhGLyYY4+LHBFkTEz/VcPH9T2bbjwf/1EADWFOFo80gKkz01sOf/K7ELV0Eieq0K
rLE6gS99prZnYXmmCbTJSEN0wyXBLcuvZVdHMy4LTnSDx/UOHLAUtQxkFW+TaP0V
oqDkVnrxZe75wZm3JdzFrBiR0C832a4c3S9JT/u614v801RKdw0IJtBlHOsGVMFE
S+V1ggyrbM3hY5Gw7nvTvNbDSXporglKQCap8eyLCNv/M181Dc42OS/2s+wHmkxb
pzFRX7XYXyZqd1dDhW5cfUlMmkhXM125afVOVAECgYEAxpawdwV2NWww8IfiXMH5
5RvMRrxWURhscFsNYvlt9v2rADoE4XUq8Sz3CkwpdxQhTBty/jk5tgEHZub6Wr5Y
4STcHDQy/hYGcPbOU8zO4yjLtMFYePbHNu90dZpNO7nQF03cYYQjIAE3ANp66+l+
nOsr9Dd/j0oeX4CSfWog5H0CgYEAvzyImN9UzSdrPD9RtghU16jpxaEMeSkSa3J9
NE5akDFRRbpVK1t/bkAicQYh44QCG7RKB6yhWHVmFd4vBKhyMTA+iJo0X6vv8N9m
DWGPIKlPBcnHqNtTiaMiuiB8OXSC3d0NdbwOmEJPWJ5mM7m5QwNzaM+2MWf9SknD
wMQg1AECgYBvEjMqrrxZzuRkc6vws3GAahbL7WTdwKpOAS8wj0+o2GI/IzQ9oZne
AIc/lC+Q8YpaFfQwKnOTz2mDbWbTQClX65TMlYl7wXfi0Le/+Qox1UxyEb3ZEjWE
DVP2vtG5uQihMCQ1yoEW0ncgHBKqRhNR0trnoe7PhuYMBk5opOY2xQKBgQCiNs/L
lM1TBFfjD/bYyPJmyKIbgIObFqrIN6Jaxwfs/dhppCfwkjlipMZSRxhh7Cz5oRGW
tulZpqPEcuZBNj0+2iSBLQS2DpuGH4lqWlHVK6ibSVCyO/D9dELVzQ5ykhEb7xEm
yDE9GCpECoC46ZZrcLbpth9eg04IQM/qaLNMAQKBgBZSfQ1SQ55nEBcCPrRzI475
oVA4pLC/jiw9jmVqhBMDBjO0LVVvN1LOvJ1Zlc5IRTuA4wDqkYTyORYgeyx5QWuW
ao+FCwL5gPuJ+VEwWt9WXx5R/X9jSl73i3WJ9HrvAxuMuw1zVKxQuXj1VdHkRQIS
GiFYKJTHieXtPBiFjF+q
-----END PRIVATE KEY-----
";

    /// 测试专用 P-256 私钥（公开于仓库，仅限单测）。
    const EC_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgdXaiR6jOrgh3rEbe
pHb/yXv73AktpoMjvU8gATmCYzahRANCAARaTpMv8+UoDvBJiqq3kmU/IosJI7Pq
iwDc6O2gSOdmS7sEhNi2sVl/D1MTfgAaGQZaccT8Fuvo3rBQqU5dOORV
-----END PRIVATE KEY-----
";

    fn jwk(kid: &str, alg: Algorithm, pem: &str) -> Jwk {
        let key = match alg {
            Algorithm::RS256 => EncodingKey::from_rsa_pem(pem.as_bytes()),
            Algorithm::ES256 => EncodingKey::from_ec_pem(pem.as_bytes()),
            _ => unreachable!(),
        }
        .unwrap();
        let mut jwk = Jwk::from_encoding_key(&key, alg).unwrap();
        jwk.common.key_id = Some(kid.to_string());
        jwk
    }

    pub fn jwks() -> JwkSet {
        JwkSet {
            keys: vec![
                jwk(KID_RSA, Algorithm::RS256, RSA_PEM),
                jwk(KID_EC, Algorithm::ES256, EC_PEM),
            ],
        }
    }

    /// 写 JWKS 到临时目录并构建已启用配置的验证器。
    pub fn web_identity(dir: &std::path::Path, max_duration: i64) -> WebIdentity {
        let path = dir.join("jwks.json");
        std::fs::write(&path, serde_json::to_vec(&jwks()).unwrap()).unwrap();
        WebIdentity::from_toml(&toml(&path, max_duration)).unwrap()
    }

    pub fn toml(jwks_file: &std::path::Path, max_duration: i64) -> WebIdentityToml {
        WebIdentityToml {
            enabled: true,
            issuer: ISSUER.into(),
            audience: AUDIENCE.into(),
            jwks_file: Some(jwks_file.to_path_buf()),
            max_duration_seconds: max_duration,
            require_email_verified: false,
            roles: vec![WebIdentityRoleToml {
                role_arn: "arn:oss:iam::oss:role/ci-bot".into(),
                claim: "sub".into(),
                values: vec!["repo:acme/infra:ref:refs/heads/main".into()],
            }],
        }
    }

    pub fn claims(sub: &str, now: i64) -> Value {
        json!({
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": sub,
            "iat": now,
            "exp": now + 600,
        })
    }

    pub fn sign(claims: &Value, kid: &str, alg: Algorithm) -> String {
        let key = match alg {
            Algorithm::RS256 => EncodingKey::from_rsa_pem(RSA_PEM.as_bytes()).unwrap(),
            Algorithm::ES256 => EncodingKey::from_ec_pem(EC_PEM.as_bytes()).unwrap(),
            // 仅用于 alg 混淆负例：HS256 token 永远不应通过。
            Algorithm::HS256 => EncodingKey::from_secret(b"test-hmac-secret"),
            _ => unreachable!(),
        };
        let mut header = Header::new(alg);
        header.kid = Some(kid.to_string());
        encode(&header, claims, &key).unwrap()
    }

    /// 无签名/任意算法 header 的手工 JWT（用于 alg=none 混淆测试）。
    pub fn unsigned_token(header_json: &str, claims: &Value) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let h = URL_SAFE_NO_PAD.encode(header_json.as_bytes());
        let c = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        format!("{h}.{c}.")
    }

    pub fn now() -> i64 {
        now_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use serde_json::json;

    const ROLE_ARN: &str = "arn:oss:iam::oss:role/ci-bot";

    fn verifier(dir: &std::path::Path, max_duration: i64) -> WebIdentity {
        web_identity(dir, max_duration)
    }

    fn verify_ok(wi: &WebIdentity, token: &str) -> VerifiedWebIdentity {
        wi.verify(token, ROLE_ARN, None).unwrap()
    }

    #[test]
    fn valid_rs256_and_es256_tokens_map_to_role() {
        let td = tempfile::tempdir().unwrap();
        let wi = verifier(td.path(), 3600);
        let now = now();
        let sub = "repo:acme/infra:ref:refs/heads/main";
        for (kid, alg) in [(KID_RSA, Algorithm::RS256), (KID_EC, Algorithm::ES256)] {
            let token = sign(&claims(sub, now), kid, alg);
            let v = verify_ok(&wi, &token);
            assert_eq!(v.subject, sub);
            assert_eq!(v.role, "ci-bot");
            assert_eq!(v.role_arn, ROLE_ARN);
            assert_eq!(v.duration_seconds, 3600);
        }
    }

    #[test]
    fn wrong_audience_or_issuer_rejected() {
        let td = tempfile::tempdir().unwrap();
        let wi = verifier(td.path(), 3600);
        let now = now();
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c["aud"] = json!("other-audience");
        assert_eq!(
            wi.verify(&sign(&c, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c["iss"] = json!("https://evil.example");
        assert_eq!(
            wi.verify(&sign(&c, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        // aud 缺失（required claim）同样拒
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c.as_object_mut().unwrap().remove("aud");
        assert_eq!(
            wi.verify(&sign(&c, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
    }

    #[test]
    fn expired_not_yet_valid_and_bad_iat_rejected() {
        let td = tempfile::tempdir().unwrap();
        let wi = verifier(td.path(), 3600);
        let now = now();
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c["iat"] = json!(now - 7200);
        c["exp"] = json!(now - 3600);
        assert_eq!(
            wi.verify(&sign(&c, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::Expired)
        );
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c["nbf"] = json!(now + 3600);
        assert_eq!(
            wi.verify(&sign(&c, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c["iat"] = json!(now + 3600);
        assert_eq!(
            wi.verify(&sign(&c, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c.as_object_mut().unwrap().remove("iat");
        assert_eq!(
            wi.verify(&sign(&c, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
    }

    #[test]
    fn unknown_kid_and_algorithm_confusion_rejected() {
        let td = tempfile::tempdir().unwrap();
        let wi = verifier(td.path(), 3600);
        let now = now();
        let c = claims("repo:acme/infra:ref:refs/heads/main", now);
        // 未知 kid
        let token = sign(&c, "unknown-kid", Algorithm::RS256);
        assert_eq!(
            wi.verify(&token, ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        // alg=none（无签名）
        let none = unsigned_token(r#"{"alg":"none","kid":"test-rsa"}"#, &c);
        assert_eq!(
            wi.verify(&none, ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        // HS256：用同一 header kid，但算法不在白名单
        let hs = sign(&c, KID_RSA, Algorithm::HS256);
        assert_eq!(
            wi.verify(&hs, ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        // RS256 token 的 kid 指向 EC key → 公钥族不匹配
        let token = sign(&c, KID_EC, Algorithm::RS256);
        assert_eq!(
            wi.verify(&token, ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
    }

    #[test]
    fn role_not_mapped_and_claim_mismatch_rejected() {
        let td = tempfile::tempdir().unwrap();
        let wi = verifier(td.path(), 3600);
        let now = now();
        let c = claims("repo:acme/infra:ref:refs/heads/main", now);
        let token = sign(&c, KID_RSA, Algorithm::RS256);
        assert_eq!(
            wi.verify(&token, "arn:oss:iam::oss:role/other", None),
            Err(WebIdentityError::RoleNotMapped)
        );
        let other = claims("repo:attacker/fork:ref:refs/heads/main", now);
        assert_eq!(
            wi.verify(&sign(&other, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        // claim 缺失
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c.as_object_mut().unwrap().remove("sub");
        assert_eq!(
            wi.verify(&sign(&c, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
    }

    #[test]
    fn duration_is_bounded_and_default_is_clamped() {
        let td = tempfile::tempdir().unwrap();
        let wi = verifier(td.path(), 900);
        let now = now();
        let token = sign(
            &claims("repo:acme/infra:ref:refs/heads/main", now),
            KID_RSA,
            Algorithm::RS256,
        );
        // 上限 900 < 默认 3600 → 缺省收敛到 900
        assert_eq!(
            wi.verify(&token, ROLE_ARN, None).unwrap().duration_seconds,
            900
        );
        assert_eq!(
            wi.verify(&token, ROLE_ARN, Some(900))
                .unwrap()
                .duration_seconds,
            900
        );
        for bad in [899, 901] {
            assert_eq!(
                wi.verify(&token, ROLE_ARN, Some(bad)),
                Err(WebIdentityError::DurationOutOfRange),
                "{bad}"
            );
        }
        // 默认上限 43200（AWS 口径）
        let td2 = tempfile::tempdir().unwrap();
        let wi2 = verifier(td2.path(), MAX_DURATION_SECONDS);
        assert_eq!(
            wi2.verify(&token, ROLE_ARN, Some(MAX_DURATION_SECONDS))
                .unwrap()
                .duration_seconds,
            43_200
        );
        assert_eq!(
            wi2.verify(&token, ROLE_ARN, Some(43_201)),
            Err(WebIdentityError::DurationOutOfRange)
        );
    }

    #[test]
    fn oversized_token_rejected() {
        let td = tempfile::tempdir().unwrap();
        let wi = verifier(td.path(), 3600);
        let big = "a".repeat(MAX_TOKEN_BYTES + 1);
        assert_eq!(
            wi.verify(&big, ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
    }

    #[test]
    fn config_is_disabled_by_default_and_validates_strictly() {
        // 缺省关闭
        assert!(StsToml::default().web_identity().unwrap().is_none());
        let td = tempfile::tempdir().unwrap();
        let jwks_path = td.path().join("jwks.json");
        std::fs::write(&jwks_path, serde_json::to_vec(&jwks()).unwrap()).unwrap();

        // enabled=false 时不做任何检查（即使字段为空）
        let disabled = StsToml {
            web_identity: Some(WebIdentityToml {
                enabled: false,
                issuer: String::new(),
                audience: String::new(),
                jwks_file: None,
                max_duration_seconds: 0,
                require_email_verified: false,
                roles: vec![],
            }),
        };
        assert!(disabled.web_identity().unwrap().is_none());

        let good = toml(&jwks_path, 3600);
        assert!(WebIdentity::from_toml(&good).is_ok());
        // 缺 jwks_file / 空 roles / 坏 issuer / 坏 audience / 越界时长
        let mut bad = good.clone();
        bad.jwks_file = None;
        assert!(WebIdentity::from_toml(&bad).is_err());
        let mut bad = good.clone();
        bad.roles = vec![];
        assert!(WebIdentity::from_toml(&bad).is_err());
        for issuer in [
            "",
            "http://issuer.example",
            "https://issuer.example?x=1",
            "https://user@issuer.example",
            "https://issuer.example#frag",
        ] {
            let mut bad = good.clone();
            bad.issuer = issuer.into();
            assert!(WebIdentity::from_toml(&bad).is_err(), "{issuer}");
        }
        for d in [0, 899, 43_201] {
            let mut bad = good.clone();
            bad.max_duration_seconds = d;
            assert!(WebIdentity::from_toml(&bad).is_err(), "{d}");
        }
        let mut bad = good.clone();
        bad.audience = String::new();
        assert!(WebIdentity::from_toml(&bad).is_err());
        // claim 白名单 / 空 values / 通配符 / 重复 role / 坏 ARN
        let mut bad = good.clone();
        bad.roles[0].claim = "iss".into();
        assert!(WebIdentity::from_toml(&bad).is_err());
        let mut bad = good.clone();
        bad.roles[0].values = vec![];
        assert!(WebIdentity::from_toml(&bad).is_err());
        let mut bad = good.clone();
        bad.roles[0].values = vec!["*".into()];
        assert!(WebIdentity::from_toml(&bad).is_err());
        let mut bad = good.clone();
        let first = bad.roles[0].clone();
        bad.roles.push(first);
        assert!(WebIdentity::from_toml(&bad).is_err());
        let mut bad = good.clone();
        bad.roles[0].role_arn = "not-an-arn".into();
        assert!(WebIdentity::from_toml(&bad).is_err());
    }

    #[test]
    fn jwks_rejects_hmac_keys_duplicate_kids_and_bad_kid() {
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("jwks.json");
        let base = toml(&path, 3600);
        let write =
            |jwks: &JwkSet| std::fs::write(&path, serde_json::to_vec(jwks).unwrap()).unwrap();

        // oct (HS256) key → 拒
        write(&JwkSet {
            keys: vec![
                serde_json::from_value(json!({
                    "kty": "oct", "kid": "secret", "alg": "HS256", "k": "c2VjcmV0"
                }))
                .unwrap(),
            ],
        });
        assert!(WebIdentity::from_toml(&base).is_err());

        // 重复 kid → 拒
        let dup = JwkSet {
            keys: vec![jwks().keys[0].clone(), jwks().keys[0].clone()],
        };
        write(&dup);
        assert!(WebIdentity::from_toml(&base).is_err());

        // 缺 kid → 拒
        let mut no_kid = jwks().keys[0].clone();
        no_kid.common.key_id = None;
        write(&JwkSet { keys: vec![no_kid] });
        assert!(WebIdentity::from_toml(&base).is_err());
    }

    #[test]
    fn error_messages_never_contain_token() {
        let td = tempfile::tempdir().unwrap();
        let wi = verifier(td.path(), 3600);
        let now = now();
        let mut c = claims("repo:acme/infra:ref:refs/heads/main", now);
        c["aud"] = json!("wrong");
        let token = sign(&c, KID_RSA, Algorithm::RS256);
        let err = wi.verify(&token, ROLE_ARN, None).unwrap_err();
        assert!(!err.to_string().contains(&token));
        assert!(!format!("{err:?}").contains(&token));
    }

    /// email 映射 + `require_email_verified`：默认不校验；开启后缺失/false 拒，
    /// 布尔 true 与字符串 "true" 放行；sub 映射不受该开关影响。
    #[test]
    fn email_verified_required_only_for_email_roles() {
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("jwks.json");
        std::fs::write(&path, serde_json::to_vec(&jwks()).unwrap()).unwrap();
        let now = now();
        let email = "dev@example.com";
        let mk = |require: bool, claim: &str| {
            let mut c = toml(&path, 3600);
            c.require_email_verified = require;
            c.roles[0].claim = claim.into();
            c.roles[0].values = if claim == "email" {
                vec![email.into()]
            } else {
                vec!["repo:acme/infra:ref:refs/heads/main".into()]
            };
            WebIdentity::from_toml(&c).unwrap()
        };

        let mut c = claims(email, now);
        c["email"] = json!(email);
        c["email_verified"] = json!(true);

        // 默认（开关关闭）：email 映射不要求 email_verified。
        let wi = mk(false, "email");
        let token = sign(&c, KID_RSA, Algorithm::RS256);
        assert!(wi.verify(&token, ROLE_ARN, None).is_ok());
        let mut no_flag = c.clone();
        no_flag.as_object_mut().unwrap().remove("email_verified");
        assert!(
            wi.verify(&sign(&no_flag, KID_RSA, Algorithm::RS256), ROLE_ARN, None)
                .is_ok()
        );

        // 开启：true 放行（布尔/字符串两种 IdP 形态）。
        let wi = mk(true, "email");
        assert!(wi.verify(&token, ROLE_ARN, None).is_ok());
        let mut s = c.clone();
        s["email_verified"] = json!("true");
        assert!(
            wi.verify(&sign(&s, KID_RSA, Algorithm::RS256), ROLE_ARN, None)
                .is_ok()
        );
        // 缺失 / false / 字符串 "false" 一律拒。
        assert_eq!(
            wi.verify(&sign(&no_flag, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        let mut f = c.clone();
        f["email_verified"] = json!(false);
        assert_eq!(
            wi.verify(&sign(&f, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );
        let mut fs = c.clone();
        fs["email_verified"] = json!("false");
        assert_eq!(
            wi.verify(&sign(&fs, KID_RSA, Algorithm::RS256), ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );

        // sub 映射的角色：开关开启也不看 email_verified（该 token 无此 claim）。
        let wi = mk(true, "sub");
        let sub_token = sign(
            &claims("repo:acme/infra:ref:refs/heads/main", now),
            KID_RSA,
            Algorithm::RS256,
        );
        assert!(wi.verify(&sub_token, ROLE_ARN, None).is_ok());
    }

    /// 热加载：mtime 变化后换 key（旧 key 失效）；坏文件/空 keys 保留旧 key；
    /// 文件恢复合法后再次生效。mtime 显式设置，避免文件系统时间粒度导致抖动。
    #[test]
    fn jwks_hot_reload_swaps_keys_and_keeps_old_on_invalid() {
        use std::time::{Duration, UNIX_EPOCH};
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("jwks.json");
        let write = |keys: Vec<Jwk>, secs: u64| {
            std::fs::write(&path, serde_json::to_vec(&JwkSet { keys }).unwrap()).unwrap();
            std::fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_times(
                    std::fs::FileTimes::new()
                        .set_modified(UNIX_EPOCH + Duration::from_secs(1_700_000_000 + secs)),
                )
                .unwrap();
        };

        let all = jwks();
        write(vec![all.keys[0].clone()], 0); // 仅 RSA
        let wi = WebIdentity::from_toml(&toml(&path, 3600)).unwrap();
        let now = now();
        let c = claims("repo:acme/infra:ref:refs/heads/main", now);
        let rsa = sign(&c, KID_RSA, Algorithm::RS256);
        let ec = sign(&c, KID_EC, Algorithm::ES256);
        assert!(wi.verify(&rsa, ROLE_ARN, None).is_ok());
        assert_eq!(
            wi.verify(&ec, ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );

        // 轮换到仅 EC：新 key 生效，旧 key 立即失效。
        write(vec![all.keys[1].clone()], 1);
        wi.reload_for_test();
        assert!(wi.verify(&ec, ROLE_ARN, None).is_ok());
        assert_eq!(
            wi.verify(&rsa, ROLE_ARN, None),
            Err(WebIdentityError::InvalidToken)
        );

        // 非法 JSON / 空 keys：保留当前（EC）key，验证不中断。
        for (secs, bytes) in [
            (2u64, b"{ not jwks".as_slice()),
            (3, br#"{"keys":[]}"#.as_slice()),
        ] {
            std::fs::write(&path, bytes).unwrap();
            std::fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_times(
                    std::fs::FileTimes::new()
                        .set_modified(UNIX_EPOCH + Duration::from_secs(1_700_000_000 + secs)),
                )
                .unwrap();
            wi.reload_for_test();
            assert!(
                wi.verify(&ec, ROLE_ARN, None).is_ok(),
                "坏文件不得清空旧 key: {secs}"
            );
        }

        // 恢复为两把合法 key：RSA 与 EC 都可用。
        write(all.keys, 4);
        wi.reload_for_test();
        assert!(wi.verify(&rsa, ROLE_ARN, None).is_ok());
        assert!(wi.verify(&ec, ROLE_ARN, None).is_ok());
    }
}
