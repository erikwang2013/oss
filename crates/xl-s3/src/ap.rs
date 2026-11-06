//! M8 S1：访问点（Access Point）——模型、严格策略解析与 op 推导。
//!
//! 路由形态：`/ap/<name>/<key>`、别名路径 `/<name>-s3alias/<key>`、别名 Host
//! `<name>-s3alias.<endpoint>`。AP 只做**额外限制**（与身份/桶策略取交集），绝不放松
//! 既有授权。策略方言 = 现有 `policy` 子集，但解析更严格（未知字段/条件运算符一律拒）。
//!
//! 账户命名空间 v1：AP ARN 的账户段 = **桶主**的 12 位 uid 派生账户（admin/NULL 桶 =
//! `LEGACY_ACCOUNT`）；旧 ARN 账户段保留一版作资源别名。跨账户授权**只经策略 Principal**
//! （账户根 ARN 解析为账户成员集合），不改资源 ARN 归属。
//! `aws:SourceVpc`/`aws:SourceVpce` 是**显式配置的来源证明**（静态 `OSS_AP_VPC_ID` /
//! `OSS_AP_VPCE_ID` 或可信反代头），不是 PrivateLink/ENI 端点身份，两者互不推断。
//! Slice A 的端点 registry（`[[ap.vpc_endpoints]]`）同样是**注册断言**：只把可信来源的
//! vpce 声明收紧为「已登记、active、受账户约束」，不能证明连接真的来自该端点。
//! 配置 `EndpointIdentityVerifier` 时，来源证明先由验证器按传输层事实收敛为
//! `ApEndpointIdentity`（可信反代网段，或可信网段内已证明代理签署的短期断言；
//! AWS/mTLS 为外部契约），再与 registry 行交叉校验；
//! 未配置时行为与 Slice A 完全一致。AP 缓存是进程本地 + TTL 有界；强跨节点一致性仍是
//! non-goal。
//!
//! 明确不支持（non-goals）：真 VPC 端点（PrivateLink/ENI）控制面身份、经 AP 的桶级
//! Create/Delete/ListBuckets（保存策略时即拒绝这些动作）、多用户账户模型、强跨节点 AP
//! 缓存一致性。

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
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

/// 条件块：IP 条件 + `StringEquals` 的 `aws:SourceVpc`/`aws:SourceVpce`（来源证明）。
/// 未知运算符/条件键由 `deny_unknown_fields` 拒绝。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApCondition {
    #[serde(default, rename = "IpAddress")]
    pub ip_address: Option<ApIpCondition>,
    #[serde(default, rename = "NotIpAddress")]
    pub not_ip_address: Option<ApIpCondition>,
    #[serde(default, rename = "StringEquals")]
    pub string_equals: Option<ApStringEqualsCondition>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApIpCondition {
    #[serde(rename = "aws:SourceIp")]
    pub source_ip: StringOrVec,
}

/// 两个键均为可选：至少出现一个；出现的键必须是合法证明 ID 的非空列表。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApStringEqualsCondition {
    #[serde(default, rename = "aws:SourceVpc")]
    pub source_vpc: Option<StringOrVec>,
    #[serde(default, rename = "aws:SourceVpce")]
    pub source_vpce: Option<StringOrVec>,
}

/// `vpc-`/`vpce-` + 1..=64 位小写字母/数字/`-`（`aws:SourceVpc` 兼容两种形态）。
pub fn valid_vpc_id(raw: &str) -> bool {
    raw.strip_prefix("vpc-")
        .or_else(|| raw.strip_prefix("vpce-"))
        .is_some_and(valid_id_suffix)
}

/// `aws:SourceVpce` 只接受端点 ID 形态 `vpce-` + 1..=64 位小写字母/数字/`-`。
pub fn valid_vpce_id(raw: &str) -> bool {
    raw.strip_prefix("vpce-").is_some_and(valid_id_suffix)
}

fn valid_id_suffix(rest: &str) -> bool {
    !rest.is_empty()
        && rest.len() <= 64
        && rest
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

impl ApCondition {
    fn validate(&self, i: usize) -> Result<(), String> {
        if self.ip_address.is_none()
            && self.not_ip_address.is_none()
            && self.string_equals.is_none()
        {
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
        if let Some(c) = &self.string_equals {
            c.validate(i)?;
        }
        Ok(())
    }

    /// 条件是否成立；`None` = 无法求值（缺对端/缺 VPC/VPCE 证明/坏 CIDR）。调用方按
    /// 语句 Effect 处理：Deny 视为命中（fail-closed），Allow 视为不命中。
    fn matches(&self, ip: Option<IpAddr>, vpc: Option<&str>, vpce: Option<&str>) -> Option<bool> {
        if let Some(c) = &self.ip_address
            && c.matches(ip?) != Some(true)
        {
            return Some(false);
        }
        if let Some(c) = &self.not_ip_address
            && c.matches(ip?) != Some(false)
        {
            return Some(false);
        }
        if let Some(c) = &self.string_equals
            && !c.matches(vpc, vpce)?
        {
            return Some(false);
        }
        Some(true)
    }
}

impl ApStringEqualsCondition {
    fn validate(&self, i: usize) -> Result<(), String> {
        if self.source_vpc.is_none() && self.source_vpce.is_none() {
            return Err(format!(
                "Statement[{i}].Condition.StringEquals 不能为空（支持 aws:SourceVpc/aws:SourceVpce）"
            ));
        }
        if let Some(v) = &self.source_vpc {
            validate_ids(i, "aws:SourceVpc", v, valid_vpc_id)?;
        }
        if let Some(v) = &self.source_vpce {
            validate_ids(i, "aws:SourceVpce", v, valid_vpce_id)?;
        }
        Ok(())
    }

    /// 两个键是 AND；任一证明缺失或 ID 不匹配 → 条件不成立/无法求值。
    fn matches(&self, vpc: Option<&str>, vpce: Option<&str>) -> Option<bool> {
        if let Some(v) = &self.source_vpc
            && !ids_match(v, vpc?)
        {
            return Some(false);
        }
        if let Some(v) = &self.source_vpce
            && !ids_match(v, vpce?)
        {
            return Some(false);
        }
        Some(true)
    }
}

fn validate_ids(
    i: usize,
    key: &str,
    v: &StringOrVec,
    valid: fn(&str) -> bool,
) -> Result<(), String> {
    if v.iter().next().is_none() {
        return Err(format!(
            "Statement[{i}].Condition.StringEquals.{key} 不能为空"
        ));
    }
    for raw in v.iter() {
        if !valid(raw) {
            return Err(format!(
                "Statement[{i}].Condition.StringEquals.{key} 非法 ID: {raw}"
            ));
        }
    }
    Ok(())
}

/// 列表内任一 ID 相等即成立（空列表在保存时已拒）。
fn ids_match(v: &StringOrVec, proof: &str) -> bool {
    v.iter().any(|raw| raw == proof)
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

/// 已解析 CIDR（router 的可信代理解析复用同一口径）。
#[derive(Debug, Clone, Copy)]
pub struct Cidr {
    net: IpAddr,
    /// 已按 `net` 家族归一（v4-mapped 会变 v4，故上限按归一后的家族算）。
    prefix: u8,
}

pub fn parse_cidr(raw: &str) -> Option<Cidr> {
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
    pub fn contains(&self, ip: IpAddr) -> bool {
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

/// AP 路由无法执行的桶级动作：保存时拒绝精确动作（`s3:*` 等通配仍允许），
/// 避免「已授权但永远不生效」的假安全感。
const UNSUPPORTED_BUCKET_ACTIONS: &[&str] =
    &["s3:CreateBucket", "s3:DeleteBucket", "s3:ListAllMyBuckets"];

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
            for act in st.action.iter() {
                if UNSUPPORTED_BUCKET_ACTIONS.contains(&act) {
                    return Err(format!(
                        "Statement[{i}].Action {act} 不适用于访问点：桶级创建/删除/列举无法经 AP 执行"
                    ));
                }
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

    /// 策略是否引用 `aws:SourceVpce`：端点归属账户（R4）只在引用时强制，避免影响
    /// 不关心端点的 AP。
    pub fn references_source_vpce(&self) -> bool {
        self.statement.iter().any(|st| {
            st.condition
                .as_ref()
                .and_then(|c| c.string_equals.as_ref())
                .is_some_and(|e| e.source_vpce.is_some())
        })
    }

    /// 单动作判定（与桶策略同语义：显式 Deny 优先；Allow 命中 true；无匹配 None）。
    /// IP 条件无法求值（缺可信对端）时：Deny **命中**（fail-closed），Allow 不命中。
    pub fn evaluate(&self, req: &ApEvalRequest<'_>, action: &str) -> Option<bool> {
        let mut allowed = false;
        let mut any = false;
        for st in &self.statement {
            if !principal_matches(&st.principal, req.ak, req.uid, req.account, req.anonymous)
                || !st.action.iter().any(|p| glob_match(p, action))
                || !st
                    .resource
                    .iter()
                    .any(|p| req.resources.iter().any(|r| glob_match(p, r)))
            {
                continue;
            }
            let cond = st
                .condition
                .as_ref()
                .map(|c| c.matches(req.source.ip, req.source.vpc, req.source.vpce));
            match (st.effect.as_str(), cond) {
                // 条件求值为 false：该语句不命中（Allow/Deny 同）
                (_, Some(Some(false))) => continue,
                // 条件无法求值 ≈ 命中 Deny（fail-closed）；Allow 不命中
                ("Deny", Some(None)) => return Some(false),
                (_, Some(None)) => continue,
                ("Deny", _) => return Some(false),
                ("Allow", _) => {
                    any = true;
                    allowed = true;
                }
                _ => {}
            }
        }
        (any && allowed).then_some(true)
    }
}

fn principal_matches(
    p: &ApPrincipal,
    ak: &str,
    uid: i64,
    account: ApAccount<'_>,
    anonymous: bool,
) -> bool {
    p.aws.iter().any(|v| {
        v == "*"
            || (!anonymous
                && (v == ak
                    || v.strip_prefix("uid:").and_then(|n| n.parse::<i64>().ok()) == Some(uid)
                    // 账户根 ARN = 请求者**当前账户**的成员集合（成员行优先，无行回落
                    // uid 派生账户）。跨账户只经此 Principal 成立；LEGACY/uid=0 永不匹配。
                    || (account.active
                        && crate::access::account_principal_matches(v, uid, account.id))))
    })
}

/// 请求者账户快照（AP 账户根 Principal 解析用）。`active=false` 时账户根一律不匹配，
/// 且 `ap_decision*` 对签名请求直接拒绝（账户停用 fail-closed）。
#[derive(Debug, Clone, Copy, Default)]
pub struct ApAccount<'a> {
    pub id: Option<&'a str>,
    pub active: bool,
}

/// 来源证明（SourceIp / SourceVpc / SourceVpce）。缺失的证明使对应条件无法求值。
#[derive(Debug, Clone, Copy, Default)]
pub struct ApSourceProof<'a> {
    pub ip: Option<IpAddr>,
    pub vpc: Option<&'a str>,
    pub vpce: Option<&'a str>,
}

/// 求值输入（policy 层不依赖 HTTP 类型）。
pub struct ApEvalRequest<'a> {
    pub ak: &'a str,
    pub uid: i64,
    /// 请求者当前账户（账户根 Principal 用）；匿名用 `Default`。
    pub account: ApAccount<'a>,
    /// 匿名请求只匹配 `Principal: "*"`（空 AK / `uid:0` 不得匹配）。
    pub anonymous: bool,
    /// AP 对象/桶 ARN 与标准 ARN（两者都参与匹配）
    pub resources: Vec<String>,
    /// 连接对端/来源证明（无则带对应条件的语句不匹配，fail-closed）。
    pub source: ApSourceProof<'a>,
}

/// AP 上下文（ARN 生成用）。`account` = 桶账户（命名空间 v1）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApContext {
    pub name: String,
    pub region: String,
    pub account: String,
}

impl ApContext {
    pub fn new(name: &str, region: &str, account: &str) -> Self {
        Self {
            name: name.to_string(),
            region: region.to_string(),
            account: account.to_string(),
        }
    }

    /// 旧版 AP ARN 的账户段别名（一版兼容，已保存的旧策略继续匹配）。
    pub fn legacy(name: &str, region: &str) -> Self {
        Self::new(name, region, crate::access::LEGACY_ACCOUNT)
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
    /// 账户命名空间 v1：**桶主**的 12 位账户（不是 AP 创建者；admin/NULL 桶 = 系统账户）。
    pub account: String,
    pub policy: Option<ApPolicy>,
    pub region: String,
}

/// 解析并校验 AP 策略 JSON（保存 400 / 运行期加载失败 500 共用同一口径）。
pub fn parse_ap_policy(raw: &str) -> Result<ApPolicy, String> {
    let doc: ApPolicy = serde_json::from_str(raw).map_err(|e| format!("AP 策略解析失败: {e}"))?;
    doc.validate()?;
    Ok(doc)
}

impl AccessPoint {
    pub fn context(&self) -> ApContext {
        ApContext::new(&self.name, &self.region, &self.account)
    }

    /// 策略是否引用 `aws:SourceVpce`（决定端点归属账户是否参与授权）。
    pub fn policy_references_source_vpce(&self) -> bool {
        self.policy
            .as_ref()
            .is_some_and(ApPolicy::references_source_vpce)
    }

    /// 从 DB 行构造（策略 JSON 解析失败 → Err，调用方按 fail-closed 处理）。
    pub fn from_row(
        name: String,
        bucket: String,
        policy_json: Option<String>,
        region: &str,
        account: &str,
    ) -> Result<Self, String> {
        let policy = match policy_json {
            Some(raw) if !raw.trim().is_empty() => Some(parse_ap_policy(&raw)?),
            _ => None,
        };
        Ok(Self {
            name,
            bucket,
            account: account.to_string(),
            policy,
            region: region.to_string(),
        })
    }
}

/// AP 策略对单请求的额外限制（与身份/桶策略取交集）：
/// 无策略 = **默认拒绝**（AWS 语义，L3）；`Some(true)` = 放行；`Some(false)` = 拒。
/// `None` 仅由调用方对 DeleteObjects 保留——键在请求体里，逐键判定在 service 层。
#[allow(clippy::too_many_arguments)] // 求值输入集合
pub fn ap_decision(
    ap: &AccessPoint,
    user: &crate::access::AuthedUser,
    account: ApAccount<'_>,
    op: &str,
    key: Option<&str>,
    source: ApSourceProof<'_>,
) -> Option<bool> {
    ap_decision_inner(ap, &user.ak, user.uid, account, false, op, key, source)
}

/// 匿名 AP 判定：只认 `Principal: "*"`（空 AK / `uid:0` 绝不匹配），其余同
/// [`ap_decision`]（Deny 优先、无策略默认拒绝、条件不可求值 fail-closed）。
pub fn ap_decision_anonymous(
    ap: &AccessPoint,
    op: &str,
    key: Option<&str>,
    source: ApSourceProof<'_>,
) -> Option<bool> {
    ap_decision_inner(ap, "", 0, ApAccount::default(), true, op, key, source)
}

#[allow(clippy::too_many_arguments)] // 同上：求值输入集合
fn ap_decision_inner(
    ap: &AccessPoint,
    ak: &str,
    uid: i64,
    account: ApAccount<'_>,
    anonymous: bool,
    op: &str,
    key: Option<&str>,
    source: ApSourceProof<'_>,
) -> Option<bool> {
    let Some(policy) = ap.policy.as_ref() else {
        return Some(false);
    };
    // 账户停用 fail-closed：认证层通常已拦截（AK 失效），此处兜底 mesh/合成身份路径。
    if !anonymous && !account.active {
        return Some(false);
    }
    // 有策略但动作不在方言词表 → 无法表达授权，fail-closed。
    let Some(action) = crate::policy::s3_action(if crate::access::is_copy(op) {
        "PutObject"
    } else {
        op
    }) else {
        return Some(false);
    };
    let ctx = ap.context();
    // 与 policy.rs 同口径：对象动作只匹配对象 ARN，桶动作只匹配桶 ARN。
    // 桶账户 AP 额外保留旧 `LEGACY_ACCOUNT` ARN 一版作资源别名（旧策略继续匹配）。
    let legacy = (ap.account != crate::access::LEGACY_ACCOUNT)
        .then(|| ApContext::legacy(&ap.name, &ap.region));
    let resources = match key {
        Some(k) => {
            let mut v = vec![format!("arn:aws:s3:::{}/{k}", ap.bucket), ctx.object_arn(k)];
            if let Some(l) = &legacy {
                v.push(l.object_arn(k));
            }
            v
        }
        None => {
            let mut v = vec![format!("arn:aws:s3:::{}", ap.bucket), ctx.bucket_arn()];
            if let Some(l) = &legacy {
                v.push(l.bucket_arn());
            }
            v
        }
    };
    let req = ApEvalRequest {
        ak,
        uid,
        account,
        anonymous,
        resources,
        source,
    };
    Some(policy.evaluate(&req, action) == Some(true))
}

/// AP 缓存：进程本地、TTL 有界、带负缓存（控制面写路径即时失效；TTL 是兜底）。
///
/// 一致性与限制：Raft 集群里控制面写会经 PostCommit 副作用刷新每个节点的缓存，
/// 但**跨节点读己之写**仍受 apply 延迟影响（可能瞬时 404）；非 Raft 多节点的 AP
/// 控制面本就是节点本地。强跨节点一致性是 non-goal（见 README）。
pub struct ApCache {
    inner: std::sync::RwLock<std::collections::HashMap<String, Entry>>,
    /// 未知 AP 负缓存（名字 → 写入时刻）；DB/解析错误**不**进负缓存。
    negative: std::sync::RwLock<std::collections::HashMap<String, Instant>>,
    ttl: Duration,
    negative_ttl: Duration,
    max_entries: usize,
}

struct Entry {
    ap: AccessPoint,
    loaded_at: Instant,
}

const DEFAULT_TTL_SECS: u64 = 30;
const DEFAULT_NEGATIVE_TTL_SECS: u64 = 5;
const DEFAULT_MAX_ENTRIES: usize = 10_000;
const MAX_NEGATIVE_ENTRIES: usize = 10_000;
const MAX_TTL_SECS: u64 = 86_400;
const MAX_MAX_ENTRIES: usize = 1_000_000;

impl Default for ApCache {
    fn default() -> Self {
        Self::with_config(
            Duration::from_secs(DEFAULT_TTL_SECS),
            Duration::from_secs(DEFAULT_NEGATIVE_TTL_SECS),
            DEFAULT_MAX_ENTRIES,
        )
    }
}

impl ApCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(ttl: Duration, negative_ttl: Duration, max_entries: usize) -> Self {
        Self {
            inner: std::sync::RwLock::new(std::collections::HashMap::new()),
            negative: std::sync::RwLock::new(std::collections::HashMap::new()),
            ttl,
            negative_ttl,
            max_entries,
        }
    }

    /// 生产装配：`OSS_AP_CACHE_TTL_SECS`（默认 30，0 = 不缓存正条目）、
    /// `OSS_AP_NEGATIVE_CACHE_TTL_SECS`（默认 5，0 = 关闭负缓存）、
    /// `OSS_AP_CACHE_MAX_ENTRIES`（默认 10000）。非法值 → 启动失败。
    pub fn from_env() -> Result<Self, String> {
        Ok(Self::with_config(
            Duration::from_secs(env_u64(
                "OSS_AP_CACHE_TTL_SECS",
                DEFAULT_TTL_SECS,
                0,
                MAX_TTL_SECS,
            )?),
            Duration::from_secs(env_u64(
                "OSS_AP_NEGATIVE_CACHE_TTL_SECS",
                DEFAULT_NEGATIVE_TTL_SECS,
                0,
                MAX_TTL_SECS,
            )?),
            env_u64(
                "OSS_AP_CACHE_MAX_ENTRIES",
                DEFAULT_MAX_ENTRIES as u64,
                1,
                MAX_MAX_ENTRIES as u64,
            )? as usize,
        ))
    }

    pub fn get(&self, name: &str) -> Option<AccessPoint> {
        let w = self.inner.read().ok()?;
        let e = w.get(name)?;
        (e.loaded_at.elapsed() < self.ttl).then(|| e.ap.clone())
    }

    /// 未知 AP 负缓存命中（load 路径据此跳过 DB 查询）。
    pub fn negative_hit(&self, name: &str) -> bool {
        match self.negative.read() {
            Ok(m) => m
                .get(name)
                .is_some_and(|at| at.elapsed() < self.negative_ttl),
            Err(_) => false,
        }
    }

    pub fn put(&self, ap: AccessPoint) {
        let name = ap.name.clone();
        if let Ok(mut w) = self.inner.write() {
            if !w.contains_key(&name) && w.len() >= self.max_entries {
                // 上限优先保可用性：先清过期条目，仍满则本条目不入缓存（直接查 DB）。
                w.retain(|_, e| e.loaded_at.elapsed() < self.ttl);
            }
            if w.contains_key(&name) || w.len() < self.max_entries {
                w.insert(
                    name.clone(),
                    Entry {
                        ap,
                        loaded_at: Instant::now(),
                    },
                );
            } else {
                tracing::warn!(
                    entries = w.len(),
                    max = self.max_entries,
                    "AP 缓存已满，新条目不缓存"
                );
            }
        }
        if let Ok(mut n) = self.negative.write() {
            n.remove(&name);
        }
    }

    /// 记录未知 AP（短 TTL；有界，满则整体清空后重来）。
    pub fn put_negative(&self, name: &str) {
        if self.negative_ttl.is_zero() {
            return;
        }
        if let Ok(mut w) = self.negative.write() {
            if w.len() >= MAX_NEGATIVE_ENTRIES {
                w.clear();
            }
            w.insert(name.to_string(), Instant::now());
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
            w.retain(|_, e| e.ap.bucket != bucket);
        }
    }

    /// 启动预加载/全量刷新。
    pub fn replace_all(&self, items: impl IntoIterator<Item = AccessPoint>) {
        let now = Instant::now();
        if let Ok(mut w) = self.inner.write() {
            *w = items
                .into_iter()
                .map(|ap| (ap.name.clone(), Entry { ap, loaded_at: now }))
                .collect();
        }
        if let Ok(mut n) = self.negative.write() {
            n.clear();
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|w| w.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn env_u64(name: &str, default: u64, min: u64, max: u64) -> Result<u64, String> {
    let raw = match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Ok(default),
    };
    let v: u64 = raw
        .trim()
        .parse()
        .map_err(|_| format!("{name} 非法: {}", raw.escape_debug()))?;
    if !(min..=max).contains(&v) {
        return Err(format!("{name} 超出范围 {min}..={max}: {v}"));
    }
    Ok(v)
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

/// HTTP 层注入的**来源 VPC 证明**（`aws:SourceVpc` 条件求值用）。
/// 缺省 → 带 VPC 条件的语句不匹配（Allow 不命中、Deny 命中，fail-closed）。
#[derive(Debug, Clone)]
pub struct ApVpc(pub String);

/// HTTP 层注入的**来源 VPC 端点证明**（`aws:SourceVpce` 条件求值用）。
/// 独立于 `ApVpc`：绝不从 SourceVpc 推断；缺省 → 带 VPCE 条件的语句 fail-closed。
#[derive(Debug, Clone)]
pub struct ApVpce(pub String);

/// 端点身份证据来源（验证器产出；判定/审计用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApEndpointEvidence {
    /// 进程级静态配置（`OSS_AP_VPCE_ID`）：部署证明，不依赖连接。
    StaticConfig,
    /// 可信反代：直连对端在显式可信网段内，声明头由该反代注入。
    TrustedProxy,
    /// 已证明代理签发的短期端点断言（HS256 JWT）：签名 + 可信 peer + 声明匹配。
    AttestedProxy,
    /// 双向 TLS 客户端证书（外部验证器；本仓库尚未把证书下沉到请求，见 trait 契约）。
    Mtls,
    /// AWS PrivateLink 控制面 API 核对（外部验证器；本仓库不伪造，见 trait 契约）。
    AwsApi,
}

impl ApEndpointEvidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StaticConfig => "static",
            Self::TrustedProxy => "trusted-proxy",
            Self::AttestedProxy => "attested-proxy",
            Self::Mtls => "mtls",
            Self::AwsApi => "aws-api",
        }
    }
}

/// 验证后的端点身份（registry 行之前）：`vpce_id` 已证明来源；`vpc_id`/`account_id`
/// 仅当证据来源能独立给出时有值（可信反代不提供），与 registry 行不一致即拒绝。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApEndpointIdentity {
    pub vpce_id: String,
    pub vpc_id: Option<String>,
    pub account_id: Option<String>,
    pub evidence: ApEndpointEvidence,
}

/// 路由器归一后的端点声明（**来源未验证**）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ApEndpointClaim {
    #[default]
    Absent,
    /// 恰好一条、形态合法的头值。
    Header(String),
    /// 头存在但重复/畸形 → 验证器必须拒绝（fail-closed）。
    Malformed,
}

/// 紧凑 JWT 形状（三段 base64url、非空、有界长度）：签名/时效由验证器判定。
/// 只用于把头归一为 `Header`/`Malformed`，本身不赋予任何信任。
pub fn valid_attestation(raw: &str) -> bool {
    (16..=4096).contains(&raw.len())
        && raw.bytes().filter(|b| *b == b'.').count() == 2
        && raw.split('.').all(|part| !part.is_empty())
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// 端点身份验证输入：只含传输层/路由器可给出的事实，不含已采信的头语义。
#[derive(Debug, Clone, Default)]
pub struct ApEndpointContext {
    pub claim: ApEndpointClaim,
    /// `x-oss-endpoint-attestation`（代理签名断言）；与 claim 同样的 fail-closed 归一。
    pub attestation: ApEndpointClaim,
    /// 进程级静态声明（部署配置，不是连接事实）。
    pub static_vpce: Option<String>,
    /// 直连 TCP 对端（ConnectInfo）；None = 传输层不提供 → 可信反代不成立。
    pub direct_peer: Option<IpAddr>,
    /// rustls 握手期已验证的客户端叶证书 SHA-256（DER 摘要）；None = 未出示/未启 mTLS。
    /// 只由 TLS 监听器下沉，客户端头/请求体无法伪造。
    pub client_cert_sha256: Option<[u8; 32]>,
}

/// 验证失败：区分「声明被拒」（403）与「验证后端不可用」（500）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApEndpointVerifyError {
    Denied(&'static str),
    Unavailable(String),
}

/// 端点身份验证器：把请求上下文收敛为已证明的端点身份，**先于** registry 查询。
///
/// 契约：
/// - 只依据不可由客户端伪造的传输层事实（直连对端、TLS 客户端证书、静态配置）；
/// - `Ok(None)` = 无端点声明（不单独构成拒绝，由策略条件决定）；`Ok(Some(_))` = 已验证；
/// - 声明存在但无法证明来源 → `Denied`（403）；验证后端失败 → `Unavailable`（500）；
/// - registry 的登记/状态/VPC/账户（R2–R5）检查仍在验证之后执行，验证器绝不放宽它们。
///
/// 实现位置：
/// - mTLS：本模块 [`MtlsEndpointVerifier`]（证书指纹 → 端点登记表），证书 SHA-256 由
///   oss-server 的 TLS 监听器从 rustls 握手结果下沉到 [`ApEndpointContext`]。
/// - AWS PrivateLink：oss-server 的 `ap_aws` 模块（EC2 `DescribeNetworkInterfaces` +
///   `DescribeVpcEndpoints`），只用 `direct_peer` 反查 ENI/端点，要求 `State=available`；
///   API 超时/限流/错误一律 `Unavailable`，结果按源 IP/TTL 缓存且失败不放行；绝不读取
///   客户端端点/账户头。真实 AWS 身份需在具备 PrivateLink 的环境做外部验收。
#[async_trait::async_trait]
pub trait EndpointIdentityVerifier: Send + Sync {
    async fn verify(
        &self,
        ctx: &ApEndpointContext,
    ) -> Result<Option<ApEndpointIdentity>, ApEndpointVerifyError>;
}

/// 可信反代验证器：直连 TCP 对端落在显式可信网段内才采信端点声明头。
/// 客户端直连（对端不在网段/无 ConnectInfo）→ 拒绝；重复/畸形头 → 拒绝。
pub struct TrustedProxyEndpointVerifier {
    trusted_peers: Vec<Cidr>,
}

impl TrustedProxyEndpointVerifier {
    pub fn new(trusted_peers: impl IntoIterator<Item = Cidr>) -> Self {
        Self {
            trusted_peers: trusted_peers.into_iter().collect(),
        }
    }
}

#[async_trait::async_trait]
impl EndpointIdentityVerifier for TrustedProxyEndpointVerifier {
    async fn verify(
        &self,
        ctx: &ApEndpointContext,
    ) -> Result<Option<ApEndpointIdentity>, ApEndpointVerifyError> {
        // 静态部署证明优先，与 `Attest` 的既有语义一致。
        if let Some(vpce) = &ctx.static_vpce {
            return Ok(Some(ApEndpointIdentity {
                vpce_id: vpce.clone(),
                vpc_id: None,
                account_id: None,
                evidence: ApEndpointEvidence::StaticConfig,
            }));
        }
        match &ctx.claim {
            ApEndpointClaim::Absent => Ok(None),
            ApEndpointClaim::Malformed => Err(ApEndpointVerifyError::Denied("malformed-claim")),
            ApEndpointClaim::Header(vpce) => {
                if !ctx
                    .direct_peer
                    .is_some_and(|ip| self.trusted_peers.iter().any(|c| c.contains(ip)))
                {
                    return Err(ApEndpointVerifyError::Denied("untrusted-source"));
                }
                Ok(Some(ApEndpointIdentity {
                    vpce_id: vpce.clone(),
                    vpc_id: None,
                    account_id: None,
                    evidence: ApEndpointEvidence::TrustedProxy,
                }))
            }
        }
    }
}

/// 已证明代理（terminates mTLS/PrivateLink）签发的短期端点断言验证器。
///
/// 代理对「连接 → VPC 端点」的映射签署 HS256 紧凑 JWT，claims：`vpce`（必需）、
/// 可选 `vpc`/`account`、`iat`/`exp`。直连客户端既无密钥也不在可信网段，无法伪造。
/// 验证顺序：静态部署证明 → 头形状 → 可信直连对端 → 签名/时效 → vpce 与声明一致。
pub struct AttestedProxyEndpointVerifier {
    trusted_peers: Vec<Cidr>,
    key: DecodingKey,
    max_age_secs: u64,
}

/// 断言 claims（额外字段忽略；时效由本验证器显式判定）。
#[derive(Deserialize)]
struct EndpointAttestationClaims {
    vpce: String,
    vpc: Option<String>,
    account: Option<String>,
    iat: u64,
    exp: u64,
}

impl AttestedProxyEndpointVerifier {
    /// HS256 共享密钥下限（RFC 7518 §3.2：不短于哈希输出）；不足拒绝启动。
    pub const MIN_SECRET_BYTES: usize = 32;

    /// `max_age_secs`：签发时效上限，也是 token 总寿命上限（`exp - iat`）。
    pub fn new(
        trusted_peers: impl IntoIterator<Item = Cidr>,
        secret: &[u8],
        max_age_secs: u64,
    ) -> Result<Self, String> {
        if secret.len() < Self::MIN_SECRET_BYTES {
            return Err(format!(
                "OSS_AP_ATTESTATION_SECRET 至少 {} 字节（当前 {}）",
                Self::MIN_SECRET_BYTES,
                secret.len()
            ));
        }
        if max_age_secs == 0 {
            return Err("OSS_AP_ATTESTATION_MAX_AGE_SECS 必须 >= 1".into());
        }
        Ok(Self {
            trusted_peers: trusted_peers.into_iter().collect(),
            key: DecodingKey::from_secret(secret),
            max_age_secs,
        })
    }

    fn decode(&self, token: &str) -> Result<EndpointAttestationClaims, ApEndpointVerifyError> {
        let mut validation = Validation::new(Algorithm::HS256);
        // 时效统一在本验证器判定（exp/iat/窗口），避免库默认 leeway 放宽。
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        validation.leeway = 0;
        decode::<EndpointAttestationClaims>(token, &self.key, &validation)
            .map(|data| data.claims)
            .map_err(|_| ApEndpointVerifyError::Denied("bad-attestation"))
    }
}

#[async_trait::async_trait]
impl EndpointIdentityVerifier for AttestedProxyEndpointVerifier {
    async fn verify(
        &self,
        ctx: &ApEndpointContext,
    ) -> Result<Option<ApEndpointIdentity>, ApEndpointVerifyError> {
        use ApEndpointClaim::{Absent, Header, Malformed};
        // 静态部署证明优先，与 `Attest` 的既有语义一致。
        if let Some(vpce) = &ctx.static_vpce {
            return Ok(Some(ApEndpointIdentity {
                vpce_id: vpce.clone(),
                vpc_id: None,
                account_id: None,
                evidence: ApEndpointEvidence::StaticConfig,
            }));
        }
        let (vpce, token) = match (&ctx.claim, &ctx.attestation) {
            (Malformed, _) => return Err(ApEndpointVerifyError::Denied("malformed-claim")),
            (_, Malformed) => return Err(ApEndpointVerifyError::Denied("malformed-attestation")),
            (Absent, Absent) => return Ok(None),
            (Absent, Header(_)) => return Err(ApEndpointVerifyError::Denied("missing-claim")),
            (Header(_), Absent) => {
                return Err(ApEndpointVerifyError::Denied("missing-attestation"));
            }
            (Header(vpce), Header(token)) => (vpce, token),
        };
        // 先验连接来源：非可信对端不消耗签名验证，也绝不采信其断言。
        if !ctx
            .direct_peer
            .is_some_and(|ip| self.trusted_peers.iter().any(|c| c.contains(ip)))
        {
            return Err(ApEndpointVerifyError::Denied("untrusted-source"));
        }
        let claims = self.decode(token)?;
        if !valid_vpce_id(&claims.vpce)
            || claims.vpc.as_deref().is_some_and(|v| !valid_vpc_id(v))
            || claims
                .account
                .as_deref()
                .is_some_and(|a| !crate::access::valid_account_id(a))
        {
            return Err(ApEndpointVerifyError::Denied("invalid-attestation"));
        }
        if claims.vpce != *vpce {
            return Err(ApEndpointVerifyError::Denied("vpce-mismatch"));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if claims.iat > now {
            return Err(ApEndpointVerifyError::Denied("attestation-not-yet-valid"));
        }
        if now - claims.iat > self.max_age_secs {
            return Err(ApEndpointVerifyError::Denied("stale-attestation"));
        }
        if claims.exp <= now {
            return Err(ApEndpointVerifyError::Denied("expired-attestation"));
        }
        if claims.exp.saturating_sub(claims.iat) > self.max_age_secs {
            return Err(ApEndpointVerifyError::Denied("attestation-window"));
        }
        Ok(Some(ApEndpointIdentity {
            vpce_id: claims.vpce,
            vpc_id: claims.vpc,
            account_id: claims.account,
            evidence: ApEndpointEvidence::AttestedProxy,
        }))
    }
}

/// 证书指纹登记的端点身份（mTLS 叶证书 → vpce/vpc/account）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MtlsEndpoint {
    pub vpce_id: String,
    pub vpc_id: Option<String>,
    pub account_id: Option<String>,
}

/// 直接 mTLS 端点身份验证器：rustls 已验证的客户端叶证书 SHA-256 → 端点登记表。
///
/// 配置载体 `OSS_AP_MTLS_ENDPOINTS`（oss-server 解析后调用 [`Self::parse`]）：
/// `<sha256>=<vpce>[=<vpc>[=<account>]]`，多条以 `;` 分隔；指纹为 64 位 hex
/// （大小写不敏感、可带 `:` 分隔），重复指纹/非法 ID 一律拒绝启动。
/// 证书由监听器在握手期验证，客户端头无法伪造；声明头若存在必须与登记 vpce 一致。
pub struct MtlsEndpointVerifier {
    endpoints: std::collections::HashMap<[u8; 32], MtlsEndpoint>,
}

impl MtlsEndpointVerifier {
    pub fn new(items: impl IntoIterator<Item = ([u8; 32], MtlsEndpoint)>) -> Result<Self, String> {
        let mut endpoints = std::collections::HashMap::new();
        for (fingerprint, endpoint) in items {
            if endpoints.insert(fingerprint, endpoint).is_some() {
                return Err("OSS_AP_MTLS_ENDPOINTS 指纹重复".into());
            }
        }
        if endpoints.is_empty() {
            return Err("OSS_AP_MTLS_ENDPOINTS 不能为空".into());
        }
        Ok(Self { endpoints })
    }

    /// 解析 `<fp>=<vpce>[=<vpc>[=<account>]];...`；空条目忽略，其余错误拒绝启动。
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut items = Vec::new();
        for entry in raw.split(';').map(str::trim).filter(|e| !e.is_empty()) {
            let mut fields = entry.split('=').map(str::trim);
            let (Some(fp), Some(vpce)) = (fields.next(), fields.next()) else {
                return Err(format!(
                    "OSS_AP_MTLS_ENDPOINTS 条目须为 <sha256>=<vpce>[=<vpc>[=<account>]]: {entry:?}"
                ));
            };
            let vpc = fields.next().filter(|v| !v.is_empty());
            let account = fields.next().filter(|v| !v.is_empty());
            if fields.next().is_some() {
                return Err(format!("OSS_AP_MTLS_ENDPOINTS 条目字段过多: {entry:?}"));
            }
            let fingerprint = parse_sha256_hex(fp)
                .ok_or_else(|| format!("OSS_AP_MTLS_ENDPOINTS 证书指纹非法: {fp:?}"))?;
            if !valid_vpce_id(vpce) {
                return Err(format!("OSS_AP_MTLS_ENDPOINTS vpce 非法: {vpce:?}"));
            }
            if vpc.is_some_and(|v| !valid_vpc_id(v)) {
                return Err(format!("OSS_AP_MTLS_ENDPOINTS vpc 非法: {vpc:?}"));
            }
            if account.is_some_and(|a| !crate::access::valid_account_id(a)) {
                return Err(format!("OSS_AP_MTLS_ENDPOINTS account 非法: {account:?}"));
            }
            items.push((
                fingerprint,
                MtlsEndpoint {
                    vpce_id: vpce.to_string(),
                    vpc_id: vpc.map(str::to_string),
                    account_id: account.map(str::to_string),
                },
            ));
        }
        Self::new(items)
    }
}

/// 64 位 hex（可带 `:` 分隔、大小写不敏感）→ SHA-256 原始摘要。
fn parse_sha256_hex(raw: &str) -> Option<[u8; 32]> {
    let bytes: Vec<u8> = raw.bytes().filter(|b| *b != b':').collect();
    if bytes.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in bytes.as_chunks::<2>().0.iter().enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = (hi * 16 + lo) as u8;
    }
    Some(out)
}

#[async_trait::async_trait]
impl EndpointIdentityVerifier for MtlsEndpointVerifier {
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
        let Some(fingerprint) = ctx.client_cert_sha256 else {
            return match &ctx.claim {
                ApEndpointClaim::Absent => Ok(None),
                _ => Err(ApEndpointVerifyError::Denied("no-client-certificate")),
            };
        };
        let Some(endpoint) = self.endpoints.get(&fingerprint) else {
            return Err(ApEndpointVerifyError::Denied("unknown-certificate"));
        };
        if let ApEndpointClaim::Header(vpce) = &ctx.claim
            && vpce != &endpoint.vpce_id
        {
            return Err(ApEndpointVerifyError::Denied("vpce-mismatch"));
        }
        Ok(Some(ApEndpointIdentity {
            vpce_id: endpoint.vpce_id.clone(),
            vpc_id: endpoint.vpc_id.clone(),
            account_id: endpoint.account_id.clone(),
            evidence: ApEndpointEvidence::Mtls,
        }))
    }
}

/// 一个 VPC 端点注册行。Slice A 的 registry 是**注册断言**（配置声明的
/// vpce → {vpc, account, state}），不是密码学端点身份：它不能证明连接真的来自该
/// PrivateLink 端点，只把「代理声明」收紧为「已登记、受账户约束、可吊销」。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApEndpoint {
    pub vpce_id: String,
    pub vpc_id: String,
    pub account_id: String,
    /// 仅 `active` 放行；其它值一律 fail-closed。
    pub state: String,
}

/// 端点 registry 查询。`Err` = 查询失败（路由按 500 fail-closed，绝不当作「未登记」）。
#[async_trait::async_trait]
pub trait ApEndpointSource: Send + Sync {
    async fn lookup(&self, vpce: &str) -> Result<Option<ApEndpoint>, String>;
}

/// Slice A 静态 registry（`[ap.vpc_endpoints]` 启动装配；测试注入同一实现）。
pub struct StaticApEndpointSource {
    rows: std::collections::HashMap<String, ApEndpoint>,
}

impl StaticApEndpointSource {
    pub fn new(items: impl IntoIterator<Item = ApEndpoint>) -> Self {
        Self {
            rows: items.into_iter().map(|e| (e.vpce_id.clone(), e)).collect(),
        }
    }
}

#[async_trait::async_trait]
impl ApEndpointSource for StaticApEndpointSource {
    async fn lookup(&self, vpce: &str) -> Result<Option<ApEndpoint>, String> {
        Ok(self.rows.get(vpce).cloned())
    }
}

/// DeleteObjects 经 AP 的逐键策略上下文：键在请求体里，整体判定必然失真，
/// 由 `service.rs::delete_objects` 在逐键循环里叠交集（被拒键绝不进入删除路径）。
#[derive(Debug, Clone)]
pub struct ApDeletePolicy {
    pub ap: AccessPoint,
    pub account_id: Option<String>,
    pub account_active: bool,
    pub source_ip: Option<IpAddr>,
    pub source_vpc: Option<String>,
    pub source_vpce: Option<String>,
}

/// `/ap/<name>/...` 前置路由：鉴权（身份/桶策略 × AP 策略交集）→ 重写到内层服务。
pub struct ApRoute {
    cache: Arc<ApCache>,
    source: Arc<dyn ApSource>,
    /// Slice A：VPC 端点注册断言（只收断言命中的 active 行；查询失败 500）。
    endpoints: Arc<dyn ApEndpointSource>,
    /// 端点身份验证器；None = Slice A 行为（可信头/静态 → registry）。
    verifier: Option<Arc<dyn EndpointIdentityVerifier>>,
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
        endpoints: Arc<dyn ApEndpointSource>,
        acl: crate::access::Acl,
        olap: Arc<crate::olap::OlapCache>,
        inner: Arc<s3s::service::S3Service>,
    ) -> Self {
        Self {
            cache,
            source,
            endpoints,
            verifier: None,
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

    /// 安装端点身份验证器：先证明来源，再查 registry。未安装 = Slice A 行为不变。
    pub fn with_verifier(mut self, verifier: Arc<dyn EndpointIdentityVerifier>) -> Self {
        self.verifier = Some(verifier);
        self
    }

    async fn load_ap(&self, name: &str) -> S3Result<AccessPoint> {
        if let Some(ap) = self.cache.get(name) {
            return Ok(ap);
        }
        // 负缓存只记「确认不存在」；DB/解析错误不缓存（保持 500 fail-closed）。
        if self.cache.negative_hit(name) {
            return Err(s3s::s3_error!(NoSuchAccessPoint, "access point not found"));
        }
        match self.source.lookup(name).await {
            Ok(Some(ap)) => {
                self.cache.put(ap.clone());
                Ok(ap)
            }
            Ok(None) => {
                self.cache.put_negative(name);
                Err(s3s::s3_error!(NoSuchAccessPoint, "access point not found"))
            }
            Err(e) => {
                tracing::error!(error = %e, ap = name, "AP 查询失败");
                Err(s3s::s3_error!(InternalError, "internal error"))
            }
        }
    }
}

/// `ApVpce` 断言的 registry 门禁结果（R2–R4）。`None` = 无断言，行为不变。
struct EndpointCheck {
    vpce: String,
    /// 命中的注册行（未登记时 None，仍要审计 vpce id）。
    endpoint: Option<ApEndpoint>,
    /// 验证器给出的证据来源（仅验证路径；None = Slice A）。
    evidence: Option<ApEndpointEvidence>,
    /// Some(reason) = 拒绝。
    denial: Option<&'static str>,
}

/// R2：有端点断言时 registry 必须有 active 行（查无/停用拒绝）；
/// R3：同时有 `ApVpc` 断言时必须等于 registry.vpc_id（防两证明拼凑）；
/// R4：策略引用 `aws:SourceVpce` 时 endpoint.account_id 必须等于 AP 桶账户；
/// R5：registry 查询失败 → 500（不当作「未登记」放行）；
/// R6：验证器独立给出的 vpc/account 与 registry 行不一致 → 拒绝（证据与登记分裂）。
async fn check_ap_endpoint(
    endpoints: &dyn ApEndpointSource,
    ap: &AccessPoint,
    source_vpc: Option<&str>,
    source_vpce: Option<&str>,
    identity: Option<&ApEndpointIdentity>,
) -> S3Result<Option<EndpointCheck>> {
    let Some(vpce) = source_vpce else {
        return Ok(None);
    };
    let endpoint = endpoints.lookup(vpce).await.map_err(|e| {
        tracing::error!(error = %e, vpce, "AP 端点 registry 查询失败");
        s3s::s3_error!(InternalError, "internal error")
    })?;
    let denial = match &endpoint {
        None => Some("unregistered"),
        Some(e) if e.state != "active" => Some("disabled"),
        Some(e) if source_vpc.is_some_and(|v| v != e.vpc_id) => Some("vpc-mismatch"),
        Some(e)
            if identity
                .and_then(|i| i.vpc_id.as_deref())
                .is_some_and(|v| v != e.vpc_id) =>
        {
            Some("identity-vpc-mismatch")
        }
        Some(e)
            if identity
                .and_then(|i| i.account_id.as_deref())
                .is_some_and(|a| a != e.account_id) =>
        {
            Some("identity-account-mismatch")
        }
        Some(e) if ap.policy_references_source_vpce() && e.account_id != ap.account => {
            Some("account-mismatch")
        }
        Some(_) => None,
    };
    Ok(Some(EndpointCheck {
        vpce: vpce.to_string(),
        endpoint,
        evidence: identity.map(|i| i.evidence),
        denial,
    }))
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

/// `x-amz-copy-source` → (源桶, 源键)；仅接受带桶名的形态（CopyObject 语义）。
fn copy_source_bucket_key(headers: &http::HeaderMap) -> Option<(String, String)> {
    headers
        .get(s3s::header::X_AMZ_COPY_SOURCE)
        .and_then(|v| v.to_str().ok())
        .map(s3s::dto::CopySource::parse)
        .and_then(|r| match r {
            Ok(s3s::dto::CopySource::Bucket { bucket, key, .. }) => {
                Some((bucket.to_string(), key.to_string()))
            }
            _ => None,
        })
}

/// 与 s3s `is_multipart_post` 同口径（忽略参数）。
fn is_multipart_form_data(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .eq_ignore_ascii_case("multipart/form-data")
}

/// 单个查询键的值；同一键出现多次（意图不可判定）→ `InvalidRequest`。
fn query_value_once<'a>(query: &'a str, key: &str) -> S3Result<Option<&'a str>> {
    let mut found = None;
    for part in query.split('&') {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        if k != key {
            continue;
        }
        if found.is_some() {
            return Err(s3s::s3_error!(InvalidRequest, "duplicate query parameter"));
        }
        found = Some(v);
    }
    Ok(found)
}

/// 查询键白名单：除列出的键外一律拒（X-Amz-* 已剥离；空段容忍）。未列出的子资源
/// 会被 s3s 内层解析成另一个 op（如 `?acl` → PutObjectAcl），授权 op 必须与执行 op 一致。
fn only_query_keys(query: &str, allowed: &[&str]) -> bool {
    query
        .split('&')
        .all(|p| p.is_empty() || allowed.contains(&p.split('=').next().unwrap_or("")))
}

fn unsupported_op<T>() -> S3Result<T> {
    Err(s3s::s3_error!(
        NotImplemented,
        "unsupported access point operation"
    ))
}

fn pick(ok: bool, op: &'static str) -> S3Result<&'static str> {
    if ok { Ok(op) } else { unsupported_op() }
}

const GET_OBJECT_QUERY: &[&str] = &[
    "versionId",
    "partNumber",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "x-id",
];
const LIST_V1_QUERY: &[&str] = &[
    "prefix",
    "delimiter",
    "marker",
    "max-keys",
    "encoding-type",
    "x-id",
];
const LIST_V2_QUERY: &[&str] = &[
    "list-type",
    "prefix",
    "delimiter",
    "continuation-token",
    "max-keys",
    "encoding-type",
    "fetch-owner",
    "start-after",
    "x-id",
];
const LIST_VERSIONS_QUERY: &[&str] = &[
    "versions",
    "prefix",
    "delimiter",
    "key-marker",
    "version-id-marker",
    "max-keys",
    "encoding-type",
    "x-id",
];
const LIST_UPLOADS_QUERY: &[&str] = &[
    "uploads",
    "prefix",
    "delimiter",
    "key-marker",
    "upload-id-marker",
    "max-uploads",
    "encoding-type",
    "x-id",
];
const LIST_PARTS_QUERY: &[&str] = &[
    "uploadId",
    "part-number-marker",
    "max-parts",
    "encoding-type",
    "x-id",
];

/// 授权用的 op 推导：与 s3s 内层的路由表逐形状对齐，超出白名单的形状一律 501。
/// 关键点：**单一子资源 + 精确参数集**，避免多子资源时内层优先级选出另一个 op。
fn derive_op(
    method: &http::Method,
    key: Option<&str>,
    query: &str,
    copy_source: bool,
) -> S3Result<&'static str> {
    use http::Method;
    let query = strip_signed_query(query);
    let query = query.as_str();
    let has = |k: &str| query_has(query, k);
    let only = |allowed: &[&str]| only_query_keys(query, allowed);
    match *method {
        Method::GET => match key {
            Some(_) => {
                if has("uploadId") {
                    pick(only(LIST_PARTS_QUERY), "ListParts")
                } else if has("tagging") {
                    pick(only(&["tagging", "versionId", "x-id"]), "GetObjectTagging")
                } else if has("retention") {
                    pick(
                        only(&["retention", "versionId", "x-id"]),
                        "GetObjectRetention",
                    )
                } else if has("legal-hold") {
                    pick(
                        only(&["legal-hold", "versionId", "x-id"]),
                        "GetObjectLegalHold",
                    )
                } else {
                    pick(only(GET_OBJECT_QUERY), "GetObject")
                }
            }
            None => {
                if has("versions") {
                    pick(only(LIST_VERSIONS_QUERY), "ListObjectVersions")
                } else if has("uploads") {
                    pick(only(LIST_UPLOADS_QUERY), "ListMultipartUploads")
                } else if query_value_once(query, "list-type")? == Some("2") {
                    pick(only(LIST_V2_QUERY), "ListObjectsV2")
                } else if has("versioning") {
                    pick(only(&["versioning", "x-id"]), "GetBucketVersioning")
                } else {
                    pick(only(LIST_V1_QUERY), "ListObjects")
                }
            }
        },
        Method::HEAD => match key {
            // 与 GET 同形：s3s 的 HeadObject 也解析 response-* 覆盖参数。
            Some(_) => pick(only(GET_OBJECT_QUERY), "HeadObject"),
            None => pick(only(&["x-id"]), "HeadBucket"),
        },
        Method::PUT => match key {
            Some(_) => {
                if has("uploadId") {
                    if !has("partNumber") {
                        unsupported_op()?
                    }
                    pick(
                        only(&["uploadId", "partNumber", "x-id"]),
                        if copy_source {
                            "UploadPartCopy"
                        } else {
                            "UploadPart"
                        },
                    )
                } else if copy_source {
                    pick(only(&["x-id"]), "CopyObject")
                } else if has("tagging") {
                    pick(only(&["tagging", "versionId", "x-id"]), "PutObjectTagging")
                } else if has("retention") {
                    pick(
                        only(&["retention", "versionId", "x-id"]),
                        "PutObjectRetention",
                    )
                } else if has("legal-hold") {
                    pick(
                        only(&["legal-hold", "versionId", "x-id"]),
                        "PutObjectLegalHold",
                    )
                } else {
                    pick(only(&["x-id"]), "PutObject")
                }
            }
            None => {
                if has("versioning") {
                    pick(only(&["versioning", "x-id"]), "PutBucketVersioning")
                } else {
                    unsupported_op()
                }
            }
        },
        Method::POST => match key {
            Some(_) => {
                if has("uploads") {
                    pick(only(&["uploads", "x-id"]), "CreateMultipartUpload")
                } else if has("uploadId") {
                    pick(only(&["uploadId", "x-id"]), "CompleteMultipartUpload")
                } else if has("restore") {
                    // S4b：内层 RestoreObject 已启用（restore 查询随 call() 原样转发）
                    pick(only(&["restore", "versionId", "x-id"]), "RestoreObject")
                } else {
                    unsupported_op()
                }
            }
            None => {
                // 键在请求体：逐键授权在 service.rs 里叠 AP 策略（check_access 只校验形状）
                if has("delete") {
                    pick(only(&["delete", "x-id"]), "DeleteObjects")
                } else {
                    // s3s 把 POST /{bucket}?uploads 也当未知操作 → 501
                    unsupported_op()
                }
            }
        },
        Method::DELETE => match key {
            Some(_) => {
                if has("uploadId") {
                    pick(only(&["uploadId", "x-id"]), "AbortMultipartUpload")
                } else if has("tagging") {
                    pick(
                        only(&["tagging", "versionId", "x-id"]),
                        "DeleteObjectTagging",
                    )
                } else {
                    pick(only(&["versionId", "x-id"]), "DeleteObject")
                }
            }
            None => unsupported_op(),
        },
        _ => unsupported_op(),
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

/// 去掉指定查询键（值一并丢），保留其余原样顺序。
fn strip_query_key(query: &str, key: &str) -> String {
    query
        .split('&')
        .filter(|p| p.split('=').next().unwrap_or("") != key)
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
            // 已缓冲 body → BodySizeLimitExceeded；流式 body → http_body_util::Limited
            // 的 LengthLimitError（s3s 不 re-export，两者都要映射为 400 EntityTooLarge）。
            if e.downcast_ref::<s3s::BodySizeLimitExceeded>().is_some()
                || e.downcast_ref::<http_body_util::LengthLimitError>()
                    .is_some()
            {
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
        // `/ap/` 前缀恒认领（未知 AP 404，绝不回落桶路由）。
        // 别名仅在同名 AP 存在时认领：否则旧 `*-s3alias` 桶可继续走普通桶路由（L4）。
        // 同名 AP 与旧桶并存时 AP 优先（后缀在建桶路径上已保留）。
        let ap_path = uri.path().starts_with("/ap/");
        let alias = !ap_path
            && host_alias(uri, headers)
                .or_else(|| path_alias(uri))
                .and_then(|a| strip_alias(&a).ok())
                .is_some_and(|name| self.cache.get(&name).is_some());
        if ap_path || alias {
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
        // 无凭据 = 匿名 AP 请求（3.1）：AP 策略 Principal "*" + 数据面白名单，
        // 且仍须通过桶策略匿名基线（AP 绝不放松既有授权）。
        let user = match req.credentials.as_ref() {
            Some(cred) => Some(
                self.acl
                    .user_of_key(&cred.access_key)
                    .await
                    .map_err(|e| {
                        tracing::error!(error = %e, "AP 鉴权查询失败");
                        s3s::s3_error!(InternalError, "internal error")
                    })?
                    .ok_or_else(|| s3s::s3_error!(AccessDenied, "unknown access key"))?,
            ),
            None => None,
        };
        let uid = user.as_ref().map_or(0, |u| u.uid);
        let (name, raw_suffix) = parse_ap_target(&req.uri, &req.headers)?;
        let ap = self.load_ap(&name).await?;
        let peer = req.extensions.get::<ApPeer>().map(|p| p.0);
        // 取 owned：后续 `validate_delete_body(&mut req)` 需要可变借用，不跨借用持有。
        let source_vpc = req.extensions.get::<ApVpc>().map(|v| v.0.clone());
        // 验证器路径：先证明来源，得到身份（registry 查询仍未发生）；失败按 403/500 收口。
        // 未配置验证器 = Slice A：只认 `ApVpce` 断言，行为逐字节不变。
        let (source_vpce, identity, verify_denial) = match &self.verifier {
            None => (
                req.extensions.get::<ApVpce>().map(|v| v.0.clone()),
                None,
                None,
            ),
            Some(verifier) => {
                let ctx = req.extensions.get::<ApEndpointContext>().ok_or_else(|| {
                    tracing::error!(ap = %name, "AP 端点身份上下文缺失（验证器已配置）");
                    s3s::s3_error!(InternalError, "internal error")
                })?;
                match verifier.verify(ctx).await {
                    Ok(identity) => (identity.as_ref().map(|i| i.vpce_id.clone()), identity, None),
                    Err(ApEndpointVerifyError::Unavailable(e)) => {
                        tracing::error!(error = %e, ap = %name, "AP 端点身份验证失败");
                        return Err(s3s::s3_error!(InternalError, "internal error"));
                    }
                    Err(ApEndpointVerifyError::Denied(reason)) => {
                        // 保留合法声明值用于审计；registry 不查（声明本身已不可信）。
                        let vpce = match &ctx.claim {
                            ApEndpointClaim::Header(v) => Some(v.clone()),
                            _ => None,
                        };
                        (vpce, None, Some(reason))
                    }
                }
            }
        };
        // AP 账户根 Principal 解析（成员行优先）与账户状态：签名请求才需要。
        // 查询失败 fail-closed（500），不得当作「无账户」放行。
        let (account_id, account_active) = match &user {
            Some(u) => self
                .acl
                .account_identity_of_user(u.uid)
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, "AP 账户身份查询失败");
                    s3s::s3_error!(InternalError, "internal error")
                })?,
            None => (None, false),
        };
        let account = ApAccount {
            id: account_id.as_deref(),
            active: account_active,
        };
        let proof = ApSourceProof {
            ip: peer,
            vpc: source_vpc.as_deref(),
            vpce: source_vpce.as_deref(),
        };
        // 端点门禁（请求级；DeleteObjects 逐键复评不再查 registry）：
        // 验证器拒绝的声明不查 registry，直接携带拒绝原因进入统一审计/拒绝路径。
        let endpoint = match verify_denial {
            Some(reason) => Some(EndpointCheck {
                vpce: source_vpce.clone().unwrap_or_default(),
                endpoint: None,
                evidence: None,
                denial: Some(reason),
            }),
            None => {
                check_ap_endpoint(
                    self.endpoints.as_ref(),
                    &ap,
                    source_vpc.as_deref(),
                    source_vpce.as_deref(),
                    identity.as_ref(),
                )
                .await?
            }
        };
        // H1：s3s 把任意 multipart/form-data POST 无条件解析为 PostObject（键在表单里，
        // 先于 OIR/子资源路由），AP 不支持该 op。即使带 `?delete` 也仍执行 PostObject，
        // 因此这里必须整类拒绝，而不是按查询推导出的 DeleteObjects 放行。
        if req.method == http::Method::POST
            && req
                .headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(is_multipart_form_data)
        {
            tracing::warn!(uid, ap = %name, "AP 拒绝 multipart/form-data POST");
            return Err(s3s::s3_error!(
                InvalidRequest,
                "multipart/form-data POST is not supported through access points"
            ));
        }
        let canonical = percent_decode_strict(&raw_suffix)?;
        let key: Option<&str> = (!canonical.is_empty()).then_some(canonical.as_str());
        let copy_src: Option<(String, String)> = if req.method == http::Method::PUT {
            copy_source_bucket_key(&req.headers)
        } else {
            None
        };
        let query = req.uri.query().unwrap_or("");
        let op = derive_op(&req.method, key, query, copy_src.is_some())?;
        // H1：x-id 是 s3s OIR 的权威 op 声明（签名含于 SigV4），可越过查询推导；
        // 只允许与推导结果一致，否则授权 op 与执行 op 会分叉。
        if let Some(declared) = query_value_once(query, "x-id")?
            && declared != op
        {
            tracing::warn!(uid, op, declared, ap = %name, "AP x-id 与推导 op 不一致——拒绝");
            return Err(s3s::s3_error!(
                InvalidRequest,
                "x-id does not match the request"
            ));
        }
        let (base_allow, base_detail) = match &user {
            Some(u) => {
                self.acl
                    .check_op(
                        u,
                        &u.ak,
                        op,
                        Some(&ap.bucket),
                        key,
                        copy_src.as_ref().map(|(b, k)| (b.as_str(), k.as_str())),
                    )
                    .await?
            }
            // L1：匿名锁/治理旁路头一律拒（匿名策略无可表达的对应动作）。
            None => match crate::access::anonymous_lock_header(&req.headers) {
                Some(h) => (false, Some(format!("anonymous:object-lock-header:{h}"))),
                None => {
                    self.acl
                        .anonymous_decision(op, Some(&ap.bucket), key)
                        .await?
                }
            },
        };
        // DeleteObjects 的键在请求体里：整体判定必然失真，逐键交集在 service 里做。
        // 无策略 = 默认拒绝（4.4），DeleteObjects 也不例外。
        let ap_policy = if op == "DeleteObjects" {
            ap.policy.is_none().then_some(false)
        } else {
            match &user {
                Some(u) => ap_decision(&ap, u, account, op, key, proof),
                None => ap_decision_anonymous(&ap, op, key, proof),
            }
        };
        let endpoint_ok = endpoint.as_ref().is_none_or(|c| c.denial.is_none());
        let allow = base_allow && ap_policy != Some(false) && endpoint_ok;
        let mut parts: Vec<String> = Vec::new();
        if let Some(d) = base_detail {
            parts.push(d);
        }
        if let Some(v) = ap_policy {
            parts.push(if v { "ap:allow" } else { "ap:deny" }.to_string());
        } else if op == "DeleteObjects" && ap.policy.is_some() {
            parts.push("ap:per-key".to_string());
        }
        if let Some(c) = &endpoint {
            parts.push(match &c.endpoint {
                Some(e) => format!("endpoint={}/account={}", c.vpce, e.account_id),
                None => format!("endpoint={}", c.vpce),
            });
            if let Some(evidence) = c.evidence {
                parts.push(format!("endpoint:evidence={}", evidence.as_str()));
            }
            if let Some(reason) = c.denial {
                parts.push(format!("endpoint:deny:{reason}"));
            }
        }
        let detail = (!parts.is_empty()).then(|| parts.join(";"));
        let audited = if let Some(u) = &user {
            self.acl
                .write_audit(
                    u.uid,
                    op,
                    Some(&ap.bucket),
                    key,
                    if allow { "allow" } else { "deny" },
                    detail.as_deref(),
                )
                .await
        } else if allow {
            self.acl
                .write_audit(0, op, Some(&ap.bucket), key, "allow", detail.as_deref())
                .await
        } else {
            self.acl
                .write_anonymous_deny_audit(op, Some(&ap.bucket), key, detail.as_deref())
                .await
        };
        if let Err(e) = audited {
            tracing::warn!(error = %e, "AP 审计落行失败");
        }
        if !allow {
            tracing::warn!(uid, op, ap = %name, "AP 授权拒绝");
            return Err(s3s::s3_error!(AccessDenied, "access denied"));
        }
        if op == "DeleteObjects" {
            validate_delete_body(req).await?;
            req.extensions.insert(ApDeletePolicy {
                ap: ap.clone(),
                account_id,
                account_active,
                source_ip: peer,
                source_vpc,
                source_vpce,
            });
        }
        match user {
            Some(u) => {
                req.extensions.insert(u);
            }
            None => {
                req.extensions.insert(crate::access::AnonymousAccess);
            }
        }
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
        // 内层没有 access 层：剥离 x-id 后按内层实际可见的输入再推导一次 op，
        // 与授权 op 不一致即 fail-closed（防御查询/头解析漂移）。
        let query = strip_query_key(&strip_signed_query(req.uri.query().unwrap_or("")), "x-id");
        let copy_src = (req.method == http::Method::PUT)
            .then(|| copy_source_bucket_key(&req.headers))
            .flatten();
        let rederived = derive_op(
            &req.method,
            inv.canonical_key.as_deref(),
            &query,
            copy_src.is_some(),
        )
        .map_err(|_| {
            tracing::error!(authorized = inv.op, ap = %inv.ap_name, "AP op 重推导失败——fail-closed");
            s3s::s3_error!(InternalError, "access point operation mismatch")
        })?;
        if rederived != inv.op {
            tracing::error!(
                authorized = inv.op,
                executed = rederived,
                ap = %inv.ap_name,
                "AP 授权 op 与执行 op 不一致——fail-closed"
            );
            return Err(s3s::s3_error!(
                InternalError,
                "access point operation mismatch"
            ));
        }
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
        // 已缓冲 body → BodySizeLimitExceeded；流式 body → http_body_util::Limited
        // 的 LengthLimitError（s3s 不 re-export，两者都要映射为 400 EntityTooLarge）。
        if e.downcast_ref::<s3s::BodySizeLimitExceeded>().is_some()
            || e.downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
        {
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
