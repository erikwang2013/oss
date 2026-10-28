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
//! AP 缓存是进程本地 + TTL 有界；强跨节点一致性仍是 non-goal。
//!
//! 明确不支持（non-goals）：真 VPC 端点（PrivateLink/ENI）、经 AP 的桶级 Create/Delete/
//! ListBuckets（保存策略时即拒绝这些动作）、多用户账户模型、强跨节点 AP 缓存一致性。

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    pub owner_id: i64,
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
        owner_id: i64,
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
            owner_id,
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
    /// Some(reason) = 拒绝。
    denial: Option<&'static str>,
}

/// R2：有 `ApVpce` 断言时 registry 必须有 active 行（查无/停用拒绝）；
/// R3：同时有 `ApVpc` 断言时必须等于 registry.vpc_id（防两证明拼凑）；
/// R4：策略引用 `aws:SourceVpce` 时 endpoint.account_id 必须等于 AP 桶账户；
/// R5：registry 查询失败 → 500（不当作「未登记」放行）。
async fn check_ap_endpoint(
    endpoints: &dyn ApEndpointSource,
    ap: &AccessPoint,
    source_vpc: Option<&str>,
    source_vpce: Option<&str>,
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
        Some(e) if ap.policy_references_source_vpce() && e.account_id != ap.account => {
            Some("account-mismatch")
        }
        Some(_) => None,
    };
    Ok(Some(EndpointCheck {
        vpce: vpce.to_string(),
        endpoint,
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
        let source_vpce = req.extensions.get::<ApVpce>().map(|v| v.0.clone());
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
        // Slice A：端点注册断言门禁（请求级；DeleteObjects 逐键复评不再查 registry）。
        let endpoint = check_ap_endpoint(
            self.endpoints.as_ref(),
            &ap,
            source_vpc.as_deref(),
            source_vpce.as_deref(),
        )
        .await?;
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
            // StringEquals 只支持 aws:SourceVpc/aws:SourceVpce；空列表/畸形 ID/其他键拒
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"StringEquals":{}}}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"StringEquals":{"aws:SourceVpc":[]}}}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"StringEquals":{"aws:SourceVpc":"not-a-vpc"}}}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"StringEquals":{"aws:SourceVpce":"vpc-abc"}}}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"StringEquals":{"aws:SourceVpce":[]}}}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:x","Condition":{"StringEquals":{"aws:SourceVpce":"not-a-vpce"}}}]}"#,
            // 桶级创建/删除/列举动作经 AP 无法执行：保存即拒（精确动作；s3:* 通配仍允许）
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:CreateBucket","Resource":"arn:x"}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:DeleteBucket"],"Resource":"arn:x"}]}"#,
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:ListAllMyBuckets","Resource":"arn:x"}]}"#,
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
        // 通配与受支持动作不受影响
        assert!(
            parse(
                r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*",
                "Resource":"arn:aws:s3:::bkt/*"}]}"#
            )
            .is_ok()
        );
        assert!(
            parse(
                r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
                "Resource":"arn:x","Condition":{"StringEquals":{"aws:SourceVpc":["vpc-abc123","vpce-x"]}}}]}"#
            )
            .is_ok()
        );
        assert!(
            parse(
                r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
                "Resource":"arn:x","Condition":{"StringEquals":{"aws:SourceVpce":"vpce-1"}}}]}"#
            )
            .is_ok()
        );
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
            derive_op(&Method::POST, Some("k"), "uploads", false).unwrap(),
            "CreateMultipartUpload"
        );
        // s3s 对 POST /{bucket}?uploads 解析为未知操作 → 501 与内层一致
        assert!(derive_op(&Method::POST, None, "uploads", false).is_err());
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
        // 对象级 POST ?delete 内层解析不出 DeleteObjects（键在桶级请求体）
        assert!(derive_op(&Method::POST, Some("k"), "delete", false).is_err());
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
            account: ApAccount::default(),
            anonymous: false,
            resources: vec![format!("arn:aws:s3:::bkt/{key}")],
            source: ApSourceProof::default(),
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
                    account: ApAccount::default(),
                    anonymous: false,
                    resources: vec![format!("arn:aws:s3:::bkt/{key}")],
                    source: ApSourceProof {
                        ip: ip.parse().ok(),
                        ..Default::default()
                    },
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
        assert_eq!(
            eval("secret/x", ""),
            Some(false),
            "缺对端时 Deny+NotIpAddress 必须命中（fail-closed）"
        );
    }

    /// `StringEquals.aws:SourceVpc`：证明匹配才成立；缺证明 fail-closed（Deny 命中、
    /// Allow 不命中）；与 IP 条件是 AND。
    #[test]
    fn source_vpc_condition_matches_and_fails_closed() {
        let p = parse(
            r#"{"Statement":[
              {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*",
               "Condition":{"StringEquals":{"aws:SourceVpc":["vpc-abc","vpce-xyz"]}}},
              {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*",
               "Condition":{"StringEquals":{"aws:SourceVpc":"vpc-abc"}}}
            ]}"#,
        )
        .unwrap();
        let eval = |key: &str, vpc: Option<&str>| {
            p.evaluate(
                &ApEvalRequest {
                    ak: "AKIA_X",
                    uid: 7,
                    account: ApAccount::default(),
                    anonymous: false,
                    resources: vec![format!("arn:aws:s3:::bkt/{key}")],
                    source: ApSourceProof {
                        vpc,
                        ..Default::default()
                    },
                },
                "s3:GetObject",
            )
        };
        assert_eq!(eval("open", Some("vpc-abc")), Some(true));
        assert_eq!(eval("open", Some("vpce-xyz")), Some(true));
        assert_eq!(
            eval("open", Some("vpc-other")),
            None,
            "不匹配 → Allow 不命中"
        );
        assert_eq!(
            eval("open", None),
            None,
            "缺证明 → Allow 不命中（fail-closed）"
        );
        assert_eq!(eval("secret/x", Some("vpc-abc")), Some(false), "Deny 命中");
        assert_eq!(
            eval("secret/x", Some("vpc-other")),
            None,
            "Deny 不命中，但 Allow 的 VPC 条件也不满足 → 无匹配"
        );
        // 缺证明时 Deny 条件无法求值 → 命中（fail-closed），绝不静默放过
        assert_eq!(eval("secret/x", None), Some(false));
        // IP 与 VPC 条件 AND：缺 IP 时整体无法求值
        let both = parse(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
               "Resource":"arn:aws:s3:::bkt/*","Condition":{
                 "IpAddress":{"aws:SourceIp":"10.0.0.0/8"},
                 "StringEquals":{"aws:SourceVpc":"vpc-abc"}}}]}"#,
        )
        .unwrap();
        let eval_both = |ip: Option<&str>, vpc: Option<&str>| {
            both.evaluate(
                &ApEvalRequest {
                    ak: "AKIA_X",
                    uid: 7,
                    account: ApAccount::default(),
                    anonymous: false,
                    resources: vec!["arn:aws:s3:::bkt/k".to_string()],
                    source: ApSourceProof {
                        ip: ip.and_then(|i| i.parse().ok()),
                        vpc,
                        vpce: None,
                    },
                },
                "s3:GetObject",
            )
        };
        assert_eq!(eval_both(Some("10.1.2.3"), Some("vpc-abc")), Some(true));
        assert_eq!(eval_both(Some("10.1.2.3"), None), None);
        assert_eq!(eval_both(None, Some("vpc-abc")), None);
        assert_eq!(eval_both(Some("192.168.1.1"), Some("vpc-abc")), None);
    }

    /// `StringEquals.aws:SourceVpce`：独立证明（绝不由 SourceVpc 推断）；
    /// 缺证明 fail-closed（Deny 命中、Allow 不命中）；与 SourceVpc 条件是 AND。
    #[test]
    fn source_vpce_condition_matches_and_fails_closed() {
        let p = parse(
            r#"{"Statement":[
              {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*",
               "Condition":{"StringEquals":{"aws:SourceVpce":["vpce-abc","vpce-def"]}}},
              {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*",
               "Condition":{"StringEquals":{"aws:SourceVpce":"vpce-abc"}}}
            ]}"#,
        )
        .unwrap();
        let eval = |key: &str, vpce: Option<&str>| {
            p.evaluate(
                &ApEvalRequest {
                    ak: "AKIA_X",
                    uid: 7,
                    account: ApAccount::default(),
                    anonymous: false,
                    resources: vec![format!("arn:aws:s3:::bkt/{key}")],
                    source: ApSourceProof {
                        vpce,
                        ..Default::default()
                    },
                },
                "s3:GetObject",
            )
        };
        assert_eq!(eval("open", Some("vpce-abc")), Some(true));
        assert_eq!(eval("open", Some("vpce-def")), Some(true));
        assert_eq!(
            eval("open", Some("vpce-other")),
            None,
            "不匹配 → Allow 不命中"
        );
        assert_eq!(
            eval("open", None),
            None,
            "缺证明 → Allow 不命中（fail-closed）"
        );
        assert_eq!(eval("secret/x", Some("vpce-abc")), Some(false), "Deny 命中");
        // 缺证明时 Deny 条件无法求值 → 命中（fail-closed）
        assert_eq!(eval("secret/x", None), Some(false));
        // SourceVpc 证明不得充当 SourceVpce 证明
        let vpc_only = ApSourceProof {
            vpc: Some("vpce-abc"),
            ..Default::default()
        };
        assert_eq!(
            p.evaluate(
                &ApEvalRequest {
                    ak: "AKIA_X",
                    uid: 7,
                    account: ApAccount::default(),
                    anonymous: false,
                    resources: vec!["arn:aws:s3:::bkt/open".to_string()],
                    source: vpc_only,
                },
                "s3:GetObject",
            ),
            None,
            "SourceVpc 头值即使同形也不得满足 SourceVpce 条件"
        );
        // 两键 AND：任一证明缺失 → 整体无法求值
        let both = parse(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject",
               "Resource":"arn:aws:s3:::bkt/*","Condition":{
                 "StringEquals":{"aws:SourceVpc":"vpc-abc","aws:SourceVpce":"vpce-abc"}}}]}"#,
        )
        .unwrap();
        let eval_both = |vpc: Option<&str>, vpce: Option<&str>| {
            both.evaluate(
                &ApEvalRequest {
                    ak: "AKIA_X",
                    uid: 7,
                    account: ApAccount::default(),
                    anonymous: false,
                    resources: vec!["arn:aws:s3:::bkt/k".to_string()],
                    source: ApSourceProof {
                        vpc,
                        vpce,
                        ..Default::default()
                    },
                },
                "s3:GetObject",
            )
        };
        assert_eq!(eval_both(Some("vpc-abc"), Some("vpce-abc")), Some(true));
        assert_eq!(eval_both(Some("vpc-abc"), None), None);
        assert_eq!(eval_both(None, Some("vpce-abc")), None);
        assert_eq!(eval_both(Some("vpc-abc"), Some("vpce-other")), None);
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
        ap_acl_pool().await.0
    }

    /// 同上，额外返回 pool（审计 detail 断言用）。
    async fn ap_acl_pool() -> (crate::access::Acl, sqlx::SqlitePool) {
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
             CREATE TABLE accounts (id TEXT PRIMARY KEY, name TEXT NOT NULL, \
              status TEXT NOT NULL DEFAULT 'active', admin_uid INTEGER, bucket_quota INTEGER, \
              created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);\
             CREATE TABLE account_members (account_id TEXT NOT NULL, uid INTEGER NOT NULL UNIQUE, \
              role TEXT NOT NULL DEFAULT 'member', created_at INTEGER NOT NULL, \
              PRIMARY KEY (account_id, uid));\
             INSERT INTO users (id,username,password_hash,is_admin) VALUES (7,'u','h',0);\
             INSERT INTO access_keys (access_key,secret_key,user_id) VALUES ('AKIA_X','s',7);\
             INSERT INTO buckets (name,owner_id,creation_date) VALUES ('bkt',7,0);",
        )
        .execute(&pool)
        .await
        .unwrap();
        (crate::access::Acl::new(pool.clone()), pool)
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
            account: crate::access::LEGACY_ACCOUNT.into(),
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
            Arc::new(StaticApEndpointSource::new([])),
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

    /// 测试用可变 registry：按用例改行，并可选注入查询错误（R5）。
    #[derive(Default)]
    struct TestEndpoints {
        rows: std::sync::Mutex<Vec<ApEndpoint>>,
        fail: std::sync::atomic::AtomicBool,
    }

    impl TestEndpoints {
        fn replace(&self, rows: Vec<ApEndpoint>) {
            *self.rows.lock().unwrap() = rows;
        }
        fn fail(&self, fail: bool) {
            self.fail.store(fail, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl ApEndpointSource for TestEndpoints {
        async fn lookup(&self, vpce: &str) -> Result<Option<ApEndpoint>, String> {
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err("registry down".into());
            }
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|e| e.vpce_id == vpce)
                .cloned())
        }
    }

    fn endpoint(state: &str, vpc: &str, account: &str) -> ApEndpoint {
        ApEndpoint {
            vpce_id: "vpce-abc".into(),
            vpc_id: vpc.into(),
            account_id: account.into(),
            state: state.into(),
        }
    }

    fn vpce_route(
        acl: crate::access::Acl,
        ap: AccessPoint,
        endpoints: Arc<dyn ApEndpointSource>,
    ) -> ApRoute {
        struct NoSource;
        #[async_trait::async_trait]
        impl ApSource for NoSource {
            async fn lookup(&self, _n: &str) -> Result<Option<AccessPoint>, String> {
                Ok(None)
            }
        }
        let cache = Arc::new(ApCache::new());
        cache.put(ap);
        let inner = Arc::new(s3s::service::S3ServiceBuilder::new(DummyS3).build());
        ApRoute::new(
            cache,
            Arc::new(NoSource),
            endpoints,
            acl,
            Arc::new(crate::olap::OlapCache::new()),
            inner,
        )
    }

    fn vpce_req(
        method: http::Method,
        path: &str,
        body: &str,
        vpc: Option<&str>,
        vpce: Option<&str>,
    ) -> S3Request<s3s::Body> {
        let mut r = S3Request {
            input: s3s::Body::from(body.to_string()),
            method,
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
        if let Some(v) = vpc {
            r.extensions.insert(ApVpc(v.into()));
        }
        if let Some(v) = vpce {
            r.extensions.insert(ApVpce(v.into()));
        }
        r
    }

    async fn last_audit_detail(pool: &sqlx::SqlitePool) -> Option<String> {
        sqlx::query_scalar("SELECT detail FROM audit_log ORDER BY id DESC LIMIT 1")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    fn vpce_ap(name: &str, policy: &str) -> AccessPoint {
        AccessPoint {
            name: name.into(),
            bucket: "bkt".into(),
            owner_id: 7,
            account: crate::access::LEGACY_ACCOUNT.into(),
            policy: Some(serde_json::from_str(policy).unwrap()),
            region: "us-east-1".into(),
        }
    }

    /// Slice A：registry 门禁 R1–R7。
    /// 注意这是**注册断言**校验，不证明连接真来自该 PrivateLink 端点。
    #[tokio::test]
    async fn ap_endpoint_registry_gate_fails_closed() {
        use s3s::S3ErrorCode as C;
        use s3s::route::S3Route as _;
        let (acl, pool) = ap_acl_pool().await;
        let registry = Arc::new(TestEndpoints::default());
        // 条件放行：策略引用 SourceVpce（R4 生效）。
        let ap1 = vpce_ap(
            "ap1",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
               "Action":["s3:GetObject","s3:DeleteObject"],"Resource":"arn:aws:s3:::bkt/*",
               "Condition":{"StringEquals":{"aws:SourceVpce":"vpce-abc"}}}]}"#,
        );
        let route = vpce_route(acl.clone(), ap1.clone(), registry.clone());
        // 不引用 SourceVpce 的策略：归属账户不参与（R4 不强制）。
        let plain_ap = vpce_ap(
            "ap2",
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
               "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}"#,
        );
        let plain = vpce_route(acl, plain_ap, registry.clone());

        // R2 正向：active + 账户匹配 → 放行；审计带 endpoint/account。
        registry.replace(vec![endpoint("active", "vpc-1", "000000000000")]);
        let mut req = vpce_req(http::Method::GET, "/ap/ap1/k", "", None, Some("vpce-abc"));
        route.check_access(&mut req).await.unwrap();
        let detail = last_audit_detail(&pool).await.unwrap();
        assert!(
            detail.contains("endpoint=vpce-abc/account=000000000000"),
            "审计 detail 缺端点信息: {detail}"
        );

        // R2：字符串匹配但未登记 → 拒。
        registry.replace(vec![]);
        let mut req = vpce_req(http::Method::GET, "/ap/ap1/k", "", None, Some("vpce-abc"));
        assert_eq!(
            route.check_access(&mut req).await.unwrap_err().code(),
            &C::AccessDenied
        );
        let detail = last_audit_detail(&pool).await.unwrap();
        assert!(
            detail.contains("endpoint:deny:unregistered"),
            "未登记拒绝须可追溯: {detail}"
        );

        // R2：停用 → 拒。
        registry.replace(vec![endpoint("disabled", "vpc-1", "000000000000")]);
        let mut req = vpce_req(http::Method::GET, "/ap/ap1/k", "", None, Some("vpce-abc"));
        assert_eq!(
            route.check_access(&mut req).await.unwrap_err().code(),
            &C::AccessDenied
        );

        // R3：双证明必须同 VPC（两证明拼凑拒绝）。
        registry.replace(vec![endpoint("active", "vpc-1", "000000000000")]);
        let mut req = vpce_req(
            http::Method::GET,
            "/ap/ap1/k",
            "",
            Some("vpc-2"),
            Some("vpce-abc"),
        );
        assert_eq!(
            route.check_access(&mut req).await.unwrap_err().code(),
            &C::AccessDenied
        );
        // VPC 证明一致 → 放行。
        let mut req = vpce_req(
            http::Method::GET,
            "/ap/ap1/k",
            "",
            Some("vpc-1"),
            Some("vpce-abc"),
        );
        route.check_access(&mut req).await.unwrap();

        // R4：策略引用 SourceVpce 时端点账户必须等于 AP 桶账户。
        registry.replace(vec![endpoint("active", "vpc-1", "123456789012")]);
        let mut req = vpce_req(http::Method::GET, "/ap/ap1/k", "", None, Some("vpce-abc"));
        assert_eq!(
            route.check_access(&mut req).await.unwrap_err().code(),
            &C::AccessDenied
        );
        // 策略不引用 SourceVpce：账户不参与（但 active + VPC 仍须成立）。
        let mut req = vpce_req(http::Method::GET, "/ap/ap2/k", "", None, Some("vpce-abc"));
        plain.check_access(&mut req).await.unwrap();
        // R3 与策略无关：不引用 vpce 也拒 VPC 拼凑。
        let mut req = vpce_req(
            http::Method::GET,
            "/ap/ap2/k",
            "",
            Some("vpc-2"),
            Some("vpce-abc"),
        );
        assert_eq!(
            plain.check_access(&mut req).await.unwrap_err().code(),
            &C::AccessDenied
        );
        // R2 与策略无关：未登记仍拒。
        registry.replace(vec![]);
        let mut req = vpce_req(http::Method::GET, "/ap/ap2/k", "", None, Some("vpce-abc"));
        assert_eq!(
            plain.check_access(&mut req).await.unwrap_err().code(),
            &C::AccessDenied
        );

        // R5：查询失败 → 500，不当作「无端点」放行。
        registry.replace(vec![endpoint("active", "vpc-1", "000000000000")]);
        registry.fail(true);
        let mut req = vpce_req(http::Method::GET, "/ap/ap1/k", "", None, Some("vpce-abc"));
        assert_eq!(
            route.check_access(&mut req).await.unwrap_err().code(),
            &C::InternalError
        );
        registry.fail(false);

        // R6：无断言 → 行为不变（不查 registry，即使 registry 报错）。
        registry.fail(true);
        let mut req = vpce_req(http::Method::GET, "/ap/ap2/k", "", None, None);
        plain.check_access(&mut req).await.unwrap();
        registry.fail(false);

        // R7：DeleteObjects 请求级门禁；通过后携带证明供逐键复评（不再查 registry）。
        registry.replace(vec![endpoint("disabled", "vpc-1", "000000000000")]);
        let mut req = vpce_req(
            http::Method::POST,
            "/ap/ap1/?delete",
            "<Delete><Object><Key>k</Key></Object></Delete>",
            None,
            Some("vpce-abc"),
        );
        assert_eq!(
            route.check_access(&mut req).await.unwrap_err().code(),
            &C::AccessDenied
        );
        assert!(req.extensions.get::<ApDeletePolicy>().is_none());
        registry.replace(vec![endpoint("active", "vpc-1", "000000000000")]);
        let mut req = vpce_req(
            http::Method::POST,
            "/ap/ap1/?delete",
            "<Delete><Object><Key>k</Key></Object></Delete>",
            None,
            Some("vpce-abc"),
        );
        route.check_access(&mut req).await.unwrap();
        let p = req.extensions.get::<ApDeletePolicy>().unwrap();
        assert_eq!(p.source_vpce.as_deref(), Some("vpce-abc"));
        // 逐键复评只读请求级证明，不再查 registry（R7）。
        registry.fail(true);
        let user = crate::access::AuthedUser {
            uid: 7,
            ak: "AKIA_X".into(),
            is_admin: false,
        };
        assert_eq!(
            ap_decision(
                &p.ap,
                &user,
                ApAccount {
                    id: Some("000000000007"),
                    active: true
                },
                "DeleteObjects",
                Some("k"),
                ApSourceProof {
                    ip: None,
                    vpc: p.source_vpc.as_deref(),
                    vpce: p.source_vpce.as_deref()
                },
            ),
            Some(true)
        );
    }

    /// H1：AP 授权 op 必须等于内层执行 op（legal-hold/retention/x-id/multipart POST）。
    #[tokio::test]
    async fn ap_route_rejects_op_mismatch_and_checks_legal_hold_action() {
        use s3s::route::S3Route as _;
        let acl = ap_acl().await;
        let cache = Arc::new(ApCache::new());
        let ap = |name: &str, actions: &str| {
            let policy: ApPolicy = serde_json::from_str(&format!(
                r#"{{"Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},
                   "Action":[{actions}],"Resource":"arn:aws:s3:::bkt/*"}}]}}"#
            ))
            .unwrap();
            AccessPoint {
                name: name.into(),
                bucket: "bkt".into(),
                owner_id: 7,
                account: crate::access::LEGACY_ACCOUNT.into(),
                policy: Some(policy),
                region: "us-east-1".into(),
            }
        };
        cache.put(ap("putonly", r#""s3:PutObject","s3:GetObject""#));
        cache.put(ap(
            "full",
            r#""s3:PutObject","s3:GetObject","s3:PutObjectLegalHold",
               "s3:GetObjectLegalHold","s3:PutObjectRetention","s3:GetObjectRetention""#,
        ));
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
            Arc::new(StaticApEndpointSource::new([])),
            acl,
            Arc::new(crate::olap::OlapCache::new()),
            inner,
        );
        let mk = |method: http::Method, path: &str, headers: http::HeaderMap, body: &str| {
            let mut r = S3Request {
                input: s3s::Body::from(body.to_string()),
                method,
                uri: path.parse().unwrap(),
                headers,
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
        let code = |r: S3Result<()>| r.err().map(|e| e.code().clone());
        use s3s::S3ErrorCode as C;

        // 策略缺少动作 → 拒（直接与经 AP 两条路径都由 classify+s3_action 收口）
        for (m, q) in [
            (http::Method::PUT, "legal-hold"),
            (http::Method::GET, "legal-hold"),
            (http::Method::PUT, "retention"),
            (http::Method::GET, "retention"),
        ] {
            let path = format!("/ap/putonly/k?{q}");
            let mut req = mk(m, &path, http::HeaderMap::new(), "");
            assert_eq!(
                code(route.check_access(&mut req).await),
                Some(C::AccessDenied),
                "{path}"
            );
        }
        // 策略含动作 → 放行且 op 精确
        let cases = [
            (
                http::Method::PUT,
                "/ap/full/k?legal-hold&x-id=PutObjectLegalHold",
                "PutObjectLegalHold",
            ),
            (
                http::Method::GET,
                "/ap/full/k?legal-hold&versionId=v1",
                "GetObjectLegalHold",
            ),
            (
                http::Method::PUT,
                "/ap/full/k?retention",
                "PutObjectRetention",
            ),
        ];
        for (m, path, want) in cases {
            let mut req = mk(m, path, http::HeaderMap::new(), "");
            route.check_access(&mut req).await.unwrap();
            assert_eq!(req.extensions.get::<ApInvocation>().unwrap().op, want);
        }
        // x-id 与推导不一致 / 重复 → 400（不再进入策略判定）
        for path in [
            "/ap/full/k?x-id=PutObjectLegalHold",
            "/ap/full/k?legal-hold&x-id=PutObject",
            "/ap/full/k?x-id=PutObject&x-id=PutObject",
        ] {
            let mut req = mk(http::Method::PUT, path, http::HeaderMap::new(), "");
            assert_eq!(
                code(route.check_access(&mut req).await),
                Some(C::InvalidRequest),
                "{path}"
            );
        }
        // multipart/form-data POST：内层会执行 PostObject → 整类拒绝（含 ?delete）
        for path in ["/ap/full?delete", "/ap/full/k?uploads"] {
            let mut h = http::HeaderMap::new();
            h.insert(
                http::header::CONTENT_TYPE,
                "multipart/form-data; boundary=x".parse().unwrap(),
            );
            let mut req = mk(http::Method::POST, path, h, "--x--");
            assert_eq!(
                code(route.check_access(&mut req).await),
                Some(C::InvalidRequest),
                "{path}"
            );
        }
        // 正常 xml DeleteObjects 不变；未知查询形状 501
        let mut h = http::HeaderMap::new();
        h.insert(
            http::header::CONTENT_TYPE,
            "application/xml".parse().unwrap(),
        );
        let body = "<Delete><Object><Key>a</Key></Object></Delete>";
        let mut req = mk(http::Method::POST, "/ap/full?delete", h, body);
        route.check_access(&mut req).await.unwrap();
        assert_eq!(
            req.extensions.get::<ApInvocation>().unwrap().op,
            "DeleteObjects"
        );
        let mut req = mk(
            http::Method::PUT,
            "/ap/full/k?tagging&retention",
            http::HeaderMap::new(),
            "",
        );
        assert_eq!(
            code(route.check_access(&mut req).await),
            Some(C::NotImplemented)
        );
        // 普通对象读写/复制不变
        let mut req = mk(
            http::Method::PUT,
            "/ap/full/k",
            http::HeaderMap::new(),
            "body",
        );
        route.check_access(&mut req).await.unwrap();
        assert_eq!(
            req.extensions.get::<ApInvocation>().unwrap().op,
            "PutObject"
        );
        let mut h = http::HeaderMap::new();
        h.insert(s3s::header::X_AMZ_COPY_SOURCE, "/bkt/src".parse().unwrap());
        let mut req = mk(http::Method::PUT, "/ap/full/k", h, "");
        route.check_access(&mut req).await.unwrap();
        assert_eq!(
            req.extensions.get::<ApInvocation>().unwrap().op,
            "CopyObject"
        );
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
        assert_eq!(
            derive_op(&Method::GET, Some("k"), "legal-hold", false).unwrap(),
            "GetObjectLegalHold"
        );
        assert_eq!(
            derive_op(&Method::PUT, Some("k"), "legal-hold&versionId=v1", false).unwrap(),
            "PutObjectLegalHold"
        );
        assert!(
            derive_op(&Method::GET, None, "acl", false).is_err(),
            "未知桶级子资源仍 fail-closed"
        );
        // H1：未列入白名单的子资源即使内层有同名 op 也必须拒（否则授权 op 分叉）
        for (m, k, q) in [
            (Method::PUT, Some("k"), "acl"),
            (Method::GET, Some("k"), "acl"),
            (Method::GET, Some("k"), "torrent"),
            (Method::GET, None, "tagging"),
            (Method::PUT, None, "tagging"),
            (Method::DELETE, None, "tagging"),
            // 多子资源：内层按位优先级选 op，无法保证与推导一致
            (Method::GET, Some("k"), "tagging&retention"),
            (Method::PUT, Some("k"), "retention&legal-hold"),
            // 对象级 POST 的 uploadId 分支不能再落回 PutObject 一类的默认臂
            (Method::PUT, Some("k"), "uploadId=u"),
        ] {
            assert!(
                derive_op(&m, k, q, false).is_err(),
                "{m} {k:?} ?{q} 必须拒绝"
            );
        }
        // 客户端转发 x-id 属于合法形状（值由 check_access 校验）
        assert_eq!(
            derive_op(
                &Method::GET,
                Some("k"),
                "x-id=GetObject&versionId=v1",
                false
            )
            .unwrap(),
            "GetObject"
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
            derive_op(&Method::POST, Some("k"), "uploads", false).unwrap(),
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
            account: crate::access::LEGACY_ACCOUNT.into(),
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
        let acct = ApAccount {
            id: Some("000000000007"),
            active: true,
        };
        let none = ApSourceProof::default();
        assert_eq!(
            ap_decision(&ap, &u, acct, "GetObject", Some("open"), none),
            Some(true)
        );
        assert_eq!(
            ap_decision(&ap, &u, acct, "GetObject", Some("secret/x"), none),
            Some(false)
        );
        // 无匹配 action → 拒（有策略 = 默认收窄）
        assert_eq!(
            ap_decision(&ap, &u, acct, "PutObject", Some("open"), none),
            Some(false)
        );
        // L3：无策略 = 默认拒绝（AWS 语义）
        let no_policy = AccessPoint { policy: None, ..ap };
        assert_eq!(
            ap_decision(&no_policy, &u, acct, "PutObject", Some("open"), none),
            Some(false)
        );
    }

    #[test]
    fn anonymous_ap_decision_only_matches_wildcard_principal() {
        let mk = |principal: &str| AccessPoint {
            name: "ap1".into(),
            bucket: "bkt".into(),
            owner_id: 7,
            account: crate::access::LEGACY_ACCOUNT.into(),
            policy: Some(
                parse(&format!(
                    r#"{{"Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},
                       "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}}]}}"#
                ))
                .unwrap(),
            ),
            region: "us-east-1".into(),
        };
        let none = ApSourceProof::default();
        assert_eq!(
            ap_decision_anonymous(&mk("*"), "GetObject", Some("open"), none),
            Some(true)
        );
        // 空 AK / uid:0 是匿名哨兵，不能被具体 Principal 放行
        for p in ["", "uid:0", "AKIA_X"] {
            assert_eq!(
                ap_decision_anonymous(&mk(p), "GetObject", Some("open"), none),
                Some(false),
                "principal {p:?} 不得匹配匿名请求"
            );
        }
    }

    #[test]
    fn cache_put_remove_and_bucket_invalidate() {
        let c = ApCache::new();
        let mk = |name: &str, bucket: &str| AccessPoint {
            name: name.into(),
            bucket: bucket.into(),
            owner_id: 1,
            account: crate::access::LEGACY_ACCOUNT.into(),
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

    /// 缓存 TTL/负缓存/上限：过期重新查 DB（此处表现为 get=None）、create 清负缓存、
    /// 满员保可用性（不缓存新条目）、replace_all 清负缓存。
    #[test]
    fn cache_ttl_negative_and_bounds() {
        let mk = |name: &str, bucket: &str| AccessPoint {
            name: name.into(),
            bucket: bucket.into(),
            owner_id: 1,
            account: crate::access::LEGACY_ACCOUNT.into(),
            policy: None,
            region: "us-east-1".into(),
        };
        let fresh = Duration::from_secs(60);
        let c = ApCache::with_config(fresh, fresh, 10);
        c.put(mk("a", "b"));
        assert!(c.get("a").is_some());
        assert!(!c.negative_hit("a"));
        // 负缓存：未知 AP 命中；put（创建）后立刻清除
        c.put_negative("ghost");
        assert!(c.negative_hit("ghost") && c.get("ghost").is_none());
        assert!(!c.negative_hit("other"));
        c.put_negative("new");
        c.put(mk("new", "b"));
        assert!(!c.negative_hit("new") && c.get("new").is_some());
        // replace_all 清负缓存
        c.put_negative("stale");
        c.replace_all([mk("x", "y")]);
        assert!(!c.negative_hit("stale"));
        assert_eq!(c.len(), 1);
        // TTL=0 = 不缓存正条目；负 TTL=0 = 关闭负缓存
        let none = ApCache::with_config(Duration::ZERO, Duration::ZERO, 10);
        none.put(mk("a", "b"));
        none.put_negative("ghost");
        assert!(none.get("a").is_none() && !none.negative_hit("ghost"));
        // 短 TTL 到点后过期（不等待生产默认 30s）
        let short = ApCache::with_config(Duration::from_millis(1), Duration::from_millis(1), 10);
        short.put(mk("a", "b"));
        short.put_negative("ghost");
        assert!(short.get("a").is_some() && short.negative_hit("ghost"));
        std::thread::sleep(Duration::from_millis(10));
        assert!(short.get("a").is_none(), "TTL 到期后必须重查");
        assert!(!short.negative_hit("ghost"), "负缓存 TTL 到期");
        // 上限：2 个新鲜条目已满 → 第 3 个不缓存（直接查 DB），已有条目仍可用
        let bound = ApCache::with_config(fresh, fresh, 2);
        bound.put(mk("a", "b"));
        bound.put(mk("b", "b"));
        bound.put(mk("c", "b"));
        assert_eq!(bound.len(), 2);
        assert!(bound.get("a").is_some() && bound.get("c").is_none());
    }

    #[test]
    fn arn_shapes() {
        let c = ApContext::new("ap1", "eu-west-1", "000000000007");
        assert_eq!(
            c.object_arn("a/b"),
            "arn:aws:s3:eu-west-1:000000000007:accesspoint/ap1/object/a/b"
        );
        assert_eq!(
            c.bucket_arn(),
            "arn:aws:s3:eu-west-1:000000000007:accesspoint/ap1"
        );
        // 旧账户段别名（一版兼容）
        let l = ApContext::legacy("ap1", "eu-west-1");
        assert_eq!(
            l.bucket_arn(),
            "arn:aws:s3:eu-west-1:000000000000:accesspoint/ap1"
        );
    }

    /// 账户命名空间 v1（AP 侧）：新 ARN 账户 = 桶账户；旧 LEGACY ARN 仍作资源别名；
    /// Principal 接受 `arn:aws:iam::<account>:root`，LEGACY/其他账户不命中。
    #[test]
    fn account_namespace_arns_and_principals() {
        use crate::access::AuthedUser;
        let mk = |account: &str, principal: &str, resource: &str| AccessPoint {
            name: "ap1".into(),
            bucket: "bkt".into(),
            owner_id: 7,
            account: account.into(),
            policy: Some(
                parse(&format!(
                    r#"{{"Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},
                       "Action":"s3:GetObject","Resource":"{resource}"}}]}}"#
                ))
                .unwrap(),
            ),
            region: "us-east-1".into(),
        };
        let u = AuthedUser {
            ak: "AKIA_X".into(),
            uid: 7,
            is_admin: false,
        };
        let none = ApSourceProof::default();
        let own = ApAccount {
            id: Some("000000000007"),
            active: true,
        };
        // 新账户 AP ARN 可匹配
        assert_eq!(
            ap_decision(
                &mk(
                    "000000000007",
                    "*",
                    "arn:aws:s3:us-east-1:000000000007:accesspoint/ap1/object/*"
                ),
                &u,
                own,
                "GetObject",
                Some("k"),
                none
            ),
            Some(true)
        );
        // 旧 LEGACY ARN 仍是资源别名（已保存的旧策略继续匹配）
        assert_eq!(
            ap_decision(
                &mk(
                    "000000000007",
                    "*",
                    "arn:aws:s3:us-east-1:000000000000:accesspoint/ap1/object/*"
                ),
                &u,
                own,
                "GetObject",
                Some("k"),
                none
            ),
            Some(true)
        );
        // Principal 账户 ARN：同账户命中，LEGACY/他账户不命中
        for (principal, want) in [
            ("arn:aws:iam::000000000007:root", Some(true)),
            ("arn:aws:iam::000000000000:root", Some(false)),
            ("arn:aws:iam::000000000008:root", Some(false)),
        ] {
            assert_eq!(
                ap_decision(
                    &mk("000000000007", principal, "arn:aws:s3:::bkt/*"),
                    &u,
                    own,
                    "GetObject",
                    Some("k"),
                    none
                ),
                want,
                "principal {principal}"
            );
        }
        // 跨账户：AP 属于桶主账户 000000000007，请求者成员关系在 123456789012。
        // 账户根 Principal 按请求者当前账户（成员行）命中。
        let cross = ApAccount {
            id: Some("123456789012"),
            active: true,
        };
        assert_eq!(
            ap_decision(
                &mk(
                    "000000000007",
                    "arn:aws:iam::123456789012:root",
                    "arn:aws:s3:::bkt/*"
                ),
                &u,
                cross,
                "GetObject",
                Some("k"),
                none
            ),
            Some(true)
        );
        // 派生账户不再命中（成员关系优先）
        assert_eq!(
            ap_decision(
                &mk(
                    "000000000007",
                    "arn:aws:iam::000000000007:root",
                    "arn:aws:s3:::bkt/*"
                ),
                &u,
                cross,
                "GetObject",
                Some("k"),
                none
            ),
            Some(false)
        );
        // 未知账户 Principal 不命中
        assert_eq!(
            ap_decision(
                &mk(
                    "000000000007",
                    "arn:aws:iam::999999999999:root",
                    "arn:aws:s3:::bkt/*"
                ),
                &u,
                cross,
                "GetObject",
                Some("k"),
                none
            ),
            Some(false)
        );
        // 账户停用 fail-closed：即使 Principal 匹配也拒（认证层之外的兜底）
        let disabled = ApAccount {
            id: Some("123456789012"),
            active: false,
        };
        assert_eq!(
            ap_decision(
                &mk(
                    "000000000007",
                    "arn:aws:iam::123456789012:root",
                    "arn:aws:s3:::bkt/*"
                ),
                &u,
                disabled,
                "GetObject",
                Some("k"),
                none
            ),
            Some(false)
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
