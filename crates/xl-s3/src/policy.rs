//! 桶策略（M4 切片 7）：AWS 形状的**子集** + 判定（显式 Deny 优先）。
//!
//! 支持的子集（超出范围的值一律解析失败 → console PUT 400，不静默忽略）：
//! - `Statement[]`：`Effect`（`Allow`/`Deny`，且 `Principal.AWS`/`Action`/`Resource` 均非空
//!   ——`PolicyDoc::validate` 在 PUT 层拦，拼错单词的 Deny 不会静默空转）、`Principal.AWS`
//!   （`"*"` | AK | `"uid:<n>"` | `arn:aws:iam::<12位账户>:root`，字符串或数组）、`Action`
//!   （`s3:GetObject` 精确 / `s3:*` / `s3:Get*` 前缀通配，字符串或数组）、`Resource`
//!   （`arn:aws:s3:::<bucket>` 桶级 / `arn:aws:s3:::<bucket>/<prefix>*` 对象级，字符串或
//!   数组；**按 op 类别单轴匹配**——桶级 op 只见桶 ARN，对象 op 只见对象 ARN，与 AWS 一致）。
//! - 判定：任一匹配的 **Deny** → 拒（最高优先）；否则任一匹配的 **Allow** → 放行；
//!   无匹配 → `None`（调用方回落既有 permissions 三档）。
//! - 匿名（无凭据）：`PolicyRequest.anonymous` 下**仅** `Principal` 恰为 `"*"` 的语句参与
//!   （`uid:<n>`/具体 AK/账户 ARN/空 AK 一律不匹配）；放行面另行由 `access.rs` 限定为数据面对象操作。
//!
//! 账户 ARN 主体是**命名空间 v1**（账户 = 用户，`access::account_id_of`），不是租户隔离；
//! `arn:...:user/<name>` 形态不支持（`PolicyRequest` 无用户名）。其余明确不做（记档）：
//! Conditions / NotAction / NotResource / 匿名桶级与 AP 操作。

use serde::Deserialize;

/// 顶层文档。序列化回存用原文，此处只做解析与判定。
#[derive(Debug, Clone, Deserialize)]
pub struct PolicyDoc {
    #[serde(default, rename = "Statement")]
    pub statement: Vec<Statement>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Statement {
    #[serde(rename = "Effect")]
    pub effect: String,
    #[serde(default, rename = "Principal")]
    pub principal: PrincipalField,
    #[serde(default, rename = "Action")]
    pub action: StringOrVec,
    #[serde(default, rename = "Resource")]
    pub resource: StringOrVec,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PrincipalField {
    #[serde(default, rename = "AWS")]
    pub aws: StringOrVec,
}

/// 字符串或字符串数组（AWS 两种写法都合法）。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(untagged)]
pub enum StringOrVec {
    #[default]
    Empty,
    One(String),
    Many(Vec<String>),
}

impl StringOrVec {
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        match self {
            StringOrVec::Empty => Box::new(std::iter::empty()),
            StringOrVec::One(s) => Box::new(std::iter::once(s.as_str())),
            StringOrVec::Many(v) => Box::new(v.iter().map(String::as_str)),
        }
    }
}

/// 请求上下文（判定输入）。
#[derive(Debug, Clone)]
pub struct PolicyRequest<'a> {
    /// s3s 线名（如 `GetObject`）
    pub op: &'a str,
    pub ak: &'a str,
    pub uid: i64,
    pub bucket: &'a str,
    /// 对象键（桶级 op 为 None）
    pub key: Option<&'a str>,
    /// 无凭据请求：Principal 只认 `"*"`（`uid:0`/空 AK 不得命中）。
    pub anonymous: bool,
}

/// op → `s3:<Action>`（子集；未映射返回 None → 策略不参与，回落既有判定）。
pub fn s3_action(op: &str) -> Option<&'static str> {
    Some(match op {
        "GetObject" | "HeadObject" | "GetObjectTagging" => "s3:GetObject",
        "PutObject"
        | "PostObject"
        | "PutObjectTagging"
        | "DeleteObjectTagging"
        | "UploadPartCopy"
        | "CreateMultipartUpload"
        | "UploadPart"
        | "CompleteMultipartUpload"
        | "AbortMultipartUpload" => "s3:PutObject",
        "DeleteObject" | "DeleteObjects" => "s3:DeleteObject",
        "HeadBucket"
        | "ListObjects"
        | "ListObjectsV2"
        | "ListMultipartUploads"
        | "ListObjectVersions"
        | "ListParts" => "s3:ListBucket",
        "GetObjectRetention" => "s3:GetObjectRetention",
        "PutObjectRetention" => "s3:PutObjectRetention",
        "GetObjectLegalHold" => "s3:GetObjectLegalHold",
        "PutObjectLegalHold" => "s3:PutObjectLegalHold",
        "GetBucketVersioning" => "s3:GetBucketVersioning",
        "PutBucketVersioning" => "s3:PutBucketVersioning",
        "GetBucketLifecycleConfiguration" => "s3:GetLifecycleConfiguration",
        "PutBucketLifecycleConfiguration" | "DeleteBucketLifecycle" => {
            "s3:PutLifecycleConfiguration"
        }
        // M8 S5：Inventory（读/写/删三种桶级动作）
        "GetBucketInventoryConfiguration" | "ListBucketInventoryConfigurations" => {
            "s3:GetInventoryConfiguration"
        }
        "PutBucketInventoryConfiguration" => "s3:PutInventoryConfiguration",
        "DeleteBucketInventoryConfiguration" => "s3:DeleteInventoryConfiguration",
        "RestoreObject" => "s3:RestoreObject",
        "DeleteBucket" => "s3:DeleteBucket",
        "CreateBucket" => "s3:CreateBucket",
        // CopyObject 需源读 + 目标写两个动作——在调用方拆开处理，不走此映射
        _ => return None,
    })
}

pub(crate) fn glob_match(pattern: &str, value: &str) -> bool {
    // 仅支持尾部 `*`（AWS 的 `s3:Get*` / 前缀资源形态；中缀通配等不做）
    match pattern.strip_suffix('*') {
        Some(prefix) => value.starts_with(prefix),
        None => pattern == value,
    }
}

fn principal_matches(p: &PrincipalField, r: &PolicyRequest<'_>) -> bool {
    p.aws.iter().any(|v| {
        if r.anonymous {
            // 匿名只认通配；uid:0 哨兵/空 AK/具体 AK 绝不放行（fail-closed）。
            return v == "*";
        }
        v == "*"
            || v == r.ak
            || v.strip_prefix("uid:").and_then(|n| n.parse::<i64>().ok()) == Some(r.uid)
            // 账户命名空间 v1：账户 = 用户；LEGACY/uid=0 永不匹配。
            || crate::access::account_arn_matches(v, r.uid)
    })
}

/// 请求的 Resource ARN（按 op 类别单轴——verifier 非阻断 2）：对象级请求（有键）只匹配
/// 对象 ARN；桶级请求（无键）只匹配桶 ARN。旧实现对象请求同时给桶+对象两个 ARN，桶级 ARN
/// 会匹配对象请求（比 AWS 放宽：`arn:...:bkt` 形态的 Deny 会误压对象读）。
fn resource_arns(p: &PolicyRequest<'_>) -> Vec<String> {
    let base = format!("arn:aws:s3:::{}", p.bucket);
    match p.key {
        Some(k) => vec![format!("{base}/{k}")],
        None => vec![base],
    }
}

fn resource_matches(s: &Statement, p: &PolicyRequest<'_>) -> bool {
    let arns = resource_arns(p);
    s.resource
        .iter()
        .any(|pat| arns.iter().any(|arn| glob_match(pat, arn)))
}

impl PolicyDoc {
    /// 语义校验（console PUT 调用）：serde 只保证形状，本方法保证必填字段有值——
    /// Effect 拼错/缺 Principal·Action·Resource 的语句会**静默空转**（不生效也不报错），
    /// 与头注「不静默忽略」矛盾（verifier 非阻断 1）。空 `Statement` 合法（= 清策略形态，
    /// evaluate 恒 `None` → 回落三档）。
    pub fn validate(&self) -> Result<(), String> {
        for (i, st) in self.statement.iter().enumerate() {
            if st.effect != "Allow" && st.effect != "Deny" {
                return Err(format!(
                    "Statement[{i}].Effect 必须是 \"Allow\" 或 \"Deny\""
                ));
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
}

/// 控制台 IAM 预设模板（静态目录，不落库）。占位符由客户端替换，保存仍走既有
/// `PUT /buckets/{bucket}/policy` 的 `PolicyDoc::validate`。
///
/// 安全口径：只有 Allow；Principal 一律 `${PRINCIPAL}`（**永不 `*`**）；不含删除动作；
/// 不提供 public-read / public-write / 匿名模板。
#[derive(Debug, Clone, serde::Serialize)]
pub struct PolicyTemplate {
    pub id: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub doc: serde_json::Value,
}

/// 预设目录（顺序与数量是对外契约，测试钉住）。
pub fn policy_templates() -> Vec<PolicyTemplate> {
    let allow = |actions: &[&str], resource: &str| {
        serde_json::json!({
            "Effect": "Allow",
            "Principal": { "AWS": "${PRINCIPAL}" },
            "Action": actions,
            "Resource": resource,
        })
    };
    vec![
        PolicyTemplate {
            id: "read-only",
            name: "只读",
            description: "列出桶并读取桶内对象（${PRINCIPAL} 替换为 AK 或 uid:<n>，${BUCKET} 替换为桶名）",
            doc: serde_json::json!({"Statement": [
                allow(&["s3:ListBucket"], "arn:aws:s3:::${BUCKET}"),
                allow(&["s3:GetObject"], "arn:aws:s3:::${BUCKET}/*"),
            ]}),
        },
        PolicyTemplate {
            id: "read-write",
            name: "读写",
            description: "列出桶、读取并写入桶内对象，不含删除（同上占位符）",
            doc: serde_json::json!({"Statement": [
                allow(&["s3:ListBucket"], "arn:aws:s3:::${BUCKET}"),
                allow(&["s3:GetObject"], "arn:aws:s3:::${BUCKET}/*"),
                allow(&["s3:PutObject"], "arn:aws:s3:::${BUCKET}/*"),
            ]}),
        },
        PolicyTemplate {
            id: "prefix-isolated",
            name: "前缀隔离",
            description: "仅允许 ${PREFIX} 前缀内对象的读写；不授予 ListBucket（避免暴露前缀外的键名）",
            doc: serde_json::json!({"Statement": [
                allow(&["s3:GetObject", "s3:PutObject"], "arn:aws:s3:::${BUCKET}/${PREFIX}*"),
            ]}),
        },
    ]
}

/// 单动作判定：`Some(true)` Allow / `Some(false)` Deny / `None` 无匹配。
pub fn evaluate(policy: &PolicyDoc, p: &PolicyRequest<'_>, action: &str) -> Option<bool> {
    let mut allowed = false;
    let mut any_match = false;
    for st in &policy.statement {
        if !principal_matches(&st.principal, p) {
            continue;
        }
        if !st.action.iter().any(|pat| glob_match(pat, action)) {
            continue;
        }
        if !resource_matches(st, p) {
            continue;
        }
        any_match = true;
        match st.effect.as_str() {
            "Deny" => return Some(false), // 显式 Deny 最高优先：立即短路
            "Allow" => allowed = true,
            _ => {} // 非法 Effect：console PUT 已被 validate 拦；直写 DB 绕过时忽略
        }
    }
    if any_match && allowed {
        Some(true)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(json: &str) -> PolicyDoc {
        serde_json::from_str(json).unwrap()
    }

    fn req<'a>(op: &'a str, key: Option<&'a str>) -> PolicyRequest<'a> {
        PolicyRequest {
            op,
            ak: "AKIA_SUB",
            uid: 7,
            bucket: "bkt",
            key,
            anonymous: false,
        }
    }

    /// 无凭据：uid=0 哨兵 + 空 AK + anonymous 标记。
    fn anon_req<'a>(op: &'a str, key: Option<&'a str>) -> PolicyRequest<'a> {
        PolicyRequest {
            op,
            ak: "",
            uid: 0,
            bucket: "bkt",
            key,
            anonymous: true,
        }
    }

    /// Deny 优先：同资源上 Deny + Allow 并存 → 拒；Resource 前缀/通配；Principal 三形态。
    #[test]
    fn deny_wins_and_matching_truth_table() {
        let p = doc(r#"{"Statement":[
            {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*","Resource":"arn:aws:s3:::bkt/*"},
            {"Effect":"Deny","Principal":{"AWS":"AKIA_SUB"},"Action":"s3:GetObject",
             "Resource":"arn:aws:s3:::bkt/secret*"}
        ]}"#);
        // 普通对象：Allow 命中
        assert_eq!(
            evaluate(&p, &req("GetObject", Some("open.txt")), "s3:GetObject"),
            Some(true)
        );
        // secret 前缀：Deny 优先
        assert_eq!(
            evaluate(&p, &req("GetObject", Some("secret/x")), "s3:GetObject"),
            Some(false)
        );
        // Principal "*" 对任意**有效 AK** 生效（签名请求口径不变），AKIA_OTHER 亦命中
        let mut r2 = req("GetObject", Some("open.txt"));
        r2.ak = "AKIA_OTHER";
        assert_eq!(evaluate(&p, &r2, "s3:GetObject"), Some(true));
        // uid: 形态
        let p2 = doc(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":["uid:7"]},
            "Action":["s3:Get*","s3:ListBucket"],
            "Resource":["arn:aws:s3:::bkt","arn:aws:s3:::bkt/*"]}]}"#,
        );
        assert_eq!(
            evaluate(&p2, &req("GetObject", Some("a")), "s3:GetObject"),
            Some(true)
        );
        // 桶级资源（无键）
        assert_eq!(
            evaluate(&p2, &req("HeadBucket", None), "s3:ListBucket"),
            Some(true)
        );
        assert_eq!(
            evaluate(&p2, &req("PutObject", Some("a")), "s3:PutObject"),
            None
        );
    }

    /// 匿名 Principal 矩阵：只认 `"*"`（字符串/数组）；`uid:0`、空 AK、具体 AK 均不命中；
    /// Allow 命中/资源前缀/Deny 优先与签名请求同口径。
    #[test]
    fn anonymous_principal_only_matches_wildcard() {
        let wildcard = doc(r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
            "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}"#);
        assert_eq!(
            evaluate(&wildcard, &anon_req("GetObject", Some("a")), "s3:GetObject"),
            Some(true)
        );
        // Deny"*" 优先于 Allow"*"
        let deny_wins = doc(r#"{"Statement":[
            {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*","Resource":"arn:aws:s3:::bkt/*"},
            {"Effect":"Deny","Principal":{"AWS":["*"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*"}
        ]}"#);
        assert_eq!(
            evaluate(
                &deny_wins,
                &anon_req("GetObject", Some("secret/x")),
                "s3:GetObject"
            ),
            Some(false)
        );
        assert_eq!(
            evaluate(
                &deny_wins,
                &anon_req("GetObject", Some("open")),
                "s3:GetObject"
            ),
            Some(true)
        );
        // 资源前缀：Allow 只覆盖 secret* 时，open 无匹配
        let prefix = doc(r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
            "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*"}]}"#);
        assert_eq!(
            evaluate(
                &prefix,
                &anon_req("GetObject", Some("open")),
                "s3:GetObject"
            ),
            None
        );
        // 非通配 Principal 不得因 uid=0 哨兵或空 AK 命中
        for principal in [
            r#""uid:0""#,
            r#""""#,
            r#""AKIA_SOMEONE""#,
            r#"["uid:0","AKIA_X"]"#,
        ] {
            let p: PolicyDoc = serde_json::from_str(&format!(
                r#"{{"Statement":[{{"Effect":"Allow","Principal":{{"AWS":{principal}}},
                "Action":"s3:*","Resource":"arn:aws:s3:::bkt/*"}}]}}"#
            ))
            .unwrap();
            assert_eq!(
                evaluate(&p, &anon_req("GetObject", Some("a")), "s3:GetObject"),
                None,
                "匿名不得命中 Principal {principal}"
            );
        }
        // 签名请求口径不变：同一 uid:0 文档对 uid=0 的有效签名用户仍按原语义匹配
        let mut signed_zero = req("GetObject", Some("a"));
        signed_zero.uid = 0;
        let p = doc(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"uid:0"},
            "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}"#,
        );
        assert_eq!(
            evaluate(&p, &signed_zero, "s3:GetObject"),
            Some(true),
            "签名请求的 uid: 匹配不得因匿名分支改变"
        );
    }

    /// 账户 ARN 主体（命名空间 v1，账户 = 用户）：只命中派生出的 12 位同账户 uid；
    /// LEGACY/非 12 位/`user/` 形态/uid=0/匿名一律不命中。
    #[test]
    fn account_root_principal_matches_only_derived_account() {
        let p = doc(r#"{"Statement":[
            {"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::000000000007:root"},
             "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"},
            {"Effect":"Deny","Principal":{"AWS":["arn:aws:iam::000000000009:root"]},
             "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/secret*"}
        ]}"#);
        // req.uid = 7
        assert_eq!(
            evaluate(&p, &req("GetObject", Some("a")), "s3:GetObject"),
            Some(true)
        );
        let mut other = req("GetObject", Some("a"));
        other.uid = 8;
        assert_eq!(evaluate(&p, &other, "s3:GetObject"), None);
        // Deny 对目标账户生效（uid=9）
        let mut nine = req("GetObject", Some("secret/x"));
        nine.uid = 9;
        assert_eq!(evaluate(&p, &nine, "s3:GetObject"), Some(false));
        // 非派生账户形态不命中：LEGACY、非 12 位、user/ 形态、缺 :root
        for arn in [
            "arn:aws:iam::000000000000:root",
            "arn:aws:iam::7:root",
            "arn:aws:iam::000000000007:user/erik",
            "arn:aws:iam::000000000007",
        ] {
            let p = doc(&format!(
                r#"{{"Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{arn}"}},
                "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}}]}}"#
            ));
            assert_eq!(
                evaluate(&p, &req("GetObject", Some("a")), "s3:GetObject"),
                None,
                "账户 ARN {arn} 不得命中"
            );
        }
        // uid 越界（> 12 位）不构成账户；匿名只认 "*"
        let mut big = req("GetObject", Some("a"));
        big.uid = 1_000_000_000_000;
        let big_doc = doc(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::1000000000000:root"},
            "Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}"#,
        );
        assert_eq!(evaluate(&big_doc, &big, "s3:GetObject"), None);
        assert_eq!(
            evaluate(&p, &anon_req("GetObject", Some("a")), "s3:GetObject"),
            None
        );
    }

    /// 解析拒收：Effect 缺失/占位以外的形状直接失败（PUT 层转 400）。
    #[test]
    fn malformed_policy_rejected() {
        assert!(serde_json::from_str::<PolicyDoc>(r#"{"Statement":[{"Action":"s3:*"}]}"#).is_err());
        // 注：裸 `[]` 会被 serde 的「结构体位置形式」吞成空策略（statement 有 default）——
        // 顶层必须为对象的硬检在控制面 PUT（console::put_policy），此处不重复测。
        assert!(
            serde_json::from_str::<PolicyDoc>(r#"{"Statement":[{"Effect":"Allow","Action":1}]}"#)
                .is_err()
        );
    }

    /// op 映射与未映射回落。
    #[test]
    fn action_mapping() {
        assert_eq!(s3_action("GetObject"), Some("s3:GetObject"));
        assert_eq!(s3_action("PutObject"), Some("s3:PutObject"));
        assert_eq!(s3_action("DeleteObjects"), Some("s3:DeleteObject"));
        assert_eq!(s3_action("ListObjects"), Some("s3:ListBucket"));
        assert_eq!(s3_action("ListObjectsV2"), Some("s3:ListBucket"));
        assert_eq!(s3_action("ListMultipartUploads"), Some("s3:ListBucket"));
        // M8 前置：标签 op 折叠到对象读/写（AWS 语义）；UploadPartCopy 目标写
        assert_eq!(s3_action("GetObjectTagging"), Some("s3:GetObject"));
        assert_eq!(s3_action("PutObjectTagging"), Some("s3:PutObject"));
        assert_eq!(s3_action("DeleteObjectTagging"), Some("s3:PutObject"));
        assert_eq!(s3_action("UploadPartCopy"), Some("s3:PutObject"));
        // 保留策略独立动作（读/写）
        assert_eq!(
            s3_action("GetObjectRetention"),
            Some("s3:GetObjectRetention")
        );
        assert_eq!(
            s3_action("PutObjectRetention"),
            Some("s3:PutObjectRetention")
        );
        // H1：法定保留独立动作，绝不复用 Put/GetObject
        assert_eq!(
            s3_action("GetObjectLegalHold"),
            Some("s3:GetObjectLegalHold")
        );
        assert_eq!(
            s3_action("PutObjectLegalHold"),
            Some("s3:PutObjectLegalHold")
        );
        assert_eq!(s3_action("CopyObject"), None);
        assert_eq!(s3_action("Frobnicate"), None);
        // M8 S4b：生命周期配置与恢复动作
        assert_eq!(
            s3_action("GetBucketLifecycleConfiguration"),
            Some("s3:GetLifecycleConfiguration")
        );
        assert_eq!(
            s3_action("PutBucketLifecycleConfiguration"),
            Some("s3:PutLifecycleConfiguration")
        );
        assert_eq!(
            s3_action("DeleteBucketLifecycle"),
            Some("s3:PutLifecycleConfiguration")
        );
        assert_eq!(s3_action("RestoreObject"), Some("s3:RestoreObject"));
        // M8 S5：Inventory 桶级动作
        assert_eq!(
            s3_action("GetBucketInventoryConfiguration"),
            Some("s3:GetInventoryConfiguration")
        );
        assert_eq!(
            s3_action("PutBucketInventoryConfiguration"),
            Some("s3:PutInventoryConfiguration")
        );
        assert_eq!(
            s3_action("DeleteBucketInventoryConfiguration"),
            Some("s3:DeleteInventoryConfiguration")
        );
    }

    /// 标签 op 折叠后，s3:GetObject / s3:PutObject 的 Deny 同样压住标签读写。
    #[test]
    fn tagging_folds_into_object_actions() {
        let deny_read = doc(r#"{"Statement":[
            {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}
        ]}"#);
        assert_eq!(
            evaluate(
                &deny_read,
                &req("GetObjectTagging", Some("a")),
                "s3:GetObject"
            ),
            Some(false)
        );
        let allow_write = doc(r#"{"Statement":[
            {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bkt/*"}
        ]}"#);
        for op in ["PutObjectTagging", "DeleteObjectTagging"] {
            assert_eq!(
                evaluate(&allow_write, &req(op, Some("a")), "s3:PutObject"),
                Some(true)
            );
        }
    }

    /// Resource 按 op 类别单轴（verifier 非阻断 2）：桶 ARN 不匹配对象请求、对象 ARN 不匹配桶请求。
    #[test]
    fn resource_arn_is_op_class_scoped() {
        let p = doc(r#"{"Statement":[
            {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt"}
        ]}"#);
        assert_eq!(
            evaluate(&p, &req("GetObject", Some("a")), "s3:GetObject"),
            None,
            "桶级 ARN 不得匹配对象请求"
        );
        let p = doc(r#"{"Statement":[
            {"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bkt/*"}
        ]}"#);
        assert_eq!(
            evaluate(&p, &req("HeadBucket", None), "s3:ListBucket"),
            None,
            "对象级 ARN 不得匹配桶请求"
        );
    }

    /// validate：Effect 拼错/缺字段拒（防静默空转）；空 Statement 合法（= 清策略形态）。
    #[test]
    fn validate_requires_explicit_fields() {
        let bad_effect = doc(r#"{"Statement":[{"Effect":"Dney","Principal":{"AWS":"*"},
            "Action":"s3:*","Resource":"arn:aws:s3:::bkt/*"}]}"#);
        assert!(bad_effect.validate().is_err());
        let missing_action = doc(r#"{"Statement":[{"Effect":"Deny","Principal":{"AWS":"*"},
            "Resource":"arn:aws:s3:::bkt/*"}]}"#);
        assert!(missing_action.validate().is_err());
        let missing_resource = doc(r#"{"Statement":[{"Effect":"Deny","Principal":{"AWS":"*"},
            "Action":"s3:*"}]}"#);
        assert!(missing_resource.validate().is_err());
        assert!(doc(r#"{"Statement":[]}"#).validate().is_ok());
        let ok = doc(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"uid:7"},
            "Action":"s3:Get*","Resource":"arn:aws:s3:::bkt/*"}]}"#,
        );
        assert!(ok.validate().is_ok());
    }

    /// 预设目录：数量/顺序稳定；占位符原样与替换后都过校验；无匿名 Principal、无删除动作。
    #[test]
    fn preset_templates_validate_and_are_safe() {
        let ts = policy_templates();
        let ids: Vec<_> = ts.iter().map(|t| t.id).collect();
        assert_eq!(ids, ["read-only", "read-write", "prefix-isolated"]);
        for t in &ts {
            let doc: PolicyDoc = serde_json::from_value(t.doc.clone()).unwrap();
            doc.validate().unwrap();
            let filled = t
                .doc
                .to_string()
                .replace("${BUCKET}", "bkt")
                .replace("${PREFIX}", "logs/")
                .replace("${PRINCIPAL}", "uid:7");
            let doc: PolicyDoc = serde_json::from_str(&filled).unwrap();
            doc.validate().unwrap();
            for st in &doc.statement {
                assert_eq!(st.effect, "Allow", "{}", t.id);
                for p in st.principal.aws.iter() {
                    assert_ne!(p, "*", "{} 不得含匿名 Principal", t.id);
                }
                for a in st.action.iter() {
                    assert!(!a.contains("Delete"), "{} 不得含删除动作", t.id);
                    assert_ne!(a, "s3:*", "{} 不得用全通配动作", t.id);
                }
            }
        }
    }
}
