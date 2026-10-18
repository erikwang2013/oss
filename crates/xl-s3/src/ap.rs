//! M8 S1：访问点（Access Point）——模型、严格策略解析与 op 推导。
//!
//! 路由形态只支持路径式 `/ap/<name>/<key>`；AP 只做**额外限制**（与身份/桶策略取交集），
//! 绝不放松既有授权。策略方言 = 现有 `policy` 子集，但解析更严格（未知字段/条件一律拒）。

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
