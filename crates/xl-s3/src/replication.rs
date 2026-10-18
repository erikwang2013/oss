//! M6 切片 4：桶复制写入侧——规则解析（严格白名单）、写提交钩子、配置存储抽象。
//!
//! 分层同事件总线：xl-s3 定义 trait 与 DTO→规则校验；oss-server 提供 SQL 实现、
//! repl_outbox 与出站 worker。规则 JSON 即 `Vec<ReplRule>`。

use async_trait::async_trait;
use s3s::dto::*;
use serde::{Deserialize, Serialize};

fn bad(msg: impl Into<String>) -> s3s::S3Error {
    s3s::s3_error!(InvalidRequest, "{}", msg.into())
}

fn default_true() -> bool {
    true
}

/// 归一化复制规则（存 `replication_rules.rules_json`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplRule {
    pub id: String,
    #[serde(default)]
    pub prefix: Option<String>,
    pub remote_id: String,
    pub dest_bucket: String,
    /// DeleteMarkerReplication（缺省 Disabled）
    #[serde(default)]
    pub delete_marker: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// DTO → 规则（严格校验；Destination.Bucket 形式为 `<remote_id>/<dest_bucket>`）。
pub fn parse_replication_config(
    cfg: &ReplicationConfiguration,
    remote_ids: &[String],
) -> s3s::S3Result<Vec<ReplRule>> {
    if cfg.rules.is_empty() {
        return Err(bad("复制配置至少需要一条规则"));
    }
    if cfg.rules.len() > 1000 {
        return Err(bad("复制规则超过 1000 条上限"));
    }
    let mut out = Vec::with_capacity(cfg.rules.len());
    for (i, r) in cfg.rules.iter().enumerate() {
        let enabled = match r.status.as_str() {
            "Enabled" => true,
            "Disabled" => false,
            other => return Err(bad(format!("非法规则状态：{other}"))),
        };
        // 过滤器：只支持 Prefix（无过滤器 = 全桶）；And/Tag 明确拒绝（不静默错结果）
        let mut prefix = match &r.filter {
            None => None,
            Some(f) => {
                if f.and.is_some() || f.tag.is_some() {
                    return Err(bad("只支持前缀过滤（And/Tag 过滤器未实现）"));
                }
                f.prefix.clone()
            }
        };
        // 旧式顶层 Prefix 与 Filter.Prefix 并存则必须一致
        if let Some(p) = &r.prefix {
            if let Some(x) = &prefix
                && x != p
            {
                return Err(bad("规则 Prefix 与 Filter.Prefix 不一致"));
            }
            prefix = Some(p.clone());
        }
        // 目标：<remote_id>/<dest_bucket>
        let Some((remote, dest)) = r.destination.bucket.split_once('/') else {
            return Err(bad("Destination.Bucket 必须形如 <remote-id>/<bucket>"));
        };
        if remote.is_empty() || dest.is_empty() || dest.contains('/') {
            return Err(bad("Destination.Bucket 必须形如 <remote-id>/<bucket>"));
        }
        if !remote_ids.iter().any(|x| x == remote) {
            return Err(bad(format!("Destination 引用了未知 remote：{remote}")));
        }
        // DeleteMarkerReplication：缺省 Disabled；仅接受两种取值
        let delete_marker = match r
            .delete_marker_replication
            .as_ref()
            .and_then(|d| d.status.as_ref())
        {
            None => false,
            Some(s) => match s.as_str() {
                "Enabled" => true,
                "Disabled" => false,
                other => return Err(bad(format!("非法 DeleteMarkerReplication 状态：{other}"))),
            },
        };
        // ExistingObjectReplication：合法枚举即接受；**不回溯**既有对象（计划裁决记档）
        if let Some(eo) = &r.existing_object_replication
            && !matches!(eo.status.as_str(), "Enabled" | "Disabled")
        {
            return Err(bad(format!(
                "非法 ExistingObjectReplication 状态：{}",
                eo.status.as_str()
            )));
        }
        let id = r.id.clone().unwrap_or_else(|| format!("rule-{}", i + 1));
        out.push(ReplRule {
            id,
            prefix,
            remote_id: remote.to_string(),
            dest_bucket: dest.to_string(),
            delete_marker,
            enabled,
        });
    }
    Ok(out)
}

/// 读回规则 JSON（worker 侧；损坏记录 → Err 由调用方记日志/跳过）。
pub fn rules_from_json(s: &str) -> Result<Vec<ReplRule>, String> {
    serde_json::from_str(s).map_err(|e| e.to_string())
}

/// 写提交后回调信息。
#[derive(Debug, Clone)]
pub struct WriteInfo<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<&'a str>,
    pub etag: &'a str,
    pub size: u64,
    /// 删除操作（版本桶 marker，或 console/lifecycle 的整键硬删）→ 目标 DELETE
    pub is_delete_marker: bool,
    /// 复制摄入对象（x-amz-replication-status: REPLICA）不再参与复制（防环）
    pub is_replica: bool,
}

/// 写路径钩子：commit 之后调用；实现侧吞错记日志（不改变请求结果，同事件 sink 口径）。
#[async_trait]
pub trait ReplicationSink: Send + Sync {
    async fn on_write(&self, info: WriteInfo<'_>);
}

/// 复制配置存储（Put/Get/DeleteBucketReplication 背后的持久层；oss-server 实现）。
#[async_trait]
pub trait ReplicationStore: Send + Sync {
    async fn load_rules(&self, bucket: &str) -> Result<Option<String>, String>;
    async fn save_rules(&self, bucket: &str, rules_json: &str) -> Result<(), String>;
    async fn remove_rules(&self, bucket: &str) -> Result<(), String>;
    /// oss.toml `[[remotes]]` 的 id 列表（校验 Destination 引用）
    async fn remote_ids(&self) -> Vec<String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(rules: Vec<ReplicationRule>) -> ReplicationConfiguration {
        ReplicationConfiguration {
            role: "arn:aws:iam::1:role/r".into(),
            rules,
        }
    }

    fn rule(dest: &str) -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: None,
            destination: Destination {
                access_control_translation: None,
                account: None,
                bucket: dest.to_string(),
                encryption_configuration: None,
                metrics: None,
                replication_time: None,
                storage_class: None,
            },
            existing_object_replication: None,
            filter: None,
            id: None,
            prefix: None,
            priority: None,
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static("Enabled"),
        }
    }

    #[test]
    fn parse_matrix() {
        let ids = vec!["r1".to_string()];
        let mut a = rule("r1/dst");
        a.id = Some("x".into());
        a.filter = Some(ReplicationRuleFilter {
            and: None,
            prefix: Some("logs/".into()),
            tag: None,
        });
        a.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static("Enabled")),
        });
        let got = parse_replication_config(&cfg(vec![a]), &ids).unwrap();
        assert_eq!(
            got,
            vec![ReplRule {
                id: "x".into(),
                prefix: Some("logs/".into()),
                remote_id: "r1".into(),
                dest_bucket: "dst".into(),
                delete_marker: true,
                enabled: true,
            }]
        );
        // 缺省值：无前缀、marker 关闭、id 生成
        let got = parse_replication_config(&cfg(vec![rule("r1/dst")]), &ids).unwrap();
        assert_eq!(got[0].id, "rule-1");
        assert_eq!(got[0].prefix, None);
        assert!(!got[0].delete_marker);
        // 非法：空规则、未知 remote、坏 destination、And 过滤器、坏状态
        assert!(parse_replication_config(&cfg(vec![]), &ids).is_err());
        assert!(parse_replication_config(&cfg(vec![rule("nope/dst")]), &ids).is_err());
        assert!(parse_replication_config(&cfg(vec![rule("r1")]), &ids).is_err());
        assert!(parse_replication_config(&cfg(vec![rule("r1/a/b")]), &ids).is_err());
        let mut f = rule("r1/dst");
        f.filter = Some(ReplicationRuleFilter {
            and: Some(ReplicationRuleAndOperator {
                prefix: None,
                tags: None,
            }),
            prefix: None,
            tag: None,
        });
        assert!(parse_replication_config(&cfg(vec![f]), &ids).is_err());
        let mut s = rule("r1/dst");
        s.status = ReplicationRuleStatus::from_static("Bogus");
        assert!(parse_replication_config(&cfg(vec![s]), &ids).is_err());
        // Prefix 冲突
        let mut p = rule("r1/dst");
        p.prefix = Some("a/".into());
        p.filter = Some(ReplicationRuleFilter {
            and: None,
            prefix: Some("b/".into()),
            tag: None,
        });
        assert!(parse_replication_config(&cfg(vec![p]), &ids).is_err());
    }
}
