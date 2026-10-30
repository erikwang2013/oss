//! M6 切片 4：桶复制写入侧——规则解析（严格白名单）、写提交钩子、配置存储抽象。
//!
//! 分层同事件总线：xl-s3 定义 trait 与 DTO→规则校验；oss-server 提供 SQL 实现、
//! repl_outbox 与出站 worker。规则 JSON 即 `Vec<ReplRule>`。

use async_trait::async_trait;
use s3s::dto::*;
use serde::{Deserialize, Serialize};
use xl_store::meta::ObjectTag;

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
    /// 标签过滤（列出的键值必须全部存在且相等；空 = 无标签条件）。
    /// 与 `prefix` 同时存在即 S3 `And` 语义；旧记录无该字段时为空。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<ObjectTag>,
    pub remote_id: String,
    pub dest_bucket: String,
    /// DeleteMarkerReplication（缺省 Disabled）
    #[serde(default)]
    pub delete_marker: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl ReplRule {
    /// 过滤器命中：前缀匹配，且列出的标签键值全部存在（S3 的 Prefix/Tag/And 归一化）。
    pub fn matches_filter(&self, key: &str, tags: &[ObjectTag]) -> bool {
        self.prefix.as_deref().is_none_or(|p| key.starts_with(p))
            && self.tags.iter().all(|want| tags.contains(want))
    }
}

/// 过滤器里的 DTO Tag → 归一化标签（Key 必填；Value 缺省按空串，同对象标签）。
fn filter_tag(t: &Tag, rule: &str) -> s3s::S3Result<ObjectTag> {
    let key = t.key.as_deref().unwrap_or_default();
    if key.is_empty() {
        return Err(bad(format!("规则 {rule}：标签过滤器缺少 Key")));
    }
    Ok(ObjectTag {
        key: key.to_string(),
        value: t.value.clone().unwrap_or_default(),
    })
}

/// `And`：至少两个条件（Prefix 计 1、每个 Tag 计 1）；标签 Key 必须唯一。
fn and_filter(
    and: &ReplicationRuleAndOperator,
    rule: &str,
) -> s3s::S3Result<(Option<String>, Vec<ObjectTag>)> {
    let mut tags = Vec::new();
    for t in and.tags.as_deref().unwrap_or_default() {
        let tag = filter_tag(t, rule)?;
        if tags.iter().any(|x: &ObjectTag| x.key == tag.key) {
            return Err(bad(format!(
                "规则 {rule}：And 过滤器标签 Key 重复：{}",
                tag.key
            )));
        }
        tags.push(tag);
    }
    if usize::from(and.prefix.is_some()) + tags.len() < 2 {
        return Err(bad(format!(
            "规则 {rule}：And 过滤器至少需要两个条件（Prefix 或 Tag）"
        )));
    }
    Ok((and.prefix.clone(), tags))
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
        let id = r.id.clone().unwrap_or_else(|| format!("rule-{}", i + 1));
        // 过滤器：Prefix/Tag/And 三选一（S3 语义）；无过滤器 = 全桶。
        let (mut prefix, tags) = match &r.filter {
            None => (None, Vec::new()),
            Some(f) => {
                let count = usize::from(f.prefix.is_some())
                    + usize::from(f.tag.is_some())
                    + usize::from(f.and.is_some());
                if count != 1 {
                    return Err(bad(format!(
                        "规则 {id}：Filter 必须且只能包含 Prefix、Tag、And 之一"
                    )));
                }
                match (&f.prefix, &f.tag, &f.and) {
                    (Some(p), None, None) => (Some(p.clone()), Vec::new()),
                    (None, Some(t), None) => (None, vec![filter_tag(t, &id)?]),
                    (None, None, Some(a)) => and_filter(a, &id)?,
                    _ => unreachable!(),
                }
            }
        };
        // 旧式顶层 Prefix 与 Filter 并存：Prefix 必须一致（既有行为）；
        // 标签过滤器与顶层 Prefix 组合必须显式写成 And，避免歧义。
        if let Some(p) = &r.prefix {
            match &prefix {
                Some(x) if x != p => return Err(bad("规则 Prefix 与 Filter.Prefix 不一致")),
                None if !tags.is_empty() => {
                    return Err(bad(format!(
                        "规则 {id}：标签过滤器不能与顶层 Prefix 并用（请改用 And）"
                    )));
                }
                _ => {}
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
        out.push(ReplRule {
            id,
            prefix,
            tags,
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
    /// 版本定向删除（`DELETE ?versionId=`）→ 目标定点删同名版本，不得造 marker。
    /// 站点对采用同源版本 id（见 sync 摄入），因此目标按同一 id 定位。
    pub is_version_delete: bool,
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

    fn tag(key: &str, value: &str) -> Tag {
        Tag {
            key: Some(key.into()),
            value: Some(value.into()),
        }
    }

    fn obj_tag(key: &str, value: &str) -> ObjectTag {
        ObjectTag {
            key: key.into(),
            value: value.into(),
        }
    }

    fn filter(
        prefix: Option<&str>,
        tag: Option<Tag>,
        and: Option<ReplicationRuleAndOperator>,
    ) -> ReplicationRuleFilter {
        ReplicationRuleFilter {
            and,
            prefix: prefix.map(str::to_string),
            tag,
        }
    }

    fn parsed(r: ReplicationRule) -> ReplRule {
        parse_replication_config(&cfg(vec![r]), &["r1".to_string()])
            .unwrap()
            .remove(0)
    }

    fn parse_err(r: ReplicationRule) -> String {
        parse_replication_config(&cfg(vec![r]), &["r1".to_string()])
            .unwrap_err()
            .message()
            .unwrap_or_default()
            .to_string()
    }

    #[test]
    fn parse_matrix() {
        let ids = vec!["r1".to_string()];
        let mut a = rule("r1/dst");
        a.id = Some("x".into());
        a.filter = Some(filter(Some("logs/"), None, None));
        a.delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static("Enabled")),
        });
        let got = parse_replication_config(&cfg(vec![a]), &ids).unwrap();
        assert_eq!(
            got,
            vec![ReplRule {
                id: "x".into(),
                prefix: Some("logs/".into()),
                tags: Vec::new(),
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
        assert!(got[0].tags.is_empty());
        assert!(!got[0].delete_marker);
        // 非法：空规则、未知 remote、坏 destination、坏状态
        assert!(parse_replication_config(&cfg(vec![]), &ids).is_err());
        assert!(parse_replication_config(&cfg(vec![rule("nope/dst")]), &ids).is_err());
        assert!(parse_replication_config(&cfg(vec![rule("r1")]), &ids).is_err());
        assert!(parse_replication_config(&cfg(vec![rule("r1/a/b")]), &ids).is_err());
        let mut s = rule("r1/dst");
        s.status = ReplicationRuleStatus::from_static("Bogus");
        assert!(parse_replication_config(&cfg(vec![s]), &ids).is_err());
        // Prefix 冲突
        let mut p = rule("r1/dst");
        p.prefix = Some("a/".into());
        p.filter = Some(filter(Some("b/"), None, None));
        assert!(parse_replication_config(&cfg(vec![p]), &ids).is_err());
        // 旧式顶层 Prefix 与 Filter.Prefix 一致 = 既有行为，保留
        let mut ok = rule("r1/dst");
        ok.prefix = Some("a/".into());
        ok.filter = Some(filter(Some("a/"), None, None));
        assert_eq!(parsed(ok).prefix.as_deref(), Some("a/"));
    }

    /// Prefix / Tag / And 的命中矩阵：只有全部条件满足才复制。
    #[test]
    fn filter_include_exclude_matrix() {
        let obj = vec![obj_tag("env", "prod"), obj_tag("team", "a")];

        // 无过滤器 = 全桶
        let r = parsed(rule("r1/dst"));
        assert!(r.matches_filter("any/key", &[]));
        assert!(r.matches_filter("any/key", &obj));

        // Prefix 单条件
        let mut d = rule("r1/dst");
        d.filter = Some(filter(Some("logs/"), None, None));
        let r = parsed(d);
        assert!(r.matches_filter("logs/a", &obj));
        assert!(!r.matches_filter("data/a", &obj));

        // Tag 单条件（键值全等；缺标签/值不同都不命中）
        let mut d = rule("r1/dst");
        d.filter = Some(filter(None, Some(tag("env", "prod")), None));
        let r = parsed(d);
        assert!(r.matches_filter("any", &obj));
        assert!(!r.matches_filter("any", &[obj_tag("env", "dev")]));
        assert!(!r.matches_filter("any", &[]));

        // And { Prefix, Tag }：两个条件同时满足才命中
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            None,
            None,
            Some(ReplicationRuleAndOperator {
                prefix: Some("logs/".into()),
                tags: Some(vec![tag("env", "prod")]),
            }),
        ));
        let r = parsed(d);
        assert!(r.matches_filter("logs/a", &obj));
        assert!(!r.matches_filter("data/a", &obj));
        assert!(!r.matches_filter("logs/a", &[obj_tag("env", "dev")]));

        // And { 多个 Tag }：全部存在才命中
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            None,
            None,
            Some(ReplicationRuleAndOperator {
                prefix: None,
                tags: Some(vec![tag("env", "prod"), tag("team", "a")]),
            }),
        ));
        let r = parsed(d);
        assert!(r.matches_filter("any", &obj));
        assert!(!r.matches_filter("any", &[obj_tag("env", "prod")]));
        assert!(!r.matches_filter("any", &[obj_tag("env", "prod"), obj_tag("team", "b")]));

        // 空值标签：DTO 缺省 Value 归一为空串，与空值对象标签全等命中
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            None,
            Some(Tag {
                key: Some("k".into()),
                value: None,
            }),
            None,
        ));
        let r = parsed(d);
        assert!(r.matches_filter("any", &[obj_tag("k", "")]));
        assert!(!r.matches_filter("any", &[obj_tag("k", "v")]));
    }

    /// 非法过滤器组合必须明确拒绝（不能静默按错误语义复制）。
    #[test]
    fn filter_invalid_configs() {
        // Filter 必须三选一：多选/全空都非法
        let mut d = rule("r1/dst");
        d.filter = Some(filter(Some("a/"), Some(tag("k", "v")), None));
        assert!(parse_err(d).contains("只能包含"), "多选");
        let mut d = rule("r1/dst");
        d.filter = Some(filter(None, None, None));
        assert!(parse_err(d).contains("只能包含"), "全空");

        // And 与 Prefix/Tag 同级同时出现
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            Some("a/"),
            None,
            Some(ReplicationRuleAndOperator {
                prefix: Some("a/".into()),
                tags: Some(vec![tag("k", "v")]),
            }),
        ));
        assert!(parse_err(d).contains("只能包含"));

        // And 至少两个条件（单 Prefix / 单 Tag 都不行）
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            None,
            None,
            Some(ReplicationRuleAndOperator {
                prefix: Some("a/".into()),
                tags: None,
            }),
        ));
        assert!(parse_err(d).contains("至少需要两个条件"));
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            None,
            None,
            Some(ReplicationRuleAndOperator {
                prefix: None,
                tags: Some(vec![tag("k", "v")]),
            }),
        ));
        assert!(parse_err(d).contains("至少需要两个条件"));
        // And 空（原「And 未实现」用例的等价拒绝）
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            None,
            None,
            Some(ReplicationRuleAndOperator {
                prefix: None,
                tags: None,
            }),
        ));
        assert!(parse_err(d).contains("至少需要两个条件"));

        // Tag 缺 Key / And 内标签 Key 重复
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            None,
            Some(Tag {
                key: None,
                value: Some("v".into()),
            }),
            None,
        ));
        assert!(parse_err(d).contains("缺少 Key"));
        let mut d = rule("r1/dst");
        d.filter = Some(filter(
            None,
            None,
            Some(ReplicationRuleAndOperator {
                prefix: Some("a/".into()),
                tags: Some(vec![tag("k", "1"), tag("k", "2")]),
            }),
        ));
        assert!(parse_err(d).contains("Key 重复"));

        // 顶层 Prefix 不能与标签过滤器并用（必须写成 And）
        let mut d = rule("r1/dst");
        d.prefix = Some("a/".into());
        d.filter = Some(filter(None, Some(tag("k", "v")), None));
        assert!(parse_err(d).contains("顶层 Prefix"));
        let mut d = rule("r1/dst");
        d.prefix = Some("a/".into());
        d.filter = Some(filter(
            None,
            None,
            Some(ReplicationRuleAndOperator {
                prefix: None,
                tags: Some(vec![tag("k", "1"), tag("j", "2")]),
            }),
        ));
        assert!(parse_err(d).contains("顶层 Prefix"));
    }
}
