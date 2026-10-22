//! M8 S4b：`LifecycleStore` 的 SQL 实现（xl-s3 trait）——console 与 S3 共用。
//!
//! 规则列是历史兼容投影，`config_json` 是权威版本化结构（只有旧列无法表达时才写）。
//! 校验统一走 `validate_rules`。

use std::collections::HashSet;
use std::sync::Arc;

use xl_s3::lifecycle::{
    ExpirationAction, LifecycleError, LifecycleRuleSpec, LifecycleStore, from_config_json,
    validate_rules,
};

use crate::db::{Db, LifecycleRuleRow};
use crate::raft::command::{ControlCommand, ControlResponse, LifecycleRuleCmd, LifecycleRuleCmdV2};
use crate::raft::control::ControlPlane;

pub struct SqlLifecycleStore {
    db: Arc<Db>,
    /// 启动期固定的 tier id（归档类规则解析/校验用）。
    tier_ids: Arc<Vec<String>>,
    /// M9 S6c：仅 Raft 模式路由控制写（None/Local = M7 直写）。
    control: Option<Arc<ControlPlane>>,
}

impl SqlLifecycleStore {
    pub fn new(db: Arc<Db>, tier_ids: Arc<Vec<String>>) -> Self {
        Self {
            db,
            tier_ids,
            control: None,
        }
    }

    pub fn with_control(mut self, control: Arc<ControlPlane>) -> Self {
        self.control = Some(control);
        self
    }

    fn raft_control(&self) -> Option<&Arc<ControlPlane>> {
        self.control.as_ref().filter(|c| c.is_raft())
    }
}

/// 控制响应/写错误 → lifecycle 错误（调用方只做粗粒度映射）。
fn control_error(
    r: Result<ControlResponse, crate::raft::control::ControlError>,
) -> Result<(), LifecycleError> {
    match r {
        Ok(ControlResponse::Ok) => Ok(()),
        Ok(ControlResponse::NotFound) => Err(LifecycleError::NoSuchBucket),
        Ok(_) => Err(LifecycleError::InvalidArgument(
            "control write rejected".into(),
        )),
        Err(e) => Err(LifecycleError::Internal(e.to_string())),
    }
}

/// DB 行 → 规则。`config_json` 存在时以其为准（坏 blob 返回 Err，绝不退化成空 Filter）；
/// 否则按旧列还原。M4 遗留行 `rule_id IS NULL` 由调用方补合成 ID。
pub(crate) fn spec_from_row(r: &LifecycleRuleRow) -> Result<LifecycleRuleSpec, LifecycleError> {
    if let Some(json) = r.config_json.as_deref().filter(|j| !j.trim().is_empty()) {
        return from_config_json(json, r.rule_id.clone(), r.enabled);
    }
    let expiration_days = r.expiration_days.or(if r.expire_days >= 0 {
        Some(r.expire_days)
    } else {
        None
    });
    LifecycleRuleSpec::from_legacy_row(
        r.rule_id.clone(),
        r.prefix.clone(),
        expiration_days,
        r.transition_days,
        r.transition_class.clone(),
        r.transition_tier.clone(),
        r.enabled,
    )
}

/// 规则 → DB 行：旧列始终写（旧读端/降级可读），无法完整表达时同时写 `config_json`。
fn row_from_spec(bucket: &str, s: &LifecycleRuleSpec) -> Result<LifecycleRuleRow, LifecycleError> {
    let first = s.transitions.first();
    Ok(LifecycleRuleRow {
        bucket: bucket.to_string(),
        rule_id: s.rule_id.clone(),
        prefix: s.prefix().to_string(),
        expire_days: s.expiration_days().map(i64::from).unwrap_or(-1),
        expiration_days: s.expiration_days().map(i64::from),
        transition_days: first.and_then(|t| t.days).map(i64::from),
        transition_class: first.map(|t| t.class.clone()),
        transition_tier: first.and_then(|t| t.tier.clone()),
        enabled: s.enabled,
        updated_at: 0, // set_lifecycle_rules 统一填 now
        config_json: s.config_json()?,
    })
}

#[async_trait::async_trait]
impl LifecycleStore for SqlLifecycleStore {
    async fn get_rules(
        &self,
        bucket: &str,
    ) -> Result<Option<Vec<LifecycleRuleSpec>>, LifecycleError> {
        if !self
            .db
            .bucket_exists(bucket)
            .await
            .map_err(|e| LifecycleError::Internal(e.to_string()))?
        {
            return Err(LifecycleError::NoSuchBucket);
        }
        let rows = self
            .db
            .list_lifecycle_rules(bucket)
            .await
            .map_err(|e| LifecycleError::Internal(e.to_string()))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut used: HashSet<String> = rows.iter().filter_map(|r| r.rule_id.clone()).collect();
        let mut next_id = 1;
        rows.iter()
            .map(|r| {
                let mut spec = spec_from_row(r)?;
                if spec.rule_id.is_none() {
                    spec.rule_id = Some(loop {
                        let id = format!("rule-{next_id}");
                        next_id += 1;
                        if used.insert(id.clone()) {
                            break id;
                        }
                    });
                }
                Ok(spec)
            })
            .collect::<Result<Vec<_>, LifecycleError>>()
            .map_err(|e| LifecycleError::Internal(format!("规则配置损坏: {e}")))
            .map(Some)
    }

    async fn put_rules(
        &self,
        bucket: &str,
        rules: &[LifecycleRuleSpec],
    ) -> Result<(), LifecycleError> {
        if !self
            .db
            .bucket_exists(bucket)
            .await
            .map_err(|e| LifecycleError::Internal(e.to_string()))?
        {
            return Err(LifecycleError::NoSuchBucket);
        }
        // M9：sync 桶只允许 Expiration（Transition 无跨站语义）；版本级删除/删标记
        // 无法跨站复制，同样拒绝，避免两端状态分叉。
        let sync = self
            .db
            .bucket_sync_state(bucket)
            .await
            .map_err(|e| LifecycleError::Internal(e.to_string()))?
            .0
            == "sync";
        if sync
            && rules
                .iter()
                .any(|r| !r.transitions.is_empty() || !r.noncurrent_transitions.is_empty())
        {
            return Err(LifecycleError::InvalidArgument(
                "lifecycle Transition is not supported on sync buckets".into(),
            ));
        }
        if sync
            && rules.iter().any(|r| {
                r.noncurrent_expiration.is_some()
                    || matches!(
                        r.expiration,
                        Some(ExpirationAction::ExpiredObjectDeleteMarker)
                    )
            })
        {
            return Err(LifecycleError::InvalidArgument(
                "lifecycle version-specific deletion is not supported on sync buckets".into(),
            ));
        }
        // 两个入口都从这里过：clone 后统一校验（就地补全 rule_id/transition_tier）。
        let mut specs = rules.to_vec();
        validate_rules(&mut specs, &self.tier_ids)?;
        let rows = specs
            .iter()
            .map(|s| row_from_spec(bucket, s))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(control) = self.raft_control() {
            let updated_at = crate::db::now();
            // 只有结构化 blob 才需要 V2；旧列可表达的写继续走 V1，保持旧副本可解码。
            return if rows.iter().any(|r| r.config_json.is_some()) {
                let rules = rows.iter().map(lifecycle_cmd_v2).collect();
                control_error(
                    control
                        .write(ControlCommand::BucketSetLifecycleV2 {
                            bucket: bucket.to_string(),
                            rules,
                            updated_at,
                        })
                        .await,
                )
            } else {
                let rules = rows.iter().map(lifecycle_cmd_v1).collect();
                control_error(
                    control
                        .write(ControlCommand::BucketSetLifecycle {
                            bucket: bucket.to_string(),
                            rules,
                            updated_at,
                        })
                        .await,
                )
            };
        }
        self.db
            .set_lifecycle_rules(bucket, &rows, crate::db::now())
            .await
            .map_err(|e| LifecycleError::Internal(e.to_string()))
    }

    async fn delete_rules(&self, bucket: &str) -> Result<(), LifecycleError> {
        if !self
            .db
            .bucket_exists(bucket)
            .await
            .map_err(|e| LifecycleError::Internal(e.to_string()))?
        {
            return Err(LifecycleError::NoSuchBucket);
        }
        if let Some(control) = self.raft_control() {
            return control_error(
                control
                    .write(ControlCommand::BucketClearLifecycle {
                        bucket: bucket.to_string(),
                    })
                    .await,
            );
        }
        self.db
            .set_lifecycle_rules(bucket, &[], crate::db::now())
            .await
            .map_err(|e| LifecycleError::Internal(e.to_string()))
    }
}

fn lifecycle_cmd_v1(r: &LifecycleRuleRow) -> LifecycleRuleCmd {
    LifecycleRuleCmd {
        rule_id: r.rule_id.clone(),
        prefix: r.prefix.clone(),
        expire_days: r.expire_days,
        expiration_days: r.expiration_days,
        transition_days: r.transition_days,
        transition_class: r.transition_class.clone(),
        transition_tier: r.transition_tier.clone(),
        enabled: r.enabled,
    }
}

fn lifecycle_cmd_v2(r: &LifecycleRuleRow) -> LifecycleRuleCmdV2 {
    LifecycleRuleCmdV2 {
        rule_id: r.rule_id.clone(),
        prefix: r.prefix.clone(),
        expire_days: r.expire_days,
        expiration_days: r.expiration_days,
        transition_days: r.transition_days,
        transition_class: r.transition_class.clone(),
        transition_tier: r.transition_tier.clone(),
        enabled: r.enabled,
        config_json: r.config_json.clone(),
    }
}
