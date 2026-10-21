//! M8 S4b：`LifecycleStore` 的 SQL 实现（xl-s3 trait）——console 与 S3 共用。
//!
//! S4b 只做 Spec ↔ `LifecycleRuleRow` 映射；校验统一走 `validate_rules`。

use std::sync::Arc;

use xl_s3::lifecycle::{LifecycleError, LifecycleRuleSpec, LifecycleStore, validate_rules};

use crate::db::{Db, LifecycleRuleRow};
use crate::raft::command::{ControlCommand, ControlResponse, LifecycleRuleCmd};
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

/// 旧 M4 行：`rule_id IS NULL` 按 `(prefix)` 序号合成 `rule-<n>`（只读，不回写）。
fn spec_from_row(
    r: &LifecycleRuleRow,
    next_id: &mut usize,
    used: &mut std::collections::HashSet<String>,
) -> LifecycleRuleSpec {
    let expiration_days = r.expiration_days.or(if r.expire_days >= 0 {
        Some(r.expire_days)
    } else {
        None
    });
    let rule_id = match &r.rule_id {
        Some(id) => id.clone(),
        None => loop {
            let id = format!("rule-{next_id}");
            *next_id += 1;
            if used.insert(id.clone()) {
                break id;
            }
        },
    };
    LifecycleRuleSpec {
        rule_id: Some(rule_id),
        prefix: r.prefix.clone(),
        expiration_days: expiration_days.and_then(|d| u32::try_from(d).ok()),
        transition_days: r.transition_days.and_then(|d| u32::try_from(d).ok()),
        transition_class: r.transition_class.clone(),
        transition_tier: r.transition_tier.clone(),
        enabled: r.enabled,
    }
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
        let mut used: std::collections::HashSet<String> =
            rows.iter().filter_map(|r| r.rule_id.clone()).collect();
        let mut next_id = 1;
        Ok(Some(
            rows.iter()
                .map(|r| spec_from_row(r, &mut next_id, &mut used))
                .collect(),
        ))
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
        // M9：sync 桶只允许 Expiration（Transition 无跨站语义）。
        if rules
            .iter()
            .any(|r| r.transition_days.is_some() || r.transition_class.is_some())
            && self
                .db
                .bucket_sync_state(bucket)
                .await
                .map_err(|e| LifecycleError::Internal(e.to_string()))?
                .0
                == "sync"
        {
            return Err(LifecycleError::InvalidArgument(
                "lifecycle Transition is not supported on sync buckets".into(),
            ));
        }
        // 两个入口都从这里过：clone 后统一校验（就地补全 rule_id/transition_tier）。
        let mut specs = rules.to_vec();
        validate_rules(&mut specs, &self.tier_ids)?;
        let rows: Vec<LifecycleRuleRow> = specs
            .iter()
            .map(|s| LifecycleRuleRow {
                bucket: bucket.to_string(),
                rule_id: s.rule_id.clone(),
                prefix: s.prefix.clone(),
                expire_days: s.expiration_days.map(i64::from).unwrap_or(-1),
                expiration_days: s.expiration_days.map(i64::from),
                transition_days: s.transition_days.map(i64::from),
                transition_class: s.transition_class.clone(),
                transition_tier: s.transition_tier.clone(),
                enabled: s.enabled,
                updated_at: 0, // set_lifecycle_rules 统一填 now
            })
            .collect();
        if let Some(control) = self.raft_control() {
            let rules = rows
                .iter()
                .map(|r| LifecycleRuleCmd {
                    rule_id: r.rule_id.clone(),
                    prefix: r.prefix.clone(),
                    expire_days: r.expire_days,
                    expiration_days: r.expiration_days,
                    transition_days: r.transition_days,
                    transition_class: r.transition_class.clone(),
                    transition_tier: r.transition_tier.clone(),
                    enabled: r.enabled,
                })
                .collect();
            return control_error(
                control
                    .write(ControlCommand::BucketSetLifecycle {
                        bucket: bucket.to_string(),
                        rules,
                        updated_at: crate::db::now(),
                    })
                    .await,
            );
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
