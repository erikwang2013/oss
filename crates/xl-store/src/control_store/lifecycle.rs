//! lifecycle 域行类型与后端中立读写 trait。

use serde::{Deserialize, Serialize};

use super::ControlError;

/// 生命周期规则行；字段声明顺序即 `ControlData` 快照顺序（已发布快照不可改写）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleRuleEntry {
    pub bucket: String,
    pub prefix: String,
    pub rule_id: Option<String>,
    pub expire_days: i64,
    pub expiration_days: Option<i64>,
    pub transition_days: Option<i64>,
    pub transition_class: Option<String>,
    pub transition_tier: Option<String>,
    pub enabled: bool,
    pub updated_at: i64,
    #[serde(default)]
    pub config_json: Option<String>,
}

/// 生命周期游标行（`m8.runstate` 扩展；字段顺序即 rmp 字节顺序）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LifecycleCursorRow {
    pub bucket: String,
    pub rule_id: String,
    pub stage: String,
    pub cursor: String,
    pub updated_at: i64,
    pub fence_term: i64,
    pub fence_leader: i64,
}

/// 只读 lifecycle 访问（console/engine 共用同一 SQL）。
#[async_trait::async_trait]
pub trait LifecycleStore: Send + Sync {
    /// 按 `(rule_id, prefix)` 升序（NULL `rule_id` 由 SQLite 排最前）。
    async fn lifecycle_rules(&self, bucket: &str) -> Result<Vec<LifecycleRuleEntry>, ControlError>;
    /// `enabled = 1`，按 `(bucket, rule_id)` 升序。
    async fn enabled_lifecycle_rules(&self) -> Result<Vec<LifecycleRuleEntry>, ControlError>;
    async fn lifecycle_cursor(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<String>, ControlError>;
    /// 整行读（job_state/apply 对账需要 fence 字段）。
    async fn lifecycle_cursor_row(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<LifecycleCursorRow>, ControlError>;
}

/// 调用方事务内的 lifecycle 写访问；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait LifecycleTxn: Send {
    /// DELETE 本桶 rules + cursors，再按入参顺序 INSERT；行的 `updated_at` 原样写入。
    async fn replace_lifecycle_rules(
        &mut self,
        bucket: &str,
        rows: &[LifecycleRuleEntry],
    ) -> Result<(), ControlError>;
    /// DELETE 全部 rules（快照安装专用；不碰 cursors）。
    async fn replace_all_lifecycle_rules(
        &mut self,
        rows: &[LifecycleRuleEntry],
    ) -> Result<(), ControlError>;
    /// DELETE 本桶 rules + cursors（PUT 空列表/清规则）。
    async fn clear_lifecycle_rules(&mut self, bucket: &str) -> Result<(), ControlError>;
    /// 只 DELETE rules（site 同步桶删除保持既有范围）。
    async fn delete_lifecycle_rules_only(&mut self, bucket: &str) -> Result<(), ControlError>;
    /// 快照导出：按 `(bucket, rule_id, prefix)` 升序。
    async fn all_lifecycle_rules(&mut self) -> Result<Vec<LifecycleRuleEntry>, ControlError>;
    async fn lifecycle_cursor(
        &mut self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<LifecycleCursorRow>, ControlError>;
    /// 返回是否恰好影响 1 行。
    async fn lifecycle_cursor_upsert(
        &mut self,
        row: &LifecycleCursorRow,
    ) -> Result<bool, ControlError>;
    async fn lifecycle_cursor_delete(
        &mut self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
        fence_term: i64,
    ) -> Result<(), ControlError>;
    /// 快照导出：按 `(bucket, rule_id, stage)` 升序。
    async fn lifecycle_cursors(&mut self) -> Result<Vec<LifecycleCursorRow>, ControlError>;
    async fn replace_lifecycle_cursors(
        &mut self,
        rows: &[LifecycleCursorRow],
    ) -> Result<(), ControlError>;
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    /// 旧快照行（与 `raft/data.rs` 的 `LifecycleRuleRow` 声明顺序一致）。
    #[derive(Serialize)]
    struct LegacyRule {
        bucket: String,
        prefix: String,
        rule_id: Option<String>,
        expire_days: i64,
        expiration_days: Option<i64>,
        transition_days: Option<i64>,
        transition_class: Option<String>,
        transition_tier: Option<String>,
        enabled: bool,
        updated_at: i64,
        config_json: Option<String>,
    }

    fn rule() -> super::LifecycleRuleEntry {
        super::LifecycleRuleEntry {
            bucket: "b1".into(),
            prefix: "logs/".into(),
            rule_id: Some("r1".into()),
            expire_days: -1,
            expiration_days: Some(30),
            transition_days: Some(7),
            transition_class: Some("GLACIER".into()),
            transition_tier: Some("ARCHIVE".into()),
            enabled: true,
            updated_at: 11,
            config_json: Some(r#"{"actions":[]}"#.into()),
        }
    }

    #[test]
    fn lifecycle_rule_rmp_matches_snapshot_field_order() {
        let row = rule();
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let legacy = LegacyRule {
            bucket: row.bucket.clone(),
            prefix: row.prefix.clone(),
            rule_id: row.rule_id.clone(),
            expire_days: row.expire_days,
            expiration_days: row.expiration_days,
            transition_days: row.transition_days,
            transition_class: row.transition_class.clone(),
            transition_tier: row.transition_tier.clone(),
            enabled: row.enabled,
            updated_at: row.updated_at,
            config_json: row.config_json.clone(),
        };
        assert_eq!(
            bytes,
            rmp_serde::to_vec_named(&legacy).unwrap(),
            "字段名与顺序必须与旧快照行逐字节一致"
        );
        assert_eq!(
            rmp_serde::from_slice::<super::LifecycleRuleEntry>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn lifecycle_rule_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            bucket: String,
            prefix: String,
            rule_id: Option<String>,
            expire_days: i64,
            expiration_days: Option<i64>,
            transition_days: Option<i64>,
            transition_class: Option<String>,
            transition_tier: Option<String>,
            enabled: bool,
            updated_at: i64,
            config_json: Option<String>,
            future_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            bucket: "b".into(),
            prefix: "p".into(),
            rule_id: None,
            expire_days: -1,
            expiration_days: None,
            transition_days: None,
            transition_class: None,
            transition_tier: None,
            enabled: false,
            updated_at: 1,
            config_json: None,
            future_field: 9,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<super::LifecycleRuleEntry>(&bytes).is_err());
    }

    /// 旧 `m8.runstate` 扩展行（与 `raft/runstate/model.rs` 声明顺序一致）。
    #[derive(Serialize)]
    struct LegacyCursor {
        bucket: String,
        rule_id: String,
        stage: String,
        cursor: String,
        updated_at: i64,
        fence_term: i64,
        fence_leader: i64,
    }

    #[test]
    fn lifecycle_cursor_rmp_matches_runstate_extension() {
        let row = super::LifecycleCursorRow {
            bucket: "b1".into(),
            rule_id: "r1".into(),
            stage: "versions".into(),
            cursor: "c1".into(),
            updated_at: 5,
            fence_term: 6,
            fence_leader: 7,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let legacy = LegacyCursor {
            bucket: row.bucket.clone(),
            rule_id: row.rule_id.clone(),
            stage: row.stage.clone(),
            cursor: row.cursor.clone(),
            updated_at: row.updated_at,
            fence_term: row.fence_term,
            fence_leader: row.fence_leader,
        };
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::LifecycleCursorRow>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn lifecycle_cursor_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            bucket: String,
            rule_id: String,
            stage: String,
            cursor: String,
            updated_at: i64,
            fence_term: i64,
            fence_leader: i64,
            future_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            bucket: "b".into(),
            rule_id: "r".into(),
            stage: "s".into(),
            cursor: "c".into(),
            updated_at: 1,
            fence_term: 2,
            fence_leader: 3,
            future_field: 4,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<super::LifecycleCursorRow>(&bytes).is_err());
    }
}
