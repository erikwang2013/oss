//! 桶 / 桶策略 / 事件通知 / 复制规则的后端中立行与读写 trait。
//!
//! `BucketEntry` 字段顺序即快照字节顺序；`BucketView.owner_uid/owner_is_admin`
//! 是运行期本地投影，不是 seam 身份键。`BucketTxn` / `BucketSnapshotTxn` 的调用方
//! 持有事务边界，适配器绝不 begin/commit/rollback。

use serde::{Deserialize, Serialize};

use super::{ControlError, OwnerRef};

/// 桶快照行（自然键 `name`；不含本地 `set_id`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketEntry {
    pub name: String,
    pub owner: OwnerRef,
    pub creation_date: i64,
    pub versioning: String,
    pub object_lock: bool,
    pub default_encryption: Option<String>,
    pub object_lock_default_mode: Option<String>,
    pub object_lock_default_days: Option<i64>,
    pub replication_mode: String,
    pub deleting: bool,
}

/// 运行期桶视图：属主解析为用户行（`None` = 悬空/无属主），另带本地投影。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketView {
    pub name: String,
    pub owner: Option<OwnerRef>,
    /// 本地外键投影（原 `buckets.owner_id`）；不是 seam 身份键。
    pub owner_uid: Option<i64>,
    /// 桶主是否管理员（本地投影；账户归系统账户）。
    pub owner_is_admin: bool,
    pub creation_date: i64,
    pub versioning: String,
    pub object_lock: bool,
    pub default_encryption: Option<String>,
    pub object_lock_default_mode: Option<String>,
    pub object_lock_default_days: Option<i64>,
    pub replication_mode: String,
    pub deleting: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketPolicyEntry {
    pub bucket: String,
    pub policy_json: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventConfigEntry {
    pub bucket: String,
    pub config_json: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicationRuleEntry {
    pub bucket: String,
    pub rules_json: String,
    pub updated_at: i64,
}

/// `bucket_create` 的三态结果（不进快照）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketCreateOutcome {
    Created,
    Updated,
    ModeConflict,
}

/// 只读桶访问。
#[async_trait::async_trait]
pub trait BucketStore: Send + Sync {
    async fn bucket(&self, name: &str) -> Result<Option<BucketView>, ControlError>;
    /// `name` 升序。
    async fn buckets(&self) -> Result<Vec<BucketView>, ControlError>;
    async fn bucket_policy(&self, bucket: &str) -> Result<Option<BucketPolicyEntry>, ControlError>;
    /// `bucket` 升序。
    async fn bucket_policies(&self) -> Result<Vec<BucketPolicyEntry>, ControlError>;
    async fn bucket_notification(
        &self,
        bucket: &str,
    ) -> Result<Option<EventConfigEntry>, ControlError>;
    /// `bucket` 升序。
    async fn event_configs(&self) -> Result<Vec<EventConfigEntry>, ControlError>;
    async fn bucket_replication_rules(
        &self,
        bucket: &str,
    ) -> Result<Option<ReplicationRuleEntry>, ControlError>;
    /// `bucket` 升序。
    async fn replication_rules(&self) -> Result<Vec<ReplicationRuleEntry>, ControlError>;
}

/// 调用方事务内的桶点写；本 trait 不 begin/commit/rollback。
#[async_trait::async_trait]
pub trait BucketTxn: Send {
    async fn bucket_create(&mut self, row: &BucketEntry)
    -> Result<BucketCreateOutcome, ControlError>;
    async fn bucket_delete(&mut self, name: &str) -> Result<bool, ControlError>;
    async fn bucket_set_deleting(
        &mut self,
        name: &str,
        deleting: bool,
    ) -> Result<bool, ControlError>;
    async fn bucket_set_versioning(
        &mut self,
        name: &str,
        versioning: &str,
    ) -> Result<bool, ControlError>;
    async fn bucket_set_object_lock(
        &mut self,
        name: &str,
        object_lock: bool,
        default_mode: Option<&str>,
        default_days: Option<i64>,
    ) -> Result<bool, ControlError>;
    async fn bucket_set_default_encryption(
        &mut self,
        name: &str,
        algorithm: &str,
    ) -> Result<bool, ControlError>;
    async fn bucket_clear_default_encryption(
        &mut self,
        name: &str,
    ) -> Result<bool, ControlError>;
    async fn bucket_policy_set(
        &mut self,
        bucket: &str,
        policy_json: &str,
        updated_at: i64,
    ) -> Result<(), ControlError>;
    async fn bucket_policy_delete(&mut self, bucket: &str) -> Result<(), ControlError>;
    async fn bucket_notification_set(
        &mut self,
        bucket: &str,
        config_json: &str,
        updated_at: i64,
    ) -> Result<(), ControlError>;
    async fn bucket_notification_delete(&mut self, bucket: &str) -> Result<(), ControlError>;
    async fn bucket_replication_rules_set(
        &mut self,
        bucket: &str,
        rules_json: &str,
        updated_at: i64,
    ) -> Result<(), ControlError>;
    async fn bucket_replication_rules_delete(&mut self, bucket: &str)
    -> Result<(), ControlError>;
    async fn bucket_exists(&mut self, name: &str) -> Result<bool, ControlError>;
    /// 删该桶的策略/事件/复制规则（桶删除级联）。
    async fn delete_bucket_configs_for_bucket(&mut self, bucket: &str)
    -> Result<(), ControlError>;
}

/// 快照导出/安装（调用方事务内）。
#[async_trait::async_trait]
pub trait BucketSnapshotTxn: Send {
    /// 严格导出映射；悬空用户 → `MissingOwner`。`name` 升序。
    async fn buckets(&mut self) -> Result<Vec<BucketEntry>, ControlError>;
    async fn bucket_policies(&mut self) -> Result<Vec<BucketPolicyEntry>, ControlError>;
    async fn replace_bucket_policies(
        &mut self,
        rows: &[BucketPolicyEntry],
    ) -> Result<(), ControlError>;
    async fn event_configs(&mut self) -> Result<Vec<EventConfigEntry>, ControlError>;
    async fn replace_event_configs(&mut self, rows: &[EventConfigEntry])
    -> Result<(), ControlError>;
    async fn replication_rules(&mut self) -> Result<Vec<ReplicationRuleEntry>, ControlError>;
    async fn replace_replication_rules(
        &mut self,
        rows: &[ReplicationRuleEntry],
    ) -> Result<(), ControlError>;
    /// 删目标独有桶，再 `UPDATE buckets SET owner_id = NULL`。
    async fn bucket_install_prepare(&mut self, wanted: &[String]) -> Result<(), ControlError>;
    /// `INSERT ... ON CONFLICT(name) DO UPDATE`（不含 `set_id`）。
    async fn bucket_upsert_install(&mut self, row: &BucketEntry) -> Result<(), ControlError>;
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    #[derive(Serialize)]
    struct LegacyBucket {
        name: String,
        owner: super::OwnerRef,
        creation_date: i64,
        versioning: String,
        object_lock: bool,
        default_encryption: Option<String>,
        object_lock_default_mode: Option<String>,
        object_lock_default_days: Option<i64>,
        replication_mode: String,
        deleting: bool,
    }

    fn bucket() -> super::BucketEntry {
        super::BucketEntry {
            name: "b1".into(),
            owner: super::OwnerRef::User("alice".into()),
            creation_date: 1,
            versioning: "Enabled".into(),
            object_lock: true,
            default_encryption: Some("AES256".into()),
            object_lock_default_mode: Some("GOVERNANCE".into()),
            object_lock_default_days: Some(7),
            replication_mode: "async".into(),
            deleting: false,
        }
    }

    #[test]
    fn bucket_entry_rmp_matches_snapshot_field_order() {
        let row = bucket();
        let legacy = LegacyBucket {
            name: row.name.clone(),
            owner: row.owner.clone(),
            creation_date: row.creation_date,
            versioning: row.versioning.clone(),
            object_lock: row.object_lock,
            default_encryption: row.default_encryption.clone(),
            object_lock_default_mode: row.object_lock_default_mode.clone(),
            object_lock_default_days: row.object_lock_default_days,
            replication_mode: row.replication_mode.clone(),
            deleting: row.deleting,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::BucketEntry>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn bucket_entry_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            name: String,
            owner: super::OwnerRef,
            creation_date: i64,
            versioning: String,
            object_lock: bool,
            default_encryption: Option<String>,
            object_lock_default_mode: Option<String>,
            object_lock_default_days: Option<i64>,
            replication_mode: String,
            deleting: bool,
            future: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            name: "b1".into(),
            owner: super::OwnerRef::Admin,
            creation_date: 1,
            versioning: "Off".into(),
            object_lock: false,
            default_encryption: None,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            replication_mode: "async".into(),
            deleting: false,
            future: 1,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<super::BucketEntry>(&bytes).is_err());
    }

    #[derive(Serialize)]
    struct LegacyPolicy {
        bucket: String,
        policy_json: String,
        updated_at: i64,
    }

    #[test]
    fn bucket_policy_entry_rmp_matches_snapshot_field_order() {
        let row = super::BucketPolicyEntry {
            bucket: "b1".into(),
            policy_json: "{}".into(),
            updated_at: 2,
        };
        let legacy = LegacyPolicy {
            bucket: row.bucket.clone(),
            policy_json: row.policy_json.clone(),
            updated_at: row.updated_at,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::BucketPolicyEntry>(&bytes).unwrap(),
            row
        );
    }

    #[derive(Serialize)]
    struct LegacyEvent {
        bucket: String,
        config_json: String,
        updated_at: i64,
    }

    #[test]
    fn event_config_entry_rmp_matches_snapshot_field_order() {
        let row = super::EventConfigEntry {
            bucket: "b1".into(),
            config_json: "{}".into(),
            updated_at: 3,
        };
        let legacy = LegacyEvent {
            bucket: row.bucket.clone(),
            config_json: row.config_json.clone(),
            updated_at: row.updated_at,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::EventConfigEntry>(&bytes).unwrap(),
            row
        );
    }

    #[derive(Serialize)]
    struct LegacyReplication {
        bucket: String,
        rules_json: String,
        updated_at: i64,
    }

    #[test]
    fn replication_rule_entry_rmp_matches_snapshot_field_order() {
        let row = super::ReplicationRuleEntry {
            bucket: "b1".into(),
            rules_json: "[]".into(),
            updated_at: 4,
        };
        let legacy = LegacyReplication {
            bucket: row.bucket.clone(),
            rules_json: row.rules_json.clone(),
            updated_at: row.updated_at,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::ReplicationRuleEntry>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn missing_owner_error_display_matches_snapshot() {
        assert_eq!(
            super::ControlError::MissingOwner(999).to_string(),
            "owner user id 999 missing"
        );
    }
}
