//! 控制面后端中立缝：`ControlStore` 是只读句柄（共享后端连接池），`ControlTxn`
//! 是调用方事务内的写句柄——本模块不 begin/commit/rollback，事务边界与原子性
//! 只由调用方（Raft apply / 快照安装）持有。
//!
//! 已迁移 `vpc_endpoints` / `access_points` / `olap_configs`。这些类型的字段名与
//! 声明顺序必须与 `oss-server/src/raft/data.rs` 的旧快照行完全一致（已发布快照
//! 不可改写）；增删字段必须同步升级快照格式。

use serde::{Deserialize, Serialize};

pub mod account;
pub mod audit;
pub mod bucket;
pub mod event_outbox;
pub mod iam;
pub mod inventory;
pub mod jobs;
pub mod lifecycle;
pub mod repl_outbox;
pub mod sites;
pub mod sync_intents;
pub mod tier;
pub mod uploads;

pub use account::{AccountEntry, AccountMemberEntry, AccountUsageLedgerEntry};
pub use bucket::{BucketEntry, BucketPolicyEntry, EventConfigEntry, ReplicationRuleEntry};
pub use iam::{AccessKeyEntry, PermissionEntry, UserEntry};
pub use inventory::InventoryConfigEntry;
pub use jobs::{JobRow, JobTaskRow, ScheduleLeaseRow};
pub use lifecycle::LifecycleRuleEntry;

use account::{AccountSnapshotTxn, AccountStore, AccountTxn};
use audit::{AuditStore, AuditTxn};
use bucket::{BucketSnapshotTxn, BucketStore, BucketTxn};
use event_outbox::{EventOutboxStore, EventOutboxTxn};
use iam::{IamSnapshotTxn, IamStore, IamTxn};
use inventory::{InventoryStore, InventoryTxn};
use jobs::{JobSnapshotTxn, JobStore, JobTxn};
use lifecycle::{LifecycleStore, LifecycleTxn};
use repl_outbox::{ReplOutboxStore, ReplOutboxTxn};
use sites::{SiteStore, SiteTxn};
use sync_intents::{SyncIntentStore, SyncIntentTxn};
use tier::{TierStore, TierTxn};
use uploads::{UploadStore, UploadTxn};

/// 属主自然键：`Admin` = 目标站点本地管理员（buckets → NULL；AP/inventory → 本地
/// 管理员 id，witness 写哨兵 0）；`User(username)` = 复制用户。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case", tag = "kind", content = "username")]
pub enum OwnerRef {
    Admin,
    User(String),
}

/// 访问点行（字段顺序与旧 `AccessPointRow` 快照逐字节一致）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessPointEntry {
    pub name: String,
    pub bucket: String,
    pub owner: OwnerRef,
    pub policy_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 变换器配置行（`module` 为原始 BLOB；快照层自行 base64）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OlapConfigEntry {
    pub access_point: String,
    pub kind: String,
    pub endpoint: Option<String>,
    pub module: Option<Vec<u8>>,
    pub module_sha256: Option<String>,
    pub timeout_ms: i64,
    pub max_response_bytes: i64,
    pub updated_at: i64,
}

/// feature 8：动态 VPC 端点注册行。
///
/// 字段顺序/Schema 与旧 `VpcEndpointRow` 逐字节兼容；`deny_unknown_fields`
/// 保证旧快照不会静默丢弃未知数据。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VpcEndpoint {
    pub vpce_id: String,
    pub vpc_id: String,
    pub account_id: String,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 后端中立错误：`Backend` = 存储层失败（连接/SQL/IO），`Invalid` = 数据本身
/// 不满足控制面约束。上层据此映射 HTTP 状态，不泄露后端类型。
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("control store backend: {0}")]
    Backend(String),
    #[error("invalid control data: {0}")]
    Invalid(String),
    /// 严格快照导出遇到悬空属主：文本与旧 `DataError::MissingUser` 一致（无前缀）。
    #[error("owner user id {0} missing")]
    MissingOwner(i64),
}

/// 只读控制面访问。实现必须复用 `Db` 的既有连接池，不得自建第二个池。
#[async_trait::async_trait]
pub trait ControlStore:
    BucketStore
    + LifecycleStore
    + InventoryStore
    + TierStore
    + JobStore
    + AuditStore
    + IamStore
    + AccountStore
    + EventOutboxStore
    + ReplOutboxStore
    + SiteStore
    + SyncIntentStore
    + UploadStore
    + Send
    + Sync
{
    async fn vpc_endpoint(&self, vpce_id: &str) -> Result<Option<VpcEndpoint>, ControlError>;
    /// 按 `vpce_id` 升序（导出/校验的确定性来源）。
    async fn vpc_endpoints(&self) -> Result<Vec<VpcEndpoint>, ControlError>;
    async fn access_point(&self, name: &str) -> Result<Option<AccessPointEntry>, ControlError>;
    /// 按 `name` 升序。
    async fn access_points(&self) -> Result<Vec<AccessPointEntry>, ControlError>;
    async fn olap_config(
        &self,
        access_point: &str,
    ) -> Result<Option<OlapConfigEntry>, ControlError>;
    /// 按 `access_point` 升序。
    async fn olap_configs(&self) -> Result<Vec<OlapConfigEntry>, ControlError>;
}

/// 调用方事务内的写访问。`&mut self` 表示独占借用，不提供 begin/commit/rollback。
#[async_trait::async_trait]
pub trait ControlTxn:
    BucketTxn
    + BucketSnapshotTxn
    + LifecycleTxn
    + InventoryTxn
    + TierTxn
    + JobTxn
    + JobSnapshotTxn
    + AuditTxn
    + IamTxn
    + IamSnapshotTxn
    + AccountTxn
    + AccountSnapshotTxn
    + EventOutboxTxn
    + ReplOutboxTxn
    + SiteTxn
    + SyncIntentTxn
    + UploadTxn
    + Send
{
    async fn vpc_endpoint_upsert(&mut self, row: &VpcEndpoint) -> Result<(), ControlError>;
    /// 返回是否确有行被删除。
    async fn vpc_endpoint_delete(&mut self, vpce_id: &str) -> Result<bool, ControlError>;
    /// 按 `vpce_id` 升序。
    async fn vpc_endpoints(&mut self) -> Result<Vec<VpcEndpoint>, ControlError>;
    /// 整表替换（DELETE all + 按入参顺序 INSERT）。
    async fn replace_vpc_endpoints(&mut self, rows: &[VpcEndpoint]) -> Result<(), ControlError>;

    /// 建 AP；同名已存在 → `Ok(false)`（不覆盖）。
    async fn access_point_create(&mut self, row: &AccessPointEntry) -> Result<bool, ControlError>;
    async fn access_point_set_policy(
        &mut self,
        name: &str,
        policy_json: Option<&str>,
        updated_at: i64,
    ) -> Result<bool, ControlError>;
    /// 先删该 AP 的 olap 行，再删 AP；返回是否确有 AP 行被删除。
    async fn access_point_delete(&mut self, name: &str) -> Result<bool, ControlError>;
    async fn access_point_exists(&mut self, name: &str) -> Result<bool, ControlError>;
    /// 严格导出映射（`is_admin=1` → `Admin`；悬空属主 → `Invalid`）。
    async fn access_points(&mut self) -> Result<Vec<AccessPointEntry>, ControlError>;
    /// 整表替换：DELETE olap + AP，再按入参顺序 INSERT。
    async fn replace_access_points(
        &mut self,
        rows: &[AccessPointEntry],
    ) -> Result<(), ControlError>;
    async fn olap_config_upsert(&mut self, row: &OlapConfigEntry) -> Result<(), ControlError>;
    async fn olap_config_delete(&mut self, access_point: &str) -> Result<bool, ControlError>;
    async fn olap_configs(&mut self) -> Result<Vec<OlapConfigEntry>, ControlError>;
    /// 只 DELETE olap_configs，再按入参顺序 INSERT。
    async fn replace_olap_configs(&mut self, rows: &[OlapConfigEntry]) -> Result<(), ControlError>;
    /// 删该桶下全部 AP 及其 olap 行（桶删除级联）。
    async fn delete_access_points_for_bucket(&mut self, bucket: &str) -> Result<(), ControlError>;
    /// 删该桶的 7 张表：inventory_runs、inventory_configs、inventory_cursors、
    /// inventory_incremental、tier_restores、lifecycle_rules、lifecycle_cursors。
    async fn delete_lifecycle_inventory_tier_for_bucket(
        &mut self,
        bucket: &str,
    ) -> Result<(), ControlError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 旧快照行的字段顺序/名字（与 raft/data.rs 的 VpcEndpointRow 一致）。
    #[derive(Serialize)]
    struct LegacyRow {
        vpce_id: String,
        vpc_id: String,
        account_id: String,
        state: String,
        created_at: i64,
        updated_at: i64,
    }

    fn row() -> VpcEndpoint {
        VpcEndpoint {
            vpce_id: "vpce-1".into(),
            vpc_id: "vpc-1".into(),
            account_id: "acct".into(),
            state: "available".into(),
            created_at: 1,
            updated_at: 2,
        }
    }

    #[test]
    fn vpc_endpoint_rmp_field_order_matches_snapshot() {
        let row = row();
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let decoded: VpcEndpoint = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(row, decoded);

        let legacy = LegacyRow {
            vpce_id: row.vpce_id.clone(),
            vpc_id: row.vpc_id.clone(),
            account_id: row.account_id.clone(),
            state: row.state.clone(),
            created_at: row.created_at,
            updated_at: row.updated_at,
        };
        assert_eq!(
            bytes,
            rmp_serde::to_vec_named(&legacy).unwrap(),
            "字段名与顺序必须与旧快照行逐字节一致"
        );
    }

    /// `deny_unknown_fields`：旧快照之外的多余字段整行拒绝，绝不静默丢弃。
    #[test]
    fn vpc_endpoint_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            vpce_id: String,
            vpc_id: String,
            account_id: String,
            state: String,
            created_at: i64,
            updated_at: i64,
            future_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            vpce_id: "v".into(),
            vpc_id: "v".into(),
            account_id: "a".into(),
            state: "active".into(),
            created_at: 1,
            updated_at: 2,
            future_field: 3,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<VpcEndpoint>(&bytes).is_err());
    }

    /// 读句柄与写句柄都必须可作 trait object 使用（Arc<dyn ControlStore> /
    /// Box<dyn ControlTxn>）。
    #[test]
    fn control_traits_are_object_safe() {
        fn store(_: std::sync::Arc<dyn ControlStore>) {}
        fn txn(_: Box<dyn ControlTxn>) {}
        let _ = (store, txn);
    }

    /// 旧快照 AP 行的字段顺序/名字（与 raft/data.rs 的 `AccessPointRow` 一致）。
    #[derive(Serialize)]
    struct LegacyApRow {
        name: String,
        bucket: String,
        owner: OwnerRef,
        policy_json: Option<String>,
        created_at: i64,
        updated_at: i64,
    }

    #[test]
    fn owner_ref_json_matches_snapshot_format() {
        assert_eq!(
            serde_json::to_string(&OwnerRef::Admin).unwrap(),
            r#"{"kind":"admin"}"#
        );
        assert_eq!(
            serde_json::to_string(&OwnerRef::User("alice".into())).unwrap(),
            r#"{"kind":"user","username":"alice"}"#
        );
        assert!(serde_json::from_str::<OwnerRef>(r#"{"kind":"admin","username":"x"}"#).is_err());
    }

    #[test]
    fn access_point_entry_rmp_field_order_matches_snapshot() {
        let row = AccessPointEntry {
            name: "ap1".into(),
            bucket: "b1".into(),
            owner: OwnerRef::User("alice".into()),
            policy_json: Some(r#"{"Statement":[]}"#.into()),
            created_at: 1,
            updated_at: 2,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let legacy = LegacyApRow {
            name: row.name.clone(),
            bucket: row.bucket.clone(),
            owner: row.owner.clone(),
            policy_json: row.policy_json.clone(),
            created_at: row.created_at,
            updated_at: row.updated_at,
        };
        assert_eq!(
            bytes,
            rmp_serde::to_vec_named(&legacy).unwrap(),
            "字段名与顺序必须与旧快照行逐字节一致"
        );
        assert_eq!(
            rmp_serde::from_slice::<AccessPointEntry>(&bytes).unwrap(),
            row
        );
        let json = serde_json::to_vec(&row).unwrap();
        assert_eq!(
            serde_json::from_slice::<AccessPointEntry>(&json).unwrap(),
            row
        );
    }

    #[test]
    fn control_error_messages_are_backend_neutral() {
        assert_eq!(
            ControlError::Backend("x".into()).to_string(),
            "control store backend: x"
        );
        assert_eq!(
            ControlError::Invalid("y".into()).to_string(),
            "invalid control data: y"
        );
    }
}
