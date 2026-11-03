//! 控制面后端中立缝：`ControlStore` 是只读句柄（共享后端连接池），`ControlTxn`
//! 是调用方事务内的写句柄——本模块不 begin/commit/rollback，事务边界与原子性
//! 只由调用方（Raft apply / 快照安装）持有。
//!
//! 第一切片只有 `vpc_endpoints`。`VpcEndpoint` 的字段名与声明顺序必须与
//! `oss-server/src/raft/data.rs` 的旧 `VpcEndpointRow` 完全一致（已发布快照
//! 不可改写）；增删字段必须同步升级快照格式。

use serde::{Deserialize, Serialize};

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
}

/// 只读控制面访问。实现必须复用 `Db` 的既有连接池，不得自建第二个池。
#[async_trait::async_trait]
pub trait ControlStore: Send + Sync {
    async fn vpc_endpoint(&self, vpce_id: &str) -> Result<Option<VpcEndpoint>, ControlError>;
    /// 按 `vpce_id` 升序（导出/校验的确定性来源）。
    async fn vpc_endpoints(&self) -> Result<Vec<VpcEndpoint>, ControlError>;
}

/// 调用方事务内的写访问。`&mut self` 表示独占借用，不提供 begin/commit/rollback。
#[async_trait::async_trait]
pub trait ControlTxn: Send {
    async fn account_status(&mut self, account_id: &str) -> Result<Option<String>, ControlError>;
    async fn vpc_endpoint_upsert(&mut self, row: &VpcEndpoint) -> Result<(), ControlError>;
    /// 返回是否确有行被删除。
    async fn vpc_endpoint_delete(&mut self, vpce_id: &str) -> Result<bool, ControlError>;
    /// 按 `vpce_id` 升序。
    async fn vpc_endpoints(&mut self) -> Result<Vec<VpcEndpoint>, ControlError>;
    /// 整表替换（DELETE all + 按入参顺序 INSERT）。
    async fn replace_vpc_endpoints(&mut self, rows: &[VpcEndpoint]) -> Result<(), ControlError>;
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
