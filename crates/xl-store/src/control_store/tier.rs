//! tier restore 域行类型与后端中立读写 trait。

use serde::{Deserialize, Serialize};

/// tier restore 行（`m8.runstate` 扩展；字段顺序即 rmp 字节顺序）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TierRestoreRow {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub expires_at: i64,
    pub job_id: Option<i64>,
    pub state: String,
    pub created_at: i64,
    pub fence_term: i64,
    pub fence_leader: i64,
}

use super::ControlError;

/// 只读 tier restore 访问。
#[async_trait::async_trait]
pub trait TierStore: Send + Sync {
    async fn tier_restore(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<TierRestoreRow>, ControlError>;
    /// `WHERE state = ?`，按 `created_at` 升序（reaper/对账）。
    async fn tier_restores(&self, state: &str) -> Result<Vec<TierRestoreRow>, ControlError>;
}

/// 调用方事务内的 tier restore 写访问；本 trait 不 begin/commit/rollback。
#[allow(clippy::too_many_arguments)] // 行字段面即参数面；拆结构体只多一层搬运。
#[async_trait::async_trait]
pub trait TierTxn: Send {
    async fn tier_restore(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<TierRestoreRow>, ControlError>;
    /// 快照导出：按 `(bucket, key, version_id)` 升序。
    async fn tier_restores(&mut self) -> Result<Vec<TierRestoreRow>, ControlError>;
    async fn replace_tier_restores(&mut self, rows: &[TierRestoreRow]) -> Result<(), ControlError>;
    /// fence 门控 upsert；返回是否恰好 1 行。
    async fn tier_restore_upsert(&mut self, row: &TierRestoreRow) -> Result<bool, ControlError>;
    async fn tier_restore_delete(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
        fence_term: i64,
    ) -> Result<(), ControlError>;
    /// `UPDATE state/expires_at/fence WHERE PK AND fence_term <= ?`。
    async fn tier_restore_set_state(
        &mut self,
        bucket: &str,
        key: &str,
        version_id: &str,
        state: &str,
        expires_at: i64,
        fence_term: i64,
        fence_leader: i64,
    ) -> Result<(), ControlError>;
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    /// 旧 `m8.runstate` 扩展行（与 `raft/runstate/model.rs` 声明顺序一致）。
    #[derive(Serialize)]
    struct LegacyRestore {
        bucket: String,
        key: String,
        version_id: String,
        expires_at: i64,
        job_id: Option<i64>,
        state: String,
        created_at: i64,
        fence_term: i64,
        fence_leader: i64,
    }

    #[test]
    fn tier_restore_rmp_matches_runstate_extension() {
        let row = super::TierRestoreRow {
            bucket: "b1".into(),
            key: "k".into(),
            version_id: "v1".into(),
            expires_at: 9,
            job_id: Some(3),
            state: "restored".into(),
            created_at: 4,
            fence_term: 5,
            fence_leader: 6,
        };
        let bytes = rmp_serde::to_vec_named(&row).unwrap();
        let legacy = LegacyRestore {
            bucket: row.bucket.clone(),
            key: row.key.clone(),
            version_id: row.version_id.clone(),
            expires_at: row.expires_at,
            job_id: row.job_id,
            state: row.state.clone(),
            created_at: row.created_at,
            fence_term: row.fence_term,
            fence_leader: row.fence_leader,
        };
        assert_eq!(bytes, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(
            rmp_serde::from_slice::<super::TierRestoreRow>(&bytes).unwrap(),
            row
        );
    }

    #[test]
    fn tier_restore_rejects_unknown_fields() {
        #[derive(Serialize)]
        struct WithExtra {
            bucket: String,
            key: String,
            version_id: String,
            expires_at: i64,
            job_id: Option<i64>,
            state: String,
            created_at: i64,
            fence_term: i64,
            fence_leader: i64,
            future_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            bucket: "b".into(),
            key: "k".into(),
            version_id: "v".into(),
            expires_at: 0,
            job_id: None,
            state: "pending".into(),
            created_at: 1,
            fence_term: 2,
            fence_leader: 3,
            future_field: 4,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<super::TierRestoreRow>(&bytes).is_err());
    }
}
