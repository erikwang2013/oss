//! 控制状态机运行时选择：对象文件（唯一后端）或 witness。
//!
//! `RaftHandle` 只保留观测/身份所需的少量操作；apply/snapshot 由 openraft 直接持有
//! 具体状态机（[`ObjectStateMachine`] 或 [`WitnessStateMachine`]），本枚举不实现
//! openraft trait。

use openraft::{BasicNode, LogId, StorageError, StoredMembership};

use super::object_sm::ObjectStateMachine;
use super::witness_sm::WitnessStateMachine;

pub(crate) enum RaftSm {
    Object(ObjectStateMachine),
    Witness(WitnessStateMachine),
}

impl RaftSm {
    pub fn activation_level(&self) -> u8 {
        match self {
            Self::Object(sm) => sm.activation_level(),
            Self::Witness(sm) => sm.activation_level(),
        }
    }

    pub async fn applied(
        &self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        match self {
            Self::Object(sm) => sm.applied().await,
            Self::Witness(sm) => sm.applied(),
        }
    }

    /// leader append 前的建桶配额判定（对象模式在根快照上只读事务内完成）。
    pub async fn bucket_create_quota_reached(
        &self,
        name: &str,
        owner: Option<&str>,
    ) -> Result<bool, String> {
        match self {
            Self::Object(sm) => sm
                .bucket_create_quota_reached(name, owner)
                .await
                .map_err(|e| e.to_string()),
            Self::Witness(_) => Err("witness 只有元数据状态机，不能判定建桶配额".into()),
        }
    }

    pub async fn snapshot_bytes(&self) -> u64 {
        match self {
            Self::Object(sm) => sm.snapshot_bytes().await,
            Self::Witness(sm) => sm.snapshot_bytes().await,
        }
    }
}
