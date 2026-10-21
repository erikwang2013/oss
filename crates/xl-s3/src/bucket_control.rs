//! M9 S6c：桶控制写跨 crate 接口（oss-server `ControlPlane` 实现，仅 Raft 模式注入）。
//!
//! xl-s3 不能依赖 oss-server；handler 只表达"写什么"（确定值由 handler 先算好），
//! 命令构造/尺寸闸/apply/副作用全在实现侧。`None` = 今日 M7 直写路径逐字节不变。

use async_trait::async_trait;

/// 建桶一次性载荷（owner+mode+versioning+lock 原子；`owner_uid=None` = 管理员属主）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketCreateSpec {
    pub name: String,
    /// 普通用户 uid（实现侧解析 username）；None = 管理员/无属主（buckets.owner_id=NULL）。
    pub owner_uid: Option<i64>,
    pub created_at: i64,
    /// `async` / `sync`（复制模式列非空）。
    pub replication_mode: String,
    /// `Off` / `Enabled` / `Suspended`。
    pub versioning: String,
    pub object_lock: bool,
    pub object_lock_default_mode: Option<String>,
    pub object_lock_default_days: Option<i64>,
    pub default_encryption: Option<String>,
}

/// 控制写拒绝（handler 映射 HTTP/S3 错误码；不做静默降级）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketControlError {
    NotFound,
    /// 业务冲突（建桶模式冲突/已存在竞态）。
    Conflict,
    /// 桶非空（删除前置拒绝）。
    NotEmpty,
    Invalid,
    /// 命令超过 24 MiB。
    TooLarge,
    /// 控制面无主/超时。
    Unavailable,
    Internal,
}

#[async_trait]
pub trait BucketControlSink: Send + Sync {
    async fn create_bucket(&self, spec: BucketCreateSpec) -> Result<(), BucketControlError>;
    async fn set_versioning(&self, bucket: &str, status: &str) -> Result<(), BucketControlError>;
    /// 开锁/改默认规则（调用方已保证版本控制 Enabled；一条命令写两列）。
    async fn set_object_lock(
        &self,
        bucket: &str,
        enabled: bool,
        default_mode: Option<String>,
        default_days: Option<i64>,
    ) -> Result<(), BucketControlError>;
    /// `config_json=None` → 清空（空配置语义在 handler 归一）。
    async fn set_notification(
        &self,
        bucket: &str,
        config_json: Option<String>,
        updated_at: i64,
    ) -> Result<(), BucketControlError>;
    /// `algorithm=None` → 清除默认加密。
    async fn set_default_encryption(
        &self,
        bucket: &str,
        algorithm: Option<String>,
    ) -> Result<(), BucketControlError>;
    /// 全量替换桶策略（`policy_json` 由 handler 校验/序列化，`updated_at` 先算好）。
    async fn set_policy(
        &self,
        bucket: &str,
        policy_json: &str,
        updated_at: i64,
    ) -> Result<(), BucketControlError>;
    /// 删除桶策略（无策略行 = 幂等成功；桶不存在 = NotFound）。
    async fn delete_policy(&self, bucket: &str) -> Result<(), BucketControlError>;
}
