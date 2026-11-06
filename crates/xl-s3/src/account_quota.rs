//! AC-3/AC-4：账户硬字节/对象数配额（写入预留、删除释放、跨节点有界陈旧）。
//!
//! 用量口径与软配额端点一致：账户全部桶的**当前版本**对象（列举天然排除 delete
//! marker/非当前版本/未完成 MPU）。本模块只定义后端中立类型与 [`QuotaBackend`]
//! 缝：SQLite 本地 `account_usage` 缓存与对象后端复制账本由 `oss-server` 适配器
//! 实现，xl-s3 不直接持有控制库连接。
//!
//! 语义约定（两个后端一致）：
//! - 写入在原子条件更新中预留；删除立即释放（版本定向删除等交给采样器）；
//! - 陈旧/未知下界只能让**已知**超限被拒（fail-open），后端错误 fail-closed（500）；
//! - `admit` 失败时调用方必须归还同样增量；`adjust` 是无条件校准。
//!
//! shortcut: 定向版本删除与无钥 SSE-C 删除不即时释放，靠采样器收敛；升级触发：
//! 需要逐版本用量索引（或删除路径携带 SSE-C 钥）时。

use xl_store::control_store::ControlError;
use xl_store::store::Store;

/// 惰性刷新阈值：热路径最多容忍这么旧的跨节点用量（SQLite 本地缓存实现使用）。
pub const USAGE_TTL_SECS: i64 = 30;

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    /// 非负；`None` = 不限（NULL/负数同义）。
    pub hard_bytes: Option<i64>,
    pub objects: Option<i64>,
}

impl Limits {
    pub fn any(&self) -> bool {
        self.hard_bytes.is_some() || self.objects.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct AccountQuota {
    pub account: String,
    /// 账户已停用：写入 fail-closed（与桶数配额同判）。
    pub disabled: bool,
    pub limits: Limits,
}

#[derive(Debug, Clone, Copy)]
pub enum QuotaDenied {
    Bytes,
    Objects,
}

impl std::fmt::Display for QuotaDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bytes => f.write_str("account byte quota exceeded"),
            Self::Objects => f.write_str("account object quota exceeded"),
        }
    }
}

/// 账户硬配额的后端中立读写缝。
///
/// 读：桶主账户的限制（`Service` 热路径）与周期采样；写：预留/校准/释放。
/// 实现必须保持调用方可见语义：`admit` 返回 `Ok(Err(_))` 才是稳定配额拒绝，
/// 后端故障返回 `Err`（调用方映射 500，绝不静默放行）。
#[async_trait::async_trait]
pub trait QuotaBackend: Send + Sync {
    /// 桶主的账户与硬配额；管理员/无账户行/未配置配额 → `None`。
    /// 账户停用时仍返回 `Some`（带 limits），由写入侧 fail-closed。
    async fn account_quota_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Option<AccountQuota>, ControlError>;

    /// 写入准入：先保证用量有界新鲜，再原子预留 `(delta_bytes, delta_objects)`。
    /// 任一下降（覆写变小/删旧版本）也走同一路径；失败时调用方必须归还同样增量。
    async fn admit(
        &self,
        store: &Store,
        quota: &AccountQuota,
        delta_bytes: i64,
        delta_objects: i64,
    ) -> Result<Result<(), QuotaDenied>, ControlError>;

    /// 无条件增量修正（写成功后的实际大小校准；或写失败时归还预留）。
    async fn adjust(
        &self,
        account: &str,
        delta_bytes: i64,
        delta_objects: i64,
    ) -> Result<(), ControlError>;

    /// 删除/失败归还（非负量；账本饱和到 0）。未知结果只告警，绝不重试。
    async fn release(&self, account: &str, bytes: i64, objects: i64) -> Result<(), ControlError> {
        self.adjust(account, -bytes, -objects).await
    }

    /// 周期采样入口：只刷新配置了硬配额的活跃账户。
    async fn refresh_all(&self, store: &Store) -> Result<(), ControlError>;

    /// strict 模式单条账本命令的播种基线（缺行/读失败 = (0, 0)）。
    async fn usage_baseline(&self, account: &str) -> (i64, i64);
}

/// 未注入后端的兼容实现：无配额、记账 no-op（xl-s3 单元/集成测试与离线命令）。
///
/// 生产 serve 路径由 `AppState::acl()` 注入真实后端；任何新的 S3 装配入口都必须
/// 调用 `Acl::with_quota`，否则硬配额会静默不生效。
#[derive(Debug, Default)]
pub struct NoQuota;

#[async_trait::async_trait]
impl QuotaBackend for NoQuota {
    async fn account_quota_for_bucket(
        &self,
        _bucket: &str,
    ) -> Result<Option<AccountQuota>, ControlError> {
        Ok(None)
    }

    async fn admit(
        &self,
        _store: &Store,
        _quota: &AccountQuota,
        _delta_bytes: i64,
        _delta_objects: i64,
    ) -> Result<Result<(), QuotaDenied>, ControlError> {
        Ok(Ok(()))
    }

    async fn adjust(
        &self,
        _account: &str,
        _delta_bytes: i64,
        _delta_objects: i64,
    ) -> Result<(), ControlError> {
        Ok(())
    }

    async fn refresh_all(&self, _store: &Store) -> Result<(), ControlError> {
        Ok(())
    }

    async fn usage_baseline(&self, _account: &str) -> (i64, i64) {
        (0, 0)
    }
}

/// strict 模式：复制账本接口（oss-server `ControlPlane` 实现；xl-s3 不依赖它）。
///
/// 命令**非幂等**。实现必须在 append 结果不明（超时/无主/无 quorum）时返回
/// `Unavailable`，调用方不得重试或反向补偿——预留泄漏是保守的，重复释放会少计。
#[async_trait::async_trait]
pub trait ClusterQuota: Send + Sync {
    async fn reserve(
        &self,
        account: &str,
        bytes: i64,
        objects: i64,
    ) -> Result<(), ClusterQuotaError>;
    async fn release(
        &self,
        account: &str,
        bytes: i64,
        objects: i64,
    ) -> Result<(), ClusterQuotaError>;
    async fn adjust(
        &self,
        account: &str,
        bytes: i64,
        objects: i64,
    ) -> Result<(), ClusterQuotaError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterQuotaError {
    /// 稳定配额拒绝（apply 侧 `Invalid`；严格模式启用后本地配额也由账本判定）。
    Exceeded,
    /// append 结果不明/控制面不可用：写入必须失败且不得盲目重试。
    Unavailable,
    Invalid,
    Internal,
}
