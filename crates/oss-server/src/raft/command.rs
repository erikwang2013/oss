//! M9 控制命令：跨站点复制的唯一写意图。
//!
//! 命令只携带**提交前**算好的确定值（Argon2 hash、封装的 SK、时间戳、策略/JSON、模块
//! base64、排序后的完整规则行）；apply 侧只做 SQL 与自然键解析，不生成随机数或时间。
//! 变体名与字段名是跨版本/跨站点协议，改名即破坏重放与快照兼容。
//!
//! 滚动升级规则（[`super::compat`]）：**绝不**给已有变体加/改名/删字段——旧节点
//! `deny_unknown_fields` 会拒绝整条 entry；新复制语义只加新变体并标 level 2。

use serde::{Deserialize, Serialize};

use super::COMMAND_MAX_BYTES;

/// 子账户 × 桶的三档权限（自然键：username + bucket）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionBits {
    pub bucket: String,
    pub read: bool,
    pub write: bool,
    pub delete: bool,
}

/// 生命周期规则行（自然键：bucket + prefix；`updated_at` 在命令级统一携带）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleRuleCmd {
    pub rule_id: Option<String>,
    pub prefix: String,
    pub expire_days: i64,
    pub expiration_days: Option<i64>,
    pub transition_days: Option<i64>,
    pub transition_class: Option<String>,
    pub transition_tier: Option<String>,
    pub enabled: bool,
}

/// 结构化生命周期规则（`config_json` = filter/多 Transition/Date/Noncurrent 的版本化 blob）。
/// 与 V1 分开携带：旧二进制无法解码 V2 变体，写 V2 前需全集群升级。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleRuleCmdV2 {
    pub rule_id: Option<String>,
    pub prefix: String,
    pub expire_days: i64,
    pub expiration_days: Option<i64>,
    pub transition_days: Option<i64>,
    pub transition_class: Option<String>,
    pub transition_tier: Option<String>,
    pub enabled: bool,
    #[serde(default)]
    pub config_json: Option<String>,
}

/// 单条控制命令。所有带 `owner` 的字段是 username；`None` = 管理员属主
/// （buckets.owner_id 写 NULL，AP/inventory 写本地管理员 id 或 witness 0）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ControlCommand {
    // ---- users ----
    UserCreate {
        username: String,
        password_hash: String,
        status: String,
        created_at: i64,
    },
    UserDelete {
        username: String,
    },
    UserSetPassword {
        username: String,
        password_hash: String,
    },
    UserSetStatus {
        username: String,
        status: String,
    },
    UserSetPermissions {
        username: String,
        permissions: Vec<PermissionBits>,
    },

    // ---- access keys ----
    AccessKeyCreate {
        access_key: String,
        secret_key: String,
        username: String,
        kind: String,
        parent_ak: Option<String>,
        session_policy: Option<String>,
        created_at: i64,
    },
    AccessKeyDelete {
        access_key: String,
    },
    AccessKeySetStatus {
        access_key: String,
        status: String,
    },

    // ---- buckets ----
    BucketCreate {
        name: String,
        owner: Option<String>,
        created_at: i64,
        replication_mode: String,
        versioning: String,
        object_lock: bool,
        object_lock_default_mode: Option<String>,
        object_lock_default_days: Option<i64>,
        default_encryption: Option<String>,
    },
    BucketDelete {
        name: String,
    },
    BucketSetDeleting {
        name: String,
        deleting: bool,
    },
    BucketSetVersioning {
        name: String,
        versioning: String,
    },
    BucketSetObjectLock {
        name: String,
        enabled: bool,
        default_mode: Option<String>,
        default_days: Option<i64>,
    },
    BucketSetNotification {
        bucket: String,
        config_json: String,
        updated_at: i64,
    },
    BucketClearNotification {
        bucket: String,
    },
    BucketSetLifecycle {
        bucket: String,
        rules: Vec<LifecycleRuleCmd>,
        updated_at: i64,
    },
    /// 未来 level-2 命令：结构化生命周期（config_json）。保留 V1 变体供旧日志/快照重放；
    /// 旧节点遇到 V2 会解码失败，因此写 V2 前必须完成全集群升级。
    BucketSetLifecycleV2 {
        bucket: String,
        rules: Vec<LifecycleRuleCmdV2>,
        updated_at: i64,
    },
    BucketClearLifecycle {
        bucket: String,
    },
    BucketSetReplication {
        bucket: String,
        rules_json: String,
        updated_at: i64,
    },
    BucketClearReplication {
        bucket: String,
    },
    BucketSetDefaultEncryption {
        bucket: String,
        algorithm: String,
    },
    BucketClearDefaultEncryption {
        bucket: String,
    },
    BucketSetPolicy {
        bucket: String,
        policy_json: String,
        updated_at: i64,
    },
    BucketDeletePolicy {
        bucket: String,
    },

    // ---- access points / olap ----
    AccessPointPut {
        name: String,
        bucket: String,
        owner: Option<String>,
        policy_json: Option<String>,
        created_at: i64,
        updated_at: i64,
    },
    AccessPointDelete {
        name: String,
    },
    AccessPointSetPolicy {
        name: String,
        policy_json: Option<String>,
        updated_at: i64,
    },
    OlapPut {
        access_point: String,
        kind: String,
        endpoint: Option<String>,
        /// Wasm 模块原文的 base64（16 MiB → ~21.4 MiB JSON）。
        module: Option<String>,
        module_sha256: Option<String>,
        timeout_ms: i64,
        max_response_bytes: i64,
        updated_at: i64,
    },
    OlapDelete {
        access_point: String,
    },

    // ---- inventory ----
    InventoryPut {
        bucket: String,
        id: String,
        owner: Option<String>,
        config_json: String,
        updated_at: i64,
    },
    InventoryDelete {
        bucket: String,
        id: String,
    },

    /// S5 基座自测 KV；仅测试构建可见，生产命令集不含随机/本地状态。
    #[cfg(test)]
    Set {
        key: String,
        value: String,
    },
    #[cfg(test)]
    Delete {
        key: String,
    },
    /// 仅单元测试：level-2 探针，验证激活标记与 apply 门控（不进生产命令集）。
    #[cfg(test)]
    FeatureProbe {
        reject: bool,
    },
}

/// 每条 entry 恰好一个响应。业务拒绝走这里（提交后不回滚）；内部错误走 `Err` fail-stop。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    Ok,
    /// 删桶/删用户前发现仍有从属数据（HTTP 409）。
    BucketNotEmpty,
    NotFound,
    Conflict,
    Invalid,
}

/// 命令尺寸检查失败：编码错误（内部）或超过 [`COMMAND_MAX_BYTES`]（413）。
#[derive(Debug)]
pub enum SizeError {
    Encode(serde_json::Error),
    TooLarge(usize),
}

impl std::fmt::Display for SizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(e) => write!(f, "control command encode: {e}"),
            Self::TooLarge(n) => write!(
                f,
                "control command {n} bytes exceeds limit {COMMAND_MAX_BYTES}"
            ),
        }
    }
}

impl std::error::Error for SizeError {}

impl ControlCommand {
    /// Level B：命令进入 raft log 所需的最低 feature 级别。只有新语义命令返回 2；
    /// 既有命令永远 1（旧二进制必须能解析并应用）。
    pub fn min_feature_level(&self) -> u8 {
        #[cfg(test)]
        if matches!(self, Self::FeatureProbe { .. }) {
            return 2;
        }
        super::compat::MIN_FEATURE_LEVEL
    }

    /// serde JSON 字节数（与 raft log / RPC 实际编码一致）。
    pub fn encoded_len(&self) -> Result<usize, serde_json::Error> {
        serde_json::to_vec(self).map(|v| v.len())
    }

    /// 入口尺寸闸：只检查，不 append（Local 模式不调用——M7 行为不变）。
    pub fn check_size(&self) -> Result<(), SizeError> {
        let n = self.encoded_len().map_err(SizeError::Encode)?;
        if n > COMMAND_MAX_BYTES {
            Err(SizeError::TooLarge(n))
        } else {
            Ok(())
        }
    }
}
