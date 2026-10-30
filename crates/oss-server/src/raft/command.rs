//! M9 控制命令：跨站点复制的唯一写意图。
//!
//! 命令只携带**提交前**算好的确定值（Argon2 hash、封装的 SK、时间戳、策略/JSON、模块
//! base64、排序后的完整规则行）；apply 侧只做 SQL 与自然键解析，不生成随机数或时间。
//! 变体名与字段名是跨版本/跨站点协议，改名即破坏重放与快照兼容。
//!
//! 滚动升级规则（[`super::compat`]）：**绝不**给已有变体加/改名/删字段——旧节点
//! `deny_unknown_fields` 会拒绝整条 entry；新语义只加新变体，按
//! [`ControlCommand::min_feature_level`] 标 level 2/3，`Ext.type_id` 还必须满足
//! [`REGISTERED_EXT_TYPES`] 不变量。

use serde::{Deserialize, Serialize};

use super::COMMAND_MAX_BYTES;
use super::runstate::{
    InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow, JobRow, JobTaskRow,
    LifecycleCursorRow, ScheduleLeaseRow, TierRestoreRow,
};

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

    // ---- level 3: accounts / members ----
    /// 新建账户。`id` 是提交前生成的 12 位十进制；`created_at`/`updated_at` 同值。
    AccountCreate {
        id: String,
        name: String,
        bucket_quota: Option<i64>,
        created_at: i64,
    },
    /// 重命名和/或启停账户。停用含在册全局管理员的账户是业务拒绝（`Conflict`）。
    AccountUpdate {
        id: String,
        name: Option<String>,
        status: Option<String>,
        updated_at: i64,
    },
    AccountSetQuota {
        id: String,
        bucket_quota: Option<i64>,
        updated_at: i64,
    },
    /// 软字节配额（AC-2）：只告警不阻写；`None` = 取消。独立变体避免给
    /// `AccountSetQuota` 加字段——旧二进制的 `deny_unknown_fields` 会 fail-stop。
    AccountSetSoftQuota {
        id: String,
        soft_byte_quota: Option<i64>,
        updated_at: i64,
    },
    AccountDelete {
        id: String,
    },
    /// 挂载已有用户（自然键 username；apply 解析为本地 uid）。
    AccountMemberAttach {
        account_id: String,
        username: String,
        role: String,
        created_at: i64,
    },
    /// 账户内成员启停（自然键 username）。
    AccountMemberSetStatus {
        account_id: String,
        username: String,
        active: bool,
    },
    /// 账户内新建用户（is_admin 恒 0；口令哈希在 append 前算好）。
    AccountUserCreate {
        account_id: String,
        username: String,
        password_hash: String,
        role: String,
        created_at: i64,
    },

    // ---- level 3: command evolution ----
    /// 命令演进容器（level 3）：`type_id` 是提交方注册的稳定类型名，`payload` 为该
    /// 类型的 JSON。旧二进制没有该变体（`deny_unknown_fields`）→ 解码 fail-stop；
    /// leader 在 append 前按 [`REGISTERED_EXT_TYPES`] 拒绝未注册/空/拼错的
    /// `type_id`（不写日志、不激活 marker）；apply 的未知类型 `Invalid` 仅作兜底。
    /// 新增类型必须先提升 feature level 或完成 per-peer 类型能力协商。
    /// 尺寸沿用 [`COMMAND_MAX_BYTES`]（24 MiB），payload 无额外豁免。
    Ext {
        type_id: String,
        payload: serde_json::Value,
    },

    // ---- level 4: M8-2 job run-state replication ----
    /// 作业行按主键 upsert（创建与每次状态转换都提交完整行）。apply 侧只做
    /// SQL：`updated_at`/计数单调 + 终态不可回退；`kind`/`created_at` 不可变。
    JobUpsert {
        job: JobRow,
    },
    /// 任务行按 `(job_id, task_no)` upsert；`fence_term` 单调、终态不可复活。
    JobTaskUpsert {
        task: JobTaskRow,
    },
    /// 一次原子提交多行（建作业 + 任务、领取、收尾）。整批仍受 24 MiB 上限。
    /// `restores` 与 Restore 建作业同批落日志（P2-1）；空时字段不序列化，旧形态不变。
    JobBatch {
        jobs: Vec<JobRow>,
        tasks: Vec<JobTaskRow>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        restores: Vec<TierRestoreRow>,
    },
    /// level 5：inventory 运行态（runs/cursors/incrementals）同批 upsert。
    /// 行按主键/自然键整行携带；apply 只做 SQL，leader 在 append 前做身份与引用校验。
    InventoryBatch {
        runs: Vec<InventoryRunRow>,
        cursors: Vec<InventoryCursorRow>,
        incrementals: Vec<InventoryIncrementalRow>,
    },
    /// 关闭增量模式：原子删除该配置的游标与增量开关（幂等，重复 apply 为 no-op）。
    InventoryClear {
        bucket: String,
        config_id: String,
    },

    // ---- level 5: lifecycle 扫描游标复制 ----
    /// 生命周期扫描游标整行 upsert（自然键 bucket+rule_id+stage）。apply 只做 SQL：
    /// 水位非空、`updated_at` 严格前进、fence 不回退；leader 在 append 前校验桶引用。
    LifecycleCursorUpsert {
        cursor: LifecycleCursorRow,
    },
    /// 阶段扫完清除游标（幂等）。`fence_term` 是删除上限：旧 term 的迟到删除不得
    /// 抹掉新 term 已推进的进度。
    LifecycleCursorClear {
        bucket: String,
        rule_id: String,
        stage: String,
        fence_term: i64,
    },

    // ---- level 6: 作业历史保留 ----
    /// 删除终态作业历史（jobs/job_tasks）。apply 复检状态：只删当前仍为终态的行，
    /// queued/running/preempted 永不删除；不存在的 id 是幂等 no-op。
    /// 约束见 [`validate_job_purge`]：非空、≤ [`JOB_PURGE_MAX_IDS`]、无重复。
    JobPurge {
        job_ids: Vec<i64>,
    },

    // ---- level 7: tier restore 后续状态与调度租约 ----
    /// Restore 行整行 upsert（受理后状态：restored/回滚 pending）。apply 只做 SQL：
    /// 自然键非空、state ∈ pending/restored、fence 不回退；逐字段相同重复 apply 幂等。
    TierRestoreUpsert {
        restore: TierRestoreRow,
    },
    /// 清理 Restore 行（幂等）。`fence_term` 是删除上限：旧 term 的迟到删除不得抹掉
    /// 新 term 已更新的行。
    TierRestoreDelete {
        bucket: String,
        key: String,
        version_id: String,
        fence_term: i64,
    },
    /// 调度租约 acquire/renew/takeover（单例行）。`now` 是提交方时钟（apply 不取时
    /// 间）：同 term 同节点只允许延长 expiry；换 term 只有旧租约过期后才能接管。
    ScheduleLeaseAcquire {
        lease: ScheduleLeaseRow,
        now: i64,
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

/// 已注册的生产 `Ext.type_id` 白名单（当前为空：level 3 只代表“能解码 Ext 容器”，
/// **不**代表认识任何业务类型）。
///
/// 不变量：新增一项即协议变更。只有当所有 voter/learner 都已换到能应用该类型的
/// 二进制时才可注册；否则必须先提升 feature level（新级别）或完成 per-peer 类型
/// 能力协商。leader 在 append 前按本表 fail-closed 拒绝未注册/空/拼错的 type_id
/// （不写日志、不激活 marker）；apply 侧未知类型的 `Invalid` 只是兜底。
pub const REGISTERED_EXT_TYPES: &[&str] = &[];

/// `JobPurge` 单条命令的 id 上限（保留 worker 每批大小）。leader append 前与 apply
/// 兜底使用同一判据；超限命令必须整体拒绝，不能截断（截断即不同结果）。
pub const JOB_PURGE_MAX_IDS: usize = 1000;

/// [`ControlCommand::JobPurge`] 的入口约束：非空、≤ [`JOB_PURGE_MAX_IDS`]、无重复。
pub fn validate_job_purge(job_ids: &[i64]) -> Result<(), String> {
    if job_ids.is_empty() {
        return Err("JobPurge: job_ids must not be empty".into());
    }
    if job_ids.len() > JOB_PURGE_MAX_IDS {
        return Err(format!(
            "JobPurge: {} job_ids exceeds limit {JOB_PURGE_MAX_IDS}",
            job_ids.len()
        ));
    }
    let mut seen = std::collections::HashSet::with_capacity(job_ids.len());
    if let Some(id) = job_ids.iter().find(|id| !seen.insert(**id)) {
        return Err(format!("JobPurge: duplicate job id {id}"));
    }
    Ok(())
}

/// 仅测试构建注册的探针类型（生产注册表必须保持为空）。
#[cfg(test)]
const TEST_EXT_TYPES: &[&str] = &["test.noop"];

/// `type_id` 是否已在本构建注册；空串/拼错/未注册一律 false。
pub(crate) fn ext_type_registered(type_id: &str) -> bool {
    #[cfg(test)]
    {
        TEST_EXT_TYPES.contains(&type_id) || REGISTERED_EXT_TYPES.contains(&type_id)
    }
    #[cfg(not(test))]
    {
        REGISTERED_EXT_TYPES.contains(&type_id)
    }
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
    /// Level B：命令进入 raft log 所需的最低 feature 级别。新语义命令返回其最低
    /// 级别（2/3）；既有命令永远 1（旧二进制必须能解析并应用）。
    pub fn min_feature_level(&self) -> u8 {
        match self {
            // 结构化生命周期：v1.13.1 的 `ControlCommand`（`deny_unknown_fields`）无法
            // 解码该变体，混合窗口内提交会让旧副本 fail-stop。
            Self::BucketSetLifecycleV2 { .. } => super::compat::FEATURE_STRUCTURED_LIFECYCLE,
            // 命令演进容器：旧二进制（最高 level 2）必须先在 leader 入口被拒，
            // 否则它们会解码失败并在 apply 侧 fail-stop。
            Self::Ext { .. } => super::compat::FEATURE_COMMAND_EXT,
            // 账户/成员命令依赖 OSSCTL3 的快照 canonical 语义，且旧二进制
            // （`deny_unknown_fields`）无法解码这些变体。
            Self::AccountCreate { .. }
            | Self::AccountUpdate { .. }
            | Self::AccountSetQuota { .. }
            | Self::AccountSetSoftQuota { .. }
            | Self::AccountDelete { .. }
            | Self::AccountMemberAttach { .. }
            | Self::AccountMemberSetStatus { .. }
            | Self::AccountUserCreate { .. } => super::compat::FEATURE_LEVEL_3,
            // M8-2：jobs/job_tasks/tier_restores 行复制。v1.22 及更早二进制的解码器
            // （`deny_unknown_fields`）不认识这些变体，必须用新 level 4 而非 3 门控。
            Self::JobUpsert { .. } | Self::JobTaskUpsert { .. } | Self::JobBatch { .. } => {
                super::compat::FEATURE_LEVEL_4
            }
            // v1.23.0 已 advertise 4 但不认识 Inventory*；必须用 level 5。
            Self::InventoryBatch { .. } | Self::InventoryClear { .. } => {
                super::compat::FEATURE_LEVEL_5
            }
            // 与 Inventory* 同级别：level-5 二进制之外无人能解码。
            Self::LifecycleCursorUpsert { .. } | Self::LifecycleCursorClear { .. } => {
                super::compat::FEATURE_LEVEL_5
            }
            // v1.24.0 及更早 advertise ≤5 且 `deny_unknown_fields` 不认识 JobPurge。
            Self::JobPurge { .. } => super::compat::FEATURE_LEVEL_6,
            // v1.25.0 及更早 advertise ≤6 且 `deny_unknown_fields` 不认识这些变体。
            Self::TierRestoreUpsert { .. }
            | Self::TierRestoreDelete { .. }
            | Self::ScheduleLeaseAcquire { .. } => super::compat::FEATURE_LEVEL_7,
            #[cfg(test)]
            Self::FeatureProbe { .. } => 2,
            _ => super::compat::MIN_FEATURE_LEVEL,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 注册表 fail-closed：空串、拼错、未注册都是 false；仅测试类型的探针为 true。
    #[test]
    fn ext_registry_rejects_empty_typo_and_unregistered() {
        assert!(!ext_type_registered(""));
        assert!(!ext_type_registered("test.nop"));
        assert!(!ext_type_registered("future.demo"));
        assert!(ext_type_registered("test.noop"));
    }

    /// JobPurge 入口约束（leader append 前与 apply 兜底共用）：空/重复/超限整体拒绝。
    #[test]
    fn job_purge_input_is_bounded() {
        assert!(validate_job_purge(&[1, 2]).is_ok());
        assert!(validate_job_purge(&[]).is_err());
        assert!(validate_job_purge(&[7, 7]).is_err());
        let at_limit: Vec<i64> = (0..JOB_PURGE_MAX_IDS as i64).collect();
        assert!(validate_job_purge(&at_limit).is_ok());
        let mut over_limit = at_limit;
        over_limit.push(JOB_PURGE_MAX_IDS as i64);
        assert!(validate_job_purge(&over_limit).is_err());
    }

    /// 混版契约：`ControlResponse` 变体集合保持 v1.20/v1.21 一致（增变体会让旧副本
    /// 解码 apply 响应失败）；配额拒绝改走 `WriteError`/`ControlError`，不进日志响应。
    #[test]
    fn control_response_variants_match_released_set() {
        let variants: Vec<String> = [
            ControlResponse::Ok,
            ControlResponse::BucketNotEmpty,
            ControlResponse::NotFound,
            ControlResponse::Conflict,
            ControlResponse::Invalid,
        ]
        .iter()
        .map(|r| serde_json::to_string(r).unwrap())
        .collect();
        assert_eq!(
            variants,
            [
                "\"Ok\"",
                "\"BucketNotEmpty\"",
                "\"NotFound\"",
                "\"Conflict\"",
                "\"Invalid\""
            ]
        );
    }
}
