//! M9 控制命令的确定性 apply：自然键解析 + 一个事务内的控制行写入。
//!
//! 约束（计划 §2.3）：apply 是 11 张复制表的唯一写入者；只在事务内做 SQL，不生成
//! 随机数/时间戳/hash/seal；业务拒绝用 [`ControlResponse`]（事务照常提交），内部不变量
//! 破坏返回 `Err`（调用方 fail-stop，事务回滚）。

use base64::Engine as _;
use std::collections::HashSet;

use xl_store::control_store::bucket::{BucketCreateOutcome, BucketEntry};
use xl_store::control_store::iam::IamTxn;
use xl_store::control_store::inventory::InventoryConfigEntry;
use xl_store::control_store::{
    AccessPointEntry, ControlError, ControlTxn, OlapConfigEntry, OwnerRef,
};

mod accounts;
pub(crate) mod inventory;
pub(crate) mod lease;
pub(crate) mod lifecycle;
mod quota;
pub(crate) mod tier_restore;

use super::command::{
    ControlCommand, ControlResponse, LifecycleRuleCmd, PermissionBits, validate_job_purge,
};
use super::runstate::{JobRow, JobTaskRow, TierRestoreRow};
use crate::db::LifecycleRuleRow;

/// apply 结果：一个响应 + 提交后副作用描述（facade 执行，apply 内绝不执行）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Applied {
    pub(crate) response: ControlResponse,
    pub(crate) effects: Vec<SideEffect>,
}

/// 提交后副作用（幂等；桶目录/缓存）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SideEffect {
    BucketDirCreate(String),
    BucketDirDelete(String),
    /// sync 桶删除：整目录 rename 到盘内 `.oss.sys/trash/`（幂等；启动清空）。
    BucketDirTombstone(String),
    ApReload(String),
    ApRemove(String),
    ApInvalidateBucket(String),
    OlapRemove(String),
    OlapInvalidateBucket(String),
    OlapReload,
}

/// 命令 → 副作用（纯函数；leader 的 apply 结果不回传，facade 用同一映射重算）。
pub(crate) fn effects_for(cmd: &ControlCommand) -> Vec<SideEffect> {
    match cmd {
        ControlCommand::BucketCreate { name, .. } => {
            vec![SideEffect::BucketDirCreate(name.clone())]
        }
        ControlCommand::BucketDelete { name } => vec![
            SideEffect::BucketDirDelete(name.clone()),
            SideEffect::ApInvalidateBucket(name.clone()),
            SideEffect::OlapInvalidateBucket(name.clone()),
        ],
        ControlCommand::AccessPointPut { name, .. }
        | ControlCommand::AccessPointSetPolicy { name, .. } => {
            vec![SideEffect::ApReload(name.clone())]
        }
        ControlCommand::AccessPointDelete { name } => vec![
            SideEffect::ApRemove(name.clone()),
            SideEffect::OlapRemove(name.clone()),
        ],
        ControlCommand::OlapPut { .. } => vec![SideEffect::OlapReload],
        ControlCommand::OlapDelete { access_point } => {
            vec![SideEffect::OlapRemove(access_point.clone())]
        }
        _ => Vec::new(),
    }
}

fn invariant(msg: String) -> ControlError {
    ControlError::Invalid(msg)
}

fn valid_user_status(s: &str) -> bool {
    matches!(s, "active" | "disabled")
}

fn valid_versioning(s: &str) -> bool {
    matches!(s, "Off" | "Enabled" | "Suspended")
}

fn valid_lock_mode(s: Option<&str>) -> bool {
    matches!(s, None | Some("GOVERNANCE") | Some("COMPLIANCE"))
}

async fn uid(txn: &mut dyn ControlTxn, username: &str) -> Result<Option<i64>, ControlError> {
    txn.user_uid(username).await
}

/// M1：管理员用户/AK 是站点本地状态，IAM 命令不得复制它们（S6b 处理器留本地）。
async fn username_is_admin(txn: &mut dyn ControlTxn, username: &str) -> Result<bool, ControlError> {
    txn.user_is_admin(username).await
}

async fn access_key_owner_is_admin(
    txn: &mut dyn ControlTxn,
    access_key: &str,
) -> Result<bool, ControlError> {
    txn.access_key_owner_is_admin(access_key).await
}

async fn bucket_exists(txn: &mut dyn ControlTxn, bucket: &str) -> Result<bool, ControlError> {
    txn.bucket_exists(bucket).await
}

fn permissions_rows(bits: &[PermissionBits]) -> Vec<xl_store::control_store::iam::PermissionRow> {
    let mut rows: Vec<xl_store::control_store::iam::PermissionRow> = bits
        .iter()
        .filter(|p| p.read || p.write || p.delete)
        .map(|p| xl_store::control_store::iam::PermissionRow {
            bucket: p.bucket.clone(),
            read: p.read,
            write: p.write,
            delete: p.delete,
        })
        .collect();
    // 全量替换的确定性顺序（重放/快照相等）。
    rows.sort_by(|a, b| a.bucket.cmp(&b.bucket));
    rows
}

fn lifecycle_rows(
    bucket: &str,
    rules: &[LifecycleRuleCmd],
    updated_at: i64,
) -> Vec<LifecycleRuleRow> {
    let mut rows: Vec<LifecycleRuleRow> = rules
        .iter()
        .map(|r| LifecycleRuleRow {
            bucket: bucket.to_string(),
            rule_id: r.rule_id.clone(),
            prefix: r.prefix.clone(),
            expire_days: r.expire_days,
            expiration_days: r.expiration_days,
            transition_days: r.transition_days,
            transition_class: r.transition_class.clone(),
            transition_tier: r.transition_tier.clone(),
            enabled: r.enabled,
            updated_at,
            config_json: None,
        })
        .collect();
    rows.sort_by(|a, b| (&a.prefix, &a.rule_id).cmp(&(&b.prefix, &b.rule_id)));
    rows
}

fn lifecycle_rows_v2(
    bucket: &str,
    rules: &[crate::raft::command::LifecycleRuleCmdV2],
    updated_at: i64,
) -> Vec<LifecycleRuleRow> {
    let mut rows: Vec<LifecycleRuleRow> = rules
        .iter()
        .map(|r| LifecycleRuleRow {
            bucket: bucket.to_string(),
            rule_id: r.rule_id.clone(),
            prefix: r.prefix.clone(),
            expire_days: r.expire_days,
            expiration_days: r.expiration_days,
            transition_days: r.transition_days,
            transition_class: r.transition_class.clone(),
            transition_tier: r.transition_tier.clone(),
            enabled: r.enabled,
            updated_at,
            config_json: r.config_json.clone(),
        })
        .collect();
    rows.sort_by(|a, b| (&a.prefix, &a.rule_id).cmp(&(&b.prefix, &b.rule_id)));
    rows
}

/// 后端无关的 apply：一个已开启的控制事务内应用一条命令。
///
/// 除响应/副作用外返回 `finish`：`UserDelete` 需由后端在删除用户行前清空
/// 桶/上传属主（SQLite 的 FK 与本地 uploads 的时序不同），由调用方收尾。
pub(crate) async fn apply_command_txn(
    txn: &mut dyn ControlTxn,
    cmd: &ControlCommand,
) -> Result<(Applied, Option<i64>), ControlError> {
    let effects = effects_for(cmd);
    // Level-3 账户/成员命令有独立的业务约束集合（自然键 + 账户不变量）。
    if let Some(response) = accounts::apply(txn, cmd).await? {
        return Ok((Applied { response, effects }, None));
    }
    // feature 9：用量账本（无副作用；判定全部在事务内完成）。
    if let Some(response) = quota::apply(txn, cmd).await? {
        return Ok((Applied { response, effects }, None));
    }
    let mut user_delete = None;
    let response = match cmd {
        ControlCommand::UserCreate {
            username,
            password_hash,
            status,
            created_at,
        } => {
            if username.is_empty() || password_hash.is_empty() || !valid_user_status(status) {
                ControlResponse::Invalid
            } else if uid(txn, username).await?.is_some() {
                ControlResponse::Conflict
            } else {
                txn.user_create(&xl_store::control_store::iam::UserEntry {
                    username: username.clone(),
                    password_hash: password_hash.clone(),
                    status: status.clone(),
                    created_at: *created_at,
                })
                .await?;
                ControlResponse::Ok
            }
        }
        ControlCommand::UserDelete { username } => {
            if username_is_admin(txn, username).await? {
                ControlResponse::Invalid
            } else {
                match uid(txn, username).await? {
                    None => ControlResponse::NotFound,
                    Some(id) => {
                        let guard = txn.user_delete_guard(id).await?;
                        if !guard {
                            // 守卫拒绝：目标是最后一个在册管理员（命令路径本不产生管理员）。
                            ControlResponse::Conflict
                        } else {
                            txn.user_delete_access_keys_permissions(id).await?;
                            // 桶/上传属主清空与用户行删除由后端收尾：SQLite 在同一事务
                            // 内做 FK 级联；对象后端在根 CAS 内清桶、CAS 后清本地上传（D3）。
                            user_delete = Some(id);
                            ControlResponse::Ok
                        }
                    }
                }
            }
        }
        ControlCommand::UserSetPassword {
            username,
            password_hash,
        } => {
            if password_hash.is_empty() || username_is_admin(txn, username).await? {
                ControlResponse::Invalid
            } else {
                match uid(txn, username).await? {
                    None => ControlResponse::NotFound,
                    Some(id) => {
                        if txn.user_set_password(id, password_hash).await? {
                            ControlResponse::Ok
                        } else {
                            ControlResponse::NotFound
                        }
                    }
                }
            }
        }
        ControlCommand::UserSetStatus { username, status } => {
            if !valid_user_status(status) || username_is_admin(txn, username).await? {
                ControlResponse::Invalid
            } else {
                match uid(txn, username).await? {
                    None => ControlResponse::NotFound,
                    Some(id) => {
                        if txn.user_set_status(id, status == "active").await? {
                            ControlResponse::Ok
                        } else {
                            ControlResponse::Conflict
                        }
                    }
                }
            }
        }
        ControlCommand::UserSetPermissions {
            username,
            permissions,
        } => {
            if username_is_admin(txn, username).await? {
                ControlResponse::Invalid
            } else {
                match uid(txn, username).await? {
                    None => ControlResponse::NotFound,
                    Some(id) => {
                        txn.permissions_replace(id, &permissions_rows(permissions))
                            .await?;
                        ControlResponse::Ok
                    }
                }
            }
        }
        ControlCommand::AccessKeyCreate {
            access_key,
            secret_key,
            username,
            kind,
            parent_ak,
            session_policy,
            created_at,
        } => {
            if access_key.is_empty()
                || secret_key.is_empty()
                || kind.is_empty()
                || username_is_admin(txn, username).await?
            {
                ControlResponse::Invalid
            } else {
                if txn.access_key_exists(access_key).await? {
                    ControlResponse::Conflict
                } else {
                    // 属主缺失（非 witness）是不变量破坏：AK 会永远无法鉴权。
                    let Some(_user_id) = txn.user_uid(username).await? else {
                        return Err(invariant(format!("access key owner {username} not found")));
                    };
                    txn.access_key_create(&xl_store::control_store::iam::AccessKeyEntry {
                        access_key: access_key.clone(),
                        secret_key: secret_key.clone(),
                        username: username.clone(),
                        status: "active".into(),
                        expires_at: None,
                        created_at: *created_at,
                        parent_ak: parent_ak.clone(),
                        session_policy: session_policy.clone(),
                        kind: kind.clone(),
                    })
                    .await?;
                    ControlResponse::Ok
                }
            }
        }
        ControlCommand::AccessKeyDelete { access_key } => {
            if access_key_owner_is_admin(txn, access_key).await? {
                ControlResponse::Invalid
            } else if txn.access_key_delete(access_key).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::AccessKeySetStatus { access_key, status } => {
            if !valid_user_status(status) || access_key_owner_is_admin(txn, access_key).await? {
                ControlResponse::Invalid
            } else if txn.access_key_set_status(access_key, status).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketCreate {
            name,
            owner,
            created_at,
            replication_mode,
            versioning,
            object_lock,
            object_lock_default_mode,
            object_lock_default_days,
            default_encryption,
        } => {
            if name.is_empty()
                || !matches!(replication_mode.as_str(), "async" | "sync")
                || !valid_versioning(versioning)
                || !valid_lock_mode(object_lock_default_mode.as_deref())
            {
                ControlResponse::Invalid
            } else {
                let owner = match owner {
                    None => OwnerRef::Admin,
                    Some(name) => OwnerRef::User(name.clone()),
                };
                match txn
                    .bucket_create(&BucketEntry {
                        name: name.clone(),
                        owner,
                        creation_date: *created_at,
                        versioning: versioning.clone(),
                        object_lock: *object_lock,
                        default_encryption: default_encryption.clone(),
                        object_lock_default_mode: object_lock_default_mode.clone(),
                        object_lock_default_days: *object_lock_default_days,
                        replication_mode: replication_mode.clone(),
                        deleting: false,
                    })
                    .await?
                {
                    BucketCreateOutcome::ModeConflict => ControlResponse::Conflict,
                    BucketCreateOutcome::Created | BucketCreateOutcome::Updated => {
                        ControlResponse::Ok
                    }
                }
            }
        }
        ControlCommand::BucketDelete { name } => {
            txn.delete_access_points_for_bucket(name).await?;
            txn.delete_lifecycle_inventory_tier_for_bucket(name).await?;
            txn.permissions_delete_for_bucket(name).await?;
            txn.delete_bucket_configs_for_bucket(name).await?;
            if txn.bucket_delete(name).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetDeleting { name, deleting } => {
            if txn.bucket_set_deleting(name, *deleting).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetVersioning { name, versioning } => {
            if !valid_versioning(versioning) {
                ControlResponse::Invalid
            } else if txn.bucket_set_versioning(name, versioning).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetObjectLock {
            name,
            enabled,
            default_mode,
            default_days,
        } => {
            if !valid_lock_mode(default_mode.as_deref()) {
                ControlResponse::Invalid
            } else if txn
                .bucket_set_object_lock(name, *enabled, default_mode.as_deref(), *default_days)
                .await?
            {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetNotification {
            bucket,
            config_json,
            updated_at,
        } => {
            if config_json.is_empty() {
                ControlResponse::Invalid
            } else if bucket_exists(txn, bucket).await? {
                txn.bucket_notification_set(bucket, config_json, *updated_at)
                    .await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketClearNotification { bucket } => {
            if bucket_exists(txn, bucket).await? {
                txn.bucket_notification_delete(bucket).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetLifecycle {
            bucket,
            rules,
            updated_at,
        } => {
            if bucket_exists(txn, bucket).await? {
                let rows = lifecycle_rows(bucket, rules, *updated_at);
                txn.replace_lifecycle_rules(bucket, &rows).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetLifecycleV2 {
            bucket,
            rules,
            updated_at,
        } => {
            if bucket_exists(txn, bucket).await? {
                let rows = lifecycle_rows_v2(bucket, rules, *updated_at);
                txn.replace_lifecycle_rules(bucket, &rows).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketClearLifecycle { bucket } => {
            if bucket_exists(txn, bucket).await? {
                txn.clear_lifecycle_rules(bucket).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetReplication {
            bucket,
            rules_json,
            updated_at,
        } => {
            if rules_json.is_empty() {
                ControlResponse::Invalid
            } else if bucket_exists(txn, bucket).await? {
                txn.bucket_replication_rules_set(bucket, rules_json, *updated_at)
                    .await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketClearReplication { bucket } => {
            if bucket_exists(txn, bucket).await? {
                txn.bucket_replication_rules_delete(bucket).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetDefaultEncryption { bucket, algorithm } => {
            if algorithm.is_empty() {
                ControlResponse::Invalid
            } else if txn.bucket_set_default_encryption(bucket, algorithm).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketClearDefaultEncryption { bucket } => {
            if txn.bucket_clear_default_encryption(bucket).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetPolicy {
            bucket,
            policy_json,
            updated_at,
        } => {
            if policy_json.is_empty() {
                ControlResponse::Invalid
            } else if bucket_exists(txn, bucket).await? {
                txn.bucket_policy_set(bucket, policy_json, *updated_at)
                    .await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketDeletePolicy { bucket } => {
            if bucket_exists(txn, bucket).await? {
                txn.bucket_policy_delete(bucket).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::AccessPointPut {
            name,
            bucket,
            owner,
            policy_json,
            created_at,
            updated_at,
        } => {
            // 预检与 apply 之间桶可能已被删：在 apply 内复核，拒绝悬挂行（M3）。
            if name.is_empty() || bucket.is_empty() || !bucket_exists(txn, bucket).await? {
                ControlResponse::Invalid
            } else {
                let owner = match owner {
                    None => OwnerRef::Admin,
                    Some(name) => OwnerRef::User(name.clone()),
                };
                let created = txn
                    .access_point_create(&AccessPointEntry {
                        name: name.clone(),
                        bucket: bucket.clone(),
                        owner,
                        policy_json: policy_json.clone(),
                        created_at: *created_at,
                        updated_at: *updated_at,
                    })
                    .await?;
                if created {
                    ControlResponse::Ok
                } else {
                    ControlResponse::Conflict
                }
            }
        }
        ControlCommand::AccessPointDelete { name } => {
            if txn.access_point_delete(name).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::AccessPointSetPolicy {
            name,
            policy_json,
            updated_at,
        } => {
            if txn
                .access_point_set_policy(name, policy_json.as_deref(), *updated_at)
                .await?
            {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::OlapPut {
            access_point,
            kind,
            endpoint,
            module,
            module_sha256,
            timeout_ms,
            max_response_bytes,
            updated_at,
        } => {
            if !matches!(kind.as_str(), "http" | "wasm") || access_point.is_empty() {
                ControlResponse::Invalid
            } else {
                let decoded = match module {
                    None => None,
                    Some(b64) => match base64::engine::general_purpose::STANDARD.decode(b64) {
                        Ok(bytes) => Some(bytes),
                        Err(_) => {
                            return Ok((
                                Applied {
                                    response: ControlResponse::Invalid,
                                    effects,
                                },
                                None,
                            ));
                        }
                    },
                };
                if kind == "wasm" && decoded.is_none() {
                    return Ok((
                        Applied {
                            response: ControlResponse::Invalid,
                            effects,
                        },
                        None,
                    ));
                }
                if !txn.access_point_exists(access_point).await? {
                    ControlResponse::NotFound
                } else {
                    txn.olap_config_upsert(&OlapConfigEntry {
                        access_point: access_point.clone(),
                        kind: kind.clone(),
                        endpoint: endpoint.clone(),
                        module: decoded,
                        module_sha256: module_sha256.clone(),
                        timeout_ms: *timeout_ms,
                        max_response_bytes: *max_response_bytes,
                        updated_at: *updated_at,
                    })
                    .await?;
                    ControlResponse::Ok
                }
            }
        }
        ControlCommand::OlapDelete { access_point } => {
            if txn.olap_config_delete(access_point).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::InventoryPut {
            bucket,
            id,
            owner,
            config_json,
            updated_at,
        } => {
            // 同 AccessPointPut：apply 内复核桶仍在，避免 dangling inventory 行。
            if bucket.is_empty()
                || id.is_empty()
                || config_json.is_empty()
                || !bucket_exists(txn, bucket).await?
            {
                ControlResponse::Invalid
            } else {
                let row = InventoryConfigEntry {
                    bucket: bucket.clone(),
                    id: id.clone(),
                    owner: match owner {
                        None => OwnerRef::Admin,
                        Some(username) => OwnerRef::User(username.clone()),
                    },
                    config_json: config_json.clone(),
                    updated_at: *updated_at,
                };
                txn.inventory_config_upsert(&row).await?;
                ControlResponse::Ok
            }
        }
        ControlCommand::InventoryDelete { bucket, id } => {
            if txn.inventory_config_delete(bucket, id).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        // 账户/成员命令已在上面由 `accounts::apply` 处理。
        ControlCommand::AccountCreate { .. }
        | ControlCommand::AccountUpdate { .. }
        | ControlCommand::AccountSetQuota { .. }
        | ControlCommand::AccountSetSoftQuota { .. }
        | ControlCommand::AccountHardQuotaSet { .. }
        | ControlCommand::AccountDelete { .. }
        | ControlCommand::AccountMemberAttach { .. }
        | ControlCommand::AccountMemberSetStatus { .. }
        | ControlCommand::AccountUserCreate { .. }
        | ControlCommand::AccountUsageReserve { .. }
        | ControlCommand::AccountUsageRelease { .. }
        | ControlCommand::AccountUsageAdjust { .. } => {
            unreachable!("account/ledger commands are handled before the command match")
        }
        // Level 3 命令容器：apply 只认识显式注册的 type_id；未知类型是业务拒绝，
        // 绝不能当空操作（静默忽略会让新旧副本的控制面状态分叉）。leader 已在
        // append 前按注册表拒绝，这里只是旧日志重放/快照恢复的 fail-closed 兜底。
        ControlCommand::Ext { type_id, .. } => match type_id.as_str() {
            #[cfg(test)]
            "test.noop" => ControlResponse::Ok,
            _ => ControlResponse::Invalid,
        },
        // M8-2：jobs/job_tasks 行复制（确定性 upsert，无时钟/随机）。
        ControlCommand::JobUpsert { job } => {
            apply_job_batch(txn, std::slice::from_ref(job), &[], &[]).await?
        }
        ControlCommand::JobTaskUpsert { task } => {
            apply_job_batch(txn, &[], std::slice::from_ref(task), &[]).await?
        }
        ControlCommand::JobBatch {
            jobs,
            tasks,
            restores,
        } => apply_job_batch(txn, jobs, tasks, restores).await?,
        // M8-2：inventory 运行态行复制（同批预检，零写入 Conflict）。
        ControlCommand::InventoryBatch {
            runs,
            cursors,
            incrementals,
        } => inventory::apply(txn, runs, cursors, incrementals).await?,
        ControlCommand::InventoryClear { bucket, config_id } => {
            inventory::clear(txn, bucket, config_id).await?
        }
        // M8-2：lifecycle 扫描游标复制（单行预检，零写入 Conflict）。
        ControlCommand::LifecycleCursorUpsert { cursor } => lifecycle::apply(txn, cursor).await?,
        ControlCommand::LifecycleCursorClear {
            bucket,
            rule_id,
            stage,
            fence_term,
        } => lifecycle::clear(txn, bucket, rule_id, stage, *fence_term).await?,
        // M8-2 level 6：作业历史保留（只删终态行；见 [`purge_jobs`]）。
        ControlCommand::JobPurge { job_ids } => purge_jobs(txn, job_ids).await?,
        // level 7：tier restore 后续状态 + 调度租约（确定性、零写入 Conflict）。
        ControlCommand::TierRestoreUpsert { restore } => tier_restore::apply(txn, restore).await?,
        ControlCommand::TierRestoreDelete {
            bucket,
            key,
            version_id,
            fence_term,
        } => tier_restore::delete(txn, bucket, key, version_id, *fence_term).await?,
        ControlCommand::ScheduleLeaseAcquire { lease, now } => {
            lease::apply(txn, lease, *now).await?
        }
        // feature 8：AP 动态 VPC 端点注册（upsert/delete 均幂等）。
        ControlCommand::VpcEndpointUpsert { endpoint } => {
            if !xl_s3::ap::valid_vpce_id(&endpoint.vpce_id)
                || !xl_s3::ap::valid_vpc_id(&endpoint.vpc_id)
                || endpoint.account_id.trim().is_empty()
                || !matches!(endpoint.state.as_str(), "active" | "disabled")
            {
                ControlResponse::Invalid
            } else {
                if txn.account_status(&endpoint.account_id).await?.as_deref() != Some("active") {
                    ControlResponse::Invalid
                } else {
                    txn.vpc_endpoint_upsert(endpoint).await?;
                    ControlResponse::Ok
                }
            }
        }
        ControlCommand::VpcEndpointDelete { vpce_id } => {
            txn.vpc_endpoint_delete(vpce_id).await?;
            ControlResponse::Ok
        }
        // 测试专用命令（文件日志编码用例构造 entry 用）：对象状态机不接受。
        #[cfg(test)]
        ControlCommand::Set { .. } | ControlCommand::Delete { .. } => {
            return Err(invariant("test-only command is not applicable".into()));
        }
        // 不触碰任何表：只用于验证 level-2 激活标记与业务拒绝同样写 marker。
        #[cfg(test)]
        ControlCommand::FeatureProbe { reject } => {
            if *reject {
                ControlResponse::Invalid
            } else {
                ControlResponse::Ok
            }
        }
    };
    Ok((Applied { response, effects }, user_delete))
}

fn task_terminal(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled")
}

/// 作业状态机：终态冻结，其余按 worker 白名单（见 `job_state::admin`）。
fn job_state_ok(from: &str, to: &str) -> bool {
    from == to
        || matches!(
            (from, to),
            ("queued", "running")
                | ("queued", "cancelled")
                | ("running", "paused")
                | ("running", "preempted")
                | ("running", "done")
                | ("running", "error")
                | ("running", "cancelled")
                | ("paused", "running")
                | ("paused", "done")
                | ("paused", "error")
                | ("paused", "cancelled")
                | ("preempted", "queued")
                | ("preempted", "paused")
                | ("preempted", "done")
                | ("preempted", "error")
                | ("preempted", "cancelled")
        )
}

fn task_state_ok(from: &str, to: &str) -> bool {
    from == to
        || matches!(
            (from, to),
            ("queued", "running")
                | ("queued", "cancelled")
                | ("running", "queued")
                | ("running", "done")
                | ("running", "failed")
                | ("running", "cancelled")
        )
}

/// 纯函数校验：当前折叠行 + 日志行。日志全序下每个副本在同一 index 看到相同输入，
/// 判定因此确定；leader 在 append 前做同一套身份/版本校验（P1-2）。
fn job_row_ok(cur: Option<&JobRow>, next: &JobRow) -> Result<(), String> {
    let Some(cur) = cur else {
        return if matches!(next.state.as_str(), "queued" | "running") {
            Ok(())
        } else {
            Err(format!("job {}: new row in state {}", next.id, next.state))
        };
    };
    if cur.kind != next.kind
        || cur.created_at != next.created_at
        || cur.bucket != next.bucket
        || cur.prefix != next.prefix
        || cur.spec_json != next.spec_json
    {
        return Err(format!("job {}: identity changed", next.id));
    }
    if next.updated_at <= cur.updated_at {
        return Err(format!("job {}: stale version", next.id));
    }
    if !job_state_ok(&cur.state, &next.state) {
        return Err(format!(
            "job {}: illegal state {} -> {}",
            next.id, cur.state, next.state
        ));
    }
    for (name, old, new) in [
        ("scanned", cur.scanned, next.scanned),
        ("deleted", cur.deleted, next.deleted),
        ("skipped", cur.skipped, next.skipped),
        ("total", cur.total, next.total),
        ("succeeded", cur.succeeded, next.succeeded),
        ("failed", cur.failed, next.failed),
    ] {
        if new < old {
            return Err(format!("job {}: {name} regressed {old} -> {new}", next.id));
        }
    }
    // progress 不设单调闸：inventory 用 scanned-1 抵消引擎的提前 +1。
    if cur.started_at.is_some() && next.started_at != cur.started_at {
        return Err(format!("job {}: started_at changed", next.id));
    }
    if cur.finished_at.is_some() && next.finished_at != cur.finished_at {
        return Err(format!("job {}: finished_at changed", next.id));
    }
    if cur.error.is_some() && next.error.is_none() {
        return Err(format!("job {}: error cleared", next.id));
    }
    Ok(())
}

fn task_row_ok(cur: Option<&JobTaskRow>, next: &JobTaskRow) -> Result<(), String> {
    let Some(cur) = cur else {
        return if matches!(next.state.as_str(), "queued" | "running") {
            Ok(())
        } else {
            Err(format!(
                "task {}/{}: new row in state {}",
                next.job_id, next.task_no, next.state
            ))
        };
    };
    if task_terminal(&cur.state) {
        // 终态行是日志事实：重复 apply 必须逐字段幂等，不得复活/改写。
        return if next.state == cur.state
            && next.fence_term == cur.fence_term
            && next.attempt == cur.attempt
            && next.fence_leader == cur.fence_leader
            && next.result_code == cur.result_code
            && next.result_string == cur.result_string
            && next.action == cur.action
            && next.payload_json == cur.payload_json
            && next.next_at == cur.next_at
            && next.deadline_at == cur.deadline_at
            && next.started_at == cur.started_at
            && next.finished_at == cur.finished_at
        {
            Ok(())
        } else {
            Err(format!(
                "task {}/{}: terminal row changed",
                next.job_id, next.task_no
            ))
        };
    }
    if next.fence_term < cur.fence_term || next.attempt < cur.attempt {
        return Err(format!(
            "task {}/{}: fence/attempt regressed",
            next.job_id, next.task_no
        ));
    }
    if !task_state_ok(&cur.state, &next.state) {
        return Err(format!(
            "task {}/{}: illegal state {} -> {}",
            next.job_id, next.task_no, cur.state, next.state
        ));
    }
    Ok(())
}

async fn current_job(txn: &mut dyn ControlTxn, id: i64) -> Result<Option<JobRow>, ControlError> {
    txn.job(id).await
}

async fn current_task(
    txn: &mut dyn ControlTxn,
    job_id: i64,
    task_no: i64,
) -> Result<Option<JobTaskRow>, ControlError> {
    txn.job_task(job_id, task_no).await
}

/// Restore 行按 `(bucket,key,version_id)` 读取（P2-1：创建行随 JobBatch 复制）。
async fn current_restore(
    txn: &mut dyn ControlTxn,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Option<TierRestoreRow>, ControlError> {
    txn.tier_restore(bucket, key, version_id).await
}

/// Restore 行只允许已知状态，且 fence 不得回退（行被更高 term 接管后旧日志不得降级）。
pub(crate) fn restore_row_ok(
    cur: Option<&TierRestoreRow>,
    next: &TierRestoreRow,
) -> Result<(), String> {
    if next.bucket.is_empty() || next.key.is_empty() || next.version_id.is_empty() {
        return Err(format!(
            "restore {}/{}/{}: empty natural key",
            next.bucket, next.key, next.version_id
        ));
    }
    if !matches!(next.state.as_str(), "pending" | "restored") {
        return Err(format!(
            "restore {}/{}/{}: unknown state {}",
            next.bucket, next.key, next.version_id, next.state
        ));
    }
    if cur.is_some_and(|cur| next.fence_term < cur.fence_term) {
        return Err(format!(
            "restore {}/{}/{}: fence regressed",
            next.bucket, next.key, next.version_id
        ));
    }
    Ok(())
}

/// 去重键唯一（partial unique index）。同 PK 保留自键；冲突是业务拒绝而非 SQL 错误。
async fn dedupe_taken(
    txn: &mut dyn ControlTxn,
    key: &str,
    job_id: i64,
    task_no: i64,
) -> Result<bool, ControlError> {
    txn.dedupe_taken(key, job_id, task_no).await
}

/// 按主键 upsert 作业行。防回退判定已由 [`job_row_ok`] 在写入前完成，这里不再
/// 用 SQL WHERE 静默吞行（避免半批边界）。
async fn apply_job(txn: &mut dyn ControlTxn, job: &JobRow) -> Result<u64, ControlError> {
    txn.job_upsert(job).await
}

/// 按 `(job_id, task_no)` upsert。单调/终态判定见 [`task_row_ok`]；终态行恒清
/// `dedupe_key`（延迟重放的旧行不能把已释放的去重键塞回去）。
async fn apply_job_task(txn: &mut dyn ControlTxn, task: &JobTaskRow) -> Result<u64, ControlError> {
    txn.job_task_upsert(task).await
}

/// 整批语义：先只读校验全部行，任一行不合法 → `Conflict` 且零写入；校验通过后
/// 每行 upsert 必须恰好影响 1 行（否则内部不变量破坏，fail-stop）。
async fn apply_job_batch(
    txn: &mut dyn ControlTxn,
    jobs: &[JobRow],
    tasks: &[JobTaskRow],
    restores: &[TierRestoreRow],
) -> Result<ControlResponse, ControlError> {
    let mut batch_ids = HashSet::new();
    for job in jobs {
        if !batch_ids.insert(job.id) {
            return Ok(ControlResponse::Conflict);
        }
        let cur = current_job(txn, job.id).await?;
        if job_row_ok(cur.as_ref(), job).is_err() {
            return Ok(ControlResponse::Conflict);
        }
    }
    let mut seen = HashSet::new();
    let mut keys = HashSet::new();
    for task in tasks {
        if !seen.insert((task.job_id, task.task_no)) {
            return Ok(ControlResponse::Conflict);
        }
        if !batch_ids.contains(&task.job_id) && current_job(txn, task.job_id).await?.is_none() {
            return Ok(ControlResponse::Conflict);
        }
        let cur = current_task(txn, task.job_id, task.task_no).await?;
        if task_row_ok(cur.as_ref(), task).is_err() {
            return Ok(ControlResponse::Conflict);
        }
        if !task_terminal(&task.state)
            && let Some(key) = &task.dedupe_key
            && (!keys.insert(key.clone())
                || dedupe_taken(txn, key, task.job_id, task.task_no).await?)
        {
            return Ok(ControlResponse::Conflict);
        }
    }
    let mut restore_keys = HashSet::new();
    for restore in restores {
        let key = (
            restore.bucket.clone(),
            restore.key.clone(),
            restore.version_id.clone(),
        );
        if !restore_keys.insert(key) {
            return Ok(ControlResponse::Conflict);
        }
        // 与任务同规则：引用的 job 必须在本批或本地已存在（RunState::validate 不允许悬空）。
        if let Some(job_id) = restore.job_id
            && !batch_ids.contains(&job_id)
            && current_job(txn, job_id).await?.is_none()
        {
            return Ok(ControlResponse::Conflict);
        }
        let cur = current_restore(txn, &restore.bucket, &restore.key, &restore.version_id).await?;
        if restore_row_ok(cur.as_ref(), restore).is_err() {
            return Ok(ControlResponse::Conflict);
        }
    }
    for job in jobs {
        if apply_job(txn, job).await? != 1 {
            return Err(invariant(format!("job {} upsert affected 0 rows", job.id)));
        }
    }
    for task in tasks {
        if apply_job_task(txn, task).await? != 1 {
            return Err(invariant(format!(
                "task {}/{} upsert affected 0 rows",
                task.job_id, task.task_no
            )));
        }
    }
    for restore in restores {
        let applied = txn.tier_restore_upsert(restore).await?;
        if !applied {
            return Err(invariant(format!(
                "restore {}/{}/{} upsert affected 0 rows",
                restore.bucket, restore.key, restore.version_id
            )));
        }
    }
    Ok(ControlResponse::Ok)
}

/// level 6 作业历史保留：只删除当前仍为终态的 jobs 及其 job_tasks，并解除
/// inventory_runs/tier_restores/`parent_job_id` 引用（保持 `RunState::validate` 无悬空）。
/// 响应语义（确定，不随副本变化）：
/// - 至少删除一行，或所有 id 都已不存在 → `Ok`（重复 apply/已清理 = 幂等成功）；
/// - 一行未删且仍有 id 存在（非终态）→ `Conflict`（零写入，保留 queued/running/preempted）；
/// - 混合时终态行照删并返回 `Ok`，非终态行保留。
async fn purge_jobs(
    txn: &mut dyn ControlTxn,
    job_ids: &[i64],
) -> Result<ControlResponse, ControlError> {
    if validate_job_purge(job_ids).is_err() {
        return Ok(ControlResponse::Invalid);
    }
    let (deleted, non_terminal) = txn.purge_terminal_jobs(job_ids).await?;
    Ok(if deleted > 0 || non_terminal == 0 {
        ControlResponse::Ok
    } else {
        ControlResponse::Conflict
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use xl_store::control_store::account::AccountTxn;

    /// 状态机白名单必须覆盖 worker 的真实转换（含 paused/preempted 收尾），且终态冻结。
    #[test]
    fn job_state_allowlist_covers_worker_transitions() {
        for (from, to) in [
            ("queued", "running"),
            ("queued", "cancelled"),
            ("running", "paused"),
            ("running", "preempted"),
            ("running", "done"),
            ("running", "error"),
            ("running", "cancelled"),
            ("paused", "running"),
            ("paused", "done"),
            ("paused", "error"),
            ("paused", "cancelled"),
            ("preempted", "queued"),
            ("preempted", "paused"),
            ("preempted", "done"),
            ("preempted", "error"),
            ("preempted", "cancelled"),
            ("done", "done"),
        ] {
            assert!(job_state_ok(from, to), "{from} -> {to}");
        }
        for (from, to) in [
            ("done", "running"),
            ("error", "queued"),
            ("cancelled", "running"),
            ("queued", "done"),
            ("preempted", "running"),
        ] {
            assert!(!job_state_ok(from, to), "{from} -> {to}");
        }
    }

}
