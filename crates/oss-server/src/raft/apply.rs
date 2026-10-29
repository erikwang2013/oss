//! M9 控制命令的确定性 apply：自然键解析 + 一个事务内的控制行写入。
//!
//! 约束（计划 §2.3）：apply 是 11 张复制表的唯一写入者；只在事务内做 SQL，不生成
//! 随机数/时间戳/hash/seal；业务拒绝用 [`ControlResponse`]（事务照常提交），内部不变量
//! 破坏返回 `Err`（调用方 fail-stop，事务回滚）。`ControlWrite` 是写入口的编译闸：
//! 构造函数只在本模块（以及 `Local` façade 调用本模块时）出现。

use base64::Engine as _;
use std::collections::HashSet;

use sqlx::SqliteConnection;

mod accounts;

use super::command::{ControlCommand, ControlResponse, LifecycleRuleCmd, PermissionBits};
use super::runstate::{JobRow, JobTaskRow, job_row_from, job_task_row_from};
use crate::db::{CreateBucketOutcome, Db, LifecycleRuleRow, PermissionRow};

/// 控制表写入令牌。字段对 crate 内 raw writer 可见；模块私有，外部无法构造。
pub(crate) struct ControlWrite<'a> {
    pub(crate) conn: &'a mut SqliteConnection,
}

/// 唯一令牌构造器（apply 事务 / Local façade 事务）。
pub(crate) fn control_tx(conn: &mut SqliteConnection) -> ControlWrite<'_> {
    ControlWrite { conn }
}

impl ControlWrite<'_> {
    pub(crate) fn conn(&mut self) -> &mut SqliteConnection {
        self.conn
    }
}

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

fn invariant(msg: String) -> sqlx::Error {
    sqlx::Error::Protocol(msg)
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

async fn uid(cw: &mut ControlWrite<'_>, username: &str) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar("SELECT id FROM users WHERE username = ?")
        .bind(username)
        .fetch_optional(&mut *cw.conn)
        .await
}

async fn admin_id(cw: &mut ControlWrite<'_>) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1")
        .fetch_optional(&mut *cw.conn)
        .await
}

/// M1：管理员用户/AK 是站点本地状态，IAM 命令不得复制它们（S6b 处理器留本地）。
async fn username_is_admin(cw: &mut ControlWrite<'_>, username: &str) -> Result<bool, sqlx::Error> {
    let row: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM users WHERE username = ? AND is_admin = 1")
            .bind(username)
            .fetch_optional(&mut *cw.conn)
            .await?;
    Ok(row.is_some())
}

async fn access_key_owner_is_admin(
    cw: &mut ControlWrite<'_>,
    access_key: &str,
) -> Result<bool, sqlx::Error> {
    let row: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM access_keys a JOIN users u ON u.id = a.user_id \
         WHERE a.access_key = ? AND u.is_admin = 1",
    )
    .bind(access_key)
    .fetch_optional(&mut *cw.conn)
    .await?;
    Ok(row.is_some())
}

async fn bucket_exists(cw: &mut ControlWrite<'_>, bucket: &str) -> Result<bool, sqlx::Error> {
    let row: Option<i64> = sqlx::query_scalar("SELECT 1 FROM buckets WHERE name = ?")
        .bind(bucket)
        .fetch_optional(&mut *cw.conn)
        .await?;
    Ok(row.is_some())
}

/// buckets.owner_id：`None`/管理员/无本地管理员（witness）→ NULL；普通 username → 本地 id。
async fn bucket_owner_id(
    cw: &mut ControlWrite<'_>,
    owner: &Option<String>,
) -> Result<Option<i64>, sqlx::Error> {
    let Some(username) = owner else {
        return Ok(None);
    };
    if let Some(id) = uid(cw, username).await? {
        return Ok(Some(id));
    }
    if admin_id(cw).await?.is_some() {
        // 有序命令流里普通用户必已先建；缺失 = 不变量破坏 → fail-stop。
        return Err(invariant(format!("bucket owner user {username} not found")));
    }
    // witness / 本地管理员尚未 bootstrap：管理员属主占位 NULL（启动修复再收敛）。
    Ok(None)
}

/// AP/inventory owner（NOT NULL）：管理员 → 本地管理员 id；witness/未 bootstrap → 0。
async fn admin_owner_id(
    cw: &mut ControlWrite<'_>,
    owner: &Option<String>,
) -> Result<i64, sqlx::Error> {
    let admin = admin_id(cw).await?;
    match owner {
        None => Ok(admin.unwrap_or(0)),
        Some(username) => match uid(cw, username).await? {
            Some(id) => Ok(id),
            None if admin.is_some() => Err(invariant(format!("owner user {username} not found"))),
            None => Ok(0),
        },
    }
}

fn permissions_rows(bits: &[PermissionBits]) -> Vec<PermissionRow> {
    let mut rows: Vec<PermissionRow> = bits
        .iter()
        .filter(|p| p.read || p.write || p.delete)
        .map(|p| PermissionRow {
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

/// 应用一条命令。业务拒绝 = `Ok(Applied{response})`；内部错误 = `Err`（事务回滚，fail-stop）。
pub(crate) async fn apply_command(
    cw: &mut ControlWrite<'_>,
    cmd: &ControlCommand,
) -> Result<Applied, sqlx::Error> {
    let effects = effects_for(cmd);
    // Level-3 账户/成员命令有独立的业务约束集合（自然键 + 账户不变量）。
    if let Some(response) = accounts::apply(cw, cmd).await? {
        return Ok(Applied { response, effects });
    }
    let response = match cmd {
        ControlCommand::UserCreate {
            username,
            password_hash,
            status,
            created_at,
        } => {
            if username.is_empty() || password_hash.is_empty() || !valid_user_status(status) {
                ControlResponse::Invalid
            } else if uid(cw, username).await?.is_some() {
                ControlResponse::Conflict
            } else {
                Db::create_user_raw(cw, username, password_hash, status, *created_at).await?;
                ControlResponse::Ok
            }
        }
        ControlCommand::UserDelete { username } => {
            if username_is_admin(cw, username).await? {
                ControlResponse::Invalid
            } else {
                match uid(cw, username).await? {
                    None => ControlResponse::NotFound,
                    Some(id) => {
                        if Db::delete_user_cascade(cw, id).await? {
                            ControlResponse::Ok
                        } else {
                            // 守卫拒绝：目标是最后一个在册管理员（命令路径本不产生管理员）。
                            ControlResponse::Conflict
                        }
                    }
                }
            }
        }
        ControlCommand::UserSetPassword {
            username,
            password_hash,
        } => {
            if password_hash.is_empty() || username_is_admin(cw, username).await? {
                ControlResponse::Invalid
            } else {
                match uid(cw, username).await? {
                    None => ControlResponse::NotFound,
                    Some(id) => {
                        if Db::set_password_raw(cw, id, password_hash).await? {
                            ControlResponse::Ok
                        } else {
                            ControlResponse::NotFound
                        }
                    }
                }
            }
        }
        ControlCommand::UserSetStatus { username, status } => {
            if !valid_user_status(status) || username_is_admin(cw, username).await? {
                ControlResponse::Invalid
            } else {
                match uid(cw, username).await? {
                    None => ControlResponse::NotFound,
                    Some(id) => {
                        if Db::set_user_status_raw(cw, id, status == "active").await? {
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
            if username_is_admin(cw, username).await? {
                ControlResponse::Invalid
            } else {
                match uid(cw, username).await? {
                    None => ControlResponse::NotFound,
                    Some(id) => {
                        Db::set_permissions_raw(cw, id, &permissions_rows(permissions)).await?;
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
                || username_is_admin(cw, username).await?
            {
                ControlResponse::Invalid
            } else {
                let exists: Option<i64> =
                    sqlx::query_scalar("SELECT 1 FROM access_keys WHERE access_key = ?")
                        .bind(access_key)
                        .fetch_optional(&mut *cw.conn)
                        .await?;
                if exists.is_some() {
                    ControlResponse::Conflict
                } else {
                    // 属主缺失（非 witness）是不变量破坏：AK 会永远无法鉴权。
                    let Some(user_id) = uid(cw, username).await? else {
                        return Err(invariant(format!("access key owner {username} not found")));
                    };
                    Db::create_access_key_raw(
                        cw,
                        access_key,
                        secret_key,
                        user_id,
                        kind,
                        parent_ak.as_deref(),
                        session_policy.as_deref(),
                        *created_at,
                    )
                    .await?;
                    ControlResponse::Ok
                }
            }
        }
        ControlCommand::AccessKeyDelete { access_key } => {
            if access_key_owner_is_admin(cw, access_key).await? {
                ControlResponse::Invalid
            } else if Db::delete_access_key_raw(cw, access_key).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::AccessKeySetStatus { access_key, status } => {
            if !valid_user_status(status) || access_key_owner_is_admin(cw, access_key).await? {
                ControlResponse::Invalid
            } else if Db::set_access_key_status_raw(cw, access_key, status).await? {
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
                let owner_id = bucket_owner_id(cw, owner).await?;
                match Db::create_bucket_tx(
                    cw,
                    name,
                    owner_id,
                    *created_at,
                    replication_mode,
                    versioning,
                    *object_lock,
                    object_lock_default_mode.as_deref(),
                    *object_lock_default_days,
                    default_encryption.as_deref(),
                )
                .await?
                {
                    CreateBucketOutcome::ModeConflict => ControlResponse::Conflict,
                    CreateBucketOutcome::Created | CreateBucketOutcome::Updated => {
                        ControlResponse::Ok
                    }
                }
            }
        }
        ControlCommand::BucketDelete { name } => {
            if Db::delete_bucket_cascade(cw, name).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetDeleting { name, deleting } => {
            if Db::set_bucket_deleting_raw(cw, name, *deleting).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetVersioning { name, versioning } => {
            if !valid_versioning(versioning) {
                ControlResponse::Invalid
            } else if Db::set_bucket_versioning_raw(cw, name, versioning).await? {
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
            } else if Db::set_bucket_object_lock_raw(
                cw,
                name,
                *enabled,
                default_mode.as_deref(),
                *default_days,
            )
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
            } else if bucket_exists(cw, bucket).await? {
                Db::set_bucket_notification_raw(cw, bucket, config_json, *updated_at).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketClearNotification { bucket } => {
            if bucket_exists(cw, bucket).await? {
                Db::clear_bucket_notification_raw(cw, bucket).await?;
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
            if bucket_exists(cw, bucket).await? {
                Db::set_lifecycle_rules_raw(
                    cw,
                    bucket,
                    &lifecycle_rows(bucket, rules, *updated_at),
                    *updated_at,
                )
                .await?;
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
            if bucket_exists(cw, bucket).await? {
                Db::set_lifecycle_rules_raw(
                    cw,
                    bucket,
                    &lifecycle_rows_v2(bucket, rules, *updated_at),
                    *updated_at,
                )
                .await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketClearLifecycle { bucket } => {
            if bucket_exists(cw, bucket).await? {
                Db::clear_lifecycle_rules_raw(cw, bucket).await?;
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
            } else if bucket_exists(cw, bucket).await? {
                Db::set_replication_rules_raw(cw, bucket, rules_json, *updated_at).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketClearReplication { bucket } => {
            if bucket_exists(cw, bucket).await? {
                Db::clear_replication_rules_raw(cw, bucket).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketSetDefaultEncryption { bucket, algorithm } => {
            if algorithm.is_empty() {
                ControlResponse::Invalid
            } else if Db::set_bucket_default_encryption_raw(cw, bucket, algorithm).await? {
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketClearDefaultEncryption { bucket } => {
            if Db::clear_bucket_default_encryption_raw(cw, bucket).await? {
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
            } else if bucket_exists(cw, bucket).await? {
                Db::set_bucket_policy_raw(cw, bucket, policy_json, *updated_at).await?;
                ControlResponse::Ok
            } else {
                ControlResponse::NotFound
            }
        }
        ControlCommand::BucketDeletePolicy { bucket } => {
            if bucket_exists(cw, bucket).await? {
                Db::delete_bucket_policy_raw(cw, bucket).await?;
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
            if name.is_empty() || bucket.is_empty() || !bucket_exists(cw, bucket).await? {
                ControlResponse::Invalid
            } else {
                let owner_id = admin_owner_id(cw, owner).await?;
                if Db::create_access_point_raw(
                    cw,
                    name,
                    bucket,
                    owner_id,
                    policy_json.as_deref(),
                    *created_at,
                    *updated_at,
                )
                .await?
                {
                    ControlResponse::Ok
                } else {
                    ControlResponse::Conflict
                }
            }
        }
        ControlCommand::AccessPointDelete { name } => {
            if Db::delete_access_point_cascade(cw, name).await? {
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
            if Db::set_access_point_policy_raw(cw, name, policy_json.as_deref(), *updated_at)
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
                            return Ok(Applied {
                                response: ControlResponse::Invalid,
                                effects,
                            });
                        }
                    },
                };
                if kind == "wasm" && decoded.is_none() {
                    return Ok(Applied {
                        response: ControlResponse::Invalid,
                        effects,
                    });
                }
                if !bucket_exists_for_ap(cw, access_point).await? {
                    ControlResponse::NotFound
                } else {
                    Db::olap_upsert_raw(
                        cw,
                        access_point,
                        kind,
                        endpoint.as_deref(),
                        decoded.as_deref(),
                        module_sha256.as_deref(),
                        *timeout_ms,
                        *max_response_bytes,
                        *updated_at,
                    )
                    .await?;
                    ControlResponse::Ok
                }
            }
        }
        ControlCommand::OlapDelete { access_point } => {
            if Db::olap_delete_raw(cw, access_point).await? {
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
                || !bucket_exists(cw, bucket).await?
            {
                ControlResponse::Invalid
            } else {
                let owner_id = admin_owner_id(cw, owner).await?;
                Db::inventory_upsert_raw(cw, bucket, id, owner_id, config_json, *updated_at)
                    .await?;
                ControlResponse::Ok
            }
        }
        ControlCommand::InventoryDelete { bucket, id } => {
            if Db::inventory_delete_raw(cw, bucket, id).await? {
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
        | ControlCommand::AccountDelete { .. }
        | ControlCommand::AccountMemberAttach { .. }
        | ControlCommand::AccountMemberSetStatus { .. }
        | ControlCommand::AccountUserCreate { .. } => {
            unreachable!("account commands are handled before the command match")
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
            apply_job_batch(cw, std::slice::from_ref(job), &[]).await?
        }
        ControlCommand::JobTaskUpsert { task } => {
            apply_job_batch(cw, &[], std::slice::from_ref(task)).await?
        }
        ControlCommand::JobBatch { jobs, tasks } => apply_job_batch(cw, jobs, tasks).await?,
        #[cfg(test)]
        ControlCommand::Set { key, value } => {
            sqlx::query("INSERT OR REPLACE INTO raft_kv (key, value) VALUES (?, ?)")
                .bind(key)
                .bind(value)
                .execute(&mut *cw.conn)
                .await?;
            ControlResponse::Ok
        }
        #[cfg(test)]
        ControlCommand::Delete { key } => {
            sqlx::query("DELETE FROM raft_kv WHERE key = ?")
                .bind(key)
                .execute(&mut *cw.conn)
                .await?;
            ControlResponse::Ok
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
    Ok(Applied { response, effects })
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
    if cur.kind != next.kind || cur.created_at != next.created_at {
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

async fn current_job(cw: &mut ControlWrite<'_>, id: i64) -> Result<Option<JobRow>, sqlx::Error> {
    let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut *cw.conn)
        .await?;
    Ok(row.as_ref().map(job_row_from))
}

async fn current_task(
    cw: &mut ControlWrite<'_>,
    job_id: i64,
    task_no: i64,
) -> Result<Option<JobTaskRow>, sqlx::Error> {
    let row = sqlx::query("SELECT * FROM job_tasks WHERE job_id = ? AND task_no = ?")
        .bind(job_id)
        .bind(task_no)
        .fetch_optional(&mut *cw.conn)
        .await?;
    Ok(row.as_ref().map(job_task_row_from))
}

/// 去重键唯一（partial unique index）。同 PK 保留自键；冲突是业务拒绝而非 SQL 错误。
async fn dedupe_taken(
    cw: &mut ControlWrite<'_>,
    key: &str,
    job_id: i64,
    task_no: i64,
) -> Result<bool, sqlx::Error> {
    let row: Option<(i64, i64)> =
        sqlx::query_as("SELECT job_id, task_no FROM job_tasks WHERE dedupe_key = ?")
            .bind(key)
            .fetch_optional(&mut *cw.conn)
            .await?;
    Ok(row.is_some_and(|(j, t)| j != job_id || t != task_no))
}

/// 按主键 upsert 作业行。防回退判定已由 [`job_row_ok`] 在写入前完成，这里不再
/// 用 SQL WHERE 静默吞行（避免半批边界）。
async fn apply_job(cw: &mut ControlWrite<'_>, job: &JobRow) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO jobs (id, kind, bucket, prefix, state, scanned, deleted, skipped, error, \
          created_at, updated_at, spec_json, progress, total, succeeded, failed, report_bucket, \
          report_prefix, priority, requested_by, parent_job_id, started_at, finished_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET \
          kind = excluded.kind, bucket = excluded.bucket, prefix = excluded.prefix, \
          state = excluded.state, scanned = excluded.scanned, deleted = excluded.deleted, \
          skipped = excluded.skipped, error = excluded.error, created_at = excluded.created_at, \
          updated_at = excluded.updated_at, spec_json = excluded.spec_json, \
          progress = excluded.progress, total = excluded.total, \
          succeeded = excluded.succeeded, failed = excluded.failed, \
          report_bucket = excluded.report_bucket, report_prefix = excluded.report_prefix, \
          priority = excluded.priority, requested_by = excluded.requested_by, \
          parent_job_id = excluded.parent_job_id, started_at = excluded.started_at, \
          finished_at = excluded.finished_at",
    )
    .bind(job.id)
    .bind(&job.kind)
    .bind(&job.bucket)
    .bind(&job.prefix)
    .bind(&job.state)
    .bind(job.scanned)
    .bind(job.deleted)
    .bind(job.skipped)
    .bind(&job.error)
    .bind(job.created_at)
    .bind(job.updated_at)
    .bind(&job.spec_json)
    .bind(job.progress)
    .bind(job.total)
    .bind(job.succeeded)
    .bind(job.failed)
    .bind(&job.report_bucket)
    .bind(&job.report_prefix)
    .bind(job.priority)
    .bind(job.requested_by)
    .bind(job.parent_job_id)
    .bind(job.started_at)
    .bind(job.finished_at)
    .execute(&mut *cw.conn)
    .await?;
    Ok(result.rows_affected())
}

/// 按 `(job_id, task_no)` upsert。单调/终态判定见 [`task_row_ok`]；终态行恒清
/// `dedupe_key`（延迟重放的旧行不能把已释放的去重键塞回去）。
async fn apply_job_task(cw: &mut ControlWrite<'_>, task: &JobTaskRow) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO job_tasks (job_id, task_no, action, payload_json, state, attempt, next_at, \
          dedupe_key, deadline_at, result_code, result_string, started_at, finished_at, \
          fence_term, fence_leader) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(job_id, task_no) DO UPDATE SET \
          action = excluded.action, payload_json = excluded.payload_json, \
          state = excluded.state, attempt = excluded.attempt, next_at = excluded.next_at, \
          dedupe_key = CASE WHEN excluded.state IN ('done','failed','cancelled') THEN NULL \
                            ELSE excluded.dedupe_key END, \
          deadline_at = excluded.deadline_at, result_code = excluded.result_code, \
          result_string = excluded.result_string, started_at = excluded.started_at, \
          finished_at = excluded.finished_at, fence_term = excluded.fence_term, \
          fence_leader = excluded.fence_leader",
    )
    .bind(task.job_id)
    .bind(task.task_no)
    .bind(&task.action)
    .bind(&task.payload_json)
    .bind(&task.state)
    .bind(task.attempt)
    .bind(task.next_at)
    .bind(&task.dedupe_key)
    .bind(task.deadline_at)
    .bind(&task.result_code)
    .bind(&task.result_string)
    .bind(task.started_at)
    .bind(task.finished_at)
    .bind(task.fence_term)
    .bind(task.fence_leader)
    .execute(&mut *cw.conn)
    .await?;
    Ok(result.rows_affected())
}

/// 整批语义：先只读校验全部行，任一行不合法 → `Conflict` 且零写入；校验通过后
/// 每行 upsert 必须恰好影响 1 行（否则内部不变量破坏，fail-stop）。
async fn apply_job_batch(
    cw: &mut ControlWrite<'_>,
    jobs: &[JobRow],
    tasks: &[JobTaskRow],
) -> Result<ControlResponse, sqlx::Error> {
    let mut batch_ids = HashSet::new();
    for job in jobs {
        if !batch_ids.insert(job.id) {
            return Ok(ControlResponse::Conflict);
        }
        let cur = current_job(cw, job.id).await?;
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
        if !batch_ids.contains(&task.job_id) && current_job(cw, task.job_id).await?.is_none() {
            return Ok(ControlResponse::Conflict);
        }
        let cur = current_task(cw, task.job_id, task.task_no).await?;
        if task_row_ok(cur.as_ref(), task).is_err() {
            return Ok(ControlResponse::Conflict);
        }
        if !task_terminal(&task.state)
            && let Some(key) = &task.dedupe_key
            && (!keys.insert(key.clone())
                || dedupe_taken(cw, key, task.job_id, task.task_no).await?)
        {
            return Ok(ControlResponse::Conflict);
        }
    }
    for job in jobs {
        if apply_job(cw, job).await? != 1 {
            return Err(invariant(format!("job {} upsert affected 0 rows", job.id)));
        }
    }
    for task in tasks {
        if apply_job_task(cw, task).await? != 1 {
            return Err(invariant(format!(
                "task {}/{} upsert affected 0 rows",
                task.job_id, task.task_no
            )));
        }
    }
    Ok(ControlResponse::Ok)
}

async fn bucket_exists_for_ap(
    cw: &mut ControlWrite<'_>,
    access_point: &str,
) -> Result<bool, sqlx::Error> {
    // 变换器只能挂到已存在的 AP（olap_configs.access_point 是外键语义）。
    let row: Option<i64> = sqlx::query_scalar("SELECT 1 FROM access_points WHERE name = ?")
        .bind(access_point)
        .fetch_optional(&mut *cw.conn)
        .await?;
    Ok(row.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Level 3 `Ext`：未知 type_id 是业务拒绝；cfg(test) 探针证明容器可提交。
    #[tokio::test]
    async fn ext_unknown_type_is_invalid_and_test_probe_is_ok() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut cw = control_tx(&mut conn);
        let noop = ControlCommand::Ext {
            type_id: "test.noop".into(),
            payload: serde_json::json!({}),
        };
        assert_eq!(
            apply_command(&mut cw, &noop).await.unwrap().response,
            ControlResponse::Ok
        );
        let unknown = ControlCommand::Ext {
            type_id: "future.unknown".into(),
            payload: serde_json::json!({"k": "v"}),
        };
        assert_eq!(
            apply_command(&mut cw, &unknown).await.unwrap().response,
            ControlResponse::Invalid
        );
    }

    /// witness 无本地管理员：AP/inventory owner 用哨兵 0（S7 快照安装同映射）。
    #[tokio::test]
    async fn witness_inventory_owner_sentinel_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("witness.db").display()
        );
        let db = Db::connect_witness(&url).await.unwrap();
        let cmd = ControlCommand::InventoryPut {
            bucket: "b".into(),
            id: "i".into(),
            owner: None,
            config_json: "{}".into(),
            updated_at: 1,
        };
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
            .execute(&mut *tx)
            .await
            .unwrap();
        // S6d 复核：InventoryPut 要求桶仍存在（同事务内造桶）。
        sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES ('b', NULL, 1)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let applied = {
            let mut cw = control_tx(&mut tx);
            apply_command(&mut cw, &cmd).await.unwrap()
        };
        tx.commit().await.unwrap();
        assert_eq!(applied.response, ControlResponse::Ok);
        let owner: i64 = sqlx::query_scalar(
            "SELECT owner_id FROM inventory_configs WHERE bucket = 'b' AND id = 'i'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(owner, 0);
    }

    /// AC-2 修订：apply 本身不做配额判定（旧副本行为不变，混版不分叉）；
    /// 权威闸在 leader append 前（`RaftHandle`），不经过 `ControlResponse`。
    #[tokio::test]
    async fn bucket_create_apply_ignores_quota() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        db.create_user("alice", "pw", false).await.unwrap();
        // 默认账户 id=000000000001，create_user 已把 alice 挂为成员；只收紧配额。
        sqlx::query("UPDATE accounts SET bucket_quota = 1 WHERE id = '000000000001'")
            .execute(db.pool())
            .await
            .unwrap();
        let cmd = |name: &str| ControlCommand::BucketCreate {
            name: name.into(),
            owner: Some("alice".into()),
            created_at: 1,
            replication_mode: "async".into(),
            versioning: "Off".into(),
            object_lock: false,
            object_lock_default_mode: None,
            object_lock_default_days: None,
            default_encryption: None,
        };
        let mut tx = db.pool().begin().await.unwrap();
        {
            let mut cw = control_tx(&mut tx);
            for name in ["q-1", "q-2"] {
                assert_eq!(
                    apply_command(&mut cw, &cmd(name)).await.unwrap().response,
                    ControlResponse::Ok,
                    "apply 不判配额（quota=1 仍落两桶）"
                );
            }
        }
        tx.commit().await.unwrap();
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 2);
    }
}
