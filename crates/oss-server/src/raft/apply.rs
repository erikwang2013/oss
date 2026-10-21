//! M9 控制命令的确定性 apply：自然键解析 + 一个事务内的控制行写入。
//!
//! 约束（计划 §2.3）：apply 是 11 张复制表的唯一写入者；只在事务内做 SQL，不生成
//! 随机数/时间戳/hash/seal；业务拒绝用 [`ControlResponse`]（事务照常提交），内部不变量
//! 破坏返回 `Err`（调用方 fail-stop，事务回滚）。`ControlWrite` 是写入口的编译闸：
//! 构造函数只在本模块（以及 `Local` façade 调用本模块时）出现。

use base64::Engine as _;
use sqlx::SqliteConnection;

use super::command::{ControlCommand, ControlResponse, LifecycleRuleCmd, PermissionBits};
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
            if name.is_empty() || bucket.is_empty() {
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
            if bucket.is_empty() || id.is_empty() || config_json.is_empty() {
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
    };
    Ok(Applied { response, effects })
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
}
