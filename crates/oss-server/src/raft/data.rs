//! M9 S7.2：控制面复制子集（11 张表）的自然键数据模型、快照封套与整表安装。
//!
//! 快照/导出/cutover/force-recover 共用 [`ControlData`]：只含复制表（ruling #15）
//! ——管理员用户/AK 是站点本地状态，`buckets.set_id` 是本地物理 set（ruling #5），
//! 两者都不入库。行序由 `read_tx` 的 `ORDER BY` 固定（checksum/diff 确定性）。
//!
//! 快照封套（`encode_snapshot`）：`OSSCTL1\n<sha256(payload) 64 位小写 hex>\n<json>`。
//! 解码先校验 magic / hex / SHA-256，任何失败都 fail-closed（install 在任何写之前拒绝）。

use std::collections::{HashMap, HashSet};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

/// 快照封套格式版本（magic `OSSCTL1` 即 1）。
pub const SNAPSHOT_FORMAT: u32 = 1;

const MAGIC: &[u8] = b"OSSCTL1\n";
/// 快照封套前缀（快照类型判定用：控制面 11 表 vs S5 基座 KV）。
pub const SNAPSHOT_MAGIC: &[u8] = MAGIC;
const HEX_LEN: usize = 64;

/// 属主自然键：`Admin` = 目标站点本地管理员（buckets → NULL；AP/inventory → 本地
/// 管理员 id，witness 写哨兵 0）；`User(username)` = 复制用户。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "username")]
pub enum OwnerRef {
    Admin,
    User(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRow {
    pub username: String,
    pub password_hash: String,
    pub status: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessKeyRow {
    pub access_key: String,
    pub secret_key: String,
    pub username: String,
    pub status: String,
    pub expires_at: Option<i64>,
    pub created_at: i64,
    pub parent_ak: Option<String>,
    pub session_policy: Option<String>,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRow {
    pub username: String,
    pub bucket: String,
    pub can_read: bool,
    pub can_write: bool,
    pub can_delete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRow {
    pub name: String,
    pub owner: OwnerRef,
    pub creation_date: i64,
    pub versioning: String,
    pub object_lock: bool,
    pub default_encryption: Option<String>,
    pub object_lock_default_mode: Option<String>,
    pub object_lock_default_days: Option<i64>,
    pub replication_mode: String,
    pub deleting: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketPolicyRow {
    pub bucket: String,
    pub policy_json: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleRuleRow {
    pub bucket: String,
    pub prefix: String,
    pub rule_id: Option<String>,
    pub expire_days: i64,
    pub expiration_days: Option<i64>,
    pub transition_days: Option<i64>,
    pub transition_class: Option<String>,
    pub transition_tier: Option<String>,
    pub enabled: bool,
    pub updated_at: i64,
    /// 结构化动作；旧快照无此字段 → None（按旧列还原）。
    #[serde(default)]
    pub config_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventConfigRow {
    pub bucket: String,
    pub config_json: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationRuleRow {
    pub bucket: String,
    pub rules_json: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessPointRow {
    pub name: String,
    pub bucket: String,
    pub owner: OwnerRef,
    pub policy_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OlapConfigRow {
    pub access_point: String,
    pub kind: String,
    /// `module BLOB` 的 base64（JSON 只放大 ~1.33×；与 S6a 命令编码一致）。
    pub endpoint: Option<String>,
    pub module_b64: Option<String>,
    pub module_sha256: Option<String>,
    pub timeout_ms: i64,
    pub max_response_bytes: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryConfigRow {
    pub bucket: String,
    pub id: String,
    pub owner: OwnerRef,
    pub config_json: String,
    pub updated_at: i64,
}

/// 11 张复制表的完整载荷（自然键；无本地自增 id / set_id / 管理员行）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ControlData {
    pub users: Vec<UserRow>,
    pub access_keys: Vec<AccessKeyRow>,
    pub permissions: Vec<PermissionRow>,
    pub buckets: Vec<BucketRow>,
    pub bucket_policies: Vec<BucketPolicyRow>,
    pub lifecycle_rules: Vec<LifecycleRuleRow>,
    pub event_configs: Vec<EventConfigRow>,
    pub replication_rules: Vec<ReplicationRuleRow>,
    pub access_points: Vec<AccessPointRow>,
    pub olap_configs: Vec<OlapConfigRow>,
    pub inventory_configs: Vec<InventoryConfigRow>,
}

#[derive(Debug)]
pub enum DataError {
    Format(String),
    Checksum,
    Json(serde_json::Error),
    MissingUser(i64),
    NoAdminUser,
}

impl std::fmt::Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format(m) => write!(f, "control data: {m}"),
            Self::Checksum => write!(f, "snapshot checksum mismatch"),
            Self::Json(e) => write!(f, "snapshot json: {e}"),
            Self::MissingUser(id) => write!(f, "owner user id {id} missing"),
            Self::NoAdminUser => write!(
                f,
                "no local admin user (admin-owned access point/inventory requires one)"
            ),
        }
    }
}

impl std::error::Error for DataError {}

impl From<serde_json::Error> for DataError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

fn sql(e: sqlx::Error) -> DataError {
    DataError::Format(format!("sql: {e}"))
}

fn owner_of(
    owner_id: Option<i64>,
    username: Option<String>,
    is_admin: Option<i64>,
) -> Result<OwnerRef, DataError> {
    match owner_id {
        None => Ok(OwnerRef::Admin),
        Some(0) => Ok(OwnerRef::Admin),
        Some(id) => match (username, is_admin) {
            (Some(_), Some(1)) => Ok(OwnerRef::Admin),
            (Some(u), _) => Ok(OwnerRef::User(u)),
            (None, _) => Err(DataError::MissingUser(id)),
        },
    }
}

/// 单事务读取 11 张复制表（调用方负责事务；行序 = checksum/自然键确定性来源）。
pub(crate) async fn read_tx(conn: &mut SqliteConnection) -> Result<ControlData, DataError> {
    let mut data = ControlData::default();

    for r in sqlx::query(
        "SELECT username, password_hash, status, created_at FROM users \
         WHERE is_admin = 0 ORDER BY username",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        data.users.push(UserRow {
            username: r.get("username"),
            password_hash: r.get("password_hash"),
            status: r.get("status"),
            created_at: r.get("created_at"),
        });
    }

    for r in sqlx::query(
        "SELECT a.access_key, a.secret_key, u.username, a.status, a.expires_at, \
                a.created_at, a.parent_ak, a.session_policy, a.kind \
         FROM access_keys a JOIN users u ON u.id = a.user_id \
         WHERE u.is_admin = 0 AND a.kind = 'long-term' AND a.parent_ak IS NULL \
         ORDER BY a.access_key",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        data.access_keys.push(AccessKeyRow {
            access_key: r.get("access_key"),
            secret_key: r.get("secret_key"),
            username: r.get("username"),
            status: r.get("status"),
            expires_at: r.get("expires_at"),
            created_at: r.get("created_at"),
            parent_ak: r.get("parent_ak"),
            session_policy: r.get("session_policy"),
            kind: r.get("kind"),
        });
    }

    for r in sqlx::query(
        "SELECT u.username, p.bucket, p.can_read, p.can_write, p.can_delete \
         FROM permissions p JOIN users u ON u.id = p.user_id \
         WHERE u.is_admin = 0 ORDER BY u.username, p.bucket",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        data.permissions.push(PermissionRow {
            username: r.get("username"),
            bucket: r.get("bucket"),
            can_read: r.get::<i64, _>("can_read") != 0,
            can_write: r.get::<i64, _>("can_write") != 0,
            can_delete: r.get::<i64, _>("can_delete") != 0,
        });
    }

    for r in sqlx::query(
        "SELECT b.name, b.owner_id, b.creation_date, b.versioning, b.object_lock, \
                b.default_encryption, b.object_lock_default_mode, b.object_lock_default_days, \
                b.replication_mode, b.deleting, u.username AS owner_username, \
                u.is_admin AS owner_is_admin \
         FROM buckets b LEFT JOIN users u ON u.id = b.owner_id ORDER BY b.name",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        data.buckets.push(BucketRow {
            name: r.get("name"),
            owner: owner_of(
                r.get("owner_id"),
                r.get("owner_username"),
                r.get("owner_is_admin"),
            )?,
            creation_date: r.get("creation_date"),
            versioning: r.get("versioning"),
            object_lock: r.get::<i64, _>("object_lock") != 0,
            default_encryption: r.get("default_encryption"),
            object_lock_default_mode: r.get("object_lock_default_mode"),
            object_lock_default_days: r.get("object_lock_default_days"),
            replication_mode: r.get("replication_mode"),
            deleting: r.get::<i64, _>("deleting") != 0,
        });
    }

    for r in
        sqlx::query("SELECT bucket, policy_json, updated_at FROM bucket_policies ORDER BY bucket")
            .fetch_all(&mut *conn)
            .await
            .map_err(sql)?
    {
        data.bucket_policies.push(BucketPolicyRow {
            bucket: r.get("bucket"),
            policy_json: r.get("policy_json"),
            updated_at: r.get("updated_at"),
        });
    }

    for r in sqlx::query(
        "SELECT bucket, prefix, rule_id, expire_days, expiration_days, transition_days, \
                transition_class, transition_tier, enabled, updated_at, config_json \
         FROM lifecycle_rules ORDER BY bucket, rule_id, prefix",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        data.lifecycle_rules.push(LifecycleRuleRow {
            bucket: r.get("bucket"),
            prefix: r.get("prefix"),
            rule_id: r.get("rule_id"),
            expire_days: r.get("expire_days"),
            expiration_days: r.get("expiration_days"),
            transition_days: r.get("transition_days"),
            transition_class: r.get("transition_class"),
            transition_tier: r.get("transition_tier"),
            enabled: r.get::<i64, _>("enabled") != 0,
            updated_at: r.get("updated_at"),
            config_json: r.get("config_json"),
        });
    }

    for r in
        sqlx::query("SELECT bucket, config_json, updated_at FROM event_configs ORDER BY bucket")
            .fetch_all(&mut *conn)
            .await
            .map_err(sql)?
    {
        data.event_configs.push(EventConfigRow {
            bucket: r.get("bucket"),
            config_json: r.get("config_json"),
            updated_at: r.get("updated_at"),
        });
    }

    for r in
        sqlx::query("SELECT bucket, rules_json, updated_at FROM replication_rules ORDER BY bucket")
            .fetch_all(&mut *conn)
            .await
            .map_err(sql)?
    {
        data.replication_rules.push(ReplicationRuleRow {
            bucket: r.get("bucket"),
            rules_json: r.get("rules_json"),
            updated_at: r.get("updated_at"),
        });
    }

    for r in sqlx::query(
        "SELECT a.name, a.bucket, a.owner_id, a.policy_json, a.created_at, a.updated_at, \
                u.username AS owner_username, u.is_admin AS owner_is_admin \
         FROM access_points a LEFT JOIN users u ON u.id = a.owner_id ORDER BY a.name",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        data.access_points.push(AccessPointRow {
            name: r.get("name"),
            bucket: r.get("bucket"),
            owner: owner_of(
                Some(r.get("owner_id")),
                r.get("owner_username"),
                r.get("owner_is_admin"),
            )?,
            policy_json: r.get("policy_json"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        });
    }

    for r in sqlx::query(
        "SELECT access_point, kind, endpoint, module, module_sha256, timeout_ms, \
                max_response_bytes, updated_at FROM olap_configs ORDER BY access_point",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        let module: Option<Vec<u8>> = r.get("module");
        data.olap_configs.push(OlapConfigRow {
            access_point: r.get("access_point"),
            kind: r.get("kind"),
            endpoint: r.get("endpoint"),
            module_b64: module.map(|m| base64::engine::general_purpose::STANDARD.encode(m)),
            module_sha256: r.get("module_sha256"),
            timeout_ms: r.get("timeout_ms"),
            max_response_bytes: r.get("max_response_bytes"),
            updated_at: r.get("updated_at"),
        });
    }

    for r in sqlx::query(
        "SELECT i.bucket, i.id, i.owner_id, i.config_json, i.updated_at, \
                u.username AS owner_username, u.is_admin AS owner_is_admin \
         FROM inventory_configs i LEFT JOIN users u ON u.id = i.owner_id \
         ORDER BY i.bucket, i.id",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(sql)?
    {
        data.inventory_configs.push(InventoryConfigRow {
            bucket: r.get("bucket"),
            id: r.get("id"),
            owner: owner_of(
                Some(r.get("owner_id")),
                r.get("owner_username"),
                r.get("owner_is_admin"),
            )?,
            config_json: r.get("config_json"),
            updated_at: r.get("updated_at"),
        });
    }

    Ok(data)
}

/// 安装上下文：数据站点必须有本地管理员（admin 属主 AP/inventory 指过去）；
/// witness 没有管理员，写哨兵 0（S6a ruling）。
pub struct InstallCtx {
    pub admin_id: Option<i64>,
    pub witness: bool,
}

/// 安装结果（插入行数，日志/诊断用）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InstallReport {
    pub users: usize,
    pub access_keys: usize,
    pub permissions: usize,
    pub buckets: usize,
    pub bucket_policies: usize,
    pub lifecycle_rules: usize,
    pub event_configs: usize,
    pub replication_rules: usize,
    pub access_points: usize,
    pub olap_configs: usize,
    pub inventory_configs: usize,
}

fn report_of(data: &ControlData) -> InstallReport {
    InstallReport {
        users: data.users.len(),
        access_keys: data.access_keys.len(),
        permissions: data.permissions.len(),
        buckets: data.buckets.len(),
        bucket_policies: data.bucket_policies.len(),
        lifecycle_rules: data.lifecycle_rules.len(),
        event_configs: data.event_configs.len(),
        replication_rules: data.replication_rules.len(),
        access_points: data.access_points.len(),
        olap_configs: data.olap_configs.len(),
        inventory_configs: data.inventory_configs.len(),
    }
}

/// 整表替换（调用方在同一事务里置 `raft_apply_ctx.active=1` 后调用）。
///
/// FK 安全顺序：先删引用 users 的从属行/目标独有桶，再把保留桶的 owner 置 NULL，
/// 然后才删 users；插入 users 后按 username 重建 id 映射。桶用 upsert 保留目标已有的
/// `set_id`（ruling #5），新桶由 DEFAULT 0 落库。
pub(crate) async fn install_tx(
    conn: &mut SqliteConnection,
    data: &ControlData,
    ctx: &InstallCtx,
) -> Result<InstallReport, DataError> {
    // 1. AP → olap（olap_configs.access_point 外键）。
    sqlx::query("DELETE FROM olap_configs")
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    sqlx::query("DELETE FROM access_points")
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    // 2. 桶配置行（无外键，先清）。
    for q in [
        "DELETE FROM inventory_configs",
        "DELETE FROM event_configs",
        "DELETE FROM replication_rules",
        "DELETE FROM lifecycle_rules",
        "DELETE FROM bucket_policies",
    ] {
        sqlx::query(q).execute(&mut *conn).await.map_err(sql)?;
    }
    // 3. 权限（引用 users）。
    sqlx::query("DELETE FROM permissions")
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    // 4. 目标独有桶（引用 users，须先于删 users）；保留桶 owner 先置 NULL。
    let existing: Vec<String> = sqlx::query_scalar("SELECT name FROM buckets")
        .fetch_all(&mut *conn)
        .await
        .map_err(sql)?;
    let wanted: HashSet<&str> = data.buckets.iter().map(|b| b.name.as_str()).collect();
    for name in existing.iter().filter(|n| !wanted.contains(n.as_str())) {
        sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(name)
            .execute(&mut *conn)
            .await
            .map_err(sql)?;
    }
    sqlx::query("UPDATE buckets SET owner_id = NULL")
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    // 5. 非管理员属主 AK 全清（long-term + session/STS；管理员本地 AK 保留）。
    sqlx::query(
        "DELETE FROM access_keys \
         WHERE user_id NOT IN (SELECT id FROM users WHERE is_admin = 1)",
    )
    .execute(&mut *conn)
    .await
    .map_err(sql)?;
    // 6. 复制用户（管理员行不动）。`uploads.owner_id` 有 FK 指向 users（本地表），
    // 与 `delete_user` 同口径先置 NULL，否则有历史上传行的库安装会 FK 失败。
    sqlx::query(
        "UPDATE uploads SET owner_id = NULL \
         WHERE owner_id IN (SELECT id FROM users WHERE is_admin = 0)",
    )
    .execute(&mut *conn)
    .await
    .map_err(sql)?;
    sqlx::query("DELETE FROM users WHERE is_admin = 0")
        .execute(&mut *conn)
        .await
        .map_err(sql)?;

    // 7. 插入用户并按 username 重建映射。
    let mut ids: HashMap<&str, i64> = HashMap::with_capacity(data.users.len());
    for u in &data.users {
        let done = sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
             VALUES (?, ?, 0, ?, ?)",
        )
        .bind(&u.username)
        .bind(&u.password_hash)
        .bind(&u.status)
        .bind(u.created_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
        ids.insert(u.username.as_str(), done.last_insert_rowid());
    }
    let user_id = |name: &str, what: &str| -> Result<i64, DataError> {
        ids.get(name)
            .copied()
            .ok_or_else(|| DataError::Format(format!("{what} owner {name} not in snapshot")))
    };

    // 8. AK（快照只含 long-term parent_ak=NULL；按 username 映射本地 id）。
    for k in &data.access_keys {
        let user = user_id(&k.username, "access key")?;
        sqlx::query(
            "INSERT INTO access_keys \
             (access_key, secret_key, user_id, status, expires_at, created_at, \
              parent_ak, session_policy, kind) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&k.access_key)
        .bind(&k.secret_key)
        .bind(user)
        .bind(&k.status)
        .bind(k.expires_at)
        .bind(k.created_at)
        .bind(&k.parent_ak)
        .bind(&k.session_policy)
        .bind(&k.kind)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    // 9. 权限（三档位原样）；目标上管理员目标的残留行清掉。
    for p in &data.permissions {
        let user = user_id(&p.username, "permission")?;
        sqlx::query(
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(user)
        .bind(&p.bucket)
        .bind(p.can_read)
        .bind(p.can_write)
        .bind(p.can_delete)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }
    sqlx::query(
        "DELETE FROM permissions WHERE user_id IN (SELECT id FROM users WHERE is_admin = 1)",
    )
    .execute(&mut *conn)
    .await
    .map_err(sql)?;

    // 10. 桶：保留目标 set_id（不在 UPDATE 列里），新桶 DEFAULT 0。
    for b in &data.buckets {
        let owner_id = match &b.owner {
            OwnerRef::Admin => None,
            OwnerRef::User(name) => Some(user_id(name, "bucket")?),
        };
        sqlx::query(
            "INSERT INTO buckets \
             (name, owner_id, creation_date, versioning, object_lock, default_encryption, \
              object_lock_default_mode, object_lock_default_days, replication_mode, deleting) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(name) DO UPDATE SET \
              owner_id = excluded.owner_id, creation_date = excluded.creation_date, \
              versioning = excluded.versioning, object_lock = excluded.object_lock, \
              default_encryption = excluded.default_encryption, \
              object_lock_default_mode = excluded.object_lock_default_mode, \
              object_lock_default_days = excluded.object_lock_default_days, \
              replication_mode = excluded.replication_mode, deleting = excluded.deleting",
        )
        .bind(&b.name)
        .bind(owner_id)
        .bind(b.creation_date)
        .bind(&b.versioning)
        .bind(b.object_lock)
        .bind(&b.default_encryption)
        .bind(&b.object_lock_default_mode)
        .bind(b.object_lock_default_days)
        .bind(&b.replication_mode)
        .bind(b.deleting)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    // 11. AP / inventory：Admin 属主 = 本地管理员 id（witness 0）。
    for ap in &data.access_points {
        let owner_id = match &ap.owner {
            OwnerRef::Admin => match ctx.admin_id {
                Some(id) => id,
                None if ctx.witness => 0,
                None => return Err(DataError::NoAdminUser),
            },
            OwnerRef::User(name) => user_id(name, "access point")?,
        };
        sqlx::query(
            "INSERT INTO access_points \
             (name, bucket, owner_id, policy_json, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&ap.name)
        .bind(&ap.bucket)
        .bind(owner_id)
        .bind(&ap.policy_json)
        .bind(ap.created_at)
        .bind(ap.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }
    for inv in &data.inventory_configs {
        let owner_id = match &inv.owner {
            OwnerRef::Admin => match ctx.admin_id {
                Some(id) => id,
                None if ctx.witness => 0,
                None => return Err(DataError::NoAdminUser),
            },
            OwnerRef::User(name) => user_id(name, "inventory")?,
        };
        sqlx::query(
            "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&inv.bucket)
        .bind(&inv.id)
        .bind(owner_id)
        .bind(&inv.config_json)
        .bind(inv.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    // 12. 桶级配置（桶已就位）。
    for p in &data.bucket_policies {
        sqlx::query(
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, ?)",
        )
        .bind(&p.bucket)
        .bind(&p.policy_json)
        .bind(p.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }
    for l in &data.lifecycle_rules {
        sqlx::query(
            "INSERT INTO lifecycle_rules \
             (bucket, prefix, expire_days, enabled, updated_at, rule_id, expiration_days, \
              transition_days, transition_class, transition_tier, config_json) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&l.bucket)
        .bind(&l.prefix)
        .bind(l.expire_days)
        .bind(l.enabled)
        .bind(l.updated_at)
        .bind(&l.rule_id)
        .bind(l.expiration_days)
        .bind(l.transition_days)
        .bind(&l.transition_class)
        .bind(&l.transition_tier)
        .bind(&l.config_json)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }
    for e in &data.event_configs {
        sqlx::query("INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, ?, ?)")
            .bind(&e.bucket)
            .bind(&e.config_json)
            .bind(e.updated_at)
            .execute(&mut *conn)
            .await
            .map_err(sql)?;
    }
    for r in &data.replication_rules {
        sqlx::query(
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, ?, ?)",
        )
        .bind(&r.bucket)
        .bind(&r.rules_json)
        .bind(r.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }
    for o in &data.olap_configs {
        let module = match &o.module_b64 {
            None => None,
            Some(b64) => Some(
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| DataError::Format(format!("olap module base64: {e}")))?,
            ),
        };
        sqlx::query(
            "INSERT INTO olap_configs \
             (access_point, kind, endpoint, module, module_sha256, timeout_ms, \
              max_response_bytes, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&o.access_point)
        .bind(&o.kind)
        .bind(&o.endpoint)
        .bind(module)
        .bind(&o.module_sha256)
        .bind(o.timeout_ms)
        .bind(o.max_response_bytes)
        .bind(o.updated_at)
        .execute(&mut *conn)
        .await
        .map_err(sql)?;
    }

    // 13. 孤儿 session/STS AK（属主已被替换而残留）。
    sqlx::query(
        "DELETE FROM access_keys \
         WHERE (kind = 'session' OR parent_ak IS NOT NULL) \
           AND user_id NOT IN (SELECT id FROM users)",
    )
    .execute(&mut *conn)
    .await
    .map_err(sql)?;

    Ok(report_of(data))
}

/// 快照封套编码：`OSSCTL1\n<sha256 hex>\n<json>`。
pub fn encode_snapshot(data: &ControlData) -> Result<Vec<u8>, DataError> {
    let payload = serde_json::to_vec(data)?;
    let sum = Sha256::digest(&payload);
    let mut out = Vec::with_capacity(MAGIC.len() + HEX_LEN + 1 + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(hex::encode(sum).as_bytes());
    out.push(b'\n');
    out.extend_from_slice(&payload);
    Ok(out)
}

/// 解码并校验快照封套；magic/hex/checksum 任一不符 → fail-closed（不写任何行）。
pub fn decode_snapshot(bytes: &[u8]) -> Result<ControlData, DataError> {
    let rest = bytes
        .strip_prefix(MAGIC)
        .ok_or_else(|| DataError::Format("snapshot magic missing".into()))?;
    let nl = rest
        .iter()
        .position(|b| *b == b'\n')
        .ok_or_else(|| DataError::Format("snapshot checksum header truncated".into()))?;
    let header = &rest[..nl];
    if header.len() != HEX_LEN
        || !header
            .iter()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(DataError::Format(
            "snapshot checksum must be 64 lowercase hex chars".into(),
        ));
    }
    let payload = &rest[nl + 1..];
    let want = hex::decode(header).map_err(|e| DataError::Format(format!("checksum hex: {e}")))?;
    if Sha256::digest(payload).as_slice() != want.as_slice() {
        return Err(DataError::Checksum);
    }
    Ok(serde_json::from_slice(payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ControlData {
        ControlData {
            users: vec![
                UserRow {
                    username: "alice".into(),
                    password_hash: "$argon2id$v=19$m=1,t=1,p=1$x$y".into(),
                    status: "active".into(),
                    created_at: 10,
                },
                UserRow {
                    username: "bob".into(),
                    password_hash: "h".into(),
                    status: "disabled".into(),
                    created_at: 11,
                },
            ],
            access_keys: vec![AccessKeyRow {
                access_key: "AKALICE".into(),
                secret_key: "secret".into(),
                username: "alice".into(),
                status: "active".into(),
                expires_at: Some(99),
                created_at: 12,
                parent_ak: None,
                session_policy: None,
                kind: "long-term".into(),
            }],
            permissions: vec![PermissionRow {
                username: "alice".into(),
                bucket: "b1".into(),
                can_read: true,
                can_write: true,
                can_delete: false,
            }],
            buckets: vec![
                BucketRow {
                    name: "b1".into(),
                    owner: OwnerRef::User("alice".into()),
                    creation_date: 13,
                    versioning: "Enabled".into(),
                    object_lock: true,
                    default_encryption: Some("AES256".into()),
                    object_lock_default_mode: Some("GOVERNANCE".into()),
                    object_lock_default_days: Some(7),
                    replication_mode: "sync".into(),
                    deleting: false,
                },
                BucketRow {
                    name: "b2".into(),
                    owner: OwnerRef::Admin,
                    creation_date: 14,
                    versioning: "Off".into(),
                    object_lock: false,
                    default_encryption: None,
                    object_lock_default_mode: None,
                    object_lock_default_days: None,
                    replication_mode: "async".into(),
                    deleting: false,
                },
            ],
            bucket_policies: vec![BucketPolicyRow {
                bucket: "b1".into(),
                policy_json: "{}".into(),
                updated_at: 15,
            }],
            lifecycle_rules: vec![LifecycleRuleRow {
                bucket: "b1".into(),
                prefix: "logs/".into(),
                rule_id: Some("r1".into()),
                expire_days: 30,
                expiration_days: Some(30),
                transition_days: Some(7),
                transition_class: Some("GLACIER".into()),
                transition_tier: Some("cold".into()),
                enabled: true,
                updated_at: 16,
                config_json: None,
            }],
            event_configs: vec![EventConfigRow {
                bucket: "b1".into(),
                config_json: "{\"q\":[]}".into(),
                updated_at: 17,
            }],
            replication_rules: vec![ReplicationRuleRow {
                bucket: "b1".into(),
                rules_json: "[]".into(),
                updated_at: 18,
            }],
            access_points: vec![AccessPointRow {
                name: "ap1".into(),
                bucket: "b1".into(),
                owner: OwnerRef::User("alice".into()),
                policy_json: Some("{}".into()),
                created_at: 19,
                updated_at: 20,
            }],
            olap_configs: vec![OlapConfigRow {
                access_point: "ap1".into(),
                kind: "http".into(),
                endpoint: Some("https://example.com/t".into()),
                module_b64: Some("aGVsbG8=".into()),
                module_sha256: Some("abc".into()),
                timeout_ms: 5000,
                max_response_bytes: 1024,
                updated_at: 21,
            }],
            inventory_configs: vec![InventoryConfigRow {
                bucket: "b1".into(),
                id: "inv1".into(),
                owner: OwnerRef::Admin,
                config_json: "{}".into(),
                updated_at: 22,
            }],
        }
    }

    /// install_tx → read_tx：11 张表经真实 SQL 往返后自然键一致，admin/session 行不入快照。
    #[tokio::test]
    async fn install_and_read_round_trip_via_sql() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("control.db").display()
        );
        let db = crate::db::Db::connect_raft(&url).await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
            .execute(&mut *tx)
            .await
            .unwrap();
        let admin_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
             VALUES ('admin', 'h', 1, 'active', 1) RETURNING id",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        let sample = sample();
        let ctx = InstallCtx {
            admin_id: Some(admin_id),
            witness: false,
        };
        install_tx(&mut tx, &sample, &ctx).await.unwrap();
        // 本地历史上传行（FK 指向非 admin 用户）不得让安装失败；owner 置 NULL。
        let alice: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username = 'alice'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, owner_id, created_at) \
             VALUES ('g/k', 'b1', 'k', 'g', 1, 'e', ?, 1)",
        )
        .bind(alice)
        .execute(&mut *tx)
        .await
        .unwrap();
        // 重复安装必须幂等（全量替换而非累积）。
        install_tx(&mut tx, &sample, &ctx).await.unwrap();
        let upload_owner: Option<i64> =
            sqlx::query_scalar("SELECT owner_id FROM uploads WHERE saved_path = 'g/k'")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(upload_owner, None, "非 admin 属主的 uploads 行必须置 NULL");
        let read = read_tx(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(read, sample, "11 表自然键往返必须一致");
        let admins: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE is_admin = 1")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(admins, 1, "本地 admin 不得被快照安装删除");
    }

    /// 11 张表全行往返：语义相等且再次编码逐字节相同（checksum/ORDER BY 确定性）。
    #[test]
    fn snapshot_round_trip_is_byte_identical() {
        let data = sample();
        let bytes = encode_snapshot(&data).unwrap();
        assert!(bytes.starts_with(b"OSSCTL1\n"));
        let decoded = decode_snapshot(&bytes).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(encode_snapshot(&decoded).unwrap(), bytes);
    }

    /// 1 字节/头部篡改/截断都拒绝；错误分类可区分 checksum 与格式。
    #[test]
    fn corrupted_snapshot_is_rejected() {
        let bytes = encode_snapshot(&sample()).unwrap();

        let mut bit = bytes.clone();
        let last = bit.len() - 1;
        bit[last] ^= 0x01;
        assert!(matches!(
            decode_snapshot(&bit),
            Err(DataError::Checksum) | Err(DataError::Json(_))
        ));

        let mut magic = bytes.clone();
        magic[0] = b'X';
        assert!(matches!(decode_snapshot(&magic), Err(DataError::Format(_))));

        let mut upper = bytes.clone();
        upper[MAGIC.len()] = b'A';
        assert!(matches!(decode_snapshot(&upper), Err(DataError::Format(_))));

        assert!(matches!(
            decode_snapshot(&bytes[..bytes.len() - 5]),
            Err(DataError::Checksum)
        ));
        assert!(matches!(
            decode_snapshot(b"OSSCTL1\n"),
            Err(DataError::Format(_))
        ));
    }
}
