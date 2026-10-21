//! S7.1：Raft 节点身份持久化（`schema_meta`）。
//!
//! 首启写种子，后续启动逐项比对；任何一项变化都拒绝启动（节点 id/角色/站点身份不可漂移）。
//! 集群令牌**不落身份**（ruling 7：泄漏面，配对另走 site token）。

use crate::config::RaftRole;
use crate::db::Db;

use super::RaftSettings;
use super::store::SqliteLogStore;

pub const META_NODE_ID: &str = "raft_node_id";
pub const META_ROLE: &str = "raft_role";
pub const META_BOOTSTRAP: &str = "raft_bootstrap";
pub const META_ADVERTISE: &str = "raft_advertise_endpoint";
pub const META_SITE_ID: &str = "raft_site_id";
pub const META_INITIALIZED: &str = "raft_initialized";

async fn seed_or_check(db: &Db, key: &str, value: &str) -> Result<(), String> {
    match db.schema_meta(key).await.map_err(|e| e.to_string())? {
        Some(old) if old != value => Err(format!(
            "Raft 身份 {key} 已持久化为 {old:?}，配置为 {value:?}：节点身份不可变更\
             （丢失/迁移见 raft-force-recover runbook）"
        )),
        Some(_) => Ok(()),
        None => {
            db.schema_meta_set_if_absent(key, value)
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        }
    }
}

/// spawn 前：种子/校验 node_id、role、bootstrap、advertise、site_id，并检测已初始化库的
/// 状态丢失（`raft_initialized=1` + raft.db 无日志 + 记忆成员为空）。
pub async fn verify_and_seed(db: &Db, settings: &RaftSettings) -> Result<(), String> {
    seed_or_check(db, META_NODE_ID, &settings.node_id.to_string()).await?;
    seed_or_check(
        db,
        META_ROLE,
        match settings.role {
            RaftRole::Voter => "voter",
            RaftRole::Witness => "witness",
        },
    )
    .await?;
    seed_or_check(
        db,
        META_BOOTSTRAP,
        if settings.bootstrap { "1" } else { "0" },
    )
    .await?;
    seed_or_check(
        db,
        META_ADVERTISE,
        settings.advertise_endpoint.trim_end_matches('/'),
    )
    .await?;
    if settings.role == RaftRole::Voter {
        // witness 无 site_config；voter 的站点 id 由 [site].id / 首启 uuid 决定。
        let site = db.site_identity().await.map_err(|e| e.to_string())?;
        seed_or_check(db, META_SITE_ID, &site.id).await?;
    }
    if db
        .schema_meta(META_INITIALIZED)
        .await
        .map_err(|e| e.to_string())?
        .as_deref()
        == Some("1")
    {
        let logs = SqliteLogStore::open(&settings.db_path)
            .await
            .map_err(|e| format!("打开 raft.db 失败（{}）: {e}", settings.db_path.display()))?;
        let has_logs = logs.has_logs().await.map_err(|e| e.to_string())?;
        if !has_logs && membership_empty(db).await? {
            return Err(format!(
                "raft_initialized=1 但 {} 无日志且成员表为空：控制面状态已丢失；\
                 使用 raft-force-recover 重建单节点集群（见 runbook）",
                settings.db_path.display()
            ));
        }
    }
    Ok(())
}

/// `raft_applied.membership_json` 是否无可解析的 voter（无行/空成员均视为空）。
/// 解析失败按非空处理（保守：不误报状态丢失）。
async fn membership_empty(db: &Db) -> Result<bool, String> {
    let raw: Option<String> =
        sqlx::query_scalar("SELECT membership_json FROM raft_applied WHERE id = 1")
            .fetch_optional(db.pool())
            .await
            .map_err(|e| e.to_string())?;
    let Some(raw) = raw else {
        return Ok(true);
    };
    match serde_json::from_str::<openraft::StoredMembership<u64, openraft::BasicNode>>(&raw) {
        Ok(m) => Ok(m.voter_ids().next().is_none()),
        Err(_) => Ok(false),
    }
}

/// spawn 后：同步 `raft_initialized`（缺省写入 live 值；0→1 只在真实成员出现后提升）。
pub async fn record_initialized(db: &Db, initialized: bool) -> Result<(), String> {
    let value = if initialized { "1" } else { "0" };
    match db
        .schema_meta(META_INITIALIZED)
        .await
        .map_err(|e| e.to_string())?
        .as_deref()
    {
        None => {
            db.schema_meta_set_if_absent(META_INITIALIZED, value)
                .await
                .map_err(|e| e.to_string())?;
        }
        Some("0") if initialized => {
            db.upsert_schema_meta(META_INITIALIZED, "1")
                .await
                .map_err(|e| e.to_string())?;
        }
        Some(_) => {}
    }
    Ok(())
}

/// 首个管理员名（pairing/identity 用；`state::load` 引导管理员时写入）。
pub async fn admin_username(db: &Db) -> Result<Option<String>, sqlx::Error> {
    db.schema_meta("admin_username").await
}
