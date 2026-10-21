//! force recovery：全新 `raft.db` + 已导出业务表 → 初始化单节点 `{N}` 后退出。
//!
//! witness 与数据站点走同一安装映射；区别仅在安装上下文（witness 无本地管理员，
//! owner 写哨兵 0）与恢复后重启身份（role 由 `[raft].role` 决定）。

use std::path::{Path, PathBuf};

use super::export::{ExportFile, verify_export};
use crate::config::RaftRole;
use crate::db::Db;
use crate::raft::data::{self, InstallCtx};
use crate::raft::store::SqliteLogStore;
use crate::raft::{self, RaftHandle, RaftSettings};

#[derive(Debug, Clone)]
pub struct ForceRecoverReport {
    pub node_id: u64,
    pub voters: Vec<u64>,
    pub raft_db: PathBuf,
}

/// force recovery：仅接受全新 `raft.db` + 已导出业务表；初始化单节点 `{N}` 后退出。
pub async fn force_recover(
    db: &Db,
    raft_db_path: &Path,
    settings: &RaftSettings,
    export: &ExportFile,
    new_node_id: u64,
) -> Result<ForceRecoverReport, String> {
    verify_export(export)?;
    if new_node_id == 0 {
        return Err("new node id must be > 0".into());
    }
    if raft_db_path.exists() {
        let logs = SqliteLogStore::open(raft_db_path)
            .await
            .map_err(|e| format!("open raft log: {e}"))?;
        if logs.has_logs().await.map_err(|e| e.to_string())? {
            return Err(format!(
                "raft log already has entries: {}; refusing to edit an old log",
                raft_db_path.display()
            ));
        }
        drop(logs);
    }
    if db
        .schema_meta("raft_mode")
        .await
        .map_err(|e| e.to_string())?
        .as_deref()
        != Some("on")
    {
        return Err("control DB is not in raft mode; run raft-cutover first".into());
    }
    let applied_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM raft_applied WHERE applied_log_id IS NOT NULL")
            .fetch_one(db.pool())
            .await
            .map_err(|e| e.to_string())?;
    let has_snapshot_table: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'raft_snapshot'",
    )
    .fetch_one(db.pool())
    .await
    .map_err(|e| e.to_string())?;
    let snapshot_count: i64 = if has_snapshot_table > 0 {
        sqlx::query_scalar("SELECT COUNT(*) FROM raft_snapshot")
            .fetch_one(db.pool())
            .await
            .map_err(|e| e.to_string())?
    } else {
        0
    };
    if applied_count != 0 || snapshot_count != 0 {
        return Err("control DB already has applied raft state; refusing to merge".into());
    }
    // witness 没有本地管理员；数据站点要求管理员（AP/inventory 属主指过去）。
    let witness = settings.role == RaftRole::Witness;
    let admin_id: Option<i64> = if witness {
        None
    } else {
        let id: Option<i64> =
            sqlx::query_scalar("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1")
                .fetch_optional(db.pool())
                .await
                .map_err(|e| e.to_string())?;
        if id.is_none() {
            return Err("no local admin user; bootstrap an admin before recovery".into());
        }
        id
    };
    let mut tx = db.pool().begin().await.map_err(|e| e.to_string())?;
    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    data::install_tx(&mut tx, &export.data, &InstallCtx { admin_id, witness })
        .await
        .map_err(|e| e.to_string())?;
    sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;

    let mut settings = settings.clone();
    settings.node_id = new_node_id;
    // 恢复时的临时 bootstrap 只为初始化 {N}；持久化身份仍写 bootstrap=0。
    settings.bootstrap = true;
    settings.db_path = raft_db_path.to_path_buf();
    let handle: RaftHandle = raft::spawn_with_control(settings, Some(db.pool().clone()), None)
        .await
        .map_err(|e| format!("initialize recovered node: {e}"))?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let voters = loop {
        let v = raft::membership::view(&handle);
        if v.voters == vec![new_node_id] {
            break v.voters;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("recovered membership did not converge to {N}".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    handle.shutdown().await;
    db.schema_meta_set("raft_node_id", &new_node_id.to_string())
        .await
        .map_err(|e| e.to_string())?;
    // 身份编码只认 "1"/"0"（raft/identity.rs）；写 "false" 会让下次启动被拒。
    db.schema_meta_set("raft_bootstrap", "0")
        .await
        .map_err(|e| e.to_string())?;
    db.schema_meta_set("raft_initialized", "1")
        .await
        .map_err(|e| e.to_string())?;
    let marker = serde_json::json!({
        "new_node_id": new_node_id,
        "source_site": export.source_site,
        "source_role": export.source_role,
        "exported_at": export.exported_at,
        "recovered_at": crate::db::now(),
    });
    db.schema_meta_set(
        "raft_force_recover",
        &serde_json::to_string(&marker).map_err(|e| e.to_string())?,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(ForceRecoverReport {
        node_id: new_node_id,
        voters,
        raft_db: raft_db_path.to_path_buf(),
    })
}
