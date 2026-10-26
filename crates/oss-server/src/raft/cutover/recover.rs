//! force recovery：全新 `raft.db` + 已导出业务表 → 初始化单节点 `{N}` 后退出。
//!
//! witness 与数据站点走同一安装映射；区别仅在安装上下文（witness 无本地管理员，
//! owner 写哨兵 0）与恢复后重启身份（role 由 `[raft].role` 决定）。

use std::path::{Path, PathBuf};

use super::export::{ExportFile, install_activation_level, verify_export};
use crate::config::RaftRole;
use crate::db::Db;
use crate::raft::data::{self, InstallCtx};
use crate::raft::runstate;
use crate::raft::store::SqliteLogStore;
use crate::raft::{self, RaftHandle, RaftSettings, compat};

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
    // 恢复后节点带来源激活 marker（激活过但无 level-2 数据也保持 2）；低于该级别的
    // 配置必然被 R8 启动守卫拒绝。在写任何行之前失败，避免留下“数据已装、节点起不来”
    // 的半恢复库。
    let activation = install_activation_level(export);
    // 空账户表上的 canonical marker 只是 Raft 启动规则（fresh DB 必写），无行可丢；
    // 只有真实账户行才阻止 level<3 恢复。
    let canonical_rows: i64 = if activation < compat::FEATURE_LEVEL_3
        && db.accounts_canonical().await.map_err(|e| e.to_string())?
    {
        sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM accounts) + (SELECT COUNT(*) FROM account_members)",
        )
        .fetch_one(db.pool())
        .await
        .map_err(|e| e.to_string())?
    } else {
        0
    };
    if canonical_rows != 0 {
        return Err(format!(
            "target has {canonical_rows} canonical account rows; a level<3 export cannot carry them"
        ));
    }
    if activation > compat::advertised_feature(settings.feature_level) {
        return Err(format!(
            "export requires activation level {activation} (source activation or level-{} \
             data); set [raft].feature_level = {activation} (current {}) before force recovery",
            data::required_feature_level(&export.data),
            settings.feature_level
        ));
    }
    let runstate = export.runstate.clone().unwrap_or_default();
    let snapshot_len = if activation >= compat::FEATURE_LEVEL_3 {
        runstate::encode_runstate_snapshot(&export.data, &runstate, activation)
    } else {
        data::encode_snapshot_at(&export.data, activation)
    }
    .map_err(|e| e.to_string())?
    .len() as u64;
    if snapshot_len > settings.snapshot_max_bytes {
        return Err(format!(
            "recovery snapshot {snapshot_len} bytes exceeds raft.snapshot_max_bytes {}",
            settings.snapshot_max_bytes
        ));
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
    let ctx = InstallCtx {
        admin_id,
        witness,
        canonical: activation >= compat::FEATURE_LEVEL_3,
    };
    data::install_tx(&mut tx, &export.data, &ctx)
        .await
        .map_err(|e| e.to_string())?;
    if activation >= compat::FEATURE_LEVEL_3 {
        runstate::install_runstate_tx(&mut tx, &runstate)
            .await
            .map_err(|e| e.to_string())?;
    }
    if activation > compat::MIN_FEATURE_LEVEL {
        compat::bump_activation_tx(&mut tx, activation)
            .await
            .map_err(|e| e.to_string())?;
    }
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
    let (voters, applied) = loop {
        let (voters_now, applied_now, membership_log) = {
            let m = handle.raft().metrics();
            let m = m.borrow();
            (
                m.membership_config.voter_ids().collect::<Vec<u64>>(),
                m.last_applied,
                *m.membership_config.log_id(),
            )
        };
        // membership 在 append 后即出现在 metrics，但 apply 要等状态机落库；
        // 必须等到 applied >= membership log，否则快照会缺刚初始化的成员/基线。
        let applied_ok = match membership_log {
            Some(l) => applied_now.is_some_and(|a| a.index >= l.index),
            None => applied_now.is_some(),
        };
        if voters_now == vec![new_node_id] && applied_ok {
            break (voters_now, applied_now);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "recovered membership/applied state did not converge to {new_node_id}"
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    let Some(applied) = applied else {
        return Err("recovered state has no applied log".into());
    };
    // 直接安装的业务行不在 Raft 日志里；先把当前状态固化为快照并清掉日志，后续
    // learner 才会从快照取得基线。否则它只回放 initialize 之后的命令，缺恢复行。
    let outcome = async {
        handle
            .raft()
            .trigger()
            .snapshot()
            .await
            .map_err(|e| format!("build recovery snapshot: {e}"))?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            let done = {
                let m = handle.raft().metrics();
                let m = m.borrow();
                m.snapshot
                    .as_ref()
                    .is_some_and(|s| s.index >= applied.index)
            };
            if done {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("recovery snapshot did not complete".to_string());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        handle
            .raft()
            .trigger()
            .purge_log(applied.index)
            .await
            .map_err(|e| format!("purge recovered log: {e}"))?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            let done = {
                let m = handle.raft().metrics();
                let m = m.borrow();
                m.purged.as_ref().is_some_and(|p| p.index >= applied.index)
            };
            if done {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("recovery log purge did not complete".to_string());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok::<(), String>(())
    }
    .await;
    handle.shutdown().await;
    outcome.map_err(|e| format!("{e}; restore the control DB and remove raft.db before retry"))?;
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
