//! §3 账户规范化 worker：serve 启动后按 leader 门控补 legacy 用户的账户/成员行。
//!
//! standalone 直接走 [`Db::canonicalize_legacy_accounts_once`]（单事务、marker 幂等）；
//! Raft 模式账户表经快照复制，绝不本地直写：leader 用既有 `AccountCreate` +
//! `AccountMemberAttach` 命令修复缺成员行的用户，不新增 Raft 命令。marker 语义不变：
//! 只有 standalone 的全量规范化事务在成功后写 `accounts_canonical_v1`；Raft 的空表
//! 启动守卫已写 marker，worker 不碰它。
//!
//! shortcut: Raft 修复只能复用既有命令，成员角色记为 admin（owner 不在命令的合法
//! 角色集内；两者在账户管理判定中等价）。升级触发：新增 level-3 owner 角色命令或
//! canonical marker 复制命令时改成 owner。

use std::time::Duration;

use crate::raft::{ControlCommand, ControlResponse, LeaderGate};
use crate::state::AppState;

/// 修复轮间隔：只是兜底对账（正常路径建用户即挂账户），不需要高频。
const INTERVAL: Duration = Duration::from_secs(300);

pub fn spawn_worker(state: AppState, leader: LeaderGate) {
    tokio::spawn(async move {
        loop {
            if leader.is_leader()
                && let Err(e) = run_once(&state).await
            {
                tracing::warn!(error = %e, "账户规范化 worker 单轮失败（下一轮重试）");
            }
            tokio::time::sleep(INTERVAL).await;
        }
    });
}

async fn run_once(state: &AppState) -> Result<usize, String> {
    if !state.control.is_raft() {
        let n = state
            .db
            .canonicalize_legacy_accounts_once()
            .await
            .map_err(|e| e.to_string())?;
        if n > 0 {
            tracing::info!(rows = n, "账户规范化 worker 补齐 legacy 账户行");
        }
        return Ok(n);
    }

    let users = state
        .db
        .legacy_users_missing_accounts()
        .await
        .map_err(|e| e.to_string())?;
    let ts = crate::db::now();
    let mut fixed = 0usize;
    for (uid, username) in users {
        // 交接中的旧 leader 立即停手，命令本身由 client_write 再校验 leader。
        if !state.control.is_leader() {
            break;
        }
        let Some(id) = xl_s3::access::account_id_of(uid) else {
            continue; // uid 0/越界：旧语义下也无法匹配任何账户 ARN
        };
        match state
            .control
            .write(ControlCommand::AccountCreate {
                id: id.clone(),
                name: username.clone(),
                bucket_quota: None,
                created_at: ts,
            })
            .await
        {
            Ok(ControlResponse::Ok | ControlResponse::Conflict) => {}
            Ok(resp) => {
                tracing::warn!(?resp, uid, "账户规范化创建被拒");
                continue;
            }
            Err(e) => return Err(e.to_string()),
        }
        // 创建与挂载之间用户可能已被管理员挂到真实账户；挂载命令按 uid upsert，
        // 重查后跳过，避免把成员从真实账户挪到 legacy 派生账户（仅缩小窗口）。
        let attached: Option<String> =
            sqlx::query_scalar("SELECT account_id FROM account_members WHERE uid = ?")
                .bind(uid)
                .fetch_optional(state.db.pool())
                .await
                .map_err(|e| e.to_string())?;
        if attached.is_some() {
            continue;
        }
        match state
            .control
            .write(ControlCommand::AccountMemberAttach {
                account_id: id,
                username,
                role: "admin".to_string(),
                created_at: ts,
            })
            .await
        {
            Ok(ControlResponse::Ok) => fixed += 1,
            Ok(ControlResponse::Conflict) => {}
            Ok(resp) => tracing::warn!(?resp, uid, "账户规范化挂载被拒"),
            Err(e) => return Err(e.to_string()),
        }
    }
    if fixed > 0 {
        tracing::info!(
            rows = fixed,
            "账户规范化 worker 补齐 legacy 账户行（Raft 命令）"
        );
    }
    Ok(fixed)
}
