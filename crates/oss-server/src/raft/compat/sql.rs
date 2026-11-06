//! SQLite（`schema_meta`）侧的激活守卫：读取/写入本地激活标记，并在启动时
//! 拒绝「未激活 marker + 非日志运行态」的本地行。纯 wire/feature 常量与对象根
//! 等价守卫分别在 [`super`] 和 [`super::object`]；本模块不参与对象后端。

use xl_store::control_store::jobs::JobStore;

use super::{
    FEATURE_LEVEL_4, FEATURE_LEVEL_5, META_FEATURE_LEVEL, META_INVENTORY_ACTIVATED,
    META_RUNSTATE_ACTIVATED, MIN_FEATURE_LEVEL, advertised_feature,
};

/// 读取本地激活标记（缺省 = 1）；值非法 = 错误（fail-closed，绝不当 1 放行）。
pub async fn read_activation(pool: &sqlx::SqlitePool) -> Result<u8, String> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| format!("获取连接读取 schema_meta.{META_FEATURE_LEVEL} 失败: {e}"))?;
    read_activation_conn(&mut conn).await
}

/// 同 [`read_activation`]，但复用调用方事务/连接（快照 build 与数据同一读快照）。
pub async fn read_activation_conn(conn: &mut sqlx::SqliteConnection) -> Result<u8, String> {
    let value: Option<String> = sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = ?")
        .bind(META_FEATURE_LEVEL)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| format!("读取 schema_meta.{META_FEATURE_LEVEL} 失败: {e}"))?;
    match value {
        None => Ok(MIN_FEATURE_LEVEL),
        Some(v) => v.trim().parse::<u8>().map_err(|_| {
            format!("schema_meta.{META_FEATURE_LEVEL}={v:?} 非法（不是 0..=255 整数）")
        }),
    }
}
/// 运行态激活标记：只由 level-4 快照安装、cutover/force_recover 显式种子或已 apply 的
/// level-4 命令在当前事务内写入。level-2/3 的本地行与 level-3 快照拷贝不是日志事实。
pub async fn mark_runstate_activated_tx(
    conn: &mut sqlx::SqliteConnection,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO schema_meta (key, value) VALUES (?, '1') \
         ON CONFLICT(key) DO UPDATE SET value = '1'",
    )
    .bind(META_RUNSTATE_ACTIVATED)
    .execute(conn)
    .await?;
    Ok(())
}

/// 普通 level-3 快照会整表替换运行态；它携带的行不是日志事实，必须撤销旧标记。
pub async fn clear_runstate_activated_tx(
    conn: &mut sqlx::SqliteConnection,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM schema_meta WHERE key = ?")
        .bind(META_RUNSTATE_ACTIVATED)
        .execute(conn)
        .await?;
    Ok(())
}

/// inventory 运行态激活标记：只由 level-5 快照安装、cutover/force_recover 显式种子或
/// 已 apply 的 level-5 命令写入。与 `META_RUNSTATE_ACTIVATED` 完全独立。
pub async fn mark_inventory_activated_tx(
    conn: &mut sqlx::SqliteConnection,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO schema_meta (key, value) VALUES (?, '1') \
         ON CONFLICT(key) DO UPDATE SET value = '1'",
    )
    .bind(META_INVENTORY_ACTIVATED)
    .execute(conn)
    .await?;
    Ok(())
}

/// 低于 level-5 的快照/激活级别会整表替换 inventory 运行态；撤销标记。
pub async fn clear_inventory_activated_tx(
    conn: &mut sqlx::SqliteConnection,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM schema_meta WHERE key = ?")
        .bind(META_INVENTORY_ACTIVATED)
        .execute(conn)
        .await?;
    Ok(())
}

/// P1-A 激活边界：feature 4（M8-2 运行态复制）要求本地运行态为空或已有 level-4 事实。
///
/// level 2/3 的 jobs/job_tasks 是各节点本地行，从未进入日志；level-3 快照只是构建者
/// 本地表在某一时刻的拷贝，同样不是日志事实。直接以 feature 4 启动会让首次 JobBatch
/// 用本副本历史行判身份/版本、用本机 MAX(id) 分配 id，可能静默丢作业、撞 id，或让
/// `RunState::validate` 的悬空引用检查卡住快照安装。因此 marker 缺失时必须四表为空
/// 且无悬空引用才放行；空表启动**不写** marker（effective 可能仍是 3，需等 level-4
/// 命令/快照或显式 cutover 种子）。feature ≤3 不启用复制，不做检查（保持本地行为）。
pub async fn guard_runstate_activation(
    pool: &sqlx::SqlitePool,
    configured: u8,
) -> Result<(), String> {
    if advertised_feature(configured) < FEATURE_LEVEL_4 {
        return Ok(());
    }
    let activated: Option<String> =
        sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = ?")
            .bind(META_RUNSTATE_ACTIVATED)
            .fetch_optional(pool)
            .await
            .map_err(|e| format!("读取 schema_meta.{META_RUNSTATE_ACTIVATED} 失败: {e}"))?;
    match activated.as_deref() {
        Some("1") => return Ok(()),
        None => {}
        Some(other) => {
            return Err(format!(
                "schema_meta.{META_RUNSTATE_ACTIVATED}={other:?} 非法（只认 \"1\"）"
            ));
        }
    }
    let mut counts = [0i64; 6];
    for (i, table) in [
        "jobs",
        "job_tasks",
        "inventory_runs",
        "inventory_cursors",
        "inventory_incremental",
        "tier_restores",
    ]
    .into_iter()
    .enumerate()
    {
        counts[i] =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(pool)
                .await
                .map_err(|e| format!("统计 {table} 失败: {e}"))?;
    }
    let [jobs, tasks, runs, cursors, incrementals, restores] = counts;
    let inv_dangling: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_runs WHERE job_id IS NOT NULL \
         AND job_id NOT IN (SELECT id FROM jobs)",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("统计 inventory_runs 悬空引用失败: {e}"))?;
    let tier_dangling: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tier_restores WHERE job_id IS NOT NULL \
         AND job_id NOT IN (SELECT id FROM jobs)",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("统计 tier_restores 悬空引用失败: {e}"))?;
    if jobs == 0
        && tasks == 0
        && runs == 0
        && cursors == 0
        && incrementals == 0
        && restores == 0
        && inv_dangling == 0
        && tier_dangling == 0
    {
        return Ok(());
    }
    Err(format!(
        "[raft].feature_level={configured} 要求 feature 4 运行态已激活，但未见 \
         schema_meta.{META_RUNSTATE_ACTIVATED} 且本地运行态含非日志行：jobs={jobs}、\
         job_tasks={tasks}、inventory_runs={runs}、inventory_cursors={cursors}、\
         inventory_incremental={incrementals}、tier_restores={restores}；\
         悬空引用：inventory_runs.job_id={inv_dangling}、tier_restores.job_id={tier_dangling}。\
         level-2/3 的本地行与 level-3 快照拷贝都不是复制日志事实，直接激活会静默丢作业、\
         产生 ID 冲突，或让快照安装因悬空引用失败。运维步骤：保持 feature_level<4 启动，\
         等待所有作业进入终态并完成/导出；备份控制库后清空 jobs、job_tasks、inventory_runs、\
         inventory_cursors、inventory_incremental、tier_restores（悬空引用随行删除；若要保留\
         inventory/tier 行，只能改用导出重灌，本守卫要求六表为空且无悬空引用），确认后以 \
         feature_level>=4 重启；或用 raft-export 备份后在新集群 raft-cutover/force-recover \
         重灌（显式种子会写激活标记）。本二进制不迁移旧本地运行态。"
    ))
}

/// P1-B 激活边界：feature 5（inventory 运行态复制）要求本地三张 inventory 表为空，
/// 且 `runstate_meta.next_run_id`/自然键与剩余行一致；通过后清空本地 `lifecycle_cursors`
/// （v1.23.0 的续扫提示，不是日志事实，重扫安全）。
///
/// v1.23.0 在 feature 4 下仍把 inventory 写在各节点本地，而 Job* 已把 jobs 的
/// level-4 标记置位；因此本守卫使用**独立**的 `META_INVENTORY_ACTIVATED`，绝不能
/// 复用或短路 `META_RUNSTATE_ACTIVATED`。marker 缺失时若有本地行，直接复制会静默
/// 去重、撞 id/自然键或按副本分叉；必须清空或导出重灌。空表启动不写 marker（effective
/// 可能仍是 4，需等 level-5 命令/快照或显式 cutover 种子）。
pub async fn guard_inventory_activation(
    pool: &sqlx::SqlitePool,
    configured: u8,
) -> Result<(), String> {
    if advertised_feature(configured) < FEATURE_LEVEL_5 {
        return Ok(());
    }
    let activated: Option<String> =
        sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = ?")
            .bind(META_INVENTORY_ACTIVATED)
            .fetch_optional(pool)
            .await
            .map_err(|e| format!("读取 schema_meta.{META_INVENTORY_ACTIVATED} 失败: {e}"))?;
    match activated.as_deref() {
        Some("1") => return Ok(()),
        None => {}
        Some(other) => {
            return Err(format!(
                "schema_meta.{META_INVENTORY_ACTIVATED}={other:?} 非法（只认 \"1\"）"
            ));
        }
    }
    let mut counts = [0i64; 3];
    for (i, table) in [
        "inventory_runs",
        "inventory_cursors",
        "inventory_incremental",
    ]
    .into_iter()
    .enumerate()
    {
        counts[i] =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(pool)
                .await
                .map_err(|e| format!("统计 {table} 失败: {e}"))?;
    }
    let [runs, cursors, incrementals] = counts;
    let duplicate_keys: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT COUNT(*) AS n FROM inventory_runs \
         GROUP BY bucket, config_id, run_key HAVING COUNT(*) > 1)",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("统计 inventory_runs 重复自然键失败: {e}"))?;
    let next_run_id = crate::control_store::SqliteControlStore::new(pool.clone())
        .runstate_meta("next_run_id")
        .await
        .map_err(|e| format!("读取 runstate_meta.next_run_id 失败: {e}"))?;
    let max_run_id: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM inventory_runs")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("读取 inventory_runs 最大 id 失败: {e}"))?;
    let next_ok = match next_run_id {
        None => max_run_id == 0,
        Some(next) => next > max_run_id,
    };
    if runs == 0 && cursors == 0 && incrementals == 0 && duplicate_keys == 0 && next_ok {
        // lifecycle 游标是续扫提示而非日志事实；首次激活清掉各副本旧水位（重扫安全），
        // 否则旧的更高 updated_at 会让复制 apply 对首次 upsert 持续 Conflict。
        sqlx::query("DELETE FROM lifecycle_cursors")
            .execute(pool)
            .await
            .map_err(|e| format!("清空本地 lifecycle_cursors 失败: {e}"))?;
        return Ok(());
    }
    Err(format!(
        "[raft].feature_level={configured} 要求 feature 5 inventory 运行态已激活，但未见 \
         schema_meta.{META_INVENTORY_ACTIVATED} 且本地 inventory 表含非日志行/不一致：\
         inventory_runs={runs}、inventory_cursors={cursors}、\
         inventory_incremental={incrementals}、重复 (bucket,config_id,run_key)={duplicate_keys}、\
         next_run_id={next_run_id:?}（须 > MAX(id)={max_run_id}）。v1.23.0 feature-4 的 \
         inventory 行是各节点本地行，不是复制日志事实；直接以 feature 5 启动会静默去重、\
         撞 id 或按副本分叉。运维步骤：保持 feature_level<5（或退回 v1.23.0）启动，等待\
         在途 inventory run 结束；备份控制库后导出 inventory 配置/运行历史并清空 \
         inventory_runs、inventory_cursors、inventory_incremental，删除 \
         runstate_meta.next_run_id，确认后以 feature_level>=5 重启并由调度重新入队；\
         或用 raft-export 备份后在新集群 raft-cutover/force-recover 重灌。\
         注意 jobs 的 schema_meta.{META_RUNSTATE_ACTIVATED} 不作数：它由 level-4 Job* 置位。\
         本二进制不迁移旧本地 inventory 运行态。"
    ))
}

/// apply 事务内更新激活标记：只升不降（R10：与首条 level-2 entry 同事务提交）。
pub async fn bump_activation_tx(
    conn: &mut sqlx::SqliteConnection,
    level: u8,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO schema_meta (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value \
         WHERE CAST(schema_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)",
    )
    .bind(META_FEATURE_LEVEL)
    .bind(level.to_string())
    .execute(conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
