//! M9 滚动升级兼容层（Level B）：wire/feature 握手、peer 版本表与生效等级。
//!
//! 这些常量与头名是**跨版本 wire 契约**（v1.13.1 = wire 1）：改名或改语义即破坏
//! 兼容窗口。两个整数头只携带能力/意愿，不改变任何 openraft JSON 结构、日志 entry
//! 或快照封套。缺失/非法头一律按 1（旧节点根本不发）——绝不能按 0。
//!
//! 生效等级只在 leader 上计算：当前 voter **和 learner** 在当前 term 的 feature 的
//! 最小值；未知/掉线/旧 term 一律按 1（保守，宁可晚激活）。旧 term 的记录不能信，
//! 否则新 leader 可能提交旧 voter 无法解码的 entry。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

/// 本二进制能解码的协议代次；v1.13.1 = 1。
pub const WIRE_VERSION: u32 = 2;
/// 可共存的最低 wire（rolling 基线 = v1.13.1）。
pub const MIN_COMPAT_WIRE_VERSION: u32 = 1;
/// 成员变更要求全集群 wire >= 2（新节点必须先能读懂新格式）。
pub const MIN_MEMBERSHIP_WIRE_VERSION: u32 = 2;
/// feature 2：结构化生命周期 + OSSCTL2（已发布；不得重编号）。
pub const FEATURE_STRUCTURED_LIFECYCLE: u8 = 2;
/// feature 3：OSSCTL3 可解码 + level-3 命令（Ext/accounts/runstate）可应用。
pub const FEATURE_LEVEL_3: u8 = 3;
/// feature 4：M8-2 jobs/job_tasks/tier_restores 运行态复制（Job* 命令）。v1.22 及
/// 更早二进制 advertise ≤3，因此混版窗口内 leader 的 effective 压回 3、Job* 不 append；
/// 全集群升级并显式配置 4 后才启用。旧二进制（`deny_unknown_fields`）无法解码 Job*，
/// 绝不能让它们收到这些 entry。
pub const FEATURE_LEVEL_4: u8 = 4;
/// feature 5：M8-2 inventory 运行态复制（Inventory* 命令）。**v1.23.0 已 advertise 4
/// 但不认识 Inventory***；level 4 挡不住它，因此必须新开 level 5。v1.23.0 及更早
/// advertise ≤4 → effective ≤4 → Inventory* 在 append 前被拒。
pub const FEATURE_LEVEL_5: u8 = 5;
/// feature 6：作业历史保留（`JobPurge` 命令）。v1.24.0 及更早二进制 advertise ≤5 且
/// 不认识该变体；level 5 挡不住它们，因此必须新开 level 6。旧节点不参与时 effective
/// 压回 ≤5，JobPurge 在 append 前被拒。快照格式仍为 OSSCTL3（无新激活标记）。
pub const FEATURE_LEVEL_6: u8 = 6;
/// feature 7：tier restore 后续状态与 schedule lease（`TierRestore*`/`ScheduleLeaseAcquire`）。
/// v1.25.0 及更早 advertise ≤6 且 `deny_unknown_fields` 不认识这些变体；level 6 挡不住
/// 它们，因此必须新开 level 7。快照复用 OSSCTL3 `m8.runstate`（两表已在格式 1 内）。
pub const FEATURE_LEVEL_7: u8 = 7;
/// feature 8：账户硬配额配置（`AccountHardQuotaSet`）。v1.26.0 及更早 advertise ≤7
/// 且 `deny_unknown_fields` 不认识该变体；level 7 挡不住它，必须新开 level 8。
pub const FEATURE_LEVEL_8: u8 = 8;
/// Level C M1 的命令演进容器（`ControlCommand::Ext`）所需级别。该级别只表示
/// “能解码/应用 Ext 容器”，不表示认识任何 `type_id`；注册新类型必须提升 feature
/// level 或先完成 per-peer 类型能力协商（见 `command::REGISTERED_EXT_TYPES`）。
pub const FEATURE_COMMAND_EXT: u8 = FEATURE_LEVEL_3;
/// OSSCTL2/3 命令与快照语义的别名（保持 level 3 不变；M8-2 复制另立 level 4/5）。
pub const FEATURE_OSSCTL2: u8 = FEATURE_LEVEL_3;
/// 本二进制支持的最高 feature level。
pub const MAX_FEATURE_LEVEL: u8 = FEATURE_LEVEL_8;
/// 缺省/旧节点的 feature level。
pub const MIN_FEATURE_LEVEL: u8 = 1;

pub const WIRE_HEADER: &str = "x-oss-raft-wire-version";
pub const FEATURE_HEADER: &str = "x-oss-raft-feature-level";
/// Level C `Ext` 类型能力头：逗号分隔的已注册 `type_id` 集合。**旧节点不发/不认该头**，
/// 缺失 = 空集（绝不把「未知」当支持）；因此新类型只有在全员显式宣告后才可 append。
pub const EXT_TYPES_HEADER: &str = "x-oss-raft-ext-types";
/// 单个 `type_id` 的入站上限（注册表条目远小于此；超长条目丢弃，防畸形头撑大内存）。
pub const EXT_TYPE_ID_MAX: usize = 96;
/// 能力头最多收录/解析的 `type_id` 数（注册表极小；超出截断只影响协商，不影响已注册类型）。
pub const EXT_TYPES_MAX: usize = 64;
/// `schema_meta` 中的激活标记（节点本地；缺省 = 1，单调不减）。
pub const META_FEATURE_LEVEL: &str = "raft_feature_level";
/// 运行态激活标记：本机运行态来自 level-4 日志/快照或显式 cutover 种子（节点本地）。
/// 普通 level-3 快照安装与空表启动**不得**置位；level-3 快照携带的是构建者本地行。
pub const META_RUNSTATE_ACTIVATED: &str = "m8_runstate_activated";
/// inventory 运行态激活标记（独立于 jobs 的 level-4 标记）：只由 level-5 事实置位。
/// v1.23.0 feature-4 的 inventory 行是节点本地行，且 Job* 会置位 level-4 marker，
/// 因此绝不能复用/短路 `META_RUNSTATE_ACTIVATED`。
pub const META_INVENTORY_ACTIVATED: &str = "m8_inventory_activated";

/// 本二进制能否应用某个 feature level（未知更高 level 一律 false）。
pub fn supports(level: u8) -> bool {
    level <= MAX_FEATURE_LEVEL
}

/// 对外宣告的 feature 是**意愿**而非二进制能力（D3）：`min(configured, MAX)`。
/// 配置 1 的节点即便能解码 level 2 也不得收到 level 2 entry（否则重启会被 R8 拦下）。
pub fn advertised_feature(configured: u8) -> u8 {
    configured.min(MAX_FEATURE_LEVEL)
}

/// 入站 wire 头：缺失/非法 = 1；`0` 也按 1（非法值不得放宽任何闸门）。
pub fn parse_wire(value: Option<&str>) -> u32 {
    value
        .and_then(|s| s.trim().parse::<u32>().ok())
        .filter(|v| *v >= MIN_COMPAT_WIRE_VERSION)
        .unwrap_or(MIN_COMPAT_WIRE_VERSION)
}

/// 入站 feature 头：缺失/非法/0 = 1。
pub fn parse_feature(value: Option<&str>) -> u8 {
    value
        .and_then(|s| s.trim().parse::<u8>().ok())
        .filter(|v| *v >= MIN_FEATURE_LEVEL)
        .unwrap_or(MIN_FEATURE_LEVEL)
}

/// 入站 Ext 类型能力头：旧节点缺失 = 空集；空串同样 = 空集（明确宣告「无类型」）。
/// 逗号分隔、去空白、去重；超长或含非可打印 ASCII 的条目直接丢弃——畸形宣告绝不能
/// 变成「支持」。
pub fn parse_ext_types(value: Option<&str>) -> BTreeSet<String> {
    value
        .into_iter()
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|t| {
            !t.is_empty()
                && t.len() <= EXT_TYPE_ID_MAX
                && t.bytes().all(|b| (0x21..=0x7e).contains(&b))
        })
        .take(EXT_TYPES_MAX)
        .map(str::to_string)
        .collect()
}

/// 渲染出站能力头（逗号分隔）。注册表为空 → 空串（= 无类型，旧节点语义相同）。
pub fn format_ext_types(types: &[&str]) -> String {
    types
        .iter()
        .filter(|t| !t.is_empty())
        .take(EXT_TYPES_MAX)
        .copied()
        .collect::<Vec<_>>()
        .join(",")
}

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

/// R8 启动守卫：marker 高于二进制最高或高于本机配置 → 拒绝启动（绝不降级运行）。
pub fn guard_startup(marker: u8, configured: u8) -> Result<(), String> {
    guard_startup_with_max(marker, configured, MAX_FEATURE_LEVEL)
}

/// 同 [`guard_startup`]，`max` 参数只为单测模拟旧二进制（v1.22 的 MAX=3）。
fn guard_startup_with_max(marker: u8, configured: u8, max: u8) -> Result<(), String> {
    let advertised = configured.min(max);
    if marker > max {
        return Err(format!(
            "schema_meta.{META_FEATURE_LEVEL}={marker} 高于本二进制最高支持 {max}；\
             集群已激活本二进制无法应用的功能，请换回支持 level {marker} 的二进制"
        ));
    }
    if marker > advertised {
        return Err(format!(
            "schema_meta.{META_FEATURE_LEVEL}={marker} 高于 [raft].feature_level={configured}\
             （宣告 {advertised}）；已激活集群不能用更低配置启动，请恢复 feature_level={marker}"
        ));
    }
    Ok(())
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
    let next_run_id: Option<i64> =
        sqlx::query_scalar("SELECT value FROM runstate_meta WHERE key = 'next_run_id'")
            .fetch_optional(pool)
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

/// 某个 peer 最近一次（带 term）的握手结果。`term` 是记录时的 leader term。
/// `ext_types` 为空 = 旧节点/未宣告（绝不推断为「支持任意类型」）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerVersion {
    pub wire: u32,
    pub feature: u8,
    pub term: u64,
    pub ext_types: BTreeSet<String>,
}

/// 节点本地内存表；不持久化（重启 = 全部未知 = 1，首次心跳自愈）。
#[derive(Debug, Default)]
pub struct PeerVersions {
    inner: Mutex<BTreeMap<u64, PeerVersion>>,
}

impl PeerVersions {
    /// 旧语义/旧对端：无 Ext 类型能力（Level C 之前的握手路径）。
    pub fn record(&self, node_id: u64, term: u64, wire: u32, feature: u8) {
        self.record_ext(node_id, term, wire, feature, BTreeSet::new());
    }

    pub fn record_ext(
        &self,
        node_id: u64,
        term: u64,
        wire: u32,
        feature: u8,
        ext_types: BTreeSet<String>,
    ) {
        if let Ok(mut map) = self.inner.lock() {
            map.insert(
                node_id,
                PeerVersion {
                    wire,
                    feature,
                    term,
                    ext_types,
                },
            );
        }
    }

    pub fn get(&self, node_id: u64) -> Option<PeerVersion> {
        self.inner.lock().ok()?.get(&node_id).cloned()
    }

    /// `/status` 诊断用快照（含旧 term 记录，便于排查）。
    pub fn snapshot(&self) -> BTreeMap<u64, PeerVersion> {
        self.inner.lock().map(|m| m.clone()).unwrap_or_default()
    }

    /// 是否有当前 term 的旧 wire peer（应告警的混合状态）。
    pub fn mixed(&self, current_term: u64) -> bool {
        self.snapshot()
            .values()
            .any(|p| p.term == current_term && p.wire < WIRE_VERSION)
    }
}

/// 生效等级 + 人类可读的阻塞原因（`None` = 无阻塞）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Effective {
    pub level: u8,
    pub blocked_reason: Option<String>,
}

/// leader 生效等级：`min(local, 每个当前 voter/learner 在当前 term 的 feature)`。
///
/// 未知/掉线/旧 term 按 1，并给出原因。learner 必须计入：它同样接收日志与快照。
pub fn effective_feature_level(
    local_configured: u8,
    own: u64,
    voters: &[u64],
    learners: &[u64],
    current_term: u64,
    peers: &PeerVersions,
) -> Effective {
    let mut level = advertised_feature(local_configured);
    let mut reason = None;
    for (id, kind) in voters
        .iter()
        .map(|id| (*id, "voter"))
        .chain(learners.iter().map(|id| (*id, "learner")))
    {
        if id == own {
            continue;
        }
        let (feature, why) = match peers.get(id) {
            Some(p) if p.term == current_term => (
                p.feature,
                format!("{kind} {id} wire {} feature {}", p.wire, p.feature),
            ),
            Some(p) => (
                MIN_FEATURE_LEVEL,
                format!("{kind} {id} stale term {} (feature 1)", p.term),
            ),
            None => (
                MIN_FEATURE_LEVEL,
                format!("{kind} {id} not heard in term {current_term} (feature 1)"),
            ),
        };
        if feature < level {
            level = feature;
            reason = Some(why);
        }
    }
    Effective {
        level,
        blocked_reason: reason,
    }
}

/// Level C leader 侧 Ext 类型协商：当前 term 的每个 voter/learner 都必须**显式宣告**
/// 支持 `type_id`（旧节点不发头 = 空集 = 不支持）。本机由注册表另判，不在此表。
///
/// 返回缺失节点的人类可读原因（空 = 全员支持）。调用方必须在 append 前拒绝；apply
/// 的未知类型 fail-closed 仍是最后一道防线（快照/旧 leader 日志仍可能带入）。
pub fn ext_type_missing(
    type_id: &str,
    own: u64,
    voters: &[u64],
    learners: &[u64],
    current_term: u64,
    peers: &PeerVersions,
) -> Vec<String> {
    let mut missing = Vec::new();
    for (id, kind) in voters
        .iter()
        .map(|id| (*id, "voter"))
        .chain(learners.iter().map(|id| (*id, "learner")))
    {
        if id == own {
            continue;
        }
        match peers.get(id) {
            Some(p) if p.term == current_term && p.ext_types.contains(type_id) => {}
            Some(p) if p.term == current_term => {
                let seen = if p.ext_types.is_empty() {
                    "advertised no ext types".to_string()
                } else {
                    format!(
                        "advertised [{}]",
                        p.ext_types.iter().cloned().collect::<Vec<_>>().join(",")
                    )
                };
                missing.push(format!("{kind} {id} {seen} (feature {})", p.feature));
            }
            Some(p) => missing.push(format!(
                "{kind} {id} stale term {} (no current-term capability handshake)",
                p.term
            )),
            None => missing.push(format!(
                "{kind} {id} not heard in term {current_term} (capability unknown)"
            )),
        }
    }
    missing
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_or_malformed_headers_mean_level_one() {
        assert_eq!(parse_wire(None), 1);
        assert_eq!(parse_wire(Some("garbage")), 1);
        assert_eq!(parse_wire(Some("0")), 1);
        assert_eq!(parse_wire(Some("")), 1);
        assert_eq!(parse_wire(Some("1")), 1);
        assert_eq!(parse_wire(Some("2")), 2);
        assert_eq!(parse_wire(Some("3")), 3);
        assert_eq!(parse_feature(None), 1);
        assert_eq!(parse_feature(Some("x")), 1);
        assert_eq!(parse_feature(Some("0")), 1);
        assert_eq!(parse_feature(Some("2")), 2);
    }

    #[test]
    fn advertised_feature_is_willingness_capped_at_binary_max() {
        assert_eq!(advertised_feature(1), 1);
        assert_eq!(advertised_feature(2), 2);
        assert_eq!(advertised_feature(9), MAX_FEATURE_LEVEL);
    }

    #[test]
    fn supports_is_bounded_by_binary_max_and_levels_are_fixed() {
        assert_eq!(MIN_FEATURE_LEVEL, 1);
        assert_eq!(FEATURE_STRUCTURED_LIFECYCLE, 2);
        assert_eq!(FEATURE_LEVEL_3, 3);
        assert_eq!(FEATURE_COMMAND_EXT, 3);
        assert_eq!(FEATURE_COMMAND_EXT, FEATURE_LEVEL_3);
        assert_eq!(FEATURE_LEVEL_4, 4, "M8-2 jobs 复制是新 level，不得复用 3");
        assert_eq!(
            FEATURE_LEVEL_5, 5,
            "v1.23.0 已 advertise 4 且不认识 Inventory*，必须新开 5"
        );
        assert_eq!(
            FEATURE_LEVEL_6, 6,
            "v1.24.0 及更早 advertise ≤5 且不认识 JobPurge，必须新开 6"
        );
        assert_eq!(
            FEATURE_LEVEL_7, 7,
            "v1.25.0 及更早 advertise ≤6 且不认识 TierRestore*/ScheduleLeaseAcquire，必须新开 7"
        );
        assert_eq!(
            FEATURE_LEVEL_8, 8,
            "v1.26.0 及更早 advertise ≤7 且不认识 AccountHardQuotaSet，必须新开 8"
        );
        assert_eq!(MAX_FEATURE_LEVEL, FEATURE_LEVEL_8);
        assert!(supports(1));
        assert!(supports(FEATURE_STRUCTURED_LIFECYCLE));
        assert!(supports(FEATURE_COMMAND_EXT));
        assert!(supports(FEATURE_LEVEL_4));
        assert!(supports(FEATURE_LEVEL_5));
        assert!(supports(FEATURE_LEVEL_6));
        assert!(supports(FEATURE_LEVEL_7));
        assert!(supports(FEATURE_LEVEL_8));
        assert!(!supports(MAX_FEATURE_LEVEL + 1));
    }

    /// JobPurge 的 append 前闸门：混版集群里只要有一个 level-5 节点，effective 就是 5，
    /// 低于命令要求的 6（`check_feature` 因此拒绝，不写日志、不激活 marker）。
    #[test]
    fn job_purge_gate_rejects_level_five_cluster_before_append() {
        let p = peers_all(7, &[1, 2], &[], 5);
        let e = effective_feature_level(FEATURE_LEVEL_6, 1, &[1, 2], &[], 7, &p);
        assert_eq!(e.level, 5);
        let purge = super::super::command::ControlCommand::JobPurge { job_ids: vec![1] };
        assert_eq!(purge.min_feature_level(), FEATURE_LEVEL_6);
        assert!(
            e.level < purge.min_feature_level(),
            "level-5 节点在场时 append 前拒绝 JobPurge"
        );
        assert!(e.blocked_reason.unwrap().contains("voter 2"));
    }

    /// level-7 的 append 前闸门：混版集群里只要有一个 level-6 节点，effective 就是 6，
    /// 低于 restore 后续状态/租约命令要求的 7（`check_feature` 拒绝，不写日志）。
    #[test]
    fn level_seven_commands_rejected_on_level_six_cluster() {
        use super::super::command::ControlCommand;
        use crate::raft::runstate::ScheduleLeaseRow;

        let p = peers_all(7, &[1, 2], &[], FEATURE_LEVEL_6);
        let e = effective_feature_level(FEATURE_LEVEL_7, 1, &[1, 2], &[], 7, &p);
        assert_eq!(e.level, FEATURE_LEVEL_6);
        let lease = ControlCommand::ScheduleLeaseAcquire {
            lease: ScheduleLeaseRow {
                term: 1,
                node: 1,
                expires_at: 2,
                acquired_at: 1,
            },
            now: 1,
        };
        let restore = ControlCommand::TierRestoreDelete {
            bucket: "b".into(),
            key: "k".into(),
            version_id: "v".into(),
            fence_term: 1,
        };
        for cmd in [lease, restore] {
            assert_eq!(cmd.min_feature_level(), FEATURE_LEVEL_7);
            assert!(
                e.level < cmd.min_feature_level(),
                "level-6 节点在场时 append 前拒绝 {cmd:?}"
            );
        }

        // 硬配额配置是 level 8：level-7 集群同样必须在 append 前拒绝。
        let p7 = peers_all(8, &[1, 2], &[], FEATURE_LEVEL_7);
        let e7 = effective_feature_level(FEATURE_LEVEL_8, 1, &[1, 2], &[], 8, &p7);
        assert_eq!(e7.level, FEATURE_LEVEL_7);
        let quota = ControlCommand::AccountHardQuotaSet {
            id: "000000000001".into(),
            hard_byte_quota: Some(1),
            object_quota: None,
            updated_at: 1,
        };
        assert_eq!(quota.min_feature_level(), FEATURE_LEVEL_8);
        assert!(e7.level < quota.min_feature_level());
        assert!(e.blocked_reason.unwrap().contains("voter 2"));
    }

    fn peers_all(term: u64, voters: &[u64], learners: &[u64], feature: u8) -> PeerVersions {
        let p = PeerVersions::default();
        for id in voters.iter().chain(learners) {
            p.record(*id, term, WIRE_VERSION, feature);
        }
        p
    }

    #[test]
    fn effective_is_min_over_voters_and_learners() {
        let p = peers_all(7, &[1, 2, 3], &[4], 2);
        assert_eq!(
            effective_feature_level(2, 1, &[1, 2, 3], &[4], 7, &p).level,
            2
        );

        // learner 掉到 1 也必须阻塞（它会收到 level-2 entry/snapshot）。
        p.record(4, 7, WIRE_VERSION, 1);
        let e = effective_feature_level(2, 1, &[1, 2, 3], &[4], 7, &p);
        assert_eq!(e.level, 1);
        assert!(e.blocked_reason.unwrap().contains("learner 4"));

        // voter 掉到 1 同理。
        let p = peers_all(7, &[1, 2, 3], &[], 2);
        p.record(3, 7, WIRE_VERSION, 1);
        let e = effective_feature_level(2, 1, &[1, 2, 3], &[], 7, &p);
        assert_eq!(e.level, 1);
        assert!(
            e.blocked_reason
                .unwrap()
                .contains("voter 3 wire 2 feature 1")
        );
    }

    fn ext(types: &[&str]) -> BTreeSet<String> {
        types.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn ext_type_header_is_parsed_conservatively() {
        assert!(parse_ext_types(None).is_empty());
        assert!(parse_ext_types(Some("")).is_empty());
        assert!(parse_ext_types(Some(" , ")).is_empty());
        assert_eq!(
            parse_ext_types(Some("a.b, c-d ,a.b,,")),
            ext(&["a.b", "c-d"]),
            "去空白且去重"
        );
        // 畸形条目绝不能变成「支持」。
        assert!(parse_ext_types(Some("ok\u{7f},bad\nname")).is_empty());
        assert!(parse_ext_types(Some(&"x".repeat(EXT_TYPE_ID_MAX + 1))).is_empty());
        assert_eq!(format_ext_types(&[]), "");
        assert_eq!(format_ext_types(&["a", "", "b"]), "a,b");
        assert_eq!(
            parse_ext_types(Some(&format_ext_types(&["a.b", "c"]))),
            ext(&["a.b", "c"])
        );
    }

    #[test]
    fn ext_type_gate_requires_every_current_voter_and_learner() {
        let p = PeerVersions::default();
        p.record_ext(1, 7, WIRE_VERSION, 3, ext(&["test.noop"]));
        p.record_ext(2, 7, WIRE_VERSION, 8, ext(&["test.noop"]));
        p.record_ext(4, 7, WIRE_VERSION, 8, ext(&["test.noop"]));
        assert!(
            ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &p).is_empty(),
            "本机记录不参与（由注册表判定）"
        );

        // 当前 term 有握手但未宣告（旧节点默认空集）→ append 前拒绝并点名。
        p.record_ext(2, 7, WIRE_VERSION, 8, BTreeSet::new());
        let m = ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &p);
        assert_eq!(m.len(), 1, "{m:?}");
        assert!(
            m[0].contains("voter 2") && m[0].contains("no ext types"),
            "{m:?}"
        );

        // learner 同样计入，且错误里有它宣告的类型便于排查。
        p.record_ext(2, 7, WIRE_VERSION, 8, ext(&["test.noop"]));
        p.record_ext(4, 7, WIRE_VERSION, 8, ext(&["other"]));
        let m = ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &p);
        assert!(
            m[0].contains("learner 4") && m[0].contains("other"),
            "{m:?}"
        );

        // 旧 term 记录不可信；完全没握手的当前成员同样缺失。
        p.record_ext(4, 6, WIRE_VERSION, 8, ext(&["test.noop"]));
        let m = ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &p);
        assert!(m[0].contains("stale term 6"), "{m:?}");
        let m = ext_type_missing("test.noop", 1, &[1, 2], &[4], 7, &PeerVersions::default());
        assert_eq!(m.len(), 2);
        assert!(m.iter().all(|s| s.contains("not heard")), "{m:?}");
    }

    #[test]
    fn effective_ignores_previous_term_records() {
        let p = peers_all(6, &[1, 2], &[], 2);
        let e = effective_feature_level(2, 1, &[1, 2], &[], 7, &p);
        assert_eq!(e.level, 1);
        assert!(e.blocked_reason.unwrap().contains("stale term 6"));
    }

    #[test]
    fn unanswered_peer_contributes_one() {
        let p = PeerVersions::default();
        let e = effective_feature_level(2, 1, &[1, 2], &[], 7, &p);
        assert_eq!(e.level, 1);
        assert!(e.blocked_reason.unwrap().contains("not heard"));

        // 本地配置 1 时即便所有 peer 都是 2 也只能是 1。
        let p = peers_all(7, &[1, 2], &[], 2);
        assert_eq!(effective_feature_level(1, 1, &[1, 2], &[], 7, &p).level, 1);
    }

    #[test]
    fn startup_guard_rejects_marker_above_config_or_binary() {
        assert!(guard_startup(1, 1).is_ok());
        assert!(guard_startup(2, 2).is_ok());
        // 已激活 2 的集群不能用 feature_level=1 启动。
        assert!(guard_startup(2, 1).is_err());
        // 二进制最高 2：marker 3 无从谈起，但必须拒绝而不是截断。
        assert!(guard_startup(3, 2).is_err());
        // feature 4 激活后：本二进制（max 4）显式配置 4 才放行。
        assert!(guard_startup(4, 4).is_ok());
        assert!(guard_startup(4, 3).is_err(), "配置低于 marker 必须拒绝");
        assert!(guard_startup(5, 5).is_ok());
        assert!(guard_startup(5, 4).is_err(), "配置低于 marker 必须拒绝");
        assert!(guard_startup(6, 6).is_ok());
        assert!(guard_startup(6, 5).is_err(), "配置低于 marker 必须拒绝");
        assert!(guard_startup(7, 7).is_ok());
        assert!(guard_startup(7, 6).is_err(), "配置低于 marker 必须拒绝");
        assert!(guard_startup(8, 7).is_err(), "未知更高 marker 必须拒绝");
        // v1.22（max 3）遇到 marker 4、v1.23.0（max 4）遇到 marker 5、
        // v1.24.0（max 5）遇到 marker 6、v1.25.0（max 6）遇到 marker 7 必须拒绝。
        assert!(guard_startup_with_max(4, 4, 3).is_err());
        assert!(guard_startup_with_max(3, 3, 3).is_ok());
        assert!(guard_startup_with_max(5, 5, 4).is_err());
        assert!(guard_startup_with_max(4, 4, 4).is_ok());
        assert!(guard_startup_with_max(6, 6, 5).is_err());
        assert!(guard_startup_with_max(5, 5, 5).is_ok());
        assert!(guard_startup_with_max(7, 7, 6).is_err());
        assert!(guard_startup_with_max(6, 6, 6).is_ok());
    }

    #[tokio::test]
    async fn activation_marker_defaults_to_one_and_only_increases() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(read_activation(&pool).await.unwrap(), 1);
        let mut conn = pool.acquire().await.unwrap();
        bump_activation_tx(&mut conn, 2).await.unwrap();
        bump_activation_tx(&mut conn, 2).await.unwrap();
        drop(conn);
        assert_eq!(read_activation(&pool).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn runstate_activation_guard_allows_empty_tables_without_marker() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        // feature ≤3 不启用复制，不检查也不落标记（保持本地作业行为）。
        for configured in [1, 2, 3] {
            assert!(
                guard_runstate_activation(db.pool(), configured)
                    .await
                    .is_ok()
            );
            assert_eq!(db.schema_meta(META_RUNSTATE_ACTIVATED).await.unwrap(), None);
        }
        // 空表 feature 4：放行，但不得提前写标记（effective 可能仍是 3）。
        assert!(guard_runstate_activation(db.pool(), 4).await.is_ok());
        assert_eq!(db.schema_meta(META_RUNSTATE_ACTIVATED).await.unwrap(), None);
    }

    #[tokio::test]
    async fn runstate_activation_guard_rejects_old_local_rows_without_marker() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        db.create_job(&crate::db::JobSpec::default(), 1)
            .await
            .unwrap();
        // feature 3 仍走本地作业路径：非空表不得误拒。
        assert!(guard_runstate_activation(db.pool(), 3).await.is_ok());
        assert_eq!(db.schema_meta(META_RUNSTATE_ACTIVATED).await.unwrap(), None);
        let err = guard_runstate_activation(db.pool(), 4).await.unwrap_err();
        assert!(
            err.contains("jobs=1") && err.contains("feature_level") && err.contains("清空"),
            "{err}"
        );
        // 显式 cutover/level-4 事实留下标记 → 非空表也放行，避免重启误拒。
        let mut conn = db.pool().acquire().await.unwrap();
        mark_runstate_activated_tx(&mut conn).await.unwrap();
        drop(conn);
        assert!(guard_runstate_activation(db.pool(), 4).await.is_ok());
    }

    #[tokio::test]
    async fn runstate_activation_guard_rejects_dangling_job_refs() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO inventory_runs \
             (bucket, config_id, run_key, started_at, state, job_id, created_at) \
             VALUES ('b', 'c', 'r', 1, 'done', 42, 1)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let err = guard_runstate_activation(db.pool(), 4).await.unwrap_err();
        assert!(
            err.contains("inventory_runs=1")
                && err.contains("inventory_runs.job_id=1")
                && err.contains("清空"),
            "{err}"
        );
        sqlx::query("DELETE FROM inventory_runs")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(guard_runstate_activation(db.pool(), 4).await.is_ok());
    }

    #[tokio::test]
    async fn runstate_activation_guard_counts_inventory_cursors_and_incrementals() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, updated_at) \
             VALUES ('b', 'c', 'x', 1, 1)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let err = guard_runstate_activation(db.pool(), 4).await.unwrap_err();
        assert!(
            err.contains("inventory_cursors=1") && err.contains("inventory_incremental=0"),
            "{err}"
        );
        sqlx::query("DELETE FROM inventory_cursors")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
             VALUES ('b', 'c', 1, 1)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let err = guard_runstate_activation(db.pool(), 4).await.unwrap_err();
        assert!(err.contains("inventory_incremental=1"), "{err}");
        sqlx::query("DELETE FROM inventory_incremental")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(guard_runstate_activation(db.pool(), 4).await.is_ok());
    }

    #[tokio::test]
    async fn inventory_activation_guard_is_independent_of_runstate_marker() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        // feature ≤4：inventory 仍是节点本地，不检查也不落标记。
        for configured in [1, 2, 3, 4] {
            assert!(
                guard_inventory_activation(db.pool(), configured)
                    .await
                    .is_ok()
            );
            assert_eq!(
                db.schema_meta(META_INVENTORY_ACTIVATED).await.unwrap(),
                None
            );
        }
        sqlx::query(
            "INSERT INTO inventory_runs \
             (bucket, config_id, run_key, started_at, state, created_at) \
             VALUES ('b', 'c', 'r', 1, 'done', 1)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        // Job* 已置位的 level-4 marker 不能让 v1.23.0 的本地行绕过 level-5 守卫。
        let mut conn = db.pool().acquire().await.unwrap();
        mark_runstate_activated_tx(&mut conn).await.unwrap();
        drop(conn);
        let err = guard_inventory_activation(db.pool(), 5).await.unwrap_err();
        assert!(
            err.contains("inventory_runs=1") && err.contains("导出") && err.contains("next_run_id"),
            "{err}"
        );
        // 显式 level-5 事实写 marker 后放行（重启不误拒）。
        let mut conn = db.pool().acquire().await.unwrap();
        mark_inventory_activated_tx(&mut conn).await.unwrap();
        drop(conn);
        assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
        // marker 独立：撤销 inventory marker 不会碰 runstate marker，反之亦然。
        let mut conn = db.pool().acquire().await.unwrap();
        clear_inventory_activated_tx(&mut conn).await.unwrap();
        drop(conn);
        assert_eq!(
            db.schema_meta(META_INVENTORY_ACTIVATED).await.unwrap(),
            None
        );
        assert_eq!(
            db.schema_meta(META_RUNSTATE_ACTIVATED).await.unwrap(),
            Some("1".into())
        );
    }

    #[tokio::test]
    async fn inventory_activation_guard_allows_empty_tables_and_checks_meta() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
        assert_eq!(
            db.schema_meta(META_INVENTORY_ACTIVATED).await.unwrap(),
            None
        );
        // 空表但 next_run_id 落后/非法仍 fail-closed，避免删除后复用 id。
        sqlx::query("INSERT INTO runstate_meta (key, value) VALUES ('next_run_id', '0')")
            .execute(db.pool())
            .await
            .unwrap();
        let err = guard_inventory_activation(db.pool(), 5).await.unwrap_err();
        assert!(err.contains("next_run_id"), "{err}");
        sqlx::query("UPDATE runstate_meta SET value = '1' WHERE key = 'next_run_id'")
            .execute(db.pool())
            .await
            .unwrap();
        assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
        // 只有 cursor/incremental 行也必须拒绝。
        sqlx::query(
            "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
             VALUES ('b', 'c', 1, 1)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let err = guard_inventory_activation(db.pool(), 5).await.unwrap_err();
        assert!(err.contains("inventory_incremental=1"), "{err}");
    }

    #[tokio::test]
    async fn first_feature_five_activation_clears_legacy_local_lifecycle_cursors() {
        let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at) \
             VALUES ('b', 'legacy:', 'current', 'k', 9999999999)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_cursors")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(left, 0, "首次 feature-5 激活必须清空本地续扫游标");
        // 已激活后的重启不清日志复制来的游标。
        sqlx::query(
            "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at) \
             VALUES ('b', 'r', 'current', 'k2', 1)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let mut conn = db.pool().acquire().await.unwrap();
        mark_inventory_activated_tx(&mut conn).await.unwrap();
        drop(conn);
        assert!(guard_inventory_activation(db.pool(), 5).await.is_ok());
        let kept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_cursors")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(kept, 1, "已激活节点重启不得清空日志复制的游标");
    }

    #[test]
    fn mixed_only_counts_current_term_legacy_wire() {
        let p = PeerVersions::default();
        p.record(2, 7, 1, 1);
        assert!(p.mixed(7));
        assert!(!p.mixed(8), "stale-term legacy record must not alarm");
        p.record(2, 8, WIRE_VERSION, 2);
        assert!(!p.mixed(8));
    }
}
