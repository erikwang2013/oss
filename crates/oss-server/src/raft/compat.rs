//! M9 滚动升级兼容层（Level B）：wire/feature 握手、peer 版本表与生效等级。
//!
//! 这些常量与头名是**跨版本 wire 契约**（v1.13.1 = wire 1）：改名或改语义即破坏
//! 兼容窗口。两个整数头只携带能力/意愿，不改变任何 openraft JSON 结构、日志 entry
//! 或快照封套。缺失/非法头一律按 1（旧节点根本不发）——绝不能按 0。
//!
//! 生效等级只在 leader 上计算：当前 voter **和 learner** 在当前 term 的 feature 的
//! 最小值；未知/掉线/旧 term 一律按 1（保守，宁可晚激活）。旧 term 的记录不能信，
//! 否则新 leader 可能提交旧 voter 无法解码的 entry。

use std::collections::BTreeMap;
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
/// Level C M1 的命令演进容器（`ControlCommand::Ext`）所需级别。该级别只表示
/// “能解码/应用 Ext 容器”，不表示认识任何 `type_id`；注册新类型必须提升 feature
/// level 或先完成 per-peer 类型能力协商（见 `command::REGISTERED_EXT_TYPES`）。
pub const FEATURE_COMMAND_EXT: u8 = FEATURE_LEVEL_3;
/// OSSCTL2/3 命令与快照语义的别名（保持 level 3 不变；M8-2 复制另立 level 4）。
pub const FEATURE_OSSCTL2: u8 = FEATURE_LEVEL_3;
/// 本二进制支持的最高 feature level。
pub const MAX_FEATURE_LEVEL: u8 = FEATURE_LEVEL_4;
/// 缺省/旧节点的 feature level。
pub const MIN_FEATURE_LEVEL: u8 = 1;

pub const WIRE_HEADER: &str = "x-oss-raft-wire-version";
pub const FEATURE_HEADER: &str = "x-oss-raft-feature-level";
/// `schema_meta` 中的激活标记（节点本地；缺省 = 1，单调不减）。
pub const META_FEATURE_LEVEL: &str = "raft_feature_level";
/// 运行态激活标记：本机运行态来自 level-4 日志/快照或显式 cutover 种子（节点本地）。
/// 普通 level-3 快照安装与空表启动**不得**置位；level-3 快照携带的是构建者本地行。
pub const META_RUNSTATE_ACTIVATED: &str = "m8_runstate_activated";

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
    let mut counts = [0i64; 4];
    for (i, table) in ["jobs", "job_tasks", "inventory_runs", "tier_restores"]
        .into_iter()
        .enumerate()
    {
        counts[i] =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(pool)
                .await
                .map_err(|e| format!("统计 {table} 失败: {e}"))?;
    }
    let [jobs, tasks, runs, restores] = counts;
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
        && restores == 0
        && inv_dangling == 0
        && tier_dangling == 0
    {
        return Ok(());
    }
    Err(format!(
        "[raft].feature_level={configured} 要求 feature 4 运行态已激活，但未见 \
         schema_meta.{META_RUNSTATE_ACTIVATED} 且本地运行态含非日志行：jobs={jobs}、\
         job_tasks={tasks}、inventory_runs={runs}、tier_restores={restores}；\
         悬空引用：inventory_runs.job_id={inv_dangling}、tier_restores.job_id={tier_dangling}。\
         level-2/3 的本地行与 level-3 快照拷贝都不是复制日志事实，直接激活会静默丢作业、\
         产生 ID 冲突，或让快照安装因悬空引用失败。运维步骤：保持 feature_level<4 启动，\
         等待所有作业进入终态并完成/导出；备份控制库后清空 jobs、job_tasks、inventory_runs、\
         tier_restores（悬空引用随行删除；若要保留 inventory/tier 行，只能改用导出重灌，\
         本守卫要求四表为空且无悬空引用），确认后以 feature_level>=4 重启；或用 \
         raft-export 备份后在新集群 raft-cutover/force-recover 重灌（显式种子会写激活\
         标记）。本二进制不迁移旧本地运行态。"
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerVersion {
    pub wire: u32,
    pub feature: u8,
    pub term: u64,
}

/// 节点本地内存表；不持久化（重启 = 全部未知 = 1，首次心跳自愈）。
#[derive(Debug, Default)]
pub struct PeerVersions {
    inner: Mutex<BTreeMap<u64, PeerVersion>>,
}

impl PeerVersions {
    pub fn record(&self, node_id: u64, term: u64, wire: u32, feature: u8) {
        if let Ok(mut map) = self.inner.lock() {
            map.insert(
                node_id,
                PeerVersion {
                    wire,
                    feature,
                    term,
                },
            );
        }
    }

    pub fn get(&self, node_id: u64) -> Option<PeerVersion> {
        self.inner.lock().ok()?.get(&node_id).copied()
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
        assert_eq!(FEATURE_LEVEL_4, 4, "M8-2 复制是新 level，不得复用 3");
        assert_eq!(MAX_FEATURE_LEVEL, FEATURE_LEVEL_4);
        assert!(supports(1));
        assert!(supports(FEATURE_STRUCTURED_LIFECYCLE));
        assert!(supports(FEATURE_COMMAND_EXT));
        assert!(supports(FEATURE_LEVEL_4));
        assert!(!supports(MAX_FEATURE_LEVEL + 1));
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
        assert!(guard_startup(5, 4).is_err(), "未知更高 marker 必须拒绝");
        // v1.22（max 3）遇到 marker 4 必须拒绝启动，而不是降级运行。
        assert!(guard_startup_with_max(4, 4, 3).is_err());
        assert!(guard_startup_with_max(3, 3, 3).is_ok());
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
