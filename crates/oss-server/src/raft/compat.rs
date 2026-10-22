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
/// 本二进制支持的最高 feature level。
pub const MAX_FEATURE_LEVEL: u8 = 2;
/// 缺省/旧节点的 feature level。
pub const MIN_FEATURE_LEVEL: u8 = 1;

pub const WIRE_HEADER: &str = "x-oss-raft-wire-version";
pub const FEATURE_HEADER: &str = "x-oss-raft-feature-level";
/// `schema_meta` 中的激活标记（节点本地；缺省 = 1，单调不减）。
pub const META_FEATURE_LEVEL: &str = "raft_feature_level";

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
    let value: Option<String> = sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = ?")
        .bind(META_FEATURE_LEVEL)
        .fetch_optional(pool)
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
    let advertised = advertised_feature(configured);
    if marker > MAX_FEATURE_LEVEL {
        return Err(format!(
            "schema_meta.{META_FEATURE_LEVEL}={marker} 高于本二进制最高支持 {MAX_FEATURE_LEVEL}；\
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
