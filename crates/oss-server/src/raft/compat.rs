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

mod object;
#[cfg(test)]
mod tests;

pub use object::guard_object_startup;

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
/// feature 9：账户用量账本（`AccountUsageReserve/Release/Adjust` + OSSCTL3 快照
/// `account_usage_ledger`）。v1.27.0 及更早 advertise ≤8 且不认这些变体/快照字段。
pub const FEATURE_LEVEL_9: u8 = 9;
/// Level C M1 的命令演进容器（`ControlCommand::Ext`）所需级别。该级别只表示
/// “能解码/应用 Ext 容器”，不表示认识任何 `type_id`；注册新类型必须提升 feature
/// level 或先完成 per-peer 类型能力协商（见 `command::REGISTERED_EXT_TYPES`）。
pub const FEATURE_COMMAND_EXT: u8 = FEATURE_LEVEL_3;
/// OSSCTL2/3 命令与快照语义的别名（保持 level 3 不变；M8-2 复制另立 level 4/5）。
pub const FEATURE_OSSCTL2: u8 = FEATURE_LEVEL_3;
/// 本二进制支持的最高 feature level。
pub const MAX_FEATURE_LEVEL: u8 = FEATURE_LEVEL_9;
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
