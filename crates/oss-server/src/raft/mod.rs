//! M9 控制面：Raft 基座（S5）+ 控制命令/apply 门面（S6）。
//!
//! S5 接入 openraft 0.9.25 单节点运行时；S6 增加 [`command`]（唯一写意图）、[`apply`]
//! （自然键映射 + 守卫事务）、[`control`]（Local/Raft 门面）并接线控制写路由。
//! 3-voter/配对/快照/cutover 已在 S7 接线；[`RAFT_READY`]/[`SYNC_READY`]/
//! [`STRETCH_READY`] 均已翻（stretch 由 e2e 2×6 验证；能力位仍要求对应配置存在）。

// openraft 的 ConfigError/StorageError/RPCError 体积由第三方类型固定，调用方无法瘦身。
#![allow(clippy::result_large_err)]

pub(crate) mod apply;
pub mod command;
pub mod compat;
pub mod control;
pub mod cutover;
pub mod data;
pub mod identity;
pub mod membership;
pub mod metrics;
pub mod network;
pub mod repair;
pub mod routes;
pub mod runstate;
pub mod status;
pub mod store;

mod settings;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, Raft};
use serde::Deserialize;
use sqlx::SqlitePool;

pub use command::{
    ControlCommand, ControlResponse, LifecycleRuleCmd, LifecycleRuleCmdV2, PermissionBits,
    SizeError,
};
pub use settings::RaftSettings;

use store::SqliteStateMachine;

/// 控制写（console/S3 配置写）是否已可走 Raft。S6 出口已翻转（单节点）。
pub const RAFT_READY: bool = true;
/// sync 桶创建/读写是否可用。S3 数据面全绿且控制面接线后翻转（S6d）。
pub const SYNC_READY: bool = true;
/// stretch 2×6 数据面（S4）已实现；控制面（Raft 桶创建/身份配对）就绪后随 S7/S9 翻转。
pub const STRETCH_READY: bool = true;

// ---- 跨版本 wire 契约（Level B 滚动升级）：以下头名/限制一旦发布必须永久保持 ----
// v1.13.1 是 rolling 基线（它发送 target 头）；wire/feature 握手见 [`compat`]。
/// Raft RPC 集群令牌头（`/api/v1/site/raft/*`；常量时间校验见 [`network::token_matches`]）。
pub const RPC_TOKEN_HEADER: &str = "x-oss-raft-token";
/// 目标节点 id：防止同一 endpoint 上换了 node_id 的新进程误答旧成员的 Raft RPC
/// （openraft 会把异日志响应当成 follower log reversion 并 panic）。永久保留发送与校验。
pub const RPC_TARGET_HEADER: &str = "x-oss-raft-target-node-id";
/// append/vote/snapshot 路由体上限：4 MiB 原始分块的 serde JSON 约 16 MiB。
pub const RPC_BODY_LIMIT: usize = 32 * 1024 * 1024;
/// 单条控制命令上限（16 MiB 模块 base64 后约 21.4 MiB）。
pub const COMMAND_MAX_BYTES: usize = 24 * 1024 * 1024;
/// 快照原始分块上限（JSON 放大约 4 倍，故与 32 MiB 路由配套）。
pub const SNAPSHOT_CHUNK_MAX: u64 = 4 * 1024 * 1024;
/// 快照总上限硬顶。
pub const SNAPSHOT_MAX_BYTES_CAP: u64 = 1024 * 1024 * 1024;

openraft::declare_raft_types!(
    /// M9 控制面 Raft 类型：u64 节点 id、BasicNode 地址、`Cursor<Vec<u8>>` 快照。
    pub TypeConfig:
        D = ControlCommand,
        R = ControlResponse,
);

/// Raft 写入口错误：超时（可能已提交）、运行时错误、需要转发到 leader、无 quorum。
#[derive(Debug)]
pub enum WriteError {
    Timeout(Duration),
    Raft(String),
    /// 本节点不是 leader 且没有可转发的地址（路由 → 409/503，绝不二次转发）。
    ForwardToLeader {
        leader: Option<u64>,
        endpoint: Option<String>,
    },
    /// 无法联系 leader / 无 quorum（写可能已提交，重试幂等）。
    NoQuorum,
    /// R4：命令要求的 feature 级别高于 leader 当前生效级别（混合版本/配置不足）。
    FeatureUnavailable {
        required: u8,
        effective: u8,
    },
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout(d) => write!(f, "raft client_write timed out after {d:?}"),
            Self::Raft(m) => write!(f, "raft client_write: {m}"),
            Self::ForwardToLeader { leader, endpoint } => {
                write!(f, "not leader (leader={leader:?}, endpoint={endpoint:?})")
            }
            Self::NoQuorum => write!(f, "no leader/quorum"),
            Self::FeatureUnavailable {
                required,
                effective,
            } => write!(
                f,
                "raft feature level {required} not active (effective {effective})"
            ),
        }
    }
}

impl std::error::Error for WriteError {}

/// 基座错误：调用方（S6/S7）负责映射为 HTTP 状态；这里只保证底层错误可上报。
pub type RaftResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

/// M8-1：fencing token = `(term, leader)`。Raft 同一 term 至多一个 leader 且 term 单调
/// 递增，所以 term 是比较用的安全单位，leader 只为诊断/状态可读。写入方必须持有当前
/// token 才能落单例运行态（见 [`LeaderGate::epoch`]）；数据库按 term 单调拒绝旧 token。
/// [`FencingEpoch::UNFENCED`] 只用于 M7/单节点（恒有效），落库为 term 0，任何后续 Raft
/// term 都能接管。
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct FencingEpoch {
    pub term: u64,
    pub leader: u64,
}

impl FencingEpoch {
    /// M7/单节点：没有 Raft term，任何写入都有效。
    pub const UNFENCED: Self = Self { term: 0, leader: 0 };

    /// 落库用（SQLite INTEGER 有符号；Raft term 实际远低于 i64::MAX）。
    pub fn term_i64(self) -> i64 {
        i64::try_from(self.term).unwrap_or(i64::MAX)
    }

    pub fn leader_i64(self) -> i64 {
        i64::try_from(self.leader).unwrap_or(i64::MAX)
    }
}

/// M8-1：leadership 判定来源。`None` = 无法确定（未选出 leader / 节点已停止），
/// 调用方必须按非 leader 处理（fail closed）。一次快照同时给出 term 与 leader，
/// 避免 `is_leader` 与取 epoch 之间的 TOCTOU。
pub trait LeadershipSource: Send + Sync {
    fn epoch(&self) -> Option<FencingEpoch>;
}

impl LeadershipSource for RaftHandle {
    fn epoch(&self) -> Option<FencingEpoch> {
        self.fencing_epoch()
    }
}

/// M8-1：单例后台任务（作业/清单/tier/生命周期调度）的 leader 门控。
///
/// 无 `[raft]`（M7/standalone）→ [`LeaderGate::always`]，行为与门控前一致。Raft 模式下
/// 每轮取一次当前 leader：只有 `current_leader == 本节点` 才放行；无法确定 → 拒绝
/// （fail closed）。放行 token（[`FencingEpoch`] = term + leader）必须随状态转换落库，
/// 旧 token 的写入按 term 单调被拒（no-op），交接中在途动作的收尾不能覆盖新 leader 的状态。
///
/// 不引入新的共识或租约，也不复制运行态——`job_tasks`/`inventory_*`/`tier_restores` 等
/// 仍是各节点本地表（跨节点持久化见 backlog §4.3 M8-2 / OSSCTL2；本切片**不做**跨节点
/// 复制，因此 leader 本地库丢失仍会丢在途状态）；本门控只防多节点重复执行，接管依赖
/// 各节点已有的 stale/expired 领取回收（at-least-once）。
#[derive(Clone)]
pub struct LeaderGate {
    source: Arc<dyn LeadershipSource>,
}

impl LeaderGate {
    /// M7/无 Raft：单节点即 leader-equivalent（保持既有调度行为）。
    pub fn always() -> Self {
        Self {
            source: Arc::new(AlwaysLeader),
        }
    }

    /// `None` = 无 `[raft]` → always；`Some` = 跟随该节点的 Raft leader 状态。
    pub fn from_raft(raft: Option<Arc<RaftHandle>>) -> Self {
        match raft {
            Some(handle) => Self { source: handle },
            None => Self::always(),
        }
    }

    /// 固定值来源（测试/嵌入）：`Some(true)` = leader（有效 epoch 为 `UNFENCED`），
    /// `Some(false)`/`None` = 不执行。
    pub fn fixed(leader: Option<bool>) -> Self {
        Self::fixed_epoch(leader.filter(|v| *v).map(|_| FencingEpoch::UNFENCED))
    }

    /// 固定 epoch 来源（测试/嵌入）：`None` = 不执行；`Some` = 该 token 恒为当前。
    pub fn fixed_epoch(epoch: Option<FencingEpoch>) -> Self {
        struct Fixed(Option<FencingEpoch>);
        impl LeadershipSource for Fixed {
            fn epoch(&self) -> Option<FencingEpoch> {
                self.0
            }
        }
        Self {
            source: Arc::new(Fixed(epoch)),
        }
    }

    /// 自定义来源（测试：可切换 epoch 的模拟 Raft）。
    pub fn from_source(source: Arc<dyn LeadershipSource>) -> Self {
        Self { source }
    }

    /// 当前 fencing token；`None` = 无法确定/非 leader（调用方 fail closed）。
    pub fn epoch(&self) -> Option<FencingEpoch> {
        self.source.epoch()
    }

    /// 当前是否应执行单例工作。unknown → false（fail closed）。
    pub fn is_leader(&self) -> bool {
        self.epoch().is_some()
    }

    /// 本 worker 拿到的 token 是否仍是当前 token（交接后为 false）。
    pub fn still_current(&self, epoch: FencingEpoch) -> bool {
        self.epoch() == Some(epoch)
    }

    /// 一次性启动工作（对账）：等到本节点成为 leader 再执行，返回当时的 token；
    /// unknown 不会误放行。调用方写状态时必须带上该 token。
    pub async fn wait_until_leader(&self) -> FencingEpoch {
        loop {
            if let Some(epoch) = self.epoch() {
                return epoch;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

struct AlwaysLeader;

impl LeadershipSource for AlwaysLeader {
    fn epoch(&self) -> Option<FencingEpoch> {
        Some(FencingEpoch::UNFENCED)
    }
}

/// 已启动的 Raft 节点。字段私有，集成方通过访问器取 `Raft`/状态机。
pub struct RaftHandle {
    raft: Raft<TypeConfig>,
    settings: RaftSettings,
    state_machine: SqliteStateMachine,
    /// 与状态机共享的观测计数（apply 错误由状态机累加）。
    counters: Arc<metrics::Metrics>,
    /// peer TLS 物料（None = 明文；Some = Raft RPC/转发/成员请求全走 https）。
    tls: Option<Arc<crate::tls::Tls>>,
    /// 本节点看到的 peer wire/feature（带 term）；只有 leader 的用于门控。
    peers: Arc<compat::PeerVersions>,
}

impl RaftHandle {
    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    pub fn node_id(&self) -> u64 {
        self.settings.node_id
    }

    pub fn tls(&self) -> Option<&Arc<crate::tls::Tls>> {
        self.tls.as_ref()
    }

    pub fn settings(&self) -> &RaftSettings {
        &self.settings
    }

    pub fn state_machine(&self) -> &SqliteStateMachine {
        &self.state_machine
    }

    pub fn counters(&self) -> &Arc<metrics::Metrics> {
        &self.counters
    }

    pub fn peers(&self) -> &Arc<compat::PeerVersions> {
        &self.peers
    }

    /// 对外宣告/愿意接受的 feature：`min(configured, MAX)`（D3：意愿而非能力）。
    pub fn advertised_feature(&self) -> u8 {
        compat::advertised_feature(self.settings.feature_level)
    }

    /// 本节点已提交的激活级别（缺省 1；基座 KV 无标记）。
    pub fn activation_level(&self) -> u8 {
        self.state_machine.activation_level()
    }

    /// Level B 门控：生效级别 = min(本机配置, 当前 term 的 voter+learner feature)。
    /// follower 上（`only_if_leader`）不做本地拒绝：它没有当前 term 记录，交给 leader 判。
    fn check_feature(&self, cmd: &ControlCommand, only_if_leader: bool) -> Result<(), WriteError> {
        let required = cmd.min_feature_level();
        if required <= compat::MIN_FEATURE_LEVEL {
            return Ok(());
        }
        let (is_leader, effective) = {
            let m = self.raft.metrics();
            let m = m.borrow();
            if m.current_leader != Some(m.id) {
                (false, compat::MIN_FEATURE_LEVEL)
            } else {
                let voters: Vec<u64> = m.membership_config.voter_ids().collect();
                let learners: Vec<u64> = m.membership_config.membership().learner_ids().collect();
                (
                    true,
                    compat::effective_feature_level(
                        self.settings.feature_level,
                        m.id,
                        &voters,
                        &learners,
                        m.current_term,
                        &self.peers,
                    )
                    .level,
                )
            }
        };
        if only_if_leader && !is_leader {
            return Ok(());
        }
        if required > effective {
            return Err(WriteError::FeatureUnavailable {
                required,
                effective,
            });
        }
        Ok(())
    }

    /// 成员表是否已有 voter（`metrics().membership_config`；勿跨 `.await` 持借用）。
    pub fn membership_nonempty(&self) -> bool {
        let m = self.raft.metrics();
        let m = m.borrow();
        m.membership_config
            .membership()
            .voter_ids()
            .next()
            .is_some()
    }

    /// M8-1：当前 fencing token（term + 本节点 id）；非 leader/节点停止 = None。
    /// 只用于单例后台任务去重，**不是**线性一致读写依据。
    pub fn fencing_epoch(&self) -> Option<FencingEpoch> {
        let m = self.raft.metrics();
        let m = m.borrow();
        if m.running_state.is_err() || m.current_leader != Some(m.id) {
            return None;
        }
        Some(FencingEpoch {
            term: m.current_term,
            leader: m.id,
        })
    }

    /// M8-1：本节点当前是否 leader（选举中/无法确定 = false）。
    pub fn is_leader(&self) -> bool {
        self.fencing_epoch().is_some()
    }

    /// 当前 leader 的 advertise 地址（成员表查不到 = None；S7.1 follower 单跳转发用）。
    pub fn leader_endpoint(&self) -> Option<String> {
        let m = self.raft.metrics();
        let m = m.borrow();
        let leader = m.current_leader?;
        m.membership_config
            .membership()
            .get_node(&leader)
            .map(|n| n.addr.clone())
    }

    /// 本地写（不做 leader 转发）：超时返回错误（写可能已提交），成功后等到 apply
    /// 越过本条日志再返回（`/write` 的 applied_index 才是可等待的下界）。
    pub async fn write(&self, cmd: ControlCommand) -> Result<ControlResponse, WriteError> {
        // 转发入口：即使本节点不是 leader 也按 level-1 生效级别拒绝 level-2（fail closed）。
        self.check_feature(&cmd, false)?;
        let timeout = Duration::from_millis(self.settings.write_timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;
        match tokio::time::timeout_at(deadline, self.raft.client_write(cmd)).await {
            Err(_) => Err(WriteError::Timeout(timeout)),
            Ok(Ok(resp)) => {
                let index = resp.log_id.index;
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if !self.wait_applied(index, left).await {
                    return Err(WriteError::Timeout(timeout));
                }
                Ok(resp.data)
            }
            Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f)))) => {
                Err(WriteError::ForwardToLeader {
                    leader: f.leader_id,
                    endpoint: f.leader_node.map(|n| n.addr),
                })
            }
            Ok(Err(e)) => Err(WriteError::Raft(e.to_string())),
        }
    }

    /// 控制写（follower 可用）：本地提交；否则向 leader 的 `/api/v1/site/raft/write`
    /// 转交一次（`x-oss-raft-forwarded: 1`，最多一跳）。
    pub async fn client_write(&self, cmd: ControlCommand) -> Result<ControlResponse, WriteError> {
        // leader 本地先判；follower 交给 leader（转发后同样会被 leader 门控）。
        self.check_feature(&cmd, true)?;
        let timeout = Duration::from_millis(self.settings.write_timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;
        // 命令可能很大（≤24 MiB）：ForwardToLeader 不带数据，转发要留一份副本。
        let forward_copy = cmd.clone();
        match tokio::time::timeout_at(deadline, self.raft.client_write(cmd)).await {
            Err(_) => Err(WriteError::Timeout(timeout)),
            Ok(Ok(resp)) => {
                let index = resp.log_id.index;
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if !self.wait_applied(index, left).await {
                    return Err(WriteError::Timeout(timeout));
                }
                Ok(resp.data)
            }
            Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f)))) => {
                let (leader, endpoint) = (f.leader_id, f.leader_node.map(|n| n.addr));
                let Some(endpoint) = endpoint else {
                    return Err(WriteError::ForwardToLeader { leader, endpoint });
                };
                self.counters.forward_total.fetch_add(1, Ordering::Relaxed);
                self.forward_write(&endpoint, leader, forward_copy, timeout)
                    .await
            }
            Ok(Err(e)) => Err(WriteError::Raft(e.to_string())),
        }
    }

    /// 向 leader 转交一次控制写；leader 侧再 409（换届）只返回错误，不再二次转发。
    async fn forward_write(
        &self,
        endpoint: &str,
        leader: Option<u64>,
        cmd: ControlCommand,
        timeout: Duration,
    ) -> Result<ControlResponse, WriteError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let (client, endpoint) = match &self.tls {
            Some(tls) => (
                tls.reqwest_client().map_err(WriteError::Raft)?,
                tls.peer_url(endpoint).map_err(WriteError::Raft)?,
            ),
            None => (
                reqwest::Client::new(),
                endpoint.trim_end_matches('/').to_string(),
            ),
        };
        let mut req = client
            .post(format!("{endpoint}/api/v1/site/raft/write"))
            .header(RPC_TOKEN_HEADER, &self.settings.cluster_token)
            .header(routes::FORWARD_HEADER, "1")
            .header(compat::WIRE_HEADER, compat::WIRE_VERSION.to_string())
            .header(
                compat::FEATURE_HEADER,
                self.advertised_feature().to_string(),
            )
            .timeout(timeout);
        if let Some(leader) = leader {
            req = req.header(RPC_TARGET_HEADER, leader.to_string());
        }
        let sent = req.json(&cmd).send().await;
        let resp = match sent {
            Ok(resp) => resp,
            // leader 地址不可达 = 本节点无法完成提交（可能已提交；重试幂等）。
            Err(_) => return Err(WriteError::NoQuorum),
        };
        let status = resp.status();
        let body = resp.bytes().await.unwrap_or_default();
        match status.as_u16() {
            200 => {
                #[derive(Deserialize)]
                struct WriteReply {
                    response: ControlResponse,
                    applied_index: u64,
                }
                let reply: WriteReply = serde_json::from_slice(&body)
                    .map_err(|e| WriteError::Raft(format!("leader write reply: {e}")))?;
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if !self.wait_applied(reply.applied_index, left).await {
                    return Err(WriteError::Timeout(timeout));
                }
                Ok(reply.response)
            }
            409 => {
                #[derive(Deserialize)]
                struct Hint {
                    #[serde(default)]
                    leader: Option<u64>,
                    #[serde(default)]
                    leader_endpoint: Option<String>,
                }
                let hint: Hint = serde_json::from_slice(&body).unwrap_or(Hint {
                    leader,
                    leader_endpoint: Some(endpoint.to_string()),
                });
                Err(WriteError::ForwardToLeader {
                    leader: hint.leader,
                    endpoint: hint.leader_endpoint,
                })
            }
            503 => {
                #[derive(Deserialize)]
                struct FeatureErr {
                    #[serde(default)]
                    error: String,
                    #[serde(default)]
                    required: u8,
                    #[serde(default)]
                    effective: u8,
                }
                if let Ok(f) = serde_json::from_slice::<FeatureErr>(&body)
                    && f.error == "feature_unavailable"
                {
                    return Err(WriteError::FeatureUnavailable {
                        required: f.required,
                        effective: f.effective,
                    });
                }
                Err(WriteError::NoQuorum)
            }
            other => Err(WriteError::Raft(format!(
                "leader write HTTP {other}: {}",
                String::from_utf8_lossy(&body)
                    .chars()
                    .take(300)
                    .collect::<String>()
            ))),
        }
    }

    /// 等待本节点 apply 到 `index`（follower 转发后的有界读点，S6 使用）。
    pub async fn wait_applied(&self, index: u64, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let applied = {
                let m = self.raft.metrics();
                let m = m.borrow();
                m.last_applied.as_ref().map(|l| l.index).unwrap_or(0)
            };
            if applied >= index {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// S6 的 `GET /api/v1/site/raft/status` 数据源。
    pub fn status_json(&self, configured: bool) -> serde_json::Value {
        status::status_json(self, configured)
    }

    pub async fn shutdown(&self) {
        if let Err(e) = self.raft.shutdown().await {
            tracing::warn!("raft shutdown: {e}");
        }
    }
}

/// S5 基座入口：打开（或恢复）`raft.db`，启动 Raft，bootstrap=true 且无成员时单节点 initialize。
///
/// `bootstrap=false` 且尚无成员时只启动运行时并等待 leader/join（S7 接线），不报错。
pub async fn spawn(settings: RaftSettings) -> RaftResult<RaftHandle> {
    spawn_with_control(settings, None, None).await
}

/// S6 生产入口：状态机指向主控制库连接池（apply 与 `raft_applied`/ctx 同库同事务）。
/// `None` = 基座自测：状态机与日志共用 `raft.db`。
/// `install_notify`：控制面快照安装提交后收到 applied index（S7.2 缓存/目录对账）。
pub async fn spawn_with_control(
    settings: RaftSettings,
    control_pool: Option<SqlitePool>,
    install_notify: Option<tokio::sync::watch::Sender<u64>>,
) -> RaftResult<RaftHandle> {
    spawn_with_control_tls(settings, control_pool, install_notify, None).await
}

/// 同 [`spawn_with_control`]；`tls = Some` 时 Raft RPC 客户端走 https。
pub async fn spawn_with_control_tls(
    settings: RaftSettings,
    control_pool: Option<SqlitePool>,
    install_notify: Option<tokio::sync::watch::Sender<u64>>,
    tls: Option<Arc<crate::tls::Tls>>,
) -> RaftResult<RaftHandle> {
    let config = Arc::new(settings.openraft_config()?);
    let log_store = store::SqliteLogStore::open(&settings.db_path).await?;
    // 未 apply 的成员日志（崩溃窗口）也要阻止再次 initialize；openraft 启动时会重放。
    let has_logs = log_store.has_logs().await?;
    let witness = matches!(settings.role, crate::config::RaftRole::Witness);
    // R8：控制库启动守卫（单点：生产/测试/witness 都经这里）。marker 高于本机配置或
    // 二进制最高 → 拒绝启动，绝不以降低的级别继续（已激活集群的回滚必须换回二进制）。
    if let Some(pool) = &control_pool {
        let marker = compat::read_activation(pool)
            .await
            .map_err(std::io::Error::other)?;
        compat::guard_startup(marker, settings.feature_level).map_err(std::io::Error::other)?;
    }
    let state_machine = match control_pool {
        Some(pool) => {
            SqliteStateMachine::open_with_pool(
                pool,
                settings.snapshot_max_bytes,
                witness,
                install_notify,
                settings.feature_level,
            )
            .await?
        }
        None => SqliteStateMachine::open(&settings.db_path, settings.snapshot_max_bytes).await?,
    };

    let counters = Arc::clone(state_machine.counters());
    let peers = Arc::new(compat::PeerVersions::default());
    let network = match &tls {
        Some(t) => network::HttpNetworkFactory::new_tls(
            settings.cluster_token.clone(),
            Arc::clone(t),
            t.reqwest_client()?,
            Arc::clone(&peers),
            compat::advertised_feature(settings.feature_level),
        ),
        None => network::HttpNetworkFactory::new(
            settings.cluster_token.clone(),
            Arc::clone(&peers),
            compat::advertised_feature(settings.feature_level),
        ),
    };
    let raft = Raft::new(
        settings.node_id,
        config,
        network,
        log_store,
        state_machine.clone(),
    )
    .await?;
    // leader 变更计数：watch 只保留最新值（合并的中间跳变不逐一计数）。
    let mut watch = raft.metrics();
    let mut last_leader = watch.borrow_and_update().current_leader;
    let watch_counters = Arc::clone(&counters);
    tokio::spawn(async move {
        while watch.changed().await.is_ok() {
            let leader = watch.borrow_and_update().current_leader;
            if leader != last_leader {
                watch_counters
                    .leader_changes_total
                    .fetch_add(1, Ordering::Relaxed);
                last_leader = leader;
            }
        }
    });

    let handle = RaftHandle {
        raft,
        settings,
        state_machine,
        counters,
        tls,
        peers,
    };
    handle.bootstrap_if_needed(has_logs).await?;
    Ok(handle)
}

/// [`spawn`] 的别名（计划中 `init_single` 名称；集成方二选一）。
pub async fn init_single(settings: RaftSettings) -> RaftResult<RaftHandle> {
    spawn(settings).await
}

impl RaftHandle {
    async fn bootstrap_if_needed(&self, has_logs: bool) -> RaftResult<()> {
        let (_, membership) = self.state_machine.applied().await?;
        if membership.voter_ids().next().is_some() || has_logs {
            return Ok(());
        }
        if self.settings.bootstrap {
            let node = BasicNode::new(self.settings.advertise_endpoint.clone());
            self.raft
                .initialize(BTreeMap::from([(self.settings.node_id, node)]))
                .await?;
        } else {
            tracing::warn!(
                node_id = self.settings.node_id,
                "Raft 尚无成员且 bootstrap=false：等待 leader/join（S7）"
            );
        }
        Ok(())
    }
}

/// `GET /api/v1/raft/status` 的 S0 骨架（M7/standalone）。
/// `configured` = `cfg.raft.is_some()`；未配置时能力位全 false（与 `/api/v1/site` 同口径）。
pub fn status_json(configured: bool) -> serde_json::Value {
    serde_json::json!({
        "enabled": false,
        "capabilities": {
            "raft": configured && RAFT_READY,
            "sync": configured && SYNC_READY,
            "stretch": configured && STRETCH_READY,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use openraft::storage::{RaftLogStorage, RaftStateMachine};
    use openraft::{CommittedLeaderId, LogId, RaftLogReader};

    use super::store::{SqliteLogStore, SqliteStateMachine};
    use super::*;

    fn test_settings(dir: &Path) -> RaftSettings {
        RaftSettings {
            node_id: 1,
            role: crate::config::RaftRole::Voter,
            bootstrap: true,
            advertise_endpoint: "http://127.0.0.1:1".to_string(),
            cluster_token: "test-cluster-token".to_string(),
            db_path: dir.join("raft.db"),
            // 共享构建机负载高：放宽选举/写超时，避免测试因 CPU 抢占抖动。
            election_timeout_ms: 2_000,
            heartbeat_interval_ms: 200,
            install_snapshot_timeout_ms: 5_000,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1024,
            write_timeout_ms: 20_000,
            snapshot_max_chunk_size: SNAPSHOT_CHUNK_MAX,
            snapshot_max_bytes: 512 * 1024 * 1024,
            feature_level: 1,
        }
    }

    async fn wait_leader(handle: &RaftHandle) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let leader = {
                let m = handle.raft().metrics();
                let m = m.borrow();
                m.current_leader
            };
            if let Some(l) = leader {
                return l;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "leader election timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_snapshot(handle: &RaftHandle, min_index: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let snap = {
                let m = handle.raft().metrics();
                let m = m.borrow();
                m.snapshot.as_ref().map(|l| l.index)
            };
            // 快照可能覆盖采样 last_applied 之后又提交的 entry（如选举后的 blank log）。
            if snap.is_some_and(|i| i >= min_index) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "snapshot build timed out (snapshot={snap:?}, want >= {min_index})"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// R8：已激活 marker=2 的库用 feature_level=1 启动必须拒绝（错误里两个值都要在）。
    #[tokio::test]
    async fn startup_refuses_activation_above_config() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("control.db").display()
        );
        let db = crate::db::Db::connect_raft(&url).await.unwrap();
        sqlx::query("INSERT INTO schema_meta (key, value) VALUES ('raft_feature_level', '2')")
            .execute(db.pool())
            .await
            .unwrap();
        let mut settings = test_settings(dir.path());
        settings.feature_level = 1;
        let err = spawn_with_control(settings, Some(db.pool().clone()), None)
            .await
            .err()
            .expect("marker 2 > config 1 必须拒绝启动");
        let msg = err.to_string();
        assert!(msg.contains("raft_feature_level=2"), "{msg}");
        assert!(msg.contains("feature_level=1"), "{msg}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_write_restart_and_membership() {
        let dir = tempfile::tempdir().unwrap();
        let settings = test_settings(dir.path());

        let handle = spawn(settings.clone()).await.unwrap();
        assert_eq!(wait_leader(&handle).await, 1);
        assert_eq!(
            handle
                .write(ControlCommand::Set {
                    key: "k".into(),
                    value: "v1".into(),
                })
                .await
                .unwrap(),
            ControlResponse::Ok
        );

        let applied = {
            let m = handle.raft().metrics();
            let m = m.borrow();
            m.last_applied.unwrap()
        };
        assert!(
            handle
                .wait_applied(applied.index, Duration::from_secs(15))
                .await
        );
        assert_eq!(
            handle.state_machine().get("k").await.unwrap(),
            Some("v1".to_string())
        );
        let (_, membership) = handle.state_machine().applied().await.unwrap();
        assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![1]);

        handle.shutdown().await;
        drop(handle);

        // 重启：不重新 initialize，完整 LogId + StoredMembership 与 SM 数据保留。
        let handle = spawn(settings).await.unwrap();
        assert_eq!(wait_leader(&handle).await, 1);
        let (reopened_applied, membership) = handle.state_machine().applied().await.unwrap();
        assert!(reopened_applied.unwrap().index >= applied.index);
        assert_eq!(membership.voter_ids().collect::<Vec<_>>(), vec![1]);
        assert_eq!(
            handle.state_machine().get("k").await.unwrap(),
            Some("v1".to_string())
        );

        handle
            .write(ControlCommand::Delete { key: "k".into() })
            .await
            .unwrap();
        assert_eq!(handle.state_machine().get("k").await.unwrap(), None);
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_build_and_install_preserves_state() {
        let dir = tempfile::tempdir().unwrap();
        let handle = spawn(test_settings(dir.path())).await.unwrap();
        wait_leader(&handle).await;
        for i in 0..3 {
            handle
                .write(ControlCommand::Set {
                    key: format!("k{i}"),
                    value: format!("v{i}"),
                })
                .await
                .unwrap();
        }
        let applied = {
            let m = handle.raft().metrics();
            let m = m.borrow();
            m.last_applied.unwrap()
        };
        handle.raft().trigger().snapshot().await.unwrap();
        wait_snapshot(&handle, applied.index).await;

        let mut sm = handle.state_machine().clone();
        let snapshot = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
            .expect("snapshot must exist after trigger");
        assert!(snapshot.meta.last_log_id.unwrap().index >= applied.index);
        assert_eq!(
            snapshot
                .meta
                .last_membership
                .voter_ids()
                .collect::<Vec<_>>(),
            vec![1]
        );
        let bytes = snapshot.snapshot.get_ref().clone();
        assert!(!bytes.is_empty());

        // 安装到全新状态机：走 begin_receiving_snapshot 的 Cursor 接收路径。
        let dir2 = tempfile::tempdir().unwrap();
        let mut fresh = SqliteStateMachine::open(&dir2.path().join("raft.db"), 512 * 1024 * 1024)
            .await
            .unwrap();
        let mut cursor = fresh.begin_receiving_snapshot().await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut *cursor, &bytes)
            .await
            .unwrap();
        assert_eq!(fresh.applied().await.unwrap().0, None);
        RaftStateMachine::<TypeConfig>::install_snapshot(&mut fresh, &snapshot.meta, cursor)
            .await
            .unwrap();

        let (fresh_applied, fresh_membership) = fresh.applied().await.unwrap();
        assert_eq!(fresh_applied, snapshot.meta.last_log_id);
        assert_eq!(fresh_membership.voter_ids().collect::<Vec<_>>(), vec![1]);
        for i in 0..3 {
            assert_eq!(
                fresh.get(&format!("k{i}")).await.unwrap(),
                Some(format!("v{i}"))
            );
        }
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn log_store_append_read_truncate_purge_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let handle = spawn(test_settings(dir.path())).await.unwrap();
        wait_leader(&handle).await;
        for i in 0..3 {
            handle
                .write(ControlCommand::Set {
                    key: format!("k{i}"),
                    value: format!("v{i}"),
                })
                .await
                .unwrap();
        }
        handle.shutdown().await;
        drop(handle);

        let mut store = SqliteLogStore::open(&path).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        let last = state.last_log_id.expect("logs must exist");
        let entries = store.try_get_log_entries(..).await.unwrap();
        assert!(
            entries.len() >= 4,
            "membership + 3 writes, got {}",
            entries.len()
        );
        assert_eq!(entries.last().unwrap().log_id, last);

        // Minor 1：[start, stop) 语义——Excluded 起点必须跳过该条。
        let mid = store
            .try_get_log_entries((
                std::ops::Bound::Excluded(entries[0].log_id.index),
                std::ops::Bound::Included(entries[2].log_id.index),
            ))
            .await
            .unwrap();
        assert_eq!(
            mid.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![entries[1].log_id.index, entries[2].log_id.index]
        );

        // vote / committed 持久化（重开文件后仍在）。
        let vote = openraft::Vote::new_committed(9, 1);
        store.save_vote(&vote).await.unwrap();
        let committed = LogId::new(CommittedLeaderId::new(9, 1), last.index);
        store.save_committed(Some(committed)).await.unwrap();
        drop(store);

        let mut store = SqliteLogStore::open(&path).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        assert_eq!(store.read_committed().await.unwrap(), Some(committed));

        // truncate 从指定 LogId（含）起删除；purge 从起点删到指定 LogId（含）。
        let truncate_at = entries[1].log_id;
        store.truncate(truncate_at).await.unwrap();
        let after_truncate = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(after_truncate.last().unwrap().log_id, entries[0].log_id);

        store.purge(entries[0].log_id).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(entries[0].log_id));
        assert_eq!(state.last_log_id, Some(entries[0].log_id));
        assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
    }

    // ---- M8-1：leader 门控（确定性 mock 来源） ----

    /// 0 = unknown，1 = leader，2 = follower；leader 时 term 取原子值（模拟换届）。
    struct MockLeadership(
        std::sync::Arc<std::sync::atomic::AtomicU8>,
        std::sync::Arc<std::sync::atomic::AtomicU64>,
    );

    impl LeadershipSource for MockLeadership {
        fn epoch(&self) -> Option<FencingEpoch> {
            match self.0.load(std::sync::atomic::Ordering::SeqCst) {
                1 => Some(FencingEpoch {
                    term: self.1.load(std::sync::atomic::Ordering::SeqCst),
                    leader: 1,
                }),
                _ => None,
            }
        }
    }

    fn mock_gate(
        v: u8,
    ) -> (
        LeaderGate,
        std::sync::Arc<std::sync::atomic::AtomicU8>,
        std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(v));
        let term = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1));
        let source = std::sync::Arc::new(MockLeadership(
            std::sync::Arc::clone(&flag),
            std::sync::Arc::clone(&term),
        ));
        (LeaderGate::from_source(source), flag, term)
    }

    #[test]
    fn leader_gate_single_node_and_unknown() {
        // 单节点/M7 回归：无 Raft 恒为 leader-equivalent，token 恒为 UNFENCED。
        assert!(LeaderGate::always().is_leader());
        assert_eq!(LeaderGate::always().epoch(), Some(FencingEpoch::UNFENCED));
        assert!(LeaderGate::from_raft(None).is_leader());
        assert!(LeaderGate::fixed(Some(true)).is_leader());
        assert_eq!(
            LeaderGate::fixed(Some(true)).epoch(),
            Some(FencingEpoch::UNFENCED)
        );
        assert!(!LeaderGate::fixed(Some(false)).is_leader());
        assert_eq!(LeaderGate::fixed(Some(false)).epoch(), None);
        // 无法确定 → fail closed。
        assert!(!LeaderGate::fixed(None).is_leader());
    }

    #[test]
    fn leader_gate_follower_skips_and_handoff_flips() {
        let (gate, flag, _term) = mock_gate(2);
        assert!(!gate.is_leader(), "follower 不领取");
        flag.store(1, std::sync::atomic::Ordering::SeqCst);
        assert!(gate.is_leader(), "交接后新 leader 开始工作");
        flag.store(2, std::sync::atomic::Ordering::SeqCst);
        assert!(!gate.is_leader(), "旧 leader 停止领取");
        flag.store(0, std::sync::atomic::Ordering::SeqCst);
        assert!(!gate.is_leader(), "unknown fail closed");
    }

    /// M8-1：term 变化 → token 变化，旧 token 不再 current（在途收尾必须被拒）。
    #[test]
    fn fencing_epoch_changes_with_term() {
        let (gate, flag, term) = mock_gate(1);
        let old = gate.epoch().unwrap();
        assert!(gate.still_current(old));
        term.store(2, std::sync::atomic::Ordering::SeqCst);
        let new = gate.epoch().unwrap();
        assert_ne!(old, new);
        assert!(!gate.still_current(old), "旧 term 的 token 已失效");
        assert!(gate.still_current(new));
        flag.store(2, std::sync::atomic::Ordering::SeqCst);
        assert!(!gate.still_current(new), "失去 leadership 后 token 失效");
    }

    /// 真实 Raft 单节点：选出 leader 后 `is_leader`/`LeaderGate` 为真（M7 语义之外的
    /// 单节点 Raft 回归），停止后 fail closed。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raft_handle_leader_gate_single_node() {
        let dir = tempfile::tempdir().unwrap();
        let handle = Arc::new(spawn(test_settings(dir.path())).await.unwrap());
        assert_eq!(wait_leader(&handle).await, 1);
        assert!(handle.is_leader());
        let epoch = handle.fencing_epoch().expect("leader 必须有 token");
        assert_eq!(epoch.leader, 1);
        assert!(epoch.term >= 1, "单节点 Raft 首个 term 也是 1");
        let gate = LeaderGate::from_raft(Some(Arc::clone(&handle)));
        assert!(gate.is_leader());
        assert_eq!(gate.epoch(), Some(epoch));
        handle.shutdown().await;
        assert!(!handle.is_leader(), "停止后不得继续放行单例任务");
        assert_eq!(handle.fencing_epoch(), None);
        assert!(!gate.still_current(epoch), "停止后旧 token 失效");
    }

    #[tokio::test]
    async fn wait_until_leader_resumes_on_handoff() {
        let (gate, flag, _term) = mock_gate(2);
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait_until_leader().await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "follower 上不得提前放行");
        flag.store(1, std::sync::atomic::Ordering::SeqCst);
        let epoch = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epoch, FencingEpoch { term: 1, leader: 1 });
    }
}
