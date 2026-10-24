//! 运行配置：`oss.toml` 反序列化。

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::tls_config::{TlsToml, normalize_peer_addr, parse_endpoint};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub listen: String,
    /// 单盘形态的数据目录（`data_dirs` 未配置时使用）。M2b-2 起多盘用 `data_dirs`。
    pub data_dir: PathBuf,
    /// 多盘形态的数据盘数组（M2b-2）；配置了它则忽略 `data_dir`。
    /// EC 8+4 需 ≥ 12 盘；单盘 = 无冗余（仍有 EC 形态，见 xl-store `open` 的盘数裁决）。
    #[serde(default)]
    pub data_dirs: Option<Vec<PathBuf>>,
    /// EC 配置节 `[ec]`：enabled（默认 true）/ data（8）/ parity（4）/ inline_threshold（128 KiB）。
    #[serde(default)]
    pub ec: EcToml,
    pub staging_dir: PathBuf,
    pub database_url: String,
    pub jwt_secret: String,
    /// 单个 S3 PutObject 允许的最大字节数（内存上限）。
    /// ponytail: 存储内核自 M4 切片 2 起分块流式，此值更多是**防护上限**（挡异常客户端
    /// 的超大单请求），不再是内存必要约束；要放宽时按业务调整。
    #[serde(default = "default_put_limit")]
    pub max_put_bytes: usize,
    #[serde(default)]
    pub groups: Vec<GroupTarget>,
    /// M3 集群节 `[cluster]`；未配置 = 单节点（v1.4.0 行为）。
    #[serde(default)]
    pub cluster: Option<ClusterToml>,
    /// M6 切片 1：Kafka 通知目标 `[[kafka.targets]]`（静态配置，同集群 secret 的配置文件信任）。
    #[serde(default)]
    pub kafka: KafkaToml,
    /// M6 切片 4：复制远端 `[[remotes]]`（Destination.Bucket 形如 `<id>/<bucket>`）。
    #[serde(default)]
    pub remotes: Vec<RemoteTarget>,
    /// M7：站点节 `[site]`——存在即启用多站点复制（单节点站点对；与 `[cluster]` 互斥）。
    #[serde(default)]
    pub site: Option<SiteToml>,
    /// M8 S4b：远端 tier `[[tiers]]`（S3 兼容端点；https 启动期拒绝）。
    #[serde(default)]
    pub tiers: Vec<TierTarget>,
    /// M9：`[raft]` 控制面（存在即 Raft 模式；要求 `[site]`；与 `[[groups]]` 互斥）。
    #[serde(default)]
    pub raft: Option<RaftToml>,
    /// M9：`[sync]` sync 桶数据面上限（缺省见 Default；无 `[raft]` 时不生效）。
    #[serde(default)]
    pub sync: SyncToml,
    /// M9：`[stretch]` 2×6 stretch EC 预设（要求 `[site]` + `[raft]`；与 `[cluster]` 互斥）。
    #[serde(default)]
    pub stretch: Option<StretchToml>,
    /// TLS：`[tls.server]` 存在 = 集群/站点/mesh/Raft 全链路启用；`[tls.client]` 补充出站信任。
    #[serde(default)]
    pub tls: TlsToml,
    /// STS：`[sts]`（AssumeRoleWithWebIdentity 的 OIDC 联合身份；缺省全关）。
    #[serde(default)]
    pub sts: xl_s3::sts_web_identity::StsToml,
}

/// `[raft]`：3-voter 控制面运行时参数。`advertise_endpoint` 是主 HTTP（Raft RPC 路由），
/// 与 `[stretch].self_addr` 的 mesh gRPC 端口必须分离。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftToml {
    /// Raft u64 节点 id（A=1,B=2,C=3）；0 非法。
    pub node_id: u64,
    #[serde(default)]
    pub role: RaftRole,
    #[serde(default)]
    pub bootstrap: bool,
    /// 主 HTTP 基址，如 `http://a:9000`（host:port 必填）。
    pub advertise_endpoint: String,
    pub cluster_token: String,
    #[serde(default)]
    pub db_path: Option<PathBuf>,
    /// witness 专用控制库（voter 出现即拒绝）；witness 必填。
    #[serde(default)]
    pub state_db: Option<PathBuf>,
    #[serde(default = "default_election_timeout_ms")]
    pub election_timeout_ms: u64,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_timeout_ms: u64,
    #[serde(default = "default_install_snapshot_timeout_ms")]
    pub install_snapshot_timeout_ms: u64,
    /// 默认 0（快照后不留 log）。
    #[serde(default)]
    pub max_in_snapshot_log_to_keep: u64,
    #[serde(default = "default_purge_batch_size")]
    pub purge_batch_size: u64,
    #[serde(default = "default_write_timeout_ms")]
    pub write_timeout_ms: u64,
    /// 原始字节；上限 4 MiB（JSON 约 16 MiB/块）。
    #[serde(default = "default_snapshot_max_chunk_size")]
    pub snapshot_max_chunk_size: u64,
    /// 快照总上限，默认 512 MiB，可配上限 1 GiB。
    #[serde(default = "default_snapshot_max_bytes")]
    pub snapshot_max_bytes: u64,
    /// 本节点愿意激活的最高协议级别：1 = 仅旧协议（默认，二进制升级不改变行为）；
    /// 2 = 允许 level-2 命令/快照（仍要等所有 voter/learner 宣告 2 才生效）。
    /// 回滚到旧二进制前必须删除此键（`RaftToml` deny_unknown_fields）。
    #[serde(default = "default_feature_level")]
    pub feature_level: u8,
}

impl RaftToml {
    /// serve/witness 共用校验：node_id、token、advertise、快照上限。
    pub fn validate_common(&self, peer_tls: bool) -> Result<(), String> {
        if self.node_id == 0 {
            return Err("[raft].node_id 必须 > 0".into());
        }
        if self.cluster_token.trim().is_empty() {
            return Err("[raft].cluster_token 不能为空".into());
        }
        let (_, https) =
            normalize_peer_addr(&self.advertise_endpoint, "[raft].advertise_endpoint")?;
        if https && !peer_tls {
            return Err("[raft].advertise_endpoint 为 https，但缺少 [tls.server]".into());
        }
        if self.snapshot_max_chunk_size == 0 || self.snapshot_max_chunk_size > 4 * 1024 * 1024 {
            return Err(format!(
                "[raft].snapshot_max_chunk_size 必须在 1..=4 MiB，实际 {}",
                self.snapshot_max_chunk_size
            ));
        }
        if self.snapshot_max_bytes == 0 || self.snapshot_max_bytes > 1024 * 1024 * 1024 {
            return Err(format!(
                "[raft].snapshot_max_bytes 必须在 1..=1 GiB，实际 {}",
                self.snapshot_max_bytes
            ));
        }
        if !(crate::raft::compat::MIN_FEATURE_LEVEL..=crate::raft::compat::MAX_FEATURE_LEVEL)
            .contains(&self.feature_level)
        {
            return Err(format!(
                "[raft].feature_level 必须在 {}..={}，实际 {}",
                crate::raft::compat::MIN_FEATURE_LEVEL,
                crate::raft::compat::MAX_FEATURE_LEVEL,
                self.feature_level
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RaftRole {
    #[default]
    Voter,
    Witness,
}

/// `[sync]`：sync 桶单对象上限与远端调用超时（仅 sync 调用；共享 ReplClient 不设默认超时）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncToml {
    #[serde(default = "default_sync_max_object_bytes")]
    pub max_object_bytes: u64,
    #[serde(default = "default_sync_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for SyncToml {
    fn default() -> Self {
        Self {
            max_object_bytes: default_sync_max_object_bytes(),
            timeout_ms: default_sync_timeout_ms(),
        }
    }
}

/// `[stretch]`：2 站点 × 6 盘、EC 8+4 预设。`node_id` 是数据面拓扑 id（非 Raft u64）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StretchToml {
    pub node_id: String,
    /// 本站 mesh gRPC 监听地址（`host:port` 或 `http://host:port`）。
    pub self_addr: String,
    pub peer_node_id: String,
    pub peer_addr: String,
    #[serde(default = "default_peer_drives")]
    pub peer_drives: u16,
    #[serde(default = "default_stretch_placement")]
    pub placement: String,
    pub mesh_secret: String,
    #[serde(default = "default_mesh_timeout_ms")]
    pub mesh_timeout_ms: u64,
}

/// witness 配置：`[raft]` 表 + role="witness"（独立子命令；无 `[site]`/数据目录）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WitnessToml {
    pub raft: RaftToml,
    /// witness 的 Raft RPC 监听 TLS（语义同 serve：server 节存在即 https）。
    #[serde(default)]
    pub tls: TlsToml,
}

impl WitnessToml {
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("读取配置文件 {} 失败: {e}", path.display()))?;
        let cfg: Self = toml::from_str(&text)?;
        cfg.validate().map_err(|e| format!("配置校验失败：{e}"))?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), String> {
        let r = &self.raft;
        if r.role != RaftRole::Witness {
            return Err("witness 配置的 [raft].role 必须是 \"witness\"".into());
        }
        if r.bootstrap {
            return Err("witness 配置的 [raft].bootstrap 必须为 false".into());
        }
        if r.state_db.is_none() {
            return Err("witness 配置缺少 [raft].state_db".into());
        }
        if r.db_path.is_none() {
            return Err("witness 配置缺少 [raft].db_path".into());
        }
        self.tls.validate()?;
        r.validate_common(self.tls.peer_tls())
    }
}

fn default_election_timeout_ms() -> u64 {
    5_000
}
fn default_heartbeat_interval_ms() -> u64 {
    1_000
}
fn default_rpc_timeout_ms() -> u64 {
    5_000
}
fn default_install_snapshot_timeout_ms() -> u64 {
    300_000
}
fn default_purge_batch_size() -> u64 {
    1_024
}
fn default_write_timeout_ms() -> u64 {
    10_000
}
fn default_snapshot_max_chunk_size() -> u64 {
    4 * 1024 * 1024
}
fn default_snapshot_max_bytes() -> u64 {
    512 * 1024 * 1024
}
fn default_feature_level() -> u8 {
    crate::raft::compat::MIN_FEATURE_LEVEL
}
fn default_sync_max_object_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_sync_timeout_ms() -> u64 {
    10_000
}
fn default_peer_drives() -> u16 {
    6
}
fn default_stretch_placement() -> String {
    "v2".to_string()
}
fn default_mesh_timeout_ms() -> u64 {
    5_000
}

/// 接受 `host:port` / `http://host:port` / `https://host:port`；拒绝 path/query/userinfo、
/// 空 host/port。返回小写 host 的 `host:port`（scheme 由 TLS 模式统一裁决）。
pub(crate) fn normalize_http_addr(raw: &str, field: &str) -> Result<String, String> {
    normalize_peer_addr(raw, field).map(|(host_port, _)| host_port)
}

/// `[listen]` 的 host:port 解析（端口 0 合法：测试/临时端口部署）。
fn listen_host_port(raw: &str) -> Option<(String, u16)> {
    let s = raw.trim();
    let rest = s
        .strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .unwrap_or(s);
    let (host, port) = rest.rsplit_once(':')?;
    if host.is_empty() {
        return None;
    }
    Some((host.to_ascii_lowercase(), port.parse::<u16>().ok()?))
}

/// tier 存储形态：`s3`（默认，旧行为）或本地冷盘池 `local`。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TierKind {
    #[default]
    S3,
    Local,
}

impl TierKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Local => "local",
        }
    }
}

/// `[[tiers]]`：归档远端或本地冷盘池。`s3` 用 endpoint/bucket/AK/SK；
/// `local` 只用 `path`（远端字段留空）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TierTarget {
    pub id: String,
    #[serde(default)]
    pub kind: TierKind,
    /// 本地冷盘池根目录（仅 `kind = "local"`）。
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub access_key: String,
    #[serde(default)]
    pub secret_key: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// TLS 证书主机名覆写（仅 https；缺省用 endpoint 的 host）。
    #[serde(default)]
    pub server_name: Option<String>,
}

/// `[site]`：本站点身份覆盖（缺省由首启种子生成）。`token` 为控制面通道共享密钥
/// （`X-OSS-Site-Token`；对端在 `POST /api/v1/sites` 中配置）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SiteToml {
    /// M9 S7.1：首启站点 id（缺省生成 UUID）；已有库不同 id 拒绝启动。
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub priority: i64,
    /// ≥32 位 hex（缺省随机生成并落库）
    #[serde(default)]
    pub token: Option<String>,
}

impl Default for SiteToml {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            region: default_region(),
            priority: 0,
            token: None,
        }
    }
}

/// `[[remotes]]`：复制出站目标端点与凭证（静态配置信任，同集群 secret 口径）。
/// **deny_unknown_fields**：拼错必须启动即拒（静默少一条 remote 会让规则校验失败）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteTarget {
    /// 规则 Destination 引用的 id（白名单 `[A-Za-z0-9._-]{1,64}`）
    pub id: String,
    /// 目标 S3 端点基址（http/https；https 由 TLS 客户端连接）
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// M8 缺口 C：出站复制带宽上限（字节/秒；None = 不限，行为与旧版完全一致）。
    #[serde(default)]
    pub max_bytes_per_sec: Option<u64>,
    /// TLS 证书主机名覆写（仅 https；缺省用 endpoint 的 host）。
    #[serde(default)]
    pub server_name: Option<String>,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// `[[kafka.targets]]`：桶通知 ARN `arn:oss:sqs::<id>:kafka` 的 id → 连接与主题映射。
/// **deny_unknown_fields**（P3-f 复查补）：`[[kafka.target]]` 这类拼错必须启动即拒，
/// 不得静默退回 Noop（事件全丢）。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaToml {
    #[serde(default)]
    pub targets: Vec<KafkaTarget>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaTarget {
    /// 与桶通知 ARN 中的 `<id>` 对应（白名单 `[A-Za-z0-9._-]{1,64}`，见 notify.rs）
    pub id: String,
    pub brokers: Vec<String>,
    pub topic: String,
    /// 启用 TLS（broker 证书须包含 broker 主机名；无 SNI 覆写）。
    #[serde(default)]
    pub tls: bool,
    /// SASL 认证（要求 tls=true；rskafka PLAIN/SCRAM）。
    #[serde(default)]
    pub sasl: Option<crate::tls_config::KafkaSaslToml>,
}

/// `[cluster]`：扁平静态拓扑（设计 §5.3——无 DC/Region/AZ，M3+ 同样适用）。
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterToml {
    /// 本节点 id（必须在 `nodes` 表内）。
    pub node_id: String,
    /// 集群内共享令牌（节点间 gRPC 的 metadata 校验；明文传输，内网假设）。
    pub secret: String,
    #[serde(default)]
    pub nodes: Vec<NodeToml>,
    /// M4 切片 5 放置开关：`"v2"` = 展开放置（扩容前全节点同步开启；混版会写失败可诊断）。
    #[serde(default)]
    pub placement: Option<String>,
}

/// 拓扑表中的一个节点。`addr` = 该节点 gRPC 监听地址（HTTP 端口另配）。
#[derive(Debug, Clone, Deserialize)]
pub struct NodeToml {
    pub id: String,
    pub addr: String,
    pub drive_count: u16,
    /// M4 切片 8：所属 set（缺省 0 = 单 set 部署；同物理节点可在多个 set 各出一条，
    /// 同 id 各条的 addr 必须一致——由 ClusterSets::new 校验）。
    #[serde(default)]
    pub set: Option<u16>,
}

/// `[ec]` 配置节（映射到 xl-store 的 `EcConfig` + `inline_threshold`）。
#[derive(Debug, Clone, Deserialize)]
pub struct EcToml {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_ec_data")]
    pub data: u8,
    #[serde(default = "default_ec_parity")]
    pub parity: u8,
    #[serde(default = "default_inline_threshold")]
    pub inline_threshold: u64,
}

impl Default for EcToml {
    fn default() -> Self {
        Self {
            enabled: true,
            data: default_ec_data(),
            parity: default_ec_parity(),
            inline_threshold: default_inline_threshold(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_ec_data() -> u8 {
    8
}
fn default_ec_parity() -> u8 {
    4
}
fn default_inline_threshold() -> u64 {
    // 与 xl-store meta::MAX_INLINE 一致（128 KiB）；超出会被 Store::open 拒绝
    128 * 1024
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupTarget {
    /// aetherupload 的分组名（**不能含下划线**）
    pub group: String,
    pub bucket: String,
    pub max_size: u64,
    pub extensions: Vec<String>,
}

fn default_put_limit() -> usize {
    256 * 1024 * 1024
}

impl AppConfig {
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("读取配置文件 {} 失败: {e}", path.display()))?;
        let cfg: Self = toml::from_str(&text)?;
        cfg.tls_validate()
            .map_err(|e| format!("配置校验失败：{e}"))?;
        cfg.kafka_validate()
            .map_err(|e| format!("配置校验失败：{e}"))?;
        cfg.remotes_validate()
            .map_err(|e| format!("配置校验失败：{e}"))?;
        cfg.tiers_validate()
            .map_err(|e| format!("配置校验失败：{e}"))?;
        cfg.site_validate()
            .map_err(|e| format!("配置校验失败：{e}"))?;
        cfg.raft_validate()
            .map_err(|e| format!("配置校验失败：{e}"))?;
        cfg.sts_validate()
            .map_err(|e| format!("配置校验失败：{e}"))?;
        Ok(cfg)
    }

    /// `[sts]`：启用 web_identity 时启动期读入并校验 JWKS（坏配置/坏文件 = 拒绝启动）。
    pub fn sts_validate(&self) -> Result<(), String> {
        self.sts.validate()
    }

    /// TLS 模式与监听地址 scheme 一致性；证书物料在 AppState::load 读取（witness 在 run 中）。
    pub fn tls_validate(&self) -> Result<(), String> {
        self.tls.validate()?;
        let listen = self.listen.trim();
        if listen.starts_with("https://") && !self.tls.peer_tls() {
            return Err("[listen] 为 https://，但缺少 [tls.server]".into());
        }
        if listen.starts_with("http://") && self.tls.peer_tls() {
            return Err(
                "[listen] 配置了 http:// 但已启用 [tls.server]；监听地址请写 host:port".into(),
            );
        }
        Ok(())
    }

    /// M6 切片 1（P3-f）：`[[kafka.targets]]` 校验——id 白名单/唯一、brokers/topic 非空。
    /// 启动即 fail-closed（错配置不静默进死信）。
    pub fn kafka_validate(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for t in &self.kafka.targets {
            if t.id.is_empty()
                || t.id.len() > 64
                || !t
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            {
                return Err(format!(
                    "[[kafka.targets]] id 非法：{:?}（白名单 [A-Za-z0-9._-]，≤64）",
                    t.id
                ));
            }
            if !seen.insert(t.id.clone()) {
                return Err(format!("[[kafka.targets]] id 重复：{}", t.id));
            }
            if t.brokers.is_empty() || t.brokers.iter().any(|b| b.trim().is_empty()) {
                return Err(format!("[[kafka.targets]] {} 的 brokers 不能为空", t.id));
            }
            if t.topic.trim().is_empty() {
                return Err(format!("[[kafka.targets]] {} 的 topic 不能为空", t.id));
            }
            if let Some(sasl) = &t.sasl {
                if !t.tls {
                    return Err(format!(
                        "[[kafka.targets]] {} 配置了 sasl 但 tls=false（SASL 必须走 TLS）",
                        t.id
                    ));
                }
                sasl.validate(&t.id)?;
            }
        }
        Ok(())
    }

    /// M6 切片 4：`[[remotes]]` 校验——id 白名单/唯一、endpoint 可解析（http + host）、
    /// AK/SK/region 非空。启动 fail-closed。
    pub fn remotes_validate(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for r in &self.remotes {
            if r.id.is_empty()
                || r.id.len() > 64
                || !r
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            {
                return Err(format!(
                    "[[remotes]] id 非法：{:?}（白名单 [A-Za-z0-9._-]，≤64）",
                    r.id
                ));
            }
            if !seen.insert(r.id.clone()) {
                return Err(format!("[[remotes]] id 重复：{}", r.id));
            }
            let ep = parse_endpoint(&r.endpoint, &format!("[[remotes]] {} 的 endpoint", r.id))?;
            if let Some(sn) = &r.server_name {
                if !ep.https {
                    return Err(format!(
                        "[[remotes]] {} 的 server_name 仅适用于 https 端点",
                        r.id
                    ));
                }
                crate::tls_config::validate_server_name(
                    sn,
                    &format!("[[remotes]] {} 的 server_name", r.id),
                )?;
            }
            if r.access_key.trim().is_empty() || r.secret_key.trim().is_empty() {
                return Err(format!(
                    "[[remotes]] {} 的 access_key/secret_key 不能为空",
                    r.id
                ));
            }
            if r.region.trim().is_empty() {
                return Err(format!("[[remotes]] {} 的 region 不能为空", r.id));
            }
            // 0 会除零、过小则单对象等待数小时——启动期拒
            if let Some(rate) = r.max_bytes_per_sec
                && rate < 1024
            {
                return Err(format!(
                    "[[remotes]] {} 的 max_bytes_per_sec 必须 ≥ 1024（1 KiB/s），实际 {rate}",
                    r.id
                ));
            }
        }
        Ok(())
    }

    /// M8 S4b：`[[tiers]]` 校验——id 白名单/唯一、endpoint http+host、bucket 非空、
    /// AK/SK/region 非空。https 启动期拒绝（TLS 后置）。
    pub fn tiers_validate(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for t in &self.tiers {
            if t.id.is_empty()
                || t.id.len() > 64
                || !t
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            {
                return Err(format!(
                    "[[tiers]] id 非法：{:?}（白名单 [A-Za-z0-9._-]，≤64）",
                    t.id
                ));
            }
            if !seen.insert(t.id.clone()) {
                return Err(format!("[[tiers]] id 重复：{}", t.id));
            }
            match t.kind {
                TierKind::Local => {
                    let Some(path) = t.path.as_ref() else {
                        return Err(format!("[[tiers]] {}（local）必须配置 path", t.id));
                    };
                    if path.as_os_str().is_empty() {
                        return Err(format!("[[tiers]] {} 的 path 不能为空", t.id));
                    }
                    if !t.endpoint.trim().is_empty()
                        || !t.bucket.trim().is_empty()
                        || !t.access_key.trim().is_empty()
                        || !t.secret_key.trim().is_empty()
                        || t.server_name.is_some()
                    {
                        return Err(format!(
                            "[[tiers]] {}（local）不得配置 endpoint/bucket/access_key/secret_key/server_name",
                            t.id
                        ));
                    }
                    if let Ok(md) = std::fs::metadata(path)
                        && !md.is_dir()
                    {
                        return Err(format!(
                            "[[tiers]] {} 的 path 不是目录：{}",
                            t.id,
                            path.display()
                        ));
                    }
                }
                TierKind::S3 => {
                    if t.path.is_some() {
                        return Err(format!(
                            "[[tiers]] {}（s3）不得配置 path（path 仅用于 local）",
                            t.id
                        ));
                    }
                    let ep =
                        parse_endpoint(&t.endpoint, &format!("[[tiers]] {} 的 endpoint", t.id))?;
                    if let Some(sn) = &t.server_name {
                        if !ep.https {
                            return Err(format!(
                                "[[tiers]] {} 的 server_name 仅适用于 https 端点",
                                t.id
                            ));
                        }
                        crate::tls_config::validate_server_name(
                            sn,
                            &format!("[[tiers]] {} 的 server_name", t.id),
                        )?;
                    }
                    if t.bucket.trim().is_empty() {
                        return Err(format!("[[tiers]] {} 的 bucket 不能为空", t.id));
                    }
                    if t.access_key.trim().is_empty() || t.secret_key.trim().is_empty() {
                        return Err(format!(
                            "[[tiers]] {} 的 access_key/secret_key 不能为空",
                            t.id
                        ));
                    }
                    if t.region.trim().is_empty() {
                        return Err(format!("[[tiers]] {} 的 region 不能为空", t.id));
                    }
                }
            }
        }
        Ok(())
    }

    /// 规则校验/归档解析用的 tier id 列表（顺序同配置）。
    pub fn tier_ids(&self) -> Vec<String> {
        self.tiers.iter().map(|t| t.id.clone()).collect()
    }

    /// M7：`[site]` 校验——name ≤64/非空、region 非空、token 32..=128 hex；与 `[cluster]` 互斥
    /// （M7 = 单节点站点对，集群站点归后续）。
    pub fn site_validate(&self) -> Result<(), String> {
        if let Some(s) = &self.site {
            if let Some(id) = &s.id
                && (id.is_empty()
                    || id.len() > 64
                    || !id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')))
            {
                return Err(format!(
                    "[site].id 非法：{id:?}（白名单 [A-Za-z0-9._-]，≤64）"
                ));
            }
            if let Some(n) = &s.name
                && (n.trim().is_empty() || n.len() > 64)
            {
                return Err(format!("[site].name 非法：{n:?}（非空且 ≤64）"));
            }
            if s.region.trim().is_empty() || s.region.len() > 64 {
                return Err(format!("[site].region 非法：{:?}", s.region));
            }
            if let Some(t) = &s.token
                && (t.len() < 32 || t.len() > 128 || !t.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                return Err("[site].token 非法：需 32–128 位十六进制".into());
            }
            if self.cluster.is_some() {
                return Err("[site] 与 [cluster] 互斥（M7 仅支持单节点站点对）".into());
            }
        }
        Ok(())
    }

    /// M9：`[raft]` 校验——必须 `[site]`、node_id>0、advertise 可解析、cluster_token 非空；
    /// 与 `[cluster]`/`[[groups]]` 互斥。`[sync]` 仅校验上限，无 `[raft]` 时不生效。
    pub fn raft_validate(&self) -> Result<(), String> {
        self.sync_validate()?;
        if let Some(r) = &self.raft {
            if r.role == RaftRole::Witness {
                return Err("witness 必须使用 witness 子命令（serve 不接受 role=witness）".into());
            }
            if r.state_db.is_some() {
                return Err("[raft].state_db 仅 witness 使用，voter 不得配置".into());
            }
            if self.site.is_none() {
                return Err("[raft] 需要 [site]（站点身份由 M7 [site] 提供）".into());
            }
            if self.cluster.is_some() {
                return Err("[raft] 与 [cluster] 互斥".into());
            }
            if !self.groups.is_empty() {
                return Err("[raft] 与 [[groups]] 互斥（guard 会拒绝分组桶写入）".into());
            }
            r.validate_common(self.tls.peer_tls())?;
        }
        self.stretch_validate()
    }

    pub fn sync_validate(&self) -> Result<(), String> {
        if self.sync.max_object_bytes == 0 {
            return Err("[sync].max_object_bytes 必须 > 0".into());
        }
        if self.sync.timeout_ms == 0 {
            return Err("[sync].timeout_ms 必须 > 0".into());
        }
        Ok(())
    }

    /// M9：`[stretch]` 校验——必须 `[site]`+`[raft]`，与 `[cluster]`/`[[groups]]` 互斥；
    /// 2×6、EC 8+4、inline=0、placement=v2、mesh_secret 非空；地址规范化后 self≠peer，
    /// 且主 HTTP（advertise）端口与 mesh gRPC（self_addr）分离。
    pub fn stretch_validate(&self) -> Result<(), String> {
        let Some(s) = &self.stretch else {
            return Ok(());
        };
        if self.site.is_none() {
            return Err("[stretch] 需要 [site]".into());
        }
        let Some(r) = &self.raft else {
            return Err("[stretch] 需要 [raft]".into());
        };
        if self.cluster.is_some() {
            return Err("[stretch] 与 [cluster] 互斥".into());
        }
        if !self.groups.is_empty() {
            return Err("[stretch] 与 [[groups]] 互斥".into());
        }
        if s.node_id.trim().is_empty() || s.peer_node_id.trim().is_empty() {
            return Err("[stretch].node_id/peer_node_id 不能为空".into());
        }
        if s.node_id == s.peer_node_id {
            return Err("[stretch].node_id 与 peer_node_id 不能相同".into());
        }
        let (self_addr, self_https) = normalize_peer_addr(&s.self_addr, "[stretch].self_addr")?;
        let (peer_addr, peer_https) = normalize_peer_addr(&s.peer_addr, "[stretch].peer_addr")?;
        let (advertise, adv_https) =
            normalize_peer_addr(&r.advertise_endpoint, "[raft].advertise_endpoint")?;
        if (self_https || peer_https || adv_https) && !self.tls.peer_tls() {
            return Err(
                "[stretch]/[raft] 地址为 https，但缺少 [tls.server]（拒绝明文降级）".into(),
            );
        }
        if self_addr == peer_addr {
            return Err("[stretch].self_addr 与 peer_addr 不能相同".into());
        }
        if advertise == self_addr {
            return Err(
                "[stretch].self_addr 不能与 [raft].advertise_endpoint 同端口（主 HTTP 与 mesh gRPC 必须分离）"
                    .into(),
            );
        }
        // M8：listen 实绑定与 mesh gRPC 同端口（同 host，或 0.0.0.0/:: 通配）必然冲突。
        if let Some((lhost, lport)) = listen_host_port(&self.listen)
            && let Some((shost, sport)) = listen_host_port(&self_addr)
            && lport == sport
            && (lhost == shost || matches!(lhost.as_str(), "0.0.0.0" | "::" | "[::]"))
        {
            return Err(
                "[listen] 不能与 [stretch].self_addr 同端口（主 HTTP 与 mesh gRPC 必须分离）"
                    .into(),
            );
        }
        if s.peer_drives != 6 {
            return Err(format!(
                "[stretch].peer_drives 必须为 6，实际 {}",
                s.peer_drives
            ));
        }
        if self.data_dirs.as_ref().is_none_or(|d| d.len() != 6) {
            return Err("[stretch] 本站点必须配置恰好 6 条 [[data_dirs]]".into());
        }
        if s.placement != "v2" {
            return Err(format!(
                "[stretch].placement 只支持 \"v2\"，实际 {:?}",
                s.placement
            ));
        }
        if s.mesh_secret.trim().is_empty() {
            return Err("[stretch].mesh_secret 不能为空".into());
        }
        if s.mesh_timeout_ms == 0 {
            return Err("[stretch].mesh_timeout_ms 必须 > 0".into());
        }
        if !self.ec.enabled || self.ec.data != 8 || self.ec.parity != 4 {
            return Err("[stretch] 要求 [ec] enabled=true、data=8、parity=4".into());
        }
        if self.ec.inline_threshold != 0 {
            return Err("[stretch] 要求 [ec].inline_threshold=0".into());
        }
        Ok(())
    }

    pub fn bucket_for_group(&self, group: &str) -> Option<&str> {
        self.groups
            .iter()
            .find(|g| g.group == group)
            .map(|g| g.bucket.as_str())
    }

    /// 密钥兜底解析（容器/无人值守部署；**显式填写优先，绝不覆盖**）：
    /// - 空 `jwt_secret` / 空集群 `secret` / 缺管理员口令 → 优先复用 `OSS_SECRETS_FILE`
    ///   已有值，缺失才生成（64 hex = 32 字节）；文件用 flock 串行化——
    ///   集群多节点并发首启共享同一份（后到者阻塞后重读，不会各生成一套）。
    /// - 未设文件：单机允许进程内生成（warn，重启即换）；**集群一律报错**——
    ///   各节点随机值 mesh 鉴权互不相认，属静默致命。
    /// - 管理员口令：`OSS_ADMIN_PASSWORD` 非空优先；空/缺**且配了文件**才生成（单机裸跑
    ///   行为不变：无口令就不引导管理员）。
    pub fn resolve_secrets(&mut self) -> Result<ResolvedSecrets, String> {
        let file = std::env::var("OSS_SECRETS_FILE")
            .ok()
            .filter(|p| !p.trim().is_empty());
        let admin_env = std::env::var("OSS_ADMIN_PASSWORD").ok();
        self.resolve_secrets_with(
            file.as_deref().map(std::path::Path::new),
            admin_env.as_deref(),
        )
    }

    /// 与 [`Self::resolve_secrets`] 同逻辑，但文件/环境显式传入（可单测，不碰进程环境）。
    pub(crate) fn resolve_secrets_with(
        &mut self,
        file: Option<&std::path::Path>,
        admin_env: Option<&str>,
    ) -> Result<ResolvedSecrets, String> {
        let need_jwt = self.jwt_secret.is_empty();
        let need_cluster = self.cluster.as_ref().is_some_and(|c| c.secret.is_empty());
        let need_admin = admin_env.map(|s| s.is_empty()).unwrap_or(true) && file.is_some();

        let mut out = ResolvedSecrets {
            admin_password: admin_env.filter(|s| !s.is_empty()).map(str::to_string),
            generated: false,
            secrets_file: file.map(std::path::Path::to_path_buf),
        };
        if !(need_jwt || need_cluster || need_admin) {
            return Ok(out);
        }
        if file.is_none() && (need_jwt || need_cluster) && self.cluster.is_some() {
            return Err(
                "集群的 jwt_secret/secret 未填写且未设 OSS_SECRETS_FILE——各节点必须共享同一份密钥（docker 栈已自动配置该文件）".into(),
            );
        }

        let mut have = std::collections::BTreeMap::<String, String>::new();
        let mut lock_file: Option<std::fs::File> = None;
        if let Some(f) = file {
            if let Some(dir) = f.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let fh = std::fs::File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false) // 只复用不截断：已有值要重读；覆盖写发生在取得锁之后
                .open(f)
                .map_err(|e| format!("打开密钥文件 {} 失败: {e}", f.display()))?;
            // 跨进程串行：并发首启时后到者在锁上阻塞，重读拿到先到者已写入的同一份
            fh.lock()
                .map_err(|e| format!("锁密钥文件 {} 失败: {e}", f.display()))?;
            let text = std::fs::read_to_string(f).unwrap_or_default();
            for line in text.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((k, v)) = line.split_once('=') {
                    let v = v.trim().trim_matches('"');
                    if !v.is_empty() {
                        have.insert(k.trim().to_string(), v.to_string());
                    }
                }
            }
            lock_file = Some(fh);
        }

        if need_jwt {
            self.jwt_secret = get_or_gen(&mut have, "JWT_SECRET", &mut out.generated);
        }
        if need_cluster && let Some(c) = self.cluster.as_mut() {
            c.secret = get_or_gen(&mut have, "CLUSTER_SECRET", &mut out.generated);
        }
        if need_admin {
            out.admin_password = Some(get_or_gen(&mut have, "ADMIN_PASSWORD", &mut out.generated));
        }

        match (file, out.generated) {
            (Some(f), true) => {
                std::fs::write(f, render_secrets(&have))
                    .map_err(|e| format!("写密钥文件 {} 失败: {e}", f.display()))?;
                #[cfg(unix)]
                {
                    // 0644：容器内 oss(10001) 落盘、宿主用户（UID 多半不同）需直读口令——
                    // 0600 会让「本地保存一份」变成只有 docker exec 才看得到。
                    // 文件在 gitignore 的宿主目录内，能碰它的用户本就握有 docker（root 等价）。
                    use std::os::unix::fs::PermissionsExt as _;
                    let _ = std::fs::set_permissions(f, std::fs::Permissions::from_mode(0o644));
                }
                drop(lock_file);
            }
            (Some(_), false) => drop(lock_file),
            (None, true) => tracing::warn!(
                "密钥未填且未设 OSS_SECRETS_FILE——本次进程内随机生成，重启即更换（生产请设置该环境变量）"
            ),
            (None, false) => {}
        }
        Ok(out)
    }
}

/// [`AppConfig::resolve_secrets`] 的返回值。
#[derive(Debug)]
pub struct ResolvedSecrets {
    /// 管理员引导口令（环境变量优先；配了密钥文件时可能是生成值）。None = 不引导。
    pub admin_password: Option<String>,
    /// 本次是否发生生成（调用方可据此提示「口令见文件」）。
    pub generated: bool,
    pub secrets_file: Option<std::path::PathBuf>,
}

/// 复用已有值；缺失则生成（64 hex = 32 字节，满足 jwt ≥32B 与集群令牌要求）。
fn get_or_gen(
    have: &mut std::collections::BTreeMap<String, String>,
    key: &str,
    generated: &mut bool,
) -> String {
    if let Some(v) = have.get(key) {
        return v.clone();
    }
    *generated = true;
    let v = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    have.insert(key.to_string(), v.clone());
    v
}

fn render_secrets(have: &std::collections::BTreeMap<String, String>) -> String {
    let mut s = String::from(
        "# oss 自动生成的密钥（首次启动生成并保存；删除本文件可整体轮换，集群各节点共享同一份）\n\
         # 想用自有值：直接改这里，或在 oss.toml / 环境变量里显式填写（显式值优先）\n",
    );
    for k in ["JWT_SECRET", "CLUSTER_SECRET", "ADMIN_PASSWORD"] {
        if let Some(v) = have.get(k) {
            s.push_str(k);
            s.push('=');
            s.push_str(v);
            s.push('\n');
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg() -> AppConfig {
        AppConfig {
            listen: "127.0.0.1:0".into(),
            data_dir: "d".into(),
            data_dirs: None,
            ec: Default::default(),
            staging_dir: "s".into(),
            database_url: "sqlite::memory:".into(),
            jwt_secret: String::new(),
            max_put_bytes: 1024,
            groups: vec![],
            kafka: KafkaToml::default(),
            cluster: None,
            remotes: vec![],
            site: None,
            tiers: vec![],
            raft: None,
            sync: Default::default(),
            stretch: None,
            tls: Default::default(),
            sts: Default::default(),
        }
    }

    #[test]
    fn sts_web_identity_default_off_and_bad_config_fails_closed() {
        let base = "listen = \"127.0.0.1:0\"\n\
                    data_dir = \"d\"\n\
                    staging_dir = \"s\"\n\
                    database_url = \"sqlite::memory:\"\n\
                    jwt_secret = \"\"\n";
        // 未配置 [sts]：缺省关闭（现有部署零改动，启动校验通过）
        let c: AppConfig = toml::from_str(base).unwrap();
        assert!(c.sts_validate().is_ok(), "缺省 [sts] 必须关闭且可启动");

        // 显式启用但 JWKS 文件不存在：启动期校验必须失败（fail-closed）
        let enabled = format!(
            "{base}\n[sts.web_identity]\n\
             enabled = true\n\
             issuer = \"https://issuer.example\"\n\
             audience = \"oss-test\"\n\
             jwks_file = \"/nonexistent/jwks.json\"\n\
             max_duration_seconds = 3600\n\
             [[sts.web_identity.roles]]\n\
             role_arn = \"arn:oss:iam::oss:role/ci-bot\"\n\
             claim = \"sub\"\n\
             values = [\"repo:acme/infra:ref:refs/heads/main\"]\n"
        );
        let c: AppConfig = toml::from_str(&enabled).unwrap();
        assert!(
            c.sts_validate().is_err(),
            "启用但 JWKS 不可读必须在启动期失败"
        );
        // 拼错的键也必须拒绝（deny_unknown_fields）
        let typo = enabled.replace("jwks_file", "jwks_url");
        assert!(
            toml::from_str::<AppConfig>(&typo).is_err(),
            "未知键必须拒绝"
        );
    }

    fn cluster_cfg() -> AppConfig {
        let mut c = base_cfg();
        c.cluster = Some(ClusterToml {
            node_id: "n1".into(),
            secret: String::new(),
            nodes: vec![],
            placement: None,
        });
        c
    }

    /// 显式填写优先：已有值一律不动，管理员口令走环境。
    #[test]
    fn resolve_keeps_explicit_values_untouched() {
        let mut c = base_cfg();
        c.jwt_secret = "explicit-32-bytes-secret-value-abcdef".into();
        let r = c.resolve_secrets_with(None, Some("pw")).unwrap();
        assert!(!r.generated);
        assert_eq!(c.jwt_secret, "explicit-32-bytes-secret-value-abcdef");
        assert_eq!(r.admin_password.as_deref(), Some("pw"));
    }

    /// 首启生成并落盘；二启（模拟重启/集群另一节点）复用同一份（flock 串行化的同进程面）。
    #[test]
    fn resolve_generates_persists_and_reuses() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("secrets.env");

        let mut c1 = cluster_cfg();
        let r1 = c1.resolve_secrets_with(Some(&f), None).unwrap();
        assert!(r1.generated);
        assert_eq!(c1.jwt_secret.len(), 64, "32 字节 hex");
        assert_eq!(c1.cluster.as_ref().unwrap().secret.len(), 64);
        assert!(f.exists(), "应落盘到本地");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&f).unwrap().permissions().mode() & 0o777,
                0o644,
                "宿主用户需可直读（权限=0644）"
            );
        }
        let jwt1 = c1.jwt_secret.clone();
        let cluster1 = c1.cluster.as_ref().unwrap().secret.clone();
        let admin1 = r1.admin_password.clone().unwrap();

        let mut c2 = cluster_cfg();
        let r2 = c2.resolve_secrets_with(Some(&f), None).unwrap();
        assert!(!r2.generated, "已有值不应触发再生成");
        assert_eq!(c2.jwt_secret, jwt1);
        assert_eq!(c2.cluster.as_ref().unwrap().secret, cluster1);
        assert_eq!(r2.admin_password.unwrap(), admin1);
    }

    /// 集群密钥缺失且未配文件 → 报错（各节点随机值会静默破坏 mesh 鉴权）。
    #[test]
    fn resolve_cluster_without_file_errors() {
        let mut c = cluster_cfg();
        let e = c.resolve_secrets_with(None, None).unwrap_err();
        assert!(e.contains("OSS_SECRETS_FILE"), "{e}");
    }

    /// 单机无文件：进程内生成 jwt（warn），但不生成管理员口令（保持裸跑不引导语义）。
    #[test]
    fn resolve_single_without_file_generates_in_memory_only() {
        let mut c = base_cfg();
        let r = c.resolve_secrets_with(None, None).unwrap();
        assert!(r.generated);
        assert_eq!(c.jwt_secret.len(), 64);
        assert!(
            r.admin_password.is_none(),
            "无文件时不生成管理员口令（裸跑无口令即不引导）"
        );
    }

    /// M6 切片 4：复制远端配置校验矩阵（启动 fail-closed）。
    #[test]
    fn remotes_validation_matrix() {
        let mk = |id: &str, endpoint: &str| RemoteTarget {
            id: id.into(),
            endpoint: endpoint.into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            region: "us-east-1".into(),
            max_bytes_per_sec: None,
            server_name: None,
        };
        let cfg = |remotes| {
            let mut c = base_cfg();
            c.remotes = remotes;
            c
        };
        assert!(
            cfg(vec![mk("r1", "http://127.0.0.1:9001")])
                .remotes_validate()
                .is_ok()
        );
        assert!(
            cfg(vec![mk("r1", "https://s3.example.com")])
                .remotes_validate()
                .is_ok()
        );
        let mut e = mk("r1", "http://h:1");
        e.server_name = Some("s3.example.com".into());
        assert!(
            cfg(vec![e]).remotes_validate().is_err(),
            "server_name 仅 https"
        );
        // 非法 id / 重复 / 坏 endpoint / 非 http(s) / 空凭证
        assert!(
            cfg(vec![mk("../x", "http://h:1")])
                .remotes_validate()
                .is_err()
        );
        assert!(
            cfg(vec![mk("r1", "http://h:1"), mk("r1", "http://h:2")])
                .remotes_validate()
                .is_err()
        );
        assert!(cfg(vec![mk("r1", "not a uri")]).remotes_validate().is_err());
        assert!(cfg(vec![mk("r1", "ftp://h:1")]).remotes_validate().is_err());
        let mut e = mk("r1", "http://h:1");
        e.secret_key = " ".into();
        assert!(cfg(vec![e]).remotes_validate().is_err());
        let mut e = mk("r1", "http://h:1");
        e.region = " ".into();
        assert!(cfg(vec![e]).remotes_validate().is_err());
        // M8 缺口 C：带宽上限——None 放行；0 与 <1 KiB/s 启动期拒
        let mut e = mk("r1", "http://h:1");
        e.max_bytes_per_sec = Some(1024);
        assert!(cfg(vec![e]).remotes_validate().is_ok());
        for bad in [0, 1, 1023] {
            let mut e = mk("r1", "http://h:1");
            e.max_bytes_per_sec = Some(bad);
            let err = cfg(vec![e]).remotes_validate().unwrap_err();
            assert!(err.contains("max_bytes_per_sec"), "{err}");
        }
    }

    /// M8 缺口 C：Toml 省略 `max_bytes_per_sec` = None（旧配置零改动）。
    #[test]
    fn remote_max_bytes_per_sec_serde_default() {
        let base = "id='r1'\nendpoint='http://h:1'\naccess_key='ak'\nsecret_key='sk'\n";
        let r: RemoteTarget = toml::from_str(base).unwrap();
        assert_eq!(r.max_bytes_per_sec, None);
        let with = format!("{base}max_bytes_per_sec = 2048\n");
        let r: RemoteTarget = toml::from_str(&with).unwrap();
        assert_eq!(r.max_bytes_per_sec, Some(2048));
    }

    /// M8 S4b：`[[tiers]]` 校验矩阵（https 启动期拒绝）。
    #[test]
    fn tiers_validation_matrix() {
        let mk = |id: &str, endpoint: &str| TierTarget {
            id: id.into(),
            kind: TierKind::S3,
            path: None,
            endpoint: endpoint.into(),
            bucket: "tb".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            region: "us-east-1".into(),
            server_name: None,
        };
        let cfg = |tiers| {
            let mut c = base_cfg();
            c.tiers = tiers;
            c
        };
        assert!(
            cfg(vec![mk("cold", "http://127.0.0.1:9001")])
                .tiers_validate()
                .is_ok()
        );
        assert!(
            cfg(vec![mk("cold", "https://s3.example.com")])
                .tiers_validate()
                .is_ok()
        );
        let mut e = mk("cold", "http://h:1");
        e.server_name = Some("s3.example.com".into());
        assert!(
            cfg(vec![e]).tiers_validate().is_err(),
            "server_name 仅 https"
        );
        assert!(
            cfg(vec![mk("cold", "http://h:1"), mk("cold", "http://h:2")])
                .tiers_validate()
                .is_err()
        );
        assert!(
            cfg(vec![mk("../x", "http://h:1")])
                .tiers_validate()
                .is_err()
        );
        let mut e = mk("cold", "http://h:1");
        e.bucket = " ".into();
        assert!(cfg(vec![e]).tiers_validate().is_err());

        // M8：本地冷盘池——只允许 path；远端字段必须为空；path 须是目录。
        let local = |path: Option<&str>| TierTarget {
            id: "local1".into(),
            kind: TierKind::Local,
            path: path.map(std::path::PathBuf::from),
            endpoint: String::new(),
            bucket: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            region: "us-east-1".into(),
            server_name: None,
        };
        let dir = tempfile::tempdir().unwrap();
        assert!(
            cfg(vec![local(Some(dir.path().to_str().unwrap()))])
                .tiers_validate()
                .is_ok()
        );
        assert!(cfg(vec![local(None)]).tiers_validate().is_err(), "缺 path");
        let mut e = local(Some(dir.path().to_str().unwrap()));
        e.endpoint = "http://h:1".into();
        assert!(
            cfg(vec![e]).tiers_validate().is_err(),
            "local 不得带远端字段"
        );
        let mut e = local(Some(dir.path().to_str().unwrap()));
        e.kind = TierKind::S3;
        assert!(cfg(vec![e]).tiers_validate().is_err(), "s3 不得带 path");
        let file = dir.path().join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        assert!(
            cfg(vec![local(Some(file.to_str().unwrap()))])
                .tiers_validate()
                .is_err(),
            "path 是文件"
        );
    }

    /// M7：`[site]` 校验矩阵（token 形态 / 与 cluster 互斥）。
    #[test]
    fn site_validation_matrix() {
        let mut c = base_cfg();
        let hex = "a".repeat(32);
        c.site = Some(SiteToml {
            id: Some("site-a_1".into()),
            name: Some("a".into()),
            region: "us-east-1".into(),
            priority: 10,
            token: Some(hex.clone()),
        });
        assert!(c.site_validate().is_ok());
        // 未给 token（缺省随机）与空 name 也合法
        c.site = Some(SiteToml::default());
        assert!(c.site_validate().is_ok());
        // 非法 token：短/非 hex
        let bad = |t: &str| {
            let mut c = base_cfg();
            c.site = Some(SiteToml {
                token: Some(t.into()),
                ..Default::default()
            });
            c.site_validate().is_err()
        };
        assert!(bad("short"));
        assert!(bad(&"z".repeat(32)));
        assert!(bad(&"a".repeat(129)));
        // S7.1：[site].id 白名单
        for (id, ok) in [
            ("abc.DEF_1-2", true),
            ("", false),
            ("a/b", false),
            ("x".repeat(65).as_str(), false),
        ] {
            let mut c = base_cfg();
            c.site = Some(SiteToml {
                id: Some(id.into()),
                ..Default::default()
            });
            assert_eq!(c.site_validate().is_ok(), ok, "[site].id={id:?}");
        }
        // name 过长 / region 空
        let mut c = base_cfg();
        c.site = Some(SiteToml {
            name: Some("x".repeat(65)),
            ..Default::default()
        });
        assert!(c.site_validate().is_err());
        let mut c = base_cfg();
        c.site = Some(SiteToml {
            region: " ".into(),
            ..Default::default()
        });
        assert!(c.site_validate().is_err());
        // cluster + site 互斥
        let mut c = cluster_cfg();
        c.site = Some(SiteToml::default());
        assert!(c.site_validate().is_err());
    }

    /// M6 切片 1（P3-f）：Kafka 目标配置校验矩阵（启动 fail-closed）。
    #[test]
    fn kafka_targets_validation_matrix() {
        let mk = |id: &str, brokers: Vec<&str>, topic: &str| KafkaTarget {
            id: id.into(),
            brokers: brokers.into_iter().map(str::to_string).collect(),
            topic: topic.into(),
            tls: false,
            sasl: None,
        };
        let cfg = |targets| AppConfig {
            listen: "127.0.0.1:0".into(),
            data_dir: std::path::PathBuf::from("/tmp/x"),
            data_dirs: None,
            ec: Default::default(),
            staging_dir: std::path::PathBuf::from("/tmp/x"),
            database_url: "sqlite::memory:".into(),
            jwt_secret: "k".repeat(32),
            max_put_bytes: 1,
            groups: vec![],
            cluster: None,
            kafka: KafkaToml { targets },
            remotes: vec![],
            site: None,
            tiers: vec![],
            raft: None,
            sync: Default::default(),
            stretch: None,
            tls: Default::default(),
            sts: Default::default(),
        };
        // 合法
        assert!(
            cfg(vec![mk("t1", vec!["127.0.0.1:9092"], "events")])
                .kafka_validate()
                .is_ok()
        );
        // 非法 id（路径穿越/超长）
        assert!(
            cfg(vec![mk("../x", vec!["b:1"], "t")])
                .kafka_validate()
                .is_err()
        );
        assert!(
            cfg(vec![mk("a/b", vec!["b:1"], "t")])
                .kafka_validate()
                .is_err()
        );
        assert!(
            cfg(vec![mk(&"a".repeat(65), vec!["b:1"], "t")])
                .kafka_validate()
                .is_err()
        );
        // 重复 id / 空 brokers / 空 broker 串 / 空 topic
        assert!(
            cfg(vec![mk("t1", vec!["b:1"], "x"), mk("t1", vec!["b:2"], "y")])
                .kafka_validate()
                .is_err()
        );
        assert!(cfg(vec![mk("t1", vec![], "x")]).kafka_validate().is_err());
        assert!(
            cfg(vec![mk("t1", vec![" "], "x")])
                .kafka_validate()
                .is_err()
        );
        assert!(
            cfg(vec![mk("t1", vec!["b:1"], " ")])
                .kafka_validate()
                .is_err()
        );
    }

    fn raft_toml() -> RaftToml {
        RaftToml {
            node_id: 1,
            role: RaftRole::Voter,
            bootstrap: true,
            advertise_endpoint: "http://a:9000".into(),
            cluster_token: "t".repeat(64),
            db_path: None,
            state_db: None,
            election_timeout_ms: default_election_timeout_ms(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            rpc_timeout_ms: default_rpc_timeout_ms(),
            install_snapshot_timeout_ms: default_install_snapshot_timeout_ms(),
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: default_purge_batch_size(),
            write_timeout_ms: default_write_timeout_ms(),
            snapshot_max_chunk_size: default_snapshot_max_chunk_size(),
            snapshot_max_bytes: default_snapshot_max_bytes(),
            feature_level: default_feature_level(),
        }
    }

    fn stretch_toml() -> StretchToml {
        StretchToml {
            node_id: "A".into(),
            self_addr: "a:9100".into(),
            peer_node_id: "B".into(),
            peer_addr: "http://b:9100".into(),
            peer_drives: 6,
            placement: "v2".into(),
            mesh_secret: "s".repeat(64),
            mesh_timeout_ms: default_mesh_timeout_ms(),
        }
    }

    fn m9_cfg() -> AppConfig {
        let mut c = base_cfg();
        c.site = Some(Default::default());
        c.data_dirs = Some((0..6).map(|i| PathBuf::from(format!("/d{i}"))).collect());
        c.ec = EcToml {
            enabled: true,
            data: 8,
            parity: 4,
            inline_threshold: 0,
        };
        c.raft = Some(raft_toml());
        c.stretch = Some(stretch_toml());
        c
    }

    #[test]
    fn m9_raft_runtime_defaults() {
        let r: RaftToml = toml::from_str(
            r#"
node_id = 1
advertise_endpoint = "http://a:9000"
cluster_token = "x"
"#,
        )
        .unwrap();
        assert_eq!(r.role, RaftRole::Voter);
        assert!(!r.bootstrap);
        assert_eq!(r.election_timeout_ms, 5_000);
        assert_eq!(r.heartbeat_interval_ms, 1_000);
        assert_eq!(r.rpc_timeout_ms, 5_000);
        assert_eq!(r.install_snapshot_timeout_ms, 300_000);
        assert_eq!(r.max_in_snapshot_log_to_keep, 0);
        assert_eq!(r.purge_batch_size, 1_024);
        assert_eq!(r.write_timeout_ms, 10_000);
        assert_eq!(r.snapshot_max_chunk_size, 4 * 1024 * 1024);
        assert_eq!(r.snapshot_max_bytes, 512 * 1024 * 1024);
        assert_eq!(r.feature_level, 1);
        let s = SyncToml::default();
        assert_eq!(s.max_object_bytes, 64 * 1024 * 1024);
        assert_eq!(s.timeout_ms, 10_000);
        let w: RaftToml = toml::from_str(
            r#"
node_id = 3
role = "witness"
advertise_endpoint = "http://w:9100"
cluster_token = "x"
"#,
        )
        .unwrap();
        assert_eq!(w.role, RaftRole::Witness);
    }

    /// S7.1：witness TOML（仅 `[raft]` 表）解析/校验；serve 拒绝 witness 与 voter state_db。
    #[test]
    fn m9_witness_toml_and_serve_role_guards() {
        let text = r#"
[raft]
node_id = 3
role = "witness"
advertise_endpoint = "http://w:9100"
cluster_token = "x"
db_path = "/x/raft.db"
state_db = "/x/state.db"
"#;
        let dir = std::env::temp_dir().join(format!("oss-cfg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("w.toml");
        std::fs::write(&path, text).unwrap();
        let w = WitnessToml::load(&path).unwrap();
        assert_eq!(w.raft.node_id, 3);
        assert_eq!(w.raft.role, RaftRole::Witness);
        assert!(w.raft.state_db.is_some());
        std::fs::remove_dir_all(&dir).unwrap();

        let mut bad = w.clone();
        bad.raft.bootstrap = true;
        assert!(bad.validate().is_err());
        let mut bad = w.clone();
        bad.raft.state_db = None;
        assert!(bad.validate().is_err());
        let mut bad = w;
        bad.raft.role = RaftRole::Voter;
        assert!(bad.validate().is_err());

        // serve：role=witness / voter+state_db 均拒绝
        let mut c = m9_cfg();
        c.raft.as_mut().unwrap().role = RaftRole::Witness;
        assert!(c.raft_validate().unwrap_err().contains("witness 子命令"));
        let mut c = m9_cfg();
        c.raft.as_mut().unwrap().state_db = Some(PathBuf::from("/x"));
        assert!(c.raft_validate().unwrap_err().contains("state_db"));
    }

    #[test]
    fn m9_raft_requires_site_and_rejects_cluster_and_groups() {
        let mut c = base_cfg();
        c.raft = Some(raft_toml());
        assert!(c.raft_validate().unwrap_err().contains("[site]"));

        let mut c = m9_cfg();
        c.site = None;
        assert!(c.raft_validate().is_err());

        let mut c = base_cfg();
        c.cluster = Some(ClusterToml {
            node_id: "n1".into(),
            secret: String::new(),
            nodes: vec![],
            placement: None,
        });
        c.site = Some(Default::default());
        c.raft = Some(raft_toml());
        assert!(c.raft_validate().unwrap_err().contains("[cluster]"));

        let mut c = m9_cfg();
        c.groups = vec![GroupTarget {
            group: "g".into(),
            bucket: "b".into(),
            max_size: 1,
            extensions: vec![],
        }];
        assert!(c.raft_validate().unwrap_err().contains("[[groups]]"));

        let mut c = m9_cfg();
        c.raft.as_mut().unwrap().node_id = 0;
        assert!(c.raft_validate().is_err());
        let mut c = m9_cfg();
        c.raft.as_mut().unwrap().cluster_token = "  ".into();
        assert!(c.raft_validate().is_err());
        for bad in ["https://a:9000", "http://a:9000/x", "a", "a:0", ":9000"] {
            let mut c = m9_cfg();
            c.raft.as_mut().unwrap().advertise_endpoint = bad.into();
            assert!(c.raft_validate().is_err(), "advertise 应拒绝 {bad:?}");
        }
        let mut c = m9_cfg();
        c.raft.as_mut().unwrap().snapshot_max_chunk_size = 4 * 1024 * 1024 + 1;
        assert!(c.raft_validate().is_err());
        let mut c = m9_cfg();
        c.raft.as_mut().unwrap().snapshot_max_bytes = 1024 * 1024 * 1024 + 1;
        assert!(c.raft_validate().is_err());
        for bad in [0u8, 3] {
            let mut c = m9_cfg();
            c.raft.as_mut().unwrap().feature_level = bad;
            assert!(c.raft_validate().is_err(), "feature_level={bad} 应拒绝");
        }
        let mut c = m9_cfg();
        c.raft.as_mut().unwrap().feature_level = 2;
        assert!(c.raft_validate().is_ok());
    }

    #[test]
    fn m9_stretch_requires_site_and_raft() {
        let mut c = base_cfg();
        c.stretch = Some(stretch_toml());
        assert!(c.stretch_validate().unwrap_err().contains("[site]"));

        let mut c = base_cfg();
        c.site = Some(Default::default());
        c.stretch = Some(stretch_toml());
        assert!(c.stretch_validate().unwrap_err().contains("[raft]"));

        let mut c = m9_cfg();
        c.cluster = Some(ClusterToml {
            node_id: "n1".into(),
            secret: String::new(),
            nodes: vec![],
            placement: None,
        });
        assert!(c.stretch_validate().unwrap_err().contains("[cluster]"));
        let mut c = m9_cfg();
        c.groups = vec![GroupTarget {
            group: "g".into(),
            bucket: "b".into(),
            max_size: 1,
            extensions: vec![],
        }];
        assert!(c.stretch_validate().unwrap_err().contains("[[groups]]"));
    }

    #[test]
    fn m9_stretch_self_addr_required_normalized_and_distinct() {
        // 缺 self_addr（serde 必填字段）
        assert!(
            toml::from_str::<StretchToml>(
                r#"
node_id = "A"
peer_node_id = "B"
peer_addr = "b:9100"
mesh_secret = "x"
"#,
            )
            .is_err()
        );
        let mut c = m9_cfg();
        c.stretch.as_mut().unwrap().self_addr = "https://a:9100".into();
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.stretch.as_mut().unwrap().peer_addr = "http://b:9100/path".into();
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.stretch.as_mut().unwrap().self_addr = "A:9100".into();
        c.stretch.as_mut().unwrap().peer_addr = "http://a:9100".into();
        assert!(c.stretch_validate().unwrap_err().contains("不能相同"));
        // 主 HTTP 与 mesh gRPC 同端口
        let mut c = m9_cfg();
        c.raft.as_mut().unwrap().advertise_endpoint = "http://a:9100".into();
        assert!(
            c.stretch_validate()
                .unwrap_err()
                .contains("advertise_endpoint")
        );
        // M8：listen 实绑定与 self_addr 同端口（同 host / 通配绑定）
        let mut c = m9_cfg();
        c.listen = "a:9100".into();
        assert!(c.stretch_validate().unwrap_err().contains("listen"));
        let mut c = m9_cfg();
        c.listen = "0.0.0.0:9100".into();
        assert!(c.stretch_validate().unwrap_err().contains("listen"));
        let mut c = m9_cfg();
        c.listen = "0.0.0.0:9000".into();
        assert!(c.stretch_validate().is_ok());
    }

    #[test]
    fn m9_stretch_topology_and_ec_validation() {
        let mut c = m9_cfg();
        c.stretch.as_mut().unwrap().peer_drives = 5;
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.stretch.as_mut().unwrap().placement = "v1".into();
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.stretch.as_mut().unwrap().mesh_secret = " ".into();
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.stretch.as_mut().unwrap().peer_node_id = "A".into();
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.data_dirs = Some(vec!["/d0".into()]);
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.ec.data = 6;
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.ec.inline_threshold = 1;
        assert!(c.stretch_validate().is_err());
        let mut c = m9_cfg();
        c.stretch.as_mut().unwrap().mesh_timeout_ms = 0;
        assert!(
            c.stretch_validate()
                .unwrap_err()
                .contains("mesh_timeout_ms")
        );
        assert!(m9_cfg().stretch_validate().is_ok());
    }

    #[test]
    fn m9_sync_validation() {
        assert!(base_cfg().raft_validate().is_ok(), "默认关闭必须通过");
        let mut c = base_cfg();
        c.sync.max_object_bytes = 0;
        assert!(c.raft_validate().is_err());
        let mut c = base_cfg();
        c.sync.timeout_ms = 0;
        assert!(c.raft_validate().is_err());
    }
}
