//! 运行配置：`oss.toml` 反序列化。

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
    /// ponytail: M1 整份读进内存；M2 换流式后可删掉这个上限。
    #[serde(default = "default_put_limit")]
    pub max_put_bytes: usize,
    #[serde(default)]
    pub groups: Vec<GroupTarget>,
    /// M3 集群节 `[cluster]`；未配置 = 单节点（v1.4.0 行为）。
    #[serde(default)]
    pub cluster: Option<ClusterToml>,
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
}

/// 拓扑表中的一个节点。`addr` = 该节点 gRPC 监听地址（HTTP 端口另配）。
#[derive(Debug, Clone, Deserialize)]
pub struct NodeToml {
    pub id: String,
    pub addr: String,
    pub drive_count: u16,
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
        Ok(toml::from_str(&text)?)
    }

    pub fn bucket_for_group(&self, group: &str) -> Option<&str> {
        self.groups
            .iter()
            .find(|g| g.group == group)
            .map(|g| g.bucket.as_str())
    }
}
