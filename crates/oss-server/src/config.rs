//! 运行配置：`oss.toml` 反序列化。

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub listen: String,
    pub data_dir: PathBuf,
    pub staging_dir: PathBuf,
    pub database_url: String,
    pub jwt_secret: String,
    /// 单个 S3 PutObject 允许的最大字节数（内存上限）。
    /// ponytail: M1 整份读进内存；M2 换流式后可删掉这个上限。
    #[serde(default = "default_put_limit")]
    pub max_put_bytes: usize,
    #[serde(default)]
    pub groups: Vec<GroupTarget>,
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
