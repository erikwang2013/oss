//! M9 S1：sync 桶写提交门（store ↔ oss-server 契约）。
//!
//! 写顺序：staging 完成（不可见）→ `before_commit` 远端确认 → 本地 meta 翻转。
//! 门实现（站点远端 PUT）在 oss-server；store 只做「staging 后、Version 前」的调用与清理。
//! 这里的 `async_trait` 只用于 xl-store 既有 trait 风格，与 openraft 无关（S5 用原生 async fn）。

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use thiserror::Error;

use crate::meta::ObjectTag;

/// 一次 sync 写提交的输入。`plaintext` 是 staging 期间 tee 出的明文文件
/// （与对象大小无关的形态差异在这里被抹平：inline/EC/加密前都是同一份明文）。
#[derive(Debug)]
pub struct GateRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    /// 本地预生成的版本 UUID（= 版本目录名；双站同 id 的来源）。
    pub local_version_id: &'a str,
    pub plaintext: &'a Path,
    pub size: u64,
    pub etag: &'a str,
    pub content_type: Option<&'a str>,
    pub user_metadata: &'a BTreeMap<String, String>,
    pub tags: Option<&'a [ObjectTag]>,
    pub storage_class: Option<&'a str>,
    /// 客户端声明的校验和（远端按同值重算校验后落盘）。
    pub checksum: Option<&'a crate::meta::ChecksumRecord>,
}

/// 远端确认回执。`remote_version_id` 必须等于 `local_version_id`（双站同 id）。
#[derive(Debug, Clone, Default)]
pub struct GateTicket {
    pub remote_version_id: Option<String>,
    /// 补偿删除所需的定位信息（abort 用）。
    pub bucket: String,
    pub key: String,
    pub local_version_id: String,
}

#[derive(Debug, Error)]
pub enum GateError {
    /// 远端不可达/超时/远端 5xx/本站配置不满足 → 503。
    #[error("{0}")]
    Unavailable(String),
    /// 超过 `[sync].max_object_bytes` → 400。
    #[error("object exceeds sync max_object_bytes")]
    TooLarge,
    /// 门拒绝（远端 400 或本地能力边界）→ 400。
    #[error("{0}")]
    Rejected(String),
    /// 本地前置不满足（sync 桶版本化未 Enabled）或远端 409 → 409。
    #[error("{0}")]
    Conflict(String),
    /// 远端 ack 的版本 id 与预生成 id 不符 → 502。
    #[error("{0}")]
    VersionMismatch(String),
    /// 本地内部不变量破坏（tee 缺失/大小不符）→ 500，不诱导客户端重试。
    #[error("{0}")]
    Internal(String),
}

impl From<GateError> for crate::error::Error {
    fn from(e: GateError) -> Self {
        match e {
            GateError::Unavailable(m) => Self::Unavailable(m),
            GateError::TooLarge => {
                Self::InvalidArgument("object exceeds sync max_object_bytes".into())
            }
            GateError::Rejected(m) => Self::InvalidArgument(m),
            GateError::Conflict(m) => Self::Conflict(m),
            GateError::VersionMismatch(m) => Self::BadGateway(m),
            GateError::Internal(m) => Self::Io(std::io::Error::other(m)),
        }
    }
}

/// sync 写提交门。`is_sync` 每写一次（实现方负责缓存）；错误一律 fail-closed。
#[async_trait]
pub trait PutCommitGate: Send + Sync + std::fmt::Debug {
    /// 该桶是否 sync 模式；`deleting=1` 返回 `Unavailable`（删桶中不得写入）。
    async fn is_sync(&self, bucket: &str) -> Result<bool, GateError>;
    /// staging 后、本地 meta 翻转前调用；Ok = 远端已确认（可本地提交）。
    async fn before_commit(&self, req: GateRequest<'_>) -> Result<GateTicket, GateError>;
    /// 本地提交失败后的补偿（best-effort，不改变返回给客户端的错误）。
    async fn abort(&self, ticket: &GateTicket);
    /// 本地 meta 已成功提交：实现方清理写 intent（崩溃窗口的恢复凭据）。默认无操作。
    async fn committed(&self, _ticket: &GateTicket) {}
    /// 桶复制模式变更（建桶/删桶/apply）失效缓存。默认无操作。
    fn invalidate(&self, _bucket: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 内部不变量破坏归 Io（s3 层 → 500），不是可重试的 503。
    #[test]
    fn internal_error_maps_to_io() {
        let e: crate::error::Error = GateError::Internal("tee missing".into()).into();
        assert!(matches!(e, crate::error::Error::Io(_)));
    }
}
