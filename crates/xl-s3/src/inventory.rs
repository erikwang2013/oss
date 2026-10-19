//! M8 S5：S3 Inventory 配置（共享类型、校验与存储 trait）。
//!
//! console 与 S3 两个入口最终都调 [`InventoryStore::put_config`]（`SqlInventoryStore` 内跑
//! [`validate_config`]），因此任何入口都无法绕过同一份约束。

use s3s::dto::{
    InventoryConfiguration, InventoryFormat, InventoryFrequency, InventoryIncludedObjectVersions,
};

use crate::access::AuthedUser;

pub const MAX_ID_CHARS: usize = 64;
pub const MAX_PREFIX_BYTES: usize = 1024;

/// CSV 必含列（顺序即 `fileSchema` 顺序）。
pub const REQUIRED_COLUMNS: [&str; 9] = [
    "Bucket",
    "Key",
    "VersionId",
    "IsLatest",
    "DeleteMarker",
    "Size",
    "LastModifiedDate",
    "ETag",
    "StorageClass",
];

/// 可选的元数据列（固定按此顺序追加；只收现有元数据可推导的字段）。
pub const OPTIONAL_COLUMNS: [&str; 6] = [
    "ReplicationStatus",
    "EncryptionStatus",
    "ObjectLockRetainUntilDate",
    "ObjectLockMode",
    "ObjectLockLegalHoldStatus",
    "ChecksumAlgorithm",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryError {
    NoSuchBucket,
    NoSuchConfiguration,
    /// 400 InvalidArgument（形状/值域/可选列不支持）。
    InvalidArgument(String),
    /// 403 AccessDenied（配置所有者没有目标桶写权限）。
    AccessDenied(String),
    /// 501 NotImplemented（线格式中本实现不支持的字段）。
    NotImplemented(String),
    Internal(String),
}

impl std::fmt::Display for InventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoSuchBucket => write!(f, "no such bucket"),
            Self::NoSuchConfiguration => write!(f, "no such inventory configuration"),
            Self::InvalidArgument(m)
            | Self::AccessDenied(m)
            | Self::NotImplemented(m)
            | Self::Internal(m) => write!(f, "{m}"),
        }
    }
}

/// 配置存储（oss-server 注入 SQL 实现；xl-s3 不碰业务库）。
#[async_trait::async_trait]
pub trait InventoryStore: Send + Sync {
    async fn put_config(
        &self,
        bucket: &str,
        owner: &AuthedUser,
        cfg: &InventoryConfiguration,
    ) -> Result<(), InventoryError>;
    async fn get_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<InventoryConfiguration>, InventoryError>;
    async fn list_configs(
        &self,
        bucket: &str,
    ) -> Result<Vec<InventoryConfiguration>, InventoryError>;
    async fn delete_config(&self, bucket: &str, id: &str) -> Result<(), InventoryError>;
}

fn check_prefix(p: &str, what: &str) -> Result<(), InventoryError> {
    if p.len() > MAX_PREFIX_BYTES {
        return Err(InventoryError::InvalidArgument(format!(
            "{what} 超过 {MAX_PREFIX_BYTES} 字节"
        )));
    }
    if p.chars().any(char::is_control) {
        return Err(InventoryError::InvalidArgument(format!(
            "{what} 含控制字符"
        )));
    }
    Ok(())
}

/// 全量校验（不改写配置；owner/目标桶存在性由 SQL 实现补）。
pub fn validate_config(cfg: &InventoryConfiguration) -> Result<(), InventoryError> {
    let id = cfg.id.as_str();
    if id.is_empty()
        || id.len() > MAX_ID_CHARS
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(InventoryError::InvalidArgument(format!(
            "id 非法（需 [A-Za-z0-9._-]{{1,{MAX_ID_CHARS}}}）：{id:?}"
        )));
    }
    let freq = cfg.schedule.frequency.as_str();
    if freq != InventoryFrequency::DAILY && freq != InventoryFrequency::WEEKLY {
        return Err(InventoryError::InvalidArgument(format!(
            "frequency 非法：{freq}"
        )));
    }
    let dest = &cfg.destination.s3_bucket_destination;
    if dest.bucket.is_empty() {
        return Err(InventoryError::InvalidArgument(
            "destination bucket 为空".into(),
        ));
    }
    if dest.format.as_str() != InventoryFormat::CSV {
        return Err(InventoryError::NotImplemented(format!(
            "inventory format {}",
            dest.format.as_str()
        )));
    }
    if dest.encryption.is_some() {
        return Err(InventoryError::NotImplemented(
            "inventory destination encryption".into(),
        ));
    }
    if let Some(p) = &dest.prefix {
        check_prefix(p, "destination prefix")?;
    }
    let iov = cfg.included_object_versions.as_str();
    if iov != InventoryIncludedObjectVersions::ALL
        && iov != InventoryIncludedObjectVersions::CURRENT
    {
        return Err(InventoryError::InvalidArgument(format!(
            "included_object_versions 非法：{iov}"
        )));
    }
    if let Some(f) = &cfg.filter {
        check_prefix(&f.prefix, "filter prefix")?;
    }
    if let Some(fields) = &cfg.optional_fields {
        let mut seen = std::collections::HashSet::new();
        for f in fields {
            let name = f.as_str();
            // C9：这两列现有元数据推不出，明确拒绝而不是静默空列。
            if name == "IsMultipartUploaded" || name == "IntelligentTieringAccessTier" {
                return Err(InventoryError::InvalidArgument(format!(
                    "optional field 不支持：{name}"
                )));
            }
            // 必含列作为 optional 重复声明是 AWS 允许的空操作。
            if !OPTIONAL_COLUMNS.contains(&name) && !REQUIRED_COLUMNS.contains(&name) {
                return Err(InventoryError::InvalidArgument(format!(
                    "optional field 非法：{name}"
                )));
            }
            if !seen.insert(name) {
                return Err(InventoryError::InvalidArgument(format!(
                    "optional field 重复：{name}"
                )));
            }
        }
    }
    Ok(())
}

/// CSV 列（必含 + 已请求的可选列，固定顺序）。
pub fn csv_columns(cfg: &InventoryConfiguration) -> Vec<&'static str> {
    let mut out = REQUIRED_COLUMNS.to_vec();
    if let Some(fields) = &cfg.optional_fields {
        for c in OPTIONAL_COLUMNS {
            if fields.iter().any(|f| f.as_str() == c) {
                out.push(c);
            }
        }
    }
    out
}

/// 扫描是否包含全部版本（否则只扫当前对象）。
pub fn includes_all_versions(cfg: &InventoryConfiguration) -> bool {
    cfg.included_object_versions.as_str() == InventoryIncludedObjectVersions::ALL
}

/// 输出前缀过滤（空串 = 全桶）。
pub fn filter_prefix(cfg: &InventoryConfiguration) -> &str {
    cfg.filter.as_ref().map_or("", |f| f.prefix.as_str())
}
