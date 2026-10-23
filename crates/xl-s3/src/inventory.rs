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

/// 存储 JSON 的扩展键：源桶 12 位账户 ID（标准 S3 线格式没有该字段）。
/// 旧读者忽略未知 JSON 键；旧配置无此键时按当前桶账户推导。
pub const SOURCE_ACCOUNT_FIELD: &str = "ossSourceAccountId";

/// 目标桶字段可为桶名，或 AWS 文档形态 `arn:aws:s3:::<bucket>`（无账户段）。
/// 非法 ARN → None；非 ARN 原样返回（空串由调用方校验）。
pub fn destination_bucket_name(raw: &str) -> Option<&str> {
    let Some(rest) = raw.strip_prefix("arn:") else {
        return Some(raw);
    };
    let bucket = rest.strip_prefix("aws:s3:::")?;
    (!bucket.is_empty() && !bucket.contains(':') && !bucket.contains('/')).then_some(bucket)
}

/// 12 位十进制账户 ID（AWS 线上形态）。
pub fn is_account_id(v: &str) -> bool {
    v.len() == 12 && v.bytes().all(|b| b.is_ascii_digit())
}

/// 标准配置 → 存储 JSON。`source_account` 有值时写扩展键；旧形态保持可读。
pub fn config_to_json(
    cfg: &InventoryConfiguration,
    source_account: Option<&str>,
) -> Result<String, serde_json::Error> {
    let mut v = serde_json::to_value(cfg)?;
    if let (Some(account), Some(obj)) = (source_account, v.as_object_mut()) {
        obj.insert(SOURCE_ACCOUNT_FIELD.to_string(), account.into());
    }
    serde_json::to_string(&v)
}

/// 存储 JSON → 标准配置 + 源桶账户（旧配置无扩展键 → None）。
pub fn config_from_json(
    json: &str,
) -> Result<(InventoryConfiguration, Option<String>), serde_json::Error> {
    let mut v: serde_json::Value = serde_json::from_str(json)?;
    let source = v
        .get(SOURCE_ACCOUNT_FIELD)
        .and_then(|x| x.as_str())
        .map(str::to_string);
    if let Some(obj) = v.as_object_mut() {
        obj.remove(SOURCE_ACCOUNT_FIELD);
    }
    Ok((serde_json::from_value(v)?, source))
}

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
    if destination_bucket_name(&dest.bucket).is_none() {
        return Err(InventoryError::InvalidArgument(format!(
            "destination bucket ARN 非法（需 arn:aws:s3:::<bucket>）：{}",
            dest.bucket
        )));
    }
    if let Some(account) = &dest.account_id
        && !is_account_id(account)
    {
        return Err(InventoryError::InvalidArgument(format!(
            "destination account_id 非法（需 12 位数字）：{account}"
        )));
    }
    let format = dest.format.as_str();
    if format != InventoryFormat::CSV && format != InventoryFormat::PARQUET {
        return Err(InventoryError::NotImplemented(format!(
            "inventory format {format}"
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> InventoryConfiguration {
        InventoryConfiguration {
            destination: s3s::dto::InventoryDestination {
                s3_bucket_destination: s3s::dto::InventoryS3BucketDestination {
                    bucket: "dst".into(),
                    format: s3s::dto::InventoryFormat::from_static("CSV"),
                    ..Default::default()
                },
            },
            id: "c1".into(),
            included_object_versions: s3s::dto::InventoryIncludedObjectVersions::from_static("All"),
            is_enabled: true,
            schedule: s3s::dto::InventorySchedule {
                frequency: s3s::dto::InventoryFrequency::from_static("Daily"),
            },
            ..Default::default()
        }
    }

    #[test]
    fn destination_bucket_forms() {
        assert_eq!(destination_bucket_name("dst"), Some("dst"));
        assert_eq!(destination_bucket_name("arn:aws:s3:::dst"), Some("dst"));
        assert_eq!(destination_bucket_name("arn:aws:s3:::dst/x"), None);
        assert_eq!(destination_bucket_name("arn:aws:s3:::dst:1"), None);
        assert_eq!(destination_bucket_name("arn:aws:s3:::"), None);
        assert_eq!(destination_bucket_name("arn:aws:iam::123:root"), None);
        assert_eq!(destination_bucket_name(""), Some(""));
    }

    #[test]
    fn account_shape() {
        assert!(is_account_id("000000000000"));
        assert!(is_account_id("123456789012"));
        assert!(!is_account_id("12345678901"));
        assert!(!is_account_id("1234567890123"));
        assert!(!is_account_id("12345678901x"));
    }

    #[test]
    fn json_roundtrip_with_and_without_source_account() {
        let c = cfg();
        let plain = config_to_json(&c, None).unwrap();
        assert!(!plain.contains(SOURCE_ACCOUNT_FIELD));
        let (got, account) = config_from_json(&plain).unwrap();
        assert_eq!(got, c);
        assert_eq!(account, None);

        let with = config_to_json(&c, Some("000000000002")).unwrap();
        let (got, account) = config_from_json(&with).unwrap();
        assert_eq!(got, c);
        assert_eq!(account.as_deref(), Some("000000000002"));
    }

    #[test]
    fn validate_account_and_arn_forms() {
        let mut c = cfg();
        c.destination.s3_bucket_destination.account_id = Some("000000000002".into());
        assert!(validate_config(&c).is_ok());
        c.destination.s3_bucket_destination.account_id = Some("abc".into());
        assert!(matches!(
            validate_config(&c),
            Err(InventoryError::InvalidArgument(_))
        ));
        c.destination.s3_bucket_destination.account_id = None;
        c.destination.s3_bucket_destination.bucket = "arn:aws:s3:::bucket".into();
        assert!(validate_config(&c).is_ok());
        c.destination.s3_bucket_destination.bucket = "arn:aws:s3:::bucket/key".into();
        assert!(matches!(
            validate_config(&c),
            Err(InventoryError::InvalidArgument(_))
        ));
    }
}
