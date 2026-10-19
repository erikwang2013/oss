use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bucket not found: {0}")]
    NoSuchBucket(String),
    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),
    #[error("bucket not empty: {0}")]
    BucketNotEmpty(String),
    #[error("key not found: {0}")]
    NoSuchKey(String),
    #[error("invalid key: {0}")]
    InvalidKey(String),
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),
    #[error("corrupt metadata: {0}")]
    CorruptMeta(String),
    #[error("no such multipart upload: {0}")]
    NoSuchUpload(String),
    #[error("invalid part: {0}")]
    InvalidPart(String),
    #[error("parts not in ascending order: {0}")]
    InvalidPartOrder(String),
    #[error("part too small: {0}")]
    EntityTooSmall(String),
    #[error("bad digest: {0}")]
    BadDigest(String),
    /// M2b-2：多盘写入未达 quorum（存活分片 < data+1）。可重试（客户端层面）。
    #[error("write quorum unmet: {0}")]
    QuorumUnmet(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("encode error: {0}")]
    Encode(String),
    /// M4 版本控制：对 delete marker 版本点名操作（GET/HEAD 具体 marker）→ S3 405。
    #[error("delete marker: {0}")]
    DeleteMarker(String),
    /// M4 版本控制：非法参数（如 version-id-marker 不在链内）→ S3 400。
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// M8 S4a：归档对象当前不可读（未 Restore/恢复期已过）→ S3 403 `InvalidObjectState`。
    #[error("invalid object state: {0}")]
    InvalidObjectState(String),
    /// M8 S4b：Restore 已在进行/已恢复 → S3 409 `RestoreAlreadyInProgress`。
    #[error("restore already in progress: {0}")]
    RestoreAlreadyInProgress(String),
    /// M5 密钥库/加解密：wrap 解密失败（篡改/错钥）、主密钥文件损坏等——独立变体供上层
    /// 精确映射（如 SSE-C 错钥 → 4xx，而非 500 泛化）。
    #[error("crypto error: {0}")]
    Crypto(String),
    /// M5 切片 4：对象锁拒绝（retention/legal hold 生效中）→ S3 403 AccessDenied（客户端可纠正）。
    #[error("object locked: {0}")]
    ObjectLocked(String),
}

pub type Result<T> = std::result::Result<T, Error>;
