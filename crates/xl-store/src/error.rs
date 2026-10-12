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
}

pub type Result<T> = std::result::Result<T, Error>;
