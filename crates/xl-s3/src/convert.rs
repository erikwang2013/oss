//! xl-store ↔ s3s 的错误与类型翻译。

use s3s::{S3Error, S3ErrorCode};
use xl_store::error::Error as StoreError;

pub fn to_s3_error(e: StoreError) -> S3Error {
    let code = match &e {
        StoreError::NoSuchBucket(_) => S3ErrorCode::NoSuchBucket,
        StoreError::BucketAlreadyExists(_) => S3ErrorCode::BucketAlreadyOwnedByYou,
        StoreError::BucketNotEmpty(_) => S3ErrorCode::BucketNotEmpty,
        StoreError::NoSuchKey(_) => S3ErrorCode::NoSuchKey,
        // s3s 没有 InvalidObjectName，键名不合法归 InvalidArgument
        StoreError::InvalidKey(_) => S3ErrorCode::InvalidArgument,
        StoreError::InvalidBucketName(_) => S3ErrorCode::InvalidBucketName,
        StoreError::NoSuchUpload(_) => S3ErrorCode::NoSuchUpload,
        StoreError::InvalidPart(_) => S3ErrorCode::InvalidPart,
        StoreError::InvalidPartOrder(_) => S3ErrorCode::InvalidPartOrder,
        StoreError::EntityTooSmall(_) => S3ErrorCode::EntityTooSmall,
        // 内部错误：细节（盘上路径、内部文案）只进日志，不进 5xx 响应体
        StoreError::CorruptMeta(_) | StoreError::Encode(_) | StoreError::Io(_) => {
            tracing::error!(error = ?e, "store internal error");
            return S3Error::with_message(S3ErrorCode::InternalError, "internal error");
        }
    };
    S3Error::with_message(code, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 4xx 是可纠正错误：原消息对客户端有指导意义，保留。
    #[test]
    fn client_errors_keep_original_message() {
        let cases = [
            (
                StoreError::NoSuchBucket("b".into()),
                S3ErrorCode::NoSuchBucket,
                "bucket not found: b",
            ),
            (
                StoreError::BucketAlreadyExists("b".into()),
                S3ErrorCode::BucketAlreadyOwnedByYou,
                "bucket already exists: b",
            ),
            (
                StoreError::BucketNotEmpty("b".into()),
                S3ErrorCode::BucketNotEmpty,
                "bucket not empty: b",
            ),
            (
                StoreError::NoSuchKey("k".into()),
                S3ErrorCode::NoSuchKey,
                "key not found: k",
            ),
            (
                StoreError::InvalidKey("k".into()),
                S3ErrorCode::InvalidArgument,
                "invalid key: k",
            ),
            (
                StoreError::InvalidBucketName("b".into()),
                S3ErrorCode::InvalidBucketName,
                "invalid bucket name: b",
            ),
        ];
        for (e, code, msg) in cases {
            let err = to_s3_error(e);
            assert_eq!(err.code(), &code);
            assert_eq!(err.message(), Some(msg));
        }
    }

    #[test]
    fn internal_errors_are_generic() {
        for e in [
            StoreError::CorruptMeta("不支持的格式版本 9".into()),
            StoreError::Encode("e".into()),
            StoreError::Io(std::io::Error::other("io")),
        ] {
            let err = to_s3_error(e);
            assert_eq!(err.code(), &S3ErrorCode::InternalError);
            assert_eq!(err.message(), Some("internal error"), "5xx 不泄漏内部细节");
        }
    }
}
