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
        StoreError::CorruptMeta(_) | StoreError::Encode(_) => S3ErrorCode::InternalError,
        StoreError::Io(_) => S3ErrorCode::InternalError,
    };
    S3Error::with_message(code, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_key_errors() {
        assert_eq!(
            to_s3_error(StoreError::NoSuchKey("k".into())).code(),
            &S3ErrorCode::NoSuchKey
        );
        assert_eq!(
            to_s3_error(StoreError::NoSuchBucket("b".into())).code(),
            &S3ErrorCode::NoSuchBucket
        );
    }

    #[test]
    fn maps_remaining_variants() {
        let cases = [
            (
                StoreError::BucketAlreadyExists("b".into()),
                &S3ErrorCode::BucketAlreadyOwnedByYou,
            ),
            (
                StoreError::BucketNotEmpty("b".into()),
                &S3ErrorCode::BucketNotEmpty,
            ),
            (
                StoreError::InvalidKey("k".into()),
                &S3ErrorCode::InvalidArgument,
            ),
            (
                StoreError::InvalidBucketName("b".into()),
                &S3ErrorCode::InvalidBucketName,
            ),
            (
                StoreError::CorruptMeta("m".into()),
                &S3ErrorCode::InternalError,
            ),
            (StoreError::Encode("e".into()), &S3ErrorCode::InternalError),
            (
                StoreError::Io(std::io::Error::other("io")),
                &S3ErrorCode::InternalError,
            ),
        ];
        for (e, code) in cases {
            assert_eq!(to_s3_error(e).code(), code);
        }
    }
}
