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
        StoreError::BadDigest(_) => S3ErrorCode::BadDigest,
        // 对 delete marker 版本点名 GET/HEAD：405（S3 语义）
        StoreError::DeleteMarker(_) => S3ErrorCode::MethodNotAllowed,
        StoreError::InvalidObjectState(_) => S3ErrorCode::InvalidObjectState,
        StoreError::RestoreAlreadyInProgress(_) => S3ErrorCode::RestoreAlreadyInProgress,
        StoreError::InvalidArgument(_) => S3ErrorCode::InvalidArgument,
        // M5 切片 4：锁定删除 → 403 AccessDenied（AWS 对对象锁拒绝的口径）
        StoreError::ObjectLocked(_) => S3ErrorCode::AccessDenied,
        // M9：sync 门不可用（远端失败/超时/配对缺失）→ 503；不降级、不重试。
        StoreError::Unavailable(_) => S3ErrorCode::ServiceUnavailable,
        // M9：明确不支持的能力 → 501。
        StoreError::Unsupported(_) => S3ErrorCode::NotImplemented,
        // M9：状态冲突（sync 版本化未就绪/同 id 异体）→ 409。
        StoreError::Conflict(_) => S3ErrorCode::ConditionalRequestConflict,
        // M9：远端 ack 的版本 id 与预生成不一致 → 502（s3s 无 BadGateway 变体，强制状态码）。
        StoreError::BadGateway(_) => {
            let mut err = S3Error::with_message(S3ErrorCode::InternalError, e.to_string());
            err.set_status_code(http::StatusCode::BAD_GATEWAY);
            return err;
        }
        // 内部错误：细节（盘上路径、内部文案）只进日志，不进 5xx 响应体。
        // M5：Crypto（wrap 解不开/主密钥缺失）暂同归 500 泛化——对客户端可纠正的形态
        // （SSE-C 错钥 → 4xx）在切片 3 接线时再细分，此处不提前放宽。
        StoreError::QuorumUnmet(_)
        | StoreError::CorruptMeta(_)
        | StoreError::Encode(_)
        | StoreError::Crypto(_)
        | StoreError::Io(_) => {
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
    fn sync_errors_map_to_503_501_409_502() {
        let err = to_s3_error(StoreError::Unavailable("remote down".into()));
        assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
        assert_eq!(err.message(), Some("unavailable: remote down"));
        let err = to_s3_error(StoreError::Unsupported("sync mpu".into()));
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
        let err = to_s3_error(StoreError::Conflict("versioning suspended".into()));
        assert_eq!(err.status_code(), Some(http::StatusCode::CONFLICT));
        let err = to_s3_error(StoreError::BadGateway("sync version id mismatch".into()));
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_GATEWAY));
        assert_eq!(err.message(), Some("bad gateway: sync version id mismatch"));
    }

    #[test]
    fn internal_errors_are_generic() {
        for e in [
            StoreError::CorruptMeta("不支持的格式版本 9".into()),
            StoreError::Encode("e".into()),
            StoreError::Crypto("unwrap 解密失败".into()),
            StoreError::Io(std::io::Error::other("io")),
        ] {
            let err = to_s3_error(e);
            assert_eq!(err.code(), &S3ErrorCode::InternalError);
            assert_eq!(err.message(), Some("internal error"), "5xx 不泄漏内部细节");
        }
    }
}
