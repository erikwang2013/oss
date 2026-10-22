//! UploadPartCopy（`PUT /{bucket}/{key}?partNumber&uploadId` + `x-amz-copy-source`）。
//!
//! 语义要点（AWS S3）：
//! - 源对象可点名版本（copy source 的 `?versionId=`），源读取受 `copy-source-sse-c*`
//!   与 copy-source 条件头约束；任一条件不满足 → **412**（复制语义，If-None-Match
//!   命中也是 412，不区分 GET 的 304）。
//! - `x-amz-copy-source-range` 支持 `bytes=a-b` / `bytes=a-` / `bytes=-n`；多段/语法
//!   错误 → 400，越界 → 416 InvalidRange。
//! - 目标 part 大小沿用 `max_put_bytes`（multipart 大对象整体不受限、按 part 计）；
//!   partNumber 1..=10000。
//! - SSE-C：源钥用于读源，目标钥的 md5 由 `store.upload_part_ext` 与 create 记录
//!   核对（不一致 400）；暂存仍明文，complete 单流加密（与 UploadPart 一致）。
//!
//! 实现位置说明：Rust 不允许同一类型多个 `impl S3`（E0119），本模块提供 free
//! function，`service.rs` 的 `impl S3` 里只有转发方法（同 `list_v1.rs` 先例）。

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};
use xl_store::store::VersioningMode;

use crate::convert::to_s3_error;
use crate::sse::{parse_sse, sse_out_from_spec};

use super::{OssS3, eval_preconditions};

pub(super) async fn upload_part_copy(
    s3: &OssS3,
    req: S3Request<UploadPartCopyInput>,
) -> S3Result<S3Response<UploadPartCopyOutput>> {
    // s3s 已解码 header（URL decode、去掉前导 '/'）；只接受桶形式（access point 不在面内）。
    let CopySource::Bucket {
        bucket: src_bucket,
        key: src_key,
        version_id,
        ..
    } = &req.input.copy_source
    else {
        return Err(s3s::s3_error!(InvalidRequest, "unsupported copy source"));
    };
    let part_number = req.input.part_number;
    if !(1..=10_000).contains(&part_number) {
        return Err(s3s::s3_error!(InvalidArgument, "part number out of range"));
    }

    // 源钥（读源）与目标钥（与 create 的 SSE-C 记录核对，store 负责 400）。
    let src_sse = parse_sse(
        req.input.copy_source_sse_customer_algorithm.as_deref(),
        req.input.copy_source_sse_customer_key.as_deref(),
        req.input.copy_source_sse_customer_key_md5.as_deref(),
        None,
        None,
    )?;
    let dst_sse = parse_sse(
        req.input.sse_customer_algorithm.as_deref(),
        req.input.sse_customer_key.as_deref(),
        req.input.sse_customer_key_md5.as_deref(),
        None,
        None,
    )?;

    // 先 head：拿到 size/etag/mtime 供条件、Range 与上限判断，条件失败不读数据。
    let src_info = match version_id.as_deref() {
        Some(v) => {
            s3.store
                .head_object_version_ext(src_bucket, src_key, v, src_sse.key.as_ref())
                .await
        }
        None => {
            s3.store
                .head_object_ext(src_bucket, src_key, src_sse.key.as_ref())
                .await
        }
    }
    .map_err(to_s3_error)?;

    // copy-source 条件：任何不满足（含 If-None-Match 命中）一律 412。
    if eval_preconditions(
        &src_info,
        req.input.copy_source_if_match.as_ref(),
        req.input.copy_source_if_unmodified_since.as_ref(),
        req.input.copy_source_if_none_match.as_ref(),
        req.input.copy_source_if_modified_since.as_ref(),
    )
    .is_err()
    {
        return Err(s3s::s3_error!(
            PreconditionFailed,
            "copy source precondition failed"
        ));
    }

    // Range 解析在 head 之后：越界判定需要源大小。多段 range 在 parse 即拒（400）。
    let (start, end) = match req.input.copy_source_range.as_deref() {
        None => (0, src_info.size),
        Some(h) => {
            let range = Range::parse(h)
                .map_err(|_| s3s::s3_error!(InvalidArgument, "invalid x-amz-copy-source-range"))?;
            let rr = range.check(src_info.size).map_err(s3s::S3Error::from)?; // 416
            (rr.start, rr.end)
        }
    };
    // 上限先于读盘：整份源 / 大 range 不得先读进内存再拒。
    if end - start > s3.max_put_bytes as u64 {
        return Err(s3s::s3_error!(EntityTooLarge, "part exceeds max_put_bytes"));
    }

    let bytes = if start == 0 && end == src_info.size {
        match version_id.as_deref() {
            Some(v) => {
                s3.store
                    .get_object_version_ext(src_bucket, src_key, v, src_sse.key.as_ref())
                    .await
            }
            None => {
                s3.store
                    .get_object_ext(src_bucket, src_key, src_sse.key.as_ref())
                    .await
            }
        }
        .map_err(to_s3_error)?
        .1
    } else {
        match version_id.as_deref() {
            Some(v) => {
                s3.store
                    .get_object_range_version_ext(
                        src_bucket,
                        src_key,
                        v,
                        start..end,
                        src_sse.key.as_ref(),
                    )
                    .await
            }
            None => {
                s3.store
                    .get_object_range_ext(src_bucket, src_key, start..end, src_sse.key.as_ref())
                    .await
            }
        }
        .map_err(to_s3_error)?
    };

    let etag = s3
        .store
        .upload_part_ext(
            &req.input.bucket,
            &req.input.key,
            &req.input.upload_id,
            part_number as u16,
            Box::pin(futures::stream::once(async move { Ok(bytes) })),
            dst_sse.sse_c_md5(),
        )
        .await
        .map_err(to_s3_error)?;

    // x-amz-copy-source-version-id：仅源桶启用版本控制时返回（未启用桶回 "null" 无意义）。
    let mode = s3.versioning_mode(src_bucket).await?;
    let copy_source_version_id = if mode == VersioningMode::Off {
        None
    } else {
        src_info.version_id.clone()
    };

    let so = sse_out_from_spec(&dst_sse);
    Ok(S3Response::new(UploadPartCopyOutput {
        copy_part_result: Some(CopyPartResult {
            e_tag: Some(ETag::Strong(etag)),
            // LastModified = 本 part 的创建时刻（与源对象 mtime 无关）
            last_modified: Some(LastModified::from(std::time::SystemTime::now())),
            ..Default::default()
        }),
        copy_source_version_id,
        server_side_encryption: so
            .server_side_encryption
            .map(ServerSideEncryption::from_static),
        ssekms_key_id: so.kms_key_id,
        sse_customer_algorithm: so.sse_c_algorithm.map(str::to_string),
        sse_customer_key_md5: so.sse_c_key_md5,
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 源 range 解析口径：三种合法形态 + 多段/越界拒绝（纯解析，不触 store）。
    #[test]
    fn copy_source_range_forms() {
        let parse = |h: &str, size: u64| {
            let r = Range::parse(h).map_err(|_| ())?;
            r.check(size).map_err(|_| ())
        };
        assert_eq!(parse("bytes=0-4", 10), Ok(0..5));
        assert_eq!(parse("bytes=5-", 10), Ok(5..10));
        assert_eq!(parse("bytes=-3", 10), Ok(7..10));
        assert_eq!(parse("bytes=5-100", 10), Ok(5..10)); // 尾端夹到 size-1
        assert!(parse("bytes=0-1,3-4", 10).is_err()); // 多段 → 400
        assert!(parse("bytes=10-", 10).is_err()); // 越界 → 416
        assert!(parse("bytes=-0", 10).is_err());
        assert!(parse("0-5", 10).is_err()); // 缺 bytes= 前缀
    }
}
