//! ListMultipartUploads（`GET /{bucket}?uploads`）。
//!
//! 分页口径（AWS）：按 (key, upload_id) 有序；`key-marker` 排他，`upload-id-marker`
//! 仅在与 `key-marker` 同时出现时生效（单独出现被忽略）；`max-uploads` 0..=1000，
//! 负数 400、超 1000 夹到 1000。`NextKeyMarker`/`NextUploadIdMarker` = 本页最后一个
//! 返回项（末项为 CommonPrefix 时不返回 upload id marker）。
//! delimiter 折叠规则与 ListObjects 一致：`cp <= key-marker` 的键整组跳过（该 cp 已在上页发出）。
//!
//! 实现位置说明：Rust 不允许同一类型多个 `impl S3`（E0119），本模块提供 free
//! function，`service.rs` 的 `impl S3` 里只有转发方法（同 `list_v1.rs` 先例）。

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};
use xl_store::store::UploadListRequest;

use crate::convert::to_s3_error;

use super::list_v1::url_encode;
use super::{OssS3, last_modified};

/// `x-oss-degraded: mpu-union`：并集可能缺项（legacy peer / 不可达）时出现。
fn mpu_union_degraded_headers(resp: &mut S3Response<ListMultipartUploadsOutput>, degraded: bool) {
    if degraded {
        resp.headers.insert(
            "x-oss-degraded",
            http::HeaderValue::from_static("mpu-union"),
        );
    }
}

pub(super) async fn list_multipart_uploads(
    s3: &OssS3,
    req: S3Request<ListMultipartUploadsInput>,
) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
    let ListMultipartUploadsInput {
        bucket,
        delimiter,
        encoding_type,
        key_marker,
        max_uploads,
        prefix,
        upload_id_marker,
        ..
    } = req.input;

    // 负数不能经 usize 回绕成巨值再被静默钳成 1000。
    let max_uploads = match max_uploads {
        Some(m) if m < 0 => {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "max-uploads must not be negative"
            ));
        }
        other => other.map(|m| m as usize),
    };
    // AWS 只接受 url；其它值不静默忽略（避免响应内容与请求头不符）。
    let encode = match encoding_type.as_ref().map(EncodingType::as_str) {
        None => false,
        Some(EncodingType::URL) => true,
        Some(_) => {
            return Err(s3s::s3_error!(InvalidArgument, "encoding-type must be url"));
        }
    };
    let effective_max = max_uploads.unwrap_or(1000).min(1000);

    let result = s3
        .store
        .list_multipart_uploads(
            &bucket,
            &UploadListRequest {
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                key_marker: key_marker.clone(),
                upload_id_marker: upload_id_marker.clone(),
                max_uploads,
            },
        )
        .await
        .map_err(to_s3_error)?;

    let enc = |s: &str| if encode { url_encode(s) } else { s.to_string() };
    let uploads: MultipartUploadList = result
        .uploads
        .iter()
        .map(|u| MultipartUpload {
            key: Some(enc(&u.key)),
            upload_id: Some(u.upload_id.clone()),
            initiated: Some(last_modified(u.initiated_nanos)),
            storage_class: Some(StorageClass::from(
                u.storage_class
                    .clone()
                    .unwrap_or_else(|| "STANDARD".to_string()),
            )),
            ..Default::default()
        })
        .collect();

    let mut resp = S3Response::new(ListMultipartUploadsOutput {
        bucket: Some(bucket),
        prefix: prefix.as_deref().map(enc),
        delimiter: delimiter.as_deref().map(enc),
        key_marker: key_marker.as_deref().map(enc),
        upload_id_marker,
        next_key_marker: result.next_key_marker.as_deref().map(enc),
        next_upload_id_marker: result.next_upload_id_marker,
        max_uploads: Some(effective_max as i32),
        is_truncated: Some(result.is_truncated),
        uploads: Some(uploads),
        common_prefixes: Some(
            result
                .common_prefixes
                .iter()
                .map(|p| CommonPrefix {
                    prefix: Some(enc(p)),
                })
                .collect(),
        ),
        encoding_type: encode.then(|| EncodingType::from_static(EncodingType::URL)),
        ..Default::default()
    });
    mpu_union_degraded_headers(&mut resp, result.degraded);
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mpu_union_degraded_header_present_iff_degraded() {
        let mut ok = S3Response::new(ListMultipartUploadsOutput::default());
        mpu_union_degraded_headers(&mut ok, false);
        assert!(ok.headers.get("x-oss-degraded").is_none());
        let mut deg = S3Response::new(ListMultipartUploadsOutput::default());
        mpu_union_degraded_headers(&mut deg, true);
        assert_eq!(deg.headers.get("x-oss-degraded").unwrap(), "mpu-union");
    }
}
