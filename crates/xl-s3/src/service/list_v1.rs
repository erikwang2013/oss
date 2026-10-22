//! S3 ListObjects v1（`GET /{bucket}`，无 `list-type=2`）：marker 排他分页、
//! delimiter/CommonPrefixes、encoding-type=url。与 V2 共用 store 列表原语，
//! V2 行为不变（`service.rs::list_objects_v2`）。
//!
//! 分页口径（AWS）：marker 是排他起点；`NextMarker` 仅在「截断 + 指定 delimiter」
//! 时返回（否则客户端用最后一个 Contents 键续页）。max-keys 上限 1000、负数 400。
//!
//! 注意：Rust 不允许同一类型有多个 `impl S3`（trait impl 不能按模块拆分，只有
//! inherent impl 可以）。本模块提供 free function，`service.rs` 的 `impl S3` 里有一个
//! 转发方法；handler 主体仍全在这里，避免 service.rs 变成第二个热点文件。

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};
use xl_store::store::{ListRequest, ListResult};

use crate::convert::to_s3_error;

use super::{OssS3, last_modified};

/// AWS 的 `encoding-type=url`：仅保留 RFC 3986 unreserved，其余字节按 UTF-8 %XX（大写）。
fn url_encode(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// 最后一个已返回输出的排序键（v1 的 NextMarker 语义：Contents 与 CommonPrefixes 取大者）。
fn last_output_key(r: &ListResult) -> Option<String> {
    match (r.objects.last(), r.common_prefixes.last()) {
        (Some(o), Some(cp)) => Some(o.key.clone().max(cp.clone())),
        (Some(o), None) => Some(o.key.clone()),
        (None, Some(cp)) => Some(cp.clone()),
        (None, None) => None,
    }
}

pub(super) async fn list_objects(
    s3: &OssS3,
    req: S3Request<ListObjectsInput>,
) -> S3Result<S3Response<ListObjectsOutput>> {
    let ListObjectsInput {
        bucket,
        prefix,
        delimiter,
        marker,
        max_keys,
        encoding_type,
        ..
    } = req.input;

    // 与 V2 同口径：负数不能经 usize 回绕成巨值再被静默钳成 1000。
    let max_keys = match max_keys {
        Some(m) if m < 0 => {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "max-keys must not be negative"
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
    let effective_max = max_keys.unwrap_or(1000).min(1000);

    let result = s3
        .store
        .list_objects(
            &bucket,
            &ListRequest {
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                max_keys,
                marker: marker.clone(),
                ..Default::default()
            },
        )
        .await
        .map_err(to_s3_error)?;

    let enc = |s: &str| if encode { url_encode(s) } else { s.to_string() };

    let contents: ObjectList = result
        .objects
        .iter()
        .map(|o| Object {
            key: Some(enc(&o.key)),
            size: Some(o.size as i64),
            e_tag: Some(ETag::Strong(o.etag.clone())),
            last_modified: Some(last_modified(o.last_modified_nanos)),
            storage_class: Some(ObjectStorageClass::from(
                o.storage_class
                    .clone()
                    .unwrap_or_else(|| "STANDARD".to_string()),
            )),
            ..Default::default()
        })
        .collect();

    // NextMarker 只在「截断 + 非空 delimiter」时返回；值是最后一个已返回输出。
    let next_marker = if result.is_truncated && delimiter.as_deref().is_some_and(|d| !d.is_empty())
    {
        last_output_key(&result).map(|k| enc(&k))
    } else {
        None
    };

    Ok(S3Response::new(ListObjectsOutput {
        name: Some(bucket),
        prefix: prefix.as_deref().map(enc),
        marker: marker.as_deref().map(enc),
        max_keys: Some(effective_max as i32),
        is_truncated: Some(result.is_truncated),
        contents: Some(contents),
        common_prefixes: Some(
            result
                .common_prefixes
                .iter()
                .map(|p| CommonPrefix {
                    prefix: Some(enc(p)),
                })
                .collect(),
        ),
        delimiter: delimiter.as_deref().map(enc),
        next_marker,
        encoding_type: encode.then(|| EncodingType::from_static(EncodingType::URL)),
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_encoding_matches_aws_examples() {
        assert_eq!(url_encode("test_file(3).png"), "test_file%283%29.png");
        assert_eq!(url_encode("a b"), "a%20b");
        assert_eq!(url_encode("dir/中文.txt"), "dir%2F%E4%B8%AD%E6%96%87.txt");
        assert_eq!(url_encode("keep-._~AZaz09"), "keep-._~AZaz09");
    }

    #[test]
    fn last_output_key_takes_max_of_contents_and_prefixes() {
        let mut r = ListResult::default();
        assert_eq!(last_output_key(&r), None);
        r.objects.push(xl_store::store::ObjectInfo {
            key: "a".into(),
            size: 0,
            etag: String::new(),
            content_type: None,
            last_modified_nanos: 0,
            user_metadata: Default::default(),
            checksum: None,
            version_id: None,
            encryption: None,
            lock: None,
            replication_status: None,
            remote_version_id: None,
            tags: Vec::new(),
            storage_class: None,
            tier_phase: None,
            restore_expires_at: None,
        });
        r.common_prefixes.push("b/".into());
        assert_eq!(last_output_key(&r).as_deref(), Some("b/"));
        r.common_prefixes.clear();
        r.common_prefixes.push("0/".into());
        assert_eq!(last_output_key(&r).as_deref(), Some("a"));
    }
}
