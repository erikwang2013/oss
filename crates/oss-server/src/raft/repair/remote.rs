//! `repair::run` 的远端原语：`/site/head-object` 三态探测与同 id 重推。

use xl_store::store::Store;

use super::{RepairReport, err};
use crate::repl_client::ReplClient;

/// 内部 `/site/head-object` 的三态：200 对象 / 405 marker / 404 缺失。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteHead {
    Object,
    Marker,
    Missing,
}

pub(super) async fn remote_head(
    client: &ReplClient,
    peer_id: &str,
    bucket: &str,
    key: &str,
    version_id: &str,
    token: &str,
) -> Result<RemoteHead, String> {
    match client
        .head_object_version(peer_id, bucket, key, version_id, Some(token))
        .await
    {
        Ok(true) => Ok(RemoteHead::Object),
        Ok(false) => Ok(RemoteHead::Marker),
        Err(e) if e.status == Some(404) => Ok(RemoteHead::Missing),
        Err(e) => Err(e.message),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn push_version(
    store: &Store,
    client: &ReplClient,
    report: &mut RepairReport,
    peer_id: &str,
    token: &str,
    bucket: &str,
    key: &str,
    version_id: &str,
    max_object_bytes: u64,
) {
    let (info, bytes) = match store.get_object_version(bucket, key, version_id).await {
        Ok(v) => v,
        Err(e) => {
            err(report, bucket, key, version_id, format!("read local: {e}"));
            return;
        }
    };
    if info.encryption.is_some() {
        err(
            report,
            bucket,
            key,
            version_id,
            "encrypted object cannot be re-pushed",
        );
        return;
    }
    if bytes.len() as u64 > max_object_bytes {
        err(
            report,
            bucket,
            key,
            version_id,
            format!("{} bytes exceeds sync.max_object_bytes", bytes.len()),
        );
        return;
    }
    let tags = (!info.tags.is_empty()).then_some(info.tags.as_slice());
    match client
        .put_object_with_meta(
            peer_id,
            bucket,
            key,
            bytes,
            &info.etag,
            info.content_type.as_deref(),
            &info.user_metadata,
            tags,
            info.storage_class.as_deref(),
            info.checksum.as_ref(),
            true,
            Some(token),
            Some(version_id),
            info.part_sizes.as_deref(), // 4.1：同 id 重推保留段长表
            None,                       // 加密对象在上方已拒绝重推
            None,
        )
        .await
    {
        Ok(remote_id) => {
            let mapped = remote_id.as_deref().unwrap_or(version_id);
            if let Err(e) = store
                .set_remote_version_id(bucket, key, version_id, Some(mapped))
                .await
            {
                err(report, bucket, key, version_id, format!("map: {e}"));
            } else {
                report.pushed += 1;
            }
        }
        Err(e) => err(
            report,
            bucket,
            key,
            version_id,
            format!("push: {}", e.message),
        ),
    }
}
