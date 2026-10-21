//! M9 S7.5：`sync-reconcile [--repair]`——B′ 同 origin id 重推、映射刷新与本地 intent/outbox 清理。

use serde::Serialize;
use xl_store::store::Store;

use crate::config::RemoteTarget;
use crate::db::Db;
use crate::repl_client::ReplClient;

#[derive(Debug, Clone, Default)]
pub struct RepairOptions {
    pub bucket: Option<String>,
    pub repair: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepairError {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RepairReport {
    pub buckets: usize,
    pub versions: usize,
    pub already_ok: usize,
    pub mappings_written: usize,
    pub pushed: usize,
    pub markers_pushed: usize,
    pub intents_cleared: u64,
    pub outbox_cleared: u64,
    pub errors: Vec<RepairError>,
}

impl RepairReport {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

fn err(
    report: &mut RepairReport,
    bucket: &str,
    key: &str,
    version_id: &str,
    message: impl Into<String>,
) {
    report.errors.push(RepairError {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: version_id.to_string(),
        message: message.into(),
    });
}

async fn remote_exists(
    client: &ReplClient,
    peer_id: &str,
    bucket: &str,
    key: &str,
    version_id: &str,
    token: &str,
) -> Result<bool, String> {
    client
        .head_object_version(peer_id, bucket, key, version_id, Some(token))
        .await
        .map_err(|e| e.message)
}

#[allow(clippy::too_many_arguments)]
async fn push_version(
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

/// 对账/修复入口。无 `repair` 时只读（HEAD + 本地映射检查）。
pub async fn run(
    db: &Db,
    store: &Store,
    vault: Option<&xl_s3::skvault::SecretVault>,
    max_object_bytes: u64,
    opts: &RepairOptions,
) -> Result<RepairReport, String> {
    let sites = db.list_sites().await.map_err(|e| e.to_string())?;
    if sites.len() != 1 {
        return Err(format!(
            "sync-reconcile requires exactly one site peer (have {})",
            sites.len()
        ));
    }
    let peer = sites.into_iter().next().unwrap();
    let secret = match vault {
        Some(v) if xl_store::kms::is_sealed(&peer.s3_secret_key) => v
            .open(&peer.s3_secret_key)
            .await
            .map_err(|e| format!("unseal peer secret: {e}"))?
            .ok_or_else(|| "peer secret cannot be unsealed".to_string())?,
        _ => peer.s3_secret_key.clone(),
    };
    let client = ReplClient::new(&[RemoteTarget {
        id: peer.peer_id.clone(),
        endpoint: peer.endpoint.clone(),
        access_key: peer.s3_access_key.clone(),
        secret_key: secret,
        region: peer.region.clone(),
        max_bytes_per_sec: None,
    }]);
    let mut buckets: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM buckets WHERE replication_mode = 'sync' ORDER BY name",
    )
    .fetch_all(db.pool())
    .await
    .map_err(|e| e.to_string())?;
    if let Some(only) = &opts.bucket {
        buckets.retain(|b| b == only);
        if buckets.is_empty() {
            return Err(format!("bucket {only} is not a sync bucket"));
        }
    }
    let mut report = RepairReport::default();
    report.buckets = buckets.len();
    for bucket in &buckets {
        let mut key_marker: Option<String> = None;
        let mut version_marker: Option<String> = None;
        loop {
            let page = match store
                .list_object_versions(
                    bucket,
                    "",
                    1000,
                    key_marker.as_deref(),
                    version_marker.as_deref(),
                )
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    err(&mut report, bucket, "", "", format!("list versions: {e}"));
                    break;
                }
            };
            for entry in &page.entries {
                report.versions += 1;
                let local_id = entry.version_id.clone();
                if entry.is_marker {
                    let exists = match remote_exists(
                        &client,
                        &peer.peer_id,
                        bucket,
                        &entry.key,
                        &local_id,
                        &peer.peer_site_token,
                    )
                    .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            err(&mut report, bucket, &entry.key, &local_id, e);
                            continue;
                        }
                    };
                    if exists {
                        report.already_ok += 1;
                        continue;
                    }
                    if !opts.repair {
                        report.markers_pushed += 1;
                        continue;
                    }
                    match client
                        .delete_marker(
                            &peer.peer_id,
                            bucket,
                            &entry.key,
                            &local_id,
                            Some(&peer.peer_site_token),
                        )
                        .await
                    {
                        Ok(()) => report.markers_pushed += 1,
                        Err(e) => {
                            err(&mut report, bucket, &entry.key, &local_id, e.message);
                        }
                    }
                    continue;
                }
                let mapped = entry.remote_version_id.as_deref();
                if mapped == Some(local_id.as_str()) {
                    match remote_exists(
                        &client,
                        &peer.peer_id,
                        bucket,
                        &entry.key,
                        &local_id,
                        &peer.peer_site_token,
                    )
                    .await
                    {
                        Ok(true) => report.already_ok += 1,
                        Ok(false) => {
                            if opts.repair {
                                push_version(
                                    store,
                                    &client,
                                    &mut report,
                                    &peer.peer_id,
                                    &peer.peer_site_token,
                                    bucket,
                                    &entry.key,
                                    &local_id,
                                    max_object_bytes,
                                )
                                .await;
                            } else {
                                err(
                                    &mut report,
                                    bucket,
                                    &entry.key,
                                    &local_id,
                                    "mapped remote version missing",
                                );
                            }
                        }
                        Err(e) => err(&mut report, bucket, &entry.key, &local_id, e),
                    }
                    continue;
                }
                if let Some(other) = mapped {
                    let other_exists = remote_exists(
                        &client,
                        &peer.peer_id,
                        bucket,
                        &entry.key,
                        other,
                        &peer.peer_site_token,
                    )
                    .await
                    .unwrap_or(false);
                    if other_exists {
                        if opts.repair {
                            match store
                                .set_remote_version_id(bucket, &entry.key, &local_id, Some(other))
                                .await
                            {
                                Ok(()) => report.mappings_written += 1,
                                Err(e) => err(
                                    &mut report,
                                    bucket,
                                    &entry.key,
                                    &local_id,
                                    format!("map: {e}"),
                                ),
                            }
                        } else {
                            report.already_ok += 1;
                        }
                        continue;
                    }
                }
                // 无有效映射：本地 id 已在 → 写映射；否则重推同 id。
                let local_exists = remote_exists(
                    &client,
                    &peer.peer_id,
                    bucket,
                    &entry.key,
                    &local_id,
                    &peer.peer_site_token,
                )
                .await
                .unwrap_or(false);
                if local_exists {
                    if opts.repair {
                        match store
                            .set_remote_version_id(bucket, &entry.key, &local_id, Some(&local_id))
                            .await
                        {
                            Ok(()) => report.mappings_written += 1,
                            Err(e) => err(
                                &mut report,
                                bucket,
                                &entry.key,
                                &local_id,
                                format!("map: {e}"),
                            ),
                        }
                    } else {
                        report.already_ok += 1;
                    }
                } else if opts.repair {
                    push_version(
                        store,
                        &client,
                        &mut report,
                        &peer.peer_id,
                        &peer.peer_site_token,
                        bucket,
                        &entry.key,
                        &local_id,
                        max_object_bytes,
                    )
                    .await;
                } else {
                    err(
                        &mut report,
                        bucket,
                        &entry.key,
                        &local_id,
                        "no remote mapping and no same-id object",
                    );
                }
            }
            if !page.is_truncated {
                break;
            }
            key_marker = page.next_key_marker;
            version_marker = page.next_version_id_marker;
        }
    }
    if opts.repair && !report.has_errors() {
        let mut tx = db.pool().begin().await.map_err(|e| e.to_string())?;
        let intents: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_write_intents")
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        let deletes: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_delete_intents")
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        let outbox: u64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ?",
        )
        .bind(&peer.peer_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM sync_write_intents")
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM sync_delete_intents")
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ?")
            .bind(&peer.peer_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("UPDATE sites SET reconcile_required = 0 WHERE peer_id = ?")
            .bind(&peer.peer_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        report.intents_cleared = intents + deletes;
        report.outbox_cleared = outbox;
    }
    Ok(report)
}
