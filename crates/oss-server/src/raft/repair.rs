//! M9 S7.5：`sync-reconcile [--repair]`——B′ 同 origin id 重推、映射刷新与本地 intent/outbox 清理。

use serde::Serialize;
use xl_store::store::Store;

use crate::config::RemoteTarget;
use crate::db::Db;
use crate::repl_client::ReplClient;

mod remote;

use remote::*;

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
    /// 只读模式：远端缺失的 delete marker 数量（未推送）。
    pub markers_missing: usize,
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
    let mut report = RepairReport {
        buckets: buckets.len(),
        ..RepairReport::default()
    };
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
                    match remote_head(
                        &client,
                        &peer.peer_id,
                        bucket,
                        &entry.key,
                        &local_id,
                        &peer.peer_site_token,
                    )
                    .await
                    {
                        Err(e) => err(&mut report, bucket, &entry.key, &local_id, e),
                        Ok(RemoteHead::Marker) => report.already_ok += 1,
                        Ok(RemoteHead::Object) => err(
                            &mut report,
                            bucket,
                            &entry.key,
                            &local_id,
                            "id collision: remote object exists with marker id",
                        ),
                        Ok(RemoteHead::Missing) => {
                            if !opts.repair {
                                report.markers_missing += 1;
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
                                    err(&mut report, bucket, &entry.key, &local_id, e.message)
                                }
                            }
                        }
                    }
                    continue;
                }
                let mapped = entry.remote_version_id.as_deref();
                if mapped == Some(local_id.as_str()) {
                    match remote_head(
                        &client,
                        &peer.peer_id,
                        bucket,
                        &entry.key,
                        &local_id,
                        &peer.peer_site_token,
                    )
                    .await
                    {
                        Err(e) => err(&mut report, bucket, &entry.key, &local_id, e),
                        Ok(RemoteHead::Object) => report.already_ok += 1,
                        Ok(RemoteHead::Marker) => err(
                            &mut report,
                            bucket,
                            &entry.key,
                            &local_id,
                            "remote marker exists with object id",
                        ),
                        Ok(RemoteHead::Missing) => {
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
                    }
                    continue;
                }
                let mut try_local = true;
                if let Some(other) = mapped {
                    match remote_head(
                        &client,
                        &peer.peer_id,
                        bucket,
                        &entry.key,
                        other,
                        &peer.peer_site_token,
                    )
                    .await
                    {
                        Ok(RemoteHead::Object) => {
                            if opts.repair {
                                match store
                                    .set_remote_version_id(
                                        bucket,
                                        &entry.key,
                                        &local_id,
                                        Some(other),
                                    )
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
                            try_local = false;
                        }
                        Ok(RemoteHead::Marker) => {
                            err(
                                &mut report,
                                bucket,
                                &entry.key,
                                &local_id,
                                "legacy mapping points at marker",
                            );
                            try_local = false;
                        }
                        Ok(RemoteHead::Missing) => {}
                        Err(e) => {
                            err(&mut report, bucket, &entry.key, &local_id, e);
                            try_local = false;
                        }
                    }
                }
                if !try_local {
                    continue;
                }
                // 无有效映射（或旧映射远端已缺失）：本地 id 已在 → 写映射；否则重推同 id。
                match remote_head(
                    &client,
                    &peer.peer_id,
                    bucket,
                    &entry.key,
                    &local_id,
                    &peer.peer_site_token,
                )
                .await
                {
                    Err(e) => err(&mut report, bucket, &entry.key, &local_id, e),
                    Ok(RemoteHead::Marker) => err(
                        &mut report,
                        bucket,
                        &entry.key,
                        &local_id,
                        "remote marker exists with object id",
                    ),
                    Ok(RemoteHead::Object) => {
                        if opts.repair {
                            match store
                                .set_remote_version_id(
                                    bucket,
                                    &entry.key,
                                    &local_id,
                                    Some(&local_id),
                                )
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
                    }
                    Ok(RemoteHead::Missing) => {
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
                                "no remote mapping and no same-id object",
                            );
                        }
                    }
                }
            }
            if !page.is_truncated {
                break;
            }
            key_marker = page.next_key_marker;
            version_marker = page.next_version_id_marker;
        }
    }
    // `--bucket` 只用于诊断：未验证其余 sync 桶前不得清站点级门/intents。
    if opts.repair && opts.bucket.is_none() && !report.has_errors() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Query;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::head;
    use std::collections::HashMap;

    /// C2 回归：内部 HEAD 的 200/405/404/5xx 必须映射为 Object/Marker/Missing/Err。
    #[tokio::test]
    async fn remote_head_maps_tristate_and_transport_errors() {
        let app = axum::Router::new().route(
            "/api/v1/site/head-object",
            head(|Query(q): Query<HashMap<String, String>>| async move {
                match q.get("version_id").map(String::as_str) {
                    Some("obj") => StatusCode::OK.into_response(),
                    Some("marker") => StatusCode::METHOD_NOT_ALLOWED.into_response(),
                    Some("boom") => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                    _ => StatusCode::NOT_FOUND.into_response(),
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client =
            ReplClient::single(&format!("http://{addr}"), "AK", "SK", "us-east-1").unwrap();
        let head = |vid: &'static str| {
            let client = &client;
            async move { remote_head(client, "site", "b", "k", vid, "tok").await }
        };
        assert_eq!(head("obj").await.unwrap(), RemoteHead::Object);
        assert_eq!(head("marker").await.unwrap(), RemoteHead::Marker);
        assert_eq!(head("gone").await.unwrap(), RemoteHead::Missing);
        assert!(head("boom").await.is_err(), "5xx 不得折叠成 Missing");
    }
}
