//! Object-backend M7 shipping round (no SQLite).

use xl_store::control_store::sites::{SiteStore, SiteTxn};

use crate::control_store_obj::{ObjectControlStore, WriteOrigin};

use super::super::{MAX_BATCH_BYTES, bad};

/// One shipping round over the control seam and the node-local cursor files.
pub(crate) async fn ship_once(
    control: &ObjectControlStore,
    tls: Option<&crate::tls::Tls>,
) -> Result<(usize, usize), String> {
    let peers = control.sites().await.map_err(|err| bad(err.to_string()))?;
    let (mut shipped_peers, mut shipped_entries) = (0usize, 0usize);
    for peer in peers {
        let cursor_file = control
            .local_store()
            .map_err(|err| bad(err.to_string()))?
            .cursor(&peer.peer_id)
            .map_err(|err| bad(err.to_string()))?;
        let cursor = cursor_file
            .load()
            .map_err(|err| bad(err.to_string()))?
            .map(|c| c.last_seq)
            .unwrap_or(0);
        let rows = control
            .site_changelog_after(cursor)
            .await
            .map_err(|err| bad(err.to_string()))?;
        let rows: Vec<_> = rows.into_iter().take(100).collect();
        if rows.is_empty() {
            continue;
        }
        let mut max_seq = cursor;
        let mut batch_bytes = 0usize;
        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            let payload = serde_json::from_str::<serde_json::Value>(&row.payload).map_err(|e| {
                bad(format!(
                    "changelog seq {} payload 非 JSON（拒绝推进 cursor）：{e}",
                    row.seq
                ))
            })?;
            if !entries.is_empty() && batch_bytes + row.payload.len() + 512 > MAX_BATCH_BYTES {
                break;
            }
            batch_bytes += row.payload.len() + 512;
            max_seq = max_seq.max(row.seq);
            entries.push(serde_json::json!({
                "seq": row.seq,
                "origin_site_id": row.origin_site_id,
                "entity": row.entity,
                "entity_id": row.entity_id,
                "op": row.op,
                "payload": payload,
                "priority": row.priority,
                "ts": row.ts,
            }));
        }
        let body = serde_json::json!({ "entries": entries }).to_string();
        match crate::site_client::post_changelog_tls(
            &peer.endpoint,
            &peer.peer_site_token,
            bytes::Bytes::from(body),
            tls,
        )
        .await
        {
            Ok((200, _)) => {
                cursor_file
                    .store(max_seq, crate::db::now())
                    .map_err(|err| bad(err.to_string()))?;
                let _ = set_last_error(control, &peer.peer_id, None).await;
                shipped_peers += 1;
                shipped_entries += entries.len();
            }
            Ok((status, body)) => {
                let msg = format!(
                    "changelog 出货 {status}：{}",
                    String::from_utf8_lossy(&body)
                        .chars()
                        .take(200)
                        .collect::<String>()
                );
                let _ = set_last_error(control, &peer.peer_id, Some(&msg)).await;
                tracing::warn!(%msg, peer = %peer.peer_id, "changelog 出货失败（下轮重试）");
            }
            Err(err) => {
                let _ = set_last_error(control, &peer.peer_id, Some(&err)).await;
                tracing::warn!(error = %err, peer = %peer.peer_id, "changelog 出货失败（下轮重试）");
            }
        }
    }
    Ok((shipped_peers, shipped_entries))
}

async fn set_last_error(
    control: &ObjectControlStore,
    peer_id: &str,
    error: Option<&str>,
) -> Result<(), String> {
    let mut txn = control
        .begin(WriteOrigin::local("site-ship"))
        .await
        .map_err(|err| bad(err.to_string()))?;
    txn.site_set_last_error(peer_id, error)
        .await
        .map_err(|err| bad(err.to_string()))?;
    txn.commit().await.map_err(|err| bad(err.to_string()))?;
    Ok(())
}
