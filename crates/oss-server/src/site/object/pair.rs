//! Object-backend M7 pairing: `sites` row + local snapshot in one root CAS.
//!
//! The snapshot only includes entities whose clock is missing or owned by this
//! site (`site.rs` does the same with a SQL join); remote values are never echoed
//! back. Cursors are node-local files, so they are created next to the CAS.

use std::collections::HashMap;

use xl_store::control_store::bucket::BucketPolicyEntry;
use xl_store::control_store::sites::{SiteChangelogRow, SiteIdentityRow, SiteStore, SiteTxn};

use crate::control_store_obj::jobs::{decode, encode, scan_txn};
use crate::control_store_obj::{
    AccessKeyRow, BucketRow, ObjectControlStore, ObjectControlTxn, TableId, WriteOrigin,
    changelog_key, decode_row, scan_permissions, scan_users,
};
use crate::db::SitePeer;

use super::super::bad;
use super::entity::read_clock;

type PairResult<T> = Result<T, String>;

/// `POST /sites` (non-Raft): insert the single peer and snapshot local rows.
pub(crate) async fn insert_peer_with_snapshot(
    control: &ObjectControlStore,
    is_raft: bool,
    peer: &SitePeer,
) -> PairResult<()> {
    let local = if is_raft {
        None
    } else {
        Some(
            control
                .site_identity()
                .await
                .map_err(|e| bad(e.to_string()))?
                .ok_or_else(|| bad("站点身份未初始化"))?,
        )
    };
    let mut txn = control
        .begin(WriteOrigin::local("site-pair"))
        .await
        .map_err(|e| bad(e.to_string()))?;
    // L9: the single-peer check must be inside the transaction that inserts.
    if !scan_txn(&txn, TableId::Sites)
        .await
        .map_err(|e| bad(e.to_string()))?
        .is_empty()
    {
        return Err("已存在站点对（M7 单 peer）".into());
    }
    txn.site_insert(&crate::db::site_peer_row(peer), crate::db::now())
        .await
        .map_err(|e| bad(e.to_string()))?;
    if let Some(local) = &local {
        // M9 S6d：Raft 配对只写 sites 行（快照/游标由 S7 流程负责）。
        snapshot(&mut txn, local).await?;
        // SQLite writes this row in the same transaction; the object backend
        // keeps it node-local, so create it before the CAS commit.
        let cursor = control
            .local_store()
            .map_err(|e| bad(e.to_string()))?
            .cursor(&peer.peer_id)
            .map_err(|e| bad(e.to_string()))?;
        if cursor.load().map_err(|e| bad(e.to_string()))?.is_none() {
            cursor
                .store(0, crate::db::now())
                .map_err(|e| bad(e.to_string()))?;
        }
    }
    txn.commit().await.map_err(|e| bad(e.to_string()))?;
    Ok(())
}

async fn snapshot(txn: &mut ObjectControlTxn, local: &SiteIdentityRow) -> PairResult<()> {
    let ts = crate::db::now();
    let users = scan_users(txn).await.map_err(|e| bad(e.to_string()))?;
    let by_uid: HashMap<i64, (String, bool)> = users
        .iter()
        .map(|u| (u.uid, (u.username.clone(), u.is_admin)))
        .collect();
    for u in &users {
        if u.is_admin {
            continue;
        }
        let payload = serde_json::json!({
            "username": u.username,
            "password_hash": u.password_hash,
            "status": u.status,
            "created_at": u.created_at,
        });
        snapshot_entry(txn, local, ts, "user", &u.username, &payload).await?;
    }
    for (_, value) in scan_txn(txn, TableId::AccessKeys)
        .await
        .map_err(|e| bad(e.to_string()))?
    {
        let row: AccessKeyRow = decode(&value).map_err(|e| bad(e.to_string()))?;
        if row.kind != "long-term" {
            continue;
        }
        let Some((username, admin)) = by_uid.get(&row.user_id) else {
            continue;
        };
        if *admin {
            continue;
        }
        let payload = serde_json::json!({
            "access_key": row.access_key,
            "secret_key": row.secret_key,
            "username": username,
            "status": row.status,
            "expires_at": row.expires_at,
            "created_at": row.created_at,
        });
        snapshot_entry(txn, local, ts, "access_key", &row.access_key, &payload).await?;
    }
    for p in scan_permissions(txn)
        .await
        .map_err(|e| bad(e.to_string()))?
    {
        let Some((username, admin)) = by_uid.get(&p.user_id) else {
            continue;
        };
        if *admin {
            continue;
        }
        let eid = format!("{username}\u{1f}{}", p.bucket);
        let payload = serde_json::json!({
            "username": username,
            "bucket": p.bucket,
            "can_read": i64::from(p.can_read),
            "can_write": i64::from(p.can_write),
            "can_delete": i64::from(p.can_delete),
        });
        snapshot_entry(txn, local, ts, "permission", &eid, &payload).await?;
    }
    for (_, value) in scan_txn(txn, TableId::Buckets)
        .await
        .map_err(|e| bad(e.to_string()))?
    {
        let row: BucketRow = decode_row(&value).map_err(|e| bad(e.to_string()))?;
        let owner_username = row
            .owner_uid
            .and_then(|uid| by_uid.get(&uid).map(|(name, _)| name.clone()));
        let payload = serde_json::json!({
            "name": row.name,
            "owner_username": owner_username,
            "creation_date": row.creation_date,
            "versioning": row.versioning,
            "object_lock": i64::from(row.object_lock),
        });
        snapshot_entry(txn, local, ts, "bucket", &row.name, &payload).await?;
    }
    for (_, value) in scan_txn(txn, TableId::BucketPolicies)
        .await
        .map_err(|e| bad(e.to_string()))?
    {
        let row: BucketPolicyEntry = decode_row(&value).map_err(|e| bad(e.to_string()))?;
        let payload = serde_json::json!({
            "bucket": row.bucket,
            "policy_json": row.policy_json,
            "updated_at": row.updated_at,
        });
        snapshot_entry(txn, local, ts, "bucket_policy", &row.bucket, &payload).await?;
    }
    Ok(())
}

async fn snapshot_entry(
    txn: &mut ObjectControlTxn,
    local: &SiteIdentityRow,
    ts: i64,
    entity: &str,
    entity_id: &str,
    payload: &serde_json::Value,
) -> PairResult<()> {
    if read_clock(txn, entity, entity_id)
        .await?
        .is_some_and(|clock| clock.2 != local.id)
    {
        return Ok(());
    }
    let seq = txn.alloc_site_seq().map_err(|e| bad(e.to_string()))?;
    let row = SiteChangelogRow {
        seq,
        origin_site_id: local.id.clone(),
        entity: entity.into(),
        entity_id: entity_id.into(),
        op: "upsert".into(),
        payload: payload.to_string(),
        priority: local.priority,
        ts,
        created_at: ts,
    };
    txn.put(
        TableId::SiteChangelog,
        &changelog_key(seq),
        &encode(&row).map_err(|e| bad(e.to_string()))?,
    )
    .await
    .map_err(|e| bad(e.to_string()))?;
    txn.site_put_clock(entity, entity_id, ts, local.priority, &local.id, seq)
        .await
        .map_err(|e| bad(e.to_string()))
}
