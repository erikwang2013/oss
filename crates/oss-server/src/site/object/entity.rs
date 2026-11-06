//! Per-entity apply handlers (bucket, policy, permission, user, access key).

use std::collections::BTreeMap;

use xl_store::control_store::bucket::BucketTxn;
use xl_store::control_store::iam::{IamTxn, UserEntry};
use xl_store::store::Store;

use crate::control_store_obj::bucket_clear_owner;
use crate::control_store_obj::jobs::decode;
use crate::control_store_obj::{
    AccessPointTxn, BucketDomainCleanup, BucketRow, ObjectControlTxn, SiteEntityClockRow, TableId,
    clock_key, decode_row, encode_row, key1, permission_key, scan_permissions, scan_users,
};

use super::super::{Entry, bad};

type ApplyResult<T> = Result<T, String>;

pub(super) fn as_str(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(str::to_string)
}

pub(super) fn as_i64(v: &serde_json::Value, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| x.as_i64())
}

pub(super) async fn read_clock(
    txn: &ObjectControlTxn,
    entity: &str,
    entity_id: &str,
) -> ApplyResult<Option<(i64, i64, String, i64)>> {
    match txn
        .get(TableId::SiteEntityClock, &clock_key(entity, entity_id))
        .await
        .map_err(|e| bad(e.to_string()))?
    {
        Some(bytes) => {
            let c: SiteEntityClockRow = decode(&bytes).map_err(|e| bad(e.to_string()))?;
            Ok(Some((c.ts, c.priority, c.origin_site_id, c.seq)))
        }
        None => Ok(None),
    }
}

/// True when the stored clock beats this entry (L7 cascade guard).
async fn clock_beats(
    txn: &ObjectControlTxn,
    entity: &str,
    entity_id: &str,
    e: &Entry,
) -> ApplyResult<bool> {
    Ok(read_clock(txn, entity, entity_id)
        .await?
        .is_some_and(|c| c > (e.ts, e.priority, e.origin_site_id.clone(), e.seq)))
}

pub(super) async fn apply_bucket(
    txn: &mut ObjectControlTxn,
    store: &Store,
    e: &Entry,
) -> ApplyResult<Result<(), String>> {
    if let Err(err) = xl_store::layout::validate_bucket(&e.entity_id) {
        return Ok(Err(format!("非法桶名：{err}")));
    }
    if e.op == "delete" {
        match store.delete_bucket(&e.entity_id).await {
            Ok(()) | Err(xl_store::error::Error::NoSuchBucket(_)) => {
                txn.bucket_delete(&e.entity_id)
                    .await
                    .map_err(|err| bad(err.to_string()))?;
                txn.bucket_notification_delete(&e.entity_id)
                    .await
                    .map_err(|err| bad(err.to_string()))?;
                txn.bucket_replication_rules_delete(&e.entity_id)
                    .await
                    .map_err(|err| bad(err.to_string()))?;
                // L7: a newer remote policy/permission survives an older bucket delete.
                if !clock_beats(txn, "bucket_policy", &e.entity_id, e).await? {
                    txn.bucket_policy_delete(&e.entity_id)
                        .await
                        .map_err(|err| bad(err.to_string()))?;
                }
                let usernames: BTreeMap<i64, String> = scan_users(txn)
                    .await
                    .map_err(|err| bad(err.to_string()))?
                    .into_iter()
                    .map(|u| (u.uid, u.username))
                    .collect();
                for perm in scan_permissions(txn)
                    .await
                    .map_err(|err| bad(err.to_string()))?
                {
                    if perm.bucket != e.entity_id {
                        continue;
                    }
                    let username = usernames.get(&perm.user_id).cloned().unwrap_or_default();
                    let id = format!("{username}\u{1f}{}", perm.bucket);
                    if !clock_beats(txn, "permission", &id, e).await? {
                        txn.delete(
                            TableId::Permissions,
                            &permission_key(perm.user_id, &perm.bucket),
                        )
                        .await
                        .map_err(|err| bad(err.to_string()))?;
                    }
                }
                txn.delete_access_points_for_bucket(&e.entity_id)
                    .await
                    .map_err(|err| bad(err.to_string()))?;
                txn.delete_lifecycle_inventory_tier_for_bucket(&e.entity_id)
                    .await
                    .map_err(|err| bad(err.to_string()))?;
                Ok(Ok(()))
            }
            Err(xl_store::error::Error::BucketNotEmpty(_)) => Ok(Err("桶非空，删除未应用".into())),
            Err(err) => Err(bad(format!("删桶失败（可重试）：{err}"))),
        }
    } else {
        let owner_uid = match as_str(&e.payload, "owner_username") {
            Some(username) => match txn
                .user_by_username(&username)
                .await
                .map_err(|err| bad(err.to_string()))?
            {
                Some(user) if !user.is_admin => Some(user.uid),
                _ => None, // admin / unknown user rows are not replicated as owners
            },
            None => None,
        };
        let key = key1(&e.entity_id);
        let mut row = match txn
            .get(TableId::Buckets, &key)
            .await
            .map_err(|err| bad(err.to_string()))?
        {
            Some(bytes) => decode_row(&bytes).map_err(|err| bad(err.to_string()))?,
            None => BucketRow {
                name: e.entity_id.clone(),
                owner_uid: None,
                set_id: 0,
                creation_date: 0,
                versioning: "Off".into(),
                object_lock: false,
                default_encryption: None,
                object_lock_default_mode: None,
                object_lock_default_days: None,
                replication_mode: "async".into(),
                deleting: false,
            },
        };
        row.owner_uid = owner_uid;
        row.creation_date = as_i64(&e.payload, "creation_date").unwrap_or(0);
        row.versioning = as_str(&e.payload, "versioning").unwrap_or_else(|| "Off".into());
        row.object_lock = as_i64(&e.payload, "object_lock").unwrap_or(0) != 0;
        txn.put(
            TableId::Buckets,
            &key,
            &encode_row(&row).map_err(|e| bad(e.to_string()))?,
        )
        .await
        .map_err(|err| bad(err.to_string()))?;
        Ok(Ok(()))
    }
}

pub(super) async fn apply_policy(
    txn: &mut ObjectControlTxn,
    e: &Entry,
) -> ApplyResult<Result<(), String>> {
    if e.op == "delete" {
        txn.bucket_policy_delete(&e.entity_id)
            .await
            .map_err(|err| bad(err.to_string()))?;
    } else {
        let Some(policy) = as_str(&e.payload, "policy_json") else {
            return Ok(Err("policy_json 缺失".into()));
        };
        txn.bucket_policy_set(
            &e.entity_id,
            &policy,
            as_i64(&e.payload, "updated_at").unwrap_or(0),
        )
        .await
        .map_err(|err| bad(err.to_string()))?;
    }
    Ok(Ok(()))
}

pub(super) async fn apply_permission(
    txn: &mut ObjectControlTxn,
    e: &Entry,
) -> ApplyResult<Result<(), String>> {
    let Some(username) = as_str(&e.payload, "username") else {
        return Ok(Err("username 缺失".into()));
    };
    let Some(user) = txn
        .user_by_username(&username)
        .await
        .map_err(|err| bad(err.to_string()))?
    else {
        return Ok(Err("未知用户（授权跳过）".into()));
    };
    if user.is_admin {
        return Ok(Err("目标为本地管理员（授权跳过）".into()));
    }
    if e.op == "delete" {
        let bucket = as_str(&e.payload, "bucket")
            .unwrap_or_else(|| e.entity_id.split('\u{1f}').nth(1).unwrap_or("").to_string());
        txn.permission_delete(user.uid, &bucket)
            .await
            .map_err(|err| bad(err.to_string()))?;
    } else {
        txn.permission_upsert(
            user.uid,
            &as_str(&e.payload, "bucket").unwrap_or_default(),
            as_i64(&e.payload, "can_read").unwrap_or(0) != 0,
            as_i64(&e.payload, "can_write").unwrap_or(0) != 0,
            as_i64(&e.payload, "can_delete").unwrap_or(0) != 0,
        )
        .await
        .map_err(|err| bad(err.to_string()))?;
    }
    Ok(Ok(()))
}

pub(super) async fn apply_user(
    txn: &mut ObjectControlTxn,
    e: &Entry,
    deleted_uids: &mut Vec<i64>,
) -> ApplyResult<Result<(), String>> {
    let existing = txn
        .user_by_username(&e.entity_id)
        .await
        .map_err(|err| bad(err.to_string()))?;
    if e.op == "delete" {
        let Some(user) = existing else {
            return Ok(Ok(()));
        };
        if user.is_admin {
            return Ok(Err("拒绝删除本地管理员".into()));
        }
        txn.user_delete_access_keys_permissions(user.uid)
            .await
            .map_err(|err| bad(err.to_string()))?;
        bucket_clear_owner(txn, user.uid)
            .await
            .map_err(|err| bad(err.to_string()))?;
        txn.user_delete(user.uid)
            .await
            .map_err(|err| bad(err.to_string()))?;
        deleted_uids.push(user.uid);
        return Ok(Ok(()));
    }
    if existing.is_none() {
        let Some(hash) = as_str(&e.payload, "password_hash") else {
            return Ok(Err("password_hash 缺失".into()));
        };
        txn.user_create(&UserEntry {
            username: e.entity_id.clone(),
            password_hash: hash,
            status: as_str(&e.payload, "status").unwrap_or_else(|| "active".into()),
            created_at: as_i64(&e.payload, "created_at").unwrap_or(0),
        })
        .await
        .map_err(|err| bad(err.to_string()))?;
        return Ok(Ok(()));
    }
    let user = existing.expect("checked above");
    if user.is_admin {
        return Ok(Err("目标是本地管理员（不覆盖）".into()));
    }
    if let Some(hash) = as_str(&e.payload, "password_hash") {
        txn.user_set_password(user.uid, &hash)
            .await
            .map_err(|err| bad(err.to_string()))?;
    }
    if let Some(status) = as_str(&e.payload, "status") {
        txn.user_set_status_value(user.uid, &status)
            .await
            .map_err(|err| bad(err.to_string()))?;
    }
    Ok(Ok(()))
}

pub(super) async fn apply_access_key(
    txn: &mut ObjectControlTxn,
    e: &Entry,
) -> ApplyResult<Result<(), String>> {
    if e.op == "delete" {
        let admin = txn
            .access_key_long_term_owner_is_admin(&e.entity_id)
            .await
            .map_err(|err| bad(err.to_string()))?;
        if admin {
            return Ok(Err("目标是本地管理员 AK（删除跳过）".into()));
        }
        txn.access_key_delete_long_term(&e.entity_id)
            .await
            .map_err(|err| bad(err.to_string()))?;
        return Ok(Ok(()));
    }
    let Some(username) = as_str(&e.payload, "username") else {
        return Ok(Err("username 缺失".into()));
    };
    let Some(user) = txn
        .user_by_username(&username)
        .await
        .map_err(|err| bad(err.to_string()))?
    else {
        return Ok(Err("未知用户（AK 跳过）".into()));
    };
    if user.is_admin {
        return Ok(Err("目标是本地管理员（AK 跳过）".into()));
    }
    if let Some((owner, kind)) = txn
        .access_key_user_and_kind(&e.entity_id)
        .await
        .map_err(|err| bad(err.to_string()))?
        && (owner != user.uid || kind != "long-term")
    {
        return Ok(Err("同 AK 属主/类型冲突（不静默改属）".into()));
    }
    let Some(secret) = as_str(&e.payload, "secret_key") else {
        return Ok(Err("secret_key 缺失".into()));
    };
    txn.access_key_site_upsert(
        &e.entity_id,
        &secret,
        user.uid,
        &as_str(&e.payload, "status").unwrap_or_else(|| "active".into()),
        as_i64(&e.payload, "expires_at"),
        as_i64(&e.payload, "created_at").unwrap_or(0),
    )
    .await
    .map_err(|err| bad(err.to_string()))?;
    Ok(Ok(()))
}
