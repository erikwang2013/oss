//! E2: user-delete cascade, composed from the same public hooks and IAM/account
//! mutations the façade runs, pinned by the frozen SQLite cascade golden.

use serde_json::json;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketEntry, BucketTxn};
use xl_store::control_store::iam::{IamTxn, UserEntry};

use super::control_site_triggers::{
    PASSWORD_HASH, USER, clock_rows, obj_rows, standalone_site, user,
};
use super::golden;
use super::site_common::{now, se};
use super::support;
use crate::control_store_obj::bucket::{BucketRow, decode_row, encode_row, key1};
use crate::control_store_obj::site_apply::{AccessKeySiteRow, BucketSiteRow, PermissionSiteRow};
use crate::control_store_obj::{TableId, WriteOrigin};
use crate::local_store::UploadRecord;

#[tokio::test]
async fn user_delete_cascade_matches_golden() {
    let real = standalone_site().await;
    let uid = support::object_iam(&real.control, WriteOrigin::local("seed"), |t| {
        Box::pin(async move {
            t.user_create(&UserEntry {
                username: USER.into(),
                password_hash: PASSWORD_HASH.into(),
                status: "active".into(),
                created_at: 111,
            })
            .await
        })
    })
    .await
    .unwrap();
    real.control
        .transaction(WriteOrigin::local("seed"), |txn| {
            Box::pin(async move {
                txn.bucket_create(&BucketEntry {
                    name: "cascade-b".into(),
                    owner: OwnerRef::User(USER.into()),
                    creation_date: 1,
                    versioning: "Off".into(),
                    object_lock: false,
                    default_encryption: None,
                    object_lock_default_mode: None,
                    object_lock_default_days: None,
                    replication_mode: "async".into(),
                    deleting: false,
                })
                .await
                .map_err(se)?;
                txn.permission_upsert(uid, "cascade-b", true, true, true)
                    .await
                    .map_err(se)?;
                txn.access_key_create_legacy("AKCASCADE-GOLDEN", "SK-CAS", uid, 559)
                    .await
                    .map_err(se)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let uploads = real.control.local_store().unwrap().uploads().unwrap();
    uploads
        .put(&UploadRecord {
            saved_path: "p/q/r".into(),
            bucket: "cascade-b".into(),
            key: "k".into(),
            group_name: "file".into(),
            size: 1,
            etag: "e".into(),
            original_name: Some("n".into()),
            owner_uid: Some(uid),
            created_at: 2,
            expires_at: None,
        })
        .unwrap();

    let ts = now();
    real.control
        .transaction(WriteOrigin::local("test"), |txn| {
            Box::pin(async move {
                let ak = AccessKeySiteRow {
                    access_key: "AKCASCADE-GOLDEN".into(),
                    secret_key: "SK-CAS".into(),
                    username: Some(USER.into()),
                    status: "active".into(),
                    expires_at: None,
                    created_at: 559,
                    kind: "long-term".into(),
                    owner_is_admin: Some(false),
                };
                txn.site_hook_access_key(Some(&ak), None, ts)
                    .await
                    .map_err(se)?;
                txn.access_key_delete("AKCASCADE-GOLDEN")
                    .await
                    .map_err(se)?;
                let perm = PermissionSiteRow {
                    username: Some(USER.into()),
                    bucket: "cascade-b".into(),
                    can_read: true,
                    can_write: true,
                    can_delete: true,
                    owner_is_admin: Some(false),
                };
                txn.site_hook_permission(Some(&perm), None, ts)
                    .await
                    .map_err(se)?;
                txn.permission_delete(uid, "cascade-b").await.map_err(se)?;
                let old = BucketSiteRow {
                    name: "cascade-b".into(),
                    owner_uid: Some(uid),
                    owner_username: Some(USER.into()),
                    creation_date: 1,
                    versioning: "Off".into(),
                    object_lock: false,
                };
                let new = BucketSiteRow {
                    owner_uid: None,
                    owner_username: None,
                    ..old.clone()
                };
                txn.site_hook_bucket(Some(&old), Some(&new), ts)
                    .await
                    .map_err(se)?;
                let key = key1("cascade-b");
                let bytes = txn.get(TableId::Buckets, &key).await?.unwrap();
                let mut row: BucketRow = decode_row(&bytes).map_err(se)?;
                row.owner_uid = None;
                let bytes = encode_row(&row).map_err(se)?;
                txn.put(TableId::Buckets, &key, &bytes).await?;
                txn.site_hook_user(Some(&user("active")), None, ts)
                    .await
                    .map_err(se)?;
                txn.user_delete(uid).await.map_err(se)?;
                Ok(())
            })
        })
        .await
        .unwrap();

    // The delete-user façade also clears the upload owner in the local store.
    let mut upload = uploads.get("p/q/r").unwrap().unwrap();
    upload.owner_uid = None;
    uploads.put(&upload).unwrap();

    let rows = obj_rows(&real).await;
    let order: Vec<String> = rows
        .iter()
        .map(|r| format!("{}.{}", r.entity, r.op))
        .collect();
    let changelog: Vec<_> = rows
        .iter()
        .map(|r| {
            assert!(r.ts > 0 && r.ts == ts && r.created_at == ts);
            json!({
                "seq": r.seq,
                "origin_site_id": &r.origin_site_id,
                "entity": &r.entity,
                "entity_id": &r.entity_id,
                "op": &r.op,
                "payload": &r.payload,
                "priority": r.priority,
                "ts": 0,
                "created_at": 0,
            })
        })
        .collect();
    let clocks: Vec<_> = clock_rows(&real)
        .await
        .iter()
        .map(|c| {
            json!({
                "entity": &c.entity,
                "entity_id": &c.entity_id,
                "ts": 0,
                "priority": c.priority,
                "origin_site_id": &c.origin_site_id,
                "seq": c.seq,
            })
        })
        .collect();
    let bucket_owner: Option<i64> = {
        let txn = real
            .control
            .begin(WriteOrigin::local("read"))
            .await
            .unwrap();
        let bytes = txn
            .get(TableId::Buckets, &key1("cascade-b"))
            .await
            .unwrap()
            .unwrap();
        decode_row::<BucketRow>(&bytes).unwrap().owner_uid
    };
    let actual = json!({
        "order": order,
        "changelog": changelog,
        "clock": clocks,
        "final": {
            "user_rows": support::object_users(&real.control).await.len(),
            "access_key_rows": support::object_keys(&real.control).await.len(),
            "permission_rows": support::object_perms(&real.control).await.len(),
            "bucket_owner_id": bucket_owner,
            "upload_owner_id": uploads.get("p/q/r").unwrap().unwrap().owner_uid,
        },
    });
    golden::assert_json("site_triggers/user_delete_cascade.json", &actual);
}
