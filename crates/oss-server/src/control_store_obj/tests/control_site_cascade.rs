//! E2: user-delete cascade, composed from the same public hooks and IAM/account
//! mutations the façade runs, pinned by the frozen SQLite cascade golden.

use serde_json::json;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketEntry, BucketTxn};
use xl_store::control_store::iam::{IamTxn, UserEntry};
use xl_store::control_store::sites::SiteTxn;

use super::control_site_triggers::{PASSWORD_HASH, SITE_ID, USER, clock_rows, obj_rows};
use super::golden;
use super::site_common::se;
use super::support;
use crate::control_store_obj::bucket::{BucketRow, decode_row, key1};
use crate::control_store_obj::{TableId, WriteOrigin};
use crate::local_store::UploadRecord;

#[tokio::test]
async fn user_delete_cascade_matches_golden() {
    // The SQLite capture pairs the peer after seeding the rows, so setup writes
    // do not emit changelog entries; mirror that ordering.
    let real = support::object_store(xl_store::control_state::root::Mode::Standalone).await;
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

    // Pair after the fixture rows exist (same as the SQLite capture).
    let peer = xl_store::control_store::sites::SitePeerRow {
        peer_id: "peer-b".into(),
        remote_site_id: "remote-b".into(),
        name: "b".into(),
        endpoint: "http://127.0.0.1:9".into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "ak".into(),
        s3_secret_key: "sk".into(),
        peer_site_token: "tok".into(),
        created_at: 0,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    };
    real.control
        .transaction(WriteOrigin::local("seed"), |txn| {
            let peer = peer.clone();
            Box::pin(async move {
                txn.site_config_seed(SITE_ID, "tok", 1).await.map_err(se)?;
                txn.site_config_apply(None, None, Some(7), None)
                    .await
                    .map_err(se)?;
                txn.site_insert(&peer, 1).await.map_err(se)?;
                Ok(())
            })
        })
        .await
        .unwrap();

    real.control
        .transaction(WriteOrigin::local("test"), |txn| {
            Box::pin(async move {
                txn.access_key_delete("AKCASCADE-GOLDEN")
                    .await
                    .map_err(se)?;
                txn.permission_delete(uid, "cascade-b").await.map_err(se)?;
                txn.bucket_clear_owner(uid).await.map_err(se)?;
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
            assert!(r.ts > 0 && r.created_at == r.ts);
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
