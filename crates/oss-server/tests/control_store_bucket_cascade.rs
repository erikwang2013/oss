//! Slice 4 Task 7：桶 seam 写路径必须保持 site 触发器语义。
//!
//! 断言：`bucket_create` 的 INSERT/UPDATE、`bucket_delete`、策略 upsert/delete
//! 产生的 changelog 与旧 raw SQL 一致；只改 deleting/default_encryption/
//! object_lock 默认值不产生条目；门控拒绝 stale 写；回滚丢弃行与 changelog。

mod common;

use common::state_with_site;
use oss_server::config::SiteToml;
use oss_server::control_store::SqliteControlTxn;
use oss_server::db::SitePeer;
use oss_server::state::AppState;
use serde_json::{Value, json};
use sqlx::Row;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketEntry, BucketTxn};

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn state(dir: &std::path::Path) -> AppState {
    let st = state_with_site(
        dir,
        Some(SiteToml {
            id: None,
            name: Some("a".into()),
            region: "us-east-1".into(),
            priority: 0,
            token: Some("a".repeat(32)),
        }),
    )
    .await;
    st.db
        .insert_site(&SitePeer {
            peer_id: "b".into(),
            remote_site_id: "remote-b".into(),
            name: "b".into(),
            endpoint: "http://127.0.0.1:9".into(),
            region: "us-east-1".into(),
            priority: 0,
            s3_access_key: "AKIATEST000000001".into(),
            s3_secret_key: "SKTEST000000000001".into(),
            peer_site_token: "b".repeat(32),
            created_at: now(),
            last_seen: None,
            last_error: None,
            raft_node_id: None,
            reconcile_required: false,
        })
        .await
        .unwrap();
    st
}

fn entry(name: &str, versioning: &str, created: i64) -> BucketEntry {
    BucketEntry {
        name: name.into(),
        owner: OwnerRef::Admin,
        creation_date: created,
        versioning: versioning.into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting: false,
    }
}

async fn cl(st: &AppState) -> Vec<(String, String, String, Value)> {
    let rows =
        sqlx::query("SELECT entity, entity_id, op, payload FROM site_changelog ORDER BY seq")
            .fetch_all(st.db.pool())
            .await
            .unwrap();
    rows.iter()
        .map(|r| {
            let p: String = r.get("payload");
            (
                r.get("entity"),
                r.get("entity_id"),
                r.get("op"),
                serde_json::from_str(&p).unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn bucket_create_update_and_delete_payloads_match_triggers() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;

    let mut tx = st.db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.bucket_create(&entry("b", "Off", 123)).await.unwrap();
    }
    tx.commit().await.unwrap();

    let rows = cl(&st).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        rows[0],
        (
            "bucket".into(),
            "b".into(),
            "upsert".into(),
            json!({
                "name": "b",
                "owner_username": null,
                "creation_date": 123,
                "versioning": "Off",
                "object_lock": 0
            })
        ),
        "INSERT 载荷逐字段复刻 site_log_bucket_ins"
    );

    let mut tx = st.db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        assert!(txn.bucket_set_versioning("b", "Enabled").await.unwrap());
    }
    tx.commit().await.unwrap();
    let rows = cl(&st).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].3["versioning"], "Enabled");
    assert_eq!(rows[1].3["creation_date"], 123, "UPDATE 不改 creation_date");

    // 只改未进触发器的列：不产生条目。
    let mut tx = st.db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.bucket_set_deleting("b", true).await.unwrap();
        txn.bucket_set_default_encryption("b", "AES256")
            .await
            .unwrap();
        txn.bucket_set_object_lock("b", false, Some("GOVERNANCE"), Some(9))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(cl(&st).await.len(), 2, "deleting/encryption/默认锁不复制");

    let mut tx = st.db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        assert!(txn.bucket_delete("b").await.unwrap());
    }
    tx.commit().await.unwrap();
    let rows = cl(&st).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[2],
        (
            "bucket".into(),
            "b".into(),
            "delete".into(),
            json!({"name": "b"})
        ),
        "delete 载荷只含 name"
    );
}

#[tokio::test]
async fn policy_upsert_and_delete_capture_events_and_rules_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;

    let mut tx = st.db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.bucket_create(&entry("b", "Off", 1)).await.unwrap();
    }
    tx.commit().await.unwrap();
    let base = cl(&st).await.len();

    let mut tx = st.db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.bucket_policy_set("b", "{\"v\":1}", 7).await.unwrap();
        txn.bucket_notification_set("b", "[]", 8).await.unwrap();
        txn.bucket_replication_rules_set("b", "[]", 9)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let rows = cl(&st).await;
    assert_eq!(rows.len(), base + 1, "只有策略有触发器：{rows:?}");
    assert_eq!(
        rows[base],
        (
            "bucket_policy".into(),
            "b".into(),
            "upsert".into(),
            json!({"bucket": "b", "policy_json": "{\"v\":1}", "updated_at": 7})
        )
    );

    let mut tx = st.db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.bucket_policy_delete("b").await.unwrap();
        txn.bucket_notification_delete("b").await.unwrap();
        txn.bucket_replication_rules_delete("b").await.unwrap();
    }
    tx.commit().await.unwrap();
    let rows = cl(&st).await;
    assert_eq!(rows.len(), base + 2);
    assert_eq!(rows[base + 1].0, "bucket_policy");
    assert_eq!(rows[base + 1].2, "delete");
}

#[tokio::test]
async fn stale_gate_rejects_write_through_txn() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;
    sqlx::query(
        "INSERT OR REPLACE INTO site_entity_clock \
         (entity, entity_id, ts, priority, origin_site_id, seq) \
         VALUES ('bucket', 'gated', ?, 0, 'remote-z', 99)",
    )
    .bind(now() + 120)
    .execute(st.db.pool())
    .await
    .unwrap();

    let mut tx = st.db.pool().begin().await.unwrap();
    let err = {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.bucket_create(&entry("gated", "Off", 1))
            .await
            .unwrap_err()
    };
    tx.rollback().await.unwrap();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    assert!(
        st.db
            .control_store()
            .bucket("gated")
            .await
            .unwrap()
            .is_none()
    );
    assert!(cl(&st).await.is_empty());
}

#[tokio::test]
async fn rollback_discards_bucket_rows_and_changelog() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await;

    let mut tx = st.db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.bucket_create(&entry("b", "Off", 1)).await.unwrap();
        txn.bucket_policy_set("b", "{}", 1).await.unwrap();
    }
    tx.rollback().await.unwrap();

    assert!(
        st.db.control_store().bucket("b").await.unwrap().is_none(),
        "回滚后桶行不存在"
    );
    assert!(
        st.db
            .control_store()
            .bucket_policy("b")
            .await
            .unwrap()
            .is_none()
    );
    assert!(cl(&st).await.is_empty(), "回滚后 triggers 无条目");
}
