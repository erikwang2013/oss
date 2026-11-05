//! Shared helpers for the D2 trigger-vs-hook golden matrix.

use futures::future::BoxFuture;
use xl_store::control_store::ControlError;
use xl_store::control_store::sites::{SitePeerRow, SiteTxn};

use super::{local, real_store};
use crate::control_store_obj::site::SiteChangelogRow;
use crate::control_store_obj::{ObjectControlTxn, WriteOrigin};

pub(super) type Real = super::Real;

pub(super) fn se(e: ControlError) -> xl_store::error::Error {
    xl_store::error::Error::InvalidArgument(e.to_string())
}

pub(super) fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// SQLite side: one seeded config (priority 7) and one paired site.
pub(super) async fn sqlite_site() -> (crate::db::Db, String) {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    db.apply_site_config(Some("a"), Some("us-east-1"), Some(7), Some(&"t".repeat(32)))
        .await
        .unwrap();
    let id: String = sqlx::query_scalar("SELECT id FROM site_config LIMIT 1")
        .fetch_one(db.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, region, priority, \
         s3_access_key, s3_secret_key, peer_site_token, created_at) \
         VALUES ('peer-b', 'remote-b', 'b', 'http://127.0.0.1:9', 'us-east-1', 0, \
                 'ak', 'sk', 'tok', 0)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    (db, id)
}

/// Object side: matching config (priority 7) and paired site.
pub(super) async fn object_site(id: &str) -> Real {
    let real = real_store().await;
    let peer = SitePeerRow {
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
    let id = id.to_string();
    tx_once(&real, local(), |txn| {
        let id = id.clone();
        Box::pin(async move {
            txn.site_config_seed(&id, "tok", 1).await.map_err(se)?;
            txn.site_config_apply(None, None, Some(7), None)
                .await
                .map_err(se)?;
            txn.site_insert(&peer, 1).await.map_err(se)?;
            Ok(())
        })
    })
    .await
    .unwrap();
    real
}

/// Single-attempt transaction for a closure that is only run once.
pub(super) async fn tx_once<T, F>(
    real: &Real,
    origin: WriteOrigin,
    f: F,
) -> xl_store::error::Result<T>
where
    F: for<'a> FnOnce(&'a mut ObjectControlTxn) -> BoxFuture<'a, xl_store::error::Result<T>>,
{
    let mut txn = real.control.begin(origin).await?;
    let out = f(&mut txn).await?;
    txn.commit().await?;
    Ok(out)
}

pub(super) async fn run_origin<F>(
    real: &Real,
    origin: WriteOrigin,
    f: F,
) -> Result<(), ControlError>
where
    F: for<'a> FnOnce(&'a mut ObjectControlTxn) -> BoxFuture<'a, Result<(), ControlError>>,
{
    let mut txn = real
        .control
        .begin(origin)
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    f(&mut txn).await?;
    txn.commit()
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    Ok(())
}

pub(super) async fn run_hook<F>(real: &Real, f: F) -> Result<(), ControlError>
where
    F: for<'a> FnOnce(&'a mut ObjectControlTxn) -> BoxFuture<'a, Result<(), ControlError>>,
{
    run_origin(real, WriteOrigin::local("test"), f).await
}

pub(super) async fn sql_rows(db: &crate::db::Db) -> Vec<(String, String, String, String)> {
    sqlx::query_as("SELECT entity, entity_id, op, payload FROM site_changelog ORDER BY seq")
        .fetch_all(db.pool())
        .await
        .unwrap()
}

pub(super) async fn obj_rows(real: &Real) -> Vec<SiteChangelogRow> {
    real.control.site_changelog_after(0).await.unwrap()
}

pub(super) fn assert_rows(sql: &[(String, String, String, String)], obj: &[SiteChangelogRow]) {
    // SQLite side may contain seed-trigger rows that the object side did not
    // produce; require every object row to appear, in order, as a SQLite row.
    let mut at = 0;
    for (i, row) in obj.iter().enumerate() {
        let found = sql[at..].iter().position(|(entity, id, op, payload)| {
            row.entity == *entity && row.entity_id == *id && row.op == *op && row.payload == *payload
        });
        match found {
            Some(offset) => at += offset + 1,
            None => panic!("object row {i} absent from SQLite changelog: sql={sql:?} obj={obj:?}"),
        }
    }
}

/// Install the same future clock on both backends so the next hook gate rejects.
pub(super) async fn block(db: &crate::db::Db, real: &Real, entity: &str, id: &str) {
    let ts = now() + 3600;
    sqlx::query(
        "INSERT OR REPLACE INTO site_entity_clock \
         (entity, entity_id, ts, priority, origin_site_id, seq) \
         VALUES (?, ?, ?, 7, 'remote-z', 99)",
    )
    .bind(entity)
    .bind(id)
    .bind(ts)
    .execute(db.pool())
    .await
    .unwrap();
    let entity = entity.to_string();
    let id = id.to_string();
    run_hook(real, |txn| {
        let entity = entity.clone();
        let id = id.clone();
        Box::pin(async move { txn.site_put_clock(&entity, &id, ts, 7, "remote-z", 99).await })
    })
    .await
    .unwrap();
}

pub(super) fn nasty() -> String {
    "q\"b\\c\u{1}\u{8}\t\n\r\u{1f}é".into()
}
