//! sync intent parity: insert/status/tee/recovery scans and delete-intent
//! ordering/limits. The object backend stores intents as KV records keyed by id.

use std::fmt::Debug;

use xl_store::control_store::ControlError;
use xl_store::control_store::sync_intents::{
    NewSyncDeleteIntent, NewSyncWriteIntent, SyncDeleteIntentRow, SyncIntentStore,
    SyncWriteIntentRow,
};

use super::support::{Boxed, Obj, Ops, both_ops, same_err, same_ok, sqlite_store};
use crate::db::Db;

async fn w<T, F>(db: &Db, obj: &Obj, f: F) -> T
where
    T: PartialEq + Debug,
    F: for<'a> FnOnce(&'a mut dyn Ops) -> Boxed<'a, T> + Copy,
{
    same_ok(both_ops(db, obj, f).await)
}

fn write_row(id: &str, updated_at: i64) -> NewSyncWriteIntent {
    NewSyncWriteIntent {
        id: id.into(),
        bucket: "b".into(),
        key: format!("k/{id}"),
        local_version_id: format!("lv-{id}"),
        remote_version_id: format!("rv-{id}"),
        tee_path: format!("/tee/{id}"),
        commit_json: "{}".into(),
        state: "pending".into(),
        created_at: 1,
        updated_at,
    }
}

fn delete_row(id: &str, updated_at: i64) -> NewSyncDeleteIntent {
    NewSyncDeleteIntent {
        id: id.into(),
        bucket: "b".into(),
        key: format!("k/{id}"),
        op: "delete".into(),
        local_version_id: String::new(),
        remote_version_id: format!("rv-{id}"),
        marker_version_id: String::new(),
        state: "pending".into(),
        created_at: 1,
        updated_at,
    }
}

async fn insert_write(db: &Db, obj: &Obj, row: &NewSyncWriteIntent) {
    w(db, obj, move |t| {
        let row = row.clone();
        Box::pin(async move { t.sync_write_intent_insert(&row).await })
    })
    .await;
}

async fn insert_delete(db: &Db, obj: &Obj, row: &NewSyncDeleteIntent) {
    w(db, obj, move |t| {
        let row = row.clone();
        Box::pin(async move { t.sync_delete_intent_insert(&row).await })
    })
    .await;
}

async fn write_stale(db: &Db, obj: &Obj, cutoff: i64) -> Vec<SyncWriteIntentRow> {
    let mut a = sqlite_store(db)
        .sync_write_intents_stale(cutoff)
        .await
        .unwrap();
    let mut b = obj.control.sync_write_intents_stale(cutoff).await.unwrap();
    a.sort_by(|x, y| x.id.cmp(&y.id));
    b.sort_by(|x, y| x.id.cmp(&y.id));
    assert_eq!(a, b);
    a
}

async fn tees(db: &Db, obj: &Obj) -> Vec<String> {
    let mut a = sqlite_store(db).sync_write_intent_tees().await.unwrap();
    let mut b = obj.control.sync_write_intent_tees().await.unwrap();
    a.sort();
    b.sort();
    assert_eq!(a, b);
    a
}

async fn delete_stale(db: &Db, obj: &Obj, cutoff: i64, limit: i64) -> Vec<SyncDeleteIntentRow> {
    let a = sqlite_store(db)
        .sync_delete_intents_stale(cutoff, limit)
        .await
        .unwrap();
    let b = obj
        .control
        .sync_delete_intents_stale(cutoff, limit)
        .await
        .unwrap();
    assert_eq!(a, b);
    a
}

#[tokio::test]
async fn write_intent_recovery_scan_tee_and_lifecycle() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    insert_write(&db, &obj, &write_row("a", 100)).await;
    insert_write(&db, &obj, &write_row("b", 300)).await;
    insert_write(&db, &obj, &write_row("c", 200)).await;

    let stale = write_stale(&db, &obj, 250).await;
    assert_eq!(
        stale.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["a", "c"]
    );

    let tee = sqlite_store(&db).sync_write_intent_tee("b").await.unwrap();
    let tee_obj = obj.control.sync_write_intent_tee("b").await.unwrap();
    assert_eq!(tee, tee_obj);
    assert_eq!(tee.as_deref(), Some("/tee/b"));
    assert_eq!(
        sqlite_store(&db)
            .sync_write_intent_tee("missing")
            .await
            .unwrap(),
        obj.control.sync_write_intent_tee("missing").await.unwrap()
    );
    assert_eq!(tees(&db, &obj).await, ["/tee/a", "/tee/b", "/tee/c"]);

    let done = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_write_intent_mark_done("a", 400).await })
    })
    .await;
    assert_eq!(done, 1);
    let missing = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_write_intent_mark_done("missing", 400).await })
    })
    .await;
    assert_eq!(missing, 0);

    let count = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_write_intent_count().await })
    })
    .await;
    assert_eq!(count, 3);

    let deleted = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_write_intent_delete("b").await })
    })
    .await;
    assert_eq!(deleted, 1);
    let again = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_write_intent_delete("b").await })
    })
    .await;
    assert_eq!(again, 0);

    let all = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_write_intent_delete_all().await })
    })
    .await;
    assert_eq!(all, 2);
    let empty = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_write_intent_count().await })
    })
    .await;
    assert_eq!(empty, 0);
}

#[tokio::test]
async fn delete_intent_scan_is_updated_at_ordered_and_limited() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    insert_delete(&db, &obj, &delete_row("d1", 300)).await;
    insert_delete(&db, &obj, &delete_row("d2", 100)).await;
    insert_delete(&db, &obj, &delete_row("d3", 200)).await;

    let rows = delete_stale(&db, &obj, 250, 10).await;
    assert_eq!(
        rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["d2", "d3"]
    );
    assert_eq!(rows[0].local_version_id.as_deref(), Some(""));
    assert_eq!(rows[0].marker_version_id.as_deref(), Some(""));
    assert_eq!(rows[0].remote_version_id.as_deref(), Some("rv-d2"));

    assert_eq!(
        delete_stale(&db, &obj, 250, 1)
            .await
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>(),
        ["d2"]
    );
    assert!(delete_stale(&db, &obj, 250, 0).await.is_empty());
    assert_eq!(delete_stale(&db, &obj, 250, -1).await.len(), 2);

    let done = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_delete_intent_mark_done("d3", 400).await })
    })
    .await;
    assert_eq!(done, 1);
    let after = delete_stale(&db, &obj, 500, 10).await;
    let d3 = after.iter().find(|r| r.id == "d3").unwrap();
    assert_eq!(d3.state, crate::sync::INTENT_DONE);

    let count = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_delete_intent_count().await })
    })
    .await;
    assert_eq!(count, 3);
    let deleted = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_delete_intent_delete("d2").await })
    })
    .await;
    assert_eq!(deleted, 1);
    let all = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_delete_intent_delete_all().await })
    })
    .await;
    assert_eq!(all, 2);
}

#[tokio::test]
async fn duplicate_ids_fail_like_the_primary_key() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    insert_write(&db, &obj, &write_row("dup", 100)).await;
    let (oracle, object) = same_err(
        both_ops(&db, &obj, |t| {
            Box::pin(async move { t.sync_write_intent_insert(&write_row("dup", 100)).await })
        })
        .await,
    );
    assert!(matches!(oracle, ControlError::Backend(_)));
    assert!(matches!(object, ControlError::Backend(_)));

    insert_delete(&db, &obj, &delete_row("dup", 100)).await;
    let (oracle, object) = same_err(
        both_ops(&db, &obj, |t| {
            Box::pin(async move { t.sync_delete_intent_insert(&delete_row("dup", 100)).await })
        })
        .await,
    );
    assert!(matches!(oracle, ControlError::Backend(_)));
    assert!(matches!(object, ControlError::Backend(_)));

    let writes = w(&db, &obj, |t| {
        Box::pin(async move { t.sync_write_intent_count().await })
    })
    .await;
    assert_eq!(writes, 1);
}
