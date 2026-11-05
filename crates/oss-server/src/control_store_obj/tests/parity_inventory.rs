//! Inventory parity: config/run/cursor natural keys, run-id allocation, owner
//! scopes and snapshot exports.

use xl_store::control_store::OwnerRef;
use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow,
    InventoryStore, InventoryTxn,
};
use xl_store::error::Error as StoreError;

use super::super::TableId;
use super::super::bucket::user_key;
use super::local;
use super::parity_util::{err, object, parity, sqlite};

fn cfg(
    bucket: &str,
    id: &str,
    owner: OwnerRef,
    config_json: &str,
    updated_at: i64,
) -> InventoryConfigEntry {
    InventoryConfigEntry {
        bucket: bucket.into(),
        id: id.into(),
        owner,
        config_json: config_json.into(),
        updated_at,
    }
}

fn icursor(bucket: &str, config_id: &str, cursor: &str, fence: i64) -> InventoryCursorRow {
    InventoryCursorRow {
        bucket: bucket.into(),
        config_id: config_id.into(),
        cursor: cursor.into(),
        config_updated_at: fence,
        updated_at: fence,
        fence_term: fence,
        fence_leader: 1,
    }
}

fn inc(bucket: &str, config_id: &str, enabled: bool, updated_at: i64) -> InventoryIncrementalRow {
    InventoryIncrementalRow {
        bucket: bucket.into(),
        config_id: config_id.into(),
        enabled,
        updated_at,
    }
}

#[allow(clippy::too_many_arguments)]
fn run(
    id: i64,
    bucket: &str,
    config_id: &str,
    run_key: &str,
    state: &str,
    created_at: i64,
) -> InventoryRunRow {
    InventoryRunRow {
        id,
        bucket: bucket.into(),
        config_id: config_id.into(),
        run_key: run_key.into(),
        started_at: 0,
        state: state.into(),
        job_id: None,
        manifest_key: None,
        error: None,
        created_at,
        fence_term: 1,
        fence_leader: 1,
    }
}

#[derive(Debug, PartialEq)]
struct InvOut {
    flags: Vec<bool>,
    created: Vec<Option<i64>>,
    configs: Vec<InventoryConfigEntry>,
    runs: Vec<InventoryRunRow>,
    cursors: Vec<InventoryCursorRow>,
    incrementals: Vec<InventoryIncrementalRow>,
}

async fn inventory_scenario<T: InventoryTxn>(txn: &mut T) -> Result<InvOut, StoreError> {
    txn.inventory_config_upsert(&cfg("b1", "c1", OwnerRef::Admin, "{}", 1))
        .await
        .map_err(err)?;
    txn.inventory_config_upsert(&cfg("b1", "c2", OwnerRef::User("alice".into()), "{}", 2))
        .await
        .map_err(err)?;
    txn.inventory_config_upsert(&cfg("b2", "c1", OwnerRef::User("root".into()), "{}", 3))
        .await
        .map_err(err)?;
    txn.inventory_cursor_upsert(&icursor("b1", "c2", "old", 1))
        .await
        .map_err(err)?;
    txn.inventory_incremental_upsert(&inc("b1", "c2", true, 2))
        .await
        .map_err(err)?;
    // Config upsert clears the cursor and incremental switch.
    txn.inventory_config_upsert(&cfg(
        "b1",
        "c2",
        OwnerRef::User("alice".into()),
        r#"{"v":2}"#,
        4,
    ))
    .await
    .map_err(err)?;
    let mut flags = vec![
        txn.inventory_cursor("b1", "c2")
            .await
            .map_err(err)?
            .is_none(),
        txn.inventory_incremental("b1", "c2")
            .await
            .map_err(err)?
            .is_none(),
    ];
    flags.push(
        txn.inventory_cursor_upsert(&icursor("b1", "c2", "cur2", 2))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.inventory_cursor_upsert(&icursor("b1", "c2", "stale", 1))
            .await
            .map_err(err)?,
    );
    txn.inventory_incremental_upsert(&inc("b1", "c2", false, 5))
        .await
        .map_err(err)?;

    let id1 = txn
        .inventory_run_create("b1", "c2", "run1", 10, 1, 1)
        .await
        .map_err(err)?;
    let dup = txn
        .inventory_run_create("b1", "c2", "run1", 11, 1, 1)
        .await
        .map_err(err)?;
    let id2 = txn
        .inventory_run_create("b2", "c1", "run2", 12, 1, 1)
        .await
        .map_err(err)?;
    let id1 = id1.unwrap();
    txn.inventory_run_set_job(id1, 77, 2, 2)
        .await
        .map_err(err)?;
    txn.inventory_run_started(id1, 100, 3, 3)
        .await
        .map_err(err)?;
    // started_at is set once only.
    txn.inventory_run_started(id1, 999, 4, 4)
        .await
        .map_err(err)?;
    txn.inventory_run_finish(id1, "done", Some("manifest"), None, 5, 5)
        .await
        .map_err(err)?;
    // Stale fence: must not overwrite the job id.
    txn.inventory_run_set_job(id1, 88, 1, 1)
        .await
        .map_err(err)?;
    let mut created = vec![Some(id1), dup, id2];
    // Snapshot install with explicit ids advances the run allocator.
    txn.replace_inventory_runs(&[run(50, "b9", "c9", "rk", "running", 20)])
        .await
        .map_err(err)?;
    created.push(
        txn.inventory_run_create("b9", "c9", "rk2", 21, 1, 1)
            .await
            .map_err(err)?,
    );
    txn.inventory_cursor_delete("b1", "c2", Some(1))
        .await
        .map_err(err)?;
    flags.push(
        txn.inventory_cursor_upsert(&icursor("b1", "c2", "final", 6))
            .await
            .map_err(err)?,
    );
    txn.inventory_clear("b1", "c2").await.map_err(err)?;
    flags.push(
        txn.inventory_cursor("b1", "c2")
            .await
            .map_err(err)?
            .is_none(),
    );
    flags.push(
        txn.inventory_incremental("b1", "c2")
            .await
            .map_err(err)?
            .is_none(),
    );
    flags.push(txn.inventory_config_delete("b1", "c2").await.map_err(err)?);
    flags.push(txn.inventory_config_delete("b1", "c2").await.map_err(err)?);
    Ok(InvOut {
        flags,
        created,
        configs: txn.inventory_configs().await.map_err(err)?,
        runs: txn.inventory_runs().await.map_err(err)?,
        cursors: txn.inventory_cursors().await.map_err(err)?,
        incrementals: txn.inventory_incrementals().await.map_err(err)?,
    })
}

#[tokio::test]
async fn inventory_writes_exports_and_run_ids_match() {
    let p = parity(&[("root", true), ("alice", false)]).await;
    let sq = sqlite(&p, |t| Box::pin(inventory_scenario(t)))
        .await
        .unwrap();
    let ob = object(&p, |t| Box::pin(inventory_scenario(t)))
        .await
        .unwrap();
    assert_eq!(sq, ob);
    assert_eq!(
        sq.created,
        vec![Some(1), None, Some(3), Some(51)],
        "ignored create burns an id; explicit snapshot id advances the allocator"
    );
    assert_eq!(
        sq.flags,
        vec![true, true, true, false, true, true, true, true, false]
    );
    assert_eq!(
        sq.configs
            .iter()
            .map(|c| (c.bucket.as_str(), c.id.as_str()))
            .collect::<Vec<_>>(),
        [("b1", "c1"), ("b2", "c1")]
    );
    assert_eq!(sq.configs[1].owner, OwnerRef::Admin, "admin user → Admin");
    assert_eq!(sq.runs.iter().map(|r| r.id).collect::<Vec<_>>(), [50, 51]);
    assert_eq!(sq.runs[1].run_key, "rk2");
    assert!(sq.cursors.is_empty(), "inventory_clear removes the cursor");
    assert!(sq.incrementals.is_empty());

    let sq_store = p.db.control_store();
    let ob_store = p.real.control.as_ref();
    assert_eq!(
        sq_store.inventory_configs_all().await.unwrap(),
        ob_store.inventory_configs_all().await.unwrap()
    );
    assert_eq!(
        sq_store.inventory_configs("b1").await.unwrap(),
        ob_store.inventory_configs("b1").await.unwrap()
    );
    assert_eq!(
        sq_store.inventory_config_count("b1").await.unwrap(),
        ob_store.inventory_config_count("b1").await.unwrap()
    );
    assert_eq!(
        sq_store.inventory_run(51).await.unwrap(),
        ob_store.inventory_run(51).await.unwrap()
    );
    assert_eq!(
        sq_store
            .inventory_run_by_key("b9", "c9", "rk2")
            .await
            .unwrap(),
        ob_store
            .inventory_run_by_key("b9", "c9", "rk2")
            .await
            .unwrap()
    );
    assert_eq!(
        sq_store.inventory_cursor("b1", "c2").await.unwrap(),
        ob_store.inventory_cursor("b1", "c2").await.unwrap()
    );
    assert_eq!(
        sq_store.inventory_incremental("b1", "c2").await.unwrap(),
        ob_store.inventory_incremental("b1", "c2").await.unwrap()
    );
}

#[tokio::test]
async fn inventory_owner_scopes_and_errors_match() {
    let p = parity(&[("root", true), ("alice", false)]).await;
    let ghost = cfg("b1", "c1", OwnerRef::User("ghost".into()), "{}", 1);
    let sq_err = sqlite(&p, |t| {
        let row = ghost.clone();
        Box::pin(async move {
            t.inventory_config_upsert(&row).await.map_err(err)?;
            Ok(())
        })
    })
    .await
    .unwrap_err();
    let ob_err = object(&p, |t| {
        let row = ghost.clone();
        Box::pin(async move {
            t.inventory_config_upsert(&row).await.map_err(err)?;
            Ok(())
        })
    })
    .await
    .unwrap_err();
    assert_eq!(sq_err, ob_err);
    assert_eq!(sq_err, "invalid control data: owner user ghost not found");

    let row = cfg("b1", "c1", OwnerRef::User("alice".into()), "{}", 1);
    sqlite(&p, |t| {
        let row = row.clone();
        Box::pin(async move { t.inventory_config_upsert(&row).await.map_err(err) })
    })
    .await
    .unwrap();
    object(&p, |t| {
        let row = row.clone();
        Box::pin(async move { t.inventory_config_upsert(&row).await.map_err(err) })
    })
    .await
    .unwrap();
    let alice = p.uid("alice");
    let mut conn = p.db.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("DELETE FROM users WHERE id = ?")
        .bind(alice)
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);
    p.real
        .control
        .transaction(local(), move |txn| {
            Box::pin(async move { txn.delete(TableId::Users, &user_key(alice)).await })
        })
        .await
        .unwrap();

    let view = p
        .real
        .control
        .inventory_config("b1", "c1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.owner, None, "runtime dangling owner is None");
    let sq_err = sqlite(&p, |t| {
        Box::pin(async move { t.inventory_configs().await.map_err(err).map(|_| ()) })
    })
    .await
    .unwrap_err();
    let ob_err = object(&p, |t| {
        Box::pin(async move { t.inventory_configs().await.map_err(err).map(|_| ()) })
    })
    .await
    .unwrap_err();
    assert_eq!(sq_err, ob_err);
    assert_eq!(
        sq_err,
        format!("invalid control data: owner user id {alice} missing")
    );
}
