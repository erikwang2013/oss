//! D1 parity: object jobs adapter vs the SQLite seam.

use std::collections::BTreeSet;
use std::sync::Arc;

use futures::future::BoxFuture;
use xl_store::control_store::inventory::InventoryRunRow;
use xl_store::control_store::jobs::{
    JobClaim, JobRow, JobSnapshotTxn, JobStore, JobTaskRow, JobTxn, NewJob, NewTask,
    ScheduleLeaseRow,
};
use xl_store::control_store::tier::TierRestoreRow;

use super::{local, real_store};
use crate::control_store::{SqliteControlStore, SqliteControlTxn};
use crate::control_store_obj::{ObjectControlStore, ObjectControlTxn, TableId};

type Sqlite = (crate::db::Db, SqliteControlStore);

async fn sqlite() -> Sqlite {
    let db = crate::db::Db::connect("sqlite::memory:").await.unwrap();
    let store = SqliteControlStore::new(db.pool().clone());
    (db, store)
}

async fn in_sqlite<R>(
    db: &crate::db::Db,
    f: impl for<'a> FnOnce(&'a mut SqliteControlTxn<'a>) -> BoxFuture<'a, R>,
) -> R {
    let mut conn = db.pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();
    let out = {
        let mut txn = SqliteControlTxn::new(&mut conn);
        f(&mut txn).await
    };
    sqlx::query("COMMIT").execute(&mut *conn).await.unwrap();
    out
}

async fn in_object<R>(
    control: &Arc<ObjectControlStore>,
    f: impl for<'a> FnMut(&'a mut ObjectControlTxn) -> BoxFuture<'a, xl_store::error::Result<R>>,
) -> R {
    control.transaction(local(), f).await.unwrap()
}

fn task(key: &str) -> NewTask {
    NewTask {
        action: "delete".into(),
        payload_json: "{\"k\":\"v\"}".into(),
        dedupe_key: Some(key.into()),
        next_at: 100,
    }
}

fn new_job() -> NewJob {
    NewJob {
        kind: "lifecycle".into(),
        bucket: "b".into(),
        prefix: "p/".into(),
        state: "queued".into(),
        total: 2,
        priority: 5,
        requested_by: Some(7),
        created_at: 100,
        updated_at: 100,
        ..NewJob::default()
    }
}

#[derive(Debug)]
struct Outcome {
    created: i64,
    claims: Vec<JobClaim>,
    finishes: Vec<&'static str>,
    dedupe_all: Option<i64>,
    dedupe_partial: Option<i64>,
}

async fn scenario(txn: &mut dyn JobTxn) -> Outcome {
    let created = txn
        .job_create(&new_job(), &[task("k1"), task("k2")])
        .await
        .unwrap();
    let c1 = txn.job_claim(200, 30, 4, 9).await.unwrap().unwrap();
    let c2 = txn.job_claim(201, 30, 4, 9).await.unwrap().unwrap();
    let f1 = txn
        .finish_task(c1.job_id, c1.task_no, true, "OK", Some("ok"), 210, 3, 4)
        .await
        .unwrap();
    // attempt 0 -> 1 of 2: retry with deterministic backoff.
    let f2 = txn
        .finish_task(
            c2.job_id,
            c2.task_no,
            false,
            "ETEMP",
            Some("boom"),
            211,
            2,
            4,
        )
        .await
        .unwrap();
    let c3 = txn.job_claim(1000, 30, 4, 9).await.unwrap().unwrap();
    let f3 = txn
        .finish_task(
            c3.job_id,
            c3.task_no,
            false,
            "ETEMP",
            Some("boom"),
            1001,
            2,
            4,
        )
        .await
        .unwrap();
    let dedupe_all = txn
        .job_create_dedup(&new_job(), &[task("k1"), task("k2")])
        .await
        .unwrap();
    let dedupe_partial = txn
        .job_create_dedup(&new_job(), &[task("k1"), task("k3"), task("k3")])
        .await
        .unwrap();
    txn.schedule_lease_upsert(&ScheduleLeaseRow {
        term: 3,
        node: 2,
        expires_at: 500,
        acquired_at: 400,
    })
    .await
    .unwrap();
    txn.runstate_meta_next_run_id(42).await.unwrap();
    Outcome {
        created,
        claims: vec![c1, c2, c3],
        finishes: vec![f1, f2, f3],
        dedupe_all,
        dedupe_partial,
    }
}

async fn dump(store: &dyn JobStore) -> String {
    let jobs = store.recent_jobs(-1).await.unwrap();
    let mut tasks: Vec<JobTaskRow> = Vec::new();
    for job in &jobs {
        tasks.extend(store.job_tasks(job.id).await.unwrap());
    }
    let mut dedupe_keys = store.dedupe_keys().await.unwrap();
    dedupe_keys.sort();
    serde_json::json!({
        "jobs": jobs,
        "tasks": tasks,
        "to_promote": store.jobs_to_promote().await.unwrap(),
        "candidate": store.queued_task_candidate(1000, 4, true).await.unwrap(),
        "inflight": store.job_inflight_counts(Some(7)).await.unwrap(),
        "running": store.running_task_count().await.unwrap(),
        "max_job": store.max_job_id().await.unwrap(),
        "dedupe_keys": dedupe_keys,
        "lease": store.schedule_lease().await.unwrap(),
        "next_run_id": store.runstate_meta("next_run_id").await.unwrap(),
        "recent_limit_1": store.recent_jobs(1).await.unwrap(),
        "tasks_page": store.job_tasks_page(1, 1, 1).await.unwrap(),
        "terminal_before_1000": store.terminal_jobs_before(1000, 10).await.unwrap(),
    })
    .to_string()
}

type Snapshot = (Vec<u8>, Vec<u8>, Option<ScheduleLeaseRow>, Option<i64>);

async fn snapshot(txn: &mut dyn JobSnapshotTxn) -> Snapshot {
    let jobs = txn.runstate_jobs().await.unwrap();
    let tasks = txn.runstate_job_tasks().await.unwrap();
    (
        rmp_serde::to_vec(&jobs).unwrap(),
        rmp_serde::to_vec(&tasks).unwrap(),
        txn.snapshot_schedule_lease().await.unwrap(),
        txn.snapshot_runstate_meta("next_run_id").await.unwrap(),
    )
}

#[tokio::test]
async fn state_machine_and_snapshot_bytes_match_sqlite() {
    let (db, sql) = sqlite().await;
    let real = real_store().await;
    let control = Arc::clone(&real.control);

    let (sq_out, ob_out) = (
        in_sqlite(&db, |txn| Box::pin(async { scenario(txn).await })).await,
        in_object(&control, |txn| Box::pin(async { Ok(scenario(txn).await) })).await,
    );
    assert_eq!(sq_out.created, 1);
    assert_eq!(sq_out.dedupe_all, Some(2));
    assert_eq!(sq_out.dedupe_partial, Some(3));
    assert_eq!(
        sq_out.claims.iter().map(|c| c.task_no).collect::<Vec<_>>(),
        vec![1, 2, 2]
    );
    assert_eq!(sq_out.finishes, vec!["done", "queued", "failed"]);
    assert_eq!(format!("{sq_out:?}"), format!("{ob_out:?}"));

    assert_eq!(dump(&sql).await, dump(&*control as &dyn JobStore).await);
    let sq_snap = in_sqlite(&db, |txn| Box::pin(async { snapshot(txn).await })).await;
    let ob_snap = in_object(&control, |txn| Box::pin(async { Ok(snapshot(txn).await) })).await;
    assert_eq!(sq_snap, ob_snap);

    // The failed job converged to error with progress 2 of 2.
    let job = control.job(1).await.unwrap().unwrap();
    assert_eq!(job.state, "error");
    assert_eq!((job.progress, job.failed), (2, 1));
    // Partial dedupe rewrote total to the single inserted task.
    let partial = control.job(3).await.unwrap().unwrap();
    assert_eq!(partial.total, 1);
}

#[tokio::test]
async fn purge_nulls_inventory_tier_and_parent_like_sqlite() {
    let (db, sql) = sqlite().await;
    let real = real_store().await;
    let control = Arc::clone(&real.control);

    in_sqlite(&db, |txn| {
        Box::pin(async {
            let a = txn
                .job_create(&new_job(), &[task("p1"), task("p2")])
                .await
                .unwrap();
            let first = txn.job_claim(200, 30, 4, 9).await.unwrap().unwrap();
            let second = txn.job_claim(201, 30, 4, 9).await.unwrap().unwrap();
            txn.finish_task(first.job_id, first.task_no, true, "OK", None, 202, 3, 4)
                .await
                .unwrap();
            txn.finish_task(second.job_id, second.task_no, true, "OK", None, 203, 3, 4)
                .await
                .unwrap();
            let mut child = new_job();
            child.total = 1;
            child.parent_job_id = Some(a);
            txn.job_create(&child, &[task("c1")]).await.unwrap();
            sqlx::query(
                "INSERT INTO inventory_runs (id, bucket, config_id, run_key, started_at, state, \
                 job_id, created_at) VALUES (1, 'b', 'c', 'r', 0, 'done', ?, 0)",
            )
            .bind(a)
            .execute(&mut *txn.conn)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO tier_restores (bucket, key, version_id, job_id, created_at) \
                 VALUES ('b', 'k', 'v', ?, 0)",
            )
            .bind(a)
            .execute(&mut *txn.conn)
            .await
            .unwrap();
        })
    })
    .await;

    in_object(&control, |txn| {
        Box::pin(async {
            let a = txn
                .job_create(&new_job(), &[task("p1"), task("p2")])
                .await
                .unwrap();
            let first = txn.job_claim(200, 30, 4, 9).await.unwrap().unwrap();
            let second = txn.job_claim(201, 30, 4, 9).await.unwrap().unwrap();
            txn.finish_task(first.job_id, first.task_no, true, "OK", None, 202, 3, 4)
                .await
                .unwrap();
            txn.finish_task(second.job_id, second.task_no, true, "OK", None, 203, 3, 4)
                .await
                .unwrap();
            let mut child = new_job();
            child.total = 1;
            child.parent_job_id = Some(a);
            txn.job_create(&child, &[task("c1")]).await.unwrap();
            let run = InventoryRunRow {
                id: 1,
                bucket: "b".into(),
                config_id: "c".into(),
                run_key: "r".into(),
                started_at: 0,
                state: "done".into(),
                job_id: Some(1),
                manifest_key: None,
                error: None,
                created_at: 0,
                fence_term: 0,
                fence_leader: 0,
            };
            let restore = TierRestoreRow {
                bucket: "b".into(),
                key: "k".into(),
                version_id: "v".into(),
                expires_at: 0,
                job_id: Some(1),
                state: "pending".into(),
                created_at: 0,
                fence_term: 0,
                fence_leader: 0,
            };
            txn.put(
                TableId::InventoryRuns,
                b"run-1",
                &rmp_serde::to_vec_named(&run).unwrap(),
            )
            .await
            .unwrap();
            txn.put(
                TableId::TierRestores,
                b"tier-1",
                &rmp_serde::to_vec_named(&restore).unwrap(),
            )
            .await
            .unwrap();
            Ok(())
        })
    })
    .await;

    let sq = in_sqlite(&db, |txn| {
        Box::pin(async {
            // job 1 is terminal; job 2 (child) is queued; 999 is absent.
            txn.purge_terminal_jobs(&[1, 2, 999]).await.unwrap()
        })
    })
    .await;
    assert_eq!(sq, (1, 1));
    let ob = in_object(&control, |txn| {
        Box::pin(async { Ok(txn.purge_terminal_jobs(&[1, 2, 999]).await.unwrap()) })
    })
    .await;
    assert_eq!(sq, ob);

    assert_eq!(dump(&sql).await, dump(&*control as &dyn JobStore).await);
    let sql_run: Option<i64> = sqlx::query_scalar("SELECT job_id FROM inventory_runs WHERE id = 1")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let sql_tier: Option<i64> =
        sqlx::query_scalar("SELECT job_id FROM tier_restores WHERE bucket = 'b'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!((sql_run, sql_tier), (None, None));
    // Both FK rows stayed present and lost their job id.
    let (run, restore) = in_object(&control, |txn| {
        Box::pin(async {
            let runs = txn
                .scan(TableId::InventoryRuns, &[0u8][..]..&[0xffu8][..])
                .await
                .unwrap();
            let restores = txn
                .scan(TableId::TierRestores, &[0u8][..]..&[0xffu8][..])
                .await
                .unwrap();
            assert_eq!((runs.len(), restores.len()), (1, 1));
            Ok((
                rmp_serde::from_slice::<InventoryRunRow>(&runs[0].1).unwrap(),
                rmp_serde::from_slice::<TierRestoreRow>(&restores[0].1).unwrap(),
            ))
        })
    })
    .await;
    assert_eq!((run.job_id, restore.job_id), (None, None));
    let child = control.job(2).await.unwrap().unwrap();
    assert_eq!(child.parent_job_id, None);
}

#[tokio::test]
async fn concurrent_claims_take_distinct_tasks() {
    let real = real_store().await;
    let control = Arc::clone(&real.control);
    in_object(&control, |txn| {
        Box::pin(async {
            txn.job_create(&new_job(), &[task("q1"), task("q2")])
                .await
                .unwrap();
            Ok(())
        })
    })
    .await;

    let a = Arc::clone(&control);
    let b = Arc::clone(&control);
    let claim = |control: Arc<ObjectControlStore>| async move {
        control
            .transaction(local(), |txn| {
                Box::pin(async {
                    txn.job_claim(200, 30, 4, 9)
                        .await
                        .map_err(|e| xl_store::error::Error::InvalidArgument(e.to_string()))
                })
            })
            .await
            .unwrap()
            .unwrap()
    };
    let (first, second) = tokio::join!(claim(a), claim(b));
    let numbers: BTreeSet<i64> = [first.task_no, second.task_no].into_iter().collect();
    assert_eq!(numbers, BTreeSet::from([1, 2]));
    assert!(control.job_task(1, 1).await.unwrap().unwrap().state == "running");
    assert!(control.job_task(1, 2).await.unwrap().unwrap().state == "running");
}

#[tokio::test]
async fn snapshot_install_orders_rows_and_advances_allocators() {
    let real = real_store().await;
    let control = Arc::clone(&real.control);
    let (jobs, tasks) = in_object(&control, |txn| {
        Box::pin(async {
            txn.insert_jobs(&[snap_row(9), snap_row(3)]).await.unwrap();
            txn.insert_job_tasks(&[snap_task(9, 2), snap_task(9, 1), snap_task(3, 1)])
                .await
                .unwrap();
            txn.insert_runstate_meta("next_job_id", 10).await.unwrap();
            Ok((
                txn.runstate_jobs().await.unwrap(),
                txn.runstate_job_tasks().await.unwrap(),
            ))
        })
    })
    .await;
    assert_eq!(jobs.iter().map(|j| j.id).collect::<Vec<_>>(), vec![3, 9]);
    assert_eq!(
        tasks
            .iter()
            .map(|t| (t.job_id, t.task_no))
            .collect::<Vec<_>>(),
        vec![(3, 1), (9, 1), (9, 2)]
    );
    assert_eq!(control.max_job_id().await.unwrap(), 9);
    assert_eq!(
        control.runstate_meta("next_job_id").await.unwrap(),
        Some(10)
    );
    // The next allocation starts at the installed high-water mark.
    let id = in_object(&control, |txn| {
        Box::pin(async { Ok(txn.job_start("lifecycle", "b", "", 20).await.unwrap()) })
    })
    .await;
    assert_eq!(id, 10);

    // Clearing keeps the allocator monotonic and removes the meta marker.
    in_object(&control, |txn| {
        Box::pin(async {
            txn.runstate_clear().await.unwrap();
            Ok(())
        })
    })
    .await;
    assert!(control.job(3).await.unwrap().is_none());
    assert_eq!(control.runstate_meta("next_job_id").await.unwrap(), None);
    assert_eq!(control.max_job_id().await.unwrap(), 0);
    let id = in_object(&control, |txn| {
        Box::pin(async { Ok(txn.job_start("lifecycle", "b", "", 21).await.unwrap()) })
    })
    .await;
    assert_eq!(id, 11);
}

fn snap_row(id: i64) -> JobRow {
    JobRow {
        id,
        kind: "lifecycle".into(),
        bucket: "b".into(),
        prefix: String::new(),
        state: "done".into(),
        created_at: id,
        updated_at: id,
        ..JobRow::default()
    }
}

fn snap_task(job_id: i64, task_no: i64) -> JobTaskRow {
    JobTaskRow {
        job_id,
        task_no,
        action: "delete".into(),
        payload_json: "{}".into(),
        state: "done".into(),
        next_at: 0,
        ..JobTaskRow::default()
    }
}
