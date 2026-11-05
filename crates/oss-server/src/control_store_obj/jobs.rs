//! jobs / job_tasks / schedule_lease / runstate_meta object adapters.
//!
//! Rows are rmp-serde maps (the same convention as every other control row).
//! Keys are big-endian ids so the tree's byte order is the SQL `ORDER BY id`
//! order; joins that SQL does with indexes are done in memory here (shortcut:
//! whole-table scan per claim, add a due index when job volume warrants it).

use std::collections::BTreeSet;

use serde::Serialize;
use serde::de::DeserializeOwned;
use xl_store::control_state::node::LeafValue;
use xl_store::control_state::tree::TableTree;
use xl_store::control_store::ControlError;
use xl_store::control_store::jobs::{JobRow, JobTaskRow, NewJob, NewTask};

use super::{ObjectControlStore, ObjectControlTxn, TableId};

pub(super) const META_PREFIX: &str = "@runstate/";

pub(super) fn backend(e: xl_store::error::Error) -> ControlError {
    ControlError::Backend(e.to_string())
}

fn codec(e: impl std::fmt::Display) -> ControlError {
    ControlError::Backend(format!("control row codec: {e}"))
}

pub(super) fn encode<T: Serialize>(row: &T) -> Result<Vec<u8>, ControlError> {
    rmp_serde::to_vec_named(row).map_err(codec)
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ControlError> {
    rmp_serde::from_slice(bytes).map_err(codec)
}

pub(super) fn job_key(id: i64) -> [u8; 8] {
    id.to_be_bytes()
}

pub(super) fn task_key(job_id: i64, task_no: i64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&job_id.to_be_bytes());
    key[8..].copy_from_slice(&task_no.to_be_bytes());
    key
}

pub(super) const LEASE_KEY: [u8; 1] = [1];
pub(super) const RUNSTATE_META_KEYS: [&str; 2] = ["next_job_id", "next_run_id"];

pub(super) fn meta_marker(key: &str) -> String {
    format!("{META_PREFIX}{key}")
}

/// Whole-table scan bounds: every key this store writes starts below `0xff`.
fn all_keys() -> (Vec<u8>, Vec<u8>) {
    (vec![0], vec![0xff])
}

pub(super) async fn scan_store(
    store: &ObjectControlStore,
    table: TableId,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ControlError> {
    let pinned = store.pin().await.map_err(backend)?;
    let Some(root) = pinned.root.table(table.as_u16()) else {
        return Ok(Vec::new());
    };
    let tree = TableTree::from_root(root.hash, root.rows, root.bytes);
    let (lo, hi) = all_keys();
    let rows = tree
        .scan_leaf(&*store.root, lo.as_slice()..hi.as_slice())
        .await
        .map_err(backend)?;
    let mut out = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        match value {
            LeafValue::Row(bytes) => out.push((key, bytes)),
            LeafValue::Blob(blob) => {
                out.push((key, store.root.get_blob(&blob).await.map_err(backend)?))
            }
            LeafValue::Tombstone => {}
        }
    }
    Ok(out)
}

pub(super) async fn get_store(
    store: &ObjectControlStore,
    table: TableId,
    key: &[u8],
) -> Result<Option<Vec<u8>>, ControlError> {
    let pinned = store.pin().await.map_err(backend)?;
    let Some(root) = pinned.root.table(table.as_u16()) else {
        return Ok(None);
    };
    let tree = TableTree::from_root(root.hash, root.rows, root.bytes);
    match tree.get_leaf(&*store.root, key).await.map_err(backend)? {
        None | Some(LeafValue::Tombstone) => Ok(None),
        Some(LeafValue::Row(bytes)) => Ok(Some(bytes)),
        Some(LeafValue::Blob(blob)) => Ok(Some(store.root.get_blob(&blob).await.map_err(backend)?)),
    }
}

pub(super) async fn scan_txn(
    txn: &ObjectControlTxn,
    table: TableId,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ControlError> {
    let (lo, hi) = all_keys();
    txn.scan(table, lo.as_slice()..hi.as_slice())
        .await
        .map_err(backend)
}

pub(super) async fn all_jobs_store(
    store: &ObjectControlStore,
) -> Result<Vec<JobRow>, ControlError> {
    let mut rows = Vec::new();
    for (_, value) in scan_store(store, TableId::Jobs).await? {
        rows.push(decode::<JobRow>(&value)?);
    }
    rows.sort_by_key(|j| j.id);
    Ok(rows)
}

pub(super) async fn all_tasks_store(
    store: &ObjectControlStore,
) -> Result<Vec<JobTaskRow>, ControlError> {
    let mut rows = Vec::new();
    for (_, value) in scan_store(store, TableId::JobTasks).await? {
        rows.push(decode::<JobTaskRow>(&value)?);
    }
    rows.sort_by_key(|t| (t.job_id, t.task_no));
    Ok(rows)
}

pub(super) async fn all_jobs_txn(txn: &ObjectControlTxn) -> Result<Vec<JobRow>, ControlError> {
    let mut rows = Vec::new();
    for (_, value) in scan_txn(txn, TableId::Jobs).await? {
        rows.push(decode::<JobRow>(&value)?);
    }
    rows.sort_by_key(|j| j.id);
    Ok(rows)
}

pub(super) async fn all_tasks_txn(txn: &ObjectControlTxn) -> Result<Vec<JobTaskRow>, ControlError> {
    let mut rows = Vec::new();
    for (_, value) in scan_txn(txn, TableId::JobTasks).await? {
        rows.push(decode::<JobTaskRow>(&value)?);
    }
    rows.sort_by_key(|t| (t.job_id, t.task_no));
    Ok(rows)
}

pub(super) async fn txn_job(
    txn: &ObjectControlTxn,
    id: i64,
) -> Result<Option<JobRow>, ControlError> {
    match txn
        .get(TableId::Jobs, &job_key(id))
        .await
        .map_err(backend)?
    {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

pub(super) async fn txn_task(
    txn: &ObjectControlTxn,
    job_id: i64,
    task_no: i64,
) -> Result<Option<JobTaskRow>, ControlError> {
    match txn
        .get(TableId::JobTasks, &task_key(job_id, task_no))
        .await
        .map_err(backend)?
    {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

pub(super) async fn runstate_meta_store(
    store: &ObjectControlStore,
    key: &str,
) -> Result<Option<i64>, ControlError> {
    let pinned = store.pin().await.map_err(backend)?;
    pinned
        .root
        .meta
        .markers
        .get(&meta_marker(key))
        .map(|v| {
            v.parse::<i64>()
                .map_err(|e| ControlError::Backend(format!("runstate_meta {key}: {e}")))
        })
        .transpose()
}

pub(super) fn same_owner(a: Option<i64>, b: Option<i64>) -> bool {
    a == b
}

pub(super) fn inflight(state: &str) -> bool {
    matches!(state, "queued" | "running" | "paused" | "preempted")
}

pub(super) fn take_limit<T>(rows: &mut Vec<T>, limit: i64) {
    if limit >= 0 {
        rows.truncate(limit as usize);
    }
}

fn invalid(e: impl std::fmt::Display) -> ControlError {
    ControlError::Invalid(e.to_string())
}

pub(super) async fn stage_job(
    txn: &mut ObjectControlTxn,
    job: &JobRow,
) -> Result<(), ControlError> {
    txn.put(TableId::Jobs, &job_key(job.id), &encode(job)?)
        .await
        .map_err(backend)
}

pub(super) async fn stage_task(
    txn: &mut ObjectControlTxn,
    task: &JobTaskRow,
) -> Result<(), ControlError> {
    txn.put(
        TableId::JobTasks,
        &task_key(task.job_id, task.task_no),
        &encode(task)?,
    )
    .await
    .map_err(backend)
}

pub(super) fn next_job_id(txn: &mut ObjectControlTxn) -> Result<i64, ControlError> {
    i64::try_from(txn.alloc_job_id().map_err(backend)?).map_err(invalid)
}

pub(super) fn blank_job(id: i64, new: &NewJob) -> JobRow {
    JobRow {
        id,
        kind: new.kind.clone(),
        bucket: new.bucket.clone(),
        prefix: new.prefix.clone(),
        state: new.state.clone(),
        scanned: 0,
        deleted: 0,
        skipped: 0,
        error: None,
        created_at: new.created_at,
        updated_at: new.updated_at,
        spec_json: new.spec_json.clone(),
        progress: 0,
        total: new.total,
        succeeded: 0,
        failed: 0,
        report_bucket: new.report_bucket.clone(),
        report_prefix: new.report_prefix.clone(),
        priority: new.priority,
        requested_by: new.requested_by,
        parent_job_id: new.parent_job_id,
        started_at: None,
        finished_at: None,
    }
}

pub(super) fn queued_task(job_id: i64, task_no: i64, task: &NewTask, next_at: i64) -> JobTaskRow {
    JobTaskRow {
        job_id,
        task_no,
        action: task.action.clone(),
        payload_json: task.payload_json.clone(),
        state: "queued".into(),
        attempt: 0,
        next_at,
        dedupe_key: task.dedupe_key.clone(),
        deadline_at: None,
        result_code: None,
        result_string: None,
        started_at: None,
        finished_at: None,
        fence_term: 0,
        fence_leader: 0,
    }
}

/// `INSERT OR IGNORE` planning: existing keys plus keys claimed earlier in the
/// batch are taken. Returns the task indexes that would be inserted.
pub(super) async fn insert_or_ignore(
    txn: &ObjectControlTxn,
    tasks: &[NewTask],
) -> Result<Vec<usize>, ControlError> {
    let mut taken: BTreeSet<String> = all_tasks_txn(txn)
        .await?
        .into_iter()
        .filter_map(|t| t.dedupe_key)
        .collect();
    let mut out = Vec::new();
    for (i, task) in tasks.iter().enumerate() {
        if let Some(key) = &task.dedupe_key
            && !taken.insert(key.clone())
        {
            continue;
        }
        out.push(i);
    }
    Ok(out)
}

/// Plain INSERT must reject duplicate dedupe keys like the SQLite unique index.
pub(super) async fn ensure_dedupe_free(
    txn: &ObjectControlTxn,
    tasks: &[NewTask],
) -> Result<(), ControlError> {
    let mut taken: BTreeSet<String> = all_tasks_txn(txn)
        .await?
        .into_iter()
        .filter_map(|t| t.dedupe_key)
        .collect();
    for task in tasks {
        if let Some(key) = &task.dedupe_key
            && !taken.insert(key.clone())
        {
            return Err(ControlError::Backend(
                "UNIQUE constraint failed: job_tasks.dedupe_key".into(),
            ));
        }
    }
    Ok(())
}

pub(super) async fn stage_tasks(
    txn: &mut ObjectControlTxn,
    job_id: i64,
    tasks: &[NewTask],
    indexes: &[usize],
) -> Result<(), ControlError> {
    for &i in indexes {
        stage_task(
            txn,
            &queued_task(job_id, i as i64 + 1, &tasks[i], tasks[i].next_at),
        )
        .await?;
    }
    Ok(())
}
