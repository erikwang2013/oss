//! E2: frozen raft-guard matrix. All 33 replicated-table cells (11 tables x
//! insert/update/delete) must reject Local and SiteApply writes in raft mode
//! with the exact SQLite trigger message and zero bytes, while RaftApply,
//! SnapshotInstall and standalone Local writes succeed. The 12 golden
//! exceptions live in `control_guards_exceptions`.

use serde::Deserialize;
use xl_store::control_state::root::{AppliedLog, Mode};
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::AccessKeyEntry;
use xl_store::error::Error;

use super::golden;
use super::support::{self, Obj};
use crate::control_store_obj::{TableId, WriteOrigin};

const RAFT_GUARD: &str = super::super::txn::RAFT_GUARD;

#[derive(Deserialize)]
struct Matrix {
    cases: Vec<Cell>,
    denied_error: String,
    exceptions: Exceptions,
}

#[derive(Deserialize)]
struct Cell {
    table: String,
    op: String,
    inside_apply: String,
    outside_apply: Outside,
}

#[derive(Deserialize)]
struct Outside {
    sqlite_message: String,
}

#[derive(Deserialize)]
struct Exceptions {
    actions: Vec<String>,
    all_allowed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Insert,
    Update,
    Delete,
}

fn matrix() -> Matrix {
    serde_json::from_value(golden::load_json("guards/matrix.json")).expect("guard golden shape")
}

fn table_id(name: &str) -> TableId {
    TableId::ALL
        .iter()
        .copied()
        .find(|t| t.name() == name)
        .unwrap_or_else(|| panic!("guard golden names unknown table {name}"))
}

fn op(name: &str) -> Op {
    match name {
        "insert" => Op::Insert,
        "update" => Op::Update,
        "delete" => Op::Delete,
        other => panic!("guard golden names unknown op {other}"),
    }
}

pub(super) fn raft(term: u64, index: u64) -> WriteOrigin {
    WriteOrigin::raft_apply(AppliedLog { term, index })
}

pub(super) fn iam_msg(e: ControlError) -> String {
    match e {
        ControlError::Invalid(msg) => msg,
        other => format!("unexpected error: {other}"),
    }
}

fn store_msg(e: Error) -> String {
    match e {
        Error::InvalidArgument(msg) => msg,
        other => format!("unexpected error: {other}"),
    }
}

pub(super) async fn iam_v<T, F>(real: &Obj, origin: WriteOrigin, f: F) -> Result<T, String>
where
    F: for<'a> FnOnce(&'a mut dyn support::Iam) -> support::Boxed<'a, T>,
{
    support::object_iam(&real.control, origin, f)
        .await
        .map_err(iam_msg)
}

pub(super) async fn seed_user(
    real: &Obj,
    origin: WriteOrigin,
    name: &str,
    admin: bool,
) -> Result<i64, String> {
    let name = name.to_owned();
    iam_v(real, origin, move |t| {
        Box::pin(async move { t.user_create_default_status(&name, "h", admin, 10).await })
    })
    .await
}

pub(super) async fn seed_alice(real: &Obj, origin: WriteOrigin) -> Result<(), String> {
    seed_user(real, origin, "alice", false).await.map(|_| ())
}

async fn seed_key(real: &Obj, origin: WriteOrigin) -> Result<(), String> {
    let r = iam_v(real, origin, |t| {
        Box::pin(async move { t.access_key_create_legacy("AKGUARD", "sk", 1, 30).await })
    })
    .await;
    eprintln!("SEED_KEY result {r:?}");
    r
}

async fn seed_permission(real: &Obj, origin: WriteOrigin) -> Result<(), String> {
    iam_v(real, origin, |t| {
        Box::pin(async move { t.permission_upsert(1, "bguard", true, false, true).await })
    })
    .await
}

fn raw_key(table: TableId) -> Vec<u8> {
    table.name().as_bytes().to_vec()
}

async fn raw(real: &Obj, origin: WriteOrigin, table: TableId, op: Op) -> Result<(), String> {
    let mut txn = real.control.begin(origin).await.map_err(store_msg)?;
    let key = raw_key(table);
    match op {
        Op::Insert | Op::Update => txn.put(table, &key, b"row").await.map_err(store_msg)?,
        Op::Delete => {
            txn.delete(table, &key).await.map_err(store_msg)?;
        }
    }
    txn.commit().await.map_err(store_msg)?;
    Ok(())
}

/// Prerequisite rows only; every probe row itself is created by `mutate`.
async fn seed(real: &Obj, origin: WriteOrigin, table: TableId, op: Op) -> Result<(), String> {
    match table {
        TableId::Users => {
            if op == Op::Insert {
                Ok(())
            } else {
                seed_alice(real, origin).await
            }
        }
        TableId::AccessKeys => {
            seed_alice(real, origin.clone()).await?;
            if op == Op::Insert {
                Ok(())
            } else {
                // A RaftApply origin carries a log id; reusing the seed's id
                // would hit the idempotent-replay path and skip the write.
                seed_key(real, raft(1, 101)).await
            }
        }
        TableId::Permissions => {
            seed_alice(real, origin.clone()).await?;
            if op == Op::Insert {
                Ok(())
            } else {
                seed_permission(real, raft(1, 101)).await
            }
        }
        _ => {
            if op == Op::Insert {
                Ok(())
            } else {
                raw(real, origin, table, Op::Insert).await
            }
        }
    }
}

async fn mutate(real: &Obj, origin: WriteOrigin, table: TableId, op: Op) -> Result<(), String> {
    match table {
        TableId::Users => match op {
            Op::Insert => {
                iam_v(real, origin, |t| {
                    Box::pin(async move {
                        t.user_create_default_status("newbie", "h", false, 111)
                            .await
                            .map(|_| ())
                    })
                })
                .await
            }
            Op::Update => {
                iam_v(real, origin, |t| {
                    Box::pin(async move { t.user_set_password(1, "h2").await.map(|_| ()) })
                })
                .await
            }
            Op::Delete => {
                iam_v(real, origin, |t| {
                    Box::pin(async move { t.user_delete(1).await.map(|_| ()) })
                })
                .await
            }
        },
        TableId::AccessKeys => match op {
            Op::Insert => {
                let entry = AccessKeyEntry {
                    access_key: "AKGUARD".into(),
                    secret_key: "sk".into(),
                    username: "alice".into(),
                    status: "active".into(),
                    expires_at: None,
                    created_at: 30,
                    parent_ak: None,
                    session_policy: None,
                    kind: "long-term".into(),
                };
                iam_v(real, origin, move |t| {
                    Box::pin(async move { t.access_key_create(&entry).await })
                })
                .await
            }
            Op::Update => {
                iam_v(real, origin, |t| {
                    Box::pin(async move {
                        t.access_key_set_status("AKGUARD", "disabled")
                            .await
                            .map(|_| ())
                    })
                })
                .await
            }
            Op::Delete => {
                iam_v(real, origin, |t| {
                    Box::pin(async move { t.access_key_delete("AKGUARD").await.map(|_| ()) })
                })
                .await
            }
        },
        TableId::Permissions => match op {
            Op::Insert => {
                iam_v(real, origin, |t| {
                    Box::pin(
                        async move { t.permission_upsert(1, "bguard", true, false, false).await },
                    )
                })
                .await
            }
            Op::Update => {
                iam_v(real, origin, |t| {
                    Box::pin(
                        async move { t.permission_upsert(1, "bguard", false, false, true).await },
                    )
                })
                .await
            }
            Op::Delete => {
                iam_v(real, origin, |t| {
                    Box::pin(async move { t.permission_delete(1, "bguard").await })
                })
                .await
            }
        },
        _ => raw(real, origin, table, op).await,
    }
}

async fn pinned(real: &Obj) -> Vec<u8> {
    real.control.pin().await.unwrap().bytes
}

async fn check_cell(table: TableId, op: Op, denial: &str) {
    // Raft mode: both local origins are refused before anything is staged.
    let real = support::object_store(Mode::Raft).await;
    seed(&real, raft(1, 1), table, op)
        .await
        .unwrap_or_else(|e| panic!("{table:?}/{op:?} seed: {e}"));
    let before = pinned(&real).await;
    let err = match mutate(&real, WriteOrigin::local("test"), table, op).await {
        Err(e) => e,
        Ok(()) => panic!("{table:?}/{op:?} Local was allowed"),
    };
    assert_eq!(err, denial, "{table:?}/{op:?} Local denial");
    assert_eq!(
        pinned(&real).await,
        before,
        "{table:?}/{op:?} Local wrote bytes"
    );
    let err = match mutate(&real, WriteOrigin::site_apply("peer-b"), table, op).await {
        Err(e) => e,
        Ok(()) => panic!("{table:?}/{op:?} SiteApply was allowed"),
    };
    assert_eq!(err, denial, "{table:?}/{op:?} SiteApply denial");
    assert_eq!(
        pinned(&real).await,
        before,
        "{table:?}/{op:?} SiteApply wrote bytes"
    );

    // Replicated origins are allowed to write the same cell.
    let real = support::object_store(Mode::Raft).await;
    seed(&real, raft(1, 1), table, op).await.unwrap();
    mutate(&real, raft(1, 2), table, op)
        .await
        .unwrap_or_else(|e| panic!("{table:?}/{op:?} RaftApply: {e}"));

    let real = support::object_store(Mode::Raft).await;
    seed(&real, raft(1, 1), table, op).await.unwrap();
    mutate(&real, WriteOrigin::snapshot_install(), table, op)
        .await
        .unwrap_or_else(|e| panic!("{table:?}/{op:?} SnapshotInstall: {e}"));

    // Standalone mode disables the guard for exactly the same mutation.
    let real = support::object_store(Mode::Standalone).await;
    seed(&real, WriteOrigin::local("seed"), table, op)
        .await
        .unwrap();
    mutate(&real, WriteOrigin::local("test"), table, op)
        .await
        .unwrap_or_else(|e| panic!("{table:?}/{op:?} standalone: {e}"));
}

#[tokio::test]
async fn raft_guard_matrix_matches_goldens() {
    let matrix = matrix();
    assert_eq!(matrix.cases.len(), 33, "33 replicated-table cells");
    assert_eq!(matrix.denied_error, RAFT_GUARD);
    let mut seen = std::collections::BTreeSet::new();
    for cell in &matrix.cases {
        let table = table_id(&cell.table);
        let op = op(&cell.op);
        assert!(
            seen.insert((table.as_u16(), cell.op.clone())),
            "duplicate golden cell {}/{}",
            cell.table,
            cell.op
        );
        assert_eq!(cell.inside_apply, "allowed");
        assert_eq!(cell.outside_apply.sqlite_message, RAFT_GUARD);
        check_cell(table, op, &cell.outside_apply.sqlite_message).await;
    }
    assert_eq!(seen.len(), 33);
}

/// Every golden exception action, asserted to be allowed under Local raft mode.
pub(super) fn exception_actions() -> Vec<String> {
    let matrix = matrix();
    assert!(matrix.exceptions.all_allowed);
    assert_eq!(matrix.exceptions.actions.len(), 12);
    matrix.exceptions.actions
}
