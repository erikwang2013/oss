//! Whole-transaction retry against a concurrent writer (design §2.2 step 5),
//! last-admin guard re-evaluation, and cache publication timing.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use xl_store::control_state::root::{AppliedLog, Mode};
use xl_store::error::{Error, Result};

use super::{FaultBackend, backend_io, fault_store, local};
use crate::control_store_obj::{ObjectControlStore, RootIo, TableId, WriteOrigin};

#[tokio::test]
async fn concurrent_writers_via_transaction_both_succeed() {
    let (_backend, control) = fault_store().await;

    // The first closure call commits another writer after this transaction
    // pinned generation 1, forcing a root CAS conflict. The helper must re-run
    // the closure against generation 2 rather than replay staged rows.
    let writer = Arc::clone(&control);
    let mut injected = false;
    let result: Result<()> = control
        .transaction(local(), |txn| {
            let writer = Arc::clone(&writer);
            let conflict = !injected;
            injected = true;
            Box::pin(async move {
                if conflict {
                    let mut a = writer.begin(local()).await?;
                    a.put(TableId::Buckets, b"bucket", b"from-a").await?;
                    a.commit().await?;
                }
                txn.put(TableId::Users, b"alice", b"from-b").await?;
                Ok(())
            })
        })
        .await;
    result.unwrap();

    let pinned = control.pin().await.unwrap();
    assert_eq!(pinned.version, 3);
    let txn = control.begin(local()).await.unwrap();
    assert_eq!(
        txn.get(TableId::Buckets, b"bucket").await.unwrap(),
        Some(b"from-a".to_vec())
    );
    assert_eq!(
        txn.get(TableId::Users, b"alice").await.unwrap(),
        Some(b"from-b".to_vec())
    );
}

#[tokio::test]
async fn transaction_retry_rechecks_guards_and_refuses_last_admin_delete() {
    let (_backend, control) = fault_store().await;
    let mut seed = control.begin(local()).await.unwrap();
    seed.put(TableId::Users, b"admin1", b"a1").await.unwrap();
    seed.put(TableId::Users, b"admin2", b"a2").await.unwrap();
    seed.commit().await.unwrap();

    // First closure call: a concurrent deletion of admin1 commits after this
    // transaction pinned the two-admin generation. The closure then stages the
    // delete of admin2 and conflicts. The retry re-reads one admin and refuses;
    // replaying the staged delete would remove the last admin.
    let writer = Arc::clone(&control);
    let mut injected = false;
    let result: Result<()> = control
        .transaction(local(), |txn| {
            let writer = Arc::clone(&writer);
            let conflict = !injected;
            injected = true;
            Box::pin(async move {
                if conflict {
                    let mut other = writer.begin(local()).await?;
                    assert!(other.delete(TableId::Users, b"admin1").await?);
                    other.commit().await?;
                }
                let admins = txn.scan(TableId::Users, &b""[..]..&b"\xff"[..]).await?;
                if admins.len() <= 1 {
                    return Err(Error::InvalidArgument(
                        "refusing to delete the last admin".into(),
                    ));
                }
                txn.delete(TableId::Users, b"admin2").await?;
                Ok(())
            })
        })
        .await;
    assert!(matches!(result, Err(Error::InvalidArgument(_))));

    let txn = control.begin(local()).await.unwrap();
    assert_eq!(txn.get(TableId::Users, b"admin1").await.unwrap(), None);
    assert_eq!(
        txn.get(TableId::Users, b"admin2").await.unwrap(),
        Some(b"a2".to_vec())
    );
}

#[tokio::test]
async fn direct_commit_conflict_is_retryable_and_does_not_replay_rows() {
    let (_backend, control) = fault_store().await;
    let mut a = control.begin(local()).await.unwrap();
    let mut b = control.begin(local()).await.unwrap();
    a.put(TableId::Buckets, b"k1", b"from-a").await.unwrap();
    b.put(TableId::Buckets, b"k2", b"from-b").await.unwrap();
    assert!(a.commit().await.unwrap().committed);

    assert!(matches!(b.commit().await, Err(Error::Conflict(_))));
    let txn = control.begin(local()).await.unwrap();
    assert_eq!(
        txn.get(TableId::Buckets, b"k1").await.unwrap(),
        Some(b"from-a".to_vec())
    );
    assert_eq!(txn.get(TableId::Buckets, b"k2").await.unwrap(), None);
    assert_eq!(control.pin().await.unwrap().version, 2);
}

#[tokio::test]
async fn conflict_after_another_apply_with_the_same_log_id_is_idempotent_success() {
    let (_backend, control) = fault_store().await;
    let log = AppliedLog { term: 2, index: 5 };
    let mut first = control.begin(WriteOrigin::raft_apply(log)).await.unwrap();
    let mut second = control.begin(WriteOrigin::raft_apply(log)).await.unwrap();
    first.put(TableId::Users, b"alice", b"row").await.unwrap();
    second.put(TableId::Users, b"bob", b"late").await.unwrap();
    assert!(first.commit().await.unwrap().committed);

    let stamp = second.commit().await.unwrap();
    assert!(!stamp.committed);
    assert_eq!(stamp.generation, 2);
    assert_eq!(stamp.applied, Some(log));
    let txn = control.begin(local()).await.unwrap();
    assert_eq!(
        txn.get(TableId::Users, b"alice").await.unwrap(),
        Some(b"row".to_vec())
    );
    assert_eq!(txn.get(TableId::Users, b"bob").await.unwrap(), None);
}

#[tokio::test]
async fn publisher_fires_once_per_successful_commit_not_on_noop_or_failure() {
    let backend = Arc::new(FaultBackend::new());
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&hits);
    let control = Arc::new(
        ObjectControlStore::new(Arc::new(RootIo::with_backend(backend_io(&backend))))
            .with_publisher(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
            }),
    );
    control.bootstrap(Mode::Standalone, 100).await.unwrap();

    // The first closure call commits another writer, so this transaction's
    // first commit attempt conflicts. Only its successful retry publishes.
    let writer = Arc::clone(&control);
    let mut injected = false;
    let result: Result<()> = control
        .transaction(local(), |txn| {
            let writer = Arc::clone(&writer);
            let conflict = !injected;
            injected = true;
            Box::pin(async move {
                if conflict {
                    let mut other = writer.begin(local()).await?;
                    other.put(TableId::Buckets, b"bucket", b"from-a").await?;
                    other.commit().await?;
                }
                txn.put(TableId::Users, b"alice", b"from-b").await?;
                Ok(())
            })
        })
        .await;
    result.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    // No-op (identical bytes) does not publish.
    let mut noop = control.begin(local()).await.unwrap();
    noop.put(TableId::Users, b"alice", b"from-b").await.unwrap();
    assert!(!noop.commit().await.unwrap().committed);
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    // A failed commit does not publish.
    backend.nodes.fail_after(Some(0));
    let mut failed = control.begin(local()).await.unwrap();
    failed
        .put(TableId::VpcEndpoints, b"endpoint", b"value")
        .await
        .unwrap();
    assert!(matches!(failed.commit().await, Err(Error::Io(_))));
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}
