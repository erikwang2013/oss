//! Crash matrix: nodes land before the root, and the root is the only commit
//! point. Every case reopens from the same backend and must see exactly the old
//! or the new generation, never a mixture.

use std::sync::atomic::Ordering;

use xl_store::error::Error;

use super::{fault_store, local, reopen_fault};
use crate::control_store_obj::TableId;

#[tokio::test]
async fn crash_before_any_node_keeps_old_generation() {
    let (backend, control) = fault_store().await;
    backend.nodes.fail_after(Some(0));
    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    assert!(matches!(txn.commit().await, Err(Error::Io(_))));
    assert_eq!(backend.nodes.puts(), 0);

    let reopened = reopen_fault(&backend);
    assert_eq!(reopened.pin().await.unwrap().version, 1);
    let txn = reopened.begin(local()).await.unwrap();
    assert_eq!(txn.get(TableId::Users, b"alice").await.unwrap(), None);
}

#[tokio::test]
async fn crash_after_some_nodes_before_root_keeps_old_generation() {
    let (backend, control) = fault_store().await;
    backend.nodes.fail_after(Some(1));
    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    txn.put(TableId::Buckets, b"bucket", b"row").await.unwrap();
    // Tables flush in id order: Users lands, Buckets fails, the root never moves.
    assert!(matches!(txn.commit().await, Err(Error::Io(_))));
    assert_eq!(backend.nodes.puts(), 1);

    let reopened = reopen_fault(&backend);
    let pinned = reopened.pin().await.unwrap();
    assert_eq!(pinned.version, 1);
    assert!(pinned.root.tables.is_empty());
    let txn = reopened.begin(local()).await.unwrap();
    assert_eq!(txn.get(TableId::Users, b"alice").await.unwrap(), None);
    assert_eq!(txn.get(TableId::Buckets, b"bucket").await.unwrap(), None);
}

#[tokio::test]
async fn indeterminate_root_cas_that_landed_commits_the_new_generation() {
    let (backend, control) = fault_store().await;
    backend.roots.fail_next_cas_partially();
    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    txn.put(TableId::Buckets, b"bucket", b"row").await.unwrap();
    let stamp = txn.commit().await.unwrap();
    assert!(stamp.committed);
    assert_eq!(stamp.generation, 2);

    let reopened = reopen_fault(&backend);
    assert_eq!(reopened.pin().await.unwrap().version, 2);
    let txn = reopened.begin(local()).await.unwrap();
    assert_eq!(
        txn.get(TableId::Users, b"alice").await.unwrap(),
        Some(b"row".to_vec())
    );
    assert_eq!(
        txn.get(TableId::Buckets, b"bucket").await.unwrap(),
        Some(b"row".to_vec())
    );
}

#[tokio::test]
async fn indeterminate_root_cas_that_did_not_land_fails_closed() {
    let (backend, control) = fault_store().await;
    backend
        .indeterminate_without_store
        .store(true, Ordering::SeqCst);
    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    assert!(matches!(txn.commit().await, Err(Error::Conflict(_))));

    let reopened = reopen_fault(&backend);
    let pinned = reopened.pin().await.unwrap();
    assert_eq!(pinned.version, 1);
    assert!(pinned.root.tables.is_empty());
    let txn = reopened.begin(local()).await.unwrap();
    assert_eq!(txn.get(TableId::Users, b"alice").await.unwrap(), None);
}

#[tokio::test]
async fn committed_root_is_readable_from_a_fresh_handle() {
    let (backend, control) = fault_store().await;
    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    txn.put(TableId::Buckets, b"bucket", b"row").await.unwrap();
    assert_eq!(txn.commit().await.unwrap().generation, 2);
    // One leaf node per touched table was written before the root CAS.
    assert_eq!(backend.nodes.puts(), 2);

    let reopened = reopen_fault(&backend);
    let txn = reopened.begin(local()).await.unwrap();
    assert_eq!(
        txn.get(TableId::Users, b"alice").await.unwrap(),
        Some(b"row".to_vec())
    );
    assert_eq!(
        txn.get(TableId::Buckets, b"bucket").await.unwrap(),
        Some(b"row".to_vec())
    );
}
