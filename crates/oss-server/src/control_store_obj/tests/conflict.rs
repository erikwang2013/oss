//! Conflict retry against a concurrent writer (design §2.2 step 5) and cache
//! publication timing.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use xl_store::control_state::root::Mode;
use xl_store::error::Error;

use super::{FaultBackend, backend_io, fault_store, local};
use crate::control_store_obj::{ObjectControlStore, RootIo, TableId};

#[tokio::test]
async fn conflicting_writers_keep_both_changes_and_untouched_roots() {
    let (_backend, control) = fault_store().await;
    let mut a = control.begin(local()).await.unwrap();
    let mut b = control.begin(local()).await.unwrap();
    a.put(TableId::Buckets, b"bucket", b"from-a").await.unwrap();
    b.put(TableId::Users, b"alice", b"from-b").await.unwrap();

    let stamp_a = a.commit().await.unwrap();
    assert!(stamp_a.committed);
    assert_eq!(stamp_a.generation, 2);
    let bucket_root = control
        .pin()
        .await
        .unwrap()
        .root
        .table(TableId::Buckets.as_u16())
        .cloned()
        .unwrap();

    // b pinned generation 1: its CAS conflicts, it rebases onto A's root and
    // retries, so A's table survives byte-for-byte.
    let stamp_b = b.commit().await.unwrap();
    assert!(stamp_b.committed);
    assert_eq!(stamp_b.generation, 3);
    let pinned = control.pin().await.unwrap();
    assert_eq!(pinned.version, 3);
    assert_eq!(
        pinned.root.table(TableId::Buckets.as_u16()),
        Some(&bucket_root)
    );
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
async fn same_table_conflict_replays_the_overlay_on_the_new_root() {
    let (_backend, control) = fault_store().await;
    let mut a = control.begin(local()).await.unwrap();
    let mut b = control.begin(local()).await.unwrap();
    a.put(TableId::Buckets, b"k1", b"from-a").await.unwrap();
    b.put(TableId::Buckets, b"k2", b"from-b").await.unwrap();
    assert!(a.commit().await.unwrap().committed);
    let stamp_b = b.commit().await.unwrap();
    assert!(stamp_b.committed);
    assert_eq!(stamp_b.generation, 3);

    let txn = control.begin(local()).await.unwrap();
    assert_eq!(
        txn.scan(TableId::Buckets, &b"k0"[..]..&b"k9"[..])
            .await
            .unwrap(),
        vec![
            (b"k1".to_vec(), b"from-a".to_vec()),
            (b"k2".to_vec(), b"from-b".to_vec()),
        ]
    );
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

    let mut a = control.begin(local()).await.unwrap();
    let mut b = control.begin(local()).await.unwrap();
    a.put(TableId::Buckets, b"bucket", b"from-a").await.unwrap();
    b.put(TableId::Users, b"alice", b"from-b").await.unwrap();
    assert!(a.commit().await.unwrap().committed);
    assert!(b.commit().await.unwrap().committed);
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
