//! Basic transaction, blob, registry and fail-closed tests.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use xl_store::control_state::blob::{CHUNK_SIZE, MAX_BLOB_BYTES};
use xl_store::control_state::hash::{ROOT_KEY, node_key};
use xl_store::control_state::io::NodeIo;
use xl_store::control_state::node::INLINE_MAX;
use xl_store::control_state::root::{AppliedLog, TableRoot, encode_root};
use xl_store::error::Error;
use xl_store::store::{Store, StoreConfig};

use super::{fault_store, local, real_store};
use crate::control_store_obj::{ObjectControlStore, RootIo, TableId, WriteOrigin};

#[tokio::test]
async fn empty_txn_does_not_commit_or_publish() {
    let real = real_store().await;
    let published = Arc::new(AtomicUsize::new(0));
    let hits = Arc::clone(&published);
    let control = Arc::new(
        ObjectControlStore::new(Arc::clone(&real.control.root)).with_publisher(move |_| {
            hits.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let stamp = control
        .begin(local())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert!(!stamp.committed);
    assert_eq!(stamp.generation, 1);
    assert_eq!(published.load(Ordering::SeqCst), 0);
    assert_eq!(control.pin().await.unwrap().version, 1);
}

#[tokio::test]
async fn one_row_put_persists_and_untouched_tables_keep_roots() {
    let real = real_store().await;
    let mut txn = real.control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"user-row")
        .await
        .unwrap();
    txn.put(TableId::Buckets, b"bucket-a", b"bucket-row")
        .await
        .unwrap();
    let first = txn.commit().await.unwrap();
    assert!(first.committed);
    assert_eq!(first.generation, 2);
    assert_eq!(first.dirty_tables, vec![1, 20]);
    let bucket_root = real
        .control
        .pin()
        .await
        .unwrap()
        .root
        .table(TableId::Buckets.as_u16())
        .cloned()
        .unwrap();

    let mut txn = real.control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"user-row-2")
        .await
        .unwrap();
    assert!(txn.commit().await.unwrap().committed);
    let pinned = real.control.pin().await.unwrap();
    assert_eq!(pinned.version, 3);
    assert_eq!(
        pinned.root.table(TableId::Buckets.as_u16()),
        Some(&bucket_root)
    );

    // Fresh store handle on the same drives: the row survives a restart.
    let mut cfg = StoreConfig::new(real._dir.path());
    cfg.fsync = false;
    let reopened_store = Arc::new(Store::open(cfg).await.unwrap());
    let reopened = Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(
        reopened_store,
    ))));
    let reopened_pin = reopened.pin().await.unwrap();
    assert_eq!(reopened_pin.version, 3);
    assert_eq!(reopened_pin.root.meta.created_at, 100);
    assert_eq!(
        reopened_pin.root.table(TableId::Buckets.as_u16()).cloned(),
        Some(bucket_root)
    );
    let txn = reopened.begin(local()).await.unwrap();
    assert_eq!(
        txn.get(TableId::Users, b"alice").await.unwrap(),
        Some(b"user-row-2".to_vec())
    );
    assert_eq!(
        txn.get(TableId::Buckets, b"bucket-a").await.unwrap(),
        Some(b"bucket-row".to_vec())
    );
}

#[tokio::test]
async fn overlay_scan_merges_tree_and_delete_removes() {
    let real = real_store().await;
    let mut txn = real.control.begin(local()).await.unwrap();
    txn.put(TableId::Buckets, b"a", b"1").await.unwrap();
    txn.put(TableId::Buckets, b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let mut txn = real.control.begin(local()).await.unwrap();
    assert!(txn.delete(TableId::Buckets, b"c").await.unwrap());
    assert!(!txn.delete(TableId::Buckets, b"missing").await.unwrap());
    txn.put(TableId::Buckets, b"b", b"2").await.unwrap();
    assert_eq!(txn.get(TableId::Buckets, b"c").await.unwrap(), None);
    let got = txn
        .scan(TableId::Buckets, &b"a"[..]..&b"z"[..])
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ]
    );
    txn.commit().await.unwrap();

    let txn = real.control.begin(local()).await.unwrap();
    assert_eq!(txn.get(TableId::Buckets, b"c").await.unwrap(), None);
    assert_eq!(
        txn.scan(TableId::Buckets, &b""[..]..&b"z"[..])
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn blob_roundtrip_and_24mib_cap() {
    let real = real_store().await;
    // Two chunks: values above the inline cap must round-trip through BlobRef.
    let value: Vec<u8> = (0..CHUNK_SIZE + INLINE_MAX)
        .map(|i| (i.wrapping_mul(17) % 251) as u8)
        .collect();
    let mut txn = real.control.begin(local()).await.unwrap();
    txn.put(TableId::OlapConfigs, b"module", &value)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = real.control.begin(local()).await.unwrap();
    assert_eq!(
        txn.get(TableId::OlapConfigs, b"module").await.unwrap(),
        Some(value)
    );

    let mut txn = real.control.begin(local()).await.unwrap();
    assert!(matches!(
        txn.put(
            TableId::OlapConfigs,
            b"too-big",
            &vec![0u8; MAX_BLOB_BYTES + 1]
        )
        .await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(!txn.commit().await.unwrap().committed);
}

#[tokio::test]
async fn identical_put_is_a_noop_without_cas_or_publication() {
    let real = real_store().await;
    let published = Arc::new(AtomicUsize::new(0));
    let hits = Arc::clone(&published);
    let control = Arc::new(
        ObjectControlStore::new(Arc::clone(&real.control.root)).with_publisher(move |_| {
            hits.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    assert!(txn.commit().await.unwrap().committed);

    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    let stamp = txn.commit().await.unwrap();
    assert!(!stamp.committed);
    assert_eq!(stamp.generation, 2);
    assert_eq!(published.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stale_version_and_fence_are_rejected() {
    let real = real_store().await;
    let root = RootIo::new(Arc::clone(&real.store));
    let pinned = root.load().await.unwrap().unwrap();
    assert!(matches!(
        root.cas_root(pinned.version + 1, pinned.fence_token + 2, &pinned.bytes)
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        root.cas_root(pinned.version, pinned.fence_token, &pinned.bytes)
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(root.load().await.unwrap().unwrap().version, pinned.version);
}

#[tokio::test]
async fn content_objects_are_create_once() {
    let real = real_store().await;
    let root = RootIo::new(Arc::clone(&real.store));
    let key = node_key(&[7u8; 32]);
    root.put_create(&key, b"same").await.unwrap();
    root.put_create(&key, b"same").await.unwrap();
    assert!(matches!(
        root.put_create(&key, b"different").await,
        Err(Error::CorruptMeta(_))
    ));
}

#[tokio::test]
async fn root_read_back_mismatch_fails_closed() {
    let (backend, control) = fault_store().await;
    backend.corrupt_next_root();
    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    assert!(matches!(txn.commit().await, Err(Error::CorruptMeta(_))));
}

#[tokio::test]
async fn applied_membership_and_allocators_commit_in_one_root_cas() {
    let real = real_store().await;
    let log = AppliedLog { term: 3, index: 9 };
    let mut txn = real
        .control
        .begin(WriteOrigin::raft_apply(log))
        .await
        .unwrap();
    assert_eq!(txn.alloc_uid().unwrap(), 1);
    txn.set_membership(Some(r#"{"v":1}"#.into()));
    txn.set_marker("initialized", "1");
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    let stamp = txn.commit().await.unwrap();
    assert!(stamp.committed);
    assert_eq!(stamp.applied, Some(log));

    let pinned = real.control.pin().await.unwrap();
    assert_eq!(pinned.root.meta.applied, Some(log));
    assert_eq!(pinned.root.meta.next_uid, 2);
    assert_eq!(
        pinned.root.meta.membership_json.as_deref(),
        Some(r#"{"v":1}"#)
    );
    assert_eq!(
        pinned
            .root
            .meta
            .markers
            .get("initialized")
            .map(String::as_str),
        Some("1")
    );

    // Replay of an already-applied entry is a no-op: its staged rows never land.
    let mut txn = real
        .control
        .begin(WriteOrigin::raft_apply(log))
        .await
        .unwrap();
    txn.put(TableId::Users, b"bob", b"late").await.unwrap();
    let stamp = txn.commit().await.unwrap();
    assert!(!stamp.committed);
    assert_eq!(stamp.generation, pinned.version);
    let txn = real.control.begin(local()).await.unwrap();
    assert_eq!(txn.get(TableId::Users, b"bob").await.unwrap(), None);
}

#[tokio::test]
async fn unknown_table_is_preserved_but_export_gated() {
    let real = real_store().await;
    let pinned = real.control.pin().await.unwrap();
    let preserved = TableRoot {
        hash: [7; 32],
        rows: 1,
        bytes: 1,
        required_minor: 0,
    };
    let mut root = pinned.root.clone();
    root.set_table(999, preserved.clone());
    let bytes = encode_root(&root).unwrap();
    real.store
        .put_local_control(
            ROOT_KEY,
            &bytes,
            Some(pinned.version),
            pinned.fence_token + 1,
        )
        .await
        .unwrap();

    let control = Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(Arc::clone(
        &real.store,
    )))));
    let mut txn = control.begin(local()).await.unwrap();
    txn.put(TableId::Users, b"alice", b"row").await.unwrap();
    assert!(txn.commit().await.unwrap().committed);
    assert_eq!(
        control.pin().await.unwrap().root.table(999),
        Some(&preserved)
    );
    assert!(matches!(
        control.check_compatible().await,
        Err(Error::Unsupported(_))
    ));
}

#[tokio::test]
async fn newer_required_minor_fails_closed() {
    let real = real_store().await;
    let pinned = real.control.pin().await.unwrap();
    let mut root = pinned.root.clone();
    root.set_table(
        998,
        TableRoot {
            hash: [8; 32],
            rows: 1,
            bytes: 1,
            required_minor: 1,
        },
    );
    let bytes = encode_root(&root).unwrap();
    real.store
        .put_local_control(
            ROOT_KEY,
            &bytes,
            Some(pinned.version),
            pinned.fence_token + 1,
        )
        .await
        .unwrap();
    let root_io = RootIo::new(Arc::clone(&real.store));
    assert!(matches!(root_io.load().await, Err(Error::Unsupported(_))));
    let control = ObjectControlStore::new(Arc::new(root_io));
    assert!(matches!(control.pin().await, Err(Error::Unsupported(_))));
}

#[test]
fn table_registry_is_fixed_and_unique() {
    assert_eq!(TableId::Users.as_u16(), 1);
    assert_eq!(TableId::AccessKeys.as_u16(), 2);
    assert_eq!(TableId::Buckets.as_u16(), 20);
    assert_eq!(TableId::Jobs.as_u16(), 40);
    assert_eq!(TableId::SiteEntityClock.as_u16(), 53);
    assert_eq!(TableId::VpcEndpoints.as_u16(), 60);
    assert_eq!(TableId::from_u16(4), None);

    let ids: Vec<u16> = TableId::ALL.iter().map(|t| t.as_u16()).collect();
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    for id in &ids {
        assert_eq!(TableId::from_u16(*id).unwrap().as_u16(), *id);
    }
    let names: BTreeSet<&str> = TableId::ALL.iter().map(|t| t.name()).collect();
    assert_eq!(names.len(), ids.len());
}
