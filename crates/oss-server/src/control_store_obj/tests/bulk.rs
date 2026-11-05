//! Bulk install path: 100k-row bound and the root-CAS crash matrix.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use xl_store::control_state::root::Mode;
use xl_store::error::Error;
use xl_store::store::{Store, StoreConfig};

use super::{fault_store, local, reopen_fault};
use crate::control_store_obj::{ObjectControlStore, RootIo, TableId};

const PER_TABLE: u64 = 50_000;
const ROW_BYTES: usize = 96;

fn range_end() -> [u8; 256] {
    [0xffu8; 256]
}

async fn fsync_store() -> (TempDir, Arc<ObjectControlStore>) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = true;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let control = Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(store))));
    control.bootstrap(Mode::Standalone, 100).await.unwrap();
    (dir, control)
}

#[tokio::test]
async fn bulk_install_100k_rows_commits_in_order_within_budget() {
    let (_dir, control) = fsync_store().await;
    let mut users = Vec::with_capacity(PER_TABLE as usize);
    let mut keys = Vec::with_capacity(PER_TABLE as usize);
    let started = Instant::now();

    let mut txn = control.begin(local()).await.unwrap();
    txn.bulk_install();
    for i in 0..PER_TABLE {
        let key = i.to_be_bytes().to_vec();
        let value = vec![(i % 251) as u8; ROW_BYTES];
        txn.put(TableId::Users, &key, &value).await.unwrap();
        users.push((key, value));
    }
    for i in 0..PER_TABLE {
        let key = format!("AKIA{i:016}").into_bytes();
        let value = vec![(i % 251) as u8; ROW_BYTES];
        txn.put(TableId::AccessKeys, &key, &value).await.unwrap();
        keys.push((key, value));
    }
    txn.commit().await.unwrap();
    let elapsed = started.elapsed();
    eprintln!("bulk install {} rows: {elapsed:?}", PER_TABLE * 2);

    // Debug builds pay unoptimized rmp encode/decode per 32 KiB node; the
    // release benchmark (scripts/bench_control_store.sh) covers the 10s design
    // budget. This bound still fails loudly if the per-row path-copy returns.
    assert!(
        elapsed < Duration::from_secs(60),
        "100k-row bulk install took {elapsed:?} in a debug build"
    );

    let end = range_end();
    assert_eq!(
        control
            .read_range(TableId::Users, &b""[..]..&end[..])
            .await
            .unwrap(),
        users
    );
    assert_eq!(
        control
            .read_range(TableId::AccessKeys, &b""[..]..&end[..])
            .await
            .unwrap(),
        keys
    );

    let pinned = control.pin().await.unwrap();
    for table in [TableId::Users, TableId::AccessKeys] {
        let root = pinned.root.table(table.as_u16()).unwrap();
        assert_eq!(root.rows, PER_TABLE);
        assert_eq!(root.bytes, PER_TABLE * ROW_BYTES as u64);
    }
}

#[tokio::test]
async fn bulk_install_root_cas_indeterminate_is_old_or_new() {
    for landed in [false, true] {
        let (backend, control) = fault_store().await;
        let mut txn = control.begin(local()).await.unwrap();
        txn.put(TableId::Users, b"old", b"old-row").await.unwrap();
        txn.commit().await.unwrap();

        if landed {
            backend.roots.fail_next_cas_partially();
        } else {
            backend
                .indeterminate_without_store
                .store(true, Ordering::SeqCst);
        }

        let mut txn = control.begin(local()).await.unwrap();
        txn.bulk_install();
        for i in 0..1000u64 {
            txn.put(TableId::Users, &i.to_be_bytes(), b"bulk-row")
                .await
                .unwrap();
        }
        let result = txn.commit().await;
        if landed {
            assert!(result.unwrap().committed);
        } else {
            assert!(matches!(result, Err(Error::Conflict(_))));
        }
        // Old nodes + one bottom-up rebuild, never one write per row.
        assert!(
            backend.nodes.puts() < 100,
            "wrote {} nodes",
            backend.nodes.puts()
        );

        let reopened = reopen_fault(&backend);
        let end = range_end();
        let rows = reopened
            .read_range(TableId::Users, &b""[..]..&end[..])
            .await
            .unwrap();
        let old = (b"old".to_vec(), b"old-row".to_vec());
        if landed {
            assert_eq!(rows.len(), 1001);
            assert!(rows.contains(&old));
            assert_eq!(
                rows.iter()
                    .filter(|(_, v)| v.as_slice() == b"bulk-row")
                    .count(),
                1000
            );
        } else {
            assert_eq!(rows, vec![old]);
        }
    }
}
