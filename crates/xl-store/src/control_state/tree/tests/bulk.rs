//! Bulk rebuild: overlay merge, write amplification, and failure atomicity.

use super::*;

#[tokio::test]
async fn bulk_flush_merges_overlay_and_rebuilds() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    insert_range(&mut tree, &io, 200, 40).await;

    tree.stage_delete(&key(5));
    tree.stage_delete(&key(199));
    tree.stage_put(&key(7), row(b"updated"));
    tree.stage_put(b"new-key", row(b"new"));
    tree.stage_delete(b"absent");
    tree.bulk_rebuild();
    tree.flush(&io).await.unwrap();

    let mut want: BTreeMap<Vec<u8>, Vec<u8>> = (0..200)
        .filter(|i| *i != 5 && *i != 199)
        .map(|i| (key(i), vec![(i % 251) as u8; 40]))
        .collect();
    want.insert(key(7), b"updated".to_vec());
    want.insert(b"new-key".to_vec(), b"new".to_vec());
    let want: Vec<_> = want.into_iter().collect();

    let got = tree.scan(&io, &b""[..]..&b"z"[..]).await.unwrap();
    assert_eq!(got, want);
    assert_eq!(tree.rows(), want.len() as u64);
    assert_eq!(
        tree.bytes(),
        want.iter().map(|(_, v)| v.len() as u64).sum::<u64>()
    );

    let reopened = TableTree::from_root(tree.root_hash().unwrap(), tree.rows(), tree.bytes());
    assert_eq!(reopened.scan(&io, &b""[..]..&b"z"[..]).await.unwrap(), want);
}

#[tokio::test]
async fn bulk_flush_writes_each_node_once() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    for i in 0..5000 {
        tree.stage_put(&key(i), row(&[(i % 251) as u8; 100]));
    }
    tree.bulk_rebuild();
    tree.flush(&io).await.unwrap();

    assert_eq!(tree.rows(), 5000);
    // Path copying would write thousands of nodes; bottom-up writes leaves and
    // a handful of internals. Each key is inserted once (see TestIo::put_create).
    assert!(io.writes() < 100, "bulk wrote {} nodes", io.writes());
}

#[tokio::test]
async fn bulk_flush_failure_keeps_old_root_and_overlay() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    insert_range(&mut tree, &io, 100, 40).await;
    let root = tree.root_hash();
    let (rows, bytes) = (tree.rows(), tree.bytes());
    let writes = io.writes();

    tree.stage_put(&key(1000), row(b"a"));
    tree.stage_put(&key(1001), row(b"b"));
    tree.stage_delete(&key(3));
    tree.bulk_rebuild();
    *io.fail_after.lock().unwrap() = Some(writes);
    assert!(tree.flush(&io).await.is_err());
    assert_eq!(tree.root_hash(), root);
    assert_eq!((tree.rows(), tree.bytes()), (rows, bytes));
    assert_eq!(tree.staged_len(), 3);
    assert_eq!(
        tree.get(&io, &key(1000)).await.unwrap(),
        Some(b"a".to_vec())
    );

    *io.fail_after.lock().unwrap() = None;
    tree.flush(&io).await.unwrap();
    assert_eq!(tree.rows(), rows + 1);
    assert_eq!(
        tree.get(&io, &key(1000)).await.unwrap(),
        Some(b"a".to_vec())
    );
}

#[tokio::test]
async fn bulk_flush_deleting_every_row_clears_the_root() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    insert_range(&mut tree, &io, 20, 8).await;
    for i in 0..20 {
        tree.stage_delete(&key(i));
    }
    tree.bulk_rebuild();
    tree.flush(&io).await.unwrap();
    assert_eq!(tree.root_hash(), None);
    assert_eq!((tree.rows(), tree.bytes()), (0, 0));
    assert!(
        tree.scan(&io, &b""[..]..&b"z"[..])
            .await
            .unwrap()
            .is_empty()
    );
}
