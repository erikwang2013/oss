use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use super::*;
use crate::control_state::blob::BlobRef;
use crate::control_state::hash;
use crate::control_state::io::NodeIo;
use crate::control_state::node::{
    self, INLINE_MAX, LeafEntry, LeafValue, Node, NodeEntries, NodeKind,
};
use crate::error::Error;

#[derive(Default)]
struct TestIo {
    map: Mutex<BTreeMap<String, Vec<u8>>>,
    writes: Mutex<usize>,
    fail_after: Mutex<Option<usize>>,
}

#[async_trait]
impl NodeIo for TestIo {
    async fn get(&self, key: &str) -> crate::error::Result<Option<Vec<u8>>> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }

    async fn put_create(&self, key: &str, bytes: &[u8]) -> crate::error::Result<()> {
        let mut writes = self.writes.lock().unwrap();
        if let Some(limit) = *self.fail_after.lock().unwrap()
            && *writes >= limit
        {
            return Err(Error::Io(std::io::Error::other("injected put failure")));
        }
        let mut map = self.map.lock().unwrap();
        if let Some(existing) = map.get(key) {
            if existing.as_slice() != bytes {
                return Err(Error::CorruptMeta(
                    "duplicate create with different bytes".into(),
                ));
            }
            return Ok(());
        }
        map.insert(key.to_owned(), bytes.to_vec());
        *writes += 1;
        Ok(())
    }
}

impl TestIo {
    fn writes(&self) -> usize {
        *self.writes.lock().unwrap()
    }

    fn force_put(&self, key: &str, bytes: &[u8]) {
        self.map
            .lock()
            .unwrap()
            .insert(key.to_owned(), bytes.to_vec());
    }
}

fn row(value: &[u8]) -> LeafValue {
    LeafValue::Row(value.to_vec())
}

fn key(n: u32) -> Vec<u8> {
    format!("k{n:04}").into_bytes()
}

async fn insert_range(tree: &mut TableTree, io: &TestIo, n: u32, value_len: usize) {
    for i in 0..n {
        tree.stage_put(&key(i), row(&vec![(i % 251) as u8; value_len]));
    }
    tree.flush(io).await.unwrap();
}

#[tokio::test]
async fn empty_tree_has_no_root_and_writes_nothing() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    assert_eq!(tree.get(&io, b"a").await.unwrap(), None);
    assert!(tree.get_leaf(&io, b"a").await.unwrap().is_none());
    assert!(
        tree.scan(&io, &b""[..]..&b"z"[..])
            .await
            .unwrap()
            .is_empty()
    );
    tree.flush(&io).await.unwrap();
    assert_eq!(io.writes(), 0);
    assert_eq!(tree.root_hash(), None);
    assert_eq!(tree.rows(), 0);
}

#[tokio::test]
async fn put_is_visible_before_flush_and_persists_after() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    tree.stage_put(b"b", row(b"two"));
    tree.stage_put(b"a", row(b"one"));
    assert_eq!(tree.get(&io, b"b").await.unwrap(), Some(b"two".to_vec()));
    assert_eq!(tree.rows(), 0); // root counters change only on flush
    tree.flush(&io).await.unwrap();
    assert_eq!(tree.rows(), 2);
    assert_eq!(tree.bytes(), 6);
    assert!(tree.root_hash().is_some());

    let reopened = TableTree::from_root(tree.root_hash().unwrap(), tree.rows(), tree.bytes());
    assert_eq!(
        reopened.get(&io, b"a").await.unwrap(),
        Some(b"one".to_vec())
    );
    assert_eq!(
        reopened.get(&io, b"b").await.unwrap(),
        Some(b"two".to_vec())
    );
    assert_eq!(reopened.get(&io, b"c").await.unwrap(), None);
}

#[tokio::test]
async fn overlay_read_your_writes_and_deletes() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    tree.stage_put(b"a", row(b"one"));
    tree.stage_put(b"b", row(b"two"));
    tree.flush(&io).await.unwrap();

    tree.stage_delete(b"a");
    tree.stage_put(b"c", row(b"three"));
    tree.stage_delete(b"missing");
    assert_eq!(tree.get(&io, b"a").await.unwrap(), None);
    assert_eq!(tree.get(&io, b"c").await.unwrap(), Some(b"three".to_vec()));
    assert_eq!(tree.rows(), 2); // still the flushed counters
    tree.flush(&io).await.unwrap();
    assert_eq!(tree.rows(), 2);
    assert_eq!(tree.bytes(), 8);
    assert_eq!(tree.get(&io, b"a").await.unwrap(), None);

    // Delete then put the same key inside one overlay wins.
    tree.stage_delete(b"b");
    tree.stage_put(b"b", row(b"two"));
    tree.stage_delete(b"b");
    tree.stage_put(b"b", row(b"TWO"));
    tree.flush(&io).await.unwrap();
    assert_eq!(tree.get(&io, b"b").await.unwrap(), Some(b"TWO".to_vec()));
}

#[tokio::test]
async fn range_scan_is_ordered_and_end_exclusive() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    for k in [b"c", b"a", b"d", b"b"] {
        tree.stage_put(k, row(k));
    }
    tree.flush(&io).await.unwrap();
    let all = tree.scan(&io, &b"a"[..]..&b"z"[..]).await.unwrap();
    let keys: Vec<Vec<u8>> = all.into_iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
    );

    let mid = tree.scan(&io, &b"b"[..]..&b"d"[..]).await.unwrap();
    assert_eq!(
        mid.into_iter().map(|(k, _)| k).collect::<Vec<_>>(),
        vec![b"b".to_vec(), b"c".to_vec()]
    );
    assert!(
        tree.scan(&io, &b"z"[..]..&b"a"[..])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn staged_values_merge_into_scan() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    tree.stage_put(b"a", row(b"1"));
    tree.stage_put(b"c", row(b"3"));
    tree.flush(&io).await.unwrap();

    tree.stage_put(b"b", row(b"2"));
    tree.stage_delete(b"c");
    let got = tree.scan_leaf(&io, &b""[..]..&b"z"[..]).await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].0, b"a");
    assert_eq!(got[1].0, b"b");
}

#[tokio::test]
async fn splits_large_leaves_and_keeps_all_rows() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    insert_range(&mut tree, &io, 900, 200).await;
    assert!(tree.depth(&io).await.unwrap() >= 2);
    assert_eq!(tree.rows(), 900);
    for i in 0..900 {
        assert!(
            tree.get(&io, &key(i)).await.unwrap().is_some(),
            "missing key {i}"
        );
    }
    let all = tree.scan(&io, &b""[..]..&b"zzzz"[..]).await.unwrap();
    assert_eq!(all.len(), 900)
}

#[tokio::test]
async fn deletes_merge_and_leave_a_usable_tree() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    insert_range(&mut tree, &io, 600, 200).await;
    let before = tree.depth(&io).await.unwrap();
    for i in 0..550 {
        tree.stage_delete(&key(i));
    }
    tree.flush(&io).await.unwrap();
    assert_eq!(tree.rows(), 50);
    for i in 0..550 {
        assert!(tree.get(&io, &key(i)).await.unwrap().is_none());
    }
    for i in 550..600 {
        assert!(tree.get(&io, &key(i)).await.unwrap().is_some());
    }
    assert!(tree.depth(&io).await.unwrap() <= before);

    // Every persisted node is a decodable node within the hard cap.
    for (k, v) in io.map.lock().unwrap().iter() {
        if k.starts_with(hash::NODE_PREFIX) {
            let n = node::decode_node(v).unwrap();
            assert!(node::encoded_size(&n).unwrap() <= node::NODE_SPLIT_BYTES);
            assert!(!matches!(n.entries, NodeEntries::Leaf(ref e) if e.is_empty()));
        }
    }
}

#[tokio::test]
async fn single_key_flush_writes_only_the_path() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    insert_range(&mut tree, &io, 500, 100).await;
    let depth = tree.depth(&io).await.unwrap() as usize;
    let before = io.writes();
    tree.stage_put(b"new-key", row(b"v"));
    tree.flush(&io).await.unwrap();
    let writes = io.writes() - before;
    assert!(writes >= 1);
    assert!(
        writes <= 2 * depth + 1,
        "one-key flush wrote {writes} nodes at depth {depth}"
    );
}

#[tokio::test]
async fn interrupted_flush_keeps_old_root_usable_and_retries() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    insert_range(&mut tree, &io, 400, 100).await;
    let old_root = tree.root_hash().unwrap();
    let old_rows = tree.rows();

    *io.fail_after.lock().unwrap() = Some(io.writes() + 3);
    for i in 0..100 {
        tree.stage_put(&key(10_000 + i), row(b"x"));
    }
    let err = tree.flush(&io).await;
    assert!(err.is_err());
    assert_eq!(tree.root_hash(), Some(old_root));
    assert_eq!(tree.rows(), old_rows);
    assert!(tree.get(&io, &key(0)).await.unwrap().is_some());
    // The failed batch stays in the overlay for read-your-writes/retry, but the
    // persisted generation (reopened from the old root) must not contain it.
    let persisted = TableTree::from_root(old_root, old_rows, tree.bytes());
    assert_eq!(persisted.get(&io, &key(10_000)).await.unwrap(), None);
    assert!(
        tree.get(&io, &key(10_000)).await.unwrap().is_some(),
        "uncommitted overlay is retained for retry"
    );
    assert_eq!(tree.staged_len(), 100);

    *io.fail_after.lock().unwrap() = None;
    tree.flush(&io).await.unwrap();
    assert_eq!(tree.rows(), 500);
    assert!(tree.get(&io, &key(10_099)).await.unwrap().is_some());
}

#[tokio::test]
async fn identical_content_deduplicates_create_once() {
    let io = TestIo::default();
    let mut a = TableTree::new();
    a.stage_put(b"a", row(b"1"));
    a.flush(&io).await.unwrap();
    let after_first = io.writes();

    let mut b = TableTree::new();
    b.stage_put(b"a", row(b"1"));
    b.flush(&io).await.unwrap();
    assert_eq!(
        io.writes(),
        after_first,
        "same bytes must not be re-written"
    );
    assert_eq!(a.root_hash(), b.root_hash());
}

#[tokio::test]
async fn tombstone_entries_are_compacted_on_rewrite() {
    let io = TestIo::default();
    let leaf = Node {
        kind: NodeKind::Leaf,
        level: 0,
        entries: NodeEntries::Leaf(vec![
            LeafEntry {
                key: b"a".to_vec(),
                value: row(b"1"),
            },
            LeafEntry {
                key: b"b".to_vec(),
                value: LeafValue::Tombstone,
            },
        ]),
    };
    let bytes = node::encode_node(&leaf).unwrap();
    let hash = hash::sha256(&bytes);
    io.force_put(&hash::node_key(&hash), &bytes);

    let mut tree = TableTree::from_root(hash, 1, 1);
    assert_eq!(tree.get(&io, b"b").await.unwrap(), None);
    tree.stage_put(b"c", row(b"3"));
    tree.flush(&io).await.unwrap();

    let root = tree.root_hash().unwrap();
    let bytes = io
        .map
        .lock()
        .unwrap()
        .get(&hash::node_key(&root))
        .cloned()
        .unwrap();
    let node = node::decode_node(&bytes).unwrap();
    let NodeEntries::Leaf(entries) = node.entries else {
        panic!("expected leaf root");
    };
    let keys: Vec<Vec<u8>> = entries.into_iter().map(|e| e.key).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
    assert_eq!(tree.rows(), 2);
}

#[tokio::test]
async fn blob_leaves_are_not_silently_returned_as_rows() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    let blob = BlobRef {
        sha256: hash::sha256(b"hello"),
        len: 5,
        chunks: 1,
    };
    tree.stage_put(b"b", LeafValue::Blob(blob));
    assert!(matches!(
        tree.get(&io, b"b").await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(
        tree.get_leaf(&io, b"b").await.unwrap(),
        Some(LeafValue::Blob(blob))
    );
    tree.flush(&io).await.unwrap();
    let reopened = TableTree::from_root(tree.root_hash().unwrap(), 1, 5);
    assert!(matches!(
        reopened.scan(&io, &b""[..]..&b"z"[..]).await,
        Err(Error::Unsupported(_))
    ));
    let scanned = reopened.scan_leaf(&io, &b""[..]..&b"z"[..]).await.unwrap();
    assert_eq!(scanned, vec![(b"b".to_vec(), LeafValue::Blob(blob))]);
}

#[tokio::test]
async fn missing_or_corrupt_node_is_corrupt_meta() {
    let io = TestIo::default();
    let tree = TableTree::from_root([9; 32], 0, 0);
    assert!(matches!(
        tree.get(&io, b"a").await,
        Err(Error::CorruptMeta(_))
    ));

    let mut a = TableTree::new();
    a.stage_put(b"a", row(b"1"));
    a.flush(&io).await.unwrap();
    let root = a.root_hash().unwrap();
    let key = hash::node_key(&root);
    let mut bytes = io.map.lock().unwrap().get(&key).cloned().unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    io.force_put(&key, &bytes);
    assert!(matches!(a.get(&io, b"a").await, Err(Error::CorruptMeta(_))));
}

#[tokio::test]
async fn inline_cap_is_enforced_at_flush() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    tree.stage_put(b"ok", row(&vec![0u8; INLINE_MAX]));
    tree.flush(&io).await.unwrap();
    tree.stage_put(b"bad", row(&vec![0u8; INLINE_MAX + 1]));
    assert!(matches!(
        tree.flush(&io).await,
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(tree.staged_len(), 1);
}

mod randomized;
