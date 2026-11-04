use super::*;
use crate::control_state::hash;
use crate::control_state::io::{NodeIo, get_node, put_node};
use crate::control_state::node::{LeafEntry, LeafValue, Node, NodeEntries, NodeKind};
use crate::error::Error;

fn leaf(key: &[u8], value: &[u8]) -> Node {
    Node {
        kind: NodeKind::Leaf,
        level: 0,
        entries: NodeEntries::Leaf(vec![LeafEntry {
            key: key.to_vec(),
            value: LeafValue::Row(value.to_vec()),
        }]),
    }
}

#[tokio::test]
async fn ordered_store_and_create_once_semantics() {
    let io = FaultIo::new();
    io.put_create("b", b"2").await.unwrap();
    io.put_create("a", b"1").await.unwrap();
    assert_eq!(io.keys(), vec!["a".to_string(), "b".to_string()]);
    assert_eq!(io.puts(), 2);

    io.put_create("a", b"1").await.unwrap();
    assert_eq!(io.puts(), 2, "identical duplicate is a no-op");
    assert!(matches!(
        io.put_create("a", b"other").await,
        Err(Error::CorruptMeta(_))
    ));
    assert_eq!(io.get("a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(io.get("missing").await.unwrap(), None);
}

#[tokio::test]
async fn injected_put_failure_after_n_puts() {
    let io = FaultIo::new();
    io.fail_after(Some(2));
    io.put_create("a", b"1").await.unwrap();
    io.put_create("b", b"2").await.unwrap();
    assert!(matches!(io.put_create("c", b"3").await, Err(Error::Io(_))));
    assert!(matches!(io.put_create("d", b"4").await, Err(Error::Io(_))));
    assert_eq!(io.puts(), 2);
    assert_eq!(io.keys(), vec!["a".to_string(), "b".to_string()]);

    io.fail_after(None);
    io.put_create("c", b"3").await.unwrap();
    assert_eq!(io.get("c").await.unwrap(), Some(b"3".to_vec()));
}

#[tokio::test]
async fn corrupted_or_missing_node_is_corrupt_meta() {
    let io = FaultIo::new();
    let hash_written = put_node(&io, &leaf(b"a", b"1")).await.unwrap();
    assert_eq!(
        get_node(&io, &hash_written).await.unwrap(),
        leaf(b"a", b"1")
    );

    io.force_put(&hash::node_key(&hash_written), b"not a node");
    assert!(matches!(
        get_node(&io, &hash_written).await,
        Err(Error::CorruptMeta(_))
    ));
    assert!(matches!(
        get_node(&io, &[7; 32]).await,
        Err(Error::CorruptMeta(_))
    ));
}

#[test]
fn root_cas_conflict_leaves_committed_generation() {
    let cas = FaultRootCas::new();
    assert!(cas.load().is_none());
    assert_eq!(cas.cas(0, b"v1").unwrap(), 1);
    assert!(matches!(cas.cas(0, b"stale"), Err(Error::Conflict(_))));
    let slot = cas.load().unwrap();
    assert_eq!(slot.version, 1);
    assert_eq!(slot.bytes, b"v1");
}

#[test]
fn partial_root_cas_is_indeterminate_but_may_have_committed() {
    let cas = FaultRootCas::new();
    cas.cas(0, b"v1").unwrap();
    cas.fail_next_cas_partially();
    assert!(matches!(cas.cas(1, b"v2"), Err(Error::Indeterminate(_))));

    // The write is visible: callers must re-read and compare, never assume rollback.
    let slot = cas.load().unwrap();
    assert_eq!(slot.version, 2);
    assert_eq!(slot.bytes, b"v2");
    // The old writer can no longer resurrect its base.
    assert!(matches!(cas.cas(1, b"v3"), Err(Error::Conflict(_))));
    assert_eq!(cas.load().unwrap().bytes, b"v2");
}

#[test]
fn failed_cas_without_partial_write_leaves_root() {
    let cas = FaultRootCas::new();
    cas.cas(0, b"v1").unwrap();
    assert!(matches!(cas.cas(9, b"nope"), Err(Error::Conflict(_))));
    assert_eq!(cas.load().unwrap().bytes, b"v1");
}
