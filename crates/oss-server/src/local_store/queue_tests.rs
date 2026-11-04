//! Tests for [`super::LocalQueue`]. Split out to keep `queue.rs` under 500 lines.

use std::sync::Arc;

use super::super::LocalStore;

fn queue(dir: &std::path::Path, max_attempts: u32) -> super::LocalQueue {
    LocalStore::open(dir)
        .unwrap()
        .queue_with_max_attempts("events", max_attempts)
        .unwrap()
}

#[test]
fn enqueue_claim_ack_happy_path() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    assert_eq!(q.append("a", b"1").unwrap(), 1);
    assert_eq!(q.append("b", b"2").unwrap(), 2);
    let seqs: Vec<_> = q.scan().iter().map(|e| e.seq).collect();
    assert_eq!(seqs, [1, 2]);

    let first = q.claim(100, 10).unwrap().unwrap();
    assert_eq!((first.seq, first.id.as_str(), first.attempts), (1, "a", 1));
    let second = q.claim(100, 10).unwrap().unwrap();
    assert_eq!(second.seq, 2);
    assert!(q.claim(100, 10).unwrap().is_none());
    assert_eq!(q.pending().len(), 2);

    assert!(q.ack(first.seq, &first.token).unwrap());
    assert!(!q.ack(first.seq, &first.token).unwrap(), "duplicate ack");
    assert!(!q.ack(second.seq, "wrong-token").unwrap());
    assert!(q.ack(second.seq, &second.token).unwrap());
    assert!(q.pending().is_empty());
}

#[test]
fn lease_expiry_redelivers() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    q.append("a", b"1").unwrap();
    let first = q.claim(100, 10).unwrap().unwrap();
    assert!(q.claim(109, 10).unwrap().is_none());
    let again = q.claim(110, 10).unwrap().unwrap();
    assert_eq!(again.seq, first.seq);
    assert_eq!(again.attempts, 2);
    assert_ne!(again.token, first.token);
    assert!(!q.ack(again.seq, &first.token).unwrap(), "stale token");
}

#[test]
fn claim_race_has_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let q = Arc::new(queue(dir.path(), 10));
    q.append("only", b"x").unwrap();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let q = q.clone();
            std::thread::spawn(move || q.claim(100, 10).unwrap())
        })
        .collect();
    let winners = threads
        .into_iter()
        .filter_map(|t| t.join().unwrap())
        .count();
    assert_eq!(winners, 1);
}

#[test]
fn compaction_preserves_live_entries_and_leases() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    q.append("acked", b"1").unwrap();
    q.append("claimed", b"2").unwrap();
    q.append("pending", b"3").unwrap();
    let acked = q.claim(100, 10).unwrap().unwrap();
    assert!(q.ack(acked.seq, &acked.token).unwrap());
    let claimed = q.claim(100, 100).unwrap().unwrap();
    q.compact().unwrap();

    drop(q);
    let q = queue(dir.path(), 10);
    let live: Vec<_> = q.scan().into_iter().map(|e| (e.seq, e.status)).collect();
    assert_eq!(
        live,
        [
            (2, super::QueueStatus::Claimed),
            (3, super::QueueStatus::Pending)
        ]
    );
    // Lease and attempt count survive compaction.
    let redelivered = q.claim(201, 10).unwrap().unwrap();
    assert_eq!((redelivered.seq, redelivered.attempts), (claimed.seq, 2));
}

#[test]
fn torn_tail_and_crc_garbage_are_ignored_and_repaired() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    q.append("a", b"1").unwrap();
    q.append("b", b"2").unwrap();
    drop(q);

    let log = dir.path().join("queue/events.log");
    let mut file = std::fs::OpenOptions::new().append(true).open(&log).unwrap();
    std::io::Write::write_all(&mut file, b"torn-tail").unwrap();
    drop(file);

    let q = queue(dir.path(), 10);
    assert_eq!(q.pending().len(), 2);
    assert_eq!(
        q.append("c", b"3").unwrap(),
        3,
        "seq continues after repair"
    );
    drop(q);
    let q = queue(dir.path(), 10);
    assert_eq!(q.pending().len(), 3);
}

#[test]
fn dead_letters_after_attempt_limit() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 2);
    q.append("poison", b"x").unwrap();
    assert!(q.claim(100, 10).unwrap().is_some());
    assert!(q.claim(111, 10).unwrap().is_some());
    assert!(
        q.claim(122, 10).unwrap().is_none(),
        "third claim dead-letters"
    );
    let all = q.scan();
    assert_eq!(all[0].status, super::QueueStatus::Dead);
    assert!(q.pending().is_empty());
    assert!(!q.ack(1, "any").unwrap());
    drop(q);
    let q = queue(dir.path(), 2);
    assert_eq!(q.scan()[0].status, super::QueueStatus::Dead);
}

#[test]
fn replay_after_reopen_and_manual_dead() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    q.append("a", b"1").unwrap();
    q.append("b", b"2").unwrap();
    let c = q.claim(100, 10).unwrap().unwrap();
    assert!(q.ack(c.seq, &c.token).unwrap());
    assert!(q.dead(2, "poison").unwrap());
    drop(q);
    let q = queue(dir.path(), 10);
    let all = q.scan();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].seq, 2);
    assert_eq!(all[0].status, super::QueueStatus::Dead);
    assert_eq!(q.append("c", b"3").unwrap(), 3);
}

#[test]
fn second_handle_with_different_config_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    let err = LocalStore::open(dir.path())
        .unwrap()
        .queue_with_max_attempts("events", 3)
        .err()
        .unwrap();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    drop(q);
}
