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
fn exact_claim_renew_retry_bury_and_revive() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    q.append("a", b"1").unwrap();

    let c = q.claim_exact(100, 30, "tok-100").unwrap().unwrap();
    assert_eq!((c.seq, c.attempts, c.lease_until), (1, 0, 130));
    assert!(q.claim_exact(100, 30, "tok-100").unwrap().is_none());

    assert!(!q.renew(c.seq, "stale", "tok-110", 110).unwrap());
    assert!(q.renew(c.seq, "tok-100", "tok-110", 110).unwrap());
    assert!(!q.retry(c.seq, "tok-100", 1, 200).unwrap());
    assert!(q.retry(c.seq, "tok-110", 1, 200).unwrap());
    assert_eq!(q.scan()[0].status, super::QueueStatus::Pending);
    assert!(q.claim_exact(199, 30, "early").unwrap().is_none());
    let c = q.claim_exact(200, 30, "tok-200").unwrap().unwrap();
    assert_eq!(c.attempts, 1);
    assert!(q.bury(c.seq, "tok-200", 2, "poison").unwrap());
    let dead = &q.scan()[0];
    assert_eq!((dead.status, dead.attempts), (super::QueueStatus::Dead, 2));
    assert_eq!(dead.reason.as_deref(), Some("poison"));

    assert!(q.revive(c.seq, 300).unwrap());
    let revived = q.claim_exact(300, 30, "tok-300").unwrap().unwrap();
    assert_eq!(revived.attempts, 0);
    assert!(q.ack(revived.seq, "tok-300").unwrap());
}

#[test]
fn reclaim_expired_preserves_attempts_and_remove_is_forced() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    q.append("a", b"1").unwrap();
    q.append("b", b"2").unwrap();
    q.claim_exact(100, 30, "tok").unwrap().unwrap();
    assert_eq!(q.reclaim_expired(129, 30).unwrap(), 0, "lease still fresh");
    assert_eq!(
        q.reclaim_expired(130, 30).unwrap(),
        0,
        "boundary is exclusive"
    );
    assert_eq!(q.reclaim_expired(131, 30).unwrap(), 1);
    let pending = &q.scan()[0];
    assert_eq!(
        (pending.status, pending.attempts),
        (super::QueueStatus::Pending, 0)
    );
    assert!(q.claim_exact(131, 30, "tok-2").unwrap().is_some());
    assert!(q.remove(2).unwrap(), "force delete ignores tokens");
    assert!(!q.remove(2).unwrap());
}

#[test]
fn compaction_preserves_retry_schedule_and_dead_attempts() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path(), 10);
    q.append("retry", b"1").unwrap();
    q.append("dead", b"2").unwrap();
    let c = q.claim_exact(100, 30, "tok").unwrap().unwrap();
    q.retry(c.seq, "tok", 4, 500).unwrap();
    let d = q.claim_exact(100, 30, "tok-d").unwrap().unwrap();
    q.bury(d.seq, "tok-d", 7, "bad").unwrap();
    q.compact().unwrap();
    drop(q);

    let q = queue(dir.path(), 10);
    let entries = q.scan();
    assert_eq!(entries[0].status, super::QueueStatus::Pending);
    assert_eq!(entries[0].retry_at, Some(500));
    assert!(q.claim_exact(499, 30, "x").unwrap().is_none());
    assert_eq!(q.claim_exact(500, 30, "x").unwrap().unwrap().attempts, 4);
    assert_eq!(entries[1].status, super::QueueStatus::Dead);
    assert_eq!(entries[1].attempts, 7);
    assert_eq!(entries[1].reason.as_deref(), Some("bad"));
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
