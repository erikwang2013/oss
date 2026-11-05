//! event_outbox parity: same enqueue/claim/token/retry/dead scenario through
//! the SQLite adapter and the object backend over its local queue.

use std::fmt::Debug;

use xl_store::control_store::event_outbox::{DeadEventRow, EventOutboxClaim, EventOutboxStore};

use super::support::{Boxed, Obj, Ops, both_ops, same_ok, sqlite_store};
use crate::db::Db;

async fn w<T, F>(db: &Db, obj: &Obj, f: F) -> T
where
    T: PartialEq + Debug,
    F: for<'a> FnOnce(&'a mut dyn Ops) -> Boxed<'a, T> + Copy,
{
    same_ok(both_ops(db, obj, f).await)
}

async fn enqueue(db: &Db, obj: &Obj, target: &str, event: &str, now: i64) {
    w(db, obj, move |t| {
        let (target, event) = (target.to_string(), event.to_string());
        Box::pin(async move { t.event_enqueue("bucket", &target, &event, now).await })
    })
    .await;
}

async fn claim(db: &Db, obj: &Obj, now: i64, limit: i64) -> Vec<EventOutboxClaim> {
    w(db, obj, move |t| {
        Box::pin(async move { t.event_claim(now, limit).await })
    })
    .await
}

async fn dead(db: &Db, obj: &Obj, limit: i64) -> Vec<DeadEventRow> {
    let a = sqlite_store(db).dead_events(limit).await.unwrap();
    let b = obj.control.dead_events(limit).await.unwrap();
    assert_eq!(a, b, "dead events parity");
    a
}

#[tokio::test]
async fn enqueue_claim_order_and_attempts() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    enqueue(&db, &obj, "t1", r#"{"a":1}"#, 10).await;
    enqueue(&db, &obj, "t2", r#"{"a":2}"#, 10).await;
    enqueue(&db, &obj, "t3", r#"{"a":3}"#, 10).await; // third one is claimed next

    let first = claim(&db, &obj, 100, 2).await;
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].id, 1);
    assert_eq!(first[1].id, 2);
    assert!(first.iter().all(|r| r.attempts == 0));
    assert!(first.iter().all(|r| r.claimed_at == Some(100)));

    let second = claim(&db, &obj, 100, 2).await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, 3, "claims stay in id order");
    assert!(claim(&db, &obj, 100, 2).await.is_empty());
}

#[tokio::test]
async fn tokens_guard_renew_finish_retry_and_bury() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    enqueue(&db, &obj, "t1", "e1", 10).await;
    let claimed = claim(&db, &obj, 100, 1).await;
    let id = claimed[0].id;

    let stale = w(&db, &obj, move |t| {
        Box::pin(async move { t.event_renew(id, Some(99), 110).await })
    })
    .await;
    assert!(!stale);
    let renewed = w(&db, &obj, move |t| {
        Box::pin(async move { t.event_renew(id, Some(100), 110).await })
    })
    .await;
    assert!(renewed);

    let old_token = w(&db, &obj, move |t| {
        Box::pin(async move { t.event_finish_ok(id, Some(100)).await })
    })
    .await;
    assert!(!old_token, "renew rotated the token");
    let finished = w(&db, &obj, move |t| {
        Box::pin(async move { t.event_finish_ok(id, Some(110)).await })
    })
    .await;
    assert!(finished);
    assert!(claim(&db, &obj, 110, 10).await.is_empty());
}

#[tokio::test]
async fn retry_schedule_bury_and_redeliver() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    enqueue(&db, &obj, "t1", "e1", 10).await;
    let id = claim(&db, &obj, 100, 1).await[0].id;
    let retried = w(&db, &obj, move |t| {
        Box::pin(async move { t.event_retry(id, Some(100), 1, 300, "transient").await })
    })
    .await;
    assert!(retried);

    assert!(
        claim(&db, &obj, 299, 10).await.is_empty(),
        "next_retry_at gates the claim"
    );
    let again = claim(&db, &obj, 300, 1).await;
    assert_eq!(again[0].id, id);
    assert_eq!(again[0].attempts, 1, "claim does not bump attempts");

    let buried = w(&db, &obj, move |t| {
        Box::pin(async move { t.event_bury(id, Some(300), 2, "permanent").await })
    })
    .await;
    assert!(buried);

    let rows = dead(&db, &obj, 10).await;
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].id, rows[0].attempts), (id, 2));
    assert_eq!(rows[0].last_error.as_deref(), Some("permanent"));
    assert_eq!(rows[0].event_json, "e1");
    assert_eq!(rows[0].created_at, 10);

    let redelivered = w(&db, &obj, move |t| {
        Box::pin(async move { t.event_redeliver(id, 400).await })
    })
    .await;
    assert!(redelivered);
    assert!(dead(&db, &obj, 10).await.is_empty());
    let revived = claim(&db, &obj, 400, 1).await;
    assert_eq!(revived[0].attempts, 0, "redeliver resets attempts");
}

#[tokio::test]
async fn reclaim_expiry_is_strict_and_preserves_attempts() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    enqueue(&db, &obj, "t1", "e1", 10).await;
    let id = claim(&db, &obj, 100, 1).await[0].id;
    let reclaimed = |now: i64| {
        let db = &db;
        let obj = &obj;
        async move {
            w(db, obj, move |t| {
                Box::pin(async move { t.event_reclaim_expired(now, 60).await })
            })
            .await
        }
    };
    assert_eq!(reclaimed(159).await, 0);
    assert_eq!(reclaimed(160).await, 0, "claimed_at < now-lease is strict");
    assert_eq!(reclaimed(161).await, 1);

    let reclaimed_row = claim(&db, &obj, 161, 1).await;
    assert_eq!(reclaimed_row[0].id, id);
    assert_eq!(reclaimed_row[0].attempts, 0);
    let old = w(&db, &obj, move |t| {
        Box::pin(async move { t.event_finish_ok(id, Some(100)).await })
    })
    .await;
    assert!(!old, "stale token after reclaim");
}

#[tokio::test]
async fn dead_list_is_id_desc_and_limited() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    for target in ["t1", "t2", "t3"] {
        enqueue(&db, &obj, target, target, 10).await;
    }
    for id in 1..=3 {
        let claimed = claim(&db, &obj, 100 + id, 1).await[0].id;
        assert_eq!(claimed, id);
        let attempts = id;
        w(&db, &obj, move |t| {
            Box::pin(async move { t.event_bury(id, Some(100 + id), attempts, "e").await })
        })
        .await;
    }
    let rows = dead(&db, &obj, 2).await;
    assert_eq!(rows.iter().map(|r| r.id).collect::<Vec<_>>(), [3, 2]);
    let all = dead(&db, &obj, -1).await;
    assert_eq!(all.iter().map(|r| r.id).collect::<Vec<_>>(), [3, 2, 1]);
}
