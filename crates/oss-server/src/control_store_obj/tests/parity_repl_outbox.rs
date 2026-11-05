//! repl_outbox parity: seq ordering, lease tokens, version aggregates, site
//! state counts and drop-site version aggregation.

use std::fmt::Debug;

use xl_store::control_store::repl_outbox::{NewReplOutbox, ReplOutboxRow, ReplOutboxStore};

use super::support::{Boxed, Obj, Ops, both_ops, same_ok, sqlite_store};
use crate::db::Db;

async fn w<T, F>(db: &Db, obj: &Obj, f: F) -> T
where
    T: PartialEq + Debug,
    F: for<'a> FnOnce(&'a mut dyn Ops) -> Boxed<'a, T> + Copy,
{
    same_ok(both_ops(db, obj, f).await)
}

fn entry(
    bucket: &str,
    key: &str,
    version: &str,
    remote: &str,
    kind: &str,
    created_at: i64,
) -> NewReplOutbox {
    NewReplOutbox {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version.into(),
        etag: format!("etag-{version}"),
        size: 7,
        remote_id: remote.into(),
        dest_bucket: "dest".into(),
        is_delete_marker: false,
        is_version_delete: false,
        target_kind: kind.into(),
        created_at,
    }
}

async fn enqueue(db: &Db, obj: &Obj, row: &NewReplOutbox) {
    w(db, obj, move |t| {
        let row = row.clone();
        Box::pin(async move { t.repl_enqueue(&row).await })
    })
    .await;
}

async fn claim(db: &Db, obj: &Obj, now: i64, limit: i64) -> Vec<ReplOutboxRow> {
    w(db, obj, move |t| {
        Box::pin(async move { t.repl_claim(now, limit).await })
    })
    .await
}

async fn counts(db: &Db, obj: &Obj, bucket: &str, key: &str, version: &str) -> (i64, i64, i64) {
    let a = sqlite_store(db)
        .repl_version_counts(bucket, key, version)
        .await
        .unwrap();
    let b = obj
        .control
        .repl_version_counts(bucket, key, version)
        .await
        .unwrap();
    assert_eq!(a, b);
    (a.pending, a.sending, a.dead)
}

#[tokio::test]
async fn claim_order_and_version_aggregates() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    enqueue(&db, &obj, &entry("b", "k1", "v1", "peerA", "remote", 10)).await;
    enqueue(&db, &obj, &entry("b", "k1", "v1", "peerA", "site", 11)).await;
    enqueue(&db, &obj, &entry("b", "k2", "v2", "peerB", "site", 12)).await;

    let claimed = claim(&db, &obj, 100, 10).await;
    assert_eq!(claimed.iter().map(|r| r.seq).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(claimed[0].target_kind, "remote");
    assert_eq!(claimed[0].attempts, 0);
    assert_eq!(claimed[0].claimed_at, Some(100));

    assert_eq!(counts(&db, &obj, "b", "k1", "v1").await, (0, 2, 0));

    let earlier = |before: i64| {
        let db = &db;
        let obj = &obj;
        async move {
            let a = sqlite_store(db)
                .repl_earlier_inflight("b", "k1", "v1", before)
                .await
                .unwrap();
            let b = obj
                .control
                .repl_earlier_inflight("b", "k1", "v1", before)
                .await
                .unwrap();
            assert_eq!(a, b);
            a
        }
    };
    assert_eq!(earlier(2).await, 1);
    assert_eq!(earlier(1).await, 0);
    assert_eq!(earlier(-1).await, 0);

    w(&db, &obj, |t| {
        Box::pin(async move { t.repl_bury(1, Some(100), 2, "boom").await })
    })
    .await;
    assert_eq!(counts(&db, &obj, "b", "k1", "v1").await, (0, 1, 1));

    let site_state = |peer: &'static str, state: &'static str| {
        let db = &db;
        let obj = &obj;
        async move {
            let a = sqlite_store(db)
                .repl_site_state_count(peer, state)
                .await
                .unwrap();
            let b = obj
                .control
                .repl_site_state_count(peer, state)
                .await
                .unwrap();
            assert_eq!(a, b);
            a
        }
    };
    assert_eq!(site_state("peerA", "sending").await, 1);
    assert_eq!(site_state("peerA", "pending").await, 0);
    assert_eq!(site_state("peerA", "dead").await, 0);
    assert_eq!(site_state("peerA", "bogus").await, 0);

    let total = |peer: &'static str| {
        let db = &db;
        let obj = &obj;
        async move {
            let a = sqlite_store(db).repl_site_total(peer).await.unwrap();
            let b = obj.control.repl_site_total(peer).await.unwrap();
            assert_eq!(a, b);
            a
        }
    };
    assert_eq!(
        total("peerA").await,
        1,
        "site rows only; the remote row is not a site row"
    );
    assert_eq!(total("peerB").await, 1);

    let finished = w(&db, &obj, |t| {
        Box::pin(async move { t.repl_finish_ok(2, Some(100)).await })
    })
    .await;
    assert!(finished);
    assert_eq!(counts(&db, &obj, "b", "k1", "v1").await, (0, 0, 1));
}

#[tokio::test]
async fn renew_retry_and_reclaim_use_the_token() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    enqueue(&db, &obj, &entry("b", "k", "v", "peer", "remote", 10)).await;
    let seq = claim(&db, &obj, 100, 1).await[0].seq;

    let stale = w(&db, &obj, move |t| {
        Box::pin(async move { t.repl_renew(seq, Some(99), 110).await })
    })
    .await;
    assert!(!stale);
    let renewed = w(&db, &obj, move |t| {
        Box::pin(async move { t.repl_renew(seq, Some(100), 110).await })
    })
    .await;
    assert!(renewed);

    let retried = w(&db, &obj, move |t| {
        Box::pin(async move { t.repl_retry(seq, Some(110), 1, 400, "busy").await })
    })
    .await;
    assert!(retried);
    assert!(claim(&db, &obj, 399, 10).await.is_empty());
    let again = claim(&db, &obj, 400, 1).await;
    assert_eq!((again[0].seq, again[0].attempts), (seq, 1));

    let stale_finish = w(&db, &obj, move |t| {
        Box::pin(async move { t.repl_finish_ok(seq, Some(110)).await })
    })
    .await;
    assert!(!stale_finish);

    assert_eq!(
        w(&db, &obj, move |t| {
            Box::pin(async move { t.repl_reclaim_expired(460, 60).await })
        })
        .await,
        0,
        "400 + 60 is not strictly older"
    );
    assert_eq!(
        w(&db, &obj, move |t| {
            Box::pin(async move { t.repl_reclaim_expired(461, 60).await })
        })
        .await,
        1
    );
    let reclaimed = claim(&db, &obj, 461, 1).await;
    assert_eq!(reclaimed[0].attempts, 1, "reclaim preserves attempts");
}

#[tokio::test]
async fn drop_site_returns_nonempty_versions_and_removes_all_site_rows() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    enqueue(&db, &obj, &entry("b", "k1", "v1", "peerA", "site", 10)).await;
    enqueue(&db, &obj, &entry("b", "k2", "", "peerA", "site", 11)).await;
    enqueue(&db, &obj, &entry("b", "k3", "v3", "peerB", "site", 12)).await;
    enqueue(&db, &obj, &entry("b", "k4", "v4", "peerA", "remote", 13)).await;

    let dropped = w(&db, &obj, |t| {
        Box::pin(async move {
            let mut rows = t.repl_drop_site("peerA").await?;
            rows.sort();
            Ok(rows)
        })
    })
    .await;
    assert_eq!(
        dropped,
        [("b".to_string(), "k1".to_string(), "v1".to_string())]
    );

    let total = |peer: &'static str| {
        let db = &db;
        let obj = &obj;
        async move {
            let a = sqlite_store(db).repl_site_total(peer).await.unwrap();
            let b = obj.control.repl_site_total(peer).await.unwrap();
            assert_eq!(a, b);
            a
        }
    };
    assert_eq!(total("peerA").await, 0, "empty and non-empty site rows go");
    assert_eq!(total("peerB").await, 1, "other peers and remote rows stay");

    let deleted = w(&db, &obj, |t| {
        Box::pin(async move { t.repl_delete_site_all("peerB").await })
    })
    .await;
    assert_eq!(deleted, 1);
    assert_eq!(total("peerB").await, 0);
}
