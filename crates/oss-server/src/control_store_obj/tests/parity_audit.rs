//! audit_log parity: AUTOINCREMENT-like ids, id-desc paging, the dynamic
//! filter matrix, and row-level retention deletes.

use std::fmt::Debug;

use xl_store::control_store::audit::{AuditQuery, AuditRecord, AuditStore, NewAuditRecord};

use super::support::{Boxed, Obj, Ops, both_ops, same_ok, sqlite_store};
use crate::db::Db;

async fn w<T, F>(db: &Db, obj: &Obj, f: F) -> T
where
    T: PartialEq + Debug,
    F: for<'a> FnOnce(&'a mut dyn Ops) -> Boxed<'a, T> + Copy,
{
    same_ok(both_ops(db, obj, f).await)
}

fn row(
    ts: i64,
    uid: i64,
    op: &str,
    bucket: Option<&str>,
    key: Option<&str>,
    decision: &str,
    detail: Option<&str>,
) -> NewAuditRecord {
    NewAuditRecord {
        ts,
        uid,
        op: op.into(),
        bucket: bucket.map(str::to_string),
        object_key: key.map(str::to_string),
        decision: decision.into(),
        detail: detail.map(str::to_string),
    }
}

async fn insert(db: &Db, obj: &Obj, rows: &[NewAuditRecord]) {
    w(db, obj, move |t| {
        let rows = rows.to_vec();
        Box::pin(async move { t.audit_insert_batch(&rows).await })
    })
    .await;
}

async fn page(db: &Db, obj: &Obj, query: &AuditQuery) -> Vec<AuditRecord> {
    let a = sqlite_store(db).audit_page(query).await.unwrap();
    let b = obj.control.audit_page(query).await.unwrap();
    assert_eq!(a, b, "audit page parity");
    a
}

fn query(limit: i64) -> AuditQuery {
    AuditQuery {
        limit,
        ..Default::default()
    }
}

async fn seed(db: &Db, obj: &Obj) {
    insert(
        db,
        obj,
        &[
            row(
                10,
                1,
                "GetObject",
                Some("b1"),
                Some("a/1"),
                "allow",
                Some("d1"),
            ),
            row(11, 2, "PutObject", Some("b1"), Some("a/2"), "deny", None),
            row(12, 1, "PutObject", None, None, "allow", Some("d3")),
            row(
                13,
                3,
                "DeleteObject",
                Some("b2"),
                Some("b/1"),
                "allow",
                None,
            ),
        ],
    )
    .await;
}

fn ids(rows: &[AuditRecord]) -> Vec<i64> {
    rows.iter().map(|r| r.id).collect()
}

#[tokio::test]
async fn batch_ids_are_descending_and_limited() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;
    seed(&db, &obj).await;

    let all = page(&db, &obj, &query(10)).await;
    assert_eq!(ids(&all), [4, 3, 2, 1]);
    assert_eq!(all[3].detail.as_deref(), Some("d1"));
    assert_eq!(all[1].bucket, None);

    assert_eq!(ids(&page(&db, &obj, &query(2)).await), [4, 3]);
    assert_eq!(page(&db, &obj, &query(-1)).await.len(), 4);
    assert!(page(&db, &obj, &query(0)).await.is_empty());
}

#[tokio::test]
async fn filters_match_sql_semantics() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;
    seed(&db, &obj).await;

    let mut q = query(10);
    q.decision = Some("deny".into());
    assert_eq!(ids(&page(&db, &obj, &q).await), [2]);

    let mut q = query(10);
    q.uid = Some(1);
    assert_eq!(ids(&page(&db, &obj, &q).await), [3, 1]);

    let mut q = query(10);
    q.op = Some("PutObject".into());
    assert_eq!(ids(&page(&db, &obj, &q).await), [3, 2]);

    let mut q = query(10);
    q.bucket = Some("b1".into());
    assert_eq!(ids(&page(&db, &obj, &q).await), [2, 1]);

    let mut q = query(10);
    q.from = Some(11);
    q.to = Some(12);
    assert_eq!(ids(&page(&db, &obj, &q).await), [3, 2]);

    let mut q = query(10);
    q.uid = Some(1);
    q.decision = Some("allow".into());
    assert_eq!(ids(&page(&db, &obj, &q).await), [3, 1]);
}

#[tokio::test]
async fn key_prefix_is_case_sensitive_and_skips_null_keys() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;
    seed(&db, &obj).await;

    let mut q = query(10);
    q.key_prefix = Some("a/".into());
    assert_eq!(
        ids(&page(&db, &obj, &q).await),
        [2, 1],
        "NULL key never matches"
    );

    let mut q = query(10);
    q.key_prefix = Some(String::new());
    assert_eq!(
        ids(&page(&db, &obj, &q).await),
        [4, 2, 1],
        "empty prefix matches non-NULL keys only"
    );

    let mut q = query(10);
    q.key_prefix = Some("A/".into());
    assert!(
        page(&db, &obj, &q).await.is_empty(),
        "prefix is case-sensitive"
    );
}

#[tokio::test]
async fn retention_deletes_oldest_rows_and_ids_keep_growing() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    insert(
        &db,
        &obj,
        &[
            row(10, 1, "op", None, None, "allow", None),
            row(20, 1, "op", None, None, "allow", None),
            row(30, 1, "op", None, None, "allow", None),
            row(40, 1, "op", None, None, "allow", None),
        ],
    )
    .await;

    let limit0 = w(&db, &obj, |t| {
        Box::pin(async move { t.audit_delete_older_than(35, 0).await })
    })
    .await;
    assert_eq!(limit0, 1, "limit is clamped to at least 1");
    assert_eq!(ids(&page(&db, &obj, &query(10)).await), [4, 3, 2]);

    let rest = w(&db, &obj, |t| {
        Box::pin(async move { t.audit_delete_older_than(35, 10).await })
    })
    .await;
    assert_eq!(rest, 2, "only ts < cutoff rows are candidates");
    assert_eq!(ids(&page(&db, &obj, &query(10)).await), [4]);

    insert(&db, &obj, &[row(50, 1, "op", None, None, "allow", None)]).await;
    let left = page(&db, &obj, &query(10)).await;
    assert_eq!(left[0].id, 5, "ids never restart after a purge");
    assert_eq!(left[1].id, 4);

    let none = w(&db, &obj, |t| {
        Box::pin(async move { t.audit_delete_older_than(0, 5).await })
    })
    .await;
    assert_eq!(none, 0);
}
