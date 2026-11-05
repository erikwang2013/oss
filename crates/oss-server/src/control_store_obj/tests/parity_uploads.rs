//! uploads parity: saved_path conflicts, owner registration/claim guards and
//! `created_at DESC, saved_path ASC` ordering.

use std::fmt::Debug;

use xl_store::control_store::uploads::{NewUpload, UploadRow, UploadStore};

use super::support::{Boxed, Obj, Ops, both_ops, same_ok, sqlite_store};
use crate::db::Db;

async fn w<T, F>(db: &Db, obj: &Obj, f: F) -> T
where
    T: PartialEq + Debug,
    F: for<'a> FnOnce(&'a mut dyn Ops) -> Boxed<'a, T> + Copy,
{
    same_ok(both_ops(db, obj, f).await)
}

fn new(saved_path: &str, bucket: &str, size: i64, created_at: i64) -> NewUpload {
    NewUpload {
        saved_path: saved_path.into(),
        bucket: bucket.into(),
        key: format!("key/{saved_path}"),
        group_name: "group".into(),
        size,
        etag: format!("etag-{saved_path}"),
        created_at,
    }
}

async fn record(db: &Db, obj: &Obj, row: &NewUpload) {
    w(db, obj, move |t| {
        let row = row.clone();
        Box::pin(async move { t.upload_record(&row).await })
    })
    .await;
}

async fn get(db: &Db, obj: &Obj, saved_path: &str) -> Option<UploadRow> {
    let a = sqlite_store(db).upload(saved_path).await.unwrap();
    let b = obj.control.upload(saved_path).await.unwrap();
    assert_eq!(a, b);
    a
}

async fn list(db: &Db, obj: &Obj, limit: i64) -> Vec<UploadRow> {
    let a = sqlite_store(db).uploads(limit).await.unwrap();
    let b = obj.control.uploads(limit).await.unwrap();
    assert_eq!(a, b);
    a
}

async fn list_for(db: &Db, obj: &Obj, owner: i64, limit: i64) -> Vec<UploadRow> {
    let a = sqlite_store(db).uploads_for(owner, limit).await.unwrap();
    let b = obj.control.uploads_for(owner, limit).await.unwrap();
    assert_eq!(a, b);
    a
}

#[tokio::test]
async fn conflict_keeps_first_row_and_order_is_stable() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;

    record(&db, &obj, &new("b", "bkt", 1, 100)).await;
    record(&db, &obj, &new("a", "bkt", 2, 100)).await;
    record(&db, &obj, &new("c", "bkt", 3, 200)).await;

    let paths: Vec<_> = list(&db, &obj, 10)
        .await
        .into_iter()
        .map(|r| r.saved_path)
        .collect();
    assert_eq!(paths, ["c", "a", "b"], "created_at DESC then path ASC");

    record(&db, &obj, &new("a", "other", 99, 999)).await;
    let kept = get(&db, &obj, "a").await.unwrap();
    assert_eq!(
        (kept.bucket.as_str(), kept.size, kept.created_at),
        ("bkt", 2, 100),
        "ON CONFLICT(saved_path) DO NOTHING"
    );

    assert_eq!(list(&db, &obj, 2).await.len(), 2);
    assert_eq!(
        list(&db, &obj, -1).await.len(),
        3,
        "negative LIMIT unbounded"
    );
}

#[tokio::test]
async fn register_and_claim_owner_guards() {
    let db = super::support::sqlite(false).await;
    let obj = super::support::object_store(xl_store::control_state::root::Mode::Standalone).await;
    let u5 = db.create_user("owner5", "pw", false).await.unwrap();
    let u6 = db.create_user("owner6", "pw", false).await.unwrap();
    let u7 = db.create_user("owner7", "pw", false).await.unwrap();

    record(&db, &obj, &new("p", "bkt", 1, 100)).await;
    record(&db, &obj, &new("q", "bkt", 2, 200)).await;
    record(&db, &obj, &new("r", "bkt", 3, 300)).await;

    let register_missing = w(&db, &obj, |t| {
        Box::pin(async move { t.upload_register("nope", "x", u5, false).await })
    })
    .await;
    assert!(!register_missing);

    let claimed = w(&db, &obj, |t| {
        Box::pin(async move { t.upload_claim("p", u5).await })
    })
    .await;
    assert!(claimed);
    let claimed_again = w(&db, &obj, |t| {
        Box::pin(async move { t.upload_claim("p", u6).await })
    })
    .await;
    assert!(!claimed_again, "claim only fills a null owner");

    let admin = w(&db, &obj, |t| {
        Box::pin(async move { t.upload_register("q", "admin.bin", u7, true).await })
    })
    .await;
    assert!(admin);

    let user = w(&db, &obj, |t| {
        Box::pin(async move { t.upload_register("r", "user.bin", u5, false).await })
    })
    .await;
    assert!(user);
    let same_owner = w(&db, &obj, |t| {
        Box::pin(async move { t.upload_register("r", "user2.bin", u5, false).await })
    })
    .await;
    assert!(same_owner);
    let other_owner = w(&db, &obj, |t| {
        Box::pin(async move { t.upload_register("r", "stolen.bin", u6, false).await })
    })
    .await;
    assert!(!other_owner, "other users cannot re-own the row");
    assert_eq!(
        get(&db, &obj, "r").await.unwrap().original_name.as_deref(),
        Some("user2.bin")
    );

    let owner_7: Vec<_> = list_for(&db, &obj, u7, 10)
        .await
        .into_iter()
        .map(|r| r.saved_path)
        .collect();
    assert_eq!(owner_7, ["q"]);
    let owner_5: Vec<_> = list_for(&db, &obj, u5, 10)
        .await
        .into_iter()
        .map(|r| r.saved_path)
        .collect();
    assert_eq!(owner_5, ["r", "p"], "created_at DESC");
    assert!(list_for(&db, &obj, u6, 10).await.is_empty());
    assert_eq!(list_for(&db, &obj, u5, -1).await.len(), 2);
}
