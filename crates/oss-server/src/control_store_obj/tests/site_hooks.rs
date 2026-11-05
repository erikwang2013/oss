//! D2 golden matrix: bucket / bucket_policy hook cells vs the SQLite triggers.

use crate::control_store_obj::site_apply::{BucketSiteRow, PolicySiteRow};

use super::site_common::{
    Real, assert_rows, block, nasty, now, obj_rows, object_site, run_hook, sql_rows, sqlite_site,
};

#[tokio::test]
async fn bucket_trigger_cells_match() {
    let (db, id) = sqlite_site().await;
    let real = object_site(&id).await;
    let name = nasty();
    let owner = "own\"er\\x\u{1}";
    sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
         VALUES (?, 'h', 0, 'active', 1)",
    )
    .bind(owner)
    .execute(db.pool())
    .await
    .unwrap();

    // INSERT (payload includes the resolved owner username).
    sqlx::query(
        "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock) \
         VALUES (?, 1, 11, 'Off', 0)",
    )
    .bind(&name)
    .execute(db.pool())
    .await
    .unwrap();
    let row = BucketSiteRow {
        name: name.clone(),
        owner_uid: Some(1),
        owner_username: Some(owner.into()),
        creation_date: 11,
        versioning: "Off".into(),
        object_lock: false,
    };
    run_hook(&real, |txn| {
        let row = row.clone();
        Box::pin(async move { txn.site_hook_bucket(None, Some(&row), 1_000).await })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    // UPDATE only when a covered column actually changes.
    sqlx::query("UPDATE buckets SET versioning = 'Enabled' WHERE name = ?")
        .bind(&name)
        .execute(db.pool())
        .await
        .unwrap();
    let mut changed = row.clone();
    changed.versioning = "Enabled".into();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_bucket(Some(&row), Some(&changed), 1_001)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);
    sqlx::query("UPDATE buckets SET set_id = 9 WHERE name = ?")
        .bind(&name)
        .execute(db.pool())
        .await
        .unwrap();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_bucket(Some(&changed), Some(&changed), 1_002)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    // Gate cells: INSERT / UPDATE / DELETE each reject before any mutation.
    let before = sql_rows(&db).await.len();
    block(&db, &real, "bucket", "gated-bucket").await;
    let err = sqlx::query(
        "INSERT INTO buckets (name, creation_date, versioning, object_lock) \
         VALUES ('gated-bucket', 1, 'Off', 0)",
    )
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let gated = BucketSiteRow {
        name: "gated-bucket".into(),
        ..BucketSiteRow::default()
    };
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_bucket(None, Some(&gated), now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );
    assert_eq!(sql_rows(&db).await.len(), before);
    assert_eq!(obj_rows(&real).await.len(), before);

    block(&db, &real, "bucket", &name).await;
    let err = sqlx::query("UPDATE buckets SET versioning = 'Blocked' WHERE name = ?")
        .bind(&name)
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let mut blocked = changed.clone();
    blocked.versioning = "Blocked".into();
    let obj_err = run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_bucket(Some(&changed), Some(&blocked), now())
                .await
        })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );
    assert_eq!(sql_rows(&db).await.len(), before);
    assert_eq!(obj_rows(&real).await.len(), before);

    let err = sqlx::query("DELETE FROM buckets WHERE name = ?")
        .bind(&name)
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_bucket(Some(&changed), None, now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );
    assert_eq!(sql_rows(&db).await.len(), before);
    assert_eq!(obj_rows(&real).await.len(), before);
}

#[tokio::test]
async fn policy_trigger_cells_match() {
    let (db, id) = sqlite_site().await;
    let real: Real = object_site(&id).await;
    let bucket = "pol\"b\\u\u{1}";
    let policy = "{\"v\":\"a\\\"b\",\"ctl\":\"\u{1}\"}";

    sqlx::query("INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, 7)")
        .bind(bucket)
        .bind(policy)
        .execute(db.pool())
        .await
        .unwrap();
    let row = PolicySiteRow {
        bucket: bucket.into(),
        policy_json: policy.into(),
        updated_at: 7,
    };
    run_hook(&real, |txn| {
        let row = row.clone();
        Box::pin(async move { txn.site_hook_policy(None, Some(&row), 1_000).await })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    sqlx::query("UPDATE bucket_policies SET policy_json = '{\"v\":2}' WHERE bucket = ?")
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
    let mut changed = row.clone();
    changed.policy_json = "{\"v\":2}".into();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_policy(Some(&row), Some(&changed), 1_001)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    sqlx::query("UPDATE bucket_policies SET policy_json = policy_json WHERE bucket = ?")
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
    run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_policy(Some(&changed), Some(&changed), 1_002)
                .await
        })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);

    let before = sql_rows(&db).await.len();
    block(&db, &real, "bucket_policy", "gated-policy").await;
    let err = sqlx::query(
        "INSERT INTO bucket_policies (bucket, policy_json, updated_at) \
         VALUES ('gated-policy', '{}', 1)",
    )
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let gated = PolicySiteRow {
        bucket: "gated-policy".into(),
        policy_json: "{}".into(),
        updated_at: 1,
    };
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_policy(None, Some(&gated), now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );

    block(&db, &real, "bucket_policy", bucket).await;
    let err = sqlx::query("UPDATE bucket_policies SET updated_at = 8 WHERE bucket = ?")
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let obj_err = run_hook(&real, |txn| {
        let changed = changed.clone();
        Box::pin(async move {
            txn.site_hook_policy(Some(&changed), Some(&changed), now())
                .await
        })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );

    let err = sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("site replication conflict"),
        "{err}"
    );
    let obj_err = run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_policy(Some(&changed), None, now()).await })
    })
    .await
    .unwrap_err();
    assert!(
        obj_err.to_string().contains("site replication conflict"),
        "{obj_err}"
    );
    assert_eq!(sql_rows(&db).await.len(), before);
    assert_eq!(obj_rows(&real).await.len(), before);

    sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
    run_hook(&real, |txn| {
        Box::pin(async move { txn.site_hook_policy(Some(&changed), None, 2_000).await })
    })
    .await
    .unwrap();
    assert_rows(&sql_rows(&db).await, &obj_rows(&real).await);
}
