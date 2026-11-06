//! G3: object-backend `Db` façade paths used by console + workers.

use std::sync::Arc;

use xl_store::control_state::root::Mode;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketCreateOutcome, BucketEntry};
use xl_store::control_store::jobs::{NewJob, NewTask};
use xl_store::store::{Store, StoreConfig};

use crate::control_backend::ObjectControl;
use crate::db::Db;
use crate::raft::FencingEpoch;

struct Fixture {
    _dir: tempfile::TempDir,
    db: Arc<Db>,
}

async fn object_db() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let object = ObjectControl::open(store, Mode::Standalone, None)
        .await
        .unwrap();
    let db = Arc::new(Db::connect_object().await.unwrap());
    db.attach_object_control(object).unwrap();
    Fixture { _dir: dir, db }
}

fn bucket_entry(name: &str, owner: OwnerRef, mode: &str) -> BucketEntry {
    BucketEntry {
        name: name.into(),
        owner,
        creation_date: crate::db::now(),
        versioning: if mode == "sync" { "Enabled" } else { "Off" }.into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: mode.into(),
        deleting: false,
    }
}

#[tokio::test]
async fn object_db_create_user_derives_account_and_rejects_duplicate() {
    let f = object_db().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    let user = f.db.control_store().user("alice").await.unwrap().unwrap();
    assert_eq!(user.uid, uid);
    assert!(!user.is_admin);
    assert_eq!(
        f.db.control_store()
            .account_member_account(uid)
            .await
            .unwrap(),
        xl_s3::access::account_id_of(uid),
        "non-admin user must be owner of its derived account"
    );

    let err =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap_err();
    assert!(crate::db::is_unique_violation(&err));

    // Admin: no derived account/member (system account semantics).
    let admin = f.db.create_user("root", "root-pw-123", true).await.unwrap();
    assert!(
        f.db.control_store()
            .account_member_account(admin)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn object_db_bucket_create_owner_mode_and_quota_seam() {
    let f = object_db().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    let entry = bucket_entry("b1", OwnerRef::User("alice".into()), "async");
    let out =
        f.db.control_write("console", move |txn| {
            let entry = entry.clone();
            Box::pin(async move {
                // Same order as the console object branch: quota only for new rows.
                if !txn.bucket_exists(&entry.name).await?
                    && txn.bucket_quota_reached(Some(uid)).await?
                {
                    return Ok((true, BucketCreateOutcome::Created));
                }
                Ok((false, txn.bucket_create(&entry).await?))
            })
        })
        .await
        .unwrap();
    assert_eq!(out, (false, BucketCreateOutcome::Created));

    let view = f.db.control_store().bucket("b1").await.unwrap().unwrap();
    assert_eq!(view.owner, Some(OwnerRef::User("alice".into())));
    assert_eq!(view.replication_mode, "async");
    assert_eq!(f.db.owned_buckets(uid).await.unwrap(), vec!["b1"]);

    // Recreate converges (Updated), mode change is a conflict.
    let again =
        f.db.control_write("console", |txn| {
            let same = bucket_entry("b1", OwnerRef::User("alice".into()), "async");
            Box::pin(async move { txn.bucket_create(&same).await })
        })
        .await
        .unwrap();
    assert_eq!(again, BucketCreateOutcome::Updated);
    let conflict =
        f.db.control_write("console", |txn| {
            let changed = bucket_entry("b1", OwnerRef::User("alice".into()), "sync");
            Box::pin(async move { txn.bucket_create(&changed).await })
        })
        .await
        .unwrap();
    assert_eq!(conflict, BucketCreateOutcome::ModeConflict);

    // Quota only gates new rows: with quota=0, recreate converges but a new name is rejected.
    let account = xl_s3::access::account_id_of(uid).unwrap();
    f.db.control_write("test", move |txn| {
        let account = account.clone();
        Box::pin(async move {
            txn.account_set_bucket_quota(&account, Some(0), crate::db::now())
                .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    let recreate =
        f.db.control_write("console", move |txn| {
            let entry = bucket_entry("b1", OwnerRef::User("alice".into()), "async");
            Box::pin(async move {
                if !txn.bucket_exists(&entry.name).await?
                    && txn.bucket_quota_reached(Some(uid)).await?
                {
                    return Ok((true, BucketCreateOutcome::Created));
                }
                Ok((false, txn.bucket_create(&entry).await?))
            })
        })
        .await
        .unwrap();
    assert_eq!(recreate, (false, BucketCreateOutcome::Updated));
    let rejected =
        f.db.control_write("console", move |txn| {
            let entry = bucket_entry("b2", OwnerRef::User("alice".into()), "async");
            Box::pin(async move {
                if !txn.bucket_exists(&entry.name).await?
                    && txn.bucket_quota_reached(Some(uid)).await?
                {
                    return Ok((true, BucketCreateOutcome::Created));
                }
                Ok((false, txn.bucket_create(&entry).await?))
            })
        })
        .await
        .unwrap();
    assert_eq!(rejected, (true, BucketCreateOutcome::Created));
}

#[tokio::test]
async fn object_db_claim_is_exclusive_and_fence_guarded() {
    let f = object_db().await;
    let now = crate::db::now();
    let job_id =
        f.db.control_write("test", |txn| {
            Box::pin(async move {
                txn.job_create(
                    &NewJob {
                        kind: "lifecycle".into(),
                        bucket: "b".into(),
                        prefix: "p/".into(),
                        state: "queued".into(),
                        total: 1,
                        priority: 5,
                        created_at: now,
                        updated_at: now,
                        ..NewJob::default()
                    },
                    &[NewTask {
                        action: "delete".into(),
                        payload_json: "{}".into(),
                        dedupe_key: None,
                        next_at: now,
                    }],
                )
                .await
            })
        })
        .await
        .unwrap();

    let fence = FencingEpoch { term: 2, leader: 1 };
    let (a, b) = tokio::join!(
        f.db.claim_task_fenced(now, 30, fence),
        f.db.claim_task_fenced(now, 30, fence)
    );
    let claims: Vec<_> = [a.unwrap(), b.unwrap()].into_iter().flatten().collect();
    assert_eq!(claims.len(), 1, "exactly one worker must claim the task");
    let claim = &claims[0];
    assert_eq!(claim.job_id, job_id);

    let task =
        f.db.control_store()
            .job_tasks(job_id)
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.task_no == claim.task_no)
            .unwrap();
    assert_eq!(task.state, "running");
    assert_eq!(task.fence_term, 2);
    assert_eq!(task.fence_leader, 1);
    assert!(
        task.deadline_at.is_some_and(|d| d > now),
        "lease deadline set"
    );

    // Stale term cannot finish; current term can.
    let stale =
        f.db.finish_task_fenced(
            job_id,
            claim.task_no,
            true,
            "OK",
            None,
            now,
            3,
            FencingEpoch { term: 1, leader: 1 },
        )
        .await
        .unwrap();
    assert_eq!(stale, "fenced");
    let done =
        f.db.finish_task_fenced(job_id, claim.task_no, true, "OK", None, now, 3, fence)
            .await
            .unwrap();
    assert_eq!(done, "done");
}
