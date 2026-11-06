use xl_store::control_state::root::Mode;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketCreateOutcome, BucketEntry};

use crate::control_backend::ObjectControl;

use super::*;

struct Fixture {
    _dir: tempfile::TempDir,
    db: Arc<Db>,
    store: Arc<Store>,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = xl_store::store::StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let object = ObjectControl::open(Arc::clone(&store), Mode::Standalone, None)
        .await
        .unwrap();
    let db = Arc::new(Db::connect_object().await.unwrap());
    db.attach_object_control(object).unwrap();
    Fixture {
        _dir: dir,
        db,
        store,
    }
}

async fn create_bucket(db: &Db, name: &str, owner: &str) {
    let entry = BucketEntry {
        name: name.into(),
        owner: OwnerRef::User(owner.into()),
        creation_date: crate::db::now(),
        versioning: "Off".into(),
        object_lock: false,
        default_encryption: None,
        object_lock_default_mode: None,
        object_lock_default_days: None,
        replication_mode: "async".into(),
        deleting: false,
    };
    let out = db
        .control_write("test", move |txn| {
            let entry = entry.clone();
            Box::pin(async move { txn.bucket_create(&entry).await })
        })
        .await
        .unwrap();
    assert_eq!(out, BucketCreateOutcome::Created);
}

async fn set_limits(db: &Db, account: &str, bytes: Option<i64>, objects: Option<i64>) {
    let account = account.to_owned();
    let changed = db
        .control_write("test", move |txn| {
            let account = account.clone();
            Box::pin(async move {
                txn.account_set_hard_limits(&account, bytes, objects, crate::db::now())
                    .await
            })
        })
        .await
        .unwrap();
    assert!(changed);
}

async fn ledger(db: &Db, account: &str) -> Option<(i64, i64)> {
    let account = account.to_owned();
    db.control_write("test", move |txn| {
        let account = account.clone();
        Box::pin(async move { txn.account_ledger(&account).await })
    })
    .await
    .unwrap()
}

/// 读路径：限制来自对象根，桶数/用量来自账户投影与账本。
#[tokio::test]
async fn reads_limits_and_bucket_count_from_object_root() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "b1", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(10), Some(2)).await;

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    let got = quota.account_quota_for_bucket("b1").await.unwrap().unwrap();
    assert_eq!(got.account, account);
    assert!(!got.disabled);
    assert_eq!(got.limits.hard_bytes, Some(10));
    assert_eq!(got.limits.objects, Some(2));

    // 桶数投影（软配额端点的同一读取口径）；桶名键必须去掉编码终止符。
    let view =
        f.db.control_store()
            .account(&account)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(view.bucket_count, 1);
    assert_eq!(view.status, "active");
    assert_eq!(
        f.db.control_store()
            .account_bucket_names(&account)
            .await
            .unwrap(),
        vec!["b1".to_string()]
    );
}

/// 预留/校准/释放：恰好等于上限放行，超过 1 即拒绝；负数修正饱和到 0。
#[tokio::test]
async fn admit_adjust_release_follow_ledger_limits() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "b1", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(10), Some(2)).await;

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    let q = quota.account_quota_for_bucket("b1").await.unwrap().unwrap();
    assert!(matches!(
        quota.admit(&f.store, &q, 9, 1).await.unwrap(),
        Ok(())
    ));
    assert!(matches!(
        quota.admit(&f.store, &q, 1, 0).await.unwrap(),
        Ok(())
    ));
    assert!(matches!(
        quota.admit(&f.store, &q, 1, 0).await.unwrap(),
        Err(QuotaDenied::Bytes)
    ));
    assert!(matches!(
        quota.admit(&f.store, &q, 0, 1).await.unwrap(),
        Ok(())
    ));
    assert!(matches!(
        quota.admit(&f.store, &q, 0, 1).await.unwrap(),
        Err(QuotaDenied::Objects)
    ));
    assert_eq!(ledger(&f.db, &account).await, Some((10, 2)));

    quota.adjust(&account, -10, -2).await.unwrap();
    assert_eq!(ledger(&f.db, &account).await, Some((0, 0)));
    assert!(matches!(
        quota.admit(&f.store, &q, 0, 2).await.unwrap(),
        Ok(())
    ));
    // release 是 adjust 的负数形式（饱和到 0）。
    quota.release(&account, 5, 5).await.unwrap();
    assert_eq!(ledger(&f.db, &account).await, Some((0, 0)));
}

/// 并发抢边界：根 CAS 串行化，恰好 limit 个成功。
#[tokio::test]
async fn concurrent_admits_stop_at_the_boundary() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "b1", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, None, Some(3)).await;

    let quota = Arc::new(ObjectQuota::new(Arc::clone(&f.db)));
    let q = quota.account_quota_for_bucket("b1").await.unwrap().unwrap();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let quota = Arc::clone(&quota);
        let q = q.clone();
        let store = Arc::clone(&f.store);
        tasks.push(tokio::spawn(async move {
            quota.admit(&store, &q, 0, 1).await.unwrap().is_ok()
        }));
    }
    let mut ok = 0;
    for t in tasks {
        if t.await.unwrap() {
            ok += 1;
        }
    }
    assert_eq!(ok, 3, "恰好 3 个并发预留成功");
    assert_eq!(ledger(&f.db, &account).await, Some((0, 3)));
}

/// 回归：对象模式必须读复制账本。占位内存库没有任何表；若实现回落到
/// `db.pool()`，这里会得到 Backend 错误而不是稳定配额拒绝。
#[tokio::test]
async fn admit_denies_from_durable_ledger_without_sqlite_tables() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "b1", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(10), None).await;
    let account_owned = account.clone();
    f.db.control_write("test", move |txn| {
        let account = account_owned.clone();
        Box::pin(async move { txn.account_ledger_upsert(&account, 10, 1).await })
    })
    .await
    .unwrap();

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    let q = quota.account_quota_for_bucket("b1").await.unwrap().unwrap();
    match quota.admit(&f.store, &q, 1, 0).await {
        Ok(Err(QuotaDenied::Bytes)) => {}
        other => panic!("必须由账本稳定拒绝，得到 {other:?}"),
    }
    assert_eq!(ledger(&f.db, &account).await, Some((10, 1)));
}

/// 采样：完整列举按当前版本对象校准账本；下界不覆盖已知更大值。
#[tokio::test]
async fn refresh_all_recomputes_ledger_from_objects() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "bkt", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(100), None).await;

    f.store.create_bucket("bkt").await.unwrap();
    for (key, body) in [("a", &b"abc"[..]), ("b", &b"12345"[..])] {
        f.store
            .put_object("bkt", key, bytes::Bytes::copy_from_slice(body), None)
            .await
            .unwrap();
    }
    // 账本先记一个偏大的值；完整采样必须校准回真实用量。
    let account_owned = account.clone();
    f.db.control_write("test", move |txn| {
        let account = account_owned.clone();
        Box::pin(async move { txn.account_ledger_upsert(&account, 999, 9).await })
    })
    .await
    .unwrap();

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    quota.refresh_all(&f.store).await.unwrap();
    assert_eq!(ledger(&f.db, &account).await, Some((8, 2)));

    // 控制面有行但物理桶不可列举 = 下界：不得覆盖已知更大值。
    create_bucket(&f.db, "ghost", "alice").await;
    let account_owned = account.clone();
    f.db.control_write("test", move |txn| {
        let account = account_owned.clone();
        Box::pin(async move { txn.account_ledger_upsert(&account, 9999, 9).await })
    })
    .await
    .unwrap();
    quota.refresh_all(&f.store).await.unwrap();
    assert_eq!(ledger(&f.db, &account).await, Some((9999, 9)));
}
