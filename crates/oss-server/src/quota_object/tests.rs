use xl_store::control_state::root::Mode;
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketCreateOutcome, BucketEntry};

use crate::control_backend::ObjectControl;
use crate::local_store::LocalStore;

use super::cache::Usage;
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

fn cache(db: &Db) -> UsageCache {
    UsageCache::new(
        db.object_control()
            .unwrap()
            .control
            .local_store()
            .unwrap()
            .clone(),
    )
}

fn cached(db: &Db, account: &str) -> Option<(i64, i64)> {
    cache(db)
        .load(account)
        .unwrap()
        .map(|u| (u.used_bytes, u.object_count))
}

fn seed_cache(db: &Db, account: &str, used: i64, count: i64) {
    cache(db)
        .store(
            account,
            &Usage {
                used_bytes: used,
                object_count: count,
                complete: true,
                computed_at: crate::db::now(),
            },
        )
        .unwrap();
}

/// 控制根代际：热路径必须保持不变（无根 CAS）。
async fn root_version(db: &Db) -> u64 {
    db.object_control()
        .unwrap()
        .control
        .pin()
        .await
        .unwrap()
        .version
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
async fn admit_adjust_release_follow_local_cache_limits() {
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
    assert_eq!(cached(&f.db, &account), Some((10, 2)));

    quota.adjust(&account, -10, -2).await.unwrap();
    assert_eq!(cached(&f.db, &account), Some((0, 0)));
    assert!(matches!(
        quota.admit(&f.store, &q, 0, 2).await.unwrap(),
        Ok(())
    ));
    // release 是 adjust 的负数形式（饱和到 0）。
    quota.release(&account, 5, 5).await.unwrap();
    assert_eq!(cached(&f.db, &account), Some((0, 0)));
}

/// 并发抢边界：同账户本地锁串行化，恰好 limit 个成功。
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
    assert_eq!(cached(&f.db, &account), Some((0, 3)));
}

/// 回归：对象模式必须读节点本地缓存。占位内存库没有任何表；若实现回落到
/// `db.pool()`，这里会得到 Backend 错误而不是稳定配额拒绝。
#[tokio::test]
async fn admit_denies_from_durable_local_cache_without_sqlite_tables() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "b1", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(10), None).await;
    seed_cache(&f.db, &account, 10, 1);

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    let q = quota.account_quota_for_bucket("b1").await.unwrap().unwrap();
    match quota.admit(&f.store, &q, 1, 0).await {
        Ok(Err(QuotaDenied::Bytes)) => {}
        other => panic!("必须由本地缓存稳定拒绝，得到 {other:?}"),
    }
    assert_eq!(cached(&f.db, &account), Some((10, 1)));
}

/// 采样：完整列举按当前版本对象校准本地缓存与账本；下界不覆盖已知更大值。
#[tokio::test]
async fn refresh_all_recomputes_cache_and_ledger_from_objects() {
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
    // 缓存与账本先记一个偏大的值；完整采样必须校准回真实用量。
    seed_cache(&f.db, &account, 999, 9);
    let account_owned = account.clone();
    f.db.control_write("test", move |txn| {
        let account = account_owned.clone();
        Box::pin(async move { txn.account_ledger_upsert(&account, 999, 9).await })
    })
    .await
    .unwrap();

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    quota.refresh_all(&f.store).await.unwrap();
    assert_eq!(cached(&f.db, &account), Some((8, 2)));
    assert_eq!(ledger(&f.db, &account).await, Some((8, 2)));

    // 控制面有行但物理桶不可列举 = 下界：不得覆盖已知更大值。
    create_bucket(&f.db, "ghost", "alice").await;
    seed_cache(&f.db, &account, 9999, 9);
    let account_owned = account.clone();
    f.db.control_write("test", move |txn| {
        let account = account_owned.clone();
        Box::pin(async move { txn.account_ledger_upsert(&account, 9999, 9).await })
    })
    .await
    .unwrap();
    quota.refresh_all(&f.store).await.unwrap();
    assert_eq!(cached(&f.db, &account), Some((9999, 9)));
    assert_eq!(ledger(&f.db, &account).await, Some((9999, 9)));
}

/// D4 回归：数据面预留/校准/释放只写节点本地文件，绝不提交控制根。
#[tokio::test]
async fn data_plane_admits_do_not_commit_the_control_root() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "b1", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(1000), Some(100)).await;

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    let q = quota.account_quota_for_bucket("b1").await.unwrap().unwrap();
    let before = root_version(&f.db).await;
    for _ in 0..5 {
        assert!(quota.admit(&f.store, &q, 10, 1).await.unwrap().is_ok());
    }
    quota.adjust(&account, -5, 0).await.unwrap();
    quota.release(&account, 5, 1).await.unwrap();
    assert_eq!(
        root_version(&f.db).await,
        before,
        "热路径不得推进控制根代际"
    );
    assert_eq!(cached(&f.db, &account), Some((40, 4)));
}

/// 重启后本地缓存仍在：新的适配器实例继续从持久化用量判定。
#[tokio::test]
async fn restart_preserves_admitted_usage() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "b1", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(10), None).await;

    let first = ObjectQuota::new(Arc::clone(&f.db));
    let q = first.account_quota_for_bucket("b1").await.unwrap().unwrap();
    assert!(first.admit(&f.store, &q, 7, 0).await.unwrap().is_ok());
    drop(first);

    // 独立打开同一节点本地目录，证明值来自磁盘而不是进程内状态。
    let local = LocalStore::open(
        f.db.object_control()
            .unwrap()
            .control
            .local_store()
            .unwrap()
            .root(),
    )
    .unwrap();
    assert_eq!(
        UsageCache::new(local)
            .load(&account)
            .unwrap()
            .map(|u| (u.used_bytes, u.object_count)),
        Some((7, 0))
    );

    let second = ObjectQuota::new(Arc::clone(&f.db));
    assert!(second.admit(&f.store, &q, 3, 0).await.unwrap().is_ok());
    assert!(matches!(
        second.admit(&f.store, &q, 1, 0).await.unwrap(),
        Err(QuotaDenied::Bytes)
    ));
    assert_eq!(usage_baseline(&f.db, &account).await, (10, 0));
}

/// 采样器在缓存被清空后按对象列举重建（启动/装快照后的路径）。
#[tokio::test]
async fn sampler_rebuilds_cache_after_wipe() {
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

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    quota.refresh_all(&f.store).await.unwrap();
    assert_eq!(cached(&f.db, &account), Some((8, 2)));
    cache(&f.db).clear().unwrap();
    assert_eq!(cached(&f.db, &account), None);

    quota.refresh_all(&f.store).await.unwrap();
    assert_eq!(cached(&f.db, &account), Some((8, 2)));
}

/// 缓存缺失时热路径先重列举，不得从 0 起算而超发。
#[tokio::test]
async fn cold_admit_rebuilds_missing_cache_from_objects() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "bkt", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(10), None).await;
    f.store.create_bucket("bkt").await.unwrap();
    f.store
        .put_object("bkt", "a", bytes::Bytes::from_static(b"12345678"), None)
        .await
        .unwrap();
    cache(&f.db).clear().unwrap();

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    let q = quota
        .account_quota_for_bucket("bkt")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        quota.admit(&f.store, &q, 3, 0).await.unwrap(),
        Err(QuotaDenied::Bytes)
    ));
    assert_eq!(cached(&f.db, &account), Some((8, 1)));
}

/// 手工基准（默认不跑）：本地预留的吞吐与根代际不变。
/// `cargo test -p oss-server --lib bench_local_admit_hot_path -- --ignored --nocapture`
#[tokio::test]
#[ignore = "manual hot-path timing"]
async fn bench_local_admit_hot_path() {
    let f = fixture().await;
    let uid =
        f.db.create_user("alice", "alice-pw-123", false)
            .await
            .unwrap();
    create_bucket(&f.db, "b1", "alice").await;
    let account = xl_s3::access::account_id_of(uid).unwrap();
    set_limits(&f.db, &account, Some(1_000_000), Some(1_000_000)).await;

    let quota = ObjectQuota::new(Arc::clone(&f.db));
    let q = quota.account_quota_for_bucket("b1").await.unwrap().unwrap();
    let before = root_version(&f.db).await;
    let started = std::time::Instant::now();
    for _ in 0..200 {
        quota.admit(&f.store, &q, 10, 1).await.unwrap().unwrap();
    }
    eprintln!(
        "200 local admits in {:?} ({:?}/op), root generation {} -> {}",
        started.elapsed(),
        started.elapsed() / 200,
        before,
        root_version(&f.db).await
    );
    assert_eq!(root_version(&f.db).await, before);
}
