//! Shared C1/B3 parity helpers: SQLite oracle DBs, object stores in both
//! modes, trait-object transaction runners, and full-table row dumps.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use sqlx::Acquire;
use tempfile::TempDir;
use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;
use xl_store::control_store::account::{AccountSnapshotTxn, AccountTxn};
use xl_store::control_store::iam::{IamSnapshotTxn, IamTxn};
use xl_store::store::{Store, StoreConfig};

use crate::control_store::{SqliteControlStore, SqliteControlTxn};
use crate::control_store_obj::{ObjectControlStore, RootIo, WriteOrigin};
use crate::db::Db;

use super::super::account::{scan_accounts, scan_ledger, scan_members};
use super::super::iam::{scan_access_keys, scan_permissions, scan_users};

pub(super) type Boxed<'a, R> = Pin<Box<dyn Future<Output = Result<R, ControlError>> + 'a>>;

/// Supertrait bundle so one transaction object exposes reads, writes and
/// snapshot install methods (trait objects allow only one non-auto trait).
pub(super) trait Iam: IamTxn + IamSnapshotTxn {}
impl<T: IamTxn + IamSnapshotTxn> Iam for T {}
pub(super) trait Account: AccountTxn + AccountSnapshotTxn {}
impl<T: AccountTxn + AccountSnapshotTxn> Account for T {}

pub(super) struct Obj {
    pub(super) _dir: TempDir,
    pub(super) store: Arc<Store>,
    pub(super) control: Arc<ObjectControlStore>,
}

pub(super) async fn sqlite(raft: bool) -> Db {
    Db::connect_site("sqlite::memory:", raft, None)
        .await
        .unwrap()
}

/// Toggle the SQLite raft guard triggers for the next local write.
pub(super) async fn set_apply_ctx(db: &Db, active: bool) {
    sqlx::query("UPDATE raft_apply_ctx SET active = ? WHERE id = 1")
        .bind(active as i64)
        .execute(db.pool())
        .await
        .unwrap();
}

pub(super) async fn object_store(mode: Mode) -> Obj {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let control = Arc::new(ObjectControlStore::new(Arc::new(RootIo::new(Arc::clone(
        &store,
    )))));
    control.bootstrap(mode, 100).await.unwrap();
    Obj {
        _dir: dir,
        store,
        control,
    }
}

pub(super) async fn sqlite_iam<R>(
    db: &Db,
    f: impl for<'a> FnOnce(&'a mut dyn Iam) -> Boxed<'a, R>,
) -> Result<R, ControlError> {
    let mut conn = db.pool().acquire().await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    let out = {
        let mut txn = SqliteControlTxn::new(&mut tx);
        f(&mut txn).await
    };
    match out {
        Ok(value) => {
            tx.commit().await.unwrap();
            Ok(value)
        }
        Err(e) => {
            tx.rollback().await.unwrap();
            Err(e)
        }
    }
}

pub(super) async fn object_iam<R>(
    control: &Arc<ObjectControlStore>,
    origin: WriteOrigin,
    f: impl for<'a> FnOnce(&'a mut dyn Iam) -> Boxed<'a, R>,
) -> Result<R, ControlError> {
    let mut txn = control
        .begin(origin)
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    let out = f(&mut txn).await?;
    txn.commit()
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    Ok(out)
}

pub(super) async fn sqlite_account<R>(
    db: &Db,
    f: impl for<'a> FnOnce(&'a mut dyn Account) -> Boxed<'a, R>,
) -> Result<R, ControlError> {
    let mut conn = db.pool().acquire().await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    let out = {
        let mut txn = SqliteControlTxn::new(&mut tx);
        f(&mut txn).await
    };
    match out {
        Ok(value) => {
            tx.commit().await.unwrap();
            Ok(value)
        }
        Err(e) => {
            tx.rollback().await.unwrap();
            Err(e)
        }
    }
}

pub(super) async fn object_account<R>(
    control: &Arc<ObjectControlStore>,
    origin: WriteOrigin,
    f: impl for<'a> FnOnce(&'a mut dyn Account) -> Boxed<'a, R>,
) -> Result<R, ControlError> {
    let mut txn = control
        .begin(origin)
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    let out = f(&mut txn).await?;
    txn.commit()
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    Ok(out)
}

pub(super) fn sqlite_store(db: &Db) -> SqliteControlStore {
    SqliteControlStore::new(db.pool().clone())
}

/// Run the same write twice: SQLite oracle, then object store under `Local`.
pub(super) async fn both_iam<T, F>(
    db: &Db,
    obj: &Obj,
    f: F,
) -> (Result<T, ControlError>, Result<T, ControlError>)
where
    F: for<'a> FnOnce(&'a mut dyn Iam) -> Boxed<'a, T> + Copy,
{
    let oracle = sqlite_iam(db, f).await;
    let object = object_iam(&obj.control, WriteOrigin::local("test"), f).await;
    (oracle, object)
}

pub(super) async fn both_account<T, F>(
    db: &Db,
    obj: &Obj,
    f: F,
) -> (Result<T, ControlError>, Result<T, ControlError>)
where
    F: for<'a> FnOnce(&'a mut dyn Account) -> Boxed<'a, T> + Copy,
{
    let oracle = sqlite_account(db, f).await;
    let object = object_account(&obj.control, WriteOrigin::local("test"), f).await;
    (oracle, object)
}

pub(super) fn same_ok<T: PartialEq + std::fmt::Debug>(
    pair: (Result<T, ControlError>, Result<T, ControlError>),
) -> T {
    match pair {
        (Ok(a), Ok(b)) => {
            assert_eq!(a, b, "oracle vs object value");
            a
        }
        (a, b) => panic!("expected both Ok, got {a:?} / {b:?}"),
    }
}

pub(super) fn same_err(
    pair: (
        Result<impl std::fmt::Debug, ControlError>,
        Result<impl std::fmt::Debug, ControlError>,
    ),
) -> (ControlError, ControlError) {
    match pair {
        (Err(a), Err(b)) => (a, b),
        (a, b) => panic!("expected both Err, got {a:?} / {b:?}"),
    }
}

pub(super) async fn dump_iam(db: &Db, obj: &Obj) {
    dump_pair(
        "users",
        sqlite_users(db).await,
        object_users(&obj.control).await,
    );
    dump_pair(
        "access keys",
        sqlite_keys(db).await,
        object_keys(&obj.control).await,
    );
    dump_pair(
        "permissions",
        sqlite_perms(db).await,
        object_perms(&obj.control).await,
    );
}

pub(super) async fn dump_accounts(db: &Db, obj: &Obj) {
    dump_pair(
        "accounts",
        sqlite_accounts(db).await,
        object_accounts(&obj.control).await,
    );
    dump_pair(
        "members",
        sqlite_members(db).await,
        object_members(&obj.control).await,
    );
    dump_pair(
        "ledger",
        sqlite_ledger(db).await,
        object_ledger(&obj.control).await,
    );
}

/// Admin (uid 1) + users 2..=4 and keys/permissions covering the guard matrix.
pub(super) async fn seed_iam(txn: &mut dyn Iam) -> Result<(), ControlError> {
    use xl_store::control_store::iam::{AccessKeyEntry, UserEntry};

    let admin = txn
        .user_create_default_status("admin", "h-admin", true, 10)
        .await?;
    let alice = txn
        .user_create(&UserEntry {
            username: "alice".into(),
            password_hash: "h-alice".into(),
            status: "active".into(),
            created_at: 20,
        })
        .await?;
    let bob = txn
        .user_create(&UserEntry {
            username: "bob".into(),
            password_hash: "h-bob".into(),
            status: "disabled".into(),
            created_at: 21,
        })
        .await?;
    txn.user_create(&UserEntry {
        username: "carol".into(),
        password_hash: "h-carol".into(),
        status: "active".into(),
        created_at: 22,
    })
    .await?;

    let key = |ak: &str, sk: &str, owner: &str, kind: &str| AccessKeyEntry {
        access_key: ak.into(),
        secret_key: sk.into(),
        username: owner.into(),
        status: "ignored".into(),
        expires_at: Some(7),
        created_at: 30,
        parent_ak: None,
        session_policy: None,
        kind: kind.into(),
    };
    txn.access_key_create(&key("AKALICE", "sk-alice", "alice", "long-term"))
        .await?;
    let mut session = key("SESSALICE", "sk-sess", "alice", "session");
    session.parent_ak = Some("AKALICE".into());
    session.session_policy = Some("pol".into());
    txn.access_key_create(&session).await?;
    txn.access_key_create(&key("AKADMIN", "sk-admin", "admin", "long-term"))
        .await?;
    txn.access_key_create_legacy("AKBOB", "sk-bob", bob, 31)
        .await?;

    txn.permission_upsert(alice, "b1", true, true, false)
        .await?;
    txn.permission_upsert(alice, "b0", false, false, false)
        .await?;
    txn.permission_upsert(admin, "b1", true, true, true).await?;
    Ok(())
}

pub(super) fn dump_pair<T: PartialEq + std::fmt::Debug>(what: &str, a: T, b: T) {
    assert_eq!(a, b, "{what} parity");
}

pub(super) type UserDump = (i64, String, String, i64, String, i64);
pub(super) type KeyDump = (
    String,
    String,
    i64,
    String,
    Option<i64>,
    i64,
    Option<String>,
    Option<String>,
    String,
);
pub(super) type PermDump = (i64, String, i64, i64, i64);
pub(super) type AccountDump = (
    String,
    String,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    i64,
    i64,
);
pub(super) type MemberDump = (String, i64, String, i64);
pub(super) type LedgerDump = (String, i64, i64);

pub(super) async fn sqlite_users(db: &Db) -> Vec<UserDump> {
    sqlx::query_as(
        "SELECT id, username, password_hash, is_admin, status, created_at \
         FROM users ORDER BY id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
}

pub(super) async fn object_users(control: &Arc<ObjectControlStore>) -> Vec<UserDump> {
    let txn = control.begin(WriteOrigin::local("read")).await.unwrap();
    scan_users(&txn)
        .await
        .unwrap()
        .into_iter()
        .map(|u| {
            (
                u.uid,
                u.username,
                u.password_hash,
                u.is_admin as i64,
                u.status,
                u.created_at,
            )
        })
        .collect()
}

pub(super) async fn sqlite_keys(db: &Db) -> Vec<KeyDump> {
    sqlx::query_as(
        "SELECT access_key, secret_key, user_id, status, expires_at, created_at, \
                parent_ak, session_policy, kind FROM access_keys ORDER BY access_key",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
}

pub(super) async fn object_keys(control: &Arc<ObjectControlStore>) -> Vec<KeyDump> {
    let txn = control.begin(WriteOrigin::local("read")).await.unwrap();
    let mut rows: Vec<KeyDump> = scan_access_keys(&txn)
        .await
        .unwrap()
        .into_iter()
        .map(|k| {
            (
                k.access_key,
                k.secret_key,
                k.user_id,
                k.status,
                k.expires_at,
                k.created_at,
                k.parent_ak,
                k.session_policy,
                k.kind,
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

pub(super) async fn sqlite_perms(db: &Db) -> Vec<PermDump> {
    sqlx::query_as(
        "SELECT user_id, bucket, can_read, can_write, can_delete \
         FROM permissions ORDER BY user_id, bucket",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
}

pub(super) async fn object_perms(control: &Arc<ObjectControlStore>) -> Vec<PermDump> {
    let txn = control.begin(WriteOrigin::local("read")).await.unwrap();
    let mut rows: Vec<PermDump> = scan_permissions(&txn)
        .await
        .unwrap()
        .into_iter()
        .map(|p| {
            (
                p.user_id,
                p.bucket,
                p.can_read as i64,
                p.can_write as i64,
                p.can_delete as i64,
            )
        })
        .collect();
    rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    rows
}

pub(super) async fn sqlite_accounts(db: &Db) -> Vec<AccountDump> {
    sqlx::query_as(
        "SELECT id, name, status, admin_uid, bucket_quota, soft_byte_quota, \
                hard_byte_quota, object_quota, created_at, updated_at \
         FROM accounts ORDER BY id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
}

pub(super) async fn object_accounts(control: &Arc<ObjectControlStore>) -> Vec<AccountDump> {
    let txn = control.begin(WriteOrigin::local("read")).await.unwrap();
    let mut rows: Vec<AccountDump> = scan_accounts(&txn)
        .await
        .unwrap()
        .into_iter()
        .map(|a| {
            (
                a.id,
                a.name,
                a.status,
                a.admin_uid,
                a.bucket_quota,
                a.soft_byte_quota,
                a.hard_byte_quota,
                a.object_quota,
                a.created_at,
                a.updated_at,
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

pub(super) async fn sqlite_members(db: &Db) -> Vec<MemberDump> {
    sqlx::query_as(
        "SELECT account_id, uid, role, created_at FROM account_members \
         ORDER BY account_id, uid",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
}

pub(super) async fn object_members(control: &Arc<ObjectControlStore>) -> Vec<MemberDump> {
    let txn = control.begin(WriteOrigin::local("read")).await.unwrap();
    let mut rows: Vec<MemberDump> = scan_members(&txn)
        .await
        .unwrap()
        .into_iter()
        .map(|m| (m.account_id, m.uid, m.role, m.created_at))
        .collect();
    rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    rows
}

pub(super) async fn sqlite_ledger(db: &Db) -> Vec<LedgerDump> {
    sqlx::query_as(
        "SELECT account_id, used_bytes, object_count FROM account_usage_ledger \
         ORDER BY account_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
}

pub(super) async fn object_ledger(control: &Arc<ObjectControlStore>) -> Vec<LedgerDump> {
    let txn = control.begin(WriteOrigin::local("read")).await.unwrap();
    let mut rows: Vec<LedgerDump> = scan_ledger(&txn)
        .await
        .unwrap()
        .into_iter()
        .map(|l| (l.account_id, l.used_bytes, l.object_count))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}
