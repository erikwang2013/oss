//! Shared B3 helpers: object stores in both modes, trait-object runners and
//! IAM row dumps.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tempfile::TempDir;
use xl_store::control_state::root::Mode;
use xl_store::control_store::ControlError;
use xl_store::control_store::iam::{IamSnapshotTxn, IamTxn};
use xl_store::store::{Store, StoreConfig};

use crate::control_store_obj::{ObjectControlStore, RootIo, WriteOrigin};
use crate::local_store::LocalStore;

use super::super::iam::{scan_access_keys, scan_permissions, scan_users};

pub(super) type Boxed<'a, R> = Pin<Box<dyn Future<Output = Result<R, ControlError>> + 'a>>;

/// Supertrait bundle so one transaction object exposes reads, writes and
/// snapshot install methods (trait objects allow only one non-auto trait).
pub(super) trait Iam: IamTxn + IamSnapshotTxn {}
impl<T: IamTxn + IamSnapshotTxn> Iam for T {}

pub(super) struct Obj {
    pub(super) _dir: TempDir,
    pub(super) _store: Arc<Store>,
    pub(super) control: Arc<ObjectControlStore>,
}

/// Object-backed fixture with a bootstrapped root and local state directory.
pub(super) async fn object_store(mode: Mode) -> Obj {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(dir.path());
    cfg.fsync = false;
    let store = Arc::new(Store::open(cfg).await.unwrap());
    let local = LocalStore::open(dir.path().join("local")).unwrap();
    let control = Arc::new(
        ObjectControlStore::new(Arc::new(RootIo::new(Arc::clone(&store)))).with_local_store(local),
    );
    control.bootstrap(mode, 100).await.unwrap();
    Obj {
        _dir: dir,
        _store: store,
        control,
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
