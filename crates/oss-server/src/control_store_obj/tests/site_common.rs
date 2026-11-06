//! Shared helpers for the D2 trigger-vs-hook golden matrix.

use futures::future::BoxFuture;
use xl_store::control_store::ControlError;
use xl_store::control_store::sites::{SitePeerRow, SiteTxn};

use super::{local, real_store};
use crate::control_store_obj::site::SiteChangelogRow;
use crate::control_store_obj::{ObjectControlTxn, WriteOrigin};

pub(super) type Real = super::Real;

pub(super) fn se(e: ControlError) -> xl_store::error::Error {
    xl_store::error::Error::InvalidArgument(e.to_string())
}

pub(super) fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Object side: matching config (priority 7) and paired site.
pub(super) async fn object_site(id: &str) -> Real {
    let real = real_store().await;
    let peer = SitePeerRow {
        peer_id: "peer-b".into(),
        remote_site_id: "remote-b".into(),
        name: "b".into(),
        endpoint: "http://127.0.0.1:9".into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "ak".into(),
        s3_secret_key: "sk".into(),
        peer_site_token: "tok".into(),
        created_at: 0,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    };
    let id = id.to_string();
    tx_once(&real, local(), |txn| {
        let id = id.clone();
        Box::pin(async move {
            txn.site_config_seed(&id, "tok", 1).await.map_err(se)?;
            txn.site_config_apply(None, None, Some(7), None)
                .await
                .map_err(se)?;
            txn.site_insert(&peer, 1).await.map_err(se)?;
            Ok(())
        })
    })
    .await
    .unwrap();
    real
}

/// Single-attempt transaction for a closure that is only run once.
pub(super) async fn tx_once<T, F>(
    real: &Real,
    origin: WriteOrigin,
    f: F,
) -> xl_store::error::Result<T>
where
    F: for<'a> FnOnce(&'a mut ObjectControlTxn) -> BoxFuture<'a, xl_store::error::Result<T>>,
{
    let mut txn = real.control.begin(origin).await?;
    let out = f(&mut txn).await?;
    txn.commit().await?;
    Ok(out)
}

pub(super) async fn run_origin<F>(
    real: &Real,
    origin: WriteOrigin,
    f: F,
) -> Result<(), ControlError>
where
    F: for<'a> FnOnce(&'a mut ObjectControlTxn) -> BoxFuture<'a, Result<(), ControlError>>,
{
    let mut txn = real
        .control
        .begin(origin)
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    f(&mut txn).await?;
    txn.commit()
        .await
        .map_err(|e| ControlError::Backend(e.to_string()))?;
    Ok(())
}

pub(super) async fn run_hook<F>(real: &Real, f: F) -> Result<(), ControlError>
where
    F: for<'a> FnOnce(&'a mut ObjectControlTxn) -> BoxFuture<'a, Result<(), ControlError>>,
{
    run_origin(real, WriteOrigin::local("test"), f).await
}

pub(super) async fn obj_rows(real: &Real) -> Vec<SiteChangelogRow> {
    real.control.site_changelog_after(0).await.unwrap()
}
