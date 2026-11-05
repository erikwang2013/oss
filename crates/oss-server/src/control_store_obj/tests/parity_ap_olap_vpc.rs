//! Access-point / OLAP / VPC parity between the SQLite seam adapters and the
//! object backend, including large-module chunking and owner strictness.

use xl_store::control_store::{AccessPointEntry, OlapConfigEntry, OwnerRef, VpcEndpoint};
use xl_store::error::Error as StoreError;

use super::super::bucket::user_key;
use super::super::{AccessPointStore, AccessPointTxn, TableId, VpcStore, VpcTxn};
use super::local;
use super::parity_util::{err, object, parity, sqlite};

fn ap(name: &str, bucket: &str, owner: OwnerRef, ts: i64) -> AccessPointEntry {
    AccessPointEntry {
        name: name.into(),
        bucket: bucket.into(),
        owner,
        policy_json: None,
        created_at: ts,
        updated_at: ts,
    }
}

fn olap(
    access_point: &str,
    kind: &str,
    endpoint: Option<&str>,
    module: Option<Vec<u8>>,
    ts: i64,
) -> OlapConfigEntry {
    OlapConfigEntry {
        access_point: access_point.into(),
        kind: kind.into(),
        endpoint: endpoint.map(Into::into),
        module,
        module_sha256: None,
        timeout_ms: 5000,
        max_response_bytes: 1024,
        updated_at: ts,
    }
}

#[derive(Debug, PartialEq)]
struct ApOut {
    flags: Vec<bool>,
    exports: Vec<AccessPointEntry>,
    olaps: Vec<OlapConfigEntry>,
}

async fn ap_scenario<T: AccessPointTxn>(txn: &mut T, big: Vec<u8>) -> Result<ApOut, StoreError> {
    let mut flags = Vec::new();
    flags.push(
        txn.access_point_create(&ap("ap1", "b1", OwnerRef::User("alice".into()), 1))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.access_point_create(&ap("ap2", "b1", OwnerRef::Admin, 2))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.access_point_create(&ap("ap3", "b1", OwnerRef::User("root".into()), 3))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.access_point_create(&ap("ap1", "b1", OwnerRef::Admin, 9))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.access_point_set_policy("ap1", Some(r#"{"p":1}"#), 5)
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.access_point_set_policy("missing", Some("{}"), 6)
            .await
            .map_err(err)?,
    );
    txn.olap_config_upsert(&olap("ap1", "wasm", None, Some(big), 7))
        .await
        .map_err(err)?;
    txn.olap_config_upsert(&olap("ap2", "http", Some("https://x"), None, 8))
        .await
        .map_err(err)?;
    txn.olap_config_upsert(&olap("ap3", "http", Some("https://y"), None, 9))
        .await
        .map_err(err)?;
    flags.push(txn.access_point_exists("ap1").await.map_err(err)?);
    flags.push(txn.access_point_exists("nope").await.map_err(err)?);
    flags.push(txn.olap_config_delete("missing").await.map_err(err)?);
    Ok(ApOut {
        flags,
        exports: txn.access_points().await.map_err(err)?,
        olaps: txn.olap_configs().await.map_err(err)?,
    })
}

#[tokio::test]
async fn ap_olap_writes_and_reads_match_with_chunked_module() {
    let p = parity(&[("root", true), ("alice", false)]).await;
    let big: Vec<u8> = (0..200 * 1024).map(|i| (i * 31 % 251) as u8).collect();
    let sq = sqlite(&p, |t| {
        let big = big.clone();
        Box::pin(async move { ap_scenario(t, big).await })
    })
    .await
    .unwrap();
    let ob = object(&p, |t| {
        let big = big.clone();
        Box::pin(async move { ap_scenario(t, big).await })
    })
    .await
    .unwrap();
    assert_eq!(sq, ob);
    assert_eq!(
        sq.flags,
        vec![true, true, true, false, true, false, true, false, false]
    );
    assert_eq!(
        sq.exports
            .iter()
            .map(|a| a.name.as_str())
            .collect::<Vec<_>>(),
        ["ap1", "ap2", "ap3"]
    );
    assert_eq!(sq.exports[0].policy_json.as_deref(), Some(r#"{"p":1}"#));
    assert_eq!(sq.exports[1].owner, OwnerRef::Admin, "admin id → Admin");
    assert_eq!(sq.exports[2].owner, OwnerRef::Admin, "admin user → Admin");
    assert_eq!(sq.olaps[0].access_point, "ap1");
    assert_eq!(sq.olaps[0].module.as_deref(), Some(&big[..]));
    assert!(sq.olaps[0].module.as_ref().unwrap().len() > 128 * 1024);

    let sq_store = p.db.control_store();
    let ob_store = p.real.control.as_ref();
    assert_eq!(
        sq_store.access_points().await.unwrap(),
        ob_store.access_points().await.unwrap()
    );
    assert_eq!(
        sq_store.olap_configs().await.unwrap(),
        ob_store.olap_configs().await.unwrap()
    );
    let ap3 = ob_store.access_point("ap3").await.unwrap().unwrap();
    assert_eq!(
        ap3.owner,
        OwnerRef::User("root".into()),
        "runtime keeps admin as User"
    );
    assert_eq!(
        ob_store.olap_config("ap1").await.unwrap().unwrap().module,
        Some(big)
    );
}

#[tokio::test]
async fn ap_owner_errors_and_dangling_match() {
    let p = parity(&[("root", true), ("alice", false)]).await;
    let ghost = ap("ap1", "b1", OwnerRef::User("ghost".into()), 1);
    let sq_err = sqlite(&p, |t| {
        let ghost = ghost.clone();
        Box::pin(async move {
            t.access_point_create(&ghost).await.map_err(err)?;
            Ok(())
        })
    })
    .await
    .unwrap_err();
    let ob_err = object(&p, |t| {
        let ghost = ghost.clone();
        Box::pin(async move {
            t.access_point_create(&ghost).await.map_err(err)?;
            Ok(())
        })
    })
    .await
    .unwrap_err();
    assert_eq!(sq_err, ob_err);
    assert_eq!(sq_err, "invalid control data: owner user ghost not found");
    assert!(p.real.control.access_points().await.unwrap().is_empty());

    // Dangling owner: create for alice, then remove the user row.
    let ap_alice = ap("ap-alice", "b1", OwnerRef::User("alice".into()), 2);
    sqlite(&p, |t| {
        let row = ap_alice.clone();
        Box::pin(async move { t.access_point_create(&row).await.map_err(err).map(|_| ()) })
    })
    .await
    .unwrap();
    object(&p, |t| {
        let row = ap_alice.clone();
        Box::pin(async move { t.access_point_create(&row).await.map_err(err).map(|_| ()) })
    })
    .await
    .unwrap();
    let alice = p.uid("alice");
    let mut conn = p.db.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("DELETE FROM users WHERE id = ?")
        .bind(alice)
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);
    p.real
        .control
        .transaction(local(), move |txn| {
            Box::pin(async move { txn.delete(TableId::Users, &user_key(alice)).await })
        })
        .await
        .unwrap();

    assert_eq!(
        p.real
            .control
            .access_point("ap-alice")
            .await
            .unwrap()
            .unwrap()
            .owner,
        OwnerRef::Admin,
        "runtime dangling → Admin"
    );
    let sq_err = sqlite(&p, |t| {
        Box::pin(async move { t.access_points().await.map_err(err).map(|_| ()) })
    })
    .await
    .unwrap_err();
    let ob_err = object(&p, |t| {
        Box::pin(async move { t.access_points().await.map_err(err).map(|_| ()) })
    })
    .await
    .unwrap_err();
    assert_eq!(sq_err, ob_err);
    assert_eq!(
        sq_err,
        format!("invalid control data: owner user id {alice} missing")
    );
}

#[tokio::test]
async fn ap_delete_cascades_only_its_bucket() {
    let p = parity(&[("alice", false)]).await;

    async fn seed<T: AccessPointTxn>(t: &mut T) -> Result<(), StoreError> {
        for (name, bucket) in [("ap-a", "b1"), ("ap-b", "b2"), ("ap-c", "b1")] {
            t.access_point_create(&ap(name, bucket, OwnerRef::User("alice".into()), 1))
                .await
                .map_err(err)?;
            t.olap_config_upsert(&olap(name, "http", Some("https://x"), None, 2))
                .await
                .map_err(err)?;
        }
        Ok(())
    }
    async fn cascade<T: AccessPointTxn>(
        t: &mut T,
    ) -> Result<(bool, bool, Vec<String>, Vec<String>), StoreError> {
        t.delete_access_points_for_bucket("b1").await.map_err(err)?;
        let deleted = t.access_point_delete("ap-b").await.map_err(err)?;
        let deleted_again = t.access_point_delete("ap-b").await.map_err(err)?;
        Ok((
            deleted,
            deleted_again,
            t.access_points()
                .await
                .map_err(err)?
                .into_iter()
                .map(|a| a.name)
                .collect(),
            t.olap_configs()
                .await
                .map_err(err)?
                .into_iter()
                .map(|o| o.access_point)
                .collect(),
        ))
    }
    sqlite(&p, |t| Box::pin(seed(t))).await.unwrap();
    object(&p, |t| Box::pin(seed(t))).await.unwrap();
    let sq = sqlite(&p, |t| Box::pin(cascade(t))).await.unwrap();
    let ob = object(&p, |t| Box::pin(cascade(t))).await.unwrap();
    assert_eq!(sq, ob);
    assert_eq!(sq, (true, false, vec![], vec![]));
    assert!(p.real.control.access_points().await.unwrap().is_empty());
    assert!(p.real.control.olap_configs().await.unwrap().is_empty());
}

#[tokio::test]
async fn ap_olap_snapshot_replace_matches_bytes() {
    let p = parity(&[("root", true), ("alice", false)]).await;
    let aps = vec![
        ap("snap-admin", "b1", OwnerRef::Admin, 1),
        ap("snap-user", "b1", OwnerRef::User("alice".into()), 2),
    ];
    let olaps = vec![
        olap("snap-admin", "wasm", None, Some(vec![0, 1, 2, 255]), 3),
        olap("snap-user", "http", Some("https://x"), None, 4),
    ];
    async fn install<T: AccessPointTxn>(
        t: &mut T,
        aps: &[AccessPointEntry],
        olaps: &[OlapConfigEntry],
    ) -> Result<(Vec<AccessPointEntry>, Vec<OlapConfigEntry>), StoreError> {
        t.replace_access_points(aps).await.map_err(err)?;
        t.replace_olap_configs(olaps).await.map_err(err)?;
        Ok((
            t.access_points().await.map_err(err)?,
            t.olap_configs().await.map_err(err)?,
        ))
    }
    let sq = sqlite(&p, |t| {
        let aps = aps.clone();
        let olaps = olaps.clone();
        Box::pin(async move { install(t, &aps, &olaps).await })
    })
    .await
    .unwrap();
    let ob = object(&p, |t| {
        let aps = aps.clone();
        let olaps = olaps.clone();
        Box::pin(async move { install(t, &aps, &olaps).await })
    })
    .await
    .unwrap();
    assert_eq!(sq, ob);
    assert_eq!(sq.0, aps);
    assert_eq!(sq.1, olaps);
    assert_eq!(
        serde_json::to_vec(&sq).unwrap(),
        serde_json::to_vec(&ob).unwrap()
    );
}

fn vp(id: &str, created_at: i64, updated_at: i64) -> VpcEndpoint {
    VpcEndpoint {
        vpce_id: id.into(),
        vpc_id: format!("vpc-{id}"),
        account_id: "acct".into(),
        state: "available".into(),
        created_at,
        updated_at,
    }
}

#[derive(Debug, PartialEq)]
struct VpcOut {
    flags: Vec<bool>,
    first: Vec<VpcEndpoint>,
    second: Vec<VpcEndpoint>,
}

async fn vpc_scenario<T: VpcTxn>(txn: &mut T) -> Result<VpcOut, StoreError> {
    txn.vpc_endpoint_upsert(&vp("v2", 20, 1))
        .await
        .map_err(err)?;
    txn.vpc_endpoint_upsert(&vp("v1", 10, 1))
        .await
        .map_err(err)?;
    txn.vpc_endpoint_upsert(&vp("v2", 99, 2))
        .await
        .map_err(err)?;
    let mut flags = vec![txn.vpc_endpoint_delete("nope").await.map_err(err)?];
    let first = txn.vpc_endpoints().await.map_err(err)?;
    txn.replace_vpc_endpoints(&[vp("v4", 40, 1), vp("v3", 30, 1)])
        .await
        .map_err(err)?;
    let second = txn.vpc_endpoints().await.map_err(err)?;
    flags.push(txn.vpc_endpoint_delete("v3").await.map_err(err)?);
    Ok(VpcOut {
        flags,
        first,
        second,
    })
}

#[tokio::test]
async fn vpc_writes_and_reads_match() {
    let p = parity(&[("alice", false)]).await;
    let sq = sqlite(&p, |t| Box::pin(vpc_scenario(t))).await.unwrap();
    let ob = object(&p, |t| Box::pin(vpc_scenario(t))).await.unwrap();
    assert_eq!(sq, ob);
    assert_eq!(sq.flags, vec![false, true]);
    assert_eq!(
        sq.first
            .iter()
            .map(|v| v.vpce_id.as_str())
            .collect::<Vec<_>>(),
        ["v1", "v2"]
    );
    assert_eq!(sq.first[1].created_at, 20, "upsert preserves created_at");
    assert_eq!(sq.first[1].updated_at, 2);
    assert_eq!(
        sq.second
            .iter()
            .map(|v| v.vpce_id.as_str())
            .collect::<Vec<_>>(),
        ["v3", "v4"]
    );
    let sq_store = p.db.control_store();
    let ob_store = p.real.control.as_ref();
    assert_eq!(
        sq_store.vpc_endpoints().await.unwrap(),
        ob_store.vpc_endpoints().await.unwrap()
    );
    assert_eq!(
        sq_store.vpc_endpoint("v4").await.unwrap(),
        ob_store.vpc_endpoint("v4").await.unwrap()
    );
    assert!(ob_store.vpc_endpoint("v3").await.unwrap().is_none());
}
