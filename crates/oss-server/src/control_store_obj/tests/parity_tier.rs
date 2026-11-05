//! Tier-restore parity: fence-gated upsert/delete/state transitions, preserved
//! immutable fields and `created_at` ordering of state scans.

use xl_store::control_store::tier::{TierRestoreRow, TierStore, TierTxn};
use xl_store::error::Error as StoreError;

use super::parity_util::{err, object, parity, sqlite};

fn row(
    bucket: &str,
    key: &str,
    version_id: &str,
    state: &str,
    created_at: i64,
    fence_term: i64,
) -> TierRestoreRow {
    TierRestoreRow {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version_id.into(),
        expires_at: 0,
        job_id: None,
        state: state.into(),
        created_at,
        fence_term,
        fence_leader: 1,
    }
}

#[derive(Debug, PartialEq)]
struct TierOut {
    flags: Vec<bool>,
    after: Vec<TierRestoreRow>,
    replaced: Vec<TierRestoreRow>,
}

async fn tier_scenario<T: TierTxn>(txn: &mut T) -> Result<TierOut, StoreError> {
    let mut flags = Vec::new();
    let mut first = row("b1", "k1", "v1", "pending", 5, 1);
    first.expires_at = 9;
    first.job_id = Some(3);
    flags.push(txn.tier_restore_upsert(&first).await.map_err(err)?);
    flags.push(
        txn.tier_restore_upsert(&row("b1", "k1", "v1", "stale", 1, 0))
            .await
            .map_err(err)?,
    );
    let mut second = row("b1", "k1", "v1", "restored", 6, 2);
    second.expires_at = 10;
    second.job_id = Some(4);
    flags.push(txn.tier_restore_upsert(&second).await.map_err(err)?);
    txn.tier_restore_set_state("b1", "k1", "v1", "expired", 20, 3, 3)
        .await
        .map_err(err)?;
    // Stale state transition is a zero write.
    txn.tier_restore_set_state("b1", "k1", "v1", "ignored", 30, 2, 2)
        .await
        .map_err(err)?;
    // Stale delete is a zero write (row fence is 3).
    txn.tier_restore_delete("b1", "k1", "v1", 2)
        .await
        .map_err(err)?;

    flags.push(
        txn.tier_restore_upsert(&row("b1", "k2", "v1", "pending", 3, 1))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.tier_restore_upsert(&row("b1", "k2", "v2", "pending", 3, 1))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.tier_restore_upsert(&row("b2", "a", "v1", "pending", 4, 1))
            .await
            .map_err(err)?,
    );
    txn.tier_restore_delete("b1", "k2", "v1", 1)
        .await
        .map_err(err)?;
    txn.tier_restore_delete("b1", "k2", "v1", 1)
        .await
        .map_err(err)?;
    let after = txn.tier_restores().await.map_err(err)?;

    txn.replace_tier_restores(&[
        row("b3", "k", "v1", "pending", 7, 1),
        row("b1", "k", "v1", "pending", 4, 1),
        row("b1", "k", "v2", "restored", 5, 1),
    ])
    .await
    .map_err(err)?;
    let replaced = txn.tier_restores().await.map_err(err)?;
    Ok(TierOut {
        flags,
        after,
        replaced,
    })
}

#[tokio::test]
async fn tier_restore_fences_and_order_match() {
    let p = parity(&[("alice", false)]).await;
    let sq = sqlite(&p, |t| Box::pin(tier_scenario(t))).await.unwrap();
    let ob = object(&p, |t| Box::pin(tier_scenario(t))).await.unwrap();
    assert_eq!(sq, ob);
    assert_eq!(sq.flags, vec![true, false, true, true, true, true]);
    assert_eq!(
        sq.after.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
        ["k1", "k2", "a"]
    );
    let k1 = &sq.after[0];
    assert_eq!(k1.state, "expired");
    assert_eq!(k1.expires_at, 20);
    assert_eq!(k1.job_id, Some(4), "set_state preserves job_id");
    assert_eq!(k1.created_at, 6, "set_state preserves created_at");
    assert_eq!(
        sq.after[1].version_id, "v2",
        "wrong-version delete leaves k2/v1 gone and v2 in place"
    );
    assert_eq!(
        sq.replaced
            .iter()
            .map(|r| (r.bucket.as_str(), r.state.as_str()))
            .collect::<Vec<_>>(),
        [("b1", "pending"), ("b1", "restored"), ("b3", "pending")]
    );

    let sq_store = p.db.control_store();
    let ob_store = p.real.control.as_ref();
    assert_eq!(
        sq_store.tier_restore("b1", "k", "v1").await.unwrap(),
        ob_store.tier_restore("b1", "k", "v1").await.unwrap()
    );
    assert_eq!(
        sq_store.tier_restores("pending").await.unwrap(),
        ob_store.tier_restores("pending").await.unwrap()
    );
    let pending = ob_store.tier_restores("pending").await.unwrap();
    assert_eq!(
        pending.iter().map(|r| r.created_at).collect::<Vec<_>>(),
        [4, 7],
        "state scan is created_at ascending"
    );
}
