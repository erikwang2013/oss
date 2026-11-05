//! Lifecycle parity: natural keys, export ordering and cursor fence gates.

use xl_store::control_store::lifecycle::{
    LifecycleCursorRow, LifecycleRuleEntry, LifecycleStore, LifecycleTxn,
};
use xl_store::error::Error as StoreError;

use super::parity_util::{err, object, parity, sqlite};

fn rule(
    bucket: &str,
    rule_id: Option<&str>,
    prefix: &str,
    enabled: bool,
    updated_at: i64,
) -> LifecycleRuleEntry {
    LifecycleRuleEntry {
        bucket: bucket.into(),
        prefix: prefix.into(),
        rule_id: rule_id.map(Into::into),
        expire_days: 30,
        expiration_days: None,
        transition_days: None,
        transition_class: None,
        transition_tier: None,
        enabled,
        updated_at,
        config_json: None,
    }
}

fn lcursor(bucket: &str, rule_id: &str, cursor: &str, fence: i64) -> LifecycleCursorRow {
    LifecycleCursorRow {
        bucket: bucket.into(),
        rule_id: rule_id.into(),
        stage: "current".into(),
        cursor: cursor.into(),
        updated_at: fence,
        fence_term: fence,
        fence_leader: 1,
    }
}

#[derive(Debug, PartialEq)]
struct LifeOut {
    flags: Vec<bool>,
    all: Vec<LifecycleRuleEntry>,
    cursors: Vec<LifecycleCursorRow>,
    cursor_value: Option<String>,
}

async fn lifecycle_scenario<T: LifecycleTxn>(txn: &mut T) -> Result<LifeOut, StoreError> {
    txn.replace_lifecycle_rules("b2", &[rule("b2", Some("r1"), "", true, 1)])
        .await
        .map_err(err)?;
    txn.replace_lifecycle_rules(
        "b1",
        &[
            rule("ignored", Some("r2"), "logs/", false, 2),
            rule("ignored", None, "", true, 3),
            rule("ignored", Some("r1"), "b", false, 5),
        ],
    )
    .await
    .map_err(err)?;
    let mut flags = Vec::new();
    flags.push(
        txn.lifecycle_cursor_upsert(&lcursor("b1", "r1", "c1", 1))
            .await
            .map_err(err)?,
    );
    txn.lifecycle_cursor_upsert(&lcursor("b1", "r2", "c2", 1))
        .await
        .map_err(err)?;
    // A lower fence is a zero-write; an equal fence still writes.
    flags.push(
        txn.lifecycle_cursor_upsert(&lcursor("b1", "r1", "stale", 0))
            .await
            .map_err(err)?,
    );
    flags.push(
        txn.lifecycle_cursor_upsert(&lcursor("b1", "r1", "c1b", 1))
            .await
            .map_err(err)?,
    );
    // Delete is gated too: r2 has fence 1, so 0 leaves it and 5 removes it.
    txn.lifecycle_cursor_delete("b1", "r2", "current", 0)
        .await
        .map_err(err)?;
    txn.lifecycle_cursor_delete("b1", "r2", "current", 5)
        .await
        .map_err(err)?;
    Ok(LifeOut {
        flags,
        all: txn.all_lifecycle_rules().await.map_err(err)?,
        cursors: txn.lifecycle_cursors().await.map_err(err)?,
        cursor_value: txn
            .lifecycle_cursor("b1", "r1", "current")
            .await
            .map_err(err)?
            .map(|row| row.cursor),
    })
}

#[tokio::test]
async fn lifecycle_rules_and_cursors_match() {
    let p = parity(&[("alice", false)]).await;
    let sq = sqlite(&p, |t| Box::pin(lifecycle_scenario(t)))
        .await
        .unwrap();
    let ob = object(&p, |t| Box::pin(lifecycle_scenario(t)))
        .await
        .unwrap();
    assert_eq!(sq, ob);
    assert_eq!(sq.flags, vec![true, false, true]);
    assert_eq!(
        sq.all.iter().map(|r| r.bucket.as_str()).collect::<Vec<_>>(),
        ["b1", "b1", "b1", "b2"],
        "replace stores the caller bucket"
    );
    assert_eq!(
        sq.all[..3]
            .iter()
            .map(|r| (r.rule_id.as_deref(), r.prefix.as_str()))
            .collect::<Vec<_>>(),
        [(None, ""), (Some("r1"), "b"), (Some("r2"), "logs/")]
    );
    assert_eq!(sq.cursors.len(), 1);
    assert_eq!(sq.cursors[0].cursor, "c1b");
    assert_eq!(sq.cursor_value.as_deref(), Some("c1b"));

    let sq_store = p.db.control_store();
    let ob_store = p.real.control.as_ref();
    assert_eq!(
        sq_store.lifecycle_rules("b1").await.unwrap(),
        ob_store.lifecycle_rules("b1").await.unwrap()
    );
    assert_eq!(
        sq_store.enabled_lifecycle_rules().await.unwrap(),
        ob_store.enabled_lifecycle_rules().await.unwrap()
    );
    assert_eq!(
        sq_store
            .lifecycle_cursor("b1", "r1", "current")
            .await
            .unwrap(),
        ob_store
            .lifecycle_cursor("b1", "r1", "current")
            .await
            .unwrap()
    );
    assert_eq!(
        sq_store
            .lifecycle_cursor_row("b1", "r1", "current")
            .await
            .unwrap(),
        ob_store
            .lifecycle_cursor_row("b1", "r1", "current")
            .await
            .unwrap()
    );
}

#[derive(Debug, PartialEq)]
struct LifeReplaceOut {
    after_all: Vec<LifecycleRuleEntry>,
    after_clear: Vec<LifecycleRuleEntry>,
    after_rules_only: Vec<LifecycleRuleEntry>,
    cursors: Vec<LifecycleCursorRow>,
}

async fn lifecycle_replace_scenario<T: LifecycleTxn>(
    txn: &mut T,
) -> Result<LifeReplaceOut, StoreError> {
    txn.replace_lifecycle_rules("b1", &[rule("b1", Some("r1"), "", true, 1)])
        .await
        .map_err(err)?;
    txn.lifecycle_cursor_upsert(&lcursor("b1", "r1", "c", 1))
        .await
        .map_err(err)?;
    txn.replace_lifecycle_cursors(&[lcursor("b2", "r9", "other", 2)])
        .await
        .map_err(err)?;
    let cursors = txn.lifecycle_cursors().await.map_err(err)?;
    txn.replace_all_lifecycle_rules(&[
        rule("b3", Some("r3"), "p", true, 3),
        rule("b4", None, "", false, 4),
    ])
    .await
    .map_err(err)?;
    let after_all = txn.all_lifecycle_rules().await.map_err(err)?;
    txn.clear_lifecycle_rules("b1").await.map_err(err)?;
    let after_clear = txn.all_lifecycle_rules().await.map_err(err)?;
    txn.delete_lifecycle_rules_only("b3").await.map_err(err)?;
    let after_rules_only = txn.all_lifecycle_rules().await.map_err(err)?;
    Ok(LifeReplaceOut {
        after_all,
        after_clear,
        after_rules_only,
        cursors,
    })
}

#[tokio::test]
async fn lifecycle_replace_paths_match() {
    let p = parity(&[("alice", false)]).await;
    let sq = sqlite(&p, |t| Box::pin(lifecycle_replace_scenario(t)))
        .await
        .unwrap();
    let ob = object(&p, |t| Box::pin(lifecycle_replace_scenario(t)))
        .await
        .unwrap();
    assert_eq!(sq, ob);
    assert_eq!(sq.after_all.len(), 2);
    assert_eq!(sq.after_clear.len(), 2, "b1 rules were already replaced");
    assert_eq!(sq.after_rules_only.len(), 1);
    assert_eq!(sq.after_rules_only[0].bucket, "b4");
    assert_eq!(sq.cursors.len(), 1);
    assert_eq!(sq.cursors[0].bucket, "b2");
}
