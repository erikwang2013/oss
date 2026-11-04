//! lifecycle 切片双后端差分：legacy raw SQL（测试内参考实现）vs `SqliteControlStore` /
//! `SqliteControlTxn` / `Db` 单机门面。

#[path = "support/control_store_rows.rs"]
mod helpers;

use helpers::*;
use oss_server::raft::FencingEpoch;
use xl_store::control_store::lifecycle::{LifecycleCursorRow, LifecycleRuleEntry, LifecycleTxn};

fn rule(
    bucket: &str,
    prefix: &str,
    rule_id: Option<&str>,
    updated_at: i64,
) -> LifecycleRuleEntry {
    LifecycleRuleEntry {
        bucket: bucket.into(),
        prefix: prefix.into(),
        rule_id: rule_id.map(str::to_string),
        expire_days: 7,
        expiration_days: Some(3),
        transition_days: None,
        transition_class: None,
        transition_tier: None,
        enabled: true,
        updated_at,
        config_json: Some(format!(r#"{{"prefix":"{prefix}"}}"#)),
    }
}

#[tokio::test]
async fn lifecycle_reads_match_legacy_order_and_null_rule_ids() {
    let store_db = fresh().await;
    let ref_db = fresh().await;
    for db in [&store_db, &ref_db] {
        raw_rule(db, "b1", "z/", None, 5, None, true, 1).await;
        raw_rule(db, "b1", "a/", Some("r1"), 7, Some(3), true, 2).await;
        raw_rule(db, "b1", "b/", Some("r2"), 9, None, false, 3).await;
        raw_rule(db, "b2", "q/", Some("r0"), 1, None, true, 4).await;
        raw_cursor(db, "b1", "r1", "versions", "c1", 5, 6, 7).await;
    }

    let store = store_db.control_store();
    let got = store.lifecycle_rules("b1").await.unwrap();
    assert_eq!(got, legacy_rules(&ref_db, "b1").await);
    assert_eq!(
        got.iter()
            .map(|r| r.rule_id.as_deref())
            .collect::<Vec<_>>(),
        [None, Some("r1"), Some("r2")],
        "NULL rule_id 排最前，其余按 rule_id 升序"
    );
    assert_eq!(got[0].config_json, None);
    assert_eq!(got[1].config_json, None);

    let enabled = store.enabled_lifecycle_rules().await.unwrap();
    assert_eq!(enabled, legacy_enabled_rules(&ref_db).await);
    assert_eq!(
        enabled
            .iter()
            .map(|r| (r.bucket.as_str(), r.rule_id.as_deref()))
            .collect::<Vec<_>>(),
        [
            ("b1", None),
            ("b1", Some("r1")),
            ("b2", Some("r0")),
        ]
    );

    assert_eq!(
        store
            .lifecycle_cursor("b1", "r1", "versions")
            .await
            .unwrap(),
        Some("c1".to_string())
    );
    assert_eq!(
        store
            .lifecycle_cursor_row("b1", "r1", "versions")
            .await
            .unwrap(),
        legacy_cursor_row(&ref_db, "b1", "r1", "versions").await
    );
    assert_eq!(
        store.lifecycle_cursor("b1", "missing", "versions").await.unwrap(),
        None
    );
}

#[tokio::test]
async fn replace_lifecycle_rules_is_bucket_scoped_and_clears_cursors() {
    let store_db = fresh().await;
    let ref_db = fresh().await;
    for db in [&store_db, &ref_db] {
        raw_rule(db, "b1", "old/", Some("r-old"), 3, None, true, 1).await;
        raw_rule(db, "b2", "keep/", Some("r-keep"), 4, Some(2), true, 2).await;
        raw_cursor(db, "b1", "r-old", "versions", "c-old", 3, 4, 5).await;
        raw_cursor(db, "b2", "r-keep", "versions", "c-keep", 4, 5, 6).await;
    }

    let rows = vec![
        rule("b1", "new/", Some("r-new"), 42),
        rule("b1", "old/", Some("r-old"), 43),
    ];
    let mut tx = store_db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.replace_lifecycle_rules("b1", &rows).await.unwrap();
    }
    tx.commit().await.unwrap();
    raw_replace_lifecycle_rules(&ref_db, "b1", &rows).await;

    assert_eq!(
        raw_rule_dump(&store_db).await,
        raw_rule_dump(&ref_db).await,
        "与 legacy 逐行一致"
    );
    assert_eq!(raw_cursor_dump(&store_db).await, raw_cursor_dump(&ref_db).await);
    assert_eq!(
        store_db.control_store().lifecycle_rules("b1").await.unwrap(),
        rows,
        "行的 updated_at 原样写入（不是额外参数）"
    );
    assert!(
        raw_cursor_dump(&store_db)
            .await
            .iter()
            .all(|c| c.0 != "b1"),
        "b1 游标被清"
    );
    assert_eq!(
        store_db.control_store().lifecycle_rules("b2").await.unwrap(),
        legacy_rules(&ref_db, "b2").await
    );
    assert_eq!(
        raw_cursor_dump(&store_db)
            .await
            .iter()
            .filter(|c| c.0 == "b2")
            .count(),
        1,
        "其他桶游标不受影响"
    );
}

#[tokio::test]
async fn lifecycle_cursor_upsert_and_delete_are_fence_bounded() {
    let store_db = fresh().await;
    let ref_db = fresh().await;
    for db in [&store_db, &ref_db] {
        raw_cursor(db, "b", "r", "s", "old", 10, 5, 5).await;
    }

    let mut row = LifecycleCursorRow {
        bucket: "b".into(),
        rule_id: "r".into(),
        stage: "s".into(),
        cursor: "stale".into(),
        updated_at: 11,
        fence_term: 4,
        fence_leader: 9,
    };
    let mut tx = store_db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        assert!(
            !txn.lifecycle_cursor_upsert(&row).await.unwrap(),
            "term 4 < 5：no-op 且返回 false"
        );
    }
    tx.commit().await.unwrap();
    assert!(!raw_cursor_upsert(&ref_db, &row).await);
    assert_eq!(raw_cursor_dump(&store_db).await, raw_cursor_dump(&ref_db).await);
    assert_eq!(
        store_db
            .control_store()
            .lifecycle_cursor_row("b", "r", "s")
            .await
            .unwrap()
            .unwrap()
            .cursor,
        "old"
    );

    row.cursor = "accepted".into();
    row.updated_at = 12;
    row.fence_term = 5;
    let mut tx = store_db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        assert!(txn.lifecycle_cursor_upsert(&row).await.unwrap(), "term 5 接受");
    }
    tx.commit().await.unwrap();
    assert!(raw_cursor_upsert(&ref_db, &row).await);
    assert_eq!(raw_cursor_dump(&store_db).await, raw_cursor_dump(&ref_db).await);

    let mut tx = store_db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.lifecycle_cursor_delete("b", "r", "s", 4).await.unwrap();
    }
    tx.commit().await.unwrap();
    raw_cursor_delete(&ref_db, "b", "r", "s", 4).await;
    assert_eq!(
        raw_cursor_dump(&store_db).await,
        raw_cursor_dump(&ref_db).await,
        "旧 term 删除 no-op"
    );

    let mut tx = store_db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.lifecycle_cursor_delete("b", "r", "s", 5).await.unwrap();
    }
    tx.commit().await.unwrap();
    raw_cursor_delete(&ref_db, "b", "r", "s", 5).await;
    assert_eq!(raw_cursor_dump(&store_db).await, raw_cursor_dump(&ref_db).await);
    assert!(raw_cursor_dump(&store_db).await.is_empty());
}

#[tokio::test]
async fn db_lifecycle_facades_preserve_signatures_and_now() {
    let db = fresh().await;
    db.set_lifecycle_rules(
        "b",
        &[
            rule("ignored", "z/", Some("r2"), 1),
            rule("ignored", "a/", Some("r1"), 2),
            rule("ignored", "m/", None, 3),
        ],
        99,
    )
    .await
    .unwrap();

    let got = db.list_lifecycle_rules("b").await.unwrap();
    assert_eq!(
        got.iter()
            .map(|r| (r.rule_id.as_deref(), r.prefix.as_str(), r.updated_at))
            .collect::<Vec<_>>(),
        [
            (None, "m/", 99),
            (Some("r1"), "a/", 99),
            (Some("r2"), "z/", 99),
        ],
        "now 覆盖每行 updated_at；排序按 (rule_id, prefix)"
    );
    assert_eq!(
        db.enabled_lifecycle_rules().await.unwrap().len(),
        3,
        "未显式停止的规则默认启用"
    );

    db.lifecycle_cursor_set(
        "b",
        "r1",
        "versions",
        Some("c1"),
        100,
        FencingEpoch { term: 5, leader: 1 },
    )
    .await
    .unwrap();
    assert_eq!(
        db.lifecycle_cursor_get("b", "r1", "versions").await.unwrap(),
        Some("c1".into())
    );
    db.lifecycle_cursor_set(
        "b",
        "r1",
        "versions",
        Some("stale"),
        101,
        FencingEpoch { term: 4, leader: 1 },
    )
    .await
    .unwrap();
    assert_eq!(
        db.lifecycle_cursor_get("b", "r1", "versions").await.unwrap(),
        Some("c1".into())
    );

    db.lifecycle_cursor_set(
        "b",
        "r1",
        "versions",
        None,
        102,
        FencingEpoch { term: 5, leader: 1 },
    )
    .await
    .unwrap();
    assert_eq!(db.lifecycle_cursor_get("b", "r1", "versions").await.unwrap(), None);

    // 清规则同时清游标（PUT 空列表语义）。
    db.set_lifecycle_rules("b", &[], 103).await.unwrap();
    assert!(db.list_lifecycle_rules("b").await.unwrap().is_empty());
}
