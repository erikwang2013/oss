//! inventory config 切片双后端差分：legacy raw SQL（测试内参考实现）vs
//! `SqliteControlStore` / `SqliteControlTxn` / `Db` 单机门面。

#[path = "support/control_store_rows.rs"]
mod helpers;

use helpers::*;
use oss_server::control_store::SqliteControlTxn;
use oss_server::db::Db;
use xl_store::control_store::inventory::{InventoryConfigEntry, InventoryConfigView, InventoryTxn};
use xl_store::control_store::{ControlError, OwnerRef};

fn entry(
    bucket: &str,
    id: &str,
    owner: OwnerRef,
    config_json: &str,
    updated_at: i64,
) -> InventoryConfigEntry {
    InventoryConfigEntry {
        bucket: bucket.into(),
        id: id.into(),
        owner,
        config_json: config_json.into(),
        updated_at,
    }
}

fn view(
    bucket: &str,
    id: &str,
    owner: Option<OwnerRef>,
    owner_uid: i64,
    config_json: &str,
    updated_at: i64,
) -> InventoryConfigView {
    InventoryConfigView {
        bucket: bucket.into(),
        id: id.into(),
        owner,
        owner_uid,
        config_json: config_json.into(),
        updated_at,
    }
}

async fn txn_upsert(db: &Db, row: &InventoryConfigEntry) {
    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    txn.inventory_config_upsert(row).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn inventory_config_runtime_view_maps_owner_four_ways() {
    let db = fresh().await;
    let root = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    raw_inventory_config(&db, "b1", "cfg-root", root, r#"{"v":1}"#, 1).await;
    raw_inventory_config(&db, "b1", "cfg-alice", alice, r#"{"v":2}"#, 2).await;
    raw_inventory_config(&db, "b1", "cfg-witness", 0, r#"{"v":3}"#, 3).await;
    raw_inventory_config(&db, "b1", "cfg-dangling", 999, r#"{"v":4}"#, 4).await;
    raw_inventory_config(&db, "b2", "cfg-b2", alice, r#"{"v":5}"#, 5).await;

    let store = db.control_store();
    assert_eq!(
        store
            .inventory_config("b1", "cfg-dangling")
            .await
            .unwrap()
            .unwrap(),
        view("b1", "cfg-dangling", None, 999, r#"{"v":4}"#, 4),
        "悬空属主：运行期 owner=None，owner_uid 保留原值"
    );
    assert_eq!(
        store.inventory_configs("b1").await.unwrap(),
        vec![
            // 管理员读成 User（运行期口径），owner_uid 仍是本地 id。
            view(
                "b1",
                "cfg-alice",
                Some(OwnerRef::User("alice".into())),
                alice,
                r#"{"v":2}"#,
                2
            ),
            view("b1", "cfg-dangling", None, 999, r#"{"v":4}"#, 4),
            view(
                "b1",
                "cfg-root",
                Some(OwnerRef::User("root".into())),
                root,
                r#"{"v":1}"#,
                1
            ),
            // owner_id == 0 → Admin（witness 哨兵）。
            view(
                "b1",
                "cfg-witness",
                Some(OwnerRef::Admin),
                0,
                r#"{"v":3}"#,
                3
            ),
        ],
        "ORDER BY id"
    );
    assert_eq!(store.inventory_config_count("b1").await.unwrap(), 4);
    assert_eq!(store.inventory_config_count("nope").await.unwrap(), 0);

    let all: Vec<(String, String)> = store
        .inventory_configs_all()
        .await
        .unwrap()
        .into_iter()
        .map(|v| (v.bucket, v.id))
        .collect();
    assert_eq!(
        all,
        vec![
            ("b1".into(), "cfg-alice".into()),
            ("b1".into(), "cfg-dangling".into()),
            ("b1".into(), "cfg-root".into()),
            ("b1".into(), "cfg-witness".into()),
            ("b2".into(), "cfg-b2".into()),
        ],
        "ORDER BY bucket, id"
    );
}

#[tokio::test]
async fn inventory_config_upsert_is_natural_key_and_clears_cursor_incremental() {
    let db = fresh().await;
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    raw_inventory_config(&db, "b1", "cfg", 0, r#"{"old":1}"#, 1).await;
    raw_inventory_config(&db, "b1", "other", 0, r#"{"keep":1}"#, 1).await;
    raw_inventory_config(&db, "b2", "cfg", 0, r#"{"other_bucket":1}"#, 1).await;
    raw_inventory_cursor(&db, "b1", "cfg", "c1", 1, 1).await;
    raw_inventory_incremental(&db, "b1", "cfg", true, 1).await;
    raw_inventory_cursor(&db, "b1", "other", "c2", 1, 1).await;
    raw_inventory_incremental(&db, "b1", "other", true, 1).await;

    txn_upsert(
        &db,
        &entry(
            "b1",
            "cfg",
            OwnerRef::User("alice".into()),
            r#"{"new":2}"#,
            9,
        ),
    )
    .await;

    assert_eq!(
        raw_inventory_config_dump(&db).await,
        vec![
            ("b1".into(), "cfg".into(), alice, r#"{"new":2}"#.into(), 9),
            (
                "b1".into(),
                "other".into(),
                0,
                r#"{"keep":1}"#.into(),
                1
            ),
            (
                "b2".into(),
                "cfg".into(),
                0,
                r#"{"other_bucket":1}"#.into(),
                1
            ),
        ],
        "自然键 (bucket,id) 原地更新；别的桶/配置不动"
    );
    assert_eq!(
        raw_inventory_cursor_dump(&db).await,
        vec![("b1".into(), "other".into(), "c2".into(), 1, 1)],
        "配置变更清掉自己的增量水位，不碰别的配置"
    );
    assert_eq!(
        raw_inventory_incremental_dump(&db).await,
        vec![("b1".into(), "other".into(), 1, 1)],
        "配置变更清掉自己的增量开关"
    );
}

#[tokio::test]
async fn inventory_config_delete_returns_false_for_missing_and_clears_children() {
    let db = fresh().await;
    raw_inventory_config(&db, "b1", "cfg", 0, "{}", 1).await;
    raw_inventory_config(&db, "b1", "other", 0, "{}", 1).await;
    raw_inventory_cursor(&db, "b1", "cfg", "c1", 1, 1).await;
    raw_inventory_incremental(&db, "b1", "cfg", true, 1).await;
    raw_inventory_cursor(&db, "b1", "other", "c2", 1, 1).await;
    raw_inventory_incremental(&db, "b1", "other", false, 1).await;

    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    assert!(
        !txn.inventory_config_delete("b1", "missing").await.unwrap(),
        "不存在的配置返回 false 且无副作用"
    );
    tx.commit().await.unwrap();

    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    assert!(txn.inventory_config_delete("b1", "cfg").await.unwrap());
    tx.commit().await.unwrap();

    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    assert!(
        !txn.inventory_config_delete("b1", "cfg").await.unwrap(),
        "重复删除返回 false"
    );
    tx.commit().await.unwrap();

    assert_eq!(
        raw_inventory_config_dump(&db).await,
        vec![("b1".into(), "other".into(), 0, "{}".into(), 1)],
    );
    assert_eq!(
        raw_inventory_cursor_dump(&db).await,
        vec![("b1".into(), "other".into(), "c2".into(), 1, 1)],
    );
    assert_eq!(
        raw_inventory_incremental_dump(&db).await,
        vec![("b1".into(), "other".into(), 0, 1)],
    );
}

#[tokio::test]
async fn inventory_config_export_is_strict_and_ordered() {
    let db = fresh().await;
    let root = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    raw_inventory_config(&db, "b2", "cfg2", 0, r#"{"b":2}"#, 2).await;
    raw_inventory_config(&db, "b1", "cfg-root", root, r#"{"b":1}"#, 1).await;
    raw_inventory_config(&db, "b1", "cfg-alice", alice, r#"{"a":1}"#, 3).await;
    raw_inventory_config(&db, "b1", "cfg-witness", 0, r#"{"w":1}"#, 4).await;
    raw_inventory_config(&db, "b1", "cfg-dangling", 999, r#"{"d":1}"#, 5).await;

    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    match txn.inventory_configs().await {
        Err(ControlError::Invalid(m)) => assert_eq!(m, "owner user id 999 missing"),
        other => panic!("悬空属主导出必须严格失败，得到 {other:?}"),
    }
    assert!(txn.inventory_config_delete("b1", "cfg-dangling").await.unwrap());
    assert_eq!(
        txn.inventory_configs().await.unwrap(),
        vec![
            entry(
                "b1",
                "cfg-alice",
                OwnerRef::User("alice".into()),
                r#"{"a":1}"#,
                3
            ),
            // is_admin=1 → Admin（快照导出口径，与运行期区分）。
            entry("b1", "cfg-root", OwnerRef::Admin, r#"{"b":1}"#, 1),
            // owner_id=0 → Admin。
            entry("b1", "cfg-witness", OwnerRef::Admin, r#"{"w":1}"#, 4),
            entry("b2", "cfg2", OwnerRef::Admin, r#"{"b":2}"#, 2),
        ],
        "ORDER BY bucket, id；严格导出映射"
    );
    tx.commit().await.unwrap();

    // 快照安装：整表替换（不看自然键冲突），保持入参顺序，不碰 cursor/incremental。
    raw_inventory_cursor(&db, "b1", "cfg-alice", "cur", 3, 3).await;
    raw_inventory_incremental(&db, "b1", "cfg-alice", true, 3).await;
    let rows = vec![
        entry("b2", "z", OwnerRef::Admin, r#"{"z":1}"#, 7),
        entry(
            "b1",
            "a",
            OwnerRef::User("alice".into()),
            r#"{"a":2}"#,
            8,
        ),
        entry("b1", "m", OwnerRef::Admin, r#"{"m":1}"#, 9),
    ];
    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    txn.replace_inventory_configs(&rows).await.unwrap();
    tx.commit().await.unwrap();

    let by_rowid: Vec<String> = sqlx::query_scalar("SELECT id FROM inventory_configs ORDER BY rowid")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(by_rowid, vec!["z", "a", "m"], "按入参顺序 INSERT");
    assert_eq!(
        raw_inventory_config_dump(&db).await,
        vec![
            ("b1".into(), "a".into(), alice, r#"{"a":2}"#.into(), 8),
            // 快照安装 Admin → 本地管理员 id（witness 才落 0）。
            ("b1".into(), "m".into(), root, r#"{"m":1}"#.into(), 9),
            ("b2".into(), "z".into(), root, r#"{"z":1}"#.into(), 7),
        ]
    );
    assert_eq!(
        raw_inventory_cursor_dump(&db).await,
        vec![("b1".into(), "cfg-alice".into(), "cur".into(), 3, 3)],
        "快照安装不碰增量游标"
    );
    assert_eq!(
        raw_inventory_incremental_dump(&db).await,
        vec![("b1".into(), "cfg-alice".into(), 1, 3)]
    );
}

#[tokio::test]
async fn inventory_apply_put_resolves_admin_and_user_natural_keys() {
    let db = fresh().await;
    let root = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();

    // apply `InventoryPut { owner: None }` → Admin（本地首个管理员 id）。
    txn_upsert(&db, &entry("b1", "cfg-admin", OwnerRef::Admin, "{}", 1)).await;
    // apply `InventoryPut { owner: Some("alice") }` → User 的本地 id。
    txn_upsert(
        &db,
        &entry("b1", "cfg-alice", OwnerRef::User("alice".into()), "{}", 2),
    )
    .await;
    assert_eq!(
        raw_inventory_config_dump(&db).await,
        vec![
            ("b1".into(), "cfg-admin".into(), root, "{}".into(), 1),
            ("b1".into(), "cfg-alice".into(), alice, "{}".into(), 2),
        ]
    );

    // 有序命令流里普通用户必已先建；缺失 = 不变量破坏，事务内失败。
    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    match txn
        .inventory_config_upsert(&entry(
            "b1",
            "cfg-ghost",
            OwnerRef::User("ghost".into()),
            "{}",
            3,
        ))
        .await
    {
        Err(ControlError::Invalid(m)) => assert_eq!(m, "owner user ghost not found"),
        other => panic!("缺失用户必须 fail-stop，得到 {other:?}"),
    }
    drop(tx);
    assert!(
        !raw_inventory_config_dump(&db)
            .await
            .iter()
            .any(|(_, id, ..)| id == "cfg-ghost"),
        "失败事务零写入"
    );

    // witness（尚无管理员）：Admin 映射哨兵 0。
    let witness = fresh().await;
    txn_upsert(
        &witness,
        &entry("b1", "cfg", OwnerRef::Admin, "{}", 1),
    )
    .await;
    assert_eq!(
        raw_inventory_config_dump(&witness).await,
        vec![("b1".into(), "cfg".into(), 0, "{}".into(), 1)]
    );

    // 单机门面保留数字 uid 签名（决策 7）：0 → Admin；存在的 uid → User；否则 RowNotFound。
    db.inventory_upsert_config("b1", "cfg-facade", alice, "{}", 5)
        .await
        .unwrap();
    db.inventory_upsert_config("b1", "cfg-zero", 0, "{}", 6)
        .await
        .unwrap();
    match db
        .inventory_upsert_config("b1", "cfg-missing", 123, "{}", 7)
        .await
    {
        Err(sqlx::Error::RowNotFound) => {}
        other => panic!("缺失 uid 必须 RowNotFound，得到 {other:?}"),
    }
    assert_eq!(
        raw_inventory_config_dump(&db)
            .await
            .into_iter()
            .filter(|(_, id, ..)| id.starts_with("cfg-"))
            .collect::<Vec<_>>(),
        vec![
            ("b1".into(), "cfg-admin".into(), root, "{}".into(), 1),
            ("b1".into(), "cfg-alice".into(), alice, "{}".into(), 2),
            ("b1".into(), "cfg-facade".into(), alice, "{}".into(), 5),
            // uid 0 → Admin → 现有管理员 id（无管理员时才是 0）。
            ("b1".into(), "cfg-zero".into(), root, "{}".into(), 6),
        ]
    );
    assert!(db.inventory_delete_config("b1", "cfg-zero").await.unwrap());
    assert!(!db.inventory_delete_config("b1", "cfg-zero").await.unwrap());
}
