//! `access_points` / `olap_configs` 切片双后端差分：legacy raw SQL（测试内参考实现）
//! vs `SqliteControlStore` / `SqliteControlTxn`。两个 fresh SQLite 库跑同一操作序列，
//! 比较完整行、排序、快照 JSON 字节、安装结果、级联、回滚与后端错误。

#[path = "support/ap_olap_diff.rs"]
mod helpers;

use helpers::*;
use oss_server::raft::data::{AccessPointRow, OlapConfigRow};
use xl_store::control_store::{
    AccessPointEntry, ControlError, ControlTxn, OlapConfigEntry, OwnerRef,
};

#[tokio::test]
async fn runtime_reads_match_legacy_uid_semantics() {
    let store_db = fresh().await;
    let ref_db = fresh().await;
    for db in [&store_db, &ref_db] {
        let root = db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        raw_ap(db, "ap-admin", "b1", root, None, 1).await;
        raw_ap(db, "ap-user", "b1", alice, Some("{}"), 2).await;
        raw_ap(db, "ap-zero", "b1", 0, None, 3).await;
        raw_ap(db, "ap-dangling", "b1", 999, None, 4).await;
        raw_olap(
            db,
            "ap-user",
            "http",
            Some("https://x"),
            Some(&[0x00, 0xff, 0x80]),
            Some("ab"),
            5000,
            1024,
            5,
        )
        .await;
        raw_olap(db, "ap-dangling", "wasm", None, None, None, 6000, 2048, 6).await;
    }

    let got = store_db.control_store().access_points().await.unwrap();
    assert_eq!(got, legacy_runtime_aps(&ref_db).await, "全字段与排序一致");
    assert_eq!(
        got.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(),
        ["ap-admin", "ap-dangling", "ap-user", "ap-zero"]
    );
    assert_eq!(
        got[0].owner,
        OwnerRef::User("root".into()),
        "管理员也读 User"
    );
    assert_eq!(got[1].owner, OwnerRef::Admin, "悬空属主宽松为 Admin");
    assert_eq!(got[2].owner, OwnerRef::User("alice".into()));
    assert_eq!(got[3].owner, OwnerRef::Admin, "owner_id=0 为 Admin");
    let one = store_db
        .control_store()
        .access_point("ap-user")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(one.owner, OwnerRef::User("alice".into()));

    let olap = store_db.control_store().olap_configs().await.unwrap();
    assert_eq!(olap, legacy_olap_configs(&ref_db).await);
    assert_eq!(
        olap.iter()
            .map(|o| o.access_point.as_str())
            .collect::<Vec<_>>(),
        ["ap-dangling", "ap-user"]
    );
    assert_eq!(olap[1].module.as_deref(), Some(&[0x00, 0xff, 0x80][..]));
    assert_eq!(
        store_db
            .control_store()
            .olap_config("ap-user")
            .await
            .unwrap()
            .unwrap()
            .module,
        Some(vec![0x00, 0xff, 0x80])
    );
}

#[tokio::test]
async fn snapshot_export_and_install_match_legacy() {
    // 导出：严格口径（is_admin=1 → Admin，悬空 → Invalid）。
    let db = fresh().await;
    let root = db.create_user("root", "pw", true).await.unwrap();
    let alice = db.create_user("alice", "pw", false).await.unwrap();
    raw_ap(&db, "ap-admin", "b1", root, None, 1).await;
    raw_ap(&db, "ap-alice", "b1", alice, Some("{}"), 2).await;
    raw_ap(&db, "ap-zero", "b1", 0, None, 3).await;
    let mut tx = db.pool().begin().await.unwrap();
    let exports = {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.access_points().await.unwrap()
    };
    tx.rollback().await.unwrap();
    assert_eq!(exports, legacy_export_aps(&db).await, "导出全字段一致");
    assert_eq!(exports[0].owner, OwnerRef::Admin, "is_admin=1 → Admin");
    assert_eq!(exports[1].owner, OwnerRef::User("alice".into()));
    assert_eq!(exports[2].owner, OwnerRef::Admin);
    assert_eq!(
        serde_json::to_vec(&exports).unwrap(),
        legacy_ap_json(&exports),
        "快照 JSON 字节一致（字段顺序冻结）"
    );

    raw_ap(&db, "ap-dangling", "b1", 999, None, 4).await;
    let mut tx = db.pool().begin().await.unwrap();
    let err = {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.access_points().await.unwrap_err()
    };
    tx.rollback().await.unwrap();
    assert!(
        matches!(&err, ControlError::Invalid(m) if m.contains("owner user id 999 missing")),
        "{err}"
    );

    // 安装：legacy raw INSERT vs txn replace，比较全表（含 module BLOB）与严格读输出。
    let legacy = fresh().await;
    let store_db = fresh().await;
    let mut roots = Vec::new();
    let mut alices = Vec::new();
    for db in [&legacy, &store_db] {
        alices.push(db.create_user("alice", "pw", false).await.unwrap());
        roots.push(db.create_user("root", "pw", true).await.unwrap());
    }
    let aps = vec![
        AccessPointEntry {
            name: "ap-admin".into(),
            bucket: "b1".into(),
            owner: OwnerRef::Admin,
            policy_json: None,
            created_at: 1,
            updated_at: 1,
        },
        AccessPointEntry {
            name: "ap-user".into(),
            bucket: "b1".into(),
            owner: OwnerRef::User("alice".into()),
            policy_json: Some("{}".into()),
            created_at: 2,
            updated_at: 2,
        },
    ];
    let olaps = vec![
        OlapConfigEntry {
            access_point: "ap-admin".into(),
            kind: "wasm".into(),
            endpoint: None,
            module: Some(vec![0x00, 0xff, 0x80, 0xfe]),
            module_sha256: Some("ab".into()),
            timeout_ms: 5000,
            max_response_bytes: 1024,
            updated_at: 3,
        },
        OlapConfigEntry {
            access_point: "ap-user".into(),
            kind: "http".into(),
            endpoint: Some("https://x".into()),
            module: None,
            module_sha256: None,
            timeout_ms: 6000,
            max_response_bytes: 2048,
            updated_at: 4,
        },
    ];
    raw_ap(&legacy, "ap-admin", "b1", roots[0], None, 1).await;
    raw_ap(&legacy, "ap-user", "b1", alices[0], Some("{}"), 2).await;
    raw_olap(
        &legacy,
        "ap-admin",
        "wasm",
        None,
        Some(&[0x00, 0xff, 0x80, 0xfe]),
        Some("ab"),
        5000,
        1024,
        3,
    )
    .await;
    raw_olap(
        &legacy,
        "ap-user",
        "http",
        Some("https://x"),
        None,
        None,
        6000,
        2048,
        4,
    )
    .await;
    let mut tx = store_db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.replace_access_points(&aps).await.unwrap();
        txn.replace_olap_configs(&olaps).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(
        raw_aps(&store_db).await,
        raw_aps(&legacy).await,
        "AP 全表一致"
    );
    assert_eq!(
        raw_olaps(&store_db).await,
        raw_olaps(&legacy).await,
        "OLAP 全表一致（含 module BLOB）"
    );
    let mut tx = store_db.pool().begin().await.unwrap();
    let (round_aps, round_olaps) = {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        (
            txn.access_points().await.unwrap(),
            txn.olap_configs().await.unwrap(),
        )
    };
    tx.rollback().await.unwrap();
    assert_eq!(round_aps, aps, "安装后严格导出往返一致");
    assert_eq!(round_olaps, olaps);
}

#[tokio::test]
async fn bucket_cascade_matches_legacy() {
    let legacy = fresh().await;
    let store_db = fresh().await;
    for db in [&legacy, &store_db] {
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        raw_ap(db, "ap-a", "b1", alice, None, 1).await;
        raw_ap(db, "ap-b", "b2", alice, None, 2).await;
        raw_olap(
            db,
            "ap-a",
            "http",
            Some("https://x"),
            None,
            None,
            5000,
            1024,
            3,
        )
        .await;
        raw_olap(
            db,
            "ap-b",
            "http",
            Some("https://y"),
            None,
            None,
            5000,
            1024,
            4,
        )
        .await;
    }
    sqlx::query(
        "DELETE FROM olap_configs WHERE access_point IN \
         (SELECT name FROM access_points WHERE bucket = ?)",
    )
    .bind("b1")
    .execute(legacy.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM access_points WHERE bucket = ?")
        .bind("b1")
        .execute(legacy.pool())
        .await
        .unwrap();
    let mut tx = store_db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.delete_access_points_for_bucket("b1").await.unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(
        raw_aps(&store_db).await,
        raw_aps(&legacy).await,
        "只删 b1 的 AP"
    );
    assert_eq!(raw_olaps(&store_db).await, raw_olaps(&legacy).await);
    let left = store_db.control_store().access_points().await.unwrap();
    assert_eq!(
        left.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(),
        ["ap-b"]
    );
}

#[tokio::test]
async fn txn_rollback_leaves_tables_empty() {
    let db = fresh().await;
    db.create_user("alice", "pw", false).await.unwrap();
    let mut tx = db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        assert!(
            txn.access_point_create(&AccessPointEntry {
                name: "ap1".into(),
                bucket: "b1".into(),
                owner: OwnerRef::User("alice".into()),
                policy_json: None,
                created_at: 1,
                updated_at: 2,
            })
            .await
            .unwrap()
        );
        txn.olap_config_upsert(&OlapConfigEntry {
            access_point: "ap1".into(),
            kind: "http".into(),
            endpoint: Some("https://x".into()),
            module: None,
            module_sha256: None,
            timeout_ms: 5000,
            max_response_bytes: 1024,
            updated_at: 3,
        })
        .await
        .unwrap();
    }
    tx.rollback().await.unwrap();
    assert!(raw_aps(&db).await.is_empty(), "适配器不得自行 commit");
    assert!(raw_olaps(&db).await.is_empty());
    assert!(db.control_store().access_points().await.unwrap().is_empty());
    assert!(db.control_store().olap_configs().await.unwrap().is_empty());
}

#[tokio::test]
async fn backend_errors_propagate() {
    let db = fresh().await;
    db.pool().close().await;
    let err = db.control_store().access_points().await.unwrap_err();
    assert!(matches!(&err, ControlError::Backend(_)), "{err}");
    assert!(
        err.to_string().starts_with("control store backend: "),
        "{err}"
    );
    assert!(db.control_store().access_point("ap-a").await.is_err());
    assert!(db.control_store().olap_configs().await.is_err());
    assert!(db.control_store().olap_config("ap-a").await.is_err());
}

#[test]
fn ap_olap_snapshot_bytes_are_frozen() {
    let ap = AccessPointRow {
        name: "ap1".into(),
        bucket: "b1".into(),
        owner: OwnerRef::User("alice".into()),
        policy_json: None,
        created_at: 1,
        updated_at: 2,
    };
    assert_eq!(
        serde_json::to_string(&ap).unwrap(),
        r#"{"name":"ap1","bucket":"b1","owner":{"kind":"user","username":"alice"},"policy_json":null,"created_at":1,"updated_at":2}"#
    );
    let legacy = LegacyApRow {
        name: ap.name.clone(),
        bucket: ap.bucket.clone(),
        owner: ap.owner.clone(),
        policy_json: ap.policy_json.clone(),
        created_at: ap.created_at,
        updated_at: ap.updated_at,
    };
    assert_eq!(
        serde_json::to_vec(&ap).unwrap(),
        serde_json::to_vec(&legacy).unwrap()
    );
    assert_eq!(
        serde_json::to_string(&AccessPointRow {
            owner: OwnerRef::Admin,
            ..ap.clone()
        })
        .unwrap(),
        r#"{"name":"ap1","bucket":"b1","owner":{"kind":"admin"},"policy_json":null,"created_at":1,"updated_at":2}"#
    );
    let frozen = r#"{"name":"ap9","bucket":"b9","owner":{"kind":"user","username":"bob"},"policy_json":"{}","created_at":3,"updated_at":4}"#;
    assert_eq!(
        serde_json::from_str::<AccessPointRow>(frozen).unwrap(),
        AccessPointRow {
            name: "ap9".into(),
            bucket: "b9".into(),
            owner: OwnerRef::User("bob".into()),
            policy_json: Some("{}".into()),
            created_at: 3,
            updated_at: 4,
        }
    );
    assert!(
        serde_json::from_str::<AccessPointRow>(
            r#"{"name":"x","bucket":"b","owner":{"kind":"admin"},"policy_json":null,"created_at":1,"updated_at":2,"extra":1}"#
        )
        .is_err(),
        "deny_unknown_fields"
    );

    let olap = OlapConfigRow {
        access_point: "ap1".into(),
        kind: "wasm".into(),
        endpoint: None,
        module_b64: Some("aGVsbG8=".into()),
        module_sha256: Some("ab".into()),
        timeout_ms: 5000,
        max_response_bytes: 1024,
        updated_at: 2,
    };
    assert_eq!(
        serde_json::to_string(&olap).unwrap(),
        r#"{"access_point":"ap1","kind":"wasm","endpoint":null,"module_b64":"aGVsbG8=","module_sha256":"ab","timeout_ms":5000,"max_response_bytes":1024,"updated_at":2}"#
    );
    assert_eq!(
        serde_json::from_str::<OlapConfigRow>(&serde_json::to_string(&olap).unwrap()).unwrap(),
        olap
    );
}
