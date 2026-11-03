//! `vpc_endpoints` 切片双后端差分：legacy raw SQL（测试内参考实现，生产方法已删除）
//! vs `SqliteControlStore` / `SqliteControlTxn`。两个 fresh SQLite 库跑同一操作
//! 序列，比较完整行、排序、幂等、回滚与快照 JSON 字节。

use std::sync::Arc;

use oss_server::db::Db;
use oss_server::raft::data::VpcEndpointRow;
use sqlx::Row;
use xl_store::control_store::ControlTxn;

async fn fresh() -> Arc<Db> {
    Arc::new(Db::connect("sqlite::memory:").await.unwrap())
}

fn row(vpce: &str, vpc: &str, state: &str, created: i64, updated: i64) -> VpcEndpointRow {
    VpcEndpointRow {
        vpce_id: vpce.into(),
        vpc_id: vpc.into(),
        account_id: "000000000001".into(),
        state: state.into(),
        created_at: created,
        updated_at: updated,
    }
}

fn from_row(r: &sqlx::sqlite::SqliteRow) -> VpcEndpointRow {
    VpcEndpointRow {
        vpce_id: r.get("vpce_id"),
        vpc_id: r.get("vpc_id"),
        account_id: r.get("account_id"),
        state: r.get("state"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

/// 旧 `Db::vpc_endpoint_get` 的 raw SQL 行为。
async fn legacy_get(db: &Db, vpce_id: &str) -> Option<VpcEndpointRow> {
    sqlx::query(
        "SELECT vpce_id, vpc_id, account_id, state, created_at, updated_at \
         FROM vpc_endpoints WHERE vpce_id = ?",
    )
    .bind(vpce_id)
    .fetch_optional(db.pool())
    .await
    .unwrap()
    .as_ref()
    .map(from_row)
}

/// 旧 `Db::vpc_endpoint_list` 的 raw SQL 行为。
async fn legacy_list(db: &Db) -> Vec<VpcEndpointRow> {
    sqlx::query(
        "SELECT vpce_id, vpc_id, account_id, state, created_at, updated_at \
         FROM vpc_endpoints ORDER BY vpce_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(from_row)
    .collect()
}

/// 旧 apply 路径的 upsert SQL（更新保留 created_at）。
async fn legacy_upsert(db: &Db, row: &VpcEndpointRow) {
    sqlx::query(
        "INSERT INTO vpc_endpoints \
         (vpce_id, vpc_id, account_id, state, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?) \
         ON CONFLICT(vpce_id) DO UPDATE SET vpc_id = excluded.vpc_id, \
           account_id = excluded.account_id, state = excluded.state, \
           updated_at = excluded.updated_at",
    )
    .bind(&row.vpce_id)
    .bind(&row.vpc_id)
    .bind(&row.account_id)
    .bind(&row.state)
    .bind(row.created_at)
    .bind(row.updated_at)
    .execute(db.pool())
    .await
    .unwrap();
}

async fn adapter_upsert(db: &Db, rows: &[VpcEndpointRow]) {
    let mut tx = db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        for row in rows {
            txn.vpc_endpoint_upsert(row).await.unwrap();
        }
    }
    tx.commit().await.unwrap();
}

/// 旧快照布局的独立参考编码：字段顺序不依赖生产 `Serialize` 实现。
#[derive(serde::Serialize)]
struct LegacyVpcRow {
    vpce_id: String,
    vpc_id: String,
    account_id: String,
    state: String,
    created_at: i64,
    updated_at: i64,
}

fn legacy_snapshot_json(rows: &[VpcEndpointRow]) -> Vec<u8> {
    let legacy: Vec<LegacyVpcRow> = rows
        .iter()
        .map(|r| LegacyVpcRow {
            vpce_id: r.vpce_id.clone(),
            vpc_id: r.vpc_id.clone(),
            account_id: r.account_id.clone(),
            state: r.state.clone(),
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
        .collect();
    serde_json::to_vec(&legacy).unwrap()
}

#[tokio::test]
async fn upsert_delete_and_ordering_match_legacy() {
    let legacy = fresh().await;
    let store_db = fresh().await;

    // 乱序写入；重复 upsert 只改 created_at（必须保留原值）。
    let first = row("vpce-b", "vpc-b", "disabled", 20, 21);
    let second = row("vpce-a", "vpc-a", "active", 10, 11);
    let mut reupsert = first.clone();
    reupsert.created_at = 999;
    for r in [&first, &second, &reupsert] {
        legacy_upsert(&legacy, r).await;
    }
    adapter_upsert(
        &store_db,
        &[first.clone(), second.clone(), reupsert.clone()],
    )
    .await;

    let legacy_rows = legacy_list(&legacy).await;
    let store = store_db.control_store();
    let store_rows = store.vpc_endpoints().await.unwrap();
    assert_eq!(store_rows, legacy_rows, "完整行与升序一致");
    assert_eq!(
        store_rows
            .iter()
            .map(|r| r.vpce_id.as_str())
            .collect::<Vec<_>>(),
        ["vpce-a", "vpce-b"]
    );
    assert_eq!(store_rows[1].created_at, 20, "重复 upsert 保留 created_at");
    assert_eq!(legacy_rows[1].created_at, 20, "legacy 同样保留 created_at");
    assert_eq!(
        store.vpc_endpoint("vpce-a").await.unwrap(),
        legacy_get(&legacy, "vpce-a").await
    );
    assert_eq!(store.vpc_endpoint("vpce-missing").await.unwrap(), None);

    // 快照字节必须逐字节一致（字段顺序/deny_unknown_fields 不漂移）。
    assert_eq!(
        serde_json::to_vec(&store_rows).unwrap(),
        legacy_snapshot_json(&legacy_rows),
        "快照字节一致"
    );

    // delete 幂等：首次 true，再次 false，与 raw SQL 的 rows_affected 一致。
    let mut tx = store_db.pool().begin().await.unwrap();
    let (first_del, second_del) = {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        (
            txn.vpc_endpoint_delete("vpce-a").await.unwrap(),
            txn.vpc_endpoint_delete("vpce-a").await.unwrap(),
        )
    };
    tx.commit().await.unwrap();
    assert!(first_del);
    assert!(!second_del);
    let legacy_del = sqlx::query("DELETE FROM vpc_endpoints WHERE vpce_id = 'vpce-a'")
        .execute(legacy.pool())
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(legacy_del, 1);
    assert_eq!(legacy_get(&legacy, "vpce-a").await, None);
    assert_eq!(store.vpc_endpoint("vpce-a").await.unwrap(), None);
}

#[tokio::test]
async fn replace_and_rollback_match_legacy() {
    let legacy = fresh().await;
    let store_db = fresh().await;
    // 目标库独有行必须被整表替换删除。
    legacy_upsert(&legacy, &row("vpce-old", "vpc-o", "active", 1, 1)).await;
    adapter_upsert(&store_db, &[row("vpce-old", "vpc-o", "active", 1, 1)]).await;

    let replacement = vec![
        row("vpce-b", "vpc-b", "disabled", 2, 3),
        row("vpce-a", "vpc-a", "active", 1, 2),
    ];
    // legacy：apply 安装路径的 DELETE all + 按序 INSERT。
    sqlx::query("DELETE FROM vpc_endpoints")
        .execute(legacy.pool())
        .await
        .unwrap();
    for r in &replacement {
        sqlx::query(
            "INSERT INTO vpc_endpoints \
             (vpce_id, vpc_id, account_id, state, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&r.vpce_id)
        .bind(&r.vpc_id)
        .bind(&r.account_id)
        .bind(&r.state)
        .bind(r.created_at)
        .bind(r.updated_at)
        .execute(legacy.pool())
        .await
        .unwrap();
    }
    let mut tx = store_db.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.replace_vpc_endpoints(&replacement).await.unwrap();
    }
    tx.commit().await.unwrap();

    let legacy_rows = legacy_list(&legacy).await;
    assert_eq!(
        store_db.control_store().vpc_endpoints().await.unwrap(),
        legacy_rows,
        "安装后读一致（升序、无独有行）"
    );
    assert_eq!(
        legacy_snapshot_json(&legacy_rows),
        serde_json::to_vec(&store_db.control_store().vpc_endpoints().await.unwrap()).unwrap()
    );

    // 回滚：同一事务内写入后不提交，表必须保持为空（适配器不自行 commit）。
    let rolled_back = fresh().await;
    let mut tx = rolled_back.pool().begin().await.unwrap();
    {
        let mut txn = oss_server::control_store::SqliteControlTxn::new(&mut tx);
        txn.vpc_endpoint_upsert(&replacement[0]).await.unwrap();
    }
    tx.rollback().await.unwrap();
    assert!(
        rolled_back
            .control_store()
            .vpc_endpoints()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn backend_errors_propagate_without_fallback() {
    let db = fresh().await;
    db.pool().close().await;
    let err = db.control_store().vpc_endpoints().await.unwrap_err();
    assert!(
        matches!(err, xl_store::control_store::ControlError::Backend(_)),
        "{err}"
    );
    assert!(
        err.to_string().starts_with("control store backend: "),
        "{err}"
    );
    assert!(db.control_store().vpc_endpoint("vpce-a").await.is_err());
}

/// 真实快照是 `encode_snapshot` 内的 serde_json 载荷：字段名/顺序冻结，
/// 且已发布的旧快照字节必须仍可反序列化。
#[test]
fn vpc_endpoint_serde_json_schema_is_frozen() {
    let endpoint = row("vpce-1", "vpc-1", "active", 1, 2);
    assert_eq!(
        serde_json::to_string(&endpoint).unwrap(),
        r#"{"vpce_id":"vpce-1","vpc_id":"vpc-1","account_id":"000000000001","state":"active","created_at":1,"updated_at":2}"#
    );

    let frozen_legacy = r#"{"vpce_id":"vpce-9","vpc_id":"vpc-9","account_id":"000000000001","state":"disabled","created_at":3,"updated_at":4}"#;
    let parsed: VpcEndpointRow = serde_json::from_str(frozen_legacy).unwrap();
    assert_eq!(parsed, row("vpce-9", "vpc-9", "disabled", 3, 4));
}
