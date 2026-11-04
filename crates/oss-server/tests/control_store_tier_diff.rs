//! tier restore 切片双后端差分：legacy raw SQL（测试内参考实现）vs
//! `SqliteControlStore` / `SqliteControlTxn`。

use std::sync::Arc;

use oss_server::control_store::SqliteControlTxn;
use oss_server::db::Db;
use sqlx::Row;
use xl_store::control_store::tier::{TierRestoreRow, TierTxn};

async fn fresh() -> Arc<Db> {
    Arc::new(Db::connect("sqlite::memory:").await.unwrap())
}

fn row(
    bucket: &str,
    key: &str,
    version: &str,
    state: &str,
    expires: i64,
    created: i64,
    term: i64,
) -> TierRestoreRow {
    TierRestoreRow {
        bucket: bucket.into(),
        key: key.into(),
        version_id: version.into(),
        expires_at: expires,
        job_id: Some(1),
        state: state.into(),
        created_at: created,
        fence_term: term,
        fence_leader: 1,
    }
}

async fn raw_insert(db: &Db, r: &TierRestoreRow) {
    sqlx::query(
        "INSERT INTO tier_restores (bucket, key, version_id, expires_at, job_id, state, \
         created_at, fence_term, fence_leader) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&r.bucket)
    .bind(&r.key)
    .bind(&r.version_id)
    .bind(r.expires_at)
    .bind(r.job_id)
    .bind(&r.state)
    .bind(r.created_at)
    .bind(r.fence_term)
    .bind(r.fence_leader)
    .execute(db.pool())
    .await
    .unwrap();
}

async fn dump(db: &Db) -> Vec<TierRestoreRow> {
    sqlx::query(
        "SELECT bucket, key, version_id, expires_at, job_id, state, created_at, \
         fence_term, fence_leader FROM tier_restores ORDER BY bucket, key, version_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| TierRestoreRow {
        bucket: r.get("bucket"),
        key: r.get("key"),
        version_id: r.get("version_id"),
        expires_at: r.get("expires_at"),
        job_id: r.get("job_id"),
        state: r.get("state"),
        created_at: r.get("created_at"),
        fence_term: r.get("fence_term"),
        fence_leader: r.get("fence_leader"),
    })
    .collect()
}

/// 旧 `upsert_restore_row` 的 SQL（create_restore_job 单机门面参考）。
async fn legacy_pending_upsert(db: &Db, r: &TierRestoreRow) {
    sqlx::query(
        "INSERT INTO tier_restores (bucket, key, version_id, expires_at, job_id, state, \
         created_at, fence_term, fence_leader) VALUES (?, ?, ?, 0, ?, 'pending', ?, ?, ?) \
         ON CONFLICT(bucket, key, version_id) DO UPDATE SET \
         expires_at = 0, job_id = excluded.job_id, state = 'pending', \
         created_at = excluded.created_at, \
         fence_term = MAX(tier_restores.fence_term, excluded.fence_term), \
         fence_leader = CASE WHEN excluded.fence_term >= tier_restores.fence_term \
           THEN excluded.fence_leader ELSE tier_restores.fence_leader END \
         WHERE tier_restores.fence_term <= excluded.fence_term",
    )
    .bind(&r.bucket)
    .bind(&r.key)
    .bind(&r.version_id)
    .bind(r.job_id)
    .bind(r.created_at)
    .bind(r.fence_term)
    .bind(r.fence_leader)
    .execute(db.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn tier_reads_match_legacy_state_and_order() {
    let db = fresh().await;
    raw_insert(&db, &row("b", "k2", "v", "restored", 100, 2, 1)).await;
    raw_insert(&db, &row("b", "k1", "v", "restored", 100, 1, 1)).await;
    raw_insert(&db, &row("b", "k3", "v", "pending", 0, 3, 1)).await;

    let store = db.control_store();
    assert_eq!(
        store
            .tier_restores("restored")
            .await
            .unwrap()
            .iter()
            .map(|r| r.key.as_str())
            .collect::<Vec<_>>(),
        ["k1", "k2"],
        "按 created_at 升序"
    );
    let got = store.tier_restore("b", "k2", "v").await.unwrap().unwrap();
    assert_eq!(got, row("b", "k2", "v", "restored", 100, 2, 1));
    assert_eq!(store.tier_restore("b", "missing", "v").await.unwrap(), None);
}

#[tokio::test]
async fn tier_upsert_delete_and_set_state_are_fence_bounded() {
    let db = fresh().await;
    raw_insert(&db, &row("b", "k", "v", "restored", 100, 1, 5)).await;

    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    assert!(
        !txn.tier_restore_upsert(&row("b", "k", "v", "pending", 0, 2, 4))
            .await
            .unwrap(),
        "旧 term 不得覆盖"
    );
    assert!(
        txn.tier_restore_upsert(&row("b", "k", "v", "pending", 0, 2, 5))
            .await
            .unwrap()
    );
    txn.tier_restore_set_state("b", "k", "v", "restored", 200, 4, 1)
        .await
        .unwrap();
    assert_eq!(
        txn.tier_restore("b", "k", "v")
            .await
            .unwrap()
            .unwrap()
            .state,
        "pending",
        "set_state 旧 term no-op"
    );
    txn.tier_restore_delete("b", "k", "v", 4).await.unwrap();
    assert!(
        txn.tier_restore("b", "k", "v").await.unwrap().is_some(),
        "delete 旧 term no-op"
    );
    txn.tier_restore_delete("b", "k", "v", 5).await.unwrap();
    assert!(txn.tier_restore("b", "k", "v").await.unwrap().is_none());
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn pending_reset_keeps_max_fence_and_matches_legacy_sql() {
    let seam = fresh().await;
    let legacy = fresh().await;
    let restored = row("b", "k", "v", "restored", 100, 1, 9);
    raw_insert(&seam, &restored).await;
    raw_insert(&legacy, &restored).await;

    // UNFENCED 新请求：旧 SQL 的 WHERE 拒绝；seam 同样零写。
    let unfenced = row("b", "k", "v", "pending", 0, 2, 0);
    legacy_pending_upsert(&legacy, &unfenced).await;
    let mut tx = seam.pool().begin().await.unwrap();
    let applied = {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.tier_restore_upsert(&unfenced).await.unwrap()
    };
    tx.commit().await.unwrap();
    assert!(!applied);
    assert_eq!(dump(&seam).await, dump(&legacy).await, "低 fence 语义一致");
    assert_eq!(dump(&seam).await[0].state, "restored");

    // 同 term 新请求：两边都重置为 pending。
    let pending = row("b", "k", "v", "pending", 0, 2, 9);
    legacy_pending_upsert(&legacy, &pending).await;
    let mut tx = seam.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.tier_restore_upsert(&pending).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(
        dump(&seam).await,
        dump(&legacy).await,
        "pending 重置语义一致"
    );
    assert_eq!(dump(&seam).await[0].state, "pending");
}
