//! 账户 / 成员 / 账本 / 配额差分：A 库旧 raw SQL，B 库 `AccountTxn`。
//!
//! 账户三表无 site 触发器，因此不比较 changelog；比较全列与布尔返回序列
//! （`INSERT OR IGNORE`、停用守卫、配额判定）。

#[path = "common/account_diff_support.rs"]
mod support;

use oss_server::control_store::SqliteControlTxn;
use oss_server::db::Db;
use sqlx::Row;
use support::{legacy_user, pair, run_legacy, run_seam};
use xl_store::control_store::account::AccountTxn;
use xl_store::control_store::iam::IamTxn;

// ---------- 导出与比较 ----------

type AccountD = (
    String,
    String,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    i64,
    i64,
);
type MemberD = (String, i64, String, i64);
type LedgerD = (String, i64, i64);
type UsageD = (String, i64, i64, i64, i64);

#[derive(Debug, PartialEq, Eq)]
struct Dump {
    accounts: Vec<AccountD>,
    members: Vec<MemberD>,
    ledger: Vec<LedgerD>,
    usage: Vec<UsageD>,
}

async fn dump(db: &Db) -> Dump {
    let accounts = sqlx::query(
        "SELECT id, name, status, admin_uid, bucket_quota, soft_byte_quota, hard_byte_quota, \
         object_quota, created_at, updated_at FROM accounts ORDER BY id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("id"),
            r.get("name"),
            r.get("status"),
            r.get("admin_uid"),
            r.get("bucket_quota"),
            r.get("soft_byte_quota"),
            r.get("hard_byte_quota"),
            r.get("object_quota"),
            r.get("created_at"),
            r.get("updated_at"),
        )
    })
    .collect();
    let members = sqlx::query(
        "SELECT account_id, uid, role, created_at FROM account_members ORDER BY account_id, uid",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("account_id"),
            r.get("uid"),
            r.get("role"),
            r.get("created_at"),
        )
    })
    .collect();
    let ledger = sqlx::query(
        "SELECT account_id, used_bytes, object_count FROM account_usage_ledger ORDER BY account_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("account_id"),
            r.get("used_bytes"),
            r.get("object_count"),
        )
    })
    .collect();
    let usage = sqlx::query(
        "SELECT account_id, used_bytes, object_count, complete, computed_at \
         FROM account_usage ORDER BY account_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("account_id"),
            r.get("used_bytes"),
            r.get("object_count"),
            r.get("complete"),
            r.get("computed_at"),
        )
    })
    .collect();
    Dump {
        accounts,
        members,
        ledger,
        usage,
    }
}

#[tokio::test]
async fn account_writes_match_legacy_sql() {
    let (a, b) = pair().await;
    let mut ca = a.pool().acquire().await.unwrap();
    let outcomes_a = run_legacy(&mut ca).await;
    drop(ca);
    let mut cb = b.pool().acquire().await.unwrap();
    let outcomes_b = run_seam(&mut cb).await;
    drop(cb);
    assert_eq!(outcomes_a, outcomes_b);
    assert_eq!(dump(&a).await, dump(&b).await);
}

#[tokio::test]
async fn rollback_discards_account_writes() {
    let (a, b) = pair().await;
    let mut ca = a.pool().acquire().await.unwrap();
    let alice = legacy_user(&mut ca, "alice", false, 1).await;
    sqlx::query(
        "INSERT INTO accounts (id, name, status, created_at, updated_at) \
         VALUES ('000000000002', 'alice', 'active', 1, 2)",
    )
    .execute(&mut *ca)
    .await
    .unwrap();
    drop(ca);

    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        let uid = t
            .user_create_default_status("alice", "h", false, 1)
            .await
            .unwrap();
        assert_eq!(uid, alice);
        t.account_insert_if_absent("000000000002", "alice", None, 1, 2)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        t.account_insert_if_absent("acct-r", "r", Some(1), 3, 4)
            .await
            .unwrap();
        t.account_member_insert("acct-r", alice, "owner", 5)
            .await
            .unwrap();
        t.account_ledger_upsert("acct-r", 9, 9).await.unwrap();
        t.account_ledger_upsert("000000000002", 1, 1).await.unwrap();
        t.account_set_soft_quota("000000000002", Some(7), 8)
            .await
            .unwrap();
    }
    tx.rollback().await.unwrap();
    assert_eq!(dump(&a).await, dump(&b).await);
}
