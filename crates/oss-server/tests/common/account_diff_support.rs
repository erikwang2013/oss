//! 账户差分测试的旧 SQL 参考实现与双库夹具（仅测试）。

use oss_server::control_store::SqliteControlTxn;
use oss_server::db::Db;
use sqlx::SqliteConnection;
use xl_store::control_store::account::AccountTxn;
use xl_store::control_store::iam::IamTxn;

pub async fn pair() -> (Db, Db) {
    let a = Db::connect("sqlite::memory:").await.unwrap();
    let b = Db::connect("sqlite::memory:").await.unwrap();
    (a, b)
}

pub async fn legacy_user(conn: &mut SqliteConnection, name: &str, admin: bool, created: i64) -> i64 {
    sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, 'h', ?, ?)",
    )
    .bind(name)
    .bind(admin as i64)
    .bind(created)
    .execute(&mut *conn)
    .await
    .unwrap()
    .last_insert_rowid()
}

async fn legacy_bucket(conn: &mut SqliteConnection, name: &str, owner: i64) {
    sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES (?, ?, 1)")
        .bind(name)
        .bind(owner)
        .execute(&mut *conn)
        .await
        .unwrap();
}

async fn legacy_quota_reached(conn: &mut SqliteConnection, owner_id: Option<i64>) -> bool {
    let Some(uid) = owner_id else {
        return false;
    };
    let account: Option<String> =
        sqlx::query_scalar("SELECT account_id FROM account_members WHERE uid = ?")
            .bind(uid)
            .fetch_optional(&mut *conn)
            .await
            .unwrap();
    let Some(account) = account.or_else(|| xl_s3::access::account_id_of(uid)) else {
        return false;
    };
    let Some((status, quota)): Option<(String, Option<i64>)> =
        sqlx::query_as("SELECT status, bucket_quota FROM accounts WHERE id = ?")
            .bind(&account)
            .fetch_optional(&mut *conn)
            .await
            .unwrap()
    else {
        return false;
    };
    if status != "active" {
        return true;
    }
    let Some(quota) = quota.filter(|q| *q >= 0) else {
        return false;
    };
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM buckets b JOIN users u ON u.id = b.owner_id \
         LEFT JOIN account_members bm ON bm.uid = u.id \
         WHERE u.is_admin = 0 AND COALESCE(bm.account_id, printf('%012d', u.id)) = ?",
    )
    .bind(&account)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    n >= quota
}

async fn legacy_member(
    conn: &mut SqliteConnection,
    account: &str,
    uid: i64,
    role: &str,
    created: i64,
) {
    sqlx::query("INSERT INTO account_members (account_id, uid, role, created_at) VALUES (?, ?, ?, ?)")
        .bind(account)
        .bind(uid)
        .bind(role)
        .bind(created)
        .execute(&mut *conn)
        .await
        .unwrap();
}

async fn legacy_member_upsert(
    conn: &mut SqliteConnection,
    account: &str,
    uid: i64,
    role: &str,
    created: i64,
) {
    sqlx::query(
        "INSERT INTO account_members (account_id, uid, role, created_at) VALUES (?, ?, ?, ?) \
         ON CONFLICT(uid) DO UPDATE SET account_id = excluded.account_id, role = excluded.role",
    )
    .bind(account)
    .bind(uid)
    .bind(role)
    .bind(created)
    .execute(&mut *conn)
    .await
    .unwrap();
}

async fn legacy_set_status(conn: &mut SqliteConnection, id: &str, status: &str, ts: i64) -> bool {
    let n = if status == "active" {
        sqlx::query("UPDATE accounts SET status = 'active', updated_at = ? WHERE id = ?")
            .bind(ts)
            .bind(id)
            .execute(&mut *conn)
            .await
            .unwrap()
            .rows_affected()
    } else {
        sqlx::query(
            "UPDATE accounts SET status = 'disabled', updated_at = ? WHERE id = ? AND NOT EXISTS (\
             SELECT 1 FROM account_members am JOIN users u ON u.id = am.uid \
             WHERE am.account_id = accounts.id AND u.is_admin = 1 AND u.status = 'active')",
        )
        .bind(ts)
        .bind(id)
        .execute(&mut *conn)
        .await
        .unwrap()
        .rows_affected()
    };
    n == 1
}

pub async fn run_legacy(conn: &mut SqliteConnection) -> Vec<bool> {
    let mut out = Vec::new();
    let root = legacy_user(conn, "root", true, 1).await;
    let alice = legacy_user(conn, "alice", false, 2).await;
    let bob = legacy_user(conn, "bob", false, 3).await;

    for _ in 0..2 {
        out.push(
            sqlx::query(
                "INSERT OR IGNORE INTO accounts (id, name, status, admin_uid, created_at, updated_at) \
                 VALUES ('000000000002', 'alice', 'active', ?, 1, 2)",
            )
            .bind(alice)
            .execute(&mut *conn)
            .await
            .unwrap()
            .rows_affected()
                == 1,
        );
    }
    out.push(
        sqlx::query(
            "INSERT OR IGNORE INTO accounts (id, name, status, created_at, updated_at) \
             VALUES ('000000000000', 'system', 'active', 3, 4)",
        )
        .execute(&mut *conn)
        .await
        .unwrap()
        .rows_affected()
            == 1,
    );
    for (id, name, quota) in [("acct-x", "team", Some(2)), ("acct-y", "team-y", None)] {
        out.push(
            sqlx::query(
                "INSERT OR IGNORE INTO accounts (id, name, status, bucket_quota, created_at, updated_at) \
                 VALUES (?, ?, 'active', ?, 5, 6)",
            )
            .bind(id)
            .bind(name)
            .bind(quota)
            .execute(&mut *conn)
            .await
            .unwrap()
            .rows_affected()
                == 1,
        );
    }
    out.push(
        sqlx::query("UPDATE accounts SET name = 'team2', updated_at = 7 WHERE id = 'acct-x'")
            .execute(&mut *conn)
            .await
            .unwrap()
            .rows_affected()
            == 1,
    );
    out.push(
        sqlx::query("UPDATE accounts SET bucket_quota = 2, updated_at = 8 WHERE id = 'acct-x'")
            .execute(&mut *conn)
            .await
            .unwrap()
            .rows_affected()
            == 1,
    );
    out.push(
        sqlx::query("UPDATE accounts SET soft_byte_quota = 100, updated_at = 9 WHERE id = 'acct-x'")
            .execute(&mut *conn)
            .await
            .unwrap()
            .rows_affected()
            == 1,
    );
    out.push(
        sqlx::query(
            "UPDATE accounts SET hard_byte_quota = 1000, object_quota = 10, updated_at = 10 \
             WHERE id = 'acct-x'",
        )
        .execute(&mut *conn)
        .await
        .unwrap()
        .rows_affected()
            == 1,
    );

    legacy_member(conn, "acct-x", bob, "member", 11).await;
    out.push(
        sqlx::query(
            "INSERT OR IGNORE INTO account_members (account_id, uid, role, created_at) \
             VALUES ('acct-x', ?, 'owner', 12)",
        )
        .bind(bob)
        .execute(&mut *conn)
        .await
        .unwrap()
        .rows_affected()
            == 1,
    );
    legacy_member_upsert(conn, "acct-y", bob, "owner", 13).await;
    legacy_member_upsert(conn, "acct-x", bob, "owner", 14).await;
    legacy_member(conn, "acct-x", root, "owner", 15).await;
    for uid in [bob, alice] {
        sqlx::query("UPDATE accounts SET admin_uid = ? WHERE id = 'acct-x' AND admin_uid IS NULL")
            .bind(uid)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    out.push(legacy_set_status(conn, "acct-x", "disabled", 18).await);
    sqlx::query(
        "DELETE FROM account_members WHERE uid IN (SELECT id FROM users WHERE is_admin = 1)",
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    out.push(legacy_set_status(conn, "acct-x", "disabled", 19).await);
    out.push(legacy_set_status(conn, "acct-x", "active", 20).await);
    sqlx::query(
        "INSERT OR IGNORE INTO accounts (id, name, status, admin_uid, created_at, updated_at) \
         VALUES ('000000000001', 'root', 'active', ?, 21, 22)",
    )
    .bind(root)
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query(
        "DELETE FROM accounts \
         WHERE id IN (SELECT printf('%012d', id) FROM users WHERE is_admin = 1) \
           AND id NOT IN (SELECT account_id FROM account_members)",
    )
    .execute(&mut *conn)
    .await
    .unwrap();

    for (name, owner) in [("ba1", alice), ("ba2", alice), ("ba3", alice), ("bb1", bob)] {
        legacy_bucket(conn, name, owner).await;
    }
    for owner in [None, Some(root), Some(alice), Some(bob)] {
        out.push(legacy_quota_reached(conn, owner).await);
    }
    let sql = "UPDATE accounts SET bucket_quota = ?, updated_at = ? WHERE id = ?";
    for (quota, ts) in [(Some(1), 23), (None, 24), (Some(-1), 25), (Some(2), 26)] {
        sqlx::query(sql)
            .bind(quota)
            .bind(ts)
            .bind("acct-x")
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    out.push(legacy_quota_reached(conn, Some(bob)).await);
    out.push(legacy_set_status(conn, "acct-x", "disabled", 27).await);
    out.push(legacy_quota_reached(conn, Some(bob)).await);
    out.push(legacy_set_status(conn, "acct-x", "active", 28).await);
    sqlx::query("UPDATE accounts SET bucket_quota = 2, updated_at = 29 WHERE id = '000000000002'")
        .execute(&mut *conn)
        .await
        .unwrap();
    out.push(legacy_quota_reached(conn, Some(alice)).await);
    sqlx::query("UPDATE accounts SET bucket_quota = 4, updated_at = 30 WHERE id = '000000000002'")
        .execute(&mut *conn)
        .await
        .unwrap();
    out.push(legacy_quota_reached(conn, Some(alice)).await);

    for (id, bytes, count) in [("acct-x", 100, 1), ("acct-x", 200, 3), ("000000000002", 10, 0)] {
        sqlx::query(
            "INSERT INTO account_usage_ledger (account_id, used_bytes, object_count) \
             VALUES (?, ?, ?) ON CONFLICT(account_id) DO UPDATE SET \
             used_bytes = excluded.used_bytes, object_count = excluded.object_count",
        )
        .bind(id)
        .bind(bytes)
        .bind(count)
        .execute(&mut *conn)
        .await
        .unwrap();
    }
    sqlx::query("INSERT INTO account_usage (account_id) VALUES ('acct-y')")
        .execute(&mut *conn)
        .await
        .unwrap();
    out.push(
        sqlx::query("DELETE FROM accounts WHERE id = 'acct-y'")
            .execute(&mut *conn)
            .await
            .unwrap()
            .rows_affected()
            == 1,
    );
    out
}

pub async fn run_seam(conn: &mut SqliteConnection) -> Vec<bool> {
    let mut out = Vec::new();
    let (root, alice, bob) = {
        let mut t = SqliteControlTxn::new(&mut *conn);
        let root = t.user_create_default_status("root", "h", true, 1).await.unwrap();
        let alice = t.user_create_default_status("alice", "h", false, 2).await.unwrap();
        let bob = t.user_create_default_status("bob", "h", false, 3).await.unwrap();
        (root, alice, bob)
    };
    for (name, owner) in [("ba1", alice), ("ba2", alice), ("ba3", alice), ("bb1", bob)] {
        legacy_bucket(&mut *conn, name, owner).await;
    }
    let mut t = SqliteControlTxn::new(conn);

    for _ in 0..2 {
        out.push(
            t.account_insert_derived_if_absent("000000000002", "alice", alice, 1, 2)
                .await
                .unwrap(),
        );
    }
    out.push(
        t.account_system_insert_if_absent("000000000000", "system", 3, 4)
            .await
            .unwrap(),
    );
    for (id, name, quota) in [("acct-x", "team", Some(2)), ("acct-y", "team-y", None)] {
        out.push(
            t.account_insert_if_absent(id, name, quota, 5, 6)
                .await
                .unwrap(),
        );
    }
    out.push(t.account_set_name("acct-x", "team2", 7).await.unwrap());
    out.push(
        t.account_set_bucket_quota("acct-x", Some(2), 8)
            .await
            .unwrap(),
    );
    out.push(
        t.account_set_soft_quota("acct-x", Some(100), 9)
            .await
            .unwrap(),
    );
    out.push(
        t.account_set_hard_limits("acct-x", Some(1000), Some(10), 10)
            .await
            .unwrap(),
    );

    t.account_member_insert("acct-x", bob, "member", 11)
        .await
        .unwrap();
    out.push(
        t.account_member_insert_if_absent("acct-x", bob, "owner", 12)
            .await
            .unwrap(),
    );
    t.account_member_upsert("acct-y", bob, "owner", 13)
        .await
        .unwrap();
    t.account_member_upsert("acct-x", bob, "owner", 14)
        .await
        .unwrap();
    t.account_member_insert("acct-x", root, "owner", 15)
        .await
        .unwrap();
    for uid in [bob, alice] {
        t.account_set_admin_if_unset("acct-x", uid).await.unwrap();
    }
    out.push(t.account_set_status("acct-x", "disabled", 18).await.unwrap());
    t.account_members_delete_admins().await.unwrap();
    out.push(t.account_set_status("acct-x", "disabled", 19).await.unwrap());
    out.push(t.account_set_status("acct-x", "active", 20).await.unwrap());
    t.account_insert_derived_if_absent("000000000001", "root", root, 21, 22)
        .await
        .unwrap();
    t.accounts_delete_admin_derived().await.unwrap();

    for owner in [None, Some(root), Some(alice), Some(bob)] {
        out.push(t.bucket_quota_reached(owner).await.unwrap());
    }
    for (quota, ts) in [(Some(1), 23), (None, 24), (Some(-1), 25), (Some(2), 26)] {
        t.account_set_bucket_quota("acct-x", quota, ts)
            .await
            .unwrap();
    }
    out.push(t.bucket_quota_reached(Some(bob)).await.unwrap());
    out.push(t.account_set_status("acct-x", "disabled", 27).await.unwrap());
    out.push(t.bucket_quota_reached(Some(bob)).await.unwrap());
    out.push(t.account_set_status("acct-x", "active", 28).await.unwrap());
    t.account_set_bucket_quota("000000000002", Some(2), 29)
        .await
        .unwrap();
    out.push(t.bucket_quota_reached(Some(alice)).await.unwrap());
    t.account_set_bucket_quota("000000000002", Some(4), 30)
        .await
        .unwrap();
    out.push(t.bucket_quota_reached(Some(alice)).await.unwrap());

    for (id, bytes, count) in [("acct-x", 100, 1), ("acct-x", 200, 3), ("000000000002", 10, 0)] {
        t.account_ledger_upsert(id, bytes, count).await.unwrap();
    }
    drop(t);
    sqlx::query("INSERT INTO account_usage (account_id) VALUES ('acct-y')")
        .execute(&mut *conn)
        .await
        .unwrap();
    let mut t = SqliteControlTxn::new(conn);
    out.push(t.account_delete("acct-y").await.unwrap());
    out
}
