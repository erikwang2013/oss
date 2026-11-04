//! IAM 差分测试的旧 SQL 参考实现与双库夹具（仅测试）。

use oss_server::control_store::SqliteControlTxn;
use oss_server::db::{Db, SitePeer};
use sqlx::SqliteConnection;
use xl_store::control_store::iam::{AccessKeyEntry, IamTxn};

fn peer() -> SitePeer {
    SitePeer {
        peer_id: "b".into(),
        remote_site_id: "remote-b".into(),
        name: "b".into(),
        endpoint: "http://127.0.0.1:9".into(),
        region: "us-east-1".into(),
        priority: 0,
        s3_access_key: "AKIATEST000000001".into(),
        s3_secret_key: "SKTEST000000000001".into(),
        peer_site_token: "b".repeat(32),
        created_at: 1,
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

pub async fn pair() -> (Db, Db) {
    let a = Db::connect_site("sqlite::memory:", false, Some("test-site"))
        .await
        .unwrap();
    let b = Db::connect_site("sqlite::memory:", false, Some("test-site"))
        .await
        .unwrap();
    for db in [&a, &b] {
        db.insert_site(&peer()).await.unwrap();
    }
    (a, b)
}

// ---------- 旧 SQL 参考实现（仅测试） ----------

pub async fn legacy_user(
    conn: &mut SqliteConnection,
    username: &str,
    hash: &str,
    admin: bool,
    created: i64,
) -> i64 {
    sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)",
    )
    .bind(username)
    .bind(hash)
    .bind(admin as i64)
    .bind(created)
    .execute(&mut *conn)
    .await
    .unwrap()
    .last_insert_rowid()
}

async fn legacy_perm_replace(
    conn: &mut SqliteConnection,
    uid: i64,
    rows: &[(&str, bool, bool, bool)],
) {
    sqlx::query("DELETE FROM permissions WHERE user_id = ?")
        .bind(uid)
        .execute(&mut *conn)
        .await
        .unwrap();
    for (bucket, r, w, d) in rows {
        sqlx::query(
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(uid)
        .bind(*bucket)
        .bind(*r as i64)
        .bind(*w as i64)
        .bind(*d as i64)
        .execute(&mut *conn)
        .await
        .unwrap();
    }
}

async fn legacy_perm_upsert(
    conn: &mut SqliteConnection,
    uid: i64,
    bucket: &str,
    r: bool,
    w: bool,
    d: bool,
) {
    let n = sqlx::query(
        "UPDATE permissions SET can_read = ?, can_write = ?, can_delete = ? \
         WHERE user_id = ? AND bucket = ?",
    )
    .bind(r as i64)
    .bind(w as i64)
    .bind(d as i64)
    .bind(uid)
    .bind(bucket)
    .execute(&mut *conn)
    .await
    .unwrap()
    .rows_affected();
    if n == 0 {
        sqlx::query(
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(uid)
        .bind(bucket)
        .bind(r as i64)
        .bind(w as i64)
        .bind(d as i64)
        .execute(&mut *conn)
        .await
        .unwrap();
    }
}

async fn legacy_ak4(conn: &mut SqliteConnection, ak: &str, sk: &str, uid: i64, created: i64) {
    sqlx::query(
        "INSERT INTO access_keys (access_key, secret_key, user_id, created_at) VALUES (?, ?, ?, ?)",
    )
    .bind(ak)
    .bind(sk)
    .bind(uid)
    .bind(created)
    .execute(&mut *conn)
    .await
    .unwrap();
}

async fn legacy_ak_apply(conn: &mut SqliteConnection, row: &AccessKeyEntry, uid: i64) {
    sqlx::query(
        "INSERT INTO access_keys (access_key, secret_key, user_id, status, kind, \
         parent_ak, session_policy, created_at) VALUES (?, ?, ?, 'active', ?, ?, ?, ?)",
    )
    .bind(&row.access_key)
    .bind(&row.secret_key)
    .bind(uid)
    .bind(&row.kind)
    .bind(&row.parent_ak)
    .bind(&row.session_policy)
    .bind(row.created_at)
    .execute(&mut *conn)
    .await
    .unwrap();
}

async fn legacy_ak_site_upsert(
    conn: &mut SqliteConnection,
    ak: &str,
    sk: &str,
    uid: i64,
    status: &str,
    expires: Option<i64>,
    created: i64,
) {
    let n = sqlx::query(
        "UPDATE access_keys SET secret_key = ?, user_id = ?, status = ?, expires_at = ? \
         WHERE access_key = ?",
    )
    .bind(sk)
    .bind(uid)
    .bind(status)
    .bind(expires)
    .bind(ak)
    .execute(&mut *conn)
    .await
    .unwrap()
    .rows_affected();
    if n == 0 {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, expires_at, \
             created_at, kind) VALUES (?, ?, ?, ?, ?, ?, 'long-term')",
        )
        .bind(ak)
        .bind(sk)
        .bind(uid)
        .bind(status)
        .bind(expires)
        .bind(created)
        .execute(&mut *conn)
        .await
        .unwrap();
    }
}

async fn legacy_delete_user(conn: &mut SqliteConnection, uid: i64) -> bool {
    let n = sqlx::query(
        "UPDATE users SET id = id WHERE id = ? AND \
         (NOT (is_admin = 1 AND status = 'active') \
          OR (SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active') > 1)",
    )
    .bind(uid)
    .execute(&mut *conn)
    .await
    .unwrap()
    .rows_affected();
    if n == 0 {
        return false;
    }
    for sql in [
        "DELETE FROM access_keys WHERE user_id = ?",
        "DELETE FROM permissions WHERE user_id = ?",
        "DELETE FROM users WHERE id = ?",
    ] {
        sqlx::query(sql)
            .bind(uid)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    true
}

pub async fn run_legacy(conn: &mut SqliteConnection) {
    let _root = legacy_user(conn, "root", "h0", true, 1).await;
    let alice = legacy_user(conn, "alice", "h0", false, 2).await;
    let bob = legacy_user(conn, "bob", "h0", false, 3).await;

    sqlx::query("UPDATE users SET password_hash = 'h1' WHERE id = ?")
        .bind(alice)
        .execute(&mut *conn)
        .await
        .unwrap();
    for status in ["disabled", "active"] {
        sqlx::query(
            "UPDATE users SET status = ? WHERE id = ? AND \
             (NOT (is_admin = 1 AND status = 'active') \
              OR (SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active') > 1)",
        )
        .bind(status)
        .bind(alice)
        .execute(&mut *conn)
        .await
        .unwrap();
    }

    legacy_perm_replace(
        conn,
        alice,
        &[("b1", true, false, false), ("b2", false, true, true)],
    )
    .await;
    legacy_perm_upsert(conn, alice, "b1", true, true, true).await;
    legacy_perm_upsert(conn, alice, "b3", true, false, false).await;
    sqlx::query("DELETE FROM permissions WHERE user_id = ? AND bucket = 'b2'")
        .bind(alice)
        .execute(&mut *conn)
        .await
        .unwrap();

    legacy_ak4(conn, "AKALICE1", "SK1", alice, 10).await;
    for status in ["disabled", "active"] {
        sqlx::query("UPDATE access_keys SET status = ? WHERE access_key = 'AKALICE1'")
            .bind(status)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
    legacy_ak_apply(
        conn,
        &AccessKeyEntry {
            access_key: "AKBOB1".into(),
            secret_key: "SKB1".into(),
            username: "bob".into(),
            status: "active".into(),
            expires_at: None,
            created_at: 20,
            parent_ak: None,
            session_policy: None,
            kind: "long-term".into(),
        },
        bob,
    )
    .await;
    legacy_ak_apply(
        conn,
        &AccessKeyEntry {
            access_key: "AKBOB2".into(),
            secret_key: "SKB2".into(),
            username: "bob".into(),
            status: "active".into(),
            expires_at: None,
            created_at: 21,
            parent_ak: Some("AKBOB1".into()),
            session_policy: Some("{}".into()),
            kind: "session".into(),
        },
        bob,
    )
    .await;
    legacy_ak_site_upsert(conn, "AKALICE2", "SKS1", alice, "active", None, 30).await;
    legacy_ak_site_upsert(conn, "AKALICE2", "SKS2", alice, "disabled", Some(40), 999).await;

    legacy_perm_upsert(conn, bob, "b1", true, false, false).await;
    legacy_ak4(conn, "AKBOB3", "SKB3", bob, 50).await;
    assert!(legacy_delete_user(conn, bob).await);
    sqlx::query("DELETE FROM access_keys WHERE access_key = 'AKALICE1' AND kind = 'long-term'")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("DELETE FROM permissions WHERE bucket = 'b1'")
        .execute(&mut *conn)
        .await
        .unwrap();
}

pub async fn run_seam(conn: &mut SqliteConnection) {
    let mut t = SqliteControlTxn::new(conn);
    let _root = t
        .user_create_default_status("root", "h0", true, 1)
        .await
        .unwrap();
    let alice = t
        .user_create_default_status("alice", "h0", false, 2)
        .await
        .unwrap();
    let bob = t
        .user_create_default_status("bob", "h0", false, 3)
        .await
        .unwrap();

    t.user_set_password(alice, "h1").await.unwrap();
    assert!(t.user_set_status(alice, false).await.unwrap());
    assert!(t.user_set_status(alice, true).await.unwrap());

    t.permissions_replace(
        alice,
        &[
            xl_store::control_store::iam::PermissionRow {
                bucket: "b1".into(),
                read: true,
                write: false,
                delete: false,
            },
            xl_store::control_store::iam::PermissionRow {
                bucket: "b2".into(),
                read: false,
                write: true,
                delete: true,
            },
        ],
    )
    .await
    .unwrap();
    t.permission_upsert(alice, "b1", true, true, true)
        .await
        .unwrap();
    t.permission_upsert(alice, "b3", true, false, false)
        .await
        .unwrap();
    t.permission_delete(alice, "b2").await.unwrap();

    t.access_key_create_legacy("AKALICE1", "SK1", alice, 10)
        .await
        .unwrap();
    assert!(
        t.access_key_set_status("AKALICE1", "disabled")
            .await
            .unwrap()
    );
    assert!(t.access_key_set_status("AKALICE1", "active").await.unwrap());
    t.access_key_create(&AccessKeyEntry {
        access_key: "AKBOB1".into(),
        secret_key: "SKB1".into(),
        username: "bob".into(),
        status: "active".into(),
        expires_at: None,
        created_at: 20,
        parent_ak: None,
        session_policy: None,
        kind: "long-term".into(),
    })
    .await
    .unwrap();
    t.access_key_create(&AccessKeyEntry {
        access_key: "AKBOB2".into(),
        secret_key: "SKB2".into(),
        username: "bob".into(),
        status: "active".into(),
        expires_at: None,
        created_at: 21,
        parent_ak: Some("AKBOB1".into()),
        session_policy: Some("{}".into()),
        kind: "session".into(),
    })
    .await
    .unwrap();
    t.access_key_site_upsert("AKALICE2", "SKS1", alice, "active", None, 30)
        .await
        .unwrap();
    t.access_key_site_upsert("AKALICE2", "SKS2", alice, "disabled", Some(40), 999)
        .await
        .unwrap();

    t.permission_upsert(bob, "b1", true, false, false)
        .await
        .unwrap();
    t.access_key_create_legacy("AKBOB3", "SKB3", bob, 50)
        .await
        .unwrap();
    assert!(t.user_delete_guard(bob).await.unwrap());
    t.user_delete_access_keys_permissions(bob).await.unwrap();
    assert!(t.user_delete(bob).await.unwrap());
    assert!(t.access_key_delete_long_term("AKALICE1").await.unwrap());
    t.permissions_delete_for_bucket("b1").await.unwrap();
}
