//! repl_outbox / uploads 迁移差分：入队、claim/renew/retry/bury、站点解配与
//! 上传登记/认领与迁移前 SQL 逐值一致；token 守卫与归属守卫保持。

use oss_server::control_store::{SqliteControlStore, SqliteControlTxn};
use oss_server::db::Db;
use sqlx::{Row, SqlitePool};
use xl_store::control_store::repl_outbox::{NewReplOutbox, ReplOutboxStore, ReplOutboxTxn};
use xl_store::control_store::uploads::{NewUpload, UploadStore, UploadTxn};

async fn pair() -> (Db, Db) {
    (
        Db::connect("sqlite::memory:").await.unwrap(),
        Db::connect("sqlite::memory:").await.unwrap(),
    )
}

fn new_repl(target_kind: &str, remote: &str, version: &str) -> NewReplOutbox {
    NewReplOutbox {
        bucket: "b".into(),
        key: format!("k-{version}"),
        version_id: version.into(),
        etag: format!("etag-{version}"),
        size: 7,
        remote_id: remote.into(),
        dest_bucket: "dest".into(),
        is_delete_marker: version.is_empty(),
        is_version_delete: !version.is_empty(),
        target_kind: target_kind.into(),
        created_at: 100,
    }
}

async fn seam_enqueue(db: &Db, r: &NewReplOutbox) {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .repl_enqueue(r)
        .await
        .unwrap();
}

async fn legacy_enqueue(pool: &SqlitePool, r: &NewReplOutbox) {
    sqlx::query(
        "INSERT INTO repl_outbox (bucket, key, version_id, etag, size, remote_id, \
         dest_bucket, is_delete_marker, is_version_delete, target_kind, state, attempts, \
         next_retry_at, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, ?)",
    )
    .bind(&r.bucket)
    .bind(&r.key)
    .bind(&r.version_id)
    .bind(&r.etag)
    .bind(r.size)
    .bind(&r.remote_id)
    .bind(&r.dest_bucket)
    .bind(i64::from(r.is_delete_marker))
    .bind(i64::from(r.is_version_delete))
    .bind(&r.target_kind)
    .bind(r.created_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn dump_repl(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(seq, bucket, key, version_id, etag, size, remote_id, dest_bucket, \
         is_delete_marker, is_version_delete, target_kind, state, attempts, claimed_at, \
         next_retry_at, created_at, last_error) FROM repl_outbox ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

fn row_json(
    rows: &[xl_store::control_store::repl_outbox::ReplOutboxRow],
) -> Vec<serde_json::Value> {
    rows.iter()
        .map(|r| {
            serde_json::json!({
                "seq": r.seq, "bucket": r.bucket, "key": r.key,
                "version_id": r.version_id, "etag": r.etag, "remote_id": r.remote_id,
                "dest_bucket": r.dest_bucket, "is_delete_marker": r.is_delete_marker,
                "is_version_delete": r.is_version_delete, "target_kind": r.target_kind,
                "attempts": r.attempts, "claimed_at": r.claimed_at,
            })
        })
        .collect()
}

async fn seam_claim(db: &Db, now: i64, limit: i64) -> Vec<serde_json::Value> {
    let mut conn = db.pool().acquire().await.unwrap();
    let rows = SqliteControlTxn::new(&mut conn)
        .repl_claim(now, limit)
        .await
        .unwrap();
    row_json(&rows)
}

async fn legacy_claim(pool: &SqlitePool, now: i64, limit: i64) -> Vec<serde_json::Value> {
    let rows = sqlx::query(
        "UPDATE repl_outbox SET state='sending', claimed_at=? \
         WHERE state='pending' AND seq IN (SELECT seq FROM repl_outbox WHERE state='pending' \
                       AND next_retry_at <= ? ORDER BY seq LIMIT ?) \
         RETURNING seq, bucket, key, version_id, etag, remote_id, dest_bucket, \
         is_delete_marker, is_version_delete, target_kind, attempts, claimed_at",
    )
    .bind(now)
    .bind(now)
    .bind(limit)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.iter()
        .map(|r| {
            serde_json::json!({
                "seq": r.get::<i64, _>("seq"), "bucket": r.get::<String, _>("bucket"),
                "key": r.get::<String, _>("key"), "version_id": r.get::<String, _>("version_id"),
                "etag": r.get::<String, _>("etag"), "remote_id": r.get::<String, _>("remote_id"),
                "dest_bucket": r.get::<String, _>("dest_bucket"),
                "is_delete_marker": r.get::<i64, _>("is_delete_marker") != 0,
                "is_version_delete": r.get::<i64, _>("is_version_delete") != 0,
                "target_kind": r.get::<String, _>("target_kind"),
                "attempts": r.get::<i64, _>("attempts"),
                "claimed_at": r.get::<Option<i64>, _>("claimed_at"),
            })
        })
        .collect()
}

async fn seam_renew(db: &Db, seq: i64, old: Option<i64>, now: i64) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .repl_renew(seq, old, now)
        .await
        .unwrap()
}

async fn legacy_renew(pool: &SqlitePool, seq: i64, old: Option<i64>, now: i64) -> bool {
    sqlx::query(
        "UPDATE repl_outbox SET claimed_at=? WHERE seq=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(now)
    .bind(seq)
    .bind(old)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
        == 1
}

async fn seam_finish(db: &Db, seq: i64, token: Option<i64>) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .repl_finish_ok(seq, token)
        .await
        .unwrap()
}

async fn legacy_finish(pool: &SqlitePool, seq: i64, token: Option<i64>) -> bool {
    sqlx::query("DELETE FROM repl_outbox WHERE seq=? AND claimed_at IS ? AND state='sending'")
        .bind(seq)
        .bind(token)
        .execute(pool)
        .await
        .unwrap()
        .rows_affected()
        == 1
}

async fn seam_retry(db: &Db, seq: i64, token: Option<i64>, attempts: i64, at: i64) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .repl_retry(seq, token, attempts, at, "boom")
        .await
        .unwrap()
}

async fn legacy_retry(
    pool: &SqlitePool,
    seq: i64,
    token: Option<i64>,
    attempts: i64,
    at: i64,
) -> bool {
    sqlx::query(
        "UPDATE repl_outbox SET state='pending', attempts=?, claimed_at=NULL, \
         next_retry_at=?, last_error=? WHERE seq=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(attempts)
    .bind(at)
    .bind("boom")
    .bind(seq)
    .bind(token)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
        == 1
}

async fn seam_bury(db: &Db, seq: i64, token: Option<i64>, attempts: i64) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .repl_bury(seq, token, attempts, "perm")
        .await
        .unwrap()
}

async fn legacy_bury(pool: &SqlitePool, seq: i64, token: Option<i64>, attempts: i64) -> bool {
    sqlx::query(
        "UPDATE repl_outbox SET state='dead', attempts=?, claimed_at=NULL, last_error=? \
         WHERE seq=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(attempts)
    .bind("perm")
    .bind(seq)
    .bind(token)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
        == 1
}

#[tokio::test]
async fn repl_enqueue_claim_renew_retry_bury_matches_legacy() {
    let (s, l) = pair().await;
    let rows = [
        new_repl("remote", "r1", "v1"),
        new_repl("site", "p1", "v2"),
        new_repl("site", "p1", ""),
    ];
    for r in &rows {
        seam_enqueue(&s, r).await;
        legacy_enqueue(l.pool(), r).await;
    }
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);

    assert_eq!(
        seam_claim(&s, 100, 10).await,
        legacy_claim(l.pool(), 100, 10).await
    );
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);

    assert!(seam_renew(&s, 1, Some(100), 110).await);
    assert!(legacy_renew(l.pool(), 1, Some(100), 110).await);
    assert!(!seam_renew(&s, 1, Some(100), 120).await);
    assert!(!legacy_renew(l.pool(), 1, Some(100), 120).await);
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);

    assert!(seam_retry(&s, 1, Some(110), 1, 130).await);
    assert!(legacy_retry(l.pool(), 1, Some(110), 1, 130).await);
    assert!(!seam_retry(&s, 1, Some(110), 1, 130).await);
    assert!(!legacy_retry(l.pool(), 1, Some(110), 1, 130).await);
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);

    assert!(seam_finish(&s, 2, Some(100)).await);
    assert!(legacy_finish(l.pool(), 2, Some(100)).await);
    assert!(!seam_finish(&s, 2, Some(100)).await);
    assert!(!legacy_finish(l.pool(), 2, Some(100)).await);
    assert!(seam_bury(&s, 3, Some(100), 10).await);
    assert!(legacy_bury(l.pool(), 3, Some(100), 10).await);
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);

    // 过期租约回收 → 重新 claim（同一条恢复路径）。
    assert_eq!(
        seam_claim(&s, 130, 10).await,
        legacy_claim(l.pool(), 130, 10).await
    );
    let mut conn = s.pool().acquire().await.unwrap();
    assert_eq!(
        SqliteControlTxn::new(&mut conn)
            .repl_reclaim_expired(200, 60)
            .await
            .unwrap(),
        1
    );
    drop(conn);
    sqlx::query(
        "UPDATE repl_outbox SET state='pending', claimed_at=NULL, next_retry_at=200 \
         WHERE state='sending' AND claimed_at < 140",
    )
    .execute(l.pool())
    .await
    .unwrap();
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);
    assert_eq!(
        seam_claim(&s, 200, 10).await,
        legacy_claim(l.pool(), 200, 10).await
    );
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);

    // 最终：seq1（v1）在租约内 sending；v2 行已成功删除；空版本死信不计入版本聚合。
    let counts = SqliteControlStore::new(s.pool().clone())
        .repl_version_counts("b", "k-v1", "v1")
        .await
        .unwrap();
    assert_eq!((counts.pending, counts.sending, counts.dead), (0, 1, 0));
    assert_eq!(
        SqliteControlStore::new(s.pool().clone())
            .repl_earlier_inflight("b", "k-v1", "v1", 2)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn repl_drop_site_and_delete_site_all_match_legacy() {
    let (s, l) = pair().await;
    for r in [
        new_repl("site", "p1", "v1"),
        new_repl("site", "p1", ""),
        new_repl("remote", "p1", "v2"),
        new_repl("site", "p2", "v3"),
    ] {
        seam_enqueue(&s, &r).await;
        legacy_enqueue(l.pool(), &r).await;
    }
    let mut sconn = s.pool().acquire().await.unwrap();
    let mut ltx = l.pool().begin().await.unwrap();
    let seam = SqliteControlTxn::new(&mut sconn)
        .repl_drop_site("p1")
        .await
        .unwrap();
    // 迁移前 SQL：DELETE ... RETURNING 后清空版本空串行。
    let legacy_rows = sqlx::query(
        "DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ? \
         AND version_id <> '' RETURNING bucket, key, version_id",
    )
    .bind("p1")
    .fetch_all(&mut *ltx)
    .await
    .unwrap();
    sqlx::query(
        "DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ? \
         AND version_id = ''",
    )
    .bind("p1")
    .execute(&mut *ltx)
    .await
    .unwrap();
    ltx.commit().await.unwrap();
    drop(sconn);
    let legacy: Vec<(String, String, String)> = legacy_rows
        .iter()
        .map(|r| (r.get("bucket"), r.get("key"), r.get("version_id")))
        .collect();
    let seam: Vec<(String, String, String)> = seam;
    assert_eq!(seam, legacy);
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);

    let mut conn = s.pool().acquire().await.unwrap();
    let (before, n) = {
        let mut txn = SqliteControlTxn::new(&mut conn);
        let before = txn.repl_site_total("p2").await.unwrap();
        (before, txn.repl_delete_site_all("p2").await.unwrap())
    };
    drop(conn);
    assert_eq!(before, 1);
    sqlx::query("DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ?")
        .bind("p2")
        .execute(l.pool())
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(dump_repl(s.pool()).await, dump_repl(l.pool()).await);
    assert_eq!(
        SqliteControlStore::new(s.pool().clone())
            .repl_site_total("p2")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        SqliteControlStore::new(s.pool().clone())
            .repl_site_state_count("p2", "pending")
            .await
            .unwrap(),
        0
    );
}

async fn dump_uploads(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(saved_path, bucket, key, group_name, size, etag, original_name, \
         owner_id, created_at) FROM uploads ORDER BY saved_path",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

fn upload(path: &str, created_at: i64) -> NewUpload {
    NewUpload {
        saved_path: path.into(),
        bucket: "b".into(),
        key: format!("k-{path}"),
        group_name: "g".into(),
        size: 3,
        etag: "e".into(),
        created_at,
    }
}

#[tokio::test]
async fn uploads_lifecycle_and_ownership_guards_match_legacy() {
    let (s, l) = pair().await;
    let owner = s.create_user("owner", "pw", false).await.unwrap();
    let other = s.create_user("other", "pw", false).await.unwrap();
    // 两个库各自建同名用户，保证 FK 与 uid 可比较（id 从 1 起）。
    l.create_user("owner", "pw", false).await.unwrap();
    l.create_user("other", "pw", false).await.unwrap();
    assert_eq!(owner, 1);
    assert_eq!(other, 2);

    for u in [upload("a", 50), upload("b", 60), upload("c", 60)] {
        let mut conn = s.pool().acquire().await.unwrap();
        SqliteControlTxn::new(&mut conn)
            .upload_record(&u)
            .await
            .unwrap();
        drop(conn);
        sqlx::query(
            "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(saved_path) DO NOTHING",
        )
        .bind(&u.saved_path)
        .bind(&u.bucket)
        .bind(&u.key)
        .bind(&u.group_name)
        .bind(u.size)
        .bind(&u.etag)
        .bind(u.created_at)
        .execute(l.pool())
        .await
        .unwrap();
    }
    // 自然键冲突保留旧行（两条路径都不覆盖）。
    let dup = upload("a", 999);
    let mut conn = s.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .upload_record(&dup)
        .await
        .unwrap();
    drop(conn);
    legacy_enqueue_upload_dup(l.pool()).await;
    assert_eq!(dump_uploads(s.pool()).await, dump_uploads(l.pool()).await);

    // owner 认领无主行；他人不能挂名；管理员可覆盖。
    let mut conn = s.pool().acquire().await.unwrap();
    assert!(
        SqliteControlTxn::new(&mut conn)
            .upload_claim("b", owner)
            .await
            .unwrap()
    );
    assert!(
        !SqliteControlTxn::new(&mut conn)
            .upload_claim("b", other)
            .await
            .unwrap()
    );
    assert!(
        SqliteControlTxn::new(&mut conn)
            .upload_register("b", "orig-b", owner, false)
            .await
            .unwrap()
    );
    assert!(
        !SqliteControlTxn::new(&mut conn)
            .upload_register("b", "stolen", other, false)
            .await
            .unwrap()
    );
    assert!(
        SqliteControlTxn::new(&mut conn)
            .upload_register("b", "admin-b", other, true)
            .await
            .unwrap()
    );
    assert!(
        !SqliteControlTxn::new(&mut conn)
            .upload_register("missing", "x", other, true)
            .await
            .unwrap()
    );
    drop(conn);
    sqlx::query("UPDATE uploads SET owner_id = ? WHERE saved_path = 'b'")
        .bind(owner)
        .execute(l.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE uploads SET original_name = ? WHERE saved_path = 'b'")
        .bind("orig-b")
        .execute(l.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE uploads SET original_name = ?, owner_id = ? WHERE saved_path = 'b'")
        .bind("admin-b")
        .bind(other)
        .execute(l.pool())
        .await
        .unwrap();
    assert_eq!(dump_uploads(s.pool()).await, dump_uploads(l.pool()).await);

    let all = SqliteControlStore::new(s.pool().clone())
        .uploads(2)
        .await
        .unwrap();
    assert_eq!(
        all.iter()
            .map(|r| r.saved_path.as_str())
            .collect::<Vec<_>>(),
        vec!["b", "c"]
    );
    let mine = SqliteControlStore::new(s.pool().clone())
        .uploads_for(other, 10)
        .await
        .unwrap();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].saved_path, "b");
    assert_eq!(
        SqliteControlStore::new(s.pool().clone())
            .upload("a")
            .await
            .unwrap()
            .unwrap()
            .created_at,
        50
    );
}

async fn legacy_enqueue_upload_dup(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, created_at) \
         VALUES ('a', 'b', 'k-a', 'g', 3, 'e', 999) ON CONFLICT(saved_path) DO NOTHING",
    )
    .execute(pool)
    .await
    .unwrap();
}
