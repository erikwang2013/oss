//! event_outbox 迁移差分：入队/claim/续租/完成/重试/死信/重投与迁移前 SQL 等价，
//! claim token 守卫和 id 顺序保持；源码审计确保生产直写只在适配器。

use oss_server::control_store::{SqliteControlStore, SqliteControlTxn};
use oss_server::db::Db;
use sqlx::{Row, SqlitePool};
use xl_store::control_store::event_outbox::{EventOutboxStore, EventOutboxTxn};

async fn pair() -> (Db, Db) {
    (
        Db::connect("sqlite::memory:").await.unwrap(),
        Db::connect("sqlite::memory:").await.unwrap(),
    )
}

async fn enqueue(db: &Db, bucket: &str, target: &str, payload: &str, now: i64) {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .event_enqueue(bucket, target, payload, now)
        .await
        .unwrap();
}

async fn legacy_enqueue(pool: &SqlitePool, bucket: &str, target: &str, payload: &str, now: i64) {
    sqlx::query(
        "INSERT INTO event_outbox (bucket, target_id, event_json, state, attempts, \
         next_retry_at, created_at) VALUES (?, ?, ?, 'pending', 0, 0, ?)",
    )
    .bind(bucket)
    .bind(target)
    .bind(payload)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}

/// 整表快照（含 AUTOINCREMENT id 与 token），用于证明两条路径逐值一致。
async fn dump(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(id, bucket, target_id, event_json, state, attempts, claimed_at, \
         next_retry_at, created_at, last_error) FROM event_outbox ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

fn claim_json(
    rows: &[xl_store::control_store::event_outbox::EventOutboxClaim],
) -> Vec<serde_json::Value> {
    rows.iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id, "target_id": r.target_id, "event_json": r.event_json,
                "attempts": r.attempts, "claimed_at": r.claimed_at,
            })
        })
        .collect()
}

async fn legacy_claim(pool: &SqlitePool, now: i64, limit: i64) -> Vec<serde_json::Value> {
    let rows = sqlx::query(
        "UPDATE event_outbox SET state='sending', claimed_at=? \
         WHERE id IN (SELECT id FROM event_outbox WHERE state='pending' AND next_retry_at <= ? \
                      ORDER BY id LIMIT ?) \
         RETURNING id, target_id, event_json, attempts, claimed_at",
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
                "id": r.get::<i64, _>("id"),
                "target_id": r.get::<String, _>("target_id"),
                "event_json": r.get::<String, _>("event_json"),
                "attempts": r.get::<i64, _>("attempts"),
                "claimed_at": r.get::<Option<i64>, _>("claimed_at"),
            })
        })
        .collect()
}

async fn seam_claim(db: &Db, now: i64, limit: i64) -> Vec<serde_json::Value> {
    let mut conn = db.pool().acquire().await.unwrap();
    let rows = SqliteControlTxn::new(&mut conn)
        .event_claim(now, limit)
        .await
        .unwrap();
    claim_json(&rows)
}

async fn seam_renew(db: &Db, id: i64, old: Option<i64>, now: i64) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .event_renew(id, old, now)
        .await
        .unwrap()
}

async fn legacy_renew(pool: &SqlitePool, id: i64, old: Option<i64>, now: i64) -> bool {
    sqlx::query(
        "UPDATE event_outbox SET claimed_at=? WHERE id=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(now)
    .bind(id)
    .bind(old)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
        == 1
}

async fn seam_finish(db: &Db, id: i64, token: Option<i64>) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .event_finish_ok(id, token)
        .await
        .unwrap()
}

async fn legacy_finish(pool: &SqlitePool, id: i64, token: Option<i64>) -> bool {
    sqlx::query("DELETE FROM event_outbox WHERE id=? AND claimed_at IS ? AND state='sending'")
        .bind(id)
        .bind(token)
        .execute(pool)
        .await
        .unwrap()
        .rows_affected()
        == 1
}

async fn seam_retry(
    db: &Db,
    id: i64,
    token: Option<i64>,
    attempts: i64,
    at: i64,
    err: &str,
) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .event_retry(id, token, attempts, at, err)
        .await
        .unwrap()
}

async fn legacy_retry(
    pool: &SqlitePool,
    id: i64,
    token: Option<i64>,
    attempts: i64,
    at: i64,
    err: &str,
) -> bool {
    sqlx::query(
        "UPDATE event_outbox SET state='pending', attempts=?, claimed_at=NULL, \
         next_retry_at=?, last_error=? WHERE id=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(attempts)
    .bind(at)
    .bind(err)
    .bind(id)
    .bind(token)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
        == 1
}

async fn seam_bury(db: &Db, id: i64, token: Option<i64>, attempts: i64, err: &str) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .event_bury(id, token, attempts, err)
        .await
        .unwrap()
}

async fn legacy_bury(
    pool: &SqlitePool,
    id: i64,
    token: Option<i64>,
    attempts: i64,
    err: &str,
) -> bool {
    sqlx::query(
        "UPDATE event_outbox SET state='dead', attempts=?, claimed_at=NULL, last_error=? \
         WHERE id=? AND claimed_at IS ? AND state='sending'",
    )
    .bind(attempts)
    .bind(err)
    .bind(id)
    .bind(token)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
        == 1
}

async fn seam_redeliver(db: &Db, id: i64, now: i64) -> bool {
    let mut conn = db.pool().acquire().await.unwrap();
    SqliteControlTxn::new(&mut conn)
        .event_redeliver(id, now)
        .await
        .unwrap()
}

async fn legacy_redeliver(pool: &SqlitePool, id: i64, now: i64) -> bool {
    sqlx::query(
        "UPDATE event_outbox SET state='pending', claimed_at=NULL, next_retry_at=?, attempts=0, \
         last_error=NULL WHERE id=? AND state='dead'",
    )
    .bind(now)
    .bind(id)
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
        == 1
}

/// 入队顺序、claim 批量/顺序与 RETURNING 列与旧 SQL 一致。
#[tokio::test]
async fn enqueue_and_claim_match_legacy_sql() {
    let (seam_db, legacy_db) = pair().await;
    for (i, (target, payload)) in [("t1", "p1"), ("t2", "p2"), ("t3", "p3")]
        .iter()
        .enumerate()
    {
        let at = 1000 + i as i64;
        enqueue(&seam_db, "bkt", target, payload, at).await;
        legacy_enqueue(legacy_db.pool(), "bkt", target, payload, at).await;
    }
    let got = seam_claim(&seam_db, 2000, 2).await;
    let want = legacy_claim(legacy_db.pool(), 2000, 2).await;
    assert_eq!(got, want, "claim 行与顺序必须一致");
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);

    // 第二轮只拿到剩余 pending（已 sending 的不再领取）。
    assert_eq!(
        seam_claim(&seam_db, 2001, 16).await,
        legacy_claim(legacy_db.pool(), 2001, 16).await
    );
    assert_eq!(
        seam_claim(&seam_db, 2002, 16).await,
        legacy_claim(legacy_db.pool(), 2002, 16).await
    );
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);
}

/// 续租 token 轮换、stale token 的完成/回写/死信均为 no-op；重试与重投字段一致。
#[tokio::test]
async fn token_guards_and_retry_bury_redeliver_match_legacy() {
    let (seam_db, legacy_db) = pair().await;
    enqueue(&seam_db, "bkt", "t1", "payload", 10).await;
    legacy_enqueue(legacy_db.pool(), "bkt", "t1", "payload", 10).await;

    // claim 得到 token=100；续租到 200。
    assert_eq!(
        seam_claim(&seam_db, 100, 1).await,
        legacy_claim(legacy_db.pool(), 100, 1).await
    );
    assert!(seam_renew(&seam_db, 1, Some(100), 200).await);
    assert!(legacy_renew(legacy_db.pool(), 1, Some(100), 200).await);
    assert!(
        !seam_renew(&seam_db, 1, Some(100), 201).await,
        "旧 token 不得再续"
    );
    assert!(!legacy_renew(legacy_db.pool(), 1, Some(100), 201).await);

    // stale token：完成/回写/死信全部 no-op。
    assert!(!seam_finish(&seam_db, 1, Some(100)).await);
    assert!(!legacy_finish(legacy_db.pool(), 1, Some(100)).await);
    assert!(!seam_retry(&seam_db, 1, Some(100), 2, 250, "conn").await);
    assert!(!legacy_retry(legacy_db.pool(), 1, Some(100), 2, 250, "conn").await);
    assert!(!seam_bury(&seam_db, 1, Some(100), 11, "perm").await);
    assert!(!legacy_bury(legacy_db.pool(), 1, Some(100), 11, "perm").await);
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);

    // 正确 token：重试回 pending 并带退避时间。
    assert!(seam_retry(&seam_db, 1, Some(200), 2, 250, "conn").await);
    assert!(legacy_retry(legacy_db.pool(), 1, Some(200), 2, 250, "conn").await);
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);

    // 到期 claim（next_retry_at <= now）→ token=250 → 死信。
    assert_eq!(
        seam_claim(&seam_db, 250, 1).await,
        legacy_claim(legacy_db.pool(), 250, 1).await
    );
    assert!(seam_bury(&seam_db, 1, Some(250), 11, "perm").await);
    assert!(legacy_bury(legacy_db.pool(), 1, Some(250), 11, "perm").await);
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);

    // 重投只对 dead 生效，复位 attempts/token/next_retry/last_error。
    assert!(seam_redeliver(&seam_db, 1, 900).await);
    assert!(legacy_redeliver(legacy_db.pool(), 1, 900).await);
    assert!(!seam_redeliver(&seam_db, 1, 901).await, "非 dead 不得重投");
    assert!(!legacy_redeliver(legacy_db.pool(), 1, 901).await);
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);
}

/// 过期租约回收严格用 `claimed_at < now - lease`；回收行不计 attempts 并保持 id 顺序。
#[tokio::test]
async fn expired_lease_reclaim_and_reclaim_order_match_legacy() {
    let (seam_db, legacy_db) = pair().await;
    for i in 0..4 {
        enqueue(&seam_db, "bkt", "t", &format!("p{i}"), 10).await;
        legacy_enqueue(legacy_db.pool(), "bkt", "t", &format!("p{i}"), 10).await;
    }
    assert_eq!(
        seam_claim(&seam_db, 100, 2).await,
        legacy_claim(legacy_db.pool(), 100, 2).await
    );

    // 严格 `< now - lease`：恰在边界（claimed_at=100，cutoff=100）不回收。
    let boundary_n = {
        let mut conn = seam_db.pool().acquire().await.unwrap();
        SqliteControlTxn::new(&mut conn)
            .event_reclaim_expired(160, 60)
            .await
            .unwrap()
    };
    assert_eq!(boundary_n, 0, "恰在租约边界不得回收");

    let seam_n = {
        let mut conn = seam_db.pool().acquire().await.unwrap();
        SqliteControlTxn::new(&mut conn)
            .event_reclaim_expired(161, 60)
            .await
            .unwrap()
    };
    let legacy_n = sqlx::query(
        "UPDATE event_outbox SET state='pending', claimed_at=NULL, next_retry_at=? \
         WHERE state='sending' AND claimed_at < ?",
    )
    .bind(161)
    .bind(101)
    .execute(legacy_db.pool())
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(seam_n, 2);
    assert_eq!(seam_n, legacy_n);
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);

    // 回收后的两行 next_retry_at=161，与未领取行一起按 id 顺序重新 claim。
    assert_eq!(
        seam_claim(&seam_db, 161, 16).await,
        legacy_claim(legacy_db.pool(), 161, 16).await
    );
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);
}

/// console 死信列表：字段投影、`id` 降序与 limit 一致。
#[tokio::test]
async fn dead_events_match_legacy_console_query() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    for (i, state) in ["pending", "dead", "dead", "sending", "dead"]
        .iter()
        .enumerate()
    {
        sqlx::query(
            "INSERT INTO event_outbox (bucket, target_id, event_json, state, attempts, \
             next_retry_at, created_at, last_error) VALUES (?, 't', ?, ?, ?, 0, ?, ?)",
        )
        .bind(format!("b{i}"))
        .bind(format!("{{\"n\":{i}}}"))
        .bind(state)
        .bind(i as i64)
        .bind(100 + i as i64)
        .bind(if *state == "dead" { Some("boom") } else { None })
        .execute(db.pool())
        .await
        .unwrap();
    }
    let store = SqliteControlStore::new(db.pool().clone());
    for limit in [1, 2, 100] {
        let got: Vec<serde_json::Value> = store
            .dead_events(limit)
            .await
            .unwrap()
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.id, "bucket": r.bucket, "event_json": r.event_json,
                    "attempts": r.attempts, "last_error": r.last_error,
                    "created_at": r.created_at,
                })
            })
            .collect();
        let rows = sqlx::query(
            "SELECT id, bucket, event_json, attempts, last_error, created_at FROM event_outbox \
             WHERE state='dead' ORDER BY id DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(db.pool())
        .await
        .unwrap();
        let want: Vec<serde_json::Value> = rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.get::<i64, _>("id"),
                    "bucket": r.get::<String, _>("bucket"),
                    "event_json": r.get::<String, _>("event_json"),
                    "attempts": r.get::<i64, _>("attempts"),
                    "last_error": r.get::<Option<String>, _>("last_error"),
                    "created_at": r.get::<i64, _>("created_at"),
                })
            })
            .collect();
        assert_eq!(got, want, "limit {limit}");
    }
}

fn strip_test_modules(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim() == "#[cfg(test)]" {
            let mut j = i + 1;
            while j < lines.len() && lines[j].trim().is_empty() {
                j += 1;
            }
            if j < lines.len()
                && lines[j].trim_start().starts_with("mod ")
                && lines[j].contains('{')
            {
                while j < lines.len() {
                    let close = lines[j].starts_with('}');
                    j += 1;
                    if close {
                        break;
                    }
                }
                i = j;
                continue;
            }
        }
        out.push_str(lines[i]);
        out.push('\n');
        i += 1;
    }
    out
}

/// 生产源码的 event_outbox 直写/直读只允许出现在 ControlStore 适配器。
#[test]
fn production_sources_have_no_raw_event_outbox_sql() {
    const ALLOWED: &[&str] = &["event_outbox.rs", "event_outbox_write.rs"];
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut stack = vec![src];
    let mut findings = Vec::new();
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if ALLOWED.contains(&name.as_str()) {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            for (idx, line) in strip_test_modules(&text).lines().enumerate() {
                let lower = line.to_ascii_lowercase();
                let hit = lower.contains("into event_outbox ")
                    || lower.contains("into event_outbox(")
                    || lower.contains("update event_outbox ")
                    || lower.contains("delete from event_outbox ")
                    || lower.contains("from event_outbox ");
                if hit {
                    findings.push(format!("{name}:{}: {}", idx + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "生产源码仍直写/直读 event_outbox（应经 ControlStore）：{findings:#?}"
    );
}
