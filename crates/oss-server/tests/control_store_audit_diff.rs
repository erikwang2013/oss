//! audit_log 迁移差分：批量插入顺序、控制台过滤、保留策略删除与迁移前 SQL 等价。

use oss_server::control_store::{SqliteControlStore, SqliteControlTxn};
use oss_server::db::{AuditFilter, Db};
use sqlx::{Row, SqlitePool};
use xl_store::control_store::audit::{AuditQuery, AuditStore, AuditTxn, NewAuditRecord};

fn records() -> Vec<NewAuditRecord> {
    vec![
        NewAuditRecord {
            ts: 1000,
            uid: 1,
            op: "PutObject".into(),
            bucket: Some("b1".into()),
            object_key: Some("a/x".into()),
            decision: "allow".into(),
            detail: Some("d1".into()),
        },
        NewAuditRecord {
            ts: 2000,
            uid: 2,
            op: "GetObject".into(),
            bucket: Some("b1".into()),
            object_key: Some("a/y".into()),
            decision: "deny".into(),
            detail: None,
        },
        NewAuditRecord {
            ts: 3000,
            uid: 2,
            op: "DeleteObject".into(),
            bucket: Some("b2".into()),
            object_key: Some("c/z".into()),
            decision: "allow".into(),
            detail: Some("d3".into()),
        },
        NewAuditRecord {
            ts: 4000,
            uid: 3,
            op: "ListBucket".into(),
            bucket: None,
            object_key: None,
            decision: "deny".into(),
            detail: None,
        },
    ]
}

async fn legacy_insert(pool: &SqlitePool, rows: &[NewAuditRecord]) {
    let mut tx = pool.begin().await.unwrap();
    for r in rows {
        sqlx::query(
            "INSERT INTO audit_log (ts, uid, op, bucket, object_key, decision, detail) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(r.ts)
        .bind(r.uid)
        .bind(&r.op)
        .bind(&r.bucket)
        .bind(&r.object_key)
        .bind(&r.decision)
        .bind(&r.detail)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}

async fn seam_insert(db: &Db, rows: &[NewAuditRecord]) {
    let mut tx = db.pool().begin().await.unwrap();
    SqliteControlTxn::new(&mut tx)
        .audit_insert_batch(rows)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn dump(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(id, ts, uid, op, bucket, object_key, decision, detail) \
         FROM audit_log ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

/// 迁移前 `Db::list_audit` 的完整副本（动态 WHERE、绑定顺序、JSON 投影）。
async fn legacy_list(pool: &SqlitePool, f: &AuditFilter<'_>) -> Vec<serde_json::Value> {
    let mut sql = String::from(
        "SELECT id, ts, uid, op, bucket, object_key, decision, detail FROM audit_log \
         WHERE 1=1",
    );
    for (present, clause) in [
        (f.decision.is_some(), " AND decision = ?"),
        (f.from.is_some(), " AND ts >= ?"),
        (f.to.is_some(), " AND ts <= ?"),
        (f.uid.is_some(), " AND uid = ?"),
        (f.op.is_some(), " AND op = ?"),
        (f.bucket.is_some(), " AND bucket = ?"),
    ] {
        if present {
            sql.push_str(clause);
        }
    }
    if f.key_prefix.is_some() {
        sql.push_str(" AND substr(object_key, 1, length(?)) = ?");
    }
    sql.push_str(" ORDER BY id DESC LIMIT ?");

    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
    if let Some(v) = f.decision {
        q = q.bind(v);
    }
    if let Some(v) = f.from {
        q = q.bind(v);
    }
    if let Some(v) = f.to {
        q = q.bind(v);
    }
    if let Some(v) = f.uid {
        q = q.bind(v);
    }
    if let Some(v) = f.op {
        q = q.bind(v);
    }
    if let Some(v) = f.bucket {
        q = q.bind(v);
    }
    if let Some(v) = f.key_prefix {
        q = q.bind(v).bind(v);
    }
    let rows = q.bind(f.limit).fetch_all(pool).await.unwrap();
    rows.iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<i64, _>("id"),
                "ts": r.get::<i64, _>("ts"),
                "uid": r.get::<i64, _>("uid"),
                "op": r.get::<String, _>("op"),
                "bucket": r.get::<Option<String>, _>("bucket"),
                "object_key": r.get::<Option<String>, _>("object_key"),
                "decision": r.get::<String, _>("decision"),
                "detail": r.get::<Option<String>, _>("detail"),
            })
        })
        .collect()
}

async fn legacy_purge(pool: &SqlitePool, cutoff: i64, limit: i64) -> u64 {
    sqlx::query(
        "DELETE FROM audit_log WHERE id IN \
         (SELECT id FROM audit_log WHERE ts < ? ORDER BY ts LIMIT ?)",
    )
    .bind(cutoff)
    .bind(limit.max(1))
    .execute(pool)
    .await
    .unwrap()
    .rows_affected()
}

/// 批量插入逐行同事务、保持入参顺序（AUTOINCREMENT id 单调跟随 ts 入参序）。
#[tokio::test]
async fn batch_insert_matches_legacy_row_order() {
    let seam_db = Db::connect("sqlite::memory:").await.unwrap();
    let legacy_db = Db::connect("sqlite::memory:").await.unwrap();
    seam_insert(&seam_db, &records()).await;
    legacy_insert(legacy_db.pool(), &records()).await;
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);

    // 空批次是 no-op。
    seam_insert(&seam_db, &[]).await;
    assert_eq!(dump(seam_db.pool()).await, dump(legacy_db.pool()).await);
}

/// 控制台查询：每种过滤组合与迁移前动态 SQL 的结果逐值一致。
#[tokio::test]
async fn audit_filters_match_legacy_query() {
    let db = Db::connect("sqlite::memory:").await.unwrap();
    seam_insert(&db, &records()).await;
    let store = SqliteControlStore::new(db.pool().clone());

    let filters = [
        AuditFilter {
            limit: 100,
            decision: None,
            from: None,
            to: None,
            uid: None,
            op: None,
            bucket: None,
            key_prefix: None,
        },
        AuditFilter {
            limit: 2,
            decision: Some("allow"),
            from: None,
            to: None,
            uid: None,
            op: None,
            bucket: None,
            key_prefix: None,
        },
        AuditFilter {
            limit: 100,
            decision: None,
            from: Some(2000),
            to: Some(3000),
            uid: None,
            op: None,
            bucket: None,
            key_prefix: None,
        },
        AuditFilter {
            limit: 100,
            decision: None,
            from: None,
            to: None,
            uid: Some(2),
            op: None,
            bucket: None,
            key_prefix: None,
        },
        AuditFilter {
            limit: 100,
            decision: None,
            from: None,
            to: None,
            uid: None,
            op: Some("PutObject"),
            bucket: None,
            key_prefix: None,
        },
        AuditFilter {
            limit: 100,
            decision: None,
            from: None,
            to: None,
            uid: None,
            op: None,
            bucket: Some("b1"),
            key_prefix: None,
        },
        AuditFilter {
            limit: 100,
            decision: None,
            from: None,
            to: None,
            uid: None,
            op: None,
            bucket: None,
            key_prefix: Some("a/"),
        },
        AuditFilter {
            limit: 1,
            decision: Some("allow"),
            from: Some(1000),
            to: Some(4000),
            uid: Some(1),
            op: Some("PutObject"),
            bucket: Some("b1"),
            key_prefix: Some("a/x"),
        },
    ];

    for f in filters {
        let got = store
            .audit_page(&AuditQuery {
                limit: f.limit,
                decision: f.decision.map(str::to_owned),
                from: f.from,
                to: f.to,
                uid: f.uid,
                op: f.op.map(str::to_owned),
                bucket: f.bucket.map(str::to_owned),
                key_prefix: f.key_prefix.map(str::to_owned),
            })
            .await
            .unwrap();
        let got: Vec<serde_json::Value> = got
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.id, "ts": r.ts, "uid": r.uid, "op": r.op,
                    "bucket": r.bucket, "object_key": r.object_key,
                    "decision": r.decision, "detail": r.detail,
                })
            })
            .collect();
        assert_eq!(got, legacy_list(db.pool(), &f).await, "filter {f:?}");
    }
}

/// 保留策略：返回删除数、剩余行与 id 子查询圈定顺序一致；limit 0 仍删 1 行。
#[tokio::test]
async fn audit_purge_matches_legacy_delete() {
    let seam_db = Db::connect("sqlite::memory:").await.unwrap();
    let legacy_db = Db::connect("sqlite::memory:").await.unwrap();
    seam_insert(&seam_db, &records()).await;
    legacy_insert(legacy_db.pool(), &records()).await;

    let mut conn = seam_db.pool().acquire().await.unwrap();
    let seam_n = SqliteControlTxn::new(&mut conn)
        .audit_delete_older_than(3500, 2)
        .await
        .unwrap();
    drop(conn);
    let legacy_n = legacy_purge(legacy_db.pool(), 3500, 2).await;
    assert_eq!(seam_n, legacy_n);
    assert_eq!(
        dump(seam_db.pool()).await,
        dump(legacy_db.pool()).await,
        "剩余行（含 id）必须一致"
    );

    // 单条旧行 + limit 0：迁移前 `.max(1)` 语义保持。
    let seam_db = Db::connect("sqlite::memory:").await.unwrap();
    let legacy_db = Db::connect("sqlite::memory:").await.unwrap();
    seam_insert(&seam_db, &records()[..1]).await;
    legacy_insert(legacy_db.pool(), &records()[..1]).await;
    let mut conn = seam_db.pool().acquire().await.unwrap();
    let seam_n = SqliteControlTxn::new(&mut conn)
        .audit_delete_older_than(2000, 0)
        .await
        .unwrap();
    drop(conn);
    assert_eq!(seam_n, 1);
    assert_eq!(seam_n, legacy_purge(legacy_db.pool(), 2000, 0).await);
    assert!(dump(seam_db.pool()).await.is_empty());
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

/// 生产源码的 audit_log 直写只允许出现在 ControlStore 适配器。
#[test]
fn production_sources_have_no_raw_audit_sql() {
    const ALLOWED: &[&str] = &["audit.rs", "audit_write.rs"];
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
                let hit = lower.contains("into audit_log ")
                    || lower.contains("into audit_log(")
                    || lower.contains("update audit_log ")
                    || lower.contains("delete from audit_log ")
                    || lower.contains("from audit_log ");
                if hit {
                    findings.push(format!("{name}:{}: {}", idx + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "生产源码仍直写/直读 audit_log（应经 ControlStore）：{findings:#?}"
    );
}
