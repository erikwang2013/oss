//! Slice 4 端到端差分与源码审计：同一操作序列分别走旧 raw SQL 与
//! `ControlTxn` 缝，比较 buckets / bucket_policies / event_configs /
//! replication_rules 与 site changelog/clock；并断言生产源码不再直写这四表。
//!
//! changelog 只比较确定性列（entity/entity_id/op/payload/priority）；`ts` 由
//! `strftime('%s','now')` 生成，跨秒会让两库不一致。

use oss_server::control_store::SqliteControlTxn;
use oss_server::db::{Db, SitePeer};
use sqlx::{Row, SqliteConnection};
use xl_store::control_store::OwnerRef;
use xl_store::control_store::bucket::{BucketCreateOutcome, BucketEntry, BucketTxn};

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

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
        created_at: now(),
        last_seen: None,
        last_error: None,
        raft_node_id: None,
        reconcile_required: false,
    }
}

/// 固定站点 id，保证 changelog 的 origin_site_id 两库一致。
async fn pair() -> (Db, Db) {
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

#[allow(clippy::too_many_arguments)]
async fn legacy_create(
    conn: &mut SqliteConnection,
    name: &str,
    owner_id: Option<i64>,
    created_at: i64,
    mode: &str,
    versioning: &str,
    lock: bool,
    lock_mode: Option<&str>,
    lock_days: Option<i64>,
    encryption: Option<&str>,
) -> BucketCreateOutcome {
    let existing: Option<String> =
        sqlx::query_scalar("SELECT replication_mode FROM buckets WHERE name = ?")
            .bind(name)
            .fetch_optional(&mut *conn)
            .await
            .unwrap();
    if let Some(m) = existing {
        if m != mode {
            return BucketCreateOutcome::ModeConflict;
        }
        sqlx::query(
            "UPDATE buckets SET owner_id = ?, versioning = ?, object_lock = ?, \
             object_lock_default_mode = ?, object_lock_default_days = ?, \
             default_encryption = ?, replication_mode = ?, deleting = 0 WHERE name = ?",
        )
        .bind(owner_id)
        .bind(versioning)
        .bind(i64::from(lock))
        .bind(lock_mode)
        .bind(lock_days)
        .bind(encryption)
        .bind(mode)
        .bind(name)
        .execute(&mut *conn)
        .await
        .unwrap();
        return BucketCreateOutcome::Updated;
    }
    sqlx::query(
        "INSERT INTO buckets \
         (name, owner_id, creation_date, versioning, set_id, object_lock, \
          object_lock_default_mode, object_lock_default_days, default_encryption, \
          replication_mode, deleting) \
         VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 0)",
    )
    .bind(name)
    .bind(owner_id)
    .bind(created_at)
    .bind(versioning)
    .bind(i64::from(lock))
    .bind(lock_mode)
    .bind(lock_days)
    .bind(encryption)
    .bind(mode)
    .execute(&mut *conn)
    .await
    .unwrap();
    BucketCreateOutcome::Created
}

async fn legacy_config(
    conn: &mut SqliteConnection,
    sql: &'static str,
    bucket: &str,
    json: &str,
    ts: i64,
) {
    sqlx::query(sql)
        .bind(bucket)
        .bind(json)
        .bind(ts)
        .execute(&mut *conn)
        .await
        .expect(sql);
}

async fn legacy_delete(conn: &mut SqliteConnection, table: &str, bucket: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {table} WHERE bucket = ?"
    )))
    .bind(bucket)
    .execute(&mut *conn)
    .await
    .unwrap();
}

// ---------- 导出与比较 ----------

type BucketRow = (
    String,
    Option<i64>,
    i64,
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    i64,
    i64,
);
type ConfigRow = (String, String, i64);
type ClRow = (String, String, String, String, i64);
type ClockRow = (String, String, i64, String);

#[derive(Debug, PartialEq, Eq)]
struct Dump {
    buckets: Vec<BucketRow>,
    policies: Vec<ConfigRow>,
    events: Vec<ConfigRow>,
    rules: Vec<ConfigRow>,
    changelog: Vec<ClRow>,
    clocks: Vec<ClockRow>,
}

async fn dump(db: &Db, since_seq: i64) -> Dump {
    let buckets = sqlx::query(
        "SELECT name, owner_id, creation_date, versioning, object_lock, default_encryption, \
         object_lock_default_mode, object_lock_default_days, replication_mode, deleting, set_id \
         FROM buckets ORDER BY name",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("name"),
            r.get("owner_id"),
            r.get("creation_date"),
            r.get("versioning"),
            r.get("object_lock"),
            r.get("default_encryption"),
            r.get("object_lock_default_mode"),
            r.get("object_lock_default_days"),
            r.get("replication_mode"),
            r.get("deleting"),
            r.get("set_id"),
        )
    })
    .collect();
    let fetch = |sql: &'static str| async move {
        sqlx::query(sql)
            .fetch_all(db.pool())
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get("bucket"), r.get("json"), r.get("updated_at")))
            .collect::<Vec<ConfigRow>>()
    };
    let policies = fetch(
        "SELECT bucket, policy_json AS json, updated_at FROM bucket_policies ORDER BY bucket",
    )
    .await;
    let events =
        fetch("SELECT bucket, config_json AS json, updated_at FROM event_configs ORDER BY bucket")
            .await;
    let rules = fetch(
        "SELECT bucket, rules_json AS json, updated_at FROM replication_rules ORDER BY bucket",
    )
    .await;
    let cl_rows = sqlx::query(
        "SELECT entity, entity_id, op, payload, priority, ts FROM site_changelog \
         WHERE seq > ? ORDER BY seq",
    )
    .bind(since_seq)
    .fetch_all(db.pool())
    .await
    .unwrap();
    // 时钟 ts 必须等于同库对应 changelog 行的 ts（而非跨库绝对时间）。
    let mut cl_ts = std::collections::HashMap::new();
    for r in &cl_rows {
        cl_ts.insert(
            (
                r.get::<String, _>("entity"),
                r.get::<String, _>("entity_id"),
            ),
            r.get::<i64, _>("ts"),
        );
    }
    let changelog = cl_rows
        .iter()
        .map(|r| {
            (
                r.get("entity"),
                r.get("entity_id"),
                r.get("op"),
                r.get("payload"),
                r.get("priority"),
            )
        })
        .collect();
    let clocks = sqlx::query(
        "SELECT entity, entity_id, priority, origin_site_id, ts FROM site_entity_clock \
         ORDER BY entity, entity_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        let entity: String = r.get("entity");
        let id: String = r.get("entity_id");
        let ts: i64 = r.get("ts");
        if let Some(cl) = cl_ts.get(&(entity.clone(), id.clone())) {
            assert_eq!(ts, *cl, "{entity}/{id} clock 必须来自 changelog 行");
        }
        (entity, id, r.get("priority"), r.get("origin_site_id"))
    })
    .collect();
    Dump {
        buckets,
        policies,
        events,
        rules,
        changelog,
        clocks,
    }
}

async fn max_seq(db: &Db) -> i64 {
    sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM site_changelog")
        .fetch_one(db.pool())
        .await
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn entry(
    name: &str,
    mode: &str,
    versioning: &str,
    lock: bool,
    lock_mode: Option<&str>,
    lock_days: Option<i64>,
    encryption: Option<&str>,
    created: i64,
) -> BucketEntry {
    BucketEntry {
        name: name.into(),
        owner: OwnerRef::Admin,
        creation_date: created,
        versioning: versioning.into(),
        object_lock: lock,
        default_encryption: encryption.map(Into::into),
        object_lock_default_mode: lock_mode.map(Into::into),
        object_lock_default_days: lock_days,
        replication_mode: mode.into(),
        deleting: false,
    }
}

// ---------- 差分 ----------

#[tokio::test]
async fn bucket_writes_match_legacy_sql() {
    let (a, b) = pair().await;
    let (start_a, start_b) = (max_seq(&a).await, max_seq(&b).await);

    // 1. 新建；2. 同模式更新（creation_date 不覆盖）；3. 模式冲突零写。
    let mut ca = a.pool().acquire().await.unwrap();
    assert_eq!(
        legacy_create(
            &mut ca, "b1", None, 100, "async", "Off", false, None, None, None
        )
        .await,
        BucketCreateOutcome::Created
    );
    assert_eq!(
        legacy_create(
            &mut ca,
            "b1",
            None,
            999,
            "async",
            "Enabled",
            true,
            Some("GOVERNANCE"),
            Some(5),
            Some("AES256")
        )
        .await,
        BucketCreateOutcome::Updated
    );
    assert_eq!(
        legacy_create(
            &mut ca, "b1", None, 1, "sync", "Off", false, None, None, None
        )
        .await,
        BucketCreateOutcome::ModeConflict
    );
    drop(ca);

    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        assert_eq!(
            t.bucket_create(&entry("b1", "async", "Off", false, None, None, None, 100))
                .await
                .unwrap(),
            BucketCreateOutcome::Created
        );
        assert_eq!(
            t.bucket_create(&entry(
                "b1",
                "async",
                "Enabled",
                true,
                Some("GOVERNANCE"),
                Some(5),
                Some("AES256"),
                999
            ))
            .await
            .unwrap(),
            BucketCreateOutcome::Updated
        );
        assert_eq!(
            t.bucket_create(&entry("b1", "sync", "Off", false, None, None, None, 1))
                .await
                .unwrap(),
            BucketCreateOutcome::ModeConflict
        );
    }
    tx.commit().await.unwrap();

    // 4-8. 点写（含缺失桶返回 false）。
    let mut ca = a.pool().acquire().await.unwrap();
    sqlx::query("UPDATE buckets SET deleting = ? WHERE name = ?")
        .bind(1i64)
        .bind("b1")
        .execute(&mut *ca)
        .await
        .unwrap();
    sqlx::query("UPDATE buckets SET versioning = ? WHERE name = ?")
        .bind("Suspended")
        .bind("b1")
        .execute(&mut *ca)
        .await
        .unwrap();
    sqlx::query("UPDATE buckets SET default_encryption = ? WHERE name = ?")
        .bind("aws:kms")
        .bind("b1")
        .execute(&mut *ca)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE buckets SET object_lock = ?, object_lock_default_mode = ?, \
         object_lock_default_days = ? WHERE name = ?",
    )
    .bind(0i64)
    .bind(None::<String>)
    .bind(None::<i64>)
    .bind("b1")
    .execute(&mut *ca)
    .await
    .unwrap();
    sqlx::query("UPDATE buckets SET default_encryption = NULL WHERE name = ?")
        .bind("b1")
        .execute(&mut *ca)
        .await
        .unwrap();
    let n = sqlx::query("UPDATE buckets SET deleting = ? WHERE name = ?")
        .bind(1i64)
        .bind("nope")
        .execute(&mut *ca)
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(n, 0, "缺失桶 UPDATE 零行");
    drop(ca);

    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        assert!(t.bucket_set_deleting("b1", true).await.unwrap());
        assert!(t.bucket_set_versioning("b1", "Suspended").await.unwrap());
        assert!(
            t.bucket_set_object_lock("b1", false, None, None)
                .await
                .unwrap()
        );
        assert!(
            t.bucket_set_default_encryption("b1", "aws:kms")
                .await
                .unwrap()
        );
        assert!(t.bucket_clear_default_encryption("b1").await.unwrap());
        assert!(!t.bucket_set_deleting("nope", true).await.unwrap());
    }
    tx.commit().await.unwrap();

    // 9. 三组配置 upsert 两次（覆盖 ON CONFLICT 路径）。
    let mut ca = a.pool().acquire().await.unwrap();
    for (sql, json, ts) in [
        (
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET policy_json = excluded.policy_json, \
             updated_at = excluded.updated_at",
            "{\"v\":1}",
            11,
        ),
        (
            "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET config_json = excluded.config_json, \
             updated_at = excluded.updated_at",
            "[]",
            12,
        ),
        (
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET rules_json = excluded.rules_json, \
             updated_at = excluded.updated_at",
            "{\"r\":1}",
            13,
        ),
    ] {
        legacy_config(&mut ca, sql, "b1", json, ts).await;
    }
    // 第二次写走 UPDATE 参考：旧 `ON CONFLICT DO UPDATE` 与 site 触发器组合在
    // SQLite 下误报 UNIQUE（见 bucket_write.rs 注释），不可作为差分基线。
    let n = sqlx::query(
        "UPDATE bucket_policies SET policy_json = ?, updated_at = ? \
         WHERE bucket = ?",
    )
    .bind("{\"v\":2}")
    .bind(14)
    .bind("b1")
    .execute(&mut *ca)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(n, 1);
    drop(ca);

    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        t.bucket_policy_set("b1", "{\"v\":1}", 11).await.unwrap();
        t.bucket_policy_set("b1", "{\"v\":2}", 14).await.unwrap();
        t.bucket_notification_set("b1", "[]", 12).await.unwrap();
        t.bucket_replication_rules_set("b1", "{\"r\":1}", 13)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    // 10. 第二桶；11. 配置删除。
    let mut ca = a.pool().acquire().await.unwrap();
    assert_eq!(
        legacy_create(
            &mut ca, "b2", None, 200, "sync", "Off", false, None, None, None
        )
        .await,
        BucketCreateOutcome::Created
    );
    legacy_delete(&mut ca, "bucket_policies", "b1").await;
    legacy_delete(&mut ca, "event_configs", "b1").await;
    legacy_delete(&mut ca, "replication_rules", "b1").await;
    drop(ca);

    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        assert_eq!(
            t.bucket_create(&entry("b2", "sync", "Off", false, None, None, None, 200))
                .await
                .unwrap(),
            BucketCreateOutcome::Created
        );
        t.bucket_policy_delete("b1").await.unwrap();
        t.bucket_notification_delete("b1").await.unwrap();
        t.bucket_replication_rules_delete("b1").await.unwrap();
    }
    tx.commit().await.unwrap();

    assert_eq!(dump(&a, start_a).await, dump(&b, start_b).await);
}

#[tokio::test]
async fn rollback_matches_legacy_seed() {
    let (a, b) = pair().await;
    let mut ca = a.pool().acquire().await.unwrap();
    legacy_create(
        &mut ca, "b1", None, 1, "async", "Off", false, None, None, None,
    )
    .await;
    drop(ca);
    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        t.bucket_create(&entry("b1", "async", "Off", false, None, None, None, 1))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        t.bucket_set_versioning("b1", "Enabled").await.unwrap();
        t.bucket_policy_set("b1", "{}", 1).await.unwrap();
        t.bucket_notification_set("b1", "[]", 1).await.unwrap();
        t.bucket_replication_rules_set("b1", "[]", 1).await.unwrap();
    }
    tx.rollback().await.unwrap();

    assert_eq!(dump(&a, 0).await, dump(&b, 0).await);
}

// ---------- 源码审计 ----------

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
            // 仅块状测试模块（`mod x {`）跳过；`mod x;` / 函数属性不跳。
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

#[test]
fn production_sources_have_no_raw_bucket_sql() {
    const ALLOWED: &[&str] = &[
        "bucket.rs",
        "bucket_write.rs",
        "bucket_snapshot.rs",
        // 账户适配器的桶计数/桶名查询属 IAM/账户切片。
        "account.rs",
        "account_write.rs",
        "db.rs",
        "site.rs",
        // 跨域 FK 置空（user 删除级联 buckets/uploads），D7，归桶/上传切片。
        "apply.rs",
        "cluster_control.rs",
        "state.rs",
    ];
    const TABLES: &[&str] = &[
        "buckets",
        "bucket_policies",
        "event_configs",
        "replication_rules",
    ];

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
            if path.extension().and_then(|e| e.to_str()) != Some("rs")
                || path.file_name().and_then(|n| n.to_str()) == Some("tests.rs")
            {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if ALLOWED.contains(&name.as_str()) {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            for (idx, line) in strip_test_modules(&text).lines().enumerate() {
                let lower = line.to_ascii_lowercase();
                let hit = TABLES.iter().any(|t| {
                    lower.contains(&format!("from {t} "))
                        || lower.contains(&format!("into {t} "))
                        || lower.contains(&format!("into {t}("))
                        || lower.contains(&format!("update {t} "))
                        || lower.contains(&format!("delete from {t} "))
                });
                if hit {
                    findings.push(format!("{name}:{}: {}", idx + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "生产源码仍直写桶控制表（应经 ControlTxn 或更新 D7 白名单）：{findings:#?}"
    );
}

#[test]
fn removed_bucket_raw_helpers_stay_removed() {
    const NAMES: &[&str] = &[
        "create_bucket_tx",
        "set_bucket_policy_raw",
        "delete_bucket_policy_raw",
        "set_bucket_notification_raw",
        "clear_bucket_notification_raw",
        "set_replication_rules_raw",
        "clear_replication_rules_raw",
        "set_bucket_default_encryption_raw",
        "clear_bucket_default_encryption_raw",
        "set_bucket_deleting_raw",
        "set_bucket_versioning_raw",
        "set_bucket_object_lock_raw",
    ];
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut stack = vec![root];
    let mut findings = Vec::new();
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let text = std::fs::read_to_string(&path).unwrap();
                for name in NAMES {
                    if text.contains(&format!("fn {name}")) || text.contains(&format!("{name}(")) {
                        findings.push(format!("{}: {name}", path.display()));
                    }
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "已删除的 raw helper 仍被引用：{findings:#?}"
    );
}
