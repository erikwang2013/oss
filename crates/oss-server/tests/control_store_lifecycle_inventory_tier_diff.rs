//! 切片 3 端到端差分与源码审计：同一操作序列分别走 `Db`/`ControlTxn` 缝与
//! 测试内 legacy raw SQL，比较 7 张表；并断言生产源码不再直写这些表
//! （决策 8 的允许例外带 `audit-allow` 注释）。

use std::sync::Arc;

use oss_server::control_store::SqliteControlTxn;
use oss_server::db::Db;
use oss_server::raft::FencingEpoch;
use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryCursorRow, InventoryIncrementalRow, InventoryRunRow,
    InventoryTxn,
};
use xl_store::control_store::lifecycle::{LifecycleCursorRow, LifecycleRuleEntry, LifecycleTxn};
use xl_store::control_store::tier::{TierRestoreRow, TierTxn};

async fn fresh() -> Arc<Db> {
    Arc::new(Db::connect("sqlite::memory:").await.unwrap())
}

type Dump = (
    Vec<LifecycleRuleEntry>,
    Vec<InventoryConfigEntry>,
    Vec<InventoryRunRow>,
    Vec<InventoryCursorRow>,
    Vec<InventoryIncrementalRow>,
    Vec<TierRestoreRow>,
    Vec<LifecycleCursorRow>,
);

async fn dump(db: &Db) -> Dump {
    let mut tx = db.pool().begin().await.unwrap();
    let mut txn = SqliteControlTxn::new(&mut tx);
    let out = (
        txn.all_lifecycle_rules().await.unwrap(),
        txn.inventory_configs().await.unwrap(),
        txn.inventory_runs().await.unwrap(),
        txn.inventory_cursors().await.unwrap(),
        txn.inventory_incrementals().await.unwrap(),
        txn.tier_restores().await.unwrap(),
        txn.lifecycle_cursors().await.unwrap(),
    );
    tx.rollback().await.unwrap();
    out
}

fn rule(bucket: &str, rule_id: &str) -> LifecycleRuleEntry {
    LifecycleRuleEntry {
        bucket: bucket.into(),
        prefix: "logs/".into(),
        rule_id: Some(rule_id.into()),
        expire_days: 30,
        expiration_days: None,
        transition_days: None,
        transition_class: None,
        transition_tier: None,
        enabled: true,
        updated_at: 7,
        config_json: Some(r#"{"filter":{"prefix":"logs/"}}"#.into()),
    }
}

async fn seed_via_seam(db: &Db, bucket: &str, owner: i64) {
    db.set_lifecycle_rules(bucket, &[rule(bucket, "r1")], 7)
        .await
        .unwrap();
    db.inventory_upsert_config(bucket, "cfg", owner, r#"{"destination":"d"}"#, 8)
        .await
        .unwrap();
    db.inventory_create_run(
        bucket,
        "cfg",
        "k",
        9,
        FencingEpoch {
            term: 2,
            leader: 1,
        },
    )
    .await
    .unwrap()
    .expect("run created");
    db.inventory_cursor_set(
        bucket,
        "cfg",
        8,
        Some("42"),
        10,
        FencingEpoch {
            term: 2,
            leader: 1,
        },
    )
    .await
    .unwrap();
    db.inventory_incremental_set(bucket, "cfg", true, 11)
        .await
        .unwrap();
    let mut tx = db.pool().begin().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut tx);
        txn.tier_restore_upsert(&TierRestoreRow {
            bucket: bucket.into(),
            key: "obj".into(),
            version_id: "v1".into(),
            expires_at: 99,
            job_id: None,
            state: "restored".into(),
            created_at: 12,
            fence_term: 2,
            fence_leader: 1,
        })
        .await
        .unwrap();
        txn.lifecycle_cursor_upsert(&LifecycleCursorRow {
            bucket: bucket.into(),
            rule_id: "r1".into(),
            stage: "scan".into(),
            cursor: "100".into(),
            updated_at: 13,
            fence_term: 2,
            fence_leader: 1,
        })
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}

async fn seed_via_legacy(db: &Db, bucket: &str, owner: i64) {
    sqlx::query(
        "INSERT INTO lifecycle_rules (bucket, prefix, rule_id, expire_days, enabled, updated_at, \
         config_json) VALUES (?, 'logs/', 'r1', 30, 1, 7, '{\"filter\":{\"prefix\":\"logs/\"}}')",
    )
    .bind(bucket)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) \
         VALUES (?, 'cfg', ?, '{\"destination\":\"d\"}', 8)",
    )
    .bind(bucket)
    .bind(owner)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO inventory_runs (bucket, config_id, run_key, started_at, state, created_at, \
         fence_term, fence_leader) VALUES (?, 'cfg', 'k', 0, 'running', 9, 2, 1)",
    )
    .bind(bucket)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, updated_at, \
         fence_term, fence_leader) VALUES (?, 'cfg', '42', 8, 10, 2, 1)",
    )
    .bind(bucket)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
         VALUES (?, 'cfg', 1, 11)",
    )
    .bind(bucket)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tier_restores (bucket, key, version_id, expires_at, job_id, state, \
         created_at, fence_term, fence_leader) VALUES (?, 'obj', 'v1', 99, NULL, 'restored', 12, 2, 1)",
    )
    .bind(bucket)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at, fence_term, \
         fence_leader) VALUES (?, 'r1', 'scan', '100', 13, 2, 1)",
    )
    .bind(bucket)
    .execute(db.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn end_to_end_seam_and_legacy_match_for_core_operations() {
    let seam_db = fresh().await;
    let ref_db = fresh().await;
    let seam_owner = seam_db.create_user("owner", "pw", false).await.unwrap();
    let ref_owner = ref_db.create_user("owner", "pw", false).await.unwrap();
    assert_eq!(seam_owner, ref_owner);

    for bucket in ["b1", "b2"] {
        seed_via_seam(&seam_db, bucket, seam_owner).await;
        seed_via_legacy(&ref_db, bucket, ref_owner).await;
    }
    assert_eq!(dump(&seam_db).await, dump(&ref_db).await, "写入端逐行一致");

    let mut conn = seam_db.pool().acquire().await.unwrap();
    Db::clear_bucket_m8_tx(&mut conn, "b1").await.unwrap();
    drop(conn);
    let mut conn = ref_db.pool().acquire().await.unwrap();
    sqlx::query("DELETE FROM inventory_runs WHERE bucket = 'b1'")
        .execute(&mut *conn)
        .await
        .unwrap();
    for sql in [
        "DELETE FROM inventory_configs WHERE bucket = 'b1'",
        "DELETE FROM inventory_cursors WHERE bucket = 'b1'",
        "DELETE FROM inventory_incremental WHERE bucket = 'b1'",
        "DELETE FROM tier_restores WHERE bucket = 'b1'",
        "DELETE FROM lifecycle_rules WHERE bucket = 'b1'",
        "DELETE FROM lifecycle_cursors WHERE bucket = 'b1'",
    ] {
        sqlx::query(sql).execute(&mut *conn).await.unwrap();
    }
    drop(conn);

    let seam_dump = dump(&seam_db).await;
    assert_eq!(seam_dump, dump(&ref_db).await, "级联后逐行一致");
    assert!(seam_dump.0.iter().all(|r| r.bucket == "b2"), "b1 规则已删");
    assert!(!seam_dump.2.is_empty(), "b2 run 保留");
    assert_eq!(seam_dump.2[0].bucket, "b2");
}

/// 生产源码不得再直写这 7 张表；`audit-allow` 行（决策 8/迁移）除外。
#[test]
fn production_sources_have_no_raw_sql_for_migrated_tables() {
    const FILES: &[(&str, &str)] = &[
        ("db.rs", include_str!("../src/db.rs")),
        ("raft/apply.rs", include_str!("../src/raft/apply.rs")),
        (
            "raft/apply/inventory.rs",
            include_str!("../src/raft/apply/inventory.rs"),
        ),
        (
            "raft/apply/tier_restore.rs",
            include_str!("../src/raft/apply/tier_restore.rs"),
        ),
        (
            "raft/apply/lifecycle.rs",
            include_str!("../src/raft/apply/lifecycle.rs"),
        ),
        ("raft/data.rs", include_str!("../src/raft/data.rs")),
        (
            "raft/runstate/store.rs",
            include_str!("../src/raft/runstate/store.rs"),
        ),
        (
            "job_state/inventory.rs",
            include_str!("../src/job_state/inventory.rs"),
        ),
        (
            "job_state/lifecycle.rs",
            include_str!("../src/job_state/lifecycle.rs"),
        ),
        (
            "job_state/restore.rs",
            include_str!("../src/job_state/restore.rs"),
        ),
        ("site.rs", include_str!("../src/site.rs")),
    ];
    const TABLES: &[&str] = &[
        "inventory_runs",
        "inventory_configs",
        "inventory_cursors",
        "inventory_incremental",
        "tier_restores",
        "lifecycle_rules",
        "lifecycle_cursors",
    ];

    fn strip_test_modules(text: &str) -> String {
        let lines: Vec<&str> = text.lines().collect();
        let mut out = String::new();
        let mut i = 0;
        while i < lines.len() {
            let line = lines[i];
            if line.contains("#[cfg(test)]") {
                // 跳到紧随其后的 mod 块结束：从首个 '{' 开始做花括号配对。
                let mut depth = 0i32;
                let mut seen = false;
                while i < lines.len() {
                    let l = lines[i];
                    for ch in l.chars() {
                        if ch == '{' {
                            depth += 1;
                            seen = true;
                        } else if ch == '}' {
                            depth -= 1;
                        }
                    }
                    i += 1;
                    if seen && depth <= 0 {
                        break;
                    }
                }
                continue;
            }
            out.push_str(line);
            out.push('\n');
            i += 1;
        }
        out
    }

    let mut findings = Vec::new();
    for (name, text) in FILES {
        let prod = strip_test_modules(text);
        let prod_lines: Vec<&str> = prod.lines().collect();
        for (idx, line) in prod_lines.iter().enumerate() {
            let lower = line.to_ascii_lowercase();
            let hit = TABLES.iter().any(|t| {
                lower.contains(&format!("from {t} "))
                    || lower.contains(&format!("into {t} "))
                    || lower.contains(&format!("into {t}("))
                    || lower.contains(&format!("update {t} "))
                    || lower.contains(&format!("delete from {t} "))
            });
            if !hit {
                continue;
            }
            let allow = prod_lines[idx.saturating_sub(6)..=idx]
                .iter()
                .any(|l| l.contains("audit-allow"));
            if !allow {
                findings.push(format!("{name}:{}: {}", idx + 1, line.trim()));
            }
        }
    }
    assert!(
        findings.is_empty(),
        "生产源码仍直写已迁移表（确认应经 ControlTxn 或加 audit-allow）：{findings:#?}"
    );
}
