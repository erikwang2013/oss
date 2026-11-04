//! IAM 写路径差分、site 触发器回归与源码审计。
//!
//! A 库执行测试内旧 SQL，B 库走 `IamTxn`；比较 users / access_keys /
//! permissions 全列与 site_changelog/clock 确定性列。changelog `ts` 由
//! `strftime` 生成，只校验同库时钟与 changelog 一致，不跨库比较。

mod common;

#[path = "common/iam_diff_support.rs"]
mod support;

use common::state_with_site;
use oss_server::config::SiteToml;
use oss_server::control_store::SqliteControlTxn;
use oss_server::db::Db;
use sqlx::Row;
use support::{legacy_user, pair, run_legacy, run_seam};
use xl_store::control_store::iam::IamTxn;

// ---------- 导出与比较 ----------

type UserD = (i64, String, String, i64, String, i64);
type AkD = (
    String,
    String,
    i64,
    String,
    Option<i64>,
    i64,
    Option<String>,
    Option<String>,
    String,
);
type PermD = (i64, String, i64, i64, i64);
type ClD = (String, String, String, String, i64);
type ClockD = (String, String, i64, String);

#[derive(Debug, PartialEq, Eq)]
struct Dump {
    users: Vec<UserD>,
    access_keys: Vec<AkD>,
    permissions: Vec<PermD>,
    changelog: Vec<ClD>,
    clocks: Vec<ClockD>,
}

async fn dump(db: &Db) -> Dump {
    let users = sqlx::query(
        "SELECT id, username, password_hash, is_admin, status, created_at \
         FROM users ORDER BY id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("id"),
            r.get("username"),
            r.get("password_hash"),
            r.get("is_admin"),
            r.get("status"),
            r.get("created_at"),
        )
    })
    .collect();
    let access_keys = sqlx::query(
        "SELECT access_key, secret_key, user_id, status, expires_at, created_at, \
         parent_ak, session_policy, kind FROM access_keys ORDER BY access_key",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("access_key"),
            r.get("secret_key"),
            r.get("user_id"),
            r.get("status"),
            r.get("expires_at"),
            r.get("created_at"),
            r.get("parent_ak"),
            r.get("session_policy"),
            r.get("kind"),
        )
    })
    .collect();
    let permissions = sqlx::query(
        "SELECT user_id, bucket, can_read, can_write, can_delete \
         FROM permissions ORDER BY user_id, bucket",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("user_id"),
            r.get("bucket"),
            r.get("can_read"),
            r.get("can_write"),
            r.get("can_delete"),
        )
    })
    .collect();
    let cl_rows = sqlx::query(
        "SELECT entity, entity_id, op, payload, priority, ts FROM site_changelog ORDER BY seq",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
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
        users,
        access_keys,
        permissions,
        changelog,
        clocks,
    }
}

#[tokio::test]
async fn iam_writes_match_legacy_sql() {
    let (a, b) = pair().await;
    let mut ca = a.pool().acquire().await.unwrap();
    run_legacy(&mut ca).await;
    drop(ca);
    let mut cb = b.pool().acquire().await.unwrap();
    run_seam(&mut cb).await;
    drop(cb);
    assert_eq!(dump(&a).await, dump(&b).await);
}

#[tokio::test]
async fn rollback_discards_iam_writes() {
    let (a, b) = pair().await;
    let mut ca = a.pool().acquire().await.unwrap();
    legacy_user(&mut ca, "root", "h", true, 1).await;
    let alice = legacy_user(&mut ca, "alice", "h", false, 2).await;
    drop(ca);
    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        t.user_create_default_status("root", "h", true, 1)
            .await
            .unwrap();
        let uid = t
            .user_create_default_status("alice", "h", false, 2)
            .await
            .unwrap();
        assert_eq!(uid, alice);
    }
    tx.commit().await.unwrap();

    let mut tx = b.pool().begin().await.unwrap();
    {
        let mut t = SqliteControlTxn::new(&mut tx);
        t.user_create_default_status("temp", "h", false, 3)
            .await
            .unwrap();
        t.permission_upsert(alice, "b1", true, false, false)
            .await
            .unwrap();
        t.access_key_create_legacy("AKTEMP", "SK", alice, 3)
            .await
            .unwrap();
    }
    tx.rollback().await.unwrap();
    assert_eq!(dump(&a).await, dump(&b).await);
}

/// 行已存在时第二次 upsert 会走 site UPDATE 触发器：旧 UPSERT 实现在 SQLite
/// 下对 `site_entity_clock` 的 `INSERT OR IGNORE` 误报 UNIQUE。
#[tokio::test]
async fn site_triggers_accept_double_upsert_for_permissions_and_access_keys() {
    let dir = tempfile::tempdir().unwrap();
    let st = state_with_site(
        dir.path(),
        Some(SiteToml {
            id: None,
            name: Some("a".into()),
            region: "us-east-1".into(),
            priority: 0,
            token: Some("a".repeat(32)),
        }),
    )
    .await;
    st.db
        .insert_site(&oss_server::db::SitePeer {
            peer_id: "peer".into(),
            remote_site_id: "b".into(),
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
        })
        .await
        .unwrap();
    st.db.create_user("root", "pw", true).await.unwrap();
    let alice = st.db.create_user("alice", "pw", false).await.unwrap();

    let mut conn = st.db.pool().acquire().await.unwrap();
    {
        let mut txn = SqliteControlTxn::new(&mut conn);
        txn.permission_upsert(alice, "bkt", true, false, false)
            .await
            .unwrap();
        txn.permission_upsert(alice, "bkt", false, true, true)
            .await
            .unwrap();
        txn.access_key_site_upsert("AKALICE000000000001", "SK1", alice, "active", None, 100)
            .await
            .unwrap();
        txn.access_key_site_upsert(
            "AKALICE000000000001",
            "SK2",
            alice,
            "disabled",
            Some(200),
            999,
        )
        .await
        .unwrap();
    }
    drop(conn);

    let perms: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM site_changelog WHERE entity = 'permission'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(perms, 2, "insert + update 各一条 upsert changelog");
    let aks: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM site_changelog WHERE entity = 'access_key'")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(aks, 2);
    let clock: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM site_entity_clock \
         WHERE entity IN ('permission', 'access_key')",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(clock, 2, "每个实体只保留一行时钟");
    let perms: (i64, i64, i64) =
        sqlx::query_as("SELECT can_read, can_write, can_delete FROM permissions")
            .fetch_one(st.db.pool())
            .await
            .unwrap();
    assert_eq!(perms, (0, 1, 1));
    let ak: (String, String, Option<i64>, i64) = sqlx::query_as(
        "SELECT secret_key, status, expires_at, created_at FROM access_keys \
         WHERE access_key = 'AKALICE000000000001'",
    )
    .fetch_one(st.db.pool())
    .await
    .unwrap();
    assert_eq!(ak, ("SK2".into(), "disabled".into(), Some(200), 100));
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
fn production_sources_have_no_raw_iam_sql() {
    const ALLOWED: &[&str] = &[
        "iam.rs",
        "iam_write.rs",
        "iam_snapshot.rs",
        "account.rs",
        "account_write.rs",
        "account_snapshot.rs",
        "db.rs",
        "site.rs",
        "cluster_control.rs",
        "state.rs",
        "console.rs",
        // D7：安装时跨域清空 uploads.owner_id，子查询只读 users。
        "data.rs",
    ];
    const TABLES: &[&str] = &[
        "users",
        "access_keys",
        "permissions",
        "accounts",
        "account_members",
        "account_usage_ledger",
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
        "生产源码仍直写 IAM/账户控制表（应经 ControlTxn 或更新 D7 白名单）：{findings:#?}"
    );
}

#[test]
fn removed_iam_raw_helpers_stay_removed() {
    const NAMES: &[&str] = &[
        "create_user_raw",
        "set_password_raw",
        "set_permissions_raw",
        "create_access_key_raw",
        "delete_access_key_raw",
        "set_access_key_status_raw",
    ];
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut stack = vec![src];
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
