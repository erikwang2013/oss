//! M9 S6d：guard 覆盖面/提权拒绝/启动模式断言 + 直写源扫描（咨询性 tripwire）。

use oss_server::db::Db;

fn db_url(dir: &std::path::Path) -> String {
    format!("sqlite://{}?mode=rwc", dir.join("guard.db").display())
}

async fn set_apply_ctx(db: &Db, active: bool) {
    sqlx::query("UPDATE raft_apply_ctx SET active = ? WHERE id = 1")
        .bind(active as i64)
        .execute(db.pool())
        .await
        .unwrap();
}

async fn trigger_count(db: &Db) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'raft_guard\\_%' ESCAPE '\\'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn guards_cover_33_triggers_and_reject_non_admin_escalation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::connect_raft(&db_url(dir.path())).await.unwrap();
    assert_eq!(trigger_count(&db).await, 33, "11 表 × ins/upd/del");

    // apply 事务内造一个普通用户（唯一允许的写窗口）。
    set_apply_ctx(&db, true).await;
    sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES ('u','h',0,0)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    set_apply_ctx(&db, false).await;

    // 提权：非管理员行 UPDATE 在 apply 外被拒（OLD.is_admin 判定）。
    let err = sqlx::query("UPDATE users SET is_admin = 1 WHERE username = 'u'")
        .execute(db.pool())
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("raft: control write outside apply"),
        "提权必须被 guard 拒绝: {err}"
    );
    let admin: i64 = sqlx::query_scalar("SELECT is_admin FROM users WHERE username = 'u'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(admin, 0);

    // 管理员行是 guard 例外（本地运维写不复制）。
    sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES ('root','h',1,0)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE users SET status = 'disabled' WHERE username = 'root'")
        .execute(db.pool())
        .await
        .unwrap();
    db.assert_mode_guards().await.unwrap();
}

#[tokio::test]
async fn mode_assertion_rejects_m7_state_in_raft_and_accepts_m7() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::connect_raft(&db_url(dir.path())).await.unwrap();
    db.assert_mode_guards().await.unwrap();

    // Raft 下 M7 changelog 行必须为空（配对 sites 行不在此列）。
    sqlx::query(
        "INSERT INTO site_changelog (origin_site_id, entity, entity_id, op, payload, priority, ts) \
         VALUES ('x','bucket','b','upsert','{}',1,1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let err = db.assert_mode_guards().await.unwrap_err();
    assert!(err.to_string().contains("site_changelog"), "{err}");
    drop(db);

    // M7 模式：触发器清零，断言放行。
    let m7 = Db::connect(&db_url(dir.path()).replace("guard.db", "m7.db"))
        .await
        .unwrap();
    assert_eq!(trigger_count(&m7).await, 0);
    m7.assert_mode_guards().await.unwrap();
}

/// 咨询性 tripwire：控制表直写只允许出现在已知文件；新增文件即失败，需人工核对是否
/// 应走 `ControlPlane`（漏接 = 分叉风险）。SQL 跨行/别名不在扫描范围内（咨询性）。
#[test]
fn source_scan_flags_new_control_table_writers() {
    const TABLES: &[&str] = &[
        "users",
        "access_keys",
        "permissions",
        "buckets",
        "lifecycle_rules",
        "bucket_policies",
        "event_configs",
        "replication_rules",
        "access_points",
        "olap_configs",
        "inventory_configs",
    ];
    // M7 直写路径 / local M3 控制面 / apply 原语所在文件（已人工核对）。
    const ALLOWED: &[&str] = &[
        "cluster_control.rs",
        "db.rs",
        "events.rs",
        "replication.rs",
        "site.rs",
        // apply 原语 / 快照安装 / 控制数据映射（唯一允许写复制表的实现层）。
        "apply.rs",
        // level-3 账户命令 apply 子模块（apply 层，经 ControlCommand 进入）。
        "accounts.rs",
        "data.rs",
        "store.rs",
    ];
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut findings = vec![];
    let mut stack = vec![src];
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
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            let lower = text.to_ascii_lowercase();
            if TABLES.iter().any(|t| {
                lower.contains(&format!("insert into {t} "))
                    || lower.contains(&format!("insert into {t}("))
                    || lower.contains(&format!("update {t} set "))
                    || lower.contains(&format!("delete from {t} "))
            }) && !ALLOWED.contains(&name.as_str())
            {
                findings.push(name);
            }
        }
    }
    assert!(
        findings.is_empty(),
        "新的控制表直写文件（确认是否应经 ControlPlane，并更新 allowlist）: {findings:?}"
    );
}
