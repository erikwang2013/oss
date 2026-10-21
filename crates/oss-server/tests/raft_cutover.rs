//! M9 S7.5：导出/校验、M7→Raft cutover（冲突中止/dry-run/overwrite）与 force recovery。

use std::path::Path;

use oss_server::config::RaftRole;
use oss_server::db::Db;
use oss_server::raft::RaftSettings;
use oss_server::raft::cutover::{self, CutoverError, CutoverOptions, TargetFacts};

async fn db_at(dir: &Path, name: &str) -> Db {
    let url = format!("sqlite://{}?mode=rwc", dir.join(name).display());
    Db::connect(&url).await.unwrap()
}

async fn seed_admin_and_rows(db: &Db) {
    sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
         VALUES ('admin', 'h', 1, 'active', 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    db.schema_meta_set("admin_username", "admin").await.unwrap();
    sqlx::query(
        "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
         VALUES ('alice', 'h', 0, 'active', 2)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
         replication_mode, deleting) VALUES ('b1', 2, 3, 'Enabled', 0, 'async', 0)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
         VALUES (2, 'b1', 1, 0, 1)",
    )
    .execute(db.pool())
    .await
    .unwrap();
}

fn facts() -> TargetFacts {
    TargetFacts {
        version: env!("CARGO_PKG_VERSION").to_string(),
        has_vault: false,
        admin_username: Some("admin".into()),
        has_cluster: false,
    }
}

#[tokio::test]
async fn export_checksum_and_cutover_dry_run_conflict_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "src.db").await;
    seed_admin_and_rows(&src).await;
    let export = cutover::build_export(
        &src,
        "site-a".into(),
        Some(1),
        "voter",
        Some("admin".into()),
        Some(false),
    )
    .await
    .unwrap();
    cutover::verify_export(&export).unwrap();
    let mut bad = export.clone();
    bad.sha256 = "0".repeat(64);
    assert!(cutover::verify_export(&bad).is_err());

    let dst = db_at(dir.path(), "dst.db").await;
    seed_admin_and_rows(&dst).await;
    // 目标已有相同行 → 无冲突，dry-run 不写。
    let report = cutover::cutover(
        &dst,
        &facts(),
        &export,
        &CutoverOptions {
            dry_run: true,
            overwrite: false,
        },
    )
    .await
    .unwrap();
    assert!(report.conflicts.is_empty());
    assert!(!report.written);

    // 目标独有行 → 冲突中止（无 overwrite）。
    sqlx::query(
        "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
                 replication_mode, deleting) VALUES ('only-target', NULL, 9, 'Off', 0, 'async', 0)",
    )
    .execute(dst.pool())
    .await
    .unwrap();
    let err = cutover::cutover(&dst, &facts(), &export, &CutoverOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, CutoverError::Conflict(_)), "{err}");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name='only-target'")
        .fetch_one(dst.pool())
        .await
        .unwrap();
    assert_eq!(n, 1, "冲突中止不得写目标");

    // overwrite 全量替换复制子集；写入后 mode=on。
    let report = cutover::cutover(
        &dst,
        &facts(),
        &export,
        &CutoverOptions {
            dry_run: false,
            overwrite: true,
        },
    )
    .await
    .unwrap();
    assert!(report.written);
    let only: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name='only-target'")
        .fetch_one(dst.pool())
        .await
        .unwrap();
    assert_eq!(only, 0, "overwrite 必须删除目标独有复制行");
    let mode = dst.schema_meta("raft_mode").await.unwrap();
    assert_eq!(mode.as_deref(), Some("on"));
    assert!(dst.schema_meta("raft_cutover").await.unwrap().is_some());
    // 无 [raft] 的旧启动路径必须拒绝已切模式的库。
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("dst.db").display());
    assert!(Db::connect(&url).await.is_err());
}

#[tokio::test]
async fn force_recover_fresh_log_and_refuses_existing() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "src2.db").await;
    seed_admin_and_rows(&src).await;
    let export = cutover::build_export(
        &src,
        "site-a".into(),
        Some(1),
        "voter",
        Some("admin".into()),
        Some(false),
    )
    .await
    .unwrap();
    let dst = db_at(dir.path(), "dst2.db").await;
    seed_admin_and_rows(&dst).await;
    cutover::cutover(
        &dst,
        &facts(),
        &export,
        &CutoverOptions {
            dry_run: false,
            overwrite: true,
        },
    )
    .await
    .unwrap();

    let raft_db = dir.path().join("fresh-raft.db");
    let settings = RaftSettings {
        node_id: 7,
        role: RaftRole::Voter,
        bootstrap: false,
        advertise_endpoint: "http://127.0.0.1:1".into(),
        cluster_token: "recover-token".into(),
        db_path: raft_db.clone(),
        election_timeout_ms: 500,
        heartbeat_interval_ms: 100,
        install_snapshot_timeout_ms: 5_000,
        max_in_snapshot_log_to_keep: 0,
        purge_batch_size: 1024,
        write_timeout_ms: 5_000,
        snapshot_max_chunk_size: 4 * 1024 * 1024,
        snapshot_max_bytes: 512 * 1024 * 1024,
    };
    let report = cutover::force_recover(&dst, &raft_db, &settings, &export, 7)
        .await
        .unwrap();
    assert_eq!(report.voters, vec![7]);
    assert_eq!(
        dst.schema_meta("raft_node_id").await.unwrap().as_deref(),
        Some("7")
    );
    assert_eq!(
        dst.schema_meta("raft_bootstrap").await.unwrap().as_deref(),
        Some("false")
    );
    // 已有日志 → 二次恢复必须拒绝，不得编辑旧日志。
    assert!(
        cutover::force_recover(&dst, &raft_db, &settings, &export, 7)
            .await
            .is_err()
    );
}
