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

fn raft_settings(node_id: u64, role: RaftRole, db_path: std::path::PathBuf) -> RaftSettings {
    RaftSettings {
        node_id,
        role,
        bootstrap: false,
        advertise_endpoint: "http://127.0.0.1:1".into(),
        cluster_token: "recover-token".into(),
        db_path,
        election_timeout_ms: 500,
        heartbeat_interval_ms: 100,
        install_snapshot_timeout_ms: 5_000,
        max_in_snapshot_log_to_keep: 0,
        purge_batch_size: 1024,
        write_timeout_ms: 5_000,
        snapshot_max_chunk_size: 4 * 1024 * 1024,
        snapshot_max_bytes: 512 * 1024 * 1024,
        feature_level: 1,
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
    // mode-on 的目标不得再次被 cutover 覆盖。
    let err = cutover::cutover(
        &dst,
        &facts(),
        &export,
        &CutoverOptions {
            dry_run: false,
            overwrite: true,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CutoverError::Invalid(_)), "{err}");
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
    let settings = raft_settings(7, RaftRole::Voter, raft_db.clone());
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
        Some("0")
    );
    // 恢复产物必须能以 bootstrap=false 再启动（身份编码 "0"/"1"）。
    let mut restart = settings.clone();
    restart.node_id = 7;
    restart.bootstrap = false;
    oss_server::raft::identity::verify_and_seed(&dst, &restart)
        .await
        .unwrap();
    restart.bootstrap = true;
    assert!(
        oss_server::raft::identity::verify_and_seed(&dst, &restart)
            .await
            .is_err(),
        "bootstrap 漂移必须被身份校验拒绝"
    );
    // 已有日志 → 二次恢复必须拒绝，不得编辑旧日志。
    assert!(
        cutover::force_recover(&dst, &raft_db, &settings, &export, 7)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cutover_diff_reports_changed_field() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "src3.db").await;
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
    let dst = db_at(dir.path(), "dst3.db").await;
    seed_admin_and_rows(&dst).await;
    sqlx::query("UPDATE buckets SET versioning = 'Suspended' WHERE name = 'b1'")
        .execute(dst.pool())
        .await
        .unwrap();
    let err = cutover::cutover(&dst, &facts(), &export, &CutoverOptions::default())
        .await
        .unwrap_err();
    let CutoverError::Conflict(diffs) = err else {
        panic!("expected conflict, got {err}");
    };
    // 只有 bucket.versioning 不同 → 一条字段级冲突，而不是整行。
    assert_eq!(diffs.len(), 1, "{diffs:?}");
    assert_eq!(diffs[0].table, "buckets");
    assert_eq!(diffs[0].key, "b1");
    assert_eq!(diffs[0].field, "versioning");
    assert_eq!(diffs[0].source.as_deref(), Some("\"Enabled\""));
    assert_eq!(diffs[0].target.as_deref(), Some("\"Suspended\""));
}

#[tokio::test]
async fn witness_force_recover_fresh_state_db_restarts_with_identity_and_membership() {
    let dir = tempfile::tempdir().unwrap();
    // 导出内容与来源库模式无关：witness 的 11 张表同样由 apply 写入；这里用可播种的
    // M7 库造同形导出，并标记 source_role=witness。
    let src = db_at(dir.path(), "w-src.db").await;
    seed_admin_and_rows(&src).await;
    let export = cutover::build_export(&src, "witness".into(), Some(3), "witness", None, None)
        .await
        .unwrap();

    let dst_url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("w-dst.db").display()
    );
    let dst = Db::connect_witness(&dst_url).await.unwrap();
    let raft_db = dir.path().join("w-fresh-raft.db");
    let settings = raft_settings(3, RaftRole::Witness, raft_db.clone());
    let report = cutover::force_recover(&dst, &raft_db, &settings, &export, 3)
        .await
        .unwrap();
    assert_eq!(report.node_id, 3);
    assert_eq!(report.voters, vec![3]);
    // witness 无本地管理员也能安装（AP/inventory 属主写哨兵 0）。
    let buckets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name = 'b1'")
        .fetch_one(dst.pool())
        .await
        .unwrap();
    assert_eq!(buckets, 1);
    for (key, want) in [
        ("raft_node_id", "3"),
        ("raft_bootstrap", "0"),
        ("raft_initialized", "1"),
    ] {
        assert_eq!(
            dst.schema_meta(key).await.unwrap().as_deref(),
            Some(want),
            "{key}"
        );
    }
    assert!(
        dst.schema_meta("raft_force_recover")
            .await
            .unwrap()
            .is_some()
    );
    // 直接安装的行不在日志里：恢复必须留下基线快照并清日志，新 learner 才能追平。
    let snapshots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raft_snapshot WHERE id = 1")
        .fetch_one(dst.pool())
        .await
        .unwrap();
    assert_eq!(snapshots, 1, "force recovery must persist a base snapshot");
    let snap_bytes: Vec<u8> = sqlx::query_scalar("SELECT data FROM raft_snapshot WHERE id = 1")
        .fetch_one(dst.pool())
        .await
        .unwrap();
    let snapshot = oss_server::raft::data::decode_snapshot(&snap_bytes).unwrap();
    assert!(
        snapshot.buckets.iter().any(|b| b.name == "b1"),
        "base snapshot must contain force-recovered rows"
    );
    let logs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raft_log")
        .fetch_one(dst.pool())
        .await
        .unwrap();
    assert_eq!(logs, 0, "recovery must purge logs covered by the snapshot");

    // witness 重启（bootstrap=false + role=witness）必须通过身份校验：
    // voter 校验 site_id，witness 无 site_config、跳过该项。
    oss_server::raft::identity::verify_and_seed(&dst, &settings)
        .await
        .unwrap();
    let handle = oss_server::raft::spawn_with_control(settings, Some(dst.pool().clone()), None)
        .await
        .unwrap();
    assert_eq!(oss_server::raft::membership::view(&handle).voters, vec![3]);
    handle.shutdown().await;
}
