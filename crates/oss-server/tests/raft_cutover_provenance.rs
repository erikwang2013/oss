//! LC-0：导出封套 provenance（snapshot_format/wire_version/feature_level/source_build）
//! 的接受、拒绝，以及旧导出（缺 provenance）的兼容策略。

use std::path::Path;

use oss_server::config::RaftRole;
use oss_server::db::Db;
use oss_server::raft::RaftSettings;
use oss_server::raft::compat;
use oss_server::raft::cutover::{self, CutoverError, CutoverOptions, ExportFile, TargetFacts};

async fn db_at(dir: &Path, name: &str) -> Db {
    let url = format!("sqlite://{}?mode=rwc", dir.join(name).display());
    Db::connect(&url).await.unwrap()
}

/// 清掉 standalone backfill 产生的系统账户与 canonical marker，模拟未 canonical 的来源。
async fn clear_canonical_accounts(db: &Db) {
    sqlx::query("DELETE FROM account_members")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM accounts")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM schema_meta WHERE key = ?")
        .bind(oss_server::db::META_ACCOUNTS_CANONICAL)
        .execute(db.pool())
        .await
        .unwrap();
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
}

async fn seed_level_two_rule(db: &Db) {
    sqlx::query(
        "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at, \
         rule_id, expiration_days, config_json) VALUES ('b1', 'logs/', 30, 1, 5, 'r1', 30, \
         '{\"filter\":{\"prefix\":\"logs/\"}}')",
    )
    .execute(db.pool())
    .await
    .unwrap();
}

/// level-3 来源：激活 marker=3 + canonical 账户表（alice uid=2）。
async fn seed_level_three_accounts(db: &Db) {
    db.schema_meta_set("raft_feature_level", "3").await.unwrap();
    db.schema_meta_set(oss_server::db::META_ACCOUNTS_CANONICAL, "done")
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO accounts \
         (id, name, status, bucket_quota, soft_byte_quota, created_at, updated_at) \
         VALUES ('000000000002', 'alice-acct', 'active', 2, 1024, 4, 5)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO account_members (account_id, uid, role, created_at) \
         VALUES ('000000000002', 2, 'owner', 4)",
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
        cluster_token: "provenance-token".into(),
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

async fn export_from(db: &Db) -> ExportFile {
    cutover::build_export(
        db,
        "site-a".into(),
        Some(1),
        "voter",
        Some("admin".into()),
        Some(false),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn export_records_provenance_and_accepts_ossctl1_and_ossctl2() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "prov-src.db").await;
    seed_admin_and_rows(&src).await;
    clear_canonical_accounts(&src).await;

    // level-1 数据：来源写 OSSCTL1。
    let v1 = export_from(&src).await;
    assert_eq!(v1.snapshot_format, Some(1));
    assert_eq!(v1.feature_level, Some(1));
    assert_eq!(v1.activation_level, Some(1));
    assert_eq!(v1.wire_version, Some(compat::WIRE_VERSION));
    assert!(v1.source_build.as_deref().is_some_and(|b| !b.is_empty()));
    assert!(
        cutover::verify_export(&v1).unwrap().is_empty(),
        "OSSCTL1 export must verify clean"
    );

    // 激活 marker=2（即使当前无 level-2 行）：来源快照为 OSSCTL2。
    src.schema_meta_set("raft_feature_level", "2")
        .await
        .unwrap();
    let v2 = export_from(&src).await;
    assert_eq!(v2.snapshot_format, Some(2));
    assert_eq!(v2.feature_level, Some(2));
    assert_eq!(
        v2.activation_level,
        Some(2),
        "activated source must record its marker even without level-2 data"
    );
    assert!(
        cutover::verify_export(&v2).unwrap().is_empty(),
        "OSSCTL2 export must verify clean on this binary"
    );

    // config_json 数据（marker 仍为 1）也只能以 OSSCTL2 导出。
    let src2 = db_at(dir.path(), "prov-src2.db").await;
    seed_admin_and_rows(&src2).await;
    seed_level_two_rule(&src2).await;
    clear_canonical_accounts(&src2).await;
    let v3 = export_from(&src2).await;
    assert_eq!(v3.snapshot_format, Some(2));
    assert_eq!(v3.feature_level, Some(2));
    assert!(cutover::verify_export(&v3).unwrap().is_empty());
}

/// level-3 账户数据只能以 OSSCTL3 导出；伪造格式 2（无论声明的级别）都拒绝。
#[tokio::test]
async fn export_carries_ossctl3_accounts_and_rejects_format_two_forgery() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "l3-src.db").await;
    seed_admin_and_rows(&src).await;
    clear_canonical_accounts(&src).await;
    seed_level_three_accounts(&src).await;
    let export = export_from(&src).await;
    assert_eq!(export.snapshot_format, Some(3));
    assert_eq!(export.feature_level, Some(3));
    assert_eq!(export.activation_level, Some(3));
    assert_eq!(export.data.accounts.len(), 1);
    assert_eq!(export.data.account_members.len(), 1);
    assert!(
        cutover::verify_export(&export).unwrap().is_empty(),
        "OSSCTL3 export must verify clean"
    );

    // 声明 feature_level=3 但封套是格式 2：格式不能携带 level-3 数据。
    let mut forged = export.clone();
    forged.snapshot_format = Some(2);
    let err = cutover::verify_export(&forged).unwrap_err();
    assert!(err.contains("cannot carry level-3 data"), "{err}");

    // 连级别一起伪造回 level 2：数据自身要求 3，同样拒绝。
    let mut forged = export;
    forged.snapshot_format = Some(2);
    forged.feature_level = Some(2);
    forged.activation_level = Some(2);
    let err = cutover::verify_export(&forged).unwrap_err();
    assert!(err.contains("cannot carry level-3 data"), "{err}");
}

#[tokio::test]
async fn verify_export_rejects_forged_or_unreadable_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "rej-src.db").await;
    seed_admin_and_rows(&src).await;
    clear_canonical_accounts(&src).await;
    let v1 = export_from(&src).await;

    let mut bad = v1.clone();
    bad.snapshot_format = Some(3);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("snapshot_format"), "{err}");

    bad = v1.clone();
    bad.snapshot_format = Some(0);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("snapshot_format"), "{err}");

    bad = v1.clone();
    bad.feature_level = Some(compat::MAX_FEATURE_LEVEL + 1);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("feature_level"), "{err}");

    bad = v1.clone();
    bad.feature_level = Some(0);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("feature_level"), "{err}");

    bad = v1.clone();
    bad.activation_level = Some(compat::MAX_FEATURE_LEVEL + 1);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("activation_level"), "{err}");

    bad = v1.clone();
    bad.activation_level = Some(0);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("activation_level"), "{err}");

    // OSSCTL1 不能记录激活级别。
    bad = v1.clone();
    bad.activation_level = Some(2);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("cannot record activation_level"), "{err}");

    // 激活级别高于封套级别 = 不一致（来源 marker 不可能超过 max(marker, data)）。
    bad = v1.clone();
    bad.snapshot_format = Some(2);
    bad.feature_level = Some(1);
    bad.activation_level = Some(2);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("activation_level"), "{err}");

    // activation_level 单独出现（无 feature_level）= 不一致。
    bad = v1.clone();
    bad.feature_level = None;
    bad.activation_level = Some(1);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("without feature_level"), "{err}");

    bad = v1.clone();
    bad.wire_version = Some(compat::WIRE_VERSION + 1);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("wire_version"), "{err}");

    bad = v1.clone();
    bad.wire_version = Some(0);
    let err = cutover::verify_export(&bad).unwrap_err();
    assert!(err.contains("wire_version"), "{err}");

    // level-2 数据伪造为 OSSCTL1 / feature_level=1：格式无法携带数据 → 拒绝。
    let src2 = db_at(dir.path(), "rej-src2.db").await;
    seed_admin_and_rows(&src2).await;
    seed_level_two_rule(&src2).await;
    clear_canonical_accounts(&src2).await;
    let v2 = export_from(&src2).await;

    let mut forged = v2.clone();
    forged.snapshot_format = Some(1);
    let err = cutover::verify_export(&forged).unwrap_err();
    assert!(err.contains("cannot carry"), "{err}");

    let mut forged = v2;
    forged.feature_level = Some(1);
    let err = cutover::verify_export(&forged).unwrap_err();
    assert!(err.contains("below the level-2 data"), "{err}");
}

#[tokio::test]
async fn cutover_and_force_recover_refuse_incompatible_export_before_writing() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "guard-src.db").await;
    seed_admin_and_rows(&src).await;
    seed_level_two_rule(&src).await;
    let mut incompatible = export_from(&src).await;
    incompatible.feature_level = Some(compat::MAX_FEATURE_LEVEL + 1);

    // cutover dry-run：在任何写之前拒绝，目标无 marker、无复制行。
    let dst = db_at(dir.path(), "guard-dst.db").await;
    seed_admin_and_rows(&dst).await;
    let err = cutover::cutover(
        &dst,
        &facts(),
        &incompatible,
        &CutoverOptions {
            dry_run: true,
            overwrite: true,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CutoverError::Invalid(_)), "{err}");
    assert!(err.to_string().contains("feature_level"), "{err}");
    assert_eq!(dst.schema_meta("raft_mode").await.unwrap(), None);
    assert_eq!(dst.schema_meta("raft_feature_level").await.unwrap(), None);
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_rules")
        .fetch_one(dst.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0, "rejected cutover must not install rows");

    // force-recover：在安装行/初始化 raft.db 之前拒绝。
    let dst2 = db_at(dir.path(), "guard-recover.db").await;
    seed_admin_and_rows(&dst2).await;
    dst2.schema_meta_set("raft_mode", "on").await.unwrap();
    let raft_db = dir.path().join("guard-raft.db");
    let settings = raft_settings(7, RaftRole::Voter, raft_db.clone());
    let err = cutover::force_recover(&dst2, &raft_db, &settings, &incompatible, 7)
        .await
        .unwrap_err();
    assert!(err.contains("feature_level"), "{err}");
    assert!(!raft_db.exists(), "rejected export must not create raft.db");
    assert_eq!(dst2.schema_meta("raft_force_recover").await.unwrap(), None);
    assert_eq!(dst2.schema_meta("raft_feature_level").await.unwrap(), None);
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_rules")
        .fetch_one(dst2.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0, "rejected recovery must not install rows");
    let snap_table: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'raft_snapshot'",
    )
    .fetch_one(dst2.pool())
    .await
    .unwrap();
    assert_eq!(
        snap_table, 0,
        "rejected recovery must not start the state machine/persist a snapshot"
    );
}

/// 激活过但无 config_json 的集群：导出/割接/force-recover 都必须保持 level 2。
/// 旧实现只看数据要求 → 恢复成 level 1，下次启动被 R8 启动守卫拒绝。
#[tokio::test]
async fn activated_export_without_level_two_data_stays_activated() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "act-src.db").await;
    seed_admin_and_rows(&src).await;
    clear_canonical_accounts(&src).await;
    src.schema_meta_set("raft_feature_level", "2")
        .await
        .unwrap();
    let export = export_from(&src).await;
    assert_eq!(export.activation_level, Some(2));
    assert_eq!(
        oss_server::raft::data::required_feature_level(&export.data),
        1,
        "fixture must not carry level-2 rows"
    );
    assert!(cutover::verify_export(&export).unwrap().is_empty());
    assert_eq!(cutover::install_activation_level(&export), 2);

    // cutover：无 level-2 行也写 marker=2。
    let dst = db_at(dir.path(), "act-dst.db").await;
    seed_admin_and_rows(&dst).await;
    clear_canonical_accounts(&dst).await;
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
    assert_eq!(
        dst.schema_meta("raft_feature_level")
            .await
            .unwrap()
            .as_deref(),
        Some("2")
    );

    // v1.16 导出形态（有 feature_level、无 activation_level）：回落 feature_level 仍保持 2。
    let mut value = serde_json::to_value(&export).unwrap();
    assert!(
        value
            .as_object_mut()
            .unwrap()
            .remove("activation_level")
            .is_some()
    );
    let v116: ExportFile = serde_json::from_value(value).unwrap();
    assert!(cutover::verify_export(&v116).unwrap().is_empty());
    assert_eq!(cutover::install_activation_level(&v116), 2);
    let dst2 = db_at(dir.path(), "act-dst2.db").await;
    seed_admin_and_rows(&dst2).await;
    clear_canonical_accounts(&dst2).await;
    cutover::cutover(
        &dst2,
        &facts(),
        &v116,
        &CutoverOptions {
            dry_run: false,
            overwrite: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        dst2.schema_meta("raft_feature_level")
            .await
            .unwrap()
            .as_deref(),
        Some("2")
    );

    // force-recover：feature_level=1 在写前拒绝；=2 接受并留 OSSCTL2 基线快照。
    let dst3 = db_at(dir.path(), "act-rec.db").await;
    seed_admin_and_rows(&dst3).await;
    clear_canonical_accounts(&dst3).await;
    dst3.schema_meta_set("raft_mode", "on").await.unwrap();
    let raft_db = dir.path().join("act-raft.db");
    let settings = raft_settings(7, RaftRole::Voter, raft_db.clone());
    let err = cutover::force_recover(&dst3, &raft_db, &settings, &export, 7)
        .await
        .unwrap_err();
    assert!(err.contains("feature_level = 2"), "{err}");
    assert_eq!(dst3.schema_meta("raft_feature_level").await.unwrap(), None);
    assert!(
        !raft_db.exists(),
        "refused recovery must not create raft.db"
    );

    let mut settings2 = settings.clone();
    settings2.feature_level = 2;
    cutover::force_recover(&dst3, &raft_db, &settings2, &export, 7)
        .await
        .unwrap();
    assert_eq!(
        dst3.schema_meta("raft_feature_level")
            .await
            .unwrap()
            .as_deref(),
        Some("2")
    );
    let snap_bytes: Vec<u8> = sqlx::query_scalar("SELECT data FROM raft_snapshot WHERE id = 1")
        .fetch_one(dst3.pool())
        .await
        .unwrap();
    assert_eq!(
        oss_server::raft::data::decode_snapshot(&snap_bytes)
            .unwrap()
            .feature_level,
        2,
        "recovered baseline snapshot must stay OSSCTL2"
    );
}

#[tokio::test]
async fn legacy_export_without_provenance_is_accepted_with_warning() {
    let dir = tempfile::tempdir().unwrap();
    let src = db_at(dir.path(), "legacy-src.db").await;
    seed_admin_and_rows(&src).await;
    clear_canonical_accounts(&src).await;
    let export = export_from(&src).await;

    let mut value = serde_json::to_value(&export).unwrap();
    let obj = value.as_object_mut().unwrap();
    for key in [
        "snapshot_format",
        "wire_version",
        "feature_level",
        "activation_level",
        "source_build",
    ] {
        assert!(obj.remove(key).is_some(), "new exports must carry {key}");
    }
    let legacy: ExportFile = serde_json::from_value(value).unwrap();
    let warnings = cutover::verify_export(&legacy).unwrap();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("without provenance"), "{warnings:?}");

    // 未验证但不阻断：dry-run 走完同一校验路径且不写。
    let dst = db_at(dir.path(), "legacy-dst.db").await;
    seed_admin_and_rows(&dst).await;
    clear_canonical_accounts(&dst).await;
    let report = cutover::cutover(
        &dst,
        &facts(),
        &legacy,
        &CutoverOptions {
            dry_run: true,
            overwrite: true,
        },
    )
    .await
    .unwrap();
    assert!(report.dry_run && !report.written);
    assert_eq!(dst.schema_meta("raft_mode").await.unwrap(), None);
}
