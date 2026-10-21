//! M9 S7.5：控制面导出、M7→Raft cutover 与 force recovery。
//!
//! 所有写路径只经 `data::install_tx`（与快照安装同一映射），不编辑/复用旧 `raft.db`。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::data::{self, ControlData, InstallCtx};
use super::store::SqliteLogStore;
use super::{RaftHandle, RaftSettings};
use crate::db::Db;

pub const EXPORT_FORMAT: u32 = 1;
pub const EXPORT_KIND: &str = "oss-raft-control-export";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFile {
    pub format: u32,
    pub kind: String,
    pub source_site: String,
    pub source_node_id: Option<u64>,
    pub source_role: String,
    pub version: String,
    pub admin_username: Option<String>,
    pub has_vault: Option<bool>,
    pub exported_at: i64,
    pub sha256: String,
    pub data: ControlData,
}

#[derive(Debug, Clone)]
pub struct TargetFacts {
    pub version: String,
    pub has_vault: bool,
    pub admin_username: Option<String>,
    pub has_cluster: bool,
}

#[derive(Debug, Clone, Default)]
pub struct CutoverOptions {
    pub dry_run: bool,
    pub overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diff {
    pub table: &'static str,
    pub key: String,
    pub field: &'static str,
    pub source: Option<String>,
    pub target: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CutoverReport {
    pub dry_run: bool,
    pub conflicts: Vec<Diff>,
    pub written: bool,
    pub rows: BTreeMap<&'static str, usize>,
}

#[derive(Debug)]
pub enum CutoverError {
    Invalid(String),
    Conflict(Vec<Diff>),
    Db(String),
    Io(String),
}

impl std::fmt::Display for CutoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(m) => write!(f, "invalid: {m}"),
            Self::Conflict(d) => write!(f, "conflicts: {} (use --overwrite)", d.len()),
            Self::Db(m) => write!(f, "db: {m}"),
            Self::Io(m) => write!(f, "io: {m}"),
        }
    }
}

impl std::error::Error for CutoverError {}

#[derive(Debug, Clone)]
pub struct ForceRecoverReport {
    pub node_id: u64,
    pub voters: Vec<u64>,
    pub raft_db: PathBuf,
}

/// 读取 11 张复制表并生成带校验和的导出文件（只读；不重放 schema/模式）。
pub async fn build_export(
    db: &Db,
    source_site: String,
    source_node_id: Option<u64>,
    source_role: &str,
    admin_username: Option<String>,
    has_vault: Option<bool>,
) -> Result<ExportFile, String> {
    // 在线导出也必须在一个读事务里完成，避免并发控制写把 11 表导出成撕裂组合。
    let mut tx = db.pool().begin().await.map_err(|e| format!("begin: {e}"))?;
    let data = data::read_tx(&mut tx)
        .await
        .map_err(|e| format!("read control data: {e}"))?;
    tx.commit().await.map_err(|e| format!("commit: {e}"))?;
    let payload = serde_json::to_vec(&data).map_err(|e| e.to_string())?;
    let sha256 = hex::encode(Sha256::digest(&payload));
    Ok(ExportFile {
        format: EXPORT_FORMAT,
        kind: EXPORT_KIND.to_string(),
        source_site,
        source_node_id,
        source_role: source_role.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        admin_username,
        has_vault,
        exported_at: crate::db::now(),
        sha256,
        data,
    })
}

pub fn verify_export(export: &ExportFile) -> Result<(), String> {
    if export.format != EXPORT_FORMAT || export.kind != EXPORT_KIND {
        return Err(format!(
            "unsupported export format/kind: {}/{}",
            export.format, export.kind
        ));
    }
    let payload = serde_json::to_vec(&export.data).map_err(|e| e.to_string())?;
    let want = hex::encode(Sha256::digest(&payload));
    if want != export.sha256 {
        return Err("export checksum mismatch".into());
    }
    Ok(())
}

pub fn write_export(path: &Path, export: &ExportFile) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(export)?;
    std::fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

pub fn read_export(path: &Path) -> Result<ExportFile, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let export: ExportFile =
        serde_json::from_slice(&bytes).map_err(|e| format!("parse export: {e}"))?;
    verify_export(&export)?;
    Ok(export)
}

fn keyed<T, F>(rows: &[T], f: F) -> BTreeMap<String, String>
where
    T: Serialize,
    F: Fn(&T) -> String,
{
    rows.iter()
        .map(|r| {
            (
                f(r),
                serde_json::to_string(r).unwrap_or_else(|_| "<unserializable>".into()),
            )
        })
        .collect()
}

fn diff_table<T, F>(table: &'static str, src: &[T], dst: &[T], f: F, out: &mut Vec<Diff>)
where
    T: Serialize,
    F: Fn(&T) -> String,
{
    let a = keyed(src, &f);
    let b = keyed(dst, &f);
    let keys: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    for k in keys {
        let (sa, sb) = (a.get(k), b.get(k));
        if sa != sb {
            out.push(Diff {
                table,
                key: k.clone(),
                field: "row",
                source: sa.cloned(),
                target: sb.cloned(),
            });
        }
    }
}

fn diff_control(src: &ControlData, dst: &ControlData) -> Vec<Diff> {
    let mut d = Vec::new();
    diff_table(
        "users",
        &src.users,
        &dst.users,
        |r| r.username.clone(),
        &mut d,
    );
    diff_table(
        "access_keys",
        &src.access_keys,
        &dst.access_keys,
        |r| r.access_key.clone(),
        &mut d,
    );
    diff_table(
        "permissions",
        &src.permissions,
        &dst.permissions,
        |r| format!("{}|{}", r.username, r.bucket),
        &mut d,
    );
    diff_table(
        "buckets",
        &src.buckets,
        &dst.buckets,
        |r| r.name.clone(),
        &mut d,
    );
    diff_table(
        "bucket_policies",
        &src.bucket_policies,
        &dst.bucket_policies,
        |r| r.bucket.clone(),
        &mut d,
    );
    diff_table(
        "lifecycle_rules",
        &src.lifecycle_rules,
        &dst.lifecycle_rules,
        |r| format!("{}|{}", r.bucket, r.prefix),
        &mut d,
    );
    diff_table(
        "event_configs",
        &src.event_configs,
        &dst.event_configs,
        |r| r.bucket.clone(),
        &mut d,
    );
    diff_table(
        "replication_rules",
        &src.replication_rules,
        &dst.replication_rules,
        |r| r.bucket.clone(),
        &mut d,
    );
    diff_table(
        "access_points",
        &src.access_points,
        &dst.access_points,
        |r| r.name.clone(),
        &mut d,
    );
    diff_table(
        "olap_configs",
        &src.olap_configs,
        &dst.olap_configs,
        |r| r.access_point.clone(),
        &mut d,
    );
    diff_table(
        "inventory_configs",
        &src.inventory_configs,
        &dst.inventory_configs,
        |r| format!("{}|{}", r.bucket, r.id),
        &mut d,
    );
    d
}

async fn target_control(db: &Db) -> Result<ControlData, CutoverError> {
    let mut tx = db
        .pool()
        .begin()
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    let data = data::read_tx(&mut tx)
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    tx.commit()
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    Ok(data)
}

async fn m7_lag_zero(db: &Db) -> Result<bool, CutoverError> {
    let lag: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sites s LEFT JOIN site_cursors c ON c.peer_id = s.peer_id \
         WHERE IFNULL((SELECT MAX(seq) FROM site_changelog), 0) > IFNULL(c.last_seq, 0)",
    )
    .fetch_one(db.pool())
    .await
    .map_err(|e| CutoverError::Db(e.to_string()))?;
    Ok(lag == 0)
}

/// M7→Raft cutover：全部校验在写之前；`--overwrite` 全量替换复制子集。
pub async fn cutover(
    db: &Db,
    facts: &TargetFacts,
    export: &ExportFile,
    opts: &CutoverOptions,
) -> Result<CutoverReport, CutoverError> {
    verify_export(export).map_err(CutoverError::Invalid)?;
    if export.source_role != "voter" {
        return Err(CutoverError::Invalid(
            "witness export cannot be used for cutover".into(),
        ));
    }
    if db
        .schema_meta("raft_mode")
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?
        .as_deref()
        == Some("on")
    {
        return Err(CutoverError::Invalid(
            "target DB is already in raft mode; refusing to overwrite live control state".into(),
        ));
    }
    if facts.version != export.version {
        return Err(CutoverError::Invalid(format!(
            "version mismatch: target {} vs export {}",
            facts.version, export.version
        )));
    }
    if let Some(v) = export.has_vault
        && v != facts.has_vault
    {
        return Err(CutoverError::Invalid("vault state mismatch".into()));
    }
    if facts.admin_username.is_none() || facts.admin_username != export.admin_username {
        return Err(CutoverError::Invalid("admin username mismatch".into()));
    }
    if facts.has_cluster {
        return Err(CutoverError::Invalid(
            "target has [cluster]/[[groups]]; raft mode is mutually exclusive".into(),
        ));
    }
    if !m7_lag_zero(db).await? {
        return Err(CutoverError::Invalid(
            "M7 site changelog lag is non-zero; wait for convergence".into(),
        ));
    }
    let dead: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM site_apply_dead")
        .fetch_one(db.pool())
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    if dead != 0 {
        return Err(CutoverError::Invalid(format!(
            "site_apply_dead has {dead} rows; resolve before cutover"
        )));
    }
    let target = target_control(db).await?;
    let conflicts = diff_control(&export.data, &target);
    let mut report = CutoverReport {
        dry_run: opts.dry_run,
        conflicts: conflicts.clone(),
        written: false,
        rows: BTreeMap::new(),
    };
    if !conflicts.is_empty() && !opts.overwrite {
        return Err(CutoverError::Conflict(conflicts));
    }
    if opts.dry_run {
        return Ok(report);
    }

    let admin_id: Option<i64> =
        sqlx::query_scalar("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1")
            .fetch_optional(db.pool())
            .await
            .map_err(|e| CutoverError::Db(e.to_string()))?;
    let ctx = InstallCtx {
        admin_id,
        witness: false,
    };
    let mut tx = db
        .pool()
        .begin()
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(&mut *tx)
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    let installed = data::install_tx(&mut tx, &export.data, &ctx)
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
        .execute(&mut *tx)
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    tx.commit()
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    report.written = true;
    report.rows = BTreeMap::from([
        ("users", installed.users),
        ("access_keys", installed.access_keys),
        ("permissions", installed.permissions),
        ("buckets", installed.buckets),
        ("bucket_policies", installed.bucket_policies),
        ("lifecycle_rules", installed.lifecycle_rules),
        ("event_configs", installed.event_configs),
        ("replication_rules", installed.replication_rules),
        ("access_points", installed.access_points),
        ("olap_configs", installed.olap_configs),
        ("inventory_configs", installed.inventory_configs),
    ]);
    let marker = serde_json::json!({
        "source_site": export.source_site,
        "source_node_id": export.source_node_id,
        "source_role": export.source_role,
        "version": export.version,
        "exported_at": export.exported_at,
        "applied_at": crate::db::now(),
    });
    db.schema_meta_set(
        "raft_cutover",
        &serde_json::to_string(&marker).map_err(|e| CutoverError::Db(e.to_string()))?,
    )
    .await
    .map_err(|e| CutoverError::Db(e.to_string()))?;
    db.prepare_raft_mode(true)
        .await
        .map_err(|e| CutoverError::Db(e.to_string()))?;
    Ok(report)
}

/// force recovery：仅接受全新 `raft.db` + 已导出业务表；初始化单节点 `{N}` 后退出。
pub async fn force_recover(
    db: &Db,
    raft_db_path: &Path,
    settings: &RaftSettings,
    export: &ExportFile,
    new_node_id: u64,
) -> Result<ForceRecoverReport, String> {
    verify_export(export)?;
    if export.source_role == "witness" {
        return Err(
            "witness force recovery is not implemented; rebuild the witness as a fresh learner"
                .into(),
        );
    }
    if new_node_id == 0 {
        return Err("new node id must be > 0".into());
    }
    if raft_db_path.exists() {
        let logs = SqliteLogStore::open(raft_db_path)
            .await
            .map_err(|e| format!("open raft log: {e}"))?;
        if logs.has_logs().await.map_err(|e| e.to_string())? {
            return Err(format!(
                "raft log already has entries: {}; refusing to edit an old log",
                raft_db_path.display()
            ));
        }
        drop(logs);
    }
    if db
        .schema_meta("raft_mode")
        .await
        .map_err(|e| e.to_string())?
        .as_deref()
        != Some("on")
    {
        return Err("control DB is not in raft mode; run raft-cutover first".into());
    }
    let applied_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM raft_applied WHERE applied_log_id IS NOT NULL")
            .fetch_one(db.pool())
            .await
            .map_err(|e| e.to_string())?;
    let has_snapshot_table: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'raft_snapshot'",
    )
    .fetch_one(db.pool())
    .await
    .map_err(|e| e.to_string())?;
    let snapshot_count: i64 = if has_snapshot_table > 0 {
        sqlx::query_scalar("SELECT COUNT(*) FROM raft_snapshot")
            .fetch_one(db.pool())
            .await
            .map_err(|e| e.to_string())?
    } else {
        0
    };
    if applied_count != 0 || snapshot_count != 0 {
        return Err("control DB already has applied raft state; refusing to merge".into());
    }
    let admin_id: Option<i64> =
        sqlx::query_scalar("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1")
            .fetch_optional(db.pool())
            .await
            .map_err(|e| e.to_string())?;
    if admin_id.is_none() {
        return Err("no local admin user; bootstrap an admin before recovery".into());
    }
    let mut tx = db.pool().begin().await.map_err(|e| e.to_string())?;
    sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    data::install_tx(
        &mut tx,
        &export.data,
        &InstallCtx {
            admin_id,
            witness: false,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;

    let mut settings = settings.clone();
    settings.node_id = new_node_id;
    settings.bootstrap = true;
    settings.db_path = raft_db_path.to_path_buf();
    let handle: RaftHandle = super::spawn_with_control(settings, Some(db.pool().clone()), None)
        .await
        .map_err(|e| format!("initialize recovered node: {e}"))?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let voters = loop {
        let v = super::membership::view(&handle);
        if v.voters == vec![new_node_id] {
            break v.voters;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("recovered membership did not converge to {N}".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    handle.shutdown().await;
    db.schema_meta_set("raft_node_id", &new_node_id.to_string())
        .await
        .map_err(|e| e.to_string())?;
    // 身份编码只认 "1"/"0"（raft/identity.rs）；写 "false" 会让下次启动被拒。
    db.schema_meta_set("raft_bootstrap", "0")
        .await
        .map_err(|e| e.to_string())?;
    db.schema_meta_set("raft_initialized", "1")
        .await
        .map_err(|e| e.to_string())?;
    let marker = serde_json::json!({
        "new_node_id": new_node_id,
        "source_site": export.source_site,
        "source_role": export.source_role,
        "exported_at": export.exported_at,
        "recovered_at": crate::db::now(),
    });
    db.schema_meta_set(
        "raft_force_recover",
        &serde_json::to_string(&marker).map_err(|e| e.to_string())?,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(ForceRecoverReport {
        node_id: new_node_id,
        voters,
        raft_db: raft_db_path.to_path_buf(),
    })
}
