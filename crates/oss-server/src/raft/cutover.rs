//! M9 S7.5：控制面导出、M7→Raft cutover 与 force recovery。
//!
//! 所有写路径只经 `data::install_tx`（与快照安装同一映射），不编辑/复用旧 `raft.db`。

mod export;
mod recover;

pub use export::{
    EXPORT_FORMAT, EXPORT_KIND, ExportFile, build_export, read_export, verify_export, write_export,
};
pub use recover::{ForceRecoverReport, force_recover};

use std::collections::{BTreeMap, BTreeSet};

use super::data::{self, ControlData, InstallCtx};
use crate::db::Db;

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
    pub field: String,
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

fn keyed<T: serde::Serialize, F: Fn(&T) -> String>(
    rows: &[T],
    f: F,
) -> BTreeMap<String, serde_json::Value> {
    rows.iter()
        .map(|r| {
            (
                f(r),
                serde_json::to_value(r).unwrap_or(serde_json::Value::Null),
            )
        })
        .collect()
}

/// 同一主键的两行逐字段比较（字段名按字典序，保证报告稳定）；非对象行退回整行。
fn diff_row(
    table: &'static str,
    key: &str,
    a: &serde_json::Value,
    b: &serde_json::Value,
) -> Vec<Diff> {
    let (Some(a), Some(b)) = (a.as_object(), b.as_object()) else {
        return vec![Diff {
            table,
            key: key.to_string(),
            field: "row".into(),
            source: Some(a.to_string()),
            target: Some(b.to_string()),
        }];
    };
    let fields: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    let mut out = Vec::new();
    for f in fields {
        let (va, vb) = (a.get(f), b.get(f));
        if va != vb {
            out.push(Diff {
                table,
                key: key.to_string(),
                field: f.clone(),
                source: va.map(|v| v.to_string()),
                target: vb.map(|v| v.to_string()),
            });
        }
    }
    out
}

fn diff_table<T, F>(table: &'static str, src: &[T], dst: &[T], f: F, out: &mut Vec<Diff>)
where
    T: serde::Serialize,
    F: Fn(&T) -> String,
{
    let a = keyed(src, &f);
    let b = keyed(dst, &f);
    let keys: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    for k in keys {
        match (a.get(k), b.get(k)) {
            (Some(va), Some(vb)) => out.extend(diff_row(table, k, va, vb)),
            (sa, sb) => out.push(Diff {
                table,
                key: k.clone(),
                field: "row".into(),
                source: sa.map(|v| v.to_string()),
                target: sb.map(|v| v.to_string()),
            }),
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
