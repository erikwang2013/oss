//! Read-only SQLite source reader for G1.
//!
//! Everything is read inside one read transaction on a `read_only` connection;
//! the caller hashes the database file before and after and fails closed if the
//! bytes changed. Unknown/optional columns are tolerated with defaults so a
//! database upgraded by `Db::connect_site` keeps its newest rows.

use std::collections::BTreeMap;
use std::path::Path;

use openraft::LogId;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{Connection, Row};
use xl_store::control_state::root::Mode;

use crate::raft::compat::{MAX_FEATURE_LEVEL, META_FEATURE_LEVEL};
use crate::raft::data::{self, ControlData};
use crate::raft::runstate::{self, RunState};

use super::local_rows::{self, LocalRows};
use super::raft_source::{self, RaftSource};
use super::source_rows::{self, RawRows};

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) fn error<T>(msg: impl Into<String>) -> Result<T, BoxError> {
    Err(std::io::Error::other(msg.into()).into())
}

pub(crate) struct Source {
    pub data: ControlData,
    pub runstate: RunState,
    pub raw: RawRows,
    pub local: LocalRows,
    pub raft: RaftSource,
    pub schema_meta: BTreeMap<String, String>,
    pub table_counts: BTreeMap<String, i64>,
    pub sqlite_sequence: BTreeMap<String, i64>,
    pub checksum: String,
    pub raft_checksum: Option<String>,
    pub feature_marker: u8,
    pub accounts_canonical: bool,
    pub mode: Mode,
    pub applied: Option<LogId<u64>>,
    pub membership_json: Option<String>,
}

pub(crate) fn checksum(path: &Path) -> Result<String, BoxError> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub(crate) async fn open_readonly(path: &Path) -> Result<SqliteConnection, BoxError> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .busy_timeout(std::time::Duration::from_secs(5));
    Ok(SqliteConnection::connect_with(&options).await?)
}

async fn read_counts(conn: &mut SqliteConnection) -> Result<BTreeMap<String, i64>, BoxError> {
    let mut out = BTreeMap::new();
    let names: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .fetch_all(&mut *conn)
            .await?;
    for name in names {
        if name.starts_with("sqlite_") {
            continue;
        }
        let quoted = name.replace('"', "\"\"");
        // Table names come from sqlite_master and are quote-escaped above.
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM \"{quoted}\""
        )))
        .fetch_one(&mut *conn)
        .await?;
        out.insert(name, count);
    }
    Ok(out)
}

type MetaRows = (
    BTreeMap<String, String>,
    BTreeMap<String, i64>,
    Option<LogId<u64>>,
    Option<String>,
);

async fn read_meta(conn: &mut SqliteConnection) -> Result<MetaRows, BoxError> {
    let mut schema_meta = BTreeMap::new();
    for r in sqlx::query("SELECT key, value FROM schema_meta ORDER BY key")
        .fetch_all(&mut *conn)
        .await?
    {
        schema_meta.insert(r.get("key"), r.get("value"));
    }
    let mut seq = BTreeMap::new();
    for r in sqlx::query("SELECT name, seq FROM sqlite_sequence ORDER BY name")
        .fetch_all(&mut *conn)
        .await?
    {
        seq.insert(r.get::<String, _>("name"), r.get("seq"));
    }
    let row = sqlx::query("SELECT applied_log_id, membership_json FROM raft_applied WHERE id = 1")
        .fetch_optional(&mut *conn)
        .await?;
    let (applied, membership) = match row {
        None => (None, None),
        Some(r) => {
            let raw: Option<String> = r.get("applied_log_id");
            let applied = match raw {
                Some(text) => Some(serde_json::from_str::<LogId<u64>>(&text).map_err(|e| {
                    std::io::Error::other(format!("raft_applied.applied_log_id: {e}"))
                })?),
                None => None,
            };
            (applied, r.get("membership_json"))
        }
    };
    Ok((schema_meta, seq, applied, membership))
}

/// Read the control database and the SQLite Raft database. Both files are
/// hashed before and after so a concurrent writer fails the migration.
pub(crate) async fn read_control(path: &Path, raft_db: &Path) -> Result<Source, BoxError> {
    if !path.is_file() {
        return error(format!("source database {} does not exist", path.display()));
    }
    let before = checksum(path)?;
    let mut conn = open_readonly(path).await?;
    sqlx::query("BEGIN").execute(&mut conn).await?;
    let canonical = sqlx::query_scalar::<_, String>("SELECT value FROM schema_meta WHERE key = ?")
        .bind(crate::db::META_ACCOUNTS_CANONICAL)
        .fetch_optional(&mut conn)
        .await?
        .is_some();
    let data = data::read_tx(&mut conn)
        .await
        .map_err(|e| std::io::Error::other(format!("read control data: {e}")))?;
    let runstate = runstate::read_runstate_tx(&mut conn)
        .await
        .map_err(|e| std::io::Error::other(format!("read runstate: {e}")))?;
    let raw = source_rows::read_raw(&mut conn, canonical).await?;
    let local = local_rows::read_local(&mut conn).await?;
    let counts = read_counts(&mut conn).await?;
    let (schema_meta, sqlite_sequence, applied, membership_json) = read_meta(&mut conn).await?;
    sqlx::query("COMMIT").execute(&mut conn).await?;
    drop(conn);
    let after = checksum(path)?;
    if before != after {
        return error(format!(
            "source database changed during migration: {before} -> {after}"
        ));
    }
    let marker = schema_meta
        .get(META_FEATURE_LEVEL)
        .map(|v| v.parse::<u8>())
        .transpose()
        .map_err(|_| std::io::Error::other("schema_meta.raft_feature_level is not an integer"))?
        .unwrap_or(1);
    if marker > MAX_FEATURE_LEVEL {
        return error(format!(
            "source feature level {marker} exceeds this binary's maximum {MAX_FEATURE_LEVEL}"
        ));
    }
    let raft = raft_source::read_raft(raft_db).await?;
    let raft_checksum = raft.checksum.clone();
    let raft_on = schema_meta.get("raft_mode").is_some_and(|v| v == "on");
    let mode = if raft_on || raft.has_state() {
        Mode::Raft
    } else {
        Mode::Standalone
    };
    Ok(Source {
        data,
        runstate,
        raw,
        local,
        raft,
        schema_meta,
        table_counts: counts,
        sqlite_sequence,
        checksum: before,
        raft_checksum,
        feature_marker: marker,
        accounts_canonical: canonical,
        mode,
        applied,
        membership_json,
    })
}

/// Effective feature level: max(source marker, encoded-data requirement).
pub(crate) fn required_feature(source: &Source) -> u8 {
    data::required_feature_level(&source.data)
        .max(source.runstate.required_feature_level())
        .max(1)
}

pub(crate) fn effective_feature(source: &Source) -> u8 {
    source.feature_marker.max(required_feature(source))
}

/// Root allocator high-water marks implied by the source.
pub(crate) fn expected_allocators(source: &Source) -> (u64, u64, u64, i64) {
    let max_uid = source.raw.users.iter().map(|u| u.uid).max().unwrap_or(0);
    let seq_uid = source.sqlite_sequence.get("users").copied().unwrap_or(0);
    let next_uid = seq_uid.max(max_uid).max(0) as u64 + 1;
    let max_seq = source
        .raw
        .changelog
        .iter()
        .map(|r| r.seq)
        .max()
        .unwrap_or(0);
    let seq_site = source
        .sqlite_sequence
        .get("site_changelog")
        .copied()
        .unwrap_or(0);
    let next_site = seq_site.max(max_seq).max(0) + 1;
    let next_job = source.runstate.next_job_id.max(1) as u64;
    let next_run = source.runstate.next_run_id.max(1) as u64;
    (next_uid, next_job, next_run, next_site)
}
