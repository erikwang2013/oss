//! S5 基座存储：`raft.db` 上的 v2 [`RaftLogStorage`] + [`RaftStateMachine`]。
//!
//! 日志/投票/成员与应用状态都落在同一 SQLite 文件（WAL + `synchronous=FULL` + 5 s
//! busy_timeout）；写事务先读后写时必须 `BEGIN IMMEDIATE`，否则 WAL 升级会立即 BUSY。S6 把 `apply` 事务扩展到控制库真实表时，可把状态机指向 `control.db`：
//! `raft_applied` 表结构已与 `0004_m9.sql` 对齐。
//!
//! 快照 > [`SNAPSHOT_CHUNK_BYTES`] 时按固定大小分块行存储（`raft_snapshot_chunk`，
//! `raft_snapshot.data` 只留清单头）；小快照与旧库仍按整行 BLOB 读取，格式对调用方透明。
//!
//! 日志：大 entry 的 JSON 用 zlib 压缩、`z1:` + base64 存入 `raft_log.entry`；旧明文行
//! （`{` 开头）照常解码。追加事务内把最新连续 >= [`SEGMENT_MIN_ENTRIES`] 行压成一个
//! `raft_log_segment`（zlib JSON 数组），读时与剩余行合并；purge/truncate 在段内切分，
//! 所有变更同事务提交，崩溃只会留下旧行或完整段。

// openraft StorageError 体积由第三方类型固定，trait 签名不能改。
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Read, Write};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use base64::Engine as _;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use openraft::entry::RaftPayload;
use openraft::storage::{LogFlushed, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState,
    OptionalSend, RaftLogReader, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Row, SqliteConnection, SqlitePool};

use super::apply;
use super::data::{self, InstallCtx};
use super::metrics::Metrics;
use super::{ControlResponse, TypeConfig};

/// 日志/元数据 + 基座状态机表。`raft_applied`/`raft_apply_ctx` 与 `0004_m9.sql` 同名同构。
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS raft_log (
    idx   INTEGER PRIMARY KEY,
    entry TEXT NOT NULL
);
-- 日志段：压缩 JSON 数组覆盖 [start_idx, end_idx]；这些 idx 不再有 raft_log 行。
CREATE TABLE IF NOT EXISTS raft_log_segment (
    start_idx INTEGER PRIMARY KEY,
    end_idx   INTEGER NOT NULL,
    data      BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS raft_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raft_applied (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    applied_log_id  TEXT,
    membership_json TEXT
);
CREATE TABLE IF NOT EXISTS raft_kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raft_snapshot (
    id   INTEGER PRIMARY KEY CHECK (id = 1),
    meta TEXT NOT NULL,
    data BLOB NOT NULL
);
-- 大快照的固定大小分块（data 列只保留清单头）；旧库无此表/无行时按整行 BLOB 读。
CREATE TABLE IF NOT EXISTS raft_snapshot_chunk (
    snapshot_id TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    data        BLOB NOT NULL,
    PRIMARY KEY (snapshot_id, seq)
);
CREATE TABLE IF NOT EXISTS raft_apply_ctx (
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    active INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO raft_apply_ctx (id, active) VALUES (1, 0);
"#;

/// 快照单块上限：超过它才分块，单行 BLOB 因此有界；不超过时保持旧版整行格式。
const SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024;
/// 分块清单 magic。旧二进制读到它会在快照校验处拒绝（fail-closed），不会当空快照。
const SNAPSHOT_CHUNK_MAGIC: &[u8; 8] = b"OSSCHNK1";
/// magic(8) + 块数 u32 LE + 原始总长 u64 LE。
const SNAPSHOT_CHUNK_HEADER_LEN: usize = 8 + 4 + 8;

/// 明文 JSON 达到该长度才尝试压缩；小 entry 保持明文（运维/集成测试直接读 `entry`）。
const ENTRY_COMPRESS_MIN_BYTES: usize = 1024;
/// 压缩 entry 的 magic。明文 Entry JSON 必以 `{` 开头，前缀无歧义。
const ENTRY_COMPRESS_MAGIC: &str = "z1:";
/// 解压上限：命令体上限 24 MiB + Entry 外壳余量；超限按损坏 fail-closed。
const ENTRY_JSON_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// 段数据 magic：magic(8) + 原始 JSON 长 u64 LE + zlib 流。
const SEGMENT_MAGIC: &[u8; 8] = b"OSSLSEG1";
const SEGMENT_HEADER_LEN: usize = 8 + 8;
/// 追加事务内，最新连续行达到该行数才压段（同时限制单段行数/解压内存）。
const SEGMENT_MIN_ENTRIES: usize = 256;
const SEGMENT_MAX_ENTRIES: usize = 1024;
/// 单段原始 JSON 上限；解压上限留足余量。
const SEGMENT_MAX_RAW_BYTES: usize = 16 * 1024 * 1024;
const SEGMENT_JSON_MAX_BYTES: u64 = 64 * 1024 * 1024;

async fn connect(path: &Path) -> Result<SqlitePool, sqlx::Error> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(sqlx::Error::Io)?;
    }
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePool::connect_with(opts).await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    Ok(pool)
}

fn storage_err(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    e: impl std::error::Error + 'static,
) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::new(&e)),
    }
}

fn ser_err(subject: ErrorSubject<u64>, verb: ErrorVerb, e: serde_json::Error) -> StorageError<u64> {
    storage_err(subject, verb, e)
}

fn encode<T: Serialize>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    v: &T,
) -> Result<String, StorageError<u64>> {
    serde_json::to_string(v).map_err(|e| ser_err(subject, verb, e))
}

fn decode<T: DeserializeOwned>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    s: &str,
) -> Result<T, StorageError<u64>> {
    serde_json::from_str(s).map_err(|e| ser_err(subject, verb, e))
}

fn log_err(verb: ErrorVerb, msg: impl std::fmt::Display) -> StorageError<u64> {
    storage_err(
        ErrorSubject::Logs,
        verb,
        std::io::Error::other(msg.to_string()),
    )
}

fn log_corrupt(msg: impl std::fmt::Display) -> StorageError<u64> {
    log_err(ErrorVerb::Read, format_args!("corrupt raft log: {msg}"))
}

/// 序列化 entry；大载荷尝试 zlib+base64，只有确实更小才用压缩形态（`z1:` 前缀）。
fn encode_entry(entry: &Entry<TypeConfig>) -> Result<String, StorageError<u64>> {
    let json = encode(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, entry)?;
    if json.len() < ENTRY_COMPRESS_MIN_BYTES {
        return Ok(json);
    }
    let io_err =
        |e: std::io::Error| storage_err(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, e);
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    enc.write_all(json.as_bytes()).map_err(io_err)?;
    let compressed = enc.finish().map_err(io_err)?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(compressed);
    if ENTRY_COMPRESS_MAGIC.len() + b64.len() < json.len() {
        Ok(format!("{ENTRY_COMPRESS_MAGIC}{b64}"))
    } else {
        Ok(json)
    }
}

/// 解码 entry，并返回原始 JSON 字节数（压段时用于限制内存）。任何损坏都 fail-closed。
fn decode_entry_with_len(s: &str) -> Result<(Entry<TypeConfig>, usize), StorageError<u64>> {
    let Some(b64) = s.strip_prefix(ENTRY_COMPRESS_MAGIC) else {
        // 旧库/小 entry：明文 JSON。
        return Ok((decode(ErrorSubject::Logs, ErrorVerb::Read, s)?, s.len()));
    };
    let corrupt = |what: String| log_corrupt(format!("compressed entry: {what}"));
    let compressed = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| corrupt(e.to_string()))?;
    let mut json = Vec::new();
    ZlibDecoder::new(compressed.as_slice())
        .take(ENTRY_JSON_MAX_BYTES + 1)
        .read_to_end(&mut json)
        .map_err(|e| corrupt(e.to_string()))?;
    if json.len() as u64 > ENTRY_JSON_MAX_BYTES {
        return Err(corrupt(format!(
            "decompressed size exceeds {ENTRY_JSON_MAX_BYTES} bytes"
        )));
    }
    let entry = serde_json::from_slice(&json)
        .map_err(|e| log_corrupt(format!("compressed entry json: {e}")))?;
    Ok((entry, json.len()))
}

fn decode_entry(s: &str) -> Result<Entry<TypeConfig>, StorageError<u64>> {
    decode_entry_with_len(s).map(|(entry, _)| entry)
}

/// 段格式：magic(8) + 原始 JSON 长 u64 LE + zlib(JSON 数组)。entry 必须连续。
fn encode_segment(entries: &[Entry<TypeConfig>]) -> Result<Vec<u8>, StorageError<u64>> {
    let Some(first) = entries.first() else {
        return Err(log_err(ErrorVerb::Write, "empty raft log segment"));
    };
    let start = first.log_id.index;
    for (i, entry) in entries.iter().enumerate() {
        let want = start
            .checked_add(i as u64)
            .ok_or_else(|| log_err(ErrorVerb::Write, "raft log segment index overflow"))?;
        if entry.log_id.index != want {
            return Err(log_err(
                ErrorVerb::Write,
                format!(
                    "raft log segment entries not contiguous: want {want}, got {}",
                    entry.log_id.index
                ),
            ));
        }
    }
    let json = serde_json::to_vec(entries)
        .map_err(|e| ser_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
    if json.len() as u64 > SEGMENT_JSON_MAX_BYTES {
        return Err(log_err(
            ErrorVerb::Write,
            format!("raft log segment JSON {} exceeds limit", json.len()),
        ));
    }
    let mut out = Vec::with_capacity(SEGMENT_HEADER_LEN + json.len() / 2);
    out.extend_from_slice(SEGMENT_MAGIC);
    out.extend_from_slice(&(json.len() as u64).to_le_bytes());
    let mut enc = ZlibEncoder::new(out, Compression::default());
    enc.write_all(&json)
        .map_err(|e| log_err(ErrorVerb::Write, e))?;
    enc.finish().map_err(|e| log_err(ErrorVerb::Write, e))
}

/// 解码并校验段：magic、声明长度、zlib、数组元素数与索引必须精确匹配 [start,end]。
fn decode_segment(
    data: &[u8],
    start_idx: i64,
    end_idx: i64,
) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
    let corrupt = |what: String| log_corrupt(format!("segment [{start_idx},{end_idx}]: {what}"));
    if data.len() < SEGMENT_HEADER_LEN {
        return Err(corrupt("truncated header".into()));
    }
    if !data.starts_with(SEGMENT_MAGIC.as_slice()) {
        return Err(corrupt("bad magic".into()));
    }
    let raw_len = u64::from_le_bytes(data[8..16].try_into().expect("8 bytes"));
    if raw_len > SEGMENT_JSON_MAX_BYTES {
        return Err(corrupt(format!(
            "declared size {raw_len} exceeds limit {SEGMENT_JSON_MAX_BYTES}"
        )));
    }
    let mut json = Vec::new();
    ZlibDecoder::new(&data[SEGMENT_HEADER_LEN..])
        .take(raw_len + 1)
        .read_to_end(&mut json)
        .map_err(|e| corrupt(e.to_string()))?;
    if json.len() as u64 != raw_len {
        return Err(corrupt(format!(
            "expected {raw_len} bytes, found {}",
            json.len()
        )));
    }
    let entries: Vec<Entry<TypeConfig>> =
        serde_json::from_slice(&json).map_err(|e| corrupt(format!("json: {e}")))?;
    let (Ok(start), Ok(end)) = (u64::try_from(start_idx), u64::try_from(end_idx)) else {
        return Err(corrupt("negative range".into()));
    };
    if end < start {
        return Err(corrupt("end before start".into()));
    }
    let want = end - start + 1;
    if entries.len() as u64 != want {
        return Err(corrupt(format!(
            "expected {want} entries, found {}",
            entries.len()
        )));
    }
    for (i, entry) in entries.iter().enumerate() {
        let idx = start + i as u64;
        if entry.log_id.index != idx {
            return Err(corrupt(format!(
                "entry index {} outside [{start_idx},{end_idx}]",
                entry.log_id.index
            )));
        }
    }
    Ok(entries)
}

/// 在调用方事务内写一个段（entries 非空且索引连续）。
async fn write_segment_tx(
    tx: &mut SqliteConnection,
    entries: &[Entry<TypeConfig>],
) -> Result<(), StorageError<u64>> {
    let data = encode_segment(entries)?;
    let start = entries[0].log_id.index as i64;
    let end = entries[entries.len() - 1].log_id.index as i64;
    sqlx::query(
        "INSERT OR REPLACE INTO raft_log_segment (start_idx, end_idx, data) VALUES (?, ?, ?)",
    )
    .bind(start)
    .bind(end)
    .bind(data)
    .execute(&mut *tx)
    .await
    .map(|_| ())
    .map_err(|e| log_err(ErrorVerb::Write, e))
}

/// 删除 idx >= `since` 的日志；落在段内时把段切成保留 [start, since) 的新段。
async fn truncate_log_tx(tx: &mut SqliteConnection, since: u64) -> Result<(), StorageError<u64>> {
    let since_i = since as i64;
    sqlx::query("DELETE FROM raft_log WHERE idx >= ?")
        .bind(since_i)
        .execute(&mut *tx)
        .await
        .map_err(|e| log_err(ErrorVerb::Delete, e))?;
    let segs = sqlx::query(
        "SELECT start_idx, end_idx, data FROM raft_log_segment WHERE end_idx >= ? \
         ORDER BY start_idx",
    )
    .bind(since_i)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| log_err(ErrorVerb::Read, e))?;
    for row in segs {
        let start: i64 = row.get("start_idx");
        let end: i64 = row.get("end_idx");
        let stored: Vec<u8> = row.get("data");
        let mut entries = decode_segment(&stored, start, end)?;
        if start >= since_i {
            sqlx::query("DELETE FROM raft_log_segment WHERE start_idx = ?")
                .bind(start)
                .execute(&mut *tx)
                .await
                .map_err(|e| log_err(ErrorVerb::Delete, e))?;
        } else {
            // start < since <= end：保留 [start, since)。
            entries.truncate((since - start as u64) as usize);
            write_segment_tx(tx, &entries).await?;
        }
    }
    Ok(())
}

/// 删除 idx <= `upto` 的日志；落在段内时把剩余 [upto+1, end] 重写为新段。
async fn purge_log_tx(tx: &mut SqliteConnection, upto: u64) -> Result<(), StorageError<u64>> {
    let upto_i = upto as i64;
    sqlx::query("DELETE FROM raft_log WHERE idx <= ?")
        .bind(upto_i)
        .execute(&mut *tx)
        .await
        .map_err(|e| log_err(ErrorVerb::Delete, e))?;
    let segs = sqlx::query(
        "SELECT start_idx, end_idx, data FROM raft_log_segment WHERE start_idx <= ? \
         ORDER BY start_idx",
    )
    .bind(upto_i)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| log_err(ErrorVerb::Read, e))?;
    for row in segs {
        let start: i64 = row.get("start_idx");
        let end: i64 = row.get("end_idx");
        let stored: Vec<u8> = row.get("data");
        let mut entries = decode_segment(&stored, start, end)?;
        if end <= upto_i {
            sqlx::query("DELETE FROM raft_log_segment WHERE start_idx = ?")
                .bind(start)
                .execute(&mut *tx)
                .await
                .map_err(|e| log_err(ErrorVerb::Delete, e))?;
        } else {
            // start <= upto < end：保留 (upto, end]。
            let kept = entries.split_off((upto + 1 - start as u64) as usize);
            sqlx::query("DELETE FROM raft_log_segment WHERE start_idx = ?")
                .bind(start)
                .execute(&mut *tx)
                .await
                .map_err(|e| log_err(ErrorVerb::Delete, e))?;
            write_segment_tx(tx, &kept).await?;
        }
    }
    Ok(())
}

/// 追加覆盖 [start,end] 时，从已有段中挖掉该区间（左右两侧保留为段）。
/// 正常追加只接在末尾，不会命中；用于重试/覆盖写时避免行与段重叠。
async fn cut_segments_tx(
    tx: &mut SqliteConnection,
    start: u64,
    end: u64,
) -> Result<(), StorageError<u64>> {
    let (start_i, end_i) = (start as i64, end as i64);
    let segs = sqlx::query(
        "SELECT start_idx, end_idx, data FROM raft_log_segment \
         WHERE end_idx >= ? AND start_idx <= ? ORDER BY start_idx",
    )
    .bind(start_i)
    .bind(end_i)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| log_err(ErrorVerb::Read, e))?;
    for row in segs {
        let seg_start: i64 = row.get("start_idx");
        let seg_end: i64 = row.get("end_idx");
        let stored: Vec<u8> = row.get("data");
        let entries = decode_segment(&stored, seg_start, seg_end)?;
        let left_len = entries.partition_point(|e| e.log_id.index < start);
        let right_from = entries.partition_point(|e| e.log_id.index <= end);
        sqlx::query("DELETE FROM raft_log_segment WHERE start_idx = ?")
            .bind(seg_start)
            .execute(&mut *tx)
            .await
            .map_err(|e| log_err(ErrorVerb::Delete, e))?;
        if left_len > 0 {
            write_segment_tx(tx, &entries[..left_len]).await?;
        }
        if right_from < entries.len() {
            write_segment_tx(tx, &entries[right_from..]).await?;
        }
    }
    Ok(())
}

/// 追加事务内把最新连续行压段（>= [`SEGMENT_MIN_ENTRIES`] 且原始 JSON 总量有界）。
/// 段与行不重叠；写段/删行同事务，崩溃只会留下完整旧行或完整新段。
async fn compact_rows_tx(tx: &mut SqliteConnection) -> Result<(), StorageError<u64>> {
    let rows = sqlx::query("SELECT idx, entry FROM raft_log ORDER BY idx DESC LIMIT ?")
        .bind(SEGMENT_MAX_ENTRIES as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| log_err(ErrorVerb::Read, e))?;
    if rows.len() < SEGMENT_MIN_ENTRIES {
        return Ok(());
    }
    let mut run: Vec<(i64, Entry<TypeConfig>)> = Vec::new();
    let mut raw_bytes = 0usize;
    let mut expected: Option<i64> = None;
    for row in rows {
        let idx: i64 = row.get("idx");
        if expected.is_some_and(|prev| idx != prev) {
            break;
        }
        let stored: String = row.get("entry");
        let (entry, raw_len) = decode_entry_with_len(&stored)?;
        if entry.log_id.index != idx as u64 {
            return Err(log_corrupt(format!(
                "row {idx} holds entry index {}",
                entry.log_id.index
            )));
        }
        if raw_bytes + raw_len > SEGMENT_MAX_RAW_BYTES {
            break;
        }
        raw_bytes += raw_len;
        run.push((idx, entry));
        expected = Some(idx - 1);
    }
    if run.len() < SEGMENT_MIN_ENTRIES {
        return Ok(());
    }
    run.reverse();
    let start = run[0].0;
    let end = run[run.len() - 1].0;
    let entries: Vec<Entry<TypeConfig>> = run.into_iter().map(|(_, entry)| entry).collect();
    let overlap: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raft_log_segment WHERE end_idx >= ? AND start_idx <= ?",
    )
    .bind(start)
    .bind(end)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| log_err(ErrorVerb::Read, e))?;
    if overlap != 0 {
        return Err(log_err(
            ErrorVerb::Write,
            format!("raft log segment overlaps rows [{start},{end}]"),
        ));
    }
    write_segment_tx(tx, &entries).await?;
    let deleted = sqlx::query("DELETE FROM raft_log WHERE idx >= ? AND idx <= ?")
        .bind(start)
        .bind(end)
        .execute(&mut *tx)
        .await
        .map_err(|e| log_err(ErrorVerb::Delete, e))?
        .rows_affected();
    if deleted as usize != entries.len() {
        return Err(log_err(
            ErrorVerb::Write,
            format!(
                "raft log compaction deleted {deleted} rows, expected {}",
                entries.len()
            ),
        ));
    }
    Ok(())
}

/// 读以 `end_idx` 结尾的段的最后一条 LogId（get_log_state 用）。
async fn read_segment_last(
    tx: &mut SqliteConnection,
    end_idx: i64,
) -> Result<Option<LogId<u64>>, StorageError<u64>> {
    let row = sqlx::query("SELECT start_idx, data FROM raft_log_segment WHERE end_idx = ?")
        .bind(end_idx)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| log_err(ErrorVerb::Read, e))?;
    let Some(row) = row else {
        return Err(log_corrupt(format!(
            "segment ending at {end_idx} disappeared"
        )));
    };
    let start: i64 = row.get("start_idx");
    let data: Vec<u8> = row.get("data");
    Ok(decode_segment(&data, start, end_idx)?
        .last()
        .map(|e| e.log_id))
}

/// 在调用方事务内写快照：> 1 块时清旧块、逐块插入，并在 `raft_snapshot.data`
/// 留清单头；否则写旧版整行 BLOB。清块与写 meta 同事务，崩溃不会留下半套块。
async fn write_snapshot_tx(
    tx: &mut SqliteConnection,
    meta_json: &str,
    snapshot_id: &str,
    bytes: &[u8],
) -> Result<(), StorageError<u64>> {
    sqlx::query("DELETE FROM raft_snapshot_chunk")
        .execute(&mut *tx)
        .await
        .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
    let data = if bytes.len() > SNAPSHOT_CHUNK_BYTES {
        let count = bytes.len().div_ceil(SNAPSHOT_CHUNK_BYTES);
        for (seq, chunk) in bytes.chunks(SNAPSHOT_CHUNK_BYTES).enumerate() {
            sqlx::query(
                "INSERT INTO raft_snapshot_chunk (snapshot_id, seq, data) VALUES (?, ?, ?)",
            )
            .bind(snapshot_id)
            .bind(seq as i64)
            .bind(chunk)
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        }
        let mut manifest = Vec::with_capacity(SNAPSHOT_CHUNK_HEADER_LEN);
        manifest.extend_from_slice(SNAPSHOT_CHUNK_MAGIC);
        manifest.extend_from_slice(&(count as u32).to_le_bytes());
        manifest.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        manifest
    } else {
        bytes.to_vec()
    };
    sqlx::query("INSERT OR REPLACE INTO raft_snapshot (id, meta, data) VALUES (1, ?, ?)")
        .bind(meta_json)
        .bind(data)
        .execute(&mut *tx)
        .await
        .map(|_| ())
        .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))
}

/// 读快照数据：`raft_snapshot.data` 是分块清单时按 `snapshot_id` 拼接并校验块数/
/// 总长；否则按旧版整行 BLOB 返回。缺块、乱序、截断一律 fail-closed。
async fn read_snapshot_data(
    conn: &mut SqliteConnection,
    snapshot_id: &str,
    stored: Vec<u8>,
    max_bytes: u64,
) -> Result<Vec<u8>, StorageError<u64>> {
    let Some(rest) = stored.strip_prefix(SNAPSHOT_CHUNK_MAGIC.as_slice()) else {
        return Ok(stored);
    };
    let corrupt = |what: String| {
        storage_err(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Read,
            std::io::Error::other(format!("corrupt raft snapshot chunk manifest: {what}")),
        )
    };
    if rest.len() != SNAPSHOT_CHUNK_HEADER_LEN - SNAPSHOT_CHUNK_MAGIC.len() {
        return Err(corrupt("bad header".into()));
    }
    let count = u32::from_le_bytes(rest[..4].try_into().expect("4 bytes")) as usize;
    let total = u64::from_le_bytes(rest[4..12].try_into().expect("8 bytes"));
    if total > max_bytes {
        return Err(corrupt(format!(
            "{total} bytes exceeds limit {max_bytes} (raft.snapshot_max_bytes)"
        )));
    }
    let rows =
        sqlx::query("SELECT seq, data FROM raft_snapshot_chunk WHERE snapshot_id = ? ORDER BY seq")
            .bind(snapshot_id)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
    if rows.len() != count {
        return Err(corrupt(format!(
            "expected {count} chunks, found {}",
            rows.len()
        )));
    }
    let mut out = Vec::with_capacity(total as usize);
    for (want, row) in rows.into_iter().enumerate() {
        let seq: i64 = row.get("seq");
        if seq != want as i64 {
            return Err(corrupt("chunk sequence gap".into()));
        }
        let chunk: Vec<u8> = row.get("data");
        out.extend_from_slice(&chunk);
    }
    if out.len() as u64 != total {
        return Err(corrupt(format!(
            "expected {total} bytes, found {}",
            out.len()
        )));
    }
    Ok(out)
}

/// SQLite 日志存储。`Raft::new` 按值消费；克隆共享同一连接池。
#[derive(Clone)]
pub struct SqliteLogStore {
    pool: SqlitePool,
}

impl SqliteLogStore {
    pub async fn open(path: &Path) -> Result<Self, sqlx::Error> {
        Ok(Self {
            pool: connect(path).await?,
        })
    }

    /// 是否已有任何日志（含未 apply 的成员日志）：`spawn` 据此避免重复 initialize。
    pub async fn has_logs(&self) -> Result<bool, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM raft_log) + (SELECT COUNT(*) FROM raft_log_segment)",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }

    /// 追加事务体：写行 + 尝试压段，提交前返回；callback 由 [`Self::append`] 负责。
    async fn append_entries(&self, entries: &[Entry<TypeConfig>]) -> Result<(), StorageError<u64>> {
        // 压段前先 SELECT 再写；deferred 事务在 WAL 下升级写会立即 BUSY_SNAPSHOT，
        // busy_timeout 不生效。IMMEDIATE 先拿写锁，等锁走 busy_timeout。
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| log_err(ErrorVerb::Write, e))?;
        // 批次通常是 [first,last] 连续；覆盖已有段时先挖掉这些 idx。
        if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
            let contiguous = entries
                .iter()
                .enumerate()
                .all(|(i, e)| e.log_id.index == first.log_id.index + i as u64);
            if contiguous {
                cut_segments_tx(&mut tx, first.log_id.index, last.log_id.index).await?;
            } else {
                for entry in entries {
                    cut_segments_tx(&mut tx, entry.log_id.index, entry.log_id.index).await?;
                }
            }
        }
        for entry in entries {
            let json = encode_entry(entry)?;
            sqlx::query("INSERT OR REPLACE INTO raft_log (idx, entry) VALUES (?, ?)")
                .bind(entry.log_id.index as i64)
                .bind(json)
                .execute(&mut *tx)
                .await
                .map_err(|e| log_err(ErrorVerb::Write, e))?;
        }
        compact_rows_tx(&mut tx).await?;
        tx.commit().await.map_err(|e| log_err(ErrorVerb::Write, e))
    }

    async fn write_meta(&self, key: &str, value: &str) -> Result<(), StorageError<u64>> {
        sqlx::query("INSERT OR REPLACE INTO raft_meta (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| storage_err(ErrorSubject::Store, ErrorVerb::Write, e))
    }

    async fn read_meta<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StorageError<u64>> {
        let row = sqlx::query("SELECT value FROM raft_meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
        row.map(|r| {
            let s: String = r.get("value");
            decode(ErrorSubject::Store, ErrorVerb::Read, &s)
        })
        .transpose()
    }
}

impl RaftLogReader<TypeConfig> for SqliteLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let start = match range.start_bound() {
            Bound::Included(i) => *i,
            // [start, stop)：Excluded(i) 从 i+1 开始（Minor 1）。
            Bound::Excluded(i) => match i.checked_add(1) {
                Some(v) => v,
                None => return Ok(Vec::new()),
            },
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(i) => match i.checked_sub(1) {
                Some(v) => v,
                None => return Ok(Vec::new()),
            },
            Bound::Unbounded => u64::MAX,
        };
        if start > end || start > i64::MAX as u64 {
            return Ok(Vec::new());
        }
        let end = end.min(i64::MAX as u64);
        // 行与段必须来自同一读快照，否则并发压段可能同时读出重叠内容。
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| log_err(ErrorVerb::Read, e))?;
        let rows =
            sqlx::query("SELECT idx, entry FROM raft_log WHERE idx >= ? AND idx <= ? ORDER BY idx")
                .bind(start as i64)
                .bind(end as i64)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| log_err(ErrorVerb::Read, e))?;
        let segs = sqlx::query(
            "SELECT start_idx, end_idx, data FROM raft_log_segment \
             WHERE end_idx >= ? AND start_idx <= ? ORDER BY start_idx",
        )
        .bind(start as i64)
        .bind(end as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| log_err(ErrorVerb::Read, e))?;
        tx.commit().await.map_err(|e| log_err(ErrorVerb::Read, e))?;

        let mut entries = Vec::new();
        for row in segs {
            let seg_start: i64 = row.get("start_idx");
            let seg_end: i64 = row.get("end_idx");
            let data: Vec<u8> = row.get("data");
            entries.extend(
                decode_segment(&data, seg_start, seg_end)?
                    .into_iter()
                    .filter(|e| e.log_id.index >= start && e.log_id.index <= end),
            );
        }
        for row in rows {
            let idx: i64 = row.get("idx");
            let stored: String = row.get("entry");
            let entry = decode_entry(&stored)?;
            if entry.log_id.index != idx as u64 {
                return Err(log_corrupt(format!(
                    "row {idx} holds entry index {}",
                    entry.log_id.index
                )));
            }
            entries.push(entry);
        }
        entries.sort_by_key(|e| e.log_id.index);
        // 日志必须无洞、无重复；内部空洞说明存储损坏（purge 后的前缀缺口不在此列）。
        for pair in entries.windows(2) {
            let (prev, next) = (pair[0].log_id.index, pair[1].log_id.index);
            if next <= prev {
                return Err(log_corrupt(format!(
                    "duplicate/out-of-order entry index {next} after {prev}"
                )));
            }
            if next != prev + 1 {
                return Err(log_corrupt(format!("index gap between {prev} and {next}")));
            }
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for SqliteLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| log_err(ErrorVerb::Read, e))?;
        let last_purged: Option<LogId<u64>> =
            sqlx::query("SELECT value FROM raft_meta WHERE key = 'last_purged'")
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| storage_err(ErrorSubject::Store, ErrorVerb::Read, e))?
                .map(|r| {
                    let s: String = r.get("value");
                    decode(ErrorSubject::Store, ErrorVerb::Read, &s)
                })
                .transpose()?
                .flatten();
        let row_max: Option<i64> = sqlx::query_scalar("SELECT MAX(idx) FROM raft_log")
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| log_err(ErrorVerb::Read, e))?;
        let seg_max: Option<i64> = sqlx::query_scalar("SELECT MAX(end_idx) FROM raft_log_segment")
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| log_err(ErrorVerb::Read, e))?;
        let last = match (row_max, seg_max) {
            (Some(r), Some(s)) if s >= r => read_segment_last(&mut tx, s).await?,
            (Some(r), _) => {
                let stored: String = sqlx::query_scalar("SELECT entry FROM raft_log WHERE idx = ?")
                    .bind(r)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| log_err(ErrorVerb::Read, e))?;
                let entry = decode_entry(&stored)?;
                if entry.log_id.index != r as u64 {
                    return Err(log_corrupt(format!(
                        "row {r} holds entry index {}",
                        entry.log_id.index
                    )));
                }
                Some(entry.log_id)
            }
            (None, Some(s)) => read_segment_last(&mut tx, s).await?,
            (None, None) => None,
        };
        tx.commit().await.map_err(|e| log_err(ErrorVerb::Read, e))?;
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let json = encode(ErrorSubject::Vote, ErrorVerb::Write, vote)?;
        self.write_meta("vote", &json).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.read_meta("vote").await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let json = encode(ErrorSubject::Store, ErrorVerb::Write, &committed)?;
        self.write_meta("committed", &json).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self
            .read_meta::<Option<LogId<u64>>>("committed")
            .await?
            .flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let result = self.append_entries(&entries).await;
        match &result {
            Ok(()) => callback.log_io_completed(Ok(())),
            // 回调只接受 io::Error；SQLite 已回滚，RaftCore 会按存储错误处理。
            Err(e) => callback.log_io_completed(Err(std::io::Error::other(e.to_string()))),
        }
        result
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_err(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))?;
        truncate_log_tx(&mut tx, log_id.index).await?;
        tx.commit()
            .await
            .map_err(|e| storage_err(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_err(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))?;
        purge_log_tx(&mut tx, log_id.index).await?;
        let json = encode(ErrorSubject::Log(log_id), ErrorVerb::Write, &Some(log_id))?;
        sqlx::query("INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('last_purged', ?)")
            .bind(json)
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Log(log_id), ErrorVerb::Write, e))?;
        tx.commit()
            .await
            .map_err(|e| storage_err(ErrorSubject::Log(log_id), ErrorVerb::Write, e))
    }
}

/// 快照内容：完整 applied LogId + StoredMembership + 基座 KV。
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotData {
    applied_log_id: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    kv: BTreeMap<String, String>,
}

async fn read_applied(
    conn: &mut SqliteConnection,
) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
    let row = sqlx::query("SELECT applied_log_id, membership_json FROM raft_applied WHERE id = 1")
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
    let Some(row) = row else {
        return Ok((None, StoredMembership::default()));
    };
    let applied: Option<LogId<u64>> = match row.get::<Option<String>, _>("applied_log_id") {
        Some(s) => decode(ErrorSubject::StateMachine, ErrorVerb::Read, &s)?,
        None => None,
    };
    let membership: StoredMembership<u64, BasicNode> =
        match row.get::<Option<String>, _>("membership_json") {
            Some(s) => decode(ErrorSubject::StateMachine, ErrorVerb::Read, &s)?,
            None => StoredMembership::default(),
        };
    Ok((applied, membership))
}

async fn read_kv(
    conn: &mut SqliteConnection,
) -> Result<BTreeMap<String, String>, StorageError<u64>> {
    let rows = sqlx::query("SELECT key, value FROM raft_kv")
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get("key"), r.get("value")))
        .collect())
}

/// SQLite 状态机：apply 每条 entry 一个事务，更新数据、`raft_applied`（完整 LogId + 成员）
/// 与 apply ctx 原子提交。生产入口 [`Self::open_with_pool`] 指向主控制库连接池。
#[derive(Clone)]
pub struct SqliteStateMachine {
    pool: SqlitePool,
    max_snapshot_bytes: u64,
    /// build 的 compare-and-write 与 install 的整表替换互斥（旧 build 不得覆盖新快照）。
    snapshot_lock: Arc<tokio::sync::Mutex<()>>,
    /// true：快照含 11 张复制表（控制库）；false：S5 基座 KV（raft.db 自测）。
    control: bool,
    /// witness 安装时 owner 用哨兵 0（无本地管理员）。
    witness: bool,
    /// 安装提交后通知缓存/目录对账任务（S7.2）。
    install_notify: Option<tokio::sync::watch::Sender<u64>>,
    /// 与 [`super::RaftHandle`] 共享的观测计数（apply 失败在此累加）。
    counters: Arc<Metrics>,
    /// apply 门控：level > 此值的命令拒绝（控制库 = `min(配置, MAX)`；基座 KV = MAX）。
    feature_level: u8,
    /// 已提交激活标记（`schema_meta.raft_feature_level`，缺省 1）的进程内缓存。
    activation: Arc<std::sync::atomic::AtomicU8>,
}

impl SqliteStateMachine {
    pub async fn open(path: &Path, max_snapshot_bytes: u64) -> Result<Self, sqlx::Error> {
        let pool = connect(path).await?;
        // 基座 KV 不经网络/控制命令，无激活概念：门控取二进制上限。
        Ok(Self::with_pool(
            pool,
            max_snapshot_bytes,
            false,
            false,
            None,
            super::compat::MAX_FEATURE_LEVEL,
        ))
    }

    /// 主控制库状态机（raft.db 只存日志/投票）。schema 幂等重放，`raft_applied`/ctx 已存在。
    pub async fn open_with_pool(
        pool: SqlitePool,
        max_snapshot_bytes: u64,
        witness: bool,
        install_notify: Option<tokio::sync::watch::Sender<u64>>,
        feature_level: u8,
    ) -> Result<Self, sqlx::Error> {
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        let marker = super::compat::read_activation(&pool)
            .await
            .map_err(sqlx::Error::Protocol)?;
        let sm = Self::with_pool(
            pool,
            max_snapshot_bytes,
            true,
            witness,
            install_notify,
            super::compat::advertised_feature(feature_level),
        );
        sm.activation.store(
            marker.max(super::compat::MIN_FEATURE_LEVEL),
            Ordering::Relaxed,
        );
        Ok(sm)
    }

    fn with_pool(
        pool: SqlitePool,
        max_snapshot_bytes: u64,
        control: bool,
        witness: bool,
        install_notify: Option<tokio::sync::watch::Sender<u64>>,
        feature_level: u8,
    ) -> Self {
        Self {
            pool,
            max_snapshot_bytes,
            snapshot_lock: Arc::new(tokio::sync::Mutex::new(())),
            control,
            witness,
            install_notify,
            counters: Arc::new(Metrics::default()),
            feature_level,
            activation: Arc::new(std::sync::atomic::AtomicU8::new(
                super::compat::MIN_FEATURE_LEVEL,
            )),
        }
    }

    /// 本节点已提交的激活级别（R8 启动守卫/状态诊断）。
    pub fn activation_level(&self) -> u8 {
        self.activation.load(Ordering::Relaxed)
    }

    /// apply 写入的控制库连接池（leader append 前的只读配额判定用）。
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// 与 [`super::RaftHandle`] 共享的观测计数（apply 失败在此累加）。
    pub fn counters(&self) -> &Arc<Metrics> {
        &self.counters
    }

    /// 已应用 LogId 与成员（`RaftStateMachine::applied_state` 的固有版本，测试/S6 用）。
    pub async fn applied(
        &self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        read_applied(&mut conn).await
    }

    /// 基座 KV 读取（S6 换成控制表查询）。
    pub async fn get(&self, key: &str) -> Result<Option<String>, StorageError<u64>> {
        let row = sqlx::query("SELECT value FROM raft_kv WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        Ok(row.map(|r| r.get("value")))
    }

    /// I1：applied/membership 与 kv 必须来自同一 SQLite 读快照（单事务两次 SELECT）。
    async fn snapshot_parts(
        &self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, BasicNode>,
            BTreeMap<String, String>,
        ),
        StorageError<u64>,
    > {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        let (applied, membership) = read_applied(&mut tx).await?;
        let kv = read_kv(&mut tx).await?;
        tx.commit()
            .await
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        Ok((applied, membership, kv))
    }

    fn data_err(e: data::DataError) -> StorageError<u64> {
        storage_err(
            ErrorSubject::StateMachine,
            ErrorVerb::Write,
            std::io::Error::other(e.to_string()),
        )
    }

    /// S7.2：11 张复制表 + applied/membership + 激活级别同一事务读，编码为带校验和的快照。
    async fn control_snapshot_bytes(
        &self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, BasicNode>,
            Vec<u8>,
        ),
        StorageError<u64>,
    > {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        let (applied, membership) = read_applied(&mut tx).await?;
        let control = data::read_tx(&mut tx).await.map_err(Self::data_err)?;
        let level = super::compat::read_activation_conn(&mut tx)
            .await
            .map_err(|e| {
                storage_err(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    std::io::Error::other(e),
                )
            })?;
        // level 3：运行态与控制面必须在同一读事务内取，避免快照撕裂。
        // 低于 3：运行态是节点本地状态，不进快照。
        let runstate = if level >= super::compat::FEATURE_LEVEL_3 {
            Some(
                super::runstate::read_runstate_tx(&mut tx)
                    .await
                    .map_err(Self::data_err)?,
            )
        } else {
            None
        };
        tx.commit()
            .await
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        let bytes = match &runstate {
            Some(rs) => super::runstate::encode_runstate_snapshot(&control, rs, level),
            None => data::encode_snapshot_at(&control, level),
        }
        .map_err(Self::data_err)?;
        Ok((applied, membership, bytes))
    }

    /// S7.2：单事务安装 11 张复制表；守卫窗口与 raft_applied/快照同事务提交。
    ///
    /// OSSCTL2 携带 `feature_level`：marker 与数据同事务 `max(marker, snapshot_level)`
    /// （只升不降）。级别高于本机配置也照装（节点必须先追平 state；R8 启动守卫会
    /// 要求下次重启前把 `[raft].feature_level` 调到该级别）。
    async fn install_control(
        &self,
        meta: &SnapshotMeta<u64, BasicNode>,
        bytes: &[u8],
    ) -> Result<(), StorageError<u64>> {
        let snapshot = data::decode_snapshot(bytes).map_err(Self::data_err)?;
        // 单向门：本机已激活 level 3（或规范化账户表里已有行）后，level<3 快照不可能
        // 携带账户行，整表安装会静默清空账户（marker 又不清除），在任何锁/写之前拒绝。
        // marker 与 activation 是独立事实（standalone→raft 直启可 marker 有而激活 1）；
        // 空表上的 marker 只是启动规则，允许加入旧级别集群（无行可清）。
        let local_activation = self.activation.load(Ordering::Relaxed);
        if snapshot.feature_level < super::compat::FEATURE_LEVEL_3 {
            let canonical: Option<String> =
                sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = ?")
                    .bind(crate::db::META_ACCOUNTS_CANONICAL)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
            let canonical_rows: i64 = if canonical.is_some() {
                sqlx::query_scalar(
                    "SELECT (SELECT COUNT(*) FROM accounts) + \
                            (SELECT COUNT(*) FROM account_members)",
                )
                .fetch_one(&self.pool)
                .await
                .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?
            } else {
                0
            };
            if local_activation >= super::compat::FEATURE_LEVEL_3 || canonical_rows != 0 {
                return Err(Self::data_err(data::DataError::Format(format!(
                    "node activation level {local_activation} (canonical accounts: {}, rows: \
                     {canonical_rows}) cannot install a level-{} snapshot",
                    canonical.is_some(),
                    snapshot.feature_level
                ))));
            }
        }
        // 只认识 m8.runstate；未知扩展仍然 fail-closed（丢弃即数据丢失）。
        let mut runstate = None;
        for (name, ext) in &snapshot.extensions {
            if name == super::runstate::RUNSTATE_EXTENSION {
                runstate = Some(
                    super::runstate::from_snapshot_extension(
                        ext,
                        super::runstate::RUNSTATE_EXTENSION_VERSION,
                    )
                    .map_err(Self::data_err)?,
                );
            } else {
                return Err(Self::data_err(data::DataError::Format(format!(
                    "snapshot carries unsupported extension {name:?}; refusing to install and \
                     drop it"
                ))));
            }
        }
        let runstate = runstate.unwrap_or_default();
        // level-3 快照只是构建者本地表的拷贝。配置 feature 4 的节点若装上带运行态行的
        // level-3 快照，就会在无 marker 的情况下持有非日志行；直接拒绝，避免运行期带病
        // 进入 feature 4。空 level-3 快照仍可装（运行态为空）。
        if snapshot.feature_level >= super::compat::FEATURE_LEVEL_3
            && snapshot.feature_level < super::compat::FEATURE_LEVEL_4
            && self.feature_level >= super::compat::FEATURE_LEVEL_4
            && (!runstate.jobs.is_empty()
                || !runstate.job_tasks.is_empty()
                || !runstate.inventory_runs.is_empty()
                || !runstate.inventory_cursors.is_empty()
                || !runstate.inventory_incremental.is_empty()
                || !runstate.tier_restores.is_empty())
        {
            return Err(Self::data_err(data::DataError::Format(format!(
                "[raft].feature_level={} cannot install a level-{} snapshot carrying \
                 non-log jobs/job_tasks/inventory_runs/inventory_cursors/\
                 inventory_incremental/tier_restores; finish feature-4 activation on the \
                 source or drain/clear these tables and re-export",
                self.feature_level, snapshot.feature_level
            ))));
        }
        // v1.23.0 的 feature-4 inventory 行是节点本地行，level-4 快照也不可信；配置 5
        // 只接受 level-5 快照携带的 inventory 运行态。
        if snapshot.feature_level >= super::compat::FEATURE_LEVEL_3
            && snapshot.feature_level < super::compat::FEATURE_LEVEL_5
            && self.feature_level >= super::compat::FEATURE_LEVEL_5
            && (!runstate.inventory_runs.is_empty()
                || !runstate.inventory_cursors.is_empty()
                || !runstate.inventory_incremental.is_empty())
        {
            return Err(Self::data_err(data::DataError::Format(format!(
                "[raft].feature_level={} cannot install a level-{} snapshot carrying \
                 non-log inventory_runs/inventory_cursors/inventory_incremental; \
                 finish inventory activation on the source or drain/export and re-export \
                 at level 5",
                self.feature_level, snapshot.feature_level
            ))));
        }
        let _guard = self.snapshot_lock.lock().await;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        let admin_id: Option<i64> =
            sqlx::query_scalar("SELECT id FROM users WHERE is_admin = 1 ORDER BY id LIMIT 1")
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        let ctx = InstallCtx {
            admin_id,
            witness: self.witness,
            canonical: snapshot.feature_level >= super::compat::FEATURE_LEVEL_3,
        };
        data::install_tx(&mut tx, &snapshot.data, &ctx)
            .await
            .map_err(Self::data_err)?;
        // level 3：九表与控制面同一事务替换；无扩展 = 空运行态（本二进制在 level 3
        // 总是写出扩展）。低于 3：不动节点本地运行态。
        if snapshot.feature_level >= super::compat::FEATURE_LEVEL_3 {
            super::runstate::install_runstate_tx(&mut tx, &runstate)
                .await
                .map_err(Self::data_err)?;
            // marker 只认证 level-4 来源；普通 level-3 安装必须撤销旧标记。
            if snapshot.feature_level >= super::compat::FEATURE_LEVEL_4 {
                super::compat::mark_runstate_activated_tx(&mut tx)
                    .await
                    .map_err(|e| {
                        storage_err(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Write,
                            std::io::Error::other(e),
                        )
                    })?;
            } else {
                super::compat::clear_runstate_activated_tx(&mut tx)
                    .await
                    .map_err(|e| {
                        storage_err(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Write,
                            std::io::Error::other(e),
                        )
                    })?;
            }
            // inventory 有独立的 level-5 marker；level-3/4 快照撤销它。
            if snapshot.feature_level >= super::compat::FEATURE_LEVEL_5 {
                super::compat::mark_inventory_activated_tx(&mut tx)
                    .await
                    .map_err(|e| {
                        storage_err(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Write,
                            std::io::Error::other(e),
                        )
                    })?;
            } else {
                super::compat::clear_inventory_activated_tx(&mut tx)
                    .await
                    .map_err(|e| {
                        storage_err(
                            ErrorSubject::Snapshot(None),
                            ErrorVerb::Write,
                            std::io::Error::other(e),
                        )
                    })?;
            }
        }
        // 只升不降；OSSCTL1（level 1）不写 marker，保持旧安装足迹不变。
        if snapshot.feature_level > super::compat::MIN_FEATURE_LEVEL {
            super::compat::bump_activation_tx(&mut tx, snapshot.feature_level)
                .await
                .map_err(|e| {
                    storage_err(
                        ErrorSubject::Snapshot(None),
                        ErrorVerb::Write,
                        std::io::Error::other(e),
                    )
                })?;
        }
        let meta_json = encode(ErrorSubject::Snapshot(None), ErrorVerb::Write, meta)?;
        let applied_json = encode(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            &meta.last_log_id,
        )?;
        let membership_json = encode(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            &meta.last_membership,
        )?;
        sqlx::query(
            "INSERT OR REPLACE INTO raft_applied (id, applied_log_id, membership_json) VALUES (1, ?, ?)",
        )
        .bind(applied_json)
        .bind(membership_json)
        .execute(&mut *tx)
        .await
        .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        write_snapshot_tx(&mut tx, &meta_json, &meta.snapshot_id, bytes).await?;
        sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        tx.commit()
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        self.activation
            .fetch_max(snapshot.feature_level, Ordering::Relaxed);
        if let Some(tx) = &self.install_notify {
            let _ = tx.send(meta.last_log_id.map(|l| l.index).unwrap_or(0));
        }
        Ok(())
    }

    /// 仅在比已存快照新时写入；与 install 共用 `snapshot_lock`，避免旧 build 覆盖新行。
    async fn persist_snapshot(
        &self,
        meta: &SnapshotMeta<u64, BasicNode>,
        bytes: &[u8],
    ) -> Result<(), StorageError<u64>> {
        let _guard = self.snapshot_lock.lock().await;
        // 比较与写入同一事务：分块清/写不能与并发读看到半套块。
        // 先 SELECT 再写 → 用 IMMEDIATE 避免 WAL 读事务升级时立即 BUSY_SNAPSHOT。
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        let current: Option<String> =
            sqlx::query_scalar("SELECT meta FROM raft_snapshot WHERE id = 1")
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        if let Some(json) = current {
            let existing: SnapshotMeta<u64, BasicNode> =
                decode(ErrorSubject::Snapshot(None), ErrorVerb::Read, &json)?;
            if existing.last_log_id >= meta.last_log_id {
                return Ok(());
            }
        }
        let meta_json = encode(ErrorSubject::Snapshot(None), ErrorVerb::Write, meta)?;
        write_snapshot_tx(&mut tx, &meta_json, &meta.snapshot_id, bytes).await?;
        tx.commit()
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))
    }

    /// 已落库快照的字节数（分块快照 = 各块之和，不含清单头；无快照 = 0；
    /// 查询失败记 0 并告警，不因 gauge 拖垮 /metrics）。
    pub async fn snapshot_bytes(&self) -> u64 {
        let query = "SELECT COALESCE((SELECT SUM(length(data)) FROM raft_snapshot_chunk), 0), \
                     COALESCE((SELECT length(data) FROM raft_snapshot WHERE id = 1), 0)";
        match sqlx::query(query).fetch_one(&self.pool).await {
            Ok(row) => {
                let chunk_bytes: i64 = row.get(0);
                let whole_row_bytes: i64 = row.get(1);
                // 有块行时 data 只是清单头；旧库/小快照没有块行，按整行长度计。
                let bytes = if chunk_bytes > 0 {
                    chunk_bytes
                } else {
                    whole_row_bytes
                };
                bytes.max(0) as u64
            }
            Err(e) => {
                tracing::warn!(error = %e, "raft snapshot bytes 查询失败；gauge 记 0");
                0
            }
        }
    }
}

impl RaftStateMachine<TypeConfig> for SqliteStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        self.applied().await
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ControlResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // 计数口径：一次 apply 批失败 +1（openraft 随后 fail-stop，不会重试同一批）。
        let result: Result<_, StorageError<u64>> = async {
            let entries: Vec<_> = entries.into_iter().collect();
            // 成员是跨 entry 状态（空白/成员日志也要落），applied 每条即本 entry。
            let (_, mut last_membership) = self.applied().await?;
            let mut responses = Vec::with_capacity(entries.len());
            // 每条 entry 一个事务：控制表写入 + raft_applied + ctx 原子提交。
            // 失败只回滚当前条（fail-stop 后 openraft 不再前进），已提交条目的响应不重复。
            for entry in &entries {
                let log_id = entry.log_id;
                if let Some(m) = entry.get_membership() {
                    last_membership = StoredMembership::new(Some(log_id), m.clone());
                }
                // R10：命令级别高于本机配置 → fail-stop（在开启事务前拒绝，绝不半应用）。
                let required = match &entry.payload {
                    EntryPayload::Normal(cmd) => cmd.min_feature_level(),
                    EntryPayload::Blank | EntryPayload::Membership(_) => {
                        super::compat::MIN_FEATURE_LEVEL
                    }
                };
                if required > self.feature_level {
                    return Err(storage_err(
                        ErrorSubject::Apply(log_id),
                        ErrorVerb::Write,
                        std::io::Error::other(format!(
                            "control command requires raft feature level {required}, but \
                             [raft].feature_level = {}; refusing to apply",
                            self.feature_level
                        )),
                    ));
                }

                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
                // 守卫放行窗口：与数据/raft_applied 同一事务；提交前复位。
                sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
                let (response, has_effects) = match &entry.payload {
                    EntryPayload::Normal(cmd) => {
                        let applied = {
                            let mut cw = apply::control_tx(&mut tx);
                            apply::apply_command(&mut cw, cmd).await.map_err(|e| {
                                storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e)
                            })?
                        };
                        // R10：与首条 level-2 entry 同事务写激活标记（业务拒绝的响应也写）。
                        if self.control && required > self.activation.load(Ordering::Relaxed) {
                            super::compat::bump_activation_tx(&mut tx, required)
                                .await
                                .map_err(|e| {
                                    storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e)
                                })?;
                        }
                        // level-4 命令 apply 是运行态“日志事实”的直接证据；与行同事务
                        // 置 marker（业务拒绝也证明集群已到 4）。
                        if self.control && required >= super::compat::FEATURE_LEVEL_4 {
                            super::compat::mark_runstate_activated_tx(&mut tx)
                                .await
                                .map_err(|e| {
                                    storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e)
                                })?;
                        }
                        // level-5 命令是 inventory 运行态“日志事实”的证据；独立 marker。
                        if self.control && required >= super::compat::FEATURE_LEVEL_5 {
                            super::compat::mark_inventory_activated_tx(&mut tx)
                                .await
                                .map_err(|e| {
                                    storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e)
                                })?;
                        }
                        (applied.response, !applied.effects.is_empty())
                    }
                    EntryPayload::Blank | EntryPayload::Membership(_) => (ControlResponse::Ok, false),
                };
                let applied_json = encode(ErrorSubject::StateMachine, ErrorVerb::Write, &Some(log_id))?;
                let membership_json = encode(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    &last_membership,
                )?;
                sqlx::query(
                    "INSERT OR REPLACE INTO raft_applied (id, applied_log_id, membership_json) VALUES (1, ?, ?)",
                )
                .bind(applied_json)
                .bind(membership_json)
                .execute(&mut *tx)
                .await
                .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
                sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
                tx.commit()
                    .await
                    .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
                if self.control {
                    self.activation.fetch_max(required, Ordering::Relaxed);
                }
                // S7.2：followers 也执行 post-commit 副作用（桶目录/缓存）。旁路通知由
                // state 的 watcher 做幂等全量对账（leader 上重复执行无害）。
                if has_effects && let Some(tx) = &self.install_notify {
                    let _ = tx.send(log_id.index);
                }
                responses.push(response);
            }
            Ok(responses)
        }
        .await;
        if result.is_err() {
            self.counters
                .apply_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.get_ref().clone();
        if bytes.len() as u64 > self.max_snapshot_bytes {
            return Err(storage_err(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                std::io::Error::other(format!(
                    "snapshot {} bytes exceeds limit {} (raft.snapshot_max_bytes)",
                    bytes.len(),
                    self.max_snapshot_bytes
                )),
            ));
        }
        if self.control {
            if !data::is_control_snapshot(&bytes) {
                return Err(storage_err(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Write,
                    std::io::Error::other(
                        "control state machine requires OSSCTL1/OSSCTL2/OSSCTL3 snapshot \
                         (legacy KV snapshot rejected)",
                    ),
                ));
            }
            return self.install_control(meta, &bytes).await;
        }
        let data: SnapshotData = serde_json::from_slice(&bytes)
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        if data.applied_log_id != meta.last_log_id || data.last_membership != meta.last_membership {
            return Err(storage_err(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                std::io::Error::other("snapshot payload disagrees with snapshot meta"),
            ));
        }

        let meta_json = encode(ErrorSubject::Snapshot(None), ErrorVerb::Write, meta)?;
        // 与 build 的 compare-and-write 互斥；安装期间同样打开守卫窗口（Minor 2），
        // DELETE/INSERT 与 ctx=1 同事务，提交前复位。
        let _guard = self.snapshot_lock.lock().await;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        sqlx::query("DELETE FROM raft_kv")
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        for (key, value) in &data.kv {
            sqlx::query("INSERT INTO raft_kv (key, value) VALUES (?, ?)")
                .bind(key)
                .bind(value)
                .execute(&mut *tx)
                .await
                .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        }
        let applied_json = encode(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            &meta.last_log_id,
        )?;
        let membership_json = encode(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
            &meta.last_membership,
        )?;
        sqlx::query(
            "INSERT OR REPLACE INTO raft_applied (id, applied_log_id, membership_json) VALUES (1, ?, ?)",
        )
        .bind(applied_json)
        .bind(membership_json)
        .execute(&mut *tx)
        .await
        .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        write_snapshot_tx(&mut tx, &meta_json, &meta.snapshot_id, &bytes).await?;
        sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        tx.commit()
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        // meta 行与块行必须来自同一读快照，避免读到替换中的 meta + 旧块。
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        let row = sqlx::query("SELECT meta, data FROM raft_snapshot WHERE id = 1")
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let meta_json: String = row.get("meta");
        let meta: SnapshotMeta<u64, BasicNode> =
            decode(ErrorSubject::Snapshot(None), ErrorVerb::Read, &meta_json)?;
        let stored: Vec<u8> = row.get("data");
        let data =
            read_snapshot_data(&mut tx, &meta.snapshot_id, stored, self.max_snapshot_bytes).await?;
        tx.commit()
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let (applied, membership, bytes) = if self.control {
            self.control_snapshot_bytes().await?
        } else {
            let (applied, membership, kv) = self.snapshot_parts().await?;
            let data = SnapshotData {
                applied_log_id: applied,
                last_membership: membership.clone(),
                kv,
            };
            let bytes = serde_json::to_vec(&data)
                .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
            (applied, membership, bytes)
        };
        // Minor 3：构造即拒绝超限快照，避免落库后 install/传输必然失败。
        if bytes.len() as u64 > self.max_snapshot_bytes {
            return Err(storage_err(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                std::io::Error::other(format!(
                    "snapshot {} bytes exceeds limit {} (raft.snapshot_max_bytes)",
                    bytes.len(),
                    self.max_snapshot_bytes
                )),
            ));
        }
        let snapshot_id = format!(
            "{}-{}",
            applied.map(|l| l.index).unwrap_or(0),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        );
        let meta = SnapshotMeta {
            last_log_id: applied,
            last_membership: membership,
            snapshot_id,
        };
        // I1：读快照与写入之间可能有更新条目被 apply；旧 build 不得覆盖更新的 raft_snapshot。
        self.persist_snapshot(&meta, &bytes).await?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::ControlCommand;
    use openraft::entry::Entry;
    use openraft::{CommittedLeaderId, RaftSnapshotBuilder};

    fn log_id(index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(1, 1), index)
    }

    fn snap_meta(index: u64, id: &str) -> SnapshotMeta<u64, BasicNode> {
        SnapshotMeta {
            last_log_id: Some(log_id(index)),
            last_membership: StoredMembership::default(),
            snapshot_id: id.to_string(),
        }
    }

    async fn test_machine(dir: &Path) -> (SqliteStateMachine, SqlitePool) {
        let pool = connect(&dir.join("sm.db")).await.unwrap();
        let sm = SqliteStateMachine::with_pool(pool.clone(), 1024 * 1024, false, false, None, 1);
        (sm, pool)
    }

    async fn set(sm: &mut SqliteStateMachine, index: u64, value: &str) {
        RaftStateMachine::<TypeConfig>::apply(
            sm,
            [Entry {
                log_id: log_id(index),
                payload: EntryPayload::Normal(ControlCommand::Set {
                    key: "k".into(),
                    value: value.into(),
                }),
            }],
        )
        .await
        .unwrap();
    }

    /// I1：applied 与 kv 的读取必须落在同一事务快照；并发 apply 不得造成 meta/data 错位。
    #[tokio::test]
    async fn snapshot_reads_share_one_transaction_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sm, pool) = test_machine(dir.path()).await;
        set(&mut sm, 1, "v1").await;

        let mut tx = pool.begin().await.unwrap();
        assert_eq!(
            read_kv(&mut tx).await.unwrap().get("k").map(String::as_str),
            Some("v1")
        );
        // 读者事务已取快照；另一连接提交下一条 apply。
        set(&mut sm, 2, "v2").await;
        let (applied, _) = read_applied(&mut tx).await.unwrap();
        assert_eq!(
            applied,
            Some(log_id(1)),
            "applied 与同一读快照的 kv 必须一致（不得读到新 meta + 旧数据）"
        );
        assert_eq!(
            read_kv(&mut tx).await.unwrap().get("k").map(String::as_str),
            Some("v1")
        );
        tx.commit().await.unwrap();

        let mut tx = pool.begin().await.unwrap();
        assert_eq!(read_applied(&mut tx).await.unwrap().0, Some(log_id(2)));
        assert_eq!(
            read_kv(&mut tx).await.unwrap().get("k").map(String::as_str),
            Some("v2")
        );
    }

    /// I1：build 与 install 交错时，旧 build 不得覆盖更新的 raft_snapshot 行。
    #[tokio::test]
    async fn older_snapshot_does_not_overwrite_newer() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sm, _pool) = test_machine(dir.path()).await;
        let meta = |index: u64, id: &str| SnapshotMeta {
            last_log_id: Some(log_id(index)),
            last_membership: StoredMembership::default(),
            snapshot_id: id.to_string(),
        };

        sm.persist_snapshot(&meta(5, "new"), b"new").await.unwrap();
        sm.persist_snapshot(&meta(3, "old"), b"old").await.unwrap();
        let snap = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snap.meta.snapshot_id, "new");
        assert_eq!(snap.snapshot.get_ref().as_slice(), b"new");

        sm.persist_snapshot(&meta(7, "newer"), b"newer")
            .await
            .unwrap();
        let snap = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snap.meta.snapshot_id, "newer");
    }

    /// Minor 2：install 在自身事务里打开守卫窗口，提交前复位。
    #[tokio::test]
    async fn install_snapshot_runs_inside_apply_guard_window() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sm, pool) = test_machine(dir.path()).await;
        sqlx::raw_sql(
            "CREATE TRIGGER test_install_guard BEFORE INSERT ON raft_kv \
             WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1 \
             BEGIN SELECT RAISE(ABORT, 'raft: control write outside apply'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        let membership = StoredMembership::default();
        let meta = SnapshotMeta {
            last_log_id: Some(log_id(1)),
            last_membership: membership.clone(),
            snapshot_id: "s1".into(),
        };
        let data = SnapshotData {
            applied_log_id: Some(log_id(1)),
            last_membership: membership,
            kv: BTreeMap::from([("k".to_string(), "v".to_string())]),
        };
        let bytes = serde_json::to_vec(&data).unwrap();
        RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm,
            &meta,
            Box::new(Cursor::new(bytes)),
        )
        .await
        .unwrap();

        assert_eq!(sm.get("k").await.unwrap(), Some("v".to_string()));
        let active: i64 = sqlx::query_scalar("SELECT active FROM raft_apply_ctx WHERE id = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(active, 0, "install 提交后守卫窗口必须复位");
    }

    /// S7.2：控制面快照（11 表）build → 全新控制库 install；篡改快照拒绝且不写行。
    #[tokio::test]
    async fn control_snapshot_round_trip_and_corruption_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let url_a = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("a-control.db").display()
        );
        let url_b = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("b-control.db").display()
        );
        let db_a = crate::db::Db::connect_raft(&url_a).await.unwrap();
        sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
            .execute(db_a.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
             VALUES ('snap-user', 'hash', 0, 'active', 1)",
        )
        .execute(db_a.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
             replication_mode, deleting) VALUES ('snap-bucket', NULL, 2, 'Enabled', 0, 'sync', 0)",
        )
        .execute(db_a.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
            .execute(db_a.pool())
            .await
            .unwrap();

        let mut sm_a =
            SqliteStateMachine::open_with_pool(db_a.pool().clone(), 1 << 20, false, None, 1)
                .await
                .unwrap();
        let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm_a)
            .await
            .unwrap();
        assert!(snap.snapshot.get_ref().starts_with(data::SNAPSHOT_MAGIC));

        let db_b = crate::db::Db::connect_raft(&url_b).await.unwrap();
        let mut sm_b =
            SqliteStateMachine::open_with_pool(db_b.pool().clone(), 1 << 20, false, None, 1)
                .await
                .unwrap();
        let bytes = snap.snapshot.get_ref().clone();
        RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm_b,
            &snap.meta,
            Box::new(Cursor::new(bytes.clone())),
        )
        .await
        .unwrap();
        let (users, buckets): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM users WHERE username='snap-user'), \
                    (SELECT COUNT(*) FROM buckets WHERE name='snap-bucket')",
        )
        .fetch_one(db_b.pool())
        .await
        .unwrap();
        assert_eq!((users, buckets), (1, 1));

        let mut bad = bytes;
        *bad.last_mut().unwrap() ^= 0x01;
        let err = RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm_b,
            &snap.meta,
            Box::new(Cursor::new(bad)),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("checksum"));
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets")
            .fetch_one(db_b.pool())
            .await
            .unwrap();
        assert_eq!(n, 1, "校验失败不得改动目标控制表");
    }

    /// OSSCTL2：带 config_json 的控制快照 build → 全新控制库 install；行不丢，
    /// 激活 marker 与数据同事务升到快照携带的级别。
    #[tokio::test]
    async fn control_snapshot_with_level_two_data_installs_and_bumps_marker() {
        let dir = tempfile::tempdir().unwrap();
        let url_a = format!("sqlite://{}?mode=rwc", dir.path().join("l2-a.db").display());
        let url_b = format!("sqlite://{}?mode=rwc", dir.path().join("l2-b.db").display());
        let db_a = crate::db::Db::connect_raft(&url_a).await.unwrap();
        db_a.schema_meta_set("raft_feature_level", "2")
            .await
            .unwrap();
        sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
            .execute(db_a.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, object_lock, \
             replication_mode, deleting) VALUES ('l2-bucket', NULL, 2, 'Enabled', 0, 'sync', 0)",
        )
        .execute(db_a.pool())
        .await
        .unwrap();
        let config_json = r#"{"filter":{"prefix":"p/"}}"#;
        sqlx::query(
            "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at, \
             rule_id, expiration_days, config_json) VALUES ('l2-bucket', 'p/', 30, 1, 3, 'r1', 30, ?)",
        )
        .bind(config_json)
        .execute(db_a.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
            .execute(db_a.pool())
            .await
            .unwrap();

        let mut sm_a =
            SqliteStateMachine::open_with_pool(db_a.pool().clone(), 1 << 20, false, None, 2)
                .await
                .unwrap();
        let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm_a)
            .await
            .unwrap();
        let bytes = snap.snapshot.get_ref().clone();
        assert!(bytes.starts_with(data::SNAPSHOT_MAGIC_V2));
        let decoded = data::decode_snapshot(&bytes).unwrap();
        assert_eq!(decoded.feature_level, 2);

        let db_b = crate::db::Db::connect_raft(&url_b).await.unwrap();
        let mut sm_b =
            SqliteStateMachine::open_with_pool(db_b.pool().clone(), 1 << 20, false, None, 2)
                .await
                .unwrap();
        RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm_b,
            &snap.meta,
            Box::new(Cursor::new(bytes)),
        )
        .await
        .unwrap();
        let config: Option<String> = sqlx::query_scalar(
            "SELECT config_json FROM lifecycle_rules WHERE bucket = 'l2-bucket'",
        )
        .fetch_one(db_b.pool())
        .await
        .unwrap();
        assert_eq!(config.as_deref(), Some(config_json));
        assert_eq!(
            db_b.schema_meta("raft_feature_level")
                .await
                .unwrap()
                .as_deref(),
            Some("2"),
            "安装 OSSCTL2 必须激活 marker"
        );
        assert_eq!(sm_b.activation_level(), 2);
    }

    /// V3 扩展区：未知命名空间 fail-closed（拒绝安装、不写行）；`m8.runstate`
    /// 畸形载荷同样在写行前拒绝；空扩展区仍可安装。
    #[tokio::test]
    async fn control_snapshot_with_opaque_extensions_is_rejected_without_writes() {
        let dir = tempfile::tempdir().unwrap();
        let url_a = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("ext-a.db").display()
        );
        let url_b = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("ext-b.db").display()
        );
        let db_a = crate::db::Db::connect_raft(&url_a).await.unwrap();
        sqlx::query("UPDATE raft_apply_ctx SET active = 1 WHERE id = 1")
            .execute(db_a.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
             VALUES ('ext-user', 'hash', 0, 'active', 1)",
        )
        .execute(db_a.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE raft_apply_ctx SET active = 0 WHERE id = 1")
            .execute(db_a.pool())
            .await
            .unwrap();

        let mut sm_a =
            SqliteStateMachine::open_with_pool(db_a.pool().clone(), 1 << 20, false, None, 3)
                .await
                .unwrap();
        let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm_a)
            .await
            .unwrap();
        let decoded = data::decode_snapshot(snap.snapshot.get_ref()).unwrap();

        let mut extensions = data::SnapshotExtensions::new();
        extensions.insert(
            "future.opaque".into(),
            data::SnapshotExtension {
                version: 1,
                payload_b64: "b3BhcXVl".into(), // base64("opaque")
            },
        );
        let with_ext = data::encode_snapshot_with_extensions(&decoded.data, 3, extensions).unwrap();
        assert!(with_ext.starts_with(data::SNAPSHOT_MAGIC_V3));

        let db_b = crate::db::Db::connect_raft(&url_b).await.unwrap();
        let mut sm_b =
            SqliteStateMachine::open_with_pool(db_b.pool().clone(), 1 << 20, false, None, 3)
                .await
                .unwrap();
        let err = RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm_b,
            &snap.meta,
            Box::new(Cursor::new(with_ext)),
        )
        .await
        .unwrap_err();
        assert!(
            format!("{err}").to_lowercase().contains("extension"),
            "{err}"
        );
        let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(db_b.pool())
            .await
            .unwrap();
        assert_eq!(users, 0, "拒绝安装不得写任何行");

        // 已消费的 m8.runstate：畸形载荷必须解码失败，同样不写行。
        let mut bad_ext = data::SnapshotExtensions::new();
        bad_ext.insert(
            "m8.runstate".into(),
            data::SnapshotExtension {
                version: 1,
                payload_b64: "b3BhcXVl".into(),
            },
        );
        let bad = data::encode_snapshot_with_extensions(&decoded.data, 3, bad_ext).unwrap();
        let err = RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm_b,
            &snap.meta,
            Box::new(Cursor::new(bad)),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("json"), "{err}");
        let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(db_b.pool())
            .await
            .unwrap();
        assert_eq!(users, 0, "畸形运行态不得写任何行");

        // 空扩展区的 V3 不触发闸门：正常安装并保留数据。
        let no_ext = data::encode_snapshot_at(&decoded.data, 3).unwrap();
        assert!(no_ext.starts_with(data::SNAPSHOT_MAGIC_V3));
        RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm_b,
            &snap.meta,
            Box::new(Cursor::new(no_ext)),
        )
        .await
        .unwrap();
        let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(db_b.pool())
            .await
            .unwrap();
        assert_eq!(users, 1);
    }

    /// Minor 3：序列化后超限的快照在 build 阶段即拒绝。
    #[tokio::test]
    async fn build_snapshot_rejects_serialized_over_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sm.db");
        let mut sm = SqliteStateMachine::open(&path, 16).await.unwrap();
        let err = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("exceeds"), "{err}");
    }

    /// 每条 entry 恰好一个响应；applied 前进到批内最后一条。
    #[tokio::test]
    async fn apply_returns_one_response_per_entry_and_advances_applied() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sm, _pool) = test_machine(dir.path()).await;
        let entries = [1, 2].map(|i| Entry {
            log_id: log_id(i),
            payload: EntryPayload::Normal(ControlCommand::Set {
                key: format!("k{i}"),
                value: "v".into(),
            }),
        });
        let responses = RaftStateMachine::<TypeConfig>::apply(&mut sm, entries)
            .await
            .unwrap();
        assert_eq!(responses, vec![ControlResponse::Ok, ControlResponse::Ok]);
        assert_eq!(sm.applied().await.unwrap().0, Some(log_id(2)));
        assert_eq!(sm.get("k1").await.unwrap(), Some("v".to_string()));
        assert_eq!(sm.get("k2").await.unwrap(), Some("v".to_string()));
    }

    /// apply 在自身事务里打开守卫窗口，提交前复位（Minor 2 的 apply 侧对照）。
    #[tokio::test]
    async fn apply_opens_guard_window_and_resets_ctx() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sm, pool) = test_machine(dir.path()).await;
        sqlx::raw_sql(
            "CREATE TRIGGER test_apply_guard BEFORE INSERT ON raft_kv \
             WHEN IFNULL((SELECT active FROM raft_apply_ctx WHERE id = 1), 0) <> 1 \
             BEGIN SELECT RAISE(ABORT, 'raft: control write outside apply'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        set(&mut sm, 1, "v").await;
        let active: i64 = sqlx::query_scalar("SELECT active FROM raft_apply_ctx WHERE id = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(active, 0);
    }

    /// R10：首条 level-2 entry 与激活标记同事务提交（业务拒绝也写）；高于配置的
    /// entry 在开启事务前 fail-stop，且不降级 marker。
    #[tokio::test]
    async fn level_two_apply_activates_marker_and_gate_rejects_above_config() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("marker-control.db").display()
        );
        let db = crate::db::Db::connect_raft(&url).await.unwrap();
        let mut sm = SqliteStateMachine::open_with_pool(db.pool().clone(), 1 << 20, false, None, 2)
            .await
            .unwrap();
        assert_eq!(sm.activation_level(), 1);
        let entries = [
            Entry {
                log_id: log_id(1),
                payload: EntryPayload::Normal(ControlCommand::FeatureProbe { reject: true }),
            },
            Entry {
                log_id: log_id(2),
                payload: EntryPayload::Normal(ControlCommand::Set {
                    key: "k".into(),
                    value: "v".into(),
                }),
            },
        ];
        let responses = RaftStateMachine::<TypeConfig>::apply(&mut sm, entries)
            .await
            .unwrap();
        assert_eq!(
            responses,
            vec![ControlResponse::Invalid, ControlResponse::Ok]
        );
        assert_eq!(sm.activation_level(), 2);
        let marker: String =
            sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = 'raft_feature_level'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(marker, "2");

        // 配置 1 的节点收到 level-2 entry：fail-stop，marker 保持 2。
        let mut sm1 =
            SqliteStateMachine::open_with_pool(db.pool().clone(), 1 << 20, false, None, 1)
                .await
                .unwrap();
        let err = RaftStateMachine::<TypeConfig>::apply(
            &mut sm1,
            [Entry {
                log_id: log_id(3),
                payload: EntryPayload::Normal(ControlCommand::FeatureProbe { reject: false }),
            }],
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("feature level 2"), "{err}");
        assert_eq!(sm1.activation_level(), 2);
    }

    /// 已规范化（marker 存在）但激活级别仍为 1 的节点：level-2 快照必须拒绝，
    /// 不得清空账户行（直接 standalone→raft 启动的旁路）。
    #[tokio::test]
    async fn level2_snapshot_rejected_when_canonical_marker_present() {
        let dir = tempfile::tempdir().unwrap();
        let url_a = format!("sqlite://{}?mode=rwc", dir.path().join("m-a.db").display());
        let url_b = format!("sqlite://{}?mode=rwc", dir.path().join("m-b.db").display());
        let db_a = crate::db::Db::connect_raft(&url_a).await.unwrap();
        let mut sm_a =
            SqliteStateMachine::open_with_pool(db_a.pool().clone(), 1 << 20, false, None, 2)
                .await
                .unwrap();
        let snap = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut sm_a)
            .await
            .unwrap();
        let level2 = data::encode_snapshot_at(&data::ControlData::default(), 2).unwrap();

        let db_b = crate::db::Db::connect_raft(&url_b).await.unwrap();
        sqlx::query(
            "INSERT INTO schema_meta (key, value) VALUES (?, 'done') \
             ON CONFLICT(key) DO UPDATE SET value = 'done'",
        )
        .bind(crate::db::META_ACCOUNTS_CANONICAL)
        .execute(db_b.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO accounts (id, name, status, created_at, updated_at) \
             VALUES ('000000000009', 'keep', 'active', 1, 1)",
        )
        .execute(db_b.pool())
        .await
        .unwrap();
        let mut sm_b =
            SqliteStateMachine::open_with_pool(db_b.pool().clone(), 1 << 20, false, None, 1)
                .await
                .unwrap();
        let err = RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm_b,
            &snap.meta,
            Box::new(Cursor::new(level2)),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("canonical"), "{err}");
        let accounts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM accounts WHERE id = '000000000009'")
                .fetch_one(db_b.pool())
                .await
                .unwrap();
        assert_eq!(accounts, 1, "拒绝安装不得清空账户行");
        let marker: Option<String> =
            sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = ?")
                .bind(crate::db::META_ACCOUNTS_CANONICAL)
                .fetch_optional(db_b.pool())
                .await
                .unwrap();
        assert!(marker.is_some(), "marker 必须保留");
    }

    /// 分块边界：空 / 单块内 / 恰好一块 / 跨块 / 多块都能透明往返；小快照保持
    /// 旧整行格式（旧二进制与旧库读取路径不变）。
    #[tokio::test]
    async fn snapshot_chunk_boundaries_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sm, pool) = test_machine(dir.path()).await;
        let cases = [
            (0usize, 0i64),
            (1, 0),
            (SNAPSHOT_CHUNK_BYTES - 1, 0),
            (SNAPSHOT_CHUNK_BYTES, 0),
            (SNAPSHOT_CHUNK_BYTES + 1, 2),
            (SNAPSHOT_CHUNK_BYTES * 2 + 7, 3),
        ];
        for (index, (len, want_chunks)) in cases.into_iter().enumerate() {
            let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let id = format!("s{index}");
            sm.persist_snapshot(&snap_meta(index as u64 + 1, &id), &bytes)
                .await
                .unwrap();
            let snap = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                snap.snapshot.get_ref().as_slice(),
                bytes.as_slice(),
                "len {len}"
            );
            assert_eq!(snap.meta.snapshot_id, id);
            let chunks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raft_snapshot_chunk")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(chunks, want_chunks, "len {len}");
            assert_eq!(sm.snapshot_bytes().await, len as u64, "len {len}");
            let stored: Vec<u8> = sqlx::query_scalar("SELECT data FROM raft_snapshot WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
            if want_chunks > 0 {
                assert!(stored.starts_with(SNAPSHOT_CHUNK_MAGIC));
            } else {
                assert_eq!(stored, bytes, "小快照保持旧整行格式");
            }
        }
    }

    /// 缺块、块内容截断、清单头损坏：读快照必须报错，绝不返回截断/空数据。
    #[tokio::test]
    async fn chunked_snapshot_corruption_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sm, pool) = test_machine(dir.path()).await;
        let bytes: Vec<u8> = (0..SNAPSHOT_CHUNK_BYTES * 2 + 5)
            .map(|i| (i % 253) as u8)
            .collect();
        sm.persist_snapshot(&snap_meta(1, "a"), &bytes)
            .await
            .unwrap();

        sqlx::query("DELETE FROM raft_snapshot_chunk WHERE seq = 1")
            .execute(&pool)
            .await
            .unwrap();
        let err = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("expected 3 chunks"), "{err}");

        sm.persist_snapshot(&snap_meta(2, "b"), &bytes)
            .await
            .unwrap();
        sqlx::query("UPDATE raft_snapshot_chunk SET data = ? WHERE seq = 0")
            .bind(&bytes[..16])
            .execute(&pool)
            .await
            .unwrap();
        let err = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("expected"), "{err}");

        sm.persist_snapshot(&snap_meta(3, "c"), &bytes)
            .await
            .unwrap();
        sqlx::query("UPDATE raft_snapshot SET data = ? WHERE id = 1")
            .bind(SNAPSHOT_CHUNK_MAGIC.as_slice())
            .execute(&pool)
            .await
            .unwrap();
        let err = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap_err();
        assert!(
            format!("{err}").contains("corrupt raft snapshot chunk manifest"),
            "{err}"
        );
    }

    /// 旧库（只有整行 BLOB、无分块表）打开时自动建分块表且旧快照原样可读；
    /// 再次打开幂等；升级后的写入走分块并可跨重启读回。
    #[tokio::test]
    async fn legacy_whole_blob_snapshot_migrates_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let opts = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(opts).await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE raft_snapshot (id INTEGER PRIMARY KEY CHECK (id = 1), \
             meta TEXT NOT NULL, data BLOB NOT NULL);",
        )
        .execute(&pool)
        .await
        .unwrap();
        let meta_json = serde_json::to_string(&snap_meta(5, "legacy")).unwrap();
        let legacy = b"legacy-whole-blob-snapshot".to_vec();
        sqlx::query("INSERT INTO raft_snapshot (id, meta, data) VALUES (1, ?, ?)")
            .bind(&meta_json)
            .bind(&legacy)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        let mut sm = SqliteStateMachine::open(&path, 1 << 20).await.unwrap();
        let snap = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snap.snapshot.get_ref().as_slice(), legacy.as_slice());
        assert_eq!(sm.snapshot_bytes().await, legacy.len() as u64);
        drop(sm);

        let big: Vec<u8> = vec![3; SNAPSHOT_CHUNK_BYTES + 1];
        let sm = SqliteStateMachine::open(&path, 1 << 20).await.unwrap();
        sm.persist_snapshot(&snap_meta(6, "upgraded"), &big)
            .await
            .unwrap();
        drop(sm);

        let mut sm = SqliteStateMachine::open(&path, 1 << 20).await.unwrap();
        let snap = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snap.snapshot.get_ref().as_slice(), big.as_slice());
        assert_eq!(sm.snapshot_bytes().await, big.len() as u64);
        let chunks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raft_snapshot_chunk")
            .fetch_one(&sm.pool)
            .await
            .unwrap();
        assert_eq!(chunks, 2);
        let table: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' \
             AND name = 'raft_snapshot_chunk'",
        )
        .fetch_one(&sm.pool)
        .await
        .unwrap();
        assert_eq!(table, 1, "迁移必须幂等建表");
    }

    /// install 路径同样透明分块：大 KE 快照写入分块行，读回与安装后的 KV 一致。
    #[tokio::test]
    async fn large_installed_snapshot_is_chunked() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sm, pool) = test_machine(dir.path()).await;
        let value = "x".repeat(SNAPSHOT_CHUNK_BYTES + 10);
        let meta = snap_meta(1, "install-big");
        let data = SnapshotData {
            applied_log_id: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            kv: BTreeMap::from([("k".to_string(), value.clone())]),
        };
        let bytes = serde_json::to_vec(&data).unwrap();
        assert!(bytes.len() > SNAPSHOT_CHUNK_BYTES);
        RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm,
            &meta,
            Box::new(Cursor::new(bytes.clone())),
        )
        .await
        .unwrap();
        let snap = RaftStateMachine::<TypeConfig>::get_current_snapshot(&mut sm)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snap.snapshot.get_ref().as_slice(), bytes.as_slice());
        assert_eq!(sm.get("k").await.unwrap(), Some(value));
        let chunks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raft_snapshot_chunk")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(chunks >= 2, "{chunks}");
    }

    /// 日志读/truncate/purge/重开的持久语义（append 本身由 mod.rs 的 spawn 测试覆盖）。
    #[tokio::test]
    async fn log_read_truncate_purge_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let mut store = SqliteLogStore::open(&path).await.unwrap();
        for i in 1..=3u64 {
            let entry = Entry::<TypeConfig> {
                log_id: log_id(i),
                payload: EntryPayload::Blank,
            };
            sqlx::query("INSERT INTO raft_log (idx, entry) VALUES (?, ?)")
                .bind(i as i64)
                .bind(serde_json::to_string(&entry).unwrap())
                .execute(&store.pool)
                .await
                .unwrap();
        }
        assert!(store.has_logs().await.unwrap());
        let all = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(
            all.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let mid = store
            .try_get_log_entries((Bound::Excluded(1u64), Bound::Included(3u64)))
            .await
            .unwrap();
        assert_eq!(
            mid.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(
            store
                .try_get_log_entries((Bound::Excluded(3u64), Bound::Included(1u64)))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(3))
        );

        store.truncate(log_id(2)).await.unwrap();
        assert_eq!(
            store
                .try_get_log_entries(..)
                .await
                .unwrap()
                .iter()
                .map(|e| e.log_id.index)
                .collect::<Vec<_>>(),
            vec![1]
        );
        let vote = openraft::Vote::new_committed(4, 1);
        store.save_vote(&vote).await.unwrap();
        store.save_committed(Some(log_id(1))).await.unwrap();
        drop(store);

        let mut store = SqliteLogStore::open(&path).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id(1)));
        store.purge(log_id(1)).await.unwrap();
        assert!(!store.has_logs().await.unwrap());
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(1)));
        assert_eq!(state.last_log_id, Some(log_id(1)));
        assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
    }

    fn blank(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Blank,
        }
    }

    fn set_value(index: u64, value: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(ControlCommand::Set {
                key: format!("k{index}"),
                value: value.into(),
            }),
        }
    }

    fn indices(entries: &[Entry<TypeConfig>]) -> Vec<u64> {
        entries.iter().map(|e| e.log_id.index).collect()
    }

    async fn append_range(store: &mut SqliteLogStore, from: u64, to: u64) {
        let entries: Vec<_> = (from..=to).map(blank).collect();
        store.append_entries(&entries).await.unwrap();
    }

    async fn count(pool: &SqlitePool, sql: &'static str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
    }

    fn pseudo_random(len: usize) -> String {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                char::from(33 + (state % 90) as u8)
            })
            .collect()
    }

    /// 大 entry 压成 `z1:` 往返；小/不可压缩 entry 与旧明文行保持原格式。
    #[tokio::test]
    async fn entry_compression_round_trips_and_legacy_rows_stay_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let mut store = SqliteLogStore::open(&path).await.unwrap();

        // 旧版直接写入的明文行。
        let legacy = blank(1);
        sqlx::query("INSERT INTO raft_log (idx, entry) VALUES (1, ?)")
            .bind(serde_json::to_string(&legacy).unwrap())
            .execute(&store.pool)
            .await
            .unwrap();

        let big = set_value(2, &"x".repeat(4096));
        let small = blank(3);
        let incompressible = set_value(4, &pseudo_random(4096));
        store
            .append_entries(&[big.clone(), small.clone(), incompressible.clone()])
            .await
            .unwrap();

        let raw: Vec<String> = sqlx::query_scalar("SELECT entry FROM raft_log ORDER BY idx")
            .fetch_all(&store.pool)
            .await
            .unwrap();
        assert!(raw[0].starts_with('{'), "legacy plain row");
        assert!(
            raw[1].starts_with(ENTRY_COMPRESS_MAGIC),
            "large payload must compress: {}",
            &raw[1][..8.min(raw[1].len())]
        );
        assert!(raw[1].len() < serde_json::to_string(&big).unwrap().len());
        assert!(raw[2].starts_with('{'), "small entry stays plain");
        assert!(raw[3].starts_with('{'), "incompressible entry stays plain");

        let all = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(all, vec![legacy, big, small, incompressible]);

        // 压缩格式跨重开可读。
        drop(store);
        let mut store = SqliteLogStore::open(&path).await.unwrap();
        assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 4);
    }

    /// 达到阈值后压段；段/行混合读、开闭区间边界与重开后的 LogState 一致。
    #[tokio::test]
    async fn segments_form_after_threshold_and_ranges_merge_across_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let mut store = SqliteLogStore::open(&path).await.unwrap();

        for chunk in 0..16u64 {
            append_range(&mut store, 1 + chunk * 16, 16 + chunk * 16).await;
        }
        assert_eq!(count(&store.pool, "SELECT COUNT(*) FROM raft_log").await, 0);
        assert_eq!(
            count(&store.pool, "SELECT COUNT(*) FROM raft_log_segment").await,
            1
        );
        let range: (i64, i64) = sqlx::query_as("SELECT start_idx, end_idx FROM raft_log_segment")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(range, (1, 256));
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(256))
        );

        append_range(&mut store, 257, 260).await;
        assert_eq!(count(&store.pool, "SELECT COUNT(*) FROM raft_log").await, 4);
        assert_eq!(
            count(&store.pool, "SELECT COUNT(*) FROM raft_log_segment").await,
            1
        );
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            (1..=260).collect::<Vec<_>>()
        );

        // 跨段/行边界，四种 Bound 组合。
        let mid = store
            .try_get_log_entries((Bound::Excluded(255u64), Bound::Included(258u64)))
            .await
            .unwrap();
        assert_eq!(indices(&mid), vec![256, 257, 258]);
        let excl_end = store
            .try_get_log_entries((Bound::Included(256u64), Bound::Excluded(260u64)))
            .await
            .unwrap();
        assert_eq!(indices(&excl_end), vec![256, 257, 258, 259]);
        let in_segment = store
            .try_get_log_entries((Bound::Included(1u64), Bound::Included(256u64)))
            .await
            .unwrap();
        assert_eq!(indices(&in_segment), (1..=256).collect::<Vec<_>>());
        let rows_only = store
            .try_get_log_entries((Bound::Included(257u64), Bound::Unbounded))
            .await
            .unwrap();
        assert_eq!(indices(&rows_only), vec![257, 258, 259, 260]);

        drop(store);
        let mut store = SqliteLogStore::open(&path).await.unwrap();
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(260))
        );
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            (1..=260).collect::<Vec<_>>()
        );
        assert_eq!(
            count(&store.pool, "SELECT COUNT(*) FROM raft_log_segment").await,
            1,
            "重开幂等：段不重复创建"
        );
    }

    /// 段内 truncate/purge 切分、重开、再追加；清空后 last_purged 语义不变。
    #[tokio::test]
    async fn truncate_and_purge_split_segments_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let mut store = SqliteLogStore::open(&path).await.unwrap();

        append_range(&mut store, 1, 300).await;
        assert_eq!(
            count(&store.pool, "SELECT COUNT(*) FROM raft_log_segment").await,
            1
        );
        assert_eq!(count(&store.pool, "SELECT COUNT(*) FROM raft_log").await, 0);

        // truncate 落在段内：保留 [1,99]。
        store.truncate(log_id(100)).await.unwrap();
        let range: (i64, i64) = sqlx::query_as("SELECT start_idx, end_idx FROM raft_log_segment")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(range, (1, 99));
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            (1..=99).collect::<Vec<_>>()
        );
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(99))
        );

        // truncate 后追加走行，与段拼接。
        append_range(&mut store, 100, 105).await;
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            (1..=105).collect::<Vec<_>>()
        );

        // purge 落在段内：保留 [51,99]；last_purged 与最后 LogId 分离。
        store.purge(log_id(50)).await.unwrap();
        let range: (i64, i64) = sqlx::query_as("SELECT start_idx, end_idx FROM raft_log_segment")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(range, (51, 99));
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(50)));
        assert_eq!(state.last_log_id, Some(log_id(105)));
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            (51..=105).collect::<Vec<_>>()
        );

        drop(store);
        let mut store = SqliteLogStore::open(&path).await.unwrap();
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            (51..=105).collect::<Vec<_>>()
        );
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(50)));
        assert_eq!(state.last_log_id, Some(log_id(105)));

        // purge 覆盖整段：段删除，仅剩行。
        store.purge(log_id(102)).await.unwrap();
        assert_eq!(
            count(&store.pool, "SELECT COUNT(*) FROM raft_log_segment").await,
            0
        );
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            vec![103, 104, 105]
        );

        append_range(&mut store, 106, 107).await;
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            vec![103, 104, 105, 106, 107]
        );

        // 清空：last_log_id 回落到 last_purged。
        store.purge(log_id(107)).await.unwrap();
        assert!(!store.has_logs().await.unwrap());
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(107)));
        assert_eq!(state.last_log_id, Some(log_id(107)));
        assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
    }

    /// 覆盖写落在段内：段切两半，行补洞；读无重复/无洞。
    #[tokio::test]
    async fn append_overwriting_inside_segment_splits_without_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let mut store = SqliteLogStore::open(&path).await.unwrap();

        append_range(&mut store, 1, 256).await;
        let replacement = set_value(100, "new");
        store
            .append_entries(std::slice::from_ref(&replacement))
            .await
            .unwrap();

        let segs: Vec<(i64, i64)> =
            sqlx::query_as("SELECT start_idx, end_idx FROM raft_log_segment ORDER BY start_idx")
                .fetch_all(&store.pool)
                .await
                .unwrap();
        assert_eq!(segs, vec![(1, 99), (101, 256)]);
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            (1..=256).collect::<Vec<_>>()
        );
        let all = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(all[99], replacement);
        assert_eq!(
            indices(
                &store
                    .try_get_log_entries((Bound::Included(99u64), Bound::Included(101u64)))
                    .await
                    .unwrap()
            ),
            vec![99, 100, 101]
        );
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(256))
        );

        drop(store);
        let mut store = SqliteLogStore::open(&path).await.unwrap();
        let all = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(indices(&all), (1..=256).collect::<Vec<_>>());
        assert_eq!(all[99], replacement);
    }

    /// 坏 magic、截断、解码索引与段范围不符：一律 StorageError，绝不返回部分数据。
    #[tokio::test]
    async fn corrupt_segments_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.db");
        let mut store = SqliteLogStore::open(&path).await.unwrap();
        append_range(&mut store, 1, 256).await;
        let valid: Vec<u8> =
            sqlx::query_scalar("SELECT data FROM raft_log_segment WHERE start_idx = 1")
                .fetch_one(&store.pool)
                .await
                .unwrap();

        sqlx::query("UPDATE raft_log_segment SET data = ? WHERE start_idx = 1")
            .bind(b"not-a-segment-but-long-enough".as_slice())
            .execute(&store.pool)
            .await
            .unwrap();
        let err = store.try_get_log_entries(..).await.unwrap_err();
        assert!(format!("{err}").contains("bad magic"), "{err}");

        sqlx::query("UPDATE raft_log_segment SET data = ? WHERE start_idx = 1")
            .bind(&valid[..valid.len() / 2])
            .execute(&store.pool)
            .await
            .unwrap();
        let err = store.try_get_log_entries(..).await.unwrap_err();
        assert!(format!("{err}").contains("corrupt raft log"), "{err}");

        // 元素数与范围一致，但解码索引不在 [start,end]。
        let wrong: Vec<_> = (3..=258).map(blank).collect();
        sqlx::query("UPDATE raft_log_segment SET data = ? WHERE start_idx = 1")
            .bind(encode_segment(&wrong).unwrap())
            .execute(&store.pool)
            .await
            .unwrap();
        let err = store.try_get_log_entries(..).await.unwrap_err();
        assert!(format!("{err}").contains("outside"), "{err}");
        let err = store.get_log_state().await.unwrap_err();
        assert!(format!("{err}").contains("outside"), "{err}");

        sqlx::query("UPDATE raft_log_segment SET data = ? WHERE start_idx = 1")
            .bind(valid.as_slice())
            .execute(&store.pool)
            .await
            .unwrap();
        assert_eq!(
            indices(&store.try_get_log_entries(..).await.unwrap()),
            (1..=256).collect::<Vec<_>>()
        );
    }
}
