//! S5 基座存储：`raft.db` 上的 v2 [`RaftLogStorage`] + [`RaftStateMachine`]。
//!
//! 日志/投票/成员与应用状态都落在同一 SQLite 文件（WAL + `synchronous=FULL` + 5 s
//! busy_timeout）。S6 把 `apply` 事务扩展到控制库真实表时，可把状态机指向 `control.db`：
//! `raft_applied` 表结构已与 `0004_m9.sql` 对齐。
//!
//! shortcut: 日志与快照按 JSON/BLOB 整条存储（非增量日志段），S7 大快照/高吞吐前再评估。

// openraft StorageError 体积由第三方类型固定，trait 签名不能改。
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::time::Duration;

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
use sqlx::{Row, SqlitePool};

use super::{ControlCommand, ControlResponse, TypeConfig};

/// 日志/元数据 + 基座状态机表。`raft_applied`/`raft_apply_ctx` 与 `0004_m9.sql` 同名同构。
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS raft_log (
    idx   INTEGER PRIMARY KEY,
    entry TEXT NOT NULL
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
CREATE TABLE IF NOT EXISTS raft_apply_ctx (
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    active INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO raft_apply_ctx (id, active) VALUES (1, 0);
"#;

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
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raft_log")
            .fetch_one(&self.pool)
            .await?;
        Ok(count > 0)
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
            Bound::Included(i) | Bound::Excluded(i) => *i,
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
        let rows =
            sqlx::query("SELECT entry FROM raft_log WHERE idx >= ? AND idx <= ? ORDER BY idx")
                .bind(start as i64)
                .bind(end as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| storage_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        rows.into_iter()
            .map(|r| {
                let s: String = r.get("entry");
                decode(ErrorSubject::Logs, ErrorVerb::Read, &s)
            })
            .collect()
    }
}

impl RaftLogStorage<TypeConfig> for SqliteLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let last_purged: Option<LogId<u64>> = self
            .read_meta::<Option<LogId<u64>>>("last_purged")
            .await?
            .flatten();
        let last = sqlx::query("SELECT entry FROM raft_log ORDER BY idx DESC LIMIT 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| storage_err(ErrorSubject::Logs, ErrorVerb::Read, e))?
            .map(|r| {
                let s: String = r.get("entry");
                decode::<Entry<TypeConfig>>(ErrorSubject::Logs, ErrorVerb::Read, &s)
            })
            .transpose()?
            .map(|e| e.log_id);
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
        let mut result = Ok(());
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                callback.log_io_completed(Err(std::io::Error::other(e.to_string())));
                return Err(storage_err(ErrorSubject::Logs, ErrorVerb::Write, e));
            }
        };
        for entry in &entries {
            let json = match encode(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, entry) {
                Ok(json) => json,
                Err(e) => {
                    result = Err(e);
                    break;
                }
            };
            if let Err(e) =
                sqlx::query("INSERT OR REPLACE INTO raft_log (idx, entry) VALUES (?, ?)")
                    .bind(entry.log_id.index as i64)
                    .bind(json)
                    .execute(&mut *tx)
                    .await
            {
                result = Err(storage_err(
                    ErrorSubject::Log(entry.log_id),
                    ErrorVerb::Write,
                    e,
                ));
                break;
            }
        }
        if result.is_ok() {
            result = tx
                .commit()
                .await
                .map_err(|e| storage_err(ErrorSubject::Logs, ErrorVerb::Write, e));
        }
        match &result {
            Ok(()) => callback.log_io_completed(Ok(())),
            // 回调只接受 io::Error；SQLite 已回滚，RaftCore 会按存储错误处理。
            Err(e) => callback.log_io_completed(Err(std::io::Error::other(e.to_string()))),
        }
        result
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        sqlx::query("DELETE FROM raft_log WHERE idx >= ?")
            .bind(log_id.index as i64)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| storage_err(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| storage_err(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))?;
        sqlx::query("DELETE FROM raft_log WHERE idx <= ?")
            .bind(log_id.index as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))?;
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

/// SQLite 状态机：apply 在同一事务内更新数据、`raft_applied`（完整 LogId + 成员）与 apply ctx。
#[derive(Clone)]
pub struct SqliteStateMachine {
    pool: SqlitePool,
    max_snapshot_bytes: u64,
}

impl SqliteStateMachine {
    pub async fn open(path: &Path, max_snapshot_bytes: u64) -> Result<Self, sqlx::Error> {
        Ok(Self {
            pool: connect(path).await?,
            max_snapshot_bytes,
        })
    }

    /// 已应用 LogId 与成员（`RaftStateMachine::applied_state` 的固有版本，测试/S6 用）。
    pub async fn applied(
        &self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let row =
            sqlx::query("SELECT applied_log_id, membership_json FROM raft_applied WHERE id = 1")
                .fetch_optional(&self.pool)
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

    /// 基座 KV 读取（S6 换成控制表查询）。
    pub async fn get(&self, key: &str) -> Result<Option<String>, StorageError<u64>> {
        let row = sqlx::query("SELECT value FROM raft_kv WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        Ok(row.map(|r| r.get("value")))
    }

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
        let (applied, membership) = self.applied().await?;
        let rows = sqlx::query("SELECT key, value FROM raft_kv")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;
        let kv = rows
            .into_iter()
            .map(|r| (r.get("key"), r.get("value")))
            .collect();
        Ok((applied, membership, kv))
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
        let entries: Vec<_> = entries.into_iter().collect();
        let (mut last_applied, mut last_membership) = self.applied().await?;
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

        let mut responses = Vec::with_capacity(entries.len());
        for entry in &entries {
            let log_id = entry.log_id;
            if let Some(m) = entry.get_membership() {
                last_membership = StoredMembership::new(Some(log_id), m.clone());
            }
            match &entry.payload {
                EntryPayload::Normal(ControlCommand::Set { key, value }) => {
                    sqlx::query("INSERT OR REPLACE INTO raft_kv (key, value) VALUES (?, ?)")
                        .bind(key)
                        .bind(value)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e)
                        })?;
                }
                EntryPayload::Normal(ControlCommand::Delete { key }) => {
                    sqlx::query("DELETE FROM raft_kv WHERE key = ?")
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e)
                        })?;
                }
                EntryPayload::Blank | EntryPayload::Membership(_) => {}
            }
            last_applied = Some(log_id);
            responses.push(ControlResponse::Ok);
        }

        let applied_json = encode(ErrorSubject::StateMachine, ErrorVerb::Write, &last_applied)?;
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
        Ok(responses)
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
        let mut tx = self
            .pool
            .begin()
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
        sqlx::query("INSERT OR REPLACE INTO raft_snapshot (id, meta, data) VALUES (1, ?, ?)")
            .bind(meta_json)
            .bind(&bytes)
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
        let row = sqlx::query("SELECT meta, data FROM raft_snapshot WHERE id = 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let meta_json: String = row.get("meta");
        let meta: SnapshotMeta<u64, BasicNode> =
            decode(ErrorSubject::Snapshot(None), ErrorVerb::Read, &meta_json)?;
        let data: Vec<u8> = row.get("data");
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let (applied, membership, kv) = self.snapshot_parts().await?;
        let data = SnapshotData {
            applied_log_id: applied,
            last_membership: membership.clone(),
            kv,
        };
        let bytes = serde_json::to_vec(&data)
            .map_err(|e| storage_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
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
        let meta_json = encode(ErrorSubject::Snapshot(None), ErrorVerb::Write, &meta)?;
        sqlx::query("INSERT OR REPLACE INTO raft_snapshot (id, meta, data) VALUES (1, ?, ?)")
            .bind(meta_json)
            .bind(&bytes)
            .execute(&self.pool)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}
