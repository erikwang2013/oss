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
use std::sync::Arc;
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
use sqlx::{Row, SqliteConnection, SqlitePool};

use super::apply;
use super::{ControlResponse, TypeConfig};

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
}

impl SqliteStateMachine {
    pub async fn open(path: &Path, max_snapshot_bytes: u64) -> Result<Self, sqlx::Error> {
        let pool = connect(path).await?;
        Ok(Self::with_pool(pool, max_snapshot_bytes))
    }

    /// 主控制库状态机（raft.db 只存日志/投票）。schema 幂等重放，`raft_applied`/ctx 已存在。
    pub async fn open_with_pool(
        pool: SqlitePool,
        max_snapshot_bytes: u64,
    ) -> Result<Self, sqlx::Error> {
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok(Self::with_pool(pool, max_snapshot_bytes))
    }

    fn with_pool(pool: SqlitePool, max_snapshot_bytes: u64) -> Self {
        Self {
            pool,
            max_snapshot_bytes,
            snapshot_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
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

    /// 仅在比已存快照新时写入；与 install 共用 `snapshot_lock`，避免旧 build 覆盖新行。
    async fn persist_snapshot(
        &self,
        meta: &SnapshotMeta<u64, BasicNode>,
        bytes: &[u8],
    ) -> Result<(), StorageError<u64>> {
        let _guard = self.snapshot_lock.lock().await;
        let current: Option<String> =
            sqlx::query_scalar("SELECT meta FROM raft_snapshot WHERE id = 1")
                .fetch_optional(&self.pool)
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
        sqlx::query("INSERT OR REPLACE INTO raft_snapshot (id, meta, data) VALUES (1, ?, ?)")
            .bind(meta_json)
            .bind(bytes)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))
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
            let response = match &entry.payload {
                EntryPayload::Normal(cmd) => {
                    let applied = {
                        let mut cw = apply::control_tx(&mut tx);
                        apply::apply_command(&mut cw, cmd).await.map_err(|e| {
                            storage_err(ErrorSubject::Apply(log_id), ErrorVerb::Write, e)
                        })?
                    };
                    applied.response
                }
                EntryPayload::Blank | EntryPayload::Membership(_) => ControlResponse::Ok,
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
            responses.push(response);
        }
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
        sqlx::query("INSERT OR REPLACE INTO raft_snapshot (id, meta, data) VALUES (1, ?, ?)")
            .bind(meta_json)
            .bind(&bytes)
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
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

    async fn test_machine(dir: &Path) -> (SqliteStateMachine, SqlitePool) {
        let pool = connect(&dir.join("sm.db")).await.unwrap();
        let sm = SqliteStateMachine::open_with_pool(pool.clone(), 1024 * 1024)
            .await
            .unwrap();
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
}
