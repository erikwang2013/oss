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
use std::sync::atomic::Ordering;
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
use super::data::{self, InstallCtx};
use super::metrics::Metrics;
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
                || !runstate.tier_restores.is_empty())
        {
            return Err(Self::data_err(data::DataError::Format(format!(
                "[raft].feature_level={} cannot install a level-{} snapshot carrying \
                 non-log jobs/job_tasks/inventory_runs/tier_restores; finish feature-4 \
                 activation on the source or drain/clear these tables and re-export",
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
        sqlx::query("INSERT OR REPLACE INTO raft_snapshot (id, meta, data) VALUES (1, ?, ?)")
            .bind(meta_json)
            .bind(bytes)
            .execute(&mut *tx)
            .await
            .map_err(|e| storage_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
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

    /// 已落库快照的字节数（无快照 = 0；查询失败记 0 并告警，不因 gauge 拖垮 /metrics）。
    pub async fn snapshot_bytes(&self) -> u64 {
        match sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(length(data), 0) FROM raft_snapshot WHERE id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        {
            Ok(v) => v.unwrap_or(0).max(0) as u64,
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
}
