//! 控制面数据访问。M1 只有五张表；对象数据永不进 SQL。

use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Row, SqliteConnection, SqlitePool};

use crate::control_store::SqliteControlTxn;
use crate::raft::FencingEpoch;
use crate::raft::apply::ControlWrite;
use xl_store::control_store::account::AccountTxn;
use xl_store::control_store::bucket::BucketTxn;
use xl_store::control_store::iam::{IamTxn, PermissionRow as IamPermissionRow};
use xl_store::control_store::inventory::{
    InventoryConfigEntry, InventoryConfigView, InventoryCursorRow, InventoryIncrementalRow,
    InventoryTxn,
};
use xl_store::control_store::jobs::{JobTxn, NewJob as JobInsert, NewTask as JobTaskInsert};
use xl_store::control_store::lifecycle::LifecycleTxn;
use xl_store::control_store::repl_outbox::ReplOutboxTxn;
use xl_store::control_store::sites::{SitePeerRow, SiteTxn};
use xl_store::control_store::tier::{TierRestoreRow, TierTxn};
use xl_store::control_store::uploads::{NewUpload, UploadTxn};
use xl_store::control_store::{AccessPointEntry, ControlTxn, OlapConfigEntry, OwnerRef};

pub type Result<T> = std::result::Result<T, sqlx::Error>;

pub struct Db {
    pool: SqlitePool,
    /// M9 S6a：Raft 模式抑制 `verify_user` 的旧 hash 本地升级（H1；各站点盐不同会分叉）。
    raft_mode: std::sync::atomic::AtomicBool,
    /// B4：`[server].control_backend = "objects"` 时由启动装配注入。未注入 =
    /// SQLite 后端（默认路径零改动）。对象模式下 `pool` 只是一份无 schema 的
    /// 内存占位：尚未双路由的 SQLite 门面会报 "no such table" 而 fail-closed，
    /// 绝不静默写盘（E/F 逐个收口）。
    object: std::sync::OnceLock<std::sync::Arc<crate::control_backend::ObjectControl>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UploadRecord {
    pub saved_path: String,
    pub bucket: String,
    pub key: String,
    pub group_name: String,
    pub size: i64,
    pub etag: String,
    pub original_name: Option<String>,
    pub created_at: i64,
}

fn upload_record(r: xl_store::control_store::uploads::UploadRow) -> UploadRecord {
    UploadRecord {
        saved_path: r.saved_path,
        bucket: r.bucket,
        key: r.key,
        group_name: r.group_name,
        size: r.size,
        etag: r.etag,
        original_name: r.original_name,
        created_at: r.created_at,
    }
}

/// 本站在复制关系中的身份（`site_config` 单行；token 仅供控制面通信用，接口层负责脱敏）。
#[derive(Debug, Clone)]
pub struct SiteIdentity {
    pub id: String,
    pub name: String,
    pub region: String,
    pub priority: i64,
    pub token: String,
}

/// 对端站点行（`sites`；含出站凭据——接口层必须脱敏）。
#[derive(Debug, Clone)]
pub struct SitePeer {
    pub peer_id: String,
    /// 对端 `site_config.id`（配对时由 identity 校验取得；dead 表按它归属）
    pub remote_site_id: String,
    pub name: String,
    pub endpoint: String,
    pub region: String,
    pub priority: i64,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub peer_site_token: String,
    pub created_at: i64,
    pub last_seen: Option<i64>,
    pub last_error: Option<String>,
    /// M9：对端 Raft u64 节点 id（M7 行 NULL）。
    pub raft_node_id: Option<u64>,
    /// M9 S7.4：替换站点后，sync 修复完成前拒绝向该对端复制/服务。
    pub reconcile_required: bool,
}

/// 控制台用户行（子账户管理页用；`permissions` 全量随行返回，页面上直接展示摘要）。
#[derive(Debug, Clone, Serialize)]
pub struct UserRow {
    pub id: i64,
    pub username: String,
    pub is_admin: bool,
    /// `"active"` / `"disabled"`（停用即全链路失效）
    pub status: String,
    pub created_at: i64,
    pub permissions: Vec<PermissionRow>,
}

/// 账户行（AC-0；控制台 `/accounts` 直接序列化）。
#[derive(Debug, Clone, Serialize)]
pub struct AccountRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub admin_uid: Option<i64>,
    pub bucket_quota: Option<i64>,
    /// 软字节配额（NULL = 不限；只告警不阻写，AC-2）。
    pub soft_byte_quota: Option<i64>,
    /// 硬字节配额（NULL/负数 = 不限；写入原子预留，删除释放）。
    pub hard_byte_quota: Option<i64>,
    /// 硬对象数配额（当前版本键数；NULL/负数 = 不限）。
    pub object_quota: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub member_count: i64,
    pub bucket_count: i64,
}

/// 账户成员行（列表页用；不返回任何凭据）。
#[derive(Debug, Clone, Serialize)]
pub struct AccountMemberRow {
    pub uid: i64,
    pub username: String,
    pub role: String,
    pub is_admin: bool,
    pub status: String,
    pub created_at: i64,
}

/// 子账户 × 桶 的三档权限（与 `permissions` 表一一对应；序列化给前端用 read/write/delete）。
#[derive(Debug, Clone, PartialEq, Serialize, sqlx::FromRow)]
pub struct PermissionRow {
    pub bucket: String,
    #[sqlx(rename = "can_read")]
    pub read: bool,
    #[sqlx(rename = "can_write")]
    pub write: bool,
    #[sqlx(rename = "can_delete")]
    pub delete: bool,
}

// ——— M8 S0：作业引擎数据结构 ———

/// 建作业时的一条任务。
#[derive(Debug, Clone)]
pub struct NewJobTask {
    pub action: String,
    pub payload: String,
    pub dedupe_key: Option<String>,
}

/// 建作业规格（S3/S4b/S5 共用）。
#[derive(Debug, Clone, Default)]
pub struct JobSpec {
    pub kind: String,
    pub bucket: String,
    pub prefix: String,
    pub requested_by: Option<i64>,
    pub priority: i64,
    pub parent_job_id: Option<i64>,
    pub report_bucket: Option<String>,
    pub report_prefix: Option<String>,
    pub spec_json: Option<String>,
    pub tasks: Vec<NewJobTask>,
}

/// `JobSpec` → 缝的整行输入（本地建作业 SQL 已收口到适配器）。
fn job_insert(spec: &JobSpec, now: i64) -> JobInsert {
    JobInsert {
        kind: spec.kind.clone(),
        bucket: spec.bucket.clone(),
        prefix: spec.prefix.clone(),
        state: "queued".into(),
        total: spec.tasks.len() as i64,
        priority: spec.priority,
        requested_by: spec.requested_by,
        parent_job_id: spec.parent_job_id,
        report_bucket: spec.report_bucket.clone(),
        report_prefix: spec.report_prefix.clone(),
        spec_json: spec.spec_json.clone(),
        created_at: now,
        updated_at: now,
        started_at: None,
    }
}

fn new_tasks(spec: &JobSpec, now: i64) -> Vec<JobTaskInsert> {
    spec.tasks
        .iter()
        .map(|t| JobTaskInsert {
            action: t.action.clone(),
            payload_json: t.payload.clone(),
            dedupe_key: t.dedupe_key.clone(),
            next_at: now,
        })
        .collect()
}

/// M8 S4b：Restore 行（`tier_restores`；reaper/对账的跨 crate 视图）。
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreRow {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub expires_at: i64,
    pub job_id: Option<i64>,
    pub state: String,
    pub created_at: i64,
}

fn restore_row_of(r: TierRestoreRow) -> RestoreRow {
    RestoreRow {
        bucket: r.bucket,
        key: r.key,
        version_id: r.version_id,
        expires_at: r.expires_at,
        job_id: r.job_id,
        state: r.state,
        created_at: r.created_at,
    }
}

/// M8 S5：Inventory 运行行（`started_at` = 实际开始执行的时刻）。
/// 类型收敛到 ControlStore 缝（含 fence 两列）；`id` 是行内不可变属性，不是身份键。
pub use xl_store::control_store::inventory::InventoryRunRow;

/// 领取到的任务（worker 执行输入）。
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub job_id: i64,
    pub task_no: i64,
    pub action: String,
    pub payload_json: String,
    pub attempt: i64,
}

// migrations/ 只是目录命名习惯：本实现用 include_str! + raw_sql 原样执行整份 schema，
// 不建 _sqlx_migrations —— 将来加 0002 不会自动执行（M2 起再引 sqlx migrate）。
const SCHEMA: &str = include_str!("../migrations/0001_init.sql");
/// M7：每次启动重放（表 IF NOT EXISTS；触发器 DROP+CREATE；必须在 ensure_columns 之后——
/// 触发器引用运行时补列 kind/status/object_lock）。
const SCHEMA_M7: &str = include_str!("../migrations/0002_m7_site.sql");
/// M8（S0）：全部新表/索引一次性建齐；后续切片只使用、不改本文件。
const SCHEMA_M8: &str = include_str!("../migrations/0003_m8.sql");
/// M9 S0：控制面契约表 + 11 张复制表的 guard。每次启动重放（IF NOT EXISTS / DROP+CREATE）。
const SCHEMA_M9: &str = include_str!("../migrations/0004_m9.sql");
/// AC-0：账户/成员表。每次启动重放（IF NOT EXISTS），回填幂等（marker 只跑一次）。
const SCHEMA_ACCOUNTS: &str = include_str!("../migrations/0005_accounts.sql");
/// AC-2：账户用量表。每次启动重放（IF NOT EXISTS）。
const SCHEMA_TENANCY: &str = include_str!("../migrations/0006_tenancy.sql");
/// M8 多节点：调度租约 + 运行态序列。每次启动重放（IF NOT EXISTS）。
const SCHEMA_RUNSTATE: &str = include_str!("../migrations/0007_m8_runstate.sql");
/// 审计/上传/秒传索引（2026-10-30 审计）。每次启动重放（IF NOT EXISTS）。
const SCHEMA_HOT_INDEXES: &str = include_str!("../migrations/0008_hot_indexes.sql");
/// 作业保留清理索引（`terminal_jobs_before` 的 state+updated_at）。每次启动重放。
const SCHEMA_JOB_RETENTION_INDEX: &str = include_str!("../migrations/0009_job_retention_index.sql");
/// 动态 VPC 端点注册表（feature 8）。每次启动重放（IF NOT EXISTS）。
const SCHEMA_VPC_ENDPOINTS: &str = include_str!("../migrations/0010_vpc_endpoints.sql");
/// 复制账户用量账本（feature 9）。每次启动重放（IF NOT EXISTS）。
const SCHEMA_ACCOUNT_USAGE_LEDGER: &str =
    include_str!("../migrations/0011_account_usage_ledger.sql");
/// 热路径查询索引（复制积压/审计过滤）。每次启动重放（IF NOT EXISTS）。
const SCHEMA_QUERY_INDEXES: &str = include_str!("../migrations/0012_query_indexes.sql");

/// 账户表已规范化（v2 回填/重写完成）。standalone 启动时写；Raft 模式由后续
/// canonicalization worker 在 level-3 写，快照读取以本 marker 为门。
pub const META_ACCOUNTS_CANONICAL: &str = "accounts_canonical_v1";

/// WAL 走连接选项、不走 URL：sqlx 0.8 的 URL 解析只认 mode/cache/immutable/vfs，
/// `?journal_mode=WAL` 会直接报 unknown query parameter。选项形式对池内每条新连接生效。
/// busy_timeout：多 job worker 并发写同库时用等待代替立即 SQLITE_BUSY（默认 5s 偏短）。
fn wal_options(url: &str) -> Result<SqliteConnectOptions> {
    Ok(url
        .parse::<SqliteConnectOptions>()?
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(30)))
}

pub(crate) fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Db {
    /// M7/M8 默认连接：重放 M9 schema 后 `prepare_raft_mode(false)`（drop guard），
    /// 库已处于 Raft 模式（`schema_meta.raft_mode=on`）时拒绝启动（降级守卫）。
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with(url, true, false, None).await
    }

    /// M9：Raft 数据站点连接（种 site_config，保留/重建 guard，drop `site_*` 触发器）。
    pub async fn connect_raft(url: &str) -> Result<Self> {
        Self::connect_with(url, true, true, None).await
    }

    /// S7.1：serve 启动路径——带 `[site].id` 首启种子（已有库 id 不一致 → 拒绝启动）。
    pub async fn connect_site(url: &str, raft: bool, site_id: Option<&str>) -> Result<Self> {
        Self::connect_with(url, true, raft, site_id).await
    }

    /// M9 witness：不种 `site_config`（witness 无站点行），其余同 Raft。
    pub async fn connect_witness(url: &str) -> Result<Self> {
        Self::connect_with(url, false, true, None).await
    }

    /// B4 对象后端启动占位：**不打开/创建 `oss.db`，也不跑任何迁移**。`pool` 是
    /// 无 schema 的内存库，仅保证未迁移子系统能构造且误写时以 "no such table"
    /// fail-closed；控制面读写走启动装配注入的 `ObjectControl`。
    pub async fn connect_object() -> Result<Self> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        Ok(Self {
            pool,
            raft_mode: std::sync::atomic::AtomicBool::new(false),
            object: std::sync::OnceLock::new(),
        })
    }

    /// 离线运维连接：只开 WAL 池，不重放 schema、不切模式（避免二次开 raft.db / 降级守卫）。
    pub async fn connect_offline(url: &str) -> Result<Self> {
        let pool = SqlitePool::connect_with(wal_options(url)?).await?;
        Ok(Self {
            pool,
            raft_mode: std::sync::atomic::AtomicBool::new(false),
            object: std::sync::OnceLock::new(),
        })
    }

    async fn connect_with(
        url: &str,
        seed_site: bool,
        raft: bool,
        site_id: Option<&str>,
    ) -> Result<Self> {
        let pool = SqlitePool::connect_with(wal_options(url)?).await?;
        // sqlx::raw_sql 支持一个字符串里的多条语句，schema 文件原样执行
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        let db = Self {
            pool,
            raft_mode: std::sync::atomic::AtomicBool::new(false),
            object: std::sync::OnceLock::new(),
        };
        db.ensure_columns().await?;
        // 生命周期表主键重建（旧 PK (bucket,prefix) → (bucket,rule_id)）：必须在 0003 建
        // 唯一索引前完成；触发器由 0004 重放重建。
        db.ensure_lifecycle_pk().await?;
        // M7：增量列就位后再建站点表/触发器（触发器引用补列），并种下本站身份。
        sqlx::raw_sql(SCHEMA_M7).execute(&db.pool).await?;
        // M8：新表无触发器依赖，独立重放；任一语句失败必须启动失败（不吞错）。
        sqlx::raw_sql(SCHEMA_M8).execute(&db.pool).await?;
        // M9：契约表 + guard 重放；列迁移在 M7 表存在后执行。
        sqlx::raw_sql(SCHEMA_M9).execute(&db.pool).await?;
        db.ensure_columns_m9().await?;
        // AC-0：账户表 + 存量用户回填（marker 幂等）。
        sqlx::raw_sql(SCHEMA_ACCOUNTS).execute(&db.pool).await?;
        sqlx::raw_sql(SCHEMA_TENANCY).execute(&db.pool).await?;
        sqlx::raw_sql(SCHEMA_RUNSTATE).execute(&db.pool).await?;
        sqlx::raw_sql(SCHEMA_HOT_INDEXES).execute(&db.pool).await?;
        sqlx::raw_sql(SCHEMA_JOB_RETENTION_INDEX)
            .execute(&db.pool)
            .await?;
        sqlx::raw_sql(SCHEMA_VPC_ENDPOINTS)
            .execute(&db.pool)
            .await?;
        sqlx::raw_sql(SCHEMA_ACCOUNT_USAGE_LEDGER)
            .execute(&db.pool)
            .await?;
        sqlx::raw_sql(SCHEMA_QUERY_INDEXES)
            .execute(&db.pool)
            .await?;
        db.ensure_columns_tenancy().await?;
        // M8-1：运行态表 fencing 列依赖 0003 已建表，故放在 M8/M9 重放之后。
        db.ensure_fence_columns().await?;
        if seed_site {
            db.site_bootstrap(site_id).await?;
        }
        db.prepare_raft_mode(raft).await?;
        // standalone 启动即回填并规范化（v2 移出全局管理员）；放在模式守卫之后：Raft 库被
        // 误按 standalone 启动时，不得在报错前写 canonical marker。Raft 下不跑 uid 派生
        // 回填（各节点 uid 不同会分叉）：空账户表原子补 canonical marker，已有非 canonical
        // 行则拒绝启动，交由快照/导出规范化。
        if raft {
            db.ensure_raft_accounts_canonical().await?;
        } else {
            db.backfill_accounts().await?;
            db.backfill_accounts_v2().await?;
        }
        Ok(db)
    }

    /// Raft 启动守卫：账户两表只能在 canonical marker 之后跨节点解释。
    /// 全新集群（两表均空）原子写 marker；任一表非空但没有 marker，说明是节点本地
    /// uid 派生数据，拒绝启动（绝不能跑 standalone v2 回填——各节点 uid 不同会分叉）。
    async fn ensure_raft_accounts_canonical(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let marker: Option<String> =
            sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = ?")
                .bind(META_ACCOUNTS_CANONICAL)
                .fetch_optional(&mut *tx)
                .await?;
        if marker.is_some() {
            return Ok(());
        }
        let (accounts, members) = crate::control_store::SqliteControlTxn::new(&mut tx)
            .account_counts()
            .await
            .map_err(crate::control_store::to_sqlx)?;
        if accounts != 0 || members != 0 {
            return Err(sqlx::Error::Protocol(format!(
                "Raft 模式启动失败：缺少 schema_meta.{META_ACCOUNTS_CANONICAL} 但账户表非空 \
                 (accounts={accounts}, account_members={members})；这些行是节点本地 uid 派生\
                 数据，跨节点不可解释。请先用 OSSCTL3 快照/导出规范化后再启动"
            )));
        }
        sqlx::query(
            "INSERT INTO schema_meta (key, value) VALUES (?, 'done') \
             ON CONFLICT(key) DO UPDATE SET value = 'done'",
        )
        .bind(META_ACCOUNTS_CANONICAL)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// 模式切换（无条件调用）：
    /// - `false`：库为 `raft_mode=on` → 拒绝启动；否则 drop 全部 `raft_guard_*`（M7 可写）。
    /// - `true`：重放 M9 重建 guard、drop 全部 `site_*`、写 on、复位 apply ctx。
    pub async fn prepare_raft_mode(&self, on: bool) -> Result<()> {
        if on {
            sqlx::raw_sql(SCHEMA_M9).execute(&self.pool).await?;
            self.drop_triggers("site_%").await?;
            sqlx::query(
                "INSERT INTO schema_meta (key, value) VALUES ('raft_mode', 'on') \
                 ON CONFLICT(key) DO UPDATE SET value = 'on'",
            )
            .execute(&self.pool)
            .await?;
            self.set_raft_apply_ctx(false).await?;
        } else {
            if self.schema_meta("raft_mode").await?.as_deref() == Some("on") {
                return Err(sqlx::Error::Protocol(
                    "配置缺少 [raft] 但数据库处于 Raft 模式（schema_meta.raft_mode=on）：\
                     请恢复 [raft] 配置，或按 runbook 执行 force recovery"
                        .into(),
                ));
            }
            self.drop_triggers("raft_guard_%").await?;
        }
        self.raft_mode
            .store(on, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// S6d 启动断言：模式与库内状态一致——Raft 下 33 个 `raft_guard_*` 触发器在、M7
    /// 站点状态表为空（`sites` 配对行允许）；M7 下守卫全灭。不一致即拒绝启动。
    pub async fn assert_mode_guards(&self) -> Result<()> {
        let guards: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'raft_guard\\_%' ESCAPE '\\'",
        )
        .fetch_one(&self.pool)
        .await?;
        if !self.raft_mode() {
            return if guards == 0 {
                Ok(())
            } else {
                Err(sqlx::Error::Protocol(format!(
                    "M7 模式不应存在 raft_guard_* 触发器，实际 {guards} 个"
                )))
            };
        }
        if guards != 33 {
            return Err(sqlx::Error::Protocol(format!(
                "Raft 模式守卫不完整：raft_guard_* 触发器 {guards}/33"
            )));
        }
        // 表名是常量；M7 日志/游标状态在 Raft 下必须为空（配对 `sites` 行不在此列）。
        for table in [
            "site_changelog",
            "site_entity_clock",
            "site_cursors",
            "site_apply_dead",
        ] {
            let n: i64 =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                    .fetch_one(&self.pool)
                    .await?;
            if n != 0 {
                return Err(sqlx::Error::Protocol(format!(
                    "Raft 模式站点状态表 {table} 应为空，实际 {n} 行"
                )));
            }
        }
        Ok(())
    }

    /// M9：是否为 Raft 控制库（`connect_raft`/`connect_witness` 或 `prepare_raft_mode(true)`）。
    pub fn raft_mode(&self) -> bool {
        self.raft_mode.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 按 `sqlite_master` 名称模式 drop 触发器（名称来自库内，非用户输入）。
    async fn drop_triggers(&self, pattern: &str) -> Result<()> {
        let rows =
            sqlx::query("SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE ?")
                .bind(pattern)
                .fetch_all(&self.pool)
                .await?;
        for row in rows {
            let name: String = row.get("name");
            let sql = format!("DROP TRIGGER IF EXISTS \"{}\"", name.replace('"', "\"\""));
            sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// `schema_meta` 读（不存在 = None）。
    pub async fn schema_meta(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT value FROM schema_meta WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| r.get::<Option<String>, _>("value")))
    }

    /// 仅在 key 不存在时写入（首启身份持久化；返回是否写入）。
    /// S7.5：无条件写 `schema_meta`（cutover/force-recover 标记）。
    pub async fn schema_meta_set(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO schema_meta (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn schema_meta_set_if_absent(&self, key: &str, value: &str) -> Result<bool> {
        let res = sqlx::query("INSERT OR IGNORE INTO schema_meta (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// 无条件写 `schema_meta`（Raft 身份状态：initialize 后置 1 等）。
    pub async fn upsert_schema_meta(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO schema_meta (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// apply 事务内标记：**只允许本模块启动路径使用**——apply 在自身事务里写同一行，
    /// 外部调用者拿不到写控制表的入口（编译闸，matrix §9）。
    async fn set_raft_apply_ctx(&self, active: bool) -> Result<()> {
        sqlx::query("UPDATE raft_apply_ctx SET active = ? WHERE id = 1")
            .bind(active as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 本站身份种子（幂等）：首启生成 uuid + 32 hex token；后续启动不动。
    /// S7.1：`[site].id` 仅用于首启；已有行 id 不同 → 拒绝启动。
    /// 同时复位 `apply_ctx`（上次进程若在启动对账/apply 中途退出，防止抑制永久残留——P12）。
    async fn site_bootstrap(&self, seed_id: Option<&str>) -> Result<()> {
        let token = uuid::Uuid::new_v4().simple().to_string();
        // 身份必须**恰好一行**：触发器 `SELECT id FROM site_config LIMIT 1` 与
        // `site_identity()` 的查询计划可能取到不同行（索引扫描 vs rowid 扫描）。
        // 旧版本缺陷可能留下多行 → 收敛到最早一行；新库用固定 id（36 字符，保持 uuid 形态）。
        let mut conn = self.pool.acquire().await?;
        let mut txn = SqliteControlTxn::new(&mut conn);
        txn.site_config_trim()
            .await
            .map_err(crate::control_store::to_sqlx)?;
        let existing = txn
            .site_config_id()
            .await
            .map_err(crate::control_store::to_sqlx)?;
        if let Some(seed) = seed_id {
            // 配置层已校验；DB 也是信任边界（旧库/手改配置）。
            if seed.is_empty()
                || seed.len() > 64
                || !seed
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            {
                return Err(sqlx::Error::Protocol(format!(
                    "[site].id 非法：{seed:?}（白名单 [A-Za-z0-9._-]，≤64）"
                )));
            }
            if let Some(old) = &existing
                && old != seed
            {
                return Err(sqlx::Error::Protocol(format!(
                    "[site].id={seed} 与库内站点 id={old} 不一致：拒绝启动（id 一经生成不可变更）"
                )));
            }
        }
        let id = match (seed_id, &existing) {
            (Some(seed), _) => seed.to_string(),
            (None, Some(old)) => old.clone(),
            (None, None) => uuid::Uuid::new_v4().to_string(),
        };
        txn.site_config_seed(&id, &token, now())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        sqlx::query("UPDATE apply_ctx SET active = 0 WHERE id = 1")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 启动对账期间抑制站点触发器（P3；结束后由调用方复位 0）。
    pub async fn set_apply_ctx(&self, active: bool) -> Result<()> {
        sqlx::query("UPDATE apply_ctx SET active = ? WHERE id = 1")
            .bind(active as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 极小增量迁移：0001 是**整份重放**（全部 IF NOT EXISTS），而 `ALTER ADD COLUMN`
    /// 不幂等，放不进重放——增量列在这里探测后补齐。新库由 0001 直接带列，两个路径收敛。
    /// 字面量 SQL（sqlx 0.9 拒动态串）；增量列：users.status · buckets.set_id ·
    /// buckets.object_lock 三列（M5 切片 4）· buckets.default_encryption（M8 缺口 C）·
    /// access_keys 会话三列（M5 切片 5）。
    async fn ensure_columns(&self) -> Result<()> {
        // M4 切片 8：buckets.set（多 set 归属；老库补列，幂等——与 status 同款竞态容忍）
        let bcols = sqlx::query("SELECT name FROM pragma_table_info('buckets')")
            .fetch_all(&self.pool)
            .await?;
        if !bcols.iter().any(|r| r.get::<String, _>("name") == "set_id")
            && let Err(e) =
                sqlx::query("ALTER TABLE buckets ADD COLUMN set_id INTEGER NOT NULL DEFAULT 0")
                    .execute(&self.pool)
                    .await
        {
            let bcols = sqlx::query("SELECT name FROM pragma_table_info('buckets')")
                .fetch_all(&self.pool)
                .await?;
            if !bcols.iter().any(|r| r.get::<String, _>("name") == "set_id") {
                return Err(e);
            }
        }
        // M5 切片 4：对象锁三列（同款幂等补列）
        let bcols = sqlx::query("SELECT name FROM pragma_table_info('buckets')")
            .fetch_all(&self.pool)
            .await?;
        let has = |n: &str| bcols.iter().any(|r| r.get::<String, _>("name") == n);
        for (name, ddl) in [
            (
                "object_lock",
                "ALTER TABLE buckets ADD COLUMN object_lock INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "object_lock_default_mode",
                "ALTER TABLE buckets ADD COLUMN object_lock_default_mode TEXT",
            ),
            (
                "object_lock_default_days",
                "ALTER TABLE buckets ADD COLUMN object_lock_default_days INTEGER",
            ),
            (
                "default_encryption",
                "ALTER TABLE buckets ADD COLUMN default_encryption TEXT",
            ),
            (
                "replication_mode",
                "ALTER TABLE buckets ADD COLUMN replication_mode TEXT NOT NULL DEFAULT 'async'",
            ),
            (
                "deleting",
                "ALTER TABLE buckets ADD COLUMN deleting INTEGER NOT NULL DEFAULT 0",
            ),
        ] {
            if !has(name)
                && let Err(e) = sqlx::query(ddl).execute(&self.pool).await
            {
                let again = sqlx::query("SELECT name FROM pragma_table_info('buckets')")
                    .fetch_all(&self.pool)
                    .await?;
                if !again.iter().any(|r| r.get::<String, _>("name") == name) {
                    return Err(e);
                }
            }
        }
        // M5 切片 5：access_keys 会话三列（parent_ak/session_policy/kind；同款幂等补列）
        let acols = sqlx::query("SELECT name FROM pragma_table_info('access_keys')")
            .fetch_all(&self.pool)
            .await?;
        let has_ak = |n: &str| acols.iter().any(|r| r.get::<String, _>("name") == n);
        for (name, ddl) in [
            (
                "parent_ak",
                "ALTER TABLE access_keys ADD COLUMN parent_ak TEXT",
            ),
            (
                "session_policy",
                "ALTER TABLE access_keys ADD COLUMN session_policy TEXT",
            ),
            (
                "kind",
                "ALTER TABLE access_keys ADD COLUMN kind TEXT NOT NULL DEFAULT 'long-term'",
            ),
        ] {
            if !has_ak(name)
                && let Err(e) = sqlx::query(ddl).execute(&self.pool).await
            {
                let again = sqlx::query("SELECT name FROM pragma_table_info('access_keys')")
                    .fetch_all(&self.pool)
                    .await?;
                if !again.iter().any(|r| r.get::<String, _>("name") == name) {
                    return Err(e);
                }
            }
        }
        let cols = sqlx::query("SELECT name FROM pragma_table_info('users')")
            .fetch_all(&self.pool)
            .await?;
        let has_status = cols.iter().any(|r| r.get::<String, _>("name") == "status");
        if !has_status {
            // 多进程同时首开老库时，双方都可能走到这里 → 后到者会吃 duplicate column；
            // 此时列已被对方补上，重查确认后视同成功（评审 P3）。
            if let Err(e) =
                sqlx::query("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
                    .execute(&self.pool)
                    .await
            {
                let cols = sqlx::query("SELECT name FROM pragma_table_info('users')")
                    .fetch_all(&self.pool)
                    .await?;
                let now_has = cols.iter().any(|r| r.get::<String, _>("name") == "status");
                if !now_has {
                    return Err(e);
                }
            }
        }
        // M7：repl_outbox 站点目标列（幂等补列；老库升级路径）
        let rcols = sqlx::query("SELECT name FROM pragma_table_info('repl_outbox')")
            .fetch_all(&self.pool)
            .await?;
        if !rcols
            .iter()
            .any(|r| r.get::<String, _>("name") == "target_kind")
            && let Err(e) = sqlx::query(
                "ALTER TABLE repl_outbox ADD COLUMN target_kind TEXT NOT NULL DEFAULT 'remote'",
            )
            .execute(&self.pool)
            .await
        {
            let again = sqlx::query("SELECT name FROM pragma_table_info('repl_outbox')")
                .fetch_all(&self.pool)
                .await?;
            if !again
                .iter()
                .any(|r| r.get::<String, _>("name") == "target_kind")
            {
                return Err(e);
            }
        }
        // 4.16：repl_outbox 版本定向删除标记（幂等补列；老库升级路径）
        if !rcols
            .iter()
            .any(|r| r.get::<String, _>("name") == "is_version_delete")
            && let Err(e) = sqlx::query(
                "ALTER TABLE repl_outbox ADD COLUMN is_version_delete INTEGER NOT NULL DEFAULT 0",
            )
            .execute(&self.pool)
            .await
        {
            let again = sqlx::query("SELECT name FROM pragma_table_info('repl_outbox')")
                .fetch_all(&self.pool)
                .await?;
            if !again
                .iter()
                .any(|r| r.get::<String, _>("name") == "is_version_delete")
            {
                return Err(e);
            }
        }
        // M8（S0）：jobs 作业引擎列（老库补列；幂等）
        let jcols = sqlx::query("SELECT name FROM pragma_table_info('jobs')")
            .fetch_all(&self.pool)
            .await?;
        let has_j = |n: &str| jcols.iter().any(|r| r.get::<String, _>("name") == n);
        for (name, ddl) in [
            ("spec_json", "ALTER TABLE jobs ADD COLUMN spec_json TEXT"),
            (
                "progress",
                "ALTER TABLE jobs ADD COLUMN progress INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "total",
                "ALTER TABLE jobs ADD COLUMN total INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "succeeded",
                "ALTER TABLE jobs ADD COLUMN succeeded INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "failed",
                "ALTER TABLE jobs ADD COLUMN failed INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "report_bucket",
                "ALTER TABLE jobs ADD COLUMN report_bucket TEXT",
            ),
            (
                "report_prefix",
                "ALTER TABLE jobs ADD COLUMN report_prefix TEXT",
            ),
            (
                "priority",
                "ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "requested_by",
                "ALTER TABLE jobs ADD COLUMN requested_by INTEGER",
            ),
            (
                "parent_job_id",
                "ALTER TABLE jobs ADD COLUMN parent_job_id INTEGER",
            ),
            (
                "started_at",
                "ALTER TABLE jobs ADD COLUMN started_at INTEGER",
            ),
            (
                "finished_at",
                "ALTER TABLE jobs ADD COLUMN finished_at INTEGER",
            ),
        ] {
            if !has_j(name)
                && let Err(e) = sqlx::query(ddl).execute(&self.pool).await
            {
                let again = sqlx::query("SELECT name FROM pragma_table_info('jobs')")
                    .fetch_all(&self.pool)
                    .await?;
                if !again.iter().any(|r| r.get::<String, _>("name") == name) {
                    return Err(e);
                }
            }
        }
        // M8（S4a/S4b）：生命周期规则 id/expiration/Transition 五列（老库补列；幂等）
        let lcols = sqlx::query("SELECT name FROM pragma_table_info('lifecycle_rules')")
            .fetch_all(&self.pool)
            .await?;
        let has_l = |n: &str| lcols.iter().any(|r| r.get::<String, _>("name") == n);
        for (name, ddl) in [
            (
                "rule_id",
                "ALTER TABLE lifecycle_rules ADD COLUMN rule_id TEXT",
            ),
            (
                "expiration_days",
                "ALTER TABLE lifecycle_rules ADD COLUMN expiration_days INTEGER",
            ),
            (
                "transition_days",
                "ALTER TABLE lifecycle_rules ADD COLUMN transition_days INTEGER",
            ),
            (
                "transition_class",
                "ALTER TABLE lifecycle_rules ADD COLUMN transition_class TEXT",
            ),
            (
                "transition_tier",
                "ALTER TABLE lifecycle_rules ADD COLUMN transition_tier TEXT",
            ),
            (
                "config_json",
                "ALTER TABLE lifecycle_rules ADD COLUMN config_json TEXT",
            ),
        ] {
            if !has_l(name)
                && let Err(e) = sqlx::query(ddl).execute(&self.pool).await
            {
                let again = sqlx::query("SELECT name FROM pragma_table_info('lifecycle_rules')")
                    .fetch_all(&self.pool)
                    .await?;
                if !again.iter().any(|r| r.get::<String, _>("name") == name) {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// M8-1：运行态本地表补 fencing 列（老库升级；新库也走这里，0003 不新增列）。
    /// `(fence_term, fence_leader)` = 最近一次被接受的状态转换 token；写入按 term 单调，
    /// 旧 leader 的迟到写入不覆盖新 term 的状态（no-op）。默认 0,0 = M7/未 fence。
    async fn ensure_fence_columns(&self) -> Result<()> {
        for table in [
            "job_tasks",
            "inventory_runs",
            "inventory_cursors",
            "tier_restores",
            "lifecycle_cursors",
        ] {
            let cols = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT name FROM pragma_table_info('{table}')"
            )))
            .fetch_all(&self.pool)
            .await?;
            let has = |n: &str| cols.iter().any(|r| r.get::<String, _>("name") == n);
            for (name, ddl) in [
                (
                    "fence_term",
                    format!("ALTER TABLE {table} ADD COLUMN fence_term INTEGER NOT NULL DEFAULT 0"),
                ),
                (
                    "fence_leader",
                    format!(
                        "ALTER TABLE {table} ADD COLUMN fence_leader INTEGER NOT NULL DEFAULT 0"
                    ),
                ),
            ] {
                if !has(name)
                    && let Err(e) = sqlx::query(sqlx::AssertSqlSafe(ddl))
                        .execute(&self.pool)
                        .await
                {
                    // 并发首开：对方已补列则视同成功。
                    let again = sqlx::query(sqlx::AssertSqlSafe(format!(
                        "SELECT name FROM pragma_table_info('{table}')"
                    )))
                    .fetch_all(&self.pool)
                    .await?;
                    if !again.iter().any(|r| r.get::<String, _>("name") == name) {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    /// 生命周期表主键重建：旧 PK `(bucket,prefix)` 与「同 prefix 多规则」冲突。
    /// SQLite 不能改 PK，只能一次性重建；幂等（新 PK 时直接返回）。
    async fn ensure_lifecycle_pk(&self) -> Result<()> {
        let pk: Vec<(String, i64)> = sqlx::query_as(
            "SELECT name, pk FROM pragma_table_info('lifecycle_rules') WHERE pk > 0 ORDER BY pk",
        )
        .fetch_all(&self.pool)
        .await?;
        if pk.len() == 2 && pk[0].0 == "bucket" && pk[1].0 == "rule_id" {
            return Ok(());
        }
        // 重建 + 搬行必须原子：崩溃落在 RENAME 与 DROP 之间会丢全部规则。
        let mut tx = self.pool.begin().await?;
        sqlx::raw_sql(
            "DROP INDEX IF EXISTS idx_lifecycle_rule_id;\
             DROP TRIGGER IF EXISTS raft_guard_lifecycle_rules_ins;\
             DROP TRIGGER IF EXISTS raft_guard_lifecycle_rules_upd;\
             DROP TRIGGER IF EXISTS raft_guard_lifecycle_rules_del;\
             ALTER TABLE lifecycle_rules RENAME TO lifecycle_rules_old;\
             CREATE TABLE lifecycle_rules (\
                 bucket TEXT NOT NULL,\
                 prefix TEXT NOT NULL DEFAULT '',\
                 expire_days INTEGER NOT NULL,\
                 enabled INTEGER NOT NULL DEFAULT 1,\
                 updated_at INTEGER NOT NULL,\
                 rule_id TEXT,\
                 expiration_days INTEGER,\
                 transition_days INTEGER,\
                 transition_class TEXT,\
                 transition_tier TEXT,\
                 config_json TEXT,\
                 PRIMARY KEY (bucket, rule_id)\
             );\
             -- audit-allow: schema migration (old lifecycle PK rewrite).\n\
             INSERT INTO lifecycle_rules (bucket,prefix,expire_days,enabled,updated_at,rule_id,\
                 expiration_days,transition_days,transition_class,transition_tier,config_json) \
                 SELECT bucket,prefix,expire_days,enabled,updated_at,rule_id,\
                 expiration_days,transition_days,transition_class,transition_tier,config_json \
                 FROM lifecycle_rules_old;\
             DROP TABLE lifecycle_rules_old;\
             CREATE UNIQUE INDEX IF NOT EXISTS idx_lifecycle_rule_id \
                 ON lifecycle_rules(bucket, rule_id);",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// M9：`sites.raft_node_id`（Raft 模式下对端身份的 u64 id；M7 行留 NULL）。
    async fn ensure_columns_m9(&self) -> Result<()> {
        let cols = sqlx::query("SELECT name FROM pragma_table_info('sites')")
            .fetch_all(&self.pool)
            .await?;
        if !cols
            .iter()
            .any(|r| r.get::<String, _>("name") == "raft_node_id")
            && let Err(e) = sqlx::query("ALTER TABLE sites ADD COLUMN raft_node_id INTEGER")
                .execute(&self.pool)
                .await
        {
            let again = sqlx::query("SELECT name FROM pragma_table_info('sites')")
                .fetch_all(&self.pool)
                .await?;
            if !again
                .iter()
                .any(|r| r.get::<String, _>("name") == "raft_node_id")
            {
                return Err(e);
            }
        }
        if !cols
            .iter()
            .any(|r| r.get::<String, _>("name") == "reconcile_required")
        {
            sqlx::query(
                "ALTER TABLE sites ADD COLUMN reconcile_required INTEGER NOT NULL DEFAULT 0",
            )
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// 多租户补列：`accounts` 的软/硬配额列与 `audit_log` 的账户审计列。
    /// 老库升级 + 并发首开容忍（对方已补列视同成功）。
    async fn ensure_columns_tenancy(&self) -> Result<()> {
        self.ensure_column(
            "accounts",
            "soft_byte_quota",
            "ALTER TABLE accounts ADD COLUMN soft_byte_quota INTEGER",
        )
        .await?;
        self.ensure_column(
            "accounts",
            "hard_byte_quota",
            "ALTER TABLE accounts ADD COLUMN hard_byte_quota INTEGER",
        )
        .await?;
        self.ensure_column(
            "accounts",
            "object_quota",
            "ALTER TABLE accounts ADD COLUMN object_quota INTEGER",
        )
        .await?;
        self.ensure_column(
            "audit_log",
            "principal_account",
            "ALTER TABLE audit_log ADD COLUMN principal_account TEXT",
        )
        .await?;
        self.ensure_column(
            "audit_log",
            "target_account",
            "ALTER TABLE audit_log ADD COLUMN target_account TEXT",
        )
        .await
    }

    /// 幂等补列：pragma 探测 → ALTER；并发首开时对方已补列即视为成功。
    async fn ensure_column(&self, table: &str, name: &str, ddl: &str) -> Result<()> {
        let cols = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT name FROM pragma_table_info('{table}')"
        )))
        .fetch_all(&self.pool)
        .await?;
        if cols.iter().any(|r| r.get::<String, _>("name") == name) {
            return Ok(());
        }
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(ddl))
            .execute(&self.pool)
            .await
        {
            let again = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT name FROM pragma_table_info('{table}')"
            )))
            .fetch_all(&self.pool)
            .await?;
            if !again.iter().any(|r| r.get::<String, _>("name") == name) {
                return Err(e);
            }
        }
        Ok(())
    }

    // ---------- M8（S1）：访问点 ----------

    /// 建 AP（同名冲突由调用方按 409 处理）。`false` = 同名已存在（不覆盖）。
    pub async fn create_access_point(
        &self,
        name: &str,
        bucket: &str,
        owner_id: i64,
        policy_json: Option<&str>,
        now: i64,
    ) -> Result<bool> {
        let owner = self.owner_ref(owner_id).await?;
        let mut tx = self.pool.begin().await?;
        let created = {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            txn.access_point_create(&AccessPointEntry {
                name: name.to_string(),
                bucket: bucket.to_string(),
                owner,
                policy_json: policy_json.map(str::to_string),
                created_at: now,
                updated_at: now,
            })
            .await
            .map_err(crate::control_store::to_sqlx)?
        };
        tx.commit().await?;
        Ok(created)
    }

    /// 账户命名空间 v1：桶主的 12 位账户 ID（NULL/管理员桶 = 系统账户）。
    /// 本节点无桶行（非 Raft 集群）时同样回落到系统账户。
    pub async fn bucket_account(&self, bucket: &str) -> Result<String> {
        Ok(
            match self
                .control_store()
                .bucket(bucket)
                .await
                .map_err(crate::control_store::to_sqlx)?
            {
                Some(v) => xl_s3::access::bucket_account_id(v.owner_uid, v.owner_is_admin),
                None => xl_s3::access::LEGACY_ACCOUNT.to_string(),
            },
        )
    }

    /// 更新 AP 策略（None = 清策略）。
    pub async fn set_access_point_policy(
        &self,
        name: &str,
        policy_json: Option<&str>,
        now: i64,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let updated = {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            txn.access_point_set_policy(name, policy_json, now)
                .await
                .map_err(crate::control_store::to_sqlx)?
        };
        tx.commit().await?;
        Ok(updated)
    }

    pub async fn delete_access_point(&self, name: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let deleted = {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            txn.access_point_delete(name)
                .await
                .map_err(crate::control_store::to_sqlx)?
        };
        tx.commit().await?;
        Ok(deleted)
    }

    // ---------- M8（S2）：Object Lambda 变换器 ----------

    /// 每 AP 至多一条（PK upsert）；保存前已由调用方编译校验。
    #[allow(clippy::too_many_arguments)]
    pub async fn set_olap_config(
        &self,
        name: &str,
        kind: &str,
        endpoint: Option<&str>,
        module: Option<&[u8]>,
        module_sha256: Option<&str>,
        timeout_ms: i64,
        max_response_bytes: i64,
        now: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            txn.olap_config_upsert(&OlapConfigEntry {
                access_point: name.to_string(),
                kind: kind.to_string(),
                endpoint: endpoint.map(str::to_string),
                module: module.map(<[u8]>::to_vec),
                module_sha256: module_sha256.map(str::to_string),
                timeout_ms,
                max_response_bytes,
                updated_at: now,
            })
            .await
            .map_err(crate::control_store::to_sqlx)?;
        }
        tx.commit().await?;
        Ok(())
    }

    // ---------- M8（S0）：桶删除的 M8 状态清理 ----------

    /// 删桶后清理 M8 表（jobs 按审计保留）。S3/console/cluster 路径走这里；
    /// site apply 在既有事务内走 [`Db::clear_bucket_m8_tx`]。
    pub async fn clear_bucket_m8(&self, bucket: &str) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        Self::clear_bucket_m8_tx(&mut conn, bucket).await
    }

    /// 事务内版本（`&mut SqliteConnection`；调用方负责 commit 与 cache 失效时机）。
    pub async fn clear_bucket_m8_tx(conn: &mut sqlx::SqliteConnection, bucket: &str) -> Result<()> {
        // 该桶的 AP 及其 OLAP 行（SQLite 默认无 FK 级联，txn 内显式先 OLAP 后 AP）。
        let mut txn = crate::control_store::SqliteControlTxn::new(&mut *conn);
        txn.delete_access_points_for_bucket(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        txn.delete_lifecycle_inventory_tier_for_bucket(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(())
    }

    // ---------- M7：站点身份与关系 ----------

    pub async fn site_identity(&self) -> Result<SiteIdentity> {
        let r = self
            .control_store()
            .site_identity()
            .await
            .map_err(crate::control_store::to_sqlx)?
            .ok_or_else(|| sqlx::Error::RowNotFound)?;
        Ok(SiteIdentity {
            id: r.id,
            name: r.name,
            region: r.region,
            priority: r.priority,
            token: r.token,
        })
    }

    /// `[site]` 配置覆盖（load 在桶对账之前调用；None 字段保持种子值）。
    pub async fn apply_site_config(
        &self,
        name: Option<&str>,
        region: Option<&str>,
        priority: Option<i64>,
        token: Option<&str>,
    ) -> Result<()> {
        let name = name.map(str::to_string);
        let region = region.map(str::to_string);
        let token = token.map(str::to_string);
        self.control_write("startup", move |txn| {
            let name = name.clone();
            let region = region.clone();
            let token = token.clone();
            Box::pin(async move {
                txn.site_config_apply(
                    name.as_deref(),
                    region.as_deref(),
                    priority,
                    token.as_deref(),
                )
                .await
            })
        })
        .await
    }

    pub async fn list_sites(&self) -> Result<Vec<SitePeer>> {
        let rows = self
            .control_store()
            .sites()
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(rows.into_iter().map(site_peer).collect())
    }

    pub async fn get_site(&self, peer_id: &str) -> Result<Option<SitePeer>> {
        Ok(self
            .control_store()
            .site(peer_id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(site_peer))
    }

    pub async fn count_sites(&self) -> Result<i64> {
        self.control_store()
            .site_count()
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// S7.4：Raft 配对行写入/更新；按 peer_id 或 remote_site_id 采用既有 M7 行，
    /// 不写 `site_cursors/changelog`（Raft 下 sites 是站点本地表，S6d 启动断言要求 site_* 为空）。
    pub async fn upsert_pair_peer(&self, p: &SitePeer) -> Result<()> {
        // 注意：UPDATE 不触碰 `reconcile_required`——该门只能由 repair 成功清除
        // （S7.4 review I2：重复/重试配对不得把 B′ 未修复状态打开）。
        let mut conn = self.pool.acquire().await?;
        SqliteControlTxn::new(&mut conn)
            .site_pair_upsert(&site_peer_row(p))
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// S7.4 B′：替换站点后、repair 完成前拒绝向该对端服务 sync。
    pub async fn set_site_reconcile_required(&self, peer_id: &str, required: bool) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        SqliteControlTxn::new(&mut conn)
            .site_set_reconcile_required(peer_id, required)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    pub async fn insert_site(&self, p: &SitePeer) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        SqliteControlTxn::new(&mut tx)
            .site_insert(&site_peer_row(p), now())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 删除 peer：同事务丢弃其 site 出站行与游标（对象复制不再尝试该目的地）。
    /// 删除 peer：同事务丢弃其 site 出站行与游标；返回被删掉的**非空版本**集合
    /// （调用方据此结算版本终态）。`DELETE ... RETURNING` 作为事务第一条写语句，
    /// 与解配并发入队的行之间不存在 TOCTOU。
    pub async fn delete_site(
        &self,
        peer_id: &str,
    ) -> Result<Option<Vec<(String, String, String)>>> {
        let mut tx = self.pool.begin().await?;
        let remote = SqliteControlTxn::new(&mut tx)
            .site_remote_id(peer_id)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        if remote.is_none() {
            return Ok(None);
        }
        let affected = SqliteControlTxn::new(&mut tx)
            .repl_drop_site(peer_id)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        SqliteControlTxn::new(&mut tx)
            .site_cursor_delete(peer_id)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        // 解配时清掉该 peer 的控制面死信（否则重配对后 status 的 dead 计数失真）。
        if let Some(remote) = &remote {
            sqlx::query("DELETE FROM site_apply_dead WHERE peer_id = ?")
                .bind(remote)
                .execute(&mut *tx)
                .await?;
        }
        let deleted = SqliteControlTxn::new(&mut tx)
            .site_delete(peer_id)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok((deleted == 1).then_some(affected))
    }

    pub async fn update_site_ping(
        &self,
        peer_id: &str,
        ts: i64,
        error: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        SqliteControlTxn::new(&mut conn)
            .site_update_ping(peer_id, ts, error)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// B4：启动装配注入对象控制面（S7：每个 `Db` 只能挂一次）。
    pub fn attach_object_control(
        &self,
        object: std::sync::Arc<crate::control_backend::ObjectControl>,
    ) -> Result<()> {
        self.object.set(object).map_err(|_| {
            sqlx::Error::Protocol("对象控制面后端被重复注入（每个 Db 只能挂一次）".into())
        })
    }

    /// 已选后端（SQLite 默认；对象后端由启动路径注入）。
    pub fn control_backend(&self) -> crate::config::ControlBackend {
        if self.object.get().is_some() {
            crate::config::ControlBackend::Objects
        } else {
            crate::config::ControlBackend::Sqlite
        }
    }

    /// 对象后端句柄（未选对象后端 = None；测试/诊断用）。
    pub fn object_control(&self) -> Option<&std::sync::Arc<crate::control_backend::ObjectControl>> {
        self.object.get()
    }

    /// 控制面读句柄：对象后端直接返回已装配句柄；否则共享本 `Db` 的 pool
    /// （只克隆池句柄，不新建连接池）。
    pub fn control_store(&self) -> std::sync::Arc<dyn xl_store::control_store::ControlStore> {
        if let Some(object) = self.object.get() {
            return object.control.clone();
        }
        std::sync::Arc::new(crate::control_store::SqliteControlStore::new(
            self.pool.clone(),
        ))
    }

    /// B4 写侧派发：在一个事务里对选中后端运行闭包。
    ///
    /// - SQLite（默认）：`BEGIN` → `SqliteControlTxn` → 成功 `COMMIT` / 失败
    ///   `ROLLBACK`；既有语义逐字节不变。
    /// - 对象：`ObjectControlStore::transaction`（根 CAS 冲突时**整段闭包重跑**），
    ///   origin 为 `Local`。D3 的 local_store 写入立即生效，绝不能放进这个闭包。
    pub async fn control_write<T, F>(&self, actor: &str, mut f: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'a> FnMut(
            &'a mut dyn ControlTxn,
        ) -> futures::future::BoxFuture<
            'a,
            std::result::Result<T, xl_store::control_store::ControlError>,
        >,
    {
        if let Some(object) = self.object.get() {
            return object
                .control
                .transaction(crate::control_store_obj::WriteOrigin::local(actor), |txn| {
                    let fut = f(txn);
                    Box::pin(async move { fut.await.map_err(crate::control_backend::store_error) })
                })
                .await
                .map_err(|e| sqlx::Error::Protocol(format!("对象控制面事务失败：{e}")));
        }
        let mut tx = self.pool.begin().await?;
        let out = {
            let mut txn = SqliteControlTxn::new(&mut tx);
            f(&mut txn).await
        };
        match out {
            Ok(value) => {
                tx.commit().await?;
                Ok(value)
            }
            Err(e) => {
                tx.rollback().await?;
                Err(crate::control_store::to_sqlx(e))
            }
        }
    }

    /// 自然键属主 → 本节点数字 id（console JSON 边界用）：`Admin` → 首个管理员；
    /// `User(name)` → `users.id`；缺失（含无管理员）→ 0。
    pub async fn owner_uid(&self, owner: &OwnerRef) -> Result<i64> {
        let store = self.control_store();
        let id = match owner {
            OwnerRef::Admin => store
                .admin_id()
                .await
                .map_err(crate::control_store::to_sqlx)?,
            OwnerRef::User(name) => store
                .user(name)
                .await
                .map_err(crate::control_store::to_sqlx)?
                .map(|u| u.uid),
        };
        Ok(id.unwrap_or(0))
    }

    /// 本地数字 id → 自然键属主（写门面用）：0 → `Admin`；用户存在 → `User`；
    /// 其余 → `RowNotFound`（uid 必须存在，不静默降级）。
    pub async fn owner_ref(&self, owner_id: i64) -> std::result::Result<OwnerRef, sqlx::Error> {
        if owner_id == 0 {
            return Ok(OwnerRef::Admin);
        }
        self.control_store()
            .user_by_id(owner_id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|u| OwnerRef::User(u.username))
            .ok_or(sqlx::Error::RowNotFound)
    }

    pub async fn create_user(&self, username: &str, password: &str, is_admin: bool) -> Result<i64> {
        let hash = crate::db::hash_password(password);
        // 旧形状：三条独立自动提交语句（失败可留部分行，不做跨语句回滚）。
        let mut conn = self.pool.acquire().await?;
        let id = crate::control_store::SqliteControlTxn::new(&mut conn)
            .user_create_default_status(username, &hash, is_admin, now())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        // AC-0：非管理员新用户加入其 uid 派生账户（与回填同语义；之后可被显式迁入其它账户）。
        // 全局管理员归系统账户、不是任何账户的成员（AC-2）：不建账户行/成员行。
        if !is_admin && let Some(acct) = xl_s3::access::account_id_of(id) {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut conn);
            txn.account_insert_derived_if_absent(&acct, username, id, now(), now())
                .await
                .map_err(crate::control_store::to_sqlx)?;
            txn.account_member_insert_if_absent(&acct, id, "owner", now())
                .await
                .map_err(crate::control_store::to_sqlx)?;
        }
        Ok(id)
    }

    pub async fn user_exists(&self, username: &str) -> Result<bool> {
        self.control_store()
            .user_exists(username)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    pub async fn verify_user(&self, username: &str, password: &str) -> Result<Option<i64>> {
        // M4 切片 7：Argon2id 恒时校验（password-hash 的 verify_password 内部恒时）；
        // 旧 M1 格式（sha256 单轮）仍可验证——**登录成功即透明重写升级**（一次迁移）。
        // status='active'：被停用的账号登录直接按「用户名或密码错误」处理（不问不答）。
        // AC-1：账户停用同样按凭据错误处理（成员行存在但账户行缺失 → 停用，fail-closed）。
        let Some(r) = self
            .control_store()
            .login_user(username)
            .await
            .map_err(crate::control_store::to_sqlx)?
        else {
            // 用户存在性 oracle 收口（verifier 切片 7 观察 A）：Argon2 让「未命中早退」
            // 与「命中校验」的时延差从 µs 放大到几十 ms、网络可测——未命中路径先对一个
            // 固定 dummy 串跑一次同参数 verify 再返回，两侧时延拉平。
            // 与真实校验共用同一并发闸（C1：同价成本、同一上限）。
            let password = password.to_owned();
            crate::db::argon2_offload(move || crate::db::dummy_verify(&password)).await?;
            return Ok(None);
        };
        let stored = r.password_hash;
        let id = r.uid;
        let legacy = crate::db::is_legacy_hash(&stored);

        // Argon2 移出 tokio worker；行查询已在闸外完成，许可在返回前释放（不跨后续 DB I/O）。
        let password_for_verify = password.to_owned();
        if !crate::db::argon2_offload(move || {
            crate::db::verify_password(&password_for_verify, &stored)
        })
        .await?
        {
            return Ok(None);
        }
        if legacy && !self.raft_mode() {
            // 透明升级（失败只 warn：本次登录已成功，下次再来）。
            // Raft 模式：随机盐会让各站点写出的 hash 不同 → 禁止本地写，只验通过。
            // 哈希同样走阻塞池 + 并发闸（旧哈希账户登录频率低，但不应占 tokio worker）。
            let password_for_hash = password.to_owned();
            let upgraded = match argon2_offload(move || hash_password(&password_for_hash)).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(error = %e, user = %username, "旧口令哈希升级计算失败");
                    return Ok(Some(id));
                }
            };
            let upgrade = async {
                let mut conn = self.pool.acquire().await?;
                crate::control_store::SqliteControlTxn::new(&mut conn)
                    .user_set_password(id, &upgraded)
                    .await
                    .map_err(crate::control_store::to_sqlx)
            }
            .await;
            if let Err(e) = upgrade {
                tracing::warn!(error = %e, user = %username, "旧口令哈希透明升级失败");
            }
        }
        Ok(Some(id))
    }

    pub async fn create_access_key(&self, ak: &str, sk: &str, user_id: i64) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        crate::control_store::SqliteControlTxn::new(&mut conn)
            .access_key_create_legacy(ak, sk, user_id, now())
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    // SigV4 的 AK → SK 查询归 xl-s3/src/auth.rs（SigV4Auth）；这里不再保留第二份，防 M5 双改。

    pub async fn list_access_keys(&self, user_id: i64) -> Result<Vec<String>> {
        // M5 切片 5：控制台只列长时键（会话行由 STS 签发、海量短寿，不进「访问密钥」页）
        self.control_store()
            .access_keys_for_user(user_id)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn insert_bucket(&self, name: &str, owner_id: Option<i64>) -> Result<()> {
        self.insert_bucket_set(name, owner_id, 0).await
    }

    /// M4 切片 8：带 set 的建桶行（单 set 缺省 0 走上面）。
    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn insert_bucket_set(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
    ) -> Result<()> {
        self.insert_bucket_full(name, owner_id, set, "async").await
    }

    /// M9 全列建桶行（INSERT OR IGNORE；已存在的权威行不重置）。`set_id` 是本地路由列。
    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn insert_bucket_full(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
        replication_mode: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO buckets \
             (name, owner_id, creation_date, versioning, set_id, object_lock, \
              object_lock_default_mode, object_lock_default_days, default_encryption, \
              replication_mode, deleting) \
             VALUES (?, ?, ?, 'Off', ?, 0, NULL, NULL, NULL, ?, 0)",
        )
        .bind(name)
        .bind(owner_id)
        .bind(now())
        .bind(set as i64)
        .bind(replication_mode)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// M5 切片 0：建桶行**覆盖**语义（console CreateBucket 用）——删桶记账容忍失败留下的
    /// 陈旧行（旧 owner/旧 set）必须在重建时收敛，否则本节点按旧 set 路由、按旧 owner 判权，
    /// 与已收广播的 peer 静默分叉（verifier 阻断 1）。
    pub async fn upsert_bucket_owner_set(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
    ) -> Result<BucketRowOutcome> {
        self.upsert_bucket_owner_set_mode(name, owner_id, set, "async")
            .await
    }

    /// M9：同 [`Self::upsert_bucket_owner_set`]，并写 `replication_mode`。
    /// ON CONFLICT 全列重置（set 随重建收敛，M5 阻断 1）；creation_date 保持首建。
    /// **新建行**在 `BEGIN IMMEDIATE` 写锁内 count+insert——并发建桶只有一个能越过账户
    /// 桶数配额；已存在行是重建收敛（桶数不变），不做配额判定。
    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn upsert_bucket_owner_set_mode(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
        replication_mode: &str,
    ) -> Result<BucketRowOutcome> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM buckets WHERE name = ?")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
        if exists.is_none() && Self::bucket_quota_reached_conn(&mut tx, owner_id).await? {
            return Ok(BucketRowOutcome::QuotaExceeded);
        }
        sqlx::query(
            "INSERT INTO buckets \
             (name, owner_id, creation_date, versioning, set_id, object_lock, \
              object_lock_default_mode, object_lock_default_days, default_encryption, \
              replication_mode, deleting) \
             VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, NULL, ?, 0) \
             ON CONFLICT(name) DO UPDATE SET \
              owner_id = excluded.owner_id, set_id = excluded.set_id, \
              versioning = excluded.versioning, \
              object_lock = excluded.object_lock, \
              object_lock_default_mode = excluded.object_lock_default_mode, \
              object_lock_default_days = excluded.object_lock_default_days, \
              default_encryption = excluded.default_encryption, \
              replication_mode = excluded.replication_mode, deleting = excluded.deleting",
        )
        .bind(name)
        .bind(owner_id)
        .bind(now())
        // M9：sync 桶建桶即 Enabled（双站同 id 依赖版本链）；async 保持 Off。
        .bind(if replication_mode == "sync" {
            "Enabled"
        } else {
            "Off"
        })
        .bind(set as i64)
        .bind(replication_mode)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(BucketRowOutcome::Written)
    }

    /// 桶的复制模式；行不存在 = "async"（读取路径与缺行旧桶同口径）。
    pub async fn bucket_replication_mode(&self, name: &str) -> Result<String> {
        Ok(self
            .control_store()
            .bucket(name)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|v| v.replication_mode)
            .unwrap_or_else(|| "async".into()))
    }

    /// M9：sync 写门所需桶状态 `(replication_mode, deleting, versioning)`；
    /// 行不存在 = ("async", false, "Off")（与缺行旧桶同口径）。
    pub async fn bucket_sync_state(&self, name: &str) -> Result<(String, bool, String)> {
        Ok(self
            .bucket_sync_state_row(name)
            .await?
            .unwrap_or_else(|| ("async".into(), false, "Off".into())))
    }

    /// M9 S3：同 [`Self::bucket_sync_state`]，但**行不存在返回 `None`**——删桶恢复据此
    /// 区分「陈旧 intent」与「同名桶仍存在」，避免误删重建的新桶。
    pub async fn bucket_sync_state_row(
        &self,
        name: &str,
    ) -> Result<Option<(String, bool, String)>> {
        Ok(self
            .control_store()
            .bucket(name)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|v| (v.replication_mode, v.deleting, v.versioning)))
    }

    /// M9 S3：删桶 quiesce 标志（S6 起改由 Raft `SetBucketDeleting` 命令承载）。
    pub async fn set_bucket_deleting(&self, name: &str, deleting: bool) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .bucket_set_deleting(name, deleting)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 全量桶名 → 模式（console 列表一次查完，避免逐桶查询）。
    pub async fn bucket_replication_modes(
        &self,
    ) -> Result<std::collections::HashMap<String, String>> {
        Ok(self
            .control_store()
            .buckets()
            .await
            .map_err(crate::control_store::to_sqlx)?
            .into_iter()
            .map(|v| (v.name, v.replication_mode))
            .collect())
    }

    /// P3-3：回填对账——把**权威**解析到的 set 覆盖到已有行（仅 set_id 列；owner/versioning
    /// 不动）。场景：桶删除后在别节点重建为另一 set，本节点陈旧行（INSERT OR IGNORE 不会更新）
    /// 会在下次启动被此处收敛；行不存在则插入（owner NULL，与历史桶回填同权）。
    /// M5 切片 0：权威值须来自 `resolve_bucket_set_authoritative`（数据面本地先行解析对本地
    /// 陈旧行会把旧值原样写回，收敛不成立——verifier 高 2）。
    // shortcut: 启动回填/set 指派/配额 BEGIN IMMEDIATE 属本地路由与 Slice 5 账户配额职责，最终 set 路由迁移时改走 seam。
    pub async fn upsert_bucket_set(&self, name: &str, set: u16) -> Result<()> {
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, set_id) VALUES (?, NULL, ?, ?) \
             ON CONFLICT(name) DO UPDATE SET set_id = excluded.set_id",
        )
        .bind(name)
        .bind(now())
        .bind(set as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_bucket_rows(&self) -> Result<Vec<String>> {
        Ok(self
            .control_store()
            .buckets()
            .await
            .map_err(crate::control_store::to_sqlx)?
            .into_iter()
            .map(|v| v.name)
            .collect())
    }

    pub async fn instant_get(&self, k: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT v FROM instant_index WHERE k = ? AND expires_at > ?")
            .bind(k)
            .bind(now())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("v")))
    }

    pub async fn instant_set(&self, k: &str, v: &str, ttl_seconds: u64) -> Result<()> {
        // ttl = 0 视为「立刻过期」，与 aetherupload 的 SETEX 语义一致
        let expires = now() + ttl_seconds as i64;
        sqlx::query(
            "INSERT INTO instant_index (k, v, expires_at) VALUES (?, ?, ?) \
                     ON CONFLICT(k) DO UPDATE SET v = excluded.v, expires_at = excluded.expires_at",
        )
        .bind(k)
        .bind(v)
        .bind(expires)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn instant_del(&self, k: &str) -> Result<()> {
        sqlx::query("DELETE FROM instant_index WHERE k = ?")
            .bind(k)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 按前缀清空秒传索引（`InstantIndex::delete_all` 的落点）。
    /// 前缀只来自 aetherupload 的 `KEY_PREFIX`（`aetherupload:resource:`，无 LIKE 通配符），
    /// 故直接 LIKE 匹配，不做转义。
    pub async fn instant_delete_prefix(&self, prefix: &str) -> Result<()> {
        sqlx::query("DELETE FROM instant_index WHERE k LIKE ? || '%'")
            .bind(prefix)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 清过期秒传索引行（有界批；过期行不影响命中，但会无限增长）。
    pub async fn instant_purge_expired(&self, limit: i64) -> Result<u64> {
        let n = sqlx::query(
            "DELETE FROM instant_index WHERE k IN \
             (SELECT k FROM instant_index WHERE expires_at <= ? ORDER BY expires_at LIMIT ?)",
        )
        .bind(now())
        .bind(limit.max(1))
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n)
    }

    pub async fn record_upload(
        &self,
        saved_path: &str,
        bucket: &str,
        key: &str,
        group: &str,
        size: i64,
        etag: &str,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        SqliteControlTxn::new(&mut conn)
            .upload_record(&NewUpload {
                saved_path: saved_path.to_string(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                group_name: group.to_string(),
                size,
                etag: etag.to_string(),
                created_at: now(),
            })
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(())
    }

    /// 登记上传的 `original_name` 与归属（规格 §5.4）。
    ///
    /// 归属防挂名：非管理员只能认领**无归属**的记录（`owner_id IS NULL`）或自己的
    /// （原实现无条件覆盖 owner 的洞在此收口）。返回是否命中（0 行 = 记录不存在 → 404）。
    pub async fn register_upload(
        &self,
        saved_path: &str,
        original_name: &str,
        owner_id: i64,
        is_admin: bool,
    ) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        SqliteControlTxn::new(&mut conn)
            .upload_register(saved_path, original_name, owner_id, is_admin)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 记录对应的桶（登记前判权用；记录不存在 → None）。
    pub async fn upload_bucket(&self, saved_path: &str) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .upload(saved_path)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|r| r.bucket))
    }

    /// 桶版本控制状态（M4 切片 3；aether 上传面按此传 versioned；无行 → "Off"）。
    pub async fn get_bucket_versioning(&self, bucket: &str) -> Result<String> {
        Ok(self
            .control_store()
            .bucket(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|v| v.versioning)
            .unwrap_or_else(|| "Off".to_string()))
    }

    /// M8 缺口 C：桶默认加密（"AES256"/"aws:kms"；None = 未配置）。
    pub async fn get_bucket_default_encryption(&self, bucket: &str) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .bucket(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .and_then(|v| v.default_encryption))
    }

    /// M8 缺口 C：写桶默认加密（桶不存在 → 0 行）。
    pub async fn set_bucket_default_encryption(
        &self,
        bucket: &str,
        algorithm: &str,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let changed = crate::control_store::SqliteControlTxn::new(&mut tx)
            .bucket_set_default_encryption(bucket, algorithm)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(changed)
    }

    /// M8 缺口 C：清桶默认加密（幂等）。
    pub async fn clear_bucket_default_encryption(&self, bucket: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let changed = crate::control_store::SqliteControlTxn::new(&mut tx)
            .bucket_clear_default_encryption(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(changed)
    }

    /// 全部桶默认加密记录（桶名 + 库内存储值），按桶名排序；CMK 引用守卫用。
    pub async fn list_bucket_default_encryptions(&self) -> Result<Vec<(String, String)>> {
        Ok(self
            .control_store()
            .buckets()
            .await
            .map_err(crate::control_store::to_sqlx)?
            .into_iter()
            .filter_map(|v| v.default_encryption.map(|e| (v.name, e)))
            .collect())
    }

    /// 某用户拥有的桶（控制台桶列表能力位计算用）。
    pub async fn owned_buckets(&self, uid: i64) -> Result<Vec<String>> {
        Ok(self
            .control_store()
            .buckets()
            .await
            .map_err(crate::control_store::to_sqlx)?
            .into_iter()
            .filter(|v| v.owner_uid == Some(uid))
            .map(|v| v.name)
            .collect())
    }

    pub async fn list_uploads(&self, limit: i64) -> Result<Vec<UploadRecord>> {
        let rows = self
            .control_store()
            .uploads(limit)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(rows.into_iter().map(upload_record).collect())
    }

    // ---- 子账户与权限（M-权限体系）----

    /// 取用户（`/me`、admin 判定、停用拦截用）：(username, is_admin, is_active)。
    pub async fn get_user(&self, uid: i64) -> Result<Option<(String, bool, bool)>> {
        Ok(self
            .control_store()
            .user_by_id(uid)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|u| (u.username, u.is_admin, u.status == "active")))
    }

    /// 启停用用户。返回是否生效（false = 用户不存在，或停用会留下 0 个在册管理员）。
    ///
    /// **防呆是原子条件**（评审 P2：先读 count 再 UPDATE 的 check-then-act 在并发互停
    /// 两个管理员时会双双放行 → 0 在册管理员锁死）。停用走单条条件 UPDATE：
    /// 非管理员恒可停；管理员仅在「当前在册管理员 > 1」时可停——子查询与写入同语句，
    /// SQLite 写锁串行化下天然防 TOCTOU。启用无锁死风险，走普通 UPDATE。
    pub async fn set_user_status(&self, uid: i64, active: bool) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        crate::control_store::SqliteControlTxn::new(&mut conn)
            .user_set_status(uid, active)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 在册（active）管理员数：保护「不能停用最后一个可用管理员」。
    pub async fn count_active_admins(&self) -> Result<i64> {
        Ok(self
            .control_store()
            .admin_counts()
            .await
            .map_err(crate::control_store::to_sqlx)?
            .0)
    }

    /// 全部用户 + 各自权限行（子账户管理页；数量级 = 人手，两步查询足够）。
    pub async fn list_users(&self) -> Result<Vec<UserRow>> {
        let users = self
            .control_store()
            .users()
            .await
            .map_err(crate::control_store::to_sqlx)?;
        let mut out = Vec::with_capacity(users.len());
        for u in users {
            out.push(UserRow {
                id: u.uid,
                username: u.username,
                is_admin: u.is_admin,
                status: u.status,
                created_at: u.created_at,
                permissions: self.get_permissions(u.uid).await?,
            });
        }
        Ok(out)
    }

    /// 管理员数量（保护「最后一个管理员」不被删）。
    pub async fn count_admins(&self) -> Result<i64> {
        Ok(self
            .control_store()
            .admin_counts()
            .await
            .map_err(crate::control_store::to_sqlx)?
            .1)
    }

    /// 改口令。返回是否有该用户。
    pub async fn set_password(&self, uid: i64, password: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        crate::control_store::SqliteControlTxn::new(&mut conn)
            .user_set_password(uid, &crate::db::hash_password(password))
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 删除用户（一个事务内处理全部外键：sqlx 默认 FK ON，裸删 users 会报错）：
    /// 级联删 access_keys 与 permissions；buckets/uploads 的 owner 置 NULL（记录保留作审计）。
    /// 返回是否删除（false = 用户不存在，或会留下 0 个在册管理员）。
    ///
    /// **防呆原子化**（评审 P2，同 set_user_status）：事务第一条是无副作用的守卫写
    /// （`UPDATE users SET id=id WHERE …`），既取写锁串行化并发删除/停用，又在同一语句里
    /// 完成判定——通过才继续级联；不通过直接返回 false，一个字节都不改。
    pub async fn delete_user(&self, uid: i64) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let gate = crate::control_store::SqliteControlTxn::new(&mut tx)
            .user_delete_guard(uid)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        if !gate {
            return Ok(false);
        }
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            txn.user_delete_access_keys_permissions(uid)
                .await
                .map_err(crate::control_store::to_sqlx)?;
        }
        // shortcut: buckets/uploads 属其它域，所有权清空仍由本域在级联中间执行（D7）。
        sqlx::query("UPDATE buckets SET owner_id = NULL WHERE owner_id = ?")
            .bind(uid)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE uploads SET owner_id = NULL WHERE owner_id = ?")
            .bind(uid)
            .execute(&mut *tx)
            .await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .user_delete(uid)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(true)
    }

    // ---- AC-0：多租户账户（standalone 直写；Raft 经 ControlCommand apply）----

    /// 启动回填：每个存量用户 → 其 uid 派生账户（role=owner），并保证系统账户存在。
    /// 只跑一次（`schema_meta.accounts_backfill_v1`）：显式迁出派生账户的用户重启后不被拉回。
    /// 返回是否实际执行（false = marker 已存在）。
    pub async fn backfill_accounts(&self) -> Result<bool> {
        if self.schema_meta("accounts_backfill_v1").await?.is_some() {
            return Ok(false);
        }
        let users = self
            .control_store()
            .users()
            .await
            .map_err(crate::control_store::to_sqlx)?;
        let ts = now();
        let mut tx = self.pool.begin().await?;
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            // 系统账户：admin/NULL 桶的账户；没有成员，主体匹配永不命中它。
            txn.account_system_insert_if_absent(xl_s3::access::LEGACY_ACCOUNT, "system", ts, ts)
                .await
                .map_err(crate::control_store::to_sqlx)?;
            for u in users {
                let uid = u.uid;
                let Some(acct) = xl_s3::access::account_id_of(uid) else {
                    continue; // uid 0/越界：旧语义下也无法匹配任何账户 ARN，保持原状
                };
                txn.account_insert_derived_if_absent(&acct, &u.username, uid, ts, ts)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
                // 首次回填时不存在成员行；UNIQUE(uid) 兜底并发/重入。
                txn.account_member_insert_if_absent(&acct, uid, "owner", ts)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
            }
        }
        sqlx::query(
            "INSERT INTO schema_meta (key, value) VALUES ('accounts_backfill_v1', 'done') \
             ON CONFLICT(key) DO UPDATE SET value = 'done'",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// 账户表是否已规范化（v2 回填/level-3 canonicalize 完成）。
    pub async fn accounts_canonical(&self) -> Result<bool> {
        Ok(self.schema_meta(META_ACCOUNTS_CANONICAL).await?.is_some())
    }

    /// 回填 v2（standalone 升级修正）：v1 把全局管理员也挂进了 uid 派生账户，
    /// 这里把管理员移出成员表并删除其无成员的派生账户，然后写 canonical marker。
    /// 单事务、marker 幂等；只跑一次（false = marker 已存在）。
    pub async fn backfill_accounts_v2(&self) -> Result<bool> {
        if self.accounts_canonical().await? {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            txn.account_members_delete_admins()
                .await
                .map_err(crate::control_store::to_sqlx)?;
            // 只删「某管理员的 uid 派生 ID 且已无成员」的账户；管理员桶归系统账户，
            // 不引用这些账户，删除不影响桶归属。
            txn.accounts_delete_admin_derived()
                .await
                .map_err(crate::control_store::to_sqlx)?;
        }
        sqlx::query(
            "INSERT INTO schema_meta (key, value) VALUES (?, 'done') \
             ON CONFLICT(key) DO UPDATE SET value = 'done'",
        )
        .bind(META_ACCOUNTS_CANONICAL)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// 缺少账户成员行的非管理员用户（legacy 派生账户路径；worker 的修复对象）。
    pub async fn legacy_users_missing_accounts(&self) -> Result<Vec<(i64, String)>> {
        self.control_store()
            .legacy_users_missing_accounts()
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// standalone 幂等规范化（serve worker 与启动路径共用）：
    /// 1. 全局管理员移出成员表、删除其无成员派生账户（同 backfill v2）；
    /// 2. 为缺失成员行的非管理员补 uid 派生账户 + owner 成员（ARN 不漂移）；
    /// 3. 无 `accounts_canonical_v1` 时写 marker（仅本事务成功后）。
    ///
    /// 单事务：任一用户失败全部回滚，重启/重跑安全；返回本轮新增行数。
    /// Raft 模式不得调用（账户表经快照复制，由 leader 用既有 AccountCreate/
    /// AccountMemberAttach 命令修复）。
    pub async fn canonicalize_legacy_accounts_once(&self) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let ts = now();
        let mut changed = 0usize;
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            txn.account_members_delete_admins()
                .await
                .map_err(crate::control_store::to_sqlx)?;
            txn.accounts_delete_admin_derived()
                .await
                .map_err(crate::control_store::to_sqlx)?;
            let users = txn
                .legacy_users_missing_accounts()
                .await
                .map_err(crate::control_store::to_sqlx)?;
            for (uid, username) in users {
                let Some(acct) = xl_s3::access::account_id_of(uid) else {
                    continue; // uid 0/越界：旧语义下也无法匹配任何账户 ARN
                };
                changed += txn
                    .account_insert_derived_if_absent(&acct, &username, uid, ts, ts)
                    .await
                    .map_err(crate::control_store::to_sqlx)? as usize;
                changed += txn
                    .account_member_insert_if_absent(&acct, uid, "owner", ts)
                    .await
                    .map_err(crate::control_store::to_sqlx)? as usize;
            }
        }
        let marker: Option<String> =
            sqlx::query_scalar("SELECT value FROM schema_meta WHERE key = ?")
                .bind(META_ACCOUNTS_CANONICAL)
                .fetch_optional(&mut *tx)
                .await?;
        if marker.is_none() {
            sqlx::query(
                "INSERT INTO schema_meta (key, value) VALUES (?, 'done') \
                 ON CONFLICT(key) DO UPDATE SET value = 'done'",
            )
            .bind(META_ACCOUNTS_CANONICAL)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(changed)
    }

    /// standalone 设置硬配额（Raft 下无复制命令，路由拒绝；见 accounts_api）。
    /// 返回 false = 账户不存在。
    pub async fn set_account_hard_limits(
        &self,
        id: &str,
        hard_byte_quota: Option<i64>,
        object_quota: Option<i64>,
    ) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        crate::control_store::SqliteControlTxn::new(&mut conn)
            .account_set_hard_limits(id, hard_byte_quota, object_quota, now())
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 用户当前账户：成员行优先，无行回落 uid 派生账户（legacy 兼容路径）。
    pub async fn account_id_of_user(&self, uid: i64) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .account_member_account(uid)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .or_else(|| xl_s3::access::account_id_of(uid)))
    }

    /// 账户状态是否允许该用户继续使用（无成员行 = 派生账户语义 = 有效）。
    /// 成员行存在但账户行缺失 → false（fail-closed）。
    pub async fn account_active(&self, uid: i64) -> Result<bool> {
        self.control_store()
            .account_active(uid)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    fn account_row_from_view(a: xl_store::control_store::account::AccountView) -> AccountRow {
        AccountRow {
            id: a.id,
            name: a.name,
            status: a.status,
            admin_uid: a.admin_uid,
            bucket_quota: a.bucket_quota,
            soft_byte_quota: a.soft_byte_quota,
            hard_byte_quota: a.hard_byte_quota,
            object_quota: a.object_quota,
            created_at: a.created_at,
            updated_at: a.updated_at,
            member_count: a.member_count,
            bucket_count: a.bucket_count,
        }
    }

    /// 账户名下桶名（软配额用量端点逐桶列举；口径见适配器 `account_buckets_from`）。
    pub async fn account_bucket_names(&self, account_id: &str) -> Result<Vec<String>> {
        self.control_store()
            .account_bucket_names(account_id)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 桶数配额判定（调用方已持写锁，或 leader 已确认本地 applied 追平）：owner 的
    /// **当前**账户桶数 >= `accounts.bucket_quota` 时返回 true。口径同
    /// `account_buckets_from`（成员行优先，无行 = uid 派生账户）；NULL/负配额、
    /// 无账户行 = 不限；停用账户 fail-closed（与 S3 层 `bucket_quota_exceeded` 同判）。
    async fn bucket_quota_reached_conn(
        conn: &mut SqliteConnection,
        owner_id: Option<i64>,
    ) -> Result<bool> {
        crate::control_store::SqliteControlTxn::new(&mut *conn)
            .bucket_quota_reached(owner_id)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 建桶配额最终判定（仅新建行；已存在行是收敛，不增加桶数）。
    /// `conn` 由调用方保证已持写锁或本地 applied 已追平。
    pub(crate) async fn bucket_create_quota_reached_conn(
        conn: &mut SqliteConnection,
        name: &str,
        owner: Option<&str>,
    ) -> Result<bool> {
        if crate::control_store::SqliteControlTxn::new(&mut *conn)
            .bucket_exists(name)
            .await
            .map_err(crate::control_store::to_sqlx)?
        {
            return Ok(false);
        }
        let Some(owner) = owner else {
            return Ok(false);
        };
        let uid = crate::control_store::SqliteControlTxn::new(&mut *conn)
            .user_uid(owner)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        let Some(uid) = uid else {
            return Ok(false);
        };
        Self::bucket_quota_reached_conn(conn, Some(uid)).await
    }

    /// [`Self::bucket_create_quota_reached_conn`] 的连接池版本（leader append 前判定）。
    pub(crate) async fn bucket_create_quota_reached(
        pool: &SqlitePool,
        name: &str,
        owner: Option<&str>,
    ) -> Result<bool> {
        let mut conn = pool.acquire().await?;
        Self::bucket_create_quota_reached_conn(&mut conn, name, owner).await
    }

    /// 全部账户（管理员视角）或某用户的账户（账户成员视角）。
    pub async fn list_accounts(&self, member_uid: Option<i64>) -> Result<Vec<AccountRow>> {
        Ok(self
            .control_store()
            .accounts(member_uid)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .into_iter()
            .map(Self::account_row_from_view)
            .collect())
    }

    pub async fn get_account(&self, id: &str) -> Result<Option<AccountRow>> {
        Ok(self
            .control_store()
            .account(id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(Self::account_row_from_view))
    }

    /// 新建账户。id 为 12 位随机数（与派生账户空间重叠概率可忽略，冲突重试）。
    pub async fn create_account(&self, name: &str, bucket_quota: Option<i64>) -> Result<String> {
        let ts = now();
        let mut conn = self.pool.acquire().await?;
        for _ in 0..8 {
            // random() 是有符号 64 位：先掩掉符号位再取模，否则 % 保留负号会生成负 id。
            let id: String = sqlx::query_scalar(
                "SELECT printf('%012d', 100000000000 + ((random() & 0x7fffffffffffffff) % 900000000000))",
            )
            .fetch_one(&mut *conn)
            .await?;
            let inserted = crate::control_store::SqliteControlTxn::new(&mut conn)
                .account_insert_if_absent(&id, name, bucket_quota, ts, ts)
                .await
                .map_err(crate::control_store::to_sqlx)?;
            if inserted {
                return Ok(id);
            }
        }
        Err(sqlx::Error::Protocol(
            "account id generation collision (retry)".into(),
        ))
    }

    pub async fn rename_account(&self, id: &str, name: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        crate::control_store::SqliteControlTxn::new(&mut conn)
            .account_set_name(id, name, now())
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 启停账户。停用是原子条件更新：账户里还有在册全局管理员时拒绝
    /// （否则该管理员会随账户一起被锁死，且可能锁死最后一个管理员）。
    pub async fn set_account_status(&self, id: &str, status: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        crate::control_store::SqliteControlTxn::new(&mut conn)
            .account_set_status(id, status, now())
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    pub async fn set_account_quota(&self, id: &str, quota: Option<i64>) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        crate::control_store::SqliteControlTxn::new(&mut conn)
            .account_set_bucket_quota(id, quota, now())
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 删除账户：要求无成员且无桶（LEGACY 系统账户永不可删）。
    /// 返回 Ok(true)；账户不存在 / 非空不删（由 handler 区分）。
    pub async fn delete_account(&self, id: &str) -> Result<bool> {
        if id == xl_s3::access::LEGACY_ACCOUNT {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        let deleted = {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            let members = txn
                .account_member_count(id)
                .await
                .map_err(crate::control_store::to_sqlx)?;
            let buckets = txn
                .account_bucket_count(id)
                .await
                .map_err(crate::control_store::to_sqlx)?;
            if members != 0 || buckets != 0 {
                return Ok(false);
            }
            txn.account_delete(id)
                .await
                .map_err(crate::control_store::to_sqlx)?
        };
        tx.commit().await?;
        Ok(deleted)
    }

    pub async fn list_account_members(&self, id: &str) -> Result<Vec<AccountMemberRow>> {
        Ok(self
            .control_store()
            .account_members(id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .into_iter()
            .map(|m| AccountMemberRow {
                uid: m.uid,
                username: m.username,
                role: m.role,
                is_admin: m.is_admin,
                status: m.status,
                created_at: m.created_at,
            })
            .collect())
    }

    /// 某用户在该账户里的角色（不在账户 → None）。
    pub async fn member_role(&self, account_id: &str, uid: i64) -> Result<Option<String>> {
        self.control_store()
            .member_role(account_id, uid)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 把用户挂到账户（已有成员行则迁入并改角色）。账户/用户不存在 → false；
    /// 全局管理员不是任何账户的成员（AC-2 不变量），恒拒绝 → false。
    /// 账户 admin_uid 为空且角色为 admin 时顺带补上。
    pub async fn attach_account_member(
        &self,
        account_id: &str,
        uid: i64,
        role: &str,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            if !txn
                .user_non_admin_exists(uid)
                .await
                .map_err(crate::control_store::to_sqlx)?
            {
                return Ok(false);
            }
            txn.account_member_upsert(account_id, uid, role, now())
                .await
                .map_err(crate::control_store::to_sqlx)?;
            if role == "admin" {
                txn.account_set_admin_if_unset(account_id, uid)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
            }
        }
        tx.commit().await?;
        Ok(true)
    }

    /// 在账户内建用户（is_admin 恒 0；角色由调用方限定 member）。
    /// 用户名冲突 → 数据库 unique violation（handler 映射 400）。
    pub async fn create_user_in_account(
        &self,
        account_id: &str,
        username: &str,
        password: &str,
        role: &str,
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let ts = now();
        let uid = {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            let uid = txn
                .user_create_default_status(
                    username,
                    &crate::db::hash_password(password),
                    false,
                    ts,
                )
                .await
                .map_err(crate::control_store::to_sqlx)?;
            txn.account_member_insert(account_id, uid, role, ts)
                .await
                .map_err(crate::control_store::to_sqlx)?;
            uid
        };
        tx.commit().await?;
        Ok(uid)
    }

    /// 某用户的权限行（全量）。
    pub async fn get_permissions(&self, uid: i64) -> Result<Vec<PermissionRow>> {
        Ok(self
            .control_store()
            .permissions_for_user(uid)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .into_iter()
            .map(|p| PermissionRow {
                bucket: p.bucket,
                read: p.read,
                write: p.write,
                delete: p.delete,
            })
            .collect())
    }

    /// 全量替换某用户的权限行（管理员的 PUT 语义）。全 0 行跳过不写（无行 = 无权限，等价且省行）。
    pub async fn set_permissions(&self, uid: i64, rows: &[PermissionRow]) -> Result<()> {
        let rows: Vec<IamPermissionRow> = rows
            .iter()
            .filter(|p| p.read || p.write || p.delete)
            .map(|p| IamPermissionRow {
                bucket: p.bucket.clone(),
                read: p.read,
                write: p.write,
                delete: p.delete,
            })
            .collect();
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .permissions_replace(uid, &rows)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 上传归属登记：仅当该记录尚无归属时写入（`AND owner_id IS NULL`）。
    /// 返回是否写入（已归属/记录不存在 → false，调用方 best-effort 忽略）。
    pub async fn set_upload_owner(&self, saved_path: &str, uid: i64) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        SqliteControlTxn::new(&mut conn)
            .upload_claim(saved_path, uid)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 某用户自己的上传记录（子账户视野）。
    pub async fn list_uploads_for(&self, uid: i64, limit: i64) -> Result<Vec<UploadRecord>> {
        let rows = self
            .control_store()
            .uploads_for(uid, limit)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(rows.into_iter().map(upload_record).collect())
    }
}

/// 进程级 Argon2 并发闸（C1 DoS）：单次校验约 19 MiB，最多 8 路并行。
/// 校验一律走阻塞池，未认证登录洪水不再占满 tokio worker。
static ARGON2_PERMITS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(8));

/// 在阻塞池执行一次 Argon2 工作（校验/哑校验），并发受 [`ARGON2_PERMITS`] 限制。
/// JoinError 转成 DB 错误上抛（登录 handler 记日志后回 500，不 panic）。
async fn argon2_offload<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T> {
    let _permit = ARGON2_PERMITS
        .acquire()
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("Argon2 并发闸不可用: {e}")))?;
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("Argon2 阻塞任务失败: {e}")))
}

/// 口令哈希（M4 切片 7 起）：Argon2id（salt 随机、串内自描述 `$argon2id$...`）。
pub fn hash_password(password: &str) -> String {
    use argon2::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
    let salt = SaltString::generate(&mut OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2 哈希（默认参数）不会失败")
        .to_string()
}

/// M1 旧格式（`sha256("oss-m1-salt:"+pw)` hex）——仅供 `verify_password` 的遗留分支使用。
fn legacy_hash(password: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"oss-m1-salt:");
    h.update(password.as_bytes());
    hex::encode(h.finalize())
}

/// 旧格式判定：非 `$argon2` 前缀的 64 位 hex（M1 单轮 sha256）。
pub fn is_legacy_hash(hash: &str) -> bool {
    !hash.starts_with("$argon2") && hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit())
}

/// 未命中路径的时延拉平（观察 A）：与真实校验同参数的 dummy verify。
/// 固定 dummy 串（公钥性质，无秘密）：仅用于消耗与真实路径同量的 CPU/内存。
/// 参数（m=19456,t=2,p=1）= `Argon2::default()`，与真实哈希同强度——测试钉住可解析性。
pub(crate) const DUMMY_ARGON2: &str = "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

pub fn dummy_verify(password: &str) {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    let parsed = PasswordHash::new(DUMMY_ARGON2)
        .expect("DUMMY_ARGON2 必须是合法 PHC 串（测试钉住；解析失败会让 oracle 收口失效）");
    let _ = argon2::Argon2::default().verify_password(password.as_bytes(), &parsed);
}

/// 恒时校验（Argon2 走 password-hash 的 verify；旧格式走定长 hex 比较）。
pub fn verify_password(password: &str, stored: &str) -> bool {
    if stored.starts_with("$argon2") {
        use argon2::password_hash::{PasswordHash, PasswordVerifier};
        match PasswordHash::new(stored) {
            Ok(parsed) => argon2::Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok(),
            Err(_) => false,
        }
    } else {
        let calc = legacy_hash(password);
        // 定长比较（两者恒 64 字符）
        calc.as_bytes()
            .iter()
            .zip(stored.as_bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
            && stored.len() == calc.len()
    }
}

fn site_peer(r: SitePeerRow) -> SitePeer {
    SitePeer {
        peer_id: r.peer_id,
        remote_site_id: r.remote_site_id,
        name: r.name,
        endpoint: r.endpoint,
        region: r.region,
        priority: r.priority,
        s3_access_key: r.s3_access_key,
        s3_secret_key: r.s3_secret_key,
        peer_site_token: r.peer_site_token,
        created_at: r.created_at,
        last_seen: r.last_seen,
        last_error: r.last_error,
        raft_node_id: r.raft_node_id,
        reconcile_required: r.reconcile_required,
    }
}

fn site_peer_row(p: &SitePeer) -> SitePeerRow {
    SitePeerRow {
        peer_id: p.peer_id.clone(),
        remote_site_id: p.remote_site_id.clone(),
        name: p.name.clone(),
        endpoint: p.endpoint.clone(),
        region: p.region.clone(),
        priority: p.priority,
        s3_access_key: p.s3_access_key.clone(),
        s3_secret_key: p.s3_secret_key.clone(),
        peer_site_token: p.peer_site_token.clone(),
        created_at: p.created_at,
        last_seen: p.last_seen,
        last_error: p.last_error.clone(),
        raft_node_id: p.raft_node_id,
        reconcile_required: p.reconcile_required,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn db() -> Db {
        Db::connect("sqlite::memory:").await.unwrap()
    }

    /// 重试退避：base 10*2^attempt 封顶 300，抖动必须在 0..9 秒且确定；不能让
    /// `now`（Unix 秒）的个位直接变成 0..999 秒。
    #[test]
    fn task_retry_backoff_is_bounded_and_deterministic() {
        let mut seen = std::collections::BTreeSet::new();
        for attempt in 0..10i64 {
            let base = 10i64
                .saturating_mul(2i64.saturating_pow(attempt as u32))
                .min(300);
            for i in 0..40i64 {
                let now = 1_700_000_000 + i;
                let b = Db::retry_backoff_secs(attempt, now, 42, 7);
                assert!(b >= base && b <= base + 9, "attempt={attempt} b={b}");
                assert_eq!(b, Db::retry_backoff_secs(attempt, now, 42, 7));
                seen.insert(b);
            }
        }
        assert!(seen.len() > 20, "jitter should spread, got {seen:?}");
        assert!(*seen.iter().max().unwrap() <= 309);
    }

    #[tokio::test]
    async fn init_creates_tables() {
        let db = db().await;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('users','access_keys','buckets','uploads','instant_index','permissions')")
            .fetch_one(db.pool()).await.unwrap();
        assert_eq!(n, 6);
    }

    /// AC-0：回填把每个存量用户映射到 uid 派生账户；二次执行幂等（marker），
    /// 且派生账户 ARN 的黄金样本不变。
    #[tokio::test]
    async fn accounts_backfill_idempotent_and_preserves_derived_arns() {
        let db = db().await;
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let sub = db.create_user("sub", "pw", false).await.unwrap();
        // 抹掉 create_user 的即时挂载与 connect 期 marker，模拟「旧库升级」。
        sqlx::query("DELETE FROM account_members")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM accounts")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM schema_meta WHERE key = 'accounts_backfill_v1'")
            .execute(db.pool())
            .await
            .unwrap();

        assert!(db.backfill_accounts().await.unwrap());
        let n_acc: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let n_mem: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members")
            .fetch_one(db.pool())
            .await
            .unwrap();
        // 两个用户账户 + LEGACY 系统账户；每个用户一条成员行（role=owner）。
        assert_eq!(n_acc, 3);
        assert_eq!(n_mem, 2);

        // 二次执行：marker 命中 → no-op，行数/内容不变。
        assert!(!db.backfill_accounts().await.unwrap());
        let n_acc2: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n_acc2, n_acc);

        for (uid, name) in [(admin, "root"), (sub, "sub")] {
            let derived = xl_s3::access::account_id_of(uid).unwrap();
            assert_eq!(
                db.account_id_of_user(uid).await.unwrap(),
                Some(derived.clone())
            );
            assert_eq!(
                db.member_role(&derived, uid).await.unwrap().as_deref(),
                Some("owner")
            );
            // 迁移前对外暴露的 arn:aws:iam::<id>:root 继续命中（黄金样本）。
            let arn = format!("arn:aws:iam::{derived}:root");
            assert!(
                xl_s3::access::account_arn_matches(&arn, uid),
                "{name} ARN 漂移"
            );
        }
        // 系统账户存在、无成员，且永不匹配真实用户。
        assert!(
            db.get_account(xl_s3::access::LEGACY_ACCOUNT)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            db.member_role(xl_s3::access::LEGACY_ACCOUNT, sub)
                .await
                .unwrap(),
            None
        );
        assert!(!xl_s3::access::account_arn_matches(
            &format!("arn:aws:iam::{}:root", xl_s3::access::LEGACY_ACCOUNT),
            sub
        ));
    }

    /// Task 1：v2 把全局管理员移出 account_members 并删除其无成员的派生账户；
    /// 非管理员的派生账户/成员保持；marker 幂等。
    #[tokio::test]
    async fn backfill_v2_moves_global_admins_to_system_account() {
        let db = db().await;
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        let admin_acct = xl_s3::access::account_id_of(admin).unwrap();
        let alice_acct = xl_s3::access::account_id_of(alice).unwrap();
        // 模拟旧库 v1 状态：v1 也给管理员建派生账户/成员行。
        sqlx::query("DELETE FROM schema_meta WHERE key = 'accounts_backfill_v1' OR key = ?")
            .bind(META_ACCOUNTS_CANONICAL)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(db.backfill_accounts().await.unwrap());
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE uid = ?")
            .bind(admin)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 1, "旧库 v1 给管理员建成员行");

        assert!(db.backfill_accounts_v2().await.unwrap());
        assert!(db.accounts_canonical().await.unwrap());
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE uid = ?")
            .bind(admin)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "全局管理员不得拥有账户成员行");
        assert!(
            db.get_account(&admin_acct).await.unwrap().is_none(),
            "全局管理员的派生账户行应删除"
        );
        // 非管理员不受影响。
        assert_eq!(
            db.account_id_of_user(alice).await.unwrap(),
            Some(alice_acct.clone())
        );
        assert_eq!(
            db.member_role(&alice_acct, alice).await.unwrap().as_deref(),
            Some("owner")
        );
        assert!(!db.backfill_accounts_v2().await.unwrap(), "marker 幂等");
    }

    /// Task 1：管理员派生账户若仍有非管理员成员，v2 必须保留账户与该成员
    /// （`NOT IN` 子句守住 0005 的 ON DELETE CASCADE）。
    #[tokio::test]
    async fn backfill_v2_keeps_admin_derived_account_with_members() {
        let db = db().await;
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let bob = db.create_user("bob", "pw", false).await.unwrap();
        let admin_acct = xl_s3::access::account_id_of(admin).unwrap();
        // 造旧库 v1 状态：管理员也有派生账户 + 成员行。
        sqlx::query("DELETE FROM schema_meta WHERE key = 'accounts_backfill_v1' OR key = ?")
            .bind(META_ACCOUNTS_CANONICAL)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(db.backfill_accounts().await.unwrap());
        // 把 bob 挪进管理员派生账户（模拟旧库手工共享）。
        sqlx::query("UPDATE account_members SET account_id = ?, role = 'member' WHERE uid = ?")
            .bind(&admin_acct)
            .bind(bob)
            .execute(db.pool())
            .await
            .unwrap();

        assert!(db.backfill_accounts_v2().await.unwrap());
        assert!(
            db.get_account(&admin_acct).await.unwrap().is_some(),
            "仍有成员的账户不得删除"
        );
        assert_eq!(
            db.member_role(&admin_acct, bob).await.unwrap().as_deref(),
            Some("member"),
            "非管理员成员不得随管理员成员行级联删除"
        );
        assert_eq!(
            db.account_id_of_user(bob).await.unwrap(),
            Some(admin_acct.clone())
        );
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE uid = ?")
            .bind(admin)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "管理员自身仍须移出成员表");
    }

    /// Task 1：v2 不动非管理员的派生账户与 ARN；新管理员不再产生 accounts/account_members 行。
    #[tokio::test]
    async fn backfill_v2_preserves_derived_arns_and_admin_create_user_rows() {
        let db = db().await;
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        let derived = xl_s3::access::account_id_of(alice).unwrap();
        sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES ('b', ?, 0)")
            .bind(alice)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM schema_meta WHERE key = ?")
            .bind(META_ACCOUNTS_CANONICAL)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(db.backfill_accounts_v2().await.unwrap());

        assert!(db.get_account(&derived).await.unwrap().is_some());
        assert_eq!(
            db.member_role(&derived, alice).await.unwrap().as_deref(),
            Some("owner")
        );
        let owner: Option<i64> =
            sqlx::query_scalar("SELECT owner_id FROM buckets WHERE name = 'b'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(owner, Some(alice));
        assert!(xl_s3::access::account_arn_matches(
            &format!("arn:aws:iam::{derived}:root"),
            alice
        ));

        // 管理员：不建账户行、不建成员行。
        let root2 = db.create_user("root2", "pw", true).await.unwrap();
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE uid = ?")
            .bind(root2)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "新管理员不得拥有成员行");
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts WHERE id = ?")
            .bind(xl_s3::access::account_id_of(root2).unwrap())
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "新管理员不得拥有派生账户行");
        // 非管理员仍即时挂载派生账户。
        let bob = db.create_user("bob", "pw", false).await.unwrap();
        let bob_acct = xl_s3::access::account_id_of(bob).unwrap();
        assert_eq!(
            db.account_id_of_user(bob).await.unwrap(),
            Some(bob_acct.clone())
        );
        assert_eq!(
            db.member_role(&bob_acct, bob).await.unwrap().as_deref(),
            Some("owner")
        );
    }

    /// 规范化 worker 的单轮事务：管理员移出、legacy 用户补派生账户/owner 成员、
    /// marker 与 ARN 保持；重复执行不新增行。
    #[tokio::test]
    async fn canonicalize_worker_creates_missing_rows_idempotently() {
        let db = db().await;
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        let bob = db.create_user("bob", "pw", false).await.unwrap();
        // 模拟旧库/快照带来的「用户有、账户表缺」：清空账户侧并删除 marker。
        sqlx::query("DELETE FROM account_members")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM accounts")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM schema_meta WHERE key = ?")
            .bind(META_ACCOUNTS_CANONICAL)
            .execute(db.pool())
            .await
            .unwrap();
        // 管理员旧成员行也必须被移出。
        sqlx::query(
            "INSERT INTO accounts (id, name, status, created_at, updated_at) \
             VALUES (?, 'root', 'active', 0, 0)",
        )
        .bind(xl_s3::access::account_id_of(admin).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO account_members (account_id, uid, role, created_at) \
             VALUES (?, ?, 'owner', 0)",
        )
        .bind(xl_s3::access::account_id_of(admin).unwrap())
        .bind(admin)
        .execute(db.pool())
        .await
        .unwrap();

        let changed = db.canonicalize_legacy_accounts_once().await.unwrap();
        assert_eq!(changed, 4, "两个用户各新增账户+成员");
        assert!(db.accounts_canonical().await.unwrap());
        for (uid, name) in [(alice, "alice"), (bob, "bob")] {
            let acct = xl_s3::access::account_id_of(uid).unwrap();
            assert_eq!(
                db.member_role(&acct, uid).await.unwrap().as_deref(),
                Some("owner")
            );
            assert!(
                xl_s3::access::account_arn_matches(&format!("arn:aws:iam::{acct}:root"), uid),
                "{name} ARN 漂移"
            );
        }
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE uid = ?")
            .bind(admin)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "管理员成员行必须移除");

        // 重跑：0 新增，行数不变。
        assert_eq!(db.canonicalize_legacy_accounts_once().await.unwrap(), 0);
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 2);
    }

    /// 部分失败：事务回滚（无半套行、marker 不提前写）；修复后重跑成功。
    #[tokio::test]
    async fn canonicalize_worker_partial_failure_rolls_back_and_reruns() {
        let db = db().await;
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        let bob = db.create_user("bob", "pw", false).await.unwrap();
        sqlx::query("DELETE FROM account_members")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM accounts")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM schema_meta WHERE key = ?")
            .bind(META_ACCOUNTS_CANONICAL)
            .execute(db.pool())
            .await
            .unwrap();
        // 第二个用户的成员插入失败（模拟磁盘/约束故障）。
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TRIGGER fail_canonical BEFORE INSERT ON account_members \
             WHEN NEW.uid = {bob} BEGIN SELECT RAISE(ABORT, 'injected'); END"
        )))
        .execute(db.pool())
        .await
        .unwrap();

        assert!(db.canonicalize_legacy_accounts_once().await.is_err());
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "失败轮不得留下半套成员行");
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 0, "失败轮不得留下半套账户行");
        assert!(!db.accounts_canonical().await.unwrap(), "marker 不得提前写");

        sqlx::query("DROP TRIGGER fail_canonical")
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(db.canonicalize_legacy_accounts_once().await.unwrap(), 4);
        assert!(db.accounts_canonical().await.unwrap());
        for uid in [alice, bob] {
            let acct = xl_s3::access::account_id_of(uid).unwrap();
            assert_eq!(
                db.member_role(&acct, uid).await.unwrap().as_deref(),
                Some("owner")
            );
        }
    }

    /// Task 1：0006 的用量表与 tenancy 补列在 connect 后就位（老库升级同样幂等）。
    #[tokio::test]
    async fn tenancy_schema_and_columns_exist() {
        let db = db().await;
        for col in ["soft_byte_quota", "hard_byte_quota", "object_quota"] {
            let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM pragma_table_info('accounts') WHERE name = '{col}'"
            )))
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(n, 1, "accounts.{col}");
        }
        for col in ["principal_account", "target_account"] {
            let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM pragma_table_info('audit_log') WHERE name = '{col}'"
            )))
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(n, 1, "audit_log.{col}");
        }
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = 'account_usage'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(n, 1, "account_usage 表");
    }

    /// AC-0：新建账户 id 必须是 12 位非负十进制（random() 取模别踩符号位）。
    #[tokio::test]
    async fn create_account_generates_12_digit_ids() {
        let db = db().await;
        let a = db.create_account("a", None).await.unwrap();
        let b = db.create_account("b", None).await.unwrap();
        for id in [&a, &b] {
            assert_eq!(id.len(), 12, "id={id}");
            assert!(id.chars().all(|c| c.is_ascii_digit()), "id={id}");
            assert!(
                !id.starts_with('0'),
                "id 应在 100000000000..=999999999999: {id}"
            );
        }
        assert_ne!(a, b);
    }

    /// AC-1：账户停用 → 登录被拒、account_active=false；恢复后登录恢复。
    /// 账户内有在册全局管理员时不允许停用（防把最后一个管理员锁死在停用账户里）。
    #[tokio::test]
    async fn disabled_account_blocks_login_and_active_admin_account_cannot_disable() {
        let db = db().await;
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let sub = db.create_user("sub", "pw", false).await.unwrap();
        let sub_acct = db.account_id_of_user(sub).await.unwrap().unwrap();
        assert!(db.account_active(sub).await.unwrap());
        assert!(db.set_account_status(&sub_acct, "disabled").await.unwrap());
        assert!(!db.account_active(sub).await.unwrap());
        assert_eq!(db.verify_user("sub", "pw").await.unwrap(), None);
        // 恢复
        assert!(db.set_account_status(&sub_acct, "active").await.unwrap());
        assert_eq!(db.verify_user("sub", "pw").await.unwrap(), Some(sub));

        // 防锁死守卫（防御性测试）：正常路径拒绝把全局管理员挂进账户，这里直接构造旧库
        // 残留状态「管理员是某账户的在册成员」，确认条件更新仍拒绝停用。
        let acct = db.create_account("legacy-admin", None).await.unwrap();
        assert!(
            !db.attach_account_member(&acct, admin, "member")
                .await
                .unwrap(),
            "全局管理员不能作为账户成员"
        );
        sqlx::query(
            "INSERT INTO account_members (account_id, uid, role, created_at) VALUES (?, ?, 'owner', 0)",
        )
        .bind(&acct)
        .bind(admin)
        .execute(db.pool())
        .await
        .unwrap();
        assert!(
            !db.set_account_status(&acct, "disabled").await.unwrap(),
            "含在册全局管理员的账户不能停用"
        );
        // 移除残留成员行后即可停用：守卫确实是唯一阻断点。
        sqlx::query("DELETE FROM account_members WHERE account_id = ?")
            .bind(&acct)
            .execute(db.pool())
            .await
            .unwrap();
        assert!(db.set_account_status(&acct, "disabled").await.unwrap());
    }

    /// AC-0：迁移用户到新账户后，派生 ARN 不再命中、新账户 ARN 命中；账户删除受成员/桶守卫。
    #[tokio::test]
    async fn account_membership_move_changes_arn_mapping_and_delete_is_guarded() {
        let db = db().await;
        let sub = db.create_user("sub", "pw", false).await.unwrap();
        let derived = db.account_id_of_user(sub).await.unwrap().unwrap();
        let acct = db.create_account("team", Some(2)).await.unwrap();
        assert!(db.attach_account_member(&acct, sub, "admin").await.unwrap());

        assert_eq!(
            db.account_id_of_user(sub).await.unwrap(),
            Some(acct.clone())
        );
        assert!(!db.delete_account(&acct).await.unwrap(), "有成员不可删");
        sqlx::query("DELETE FROM account_members WHERE account_id = ?")
            .bind(&acct)
            .execute(db.pool())
            .await
            .unwrap();
        // 成员移除后回落派生账户（legacy fallback）
        assert_eq!(db.account_id_of_user(sub).await.unwrap(), Some(derived));
        assert!(db.delete_account(&acct).await.unwrap());
        assert!(
            !db.delete_account(xl_s3::access::LEGACY_ACCOUNT)
                .await
                .unwrap()
        );
    }

    /// M8-1：运行态表的 fencing 列齐全；更高 term 接管后旧 term 的状态转换被拒绝。
    #[tokio::test]
    async fn fencing_stamps_runtime_tables_and_rejects_stale_writes() {
        let db = db().await;
        for table in [
            "job_tasks",
            "inventory_runs",
            "inventory_cursors",
            "tier_restores",
            "lifecycle_cursors",
        ] {
            let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM pragma_table_info('{table}') \
                 WHERE name IN ('fence_term','fence_leader')"
            )))
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(n, 2, "{table} 缺 fencing 列");
        }

        let old = FencingEpoch { term: 5, leader: 1 };
        let newer = FencingEpoch { term: 6, leader: 2 };

        // 生命周期游标：旧 term 不能覆盖/删除新 term 的游标。
        db.lifecycle_cursor_set("b", "r", "current", Some("a"), 1, old)
            .await
            .unwrap();
        db.lifecycle_cursor_set(
            "b",
            "r",
            "current",
            Some("b"),
            2,
            FencingEpoch { term: 4, leader: 0 },
        )
        .await
        .unwrap();
        assert_eq!(
            db.lifecycle_cursor_get("b", "r", "current").await.unwrap(),
            Some("a".to_string())
        );
        db.lifecycle_cursor_set("b", "r", "current", Some("c"), 3, newer)
            .await
            .unwrap();
        assert_eq!(
            db.lifecycle_cursor_get("b", "r", "current").await.unwrap(),
            Some("c".to_string())
        );
        db.lifecycle_cursor_set(
            "b",
            "r",
            "current",
            None,
            4,
            FencingEpoch { term: 4, leader: 0 },
        )
        .await
        .unwrap();
        assert_eq!(
            db.lifecycle_cursor_get("b", "r", "current").await.unwrap(),
            Some("c".to_string()),
            "旧 term 不得清游标"
        );

        // Inventory：旧 term 的收尾被拒，新 term 可收尾。
        let run = db
            .inventory_create_run("b", "c", "k", 1, old)
            .await
            .unwrap()
            .unwrap();
        db.inventory_run_started(run, 2, old).await.unwrap();
        db.inventory_run_finish(
            run,
            "done",
            Some("m"),
            None,
            FencingEpoch { term: 4, leader: 0 },
        )
        .await
        .unwrap();
        assert_eq!(
            db.inventory_run_by_key("b", "c", "k")
                .await
                .unwrap()
                .unwrap()
                .state,
            "running"
        );
        db.inventory_run_finish(run, "done", Some("m2"), None, newer)
            .await
            .unwrap();
        let row = db
            .inventory_run_by_key("b", "c", "k")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, "done");
        assert_eq!(row.manifest_key.as_deref(), Some("m2"));

        // Tier restore：旧 term 的状态转换与删除都被拒（行留给新 term）。
        sqlx::query(
            "INSERT INTO tier_restores \
             (bucket, key, version_id, expires_at, state, created_at, fence_term, fence_leader) \
             VALUES ('b', 'k', 'v', 0, 'pending', 1, 5, 1)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        db.set_restore_state_fenced(
            "b",
            "k",
            "v",
            "restored",
            99,
            FencingEpoch { term: 4, leader: 0 },
        )
        .await
        .unwrap();
        db.delete_restore_fenced("b", "k", "v", FencingEpoch { term: 4, leader: 0 })
            .await
            .unwrap();
        let r = db.restore_row("b", "k", "v").await.unwrap().unwrap();
        assert_eq!(r.state, "pending");
        db.set_restore_state_fenced("b", "k", "v", "restored", 99, newer)
            .await
            .unwrap();
        assert_eq!(
            db.restore_row("b", "k", "v").await.unwrap().unwrap().state,
            "restored"
        );
    }

    #[tokio::test]
    async fn users_permissions_and_uploads_owner() {
        let db = db().await;
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let sub = db.create_user("sub", "pw", false).await.unwrap();

        // get_user / count_admins / set_password
        assert_eq!(
            db.get_user(sub).await.unwrap(),
            Some(("sub".to_string(), false, true))
        );
        assert_eq!(db.get_user(999).await.unwrap(), None);
        assert_eq!(db.count_admins().await.unwrap(), 1);
        assert!(db.set_password(sub, "new-pw").await.unwrap());
        assert!(db.verify_user("sub", "new-pw").await.unwrap().is_some());
        assert!(!db.set_password(999, "x").await.unwrap());

        // permissions：全量替换；全 0 行不落库
        db.set_permissions(
            sub,
            &[
                PermissionRow {
                    bucket: "a".into(),
                    read: true,
                    write: false,
                    delete: false,
                },
                PermissionRow {
                    bucket: "b".into(),
                    read: false,
                    write: true,
                    delete: true,
                },
                PermissionRow {
                    bucket: "c".into(),
                    read: false,
                    write: false,
                    delete: false,
                },
            ],
        )
        .await
        .unwrap();
        let got = db.get_permissions(sub).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].bucket, "a");
        assert!(got[0].read && !got[0].write);
        db.set_permissions(
            sub,
            &[PermissionRow {
                bucket: "z".into(),
                read: true,
                write: false,
                delete: false,
            }],
        )
        .await
        .unwrap();
        let got = db.get_permissions(sub).await.unwrap();
        assert_eq!(got.len(), 1, "全量替换");

        // list_users 带权限摘要
        let users = db.list_users().await.unwrap();
        assert_eq!(users.len(), 2);
        assert!(users[0].is_admin);
        assert_eq!(users[1].permissions[0].bucket, "z");

        // 删桶清授权（Acl::forget_bucket 是两处删桶路径的共用实现）
        xl_s3::access::Acl::new(db.pool().clone())
            .forget_bucket("z")
            .await
            .unwrap();
        assert!(db.get_permissions(sub).await.unwrap().is_empty());

        // 上传归属：仅在无归属时写入
        db.record_upload("g_202610_x.txt", "files", "x.txt", "file", 1, "e")
            .await
            .unwrap();
        assert!(db.set_upload_owner("g_202610_x.txt", sub).await.unwrap());
        assert!(
            !db.set_upload_owner("g_202610_x.txt", admin).await.unwrap(),
            "二次不覆盖"
        );
        assert!(!db.set_upload_owner("nope.txt", sub).await.unwrap());
        db.record_upload("g_202610_y.txt", "files", "y.txt", "file", 1, "e")
            .await
            .unwrap();
        let mine = db.list_uploads_for(sub, 10).await.unwrap();
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0].saved_path, "g_202610_x.txt");

        // 删用户：级联清 keys/permissions、buckets owner 置 NULL、users 行消失
        db.create_access_key("AK_SUB_0000000000", "SK_SUB_0000000000", sub)
            .await
            .unwrap();
        db.insert_bucket("owned-by-sub", Some(sub)).await.unwrap();
        db.set_permissions(
            sub,
            &[PermissionRow {
                bucket: "z".into(),
                read: true,
                write: false,
                delete: false,
            }],
        )
        .await
        .unwrap();
        assert!(db.delete_user(sub).await.unwrap());
        assert_eq!(db.get_user(sub).await.unwrap(), None);
        assert!(db.get_permissions(sub).await.unwrap().is_empty());
        let owner: Option<i64> =
            sqlx::query_scalar("SELECT owner_id FROM buckets WHERE name = 'owned-by-sub'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(owner, None);
        let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM access_keys WHERE user_id = ?")
            .bind(sub)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(keys, 0);
    }

    #[tokio::test]
    async fn user_and_access_key_roundtrip() {
        let db = db().await;
        let uid = db.create_user("erik", "secret", true).await.unwrap();
        db.create_access_key("AKIA_TEST", "SK_TEST", uid)
            .await
            .unwrap();
        let sk: String =
            sqlx::query_scalar("SELECT secret_key FROM access_keys WHERE access_key = ?")
                .bind("AKIA_TEST")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(sk, "SK_TEST");
    }

    #[tokio::test]
    async fn instant_index_expires() {
        let db = db().await;
        db.instant_set("k", "v", 3600).await.unwrap();
        assert_eq!(db.instant_get("k").await.unwrap().as_deref(), Some("v"));
        db.instant_set("expired", "v", 0).await.unwrap();
        assert_eq!(db.instant_get("expired").await.unwrap(), None);
    }

    #[tokio::test]
    async fn instant_delete_prefix_clears_only_that_prefix() {
        let db = db().await;
        db.instant_set("aetherupload:resource:file_a", "v1", 3600)
            .await
            .unwrap();
        db.instant_set("aetherupload:resource:file_b", "v2", 3600)
            .await
            .unwrap();
        db.instant_set("other", "keep", 3600).await.unwrap();

        db.instant_delete_prefix("aetherupload:resource:")
            .await
            .unwrap();

        assert_eq!(
            db.instant_get("aetherupload:resource:file_a")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            db.instant_get("aetherupload:resource:file_b")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            db.instant_get("other").await.unwrap().as_deref(),
            Some("keep")
        );
    }

    #[tokio::test]
    async fn upload_records() {
        let db = db().await;
        let uid = db.create_user("u", "pw", false).await.unwrap();
        db.record_upload("g_202610_abc.txt", "files", "abc.txt", "file", 7, "etag1")
            .await
            .unwrap();
        assert!(
            db.register_upload("g_202610_abc.txt", "my.txt", uid, false)
                .await
                .unwrap()
        );
        let rows = db.list_uploads(10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].original_name.as_deref(), Some("my.txt"));
    }

    #[tokio::test]
    async fn register_upload_without_record_is_false() {
        let db = db().await;
        let uid = db.create_user("u", "pw", false).await.unwrap();
        assert!(
            !db.register_upload("nope.txt", "my.txt", uid, false)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn register_upload_ownership_guard() {
        // 防挂名：非管理员不能改别人已归属的记录；管理员可
        let db = db().await;
        let a = db.create_user("a", "pw", false).await.unwrap();
        let b = db.create_user("b", "pw", false).await.unwrap();
        let root = db.create_user("root", "pw", true).await.unwrap();
        db.record_upload("p", "files", "k", "file", 1, "e")
            .await
            .unwrap();

        assert!(
            db.register_upload("p", "n1", a, false).await.unwrap(),
            "无归属可认领"
        );
        assert!(
            !db.register_upload("p", "n2", b, false).await.unwrap(),
            "已归属他人：拒"
        );
        assert!(
            db.register_upload("p", "n3", root, true).await.unwrap(),
            "管理员可改"
        );
        let owner: Option<i64> =
            sqlx::query_scalar("SELECT owner_id FROM uploads WHERE saved_path = 'p'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(owner, Some(root));
        let name: String =
            sqlx::query_scalar("SELECT original_name FROM uploads WHERE saved_path = 'p'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(name, "n3");
    }

    #[tokio::test]
    async fn user_status_disable_flow() {
        let db = db().await;
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let admin2 = db.create_user("root2", "pw", true).await.unwrap();
        let sub = db.create_user("sub", "pw12345678", false).await.unwrap();

        // 停用：登录与 get_user 的 active 位立即反映
        assert!(db.set_user_status(sub, false).await.unwrap());
        assert_eq!(
            db.get_user(sub).await.unwrap(),
            Some(("sub".to_string(), false, false))
        );
        assert!(db.verify_user("sub", "pw12345678").await.unwrap().is_none());
        // 在册管理员：两人在册 → 可停其一；剩最后一人 → 原子守卫拦下（评审 P2）
        assert_eq!(db.count_active_admins().await.unwrap(), 2);
        assert!(db.set_user_status(admin2, false).await.unwrap());
        assert_eq!(db.count_active_admins().await.unwrap(), 1);
        assert!(
            !db.set_user_status(admin, false).await.unwrap(),
            "最后一个在册管理员不可停用（单条条件 UPDATE 原子守卫）"
        );
        assert_eq!(db.count_active_admins().await.unwrap(), 1);
        // 重新启用恢复
        assert!(db.set_user_status(admin2, true).await.unwrap());
        assert_eq!(db.count_active_admins().await.unwrap(), 2);
        assert!(
            db.verify_user("sub", "pw12345678").await.unwrap().is_none(),
            "停用者仍登录不了"
        );

        // 删除的原子守卫：非管理员恒可删；最后一个在册管理员不可删
        assert!(
            db.delete_user(admin2).await.unwrap(),
            "两名在册管理员时可删其一"
        );
        assert!(
            !db.delete_user(admin).await.unwrap(),
            "最后一个在册管理员不可删"
        );
        assert!(db.get_user(admin).await.unwrap().is_some());
        assert!(db.delete_user(sub).await.unwrap(), "非管理员恒可删");
        assert!(!db.set_user_status(999, true).await.unwrap());

        // 评审 nit①：对「已停用」的管理员停用是幂等 no-op——不得被计数守卫误杀
        //（守卫只拦「把在册管理员停掉且会留下 0 在册」）。此处 admin2 已被删除，
        // 先造一个已停用的管理员：admin 在册，把另一名管理员停用后重复停用。
        let admin3 = db.create_user("root3", "pw", true).await.unwrap();
        assert!(db.set_user_status(admin3, false).await.unwrap());
        assert!(
            db.set_user_status(admin3, false).await.unwrap(),
            "对已停用管理员重复停用：幂等 no-op，不得被守卫误杀"
        );
    }

    /// M7：site_config 种子幂等（同库二次 connect 不改 id/token）；apply_site_config 覆盖。
    #[tokio::test]
    async fn site_bootstrap_idempotent_and_config_applies() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("site.db").display());
        let db = Db::connect(&url).await.unwrap();
        let a = db.site_identity().await.unwrap();
        assert_eq!(a.id.len(), 36, "uuid");
        assert_eq!(a.token.len(), 32, "32 hex token");
        let db2 = Db::connect(&url).await.unwrap();
        let b = db2.site_identity().await.unwrap();
        assert_eq!(
            (a.id.as_str(), a.token.as_str()),
            (b.id.as_str(), b.token.as_str())
        );
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM site_config")
            .fetch_one(db2.pool())
            .await
            .unwrap();
        assert_eq!(n, 1, "site_config 必须始终恰好一行（身份/触发器同源）");
        db2.apply_site_config(
            Some("east"),
            Some("eu-west-1"),
            Some(7),
            Some(&"b".repeat(32)),
        )
        .await
        .unwrap();
        let c = db2.site_identity().await.unwrap();
        assert_eq!(c.name, "east");
        assert_eq!(c.region, "eu-west-1");
        assert_eq!(c.priority, 7);
        assert_eq!(c.token, "b".repeat(32));
    }

    /// M7：v1.10 老库（无 0002、无 target_kind）升级——补列 + 建表 + 种子，二次启动幂等；
    /// 写入一次证明 schema 可用（触发器在切片 1a 才创建）。
    #[tokio::test]
    async fn m7_upgrade_from_v110_db() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.db");
        let pool = SqlitePool::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
        sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
        // 造一条 pre-M7 的 repl_outbox 行（无 target_kind 列）
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS replication_rules (bucket TEXT PRIMARY KEY, rules_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);\
             CREATE TABLE IF NOT EXISTS repl_outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, bucket TEXT NOT NULL, key TEXT NOT NULL, \
              version_id TEXT NOT NULL, etag TEXT NOT NULL, size INTEGER NOT NULL, remote_id TEXT NOT NULL, \
              dest_bucket TEXT NOT NULL, is_delete_marker INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending', \
              attempts INTEGER NOT NULL DEFAULT 0, claimed_at INTEGER, next_retry_at INTEGER NOT NULL DEFAULT 0, \
              created_at INTEGER NOT NULL DEFAULT 0, last_error TEXT);",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let db = Db::connect(&url).await.unwrap();
        let cols = sqlx::query("SELECT name FROM pragma_table_info('repl_outbox')")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert!(
            cols.iter()
                .any(|r| r.get::<String, _>("name") == "target_kind")
        );
        assert!(
            cols.iter()
                .any(|r| r.get::<String, _>("name") == "is_version_delete")
        );
        assert!(db.site_identity().await.is_ok());
        // 实写一次（触发器纳入后此测试升级为「写一次验证触发器」）
        db.insert_bucket("upgrade-bkt", None).await.unwrap();
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM buckets WHERE name='upgrade-bkt'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(n, 1);
        // 二次连接不报错
        drop(db);
        let _ = Db::connect(&url).await.unwrap();
    }

    /// ensure_columns 幂等：老库（无 status 列）升级补列；再开一次不得重复 ALTER 报错。
    #[tokio::test]
    async fn ensure_columns_migrates_legacy_db_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("legacy.db");

        // 手工造一个「老库」：0001 的老形态（users 无 status）+ 必要表
        let pool = SqlitePool::connect(&format!("sqlite://{}?mode=rwc", db_path.display()))
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE, \
              password_hash TEXT NOT NULL, is_admin INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL);",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO users (username, password_hash, is_admin, created_at) VALUES ('old', 'h', 1, 0)")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;

        // 首次 connect：重放 0001（IF NOT EXISTS 不动老表）+ ensure_columns 补 status
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let db = Db::connect(&url).await.unwrap();
        assert_eq!(
            db.get_user(1).await.unwrap(),
            Some(("old".to_string(), true, true)),
            "老行应补出 status='active'"
        );
        drop(db);

        // 二次 connect：列已存在，不得因重复 ALTER 失败
        let db = Db::connect(&url).await.unwrap();
        assert!(db.get_user(1).await.unwrap().is_some());
    }

    /// M8 缺口 C：老库 buckets 无 default_encryption → ensure_columns 补列后可用。
    #[tokio::test]
    async fn legacy_db_gains_default_encryption_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-enc.db");
        let pool = SqlitePool::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, \
              creation_date INTEGER NOT NULL, versioning TEXT NOT NULL DEFAULT 'Off');",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let db = Db::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
        db.insert_bucket("old-b", None).await.unwrap();
        assert_eq!(
            db.get_bucket_default_encryption("old-b").await.unwrap(),
            None
        );
        assert!(
            db.set_bucket_default_encryption("old-b", "aws:kms")
                .await
                .unwrap()
        );
        assert_eq!(
            db.get_bucket_default_encryption("old-b").await.unwrap(),
            Some("aws:kms".to_string())
        );
        assert!(db.clear_bucket_default_encryption("old-b").await.unwrap());
        assert_eq!(
            db.get_bucket_default_encryption("old-b").await.unwrap(),
            None
        );
    }

    /// M8 S0：旧库（只有 0001）升级——0003 全部表 + jobs/lifecycle 补列；二次连接幂等。
    #[tokio::test]
    async fn m8_schema_and_columns_exist_on_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v110.db");
        let pool = SqlitePool::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
        sqlx::raw_sql(SCHEMA).execute(&pool).await.unwrap();
        // 旧 PK (bucket,prefix) 下的既有行：重建必须原样搬过去。
        sqlx::raw_sql(
            "INSERT INTO lifecycle_rules (bucket,prefix,expire_days,enabled,updated_at) \
             VALUES ('old','p/',7,1,1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let url = format!("sqlite://{}?mode=rwc", path.display());
        let db = Db::connect(&url).await.unwrap();
        for t in [
            "job_tasks",
            "access_points",
            "olap_configs",
            "inventory_configs",
            "inventory_runs",
            "tier_restores",
        ] {
            let n: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            )
            .bind(t)
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(n, 1, "缺少表 {t}");
        }
        let jcols = sqlx::query("SELECT name FROM pragma_table_info('jobs')")
            .fetch_all(db.pool())
            .await
            .unwrap();
        for c in [
            "spec_json",
            "progress",
            "total",
            "succeeded",
            "failed",
            "report_bucket",
            "report_prefix",
            "priority",
            "requested_by",
            "parent_job_id",
            "started_at",
            "finished_at",
        ] {
            assert!(
                jcols.iter().any(|r| r.get::<String, _>("name") == c),
                "jobs 缺列 {c}"
            );
        }
        let lcols = sqlx::query("SELECT name FROM pragma_table_info('lifecycle_rules')")
            .fetch_all(db.pool())
            .await
            .unwrap();
        for c in [
            "rule_id",
            "expiration_days",
            "transition_days",
            "transition_class",
            "transition_tier",
            "config_json",
        ] {
            assert!(
                lcols.iter().any(|r| r.get::<String, _>("name") == c),
                "lifecycle_rules 缺列 {c}"
            );
        }
        let idx: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_lifecycle_rule_id'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(idx, 1, "缺少 idx_lifecycle_rule_id");
        // 旧行保留，且新 PK 允许同桶同 prefix 多规则（旧 PK 下第二条会冲突）。
        assert_eq!(db.list_lifecycle_rules("old").await.unwrap().len(), 1);
        let row = |id: &str| LifecycleRuleRow {
            bucket: "old".into(),
            rule_id: Some(id.into()),
            prefix: "p/".into(),
            expire_days: 7,
            expiration_days: Some(7),
            transition_days: None,
            transition_class: None,
            transition_tier: None,
            enabled: true,
            updated_at: 0,
            config_json: None,
        };
        db.set_lifecycle_rules("old", &[row("r1"), row("r2")], 2)
            .await
            .unwrap();
        assert_eq!(db.list_lifecycle_rules("old").await.unwrap().len(), 2);
        drop(db);
        // 二次连接：0003/补列均幂等
        let _ = Db::connect(&url).await.unwrap();
    }

    /// M8 S0：clear_bucket_m8 清 M8 表（含生命周期规则）、保留 jobs 与其他桶行。
    #[tokio::test]
    async fn clear_bucket_m8_cleans_m8_tables_and_keeps_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::connect(&format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("m8.db").display()
        ))
        .await
        .unwrap();
        sqlx::raw_sql(
            "INSERT INTO access_points (name,bucket,owner_id,policy_json,created_at,updated_at) \
               VALUES ('ap1','b1',7,NULL,1,1),('ap2','b2',7,NULL,1,1);\
             INSERT INTO olap_configs (access_point,kind,endpoint,updated_at) \
               VALUES ('ap1','http','http://x',1),('ap2','http','http://y',1);\
             INSERT INTO inventory_configs (bucket,id,owner_id,config_json,updated_at) \
               VALUES ('b1','daily',7,'{}',1),('b2','daily',7,'{}',1);\
             INSERT INTO inventory_runs (bucket,config_id,run_key,started_at,state,created_at) \
               VALUES ('b1','daily','k',1,'done',1),('b2','daily','k',1,'done',1);\
             INSERT INTO tier_restores (bucket,key,version_id,created_at) \
               VALUES ('b1','k','v',1),('b2','k','v',1);\
             INSERT INTO lifecycle_rules (bucket,prefix,expire_days,enabled,updated_at) \
               VALUES ('b1','',1,1,1),('b2','',1,1,1);\
             INSERT INTO jobs (kind,bucket,state,created_at,updated_at) \
               VALUES ('batch','b1','done',1,1);",
        )
        .execute(db.pool())
        .await
        .unwrap();
        db.clear_bucket_m8("b1").await.unwrap();
        for (table, sql) in [
            (
                "access_points",
                "SELECT COUNT(*) FROM access_points WHERE bucket='b1'",
            ),
            (
                "inventory_configs",
                "SELECT COUNT(*) FROM inventory_configs WHERE bucket='b1'",
            ),
            (
                "inventory_runs",
                "SELECT COUNT(*) FROM inventory_runs WHERE bucket='b1'",
            ),
            (
                "tier_restores",
                "SELECT COUNT(*) FROM tier_restores WHERE bucket='b1'",
            ),
            (
                "lifecycle_rules",
                "SELECT COUNT(*) FROM lifecycle_rules WHERE bucket='b1'",
            ),
        ] {
            let n: i64 = sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap();
            assert_eq!(n, 0, "{table} 未清");
        }
        // 其他桶的行必须保留（抽查 AP/Inventory/规则）
        for (table, sql) in [
            (
                "access_points",
                "SELECT COUNT(*) FROM access_points WHERE bucket='b2'",
            ),
            (
                "inventory_configs",
                "SELECT COUNT(*) FROM inventory_configs WHERE bucket='b2'",
            ),
            (
                "inventory_runs",
                "SELECT COUNT(*) FROM inventory_runs WHERE bucket='b2'",
            ),
            (
                "tier_restores",
                "SELECT COUNT(*) FROM tier_restores WHERE bucket='b2'",
            ),
            (
                "lifecycle_rules",
                "SELECT COUNT(*) FROM lifecycle_rules WHERE bucket='b2'",
            ),
        ] {
            let n: i64 = sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap();
            assert_eq!(n, 1, "{table} 误删其他桶行");
        }
        let ap2: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM access_points WHERE bucket='b2'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(ap2, 1, "其他桶 AP 被误删");
        let olap1: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM olap_configs WHERE access_point='ap1'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(olap1, 0, "olap_configs 未级联清理");
        let olap2: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM olap_configs WHERE access_point='ap2'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(olap2, 1);
        let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(jobs, 1, "jobs 必须按审计保留");
    }

    fn task(action: &str) -> NewJobTask {
        NewJobTask {
            action: action.into(),
            payload: "{}".into(),
            dedupe_key: None,
        }
    }

    /// M8 S0：领取优先级/重试退避/上限/作业收敛/暂停取消/重试子作业/配额。
    #[tokio::test]
    async fn job_engine_state_machine() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::connect(&format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("jobs.db").display()
        ))
        .await
        .unwrap();
        // 低优先级先建、高优先级后建：领取必须按 priority DESC
        let low = db
            .create_job(
                &JobSpec {
                    kind: "batch".into(),
                    bucket: "b1".into(),
                    requested_by: Some(7),
                    priority: 1,
                    tasks: vec![task("ReplaceTags")],
                    ..Default::default()
                },
                1000,
            )
            .await
            .unwrap();
        let high = db
            .create_job(
                &JobSpec {
                    kind: "batch".into(),
                    bucket: "b1".into(),
                    requested_by: Some(7),
                    priority: 9,
                    tasks: vec![task("ReplaceTags")],
                    ..Default::default()
                },
                1000,
            )
            .await
            .unwrap();
        assert_eq!(db.job_inflight_counts(Some(7)).await.unwrap(), (2, 2));
        let claimed = db.claim_task(1000, 900).await.unwrap().unwrap();
        assert_eq!(claimed.job_id, high, "高优先级必须先领");
        // 成功 → 作业 done
        assert_eq!(
            db.finish_task(high, 1, true, "Succeeded", None, 1001, 3)
                .await
                .unwrap(),
            "done"
        );
        let j = db.job_get(high).await.unwrap().unwrap();
        assert_eq!(j["state"], "done");
        assert_eq!(j["succeeded"], 1);

        // 低优先级任务：失败 2 次（退避）后第 3 次达上限 → failed/error
        let c = db.claim_task(1000, 900).await.unwrap().unwrap();
        assert_eq!(c.job_id, low);
        assert_eq!(
            db.finish_task(low, 1, false, "InternalError", Some("x"), 1001, 3)
                .await
                .unwrap(),
            "queued"
        );
        // 未到 next_at 不可领
        assert!(db.claim_task(1001, 900).await.unwrap().is_none());
        let c = db.claim_task(5000, 900).await.unwrap().unwrap();
        assert_eq!(c.attempt, 1);
        assert_eq!(
            db.finish_task(low, 1, false, "InternalError", None, 5001, 3)
                .await
                .unwrap(),
            "queued"
        );
        let c = db.claim_task(10_000, 900).await.unwrap().unwrap();
        assert_eq!(c.attempt, 2);
        assert_eq!(
            db.finish_task(low, 1, false, "PermanentFailure", None, 10_001, 3)
                .await
                .unwrap(),
            "failed"
        );
        let j = db.job_get(low).await.unwrap().unwrap();
        assert_eq!(j["state"], "error");
        assert_eq!(j["failed"], 1);

        // retry-failed 建子作业（只含失败任务）
        let child = db.job_retry_failed(low, 20_000).await.unwrap().unwrap();
        let cj = db.job_get(child).await.unwrap().unwrap();
        assert_eq!(cj["parent_job_id"], low);
        assert_eq!(cj["total"], 1);
        let tasks = db.job_tasks(child, 0, 10).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["action"], "ReplaceTags");
        // 清空子作业，避免干扰后续领取
        let cc = db.claim_task(20_001, 900).await.unwrap().unwrap();
        assert_eq!(cc.job_id, child);
        db.finish_task(child, 1, true, "Succeeded", None, 20_002, 3)
            .await
            .unwrap();

        // 暂停/恢复/取消
        let p = db
            .create_job(
                &JobSpec {
                    kind: "batch".into(),
                    bucket: "b1".into(),
                    tasks: vec![task("Copy"), task("Delete")],
                    ..Default::default()
                },
                30_000,
            )
            .await
            .unwrap();
        let c = db.claim_task(30_001, 900).await.unwrap().unwrap();
        assert_eq!(c.job_id, p);
        assert!(db.job_set_state(p, "pause", 30_002).await.unwrap());
        assert!(db.claim_task(30_003, 900).await.unwrap().is_none());
        assert!(db.job_set_state(p, "resume", 30_004).await.unwrap());
        assert!(db.job_set_state(p, "cancel", 30_005).await.unwrap());
        // 排队任务被取消；在跑任务允许完成
        let tasks = db.job_tasks(p, 0, 10).await.unwrap();
        assert_eq!(tasks[0]["state"], "running");
        assert_eq!(tasks[1]["state"], "cancelled");
        assert_eq!(
            db.finish_task(p, 1, true, "Succeeded", None, 30_006, 3)
                .await
                .unwrap(),
            "done"
        );
        let j = db.job_get(p).await.unwrap().unwrap();
        assert_eq!(j["state"], "cancelled", "取消后不得被收敛覆盖");

        // 超时回收：running 且 deadline 过期 → 一次失败
        let t = db
            .create_job(
                &JobSpec {
                    kind: "batch".into(),
                    bucket: "b1".into(),
                    tasks: vec![task("Copy")],
                    ..Default::default()
                },
                40_000,
            )
            .await
            .unwrap();
        db.claim_task(40_000, 10).await.unwrap().unwrap();
        assert_eq!(
            db.recover_expired_tasks(40_100, 3, FencingEpoch::UNFENCED)
                .await
                .unwrap(),
            1
        );
        let tasks = db.job_tasks(t, 0, 10).await.unwrap();
        assert_eq!(tasks[0]["state"], "queued");
        assert_eq!(tasks[0]["attempt"], 1);
        // 清掉退避中的超时作业，避免下一条 claim 抢到它
        assert!(db.job_set_state(t, "cancel", 40_200).await.unwrap());

        // 启动恢复：running 且 started_at 早于 cutoff（崩溃遗留，deadline 未到）
        let s = db
            .create_job(
                &JobSpec {
                    kind: "batch".into(),
                    bucket: "b1".into(),
                    tasks: vec![task("Copy")],
                    ..Default::default()
                },
                50_000,
            )
            .await
            .unwrap();
        db.claim_task(50_000, 3600).await.unwrap().unwrap();
        assert_eq!(
            db.recover_stale_tasks(50_100, 3, FencingEpoch::UNFENCED)
                .await
                .unwrap(),
            1
        );
        let tasks = db.job_tasks(s, 0, 10).await.unwrap();
        assert_eq!(tasks[0]["state"], "queued");
        assert_eq!(tasks[0]["attempt"], 1);
    }

    /// M8 S1：访问点 CRUD 与按桶列举。
    #[tokio::test]
    async fn access_point_crud() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::connect(&format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("ap.db").display()
        ))
        .await
        .unwrap();
        let u7 = db.create_user("u7", "pw", false).await.unwrap();
        let u8 = db.create_user("u8", "pw", false).await.unwrap();
        db.create_access_point("ap1", "b1", u7, None, 1)
            .await
            .unwrap();
        db.create_access_point("ap2", "b2", u8, Some(r#"{"Statement":[]}"#), 2)
            .await
            .unwrap();
        let store = db.control_store();
        let a = store.access_point("ap1").await.unwrap().unwrap();
        assert_eq!(a.bucket, "b1");
        assert_eq!(a.owner, OwnerRef::User("u7".into()));
        assert!(a.policy_json.is_none());
        assert_eq!(store.access_points().await.unwrap().len(), 2);
        // 更新策略 + 清空
        assert!(
            db.set_access_point_policy("ap1", Some(r#"{"Statement":[]}"#), 3)
                .await
                .unwrap()
        );
        assert!(
            store
                .access_point("ap1")
                .await
                .unwrap()
                .unwrap()
                .policy_json
                .is_some()
        );
        db.set_access_point_policy("ap1", None, 4).await.unwrap();
        assert!(
            store
                .access_point("ap1")
                .await
                .unwrap()
                .unwrap()
                .policy_json
                .is_none()
        );
        assert!(db.delete_access_point("ap2").await.unwrap());
        assert!(store.access_point("ap2").await.unwrap().is_none());
        assert!(!db.delete_access_point("missing").await.unwrap());
    }

    #[tokio::test]
    async fn standalone_writes_use_control_txn_and_preserve_owner_id() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let _admin = db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        assert!(
            db.create_access_point("ap1", "b1", alice, None, 1)
                .await
                .unwrap()
        );
        assert!(
            !db.create_access_point("ap1", "b1", alice, None, 2)
                .await
                .unwrap()
        );
        let stored: i64 = sqlx::query_scalar("SELECT owner_id FROM access_points WHERE name='ap1'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(stored, alice, "单机创建仍写创建者本地 id");
        assert_eq!(
            db.control_store()
                .access_point("ap1")
                .await
                .unwrap()
                .unwrap()
                .owner,
            OwnerRef::User("alice".into())
        );
        assert!(
            db.set_access_point_policy("ap1", Some(r#"{"Statement":[]}"#), 3)
                .await
                .unwrap()
        );
        assert!(db.delete_access_point("ap1").await.unwrap());
        assert!(!db.delete_access_point("ap1").await.unwrap());
    }

    /// 桶删除级联只清该桶的 AP/OLAP（characterization pin：迁 txn 后行为不变）。
    #[tokio::test]
    async fn clear_bucket_m8_tx_deletes_bucket_ap_olap_only() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let _ = db.create_user("root", "pw", true).await.unwrap();
        let root: i64 = db.owner_uid(&OwnerRef::Admin).await.unwrap();
        db.create_access_point("ap-a", "b1", root, None, 1)
            .await
            .unwrap();
        db.create_access_point("ap-b", "b2", root, None, 2)
            .await
            .unwrap();
        db.set_olap_config("ap-a", "http", Some("https://x"), None, None, 5000, 1024, 3)
            .await
            .unwrap();
        db.set_olap_config("ap-b", "http", Some("https://y"), None, None, 5000, 1024, 3)
            .await
            .unwrap();
        let mut conn = db.pool().acquire().await.unwrap();
        Db::clear_bucket_m8_tx(&mut conn, "b1").await.unwrap();
        drop(conn);
        let store = db.control_store();
        assert!(store.access_point("ap-a").await.unwrap().is_none());
        assert!(store.olap_config("ap-a").await.unwrap().is_none());
        assert!(store.access_point("ap-b").await.unwrap().is_some());
        assert!(store.olap_config("ap-b").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn control_store_reads_ap_olap_with_natural_owner() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        db.create_access_point("ap-alice", "b1", alice, None, 1)
            .await
            .unwrap();
        db.create_access_point("ap-root", "b1", admin, None, 2)
            .await
            .unwrap();
        // owner_id=0（witness 哨兵）与悬空 id 只由快照安装产生，直接 raw 插入，
        // 避免 Task 5 把单机写 API 改成 uid→username 解析后测试口径失真。
        sqlx::query(
            "INSERT INTO access_points (name, bucket, owner_id, policy_json, created_at, updated_at) \
             VALUES ('ap-witness', 'b1', 0, NULL, 3, 3)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO access_points (name, bucket, owner_id, policy_json, created_at, updated_at) \
             VALUES ('ap-dangling', 'b1', 999, NULL, 4, 4)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        db.set_olap_config(
            "ap-alice",
            "http",
            Some("https://x"),
            None,
            None,
            5000,
            1024,
            5,
        )
        .await
        .unwrap();

        let store = db.control_store();
        assert_eq!(
            store.access_point("ap-alice").await.unwrap().unwrap().owner,
            OwnerRef::User("alice".into())
        );
        // 管理员拥有也读成 User（保持 console JSON/筛选语义），不是 Admin。
        assert_eq!(
            store.access_point("ap-root").await.unwrap().unwrap().owner,
            OwnerRef::User("root".into())
        );
        assert_eq!(
            store
                .access_point("ap-witness")
                .await
                .unwrap()
                .unwrap()
                .owner,
            OwnerRef::Admin
        );
        assert_eq!(
            store
                .access_point("ap-dangling")
                .await
                .unwrap()
                .unwrap()
                .owner,
            OwnerRef::Admin
        );
        assert_eq!(
            store
                .access_points()
                .await
                .unwrap()
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>(),
            ["ap-alice", "ap-dangling", "ap-root", "ap-witness"]
        );

        let cfg = store.olap_config("ap-alice").await.unwrap().unwrap();
        assert_eq!(cfg.kind, "http");
        assert_eq!(cfg.endpoint.as_deref(), Some("https://x"));
        assert_eq!(cfg.module, None);
        assert_eq!(store.olap_configs().await.unwrap(), vec![cfg]);

        assert_eq!(
            db.owner_uid(&OwnerRef::User("alice".into())).await.unwrap(),
            alice
        );
        assert_eq!(db.owner_uid(&OwnerRef::Admin).await.unwrap(), admin);
        assert_eq!(
            db.owner_uid(&OwnerRef::User("ghost".into())).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn control_txn_ap_olap_writes_and_owner_resolution() {
        use xl_store::control_store::{AccessPointEntry, ControlTxn, OlapConfigEntry};
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let admin = db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            assert!(
                txn.access_point_create(&AccessPointEntry {
                    name: "ap1".into(),
                    bucket: "b1".into(),
                    owner: OwnerRef::User("alice".into()),
                    policy_json: None,
                    created_at: 1,
                    updated_at: 2,
                })
                .await
                .unwrap()
            );
            assert!(
                !txn.access_point_create(&AccessPointEntry {
                    name: "ap1".into(),
                    bucket: "b1".into(),
                    owner: OwnerRef::Admin,
                    policy_json: None,
                    created_at: 3,
                    updated_at: 4,
                })
                .await
                .unwrap(),
                "同名不覆盖"
            );
            assert!(txn.access_point_exists("ap1").await.unwrap());
            txn.olap_config_upsert(&OlapConfigEntry {
                access_point: "ap1".into(),
                kind: "http".into(),
                endpoint: Some("https://x".into()),
                module: Some(vec![1, 2, 3]),
                module_sha256: Some("ab".into()),
                timeout_ms: 5000,
                max_response_bytes: 1024,
                updated_at: 5,
            })
            .await
            .unwrap();
            let rows = txn.access_points().await.unwrap();
            assert_eq!(rows[0].owner, OwnerRef::User("alice".into()));
            assert!(txn.access_point_delete("ap1").await.unwrap());
            assert!(!txn.access_point_delete("ap1").await.unwrap());
            assert!(
                txn.olap_configs().await.unwrap().is_empty(),
                "AP 删除级联 OLAP"
            );
            // Admin 属主映射到本节点管理员 id；witness 由 install 测试覆盖。
            assert!(
                txn.access_point_create(&AccessPointEntry {
                    name: "ap2".into(),
                    bucket: "b1".into(),
                    owner: OwnerRef::Admin,
                    policy_json: None,
                    created_at: 6,
                    updated_at: 6,
                })
                .await
                .unwrap()
            );
            let admin_id: i64 =
                sqlx::query_scalar("SELECT owner_id FROM access_points WHERE name = 'ap2'")
                    .fetch_one(&mut *tx)
                    .await
                    .unwrap();
            assert_eq!(admin_id, admin);
        }
        tx.commit().await.unwrap();
        assert_eq!(
            alice,
            db.owner_uid(&OwnerRef::User("alice".into())).await.unwrap()
        );
    }
}

// ——— M4 切片 6：生命周期规则与作业（jobs 表进度） ———

/// 生命周期规则行。M8 S0 扩为全字段（S4b 只做映射，不再改 db.rs）；
/// `expire_days` 为 M4 旧列（无 Expiration 的新规则写哨兵 -1），`expiration_days` 为新语义。
/// 类型收敛到 ControlStore 缝，行为字段不变。
pub use xl_store::control_store::LifecycleRuleEntry as LifecycleRuleRow;

/// CMK 删除/扫描的桶默认加密引用源（xl-store 不依赖 SQL，经 trait 注入）。
/// 存值形态 `aws:kms:<名或 ARN>`（S3/控制台写入口径）——按规范名归一后比对。
#[async_trait::async_trait]
impl xl_store::store::CmkConfigRefs for Db {
    async fn cmk_config_refs(
        &self,
        cmk: &str,
    ) -> xl_store::error::Result<Vec<xl_store::store::CmkBucketRef>> {
        let rows = self.list_bucket_default_encryptions().await.map_err(|e| {
            xl_store::error::Error::Unavailable(format!("读取桶默认加密配置失败: {e}"))
        })?;
        Ok(rows
            .into_iter()
            .filter_map(|(bucket, setting)| {
                let raw = setting.strip_prefix("aws:kms:")?;
                (xl_store::kms::parse_cmk_id(raw).ok() == Some(cmk))
                    .then_some(xl_store::store::CmkBucketRef { bucket, setting })
            })
            .collect())
    }
}

impl Db {
    /// 某桶的全部规则（控制台 GET 用）。
    pub async fn list_lifecycle_rules(&self, bucket: &str) -> Result<Vec<LifecycleRuleRow>> {
        self.control_store()
            .lifecycle_rules(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 全量替换某桶规则（PUT 语义：先删后插，一个事务）。`now` 由调用方给（可测）。
    pub async fn set_lifecycle_rules(
        &self,
        bucket: &str,
        rules: &[LifecycleRuleRow],
        now: i64,
    ) -> Result<()> {
        let mut rows: Vec<LifecycleRuleRow> = rules.to_vec();
        for r in &mut rows {
            r.updated_at = now;
        }
        let mut tx = self.pool.begin().await?;
        let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
        txn.replace_lifecycle_rules(bucket, &rows)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 4.10：读取某规则某阶段的扫描游标（None = 从头扫）。
    pub async fn lifecycle_cursor_get(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
    ) -> Result<Option<String>> {
        self.control_store()
            .lifecycle_cursor(bucket, rule_id, stage)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 4.10：持久化/清除扫描游标（`None` → 删除行，表示本阶段已扫完，下轮从头）。
    /// M8-1：游标写入同样受 fencing 保护——行已被更高 term 接管时旧 leader 的迟到游标
    /// 写入是 no-op（`fence_term <= token.term` 才接受并重标 token）。
    pub async fn lifecycle_cursor_set(
        &self,
        bucket: &str,
        rule_id: &str,
        stage: &str,
        cursor: Option<&str>,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
        match cursor {
            Some(c) if !c.is_empty() => {
                let row = xl_store::control_store::lifecycle::LifecycleCursorRow {
                    bucket: bucket.to_string(),
                    rule_id: rule_id.to_string(),
                    stage: stage.to_string(),
                    cursor: c.to_string(),
                    updated_at: now,
                    fence_term: fence.term_i64(),
                    fence_leader: fence.leader_i64(),
                };
                txn.lifecycle_cursor_upsert(&row)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
            }
            _ => {
                txn.lifecycle_cursor_delete(bucket, rule_id, stage, fence.term_i64())
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// 全部启用规则（worker 扫描用）。
    pub async fn enabled_lifecycle_rules(&self) -> Result<Vec<LifecycleRuleRow>> {
        self.control_store()
            .enabled_lifecycle_rules()
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 开一条作业（worker 每次执行一条）。
    pub async fn job_start(&self, kind: &str, bucket: &str, prefix: &str, now: i64) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let id = crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_start(kind, bucket, prefix, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(id)
    }

    /// 收尾一条作业（done/error + 计数 + 可选错误）。`error=None` 保留已有错误
    /// （与复制路径一致：错误一旦落库不可被收尾静默清空）。
    #[allow(clippy::too_many_arguments)]
    pub async fn job_finish(
        &self,
        id: i64,
        state: &str,
        scanned: i64,
        deleted: i64,
        skipped: i64,
        error: Option<&str>,
        now: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_finish(id, state, scanned, deleted, skipped, error, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 最近作业（控制台 GET /jobs；最新在前）。列投影与 `job_get` 同值。
    pub async fn list_jobs(&self, limit: i64) -> Result<Vec<serde_json::Value>> {
        let rows = self
            .control_store()
            .recent_jobs(limit)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(rows
            .iter()
            .map(|j| {
                serde_json::json!({
                    "id": j.id,
                    "kind": j.kind,
                    "bucket": j.bucket,
                    "prefix": j.prefix,
                    "state": j.state,
                    "scanned": j.scanned,
                    "deleted": j.deleted,
                    "skipped": j.skipped,
                    "error": j.error,
                    "priority": j.priority,
                    "total": j.total,
                    "succeeded": j.succeeded,
                    "failed": j.failed,
                    "requested_by": j.requested_by,
                    "report_bucket": j.report_bucket,
                    "report_prefix": j.report_prefix,
                    "parent_job_id": j.parent_job_id,
                    "started_at": j.started_at,
                    "finished_at": j.finished_at,
                    "created_at": j.created_at,
                    "updated_at": j.updated_at,
                })
            })
            .collect())
    }

    // ——— M8 S0：作业引擎（jobs/job_tasks） ———

    /// 建作业 + 任务（同一事务）。任务 `task_no` 从 1 起；作业初始 `queued`。
    pub async fn create_job(&self, spec: &JobSpec, now: i64) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let id = crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_create(&job_insert(spec, now), &new_tasks(spec, now))
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(id)
    }

    /// 建带 dedupe 的作业：任务 `INSERT OR IGNORE`，一条都没插入（全被去重）→
    /// 整个事务回滚并返回 None（C7：不产生空作业）。
    pub async fn create_job_dedup(&self, spec: &JobSpec, now: i64) -> Result<Option<i64>> {
        let mut tx = self.pool.begin().await?;
        let id = crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_create_dedup(&job_insert(spec, now), &new_tasks(spec, now))
            .await
            .map_err(crate::control_store::to_sqlx)?;
        match id {
            Some(id) => {
                tx.commit().await?;
                Ok(Some(id))
            }
            None => {
                tx.rollback().await?;
                Ok(None)
            }
        }
    }

    /// M8 S4b：Restore 受理（单事务）——建 kind=restore 单任务作业 + upsert `tier_restores`
    /// `state='pending', expires_at=0`。返回 `(job_id, 是否新建任务)`。
    /// M8-1：`fence` 是入队者 token。请求路径（API/用户）用 `UNFENCED`，行上的 fence
    /// 只升不降（`MAX`），避免新请求把已被高 term 接管的行降回 0；执行者的状态转换
    /// 仍按 `fence_term <= token.term` 门控。
    pub async fn create_restore_job(
        &self,
        spec: &JobSpec,
        bucket: &str,
        key: &str,
        version_id: &str,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<(i64, bool)> {
        let mut tx = self.pool.begin().await?;
        let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
        let (id, inserted) = txn
            .job_create_restore(&job_insert(spec, now), &new_tasks(spec, now))
            .await
            .map_err(crate::control_store::to_sqlx)?;
        txn.tier_restore_upsert(&TierRestoreRow {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.to_string(),
            expires_at: 0,
            job_id: Some(id),
            state: "pending".into(),
            created_at: now,
            fence_term: fence.term_i64(),
            fence_leader: fence.leader_i64(),
        })
        .await
        .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok((id, inserted))
    }

    /// 某状态的 Restore 行（reaper/对账）。
    pub async fn restore_rows(&self, state: &str) -> Result<Vec<RestoreRow>> {
        Ok(self
            .control_store()
            .tier_restores(state)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .into_iter()
            .map(restore_row_of)
            .collect())
    }

    pub async fn restore_row(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<RestoreRow>> {
        Ok(self
            .control_store()
            .tier_restore(bucket, key, version_id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(restore_row_of))
    }

    /// M8-1：带 fencing token 的恢复状态转换。行已被更高 term 接管 → no-op。
    /// 单节点/M7 入口：不带 fencing token（见 [`Self::set_restore_state_fenced`]）。
    pub async fn set_restore_state(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        state: &str,
        expires_at: i64,
    ) -> Result<()> {
        self.set_restore_state_fenced(
            bucket,
            key,
            version_id,
            state,
            expires_at,
            FencingEpoch::UNFENCED,
        )
        .await
    }

    /// M8-1：行已被更高 term 接管（`fence_term > token.term`）→ no-op。
    pub async fn set_restore_state_fenced(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        state: &str,
        expires_at: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .tier_restore_set_state(
                bucket,
                key,
                version_id,
                state,
                expires_at,
                fence.term_i64(),
                fence.leader_i64(),
            )
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 单节点/M7 入口：不带 fencing token（见 [`Self::delete_restore_fenced`]）。
    pub async fn delete_restore(&self, bucket: &str, key: &str, version_id: &str) -> Result<()> {
        self.delete_restore_fenced(bucket, key, version_id, FencingEpoch::UNFENCED)
            .await
    }

    /// M8-1：行已被更高 term 接管（`fence_term > token.term`）→ 不删除（留给新 leader）。
    pub async fn delete_restore_fenced(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        fence: FencingEpoch,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .tier_restore_delete(bucket, key, version_id, fence.term_i64())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 作业是否仍有 queued/running 任务（启动对账判断是否需要重排）。
    pub async fn job_task_active(&self, job_id: i64) -> Result<bool> {
        let n = self
            .control_store()
            .active_task_count(job_id, None)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(n > 0)
    }

    /// `(该用户, 全局)` 的 inflight 作业数（queued/running/paused/preempted）。
    pub async fn job_inflight_counts(&self, requested_by: Option<i64>) -> Result<(i64, i64)> {
        self.control_store()
            .job_inflight_counts(requested_by)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 原子领取一条任务（BEGIN IMMEDIATE）。先提升 queued 作业，再按优先级/时间取一条。
    /// 单节点/M7 入口：不带 fencing token（见 [`Self::claim_task_fenced`]）。
    pub async fn claim_task(&self, now: i64, timeout_secs: i64) -> Result<Option<ClaimedTask>> {
        self.claim_task_fenced(now, timeout_secs, FencingEpoch::UNFENCED)
            .await
    }

    /// M8-1：带 fencing token 的领取。只领 `fence_term <= token.term` 的排队任务
    /// （被更高 term 接管过的行不再归旧 leader），并把 token 落到行上作为租约归属。
    pub async fn claim_task_fenced(
        &self,
        now: i64,
        timeout_secs: i64,
        fence: FencingEpoch,
    ) -> Result<Option<ClaimedTask>> {
        let mut conn = self.pool.acquire().await?;
        // 单节点领取仍需 SQLite 写锁串行化；缝只借用调用方事务，不接管 BEGIN/COMMIT。
        sqlx::raw_sql("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let out = async {
            let claim = crate::control_store::SqliteControlTxn::new(&mut conn)
                .job_claim(now, timeout_secs, fence.term_i64(), fence.leader_i64())
                .await
                .map_err(crate::control_store::to_sqlx)?;
            Ok::<Option<ClaimedTask>, sqlx::Error>(claim.map(|c| ClaimedTask {
                job_id: c.job_id,
                task_no: c.task_no,
                action: c.action,
                payload_json: c.payload_json,
                attempt: c.attempt,
            }))
        }
        .await;
        let tail = if out.is_ok() { "COMMIT" } else { "ROLLBACK" };
        if let Err(e) = sqlx::raw_sql(tail).execute(&mut *conn).await {
            tracing::warn!(error = %e, "job claim {tail} 失败");
        }
        out
    }

    /// 领取后按动作类型收紧截止时间（claim 时动作未知，只能先写两类超时的较大值）。
    /// 只改 running 行：终态/已重排任务的 deadline 不被拉回。M8-1：只改本 token
    /// 领取的行（被更高 term 接管后旧 worker 的收尾是 no-op）。
    pub async fn set_task_deadline(
        &self,
        job_id: i64,
        task_no: i64,
        deadline_at: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .task_set_deadline(job_id, task_no, deadline_at, fence.term_i64())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 作业当前状态（None = 不存在）。
    pub async fn job_state(&self, id: i64) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .job(id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|j| j.state))
    }

    /// 抢占边界回退：把已领取但**尚未执行**的 running 任务放回 queued。
    /// 不消耗重试次数（attempt 不变）、不改结果列；已执行完的动作仍是 done。
    /// 只命中 running 行，重复调用幂等（返回是否命中）。
    pub async fn requeue_task(
        &self,
        job_id: i64,
        task_no: i64,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let hit = crate::control_store::SqliteControlTxn::new(&mut tx)
            .requeue_task(job_id, task_no, now, fence.term_i64())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(hit)
    }

    /// 协作式抢占（动作边界，单节点）：高等优先级作业有**就绪**排队任务、且 worker
    /// 容量已满（running 任务数 ≥ `capacity`）时，把仍有排队任务的低优先级 running
    /// 作业标记为 `preempted`。只改作业状态——正在执行的动作不被中断，已领取未执行
    /// 的任务由 worker 在动作边界自行回退（[`Self::requeue_task`]）。返回被抢占数。
    ///
    /// M-2：抢占只在**同一 `requested_by`** 内生效（NULL 与 NULL 视为同一系统账户），
    /// 一个租户的高优先级作业不能抢占另一个租户的作业。
    /// 严格大于才抢占：同优先级按 FIFO（next_at/task_no），不互相打断。
    pub async fn preempt_lower_priority(&self, capacity: usize, now: i64) -> Result<usize> {
        if capacity == 0 {
            return Ok(0);
        }
        let mut tx = self.pool.begin().await?;
        let n = crate::control_store::SqliteControlTxn::new(&mut tx)
            .preempt_lower_priority(capacity, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(n)
    }

    /// 释放抢占：同一 `requested_by` 内高等优先级的**就绪**排队任务已清空（都已领取
    /// 或完成）后，把 `preempted` 作业放回 `queued`（下一条领取按正常优先级顺序恢复）。
    /// 返回释放数。正在运行的高优先级任务不阻塞释放——空闲容量应回到低优先级作业，
    /// 领取顺序仍保证后续高优先级任务先被取走。
    pub async fn release_preempted(&self, now: i64) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let n = crate::control_store::SqliteControlTxn::new(&mut tx)
            .release_preempted(now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(n)
    }

    /// 任务终态/重试。成功 → done；失败未达上限 → queued（指数退避）；达上限 → failed。
    /// 返回任务新状态；作业在无 queued/running 任务时收敛为 done/error。
    /// 单节点/M7 入口：不带 fencing token（见 [`Self::finish_task_fenced`]）。
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_task(
        &self,
        job_id: i64,
        task_no: i64,
        ok: bool,
        code: &str,
        message: Option<&str>,
        now: i64,
        max_attempts: i64,
    ) -> Result<&'static str> {
        self.finish_task_fenced(
            job_id,
            task_no,
            ok,
            code,
            message,
            now,
            max_attempts,
            FencingEpoch::UNFENCED,
        )
        .await
    }

    /// 任务失败后的重试退避（秒）：`10 * 2^attempt` 封顶 300，再加 0..9s 确定性抖动。
    /// `now` 是 Unix 秒；旧实现用 `now % 1000`，实际抖动是 0..999 **秒**，默认 3 次
    /// 重试可能拖到半小时。抖动只依赖 (job_id, task_no, now, attempt)，重放结果一致。
    pub(crate) fn retry_backoff_secs(attempt: i64, now: i64, job_id: i64, task_no: i64) -> i64 {
        let base = 10i64
            .saturating_mul(2i64.saturating_pow(attempt.min(30) as u32))
            .min(300);
        let mut h = (job_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (task_no as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
            ^ (now as u64).wrapping_mul(0x1656_67B1_9E37_79F9)
            ^ (attempt as u64);
        h ^= h >> 33;
        h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        h ^= h >> 33;
        base + (h % 10) as i64
    }

    /// M8-1：带 fencing token 的收尾。行已被更高 term 接管（`fence_term > token.term`）
    /// → `Ok("fenced")` 且不做任何写入；调用方的动作结果丢弃，任务留 running 交给
    /// 新 leader 的过期/接管回收（at-least-once）。
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_task_fenced(
        &self,
        job_id: i64,
        task_no: i64,
        ok: bool,
        code: &str,
        message: Option<&str>,
        now: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<&'static str> {
        let mut tx = self.pool.begin().await?;
        let out = crate::control_store::SqliteControlTxn::new(&mut tx)
            .finish_task(
                job_id,
                task_no,
                ok,
                code,
                message,
                now,
                max_attempts,
                fence.term_i64(),
            )
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(out)
    }

    /// 超时回收：running 且 deadline 过期 → 按一次失败处理。返回回收条数。
    /// M8-1：接管时把行重标为当前 token，旧 holder 的迟到 finish 因此被拒。
    pub async fn recover_expired_tasks(
        &self,
        now: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<usize> {
        let rows = self
            .control_store()
            .recover_expired_task_ids(now, fence.term_i64())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        for (job_id, task_no) in &rows {
            self.take_over_task(*job_id, *task_no, None, now, fence)
                .await?;
            self.finish_task_fenced(
                *job_id,
                *task_no,
                false,
                "Timeout",
                Some("task deadline exceeded"),
                now,
                max_attempts,
                fence,
            )
            .await?;
        }
        Ok(rows.len())
    }

    /// M8-1：把一条 running 任务重标为当前 token（接管租约）。`started_before` 为 Some 时
    /// 只接管 `started_at <` 阈值的行（启动崩溃回收）；None = 只看 deadline 已过的行。
    async fn take_over_task(
        &self,
        job_id: i64,
        task_no: i64,
        started_before: Option<i64>,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .take_over_task(
                job_id,
                task_no,
                started_before,
                now,
                fence.term_i64(),
                fence.leader_i64(),
            )
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 4.11/M2：回收「state='running' 但没有任何 job_tasks」的作业行。
    /// lifecycle/inventory 等单条作业只写 jobs 行；进程在 job_start 之后崩溃会永久停在
    /// running。`cutoff` 限定 updated_at（长作业靠心跳续命，避免把仍在跑的扫描误标 error）。
    pub async fn recover_orphan_jobs(&self, cutoff: i64, now: i64) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let n = crate::control_store::SqliteControlTxn::new(&mut tx)
            .recover_orphan_jobs(cutoff, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(n)
    }

    /// M2：长作业心跳（只续 running 行），让 orphan 回收按 liveness 而非创建时间判断。
    pub async fn job_heartbeat(&self, job_id: i64, now: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_heartbeat(job_id, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 启动恢复：running 且 `started_at < cutoff`（进程崩溃遗留）按一次失败处理。
    /// M8-1：同样先接管 token 再收尾（旧进程若还活着，其 finish 会被拒）。
    pub async fn recover_stale_tasks(
        &self,
        cutoff: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<usize> {
        let rows = self
            .control_store()
            .recover_stale_task_ids(cutoff, fence.term_i64())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        let now = crate::db::now();
        for (job_id, task_no) in &rows {
            self.take_over_task(*job_id, *task_no, Some(cutoff), now, fence)
                .await?;
            self.finish_task_fenced(
                *job_id,
                *task_no,
                false,
                "Timeout",
                Some("process restart recovery"),
                now,
                max_attempts,
                fence,
            )
            .await?;
        }
        Ok(rows.len())
    }

    /// 作业详情（None = 不存在）。
    pub async fn job_get(&self, id: i64) -> Result<Option<serde_json::Value>> {
        let row = self
            .control_store()
            .job(id)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(row.map(|j| {
            serde_json::json!({
                "id": j.id,
                "kind": j.kind,
                "bucket": j.bucket,
                "prefix": j.prefix,
                "state": j.state,
                "spec_json": j.spec_json,
                "progress": j.progress,
                "total": j.total,
                "succeeded": j.succeeded,
                "failed": j.failed,
                "report_bucket": j.report_bucket,
                "report_prefix": j.report_prefix,
                "priority": j.priority,
                "requested_by": j.requested_by,
                "parent_job_id": j.parent_job_id,
                "error": j.error,
                "created_at": j.created_at,
                "started_at": j.started_at,
                "finished_at": j.finished_at,
                "updated_at": j.updated_at,
            })
        }))
    }

    /// 作业级错误文案（报告写失败等；不改任务终态）。
    pub async fn job_set_error(&self, id: i64, msg: &str, now: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_set_error(id, msg, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 作业归属/状态（API 判权用）。
    pub async fn job_owner(&self, id: i64) -> Result<Option<(Option<i64>, String)>> {
        Ok(self
            .control_store()
            .job(id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|j| (j.requested_by, j.state)))
    }

    /// 任务分页（task_no 升序）。
    pub async fn job_tasks(
        &self,
        id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>> {
        let rows = self
            .control_store()
            .job_tasks_page(id, offset.max(0), limit.clamp(1, 500))
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(rows
            .iter()
            .map(|t| {
                serde_json::json!({
                    "task_no": t.task_no,
                    "action": t.action,
                    "state": t.state,
                    "attempt": t.attempt,
                    "next_at": t.next_at,
                    "result_code": t.result_code,
                    "result_string": t.result_string,
                    "started_at": t.started_at,
                    "finished_at": t.finished_at,
                })
            })
            .collect())
    }

    /// 状态操作：`pause`/`resume`/`cancel`。返回是否有行被改（false = 不存在或状态不允许）。
    pub async fn job_set_state(&self, id: i64, action: &str, now: i64) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let hit = crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_set_state(id, action, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(hit)
    }

    /// 失败任务重试：建子作业（只含 failed 任务，原 task_no 保留为顺序），返回新 job id。
    pub async fn job_retry_failed(&self, id: i64, now: i64) -> Result<Option<i64>> {
        let mut tx = self.pool.begin().await?;
        let new_id = crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_retry_failed(id, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(new_id)
    }

    /// 报告行（含 `payload_json`）：S3 Batch 报告需要从载荷回读 bucket/key/versionId。
    /// 与 [`Self::job_tasks`] 分列——控制台分页不暴露内部载荷。
    pub async fn job_report_rows(&self, id: i64) -> Result<Vec<serde_json::Value>> {
        let rows = self
            .control_store()
            .job_tasks(id)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(rows
            .iter()
            .map(|t| {
                serde_json::json!({
                    "task_no": t.task_no,
                    "payload_json": t.payload_json,
                    "result_code": t.result_code,
                    "result_string": t.result_string,
                    "started_at": t.started_at,
                    "finished_at": t.finished_at,
                })
            })
            .collect())
    }

    /// 作业历史保留（feature 6）：`cutoff` 之前进入终态、按 id 升序的作业 id（最多
    /// `limit` 条）。只返回终态，调用方无需再筛 queued/running/preempted。
    pub async fn terminal_jobs_before(&self, cutoff: i64, limit: i64) -> Result<Vec<i64>> {
        self.control_store()
            .terminal_jobs_before(cutoff, limit)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 单机保留清理：一个事务内删除终态作业历史，返回删除的 jobs 行数。
    pub async fn purge_terminal_jobs_local(&self, ids: &[i64]) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let (deleted, _) = {
            let mut cw = crate::raft::apply::control_tx(&mut tx);
            Self::purge_terminal_jobs_raw(&mut cw, ids).await?
        };
        tx.commit().await?;
        Ok(deleted)
    }

    /// 保留清理的共享 SQL（standalone 事务 / Raft apply 事务）。只删除当前仍为终态的
    /// jobs 及其 job_tasks；先解除 inventory_runs/tier_restores/`parent_job_id` 的引用，
    /// 否则 `RunState::validate` 会因悬空引用拒绝之后的快照安装。返回 (删除行数,
    /// 存在但非终态的 id 数)；非终态行永不删除。
    pub(crate) async fn purge_terminal_jobs_raw(
        cw: &mut ControlWrite<'_>,
        ids: &[i64],
    ) -> Result<(u64, usize)> {
        crate::control_store::SqliteControlTxn::new(cw.conn())
            .purge_terminal_jobs(ids)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    // ——— M8 S5：Inventory 配置与运行行 ———

    pub async fn inventory_config_count(&self, bucket: &str) -> Result<i64> {
        self.control_store()
            .inventory_config_count(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    pub async fn inventory_get_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<InventoryConfigView>> {
        self.control_store()
            .inventory_config(bucket, id)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    pub async fn inventory_list_configs(&self, bucket: &str) -> Result<Vec<InventoryConfigView>> {
        self.control_store()
            .inventory_configs(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 生成器用：全部桶的配置（ORDER BY bucket, id 保证调度确定性）。
    pub async fn inventory_list_all_configs(&self) -> Result<Vec<InventoryConfigView>> {
        self.control_store()
            .inventory_configs_all()
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// 单机门面保留数字 uid 签名（决策 7）：0 → Admin；uid 必须存在，否则 RowNotFound。
    pub async fn inventory_upsert_config(
        &self,
        bucket: &str,
        id: &str,
        owner_id: i64,
        config_json: &str,
        now: i64,
    ) -> Result<()> {
        let row = InventoryConfigEntry {
            bucket: bucket.to_string(),
            id: id.to_string(),
            owner: self.owner_ref(owner_id).await?,
            config_json: config_json.to_string(),
            updated_at: now,
        };
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .inventory_config_upsert(&row)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn inventory_delete_config(&self, bucket: &str, id: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let deleted = crate::control_store::SqliteControlTxn::new(&mut tx)
            .inventory_config_delete(bucket, id)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(deleted)
    }

    /// 3.6：Inventory 增量模式（默认全量；本地表，不进 Raft）。
    pub async fn inventory_incremental_get(&self, bucket: &str, config_id: &str) -> Result<bool> {
        Ok(self
            .control_store()
            .inventory_incremental(bucket, config_id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .is_some_and(|r| r.enabled))
    }

    /// 3.6：设置增量模式；关闭时连旧水位一起清掉（重新开启先跑全量）。
    pub async fn inventory_incremental_set(
        &self,
        bucket: &str,
        config_id: &str,
        enabled: bool,
        now: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            if enabled {
                txn.inventory_incremental_upsert(&InventoryIncrementalRow {
                    bucket: bucket.to_string(),
                    config_id: config_id.to_string(),
                    enabled: true,
                    updated_at: now,
                })
                .await
                .map_err(crate::control_store::to_sqlx)?;
            } else {
                txn.inventory_incremental_delete(bucket, config_id)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
                txn.inventory_cursor_delete(bucket, config_id, None)
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// 3.6：读取增量水位；`config_updated_at` 不匹配（配置已改）→ None，从头全量。
    pub async fn inventory_cursor_get(
        &self,
        bucket: &str,
        config_id: &str,
        config_updated_at: i64,
    ) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .inventory_cursor(bucket, config_id)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .filter(|r| r.config_updated_at == config_updated_at)
            .map(|r| r.cursor))
    }

    /// 3.6：写入/清除增量水位（`None` → 删除，回到全量起点）。
    /// M8-1：行已被更高 term 接管时旧 leader 的迟到写入是 no-op。
    pub async fn inventory_cursor_set(
        &self,
        bucket: &str,
        config_id: &str,
        config_updated_at: i64,
        cursor: Option<&str>,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            match cursor {
                Some(c) if !c.is_empty() => {
                    txn.inventory_cursor_upsert(&InventoryCursorRow {
                        bucket: bucket.to_string(),
                        config_id: config_id.to_string(),
                        cursor: c.to_string(),
                        config_updated_at,
                        updated_at: now,
                        fence_term: fence.term_i64(),
                        fence_leader: fence.leader_i64(),
                    })
                    .await
                    .map_err(crate::control_store::to_sqlx)?;
                }
                _ => {
                    txn.inventory_cursor_delete(bucket, config_id, Some(fence.term_i64()))
                        .await
                        .map_err(crate::control_store::to_sqlx)?;
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// 建运行行；`(bucket, config_id, run_key)` 已存在 → None（同槽去重）。
    /// M8-1：带调度者 token（新行归当前 leader；旧 term 的执行器不能改其状态）。
    pub async fn inventory_create_run(
        &self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<Option<i64>> {
        let mut tx = self.pool.begin().await?;
        let id = crate::control_store::SqliteControlTxn::new(&mut tx)
            .inventory_run_create(
                bucket,
                config_id,
                run_key,
                now,
                fence.term_i64(),
                fence.leader_i64(),
            )
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn inventory_run_set_job(
        &self,
        run_id: i64,
        job_id: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .inventory_run_set_job(run_id, job_id, fence.term_i64(), fence.leader_i64())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 执行器开始跑时写实际开始时刻（槽时间只体现在 run_key）。接管旧 term 的行时
    /// 重标 token，旧执行器的后续转换因此被拒。
    pub async fn inventory_run_started(
        &self,
        run_id: i64,
        now: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        // `started_at` 只在首次执行时写；更高 term 的重试要重标 token，旧执行器的收尾
        // 才会被 `fence_term <= ?` 拒绝。
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .inventory_run_started(run_id, now, fence.term_i64(), fence.leader_i64())
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn inventory_run_by_key(
        &self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> Result<Option<InventoryRunRow>> {
        self.control_store()
            .inventory_run_by_key(bucket, config_id, run_key)
            .await
            .map_err(crate::control_store::to_sqlx)
    }

    /// M8-1：行已被更高 term 接管 → no-op（旧 leader 的迟到完成不覆盖新状态）。
    pub async fn inventory_run_finish(
        &self,
        run_id: i64,
        state: &str,
        manifest_key: Option<&str>,
        error: Option<&str>,
        fence: FencingEpoch,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .inventory_run_finish(
                run_id,
                state,
                manifest_key,
                error,
                fence.term_i64(),
                fence.leader_i64(),
            )
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// Inventory 作业的 `progress` = 已扫描对象数（任务收尾时引擎会 +1，调用方存 n-1）。
    pub async fn job_set_progress(&self, job_id: i64, progress: i64, now: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .job_set_progress(job_id, progress, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    // ——— M4 切片 7：桶策略与审计 ———

    /// 桶 DB 行存在性（控制面写前置校验：防「给不存在桶预置策略，同名桶日后创建即继承」
    /// ——S6-1 同类边角，verifier 非阻断 3）。盘上有、表里无的历史桶由启动回填补齐。
    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        Ok(self
            .control_store()
            .bucket(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .is_some())
    }

    /// 桶策略原文（None = 未配）。
    pub async fn bucket_policy(&self, bucket: &str) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .bucket_policy(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|p| p.policy_json))
    }

    /// 桶事件通知配置原文（None = 未配）。
    pub async fn bucket_notification(&self, bucket: &str) -> Result<Option<String>> {
        Ok(self
            .control_store()
            .bucket_notification(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?
            .map(|e| e.config_json))
    }

    /// 写桶策略（全量替换）。
    pub async fn set_bucket_policy(&self, bucket: &str, json: &str, now: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .bucket_policy_set(bucket, json, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 清桶策略（删桶时连同清理——桶名可复用，S6-1 同款语义）。
    pub async fn clear_bucket_policy(&self, bucket: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .bucket_policy_delete(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 写桶事件通知配置（全量替换）。
    pub async fn set_bucket_notification(
        &self,
        bucket: &str,
        config_json: &str,
        now: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .bucket_notification_set(bucket, config_json, now)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// 清桶事件通知配置（幂等）。
    pub async fn clear_bucket_notification(&self, bucket: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        crate::control_store::SqliteControlTxn::new(&mut tx)
            .bucket_notification_delete(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        tx.commit().await?;
        Ok(())
    }

    /// C5：审计保留策略——删除 ts < cutoff 的行，返回删除数（worker 按 OSS_AUDIT_RETENTION_DAYS）。
    /// 每批最多 `limit` 行（id 子查询圈定），大积压时不让单条 DELETE 长时间占写锁。
    pub async fn delete_audit_older_than(&self, cutoff: i64, limit: i64) -> Result<u64> {
        use xl_store::control_store::audit::AuditTxn;

        let mut conn = self.pool.acquire().await?;
        let n = crate::control_store::SqliteControlTxn::new(&mut conn)
            .audit_delete_older_than(cutoff, limit)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(n)
    }

    /// 审计查询（控制台；可选 decision/时间窗/uid/op/bucket/key 前缀过滤；最新在前）。
    /// 动态 WHERE 只拼**固定片段**，用户值一律绑定参数。
    pub async fn list_audit(&self, f: &AuditFilter<'_>) -> Result<Vec<serde_json::Value>> {
        use xl_store::control_store::audit::{AuditQuery, AuditStore};

        let query = AuditQuery {
            limit: f.limit,
            decision: f.decision.map(str::to_owned),
            from: f.from,
            to: f.to,
            uid: f.uid,
            op: f.op.map(str::to_owned),
            bucket: f.bucket.map(str::to_owned),
            key_prefix: f.key_prefix.map(str::to_owned),
        };
        let rows = crate::control_store::SqliteControlStore::new(self.pool.clone())
            .audit_page(&query)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        Ok(rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.id,
                    "ts": r.ts,
                    "uid": r.uid,
                    "op": r.op,
                    "bucket": r.bucket,
                    "object_key": r.object_key,
                    "decision": r.decision,
                    "detail": r.detail,
                })
            })
            .collect())
    }
}

// ——— M9 S6a：命令型原始写入 ———
//
// apply（以及 M7 的 ControlPlane::Local）在一事务内调用；随机值/时间戳/hash 一律由调用方
// 预先算好传入。参数带 `ControlWrite` 令牌：令牌只能由 `raft::apply::control_tx` 构造，
// handler 无法直接拿到这些写入口（matrix §9 编译闸）。

/// 建桶行写入结果（console 建桶的配额最终判定）。
pub enum BucketRowOutcome {
    Written,
    QuotaExceeded,
}

impl Db {
    /// 全列建桶（S6c 的 `db`/`access`/`console` 共用 writer）：
    /// 行不存在 → INSERT（set_id=0，deleting=0）；同名同模式 → 全列收敛但**不写 set_id**；
    /// 同名不同模式 → `ModeConflict`（无写入）。
    #[allow(clippy::too_many_arguments)]
    /// 删桶级联：所有从属行（含 level-4/5 复制的 inventory_runs/tier_restores）都在
    /// 同一条日志/事务内删除；各副本 apply 同一命令，不得由服务节点本地直删。
    /// 返回桶行是否存在（不存在 → NotFound，但仍清残留从属行，幂等）。
    #[allow(dead_code)] // Direct-apply SQLite façade retained for tests/migration until H1.
    pub(crate) async fn delete_bucket_cascade(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
    ) -> Result<bool> {
        let conn = cw.conn();
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut *conn);
            txn.delete_access_points_for_bucket(bucket)
                .await
                .map_err(crate::control_store::to_sqlx)?;
            txn.delete_lifecycle_inventory_tier_for_bucket(bucket)
                .await
                .map_err(crate::control_store::to_sqlx)?;
        }
        let mut txn = crate::control_store::SqliteControlTxn::new(&mut *conn);
        txn.permissions_delete_for_bucket(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        txn.delete_bucket_configs_for_bucket(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)?;
        txn.bucket_delete(bucket)
            .await
            .map_err(crate::control_store::to_sqlx)
    }
}

/// 审计查询的过滤条件（`Db::list_audit`）；`key_prefix` 为 object_key 前缀，其余精确匹配。
#[derive(Debug)]
pub struct AuditFilter<'a> {
    pub limit: i64,
    pub decision: Option<&'a str>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub uid: Option<i64>,
    pub op: Option<&'a str>,
    pub bucket: Option<&'a str>,
    pub key_prefix: Option<&'a str>,
}

#[cfg(test)]
mod bucket_set_tests {
    use super::*;

    /// P3-3：回填对账只动 set_id——owner 保留；缺行时插入。
    #[tokio::test]
    async fn upsert_bucket_set_reconciles_without_clobbering_owner() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let uid = db.create_user("u1", "pw-123456", false).await.unwrap();
        db.insert_bucket_set("bk", Some(uid), 0).await.unwrap();
        // 权威解析 = set1 → 对账覆盖
        db.upsert_bucket_set("bk", 1).await.unwrap();
        let row = sqlx::query("SELECT owner_id, set_id FROM buckets WHERE name = 'bk'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(
            row.get::<Option<i64>, _>("owner_id"),
            Some(uid),
            "owner 不得被对账清掉"
        );
        assert_eq!(row.get::<i64, _>("set_id"), 1, "set_id 应收敛到权威值");
        // 缺行：对账插入 owner NULL
        db.upsert_bucket_set("bk2", 2).await.unwrap();
        let row = sqlx::query("SELECT owner_id, set_id FROM buckets WHERE name = 'bk2'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<Option<i64>, _>("owner_id"), None);
        assert_eq!(row.get::<i64, _>("set_id"), 2);
    }

    /// M5 切片 0（verifier 阻断 1 修复）：重建覆盖——陈旧行（旧 owner/旧 set）被新 owner/set
    /// 覆盖；缺行则插入。旧实现 INSERT OR IGNORE 在陈旧行上整条 no-op，本节点会按旧 set 路由、
    /// 按旧 owner 判权，与已收广播的 peer 静默分叉。
    #[tokio::test]
    async fn upsert_bucket_owner_set_overrides_stale_row() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let o1 = db.create_user("o1", "pw-123456", false).await.unwrap();
        let o2 = db.create_user("o2", "pw-123456", false).await.unwrap();
        // 删桶记账曾被容忍失败：行残留（旧 owner o1、旧 set0）
        db.insert_bucket_set("bk", Some(o1), 0).await.unwrap();
        db.upsert_bucket_owner_set("bk", Some(o2), 1).await.unwrap();
        let row = sqlx::query("SELECT owner_id, set_id FROM buckets WHERE name = 'bk'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(
            row.get::<Option<i64>, _>("owner_id"),
            Some(o2),
            "owner 应换新"
        );
        assert_eq!(row.get::<i64, _>("set_id"), 1, "set 应收敛");
        // 缺行：插入
        db.upsert_bucket_owner_set("bk2", Some(o1), 2)
            .await
            .unwrap();
        let row = sqlx::query("SELECT owner_id, set_id FROM buckets WHERE name = 'bk2'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<Option<i64>, _>("owner_id"), Some(o1));
        assert_eq!(row.get::<i64, _>("set_id"), 2);
    }

    /// C5：审计保留——只删窗口外旧行。
    #[tokio::test]
    async fn audit_retention_deletes_only_old_rows() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let now = now();
        for (ts, dec) in [(1i64, "allow"), (now, "deny")] {
            sqlx::query(
                "INSERT INTO audit_log (ts, uid, op, decision) VALUES (?, 1, 'GetObject', ?)",
            )
            .bind(ts)
            .bind(dec)
            .execute(db.pool())
            .await
            .unwrap();
        }
        let n = db.delete_audit_older_than(now - 100, 10).await.unwrap();
        assert_eq!(n, 1, "只删窗口外旧行");
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(left, 1);

        // 批次上限：一次最多 limit 行（积压需多轮收敛）
        for ts in [now - 300, now - 200] {
            sqlx::query(
                "INSERT INTO audit_log (ts, uid, op, decision) VALUES (?, 1, 'GetObject', 'deny')",
            )
            .bind(ts)
            .execute(db.pool())
            .await
            .unwrap();
        }
        let first = db.delete_audit_older_than(now - 100, 1).await.unwrap();
        assert_eq!(first, 1, "每批上限 1 行");
        let rest = db.delete_audit_older_than(now - 100, 10).await.unwrap();
        assert_eq!(rest, 1, "下一轮清掉剩余旧行");
    }
}

#[cfg(test)]
mod argon2_tests {
    use super::*;

    async fn db() -> Db {
        Db::connect("sqlite::memory:").await.unwrap()
    }

    /// M4 切片 7：Argon2id 新哈希 + 旧格式透明升级 + dummy 串可解析。
    #[tokio::test]
    async fn argon2_hashing_with_legacy_upgrade() {
        let db = db().await;
        // 新哈希：Argon2id 且可验证
        db.create_user("u1", "pw-123456", false).await.unwrap();
        let (h1,): (String,) =
            sqlx::query_as("SELECT password_hash FROM users WHERE username = 'u1'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(h1.starts_with("$argon2"), "新哈希必须是 Argon2id：{h1}");
        assert!(crate::db::verify_password("pw-123456", &h1));
        assert!(!crate::db::verify_password("wrong", &h1));

        // 旧格式：直插 legacy 哈希 → 登录成功并透明升级；错口令仍拒
        let legacy = legacy_hash("old-pw-123");
        sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, created_at) \
             VALUES ('legacy', ?, 0, 1)",
        )
        .bind(&legacy)
        .execute(db.pool())
        .await
        .unwrap();
        assert!(
            db.verify_user("legacy", "old-pw-123")
                .await
                .unwrap()
                .is_some()
        );
        let (h2,): (String,) =
            sqlx::query_as("SELECT password_hash FROM users WHERE username = 'legacy'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(
            h2.starts_with("$argon2"),
            "登录成功应透明升级为 Argon2：{h2}"
        );
        assert!(db.verify_user("legacy", "nope").await.unwrap().is_none());

        // dummy 串（oracle 收口）必须可解析
        assert!(
            argon2::password_hash::PasswordHash::new(DUMMY_ARGON2).is_ok(),
            "DUMMY_ARGON2 PHC 解析失败会让未命中路径的时延拉平失效"
        );
    }

    /// H1：Raft 模式禁止登录时重写旧哈希（随机盐会让各站点分叉）。
    #[tokio::test]
    async fn raft_mode_verify_user_keeps_legacy_hash() {
        let db = db().await;
        db.prepare_raft_mode(true).await.unwrap();
        assert!(db.raft_mode());
        let legacy = legacy_hash("old-pw-123");
        db.set_raft_apply_ctx(true).await.unwrap();
        sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, created_at) \
             VALUES ('legacy', ?, 0, 1)",
        )
        .bind(&legacy)
        .execute(db.pool())
        .await
        .unwrap();
        db.set_raft_apply_ctx(false).await.unwrap();

        assert!(
            db.verify_user("legacy", "old-pw-123")
                .await
                .unwrap()
                .is_some()
        );
        let stored: String =
            sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'legacy'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(stored, legacy, "Raft 模式不得本地重写旧口令哈希");
    }

    /// 保留清理查询走 0009 的 `(state, updated_at)` 覆盖索引（不满表扫）。
    #[tokio::test]
    async fn terminal_jobs_before_uses_retention_index() {
        let db = db().await;
        let rows = sqlx::query(
            "EXPLAIN QUERY PLAN SELECT id FROM jobs WHERE state IN ('done','error','cancelled') \
             AND updated_at < ? ORDER BY id LIMIT ?",
        )
        .bind(500i64)
        .bind(10i64)
        .fetch_all(db.pool())
        .await
        .unwrap();
        let plan: Vec<String> = rows.iter().map(|r| r.get::<String, _>("detail")).collect();
        assert!(
            plan.iter().any(|d| d.contains("idx_jobs_state_updated_at")),
            "terminal_jobs_before 未使用保留索引：{plan:?}"
        );
    }

    /// Level 6 独立模式保留：只按 cutoff 选终态行，limit 生效；本地事务先删
    /// task 再删 job，非终态行与其 task 永不受影响；重复清理幂等。
    #[tokio::test]
    async fn purge_terminal_jobs_local_removes_only_old_terminal_rows() {
        let db = db().await;
        for (id, state, updated_at) in [
            (1, "done", 100),
            (2, "error", 100),
            (3, "cancelled", 100),
            (4, "running", 100),
            (5, "done", 900),
            (6, "queued", 100),
        ] {
            sqlx::query(
                "INSERT INTO jobs (id, kind, state, created_at, updated_at) \
                 VALUES (?, 'lifecycle', ?, ?, ?)",
            )
            .bind(id)
            .bind(state)
            .bind(updated_at)
            .bind(updated_at)
            .execute(db.pool())
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO job_tasks (job_id, task_no, action, payload_json, state) \
                 VALUES (?, 1, 'a', '{}', 'done')",
            )
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
        }

        assert_eq!(
            db.terminal_jobs_before(500, 10).await.unwrap(),
            vec![1, 2, 3],
            "只选 cutoff 之前的终态行"
        );
        assert_eq!(
            db.terminal_jobs_before(500, 2).await.unwrap(),
            vec![1, 2],
            "LIMIT 生效"
        );
        assert_eq!(db.purge_terminal_jobs_local(&[4, 6]).await.unwrap(), 0);
        let ids = db.terminal_jobs_before(500, 10).await.unwrap();
        assert_eq!(db.purge_terminal_jobs_local(&ids).await.unwrap(), 3);
        assert_eq!(db.purge_terminal_jobs_local(&ids).await.unwrap(), 0);

        let jobs: Vec<i64> = sqlx::query_scalar("SELECT id FROM jobs ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(jobs, vec![4, 5, 6]);
        let task_jobs: Vec<i64> =
            sqlx::query_scalar("SELECT DISTINCT job_id FROM job_tasks ORDER BY job_id")
                .fetch_all(db.pool())
                .await
                .unwrap();
        assert_eq!(task_jobs, vec![4, 5, 6]);
    }

    async fn seed_cascade_rows(db: &Db, bucket: &str, owner: i64) {
        sqlx::query(
            "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) \
             VALUES (?, 'cfg', ?, '{}', 1)",
        )
        .bind(bucket)
        .bind(owner)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO inventory_runs (id, bucket, config_id, run_key, started_at, state, \
             created_at, fence_term, fence_leader) \
             VALUES (abs(random()) % 1000000, ?, 'cfg', 'k', 0, 'running', 1, 0, 0)",
        )
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO inventory_cursors (bucket, config_id, cursor, config_updated_at, \
             updated_at, fence_term, fence_leader) VALUES (?, 'cfg', '1', 1, 1, 0, 0)",
        )
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
             VALUES (?, 'cfg', 1, 1)",
        )
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO tier_restores (bucket, key, version_id, expires_at, job_id, state, \
             created_at, fence_term, fence_leader) VALUES (?, 'k', 'v', 0, NULL, 'restored', 1, 0, 0)",
        )
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at, rule_id) \
             VALUES (?, 'p', 1, 1, 1, 'r')",
        )
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at, \
             fence_term, fence_leader) VALUES (?, 'r', 'scan', '1', 1, 0, 0)",
        )
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
        for sql in [
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, '{}', 1)",
            "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, '[]', 1)",
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, '[]', 1)",
        ] {
            sqlx::query(sql)
                .bind(bucket)
                .execute(db.pool())
                .await
                .unwrap();
        }
        sqlx::query(
            "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
             VALUES (?, ?, 1, 1, 1)",
        )
        .bind(owner)
        .bind(bucket)
        .execute(db.pool())
        .await
        .unwrap();
        db.create_access_point(&format!("ap-{bucket}"), bucket, owner, None, 1)
            .await
            .unwrap();
        db.set_olap_config(
            &format!("ap-{bucket}"),
            "http",
            Some("https://x"),
            None,
            None,
            5000,
            1024,
            1,
        )
        .await
        .unwrap();
    }

    async fn count_bucket_rows(db: &Db, table: &str, bucket: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {table} WHERE bucket = ?"
        )))
        .bind(bucket)
        .fetch_one(db.pool())
        .await
        .unwrap()
    }

    /// Task 7：clear_bucket_m8_tx 删 b1 的 7 张表 + AP/OLAP，b2 原样。
    #[tokio::test]
    async fn cascade_deletes_all_seven_tables_and_spares_other_bucket() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let root = db.create_user("root", "pw", true).await.unwrap();
        seed_cascade_rows(&db, "b1", root).await;
        seed_cascade_rows(&db, "b2", root).await;

        let mut conn = db.pool().acquire().await.unwrap();
        Db::clear_bucket_m8_tx(&mut conn, "b1").await.unwrap();
        drop(conn);

        for table in [
            "inventory_runs",
            "inventory_configs",
            "inventory_cursors",
            "inventory_incremental",
            "tier_restores",
            "lifecycle_rules",
            "lifecycle_cursors",
        ] {
            assert_eq!(count_bucket_rows(&db, table, "b1").await, 0, "{table} b1");
            assert_eq!(count_bucket_rows(&db, table, "b2").await, 1, "{table} b2");
        }
        let store = db.control_store();
        assert!(store.access_point("ap-b1").await.unwrap().is_none());
        assert!(store.olap_config("ap-b1").await.unwrap().is_none());
        assert!(store.access_point("ap-b2").await.unwrap().is_some());
        assert!(store.olap_config("ap-b2").await.unwrap().is_some());
    }

    /// Task 7：delete_bucket_cascade 覆盖 13 张表，桶行最后删除，其他桶原样。
    #[tokio::test]
    async fn cascade_deletes_all_13_tables_and_spares_other_bucket() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        let root = db.create_user("root", "pw", true).await.unwrap();
        for name in ["b1", "b2"] {
            sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES (?, ?, 1)")
                .bind(name)
                .bind(root)
                .execute(db.pool())
                .await
                .unwrap();
        }
        seed_cascade_rows(&db, "b1", root).await;
        seed_cascade_rows(&db, "b2", root).await;

        let mut tx = db.pool().begin().await.unwrap();
        {
            let mut cw = crate::raft::apply::control_tx(&mut tx);
            assert!(Db::delete_bucket_cascade(&mut cw, "b1").await.unwrap());
            assert!(!Db::delete_bucket_cascade(&mut cw, "b1").await.unwrap());
        }
        tx.commit().await.unwrap();

        for table in [
            "inventory_runs",
            "inventory_configs",
            "inventory_cursors",
            "inventory_incremental",
            "tier_restores",
            "lifecycle_rules",
            "lifecycle_cursors",
            "permissions",
            "bucket_policies",
            "event_configs",
            "replication_rules",
        ] {
            assert_eq!(count_bucket_rows(&db, table, "b1").await, 0, "{table} b1");
            assert_eq!(count_bucket_rows(&db, table, "b2").await, 1, "{table} b2");
        }
        let store = db.control_store();
        assert!(store.access_point("ap-b1").await.unwrap().is_none());
        assert!(store.olap_config("ap-b1").await.unwrap().is_none());
        assert!(store.access_point("ap-b2").await.unwrap().is_some());
        assert!(store.olap_config("ap-b2").await.unwrap().is_some());
        let names: Vec<String> = sqlx::query_scalar("SELECT name FROM buckets ORDER BY name")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(names, ["b2"]);
    }

    /// Task 8：现级联与旧 SQL 参考实现的最终状态逐表一致。
    #[tokio::test]
    async fn cascade_matches_legacy_reference() {
        let a = Db::connect("sqlite::memory:").await.unwrap();
        let b = Db::connect("sqlite::memory:").await.unwrap();
        for db in [&a, &b] {
            let root = db.create_user("root", "pw", true).await.unwrap();
            for name in ["b1", "b2"] {
                sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES (?, ?, 1)")
                    .bind(name)
                    .bind(root)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            seed_cascade_rows(db, "b1", root).await;
            seed_cascade_rows(db, "b2", root).await;
        }

        let mut conn = a.pool().acquire().await.unwrap();
        sqlx::query(
            "DELETE FROM olap_configs WHERE access_point IN \
             (SELECT name FROM access_points WHERE bucket = ?)",
        )
        .bind("b1")
        .execute(&mut *conn)
        .await
        .unwrap();
        for sql in [
            "DELETE FROM access_points WHERE bucket = ?",
            "DELETE FROM inventory_runs WHERE bucket = ?",
            "DELETE FROM inventory_configs WHERE bucket = ?",
            "DELETE FROM inventory_cursors WHERE bucket = ?",
            "DELETE FROM inventory_incremental WHERE bucket = ?",
            "DELETE FROM tier_restores WHERE bucket = ?",
            "DELETE FROM lifecycle_rules WHERE bucket = ?",
            "DELETE FROM lifecycle_cursors WHERE bucket = ?",
            "DELETE FROM permissions WHERE bucket = ?",
            "DELETE FROM bucket_policies WHERE bucket = ?",
            "DELETE FROM event_configs WHERE bucket = ?",
            "DELETE FROM replication_rules WHERE bucket = ?",
            "DELETE FROM buckets WHERE name = ?",
        ] {
            sqlx::query(sql)
                .bind("b1")
                .execute(&mut *conn)
                .await
                .unwrap();
        }
        drop(conn);

        let mut tx = b.pool().begin().await.unwrap();
        {
            let mut cw = crate::raft::apply::control_tx(&mut tx);
            assert!(Db::delete_bucket_cascade(&mut cw, "b1").await.unwrap());
        }
        tx.commit().await.unwrap();

        for table in [
            "inventory_runs",
            "inventory_configs",
            "inventory_cursors",
            "inventory_incremental",
            "tier_restores",
            "lifecycle_rules",
            "lifecycle_cursors",
            "permissions",
            "bucket_policies",
            "event_configs",
            "replication_rules",
        ] {
            for bucket in ["b1", "b2"] {
                assert_eq!(
                    count_bucket_rows(&a, table, bucket).await,
                    count_bucket_rows(&b, table, bucket).await,
                    "{table} {bucket}"
                );
            }
        }
        for db in [&a, &b] {
            let names: Vec<String> = sqlx::query_scalar("SELECT name FROM buckets ORDER BY name")
                .fetch_all(db.pool())
                .await
                .unwrap();
            assert_eq!(names, ["b2"]);
            for (sql, expect) in [
                ("SELECT COUNT(*) FROM access_points WHERE bucket = 'b1'", 0),
                ("SELECT COUNT(*) FROM access_points WHERE bucket = 'b2'", 1),
                (
                    "SELECT COUNT(*) FROM olap_configs WHERE access_point = 'ap-b1'",
                    0,
                ),
                (
                    "SELECT COUNT(*) FROM olap_configs WHERE access_point = 'ap-b2'",
                    1,
                ),
            ] {
                let n: i64 = sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap();
                assert_eq!(n, expect, "{sql}");
            }
        }
    }

    /// Task 7：site 删桶只删 lifecycle_rules（保留 cursors），但走 txn。
    #[tokio::test]
    async fn site_bucket_delete_keeps_scope_but_uses_txn() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at, rule_id) \
             VALUES ('b1', 'p', 1, 1, 1, 'r')",
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO lifecycle_cursors (bucket, rule_id, stage, cursor, updated_at, \
             fence_term, fence_leader) VALUES ('b1', 'r', 'scan', '1', 1, 0, 0)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        {
            let mut txn = crate::control_store::SqliteControlTxn::new(&mut tx);
            txn.delete_lifecycle_rules_only("b1").await.unwrap();
        }
        tx.commit().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM lifecycle_rules WHERE bucket = 'b1'"
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM lifecycle_cursors WHERE bucket = 'b1'"
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            1,
            "site 删除不扩大游标范围"
        );
    }

    /// Task 3：桶读 façade 的属主/配置投影与缺行默认值。
    #[tokio::test]
    async fn bucket_read_facades_project_owner_and_configs() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        db.create_user("root", "pw", true).await.unwrap();
        let alice = db.create_user("alice", "pw", false).await.unwrap();
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, versioning, replication_mode) \
             VALUES ('b1', ?, 1, 'Enabled', 'sync'), ('b2', NULL, 2, 'Off', 'async')",
        )
        .bind(alice)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES ('b1', '{}', 5)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES ('b1', '[]', 6)",
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES ('b1', '[]', 7)",
        )
        .execute(db.pool())
        .await
        .unwrap();

        assert!(db.bucket_exists("b1").await.unwrap());
        assert!(!db.bucket_exists("missing").await.unwrap());
        assert_eq!(db.bucket_policy("b1").await.unwrap().as_deref(), Some("{}"));
        assert_eq!(
            db.bucket_notification("b1").await.unwrap().as_deref(),
            Some("[]")
        );
        assert_eq!(db.owned_buckets(alice).await.unwrap(), vec!["b1"]);
        assert_eq!(
            db.bucket_replication_modes()
                .await
                .unwrap()
                .get("b1")
                .unwrap(),
            "sync"
        );
        assert_eq!(
            db.bucket_sync_state_row("b1").await.unwrap(),
            Some(("sync".into(), false, "Enabled".into()))
        );
        assert_eq!(db.bucket_sync_state_row("missing").await.unwrap(), None);
        assert_eq!(db.get_bucket_versioning("missing").await.unwrap(), "Off");
        assert_eq!(db.list_bucket_rows().await.unwrap(), vec!["b1", "b2"]);
        assert_eq!(
            db.bucket_account("b1").await.unwrap(),
            xl_s3::access::bucket_account_id(Some(alice), false)
        );
        assert_eq!(
            db.bucket_account("missing").await.unwrap(),
            xl_s3::access::LEGACY_ACCOUNT
        );
    }

    /// Task 5：单机桶配置写 façade 往返 + 缺桶语义。
    #[tokio::test]
    async fn single_node_bucket_config_writes_roundtrip() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        db.create_user("root", "pw", true).await.unwrap();
        sqlx::query("INSERT INTO buckets (name, owner_id, creation_date) VALUES ('b', NULL, 1)")
            .execute(db.pool())
            .await
            .unwrap();

        db.set_bucket_policy("b", "{}", 10).await.unwrap();
        assert_eq!(db.bucket_policy("b").await.unwrap().as_deref(), Some("{}"));
        db.clear_bucket_policy("b").await.unwrap();
        assert!(db.bucket_policy("b").await.unwrap().is_none());

        db.set_bucket_notification("b", "[]", 11).await.unwrap();
        assert_eq!(
            db.bucket_notification("b").await.unwrap().as_deref(),
            Some("[]")
        );
        db.clear_bucket_notification("b").await.unwrap();
        assert!(db.bucket_notification("b").await.unwrap().is_none());

        assert!(
            db.set_bucket_default_encryption("b", "AES256")
                .await
                .unwrap()
        );
        assert_eq!(
            db.get_bucket_default_encryption("b")
                .await
                .unwrap()
                .as_deref(),
            Some("AES256")
        );
        assert!(db.clear_bucket_default_encryption("b").await.unwrap());
        assert!(
            db.get_bucket_default_encryption("b")
                .await
                .unwrap()
                .is_none()
        );

        db.set_bucket_deleting("b", true).await.unwrap();
        assert!(db.bucket_sync_state_row("b").await.unwrap().unwrap().1);
    }

    /// Task 5：缺桶时删除幂等、加密写入返回 false。
    #[tokio::test]
    async fn config_write_missing_bucket_is_idempotent_or_false() {
        let db = Db::connect("sqlite::memory:").await.unwrap();
        db.clear_bucket_policy("missing").await.unwrap();
        db.clear_bucket_notification("missing").await.unwrap();
        assert!(
            !db.set_bucket_default_encryption("missing", "AES256")
                .await
                .unwrap()
        );
        assert!(!db.clear_bucket_default_encryption("missing").await.unwrap());
    }
}
