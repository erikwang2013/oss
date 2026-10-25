//! 控制面数据访问。M1 只有五张表；对象数据永不进 SQL。

use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Row, SqlitePool};

use crate::raft::FencingEpoch;
use crate::raft::apply::ControlWrite;

pub type Result<T> = std::result::Result<T, sqlx::Error>;

pub struct Db {
    pool: SqlitePool,
    /// M9 S6a：Raft 模式抑制 `verify_user` 的旧 hash 本地升级（H1；各站点盐不同会分叉）。
    raft_mode: std::sync::atomic::AtomicBool,
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

fn row_to_restore(r: &sqlx::sqlite::SqliteRow) -> RestoreRow {
    RestoreRow {
        bucket: r.get("bucket"),
        key: r.get("key"),
        version_id: r.get("version_id"),
        expires_at: r.get("expires_at"),
        job_id: r.get("job_id"),
        state: r.get("state"),
        created_at: r.get("created_at"),
    }
}

/// M8 S5：Inventory 配置行（`config_json` = s3s DTO 的 JSON）。
#[derive(Debug, Clone, PartialEq)]
pub struct InventoryConfigRow {
    pub bucket: String,
    pub id: String,
    pub owner_id: i64,
    pub config_json: String,
    pub updated_at: i64,
}

/// M8 S5：Inventory 运行行（`started_at` = 实际开始执行的时刻）。
#[derive(Debug, Clone, PartialEq)]
pub struct InventoryRunRow {
    pub id: i64,
    pub bucket: String,
    pub config_id: String,
    pub run_key: String,
    pub started_at: i64,
    pub state: String,
    pub job_id: Option<i64>,
    pub manifest_key: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
}

fn inventory_run_row(r: &sqlx::sqlite::SqliteRow) -> InventoryRunRow {
    InventoryRunRow {
        id: r.get("id"),
        bucket: r.get("bucket"),
        config_id: r.get("config_id"),
        run_key: r.get("run_key"),
        started_at: r.get("started_at"),
        state: r.get("state"),
        job_id: r.get("job_id"),
        manifest_key: r.get("manifest_key"),
        error: r.get("error"),
        created_at: r.get("created_at"),
    }
}

/// 访问点行（M8 S1）。
#[derive(Debug, Clone, PartialEq)]
pub struct AccessPointRow {
    pub name: String,
    pub bucket: String,
    /// AP 创建者 uid（不是账户来源；账户取桶主）。
    pub owner_id: i64,
    pub policy_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    /// 桶主 uid（NULL = 无主桶或本节点无桶行）。
    pub bucket_owner_id: Option<i64>,
    /// 桶主是否管理员（管理员桶归系统账户）。
    pub bucket_owner_is_admin: bool,
}

impl AccessPointRow {
    /// 账户命名空间 v1：AP ARN 的账户段 = 桶账户。
    pub fn account(&self) -> String {
        xl_s3::access::bucket_account_id(self.bucket_owner_id, self.bucket_owner_is_admin)
    }
}

/// AP 行查询：联桶主与管理员标记（账户命名空间 v1）；本节点无桶行 → NULL → 系统账户。
const AP_ROW_SELECT_BASE: &str = "SELECT ap.name, ap.bucket, ap.owner_id, ap.policy_json, \
     ap.created_at, ap.updated_at, b.owner_id AS bucket_owner_id, \
     COALESCE(u.is_admin, 0) AS bucket_owner_is_admin \
     FROM access_points ap LEFT JOIN buckets b ON b.name = ap.bucket \
     LEFT JOIN users u ON u.id = b.owner_id";

fn ap_row(r: &sqlx::sqlite::SqliteRow) -> AccessPointRow {
    AccessPointRow {
        name: r.get("name"),
        bucket: r.get("bucket"),
        owner_id: r.get("owner_id"),
        policy_json: r.get("policy_json"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
        bucket_owner_id: r.get("bucket_owner_id"),
        bucket_owner_is_admin: r.get::<i64, _>("bucket_owner_is_admin") != 0,
    }
}

/// 变换器配置行（M8 S2）。
#[derive(Debug, Clone, PartialEq)]
pub struct OlapConfigRow {
    pub access_point: String,
    pub kind: String,
    pub endpoint: Option<String>,
    pub module: Option<Vec<u8>>,
    pub module_sha256: Option<String>,
    pub timeout_ms: i64,
    pub max_response_bytes: i64,
    pub updated_at: i64,
}

fn olap_row(r: &sqlx::sqlite::SqliteRow) -> OlapConfigRow {
    OlapConfigRow {
        access_point: r.get("access_point"),
        kind: r.get("kind"),
        endpoint: r.get("endpoint"),
        module: r.get("module"),
        module_sha256: r.get("module_sha256"),
        timeout_ms: r.get("timeout_ms"),
        max_response_bytes: r.get("max_response_bytes"),
        updated_at: r.get("updated_at"),
    }
}

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

    /// 离线运维连接：只开 WAL 池，不重放 schema、不切模式（避免二次开 raft.db / 降级守卫）。
    pub async fn connect_offline(url: &str) -> Result<Self> {
        let pool = SqlitePool::connect_with(wal_options(url)?).await?;
        Ok(Self {
            pool,
            raft_mode: std::sync::atomic::AtomicBool::new(false),
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
        db.ensure_columns_tenancy().await?;
        // M8-1：运行态表 fencing 列依赖 0003 已建表，故放在 M8/M9 重放之后。
        db.ensure_fence_columns().await?;
        if seed_site {
            db.site_bootstrap(site_id).await?;
        }
        db.prepare_raft_mode(raft).await?;
        // standalone 启动即回填并规范化（v2 移出全局管理员）；放在模式守卫之后：Raft 库被
        // 误按 standalone 启动时，不得在报错前写 canonical marker。Raft 模式下账户表由
        // leader 的 canonicalization worker 写，节点本地不跑回填（否则各节点派生 ID 分叉）。
        if !raft {
            db.backfill_accounts().await?;
            db.backfill_accounts_v2().await?;
        }
        Ok(db)
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
        sqlx::query("DELETE FROM site_config WHERE rowid <> (SELECT MIN(rowid) FROM site_config)")
            .execute(&self.pool)
            .await?;
        let existing: Option<String> = sqlx::query_scalar("SELECT id FROM site_config LIMIT 1")
            .fetch_optional(&self.pool)
            .await?;
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
        sqlx::query(
            "INSERT INTO site_config (id, name, region, priority, site_token, created_at) \
             SELECT ?, 'site', 'us-east-1', 0, ?, ? \
             WHERE NOT EXISTS (SELECT 1 FROM site_config)",
        )
        .bind(id)
        .bind(token)
        .bind(now())
        .execute(&self.pool)
        .await?;
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

    /// 多租户补列：`accounts.soft_byte_quota` 与 `audit_log` 的账户审计列。
    /// 老库升级 + 并发首开容忍（对方已补列视同成功）。
    async fn ensure_columns_tenancy(&self) -> Result<()> {
        self.ensure_column(
            "accounts",
            "soft_byte_quota",
            "ALTER TABLE accounts ADD COLUMN soft_byte_quota INTEGER",
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

    /// 建 AP（同名冲突由调用方按 409 处理）。
    pub async fn create_access_point(
        &self,
        name: &str,
        bucket: &str,
        owner_id: i64,
        policy_json: Option<&str>,
        now: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO access_points (name, bucket, owner_id, policy_json, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(name)
        .bind(bucket)
        .bind(owner_id)
        .bind(policy_json)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_access_point(&self, name: &str) -> Result<Option<AccessPointRow>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "{AP_ROW_SELECT_BASE} WHERE ap.name = ?"
        )))
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| ap_row(&r)))
    }

    /// `bucket=None` → 全部（缓存预加载）；Some → 该桶（级联失效/列举）。
    pub async fn list_access_points(&self, bucket: Option<&str>) -> Result<Vec<AccessPointRow>> {
        let rows = match bucket {
            Some(b) => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "{AP_ROW_SELECT_BASE} WHERE ap.bucket = ? ORDER BY ap.name"
                )))
                .bind(b)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "{AP_ROW_SELECT_BASE} ORDER BY ap.name"
                )))
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows.iter().map(ap_row).collect())
    }

    /// 账户命名空间 v1：桶主的 12 位账户 ID（NULL/管理员桶 = 系统账户）。
    /// 本节点无桶行（非 Raft 集群）时同样回落到系统账户。
    pub async fn bucket_account(&self, bucket: &str) -> Result<String> {
        let row = sqlx::query(
            "SELECT b.owner_id AS owner_id, COALESCE(u.is_admin, 0) AS is_admin \
             FROM buckets b LEFT JOIN users u ON u.id = b.owner_id WHERE b.name = ?",
        )
        .bind(bucket)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match row {
            Some(r) => xl_s3::access::bucket_account_id(
                r.get::<Option<i64>, _>("owner_id"),
                r.get::<i64, _>("is_admin") != 0,
            ),
            None => xl_s3::access::LEGACY_ACCOUNT.to_string(),
        })
    }

    /// 更新 AP 策略（None = 清策略）。
    pub async fn set_access_point_policy(
        &self,
        name: &str,
        policy_json: Option<&str>,
        now: i64,
    ) -> Result<bool> {
        let n =
            sqlx::query("UPDATE access_points SET policy_json = ?, updated_at = ? WHERE name = ?")
                .bind(policy_json)
                .bind(now)
                .bind(name)
                .execute(&self.pool)
                .await?
                .rows_affected();
        Ok(n > 0)
    }

    pub async fn delete_access_point(&self, name: &str) -> Result<bool> {
        // SQLite 默认不启用 FK —— 显式删变换器行，不依赖 ON DELETE CASCADE。
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM olap_configs WHERE access_point = ?")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        let n = sqlx::query("DELETE FROM access_points WHERE name = ?")
            .bind(name)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(n > 0)
    }

    // ---------- M8（S2）：Object Lambda 变换器 ----------

    pub async fn get_olap_config(&self, name: &str) -> Result<Option<OlapConfigRow>> {
        let row = sqlx::query(
            "SELECT access_point, kind, endpoint, module, module_sha256, timeout_ms, \
             max_response_bytes, updated_at FROM olap_configs WHERE access_point = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| olap_row(&r)))
    }

    /// 启动预加载用（全部）。
    pub async fn list_olap_configs(&self) -> Result<Vec<OlapConfigRow>> {
        let rows = sqlx::query(
            "SELECT access_point, kind, endpoint, module, module_sha256, timeout_ms, \
             max_response_bytes, updated_at FROM olap_configs ORDER BY access_point",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(olap_row).collect())
    }

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
        sqlx::query(
            "INSERT INTO olap_configs (access_point, kind, endpoint, module, module_sha256, \
             timeout_ms, max_response_bytes, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(access_point) DO UPDATE SET kind = excluded.kind, \
             endpoint = excluded.endpoint, module = excluded.module, \
             module_sha256 = excluded.module_sha256, timeout_ms = excluded.timeout_ms, \
             max_response_bytes = excluded.max_response_bytes, updated_at = excluded.updated_at",
        )
        .bind(name)
        .bind(kind)
        .bind(endpoint)
        .bind(module)
        .bind(module_sha256)
        .bind(timeout_ms)
        .bind(max_response_bytes)
        .bind(now)
        .execute(&self.pool)
        .await?;
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
        // olap_configs 无 FK 级联保证时也必须清（先按 access_point 删，再删 AP）
        sqlx::query(
            "DELETE FROM olap_configs WHERE access_point IN \
             (SELECT name FROM access_points WHERE bucket = ?)",
        )
        .bind(bucket)
        .execute(&mut *conn)
        .await?;
        for sql in [
            "DELETE FROM access_points WHERE bucket = ?",
            "DELETE FROM inventory_runs WHERE bucket = ?",
            "DELETE FROM inventory_configs WHERE bucket = ?",
            "DELETE FROM inventory_cursors WHERE bucket = ?",
            "DELETE FROM inventory_incremental WHERE bucket = ?",
            "DELETE FROM tier_restores WHERE bucket = ?",
            "DELETE FROM lifecycle_rules WHERE bucket = ?",
            "DELETE FROM lifecycle_cursors WHERE bucket = ?",
        ] {
            sqlx::query(sql).bind(bucket).execute(&mut *conn).await?;
        }
        Ok(())
    }

    /// M9 S6c（M-1）：Raft 删桶后只清**本地** M8 行。复制表已由 apply 级联清空，
    /// 且受 `raft_guard_*` 保护不得再直写（会 ABORT）；这里仅补 `inventory_runs`/
    /// `tier_restores`/扫描游标等本地表。幂等。
    pub async fn clear_bucket_local_m8(&self, bucket: &str) -> Result<()> {
        for sql in [
            "DELETE FROM inventory_runs WHERE bucket = ?",
            "DELETE FROM inventory_cursors WHERE bucket = ?",
            "DELETE FROM inventory_incremental WHERE bucket = ?",
            "DELETE FROM lifecycle_cursors WHERE bucket = ?",
            "DELETE FROM tier_restores WHERE bucket = ?",
        ] {
            sqlx::query(sql).bind(bucket).execute(&self.pool).await?;
        }
        Ok(())
    }

    // ---------- M7：站点身份与关系 ----------

    pub async fn site_identity(&self) -> Result<SiteIdentity> {
        let r =
            sqlx::query("SELECT id, name, region, priority, site_token FROM site_config LIMIT 1")
                .fetch_one(&self.pool)
                .await?;
        Ok(SiteIdentity {
            id: r.get("id"),
            name: r.get("name"),
            region: r.get("region"),
            priority: r.get("priority"),
            token: r.get("site_token"),
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
        sqlx::query(
            "UPDATE site_config SET name = COALESCE(?, name), region = COALESCE(?, region), \
             priority = COALESCE(?, priority), site_token = COALESCE(?, site_token)",
        )
        .bind(name)
        .bind(region)
        .bind(priority)
        .bind(token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_sites(&self) -> Result<Vec<SitePeer>> {
        let rows = sqlx::query("SELECT * FROM sites ORDER BY created_at, peer_id")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(site_peer_from_row).collect())
    }

    pub async fn get_site(&self, peer_id: &str) -> Result<Option<SitePeer>> {
        let r = sqlx::query("SELECT * FROM sites WHERE peer_id = ?")
            .bind(peer_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(r.as_ref().map(site_peer_from_row))
    }

    pub async fn count_sites(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT COUNT(*) FROM sites")
            .fetch_one(&self.pool)
            .await
    }

    /// S7.4：Raft 配对行写入/更新；按 peer_id 或 remote_site_id 采用既有 M7 行，
    /// 不写 `site_cursors/changelog`（Raft 下 sites 是站点本地表，S6d 启动断言要求 site_* 为空）。
    pub async fn upsert_pair_peer(&self, p: &SitePeer) -> Result<()> {
        // 注意：UPDATE 不触碰 `reconcile_required`——该门只能由 repair 成功清除
        // （S7.4 review I2：重复/重试配对不得把 B′ 未修复状态打开）。
        let updated = sqlx::query(
            "UPDATE sites SET remote_site_id = ?, name = ?, endpoint = ?, region = ?, priority = ?, \
             s3_access_key = ?, s3_secret_key = ?, peer_site_token = ?, raft_node_id = ? \
             WHERE peer_id = ? OR remote_site_id = ?",
        )
        .bind(&p.remote_site_id)
        .bind(&p.name)
        .bind(&p.endpoint)
        .bind(&p.region)
        .bind(p.priority)
        .bind(&p.s3_access_key)
        .bind(&p.s3_secret_key)
        .bind(&p.peer_site_token)
        .bind(p.raft_node_id.map(|v| v as i64))
        .bind(&p.peer_id)
        .bind(&p.remote_site_id)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() == 0 {
            sqlx::query(
                "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, region, priority, \
                 s3_access_key, s3_secret_key, peer_site_token, created_at, raft_node_id, \
                 reconcile_required) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&p.peer_id)
            .bind(&p.remote_site_id)
            .bind(&p.name)
            .bind(&p.endpoint)
            .bind(&p.region)
            .bind(p.priority)
            .bind(&p.s3_access_key)
            .bind(&p.s3_secret_key)
            .bind(&p.peer_site_token)
            .bind(p.created_at)
            .bind(p.raft_node_id.map(|v| v as i64))
            .bind(if p.reconcile_required { 1i64 } else { 0 })
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// S7.4 B′：替换站点后、repair 完成前拒绝向该对端服务 sync。
    pub async fn set_site_reconcile_required(&self, peer_id: &str, required: bool) -> Result<()> {
        sqlx::query("UPDATE sites SET reconcile_required = ? WHERE peer_id = ?")
            .bind(if required { 1i64 } else { 0 })
            .bind(peer_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert_site(&self, p: &SitePeer) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO sites (peer_id, remote_site_id, name, endpoint, region, priority, \
             s3_access_key, s3_secret_key, peer_site_token, created_at, raft_node_id) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&p.peer_id)
        .bind(&p.remote_site_id)
        .bind(&p.name)
        .bind(&p.endpoint)
        .bind(&p.region)
        .bind(p.priority)
        .bind(&p.s3_access_key)
        .bind(&p.s3_secret_key)
        .bind(&p.peer_site_token)
        .bind(p.created_at)
        .bind(p.raft_node_id.map(|v| v as i64))
        .execute(&mut *tx)
        .await?;
        // 出货游标从 0 起（首轮全量送达含配对快照）
        sqlx::query(
            "INSERT OR IGNORE INTO site_cursors (peer_id, last_seq, updated_at) VALUES (?, 0, ?)",
        )
        .bind(&p.peer_id)
        .bind(now())
        .execute(&mut *tx)
        .await?;
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
        use sqlx::Row as _;
        let mut tx = self.pool.begin().await?;
        let remote: Option<String> =
            sqlx::query_scalar("SELECT remote_site_id FROM sites WHERE peer_id = ?")
                .bind(peer_id)
                .fetch_optional(&mut *tx)
                .await?;
        if remote.is_none() {
            return Ok(None);
        }
        let rows = sqlx::query(
            "DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ? \
             AND version_id <> '' RETURNING bucket, key, version_id",
        )
        .bind(peer_id)
        .fetch_all(&mut *tx)
        .await?;
        let affected: Vec<(String, String, String)> = rows
            .iter()
            .map(|r| (r.get("bucket"), r.get("key"), r.get("version_id")))
            .collect();
        sqlx::query(
            "DELETE FROM repl_outbox WHERE target_kind = 'site' AND remote_id = ? \
             AND version_id = ''",
        )
        .bind(peer_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM site_cursors WHERE peer_id = ?")
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        // 解配时清掉该 peer 的控制面死信（否则重配对后 status 的 dead 计数失真）。
        if let Some(remote) = &remote {
            sqlx::query("DELETE FROM site_apply_dead WHERE peer_id = ?")
                .bind(remote)
                .execute(&mut *tx)
                .await?;
        }
        let r = sqlx::query("DELETE FROM sites WHERE peer_id = ?")
            .bind(peer_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((r.rows_affected() == 1).then_some(affected))
    }

    pub async fn update_site_ping(
        &self,
        peer_id: &str,
        ts: i64,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query("UPDATE sites SET last_seen = ?, last_error = ? WHERE peer_id = ?")
            .bind(ts)
            .bind(error)
            .bind(peer_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn create_user(&self, username: &str, password: &str, is_admin: bool) -> Result<i64> {
        let hash = crate::db::hash_password(password);
        let id = sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(username)
        .bind(hash)
        .bind(is_admin as i64)
        .bind(now())
        .execute(&self.pool)
        .await?
        .last_insert_rowid();
        // AC-0：非管理员新用户加入其 uid 派生账户（与回填同语义；之后可被显式迁入其它账户）。
        // 全局管理员归系统账户、不是任何账户的成员（AC-2）：不建账户行/成员行。
        if !is_admin && let Some(acct) = xl_s3::access::account_id_of(id) {
            sqlx::query(
                "INSERT OR IGNORE INTO accounts (id, name, status, admin_uid, created_at, updated_at) \
                 VALUES (?, ?, 'active', ?, ?, ?)",
            )
            .bind(&acct)
            .bind(username)
            .bind(id)
            .bind(now())
            .bind(now())
            .execute(&self.pool)
            .await?;
            sqlx::query(
                "INSERT OR IGNORE INTO account_members (account_id, uid, role, created_at) \
                 VALUES (?, ?, 'owner', ?)",
            )
            .bind(&acct)
            .bind(id)
            .bind(now())
            .execute(&self.pool)
            .await?;
        }
        Ok(id)
    }

    pub async fn user_exists(&self, username: &str) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM users WHERE username = ?")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    pub async fn verify_user(&self, username: &str, password: &str) -> Result<Option<i64>> {
        // M4 切片 7：Argon2id 恒时校验（password-hash 的 verify_password 内部恒时）；
        // 旧 M1 格式（sha256 单轮）仍可验证——**登录成功即透明重写升级**（一次迁移）。
        // status='active'：被停用的账号登录直接按「用户名或密码错误」处理（不问不答）。
        // AC-1：账户停用同样按凭据错误处理（成员行存在但账户行缺失 → 停用，fail-closed）。
        let row = sqlx::query(
            "SELECT id, password_hash FROM users u WHERE username = ? AND status = 'active' \
             AND NOT EXISTS (SELECT 1 FROM account_members am \
                LEFT JOIN accounts a ON a.id = am.account_id \
                WHERE am.uid = u.id AND COALESCE(a.status, 'disabled') <> 'active')",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else {
            // 用户存在性 oracle 收口（verifier 切片 7 观察 A）：Argon2 让「未命中早退」
            // 与「命中校验」的时延差从 µs 放大到几十 ms、网络可测——未命中路径先对一个
            // 固定 dummy 串跑一次同参数 verify 再返回，两侧时延拉平。
            crate::db::dummy_verify(password);
            return Ok(None);
        };
        let stored: String = r.get("password_hash");
        let id: i64 = r.get("id");
        if !crate::db::verify_password(password, &stored) {
            return Ok(None);
        }
        if crate::db::is_legacy_hash(&stored) && !self.raft_mode() {
            // 透明升级（失败只 warn：本次登录已成功，下次再来）。
            // Raft 模式：随机盐会让各站点写出的 hash 不同 → 禁止本地写，只验通过。
            let upgraded = crate::db::hash_password(password);
            if let Err(e) = sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
                .bind(&upgraded)
                .bind(id)
                .execute(&self.pool)
                .await
            {
                tracing::warn!(error = %e, user = %username, "旧口令哈希透明升级失败");
            }
        }
        Ok(Some(id))
    }

    pub async fn create_access_key(&self, ak: &str, sk: &str, user_id: i64) -> Result<()> {
        sqlx::query("INSERT INTO access_keys (access_key, secret_key, user_id, created_at) VALUES (?, ?, ?, ?)")
            .bind(ak)
            .bind(sk)
            .bind(user_id)
            .bind(now())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // SigV4 的 AK → SK 查询归 xl-s3/src/auth.rs（SigV4Auth）；这里不再保留第二份，防 M5 双改。

    pub async fn list_access_keys(&self, user_id: i64) -> Result<Vec<String>> {
        // M5 切片 5：控制台只列长时键（会话行由 STS 签发、海量短寿，不进「访问密钥」页）
        let rows = sqlx::query(
            "SELECT access_key FROM access_keys WHERE user_id = ? AND kind = 'long-term' \
             ORDER BY created_at, access_key",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.get("access_key")).collect())
    }

    pub async fn insert_bucket(&self, name: &str, owner_id: Option<i64>) -> Result<()> {
        self.insert_bucket_set(name, owner_id, 0).await
    }

    /// M4 切片 8：带 set 的建桶行（单 set 缺省 0 走上面）。
    pub async fn insert_bucket_set(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
    ) -> Result<()> {
        self.insert_bucket_full(name, owner_id, set, "async").await
    }

    /// M9 全列建桶行（INSERT OR IGNORE；已存在的权威行不重置）。`set_id` 是本地路由列。
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
    ) -> Result<()> {
        self.upsert_bucket_owner_set_mode(name, owner_id, set, "async")
            .await
    }

    /// M9：同 [`Self::upsert_bucket_owner_set`]，并写 `replication_mode`。
    /// ON CONFLICT 全列重置（set 随重建收敛，M5 阻断 1）；creation_date 保持首建。
    pub async fn upsert_bucket_owner_set_mode(
        &self,
        name: &str,
        owner_id: Option<i64>,
        set: u16,
        replication_mode: &str,
    ) -> Result<()> {
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
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 桶的复制模式；行不存在 = "async"（读取路径与缺行旧桶同口径）。
    pub async fn bucket_replication_mode(&self, name: &str) -> Result<String> {
        let row = sqlx::query("SELECT replication_mode FROM buckets WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(|r| r.get::<String, _>("replication_mode"))
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
        let row = sqlx::query(
            "SELECT replication_mode, deleting, versioning FROM buckets WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                r.get::<String, _>("replication_mode"),
                r.get::<i64, _>("deleting") != 0,
                r.get::<String, _>("versioning"),
            )
        }))
    }

    /// M9 S3：删桶 quiesce 标志（S6 起改由 Raft `SetBucketDeleting` 命令承载）。
    pub async fn set_bucket_deleting(&self, name: &str, deleting: bool) -> Result<()> {
        sqlx::query("UPDATE buckets SET deleting = ? WHERE name = ?")
            .bind(i64::from(deleting))
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 全量桶名 → 模式（console 列表一次查完，避免逐桶查询）。
    pub async fn bucket_replication_modes(
        &self,
    ) -> Result<std::collections::HashMap<String, String>> {
        let rows = sqlx::query("SELECT name, replication_mode FROM buckets")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<String, _>("name"),
                    r.get::<String, _>("replication_mode"),
                )
            })
            .collect())
    }

    /// P3-3：回填对账——把**权威**解析到的 set 覆盖到已有行（仅 set_id 列；owner/versioning
    /// 不动）。场景：桶删除后在别节点重建为另一 set，本节点陈旧行（INSERT OR IGNORE 不会更新）
    /// 会在下次启动被此处收敛；行不存在则插入（owner NULL，与历史桶回填同权）。
    /// M5 切片 0：权威值须来自 `resolve_bucket_set_authoritative`（数据面本地先行解析对本地
    /// 陈旧行会把旧值原样写回，收敛不成立——verifier 高 2）。
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
        let rows = sqlx::query("SELECT name FROM buckets ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|r| r.get("name")).collect())
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

    pub async fn record_upload(
        &self,
        saved_path: &str,
        bucket: &str,
        key: &str,
        group: &str,
        size: i64,
        etag: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO uploads (saved_path, bucket, key, group_name, size, etag, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(saved_path) DO NOTHING",
        )
        .bind(saved_path)
        .bind(bucket)
        .bind(key)
        .bind(group)
        .bind(size)
        .bind(etag)
        .bind(now())
        .execute(&self.pool)
        .await?;
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
        if is_admin {
            let n = sqlx::query(
                "UPDATE uploads SET original_name = ?, owner_id = ? WHERE saved_path = ?",
            )
            .bind(original_name)
            .bind(owner_id)
            .bind(saved_path)
            .execute(&self.pool)
            .await?
            .rows_affected();
            return Ok(n == 1);
        }
        let n = sqlx::query(
            "UPDATE uploads SET original_name = ?, owner_id = ? \
             WHERE saved_path = ? AND (owner_id IS NULL OR owner_id = ?)",
        )
        .bind(original_name)
        .bind(owner_id)
        .bind(saved_path)
        .bind(owner_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n == 1)
    }

    /// 记录对应的桶（登记前判权用；记录不存在 → None）。
    pub async fn upload_bucket(&self, saved_path: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT bucket FROM uploads WHERE saved_path = ?")
            .bind(saved_path)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("bucket")))
    }

    /// 桶版本控制状态（M4 切片 3；aether 上传面按此传 versioned；无行 → "Off"）。
    pub async fn get_bucket_versioning(&self, bucket: &str) -> Result<String> {
        let row = sqlx::query("SELECT versioning FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(|r| r.get::<String, _>("versioning"))
            .unwrap_or_else(|| "Off".to_string()))
    }

    /// M8 缺口 C：桶默认加密（"AES256"/"aws:kms"；None = 未配置）。
    pub async fn get_bucket_default_encryption(&self, bucket: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT default_encryption FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| r.get::<Option<String>, _>("default_encryption")))
    }

    /// M8 缺口 C：写桶默认加密（桶不存在 → 0 行）。
    pub async fn set_bucket_default_encryption(
        &self,
        bucket: &str,
        algorithm: &str,
    ) -> Result<bool> {
        let res = sqlx::query("UPDATE buckets SET default_encryption = ? WHERE name = ?")
            .bind(algorithm)
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// M8 缺口 C：清桶默认加密（幂等）。
    pub async fn clear_bucket_default_encryption(&self, bucket: &str) -> Result<bool> {
        let res = sqlx::query("UPDATE buckets SET default_encryption = NULL WHERE name = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    /// 全部桶默认加密记录（桶名 + 库内存储值），按桶名排序；CMK 引用守卫用。
    pub async fn list_bucket_default_encryptions(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT name, default_encryption FROM buckets \
             WHERE default_encryption IS NOT NULL ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<String, _>("name"),
                    r.get::<String, _>("default_encryption"),
                )
            })
            .collect())
    }

    /// 某用户拥有的桶（控制台桶列表能力位计算用）。
    pub async fn owned_buckets(&self, uid: i64) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT name FROM buckets WHERE owner_id = ? ORDER BY name")
            .bind(uid)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|r| r.get("name")).collect())
    }

    pub async fn list_uploads(&self, limit: i64) -> Result<Vec<UploadRecord>> {
        let rows =
            sqlx::query("SELECT * FROM uploads ORDER BY created_at DESC, saved_path LIMIT ?")
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|r| UploadRecord {
                saved_path: r.get("saved_path"),
                bucket: r.get("bucket"),
                key: r.get("key"),
                group_name: r.get("group_name"),
                size: r.get("size"),
                etag: r.get("etag"),
                original_name: r.get("original_name"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    // ---- 子账户与权限（M-权限体系）----

    /// 取用户（`/me`、admin 判定、停用拦截用）：(username, is_admin, is_active)。
    pub async fn get_user(&self, uid: i64) -> Result<Option<(String, bool, bool)>> {
        let row = sqlx::query("SELECT username, is_admin, status FROM users WHERE id = ?")
            .bind(uid)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| {
            (
                r.get::<String, _>("username"),
                r.get::<i64, _>("is_admin") != 0,
                r.get::<String, _>("status") == "active",
            )
        }))
    }

    /// 启停用用户。返回是否生效（false = 用户不存在，或停用会留下 0 个在册管理员）。
    ///
    /// **防呆是原子条件**（评审 P2：先读 count 再 UPDATE 的 check-then-act 在并发互停
    /// 两个管理员时会双双放行 → 0 在册管理员锁死）。停用走单条条件 UPDATE：
    /// 非管理员恒可停；管理员仅在「当前在册管理员 > 1」时可停——子查询与写入同语句，
    /// SQLite 写锁串行化下天然防 TOCTOU。启用无锁死风险，走普通 UPDATE。
    pub async fn set_user_status(&self, uid: i64, active: bool) -> Result<bool> {
        let n = if active {
            sqlx::query("UPDATE users SET status = 'active' WHERE id = ?")
                .bind(uid)
                .execute(&self.pool)
                .await?
                .rows_affected()
        } else {
            // 评审 nit①：守卫只对「把**在册**管理员停掉」生效；对已停用者重复停用是幂等
            // no-op（不触碰计数），否则会误报「不能停用最后一个在册管理员」。
            sqlx::query(
                "UPDATE users SET status = 'disabled' WHERE id = ? AND \
                 (NOT (is_admin = 1 AND status = 'active') \
                  OR (SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active') > 1)",
            )
            .bind(uid)
            .execute(&self.pool)
            .await?
            .rows_affected()
        };
        Ok(n == 1)
    }

    /// 在册（active）管理员数：保护「不能停用最后一个可用管理员」。
    pub async fn count_active_admins(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active'")
            .fetch_one(&self.pool)
            .await
    }

    /// 全部用户 + 各自权限行（子账户管理页；数量级 = 人手，两步查询足够）。
    pub async fn list_users(&self) -> Result<Vec<UserRow>> {
        let users =
            sqlx::query("SELECT id, username, is_admin, status, created_at FROM users ORDER BY id")
                .fetch_all(&self.pool)
                .await?;
        let mut out = Vec::with_capacity(users.len());
        for r in users {
            let id: i64 = r.get("id");
            out.push(UserRow {
                id,
                username: r.get("username"),
                is_admin: r.get::<i64, _>("is_admin") != 0,
                status: r.get("status"),
                created_at: r.get("created_at"),
                permissions: self.get_permissions(id).await?,
            });
        }
        Ok(out)
    }

    /// 管理员数量（保护「最后一个管理员」不被删）。
    pub async fn count_admins(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT count(*) FROM users WHERE is_admin = 1")
            .fetch_one(&self.pool)
            .await
    }

    /// 改口令。返回是否有该用户。
    pub async fn set_password(&self, uid: i64, password: &str) -> Result<bool> {
        let n = sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
            .bind(crate::db::hash_password(password))
            .bind(uid)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n == 1)
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
        let gate = sqlx::query(
            "UPDATE users SET id = id WHERE id = ? AND \
             (NOT (is_admin = 1 AND status = 'active') \
              OR (SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active') > 1)",
        )
        .bind(uid)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if gate == 0 {
            return Ok(false);
        }
        sqlx::query("DELETE FROM access_keys WHERE user_id = ?")
            .bind(uid)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM permissions WHERE user_id = ?")
            .bind(uid)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE buckets SET owner_id = NULL WHERE owner_id = ?")
            .bind(uid)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE uploads SET owner_id = NULL WHERE owner_id = ?")
            .bind(uid)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM users WHERE id = ?")
            .bind(uid)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    // ---- AC-0：多租户账户（standalone 模式；Raft 下只读回填结果）----

    /// 启动回填：每个存量用户 → 其 uid 派生账户（role=owner），并保证系统账户存在。
    /// 只跑一次（`schema_meta.accounts_backfill_v1`）：显式迁出派生账户的用户重启后不被拉回。
    /// 返回是否实际执行（false = marker 已存在）。
    pub async fn backfill_accounts(&self) -> Result<bool> {
        if self.schema_meta("accounts_backfill_v1").await?.is_some() {
            return Ok(false);
        }
        let users = sqlx::query("SELECT id, username FROM users ORDER BY id")
            .fetch_all(&self.pool)
            .await?;
        let ts = now();
        let mut tx = self.pool.begin().await?;
        // 系统账户：admin/NULL 桶的账户；没有成员，主体匹配永不命中它。
        sqlx::query(
            "INSERT OR IGNORE INTO accounts (id, name, status, created_at, updated_at) \
             VALUES (?, 'system', 'active', ?, ?)",
        )
        .bind(xl_s3::access::LEGACY_ACCOUNT)
        .bind(ts)
        .bind(ts)
        .execute(&mut *tx)
        .await?;
        for r in users {
            let uid: i64 = r.get("id");
            let username: String = r.get("username");
            let Some(acct) = xl_s3::access::account_id_of(uid) else {
                continue; // uid 0/越界：旧语义下也无法匹配任何账户 ARN，保持原状
            };
            sqlx::query(
                "INSERT OR IGNORE INTO accounts \
                 (id, name, status, admin_uid, created_at, updated_at) \
                 VALUES (?, ?, 'active', ?, ?, ?)",
            )
            .bind(&acct)
            .bind(&username)
            .bind(uid)
            .bind(ts)
            .bind(ts)
            .execute(&mut *tx)
            .await?;
            // 首次回填时不存在成员行；UNIQUE(uid) 兜底并发/重入。
            sqlx::query(
                "INSERT OR IGNORE INTO account_members (account_id, uid, role, created_at) \
                 VALUES (?, ?, 'owner', ?)",
            )
            .bind(&acct)
            .bind(uid)
            .bind(ts)
            .execute(&mut *tx)
            .await?;
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
        sqlx::query(
            "DELETE FROM account_members WHERE uid IN (SELECT id FROM users WHERE is_admin = 1)",
        )
        .execute(&mut *tx)
        .await?;
        // 只删「某管理员的 uid 派生 ID 且已无成员」的账户；管理员桶归系统账户，
        // 不引用这些账户，删除不影响桶归属。
        sqlx::query(
            "DELETE FROM accounts \
             WHERE id IN (SELECT printf('%012d', id) FROM users WHERE is_admin = 1) \
               AND id NOT IN (SELECT account_id FROM account_members)",
        )
        .execute(&mut *tx)
        .await?;
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

    /// 用户当前账户：成员行优先，无行回落 uid 派生账户（legacy 兼容路径）。
    pub async fn account_id_of_user(&self, uid: i64) -> Result<Option<String>> {
        let row = sqlx::query("SELECT account_id FROM account_members WHERE uid = ?")
            .bind(uid)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(|r| r.get::<String, _>("account_id"))
            .or_else(|| xl_s3::access::account_id_of(uid)))
    }

    /// 账户状态是否允许该用户继续使用（无成员行 = 派生账户语义 = 有效）。
    /// 成员行存在但账户行缺失 → false（fail-closed）。
    pub async fn account_active(&self, uid: i64) -> Result<bool> {
        let inactive: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM account_members am \
             LEFT JOIN accounts a ON a.id = am.account_id \
             WHERE am.uid = ? AND COALESCE(a.status, 'disabled') <> 'active'",
        )
        .bind(uid)
        .fetch_one(&self.pool)
        .await?;
        Ok(inactive == 0)
    }

    fn account_row_from(r: &sqlx::sqlite::SqliteRow) -> AccountRow {
        AccountRow {
            id: r.get("id"),
            name: r.get("name"),
            status: r.get("status"),
            admin_uid: r.get("admin_uid"),
            bucket_quota: r.get("bucket_quota"),
            soft_byte_quota: r.get("soft_byte_quota"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
            member_count: r.get("member_count"),
            bucket_count: r.get("bucket_count"),
        }
    }

    /// 账户桶数：桶主的**当前**账户（成员行优先，无行 = uid 派生）；管理员桶归系统账户
    /// 但永不匹配真实账户，故不计入任何用户账户。
    fn account_bucket_count_sql() -> &'static str {
        "(SELECT COUNT(*) FROM buckets b JOIN users u ON u.id = b.owner_id \
          LEFT JOIN account_members bm ON bm.uid = u.id \
          WHERE u.is_admin = 0 AND COALESCE(bm.account_id, printf('%012d', u.id)) = a.id)"
    }

    /// 全部账户（管理员视角）或某用户的账户（账户成员视角）。
    pub async fn list_accounts(&self, member_uid: Option<i64>) -> Result<Vec<AccountRow>> {
        let sql = format!(
            "SELECT a.*, \
             (SELECT COUNT(*) FROM account_members m WHERE m.account_id = a.id) AS member_count, \
             {} AS bucket_count \
             FROM accounts a {} ORDER BY a.id",
            Self::account_bucket_count_sql(),
            if member_uid.is_some() {
                "WHERE a.id IN (SELECT account_id FROM account_members WHERE uid = ?)"
            } else {
                ""
            }
        );
        // 字面量拼接只来自上面两个常量分支，无用户输入。
        let q = sqlx::query(sqlx::AssertSqlSafe(sql));
        let q = match member_uid {
            Some(uid) => q.bind(uid),
            None => q,
        };
        let rows = q.fetch_all(&self.pool).await?;
        Ok(rows.iter().map(Self::account_row_from).collect())
    }

    pub async fn get_account(&self, id: &str) -> Result<Option<AccountRow>> {
        let sql = format!(
            "SELECT a.*, \
             (SELECT COUNT(*) FROM account_members m WHERE m.account_id = a.id) AS member_count, \
             {} AS bucket_count FROM accounts a WHERE a.id = ?",
            Self::account_bucket_count_sql()
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(Self::account_row_from))
    }

    /// 新建账户。id 为 12 位随机数（与派生账户空间重叠概率可忽略，冲突重试）。
    pub async fn create_account(&self, name: &str, bucket_quota: Option<i64>) -> Result<String> {
        let ts = now();
        for _ in 0..8 {
            // random() 是有符号 64 位：先掩掉符号位再取模，否则 % 保留负号会生成负 id。
            let id: String = sqlx::query_scalar(
                "SELECT printf('%012d', 100000000000 + ((random() & 0x7fffffffffffffff) % 900000000000))",
            )
            .fetch_one(&self.pool)
            .await?;
            let n = sqlx::query(
                "INSERT OR IGNORE INTO accounts \
                 (id, name, status, bucket_quota, created_at, updated_at) VALUES (?, ?, 'active', ?, ?, ?)",
            )
            .bind(&id)
            .bind(name)
            .bind(bucket_quota)
            .bind(ts)
            .bind(ts)
            .execute(&self.pool)
            .await?
            .rows_affected();
            if n == 1 {
                return Ok(id);
            }
        }
        Err(sqlx::Error::Protocol(
            "account id generation collision (retry)".into(),
        ))
    }

    pub async fn rename_account(&self, id: &str, name: &str) -> Result<bool> {
        let n = sqlx::query("UPDATE accounts SET name = ?, updated_at = ? WHERE id = ?")
            .bind(name)
            .bind(now())
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n == 1)
    }

    /// 启停账户。停用是原子条件更新：账户里还有在册全局管理员时拒绝
    /// （否则该管理员会随账户一起被锁死，且可能锁死最后一个管理员）。
    pub async fn set_account_status(&self, id: &str, status: &str) -> Result<bool> {
        let n = if status == "active" {
            sqlx::query("UPDATE accounts SET status = 'active', updated_at = ? WHERE id = ?")
                .bind(now())
                .bind(id)
                .execute(&self.pool)
                .await?
                .rows_affected()
        } else {
            sqlx::query(
                "UPDATE accounts SET status = 'disabled', updated_at = ? WHERE id = ? AND NOT EXISTS (\
                   SELECT 1 FROM account_members am JOIN users u ON u.id = am.uid \
                   WHERE am.account_id = accounts.id AND u.is_admin = 1 AND u.status = 'active')",
            )
            .bind(now())
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
        };
        Ok(n == 1)
    }

    pub async fn set_account_quota(&self, id: &str, quota: Option<i64>) -> Result<bool> {
        let n = sqlx::query("UPDATE accounts SET bucket_quota = ?, updated_at = ? WHERE id = ?")
            .bind(quota)
            .bind(now())
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n == 1)
    }

    /// 删除账户：要求无成员且无桶（LEGACY 系统账户永不可删）。
    /// 返回 Ok(true)；账户不存在 / 非空不删（由 handler 区分）。
    pub async fn delete_account(&self, id: &str) -> Result<bool> {
        if id == xl_s3::access::LEGACY_ACCOUNT {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        let members: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = ?")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        let sql = format!(
            "SELECT {} FROM accounts a WHERE a.id = ?",
            Self::account_bucket_count_sql()
        );
        let buckets: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        if members != 0 || buckets.unwrap_or(0) != 0 {
            return Ok(false);
        }
        let n = sqlx::query("DELETE FROM accounts WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(n == 1)
    }

    pub async fn list_account_members(&self, id: &str) -> Result<Vec<AccountMemberRow>> {
        let rows = sqlx::query(
            "SELECT am.uid AS uid, u.username AS username, am.role AS role, \
                    u.is_admin AS is_admin, u.status AS status, am.created_at AS created_at \
             FROM account_members am JOIN users u ON u.id = am.uid \
             WHERE am.account_id = ? ORDER BY am.uid",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| AccountMemberRow {
                uid: r.get("uid"),
                username: r.get("username"),
                role: r.get("role"),
                is_admin: r.get::<i64, _>("is_admin") != 0,
                status: r.get("status"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    /// 某用户在该账户里的角色（不在账户 → None）。
    pub async fn member_role(&self, account_id: &str, uid: i64) -> Result<Option<String>> {
        let row = sqlx::query("SELECT role FROM account_members WHERE account_id = ? AND uid = ?")
            .bind(account_id)
            .bind(uid)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("role")))
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
        let user: Option<i64> =
            sqlx::query_scalar("SELECT id FROM users WHERE id = ? AND is_admin = 0")
                .bind(uid)
                .fetch_optional(&mut *tx)
                .await?;
        if user.is_none() {
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO account_members (account_id, uid, role, created_at) VALUES (?, ?, ?, ?) \
             ON CONFLICT(uid) DO UPDATE SET account_id = excluded.account_id, role = excluded.role",
        )
        .bind(account_id)
        .bind(uid)
        .bind(role)
        .bind(now())
        .execute(&mut *tx)
        .await?;
        if role == "admin" {
            sqlx::query("UPDATE accounts SET admin_uid = ? WHERE id = ? AND admin_uid IS NULL")
                .bind(uid)
                .bind(account_id)
                .execute(&mut *tx)
                .await?;
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
        let uid = sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, 0, ?)",
        )
        .bind(username)
        .bind(crate::db::hash_password(password))
        .bind(ts)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
        sqlx::query(
            "INSERT INTO account_members (account_id, uid, role, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(uid)
        .bind(role)
        .bind(ts)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(uid)
    }

    /// 某用户的权限行（全量）。
    pub async fn get_permissions(&self, uid: i64) -> Result<Vec<PermissionRow>> {
        let rows = sqlx::query_as::<_, PermissionRow>(
            "SELECT bucket, can_read, can_write, can_delete FROM permissions WHERE user_id = ? ORDER BY bucket",
        )
        .bind(uid)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// 全量替换某用户的权限行（管理员的 PUT 语义）。全 0 行跳过不写（无行 = 无权限，等价且省行）。
    pub async fn set_permissions(&self, uid: i64, rows: &[PermissionRow]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM permissions WHERE user_id = ?")
            .bind(uid)
            .execute(&mut *tx)
            .await?;
        for p in rows {
            if !(p.read || p.write || p.delete) {
                continue;
            }
            sqlx::query(
                "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(uid)
            .bind(&p.bucket)
            .bind(p.read as i64)
            .bind(p.write as i64)
            .bind(p.delete as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// 上传归属登记：仅当该记录尚无归属时写入（`AND owner_id IS NULL`）。
    /// 返回是否写入（已归属/记录不存在 → false，调用方 best-effort 忽略）。
    pub async fn set_upload_owner(&self, saved_path: &str, uid: i64) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE uploads SET owner_id = ? WHERE saved_path = ? AND owner_id IS NULL",
        )
        .bind(uid)
        .bind(saved_path)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n == 1)
    }

    /// 某用户自己的上传记录（子账户视野）。
    pub async fn list_uploads_for(&self, uid: i64, limit: i64) -> Result<Vec<UploadRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM uploads WHERE owner_id = ? ORDER BY created_at DESC, saved_path LIMIT ?",
        )
        .bind(uid)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| UploadRecord {
                saved_path: r.get("saved_path"),
                bucket: r.get("bucket"),
                key: r.get("key"),
                group_name: r.get("group_name"),
                size: r.get("size"),
                etag: r.get("etag"),
                original_name: r.get("original_name"),
                created_at: r.get("created_at"),
            })
            .collect())
    }
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

fn site_peer_from_row(r: &sqlx::sqlite::SqliteRow) -> SitePeer {
    SitePeer {
        peer_id: r.get("peer_id"),
        remote_site_id: r.get("remote_site_id"),
        name: r.get("name"),
        endpoint: r.get("endpoint"),
        region: r.get("region"),
        priority: r.get("priority"),
        s3_access_key: r.get("s3_access_key"),
        s3_secret_key: r.get("s3_secret_key"),
        peer_site_token: r.get("peer_site_token"),
        created_at: r.get("created_at"),
        last_seen: r.get("last_seen"),
        last_error: r.get("last_error"),
        raft_node_id: r.get::<Option<i64>, _>("raft_node_id").map(|v| v as u64),
        reconcile_required: r.get::<i64, _>("reconcile_required") != 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn db() -> Db {
        Db::connect("sqlite::memory:").await.unwrap()
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

    /// Task 1：0006 的用量表与 tenancy 补列在 connect 后就位（老库升级同样幂等）。
    #[tokio::test]
    async fn tenancy_schema_and_columns_exist() {
        let db = db().await;
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('accounts') WHERE name = 'soft_byte_quota'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(n, 1, "accounts.soft_byte_quota");
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
        db.create_access_point("ap1", "b1", 7, None, 1)
            .await
            .unwrap();
        db.create_access_point("ap2", "b2", 8, Some(r#"{"Statement":[]}"#), 2)
            .await
            .unwrap();
        let a = db.get_access_point("ap1").await.unwrap().unwrap();
        assert_eq!(a.bucket, "b1");
        assert_eq!(a.owner_id, 7);
        assert!(a.policy_json.is_none());
        assert_eq!(db.list_access_points(None).await.unwrap().len(), 2);
        let b1 = db.list_access_points(Some("b1")).await.unwrap();
        assert_eq!(b1.len(), 1);
        assert_eq!(b1[0].name, "ap1");
        // 更新策略 + 清空
        assert!(
            db.set_access_point_policy("ap1", Some(r#"{"Statement":[]}"#), 3)
                .await
                .unwrap()
        );
        assert!(
            db.get_access_point("ap1")
                .await
                .unwrap()
                .unwrap()
                .policy_json
                .is_some()
        );
        db.set_access_point_policy("ap1", None, 4).await.unwrap();
        assert!(
            db.get_access_point("ap1")
                .await
                .unwrap()
                .unwrap()
                .policy_json
                .is_none()
        );
        assert!(db.delete_access_point("ap2").await.unwrap());
        assert!(db.get_access_point("ap2").await.unwrap().is_none());
        assert!(!db.delete_access_point("missing").await.unwrap());
    }
}

// ——— M4 切片 6：生命周期规则与作业（jobs 表进度） ———

/// 生命周期规则行。M8 S0 扩为全字段（S4b 只做映射，不再改 db.rs）；
/// `expire_days` 为 M4 旧列（无 Expiration 的新规则写哨兵 -1），`expiration_days` 为新语义。
#[derive(Debug, Clone, PartialEq)]
pub struct LifecycleRuleRow {
    pub bucket: String,
    pub rule_id: Option<String>,
    pub prefix: String,
    pub expire_days: i64,
    pub expiration_days: Option<i64>,
    pub transition_days: Option<i64>,
    pub transition_class: Option<String>,
    pub transition_tier: Option<String>,
    pub enabled: bool,
    pub updated_at: i64,
    /// 结构化动作（None = 旧列可完整表达/旧 M4 行）。
    pub config_json: Option<String>,
}

fn row_to_lifecycle_rule(r: &sqlx::sqlite::SqliteRow) -> LifecycleRuleRow {
    LifecycleRuleRow {
        bucket: r.get("bucket"),
        rule_id: r.get("rule_id"),
        prefix: r.get("prefix"),
        expire_days: r.get("expire_days"),
        expiration_days: r.get("expiration_days"),
        transition_days: r.get("transition_days"),
        transition_class: r.get("transition_class"),
        transition_tier: r.get("transition_tier"),
        enabled: r.get::<i64, _>("enabled") != 0,
        updated_at: r.get("updated_at"),
        config_json: r.get("config_json"),
    }
}

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
        let rows = sqlx::query(
            "SELECT bucket, rule_id, prefix, expire_days, expiration_days, transition_days, \
             transition_class, transition_tier, enabled, updated_at, config_json \
             FROM lifecycle_rules WHERE bucket = ? ORDER BY rule_id, prefix",
        )
        .bind(bucket)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_lifecycle_rule).collect())
    }

    /// 全量替换某桶规则（PUT 语义：先删后插，一个事务）。`now` 由调用方给（可测）。
    pub async fn set_lifecycle_rules(
        &self,
        bucket: &str,
        rules: &[LifecycleRuleRow],
        now: i64,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        // 4.10：规则整桶替换 → 旧扫描游标作废（prefix/动作变了，续扫位置不再可信）。
        sqlx::query("DELETE FROM lifecycle_cursors WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *tx)
            .await?;
        for r in rules {
            sqlx::query(
                "INSERT INTO lifecycle_rules (bucket, rule_id, prefix, expire_days, expiration_days, \
                 transition_days, transition_class, transition_tier, enabled, updated_at, config_json) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(bucket)
            .bind(&r.rule_id)
            .bind(&r.prefix)
            .bind(r.expire_days)
            .bind(r.expiration_days)
            .bind(r.transition_days)
            .bind(&r.transition_class)
            .bind(&r.transition_tier)
            .bind(if r.enabled { 1 } else { 0 })
            .bind(now)
            .bind(&r.config_json)
            .execute(&mut *tx)
            .await?;
        }
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
        let row = sqlx::query(
            "SELECT cursor FROM lifecycle_cursors \
             WHERE bucket = ? AND rule_id = ? AND stage = ?",
        )
        .bind(bucket)
        .bind(rule_id)
        .bind(stage)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get("cursor")))
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
        match cursor {
            Some(c) if !c.is_empty() => {
                sqlx::query(
                    "INSERT INTO lifecycle_cursors \
                     (bucket, rule_id, stage, cursor, updated_at, fence_term, fence_leader) \
                     VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(bucket, rule_id, stage) DO UPDATE SET \
                     cursor = excluded.cursor, updated_at = excluded.updated_at, \
                     fence_term = excluded.fence_term, fence_leader = excluded.fence_leader \
                     WHERE lifecycle_cursors.fence_term <= excluded.fence_term",
                )
                .bind(bucket)
                .bind(rule_id)
                .bind(stage)
                .bind(c)
                .bind(now)
                .bind(fence.term_i64())
                .bind(fence.leader_i64())
                .execute(&self.pool)
                .await?;
            }
            _ => {
                sqlx::query(
                    "DELETE FROM lifecycle_cursors \
                     WHERE bucket = ? AND rule_id = ? AND stage = ? AND fence_term <= ?",
                )
                .bind(bucket)
                .bind(rule_id)
                .bind(stage)
                .bind(fence.term_i64())
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }

    /// 全部启用规则（worker 扫描用）。
    pub async fn enabled_lifecycle_rules(&self) -> Result<Vec<LifecycleRuleRow>> {
        let rows = sqlx::query(
            "SELECT bucket, rule_id, prefix, expire_days, expiration_days, transition_days, \
             transition_class, transition_tier, enabled, updated_at, config_json \
             FROM lifecycle_rules WHERE enabled = 1 ORDER BY bucket, rule_id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_lifecycle_rule).collect())
    }

    /// 开一条作业（worker 每次执行一条）。
    pub async fn job_start(&self, kind: &str, bucket: &str, prefix: &str, now: i64) -> Result<i64> {
        let id = sqlx::query(
            "INSERT INTO jobs (kind, bucket, prefix, state, created_at, updated_at) \
             VALUES (?, ?, ?, 'running', ?, ?)",
        )
        .bind(kind)
        .bind(bucket)
        .bind(prefix)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?
        .last_insert_rowid();
        Ok(id)
    }

    /// 收尾一条作业（done/error + 计数 + 可选错误）。
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
        sqlx::query(
            "UPDATE jobs SET state = ?, scanned = ?, deleted = ?, skipped = ?, error = ?, \
             updated_at = ? WHERE id = ?",
        )
        .bind(state)
        .bind(scanned)
        .bind(deleted)
        .bind(skipped)
        .bind(error)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 最近作业（控制台 GET /jobs；最新在前）。列投影与 `job_get` 同值。
    pub async fn list_jobs(&self, limit: i64) -> Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            "SELECT id, kind, bucket, prefix, state, scanned, deleted, skipped, error, \
             priority, total, succeeded, failed, requested_by, report_bucket, report_prefix, \
             parent_job_id, started_at, finished_at, created_at, updated_at \
             FROM jobs ORDER BY id DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.get::<i64, _>("id"),
                    "kind": r.get::<String, _>("kind"),
                    "bucket": r.get::<String, _>("bucket"),
                    "prefix": r.get::<String, _>("prefix"),
                    "state": r.get::<String, _>("state"),
                    "scanned": r.get::<i64, _>("scanned"),
                    "deleted": r.get::<i64, _>("deleted"),
                    "skipped": r.get::<i64, _>("skipped"),
                    "error": r.get::<Option<String>, _>("error"),
                    "priority": r.get::<i64, _>("priority"),
                    "total": r.get::<i64, _>("total"),
                    "succeeded": r.get::<i64, _>("succeeded"),
                    "failed": r.get::<i64, _>("failed"),
                    "requested_by": r.get::<Option<i64>, _>("requested_by"),
                    "report_bucket": r.get::<Option<String>, _>("report_bucket"),
                    "report_prefix": r.get::<Option<String>, _>("report_prefix"),
                    "parent_job_id": r.get::<Option<i64>, _>("parent_job_id"),
                    "started_at": r.get::<Option<i64>, _>("started_at"),
                    "finished_at": r.get::<Option<i64>, _>("finished_at"),
                    "created_at": r.get::<i64, _>("created_at"),
                    "updated_at": r.get::<i64, _>("updated_at"),
                })
            })
            .collect())
    }

    // ——— M8 S0：作业引擎（jobs/job_tasks） ———

    /// 建作业 + 任务（同一事务）。任务 `task_no` 从 1 起；作业初始 `queued`。
    pub async fn create_job(&self, spec: &JobSpec, now: i64) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let id = sqlx::query(
            "INSERT INTO jobs (kind, bucket, prefix, state, total, priority, requested_by, \
             parent_job_id, report_bucket, report_prefix, spec_json, created_at, updated_at) \
             VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&spec.kind)
        .bind(&spec.bucket)
        .bind(&spec.prefix)
        .bind(spec.tasks.len() as i64)
        .bind(spec.priority)
        .bind(spec.requested_by)
        .bind(spec.parent_job_id)
        .bind(&spec.report_bucket)
        .bind(&spec.report_prefix)
        .bind(&spec.spec_json)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
        for (i, t) in spec.tasks.iter().enumerate() {
            sqlx::query(
                "INSERT INTO job_tasks (job_id, task_no, action, payload_json, state, next_at, dedupe_key) \
                 VALUES (?, ?, ?, ?, 'queued', ?, ?)",
            )
            .bind(id)
            .bind(i as i64 + 1)
            .bind(&t.action)
            .bind(&t.payload)
            .bind(now)
            .bind(&t.dedupe_key)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(id)
    }

    /// 建带 dedupe 的作业：任务 `INSERT OR IGNORE`，一条都没插入（全被去重）→
    /// 整个事务回滚并返回 None（C7：不产生空作业）。
    pub async fn create_job_dedup(&self, spec: &JobSpec, now: i64) -> Result<Option<i64>> {
        let mut tx = self.pool.begin().await?;
        let id = sqlx::query(
            "INSERT INTO jobs (kind, bucket, prefix, state, total, priority, requested_by, \
             parent_job_id, report_bucket, report_prefix, spec_json, created_at, updated_at) \
             VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&spec.kind)
        .bind(&spec.bucket)
        .bind(&spec.prefix)
        .bind(spec.tasks.len() as i64)
        .bind(spec.priority)
        .bind(spec.requested_by)
        .bind(spec.parent_job_id)
        .bind(&spec.report_bucket)
        .bind(&spec.report_prefix)
        .bind(&spec.spec_json)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
        let mut inserted = 0i64;
        for (i, t) in spec.tasks.iter().enumerate() {
            let n = sqlx::query(
                "INSERT OR IGNORE INTO job_tasks \
                 (job_id, task_no, action, payload_json, state, next_at, dedupe_key) \
                 VALUES (?, ?, ?, ?, 'queued', ?, ?)",
            )
            .bind(id)
            .bind(i as i64 + 1)
            .bind(&t.action)
            .bind(&t.payload)
            .bind(now)
            .bind(&t.dedupe_key)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            inserted += n as i64;
        }
        if inserted == 0 {
            tx.rollback().await?;
            return Ok(None);
        }
        if inserted != spec.tasks.len() as i64 {
            sqlx::query("UPDATE jobs SET total = ? WHERE id = ?")
                .bind(inserted)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(Some(id))
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
        let id = sqlx::query(
            "INSERT INTO jobs (kind, bucket, prefix, state, total, priority, requested_by, \
             parent_job_id, report_bucket, report_prefix, spec_json, created_at, updated_at) \
             VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&spec.kind)
        .bind(&spec.bucket)
        .bind(&spec.prefix)
        .bind(spec.tasks.len() as i64)
        .bind(spec.priority)
        .bind(spec.requested_by)
        .bind(spec.parent_job_id)
        .bind(&spec.report_bucket)
        .bind(&spec.report_prefix)
        .bind(&spec.spec_json)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
        let mut inserted = false;
        for (i, t) in spec.tasks.iter().enumerate() {
            let n = sqlx::query(
                "INSERT OR IGNORE INTO job_tasks \
                 (job_id, task_no, action, payload_json, state, next_at, dedupe_key) \
                 VALUES (?, ?, ?, ?, 'queued', ?, ?)",
            )
            .bind(id)
            .bind(i as i64 + 1)
            .bind(&t.action)
            .bind(&t.payload)
            .bind(now)
            .bind(&t.dedupe_key)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            inserted |= n > 0;
        }
        sqlx::query(
            "INSERT INTO tier_restores (bucket, key, version_id, expires_at, job_id, state, created_at, \
             fence_term, fence_leader) \
             VALUES (?, ?, ?, 0, ?, 'pending', ?, ?, ?) \
             ON CONFLICT(bucket, key, version_id) DO UPDATE SET \
             expires_at = 0, job_id = excluded.job_id, state = 'pending', created_at = excluded.created_at, \
             fence_term = MAX(tier_restores.fence_term, excluded.fence_term), \
             fence_leader = CASE WHEN excluded.fence_term >= tier_restores.fence_term \
               THEN excluded.fence_leader ELSE tier_restores.fence_leader END \
             WHERE tier_restores.fence_term <= excluded.fence_term",
        )
        .bind(bucket)
        .bind(key)
        .bind(version_id)
        .bind(id)
        .bind(now)
        .bind(fence.term_i64())
        .bind(fence.leader_i64())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok((id, inserted))
    }

    /// 某状态的 Restore 行（reaper/对账）。
    pub async fn restore_rows(&self, state: &str) -> Result<Vec<RestoreRow>> {
        let rows = sqlx::query(
            "SELECT bucket, key, version_id, expires_at, job_id, state, created_at \
             FROM tier_restores WHERE state = ? ORDER BY created_at",
        )
        .bind(state)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_restore).collect())
    }

    pub async fn restore_row(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<RestoreRow>> {
        let row = sqlx::query(
            "SELECT bucket, key, version_id, expires_at, job_id, state, created_at \
             FROM tier_restores WHERE bucket = ? AND key = ? AND version_id = ?",
        )
        .bind(bucket)
        .bind(key)
        .bind(version_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(row_to_restore))
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
        sqlx::query(
            "UPDATE tier_restores SET state = ?, expires_at = ?, \
             fence_term = ?, fence_leader = ? \
             WHERE bucket = ? AND key = ? AND version_id = ? AND fence_term <= ?",
        )
        .bind(state)
        .bind(expires_at)
        .bind(fence.term_i64())
        .bind(fence.leader_i64())
        .bind(bucket)
        .bind(key)
        .bind(version_id)
        .bind(fence.term_i64())
        .execute(&self.pool)
        .await?;
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
        sqlx::query(
            "DELETE FROM tier_restores \
             WHERE bucket = ? AND key = ? AND version_id = ? AND fence_term <= ?",
        )
        .bind(bucket)
        .bind(key)
        .bind(version_id)
        .bind(fence.term_i64())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 作业是否仍有 queued/running 任务（启动对账判断是否需要重排）。
    pub async fn job_task_active(&self, job_id: i64) -> Result<bool> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_tasks WHERE job_id = ? AND state IN ('queued','running')",
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(n > 0)
    }

    /// `(该用户, 全局)` 的 inflight 作业数（queued/running/paused/preempted）。
    pub async fn job_inflight_counts(&self, requested_by: Option<i64>) -> Result<(i64, i64)> {
        let user: i64 = match requested_by {
            Some(uid) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM jobs WHERE requested_by = ? \
                     AND state IN ('queued','running','paused','preempted')",
                )
                .bind(uid)
                .fetch_one(&self.pool)
                .await?
            }
            None => 0,
        };
        let global: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs \
             WHERE state IN ('queued','running','paused','preempted')",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok((user, global))
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
        sqlx::raw_sql("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        let out = async {
            sqlx::query(
                "UPDATE jobs SET state = 'running', started_at = COALESCE(started_at, ?), updated_at = ? \
                 WHERE state = 'queued' AND EXISTS (SELECT 1 FROM job_tasks t \
                 WHERE t.job_id = jobs.id AND t.state IN ('queued','running'))",
            )
            .bind(now)
            .bind(now)
            .execute(&mut *conn)
            .await?;
            let row = sqlx::query(
                "SELECT t.job_id, t.task_no, t.action, t.payload_json, t.attempt \
                 FROM job_tasks t JOIN jobs j ON j.id = t.job_id \
                 WHERE t.state = 'queued' AND t.next_at <= ? AND j.state = 'running' \
                   AND t.fence_term <= ? \
                 ORDER BY j.priority DESC, t.next_at, t.task_no LIMIT 1",
            )
            .bind(now)
            .bind(fence.term_i64())
            .fetch_optional(&mut *conn)
            .await?;
            let Some(r) = row else {
                return Ok::<Option<ClaimedTask>, sqlx::Error>(None);
            };
            let (job_id, task_no): (i64, i64) = (r.get("job_id"), r.get("task_no"));
            sqlx::query(
                "UPDATE job_tasks SET state = 'running', started_at = ?, deadline_at = ?, \
                 fence_term = ?, fence_leader = ? \
                 WHERE job_id = ? AND task_no = ? AND state = 'queued' AND fence_term <= ?",
            )
            .bind(now)
            .bind(now + timeout_secs)
            .bind(fence.term_i64())
            .bind(fence.leader_i64())
            .bind(job_id)
            .bind(task_no)
            .bind(fence.term_i64())
            .execute(&mut *conn)
            .await?;
            Ok(Some(ClaimedTask {
                job_id,
                task_no,
                action: r.get("action"),
                payload_json: r.get("payload_json"),
                attempt: r.get("attempt"),
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
        sqlx::query(
            "UPDATE job_tasks SET deadline_at = ? \
             WHERE job_id = ? AND task_no = ? AND state = 'running' AND fence_term = ?",
        )
        .bind(deadline_at)
        .bind(job_id)
        .bind(task_no)
        .bind(fence.term_i64())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 作业当前状态（None = 不存在）。
    pub async fn job_state(&self, id: i64) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT state FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
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
        let n = sqlx::query(
            "UPDATE job_tasks SET state = 'queued', started_at = NULL, deadline_at = NULL, \
             next_at = ? WHERE job_id = ? AND task_no = ? AND state = 'running' \
             AND fence_term = ?",
        )
        .bind(now)
        .bind(job_id)
        .bind(task_no)
        .bind(fence.term_i64())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
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
        let running: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM job_tasks WHERE state = 'running'")
                .fetch_one(&mut *tx)
                .await?;
        if running < capacity as i64 {
            tx.commit().await?;
            return Ok(0);
        }
        let n = sqlx::query(
            "UPDATE jobs SET state = 'preempted', updated_at = ? \
             WHERE state = 'running' \
             AND EXISTS (SELECT 1 FROM job_tasks WHERE job_id = jobs.id AND state = 'queued') \
             AND EXISTS ( \
               SELECT 1 FROM jobs h JOIN job_tasks t ON t.job_id = h.id \
               WHERE h.state IN ('queued','running') \
                 AND h.priority > jobs.priority \
                 AND h.requested_by IS jobs.requested_by \
                 AND t.state = 'queued' AND t.next_at <= ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(n as usize)
    }

    /// 释放抢占：同一 `requested_by` 内高等优先级的**就绪**排队任务已清空（都已领取
    /// 或完成）后，把 `preempted` 作业放回 `queued`（下一条领取按正常优先级顺序恢复）。
    /// 返回释放数。正在运行的高优先级任务不阻塞释放——空闲容量应回到低优先级作业，
    /// 领取顺序仍保证后续高优先级任务先被取走。
    pub async fn release_preempted(&self, now: i64) -> Result<usize> {
        let n = sqlx::query(
            "UPDATE jobs SET state = 'queued', updated_at = ? \
             WHERE state = 'preempted' AND NOT EXISTS ( \
               SELECT 1 FROM jobs h JOIN job_tasks t ON t.job_id = h.id \
               WHERE h.id != jobs.id AND h.priority > jobs.priority \
                 AND h.requested_by IS jobs.requested_by \
                 AND h.state IN ('queued','running') \
                 AND t.state = 'queued' AND t.next_at <= ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n as usize)
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
        let row = sqlx::query(
            "SELECT state, attempt, fence_term FROM job_tasks WHERE job_id = ? AND task_no = ?",
        )
        .bind(job_id)
        .bind(task_no)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(r) = row else {
            tx.commit().await?;
            return Ok("missing");
        };
        if r.get::<String, _>("state") != "running" {
            tx.commit().await?;
            return Ok("not-running");
        }
        if r.get::<i64, _>("fence_term") > fence.term_i64() {
            tx.commit().await?;
            return Ok("fenced");
        }
        let attempt: i64 = r.get("attempt");
        if ok {
            // C7：终态清空 dedupe_key（成功后同键可再次入队）。
            sqlx::query(
                "UPDATE job_tasks SET state = 'done', result_code = ?, result_string = ?, \
                 finished_at = ?, dedupe_key = NULL \
                 WHERE job_id = ? AND task_no = ? AND fence_term <= ?",
            )
            .bind(code)
            .bind(message)
            .bind(now)
            .bind(job_id)
            .bind(task_no)
            .bind(fence.term_i64())
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE jobs SET succeeded = succeeded + 1, progress = progress + 1, updated_at = ? \
                 WHERE id = ?",
            )
            .bind(now)
            .bind(job_id)
            .execute(&mut *tx)
            .await?;
        } else if attempt + 1 < max_attempts {
            let backoff = 10i64
                .saturating_mul(2i64.saturating_pow(attempt.min(30) as u32))
                .min(300)
                + (now % 1000).abs();
            sqlx::query(
                "UPDATE job_tasks SET state = 'queued', attempt = attempt + 1, next_at = ?, \
                 result_code = ?, result_string = ?, started_at = NULL, deadline_at = NULL \
                 WHERE job_id = ? AND task_no = ? AND fence_term <= ?",
            )
            .bind(now + backoff)
            .bind(code)
            .bind(message)
            .bind(job_id)
            .bind(task_no)
            .bind(fence.term_i64())
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE jobs SET updated_at = ? WHERE id = ?")
                .bind(now)
                .bind(job_id)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(
                "UPDATE job_tasks SET state = 'failed', attempt = attempt + 1, \
                 result_code = 'PermanentFailure', result_string = ?, finished_at = ?, \
                 dedupe_key = NULL WHERE job_id = ? AND task_no = ? AND fence_term <= ?",
            )
            .bind(message)
            .bind(now)
            .bind(job_id)
            .bind(task_no)
            .bind(fence.term_i64())
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE jobs SET failed = failed + 1, progress = progress + 1, updated_at = ? \
                 WHERE id = ?",
            )
            .bind(now)
            .bind(job_id)
            .execute(&mut *tx)
            .await?;
        }
        // 作业收敛：仍 running/preempted 且无未终态任务 → done（无 failed）/error。
        // preempted 也要收敛：被抢占时在途动作完成后可能已无剩余任务。
        sqlx::query(
            "UPDATE jobs SET state = CASE WHEN failed > 0 THEN 'error' ELSE 'done' END, \
             finished_at = ?, updated_at = ? \
             WHERE id = ? AND state IN ('running','preempted') AND NOT EXISTS \
             (SELECT 1 FROM job_tasks WHERE job_id = ? AND state IN ('queued','running'))",
        )
        .bind(now)
        .bind(now)
        .bind(job_id)
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(if ok {
            "done"
        } else if attempt + 1 < max_attempts {
            "queued"
        } else {
            "failed"
        })
    }

    /// 超时回收：running 且 deadline 过期 → 按一次失败处理。返回回收条数。
    /// M8-1：接管时把行重标为当前 token，旧 holder 的迟到 finish 因此被拒。
    pub async fn recover_expired_tasks(
        &self,
        now: i64,
        max_attempts: i64,
        fence: FencingEpoch,
    ) -> Result<usize> {
        let rows = sqlx::query(
            "SELECT job_id, task_no FROM job_tasks \
             WHERE state = 'running' AND deadline_at IS NOT NULL AND deadline_at < ? \
               AND fence_term <= ?",
        )
        .bind(now)
        .bind(fence.term_i64())
        .fetch_all(&self.pool)
        .await?;
        for r in &rows {
            self.take_over_task(r.get("job_id"), r.get("task_no"), None, now, fence)
                .await?;
            self.finish_task_fenced(
                r.get("job_id"),
                r.get("task_no"),
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
        let deadline_cond = if started_before.is_some() {
            "started_at IS NOT NULL AND started_at < ?"
        } else {
            "deadline_at IS NOT NULL AND deadline_at < ?"
        };
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE job_tasks SET fence_term = ?, fence_leader = ? \
             WHERE job_id = ? AND task_no = ? AND state = 'running' AND {deadline_cond} \
               AND fence_term <= ?"
        )))
        .bind(fence.term_i64())
        .bind(fence.leader_i64())
        .bind(job_id)
        .bind(task_no)
        .bind(started_before.unwrap_or(now))
        .bind(fence.term_i64())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 4.11/M2：回收「state='running' 但没有任何 job_tasks」的作业行。
    /// lifecycle/inventory 等单条作业只写 jobs 行；进程在 job_start 之后崩溃会永久停在
    /// running。`cutoff` 限定 updated_at（长作业靠心跳续命，避免把仍在跑的扫描误标 error）。
    pub async fn recover_orphan_jobs(&self, cutoff: i64, now: i64) -> Result<usize> {
        let n = sqlx::query(
            "UPDATE jobs SET state = 'error', \
             error = COALESCE(error, '进程崩溃遗留的 running 作业，已自动回收'), \
             finished_at = ?, updated_at = ? \
             WHERE state = 'running' AND updated_at <= ? \
             AND NOT EXISTS (SELECT 1 FROM job_tasks WHERE job_tasks.job_id = jobs.id)",
        )
        .bind(now)
        .bind(now)
        .bind(cutoff)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n as usize)
    }

    /// M2：长作业心跳（只续 running 行），让 orphan 回收按 liveness 而非创建时间判断。
    pub async fn job_heartbeat(&self, job_id: i64, now: i64) -> Result<()> {
        sqlx::query("UPDATE jobs SET updated_at = ? WHERE id = ? AND state = 'running'")
            .bind(now)
            .bind(job_id)
            .execute(&self.pool)
            .await?;
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
        let rows = sqlx::query(
            "SELECT job_id, task_no FROM job_tasks \
             WHERE state = 'running' AND started_at IS NOT NULL AND started_at < ? \
               AND fence_term <= ?",
        )
        .bind(cutoff)
        .bind(fence.term_i64())
        .fetch_all(&self.pool)
        .await?;
        let now = crate::db::now();
        for r in &rows {
            self.take_over_task(r.get("job_id"), r.get("task_no"), Some(cutoff), now, fence)
                .await?;
            self.finish_task_fenced(
                r.get("job_id"),
                r.get("task_no"),
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
        let row = sqlx::query(
            "SELECT id, kind, bucket, prefix, state, spec_json, progress, total, succeeded, \
             failed, report_bucket, report_prefix, priority, requested_by, parent_job_id, \
             error, created_at, started_at, finished_at, updated_at FROM jobs WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            serde_json::json!({
                "id": r.get::<i64, _>("id"),
                "kind": r.get::<String, _>("kind"),
                "bucket": r.get::<String, _>("bucket"),
                "prefix": r.get::<String, _>("prefix"),
                "state": r.get::<String, _>("state"),
                "spec_json": r.get::<Option<String>, _>("spec_json"),
                "progress": r.get::<i64, _>("progress"),
                "total": r.get::<i64, _>("total"),
                "succeeded": r.get::<i64, _>("succeeded"),
                "failed": r.get::<i64, _>("failed"),
                "report_bucket": r.get::<Option<String>, _>("report_bucket"),
                "report_prefix": r.get::<Option<String>, _>("report_prefix"),
                "priority": r.get::<i64, _>("priority"),
                "requested_by": r.get::<Option<i64>, _>("requested_by"),
                "parent_job_id": r.get::<Option<i64>, _>("parent_job_id"),
                "error": r.get::<Option<String>, _>("error"),
                "created_at": r.get::<i64, _>("created_at"),
                "started_at": r.get::<Option<i64>, _>("started_at"),
                "finished_at": r.get::<Option<i64>, _>("finished_at"),
                "updated_at": r.get::<i64, _>("updated_at"),
            })
        }))
    }

    /// 作业级错误文案（报告写失败等；不改任务终态）。
    pub async fn job_set_error(&self, id: i64, msg: &str, now: i64) -> Result<()> {
        sqlx::query("UPDATE jobs SET error = ?, updated_at = ? WHERE id = ?")
            .bind(msg)
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 作业归属/状态（API 判权用）。
    pub async fn job_owner(&self, id: i64) -> Result<Option<(Option<i64>, String)>> {
        let row = sqlx::query("SELECT requested_by, state FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| (r.get("requested_by"), r.get("state"))))
    }

    /// 任务分页（task_no 升序）。
    pub async fn job_tasks(
        &self,
        id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            "SELECT task_no, action, state, attempt, next_at, result_code, result_string, \
             started_at, finished_at FROM job_tasks WHERE job_id = ? \
             ORDER BY task_no LIMIT ? OFFSET ?",
        )
        .bind(id)
        .bind(limit.clamp(1, 500))
        .bind(offset.max(0))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "task_no": r.get::<i64, _>("task_no"),
                    "action": r.get::<String, _>("action"),
                    "state": r.get::<String, _>("state"),
                    "attempt": r.get::<i64, _>("attempt"),
                    "next_at": r.get::<i64, _>("next_at"),
                    "result_code": r.get::<Option<String>, _>("result_code"),
                    "result_string": r.get::<Option<String>, _>("result_string"),
                    "started_at": r.get::<Option<i64>, _>("started_at"),
                    "finished_at": r.get::<Option<i64>, _>("finished_at"),
                })
            })
            .collect())
    }

    /// 状态操作：`pause`/`resume`/`cancel`。返回是否有行被改（false = 不存在或状态不允许）。
    pub async fn job_set_state(&self, id: i64, action: &str, now: i64) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let n = match action {
            "pause" => sqlx::query(
                "UPDATE jobs SET state = 'paused', updated_at = ? \
                 WHERE id = ? AND state IN ('running','preempted')",
            )
            .bind(now)
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected(),
            "resume" => sqlx::query(
                "UPDATE jobs SET state = 'running', updated_at = ? WHERE id = ? AND state = 'paused'",
            )
            .bind(now)
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected(),
            "cancel" => {
                let n = sqlx::query(
                    "UPDATE jobs SET state = 'cancelled', updated_at = ? \
                     WHERE id = ? AND state IN ('queued','running','paused','preempted')",
                )
                .bind(now)
                .bind(id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if n > 0 {
                    sqlx::query(
                        "UPDATE job_tasks SET state = 'cancelled', finished_at = ? \
                         WHERE job_id = ? AND state = 'queued'",
                    )
                    .bind(now)
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                }
                n
            }
            _ => 0,
        };
        tx.commit().await?;
        Ok(n > 0)
    }

    /// 失败任务重试：建子作业（只含 failed 任务，原 task_no 保留为顺序），返回新 job id。
    pub async fn job_retry_failed(&self, id: i64, now: i64) -> Result<Option<i64>> {
        let mut tx = self.pool.begin().await?;
        let job = sqlx::query(
            "SELECT kind, bucket, prefix, priority, requested_by, report_bucket, report_prefix, spec_json \
             FROM jobs WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(j) = job else {
            tx.commit().await?;
            return Ok(None);
        };
        let tasks = sqlx::query(
            "SELECT action, payload_json FROM job_tasks WHERE job_id = ? AND state = 'failed' \
             ORDER BY task_no",
        )
        .bind(id)
        .fetch_all(&mut *tx)
        .await?;
        if tasks.is_empty() {
            tx.commit().await?;
            return Ok(None);
        }
        let new_id = sqlx::query(
            "INSERT INTO jobs (kind, bucket, prefix, state, total, priority, requested_by, \
             parent_job_id, report_bucket, report_prefix, spec_json, created_at, updated_at) \
             VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(j.get::<String, _>("kind"))
        .bind(j.get::<String, _>("bucket"))
        .bind(j.get::<String, _>("prefix"))
        .bind(tasks.len() as i64)
        .bind(j.get::<i64, _>("priority"))
        .bind(j.get::<Option<i64>, _>("requested_by"))
        .bind(id)
        .bind(j.get::<Option<String>, _>("report_bucket"))
        .bind(j.get::<Option<String>, _>("report_prefix"))
        .bind(j.get::<Option<String>, _>("spec_json"))
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
        for (i, t) in tasks.iter().enumerate() {
            sqlx::query(
                "INSERT INTO job_tasks (job_id, task_no, action, payload_json, state, next_at) \
                 VALUES (?, ?, ?, ?, 'queued', ?)",
            )
            .bind(new_id)
            .bind(i as i64 + 1)
            .bind(t.get::<String, _>("action"))
            .bind(t.get::<String, _>("payload_json"))
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(Some(new_id))
    }

    /// 报告行（含 `payload_json`）：S3 Batch 报告需要从载荷回读 bucket/key/versionId。
    /// 与 [`Self::job_tasks`] 分列——控制台分页不暴露内部载荷。
    pub async fn job_report_rows(&self, id: i64) -> Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            "SELECT task_no, payload_json, result_code, result_string, started_at, finished_at \
             FROM job_tasks WHERE job_id = ? ORDER BY task_no",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "task_no": r.get::<i64, _>("task_no"),
                    "payload_json": r.get::<String, _>("payload_json"),
                    "result_code": r.get::<Option<String>, _>("result_code"),
                    "result_string": r.get::<Option<String>, _>("result_string"),
                    "started_at": r.get::<Option<i64>, _>("started_at"),
                    "finished_at": r.get::<Option<i64>, _>("finished_at"),
                })
            })
            .collect())
    }

    // ——— M8 S5：Inventory 配置与运行行 ———

    pub async fn inventory_config_count(&self, bucket: &str) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS n FROM inventory_configs WHERE bucket = ?")
            .bind(bucket)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get("n"))
    }

    pub async fn inventory_get_config(
        &self,
        bucket: &str,
        id: &str,
    ) -> Result<Option<InventoryConfigRow>> {
        let row = sqlx::query(
            "SELECT bucket, id, owner_id, config_json, updated_at FROM inventory_configs \
             WHERE bucket = ? AND id = ?",
        )
        .bind(bucket)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| InventoryConfigRow {
            bucket: r.get("bucket"),
            id: r.get("id"),
            owner_id: r.get("owner_id"),
            config_json: r.get("config_json"),
            updated_at: r.get("updated_at"),
        }))
    }

    pub async fn inventory_list_configs(&self, bucket: &str) -> Result<Vec<InventoryConfigRow>> {
        let rows = sqlx::query(
            "SELECT bucket, id, owner_id, config_json, updated_at FROM inventory_configs \
             WHERE bucket = ? ORDER BY id",
        )
        .bind(bucket)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| InventoryConfigRow {
                bucket: r.get("bucket"),
                id: r.get("id"),
                owner_id: r.get("owner_id"),
                config_json: r.get("config_json"),
                updated_at: r.get("updated_at"),
            })
            .collect())
    }

    /// 生成器用：全部桶的配置（ORDER BY bucket, id 保证调度确定性）。
    pub async fn inventory_list_all_configs(&self) -> Result<Vec<InventoryConfigRow>> {
        let rows = sqlx::query(
            "SELECT bucket, id, owner_id, config_json, updated_at FROM inventory_configs \
             ORDER BY bucket, id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| InventoryConfigRow {
                bucket: r.get("bucket"),
                id: r.get("id"),
                owner_id: r.get("owner_id"),
                config_json: r.get("config_json"),
                updated_at: r.get("updated_at"),
            })
            .collect())
    }

    pub async fn inventory_upsert_config(
        &self,
        bucket: &str,
        id: &str,
        owner_id: i64,
        config_json: &str,
        now: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) \
             VALUES (?, ?, ?, ?, ?) ON CONFLICT(bucket, id) DO UPDATE SET \
             owner_id = excluded.owner_id, config_json = excluded.config_json, \
             updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(id)
        .bind(owner_id)
        .bind(config_json)
        .bind(now)
        .execute(&self.pool)
        .await?;
        // 3.6：配置变更 → 旧增量水位/模式失效（console 随后按请求重新设置模式）。
        sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn inventory_delete_config(&self, bucket: &str, id: &str) -> Result<bool> {
        sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&self.pool)
            .await?;
        let r = sqlx::query("DELETE FROM inventory_configs WHERE bucket = ? AND id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected() > 0)
    }

    /// 3.6：Inventory 增量模式（默认全量；本地表，不进 Raft）。
    pub async fn inventory_incremental_get(&self, bucket: &str, config_id: &str) -> Result<bool> {
        let row = sqlx::query(
            "SELECT enabled FROM inventory_incremental WHERE bucket = ? AND config_id = ?",
        )
        .bind(bucket)
        .bind(config_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some_and(|r| r.get::<i64, _>("enabled") != 0))
    }

    /// 3.6：设置增量模式；关闭时连旧水位一起清掉（重新开启先跑全量）。
    pub async fn inventory_incremental_set(
        &self,
        bucket: &str,
        config_id: &str,
        enabled: bool,
        now: i64,
    ) -> Result<()> {
        if enabled {
            sqlx::query(
                "INSERT INTO inventory_incremental (bucket, config_id, enabled, updated_at) \
                 VALUES (?, ?, 1, ?) ON CONFLICT(bucket, config_id) DO UPDATE SET \
                 enabled = 1, updated_at = excluded.updated_at",
            )
            .bind(bucket)
            .bind(config_id)
            .bind(now)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
                .bind(bucket)
                .bind(config_id)
                .execute(&self.pool)
                .await?;
            sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
                .bind(bucket)
                .bind(config_id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// 3.6：读取增量水位；`config_updated_at` 不匹配（配置已改）→ None，从头全量。
    pub async fn inventory_cursor_get(
        &self,
        bucket: &str,
        config_id: &str,
        config_updated_at: i64,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT cursor FROM inventory_cursors \
             WHERE bucket = ? AND config_id = ? AND config_updated_at = ?",
        )
        .bind(bucket)
        .bind(config_id)
        .bind(config_updated_at)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get("cursor")))
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
        match cursor {
            Some(c) if !c.is_empty() => {
                sqlx::query(
                    "INSERT INTO inventory_cursors \
                     (bucket, config_id, cursor, config_updated_at, updated_at, fence_term, fence_leader) \
                     VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(bucket, config_id) DO UPDATE SET \
                     cursor = excluded.cursor, \
                     config_updated_at = excluded.config_updated_at, \
                     updated_at = excluded.updated_at, \
                     fence_term = excluded.fence_term, fence_leader = excluded.fence_leader \
                     WHERE inventory_cursors.fence_term <= excluded.fence_term",
                )
                .bind(bucket)
                .bind(config_id)
                .bind(c)
                .bind(config_updated_at)
                .bind(now)
                .bind(fence.term_i64())
                .bind(fence.leader_i64())
                .execute(&self.pool)
                .await?;
            }
            _ => {
                sqlx::query(
                    "DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ? \
                     AND fence_term <= ?",
                )
                .bind(bucket)
                .bind(config_id)
                .bind(fence.term_i64())
                .execute(&self.pool)
                .await?;
            }
        }
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
        let r = sqlx::query(
            "INSERT OR IGNORE INTO inventory_runs \
             (bucket, config_id, run_key, started_at, state, created_at, fence_term, fence_leader) \
             VALUES (?, ?, ?, 0, 'running', ?, ?, ?)",
        )
        .bind(bucket)
        .bind(config_id)
        .bind(run_key)
        .bind(now)
        .bind(fence.term_i64())
        .bind(fence.leader_i64())
        .execute(&self.pool)
        .await?;
        Ok((r.rows_affected() > 0).then(|| r.last_insert_rowid()))
    }

    pub async fn inventory_run_set_job(
        &self,
        run_id: i64,
        job_id: i64,
        fence: FencingEpoch,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE inventory_runs SET job_id = ?, fence_term = ?, fence_leader = ? \
             WHERE id = ? AND fence_term <= ?",
        )
        .bind(job_id)
        .bind(fence.term_i64())
        .bind(fence.leader_i64())
        .bind(run_id)
        .bind(fence.term_i64())
        .execute(&self.pool)
        .await?;
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
        sqlx::query(
            "UPDATE inventory_runs SET \
             started_at = CASE WHEN started_at = 0 THEN ? ELSE started_at END, \
             fence_term = ?, fence_leader = ? WHERE id = ? AND fence_term <= ?",
        )
        .bind(now)
        .bind(fence.term_i64())
        .bind(fence.leader_i64())
        .bind(run_id)
        .bind(fence.term_i64())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn inventory_run_by_key(
        &self,
        bucket: &str,
        config_id: &str,
        run_key: &str,
    ) -> Result<Option<InventoryRunRow>> {
        let row = sqlx::query(
            "SELECT id, bucket, config_id, run_key, started_at, state, job_id, manifest_key, \
             error, created_at FROM inventory_runs WHERE bucket = ? AND config_id = ? AND run_key = ?",
        )
        .bind(bucket)
        .bind(config_id)
        .bind(run_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(inventory_run_row))
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
        sqlx::query(
            "UPDATE inventory_runs SET state = ?, manifest_key = ?, error = ?, \
             fence_term = ?, fence_leader = ? WHERE id = ? AND fence_term <= ?",
        )
        .bind(state)
        .bind(manifest_key)
        .bind(error)
        .bind(fence.term_i64())
        .bind(fence.leader_i64())
        .bind(run_id)
        .bind(fence.term_i64())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Inventory 作业的 `progress` = 已扫描对象数（任务收尾时引擎会 +1，调用方存 n-1）。
    pub async fn job_set_progress(&self, job_id: i64, progress: i64, now: i64) -> Result<()> {
        sqlx::query("UPDATE jobs SET progress = ?, updated_at = ? WHERE id = ?")
            .bind(progress)
            .bind(now)
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ——— M4 切片 7：桶策略与审计 ———

    /// 桶 DB 行存在性（控制面写前置校验：防「给不存在桶预置策略，同名桶日后创建即继承」
    /// ——S6-1 同类边角，verifier 非阻断 3）。盘上有、表里无的历史桶由启动回填补齐。
    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM buckets WHERE name = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// 桶策略原文（None = 未配）。
    pub async fn bucket_policy(&self, bucket: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT policy_json FROM bucket_policies WHERE bucket = ?")
            .bind(bucket)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("policy_json")))
    }

    /// 写桶策略（全量替换）。
    pub async fn set_bucket_policy(&self, bucket: &str, json: &str, now: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET policy_json = excluded.policy_json, \
             updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(json)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 清桶策略（删桶时连同清理——桶名可复用，S6-1 同款语义）。
    pub async fn clear_bucket_policy(&self, bucket: &str) -> Result<()> {
        sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// C5：审计保留策略——删除 ts < cutoff 的行，返回删除数（worker 按 OSS_AUDIT_RETENTION_DAYS）。
    /// 每批最多 `limit` 行（id 子查询圈定），大积压时不让单条 DELETE 长时间占写锁。
    pub async fn delete_audit_older_than(&self, cutoff: i64, limit: i64) -> Result<u64> {
        let n = sqlx::query(
            "DELETE FROM audit_log WHERE id IN \
             (SELECT id FROM audit_log WHERE ts < ? ORDER BY ts LIMIT ?)",
        )
        .bind(cutoff)
        .bind(limit.max(1))
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n)
    }

    /// 审计查询（控制台；可选 decision/时间窗/uid/op/bucket/key 前缀过滤；最新在前）。
    /// 动态 WHERE 只拼**固定片段**，用户值一律绑定参数。
    pub async fn list_audit(&self, f: &AuditFilter<'_>) -> Result<Vec<serde_json::Value>> {
        let mut sql = String::from(
            "SELECT id, ts, uid, op, bucket, object_key, decision, detail FROM audit_log \
             WHERE 1=1",
        );
        for (present, clause) in [
            (f.decision.is_some(), " AND decision = ?"),
            (f.from.is_some(), " AND ts >= ?"),
            (f.to.is_some(), " AND ts <= ?"),
            (f.uid.is_some(), " AND uid = ?"),
            (f.op.is_some(), " AND op = ?"),
            (f.bucket.is_some(), " AND bucket = ?"),
        ] {
            if present {
                sql.push_str(clause);
            }
        }
        // 前缀按字节字面量匹配：键区分大小写（LIKE 对 ASCII 不区分），无需转义通配符
        if f.key_prefix.is_some() {
            sql.push_str(" AND substr(object_key, 1, length(?)) = ?");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");

        // 绑定顺序必须与上面的 WHERE 片段顺序一致
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
        if let Some(v) = f.decision {
            q = q.bind(v);
        }
        if let Some(v) = f.from {
            q = q.bind(v);
        }
        if let Some(v) = f.to {
            q = q.bind(v);
        }
        if let Some(v) = f.uid {
            q = q.bind(v);
        }
        if let Some(v) = f.op {
            q = q.bind(v);
        }
        if let Some(v) = f.bucket {
            q = q.bind(v);
        }
        if let Some(v) = f.key_prefix {
            q = q.bind(v).bind(v);
        }
        let rows = q.bind(f.limit).fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.get::<i64, _>("id"),
                    "ts": r.get::<i64, _>("ts"),
                    "uid": r.get::<i64, _>("uid"),
                    "op": r.get::<String, _>("op"),
                    "bucket": r.get::<Option<String>, _>("bucket"),
                    "object_key": r.get::<Option<String>, _>("object_key"),
                    "decision": r.get::<String, _>("decision"),
                    "detail": r.get::<Option<String>, _>("detail"),
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

/// 建桶结果：同名同模式=收敛更新（M5 重建语义），模式不同=409。
pub(crate) enum CreateBucketOutcome {
    Created,
    Updated,
    ModeConflict,
}

impl Db {
    pub(crate) async fn create_user_raw(
        cw: &mut ControlWrite<'_>,
        username: &str,
        password_hash: &str,
        status: &str,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, status, created_at) \
             VALUES (?, ?, 0, ?, ?)",
        )
        .bind(username)
        .bind(password_hash)
        .bind(status)
        .bind(created_at)
        .execute(&mut *cw.conn())
        .await?;
        Ok(())
    }

    /// 与 [`Db::delete_user`] 完全同 SQL：守卫写取写锁 + 最后管理员判定 + 全量级联。
    pub(crate) async fn delete_user_cascade(cw: &mut ControlWrite<'_>, uid: i64) -> Result<bool> {
        let conn = cw.conn();
        let gate = sqlx::query(
            "UPDATE users SET id = id WHERE id = ? AND \
             (NOT (is_admin = 1 AND status = 'active') \
              OR (SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active') > 1)",
        )
        .bind(uid)
        .execute(&mut *conn)
        .await?
        .rows_affected();
        if gate == 0 {
            return Ok(false);
        }
        for sql in [
            "DELETE FROM access_keys WHERE user_id = ?",
            "DELETE FROM permissions WHERE user_id = ?",
            "UPDATE buckets SET owner_id = NULL WHERE owner_id = ?",
            "UPDATE uploads SET owner_id = NULL WHERE owner_id = ?",
            "DELETE FROM users WHERE id = ?",
        ] {
            sqlx::query(sql).bind(uid).execute(&mut *conn).await?;
        }
        Ok(true)
    }

    pub(crate) async fn set_password_raw(
        cw: &mut ControlWrite<'_>,
        uid: i64,
        password_hash: &str,
    ) -> Result<bool> {
        let n = sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
            .bind(password_hash)
            .bind(uid)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n == 1)
    }

    /// 与 [`Db::set_user_status`] 同语义（停用走原子最后管理员守卫）。
    pub(crate) async fn set_user_status_raw(
        cw: &mut ControlWrite<'_>,
        uid: i64,
        active: bool,
    ) -> Result<bool> {
        let n = if active {
            sqlx::query("UPDATE users SET status = 'active' WHERE id = ?")
                .bind(uid)
                .execute(&mut *cw.conn())
                .await?
                .rows_affected()
        } else {
            sqlx::query(
                "UPDATE users SET status = 'disabled' WHERE id = ? AND \
                 (NOT (is_admin = 1 AND status = 'active') \
                  OR (SELECT count(*) FROM users WHERE is_admin = 1 AND status = 'active') > 1)",
            )
            .bind(uid)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected()
        };
        Ok(n == 1)
    }

    pub(crate) async fn set_permissions_raw(
        cw: &mut ControlWrite<'_>,
        uid: i64,
        rows: &[PermissionRow],
    ) -> Result<()> {
        let conn = cw.conn();
        sqlx::query("DELETE FROM permissions WHERE user_id = ?")
            .bind(uid)
            .execute(&mut *conn)
            .await?;
        for p in rows {
            sqlx::query(
                "INSERT INTO permissions (user_id, bucket, can_read, can_write, can_delete) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(uid)
            .bind(&p.bucket)
            .bind(p.read as i64)
            .bind(p.write as i64)
            .bind(p.delete as i64)
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_access_key_raw(
        cw: &mut ControlWrite<'_>,
        access_key: &str,
        secret_key: &str,
        user_id: i64,
        kind: &str,
        parent_ak: Option<&str>,
        session_policy: Option<&str>,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, kind, \
             parent_ak, session_policy, created_at) VALUES (?, ?, ?, 'active', ?, ?, ?, ?)",
        )
        .bind(access_key)
        .bind(secret_key)
        .bind(user_id)
        .bind(kind)
        .bind(parent_ak)
        .bind(session_policy)
        .bind(created_at)
        .execute(&mut *cw.conn())
        .await?;
        Ok(())
    }

    pub(crate) async fn delete_access_key_raw(
        cw: &mut ControlWrite<'_>,
        access_key: &str,
    ) -> Result<bool> {
        let n = sqlx::query("DELETE FROM access_keys WHERE access_key = ?")
            .bind(access_key)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn set_access_key_status_raw(
        cw: &mut ControlWrite<'_>,
        access_key: &str,
        status: &str,
    ) -> Result<bool> {
        let n = sqlx::query("UPDATE access_keys SET status = ? WHERE access_key = ?")
            .bind(status)
            .bind(access_key)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    /// 全列建桶（S6c 的 `db`/`access`/`console` 共用 writer）：
    /// 行不存在 → INSERT（set_id=0，deleting=0）；同名同模式 → 全列收敛但**不写 set_id**；
    /// 同名不同模式 → `ModeConflict`（无写入）。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_bucket_tx(
        cw: &mut ControlWrite<'_>,
        name: &str,
        owner_id: Option<i64>,
        created_at: i64,
        replication_mode: &str,
        versioning: &str,
        object_lock: bool,
        lock_mode: Option<&str>,
        lock_days: Option<i64>,
        default_encryption: Option<&str>,
    ) -> Result<CreateBucketOutcome> {
        let conn = cw.conn();
        let existing: Option<String> =
            sqlx::query_scalar("SELECT replication_mode FROM buckets WHERE name = ?")
                .bind(name)
                .fetch_optional(&mut *conn)
                .await?;
        if let Some(mode) = existing {
            if mode != replication_mode {
                return Ok(CreateBucketOutcome::ModeConflict);
            }
            sqlx::query(
                "UPDATE buckets SET owner_id = ?, versioning = ?, object_lock = ?, \
                 object_lock_default_mode = ?, object_lock_default_days = ?, \
                 default_encryption = ?, replication_mode = ?, deleting = 0 WHERE name = ?",
            )
            .bind(owner_id)
            .bind(versioning)
            .bind(i64::from(object_lock))
            .bind(lock_mode)
            .bind(lock_days)
            .bind(default_encryption)
            .bind(replication_mode)
            .bind(name)
            .execute(&mut *conn)
            .await?;
            return Ok(CreateBucketOutcome::Updated);
        }
        sqlx::query(
            "INSERT INTO buckets \
             (name, owner_id, creation_date, versioning, set_id, object_lock, \
              object_lock_default_mode, object_lock_default_days, default_encryption, \
              replication_mode, deleting) \
             VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 0)",
        )
        .bind(name)
        .bind(owner_id)
        .bind(created_at)
        .bind(versioning)
        .bind(i64::from(object_lock))
        .bind(lock_mode)
        .bind(lock_days)
        .bind(default_encryption)
        .bind(replication_mode)
        .execute(&mut *conn)
        .await?;
        Ok(CreateBucketOutcome::Created)
    }

    /// 删桶级联：与 `clear_bucket_m8_tx` + console 清理的并集一致（11 表内的从属行）。
    /// 返回桶行是否存在（不存在 → NotFound，但仍清残留从属行，幂等）。
    pub(crate) async fn delete_bucket_cascade(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
    ) -> Result<bool> {
        let conn = cw.conn();
        sqlx::query(
            "DELETE FROM olap_configs WHERE access_point IN \
             (SELECT name FROM access_points WHERE bucket = ?)",
        )
        .bind(bucket)
        .execute(&mut *conn)
        .await?;
        for sql in [
            "DELETE FROM access_points WHERE bucket = ?",
            "DELETE FROM inventory_configs WHERE bucket = ?",
            "DELETE FROM inventory_cursors WHERE bucket = ?",
            "DELETE FROM inventory_incremental WHERE bucket = ?",
            "DELETE FROM permissions WHERE bucket = ?",
            "DELETE FROM lifecycle_rules WHERE bucket = ?",
            "DELETE FROM lifecycle_cursors WHERE bucket = ?",
            "DELETE FROM bucket_policies WHERE bucket = ?",
            "DELETE FROM event_configs WHERE bucket = ?",
            "DELETE FROM replication_rules WHERE bucket = ?",
        ] {
            sqlx::query(sql).bind(bucket).execute(&mut *conn).await?;
        }
        let n = sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(bucket)
            .execute(&mut *conn)
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn set_bucket_deleting_raw(
        cw: &mut ControlWrite<'_>,
        name: &str,
        deleting: bool,
    ) -> Result<bool> {
        let n = sqlx::query("UPDATE buckets SET deleting = ? WHERE name = ?")
            .bind(i64::from(deleting))
            .bind(name)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn set_bucket_versioning_raw(
        cw: &mut ControlWrite<'_>,
        name: &str,
        versioning: &str,
    ) -> Result<bool> {
        let n = sqlx::query("UPDATE buckets SET versioning = ? WHERE name = ?")
            .bind(versioning)
            .bind(name)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn set_bucket_object_lock_raw(
        cw: &mut ControlWrite<'_>,
        name: &str,
        enabled: bool,
        default_mode: Option<&str>,
        default_days: Option<i64>,
    ) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE buckets SET object_lock = ?, object_lock_default_mode = ?, \
             object_lock_default_days = ? WHERE name = ?",
        )
        .bind(i64::from(enabled))
        .bind(default_mode)
        .bind(default_days)
        .bind(name)
        .execute(&mut *cw.conn())
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn set_bucket_notification_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
        config_json: &str,
        updated_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO event_configs (bucket, config_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET config_json = excluded.config_json, \
             updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(config_json)
        .bind(updated_at)
        .execute(&mut *cw.conn())
        .await?;
        Ok(())
    }

    pub(crate) async fn clear_bucket_notification_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
    ) -> Result<()> {
        sqlx::query("DELETE FROM event_configs WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *cw.conn())
            .await?;
        Ok(())
    }

    pub(crate) async fn set_lifecycle_rules_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
        rules: &[LifecycleRuleRow],
        updated_at: i64,
    ) -> Result<()> {
        let conn = cw.conn();
        sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *conn)
            .await?;
        // 4.10：规则整桶替换 → 旧扫描游标作废（prefix/动作变了，续扫位置不再可信）。
        sqlx::query("DELETE FROM lifecycle_cursors WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *conn)
            .await?;
        for r in rules {
            sqlx::query(
                "INSERT INTO lifecycle_rules (bucket, rule_id, prefix, expire_days, \
                 expiration_days, transition_days, transition_class, transition_tier, \
                 enabled, updated_at, config_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(bucket)
            .bind(&r.rule_id)
            .bind(&r.prefix)
            .bind(r.expire_days)
            .bind(r.expiration_days)
            .bind(r.transition_days)
            .bind(&r.transition_class)
            .bind(&r.transition_tier)
            .bind(i64::from(r.enabled))
            .bind(updated_at)
            .bind(&r.config_json)
            .execute(&mut *conn)
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn clear_lifecycle_rules_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
    ) -> Result<()> {
        sqlx::query("DELETE FROM lifecycle_rules WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *cw.conn())
            .await?;
        sqlx::query("DELETE FROM lifecycle_cursors WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *cw.conn())
            .await?;
        Ok(())
    }

    pub(crate) async fn set_replication_rules_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
        rules_json: &str,
        updated_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO replication_rules (bucket, rules_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET rules_json = excluded.rules_json, \
             updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(rules_json)
        .bind(updated_at)
        .execute(&mut *cw.conn())
        .await?;
        Ok(())
    }

    pub(crate) async fn clear_replication_rules_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
    ) -> Result<()> {
        sqlx::query("DELETE FROM replication_rules WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *cw.conn())
            .await?;
        Ok(())
    }

    pub(crate) async fn set_bucket_default_encryption_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
        algorithm: &str,
    ) -> Result<bool> {
        let n = sqlx::query("UPDATE buckets SET default_encryption = ? WHERE name = ?")
            .bind(algorithm)
            .bind(bucket)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn clear_bucket_default_encryption_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
    ) -> Result<bool> {
        let n = sqlx::query("UPDATE buckets SET default_encryption = NULL WHERE name = ?")
            .bind(bucket)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn set_bucket_policy_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
        policy_json: &str,
        updated_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO bucket_policies (bucket, policy_json, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(bucket) DO UPDATE SET policy_json = excluded.policy_json, \
             updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(policy_json)
        .bind(updated_at)
        .execute(&mut *cw.conn())
        .await?;
        Ok(())
    }

    pub(crate) async fn delete_bucket_policy_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
    ) -> Result<()> {
        sqlx::query("DELETE FROM bucket_policies WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *cw.conn())
            .await?;
        Ok(())
    }

    /// `false` = 同名 AP 已存在（调用方映射 Conflict，不覆盖策略）。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_access_point_raw(
        cw: &mut ControlWrite<'_>,
        name: &str,
        bucket: &str,
        owner_id: i64,
        policy_json: Option<&str>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<bool> {
        let conn = cw.conn();
        let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM access_points WHERE name = ?")
            .bind(name)
            .fetch_optional(&mut *conn)
            .await?;
        if exists.is_some() {
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO access_points (name, bucket, owner_id, policy_json, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(name)
        .bind(bucket)
        .bind(owner_id)
        .bind(policy_json)
        .bind(created_at)
        .bind(updated_at)
        .execute(&mut *conn)
        .await?;
        Ok(true)
    }

    pub(crate) async fn set_access_point_policy_raw(
        cw: &mut ControlWrite<'_>,
        name: &str,
        policy_json: Option<&str>,
        updated_at: i64,
    ) -> Result<bool> {
        let n =
            sqlx::query("UPDATE access_points SET policy_json = ?, updated_at = ? WHERE name = ?")
                .bind(policy_json)
                .bind(updated_at)
                .bind(name)
                .execute(&mut *cw.conn())
                .await?
                .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn delete_access_point_cascade(
        cw: &mut ControlWrite<'_>,
        name: &str,
    ) -> Result<bool> {
        let conn = cw.conn();
        sqlx::query("DELETE FROM olap_configs WHERE access_point = ?")
            .bind(name)
            .execute(&mut *conn)
            .await?;
        let n = sqlx::query("DELETE FROM access_points WHERE name = ?")
            .bind(name)
            .execute(&mut *conn)
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn olap_upsert_raw(
        cw: &mut ControlWrite<'_>,
        access_point: &str,
        kind: &str,
        endpoint: Option<&str>,
        module: Option<&[u8]>,
        module_sha256: Option<&str>,
        timeout_ms: i64,
        max_response_bytes: i64,
        updated_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO olap_configs (access_point, kind, endpoint, module, module_sha256, \
             timeout_ms, max_response_bytes, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(access_point) DO UPDATE SET kind = excluded.kind, \
             endpoint = excluded.endpoint, module = excluded.module, \
             module_sha256 = excluded.module_sha256, timeout_ms = excluded.timeout_ms, \
             max_response_bytes = excluded.max_response_bytes, updated_at = excluded.updated_at",
        )
        .bind(access_point)
        .bind(kind)
        .bind(endpoint)
        .bind(module)
        .bind(module_sha256)
        .bind(timeout_ms)
        .bind(max_response_bytes)
        .bind(updated_at)
        .execute(&mut *cw.conn())
        .await?;
        Ok(())
    }

    pub(crate) async fn olap_delete_raw(
        cw: &mut ControlWrite<'_>,
        access_point: &str,
    ) -> Result<bool> {
        let n = sqlx::query("DELETE FROM olap_configs WHERE access_point = ?")
            .bind(access_point)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    pub(crate) async fn inventory_upsert_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
        id: &str,
        owner_id: i64,
        config_json: &str,
        updated_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO inventory_configs (bucket, id, owner_id, config_json, updated_at) \
             VALUES (?, ?, ?, ?, ?) ON CONFLICT(bucket, id) DO UPDATE SET \
             owner_id = excluded.owner_id, config_json = excluded.config_json, \
             updated_at = excluded.updated_at",
        )
        .bind(bucket)
        .bind(id)
        .bind(owner_id)
        .bind(config_json)
        .bind(updated_at)
        .execute(&mut *cw.conn())
        .await?;
        sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&mut *cw.conn())
            .await?;
        sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&mut *cw.conn())
            .await?;
        Ok(())
    }

    pub(crate) async fn inventory_delete_raw(
        cw: &mut ControlWrite<'_>,
        bucket: &str,
        id: &str,
    ) -> Result<bool> {
        sqlx::query("DELETE FROM inventory_cursors WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&mut *cw.conn())
            .await?;
        sqlx::query("DELETE FROM inventory_incremental WHERE bucket = ? AND config_id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&mut *cw.conn())
            .await?;
        let n = sqlx::query("DELETE FROM inventory_configs WHERE bucket = ? AND id = ?")
            .bind(bucket)
            .bind(id)
            .execute(&mut *cw.conn())
            .await?
            .rows_affected();
        Ok(n > 0)
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
}
