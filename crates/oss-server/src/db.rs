//! 控制面数据访问。M1 只有五张表；对象数据永不进 SQL。

use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Row, SqlitePool};

pub type Result<T> = std::result::Result<T, sqlx::Error>;

pub struct Db {
    pool: SqlitePool,
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

// migrations/ 只是目录命名习惯：本实现用 include_str! + raw_sql 原样执行整份 schema，
// 不建 _sqlx_migrations —— 将来加 0002 不会自动执行（M2 起再引 sqlx migrate）。
const SCHEMA: &str = include_str!("../migrations/0001_init.sql");

pub(crate) fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        // WAL 走连接选项、不走 URL：sqlx 0.8 的 URL 解析只认 mode/cache/immutable/vfs，
        // `?journal_mode=WAL` 会直接报 unknown query parameter。选项形式对池内每条新连接生效。
        let opts = url
            .parse::<SqliteConnectOptions>()?
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePool::connect_with(opts).await?;
        // sqlx::raw_sql 支持一个字符串里的多条语句，schema 文件原样执行
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        let db = Self { pool };
        db.ensure_columns().await?;
        Ok(db)
    }

    /// 极小增量迁移：0001 是**整份重放**（全部 IF NOT EXISTS），而 `ALTER ADD COLUMN`
    /// 不幂等，放不进重放——增量列在这里探测后补齐。新库由 0001 直接带列，两个路径收敛。
    /// 字面量 SQL（sqlx 0.9 拒动态串）；增量列：users.status · buckets.set_id ·
    /// buckets.object_lock 三列（M5 切片 4）· access_keys 会话三列（M5 切片 5）。
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
        let row = sqlx::query(
            "SELECT id, password_hash FROM users WHERE username = ? AND status = 'active'",
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
        if crate::db::is_legacy_hash(&stored) {
            // 透明升级（失败只 warn：本次登录已成功，下次再来）
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
        sqlx::query(
            "INSERT OR IGNORE INTO buckets (name, owner_id, creation_date, set_id) VALUES (?, ?, ?, ?)",
        )
        .bind(name)
        .bind(owner_id)
        .bind(now())
        .bind(set as i64)
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
        sqlx::query(
            "INSERT INTO buckets (name, owner_id, creation_date, set_id) VALUES (?, ?, ?, ?) \
             ON CONFLICT(name) DO UPDATE SET owner_id = excluded.owner_id, set_id = excluded.set_id",
        )
        .bind(name)
        .bind(owner_id)
        .bind(now())
        .bind(set as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
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
}

// ——— M4 切片 6：生命周期规则与作业（jobs 表进度） ———

/// 生命周期规则行。
#[derive(Debug, Clone, PartialEq)]
pub struct LifecycleRuleRow {
    pub bucket: String,
    pub prefix: String,
    pub expire_days: i64,
    pub enabled: bool,
}

impl Db {
    /// 某桶的全部规则（控制台 GET 用）。
    pub async fn list_lifecycle_rules(&self, bucket: &str) -> Result<Vec<LifecycleRuleRow>> {
        let rows = sqlx::query(
            "SELECT bucket, prefix, expire_days, enabled FROM lifecycle_rules \
             WHERE bucket = ? ORDER BY prefix",
        )
        .bind(bucket)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| LifecycleRuleRow {
                bucket: r.get("bucket"),
                prefix: r.get("prefix"),
                expire_days: r.get("expire_days"),
                enabled: r.get::<i64, _>("enabled") != 0,
            })
            .collect())
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
        for r in rules {
            sqlx::query(
                "INSERT INTO lifecycle_rules (bucket, prefix, expire_days, enabled, updated_at) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(bucket)
            .bind(&r.prefix)
            .bind(r.expire_days)
            .bind(if r.enabled { 1 } else { 0 })
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// 全部启用规则（worker 扫描用）。
    pub async fn enabled_lifecycle_rules(&self) -> Result<Vec<LifecycleRuleRow>> {
        let rows = sqlx::query(
            "SELECT bucket, prefix, expire_days, enabled FROM lifecycle_rules WHERE enabled = 1",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| LifecycleRuleRow {
                bucket: r.get("bucket"),
                prefix: r.get("prefix"),
                expire_days: r.get("expire_days"),
                enabled: true,
            })
            .collect())
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

    /// 最近作业（控制台 GET /jobs；最新在前）。
    pub async fn list_jobs(&self, limit: i64) -> Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            "SELECT id, kind, bucket, prefix, state, scanned, deleted, skipped, error, \
             created_at, updated_at FROM jobs ORDER BY id DESC LIMIT ?",
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
                    "created_at": r.get::<i64, _>("created_at"),
                    "updated_at": r.get::<i64, _>("updated_at"),
                })
            })
            .collect())
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
    pub async fn delete_audit_older_than(&self, cutoff: i64) -> Result<u64> {
        let n = sqlx::query("DELETE FROM audit_log WHERE ts < ?")
            .bind(cutoff)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n)
    }

    /// 审计查询（控制台；可选 decision 过滤；最新在前）。
    pub async fn list_audit(
        &self,
        limit: i64,
        decision: Option<&str>,
    ) -> Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            "SELECT id, ts, uid, op, bucket, object_key, decision, detail FROM audit_log \
             WHERE (?1 IS NULL OR decision = ?1) ORDER BY id DESC LIMIT ?2",
        )
        .bind(decision)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
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
        let n = db.delete_audit_older_than(now - 100).await.unwrap();
        assert_eq!(n, 1, "只删窗口外旧行");
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(left, 1);
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
}
