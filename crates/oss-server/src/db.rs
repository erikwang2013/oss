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

fn now() -> i64 {
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
        Ok(Self { pool })
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
        // ponytail: 非常量时间比较 + 用户存在性时序（未命中不比对）M1 已接受；M4 换 Argon2 时随方案处置。
        let row = sqlx::query("SELECT id, password_hash FROM users WHERE username = ?")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| {
            let hash: String = r.get("password_hash");
            (crate::db::hash_password(password) == hash).then(|| r.get::<i64, _>("id"))
        }))
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
        let rows = sqlx::query(
            "SELECT access_key FROM access_keys WHERE user_id = ? ORDER BY created_at, access_key",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.get("access_key")).collect())
    }

    pub async fn insert_bucket(&self, name: &str, owner_id: Option<i64>) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO buckets (name, owner_id, creation_date) VALUES (?, ?, ?)",
        )
        .bind(name)
        .bind(owner_id)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_bucket_row(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(name)
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

    /// 返回是否命中：0 行命中 = saved_path 不存在（调用方按 404 处理），不静默吞掉 original_name（规格 §5.4）。
    pub async fn register_upload(
        &self,
        saved_path: &str,
        original_name: &str,
        owner_id: Option<i64>,
    ) -> Result<bool> {
        let n =
            sqlx::query("UPDATE uploads SET original_name = ?, owner_id = ? WHERE saved_path = ?")
                .bind(original_name)
                .bind(owner_id)
                .bind(saved_path)
                .execute(&self.pool)
                .await?
                .rows_affected();
        Ok(n == 1)
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

    /// 取用户（`/me`、admin 判定用）：(username, is_admin)。
    pub async fn get_user(&self, uid: i64) -> Result<Option<(String, bool)>> {
        let row = sqlx::query("SELECT username, is_admin FROM users WHERE id = ?")
            .bind(uid)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| {
            (
                r.get::<String, _>("username"),
                r.get::<i64, _>("is_admin") != 0,
            )
        }))
    }

    /// 全部用户 + 各自权限行（子账户管理页；数量级 = 人手，两步查询足够）。
    pub async fn list_users(&self) -> Result<Vec<UserRow>> {
        let users = sqlx::query("SELECT id, username, is_admin, created_at FROM users ORDER BY id")
            .fetch_all(&self.pool)
            .await?;
        let mut out = Vec::with_capacity(users.len());
        for r in users {
            let id: i64 = r.get("id");
            out.push(UserRow {
                id,
                username: r.get("username"),
                is_admin: r.get::<i64, _>("is_admin") != 0,
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
    pub async fn delete_user(&self, uid: i64) -> Result<()> {
        let mut tx = self.pool.begin().await?;
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
        Ok(())
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

    /// 删桶时顺手清该桶的授权行（防「重建同名桶继承旧授权」）。
    pub async fn delete_bucket_permissions(&self, bucket: &str) -> Result<()> {
        sqlx::query("DELETE FROM permissions WHERE bucket = ?")
            .bind(bucket)
            .execute(&self.pool)
            .await?;
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

/// M1 的口令哈希：SHA-256 加固定前缀盐。
/// ponytail: 不是 Argon2/bcrypt；M4 引入真实口令策略时一并换掉。
pub fn hash_password(password: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"oss-m1-salt:");
    h.update(password.as_bytes());
    hex::encode(h.finalize())
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
            Some(("sub".to_string(), false))
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

        // 删桶清授权
        db.delete_bucket_permissions("z").await.unwrap();
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
        db.delete_user(sub).await.unwrap();
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
        db.record_upload("g_202610_abc.txt", "files", "abc.txt", "file", 7, "etag1")
            .await
            .unwrap();
        assert!(
            db.register_upload("g_202610_abc.txt", "my.txt", None)
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
        assert!(
            !db.register_upload("nope.txt", "my.txt", None)
                .await
                .unwrap()
        );
    }
}
