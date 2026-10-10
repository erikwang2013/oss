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
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('users','access_keys','buckets','uploads','instant_index')")
            .fetch_one(db.pool()).await.unwrap();
        assert_eq!(n, 5);
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
