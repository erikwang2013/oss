//! SigV4 凭据解析：s3s 负责验签，我们只负责「AK 换 SK」。
//! M1 只有认证没有授权 —— 任何有效 AK 可访问所有桶；授权在 M4。

use sqlx::SqlitePool;

use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Error, S3ErrorCode, S3Result};

#[derive(Clone)]
pub struct SigV4Auth {
    pool: SqlitePool,
}

impl SigV4Auth {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl S3Auth for SigV4Auth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let row = sqlx::query(
            "SELECT secret_key FROM access_keys \
             WHERE access_key = ? AND status = 'active' AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(access_key)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        row.map(|r| SecretKey::from(sqlx::Row::get::<String, _>(&r, "secret_key")))
            .ok_or_else(|| {
                S3Error::with_message(S3ErrorCode::InvalidAccessKeyId, "unknown access key")
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 最小控制面 schema 的内联建库：`xl-s3` 不能反向依赖 `oss-server`，
    /// 列与 `oss-server/migrations/0001_init.sql` 对齐（只取本任务用到的列）。
    async fn oss_test_db() -> sqlx::SqlitePool {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);\
             CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, user_id INTEGER NOT NULL, \
              status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL);",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[tokio::test]
    async fn known_access_key_resolves() {
        let pool = oss_test_db().await;
        sqlx::query("INSERT INTO users (id, username) VALUES (1, 'u')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, created_at) \
             VALUES ('AKIA_TEST', 'SK_TEST', 1, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let auth = SigV4Auth::new(pool);
        assert_eq!(
            auth.get_secret_key("AKIA_TEST").await.unwrap().expose(),
            "SK_TEST"
        );
    }

    #[tokio::test]
    async fn unknown_access_key_is_rejected() {
        let auth = SigV4Auth::new(oss_test_db().await);
        assert!(auth.get_secret_key("NOPE").await.is_err());
    }

    #[tokio::test]
    async fn inactive_or_expired_access_key_is_rejected() {
        let pool = oss_test_db().await;
        sqlx::query("INSERT INTO users (id, username) VALUES (1, 'u')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, created_at) \
             VALUES ('AKIA_OFF', 'SK_OFF', 1, 'inactive', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, expires_at, created_at) \
             VALUES ('AKIA_OLD', 'SK_OLD', 1, 1, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let auth = SigV4Auth::new(pool);
        assert!(auth.get_secret_key("AKIA_OFF").await.is_err());
        assert!(auth.get_secret_key("AKIA_OLD").await.is_err());
    }
}
