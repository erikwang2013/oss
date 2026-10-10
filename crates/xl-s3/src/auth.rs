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

// ponytail: 每请求查一次 AK、无缓存 —— SQLite 读不阻塞、撤销即时生效；量大再加缓存。
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
        .map_err(|e| {
            tracing::error!(error = ?e, access_key, "access key lookup failed");
            S3Error::with_message(S3ErrorCode::InternalError, "internal error")
        })?;

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

    async fn insert_user(pool: &SqlitePool) {
        sqlx::query("INSERT INTO users (id, username) VALUES (1, 'u')")
            .execute(pool)
            .await
            .unwrap();
    }

    async fn insert_key(pool: &SqlitePool, ak: &str, sk: &str, expires_at: Option<i64>) {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, expires_at, created_at) \
             VALUES (?, ?, 1, ?, 0)",
        )
        .bind(ak)
        .bind(sk)
        .bind(expires_at)
        .execute(pool)
        .await
        .unwrap();
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// 负例锁死错误码：认证失败的对外形态只有 InvalidAccessKeyId。
    #[track_caller]
    fn assert_invalid_ak(err: &S3Error) {
        assert!(
            matches!(err.code(), S3ErrorCode::InvalidAccessKeyId),
            "期望 InvalidAccessKeyId，实得 {:?}",
            err.code()
        );
    }

    #[tokio::test]
    async fn known_access_key_resolves() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        insert_key(&pool, "AKIA_TEST", "SK_TEST", None).await;
        let auth = SigV4Auth::new(pool);
        assert_eq!(
            auth.get_secret_key("AKIA_TEST").await.unwrap().expose(),
            "SK_TEST"
        );
    }

    #[tokio::test]
    async fn future_expiry_access_key_resolves() {
        // 钉住秒单位与比较方向（expires_at > now 才通过）；M5 的 STS 会话用同一列
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        insert_key(&pool, "AKIA_TTL", "SK_TTL", Some(now() + 3600)).await;
        let auth = SigV4Auth::new(pool);
        assert_eq!(
            auth.get_secret_key("AKIA_TTL").await.unwrap().expose(),
            "SK_TTL"
        );
    }

    #[tokio::test]
    async fn unknown_access_key_is_rejected() {
        let auth = SigV4Auth::new(oss_test_db().await);
        assert_invalid_ak(&auth.get_secret_key("NOPE").await.unwrap_err());
    }

    #[tokio::test]
    async fn inactive_or_expired_access_key_is_rejected() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, status, created_at) \
             VALUES ('AKIA_OFF', 'SK_OFF', 1, 'inactive', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        insert_key(&pool, "AKIA_OLD", "SK_OLD", Some(1)).await;
        let auth = SigV4Auth::new(pool);
        assert_invalid_ak(&auth.get_secret_key("AKIA_OFF").await.unwrap_err());
        assert_invalid_ak(&auth.get_secret_key("AKIA_OLD").await.unwrap_err());
    }

    #[tokio::test]
    async fn db_error_maps_to_internal_error() {
        let pool = oss_test_db().await;
        sqlx::query("DROP TABLE access_keys")
            .execute(&pool)
            .await
            .unwrap();
        let auth = SigV4Auth::new(pool);
        let err = auth.get_secret_key("AKIA_TEST").await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("internal error"), "5xx 不泄漏内部细节");
    }
}
