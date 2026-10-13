//! SigV4 凭据解析：s3s 负责验签，我们只负责「AK 换 SK」。
//! M1 只有认证没有授权 —— 任何有效 AK 可访问所有桶；授权在 M4。

use sqlx::SqlitePool;

use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Error, S3ErrorCode, S3Result};

#[derive(Clone)]
pub struct SigV4Auth {
    pool: SqlitePool,
    /// 集群控制面扇出（Task 9；None = 单节点：本地查无即拒）
    mesh: Option<crate::access::ControlMesh>,
}

impl SigV4Auth {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool, mesh: None }
    }

    /// 集群形态（Task 9）：本地查无此 AK → 扇出 peers（节点 B 签的 AK，节点 A 也认）。
    pub fn with_mesh(pool: SqlitePool, mesh: Option<crate::access::ControlMesh>) -> Self {
        Self { pool, mesh }
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

        // JOIN users：账号停用（users.status）同样让 AK 失效——认证面与授权面同口径，
        // 停用即全链路出局（授权层 Acl::user_of_key 有一份同样的 JOIN，防单点遗漏）。
        let row = sqlx::query(
            "SELECT ak.secret_key FROM access_keys ak JOIN users u ON u.id = ak.user_id \
             WHERE ak.access_key = ? AND ak.status = 'active' AND u.status = 'active' \
               AND (ak.expires_at IS NULL OR ak.expires_at > ?)",
        )
        .bind(access_key)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, access_key, "access key lookup failed");
            S3Error::with_message(S3ErrorCode::InternalError, "internal error")
        })?;

        if let Some(r) = row {
            return Ok(SecretKey::from(sqlx::Row::get::<String, _>(
                &r,
                "secret_key",
            )));
        }
        // 集群（Task 9）：本地查无 → 扇出（对端查询自带 status/expires 同口径条件；
        // 找到即有效）。全 miss/不可达 → 仍然拒绝（fail-closed）。
        if let Some(m) = &self.mesh
            && let Some(row) = m
                .ask(xl_store::mesh::ControlQuery::AccessKey(
                    access_key.to_string(),
                ))
                .await
            && let Some(sk) = row.secret
        {
            return Ok(SecretKey::from(sk));
        }
        Err(S3Error::with_message(
            S3ErrorCode::InvalidAccessKeyId,
            "unknown access key",
        ))
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
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, status TEXT NOT NULL DEFAULT 'active');\
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

    /// 账号停用（users.status）→ AK 在**认证层**即失效（评审补测；e2e_s3.sh 15h 有端到端版）。
    #[tokio::test]
    async fn disabled_user_key_is_rejected_at_auth_layer() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        sqlx::query("UPDATE users SET status = 'disabled' WHERE id = 1")
            .execute(&pool)
            .await
            .unwrap();
        insert_key(&pool, "AKIA_DISABLED", "SK", None).await;
        let auth = SigV4Auth::new(pool);
        assert_invalid_ak(&auth.get_secret_key("AKIA_DISABLED").await.unwrap_err());
    }

    // ---------- Task 9：控制面读扇出 ----------

    fn mesh_with(peer_row: Option<xl_store::mesh::ControlRow>) -> crate::access::ControlMesh {
        let mesh = crate::access::test_support::FakeMesh::new();
        mesh.answer("peer1", peer_row);
        crate::access::ControlMesh {
            transport: mesh,
            self_id: "n1".into(),
            peers: vec!["peer1".into()],
        }
    }

    /// 节点 B 签的 AK：本地查无 → 扇出解得 SK。
    #[tokio::test]
    async fn ak_resolves_via_peer_fanout() {
        let pool = oss_test_db().await; // 本地为空
        let auth = SigV4Auth::with_mesh(
            pool,
            Some(mesh_with(Some(xl_store::mesh::ControlRow {
                uid: Some(7),
                is_admin: Some(false),
                active: Some(true),
                secret: Some("SK_PEER".into()),
                flag: None,
                names: None,
            }))),
        );
        assert_eq!(
            auth.get_secret_key("AK_PEER").await.unwrap().expose(),
            "SK_PEER"
        );
    }

    /// 本地命中的 AK 不依赖扇出；对端不可达时未知 AK 仍拒（fail-closed）。
    #[tokio::test]
    async fn local_hit_ok_and_peer_down_rejects() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        insert_key(&pool, "AKIA_TEST", "SK_TEST", None).await;
        let auth = SigV4Auth::with_mesh(pool, Some(mesh_with(None))); // 对端"查无"（等价不可达后的 None）
        assert_eq!(
            auth.get_secret_key("AKIA_TEST").await.unwrap().expose(),
            "SK_TEST"
        );
        assert_invalid_ak(&auth.get_secret_key("NOPE").await.unwrap_err());
    }
}
