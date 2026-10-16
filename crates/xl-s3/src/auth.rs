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
    /// M5 切片 6：SK 加密库（None = 未启用=明文直存；T2/T3）。扇出路径携带**封存字节**
    /// （mesh 上不出现明文 SK），消费端（本处）统一解密。
    vault: Option<std::sync::Arc<crate::skvault::SecretVault>>,
}

impl SigV4Auth {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            mesh: None,
            vault: None,
        }
    }

    /// 集群形态（Task 9）：本地查无此 AK → 扇出 peers（节点 B 签的 AK，节点 A 也认）。
    pub fn with_mesh(pool: SqlitePool, mesh: Option<crate::access::ControlMesh>) -> Self {
        Self {
            pool,
            mesh,
            vault: None,
        }
    }

    /// M5 切片 6：接 SK 加密库（builder 风格；router 装配处调用）。
    pub fn with_vault(
        mut self,
        vault: Option<std::sync::Arc<crate::skvault::SecretVault>>,
    ) -> Self {
        self.vault = vault;
        self
    }

    /// SK 归解（T3/T4）：未启用库 → 明文直读（`enc:v1:` 行则 500 fail-closed）；
    /// 启用库 → 封存行解密 / 明文行**透明重写**（仅本地行，`local=true`；best-effort 不阻断）。
    async fn resolve_sk(&self, access_key: &str, raw: String, local: bool) -> S3Result<String> {
        let Some(v) = &self.vault else {
            if xl_store::kms::is_sealed(&raw) {
                tracing::error!(access_key, "SK 为密文但未配置密钥库（env 缺失？）——拒认证");
                return Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "internal error",
                ));
            }
            return Ok(raw);
        };
        match v.open(&raw).await {
            Ok(Some(plain)) => Ok(plain),
            Ok(None) => {
                if local {
                    match v.seal(&raw).await {
                        Ok(sealed) => {
                            // guarded（防并发读互覆）：只把「读到的这一版」换成密文
                            match sqlx::query(
                                "UPDATE access_keys SET secret_key = ? \
                                 WHERE access_key = ? AND secret_key = ?",
                            )
                            .bind(&sealed)
                            .bind(access_key)
                            .bind(&raw)
                            .execute(&self.pool)
                            .await
                            {
                                // guarded no-op（并发已由他者转正）静默；真失败 warn
                                Ok(r) if r.rows_affected() > 0 => {
                                    tracing::info!(access_key, "SK 透明重写为密文（存量迁移）")
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::warn!(error = %e, access_key, "SK 透明重写失败（下轮再试）")
                                }
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, access_key, "SK 加密失败（保持明文）"),
                    }
                }
                Ok(raw)
            }
            Err(e) => {
                tracing::error!(error = %e, access_key, "SK 解密失败（丢钥/篡改/域不符？）——拒认证");
                Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "internal error",
                ))
            }
        }
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
        // M5 切片 5（S2）：会话父链——`parent_ak` 非空的会话行要求父为 long-term 且
        // active/未过期，**且父的属主用户 active**（P3-1 复验补：停用即全链路出局，
        // 父链同样传播——LEFT JOIN users pu 条件式，非会话行恒真）。父删/停 → 子即失效。
        let row = sqlx::query(
            "SELECT ak.secret_key FROM access_keys ak JOIN users u ON u.id = ak.user_id \
             LEFT JOIN access_keys p ON p.access_key = ak.parent_ak \
             LEFT JOIN users pu ON pu.id = p.user_id \
             WHERE ak.access_key = ? AND ak.status = 'active' AND u.status = 'active' \
               AND (ak.expires_at IS NULL OR ak.expires_at > ?) \
               AND (ak.parent_ak IS NULL OR (p.kind = 'long-term' \
                    AND p.status = 'active' AND (p.expires_at IS NULL OR p.expires_at > ?) \
                    AND pu.status = 'active'))",
        )
        .bind(access_key)
        .bind(now)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, access_key, "access key lookup failed");
            S3Error::with_message(S3ErrorCode::InternalError, "internal error")
        })?;

        if let Some(r) = row {
            let raw = sqlx::Row::get::<String, _>(&r, "secret_key");
            return self
                .resolve_sk(access_key, raw, true)
                .await
                .map(SecretKey::from);
        }
        // S3 错误面（spec §0.2）：会话行在册但已过期 → ExpiredToken（仅 kind='session'；
        // 长时键维持 InvalidAccessKeyId——「负例锁死错误码」的既有测试扩面而非破坏）。
        // 父链失败/停用等归 InvalidAccessKeyId（不区分原因，保既有不泄露口径）。
        let expired = sqlx::query(
            "SELECT 1 FROM access_keys ak WHERE ak.access_key = ? AND ak.kind = 'session' \
             AND ak.status = 'active' AND ak.expires_at IS NOT NULL AND ak.expires_at <= ?",
        )
        .bind(access_key)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, access_key, "session expiry probe failed");
            S3Error::with_message(S3ErrorCode::InternalError, "internal error")
        })?;
        if expired.is_some() {
            return Err(S3Error::with_message(
                S3ErrorCode::ExpiredToken,
                "session token has expired",
            ));
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
            // M5 切片 6：对端行可为 enc:v1: 封存（mesh 不带明文；本节点共享同一主密钥）
            return self
                .resolve_sk(access_key, sk, false)
                .await
                .map(SecretKey::from);
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
              status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL, \
              parent_ak TEXT, session_policy TEXT, kind TEXT NOT NULL DEFAULT 'long-term');",
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

    // ---------- M5 切片 6：SK 加密存 ----------

    async fn vault(td: &tempfile::TempDir) -> std::sync::Arc<crate::skvault::SecretVault> {
        use xl_store::crypto::EncState;
        let p = xl_store::kms::FileKeyProvider::open_or_init(&td.path().join("m.key"))
            .await
            .unwrap();
        std::sync::Arc::new(
            crate::skvault::SecretVault::from_enc_state(Some(&EncState {
                file: Some(std::sync::Arc::new(p)),
                kms: None,
            }))
            .unwrap(),
        )
    }

    /// 三态：密文行可解；明文行 + 库 → 可解且**透明重写**为 enc:v1:；明文行无库 → 照旧。
    #[tokio::test]
    async fn sk_sealed_resolves_and_plaintext_migrates() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        let td = tempfile::tempdir().unwrap();
        let v = vault(&td).await;
        // (a) 密文行
        let sealed = v.seal("SK_SEALED_0000001").await.unwrap();
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, created_at) VALUES ('AKIA_ENC', ?, 1, 0)",
        )
        .bind(&sealed)
        .execute(&pool)
        .await
        .unwrap();
        let auth = SigV4Auth::new(pool.clone()).with_vault(Some(v.clone()));
        assert_eq!(
            auth.get_secret_key("AKIA_ENC").await.unwrap().expose(),
            "SK_SEALED_0000001"
        );
        // (b) 明文行 + 库：可解 + 行被重写为密文（透明迁移）
        insert_key(&pool, "AKIA_PLAIN", "SK_PLAIN_0000001", None).await;
        assert_eq!(
            auth.get_secret_key("AKIA_PLAIN").await.unwrap().expose(),
            "SK_PLAIN_0000001"
        );
        let now_stored: String = sqlx::query_scalar(
            "SELECT secret_key FROM access_keys WHERE access_key = 'AKIA_PLAIN'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            xl_store::kms::is_sealed(&now_stored),
            "透明重写未发生：{now_stored}"
        );
        // 重写后再解（走密文分支）
        assert_eq!(
            auth.get_secret_key("AKIA_PLAIN").await.unwrap().expose(),
            "SK_PLAIN_0000001"
        );
        // (c) 无库 + 明文行：照旧可解（零行为变化）；无库 + 密文行（上方已迁移的）必须拒
        insert_key(&pool, "AKIA_PLAIN2", "SK_PLAIN2_0000001", None).await;
        let auth_novault = SigV4Auth::new(pool.clone());
        assert_eq!(
            auth_novault
                .get_secret_key("AKIA_PLAIN2")
                .await
                .unwrap()
                .expose(),
            "SK_PLAIN2_0000001"
        );
        assert!(
            auth_novault.get_secret_key("AKIA_PLAIN").await.is_err(),
            "已迁移为密文的行在无库实例下必须拒"
        );
    }

    /// 密文行 + 无库 → InternalError（fail-closed，不伪装 InvalidAccessKeyId）。
    #[tokio::test]
    async fn sealed_sk_without_vault_fails_closed() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        let td = tempfile::tempdir().unwrap();
        let v = vault(&td).await;
        let sealed = v.seal("SK_X_000000000001").await.unwrap();
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, created_at) VALUES ('AKIA_ENC2', ?, 1, 0)",
        )
        .bind(&sealed)
        .execute(&pool)
        .await
        .unwrap();
        let auth = SigV4Auth::new(pool.clone()); // 无 vault
        let e = auth.get_secret_key("AKIA_ENC2").await.unwrap_err();
        assert_eq!(e.code(), &S3ErrorCode::InternalError, "{e:?}");
        // 丢钥（换个库）→ 解密失败 → InternalError
        let td2 = tempfile::tempdir().unwrap();
        let v2 = vault(&td2).await;
        let auth2 = SigV4Auth::new(pool.clone()).with_vault(Some(v2));
        let e = auth2.get_secret_key("AKIA_ENC2").await.unwrap_err();
        assert_eq!(e.code(), &S3ErrorCode::InternalError, "{e:?}");
    }

    /// M5 切片 6（P3-2 承诺测试）：扇出路径携带**封存字节**（mesh 无明文 SK）——
    /// 本节点用共享同一库解密；无库实例对封存应答必须拒（不伪装 InvalidAccessKeyId）。
    #[tokio::test]
    async fn peer_fanout_carries_sealed_sk() {
        let td = tempfile::tempdir().unwrap();
        let v = vault(&td).await;
        let sealed = v.seal("SK_PEER_SEALED_01").await.unwrap();
        let row = || xl_store::mesh::ControlRow {
            uid: Some(7),
            is_admin: Some(false),
            active: Some(true),
            secret: Some(sealed.clone()),
            ..Default::default()
        };
        // 有库：解出明文 SK
        let auth = SigV4Auth::with_mesh(oss_test_db().await, Some(mesh_with(Some(row()))))
            .with_vault(Some(v));
        assert_eq!(
            auth.get_secret_key("AKIA_PEER_ENC").await.unwrap().expose(),
            "SK_PEER_SEALED_01"
        );
        // 无库：封存应答拒（InternalError，fail-closed）
        let auth2 = SigV4Auth::with_mesh(oss_test_db().await, Some(mesh_with(Some(row()))));
        let e = auth2.get_secret_key("AKIA_PEER_ENC").await.unwrap_err();
        assert_eq!(e.code(), &S3ErrorCode::InternalError, "{e:?}");
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
                set: None,
                session_policy: None,
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
    // ---------- M5 切片 5：会话父链与 ExpiredToken ----------

    async fn insert_session(pool: &SqlitePool, ak: &str, parent: &str, expires_at: i64) {
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, expires_at, created_at, parent_ak, kind) \
             VALUES (?, 'SK_SESS', 1, ?, 0, ?, 'session')",
        )
        .bind(ak)
        .bind(expires_at)
        .bind(parent)
        .execute(pool)
        .await
        .unwrap();
    }

    /// 会话键：父 active 可解；父停用/过期/删除/父本身是会话 → 一律拒。
    #[tokio::test]
    async fn session_key_requires_active_longterm_parent() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        insert_key(&pool, "AKIA_P1", "SK_P1", None).await;
        insert_key(&pool, "AKIA_P2", "SK_P2", Some(now() + 3600)).await;
        insert_session(&pool, "ASIA_OK", "AKIA_P1", now() + 900).await;
        let auth = SigV4Auth::new(pool.clone());
        assert!(auth.get_secret_key("ASIA_OK").await.is_ok());

        // 父停用 → 子拒
        sqlx::query("UPDATE access_keys SET status = 'inactive' WHERE access_key = 'AKIA_P1'")
            .execute(&pool)
            .await
            .unwrap();
        assert_invalid_ak(&auth.get_secret_key("ASIA_OK").await.unwrap_err());
        sqlx::query("UPDATE access_keys SET status = 'active' WHERE access_key = 'AKIA_P1'")
            .execute(&pool)
            .await
            .unwrap();

        // 父过期 → 子拒
        insert_session(&pool, "ASIA_EXP_P", "AKIA_P2", now() + 900).await;
        sqlx::query("UPDATE access_keys SET expires_at = 1 WHERE access_key = 'AKIA_P2'")
            .execute(&pool)
            .await
            .unwrap();
        assert_invalid_ak(&auth.get_secret_key("ASIA_EXP_P").await.unwrap_err());

        // 父删除（行没了）→ 子拒
        sqlx::query("DELETE FROM access_keys WHERE access_key = 'AKIA_P1'")
            .execute(&pool)
            .await
            .unwrap();
        assert_invalid_ak(&auth.get_secret_key("ASIA_OK").await.unwrap_err());

        // 父本身是会话（kind='session'）→ 子拒（父链只一层，S2）
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, expires_at, created_at, parent_ak, kind) \
             VALUES ('ASIA_PARENT', 'SK', 1, ?, 0, NULL, 'session')",
        )
        .bind(now() + 3600)
        .execute(&pool)
        .await
        .unwrap();
        insert_session(&pool, "ASIA_CHILD", "ASIA_PARENT", now() + 900).await;
        assert_invalid_ak(&auth.get_secret_key("ASIA_CHILD").await.unwrap_err());
    }

    /// P3-1 复验修复判别：会话属主 ≠ 父属主时，**父属主用户**停用同样传播（全链路出局）。
    #[tokio::test]
    async fn parent_user_disable_propagates_to_session() {
        let pool = oss_test_db().await;
        insert_user(&pool).await; // user 1（父属主）
        sqlx::query("INSERT INTO users (id, username) VALUES (2, 'role')")
            .execute(&pool)
            .await
            .unwrap();
        insert_key(&pool, "AKIA_ADMIN", "SK_A", None).await; // user 1 的键
        sqlx::query(
            "INSERT INTO access_keys (access_key, secret_key, user_id, expires_at, created_at, parent_ak, kind) \
             VALUES ('ASIA_R', 'SK_R', 2, ?, 0, 'AKIA_ADMIN', 'session')",
        )
        .bind(now() + 900)
        .execute(&pool)
        .await
        .unwrap();
        let auth = SigV4Auth::new(pool.clone());
        // 会话属主（user 2）active、父键 active、父属主（user 1）active → 可解
        assert!(auth.get_secret_key("ASIA_R").await.is_ok());
        // 停用**父属主**（user 1；会话属主 user 2 仍 active）→ 子必须失效（P3-1）
        sqlx::query("UPDATE users SET status = 'disabled' WHERE id = 1")
            .execute(&pool)
            .await
            .unwrap();
        assert_invalid_ak(&auth.get_secret_key("ASIA_R").await.unwrap_err());
    }

    /// 过期错误面：会话行过期 → ExpiredToken；长时键过期维持 InvalidAccessKeyId（扩面不破坏）。
    #[tokio::test]
    async fn expired_session_reports_expired_token() {
        let pool = oss_test_db().await;
        insert_user(&pool).await;
        insert_key(&pool, "AKIA_P", "SK_P", None).await;
        insert_session(&pool, "ASIA_OLD", "AKIA_P", 1).await;
        let auth = SigV4Auth::new(pool.clone());
        let e = auth.get_secret_key("ASIA_OLD").await.unwrap_err();
        assert_eq!(e.code(), &S3ErrorCode::ExpiredToken, "{e:?}");
        // 长时键过期：既有口径（InvalidAccessKeyId）
        insert_key(&pool, "AKIA_OLD", "SK_O", Some(1)).await;
        assert_invalid_ak(&auth.get_secret_key("AKIA_OLD").await.unwrap_err());
    }
}
