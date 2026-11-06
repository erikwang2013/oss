//! SigV4 凭据解析：s3s 负责验签，我们只负责「AK 换 SK」。

use std::sync::Arc;

use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Error, S3ErrorCode, S3Result};

#[derive(Clone)]
pub struct SigV4Auth {
    /// SK 加密库（None = 未启用=明文直存）。
    vault: Option<Arc<crate::skvault::SecretVault>>,
}

impl SigV4Auth {
    pub fn new() -> Self {
        Self { vault: None }
    }

    /// 接 SK 加密库（builder 风格；router 装配处调用）。
    pub fn with_vault(
        mut self,
        vault: Option<std::sync::Arc<crate::skvault::SecretVault>>,
    ) -> Self {
        self.vault = vault;
        self
    }

    /// SK 解封：未启用库 → 明文直读（`enc:v1:` 行则 500 fail-closed）；
    /// 启用库 → 封存行解密 / 明文行原样返回（写回控制面事务，读路径不改状态）。
    async fn resolve_sk(&self, access_key: &str, raw: String) -> S3Result<String> {
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
            Ok(None) => Ok(raw),
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

impl Default for SigV4Auth {
    fn default() -> Self {
        Self::new()
    }
}

// ponytail: 每请求查一次 AK（请求热视图点查），撤销即时生效。
#[async_trait::async_trait]
impl S3Auth for SigV4Auth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        // 对象控制面：每个请求已捕获一份 HotView；缺失 = 不在请求作用域，fail-closed。
        let Some(view) = crate::control::request_view() else {
            tracing::error!(access_key, "control view missing outside request scope");
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "internal error",
            ));
        };
        match view.key_auth(access_key, now).await {
            // 视图返回的是存储态 SK（封存/明文），不在读路径重写。
            Ok(crate::control::KeyLookup::Valid(key)) => self
                .resolve_sk(access_key, key.secret_key)
                .await
                .map(SecretKey::from),
            Ok(crate::control::KeyLookup::ExpiredSession) => Err(S3Error::with_message(
                S3ErrorCode::ExpiredToken,
                "session token has expired",
            )),
            Ok(crate::control::KeyLookup::Invalid) => Err(S3Error::with_message(
                S3ErrorCode::InvalidAccessKeyId,
                "unknown access key",
            )),
            Err(e) => {
                tracing::error!(error = %e, access_key, "control view key lookup failed");
                Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "internal error",
                ))
            }
        }
    }
}
