//! M5 切片 6：SK 加密库（裁决 T1-T6）——`access_keys.secret_key` 的信封加密路由。
//!
//! 密文形态与加解密本体在 `xl-store/kms.rs`（seal_secret/open_secret）；本模块只做
//! **provider 选择与 key_id 路由**：kms 目录优先、file 兜底（T2）；两库皆无 = 未启用。
//! 读面（auth.rs / cluster_control 扇出）经 [`SecretVault::open`]；写面（console）经
//! [`SecretVault::seal`]；明文存量 + 库在 → 读侧透明重写（T4，调用方）。

use std::sync::Arc;

use xl_store::crypto::EncState;
use xl_store::error::Error;
use xl_store::kms::{
    FileKeyProvider, KeyProvider, KmsFileProvider, is_sealed, open_secret, seal_secret,
};

pub struct SecretVault {
    file: Option<Arc<FileKeyProvider>>,
    kms: Option<Arc<KmsFileProvider>>,
}

impl SecretVault {
    /// 自 Store 的密钥库状态构造（无 provider → None = 未启用，零行为变化）。
    pub fn from_enc_state(es: Option<&EncState>) -> Option<Self> {
        let es = es?;
        if es.file.is_none() && es.kms.is_none() {
            return None;
        }
        Some(Self {
            file: es.file.clone(),
            kms: es.kms.clone(),
        })
    }

    /// 封装明文 SK（T2：kms 优先、file 兜底）。
    pub async fn seal(&self, plaintext: &str) -> Result<String, Error> {
        seal_secret(self.current_provider()?, plaintext).await
    }

    /// 解封：未封装行 → `Ok(None)`（明文，调用方原地使用）；封装行 → `Ok(Some(plaintext))`。
    /// key_id 路由：`"file"` → file；其余（`"k<n>"`）→ kms（未知 key_id 由 provider 拒绝）。
    pub async fn open(&self, stored: &str) -> Result<Option<String>, Error> {
        if !is_sealed(stored) {
            return Ok(None);
        }
        let key_id = xl_store::kms::sealed_key_id(stored)
            .ok_or_else(|| Error::Crypto("SK 封装行缺 key_id".into()))?;
        let p: &dyn KeyProvider = if key_id == "file" {
            self.file
                .as_deref()
                .map(|p| p as &dyn KeyProvider)
                .ok_or_else(|| Error::Crypto("SK 为 file 封装但未配置文件主密钥".into()))?
        } else {
            self.kms
                .as_deref()
                .map(|p| p as &dyn KeyProvider)
                .ok_or_else(|| Error::Crypto(format!("SK 为 {key_id} 封装但未配置 KMS 目录")))?
        };
        open_secret(p, stored).await.map(Some)
    }

    /// 当前写的 provider（T2：kms 优先、file 兜底；构造时已保证至少一个）。
    fn current_provider(&self) -> Result<&dyn KeyProvider, Error> {
        if let Some(k) = &self.kms {
            return Ok(k.as_ref());
        }
        if let Some(f) = &self.file {
            return Ok(f.as_ref());
        }
        Err(Error::Crypto("SK 加密库未配置 provider".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xl_store::crypto::EncState;

    async fn vault_file(td: &tempfile::TempDir) -> SecretVault {
        let p = FileKeyProvider::open_or_init(&td.path().join("master.key"))
            .await
            .unwrap();
        SecretVault {
            file: Some(Arc::new(p)),
            kms: None,
        }
    }

    #[tokio::test]
    async fn seal_open_roundtrip_file_provider() {
        let td = tempfile::tempdir().unwrap();
        let v = vault_file(&td).await;
        let sealed = v.seal("SK_TEST_SECRET_KEY_0001").await.unwrap();
        assert!(is_sealed(&sealed), "{sealed}");
        assert!(sealed.starts_with("enc:v1:file:"));
        assert_eq!(
            v.open(&sealed).await.unwrap().as_deref(),
            Some("SK_TEST_SECRET_KEY_0001")
        );
        // 明文行 → None（不动）
        assert_eq!(v.open("plain-sk").await.unwrap(), None);
    }

    #[tokio::test]
    async fn seal_uses_kms_when_present() {
        let td = tempfile::tempdir().unwrap();
        let k = KmsFileProvider::open_or_init(&td.path().join("kms"))
            .await
            .unwrap();
        let f = FileKeyProvider::open_or_init(&td.path().join("master.key"))
            .await
            .unwrap();
        let v = SecretVault {
            file: Some(Arc::new(f)),
            kms: Some(Arc::new(k)),
        };
        let sealed = v.seal("SK_ABC").await.unwrap();
        assert!(sealed.starts_with("enc:v1:k"), "kms 优先：{sealed}");
        assert_eq!(v.open(&sealed).await.unwrap().as_deref(), Some("SK_ABC"));
    }

    /// 路由失败面：file 封装行 + 无 file provider → Err；k* 封装 + 无 kms → Err。
    #[tokio::test]
    async fn open_routes_and_fails_closed() {
        let td = tempfile::tempdir().unwrap();
        let vf = vault_file(&td).await;
        let sealed_file = vf.seal("SK_1").await.unwrap();
        let v_kms_only = SecretVault {
            file: None,
            kms: None,
        };
        let e = v_kms_only.open(&sealed_file).await.unwrap_err();
        assert!(matches!(e, Error::Crypto(_)), "{e:?}");

        let k = KmsFileProvider::open_or_init(&td.path().join("kms2"))
            .await
            .unwrap();
        let vk = SecretVault {
            file: None,
            kms: Some(Arc::new(k)),
        };
        let sealed_k = vk.seal("SK_2").await.unwrap();
        let e = v_kms_only.open(&sealed_k).await.unwrap_err();
        assert!(matches!(e, Error::Crypto(_)), "{e:?}");
    }

    /// from_enc_state：空状态 → None（未启用）；有 provider → Some。
    #[tokio::test]
    async fn from_enc_state_gate() {
        assert!(SecretVault::from_enc_state(None).is_none());
        let td = tempfile::tempdir().unwrap();
        let f = FileKeyProvider::open_or_init(&td.path().join("m.key"))
            .await
            .unwrap();
        assert!(
            SecretVault::from_enc_state(Some(&EncState {
                file: None,
                kms: None
            }))
            .is_none()
        );
        assert!(
            SecretVault::from_enc_state(Some(&EncState {
                file: Some(Arc::new(f)),
                kms: None
            }))
            .is_some()
        );
    }
}
