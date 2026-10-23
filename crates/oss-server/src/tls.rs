//! 进程内 TLS 物料：加载 `[tls.server]`/`[tls.client]`，提供 ecat/tonic/reqwest/
//! hyper/rskafka 各传输层的客户端与服务端配置。
//!
//! rustls 0.23 同时编译 ring 与 aws-lc-rs 时 `builder()` 会 panic；进程启动时
//! 通过 [`install_provider`] 安装 ring，并在所有自建配置里显式 `builder_with_provider`。

use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};

use crate::tls_config::TlsToml;

/// 进程启动即调用：固定 ring provider（幂等；测试也从 [`Tls::load`] 调用）。
pub fn install_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

struct ServerTls {
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
    client_ca_path: Option<std::path::PathBuf>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    client_ca_pem: Option<Vec<u8>>,
}

#[derive(Default)]
struct ClientTls {
    ca_pem: Option<Vec<u8>>,
    cert_pem: Option<Vec<u8>>,
    key_pem: Option<Vec<u8>>,
}

/// 加载后的 TLS 物料。没有 `[tls]` 也构造一个：出站 https 用平台根，peer 明文不变。
pub struct Tls {
    server: Option<ServerTls>,
    client: ClientTls,
    client_config: OnceLock<Result<Arc<ClientConfig>, String>>,
    reqwest: OnceLock<Result<reqwest::Client, String>>,
    warned_peer_upgrade: AtomicBool,
}

impl Tls {
    pub fn load(cfg: &TlsToml) -> Result<Arc<Self>, String> {
        install_provider();
        cfg.validate()?;
        let server = match &cfg.server {
            None => None,
            Some(s) => {
                let cert_pem = read_pem(&s.cert, "[tls.server].cert")?;
                let key_pem = read_private_key(&s.key, "[tls.server].key")?;
                parse_certs(&cert_pem, "[tls.server].cert")?;
                parse_key(&key_pem, "[tls.server].key")?;
                let client_ca_pem = match &s.client_ca {
                    None => None,
                    Some(p) => {
                        let pem = read_pem(p, "[tls.server].client_ca")?;
                        parse_certs(&pem, "[tls.server].client_ca")?;
                        Some(pem)
                    }
                };
                Some(ServerTls {
                    cert_path: s.cert.clone(),
                    key_path: s.key.clone(),
                    client_ca_path: s.client_ca.clone(),
                    cert_pem,
                    key_pem,
                    client_ca_pem,
                })
            }
        };
        let mut client = ClientTls::default();
        if let Some(c) = &cfg.client {
            if let Some(p) = &c.ca_cert {
                let pem = read_pem(p, "[tls.client].ca_cert")?;
                parse_certs(&pem, "[tls.client].ca_cert")?;
                client.ca_pem = Some(pem);
            }
            if let (Some(cp), Some(kp)) = (&c.client_cert, &c.client_key) {
                let cert_pem = read_pem(cp, "[tls.client].client_cert")?;
                let key_pem = read_private_key(kp, "[tls.client].client_key")?;
                parse_certs(&cert_pem, "[tls.client].client_cert")?;
                parse_key(&key_pem, "[tls.client].client_key")?;
                client.cert_pem = Some(cert_pem);
                client.key_pem = Some(key_pem);
            }
        }
        Ok(Arc::new(Self {
            server,
            client,
            client_config: OnceLock::new(),
            reqwest: OnceLock::new(),
            warned_peer_upgrade: AtomicBool::new(false),
        }))
    }

    /// 无 `[tls]` 配置的实例（旧明文路径/测试）。
    pub fn plaintext() -> Arc<Self> {
        Self::load(&TlsToml::default()).expect("空 TLS 配置不会失败")
    }

    /// `[tls.server]` 存在 = 集群/站点/mesh/Raft 全链路 TLS。
    pub fn peer_tls(&self) -> bool {
        self.server.is_some()
    }

    /// 对端地址规范化并套用 peer TLS 模式：明文 `http://h:p` 在 peer TLS 下升级为
    /// `https://h:p`；未启用 peer TLS 时 `https://` 拒绝（不静默降级）。
    pub fn peer_url(&self, endpoint: &str) -> Result<String, String> {
        let (host_port, https) = crate::tls_config::normalize_peer_addr(endpoint, "对端地址")?;
        if https && !self.peer_tls() {
            return Err(format!(
                "对端地址 {endpoint} 为 https，但未配置 [tls.server]（拒绝明文降级）"
            ));
        }
        if self.peer_tls() && !https {
            if !self.warned_peer_upgrade.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    endpoint,
                    "对端地址为明文 http；已按 [tls.server] 升级为 https"
                );
            }
            Ok(format!("https://{host_port}"))
        } else if https {
            Ok(format!("https://{host_port}"))
        } else {
            Ok(format!("http://{host_port}"))
        }
    }

    pub fn peer_scheme(&self) -> &'static str {
        if self.peer_tls() { "https" } else { "http" }
    }

    /// ecat 主监听器 TLS 配置；未启用返回 None（保持旧明文路径）。
    pub fn ecat_server(&self) -> Option<ecat_transport::TlsConfig> {
        let s = self.server.as_ref()?;
        let mut c = ecat_transport::TlsConfig::new(&s.cert_path, &s.key_path);
        if let Some(ca) = &s.client_ca_path {
            c = c.with_client_auth(ca);
        }
        Some(c)
    }

    pub fn tonic_server(&self) -> Option<tonic::transport::ServerTlsConfig> {
        let s = self.server.as_ref()?;
        let identity = tonic::transport::Identity::from_pem(&s.cert_pem, &s.key_pem);
        let mut c = tonic::transport::ServerTlsConfig::new().identity(identity);
        if let Some(ca) = &s.client_ca_pem {
            c = c.client_ca_root(tonic::transport::Certificate::from_pem(ca));
        }
        Some(c)
    }

    /// mesh gRPC 客户端配置：平台根 + 自定义 CA + 可选 mTLS 身份。
    pub fn tonic_client(&self) -> tonic::transport::ClientTlsConfig {
        let mut c = tonic::transport::ClientTlsConfig::new().with_native_roots();
        if let Some(ca) = &self.client.ca_pem {
            c = c.ca_certificate(tonic::transport::Certificate::from_pem(ca));
        }
        if let (Some(cert), Some(key)) = (&self.client.cert_pem, &self.client.key_pem) {
            c = c.identity(tonic::transport::Identity::from_pem(cert, key));
        }
        c
    }

    /// 出站 hyper（site_client/repl_client/tier）用的 rustls 配置（无 ALPN）。
    pub fn rustls_client_config(&self) -> Result<Arc<ClientConfig>, String> {
        self.client_config
            .get_or_init(|| self.build_client_config())
            .clone()
    }

    fn build_client_config(&self) -> Result<Arc<ClientConfig>, String> {
        let mut roots = RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        for e in &native.errors {
            tracing::debug!(error = %e, "加载平台根证书失败（继续）");
        }
        roots.add_parsable_certificates(native.certs);
        if let Some(ca) = &self.client.ca_pem {
            let certs = parse_certs(ca, "[tls.client].ca_cert")?;
            let (ok, bad) = roots.add_parsable_certificates(certs);
            if ok == 0 || bad > 0 {
                return Err(format!(
                    "[tls.client].ca_cert 含无法解析的证书（有效 {ok}，坏 {bad}）"
                ));
            }
        }
        if roots.is_empty() {
            return Err("TLS 客户端无可用信任根（平台根为空且未配置 [tls.client].ca_cert）".into());
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("rustls 协议版本配置失败：{e}"))?
            .with_root_certificates(roots);
        let config = match (&self.client.cert_pem, &self.client.key_pem) {
            (Some(cert), Some(key)) => builder
                .with_client_auth_cert(
                    parse_certs(cert, "[tls.client].client_cert")?,
                    parse_key(key, "[tls.client].client_key")?,
                )
                .map_err(|e| format!("[tls.client] 客户端证书/私钥不匹配：{e}"))?,
            _ => builder.with_no_client_auth(),
        };
        Ok(Arc::new(config))
    }

    /// Raft 转发/成员变更用 reqwest 客户端（平台根 + 自定义 CA + 可选身份）。
    pub fn reqwest_client(&self) -> Result<reqwest::Client, String> {
        self.reqwest.get_or_init(|| self.build_reqwest()).clone()
    }

    fn build_reqwest(&self) -> Result<reqwest::Client, String> {
        let mut b = reqwest::Client::builder();
        if let Some(ca) = &self.client.ca_pem {
            let certs = reqwest::Certificate::from_pem_bundle(ca)
                .map_err(|e| format!("[tls.client].ca_cert 解析失败：{e}"))?;
            for c in certs {
                b = b.add_root_certificate(c);
            }
        }
        if let (Some(cert), Some(key)) = (&self.client.cert_pem, &self.client.key_pem) {
            let mut pem = cert.clone();
            pem.extend_from_slice(key);
            b = b.identity(
                reqwest::Identity::from_pem(&pem)
                    .map_err(|e| format!("[tls.client] 客户端身份解析失败：{e}"))?,
            );
        }
        b.build().map_err(|e| format!("构建 HTTPS 客户端失败：{e}"))
    }

    /// Kafka TLS 复用同一客户端配置（broker 主机名必须出现在证书 SAN 中）。
    pub fn kafka_config(&self) -> Result<Arc<ClientConfig>, String> {
        self.rustls_client_config()
    }

    /// 建立到 `host:port` 的流：明文 TCP 或 TLS（SNI 缺省用 host，可由
    /// `server_name` 覆写）。返回统一 trait object 供 hyper http1 handshake 使用。
    pub async fn connect(
        &self,
        https: bool,
        host: &str,
        port: u16,
        server_name: Option<&str>,
    ) -> Result<Box<dyn Io>, String> {
        let tcp = tokio::net::TcpStream::connect((host, port))
            .await
            .map_err(|e| format!("连接 {host}:{port} 失败：{e}"))?;
        if !https {
            return Ok(Box::new(tcp));
        }
        let config = self.rustls_client_config()?;
        let name = server_name.unwrap_or(host);
        let sni = ServerName::try_from(name.to_string())
            .map_err(|_| format!("TLS server_name 非法：{name:?}"))?;
        let stream = tokio_rustls::TlsConnector::from(config)
            .connect(sni, tcp)
            .await
            .map_err(|e| format!("TLS 握手 {host}:{port} 失败：{e}"))?;
        Ok(Box::new(stream))
    }
}

/// hyper 出站统一流类型：明文 `TcpStream` 与 `TlsStream` 都实现。
pub trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Io for T {}

fn read_pem(path: &std::path::Path, field: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("读取 {field}（{}）失败：{e}", path.display()))
}

/// 读私钥前强制校验文件权限：只接受 0400/0600（组/其他任何位都拒绝）。
/// 私钥可读即等于身份可冒用，权限过宽按配置错误 fail closed，不降级继续。
fn read_private_key(path: &std::path::Path, field: &str) -> Result<Vec<u8>, String> {
    check_key_permissions(path, field)?;
    read_pem(path, field)
}

#[cfg(unix)]
fn check_key_permissions(path: &std::path::Path, field: &str) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path)
        .map_err(|e| format!("读取 {field}（{}）失败：{e}", path.display()))?;
    let mode = meta.permissions().mode() & 0o7777;
    if mode != 0o400 && mode != 0o600 {
        return Err(format!(
            "私钥 {field}（{}）权限不安全：{mode:04o}；必须仅属主可读（0400 或 0600），请执行 chmod 600 {}",
            path.display(),
            path.display()
        ));
    }
    Ok(())
}

/// 非 Unix 平台没有 st_mode 语义（Windows 用 ACL），不做位检查。
#[cfg(not(unix))]
fn check_key_permissions(_path: &std::path::Path, _field: &str) -> Result<(), String> {
    Ok(())
}

fn parse_certs(pem: &[u8], field: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{field} PEM 解析失败：{e}"))?;
    if certs.is_empty() {
        return Err(format!("{field} 中没有证书"));
    }
    Ok(certs)
}

fn parse_key(pem: &[u8], field: &str) -> Result<PrivateKeyDer<'static>, String> {
    rustls_pemfile::private_key(&mut Cursor::new(pem))
        .map_err(|e| format!("{field} 私钥解析失败：{e}"))?
        .ok_or_else(|| format!("{field} 中没有私钥"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn self_signed() -> (String, String) {
        let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// 私钥权限：0400/0600 允许，其余（含组/其他可读）一律拒绝；证书不受此检查。
    #[cfg(unix)]
    #[test]
    fn private_key_permissions_enforced() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, key_pem) = self_signed();
        let cert = dir.path().join("server.crt");
        let key = dir.path().join("server.key");
        std::fs::write(&cert, &cert_pem).unwrap();
        std::fs::write(&key, &key_pem).unwrap();
        // 证书 0644 合法；只有私钥受限。
        std::fs::set_permissions(&cert, std::fs::Permissions::from_mode(0o644)).unwrap();

        let server_cfg = |key: &std::path::Path| TlsToml {
            server: Some(crate::tls_config::TlsServerToml {
                cert: cert.clone(),
                key: key.to_path_buf(),
                client_ca: None,
            }),
            client: None,
        };
        for (mode, ok) in [
            (0o600, true),
            (0o400, true),
            (0o644, false),
            (0o640, false),
            (0o660, false),
            (0o604, false),
            (0o000, false),
        ] {
            std::fs::set_permissions(&key, std::fs::Permissions::from_mode(mode)).unwrap();
            let result = Tls::load(&server_cfg(&key));
            assert_eq!(result.is_ok(), ok, "server key mode {mode:04o}");
            if let Err(e) = result {
                assert!(
                    e.contains(&format!("{mode:04o}")),
                    "错误需列出实际权限：{e}"
                );
            }
        }

        // [tls.client].client_key 同样受限（成对配置才校验）。
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        let client_cfg = |key_mode: u32| {
            std::fs::set_permissions(&key, std::fs::Permissions::from_mode(key_mode)).unwrap();
            TlsToml {
                server: None,
                client: Some(crate::tls_config::TlsClientToml {
                    ca_cert: None,
                    client_cert: Some(cert.clone()),
                    client_key: Some(key.clone()),
                }),
            }
        };
        assert!(Tls::load(&client_cfg(0o600)).is_ok());
        assert!(Tls::load(&client_cfg(0o644)).is_err());
    }

    #[test]
    fn missing_server_files_fail_closed() {
        let cfg: TlsToml = toml::from_str(
            r#"
[server]
cert = "/nonexistent/cert.pem"
key = "/nonexistent/key.pem"
"#,
        )
        .unwrap();
        assert!(Tls::load(&cfg).is_err());
    }

    #[test]
    fn peer_url_plaintext_modes() {
        let plain = Tls::load(&TlsToml::default()).unwrap();
        assert!(!plain.peer_tls());
        assert_eq!(plain.peer_url("http://A:9000/").unwrap(), "http://a:9000");
        assert_eq!(plain.peer_url("a:9000").unwrap(), "http://a:9000");
        assert!(plain.peer_url("https://a:9000").is_err());
        assert_eq!(plain.peer_scheme(), "http");
    }
}
