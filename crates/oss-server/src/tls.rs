//! 进程内 TLS 物料：加载 `[tls.server]`/`[tls.client]`，提供 ecat/tonic/reqwest/
//! hyper/rskafka 各传输层的客户端与服务端配置。
//!
//! 物料支持热加载（SIGHUP / 文件内容轮询）：成功构建的新配置整体替换旧快照，
//! 任何读取/解析/构建错误都保留旧物料（fail closed）。主监听器每个新连接取一次
//! 快照（见 [`crate::tls_server`]），出站客户端在下次取用时拿到新配置；长连接
//! 组件按 [`Tls::generation`]（Kafka）或 [`Tls::tonic_client_watch`]（mesh gRPC）
//! 重建客户端，旧连接上的在途请求继续完成。
//!
//! rustls 0.23 同时编译 ring 与 aws-lc-rs 时 `builder()` 会 panic；进程启动时
//! 通过 [`install_provider`] 安装 ring，并在所有自建配置里显式 `builder_with_provider`。

use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use sha2::{Digest, Sha256};

use crate::tls_config::TlsToml;

/// 热加载最小间隔：SIGHUP 洪水/轮询撞车时合并，1s 内的重复触发直接跳过。
const RELOAD_MIN_INTERVAL: Duration = Duration::from_secs(1);

/// 进程启动即调用：固定 ring provider（幂等；测试也从 [`Tls::load`] 调用）。
pub fn install_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

struct ServerPaths {
    cert: PathBuf,
    key: PathBuf,
    client_ca: Option<PathBuf>,
}

#[derive(Default)]
struct ClientPaths {
    ca: Option<PathBuf>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
}

struct ServerMaterial {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    client_ca_pem: Option<Vec<u8>>,
    config: Arc<ServerConfig>,
    /// 叶证书 SHA-256（日志只记指纹，绝不记私钥）。
    fingerprint: String,
}

#[derive(Default)]
struct ClientMaterial {
    ca_pem: Option<Vec<u8>>,
    cert_pem: Option<Vec<u8>>,
    key_pem: Option<Vec<u8>>,
}

struct Loaded {
    server: Option<ServerMaterial>,
    client: ClientMaterial,
    /// 出站配置缓存：热加载整体替换；构建失败时不会替换旧快照。
    client_config: OnceLock<Result<Arc<ClientConfig>, String>>,
    reqwest: OnceLock<Result<reqwest::Client, String>>,
    /// 全部物料文件内容的 SHA-256（轮询去重，与 mtime 无关，兼容原子替换）。
    material_hash: [u8; 32],
}

/// 加载后的 TLS 物料快照。没有 `[tls]` 也构造一个：出站 https 用平台根，peer 明文不变。
pub struct Tls {
    server: Option<ServerPaths>,
    client: ClientPaths,
    loaded: RwLock<Arc<Loaded>>,
    warned_peer_upgrade: AtomicBool,
    last_reload: Mutex<Option<Instant>>,
    /// 成功热加载的代次（只增；内容未变/加载失败不递增）。长连接客户端（Kafka）
    /// 据此丢弃旧连接，下次取用时按新物料重建。
    generation: AtomicU64,
    /// mesh gRPC 通道热加载信号：值 = 当前 tonic 客户端配置（`None` = 未启用 peer TLS）。
    /// 只有通过校验的新物料才发送；reload 失败不发送，订阅者继续用旧通道（fail closed）。
    tonic_client_tx: watch::Sender<Option<tonic::transport::ClientTlsConfig>>,
    /// `tonic_client`/`tonic_client_watch` 被使用过：reload 必须先成功构建客户端配置
    /// （CA/身份解析校验）才能替换快照——tonic 配置本身不解析 PEM，不能只靠它兜底。
    tonic_in_use: AtomicBool,
}

impl Tls {
    pub fn load(cfg: &TlsToml) -> Result<Arc<Self>, String> {
        install_provider();
        cfg.validate()?;
        let server = cfg.server.as_ref().map(|s| ServerPaths {
            cert: s.cert.clone(),
            key: s.key.clone(),
            client_ca: s.client_ca.clone(),
        });
        let client = match &cfg.client {
            None => ClientPaths::default(),
            Some(c) => ClientPaths {
                ca: c.ca_cert.clone(),
                cert: c.client_cert.clone(),
                key: c.client_key.clone(),
            },
        };
        let loaded = read_loaded(server.as_ref(), &client)?;
        let initial_tonic = loaded.server.as_ref().map(|_| tonic_client_from(&loaded));
        let (tonic_client_tx, _) = watch::channel(initial_tonic);
        Ok(Arc::new(Self {
            server,
            client,
            loaded: RwLock::new(Arc::new(loaded)),
            warned_peer_upgrade: AtomicBool::new(false),
            last_reload: Mutex::new(None),
            generation: AtomicU64::new(0),
            tonic_client_tx,
            tonic_in_use: AtomicBool::new(false),
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

    /// 是否配置了任何 TLS 物料（服务端或出站）；决定是否需要挂热加载任务。
    pub fn has_material(&self) -> bool {
        self.server.is_some() || self.client.ca.is_some() || self.client.cert.is_some()
    }

    fn snapshot(&self) -> Arc<Loaded> {
        self.loaded
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
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

    /// ecat 主监听器 TLS 配置（路径形式）；witness/测试用。
    /// serve 主监听器走 [`crate::tls_server::ReloadableHttpServer`] + [`Self::server_config`]。
    pub fn ecat_server(&self) -> Option<ecat_transport::TlsConfig> {
        let s = self.server.as_ref()?;
        let mut c = ecat_transport::TlsConfig::new(&s.cert, &s.key);
        if let Some(ca) = &s.client_ca {
            c = c.with_client_auth(ca);
        }
        Some(c)
    }

    /// 当前服务端 TLS 配置快照；每个新连接取一次，热加载后新连接立即生效。
    pub fn server_config(&self) -> Result<Arc<ServerConfig>, String> {
        self.snapshot()
            .server
            .as_ref()
            .map(|s| Arc::clone(&s.config))
            .ok_or_else(|| "未配置 [tls.server]，无法构建服务端 TLS 配置".to_string())
    }

    pub fn tonic_server(&self) -> Option<tonic::transport::ServerTlsConfig> {
        let loaded = self.snapshot();
        let s = loaded.server.as_ref()?;
        let identity = tonic::transport::Identity::from_pem(&s.cert_pem, &s.key_pem);
        let mut c = tonic::transport::ServerTlsConfig::new().identity(identity);
        if let Some(ca) = &s.client_ca_pem {
            c = c.client_ca_root(tonic::transport::Certificate::from_pem(ca));
        }
        Some(c)
    }

    /// mesh gRPC 客户端配置：平台根 + 自定义 CA + 可选 mTLS 身份。
    pub fn tonic_client(&self) -> tonic::transport::ClientTlsConfig {
        self.tonic_in_use.store(true, Ordering::Relaxed);
        tonic_client_from(&self.snapshot())
    }

    /// mesh gRPC 通道热加载订阅：初始值为当前配置，之后每次成功 reload 发布新值。
    /// 订阅者（TonicMesh）在下次 RPC 前重建通道；旧通道上的在途请求继续完成。
    pub fn tonic_client_watch(&self) -> watch::Receiver<Option<tonic::transport::ClientTlsConfig>> {
        self.tonic_in_use.store(true, Ordering::Relaxed);
        self.tonic_client_tx.subscribe()
    }

    /// 成功热加载的代次（内容未变/失败不递增）。
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// 出站 hyper（site_client/repl_client/tier）用的 rustls 配置（无 ALPN）。
    pub fn rustls_client_config(&self) -> Result<Arc<ClientConfig>, String> {
        let loaded = self.snapshot();
        loaded
            .client_config
            .get_or_init(|| build_client_config(&loaded.client))
            .clone()
    }

    /// Raft 转发/成员变更用 reqwest 客户端（平台根 + 自定义 CA + 可选身份）。
    pub fn reqwest_client(&self) -> Result<reqwest::Client, String> {
        let loaded = self.snapshot();
        loaded
            .reqwest
            .get_or_init(|| build_reqwest(&loaded.client))
            .clone()
    }

    /// Kafka TLS 复用同一客户端配置（broker 主机名必须出现在证书 SAN 中）。
    pub fn kafka_config(&self) -> Result<Arc<ClientConfig>, String> {
        self.rustls_client_config()
    }

    /// 重新读取全部物料并构建新快照；内容未变返回 `Ok(false)`（不重建）。
    /// 任一文件读取/解析/构建失败返回 `Err`，**旧快照保持不变**。
    pub fn reload(&self) -> Result<bool, String> {
        let fresh = read_loaded(self.server.as_ref(), &self.client)?;
        let prev = self.snapshot();
        if prev.material_hash == fresh.material_hash {
            return Ok(false);
        }
        // 出站配置此前已被使用过时，新物料必须先构建成功才能替换；
        // 否则坏 CA/坏身份会让后续出站静默失败（而旧配置本来可用）。
        let client_in_use = prev.client_config.get().is_some()
            || prev.reqwest.get().is_some()
            || self.tonic_in_use.load(Ordering::Relaxed);
        let fresh = Arc::new(fresh);
        if client_in_use {
            fresh
                .client_config
                .get_or_init(|| build_client_config(&fresh.client))
                .clone()?;
            fresh
                .reqwest
                .get_or_init(|| build_reqwest(&fresh.client))
                .clone()?;
        }
        if let Some(s) = &fresh.server {
            tracing::info!(sha256 = %s.fingerprint, "TLS 服务端证书已热加载");
        }
        let tonic = fresh.server.as_ref().map(|_| tonic_client_from(&fresh));
        *self.loaded.write().unwrap_or_else(|e| e.into_inner()) = fresh;
        self.generation.fetch_add(1, Ordering::Release);
        // 订阅者（mesh gRPC）下次 RPC 前重建通道；发送失败仅表示当前无订阅者。
        let _ = self.tonic_client_tx.send(tonic);
        Ok(true)
    }

    /// 热加载入口（SIGHUP/轮询共用）：1s 内重复触发返回 `None`（合并）。
    pub fn reload_throttled(&self) -> Option<Result<bool, String>> {
        {
            let mut last = self.last_reload.lock().unwrap_or_else(|e| e.into_inner());
            if last.is_some_and(|t| t.elapsed() < RELOAD_MIN_INTERVAL) {
                return None;
            }
            *last = Some(Instant::now());
        }
        Some(self.reload())
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

/// 读取全部物料并构建快照。服务端配置在加载期即构建：证书/私钥不匹配等
/// 配置错误一律启动失败（或热加载失败并保留旧快照），不会拖到首个连接才暴露。
fn read_loaded(server: Option<&ServerPaths>, client: &ClientPaths) -> Result<Loaded, String> {
    let mut hasher = Sha256::new();
    let server_material = match server {
        None => None,
        Some(s) => {
            let cert_pem = read_pem(&s.cert, "[tls.server].cert")?;
            let key_pem = read_private_key(&s.key, "[tls.server].key")?;
            let client_ca_pem = match &s.client_ca {
                None => None,
                Some(p) => Some(read_pem(p, "[tls.server].client_ca")?),
            };
            hash_material(
                &mut hasher,
                &[
                    &cert_pem,
                    &key_pem,
                    client_ca_pem.as_deref().unwrap_or_default(),
                ],
            );
            let config = build_server_config(&cert_pem, &key_pem, client_ca_pem.as_deref())?;
            let fingerprint = cert_fingerprint(&cert_pem, "[tls.server].cert")?;
            Some(ServerMaterial {
                cert_pem,
                key_pem,
                client_ca_pem,
                config,
                fingerprint,
            })
        }
    };
    let ca_pem = match &client.ca {
        None => None,
        Some(p) => Some(read_pem(p, "[tls.client].ca_cert")?),
    };
    let (cert_pem, key_pem) = match (&client.cert, &client.key) {
        (None, None) => (None, None),
        (Some(cp), Some(kp)) => (
            Some(read_pem(cp, "[tls.client].client_cert")?),
            Some(read_private_key(kp, "[tls.client].client_key")?),
        ),
        _ => return Err("[tls.client] client_cert 与 client_key 必须成对配置".into()),
    };
    hash_material(
        &mut hasher,
        &[
            ca_pem.as_deref().unwrap_or_default(),
            cert_pem.as_deref().unwrap_or_default(),
            key_pem.as_deref().unwrap_or_default(),
        ],
    );
    Ok(Loaded {
        server: server_material,
        client: ClientMaterial {
            ca_pem,
            cert_pem,
            key_pem,
        },
        client_config: OnceLock::new(),
        reqwest: OnceLock::new(),
        material_hash: hasher.finalize().into(),
    })
}

/// 内容哈希：长度前缀避免不同字段拼接歧义；与文件 mtime 无关，
/// 原子替换（rename）后内容不变也不会触发重建。
fn hash_material(hasher: &mut Sha256, parts: &[&[u8]]) {
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
}

fn read_pem(path: &std::path::Path, field: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("读取 {field}（{}）失败：{e}", path.display()))
}

/// 读私钥前强制校验文件权限：只接受 0400/0600（组/其他任何位都拒绝）。
/// 私钥可读即等于身份可冒用，权限过宽按配置错误 fail closed，不降级继续。
/// 热加载同样走这里：轮换时把权限改宽会被拒绝并保留旧物料。
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

/// 叶证书 SHA-256 指纹（热加载日志用；不涉及私钥）。
fn cert_fingerprint(pem: &[u8], field: &str) -> Result<String, String> {
    let certs = parse_certs(pem, field)?;
    Ok(hex::encode(Sha256::digest(certs[0].as_ref())))
}

/// 服务端 rustls 配置：mTLS 的 client_ca 支持多证书 PEM bundle（全量入根），
/// ALPN 与 ecat 保持一致（h2 + http/1.1）。
fn build_server_config(
    cert_pem: &[u8],
    key_pem: &[u8],
    client_ca_pem: Option<&[u8]>,
) -> Result<Arc<ServerConfig>, String> {
    let certs = parse_certs(cert_pem, "[tls.server].cert")?;
    let key = parse_key(key_pem, "[tls.server].key")?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("rustls 协议版本配置失败：{e}"))?;
    let builder = match client_ca_pem {
        None => builder.with_no_client_auth(),
        Some(ca) => {
            let mut roots = RootCertStore::empty();
            let (ok, bad) =
                roots.add_parsable_certificates(parse_certs(ca, "[tls.server].client_ca")?);
            if ok == 0 || bad > 0 {
                return Err(format!(
                    "[tls.server].client_ca 含无法解析的证书（有效 {ok}，坏 {bad}）"
                ));
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| format!("[tls.server].client_ca 构造客户端校验器失败：{e}"))?;
            builder.with_client_cert_verifier(verifier)
        }
    };
    let mut config = builder
        .with_single_cert(certs, key)
        .map_err(|e| format!("[tls.server] 证书/私钥不匹配：{e}"))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// mesh gRPC 客户端配置（快照 → tonic 配置；仅供热加载信号与兼容访问器复用）。
fn tonic_client_from(loaded: &Loaded) -> tonic::transport::ClientTlsConfig {
    let mut c = tonic::transport::ClientTlsConfig::new().with_native_roots();
    if let Some(ca) = &loaded.client.ca_pem {
        c = c.ca_certificate(tonic::transport::Certificate::from_pem(ca));
    }
    if let (Some(cert), Some(key)) = (&loaded.client.cert_pem, &loaded.client.key_pem) {
        c = c.identity(tonic::transport::Identity::from_pem(cert, key));
    }
    c
}

/// 出站 rustls 配置：平台根 + `[tls.client].ca_cert` 多证书 bundle + 可选身份。
fn build_client_config(client: &ClientMaterial) -> Result<Arc<ClientConfig>, String> {
    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for e in &native.errors {
        tracing::debug!(error = %e, "加载平台根证书失败（继续）");
    }
    roots.add_parsable_certificates(native.certs);
    if let Some(ca) = &client.ca_pem {
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
    let config = match (&client.cert_pem, &client.key_pem) {
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

fn build_reqwest(client: &ClientMaterial) -> Result<reqwest::Client, String> {
    let mut b = reqwest::Client::builder();
    if let Some(ca) = &client.ca_pem {
        let certs = reqwest::Certificate::from_pem_bundle(ca)
            .map_err(|e| format!("[tls.client].ca_cert 解析失败：{e}"))?;
        for c in certs {
            b = b.add_root_certificate(c);
        }
    }
    if let (Some(cert), Some(key)) = (&client.cert_pem, &client.key_pem) {
        let mut pem = cert.clone();
        pem.extend_from_slice(key);
        b = b.identity(
            reqwest::Identity::from_pem(&pem)
                .map_err(|e| format!("[tls.client] 客户端身份解析失败：{e}"))?,
        );
    }
    b.build().map_err(|e| format!("构建 HTTPS 客户端失败：{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
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
            ..Default::default()
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
                ..Default::default()
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
