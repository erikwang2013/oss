#![allow(dead_code)]
//! 测试 PKI：rcgen 生成临时 CA / 服务端 / 客户端证书（不落仓库、不共享密钥）。

use std::net::SocketAddr;
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use tokio::task::JoinHandle;

pub struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    pub pem: String,
}

impl TestCa {
    pub fn new() -> Self {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "oss-test-ca");
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().unwrap();
        let pem = params.self_signed(&key).unwrap().pem();
        Self {
            issuer: Issuer::new(params, key),
            pem,
        }
    }

    /// 签发终端证书；`client = true` 时带 clientAuth EKU，否则 serverAuth。
    pub fn issue(&self, sans: &[&str], client: bool) -> (String, String) {
        let mut params =
            CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap();
        params.distinguished_name.push(
            DnType::CommonName,
            if client {
                "oss-test-client"
            } else {
                "oss-test-server"
            },
        );
        params.extended_key_usages = vec![if client {
            ExtendedKeyUsagePurpose::ClientAuth
        } else {
            ExtendedKeyUsagePurpose::ServerAuth
        }];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

pub fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

pub async fn wait_tcp(port: u16) {
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("测试服务 {port} 未就绪");
}

pub type ServerTask = JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

/// 起 ecat HTTP 服务（可选 TLS），返回句柄、地址与后台任务。
pub async fn start_http(
    router: axum::Router,
    tls: Option<ecat_transport::TlsConfig>,
) -> (
    Arc<ecat::transport_http::HttpServer>,
    SocketAddr,
    ServerTask,
) {
    use ecat_transport::Server as _;
    let port = free_port();
    let mut srv = ecat::transport_http::HttpServer::new(format!("127.0.0.1:{port}")).router(router);
    if let Some(t) = tls {
        srv = srv.tls(t);
    }
    let srv = Arc::new(srv);
    let task_srv = Arc::clone(&srv);
    let task = tokio::spawn(async move { task_srv.start().await });
    wait_tcp(port).await;
    (srv, SocketAddr::from(([127, 0, 0, 1], port)), task)
}

/// 把 PEM 写入临时目录并返回路径（测试结束后由 TempDir drop 清理）。
pub fn write_pem(dir: &std::path::Path, name: &str, pem: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, pem).unwrap();
    path
}
