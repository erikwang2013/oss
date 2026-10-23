//! ecat 监听器 TLS：可信 CA 成功、明文被拒、错 CA/错主机名失败、mTLS 强制客户端证书。

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use oss_server::tls::Tls;
use oss_server::tls_config::{TlsClientToml, TlsServerToml, TlsToml};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use common::tls::{TestCa, start_http};

fn router() -> axum::Router {
    axum::Router::new().route("/health", get(|| async { "ok" }))
}

fn server_tls(
    root: &std::path::Path,
    cert: &str,
    key: &str,
    client_ca: Option<&std::path::Path>,
) -> Arc<Tls> {
    let cert = common::tls::write_pem(root, "server.crt", cert);
    let key = common::tls::write_pem(root, "server.key", key);
    let cfg = TlsToml {
        server: Some(TlsServerToml {
            cert,
            key,
            client_ca: client_ca.map(|p| p.to_path_buf()),
        }),
        client: None,
        ..Default::default()
    };
    Tls::load(&cfg).unwrap()
}

fn client_builder(ca_pem: &str) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap())
}

#[tokio::test]
async fn trusted_ca_succeeds_and_plaintext_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let tls = server_tls(dir.path(), &cert, &key, None);
    let (_srv, addr, _task) = start_http(router(), tls.ecat_server()).await;

    let http = client_builder(&ca.pem).build().unwrap();
    let resp = http
        .get(format!("https://{addr}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");

    // 明文 HTTP 打到 TLS 端口：握手失败，不得返回 HTTP 响应。
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut buf)).await;
    assert!(
        !String::from_utf8_lossy(&buf).contains("200 OK"),
        "TLS 监听器不得接受明文请求"
    );
}

#[tokio::test]
async fn wrong_ca_and_hostname_mismatch_fail() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let other_ca = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let tls = server_tls(dir.path(), &cert, &key, None);
    let (_srv, addr, _task) = start_http(router(), tls.ecat_server()).await;

    // 错 CA：链路/证书校验必须失败（无 verify=false 旁路）。
    let wrong = client_builder(&other_ca.pem).build().unwrap();
    assert!(
        wrong
            .get(format!("https://{addr}/health"))
            .send()
            .await
            .is_err()
    );

    // 正确 CA + 证书只签 localhost：用 IP 连接必须因主机名不匹配失败。
    let dir2 = tempfile::tempdir().unwrap();
    let (cert2, key2) = ca.issue(&["localhost"], false);
    let tls2 = server_tls(dir2.path(), &cert2, &key2, None);
    let (_srv2, addr2, _task2) = start_http(router(), tls2.ecat_server()).await;
    let http = client_builder(&ca.pem).build().unwrap();
    assert!(
        http.get(format!("https://{addr2}/health"))
            .send()
            .await
            .is_err(),
        "证书主机名与连接地址不匹配时必须失败"
    );
}

#[tokio::test]
async fn mtls_rejects_missing_client_cert() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let (client_cert, client_key) = ca.issue(&["oss-test-client"], true);
    let ca_path = common::tls::write_pem(dir.path(), "client-ca.crt", &ca.pem);
    let tls = server_tls(dir.path(), &cert, &key, Some(&ca_path));
    let (_srv, addr, _task) = start_http(router(), tls.ecat_server()).await;

    // 无客户端证书：握手拒绝。
    let anonymous = client_builder(&ca.pem).build().unwrap();
    assert!(
        anonymous
            .get(format!("https://{addr}/health"))
            .send()
            .await
            .is_err(),
        "mTLS 必须拒绝无客户端证书的连接"
    );

    // 带 CA 签发的客户端证书：成功。
    let mut identity_pem = client_cert.into_bytes();
    identity_pem.extend_from_slice(client_key.as_bytes());
    let authenticated = client_builder(&ca.pem)
        .identity(reqwest::Identity::from_pem(&identity_pem).unwrap())
        .build()
        .unwrap();
    let resp = authenticated
        .get(format!("https://{addr}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// TlsClientToml 仅用于确保配置类型在同一测试 crate 可见（编译期接线检查）。
#[allow(dead_code)]
fn _client_cfg() -> TlsClientToml {
    TlsClientToml {
        ca_cert: None,
        client_cert: None,
        client_key: None,
    }
}
