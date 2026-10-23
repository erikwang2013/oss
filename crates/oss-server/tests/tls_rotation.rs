//! CA 轮换的双信任窗口：新旧 CA 并存时新旧客户端都可接入；只信任旧 CA 的客户端
//! 对已切换的新叶证书必须失败（证明必须先在所有 bundle 里加新 CA，再换叶）。

mod common;

use std::sync::Arc;

use axum::routing::get;
use oss_server::tls::Tls;
use oss_server::tls_config::{TlsClientToml, TlsServerToml, TlsToml};

use common::tls::{TestCa, start_http};

fn router() -> axum::Router {
    axum::Router::new().route("/health", get(|| async { "ok" }))
}

/// 证书 bundle：多个 PEM 直接拼接（配置格式即多证书 PEM 串联）。
fn bundle(pems: &[&str]) -> String {
    pems.concat()
}

fn roots_builder(ca_pem: &str) -> reqwest::ClientBuilder {
    let mut b = reqwest::Client::builder().no_proxy();
    for cert in reqwest::Certificate::from_pem_bundle(ca_pem.as_bytes()).unwrap() {
        b = b.add_root_certificate(cert);
    }
    b
}

fn client_with_roots(ca_pem: &str) -> reqwest::Client {
    roots_builder(ca_pem).build().unwrap()
}

fn client_with_identity(ca_pem: &str, cert_pem: &str, key_pem: &str) -> reqwest::Client {
    let mut identity = cert_pem.to_string();
    identity.push_str(key_pem);
    roots_builder(ca_pem)
        .identity(reqwest::Identity::from_pem(identity.as_bytes()).unwrap())
        .build()
        .unwrap()
}

/// 服务端叶证书由 `leaf_ca` 签发；`client_ca` 为可选的 mTLS 客户端信任 bundle。
fn server_tls(dir: &std::path::Path, leaf: (&str, &str), client_ca: Option<&str>) -> Arc<Tls> {
    let cert = common::tls::write_pem(dir, "server.crt", leaf.0);
    let key = common::tls::write_pem(dir, "server.key", leaf.1);
    let client_ca = client_ca.map(|pem| common::tls::write_pem(dir, "client-ca.crt", pem));
    Tls::load(&TlsToml {
        server: Some(TlsServerToml {
            cert,
            key,
            client_ca,
        }),
        client: None,
    })
    .unwrap()
}

#[tokio::test]
async fn client_trust_bundle_survives_server_leaf_switch() {
    let old_ca = TestCa::new();
    let new_ca = TestCa::new();
    let dir = tempfile::tempdir().unwrap();
    // 叶证书已切到新 CA（轮换第 2 步）。
    let (cert, key) = new_ca.issue(&["127.0.0.1"], false);
    let tls = server_tls(dir.path(), (&cert, &key), None);
    let (_srv, addr, _task) = start_http(router(), tls.ecat_server()).await;
    let url = format!("https://{addr}/health");

    // 只信任旧 CA：新叶必须失败（旧客户端不能独闯新叶）。
    let old_only = client_with_roots(&old_ca.pem);
    assert!(
        old_only.get(&url).send().await.is_err(),
        "只信任旧 CA 的客户端不得接受新叶证书"
    );

    // 旧+新 CA bundle：信任窗口内成功。
    let window = client_with_roots(&bundle(&[&old_ca.pem, &new_ca.pem]));
    assert_eq!(window.get(&url).send().await.unwrap().status(), 200);
    // 只信任新 CA：成功。
    let new_only = client_with_roots(&new_ca.pem);
    assert_eq!(new_only.get(&url).send().await.unwrap().status(), 200);
}

#[tokio::test]
async fn mtls_client_ca_bundle_accepts_old_and_new_client_certs() {
    let old_ca = TestCa::new();
    let new_ca = TestCa::new();
    let (server_cert, server_key) = old_ca.issue(&["127.0.0.1"], false);
    let (old_client, old_key) = old_ca.issue(&["old-client"], true);
    let (new_client, new_key) = new_ca.issue(&["new-client"], true);

    // 窗口期：服务端 client_ca = 旧+新 bundle，两种客户端证书都接受。
    let dir = tempfile::tempdir().unwrap();
    let tls = server_tls(
        dir.path(),
        (&server_cert, &server_key),
        Some(&bundle(&[&old_ca.pem, &new_ca.pem])),
    );
    let (_srv, addr, _task) = start_http(router(), tls.ecat_server()).await;
    let url = format!("https://{addr}/health");
    for (name, cert, key) in [
        ("旧 CA 客户端", &old_client, &old_key),
        ("新 CA 客户端", &new_client, &new_key),
    ] {
        assert_eq!(
            client_with_identity(&old_ca.pem, cert, key)
                .get(&url)
                .send()
                .await
                .unwrap()
                .status(),
            200,
            "{name} 在信任窗口内必须被接受"
        );
    }
    // 无客户端证书仍拒绝。
    assert!(
        client_with_roots(&old_ca.pem)
            .get(&url)
            .send()
            .await
            .is_err()
    );

    // 移除旧 CA 后：新客户端仍可用，旧客户端被拒绝（移除必须等旧客户端全部切换）。
    let dir2 = tempfile::tempdir().unwrap();
    let tls2 = server_tls(dir2.path(), (&server_cert, &server_key), Some(&new_ca.pem));
    let (_srv2, addr2, _task2) = start_http(router(), tls2.ecat_server()).await;
    let url2 = format!("https://{addr2}/health");
    assert_eq!(
        client_with_identity(&old_ca.pem, &new_client, &new_key)
            .get(&url2)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(
        client_with_identity(&old_ca.pem, &old_client, &old_key)
            .get(&url2)
            .send()
            .await
            .is_err(),
        "client_ca 移除旧 CA 后，旧客户端证书必须被拒绝"
    );
}

#[tokio::test]
async fn outbound_ca_bundle_trusts_both_old_and_new_ca() {
    let old_ca = TestCa::new();
    let new_ca = TestCa::new();
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = new_ca.issue(&["127.0.0.1"], false);
    let tls = server_tls(dir.path(), (&cert, &key), None);
    let (_srv, addr, _task) = start_http(router(), tls.ecat_server()).await;
    let url = format!("https://{addr}/health");

    let outbound = |ca_pem: &str| {
        let ca = common::tls::write_pem(dir.path(), "out-ca.crt", ca_pem);
        Tls::load(&TlsToml {
            server: None,
            client: Some(TlsClientToml {
                ca_cert: Some(ca),
                client_cert: None,
                client_key: None,
            }),
        })
        .unwrap()
    };

    // 出站 ca_cert 多证书 bundle：旧+新 CA 都进入信任根，新叶成功。
    let window = outbound(&bundle(&[&old_ca.pem, &new_ca.pem]));
    assert_eq!(
        window
            .reqwest_client()
            .unwrap()
            .get(&url)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    // 只配旧 CA：失败（bundle 解析不能只取第一张或全量忽略）。
    let old_only = outbound(&old_ca.pem);
    assert!(
        old_only
            .reqwest_client()
            .unwrap()
            .get(&url)
            .send()
            .await
            .is_err()
    );
}
