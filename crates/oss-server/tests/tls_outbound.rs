//! 出站 HTTPS：site_client/repl_client/tier 共用 rustls 物料（自定义 CA、peer 升级、
//! server_name 覆写、错误主机名拒绝）。

mod common;

use std::sync::Arc;

use axum::routing::get;
use oss_server::config::{TierKind, TierTarget};
use oss_server::repl_client::ReplClient;
use oss_server::tls::Tls;
use oss_server::tls_config::{TlsClientToml, TlsServerToml, TlsToml};

use common::tls::{TestCa, start_http};

fn tls_material(dir: &std::path::Path, cert: &str, key: &str, ca_pem: &str) -> Arc<Tls> {
    let cert = common::tls::write_pem(dir, "server.crt", cert);
    let key = common::tls::write_pem(dir, "server.key", key);
    let ca = common::tls::write_pem(dir, "ca.crt", ca_pem);
    Tls::load(&TlsToml {
        server: Some(TlsServerToml {
            cert,
            key,
            client_ca: None,
        }),
        client: Some(TlsClientToml {
            ca_cert: Some(ca),
            client_cert: None,
            client_key: None,
        }),
        ..Default::default()
    })
    .unwrap()
}

fn mock_router() -> axum::Router {
    axum::Router::new()
        .route(
            "/api/v1/site/identity",
            get(|| async {
                axum::Json(serde_json::json!({
                    "id": "peer-1",
                    "name": "peer",
                    "region": "us-east-1",
                    "priority": 0,
                }))
            }),
        )
        .fallback(|| async { axum::http::StatusCode::NOT_FOUND })
}

#[tokio::test]
async fn site_client_upgrades_http_endpoint_to_https() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let tls = tls_material(dir.path(), &cert, &key, &ca.pem);
    let (_srv, addr, _task) = start_http(mock_router(), tls.ecat_server()).await;

    let ident =
        oss_server::site_client::get_identity_tls(&format!("http://{addr}"), "token", Some(&tls))
            .await
            .unwrap();
    assert_eq!(ident.id, "peer-1");
    assert_eq!(ident.region, "us-east-1");
}

#[tokio::test]
async fn repl_single_site_peer_uses_https() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let tls = tls_material(dir.path(), &cert, &key, &ca.pem);
    let (_srv, addr, _task) = start_http(mock_router(), tls.ecat_server()).await;

    let client = ReplClient::single(&format!("http://{addr}"), "ak", "sk", "us-east-1")
        .unwrap()
        .with_tls(Arc::clone(&tls));
    // mock 的 fallback 返回 404；delete_object_all 把 404 当幂等成功。
    client
        .delete_object_all("site", "b", "k", Some("site-token"))
        .await
        .unwrap();
}

#[tokio::test]
async fn tier_https_with_server_name_override() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    // 证书只签 localhost：用 IP 连接时必须靠 server_name 覆写通过校验。
    let (cert, key) = ca.issue(&["localhost"], false);
    let tls = tls_material(dir.path(), &cert, &key, &ca.pem);
    let (_srv, addr, _task) = start_http(mock_router(), tls.ecat_server()).await;

    let target = |server_name: Option<&str>| TierTarget {
        id: "cold".into(),
        kind: TierKind::S3,
        path: None,
        endpoint: format!("https://{addr}"),
        bucket: "tb".into(),
        access_key: "ak".into(),
        secret_key: "sk".into(),
        region: "us-east-1".into(),
        server_name: server_name.map(str::to_string),
    };
    let good = ReplClient::new_tiers(&[target(Some("localhost"))]).with_tls(Arc::clone(&tls));
    assert_eq!(good.tier_head("cold", "b", "k").await.unwrap(), None);

    let bad = ReplClient::new_tiers(&[target(Some("wrong.invalid"))]).with_tls(tls);
    assert!(
        bad.tier_head("cold", "b", "k").await.is_err(),
        "server_name 与证书不匹配时必须失败"
    );
}
