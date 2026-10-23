//! TLS 物料热加载：主监听器新连接换新证书、旧连接继续用旧配置；坏物料保留旧配置；
//! 内容哈希去重与 1s 节流；出站 CA 热加载。

mod common;

use std::sync::Arc;

use axum::routing::get;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_util::rt::TokioIo;
use oss_server::tls::Tls;
use oss_server::tls_config::{TlsClientToml, TlsServerToml, TlsToml};
use oss_server::tls_server::ReloadableHttpServer;

use common::tls::{TestCa, free_port, wait_tcp, write_pem};

fn router() -> axum::Router {
    axum::Router::new().route("/health", get(|| async { "ok" }))
}

fn server_toml(cert: &std::path::Path, key: &std::path::Path) -> TlsToml {
    TlsToml {
        server: Some(TlsServerToml {
            cert: cert.to_path_buf(),
            key: key.to_path_buf(),
            client_ca: None,
        }),
        ..Default::default()
    }
}

fn client_toml(ca: &std::path::Path) -> TlsToml {
    TlsToml {
        server: None,
        client: Some(TlsClientToml {
            ca_cert: Some(ca.to_path_buf()),
            client_cert: None,
            client_key: None,
        }),
        ..Default::default()
    }
}

async fn start_reloadable(tls: Arc<Tls>) -> std::net::SocketAddr {
    use ecat_transport::Server as _;
    let port = free_port();
    let server = Arc::new(ReloadableHttpServer::new(
        format!("127.0.0.1:{port}"),
        router(),
        tls,
    ));
    // 任务持有 server 直到测试进程结束（与 common::tls::start_http 相同做法）。
    tokio::spawn(async move {
        let _ = server.start().await;
    });
    wait_tcp(port).await;
    std::net::SocketAddr::from(([127, 0, 0, 1], port))
}

/// 原子替换：同目录临时文件 + rename（与 README runbook 一致）。
fn replace_pem(dir: &std::path::Path, name: &str, pem: &str) {
    let path = dir.join(name);
    let tmp = dir.join(format!("{name}.tmp"));
    std::fs::write(&tmp, pem).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    std::fs::rename(&tmp, path).unwrap();
}

fn reqwest_trusting(ca_pem: &str) -> reqwest::Client {
    let mut b = reqwest::Client::builder().no_proxy();
    for cert in reqwest::Certificate::from_pem_bundle(ca_pem.as_bytes()).unwrap() {
        b = b.add_root_certificate(cert);
    }
    b.build().unwrap()
}

async fn get_on(sender: &mut hyper::client::conn::http1::SendRequest<Full<Bytes>>) -> String {
    let req = Request::builder()
        .uri("/health")
        .header("host", "127.0.0.1")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&body).into_owned()
}

#[tokio::test]
async fn reload_switches_new_connections_and_keeps_old_connection_alive() {
    let dir = tempfile::tempdir().unwrap();
    let ca_old = TestCa::new();
    let ca_new = TestCa::new();
    let (cert_a, key_a) = ca_old.issue(&["127.0.0.1"], false);
    let cert = write_pem(dir.path(), "server.crt", &cert_a);
    let key = write_pem(dir.path(), "server.key", &key_a);
    let tls = Tls::load(&server_toml(&cert, &key)).unwrap();

    let ca_old_path = write_pem(dir.path(), "old-ca.crt", &ca_old.pem);
    let pinned_old = Tls::load(&client_toml(&ca_old_path)).unwrap();
    let addr = start_reloadable(Arc::clone(&tls)).await;
    let url = format!("https://{addr}/health");

    // 旧客户端可用，并建立一条长连接（后续跨热加载复用）。
    assert_eq!(
        reqwest_trusting(&ca_old.pem)
            .get(&url)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let stream = pinned_old
        .connect(true, "127.0.0.1", addr.port(), None)
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    assert_eq!(get_on(&mut sender).await, "ok");

    // 轮换叶证书（新 CA 签发）并热加载。
    let (cert_b, key_b) = ca_new.issue(&["127.0.0.1"], false);
    replace_pem(dir.path(), "server.crt", &cert_b);
    replace_pem(dir.path(), "server.key", &key_b);
    assert!(tls.reload().unwrap(), "内容变化必须触发重建");

    // 新连接：只信任旧 CA 失败，只信任新 CA 成功。
    assert!(
        reqwest_trusting(&ca_old.pem)
            .get(&url)
            .send()
            .await
            .is_err()
    );
    assert_eq!(
        reqwest_trusting(&ca_new.pem)
            .get(&url)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    // 热加载前建立的连接继续用旧配置完成请求（不被新配置中断）。
    assert_eq!(get_on(&mut sender).await, "ok");
}

#[tokio::test]
async fn bad_material_never_replaces_good_config() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let other = TestCa::new();
    let (cert_a, key_a) = ca.issue(&["127.0.0.1"], false);
    let (cert_b, key_b) = other.issue(&["127.0.0.1"], false);
    let cert = write_pem(dir.path(), "server.crt", &cert_a);
    let key = write_pem(dir.path(), "server.key", &key_a);
    let tls = Tls::load(&server_toml(&cert, &key)).unwrap();
    let addr = start_reloadable(Arc::clone(&tls)).await;
    let url = format!("https://{addr}/health");
    let good = reqwest_trusting(&ca.pem);

    async fn assert_old_serves(client: &reqwest::Client, url: &str) {
        assert_eq!(client.get(url).send().await.unwrap().status(), 200);
    }

    // 1) 证书/私钥不匹配。
    replace_pem(dir.path(), "server.crt", &cert_b);
    assert!(tls.reload().is_err(), "叶 B + 私钥 A 必须被拒绝");
    assert_old_serves(&good, &url).await;

    // 2) 私钥权限变宽（0644）。
    replace_pem(dir.path(), "server.crt", &cert_a);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(tls.reload().is_err(), "私钥 0644 必须在热加载时拒绝");
        assert_old_serves(&good, &url).await;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    // 3) 非法 PEM。
    replace_pem(dir.path(), "server.key", "not a private key");
    assert!(tls.reload().is_err(), "非法私钥必须被拒绝");
    assert_old_serves(&good, &url).await;
    replace_pem(dir.path(), "server.key", &key_a);

    // 恢复后可以正常加载新物料。
    replace_pem(dir.path(), "server.crt", &cert_b);
    replace_pem(dir.path(), "server.key", &key_b);
    assert!(tls.reload().unwrap());
    assert_eq!(
        reqwest_trusting(&other.pem)
            .get(&url)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

#[tokio::test]
async fn reload_hash_dedupe_and_one_second_throttle() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert_a, key_a) = ca.issue(&["127.0.0.1"], false);
    let cert = write_pem(dir.path(), "server.crt", &cert_a);
    let key = write_pem(dir.path(), "server.key", &key_a);
    let tls = Tls::load(&server_toml(&cert, &key)).unwrap();

    // 内容未变：不重建。
    assert!(!tls.reload().unwrap());
    // 内容变化：重建一次；随后同内容再次 reload 为 no-op。
    let (cert_b, key_b) = ca.issue(&["127.0.0.1"], false);
    replace_pem(dir.path(), "server.crt", &cert_b);
    replace_pem(dir.path(), "server.key", &key_b);
    assert!(tls.reload().unwrap());
    assert!(!tls.reload().unwrap());

    // 节流：窗口内重复触发合并，窗口过后恢复。
    assert!(tls.reload_throttled().is_some());
    assert!(tls.reload_throttled().is_none());
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(tls.reload_throttled().is_some());
}

#[tokio::test]
async fn outbound_client_config_picks_up_new_ca_after_reload() {
    let dir = tempfile::tempdir().unwrap();
    let ca_old = TestCa::new();
    let ca_new = TestCa::new();
    // 服务端叶证书由新 CA 签发。
    let (cert, key) = ca_new.issue(&["127.0.0.1"], false);
    let server_cert = write_pem(dir.path(), "server.crt", &cert);
    let server_key = write_pem(dir.path(), "server.key", &key);
    let server_tls = Tls::load(&server_toml(&server_cert, &server_key)).unwrap();
    let addr = start_reloadable(server_tls).await;
    let url = format!("https://{addr}/health");

    // 出站 CA 先只有旧 CA：请求与裸 TLS 握手都失败。
    let ca_path = common::tls::write_pem(dir.path(), "client-ca.crt", &ca_old.pem);
    let client = Tls::load(&client_toml(&ca_path)).unwrap();
    assert!(
        client
            .reqwest_client()
            .unwrap()
            .get(&url)
            .send()
            .await
            .is_err()
    );
    assert!(
        client
            .connect(true, "127.0.0.1", addr.port(), None)
            .await
            .is_err()
    );

    // 加入新 CA 后热加载：两个缓存（reqwest / rustls）都必须换成新配置。
    replace_pem(
        dir.path(),
        "client-ca.crt",
        &format!("{}{}", ca_old.pem, ca_new.pem),
    );
    assert!(client.reload().unwrap());
    assert_eq!(
        client
            .reqwest_client()
            .unwrap()
            .get(&url)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(
        client
            .connect(true, "127.0.0.1", addr.port(), None)
            .await
            .is_ok()
    );

    // 坏 CA 热加载失败：保留旧（可用）配置。
    replace_pem(dir.path(), "client-ca.crt", "not a certificate");
    assert!(client.reload().is_err());
    assert_eq!(
        client
            .reqwest_client()
            .unwrap()
            .get(&url)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

/// 代次只在成功换新物料时递增：内容未变、坏物料都不推进。
/// 长连接客户端（Kafka）据此判断是否必须丢弃旧连接并重建。
#[test]
fn generation_advances_only_on_successful_material_swap() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert_a, key_a) = ca.issue(&["127.0.0.1"], false);
    let cert = write_pem(dir.path(), "server.crt", &cert_a);
    let key = write_pem(dir.path(), "server.key", &key_a);
    let tls = Tls::load(&server_toml(&cert, &key)).unwrap();
    assert_eq!(tls.generation(), 0);

    // 内容未变：不重建、不推进。
    assert!(!tls.reload().unwrap());
    assert_eq!(tls.generation(), 0);

    // 有效新物料：推进一次。
    let (cert_b, key_b) = ca.issue(&["127.0.0.1"], false);
    replace_pem(dir.path(), "server.crt", &cert_b);
    replace_pem(dir.path(), "server.key", &key_b);
    assert!(tls.reload().unwrap());
    assert_eq!(tls.generation(), 1);

    // 坏物料：fail closed，代次保持。
    replace_pem(dir.path(), "server.key", "not a private key");
    assert!(tls.reload().is_err());
    assert_eq!(tls.generation(), 1);
}
