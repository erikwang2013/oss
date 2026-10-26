//! mesh gRPC **入站** TLS 热加载：新连接换新证书 / mTLS 客户端 CA，旧连接继续用旧物料；
//! 坏物料 reload 失败保留旧快照；明文路径行为不变。

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use oss_server::tls::Tls;
use oss_server::tls_config::{TlsServerToml, TlsToml};
use sha2::Digest;
use xl_store::mesh::MeshTransport;
use xl_store::store::{Store, StoreConfig};

use common::tls::{TestCa, free_port, wait_tcp, write_pem};

const SECRET: &str = "mesh-secret";

fn server_toml(cert: &Path, key: &Path, client_ca: Option<&Path>) -> TlsToml {
    TlsToml {
        server: Some(TlsServerToml {
            cert: cert.to_path_buf(),
            key: key.to_path_buf(),
            client_ca: client_ca.map(|p| p.to_path_buf()),
        }),
        ..Default::default()
    }
}

fn client_tls(ca_pem: &str) -> tonic::transport::ClientTlsConfig {
    tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca_pem))
}

fn client_tls_identity(
    ca_pem: &str,
    cert_pem: &str,
    key_pem: &str,
) -> tonic::transport::ClientTlsConfig {
    client_tls(ca_pem).identity(tonic::transport::Identity::from_pem(cert_pem, key_pem))
}

/// 起 mesh gRPC 服务端：`Some(tls)` 走入站热加载路径，`None` 走明文。
async fn start_mesh(tls: Option<Arc<Tls>>) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    // TempDir 必须活到 server 结束；泄漏到进程结束（测试进程短命）。
    std::mem::forget(dir);
    let port = free_port();
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let watch = tls.map(|t| t.mesh_server_watch());
    let task = tokio::spawn(async move {
        let _ = xl_client::serve_tls_watch(store, addr, SECRET.into(), watch).await;
    });
    wait_tcp(port).await;
    (addr, task)
}

fn leaf_fingerprint(pem: &str) -> String {
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(pem.as_bytes()))
        .next()
        .unwrap()
        .unwrap();
    hex::encode(sha2::Sha256::digest(der.as_ref()))
}

/// 裸 rustls 客户端握手后取服务端叶证书指纹（证明新连接实际看到的是哪张证书）。
async fn peer_fingerprint(addr: SocketAddr, ca_pem: &str) -> String {
    let mut roots = rustls::RootCertStore::empty();
    let _ = roots.add_parsable_certificates(
        rustls_pemfile::certs(&mut std::io::Cursor::new(ca_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let sni = rustls::pki_types::ServerName::try_from("127.0.0.1".to_string()).unwrap();
    let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(sni, tcp)
        .await
        .unwrap();
    let peer = stream.get_ref().1.peer_certificates().unwrap();
    hex::encode(sha2::Sha256::digest(peer[0].as_ref()))
}

/// 证书轮换：旧连接（只信任 A）继续服务，新连接只信任 B 成功且看到的正是新叶证书；
/// 坏物料 reload 失败，旧快照继续服务新连接。
#[tokio::test]
async fn inbound_cert_rotation_serves_new_connections_and_keeps_old_alive() {
    let dir = tempfile::tempdir().unwrap();
    let ca_a = TestCa::new();
    let ca_b = TestCa::new();
    let (cert_a, key_a) = ca_a.issue(&["127.0.0.1"], false);
    let (cert_b, key_b) = ca_b.issue(&["127.0.0.1"], false);
    let cert = write_pem(dir.path(), "server.crt", &cert_a);
    let key = write_pem(dir.path(), "server.key", &key_a);
    let tls = Tls::load(&server_toml(&cert, &key, None)).unwrap();
    let (addr, _task) = start_mesh(Some(Arc::clone(&tls))).await;
    let nodes = vec![("n1".to_string(), addr.to_string())];

    // 初始：只信任 A 的客户端可用，并建立一条长连接。
    let old = xl_client::TonicMesh::new_with_tls("n1", SECRET, &nodes, Some(client_tls(&ca_a.pem)))
        .unwrap();
    assert!(old.health("n1").await.is_ok(), "证书 A 必须可连");

    // 轮换叶证书（CA B 签发）并热加载。
    write_pem(dir.path(), "server.crt", &cert_b);
    write_pem(dir.path(), "server.key", &key_b);
    assert!(tls.reload().unwrap(), "内容变化必须触发重建");
    assert_eq!(tls.generation(), 1);

    // 旧连接继续用旧物料：若被迫重连，新叶 B 不可能通过 A 校验。
    assert!(old.health("n1").await.is_ok(), "热加载不得中断已建立连接");

    // 新连接：只信任 B 成功，只信任 A 失败。
    let new = xl_client::TonicMesh::new_with_tls("n1", SECRET, &nodes, Some(client_tls(&ca_b.pem)))
        .unwrap();
    assert!(new.health("n1").await.is_ok(), "新连接必须使用新证书");
    let stale =
        xl_client::TonicMesh::new_with_tls("n1", SECRET, &nodes, Some(client_tls(&ca_a.pem)))
            .unwrap();
    assert!(
        stale.health("n1").await.is_err(),
        "新连接不得继续提供旧证书"
    );

    // 新连接看到的叶证书指纹 = B。
    assert_eq!(
        peer_fingerprint(addr, &ca_b.pem).await,
        leaf_fingerprint(&cert_b)
    );

    // 坏物料（B 证书 + A 私钥）：reload fail closed，代次不推进，旧快照（B）继续服务。
    write_pem(dir.path(), "server.key", &key_a);
    assert!(tls.reload().is_err(), "证书/私钥不匹配必须被拒绝");
    assert_eq!(tls.generation(), 1, "失败的 reload 不得推进代次");
    let still_b =
        xl_client::TonicMesh::new_with_tls("n1", SECRET, &nodes, Some(client_tls(&ca_b.pem)))
            .unwrap();
    assert!(
        still_b.health("n1").await.is_ok(),
        "坏物料不得替换可用的旧配置"
    );
}

/// mTLS 客户端 CA 轮换：新连接按新 client_ca 校验，旧连接保持旧校验器；
/// 新 CA 签发的身份可连，旧 CA 身份被拒（fail closed）。
#[tokio::test]
async fn inbound_mtls_client_ca_rotation_applies_to_new_connections() {
    let dir = tempfile::tempdir().unwrap();
    let ca_a = TestCa::new();
    let ca_b = TestCa::new();
    let (cert, key) = ca_a.issue(&["127.0.0.1"], false);
    let (id_a, id_key_a) = ca_a.issue(&["oss-client-a"], true);
    let (id_b, id_key_b) = ca_b.issue(&["oss-client-b"], true);
    let cert = write_pem(dir.path(), "server.crt", &cert);
    let key = write_pem(dir.path(), "server.key", &key);
    let client_ca = write_pem(dir.path(), "client-ca.crt", &ca_a.pem);
    let tls = Tls::load(&server_toml(&cert, &key, Some(client_ca.as_path()))).unwrap();
    let (addr, _task) = start_mesh(Some(Arc::clone(&tls))).await;
    let nodes = vec![("n1".to_string(), addr.to_string())];

    let old = xl_client::TonicMesh::new_with_tls(
        "n1",
        SECRET,
        &nodes,
        Some(client_tls_identity(&ca_a.pem, &id_a, &id_key_a)),
    )
    .unwrap();
    assert!(old.health("n1").await.is_ok(), "mTLS 客户端 A 必须可连");

    // 客户端 CA 轮换到 B。
    write_pem(dir.path(), "client-ca.crt", &ca_b.pem);
    assert!(tls.reload().unwrap(), "客户端 CA 变化必须触发重建");

    // 旧连接（客户端证书 A）在旧校验器下继续服务。
    assert!(
        old.health("n1").await.is_ok(),
        "客户端 CA 轮换不得中断已建立连接"
    );

    // 新连接：B 身份可连，A 身份被新校验器拒绝。
    let new = xl_client::TonicMesh::new_with_tls(
        "n1",
        SECRET,
        &nodes,
        Some(client_tls_identity(&ca_a.pem, &id_b, &id_key_b)),
    )
    .unwrap();
    assert!(
        new.health("n1").await.is_ok(),
        "新 client_ca 必须对新连接生效"
    );
    let rejected = xl_client::TonicMesh::new_with_tls(
        "n1",
        SECRET,
        &nodes,
        Some(client_tls_identity(&ca_a.pem, &id_a, &id_key_a)),
    )
    .unwrap();
    assert!(
        rejected.health("n1").await.is_err(),
        "旧 CA 身份必须被新连接校验拒绝"
    );
}

/// 明文部署：serve_tls_watch(None) 与旧 serve 行为一致。
#[tokio::test]
async fn plaintext_mesh_still_works() {
    let (addr, _task) = start_mesh(None).await;
    let nodes = vec![("n1".to_string(), addr.to_string())];
    let mesh = xl_client::TonicMesh::new("n1", SECRET, &nodes).unwrap();
    assert!(mesh.health("n1").await.is_ok());
}
