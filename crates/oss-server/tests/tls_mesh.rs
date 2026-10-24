//! 节点间 gRPC mesh TLS：tonic 服务端/客户端证书校验、secret 拦截器、mTLS。

mod common;

use std::sync::Arc;

use oss_server::tls::Tls;
use oss_server::tls_config::{TlsClientToml, TlsServerToml, TlsToml};
use xl_store::error::Error as StoreError;
use xl_store::mesh::{MeshTransport, MpuComplete, MpuEnc, MpuPartUpload};
use xl_store::store::{ByteStream, Store, StoreConfig, VersioningMode};

use common::tls::{TestCa, free_port, wait_tcp};

fn material(
    dir: &std::path::Path,
    ca_pem: &str,
    server: (&str, &str),
    client_identity: Option<(&str, &str)>,
    client_ca: Option<&std::path::Path>,
) -> Arc<Tls> {
    let cert = common::tls::write_pem(dir, "server.crt", server.0);
    let key = common::tls::write_pem(dir, "server.key", server.1);
    let ca = common::tls::write_pem(dir, "ca.crt", ca_pem);
    let identity = client_identity.map(|(c, k)| {
        (
            common::tls::write_pem(dir, "client.crt", c),
            common::tls::write_pem(dir, "client.key", k),
        )
    });
    Tls::load(&TlsToml {
        server: Some(TlsServerToml {
            cert,
            key,
            client_ca: client_ca.map(|p| p.to_path_buf()),
        }),
        client: Some(TlsClientToml {
            ca_cert: Some(ca),
            client_cert: identity.as_ref().map(|(c, _)| c.clone()),
            client_key: identity.map(|(_, k)| k),
        }),
        ..Default::default()
    })
    .unwrap()
}

async fn start_mesh(tls: &Tls) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    // TempDir 必须活到 server 结束；泄漏到进程结束（测试进程短命）。
    std::mem::forget(dir);
    let port = free_port();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let tls = tls.tonic_server();
    let task = tokio::spawn(async move {
        let _ = xl_client::serve_tls(store, addr, "mesh-secret".into(), tls).await;
    });
    wait_tcp(port).await;
    (addr, task)
}

#[tokio::test]
async fn mesh_health_over_tls_and_secret_required() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let tls = material(dir.path(), &ca.pem, (&cert, &key), None, None);
    let (addr, _task) = start_mesh(&tls).await;

    let nodes = vec![("n1".to_string(), addr.to_string())];
    let mesh =
        xl_client::TonicMesh::new_with_tls("n1", "mesh-secret", &nodes, Some(tls.tonic_client()))
            .unwrap();
    assert!(mesh.health("n1").await.is_ok());

    let wrong =
        xl_client::TonicMesh::new_with_tls("n1", "wrong-secret", &nodes, Some(tls.tonic_client()))
            .unwrap();
    assert!(wrong.health("n1").await.is_err());
}

#[tokio::test]
async fn mesh_wrong_ca_or_hostname_fails() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let other = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let tls = material(dir.path(), &ca.pem, (&cert, &key), None, None);
    let (addr, _task) = start_mesh(&tls).await;
    let nodes = vec![("n1".to_string(), addr.to_string())];

    let wrong_ca = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&other.pem));
    let mesh =
        xl_client::TonicMesh::new_with_tls("n1", "mesh-secret", &nodes, Some(wrong_ca)).unwrap();
    assert!(mesh.health("n1").await.is_err(), "错 CA 必须失败");

    // 证书只签 localhost：IP 连接必须因主机名不匹配失败。
    let dir2 = tempfile::tempdir().unwrap();
    let (cert2, key2) = ca.issue(&["localhost"], false);
    let tls2 = material(dir2.path(), &ca.pem, (&cert2, &key2), None, None);
    let (addr2, _task2) = start_mesh(&tls2).await;
    let nodes2 = vec![("n1".to_string(), addr2.to_string())];
    let mesh2 =
        xl_client::TonicMesh::new_with_tls("n1", "mesh-secret", &nodes2, Some(tls2.tonic_client()))
            .unwrap();
    assert!(mesh2.health("n1").await.is_err(), "主机名不匹配必须失败");
}

#[tokio::test]
async fn mesh_mtls_requires_client_identity() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let (client_cert, client_key) = ca.issue(&["oss-test-client"], true);
    let ca_path = common::tls::write_pem(dir.path(), "client-ca.crt", &ca.pem);

    let server = material(
        dir.path(),
        &ca.pem,
        (&cert, &key),
        Some((&client_cert, &client_key)),
        Some(&ca_path),
    );
    let (addr, _task) = start_mesh(&server).await;
    let nodes = vec![("n1".to_string(), addr.to_string())];

    let with_identity = xl_client::TonicMesh::new_with_tls(
        "n1",
        "mesh-secret",
        &nodes,
        Some(server.tonic_client()),
    )
    .unwrap();
    assert!(with_identity.health("n1").await.is_ok());

    // 无客户端身份（仅 CA）：mTLS 服务端必须拒绝。
    let anonymous = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&ca.pem));
    let mesh =
        xl_client::TonicMesh::new_with_tls("n1", "mesh-secret", &nodes, Some(anonymous)).unwrap();
    assert!(
        mesh.health("n1").await.is_err(),
        "mTLS 必须拒绝无客户端证书的 mesh 连接"
    );
}

/// mesh gRPC 通道热加载：客户端 CA 轮换后下一次 RPC 用新物料重建通道；
/// 坏物料被 reload 拒绝（旧快照与代次都不变），旧通道继续服务。
#[tokio::test]
async fn mesh_channel_adopts_tls_reload_and_rejects_bad_material() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let other = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    // 服务端叶证书由 ca 签发；客户端初始只信任无关的 other（模拟轮换前状态）。
    let tls = material(dir.path(), &other.pem, (&cert, &key), None, None);
    let (addr, _task) = start_mesh(&tls).await;
    let nodes = vec![("n1".to_string(), addr.to_string())];
    let mesh = xl_client::TonicMesh::new_with_tls_watch(
        "n1",
        "mesh-secret",
        &nodes,
        Some(tls.tonic_client_watch()),
    )
    .unwrap();
    assert!(mesh.health("n1").await.is_err(), "错 CA 必须失败");
    assert_eq!(tls.generation(), 0);

    // 只换客户端信任根（服务端证书不变）：成功 reload 后代次推进，下一次 RPC 重建通道。
    common::tls::write_pem(dir.path(), "ca.crt", &ca.pem);
    assert!(tls.reload().unwrap(), "CA 内容变化必须触发重建");
    assert_eq!(tls.generation(), 1);
    assert!(
        mesh.health("n1").await.is_ok(),
        "reload 后下一次 RPC 必须使用新 TLS 物料"
    );

    // 坏 CA：reload fail closed；快照/代次不变，旧通道继续可用。
    std::fs::write(dir.path().join("ca.crt"), b"not a pem").unwrap();
    assert!(tls.reload().is_err(), "非法 CA 必须被拒绝");
    assert_eq!(tls.generation(), 1, "失败的 reload 不得推进代次");
    assert!(
        mesh.health("n1").await.is_ok(),
        "坏物料不得破坏已建立的 mesh 通道"
    );
}

fn ssec_mpu_complete(upload_id: &str) -> MpuComplete {
    MpuComplete {
        bucket: "bucket".into(),
        key: "k".into(),
        upload_id: upload_id.into(),
        parts: vec![(1, "etag".into())],
        mode: VersioningMode::Off,
        encryption: Some(MpuEnc::SseC([7u8; 32])),
        lock: None,
    }
}

/// L-3：明文 mesh 上 SSE-C MPU 转发必须 fail-closed——客户密钥/分片不得出网。
#[tokio::test]
async fn plaintext_mesh_refuses_ssec_mpu_forwarding() {
    // 端口 1 不可达即可：拒绝发生在任何 RPC/连接之前。
    let nodes = vec![("n1".to_string(), "127.0.0.1:1".to_string())];
    let mesh = xl_client::TonicMesh::new("n1", "mesh-secret", &nodes).unwrap();

    let err = mesh
        .mpu_complete("n1", ssec_mpu_complete("upload"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Unavailable(_)), "实得：{err:?}");
    assert!(
        err.to_string().contains("TLS"),
        "必须给出明确的 TLS 配置要求：{err}"
    );

    // 带 SSE-C 校验头的 UploadPart 同样拒绝（不把分片送上明文通道）。
    let body: ByteStream = Box::pin(futures::stream::empty());
    let err = mesh
        .mpu_upload_part(
            "n1",
            MpuPartUpload {
                bucket: "bucket".into(),
                key: "k".into(),
                upload_id: "upload".into(),
                part_number: 1,
                sse_c_md5: Some("md5".into()),
                body,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Unavailable(_)), "实得：{err:?}");
    assert!(err.to_string().contains("TLS"), "实得：{err}");
}

/// L-3：启用 mesh TLS 后 SSE-C Complete 可跨节点转发（到达属主；语义错误来自属主）。
#[tokio::test]
async fn tls_mesh_forwards_ssec_mpu_complete() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert, key) = ca.issue(&["127.0.0.1"], false);
    let tls = material(dir.path(), &ca.pem, (&cert, &key), None, None);
    let (addr, _task) = start_mesh(&tls).await;
    let nodes = vec![("n1".to_string(), addr.to_string())];
    let mesh =
        xl_client::TonicMesh::new_with_tls("n1", "mesh-secret", &nodes, Some(tls.tonic_client()))
            .unwrap();

    let err = mesh
        .mpu_complete("n1", ssec_mpu_complete("missing-upload"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            StoreError::NoSuchBucket(_)
                | StoreError::NoSuchUpload(_)
                | StoreError::InvalidArgument(_)
        ),
        "TLS 下 SSE-C Complete 必须放行到属主（本地 TLS 门不得误拒）：{err:?}"
    );
}
