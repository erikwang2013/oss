//! TLS 配置矩阵：证书加载、Kafka TLS/SASL 构建、remotes/tiers https、fail-closed 校验。

mod common;

use oss_server::config::{AppConfig, WitnessToml};
use oss_server::tls::Tls;
use oss_server::tls_config::TlsToml;

use common::tls::TestCa;

fn base_toml(extra: &str) -> String {
    format!(
        r#"
listen = "127.0.0.1:0"
data_dir = "/tmp/oss-tls-test/data"
staging_dir = "/tmp/oss-tls-test/staging"
database_url = "sqlite::memory:"
jwt_secret = "0123456789abcdef0123456789abcdef"
{extra}
"#
    )
}

fn parse(extra: &str) -> AppConfig {
    toml::from_str(&base_toml(extra)).unwrap()
}

#[test]
fn certs_load_and_all_client_backends_build() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let (cert, key) = ca.issue(&["localhost"], false);
    let cert = common::tls::write_pem(dir.path(), "server.crt", &cert);
    let key = common::tls::write_pem(dir.path(), "server.key", &key);
    let ca_path = common::tls::write_pem(dir.path(), "ca.crt", &ca.pem);
    let cfg = TlsToml {
        server: Some(oss_server::tls_config::TlsServerToml {
            cert,
            key,
            client_ca: None,
        }),
        client: Some(oss_server::tls_config::TlsClientToml {
            ca_cert: Some(ca_path),
            client_cert: None,
            client_key: None,
        }),
    };
    let tls = Tls::load(&cfg).unwrap();
    assert!(tls.peer_tls());
    assert!(tls.ecat_server().is_some());
    assert!(tls.tonic_server().is_some());
    assert!(tls.rustls_client_config().is_ok());
    assert!(tls.reqwest_client().is_ok());
    assert!(tls.kafka_config().is_ok());
    assert_eq!(tls.peer_url("http://h:1").unwrap(), "https://h:1");
    assert_eq!(tls.peer_url("https://h:1").unwrap(), "https://h:1");
}

#[test]
fn plaintext_mode_rejects_https_peer_addresses() {
    let tls = Tls::load(&TlsToml::default()).unwrap();
    assert!(!tls.peer_tls());
    // 明文默认不变：不生成任何监听器 TLS 配置。
    assert!(tls.ecat_server().is_none());
    assert!(tls.tonic_server().is_none());
    assert!(tls.rustls_client_config().is_ok());
    assert!(tls.peer_url("https://h:1").is_err());
    assert_eq!(tls.peer_url("h:1").unwrap(), "http://h:1");
}

#[test]
fn remotes_and_tiers_accept_https_and_server_name() {
    let cfg = parse(
        r#"
[[remotes]]
id = "r1"
endpoint = "https://s3.example.com"
access_key = "ak"
secret_key = "sk"
server_name = "s3.example.com"

[[tiers]]
id = "t1"
endpoint = "https://tier.example.com:8443"
bucket = "cold"
access_key = "ak"
secret_key = "sk"
server_name = "tier.internal"
"#,
    );
    cfg.remotes_validate().unwrap();
    cfg.tiers_validate().unwrap();
}

#[test]
fn server_name_requires_https() {
    let cfg = parse(
        r#"
[[remotes]]
id = "r1"
endpoint = "http://s3.example.com"
access_key = "ak"
secret_key = "sk"
server_name = "s3.example.com"
"#,
    );
    assert!(cfg.remotes_validate().is_err());

    let cfg = parse(
        r#"
[[tiers]]
id = "t1"
endpoint = "http://tier.example.com"
bucket = "cold"
access_key = "ak"
secret_key = "sk"
server_name = "tier.internal"
"#,
    );
    assert!(cfg.tiers_validate().is_err());
}

#[test]
fn kafka_tls_sasl_matrix() {
    let ok = parse(
        r#"
[[kafka.targets]]
id = "k1"
brokers = ["broker:9093"]
topic = "events"
tls = true
[kafka.targets.sasl]
mechanism = "scram-sha-256"
username = "u"
password = "p"
"#,
    );
    ok.kafka_validate().unwrap();

    // SASL 不走 TLS：启动期拒绝。
    let sasl_without_tls = parse(
        r#"
[[kafka.targets]]
id = "k1"
brokers = ["broker:9093"]
topic = "events"
[kafka.targets.sasl]
username = "u"
password = "p"
"#,
    );
    assert!(sasl_without_tls.kafka_validate().is_err());

    // 非法 mechanism / 空凭证。
    for sasl in [
        "mechanism = \"oauth\"\nusername = \"u\"\npassword = \"p\"",
        "mechanism = \"plain\"\nusername = \"u\"\npassword = \"\"",
    ] {
        let cfg = parse(&format!(
            r#"
[[kafka.targets]]
id = "k1"
brokers = ["broker:9093"]
topic = "events"
tls = true
[kafka.targets.sasl]
{sasl}
"#
        ));
        assert!(cfg.kafka_validate().is_err(), "{sasl}");
    }
}

#[test]
fn listen_scheme_must_match_tls_mode() {
    let mut cfg = parse("");
    cfg.listen = "https://127.0.0.1:9443".into();
    assert!(
        cfg.tls_validate().is_err(),
        "https listen 缺 [tls.server] 必须拒绝"
    );

    cfg.listen = "127.0.0.1:9443".into();
    cfg.tls = toml::from_str("[server]\ncert = \"c.pem\"\nkey = \"k.pem\"\n").unwrap();
    cfg.tls_validate().unwrap();

    cfg.listen = "http://127.0.0.1:9443".into();
    assert!(
        cfg.tls_validate().is_err(),
        "[tls.server] 下写 http:// listen 必须拒绝（避免误以为明文）"
    );
}

#[test]
fn raft_and_witness_https_advertise_require_server_tls() {
    let cfg = parse(
        r#"
[site]
[raft]
node_id = 1
bootstrap = true
advertise_endpoint = "https://127.0.0.1:9000"
cluster_token = "token-token-token-token-token-token"
"#,
    );
    assert!(cfg.raft_validate().is_err());

    let cfg = parse(
        r#"
[site]
[raft]
node_id = 1
bootstrap = true
advertise_endpoint = "https://127.0.0.1:9000"
cluster_token = "token-token-token-token-token-token"
[tls.server]
cert = "c.pem"
key = "k.pem"
"#,
    );
    cfg.raft_validate().unwrap();

    let witness: WitnessToml = toml::from_str(
        r#"
[raft]
node_id = 3
role = "witness"
bootstrap = false
advertise_endpoint = "https://127.0.0.1:9100"
cluster_token = "token-token-token-token-token-token"
db_path = "/tmp/w/raft.db"
state_db = "/tmp/w/state.db"
"#,
    )
    .unwrap();
    assert!(witness.validate().is_err());
    let witness: WitnessToml = toml::from_str(
        r#"
[raft]
node_id = 3
role = "witness"
bootstrap = false
advertise_endpoint = "https://127.0.0.1:9100"
cluster_token = "token-token-token-token-token-token"
db_path = "/tmp/w/raft.db"
state_db = "/tmp/w/state.db"
[tls.server]
cert = "c.pem"
key = "k.pem"
"#,
    )
    .unwrap();
    witness.validate().unwrap();
}

#[test]
fn client_tls_section_validates_pairing_and_nonempty() {
    let empty: TlsToml = toml::from_str("[client]\n").unwrap();
    assert!(empty.validate().is_err());
    let half: TlsToml = toml::from_str("[client]\nclient_cert = \"c\"\n").unwrap();
    assert!(half.validate().is_err());
    let ok: TlsToml =
        toml::from_str("[client]\nclient_cert = \"c\"\nclient_key = \"k\"\n").unwrap();
    ok.validate().unwrap();
}
