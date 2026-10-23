//! `[tls.server]` / `[tls.client]` 与 Kafka `tls`/`sasl` 配置模型及地址校验。
//!
//! 模式模型：`[tls.server]` 存在 = 集群/站点/mesh/Raft 全链路 TLS（peer TLS）。
//! 配置里的对端地址仍可写 `http://`，连接时由 [`crate::tls::Tls::peer_url`] 升级为
//! https；未启用 peer TLS 时写 `https://` 一律启动期拒绝（不静默降级明文）。

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsToml {
    /// 服务端证书（主 HTTP/S3/控制台/witness/Raft RPC 共用）。存在即启用 peer TLS。
    #[serde(default)]
    pub server: Option<TlsServerToml>,
    /// 出站信任：自定义 CA 与可选客户端身份（mTLS）；不开启任何服务端 TLS。
    #[serde(default)]
    pub client: Option<TlsClientToml>,
    /// 物料热加载轮询间隔（秒）：默认 30；`0` = 关闭轮询、只响应 SIGHUP；最小 1。
    /// 轮询按文件内容哈希去重，内容不变不重建。
    #[serde(default)]
    pub reload_interval_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsServerToml {
    pub cert: PathBuf,
    pub key: PathBuf,
    /// 配置后要求并校验客户端证书（mTLS）；共享监听器上的 S3/控制台/Raft 客户端都必须出示。
    #[serde(default)]
    pub client_ca: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsClientToml {
    /// 额外信任根（PEM，可多证书）。平台根始终参与校验。
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
    /// 客户端证书 + 私钥（成对；mTLS 出站身份）。
    #[serde(default)]
    pub client_cert: Option<PathBuf>,
    #[serde(default)]
    pub client_key: Option<PathBuf>,
}

impl TlsToml {
    pub fn peer_tls(&self) -> bool {
        self.server.is_some()
    }

    pub fn validate(&self) -> Result<(), String> {
        if let Some(c) = &self.client {
            if c.ca_cert.is_none() && c.client_cert.is_none() && c.client_key.is_none() {
                return Err(
                    "[tls.client] 不能为空（配置 ca_cert 和/或 client_cert+client_key；不需要则删除该节）"
                        .into(),
                );
            }
            if c.client_cert.is_some() != c.client_key.is_some() {
                return Err("[tls.client] client_cert 与 client_key 必须成对配置".into());
            }
        }
        Ok(())
    }
}

/// `[[kafka.targets]].sasl`：rskafka 支持 PLAIN/SCRAM-SHA-256/SCRAM-SHA-512。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSaslToml {
    /// `plain`（默认）/ `scram-sha-256` / `scram-sha-512`。
    #[serde(default = "default_sasl_mechanism")]
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

fn default_sasl_mechanism() -> String {
    "plain".to_string()
}

impl KafkaSaslToml {
    pub fn validate(&self, target_id: &str) -> Result<(), String> {
        match self.mechanism.as_str() {
            "plain" | "scram-sha-256" | "scram-sha-512" => {}
            m => {
                return Err(format!(
                    "[[kafka.targets]] {target_id} 的 sasl.mechanism 非法：{m:?}（plain/scram-sha-256/scram-sha-512）"
                ));
            }
        }
        if self.username.is_empty() || self.password.is_empty() {
            return Err(format!(
                "[[kafka.targets]] {target_id} 的 sasl.username/password 不能为空"
            ));
        }
        Ok(())
    }
}

/// 解析后的出站端点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedEndpoint {
    /// 去除尾斜杠的 `scheme://host[:port]`（不含 path/query/userinfo）。
    pub base: String,
    pub https: bool,
    pub host: String,
    pub port: u16,
}

/// 出站 S3 端点（`[[remotes]]`/`[[tiers]]`/DB 内站点地址）校验：
/// `http|https://host[:port]`，无 path/query/userinfo；缺省端口 80/443。
pub fn parse_endpoint(raw: &str, field: &str) -> Result<ParsedEndpoint, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let uri: hyper::Uri = trimmed
        .parse()
        .map_err(|_| format!("{field} 非法：{raw}"))?;
    let https = match uri.scheme_str() {
        Some("http") => false,
        Some("https") => true,
        _ => return Err(format!("{field} 必须是 http:// 或 https://：{raw}")),
    };
    let host = uri
        .host()
        .ok_or_else(|| format!("{field} 缺 host：{raw}"))?
        .to_string();
    if uri.path() != "/" && !uri.path().is_empty() {
        return Err(format!("{field} 不得带 path：{raw}"));
    }
    if uri.query().is_some() {
        return Err(format!("{field} 不得带 query：{raw}"));
    }
    if uri.authority().is_some_and(|a| a.as_str().contains('@')) {
        return Err(format!("{field} 不得带 userinfo：{raw}"));
    }
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    Ok(ParsedEndpoint {
        base: trimmed.to_string(),
        https,
        host,
        port,
    })
}

/// 集群/站点/Raft/mesh 地址：接受 `host:port`、`http://host:port`、`https://host:port`。
/// 返回（小写 host 的 `host:port`，是否 https）。
pub fn normalize_peer_addr(raw: &str, field: &str) -> Result<(String, bool), String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(format!("{field} 不能为空"));
    }
    let (rest, https) = if let Some(r) = s.strip_prefix("http://") {
        (r, false)
    } else if let Some(r) = s.strip_prefix("https://") {
        (r, true)
    } else if s.contains("://") {
        return Err(format!(
            "{field} 必须是 host:port 或 http(s)://host:port：{raw}"
        ));
    } else {
        (s, false)
    };
    // URL 形式的配置常带一个尾斜杠（空 path），等价于 host:port；其余 path 仍拒绝。
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    if rest.is_empty()
        || rest.contains('/')
        || rest.contains('?')
        || rest.contains('#')
        || rest.contains('@')
    {
        return Err(format!(
            "{field} 非法：{raw:?}（不接受 path/query/userinfo，需 host:port）"
        ));
    }
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| format!("{field} 缺少端口：{raw:?}"))?;
    if host.is_empty() || port.is_empty() || port.parse::<u16>().ok().filter(|p| *p != 0).is_none()
    {
        return Err(format!("{field} 的 host/port 非法：{raw:?}"));
    }
    Ok((format!("{}:{port}", host.to_ascii_lowercase()), https))
}

/// `server_name` 覆写只允许 DNS 名或 IP（rustls 校验用）。
pub fn validate_server_name(name: &str, field: &str) -> Result<(), String> {
    rustls::pki_types::ServerName::try_from(name.to_string())
        .map(|_| ())
        .map_err(|_| format!("{field} 不是合法的 DNS 名或 IP：{name:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_endpoint_matrix() {
        let p = parse_endpoint("https://S3.Example.com/", "ep").unwrap();
        assert!(p.https);
        assert_eq!(p.base, "https://S3.Example.com");
        assert_eq!(p.host, "S3.Example.com");
        assert_eq!(p.port, 443);
        assert_eq!(parse_endpoint("http://h:9000", "ep").unwrap().port, 9000);
        assert!(!parse_endpoint("http://h", "ep").unwrap().https);
        for bad in [
            "ftp://h:1",
            "https://h:1/path",
            "https://h:1?x=1",
            "https://u@h:1",
            "h:1",
        ] {
            assert!(parse_endpoint(bad, "ep").is_err(), "{bad}");
        }
    }

    #[test]
    fn normalize_peer_addr_matrix() {
        assert!(normalize_peer_addr("HTTP://A:9000/", "f").is_err());
        assert_eq!(
            normalize_peer_addr("https://A:9000", "f").unwrap(),
            ("a:9000".into(), true)
        );
        assert_eq!(
            normalize_peer_addr("http://A:9000/", "f").unwrap(),
            ("a:9000".into(), false)
        );
        assert_eq!(
            normalize_peer_addr("127.0.0.1:1", "f").unwrap(),
            ("127.0.0.1:1".into(), false)
        );
        for bad in [
            "a",
            "a:0",
            ":9000",
            "http://a:9000/x",
            "http://a:9000//",
            "https://u@a:1",
        ] {
            assert!(normalize_peer_addr(bad, "f").is_err(), "{bad}");
        }
    }

    #[test]
    fn tls_client_must_be_nonempty_and_pair_identity() {
        let c: TlsToml = toml::from_str("[client]\n").unwrap();
        assert!(c.validate().is_err());
        let c: TlsToml = toml::from_str("[client]\nclient_cert = \"c.pem\"\n").unwrap();
        assert!(c.validate().is_err());
        let c: TlsToml =
            toml::from_str("[client]\nclient_cert = \"c.pem\"\nclient_key = \"k.pem\"\n").unwrap();
        assert!(c.validate().is_ok());
        let c: TlsToml = toml::from_str("[client]\nca_cert = \"ca.pem\"\n").unwrap();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn kafka_sasl_mechanism_and_credentials() {
        let ok: KafkaSaslToml = toml::from_str("username = \"u\"\npassword = \"p\"\n").unwrap();
        assert!(ok.validate("t1").is_ok());
        let bad: KafkaSaslToml =
            toml::from_str("mechanism = \"oauth\"\nusername = \"u\"\npassword = \"p\"\n").unwrap();
        assert!(bad.validate("t1").is_err());
        let empty: KafkaSaslToml = toml::from_str("username = \"u\"\npassword = \"\"\n").unwrap();
        assert!(empty.validate("t1").is_err());
    }
}
