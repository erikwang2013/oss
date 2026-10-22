//! S5 基座网络：openraft v2 `RaftNetwork` 的 HTTP/1.1 实现骨架。
//!
//! 数据面走 `POST {endpoint}/api/v1/site/raft/{append,vote,snapshot}`，JSON 体 + 共享
//! `x-oss-raft-token` 集群令牌。服务端路由由 S6/S7 接入；本文件只保证客户端形状与错误映射。
//! 4 MiB 原始快照分块的 JSON 约 16 MiB，服务端路由必须放行 32 MiB（[`super::RPC_BODY_LIMIT`]）。

// openraft RPCError 体积由第三方类型固定，trait 签名不能改。
#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError, RemoteError,
    Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::Serialize;

use super::compat::{self, PeerVersions};
use super::{RPC_TARGET_HEADER, RPC_TOKEN_HEADER, TypeConfig};

const APPEND_PATH: &str = "/api/v1/site/raft/append";
const VOTE_PATH: &str = "/api/v1/site/raft/vote";
const SNAPSHOT_PATH: &str = "/api/v1/site/raft/snapshot";

/// 常量时间比较集群令牌（长度不同直接拒绝；不因前缀匹配提前返回）。
pub fn token_matches(expected: &str, provided: Option<&str>) -> bool {
    let Some(provided) = provided else {
        return false;
    };
    if expected.is_empty() {
        return false;
    }
    let (a, b) = (expected.as_bytes(), provided.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// 按目标节点地址构造 `HttpNetwork`。单节点基座不会发出 RPC。
#[derive(Clone)]
pub struct HttpNetworkFactory {
    client: reqwest::Client,
    token: Arc<String>,
    tls: Option<Arc<crate::tls::Tls>>,
    /// leader 侧 peer 版本表（follower 也记录，只有 leader 的用于门控）。
    peers: Arc<PeerVersions>,
    /// 本节点宣告的 feature = min(configured, MAX)（D3）。
    feature: u8,
}

impl HttpNetworkFactory {
    pub fn new(cluster_token: String, peers: Arc<PeerVersions>, feature: u8) -> Self {
        Self {
            client: reqwest::Client::new(),
            token: Arc::new(cluster_token),
            tls: None,
            peers,
            feature,
        }
    }

    /// peer TLS：RPC 地址按 [`crate::tls::Tls::peer_url`] 升级，客户端带自定义 CA/身份。
    pub fn new_tls(
        cluster_token: String,
        tls: Arc<crate::tls::Tls>,
        client: reqwest::Client,
        peers: Arc<PeerVersions>,
        feature: u8,
    ) -> Self {
        Self {
            client,
            token: Arc::new(cluster_token),
            tls: Some(tls),
            peers,
            feature,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let endpoint = match &self.tls {
            Some(tls) => match tls.peer_url(&node.addr) {
                Ok(url) => url,
                Err(e) => {
                    // new_client 不能返回错误：记录后指向必然失败的本地端口，绝不回退明文。
                    tracing::error!(target, error = %e, "Raft 对端地址无 TLS 可用模式");
                    "https://127.0.0.1:1".to_string()
                }
            },
            None => node.addr.trim_end_matches('/').to_string(),
        };
        HttpNetwork {
            client: self.client.clone(),
            token: self.token.clone(),
            target,
            endpoint,
            peers: Arc::clone(&self.peers),
            feature: self.feature,
        }
    }
}

/// 单目标连接；`Raft` 每次 RPC 克隆工厂后新建。
#[derive(Clone)]
pub struct HttpNetwork {
    client: reqwest::Client,
    token: Arc<String>,
    target: u64,
    endpoint: String,
    peers: Arc<PeerVersions>,
    feature: u8,
}

impl HttpNetwork {
    async fn post(
        &self,
        path: &str,
        body: Vec<u8>,
        ttl: Duration,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.client
            .post(format!("{}{}", self.endpoint, path))
            .header(RPC_TOKEN_HEADER, self.token.as_str())
            .header(RPC_TARGET_HEADER, self.target.to_string())
            .header(compat::WIRE_HEADER, compat::WIRE_VERSION.to_string())
            .header(compat::FEATURE_HEADER, self.feature.to_string())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .timeout(ttl)
            .body(body)
            .send()
            .await
    }

    /// 2xx 响应的握手结果按请求 term 记录；旧节点缺头 = 1（保守）。
    /// 非 2xx（421/422/500）不是可用记录，不覆盖已知版本。
    fn record_peer(&self, term: u64, resp: &reqwest::Response) {
        if !resp.status().is_success() {
            return;
        }
        let wire = compat::parse_wire(
            resp.headers()
                .get(compat::WIRE_HEADER)
                .and_then(|v| v.to_str().ok()),
        );
        let feature = compat::parse_feature(
            resp.headers()
                .get(compat::FEATURE_HEADER)
                .and_then(|v| v.to_str().ok()),
        );
        self.peers.record(self.target, term, wire, feature);
    }
}

fn transport_err<E: std::error::Error>(e: reqwest::Error) -> RPCError<u64, BasicNode, E> {
    if e.is_connect() || e.is_timeout() {
        RPCError::Unreachable(Unreachable::new(&e))
    } else {
        RPCError::Network(NetworkError::new(&e))
    }
}

fn encode<T: Serialize, E: std::error::Error>(
    v: &T,
) -> Result<Vec<u8>, RPCError<u64, BasicNode, E>> {
    serde_json::to_vec(v).map_err(|e| RPCError::Network(NetworkError::new(&e)))
}

impl RaftNetwork<TypeConfig> for HttpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let entries = rpc.entries.len();
        let term = rpc.vote.leader_id.term;
        let body = encode(&rpc)?;
        let resp = self
            .post(APPEND_PATH, body, option.hard_ttl())
            .await
            .map_err(transport_err)?;
        self.record_peer(term, &resp);
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(transport_err)?;
        if status.is_success() {
            return serde_json::from_slice(&bytes)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        }
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            // I2：按半数重试；单条仍超限时无法再拆，显式不可达而不是把 1 当 hint 死循环。
            if entries > 1 {
                return Err(RPCError::PayloadTooLarge(
                    PayloadTooLarge::new_entries_hint((entries / 2).max(1) as u64),
                ));
            }
            return Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(
                    "raft append entry exceeds server body limit; raise the HTTP body limit",
                ),
            )));
        }
        let remote: RaftError<u64> =
            serde_json::from_slice(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Err(RPCError::RemoteError(RemoteError::new(self.target, remote)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let term = rpc.vote.leader_id.term;
        let body = encode(&rpc)?;
        let resp = self
            .post(VOTE_PATH, body, option.hard_ttl())
            .await
            .map_err(transport_err)?;
        self.record_peer(term, &resp);
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(transport_err)?;
        if status.is_success() {
            return serde_json::from_slice(&bytes)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        }
        let remote: RaftError<u64> =
            serde_json::from_slice(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Err(RPCError::RemoteError(RemoteError::new(self.target, remote)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let term = rpc.vote.leader_id.term;
        let body = encode(&rpc)?;
        let resp = self
            .post(SNAPSHOT_PATH, body, option.hard_ttl())
            .await
            .map_err(transport_err)?;
        self.record_peer(term, &resp);
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(transport_err)?;
        if status.is_success() {
            return serde_json::from_slice(&bytes)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        }
        // 413：分块仍超路由上限（服务端配置错误）。openraft 对快照没有更小分块提示，
        // 这里按不可达报错让上层显式失败而不是无限重试。
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            return Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other("raft snapshot chunk exceeds server body limit"),
            )));
        }
        let remote: RaftError<u64, InstallSnapshotError> =
            serde_json::from_slice(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Err(RPCError::RemoteError(RemoteError::new(self.target, remote)))
    }
}

#[cfg(test)]
mod tests {
    use super::token_matches;
    use super::*;
    use openraft::entry::Entry;
    use openraft::{CommittedLeaderId, LogId, Vote};
    use std::time::Duration;

    #[test]
    fn token_auth_is_exact_and_fails_closed() {
        assert!(token_matches("abcd", Some("abcd")));
        assert!(!token_matches("abcd", Some("abce")));
        assert!(!token_matches("abcd", Some("abc")));
        assert!(!token_matches("abcd", None));
        assert!(!token_matches("", Some("")));
    }

    async fn rejecting_server() -> String {
        let app = axum::Router::new().route(
            APPEND_PATH,
            axum::routing::post(|| async { axum::http::StatusCode::PAYLOAD_TOO_LARGE }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    async fn append(
        endpoint: &str,
        entries: usize,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut factory = HttpNetworkFactory::new(
            "t".into(),
            Arc::new(PeerVersions::default()),
            compat::MIN_FEATURE_LEVEL,
        );
        let node = BasicNode {
            addr: endpoint.to_string(),
        };
        let mut net = factory.new_client(2, &node).await;
        let leader = LogId::new(CommittedLeaderId::new(1, 1), 0);
        let entries: Vec<_> = (1..=entries as u64)
            .map(|i| Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), i),
                payload: openraft::EntryPayload::Blank,
            })
            .collect();
        let last = entries.last().map(|e| e.log_id).unwrap_or(leader);
        net.append_entries(
            AppendEntriesRequest {
                vote: Vote::new_committed(1, 1),
                prev_log_id: Some(leader),
                entries,
                leader_commit: Some(last),
            },
            RPCOption::new(Duration::from_secs(5)),
        )
        .await
    }

    #[tokio::test]
    async fn payload_too_large_halves_batch_and_fails_loudly_for_single_entry() {
        let endpoint = rejecting_server().await;
        match append(&endpoint, 4).await {
            Err(RPCError::PayloadTooLarge(e)) => assert_eq!(e.entries_hint(), 2),
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
        match append(&endpoint, 1).await {
            Err(RPCError::Unreachable(_)) => {}
            other => panic!("expected Unreachable for single entry, got {other:?}"),
        }
    }
}
