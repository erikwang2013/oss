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

use super::{RPC_TOKEN_HEADER, TypeConfig};

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
}

impl HttpNetworkFactory {
    pub fn new(cluster_token: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            token: Arc::new(cluster_token),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        HttpNetwork {
            client: self.client.clone(),
            token: self.token.clone(),
            target,
            endpoint: node.addr.trim_end_matches('/').to_string(),
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
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .timeout(ttl)
            .body(body)
            .send()
            .await
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
        let body = encode(&rpc)?;
        let resp = self
            .post(APPEND_PATH, body, option.hard_ttl())
            .await
            .map_err(transport_err)?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(transport_err)?;
        if status.is_success() {
            return serde_json::from_slice(&bytes)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        }
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            return Err(RPCError::PayloadTooLarge(
                PayloadTooLarge::new_entries_hint(entries.max(1) as u64),
            ));
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
        let body = encode(&rpc)?;
        let resp = self
            .post(VOTE_PATH, body, option.hard_ttl())
            .await
            .map_err(transport_err)?;
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
        let body = encode(&rpc)?;
        let resp = self
            .post(SNAPSHOT_PATH, body, option.hard_ttl())
            .await
            .map_err(transport_err)?;
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

    #[test]
    fn token_auth_is_exact_and_fails_closed() {
        assert!(token_matches("abcd", Some("abcd")));
        assert!(!token_matches("abcd", Some("abce")));
        assert!(!token_matches("abcd", Some("abc")));
        assert!(!token_matches("abcd", None));
        assert!(!token_matches("", Some("")));
    }
}
