//! S7.1：witness 运行时（`oss-server witness --config <w.toml>`）。
//!
//! witness 只有控制面：文件 Raft 日志 + 元数据状态机 + `/api/v1/site/raft/*` HTTP。
//! 无 `AppState`、无 JWT、无 `[site]`/数据目录/S3 面；F2 起不再打开 SQLite（`state_db`
//! 只作为旧库迁移输入保留）。

use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use ecat_transport::Server as _;

use crate::config::WitnessToml;
use crate::raft::{self, RaftHandle, RaftSettings};

/// 启动 witness，监听 `advertise_endpoint` 的 host:port，ctrl_c 优雅退出。
pub async fn run(config_path: &Path) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let cfg = WitnessToml::load(config_path)?;
    let token = cfg.raft.cluster_token.clone();
    let (handle, tls) = start(&cfg).await?;
    let addr = bind_addr(&cfg.raft.advertise_endpoint).await?;
    tracing::info!(%addr, node_id = handle.node_id(), "witness 启动");
    // witness 与主服务共用 ecat 监听器（同一条 TLS/mTLS 配置）。
    let mut server =
        ecat::transport_http::HttpServer::new(addr.to_string()).router(build_app(handle, token));
    if let Some(t) = tls.ecat_server() {
        server = server.tls(t);
    }
    let server = Arc::new(server);
    let running = {
        let s = Arc::clone(&server);
        tokio::spawn(async move { s.start().await })
    };
    let _ = tokio::signal::ctrl_c().await;
    server.stop().await?;
    running.await??;
    Ok(0)
}

/// 文件日志 + 元数据状态机 → 启动 Raft（bootstrap=false，等待 join）。
async fn start(
    cfg: &WitnessToml,
) -> Result<(Arc<RaftHandle>, Arc<crate::tls::Tls>), Box<dyn std::error::Error + Send + Sync>> {
    if cfg.raft.state_db.is_some() {
        tracing::warn!("witness [raft].state_db 仅作为旧库迁移输入保留；本进程不打开 SQLite");
    }
    let settings =
        RaftSettings::from_toml(&cfg.raft).map_err(|e| format!("[raft] 配置非法: {e}"))?;
    if let Some(dir) = settings
        .db_path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
    {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("创建 witness 目录 {} 失败: {e}", dir.display()))?;
    }
    let tls = crate::tls::Tls::load(&cfg.tls)?;
    let handle =
        Arc::new(raft::spawn_witness(settings, tls.peer_tls().then(|| Arc::clone(&tls))).await?);
    Ok((handle, tls))
}

fn build_app(handle: Arc<RaftHandle>, token: String) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/health/live", get(|| async { "ok" }))
        .merge(raft::routes::rpc_router(handle, token, false, false))
}

async fn bind_addr(
    endpoint: &str,
) -> Result<std::net::SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let host_port = crate::config::normalize_http_addr(endpoint, "[raft].advertise_endpoint")?;
    match host_port.parse() {
        Ok(addr) => Ok(addr),
        Err(_) => tokio::net::lookup_host(&host_port)
            .await
            .map_err(|e| format!("witness 地址解析失败（{host_port}）: {e}"))?
            .next()
            .ok_or_else(|| format!("witness 地址无解析结果（{host_port}）").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn witness_runtime_serves_rpc_status_without_site_rows() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("witness.toml");
        std::fs::write(
            &cfg_path,
            format!(
                r#"
[raft]
node_id = 3
role = "witness"
bootstrap = false
advertise_endpoint = "http://127.0.0.1:1"
cluster_token = "witness-token"
db_path = "{}"
state_db = "{}"
"#,
                dir.path().join("raft.db").display(),
                dir.path().join("state.db").display(),
            ),
        )
        .unwrap();
        let cfg = WitnessToml::load(&cfg_path).unwrap();
        let (handle, _tls) = start(&cfg).await.unwrap();
        assert!(!handle.membership_nonempty());

        // F2：状态在 `<db_path>.d/witness/state`；旧 state_db 不再打开。
        assert!(!dir.path().join("state.db").exists(), "state_db 不得被打开");
        let state_path = dir.path().join("raft.db.d").join("witness").join("state");
        let state: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert_eq!(state["identity"]["raft_node_id"], "3");
        assert_eq!(state["identity"]["raft_role"], "witness");
        assert_eq!(state["identity"]["raft_initialized"], "0");
        assert!(state["identity"].get("raft_site_id").is_none());
        assert!(state["applied"].is_null());

        let app = build_app(Arc::clone(&handle), cfg.raft.cluster_token.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let http = reqwest::Client::new();
        assert_eq!(
            http.get(format!("{base}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let status_url = format!("{base}/api/v1/site/raft/status");
        assert_eq!(http.get(&status_url).send().await.unwrap().status(), 401);
        assert_eq!(
            http.get(&status_url)
                .header(raft::RPC_TOKEN_HEADER, "wrong")
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        let v: serde_json::Value = http
            .get(&status_url)
            .header(raft::RPC_TOKEN_HEADER, "witness-token")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(v["role"], "witness");
        assert_eq!(v["node_id"], 3);
        assert_eq!(v["bootstrap"], false);
        assert_eq!(v["initialized"], false);
        assert_eq!(v["production_ready"], false);

        handle.shutdown().await;
    }
}
