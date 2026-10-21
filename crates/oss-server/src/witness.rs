//! S7.1：witness 运行时（`oss-server witness --config <w.toml>`）。
//!
//! witness 只有控制面：控制库 + Raft 句柄 + `/api/v1/site/raft/*` HTTP。无 `AppState`、
//! 无 JWT、无 `[site]`/数据目录/S3 面；控制库不种 `site_config`。

use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;

use crate::config::WitnessToml;
use crate::db::Db;
use crate::raft::{self, RaftHandle, RaftSettings, identity};

/// 启动 witness，监听 `advertise_endpoint` 的 host:port，ctrl_c 优雅退出。
pub async fn run(config_path: &Path) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let cfg = WitnessToml::load(config_path)?;
    let token = cfg.raft.cluster_token.clone();
    let (_db, handle) = start(&cfg).await?;
    let addr = bind_addr(&cfg.raft.advertise_endpoint).await?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("witness 监听 {addr} 绑定失败: {e}"))?;
    tracing::info!(%addr, node_id = handle.node_id(), "witness 启动");
    axum::serve(listener, build_app(Arc::clone(&handle), token))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    handle.shutdown().await;
    Ok(0)
}

/// 打开控制库 → 身份校验 → 启动 Raft（bootstrap=false，等待 join）→ 记录 initialized。
async fn start(
    cfg: &WitnessToml,
) -> Result<(Arc<Db>, Arc<RaftHandle>), Box<dyn std::error::Error + Send + Sync>> {
    let state_db = cfg
        .raft
        .state_db
        .as_ref()
        .ok_or("witness 配置缺少 [raft].state_db")?;
    let settings =
        RaftSettings::from_toml(&cfg.raft).map_err(|e| format!("[raft] 配置非法: {e}"))?;
    for p in [state_db.as_path(), settings.db_path.as_path()] {
        if let Some(dir) = p.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)
                .map_err(|e| format!("创建 witness 目录 {} 失败: {e}", dir.display()))?;
        }
    }
    let url = format!("sqlite://{}?mode=rwc", state_db.display());
    let db = Arc::new(Db::connect_witness(&url).await?);
    identity::verify_and_seed(&db, &settings).await?;
    let handle = Arc::new(raft::spawn_with_control(settings, Some(db.pool().clone())).await?);
    identity::record_initialized(&db, handle.membership_nonempty()).await?;
    Ok((db, handle))
}

fn build_app(handle: Arc<RaftHandle>, token: String) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/health/live", get(|| async { "ok" }))
        .merge(raft::routes::rpc_router(handle, token))
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
        let (db, handle) = start(&cfg).await.unwrap();
        assert!(!handle.membership_nonempty());

        // 身份持久化 + witness 控制库无站点行。
        assert_eq!(
            db.schema_meta(identity::META_NODE_ID)
                .await
                .unwrap()
                .as_deref(),
            Some("3")
        );
        assert_eq!(
            db.schema_meta(identity::META_ROLE)
                .await
                .unwrap()
                .as_deref(),
            Some("witness")
        );
        assert_eq!(
            db.schema_meta(identity::META_INITIALIZED)
                .await
                .unwrap()
                .as_deref(),
            Some("0")
        );
        let sites: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM site_config")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(sites, 0);

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
