//! M8-3：控制台读一致性（`?consistent=1`）与 `x-oss-applied-index`。
//!
//! 默认 follower 是本地弱读；`consistent=1` 先有界追赶 leader 请求时刻的 applied
//! index，再读本地；无 leader / 追赶超时 → 503（fail-closed）。

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use oss_server::raft::control::ControlPlane;

const APPLIED_HEADER: &str = "x-oss-applied-index";

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn with_raft_control(
    st: &mut oss_server::state::AppState,
    handle: &Arc<oss_server::raft::RaftHandle>,
) {
    let control = ControlPlane::raft(
        Arc::clone(handle),
        Arc::clone(&st.db),
        Arc::clone(&st.store),
        Arc::clone(&st.ap_cache),
        Arc::clone(&st.olap_cache),
        st.ap_region.clone(),
    );
    st.control = Arc::new(control);
}

fn applied_header(resp: &reqwest::Response) -> u64 {
    resp.headers()
        .get(APPLIED_HEADER)
        .unwrap_or_else(|| panic!("缺少 {APPLIED_HEADER}"))
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

/// 无 leader（bootstrap=false 且成员表为空）：`consistent=1` 必须 503；
/// 默认弱读保持 200。中间件判定与 fail-closed 分支不需要真实集群。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consistent_read_without_leader_is_503_and_default_read_is_200() {
    let dir = tempfile::tempdir().unwrap();
    let node =
        common::raft_harness::spawn_node(common::raft_harness::settings(dir.path(), 1, false))
            .await;
    assert!(!node.handle.is_leader(), "无成员节点不应是 leader");
    assert_eq!(node.handle.leader_endpoint(), None);

    let mut st = common::state(dir.path()).await;
    st.db.create_user("erik", "pw", true).await.unwrap();
    with_raft_control(&mut st, &node.handle);
    let base = serve(oss_server::router::build_router(st).unwrap()).await;
    let http = reqwest::Client::new();
    let login: serde_json::Value = http
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tok = login["token"].as_str().unwrap().to_string();

    let weak = http
        .get(format!("{base}/api/v1/accounts"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(weak.status(), StatusCode::OK, "默认弱读保持 200");
    assert_eq!(applied_header(&weak), 0, "无 apply 记录时上报 0");

    let consistent = http
        .get(format!("{base}/api/v1/accounts?consistent=1"))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    assert_eq!(
        consistent.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "无 leader 时 consistent=1 必须 fail-closed 503"
    );
    assert_eq!(applied_header(&consistent), 0);

    node.shutdown().await;
}
