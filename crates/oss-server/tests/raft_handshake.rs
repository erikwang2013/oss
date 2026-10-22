//! M9 滚动升级 Level B：RPC 握手中间件（R1/R3）与响应头。

mod common;

use common::raft_harness;
use oss_server::raft::compat::{FEATURE_HEADER, WIRE_HEADER};
use oss_server::raft::{RPC_TARGET_HEADER, RPC_TOKEN_HEADER};

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

fn header(resp: &reqwest::Response, name: &str) -> String {
    resp.headers()
        .get(name)
        .unwrap_or_else(|| panic!("missing response header {name}"))
        .to_str()
        .unwrap()
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_three_is_rejected_except_status_and_responses_advertise_compat() {
    let dir = tempfile::tempdir().unwrap();
    let node = raft_harness::spawn_node(raft_harness::settings(dir.path(), 1, true)).await;

    // 对端更新（wire 3）→ 426 + 本机能力；响应头在所有分支都存在。
    let resp = client()
        .post(format!("{}/api/v1/site/raft/append", node.base))
        .header(RPC_TOKEN_HEADER, &node.token)
        .header(RPC_TARGET_HEADER, "1")
        .header(WIRE_HEADER, "3")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UPGRADE_REQUIRED);
    assert_eq!(header(&resp, WIRE_HEADER), "2");
    assert_eq!(header(&resp, FEATURE_HEADER), "1");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "wire_version_unsupported");

    // R3：诊断探针跨版本可用，wire 3 也照常返回 200。
    let resp = client()
        .get(format!("{}/api/v1/site/raft/status", node.base))
        .header(RPC_TOKEN_HEADER, &node.token)
        .header(WIRE_HEADER, "3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(header(&resp, WIRE_HEADER), "2");
    assert_eq!(header(&resp, FEATURE_HEADER), "1");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["wire_version"], 2);
    assert_eq!(body["feature_level"], 1);
    assert_eq!(body["activation_level"], 1);

    // R2：缺失头按 1，不会触发 426（请求体非法由 Json 提取器拒绝）。
    let resp = client()
        .post(format!("{}/api/v1/site/raft/append", node.base))
        .header(RPC_TOKEN_HEADER, &node.token)
        .header(RPC_TARGET_HEADER, "1")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), reqwest::StatusCode::UPGRADE_REQUIRED);

    node.shutdown().await;
}
