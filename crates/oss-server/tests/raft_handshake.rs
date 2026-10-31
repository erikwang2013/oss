//! M9 滚动升级 Level B：RPC 握手中间件（R1/R3）与响应头。

mod common;

use common::raft_harness;
use oss_server::raft::compat::{EXT_TYPES_HEADER, FEATURE_HEADER, WIRE_HEADER};
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

/// Level C M1：`/status` 暴露能力上限与快照格式，console/runbook 才能判断
/// “level 4 = M8-2 作业运行态复制、level 5 = inventory 运行态复制”
/// （而非某项业务已启用）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compat_status_exposes_feature_max_and_snapshot_format() {
    use oss_server::raft::compat::{MAX_FEATURE_LEVEL, MIN_FEATURE_LEVEL};
    use oss_server::raft::data::SNAPSHOT_FORMAT;

    let dir = tempfile::tempdir().unwrap();
    let mut settings = raft_harness::settings(dir.path(), 1, true);
    settings.feature_level = MAX_FEATURE_LEVEL;
    let node = raft_harness::spawn_node(settings).await;
    raft_harness::wait_leader(&node.handle).await;

    let body: serde_json::Value = client()
        .get(format!("{}/api/v1/site/raft/status", node.base))
        .header(RPC_TOKEN_HEADER, &node.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["feature_level"], MAX_FEATURE_LEVEL);
    // 生产注册表为空：能力集必须是空数组（旧节点无此键 = 同一语义）。
    assert_eq!(body["ext_types"], serde_json::json!([]));
    assert_eq!(body["feature_max"], MAX_FEATURE_LEVEL);
    assert_eq!(body["snapshot_format"], SNAPSHOT_FORMAT);
    assert_eq!(body["effective_feature_level"], MAX_FEATURE_LEVEL);
    // 当前二进制没有只读模式（1.13.2 patch 才会置 true）。
    assert_eq!(body["read_only"].as_bool(), Some(false));
    assert_eq!(body["activation_level"], MIN_FEATURE_LEVEL);

    node.shutdown().await;
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
    // 错误分支也宣告 Ext 能力（生产为空 = 空串）。
    assert_eq!(header(&resp, EXT_TYPES_HEADER), "");
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
